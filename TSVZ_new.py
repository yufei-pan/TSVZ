#! /usr/bin/env python3
"""
TSVZ core library implementing tsvz-spec-v1.

This module provides an append-only write-ahead log (WAL) for tabular
key–value data. Compaction is performed by :func:`snapshot_part` (a
simplified form of specification §19), which rewrites a part from live
state rather than mutating historical records in place. Readers follow the
current specification only; legacy escaping and tombstone conventions are
not accepted.

Preferred public entry points for new code are :func:`read_store`,
:func:`append_records`, :class:`WalStore`, and :func:`snapshot_part`.
Legacy names such as :func:`readTabularFile` and :class:`TSVZed` remain as
thin compatibility wrappers.

Implemented
-----------
- §4 — commit boundary and torn-tail discard (:func:`committed_payload`)
- §5 — delimiter inference from ``.tsv`` / ``.csv`` / ``.nsv`` / ``.psv``
  and the corresponding ``*z`` extensions
- §6–§7 — single-part replay (:func:`process_record`, :func:`replay_bytes`,
  :func:`replay_part`)
- §8 — empty-key ignore; §9 — lone-key tombstones; §10 — trailing-whitespace
  strip
- §11 — ``#`` comments and header-as-comment
- §12 — stateful markers: ``#_version_#``, ``#_defaults_#``,
  ``#_strip_trailing_whites_#``, ``#_fill_empty_with_default_#``,
  ``#_return_defaults_when_missing_#``
- §13 — field escaping (``<sep>``, ``<LF>``, ``<lt>``, ``<#>``)
- §14 — defaults, fill-empty, and return-on-missing behaviour
- §16 — transparent compression for ``.gz`` / ``.bz2`` / ``.xz`` / ``.zst``
- Simplified §19 — :func:`snapshot_part` rewrites one part with a marker
  preamble and live rows, discarding superseded values, tombstones, and
  non-header comments
- Stores — :class:`WalStore` (asynchronous append) and :class:`OffsetStore`
  (key→byte-offset index)

Not yet implemented
-------------------
- §15 integrity — ``#_checksum_<algo>_#`` is classified and ignored (no
  digest arming or verification); unrecognized markers are likewise skipped
  on the data path
- ``#_rotate_#`` / ``#_write_ack_#`` — recognized as markers but treated as
  no-ops in :func:`apply_marker`
- §17 multi-part stores (``store.tsvz.<ordinal>``, cross-part replay)
- Full §19 snapshot procedure (ordinal slotting, ``.rotated`` exclusion,
  immutable prefix with an active writer)

Examples:
	>>> import os, tempfile
	>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd); os.unlink(path)
	>>> append_records(path, [['alice', 'Alice', '10'], ['bob', 'Bob', '20']], create=True)
	>>> dict(read_store(path))
	{'alice': ['alice', 'Alice', '10'], 'bob': ['bob', 'Bob', '20']}
	>>> append_record(path, ['bob'])  # lone key = tombstone (§9)
	>>> dict(read_store(path))
	{'alice': ['alice', 'Alice', '10']}
	>>> os.unlink(path)
"""
import atexit
import contextlib
import io
import os
import re
import sys
import threading
import time
from collections import OrderedDict, deque
from collections.abc import MutableMapping

if os.name == 'nt':
	import msvcrt
elif os.name == 'posix':
	import fcntl

__version__ = '4.0.0'

DEFAULT_DELIMITER = '\t'
MARKER_DEFAULTS = '#_defaults_#'
MAX_SPEC_VERSION = 1

COMPRESSION_EXTENSIONS = frozenset(
	{'gz', 'gzip', 'bz2', 'bzip2', 'xz', 'lzma', 'zst', 'zstd'})
STRICT_EXTENSIONS = frozenset({'.tsvz', '.csvz', '.nsvz', '.psvz'})

MARKER_RE = re.compile(r'^#_[A-Za-z0-9_-]+_#$', re.ASCII)
CHECKSUM_MARKER_RE = re.compile(r'^#_checksum_[A-Za-z0-9_-]+_#$', re.ASCII)
OFFICIAL_MARKERS = frozenset({
	'#_version_#', '#_defaults_#', '#_strip_trailing_whites_#',
	'#_fill_empty_with_default_#', '#_return_defaults_when_missing_#',
	'#_rotate_#', '#_write_ack_#',
})

_TOMBSTONE = object()


# ---------------------------------------------------------------------------
# §7 reading pipeline
# ---------------------------------------------------------------------------

class ReaderState:
	"""Mutable reader configuration accumulated while replaying a part.

	State is updated by official markers (§12) and consulted when decoding
	data rows (§6–§7, §14).

	Attributes:
		version (int): Effective specification version (currently always 1).
		defaults (list): Value-column defaults from ``#_defaults_#`` (excludes
			the key field).
		strip_trailing (bool): If True, strip trailing spaces and tabs from
			each field before decoding.
		fill_empty (bool): If True, replace empty present value cells from
			``defaults``.
		return_on_missing (bool): If True, synthesize a defaults row for
			absent keys instead of raising ``KeyError``.
	"""

	__slots__ = ('defaults', 'fill_empty', 'return_on_missing', 'strip_trailing', 'version')

	def __init__(self):
		self.version = 1
		self.defaults = []
		self.strip_trailing = True
		self.fill_empty = False
		self.return_on_missing = True

	def copy(self):
		"""Return a deep copy of this state (defaults list is independent).

		Returns:
			ReaderState: A new state with the same field values.

		Examples:
			>>> s = ReaderState(); s.defaults = ['x']; s.fill_empty = True
			>>> t = s.copy()
			>>> t.defaults, t.fill_empty
			(['x'], True)
			>>> t.defaults.append('y'); s.defaults
			['x']
		"""
		s = ReaderState()
		s.version = self.version
		s.defaults = list(self.defaults)
		s.strip_trailing = self.strip_trailing
		s.fill_empty = self.fill_empty
		s.return_on_missing = self.return_on_missing
		return s


class StoreEntry:
	"""A live data row held during replay, prior to list materialization.

	Attributes:
		row (list): Fields in the form ``[key, value0, value1, ...]``.
		row_defaults (list): Snapshot of reader defaults at write time.
		fill_empty (bool): Whether fill-empty was enabled when the row was
			written.
	"""

	__slots__ = ('fill_empty', 'row', 'row_defaults')

	def __init__(self, row, row_defaults, fill_empty):
		self.row = row
		self.row_defaults = list(row_defaults)
		self.fill_empty = fill_empty


def _strip_field(raw, enabled):
	return raw if not enabled else raw.rstrip(' \t')


def decode_field(raw, delimiter):
	"""Decode a single escaped field according to specification §13.

	Recognized escape sequences are ``<sep>`` (delimiter), ``<LF>`` (newline),
	``<lt>`` (literal ``<``), and ``<#>`` (literal ``#``). Unrecognized
	``<...>`` sequences are left unchanged.

	Args:
		raw: Escaped field text.
		delimiter: Field delimiter used for ``<sep>`` expansion.

	Returns:
		str: Decoded field value.

	Examples:
		>>> decode_field('a<sep>b', '\\t')
		'a\\tb'
		>>> decode_field('line<LF>two', '\\t')
		'line\\ntwo'
		>>> decode_field('<lt>sep>', '\\t')
		'<sep>'
		>>> decode_field('<#>foo', '\\t')
		'#foo'
		>>> decode_field('<future>', '\\t')
		'<future>'
	"""
	out = []
	i = 0
	while i < len(raw):
		if raw[i] == '<':
			end = raw.find('>', i + 1)
			if end == -1:
				out.append(raw[i:])
				break
			name = raw[i + 1:end]
			if name == 'sep':
				out.append(delimiter)
			elif name == 'LF':
				out.append('\n')
			elif name == 'lt':
				out.append('<')
			elif name == '#':
				out.append('#')
			else:
				out.append(raw[i:end + 1])
			i = end + 1
		else:
			out.append(raw[i])
			i += 1
	return ''.join(out)


def encode_field(value, delimiter, *, is_key=False):
	"""Encode a single field for writing according to specification §13.

	The delimiter, newline, and ``<`` characters are always escaped. A leading
	``#`` is escaped only when ``is_key`` is True (so that keys cannot be
	misclassified as comments). ``None`` is encoded as the empty string.

	Args:
		value: Field value to encode; ``None`` becomes ``''``.
		delimiter: Field delimiter to escape as ``<sep>``.
		is_key: If True, escape a leading ``#`` as ``<#>``.

	Returns:
		str: Escaped field text suitable for writing.

	Examples:
		>>> encode_field('a\\tb', '\\t')
		'a<sep>b'
		>>> encode_field('line\\ntwo', '\\t')
		'line<LF>two'
		>>> encode_field('<sep>', '\\t')
		'<lt>sep>'
		>>> encode_field('#foo', '\\t', is_key=True)
		'<#>foo'
		>>> encode_field('#foo', '\\t', is_key=False)
		'#foo'
		>>> encode_field(None, '\\t')
		''
	"""
	value = '' if value is None else str(value)
	out = []
	for i, ch in enumerate(value):
		if ch == delimiter:
			out.append('<sep>')
		elif ch == '\n':
			out.append('<LF>')
		elif ch == '<':
			out.append('<lt>')
		elif ch == '#' and is_key and i == 0:
			out.append('<#>')
		else:
			out.append(ch)
	return ''.join(out)


def _default_at(defaults, col_j):
	idx = col_j - 1
	return defaults[idx] if idx < len(defaults) else ''


def _parse_bool(s):
	"""Parse a boolean marker argument.

	Accepted truthy values are ``true``, ``yes``, ``on``, and ``1``; falsy
	values are ``false``, ``no``, ``off``, and ``0`` (case-insensitive). An
	empty or unrecognized string returns ``None``, signalling that the
	built-in default for that marker should be retained.

	Args:
		s: Raw marker value field.

	Returns:
		bool | None: Parsed boolean, or ``None`` when unset or unrecognized.

	Examples:
		>>> _parse_bool('true'), _parse_bool('FALSE'), _parse_bool(''), _parse_bool('maybe')
		(True, False, None, None)
		>>> _parse_bool('yes'), _parse_bool('no'), _parse_bool('on'), _parse_bool('off'), _parse_bool('1'), _parse_bool('0')
		(True, False, True, False, True, False)
	"""
	if not s:
		return None
	s = s.strip().lower()
	true_values = {'true', 'yes', 'on', '1'}
	false_values = {'false', 'no', 'off', '0'}
	if s in true_values:
		return True
	if s in false_values:
		return False
	return None


def classify_record(f0_raw):
	"""Classify the first raw field of a record before decoding.

	Classification follows specification §§7.4–7.5 and §§11–12.

	Args:
		f0_raw: Undecoded first field of the line.

	Returns:
		str: One of ``'data'``, ``'comment'``, ``'marker'``, or ``'ignore'``.

	Examples:
		>>> classify_record('alice')
		'data'
		>>> classify_record('# comment')
		'comment'
		>>> classify_record('#_defaults_#')
		'marker'
		>>> classify_record('#_checksum_sha256_#')
		'ignore'
		>>> classify_record('#_future_marker_#')
		'ignore'
	"""
	if not f0_raw.startswith('#'):
		return 'data'
	if not MARKER_RE.match(f0_raw):
		return 'comment'
	kl = f0_raw.lower()
	if CHECKSUM_MARKER_RE.match(f0_raw):
		return 'ignore'
	if kl in OFFICIAL_MARKERS:
		return 'marker'
	return 'ignore'


def apply_marker(state, f0_raw, value_fields, delimiter):
	"""Apply an official marker line to ``state`` (specification §12).

	Unrecognized or no-op markers (``#_rotate_#``, ``#_write_ack_#``) leave
	``state`` unchanged. Value fields are decoded without trailing-whitespace
	stripping.

	Args:
		state: Reader state to update in place.
		f0_raw: Undecoded marker key (field 0).
		value_fields: Remaining raw fields after the marker key.
		delimiter: Field delimiter used when decoding values.

	Returns:
		None: ``state`` is mutated in place.

	Examples:
		>>> st = ReaderState()
		>>> apply_marker(st, '#_defaults_#', ['n/a', '0'], '\\t')
		>>> st.defaults
		['n/a', '0']
		>>> apply_marker(st, '#_fill_empty_with_default_#', ['true'], '\\t')
		>>> st.fill_empty
		True
		>>> apply_marker(st, '#_strip_trailing_whites_#', ['false'], '\\t')
		>>> st.strip_trailing
		False
	"""
	kl = f0_raw.lower()
	decoded = [decode_field(_strip_field(f, False), delimiter) for f in value_fields]
	if kl == '#_version_#':
		if not decoded or decoded[0] == '':
			state.version = 1
		else:
			try:
				state.version = min(max(int(decoded[0]), 1), MAX_SPEC_VERSION)
			except ValueError:
				state.version = 1
	elif kl == '#_defaults_#':
		state.defaults = [] if not value_fields else decoded
	elif kl == '#_strip_trailing_whites_#':
		b = _parse_bool(decoded[0] if decoded else '')
		state.strip_trailing = True if b is None else b
	elif kl == '#_fill_empty_with_default_#':
		b = _parse_bool(decoded[0] if decoded else '')
		state.fill_empty = False if b is None else b
	elif kl == '#_return_defaults_when_missing_#':
		b = _parse_bool(decoded[0] if decoded else '')
		state.return_on_missing = True if b is None else b


def committed_payload(data):
	"""Return the committed prefix of ``data`` (specification §4).

	Only bytes through the last newline are retained; a torn trailing line
	without a terminating ``\\n`` is discarded. If no newline is present, an
	empty bytes object is returned.

	Args:
		data: Raw part bytes, possibly ending in a torn line.

	Returns:
		bytes: Committed prefix ending at the last ``\\n``, or ``b''``.

	Examples:
		>>> committed_payload(b'a\\nb\\n')
		b'a\\nb\\n'
		>>> committed_payload(b'a\\nb')
		b'a\\n'
		>>> committed_payload(b'torn')
		b''
		>>> committed_payload(b'')
		b''
	"""
	if not data:
		return b''
	last_nl = data.rfind(b'\n')
	return b'' if last_nl == -1 else data[:last_nl + 1]


def _resolve_value_columns(fields, state, delimiter):
	"""Strip, decode, and optionally fill present-empty value cells.

	Implements specification §§7.9 and 14.2. Absent trailing columns are not
	padded; only cells that are present and empty are filled when
	``state.fill_empty`` is True.

	Args:
		fields: Raw delimiter-split fields with the key at index 0.
		state: Current reader state controlling strip/fill behaviour.
		delimiter: Field delimiter used when decoding.

	Returns:
		tuple: ``(row, defaults_snapshot, fill_empty)`` where ``row`` is
		``[key, value…]``.

	Examples:
		>>> st = ReaderState(); st.defaults = ['n/a', '0']
		>>> _resolve_value_columns(['alice', 'Alice', '10'], st, '\\t')
		(['alice', 'Alice', '10'], ['n/a', '0'], False)
		>>> st.fill_empty = True
		>>> _resolve_value_columns(['bob', '', ''], st, '\\t')
		(['bob', 'n/a', '0'], ['n/a', '0'], True)
		>>> st.strip_trailing = True
		>>> _resolve_value_columns(['k ', 'v \\t'], st, '\\t')[0]
		['k', 'v']
		>>> _resolve_value_columns(['k', 'a<sep>b'], st, '\\t')[0]
		['k', 'a\\tb']
	"""
	key = decode_field(_strip_field(fields[0], state.strip_trailing), delimiter)
	row = [key]
	for j in range(1, len(fields)):
		cell = decode_field(_strip_field(fields[j], state.strip_trailing), delimiter)
		if cell == '' and state.fill_empty:
			cell = _default_at(state.defaults, j)
		row.append(cell)
	return row, list(state.defaults), state.fill_empty


def process_record(raw_line, state, store, delimiter, *, offset=None,
				   store_offset=False, values_cache=None):
	"""Process one logical line and update ``store``.

	A line consisting of a lone key (no delimiter) is a tombstone
	(specification §9.2). A key followed by a delimiter with an empty value
	(``key\\t``) is a live data row with an empty value cell, not a tombstone.
	Empty keys are ignored.

	Args:
		raw_line: One logical line without its terminating newline.
		state: Reader state; updated when a marker is encountered.
		store: Mutable mapping updated with live entries or offsets.
		delimiter: Field delimiter.
		offset: Byte offset of this line within the part (optional).
		store_offset: If True, store ``offset`` instead of a :class:`StoreEntry`.
		values_cache: Optional key→row cache updated alongside ``store``.

	Returns:
		tuple: ``(kind, payload)`` where ``kind`` is one of ``'data'``,
		``'tombstone'``, ``'marker'``, ``'comment'``, or ``'ignore'``.

	Examples:
		>>> st, store = ReaderState(), OrderedDict()
		>>> process_record('alice\\tAlice\\t10', st, store, '\\t')[0]
		'data'
		>>> list(store['alice'].row)
		['alice', 'Alice', '10']
		>>> process_record('alice', st, store, '\\t')
		('tombstone', 'alice')
		>>> 'alice' in store
		False
		>>> process_record('bob\\t', st, store, '\\t')[0]  # live empty value
		'data'
		>>> list(store['bob'].row)
		['bob', '']
	"""
	fields = raw_line.split(delimiter)
	f0 = fields[0]
	kind = classify_record(f0)
	if kind in ('comment', 'ignore'):
		return kind, None
	if kind == 'marker':
		apply_marker(state, f0, fields[1:], delimiter)
		return kind, None
	is_tombstone = len(fields) == 1
	key = decode_field(_strip_field(f0, state.strip_trailing), delimiter)
	if key == '':
		return 'data', None
	if is_tombstone:
		store.pop(key, None)
		if values_cache is not None:
			values_cache.pop(key, None)
		return 'tombstone', key
	row, row_defaults, fill_empty = _resolve_value_columns(fields, state, delimiter)
	entry = StoreEntry(row, row_defaults, fill_empty)
	if store_offset and offset is not None:
		store[key] = offset
	else:
		store[key] = entry
	if values_cache is not None:
		values_cache[key] = list(row)
	return 'data', entry


def replay_bytes(data, delimiter, *, encoding='utf8', store=None,
				 store_offset=False, values_cache=None):
	"""Replay committed bytes into a key→entry mapping (last write wins).

	Only the committed payload is processed (see :func:`committed_payload`).

	Args:
		data: Raw part bytes.
		delimiter: Field delimiter.
		encoding: Text encoding used to decode lines.
		store: Optional existing mapping to update; a new
			:class:`~collections.OrderedDict` is created when ``None``.
		store_offset: If True, store byte offsets instead of entries.
		values_cache: Optional key→row cache filled during replay.

	Returns:
		tuple: ``(store, state)`` where ``state`` is the final
		:class:`ReaderState` after all markers have been applied.

	Examples:
		>>> store, _ = replay_bytes(b'a\\t1\\na\\t2\\nb\\t3\\nb\\n', '\\t')
		>>> sorted((k, list(v.row)) for k, v in store.items())
		[('a', ['a', '2'])]
	"""
	if store is None:
		store = OrderedDict()
	state = ReaderState()
	payload = committed_payload(data)
	pos = 0
	while pos < len(payload):
		nl = payload.find(b'\n', pos)
		if nl == -1:
			break
		line = payload[pos:nl].decode(encoding, errors='replace')
		if line.endswith('\r'):  # noqa: FURB188  # removesuffix needs 3.9+
			line = line[:-1]
		if line:
			process_record(
				line, state, store, delimiter,
				offset=pos, store_offset=store_offset, values_cache=values_cache,
			)
		pos = nl + 1
	return store, state


def replay_part(path, delimiter, *, encoding='utf8', store=None,
				store_offset=False, values_cache=None):
	"""Replay a part file from disk into a key→entry mapping.

	If ``path`` does not exist, returns an empty store paired with a fresh
	:class:`ReaderState`. Otherwise delegates to :func:`replay_bytes`.

	Args:
		path: Filesystem path of the part.
		delimiter: Field delimiter.
		encoding: Text encoding used to decode lines.
		store: Optional existing mapping to update.
		store_offset: If True, store byte offsets instead of entries.
		values_cache: Optional key→row cache filled during replay.

	Returns:
		tuple: ``(store, state)`` after replaying the part.
	"""
	if store is None:
		store = OrderedDict()
	try:
		with open_part(path, 'rb', encoding=encoding) as f:
			data = f.read()
	except FileNotFoundError:
		return store, ReaderState()
	return replay_bytes(
		data, delimiter, encoding=encoding, store=store,
		store_offset=store_offset, values_cache=values_cache,
	)


def resolve_missing_key(key, state):
	"""Resolve a missing key according to specification §14.

	When ``state.return_on_missing`` is True, returns a synthesized row of
	``[key]`` followed by the current defaults. Otherwise raises ``KeyError``.

	Args:
		key: Missing key to resolve.
		state: Reader state controlling return-on-missing and defaults.

	Returns:
		list: Synthesized ``[key, ...]`` row when return-on-missing is enabled.

	Examples:
		>>> resolve_missing_key('x', ReaderState())
		['x']
		>>> st = ReaderState(); st.defaults = ['n/a', '0']
		>>> resolve_missing_key('x', st)
		['x', 'n/a', '0']
		>>> st.return_on_missing = False
		>>> resolve_missing_key('x', st)
		Traceback (most recent call last):
			...
		KeyError: 'x'
	"""
	if not state.return_on_missing:
		raise KeyError(key)
	row = [key]
	for j in range(1, max(len(state.defaults) + 1, 1)):
		row.append(_default_at(state.defaults, j))
	return row


# ---------------------------------------------------------------------------
# §13 / §4 writers
# ---------------------------------------------------------------------------

def format_data_row(fields, delimiter):
	"""Format a data row (key plus value columns) for appending.

	Args:
		fields: Sequence of field values; index 0 is the key.
		delimiter: Field delimiter.

	Returns:
		str: Encoded line without a trailing newline.

	Examples:
		>>> format_data_row(['k', 'a\\tb'], '\\t')
		'k\\ta<sep>b'
	"""
	return delimiter.join(encode_field(f, delimiter, is_key=(i == 0)) for i, f in enumerate(fields))


def format_tombstone(key, delimiter):
	"""Format a tombstone line consisting of the encoded key alone (§9).

	The result contains no delimiter, distinguishing it from a live empty
	value (``key\\t``).

	Args:
		key: Key to delete.
		delimiter: Field delimiter (used only for key escaping).

	Returns:
		str: Encoded tombstone line without a trailing newline.

	Examples:
		>>> format_tombstone('alice', '\\t')
		'alice'
		>>> format_tombstone('#x', '\\t')
		'<#>x'
	"""
	return encode_field(key, delimiter, is_key=True)


def format_marker_line(marker_key, values, delimiter):
	"""Format an official marker line with optional value fields.

	Args:
		marker_key: Official marker token (for example ``#_defaults_#``).
		values: Value fields following the marker key.
		delimiter: Field delimiter.

	Returns:
		str: Encoded marker line without a trailing newline.

	Examples:
		>>> format_marker_line('#_defaults_#', ['n/a', '0'], '\\t')
		'#_defaults_#\\tn/a\\t0'
	"""
	parts = [marker_key] + [encode_field(v, delimiter) for v in values]
	return delimiter.join(parts)


def format_header_comment(columns, delimiter):
	"""Format column names as a ``#`` comment header line (§11).

	Args:
		columns: Column name sequence.
		delimiter: Field delimiter.

	Returns:
		str: Encoded header comment, or ``''`` when ``columns`` is empty.

	Examples:
		>>> format_header_comment(['id', 'name'], '\\t')
		'#id\\tname'
		>>> format_header_comment([], '\\t')
		''
	"""
	if not columns:
		return ''
	encoded = [encode_field(c, delimiter) for c in columns]
	encoded[0] = '#' + encoded[0]
	return delimiter.join(encoded)


def build_snapshot_preamble(state, delimiter):
	"""Build the marker preamble written at the start of a snapshot (§19).

	Always includes ``#_version_#``. Optional markers are emitted only when
	their values differ from the built-in defaults, except ``#_defaults_#``
	which is emitted whenever defaults are non-empty.

	Args:
		state: Reader state whose non-default settings are serialized.
		delimiter: Field delimiter.

	Returns:
		list[str]: Marker lines without trailing newlines.

	Examples:
		>>> build_snapshot_preamble(ReaderState(), '\\t')
		['#_version_#\\t1']
		>>> st = ReaderState(); st.fill_empty = True; st.defaults = ['x']
		>>> build_snapshot_preamble(st, '\\t')
		['#_version_#\\t1', '#_fill_empty_with_default_#\\ttrue', '#_defaults_#\\tx']
	"""
	lines = [format_marker_line('#_version_#', ['1'], delimiter)]
	if not state.strip_trailing:
		lines.append(format_marker_line('#_strip_trailing_whites_#', ['false'], delimiter))
	if state.fill_empty:
		lines.append(format_marker_line('#_fill_empty_with_default_#', ['true'], delimiter))
	if not state.return_on_missing:
		lines.append(format_marker_line('#_return_defaults_when_missing_#', ['false'], delimiter))
	if state.defaults:
		lines.append(format_marker_line('#_defaults_#', state.defaults, delimiter))
	return lines


def _queue_item_to_bytes(item, delimiter, encoding):
	if isinstance(item, tuple) and len(item) == 2 and item[0] is _TOMBSTONE:
		line = format_tombstone(item[1], delimiter)
	elif isinstance(item, list):
		if len(item) == 1:
			line = format_tombstone(item[0], delimiter)
		elif item[0] == MARKER_DEFAULTS:
			line = format_marker_line(MARKER_DEFAULTS, item[1:], delimiter)
		else:
			line = format_data_row(item, delimiter)
	else:
		return b''
	return line.encode(encoding, errors='replace') + b'\n'


# ---------------------------------------------------------------------------
# §5 / §16 I/O
# ---------------------------------------------------------------------------

def _strip_compression_suffix(name):
	base, _, ext = name.lower().rpartition('.')
	return base if ext in COMPRESSION_EXTENSIONS else name.lower()


def is_strict_store(path):
	"""Return True if ``path`` uses a strict ``*z`` store extension (§5).

	Compression suffixes (``.gz``, ``.bz2``, and so on) are stripped before
	the extension is examined.

	Args:
		path: Filesystem path to inspect.

	Returns:
		bool: True when the path is a strict store (``.tsvz``, ``.csvz``,
		``.nsvz``, or ``.psvz``, with optional compression suffix).

	Examples:
		>>> is_strict_store('data.tsvz'), is_strict_store('data.tsv')
		(True, False)
		>>> is_strict_store('data.csvz.gz')
		True
	"""
	lower = _strip_compression_suffix(path)
	return any(lower.endswith(ext) for ext in STRICT_EXTENSIONS)


def delimiter_for_path(path, delimiter=None):
	"""Infer the field delimiter from ``path``, or honor an explicit override.

	When ``delimiter`` is not ``None``, it is returned as-is (or
	:data:`DEFAULT_DELIMITER` if empty). Otherwise the delimiter is chosen
	from the path extension: comma for ``.csv``/``.csvz``, NUL for
	``.nsv``/``.nsvz``, pipe for ``.psv``/``.psvz``, and tab otherwise.

	Args:
		path: Filesystem path whose extension selects the delimiter.
		delimiter: Explicit override; when ``None``, infer from ``path``.

	Returns:
		str: Field delimiter character.

	Examples:
		>>> delimiter_for_path('x.tsv'), delimiter_for_path('x.csv'), delimiter_for_path('x.psv')
		('\\t', ',', '|')
		>>> delimiter_for_path('data.csv.gz')
		','
		>>> delimiter_for_path('x.unknown', delimiter='|')
		'|'
	"""
	if delimiter is not None:
		return delimiter or DEFAULT_DELIMITER
	lower = _strip_compression_suffix(path)
	if lower.endswith(('.csv', '.csvz')):
		return ','
	if lower.endswith(('.nsv', '.nsvz')):
		return '\0'
	if lower.endswith(('.psv', '.psvz')):
		return '|'
	return DEFAULT_DELIMITER


def open_part(path, mode='rb', *, encoding='utf8', compress_level=1):
	"""Open a part file, transparently handling common compression suffixes.

	Supports ``.gz``/``.gzip``, ``.bz2``/``.bzip2``, ``.xz``/``.lzma``, and
	``.zst``/``.zstd`` (specification §16). Uncompressed paths fall through
	to the built-in :func:`open`.

	Args:
		path: Filesystem path of the part.
		mode: Open mode (binary preferred; text modes are normalized).
		encoding: Text encoding when opening in text mode.
		compress_level: Compression level for write modes.

	Returns:
		file: An open file-like object for the part.
	"""
	lower = path.lower()
	if 'b' not in mode:
		mode += 't'
	kwargs = {}
	if 'r' not in mode:
		if lower.endswith('.xz'):
			kwargs['preset'] = compress_level
		elif lower.endswith(('.zst', '.zstd')):
			kwargs['level'] = compress_level
		else:
			kwargs['compresslevel'] = compress_level
	if 'b' not in mode:
		kwargs['encoding'] = encoding
	if lower.endswith(('.xz', '.lzma')):
		import lzma
		return lzma.open(path, mode, **kwargs)
	if lower.endswith(('.gz', '.gzip')):
		import gzip
		return gzip.open(path, mode, **kwargs)
	if lower.endswith(('.bz2', '.bzip2')):
		import bz2
		return bz2.open(path, mode, **kwargs)
	if lower.endswith(('.zst', '.zstd')):
		try:
			from compression import zstd
			return zstd.open(path, mode, **kwargs)
		except ImportError:
			pass
	if 't' in mode:
		return open(path, mode.replace('t', ''), encoding=encoding)
	if 'b' not in mode:
		mode += 'b'
	return open(path, mode)


def _parse_columns(header, delimiter):
	if not header:
		return []
	if isinstance(header, str):
		return header.split(delimiter)
	return [str(c).rstrip() for c in header]


def _normalize_defaults(defaults):
	if defaults is None:
		return []
	if isinstance(defaults, list) and defaults and defaults[0] == MARKER_DEFAULTS:
		return list(defaults[1:])
	return list(defaults)


def ensure_part_exists(path, *, create=True, encoding='utf8', delimiter=None,
					   header=None, defaults=None):
	"""Ensure that a part file exists at ``path``.

	When the file is absent and ``create`` is True, an empty part is created,
	optionally seeded with a header comment and a ``#_defaults_#`` marker.
	When ``create`` is False and the file is absent, raises
	``FileNotFoundError``.

	Args:
		path: Filesystem path of the part.
		create: If True, create a missing part; otherwise raise.
		encoding: Text encoding for newly written header/defaults lines.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		header: Optional column names written as a ``#`` comment.
		defaults: Optional value-column defaults written as a marker.

	Returns:
		bool: True when the part exists (or was created).

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd); os.unlink(path)
		>>> ensure_part_exists(path, create=True, header=['id', 'name'])
		True
		>>> open(path).readline()
		'#id\\tname\\n'
		>>> os.unlink(path)
		>>> ensure_part_exists(path, create=False)
		Traceback (most recent call last):
			...
		FileNotFoundError: ...
	"""
	delimiter = delimiter or delimiter_for_path(path)
	header = _parse_columns(header, delimiter)
	defaults = _normalize_defaults(defaults)
	if os.path.isfile(path):
		return True
	if not create:
		raise FileNotFoundError(path)
	with open_part(path, 'wb', encoding=encoding) as f:
		if header:
			f.write(format_header_comment(header, delimiter).encode(encoding, errors='replace') + b'\n')
		if defaults:
			line = format_marker_line(MARKER_DEFAULTS, defaults, delimiter)
			f.write(line.encode(encoding, errors='replace') + b'\n')
	return True


def _attach_replay_meta(target, state, values_cache=None):
	try:
		target._reader_state = state
		if values_cache is not None:
			target._values_cache = values_cache
	except AttributeError:
		pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_last_record(path, *, encoding='utf8', delimiter=None, store_offset=False):
	"""Return the last committed data row in a part file.

	When ``store_offset`` is True, returns the byte offset of that row instead
	of its field list. Returns an empty list (or ``-1`` when
	``store_offset`` is True) if the file is missing or contains no data rows.

	Args:
		path: Filesystem path of the part.
		encoding: Text encoding used to decode lines.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		store_offset: If True, return the byte offset instead of the row.

	Returns:
		list | int: Last data row, its byte offset, or an empty sentinel.

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsv'); os.close(fd)
		>>> open(path, 'w').write('a\\t1\\nb\\t2\\nc\\t3\\n')
		12
		>>> read_last_record(path)
		['c', '3']
		>>> os.unlink(path)
	"""
	delimiter = delimiter or delimiter_for_path(path)
	empty = -1 if store_offset else []
	try:
		with open_part(path, 'rb', encoding=encoding) as f:
			data = f.read()
	except FileNotFoundError:
		return empty
	state = ReaderState()
	result = empty
	last_offset = -1
	pos = 0
	payload = committed_payload(data)
	while pos < len(payload):
		nl = payload.find(b'\n', pos)
		if nl == -1:
			break
		line = payload[pos:nl].decode(encoding, errors='replace')
		if line.endswith('\r'):  # noqa: FURB188  # removesuffix needs 3.9+
			line = line[:-1]
		if line:
			scratch = OrderedDict()
			kind, entry = process_record(line, state, scratch, delimiter)
			if kind == 'data' and entry is not None:
				last_offset = pos
				result = pos if store_offset else list(entry.row)
		pos = nl + 1
	return last_offset if store_offset else result


def _fit_row(row, column_count):
	"""Pad or truncate ``row`` to exactly ``column_count`` fields.

	Used by the legacy reader when a fixed schema width is required.

	Args:
		row: Row list to adjust.
		column_count: Target field count; ``None`` or negative leaves
			``row`` unchanged.

	Returns:
		list: Row padded with empty strings or truncated to ``column_count``.

	Examples:
		>>> _fit_row(['k', 'a'], 4)
		['k', 'a', '', '']
		>>> _fit_row(['k', 'a', 'b', 'c'], 2)
		['k', 'a']
	"""
	if column_count is None or column_count < 0 or not isinstance(row, list):
		return row
	if len(row) < column_count:
		return list(row) + [''] * (column_count - len(row))
	if len(row) > column_count:
		return list(row)[:column_count]
	return list(row)


def read_store(path, *, create=False, encoding='utf8', delimiter=None,
			   defaults=None, store=None, store_offset=False, last_record_only=False,
			   header=None):
	"""Replay a part into an ordered mapping of key to row list (or byte offset).

	Each row retains the width it was written with (specification §3.6).
	Callers that require a fixed schema may pad or trim rows themselves, or
	use the legacy :func:`readTabularFile` helper.

	When ``store_offset`` is True, values are byte offsets rather than row
	lists, and a ``_values_cache`` attribute is attached for on-demand
	materialization. When ``last_record_only`` is True, delegates to
	:func:`read_last_record`.

	Args:
		path: Filesystem path of the part.
		create: If True, create a missing part before reading.
		encoding: Text encoding used to decode lines.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		defaults: Optional defaults written when creating a new part.
		store: Optional mapping to populate; a new ordered dict is used
			when ``None``.
		store_offset: If True, map keys to byte offsets instead of rows.
		last_record_only: If True, return only the last committed data row.
		header: Optional header columns written when creating a new part.

	Returns:
		MutableMapping: Live key→row (or key→offset) mapping.

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd); os.unlink(path)
		>>> append_records(path, [['a', '1'], ['b', '2']], create=True, header=['id', 'v'])
		>>> dict(read_store(path))
		{'a': ['a', '1'], 'b': ['b', '2']}
		>>> os.unlink(path)
	"""
	delimiter = delimiter or delimiter_for_path(path)
	if store is None:
		store = OrderedDict()
	header_cols = _parse_columns(header, delimiter) if header else []
	ensure_part_exists(
		path, create=create, encoding=encoding, delimiter=delimiter,
		header=header_cols or None, defaults=_normalize_defaults(defaults),
	)
	if last_record_only:
		return read_last_record(path, encoding=encoding, delimiter=delimiter,
								store_offset=store_offset)
	values_cache = {} if store_offset else None
	internal = OrderedDict()
	replayed, state = replay_part(
		path, delimiter, encoding=encoding, store=internal,
		store_offset=store_offset, values_cache=values_cache,
	)
	store.clear()
	if store_offset:
		store.update(replayed)
		_attach_replay_meta(store, state, values_cache)
	else:
		for key, entry in replayed.items():
			store[key] = list(entry.row) if isinstance(entry, StoreEntry) else entry
		_attach_replay_meta(store, state)
	return store


def _coerce_row(row, delimiter):
	"""Normalize a row argument to a list of strings.

	A string argument is split on ``delimiter``. Sequence elements are
	converted with ``str``; falsy elements become empty strings.

	Args:
		row: String or sequence of field values.
		delimiter: Field delimiter used when splitting a string ``row``.

	Returns:
		list[str]: Normalized field list.

	Examples:
		>>> _coerce_row('a\\tb\\tc', '\\t')
		['a', 'b', 'c']
		>>> _coerce_row(['a', 1, None], '\\t')
		['a', '1', '']
	"""
	if isinstance(row, str):
		return row.split(delimiter)
	return [str(c).rstrip() if c else '' for c in row]


def append_records(path, rows, *, create=False, encoding='utf8', delimiter=None,
				   header=None):
	"""Append data and tombstone rows to a part file.

	Each item in ``rows`` may be a sequence of fields or, when ``rows`` is a
	mapping, a ``(key, value)`` pair. A row consisting of a single field
	(the key alone) is written as a tombstone (specification §9).

	Args:
		path: Filesystem path of the part.
		rows: Iterable of rows, or a mapping of key→row.
		create: If True, create a missing part before appending.
		encoding: Text encoding for written lines.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		header: Optional header columns written when creating a new part.

	Returns:
		None: Rows are appended to ``path`` as a side effect.

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd); os.unlink(path)
		>>> append_records(path, [['k', 'v'], ['k', '']], create=True)  # live empty value
		>>> open(path).read()
		'k\\tv\\nk\\t\\n'
		>>> append_records(path, [['k']])  # tombstone
		>>> dict(read_store(path))
		{}
		>>> os.unlink(path)
	"""
	delimiter = delimiter or delimiter_for_path(path)
	ensure_part_exists(
		path, create=create, encoding=encoding, delimiter=delimiter, header=header,
	)
	lines = []
	if isinstance(rows, dict):
		items = [(k, rows[k]) for k in rows]
	else:
		items = [(None, r) for r in rows]
	for key, row in items:
		row = _coerce_row(row, delimiter)
		if key is not None and (not row or row[0] != key):
			row = [key] + list(row)
		if not row:
			continue
		lines.append(format_tombstone(row[0], delimiter) if len(row) == 1
					 else format_data_row(row, delimiter))
	if not lines:
		return
	with open_part(path, 'ab', encoding=encoding) as f:
		f.write(('\n'.join(lines) + '\n').encode(encoding, errors='replace'))


def append_record(path, row, **kwargs):
	"""Append a single row to a part file.

	Equivalent to :func:`append_records` with a one-element sequence.
	Keyword arguments are forwarded unchanged.

	Args:
		path: Filesystem path of the part.
		row: Single row to append (lone key = tombstone).
		**kwargs: Forwarded to :func:`append_records`.

	Returns:
		None: The row is appended to ``path`` as a side effect.
	"""
	append_records(path, [row], **kwargs)


def truncate_part(path, *, encoding='utf8', delimiter=None, header=None, defaults=None):
	"""Replace part contents with an optional header comment and defaults marker.

	The file is created if it does not already exist. Existing data rows,
	tombstones, and non-header comments are discarded.

	Args:
		path: Filesystem path of the part.
		encoding: Text encoding for written lines.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		header: Optional column names written as a ``#`` comment.
		defaults: Optional value-column defaults written as a marker.

	Returns:
		None: The part file is rewritten as a side effect.
	"""
	delimiter = delimiter or delimiter_for_path(path)
	header = _parse_columns(header, delimiter)
	defaults = _normalize_defaults(defaults)
	ensure_part_exists(path, create=True, encoding=encoding, delimiter=delimiter)
	with open_part(path, 'wb', encoding=encoding) as f:
		if header:
			f.write(format_header_comment(header, delimiter).encode(encoding, errors='replace') + b'\n')
		if defaults:
			line = format_marker_line(MARKER_DEFAULTS, defaults, delimiter)
			f.write(line.encode(encoding, errors='replace') + b'\n')


def snapshot_part(path, *, encoding='utf8', delimiter=None, header=None, store=None):
	"""Materialize live state into a single part (simplified specification §19).

	Rewrites ``path`` in place: superseded values, tombstones, and non-header
	comments are dropped. The resulting file begins with a ``#_version_#``
	preamble (and any non-default markers) followed by live rows in
	first-appearance order.

	Args:
		path: Filesystem path of the part to compact.
		encoding: Text encoding for read/write.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		header: Optional header retained when truncating before rewrite.
		store: Optional mapping to populate during the pre-snapshot read.

	Returns:
		MutableMapping: Live key→row mapping that was written.

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd); os.unlink(path)
		>>> append_records(path, [['a', '1'], ['a', '2'], ['b', 'x'], ['b']], create=True)
		>>> dict(snapshot_part(path))
		{'a': ['a', '2']}
		>>> '#_version_#' in open(path).read()
		True
		>>> os.unlink(path)
	"""
	data = read_store(path, encoding=encoding, delimiter=delimiter, store=store)
	if not data:
		return data
	delimiter = delimiter or delimiter_for_path(path)
	state = getattr(data, '_reader_state', ReaderState())
	snap = ReaderState()
	snap.defaults = list(state.defaults)
	snap.return_on_missing = state.return_on_missing
	lines = build_snapshot_preamble(snap, delimiter)
	for key, row in data.items():
		if str(key).startswith('#'):
			continue
		if isinstance(row, list) and row:
			lines.append(format_data_row(row, delimiter))
	truncate_part(path, encoding=encoding, delimiter=delimiter, header=header)
	with open_part(path, 'ab', encoding=encoding) as f:
		f.write(('\n'.join(lines) + '\n').encode(encoding, errors='replace'))
	return data


# ---------------------------------------------------------------------------
# WalStore — in-memory store + async append-only writer (§18)
# ---------------------------------------------------------------------------

class WalStore(OrderedDict):
	"""Ordered key→row store backed by an append-only part file.

	Mutations are enqueued for a background flusher; :meth:`flush` and
	:meth:`close` drain the pending queue to disk. Deletion appends a
	tombstone (specification §9). Prefer :meth:`pop`, :meth:`popitem`, or
	``del`` for removals — all persist to the WAL.

	Args:
		path: Filesystem path of the backing part.
		header: Optional column names written when creating the part.
		create: If True, create a missing part on open.
		encoding: Text encoding for append I/O.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		defaults: Optional value-column defaults.
		flush_interval: Seconds between background flush attempts.

	Examples:
		>>> import os, tempfile, time
		>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd); os.unlink(path)
		>>> db = WalStore(path, header=['id', 'name'], create=True, flush_interval=0.01)
		>>> db['alice'] = ['Alice']
		>>> db['bob'] = ['Bob']
		>>> del db['bob']
		>>> _ = db.flush(); time.sleep(0.05)
		>>> db['alice']
		['alice', 'Alice']
		>>> 'bob' in db
		False
		>>> _ = db.close(); os.unlink(path)
	"""

	def __init__(self, path, *, header=None, create=True, encoding='utf8',
				 delimiter=None, defaults=None, flush_interval=0.01):
		super().__init__()
		self.path = path
		self.encoding = encoding
		self.delimiter = delimiter or delimiter_for_path(path)
		self.header = _parse_columns(header, self.delimiter)
		self.create = create
		self._pending = deque()
		self._lock = threading.Lock()
		self._shutdown = threading.Event()
		self._reader_state = ReaderState()
		self._defaults_row = [MARKER_DEFAULTS]
		self.set_defaults(defaults)
		self.flush_interval = flush_interval
		self._worker = threading.Thread(target=self._flush_worker, daemon=True)
		self._worker.start()
		self.reload()
		atexit.register(self.close)

	def set_defaults(self, defaults):
		"""Set value-column defaults (excluding the ``#_defaults_#`` key field).

		Args:
			defaults: Value-column defaults, or a legacy row beginning with
				``#_defaults_#``.

		Returns:
			None: Defaults are stored on ``self``.
		"""
		vals = _normalize_defaults(defaults)
		self._defaults_row = [MARKER_DEFAULTS] + vals if vals else [MARKER_DEFAULTS]
		self._reader_state.defaults = list(vals)

	@property
	def defaults(self):
		"""Return the defaults row with ``#_defaults_#`` as field 0.

		Returns:
			list: Copy of the defaults row including the marker key.
		"""
		return list(self._defaults_row)

	def reload(self):
		"""Discard in-memory state and replay the part from disk.

		Pending writes that have not yet been flushed are preserved across
		the reload.

		Returns:
			WalStore: ``self``, after replay.
		"""
		prev = self._pending
		self._pending = deque()
		super().clear()
		try:
			read_store(
				self.path, create=self.create, encoding=self.encoding,
				delimiter=self.delimiter, store=self, header=self.header or None,
			)
		except FileNotFoundError:
			if self.create:
				raise
		self._reader_state = getattr(self, '_reader_state', self._reader_state)
		self._pending = prev
		return self

	def __getitem__(self, key):
		key = str(key).rstrip()
		try:
			return super().__getitem__(key)
		except KeyError:
			if self._reader_state.return_on_missing:
				return resolve_missing_key(key, self._reader_state)
			raise

	def __setitem__(self, key, value):
		key = str(key).rstrip()
		if not key:
			return
		value = _coerce_row(value, self.delimiter)
		if not value or value[0] != key:
			value = [key] + list(value)
		if len(value) == 1:
			del self[key]
			return
		if key == MARKER_DEFAULTS:
			self.set_defaults(value[1:])
			self._pending.append(list(self._defaults_row))
			return
		super().__setitem__(key, value)
		if key.startswith('#'):
			return
		self._pending.append(list(value))

	def __delitem__(self, key):
		key = str(key).rstrip()
		if key == MARKER_DEFAULTS:
			self.set_defaults([])
			self._pending.append([MARKER_DEFAULTS])
			return
		if key not in self:
			return
		super().__delitem__(key)
		if key.startswith('#'):
			return
		self._pending.append((_TOMBSTONE, key))

	def pop(self, key, *args):
		"""Remove ``key`` and persist a tombstone.

		Overrides the C :meth:`OrderedDict.pop` implementation, which does
		not invoke :meth:`__delitem__`.

		Args:
			key: Key to remove.
			*args: Optional default returned when ``key`` is absent.

		Returns:
			list: Removed row, or the provided default.
		"""
		key = str(key).rstrip()
		if key in self:
			value = self[key]
			del self[key]
			return value
		if args:
			return args[0]
		raise KeyError(key)

	def popitem(self, last=True):
		"""Remove and return a ``(key, value)`` pair, persisting a tombstone.

		Overrides the C :meth:`OrderedDict.popitem` implementation, which
		does not invoke :meth:`__delitem__`.

		Args:
			last: If True, pop the most recently inserted item.

		Returns:
			tuple: ``(key, value)`` of the removed item.
		"""
		if not self:
			raise KeyError('dictionary is empty')
		key = next(reversed(self)) if last else next(iter(self))
		value = self[key]
		del self[key]
		return key, value

	def clear(self):
		"""Clear in-memory state and truncate the part on disk.

		The optional header comment and defaults marker are retained.

		Returns:
			WalStore: ``self``, after truncation.
		"""
		self._pending.clear()
		super().clear()
		truncate_part(self.path, encoding=self.encoding, delimiter=self.delimiter,
					  header=self.header, defaults=self._reader_state.defaults)
		return self

	def flush(self):
		"""Write all pending append and tombstone lines under an exclusive lock.

		Returns:
			WalStore: ``self``, after draining the pending queue.
		"""
		if not self._pending:
			return self
		try:
			with self._open_locked('ab') as f:
				buf = io.BufferedWriter(f, buffer_size=65536)
				while self._pending:
					buf.write(_queue_item_to_bytes(self._pending.popleft(), self.delimiter, self.encoding))
				buf.flush()
		except OSError:
			self._pending.clear()
		return self

	def close(self):
		"""Stop the background flush worker and drain the pending queue.

		Idempotent if already closed.

		Returns:
			WalStore: ``self``.
		"""
		if self._shutdown.is_set():
			return self
		self._shutdown.set()
		self._worker.join()
		return self

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		self.close()

	def __del__(self):
		with contextlib.suppress(AttributeError, RuntimeError, OSError, TypeError):
			self.close()

	def _flush_worker(self):
		while not self._shutdown.is_set():
			self.flush()
			time.sleep(self.flush_interval)
		self.flush()

	def _open_locked(self, mode):
		self._lock.acquire()
		f = open_part(self.path, mode, encoding=self.encoding)
		if os.name == 'posix':
			fcntl.lockf(f, fcntl.LOCK_EX)
		elif os.name == 'nt':
			msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 2147483647)
		return _LockedPart(f, self._lock)


class _LockedPart:
	"""File wrapper that releases its lock and closes the file on exit.

	Args:
		file_obj: Open part file handle.
		lock: Threading lock held for the duration of the context.
	"""

	def __init__(self, file_obj, lock):
		self._file = file_obj
		self._lock = lock

	def write(self, data):
		return self._file.write(data)

	def flush(self):
		return self._file.flush()

	def __enter__(self):
		return self._file

	def __exit__(self, *exc):
		try:
			self._file.flush()
			os.fsync(self._file.fileno())
		except OSError:
			pass
		if not self._file.closed:
			if os.name == 'posix':
				fcntl.lockf(self._file, fcntl.LOCK_UN)
			elif os.name == 'nt':
				try:
					msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 2147483647)
				except OSError:
					pass
			self._file.close()
		if self._lock.locked():
			self._lock.release()


# ---------------------------------------------------------------------------
# OffsetStore — key→offset index, synchronous append (§18 single-process)
# ---------------------------------------------------------------------------

class OffsetStore(MutableMapping):
	"""Key→byte-offset index with on-demand value materialization.

	Uses less memory than :class:`WalStore` because row contents are read
	from disk when accessed. Writes are synchronous. Intended for
	single-process use (specification §18).

	Args:
		path: Filesystem path of the backing part.
		header: Optional column names written when creating the part.
		create: If True, create a missing part on open.
		encoding: Text encoding for append I/O.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		defaults: Optional value-column defaults.
	"""

	def __init__(self, path, *, header=None, create=True, encoding='utf8',
				 delimiter=None, defaults=None):
		self.path = path
		self.encoding = encoding
		self.delimiter = delimiter or delimiter_for_path(path)
		self.header = _parse_columns(header, self.delimiter)
		self.create = create
		self._reader_state = ReaderState()
		self._values = {}
		self._offsets = {}
		self.set_defaults(defaults)
		try:
			ensure_part_exists(self.path, create=self.create, encoding=self.encoding,
							   delimiter=self.delimiter, header=self.header)
		except FileNotFoundError:
			if self.create:
				raise
			# create=False and missing: empty in-memory index, no real file handle.
			self._file = open(os.devnull, 'r+b')  # noqa: SIM115
			atexit.register(self.close)
			return
		# Long-lived handle; closed via close()/__exit__/atexit.
		self._file = open(self.path, 'r+b')  # noqa: SIM115
		self.reload()
		atexit.register(self.close)

	def set_defaults(self, defaults):
		vals = _normalize_defaults(defaults)
		self._defaults_row = [MARKER_DEFAULTS] + vals if vals else [MARKER_DEFAULTS]
		self._reader_state.defaults = list(vals)

	@property
	def defaults(self):
		return list(self._defaults_row)

	def reload(self):
		self._offsets.clear()
		self._values.clear()
		loaded = OrderedDict()
		read_store(self.path, create=self.create, encoding=self.encoding,
				   delimiter=self.delimiter, store=loaded, store_offset=True)
		self._offsets.update(loaded)
		self._values.update(getattr(loaded, '_values_cache', {}))
		self._reader_state = getattr(loaded, '_reader_state', self._reader_state)
		return self

	def _append_line(self, line):
		self._file.seek(0, os.SEEK_END)
		pos = self._file.tell()
		self._file.write(line.encode(self.encoding, errors='replace') + b'\n')
		return pos

	def _write_row(self, fields):
		if len(fields) == 1:
			line = format_tombstone(fields[0], self.delimiter)
		elif fields[0] == MARKER_DEFAULTS:
			line = format_marker_line(MARKER_DEFAULTS, fields[1:], self.delimiter)
		else:
			line = format_data_row(fields, self.delimiter)
		return self._append_line(line)

	def _read_at(self, offset, key=None):
		if key is not None and key in self._values:
			return list(self._values[key])
		self._file.seek(offset)
		line = self._file.readline().decode(self.encoding, errors='replace').rstrip('\r\n')
		scratch = OrderedDict()
		kind, entry = process_record(line, self._reader_state.copy(), scratch, self.delimiter)
		if kind == 'data' and entry is not None:
			return list(entry.row)
		if key is not None:
			raise KeyError(key)
		return []

	def __getitem__(self, key):
		key = str(key).rstrip()
		if key == MARKER_DEFAULTS:
			return self.defaults
		if key not in self._offsets:
			if self._reader_state.return_on_missing:
				return resolve_missing_key(key, self._reader_state)
			raise KeyError(key)
		return self._read_at(self._offsets[key], key)

	def __setitem__(self, key, value):
		key = str(key).rstrip()
		if not key:
			return
		value = _coerce_row(value, self.delimiter)
		if not value or value[0] != key:
			value = [key] + list(value)
		if len(value) == 1:
			del self[key]
			return
		if key == MARKER_DEFAULTS:
			self.set_defaults(value[1:])
			return
		if key.startswith('#'):
			self._offsets[key] = value
			self._values[key] = list(value)
			return
		pos = self._write_row(value)
		self._offsets[key] = pos
		self._values[key] = list(value)

	def __delitem__(self, key):
		key = str(key).rstrip()
		if key == MARKER_DEFAULTS:
			self.set_defaults([])
			self._write_row([MARKER_DEFAULTS])
			return
		if key not in self._offsets:
			return
		self._offsets.pop(key, None)
		self._values.pop(key, None)
		if not key.startswith('#'):
			self._write_row([key])

	def pop(self, key, *args):
		"""Remove ``key`` and persist a tombstone to the part file.

		Args:
			key: Key to remove.
			*args: Optional default returned when ``key`` is absent.

		Returns:
			list: Removed row, or the provided default.
		"""
		key = str(key).rstrip()
		if key in self._offsets:
			value = self[key]
			del self[key]
			return value
		if args:
			return args[0]
		raise KeyError(key)

	def popitem(self, last=True):
		"""Remove and return a ``(key, value)`` pair, persisting a tombstone.

		Args:
			last: If True, pop the most recently inserted item.

		Returns:
			tuple: ``(key, value)`` of the removed item.
		"""
		if not self._offsets:
			raise KeyError('dictionary is empty')
		key = next(reversed(self._offsets)) if last else next(iter(self._offsets))
		value = self[key]
		del self[key]
		return key, value

	def __iter__(self):
		return iter(self._offsets)

	def __len__(self):
		return len(self._offsets)

	def __contains__(self, key):
		return str(key).rstrip() in self._offsets

	def clear(self):
		self._offsets.clear()
		self._values.clear()
		self._file.seek(0)
		self._file.truncate()
		if self.header:
			self._append_line(format_header_comment(self.header, self.delimiter))
		return self

	def close(self):
		if not self._file.closed:
			self._file.close()
		return self

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		self.close()


# ---------------------------------------------------------------------------
# Legacy API (TSVZ.py / pre-4.0 TSVZ_new names and kwargs)
# ---------------------------------------------------------------------------

DEFAULTS_INDICATOR_KEY = MARKER_DEFAULTS
build_scrub_preamble = build_snapshot_preamble
openFileAsCompressed = open_part


def _legacy_delimiter(delimiter=..., file_name='', path=''):
	"""Map legacy ``get_delimiter`` calling conventions to a concrete delimiter.

	Accepts symbolic names (``'comma'``, ``'tab'``, ``'pipe'``, ``'null'``),
	unicode-escape sequences, or an ellipsis sentinel that triggers path-based
	inference via :func:`delimiter_for_path`.

	Args:
		delimiter: Explicit delimiter, symbolic name, escape sequence, or
			``...`` to infer from the file name.
		file_name: Path used for inference when ``delimiter`` is ``...``.
		path: Alternate path argument (used when ``file_name`` is empty).

	Returns:
		str: Concrete field delimiter.

	Examples:
		>>> _legacy_delimiter(delimiter='comma')
		','
		>>> _legacy_delimiter(delimiter=..., file_name='x.csv')
		','
		>>> _legacy_delimiter(delimiter='\\\\t')
		'\\t'
	"""
	name = file_name or path
	if delimiter is ...:
		return delimiter_for_path(name) if name else DEFAULT_DELIMITER
	if not delimiter:
		return DEFAULT_DELIMITER
	if delimiter == 'comma':
		return ','
	if delimiter == 'tab':
		return '\t'
	if delimiter == 'pipe':
		return '|'
	if delimiter == 'null':
		return '\0'
	if isinstance(delimiter, str):
		try:
			return delimiter.encode().decode('unicode_escape')
		except UnicodeError:
			return delimiter
	return delimiter

def get_delimiter(delimiter=..., file_name=''):
	"""Legacy alias for :func:`_legacy_delimiter` / :func:`delimiter_for_path`.

	Args:
		delimiter: Explicit delimiter, symbolic name, or ``...`` to infer.
		file_name: Path used for inference when ``delimiter`` is ``...``.

	Returns:
		str: Concrete field delimiter.
	"""
	return _legacy_delimiter(delimiter=delimiter, file_name=file_name)

def read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=False, teeLogger=None,
						 strict=False, encoding='utf8', delimiter=..., defaults=...,
						 storeOffset=False):
	"""Legacy alias for :func:`read_last_record`.

	Unused parameters are accepted for signature compatibility and ignored.

	Args:
		fileName: Path of the part file.
		taskDic: Unused; retained for compatibility.
		correctColumnNum: Unused; retained for compatibility.
		verbose: Unused; retained for compatibility.
		teeLogger: Unused; retained for compatibility.
		strict: Unused; retained for compatibility.
		encoding: Text encoding forwarded to :func:`read_last_record`.
		delimiter: Legacy delimiter argument.
		defaults: Unused; retained for compatibility.
		storeOffset: If True, return the byte offset of the last data row.

	Returns:
		list | int: Last data row or its byte offset.
	"""
	_ = (taskDic, correctColumnNum, verbose, teeLogger, strict, defaults)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	return read_last_record(
		fileName, encoding=encoding, delimiter=d, store_offset=storeOffset,
	)

def readTabularFile(fileName, teeLogger=None, header='', createIfNotExist=False,
					lastLineOnly=False, verifyHeader=True, verbose=False, taskDic=None,
					encoding='utf8', strict=True, delimiter=..., defaults=...,
					correctColumnNum=-1, storeOffset=False):
	"""Legacy reader that pads or trims rows to a fixed column width.

	When ``header`` or ``correctColumnNum`` specifies a schema width, each
	returned row is adjusted to that width via :func:`_fit_row`. Prefer
	:func:`read_store` in new code.

	Args:
		fileName: Path of the part file.
		teeLogger: Unused; retained for compatibility.
		header: Optional column names / schema width hint.
		createIfNotExist: If True, create a missing part before reading.
		lastLineOnly: If True, return only the last committed data row.
		verifyHeader: Unused; retained for compatibility.
		verbose: Unused; retained for compatibility.
		taskDic: Optional mapping to populate.
		encoding: Text encoding used to decode lines.
		strict: If True, propagate ``FileNotFoundError`` for missing files.
		delimiter: Legacy delimiter argument.
		defaults: Optional defaults for create / missing-key behaviour.
		correctColumnNum: Explicit schema width; ``-1`` means unset.
		storeOffset: If True, map keys to byte offsets instead of rows.

	Returns:
		MutableMapping | list | int: Live store, last row, or last offset.

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsv'); os.close(fd)
		>>> open(path, 'w').write('a\\t1\\n')
		4
		>>> dict(readTabularFile(path, header=['id', 'v', 'extra'], strict=False))
		{'a': ['a', '1', '']}
		>>> os.unlink(path)
	"""
	_ = (teeLogger, verifyHeader, verbose, strict)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	store = taskDic if taskDic is not None else OrderedDict()
	header_cols = _parse_columns(header, d) if header else []
	cols = correctColumnNum
	if (cols is None or cols < 0) and header_cols:
		cols = len(header_cols)
	try:
		result = read_store(
			fileName, create=createIfNotExist, encoding=encoding, delimiter=d,
			defaults=defaults if defaults is not ... else None,
			store=store, store_offset=storeOffset, last_record_only=lastLineOnly,
			header=header_cols or None,
		)
	except FileNotFoundError:
		if strict and not createIfNotExist:
			raise
		return (-1 if storeOffset else []) if lastLineOnly else store
	if lastLineOnly:
		if isinstance(result, list) and cols and cols > 0:
			return _fit_row(result, cols)
		return result
	if cols and cols > 0 and not storeOffset:
		for key in list(result.keys()):
			result[key] = _fit_row(result[key], cols)
	elif cols and cols > 0 and storeOffset and hasattr(result, '_values_cache'):
		for key, row in list(result._values_cache.items()):
			result._values_cache[key] = _fit_row(row, cols)
	try:
		if hasattr(store, '_reader_state'):
			store._tsvz_reader_state = store._reader_state
		if storeOffset and hasattr(store, '_values_cache'):
			store._tsvz_values_cache = store._values_cache
	except AttributeError:
		pass
	return result

def appendLinesTabularFile(fileName, linesToAppend, teeLogger=None, header='',
						   createIfNotExist=False, verifyHeader=True, verbose=False,
						   encoding='utf8', strict=True, delimiter=...):
	"""Legacy multi-row append; delegates to :func:`append_records`.

	Args:
		fileName: Path of the part file.
		linesToAppend: Rows to append.
		teeLogger: Unused; retained for compatibility.
		header: Optional header written when creating the part.
		createIfNotExist: If True, create a missing part before appending.
		verifyHeader: Unused; retained for compatibility.
		verbose: Unused; retained for compatibility.
		encoding: Text encoding for written lines.
		strict: Unused; retained for compatibility.
		delimiter: Legacy delimiter argument.

	Returns:
		None: Rows are appended as a side effect.
	"""
	_ = (teeLogger, verifyHeader, verbose, strict)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	append_records(
		fileName, linesToAppend, create=createIfNotExist, encoding=encoding,
		delimiter=d, header=header or None,
	)

def appendTabularFile(fileName, lineToAppend, teeLogger=None, header='',
					  createIfNotExist=False, verifyHeader=True, verbose=False,
					  encoding='utf8', strict=True, delimiter=...):
	"""Legacy single-row append.

	A lone-key row is written as a tombstone (specification §9).

	Args:
		fileName: Path of the part file.
		lineToAppend: Single row to append.
		teeLogger: Unused; retained for compatibility.
		header: Optional header written when creating the part.
		createIfNotExist: If True, create a missing part before appending.
		verifyHeader: Unused; retained for compatibility.
		verbose: Unused; retained for compatibility.
		encoding: Text encoding for written lines.
		strict: Unused; retained for compatibility.
		delimiter: Legacy delimiter argument.

	Returns:
		None: The row is appended as a side effect.
	"""
	appendLinesTabularFile(
		fileName, [lineToAppend], teeLogger=teeLogger, header=header,
		createIfNotExist=createIfNotExist, verifyHeader=verifyHeader,
		verbose=verbose, encoding=encoding, strict=strict, delimiter=delimiter,
	)

def clearTabularFile(fileName, teeLogger=None, header='', verifyHeader=False, verbose=False,
					 encoding='utf8', strict=False, delimiter=..., defaults=...):
	"""Legacy truncate; creates the file with an optional ``#`` header comment.

	Args:
		fileName: Path of the part file.
		teeLogger: Unused; retained for compatibility.
		header: Optional header written after truncation.
		verifyHeader: Unused; retained for compatibility.
		verbose: Unused; retained for compatibility.
		encoding: Text encoding for written lines.
		strict: Unused; retained for compatibility.
		delimiter: Legacy delimiter argument.
		defaults: Optional defaults marker written after truncation.

	Returns:
		None: The part is truncated as a side effect.
	"""
	_ = (teeLogger, verifyHeader, verbose, strict)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	defs = _normalize_defaults(defaults if defaults is not ... else None)
	truncate_part(fileName, encoding=encoding, delimiter=d, header=header, defaults=defs)

def scrubTabularFile(fileName, teeLogger=None, header='', createIfNotExist=False,
					 lastLineOnly=False, verifyHeader=True, verbose=False, taskDic=None,
					 encoding='utf8', strict=False, delimiter=..., defaults=...,
					 correctColumnNum=-1):
	"""Legacy compact operation; delegates to :func:`snapshot_part`.

	When ``lastLineOnly`` is True, returns the last data row instead of
	compacting (equivalent to a last-line :func:`readTabularFile` call).

	Args:
		fileName: Path of the part file.
		teeLogger: Unused; retained for compatibility.
		header: Optional header retained across compaction.
		createIfNotExist: Unused; retained for compatibility.
		lastLineOnly: If True, return the last data row instead of compacting.
		verifyHeader: Unused; retained for compatibility.
		verbose: Unused; retained for compatibility.
		taskDic: Optional mapping forwarded to :func:`snapshot_part`.
		encoding: Text encoding for read/write.
		strict: Unused; retained for compatibility.
		delimiter: Legacy delimiter argument.
		defaults: Defaults used when ``lastLineOnly`` reads the file.
		correctColumnNum: Unused; retained for compatibility.

	Returns:
		MutableMapping | list: Compacted store, or the last data row.
	"""
	_ = (teeLogger, createIfNotExist, lastLineOnly, verifyHeader, verbose, strict, correctColumnNum)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	if lastLineOnly:
		return readTabularFile(
			fileName, header=header, encoding=encoding, delimiter=d,
			defaults=defaults, taskDic=taskDic, lastLineOnly=True,
		)
	return snapshot_part(
		fileName, encoding=encoding, delimiter=d, header=header or None, store=taskDic,
	)

def getListView(tsvzDic, header=None, delimiter=...):
	"""Return store contents as a list of row lists.

	When ``header`` is provided and does not already appear as the first row,
	it is prepended to the result.

	Args:
		tsvzDic: Mapping of key→row values.
		header: Optional header row to prepend.
		delimiter: Delimiter used when splitting a string ``header``.

	Returns:
		list: Rows as lists, optionally prefixed with ``header``.

	Examples:
		>>> getListView({'a': ['a', '1']}, header=['id', 'v'])
		[['id', 'v'], ['a', '1']]
		>>> getListView({})
		[]
	"""
	if header is None:
		header = []
	d = get_delimiter(delimiter=delimiter)
	if header:
		if isinstance(header, str):
			header = header.split(d)
		elif not isinstance(header, list):
			try:
				header = list(header)
			except TypeError:
				header = []
	if not tsvzDic:
		return [header] if header else []
	if not header:
		return [list(v) if isinstance(v, (list, tuple)) else v for v in tsvzDic.values()]
	values = [list(v) if isinstance(v, (list, tuple)) else v for v in tsvzDic.values()]
	if values and values[0] == header:
		return values
	return [header] + values

readTSV = readTabularFile
appendTSV = appendTabularFile
clearTSV = clearTabularFile
scrubTSV = scrubTabularFile

class TSVZed(WalStore):
	"""Legacy wrapper around :class:`WalStore`.

	Provides an append-only WAL with optional periodic snapshots.
	``rewrite_on_load``, ``rewrite_on_exit``, and ``rewrite_interval`` map to
	:meth:`hardMapToFile` (which calls :func:`snapshot_part`). Prefer
	invoking ``hardMapToFile()`` explicitly in new code.

	Args:
		fileName: Path of the backing part.
		teeLogger: Optional logger retained for compatibility.
		header: Optional column names.
		createIfNotExist: If True, create a missing part on open.
		verifyHeader: Unused; retained for compatibility.
		rewrite_on_load: If True, compact once after loading.
		rewrite_on_exit: If True, compact during :meth:`close`.
		rewrite_interval: Seconds between automatic compact attempts.
		append_check_delay: Background flush interval in seconds.
		monitor_external_changes: Unused; retained for compatibility.
		verbose: Unused; retained for compatibility.
		encoding: Text encoding for append I/O.
		delimiter: Legacy delimiter argument.
		defaults: Optional value-column defaults.
		strict: Unused; retained for compatibility.
		correctColumnNum: Unused; retained for compatibility.
	"""

	def __init__(self, fileName, teeLogger=None, header='', createIfNotExist=True,
				 verifyHeader=True, rewrite_on_load=False, rewrite_on_exit=False,
				 rewrite_interval=0, append_check_delay=0.01, monitor_external_changes=True,
				 verbose=False, encoding='utf8', delimiter=..., defaults=None,
				 strict=False, correctColumnNum=-1):
		_ = (verifyHeader, monitor_external_changes, verbose, strict, correctColumnNum)
		d = None if delimiter is ... else _legacy_delimiter(delimiter=delimiter, file_name=fileName)
		self._fileName = fileName
		self.teeLogger = teeLogger
		self.verifyHeader = verifyHeader
		self.verbose = verbose
		self.strict = strict
		self.correctColumnNum = correctColumnNum
		self.rewrite_on_load = rewrite_on_load
		self.rewrite_on_exit = rewrite_on_exit
		self.rewrite_interval = float(rewrite_interval or 0)
		self._last_rewrite = time.monotonic()
		self._rewriting = False
		super().__init__(
			fileName, header=header or None, create=createIfNotExist,
			encoding=encoding, delimiter=d, defaults=defaults,
			flush_interval=append_check_delay,
		)
		self.appendQueue = self._pending
		if self.rewrite_on_load and os.path.isfile(self.path):
			self.hardMapToFile()

	def commitAppendToFile(self):
		"""Legacy alias for :meth:`WalStore.flush`.

		Returns:
			TSVZed: ``self``, after flush.
		"""
		return self.flush()

	def stopAppendThread(self):
		"""Legacy alias for :meth:`WalStore.close`.

		Returns:
			TSVZed: ``self``, after close.
		"""
		return self.close()

	def clear_file(self):
		"""Truncate the part while retaining the header and defaults markers.

		Returns:
			TSVZed: ``self``, after truncation.
		"""
		truncate_part(
			self.path, encoding=self.encoding, delimiter=self.delimiter,
			header=self.header, defaults=self._reader_state.defaults,
		)
		return self

	def getListView(self):
		return getListView(self, header=self.header, delimiter=self.delimiter)

	def load(self):
		return self.reload()

	def rewrite(self, *args, **kwargs):
		"""Legacy alias for :meth:`hardMapToFile`.

		Args:
			*args: Ignored; retained for compatibility.
			**kwargs: Ignored; retained for compatibility.

		Returns:
			bool: Result of :meth:`hardMapToFile`.
		"""
		_ = (args, kwargs)
		return self.hardMapToFile()

	def hardMapToFile(self):
		"""Compact the part via :func:`snapshot_part` and refresh memory state.

		Returns:
			bool: True on success, or False if a rewrite is already in
			progress.
		"""
		if self._rewriting:
			return False
		self._rewriting = True
		try:
			WalStore.flush(self)
			data = snapshot_part(
				self.path, encoding=self.encoding, delimiter=self.delimiter,
				header=self.header or None,
			)
			self._last_rewrite = time.monotonic()
			prev = self._pending
			self._pending = deque()
			super(WalStore, self).clear()
			if data:
				self.update(data)
				self._reader_state = getattr(data, '_reader_state', self._reader_state)
			self._pending = prev
			return True
		finally:
			self._rewriting = False

	mapToFile = hardMapToFile

	def checkExternalChanges(self):
		"""No-op retained for API compatibility.

		Returns:
			TSVZed: ``self``.
		"""
		return self

	def flush(self):
		WalStore.flush(self)
		if (not self._rewriting and self.rewrite_interval > 0
				and time.monotonic() - self._last_rewrite >= self.rewrite_interval):
			self.hardMapToFile()
		return self

	def close(self):
		if self._shutdown.is_set():
			return self
		if self.rewrite_on_exit:
			with contextlib.suppress(OSError, ValueError, TypeError):
				self.hardMapToFile()
		return WalStore.close(self)


class TSVZedLite(OffsetStore):
	"""Legacy wrapper around :class:`OffsetStore`.

	Args:
		fileName: Path of the backing part.
		header: Optional column names.
		createIfNotExist: If True, create a missing part on open.
		verifyHeader: Unused; retained for compatibility.
		verbose: Unused; retained for compatibility.
		encoding: Text encoding for append I/O.
		delimiter: Legacy delimiter argument.
		defaults: Optional value-column defaults.
		strict: Unused; retained for compatibility.
		correctColumnNum: Unused; retained for compatibility.
		indexes: Optional pre-built offset mapping.
		fileObj: Optional open file object to adopt.
	"""

	def __init__(self, fileName, header='', createIfNotExist=True, verifyHeader=True,
				 verbose=False, encoding='utf8', delimiter=..., defaults=None,
				 strict=True, correctColumnNum=-1, indexes=..., fileObj=...):
		_ = (verifyHeader, verbose, strict, correctColumnNum)
		d = None if delimiter is ... else _legacy_delimiter(delimiter=delimiter, file_name=fileName)
		super().__init__(
			fileName, header=header or None, create=createIfNotExist,
			encoding=encoding, delimiter=d, defaults=defaults,
		)
		self._fileName = fileName
		self.verifyHeader = verifyHeader
		self.verbose = verbose
		self.strict = strict
		self.correctColumnNum = correctColumnNum
		self.indexes = self._offsets
		if indexes is not ...:
			self._offsets = indexes
			self.indexes = indexes
		if fileObj is not ...:
			self._file.close()
			self._file = fileObj

	def getListView(self):
		return getListView(self._values, header=self.header, delimiter=self.delimiter)

	def clear_file(self):
		return self.clear()

	def switchFile(self, newFileName, createIfNotExist=..., verifyHeader=...):
		"""Close the current part and reopen ``newFileName`` as the active store.

		Args:
			newFileName: Path of the part to open.
			createIfNotExist: Optional override for the create flag.
			verifyHeader: Optional override retained for compatibility.

		Returns:
			TSVZedLite: ``self``, after switching.
		"""
		self._file.close()
		self.path = newFileName
		self._fileName = newFileName
		if createIfNotExist is not ...:
			self.create = createIfNotExist
		if verifyHeader is not ...:
			self.verifyHeader = verifyHeader
		self.reload()
		self._file = open(self.path, 'r+b')  # noqa: SIM115
		return self


# ---------------------------------------------------------------------------
# CLI (formatting via multiCMD when available; TSVZ stays format-focused)
# ---------------------------------------------------------------------------

version = __version__
author = 'pan@zopyr.us'
COMMIT_DATE = '2026-07-30'


def _cli_pretty_format_table(data, delimiter='\t'):
	"""Format tabular data for CLI display.

	Prefers :mod:`multiCMD` when available; otherwise falls back to a
	minimal delimiter-joined representation suitable for standalone installs.

	Args:
		data: Iterable of rows (lists/tuples) or scalar values.
		delimiter: Field delimiter used in the fallback formatter.

	Returns:
		str: Formatted table text.

	Examples:
		>>> out = _cli_pretty_format_table([['a', '1'], ['b', '2']], delimiter='\\t')
		>>> 'a' in out and '1' in out and 'b' in out
		True
	"""
	try:
		import multiCMD
		return multiCMD.pretty_format_table(data, delimiter=delimiter)
	except Exception:
		rows = list(data) if not isinstance(data, list) else data
		if not rows:
			return ''
		lines = []
		for row in rows:
			if isinstance(row, (list, tuple)):
				lines.append(delimiter.join(str(c) for c in row))
			else:
				lines.append(str(row))
		return '\n'.join(lines) + '\n'


def __main__():
	"""Command-line entry point.

	Supported operations: ``read``, ``append``, ``delete``, ``clear``, and
	``scrub`` (snapshot/compact).

	Returns:
		None: Executes the selected CLI operation.
	"""
	import argparse
	parser = argparse.ArgumentParser(description='TSVZ: append-only tabular key–value store (tsvz-spec-v1)')
	parser.add_argument('filename', type=str, help='The file to read')
	parser.add_argument(
		'operation', type=str, nargs='?', choices=['read', 'append', 'delete', 'clear', 'scrub'],
		help='Operation to perform. scrub = snapshot/compact. Default: read',
		default='read',
	)
	parser.add_argument(
		'line', type=str, nargs='*',
		help='Row fields: {key} {value1} ...; key-only appends a tombstone (delete).',
	)
	parser.add_argument(
		'-d', '--delimiter', type=str, default=...,
		help='Delimiter (inferred from filename when omitted).',
	)
	parser.add_argument('-c', '--header', type=str, help='Header columns separated by --delimiter.')
	parser.add_argument('--defaults', type=str, help='Default column values separated by --delimiter.')
	strict_mode = parser.add_mutually_exclusive_group()
	strict_mode.add_argument('-s', '--strict', dest='strict', action='store_true')
	strict_mode.add_argument('-f', '--force', dest='strict', action='store_false')
	parser.set_defaults(strict=True)
	parser.add_argument('-v', '--verbose', action='store_true')
	parser.add_argument('-V', '--version', action='version',
						version=f'%(prog)s {version} @ {COMMIT_DATE} by {author}')
	try:
		import argcomplete
		argcomplete.autocomplete(parser, always_complete_options='long')
	except ImportError:
		pass
	args = parser.parse_args()
	args.delimiter = get_delimiter(delimiter=args.delimiter, file_name=args.filename)
	header = ''
	if args.header:
		try:
			header = args.header.encode().decode('unicode_escape')
		except Exception:
			header = args.header
	defaults = []
	if args.defaults:
		try:
			defaults = args.defaults.encode().decode('unicode_escape').split(args.delimiter)
		except Exception:
			defaults = args.defaults.split(args.delimiter)

	if args.operation == 'read':
		if not os.path.isfile(args.filename):
			print(f'File not found: {args.filename}')
			return
		data = readTabularFile(
			args.filename, verifyHeader=False, verbose=args.verbose,
			strict=args.strict, delimiter=args.delimiter, defaults=defaults,
		)
		formatted = _cli_pretty_format_table(data.values(), delimiter=args.delimiter)
		print(formatted, end='' if formatted.endswith('\n') else '\n')
	elif args.operation == 'append':
		appendTabularFile(
			args.filename, args.line, createIfNotExist=True, header=header,
			verbose=args.verbose, strict=args.strict, delimiter=args.delimiter,
		)
	elif args.operation == 'delete':
		# Spec §9: tombstone = lone key (no value fields).
		appendTabularFile(
			args.filename, args.line[:1], createIfNotExist=True, header=header,
			verbose=args.verbose, strict=args.strict, delimiter=args.delimiter,
		)
	elif args.operation == 'clear':
		clearTabularFile(
			args.filename, header=header, verbose=args.verbose,
			verifyHeader=args.strict, delimiter=args.delimiter,
		)
	elif args.operation == 'scrub':
		scrubTabularFile(
			args.filename, verifyHeader=False, verbose=args.verbose,
			strict=args.strict, delimiter=args.delimiter, defaults=defaults,
		)
	else:
		print('Invalid operation', file=sys.stderr)


if __name__ == '__main__':
	# ``python TSVZ_new.py --doctest`` or ``python -m doctest TSVZ_new.py``.
	if len(sys.argv) > 1 and sys.argv[1] in ('--doctest', '-t'):
		import doctest
		failures, _ = doctest.testmod(optionflags=doctest.ELLIPSIS)
		sys.exit(1 if failures else 0)
	__main__()
