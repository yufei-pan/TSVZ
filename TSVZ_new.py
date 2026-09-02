#! /usr/bin/env python3
"""
TSVZ reference implementation of tsvz-spec-v1.

This module provides an append-only write-ahead log (WAL) for tabular
key–value data. Compaction is performed by :func:`snapshot_part` (a
simplified form of specification §19), which rewrites a part from live
state rather than mutating historical records in place. Readers follow the
current specification only; legacy escaping and tombstone conventions are
not accepted.

Preferred public entry points for new code are :func:`read_store`,
:func:`read_offsets`, :func:`append_records`, :func:`delete_records`,
:class:`WalStore`, and :func:`snapshot_part`. Legacy names such as
:func:`readTabularFile` and :class:`TSVZed` remain as thin compatibility
wrappers; they emit :class:`DeprecationWarning` and are scheduled for removal
in TSVZ :data:`LEGACY_REMOVAL_VERSION`.

A ``#``-prefixed key is ordinary data at every layer, written escaped as
``<#>key`` per §8.3 -- there is no "in-memory only" key convention.

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
- §14 — defaults, fill-empty, return-on-missing, and absent-column
  resolution (§14.3, via :func:`materialize_row`)
- §15 — opt-in ``#_checksum_<algo>_#`` integrity: arming, marker-to-marker
  segments spanning parts, per-algorithm accumulators, and detection-only
  reporting (:class:`DigestSet`, :func:`verify_part`, :func:`append_checksum`)
- §16 — transparent compression for ``.gz`` / ``.bz2`` / ``.xz`` / ``.zst``
  (``.zst`` requires Python 3.14+; it never degrades to plaintext)
- §17 — multi-part stores: the Appendix A filename grammar, hexadecimal
  ordinal ordering, ``.rotated`` exclusion, UUIDv7 ordinals, and replay of
  the parts as one concatenation (:func:`read_multipart`, ``WalStore(
  multipart=True)``)
- §18 — batched whole-record appends under an exclusive lock (§18.2–§18.3);
  ``#_write_ack_#`` selects per-batch fsync (§18.4)
- §19 — both forms of compaction: :func:`snapshot_part` rewrites a single
  file atomically, and :func:`snapshot_store` performs the full race-free
  procedure (immutable prefix, ordinal slotting between prefix and active
  part, §19.4 compensation, and the §19.5 ``#_rotate_#`` actions)
- Stores — :class:`WalStore` (asynchronous append) and :class:`OffsetStore`
  (key→byte-offset index; uncompressed, single-part)

Not yet implemented
-------------------
- ``blake3`` digests require the optional third-party module; without it the
  marker is inert, exactly as §15.3 prescribes for an unimplemented algorithm
- Byte-offset indexing is single-part: an offset alone cannot address a
  multi-part store, which would need a ``(part, offset)`` pair (§19.7)

Known deviations
----------------
- §19.2.3c says a snapshot contains no comments. Passing ``header=`` to
  :func:`snapshot_part` re-emits the header comment above the preamble; the
  default (``header=None``) is conformant.
- §12.4 gives the boolean marker value form as ``true``/``false``.
  :func:`_parse_bool` also accepts ``yes``/``no``/``on``/``off``/``1``/``0``
  on read; writers only ever emit ``true``/``false``.
- v1 has no ``<CR>`` escape token, so a value whose **last** field ends in a
  literal ``\\r`` is indistinguishable from a CRLF terminator (§4.2) and reads
  back without it.
- §5.2 calls a loose extension (``.tsv``, ``.csv``, ...) a fallback with no
  semantic guarantees. Writers here emit identical bytes for loose and strict
  parts, but warn once when a write to a loose part actually emits an escape
  token or a marker -- the cases a generic reader would misread.
- Invalid UTF-8 is repaired with replacement characters and warned about once
  per read rather than raising; pass ``errors='strict'`` to
  :func:`replay_part` / :func:`replay_bytes` to make it an error instead.
- §19.5 calls its actions advisory and permits a conservative processor to
  downgrade ``delete`` -> ``rename`` -> ``keep``. This implementation takes
  that option: ``delete`` becomes ``rename`` unless the caller passes
  ``allow_delete=True``.
- When ``#_defaults_#`` varied across a compacted span, §19.4 compensation
  bakes each row out to the width the re-emitted marker covers. Every column
  *value* is preserved exactly; a row narrower than the marker gains
  explicitly-empty trailing cells, which §3.6 permits since a reconstructed
  row has no guaranteed width.

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
import functools
import hashlib
import io
import os
import re
import secrets
import sys
import tempfile
import threading
import time
import warnings
import zlib
from collections import OrderedDict, deque, namedtuple
from collections.abc import MutableMapping

if os.name == 'nt':
	import msvcrt
elif os.name == 'posix':
	import fcntl

__version__ = '4.0.0'

DEFAULT_DELIMITER = '\t'
MARKER_DEFAULTS = '#_defaults_#'
MARKER_WRITE_ACK = '#_write_ack_#'
MAX_SPEC_VERSION = 1

#: §18.4 write-acknowledgement modes. ``memory`` (the spec's built-in default)
#: acknowledges once a record is in the batch buffer; ``disk`` only after the
#: batch is fsynced.
WRITE_ACK_MEMORY = 'memory'
WRITE_ACK_DISK = 'disk'
WRITE_ACK_MODES = frozenset({WRITE_ACK_MEMORY, WRITE_ACK_DISK})

MARKER_ROTATE = '#_rotate_#'
#: §19.5 actions for parts superseded by a snapshot. Advisory: a conservative
#: processor MAY downgrade delete -> rename -> keep.
ROTATE_KEEP = 'keep'
ROTATE_RENAME = 'rename'
ROTATE_DELETE = 'delete'
ROTATE_ACTIONS = (ROTATE_KEEP, ROTATE_RENAME, ROTATE_DELETE)

#: Longest a Condition-signalled flusher sleeps when its queue is empty. Only a
#: safety net against a lost wakeup -- writes notify the worker directly.
_IDLE_WAKE_SECONDS = 1.0

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

__all__ = [  # noqa: RUF022  # grouped by concern, which reads better than sorted
	# Reading
	'read_store', 'read_offsets', 'read_last_record', 'read_multipart',
	'replay_bytes', 'replay_part', 'replay_parts', 'process_record',
	'classify_record', 'apply_marker', 'committed_payload',
	'resolve_missing_key', 'materialize_row',
	# Writing
	'append_record', 'append_records', 'delete_record', 'delete_records',
	'truncate_part', 'snapshot_part', 'snapshot_store', 'ensure_part_exists',
	# §15 integrity
	'DigestSet', 'new_digest', 'verify_part', 'append_checksum',
	'checksum_marker_key', 'checksum_algorithm', 'report_corruption',
	'IntegrityError', 'IntegrityWarning', 'CORRUPTION_POLICIES',
	# §17 multi-part / §19.5 rotation
	'parse_part_name', 'part_path', 'new_ordinal', 'store_parts',
	'store_part_paths', 'format_base', 'PartName', 'rotate_parts',
	'SnapshotResult', 'ROTATE_ACTIONS', 'MARKER_ROTATE',
	# Format primitives
	'decode_field', 'encode_field', 'format_data_row', 'format_tombstone',
	'format_marker_line', 'format_header_comment', 'build_snapshot_preamble',
	# Paths and I/O
	'open_part', 'delimiter_for_path', 'is_strict_store', 'is_compressed_path',
	# Stores and state
	'WalStore', 'OffsetStore', 'ReaderState', 'StoreEntry',
	# Constants
	'DEFAULT_DELIMITER', 'MARKER_DEFAULTS', 'MARKER_WRITE_ACK', 'MAX_SPEC_VERSION',
	'OFFICIAL_MARKERS', 'STRICT_EXTENSIONS', 'COMPRESSION_EXTENSIONS',
	'WRITE_ACK_MEMORY', 'WRITE_ACK_DISK', 'WRITE_ACK_MODES',
	'ROTATE_KEEP', 'ROTATE_RENAME', 'ROTATE_DELETE', '__version__',
	# Deprecated legacy surface (removal: see LEGACY_REMOVAL_VERSION)
	'readTabularFile', 'appendTabularFile', 'appendLinesTabularFile',
	'clearTabularFile', 'scrubTabularFile', 'read_last_valid_line', 'get_delimiter',
	'getListView', 'TSVZed', 'TSVZedLite',
	'readTSV', 'appendTSV', 'clearTSV', 'scrubTSV',
	'LEGACY_REMOVAL_VERSION',
]

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
		write_ack (str): ``'memory'`` or ``'disk'`` from ``#_write_ack_#``
			(§18.4). Advisory — it never affects reconstructed data (§12.4.3),
			only when a writer considers a record acknowledged.
		rotate (str): ``'keep'``, ``'rename'``, or ``'delete'`` from
			``#_rotate_#`` (§19.5). Advisory — the recommended disposition of
			parts a snapshot supersedes.
	"""

	__slots__ = ('defaults', 'fill_empty', 'return_on_missing', 'rotate',
				 'strip_trailing', 'version', 'write_ack')

	def __init__(self):
		self.version = 1
		self.defaults = []
		self.strip_trailing = True
		self.fill_empty = False
		self.return_on_missing = True
		self.write_ack = WRITE_ACK_MEMORY
		self.rotate = ROTATE_KEEP

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
		s.write_ack = self.write_ack
		s.rotate = self.rotate
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
	if '<' not in raw:
		# §13.3 forbids a bare '<', so no '<' means no control token and the
		# field is already its own decoding. Skips the character walk below
		# for the overwhelming majority of fields.
		return raw
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


# ---------------------------------------------------------------------------
# §15 integrity
# ---------------------------------------------------------------------------

class _Crc32:
	"""CRC-32 (IEEE 802.3) accumulator with a hashlib-shaped interface."""

	__slots__ = ('_v',)

	def __init__(self):
		self._v = 0

	def update(self, data):
		self._v = zlib.crc32(data, self._v)

	def hexdigest(self):
		return format(self._v & 0xFFFFFFFF, '08x')


_CRC32C_TABLE = None


def _crc32c_table():
	"""Build (once) the reflected Castagnoli table used by :class:`_Crc32c`."""
	global _CRC32C_TABLE  # memoized lookup table
	if _CRC32C_TABLE is None:
		table = []
		for i in range(256):
			crc = i
			for _ in range(8):
				crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
			table.append(crc)
		_CRC32C_TABLE = tuple(table)
	return _CRC32C_TABLE


class _Crc32c:
	"""CRC-32C (Castagnoli) accumulator, table-driven and dependency-free."""

	__slots__ = ('_v',)

	def __init__(self):
		self._v = 0xFFFFFFFF

	def update(self, data):
		table = _crc32c_table()
		v = self._v
		for byte in data:
			v = table[(v ^ byte) & 0xFF] ^ (v >> 8)
		self._v = v

	def hexdigest(self):
		return format(self._v ^ 0xFFFFFFFF, '08x')


def new_digest(algo):
	"""Return a fresh accumulator for ``algo``, or ``None`` if unimplemented.

	``None`` makes the corresponding ``#_checksum_<algo>_#`` line inert, which
	is exactly what §15.3 requires of a reader that does not implement an
	algorithm: no arming, no verification, no effect on reconstructed state.

	Recognizes ``crc32`` and ``crc32c`` natively, ``blake3`` when the optional
	third-party module is installed, and every fixed-length algorithm
	:mod:`hashlib` offers. Variable-length XOFs (``shake_*``) are rejected
	because §15.2 expects a single canonical hex digest.

	Args:
		algo: Algorithm name from the marker key, any case.

	Returns:
		object | None: Accumulator with ``update``/``hexdigest``, or ``None``.

	Examples:
		>>> d = new_digest('crc32'); d.update(b'123456789'); d.hexdigest()
		'cbf43926'
		>>> d = new_digest('crc32c'); d.update(b'123456789'); d.hexdigest()
		'e3069283'
		>>> new_digest('sha256').hexdigest()[:8]
		'e3b0c442'
		>>> new_digest('definitely-not-an-algorithm') is None
		True
	"""
	algo = algo.lower()
	if algo == 'crc32':
		return _Crc32()
	if algo == 'crc32c':
		return _Crc32c()
	if algo == 'blake3':
		try:
			import blake3
		except ImportError:
			return None
		return blake3.blake3()
	if algo.startswith('shake'):
		return None  # XOF: no single canonical digest length
	try:
		return hashlib.new(algo)
	except (ValueError, TypeError):
		return None


class DigestSet:
	"""Armed per-algorithm digest accumulators (§15).

	Integrity is opt-in: nothing is computed until a ``#_checksum_<algo>_#``
	line arms an algorithm, and each algorithm keeps an independent
	accumulator and independent segmentation (§15.5). Segments run
	marker-to-marker and may span part boundaries (§15.4, §15.7).

	Attributes:
		mismatches (list): ``(algo, expected, actual)`` per detected
			corruption. Detection only — §15.1 leaves the response to the
			implementation.
		verified (int): Count of segments that matched their stored digest.
	"""

	__slots__ = ('_acc', '_unimplemented', 'mismatches', 'verified')

	def __init__(self):
		self._acc = {}
		self._unimplemented = set()
		self.mismatches = []
		self.verified = 0

	def __bool__(self):
		return bool(self._acc)

	@property
	def armed(self):
		"""Return the set of currently armed algorithm names."""
		return frozenset(self._acc)

	def feed(self, raw, exclude=None):
		"""Feed one record's raw on-disk bytes to every armed accumulator.

		Args:
			raw: Full raw line bytes including delimiters and the terminating
				``\n`` (and a preceding ``\r`` if present) — §15.4.
			exclude: Algorithm whose own marker line this is; a digest can
				never cover its own marker (§15.4, §15.6).

		Returns:
			None
		"""
		if not self._acc:
			return
		for algo, acc in self._acc.items():
			if algo != exclude:
				acc.update(raw)

	def checkpoint(self, algo, expected):
		"""Process one ``#_checksum_<algo>_#`` line per §15.3.

		Args:
			algo: Algorithm named in the marker key.
			expected: The marker's value field — a lowercase hex digest, or
				``''``.

		Returns:
			str: ``'armed'`` (first marker for this algorithm; any value is
			ignored because there is nothing before it to verify),
			``'ok'``/``'mismatch'`` (segment verified), ``'reset'`` (empty
			value: new segment, previous one left unverified), or ``'inert'``
			(algorithm not implemented here).
		"""
		algo = algo.lower()
		if algo in self._unimplemented:
			return 'inert'
		if algo not in self._acc:
			acc = new_digest(algo)
			if acc is None:
				self._unimplemented.add(algo)
				return 'inert'
			self._acc[algo] = acc
			return 'armed'
		actual = self._acc[algo].hexdigest()
		self._acc[algo] = new_digest(algo)
		if not expected:
			return 'reset'
		if actual == expected.strip().lower():
			self.verified += 1
			return 'ok'
		self.mismatches.append((algo, expected.strip().lower(), actual))
		return 'mismatch'

	def digest_for(self, algo):
		"""Return the current hex digest of ``algo``'s open segment, or ``None``."""
		acc = self._acc.get(algo.lower())
		return None if acc is None else acc.hexdigest()


class IntegrityError(Exception):
	"""A §15 digest did not match and the corruption policy is ``'raise'``."""


class IntegrityWarning(UserWarning):
	"""A §15 digest did not match; §15.1 leaves the response to the reader."""


#: Responses to a detected §15 mismatch. Detection is mandatory once armed;
#: §15.1 makes the action implementation-defined.
CORRUPTION_POLICIES = ('warn', 'raise', 'ignore')


def report_corruption(digests, source, policy='warn'):
	"""Act on any mismatches a :class:`DigestSet` collected (§15.1).

	Args:
		digests: The :class:`DigestSet` used during replay.
		source: Path or description named in the message.
		policy: ``'warn'`` (default), ``'raise'``, or ``'ignore'``.

	Returns:
		list: The mismatches, whatever the policy.

	Raises:
		IntegrityError: If ``policy`` is ``'raise'`` and a segment mismatched.
		ValueError: If ``policy`` is not a recognized policy.
	"""
	if policy not in CORRUPTION_POLICIES:
		raise ValueError(f'policy must be one of {CORRUPTION_POLICIES}, got {policy!r}')
	if digests is None or not digests.mismatches:
		return []
	if policy == 'ignore':
		return list(digests.mismatches)
	detail = '; '.join(f'{algo}: expected {want}, computed {got}'
					   for algo, want, got in digests.mismatches)
	message = f'integrity check failed for {source!r} -- {detail}'
	if policy == 'raise':
		raise IntegrityError(message)
	warnings.warn(message, IntegrityWarning, stacklevel=3)
	return list(digests.mismatches)


def verify_part(path, *, encoding='utf8', delimiter=None, errors='replace',
				policy='ignore'):
	"""Replay a part and return the §15 integrity result.

	Args:
		path: Filesystem path of the part.
		encoding: Text encoding used to decode lines.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		errors: Decode error policy; see :func:`_iter_records`.
		policy: Passed to :func:`report_corruption`; defaults to ``'ignore'``
			so the caller inspects the result rather than being warned.

	Returns:
		DigestSet: Carries ``mismatches`` and ``verified``. A part with no
		``#_checksum_*_#`` markers verifies vacuously (§15.1).

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd)
		>>> append_records(path, [['a', '1']])
		>>> _ = append_checksum(path, 'crc32')       # arms; no value to verify
		>>> append_records(path, [['b', '2']])
		>>> _ = append_checksum(path, 'crc32')       # closes and records segment
		>>> d = verify_part(path); d.verified, d.mismatches
		(1, [])
		>>> os.unlink(path)
	"""
	delimiter = delimiter or delimiter_for_path(path)
	digests = DigestSet()
	replay_part(path, delimiter, encoding=encoding, errors=errors, digests=digests)
	report_corruption(digests, path, policy)
	return digests


def append_checksum(path, algo, *, encoding='utf8', delimiter=None, preceding=None):
	"""Close the current §15 segment for ``algo`` with a checksum marker.

	Replays the store to compute the digest of everything written since the
	previous ``#_checksum_<algo>_#`` line, then appends a new marker carrying
	it. The first marker for an algorithm carries no value, because §15.3
	gives it nothing to verify — it only arms the accumulator.

	When ``path`` names a part of a multi-part store, every earlier part is
	replayed first: §15.4 segments run marker-to-marker across the whole
	concatenation (§17.4), so a marker in one part closes a segment that may
	have been armed in an earlier one.

	§15.6: a digest can never cover its own marker line, so a single algorithm
	leaves its own markers unprotected. Alternate two algorithms to have each
	cover the other's digest lines.

	Args:
		path: Filesystem path of the part.
		algo: Digest algorithm name (for example ``'sha256'``, ``'crc32'``).
		encoding: Text encoding for the appended line.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		preceding: Explicit list of earlier part paths. ``None`` auto-detects
			them from ``path``'s store; pass ``()`` to treat ``path`` as
			standalone.

	Returns:
		str: The digest written, or ``''`` for the arming marker.

	Raises:
		ValueError: If ``algo`` is not implemented here (§15.3 would make the
			marker inert, so writing one would be pointless).
	"""
	if new_digest(algo) is None:
		raise ValueError(
			f'digest algorithm {algo!r} is not available; a marker for it would '
			f'be inert on read (§15.3)')
	delimiter = delimiter or delimiter_for_path(path)
	if preceding is None:
		preceding = _preceding_parts(path)
	digests = DigestSet()
	replay_parts([*preceding, path], delimiter, encoding=encoding, digests=digests)
	value = digests.digest_for(algo) if algo.lower() in digests.armed else ''
	key = checksum_marker_key(algo)
	line = format_marker_line(key, [value], delimiter) if value else key
	_warn_loose_extension(path, [line], delimiter)
	with open_part(path, 'ab', encoding=encoding) as f:
		f.write((line + '\n').encode(encoding, errors='replace'))
	return value


def _preceding_parts(path):
	"""Return the parts of ``path``'s store that precede it in ordinal order.

	Empty for a single unnumbered file. Used so a checksum marker closes the
	segment its algorithm actually opened, which §15.4 allows to have started
	in an earlier part.
	"""
	parsed = parse_part_name(path)
	if parsed is None:
		return []
	store = f'{parsed.store_path}.{parsed.format_ext}'
	return [pn.path for pn in store_parts(store)
			if pn.ordinal_value < parsed.ordinal_value]


def checksum_marker_key(algo):
	"""Return the ``#_checksum_<algo>_#`` marker key for ``algo`` (§15.2).

	Examples:
		>>> checksum_marker_key('SHA256')
		'#_checksum_sha256_#'
	"""
	return f'#_checksum_{algo.lower()}_#'


def checksum_algorithm(f0_raw):
	"""Return the algorithm named by a checksum marker key, or ``None``.

	Examples:
		>>> checksum_algorithm('#_checksum_sha256_#')
		'sha256'
		>>> checksum_algorithm('#_CHECKSUM_CRC32_#')
		'crc32'
		>>> checksum_algorithm('#_defaults_#') is None
		True
	"""
	kl = f0_raw.lower()
	if not CHECKSUM_MARKER_RE.match(kl):
		return None
	return kl[len('#_checksum_'):-len('_#')]


def _needs_escape(value, delimiter):
	"""Return True if ``value`` holds a character §13.3 requires escaping."""
	return delimiter in value or '\n' in value or '<' in value


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
	if not (is_key and value.startswith('#')) and not _needs_escape(value, delimiter):
		return value  # nothing to escape; skip the character walk
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
		str: One of ``'data'``, ``'comment'``, ``'marker'``, ``'checksum'``,
		or ``'ignore'``.

	Examples:
		>>> classify_record('alice')
		'data'
		>>> classify_record('# comment')
		'comment'
		>>> classify_record('#_defaults_#')
		'marker'
		>>> classify_record('#_checksum_sha256_#')
		'checksum'
		>>> classify_record('#_future_marker_#')
		'ignore'
	"""
	if not f0_raw.startswith('#'):
		return 'data'
	if not MARKER_RE.match(f0_raw):
		return 'comment'
	# §12.2.1: marker keys are compared case-insensitively over ASCII.
	kl = f0_raw.lower()
	if CHECKSUM_MARKER_RE.match(kl):
		return 'checksum'
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
	elif kl == MARKER_WRITE_ACK:
		# Advisory only (§12.4.3): recorded for writers, never applied to data.
		mode = (decoded[0].strip().lower() if decoded else '')
		state.write_ack = mode if mode in WRITE_ACK_MODES else WRITE_ACK_MEMORY
	elif kl == MARKER_ROTATE:
		# Advisory only (§12.4.3); consumed by snapshot_store (§19.5).
		action = (decoded[0].strip().lower() if decoded else '')
		state.rotate = action if action in ROTATE_ACTIONS else ROTATE_KEEP


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


def materialize_row(entry):
	"""Return ``entry``'s row with absent trailing columns resolved (§14.3).

	A row is stored at the width it was written with. Specification §14.3
	resolves a value column at or beyond that width to the default active at
	the row's position, so materialization pads the row out to the width of
	its write-time defaults. Rows written while no defaults were active are
	returned unchanged, and a row wider than the defaults is never trimmed.

	Args:
		entry: :class:`StoreEntry` produced during replay.

	Returns:
		list: ``[key, value…]`` padded to ``len(entry.row_defaults) + 1``.

	Examples:
		>>> materialize_row(StoreEntry(['k', 'v1'], ['D1', 'D2'], False))
		['k', 'v1', 'D2']
		>>> materialize_row(StoreEntry(['k', 'v1'], [], False))
		['k', 'v1']
		>>> materialize_row(StoreEntry(['k', 'a', 'b', 'c'], ['D1'], False))
		['k', 'a', 'b', 'c']
	"""
	row = list(entry.row)
	for j in range(len(row), len(entry.row_defaults) + 1):
		row.append(_default_at(entry.row_defaults, j))
	return row


def process_record(raw_line, state, store, delimiter, *, offset=None,
				   store_offset=False, values_cache=None, digests=None,
				   raw_bytes=None):
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
		digests: Optional :class:`DigestSet` to feed and checkpoint (§15).
		raw_bytes: The record's raw on-disk bytes including its terminator,
			required when ``digests`` is given (§15.4).

	Returns:
		tuple: ``(kind, payload)`` where ``kind`` is one of ``'data'``,
		``'tombstone'``, ``'marker'``, ``'comment'``, ``'checksum'``, or
		``'ignore'``. For ``'checksum'`` the payload is the algorithm name.

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
	algo = checksum_algorithm(f0) if kind == 'checksum' else None
	# Appendix B ordering: classify, then feed every armed accumulator, then
	# act. A checksum line is withheld only from its own algorithm (§15.4);
	# other algorithms see it as ordinary content, which is what lets two
	# staggered algorithms protect each other's digest lines (§15.6).
	if digests is not None and raw_bytes is not None:
		digests.feed(raw_bytes, exclude=algo)
	if kind == 'checksum':
		if digests is not None:
			expected = decode_field(fields[1], delimiter) if len(fields) > 1 else ''
			digests.checkpoint(algo, expected)
		return kind, algo
	if kind in ('comment', 'ignore'):
		return kind, None
	if kind == 'marker':
		apply_marker(state, f0, fields[1:], delimiter)
		return kind, None
	is_tombstone = len(fields) == 1
	key = decode_field(_strip_field(f0, state.strip_trailing), delimiter)
	if key == '':
		return 'ignore', None  # §8.4: empty-key rows are discarded like comments
	if is_tombstone:
		store.pop(key, None)
		if values_cache is not None:
			values_cache.pop(key, None)
		return 'tombstone', key
	row, row_defaults, fill_empty = _resolve_value_columns(fields, state, delimiter)
	entry = StoreEntry(row, row_defaults, fill_empty)
	if store_offset:
		if offset is None:
			raise ValueError('store_offset=True requires an offset')
		store[key] = offset
	else:
		store[key] = entry
	if values_cache is not None:
		values_cache[key] = materialize_row(entry)
	return 'data', entry


def _iter_records(stream, encoding, *, errors='replace', source=''):
	"""Yield ``(offset, text)`` for every **committed** record in ``stream``.

	Implements the framing rules of §4 once, for every reader:

	- records split on ``\\n`` (§4.2); at most one ``\\r`` immediately before
	  the terminator is stripped, and a ``\\r`` anywhere else is field data;
	- the terminating ``\\n`` is the commit marker, so a trailing unterminated
	  line is discarded whether or not it would parse (§4.3-§4.5).

	Blank lines are yielded, not skipped: their raw bytes belong to any open
	integrity segment (§15.4), and the reader discards them anyway as
	empty-key rows (§8.4).

	Streams rather than slurping, so peak memory does not include a copy of
	the whole part. ``offset`` is a byte offset into the *decoded* stream,
	which is what an offset index needs (§19.7).

	Args:
		stream: Binary file-like object positioned at the start of the part.
		encoding: Text encoding (§4.1 mandates UTF-8).
		errors: Decode error policy. ``'replace'`` (the default) salvages a
			damaged part but warns once, naming the byte offset; ``'strict'``
			raises ``UnicodeDecodeError``.
		source: Path used in the warning message.

	Yields:
		tuple: ``(offset, text, raw)`` per committed record, where ``raw`` is
		the untouched on-disk bytes including the terminator (§15.4).

	Examples:
		>>> import io
		>>> [(o, t) for o, t, _ in _iter_records(io.BytesIO(b'a\\n\\nb\\r\\ntorn'), 'utf8')]
		[(0, 'a'), (2, ''), (3, 'b')]
		>>> [r for _, _, r in _iter_records(io.BytesIO(b'a\\r\\n'), 'utf8')]
		[b'a\\r\\n']
	"""
	pos = 0
	warned = False
	for raw in stream:
		if not raw.endswith(b'\n'):
			break  # §4.3: uncommitted trailing bytes are discarded
		start = pos
		pos += len(raw)
		line = raw[:-1]
		if line.endswith(b'\r'):  # removesuffix needs 3.9+
			line = line[:-1]
		if not line:
			yield start, '', raw
			continue
		try:
			text = line.decode(encoding)
		except UnicodeDecodeError as exc:
			if errors == 'strict':
				raise
			if not warned:
				warned = True
				warnings.warn(
					f'TSVZ: undecodable {encoding} bytes at offset '
					f'{start + exc.start}'
					f'{" in " + repr(source) if source else ""}; substituting '
					f'replacement characters. Further occurrences not reported.',
					UnicodeWarning, stacklevel=2)
			text = line.decode(encoding, errors=errors)
		yield start, text, raw


def replay_bytes(data, delimiter, *, encoding='utf8', store=None,
				 store_offset=False, values_cache=None, errors='replace',
				 digests=None):
	"""Replay committed bytes into a key->entry mapping (last write wins).

	Only the committed payload is processed (§4.3).

	Args:
		data: Raw part bytes.
		delimiter: Field delimiter.
		encoding: Text encoding used to decode lines.
		store: Optional existing mapping to update; a new
			:class:`~collections.OrderedDict` is created when ``None``.
		store_offset: If True, store byte offsets instead of entries.
		values_cache: Optional key->row cache filled during replay.
		errors: Decode error policy; see :func:`_iter_records`.
		digests: Optional :class:`DigestSet` collecting §15 verification.

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
	for offset, line, raw in _iter_records(io.BytesIO(data), encoding, errors=errors):
		process_record(
			line, state, store, delimiter,
			offset=offset, store_offset=store_offset, values_cache=values_cache,
			digests=digests, raw_bytes=raw,
		)
	return store, state


def replay_part(path, delimiter, *, encoding='utf8', store=None,
				store_offset=False, values_cache=None, errors='replace',
				digests=None, state=None):
	"""Replay a part file from disk into a key->entry mapping.

	Streams the part rather than reading it whole, so peak memory scales with
	the live key set rather than with the file. If ``path`` does not exist,
	returns an empty store paired with a fresh :class:`ReaderState`.

	Args:
		path: Filesystem path of the part.
		delimiter: Field delimiter.
		encoding: Text encoding used to decode lines.
		store: Optional existing mapping to update.
		store_offset: If True, store byte offsets instead of entries.
		values_cache: Optional key->row cache filled during replay.
		errors: Decode error policy; see :func:`_iter_records`.
		digests: Optional :class:`DigestSet` collecting §15 verification.
		state: Optional :class:`ReaderState` to continue from. Multi-part
			replay passes the previous part's state so marker state carries
			across the boundary (§17.4).

	Returns:
		tuple: ``(store, state)`` after replaying the part.
	"""
	if store is None:
		store = OrderedDict()
	if state is None:
		state = ReaderState()
	try:
		with open_part(path, 'rb', encoding=encoding) as f:
			for offset, line, raw in _iter_records(f, encoding, errors=errors, source=path):
				process_record(
					line, state, store, delimiter, offset=offset,
					store_offset=store_offset, values_cache=values_cache,
					digests=digests, raw_bytes=raw,
				)
	except FileNotFoundError:
		return store, state
	return store, state


def replay_parts(paths, delimiter, *, encoding='utf8', store=None, state=None,
				 digests=None, errors='replace'):
	"""Replay several parts as ONE concatenation (§17.4).

	Marker state (§12) and integrity accumulators (§15.4) both carry across
	part boundaries, because the logical store is the concatenation of its
	parts in ordinal order — not a sequence of independent files.

	Args:
		paths: Part paths already in ordinal order (see :func:`store_parts`).
		delimiter: Field delimiter.
		encoding: Text encoding used to decode lines.
		store: Optional mapping to populate.
		state: Optional starting :class:`ReaderState`.
		digests: Optional :class:`DigestSet` spanning every part.
		errors: Decode error policy; see :func:`_iter_records`.

	Returns:
		tuple: ``(store, state, digests)`` after the last part.
	"""
	if store is None:
		store = OrderedDict()
	if state is None:
		state = ReaderState()
	if digests is None:
		digests = DigestSet()
	for path in paths:
		store, state = replay_part(
			path, delimiter, encoding=encoding, store=store, state=state,
			digests=digests, errors=errors,
		)
	return store, state, digests


def read_multipart(store_path, *, encoding='utf8', delimiter=None, store=None,
				   errors='replace', policy='warn', include_rotated=False):
	"""Replay a whole multi-part store into a key→row mapping (§17).

	Parts are discovered by :func:`store_part_paths` and replayed in ordinal
	order as one stream, so last-wins, first-appearance order, marker state,
	and integrity segments all behave as if the parts were concatenated.
	A single unnumbered file is handled as the one-part case (§17.1).

	Byte-offset indexing (:func:`read_offsets`, :class:`OffsetStore`) is
	single-part only: an offset alone cannot address a multi-part store, which
	would need a ``(part, offset)`` pair.

	Args:
		store_path: Store path including its format extension.
		encoding: Text encoding used to decode lines.
		delimiter: Field delimiter; inferred from ``store_path`` when ``None``.
		store: Optional mapping to populate.
		errors: Decode error policy; see :func:`_iter_records`.
		policy: Response to a §15 mismatch — ``'warn'``, ``'raise'``,
			``'ignore'``.
		include_rotated: If True, also replay ``.rotated`` parts. Off by
			default because §17.2 excludes them from normal loading.

	Returns:
		MutableMapping: Ordered key→row mapping, carrying ``_reader_state``,
		``_digests``, and ``_parts`` attributes.

	Raises:
		FileNotFoundError: If the store has no parts and no single file.

	Examples:
		>>> import os, tempfile
		>>> d = tempfile.mkdtemp(); base = os.path.join(d, 'events.tsvz')
		>>> append_records(part_path(base, 1), [['a', '1'], ['b', '2']], create=True)
		>>> append_records(part_path(base, 2), [['b', '9'], ['a']], create=True)
		>>> dict(read_multipart(base))
		{'b': ['b', '9']}
	"""
	delimiter = delimiter or delimiter_for_path(store_path)
	paths = store_part_paths(store_path, include_rotated=include_rotated)
	if not paths:
		raise FileNotFoundError(store_path)
	if store is None:
		store = OrderedDict()
	replayed, state, digests = replay_parts(
		paths, delimiter, encoding=encoding, errors=errors)
	report_corruption(digests, store_path, policy)
	clear, setitem = _base_mutators(store)
	clear(store)
	for key, entry in replayed.items():
		setitem(store, key, materialize_row(entry) if isinstance(entry, StoreEntry) else entry)
	_attach_replay_meta(store, state, None, digests)
	with contextlib.suppress(AttributeError):
		store._parts = list(paths)
	return store


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


def _bake_row(entry, width):
	"""Resolve ``entry`` to at least ``width`` fields for a snapshot (§19.4).

	Beyond the row's own materialized width, each extra column takes the value
	§14.3 gives it *at the row's original position* — i.e. from the defaults
	that were in force when the row was written, not the snapshot's. That is
	what lets a snapshot hoist ``#_defaults_#`` to the top without changing
	how a row written before that marker resolves.
	"""
	row = materialize_row(entry) if isinstance(entry, StoreEntry) else list(entry)
	defaults = entry.row_defaults if isinstance(entry, StoreEntry) else []
	while len(row) < width:
		row.append(_default_at(defaults, len(row)))
	return row


def _snapshot_body(replayed, state, delimiter, header=None):
	"""Build the §19 snapshot lines for a set of replayed entries.

	§19.4 requires a snapshot to reproduce the store's exact observable read
	result. Hoisting the final ``#_defaults_#`` to the top of the snapshot is
	safe only when that marker never changed over the compacted span, because
	forward-only binding (§12.4.4) means a row written before a defaults line
	resolves its absent columns differently from one written after.

	So: when every live row bound the same defaults as the final state, the
	rows stay sparse and the marker is simply re-emitted. When they did not,
	every row is baked out to the width the marker covers, carrying the value
	each column resolved to at its own position. ``#_defaults_#`` is still
	emitted either way, because a missing-key read returns the active defaults
	(§14.3) and dropping it would change *that* observable.

	Args:
		replayed: Key -> :class:`StoreEntry` mapping from replay.
		state: Final :class:`ReaderState` of the compacted span.
		delimiter: Field delimiter.
		header: Optional header comment (a §19.2.3c deviation when given).

	Returns:
		list[str]: Snapshot lines, markers first, then live rows in
		first-appearance order.
	"""
	entries = [e for e in replayed.values() if isinstance(e, StoreEntry)]
	uniform = all(e.row_defaults == state.defaults for e in entries)
	snap = ReaderState()
	snap.defaults = list(state.defaults)
	snap.return_on_missing = state.return_on_missing
	snap.write_ack = state.write_ack
	lines = []
	if header:
		lines.append(format_header_comment(_parse_columns(header, delimiter), delimiter))
	lines.extend(build_snapshot_preamble(snap, delimiter))
	width = 0 if uniform else len(state.defaults) + 1
	for entry in replayed.values():
		row = (list(entry.row) if uniform and isinstance(entry, StoreEntry)
			   else _bake_row(entry, width))
		if isinstance(row, list) and row:
			lines.append(format_data_row(row, delimiter))
	return lines


def build_snapshot_preamble(state, delimiter):
	"""Build the marker preamble written at the start of a snapshot (§19).

	Implements the always-correct strategy named in §19.4: because a snapshot
	writes **fully resolved** value cells, it pins ``#_strip_trailing_whites_#``
	and ``#_fill_empty_with_default_#`` to ``false`` so that no reader
	re-applies stripping or filling to values that already went through it.
	``#_defaults_#`` is re-emitted last (§19.2.3a) and remains meaningful as
	the fallback for absent trailing columns (§14.3).

	Pinning both markers is what makes §19.4 fidelity hold when the source
	state varied over the life of the compacted prefix — emitting only the
	final state would silently re-resolve rows written under earlier state.

	Args:
		state: Reader state whose settings are serialized.
		delimiter: Field delimiter.

	Returns:
		list[str]: Marker lines without trailing newlines.

	Examples:
		>>> build_snapshot_preamble(ReaderState(), '\\t')
		['#_version_#\\t1', '#_strip_trailing_whites_#\\tfalse', '#_fill_empty_with_default_#\\tfalse']
		>>> st = ReaderState(); st.defaults = ['x']; st.return_on_missing = False
		>>> build_snapshot_preamble(st, '\\t')[-2:]
		['#_return_defaults_when_missing_#\\tfalse', '#_defaults_#\\tx']
	"""
	lines = [
		format_marker_line('#_version_#', ['1'], delimiter),
		format_marker_line('#_strip_trailing_whites_#', ['false'], delimiter),
		format_marker_line('#_fill_empty_with_default_#', ['false'], delimiter),
	]
	if not state.return_on_missing:
		lines.append(format_marker_line('#_return_defaults_when_missing_#', ['false'], delimiter))
	if state.write_ack != WRITE_ACK_MEMORY:
		lines.append(format_marker_line(MARKER_WRITE_ACK, [state.write_ack], delimiter))
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


def _compression_suffix(name):
	"""Return ``name``'s trailing compression suffix, or ``''`` if uncompressed.

	Examples:
		>>> _compression_suffix('store.tsvz.gz'), _compression_suffix('store.tsvz')
		('.gz', '')
	"""
	_, _, ext = name.rpartition('.')
	return f'.{ext}' if ext.lower() in COMPRESSION_EXTENSIONS else ''


def is_compressed_path(path):
	"""Return True when ``path`` carries a recognized compression suffix (§16).

	Examples:
		>>> is_compressed_path('store.tsvz.gz'), is_compressed_path('store.tsvz')
		(True, False)
	"""
	return bool(_compression_suffix(path))


def _warn_loose_extension(path, lines, delimiter):
	"""Warn when spec-only syntax is written to a loose extension (§5.2).

	A loose extension (``.tsv``, ``.csv``, ...) advertises "a tabular fallback
	with no semantic guarantees", i.e. that generic delimiter-separated tooling
	can read it. Escape tokens (§13) and markers (§12) break that promise:
	a generic reader sees ``a<sep>b`` as the literal seven characters. Plain
	rows are written to a loose part silently, because those really are
	interchangeable.

	Args:
		path: Destination part path.
		lines: Encoded lines about to be written (without terminators).
		delimiter: Field delimiter in use.

	Returns:
		None: Warns via :mod:`warnings` as a side effect.
	"""
	if is_strict_store(path):
		return
	for line in lines:
		token = '<' in line
		marker = line.startswith('#')
		if token or marker:
			what = 'an escape token' if token else 'a marker line'
			warnings.warn(
				f'wrote {what} to the loose extension {path!r}; §5.2 says such a '
				f'file carries no semantic guarantees, and generic '
				f'delimiter-separated tools will read it literally. Use a strict '
				f'extension ({", ".join(sorted(STRICT_EXTENSIONS))}) for full '
				f'semantics.',
				UserWarning, stacklevel=3)
			return


# ---------------------------------------------------------------------------
# §17 multi-part stores
# ---------------------------------------------------------------------------

#: Appendix A part-file grammar:
#: ``store-path "." format-ext "." ordinal [".rotated"] ["." codec]``
PART_NAME_RE = re.compile(
	r'^(?P<store>.+)'
	r'\.(?P<ext>tsvz|csvz|nsvz|psvz|tsv|csv|nsv|psv)'
	r'\.(?P<ordinal>[0-9A-Fa-f]+)'
	r'(?P<rotated>\.rotated)?'
	r'(?:\.(?P<codec>gz|gzip|bz2|bzip2|xz|lzma|zst|zstd))?$',
	re.ASCII | re.IGNORECASE)

PartName = namedtuple(
	'PartName', 'path store_path format_ext ordinal ordinal_value rotated codec')


def parse_part_name(path):
	"""Parse a §17.2 part filename, or return ``None`` if it is not one.

	The ordinal is a hexadecimal integer (§17.3); ``.rotated`` is an infix
	before any compression suffix (§16.2). A plain ``store.tsvz`` with no
	ordinal is *not* a part — it is the degenerate single-file store of §17.1.

	Args:
		path: Filesystem path to parse.

	Returns:
		PartName | None: Parsed components, with ``ordinal_value`` the ordinal
		as an integer.

	Examples:
		>>> pn = parse_part_name('/d/events.tsvz.1f')
		>>> pn.store_path, pn.format_ext, pn.ordinal_value, pn.rotated, pn.codec
		('/d/events', 'tsvz', 31, False, '')
		>>> pn = parse_part_name('/d/events.tsvz.0a.rotated.zst')
		>>> pn.ordinal_value, pn.rotated, pn.codec
		(10, True, 'zst')
		>>> parse_part_name('/d/events.tsvz') is None
		True
		>>> parse_part_name('/d/events.tsvz.gz') is None
		True
	"""
	match = PART_NAME_RE.match(str(path))
	if match is None:
		return None
	ordinal = match.group('ordinal')
	return PartName(
		path=str(path),
		store_path=match.group('store'),
		format_ext=match.group('ext'),
		ordinal=ordinal,
		ordinal_value=int(ordinal, 16),
		rotated=bool(match.group('rotated')),
		codec=match.group('codec') or '',
	)


def part_path(store_path, ordinal, *, rotated=False, codec=''):
	"""Build a §17.2 part filename.

	Args:
		store_path: Store path including its format extension, e.g.
			``/d/events.tsvz``.
		ordinal: Hex ordinal string, or an int to be rendered as hex.
		rotated: If True, insert the ``.rotated`` component (§19.5).
		codec: Optional compression suffix, with or without a leading dot.

	Returns:
		str: The part path.

	Examples:
		>>> part_path('/d/events.tsvz', 31)
		'/d/events.tsvz.1f'
		>>> part_path('/d/events.tsvz', '0a', rotated=True, codec='zst')
		'/d/events.tsvz.0a.rotated.zst'
	"""
	if isinstance(ordinal, int):
		ordinal = format(ordinal, 'x')
	suffix = '.rotated' if rotated else ''
	if codec:
		suffix += '.' + codec.lstrip('.')
	return f'{store_path}.{ordinal}{suffix}'


def new_ordinal():
	"""Return a fresh ordinal: a UUIDv7 as 32 hex characters (§17.6).

	UUIDv7 is time-ordered, so parts sort chronologically under the
	hexadecimal-integer ordering of §17.3, and carries random bits so two
	processes starting in the same millisecond do not collide.

	Returns:
		str: 32 lowercase hex characters.

	Examples:
		>>> o = new_ordinal()
		>>> len(o), int(o, 16) > 0, (int(o, 16) >> 76) & 0xF
		(32, True, 7)
		>>> new_ordinal() != new_ordinal()
		True
	"""
	timestamp = int(time.time() * 1000) & ((1 << 48) - 1)
	rand_a = secrets.randbits(12)
	rand_b = secrets.randbits(62)
	value = (timestamp << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
	return format(value, '032x')


def store_parts(store_path, *, include_rotated=False):
	"""Return a store's parts in ordinal order (§17.3).

	Ordering is by the **integer** value of the hex ordinal, never
	lexicographic — ``.f`` (15) precedes ``.10`` (16) even though it follows
	it as a string. Parts carrying ``.rotated`` are excluded from normal
	loading (§17.2, §19.5).

	Args:
		store_path: Store path including its format extension. A part path is
			accepted too and normalized back to its store.
		include_rotated: If True, also return ``.rotated`` parts.

	Returns:
		list[PartName]: Parts in ascending ordinal order. Empty when the store
		is a single unnumbered file or does not exist.
	"""
	parsed = parse_part_name(store_path)
	if parsed is not None:
		store_path = f'{parsed.store_path}.{parsed.format_ext}'
	directory = os.path.dirname(store_path) or '.'
	base = os.path.basename(store_path)
	try:
		names = os.listdir(directory)
	except (FileNotFoundError, NotADirectoryError):
		return []
	found = []
	for name in names:
		candidate = parse_part_name(os.path.join(directory, name))
		if candidate is None:
			continue
		if os.path.basename(f'{candidate.store_path}.{candidate.format_ext}') != base:
			continue
		if candidate.rotated and not include_rotated:
			continue
		found.append(candidate)
	# §17.3: integer ordering; path breaks ties so the result is deterministic.
	found.sort(key=lambda pn: (pn.ordinal_value, pn.path))
	return found


def store_part_paths(store_path, *, include_rotated=False):
	"""Return the paths making up a store, in replay order.

	Falls back to ``[store_path]`` for the single-file case of §17.1 when no
	numbered parts exist.

	Args:
		store_path: Store path including its format extension.
		include_rotated: If True, also include ``.rotated`` parts.

	Returns:
		list[str]: Part paths in ordinal order, or the single file, or ``[]``.
	"""
	parts = store_parts(store_path, include_rotated=include_rotated)
	if parts:
		if os.path.isfile(store_path):
			warnings.warn(
				f'{store_path!r} exists alongside numbered parts; the numbered '
				f'parts are the store (§17.1) and the unnumbered file is ignored.',
				UserWarning, stacklevel=2)
		return [pn.path for pn in parts]
	return [store_path] if os.path.isfile(store_path) else []


def format_base(path):
	"""Reduce ``path`` to its store path plus format extension, lowercased.

	§16.3: format and delimiter inference strip the compression suffix first,
	and then the §17 ordinal and ``.rotated`` components. Without this a part
	named ``events.csvz.1f`` infers the fallback TAB delimiter instead of the
	comma its CSVZ variant requires.

	Args:
		path: Any part path, store path, or plain filename.

	Returns:
		str: Lowercased path through the format extension.

	Examples:
		>>> format_base('events.csvz.1f')
		'events.csvz'
		>>> format_base('events.psvz.0a.rotated.zst')
		'events.psvz'
		>>> format_base('data.csv.gz')
		'data.csv'
		>>> format_base('plain.tsvz')
		'plain.tsvz'
	"""
	parsed = parse_part_name(path)
	if parsed is not None:
		return f'{parsed.store_path}.{parsed.format_ext}'.lower()
	return _strip_compression_suffix(path)


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
		>>> is_strict_store('data.csvz.1f'), is_strict_store('data.csv.1f')
		(True, False)
	"""
	lower = format_base(path)
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
		>>> delimiter_for_path('events.csvz.1f'), delimiter_for_path('events.psvz.0a.rotated')
		(',', '|')
		>>> delimiter_for_path('x.unknown', delimiter='|')
		'|'
	"""
	if delimiter is not None:
		return delimiter or DEFAULT_DELIMITER
	lower = format_base(path)
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
	writing = 'r' not in mode
	kwargs = {}
	if 'b' not in mode:
		kwargs['encoding'] = encoding
	if lower.endswith(('.xz', '.lzma')):
		import lzma
		# lzma.open takes `preset`, never `compresslevel`, for both suffixes.
		return lzma.open(path, mode, **({'preset': compress_level} if writing else {}), **kwargs)
	if lower.endswith(('.gz', '.gzip')):
		import gzip
		return gzip.open(path, mode, **({'compresslevel': compress_level} if writing else {}), **kwargs)
	if lower.endswith(('.bz2', '.bzip2')):
		import bz2
		return bz2.open(path, mode, **({'compresslevel': compress_level} if writing else {}), **kwargs)
	if lower.endswith(('.zst', '.zstd')):
		try:
			from compression import zstd
		except ImportError:
			try:
				import zstandard  # noqa: F401  # third-party fallback probe
			except ImportError:
				raise ImportError(
					f'zstd support is unavailable for {path!r}: needs Python 3.14+ '
					f'(compression.zstd). Refusing to fall back to plaintext.',
				) from None
			raise ImportError(
				f'zstd support for {path!r} requires the stdlib compression.zstd '
				f'module (Python 3.14+); the third-party zstandard package is not used.',
			) from None
		return zstd.open(path, mode, **({'level': compress_level} if writing else {}), **kwargs)
	# Uncompressed: builtins.open needs no explicit 't', and 'b' is already
	# present in every mode that reaches here without one having been added.
	if 't' in mode:
		return open(path, mode.replace('t', ''), encoding=encoding)
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
	try:
		# 'x' so a concurrent creator's part is never truncated (the isfile
		# check above is only a fast path, not a guarantee).
		seed = []
		if header:
			seed.append(format_header_comment(header, delimiter))
		if defaults:
			seed.append(format_marker_line(MARKER_DEFAULTS, defaults, delimiter))
		_warn_loose_extension(path, seed, delimiter)
		with open_part(path, 'xb', encoding=encoding) as f:
			for line in seed:
				f.write(line.encode(encoding, errors='replace') + b'\n')
	except FileExistsError:
		pass  # another writer won the race; its part is authoritative
	return True


def _base_mutators(store):
	"""Return ``(clear, setitem)`` that bypass a live store's overrides.

	Replaying into a caller-supplied mapping must not trigger that mapping's
	side effects: :class:`WalStore` truncates the part in ``clear()`` and
	queues a WAL append in ``__setitem__``, so populating one directly would
	destroy the very file being read and then re-append every replayed row.
	Dict subclasses are therefore filled through the base implementation
	(``OrderedDict``'s, to keep its linked list consistent).

	Args:
		store: Target mapping for a replay.

	Returns:
		tuple: ``(clear(store), setitem(store, key, value))`` callables.

	Examples:
		>>> clear, setitem = _base_mutators(OrderedDict())
		>>> clear is OrderedDict.clear, setitem is OrderedDict.__setitem__
		(True, True)
	"""
	if isinstance(store, OrderedDict):
		return OrderedDict.clear, OrderedDict.__setitem__
	if isinstance(store, dict):
		return dict.clear, dict.__setitem__
	return type(store).clear, type(store).__setitem__


def _attach_replay_meta(target, state, values_cache=None, digests=None):
	try:
		target._reader_state = state
		if values_cache is not None:
			target._values_cache = values_cache
		if digests is not None:
			target._digests = digests
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
	state = ReaderState()
	result = empty
	scratch = OrderedDict()
	# A full forward pass is required, not just a seek to the tail: marker
	# state is forward-only (§12.1), so the defaults that resolve the last
	# row's absent columns (§14.3) are only known after replaying everything
	# before it.
	try:
		with open_part(path, 'rb', encoding=encoding) as f:
			for offset, line, _raw in _iter_records(f, encoding, source=path):
				scratch.clear()
				kind, entry = process_record(line, state, scratch, delimiter)
				if kind == 'data' and entry is not None:
					result = offset if store_offset else materialize_row(entry)
	except FileNotFoundError:
		return empty
	return result


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


def _replay_into(path, store, *, create, encoding, delimiter, defaults, header,
				 store_offset, cache_values=True, policy='warn'):
	"""Shared body of :func:`read_store` and :func:`read_offsets`.

	Returns:
		tuple: ``(store, state, values_cache, digests)``; ``values_cache`` is
		``None`` unless ``store_offset`` is True.
	"""
	header_cols = _parse_columns(header, delimiter) if header else []
	ensure_part_exists(
		path, create=create, encoding=encoding, delimiter=delimiter,
		header=header_cols or None, defaults=_normalize_defaults(defaults),
	)
	values_cache = {} if (store_offset and cache_values) else None
	# A DigestSet costs nothing until a #_checksum_*_# line arms it (§15.1).
	digests = DigestSet()
	replayed, state = replay_part(
		path, delimiter, encoding=encoding, store=OrderedDict(),
		store_offset=store_offset, values_cache=values_cache, digests=digests,
	)
	report_corruption(digests, path, policy)
	# Replay is a read: fill the target through its base mapping so a live
	# store's clear()/__setitem__ side effects (truncate, WAL append) never
	# fire. See _base_mutators.
	clear, setitem = _base_mutators(store)
	clear(store)
	for key, value in replayed.items():
		setitem(store, key, value if store_offset or not isinstance(value, StoreEntry)
				else materialize_row(value))
	return store, state, values_cache, digests


def read_store(path, *, create=False, encoding='utf8', delimiter=None,
			   defaults=None, store=None, store_offset=False, last_record_only=False,
			   header=None, policy='warn'):
	"""Replay a part into an ordered mapping of key to row list.

	Rows are in first-appearance order (§3.4). Each row keeps the width it
	was written with (§3.6), extended only where §14.3 resolves an absent
	trailing column from the defaults active at that row.

	Args:
		path: Filesystem path of the part.
		create: If True, create a missing part before reading.
		encoding: Text encoding used to decode lines.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		defaults: Defaults marker written **only when creating** a new part;
			ignored for a part that already exists.
		store: Optional mapping to populate; a new ordered dict is used
			when ``None``. Filled through its base mapping, so passing a
			live :class:`WalStore` neither truncates the part nor queues
			appends.
		store_offset: Deprecated; use :func:`read_offsets`.
		last_record_only: Deprecated; use :func:`read_last_record`.
		header: Header comment written **only when creating** a new part.
		policy: Response to a §15 integrity mismatch — ``'warn'`` (default),
			``'raise'``, or ``'ignore'``. A part with no ``#_checksum_*_#``
			markers is never checked (§15.1).

	Returns:
		MutableMapping: Ordered key→row mapping. Carries a ``_digests``
		attribute with the §15 result.

	Raises:
		FileNotFoundError: If the part is absent and ``create`` is False.

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd); os.unlink(path)
		>>> append_records(path, [['a', '1'], ['b', '2']], create=True, header=['id', 'v'])
		>>> dict(read_store(path))
		{'a': ['a', '1'], 'b': ['b', '2']}
		>>> os.unlink(path)
	"""
	delimiter = delimiter or delimiter_for_path(path)
	if last_record_only:
		warnings.warn(
			'read_store(last_record_only=True) is deprecated; call '
			f'read_last_record() instead. Removal in TSVZ {LEGACY_REMOVAL_VERSION}.',
			DeprecationWarning, stacklevel=2)
		ensure_part_exists(path, create=create, encoding=encoding, delimiter=delimiter,
						   header=_parse_columns(header, delimiter) or None,
						   defaults=_normalize_defaults(defaults))
		return read_last_record(path, encoding=encoding, delimiter=delimiter,
								store_offset=store_offset)
	if store_offset:
		warnings.warn(
			'read_store(store_offset=True) is deprecated; call read_offsets() '
			f'instead. Removal in TSVZ {LEGACY_REMOVAL_VERSION}.',
			DeprecationWarning, stacklevel=2)
		if store is None:
			store = OrderedDict()
		store, state, values_cache, digests = _replay_into(
			path, store, create=create, encoding=encoding, delimiter=delimiter,
			defaults=defaults, header=header, store_offset=True, policy=policy)
		_attach_replay_meta(store, state, values_cache, digests)
		return store
	if store is None:
		store = OrderedDict()
	store, state, _values, digests = _replay_into(
		path, store, create=create, encoding=encoding, delimiter=delimiter,
		defaults=defaults, header=header, store_offset=False, policy=policy)
	_attach_replay_meta(store, state, None, digests)
	return store


def read_offsets(path, *, create=False, encoding='utf8', delimiter=None,
				 defaults=None, header=None, store=None, cache_values=True,
				 policy='warn'):
	"""Replay a part into a key→byte-offset index instead of materialized rows.

	Offsets address the start of each key's winning record in the *decoded*
	byte stream, so they are only meaningful for an uncompressed part.

	Args:
		path: Filesystem path of the part.
		create: If True, create a missing part before reading.
		encoding: Text encoding used to decode lines.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		defaults: Defaults marker written only when creating a new part.
		header: Header comment written only when creating a new part.
		store: Optional mapping to populate with the offsets.
		cache_values: If True, also materialize every row into a cache and
			return it. Set False to build the index alone — that is the whole
			point of an offset index, and it keeps memory proportional to the
			key count rather than to the data.

	Returns:
		tuple: ``(offsets, values, state)`` — key→offset mapping, the
		key→row cache (``None`` when ``cache_values`` is False), and the
		final :class:`ReaderState`.

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsv'); os.close(fd)
		>>> _ = open(path, 'w').write('a\\t1\\nb\\t2\\n')
		>>> offsets, values, _ = read_offsets(path)
		>>> offsets['b'], values['b']
		(4, ['b', '2'])
		>>> os.unlink(path)
	"""
	delimiter = delimiter or delimiter_for_path(path)
	if store is None:
		store = OrderedDict()
	store, state, values_cache, digests = _replay_into(
		path, store, create=create, encoding=encoding, delimiter=delimiter,
		defaults=defaults, header=header, store_offset=True,
		cache_values=cache_values, policy=policy)
	_attach_replay_meta(store, state, values_cache, digests)
	return store, values_cache, state


def _coerce_row(row, delimiter):
	"""Normalize a row argument to a list of strings.

	A string argument is split on ``delimiter``. Sequence elements are
	converted with ``str``; only ``None`` becomes the empty string. Values
	are passed through verbatim — trailing whitespace is preserved here and
	stripped (or not) by the reader per ``#_strip_trailing_whites_#`` (§10).

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
		>>> _coerce_row(['a', 0, False, 'keep  '], '\\t')  # falsy values survive
		['a', '0', 'False', 'keep  ']
	"""
	if isinstance(row, str):
		return row.split(delimiter)
	return ['' if c is None else str(c) for c in row]


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
	_warn_loose_extension(path, lines, delimiter)
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


def delete_records(path, keys, **kwargs):
	"""Append a tombstone for each key in ``keys`` (specification §9).

	A tombstone is physically a lone-key row, so this is exactly
	``append_records(path, [[k] for k in keys])`` — but it says at the call
	site what it does. Prefer it over the bare append form, especially in a
	single-column store where *every* row is a tombstone (§9.5).

	Deleting an absent key is a harmless no-op on replay (§9.3).

	Args:
		path: Filesystem path of the part.
		keys: Iterable of keys to tombstone.
		**kwargs: Forwarded to :func:`append_records` (``create``,
			``encoding``, ``delimiter``, ``header``).

	Returns:
		None: Tombstones are appended to ``path`` as a side effect.

	Examples:
		>>> import os, tempfile
		>>> fd, path = tempfile.mkstemp(suffix='.tsvz'); os.close(fd); os.unlink(path)
		>>> append_records(path, [['a', '1'], ['b', '2']], create=True)
		>>> delete_records(path, ['a'])
		>>> dict(read_store(path))
		{'b': ['b', '2']}
		>>> os.unlink(path)
	"""
	append_records(path, [[key] for key in keys], **kwargs)


def delete_record(path, key, **kwargs):
	"""Append a tombstone for a single key (specification §9).

	Args:
		path: Filesystem path of the part.
		key: Key to tombstone.
		**kwargs: Forwarded to :func:`append_records`.

	Returns:
		None: A tombstone is appended to ``path`` as a side effect.
	"""
	delete_records(path, [key], **kwargs)


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
	lines = []
	if header:
		lines.append(format_header_comment(header, delimiter))
	if defaults:
		lines.append(format_marker_line(MARKER_DEFAULTS, defaults, delimiter))
	_warn_loose_extension(path, lines, delimiter)
	payload = ''.join(line + '\n' for line in lines).encode(encoding, errors='replace')
	_atomic_rewrite(path, payload, encoding=encoding)


def _atomic_rewrite(path, payload, *, encoding='utf8'):
	"""Replace ``path`` with ``payload`` atomically.

	Writes a sibling temporary part, fsyncs it, then :func:`os.replace`\\ s it
	over ``path``. Readers see either the whole old part or the whole new one,
	never a truncated intermediate — which matters because §3.1 makes every
	other write an append, so a half-written rewrite is the one way to lose a
	whole store.

	Args:
		path: Destination part path.
		payload: Complete new part contents, already encoded.
		encoding: Text encoding forwarded to :func:`open_part`.

	Returns:
		None: ``path`` is replaced as a side effect.
	"""
	directory = os.path.dirname(os.path.abspath(path))
	# mkstemp, not a PID-derived name: two threads snapshotting the same part
	# share a PID and would otherwise clobber each other's temporary. The
	# compression suffix is preserved so open_part picks the same codec as the
	# destination.
	fd, tmp = tempfile.mkstemp(dir=directory, prefix=f'.{os.path.basename(path)}.',
							   suffix=f'.tmp{_compression_suffix(path)}')
	os.close(fd)
	try:
		with open_part(tmp, 'wb', encoding=encoding) as f:
			f.write(payload)
			f.flush()
			with contextlib.suppress(OSError, AttributeError):
				os.fsync(f.fileno())
		os.replace(tmp, path)
	except BaseException:
		with contextlib.suppress(OSError):
			os.unlink(tmp)
		raise
	with contextlib.suppress(OSError):  # durably link the new name into the dir
		dir_fd = os.open(directory, os.O_RDONLY)
		try:
			os.fsync(dir_fd)
		finally:
			os.close(dir_fd)


def snapshot_part(path, *, encoding='utf8', delimiter=None, header=None, store=None):
	"""Materialize live state into a single part (simplified specification §19).

	Rewrites ``path``: superseded values, tombstones, and non-header comments
	are dropped. The result begins with the §19.4 marker preamble followed by
	live rows in first-appearance order (§3.4), each carrying fully resolved
	value cells.

	The rewrite is atomic — the new part is built in a sibling temporary file,
	fsynced, and then :func:`os.replace`\\ d over ``path`` — so a crash mid-
	snapshot leaves the original intact rather than a truncated store.

	Unlike :class:`WalStore`, which treats a ``#``-prefixed key as an
	in-memory-only scratch entry, this operates on what is already committed
	to disk: a key stored as ``<#>key`` per §8.3 is real data and is preserved.

	Args:
		path: Filesystem path of the part to compact.
		encoding: Text encoding for read/write.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		header: Optional header re-emitted above the preamble.
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
		>>> append_records(path, [['a']])  # tombstone the last live key
		>>> dict(snapshot_part(path))
		{}
		>>> [ln for ln in open(path).read().splitlines() if not ln.startswith('#')]
		[]
		>>> os.unlink(path)
	"""
	delimiter = delimiter or delimiter_for_path(path)
	# Replay to entries, not materialized rows: §19.4 compensation needs each
	# row's write-time defaults, which materialization has already folded away.
	replayed, state = replay_part(path, delimiter, encoding=encoding)
	data = OrderedDict(
		(key, materialize_row(entry) if isinstance(entry, StoreEntry) else entry)
		for key, entry in replayed.items())
	if store is not None:
		clear, setitem = _base_mutators(store)
		clear(store)
		for key, row in data.items():
			setitem(store, key, row)
		_attach_replay_meta(store, state)
		data = store
	lines = _snapshot_body(replayed, state, delimiter, header=header)
	_warn_loose_extension(path, lines, delimiter)
	_atomic_rewrite(path, ('\n'.join(lines) + '\n').encode(encoding, errors='replace'),
					encoding=encoding)
	return data


SnapshotResult = namedtuple(
	'SnapshotResult', 'path ordinal subsumed rotate_action data')


def rotate_parts(parts, action, *, allow_delete=False):
	"""Apply a §19.5 rotate action to the parts a snapshot superseded.

	Args:
		parts: :class:`PartName` records to act on.
		action: ``'keep'``, ``'rename'``, or ``'delete'``.
		allow_delete: §19.5 lets a conservative processor downgrade
			``delete`` -> ``rename`` -> ``keep``. This implementation takes
			that option by default: ``delete`` becomes ``rename`` unless the
			caller opts in, so a stale marker cannot destroy history.

	Returns:
		tuple: ``(effective_action, [(old_path, new_path_or_None), ...])``.

	Raises:
		ValueError: If ``action`` is not a §19.5 action.
	"""
	if action not in ROTATE_ACTIONS:
		raise ValueError(f'rotate action must be one of {ROTATE_ACTIONS}, got {action!r}')
	if action == ROTATE_DELETE and not allow_delete:
		action = ROTATE_RENAME
	changes = []
	if action == ROTATE_KEEP:
		return action, changes
	for part in parts:
		if action == ROTATE_DELETE:
			with contextlib.suppress(FileNotFoundError):
				os.unlink(part.path)
			changes.append((part.path, None))
			continue
		if part.rotated:
			continue
		store_base = f'{part.store_path}.{part.format_ext}'
		target = part_path(store_base, part.ordinal, rotated=True, codec=part.codec)
		os.replace(part.path, target)
		changes.append((part.path, target))
	return action, changes


def snapshot_store(store_path, *, encoding='utf8', delimiter=None, header=None,
				   rotate=None, allow_delete=False, quiesce=False,
				   policy='warn'):
	"""Compact a multi-part store following the full §19.2 procedure.

	Unlike :func:`snapshot_part`, which rewrites one file in place, this
	implements the race-free form: it identifies an **immutable prefix**, and
	emits the snapshot as a *new* part whose ordinal lies strictly between
	that prefix and the still-live active part. The writer is never paused and
	never notified; because the snapshot lands in an older slot than the
	active part, no regression is possible (§19.2.5).

	  ``… prefix parts … | snapshot S | active part (writer still appending)``

	The highest-ordinal part is assumed to be the active one. Pass
	``quiesce=True`` when no writer is running to fold every part into the
	snapshot instead (§19.3).

	Rows carry fully resolved values under the §19.4 preamble, so the
	compacted store reads back identically (see
	:func:`build_snapshot_preamble`).

	Args:
		store_path: Store path including its format extension.
		encoding: Text encoding for read/write.
		delimiter: Field delimiter; inferred from ``store_path`` when ``None``.
		header: Optional header comment. Off by default: §19.2.3c says a
			snapshot contains no comments.
		rotate: §19.5 action for the subsumed prefix. ``None`` uses the
			store's ``#_rotate_#`` marker (built-in default ``keep``).
		allow_delete: Permit the ``delete`` action; otherwise it is downgraded
			to ``rename``, which §19.5 explicitly sanctions.
		quiesce: Treat every part as immutable and place the snapshot above
			them all. Only safe with no live writer (§19.3).
		policy: Response to a §15 mismatch while replaying the prefix.

	Returns:
		SnapshotResult | None: ``None`` when there is nothing to compact (a
		store with no immutable prefix).

	Raises:
		FileNotFoundError: If the store has no parts.
		ValueError: If the store is a single unnumbered file (use
			:func:`snapshot_part`), or if no ordinal fits strictly between the
			prefix and the active part (§19.3).
	"""
	delimiter = delimiter or delimiter_for_path(store_path)
	parts = store_parts(store_path)
	if not parts:
		if os.path.isfile(store_path):
			raise ValueError(
				f'{store_path!r} is a single unnumbered file (§17.1); use '
				f'snapshot_part() for that case.')
		raise FileNotFoundError(store_path)

	if quiesce:
		prefix, active = parts, None
	else:
		prefix, active = parts[:-1], parts[-1]
	if not prefix:
		return None  # only the active part exists; nothing is immutable yet

	# §19.2.2: replay the immutable prefix only -- never the active part.
	replayed, state, digests = replay_parts(
		[pn.path for pn in prefix], delimiter, encoding=encoding)
	report_corruption(digests, store_path, policy)
	data = OrderedDict(
		(key, materialize_row(entry) if isinstance(entry, StoreEntry) else entry)
		for key, entry in replayed.items())

	# §19.3: slot S strictly between the prefix maximum and the active part.
	low = prefix[-1].ordinal_value
	width = max(len(pn.ordinal) for pn in parts)
	if active is None:
		ordinal_value = low + 1
	else:
		high = active.ordinal_value
		if high - low < 2:
			raise ValueError(
				f'no ordinal fits strictly between {prefix[-1].ordinal} and '
				f'{active.ordinal} (§19.3): use sparse ordinals such as '
				f'new_ordinal(), or quiesce writes and pass quiesce=True.')
		ordinal_value = low + (high - low) // 2
	ordinal = format(ordinal_value, f'0{width}x')

	lines = _snapshot_body(replayed, state, delimiter, header=header)

	target = part_path(store_path, ordinal, codec=prefix[-1].codec)
	_warn_loose_extension(target, lines, delimiter)
	# §19.2.3: write it durably before anything else changes.
	_atomic_rewrite(target, ('\n'.join(lines) + '\n').encode(encoding, errors='replace'),
					encoding=encoding)

	# §19.2.4: only once S is durable, dispose of the prefix it subsumes.
	action = state.rotate if rotate is None else rotate
	action, _changes = rotate_parts(prefix, action, allow_delete=allow_delete)
	return SnapshotResult(path=target, ordinal=ordinal, subsumed=[pn.path for pn in prefix],
						  rotate_action=action, data=data)


# ---------------------------------------------------------------------------
# Shared store behaviour
# ---------------------------------------------------------------------------

class _StoreCommon:
	"""Key, defaults, and removal semantics shared by the two store classes.

	:class:`WalStore` and :class:`OffsetStore` differ only in how they hold
	rows (in memory vs. by byte offset) and how they persist them (queued vs.
	synchronous). Everything above that — key normalization, ``#_defaults_#``
	handling, the constructor-defaults ordering rule, and ``pop``/``popitem``
	— lives here so a fix lands once rather than in two places.

	Subclasses provide the persistence hooks :meth:`_persist_row`,
	:meth:`_persist_tombstone`, and :meth:`_persist_defaults`.
	"""

	def _init_common(self, path, header, create, encoding, delimiter, defaults):
		self.path = path
		self.encoding = encoding
		self.delimiter = delimiter or delimiter_for_path(path)
		self.header = _parse_columns(header, self.delimiter)
		self.create = create
		self._reader_state = ReaderState()
		self._defaults_row = [MARKER_DEFAULTS]
		# True once no further §5.2 check is needed: either the part uses a
		# strict extension, or we have already warned about this one.
		self._loose_checked = is_strict_store(path)
		self.set_defaults(defaults)

	def _adopt_constructor_defaults(self, defaults):
		"""Re-apply and persist constructor defaults after a replay.

		``reload()`` replaces ``_reader_state`` wholesale with whatever the
		part declared, so defaults passed to ``__init__`` are otherwise
		silently discarded. They are persisted too, so the next reader
		resolves rows the same way this one does.

		Args:
			defaults: The constructor's ``defaults`` argument.

		Returns:
			None
		"""
		wanted = _normalize_defaults(defaults)
		if wanted and wanted != self._reader_state.defaults:
			self.set_defaults(wanted)
			self._persist_defaults(list(self._defaults_row))

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

	def _check_loose(self, lines):
		"""Warn once if spec-only syntax reaches a loose extension (§5.2).

		A single boolean test for the recommended strict-extension case, so
		this costs nothing on the hot write path.
		"""
		if self._loose_checked:
			return
		_warn_loose_extension(self.path, lines, self.delimiter)
		self._loose_checked = True

	def _resolve_write_ack(self, write_ack):
		"""Pick the effective §18.4 acknowledgement mode.

		An explicit constructor argument wins; otherwise the part's own
		``#_write_ack_#`` marker applies, falling back to the spec's built-in
		default of ``memory``.

		Args:
			write_ack: ``'memory'``, ``'disk'``, or ``None`` to defer to the part.

		Returns:
			str: The effective mode.

		Raises:
			ValueError: If ``write_ack`` is not a recognized mode.
		"""
		if write_ack is None:
			return self._reader_state.write_ack
		mode = str(write_ack).strip().lower()
		if mode not in WRITE_ACK_MODES:
			raise ValueError(
				f'write_ack must be one of {sorted(WRITE_ACK_MODES)}, got {write_ack!r}')
		return mode

	def get(self, key, default=None):
		"""Return ``self[key]``, or ``default`` if that would raise ``KeyError``.

		Aligned with :meth:`__getitem__`, which is the point: the C-level
		``dict.get`` bypasses it, so a store with ``return_on_missing`` on
		would otherwise answer ``None`` from ``get()`` and a synthesized
		defaults row from ``[]`` for the very same key.

		Note that while ``return_on_missing`` is enabled (§12.4's built-in
		default) a missing key resolves to a defaults row rather than
		``default``. Membership stays literal: ``key in store`` and ``len()``
		count only keys actually stored.

		Args:
			key: Key to look up.
			default: Returned only when the lookup raises ``KeyError``.

		Returns:
			list: The stored row, a synthesized defaults row, or ``default``.
		"""
		try:
			return self[key]
		except KeyError:
			return default

	@staticmethod
	def _normalize_key(key):
		"""Normalize a key to its stored identity (§10.2 trailing strip)."""
		return str(key).rstrip(' \t')

	def _prepare_row(self, key, value):
		"""Coerce an assigned value into a full ``[key, value…]`` row.

		Returns:
			list | None: The row to persist, or ``None`` when the assignment
			is a lone key and therefore a deletion (§9.2).
		"""
		row = _coerce_row(value, self.delimiter)
		if not row or row[0] != key:
			row = [key] + list(row)
		return None if len(row) == 1 else row

	def pop(self, key, *args):
		"""Remove ``key`` and persist a tombstone.

		Overrides the C ``pop`` implementations, which do not route through
		``__delitem__``.

		Args:
			key: Key to remove.
			*args: Optional default returned when ``key`` is absent.

		Returns:
			list: Removed row, or the provided default.
		"""
		key = self._normalize_key(key)
		if key in self:
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
		if not len(self):
			raise KeyError('dictionary is empty')
		key = next(reversed(self)) if last else next(iter(self))
		value = self[key]
		del self[key]
		return key, value

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		self.close()

	def _persist_row(self, row):
		raise NotImplementedError

	def _persist_tombstone(self, key):
		raise NotImplementedError

	def _persist_defaults(self, defaults_row):
		raise NotImplementedError


# ---------------------------------------------------------------------------
# WalStore — in-memory store + async append-only writer (§18)
# ---------------------------------------------------------------------------

class WalStore(_StoreCommon, OrderedDict):
	"""Ordered key→row store backed by an append-only part file.

	Mutations are enqueued for a background flusher; :meth:`flush` and
	:meth:`close` drain the pending queue to disk. Deletion appends a
	tombstone (specification §9). Prefer :meth:`pop`, :meth:`popitem`, or
	``del`` for removals — all persist to the WAL.

	Args:
		path: Filesystem path of the backing part, or -- with
			``multipart=True`` -- of the store (§17.1).
		header: Optional column names written when creating the part.
		create: If True, create a missing part on open.
		encoding: Text encoding for append I/O.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		defaults: Optional value-column defaults.
		flush_interval: Longest a queued write waits before the background
			flusher commits it; also how long a burst may coalesce.
		write_ack: §18.4 mode, ``'memory'`` or ``'disk'``. ``None`` defers to
			the part's ``#_write_ack_#`` marker.
		multipart: If True, treat ``path`` as a §17 store: replay every part
			in ordinal order, and append to a **new** part opened with a
			fresh ordinal (§17.7).

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
				 delimiter=None, defaults=None, flush_interval=0.01,
				 write_ack=None, multipart=False):
		super().__init__()
		self._pending = deque()
		self._lock = threading.Lock()
		self._wake = threading.Condition()
		self._shutdown = threading.Event()
		self._flush_error = None
		self.multipart = multipart
		self.store_path = path
		if multipart:
			# §17.7: open a fresh part on startup and treat the existing ones
			# as immutable. Appends go only to this part; replay spans them all.
			path = part_path(path, new_ordinal())
		self._init_common(path, header, create, encoding, delimiter, defaults)
		self.flush_interval = flush_interval
		self.reload()
		self._adopt_constructor_defaults(defaults)
		self.write_ack = self._resolve_write_ack(write_ack)
		self._worker = threading.Thread(target=self._flush_worker, daemon=True)
		self._worker.start()
		atexit.register(self.close)

	def reload(self):
		"""Discard in-memory state and replay the part from disk.

		Pending writes that have not yet been flushed are preserved across
		the reload.

		Replay reads into a private mapping and repopulates ``self`` through
		``OrderedDict.__setitem__``. Handing ``self`` to :func:`read_store`
		instead would route through this class's :meth:`clear` — which
		*truncates the part* — and then through :meth:`__setitem__`, re-queuing
		every replayed row as a fresh append.

		Returns:
			WalStore: ``self``, after replay.
		"""
		loaded = OrderedDict()
		try:
			if self.multipart:
				# §17.4: every part, in ordinal order, as one concatenation.
				ensure_part_exists(
					self.path, create=self.create, encoding=self.encoding,
					delimiter=self.delimiter, header=self.header or None)
				read_multipart(
					self.store_path, encoding=self.encoding,
					delimiter=self.delimiter, store=loaded)
			else:
				read_store(
					self.path, create=self.create, encoding=self.encoding,
					delimiter=self.delimiter, store=loaded, header=self.header or None,
				)
		except FileNotFoundError:
			if self.create:
				raise
		super().clear()
		for key, row in loaded.items():
			OrderedDict.__setitem__(self, key, row)
		self._reader_state = getattr(loaded, '_reader_state', self._reader_state)
		return self

	def __getitem__(self, key):
		key = self._normalize_key(key)
		try:
			return OrderedDict.__getitem__(self, key)
		except KeyError:
			if self._reader_state.return_on_missing:
				return resolve_missing_key(key, self._reader_state)
			raise

	def __setitem__(self, key, value):
		key = self._normalize_key(key)
		if not key:
			raise KeyError('empty key')  # §8.4: an empty key can never be stored
		row = self._prepare_row(key, value)
		if row is None:  # lone key -- §9.2 deletion
			del self[key]
			return
		if key == MARKER_DEFAULTS:
			self.set_defaults(row[1:])
			self._persist_defaults(list(self._defaults_row))
			return
		OrderedDict.__setitem__(self, key, row)
		self._persist_row(row)

	def _enqueue(self, item):
		"""Queue one record and wake the flusher.

		The notify is what lets the worker sleep instead of poll; it is
		unconditional because a "only notify when the queue was empty"
		optimization can lose a wakeup when two writers race.
		"""
		self._pending.append(item)
		with self._wake:
			self._wake.notify()

	def _persist_row(self, row):
		self._enqueue(list(row))

	def _persist_tombstone(self, key):
		self._enqueue((_TOMBSTONE, key))

	def _persist_defaults(self, defaults_row):
		self._enqueue(list(defaults_row))

	def __delitem__(self, key):
		key = self._normalize_key(key)
		if key == MARKER_DEFAULTS:
			self.set_defaults([])
			self._persist_defaults([MARKER_DEFAULTS])
			return
		if key not in self:
			# §9.3 makes a tombstone for an absent key a harmless no-op on
			# disk, but `del` must still honour the MutableMapping contract.
			raise KeyError(key)
		OrderedDict.__delitem__(self, key)
		self._persist_tombstone(key)

	def clear(self):
		"""Clear in-memory state and truncate the part on disk.

		The optional header comment and defaults marker are retained. The
		rewrite happens under the same lock the flusher uses, so a concurrent
		flush cannot interleave rows into the truncated part.

		Returns:
			WalStore: ``self``, after truncation.
		"""
		if self.multipart:
			# Truncating one part would not empty the store, and rewriting the
			# earlier parts would break §17.7's immutability. Append a
			# tombstone per live key instead -- append-only and correct.
			for key in list(self):
				del self[key]
			return self
		with self._lock:
			self._pending.clear()
			super().clear()
			truncate_part(self.path, encoding=self.encoding, delimiter=self.delimiter,
						  header=self.header, defaults=self._reader_state.defaults)
		return self

	def flush(self):
		"""Write pending append and tombstone lines under an exclusive lock.

		Drains at most the number of items queued when the flush began. That
		bound matters: an unbounded ``while self._pending`` drain never
		terminates when producers append faster than the writer, and because
		the drain runs inside the exclusive lock it would starve every other
		caller (``close``, a snapshot) indefinitely while the queue grows
		without bound.

		Items that cannot be written are put back at the head of the queue and
		the underlying ``OSError`` propagates, so a failed flush loses nothing.

		Returns:
			WalStore: ``self``, after draining the pending queue.

		Raises:
			OSError: If the part cannot be opened or written.
		"""
		if not self._pending:
			return self
		batch = []
		for _ in range(len(self._pending)):
			try:
				batch.append(self._pending.popleft())
			except IndexError:
				break
		if not batch:
			return self
		# §18.2/§18.3: commit the batch as one append of whole records, then a
		# single fsync (in _LockedPart.__exit__). A record is never split
		# across write() calls.
		encoded = [_queue_item_to_bytes(item, self.delimiter, self.encoding)
				   for item in batch]
		if not self._loose_checked:
			self._check_loose([b.decode(self.encoding, errors='replace').rstrip('\n')
							   for b in encoded])
		payload = b''.join(encoded)
		try:
			with self._open_locked('ab', fsync=self.write_ack == WRITE_ACK_DISK) as f:
				f.write(payload)
				f.flush()
		except OSError:
			self._pending.extendleft(reversed(batch))  # restore original order
			raise
		return self

	@property
	def last_flush_error(self):
		"""Return the last ``OSError`` a background flush hit, or ``None``.

		Background flushes cannot propagate to a caller, so the error is
		recorded here (and warned about once on stderr) instead of being
		swallowed. Cleared by a subsequent successful :meth:`flush`.

		Returns:
			OSError | None: The most recent background flush failure.
		"""
		return self._flush_error

	def close(self):
		"""Stop the background flush worker and drain the pending queue.

		Idempotent if already closed. Unregisters the ``atexit`` hook so a
		closed store is no longer pinned alive by the interpreter's exit
		registry.

		Returns:
			WalStore: ``self``.
		"""
		if self._shutdown.is_set():
			return self
		self._shutdown.set()
		with self._wake:  # a worker parked on the Condition must be woken
			self._wake.notify_all()
		if self._worker.is_alive() and self._worker is not threading.current_thread():
			self._worker.join()
		atexit.unregister(self.close)
		return self

	def __del__(self):
		with contextlib.suppress(AttributeError, RuntimeError, OSError, TypeError):
			self.close()

	def _flush_worker(self):
		"""Drain the queue, waking on writes rather than polling for them.

		Sleeps on a :class:`threading.Condition` that every write notifies, so
		an idle store costs essentially nothing (the old loop woke
		``1/flush_interval`` times a second forever -- ~100 Hz at the default).
		``flush_interval`` now means "how long a burst of writes may coalesce
		into one batch" (§18.3) and doubles as the retry backoff.
		"""
		while not self._shutdown.is_set():
			with self._wake:
				if not self._pending:
					# Timeout is only a lost-wakeup safety net.
					self._wake.wait(max(self.flush_interval, _IDLE_WAKE_SECONDS))
			if self._shutdown.is_set():
				break
			if not self._pending:
				continue
			# Let a write burst accumulate into a single append; back off
			# harder when the previous attempt failed.
			backoff = (self.flush_interval if self._flush_error is None
					   else max(self.flush_interval, _IDLE_WAKE_SECONDS))
			self._shutdown.wait(backoff)
			self._flush_quietly()
		self._flush_quietly()

	def _flush_quietly(self):
		"""Flush from the background worker, recording rather than raising.

		Catches ``Exception``, not just ``OSError``: subclasses hook extra work
		into :meth:`flush` (``TSVZed`` runs a periodic snapshot there), and
		anything escaping would kill the worker thread and silently strand
		every queued record with nobody left to drain it.
		"""
		try:
			self.flush()
		except Exception as exc:  # noqa: BLE001  # a dead flusher loses data
			first = self._flush_error is None
			self._flush_error = exc
			if first:
				print(f'TSVZ: flush to {self.path!r} failed: {exc!r}; '
					  f'{len(self._pending)} record(s) still queued', file=sys.stderr)
		else:
			self._flush_error = None

	def _open_locked(self, mode, *, fsync=True):
		"""Acquire the thread lock plus an exclusive file lock on the part.

		Args:
			mode: Open mode passed to :func:`open_part`.
			fsync: If True, fsync the part when the context exits (§18.4
				``disk``); if False, only flush to the OS (``memory``).

		Returns:
			_LockedPart: Context manager that releases both on exit.

		Raises:
			OSError: If the part cannot be opened or locked. The thread lock
				is released first, so a failure never wedges later writers.
		"""
		self._lock.acquire()
		f = None
		try:
			f = open_part(self.path, mode, encoding=self.encoding)
			if os.name == 'posix':
				fcntl.lockf(f, fcntl.LOCK_EX)
			elif os.name == 'nt':
				msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 2147483647)
		except BaseException:
			# _LockedPart never gets built, so nothing else will release these.
			if f is not None:
				with contextlib.suppress(OSError):
					f.close()
			self._lock.release()
			raise
		return _LockedPart(f, self._lock, fsync=fsync)


class _LockedPart:
	"""File wrapper that releases its lock and closes the file on exit.

	Args:
		file_obj: Open part file handle.
		lock: Threading lock held for the duration of the context.
	"""

	def __init__(self, file_obj, lock, *, fsync=True):
		self._file = file_obj
		self._lock = lock
		self._fsync = fsync

	def __enter__(self):
		return self._file

	def __exit__(self, *exc):
		# §18.3: at most one fsync per committed batch, and only when the
		# store is running write_ack='disk' (§18.4).
		try:
			self._file.flush()
			if self._fsync:
				os.fsync(self._file.fileno())
		except (OSError, ValueError, AttributeError):
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

class OffsetStore(_StoreCommon, MutableMapping):
	"""Key→byte-offset index with on-demand value materialization.

	Uses less memory than :class:`WalStore` because row contents are read
	from disk when accessed. Writes are synchronous. Intended for
	single-process use (specification §18).

	Compressed parts are rejected: an index of byte offsets into the
	*decoded* stream cannot address a compressed file, so seeking to a stored
	offset would read garbage and appending would corrupt the container. Use
	:class:`WalStore` for ``.gz``/``.bz2``/``.xz``/``.zst`` parts.

	Rows are **not** all held in memory: replay builds the offset index only,
	and values are read from disk on access through a bounded LRU cache
	(``cache_size``). Caching every row would make this class cost *more*
	than :class:`WalStore` -- the full row set plus an index -- which defeats
	the purpose of indexing by offset.

	Args:
		path: Filesystem path of the backing part.
		header: Optional column names written when creating the part.
		create: If True, create a missing part on open.
		encoding: Text encoding for append I/O.
		delimiter: Field delimiter; inferred from ``path`` when ``None``.
		defaults: Optional value-column defaults.
		write_ack: §18.4 acknowledgement mode, ``'memory'`` or ``'disk'``.
			``None`` defers to the part's ``#_write_ack_#`` marker.
		cache_size: Maximum rows retained in the LRU value cache; ``0``
			disables caching so every read goes to disk.

	Raises:
		ValueError: If ``path`` carries a compression suffix (§16), or
			``write_ack`` is not a recognized mode.
	"""

	def __init__(self, path, *, header=None, create=True, encoding='utf8',
				 delimiter=None, defaults=None, write_ack=None, cache_size=4096):
		if is_compressed_path(path):
			raise ValueError(
				f'OffsetStore cannot index a compressed part ({path!r}): byte '
				f'offsets do not address a compressed stream. Use WalStore instead.')
		self._cache_size = max(0, int(cache_size))
		self._values = OrderedDict()   # bounded LRU: key -> row
		self._offsets = {}
		self._init_common(path, header, create, encoding, delimiter, defaults)
		try:
			ensure_part_exists(self.path, create=self.create, encoding=self.encoding,
							   delimiter=self.delimiter, header=self.header)
		except FileNotFoundError:
			if self.create:
				raise
			# create=False and missing: empty in-memory index, no real file handle.
			self._file = open(os.devnull, 'r+b')  # noqa: SIM115
			self.write_ack = self._resolve_write_ack(write_ack)
			atexit.register(self.close)
			return
		# Long-lived handle; closed via close()/__exit__/atexit.
		self._file = open(self.path, 'r+b')  # noqa: SIM115
		self.reload()
		self._adopt_constructor_defaults(defaults)
		self.write_ack = self._resolve_write_ack(write_ack)
		atexit.register(self.close)

	def reload(self):
		"""Rebuild the offset index by replaying the part from disk.

		Returns:
			OffsetStore: ``self``, after replay.
		"""
		self._offsets.clear()
		self._values.clear()
		offsets, _values, state = read_offsets(
			self.path, create=self.create, encoding=self.encoding,
			delimiter=self.delimiter, cache_values=False)
		self._offsets.update(offsets)
		self._reader_state = state
		return self

	def _cache_put(self, key, row):
		"""Insert ``row`` into the bounded LRU value cache."""
		if not self._cache_size:
			return
		self._values[key] = row
		self._values.move_to_end(key)
		while len(self._values) > self._cache_size:
			self._values.popitem(last=False)

	def _cache_get(self, key):
		"""Return the cached row for ``key`` and mark it recently used."""
		row = self._values.get(key)
		if row is not None:
			self._values.move_to_end(key)
		return row

	def flush(self, *, fsync=None):
		"""Push buffered appends to the OS, fsyncing per §18.4.

		Writes are otherwise only visible to other readers once the handle is
		closed, since :class:`OffsetStore` keeps one long-lived buffered
		handle open for the life of the store.

		Args:
			fsync: Override the store's ``write_ack`` mode for this call.
				``None`` fsyncs only when ``write_ack == 'disk'``.

		Returns:
			OffsetStore: ``self``.
		"""
		if self._file.closed:
			return self
		self._file.flush()
		if fsync is None:
			fsync = self.write_ack == WRITE_ACK_DISK
		if fsync:
			with contextlib.suppress(OSError, ValueError):
				os.fsync(self._file.fileno())
		return self

	def _append_line(self, line):
		self._check_loose([line])
		self._file.seek(0, os.SEEK_END)
		pos = self._file.tell()
		# §18.2: one whole record, terminated, in a single write().
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
		if key is not None:
			cached = self._cache_get(key)
			if cached is not None:
				return list(cached)
		if not isinstance(offset, int):
			raise TypeError(f'offset index for {key!r} is {type(offset).__name__}, not int')
		self._file.flush()  # buffered appends must be on disk before we seek back
		self._file.seek(offset)
		line = self._file.readline().decode(self.encoding, errors='replace')
		if line.endswith('\n'):  # §4.2: at most one \r immediately before the \n
			line = line[:-1]
			if line.endswith('\r'):  # noqa: FURB188  # removesuffix needs 3.9+
				line = line[:-1]
		scratch = OrderedDict()
		kind, entry = process_record(line, self._reader_state.copy(), scratch, self.delimiter)
		if kind == 'data' and entry is not None:
			row = materialize_row(entry)
			if key is not None:
				self._cache_put(key, list(row))
			return row
		if key is not None:
			raise KeyError(key)
		return []

	def __getitem__(self, key):
		key = self._normalize_key(key)
		if key == MARKER_DEFAULTS:
			return self.defaults
		if key not in self._offsets:
			if self._reader_state.return_on_missing:
				return resolve_missing_key(key, self._reader_state)
			raise KeyError(key)
		return self._read_at(self._offsets[key], key)

	def __setitem__(self, key, value):
		key = self._normalize_key(key)
		if not key:
			raise KeyError('empty key')  # §8.4: an empty key can never be stored
		row = self._prepare_row(key, value)
		if row is None:  # lone key -- §9.2 deletion
			del self[key]
			return
		if key == MARKER_DEFAULTS:
			self.set_defaults(row[1:])
			self._persist_defaults(list(self._defaults_row))
			return
		self._offsets[key] = self._persist_row(row)
		self._cache_put(key, list(row))

	def __delitem__(self, key):
		key = self._normalize_key(key)
		if key == MARKER_DEFAULTS:
			self.set_defaults([])
			self._persist_defaults([MARKER_DEFAULTS])
			return
		if key not in self._offsets:
			raise KeyError(key)
		self._offsets.pop(key, None)
		self._values.pop(key, None)
		self._persist_tombstone(key)

	def _persist_row(self, row):
		return self._write_row(row)

	def _persist_tombstone(self, key):
		self._write_row([key])

	def _persist_defaults(self, defaults_row):
		self._write_row(list(defaults_row))

	def __iter__(self):
		return iter(self._offsets)

	def __len__(self):
		return len(self._offsets)

	def __contains__(self, key):
		return self._normalize_key(key) in self._offsets

	def clear(self):
		"""Truncate the part, retaining the header comment and defaults marker.

		Returns:
			OffsetStore: ``self``, after truncation.
		"""
		self._offsets.clear()
		self._values.clear()
		self._file.seek(0)
		self._file.truncate()
		if self.header:
			self._append_line(format_header_comment(self.header, self.delimiter))
		if self._reader_state.defaults:
			self._write_row(list(self._defaults_row))
		self.flush(fsync=True)
		return self

	def close(self):
		"""Flush buffered appends and release the part handle.

		Idempotent. Unregisters the ``atexit`` hook so a closed store is not
		pinned alive by the exit registry.

		Returns:
			OffsetStore: ``self``.
		"""
		if not self._file.closed:
			self.flush(fsync=True)  # closing is a durability point either way
			self._file.close()
		atexit.unregister(self.close)
		return self


# ---------------------------------------------------------------------------
# Legacy API (TSVZ.py / pre-4.0 TSVZ_new names and kwargs)
# ---------------------------------------------------------------------------

DEFAULTS_INDICATOR_KEY = MARKER_DEFAULTS
build_scrub_preamble = build_snapshot_preamble
openFileAsCompressed = open_part

#: Release in which the legacy names below are scheduled for removal.
LEGACY_REMOVAL_VERSION = '5.0'


def _deprecated(replacement):
	"""Mark a legacy entry point as deprecated in favour of ``replacement``.

	Emits :class:`DeprecationWarning` at the caller's stack level on first
	use, and appends a note to the wrapped object's docstring.

	Args:
		replacement: Name of the supported API to use instead.

	Returns:
		callable: Decorator applying the warning.
	"""
	def decorate(obj):
		note = (f'\n\n.. deprecated:: {__version__}\n'
				f'\tUse :obj:`{replacement}` instead; scheduled for removal in '
				f'TSVZ {LEGACY_REMOVAL_VERSION}.\n')
		if isinstance(obj, type):
			original_init = obj.__init__

			def __init__(self, *args, **kwargs):
				warnings.warn(
					f'{obj.__name__} is deprecated; use {replacement} instead. '
					f'It will be removed in TSVZ {LEGACY_REMOVAL_VERSION}.',
					DeprecationWarning, stacklevel=2)
				original_init(self, *args, **kwargs)
			__init__.__doc__ = original_init.__doc__
			obj.__init__ = __init__
			obj.__doc__ = (obj.__doc__ or '') + note
			return obj

		@functools.wraps(obj)
		def wrapper(*args, **kwargs):
			warnings.warn(
				f'{obj.__name__}() is deprecated; use {replacement}() instead. '
				f'It will be removed in TSVZ {LEGACY_REMOVAL_VERSION}.',
				DeprecationWarning, stacklevel=2)
			return obj(*args, **kwargs)
		wrapper.__doc__ = (obj.__doc__ or '') + note
		return wrapper
	return decorate


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
		# unicode_escape round-trips through latin-1, so it mangles any
		# non-ASCII delimiter ('§' -> 'Â§'). Only unescape pure ASCII.
		if not delimiter.isascii():
			return delimiter
		try:
			return delimiter.encode().decode('unicode_escape')
		except UnicodeError:
			return delimiter
	return delimiter

@_deprecated('delimiter_for_path')
def get_delimiter(delimiter=..., file_name=''):
	"""Legacy alias for :func:`_legacy_delimiter` / :func:`delimiter_for_path`.

	Args:
		delimiter: Explicit delimiter, symbolic name, or ``...`` to infer.
		file_name: Path used for inference when ``delimiter`` is ``...``.

	Returns:
		str: Concrete field delimiter.
	"""
	return _legacy_delimiter(delimiter=delimiter, file_name=file_name)

@_deprecated('read_last_record')
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

@_deprecated('read_store')
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
		>>> import os, tempfile, warnings
		>>> fd, path = tempfile.mkstemp(suffix='.tsv'); os.close(fd)
		>>> open(path, 'w').write('a\\t1\\n')
		4
		>>> with warnings.catch_warnings():
		...     _ = warnings.simplefilter('ignore', DeprecationWarning)
		...     dict(readTabularFile(path, header=['id', 'v', 'extra'], strict=False))
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
	defs = defaults if defaults is not ... else None
	try:
		if lastLineOnly:
			ensure_part_exists(fileName, create=createIfNotExist, encoding=encoding,
							   delimiter=d, header=header_cols or None,
							   defaults=_normalize_defaults(defs))
			result = read_last_record(fileName, encoding=encoding, delimiter=d,
									  store_offset=storeOffset)
		elif storeOffset:
			result, _values, _state = read_offsets(
				fileName, create=createIfNotExist, encoding=encoding, delimiter=d,
				defaults=defs, store=store, header=header_cols or None,
			)
		else:
			result = read_store(
				fileName, create=createIfNotExist, encoding=encoding, delimiter=d,
				defaults=defs, store=store, header=header_cols or None,
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
		# Assign through the base mapping: a live store's __setitem__ would
		# turn this read into a burst of WAL appends.
		setter = OrderedDict.__setitem__ if isinstance(result, OrderedDict) else type(result).__setitem__
		for key in list(result.keys()):
			setter(result, key, _fit_row(result[key], cols))
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

@_deprecated('append_records')
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

@_deprecated('append_record')
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
	_ = (teeLogger, verifyHeader, verbose, strict)
	append_record(
		fileName, lineToAppend, create=createIfNotExist, encoding=encoding,
		delimiter=_legacy_delimiter(delimiter=delimiter, file_name=fileName),
		header=header or None,
	)

@_deprecated('truncate_part')
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

@_deprecated('snapshot_part')
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
		return read_last_record(fileName, encoding=encoding, delimiter=d)
	return snapshot_part(
		fileName, encoding=encoding, delimiter=d, header=header or None, store=taskDic,
	)

def _list_view(tsvzDic, header=None, delimiter=...):
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
		>>> _list_view({'a': ['a', '1']}, header=['id', 'v'])
		[['id', 'v'], ['a', '1']]
		>>> _list_view({})
		[]
	"""
	if header is None:
		header = []
	d = _legacy_delimiter(delimiter=delimiter)
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

getListView = _deprecated('dict(store).values()')(_list_view)
readTSV = readTabularFile
appendTSV = appendTabularFile
clearTSV = clearTabularFile
scrubTSV = scrubTabularFile

@_deprecated('WalStore')
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
		return _list_view(self, header=self.header, delimiter=self.delimiter)

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

		The drain and the rewrite both run under the store's write lock, so a
		background flush cannot land rows in the part between the snapshot's
		read and its atomic replace — which would otherwise leave data rows
		ahead of the marker preamble, resolved under the wrong forward-only
		marker state (§12.1, §19.2).

		Returns:
			bool: True on success, or False if a rewrite is already in
			progress.
		"""
		if self._rewriting:
			return False
		self._rewriting = True
		try:
			WalStore.flush(self)
			with self._lock:
				data = snapshot_part(
					self.path, encoding=self.encoding, delimiter=self.delimiter,
					header=self.header or None,
				)
				self._last_rewrite = time.monotonic()
				# Repopulate through OrderedDict so the refresh is not itself
				# re-queued as a fresh batch of appends.
				super(WalStore, self).clear()
				for key, row in data.items():
					OrderedDict.__setitem__(self, key, row)
				self._reader_state = getattr(data, '_reader_state', self._reader_state)
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


@_deprecated('OffsetStore')
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
		# Build from the index, not the LRU cache: the cache is bounded and
		# holds only recently touched rows.
		rows = OrderedDict((key, self[key]) for key in self._offsets)
		return _list_view(rows, header=self.header, delimiter=self.delimiter)

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
		if is_compressed_path(newFileName):
			raise ValueError(
				f'OffsetStore cannot index a compressed part ({newFileName!r}).')
		self.flush()
		self._file.close()
		self.path = newFileName
		self._fileName = newFileName
		if createIfNotExist is not ...:
			self.create = createIfNotExist
		if verifyHeader is not ...:
			self.verifyHeader = verifyHeader
		ensure_part_exists(self.path, create=self.create, encoding=self.encoding,
						   delimiter=self.delimiter, header=self.header)
		self._file = open(self.path, 'r+b')  # noqa: SIM115
		self.reload()
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
	except ImportError:
		pass  # standalone install: fall through to the built-in formatter
	else:
		return multiCMD.pretty_format_table(data, delimiter=delimiter)
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

	Supported operations: ``read``, ``append``, ``delete``, ``clear``,
	``scrub`` (snapshot/compact), ``verify`` (§15 integrity), and ``parts``
	(list a §17 store's parts).

	Returns:
		int: Process exit status — ``0`` on success, ``1`` when ``--strict``
		and the part is missing, ``2`` for a usage error.
	"""
	import argparse
	parser = argparse.ArgumentParser(description='TSVZ: append-only tabular key–value store (tsvz-spec-v1)')
	parser.add_argument('filename', type=str, help='The file to read')
	parser.add_argument(
		'operation', type=str, nargs='?',
		choices=['read', 'append', 'delete', 'clear', 'scrub', 'verify', 'parts'],
		help='Operation to perform. scrub = snapshot/compact; verify = check '
			 '§15 checksums; parts = list the §17 parts. Default: read',
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
	strict_mode.add_argument(
		'-s', '--strict', dest='strict', action='store_true',
		help='Fail with a non-zero exit status when the part does not exist.')
	strict_mode.add_argument(
		'-f', '--force', dest='strict', action='store_false',
		help='Treat a missing part as empty instead of an error (default).')
	parser.set_defaults(strict=False)
	parser.add_argument(
		'-m', '--multipart', action='store_true',
		help='Treat FILENAME as a §17 multi-part store rather than one part.')
	parser.add_argument(
		'--checksum', metavar='ALGO',
		help='With append: also close a §15 segment with this digest.')
	parser.add_argument('-V', '--version', action='version',
						version=f'%(prog)s {version} @ {COMMIT_DATE} by {author}')
	try:
		import argcomplete
		argcomplete.autocomplete(parser, always_complete_options='long')
	except ImportError:
		pass
	args = parser.parse_args()
	args.delimiter = _legacy_delimiter(delimiter=args.delimiter, file_name=args.filename)

	def _unescape(text):
		"""Expand \\t / \\n style escapes in a CLI argument, ASCII only."""
		if not text or not text.isascii():
			return text
		try:
			return text.encode().decode('unicode_escape')
		except UnicodeError:
			return text

	header = _unescape(args.header) if args.header else ''
	defaults = _unescape(args.defaults).split(args.delimiter) if args.defaults else []
	# Field values get the same treatment as --header/--defaults, so
	# `tsvz f.tsvz append k 'a\tb'` means the same thing everywhere.
	args.line = [_unescape(field) for field in args.line]

	if args.operation == 'parts':
		for path in store_part_paths(args.filename, include_rotated=True):
			parsed = parse_part_name(path)
			tag = ' (rotated)' if parsed is not None and parsed.rotated else ''
			print(f'{path}{tag}')
		return 0
	if args.operation == 'verify':
		paths = (store_part_paths(args.filename) if args.multipart
				 else [args.filename])
		if not paths:
			print(f'tsvz: no such store: {args.filename}', file=sys.stderr)
			return 1
		digests = DigestSet()
		for path in paths:
			replay_part(path, args.delimiter, digests=digests)
		if digests.mismatches:
			for algo, want, got in digests.mismatches:
				print(f'tsvz: {algo} mismatch: expected {want}, computed {got}',
					  file=sys.stderr)
			return 1
		scope = 'store' if args.multipart else 'part'
		print(f'{digests.verified} segment(s) verified across {len(paths)} '
			  f'{scope} file(s); {len(digests.armed)} algorithm(s) armed')
		return 0
	if args.operation == 'read':
		try:
			reader = read_multipart if args.multipart else read_store
			data = reader(args.filename, delimiter=args.delimiter)
		except FileNotFoundError:
			if args.strict:
				print(f'tsvz: no such part: {args.filename}', file=sys.stderr)
				return 1
			return 0
		formatted = _cli_pretty_format_table(data.values(), delimiter=args.delimiter)
		print(formatted, end='' if formatted.endswith('\n') else '\n')
	elif args.operation == 'append':
		append_record(
			args.filename, args.line, create=True, header=header or None,
			delimiter=args.delimiter,
		)
		if args.checksum:
			append_checksum(args.filename, args.checksum, delimiter=args.delimiter)
	elif args.operation == 'delete':
		if not args.line:
			print('delete needs a key', file=sys.stderr)
			return 2
		# Spec §9: tombstone = lone key (no value fields).
		append_record(
			args.filename, args.line[:1], create=True, header=header or None,
			delimiter=args.delimiter,
		)
	elif args.operation == 'clear':
		truncate_part(
			args.filename, delimiter=args.delimiter, header=header or None,
			defaults=defaults or None,
		)
	elif args.operation == 'scrub':
		if args.multipart:
			result = snapshot_store(args.filename, delimiter=args.delimiter,
									header=header or None)
			if result is None:
				print('tsvz: nothing to compact (no immutable prefix)',
					  file=sys.stderr)
				return 0
			print(f'{result.path} ({len(result.subsumed)} part(s) subsumed, '
				  f'rotate={result.rotate_action})')
		else:
			snapshot_part(args.filename, delimiter=args.delimiter, header=header or None)
	else:
		print('Invalid operation', file=sys.stderr)
		return 2
	return 0


if __name__ == '__main__':
	# ``python TSVZ_new.py --doctest`` or ``python -m doctest TSVZ_new.py``.
	if len(sys.argv) > 1 and sys.argv[1] in ('--doctest', '-t'):
		import doctest
		failures, _ = doctest.testmod(optionflags=doctest.ELLIPSIS)
		sys.exit(1 if failures else 0)
	sys.exit(__main__())
