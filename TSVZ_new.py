#! /usr/bin/env python3
"""
TSVZ — tsvz-spec-v1.md core implementation (§4–§14, §16).

Append-only WAL semantics; compaction is ``snapshot_part`` (simplified §19), not
in-place rewrite. Spec-only on read (no legacy escaping or tombstone rules).
"""
import atexit
import io
import os
import re
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
	__slots__ = ('version', 'defaults', 'strip_trailing', 'fill_empty', 'return_on_missing')

	def __init__(self):
		self.version = 1
		self.defaults = []
		self.strip_trailing = True
		self.fill_empty = False
		self.return_on_missing = True

	def copy(self):
		s = ReaderState()
		s.version = self.version
		s.defaults = list(self.defaults)
		s.strip_trailing = self.strip_trailing
		s.fill_empty = self.fill_empty
		s.return_on_missing = self.return_on_missing
		return s


class StoreEntry:
	__slots__ = ('row', 'row_defaults', 'fill_empty')

	def __init__(self, row, row_defaults, fill_empty):
		self.row = row
		self.row_defaults = list(row_defaults)
		self.fill_empty = fill_empty


def _strip_field(raw, enabled):
	return raw if not enabled else raw.rstrip(' \t')


def decode_field(raw, delimiter):
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
	if not s:
		return None
	s = s.strip().lower()
	if s == 'true':
		return True
	if s == 'false':
		return False
	return None


def classify_record(f0_raw):
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
	if not data:
		return b''
	last_nl = data.rfind(b'\n')
	return b'' if last_nl == -1 else data[:last_nl + 1]


def _resolve_value_columns(fields, state, delimiter):
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
		if line.endswith('\r'):
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
	return delimiter.join(encode_field(f, delimiter, is_key=(i == 0)) for i, f in enumerate(fields))


def format_tombstone(key, delimiter):
	return encode_field(key, delimiter, is_key=True)


def format_marker_line(marker_key, values, delimiter):
	parts = [marker_key] + [encode_field(v, delimiter) for v in values]
	return delimiter.join(parts)


def format_header_comment(columns, delimiter):
	if not columns:
		return ''
	encoded = [encode_field(c, delimiter) for c in columns]
	encoded[0] = '#' + encoded[0]
	return delimiter.join(encoded)


def build_snapshot_preamble(state, delimiter):
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
	lower = _strip_compression_suffix(path)
	return any(lower.endswith(ext) for ext in STRICT_EXTENSIONS)


def delimiter_for_path(path, delimiter=None):
	if delimiter is not None:
		return delimiter or DEFAULT_DELIMITER
	lower = _strip_compression_suffix(path)
	if lower.endswith('.csv') or lower.endswith('.csvz'):
		return ','
	if lower.endswith('.nsv') or lower.endswith('.nsvz'):
		return '\0'
	if lower.endswith('.psv') or lower.endswith('.psvz'):
		return '|'
	return DEFAULT_DELIMITER


def open_part(path, mode='rb', *, encoding='utf8', compress_level=1):
	lower = path.lower()
	if 'b' not in mode:
		mode += 't'
	kwargs = {}
	if 'r' not in mode:
		if lower.endswith('.xz'):
			kwargs['preset'] = compress_level
		elif lower.endswith('.zst') or lower.endswith('.zstd'):
			kwargs['level'] = compress_level
		else:
			kwargs['compresslevel'] = compress_level
	if 'b' not in mode:
		kwargs['encoding'] = encoding
	if lower.endswith('.xz') or lower.endswith('.lzma'):
		import lzma
		return lzma.open(path, mode, **kwargs)
	if lower.endswith('.gz') or lower.endswith('.gzip'):
		import gzip
		return gzip.open(path, mode, **kwargs)
	if lower.endswith('.bz2') or lower.endswith('.bzip2'):
		import bz2
		return bz2.open(path, mode, **kwargs)
	if lower.endswith('.zst') or lower.endswith('.zstd'):
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
	"""Return the last committed data row, or its byte offset when ``store_offset=True``."""
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
		if line.endswith('\r'):
			line = line[:-1]
		if line:
			scratch = OrderedDict()
			kind, entry = process_record(line, state, scratch, delimiter)
			if kind == 'data' and entry is not None:
				last_offset = pos
				result = pos if store_offset else list(entry.row)
		pos = nl + 1
	return last_offset if store_offset else result


def read_store(path, *, create=False, encoding='utf8', delimiter=None,
			   defaults=None, store=None, store_offset=False, last_record_only=False):
	"""Replay a part into an ordered mapping (key → row list or byte offset)."""
	delimiter = delimiter or delimiter_for_path(path)
	if store is None:
		store = OrderedDict()
	ensure_part_exists(path, create=create, encoding=encoding, delimiter=delimiter,
					   defaults=_normalize_defaults(defaults))
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
	if isinstance(row, str):
		return row.split(delimiter)
	return [str(c).rstrip() if c else '' for c in row]


def append_records(path, rows, *, create=False, encoding='utf8', delimiter=None):
	delimiter = delimiter or delimiter_for_path(path)
	ensure_part_exists(path, create=create, encoding=encoding, delimiter=delimiter)
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
	append_records(path, [row], **kwargs)


def truncate_part(path, *, encoding='utf8', delimiter=None, header=None, defaults=None):
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
	"""Materialize live state into a single part (simplified §19; in-place)."""
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
	"""Ordered key→row store backed by an append-only part."""

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
		vals = _normalize_defaults(defaults)
		self._defaults_row = [MARKER_DEFAULTS] + vals if vals else [MARKER_DEFAULTS]
		self._reader_state.defaults = list(vals)

	@property
	def defaults(self):
		return list(self._defaults_row)

	def reload(self):
		prev = self._pending
		self._pending = deque()
		super().clear()
		read_store(
			self.path, create=self.create, encoding=self.encoding,
			delimiter=self.delimiter, store=self,
		)
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

	def clear(self):
		super().clear()
		truncate_part(self.path, encoding=self.encoding, delimiter=self.delimiter,
					  header=self.header, defaults=self._reader_state.defaults)
		return self

	def flush(self):
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
		try:
			self.close()
		except Exception:
			pass

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
	"""Key→file-offset index; values read from disk on demand."""

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
		ensure_part_exists(self.path, create=self.create, encoding=self.encoding,
						   delimiter=self.delimiter, header=self.header)
		self._file = open(self.path, 'r+b')
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
	"""Map old get_delimiter(...) calling conventions."""
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
		except Exception:
			return delimiter
	return delimiter

def get_delimiter(delimiter=..., file_name=''):
	return _legacy_delimiter(delimiter=delimiter, file_name=file_name)

def read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=False, teeLogger=None,
						 strict=False, encoding='utf8', delimiter=..., defaults=...,
						 storeOffset=False):
	_ = (taskDic, correctColumnNum, verbose, teeLogger, strict, defaults)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	return read_last_record(
		fileName, encoding=encoding, delimiter=d, store_offset=storeOffset,
	)

def readTabularFile(fileName, teeLogger=None, header='', createIfNotExist=False,
					lastLineOnly=False, verifyHeader=True, verbose=False, taskDic=None,
					encoding='utf8', strict=True, delimiter=..., defaults=...,
					correctColumnNum=-1, storeOffset=False):
	_ = (teeLogger, verifyHeader, verbose, strict, correctColumnNum)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	store = taskDic if taskDic is not None else OrderedDict()
	result = read_store(
		fileName, create=createIfNotExist, encoding=encoding, delimiter=d,
		defaults=defaults if defaults is not ... else None,
		store=store, store_offset=storeOffset, last_record_only=lastLineOnly,
	)
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
	_ = (teeLogger, header, verifyHeader, verbose, strict)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	append_records(fileName, linesToAppend, create=createIfNotExist, encoding=encoding, delimiter=d)

def appendTabularFile(fileName, lineToAppend, teeLogger=None, header='', createIfNotExist=False,
					  verifyHeader=True, verbose=False, encoding='utf8', strict=True, delimiter=...):
	appendLinesTabularFile(
		fileName, [lineToAppend], teeLogger=teeLogger, header=header,
		createIfNotExist=createIfNotExist, verifyHeader=verifyHeader,
		verbose=verbose, encoding=encoding, strict=strict, delimiter=delimiter,
	)

def clearTabularFile(fileName, teeLogger=None, header='', verifyHeader=False, verbose=False,
					 encoding='utf8', strict=False, delimiter=..., defaults=...):
	_ = (teeLogger, verifyHeader, verbose, strict)
	d = _legacy_delimiter(delimiter=delimiter, file_name=fileName)
	defs = _normalize_defaults(defaults if defaults is not ... else None)
	truncate_part(fileName, encoding=encoding, delimiter=d, header=header, defaults=defs)

def scrubTabularFile(fileName, teeLogger=None, header='', createIfNotExist=False,
					 lastLineOnly=False, verifyHeader=True, verbose=False, taskDic=None,
					 encoding='utf8', strict=False, delimiter=..., defaults=...,
					 correctColumnNum=-1):
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

def getListView(tsvzDic, header=[], delimiter=...):
	d = get_delimiter(delimiter=delimiter)
	if header:
		if isinstance(header, str):
			header = header.split(d)
		elif not isinstance(header, list):
			try:
				header = list(header)
			except Exception:
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
	"""Legacy wrapper around :class:`WalStore` (append-only; rewrite kwargs ignored)."""

	def __init__(self, fileName, teeLogger=None, header='', createIfNotExist=True,
				 verifyHeader=True, rewrite_on_load=False, rewrite_on_exit=False,
				 rewrite_interval=0, append_check_delay=0.01, monitor_external_changes=True,
				 verbose=False, encoding='utf8', delimiter=..., defaults=None,
				 strict=False, correctColumnNum=-1):
		_ = (teeLogger, verifyHeader, rewrite_on_load, rewrite_on_exit, rewrite_interval,
			 monitor_external_changes, verbose, strict, correctColumnNum)
		d = None if delimiter is ... else _legacy_delimiter(delimiter=delimiter, file_name=fileName)
		super().__init__(
			fileName, header=header or None, create=createIfNotExist,
			encoding=encoding, delimiter=d, defaults=defaults,
			flush_interval=append_check_delay,
		)
		self._fileName = fileName
		self.teeLogger = teeLogger
		self.verifyHeader = verifyHeader
		self.verbose = verbose
		self.strict = strict
		self.correctColumnNum = correctColumnNum
		self.appendQueue = self._pending

	def commitAppendToFile(self):
		return self.flush()

	def stopAppendThread(self):
		return self.close()

	def clear_file(self):
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
		"""No-op; use :func:`snapshot_part` / :meth:`hardMapToFile` instead."""
		return False

	def hardMapToFile(self):
		return snapshot_part(
			self.path, encoding=self.encoding, delimiter=self.delimiter,
			header=self.header or None,
		)

	mapToFile = hardMapToFile

	def checkExternalChanges(self):
		return self

class TSVZedLite(OffsetStore):
	"""Legacy wrapper around :class:`OffsetStore`."""

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
		self._file.close()
		self.path = newFileName
		self._fileName = newFileName
		if createIfNotExist is not ...:
			self.create = createIfNotExist
		if verifyHeader is not ...:
			self.verifyHeader = verifyHeader
		self.reload()
		self._file = open(self.path, 'r+b')
		return self
