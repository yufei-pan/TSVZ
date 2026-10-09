#! /usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
# /// script
# requires-python = ">=3.6"
# dependencies = [
# ]
# ///
"""TSVZ 4.1: a key-value store kept in a delimiter-separated text file.

Plain ``.tsv`` / ``.csv`` / ``.nsv`` / ``.psv`` files (and any other
extension) keep the exact TSVZ 3.39 format. ``.tsvz`` / ``.csvz`` / ``.nsvz``
/ ``.psvz`` files follow tsvz-spec-v1 (tsvz-spec-v1.md). Both use the same
API: ``TSVZed``, ``TSVZedLite``, ``readTabularFile`` and friends.
"""
import atexit
import bisect
import codecs
import contextlib
import functools
import hashlib
import io
import os
import re
import shutil
import threading
import time
import sys
from collections import OrderedDict, deque, namedtuple
from collections.abc import MutableMapping
RESOURCE_LIB_AVAILABLE = True
try:
	import resource
except ImportError:
	RESOURCE_LIB_AVAILABLE = False

if os.name == 'nt':
	import msvcrt
elif os.name == 'posix':
	import fcntl

version = '4.1'
__version__ = version
author = 'pan@zopyr.us'
COMMIT_DATE = '2026-10-07'

DEFAULT_DELIMITER = '\t'
DEFAULTS_INDICATOR_KEY = '#_defaults_#'

COMPRESSED_FILE_EXTENSIONS = ['gz','gzip','bz2','bzip2','xz','lzma','zst','zstd']

def _isCompressedFile(fileName):
	return fileName.rpartition('.')[2].lower() in COMPRESSED_FILE_EXTENSIONS

def get_delimiter(delimiter = ...,file_name = ''):
	"""
	Resolve the delimiter to use for an operation.

	When the delimiter is not provided (the ``...`` sentinel or a falsy value),
	it is determined automatically: inferred from the file name when one is
	available (a trailing compression extension such as ``.gz`` is ignored for
	the purpose of inference), otherwise falling back to the module default
	(``DEFAULT_DELIMITER``, a tab). Explicit delimiters (including the aliases
	'comma', 'tab', 'pipe', 'null' and escape sequences) are normalized.

	This function is pure: it has no side effects and never mutates module
	state, so concurrent callers operating on different file types do not
	interfere with one another.

	Parameters:
	- delimiter: The delimiter or alias, or ``...``/``None`` to auto-determine.
	- file_name (str, optional): The file name to infer the delimiter from.

	Returns:
	str: The resolved delimiter.
	"""
	if delimiter is ...:
		if not file_name:
			# Nothing to infer from; fall back to the module default.
			return DEFAULT_DELIMITER
		name = _parsePartName(file_name)
		if name.ext in STRICT_EXTENSIONS:
			return _EXTENSION_DELIMITERS[name.ext]
		# Ignore a trailing compression extension so 'data.csv.gz' still
		# infers ',' rather than the tab fallback.
		lowerName = file_name.lower()
		base, _, ext = lowerName.rpartition('.')
		if ext in COMPRESSED_FILE_EXTENSIONS:
			lowerName = base
		if lowerName.endswith('.csv'):
			return ','
		elif lowerName.endswith('.nsv'):
			return '\0'
		elif lowerName.endswith('.psv'):
			return '|'
		else:
			return '\t'
	elif not delimiter:
		return DEFAULT_DELIMITER
	elif delimiter == 'comma':
		return ','
	elif delimiter == 'tab':
		return '\t'
	elif delimiter == 'pipe':
		return '|'
	elif delimiter == 'null':
		return '\0'
	else:
		return delimiter.encode().decode('unicode_escape')

def eprint(*args, **kwargs):
	try:
		if 'file' in kwargs:
			print(*args, **kwargs)
		else:
			print(*args, file=sys.stderr, **kwargs)
	except Exception as e:
		print(f"Error: Cannot print to stderr: {e}")
		print(*args, **kwargs)

def openFileAsCompressed(fileName,mode = 'rb',encoding = 'utf8',teeLogger = None,compressLevel = 1):
	lowerFileName = fileName.lower()
	if 'b' not in mode:
		mode += 't'
	kwargs = {}
	if 'r' not in mode:
		if lowerFileName.endswith('.xz'):
			kwargs['preset'] = compressLevel
		elif lowerFileName.endswith('.zst') or lowerFileName.endswith('.zstd'):
			kwargs['level'] = compressLevel
		else:
			kwargs['compresslevel'] = compressLevel
	if 'b' not in mode:
		kwargs['encoding'] = encoding
	if lowerFileName.endswith('.xz') or lowerFileName.endswith('.lzma'):
		try:
			import lzma
			return lzma.open(fileName, mode, **kwargs)
		except Exception:
			__teePrintOrNot(f"Failed to open {fileName} with lzma, trying bin",teeLogger=teeLogger)
	elif lowerFileName.endswith('.gz') or lowerFileName.endswith('.gzip'):
		try:
			import gzip
			return gzip.open(fileName, mode, **kwargs)
		except Exception:
			__teePrintOrNot(f"Failed to open {fileName} with gzip, trying bin",teeLogger=teeLogger)
	elif lowerFileName.endswith('.bz2') or lowerFileName.endswith('.bzip2'):
		try:
			import bz2
			return bz2.open(fileName, mode, **kwargs)
		except Exception:
			__teePrintOrNot(f"Failed to open {fileName} with bz2, trying bin",teeLogger=teeLogger)
	elif lowerFileName.endswith('.zst') or lowerFileName.endswith('.zstd'):
		try:
			from compression import zstd
			return zstd.open(fileName, mode, **kwargs)
		except Exception:
			__teePrintOrNot(f"Failed to open {fileName} with zstd, trying bin",teeLogger=teeLogger)
	if 't' in mode:
		mode = mode.replace('t','')
		return open(fileName, mode, encoding=encoding)
	if 'b' not in mode:
		mode += 'b'
	return open(fileName, mode)
	
def get_terminal_size():
	'''
	Get the terminal size

	@params:
		None

	@returns:
		(int,int): the number of columns and rows of the terminal
	'''
	try:
		import os
		_tsize = os.get_terminal_size()
	except Exception:
		try:
			import fcntl
			import struct
			import termios
			packed = fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack('HHHH', 0, 0, 0, 0))
			_tsize = struct.unpack('HHHH', packed)[:2]
		except Exception:
			import shutil
			_tsize = shutil.get_terminal_size(fallback=(240, 50))
	return _tsize

def pretty_format_table(data, delimiter="\t", header=None, full=False):
	version = 1.12
	_ = version
	def visible_len(s):
		return len(re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", s))
	def table_width(col_widths, sep_len):
		# total width = sum of column widths + separators between columns
		return sum(col_widths) + sep_len * (len(col_widths) - 1)
	def truncate_to_width(s, width):
		# If fits, leave as is. If too long and width >= 1, keep width-1 chars + "."
		# If width == 0, nothing fits; return empty string.
		if visible_len(s) <= width:
			return s
		if width <= 0:
			return ""
		# Build a truncated plain string based on visible chars (no ANSI awareness for slicing)
		# For simplicity, slice the raw string. This may cut ANSI; best to avoid ANSI in data if truncation occurs.
		return s[: max(width - 2, 0)] + ".."
	if not data:
		return ""
	# Normalize input data structure
	if isinstance(data, str):
		data = data.strip("\n").split("\n")
		data = [line.split(delimiter) for line in data]
	elif isinstance(data, dict):
		if isinstance(next(iter(data.values())), dict):
			tempData = [["key"] + list(next(iter(data.values())).keys())]
			tempData.extend([[key] + list(value.values()) for key, value in data.items()])
			data = tempData
		else:
			data = [[key] + list(value) for key, value in data.items()]
	elif not isinstance(data, list):
		data = list(data)
	if isinstance(data[0], dict):
		tempData = [list(data[0].keys())]
		tempData.extend([list(item.values()) for item in data])
		data = tempData
	data = [[str(item) for item in row] for row in data]
	num_cols = len(data[0])
	# Resolve header and rows
	using_provided_header = header is not None
	if not using_provided_header:
		header = data[0]
		rows = data[1:]
	else:
		if isinstance(header, str):
			header = header.split(delimiter)
		# Pad/trim header to match num_cols
		if len(header) < num_cols:
			header = header + [""] * (num_cols - len(header))
		elif len(header) > num_cols:
			header = header[:num_cols]
		rows = data
	# Compute initial column widths based on data and header
	def compute_col_widths(hdr, rows_):
		col_w = [0] * len(hdr)
		for i in range(len(hdr)):
			col_w[i] = max(0, visible_len(hdr[i]), *(visible_len(r[i]) for r in rows_ if i < len(r)))
		return col_w
	# Ensure all rows have the same number of columns
	normalized_rows = []
	for r in rows:
		if len(r) < num_cols:
			r = r + [""] * (num_cols - len(r))
		elif len(r) > num_cols:
			r = r[:num_cols]
		normalized_rows.append(r)
	rows = normalized_rows
	col_widths = compute_col_widths(header, rows)
	# If full=True, keep existing formatting
	# Else try to fit within the terminal width by:
	# 1) Switching to compressed separators if needed
	# 2) Recursively compressing columns (truncating with ".")
	sep = " | "
	hsep = "-+-"
	cols = get_terminal_size()[0]
	def render(hdr, rows, col_w, sep_str, hsep_str):
		row_fmt = sep_str.join("{{:<{}}}".format(w) for w in col_w)
		out = []
		out.append(row_fmt.format(*hdr))
		out.append(hsep_str.join("-" * w for w in col_w))
		for row in rows:
			if not any(row):
				out.append(hsep_str.join("-" * w for w in col_w))
			else:
				row = [truncate_to_width(row[i], col_w[i]) for i in range(len(row))]
				out.append(row_fmt.format(*row))
		return "\n".join(out) + "\n"
	if full:
		return render(header, rows, col_widths, sep, hsep)
	# Try default separators first
	if table_width(col_widths, len(sep)) <= cols:
		return render(header, rows, col_widths, sep, hsep)
	# Use compressed separators (no spaces)
	sep = "|"
	hsep = "+"
	if table_width(col_widths, len(sep)) <= cols:
		return render(header, rows, col_widths, sep, hsep)
	# Begin column compression
	# Track which columns have been compressed already to header width
	header_widths = [visible_len(h) for h in header]
	width_diff = [max(col_widths[i] - header_widths[i],0) for i in range(num_cols)]
	total_overflow_width = table_width(col_widths, len(sep)) - cols
	for i, diff in sorted(enumerate(width_diff), key=lambda x: -x[1]):
		if total_overflow_width <= 0:
			break
		if diff <= 0:
			continue
		reduce_by = min(diff, total_overflow_width)
		col_widths[i] -= reduce_by
		total_overflow_width -= reduce_by
	return render(header, rows, col_widths, sep, hsep)

def format_bytes(size, use_1024_bytes=None, to_int=False, to_str=False,str_format='.2f'):
	"""
	Format the size in bytes to a human-readable format or vice versa.
	From hpcp: https://github.com/yufei-pan/hpcp

	Args:
		size (int or str): The size in bytes or a string representation of the size.
		use_1024_bytes (bool, optional): Whether to use 1024 bytes as the base for conversion. If None, it will be determined automatically. Default is None.
		to_int (bool, optional): Whether to convert the size to an integer. Default is False.
		to_str (bool, optional): Whether to convert the size to a string representation. Default is False.
		str_format (str, optional): The format string to use when converting the size to a string. Default is '.2f'.

	Returns:
		int or str: The formatted size based on the provided arguments.

	Examples:
		>>> format_bytes(1500, use_1024_bytes=False)
		'1.50 K'
		>>> format_bytes('1.5 GiB', to_int=True)
		1610612736
		>>> format_bytes('1.5 GiB', to_str=True)
		'1.50 Gi'
		>>> format_bytes(1610612736, use_1024_bytes=True, to_str=True)
		'1.50 Gi'
		>>> format_bytes(1610612736, use_1024_bytes=False, to_str=True)
		'1.61 G'
	"""
	if to_int or isinstance(size, str):
		if isinstance(size, int):
			return size
		elif isinstance(size, str):
			# Use regular expression to split the numeric part from the unit, handling optional whitespace
			match = re.match(r"(\d+(\.\d+)?)\s*([a-zA-Z]*)", size)
			if not match:
				if to_str:
					return size
				print("Invalid size format. Expected format: 'number [unit]', e.g., '1.5 GiB' or '1.5GiB'")
				print(f"Got: {size}")
				return 0
			number, _, unit = match.groups()
			number = float(number)
			unit  = unit.strip().lower().rstrip('b')
			# Define the unit conversion dictionary
			if unit.endswith('i'):
				# this means we treat the unit as 1024 bytes if it ends with 'i'
				use_1024_bytes = True
			elif use_1024_bytes is None:
				use_1024_bytes = False
			unit  = unit.rstrip('i')
			if use_1024_bytes:
				power = 2**10
			else:
				power = 10**3
			unit_labels = {'': 0, 'k': 1, 'm': 2, 'g': 3, 't': 4, 'p': 5}
			if unit not in unit_labels:
				if to_str:
					return size
				print(f"Invalid unit '{unit}'. Expected one of {list(unit_labels.keys())}")
				return 0
			if to_str:
				return format_bytes(size=int(number * (power ** unit_labels[unit])), use_1024_bytes=use_1024_bytes, to_str=True, str_format=str_format)
			# Calculate the bytes
			return int(number * (power ** unit_labels[unit]))
		else:
			try:
				return int(size)
			except Exception:
				return 0
	elif to_str or isinstance(size, int) or isinstance(size, float):
		if isinstance(size, str):
			try:
				size = size.rstrip('B').rstrip('b')
				size = float(size.lower().strip())
			except Exception:
				return size
		# size is in bytes
		if use_1024_bytes or use_1024_bytes is None:
			power = 2**10
			n = 0
			power_labels = {0 : '', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti', 5: 'Pi'}
			while size >= power:
				size /= power
				n += 1
			return f"{size:{str_format}}{' '}{power_labels[n]}"
		else:
			power = 10**3
			n = 0
			power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T', 5: 'P'}
			while size >= power:
				size /= power
				n += 1
			return f"{size:{str_format}}{' '}{power_labels[n]}"
	else:
		try:
			return format_bytes(float(size), use_1024_bytes)
		except Exception as e:
			import traceback
			print(f"Error: {e}")
			print(traceback.format_exc())
			print(f"Invalid size: {size}")
		return 0

def get_resource_usage(return_dict = False):
	try:
		if RESOURCE_LIB_AVAILABLE:
			rawResource =  resource.getrusage(resource.RUSAGE_SELF)
			resourceDict = {}
			resourceDict['user mode time'] = f'{rawResource.ru_utime} seconds'
			resourceDict['system mode time'] = f'{rawResource.ru_stime} seconds'
			resourceDict['max resident set size'] = f'{format_bytes(rawResource.ru_maxrss * 1024)}B'
			resourceDict['shared memory size'] = f'{format_bytes(rawResource.ru_ixrss * 1024)}B'
			resourceDict['unshared memory size'] = f'{format_bytes(rawResource.ru_idrss * 1024)}B'
			resourceDict['unshared stack size'] = f'{format_bytes(rawResource.ru_isrss * 1024)}B'
			resourceDict['cached page hits'] = f'{rawResource.ru_minflt}'
			resourceDict['missed page hits'] = f'{rawResource.ru_majflt}'
			resourceDict['swapped out page count'] = f'{rawResource.ru_nswap}'
			resourceDict['block input operations'] = f'{rawResource.ru_inblock}'
			resourceDict['block output operations'] = f'{rawResource.ru_oublock}'
			resourceDict['IPC messages sent'] = f'{rawResource.ru_msgsnd}'
			resourceDict['IPC messages received'] = f'{rawResource.ru_msgrcv}'
			resourceDict['signals received'] = f'{rawResource.ru_nsignals}'
			resourceDict['voluntary context sw'] = f'{rawResource.ru_nvcsw}'
			resourceDict['involuntary context sw'] = f'{rawResource.ru_nivcsw}'
			if return_dict:
				return resourceDict
			return '\n'.join(['\t'.join(line) for line in resourceDict.items()])
	except Exception as e:
		print(f"Error: {e}")
	if return_dict:
		return {}
	return ''

def __teePrintOrNot(message,level = 'info',teeLogger = None):
	"""
	Prints the given message or logs it using the provided teeLogger.

	Parameters:
		message (str): The message to be printed or logged.
		level (str, optional): The log level. Defaults to 'info'.
		teeLogger (object, optional): The logger object used for logging. Defaults to None.

	Returns:
		None
	"""
	try:
		if teeLogger:
			try:
				teeLogger.teelog(message,level,callerStackDepth=3)
			except Exception:
				teeLogger.teelog(message,level)
		else:
			print(message,flush=True)
	except Exception:
		print(message,flush=True)

# ---------------------------------------------------------------------------
# 4.1 infrastructure: file names, dialect selection, warnings, locked appends
# ---------------------------------------------------------------------------
STRICT_EXTENSIONS = ('tsvz', 'csvz', 'nsvz', 'psvz')
_EXTENSION_DELIMITERS = {'tsv': '\t', 'csv': ',', 'nsv': '\0', 'psv': '|',
						 'tsvz': '\t', 'csvz': ',', 'nsvz': '\0', 'psvz': '|'}
_HEX_RE = re.compile(r'^[0-9A-Fa-f]+$')
_PartName = namedtuple('_PartName', 'store ext ordinal rotated codec')
#: Read size for spec-dialect parts (tests shrink it to force chunk splits).
_CHUNK = 1 << 20


def _parsePartName(path):
	"""Split a file name per tsvz-spec-v1 Appendix A.

	Returns ``_PartName(store, ext, ordinal, rotated, codec)``: ``store`` is the
	path up to and including the format extension, ``ext`` the lowercase format
	extension ('' when the name has none), ``ordinal`` an int or None,
	``rotated`` a bool and ``codec`` the lowercase compression suffix or ''.
	"""
	rest, codec = path, ''
	head, dot, tail = rest.rpartition('.')
	if dot and tail.lower() in COMPRESSED_FILE_EXTENSIONS:
		rest, codec = head, tail.lower()
	plain = rest
	rotated = False
	head, dot, tail = rest.rpartition('.')
	if dot and tail.lower() == 'rotated':
		rest, rotated = head, True
	ordinal = None
	head, dot, tail = rest.rpartition('.')
	if dot and _HEX_RE.match(tail) and head.rpartition('.')[2].lower() in _EXTENSION_DELIMITERS:
		rest, ordinal = head, int(tail, 16)
	head, dot, tail = rest.rpartition('.')
	ext = tail.lower() if dot else ''
	if ext not in _EXTENSION_DELIMITERS:
		return _PartName(plain, '', None, False, codec)
	return _PartName(rest, ext, ordinal, rotated, codec)


def _isSpecPath(path):
	"""True when ``path`` names a tsvz-spec-v1 file (``.tsvz``/``.csvz``/``.nsvz``/``.psvz``)."""
	return _parsePartName(path).ext in STRICT_EXTENSIONS


def _warn(message, teeLogger=None):
	"""Report a tolerance warning through ``teeLogger``, else stderr (never stdout)."""
	if teeLogger:
		try:
			teeLogger.teelog(message, 'warning')
			return
		except Exception:
			pass
	eprint(message)


class _Reporter(object):
	"""Collect tolerance events for one operation and report each kind once.

	``note(kind, where, detail)`` records an event; ``where`` is a location such
	as ``'line 17'`` or None. ``flush()`` prints one line per kind, in first-seen
	order, with the number of occurrences and the first location.
	"""

	def __init__(self, path, teeLogger=None):
		self.path = path
		self.teeLogger = teeLogger
		self._events = OrderedDict()

	def note(self, kind, where, detail):
		event = self._events.get(kind)
		if event is None:
			self._events[kind] = [1, where, detail]
		else:
			event[0] += 1

	def discard(self, kind):
		"""Forget the events of ``kind`` (for a caller that reports them another way)."""
		self._events.pop(kind, None)

	def has(self, kind):
		"""True when an event of ``kind`` was noted and not yet flushed."""
		return kind in self._events

	def flush(self):
		for count, where, detail in self._events.values():
			message = 'TSVZ warning: {}: {}'.format(self.path, detail)
			if count > 1:
				message += ' ({} occurrences{})'.format(count, ', first at ' + where if where else '')
			elif where:
				message += ' ({})'.format(where)
			_warn(message, self.teeLogger)
		self._events.clear()


def _warnOnce(owner, kind, path, detail, teeLogger=None):
	"""Warn about ``detail`` once per ``owner`` object and ``kind``."""
	warned = owner.__dict__.setdefault('_tsvzWarned', set())
	if kind in warned:
		return
	warned.add(kind)
	_warn('TSVZ warning: {}: {}'.format(path, detail), teeLogger)


_PATH_LOCKS = {}
_PATH_LOCKS_GUARD = threading.Lock()


def _pathLock(path):
	"""Return the process-wide lock for ``path``.

	POSIX ``lockf`` does not exclude two handles of the same process, so 4.1
	writers also hold this lock while they append to or rewrite a file.
	"""
	key = os.path.realpath(path)
	with _PATH_LOCKS_GUARD:
		lock = _PATH_LOCKS.get(key)
		if lock is None:
			lock = _PATH_LOCKS[key] = threading.RLock()
		return lock


def _lockFile(f):
	"""Take the exclusive whole-file lock that 3.39 writers take."""
	if os.name == 'posix':
		fcntl.lockf(f, fcntl.LOCK_EX)
	elif os.name == 'nt':
		msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 2147483647)


def _tryLockFile(f):
	"""Take an exclusive whole-file lock without waiting; return False when another handle holds it.

	POSIX uses ``flock``: it belongs to the open file description, so closing
	another descriptor of the same file in this process does not drop it.
	Windows locks are mandatory, so there one byte far past the content is
	locked and readers can still read the file.
	"""
	try:
		if os.name == 'posix':
			fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
		elif os.name == 'nt':
			os.lseek(f.fileno(), 1 << 30, os.SEEK_SET)
			msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
		return True
	except OSError:
		return False


def _unlockFile(f):
	try:
		if os.name == 'posix':
			fcntl.lockf(f, fcntl.LOCK_UN)
		elif os.name == 'nt':
			msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 2147483647)
	except Exception:
		pass


def _seekRawStart(f):
	"""Move a buffered file object's underlying fd to offset 0.

	A plain ``f.seek(0)`` can be served from the read buffer without moving the
	raw position, but ``msvcrt.locking`` locks from the raw position. Seeking to
	the end first discards the buffer, so the following seek really moves it.
	"""
	f.seek(0, os.SEEK_END)
	f.seek(0)


def _lastNewlineEnd(f, size):
	"""Return the offset just after the last b'\\n' within the first ``size`` bytes of ``f``, or 0."""
	position = size
	while position > 0:
		step = min(65536, position)
		position -= step
		f.seek(position)
		index = f.read(step).rfind(b'\n')
		if index >= 0:
			return position + index + 1
	return 0


def _endsWithoutNewline(f):
	"""True when the readable binary file ``f`` is non-empty and does not end in b'\\n'."""
	size = f.seek(0, os.SEEK_END)
	if not size:
		return False
	f.seek(size - 1)
	return f.read(1) != b'\n'


def _lockedAppend(path, payload, tailPolicy, reporter, fsync=False):
	"""Append ``payload`` (whole records ending in b'\\n') to an uncompressed file.

	The file is opened ``O_APPEND`` and locked (``_pathLock`` + ``lockf``). An
	unterminated last line is handled per ``tailPolicy``: ``'newline'`` writes
	b'\\n' first (legacy dialect: that line is a record), ``'truncate'`` cuts it
	off (spec dialect: those bytes were never committed, spec §4.3). Either
	repair is noted on ``reporter``. A missing file is created.
	"""
	with _pathLock(path):
		with open(path, 'a+b') as f:
			_lockFile(f)
			try:
				if _endsWithoutNewline(f):
					size = f.seek(0, os.SEEK_END)
					if tailPolicy == 'newline':
						payload = b'\n' + payload
						reporter.note('missing-newline', None, 'last line had no trailing newline; added one before appending')
					else:
						keep = _lastNewlineEnd(f, size)
						f.seek(keep)
						tail = f.read(size - keep)
						f.truncate(keep)
						reporter.note('truncated-tail', None, 'removed uncommitted tail {!r} before appending'.format(tail[:80]))
				f.write(payload)
				f.flush()
				if fsync:
					os.fsync(f.fileno())
			finally:
				_unlockFile(f)


def _compressBytes(codec, data, level=1):
	"""Compress ``data`` as one complete member / stream for ``codec``."""
	if codec in ('gz', 'gzip'):
		import gzip
		return gzip.compress(data, compresslevel=level)
	if codec in ('bz2', 'bzip2'):
		import bz2
		return bz2.compress(data, compresslevel=level)
	if codec in ('xz', 'lzma'):
		import lzma
		return lzma.compress(data, preset=level)
	from compression import zstd  # Python 3.14+
	return zstd.compress(data, level=level)


def _dirMtimeNs(path):
	"""Modification time (ns) of the directory holding ``path``, or 0."""
	try:
		return os.stat(os.path.dirname(os.path.abspath(path))).st_mtime_ns
	except OSError:
		return 0


def _processLine(line,taskDic,correctColumnNum,strict = True,delimiter = ...,defaults = ...,
				 storeOffset = False, offset = -1, reporter = None):
	"""
	Process a line of text and update the task dictionary.

	Parameters:
	line (str): The line of text to process.
	taskDic (dict): The dictionary to update with the processed line.
	correctColumnNum (int): The expected number of columns in the line.
	strict (bool, optional): Whether to strictly enforce the correct number of columns. Defaults to True.
	defaults (list, optional): The default values to use for missing columns. Defaults to [].
	storeOffset (bool, optional): Whether to store the offset of the line in the taskDic. Defaults to False.
	offset (int, optional): The offset of the line in the file. Defaults to -1.
	reporter (_Reporter, optional): Receives a note for each line dropped by strict mode (L4).

	Returns:
	tuple: A tuple containing the updated correctColumnNum and the processed lineCache or offset.

	"""
	if defaults is ...:
		defaults = []
	delimiter = get_delimiter(delimiter)
	line = line.strip('\x00').rstrip('\r\n')
	if not line or (line.startswith('#') and not line.startswith(DEFAULTS_INDICATOR_KEY)):
		# if verbose:
		# 	__teePrintOrNot(f"Ignoring comment line: {line}",teeLogger=teeLogger)
		return correctColumnNum , []
	# we only interested in the lines that have the correct number of columns
	lineCache = _unsanitize(line.split(delimiter),delimiter)
	if not lineCache or not lineCache[0]:
		return correctColumnNum , []
	if correctColumnNum == -1:
		if defaults and len(defaults) > 1:
			correctColumnNum = len(defaults)
		else:
			correctColumnNum = len(lineCache)
		# if verbose:
		# 	__teePrintOrNot(f"detected correctColumnNum: {len(lineCache)}",teeLogger=teeLogger)
	if len(lineCache) == 1 or not any(lineCache[1:]):
		if correctColumnNum == 1: 
			taskDic[lineCache[0]] = lineCache if not storeOffset else offset
		elif lineCache[0] == DEFAULTS_INDICATOR_KEY:
			# if verbose:
			# 	__teePrintOrNot(f"Empty defaults line found: {line}",teeLogger=teeLogger)
			defaults.clear()
			defaults[:] = [DEFAULTS_INDICATOR_KEY]
		else:
			# if verbose:
			# 	__teePrintOrNot(f"Key {lineCache[0]} found with empty value, deleting such key's representaion",teeLogger=teeLogger)
			if lineCache[0] in taskDic:
				del taskDic[lineCache[0]]
		return correctColumnNum , []
	elif len(lineCache) != correctColumnNum:
		if strict and not any(defaults[1:]):
			if reporter is not None:
				reporter.note('strict-drop', None, 'dropped a line whose column count is not {} (strict mode)'.format(correctColumnNum))
			return correctColumnNum , []
		else:
			# fill / cut the line with empty entries til the correct number of columns
			if len(lineCache) < correctColumnNum:
				lineCache += ['']*(correctColumnNum-len(lineCache))
			elif len(lineCache) > correctColumnNum:
				lineCache = lineCache[:correctColumnNum]
			# if verbose:
			# 	__teePrintOrNot(f"Correcting {lineCache[0]}",teeLogger=teeLogger)
	# now replace empty values with defaults
	if defaults and len(defaults) > 1:
		for i in range(1,len(lineCache)):
			if not lineCache[i] and i < len(defaults) and defaults[i]:
				lineCache[i] = defaults[i]
	if lineCache[0] == DEFAULTS_INDICATOR_KEY:
		# if verbose:
		# 	__teePrintOrNot(f"Defaults line found: {line}",teeLogger=teeLogger)
		defaults[:] = lineCache
		return correctColumnNum , []
	taskDic[lineCache[0]] = lineCache if not storeOffset else offset
	# if verbose:
	# 	__teePrintOrNot(f"Key {lineCache[0]} added",teeLogger=teeLogger)
	return correctColumnNum, lineCache

def _read_last_valid_line_forward(fileName, taskDic, correctColumnNum, verbose=False, teeLogger=None,
								  strict=False, encoding='utf8', delimiter=...,
								  defaults=None, storeOffset=False):
	"""
	Forward, single-pass variant of read_last_valid_line for compressed files.

	Backward chunked seeking forces a full re-decompress on every seek (and is not
	reliably supported for some codecs e.g. zstd), so for compressed files we decompress
	once, streaming forward, and keep only the last valid line / offset. A scratch dict is
	used so memory stays flat instead of accumulating the whole file into taskDic.
	"""
	if defaults is None:
		defaults = []
	delimiter = get_delimiter(delimiter, file_name=fileName)
	last_valid = -1 if storeOffset else []
	scratch = {}
	with openFileAsCompressed(fileName, 'rb', encoding=encoding, teeLogger=teeLogger) as file:
		while True:
			offset = file.tell()
			line = file.readline()
			if not line:
				break
			if not line.strip():
				continue
			correctColumnNum, lineCache = _processLine(
				line=line.decode(encoding=encoding, errors='replace'),
				taskDic=scratch,
				correctColumnNum=correctColumnNum,
				strict=strict,
				delimiter=delimiter,
				defaults=defaults,
				storeOffset=storeOffset,
				offset=offset,
			)
			if lineCache:
				last_valid = offset if storeOffset else lineCache
	return last_valid

def read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=False, teeLogger=None, strict=False,
						 encoding = 'utf8',delimiter = ...,defaults = ...,storeOffset = False	):
	"""
	Reads the last valid line from a file.

	Args:
		fileName (str): The name of the file to read.
		taskDic (dict): A dictionary to pass to processLine function.
		correctColumnNum (int): A column number to pass to processLine function.
		verbose (bool, optional): Whether to print verbose output. Defaults to False.
		teeLogger (optional): Logger to use for tee print. Defaults to None.
		encoding (str, optional): The encoding of the file. Defaults to None.
		strict (bool, optional): Whether to enforce strict processing. Defaults to False.
		delimiter (str, optional): The delimiter used in the file. Defaults to None.
		defaults (list, optional): The default values to use for missing columns. Defaults to [].
		storeOffset (bool, optional): Instead of storing the data in taskDic, store the offset of each line. Defaults to False.

	Returns:
		list: The last valid line as a list of strings, or an empty list if no valid line is found.
	"""
	if _isSpecPath(fileName):
		return _specReadTabularFile(fileName, teeLogger=teeLogger, lastLineOnly=True, verifyHeader=False,
									verbose=verbose, encoding=encoding, strict=strict, delimiter=delimiter,
									defaults=defaults, correctColumnNum=correctColumnNum, storeOffset=storeOffset)
	chunk_size = 1024  # Read in chunks of 1024 bytes
	last_valid_line = []
	if defaults is ...:
		defaults = []
	delimiter = get_delimiter(delimiter,file_name=fileName)
	if verbose:
		__teePrintOrNot(f"Reading last line only from {fileName}",teeLogger=teeLogger)
	if _isCompressedFile(fileName):
		# Backward seeking through a codec is slow/unsupported; decompress once, forward.
		return _read_last_valid_line_forward(
			fileName, taskDic, correctColumnNum, verbose=verbose, teeLogger=teeLogger,
			strict=strict, encoding=encoding, delimiter=delimiter, defaults=defaults,
			storeOffset=storeOffset)
	with openFileAsCompressed(fileName, 'rb',encoding=encoding, teeLogger=teeLogger) as file:
		file.seek(0, os.SEEK_END)
		file_size = file.tell()
		buffer = b''
		position = file_size
		processedSize = 0

		while position > 0:
			# Read chunks from the end of the file
			read_size = min(chunk_size, position)
			position -= read_size
			file.seek(position)
			chunk = file.read(read_size)
			
			# Prepend new chunk to buffer
			buffer = chunk + buffer
			
			# Split the buffer into lines
			lines = buffer.split(b'\n')

			# When more of the file remains to the left, lines[0] may be an
			# incomplete line; defer it (it is carried into the next chunk via
			# `buffer = lines[0]` below) so it is counted/processed exactly once.
			# Only when we have reached the start of the file (position == 0) is
			# lines[0] a complete line that must be processed here.
			lowest = 1 if position > 0 else 0
			# Process lines from the last to the first
			for i in range(len(lines) - 1, lowest - 1, -1):
				processedSize += len(lines[i]) + 1  # +1 for the newline character
				if lines[i].strip():  # Skip empty lines
					# Process the line
					correctColumnNum, lineCache = _processLine(
						line=lines[i].decode(encoding=encoding,errors='replace'),
						taskDic=taskDic,
						correctColumnNum=correctColumnNum,
						strict=strict,
						delimiter=delimiter,
						defaults=defaults,
						storeOffset=storeOffset,
						offset=file_size - processedSize + 1
					)
					# If the line is valid, return it
					if lineCache:
						if storeOffset :
							return file_size - processedSize + 1
						return lineCache
			
			# Keep the last (possibly incomplete) line in buffer for the next read
			buffer = lines[0]

	# Return empty list if no valid line found
	if storeOffset:
		return -1
	return last_valid_line

@functools.lru_cache(maxsize=None)
def _get_sanitization_re(delimiter = ...):
	delimiter = get_delimiter(delimiter)
	return re.compile(r"(</sep/>|</LF/>|<sep>|<LF>|\n|" + re.escape(delimiter) + r")")

_sanitize_replacements = {
	"<sep>":"</sep/>",
	"<LF>":"</LF/>",
	"\n":"<LF>",
}
_inverse_sanitize_replacements = {v: k for k, v in _sanitize_replacements.items()}

def _sanitize(data,delimiter = ...):
	if not data:
		return data
	delimiter = get_delimiter(delimiter)
	def repl(m):
		tok = m.group(0)
		if tok == delimiter:
			return "<sep>"
		if tok == "</sep/>":
			eprint("Warning: '</sep/>' is a reserved token and is invalid as field data; "
				   "it will be read back as the literal '<sep>'.")
			return tok
		if tok == "</LF/>":
			eprint("Warning: '</LF/>' is a reserved token and is invalid as field data; "
				   "it will be read back as the literal '<LF>'.")
			return tok
		return _sanitize_replacements.get(tok, tok)
	pattern = _get_sanitization_re(delimiter)
	if isinstance(data,str):
		return pattern.sub(repl, data)
	else:
		return [pattern.sub(repl,str(segment)) if segment else '' for segment in data]

def _unsanitize(data,delimiter = ...):
	if not data:
		return data
	delimiter = get_delimiter(delimiter)
	def repl(m):
		tok = m.group(0)
		if tok == "<sep>":
			return delimiter
		return _inverse_sanitize_replacements.get(tok, tok)
	pattern = _get_sanitization_re(delimiter)
	if isinstance(data,str):
		return pattern.sub(repl, data.rstrip())
	else:
		return [pattern.sub(repl,str(segment).rstrip()) if segment else '' for segment in data]

def _formatHeader(header,verbose = False,teeLogger = None,delimiter = ...):
	"""
	Format the header string.

	Parameters:
	- header (str or list): The header string or list to format.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- teeLogger (object, optional): The tee logger object for printing output. Defaults to None.

	Returns:
		list: The formatted header list of string.
	"""
	if isinstance(header,str):
		header = header.split(get_delimiter(delimiter))
	else:
		try:
			header = [str(s) for s in header]
		except Exception:
			if verbose:
				__teePrintOrNot('Invalid header, setting header to empty.','error',teeLogger=teeLogger)
			header = []
	return [s.rstrip() for s in header]

def _lineContainHeader(header,line,verbose = False,teeLogger = None,strict = False,delimiter = ...):
	"""
	Verify if a line contains the header.

	Parameters:
	- header (str): The header string to verify.
	- line (str): The line to verify against the header.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- teeLogger (object, optional): The tee logger object for printing output. Defaults to None.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to False.

	Returns:
	bool: True if the header matches the line, False otherwise.
	"""
	delimiter = get_delimiter(delimiter)
	line = _formatHeader(line,verbose=verbose,teeLogger=teeLogger,delimiter=delimiter)
	if verbose:
		__teePrintOrNot(f"Header: \n{header}",teeLogger=teeLogger)
		__teePrintOrNot(f"First line: \n{line}",teeLogger=teeLogger)
	if len(header) != len(line) or any([header[i] not in line[i] for i in range(len(header))]):
		__teePrintOrNot(f"Header mismatch: \n{line} \n!= \n{header}",teeLogger=teeLogger)
		if strict:
			raise ValueError("Data format error! Header mismatch")
		return False
	return True

def _verifyFileExistence(fileName,createIfNotExist = True,teeLogger = None,header = [],encoding = 'utf8',strict = True,delimiter = ...):
	"""
	Verify the existence of the tabular file.

	Parameters:
	- fileName (str): The path of the tabular file.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to True.
	- teeLogger (object, optional): The tee logger object for printing output. Defaults to None.
	- header (list, optional): The header line to verify against. Defaults to [].
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.

	Returns:
	bool: True if the file exists, False otherwise.
	"""
	delimiter = get_delimiter(delimiter, file_name=fileName)
	remainingFileName, _ ,extenstionName = fileName.rpartition('.')
	if extenstionName in COMPRESSED_FILE_EXTENSIONS:
		remainingFileName, _ ,extenstionName = remainingFileName.rpartition('.')
	if delimiter and delimiter == '\t' and not extenstionName == 'tsv':
		__teePrintOrNot(f'Warning: Filename {fileName} does not end with .tsv','warning',teeLogger=teeLogger)
	elif delimiter and delimiter == ',' and not extenstionName == 'csv':
		__teePrintOrNot(f'Warning: Filename {fileName} does not end with .csv','warning',teeLogger=teeLogger)
	elif delimiter and delimiter == '\0' and not extenstionName == 'nsv':
		__teePrintOrNot(f'Warning: Filename {fileName} does not end with .nsv','warning',teeLogger=teeLogger)
	elif delimiter and delimiter == '|' and not extenstionName == 'psv':
		__teePrintOrNot(f'Warning: Filename {fileName} does not end with .psv','warning',teeLogger=teeLogger)
	if not os.path.isfile(fileName):
		if createIfNotExist:
			try:
				with openFileAsCompressed(fileName, mode ='wb',encoding=encoding,teeLogger=teeLogger)as file:
					header = delimiter.join(_sanitize(_formatHeader(header,
													 teeLogger=teeLogger,
													 delimiter=delimiter,
													 ),delimiter=delimiter))
					file.write(header.encode(encoding=encoding,errors='replace')+b'\n')
				__teePrintOrNot('Created '+fileName,teeLogger=teeLogger)
				return True
			except Exception:
				__teePrintOrNot('Failed to create '+fileName,'error',teeLogger=teeLogger)
				if strict:
					raise FileNotFoundError("Failed to create file")
				return False
		elif strict:
			__teePrintOrNot('File not found','error',teeLogger=teeLogger)
			raise FileNotFoundError("File not found")
		else:
			return False
	return True

def readTSV(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,
			verbose = False,taskDic = None,encoding = 'utf8',strict = True,delimiter = '\t',defaults = ...,
			correctColumnNum = -1):
	"""
	Compatibility method, calls readTabularFile. 
	Read a Tabular (CSV / TSV / NSV) file and return the data as a dictionary.

	Parameters:
	- fileName (str): The path to the Tabular file.
	- teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
	- header (str or list, optional): The header of the Tabular file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
	- lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
	- verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t'.
	- defaults (list, optional): The default values to use for missing columns. Defaults to [].
	- correctColumnNum (int, optional): The expected number of columns in the file. If -1, it will be determined from the first valid line. Defaults to -1.

	Returns:
	- OrderedDict: The dictionary containing the data from the Tabular file.

	Raises:
	- Exception: If the file is not found or there is a data format error.

	"""
	return readTabularFile(fileName,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,
						   lastLineOnly = lastLineOnly,verifyHeader = verifyHeader,verbose = verbose,taskDic = taskDic,
						   encoding = encoding,strict = strict,delimiter = delimiter,defaults=defaults,
						   correctColumnNum = correctColumnNum)

def readTabularFile(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,
					verbose = False,taskDic = None,encoding = 'utf8',strict = True,delimiter = ...,defaults = ...,
					correctColumnNum = -1,storeOffset = False):
	"""
	Read a Tabular (CSV / TSV / NSV) file and return the data as a dictionary.

	Parameters:
	- fileName (str): The path to the Tabular file.
	- teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
	- header (str or list, optional): The header of the Tabular file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
	- lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
	- verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	- defaults (list, optional): The default values to use for missing columns. Defaults to [].
	- correctColumnNum (int, optional): The expected number of columns in the file. If -1, it will be determined from the first valid line. Defaults to -1.
	- storeOffset (bool, optional): Instead of storing the data in taskDic, store the offset of each line. Defaults to False.
	
	Returns:
	- OrderedDict: The dictionary containing the data from the Tabular file.

	Raises:
	- Exception: If the file is not found or there is a data format error.

	"""
	if _isSpecPath(fileName):
		return _specReadTabularFile(fileName, teeLogger=teeLogger, header=header, createIfNotExist=createIfNotExist,
									lastLineOnly=lastLineOnly, verifyHeader=verifyHeader, verbose=verbose,
									taskDic=taskDic, encoding=encoding, strict=strict, delimiter=delimiter,
									defaults=defaults, correctColumnNum=correctColumnNum, storeOffset=storeOffset)
	if taskDic is None:
		taskDic = {}
	if defaults is ...:
		defaults = []
	delimiter = get_delimiter(delimiter,file_name=fileName)
	header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger, delimiter = delimiter)
	if not _verifyFileExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict,delimiter=delimiter):
		return taskDic
	reporter = _Reporter(fileName, teeLogger)
	try:
		with openFileAsCompressed(fileName, mode ='rb',encoding=encoding,teeLogger=teeLogger)as file:
			lineNo = 0
			if any(header) and verifyHeader:
				try:
					raw = file.readline()
				except Exception as e:
					# L5: a compressed stream damaged inside the header line ends the read.
					if lastLineOnly or not _isCompressedFile(fileName):
						raise
					reporter.note('damaged', None, 'compressed stream is damaged ({}: {}); read up to the damage'.format(type(e).__name__, e))
					return taskDic
				lineNo = 1
				try:
					line = raw.decode(encoding=encoding)
				except UnicodeDecodeError:
					line = raw.decode(encoding=encoding,errors='replace')
					reporter.note('decode', 'line 1', 'invalid {} replaced with U+FFFD'.format(encoding))
				if _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict,delimiter = delimiter) and correctColumnNum == -1:
					correctColumnNum = len(header)
					if verbose:
						__teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
			if lastLineOnly:
				lineCache = read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=verbose, teeLogger=teeLogger, strict=strict, delimiter=delimiter, defaults=defaults,storeOffset=storeOffset)
				# if lineCache:
				# 	taskDic[lineCache[0]] = lineCache
				return lineCache
			_legacyReadLoop(file, fileName, taskDic, correctColumnNum, lineNo, strict, delimiter, defaults,
							storeOffset, encoding, reporter)
	finally:
		reporter.flush()
	return taskDic


def _legacyReadLoop(file, fileName, taskDic, correctColumnNum, lineNo, strict, delimiter, defaults, storeOffset,
					encoding, reporter):
	"""3.39's read loop over the rest of an open legacy file (``readTabularFile``).

	Returns ``(correctColumnNum, committed, lineNo)``: the column count after
	the last line, the offset just past the last line that ends in b'\\n'
	(decompressed bytes for a compressed file) and the number of lines read.
	"""
	committed = file.tell()
	lines = iter(file)
	while True:
		try:
			line = next(lines)
		except StopIteration:
			break
		except Exception as e:
			# L5: a damaged compressed stream ends the read instead of raising.
			if not _isCompressedFile(fileName):
				raise
			reporter.note('damaged', None, 'compressed stream is damaged ({}: {}); read up to the damage'.format(type(e).__name__, e))
			break
		lineNo += 1
		try:
			text = line.decode(encoding=encoding)
		except UnicodeDecodeError:
			text = line.decode(encoding=encoding,errors='replace')
			reporter.note('decode', 'line {}'.format(lineNo), 'invalid {} replaced with U+FFFD'.format(encoding))
		position = file.tell()
		correctColumnNum, _ = _processLine(text,taskDic,correctColumnNum,strict = strict,delimiter=delimiter,defaults = defaults,storeOffset=storeOffset,offset=position-len(line),reporter=reporter)
		if line.endswith(b'\n'):
			committed = position
	return correctColumnNum, committed, lineNo

def appendTSV(fileName,lineToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8', strict = True, delimiter = '\t'):
	"""
	Compatibility method, calls appendTabularFile.
	Append a line of data to a Tabular file.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- lineToAppend (str or list): The line of data to append. If it is a string, it will be split by delimiter to form a list.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- createIfNotExist (bool, optional): If True, the file will be created if it does not exist. If False and the file does not exist, an exception will be raised.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	Raises:
	- Exception: If the file does not exist and createIfNotExist is False.
	- Exception: If the existing header does not match the provided header.
	"""
	return appendTabularFile(fileName,lineToAppend,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding, strict = strict, delimiter = delimiter)

def appendTabularFile(fileName,lineToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8', strict = True, delimiter = ...):
	"""
	Append a line of data to a Tabular file.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- lineToAppend (str or list): The line of data to append. If it is a string, it will be split by delimiter to form a list.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- createIfNotExist (bool, optional): If True, the file will be created if it does not exist. If False and the file does not exist, an exception will be raised.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	Raises:
	- Exception: If the file does not exist and createIfNotExist is False.
	- Exception: If the existing header does not match the provided header.
	"""
	return appendLinesTabularFile(fileName,[lineToAppend],teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding, strict = strict, delimiter = delimiter)

def appendLinesTabularFile(fileName,linesToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8', strict = True, delimiter = ...):
	"""
	Append lines of data to a Tabular file.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- linesToAppend (list): The lines of data to append. If it is a list of string, then each string will be split by delimiter to form a list.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- createIfNotExist (bool, optional): If True, the file will be created if it does not exist. If False and the file does not exist, an exception will be raised.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	Raises:
	- Exception: If the file does not exist and createIfNotExist is False.
	- Exception: If the existing header does not match the provided header.
	"""
	if _isSpecPath(fileName):
		return _specAppendLinesTabularFile(fileName, linesToAppend, teeLogger=teeLogger, header=header,
										   createIfNotExist=createIfNotExist, verifyHeader=verifyHeader,
										   verbose=verbose, encoding=encoding, strict=strict, delimiter=delimiter)
	delimiter = get_delimiter(delimiter,file_name=fileName)
	header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger,delimiter=delimiter)
	if not _verifyFileExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict,delimiter=delimiter):
		return
	payload, count = _legacyFormatPayload(fileName, linesToAppend, teeLogger, header, verifyHeader, verbose, encoding,
										  strict, delimiter)
	if not count:
		if verbose:
			__teePrintOrNot(f"No lines to append to {fileName}",teeLogger=teeLogger)
		return
	if _isCompressedFile(fileName):
		with openFileAsCompressed(fileName, mode ='ab',encoding=encoding,teeLogger=teeLogger)as file:
			file.write(payload)
	else:
		# L1: a last line without '\n' is a record to the 3.39 reader; terminate it
		# instead of gluing the new record onto it.
		reporter = _Reporter(fileName, teeLogger)
		_lockedAppend(fileName, payload, 'newline', reporter)
		reporter.flush()
	if verbose:
		__teePrintOrNot(f"Appended {count} lines to {fileName}",teeLogger=teeLogger)


def _legacyFormatPayload(fileName, linesToAppend, teeLogger, header, verifyHeader, verbose, encoding, strict, delimiter):
	"""Format rows as 3.39's ``appendLinesTabularFile`` writes them; return ``(payload, count)``.

	``header`` is a formatted header list and ``delimiter`` a resolved
	delimiter. Rows are sanitised and padded or cut to the column count: the
	header's when the file's first line matches it, else the longest row's.
	"""
	formatedLines = []
	for line in linesToAppend:
		if isinstance(linesToAppend,dict):
			key = line
			line = linesToAppend[key]
		if isinstance(line,str):
			line = line.split(delimiter)
		elif line:
			# Build a new list rather than mutating the caller's list in place.
			newLine = []
			for item in line:
				if isinstance(item,str):
					newLine.append(item)
				else:
					try:
						newLine.append(str(item).rstrip())
					except Exception as e:
						newLine.append(str(e))
			line = newLine
		if isinstance(linesToAppend,dict):
			if (not line or line[0] != key):
				line = [key]+line
		formatedLines.append(_sanitize(line,delimiter=delimiter))
	if not formatedLines:
		return b'', 0
	correctColumnNum = max([len(line) for line in formatedLines])
	if any(header) and verifyHeader:
		with openFileAsCompressed(fileName, mode ='rb',encoding=encoding,teeLogger=teeLogger)as file:
			line = file.readline().decode(encoding=encoding,errors='replace')
			if _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict,delimiter = delimiter):
				correctColumnNum = len(header)
				if verbose:
					__teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
	# truncate / fill the lines to the correct number of columns
	for i in range(len(formatedLines)):
		if len(formatedLines[i]) < correctColumnNum:
			formatedLines[i] += ['']*(correctColumnNum-len(formatedLines[i]))
		elif len(formatedLines[i]) > correctColumnNum:
			formatedLines[i] = formatedLines[i][:correctColumnNum]
	payload = b'\n'.join([delimiter.join(line).encode(encoding=encoding,errors='replace') for line in formatedLines]) + b'\n'
	return payload, len(formatedLines)

def clearTSV(fileName,teeLogger = None,header = '',verifyHeader = False,verbose = False,encoding = 'utf8',strict = False,delimiter = '\t'):
	"""
	Compatibility method, calls clearTabularFile.
	Clear the contents of a Tabular file. Will create if not exist.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	"""
	return clearTabularFile(fileName,teeLogger = teeLogger,header = header,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding,strict = strict,delimiter = delimiter)

def clearTabularFile(fileName,teeLogger = None,header = '',verifyHeader = False,verbose = False,encoding = 'utf8',strict = False,delimiter = ...):
	"""
	Clear the contents of a Tabular file. Will create if not exist.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	"""
	if _isSpecPath(fileName):
		_specClearTabularFile(fileName, teeLogger=teeLogger, header=header, verifyHeader=verifyHeader,
							  verbose=verbose, encoding=encoding, strict=strict, delimiter=delimiter)
		return
	delimiter = get_delimiter(delimiter,file_name=fileName)
	header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger,delimiter=delimiter)
	if not _verifyFileExistence(fileName,createIfNotExist = True,teeLogger = teeLogger,header = header,encoding = encoding,strict = False,delimiter=delimiter):
		raise FileNotFoundError("Something catastrophic happened! File still not found after creation")
	else:
		with openFileAsCompressed(fileName, mode ='rb',encoding=encoding,teeLogger=teeLogger)as file:
			if any(header) and verifyHeader:
				line = file.readline().decode(encoding=encoding,errors='replace')
				if not _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict,delimiter = delimiter):
					__teePrintOrNot(f'Warning: Header mismatch in {fileName}. Keeping original header in file...','warning',teeLogger)
				header = _formatHeader(line,verbose = verbose,teeLogger = teeLogger,delimiter=delimiter)
		with openFileAsCompressed(fileName, mode ='wb',encoding=encoding,teeLogger=teeLogger)as file:
			if header:
				header = delimiter.join(_sanitize(header,delimiter=delimiter))
				file.write(header.encode(encoding=encoding,errors='replace')+b'\n')
	if verbose:
		__teePrintOrNot(f"Cleared {fileName}",teeLogger=teeLogger)

def getFileUpdateTimeNs(fileName):
	# return 0 if the file does not exist
	if not os.path.isfile(fileName):
		return 0
	try:
		return os.stat(fileName).st_mtime_ns
	except Exception:
		__teePrintOrNot(f"Failed to get file update time for {fileName}",'error')
		return get_time_ns()

def get_time_ns():
	try:
		return time.time_ns()
	except Exception:
		# try to get the time in nanoseconds
		return int(time.time()*1e9)

def scrubTSV(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,verbose = False,taskDic = None,encoding = 'utf8',strict = False,delimiter = '\t',defaults = ...):
	"""
	Compatibility method, calls scrubTabularFile.
	Scrub a Tabular (CSV / TSV / NSV) file by reading it and writing the contents back into the file.
	Return the data as a dictionary.

	Parameters:
	- fileName (str): The path to the Tabular file.
	- teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
	- header (str or list, optional): The header of the Tabular file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
	- lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
	- verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to False.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	- defaults (list, optional): The default values to use for missing columns. Defaults to [].

	Returns:
	- OrderedDict: The dictionary containing the data from the Tabular file.

	Raises:
	- Exception: If the file is not found or there is a data format error.

	"""
	return scrubTabularFile(fileName,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,lastLineOnly = lastLineOnly,verifyHeader = verifyHeader,verbose = verbose,taskDic = taskDic,encoding = encoding,strict = strict,delimiter = delimiter,defaults=defaults)

def scrubTabularFile(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,
					 verbose = False,taskDic = None,encoding = 'utf8',strict = False,delimiter = ...,defaults = ...,correctColumnNum = -1):
	"""
	Scrub a Tabular (CSV / TSV / NSV) file by reading it and writing the contents back into the file.
	If using compressed files. This will recompress the file in whole and possibily increase the compression ratio reducing the file size.
	Return the data as a dictionary.
	
	Parameters:
	- fileName (str): The path to the Tabular file.
	- teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
	- header (str or list, optional): The header of the Tabular file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
	- lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
	- verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to False.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	- defaults (list, optional): The default values to use for missing columns. Defaults to [].
	- correctColumnNum (int, optional): The expected number of columns in the file. If -1, it will be determined from the first valid line. Defaults to -1.

	Returns:
	- OrderedDict: The dictionary containing the data from the Tabular file.

	Raises:
	- Exception: If the file is not found or there is a data format error.

	"""
	if _isSpecPath(fileName):
		return _specScrubTabularFile(fileName, teeLogger=teeLogger, header=header, createIfNotExist=createIfNotExist,
									 lastLineOnly=lastLineOnly, verifyHeader=verifyHeader, verbose=verbose,
									 taskDic=taskDic, encoding=encoding, strict=strict, delimiter=delimiter,
									 defaults=defaults, correctColumnNum=correctColumnNum)
	file =  readTabularFile(fileName,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,
							lastLineOnly = lastLineOnly,verifyHeader = verifyHeader,verbose = verbose,taskDic = taskDic,
							encoding = encoding,strict = strict,delimiter = delimiter,defaults=defaults,correctColumnNum = correctColumnNum)
	if file:
		clearTabularFile(fileName,teeLogger = teeLogger,header = header,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding,strict = strict,delimiter = delimiter)
		appendLinesTabularFile(fileName,file,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding,strict = strict,delimiter = delimiter)
	return file

# ===========================================================================
# TSVZ spec v1 dialect (.tsvz / .csvz / .nsvz / .psvz) -- see tsvz-spec-v1.md
# ===========================================================================
MAX_SPEC_VERSION = 1
_MARKER_RE = re.compile(r'^#_[A-Za-z0-9_-]+_#$')
_CHECKSUM_RE = re.compile(r'^#_checksum_([A-Za-z0-9_-]+)_#$', re.IGNORECASE)
_ASCII_DIGITS_RE = re.compile(r'^[0-9]+$')
_SPEC_DECODE_RE = re.compile(r'<(?:sep|LF|lt|#)>')
_SPEC_TOKENS = {'<LF>': '\n', '<lt>': '<', '<#>': '#'}
_RowState = namedtuple('_RowState', 'defaults strip fillEmpty')
_BOOL_VALUES = {'true': True, 'yes': True, 'on': True, '1': True,
				'false': False, 'no': False, 'off': False, '0': False}
#: Stateful boolean markers: key -> (state attribute, built-in default).
_BOOL_MARKERS = {'#_strip_trailing_whites_#': ('strip', True),
				 '#_fill_empty_with_default_#': ('fillEmpty', False),
				 '#_return_defaults_when_missing_#': ('returnDefaults', True)}
#: Advisory enumerated markers: key -> (state attribute, choices; first is the default).
_ENUM_MARKERS = {'#_rotate_#': ('rotate', ('keep', 'rename', 'delete')),
				 '#_write_ack_#': ('writeAck', ('memory', 'disk'))}
_SPEC_MARKER_KEYS = frozenset(['#_version_#', DEFAULTS_INDICATOR_KEY]) | frozenset(_BOOL_MARKERS) | frozenset(_ENUM_MARKERS)


def _specDecodeField(field, delimiter):
	"""Decode spec §13 tokens; any other ``<...>`` passes through literally."""
	if '<' not in field:
		return field
	return _SPEC_DECODE_RE.sub(lambda m: delimiter if m.group(0) == '<sep>' else _SPEC_TOKENS[m.group(0)], field)


def _specEncodeField(value, delimiter, isKey=False):
	"""Encode one field per spec §13.3 (``isKey`` also escapes a leading '#')."""
	if '<' in value:
		value = value.replace('<', '<lt>')
	if delimiter in value:
		value = value.replace(delimiter, '<sep>')
	if '\n' in value:
		value = value.replace('\n', '<LF>')
	if isKey and value.startswith('#'):
		value = '<#>' + value[1:]
	return value


def _specFormatRecord(cells, delimiter, marker=False):
	"""Format one record without its '\\n'. ``marker`` writes ``cells[0]`` raw as a marker key."""
	first = cells[0] if marker else _specEncodeField(cells[0], delimiter, isKey=True)
	if len(cells) == 1:
		return first
	return delimiter.join([first] + [_specEncodeField(cell, delimiter) for cell in cells[1:]])


class _SpecState(object):
	"""Reader state of spec §12, carried across records and parts."""

	def __init__(self, defaults=None):
		self.version = 1
		self.defaults = list(defaults) if defaults and len(defaults) > 1 else [DEFAULTS_INDICATOR_KEY]
		self.strip = True
		self.fillEmpty = False
		self.returnDefaults = True
		self.rotate = 'keep'
		self.writeAck = 'memory'
		self.sawDefaults = False
		self.digests = {}
		self.mismatches = []
		self.rowState = None
		self.refresh()

	def refresh(self):
		"""Rebuild ``rowState``, the part of the state a data row binds to (spec §12.4.4)."""
		self.rowState = _RowState(tuple(self.defaults), self.strip, self.fillEmpty)

	@classmethod
	def fromRowState(cls, rowState):
		state = cls(list(rowState.defaults))
		state.strip = rowState.strip
		state.fillEmpty = rowState.fillEmpty
		state.rowState = rowState
		return state


def _specApplyMarker(state, keyLower, values, reporter, where):
	"""Apply an official marker (spec §12.4). Returns False when its value was rejected."""
	value = values[0].strip() if values else ''
	if keyLower == DEFAULTS_INDICATOR_KEY:
		state.defaults = [DEFAULTS_INDICATOR_KEY] + list(values)
		state.sawDefaults = True
	elif keyLower == '#_version_#':
		if not value:
			state.version = 1
		else:
			number = 0
			if _ASCII_DIGITS_RE.match(value):
				try:
					number = int(value)
				except ValueError:  # more digits than int() converts (Python 3.11+ limit)
					pass
			if number < 1:
				reporter.note('bad-marker', where, 'ignored #_version_# with invalid value {!r}'.format(value))
				return False
			if number > MAX_SPEC_VERSION:
				reporter.note('newer-version', where, 'declares spec version {}; read as version {}'.format(value, MAX_SPEC_VERSION))
			state.version = number
	elif keyLower in _BOOL_MARKERS:
		attr, default = _BOOL_MARKERS[keyLower]
		if not value:
			setattr(state, attr, default)
		elif value.lower() in _BOOL_VALUES:
			setattr(state, attr, _BOOL_VALUES[value.lower()])
		else:
			reporter.note('bad-marker', where, 'ignored {} with invalid value {!r}'.format(keyLower, value))
			return False
	elif keyLower in _ENUM_MARKERS:
		attr, choices = _ENUM_MARKERS[keyLower]
		if not value:
			setattr(state, attr, choices[0])
		elif value.lower() in choices:
			setattr(state, attr, value.lower())
		else:
			reporter.note('bad-marker', where, 'ignored {} with invalid value {!r}'.format(keyLower, value))
			return False
	state.refresh()
	return True


def _specProcessRecord(text, state, delimiter, reporter, where):
	"""Classify and apply one committed record (spec §7.2-§7.9).

	``text`` is the decoded line without its terminator. Returns None for a
	comment, a marker or an empty key; ``(key, None)`` for a tombstone; and
	``(key, row)`` for a data row, where ``row`` holds the key and the written
	cells after stripping, decoding and fill-empty.
	"""
	fields = text.split(delimiter)
	f0 = fields[0]
	if f0.startswith('#'):
		if _MARKER_RE.match(f0):
			keyLower = f0.lower()
			if keyLower in _SPEC_MARKER_KEYS:
				_specApplyMarker(state, keyLower, [_specDecodeField(f, delimiter) for f in fields[1:]], reporter, where)
		return None
	strip = state.strip
	key = _specDecodeField(f0.rstrip(' \t') if strip else f0, delimiter)
	if not key:
		return None
	if len(fields) == 1:
		return (key, None)
	defaults = state.defaults
	fill = state.fillEmpty
	row = [key]
	for j in range(1, len(fields)):
		cell = fields[j]
		if strip:
			cell = cell.rstrip(' \t')
		if '<' in cell:
			cell = _specDecodeField(cell, delimiter)
		if not cell and fill and j < len(defaults):
			cell = defaults[j]
		row.append(cell)
	return (key, row)


class _Crc32(object):
	def __init__(self):
		self.value = 0

	def update(self, data):
		import zlib
		self.value = zlib.crc32(data, self.value)

	def hexdigest(self):
		return '%08x' % (self.value & 0xffffffff)


_HASHLIB_ALGORITHMS = frozenset(name.lower() for name in hashlib.algorithms_available)
_DIGEST_SUPPORT = {}


def _newDigest(algo):
	"""Return a fresh accumulator for ``algo`` (spec §15), or None if unsupported."""
	if algo == 'crc32':
		return _Crc32()
	if algo not in _HASHLIB_ALGORITHMS:
		return None
	try:
		digest = hashlib.new(algo)
		digest.hexdigest()
		return digest
	except (ValueError, TypeError):
		return None


def _digestSupported(algo):
	if algo not in _DIGEST_SUPPORT:
		_DIGEST_SUPPORT[algo] = _newDigest(algo) is not None
	return _DIGEST_SUPPORT[algo]


def _normalizeDefaults(defaults, delimiter):
	"""Accept every 3.39 ``defaults=`` form and return ``['#_defaults_#', d1, ...]``."""
	if not defaults or defaults is ...:
		return [DEFAULTS_INDICATOR_KEY]
	if isinstance(defaults, str):
		defaults = defaults.split(delimiter)
	else:
		try:
			defaults = list(defaults)
		except Exception:
			return [DEFAULTS_INDICATOR_KEY]
	defaults = [str(s).rstrip() if s else '' for s in defaults]
	if not any(defaults):
		return [DEFAULTS_INDICATOR_KEY]
	if defaults[0] != DEFAULTS_INDICATOR_KEY:
		defaults = [DEFAULTS_INDICATOR_KEY] + defaults
	return defaults


def _cellText(item):
	"""3.39's conversion of a non-str cell given to the append helpers."""
	try:
		return str(item).rstrip()
	except Exception as e:
		return str(e)


class _PartInfo(object):
	"""What reading one part found: its codec, uncommitted tail and damage."""

	def __init__(self, path):
		self.path = path
		self.codec = _parsePartName(path).codec
		self.tail = b''
		self.damaged = ''
		self.goodRawEnd = 0
		self.committed = 0  # bytes of committed lines read (decompressed bytes for a compressed part)
		self.lines = 0  # committed lines replayed (set by _specReplay)


def _newDecompressor(codec):
	if codec in ('gz', 'gzip'):
		import zlib
		return zlib.decompressobj(wbits=47)
	if codec in ('bz2', 'bzip2'):
		import bz2
		return bz2.BZ2Decompressor()
	if codec in ('xz', 'lzma'):
		import lzma
		return lzma.LZMADecompressor()
	from compression import zstd  # Python 3.14+
	return zstd.ZstdDecompressor()


def _decompressChunks(f, info):
	"""Decompress ``f`` member by member, recording damage on ``info``.

	Zero padding between gzip members is skipped, as Python's gzip module does.
	A truncated trailing member still yields what it decompresses to
	(``info.damaged = 'truncated'``); a corrupt member ends the stream
	(``'corrupt (...)'``). ``info.goodRawEnd`` is the raw offset after the last
	complete member.
	"""
	try:
		decompressor = _newDecompressor(info.codec)
	except ImportError as e:
		info.damaged = 'cannot decompress ({})'.format(e)
		return
	isGzip = info.codec in ('gz', 'gzip')
	rawPos = 0
	inMember = False
	pending = b''
	while True:
		chunk = pending or f.read(_CHUNK)
		pending = b''
		if not chunk:
			break
		if not inMember and isGzip:
			stripped = chunk.lstrip(b'\0')
			rawPos += len(chunk) - len(stripped)
			if not stripped:
				continue
			chunk = stripped
		inMember = True
		try:
			out = decompressor.decompress(chunk)
		except Exception as e:
			info.damaged = 'corrupt ({})'.format(e)
			return
		if out:
			yield out
		if decompressor.eof:
			pending = decompressor.unused_data
			rawPos += len(chunk) - len(pending)
			info.goodRawEnd = rawPos
			inMember = False
			decompressor = _newDecompressor(info.codec)
		else:
			rawPos += len(chunk)
	if inMember:
		info.damaged = 'truncated'


def _iterPartLines(info, reporter):
	"""Yield ``(rawLine, offset)`` for each committed line of one part (spec §4.3).

	Bytes after the last b'\\n' are not yielded; they are kept on ``info.tail``
	and reported, as is any damage. An unreadable part is reported and skipped.
	"""
	try:
		f = open(info.path, 'rb')
	except OSError as e:
		reporter.note('unreadable', None, 'skipped unreadable part {} ({})'.format(info.path, e))
		return
	rest = b''
	with f:
		chunks = _decompressChunks(f, info) if info.codec else iter(lambda: f.read(_CHUNK), b'')
		offset = 0
		try:
			for chunk in chunks:
				data = rest + chunk if rest else chunk
				start = 0
				while True:
					nl = data.find(b'\n', start)
					if nl < 0:
						break
					yield data[start:nl + 1], offset
					offset += nl + 1 - start
					start = nl + 1
				rest = data[start:]
		except OSError as e:
			reporter.note('unreadable', None, 'stopped reading part {} ({})'.format(info.path, e))
			info.committed = offset
			return
	info.committed = offset
	if rest:
		info.tail = rest
		reporter.note('tail', None, 'ignored uncommitted bytes after the last newline: {!r}'.format(rest[:80]))
	if info.damaged:
		reporter.note('damaged', None, 'compressed part {} is damaged ({}); read up to the damage'.format(info.path, info.damaged))


def _decodeLine(raw, reporter, where):
	"""Decode one committed line as UTF-8 (replacing bad bytes) and drop its terminator (spec §4.2)."""
	try:
		text = raw.decode('utf-8')
	except UnicodeDecodeError:
		text = raw.decode('utf-8', errors='replace')
		reporter.note('utf8', where, 'invalid UTF-8 replaced with U+FFFD')
	if text.endswith('\n'):
		text = text[:-1]
	if text.endswith('\r'):
		text = text[:-1]
	return text


def _specChecksum(state, algo, text, delimiter, reporter, where, position=None):
	"""Arm or verify-and-reset the ``algo`` accumulator (spec §15.3).

	A mismatch is noted on ``reporter`` and appended to ``state.mismatches`` as
	``position + (algo, expected, computed)``; ``position`` is ``(partPath, lineNo)``.
	"""
	if algo in state.digests:
		fields = text.split(delimiter)
		expected = fields[1].strip().lower() if len(fields) > 1 else ''
		if expected:
			actual = state.digests[algo].hexdigest()
			if actual != expected:
				reporter.note('checksum', where, '#_checksum_{}_# mismatch: expected {}, computed {}; data kept'.format(algo, expected, actual))
				state.mismatches.append(tuple(position or (None, None)) + (algo, expected, actual))
	state.digests[algo] = _newDigest(algo)


def _specReplay(parts, delimiter, state, reporter, infos):
	"""Replay ``parts`` as one stream (spec §7, §17.4).

	Yields ``(partIndex, offset, text, record)`` for every committed line except
	checksum markers of supported algorithms, where ``record`` is what
	``_specProcessRecord`` returned. Marker state and checksum accumulators in
	``state`` carry across parts. One ``_PartInfo`` per part is appended to
	``infos``.
	"""
	multi = len(parts) > 1
	for partIndex, path in enumerate(parts):
		info = _PartInfo(path)
		infos.append(info)
		label = os.path.basename(path) + ' ' if multi else ''
		lineNo = 0
		for raw, offset in _iterPartLines(info, reporter):
			lineNo += 1
			where = '{}line {}'.format(label, lineNo)
			if lineNo == 1 and raw.startswith(b'\xef\xbb\xbf'):
				raw = raw[3:]
				reporter.note('bom', where, 'stripped a UTF-8 byte order mark')
			text = _decodeLine(raw, reporter, where)
			check = _CHECKSUM_RE.match(text.split(delimiter, 1)[0]) if text.startswith('#_') else None
			algo = check.group(1).lower() if check and _digestSupported(check.group(1).lower()) else None
			for name, digest in state.digests.items():
				if name != algo:
					digest.update(raw)
			if algo:
				_specChecksum(state, algo, text, delimiter, reporter, where, (path, lineNo))
				continue
			yield partIndex, offset, text, _specProcessRecord(text, state, delimiter, reporter, where)
		info.lines = lineNo


def _specReplayLine(raw, lineNo, label, path, state, delimiter, reporter):
	"""Replay one committed line of the part ``path``, as ``_specReplay`` does for each line.

	``raw`` is the line with its b'\\n'; ``lineNo`` counts from 1 within the
	part and ``label`` prefixes locations in reports. Returns ``(text,
	record)`` as ``_specProcessRecord`` sees it, or None for the checksum
	marker of a supported algorithm. (``_specReplay`` keeps this logic inline
	for speed; the two must stay in step.)
	"""
	where = '{}line {}'.format(label, lineNo)
	if lineNo == 1 and raw.startswith(b'\xef\xbb\xbf'):
		raw = raw[3:]
		reporter.note('bom', where, 'stripped a UTF-8 byte order mark')
	text = _decodeLine(raw, reporter, where)
	check = _CHECKSUM_RE.match(text.split(delimiter, 1)[0]) if text.startswith('#_') else None
	algo = check.group(1).lower() if check and _digestSupported(check.group(1).lower()) else None
	for name, digest in state.digests.items():
		if name != algo:
			digest.update(raw)
	if algo:
		_specChecksum(state, algo, text, delimiter, reporter, where, (path, lineNo))
		return None
	return text, _specProcessRecord(text, state, delimiter, reporter, where)


def _storeParts(path, reporter=None):
	"""Return ``(parts, active)`` for the store ``path`` names (spec §17).

	``parts`` lists existing parts in replay order: the unnumbered file (part
	0) first, then ``<store>.<hex>`` parts by integer ordinal; ``.rotated``
	parts are excluded. ``active`` is the part appends go to: the highest
	numbered part, else ``path``. A path naming a numbered or rotated part is
	read alone (with a warning).
	"""
	name = _parsePartName(path)
	if name.ordinal is not None or name.rotated:
		if reporter is not None:
			reporter.note('fragment', None, 'names one part of a multi-part store; reading that part alone')
		return ([path] if os.path.isfile(path) else []), path
	directory = os.path.dirname(name.store)
	base = os.path.basename(name.store)
	own = os.path.basename(path)
	try:
		entries = os.listdir(directory or '.')
	except OSError:
		entries = []
	numbered = []
	for entry in entries:
		if entry == own or not (entry == base or entry.startswith(base + '.')):
			continue
		part = _parsePartName(entry)
		if not part.ext or part.store != base or part.rotated:
			continue
		if part.ordinal is None:
			if reporter is not None:
				reporter.note('part0-ambiguous', None, 'ignored {} beside {}: only the named file is part 0'.format(entry, own))
			continue
		numbered.append((part.ordinal, entry))
	numbered.sort()
	if reporter is not None:
		for i in range(1, len(numbered)):
			if numbered[i][0] == numbered[i - 1][0]:
				reporter.note('ordinal-tie', None, 'parts {} and {} share an ordinal; ordered by name'.format(numbered[i - 1][1], numbered[i][1]))
	parts = [path] if os.path.isfile(path) else []
	parts += [os.path.join(directory, entry) for _, entry in numbered]
	return parts, (parts[-1] if numbered else path)



def _specVerify(fileName, delimiter, reporter):
	"""Replay the store ``fileName`` names and return its checksum mismatches (spec §15).

	Each mismatch is ``(partPath, lineNo, algo, expected, computed)``, where
	``lineNo`` is the 1-based line of the checksum marker in its part. The
	mismatches are returned rather than reported. A checksum marker whose
	algorithm this Python lacks is reported, since it cannot be verified.
	"""
	state = _SpecState()
	parts, _ = _storeParts(fileName, reporter)
	for _, _, text, _ in _specReplay(parts, delimiter, state, reporter, []):
		check = _CHECKSUM_RE.match(text.split(delimiter, 1)[0]) if text.startswith('#_') else None
		if check:
			reporter.note('checksum-unsupported', None, '#_checksum_{}_# not verified: this Python does not provide that algorithm'.format(check.group(1).lower()))
	reporter.discard('checksum')
	return state.mismatches


class _SpecLoad(object):
	"""Result of ``_specLoad``."""

	def __init__(self):
		self.data = None
		self.state = None
		self.parts = []
		self.active = None
		self.infos = []
		self.transitions = []
		self.correctColumnNum = -1
		self.lastLine = None
		self.headerLine = None


def _specLoad(fileName, delimiter, header=(), verifyHeader=True, strict=True, defaults=None,
			  correctColumnNum=-1, storeOffset=False, taskDic=None, reporter=None, teeLogger=None,
			  verbose=False):
	"""Replay the store ``fileName`` names into ``taskDic`` (design §4).

	``defaults`` is the normalised ``['#_defaults_#', ...]`` preamble. Rows are
	padded to ``max(W, len(row-bound defaults))`` with the row-bound defaults;
	nothing is trimmed or dropped for its width. Header verification follows
	3.39 (``strict`` raises ``ValueError`` on a mismatch) against the first
	line of part 0 that is neither a marker nor blank.
	"""
	if reporter is None:
		reporter = _Reporter(fileName, teeLogger)
	result = _SpecLoad()
	result.data = taskDic if taskDic is not None else {}
	data = result.data
	state = _SpecState(defaults)
	result.state = state
	parts, active = _storeParts(fileName, reporter)
	result.parts, result.active = parts, active
	activeIndex = len(parts) - 1
	width = correctColumnNum
	headerPending = bool(header and any(header) and verifyHeader)
	firstPartPending = True
	lastRowState = None
	for partIndex, offset, text, record in _specReplay(parts, delimiter, state, reporter, result.infos):
		if partIndex == 0 and firstPartPending and text.strip() and not _MARKER_RE.match(text.split(delimiter, 1)[0]):
			firstPartPending = False
			isComment = text.startswith('#')
			if isComment:
				result.headerLine = text
			if headerPending:
				headerPending = False
				candidate = text[1:] if isComment else text
				if _lineContainHeader(header, candidate, verbose=verbose, teeLogger=teeLogger, strict=strict, delimiter=delimiter):
					if not isComment:
						reporter.note('header-as-data', 'line 1', 'first line matches the header but is not a # comment; read as data key {!r} (prefix it with #)'.format(record[0] if record else text))
					if width == -1:
						width = len(header)
		if record is None:
			if verbose and text.startswith('#_'):
				f0 = text.split(delimiter, 1)[0]
				if _MARKER_RE.match(f0) and f0.lower() not in _SPEC_MARKER_KEYS:
					__teePrintOrNot('Ignored unrecognised marker line {!r}'.format(text[:80]), teeLogger=teeLogger)
			continue
		key, row = record
		if row is None:
			data.pop(key, None)
			continue
		width = _specPadRow(row, state, width)
		result.lastLine = (offset, row)
		if storeOffset and partIndex == activeIndex and not result.infos[partIndex].codec:
			if state.rowState is not lastRowState:
				result.transitions.append((offset, state.rowState))
				lastRowState = state.rowState
			data[key] = offset
		else:
			data[key] = row
	if headerPending:
		_lineContainHeader(header, '', verbose=verbose, teeLogger=teeLogger, strict=strict, delimiter=delimiter)
	result.correctColumnNum = width
	return result


def _specPadRow(row, state, width):
	"""Pad a data row in place to the store width with its row-bound defaults (design §4.7).

	``width`` is the store width so far (-1 before the first data row); the
	width after this row is returned. Nothing is trimmed.
	"""
	if width == -1:
		width = len(state.defaults) if len(state.defaults) > 1 else len(row)
	target = max(width, len(state.defaults))
	if len(row) < target:
		bound = state.defaults
		row.extend(bound[j] if j < len(bound) else '' for j in range(len(row), target))
	return width


def _specOptions(fileName, delimiter, encoding, reporter):
	"""Return ``(delimiter, 'utf8')`` for a spec path, overriding conflicting arguments (spec §4.1, §5.1)."""
	expected = _EXTENSION_DELIMITERS[_parsePartName(fileName).ext]
	if delimiter is not ... and delimiter and get_delimiter(delimiter) != expected:
		reporter.note('delimiter', None, 'delimiter {!r} conflicts with the file extension; using {!r} (spec §5.1)'.format(get_delimiter(delimiter), expected))
	if encoding:
		try:
			name = codecs.lookup(encoding).name
		except LookupError:
			name = str(encoding)
		if name != 'utf-8':
			reporter.note('encoding', None, 'encoding {!r} conflicts with spec §4.1; using UTF-8'.format(encoding))
	return expected, 'utf8'


def _specHeaderComment(header, delimiter):
	"""Format a header as a spec §11 comment line (without its '\\n')."""
	line = delimiter.join(_specEncodeField(cell, delimiter) for cell in header)
	return line if line.startswith('#') else '#' + line


def _specEnsureStore(fileName, createIfNotExist, header, defaults, strict, teeLogger, delimiter):
	"""Make sure the store exists (3.39 ``_verifyFileExistence`` semantics).

	A new file starts with the header comment and, when ``defaults`` carry a
	value, the ``#_defaults_#`` marker; with neither it is 0 bytes. Returns
	False when the store is missing and may not be created (non-strict).
	"""
	parts, _ = _storeParts(fileName)
	if parts:
		return True
	if not createIfNotExist:
		if strict:
			__teePrintOrNot('File not found','error',teeLogger=teeLogger)
			raise FileNotFoundError("File not found")
		return False
	lines = []
	if header and any(header):
		lines.append(_specHeaderComment(header, delimiter))
	if defaults and any(defaults[1:]):
		lines.append(_specFormatRecord(defaults, delimiter, marker=True))
	content = ''.join(line + '\n' for line in lines).encode('utf-8')
	codec = _parsePartName(fileName).codec
	try:
		with open(fileName, 'xb') as f:
			f.write(_compressBytes(codec, content) if codec and content else content)
		__teePrintOrNot('Created '+fileName,teeLogger=teeLogger)
	except FileExistsError:
		pass
	except Exception:
		__teePrintOrNot('Failed to create '+fileName,'error',teeLogger=teeLogger)
		if strict:
			raise FileNotFoundError("Failed to create file")
		return False
	return True


def _specReadTabularFile(fileName, teeLogger=None, header='', createIfNotExist=False, lastLineOnly=False,
						 verifyHeader=True, verbose=False, taskDic=None, encoding='utf8', strict=True,
						 delimiter=..., defaults=..., correctColumnNum=-1, storeOffset=False):
	"""``readTabularFile`` for spec paths (same signature and return values as 3.39)."""
	if taskDic is None:
		taskDic = {}
	reporter = _Reporter(fileName, teeLogger)
	try:
		delimiter, encoding = _specOptions(fileName, delimiter, encoding, reporter)
		header = _formatHeader(header, verbose=verbose, teeLogger=teeLogger, delimiter=delimiter)
		initial = _normalizeDefaults(defaults, delimiter)
		if not _specEnsureStore(fileName, createIfNotExist, header, initial, strict, teeLogger, delimiter):
			if lastLineOnly:
				return -1 if storeOffset else []
			return taskDic
		load = _specLoad(fileName, delimiter, header=header, verifyHeader=verifyHeader, strict=strict,
						 defaults=initial, correctColumnNum=correctColumnNum,
						 storeOffset=storeOffset and not lastLineOnly,
						 taskDic={} if lastLineOnly else taskDic, reporter=reporter,
						 teeLogger=teeLogger, verbose=verbose)
	finally:
		reporter.flush()
	if isinstance(defaults, list):
		defaults[:] = load.state.defaults
	if lastLineOnly:
		if load.lastLine is None:
			return -1 if storeOffset else []
		return load.lastLine[0] if storeOffset else load.lastLine[1]
	return taskDic


def _specRepairCompressed(path, reporter):
	"""Re-encode a damaged compressed part in place, keeping every committed record.

	The original raw bytes are first copied to a new file
	``<path>.damaged-<timestamp>`` (``.1``, ``.2``, ... appended when that name
	is taken) and synced to disk. Returns False when the part turned out to be
	intact.
	"""
	info = _PartInfo(path)
	with _pathLock(path):
		with open(path, 'r+b') as f:
			_lockFile(f)
			try:
				content = b''.join(_decompressChunks(f, info))
				keep = content[:content.rfind(b'\n') + 1]
				if not info.damaged and len(keep) == len(content):
					return False
				first = backup = '{}.damaged-{}'.format(path, time.strftime('%Y%m%dT%H%M%S'))
				suffix = 0
				while True:
					try:
						copy = open(backup, 'xb')  # never overwrite an earlier backup
						break
					except FileExistsError:
						suffix += 1
						backup = '{}.{}'.format(first, suffix)
				with copy:
					f.seek(0)
					shutil.copyfileobj(f, copy)
					copy.flush()
					os.fsync(copy.fileno())  # the backup is durable before the part is overwritten
				raw = _compressBytes(info.codec, keep) if keep else b''
				f.seek(0)
				f.write(raw)
				f.truncate()
				f.flush()
				os.fsync(f.fileno())
			finally:
				_unlockFile(f)
	reporter.note('repaired', None, 'repaired a damaged compressed part in place; the original bytes are in {}'.format(backup))
	return True


def _specAppendPayload(path, payload, reporter, repair=False, fsync=False):
	"""Append whole records to a spec part (design §5.4).

	Uncompressed: an uncommitted tail is truncated first. Compressed: the
	payload becomes a new member, after ``_specRepairCompressed`` when
	``repair`` is set. A missing part is created.
	"""
	codec = _parsePartName(path).codec
	if not codec:
		_lockedAppend(path, payload, 'truncate', reporter, fsync=fsync)
		return
	if repair and os.path.exists(path):
		_specRepairCompressed(path, reporter)
	with _pathLock(path):
		with open(path, 'ab') as f:
			_lockFile(f)
			try:
				f.write(_compressBytes(codec, payload))
				f.flush()
				if fsync:
					os.fsync(f.fileno())
			finally:
				_unlockFile(f)


#: Reads a scrub or clear makes before it gives up on a store that keeps changing.
_SPEC_REWRITE_ATTEMPTS = 3


def _specStamp(st):
	"""What identifies a part's bytes between a read and a rewrite: (inode, size, mtime)."""
	return (st.st_ino, st.st_size, st.st_mtime_ns)


def _specSnapshot(fileName):
	"""Return ``(parts, stamp)`` before a read: ``stamp`` is the single part's ``_specStamp``, else None."""
	parts, _ = _storeParts(fileName)
	if len(parts) != 1:
		return parts, None
	try:
		return parts, _specStamp(os.stat(parts[0]))
	except OSError:
		return parts, None


def _specRewriteInPlace(path, content, expect=None):
	"""Replace a part's bytes in place (same inode, spec §18.7 / §19.8).

	``content`` is uncompressed; it is compressed for a compressed part. The
	file is left alone when its bytes already match. Returns True if written,
	False if unchanged. ``expect`` is a ``_specStamp`` taken before the caller
	read the part: when the part no longer matches it under the lock, another
	writer committed in between, so nothing is written and None is returned.
	"""
	codec = _parsePartName(path).codec
	raw = _compressBytes(codec, content) if codec and content else content
	with _pathLock(path):
		with open(path, 'r+b') as f:
			_lockFile(f)
			try:
				st = os.fstat(f.fileno())
				if expect is not None and _specStamp(st) != expect:
					return None
				if st.st_size == len(raw) and f.read() == raw:
					return False
				f.seek(0)
				f.write(raw)
				f.truncate()
				f.flush()
				os.fsync(f.fileno())
				return True
			finally:
				_unlockFile(f)


def _specPreamble(state, delimiter, compensate=False, version=True):
	"""Marker lines that re-create ``state`` (spec §19.2.3a): version, non-default markers, defaults last.

	``compensate`` forces stripping and fill-empty off for the rows that follow
	(the §19.4 compensating layout; ``_specPostamble`` restores them).
	"""
	d = delimiter
	lines = ['#_version_#' + d + '1'] if version else []
	if compensate:
		lines.append('#_strip_trailing_whites_#' + d + 'false')
		lines.append('#_fill_empty_with_default_#' + d + 'false')
	else:
		if not state.strip:
			lines.append('#_strip_trailing_whites_#' + d + 'false')
		if state.fillEmpty:
			lines.append('#_fill_empty_with_default_#' + d + 'true')
	if not state.returnDefaults:
		lines.append('#_return_defaults_when_missing_#' + d + 'false')
	if state.rotate != 'keep':
		lines.append('#_rotate_#' + d + state.rotate)
	if state.writeAck != 'memory':
		lines.append('#_write_ack_#' + d + state.writeAck)
	if any(state.defaults[1:]):
		lines.append(_specFormatRecord(state.defaults, d, marker=True))
	return lines


def _specCheckHeader(fileName, header, delimiter, strict, verbose, teeLogger):
	"""3.39 header verification against the first non-marker, non-blank line of part 0."""
	parts, _ = _storeParts(fileName)
	line = ''
	if parts:
		reporter = _Reporter(fileName)  # not flushed: the full read reports these
		lines = _iterPartLines(_PartInfo(parts[0]), reporter)
		try:
			for raw, _ in lines:
				text = _decodeLine(raw, reporter, None)
				if text.strip() and not _MARKER_RE.match(text.split(delimiter, 1)[0]):
					line = text
					break
		finally:
			lines.close()
	candidate = line[1:] if line.startswith('#') else line
	return _lineContainHeader(header, candidate, verbose=verbose, teeLogger=teeLogger, strict=strict, delimiter=delimiter)


def _specAppendLinesTabularFile(fileName, linesToAppend, teeLogger=None, header='', createIfNotExist=False,
								verifyHeader=True, verbose=False, encoding='utf8', strict=True, delimiter=...):
	"""``appendLinesTabularFile`` for spec paths: rows are written exactly as given (design §5.2)."""
	reporter = _Reporter(fileName, teeLogger)
	try:
		delimiter, encoding = _specOptions(fileName, delimiter, encoding, reporter)
		header = _formatHeader(header, verbose=verbose, teeLogger=teeLogger, delimiter=delimiter)
		records = []
		for line in linesToAppend:
			if isinstance(linesToAppend, dict):
				key = line
				line = linesToAppend[key]
			if isinstance(line, str):
				line = line.split(delimiter)
			elif line:
				line = [item if isinstance(item, str) else _cellText(item) for item in line]
			else:
				line = []
			if isinstance(linesToAppend, dict) and (not line or line[0] != key):
				line = [key] + line
			if not line:
				continue
			records.append(_specFormatRecord(line, delimiter, marker=bool(_MARKER_RE.match(line[0]))))
		_specAppendRecords(fileName, records, reporter, teeLogger=teeLogger, header=header,
						   createIfNotExist=createIfNotExist, verifyHeader=verifyHeader, verbose=verbose,
						   strict=strict, delimiter=delimiter)
	finally:
		reporter.flush()


def _specAppendRecords(fileName, records, reporter, teeLogger=None, header=(), createIfNotExist=False,
					   verifyHeader=True, verbose=False, strict=True, delimiter='\t'):
	"""Append formatted records (each without its '\\n') to the store in one write (spec §18.2).

	The store is created first when ``createIfNotExist`` allows it (see
	``_specEnsureStore``); ``header`` is a formatted header list. Returns False
	when the store is missing and may not be created.
	"""
	if not _specEnsureStore(fileName, createIfNotExist, header, [DEFAULTS_INDICATOR_KEY], strict, teeLogger, delimiter):
		return False
	if not records:
		if verbose:
			__teePrintOrNot(f"No lines to append to {fileName}",teeLogger=teeLogger)
		return True
	if any(header) and verifyHeader:
		_specCheckHeader(fileName, header, delimiter, strict, verbose, teeLogger)
	_, active = _storeParts(fileName, reporter)
	_specAppendPayload(active, ''.join(record + '\n' for record in records).encode('utf-8'), reporter)
	if verbose:
		__teePrintOrNot(f"Appended {len(records)} lines to {fileName}",teeLogger=teeLogger)
	return True


def _specClearTabularFile(fileName, teeLogger=None, header='', verifyHeader=False, verbose=False,
						  encoding='utf8', strict=False, delimiter=...):
	"""``clearTabularFile`` for spec paths (design §5.5, §5.8). Returns True when the store was cleared.

	A single file is read without a lock and rewritten in place only if no
	other writer changed it since the read (else it is read again, up to
	``_SPEC_REWRITE_ATTEMPTS`` times). A path naming one part of a multi-part
	store is refused.
	"""
	reporter = _Reporter(fileName, teeLogger)
	attempt = None
	cleared = False
	try:
		delimiter, encoding = _specOptions(fileName, delimiter, encoding, reporter)
		header = _formatHeader(header, verbose=verbose, teeLogger=teeLogger, delimiter=delimiter)
		name = _parsePartName(fileName)
		if name.ordinal is not None or name.rotated:
			reporter.note('fragment', None, 'clear skipped: {} names one part of a multi-part store; clear the store path instead; nothing was written'.format(fileName))
			return False
		if not _specEnsureStore(fileName, True, header, [DEFAULTS_INDICATOR_KEY], False, teeLogger, delimiter):
			raise FileNotFoundError("Something catastrophic happened! File still not found after creation")
		headerChecked = False
		for _ in range(_SPEC_REWRITE_ATTEMPTS):
			attempt = _Reporter(fileName, teeLogger)  # only the last read reports its tolerance events
			parts, stamp = _specSnapshot(fileName)
			load = _specLoad(fileName, delimiter, header=header, verifyHeader=False, strict=False,
							 reporter=attempt, teeLogger=teeLogger, verbose=verbose)
			if len(load.parts) > 1:
				tombstones = ''.join(_specFormatRecord([key], delimiter) + '\n' for key in load.data)
				if tombstones:
					_specAppendPayload(load.active, tombstones.encode('utf-8'), attempt)
				attempt.note('multipart', None, 'cleared a multi-part store by appending tombstones; older parts are not compacted')
				cleared = True
				break
			if stamp is None or load.parts != parts:
				continue  # the set of parts changed under the read
			if any(header) and verifyHeader and load.headerLine is not None and not headerChecked:
				headerChecked = True
				if not _lineContainHeader(header, load.headerLine[1:], verbose=verbose, teeLogger=teeLogger, strict=strict, delimiter=delimiter):
					__teePrintOrNot(f'Warning: Header mismatch in {fileName}. Keeping original header in file...','warning',teeLogger)
			lines = []
			headerLine = load.headerLine or (_specHeaderComment(header, delimiter) if any(header) else None)
			if headerLine:
				lines.append(headerLine)
			lines += _specPreamble(load.state, delimiter, version=False)
			if _specRewriteInPlace(load.parts[0], ''.join(line + '\n' for line in lines).encode('utf-8'), expect=stamp) is not None:
				cleared = True
				break
		else:
			attempt.note('changing', None, 'clear skipped: the store kept changing while it was being cleared; nothing was written')
	finally:
		reporter.flush()
		if attempt is not None:
			attempt.flush()
	if verbose and cleared:
		__teePrintOrNot(f"Cleared {fileName}",teeLogger=teeLogger)
	return cleared


def _specPostamble(state, delimiter):
	"""Marker lines that restore stripping / fill-empty after a compensating scrub layout."""
	lines = []
	if state.strip:
		lines.append('#_strip_trailing_whites_#' + delimiter + 'true')
	if state.fillEmpty:
		lines.append('#_fill_empty_with_default_#' + delimiter + 'true')
	return lines


def _specScrubCells(row, state):
	"""Cells to write for a materialised row: extended with '' to the final defaults' width.

	A materialised row already carries every column that is not '' (design
	§4.7); the explicit '' cells keep a final default it was never bound to
	from filling the columns beyond it.
	"""
	final = state.defaults
	if len(final) > len(row):
		return row + [''] * (len(final) - len(row))
	return row


def _specRoundTrips(cells, state):
	"""True when ``cells`` read back unchanged under the final marker state (spec §19.4)."""
	if state.strip:
		for cell in cells:
			if cell and cell[-1] in ' \t':
				return False
	if state.fillEmpty:
		final = state.defaults
		for j in range(1, len(cells)):
			if not cells[j] and j < len(final) and final[j]:
				return False
	return True


def _specScrubTabularFile(fileName, teeLogger=None, header='', createIfNotExist=False, lastLineOnly=False,
						  verifyHeader=True, verbose=False, taskDic=None, encoding='utf8', strict=False,
						  delimiter=..., defaults=..., correctColumnNum=-1):
	"""``scrubTabularFile`` for spec paths: archival in-place compaction (design §5.7)."""
	if lastLineOnly:
		return _specReadTabularFile(fileName, teeLogger=teeLogger, header=header, createIfNotExist=createIfNotExist,
									lastLineOnly=True, verifyHeader=verifyHeader, verbose=verbose, encoding=encoding,
									strict=strict, delimiter=delimiter, defaults=defaults, correctColumnNum=correctColumnNum)
	return _specScrub(fileName, teeLogger=teeLogger, header=header, createIfNotExist=createIfNotExist,
					  verifyHeader=verifyHeader, verbose=verbose, taskDic=taskDic, encoding=encoding,
					  strict=strict, delimiter=delimiter, defaults=defaults, correctColumnNum=correctColumnNum)[0]


def _specScrub(fileName, teeLogger=None, header='', createIfNotExist=False, verifyHeader=True, verbose=False,
			   taskDic=None, encoding='utf8', strict=False, delimiter=..., defaults=..., correctColumnNum=-1):
	"""Compact a single-part store in place; return ``(taskDic, done)``.

	The store is read without a lock and rewritten in place only if no other
	writer changed it since the read (else it is read again, up to
	``_SPEC_REWRITE_ATTEMPTS`` times), so a record committed meanwhile is
	never compacted away. ``done`` is False when nothing was written because
	the store is missing, has several parts (or the path names one part), or
	kept changing.
	"""
	if taskDic is None:
		taskDic = {}
	reporter = _Reporter(fileName, teeLogger)
	attempt = None
	try:
		delimiter, encoding = _specOptions(fileName, delimiter, encoding, reporter)
		header = _formatHeader(header, verbose=verbose, teeLogger=teeLogger, delimiter=delimiter)
		initial = _normalizeDefaults(defaults, delimiter)
		if not _specEnsureStore(fileName, createIfNotExist, header, initial, strict, teeLogger, delimiter):
			return taskDic, False
		name = _parsePartName(fileName)
		given = list(taskDic.items()) if taskDic else []
		for tries in range(_SPEC_REWRITE_ATTEMPTS):
			if tries:
				# Read again into the dict as the caller passed it.
				taskDic.clear()
				taskDic.update(given)
			attempt = _Reporter(fileName, teeLogger)  # only the last read reports its tolerance events
			parts, stamp = _specSnapshot(fileName)
			load = _specLoad(fileName, delimiter, header=header, verifyHeader=verifyHeader, strict=strict,
							 defaults=initial, correctColumnNum=correctColumnNum, taskDic=taskDic,
							 reporter=attempt, teeLogger=teeLogger, verbose=verbose)
			if len(load.parts) != 1 or name.ordinal is not None or name.rotated:
				attempt.note('multipart', None, 'scrub skipped: 4.1 does not compact multi-part stores or single parts of them; nothing was written')
				return taskDic, False
			if stamp is None or load.parts != parts:
				continue  # the set of parts changed under the read
			state = load.state
			rows = [_specScrubCells(row, state) for row in taskDic.values()]
			compensate = not all(_specRoundTrips(cells, state) for cells in rows)
			lines = []
			headerLine = load.headerLine or (_specHeaderComment(header, delimiter) if any(header) else None)
			if headerLine:
				lines.append(headerLine)
			lines += _specPreamble(state, delimiter, compensate=compensate)
			lines += [_specFormatRecord(cells, delimiter) for cells in rows]
			if compensate:
				lines += _specPostamble(state, delimiter)
			written = _specRewriteInPlace(load.parts[0], ''.join(line + '\n' for line in lines).encode('utf-8'), expect=stamp)
			if written is None:
				continue  # another writer committed after the read
			if verbose:
				__teePrintOrNot(f"Scrubbed {fileName}: {len(rows)} records, {'rewritten' if written else 'unchanged'}",teeLogger=teeLogger)
			return taskDic, True
		attempt.note('changing', None, 'scrub skipped: the store kept changing while it was being compacted; nothing was written')
	finally:
		reporter.flush()
		if attempt is not None:
			attempt.flush()
	return taskDic, False


def getListView(tsvzDic,header = [],delimiter = ...):
	if header:
		if isinstance(header,str):
			header = header.split(get_delimiter(delimiter))
		elif not isinstance(header,list):
			try:
				header = list(header)
			except Exception:
				header = []
	if not tsvzDic:
		if not header:
			return []
		else:
			return [header]
	if not header:
		return list(tsvzDic.values())
	else:
		values = list(tsvzDic.values())
		if values[0] and values[0] == header:
			return values
		else:
			return [header] + values

# create a tsv class that functions like a ordered dictionary but will update the file when modified
class TSVZed(OrderedDict):
	"""
	A thread-safe, file-backed ordered dictionary for managing TSV (Tab-Separated Values) files.
	TSVZed extends OrderedDict to provide automatic synchronization between an in-memory
	dictionary and a TSV file on disk. It supports concurrent file access, automatic
	persistence, and configurable sync strategies.
	Parameters
	----------
	fileName : str
		Path to the TSV file to be managed.
	teeLogger : object, optional
		Logger object with a teelog method for logging messages. If None, uses print.
	header : str, optional
		Column header line for the TSV file. Used for validation and file creation.
	createIfNotExist : bool, default=True
		If True, creates the file if it doesn't exist.
	verifyHeader : bool, default=True
		If True, verifies that the file header matches the provided header.
	rewrite_on_load : bool, default=True
		If True, rewrites the entire file when loading to ensure consistency.
	rewrite_on_exit : bool, default=False
		If True, rewrites the entire file when closing/exiting.
	rewrite_interval : float, default=0
		Minimum time interval (in seconds) between full file rewrites. 0 means do not rewrite.
	append_check_delay : float, default=0.01
		Time delay (in seconds) between checks of the append queue by the worker thread.
	monitor_external_changes : bool, default=True
		If True, monitors and detects external file modifications.
	verbose : bool, default=False
		If True, prints detailed operation logs.
	encoding : str, default='utf8'
		Character encoding for reading/writing the file.
	delimiter : str, optional
		Field delimiter character. Auto-detected from filename if not specified.
	defaults : list or str, optional
		Default values for columns when values are missing.
	strict : bool, default=False
		If True, enforces strict validation of column counts and raises errors on mismatch.
	correctColumnNum : int, default=-1
		Expected number of columns. -1 means auto-detect from header or first record.
	Attributes
	----------
	version : str
		Version of the TSVZed implementation.
	dirty : bool
		True if the in-memory data differs from the file on disk.
	deSynced : bool
		True if synchronization with the file has failed or external changes detected.
	memoryOnly : bool
		If True, changes are kept in memory only and not written to disk.
	appendQueue : deque
		Queue of lines waiting to be appended to the file.
	writeLock : threading.Lock
		Lock for ensuring thread-safe file operations.
	shutdownEvent : threading.Event
		Event signal for stopping the append worker thread.
	appendThread : threading.Thread
		Background thread that handles asynchronous file appending.
	Methods
	-------
	load()
		Load or reload data from the TSV file.
	reload()
		Refresh data from the TSV file, discarding in-memory changes.
	rewrite(force=False, reloadInternalFromFile=None)
		Rewrite the entire file with current in-memory data.
	mapToFile()
		Synchronize in-memory data to the file using in-place updates.
	hardMapToFile()
		Completely rewrite the file from scratch with current data.
	clear()
		Clear all data from memory and optionally the file.
	clear_file()
		Clear the file, keeping only the header.
	commitAppendToFile()
		Write all queued append operations to the file.
	stopAppendThread()
		Stop the background append worker thread and perform final sync.
	setDefaults(defaults)
		Set default values for columns.
	getListView()
		Get a list representation of the data with headers.
	getResourceUsage(return_dict=False)
		Get current resource usage statistics.
	checkExternalChanges()
		Check if the file has been modified externally.
	close()
		Close the TSVZed object, stopping background threads and syncing data.
	Notes
	-----
	- The class uses a background thread to handle asynchronous file operations.
	- File locking is implemented for both POSIX and Windows systems.
	- Keys starting with '#' are treated as comments and not persisted to file.
	- The special key '#_defaults_#' is used to store column default values.
	- Supports compressed file formats through automatic detection.
	- Thread-safe for concurrent access from multiple threads.
	Examples
	--------
	>>> with TSVZed('data.tsv', header='id\tname\tvalue') as tsv:
	...     tsv['key1'] = ['key1', 'John', '100']
	...     tsv['key2'] = ['key2', 'Jane', '200']
	...     print(tsv['key1'])
	['key1', 'John', '100']
	>>> tsv = TSVZed('data.tsv', verbose=True, rewrite_on_exit=True)
	>>> tsv['key3'] = 'key3\tBob\t300'
	>>> tsv.close()
	"""
	def __teePrintOrNot(self,message,level = 'info'):
		try:
			if self.teeLogger:
				self.teeLogger.teelog(message,level)
			else:
				print(message,flush=True)
		except Exception:
			print(message,flush=True)

	def getResourceUsage(self,return_dict = False):
		return get_resource_usage(return_dict = return_dict)

	def __init__ (self,fileName,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = ...,
				  rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,monitor_external_changes = True,
				  verbose = False,encoding = 'utf8',delimiter = ...,defaults = None,strict = False,correctColumnNum = -1):
		super().__init__()
		self._constructed = False  # setDefaults() writes a #_defaults_# marker only once this is True
		self._spec = _isSpecPath(fileName)
		self.version = version
		self.strict = strict
		self.externalFileUpdateTime = getFileUpdateTimeNs(fileName)
		self.lastUpdateTime = self.externalFileUpdateTime
		self._fileName = fileName
		self.teeLogger = teeLogger
		if self._spec:
			reporter = _Reporter(fileName, teeLogger)
			self.delimiter, encoding = _specOptions(fileName, delimiter, encoding, reporter)
			reporter.flush()
		else:
			self.delimiter = get_delimiter(delimiter,file_name=fileName)
		self.setDefaults(defaults)
		self._constructorDefaults = list(self.defaults)
		self.header = _formatHeader(header,verbose = verbose,teeLogger = self.teeLogger,delimiter=self.delimiter)
		self.correctColumnNum = correctColumnNum
		self.createIfNotExist = createIfNotExist
		self.verifyHeader = verifyHeader
		if rewrite_on_load is ...:
			# 3.39 rewrote on load by default; spec stores are never rewritten implicitly.
			rewrite_on_load = not self._spec
		self.rewrite_on_load = rewrite_on_load
		self.rewrite_on_exit = rewrite_on_exit
		self.rewrite_interval = rewrite_interval
		self.monitor_external_changes = monitor_external_changes
		if not monitor_external_changes:
			self.__teePrintOrNot(f"Warning: External changes monitoring disabled for {self._fileName}. Will overwrite external changes.",'warning')
		self.verbose = verbose
		if append_check_delay < 0:
			append_check_delay = 0.00001
			self.__teePrintOrNot('append_check_delay cannot be less than 0, setting it to 0.00001','error')
		self.append_check_delay = append_check_delay
		self.appendQueue = deque()
		self.dirty = False
		self.deSynced = False
		self.memoryOnly = False
		self.encoding = encoding
		self.writeLock = threading.Lock()
		self.shutdownEvent = threading.Event()
		if self._spec:
			for flag, given in (('rewrite_on_load', rewrite_on_load), ('rewrite_on_exit', rewrite_on_exit),
								('rewrite_interval', rewrite_interval)):
				if given:
					_warnOnce(self, flag, fileName,
							  f'{flag}={given!r} is ignored for .tsvz; behaviour undefined; use scrubTabularFile for archival compaction',
							  teeLogger)
			self._specState = _SpecState(self.defaults)
			self._parts = []
			self._activePath = fileName
			self._activeInfo = None
			self._dirMtimeNs = _dirMtimeNs(fileName)
			self._writeFailing = False
			self._forceRepair = False
			self._specLoaded = False
		#self.appendEvent = threading.Event()
		self.appendThread  = threading.Thread(target=self._appendWorker,daemon=True)
		self.appendThread.start()
		self.load()
		atexit.register(self.stopAppendThread)
		self._constructed = True

	def setDefaults(self,defaults):
		if not defaults:
			defaults = []
		if isinstance(defaults,str):
			defaults = defaults.split(self.delimiter)
		elif not isinstance(defaults,list):
			try:
				defaults = list(defaults)
			except Exception:
				if self.verbose:
					self.__teePrintOrNot('Invalid defaults, setting defaults to empty.','error')
				defaults = []
		defaults = [str(s).rstrip() if s else '' for s in defaults]
		if not any(defaults):
			defaults = []
		if not defaults or defaults[0] != DEFAULTS_INDICATOR_KEY:
			defaults = [DEFAULTS_INDICATOR_KEY]+defaults
		if self._spec and self._constructed:
			# Spec rows are written sparse and read back under the file's
			# #_defaults_#, so new defaults are written as that marker.
			self._specSetMarker(DEFAULTS_INDICATOR_KEY, defaults)
			return
		self.defaults = defaults

	def load(self):
		self.reload()
		if self.rewrite_on_load and not self._spec:
			self.rewrite(force = True,reloadInternalFromFile = False)
		return self

	def reload(self):
		if self._spec:
			return self._specReload()
		# Load or refresh data from the TSV file
		mo = self.memoryOnly
		self.memoryOnly = True
		if self.verbose:
			self.__teePrintOrNot(f"Loading {self._fileName}")
		super().clear()
		readTabularFile(self._fileName, teeLogger = self.teeLogger, header = self.header,
					createIfNotExist = self.createIfNotExist, verifyHeader = self.verifyHeader,
					verbose = self.verbose, taskDic = self,encoding = self.encoding if self.encoding else None,
					strict = self.strict, delimiter = self.delimiter, defaults=self.defaults)
		if self.verbose:
			self.__teePrintOrNot(f"Loaded {len(self)} records from {self._fileName}")
		if self.header and any(self.header) and self.verifyHeader:
			self.correctColumnNum = len(self.header)
		elif self:
			self.correctColumnNum = len(self[next(iter(self))])
		else:
			self.correctColumnNum = -1
		if self.verbose:
			self.__teePrintOrNot(f"correctColumnNum: {self.correctColumnNum}")
		#super().update(loadedData)
		if self.verbose:
			self.__teePrintOrNot(f"TSVZed({self._fileName}) loaded")
		self.externalFileUpdateTime = getFileUpdateTimeNs(self._fileName)
		self.lastUpdateTime = self.externalFileUpdateTime
		self.memoryOnly = mo
		return self

	def _specReload(self):
		"""Load a spec store into memory (design §4); rows bypass __setitem__ so nothing is re-normalised."""
		if self.verbose:
			self.__teePrintOrNot(f"Loading {self._fileName}")
		reporter = _Reporter(self._fileName, self.teeLogger)
		load = None
		try:
			if _specEnsureStore(self._fileName, self.createIfNotExist, self.header, self._constructorDefaults,
								self.strict, self.teeLogger, self.delimiter):
				load = _specLoad(self._fileName, self.delimiter, header=self.header, verifyHeader=self.verifyHeader,
								 strict=self.strict, defaults=self._constructorDefaults, taskDic=OrderedDict(),
								 reporter=reporter, teeLogger=self.teeLogger, verbose=self.verbose)
		finally:
			reporter.flush()
		OrderedDict.clear(self)
		width = -1
		if load is not None:
			for key, row in load.data.items():
				OrderedDict.__setitem__(self, key, row)
			self._specState = load.state
			self.defaults = list(load.state.defaults)
			self._parts, self._activePath = load.parts, load.active
			info = load.infos[-1] if load.infos else None
			self._activeInfo = info if info is not None and info.path == load.active else None
			width = load.correctColumnNum
		if self.header and any(self.header) and self.verifyHeader:
			self.correctColumnNum = len(self.header)
		else:
			self.correctColumnNum = width
		if self.verbose:
			self.__teePrintOrNot(f"Loaded {len(self)} records from {self._fileName}")
		self.externalFileUpdateTime = getFileUpdateTimeNs(self._activePath)
		self.lastUpdateTime = self.externalFileUpdateTime
		self._dirMtimeNs = _dirMtimeNs(self._fileName)
		if (load is not None and not load.state.sawDefaults and any(self._constructorDefaults[1:])
				and not self.memoryOnly):
			# Design §5.3: persist constructor defaults by appending, as 3.39 did by rewriting.
			self.appendQueue.append(_specFormatRecord(self._constructorDefaults, self.delimiter, marker=True))
			self._specState.sawDefaults = True
		self._specLoaded = True
		return self

	def __missing__(self, key):
		# Spec §14.5: while #_return_defaults_when_missing_# is true a missing key
		# reads as the active defaults. Only t[key] reaches this; get / in /
		# setdefault / pop keep dict semantics.
		if self._spec and self._specState.returnDefaults:
			row = [key] + list(self.defaults[1:])
			if self.correctColumnNum > len(row):
				row += [''] * (self.correctColumnNum - len(row))
			return row
		raise KeyError(key)

	@property
	def dialect(self):
		"""'tsvz' for tsvz-spec-v1 files (.tsvz/.csvz/.nsvz/.psvz), 'tsv' for 3.39-format files."""
		return 'tsvz' if self._spec else 'tsv'

	def _specSync(self, reloadInternalFromFile=None):
		"""The part of 3.39's rewrite() that .tsvz keeps: reload after an external change."""
		if not self.deSynced:
			return False
		if reloadInternalFromFile is None:
			reloadInternalFromFile = self.monitor_external_changes
		if reloadInternalFromFile:
			try:
				self.commitAppendToFile()
				if self.appendQueue:
					return False  # writes are failing: keep the queue in memory, retry next tick
				self.reload()
			except Exception as e:
				self.__teePrintOrNot(f"Failed to reload {self._fileName} after an external change: {e}; keeping the in-memory state",'error')
				# Mark the change as seen so the failure is reported once, not every tick.
				try:
					self.externalFileUpdateTime = getFileUpdateTimeNs(self._activePath)
					self._dirMtimeNs = _dirMtimeNs(self._fileName)
					self._parts = _storeParts(self._fileName)[0]
				except Exception:
					pass
				self.deSynced = False
				return False
		self.deSynced = False
		return True

	def _specExternalChanged(self):
		"""True when another writer changed the active part or the set of parts."""
		if getFileUpdateTimeNs(self._activePath) > self.externalFileUpdateTime:
			return True
		dirMtime = _dirMtimeNs(self._fileName)
		if dirMtime != self._dirMtimeNs:
			self._dirMtimeNs = dirMtime
			if _storeParts(self._fileName)[0] != self._parts:
				return True
		return False

	def __setitem__(self,key,value):
		if self._spec:
			return self._specSetItem(key, value)
		key = str(key).rstrip()
		if not key:
			self.__teePrintOrNot('Key cannot be empty','error')
			return
		if isinstance(value,str):
			value = value.split(self.delimiter)
		# sanitize the value
		value = [str(s).rstrip() if s else '' for s in value]
		# the first field in value should be the key
		# add it if it is not there
		if not value or value[0] != key:
			value = [key]+value
		# verify the value has the correct number of columns
		if self.correctColumnNum != 1 and len(value) == 1:
			# a lone key (no following values) means delete the key, per the
			# TSVZ spec. del is a no-op when the key is absent (tolerated).
			del self[key]
			return
		elif self.correctColumnNum > 0:
			if len(value) != self.correctColumnNum:
				if self.strict:
					self.__teePrintOrNot(f"Value {value} does not have the correct number of columns: {self.correctColumnNum}. Refuse adding key...",'error')
					return
				elif self.verbose:
					self.__teePrintOrNot(f"Value {value} does not have the correct number of columns: {self.correctColumnNum}, correcting...",'warning')
				if len(value) < self.correctColumnNum:
					value += ['']*(self.correctColumnNum-len(value))
				elif len(value) > self.correctColumnNum:
					value = value[:self.correctColumnNum]
		else:
			self.correctColumnNum = len(value)
		if self.defaults and len(self.defaults) > 1:
			for i in range(1,len(value)):
				if not value[i] and i < len(self.defaults) and self.defaults[i]:
					value[i] = self.defaults[i]
					if self.verbose:
						self.__teePrintOrNot(f"    Replacing empty value at {i} with default: {self.defaults[i]}")
		if (len(value) > 1 and not any(value[1:]) and key != DEFAULTS_INDICATOR_KEY
				and not key.startswith('#') and not self.memoryOnly):
			# L2: on disk this row is a delete; make memory agree with the file.
			_warnOnce(self, 'empty-row', self._fileName,
					  f'in .tsv files a row whose values are all empty is a delete; deleted {key!r} (use .tsvz to store empty rows)',
					  self.teeLogger)
			del self[key]
			return
		if key == DEFAULTS_INDICATOR_KEY:
			self.defaults = value
			if self.verbose:
				self.__teePrintOrNot(f"Defaults set to {value}")
			if not self.memoryOnly:
				self.appendQueue.append(value)
				self.lastUpdateTime = get_time_ns()
				if self.verbose:
					self.__teePrintOrNot(f"Appending Defaults {key} to the appendQueue")
			return
		if self.verbose:
			self.__teePrintOrNot(f"Setting {key} to {value}")
		if key in self:
			if self[key] == value:
				if self.verbose:
					self.__teePrintOrNot(f"Key {key} already exists with the same value")
				return
			self.dirty = True
		# update the dictionary, 
		super().__setitem__(key,value)
		if self.memoryOnly:
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} updated in memory only")
			return
		elif key.startswith('#'):
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} updated in memory only as it starts with #")
			return
		if self.verbose:
			self.__teePrintOrNot(f"Appending {key} to the appendQueue")
		self.appendQueue.append(value)
		self.lastUpdateTime = get_time_ns()
		# if not self.appendThread.is_alive():
		#     self.commitAppendToFile()
		# else:
		#     self.appendEvent.set()

	def __getitem__(self, key):
		return super().__getitem__(str(key).rstrip())
		

	def __delitem__(self,key):
		if self._spec:
			return self._specDelItem(key)
		key = str(key).rstrip()
		if key == DEFAULTS_INDICATOR_KEY:
			self.defaults = [DEFAULTS_INDICATOR_KEY]
			if self.verbose:
				self.__teePrintOrNot("Defaults cleared")
			if not self.memoryOnly:
				self.__appendEmptyLine(key)
				if self.verbose:
					self.__teePrintOrNot(f"Appending empty default line {key}")
			return
		# delete the key from the dictionary and update the file
		if key not in self:
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} not found")
			return
		super().__delitem__(key)
		if self.memoryOnly or key.startswith('#'):
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} deleted in memory")
			return
		self.__appendEmptyLine(key)
		if self.verbose:
			self.__teePrintOrNot(f"Appending empty line {key}")
		self.lastUpdateTime = get_time_ns()
		
	def __appendEmptyLine(self,key):
		# Append a tombstone line: the key followed by empty values. On read a
		# row whose values are all empty deletes the key (see _processLine).
		# Fillers MUST be empty strings ('') -- using the delimiter character
		# here would be sanitized to '<sep>' and read back as a non-empty value,
		# resurrecting the deleted key. The key has already been removed from
		# the mapping by the caller, so self[key] must not be accessed.
		self.dirty = True
		if self._spec:
			# Spec §9.2: a tombstone is the lone key, never padded.
			self.appendQueue.append(_specFormatRecord([key], self.delimiter))
			return self
		if self.correctColumnNum > 1:
			emptyLine = [key]+['']*(self.correctColumnNum-1)
		else:
			emptyLine = [key]
		if self.verbose:
			self.__teePrintOrNot(f"Appending {emptyLine} to the appendQueue")
		self.appendQueue.append(emptyLine)
		return self

	def _specSetItem(self, key, value):
		"""__setitem__ for .tsvz (design §5.2): write the given cells, keep the padded row in memory."""
		key = str(key).rstrip()
		if not key:
			self.__teePrintOrNot('Key cannot be empty','error')
			return
		if isinstance(value,str):
			value = value.split(self.delimiter)
		value = [str(s).rstrip() if s else '' for s in value]
		if not value or value[0] != key:
			value = [key]+value
		if _MARKER_RE.match(key):
			self._specSetMarker(key, value)
			return
		if len(value) == 1:
			del self[key]
			return
		if self.strict and self.correctColumnNum > 0 and len(value) != self.correctColumnNum:
			self.__teePrintOrNot(f"Value {value} does not have the correct number of columns: {self.correctColumnNum}. Refuse adding key...",'error')
			return
		defaults = self.defaults
		for i in range(1, min(len(value), len(defaults))):
			if not value[i] and defaults[i]:
				value[i] = defaults[i]
		written = list(value)
		if self.correctColumnNum <= 0:
			self.correctColumnNum = len(defaults) if len(defaults) > 1 else len(value)
		target = max(self.correctColumnNum, len(defaults))
		if len(value) < target:
			value += [defaults[j] if j < len(defaults) else '' for j in range(len(value), target)]
		if key in self:
			if OrderedDict.__getitem__(self, key) == value:
				return
			self.dirty = True
		OrderedDict.__setitem__(self, key, value)
		if self.memoryOnly:
			return
		self.appendQueue.append(_specFormatRecord(written, self.delimiter))
		self.lastUpdateTime = get_time_ns()

	def _specSetMarker(self, key, value):
		"""Write a reserved-key marker (design §5.2 e); official markers also update the reader state."""
		keyLower = key.lower()
		if keyLower in _SPEC_MARKER_KEYS:
			reporter = _Reporter(self._fileName, self.teeLogger)
			applied = _specApplyMarker(self._specState, keyLower, value[1:], reporter, None)
			reporter.flush()
			if not applied:
				return
			self.defaults = list(self._specState.defaults)
		if self.verbose:
			self.__teePrintOrNot(f"Marker {key} set to {value[1:]}")
		if self.memoryOnly:
			return
		self.appendQueue.append(_specFormatRecord(value, self.delimiter, marker=True))
		self.lastUpdateTime = get_time_ns()

	def _specDelItem(self, key):
		key = str(key).rstrip()
		if _MARKER_RE.match(key):
			# A lone marker key resets that marker (spec §12.1).
			self._specSetMarker(key, [key])
			return
		if key not in self:
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} not found")
			return
		OrderedDict.__delitem__(self, key)
		if self.memoryOnly:
			return
		self.__appendEmptyLine(key)
		self.lastUpdateTime = get_time_ns()

	def _specCommit(self):
		"""Append the queued records to the active part (design §5.4).

		Records stay queued until the write succeeds; a failure is reported once
		and retried on the next tick. Nothing raised here may reach the worker.
		"""
		with self.writeLock:
			return self._specCommitLocked()

	def _specCommitLocked(self):
		# The caller holds self.writeLock (a non-reentrant lock).
		if not self.appendQueue:
			return self
		if self.memoryOnly:
			self.appendQueue.clear()
			return self
		items = []
		while self.appendQueue:
			items.append(self.appendQueue.popleft())
		reporter = _Reporter(self._fileName, self.teeLogger)
		try:
			payload = ''.join(item + '\n' for item in items).encode('utf-8', errors='replace')
			info = self._activeInfo
			repair = bool(self._forceRepair or (info is not None and info.codec and (info.damaged or info.tail)))
			_specAppendPayload(self._activePath, payload, reporter, repair=repair, fsync=True)
			self._forceRepair = False
			self._activeInfo = None
			self.externalFileUpdateTime = getFileUpdateTimeNs(self._activePath)
			if self._writeFailing:
				self._writeFailing = False
				_warn(f'TSVZ warning: {self._fileName}: writes recovered', self.teeLogger)
		except Exception as e:
			self.appendQueue.extendleft(reversed(items))
			# A failed append to a compressed part may have left a torn member.
			self._forceRepair = True
			if not self._writeFailing:
				self._writeFailing = True
				self.__teePrintOrNot(f"Failed to write at commitAppendToFile to {self._activePath}: {e}; will retry",'error')
				import traceback
				self.__teePrintOrNot(traceback.format_exc(),'error')
		reporter.flush()
		return self

	def _specClearFile(self):
		"""clear_file for .tsvz (design §5.8): commit queued markers, then clear in place."""
		try:
			with self.writeLock:
				self._specCommitLocked()
				if self.appendQueue:
					self.__teePrintOrNot(f"Failed to write at clear_file() to {self._fileName}: queued records could not be committed; not clearing",'error')
					self.deSynced = True
					return self
				cleared = _specClearTabularFile(self._fileName, teeLogger=self.teeLogger, header=self.header,
												verbose=self.verbose, delimiter=self.delimiter)
				self.externalFileUpdateTime = getFileUpdateTimeNs(self._activePath)
			self.dirty = False
			# A skipped clear (reported) left the store as it was: reload it into memory.
			self.deSynced = not cleared
		except Exception as e:
			self.deSynced = True
			self.__teePrintOrNot(f"Failed to write at clear_file() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
		return self

	def getListView(self):
		return getListView(self,header=self.header,delimiter=self.delimiter)

	def clear(self):
		# clear the dictionary and update the file
		super().clear()
		if self.verbose:
			self.__teePrintOrNot(f"Clearing {self._fileName}")
		if self.memoryOnly:
			return self
		self.clear_file()
		self.lastUpdateTime = self.externalFileUpdateTime
		return self

	def clear_file(self):
		if self._spec:
			return self._specClearFile()
		file = None
		try:
			if self.header:
				file = self.get_file_obj('wb')
				header = self.delimiter.join(_sanitize(self.header,delimiter=self.delimiter))
				file.write(header.encode(self.encoding,errors='replace') + b'\n')
				self.release_file_obj(file)
				if self.verbose:
					self.__teePrintOrNot(f"Header {header} written to {self._fileName}")
					self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
			else:
				file = self.get_file_obj('wb')
				self.release_file_obj(file)
				if self.verbose:
					self.__teePrintOrNot(f"File {self._fileName} cleared empty")
					self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
			self.dirty = False
			self.deSynced = False
		except Exception as e:
			self.release_file_obj(file)
			self.__teePrintOrNot(f"Failed to write at clear_file() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
			self.deSynced = True
		return self
	
	def __enter__(self):
		return self
	
	def close(self):
		self.stopAppendThread()
		return self

	def __exit__(self,exc_type,exc_value,traceback):
		return self.close()
	
	def __repr__(self):
		return f"""TSVZed(
file_name:{self._fileName}
teeLogger:{self.teeLogger}
header:{self.header}
correctColumnNum:{self.correctColumnNum}
createIfNotExist:{self.createIfNotExist}
verifyHeader:{self.verifyHeader}
rewrite_on_load:{self.rewrite_on_load}
rewrite_on_exit:{self.rewrite_on_exit}
rewrite_interval:{self.rewrite_interval}
append_check_delay:{self.append_check_delay}
appendQueueLength:{len(self.appendQueue)}
appendThreadAlive:{self.appendThread.is_alive()}
dirty:{self.dirty}
deSynced:{self.deSynced}
memoryOnly:{self.memoryOnly}
{dict(self)})"""
	
	def __str__(self):
		return f"TSVZed({self._fileName},{dict(self)})"

	def __del__(self):
		return self.close()

	def popitem(self, last=True):
		key, value = super().popitem(last)
		if not self.memoryOnly:
			self.__appendEmptyLine(key)
		self.lastUpdateTime = get_time_ns()
		return key, value
	
	__marker = object()

	def pop(self, key, default=__marker):
		'''od.pop(k[,d]) -> v, remove specified key and return the corresponding
		value.  If key is not found, d is returned if given, otherwise KeyError
		is raised.

		'''
		key = str(key).rstrip()
		if key not in self:
			if default is self.__marker:
				raise KeyError(key)
			return default
		value = super().pop(key)
		if not self.memoryOnly:
			self.__appendEmptyLine(key)
		self.lastUpdateTime = get_time_ns()
		return value
	
	def move_to_end(self, key, last=True):
		'''Move an existing element to the end (or beginning if last is false).
		Raise KeyError if the element does not exist.
		'''
		key = str(key).rstrip()
		super().move_to_end(key, last)
		if self._spec:
			_warnOnce(self, 'move_to_end', self._fileName,
					  'move_to_end only reorders memory for .tsvz; the file keeps first-appearance order',
					  self.teeLogger)
			return self
		self.dirty = True
		if not self.rewrite_on_exit:
			self.rewrite_on_exit = True
			self.__teePrintOrNot("Warning: move_to_end had been called. Need to resync for changes to apply to disk.")
			self.__teePrintOrNot("rewrite_on_exit set to True")
		if self.verbose:
			self.__teePrintOrNot(f"Warning: Trying to move Key {key} moved to {'end' if last else 'beginning'} Need to resync for changes to apply to disk")
		self.lastUpdateTime = get_time_ns()
		return self

	def __sizeof__(self):
		sizeof = sys.getsizeof
		size = sizeof(super()) + sizeof(True) * 12  # for the booleans / integers
		size += sizeof(self.externalFileUpdateTime)
		size += sizeof(self.lastUpdateTime)
		size += sizeof(self._fileName)
		size += sizeof(self.teeLogger)
		size += sizeof(self.delimiter)
		size += sizeof(self.defaults)
		size += sizeof(self.header)
		size += sizeof(self.appendQueue)
		size += sizeof(self.encoding)
		size += sizeof(self.writeLock)
		size += sizeof(self.shutdownEvent)
		size += sizeof(self.appendThread)
		size += super().__sizeof__()
		return size

	@classmethod
	def fromkeys(cls, iterable, value=None,fileName = None,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = ...,rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,verbose = False):
		'''Create a new ordered dictionary with keys from iterable and values set to value.
		'''
		self = cls(fileName,teeLogger,header,createIfNotExist,verifyHeader,rewrite_on_load,rewrite_on_exit,rewrite_interval,append_check_delay,verbose)
		for key in iterable:
			self[key] = value
		return self


	def rewrite(self,force = False,reloadInternalFromFile = None):
		if self._spec:
			_warnOnce(self, 'rewrite()', self._fileName,
					  'rewrite() is ignored for .tsvz; behaviour undefined; use scrubTabularFile for archival compaction',
					  self.teeLogger)
			self._specSync(reloadInternalFromFile)
			return False
		if not self.deSynced and not force:
			if not self.dirty:
				return False
			if self.rewrite_interval == 0 or time.time() - os.path.getmtime(self._fileName) < self.rewrite_interval:
				return False
		try:

			if reloadInternalFromFile is None:
				reloadInternalFromFile = self.monitor_external_changes
			if reloadInternalFromFile and self.externalFileUpdateTime < getFileUpdateTimeNs(self._fileName):
				# this will be needed if more than 1 process is accessing the file
				self.commitAppendToFile()
				self.reload()
			if self.memoryOnly:
				if self.verbose:
					self.__teePrintOrNot("Memory only mode. Map to file skipped.")
				return False
			if self.dirty:
				if self.verbose:
					self.__teePrintOrNot(f"Rewriting {self._fileName}")
				self.mapToFile()
				if self.verbose:
					self.__teePrintOrNot(f"{len(self)} records rewrote to {self._fileName}")
			if not self.appendThread.is_alive():
				self.commitAppendToFile()
			# else:
			#     self.appendEvent.set()
			return True
		except Exception as e:
			self.__teePrintOrNot(f"Failed to write at sync() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
			self.deSynced = True
			return False
		
	def hardMapToFile(self):
		if self._spec:
			_warnOnce(self, 'hardMapToFile()', self._fileName,
					  'hardMapToFile() is ignored for .tsvz; behaviour undefined; use scrubTabularFile for archival compaction',
					  self.teeLogger)
			return self
		file = None
		try:
			if (not self.monitor_external_changes) and self.externalFileUpdateTime < getFileUpdateTimeNs(self._fileName):
				self.__teePrintOrNot(f"Warning: Overwriting external changes in {self._fileName}",'warning')
			file = self.get_file_obj('wb')
			buf = io.BufferedWriter(file, buffer_size=64*1024*1024)  # 64MB buffer
			if self.header:
				header = self.delimiter.join(_sanitize(self.header,delimiter=self.delimiter))
				buf.write(header.encode(self.encoding,errors='replace') + b'\n')
			# Persist the defaults line (right after the header) so it survives a
			# full rewrite -- it is not a regular mapping key.
			if self.defaults and len(self.defaults) > 1:
				defaultsLine = self.delimiter.join(_sanitize(self.defaults,delimiter=self.delimiter))
				buf.write(defaultsLine.encode(self.encoding,errors='replace') + b'\n')
			for key in self:
				# '#'-prefixed keys are in-memory only (comments / internal); never written.
				if str(key).startswith('#'):
					continue
				segments = _sanitize(self[key],delimiter=self.delimiter)
				buf.write(self.delimiter.join(segments).encode(encoding=self.encoding,errors='replace')+b'\n')
			buf.flush()
			self.release_file_obj(file)
			if self.verbose:
				self.__teePrintOrNot(f"{len(self)} records written to {self._fileName}")
				self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
			self.dirty = False
			self.deSynced = False
		except Exception as e:
			self.release_file_obj(file)
			self.__teePrintOrNot(f"Failed to write at hardMapToFile() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
			self.deSynced = True
		return self
	
	def mapToFile(self):
		if self._spec:
			_warnOnce(self, 'mapToFile()', self._fileName,
					  'mapToFile() is ignored for .tsvz; behaviour undefined; use scrubTabularFile for archival compaction',
					  self.teeLogger)
			return self
		mec = self.monitor_external_changes
		self.monitor_external_changes = False
		file = None
		try:
			if (not self.monitor_external_changes) and self.externalFileUpdateTime < getFileUpdateTimeNs(self._fileName):
				self.__teePrintOrNot(f"Warning: Overwriting external changes in {self._fileName}",'warning')
			if self._fileName.rpartition('.')[2] in COMPRESSED_FILE_EXTENSIONS:
				# if the file is compressed, we need to use the hardMapToFile method
				return self.hardMapToFile()
			file = self.get_file_obj('r+b')
			overWrite = False
			if self.header:
				line = file.readline().decode(self.encoding,errors='replace')
				aftPos = file.tell()
				if not _lineContainHeader(self.header,line,verbose = self.verbose,teeLogger = self.teeLogger,strict = self.strict,delimiter = self.delimiter):
					header = self.delimiter.join(_sanitize(self.header,delimiter=self.delimiter))
					file.seek(0)
					file.write(f'{header}\n'.encode(encoding=self.encoding,errors='replace'))
					# if the header is not the same length as the line, we need to overwrite the file
					if aftPos != file.tell():
						overWrite = True
					if self.verbose:
						self.__teePrintOrNot(f"Header {header} written to {self._fileName}")
			# Rows to (re)write, in order: the defaults line first (right after the
			# header, if set) then every data row. '#'-prefixed keys are in-memory
			# only (comments / internal) and are never written.
			rowsToWrite = []
			if self.defaults and len(self.defaults) > 1:
				rowsToWrite.append(self.defaults)
			rowsToWrite.extend(v for v in self.values() if v and not str(v[0]).startswith('#'))
			for value in rowsToWrite:
				segments = _sanitize(value,delimiter=self.delimiter)
				strToWrite = self.delimiter.join(segments)
				if overWrite:
					if self.verbose:
						self.__teePrintOrNot(f"Overwriting {value} to {self._fileName}")
					file.write(strToWrite.encode(encoding=self.encoding,errors='replace')+b'\n')
					continue
				pos = file.tell()
				line = file.readline()
				aftPos = file.tell()
				if not line or pos == aftPos:
					if self.verbose:
						self.__teePrintOrNot(f"End of file reached. Appending {value} to {self._fileName}")
					# L6: 3.39 omitted this newline and glued the row to the next one.
					file.write(strToWrite.encode(encoding=self.encoding,errors='replace')+b'\n')
					overWrite = True
					continue
				strToWrite = strToWrite.encode(encoding=self.encoding,errors='replace').ljust(len(line)-1)+b'\n'
				if line != strToWrite:
					if self.verbose:
						self.__teePrintOrNot(f"Modifing {value} to {self._fileName}")
					file.seek(pos)
					# fill the string with space to write to the correct length
					file.write(strToWrite)
					if aftPos != file.tell():
						overWrite = True
			file.truncate()
			self.release_file_obj(file)
			if self.verbose:
				self.__teePrintOrNot(f"{len(self)} records written to {self._fileName}")
				self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
			self.dirty = False
			self.deSynced = False
		except Exception as e:
			self.release_file_obj(file)
			self.__teePrintOrNot(f"Failed to write at mapToFile() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
			self.deSynced = True
			self.__teePrintOrNot("Trying failback hardMapToFile()")
			self.hardMapToFile()
		self.externalFileUpdateTime = getFileUpdateTimeNs(self._fileName)
		self.monitor_external_changes = mec
		return self
	
	def checkExternalChanges(self):
		if self.deSynced:
			return self
		if not self.monitor_external_changes:
			return self
		if self._spec:
			if self._specLoaded and self._specExternalChanged():
				self.deSynced = True
				self.__teePrintOrNot(f"External changes detected in {self._fileName}")
			return self
		realExternalFileUpdateTime = getFileUpdateTimeNs(self._fileName)
		if self.externalFileUpdateTime < realExternalFileUpdateTime:
			self.deSynced = True
			self.__teePrintOrNot(f"External changes detected in {self._fileName}")
		elif self.externalFileUpdateTime > realExternalFileUpdateTime:
			self.__teePrintOrNot(f"Time anomalies detected in {self._fileName}, resetting externalFileUpdateTime")
			self.externalFileUpdateTime = realExternalFileUpdateTime
		return self

	def _appendWorker(self):
		while not self.shutdownEvent.is_set():
			if not self.memoryOnly:
				self.checkExternalChanges()
				if self._spec:
					self._specSync()
				else:
					self.rewrite()
				self.commitAppendToFile()
			time.sleep(self.append_check_delay)
			# self.appendEvent.wait()
			# self.appendEvent.clear()
		if self.verbose:
			self.__teePrintOrNot(f"Append worker for {self._fileName} shut down")
		self.commitAppendToFile()

	def commitAppendToFile(self):
		if self._spec:
			return self._specCommit()
		if self.appendQueue:
			if self.memoryOnly:
				self.appendQueue.clear()
				if self.verbose:
					self.__teePrintOrNot("Memory only mode. Append queue cleared.") 
				return self
			file = None
			try:
				if self.verbose:
					self.__teePrintOrNot(f"Commiting {len(self.appendQueue)} records to {self._fileName}")
					self.__teePrintOrNot(f"Before size of {self._fileName}: {os.path.getsize(self._fileName)}")
				compressed = _isCompressedFile(self._fileName)
				file = self.get_file_obj('ab' if compressed else 'a+b')
				chunks = []
				while self.appendQueue:
					line = _sanitize(self.appendQueue.popleft(),delimiter=self.delimiter)
					chunks.append(self.delimiter.join(line).encode(encoding=self.encoding,errors='replace')+b'\n')
				payload = b''.join(chunks)
				if not compressed and _endsWithoutNewline(file):
					# L1: terminate a hand-edited last line instead of gluing onto it.
					payload = b'\n' + payload
					_warn(f'TSVZ warning: {self._fileName}: last line had no trailing newline; added one before appending', self.teeLogger)
				file.write(payload)
				self.release_file_obj(file)
				if self.verbose:
					self.__teePrintOrNot(f"Records commited to {self._fileName}")
					self.__teePrintOrNot(f"After size of {self._fileName}: {os.path.getsize(self._fileName)}")
			except Exception as e:
				self.release_file_obj(file)
				self.__teePrintOrNot(f"Failed to write at commitAppendToFile to {self._fileName}: {e}",'error')
				import traceback
				self.__teePrintOrNot(traceback.format_exc(),'error')
				self.deSynced = True
		return self
	
	def stopAppendThread(self):
		try:
			if self.shutdownEvent.is_set():
				# if self.verbose:
				#     self.__teePrintOrNot(f"Append thread for {self._fileName} already stopped")
				return
			if self._spec:
				self._specSync()
			else:
				self.rewrite(force=self.rewrite_on_exit)  # Ensure any final sync operations are performed
			# self.appendEvent.set()
			self.shutdownEvent.set()  # Signal the append thread to shut down
			self.appendThread.join()  # Wait for the append thread to complete 
			if self.verbose:
				self.__teePrintOrNot(f"Append thread for {self._fileName} stopped")
		except Exception as e:
			self.__teePrintOrNot(f"Failed to stop append thread for {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
	
	def get_file_obj(self,modes = 'ab'):
		self.writeLock.acquire()
		file = None
		try:
			if not self.encoding:
				self.encoding = 'utf8'
			file = openFileAsCompressed(self._fileName, mode=modes, encoding=self.encoding,teeLogger=self.teeLogger)
			# Lock the file after opening
			if os.name == 'posix':
				fcntl.lockf(file, fcntl.LOCK_EX)
			elif os.name == 'nt':
				# For Windows, locking the entire file, avoiding locking an empty file
				#lock_length = max(1, os.path.getsize(self._fileName))
				lock_length = 2147483647
				msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, lock_length)
			if self.verbose:
				self.__teePrintOrNot(f"File {self._fileName} locked with mode {modes}")
		except Exception as e:
			try:
				self.writeLock.release()  # Release the thread lock in case of an error
			except Exception as e:
				self.__teePrintOrNot(f"Failed to release writeLock for {self._fileName}: {e}",'error')
			self.__teePrintOrNot(f"Failed to open file {self._fileName}: {e}",'error')
		return file

	def release_file_obj(self,file):
		# if write lock is already released, return
		if not self.writeLock.locked():
			return
		if file is None:
			return
		try:
			file.flush()  # Ensure the file is flushed before unlocking
			os.fsync(file.fileno())  # Ensure the file is synced to disk before unlocking
			if not file.closed:
				if os.name == 'posix':
					fcntl.lockf(file, fcntl.LOCK_UN)
				elif os.name == 'nt':
					# Unlocking the entire file; for Windows, ensure not unlocking an empty file
					#unlock_length = max(1, os.path.getsize(os.path.realpath(file.name)))
					unlock_length = 2147483647
					try:
						msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, unlock_length)
					except Exception:
						pass
				file.close()  # Ensure file is closed after unlocking
			if self.verbose:
				self.__teePrintOrNot(f"File {file.name} unlocked / released")
		except Exception as e:
			try:
				self.writeLock.release()  # Ensure the thread lock is always released
			except Exception as e:
				self.__teePrintOrNot(f"Failed to release writeLock for {file.name}: {e}",'error')
			self.__teePrintOrNot(f"Failed to release file {file.name}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
		# release the write lock if not already released
		if self.writeLock.locked():
			try:
				self.writeLock.release()  # Ensure the thread lock is always released
			except Exception as e:
				self.__teePrintOrNot(f"Failed to release writeLock for {file.name}: {e}",'error')
			self.externalFileUpdateTime = getFileUpdateTimeNs(self._fileName)

class TSVZedLite(MutableMapping):
	"""
	A mutable mapping class that provides a dictionary-like interface to a Tabular (TSV by default) file.
	TSVZedLite stores key-value pairs where each row in the file represents an entry, with the first
	column serving as the key. The class maintains an in-memory index of file positions for efficient
	random access while keeping the actual data on disk.
	TSVZedLite is designed for light memory footprint and forgoes some features from TSVZed, Notably,
	- Does not support simultaneous multi-process access.
	- Does not support compressed file formats.
	- Does not support automatic file rewriting on load / exit / periodically.
	- Does not support append worker thread for background writes.
	- Does not support external file change monitoring.
	- Does not support in-place updates; updates are append-only.
	- Does not support logging via teeLogger.
	- Does not support move_to_end method.
	- Does not support in-memory only mode. ( please just use a dict )
	- Does not lock the file during operations (a .tsvz write takes the exclusive lock other writers take).
	- Does not track last update times.
	
	However, it may be preferred in scenarios when:
	- Memory usage needs to be minimized.
	- Working with extremely large datasets where loading everything into memory is impractical.
	- Simplicity and ease of use are prioritized over advanced features.
	- The dataset is primarily write-only with infrequent reads.
	- The application can tolerate the lack of concurrency control. (single process access only)
	- Underlying file system is fast and can do constant time random seek (e.g., SSD).

	Note: It is possible to load a custom dict like object for indexes (like TSVZed or pre-built dict)
	to avoid reading the entire data file to load the indexes at startup. 
	Index consistency is not enforced in this case.
	Will raise error if mismatch happen (only checkes key exist in file) and strict mode is enabled.
	If using an external file-backed Index. This can function similar to a key-value store (like nosql).

	Parameters
	----------
	fileName : str
		Path to the Tabular file to read from or create.
	header : str, optional
		Header row for the file. Can be a delimited string or empty string (default: '').
	createIfNotExist : bool, optional
		If True, creates the file if it doesn't exist (default: True).
	verifyHeader : bool, optional
		If True, verifies that the file header matches the provided header (default: True).
	verbose : bool, optional
		If True, prints detailed operation information to stderr (default: False).
	encoding : str, optional
		Character encoding for the file (default: 'utf8').
	delimiter : str, optional
		Field delimiter character. If Ellipsis (...), automatically detects from filename (default: ...).
	defaults : str, list, or None, optional
		Default values for columns. Can be a delimited string, list, or None (default: None).
	strict : bool, optional
		If True, enforces strict column count validation and raises errors on mismatches (default: True).
	correctColumnNum : int, optional
		Expected number of columns. -1 means auto-detect (default: -1).
	indexes : dict, optional
		Pre-existing index dictionary mapping keys to file positions (default: ...).
	fileObj : file object, optional
		Pre-existing file object to use (default: ...).
	Attributes
	----------
	version : str
		Version identifier for the TSVZedLite format.
	indexes : dict
		Dictionary mapping keys to their file positions (or in-memory data for keys starting with '#').
	fileObj : file object
		Binary file object for reading/writing the underlying file.
	defaults : list
		List of default values for columns, with DEFAULTS_INDICATOR_KEY as the first element.
	correctColumnNum : int
		The validated number of columns per row.
	Notes
	-----
	- Keys starting with '#' are stored in memory only and not written to file.
	- The special key DEFAULTS_INDICATOR_KEY is used to store and retrieve default column values.
	- Empty values in rows are automatically filled with defaults if available.
	- The class implements the MutableMapping interface, providing dict-like operations.
	- File operations are buffered and written immediately (append-only for updates).
	- Deleted entries are marked by writing a row with only the key (empty values).
	Examples
	--------
	>>> db = TSVZedLite('data.tsv', header='id\tname\tage')
	>>> db['user1'] = ['user1', 'Alice', '30']
	>>> print(db['user1'])
	['user1', 'Alice', '30']
	>>> del db['user1']
	>>> 'user1' in db
	False
	See Also
	--------
	collections.abc.MutableMapping : The abstract base class that this class implements.
	"""

	#['__new__', '__repr__', '__hash__', '__lt__', '__le__', '__eq__', '__ne__', '__gt__', '__ge__', '__iter__', '__init__',
	#  '__or__', '__ror__', '__ior__', '__len__', '__getitem__', '__setitem__', '__delitem__', '__contains__', '__sizeof__',
	#  'get', 'setdefault', 'pop', 'popitem', 'keys', 'items', 'values', 'update', 'fromkeys', 'clear', 'copy', '__reversed__',
	#  '__class_getitem__', '__doc__']
	def __init__ (self,fileName,header = '',createIfNotExist = True,verifyHeader = True,
					verbose = False,encoding = 'utf8',
					delimiter = ...,defaults = None,strict = True,correctColumnNum = -1,
					indexes = ..., fileObj = ...
					):
		self._constructed = False  # setDefaults() writes a #_defaults_# marker only once this is True
		self._spec = _isSpecPath(fileName)
		self.version = version
		self.strict = strict
		self._fileName = fileName
		if self._spec:
			reporter = _Reporter(fileName)
			self.delimiter, encoding = _specOptions(fileName, delimiter, encoding, reporter)
			reporter.flush()
		else:
			self.delimiter = get_delimiter(delimiter,file_name=fileName)
		self.setDefaults(defaults)
		self._constructorDefaults = list(self.defaults)
		self.header = _formatHeader(header,verbose = verbose,delimiter=self.delimiter)
		self.correctColumnNum = correctColumnNum
		self.createIfNotExist = createIfNotExist
		self.verifyHeader = verifyHeader
		self.verbose = verbose
		self.encoding = encoding
		if self._spec:
			self._specInit(indexes, fileObj)
		else:
			# L3: offsets cannot address a compressed file; keep its rows in memory.
			self._inMemoryRows = _isCompressedFile(fileName)
			if self._inMemoryRows:
				_warnOnce(self, 'compressed', fileName, 'compressed file: TSVZedLite keeps rows in memory and appends new compressed members')
			if indexes is ...:
				self.indexes = dict()
				self.load()
			else:
				self.indexes = indexes
			if fileObj is ...:
				# 'r+b' requires the file to exist. When indexes were supplied
				# externally, load() (which would have created it) is skipped, so
				# ensure the file exists first -- creating it when allowed, or
				# raising a clear FileNotFoundError instead of a cryptic open error.
				_verifyFileExistence(self._fileName, createIfNotExist = self.createIfNotExist,
									  header = self.header, encoding = self.encoding,
									  strict = self.strict, delimiter = self.delimiter)
				self.fileObj = None if self._inMemoryRows else open(self._fileName,'r+b')
			else:
				self.fileObj = fileObj
		atexit.register(self.close)
		self._constructed = True

	# Implement custom methods just for TSVZedLite
	def getResourceUsage(self,return_dict = False):
		return get_resource_usage(return_dict = return_dict)

	@property
	def dialect(self):
		"""'tsvz' for tsvz-spec-v1 files (.tsvz/.csvz/.nsvz/.psvz), 'tsv' for 3.39-format files."""
		return 'tsvz' if self._spec else 'tsv'

	def __contains__(self, key):
		if self._spec:
			return str(key).rstrip() in self.indexes
		return MutableMapping.__contains__(self, key)

	def get(self, key, default=None):
		if self._spec:
			return self[key] if key in self else default
		return MutableMapping.get(self, key, default)

	def setdefault(self, key, default=None):
		if self._spec:
			if key not in self:
				self[key] = default
			return self[key]
		return MutableMapping.setdefault(self, key, default)

	def _specInit(self, indexes, fileObj):
		self._inMemoryRows = False
		self._specState = _SpecState(self.defaults)
		self._transOffsets = []
		self._transStates = []
		self._activePath = self._fileName
		self._pendingDefaults = False
		self._pendingRepair = False
		if indexes is ...:
			self.indexes = dict()
			self.load()
		else:
			self.indexes = indexes
			# One scan for the marker state, the transitions and the active part.
			self._specLiteLoad({})
		if fileObj is ...:
			self.fileObj = None if self._inMemoryRows else open(self._activePath, 'r+b')
		else:
			self.fileObj = fileObj
		self._specFlushPendingDefaults()

	def _specLiteLoad(self, target):
		"""Load key -> offset (active, uncompressed part) or key -> row (elsewhere) into ``target``."""
		reporter = _Reporter(self._fileName)
		try:
			if not _specEnsureStore(self._fileName, self.createIfNotExist, self.header, self._constructorDefaults,
									self.strict, None, self.delimiter):
				return self
			load = _specLoad(self._fileName, self.delimiter, header=self.header, verifyHeader=self.verifyHeader,
							 strict=self.strict, defaults=self._constructorDefaults,
							 correctColumnNum=self.correctColumnNum, storeOffset=True, taskDic=target,
							 reporter=reporter, verbose=self.verbose)
		finally:
			reporter.flush()
		self._specState = load.state
		self.defaults = list(load.state.defaults)
		self._activePath = load.active
		self._inMemoryRows = bool(_parsePartName(load.active).codec)
		info = load.infos[-1] if load.infos else None
		self._pendingRepair = bool(info is not None and info.path == load.active and info.codec
								   and (info.damaged or info.tail))
		self._transOffsets = [offset for offset, _ in load.transitions]
		self._transStates = [rowState for _, rowState in load.transitions]
		if self.correctColumnNum == -1:
			self.correctColumnNum = load.correctColumnNum
		held = sum(1 for value in target.values() if isinstance(value, list))
		if held:
			_warnOnce(self, 'held', self._fileName, f'{held} keys from older or compressed parts are held in memory (offsets cannot address them)')
		if not load.state.sawDefaults and any(self._constructorDefaults[1:]):
			self._pendingDefaults = True
		return self

	def _specFlushPendingDefaults(self):
		"""Persist constructor defaults the store never declared (design §5.3)."""
		if self._pendingDefaults:
			self._pendingDefaults = False
			self._specWriteMarker(DEFAULTS_INDICATOR_KEY, list(self._constructorDefaults))

	def _specReadAt(self, pos, key=...):
		"""Read the row at ``pos`` under the marker state in force there (found by bisection)."""
		self.fileObj.seek(pos)
		raw = self.fileObj.readline()
		index = bisect.bisect_right(self._transOffsets, pos) - 1
		rowState = self._transStates[index] if index >= 0 else self._specState.rowState
		state = _SpecState.fromRowState(rowState)
		reporter = _Reporter(self._fileName)
		record = None
		if raw.endswith(b'\n'):
			record = _specProcessRecord(_decodeLine(raw, reporter, f'offset {pos}'), state, self.delimiter, reporter, None)
		reporter.flush()
		segments = []
		if record and record[1] is not None:
			segments = record[1]
			target = max(self.correctColumnNum, len(state.defaults))
			if len(segments) < target:
				segments.extend(state.defaults[j] if j < len(state.defaults) else '' for j in range(len(segments), target))
		if self.verbose:
			eprint(f"Read at position {pos}: {segments}")
		if key is not ...:
			if not segments:
				eprint(f"Error: No segments found at position {pos}")
			elif segments[0] != key:
				eprint(f"Warning: Key mismatch at position {pos}: expected {key}, got {segments[0]}")
			else:
				return segments
			if self.strict:
				eprint("Error: Key mismatch and strict mode enabled. Raising KeyError.")
				raise KeyError(key)
			else:
				eprint("Continuing despite key mismatch due to non-strict mode. Expect errors!")
		return segments

	def _specSetItem(self, key, value):
		"""__setitem__ for .tsvz: same normalisation as TSVZed, written immediately."""
		key = str(key).rstrip()
		if not key:
			eprint('Error: Key cannot be empty')
			return
		if isinstance(value,str):
			value = value.split(self.delimiter)
		value = [str(s).rstrip() if s else '' for s in value]
		if not value or value[0] != key:
			value = [key]+value
		if _MARKER_RE.match(key):
			self._specWriteMarker(key, value)
			return
		if len(value) == 1:
			del self[key]
			return
		if self.strict and self.correctColumnNum > 0 and len(value) != self.correctColumnNum:
			eprint(f"Error: Value {value} does not have the correct number of columns: {self.correctColumnNum}. Refuse adding key...")
			return
		defaults = self.defaults
		for i in range(1, min(len(value), len(defaults))):
			if not value[i] and defaults[i]:
				value[i] = defaults[i]
		if self.correctColumnNum <= 0:
			# Mirror _specLoad's width rule so memory and reads equal a reload.
			self.correctColumnNum = len(defaults) if len(defaults) > 1 else len(value)
		offset = self._specWrite(_specFormatRecord(value, self.delimiter))
		if offset is None:
			target = max(self.correctColumnNum, len(defaults))
			value += [defaults[j] if j < len(defaults) else '' for j in range(len(value), target)]
			self.indexes[key] = value
		else:
			self.indexes[key] = offset

	def _specWriteMarker(self, key, value):
		keyLower = key.lower()
		if keyLower in _SPEC_MARKER_KEYS:
			reporter = _Reporter(self._fileName)
			applied = _specApplyMarker(self._specState, keyLower, value[1:], reporter, None)
			reporter.flush()
			if not applied:
				return
			self.defaults = list(self._specState.defaults)
		self._specWrite(_specFormatRecord(value, self.delimiter, marker=True))

	def _specWrite(self, line):
		"""Append one record to the active part; return its offset, or None for a compressed part."""
		data = (line + '\n').encode('utf-8', errors='replace')
		if self.fileObj is None:
			reporter = _Reporter(self._activePath)
			_specAppendPayload(self._activePath, data, reporter, repair=self._pendingRepair)
			self._pendingRepair = False
			reporter.flush()
			return None
		f = self.fileObj
		with _pathLock(self._activePath):
			# The 3.39 exclusive lockf, as every other writer takes: under it an
			# unterminated tail really is uncommitted, and the end cannot move.
			_seekRawStart(f)  # msvcrt locks from the raw position: lock and unlock the same range
			_lockFile(f)
			try:
				size = f.seek(0, os.SEEK_END)
				if size:
					f.seek(size - 1)
					if f.read(1) != b'\n':
						keep = _lastNewlineEnd(f, size)
						f.seek(keep)
						tail = f.read(size - keep)
						f.truncate(keep)
						size = keep
						_warn(f'TSVZ warning: {self._activePath}: removed uncommitted tail {tail[:80]!r} before appending')
				f.seek(size)
				f.write(data)
				f.flush()
			finally:
				try:
					_seekRawStart(f)
				except Exception:
					pass  # a failed write is already propagating; the lock must still be released
				_unlockFile(f)
		if not self._transStates or self._specState.rowState is not self._transStates[-1]:
			self._transOffsets.append(size)
			self._transStates.append(self._specState.rowState)
		return size

	def setDefaults(self,defaults):
		if not defaults:
			defaults = []
		if isinstance(defaults,str):
			defaults = defaults.split(self.delimiter)
		elif not isinstance(defaults,list):
			try:
				defaults = list(defaults)
			except Exception:
				if self.verbose:
					eprint('Error: Invalid defaults, setting defaults to empty.')
				defaults = []
		defaults = [str(s).rstrip() if s else '' for s in defaults]
		if not any(defaults):
			defaults = []
		if not defaults or defaults[0] != DEFAULTS_INDICATOR_KEY:
			defaults = [DEFAULTS_INDICATOR_KEY]+defaults
		if self._spec and self._constructed:
			# Spec rows are written sparse and read back under the file's
			# #_defaults_#, so new defaults are written as that marker.
			self._specWriteMarker(DEFAULTS_INDICATOR_KEY, defaults)
			return
		self.defaults = defaults

	def load(self):
		if self._spec:
			if self.verbose:
				eprint(f"Loading {self._fileName}")
			return self._specLiteLoad(self.indexes)
		if self.verbose:
			eprint(f"Loading {self._fileName}")
		readTabularFile(self._fileName, header = self.header, createIfNotExist = self.createIfNotExist,
				   verifyHeader = self.verifyHeader, verbose = self.verbose, taskDic = self.indexes,
				   encoding = self.encoding if self.encoding else None, strict = self.strict, 
				   delimiter = self.delimiter, defaults=self.defaults,storeOffset=not self._inMemoryRows)
		return self

	def positions(self):
		return self.indexes.values()

	def reload(self):
		self.indexes.clear()
		return self.load()

	def getListView(self):
		return getListView(self,header=self.header,delimiter=self.delimiter)

	def clear_file(self):
		if self._spec:
			_specClearTabularFile(self._fileName, header=self.header, verbose=self.verbose, delimiter=self.delimiter)
			if self.fileObj is not None:
				self.fileObj.close()
			self.indexes.clear()
			self._specLiteLoad(self.indexes)
			self.fileObj = None if self._inMemoryRows else open(self._activePath, 'r+b')
			self._specFlushPendingDefaults()
			return self
		if self.fileObj is None:
			clearTabularFile(self._fileName, header=self.header, verbose=self.verbose,
							 encoding=self.encoding, delimiter=self.delimiter)
			return self
		if self.verbose:
			eprint(f"Clearing {self._fileName}")
		self.fileObj.seek(0)
		self.fileObj.truncate()
		if self.verbose:
			eprint(f"File {self._fileName} cleared empty")
		if self.header:
			location = self.__writeValues(self.header)
			if self.verbose:
				eprint(f"Header {self.header} written to {self._fileName}")
				eprint(f"At {location} size: {self.fileObj.tell()}")
		return self
	
	def switchFile(self,newFileName,createIfNotExist = ...,verifyHeader = ...):
		if createIfNotExist is ...:
			createIfNotExist = self.createIfNotExist
		if verifyHeader is ...:
			verifyHeader = self.verifyHeader
		if self.fileObj is not None:
			self.fileObj.close()
		self._fileName = newFileName
		self._spec = _isSpecPath(newFileName)
		if self._spec:
			self.delimiter = _EXTENSION_DELIMITERS[_parsePartName(newFileName).ext]
			self._specState = _SpecState(self._constructorDefaults)
			self._transOffsets, self._transStates = [], []
			self._activePath = newFileName
			self._pendingDefaults = False
			self._pendingRepair = False
			self.reload()
			self.fileObj = None if self._inMemoryRows else open(self._activePath, 'r+b')
			self._specFlushPendingDefaults()
		else:
			self._inMemoryRows = _isCompressedFile(newFileName)
			self.reload()
			self.fileObj = None if self._inMemoryRows else open(self._fileName,'r+b')
		self.createIfNotExist = createIfNotExist
		self.verifyHeader = verifyHeader
		return self

	# Private methods for reading and writing values for TSVZedLite

	def __writeValues(self,data):
		if self.fileObj is None:
			line = self.delimiter.join(_sanitize(data,delimiter=self.delimiter))
			with openFileAsCompressed(self._fileName, mode='ab', encoding=self.encoding) as f:
				f.write(line.encode(encoding=self.encoding,errors='replace') + b'\n')
			return list(data)
		write_at = self.fileObj.seek(0, os.SEEK_END)
		if write_at:
			self.fileObj.seek(write_at - 1)
			if self.fileObj.read(1) != b'\n':
				# L1: terminate a hand-edited last line instead of gluing onto it.
				self.fileObj.seek(write_at)
				self.fileObj.write(b'\n')
				write_at += 1
				_warn(f'TSVZ warning: {self._fileName}: last line had no trailing newline; added one before appending')
			self.fileObj.seek(write_at)
		if self.verbose:
			eprint(f"Writing at position {write_at}")
		data = _sanitize(data,delimiter=self.delimiter)
		data = self.delimiter.join(data)
		bytes = self.fileObj.write((data.encode(encoding=self.encoding,errors='replace') + b'\n'))
		if self.verbose:
			eprint(f"Wrote {bytes} bytes")
		return write_at

	def __mapDeleteToFile(self,key):
		if self._spec:
			self._specWrite(_specFormatRecord([key], self.delimiter))
			return
		# Persist a deletion by appending a tombstone (a lone key with no
		# values); on read such a row deletes the key (see _processLine). The
		# key must already have been removed from self.indexes by the caller --
		# this method does NOT touch the index (previously it re-added the key,
		# pointing it at the tombstone line).
		if key == DEFAULTS_INDICATOR_KEY:
			self.defaults = [DEFAULTS_INDICATOR_KEY]
			if self.verbose:
				eprint("Defaults cleared")
			self.__writeValues([key])
			return
		if key.startswith('#'):
			# comment / internal keys are in-memory only; nothing to persist
			if self.verbose:
				eprint(f"Key {key} deleted in memory")
			return
		if self.verbose:
			eprint(f"Writing tombstone line for {key}")
		self.__writeValues([key])

	def __readValuesAtPos(self,pos,key = ...):
		if self._spec:
			return self._specReadAt(pos, key)
		self.fileObj.seek(pos)
		line = self.fileObj.readline().decode(self.encoding,errors='replace')
		self.correctColumnNum, segments = _processLine(
						line=line,
						taskDic={},
						correctColumnNum=self.correctColumnNum,
						strict=self.strict,
						delimiter=self.delimiter,
						defaults=self.defaults,
						storeOffset=True,
					)
		if self.verbose:
			eprint(f"Read at position {pos}: {segments}")
		if key is not ...:
			if not segments:
				eprint(f"Error: No segments found at position {pos}")
			elif segments[0] != key:
				eprint(f"Warning: Key mismatch at position {pos}: expected {key}, got {segments[0]}")
			else:
				return segments
			if self.strict:
				eprint("Error: Key mismatch and strict mode enabled. Raising KeyError.")
				raise KeyError(key)
			else :
				eprint("Continuing despite key mismatch due to non-strict mode. Expect errors!")
		return segments

	# Implement basic __getitem__, __setitem__, __delitem__, __iter__, and __len__. needed for MutableMapping
	def __getitem__(self,key):
		key = str(key).rstrip()
		if key not in self.indexes:
			if key == DEFAULTS_INDICATOR_KEY:
				return self.defaults
			if self._spec and self._specState.returnDefaults:
				# Spec §14.5: a missing key reads as the active defaults.
				row = [key] + list(self.defaults[1:])
				if self.correctColumnNum > len(row):
					row += [''] * (self.correctColumnNum - len(row))
				return row
			raise KeyError(key)
		pos = self.indexes[key]
		if isinstance(pos, list):
			# L3: rows held in memory ('#' keys, compressed files, older parts).
			return pos
		return self.__readValuesAtPos(pos,key)

	def __setitem__(self,key,value):
		if self._spec:
			return self._specSetItem(key, value)
		key = str(key).rstrip()
		if not key:
			eprint('Error: Key cannot be empty')
			return
		if isinstance(value,str):
			value = value.split(self.delimiter)
		# sanitize the value
		value = [str(s).rstrip() if s else '' for s in value]
		# the first field in value should be the key
		# add it if it is not there
		if not value or value[0] != key:
			value = [key]+value
		# verify the value has the correct number of columns
		if self.correctColumnNum != 1 and len(value) == 1:
			# a lone key (no following values) means delete the key, per the
			# TSVZ spec. del is a no-op when the key is absent (tolerated).
			del self[key]
			return
		elif self.correctColumnNum > 0:
			if len(value) != self.correctColumnNum:
				if self.strict:
					eprint(f"Error: Value {value} does not have the correct number of columns: {self.correctColumnNum}. Refuse adding key...")
					return
				elif self.verbose:
					eprint(f"Warning: Value {value} does not have the correct number of columns: {self.correctColumnNum}, correcting...")
				if len(value) < self.correctColumnNum:
					value += ['']*(self.correctColumnNum-len(value))
				elif len(value) > self.correctColumnNum:
					value = value[:self.correctColumnNum]
		else:
			self.correctColumnNum = len(value)
		if self.defaults and len(self.defaults) > 1:
			for i in range(1,len(value)):
				if not value[i] and i < len(self.defaults) and self.defaults[i]:
					value[i] = self.defaults[i]
					if self.verbose:
						eprint(f"    Replacing empty value at {i} with default: {self.defaults[i]}")
		if (len(value) > 1 and not any(value[1:]) and key != DEFAULTS_INDICATOR_KEY
				and not key.startswith('#')):
			# L2: on disk this row is a delete; make memory agree with the file.
			_warnOnce(self, 'empty-row', self._fileName,
					  f'in .tsv files a row whose values are all empty is a delete; deleted {key!r} (use .tsvz to store empty rows)')
			del self[key]
			return
		if key == DEFAULTS_INDICATOR_KEY:
			self.defaults = value
			if self.verbose:
				eprint(f"Defaults set to {value}")
			return
		elif key.startswith('#'):
			if self.verbose:
				eprint(f"Key {key} updated in memory (data in index) as it starts with #")
			self.indexes[key] = value
			return
		if self.verbose:
			eprint(f"Writing {key}: {value}")
		self.indexes[key] = self.__writeValues(value)
		
	def __delitem__(self,key):
		key = str(key).rstrip()
		if self._spec:
			if _MARKER_RE.match(key):
				# A lone marker key resets that marker (spec §12.1).
				self._specWriteMarker(key, [key])
				return
			if key not in self.indexes:
				if self.verbose:
					eprint(f"Key {key} not found")
				return
			self.indexes.pop(key, None)
			self._specWrite(_specFormatRecord([key], self.delimiter))
			return
		if key == DEFAULTS_INDICATOR_KEY:
			self.__mapDeleteToFile(key)
			return
		if key not in self.indexes:
			if self.verbose:
				eprint(f"Key {key} not found")
			return
		# Remove from the index first, then persist the tombstone to the file.
		self.indexes.pop(key,None)
		self.__mapDeleteToFile(key)

	def __iter__(self):
		return iter(self.indexes)

	def __len__(self):
		return len(self.indexes)

	# Implement additional methods for dict like interface (order of function are somewhat from OrderedDict)
	def __reversed__(self):
		return reversed(self.indexes)

	def clear(self):
		# clear the dictionary and update the file
		self.indexes.clear()
		self.clear_file()
		return self

	def popitem(self, last=True,return_pos = False):
		if last:
			key, pos = self.indexes.popitem()
		else:
			try:
				key = next(iter(self.indexes))
				pos = self.indexes.pop(key)
			except StopIteration:
				raise KeyError("popitem(): dictionary is empty")
		if return_pos or isinstance(pos, list):
			value = pos
		else:
			value = self.__readValuesAtPos(pos,key)
		self.__mapDeleteToFile(key)
		return key, value

	__marker = object()
	def pop(self, key, default=__marker, return_pos = False):
		key = str(key).rstrip()
		try:
			pos = self.indexes.pop(key)
		except KeyError:
			if default is self.__marker:
				raise KeyError(key)
			elif default is ...:
				return self.defaults
			return default
		if return_pos or isinstance(pos, list):
			value = pos
		else:
			value = self.__readValuesAtPos(pos,key)
		self.__mapDeleteToFile(key)
		return value

	def __sizeof__(self):
		sizeof = sys.getsizeof
		size = sizeof(super()) + sizeof(True) * 6  # for the booleans / integers
		size += sizeof(self._fileName)
		size += sizeof(self.header)
		size += sizeof(self.encoding)
		size += sizeof(self.delimiter)
		size += sizeof(self.defaults)
		size += sizeof(self.indexes)
		size += sizeof(self.fileObj)
		return size
	
	def __repr__(self):
		return f"""TSVZed at {hex(id(self))}(
file_name:{self._fileName}
index_count:{len(self.indexes)}
header:{self.header}
correctColumnNum:{self.correctColumnNum}
createIfNotExist:{self.createIfNotExist}
verifyHeader:{self.verifyHeader}
strict:{self.strict}
delimiter:{self.delimiter}
defaults:{self.defaults}
verbose:{self.verbose}
encoding:{self.encoding}
file_descriptor:{self.fileObj.fileno() if self.fileObj is not None else None}
)"""

	def __str__(self):
		return f"TSVZedLite({self._fileName})"
	
	def __reduce__(self):
		'Return state information for pickling'
		# Return minimal state needed to reconstruct
		return (
			self.__class__,
			(self._fileName, self.header, self.createIfNotExist, self.verifyHeader,
			 self.verbose, self.encoding, self.delimiter, self.defaults, self.strict, 
			 self.correctColumnNum),
			None,
			None,
			None
		)
	def copy(self):
		'Return a shallow copy of the ordered dictionary.'
		new = self.__class__(
			self._fileName,
			self.header,
			self.createIfNotExist,
			self.verifyHeader,
			self.verbose,
			self.encoding,
			self.delimiter,
			self.defaults,
			self.strict,
			self.correctColumnNum,
			self.indexes,
			self.fileObj,
		)
		eprint("""
		Warning: Copying TSVZedLite will share the same file object and indexes. 
		Changes in one will affect the other.
		There is likely very little reason to copy a TSVZedLite instance unless you are immadiately then calling switchFile() on it.
		""")
		return new

	@classmethod
	def fromkeys(cls, iterable, value=None,fileName = None,header = '',createIfNotExist = True,verifyHeader = True,verbose = False,encoding = 'utf8',
					delimiter = ...,defaults = None,strict = True,correctColumnNum = -1):
		'''Create a new ordered dictionary with keys from iterable and values set to value.
		'''
		self = cls(fileName,header,createIfNotExist,verifyHeader,verbose,encoding,delimiter,defaults,strict,correctColumnNum)
		for key in iterable:
			self[key] = value
		return self
	
	def __eq__(self, other):
		if isinstance(other, TSVZedLite):
			eprint("Warning: Comparing two TSVZedLite instances will only compare their indexes. Data content is not compared.")
			return self.indexes == other.indexes
		return super().__eq__(other)
	
	def __ior__(self, other):
		self.update(other)
		return self

	# Implement context manager methods
	def __enter__(self):
		return self
	
	def close(self):
		if self.fileObj is not None:
			self.fileObj.close()
		return self

	def __exit__(self,exc_type,exc_value,traceback):
		return self.close()




# ===========================================================================
# Write handler (tsvz-spec-v1 §21): the store engine
# ===========================================================================
#: Bytes before the read offset that must be unchanged for an incremental read.
_SERVE_CHECK_BYTES = 64
#: A directory modified less than this long ago may change again within the same
#: timestamp tick, so its part list is checked on every sync until it is older.
_SERVE_RACY_SECONDS = 2.0


def _partRows(store, reporter):
	"""Rows for ``parts`` (spec §20.2): index, ordinal as the file name spells it, path, flags."""
	if _isSpecPath(store):
		parts, active = _storeParts(store, reporter)
	else:
		parts, active = [store], store  # a loose file is its only part (spec §20.2.4)
	rows = []
	for index, path in enumerate(parts):
		name = _parsePartName(path)
		ordinal = ''
		if name.ordinal is not None:
			base = os.path.basename(path)
			if name.codec:
				base = base[:-len(name.codec) - 1]
			ordinal = base.rpartition('.')[2]
		flags = (['active'] if path == active else []) + ([name.codec] if name.codec else [])
		rows.append([str(index), ordinal, path, ','.join(flags)])
	return rows


class _ServeWriter(object):
	"""The single appender of a store (spec §18.3): each batch of queued payloads is one write.

	``submit`` returns a sequence number; ``wait`` blocks until that payload
	is written (or written and fsync'd). The thread never takes the store's
	state lock.
	"""

	def __init__(self, store):
		self.store = store
		self.cond = threading.Condition()
		self.queue = []
		self.submitted = 0
		self.written = 0
		self.durable = 0
		self.failures = deque(maxlen=64)
		self.unsynced = False
		self.closing = False
		self.thread = threading.Thread(target=self._run, name='tsvz-writer')
		self.thread.daemon = True
		self.thread.start()

	def submit(self, payload, durable):
		with self.cond:
			if self.closing:
				raise RuntimeError('the store is closing')
			self.submitted += 1
			self.queue.append((self.submitted, payload, durable))
			self.cond.notify_all()
			return self.submitted

	def wait(self, seq, durable=False):
		"""Block until payload ``seq`` is written (``durable``: and fsync'd); raise its write error."""
		with self.cond:
			while (self.durable if durable else self.written) < seq:
				self.cond.wait()
			for first, last, error in self.failures:
				if first <= seq <= last:
					raise error

	def barrier(self):
		"""Block until everything submitted so far is written (spec §21.8 read-your-writes)."""
		with self.cond:
			while self.written < self.submitted:
				self.cond.wait()

	def close(self):
		"""Write everything queued, fsync it, and stop the thread."""
		with self.cond:
			self.closing = True
			self.cond.notify_all()
		self.thread.join()

	def _run(self):
		while True:
			with self.cond:
				while not self.queue and not self.closing:
					self.cond.wait()
				if not self.queue:
					break
				batch, self.queue = self.queue, []
			durable = any(entry[2] for entry in batch)
			error = None
			try:
				self.store._append(b''.join(entry[1] for entry in batch), durable)
			except Exception as e:
				error = e
				_warn('TSVZ error: {}: a batch of {} writes failed: {}'.format(self.store.path, len(batch), e),
					  self.store.teeLogger)
			with self.cond:
				last = batch[-1][0]
				self.written = last
				if durable or error is not None:
					self.durable = last
				if error is not None:
					self.failures.append((batch[0][0], last, error))
				elif not durable:
					self.unsynced = True
				self.cond.notify_all()
		if self.unsynced:
			try:
				self.store._fsync()
			except Exception as e:
				_warn('TSVZ error: {}: fsync failed: {}'.format(self.store.path, e), self.store.teeLogger)


class _ServeStore(object):
	"""One store's live view for the write handler (spec §21, design §3).

	``data`` maps each live key to its resolved row, in first-appearance
	order, and is always a replay of the store's files: writes are appended
	through ``_ServeWriter`` and come back through ``sync()`` like any other
	writer's. ``sync()`` replays the newly committed bytes of the active part
	and reloads everything when anything else changed. Every public method
	takes ``self.lock``; ``log`` is the teeLogger its messages go to.
	"""

	def __init__(self, fileName, delimiter=..., header='', defaults=None, strict=False, teeLogger=None,
				 verbose=False, create=False, createDefaults=None):
		self.path = fileName
		self.spec = _isSpecPath(fileName)
		if self.spec:
			self.delimiter = _EXTENSION_DELIMITERS[_parsePartName(fileName).ext]
		else:
			self.delimiter = get_delimiter(delimiter, file_name=fileName)
		self.defaults = list(defaults) if defaults else []
		self.strict = strict
		self.teeLogger = teeLogger
		self.verbose = verbose
		self.lock = threading.RLock()
		self.data = OrderedDict()
		self.unreadable = False
		if create:
			self._create(header, self.defaults if createDefaults is None else createDefaults)
		self.reload(teeLogger)
		self.writer = _ServeWriter(self)

	def _create(self, header, defaults):
		header = _formatHeader(header, teeLogger=self.teeLogger, delimiter=self.delimiter)
		if self.spec:
			_specEnsureStore(self.path, True, header, _normalizeDefaults(defaults, self.delimiter), False,
							 self.teeLogger, self.delimiter)
		else:
			_verifyFileExistence(self.path, createIfNotExist=True, teeLogger=self.teeLogger, header=header,
								 strict=False, delimiter=self.delimiter)

	# -- reading the files ---------------------------------------------------
	def reload(self, log):
		"""Replay the whole store."""
		with self.lock:
			reporter = _Reporter(self.path, log)
			try:
				self._noteDirMtime(_dirMtimeNs(self.path))
				if self.spec:
					self._loadSpec(reporter, log)
				else:
					self._loadLegacy(reporter)
				self.unreadable = reporter.has('unreadable')
			finally:
				reporter.flush()

	def _noteDirMtime(self, dirMtime):
		self.dirMtime = dirMtime
		self.dirRacy = time.time() - dirMtime / 1e9 < _SERVE_RACY_SECONDS

	def _stamps(self, parts):
		stamps = {}
		for path in parts:
			try:
				stamps[path] = _specStamp(os.stat(path))
			except OSError:
				stamps[path] = None
		return stamps

	def _loadSpec(self, reporter, log):
		self.parts = _storeParts(self.path)[0]
		self.stamps = self._stamps(self.parts)
		load = _specLoad(self.path, self.delimiter, defaults=_normalizeDefaults(self.defaults, self.delimiter),
						 strict=self.strict, taskDic=OrderedDict(), reporter=reporter, teeLogger=log,
						 verbose=self.verbose)
		self.data, self.state, self.width = load.data, load.state, load.correctColumnNum
		if load.parts != self.parts:  # the part list changed while it was read
			self.parts = load.parts
			self.stamps = {}
		info = load.infos[-1] if load.infos else None
		self.offset = info.committed if info is not None else 0
		self.lineNo = info.lines if info is not None else 0
		self.tailSeen = info.tail if info is not None else b''
		self.label = os.path.basename(self.parts[-1]) + ' ' if len(self.parts) > 1 else ''
		self.check = self._readCheck()

	def _loadLegacy(self, reporter):
		self.parts = [self.path] if os.path.isfile(self.path) else []
		self.stamps = self._stamps(self.parts)
		self.data = OrderedDict()
		self.legacyDefaults = list(self.defaults)
		self.width, self.offset, self.lineNo, self.tailSeen = -1, 0, 0, b''
		if self.parts:
			try:
				with openFileAsCompressed(self.path, mode='rb', teeLogger=self.teeLogger) as f:
					self.width, self.offset, self.lineNo = _legacyReadLoop(
						f, self.path, self.data, -1, 0, self.strict, self.delimiter, self.legacyDefaults, False,
						'utf8', reporter)
			except OSError as e:
				reporter.note('unreadable', None, 'could not read {} ({})'.format(self.path, e))
		self.label = ''
		self.check = self._readCheck()

	def _readCheck(self):
		"""The bytes just before the read offset of the active part ('' when compressed or empty)."""
		if not self.parts or _parsePartName(self.parts[-1]).codec or not self.offset:
			return b''
		try:
			with open(self.parts[-1], 'rb') as f:
				start = max(0, self.offset - _SERVE_CHECK_BYTES)
				f.seek(start)
				return f.read(self.offset - start)
		except OSError:
			return b''

	def sync(self, log):
		"""Bring the view up to date with the files: follow appends, reload on anything else."""
		with self.lock:
			dirMtime = _dirMtimeNs(self.path)
			if dirMtime != self.dirMtime or self.dirRacy:
				parts = _storeParts(self.path)[0] if self.spec else ([self.path] if os.path.isfile(self.path) else [])
				if parts != self.parts:
					return self.reload(log)
				self._noteDirMtime(dirMtime)
			for path in self.parts:
				try:
					stamp = _specStamp(os.stat(path))
				except OSError:
					return self.reload(log)
				if stamp == self.stamps.get(path):
					continue
				if path != self.parts[-1] or not self._follow(path, stamp, log):
					return self.reload(log)

	def _follow(self, path, stamp, log):
		"""Replay what was appended to the active part; False when it changed some other way."""
		old = self.stamps.get(path)
		if _parsePartName(path).codec or old is None or stamp[0] != old[0] or stamp[1] < self.offset:
			return False
		try:
			with open(path, 'rb') as f:
				f.seek(self.offset - len(self.check))
				if f.read(len(self.check)) != self.check:
					return False
				data = f.read()
		except OSError:
			return False
		end = data.rfind(b'\n') + 1
		reporter = _Reporter(self.path, log)
		try:
			if end:
				for raw in data[:end].split(b'\n')[:-1]:
					self._apply(raw + b'\n', reporter)
			tail = data[end:]
			if tail and tail != self.tailSeen:
				reporter.note('tail', None, 'ignored uncommitted bytes after the last newline: {!r}'.format(tail[:80]))
			self.tailSeen = tail
		finally:
			reporter.flush()
		self.offset += end
		self.check = (self.check + data[:end])[-_SERVE_CHECK_BYTES:]
		self.stamps[path] = (stamp[0], self.offset + len(data) - end, stamp[2])
		return True

	def _apply(self, raw, reporter):
		"""Replay one appended committed line of the active part into the view."""
		self.lineNo += 1
		if not self.spec:
			try:
				text = raw.decode('utf8')
			except UnicodeDecodeError:
				text = raw.decode('utf8', errors='replace')
				reporter.note('decode', 'line {}'.format(self.lineNo), 'invalid utf8 replaced with U+FFFD')
			self.width, _ = _processLine(text, self.data, self.width, strict=self.strict, delimiter=self.delimiter,
										 defaults=self.legacyDefaults, reporter=reporter)
			return
		replayed = _specReplayLine(raw, self.lineNo, self.label, self.parts[-1], self.state, self.delimiter,
								   reporter)
		if replayed is None or replayed[1] is None:
			return
		key, row = replayed[1]
		if row is None:
			self.data.pop(key, None)
			return
		self.width = _specPadRow(row, self.state, self.width)
		self.data[key] = row

	def _fresh(self, log):
		"""Read-your-writes (spec §21.8): wait for queued writes, then follow the files."""
		self.writer.barrier()
		self.sync(log)

	# -- reads ----------------------------------------------------------------
	def resolve(self, key):
		"""A requested key as a reader resolves it (spec §7.7; 3.39 rstrip for loose files)."""
		if self.spec:
			return key.rstrip(' \t') if self.state.strip else key
		return key.rstrip()

	def read(self, log):
		with self.lock:
			self._fresh(log)
			return [list(row) for row in self.data.values()]

	def get(self, keys, log):
		"""Rows for ``keys`` and whether one was missing (spec §20.2 ``get``, §14.5)."""
		with self.lock:
			self._fresh(log)
			rows = []
			missing = False
			for key in keys:
				key = self.resolve(key)
				row = self.data.get(key)
				if row is None:
					missing = True
					if not (self.spec and self.state.returnDefaults):
						continue
					row = [key] + list(self.state.defaults[1:])
					row += [''] * (self.width - len(row))
				rows.append(list(row))
			return rows, missing

	def has(self, key, log):
		with self.lock:
			self._fresh(log)
			return self.resolve(key) in self.data

	def length(self, log):
		with self.lock:
			self._fresh(log)
			return len(self.data)

	def keys(self, log):
		with self.lock:
			self._fresh(log)
			return list(self.data)

	def verify(self, log):
		"""Checksum mismatches (spec §15) and whether every part could be read."""
		with self.lock:
			self.writer.barrier()
			if not self.spec:
				return [], True
			reporter = _Reporter(self.path, log)
			try:
				mismatches = _specVerify(self.path, self.delimiter, reporter)
				return mismatches, not reporter.has('unreadable')
			finally:
				reporter.flush()

	def partRows(self, log):
		with self.lock:
			self.writer.barrier()
			reporter = _Reporter(self.path, log)
			try:
				return _partRows(self.path, reporter)
			finally:
				reporter.flush()

	# -- writes ---------------------------------------------------------------
	def _payload(self, rows, log):
		"""Bytes for ``rows`` (``[(cells, marker), ...]``), formatted as the direct writers format them."""
		if self.spec:
			return ''.join(_specFormatRecord(cells, self.delimiter, marker=marker) + '\n'
						   for cells, marker in rows).encode('utf-8')
		return _legacyFormatPayload(self.path, [cells for cells, marker in rows], log, [''], True, self.verbose,
									'utf8', self.strict, self.delimiter)[0]

	def _durable(self, sync):
		return sync or (self.spec and self.state.writeAck == 'disk')

	def _submit(self, rows, sync, log):
		"""Queue ``rows`` (caller holds the lock); return ``(seq, durable)`` or None when there is nothing to write."""
		payload = self._payload(rows, log)
		if not payload:
			return None
		durable = self._durable(sync)
		return self.writer.submit(payload, durable), durable

	def _ack(self, queued):
		"""Acknowledge per spec §21.9: at once for ``memory``, after fsync for ``disk``."""
		if queued is not None and queued[1]:
			self.writer.wait(queued[0], durable=True)

	def write(self, rows, sync, log):
		"""Append ``rows`` (``[(cells, marker), ...]``) as one batch: ``set``, ``delete`` and bulk input."""
		with self.lock:
			queued = self._submit(rows, sync, log)
		self._ack(queued)

	def pop(self, key, sync, log):
		"""Atomically return the row of ``key`` and append its tombstone; None when it is missing."""
		with self.lock:
			self._fresh(log)
			key = self.resolve(key)
			row = self.data.get(key)
			if row is None:
				return None
			row = list(row)
			queued = self._submit([([key], False)], sync, log)
		self._ack(queued)
		return row

	def popitem(self, last, sync, log):
		"""Atomically return the row of the last (or first) live key and delete it; None when empty."""
		with self.lock:
			self._fresh(log)
			if not self.data:
				return None
			key = next(reversed(self.data)) if last else next(iter(self.data))
			row = list(self.data[key])
			queued = self._submit([([key], False)], sync, log)
		self._ack(queued)
		return row

	def setdefault(self, cells, sync, log):
		"""Atomically return the row of ``cells[0]``, appending ``cells`` first when the key is missing."""
		with self.lock:
			self._fresh(log)
			key = self.resolve(cells[0])
			row = self.data.get(key)
			if row is not None:
				return list(row)
			queued = self._submit([(cells, False)], sync, log)
			if queued is not None:
				self.writer.wait(queued[0])
			self.sync(log)
			row = self.data.get(key)
			row = list(row) if row is not None else None
		self._ack(queued)
		return row

	def clear(self, log):
		"""``clear`` (spec §20.2) through the library; True when the store was cleared."""
		with self.lock:
			self.writer.barrier()
			try:
				if self.spec:
					return _specClearTabularFile(self.path, teeLogger=log, verbose=self.verbose,
												 delimiter=self.delimiter)
				clearTabularFile(self.path, teeLogger=log, verbose=self.verbose, delimiter=self.delimiter)
				return True
			finally:
				self.reload(log)

	def scrub(self, log):
		"""``scrub`` (spec §20.2) through the library; True when it was not refused."""
		with self.lock:
			self.writer.barrier()
			try:
				if self.spec:
					return _specScrub(self.path, teeLogger=log, verifyHeader=False, verbose=self.verbose,
									  strict=self.strict, delimiter=self.delimiter, defaults=self.defaults)[1]
				scrubTabularFile(self.path, teeLogger=log, verifyHeader=False, verbose=self.verbose,
								 strict=self.strict, delimiter=self.delimiter, defaults=self.defaults)
				return True
			finally:
				self.reload(log)

	def flush(self):
		"""Write and fsync everything submitted so far (``stop``, spec §21.9)."""
		self.writer.barrier()
		with self.lock:
			self._fsync()

	def close(self):
		"""Write and fsync everything queued and stop the writer."""
		self.writer.close()

	# -- called by the writer thread -----------------------------------------------
	def _append(self, payload, fsync):
		reporter = _Reporter(self.path, self.teeLogger)
		try:
			if self.spec:
				_specAppendPayload(_storeParts(self.path)[1], payload, reporter, fsync=fsync)
			elif _isCompressedFile(self.path):
				with openFileAsCompressed(self.path, mode='ab', teeLogger=self.teeLogger) as f:
					f.write(payload)
			else:
				_lockedAppend(self.path, payload, 'newline', reporter, fsync=fsync)
		finally:
			reporter.flush()

	def _fsync(self):
		path = _storeParts(self.path)[1] if self.spec else self.path
		if os.path.exists(path):
			with open(path, 'ab') as f:
				os.fsync(f.fileno())


# ===========================================================================
# Write handler (tsvz-spec-v1 §21): the protocol
# ===========================================================================
_SERVE_PROTOCOL_VERSION = 1
#: A request or bulk line longer than this closes the connection (spec §21.10).
_SERVE_LINE_LIMIT = 64 << 20
#: operation -> (minimum, maximum) number of arguments (None: no maximum).
_SERVE_ARITY = {
	'read': (0, 0), 'len': (0, 0), 'keys': (0, 0), 'clear': (0, 0), 'scrub': (0, 0), 'verify': (0, 0),
	'parts': (0, 0), 'stop': (0, 0), 'get': (1, None), 'set': (1, None), 'append': (1, None),
	'delete': (1, None), 'has': (1, 1), 'pop': (1, 1), 'popitem': (0, 1), 'setdefault': (2, None),
}
_SERVE_UNREADABLE = 'could not read every part of the store'


class _ServeUsage(Exception):
	"""A request that does not follow spec §21.5 / §21.7 (status 2)."""


def _serveEncode(value):
	"""A protocol field (spec §21.4): §13 with TAB, a leading '#' left as written."""
	return _specEncodeField(value, '\t')


def _serveRecordLine(cells, marker):
	"""A record as a protocol line: a marker key as written, a data key with ``<#>`` (spec §21.4)."""
	if marker:
		return '\t'.join([cells[0]] + [_serveEncode(cell) for cell in cells[1:]])
	return _specFormatRecord(cells, '\t')


def _serveFieldsLine(fields):
	"""An output line of plain fields (``verify``, ``parts``): §13 with TAB, a leading '#' as ``<#>``."""
	return '\t'.join([_specEncodeField(str(fields[0]), '\t', isKey=True)] + [_serveEncode(str(field)) for field in fields[1:]])


def _serveDecodeLine(line):
	"""The fields of a protocol record line, decoded per §13."""
	return [_specDecodeField(field, '\t') for field in line.split('\t')]


def _serveParseRequest(line):
	"""Split a request line (spec §21.5) into ``(options, operation, args, rawArgs)``."""
	raw = line.split('\t')
	i = 0
	while i < len(raw) and raw[i].startswith('--'):
		i += 1
	if i >= len(raw) or not raw[i]:
		raise _ServeUsage('missing operation')
	return raw[:i], raw[i], [_specDecodeField(field, '\t') for field in raw[i + 1:]], raw[i + 1:]


def _serveNeedsBulk(line):
	"""True when ``line`` is ``set -`` / ``append -`` / ``delete -``: record lines up to ``#`` follow."""
	try:
		_, operation, _, raw = _serveParseRequest(line)
	except _ServeUsage:
		return False
	return operation in ('set', 'append', 'delete') and raw == ['-']


class _ServeLog(object):
	"""Per-request teeLogger: warnings and errors become ``#!`` lines and also go to ``logger``."""

	def __init__(self, logger=None):
		self.logger = logger
		self.lines = []

	def teelog(self, message, level='info', callerStackDepth=None):
		if level in ('warning', 'error', 'critical'):
			self.lines.append(str(message))
		if self.logger is not None:
			self.logger.teelog(message, level)


def _serveDispatch(store, line, bulk, log):
	"""Run one request against ``store`` (spec §21.7); return ``(status, lines, message)``."""
	options, operation, args, raw = _serveParseRequest(line)
	for option in options:
		if option != '--sync':
			raise _ServeUsage('unknown option {}'.format(option))
	sync = '--sync' in options
	if operation not in _SERVE_ARITY:
		raise _ServeUsage('unknown operation {}'.format(operation))
	low, high = _SERVE_ARITY[operation]
	if len(args) < low or (high is not None and len(args) > high):
		raise _ServeUsage('wrong number of arguments for {}'.format(operation))
	isBulk = operation in ('set', 'append', 'delete') and raw == ['-']
	keys = args if operation in ('get', 'delete', 'has', 'pop') else args[:1]
	if operation not in ('read', 'len', 'keys', 'clear', 'scrub', 'verify', 'parts', 'stop', 'popitem') \
			and not isBulk and '' in keys:
		raise _ServeUsage('a KEY must not be empty')
	unreadable = (1, _SERVE_UNREADABLE)
	if operation == 'read':
		rows = store.read(log)
		status = unreadable if store.unreadable else (0, '')
		return status[0], [_specFormatRecord(row, '\t') for row in rows], status[1]
	if operation == 'get':
		rows, missing = store.get(args, log)
		status = unreadable if store.unreadable else (3 if missing else 0, '')
		return status[0], [_specFormatRecord(row, '\t') for row in rows], status[1]
	if operation == 'has':
		found = store.has(args[0], log)
		return (1, [], _SERVE_UNREADABLE) if store.unreadable else (0 if found else 3, [], '')
	if operation == 'len':
		return 0, [str(store.length(log))], ''
	if operation == 'keys':
		return 0, [_specEncodeField(key, '\t', isKey=True) for key in store.keys(log)], ''
	if operation in ('set', 'append', 'delete'):
		keysOnly = operation == 'delete'
		if isBulk:
			reporter = _Reporter('<request>', log)
			try:
				rows = _parseRecordLines(bulk or [], '\t', True, keysOnly, reporter)
			finally:
				reporter.flush()
		elif keysOnly:
			rows = [([key], bool(_MARKER_RE.match(field))) for key, field in zip(args, raw)]
		else:
			rows = [(args, bool(_MARKER_RE.match(raw[0])))]
		store.write(rows, sync, log)
		return 0, [], ''
	if operation in ('pop', 'popitem'):
		if operation == 'pop':
			row = store.pop(args[0], sync, log)
		else:
			if args and args[0] not in ('first', 'last'):
				raise _ServeUsage('popitem takes first or last')
			row = store.popitem(not args or args[0] == 'last', sync, log)
		return (3, [], '') if row is None else (0, [_specFormatRecord(row, '\t')], '')
	if operation == 'setdefault':
		if _MARKER_RE.match(raw[0]):
			raise _ServeUsage('setdefault takes a data KEY, not a marker')
		row = store.setdefault(args, sync, log)
		return (3, [], '') if row is None else (0, [_specFormatRecord(row, '\t')], '')
	if operation in ('clear', 'scrub'):
		done = store.clear(log) if operation == 'clear' else store.scrub(log)
		return (0, [], '') if done else (1, [], '{} refused; nothing was written'.format(operation))
	if operation == 'verify':
		mismatches, readable = store.verify(log)
		lines = [_serveFieldsLine(entry) for entry in mismatches]
		return (1 if not readable else 4 if lines else 0), lines, '' if readable else _SERVE_UNREADABLE
	if operation == 'parts':
		return 0, [_serveFieldsLine(row) for row in store.partRows(log)], ''
	return 1, [], 'not served by a handler'  # stop, sent to an in-process store


def _serveRespond(store, line, bulk=None, logger=None):
	"""The response to one request (spec §21.6), as lines without their '\\n'."""
	log = _ServeLog(logger)
	try:
		status, lines, message = _serveDispatch(store, line, bulk, log)
	except _ServeUsage as e:
		status, lines, message = 2, [], str(e)
	except Exception as e:
		status, lines, message = 1, [], str(e) or type(e).__name__
		if logger is not None:
			logger.teelog('tsvz: {}: request {!r} failed: {}'.format(store.path, line[:80], message), 'error')
	response = ['#!\t' + _serveEncode(text) for text in log.lines] + list(lines)
	response.append('#{}\t{}'.format(status, _serveEncode(message)) if message else '#{}'.format(status))
	return response


def _serveParseResponse(lines):
	"""``(lines, diagnostics, status, message)`` from a response's lines (spec §21.6)."""
	out, diagnostics = [], []
	for line in lines:
		if line.startswith('#!'):
			diagnostics.append(_specDecodeField(line[3:], '\t'))
		elif line.startswith('#'):
			head, _, message = line[1:].partition('\t')
			status = int(head) if _ASCII_DIGITS_RE.match(head) else 1
			return out, diagnostics, status, _specDecodeField(message, '\t')
		else:
			out.append(line)
	raise _ServeLost('the response ended without a status line')


class _ServeLost(Exception):
	"""The connection to a handler broke during a request; whether it was applied is unknown."""


class _ServeLocal(object):
	"""The in-process stand-in for a handler connection: the same engine and requests, no socket."""

	def __init__(self, store):
		self.store = store

	def request(self, line, bulk=None):
		return _serveParseResponse(_serveRespond(self.store, line, bulk))

	def close(self):
		self.store.close()


# ===========================================================================
# Write handler (tsvz-spec-v1 §21): pointer files, connections and the server
# ===========================================================================
class _ServeBusy(Exception):
	"""Another handler holds the store's pointer file (spec §21.2); ``args[0]`` is its pointer, as a dict."""


class _ServeUnreachable(Exception):
	"""A pointer file whose handler cannot be used; ``quiet`` when that is expected (other host, no permission)."""

	def __init__(self, message, quiet=False):
		Exception.__init__(self, message)
		self.quiet = quiet


class _ServeSignal(Exception):
	"""SIGTERM arrived while ``tsvz serve`` was serving."""


def _servePointerPath(store):
	"""The pointer file of a store (spec §21.2): the store path plus ``.serve``."""
	return store + '.serve'


def _serveFind(store):
	"""Read the pointer file of ``store`` (spec §21.2); return its fields as a dict, or None.

	None when there is no pointer file, or it does not parse even after one
	short retry (a handler may be writing it).
	"""
	path = _servePointerPath(store)
	for attempt in range(2):
		if attempt:
			time.sleep(0.05)
		try:
			with open(path, 'rb') as f:
				lines = f.read().decode('utf-8', 'replace').split('\n')
		except OSError:
			return None
		if lines[0] != 'tsvz-handler\t{}'.format(_SERVE_PROTOCOL_VERSION):
			continue
		info = {}
		for line in lines[1:]:
			name, tab, value = line.partition('\t')
			if tab:
				info[name] = _specDecodeField(value, '\t')
		if 'address' in info and 'host' in info:
			return info
	return None


class _ServeConnection(object):
	"""A connection to the handler a pointer file names (spec §21.3–§21.6, §21.11)."""

	def __init__(self, info, timeout=None):
		import errno
		import socket
		if info.get('host') != socket.gethostname():
			raise _ServeUnreachable('it runs on host {}'.format(info.get('host')), quiet=True)
		address = info.get('address', '')
		try:
			if address.startswith('unix:') and hasattr(socket, 'AF_UNIX'):
				sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
				try:
					sock.settimeout(0.5)
					sock.connect(address[5:])
				except Exception:
					sock.close()
					raise
			elif address.startswith('tcp:'):
				host, _, port = address[4:].rpartition(':')
				sock = socket.create_connection((host.strip('[]'), int(port)), 0.5)
			else:
				raise _ServeUnreachable('unusable address {!r}'.format(address), quiet=True)
		except (OSError, ValueError) as e:
			raise _ServeUnreachable(str(e), quiet=getattr(e, 'errno', None) in (errno.EACCES, errno.EPERM))
		sock.settimeout(timeout)
		self.sock = sock
		self.rfile = sock.makefile('rb')
		if info.get('token'):
			try:
				status = self.request('auth\t' + info['token'])[2]
			except _ServeLost:
				status = 1
			if status != 0:
				self.close()
				raise _ServeUnreachable('the handler refused the token')

	def request(self, line, bulk=None):
		"""Send one request, followed by ``bulk`` record lines and ``#`` when given; return the parsed response."""
		text = line + '\n'
		if bulk is not None:
			text += ''.join(record + '\n' for record in bulk) + '#\n'
		try:
			self.sock.sendall(text.encode('utf-8', 'replace'))
			lines = []
			while True:
				raw = self.rfile.readline()
				if not raw.endswith(b'\n'):
					raise _ServeLost('the handler closed the connection')
				line = raw[:-1].decode('utf-8', 'replace')
				lines.append(line)
				if line.startswith('#') and not line.startswith('#!'):
					return _serveParseResponse(lines)
		except OSError as e:
			raise _ServeLost(str(e))

	def close(self):
		for thing in (self.rfile, self.sock):
			try:
				thing.close()
			except Exception:
				pass


_SERVE_CLASSES = {}


def _serveClasses():
	"""The socketserver classes of the handler; ``socketserver`` is imported on first use."""
	if not _SERVE_CLASSES:
		import socketserver

		class Handler(socketserver.StreamRequestHandler):
			def handle(self):
				self.server.serving.connection(self.rfile, self.wfile, self.request)

		class TCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
			daemon_threads = True
			allow_reuse_address = True

		_SERVE_CLASSES.update(handler=Handler, tcp=TCPServer)
		if hasattr(socketserver, 'UnixStreamServer'):
			class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
				daemon_threads = True

			_SERVE_CLASSES['unix'] = UnixServer
	return _SERVE_CLASSES


def _serveGroup(group):
	"""A group name or number as a gid (None stays None)."""
	if group is None or group == '':
		return None
	if _ASCII_DIGITS_RE.match(str(group)):
		return int(group)
	import grp
	return grp.getgrnam(group).gr_gid


def _serveIsStop(line):
	"""True when ``line`` is a well-formed ``stop`` request."""
	try:
		options, operation, args, _ = _serveParseRequest(line)
	except _ServeUsage:
		return False
	return operation == 'stop' and not args and all(option == '--sync' for option in options)


class _ServeHandle(object):
	"""A running write handler for one store (spec §21; design §3).

	``lock()`` takes the pointer file, ``start(store)`` binds the socket and
	writes the pointer, ``serve()`` answers requests until ``stop()`` (or an
	exception in the serving thread), ``close()`` writes everything and
	removes the socket and the pointer file.
	"""

	def __init__(self, storePath, logger=None, group=None, mode=None, tcp=False, idleTimeout=0):
		self.storePath = storePath
		self.logger = logger
		self.group = group
		self.mode = mode
		self.tcp = tcp
		self.idleTimeout = idleTimeout
		self.pointer = _servePointerPath(storePath)
		self.pointerFile = None
		self.store = None
		self.server = None
		self.socketDir = None
		self.address = None
		self.token = None
		self.guard = threading.Lock()
		self.active = 0
		self.sockets = set()
		self.lastActivity = time.monotonic()
		self.stopping = threading.Event()

	def lock(self):
		"""Take the pointer file's lock (spec §21.2); raise ``_ServeBusy`` when another handler has it."""
		f = os.fdopen(os.open(self.pointer, os.O_RDWR | os.O_CREAT, 0o644), 'r+b')
		if not _tryLockFile(f):
			f.close()
			raise _ServeBusy(_serveFind(self.storePath) or {})
		self.pointerFile = f

	def start(self, store):
		"""Bind the socket for ``store`` and write the pointer file."""
		import socket
		import tempfile
		self.store = store
		classes = _serveClasses()
		gid = _serveGroup(self.group)
		mode = self.mode if self.mode is not None else (0o660 if gid is not None else 0o600)
		if self.tcp or 'unix' not in classes:
			self.server = classes['tcp'](('127.0.0.1', 0), classes['handler'])
			self.address = 'tcp:127.0.0.1:{}'.format(self.server.server_address[1])
			self.token = ''.join('%02x' % b for b in os.urandom(16))
		else:
			base = os.environ.get('XDG_RUNTIME_DIR', '')
			if not base or not os.path.isdir(base) or not os.access(base, os.W_OK):
				base = tempfile.gettempdir()
			self.socketDir = tempfile.mkdtemp(prefix='tsvz-', dir=base)
			path = os.path.join(self.socketDir, 's')
			self.server = classes['unix'](path, classes['handler'])
			if gid is not None:
				os.chown(self.socketDir, -1, gid)
				os.chown(path, -1, gid)
			os.chmod(path, mode)
			os.chmod(self.socketDir, 0o700 | (0o010 if mode & 0o070 else 0) | (0o001 if mode & 0o007 else 0))
			self.address = 'unix:' + path
		self.server.serving = self
		lines = ['tsvz-handler\t{}'.format(_SERVE_PROTOCOL_VERSION), 'address\t' + _serveEncode(self.address),
				 'host\t' + _serveEncode(socket.gethostname()), 'pid\t{}'.format(os.getpid())]
		if self.token:
			lines.append('token\t' + self.token)
		if not store.spec:
			lines.append('x-delimiter\t' + _serveEncode(store.delimiter))
		try:
			os.chmod(self.pointer, mode if self.token else 0o644)
		except OSError:
			pass
		f = self.pointerFile
		f.seek(0)
		f.truncate()
		f.write(''.join(line + '\n' for line in lines).encode('utf-8'))
		f.flush()
		os.fsync(f.fileno())

	def serve(self):
		"""Answer requests in this thread until ``stop()`` or the idle timeout."""
		if self.idleTimeout:
			watcher = threading.Thread(target=self._watchIdle, name='tsvz-idle')
			watcher.daemon = True
			watcher.start()
		self.server.serve_forever(poll_interval=0.2)

	def stop(self):
		"""Make ``serve()`` return (from any other thread)."""
		self.stopping.set()
		if self.server is not None:
			stopper = threading.Thread(target=self.server.shutdown, name='tsvz-stop')
			stopper.daemon = True
			stopper.start()

	def close(self):
		"""Stop listening, end open connections, write and fsync everything, remove the socket and the pointer."""
		import socket
		if self.server is not None:
			self.server.server_close()
		with self.guard:
			sockets = list(self.sockets)
		for sock in sockets:
			try:
				sock.shutdown(socket.SHUT_RDWR)
			except OSError:
				pass
		if self.store is not None:
			self.store.close()
		if self.socketDir is not None:
			shutil.rmtree(self.socketDir, ignore_errors=True)
		if self.pointerFile is not None:
			try:
				if os.stat(self.pointer).st_ino == os.fstat(self.pointerFile.fileno()).st_ino:
					os.unlink(self.pointer)
			except OSError:
				pass
			self.pointerFile.close()
			self.pointerFile = None

	def _watchIdle(self):
		while not self.stopping.wait(min(1.0, self.idleTimeout / 4.0)):
			with self.guard:
				idle = not self.active and time.monotonic() - self.lastActivity >= self.idleTimeout
			if idle:
				if self.logger is not None:
					self.logger.teelog('tsvz: {}: idle for {:g} s; stopping'.format(self.storePath, self.idleTimeout))
				self.stop()
				return

	def connection(self, rfile, wfile, sock=None):
		"""Answer the requests of one connection (spec §21.3–§21.11)."""
		with self.guard:
			self.active += 1
			if sock is not None:
				self.sockets.add(sock)
		try:
			authed = self.token is None
			while True:
				line = self._readLine(rfile)
				if line is None:
					return
				fields = line.split('\t')
				if fields[0] == 'auth':
					import hmac
					ok = len(fields) == 2 and (self.token is None or hmac.compare_digest(fields[1], self.token))
					self._send(wfile, ['#0'] if ok else ['#1\tauthentication failed'])
					if not ok:
						return
					authed = True
					continue
				if not authed:
					self._send(wfile, ['#1\tauthentication required'])
					return
				bulk = None
				if _serveNeedsBulk(line):
					bulk = []
					while True:
						record = self._readLine(rfile)
						if record is None:
							return
						if record == '#':
							break
						bulk.append(record)
				if _serveIsStop(line):
					self.store.flush()
					self._send(wfile, ['#0'])
					self.stop()
					return
				self._send(wfile, _serveRespond(self.store, line, bulk, self.logger))
		except OSError:
			return  # the client went away
		finally:
			with self.guard:
				self.active -= 1
				self.sockets.discard(sock)
				self.lastActivity = time.monotonic()

	def _readLine(self, rfile):
		"""One request line without its terminator; None at the end of the connection or past the limit."""
		raw = rfile.readline(_SERVE_LINE_LIMIT + 1)
		if not raw.endswith(b'\n'):
			return None
		text = raw[:-1].decode('utf-8', 'replace')
		return text[:-1] if text.endswith('\r') else text

	def _send(self, wfile, lines):
		wfile.write(''.join(line + '\n' for line in lines).encode('utf-8', 'replace'))
		wfile.flush()


def _serveSignals():
	"""Make SIGTERM stop ``serve`` as SIGINT does; return a function that restores the old handler."""
	import signal
	if threading.current_thread() is not threading.main_thread() or not hasattr(signal, 'SIGTERM'):
		return lambda: None

	def handler(signum, frame):
		raise _ServeSignal()
	old = signal.signal(signal.SIGTERM, handler)
	return lambda: signal.signal(signal.SIGTERM, old)


# ===========================================================================
# Command-line interface (tsvz-spec-v1 §20)
# ===========================================================================
_CLI_OPERATIONS = ('read', 'get', 'set', 'append', 'delete', 'clear', 'scrub', 'verify', 'parts',
				   'has', 'len', 'keys', 'pop', 'popitem', 'setdefault', 'serve', 'stop')
#: option -> (destination, takes a value). The 3.39 spellings -c/--header,
#: --defaults, -s/--strict and -f/--force are undocumented aliases of the
#: --x- extensions (spec §20.7.4).
_CLI_OPTIONS = {
	'-h': ('help', False), '--help': ('help', False),
	'-V': ('version', False), '--version': ('version', False),
	'-q': ('quiet', False), '--quiet': ('quiet', False),
	'-v': ('verbose', False), '--verbose': ('verbose', False),
	'--format': ('format', True),
	'-d': ('delimiter', True), '--delimiter': ('delimiter', True),
	'--x-header': ('header', True), '-c': ('header', True), '--header': ('header', True),
	'--x-defaults': ('defaults', True), '--defaults': ('defaults', True),
	'--x-strict': ('strict', False), '-s': ('strict', False), '--strict': ('strict', False),
	'--x-force': ('force', False), '-f': ('force', False), '--force': ('force', False),
	'--x-direct': ('direct', False), '--x-idle-timeout': ('idleTimeout', True),
	'--x-group': ('group', True), '--x-mode': ('mode', True), '--x-tcp': ('tcp', False),
}
_CLI_NEGATIVE_NUMBER_RE = re.compile(r'^-[0-9.]')
_CLI_USAGE = 'usage: tsvz [OPTION ...] OPERATION STORE [ARG ...]   (tsvz -h for help)'
_CLI_HELP = '''usage: tsvz [OPTION ...] OPERATION STORE [ARG ...]
       tsvz [OPTION ...] STORE [OPERATION] [ARG ...]   (3.39 form; OPERATION defaults to read)

TSVZ {version}: key-value stores in .tsv/.csv/.nsv/.psv files (TSVZ 3.39 rules)
and in .tsvz/.csvz/.nsvz/.psvz files (tsvz-spec-v1).

operations:
  read STORE                   print every live row
  get STORE KEY [KEY ...]      print the rows of KEYs (exit 3 if one is missing)
  set STORE KEY [VALUE ...]    write a row; KEY alone deletes it; '-' reads rows from stdin
  append ...                   same as set
  delete STORE KEY [KEY ...]   delete KEYs; '-' reads keys from stdin
  clear STORE                  empty the store, keeping its header and markers
  scrub STORE                  compact the store in place (archival maintenance)
  verify STORE                 check #_checksum_*_# segments (exit 4 on a mismatch)
  parts STORE                  list the parts of a multi-part store
  has STORE KEY                exit 0 if KEY is live, 3 if not
  len STORE                    print the number of live keys
  keys STORE                   print the live keys
  pop STORE KEY                print the row of KEY and delete it (exit 3 if missing)
  popitem STORE [first|last]   pop the first or last live key (default: last)
  setdefault STORE KEY VALUE [VALUE ...]
                               print the row of KEY, setting it to the VALUEs if missing
  serve STORE                  keep STORE loaded and answer requests on a local socket
  stop STORE                   stop the server of STORE

options:
  -h, --help                   show this help
  -V, --version                show the version
  --format table|records       output format (default: table on a terminal, else records)
  -q, --quiet                  print errors only
  -v, --verbose                print more detail
  -d, --delimiter D            .tsv-family delimiter: tab, comma, pipe, null or one character
  --x-header H                 header to check, or to create a new file with
  --x-defaults D               defaults row KEY D1 D2 ... joined by the delimiter (KEY is ignored)
  --x-strict, --x-force        turn the 3.39 column and header checks on or off (default: off)
  --x-direct                   work on the files even when a server runs
  --x-idle-timeout S           serve: stop after S seconds without connections
  --x-group G, --x-mode M      serve: let group G connect / set the socket's octal mode
  --x-tcp                      serve: listen on 127.0.0.1 instead of a Unix socket
  --                           every argument after this is positional
  (--x-header and --x-defaults decode backslash escapes such as \\t)

exit status: 0 done, 1 failed or refused, 2 usage error, 3 missing key, 4 checksum mismatch
'''


class _CliUsageError(Exception):
	"""A command line that does not follow spec §20.1 / §20.7 (exit status 2)."""


class _CliArgs(object):
	"""A parsed command line (spec §20.1)."""

	def __init__(self):
		self.help = False
		self.version = False
		self.quiet = False
		self.verbose = False
		self.format = None
		self.delimiter = ...
		self.header = ''
		self.defaults = None
		self.strict = False
		self.direct = False
		self.idleTimeout = None
		self.group = None
		self.mode = None
		self.tcp = False
		self.operation = None
		self.store = None
		self.args = []


_CLI_UNDECODED_RE = re.compile('[\udc80-\udcff]')


def _cliDecodeArgv(argv):
	"""Re-decode as UTF-8 the arguments holding bytes the locale could not decode (Python 3.6 under LANG=C).

	Only data arguments go through here: a STORE path stays as the OS decoded
	it, so that ``open()`` encodes it back to the same bytes.
	"""
	return [os.fsencode(arg).decode('utf-8', 'replace') if _CLI_UNDECODED_RE.search(arg) else arg for arg in argv]


def _cliParseArgs(argv):
	"""Split ``argv`` per spec §20.1 into a ``_CliArgs``; raise ``_CliUsageError`` if it does not parse.

	Options may appear anywhere before a lone ``--``. ``-`` and negative numbers
	(``-5``, ``-.5``) are positional. Single-letter flags combine (``-qv``) and a
	single-letter option takes its value attached (``-dcomma``). The first
	positional argument decides the form: an operation name selects
	``OPERATION STORE [ARG ...]``, anything else the 3.39
	``STORE [OPERATION] [ARG ...]`` form.
	"""
	args = _CliArgs()
	argv = list(argv)
	positionals = []
	i = 0
	while i < len(argv):
		arg = argv[i]
		i += 1
		if arg == '--':
			positionals.extend(argv[i:])
			break
		if not arg.startswith('-') or arg == '-' or _CLI_NEGATIVE_NUMBER_RE.match(arg):
			positionals.append(arg)
			continue
		if arg.startswith('--'):
			name, eq, value = arg.partition('=')
		elif len(arg) > 2 and _CLI_OPTIONS.get(arg[:2], (None, False))[1]:
			value = arg[2:]
			name, eq, value = arg[:2], '=', value[1:] if value.startswith('=') else value  # -dcomma, -d=,
		elif len(arg) > 2:
			name, eq, value = arg[:2], '', ''
			argv.insert(i, '-' + arg[2:])  # -qv: take one flag at a time
		else:
			name, eq, value = arg, '', ''
		if name not in _CLI_OPTIONS and name.startswith('--'):
			# A unique prefix of a long option, as 3.39's argparse accepted (--verb, --delim).
			matches = sorted(option for option in _CLI_OPTIONS if option.startswith(name) and option.startswith('--'))
			if len(set(_CLI_OPTIONS[option] for option in matches)) > 1:
				raise _CliUsageError('ambiguous option {}: {}'.format(name, ', '.join(matches)))
			if matches:
				name = matches[0]
		if name not in _CLI_OPTIONS:
			raise _CliUsageError('unknown option {}'.format(name))
		dest, takesValue = _CLI_OPTIONS[name]
		if takesValue:
			if not eq:
				if i >= len(argv):
					raise _CliUsageError('option {} needs a value'.format(name))
				value = argv[i]
				i += 1
			setattr(args, dest, value)
		elif eq:
			raise _CliUsageError('option {} takes no value'.format(name))
		elif dest == 'force':
			args.strict = False
		else:
			setattr(args, dest, True)
	if args.help or args.version:
		return args
	if args.format not in (None, 'table', 'records'):
		raise _CliUsageError('--format must be table or records, not {!r}'.format(args.format))
	if not positionals:
		raise _CliUsageError('missing OPERATION and STORE')
	if positionals[0] in _CLI_OPERATIONS:
		if len(positionals) < 2:
			raise _CliUsageError('{} needs a STORE'.format(positionals[0]))
		args.operation, args.store, args.args = positionals[0], positionals[1], positionals[2:]
	else:
		args.store = positionals[0]
		if len(positionals) == 1:
			args.operation = 'read'
		elif positionals[1] in _CLI_OPERATIONS:
			args.operation, args.args = positionals[1], positionals[2:]
		else:
			raise _CliUsageError('unknown operation: neither {!r} nor {!r} is one of {}'.format(
				positionals[0], positionals[1], ', '.join(_CLI_OPERATIONS)))
	if args.operation in ('read', 'clear', 'scrub', 'verify', 'parts', 'len', 'keys', 'serve', 'stop') and args.args:
		raise _CliUsageError('{} takes no arguments after STORE'.format(args.operation))
	if args.operation == 'popitem' and args.args not in ([], ['first'], ['last']):
		raise _CliUsageError('popitem takes first or last')
	if args.operation in ('has', 'pop') and len(args.args) > 1:
		raise _CliUsageError('{} takes one KEY'.format(args.operation))
	if args.operation == 'setdefault' and len(args.args) == 1:
		raise _CliUsageError('setdefault needs a KEY and a VALUE')
	if args.operation in ('get', 'set', 'append', 'delete', 'has', 'pop', 'setdefault'):
		if not args.args:
			raise _CliUsageError('{} needs a KEY'.format(args.operation))
		keys = args.args if args.operation in ('get', 'delete') else args.args[:1]
		if '' in keys:
			raise _CliUsageError('{}: a KEY must not be empty'.format(args.operation))
	return args


def _cliWrite(stream, text):
	"""Write ``text`` to ``stream`` as UTF-8 and flush (Python 3.6 under LANG=C cannot encode non-ASCII)."""
	buffer = getattr(stream, 'buffer', None)
	if buffer is None:
		stream.write(text)
	else:
		stream.flush()  # keep the order of text already written through the text layer
		try:
			data = text.encode('utf-8', 'surrogateescape')  # a STORE path the locale could not decode
		except UnicodeEncodeError:
			data = text.encode('utf-8', 'replace')
		buffer.write(data)
	stream.flush()


class _CliLogger(object):
	"""``teeLogger`` for the CLI: every diagnostic on stderr (spec §20.5); ``quiet`` keeps errors only."""

	def __init__(self, stream, quiet=False):
		self.stream = stream
		self.quiet = quiet

	def teelog(self, message, level='info', callerStackDepth=None):
		if self.quiet and level not in ('error', 'critical'):
			return
		_cliWrite(self.stream, '{}\n'.format(message))


def _cliFormat(args, stdout):
	"""The output format (spec §20.4.3): ``--format``, else table on a terminal and records otherwise."""
	if args.format:
		return args.format
	try:
		return 'table' if stdout.isatty() else 'records'
	except Exception:
		return 'records'


def _cliEmitRows(rows, args, stdout, spec, delimiter):
	"""Print store rows for ``read`` / ``get`` (spec §20.4): §13 records, 3.39 records or the 3.39 table."""
	if _cliFormat(args, stdout) == 'table':
		_cliWrite(stdout, pretty_format_table(rows, delimiter=delimiter) + '\n')
		return
	if spec:
		lines = [_specFormatRecord(row, delimiter) for row in rows]
	else:
		lines = [delimiter.join(_sanitize(row, delimiter=delimiter)) for row in rows]
	_cliWrite(stdout, ''.join(line + '\n' for line in lines))


def _cliEmitFields(rows, header, args, stdout):
	"""Print ``verify`` / ``parts`` rows: TAB-joined §13-encoded fields, or a table under ``header``."""
	if _cliFormat(args, stdout) == 'table':
		if rows:
			_cliWrite(stdout, pretty_format_table([header] + rows) + '\n')
		return
	_cliWrite(stdout, ''.join('\t'.join(_specEncodeField(str(field), '\t') for field in row) + '\n' for row in rows))


class _CliStoreMissing(Exception):
	"""An operation that reads a store found none at STORE (spec §20.2.2, exit status 1)."""


def _cliDecodeEscapes(text, label, logger):
	"""Decode backslash escapes in an extension option's value as 3.39 did (``-c 'id\\tval'``).

	Unlike 3.39, non-ASCII text survives. A value that does not decode is
	reported and ignored ('' is returned), as in 3.39.
	"""
	if text.endswith('\\'):
		text += '\\'
	try:
		return codecs.decode(text.encode('latin-1', 'backslashreplace'), 'unicode_escape')
	except Exception:
		logger.teelog('tsvz: failed to decode {} {!r}; ignored'.format(label, text), 'warning')
		return ''


def _cliDelimiter(args, logger):
	"""STORE's delimiter: a strict extension's own (spec §5.1, §20.7), else ``-d``, else inferred from the name.

	A ``-d`` that does not decode is a usage error; one longer than a
	character is used, as 3.39 did, with a warning.
	"""
	given = None
	if args.delimiter is not ... and args.delimiter:
		try:
			given = get_delimiter(args.delimiter)
		except Exception:
			raise _CliUsageError('-d {!r} is not a delimiter'.format(args.delimiter))
	if _isSpecPath(args.store):
		expected = _EXTENSION_DELIMITERS[_parsePartName(args.store).ext]
		if given is not None and given != expected:
			logger.teelog('TSVZ warning: {}: -d {!r} conflicts with the file extension; using {!r} (spec §5.1)'.format(
				args.store, args.delimiter, expected), 'warning')
		return expected
	if given is None:
		return get_delimiter(args.delimiter, file_name=args.store)
	if len(given) != 1:
		logger.teelog('TSVZ warning: {}: -d {!r} is {} characters long; a delimiter should be one character'.format(
			args.store, args.delimiter, len(given)), 'warning')
	return given


def _cliStoreExists(store):
	"""True when STORE exists; a spec store may consist of numbered parts only (spec §17.1)."""
	if _isSpecPath(store):
		return bool(_storeParts(store)[0])
	return os.path.isfile(store)


def _cliWritten(args, logger):
	"""Exit status after a write: 1, with an error, when STORE still does not exist (it could not be created)."""
	if _cliStoreExists(args.store):
		return 0
	logger.teelog('tsvz: {}: the store could not be created; nothing was written'.format(args.store), 'error')
	return 1


def _cliSpecLoad(args, delimiter, logger):
	"""Load a spec store for ``read`` / ``get``; return ``(load, readable)``.

	``readable`` is False when a part could not be opened or read to its end;
	the rows of the other parts are still loaded.
	"""
	reporter = _Reporter(args.store, logger)
	try:
		load = _specLoad(args.store, delimiter, defaults=_normalizeDefaults(args.defaults, delimiter),
						 strict=args.strict, reporter=reporter, teeLogger=logger, verbose=args.verbose)
		readable = not reporter.has('unreadable')
	finally:
		reporter.flush()
	return load, readable


def _cliUnreadable(args, logger):
	"""Exit status 1, with an error that -q keeps, for a store with a part that could not be read."""
	logger.teelog('tsvz: {}: could not read every part of the store'.format(args.store), 'error')
	return 1


def _cliRead(args, delimiter, logger, stdin, stdout):
	"""``read STORE`` (spec §20.2): every live row, in first-appearance order.

	Exit 1 when a part could not be read; the rows of the other parts are printed.
	"""
	if not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	if not _isSpecPath(args.store):
		data = readTabularFile(args.store, teeLogger=logger, verifyHeader=False, verbose=args.verbose,
							   strict=args.strict, delimiter=delimiter, defaults=args.defaults)
		_cliEmitRows(list(data.values()), args, stdout, False, delimiter)
		return 0
	load, readable = _cliSpecLoad(args, delimiter, logger)
	_cliEmitRows(list(load.data.values()), args, stdout, True, delimiter)
	return 0 if readable else _cliUnreadable(args, logger)


def _cliGet(args, delimiter, logger, stdin, stdout):
	"""``get STORE KEY ...`` (spec §20.2): each KEY's row in argument order; exit 3 when one is missing.

	Keys resolve as a reader resolves them (§7.7). For a missing key of a
	spec store this prints what ``TSVZed`` returns for it (§14.5): the key and
	the active defaults while ``#_return_defaults_when_missing_#`` is true,
	else nothing. A missing key of a loose file prints nothing.
	"""
	if not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	spec = _isSpecPath(args.store)
	readable = True
	if spec:
		load, readable = _cliSpecLoad(args, delimiter, logger)
		data, state = load.data, load.state
	else:
		data = readTabularFile(args.store, teeLogger=logger, verifyHeader=False, verbose=args.verbose,
							   strict=args.strict, delimiter=delimiter, defaults=args.defaults)
	rows = []
	missing = False
	for key in args.args:
		if spec:
			key = key.rstrip(' \t') if state.strip else key
		else:
			key = key.rstrip()
		row = data.get(key)
		if row is None:
			missing = True
			if not (spec and state.returnDefaults):
				continue
			row = [key] + list(state.defaults[1:])
			row += [''] * (load.correctColumnNum - len(row))
		rows.append(row)
	_cliEmitRows(rows, args, stdout, spec, delimiter)
	if not readable:
		return _cliUnreadable(args, logger)
	return 3 if missing else 0


def _cliStdinLines(stdin, reporter):
	"""The committed lines of ``stdin`` (spec §4.3, §20.3), decoded as UTF-8, without terminators.

	An unterminated last line is dropped and reported; a leading byte order
	mark is dropped.
	"""
	source = getattr(stdin, 'buffer', stdin)
	data = source.read()
	if isinstance(data, str):
		data = data.encode('utf-8', 'surrogateescape')
	lines = data.split(b'\n')
	tail = lines.pop()
	if tail:
		reporter.note('stdin-tail', None, 'ignored an unterminated last line on standard input: {!r}'.format(tail[:80]))
	if lines and lines[0].startswith(b'\xef\xbb\xbf'):
		lines[0] = lines[0][3:]
	return [_decodeLine(raw, reporter, 'line {}'.format(lineNo)) for lineNo, raw in enumerate(lines, 1)]


def _cliStdinBatch(args, delimiter, logger, stdin, keysOnly):
	"""Turn standard input into one batch for ``set STORE -`` / ``delete STORE -`` (spec §20.3).

	A spec store gets formatted records: a data line's fields are decoded
	(§13) and written again as ``set`` writes them, and a line whose first
	field matches the reserved pattern is a marker line, written as given. A
	loose file gets 3.39 rows. Comment lines and empty lines are skipped, and
	so is a line with an empty key (reported). ``keysOnly`` keeps the first
	field only: a tombstone, or a marker reset.
	"""
	spec = _isSpecPath(args.store)
	source = _Reporter('<stdin>', logger)
	try:
		rows = _parseRecordLines(_cliStdinLines(stdin, source), delimiter, spec, keysOnly, source)
	finally:
		source.flush()
	if not spec:
		return [cells for cells, marker in rows]
	return [_specFormatRecord(cells, delimiter, marker=marker) for cells, marker in rows]


def _parseRecordLines(texts, delimiter, spec, keysOnly, reporter):
	"""Parse record lines of a stream (spec §20.3) into ``[(cells, marker), ...]``.

	A line whose first field matches the reserved pattern is a marker line
	(``marker`` True, key kept as written). Other fields are decoded per §13
	for a spec store, by 3.39's rules otherwise. Comment lines and empty lines
	are skipped, and so is a line with an empty key (reported on
	``reporter``). ``keysOnly`` keeps the first field only.
	"""
	rows = []
	for lineNo, text in enumerate(texts, 1):
		fields = text.split(delimiter)
		if keysOnly:
			fields = fields[:1]
		first = fields[0]
		marker = bool(_MARKER_RE.match(first))
		if not marker and (first.startswith('#') or not text):
			continue
		if spec:
			cells = [first] + [_specDecodeField(field, delimiter) for field in fields[1:]] if marker else \
				[_specDecodeField(field, delimiter) for field in fields]
		else:
			cells = _unsanitize(fields, delimiter)
		if not cells[0]:
			reporter.note('empty-key', 'line {}'.format(lineNo), 'skipped a line with an empty key')
			continue
		rows.append((cells, marker))
	return rows


def _cliAppendBatch(args, delimiter, logger, batch):
	"""Append ``batch`` from ``_cliStdinBatch`` to STORE in one write, creating STORE if needed."""
	if not _isSpecPath(args.store):
		appendLinesTabularFile(args.store, batch, teeLogger=logger, header=args.header, createIfNotExist=True,
							   verbose=args.verbose, strict=args.strict, delimiter=delimiter)
		return
	reporter = _Reporter(args.store, logger)
	try:
		_specAppendRecords(args.store, batch, reporter, teeLogger=logger,
						   header=_formatHeader(args.header, delimiter=delimiter), createIfNotExist=True,
						   verbose=args.verbose, strict=args.strict, delimiter=delimiter)
	finally:
		reporter.flush()


def _cliSet(args, delimiter, logger, stdin, stdout):
	"""``set`` / ``append STORE KEY [VALUE ...]`` (spec §20.2): one record; a lone KEY is a tombstone.

	``set STORE -`` appends every record on standard input in one write (§20.3).
	"""
	if args.args == ['-']:
		_cliAppendBatch(args, delimiter, logger, _cliStdinBatch(args, delimiter, logger, stdin, keysOnly=False))
		return _cliWritten(args, logger)
	appendTabularFile(args.store, args.args, teeLogger=logger, header=args.header, createIfNotExist=True,
					  verbose=args.verbose, strict=args.strict, delimiter=delimiter)
	return _cliWritten(args, logger)


def _cliDelete(args, delimiter, logger, stdin, stdout):
	"""``delete STORE KEY [KEY ...]`` (spec §20.2): one tombstone per KEY, appended in one write.

	``delete STORE -`` deletes the first field of every line on standard input (§20.3).
	"""
	if args.args == ['-']:
		_cliAppendBatch(args, delimiter, logger, _cliStdinBatch(args, delimiter, logger, stdin, keysOnly=True))
		return _cliWritten(args, logger)
	appendLinesTabularFile(args.store, [[key] for key in args.args], teeLogger=logger, header=args.header,
						   createIfNotExist=True, verbose=args.verbose, strict=args.strict, delimiter=delimiter)
	return _cliWritten(args, logger)


def _cliClear(args, delimiter, logger, stdin, stdout):
	"""``clear STORE`` (spec §20.2); exit 1 when it was refused and nothing was written."""
	if not _isSpecPath(args.store):
		clearTabularFile(args.store, teeLogger=logger, header=args.header, verifyHeader=args.strict,
						 verbose=args.verbose, delimiter=delimiter)
		return 0
	if _specClearTabularFile(args.store, teeLogger=logger, header=args.header, verifyHeader=args.strict,
							 verbose=args.verbose, delimiter=delimiter):
		return 0
	logger.teelog('tsvz: {}: clear refused; nothing was written'.format(args.store), 'error')
	return 1


def _cliScrub(args, delimiter, logger, stdin, stdout):
	"""``scrub STORE`` (spec §20.2): compact a single-part store in place; exit 1 when refused."""
	if not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	if not _isSpecPath(args.store):
		scrubTabularFile(args.store, teeLogger=logger, verifyHeader=False, verbose=args.verbose,
						 strict=args.strict, delimiter=delimiter, defaults=args.defaults)
		return 0
	_, done = _specScrub(args.store, teeLogger=logger, verifyHeader=False, verbose=args.verbose,
						 strict=args.strict, delimiter=delimiter, defaults=args.defaults)
	if done:
		return 0
	logger.teelog('tsvz: {}: scrub refused; nothing was written'.format(args.store), 'error')
	return 1


def _cliVerify(args, delimiter, logger, stdin, stdout):
	"""``verify STORE`` (spec §20.2): one line per checksum mismatch; exit 4 when there is one."""
	if not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	if not _isSpecPath(args.store):
		return 0  # a loose file has no checksums (spec §20.2.4)
	reporter = _Reporter(args.store, logger)
	try:
		mismatches = _specVerify(args.store, delimiter, reporter)
		readable = not reporter.has('unreadable')
	finally:
		reporter.flush()
	rows = [[path, str(lineNo), algo, expected, computed] for path, lineNo, algo, expected, computed in mismatches]
	_cliEmitFields(rows, ['path', 'line', 'algorithm', 'expected', 'computed'], args, stdout)
	if not readable:
		return _cliUnreadable(args, logger)  # not every segment could be checked
	return 4 if rows else 0


def _cliParts(args, delimiter, logger, stdin, stdout):
	"""``parts STORE`` (spec §20.2): index, hexadecimal ordinal, path and flags of each part, in replay order."""
	if not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	reporter = _Reporter(args.store, logger)
	try:
		rows = _partRows(args.store, reporter)
	finally:
		reporter.flush()
	_cliEmitFields(rows, ['index', 'ordinal', 'path', 'flags'], args, stdout)
	return 0


def _cliRequestLine(args):
	"""The protocol request (spec §21.5) for a parsed command line: its arguments are literal."""
	return '\t'.join([args.operation] + [_serveEncode(arg) for arg in args.args])


def _cliEmitServed(args, delimiter, logger, stdout, response):
	"""Print a response (spec §21.6) as the operation prints on the files (§20.8); return the exit status."""
	lines, diagnostics, status, message = response
	for text in diagnostics:
		logger.teelog(text, 'warning')
	spec = _isSpecPath(args.store)
	operation = args.operation
	if operation in ('read', 'get', 'pop', 'popitem', 'setdefault'):
		_cliEmitRows([_serveDecodeLine(line) for line in lines], args, stdout, spec, delimiter)
	elif operation == 'keys':
		_cliEmitRows([[_specDecodeField(line, '\t')] for line in lines], args, stdout, spec, delimiter)
	elif operation == 'len':
		_cliWrite(stdout, ''.join(line + '\n' for line in lines))
	elif operation == 'verify':
		_cliEmitFields([_serveDecodeLine(line) for line in lines], ['path', 'line', 'algorithm', 'expected', 'computed'],
					   args, stdout)
	elif operation == 'parts':
		_cliEmitFields([_serveDecodeLine(line) for line in lines], ['index', 'ordinal', 'path', 'flags'], args, stdout)
	if message and status in (1, 2):
		logger.teelog('tsvz: {}: {}'.format(args.store, message), 'error')
	return status


def _cliEngineOp(args, delimiter, logger, stdin, stdout):
	"""``has``, ``len``, ``keys``, ``pop``, ``popitem``, ``setdefault`` (spec §20.2) on the files.

	They run through an in-process write-handler engine, so they behave as
	they do through a handler, except that ``pop``, ``popitem`` and
	``setdefault`` are not atomic against other writers.
	"""
	create = args.operation == 'setdefault'
	if not create and not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	store = _ServeStore(args.store, delimiter=delimiter, header=args.header, defaults=args.defaults,
						strict=args.strict, teeLogger=logger, verbose=args.verbose, create=create)
	try:
		response = _ServeLocal(store).request(_cliRequestLine(args))
	finally:
		store.close()
	status = _cliEmitServed(args, delimiter, logger, stdout, response)
	if store.writer.failures:
		return 1  # the write was not made; the writer reported why
	return status


def _cliServe(args, delimiter, logger, stdin, stdout):
	"""``serve STORE`` (spec §21.13): run a write handler for STORE in the foreground until it is stopped."""
	try:
		idle = float(args.idleTimeout) if args.idleTimeout else 0.0
		mode = int(args.mode, 8) if args.mode else None
		_serveGroup(args.group)
	except (ValueError, KeyError) as e:
		raise _CliUsageError('serve: bad --x-idle-timeout, --x-mode or --x-group ({})'.format(e))
	handle = _ServeHandle(args.store, logger=logger, group=args.group, mode=mode, tcp=args.tcp, idleTimeout=idle)
	try:
		handle.lock()
	except _ServeBusy as e:
		info = e.args[0]
		logger.teelog('tsvz: {}: already served by pid {} on host {}'.format(
			args.store, info.get('pid', '?'), info.get('host', '?')), 'error')
		return 1
	restore = _serveSignals()  # before the pointer file announces this handler
	try:
		store = _ServeStore(args.store, delimiter=delimiter, header=args.header, createDefaults=args.defaults,
							teeLogger=logger, verbose=args.verbose, create=True)
		handle.start(store)
		logger.teelog('tsvz: serving {} at {} (pid {})'.format(args.store, handle.address, os.getpid()))
		handle.serve()
	except (KeyboardInterrupt, _ServeSignal):
		pass
	finally:
		restore()
		handle.close()
	logger.teelog('tsvz: stopped serving {}'.format(args.store))
	return 0


def _cliStop(args, delimiter, logger, stdin, stdout):
	"""``stop STORE`` (spec §21.13): ask the handler of STORE to stop, and wait until it has."""
	info = _serveFind(args.store)
	if info is None:
		logger.teelog('tsvz: {}: no handler is running'.format(args.store), 'error')
		return 1
	try:
		connection = _ServeConnection(info)
	except _ServeUnreachable as e:
		logger.teelog('tsvz: {}: the handler does not answer ({})'.format(args.store, e), 'error')
		return 1
	try:
		status = connection.request('stop')[2]
	except _ServeLost as e:
		logger.teelog('tsvz: {}: {}'.format(args.store, e), 'error')
		return 1
	finally:
		connection.close()
	deadline = time.monotonic() + 10
	while time.monotonic() < deadline and (_serveFind(args.store) or {}).get('pid') == info.get('pid'):
		time.sleep(0.05)
	return 0 if status == 0 else 1


_CLI_READ_ONLY = ('read', 'get', 'has', 'len', 'keys', 'verify', 'parts')


def _cliHandler(args, delimiter, logger):
	"""A connection to STORE's handler when this command line may go through it (spec §20.8), else None.

	TSVZ routes a command line only when its result does not depend on
	options the handler does not share: no --x-direct, --x-header,
	--x-defaults or --x-strict, and for a loose file the handler's own
	delimiter. A pointer whose handler does not answer is reported and the
	files are used; another host's handler, or one this user may not
	connect to, is skipped quietly.
	"""
	if args.operation in ('serve', 'stop') or args.direct or args.header or args.defaults or args.strict:
		return None
	info = _serveFind(args.store)
	if info is None:
		return None
	if not _isSpecPath(args.store) and info.get('x-delimiter', delimiter) != delimiter:
		return None
	try:
		return _ServeConnection(info)
	except _ServeUnreachable as e:
		if not e.quiet:
			logger.teelog('TSVZ warning: {}: the handler in {} does not answer ({}); using the files directly'.format(
				args.store, _servePointerPath(args.store), e), 'warning')
		return None


def _cliRouted(connection, args, delimiter, logger, stdin, stdout):
	"""Run a command line through STORE's handler; print the response as the files would give it (§20.8)."""
	bulk = None
	if args.operation in ('set', 'append', 'delete') and args.args == ['-']:
		source = _Reporter('<stdin>', logger)
		try:
			rows = _parseRecordLines(_cliStdinLines(stdin, source), delimiter, _isSpecPath(args.store),
									 args.operation == 'delete', source)
		finally:
			source.flush()
		bulk = [_serveRecordLine(cells, marker) for cells, marker in rows]
	try:
		response = connection.request(_cliRequestLine(args), bulk)
	except _ServeLost as e:
		if args.operation in _CLI_READ_ONLY:
			logger.teelog('TSVZ warning: {}: lost the handler ({}); using the files directly'.format(args.store, e),
						  'warning')
			return _CLI_HANDLERS[args.operation](args, delimiter, logger, stdin, stdout)
		logger.teelog('tsvz: {}: lost the handler ({}); the {} may or may not have been applied'.format(
			args.store, e, args.operation), 'error')
		return 1
	return _cliEmitServed(args, delimiter, logger, stdout, response)


_CLI_HANDLERS = {'read': _cliRead, 'get': _cliGet, 'set': _cliSet, 'append': _cliSet, 'delete': _cliDelete,
				 'clear': _cliClear, 'scrub': _cliScrub, 'verify': _cliVerify, 'parts': _cliParts,
				 'has': _cliEngineOp, 'len': _cliEngineOp, 'keys': _cliEngineOp, 'pop': _cliEngineOp,
				 'popitem': _cliEngineOp, 'setdefault': _cliEngineOp, 'serve': _cliServe, 'stop': _cliStop}


def _cliMain(argv, stdin=None, stdout=None, stderr=None):
	"""Run one ``tsvz`` command line (spec §20) and return its exit status (§20.6).

	The streams default to ``sys.stdin`` / ``sys.stdout`` / ``sys.stderr``.
	Library messages printed to stdout for 3.39 compatibility go to stderr.
	"""
	stdin = sys.stdin if stdin is None else stdin
	stdout = sys.stdout if stdout is None else stdout
	stderr = sys.stderr if stderr is None else stderr
	try:
		args = _cliParseArgs(argv)
	except _CliUsageError as e:
		_cliWrite(stderr, 'tsvz: {}\n{}\n'.format(e, _CLI_USAGE))
		return 2
	args.args = _cliDecodeArgv(args.args)
	for name in ('header', 'defaults', 'delimiter'):
		if isinstance(getattr(args, name), str):
			setattr(args, name, _cliDecodeArgv([getattr(args, name)])[0])
	if args.help:
		_cliWrite(stdout, _CLI_HELP.format(version=version))
		return 0
	if args.version:
		prog = os.path.basename(sys.argv[0]) if sys.argv and sys.argv[0] else 'tsvz'
		_cliWrite(stdout, '{} {} @ {} by {}\n'.format(prog, version, COMMIT_DATE, author))
		return 0
	logger = _CliLogger(stderr, quiet=args.quiet)
	try:
		with contextlib.redirect_stdout(stderr):
			delimiter = _cliDelimiter(args, logger)
			args.header = _cliDecodeEscapes(args.header, '--x-header', logger) if args.header else ''
			if args.defaults:
				# 3.39's --defaults names the key column first; it is never used.
				values = _cliDecodeEscapes(args.defaults, '--x-defaults', logger).split(delimiter)
				args.defaults = [DEFAULTS_INDICATOR_KEY] + values[1:]
			else:
				args.defaults = []
			connection = _cliHandler(args, delimiter, logger)
			if connection is not None:
				try:
					return _cliRouted(connection, args, delimiter, logger, stdin, stdout)
				finally:
					connection.close()
			return _CLI_HANDLERS[args.operation](args, delimiter, logger, stdin, stdout)
	except _CliUsageError as e:
		_cliWrite(stderr, 'tsvz: {}\n{}\n'.format(e, _CLI_USAGE))
		return 2
	except _CliStoreMissing:
		logger.teelog('tsvz: {}: no such store'.format(args.store), 'error')
	except BrokenPipeError:
		# The reader went away (``tsvz read big | head -1``). Point stdout at
		# devnull so the interpreter's final flush cannot raise again.
		try:
			os.dup2(os.open(os.devnull, os.O_WRONLY), stdout.fileno())
		except Exception:
			pass
	except KeyboardInterrupt:
		return 130
	except Exception as e:
		logger.teelog('tsvz: {}: {}'.format(args.store, str(e) or type(e).__name__), 'error')
		if args.verbose:
			import traceback
			logger.teelog(traceback.format_exc().rstrip(), 'error')
	return 1


def _cliComplete():
	"""Shell completion through argcomplete when it is installed and driving this process (as in 3.39)."""
	if '_ARGCOMPLETE' not in os.environ:
		return
	try:
		import argparse
		import argcomplete
	except ImportError:
		return
	parser = argparse.ArgumentParser(prog='tsvz')
	parser.add_argument('operation', choices=_CLI_OPERATIONS)
	parser.add_argument('store')
	parser.add_argument('args', nargs='*')
	parser.add_argument('-V', '--version', action='store_true')
	parser.add_argument('--format', choices=('table', 'records'))
	parser.add_argument('-q', '--quiet', action='store_true')
	parser.add_argument('-v', '--verbose', action='store_true')
	parser.add_argument('-d', '--delimiter')
	parser.add_argument('--x-header')
	parser.add_argument('--x-defaults')
	parser.add_argument('--x-strict', action='store_true')
	parser.add_argument('--x-force', action='store_true')
	argcomplete.autocomplete(parser, always_complete_options='long')


def __main__():
	_cliComplete()
	sys.exit(_cliMain(sys.argv[1:]))
if __name__ == '__main__':
	__main__()
