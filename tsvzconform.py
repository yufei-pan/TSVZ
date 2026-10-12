#!/usr/bin/env python3
"""tsvzconform: test how closely a tsvz-spec-v1 implementation follows the spec.

The implementation is a black box, driven through the spec's command line
(§20) and, when it claims a handler, its handler socket (§21). The cases live
in conformance/cases/ beside this file, as JSON any language can load; each
names the § clauses it tests, and every failure is printed with the command
line that reruns it.

	python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature crc32,sha256,handler
	python3 tsvzconform.py --impl ./tsvz --class reader --only 13
"""
import argparse
import base64
import bz2
import collections
import concurrent.futures
import fnmatch
import json
import lzma
import os
import re
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zlib

version = '1.0'
__version__ = version

CASES_FORMAT = 1
CLASSES = ('reader', 'writer', 'cli', 'handler')
MODES = ('files', 'socket', 'forwarded')
FEATURES = ('bzip2', 'xz', 'zstd', 'crc32', 'crc32c', 'sha256', 'blake3', 'file-first', 'handler', 'scrub-multipart')
LEVELS = ('MUST', 'SHOULD')
OPERATIONS = ('read', 'get', 'set', 'append', 'delete', 'clear', 'scrub', 'verify', 'parts', 'has', 'len', 'keys',
	'pop', 'popitem', 'setdefault', 'serve', 'stop')
PASS, FAIL, WARN, SKIP, ERROR = 'pass', 'FAIL', 'warn', 'skip', 'error'
HERE = os.path.dirname(os.path.abspath(__file__))
CASES_DIR = os.path.join(HERE, 'conformance', 'cases')
PROBE_PREFIX = 'tsvzconform-probe'
HANDLER_WAIT = 10.0
STOP = threading.Event()


def eprint(*args):
	print(*args, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------

def _names(text, allowed, what, parser):
	names = [n for n in text.split(',') if n]
	unknown = [n for n in names if n not in allowed]
	if unknown or not names:
		parser.error('%s takes names from %s; got %r' % (what, ','.join(allowed), text))
	return tuple(n for n in allowed if n in names)


def parse_args(argv=None):
	"""Parse the runner's options; usage errors exit with status 2."""
	parser = argparse.ArgumentParser(prog='tsvzconform',
		description='Test a tsvz-spec-v1 implementation against the conformance cases.')
	parser.add_argument('--impl', metavar='COMMAND', help='the command that runs the implementation\'s tsvz tool')
	parser.add_argument('--class', dest='classes', metavar='CLASSES',
		help='comma-separated classes to run, from %s (default: all); naming handler also claims it' % ','.join(CLASSES))
	parser.add_argument('--only', action='append', default=[], metavar='PATTERNS',
		help='run only cases whose id matches: an id, a clause (9.2) or a glob (13.*); comma-separated, repeatable')
	parser.add_argument('--mode', dest='modes', default=','.join(MODES), metavar='MODES',
		help='comma-separated modes, from %s (default: all the claims allow)' % ','.join(MODES))
	parser.add_argument('--feature', action='append', default=[], metavar='NAMES',
		help='optional features the implementation claims, from %s, or all (comma-separated, repeatable)' % ','.join(FEATURES))
	parser.add_argument('--level', choices=('must', 'all'), default='all', help='must: run MUST cases only (default: all)')
	parser.add_argument('-j', '--jobs', type=int, default=os.cpu_count() or 1, metavar='N', help='cases run in parallel (default: CPUs)')
	parser.add_argument('-x', '--fail-fast', action='store_true', help='stop at the first FAIL')
	parser.add_argument('--timeout', type=float, default=10.0, metavar='SECONDS', help='limit for one step (default: 10)')
	parser.add_argument('--work-dir', metavar='DIR', help='where case directories go (default: a new temporary directory)')
	parser.add_argument('--json', metavar='PATH', help='also write every result to PATH')
	parser.add_argument('--list', action='store_true', help='list the selected cases and exit')
	parser.add_argument('-v', '--verbose', action='store_true', help='also list skipped cases')
	parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version)
	args = parser.parse_args(argv)
	if not args.impl and not args.list:
		parser.error('give --impl COMMAND (or --list)')
	if args.impl is not None and not shlex.split(args.impl):
		parser.error('--impl COMMAND is empty')
	classes = _names(args.classes, CLASSES, '--class', parser) if args.classes else CLASSES
	modes = _names(args.modes, MODES, '--mode', parser)
	features = set()
	for text in args.feature:
		for name in text.split(','):
			if name == 'all':
				features.update(FEATURES)
			elif name in FEATURES:
				features.add(name)
			elif name:
				parser.error('unknown feature %r; features are %s, or all' % (name, ','.join(FEATURES)))
	if args.classes and 'handler' in classes:
		features.add('handler')
	if args.jobs < 1:
		parser.error('-j must be at least 1')
	if args.timeout <= 0:
		parser.error('--timeout must be positive')
	only = tuple(p for text in args.only for p in text.split(',') if p)
	return argparse.Namespace(impl=args.impl, classes=classes, only=only, modes=modes, features=frozenset(features),
		level=args.level, jobs=args.jobs, fail_fast=args.fail_fast, timeout=args.timeout, work_dir=args.work_dir,
		json=args.json, list=args.list, verbose=args.verbose)


# ---------------------------------------------------------------------------
# Spec §13 codec, variants and file names
# ---------------------------------------------------------------------------

VARIANTS = {'.tsvz': '\t', '.csvz': ',', '.nsvz': '\0', '.psvz': '|'}
LOOSE_VARIANTS = {'.tsv': '\t', '.csv': ',', '.nsv': '\0', '.psv': '|'}
CODECS = {'.gz': 'gzip', '.gzip': 'gzip', '.bz2': 'bzip2', '.bzip2': 'bzip2', '.xz': 'xz', '.lzma': 'xz',
	'.zst': 'zstd', '.zstd': 'zstd'}
RESERVED_RE = re.compile(r'#_[A-Za-z0-9_-]+_#\Z')
_TOKEN_RE = re.compile(r'<(sep|LF|CR|lt|#)>')
_TOKENS = {'LF': '\n', 'CR': '\r', 'lt': '<', '#': '#'}
_BARE_LT_RE = re.compile(r'<(?!(?:sep|LF|CR|lt|#)>)')


def decode_field(text, delimiter):
	"""Decode §13 tokens (names are case-sensitive); any other <...> passes through."""
	if '<' not in text:
		return text
	return _TOKEN_RE.sub(lambda m: delimiter if m.group(1) == 'sep' else _TOKENS[m.group(1)], text)


def encode_field(value, delimiter, is_key=False):
	"""Encode one field per §13.3; a key also gets <#> for a leading '#'."""
	value = value.replace('<', '<lt>').replace(delimiter, '<sep>').replace('\n', '<LF>').replace('\r', '<CR>')
	if is_key and value.startswith('#'):
		value = '<#>' + value[1:]
	return value


def is_marker_key(text):
	return RESERVED_RE.match(text) is not None


def trim(cells):
	"""Drop trailing empty cells (§3.6 lets a reader pad or trim)."""
	cells = list(cells)
	while cells and cells[-1] == '':
		cells.pop()
	return cells


def trim_row(row):
	"""A row's comparable form: its key, then its cells without trailing empties."""
	return [row[0]] + trim(row[1:]) if row else []


def split_codec(path):
	"""(path without its compression suffix, codec name or None) (§16.2)."""
	root, ext = os.path.splitext(path)
	codec = CODECS.get(ext.lower())
	return (root, codec) if codec else (path, None)


def delimiter_of(path):
	"""The field delimiter for a store or part path, from its variant extension (§5.1, §17.2)."""
	base = split_codec(path)[0]
	if base.endswith('.rotated'):
		base = base[:-len('.rotated')]
	for _ in range(2):
		ext = os.path.splitext(base)[1].lower()
		if ext in VARIANTS:
			return VARIANTS[ext]
		if ext in LOOSE_VARIANTS:
			return LOOSE_VARIANTS[ext]
		base = os.path.splitext(base)[0]
	raise ValueError('%s has no TSVZ variant extension' % path)


def is_loose(path):
	try:
		return delimiter_of(path) and os.path.splitext(split_codec(path)[0])[1].lower() in LOOSE_VARIANTS
	except ValueError:
		return False


def show(value):
	"""A value as JSON, so tabs, CRs and NULs are visible."""
	if isinstance(value, bytes):
		value = value.decode('utf-8', 'replace')
	return json.dumps(value, ensure_ascii=False)


# ---------------------------------------------------------------------------
# The oracle: the runner's own reader (§4-§17)
# ---------------------------------------------------------------------------

class CodecUnavailable(Exception):
	"""The runner's Python cannot decompress this codec (zstd before 3.14)."""


_BOOL_VALUES = {'true': True, 'yes': True, 'on': True, '1': True, 'false': False, 'no': False, 'off': False, '0': False}
_BOOL_MARKERS = {'#_strip_trailing_whites_#': ('strip', True), '#_fill_empty_with_default_#': ('fill_empty', False),
	'#_return_defaults_when_missing_#': ('return_missing', True)}


class Replay(object):
	"""Replays committed records (§7) and resolves rows (§14).

	Each live key keeps its decoded cells and the marker state in force at its
	row, so absent and present-empty cells resolve against that state.
	"""

	def __init__(self, delimiter):
		self.delimiter = delimiter
		self.version = 1
		self.defaults = ()
		self.strip = True
		self.fill_empty = False
		self.return_missing = True
		self.entries = collections.OrderedDict()
		self._bound = None

	def feed_text(self, line):
		"""Apply one committed record, given without its '\\n' (§7.1-§7.10)."""
		if line.endswith('\r'):
			line = line[:-1]
		d = self.delimiter
		fields = line.split(d)
		first = fields[0]
		if first.startswith('#'):
			if is_marker_key(first):
				self._marker(first.lower(), fields[1:])
			return
		strip = self.strip
		key = decode_field(first.rstrip(' \t') if strip else first, d)
		if key == '':
			return
		if len(fields) == 1:
			self.entries.pop(key, None)
			return
		cells = [decode_field(f.rstrip(' \t') if strip else f, d) for f in fields[1:]]
		if self._bound is None:
			self._bound = (self.defaults, self.fill_empty)
		self.entries[key] = (cells, self._bound)

	def _marker(self, key, values):
		value = decode_field(values[0], self.delimiter) if values else ''
		if key == '#_version_#':
			if value == '':
				self.version = 1
			elif re.match(r'[0-9]+\Z', value) and int(value) >= 1:
				self.version = int(value)
			return
		if key == '#_defaults_#':
			self.defaults = tuple(decode_field(v, self.delimiter) for v in values)
		elif key in _BOOL_MARKERS:
			attr, default = _BOOL_MARKERS[key]
			lowered = value.lower() if all(ord(c) < 128 for c in value) else value
			if value == '':
				setattr(self, attr, default)
			elif lowered in _BOOL_VALUES:
				setattr(self, attr, _BOOL_VALUES[lowered])
			else:
				return
			if attr == 'return_missing':
				return
		else:
			return
		self._bound = None

	def feed_part(self, data):
		"""Apply one part's bytes: BOM removed (§4.7), bytes after the last '\\n' dropped (§4.3), U+FFFD for bad UTF-8 (§4.8)."""
		if data.startswith(b'\xef\xbb\xbf'):
			data = data[3:]
		end = data.rfind(b'\n')
		if end < 0:
			return
		for line in data[:end].decode('utf-8', 'replace').split('\n'):
			self.feed_text(line)

	def resolve(self, key):
		cells, (defaults, fill_empty) = self.entries[key]
		out = [defaults[j] if c == '' and fill_empty and j < len(defaults) else c for j, c in enumerate(cells)]
		out.extend(defaults[len(out):])
		return trim(out)

	def rows(self):
		return [[key] + self.resolve(key) for key in self.entries]

	def missing(self):
		"""What a read of a missing key returns (§14.5): the current defaults, or None for not-found."""
		return trim(self.defaults) if self.return_missing else None


_PART_RE = re.compile(r'([0-9A-Fa-f]+)(\.rotated)?(\.[A-Za-z0-9]+)?\Z')


def store_parts(store):
	"""The parts of a store in replay order (§17): the unnumbered file, then hex ordinals ascending; .rotated skipped."""
	base = split_codec(store)[0]
	directory = os.path.dirname(base) or '.'
	prefix = os.path.basename(base) + '.'
	numbered = []
	try:
		names = os.listdir(directory)
	except OSError:
		names = []
	for name in names:
		if not name.startswith(prefix):
			continue
		match = _PART_RE.match(name[len(prefix):])
		if not match or match.group(2) or (match.group(3) and match.group(3).lower() not in CODECS):
			continue
		numbered.append((int(match.group(1), 16), name))
	parts = [store] if os.path.isfile(store) else []
	return parts + [os.path.join(directory, name) for _, name in sorted(numbered)]


def _decompressor(codec):
	if codec == 'gzip':
		return zlib.decompressobj(31)
	if codec == 'bzip2':
		return bz2.BZ2Decompressor()
	if codec == 'xz':
		return lzma.LZMADecompressor()
	try:
		from compression import zstd
	except ImportError:
		raise CodecUnavailable('zstd')
	return zstd.ZstdDecompressor()


def decompress(data, codec):
	"""Every member of a compressed part as one stream; a member cut short yields what it decoded (§16.6)."""
	out = []
	while data:
		decompressor = _decompressor(codec)
		try:
			out.append(decompressor.decompress(data))
		except (OSError, EOFError, ValueError, zlib.error, lzma.LZMAError):
			break
		if not decompressor.eof:
			break
		data = decompressor.unused_data
	return b''.join(out)


def read_part(path):
	"""A part's bytes, decompressed by its suffix (§16)."""
	with open(path, 'rb') as f:
		data = f.read()
	codec = split_codec(path)[1]
	return decompress(data, codec) if codec else data


def replay_store(store):
	replay = Replay(delimiter_of(store))
	for part in store_parts(store):
		replay.feed_part(read_part(part))
	return replay


Observed = collections.namedtuple('Observed', 'rows missing probe')
PROBE_LINES = (PROBE_PREFIX + '-1{d}v  {d}{d}', PROBE_PREFIX + '-2 {d}w')


def observe(store):
	"""The observable state of a store (design section 3.2): rows, the missing-key read, and the probe."""
	replay = replay_store(store)
	rows = replay.rows()
	missing = replay.missing()
	for line in PROBE_LINES:
		replay.feed_text(line.format(d=replay.delimiter))
	probe = [row for row in replay.rows() if row[0].startswith(PROBE_PREFIX)]
	return Observed(rows, missing, probe)
