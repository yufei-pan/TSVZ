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


# ---------------------------------------------------------------------------
# The corpus: case files and procedural checks
# ---------------------------------------------------------------------------

class CorpusError(Exception):
	pass


class Skip(Exception):
	"""Raised by a case that the environment cannot run; its message is the reason."""


_ID_RE = re.compile(r'(\d+(?:\.\d+)*)-[a-z0-9]+(?:-[a-z0-9]+)*\Z')
_CLAUSE_RE = re.compile(r'\d+(?:\.\d+)*\Z')
_CASE_KEYS = {'id', 'spec', 'level', 'requires', 'note', 'store', 'files', 'modes', 'expect', 'steps', 'after'}
_EXPECT_KEYS = {'read', 'keys', 'len', 'get', 'verify'}
_STEP_KEYS = {
	'op': {'stdin', 'body', 'exit', 'stdout', 'stderr'},
	'argv': {'stdin', 'exit', 'stdout', 'stderr'},
	'request': {'options', 'body', 'exit', 'stdout'},
	'raw': {'exit', 'stdout'},
	'append': {'path'},
	'write': {'path'},
	'check': set(),
}
_AFTER_KEYS = {'rows', 'missing', 'fidelity', 'exists', 'absent', 'bytes', 'serve_exit'}
BYTE_RULES = ('encoded', 'lf-terminated', 'appended', 'last-part-only', 'unchanged', 'same-file', 'header-kept', 'snapshot-layout')


class Case(object):
	"""One conformance case, from a case file or a procedural check."""

	def __init__(self, id, cls, spec, level, note, requires=(), store='s.tsvz', files=None, modes=None,
			expect=None, steps=None, after=None, source=None, func=None):
		self.id = id
		self.cls = cls
		self.spec = list(spec)
		self.level = level
		self.note = note
		self.requires = tuple(requires)
		self.store = store
		self.files = files or {}
		self.modes = tuple(modes) if modes else None
		self.expect = expect
		self.steps = steps
		self.after = after
		self.source = source
		self.func = func


CHECKS = []


def procedural(id, cls, spec, level, note, requires=()):
	"""Register a check written in Python (design section 1.8); it gets a CheckContext and returns problems."""
	def wrap(func):
		CHECKS.append(Case(id, cls, spec, level, note, requires=requires, source=__file__, func=func))
		return func
	return wrap


def _fail(where, message):
	raise CorpusError('%s: %s' % (where, message))


def _content_ok(value):
	return isinstance(value, str) or (isinstance(value, dict) and set(value) == {'b64'} and isinstance(value['b64'], str))


def _check_matcher(matcher, where):
	if matcher in ('empty', 'nonempty'):
		return
	if not isinstance(matcher, dict) or len(set(matcher) & {'rows', 'lines', 'text'}) != 1 or set(matcher) - {'rows', 'lines', 'text'}:
		_fail(where, 'stdout must be "empty", "nonempty", or one of {"rows": ...}, {"lines": ...}, {"text": ...}')
	for name in ('rows', 'lines'):
		if name in matcher and not (isinstance(matcher[name], list) and all(
				isinstance(r, list) and all(isinstance(c, str) for c in r) for r in matcher[name])):
			_fail(where, '%s must be a list of lists of strings' % name)
	if 'text' in matcher and not isinstance(matcher['text'], str):
		_fail(where, 'text must be a string')


def _check_after(block, where):
	if not isinstance(block, dict) or set(block) - _AFTER_KEYS:
		_fail(where, 'an after/check block takes %s' % ', '.join(sorted(_AFTER_KEYS)))
	if 'rows' in block:
		_check_matcher({'rows': block['rows']}, where)
	if 'missing' in block and not (block['missing'] is None or (isinstance(block['missing'], list) and all(isinstance(c, str) for c in block['missing']))):
		_fail(where, 'missing must be a list of strings or null')
	if block.get('fidelity', 'full') not in ('full', 'markers'):
		_fail(where, 'fidelity must be "full" or "markers"')
	for name in ('exists', 'absent'):
		if name in block and not (isinstance(block[name], list) and all(isinstance(p, str) for p in block[name])):
			_fail(where, '%s must be a list of paths' % name)
	for rule in block.get('bytes', []):
		if rule not in BYTE_RULES:
			_fail(where, 'unknown byte rule %r' % rule)
	if 'serve_exit' in block and not isinstance(block['serve_exit'], int):
		_fail(where, 'serve_exit must be an integer')


def action_of(step):
	actions = [a for a in _STEP_KEYS if a in step]
	return actions[0] if len(actions) == 1 else None


def _check_step(step, where):
	action = action_of(step) if isinstance(step, dict) else None
	if action is None:
		_fail(where, 'a step has exactly one of %s' % ', '.join(_STEP_KEYS))
	extra = set(step) - {action} - _STEP_KEYS[action]
	if extra:
		_fail(where, '%s step does not take %s' % (action, ', '.join(sorted(extra))))
	value = step[action]
	if action in ('op', 'argv', 'request'):
		if not (isinstance(value, list) and (value or action == 'argv') and all(isinstance(a, str) for a in value)):
			_fail(where, '%s must be a list of strings, not empty' % action if action != 'argv' else 'argv must be a list of strings')
		if action != 'argv' and value[0] not in OPERATIONS:
			_fail(where, 'unknown operation %r' % value[0])
		if action == 'op' and value[0] in ('serve', 'stop'):
			_fail(where, 'serve and stop are not op steps; use argv or request')
	elif action == 'raw':
		if not isinstance(value, str):
			_fail(where, 'raw must be a string')
	elif action in ('append', 'write'):
		if not _content_ok(value) or not isinstance(step.get('path'), str):
			_fail(where, '%s needs content and a path' % action)
		if os.path.isabs(step['path']) or '..' in step['path'].split('/'):
			_fail(where, '%s takes a relative path inside the case directory' % action)
	elif action == 'check':
		_check_after(value, where)
	if 'stdin' in step and not _content_ok(step['stdin']):
		_fail(where, 'stdin must be a string or {"b64": ...}')
	if 'body' in step and not (isinstance(step['body'], list) and all(isinstance(l, str) for l in step['body'])):
		_fail(where, 'body must be a list of protocol lines')
	if 'exit' in step and not isinstance(step['exit'], int):
		_fail(where, 'exit must be an integer')
	if 'stdout' in step:
		_check_matcher(step['stdout'], where)
	if step.get('stderr', 'empty') not in ('empty', 'nonempty'):
		_fail(where, 'stderr must be "empty" or "nonempty"')
	if 'options' in step and not (isinstance(step['options'], list) and all(isinstance(o, str) and o.startswith('--') for o in step['options'])):
		_fail(where, 'options must be a list of --options')


def _check_expect(expect, where):
	if not isinstance(expect, dict) or not expect or set(expect) - _EXPECT_KEYS:
		_fail(where, 'expect takes %s' % ', '.join(sorted(_EXPECT_KEYS)))
	if 'read' in expect:
		read = expect['read']
		if isinstance(read, dict):
			if set(read) - {'rows', 'exit'} or not isinstance(read.get('exit', 0), int):
				_fail(where, 'read takes rows and exit')
			if read.get('rows') is not None:
				_check_matcher({'rows': read['rows']}, where)
		else:
			_check_matcher({'rows': read}, where)
	if 'keys' in expect and not (isinstance(expect['keys'], list) and all(isinstance(k, str) for k in expect['keys'])):
		_fail(where, 'keys must be a list of strings')
	if 'len' in expect and not isinstance(expect['len'], int):
		_fail(where, 'len must be an integer')
	for query in expect.get('get', []):
		if not isinstance(query, dict) or set(query) - {'args', 'rows', 'exit'} or not query.get('args'):
			_fail(where, 'each get takes args, rows and exit')
		_check_matcher({'rows': query.get('rows', [])}, where)
	if 'verify' in expect:
		verify = expect['verify']
		if not isinstance(verify, dict) or set(verify) - {'lines', 'exit'}:
			_fail(where, 'verify takes lines and exit')
		_check_matcher({'lines': verify.get('lines', [])}, where)


def validate_case(raw, cls, where):
	"""Check one case object against the format (design section 1.3) and return a Case."""
	if not isinstance(raw, dict):
		_fail(where, 'a case is a JSON object')
	where = '%s: %s' % (where, raw.get('id', '?'))
	extra = set(raw) - _CASE_KEYS
	if extra:
		_fail(where, 'unknown fields %s' % ', '.join(sorted(extra)))
	for name in ('id', 'spec', 'level', 'note'):
		if name not in raw:
			_fail(where, 'missing %s' % name)
	match = _ID_RE.match(raw['id']) if isinstance(raw['id'], str) else None
	if not match:
		_fail(where, 'id must be CLAUSE-slug, e.g. 9.2-lone-key-deletes')
	spec = raw['spec']
	if not (isinstance(spec, list) and spec and all(isinstance(c, str) and _CLAUSE_RE.match(c) for c in spec)):
		_fail(where, 'spec must be a non-empty list of clauses like "9.2"')
	if match.group(1) not in spec:
		_fail(where, 'the clause of the id (%s) must be listed in spec' % match.group(1))
	if raw['level'] not in LEVELS:
		_fail(where, 'level must be MUST or SHOULD')
	if not (isinstance(raw['note'], str) and raw['note'].strip()):
		_fail(where, 'note must be a sentence')
	requires = raw.get('requires', [])
	if not isinstance(requires, list) or any(not isinstance(r, str) or r.lstrip('!') not in FEATURES for r in requires):
		_fail(where, 'requires names features from %s, optionally with !' % ','.join(FEATURES))
	store = raw.get('store', 's.tsvz')
	if not isinstance(store, str) or os.path.isabs(store) or '..' in store.split('/'):
		_fail(where, 'store must be a relative path')
	try:
		delimiter_of(store)
	except ValueError:
		_fail(where, 'store must have a TSVZ variant extension')
	files = raw.get('files', {})
	if not isinstance(files, dict) or any(os.path.isabs(p) or '..' in p.split('/') or not _content_ok(c) for p, c in files.items()):
		_fail(where, 'files maps relative paths to a string or {"b64": ...}')
	modes = raw.get('modes')
	if modes is not None and not (isinstance(modes, list) and modes and all(m in MODES for m in modes)):
		_fail(where, 'modes must list modes from %s' % ','.join(MODES))
	if cls == 'reader':
		if 'steps' in raw or 'expect' not in raw:
			_fail(where, 'a reader case has expect and no steps')
		_check_expect(raw['expect'], where)
	else:
		if 'expect' in raw or not isinstance(raw.get('steps'), list) or not raw['steps']:
			_fail(where, 'a %s case has steps and no expect' % cls)
		for step in raw['steps']:
			_check_step(step, where)
		kinds = {action_of(s) for s in raw['steps']}
		if kinds & {'request', 'raw'} and 'argv' in kinds:
			_fail(where, 'a case cannot mix argv steps with request or raw steps')
	if 'after' in raw:
		_check_after(raw['after'], where)
	if PROBE_PREFIX in json.dumps(raw):
		_fail(where, 'cases must not use keys starting %s' % PROBE_PREFIX)
	return Case(raw['id'], cls, spec, raw['level'], raw['note'], requires, store, files, modes,
		raw.get('expect'), raw.get('steps'), raw.get('after'), where.split(': ')[0])


def load_corpus(directory=None):
	"""Every case of every case file under `directory` (default CASES_DIR), in class order then file order; raises CorpusError."""
	directory = directory or CASES_DIR
	cases = []
	for cls in CLASSES:
		folder = os.path.join(directory, cls)
		if not os.path.isdir(folder):
			continue
		for name in sorted(os.listdir(folder)):
			if not name.endswith('.json'):
				continue
			path = os.path.join(folder, name)
			where = os.path.relpath(path, HERE)
			try:
				with open(path, encoding='utf-8') as f:
					data = json.load(f)
			except (OSError, ValueError) as e:
				raise CorpusError('%s: %s' % (where, e))
			if not isinstance(data, dict) or set(data) != {'format', 'class', 'cases'}:
				_fail(where, 'a case file is {"format": 1, "class": ..., "cases": [...]}')
			if data['format'] != CASES_FORMAT:
				_fail(where, 'format %r is not %d' % (data['format'], CASES_FORMAT))
			if data['class'] != cls:
				_fail(where, 'class %r does not match its folder %s' % (data['class'], cls))
			if not isinstance(data['cases'], list):
				_fail(where, 'cases must be a list')
			cases.extend(validate_case(raw, cls, where) for raw in data['cases'])
	return cases


def all_cases(directory=None):
	"""The corpus plus the procedural checks; ids must be unique across both."""
	cases = load_corpus(directory) + list(CHECKS)
	seen = {}
	for case in cases:
		if case.id in seen:
			raise CorpusError('%s: id %s is also used in %s' % (case.source, case.id, seen[case.id]))
		seen[case.id] = case.source
	return cases


def case_modes(case):
	"""The modes a case can run in (design section 1.6.1), before claims are applied."""
	if case.func:
		return ('files',)
	steps = case.steps or []
	kinds = {action_of(s) for s in steps}
	if kinds & {'request', 'raw'}:
		allowed = ['socket']
	elif 'argv' in kinds or 'check' in kinds or is_loose(case.store):
		allowed = ['files']
	else:
		allowed = list(MODES)
		if not case.store.endswith('.tsvz') and any('stdin' in s and 'body' not in s for s in steps):
			allowed.remove('socket')
	if case.cls == 'handler' and 'files' in allowed and len(allowed) > 1:
		allowed.remove('files')
	if case.modes:
		allowed = [m for m in allowed if m in case.modes]
	return tuple(allowed)


# ---------------------------------------------------------------------------
# Matching output and files
# ---------------------------------------------------------------------------

def output_lines(data):
	"""(lines, terminated) of a command's stdout."""
	text = data.decode('utf-8', 'replace')
	if not text:
		return [], True
	lines = text.split('\n')
	terminated = lines[-1] == ''
	if terminated:
		lines.pop()
	return lines, terminated


def compare_lists(got, expected, what):
	"""None when equal, else a sentence naming the first difference."""
	for i, (g, e) in enumerate(zip(got, expected), 1):
		if g != e:
			return '%s %d: expected %s, got %s' % (what, i, show(e), show(g))
	if len(got) < len(expected):
		return '%s %d missing: expected %s (got %d %ss, expected %d)' % (what, len(got) + 1, show(expected[len(got)]), len(got), what, len(expected))
	if len(got) > len(expected):
		return 'extra %s %d: %s (got %d %ss, expected %d)' % (what, len(expected) + 1, show(got[len(expected)]), len(got), what, len(expected))
	return None


def match_stdout(matcher, data, delimiter, live=False, line_delimiter=None):
	"""None when stdout matches, else the difference (design section 1.6.2)."""
	if matcher == 'empty':
		return None if not data else 'stdout should be empty; got %s' % show(data)
	if matcher == 'nonempty':
		return None if data else 'stdout should not be empty'
	if 'text' in matcher:
		return None if data == matcher['text'].encode('utf-8') else 'stdout: expected %s, got %s' % (show(matcher['text']), show(data))
	lines, terminated = output_lines(data)
	if not terminated:
		return 'stdout does not end with a newline: %s' % show(data)
	if 'rows' in matcher:
		got = []
		for i, line in enumerate(lines, 1):
			fields = line.split(delimiter)
			if live and len(fields) == 1:
				return 'row %d %s has no delimiter, so it would read back as a tombstone (§20.3.2)' % (i, show(line))
			got.append(trim_row([decode_field(f, delimiter) for f in fields]))
		return compare_lists(got, [trim_row(r) for r in matcher['rows']], 'row')
	split = line_delimiter or delimiter
	got = [[decode_field(f, split) for f in line.split(split)] for line in lines]
	return compare_lists(got, matcher['lines'], 'line')


PartState = collections.namedtuple('PartState', 'raw data ino mode')


def snapshot_store(store):
	"""Every part's raw bytes, decompressed bytes (None if the runner cannot decompress), inode and mode."""
	parts = {}
	for path in store_parts(store):
		info = os.stat(path)
		with open(path, 'rb') as f:
			raw = f.read()
		try:
			data = read_part(path)
		except CodecUnavailable:
			data = None
		parts[path] = PartState(raw, data, info.st_ino, stat.S_IMODE(info.st_mode))
	return parts


def first_line(parts):
	"""The first committed line of the first part, decoded, or None."""
	for state in parts.values():
		data = state.data or b''
		if data.startswith(b'\xef\xbb\xbf'):
			data = data[3:]
		end = data.find(b'\n')
		return data[:end].decode('utf-8', 'replace').rstrip('\r') if end >= 0 else None
	return None


def written_chunks(before, now):
	"""What was written since `before`: appended bytes, or the whole part when it was rewritten or is new."""
	chunks = []
	for path, state in now.items():
		old = before.get(path)
		data = state.data or b''
		if old and old.data is not None and state.ino == old.ino and data.startswith(old.data):
			data = data[len(old.data):]
		if data:
			chunks.append((path, data))
	return chunks


def _is_marker_line(line, delimiter):
	first = line.split(delimiter)[0]
	return first.startswith('#') and is_marker_key(first)


def snapshot_layout(store, before):
	"""Problems with a scrubbed store's layout (§19.2.3, decisions 9 and 12)."""
	parts = store_parts(store)
	if len(parts) != 1:
		return ['the scrubbed store has %d parts, expected 1' % len(parts)]
	d = delimiter_of(store)
	data = read_part(parts[0])
	if data.startswith(b'\xef\xbb\xbf'):
		data = data[3:]
	lines = data[:data.rfind(b'\n') + 1].decode('utf-8', 'replace').split('\n')[:-1]
	lines = [line[:-1] if line.endswith('\r') else line for line in lines]
	i = 0
	if lines and lines[0].startswith('#') and not _is_marker_line(lines[0], d):
		if lines[0] != first_line(before):
			return ['line 1 is a comment but not the store\'s header comment: %s' % show(lines[0])]
		i = 1
	block = []
	while i < len(lines) and _is_marker_line(lines[i], d):
		block.append(lines[i].split(d)[0].lower())
		i += 1
	if not block or block[0] != '#_version_#':
		return ['line %d should be #_version_#, the first marker of a snapshot; got %s' % (i - len(block) + 1, show(lines[i - len(block)] if i - len(block) < len(lines) else ''))]
	if '#_defaults_#' in block and block[-1] != '#_defaults_#':
		return ['#_defaults_# should be the last marker before the rows; the markers are %s' % show(block)]
	keys = []
	while i < len(lines) and not lines[i].startswith('#'):
		if d not in lines[i]:
			return ['line %d is a tombstone or empty: %s' % (i + 1, show(lines[i]))]
		keys.append(lines[i].split(d)[0])
		i += 1
	if len(set(keys)) != len(keys):
		return ['a key is written twice in the rows']
	live = len(replay_store(store).entries)
	if len(keys) != live:
		return ['%d data rows for %d live keys' % (len(keys), live)]
	while i < len(lines):
		if not _is_marker_line(lines[i], d):
			return ['line %d after the rows is not a marker: %s' % (i + 1, show(lines[i]))]
		i += 1
	return []


def byte_rule(rule, store, before, now):
	"""Problems with one byte rule (design section 1.7)."""
	problems = []
	if rule == 'encoded':
		for path, chunk in written_chunks(before, now):
			if chunk.startswith(b'\xef\xbb\xbf') and chunk == (now[path].data or b''):
				problems.append('%s: starts with a byte-order mark (§4.1)' % os.path.basename(path))
			text = chunk.decode('utf-8', 'replace')
			bare = _BARE_LT_RE.search(text)
			if bare:
				problems.append('%s: a "<" that begins no token: %s' % (os.path.basename(path), show(text[max(0, bare.start() - 10):bare.start() + 10])))
			if '\r' in text:
				at = text.index('\r')
				problems.append('%s: a raw carriage return: %s' % (os.path.basename(path), show(text[max(0, at - 10):at + 10])))
	elif rule == 'lf-terminated':
		for path, chunk in written_chunks(before, now):
			if not chunk.endswith(b'\n'):
				problems.append('%s: the written bytes do not end with a newline: %s' % (os.path.basename(path), show(chunk[-20:])))
	elif rule == 'appended':
		for path, old in before.items():
			new = now.get(path)
			if new is None or not new.raw.startswith(old.raw):
				problems.append('%s: earlier bytes were changed, not appended to' % os.path.basename(path))
	elif rule == 'last-part-only':
		paths = list(before)
		if list(now) != paths:
			problems.append('the parts changed: %s became %s' % (show([os.path.basename(p) for p in paths]), show([os.path.basename(p) for p in now])))
		else:
			for path in paths[:-1]:
				if now[path].raw != before[path].raw:
					problems.append('%s changed; only the last part may grow (§17.7)' % os.path.basename(path))
			if paths and not now[paths[-1]].raw.startswith(before[paths[-1]].raw):
				problems.append('%s: earlier bytes were changed, not appended to' % os.path.basename(paths[-1]))
	elif rule == 'unchanged':
		if sorted(before) != sorted(now):
			problems.append('the parts changed: %s became %s' % (show([os.path.basename(p) for p in before]), show([os.path.basename(p) for p in now])))
		for path, old in before.items():
			if path in now and now[path].raw != old.raw:
				problems.append('%s was modified' % os.path.basename(path))
	elif rule == 'same-file':
		for path, old in before.items():
			new = now.get(path)
			if new is None:
				problems.append('%s is gone' % os.path.basename(path))
			elif new.ino != old.ino:
				problems.append('%s was replaced by another file (inode %d, was %d; §18.7)' % (os.path.basename(path), new.ino, old.ino))
			elif new.mode != old.mode:
				problems.append('%s mode is %o, was %o' % (os.path.basename(path), new.mode, old.mode))
	elif rule == 'header-kept':
		header = first_line(before)
		if header is None or not header.startswith('#'):
			problems.append('the case has no header comment to keep')
		elif first_line(now) != header:
			problems.append('the header comment %s is not the first line any more; it is %s' % (show(header), show(first_line(now))))
	elif rule == 'snapshot-layout':
		problems.extend(snapshot_layout(store, before))
	return problems


Before = collections.namedtuple('Before', 'parts observed')


def check_after(block, store, case_dir, before, serve_exit=None):
	"""Problems with an after or check block (design section 1.7)."""
	problems = []
	if set(block) & {'rows', 'missing', 'fidelity'}:
		try:
			now = observe(store)
		except CodecUnavailable as e:
			raise Skip('the runner cannot read %s parts' % e)
		if 'rows' in block:
			diff = compare_lists(now.rows, [trim_row(r) for r in block['rows']], 'row')
			if diff:
				problems.append('the store reads back wrong: ' + diff)
		if 'missing' in block:
			want = None if block['missing'] is None else trim(block['missing'])
			if now.missing != want:
				problems.append('a missing key reads %s, expected %s' % (show(now.missing) if now.missing is not None else 'not-found', show(want) if want is not None else 'not-found'))
		if 'fidelity' in block:
			was = before.observed
			if block['fidelity'] == 'full' and now.rows != was.rows:
				problems.append('the rows changed: ' + compare_lists(now.rows, was.rows, 'row'))
			if block['fidelity'] == 'markers' and now.rows:
				problems.append('rows are left: %s' % show(now.rows[0]))
			if now.missing != was.missing:
				problems.append('a missing key now reads %s, was %s' % (show(now.missing), show(was.missing)))
			if now.probe != was.probe:
				problems.append('rows appended now read differently, so the final marker state changed: %s' % compare_lists(now.probe, was.probe, 'probe row'))
	for rel in block.get('exists', []):
		if not os.path.lexists(os.path.join(case_dir, rel)):
			problems.append('%s does not exist' % rel)
	for rel in block.get('absent', []):
		if os.path.lexists(os.path.join(case_dir, rel)):
			problems.append('%s exists' % rel)
	if block.get('bytes'):
		now_parts = snapshot_store(store)
		for rule in block['bytes']:
			problems.extend(byte_rule(rule, store, before.parts, now_parts))
	if 'serve_exit' in block and serve_exit != block['serve_exit']:
		problems.append('serve exited with status %s, expected %d' % (serve_exit, block['serve_exit']))
	return problems


# ---------------------------------------------------------------------------
# Processes
# ---------------------------------------------------------------------------

LIVE_GROUPS = set()
_REAP_LOCK = threading.Lock()


def kill_group(pid, sig=signal.SIGKILL):
	try:
		os.killpg(pid, sig)
	except (ProcessLookupError, PermissionError):
		pass


def exit_code(status):
	if os.WIFEXITED(status):
		return os.WEXITSTATUS(status)
	if os.WIFSIGNALED(status):
		return -os.WTERMSIG(status)
	return -1


def reap(proc, timeout, first_signal=signal.SIGKILL):
	"""Wait for `proc`, signalling its group after `timeout` (SIGTERM first means SIGKILL 5 s later); then kill what is left of the group.

	Where os.waitid exists the group is killed while the leader is a zombie, so
	its id cannot have been reused. A second thread reaping the same process (a
	worker stopping a handler while Ctrl-C kills every handler) gets the first
	one's result. Returns (exit status, timed out).
	"""
	pid = proc.pid
	fired = threading.Event()

	def expire(sig):
		fired.set()
		kill_group(pid, sig)

	timers = [threading.Timer(timeout, expire, (first_signal,))]
	if first_signal != signal.SIGKILL:
		timers.append(threading.Timer(timeout + 5.0, expire, (signal.SIGKILL,)))
	for timer in timers:
		timer.daemon = True
		timer.start()
	try:
		if hasattr(os, 'waitid'):
			os.waitid(os.P_PID, pid, os.WEXITED | os.WNOWAIT)
			with _REAP_LOCK:
				if proc.returncode is None:
					kill_group(pid)
					proc.returncode = exit_code(os.waitpid(pid, 0)[1])
		else:
			status = os.waitpid(pid, 0)[1]
			with _REAP_LOCK:
				if proc.returncode is None:
					proc.returncode = exit_code(status)
			kill_group(pid)
	except ChildProcessError:
		pass
	finally:
		for timer in timers:
			timer.cancel()
	deadline = time.time() + 5.0
	while proc.returncode is None and time.time() < deadline:
		time.sleep(0.01)
	LIVE_GROUPS.discard(pid)
	return proc.returncode, fired.is_set()


Ran = collections.namedtuple('Ran', 'exit timed_out stdout stderr error')


def run_command(argv, cwd, timeout, stdin=b'', out_dir=None, name='step'):
	"""Run one command in a session of its own, stdin from a file, stdout and stderr to files."""
	out_dir = out_dir or tempfile.mkdtemp(prefix='tsvzconform-run-')
	paths = [os.path.join(out_dir, name + suffix) for suffix in ('.in', '.out', '.err')]
	with open(paths[0], 'wb') as f:
		f.write(stdin)
	with open(paths[0], 'rb') as fin, open(paths[1], 'wb') as fout, open(paths[2], 'wb') as ferr:
		try:
			proc = subprocess.Popen(argv, cwd=cwd, stdin=fin, stdout=fout, stderr=ferr, start_new_session=True)
		except OSError as e:
			return Ran(None, False, b'', b'', 'cannot run %s: %s' % (quote(argv), e))
	LIVE_GROUPS.add(proc.pid)
	code, timed_out = reap(proc, timeout)
	with open(paths[1], 'rb') as f:
		stdout = f.read()
	with open(paths[2], 'rb') as f:
		stderr = f.read()
	return Ran(code, timed_out, stdout, stderr, None)


class Impl(object):
	"""The implementation under test. Its commands run from `cwd`, the directory the runner was started in."""

	def __init__(self, command, cwd=None):
		self.command = command
		self.argv = shlex.split(command)
		self.cwd = cwd or os.getcwd()
		self.version = ''

	def cli(self, op, store, args=()):
		"""The canonical §20.1 command line for one operation."""
		return self.argv + ['--format', 'records', '-q', op, store] + list(args)


def kill_live_groups():
	for pid in list(LIVE_GROUPS):
		kill_group(pid)
	for handler in list(LIVE_HANDLERS):
		try:
			handler.kill()
		except Exception:
			pass


def quote(argv):
	return ' '.join(shlex.quote(a) for a in argv)


# ---------------------------------------------------------------------------
# The handler protocol (§21)
# ---------------------------------------------------------------------------

class HandlerError(Exception):
	pass


def read_pointer(store):
	"""The values of a store's pointer file (§21.2) as a dict, or None when it is absent or not a valid pointer."""
	try:
		with open(store + '.serve', 'rb') as f:
			lines = f.read().decode('utf-8').split('\n')
	except (OSError, UnicodeDecodeError):
		return None
	if lines[0] != 'tsvz-handler\t1':
		return None
	values = {}
	for line in lines[1:]:
		name, sep, value = line.partition('\t')
		if sep:
			values[name] = decode_field(value, '\t')
	if not values.get('address', '').startswith('unix:'):
		return None
	return values


def encode_arg(arg):
	"""A request field for one argument: marker keys raw, anything else §13-encoded with <#> (§21.4.3)."""
	return arg if is_marker_key(arg) else encode_field(arg, '\t', is_key=True)


Response = collections.namedtuple('Response', 'status message output diagnostics')


class Client(object):
	"""One connection to a handler: a request, then its whole response (§21.3.2)."""

	def __init__(self, path, timeout):
		self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		try:
			self.sock.settimeout(timeout)
			self.sock.connect(path)
		except BaseException:
			self.sock.close()
			raise
		self.rfile = self.sock.makefile('rb')

	def send(self, payload):
		self.sock.sendall(payload)

	def response(self):
		"""Read one response (§21.6); raise ConnectionError if the handler closes first."""
		output = []
		diagnostics = []
		while True:
			line = self.rfile.readline()
			if not line:
				raise ConnectionError('the handler closed the connection')
			if line.startswith(b'#!'):
				diagnostics.append(line)
			elif line.startswith(b'#'):
				text = line.rstrip(b'\n').decode('utf-8', 'replace')
				code, _, message = text[1:].partition('\t')
				if not re.match(r'[0-9]+\Z', code):
					raise ConnectionError('a status line that is not #N: %s' % show(text))
				return Response(int(code), decode_field(message, '\t'), b''.join(output), diagnostics)
			else:
				output.append(line)

	def request(self, fields, options=(), body=None):
		"""Send one request (fields already in the protocol encoding) and read its response."""
		payload = '\t'.join(list(options) + list(fields)) + '\n'
		if body is not None:
			payload += ''.join(line + '\n' for line in body) + '#\n'
		self.send(payload.encode('utf-8'))
		return self.response()

	def close(self):
		try:
			self.rfile.close()
		finally:
			self.sock.close()


LIVE_HANDLERS = set()


def _exited(proc):
	"""True once `proc` has exited. Where os.waitid exists the leader is left a zombie, so its group can still be killed safely."""
	if hasattr(os, 'waitid'):
		return os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is not None
	return proc.poll() is not None


class Handler(object):
	"""The implementation's own `serve STORE` (§21.13), started and stopped by the runner (design section 2.7)."""

	def __init__(self, impl, store, out_dir, wait=HANDLER_WAIT):
		self.impl = impl
		self.store = store
		self.out_dir = out_dir
		self.wait = wait
		self.proc = None
		self.path = None
		self.exit = None

	def start(self):
		"""Start serving; raise HandlerError when no valid pointer and answer come within the wait."""
		if STOP.is_set():
			raise HandlerError('interrupted')
		with open(os.path.join(self.out_dir, 'serve.out'), 'wb') as out, open(os.path.join(self.out_dir, 'serve.err'), 'wb') as err:
			self.proc = subprocess.Popen(self.impl.argv + ['serve', self.store], cwd=self.impl.cwd, stdin=subprocess.DEVNULL,
				stdout=out, stderr=err, start_new_session=True)
		LIVE_GROUPS.add(self.proc.pid)
		LIVE_HANDLERS.add(self)
		deadline = time.time() + self.wait
		while True:
			if _exited(self.proc):
				kill_group(self.proc.pid)
				self.exit = reap(self.proc, self.wait)[0]
				self._forget()
				raise HandlerError('serve exited with status %d before answering%s' % (self.exit, self.stderr_tail()))
			pointer = read_pointer(self.store)
			if pointer:
				try:
					client = Client(pointer['address'][5:], self.wait)
					try:
						client.request(['len'])
					finally:
						client.close()
					self.path = pointer['address'][5:]
					return self
				except (OSError, ConnectionError):
					pass
			if time.time() > deadline:
				self.kill()
				raise HandlerError('no valid pointer file and answer within %.0f s%s' % (self.wait, self.stderr_tail()))
			time.sleep(0.01)

	def stderr_tail(self):
		try:
			with open(os.path.join(self.out_dir, 'serve.err'), 'rb') as f:
				lines = f.read().decode('utf-8', 'replace').splitlines()[-5:]
		except OSError:
			return ''
		return ''.join('\n    ' + line for line in lines)

	def client(self):
		return Client(self.path, self.wait)

	def stop(self):
		"""Send stop (§21.7.1) and reap the handler; return problems (empty when it answered #0 and exited)."""
		problems = []
		if self.proc is None or self.proc.returncode is not None:
			return problems
		if self.path:
			try:
				client = self.client()
				try:
					response = client.request(['stop'])
				finally:
					client.close()
				if response.status != 0:
					problems.append('stop answered #%d' % response.status)
			except (OSError, ConnectionError) as e:
				problems.append('stop failed: %s' % e)
		else:
			kill_group(self.proc.pid, signal.SIGTERM)
		self.exit, timed_out = reap(self.proc, self.wait, signal.SIGTERM)
		self._forget()
		if timed_out:
			problems.append('the handler did not exit within %.0f s of stop' % self.wait)
		return problems

	def kill(self):
		if self.proc is not None and self.proc.returncode is None:
			kill_group(self.proc.pid)
			self.exit = reap(self.proc, 5)[0]
		self._forget()

	def _forget(self):
		LIVE_HANDLERS.discard(self)
		if self.proc is not None:
			LIVE_GROUPS.discard(self.proc.pid)


# ---------------------------------------------------------------------------
# Running cases
# ---------------------------------------------------------------------------

class Context(object):
	def __init__(self, impl, work, timeout=10.0, features=frozenset()):
		self.impl = impl
		self.work = work
		self.timeout = timeout
		self.features = features


class Result(object):
	def __init__(self, case, mode, result, detail=(), seconds=0.0, kept=None):
		self.case = case
		self.mode = mode
		self.cls = case.cls if mode == 'files' else 'handler'
		self.result = result
		self.detail = list(detail)
		self.seconds = seconds
		self.kept = kept

	@property
	def name(self):
		return self.case.id if self.mode == 'files' or self.case.func else '%s@%s' % (self.case.id, self.mode)


class Failed(Exception):
	"""A case's expectation did not hold; args are the detail lines."""


def content_bytes(content):
	return base64.b64decode(content['b64']) if isinstance(content, dict) else content.encode('utf-8')


def fill(text, case_dir, store):
	return text.replace('{dir}', case_dir).replace('{store}', store)


def fill_matcher(matcher, case_dir, store):
	if not isinstance(matcher, dict):
		return matcher
	if 'text' in matcher:
		return {'text': fill(matcher['text'], case_dir, store)}
	name = 'rows' if 'rows' in matcher else 'lines'
	return {name: [[fill(c, case_dir, store) for c in row] for row in matcher[name]]}


def expand_expect(expect):
	"""A reader case's expectations as op steps (design section 1.4)."""
	steps = []
	if 'read' in expect:
		read = expect['read']
		if isinstance(read, list):
			read = {'rows': read}
		step = {'op': ['read'], 'exit': read.get('exit', 0)}
		if read.get('rows') is not None:
			step['stdout'] = {'rows': read['rows']}
		steps.append(step)
	if 'keys' in expect:
		steps.append({'op': ['keys'], 'stdout': {'lines': [[k] for k in expect['keys']]}})
	if 'len' in expect:
		steps.append({'op': ['len'], 'stdout': {'text': '%d\n' % expect['len']}})
	for query in expect.get('get', []):
		steps.append({'op': ['get'] + query['args'], 'exit': query.get('exit', 0), 'stdout': {'rows': query.get('rows', [])}})
	if 'verify' in expect:
		steps.append({'op': ['verify'], 'exit': expect['verify'].get('exit', 0), 'stdout': {'lines': expect['verify'].get('lines', [])}})
	return steps


def step_op(step):
	"""The operation a step runs, or None."""
	for name in ('op', 'request'):
		if name in step:
			return step[name][0]
	if 'argv' in step:
		for arg in step['argv']:
			if arg in OPERATIONS:
				return arg
	return None


def materialize(case, case_dir):
	for rel, content in case.files.items():
		path = os.path.join(case_dir, rel)
		os.makedirs(os.path.dirname(path), exist_ok=True)
		with open(path, 'wb') as f:
			f.write(content_bytes(content))


def _committed_lines(text):
	return text[:text.rfind('\n') + 1].split('\n')[:-1] if '\n' in text else []


class CaseRun(object):
	"""One case in one mode, in its own directory."""

	def __init__(self, case, mode, ctx, case_dir):
		self.case = case
		self.mode = mode
		self.ctx = ctx
		self.dir = case_dir
		self.store = os.path.join(case_dir, case.store)
		self.delimiter = delimiter_of(self.store)
		self.handler = None
		self.client = None
		self.before = None
		self.trace = []
		self.count = 0

	def run(self):
		case = self.case
		materialize(case, self.dir)
		blocks = [case.after or {}] + [s['check'] for s in (case.steps or []) if 'check' in s]
		observed = None
		if any('fidelity' in b for b in blocks):
			try:
				observed = observe(self.store)
			except CodecUnavailable as e:
				raise Skip('the runner cannot read %s parts' % e)
		self.before = Before(snapshot_store(self.store), observed)
		steps = case.steps or expand_expect(case.expect)
		problems = []
		try:
			if self.mode != 'files':
				try:
					self.handler = Handler(self.ctx.impl, self.store, self.dir).start()
				except HandlerError as e:
					raise Failed('serve   the handler did not start: %s' % e)
			for step in steps:
				self.count += 1
				self.step(step)
		finally:
			if self.client is not None:
				self.client.close()
			if self.handler is not None:
				problems = self.handler.stop()
		if problems:
			raise Failed('after   ' + '; '.join(problems))
		if case.after:
			found = check_after(case.after, self.store, self.dir, self.before, self.handler.exit if self.handler else None)
			if found:
				raise Failed(*(['after   ' + found[0]] + ['        ' + p for p in found[1:]]))

	def describe(self, text):
		self.trace.append('step %d  %s' % (self.count, text))

	def fail(self, problems, stderr=b''):
		lines = self.trace[-1:] + ['        ' + p for p in problems]
		tail = stderr.decode('utf-8', 'replace').splitlines()[-5:]
		if tail:
			lines.append('stderr  ' + '\n        '.join(tail))
		raise Failed(*lines)

	def step(self, step):
		action = action_of(step)
		if action in ('append', 'write'):
			path = os.path.join(self.dir, step['path'])
			with open(path, 'ab' if action == 'append' else 'wb') as f:
				f.write(content_bytes(step[action]))
			self.describe('%s %s to %s' % (action, show(content_bytes(step[action])), step['path']))
			return
		if action == 'check':
			self.describe('check the files')
			found = check_after(step['check'], self.store, self.dir, self.before)
			if found:
				self.fail(found)
			return
		if self.mode == 'socket':
			return self.socket_step(step, action)
		if action == 'op':
			argv = self.ctx.impl.cli(step['op'][0], self.store, step['op'][1:])
		else:
			argv = self.ctx.impl.argv + [fill(a, self.dir, self.store) for a in step['argv']]
		stdin = content_bytes(step['stdin']) if 'stdin' in step else b''
		self.describe(quote(argv) + (' < ' + show(stdin) if stdin else ''))
		ran = run_command(argv, self.ctx.impl.cwd, self.ctx.timeout, stdin, self.dir, 'step%d' % self.count)
		if ran.error:
			self.fail([ran.error])
		if ran.timed_out:
			self.fail(['timed out after %.0f s' % self.ctx.timeout], ran.stderr)
		problems = []
		want = step.get('exit', 0)
		if ran.exit != want:
			problems.append('exit status %d, expected %d' % (ran.exit, want))
		op = step_op(step)
		if 'stdout' in step:
			diff = match_stdout(fill_matcher(step['stdout'], self.dir, self.store), ran.stdout, self.delimiter,
				live=(op == 'read'), line_delimiter='\t' if op in ('verify', 'parts') else None)
			if diff:
				problems.append(diff)
		if self.mode == 'files' and 'stderr' in step:
			if step['stderr'] == 'empty' and ran.stderr:
				problems.append('stderr should be empty')
			if step['stderr'] == 'nonempty' and not ran.stderr:
				problems.append('stderr should carry a diagnostic, but is empty')
		if problems:
			self.fail(problems, ran.stderr)

	def socket_step(self, step, action):
		if self.client is None:
			try:
				self.client = self.handler.client()
			except OSError as e:
				self.fail(['cannot connect to the handler: %s' % e])
		try:
			if action == 'raw':
				payload = fill(step['raw'], self.dir, self.store)
				self.describe('send ' + show(payload))
				self.client.send(payload.encode('utf-8'))
				response = self.client.response()
			else:
				words = step[action]
				fields = [words[0]] + [encode_arg(fill(a, self.dir, self.store)) for a in words[1:]]
				options = step.get('options', [])
				body = step.get('body')
				if body is None and 'stdin' in step:
					body = _committed_lines(content_bytes(step['stdin']).decode('utf-8'))
				self.describe('request ' + show('\t'.join(options + fields)) + (' with body ' + show(body) if body is not None else ''))
				response = self.client.request(fields, options, body)
		except (OSError, ConnectionError) as e:
			self.client.close()
			self.client = None
			self.fail(['no response: %s' % e])
		problems = []
		want = step.get('exit', 0)
		if response.status != want:
			problems.append('status #%d, expected #%d%s' % (response.status, want, (' (%s)' % response.message) if response.message else ''))
		op = step_op(step)
		if 'stdout' in step:
			diff = match_stdout(fill_matcher(step['stdout'], self.dir, self.store), response.output, '\t', live=(op == 'read'))
			if diff:
				problems.append(diff)
		if problems:
			self.fail(problems)


class CheckContext(object):
	"""What a procedural check gets: the implementation, its own directory and helpers."""

	def __init__(self, ctx, case_dir):
		self.impl = ctx.impl
		self.timeout = ctx.timeout
		self.dir = case_dir
		self.count = 0
		self.cleanup = []

	def path(self, name):
		return os.path.join(self.dir, name)

	def write(self, name, content):
		path = self.path(name)
		with open(path, 'wb') as f:
			f.write(content if isinstance(content, bytes) else content.encode('utf-8'))
		return path

	def run(self, argv, stdin=b''):
		self.count += 1
		return run_command(argv, self.impl.cwd, self.timeout, stdin, self.dir, 'run%d' % self.count)

	def handler(self, store):
		return Handler(self.impl, store, self.dir).start()


def run_name(case, mode):
	return case.id if mode == 'files' else '%s@%s' % (case.id, mode)


def run_case(case, mode, ctx):
	"""Run one case in one mode and return its Result; a passing or skipped case's directory is removed."""
	started = time.time()
	case_dir = os.path.join(ctx.work, run_name(case, mode))
	shutil.rmtree(case_dir, ignore_errors=True)
	os.makedirs(case_dir)
	detail = []
	try:
		if case.func:
			cx = CheckContext(ctx, case_dir)
			try:
				problems = case.func(cx)
			finally:
				for undo in cx.cleanup:
					undo()
			result = PASS if not problems else (FAIL if case.level == 'MUST' else WARN)
			detail = list(problems)
		else:
			try:
				CaseRun(case, mode, ctx, case_dir).run()
				result = PASS
			except Failed as e:
				result = FAIL if case.level == 'MUST' else WARN
				detail = list(e.args)
	except Skip as e:
		result, detail = SKIP, [str(e)]
	except Exception:
		result, detail = ERROR, traceback.format_exc().rstrip().splitlines()
	kept = None
	if result in (PASS, SKIP):
		shutil.rmtree(case_dir, ignore_errors=True)
	else:
		kept = case_dir
	return Result(case, mode, result, detail, time.time() - started, kept)


# ---------------------------------------------------------------------------
# Procedural checks (design section 4.3)
# ---------------------------------------------------------------------------

@procedural('20.4.3-terminal-output-is-a-table', 'cli', ['20.4.3'], 'SHOULD',
	'Without --format, output to a terminal uses the table layout, which differs from the records the same command prints to a file.')
def check_terminal_default(cx):
	import pty
	from select import select as wait_readable
	store = cx.write('s.tsvz', 'alpha\tA\t1\nbeta\tB<sep>x\t2\n')
	records = cx.run(cx.impl.argv + ['--format', 'records', 'read', store])
	if records.exit != 0:
		return ['read --format records exited %s' % records.exit]
	master, slave = pty.openpty()
	try:
		proc = subprocess.Popen(cx.impl.argv + ['read', store], cwd=cx.impl.cwd, stdin=subprocess.DEVNULL, stdout=slave,
			stderr=subprocess.DEVNULL, start_new_session=True)
		LIVE_GROUPS.add(proc.pid)
		os.close(slave)
		slave = None
		chunks = []
		deadline = time.time() + cx.timeout
		timed_out = False
		while True:
			left = deadline - time.time()
			if left <= 0 or not wait_readable([master], [], [], left)[0]:
				timed_out = True
				kill_group(proc.pid)
				break
			try:
				chunk = os.read(master, 65536)
			except OSError:
				break
			if not chunk:
				break
			chunks.append(chunk)
		code, _ = reap(proc, max(0.1, deadline - time.time()))
	finally:
		os.close(master)
		if slave is not None:
			os.close(slave)
	shown = b''.join(chunks).replace(b'\r\n', b'\n')
	if timed_out:
		return ['read on a terminal timed out after %.0f s' % cx.timeout]
	if code != 0:
		return ['read on a terminal exited %s' % code]
	if shown == records.stdout:
		return ['read on a terminal printed exactly the records format: %s' % show(shown)]
	return []


def _serving(cx, content='k\tv\n'):
	store = cx.write('s.tsvz', content)
	return store, cx.handler(store)


@procedural('21.2.3-pointer-is-locked', 'handler', ['21.2.3'], 'MUST',
	'While a handler serves, it holds an exclusive flock on its pointer file.', requires=['handler'])
def check_pointer_locked(cx):
	import fcntl
	store, handler = _serving(cx)
	problems = []
	try:
		with open(store + '.serve', 'rb') as f:
			try:
				fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
				problems.append('the runner could lock the pointer file while the handler serves')
				fcntl.flock(f.fileno(), fcntl.LOCK_UN)
			except OSError:
				pass
	finally:
		problems.extend(handler.stop())
	return problems


@procedural('21.2.2-pointer-names-host-and-pid', 'handler', ['21.2.2'], 'MUST',
	'The pointer file names this host and the pid of a running process.', requires=['handler'])
def check_pointer_fields(cx):
	store, handler = _serving(cx)
	problems = []
	try:
		pointer = read_pointer(store) or {}
		if pointer.get('host') != socket.gethostname():
			problems.append('host is %s, expected %s' % (show(pointer.get('host')), show(socket.gethostname())))
		pid = pointer.get('pid', '')
		if not pid.isdigit():
			problems.append('pid is %s' % show(pid))
		else:
			try:
				os.kill(int(pid), 0)
			except ProcessLookupError:
				problems.append('pid %s is not a running process' % pid)
			except PermissionError:
				pass
	finally:
		problems.extend(handler.stop())
	return problems


@procedural('21.2.3-pointer-not-writable-by-others', 'handler', ['21.2.3'], 'MUST',
	'The pointer file is not writable by group or others.', requires=['handler'])
def check_pointer_mode(cx):
	store, handler = _serving(cx)
	mode = None
	try:
		mode = stat.S_IMODE(os.stat(store + '.serve').st_mode)
	finally:
		problems = handler.stop()
	if mode & 0o022:
		problems.append('the pointer file has mode %o' % mode)
	return problems


@procedural('21.13.1-second-serve-exits-1', 'handler', ['21.13.1', '21.2.3'], 'MUST',
	'serve exits with status 1 while another handler holds the pointer lock, and the first keeps serving.', requires=['handler'])
def check_second_serve(cx):
	store, handler = _serving(cx)
	problems = []
	try:
		second = cx.run(cx.impl.argv + ['serve', store])
		if second.timed_out:
			problems.append('a second serve did not exit within %.0f s' % cx.timeout)
		elif second.exit != 1:
			problems.append('a second serve exited %s, expected 1' % second.exit)
		client = handler.client()
		try:
			if client.request(['len']).status != 0:
				problems.append('the first handler stopped answering')
		finally:
			client.close()
	finally:
		problems.extend(handler.stop())
	return problems


@procedural('21.2.3-stale-pointer-is-replaced', 'handler', ['21.2.3'], 'MUST',
	'A pointer file that no process locks is stale: serve replaces it and starts.', requires=['handler'])
def check_stale_pointer(cx):
	store = cx.write('s.tsvz', 'k\tv\n')
	cx.write('s.tsvz.serve', 'tsvz-handler\t1\naddress\tunix:%s\nhost\t%s\npid\t999999999\n' % (cx.path('gone.sock'), socket.gethostname()))
	os.chmod(store + '.serve', 0o600)
	try:
		handler = cx.handler(store)
	except HandlerError as e:
		return ['serve did not take over a stale pointer: %s' % e]
	return handler.stop()


@procedural('21.2.3-no-write-through-symlink', 'handler', ['21.2.3'], 'MUST',
	'A pointer path that is a symbolic link is never written through: the file it points to is unchanged.', requires=['handler'])
def check_pointer_symlink(cx):
	store = cx.write('s.tsvz', 'k\tv\n')
	victim = cx.write('victim.txt', 'do not touch\n')
	os.symlink(victim, store + '.serve')
	problems = []
	try:
		handler = cx.handler(store)
		problems.extend(handler.stop())
	except HandlerError:
		pass
	with open(victim, 'rb') as f:
		if f.read() != b'do not touch\n':
			problems.append('the file the symlink points to was modified')
	return problems


def _decoy(cx, kind='unix'):
	"""A listening socket that records whether anything connects to it.

	A Unix socket goes in a short directory of its own: socket paths are
	limited to about 100 bytes, and a case directory can be longer.
	"""
	if kind == 'unix':
		directory = tempfile.mkdtemp(prefix='tsvzconform-', dir='/tmp' if os.path.isdir('/tmp') else None)
		cx.cleanup.append(lambda: shutil.rmtree(directory, ignore_errors=True))
		path = os.path.join(directory, 'decoy.sock')
		sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		sock.bind(path)
		address = 'unix:' + path
	else:
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind(('127.0.0.1', 0))
		address = 'tcp:127.0.0.1:%d' % sock.getsockname()[1]
	sock.listen(8)
	sock.setblocking(False)
	return sock, address


def _contacted(sock):
	try:
		conn, _ = sock.accept()
		conn.close()
		return True
	except (BlockingIOError, OSError):
		return False


def _pointer_text(address, host=None, extra=''):
	return 'tsvz-handler\t1\naddress\t%s\nhost\t%s\npid\t%d\n%s' % (address, host or socket.gethostname(), os.getpid(), extra)


def _refuse_pointer(cx, setup, kind='unix', answer=False):
	"""Plant a bad pointer to a decoy: the CLI must not connect (§21.2.4); with `answer`, it should answer from the files (§20.8.2)."""
	store = cx.write('s.tsvz', 'k\tv\n')
	sock, address = _decoy(cx, kind)
	try:
		setup(store, address)
		ran = cx.run(cx.impl.cli('get', store, ['k']))
		problems = []
		if _contacted(sock):
			problems.append('the command connected to the address in a pointer file it must not use (§21.2.4)')
		if ran.timed_out:
			problems.append('get timed out')
		elif answer and (ran.exit != 0 or match_stdout({'rows': [['k', 'v']]}, ran.stdout, '\t')):
			problems.append('get did not answer from the files: exit %s, stdout %s' % (ran.exit, show(ran.stdout)))
		return problems
	finally:
		sock.close()


def _write_pointer(path, text, mode=0o600):
	with open(path, 'w', encoding='utf-8') as f:
		f.write(text)
	os.chmod(path, mode)


def _register_pointer_checks():
	def symlink(store, address):
		real = os.path.join(os.path.dirname(store), 'real.serve')
		_write_pointer(real, _pointer_text(address))
		os.symlink(real, store + '.serve')

	def group_writable(store, address):
		_write_pointer(store + '.serve', _pointer_text(address), 0o620)

	def wrong_first_line(store, address):
		_write_pointer(store + '.serve', _pointer_text(address).replace('tsvz-handler\t1', 'tsvz-handler\t2', 1))

	def control_character(store, address):
		_write_pointer(store + '.serve', _pointer_text(address, extra='x-note\tbad\x07value\n'))

	def other_host(store, address):
		_write_pointer(store + '.serve', _pointer_text(address, host='other-host.invalid'))

	def not_unix(store, address):
		_write_pointer(store + '.serve', _pointer_text(address))

	def other_owner(store, address):
		if os.geteuid() != 0:
			raise Skip('needs root to give the pointer file another owner')
		_write_pointer(store + '.serve', _pointer_text(address))
		os.chown(store + '.serve', 65534, 65534)

	def socket_other_owner(store, address):
		if os.geteuid() != 0:
			raise Skip('needs root to give the socket another owner')
		_write_pointer(store + '.serve', _pointer_text(address))
		os.chown(address[5:], 65534, 65534)

	variants = [
		('symlink', 'a pointer file that is a symbolic link', symlink, 'unix'),
		('group-writable', 'a pointer file group members can write', group_writable, 'unix'),
		('wrong-first-line', 'a pointer file whose first line is not tsvz-handler<TAB>1', wrong_first_line, 'unix'),
		('control-character', 'a pointer file with a control character in a value', control_character, 'unix'),
		('other-host', 'a pointer file naming another host', other_host, 'unix'),
		('not-unix', 'a pointer file whose address is not a Unix-domain socket', not_unix, 'tcp'),
		('other-owner', 'a pointer file owned by another user', other_owner, 'unix'),
		('socket-other-owner', 'a pointer naming a socket owned by another user', socket_other_owner, 'unix'),
	]
	for slug, what, setup, kind in variants:
		def check(cx, setup=setup, kind=kind):
			return _refuse_pointer(cx, setup, kind)
		procedural('21.2.4-refuse-%s' % slug, 'cli', ['21.2.4'], 'MUST',
			'A command does not use %s.' % what)(check)

	def fall_back(cx):
		return _refuse_pointer(cx, wrong_first_line, answer=True)
	procedural('20.8.2-files-answer-past-a-bad-pointer', 'cli', ['20.8.2', '21.2.4'], 'SHOULD',
		'A command that ignores a pointer file it must not use operates on the files instead.')(fall_back)


_register_pointer_checks()


@procedural('21.10.2-one-mib-request-line', 'handler', ['21.10.2'], 'MUST',
	'A request line of 1 MiB is within the limit every handler must accept.', requires=['handler'])
def check_line_limit(cx):
	store, handler = _serving(cx, '')
	problems = []
	try:
		value = 'x' * ((1 << 20) - len('set\tbig\t\n'))
		client = handler.client()
		try:
			response = client.request(['set', 'big', value])
			if response.status != 0:
				problems.append('a 1 MiB set answered #%d' % response.status)
			got = client.request(['get', 'big'])
			if got.output != ('big\t%s\n' % value).encode('utf-8'):
				problems.append('get did not return the 1 MiB value')
		except (OSError, ConnectionError) as e:
			problems.append('the handler did not answer a 1 MiB line: %s' % e)
		finally:
			client.close()
	finally:
		problems.extend(handler.stop())
	return problems


@procedural('21.7.2-cut-off-bulk-writes-nothing', 'handler', ['21.7.2'], 'MUST',
	'A set - whose connection ends before the closing # line writes nothing.', requires=['handler'])
def check_cut_off_bulk(cx):
	store, handler = _serving(cx, '')
	problems = []
	try:
		client = handler.client()
		client.send(b'set\t-\na\t1\nb\t2\n')
		client.close()
		time.sleep(0.2)
	finally:
		problems.extend(handler.stop())
	rows = replay_store(store).rows()
	if rows:
		problems.append('the store has rows: %s' % show(rows))
	return problems


@procedural('21.8.2-concurrent-pop-is-atomic', 'handler', ['21.8.2'], 'MUST',
	'Eight clients pop one key at once: exactly one gets the row, the others status 3, and one tombstone is written.', requires=['handler'])
def check_atomic_pop(cx):
	store, handler = _serving(cx, 'k\tv\n')
	problems = []
	results = []
	barrier = threading.Barrier(8)

	def pop():
		try:
			client = handler.client()
			try:
				barrier.wait(cx.timeout)
				results.append(client.request(['pop', 'k']))
			finally:
				client.close()
		except Exception as e:
			results.append(e)

	try:
		threads = [threading.Thread(target=pop) for _ in range(8)]
		for thread in threads:
			thread.daemon = True
			thread.start()
		for thread in threads:
			thread.join(cx.timeout * 2)
	finally:
		problems.extend(handler.stop())
	errors = [r for r in results if isinstance(r, Exception)]
	if errors:
		return problems + ['a client failed: %s' % errors[0]]
	statuses = sorted(r.status for r in results)
	if statuses != [0] + [3] * 7:
		problems.append('statuses %s, expected one 0 and seven 3' % statuses)
	tombstones = 0
	for part in store_parts(store):
		tombstones += read_part(part).split(b'\n').count(b'k')
	if tombstones != 1:
		problems.append('%d tombstones for k, expected 1' % tombstones)
	return problems


@procedural('21.11.1-socket-closed-to-others', 'handler', ['21.11.1'], 'SHOULD',
	'By default the socket, or a directory on its path, is closed to group and others.', requires=['handler'])
def check_socket_access(cx):
	store, handler = _serving(cx)
	try:
		path = handler.path
		closed = not (stat.S_IMODE(os.stat(path).st_mode) & 0o066)
		directory = os.path.dirname(os.path.abspath(path))
		while not closed and directory not in ('', '/'):
			info = os.stat(directory)
			closed = info.st_uid == os.geteuid() and not (stat.S_IMODE(info.st_mode) & 0o011)
			directory = os.path.dirname(directory)
	finally:
		problems = handler.stop()
	if not closed:
		problems.append('%s and every directory above it can be reached by others' % path)
	return problems


# ---------------------------------------------------------------------------
# Selection, the run, and the report
# ---------------------------------------------------------------------------

def matches_only(case_id, patterns):
	"""True when `case_id` matches one of the --only patterns (or there are none)."""
	if not patterns:
		return True
	clause = case_id.split('-', 1)[0]
	for pattern in patterns:
		if pattern == case_id or fnmatch.fnmatchcase(case_id, pattern):
			return True
		if _CLAUSE_RE.match(pattern) and (clause == pattern or clause.startswith(pattern + '.')):
			return True
	return False


def select(cases, opts):
	"""(runs, skipped): the (case, mode) pairs to run, and Results for those skipped by claims."""
	runs = []
	skipped = []
	for case in cases:
		if not matches_only(case.id, opts.only):
			continue
		if opts.level == 'must' and case.level != 'MUST':
			continue
		for mode in case_modes(case):
			cls = case.cls if mode == 'files' else 'handler'
			if cls not in opts.classes or mode not in opts.modes:
				continue
			if mode != 'files' and 'handler' not in opts.features:
				if case.cls == 'handler':
					skipped.append(Result(case, mode, SKIP, ['handler not claimed']))
				continue
			reason = None
			for need in case.requires:
				if need.startswith('!') and need[1:] in opts.features:
					reason = 'applies when %s is not claimed' % need[1:]
				elif not need.startswith('!') and need not in opts.features:
					reason = '%s not claimed' % need
			if reason:
				skipped.append(Result(case, mode, SKIP, [reason]))
			else:
				runs.append((case, mode))
	return runs, skipped


def run_all(runs, ctx, jobs, fail_fast=False, progress=None):
	"""Run every (case, mode), `jobs` at a time; return (results in the order of `runs`, interrupted).

	After Ctrl-C the results are those finished before it: cases still running
	then are killed, so their outcome says nothing about the implementation.
	"""
	results = [None] * len(runs)
	finished = None
	stop_early = threading.Event()

	def one(index):
		case, mode = runs[index]
		if STOP.is_set() or stop_early.is_set():
			return
		result = run_case(case, mode, ctx)
		results[index] = result
		if fail_fast and result.result in (FAIL, ERROR):
			stop_early.set()
		if progress:
			progress(result)

	with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
		futures = [pool.submit(one, i) for i in range(len(runs))]
		try:
			for future in futures:
				while True:
					try:
						future.result(timeout=0.2)
						break
					except concurrent.futures.TimeoutError:
						continue
		except KeyboardInterrupt:
			finished = [r for r in results if r is not None]
			STOP.set()
			kill_live_groups()
			for future in futures:
				future.cancel()
	if finished is not None:
		return finished, True
	return [r for r in results if r is not None], False


def rerun_command(impl, result, features):
	prog = os.path.relpath(os.path.abspath(__file__))
	argv = ['python3', prog, '--impl', impl.command, '--only', result.case.id]
	if not result.case.func:
		argv += ['--mode', result.mode]
	if features:
		argv += ['--feature', ','.join(f for f in FEATURES if f in features)]
	return quote(argv)


def render_failure(result, impl, features):
	case = result.case
	label = result.result
	lines = ['%s %s   %s %s' % (label, result.name, case.level, ' '.join('§' + c for c in case.spec)),
		'  ' + case.note]
	if case.files:
		for rel, content in sorted(case.files.items()):
			text = content_bytes(content)
			lines.append('  files   %s = %s' % (rel, show(text) if not isinstance(content, dict) else 'b64 ' + show(content['b64'])))
	for line in result.detail:
		lines.append('  ' + line)
	if result.kept:
		lines.append('  kept    ' + result.kept)
	lines.append('  rerun   ' + rerun_command(impl, result, features))
	return '\n'.join(lines)


def verdicts(results, classes):
	"""Conformance per class (§2.1): no FAIL, and for cli and handler also reader and writer."""
	counts = {cls: collections.Counter(r.result for r in results if r.cls == cls) for cls in CLASSES}
	ran = {cls: bool(counts[cls]) and cls in classes for cls in CLASSES}
	out = {}
	for cls in CLASSES:
		if not ran[cls]:
			out[cls] = '–'
		elif counts[cls][FAIL] or counts[cls][ERROR]:
			out[cls] = '✗ (%d MUST%s)' % (counts[cls][FAIL], ', %d error' % counts[cls][ERROR] if counts[cls][ERROR] else '')
		elif cls in ('cli', 'handler'):
			bad = [c for c in ('reader', 'writer') if ran[c] and out[c] != '✓']
			missing = [c for c in ('reader', 'writer') if not ran[c]]
			if bad:
				out[cls] = '✗ (needs %s)' % ' and '.join(bad)
			elif missing:
				out[cls] = '✓ alone (%s not run)' % ' and '.join(missing)
			else:
				out[cls] = '✓'
		else:
			out[cls] = '✓'
	return out


def render_summary(results, impl, features, classes):
	lines = ['tsvzconform %s · cases format %d · impl: %s (%s)' % (version, CASES_FORMAT, impl.version or '?', impl.command),
		'claimed: %s' % (', '.join(f for f in FEATURES if f in features) or 'nothing'),
		'           pass  FAIL  warn  skip  error']
	for cls in CLASSES:
		c = collections.Counter(r.result for r in results if r.cls == cls)
		if sum(c.values()):
			lines.append('%-9s %6d %5d %5d %5d %6d' % (cls, c[PASS], c[FAIL], c[WARN], c[SKIP], c[ERROR]))
	v = verdicts(results, classes)
	lines.append('conformant: ' + '   '.join('%s %s' % (cls, v[cls]) for cls in CLASSES))
	return '\n'.join(lines)


def write_json(path, results, impl, features):
	data = {'tsvzconform': version, 'cases_format': CASES_FORMAT, 'impl': impl.command, 'impl_version': impl.version,
		'features': [f for f in FEATURES if f in features],
		'results': [{'id': r.case.id, 'mode': r.mode, 'class': r.cls, 'spec': r.case.spec, 'level': r.case.level,
			'result': r.result, 'detail': r.detail, 'seconds': round(r.seconds, 3)} for r in results]}
	with open(path, 'w', encoding='utf-8') as f:
		json.dump(data, f, ensure_ascii=False, indent=1)
		f.write('\n')


def probe_version(impl, timeout, work):
	"""The first line of `COMMAND -V`, or raise RuntimeError (design section 2.2)."""
	ran = run_command(impl.argv + ['-V'], impl.cwd, timeout, b'', work, 'version')
	if ran.error:
		raise RuntimeError(ran.error)
	if ran.timed_out or ran.exit != 0:
		raise RuntimeError('%s -V %s' % (impl.command, 'timed out' if ran.timed_out else 'exited %d' % ran.exit))
	return ran.stdout.decode('utf-8', 'replace').split('\n')[0].strip()


def _interrupt(signum, frame):
	raise KeyboardInterrupt


def main(argv=None):
	try:
		opts = parse_args(argv)
	except SystemExit as e:
		return e.code if isinstance(e.code, int) else 2
	if not (hasattr(socket, 'AF_UNIX') and hasattr(os, 'setsid')):
		eprint('tsvzconform: needs a POSIX system with Unix-domain sockets')
		return 1
	try:
		cases = all_cases()
	except CorpusError as e:
		eprint('tsvzconform: bad case file: %s' % e)
		return 1
	if opts.list:
		for case in cases:
			if case.cls in opts.classes and matches_only(case.id, opts.only) and (opts.level == 'all' or case.level == 'MUST'):
				print('%s\t%s\t%s\t%s\t%s' % (case.id, case.cls, case.level, ','.join(case.spec), ','.join(case.requires)))
		return 0
	impl = Impl(opts.impl)
	own_work = opts.work_dir is None
	work = tempfile.mkdtemp(prefix='tsvzconform-') if own_work else os.path.abspath(opts.work_dir)
	os.makedirs(work, exist_ok=True)
	previous = signal.signal(signal.SIGTERM, _interrupt)
	results = []
	interrupted = False
	try:
		try:
			impl.version = probe_version(impl, opts.timeout, work)
		except RuntimeError as e:
			eprint('tsvzconform: the implementation failed its -V probe: %s' % e)
			if own_work:
				shutil.rmtree(work, ignore_errors=True)
			return 1
		ctx = Context(impl, work, opts.timeout, opts.features)
		runs, skipped = select(cases, opts)
		tty = sys.stderr.isatty()
		done = [0]

		def progress(result):
			done[0] += 1
			if tty:
				sys.stderr.write('\r  %d/%d' % (done[0], len(runs)))
				sys.stderr.flush()

		results, interrupted = run_all(runs, ctx, opts.jobs, opts.fail_fast, progress)
		if tty:
			sys.stderr.write('\r' + ' ' * 20 + '\r')
		results = results + skipped
		order = {id(case): i for i, case in enumerate(cases)}
		results.sort(key=lambda r: (order.get(id(r.case), 0), MODES.index(r.mode)))
	except KeyboardInterrupt:
		interrupted = True
	finally:
		signal.signal(signal.SIGTERM, previous)
		kill_live_groups()
	shown = [r for r in results if r.result in (FAIL, ERROR)]
	for r in shown:
		print(render_failure(r, impl, opts.features))
		print()
	for r in results:
		if r.result == WARN:
			print('warn %s   %s: %s' % (r.name, ' '.join('§' + c for c in r.case.spec), r.detail[-1] if r.detail else r.case.note))
	reasons = collections.Counter(r.detail[0] for r in results if r.result == SKIP)
	if reasons:
		print('skipped: ' + '; '.join('%d %s' % (n, why) for why, n in sorted(reasons.items())))
		if opts.verbose:
			for r in results:
				if r.result == SKIP:
					print('  skip %s (%s)' % (r.name, r.detail[0]))
	print(render_summary(results, impl, opts.features, opts.classes))
	if opts.json:
		write_json(opts.json, results, impl, opts.features)
	failed = any(r.result in (FAIL, ERROR) for r in results)
	if own_work and not failed:
		shutil.rmtree(work, ignore_errors=True)
	elif failed:
		print('failed cases are kept in %s' % work)
	if interrupted:
		return 130
	return 1 if failed else 0


if __name__ == '__main__':
	sys.exit(main())
