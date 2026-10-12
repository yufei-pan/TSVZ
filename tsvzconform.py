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
