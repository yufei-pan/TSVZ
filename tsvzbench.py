#!/usr/bin/env python3
"""tsvzbench: benchmark tsvz-spec-v1 implementations side by side.

Every implementation is a black box, driven only through the spec's command
line (§20) and, when it has one, its handler socket (§21). The harness writes
the input stores itself, measures every child process with wait4 (wall time,
CPU, peak RSS), and checks every answer against its own replay model, so a
fast result only counts when it is also right.

	python3 tsvzbench.py --impl ref='python3 TSVZ.py' --impl new='./tsvz' --suite quick
"""
import argparse
import atexit
import base64
import gzip
import hashlib
import itertools
import json
import math
import os
import platform
import random
import re
import shlex
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import zlib

version = '1.0'
__version__ = version

GENERATOR_VERSION = 1

SUITES = {
	'smoke': dict(sizes=(100,), mid=100, calls=3, seq=100, sync=10, clients=(1, 4), per_client=50, repeat=1, timeout=30.0),
	'quick': dict(sizes=(1000, 10000), mid=10000, calls=20, seq=2000, sync=200, clients=(1, 4), per_client=1000, repeat=3, timeout=60.0),
	'standard': dict(sizes=(10000, 100000, 1000000), mid=100000, calls=30, seq=10000, sync=1000, clients=(1, 4, 16), per_client=5000, repeat=5, timeout=300.0),
	'large': dict(sizes=(10000, 100000, 1000000, 10000000), mid=1000000, calls=30, seq=10000, sync=1000, clients=(1, 4, 16), per_client=5000, repeat=3, timeout=1800.0),
}
FAMILIES = ('cold', 'bulk', 'perop', 'maint')
FEATURE_SHAPES = ('churn', 'escaped', 'markers', 'checksum', 'gz', 'multipart')
OPTIONAL_SHAPES = ('gz', 'multipart')
NEW_KEY_BASE = 1 << 40
ROTATED_KEY_BASE = 1 << 45
MISSING_INDEX = (1 << 47) - 1
HANDLER_WAIT = 10.0
ROW_BYTES = 80

OK, WRONG, EXIT, TIMEOUT, NA, REFUSED = 'ok', 'wrong', 'exit', 'timeout', 'n/a', 'refused'
FAILED = (WRONG, EXIT, TIMEOUT)


def eprint(*args):
	print(*args, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------

def parse_args(argv=None):
	"""Parse the harness's own options; usage errors exit with status 2."""
	parser = argparse.ArgumentParser(
		prog='tsvzbench',
		description='Benchmark tsvz-spec-v1 implementations through the spec command line (§20) and handler socket (§21).')
	parser.add_argument('--impl', action='append', default=[], metavar='NAME=COMMAND',
		help='an implementation: a name, then the command that runs its tsvz tool (repeatable; the first is the baseline)')
	parser.add_argument('--suite', choices=list(SUITES), default='standard', help='sizes and counts (default: standard)')
	parser.add_argument('--only', default=','.join(FAMILIES), metavar='FAMILIES',
		help='comma-separated families to run, from cold,bulk,perop,maint (default: all)')
	parser.add_argument('--repeat', type=int, metavar='N', help="timed repetitions per workload (default: the suite's)")
	parser.add_argument('--timeout', type=float, metavar='SECONDS', help="limit for one run (default: the suite's)")
	parser.add_argument('--seed', type=int, default=1, help='fixture seed (default: 1)')
	parser.add_argument('--work-dir', metavar='DIR',
		help='directory for fixtures and run copies; its fixture cache is kept (default: a new tsvzbench-* directory here, removed at exit)')
	parser.add_argument('--keep', action='store_true', help='keep the work directory with the last copy of every run')
	parser.add_argument('--json', metavar='PATH', help='also write every raw sample to PATH')
	parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version)
	args = parser.parse_args(argv)
	impls = []
	for spec in args.impl:
		name, sep, command = spec.partition('=')
		if not sep or not re.match(r'[A-Za-z0-9_.-]+\Z', name) or not command.strip():
			parser.error('--impl takes NAME=COMMAND, NAME made of letters, digits, _ . -: %r' % spec)
		if name in [n for n, _ in impls]:
			parser.error('--impl name given twice: %s' % name)
		impls.append((name, command))
	if not impls:
		parser.error('give at least one --impl NAME=COMMAND')
	wanted = [f for f in args.only.split(',') if f]
	if not wanted or any(f not in FAMILIES for f in wanted):
		parser.error('--only takes families from %s' % ','.join(FAMILIES))
	if args.repeat is not None and args.repeat < 1:
		parser.error('--repeat must be at least 1')
	if args.timeout is not None and args.timeout <= 0:
		parser.error('--timeout must be positive')
	params = dict(SUITES[args.suite])
	if args.repeat is not None:
		params['repeat'] = args.repeat
	if args.timeout is not None:
		params['timeout'] = args.timeout
	return argparse.Namespace(impls=impls, suite=args.suite, params=params,
		families=tuple(f for f in FAMILIES if f in wanted), seed=args.seed,
		work_dir=args.work_dir, keep=args.keep, json=args.json)


# ---------------------------------------------------------------------------
# Spec §13 field codec and the replay model (§7)
# ---------------------------------------------------------------------------

DELIM = '\t'
_TOKEN_RE = re.compile(r'<(sep|LF|lt|#)>')
_TOKEN_MAP = {'LF': '\n', 'lt': '<', '#': '#'}
_RESERVED_RE = re.compile(r'#_[A-Za-z0-9_-]+_#\Z')
_COMPRESSION = ('.gz', '.gzip', '.bz2', '.bzip2', '.xz', '.lzma', '.zst', '.zstd')


def decode_field(text, delimiter=DELIM):
	"""Decode §13 tokens; any other <...> passes through literally."""
	if '<' not in text:
		return text
	return _TOKEN_RE.sub(lambda m: delimiter if m.group(1) == 'sep' else _TOKEN_MAP[m.group(1)], text)


def encode_field(value, delimiter=DELIM, is_key=False):
	"""Encode one field per §13.3; a key also gets <#> for a leading '#'."""
	value = value.replace('<', '<lt>').replace(delimiter, '<sep>').replace('\n', '<LF>')
	if is_key and value.startswith('#'):
		value = '<#>' + value[1:]
	return value


def record(key, cells):
	"""One record line (with its '\\n') for key and value cells."""
	return DELIM.join([encode_field(key, is_key=True)] + [encode_field(c) for c in cells]) + '\n'


def trim(cells):
	"""Drop trailing empty cells: the gate's normal form of a row (§3.6)."""
	end = len(cells)
	while end and cells[end - 1] == '':
		end -= 1
	return cells[:end]


def _bool(value, default):
	value = value.lower()
	if value == 'true':
		return True
	if value == 'false':
		return False
	return default


class Model(object):
	"""Replay of strict TSVZ lines (§7, version 1).

	Each live key keeps its raw value fields and the index of the marker state
	in force at its row, so §14 resolution happens when a row is read.
	"""

	def __init__(self):
		self.rows = {}
		self.defaults = ()
		self.strip = True
		self.fill_empty = False
		self.return_defaults = True
		self.states = []
		self._state = None

	def _state_index(self):
		if self._state is None:
			self.states.append((self.defaults, self.strip, self.fill_empty))
			self._state = len(self.states) - 1
		return self._state

	def apply_line(self, raw):
		"""Apply one committed record, given without its '\\n'."""
		if raw.endswith('\r'):
			raw = raw[:-1]
		first, sep, rest = raw.partition(DELIM)
		if first.startswith('#'):
			if _RESERVED_RE.match(first):
				self._marker(first.lower(), rest.split(DELIM) if sep else [])
			return
		if self.strip:
			first = first.rstrip(' \t')
		key = decode_field(first)
		if key == '':
			return
		if not sep:
			self.rows.pop(key, None)
			return
		self.rows[key] = (rest, self._state_index())

	def _marker(self, key, values):
		value = decode_field(values[0]) if values else ''
		if key == '#_defaults_#':
			self.defaults = tuple(decode_field(v) for v in values)
		elif key == '#_strip_trailing_whites_#':
			self.strip = _bool(value, True)
		elif key == '#_fill_empty_with_default_#':
			self.fill_empty = _bool(value, False)
		elif key == '#_return_defaults_when_missing_#':
			self.return_defaults = _bool(value, True)
			return
		else:
			return
		self._state = None

	def replay(self, data):
		"""Apply every committed line of one part's bytes (§4.3: bytes after the last '\\n' are dropped)."""
		end = data.rfind(b'\n')
		if end < 0:
			return self
		for line in data[:end].decode('utf-8', 'surrogateescape').split('\n'):
			self.apply_line(line)
		return self

	def replay_file(self, path):
		opener = gzip.open if path.endswith(('.gz', '.gzip')) else open
		with opener(path, 'rb') as f:
			return self.replay(f.read())

	def resolve(self, key):
		"""The resolved value cells of a live key, trimmed (§14.2, §14.3)."""
		rest, index = self.rows[key]
		defaults, strip, fill_empty = self.states[index]
		cells = []
		for j, field in enumerate(rest.split(DELIM)):
			if strip:
				field = field.rstrip(' \t')
			cell = decode_field(field)
			if cell == '' and fill_empty and j < len(defaults):
				cell = defaults[j]
			cells.append(cell)
		cells.extend(defaults[len(cells):])
		return trim(cells)

	def missing(self):
		"""What a read of a missing key returns (§14.5): the current defaults, or None."""
		return trim(list(self.defaults)) if self.return_defaults else None

	def iter_rows(self):
		for key in self.rows:
			yield [key] + self.resolve(key)


def store_parts(store):
	"""The parts of a store in replay order (§17): the unnumbered file, then numbered parts by hex ordinal; .rotated skipped."""
	base = store
	for suffix in _COMPRESSION:
		if base.endswith(suffix):
			base = base[:-len(suffix)]
			break
	directory = os.path.dirname(base) or '.'
	prefix = os.path.basename(base) + '.'
	numbered = []
	for name in os.listdir(directory):
		if not name.startswith(prefix):
			continue
		match = re.match(r'([0-9A-Fa-f]+)(\.rotated)?(\.[A-Za-z0-9]+)?\Z', name[len(prefix):])
		if not match or match.group(2) or (match.group(3) and match.group(3) not in _COMPRESSION):
			continue
		numbered.append((int(match.group(1), 16), os.path.join(directory, name)))
	parts = [store] if os.path.exists(store) else []
	return parts + [path for _, path in sorted(numbered)]


def read_store(store):
	"""The model of a store as its files are now."""
	model = Model()
	for part in store_parts(store):
		model.replay_file(part)
	return model


def store_bytes(store):
	return sum(os.path.getsize(part) for part in store_parts(store))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_B64_TABLE = bytes.maketrans(b'+/', b'xz')
_KEY_MULT = 0x9E3779B97F4B
_KEY_MASK = (1 << 48) - 1
HEADER = '#key\tc1\tc2\tc3\tc4\tc5\n'
MULTIPART_ORDINALS = ('f', '10', 'ff', '100', '0192a3b4c5d67e8f9a0b1c2d3e4f5061',
	'0192a3b4c5d67e8f9a0b1c2d3e4f5062', '0192a3b4c5d67e8f9a0b1c2d3e4f5063')
_SPECIALS = ('\t', '\n', '<', '<sep>', '#', 'é', 'ß', '漢字', '😀')


def key_of(index):
	"""Key number `index`: 12 hex digits of an odd-multiplier permutation of [0, 2**48), so unique and unsorted."""
	return '%012x' % ((index * _KEY_MULT + 0x5DEECE66D) & _KEY_MASK)


def random_values(rng, count=5):
	"""`count` random [A-Za-z0-9] cells of 8 to 15 characters."""
	text = base64.b64encode(rng.getrandbits(480).to_bytes(60, 'big')).translate(_B64_TABLE).decode('ascii')
	return [text[16 * j:16 * j + 8 + rng.getrandbits(3)] for j in range(count)]


def gen_plain(rng, rows, base=0):
	yield HEADER
	for i in range(rows):
		yield record(key_of(base + i), random_values(rng))


def gen_churn(rng, rows):
	"""About 4*rows data lines leaving exactly `rows` live keys: overwrites, 20% tombstones, re-inserts."""
	pool = rows + (3 * rows) // 5
	reinsert = rows // 5
	tomb = (pool - rows) + reinsert
	overwrites = 4 * rows - pool - tomb - reinsert
	early = overwrites - overwrites // 3
	yield HEADER
	for i in range(pool):
		yield record(key_of(i), random_values(rng))
	for _ in range(early):
		yield record(key_of(rng.randrange(pool)), random_values(rng))
	dead = rng.sample(range(pool), tomb)
	for i in dead:
		yield encode_field(key_of(i), is_key=True) + '\n'
	for i in dead[:reinsert]:
		yield record(key_of(i), random_values(rng))
	gone = set(dead[reinsert:])
	live = [i for i in range(pool) if i not in gone]
	for _ in range(overwrites - early):
		yield record(key_of(rng.choice(live)), random_values(rng))


def gen_escaped(rng, rows):
	"""Like plain, with §13 specials in ~30% of cells and a leading '#' in ~2% of keys."""
	yield HEADER
	for i in range(rows):
		key = key_of(i)
		if rng.random() < 0.02:
			key = '#' + key[1:]
		cells = random_values(rng)
		for j, cell in enumerate(cells):
			if rng.random() < 0.3:
				at = rng.randrange(len(cell) + 1)
				cells[j] = cell[:at] + rng.choice(_SPECIALS) + cell[at:]
		yield record(key, cells)


def _sparse_row(rng, key):
	cells = []
	for value in random_values(rng)[:1 + rng.randrange(5)]:
		roll = rng.random()
		cells.append('' if roll < 0.2 else value + ' ' if roll < 0.3 else value)
	return record(key, cells)


def gen_markers(rng, rows):
	"""Defaults, fill-empty and sparse rows; defaults change halfway, stripping stops for the last quarter."""
	yield HEADER
	yield '#_version_#\t1\n'
	yield '#_defaults_#\t' + '\t'.join(random_values(rng)) + '\n'
	yield '#_fill_empty_with_default_#\ttrue\n'
	for i in range(rows):
		if i == rows // 2:
			yield '#_defaults_#\t' + '\t'.join(random_values(rng)) + '\n'
			for _ in range(rows // 20):
				yield _sparse_row(rng, key_of(rng.randrange(i)))
		if i == (3 * rows) // 4:
			yield '#_strip_trailing_whites_#\tfalse\n'
		yield _sparse_row(rng, key_of(i))


def gen_checksum(rng, rows, crc_every=1000, sha_every=1500, sha_offset=500):
	"""Plain rows with staggered crc32 and sha256 segments carrying correct digests (§15)."""
	accs = {'crc32': None, 'sha256': None}

	def feed(line, skip):
		data = line.encode('utf-8')
		for algo, acc in accs.items():
			if acc is None or algo == skip:
				continue
			if algo == 'crc32':
				accs[algo] = zlib.crc32(data, acc)
			else:
				acc.update(data)

	def marker(algo):
		acc = accs[algo]
		if acc is None:
			value = ''
		elif algo == 'crc32':
			value = '%08x' % (acc & 0xffffffff)
		else:
			value = acc.hexdigest()
		line = '#_checksum_%s_#\t%s\n' % (algo, value)
		feed(line, algo)
		accs[algo] = 0 if algo == 'crc32' else hashlib.sha256()
		return line

	yield HEADER
	for i in range(rows):
		if i % crc_every == 0:
			yield marker('crc32')
		if i >= sha_offset and (i - sha_offset) % sha_every == 0:
			yield marker('sha256')
		line = record(key_of(i), random_values(rng))
		feed(line, None)
		yield line
	yield marker('crc32')
	yield marker('sha256')


def write_lines(path, lines, compress=False):
	with open(path, 'wb') as raw:
		out = gzip.GzipFile(filename='', mode='wb', fileobj=raw, compresslevel=6, mtime=0) if compress else raw
		try:
			for chunk in iter(lambda: list(itertools.islice(lines, 4096)), []):
				out.write(''.join(chunk).encode('utf-8'))
		finally:
			if compress:
				out.close()


def _make_multipart(rng, rows, directory, store_name):
	lines = gen_plain(rng, rows)
	header = next(lines)
	chunk = -(-rows // 8)
	names = [store_name] + [store_name + '.' + ordinal for ordinal in MULTIPART_ORDINALS]
	for index, name in enumerate(names):
		write_lines(os.path.join(directory, name), itertools.chain([header] if index == 0 else [], itertools.islice(lines, chunk)))
	rotated = (record(key_of(ROTATED_KEY_BASE + i), random_values(rng)) for i in range(10))
	write_lines(os.path.join(directory, store_name + '.11.rotated'), rotated)


_MAKERS = {
	'plain': lambda rng, rows, d, name: write_lines(os.path.join(d, name), gen_plain(rng, rows)),
	'churn': lambda rng, rows, d, name: write_lines(os.path.join(d, name), gen_churn(rng, rows)),
	'escaped': lambda rng, rows, d, name: write_lines(os.path.join(d, name), gen_escaped(rng, rows)),
	'markers': lambda rng, rows, d, name: write_lines(os.path.join(d, name), gen_markers(rng, rows)),
	'checksum': lambda rng, rows, d, name: write_lines(os.path.join(d, name), gen_checksum(rng, rows)),
	'gz': lambda rng, rows, d, name: write_lines(os.path.join(d, name), gen_plain(rng, rows), compress=True),
	'multipart': _make_multipart,
	'bulk-new': lambda rng, rows, d, name: write_lines(os.path.join(d, name),
		(record(key_of(NEW_KEY_BASE + i), random_values(rng)) for i in range(rows))),
	'bulk-add': lambda rng, rows, d, name: write_lines(os.path.join(d, name),
		(record(key_of(NEW_KEY_BASE + i), random_values(rng)) for i in range(max(1, rows // 100)))),
	'bulk-del': lambda rng, rows, d, name: write_lines(os.path.join(d, name),
		(encode_field(key_of(i), is_key=True) + '\n' for i in rng.sample(range(rows), max(1, rows // 10)))),
}
_SIZE_FACTOR = {'churn': 4.0, 'bulk-add': 0.01, 'bulk-del': 0.2}


class Fixture(object):
	"""One generated store or bulk input: `files` in `directory`; a tool is given `store_name`."""

	def __init__(self, kind, rows, directory, store_name):
		self.kind = kind
		self.rows = rows
		self.directory = directory
		self.store_name = store_name
		self.files = sorted(name for name in os.listdir(directory) if name != 'complete')

	@property
	def path(self):
		return os.path.join(self.directory, self.store_name)

	def copy_to(self, run_dir):
		for name in self.files:
			shutil.copyfile(os.path.join(self.directory, name), os.path.join(run_dir, name))
		return os.path.join(run_dir, self.store_name)

	def model(self):
		return read_store(self.path)


class Fixtures(object):
	"""The fixture cache under `directory`, one subdirectory per (kind, rows, seed, generator version)."""

	def __init__(self, directory, seed):
		self.directory = directory
		self.seed = seed

	def _stem(self, kind, rows):
		return '%s-%d-s%d-g%d' % (kind, rows, self.seed, GENERATOR_VERSION)

	def cached(self, kind, rows):
		return os.path.exists(os.path.join(self.directory, self._stem(kind, rows), 'complete'))

	def get(self, kind, rows):
		stem = self._stem(kind, rows)
		directory = os.path.join(self.directory, stem)
		store_name = 'gz.tsvz.gz' if kind == 'gz' else kind + '.tsvz'
		if not self.cached(kind, rows):
			shutil.rmtree(directory, ignore_errors=True)
			os.makedirs(directory)
			_MAKERS[kind](random.Random(stem), rows, directory, store_name)
			open(os.path.join(directory, 'complete'), 'w').close()
		return Fixture(kind, rows, directory, store_name)

	def estimate(self, kind, rows):
		"""Bytes a fixture not yet cached will take."""
		if self.cached(kind, rows):
			return 0
		return int(rows * ROW_BYTES * _SIZE_FACTOR.get(kind, 1.0))


def planned_fixtures(params, families):
	"""Every (kind, rows) fixture the selected families use."""
	sizes, mid = params['sizes'], params['mid']
	planned = []
	if 'cold' in families:
		planned += [('plain', rows) for rows in sizes] + [(shape, mid) for shape in FEATURE_SHAPES]
	if 'bulk' in families:
		planned += [('bulk-new', rows) for rows in sizes] + [('plain', mid), ('bulk-add', mid), ('bulk-del', mid)]
	if 'perop' in families:
		planned.append(('plain', mid))
	if 'maint' in families:
		planned += [('churn', mid), ('markers', mid), ('plain', mid), ('multipart', mid)]
	seen = []
	for item in planned:
		if item not in seen:
			seen.append(item)
	return seen


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

def _output_row(line):
	cells = [decode_field(cell) for cell in line.split(DELIM)]
	return [cells[0]] + trim(cells[1:])


def output_rows(data):
	"""Rows of records-format output (§20.4.1): decoded, trailing empty cells trimmed."""
	text = data.decode('utf-8', 'surrogateescape')
	if text.endswith('\n'):
		text = text[:-1]
	if not text:
		return []
	return [_output_row(line) for line in text.split('\n')]


def iter_output_rows(path):
	"""output_rows of a file, one line at a time, so a large `read` is never held whole."""
	with open(path, 'rb') as f:
		for raw in f:
			line = raw.decode('utf-8', 'surrogateescape')
			yield _output_row(line[:-1] if line.endswith('\n') else line)


def _show(row):
	text = repr(row)
	return text if len(text) <= 80 else text[:77] + '...'


def compare_rows(got, expected):
	"""None when the two row sequences are equal, else a description of the first difference."""
	for index, (have, want) in enumerate(itertools.zip_longest(got, expected), 1):
		if have == want:
			continue
		if have is None:
			return 'row %d missing: expected %s' % (index, _show(want))
		if want is None:
			return 'extra row %d: %s' % (index, _show(have))
		return 'row %d: got %s, expected %s' % (index, _show(have), _show(want))
	return None


def _rows_mark(difference):
	return (WRONG, difference) if difference else (OK, '')


def _read(path):
	with open(path, 'rb') as f:
		return f.read()


def check_exit_only(ctx):
	return OK, ''


def check_read(ctx):
	return _rows_mark(compare_rows(iter_output_rows(ctx.stdout), ctx.fixture.model().iter_rows()))


def check_len(ctx):
	got = _read(ctx.stdout).decode('utf-8', 'replace').strip()
	want = str(len(ctx.fixture.model().rows))
	return (OK, '') if got == want else (WRONG, 'len printed %r, expected %s' % (got[:40], want))


def check_get(ctx):
	model = ctx.fixture.model()
	live, missing = ctx.case.args
	want = [[live] + model.resolve(live)]
	if model.missing() is not None:
		want.append([missing] + model.missing())
	return _rows_mark(compare_rows(output_rows(_read(ctx.stdout)), want))


def check_no_output(ctx):
	data = _read(ctx.stdout)
	return (OK, '') if not data else (WRONG, 'unexpected output: %r' % data[:60])


def expected_post(ctx):
	model = ctx.fixture.model() if ctx.fixture else Model()
	if ctx.stdin:
		model.replay_file(ctx.stdin)
	return model


def check_post(ctx):
	return _rows_mark(compare_rows(read_store(ctx.store).iter_rows(), expected_post(ctx).iter_rows()))


def check_scrub(ctx):
	mark = check_post(ctx)
	if mark[0] != OK:
		return mark
	after = store_bytes(ctx.store)
	if after >= ctx.before_bytes:
		return WRONG, 'not smaller: %d bytes before, %d after' % (ctx.before_bytes, after)
	return OK, ''


def check_clear(ctx):
	return _rows_mark(compare_rows(read_store(ctx.store).iter_rows(), []))


def _hashes(run_dir, names):
	return {name: hashlib.sha256(_read(os.path.join(run_dir, name))).hexdigest()
		for name in names if os.path.exists(os.path.join(run_dir, name))}


def check_scrub_parts(ctx):
	"""scrub of a multi-part store: compacting it or refusing it untouched (§20.2.1) are both allowed."""
	if ctx.sample.exit == 0:
		return check_post(ctx)
	if ctx.sample.exit == 1:
		if _hashes(ctx.run_dir, ctx.fixture.files) == ctx.before_hashes:
			return REFUSED, 'exit 1, parts unchanged'
		return WRONG, 'exit 1 but the parts changed'
	return EXIT, str(ctx.sample.exit)


# ---------------------------------------------------------------------------
# Running children
# ---------------------------------------------------------------------------

_RSS_SCALE = 1 if sys.platform == 'darwin' else 1024
LIVE_GROUPS = set()

# Every command runs as the child of this launcher, a fresh `python -S -I`. On
# exec, Linux folds the old memory map's high-water mark into the new program's
# ru_maxrss, and a forked child's map is a copy of its parent's: started from
# the harness, a child would report the harness's own size. Started from the
# launcher it reports at least the launcher's small map (its `floor`, VmHWM:
# the launcher's own ru_maxrss carries the harness's size the same way), and
# its own peak above that. The launcher also times the child, so its own
# start-up is not measured.
_LAUNCHER = '''
import os, sys, time
fd = int(sys.argv[1])
argv = sys.argv[2:]
try:
	with open('/proc/self/status') as status:
		floor = [int(line.split()[1]) for line in status if line.startswith('VmHWM:')][0]
except (OSError, IndexError):
	import resource
	floor = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
start = time.perf_counter()
pid = os.fork()
if pid == 0:
	os.close(fd)
	try:
		os.execvp(argv[0], argv)
	except OSError as error:
		os.write(2, ('cannot run %s: %s\\n' % (argv[0], error)).encode())
	os._exit(127)
_, status, usage = os.wait4(pid, 0)
wall = time.perf_counter() - start
code = os.WEXITSTATUS(status) if os.WIFEXITED(status) else -os.WTERMSIG(status)
os.write(fd, ('%r %r %r %d %d %d %d %d %d %d' % (wall, usage.ru_utime, usage.ru_stime, usage.ru_maxrss,
	usage.ru_inblock, usage.ru_oublock, usage.ru_nvcsw, usage.ru_nivcsw, code, floor)).encode())
'''


class Sample(object):
	"""What one child run cost: wall seconds, rusage, exit status.

	rss_floor is the launcher's own size; a maxrss near it (or a little below)
	only says the child's true peak is no higher than that.
	"""
	FIELDS = ('wall', 'utime', 'stime', 'maxrss', 'rss_floor', 'inblock', 'oublock', 'nvcsw', 'nivcsw', 'exit',
		'timed_out', 'store_bytes')

	def __init__(self, **values):
		for name in self.FIELDS:
			setattr(self, name, values.get(name))

	@property
	def cpu(self):
		return (self.utime or 0.0) + (self.stime or 0.0)

	def as_dict(self):
		return {name: getattr(self, name) for name in self.FIELDS}


def exit_code(status):
	if os.WIFEXITED(status):
		return os.WEXITSTATUS(status)
	if os.WIFSIGNALED(status):
		return -os.WTERMSIG(status)
	return -1


def kill_group(pid, sig=signal.SIGKILL):
	try:
		os.killpg(pid, sig)
	except (ProcessLookupError, PermissionError):
		pass


def _read_all(fd):
	chunks = []
	while True:
		chunk = os.read(fd, 65536)
		if not chunk:
			return b''.join(chunks)
		chunks.append(chunk)


class Child(object):
	"""A command started through the launcher in a session of its own; `sample` is set once it is reaped."""

	def __init__(self, argv, cwd, stdin_path=None, stdout_path=None, stderr_path=None):
		self.read_fd, write_fd = os.pipe()
		stdin = open(stdin_path or os.devnull, 'rb')
		stdout = open(stdout_path or os.devnull, 'wb')
		stderr = open(stderr_path or os.devnull, 'wb')
		try:
			self.started = time.perf_counter()
			self.proc = subprocess.Popen([sys.executable, '-S', '-I', '-c', _LAUNCHER, str(write_fd)] + list(argv),
				cwd=cwd, stdin=stdin, stdout=stdout, stderr=stderr, pass_fds=(write_fd,), start_new_session=True)
		except BaseException:
			os.close(self.read_fd)
			raise
		finally:
			os.close(write_fd)
			stdin.close()
			stdout.close()
			stderr.close()
		LIVE_GROUPS.add(self.proc.pid)
		self.sample = None

	@property
	def pid(self):
		return self.proc.pid

	def poll(self):
		"""True once the launcher has exited (and the sample is set)."""
		if self.sample is None:
			pid, status, _ = os.wait4(self.proc.pid, os.WNOHANG)
			if pid:
				self._finish(status, False)
		return self.sample is not None

	def wait(self, timeout, first_signal=signal.SIGKILL):
		"""Reap the launcher; after `timeout` signal the group (SIGTERM first means SIGKILL 5 s later)."""
		if self.sample is not None:
			return self.sample
		fired = threading.Event()

		def expire(sig):
			fired.set()
			kill_group(self.proc.pid, sig)

		timers = [threading.Timer(timeout, expire, (first_signal,))]
		if first_signal != signal.SIGKILL:
			timers.append(threading.Timer(timeout + 5.0, expire, (signal.SIGKILL,)))
		for timer in timers:
			timer.daemon = True
			timer.start()
		try:
			_, status, _ = os.wait4(self.proc.pid, 0)
		except BaseException:
			kill_group(self.proc.pid)
			os.wait4(self.proc.pid, 0)
			self.proc.returncode = -signal.SIGKILL
			LIVE_GROUPS.discard(self.proc.pid)
			os.close(self.read_fd)
			raise
		finally:
			for timer in timers:
				timer.cancel()
		return self._finish(status, fired.is_set())

	def _finish(self, status, timed_out):
		self.proc.returncode = exit_code(status)
		LIVE_GROUPS.discard(self.proc.pid)
		fields = _read_all(self.read_fd).split()
		os.close(self.read_fd)
		if len(fields) != 10:
			self.sample = Sample(wall=time.perf_counter() - self.started, exit=self.proc.returncode, timed_out=timed_out)
			return self.sample
		wall, utime, stime = (float(f) for f in fields[:3])
		maxrss, inblock, oublock, nvcsw, nivcsw, code, floor = (int(f) for f in fields[3:])
		self.sample = Sample(wall=wall, utime=utime, stime=stime, maxrss=maxrss * _RSS_SCALE, rss_floor=floor * _RSS_SCALE,
			inblock=inblock, oublock=oublock, nvcsw=nvcsw, nivcsw=nivcsw, exit=code, timed_out=timed_out)
		return self.sample


def run_child(argv, cwd, timeout, stdin_path=None, stdout_path=None, stderr_path=None):
	"""Run one command to completion and measure it."""
	return Child(argv, cwd, stdin_path, stdout_path, stderr_path).wait(timeout)


def kill_live_groups():
	for pid in list(LIVE_GROUPS):
		kill_group(pid)
		try:
			os.waitpid(pid, 0)
		except ChildProcessError:
			pass
		LIVE_GROUPS.discard(pid)


atexit.register(kill_live_groups)


# ---------------------------------------------------------------------------
# The handler protocol (§21)
# ---------------------------------------------------------------------------

class HandlerError(Exception):
	pass


def read_pointer(store):
	"""(socket path, pid) from the store's pointer file (§21.2), or None when it is absent or unusable."""
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
			values[name] = decode_field(value)
	address = values.get('address', '')
	if not address.startswith('unix:'):
		return None
	pid = values.get('pid', '')
	return address[5:], int(pid) if pid.isdigit() else None


def request(op, args=(), options=()):
	"""One §21.5 request line: options, the operation, the key (with <#>) and its values."""
	fields = list(options) + [op]
	if args:
		fields.append(encode_field(args[0], is_key=True))
		fields.extend(encode_field(arg) for arg in args[1:])
	return ('\t'.join(fields) + '\n').encode('utf-8')


def response_status(lines):
	text = lines[-1][1:].split(b'\t', 1)[0].strip()
	return int(text) if text.isdigit() else -1


def response_rows(lines):
	return output_rows(b''.join(line for line in lines[:-1] if not line.startswith(b'#!')))


class Client(object):
	"""One connection to a handler: send a request, read its whole response (§21.3.2)."""

	def __init__(self, path, timeout=30.0):
		self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		try:
			self.sock.settimeout(timeout)
			self.sock.connect(path)
		except BaseException:
			self.sock.close()
			raise
		self.rfile = self.sock.makefile('rb')

	def roundtrip(self, payload):
		self.sock.sendall(payload)
		lines = []
		while True:
			line = self.rfile.readline()
			if not line:
				raise ConnectionError('the handler closed the connection')
			lines.append(line)
			if line[:1] == b'#' and line[:2] != b'#!':
				return lines

	def close(self):
		self.rfile.close()
		self.sock.close()


def proc_rss(pid):
	"""Current resident set of `pid` in bytes from /proc, or None where there is no /proc."""
	try:
		with open('/proc/%d/status' % pid) as f:
			for line in f:
				if line.startswith('VmRSS:'):
					return int(line.split()[1]) * 1024
	except (OSError, ValueError, TypeError):
		pass
	return None


class Impl(object):
	"""An implementation under test: a name and the command that runs its tsvz tool.

	Its commands run in `cwd`, the directory tsvzbench was started from, so a
	relative COMMAND resolves as it would in the user's shell; stores are
	always given as absolute paths.
	"""

	def __init__(self, name, command, cwd=None):
		self.name = name
		self.command = command
		self.argv = shlex.split(command)
		self.cwd = cwd or os.getcwd()
		self.version = ''
		self.handler = False
		self.problem = None
		self.handler_problem = None

	def cli(self, op, store, *args):
		"""The canonical §20.1 command line for one operation."""
		return self.argv + ['--format', 'records', '-q', op, store] + list(args)


class Handler(object):
	"""An implementation's own `serve STORE` (§21.13), started and stopped by the harness."""

	def __init__(self, impl, store, run_dir, wait):
		self.impl = impl
		self.store = store
		self.run_dir = run_dir
		self.wait = wait
		self.child = None
		self.path = None
		self.pid = None
		self.load_time = None
		self.rss_after_load = None
		self.sample = None

	def start(self):
		"""Spawn the handler and wait for its first `len` answer; raise HandlerError when it never answers."""
		self.child = Child(self.impl.cli('serve', self.store), self.impl.cwd,
			stdout_path=os.path.join(self.run_dir, 'serve.out'), stderr_path=os.path.join(self.run_dir, 'serve.err'))
		start = self.child.started
		while True:
			if self.child.poll():
				self.sample = self.child.sample
				raise HandlerError('serve exited with status %d' % self.sample.exit)
			pointer = read_pointer(self.store)
			if pointer:
				try:
					client = Client(pointer[0], timeout=self.wait)
					try:
						lines = client.roundtrip(b'len\n')
					finally:
						client.close()
					if response_status(lines) == 0:
						self.path, self.pid = pointer
						self.load_time = time.perf_counter() - start
						self.rss_after_load = proc_rss(self.pid) if self.pid else None
						return
				except OSError:
					pass
			if time.perf_counter() - start > self.wait:
				raise HandlerError('no handler answered within %.0f s' % self.wait)
			time.sleep(0.005)

	def stop(self, wait=10.0):
		"""Send `stop`, then reap the handler (SIGTERM after `wait` s, SIGKILL 5 s later); return its Sample."""
		if self.child is None or self.child.sample is not None:
			return self.sample
		if self.path:
			try:
				client = Client(self.path, timeout=wait)
				try:
					client.roundtrip(b'stop\n')
				finally:
					client.close()
			except OSError:
				pass
		else:
			kill_group(self.child.pid, signal.SIGTERM)
		self.sample = self.child.wait(wait, signal.SIGTERM)
		return self.sample

def client_loop(client, payloads):
	"""Send each payload in turn; return (latencies, responses, start, end)."""
	latencies = []
	responses = []
	start = time.perf_counter()
	for payload in payloads:
		began = time.perf_counter()
		lines = client.roundtrip(payload)
		latencies.append(time.perf_counter() - began)
		responses.append(lines)
	return latencies, responses, start, time.perf_counter()


def check_responses(responses, expected):
	"""None when every status is #0 and every expected row matches, else the first problem."""
	for index, (lines, want) in enumerate(zip(responses, expected), 1):
		status = response_status(lines)
		if status != 0:
			return 'request %d: status %d' % (index, status)
		if want is not None:
			difference = compare_rows(response_rows(lines), [want])
			if difference:
				return 'request %d: %s' % (index, difference)
	return None


def _write_all(fd, data):
	while data:
		data = data[os.write(fd, data):]


def run_clients(path, plans, timeout):
	"""Run one forked client per plan, all released together; each plan is a list of (payload, expected row).

	Returns (latencies, span seconds, problem or None).
	"""
	ready_r, ready_w = os.pipe()
	go_r, go_w = os.pipe()
	children = []
	for plan in plans:
		result_r, result_w = os.pipe()
		pid = os.fork()
		if pid == 0:
			code = 1
			try:
				os.close(ready_r)
				os.close(go_w)
				os.close(result_r)
				try:
					client = Client(path, timeout)
				except OSError as error:
					os.write(ready_w, b'x')
					_write_all(result_w, json.dumps({'problem': 'connect: %s' % error}).encode())
					raise
				os.write(ready_w, b'r')
				os.read(go_r, 1)
				latencies, responses, start, end = client_loop(client, [payload for payload, _ in plan])
				problem = check_responses(responses, [want for _, want in plan])
				_write_all(result_w, json.dumps({'lat': latencies, 'start': start, 'end': end, 'problem': problem}).encode())
				code = 0
			except BaseException as error:
				try:
					_write_all(result_w, json.dumps({'problem': repr(error)}).encode())
				except OSError:
					pass
			finally:
				os._exit(code)
		os.close(result_w)
		children.append((pid, result_r))
	os.close(ready_w)
	os.close(go_r)
	for _ in plans:
		if os.read(ready_r, 1) != b'r':
			break
	os.close(ready_r)
	_write_all(go_w, b'g' * len(plans))
	os.close(go_w)
	latencies, starts, ends, problem = [], [], [], None
	for pid, result_r in children:
		data = _read_all(result_r)
		os.close(result_r)
		os.waitpid(pid, 0)
		try:
			result = json.loads(data.decode('utf-8'))
		except ValueError:
			result = {'problem': 'a client died without a result'}
		problem = problem or result.get('problem')
		latencies.extend(result.get('lat', []))
		if 'start' in result:
			starts.append(result['start'])
			ends.append(result['end'])
	span = max(ends) - min(starts) if starts else 0.0
	return latencies, span, problem


def start_dummy(path):
	"""Fork a server that answers `#0` to every request line (one process per connection); return its pid."""
	server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	server.bind(path)
	server.listen(128)
	pid = os.fork()
	if pid == 0:
		try:
			os.setsid()
			signal.signal(signal.SIGCHLD, signal.SIG_IGN)
			while True:
				conn, _ = server.accept()
				if os.fork() == 0:
					server.close()
					rfile = conn.makefile('rb')
					for _ in rfile:
						conn.sendall(b'#0\n')
					os._exit(0)
				conn.close()
		finally:
			os._exit(0)
	server.close()
	LIVE_GROUPS.add(pid)
	return pid


def stop_dummy(pid):
	kill_group(pid)
	os.waitpid(pid, 0)
	LIVE_GROUPS.discard(pid)


# ---------------------------------------------------------------------------
# Statistics and formatting
# ---------------------------------------------------------------------------

def median(values):
	values = [v for v in values if v is not None]
	return statistics.median(values) if values else None


def spread_pct(values):
	"""Median absolute deviation as a percentage of the median."""
	values = [v for v in values if v is not None]
	if len(values) < 2:
		return None
	middle = statistics.median(values)
	if not middle:
		return None
	return 100.0 * statistics.median([abs(v - middle) for v in values]) / middle


def percentile(values, q):
	if not values:
		return None
	ordered = sorted(values)
	return ordered[min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))]


def geomean(values):
	logs = [math.log(v) for v in values if v and v > 0]
	return math.exp(sum(logs) / len(logs)) if logs else None


def fmt_time(seconds):
	if seconds is None:
		return '—'
	if abs(seconds) >= 1:
		return '%.2f s' % seconds
	if abs(seconds) >= 1e-3:
		return '%.1f ms' % (seconds * 1e3)
	return '%.0f µs' % (seconds * 1e6)


def fmt_net(seconds):
	"""Net wall time; a negative one is within the startup's noise."""
	if seconds is None:
		return ''
	return '≈0' if seconds <= 0 else fmt_time(seconds)


def fmt_bytes(count):
	if count is None:
		return '—'
	value = float(count)
	for unit in ('B', 'KiB', 'MiB', 'GiB'):
		if abs(value) < 1024 or unit == 'GiB':
			return ('%.0f %s' if unit == 'B' else '%.1f %s') % (value, unit)
		value /= 1024


def fmt_rss(value, floor):
	"""Peak RSS; one at the launcher's floor only says the true peak is no higher."""
	if value is None:
		return '—'
	if floor and value <= floor + 256 * 1024:
		return '≤' + fmt_bytes(floor)
	return fmt_bytes(value)


def fmt_rows(rows):
	for size, suffix in ((1000000, 'M'), (1000, 'k')):
		if rows >= size and rows % size == 0:
			return '%d%s' % (rows // size, suffix)
	return str(rows)


def fmt_rate(rate):
	if rate is None:
		return '—'
	if rate >= 1e6:
		return '%.2fM/s' % (rate / 1e6)
	if rate >= 1e3:
		return '%.1fk/s' % (rate / 1e3)
	return '%.0f/s' % rate


def fmt_ratio(value):
	return '—' if value is None else '×%.2f' % value


def mark_text(mark):
	kind, detail = mark
	if kind == OK:
		return '✓'
	if kind == WRONG:
		return '✗'
	if kind == EXIT:
		return '! ' + detail
	if kind == TIMEOUT:
		return '⏱'
	return kind


class Style(object):
	def __init__(self, enabled):
		self.enabled = enabled

	def wrap(self, text, code):
		return '\x1b[%sm%s\x1b[0m' % (code, text) if self.enabled else text


DROP_ORDER = ('±', 'cpu', 'net', 'disk', 'p99', 'rss', 'p95', 'ceiling')


def render_table(columns, rows, width=None, style=None):
	"""columns: [(title, group)]; rows: lists of (text, ansi code or None).

	When the table is wider than `width`, whole column groups go in DROP_ORDER,
	for every implementation at once; a None group always stays.
	"""
	keep = list(range(len(columns)))

	def widths():
		return [max([len(columns[i][0])] + [len(row[i][0]) for row in rows]) for i in keep]

	for group in DROP_ORDER:
		if width is None or sum(widths()) + 3 * (len(keep) - 1) <= width:
			break
		keep = [i for i in keep if columns[i][1] != group]
	sizes = widths()
	lines = [' | '.join(columns[i][0].ljust(size) for i, size in zip(keep, sizes)).rstrip(),
		'-+-'.join('-' * size for size in sizes)]
	for row in rows:
		cells = []
		for i, size in zip(keep, sizes):
			text, code = row[i]
			padded = text.ljust(size)
			cells.append(style.wrap(padded, code) if style and code else padded)
		lines.append(' | '.join(cells).rstrip())
	return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Implementations and workloads
# ---------------------------------------------------------------------------

def probe(impl, directory, timeout):
	"""Record the implementation's -V version and whether its `serve` answers (§21.13)."""
	os.makedirs(directory, exist_ok=True)
	out = os.path.join(directory, 'version.out')
	sample = run_child(impl.argv + ['-V'], impl.cwd, min(timeout, 30.0), stdout_path=out,
		stderr_path=os.path.join(directory, 'version.err'))
	if sample.timed_out or sample.exit != 0:
		impl.problem = '-V failed: %s' % ('timed out' if sample.timed_out else 'exit %d' % sample.exit)
		return
	impl.version = _read(out).decode('utf-8', 'replace').strip().split('\n')[0]
	handler = Handler(impl, os.path.join(directory, 'probe.tsvz'), directory, HANDLER_WAIT)
	try:
		handler.start()
		impl.handler = True
	except HandlerError as error:
		impl.handler_problem = str(error)
	finally:
		handler.stop()


class Case(object):
	"""One workload. kind is 'cli' (one command per run), 'calls' (single-op CLI calls) or 'socket' (§21 loads)."""

	def __init__(self, family, name, kind, shape=None, rows=None, op=None, args=(), stdin=None,
			expect_exit=0, check=check_exit_only, via_handler=False, min_repeat=1):
		self.family = family
		self.name = name
		self.kind = kind
		self.shape = shape
		self.rows = rows
		self.op = op
		self.args = tuple(args)
		self.stdin = stdin
		self.expect_exit = expect_exit
		self.check = check
		self.via_handler = via_handler
		self.min_repeat = min_repeat

	@property
	def optional(self):
		return self.shape in OPTIONAL_SHAPES

	@property
	def ident(self):
		return '%s-%s-%s-%s' % (self.family, re.sub(r'[^A-Za-z0-9]+', '_', self.name), self.shape or 'new', self.rows or 0)

	@property
	def label(self):
		parts = [self.name]
		if self.shape and self.shape != 'plain':
			parts.append(self.shape)
		if self.rows:
			parts.append(fmt_rows(self.rows))
		return ' '.join(parts)


def build_cases(params, families):
	"""Every workload of the selected families, in run order; `startup` always runs first."""
	sizes, mid = params['sizes'], params['mid']
	get_args = (key_of(0), key_of(MISSING_INDEX))
	cases = [Case('cold', 'startup', 'cli', min_repeat=5)]
	if 'cold' in families:
		for rows in sizes:
			cases.append(Case('cold', 'read', 'cli', 'plain', rows, 'read', check=check_read))
			cases.append(Case('cold', 'len', 'cli', 'plain', rows, 'len', check=check_len))
			cases.append(Case('cold', 'get', 'cli', 'plain', rows, 'get', get_args, expect_exit=3, check=check_get))
		for shape in FEATURE_SHAPES:
			cases.append(Case('cold', 'read', 'cli', shape, mid, 'read', check=check_read))
		cases.append(Case('cold', 'verify', 'cli', 'checksum', mid, 'verify', check=check_no_output))
	if 'bulk' in families:
		for rows in sizes:
			cases.append(Case('bulk', 'set - new', 'cli', None, rows, 'set', ('-',), ('bulk-new', rows), check=check_post))
		cases.append(Case('bulk', 'set - add', 'cli', 'plain', mid, 'set', ('-',), ('bulk-add', mid), check=check_post))
		cases.append(Case('bulk', 'delete -', 'cli', 'plain', mid, 'delete', ('-',), ('bulk-del', mid), check=check_post))
	if 'perop' in families:
		cases.append(Case('perop', 'cli', 'calls', 'plain', mid))
		cases.append(Case('perop', 'cli via handler', 'calls', 'plain', mid, via_handler=True))
		cases.append(Case('perop', 'socket', 'socket', 'plain', mid))
	if 'maint' in families:
		cases.append(Case('maint', 'scrub', 'cli', 'churn', mid, 'scrub', check=check_scrub))
		cases.append(Case('maint', 'scrub', 'cli', 'markers', mid, 'scrub', check=check_post))
		cases.append(Case('maint', 'clear', 'cli', 'plain', mid, 'clear', check=check_clear))
		cases.append(Case('maint', 'scrub', 'cli', 'multipart', mid, 'scrub', check=check_scrub_parts))
	return cases


class Result(object):
	"""What one (workload, implementation) produced: the gate mark, timed samples and latency series."""

	def __init__(self, case, impl):
		self.case = case
		self.impl = impl
		self.mark = (NA, '')
		self.samples = []
		self.series = {}
		self.extra = {}
		self.log = ''
		self.drift = False

	def add_series(self, name, latencies, ops=None, rss=None, floors=None):
		entry = self.series.setdefault(name, {'lat': [], 'ops': [], 'rss': [], 'floor': []})
		entry['lat'].extend(latencies)
		if ops is not None:
			entry['ops'].append(ops)
		if rss:
			entry['rss'].extend(rss)
		if floors:
			entry['floor'].extend(floors)

	def add_extra(self, name, value):
		if value is not None:
			self.extra.setdefault(name, []).append(value)


class RunContext(object):
	"""What a gate check sees of one run."""

	def __init__(self, case, sample, run_dir, store, stdout, fixture, stdin):
		self.case = case
		self.sample = sample
		self.run_dir = run_dir
		self.store = store
		self.stdout = stdout
		self.fixture = fixture
		self.stdin = stdin
		self.before_bytes = 0
		self.before_hashes = {}


class SocketPlan(object):
	"""Deterministic §21 request lists for a mid-size plain store, with the rows each `get` must return."""

	def __init__(self, fixture, params):
		model = fixture.model()
		rng = random.Random('socket-%d' % fixture.rows)
		rows = fixture.rows
		self.gets = []
		for _ in range(params['seq']):
			key = key_of(rng.randrange(rows))
			self.gets.append((request('get', (key,)), [key] + model.resolve(key)))
		self.sets = []
		for j in range(params['seq']):
			key = key_of(NEW_KEY_BASE + j) if rng.random() < 0.1 else key_of(rng.randrange(rows))
			cells = random_values(rng)
			self.sets.append((request('set', [key] + cells), None))
			model.apply_line(record(key, cells)[:-1])
		self.syncs = []
		for _ in range(params['sync']):
			key = key_of(rng.randrange(rows))
			cells = random_values(rng)
			self.syncs.append((request('set', [key] + cells, ('--sync',)), None))
			model.apply_line(record(key, cells)[:-1])
		owned = max(1, rows // max(params['clients']))
		self.mix = {}
		for count in params['clients']:
			plans = []
			for client in range(count):
				keys = [key_of(client * owned + k) for k in range(owned)]
				plan = []
				for j in range(params['per_client']):
					key = keys[(j // 2) % owned]
					if j % 2 == 0:
						cells = random_values(rng)
						plan.append((request('set', [key] + cells), None))
						model.apply_line(record(key, cells)[:-1])
					else:
						plan.append((request('get', (key,)), [key] + model.resolve(key)))
				plans.append(plan)
			self.mix[count] = plans
		self.final = model


class _BlindPlan(object):
	"""A SocketPlan without expected rows: the dummy server's answers carry none."""

	def __init__(self, plan):
		strip = lambda requests: [(payload, None) for payload, _ in requests]
		self.gets = strip(plan.gets)
		self.sets = strip(plan.sets)
		self.syncs = strip(plan.syncs)
		self.mix = {count: [strip(p) for p in plans] for count, plans in plan.mix.items()}


# ---------------------------------------------------------------------------
# The benchmark: running
# ---------------------------------------------------------------------------

def environment(work):
	cpu = platform.processor() or platform.machine()
	try:
		with open('/proc/cpuinfo') as f:
			for line in f:
				if line.startswith('model name'):
					cpu = line.split(':', 1)[1].strip()
					break
	except OSError:
		pass
	fs = 'unknown'
	try:
		best = ''
		with open('/proc/mounts') as f:
			for line in f:
				fields = line.split()
				mount = fields[1].replace('\\040', ' ')
				if (work == mount or work.startswith(mount.rstrip('/') + '/')) and len(mount) >= len(best):
					best, fs = mount, fields[2]
	except (OSError, IndexError):
		pass
	return {'host': socket.gethostname(), 'cpu': cpu, 'cpus': os.cpu_count(), 'python': platform.python_version(),
		'kernel': '%s %s' % (platform.system(), platform.release()), 'fs': fs}


def fs_warning(fs):
	"""A warning when the work directory is on a FUSE file system (mergerfs and the like), else None."""
	if fs.startswith('fuse'):
		return ('the work directory is on %s: file operations there cost more than on the disk below it, '
			'which can hide differences; consider --work-dir on a directly mounted file system' % fs)
	return None


class Bench(object):
	def __init__(self, opts, out=None):
		self.opts = opts
		self.params = opts.params
		self.out = out or sys.stdout
		self.impls = [Impl(name, command) for name, command in opts.impls]
		self.results = []
		self.ceiling = None
		self.work = None
		self.own_work = False
		self.started = time.strftime('%Y-%m-%dT%H:%M:%S%z')

	def setup(self):
		if self.opts.work_dir:
			self.work = os.path.abspath(self.opts.work_dir)
			os.makedirs(self.work, exist_ok=True)
		else:
			self.work = tempfile.mkdtemp(prefix='tsvzbench-', dir=os.getcwd())
			self.own_work = True
		self.fixtures = Fixtures(os.path.join(self.work, 'fixtures'), self.opts.seed)
		os.makedirs(self.fixtures.directory, exist_ok=True)

	def preflight(self):
		"""None when the work directory has room for the suite, else a message."""
		planned = planned_fixtures(self.params, self.opts.families)
		need = sum(self.fixtures.estimate(kind, rows) for kind, rows in planned)
		need += 4 * max(self.params['sizes']) * ROW_BYTES
		need = int(need * 1.2)
		stat = os.statvfs(self.work)
		free = stat.f_bavail * stat.f_frsize
		if need > free:
			return 'the %s suite needs about %s in %s; %s is free' % (self.opts.suite, fmt_bytes(need), self.work, fmt_bytes(free))
		return None

	def cleanup(self):
		kill_live_groups()
		if self.work and not self.opts.keep:
			if self.own_work:
				shutil.rmtree(self.work, ignore_errors=True)
			else:
				shutil.rmtree(os.path.join(self.work, 'runs'), ignore_errors=True)
				shutil.rmtree(os.path.join(self.work, 'probe'), ignore_errors=True)

	def prepare(self):
		"""Work directory, preflight, probes and fixtures; returns a problem message or None."""
		self.setup()
		problem = self.preflight()
		if problem:
			return problem
		warning = fs_warning(environment(self.work)['fs'])
		if warning:
			eprint('tsvzbench: ' + warning)
		for impl in self.impls:
			eprint('tsvzbench: probing %s' % impl.name)
			probe(impl, os.path.join(self.work, 'probe', impl.name), self.params['timeout'])
		for kind, rows in planned_fixtures(self.params, self.opts.families):
			if not self.fixtures.cached(kind, rows):
				eprint('tsvzbench: generating %s %s' % (kind, fmt_rows(rows)))
			self.fixtures.get(kind, rows)
		return None

	def run_cases(self, cases):
		for number, case in enumerate(cases, 1):
			eprint('tsvzbench: [%d/%d] %s %s' % (number, len(cases), case.family, case.label))
			if case.kind == 'cli':
				self.run_cli_case(case)
			elif case.kind == 'calls':
				self.run_calls_case(case)
			else:
				self.run_socket_case(case)

	def active(self):
		return [impl for impl in self.impls if impl.problem is None]

	def _new_results(self, case):
		results = {}
		for impl in self.impls:
			result = Result(case, impl)
			if impl.problem:
				result.mark = (NA, 'skipped: ' + impl.problem)
			results[impl.name] = result
			self.results.append(result)
		return results

	def _run_dir(self, impl, case):
		run_dir = os.path.join(self.work, 'runs', impl.name, case.ident)
		shutil.rmtree(run_dir, ignore_errors=True)
		os.makedirs(run_dir)
		return run_dir

	def _rounds(self, results, once, min_repeat=1):
		"""The warm-up (gated) run of every implementation, then the timed repetitions in rotating order."""
		active = [impl for impl in self.active() if results[impl.name].mark[0] == NA and not results[impl.name].mark[1]]
		for impl in active:
			once(impl, results[impl.name], True)
		timed = [impl for impl in active if results[impl.name].mark[0] not in (TIMEOUT, NA)]
		for rep in range(max(self.params['repeat'], min_repeat)):
			for slot in range(len(timed)):
				impl = timed[(slot + rep) % len(timed)]
				once(impl, results[impl.name], False)

	def _fail_log(self, result, path):
		if os.path.exists(path):
			lines = _read(path).decode('utf-8', 'replace').rstrip('\n').split('\n')
			result.log = '\n'.join(lines[-5:])

	def run_cli_case(self, case):
		results = self._new_results(case)
		fixture = self.fixtures.get(case.shape, case.rows) if case.shape else None
		stdin = self.fixtures.get(*case.stdin).path if case.stdin else None

		def once(impl, result, gate):
			run_dir = self._run_dir(impl, case)
			if fixture:
				store = fixture.copy_to(run_dir)
			else:
				store = os.path.join(run_dir, 'new.tsvz')
			if case.op:
				argv = impl.cli(case.op, store, *case.args)
			else:
				argv = impl.argv + ['-V']
			ctx = RunContext(case, None, run_dir, store, os.path.join(run_dir, 'stdout'), fixture, stdin)
			if case.op:
				ctx.before_bytes = store_bytes(store)
			if case.check is check_scrub_parts:
				ctx.before_hashes = _hashes(run_dir, fixture.files)
			sample = run_child(argv, impl.cwd, self.params['timeout'], stdin_path=stdin,
				stdout_path=ctx.stdout, stderr_path=os.path.join(run_dir, 'stderr'))
			if case.op:
				sample.store_bytes = store_bytes(store)
			ctx.sample = sample
			if gate:
				result.mark = self.judge(case, ctx)
				result.extra['exit'] = sample.exit
				result.extra['before_bytes'] = ctx.before_bytes
				if result.mark[0] in FAILED:
					self._fail_log(result, os.path.join(run_dir, 'stderr'))
			else:
				result.samples.append(sample)
				if sample.exit != result.extra.get('exit'):
					result.drift = True

		self._rounds(results, once, case.min_repeat)

	def judge(self, case, ctx):
		sample = ctx.sample
		if sample.timed_out:
			return TIMEOUT, 'after %.0f s' % self.params['timeout']
		if case.check is not check_scrub_parts and sample.exit != case.expect_exit:
			return EXIT, '%d%s' % (sample.exit, ' (unsupported?)' if case.optional else '')
		try:
			return case.check(ctx)
		except Exception as error:
			return WRONG, 'could not check: %r' % error

	def run_calls_case(self, case):
		results = self._new_results(case)
		fixture = self.fixtures.get(case.shape, case.rows)
		rng = random.Random('calls-%d' % case.rows)
		keys = [key_of(rng.randrange(case.rows)) for _ in range(self.params['calls'])]
		values = [random_values(rng) for _ in keys]
		timeout = self.params['timeout']

		def once(impl, result, gate):
			if case.via_handler and not impl.handler:
				result.mark = (NA, 'no handler')
				return
			run_dir = self._run_dir(impl, case)
			store = fixture.copy_to(run_dir)
			handler = None
			if case.via_handler:
				handler = Handler(impl, store, run_dir, timeout)
				try:
					handler.start()
				except HandlerError as error:
					handler.stop()
					if gate:
						result.mark = (WRONG, 'handler: %s' % error)
						self._fail_log(result, os.path.join(run_dir, 'serve.err'))
					return
			expected = fixture.model() if gate else None
			problem = None
			out, err = os.path.join(run_dir, 'stdout'), os.path.join(run_dir, 'stderr')
			sets, gets = [], []
			try:
				for key, cells in zip(keys, values):
					sample = run_child(impl.cli('set', store, key, *cells), impl.cwd, timeout, stdout_path=out, stderr_path=err)
					sets.append(sample)
					problem = problem or self._call_problem(sample, 0)
					if expected is not None:
						expected.apply_line(record(key, cells)[:-1])
				for key in keys:
					sample = run_child(impl.cli('get', store, key), impl.cwd, timeout, stdout_path=out, stderr_path=err)
					gets.append(sample)
					problem = problem or self._call_problem(sample, 0)
					if expected is not None and not problem:
						difference = compare_rows(output_rows(_read(out)), [[key] + expected.resolve(key)])
						if difference:
							problem = (WRONG, 'get %s: %s' % (key, difference))
			finally:
				if handler:
					handler.stop()
			if gate:
				if not problem:
					difference = compare_rows(read_store(store).iter_rows(), expected.iter_rows())
					problem = (WRONG, 'post-state: %s' % difference) if difference else None
				result.mark = problem or (OK, '')
				if problem:
					self._fail_log(result, err)
				return
			if problem:
				result.drift = True
			for name, samples in (('set', sets), ('get', gets)):
				result.add_series(name, [s.wall for s in samples], len(samples) / sum(s.wall for s in samples),
					[s.maxrss for s in samples], [s.rss_floor for s in samples])

		self._rounds(results, once)

	def _call_problem(self, sample, want):
		if sample.timed_out:
			return (TIMEOUT, 'after %.0f s' % self.params['timeout'])
		if sample.exit != want:
			return (EXIT, str(sample.exit))
		return None

	def socket_plan(self, fixture):
		if getattr(self, '_plan', None) is None:
			self._plan = SocketPlan(fixture, self.params)
		return self._plan

	def _socket_loads(self, path, plan, timeout):
		"""Every §21 load against one server; returns ({name: (latencies, span)}, problem)."""
		series = {}
		problem = None
		client = Client(path, timeout)
		try:
			for name, requests in (('get', plan.gets), ('set', plan.sets), ('--sync set', plan.syncs)):
				latencies, responses, start, end = client_loop(client, [payload for payload, _ in requests])
				series[name] = (latencies, end - start)
				problem = problem or check_responses(responses, [want for _, want in requests])
		finally:
			client.close()
		for count in self.params['clients']:
			latencies, span, trouble = run_clients(path, plan.mix[count], timeout)
			series['mix ×%d' % count] = (latencies, span)
			problem = problem or trouble
		return series, problem

	def run_socket_case(self, case):
		results = self._new_results(case)
		fixture = self.fixtures.get(case.shape, case.rows)
		plan = self.socket_plan(fixture)
		timeout = self.params['timeout']
		self.run_ceiling(plan)

		def once(impl, result, gate):
			if not impl.handler:
				result.mark = (NA, 'no handler')
				return
			run_dir = self._run_dir(impl, case)
			store = fixture.copy_to(run_dir)
			handler = Handler(impl, store, run_dir, timeout)
			problem = None
			try:
				handler.start()
				series, trouble = self._socket_loads(handler.path, plan, timeout)
				problem = (WRONG, trouble) if trouble else None
			except (HandlerError, OSError) as error:
				series, problem = {}, (WRONG, 'handler: %s' % error)
			finally:
				served = handler.stop()
			if gate:
				if not problem:
					difference = compare_rows(read_store(store).iter_rows(), plan.final.iter_rows())
					problem = (WRONG, 'post-state: %s' % difference) if difference else None
				result.mark = problem or (OK, '')
				if problem:
					self._fail_log(result, os.path.join(run_dir, 'serve.err'))
				return
			if problem:
				result.drift = True
			for name, (latencies, span) in series.items():
				result.add_series(name, latencies, len(latencies) / span if span else None)
			result.add_extra('load_time', handler.load_time)
			result.add_extra('rss_after_load', handler.rss_after_load)
			if served is not None and served.maxrss is not None:
				result.add_extra('handler_cpu', served.cpu)
				# rusage peaks are approximate (to about 1 MiB) on recent Linux, while the
				# true peak is at least the RSS /proc showed after load
				result.add_extra('handler_maxrss', max(served.maxrss, handler.rss_after_load or 0))
				result.add_extra('handler_rss_floor', served.rss_floor)

		self._rounds(results, once)

	def run_ceiling(self, plan):
		"""The same loads against a server that answers #0 to everything: where the harness client is the limit."""
		directory = tempfile.mkdtemp(prefix='tsvzb-')
		path = os.path.join(directory, 's')
		ceiling = Result(Case('perop', 'ceiling', 'socket'), None)
		pid = start_dummy(path)
		try:
			for rep in range(self.params['repeat'] + 1):
				series, _ = self._socket_loads(path, _BlindPlan(plan), self.params['timeout'])
				if rep:
					for name, (latencies, span) in series.items():
						ceiling.add_series(name, latencies, len(latencies) / span if span else None)
		finally:
			stop_dummy(pid)
			shutil.rmtree(directory, ignore_errors=True)
		self.ceiling = ceiling

	def exit_status(self):
		if any(impl.problem for impl in self.impls):
			return 1
		if any(result.mark[0] in FAILED for result in self.results):
			return 1
		return 0

	def _startup(self, impl):
		for result in self.results:
			if result.case.name == 'startup' and result.impl is impl:
				return median([s.wall for s in result.samples])
		return None

	def run(self):
		"""The whole benchmark: prepare, every case, the report; returns the exit status."""
		problem = self.prepare()
		if problem:
			eprint('tsvzbench: ' + problem)
			return 1
		self.run_cases(build_cases(self.params, self.opts.families))
		self.report()
		return self.exit_status()

	def report(self, partial=False):
		write = lambda text='': print(text, file=self.out)
		tty = self.out.isatty() if hasattr(self.out, 'isatty') else False
		style = Style(tty)
		width = shutil.get_terminal_size((160, 40)).columns if tty else None
		env = environment(self.work)
		write('tsvzbench %s  %s  suite %s  seed %d%s' % (version, self.started, self.opts.suite, self.opts.seed,
			'  (interrupted: partial results)' if partial else ''))
		write('host %s  cpu %s ×%s  %s  python %s  fs %s  warm page cache' % (env['host'], env['cpu'], env['cpus'],
			env['kernel'], env['python'], env['fs']))
		warning = fs_warning(env['fs'])
		if warning:
			write('warning: ' + warning)
		for impl in self.impls:
			state = impl.problem or ('handler: yes' if impl.handler else 'handler: no (%s)' % (impl.handler_problem or 'not probed'))
			write('  %-12s %s  [%s]  %s  startup %s' % (impl.name, impl.command, impl.version or '?', state, fmt_time(self._startup(impl))))
		for family in FAMILIES:
			if family not in self.opts.families:
				continue
			family_results = [r for r in self.results if r.case.family == family and r.case.name != 'startup']
			if family == 'cold':
				family_results = [r for r in self.results if r.case.family == 'cold']
			if not family_results:
				continue
			write()
			write('[%s]' % family)
			if family == 'perop':
				write(self.perop_table(family_results, width, style))
			else:
				write(self.cli_table(family_results, width, style, disk=family in ('bulk', 'maint')))
		write()
		write(self.footprint())
		write()
		write(self.summary())
		if self.opts.json:
			self.write_json(env)

	def _by_case(self, results):
		cases = []
		for result in results:
			if result.case not in cases:
				cases.append(result.case)
		return [(case, {r.impl.name: r for r in results if r.case is case}) for case in cases]

	def cli_table(self, results, width, style, disk=False):
		names = [impl.name for impl in self.impls]
		columns = [('workload', None)]
		for index, name in enumerate(names):
			columns += [('%s wall' % name, None), ('±', '±'), ('net', 'net'), ('cpu', 'cpu'), ('rss', None)]
			if disk:
				columns.append(('disk', 'disk'))
			columns.append(('ok', None))
			if index:
				columns.append(('×', None))
		rows = []
		for case, by_impl in self._by_case(results):
			base = by_impl[names[0]]
			base_wall = median([s.wall for s in base.samples]) if base.mark[0] == OK else None
			row = [(case.label, None)]
			for index, name in enumerate(names):
				result = by_impl[name]
				samples = result.samples
				wall = median([s.wall for s in samples])
				failed = result.mark[0] in FAILED
				dim = '2' if failed else None
				startup = self._startup(result.impl)
				net = wall - startup if wall is not None and startup is not None and case.op else None
				spread = spread_pct([s.wall for s in samples])
				row += [(fmt_time(wall), dim), ('%.0f%%' % spread if spread is not None else '', dim),
					(fmt_net(net), dim), (fmt_time(median([s.cpu for s in samples])), dim),
					(fmt_rss(median([s.maxrss for s in samples]), median([s.rss_floor for s in samples])), dim)]
				if disk:
					after = fmt_bytes(median([s.store_bytes for s in samples]))
					before = result.extra.get('before_bytes')
					if case.family == 'maint' and before and samples:
						after = '%s→%s' % (fmt_bytes(before), after)
					row.append((after, dim))
				gate = mark_text(result.mark) + ('*' if result.drift else '')
				row.append((gate, '31' if failed else None))
				if index:
					ratio = wall / base_wall if wall and base_wall and result.mark[0] == OK else None
					row.append((fmt_ratio(ratio), None))
			rows.append(row)
		return render_table(columns, rows, width, style)

	def perop_table(self, results, width, style):
		names = [impl.name for impl in self.impls]
		columns = [('workload', None)]
		for index, name in enumerate(names):
			columns += [('%s p50' % name, None), ('p95', 'p95'), ('p99', 'p99'), ('ops/s', None), ('rss', 'rss'), ('ok', None)]
			if index:
				columns.append(('×', None))
		columns.append(('ceiling ops/s', 'ceiling'))
		rows = []
		for case, by_impl in self._by_case(results):
			series_names = []
			for result in by_impl.values():
				for name in result.series:
					if name not in series_names:
						series_names.append(name)
			if not series_names:
				series_names = ['set', 'get'] if case.kind == 'calls' else ['get', 'set', '--sync set'] + ['mix ×%d' % c for c in self.params['clients']]
			for series in series_names:
				row = [('%s %s' % (case.name, series), None)]
				base = by_impl[names[0]].series.get(series)
				base_p50 = percentile(base['lat'], 0.5) if base and by_impl[names[0]].mark[0] == OK else None
				for index, name in enumerate(names):
					result = by_impl[name]
					entry = result.series.get(series, {'lat': [], 'ops': [], 'rss': [], 'floor': []})
					failed = result.mark[0] in FAILED
					dim = '2' if failed else None
					p50 = percentile(entry['lat'], 0.5)
					row += [(fmt_time(p50), dim), (fmt_time(percentile(entry['lat'], 0.95)), dim),
						(fmt_time(percentile(entry['lat'], 0.99)), dim), (fmt_rate(median(entry['ops'])), dim),
						(fmt_rss(median(entry['rss']), median(entry['floor'])) if entry['rss'] else '', dim),
						(mark_text(result.mark) + ('*' if result.drift else ''), '31' if failed else None)]
					if index:
						ratio = p50 / base_p50 if p50 and base_p50 and result.mark[0] == OK else None
						row.append((fmt_ratio(ratio), None))
				ceiling = self.ceiling.series.get(series) if self.ceiling and case.kind == 'socket' else None
				row.append((fmt_rate(median(ceiling['ops'])) if ceiling else '', None))
				rows.append(row)
		return render_table(columns, rows, width, style)

	def _cold_slope(self, impl, op):
		points = []
		for result in self.results:
			case = result.case
			if result.impl is impl and case.family == 'cold' and case.name == op and case.shape == 'plain' and result.mark[0] == OK:
				points.append((case.rows, median([s.maxrss for s in result.samples]),
					median([s.rss_floor for s in result.samples])))
		points = sorted(p for p in points if p[1] is not None)
		if len(points) < 2 or points[-1][0] == points[0][0]:
			return None
		if points[-1][2] and points[-1][1] <= points[-1][2] + 1024 * 1024:
			return None
		return (points[-1][1] - points[0][1]) / float(points[-1][0] - points[0][0])

	def footprint(self):
		lines = ['[footprint]']
		for impl in self.impls:
			parts = []
			for op in ('read', 'len'):
				slope = self._cold_slope(impl, op)
				parts.append('memory/row (%s) %s' % (op, fmt_bytes(slope) if slope is not None else '—'))
			disk = None
			for result in self.results:
				if result.impl is impl and result.case.name == 'set - new' and result.mark[0] == OK:
					if result.case.rows == max(self.params['sizes']):
						disk = median([s.store_bytes for s in result.samples])
						disk = disk / float(result.case.rows) if disk is not None else None
			parts.append('disk/record %s' % (fmt_bytes(disk) if disk is not None else '—'))
			for result in self.results:
				if result.impl is impl and result.case.kind == 'socket':
					extra = result.extra
					parts.append('handler load %s, rss after load %s, peak rss %s, cpu %s' % (
						fmt_time(median(extra.get('load_time', []))), fmt_bytes(median(extra.get('rss_after_load', []))),
						fmt_rss(median(extra.get('handler_maxrss', [])), median(extra.get('handler_rss_floor', []))),
						fmt_time(median(extra.get('handler_cpu', [])))))
			lines.append('  %-12s %s' % (impl.name, '; '.join(parts)))
		return '\n'.join(lines)

	def summary(self):
		lines = ['[summary] ratio = time against %s, geometric mean per family (lower is faster)' % self.impls[0].name]
		baseline = self.impls[0]
		for impl in self.impls:
			ratios = []
			for family in self.opts.families:
				values = []
				for case, by_impl in self._by_case([r for r in self.results if r.case.family == family]):
					mine, base = by_impl[impl.name], by_impl[baseline.name]
					if mine.mark[0] != OK or base.mark[0] != OK:
						continue
					if case.kind == 'cli':
						a, b = median([s.wall for s in mine.samples]), median([s.wall for s in base.samples])
						if a and b:
							values.append(a / b)
					else:
						for name, entry in mine.series.items():
							other = base.series.get(name)
							if other:
								a, b = percentile(entry['lat'], 0.5), percentile(other['lat'], 0.5)
								if a and b:
									values.append(a / b)
				ratios.append('%s %s' % (family, fmt_ratio(geomean(values))))
			marks = [r.mark[0] for r in self.results if r.impl is impl]
			lines.append('  %-12s %s   ✗ %d  ! %d  ⏱ %d' % (impl.name, '  '.join(ratios),
				marks.count(WRONG), marks.count(EXIT), marks.count(TIMEOUT)))
		failures = [r for r in self.results if r.mark[0] in FAILED]
		for result in failures:
			lines.append('  %s %s %s: %s' % (mark_text(result.mark), result.impl.name, result.case.label, result.mark[1]))
			for log_line in result.log.split('\n') if result.log else []:
				lines.append('      ' + log_line)
		return '\n'.join(lines)

	def write_json(self, env):
		document = {
			'schema': 1,
			'harness': {'version': version, 'suite': self.opts.suite, 'seed': self.opts.seed, 'started': self.started,
				'params': self.params},
			'env': env,
			'impls': [{'name': i.name, 'command': i.command, 'version': i.version, 'handler': i.handler,
				'problem': i.problem} for i in self.impls],
			'results': [],
		}
		for result in self.results:
			entry = {'family': result.case.family, 'workload': result.case.name, 'shape': result.case.shape,
				'rows': result.case.rows, 'impl': result.impl.name,
				'gate': {'mark': result.mark[0], 'detail': result.mark[1]},
				'samples': [s.as_dict() for s in result.samples], 'latency': None, 'extra': result.extra}
			if result.series:
				entry['latency'] = {name: {'p50_us': _us(percentile(s['lat'], 0.5)), 'p95_us': _us(percentile(s['lat'], 0.95)),
					'p99_us': _us(percentile(s['lat'], 0.99)), 'ops_s': median(s['ops']), 'requests': len(s['lat']),
					'latencies_us': [_us(v) for v in s['lat']]} for name, s in result.series.items()}
			document['results'].append(entry)
		if self.ceiling:
			document['ceiling'] = {name: {'ops_s': median(s['ops']), 'p50_us': _us(percentile(s['lat'], 0.5))}
				for name, s in self.ceiling.series.items()}
		with open(self.opts.json, 'w') as f:
			json.dump(document, f, indent=1, sort_keys=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _us(seconds):
	return None if seconds is None else round(seconds * 1e6, 1)


def _interrupt(signum, frame):
	raise KeyboardInterrupt


def main(argv=None):
	opts = parse_args(argv)
	if not hasattr(os, 'wait4') or not hasattr(socket, 'AF_UNIX'):
		eprint('tsvzbench: needs os.wait4 and Unix-domain sockets (Linux or macOS)')
		return 1
	bench = Bench(opts)
	previous = signal.signal(signal.SIGTERM, _interrupt)
	try:
		return bench.run()
	except KeyboardInterrupt:
		kill_live_groups()
		eprint('tsvzbench: interrupted')
		try:
			bench.report(partial=True)
		except Exception as error:
			eprint('tsvzbench: no partial report: %r' % error)
		return 130
	finally:
		bench.cleanup()
		signal.signal(signal.SIGTERM, previous)


if __name__ == '__main__':
	sys.exit(main())
