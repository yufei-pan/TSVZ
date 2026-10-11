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


