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


