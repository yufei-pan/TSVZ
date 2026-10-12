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
