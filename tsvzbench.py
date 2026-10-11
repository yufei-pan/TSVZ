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


