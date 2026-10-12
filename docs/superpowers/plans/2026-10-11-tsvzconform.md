# tsvzconform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A language-neutral conformance suite for tsvz-spec-v1: a JSON case corpus in
`TSVZ/conformance/cases/`, a standard-library runner `TSVZ/tsvzconform.py` that drives any
implementation through the spec's command line (§20) and handler socket (§21) and reports a
verdict per conformance class, and the fourteen spec clarifications the cases depend on.

**Architecture:** One self-contained runner module grown in eight sections, one per task:
command line and §13 codec → the oracle (the runner's own §4–§17 reader) → the case format →
output matchers and byte rules → processes, the §21 client and the handler lifecycle → running a
case in its modes → procedural checks → selection, report and `main`. Then six tasks add the
corpus area by area; each spec decision lands as its own commit together with the case file that
tests it. A final pair of tasks pins the reference implementation's results in a baseline file and
writes the documentation.

**Tech Stack:** Python ≥ 3.8 standard library (argparse, subprocess, `os.waitid`, AF_UNIX sockets,
pty, fcntl, gzip/bz2/lzma/zlib, `compression.zstd` where present, concurrent.futures, json); pytest
for tests.

**Spec:** `TSVZ/docs/superpowers/specs/2026-10-11-tsvz-conformance-design.md`. Here, as there,
`§N` means tsvz-spec-v1 and "section N" means the design. The design lists twelve spec decisions;
two more were settled with the user while the cases were written (decision 13: a command appends to
the last part and adds a compressed member to a compressed part; decision 14: `parts` names codecs
`gzip`, `bzip2`, `xz`, `zstd`). Task 12 and Task 13 carry them.

**Provenance.** Everything below was built and run as a prototype before this plan was written:
each code task was rebuilt on its own, its tests checked to fail before its code (the failure is
quoted in its Step 2) and to pass after it; every corpus step was replayed in order, with the test
suite, the website tests and the reference implementation run after each, and the reference's
failing cases are quoted. The finished suite passes on Python 3.8, 3.13 and 3.14. Copy the blocks
exactly; code is **tab-indented**, and JSON strings keep their escapes (`\t`, `\n`, `\u0000`).

## Global Constraints

- Runner `TSVZ/tsvzconform.py`, tests `TSVZ/tsvzconform_test.py`, cases under `TSVZ/conformance/cases/<class>/*.json`, the reference's baseline `TSVZ/conformance/reference-baseline.json`, implementer docs `TSVZ/conformance/README.md`.
- Standard library only, Python ≥ 3.8, POSIX (Linux primary, macOS works). Tab indentation, as in `TSVZ.py` and `tsvzbench.py`.
- `version = '1.0'` and `__version__ = version` near the top; `-V` prints `tsvzconform 1.0`.
- The runner never imports `TSVZ`: it stays neutral between implementations. It does not import `tsvzbench` either.
- Not added to `setup.py`'s `py_modules`: a development tool, not part of the wheel.
- Tests are plain pytest `test_*` functions, run from `TSVZ/` with `python3 -m pytest -q tsvzconform_test.py`. They use `TSVZ.py` beside them as the reference (`TSVZCONFORM_REFERENCE=/path/TSVZ.py` overrides it).
- Case files are JSON `{"format": 1, "class": ..., "cases": [...]}`; write them byte for byte as given.
- A step that appends a section to a Python file leaves exactly two blank lines between what is there and the new section (the sections are shown without them).
- Spec edits are exact replacements in `TSVZ/tsvz-spec-v1.md`; each matches exactly once. v1 is a Draft: no version bump. Each decision is its own commit with its cases.
- The site goes live only when `tsvz-site.service` restarts: do **not** restart it; the user decides.
- `/mnt/klein/work/cas/CLAUDE.md` is outside any git repository: edit it, do not commit it.
- Commit on `main` of the TSVZ repo, as the design was; sentence-style messages like the history, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Do not push.
- Step counts quoted below are from Python 3.13 on this machine, as root, without the `blake3` package. On Python 3.14 three more tests run (zstd); as a non-root user the two ownership pointer checks report `skip`.

## Review Focus

Inputs and conditions the design implies but its sections do not spell out, most likely to bite
first; each has a test in the task that owns the code:

1. **A work directory whose path has spaces or non-ASCII characters**: every case must still pass; paths reach implementations as single arguments. (Task 8, `test_a_work_directory_with_spaces_and_non_ascii`.)
2. **A long work directory path**: Unix socket paths are limited to about 100 bytes, so the decoy socket of the pointer checks must not live in the case directory. (Task 15, `test_a_long_work_directory_path`; the code is `_decoy` in Task 7.)
3. **A handler that answers but ignores `stop`**: it must be killed (SIGTERM, then SIGKILL) and reported, with nothing left running. (Task 5, `test_a_handler_that_ignores_stop_is_killed`.)
4. **A work directory reused after a failed run**: the kept directory of a failed case must not break the next run of that case. (Task 8, `test_a_reused_work_directory`; the code is the `shutil.rmtree` at the top of `run_case` in Task 6.)
5. **An implementation command that does not exist**: a clear message and exit 1, not a traceback. (Task 8, `test_main_usage_list_and_probe`; Task 6, `test_a_missing_tool_fails_and_a_runner_bug_is_an_error`.)

## File map

- Create `TSVZ/tsvzconform.py`, built section by section (Tasks 1–8).
- Create `TSVZ/tsvzconform_test.py`, one section per task (Tasks 1–9, 11, 15).
- Create `TSVZ/conformance/cases/{reader,writer,cli,handler}/*.json` (Tasks 9–14), `TSVZ/conformance/reference-baseline.json` (Task 15), `TSVZ/conformance/README.md` (Task 16).
- Modify `TSVZ/tsvz-spec-v1.md` (Tasks 9–13, one commit per decision).
- Modify `TSVZ/README.md`, `TSVZ/website/index.md`, `TSVZ/website/spec-glance.md` (Task 16).
- Modify `/mnt/klein/work/cas/CLAUDE.md` (Task 16; not committed).

---

### Task 1: Command line, constants and the §13 codec

**Files:**
- Create: `TSVZ/tsvzconform.py`
- Create: `TSVZ/tsvzconform_test.py`

**Interfaces:**
- Consumes: Nothing.
- Produces: `version`; the constants `CASES_FORMAT`, `CLASSES = ('reader', 'writer', 'cli', 'handler')`, `MODES = ('files', 'socket', 'forwarded')`, `FEATURES`, `LEVELS`, `OPERATIONS`, `PASS, FAIL, WARN, SKIP, ERROR`, `HERE`, `CASES_DIR`, `PROBE_PREFIX`, `HANDLER_WAIT`, `STOP` (a `threading.Event`); `eprint(*args)`; `parse_args(argv) -> Namespace(impl, classes, only, modes, features: frozenset, level, jobs, fail_fast, timeout, work_dir, json, list, verbose)`; `VARIANTS`, `LOOSE_VARIANTS`, `CODECS` (suffix → codec name), `RESERVED_RE`; `decode_field(text, delimiter) -> str`; `encode_field(value, delimiter, is_key=False) -> str`; `is_marker_key(text) -> bool`; `trim(cells) -> list`; `trim_row(row) -> list`; `split_codec(path) -> (path, codec or None)`; `delimiter_of(path) -> str` (raises `ValueError`); `is_loose(path) -> bool`; `show(value) -> str` (JSON, so control characters are visible).

- [ ] **Step 1: Write the failing tests**

Create `TSVZ/tsvzconform_test.py` with this content (its imports and constants, then the first section):

```python
import argparse
import bz2
import gzip
import hashlib
import json
import lzma
import os
import re
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import zlib

import pytest

import tsvzconform as tc

HERE = os.path.dirname(os.path.abspath(__file__))
REFERENCE = os.environ.get('TSVZCONFORM_REFERENCE', os.path.join(HERE, 'TSVZ.py'))
REF_COMMAND = '%s %s' % (shlex.quote(sys.executable), shlex.quote(REFERENCE))
SPEC = os.path.join(HERE, 'tsvz-spec-v1.md')
BASELINE = os.path.join(HERE, 'conformance', 'reference-baseline.json')


# --- command line and the §13 codec ---------------------------------------------------

@pytest.mark.parametrize('argv', [
	[],
	['--impl', ''],
	['--impl', 'x', '--class', 'nope'],
	['--impl', 'x', '--mode', 'files,nope'],
	['--impl', 'x', '--feature', 'gzip'],
	['--impl', 'x', '--level', 'should'],
	['--impl', 'x', '-j', '0'],
	['--impl', 'x', '--timeout', '0'],
])
def test_usage_errors_exit_2(argv, capsys):
	with pytest.raises(SystemExit) as info:
		tc.parse_args(argv)
	assert info.value.code == 2


def test_version(capsys):
	with pytest.raises(SystemExit) as info:
		tc.parse_args(['-V'])
	assert info.value.code == 0
	assert capsys.readouterr().out.strip() == 'tsvzconform ' + tc.version


def test_options():
	opts = tc.parse_args(['--impl', 'x y', '--feature', 'crc32,sha256', '--feature', 'xz', '--only', '9.2,13.*', '--only', '4'])
	assert opts.impl == 'x y' and opts.classes == tc.CLASSES and opts.modes == tc.MODES
	assert opts.features == frozenset(['crc32', 'sha256', 'xz'])
	assert opts.only == ('9.2', '13.*', '4')
	assert tc.parse_args(['--impl', 'x', '--feature', 'all']).features == frozenset(tc.FEATURES)
	assert 'handler' in tc.parse_args(['--impl', 'x', '--class', 'handler']).features
	assert 'handler' not in tc.parse_args(['--impl', 'x']).features
	assert tc.parse_args(['--list']).impl is None


def test_codec_round_trips():
	for delimiter in ('\t', ',', '\0', '|'):
		for value in ('plain', 'a\tb', 'a,b', 'a|b', 'a\0b', 'l\nf', 'c\rr', 'x<y', '<sep>', '<LF>', '<CR>', '<#>', 'é😀'):
			assert tc.decode_field(tc.encode_field(value, delimiter), delimiter) == value
	assert tc.encode_field('#k', '\t', is_key=True) == '<#>k'
	assert tc.encode_field('#k', '\t') == '#k'
	assert tc.encode_field('a\r', '\t') == 'a<CR>'
	assert tc.decode_field('<lf><SEP><Lt><cr><foo>', '\t') == '<lf><SEP><Lt><cr><foo>'
	assert tc.decode_field('<lt>lt>', '\t') == '<lt>'


def test_delimiters_and_codecs():
	assert tc.delimiter_of('/x/s.csvz') == ','
	assert tc.delimiter_of('s.nsvz.ff.gz') == '\0'
	assert tc.delimiter_of('s.psvz.5.rotated.zst') == '|'
	assert tc.delimiter_of('s.tsv') == '\t'
	assert tc.split_codec('s.tsvz.GZ') == ('s.tsvz', 'gzip')
	assert tc.split_codec('s.tsvz') == ('s.tsvz', None)
	assert tc.is_loose('s.csv') and not tc.is_loose('s.csvz')
	with pytest.raises(ValueError):
		tc.delimiter_of('s.txt')
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: ERROR collecting tsvzconform_test.py ... ModuleNotFoundError: No module named 'tsvzconform'.

- [ ] **Step 3: Write the code**

Create `TSVZ/tsvzconform.py` with this content (the module header, then two sections):

```python
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `12 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvzconform.py tsvzconform_test.py && git commit -m "Add tsvzconform: its command line, constants and the §13 codec.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: The oracle

**Files:**
- Modify: `TSVZ/tsvzconform.py` (append)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Task 1: `decode_field`, `trim`, `split_codec`, `delimiter_of`, `is_marker_key`, `PROBE_PREFIX`.
- Produces: `CodecUnavailable(Exception)`; `Replay(delimiter)` with `feed_text(line)`, `feed_part(data: bytes)`, `resolve(key) -> list`, `rows() -> [[key, cell...]]`, `missing() -> list or None`, and the attributes `entries` (ordered key → (cells, bound state)), `strip`, `fill_empty`, `return_missing`, `defaults`, `version`; `store_parts(store) -> [path]`; `decompress(data, codec) -> bytes`; `read_part(path) -> bytes`; `replay_store(store) -> Replay`; `Observed = namedtuple(rows, missing, probe)`; `PROBE_LINES`; `observe(store) -> Observed`.

- [ ] **Step 1: Write the failing tests**

Append this section to `TSVZ/tsvzconform_test.py`:

```python
# --- the oracle ---------------------------------------------------------------------------

def replay(text, delimiter='\t'):
	r = tc.Replay(delimiter)
	r.feed_part(text.encode('utf-8'))
	return r


def test_replay_basics():
	r = replay('#_defaults_#\tguest\t0\nalice\tAlice\t30\nbob\tBob\ncarol\t\t25\nalice\tAlice\t31\nbob\n')
	assert r.rows() == [['alice', 'Alice', '31'], ['carol', '', '25']]
	assert r.missing() == ['guest', '0']
	assert replay('a\t1\nb\t2\na\nc\t3\na\t4\n').rows() == [['b', '2'], ['c', '3'], ['a', '4']]
	assert replay('a\tx\na \n').rows() == []
	assert replay('a\tx\nb\ty').rows() == [['a', 'x']]
	assert replay('﻿#_strip_trailing_whites_#\tno\na\tx \n').rows() == [['a', 'x ']]
	assert replay('#_strip_trailing_whites_#\tfalse\n#_strip_trailing_whites_#\tmaybe\na\tx \n').rows() == [['a', 'x ']]
	assert replay('#_strip_trailing_whites_#\tfalse \na\tx \n').rows() == [['a', 'x']]
	assert replay('#_return_defaults_when_missing_#\tfalse\n').missing() is None
	assert replay('a,x<sep>y\n', ',').rows() == [['a', 'x,y']]


def test_invalid_utf8_becomes_replacement_characters():
	r = tc.Replay('\t')
	r.feed_part(b'c\t\xe2\x82\xed\xa0\x80z\n')
	assert r.rows() == [['c', '�' * 4 + 'z']]


@pytest.mark.parametrize('codec, pack', [
	('gzip', lambda d: gzip.compress(d, mtime=0)),
	('bzip2', bz2.compress),
	('xz', lzma.compress),
])
def test_decompress_reads_every_member_and_keeps_a_cut_short_one(codec, pack):
	assert tc.decompress(pack(b'a\t1\n') + pack(b'b\t2\n'), codec) == b'a\t1\nb\t2\n'
	assert tc.decompress(pack(b'a\t1\n') + pack(b'b\t2\n')[:4], codec) == b'a\t1\n'


def test_store_parts_and_observe(tmp_path):
	for name, text in (('s.tsvz', '#_defaults_#\td1\td2\n'), ('s.tsvz.10', 'k\t2\n'), ('s.tsvz.f', 'k\t1\n'),
			('s.tsvz.5.rotated', 'r\t9\n'), ('s.tsvz.serve', 'x'), ('s.tsvzx.1', 'y\n')):
		(tmp_path / name).write_text(text)
	store = str(tmp_path / 's.tsvz')
	assert [os.path.basename(p) for p in tc.store_parts(store)] == ['s.tsvz', 's.tsvz.f', 's.tsvz.10']
	seen = tc.observe(store)
	assert seen.rows == [['k', '2', 'd2']] and seen.missing == ['d1', 'd2']
	assert seen.probe == [['tsvzconform-probe-1', 'v'], ['tsvzconform-probe-2', 'w', 'd2']]


def test_the_probe_shows_the_final_marker_state(tmp_path):
	path = tmp_path / 's.tsvz'
	path.write_text('#_strip_trailing_whites_#\tfalse\n#_fill_empty_with_default_#\ttrue\n#_defaults_#\td1\td2\td3\td4\n')
	probe = tc.observe(str(path)).probe
	assert probe == [['tsvzconform-probe-1', 'v  ', 'd2', 'd3', 'd4'], ['tsvzconform-probe-2 ', 'w', 'd2', 'd3', 'd4']]
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: 7 failed, 12 passed — AttributeError: module 'tsvzconform' has no attribute 'Replay'.

- [ ] **Step 3: Write the code**

Append this section to `TSVZ/tsvzconform.py`:

```python
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `19 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvzconform.py tsvzconform_test.py && git commit -m "Add tsvzconform's oracle: a reader of §4–§17 and the observable state with its probe.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: The case format

**Files:**
- Modify: `TSVZ/tsvzconform.py` (append)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Tasks 1–2: `CLASSES`, `MODES`, `FEATURES`, `LEVELS`, `OPERATIONS`, `CASES_FORMAT`, `CASES_DIR`, `HERE`, `PROBE_PREFIX`, `delimiter_of`, `is_loose`.
- Produces: `CorpusError`; `Skip` (an exception whose message is a skip reason); `Case(id, cls, spec, level, note, requires=(), store='s.tsvz', files=None, modes=None, expect=None, steps=None, after=None, source=None, func=None)`; `CHECKS` (the procedural checks); `procedural(id, cls, spec, level, note, requires=())` (a decorator registering a check); `BYTE_RULES`; `action_of(step) -> str or None`; `validate_case(raw, cls, where) -> Case`; `load_corpus(directory=None) -> [Case]`; `all_cases(directory=None) -> [Case]` (corpus plus `CHECKS`, ids unique); `case_modes(case) -> tuple`.

- [ ] **Step 1: Write the failing tests**

Append this section to `TSVZ/tsvzconform_test.py`:

```python
# --- case files ---------------------------------------------------------------------------

@pytest.mark.parametrize('raw, problem', [
	({'id': 'nope', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'len': 0}}, 'CLAUSE-slug'),
	({'id': '9.2-x', 'spec': ['9.3'], 'level': 'MUST', 'note': 'n', 'expect': {'len': 0}}, 'must be listed'),
	({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MAY', 'note': 'n', 'expect': {'len': 0}}, 'level'),
	({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'len': 0}, 'requires': ['gzip']}, 'requires'),
	({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'len': 0}, 'store': '../s.tsvz'}, 'relative'),
	({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'len': 0}, 'store': 's.txt'}, 'variant'),
	({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'nope': 0}}, 'expect takes'),
	({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'read': [['tsvzconform-probe-1']]}}, 'must not use'),
	({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'steps': [{'op': ['set']}]}, 'has expect'),
	({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'len': 0}, 'extra': 1}, 'unknown fields'),
])
def test_bad_reader_cases_are_refused(raw, problem):
	with pytest.raises(tc.CorpusError, match=problem):
		tc.validate_case(raw, 'reader', 'x.json')


@pytest.mark.parametrize('steps, problem', [
	([{'op': ['serve']}], 'not op steps'),
	([{'op': ['nope']}], 'unknown operation'),
	([{'op': ['set'], 'argv': []}], 'exactly one'),
	([{'op': ['set'], 'stdout': {'rows': 'x'}}], 'list of lists'),
	([{'op': ['set'], 'stderr': 'some'}], 'stderr'),
	([{'argv': []}, {'raw': 'len\n'}], 'cannot mix'),
	([{'check': {'bytes': ['nope']}}], 'byte rule'),
	([{'append': 'x'}], 'needs content and a path'),
])
def test_bad_steps_are_refused(steps, problem):
	raw = {'id': '20.2-x', 'spec': ['20.2'], 'level': 'MUST', 'note': 'n', 'steps': steps}
	with pytest.raises(tc.CorpusError, match=problem):
		tc.validate_case(raw, 'cli', 'x.json')


def test_case_files_load_in_class_and_file_order(tmp_path):
	for cls, name, ids in (('reader', '02.json', ['9.2-b']), ('reader', '01.json', ['9.2-a']), ('cli', '01.json', ['20.2-c'])):
		folder = tmp_path / cls
		folder.mkdir(exist_ok=True)
		key = 'expect' if cls == 'reader' else 'steps'
		value = {'len': 0} if cls == 'reader' else [{'op': ['len']}]
		cases = [{'id': i, 'spec': [i.split('-')[0]], 'level': 'MUST', 'note': 'n', key: value} for i in ids]
		(folder / name).write_text(json.dumps({'format': 1, 'class': cls, 'cases': cases}))
	assert [c.id for c in tc.load_corpus(str(tmp_path))] == ['9.2-a', '9.2-b', '20.2-c']
	(tmp_path / 'cli' / '02.json').write_text(json.dumps({'format': 1, 'class': 'cli', 'cases': [
		{'id': '9.2-a', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'steps': [{'op': ['len']}]}]}))
	with pytest.raises(tc.CorpusError, match='also used'):
		tc.all_cases(str(tmp_path))


def test_a_bad_case_file_stops_loading(tmp_path):
	folder = tmp_path / 'reader'
	folder.mkdir()
	(folder / '01.json').write_text('{"format": 2, "class": "reader", "cases": []}')
	with pytest.raises(tc.CorpusError, match='format'):
		tc.load_corpus(str(tmp_path))
	(folder / '01.json').write_text('{"format": 1, "class": "writer", "cases": []}')
	with pytest.raises(tc.CorpusError, match='folder'):
		tc.load_corpus(str(tmp_path))
	(folder / '01.json').write_text('{"format": 1, ')
	with pytest.raises(tc.CorpusError, match='01.json'):
		tc.load_corpus(str(tmp_path))


def case(**fields):
	"""A cli case with these fields, validated."""
	return tc.validate_case(dict({'id': '20.2-x', 'spec': ['20.2'], 'level': 'MUST', 'note': 'n'}, **fields), 'cli', 'x.json')


def test_case_modes():
	assert tc.case_modes(tc.validate_case({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'len': 0}}, 'reader', 'x')) == tc.MODES
	assert tc.case_modes(case(steps=[{'argv': ['-h']}])) == ('files',)
	assert tc.case_modes(case(steps=[{'op': ['len']}, {'check': {'rows': []}}])) == ('files',)
	assert tc.case_modes(case(steps=[{'op': ['set', '-'], 'stdin': 'a,b\n'}], store='s.csvz')) == ('files', 'forwarded')
	assert tc.case_modes(case(steps=[{'op': ['set', '-'], 'stdin': 'a,b\n', 'body': ['a\tb']}], store='s.csvz')) == tc.MODES
	assert tc.case_modes(case(steps=[{'op': ['len']}], store='s.tsv')) == ('files',)
	assert tc.case_modes(case(steps=[{'op': ['len']}], modes=['files', 'socket'])) == ('files', 'socket')
	handler = tc.validate_case({'id': '21.8-x', 'spec': ['21.8'], 'level': 'MUST', 'note': 'n', 'steps': [{'op': ['len']}]}, 'handler', 'x')
	assert tc.case_modes(handler) == ('socket', 'forwarded')
	assert tc.case_modes(case(steps=[{'raw': 'len\n'}])) == ('socket',)
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: 21 failed, 19 passed — AttributeError: module 'tsvzconform' has no attribute 'CorpusError'.

- [ ] **Step 3: Write the code**

Append this section to `TSVZ/tsvzconform.py`:

```python
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `40 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvzconform.py tsvzconform_test.py && git commit -m "Add tsvzconform's case format: validation, loading and modes.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Matching output and checking files

**Files:**
- Modify: `TSVZ/tsvzconform.py` (append)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Tasks 1–3: `decode_field`, `trim`, `trim_row`, `show`, `is_marker_key`, `delimiter_of`, `store_parts`, `read_part`, `replay_store`, `observe`, `CodecUnavailable`, `Skip`.
- Produces: `output_lines(data) -> (lines, terminated)`; `compare_lists(got, expected, what) -> str or None`; `match_stdout(matcher, data, delimiter, live=False, line_delimiter=None) -> str or None`; `PartState(raw, data, ino, mode)`; `snapshot_store(store) -> {path: PartState}`; `first_line(parts) -> str or None`; `written_chunks(before, now)`; `snapshot_layout(store, before_parts) -> [problem]`; `byte_rule(rule, store, before, now) -> [problem]`; `Before(parts, observed)`; `check_after(block, store, case_dir, before, serve_exit=None) -> [problem]`.

- [ ] **Step 1: Write the failing tests**

Append this section to `TSVZ/tsvzconform_test.py`:

```python
# --- matching output and files ----------------------------------------------------------

def test_rows_accept_padding_but_not_a_missing_default():
	assert tc.match_stdout({'rows': [['b', '3']]}, b'b\t3\t\n', '\t') is None
	assert 'row 1' in tc.match_stdout({'rows': [['b', '3', 'd']]}, b'b\t3\n', '\t')
	assert 'tombstone' in tc.match_stdout({'rows': [['a', '']]}, b'a\n', '\t', live=True)
	assert tc.match_stdout({'rows': [['a', '']]}, b'a\t\n', '\t', live=True) is None
	assert 'newline' in tc.match_stdout({'rows': [['a', 'x']]}, b'a\tx', '\t')
	assert 'missing' in tc.match_stdout({'rows': [['a', 'x'], ['b', 'y']]}, b'a\tx\n', '\t')
	assert 'extra' in tc.match_stdout({'rows': []}, b'a\tx\n', '\t')
	assert tc.match_stdout({'rows': [['a', 'x,y']]}, b'a,x<sep>y\n', ',') is None
	assert tc.match_stdout({'lines': [['0', '', '/s', 'active']]}, b'0\t\t/s\tactive\n', ',', line_delimiter='\t') is None
	assert tc.match_stdout({'text': '2\n'}, b'2\n', '\t') is None
	assert tc.match_stdout('empty', b'x', '\t') and tc.match_stdout('nonempty', b'', '\t')


def _rule(rule, tmp_path, before_files, after_files, store='s.tsvz'):
	"""Apply a byte rule: write before_files, snapshot, apply after_files (bytes appended or created, None removed, 'replace' renamed over), snapshot again."""
	for name, data in before_files.items():
		(tmp_path / name).write_bytes(data)
	path = str(tmp_path / store)
	before = tc.snapshot_store(path)
	for name, data in after_files.items():
		if data is None:
			os.remove(str(tmp_path / name))
		elif data == 'replace':
			text = (tmp_path / name).read_bytes()
			(tmp_path / (name + '.new')).write_bytes(text)
			os.replace(str(tmp_path / (name + '.new')), str(tmp_path / name))
		else:
			with open(str(tmp_path / name), 'ab' if name in before_files else 'wb') as f:
				f.write(data)
	return tc.byte_rule(rule, path, before, tc.snapshot_store(path))


def test_byte_rule_encoded_judges_only_what_was_written(tmp_path):
	assert _rule('encoded', tmp_path, {'s.tsvz': b'a<b\n'}, {'s.tsvz': b'k\tx<lt>y<sep><CR>\n'}) == []


@pytest.mark.parametrize('written, problem', [
	(b'k\tx\ry\n', 'carriage return'),
	(b'\xef\xbb\xbfk\tx\n', 'byte-order mark'),
	(b'k\tx<y\n', 'begins no token'),
])
def test_byte_rule_encoded_problems(tmp_path, written, problem):
	assert problem in ' '.join(_rule('encoded', tmp_path, {}, {'s.tsvz': written}))


def test_byte_rules_appended_last_part_unchanged_and_same_file(tmp_path):
	two = {'s.tsvz': b'a\t1\n', 's.tsvz.1': b'b\t2\n'}
	d = tmp_path / 'a'
	d.mkdir()
	assert _rule('last-part-only', d, two, {'s.tsvz.1': b'c\t3\n'}) == []
	d = tmp_path / 'g'
	d.mkdir()
	assert _rule('appended', d, two, {'s.tsvz': b'c\t3\n', 's.tsvz.1': b'd\t4\n'}) == []
	d = tmp_path / 'h'
	d.mkdir()
	assert 'not appended' in _rule('appended', d, two, {'s.tsvz.1': None})[0]
	d = tmp_path / 'b'
	d.mkdir()
	assert 'only the last part' in _rule('last-part-only', d, two, {'s.tsvz': b'c\t3\n'})[0]
	d = tmp_path / 'c'
	d.mkdir()
	assert 'parts changed' in _rule('last-part-only', d, two, {'s.tsvz.2': b'c\t3\n'})[0]
	d = tmp_path / 'd'
	d.mkdir()
	assert 'modified' in _rule('unchanged', d, two, {'s.tsvz': b'x'})[0]
	d = tmp_path / 'e'
	d.mkdir()
	assert 'replaced by another file' in _rule('same-file', d, two, {'s.tsvz': 'replace'})[0]
	d = tmp_path / 'f'
	d.mkdir()
	assert _rule('same-file', d, two, {'s.tsvz': b'c\t3\n'}) == []


def test_byte_rule_header_kept(tmp_path):
	(tmp_path / 's.tsvz').write_text('#hdr\na\tx\n')
	before = tc.snapshot_store(str(tmp_path / 's.tsvz'))
	(tmp_path / 's.tsvz').write_text('#hdr\n#_version_#\t1\n')
	assert tc.byte_rule('header-kept', str(tmp_path / 's.tsvz'), before, tc.snapshot_store(str(tmp_path / 's.tsvz'))) == []
	(tmp_path / 's.tsvz').write_text('#_version_#\t1\n')
	assert 'not the first line' in tc.byte_rule('header-kept', str(tmp_path / 's.tsvz'), before, tc.snapshot_store(str(tmp_path / 's.tsvz')))[0]


@pytest.mark.parametrize('content, problem', [
	('#hdr\n#_version_#\t1\n#_defaults_#\td\na\tx\n#_strip_trailing_whites_#\ttrue\n', None),
	('#_version_#\t1\na\tx\n', None),
	('#other\n#_version_#\t1\na\tx\n', 'not the store\'s header'),
	('a\tx\n', '#_version_#'),
	('#_version_#\t1\n#_defaults_#\td\n#_strip_trailing_whites_#\ttrue\na\tx\n', '#_defaults_#'),
	('#_version_#\t1\na\tx\na\n', 'tombstone'),
	('#_version_#\t1\na\tx\n#c\n', 'not a marker'),
	('#_version_#\t1\na\tx\n\tz\n', 'data rows for'),
])
def test_snapshot_layout(tmp_path, content, problem):
	(tmp_path / 's.tsvz').write_text('#hdr\na\tx\n')
	before = tc.snapshot_store(str(tmp_path / 's.tsvz'))
	(tmp_path / 's.tsvz').write_text(content)
	found = tc.snapshot_layout(str(tmp_path / 's.tsvz'), before)
	if problem is None:
		assert found == []
	else:
		assert found and problem in found[0]


def test_check_after_fidelity_and_paths(tmp_path):
	store = tmp_path / 's.tsvz'
	store.write_text('#_strip_trailing_whites_#\tfalse\na\tx \n')
	before = tc.Before(tc.snapshot_store(str(store)), tc.observe(str(store)))
	store.write_text('#_version_#\t1\na\tx \n')
	found = tc.check_after({'fidelity': 'full', 'exists': ['s.tsvz'], 'absent': ['s.tsvz.serve']}, str(store), str(tmp_path), before)
	assert len(found) == 2 and 'rows changed' in found[0] and 'final marker state' in found[1]
	assert tc.check_after({'serve_exit': 0}, str(store), str(tmp_path), before, serve_exit=1) == ['serve exited with status 1, expected 0']
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: 16 failed, 40 passed — AttributeError: module 'tsvzconform' has no attribute 'match_stdout'.

- [ ] **Step 3: Write the code**

Append this section to `TSVZ/tsvzconform.py`:

```python
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `56 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvzconform.py tsvzconform_test.py && git commit -m "Add tsvzconform's output matchers, byte rules and after-block checks.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Processes, the §21 client and the handler lifecycle

**Files:**
- Modify: `TSVZ/tsvzconform.py` (append)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Tasks 1–4: `STOP`, `HANDLER_WAIT`, `decode_field`, `encode_field`, `is_marker_key`, `show`.
- Produces: `LIVE_GROUPS`; `kill_group(pid, sig=SIGKILL)`; `exit_code(status)`; `reap(proc, timeout, first_signal=SIGKILL) -> (exit, timed_out)`; `Ran(exit, timed_out, stdout, stderr, error)`; `run_command(argv, cwd, timeout, stdin=b'', out_dir=None, name='step') -> Ran`; `Impl(command, cwd=None)` with `argv`, `cwd`, `version`, `cli(op, store, args=()) -> argv`; `kill_live_groups()`; `quote(argv) -> str`; `HandlerError`; `read_pointer(store) -> dict or None`; `encode_arg(arg) -> str`; `Response(status, message, output, diagnostics)`; `Client(path, timeout)` with `send(payload)`, `response() -> Response`, `request(fields, options=(), body=None) -> Response`, `close()`; `LIVE_HANDLERS`; `Handler(impl, store, out_dir, wait=HANDLER_WAIT)` with `start() -> self`, `client() -> Client`, `stop() -> [problem]`, `kill()`, and `proc`, `path`, `exit`.

- [ ] **Step 1: Write the failing tests**

Append this section to `TSVZ/tsvzconform_test.py`:

```python
# --- processes and the handler protocol ------------------------------------------------

def test_run_command_exit_output_and_timeout(tmp_path):
	ran = tc.run_command([sys.executable, '-c', 'import sys; print(sys.stdin.read().upper()); sys.exit(3)'], str(tmp_path), 10, b'hi', str(tmp_path))
	assert ran.exit == 3 and ran.stdout == b'HI\n' and not ran.timed_out
	started = time.time()
	ran = tc.run_command([sys.executable, '-c', 'import time; time.sleep(30)'], str(tmp_path), 0.5, b'', str(tmp_path), 'slow')
	assert ran.timed_out and time.time() - started < 10
	assert 'cannot run' in tc.run_command(['/nonexistent/tool'], str(tmp_path), 5, b'', str(tmp_path), 'none').error


def test_processes_left_in_the_group_are_killed(tmp_path):
	pidfile = tmp_path / 'pid'
	code = ('import subprocess, sys; p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"]); '
		'open(%r, "w").write(str(p.pid))' % str(pidfile))
	ran = tc.run_command([sys.executable, '-c', code], str(tmp_path), 10, b'', str(tmp_path))
	assert ran.exit == 0
	pid = int(pidfile.read_text())
	time.sleep(0.2)
	with pytest.raises(ProcessLookupError):
		os.kill(pid, 0)


def _short_socket_dir():
	return tempfile.mkdtemp(prefix='tc-', dir='/tmp')


def test_read_pointer(tmp_path):
	store = str(tmp_path / 's.tsvz')
	with open(store + '.serve', 'w') as f:
		f.write('tsvz-handler\t1\naddress\tunix:/run/x<sep>y\npid\t12\nx-extra\tz\n')
	assert tc.read_pointer(store) == {'address': 'unix:/run/x\ty', 'pid': '12', 'x-extra': 'z'}
	with open(store + '.serve', 'w') as f:
		f.write('tsvz-handler\t2\naddress\tunix:/x\n')
	assert tc.read_pointer(store) is None
	assert tc.read_pointer(str(tmp_path / 'none.tsvz')) is None


def test_encode_arg():
	assert tc.encode_arg('#_defaults_#') == '#_defaults_#'
	assert tc.encode_arg('#k') == '<#>k'
	assert tc.encode_arg('a\tb') == 'a<sep>b'


def test_client_reads_diagnostics_status_and_bulk_requests():
	path = os.path.join(_short_socket_dir(), 's')
	server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	server.bind(path)
	server.listen(1)
	got = []

	def serve():
		conn, _ = server.accept()
		f = conn.makefile('rb')
		got.append(f.readline())
		conn.sendall(b'#!\tnote<sep>1\nk\tv\n#3\tgone<LF>away\n')
		while True:
			line = f.readline()
			got.append(line)
			if line == b'#\n':
				break
		conn.sendall(b'#0\n')
		conn.close()

	thread = threading.Thread(target=serve)
	thread.daemon = True
	thread.start()
	client = tc.Client(path, 5)
	try:
		first = client.request(['get', 'k'])
		assert first.status == 3 and first.message == 'gone\naway' and first.output == b'k\tv\n'
		assert first.diagnostics == [b'#!\tnote<sep>1\n']
		assert client.request(['set', '-'], ['--sync'], ['a\tx', 'b\ty']).status == 0
	finally:
		client.close()
		thread.join(5)
		server.close()
	assert got == [b'get\tk\n', b'--sync\tset\t-\n', b'a\tx\n', b'b\ty\n', b'#\n']


def test_handler_lifecycle_with_the_reference(tmp_path):
	store = tmp_path / 's.tsvz'
	store.write_text('a\tx\n')
	handler = tc.Handler(tc.Impl(REF_COMMAND), str(store), str(tmp_path)).start()
	client = handler.client()
	try:
		assert client.request(['get', 'a']).output == b'a\tx\n'
	finally:
		client.close()
	assert handler.stop() == [] and handler.exit == 0
	assert not os.path.exists(str(store) + '.serve')


def test_a_handler_that_never_starts(tmp_path):
	store = tmp_path / 's.tsvz'
	store.write_text('')
	impl = tc.Impl('%s -c "import sys; sys.stderr.write(\'no handler here\\n\'); sys.exit(1)"' % shlex.quote(sys.executable))
	with pytest.raises(tc.HandlerError, match='no handler here'):
		tc.Handler(impl, str(store), str(tmp_path)).start()
	impl = tc.Impl('%s -c "import time; time.sleep(30)"' % shlex.quote(sys.executable))
	with pytest.raises(tc.HandlerError, match='within'):
		tc.Handler(impl, str(store), str(tmp_path), wait=1).start()
	assert not tc.LIVE_HANDLERS


FAKE_SERVE = r'''
import os, socket, sys, tempfile
args = sys.argv[1:]
if '-V' in args:
	print('stubborn 1.0')
	sys.exit(0)
if 'serve' not in args:
	sys.exit(1)
store = args[-1]
path = os.path.join(tempfile.mkdtemp(prefix='tc-', dir='/tmp'), 's')
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(path)
server.listen(4)
with open(store + '.serve', 'w') as f:
	f.write('tsvz-handler\t1\naddress\tunix:%s\nhost\t%s\npid\t%d\n' % (path, socket.gethostname(), os.getpid()))
while True:
	conn, _ = server.accept()
	f = conn.makefile('rb')
	try:
		while f.readline():
			conn.sendall(b'0\n#0\n')
	except OSError:
		pass
	conn.close()
'''


def test_a_handler_that_ignores_stop_is_killed(tmp_path):
	script = tmp_path / 'stubborn.py'
	script.write_text(FAKE_SERVE)
	store = tmp_path / 's.tsvz'
	store.write_text('')
	impl = tc.Impl('%s %s' % (shlex.quote(sys.executable), shlex.quote(str(script))))
	handler = tc.Handler(impl, str(store), str(tmp_path), wait=1).start()
	pid = handler.proc.pid
	started = time.time()
	problems = handler.stop()
	assert problems == ['the handler did not exit within 1 s of stop'] and time.time() - started < 15
	with pytest.raises(ProcessLookupError):
		os.kill(pid, 0)
	assert not tc.LIVE_HANDLERS and pid not in tc.LIVE_GROUPS
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: 8 failed, 56 passed — AttributeError: module 'tsvzconform' has no attribute 'run_command'.

- [ ] **Step 3: Write the code**

Append these two sections to `TSVZ/tsvzconform.py`:

```python
# ---------------------------------------------------------------------------
# Processes
# ---------------------------------------------------------------------------

LIVE_GROUPS = set()


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
	its id cannot have been reused. Returns (exit status, timed out).
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
			kill_group(pid)
			status = os.waitpid(pid, 0)[1]
		else:
			status = os.waitpid(pid, 0)[1]
			kill_group(pid)
	finally:
		for timer in timers:
			timer.cancel()
	proc.returncode = exit_code(status)
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
		handler.kill()


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
				if not code.isdigit():
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
			if self.proc.poll() is not None:
				self.exit = self.proc.returncode
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `64 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvzconform.py tsvzconform_test.py && git commit -m "Add tsvzconform's process runner, §21 client and handler lifecycle.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: Running a case

**Files:**
- Modify: `TSVZ/tsvzconform.py` (append)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Tasks 1–5: everything above, in particular `validate_case`, `action_of`, `Handler`, `run_command`, `match_stdout`, `check_after`, `snapshot_store`, `observe`, `Before`, `encode_arg`, `Skip`.
- Produces: `Context(impl, work, timeout=10.0, features=frozenset())`; `Result(case, mode, result, detail=(), seconds=0.0, kept=None)` with `cls` (the case's class in `files` mode, `handler` otherwise) and `name` (`id` or `id@mode`); `Failed`; `content_bytes(content) -> bytes`; `fill(text, case_dir, store)`; `fill_matcher(matcher, case_dir, store)`; `expand_expect(expect) -> [step]`; `step_op(step) -> str or None`; `materialize(case, case_dir)`; `CaseRun(case, mode, ctx, case_dir)` with `run()`; `CheckContext(ctx, case_dir)` with `impl`, `timeout`, `dir`, `cleanup` (a list of callables run afterwards), `path(name)`, `write(name, content) -> path`, `run(argv, stdin=b'') -> Ran`, `handler(store) -> Handler`; `run_name(case, mode)`; `run_case(case, mode, ctx) -> Result`.

- [ ] **Step 1: Write the failing tests**

Append this section to `TSVZ/tsvzconform_test.py`:

```python
# --- running cases ---------------------------------------------------------------------------

FAKE = r'''
import os, re, socket, subprocess, sys, time
mode, reference = sys.argv[1], sys.argv[2]
args = sys.argv[3:]
ops = ('read', 'get', 'set', 'append', 'delete', 'clear', 'scrub', 'verify', 'parts', 'has', 'len', 'keys', 'pop', 'popitem', 'setdefault', 'serve', 'stop')
op = next((a for a in args if a in ops), None)
store = args[args.index(op) + 1] if op and args.index(op) + 1 < len(args) else None
if mode == 'hang' and op == 'read':
	child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
	with open(os.environ['FAKE_PIDFILE'], 'w') as f:
		f.write(str(child.pid))
	time.sleep(60)
if mode == 'crash' and op == 'len':
	raise RuntimeError('crashed')
if mode == 'decoy' and op == 'get':
	try:
		lines = open(store + '.serve').read().split('\n')
		address = dict(l.split('\t', 1) for l in lines[1:] if '\t' in l)['address']
		if address.startswith('unix:'):
			s = socket.socket(socket.AF_UNIX)
			s.connect(address[5:])
		else:
			host, port = address.split(':')[1:]
			s = socket.create_connection((host, int(port)))
		s.close()
	except Exception:
		pass
if mode == 'rename' and op == 'scrub':
	subprocess.run([sys.executable, reference] + args, check=True)
	data = open(store, 'rb').read()
	with open(store + '.tmp', 'wb') as f:
		f.write(data)
	os.replace(store + '.tmp', store)
	sys.exit(0)
if mode == 'nomarkers' and op == 'scrub':
	rows = subprocess.run([sys.executable, reference, '--format', 'records', 'read', store], stdout=subprocess.PIPE).stdout
	with open(store, 'r+b') as f:
		f.write(b'#_version_#\t1\n' + rows)
		f.truncate()
	sys.exit(0)
data = sys.stdin.buffer.read() if args[-1:] == ['-'] else b''
done = subprocess.run([sys.executable, reference] + args, input=data, stdout=subprocess.PIPE)
out, code = done.stdout, done.returncode
if mode == 'drop' and op == 'read':
	out = b''.join(out.splitlines(True)[:-1])
if mode == 'nodelim' and op == 'read':
	out = re.sub(rb'\t+\n', b'\n', out)
if mode == 'exit0' and op == 'get':
	code = 0
sys.stdout.buffer.write(out)
sys.exit(code)
'''


def fake(tmp_path, mode):
	"""The command line of a fake implementation that wraps the reference and breaks one thing."""
	script = tmp_path / 'fake.py'
	script.write_text(FAKE)
	return '%s %s %s %s' % (shlex.quote(sys.executable), shlex.quote(str(script)), mode, shlex.quote(REFERENCE))


def context(work, command=REF_COMMAND, features=()):
	os.makedirs(str(work), exist_ok=True)
	return tc.Context(tc.Impl(command), str(work), 10.0, frozenset(features))


DELETES = {'id': '9.2-lone-key-deletes', 'spec': ['9.2', '9.3'], 'level': 'MUST', 'note': 'A row with no delimiter deletes its key.',
	'files': {'s.tsvz': 'a\tx\nb\ty\na\n'}, 'expect': {'read': [['b', 'y']], 'len': 1, 'get': [{'args': ['a'], 'rows': [['a']], 'exit': 3}]}}
WRITES = {'id': '13.3-set-and-check', 'spec': ['13.3'], 'level': 'MUST', 'note': 'set writes an encoded record and appends only.',
	'files': {'s.tsvz': '#hdr\na\tx\n'},
	'steps': [{'op': ['set', 'k', 'v<w', 'l\nf']}, {'op': ['get', 'k'], 'stdout': {'rows': [['k', 'v<w', 'l\nf']]}},
		{'append': 'z\t1\n', 'path': 's.tsvz'}, {'op': ['len'], 'stdout': {'text': '3\n'}}],
	'after': {'rows': [['a', 'x'], ['k', 'v<w', 'l\nf'], ['z', '1']], 'bytes': ['appended', 'encoded']}}


def test_expand_expect_and_step_op():
	steps = tc.expand_expect(DELETES['expect'])
	assert steps == [{'op': ['read'], 'exit': 0, 'stdout': {'rows': [['b', 'y']]}}, {'op': ['len'], 'stdout': {'text': '1\n'}},
		{'op': ['get', 'a'], 'exit': 3, 'stdout': {'rows': [['a']]}}]
	assert tc.expand_expect({'read': {'rows': None, 'exit': 1}}) == [{'op': ['read'], 'exit': 1}]
	assert tc.step_op({'argv': ['-q', 'verify', '{store}']}) == 'verify' and tc.step_op({'append': 'x'}) is None


@pytest.mark.parametrize('raw, cls', [(DELETES, 'reader'), (WRITES, 'writer')])
def test_cases_pass_with_the_reference_in_every_mode(tmp_path, raw, cls):
	case = tc.validate_case(raw, cls, 'x')
	for mode in tc.MODES:
		result = tc.run_case(case, mode, context(tmp_path / 'work', features=['handler']))
		assert result.result == tc.PASS, (mode, result.detail)
	assert os.listdir(str(tmp_path / 'work')) == []


def test_a_failing_case_keeps_its_directory_and_names_the_step(tmp_path):
	result = tc.run_case(tc.validate_case(DELETES, 'reader', 'x'), 'files', context(tmp_path / 'work', fake(tmp_path, 'drop')))
	assert result.result == tc.FAIL and result.detail[0].startswith('step 1  ') and 'missing' in result.detail[1]
	assert result.kept and os.path.isdir(result.kept)
	should = dict(DELETES, level='SHOULD')
	assert tc.run_case(tc.validate_case(should, 'reader', 'x'), 'files', context(tmp_path / 'w2', fake(tmp_path, 'drop'))).result == tc.WARN


def test_a_missing_tool_fails_and_a_runner_bug_is_an_error(tmp_path, monkeypatch):
	result = tc.run_case(tc.validate_case(DELETES, 'reader', 'x'), 'files', context(tmp_path / 'work', '/nonexistent/tsvz'))
	assert result.result == tc.FAIL and 'cannot run' in ' '.join(result.detail)

	def boom(store):
		raise RuntimeError('a bug in the runner')

	monkeypatch.setattr(tc, 'observe', boom)
	faithful = dict(WRITES, after={'fidelity': 'full'})
	result = tc.run_case(tc.validate_case(faithful, 'writer', 'x'), 'files', context(tmp_path / 'w2'))
	assert result.result == tc.ERROR and 'a bug in the runner' in result.detail[-1]
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: 5 failed, 64 passed — AttributeError: module 'tsvzconform' has no attribute 'expand_expect'.

- [ ] **Step 3: Write the code**

Append this section to `TSVZ/tsvzconform.py`:

```python
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `69 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvzconform.py tsvzconform_test.py && git commit -m "Run tsvzconform's cases: steps, modes and results.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: Procedural checks

**Files:**
- Modify: `TSVZ/tsvzconform.py` (append)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Tasks 1–6: `procedural`, `CheckContext`, `Handler`, `HandlerError`, `Skip`, `reap`, `read_pointer`, `replay_store`, `match_stdout`, `show`.
- Produces: Nineteen entries in `CHECKS`: `20.4.3-terminal-output-is-a-table` (cli, SHOULD); `21.2.2-pointer-names-host-and-pid`, `21.2.3-pointer-is-locked`, `21.2.3-pointer-not-writable-by-others`, `21.13.1-second-serve-exits-1`, `21.2.3-stale-pointer-is-replaced`, `21.2.3-no-write-through-symlink`, `21.10.2-one-mib-request-line`, `21.7.2-cut-off-bulk-writes-nothing`, `21.8.2-concurrent-pop-is-atomic`, `21.11.1-socket-closed-to-others` (SHOULD) (handler); and the eight `21.2.4-refuse-*` pointer checks (cli): `symlink`, `group-writable`, `wrong-first-line`, `control-character`, `other-host`, `not-unix`, `other-owner`, `socket-other-owner`.

- [ ] **Step 1: Write the failing tests**

Append this section to `TSVZ/tsvzconform_test.py`:

```python
# --- procedural checks -----------------------------------------------------------------------

@pytest.mark.parametrize('check', tc.CHECKS, ids=[c.id for c in tc.CHECKS])
def test_every_procedural_check_passes_with_the_reference(check, tmp_path):
	result = tc.run_case(check, 'files', context(tmp_path / 'work', features=['handler']))
	assert result.result in (tc.PASS, tc.SKIP), result.detail


def test_a_command_that_follows_a_bad_pointer_fails(tmp_path):
	check = next(c for c in tc.CHECKS if c.id == '21.2.4-refuse-wrong-first-line')
	result = tc.run_case(check, 'files', context(tmp_path / 'work', fake(tmp_path, 'decoy')))
	assert result.result == tc.FAIL and 'must not use' in result.detail[0]
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: 1 failed, 69 passed, 1 skipped — `test_a_command_that_follows_a_bad_pointer_fails` raises StopIteration (no check is registered yet) and the parametrized check test is skipped with "got empty parameter set".

- [ ] **Step 3: Write the code**

Append this section to `TSVZ/tsvzconform.py`:

```python
# ---------------------------------------------------------------------------
# Procedural checks (design section 4.3)
# ---------------------------------------------------------------------------

@procedural('20.4.3-terminal-output-is-a-table', 'cli', ['20.4.3'], 'SHOULD',
	'Without --format, output to a terminal uses the table layout, which differs from the records the same command prints to a file.')
def check_terminal_default(cx):
	import pty
	store = cx.write('s.tsvz', 'alpha\tA\t1\nbeta\tB<sep>x\t2\n')
	records = cx.run(cx.impl.argv + ['--format', 'records', 'read', store])
	if records.exit != 0:
		return ['read --format records exited %s' % records.exit]
	master, slave = pty.openpty()
	try:
		proc = subprocess.Popen(cx.impl.argv + ['read', store], cwd=cx.impl.cwd, stdin=subprocess.DEVNULL, stdout=slave,
			stderr=subprocess.DEVNULL, start_new_session=True)
		os.close(slave)
		slave = None
		chunks = []
		deadline = time.time() + cx.timeout
		while time.time() < deadline:
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


def _refuse_pointer(cx, setup, kind='unix'):
	"""Plant a bad pointer to a decoy; the CLI must not connect and must answer from the files."""
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
		elif ran.exit != 0 or match_stdout({'rows': [['k', 'v']]}, ran.stdout, '\t'):
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
		procedural('21.2.4-refuse-%s' % slug, 'cli', ['21.2.4', '20.8.2'], 'MUST',
			'A command does not use %s, and answers from the files.' % what)(check)


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
	with open(store, 'rb') as f:
		tombstones = f.read().split(b'\n').count(b'k')
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `89 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvzconform.py tsvzconform_test.py && git commit -m "Add tsvzconform's procedural checks for the handler and pointer files.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Selection, the report and main

**Files:**
- Modify: `TSVZ/tsvzconform.py` (append)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Tasks 1–7: everything above.
- Produces: `matches_only(case_id, patterns) -> bool`; `select(cases, opts) -> (runs: [(Case, mode)], skipped: [Result])`; `run_all(runs, ctx, jobs, fail_fast=False, progress=None) -> [Result]`; `rerun_command(impl, result, features)`; `render_failure(result, impl, features)`; `verdicts(results, classes) -> {class: text}`; `render_summary(results, impl, features, classes)`; `write_json(path, results, impl, features)`; `probe_version(impl, timeout, work) -> str`; `main(argv=None) -> int` (0 nothing failed, 1 a FAIL or error or a failed `-V` probe, 2 usage, 130 interrupted); the `__main__` entry.

- [ ] **Step 1: Write the failing tests**

Append this section to `TSVZ/tsvzconform_test.py`:

```python
# --- selection, report and main --------------------------------------------------------------

def test_matches_only():
	assert tc.matches_only('9.2-lone-key-deletes', ())
	assert tc.matches_only('9.2-lone-key-deletes', ('9.2',))
	assert tc.matches_only('9.2.1-x', ('9.2',))
	assert tc.matches_only('9.2-lone-key-deletes', ('9',))
	assert not tc.matches_only('9.20-x', ('9.2',))
	assert tc.matches_only('13.4-x', ('13.*',))
	assert tc.matches_only('13.4-x', ('13.4-x',))
	assert not tc.matches_only('13.4-x', ('14',))


def test_select_applies_classes_modes_claims_and_level():
	reader = tc.validate_case({'id': '9.2-x', 'spec': ['9.2'], 'level': 'MUST', 'note': 'n', 'expect': {'len': 0}}, 'reader', 'x')
	needs = tc.validate_case({'id': '15.3-x', 'spec': ['15.3'], 'level': 'MUST', 'note': 'n', 'requires': ['crc32'], 'expect': {'len': 0}}, 'reader', 'x')
	unless = tc.validate_case({'id': '15.3-y', 'spec': ['15.3'], 'level': 'MUST', 'note': 'n', 'requires': ['!crc32'], 'expect': {'len': 0}}, 'reader', 'x')
	should = tc.validate_case({'id': '20.2-s', 'spec': ['20.2'], 'level': 'SHOULD', 'note': 'n', 'expect': {'len': 0}}, 'reader', 'x')
	handler = tc.validate_case({'id': '21.8-x', 'spec': ['21.8'], 'level': 'MUST', 'note': 'n', 'steps': [{'raw': 'len\n'}]}, 'handler', 'x')
	cases = [reader, needs, unless, should, handler]

	def pick(**kw):
		opts = argparse.Namespace(only=(), level='all', classes=tc.CLASSES, modes=tc.MODES, features=frozenset())
		vars(opts).update(kw)
		runs, skipped = tc.select(cases, opts)
		return sorted((c.id, m) for c, m in runs), sorted((r.case.id, r.mode, r.detail[0]) for r in skipped)

	runs, skipped = pick()
	assert runs == [('15.3-y', 'files'), ('20.2-s', 'files'), ('9.2-x', 'files')]
	assert skipped == [('15.3-x', 'files', 'crc32 not claimed'), ('21.8-x', 'socket', 'handler not claimed')]
	runs, skipped = pick(features=frozenset(['crc32', 'handler']), level='must')
	assert ('15.3-x', 'socket') in runs and ('9.2-x', 'forwarded') in runs and ('21.8-x', 'socket') in runs
	assert ('15.3-y', 'files', 'applies when crc32 is not claimed') in skipped and not [r for r in runs if r[0] == '20.2-s']
	runs, _ = pick(features=frozenset(['handler']), classes=('reader',))
	assert runs == [('15.3-y', 'files'), ('20.2-s', 'files'), ('9.2-x', 'files')]


SMALL_CORPUS = {
	'reader': [DELETES,
		{'id': '9.2-trailing-delimiter-is-not-a-tombstone', 'spec': ['9.2'], 'level': 'MUST', 'note': 'key<TAB> is a present-empty cell.',
			'files': {'s.tsvz': 'a\tx\na\t\n'}, 'expect': {'read': [['a', '']]}}],
	'writer': [{'id': '19.4-scrub-twice', 'spec': ['19.4'], 'level': 'MUST', 'note': 'Scrubbing twice changes nothing a reader sees.',
			'files': {'s.tsvz': '#_defaults_#\td1\td2\na\tx\n#_strip_trailing_whites_#\tfalse\nb\ty \n'},
			'steps': [{'op': ['scrub']}, {'op': ['scrub']}], 'after': {'fidelity': 'full', 'bytes': ['snapshot-layout', 'same-file']}}],
	'cli': [{'id': '20.2-len', 'spec': ['20.2'], 'level': 'MUST', 'note': 'len prints the number of live keys.',
			'files': {'s.tsvz': 'a\t1\nb\t2\na\nc\t3\n'}, 'steps': [{'op': ['len'], 'stdout': {'text': '2\n'}}]}],
}


@pytest.fixture
def small_corpus(tmp_path, monkeypatch):
	"""A few cases in a case directory of their own, used by main()."""
	for cls, cases in SMALL_CORPUS.items():
		folder = tmp_path / 'cases' / cls
		folder.mkdir(parents=True)
		(folder / '01.json').write_text(json.dumps({'format': 1, 'class': cls, 'cases': cases}))
	monkeypatch.setattr(tc, 'CASES_DIR', str(tmp_path / 'cases'))


def run_main(tmp_path, command, *extra):
	out = tmp_path / 'report.json'
	argv = ['--impl', command, '--work-dir', str(tmp_path / 'work'), '--json', str(out), '-j', '4'] + list(extra)
	code = tc.main(argv)
	with open(str(out), encoding='utf-8') as f:
		results = json.load(f)['results']
	return code, {(r['id'], r['mode']): r for r in results}


@pytest.mark.parametrize('mode, case_id, words', [
	('drop', '9.2-lone-key-deletes', 'missing'),
	('nodelim', '9.2-trailing-delimiter-is-not-a-tombstone', 'tombstone'),
	('exit0', '9.2-lone-key-deletes', 'exit status 0, expected 3'),
	('crash', '20.2-len', 'exit status 1, expected 0'),
	('rename', '19.4-scrub-twice', 'replaced by another file'),
	('nomarkers', '19.4-scrub-twice', 'final marker state'),
])
def test_the_runner_catches_a_misbehaving_implementation(small_corpus, tmp_path, capsys, mode, case_id, words):
	code, results = run_main(tmp_path, fake(tmp_path, mode), '--only', case_id, '--mode', 'files')
	result = results[(case_id, 'files')]
	assert result['result'] == 'FAIL' and words in ' '.join(result['detail'])
	assert code == 1
	out = capsys.readouterr().out
	assert 'FAIL ' + case_id in out and 'rerun   python3' in out and 'kept    ' in out
	assert 'conformant: ' in out and 'failed cases are kept in' in out


def test_the_reference_passes_the_small_corpus(small_corpus, tmp_path, capsys):
	code, results = run_main(tmp_path, REF_COMMAND, '--feature', 'handler')
	assert code == 0 and {r['result'] for r in results.values()} <= {'pass', 'skip'}
	assert ('9.2-lone-key-deletes', 'socket') in results and ('20.2-len', 'forwarded') in results
	assert 'conformant: reader ✓   writer ✓   cli ✓   handler ✓' in capsys.readouterr().out
	assert not os.path.exists(str(tmp_path / 'work' / '9.2-lone-key-deletes'))


def test_a_hang_is_a_timeout_and_leaves_nothing_running(small_corpus, tmp_path, capsys, monkeypatch):
	monkeypatch.setenv('FAKE_PIDFILE', str(tmp_path / 'pid'))
	code, results = run_main(tmp_path, fake(tmp_path, 'hang'), '--only', '9.2-lone-key-deletes', '--mode', 'files', '--timeout', '2')
	assert code == 1 and 'timed out' in ' '.join(results[('9.2-lone-key-deletes', 'files')]['detail'])
	pid = int((tmp_path / 'pid').read_text())
	time.sleep(0.2)
	with pytest.raises(ProcessLookupError):
		os.kill(pid, 0)


def test_a_work_directory_with_spaces_and_non_ascii(small_corpus, tmp_path, capsys):
	work = tmp_path / 'my work dïr'
	code, results = run_main(work, REF_COMMAND, '--feature', 'handler', '--only', '9.2,20.2,21.2.4-refuse-symlink')
	assert code == 0 and len(results) == 10 and all(r['result'] == 'pass' for r in results.values())


def test_a_reused_work_directory(small_corpus, tmp_path, capsys):
	first, _ = run_main(tmp_path, fake(tmp_path, 'drop'), '--only', '9.2-lone-key-deletes', '--mode', 'files')
	assert first == 1 and os.path.isdir(str(tmp_path / 'work' / '9.2-lone-key-deletes'))
	again, results = run_main(tmp_path, REF_COMMAND, '--only', '9.2-lone-key-deletes', '--mode', 'files')
	assert again == 0 and results[('9.2-lone-key-deletes', 'files')]['result'] == 'pass'


def test_main_usage_list_and_probe(small_corpus, tmp_path, capsys):
	assert tc.main(['--impl', 'x', '--feature', 'nope']) == 2
	capsys.readouterr()
	assert tc.main(['--list', '--only', '9.2']) == 0
	listed = [line.split('\t') for line in capsys.readouterr().out.splitlines()]
	assert [row[0] for row in listed] == ['9.2-lone-key-deletes', '9.2-trailing-delimiter-is-not-a-tombstone']
	assert tc.main(['--list', '--class', 'handler', '--level', 'must']) == 0
	listed = [line.split('\t') for line in capsys.readouterr().out.splitlines()]
	assert listed and all(row[1] == 'handler' and row[2] == 'MUST' for row in listed)
	assert tc.main(['--impl', '%s -c "import sys; sys.exit(1)"' % shlex.quote(sys.executable), '--work-dir', str(tmp_path)]) == 1
	assert 'failed its -V probe' in capsys.readouterr().err
	assert tc.main(['--impl', '/nonexistent/tsvz']) == 1
	assert 'cannot run' in capsys.readouterr().err


def test_an_interrupt_prints_the_results_so_far(tmp_path):
	env = dict(os.environ, FAKE_PIDFILE=str(tmp_path / 'pid'))
	proc = subprocess.Popen([sys.executable, os.path.join(HERE, 'tsvzconform.py'), '--impl', fake(tmp_path, 'hang'),
		'--only', '20.4.3', '--timeout', '60', '--work-dir', str(tmp_path / 'work')],
		cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
	deadline = time.time() + 20
	while not (tmp_path / 'pid').exists() and time.time() < deadline:
		time.sleep(0.1)
	proc.send_signal(signal.SIGINT)
	out, _ = proc.communicate(timeout=30)
	assert proc.returncode == 130
	assert b'tsvzconform ' + tc.version.encode() in out
	with pytest.raises(ProcessLookupError):
		os.kill(int((tmp_path / 'pid').read_text()), 0)
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: 14 failed, 89 passed — AttributeError: module 'tsvzconform' has no attribute 'matches_only'.

- [ ] **Step 3: Write the code**

Append this section to `TSVZ/tsvzconform.py` (it ends the module):

```python
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
	"""Run every (case, mode), `jobs` at a time; results come back in the order of `runs`."""
	results = [None] * len(runs)
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
			STOP.set()
			kill_live_groups()
			for future in futures:
				future.cancel()
			raise
	return [r for r in results if r is not None]


def rerun_command(impl, result, features):
	prog = os.path.relpath(os.path.abspath(__file__))
	argv = ['python3', prog, '--impl', impl.command, '--only', result.case.id]
	if result.mode != 'files' and not result.case.func:
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

		try:
			results = run_all(runs, ctx, opts.jobs, opts.fail_fast, progress)
		except KeyboardInterrupt:
			interrupted = True
			results = []
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `103 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvzconform.py tsvzconform_test.py && git commit -m "Add tsvzconform's selection, report and main.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Reader cases for §4–§11, with decisions 1, 7 and 8

**Files:**
- Create: `TSVZ/conformance/cases/reader/04-framing.json`
- Create: `TSVZ/conformance/cases/reader/06-versions.json`
- Create: `TSVZ/conformance/cases/reader/07-pipeline.json`
- Create: `TSVZ/conformance/cases/reader/09-tombstones.json`
- Create: `TSVZ/conformance/cases/reader/10-whitespace.json`
- Create: `TSVZ/conformance/cases/reader/11-comments.json`
- Create: `TSVZ/conformance/cases/reader/05-variants.json`
- Create: `TSVZ/conformance/cases/reader/04-bom.json`
- Create: `TSVZ/conformance/cases/reader/04-utf8.json`
- Modify: `TSVZ/tsvz-spec-v1.md` (decisions 1, 7, 8)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Tasks 1–8 (the whole runner).
- Produces: The corpus tests (`CASES`, `READER_CASES`, `test_the_corpus_loads_and_ids_are_unique`, `test_every_clause_cited_exists_in_the_spec`, `test_the_oracle_passes_every_reader_case`), which check every case file added from here on; spec §2.1, §4.1, §4.7, §4.8.

- [ ] **Step 1: Add the corpus tests**

Append this section to `TSVZ/tsvzconform_test.py`. It checks every case file: ids unique, every cited clause present in the spec, and the runner's own reader agreeing with every reader case.

```python
# --- the corpus --------------------------------------------------------------------------------

CASES = tc.all_cases()
READER_CASES = [c for c in CASES if c.cls == 'reader']


def test_the_corpus_loads_and_ids_are_unique():
	assert len(CASES) == len({c.id for c in CASES})


def test_every_clause_cited_exists_in_the_spec():
	with open(SPEC, encoding='utf-8') as f:
		text = f.read()
	clauses = set(re.findall(r'^(?:#+ )?(\d+(?:\.\d+)*)[ .]', text, re.M))
	missing = sorted({(c.id, clause) for c in CASES for clause in c.spec if clause not in clauses})
	assert missing == []


def _get(r, args):
	rows, missing = [], False
	for key in args:
		if r.strip:
			key = key.rstrip(' \t')
		if key in r.entries:
			rows.append([key] + r.resolve(key))
		else:
			missing = True
			if r.missing() is not None:
				rows.append([key] + r.missing())
	return rows, 3 if missing else 0


@pytest.mark.parametrize('case', READER_CASES, ids=[c.id for c in READER_CASES])
def test_the_oracle_passes_every_reader_case(case, tmp_path):
	"""The runner's own reader agrees with every reader case it can judge (design section 3.1.2)."""
	read = case.expect.get('read')
	if isinstance(read, dict) and read.get('exit', 0) != 0:
		pytest.skip('an unreadable store: a refusal the oracle does not model')
	tc.materialize(case, str(tmp_path))
	try:
		r = tc.replay_store(str(tmp_path / case.store))
	except tc.CodecUnavailable:
		pytest.skip('this Python has no zstd')
	trimmed = [tc.trim_row(row) for row in r.rows()]
	if read is not None:
		want = read if isinstance(read, list) else read['rows']
		assert trimmed == [tc.trim_row(row) for row in want]
	if 'keys' in case.expect:
		assert list(r.entries) == case.expect['keys']
	if 'len' in case.expect:
		assert len(r.entries) == case.expect['len']
	for query in case.expect.get('get', []):
		rows, code = _get(r, query['args'])
		assert [tc.trim_row(x) for x in rows] == [tc.trim_row(x) for x in query.get('rows', [])]
		assert code == query.get('exit', 0)
```

- [ ] **Step 2: Run the tests**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `105 passed, 1 skipped` (no reader case yet: the oracle test has an empty parameter set).

- [ ] **Step 3: Write the case files**

Write these files exactly. Each expectation follows from the clause its `note` gives.

`TSVZ/conformance/cases/reader/04-framing.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "4.2-lf-ends-each-record", "spec": ["4.2", "3.3"], "level": "MUST",
 "note": "Records are separated by \\n; field 0 is the key.",
 "files": {"s.tsvz": "a\tx\nb\ty\tz\n"},
 "expect": {"read": [["a", "x"], ["b", "y", "z"]], "len": 2, "keys": ["a", "b"]}},
{"id": "4.2-crlf-is-tolerated", "spec": ["4.2"], "level": "MUST",
 "note": "A reader removes one \\r before the \\n.",
 "files": {"s.tsvz": "a\tx\r\nb\ty\r\n"},
 "expect": {"read": [["a", "x"], ["b", "y"]], "len": 2}},
{"id": "4.2-only-one-cr-is-removed", "spec": ["4.2"], "level": "MUST",
 "note": "CRLF tolerance removes at most one \\r; a second one is field data.",
 "files": {"s.tsvz": "a\tx\r\r\n"},
 "expect": {"read": [["a", "x\r"]]}},
{"id": "4.2-cr-inside-a-row-is-data", "spec": ["4.2"], "level": "MUST",
 "note": "A \\r that is not right before the \\n is ordinary field data.",
 "files": {"s.tsvz": "a\tx\ry\n"},
 "expect": {"read": [["a", "x\ry"]]}},
{"id": "4.3-unterminated-tail-is-discarded", "spec": ["4.3", "4.4", "4.5"], "level": "MUST",
 "note": "Bytes after the last \\n are not a record even when they would parse, and their absence is not an error.",
 "files": {"s.tsvz": "a\tx\nb\ty"},
 "expect": {"read": [["a", "x"]], "len": 1, "get": [{"args": ["b"], "rows": [["b"]], "exit": 3}]}},
{"id": "4.3-unterminated-tombstone-is-discarded", "spec": ["4.3"], "level": "MUST",
 "note": "An unterminated lone key is not committed, so it deletes nothing.",
 "files": {"s.tsvz": "a\tx\na"},
 "expect": {"read": [["a", "x"]]}},
{"id": "4.3-unterminated-marker-is-discarded", "spec": ["4.3"], "level": "MUST",
 "note": "An unterminated marker line is not committed, so it changes no state.",
 "files": {"s.tsvz": "a\tx\n#_return_defaults_when_missing_#\tfalse"},
 "expect": {"get": [{"args": ["m"], "rows": [["m"]], "exit": 3}]}},
{"id": "4.3-tail-is-discarded-per-part", "spec": ["4.3", "17.4"], "level": "MUST",
 "note": "Each part's unterminated tail is dropped; it is not joined to the next part's first line.",
 "files": {"s.tsvz": "a\tx\nb\ty", "s.tsvz.1": "c\tz\n"},
 "expect": {"read": [["a", "x"], ["c", "z"]], "len": 2}},
{"id": "4.6-empty-file-is-an-empty-store", "spec": ["4.6"], "level": "MUST",
 "note": "A zero-byte part is a valid, empty store.",
 "files": {"s.tsvz": ""},
 "expect": {"read": [], "len": 0, "keys": []}},
{"id": "4.6-comments-and-markers-only", "spec": ["4.6"], "level": "MUST",
 "note": "A part holding only comments and markers is an empty store.",
 "files": {"s.tsvz": "#key\tvalue\n#_version_#\t1\n#_defaults_#\td\n"},
 "expect": {"read": [], "len": 0}}
]}
```

`TSVZ/conformance/cases/reader/06-versions.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "6.4-no-version-line-means-version-1", "spec": ["6.4"], "level": "MUST",
 "note": "A store without #_version_# is read under version 1.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\na\tx\n"},
 "expect": {"read": [["a", "x", "d2"]]}},
{"id": "6.3-higher-version-reads-as-highest-known", "spec": ["6.3", "6.4"], "level": "MUST",
 "note": "A version-1 reader applies version 1 to lines declared version 3, so version-1 markers still work.",
 "files": {"s.tsvz": "#_version_#\t3\n#_defaults_#\td1\td2\na\tx\n"},
 "expect": {"read": [["a", "x", "d2"]], "len": 1}},
{"id": "6.4-version-can-switch", "spec": ["6.4"], "level": "MUST",
 "note": "A store may switch versions 2, 1 and back to the default; every row is still read.",
 "files": {"s.tsvz": "#_version_#\t2\na\tx\n#_version_#\t1\nb\ty\n#_version_#\nc\tz\n"},
 "expect": {"read": [["a", "x"], ["b", "y"], ["c", "z"]]}},
{"id": "6.4-unknown-token-from-a-higher-version", "spec": ["6.4", "13.4"], "level": "MUST",
 "note": "A token a later version might define is passed through literally.",
 "files": {"s.tsvz": "#_version_#\t2\na\tx<TAB>y\n"},
 "expect": {"read": [["a", "x<TAB>y"]]}}
]}
```

`TSVZ/conformance/cases/reader/07-pipeline.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "7.6-tombstone-is-decided-before-stripping", "spec": ["7.6", "9.1"], "level": "MUST",
 "note": "\"a \" has no delimiter, so it is a tombstone; its key strips to a, which it deletes.",
 "files": {"s.tsvz": "a\tx\na \n"},
 "expect": {"read": [], "len": 0}},
{"id": "7.6-delimiter-then-space-is-not-a-tombstone", "spec": ["7.6", "9.2"], "level": "MUST",
 "note": "\"a<TAB> \" has a delimiter, so it sets a; the cell strips to empty.",
 "files": {"s.tsvz": "a\tx\na\t \n"},
 "expect": {"read": [["a", ""]], "len": 1}},
{"id": "7.7-empty-key-row-is-ignored", "spec": ["7.7", "8.4"], "level": "MUST",
 "note": "A row whose key is empty has no effect.",
 "files": {"s.tsvz": "a\tx\n\ty\n"},
 "expect": {"read": [["a", "x"]], "len": 1}},
{"id": "7.7-blank-line-is-not-a-deletion", "spec": ["7.7", "8.4", "9.3"], "level": "MUST",
 "note": "An empty line is an empty-key row: ignored, not a tombstone.",
 "files": {"s.tsvz": "a\tx\n\nb\ty\n"},
 "expect": {"read": [["a", "x"], ["b", "y"]], "len": 2}},
{"id": "7.7-key-of-spaces-strips-to-empty", "spec": ["7.7", "8.4", "10.1"], "level": "MUST",
 "note": "With stripping on, a key of spaces resolves to empty and the row is ignored, with or without a delimiter.",
 "files": {"s.tsvz": "a\tx\n  \tz\n   \n"},
 "expect": {"read": [["a", "x"]], "len": 1}},
{"id": "7.7-key-of-spaces-without-stripping", "spec": ["7.7", "10.3"], "level": "MUST",
 "note": "With stripping off, a key of one space is a real key.",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tfalse\n \tz\n"},
 "expect": {"read": [[" ", "z"]], "keys": [" "]}},
{"id": "7.9-strip-before-decode", "spec": ["7.9", "10.2"], "level": "MUST",
 "note": "Trailing spaces are stripped from the raw field, then tokens are decoded, so an encoded newline survives.",
 "files": {"s.tsvz": "a\tx<LF> \n"},
 "expect": {"read": [["a", "x\n"]]}},
{"id": "7.10-update-keeps-first-position", "spec": ["7.10", "3.4"], "level": "MUST",
 "note": "A later record changes a key's value, not its order.",
 "files": {"s.tsvz": "b\t1\na\t2\nb\t3\n"},
 "expect": {"read": [["b", "3"], ["a", "2"]], "keys": ["b", "a"]}},
{"id": "8.3-escaped-hash-key-is-data", "spec": ["8.3", "13.2"], "level": "MUST",
 "note": "A key written <#>foo reads as #foo and is data, not a comment.",
 "files": {"s.tsvz": "<#>foo\tx\n"},
 "expect": {"read": [["#foo", "x"]], "keys": ["#foo"], "get": [{"args": ["#foo"], "rows": [["#foo", "x"]], "exit": 0}]}},
{"id": "8.3-escaped-marker-name-is-a-data-key", "spec": ["8.3", "12.2.1"], "level": "MUST",
 "note": "<#>_defaults_# is a data key named #_defaults_#, not a marker.",
 "files": {"s.tsvz": "<#>_defaults_#\tq\na\tx\n"},
 "expect": {"read": [["#_defaults_#", "q"], ["a", "x"]], "len": 2}}
]}
```

`TSVZ/conformance/cases/reader/09-tombstones.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "9.2-lone-key-deletes", "spec": ["9.2", "9.3"], "level": "MUST",
 "note": "A row with no delimiter deletes its key.",
 "files": {"s.tsvz": "a\tx\nb\ty\na\n"},
 "expect": {"read": [["b", "y"]], "len": 1, "get": [{"args": ["a"], "rows": [["a"]], "exit": 3}]}},
{"id": "9.2-trailing-delimiter-is-not-a-tombstone", "spec": ["9.2", "14.2"], "level": "MUST",
 "note": "key<TAB> sets a present-empty cell; only a lone key deletes.",
 "files": {"s.tsvz": "a\tx\na\t\n"},
 "expect": {"read": [["a", ""]], "len": 1, "get": [{"args": ["a"], "rows": [["a", ""]], "exit": 0}]}},
{"id": "9.2-several-empty-cells", "spec": ["9.2"], "level": "MUST",
 "note": "key<TAB><TAB> is a live key with two present-empty cells.",
 "files": {"s.tsvz": "a\t\t\n"},
 "expect": {"read": [["a", "", ""]], "len": 1}},
{"id": "9.3-deleting-an-absent-key-is-a-no-op", "spec": ["9.3"], "level": "MUST",
 "note": "A tombstone for a key that was never written changes nothing.",
 "files": {"s.tsvz": "zz\na\tx\n"},
 "expect": {"read": [["a", "x"]], "len": 1}},
{"id": "3.4-rewrite-after-delete-moves-to-end", "spec": ["3.4", "9.3"], "level": "MUST",
 "note": "The spec's example: a, b, tombstone a, c, a read back in the order b, c, a.",
 "files": {"s.tsvz": "a\t1\nb\t2\na\nc\t3\na\t4\n"},
 "expect": {"read": [["b", "2"], ["c", "3"], ["a", "4"]], "keys": ["b", "c", "a"]}},
{"id": "9.4-defaults-do-not-resurrect", "spec": ["9.4", "14.5"], "level": "MUST",
 "note": "A deleted key stays deleted; a read of it returns the defaults and status 3.",
 "files": {"s.tsvz": "#_defaults_#\td\na\tx\na\n"},
 "expect": {"read": [], "len": 0, "get": [{"args": ["a"], "rows": [["a", "d"]], "exit": 3}]}},
{"id": "9.5-single-column-store-only-deletes", "spec": ["9.5"], "level": "MUST",
 "note": "Rows without a delimiter are all tombstones, so the store is empty.",
 "files": {"s.tsvz": "a\nb\n"},
 "expect": {"read": [], "len": 0}},
{"id": "9.1-tombstone-deletes-the-resolved-key", "spec": ["9.1"], "level": "MUST",
 "note": "A tombstone's key is stripped and decoded, so <#>a and a<sep>b delete #a and a<TAB>b.",
 "files": {"s.tsvz": "<#>a\tx\na<sep>b\ty\nc\tz\n<#>a  \na<sep>b\n"},
 "expect": {"read": [["c", "z"]], "len": 1}}
]}
```

`TSVZ/conformance/cases/reader/10-whitespace.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "10.1-trailing-spaces-are-stripped", "spec": ["10.1", "10.3"], "level": "MUST",
 "note": "Stripping is on by default and applies to the key and every cell.",
 "files": {"s.tsvz": "a  \tx  \ty \n"},
 "expect": {"read": [["a", "x", "y"]], "keys": ["a"]}},
{"id": "10.1-leading-whitespace-is-kept", "spec": ["10.1"], "level": "MUST",
 "note": "Only trailing whitespace is stripped.",
 "files": {"s.tsvz": "  a\t  x\n"},
 "expect": {"read": [["  a", "  x"]], "keys": ["  a"]}},
{"id": "10.3-strip-off-keeps-trailing-spaces", "spec": ["10.3"], "level": "MUST",
 "note": "With #_strip_trailing_whites_# false, trailing spaces stay.",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tfalse\na \tx \n"},
 "expect": {"read": [["a ", "x "]], "keys": ["a "]}},
{"id": "10.2-trailing-tab-is-stripped-in-csvz", "spec": ["10.2"], "level": "MUST",
 "note": "In variants where a tab is data, a trailing tab is stripped like a space.",
 "store": "s.csvz", "files": {"s.csvz": "a,x\t \t\n"},
 "expect": {"read": [["a", "x"]]}},
{"id": "10.2-encoded-trailing-tab-survives", "spec": ["10.2", "7.9"], "level": "MUST",
 "note": "Stripping happens before decoding, so a trailing <sep> stays.",
 "files": {"s.tsvz": "a\tx<sep>\n"},
 "expect": {"read": [["a", "x\t"]]}},
{"id": "10.2-only-space-and-tab-are-stripped", "spec": ["10.2"], "level": "MUST",
 "note": "A trailing no-break space is not whitespace for stripping.",
 "files": {"s.tsvz": "a\tx \n"},
 "expect": {"read": [["a", "x "]]}},
{"id": "10.3-strip-toggles-forward", "spec": ["10.3", "12.1"], "level": "MUST",
 "note": "Each row is stripped under the state in force at that row.",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tfalse\na\tx \n#_strip_trailing_whites_#\ttrue\nb\ty \n"},
 "expect": {"read": [["a", "x "], ["b", "y"]]}},
{"id": "10.4-marker-key-with-trailing-space-is-a-comment", "spec": ["10.4", "12.2.1"], "level": "MUST",
 "note": "Marker keys are matched on raw bytes, so a trailing space makes the line a comment.",
 "files": {"s.tsvz": "#_strip_trailing_whites_# \tfalse\na\tx \n"},
 "expect": {"read": [["a", "x"]]}}
]}
```

`TSVZ/conformance/cases/reader/11-comments.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "11.1-hash-line-is-a-comment", "spec": ["11.1", "11.2"], "level": "MUST",
 "note": "A line whose raw first field starts with # is a comment, with any fields.",
 "files": {"s.tsvz": "#a\tx\n#\n##\nb\ty\n"},
 "expect": {"read": [["b", "y"]], "len": 1}},
{"id": "11.1-comment-with-trailing-space", "spec": ["11.1"], "level": "MUST",
 "note": "Trailing whitespace does not matter to comment classification.",
 "files": {"s.tsvz": "#foo \n#foo\t \nb\ty\n"},
 "expect": {"read": [["b", "y"]], "len": 1}},
{"id": "11.1-leading-space-defeats-a-comment", "spec": ["11.1", "7.3"], "level": "MUST",
 "note": "\" #foo\" is classified on raw bytes, so it is a data key.",
 "files": {"s.tsvz": " #foo\tx\n"},
 "expect": {"read": [[" #foo", "x"]], "keys": [" #foo"]}},
{"id": "11.3-header-row-is-a-comment", "spec": ["11.3"], "level": "MUST",
 "note": "A header is just a comment and carries no data.",
 "files": {"s.tsvz": "#key\tname\tscore\nalice\tAlice\t30\n"},
 "expect": {"read": [["alice", "Alice", "30"]], "len": 1}}
]}
```


- [ ] **Step 4: Run the tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `149 passed`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: no `FAIL`, `warn` or `error` line, and the `conformant:` line shows no ✗ (classes with no cases yet show `–`).

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add conformance/cases/reader/04-framing.json conformance/cases/reader/06-versions.json conformance/cases/reader/07-pipeline.json conformance/cases/reader/09-tombstones.json conformance/cases/reader/10-whitespace.json conformance/cases/reader/11-comments.json tsvzconform_test.py && git commit -m "Add tsvzconform's corpus tests and the reader cases for §4 and §6–§11.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Decision 1 — spec §2.1: conformant readers and writers support all four strict variants**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
A **conformant reader** is software that reconstructs store state from a TSVZ file
in accordance with §6–§15. A **conformant writer** is software that produces TSVZ
files in accordance with this document. Some requirements apply only to one role
and are marked accordingly.
```

with:

```markdown
A **conformant reader** is software that reconstructs store state from a TSVZ file
in accordance with §4–§17. A **conformant writer** is software that produces TSVZ
files in accordance with this document. Both support all four strict variants of
§5. Some requirements apply only to one role and are marked accordingly.
```

- [ ] **Step 7: Decision 1 — its cases**

`TSVZ/conformance/cases/reader/05-variants.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "5.1-csvz-splits-on-comma", "spec": ["5.1", "2.1"], "level": "MUST",
 "note": "A .csvz store's delimiter is the comma.",
 "store": "s.csvz", "files": {"s.csvz": "a,x,y\nb,z\n"},
 "expect": {"read": [["a", "x", "y"], ["b", "z"]], "len": 2, "keys": ["a", "b"]}},
{"id": "5.1-nsvz-splits-on-nul", "spec": ["5.1", "2.1"], "level": "MUST",
 "note": "A .nsvz store's delimiter is NUL.",
 "store": "s.nsvz", "files": {"s.nsvz": "a\u0000x\u0000y\nb\u0000z\n"},
 "expect": {"read": [["a", "x", "y"], ["b", "z"]], "len": 2, "keys": ["a", "b"]}},
{"id": "5.1-psvz-splits-on-pipe", "spec": ["5.1", "2.1"], "level": "MUST",
 "note": "A .psvz store's delimiter is the pipe.",
 "store": "s.psvz", "files": {"s.psvz": "a|x|y\nb|z\n"},
 "expect": {"read": [["a", "x", "y"], ["b", "z"]], "len": 2, "keys": ["a", "b"]}},
{"id": "5.1-other-delimiters-are-data-in-tsvz", "spec": ["5.1"], "level": "MUST",
 "note": "Only the variant's own delimiter splits fields; comma, pipe and NUL are data in .tsvz.",
 "files": {"s.tsvz": "a\tx,y|z\u0000w\n"},
 "expect": {"read": [["a", "x,y|z\u0000w"]]}},
{"id": "5.1-other-delimiters-are-data-in-csvz", "spec": ["5.1", "2.1"], "level": "MUST",
 "note": "In .csvz a tab, a pipe and NUL are data.",
 "store": "s.csvz", "files": {"s.csvz": "a,x\ty|z\u0000w\n"},
 "expect": {"read": [["a", "x\ty|z\u0000w"]]}},
{"id": "5.1-other-delimiters-are-data-in-nsvz", "spec": ["5.1", "2.1"], "level": "MUST",
 "note": "In .nsvz a tab, a comma and a pipe are data.",
 "store": "s.nsvz", "files": {"s.nsvz": "a\u0000x\ty,z|w\n"},
 "expect": {"read": [["a", "x\ty,z|w"]]}},
{"id": "5.1-other-delimiters-are-data-in-psvz", "spec": ["5.1", "2.1"], "level": "MUST",
 "note": "In .psvz a tab, a comma and NUL are data.",
 "store": "s.psvz", "files": {"s.psvz": "a|x\ty,z\u0000w\n"},
 "expect": {"read": [["a", "x\ty,z\u0000w"]]}},
{"id": "5.3-no-quoting", "spec": ["5.3"], "level": "MUST",
 "note": "Quotes have no meaning: a comma inside quotes still splits a .csvz field.",
 "store": "s.csvz", "files": {"s.csvz": "a,\"x,y\"\n"},
 "expect": {"read": [["a", "\"x", "y\""]]}},
{"id": "5.1-tombstone-in-csvz", "spec": ["5.1", "9.2", "2.1"], "level": "MUST",
 "note": "A lone key with no comma deletes it in .csvz.",
 "store": "s.csvz", "files": {"s.csvz": "a,1\nb,2\na\n"},
 "expect": {"read": [["b", "2"]], "len": 1}}
]}
```


- [ ] **Step 8: Decision 1 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `158 passed`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: no `FAIL`, `warn` or `error` line, and the `conformant:` line shows no ✗ (classes with no cases yet show `–`).

- [ ] **Step 9: Decision 1 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/05-variants.json && git commit -m "Say in spec §2.1 that conformant readers and writers support all four strict variants and read by §4–§17, with the variant cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 10: Decision 7 — spec §4.1, §4.7: a reader removes a byte-order mark at the start of each part**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
4.1 A file is encoded in **UTF-8** with no byte-order mark.
```

with:

```markdown
4.1 A file is encoded in **UTF-8** with no byte-order mark. A writer **MUST NOT**
write a byte-order mark.
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
4.6 An empty file (zero bytes), and a file containing only comments and markers,
are valid and represent an empty store.
```

with:

```markdown
4.6 An empty file (zero bytes), and a file containing only comments and markers,
are valid and represent an empty store.

4.7 **Byte-order mark.** A reader **MUST** remove one UTF-8 byte-order mark (the
bytes `EF BB BF`) at the start of each part before it frames records. The mark is
not part of the part's first line, is not fed into any integrity accumulator
(§15.4), and does not change line numbering. A reader MAY warn about it.
```

- [ ] **Step 11: Decision 7 — its cases**

`TSVZ/conformance/cases/reader/04-bom.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "4.7-leading-bom-is-removed", "spec": ["4.7"], "level": "MUST",
 "note": "A byte-order mark is not part of the first line, so the marker on that line still applies.",
 "files": {"s.tsvz": "﻿#_fill_empty_with_default_#\ttrue\n#_defaults_#\td\na\t\n"},
 "expect": {"read": [["a", "d"]], "len": 1, "keys": ["a"]}},
{"id": "4.7-bom-is-removed-in-every-part", "spec": ["4.7", "17.4"], "level": "MUST",
 "note": "Each part may start with its own byte-order mark; a later part's mark does not turn a comment into data.",
 "files": {"s.tsvz": "a\tx\n", "s.tsvz.1": "﻿#c\tz\nb\ty\n"},
 "expect": {"read": [["a", "x"], ["b", "y"]], "len": 2}},
{"id": "4.7-only-a-leading-bom-is-removed", "spec": ["4.7"], "level": "MUST",
 "note": "U+FEFF anywhere but the start of a part is ordinary data.",
 "files": {"s.tsvz": "a\tx\n﻿b\ty\n"},
 "expect": {"read": [["a", "x"], ["﻿b", "y"]], "keys": ["a", "﻿b"]}}
]}
```


- [ ] **Step 12: Decision 7 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `161 passed`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: no `FAIL`, `warn` or `error` line, and the `conformant:` line shows no ✗ (classes with no cases yet show `–`).

- [ ] **Step 13: Decision 7 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/04-bom.json && git commit -m "Make readers remove a byte-order mark at the start of each part (spec §4.7), with its cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 14: Decision 8 — spec §4.8: invalid UTF-8 becomes U+FFFD by maximal subparts**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
(§15.4), and does not change line numbering. A reader MAY warn about it.
```

with:

```markdown
(§15.4), and does not change line numbering. A reader MAY warn about it.

4.8 **Invalid UTF-8.** A reader **MUST** decode bytes that are not well-formed
UTF-8 by replacing each maximal subpart of an ill-formed sequence with U+FFFD (the
Unicode Standard, section 3.9, "U+FFFD Substitution of Maximal Subparts"), and
continue reading. A reader MAY warn about it. Integrity digests (§15) cover the
raw bytes, not the decoded text. Because the delimiters (§5) and `\n` are ASCII,
and an ASCII byte always ends a maximal subpart, decoding a part record by record
or as a whole gives the same text.
```

- [ ] **Step 15: Decision 8 — its cases**

`TSVZ/conformance/cases/reader/04-utf8.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "4.8-invalid-utf8-becomes-replacement-characters", "spec": ["4.8"], "level": "MUST",
 "note": "Each maximal subpart of an ill-formed sequence becomes one U+FFFD: a lone 0xFF, a truncated E2 82, and the three bytes ED A0 80 one by one.",
 "files": {"s.tsvz": {"b64": "YQn/CmIJeMMKYwnigu2ggHoK"}},
 "expect": {"read": [["a", "�"], ["b", "x�"], ["c", "����z"]], "len": 3}},
{"id": "4.8-invalid-utf8-in-a-key", "spec": ["4.8"], "level": "MUST",
 "note": "Replacement happens before the key is resolved, so the key is U+FFFD.",
 "files": {"s.tsvz": {"b64": "/wl2Cg=="}},
 "expect": {"read": [["�", "v"]], "keys": ["�"]}}
]}
```


- [ ] **Step 16: Decision 8 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `163 passed`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: no `FAIL`, `warn` or `error` line, and the `conformant:` line shows no ✗ (classes with no cases yet show `–`).

- [ ] **Step 17: Decision 8 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/04-utf8.json && git commit -m "Make readers decode invalid UTF-8 to U+FFFD by maximal subparts (spec §4.8), with its cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: Reader cases for §12–§14, with decisions 4 and 5

**Files:**
- Create: `TSVZ/conformance/cases/reader/12-markers.json`
- Create: `TSVZ/conformance/cases/reader/13-escaping.json`
- Create: `TSVZ/conformance/cases/reader/14-defaults.json`
- Create: `TSVZ/conformance/cases/reader/13-token-case.json`
- Create: `TSVZ/conformance/cases/reader/12-marker-values.json`
- Modify: `TSVZ/tsvz-spec-v1.md` (decisions 4, 5)

**Interfaces:**
- Consumes: Task 9.
- Produces: Spec §12.4.1, §12.4.5, §13.4.

- [ ] **Step 1: Write the case files**

Write these files exactly. Each expectation follows from the clause its `note` gives.

`TSVZ/conformance/cases/reader/12-markers.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "12.2.1-marker-keys-ignore-case", "spec": ["12.2.1", "12.4.1"], "level": "MUST",
 "note": "Marker keys and boolean values are matched ignoring ASCII case.",
 "files": {"s.tsvz": "#_DEFAULTS_#\td\n#_Fill_Empty_With_Default_#\tTRUE\na\t\n"},
 "expect": {"read": [["a", "d"]]}},
{"id": "12.2.1-pattern-is-anchored", "spec": ["12.2.1", "11.1"], "level": "MUST",
 "note": "#_defaults_#x does not match the reserved pattern as a whole, so it is a comment.",
 "files": {"s.tsvz": "#_defaults_#x\tq\na\tx\n"},
 "expect": {"read": [["a", "x"]], "get": [{"args": ["m"], "rows": [["m"]], "exit": 3}]}},
{"id": "12.3-unknown-markers-are-ignored", "spec": ["12.3", "12.2.2"], "level": "MUST",
 "note": "Unrecognized official-form and extension-form markers have no effect and are not data.",
 "files": {"s.tsvz": "#_frobnicate_#\tx\n#__ext__#\ty\na\tz\n"},
 "expect": {"read": [["a", "z"]], "len": 1}},
{"id": "12.1-empty-value-resets", "spec": ["12.1", "12.4.1"], "level": "MUST",
 "note": "A marker with an empty value resets to its built-in default (stripping on).",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tfalse\n#_strip_trailing_whites_#\t\na\tx \n"},
 "expect": {"read": [["a", "x"]]}},
{"id": "12.1-lone-marker-key-resets", "spec": ["12.1"], "level": "MUST",
 "note": "A marker line with no value resets it: here the defaults become empty.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\n#_defaults_#\na\tx\n"},
 "expect": {"read": [["a", "x"]], "get": [{"args": ["m"], "rows": [["m"]], "exit": 3}]}},
{"id": "12.1-later-marker-replaces", "spec": ["12.1", "14.1"], "level": "MUST",
 "note": "A later #_defaults_# replaces the earlier one completely; nothing is merged.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\n#_defaults_#\te1\na\tx\n"},
 "expect": {"read": [["a", "x"]], "get": [{"args": ["m"], "rows": [["m", "e1"]], "exit": 3}]}},
{"id": "12.1-markers-act-forward-only", "spec": ["12.1", "12.4.4"], "level": "MUST",
 "note": "A marker affects only the records after it.",
 "files": {"s.tsvz": "a\t\n#_fill_empty_with_default_#\ttrue\n#_defaults_#\td\nb\t\n"},
 "expect": {"read": [["a", ""], ["b", "d"]]}},
{"id": "12.4.1-marker-values-are-decoded", "spec": ["12.4.1", "10.4", "13.6"], "level": "MUST",
 "note": "Marker values are token-decoded like fields.",
 "files": {"s.tsvz": "#_defaults_#\tx<sep>y\t<lt>z\na\tv\n"},
 "expect": {"read": [["a", "v", "<z"]], "get": [{"args": ["m"], "rows": [["m", "x\ty", "<z"]], "exit": 3}]}},
{"id": "12.4.3-advisory-markers-do-not-change-data", "spec": ["12.4.3"], "level": "MUST",
 "note": "#_rotate_# and #_write_ack_# are hints with no effect on what is read.",
 "files": {"s.tsvz": "#_rotate_#\tdelete\n#_write_ack_#\tdisk\na\tx\n"},
 "expect": {"read": [["a", "x"]], "len": 1}}
]}
```

`TSVZ/conformance/cases/reader/13-escaping.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "13.2-every-token-decodes", "spec": ["13.2", "13.4"], "level": "MUST",
 "note": "<sep>, <LF>, <lt> and <#> read back as the delimiter, a newline, < and #.",
 "files": {"s.tsvz": "a\tx<sep>y\tl<LF>f\tl<lt>t\th<#>h\n"},
 "expect": {"read": [["a", "x\ty", "l\nf", "l<t", "h#h"]]}},
{"id": "13.5-literal-token-text", "spec": ["13.5"], "level": "MUST",
 "note": "<lt>sep> and <lt>LF> read back as the literal text <sep> and <LF>.",
 "files": {"s.tsvz": "a\t<lt>sep>\t<lt>LF>\n"},
 "expect": {"read": [["a", "<sep>", "<LF>"]]}},
{"id": "13.4-decoding-is-not-recursive", "spec": ["13.4"], "level": "MUST",
 "note": "Decoding runs once: <lt>lt> is <lt>, and <lt>#> is <#>.",
 "files": {"s.tsvz": "a\t<lt>lt>\t<lt>#>\n"},
 "expect": {"read": [["a", "<lt>", "<#>"]]}},
{"id": "13.4-unknown-token-passes-through", "spec": ["13.4"], "level": "MUST",
 "note": "A <...> that is not a token is kept literally.",
 "files": {"s.tsvz": "a\t<foo>\t<>\t<sep\n"},
 "expect": {"read": [["a", "<foo>", "<>", "<sep"]]}},
{"id": "13.4-bare-lt-passes-through", "spec": ["13.4"], "level": "MUST",
 "note": "A < that begins no token, which a conformant writer never produces, is read literally.",
 "files": {"s.tsvz": "a\tx<y\tz<\n"},
 "expect": {"read": [["a", "x<y", "z<"]]}},
{"id": "13.4-splitting-happens-before-decoding", "spec": ["13.4", "7.2"], "level": "MUST",
 "note": "An encoded delimiter in the key never splits the row.",
 "files": {"s.tsvz": "a<sep>b\tc\n"},
 "expect": {"read": [["a\tb", "c"]], "keys": ["a\tb"], "len": 1}},
{"id": "13.2-hash-token-anywhere", "spec": ["13.2"], "level": "MUST",
 "note": "<#> decodes to # wherever it appears.",
 "files": {"s.tsvz": "a\tx<#>y\n"},
 "expect": {"read": [["a", "x#y"]]}},
{"id": "13.2-sep-is-the-csvz-delimiter", "spec": ["13.2", "2.1"], "level": "MUST",
 "note": "In .csvz <sep> stands for a comma.",
 "store": "s.csvz", "files": {"s.csvz": "a,x<sep>y\n"},
 "expect": {"read": [["a", "x,y"]]}},
{"id": "13.2-sep-is-the-nsvz-delimiter", "spec": ["13.2", "2.1"], "level": "MUST",
 "note": "In .nsvz <sep> stands for NUL.",
 "store": "s.nsvz", "files": {"s.nsvz": "a\u0000x<sep>y\n"},
 "expect": {"read": [["a", "x\u0000y"]]}},
{"id": "13.2-sep-is-the-psvz-delimiter", "spec": ["13.2", "2.1"], "level": "MUST",
 "note": "In .psvz <sep> stands for a pipe.",
 "store": "s.psvz", "files": {"s.psvz": "a|x<sep>y\n"},
 "expect": {"read": [["a", "x|y"]]}},
{"id": "13.4-non-ascii-passes-through", "spec": ["13.4", "4.1"], "level": "MUST",
 "note": "UTF-8 text of every plane reads back unchanged.",
 "files": {"s.tsvz": "é\tß\t漢字\t😀\n"},
 "expect": {"read": [["é", "ß", "漢字", "😀"]], "keys": ["é"]}}
]}
```

`TSVZ/conformance/cases/reader/14-defaults.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "14-appendix-c-worked-example", "spec": ["14", "3.4", "9.2"], "level": "MUST",
 "note": "The spec's Appendix C: alice and carol are live, bob is deleted and reads as the defaults.",
 "files": {"s.tsvz": "#_version_#\t1\n#_defaults_#\tguest\t0\nalice\tAlice\t30\nbob\tBob\ncarol\t\t25\nalice\tAlice\t31\nbob\n"},
 "expect": {"read": [["alice", "Alice", "31"], ["carol", "", "25"]], "len": 2, "keys": ["alice", "carol"],
  "get": [{"args": ["bob"], "rows": [["bob", "guest", "0"]], "exit": 3}, {"args": ["carol", "alice"], "rows": [["carol", "", "25"], ["alice", "Alice", "31"]], "exit": 0}]}},
{"id": "14.3-absent-cells-take-defaults", "spec": ["14.3"], "level": "MUST",
 "note": "Cells past a row's written width are the defaults.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\td3\na\tx\n"},
 "expect": {"read": [["a", "x", "d2", "d3"]]}},
{"id": "14.2-present-empty-stays-empty", "spec": ["14.2"], "level": "MUST",
 "note": "With fill-empty off (its default), a written empty cell stays empty.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\na\t\tq\n"},
 "expect": {"read": [["a", "", "q"]]}},
{"id": "14.2-fill-empty-takes-the-default", "spec": ["14.2"], "level": "MUST",
 "note": "With fill-empty on, a written empty cell takes its default.",
 "files": {"s.tsvz": "#_fill_empty_with_default_#\ttrue\n#_defaults_#\td1\td2\na\t\tq\n"},
 "expect": {"read": [["a", "d1", "q"]]}},
{"id": "14.2-fill-empty-after-stripping", "spec": ["14.2", "7.9"], "level": "MUST",
 "note": "A cell of spaces strips to empty and then takes its default.",
 "files": {"s.tsvz": "#_fill_empty_with_default_#\ttrue\n#_defaults_#\td\na\t  \n"},
 "expect": {"read": [["a", "d"]]}},
{"id": "14.4-defaults-bind-at-the-row", "spec": ["14.4", "12.4.4"], "level": "MUST",
 "note": "Each row keeps the defaults in force where it was written.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\na\tx\n#_defaults_#\te1\te2\nb\ty\n"},
 "expect": {"read": [["a", "x", "d2"], ["b", "y", "e2"]]}},
{"id": "14.4-fill-empty-binds-at-the-row", "spec": ["14.4", "12.4.4"], "level": "MUST",
 "note": "Each row keeps the fill-empty state in force where it was written.",
 "files": {"s.tsvz": "#_defaults_#\td\na\t\n#_fill_empty_with_default_#\ttrue\nb\t\n#_fill_empty_with_default_#\tfalse\nc\t\n"},
 "expect": {"read": [["a", ""], ["b", "d"], ["c", ""]]}},
{"id": "14.4-rewrite-binds-to-current-defaults", "spec": ["14.4"], "level": "MUST",
 "note": "Writing a key again binds it to the defaults in force at the new row.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\na\tx\n#_defaults_#\te1\te2\na\tz\n"},
 "expect": {"read": [["a", "z", "e2"]]}},
{"id": "14.5-missing-key-reads-current-defaults", "spec": ["14.5", "20.2", "20.5.3"], "level": "MUST",
 "note": "A missing key reads as the defaults at the end of replay; get prints that row and exits 3.",
 "files": {"s.tsvz": "#_defaults_#\td1\na\tx\n#_defaults_#\te1\te2\n"},
 "expect": {"get": [{"args": ["m"], "rows": [["m", "e1", "e2"]], "exit": 3}, {"args": ["a", "m"], "rows": [["a", "x"], ["m", "e1", "e2"]], "exit": 3}]}},
{"id": "14.5-not-found-prints-nothing", "spec": ["14.5", "20.2"], "level": "MUST",
 "note": "With #_return_defaults_when_missing_# false, get prints nothing for a missing key and exits 3.",
 "files": {"s.tsvz": "#_defaults_#\td\n#_return_defaults_when_missing_#\tfalse\na\tx\n"},
 "expect": {"get": [{"args": ["m"], "rows": [], "exit": 3}, {"args": ["a", "m"], "rows": [["a", "x"]], "exit": 3}]}},
{"id": "14.5-return-defaults-reset", "spec": ["14.5", "12.1"], "level": "MUST",
 "note": "A lone #_return_defaults_when_missing_# resets it to true.",
 "files": {"s.tsvz": "#_defaults_#\td\n#_return_defaults_when_missing_#\tfalse\n#_return_defaults_when_missing_#\n"},
 "expect": {"get": [{"args": ["m"], "rows": [["m", "d"]], "exit": 3}]}},
{"id": "20.2-get-resolves-the-key", "spec": ["20.2", "7.7"], "level": "SHOULD",
 "note": "get strips a requested key's trailing space while stripping is in force, as a reader resolves keys.",
 "files": {"s.tsvz": "a\tx\n"},
 "expect": {"get": [{"args": ["a "], "rows": [["a", "x"]], "exit": 0}]}}
]}
```


- [ ] **Step 2: Run the tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `195 passed`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: no `FAIL`, `warn` or `error` line, and the `conformant:` line shows no ✗ (classes with no cases yet show `–`).

- [ ] **Step 3: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add conformance/cases/reader/12-markers.json conformance/cases/reader/13-escaping.json conformance/cases/reader/14-defaults.json && git commit -m "Add the reader cases for §12–§14.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Decision 4 — spec §13.4: token names are case-sensitive**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
tokens of §13.2; a `<...>` token it does not recognize (for example, one
introduced by a higher version) is **passed through literally**.
```

with:

```markdown
tokens of §13.2; a `<...>` token it does not recognize (for example, one
introduced by a higher version) is **passed through literally**. Token names are
case-sensitive: only the spellings of §13.2 are tokens, so `<lf>` or `<SEP>` is
not recognized and passes through literally.
```

- [ ] **Step 5: Decision 4 — its cases**

`TSVZ/conformance/cases/reader/13-token-case.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "13.4-token-names-are-case-sensitive", "spec": ["13.4"], "level": "MUST",
 "note": "<lf>, <SEP>, <Lt> and <cr> are not tokens.",
 "files": {"s.tsvz": "a\t<lf>\t<SEP>\t<Lt>\t<cr>\n"},
 "expect": {"read": [["a", "<lf>", "<SEP>", "<Lt>", "<cr>"]]}}
]}
```


- [ ] **Step 6: Decision 4 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `196 passed`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: no `FAIL`, `warn` or `error` line, and the `conformant:` line shows no ✗ (classes with no cases yet show `–`).

- [ ] **Step 7: Decision 4 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/13-token-case.json && git commit -m "Say in spec §13.4 that token names are case-sensitive, with its case.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 8: Decision 5 — spec §12.4.1, §12.4.5: boolean spellings, and invalid marker values are ignored**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
12.4.1 Boolean values (`true`/`false`), enumerated values, and `<algo>` names are
matched case-insensitively over ASCII. An empty value resets a stateful marker to
its built-in default; for `#_checksum_<algo>_#`, an empty value begins a new
segment without verification (§15.3).
```

with:

```markdown
12.4.1 A boolean value is `true` or `false`; `yes`, `on` and `1` are also accepted
as `true`, and `no`, `off` and `0` as `false`. A writer SHOULD write `true` or
`false`. Boolean values, enumerated values, and `<algo>` names are matched
case-insensitively over ASCII. The value of `#_version_#` is written in ASCII
decimal digits. An empty value resets a stateful marker to its built-in default;
for `#_checksum_<algo>_#`, an empty value begins a new segment without
verification (§15.3).
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
processed under the earlier state. See §14.4 and §19.4 for the consequences for
defaulting and snapshots.
```

with:

```markdown
processed under the earlier state. See §14.4 and §19.4 for the consequences for
defaulting and snapshots.

12.4.5 **Invalid values.** A marker line whose value is not valid for its marker
(for example `#_version_#` with `abc` or `0`, or `#_strip_trailing_whites_#` with
`maybe`) is ignored: the marker keeps the state it had before the line. A reader
MAY warn about it.
```

- [ ] **Step 9: Decision 5 — its cases**

`TSVZ/conformance/cases/reader/12-marker-values.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "12.4.1-boolean-spellings", "spec": ["12.4.1"], "level": "MUST",
 "note": "no, off and 0 mean false; yes, on and 1 mean true.",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tno\na\tx \n#_strip_trailing_whites_#\tyes\nb\ty \n#_strip_trailing_whites_#\tOff\nc\tz \n#_strip_trailing_whites_#\tON\nd\tw \n#_strip_trailing_whites_#\t0\ne\tv \n#_strip_trailing_whites_#\t1\nf\tu \n"},
 "expect": {"read": [["a", "x "], ["b", "y"], ["c", "z "], ["d", "w"], ["e", "v "], ["f", "u"]]}},
{"id": "12.4.5-invalid-boolean-is-ignored", "spec": ["12.4.5"], "level": "MUST",
 "note": "An invalid value leaves the marker as it was (stripping stays off).",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tfalse\n#_strip_trailing_whites_#\tmaybe\na\tx \n"},
 "expect": {"read": [["a", "x "]]}},
{"id": "12.4.5-invalid-return-defaults-is-ignored", "spec": ["12.4.5", "14.5"], "level": "MUST",
 "note": "An invalid value leaves #_return_defaults_when_missing_# false.",
 "files": {"s.tsvz": "#_return_defaults_when_missing_#\tfalse\n#_return_defaults_when_missing_#\tnope\na\tx\n"},
 "expect": {"get": [{"args": ["m"], "rows": [], "exit": 3}]}},
{"id": "12.4.5-marker-value-is-not-stripped", "spec": ["12.4.5", "10.4"], "level": "MUST",
 "note": "Marker lines are not stripped, so \"false \" is not a valid boolean and stripping stays on.",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tfalse \na\tx \n"},
 "expect": {"read": [["a", "x"]]}}
]}
```


- [ ] **Step 10: Decision 5 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `200 passed`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`. Nothing else fails.

- [ ] **Step 11: Decision 5 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/12-marker-values.json && git commit -m "Define the boolean spellings and ignore marker lines with invalid values (spec §12.4.1, §12.4.5), with their cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: Reader cases for §15–§17, with decisions 2, 11, 3 and 6

**Files:**
- Create: `TSVZ/conformance/cases/reader/17-multipart.json`
- Create: `TSVZ/conformance/cases/reader/16-compression.json`
- Create: `TSVZ/conformance/cases/reader/15-integrity.json`
- Create: `TSVZ/conformance/cases/reader/15-verify-lines.json`
- Create: `TSVZ/conformance/cases/reader/15-digest-case.json`
- Modify: `TSVZ/tsvz-spec-v1.md` (decisions 2, 11, 3, 6)
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Task 9.
- Produces: `test_digests_in_the_corpus_are_right` and its helpers; spec §15.2, §15.2.1, §16.2, §16.5, and the `verify` row of §20.2.

- [ ] **Step 1: Write the case files**

Write these files exactly. Each expectation follows from the clause its `note` gives.

`TSVZ/conformance/cases/reader/17-multipart.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "17.3-hex-order-not-lexical", "spec": ["17.3"], "level": "MUST",
 "note": "Ordinal 10 (16) comes after f (15), though it sorts before it as text.",
 "files": {"s.tsvz.f": "k\tfrom-f\n", "s.tsvz.10": "k\tfrom-10\n"},
 "expect": {"read": [["k", "from-10"]]}},
{"id": "17.3-leading-zeros-are-insignificant", "spec": ["17.3"], "level": "MUST",
 "note": "Ordinal 002 is 2, after 1, though \"002\" sorts first as text.",
 "files": {"s.tsvz.1": "k\tfrom-1\n", "s.tsvz.002": "k\tfrom-002\n"},
 "expect": {"read": [["k", "from-002"]]}},
{"id": "17.3-uppercase-hex-ordinals", "spec": ["17.3", "17.2"], "level": "MUST",
 "note": "Ordinals are hexadecimal in either case: a (10) comes before B (11).",
 "files": {"s.tsvz.a": "k\tfrom-a\n", "s.tsvz.B": "k\tfrom-B\n"},
 "expect": {"read": [["k", "from-B"]]}},
{"id": "17.1-unnumbered-file-is-part-0", "spec": ["17.1"], "level": "MUST",
 "note": "The unnumbered file comes before every numbered part, ordinal 0 included.",
 "files": {"s.tsvz": "k\tfrom-file\n", "s.tsvz.0": "k\tfrom-0\n"},
 "expect": {"read": [["k", "from-0"]]}},
{"id": "17.6-uuid-width-ordinals", "spec": ["17.6", "17.3"], "level": "MUST",
 "note": "Full-width UUIDv7 ordinals order numerically after short ones.",
 "files": {"s.tsvz.ff": "k\t1\n", "s.tsvz.0192a3b4c5d67e8f9a0b1c2d3e4f5061": "k\t2\n", "s.tsvz.0192a3b4c5d67e8f9a0b1c2d3e4f5062": "k\t3\nj\t4\n"},
 "expect": {"read": [["k", "3"], ["j", "4"]]}},
{"id": "17.2-rotated-parts-are-excluded", "spec": ["17.2", "19.5"], "level": "MUST",
 "note": "A part named .rotated is not loaded.",
 "files": {"s.tsvz": "a\t1\n", "s.tsvz.5.rotated": "b\t2\n", "s.tsvz.6": "c\t3\n"},
 "expect": {"read": [["a", "1"], ["c", "3"]], "len": 2}},
{"id": "17.2-rotated-compressed-part-is-excluded", "spec": ["17.2", "19.5"], "level": "MUST",
 "note": ".rotated before a compression suffix also excludes the part.",
 "files": {"s.tsvz": "a\t1\n", "s.tsvz.5.rotated.gz": {"b64": "H4sIAAAAAAAA/0viNOICAHRmH7IEAAAA"}},
 "expect": {"read": [["a", "1"]]}},
{"id": "17.4-markers-span-parts", "spec": ["17.4", "12.1"], "level": "MUST",
 "note": "Marker state carries from one part into the next.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\n", "s.tsvz.1": "a\tx\n"},
 "expect": {"read": [["a", "x", "d2"]]}},
{"id": "17.4-last-wins-across-parts", "spec": ["17.4", "3.3"], "level": "MUST",
 "note": "A later part overrides and deletes keys of an earlier one.",
 "files": {"s.tsvz": "a\t1\nb\t2\n", "s.tsvz.1": "a\t3\nb\n"},
 "expect": {"read": [["a", "3"]], "len": 1}},
{"id": "17.4-rewrite-in-a-later-part-moves-to-the-end", "spec": ["17.4", "3.4"], "level": "MUST",
 "note": "A key deleted and written again in a later part moves to the end.",
 "files": {"s.tsvz": "a\t1\nb\t2\n", "s.tsvz.1": "a\na\t9\n"},
 "expect": {"read": [["b", "2"], ["a", "9"]], "keys": ["b", "a"]}},
{"id": "17.1-numbered-parts-without-the-file", "spec": ["17.1", "20.1.3"], "level": "MUST",
 "note": "STORE names the unnumbered path even when only numbered parts exist.",
 "files": {"s.tsvz.1": "a\tx\n", "s.tsvz.2": "b\ty\n"},
 "expect": {"read": [["a", "x"], ["b", "y"]]}}
]}
```


- [ ] **Step 2: Run the tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `211 passed`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`. Nothing else fails.

- [ ] **Step 3: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add conformance/cases/reader/17-multipart.json && git commit -m "Add the multi-part reader cases (§17).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Decision 2 — spec §16.2, §16.5: gzip is required, other codecs optional, an unsupported codec fails**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
correctly. Supported suffixes: `.gz`/`.gzip`, `.bz2`/`.bzip2`, `.xz`/`.lzma`,
`.zst`/`.zstd`. Example: `events.tsvz.<ordinal>.zst`.
```

with:

```markdown
correctly. The defined suffixes are `.gz`/`.gzip` (gzip), `.bz2`/`.bzip2`
(bzip2), `.xz`/`.lzma` (xz) and `.zst`/`.zstd` (Zstandard). Example:
`events.tsvz.<ordinal>.zst`.
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
unit. Such a writer SHOULD keep the active part uncompressed and leave compression of
prior immutable parts to a later pass (§17.7). Where a compressed part is not held
open as a stream, appending requires rewriting it as a whole.
```

with:

```markdown
unit. Such a writer SHOULD keep the active part uncompressed and leave compression of
prior immutable parts to a later pass (§17.7). Where a compressed part is not held
open as a stream, appending requires rewriting it as a whole.

16.5 **Supported codecs.** A conformant reader **MUST** support gzip; support for
the other codecs of §16.2 is optional. A reader that meets a part whose codec it
does not support **MUST** fail to read the store rather than skip the part or read
its bytes as uncompressed text. A command-line tool reports this as a failure
(§20.6, status 1); a handler answers as §21.8.5 says.
```

- [ ] **Step 5: Decision 2 — its cases**

`TSVZ/conformance/cases/reader/16-compression.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "16.5-gzip-store", "spec": ["16.5", "16.2"], "level": "MUST",
 "note": "A store whose only part is gzip-compressed reads like the plain file.",
 "store": "s.tsvz.gz", "files": {"s.tsvz.gz": {"b64": "H4sIAAAAAAAA/0vkrOBK4qzkAgA0guzBCAAAAA=="}},
 "expect": {"read": [["a", "x"], ["b", "y"]], "len": 2}},
{"id": "16.2-gzip-long-suffix", "spec": ["16.2", "16.5"], "level": "MUST",
 "note": ".gzip is the same codec as .gz.",
 "store": "s.tsvz.gzip", "files": {"s.tsvz.gzip": {"b64": "H4sIAAAAAAAA/0vkrOBK4qzkAgA0guzBCAAAAA=="}},
 "expect": {"read": [["a", "x"], ["b", "y"]]}},
{"id": "16.5-gzip-numbered-part", "spec": ["16.5", "17.2"], "level": "MUST",
 "note": "A numbered part may be compressed while the others are not.",
 "files": {"s.tsvz": "a\tx\n", "s.tsvz.1.gz": {"b64": "H4sIAAAAAAAA/0virOQCALrwkqEEAAAA"}},
 "expect": {"read": [["a", "x"], ["b", "y"]]}},
{"id": "16.3-variant-under-the-codec-suffix", "spec": ["16.3", "5.1"], "level": "MUST",
 "note": "The delimiter comes from the extension under the compression suffix.",
 "store": "s.csvz.gz", "files": {"s.csvz.gz": {"b64": "H4sIAAAAAAAA/0vUqdCp5AIAD6Ov9wYAAAA="}},
 "expect": {"read": [["a", "x", "y"]]}},
{"id": "16.2-bzip2-store", "spec": ["16.2"], "level": "MUST", "requires": ["bzip2"],
 "note": "A bzip2 store reads like the plain file.",
 "store": "s.tsvz.bz2", "files": {"s.tsvz.bz2": {"b64": "QlpoOTFBWSZTWZoDLZsAAAJBgAAwMAAAYCAAIaaZoMA+AIWF3JFOFCQmgMtmwA=="}},
 "expect": {"read": [["a", "x"], ["b", "y"]]}},
{"id": "16.2-bzip2-long-suffix", "spec": ["16.2"], "level": "MUST", "requires": ["bzip2"],
 "note": ".bzip2 is the same codec as .bz2.",
 "files": {"s.tsvz": "a\tx\n", "s.tsvz.1.bzip2": {"b64": "QlpoOTFBWSZTWbZpgWcAAAFBgAAwEAAAICAAIZpoM00yvF3JFOFCQtmmBZw="}},
 "expect": {"read": [["a", "x"], ["b", "y"]]}},
{"id": "16.2-xz-store", "spec": ["16.2"], "level": "MUST", "requires": ["xz"],
 "note": "An xz store reads like the plain file.",
 "store": "s.tsvz.xz", "files": {"s.tsvz.xz": {"b64": "/Td6WFoAAATm1rRGAgAhARYAAAB0L+WjAQAHYQl4CmIJeQoAVXdMHI5t0xAAASAIuxnZux+2830BAAAAAARZWg=="}},
 "expect": {"read": [["a", "x"], ["b", "y"]]}},
{"id": "16.2-zstd-store", "spec": ["16.2"], "level": "MUST", "requires": ["zstd"],
 "note": "A Zstandard store reads like the plain file.",
 "store": "s.tsvz.zst", "files": {"s.tsvz.zst": {"b64": "KLUv/SAIQQAAYQl4CmIJeQo="}},
 "expect": {"read": [["a", "x"], ["b", "y"]]}},
{"id": "16.2-zstd-long-suffix", "spec": ["16.2"], "level": "MUST", "requires": ["zstd"],
 "note": ".zstd is the same codec as .zst.",
 "files": {"s.tsvz": "a\tx\n", "s.tsvz.1.zstd": {"b64": "KLUv/SAEIQAAYgl5Cg=="}},
 "expect": {"read": [["a", "x"], ["b", "y"]]}},
{"id": "16.5-unsupported-bzip2-part-fails", "spec": ["16.5", "20.6"], "level": "MUST", "requires": ["!bzip2"],
 "note": "A reader without bzip2 fails rather than skip the part or read it as text.",
 "files": {"s.tsvz": "a\tx\n", "s.tsvz.1.bz2": {"b64": "QlpoOTFBWSZTWbZpgWcAAAFBgAAwEAAAICAAIZpoM00yvF3JFOFCQtmmBZw="}},
 "expect": {"read": {"rows": null, "exit": 1}}},
{"id": "16.5-unsupported-xz-part-fails", "spec": ["16.5", "20.6"], "level": "MUST", "requires": ["!xz"],
 "note": "A reader without xz fails rather than skip the part or read it as text.",
 "files": {"s.tsvz": "a\tx\n", "s.tsvz.1.xz": {"b64": "/Td6WFoAAATm1rRGAgAhARYAAAB0L+WjAQADYgl5CgBDNSi9FNG/BgABHARvLJzBH7bzfQEAAAAABFla"}},
 "expect": {"read": {"rows": null, "exit": 1}}},
{"id": "16.5-unsupported-zstd-part-fails", "spec": ["16.5", "20.6"], "level": "MUST", "requires": ["!zstd"],
 "note": "A reader without zstd fails rather than skip the part or read it as text.",
 "files": {"s.tsvz": "a\tx\n", "s.tsvz.1.zst": {"b64": "KLUv/SAEIQAAYgl5Cg=="}},
 "expect": {"read": {"rows": null, "exit": 1}}}
]}
```


- [ ] **Step 6: Decision 2 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `218 passed, 5 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `16.5-unsupported-zstd-part-fails`. Nothing else fails.

- [ ] **Step 7: Decision 2 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/16-compression.json && git commit -m "Require gzip, make the other codecs optional and make an unsupported codec fail (spec §16.2, §16.5), with the compression cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 8: Add the digest test**

Append this to the end of `TSVZ/tsvzconform_test.py` (it recomputes every digest the integrity cases quote: crc32 and sha256 with the standard library, crc32c in pure Python, blake3 when the `blake3` package is installed):

```python
def _crc32c(data):
	crc = 0xffffffff
	for byte in bytearray(data):
		crc ^= byte
		for _ in range(8):
			crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
	return '%08x' % (crc ^ 0xffffffff)


def _digest(algo, data):
	"""The registry's digest of `data` (spec §15.2.1), or None for blake3 without its package and for other names."""
	if algo == 'crc32':
		return '%08x' % (zlib.crc32(data) & 0xffffffff)
	if algo == 'crc32c':
		return _crc32c(data)
	if algo == 'sha256':
		return hashlib.sha256(data).hexdigest()
	if algo != 'blake3':
		return None
	try:
		import blake3
	except ImportError:
		return None
	return blake3.blake3(data).hexdigest()


def _segments(data):
	"""(algo, line number in its part, expected, segment bytes) for every closing checksum marker of a one-part store."""
	found = []
	armed = {}
	data = data[3:] if data.startswith(b'\xef\xbb\xbf') else data
	for number, line in enumerate(data.split(b'\n')[:-1], 1):
		match = re.match(rb'#_checksum_([A-Za-z0-9_-]+)_#(?:\t(.*))?\Z', line, re.I)
		algo = match.group(1).decode().lower() if match else None
		for name in armed:
			if name != algo:
				armed[name] += line + b'\n'
		if algo:
			if algo in armed and match.group(2):
				found.append((algo, number, match.group(2).decode().lower(), armed[algo]))
			armed[algo] = b''
	return found


def test_digests_in_the_corpus_are_right():
	checked = 0
	for case in READER_CASES:
		if len(case.files) != 1 or 'verify' not in case.expect or case.id.startswith('15.3-unclaimed'):
			continue
		data = tc.content_bytes(case.files['s.tsvz'])
		lines = {(l[2], int(l[1])): l for l in case.expect['verify']['lines']}
		for algo, number, expected, segment in _segments(data):
			computed = _digest(algo, segment)
			if computed is None:
				continue
			if (algo, number) in lines:
				assert lines[(algo, number)][3:] == [expected, computed], case.id
			else:
				assert expected == computed, case.id
			checked += 1
	assert checked >= 8
```

- [ ] **Step 9: Decision 11 — spec §15.2.1: the digest algorithm registry**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
15.2 **Marker.** `#_checksum_<algo>_#`, where `<algo>` names a digest algorithm
(for example `crc32`, `crc32c`, `sha256`, `blake3`). `<algo>` is part of the
case-insensitive reserved key and matches the inner character class of §12.2.1. The
value is the lowercase hexadecimal expected digest, or empty.
```

with:

```markdown
15.2 **Marker.** `#_checksum_<algo>_#`, where `<algo>` names a digest algorithm
(§15.2.1). `<algo>` is part of the case-insensitive reserved key and matches the
inner character class of §12.2.1. The value is the lowercase hexadecimal expected
digest, or empty.

15.2.1 **Algorithms.** This specification defines these names:

| `<algo>` | Algorithm | Digest written as |
|---|---|---|
| `crc32` | CRC-32/ISO-HDLC, the CRC-32 of zlib, gzip and PNG | its 32-bit value as 8 hexadecimal digits, most significant first |
| `crc32c` | CRC-32C (Castagnoli), as in iSCSI | its 32-bit value as 8 hexadecimal digits, most significant first |
| `sha256` | SHA-256 (FIPS 180-4) | its 32 output bytes, in order, as 64 hexadecimal digits |
| `blake3` | BLAKE3 with its default 32-byte output | its 32 output bytes, in order, as 64 hexadecimal digits |

Implementing any of them is optional (§15.3). A reader MAY implement other
algorithms under other names; their meaning is implementation-defined.
```

- [ ] **Step 10: Decision 11 — its cases**

`TSVZ/conformance/cases/reader/15-integrity.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "15.3-first-marker-arms-and-ignores-its-value", "spec": ["15.3"], "level": "MUST", "requires": ["crc32"],
 "note": "The first #_checksum_crc32_# only starts a segment; the value it carries is not checked.",
 "files": {"s.tsvz": "#_checksum_crc32_#\tdeadbeef\na\tx\n"},
 "expect": {"verify": {"lines": [], "exit": 0}, "read": [["a", "x"]]}},
{"id": "15.3-matching-segment-verifies", "spec": ["15.3", "15.4", "15.2.1"], "level": "MUST", "requires": ["crc32"],
 "note": "The digest of the lines between two markers matches the second marker.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\nb\ty\n#_checksum_crc32_#\tc1ec8234\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.3-empty-value-starts-a-new-segment", "spec": ["15.3"], "level": "MUST", "requires": ["crc32"],
 "note": "A later marker with an empty value checks nothing and starts a new segment.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\n#_checksum_crc32_#\nb\ty\n#_checksum_crc32_#\ta192f0ba\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.4-crlf-bytes-are-covered", "spec": ["15.4"], "level": "MUST", "requires": ["crc32"],
 "note": "A line contributes its raw bytes, including the \\r before its \\n.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\r\n#_checksum_crc32_#\t10e148d1\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.4-comments-are-covered", "spec": ["15.4", "11.2"], "level": "MUST", "requires": ["crc32"],
 "note": "Comments count toward the digest.",
 "files": {"s.tsvz": "#_checksum_crc32_#\n#c\na\tx\n#_checksum_crc32_#\t2c912823\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.4-other-algorithms-markers-are-covered", "spec": ["15.4", "15.5", "15.6"], "level": "MUST", "requires": ["crc32", "sha256"],
 "note": "Each algorithm has its own segments, and each covers the other's marker lines.",
 "files": {"s.tsvz": "#_checksum_crc32_#\n#_checksum_sha256_#\na\tx\n#_checksum_crc32_#\ta7601900\nb\ty\n#_checksum_sha256_#\t71a89e11f30550171639d347f0a1bd3b70a1454cbf69e6a2f7499140545fbe37\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.4-segment-spans-parts", "spec": ["15.4", "17.4"], "level": "MUST", "requires": ["crc32"],
 "note": "Parts are one concatenation, so a segment can start in one part and end in the next.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\n", "s.tsvz.1": "b\ty\n#_checksum_crc32_#\tc1ec8234\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.3-unknown-algorithm-is-inert", "spec": ["15.3", "12.3"], "level": "MUST",
 "note": "An algorithm no reader implements is ignored: nothing is verified.",
 "files": {"s.tsvz": "#_checksum_nosuchalgo_#\na\tx\n#_checksum_nosuchalgo_#\t00\n"},
 "expect": {"verify": {"lines": [], "exit": 0}, "read": [["a", "x"]]}},
{"id": "15.2.1-crc32-digest", "spec": ["15.2.1", "15.4"], "level": "MUST", "requires": ["crc32"],
 "note": "The crc32 digest of a segment, written as the registry says.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\n#_checksum_crc32_#\t00000000\n"},
 "expect": {"verify": {"lines": [["{store}", "3", "crc32", "00000000", "aa3c6e15"]], "exit": 4}}},
{"id": "15.2.1-crc32c-digest", "spec": ["15.2.1", "15.4"], "level": "MUST", "requires": ["crc32c"],
 "note": "The crc32c digest of a segment, written as the registry says.",
 "files": {"s.tsvz": "#_checksum_crc32c_#\na\tx\n#_checksum_crc32c_#\t00000000\n"},
 "expect": {"verify": {"lines": [["{store}", "3", "crc32c", "00000000", "3d1e39af"]], "exit": 4}}},
{"id": "15.2.1-sha256-digest", "spec": ["15.2.1", "15.4"], "level": "MUST", "requires": ["sha256"],
 "note": "The sha256 digest of a segment, written as the registry says.",
 "files": {"s.tsvz": "#_checksum_sha256_#\na\tx\n#_checksum_sha256_#\t00000000\n"},
 "expect": {"verify": {"lines": [["{store}", "3", "sha256", "00000000", "d90081846a82464321aca1346aac2f7711addd72b50c0ea381972c73d463ff45"]], "exit": 4}}},
{"id": "15.2.1-blake3-digest", "spec": ["15.2.1", "15.4"], "level": "MUST", "requires": ["blake3"],
 "note": "The blake3 digest of a segment, written as the registry says.",
 "files": {"s.tsvz": "#_checksum_blake3_#\na\tx\n#_checksum_blake3_#\t00000000\n"},
 "expect": {"verify": {"lines": [["{store}", "3", "blake3", "00000000", "18edf32bb9fb2dc66b8dbfc7884f279394b8f9c3faef8f1241824b240d9372ce"]], "exit": 4}}},
{"id": "15.3-unclaimed-crc32-is-inert", "spec": ["15.3"], "level": "MUST", "requires": ["!crc32"],
 "note": "A reader that does not implement crc32 treats its markers as inert.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\n#_checksum_crc32_#\t00000000\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.3-unclaimed-crc32c-is-inert", "spec": ["15.3"], "level": "MUST", "requires": ["!crc32c"],
 "note": "A reader that does not implement crc32c treats its markers as inert.",
 "files": {"s.tsvz": "#_checksum_crc32c_#\na\tx\n#_checksum_crc32c_#\t00000000\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.3-unclaimed-sha256-is-inert", "spec": ["15.3"], "level": "MUST", "requires": ["!sha256"],
 "note": "A reader that does not implement sha256 treats its markers as inert.",
 "files": {"s.tsvz": "#_checksum_sha256_#\na\tx\n#_checksum_sha256_#\t00000000\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.3-unclaimed-blake3-is-inert", "spec": ["15.3"], "level": "MUST", "requires": ["!blake3"],
 "note": "A reader that does not implement blake3 treats its markers as inert.",
 "files": {"s.tsvz": "#_checksum_blake3_#\na\tx\n#_checksum_blake3_#\t00000000\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}}
]}
```


- [ ] **Step 11: Decision 11 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `235 passed, 5 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `16.5-unsupported-zstd-part-fails`. Nothing else fails.

- [ ] **Step 12: Decision 11 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/15-integrity.json tsvzconform_test.py && git commit -m "Define the digest algorithm registry (spec §15.2.1), with the integrity cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 13: Decision 3 — spec §20.2: what `verify` prints, and its line number**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
| `verify` | `STORE` | Replay the store with integrity checking (§15) and print one line per segment whose digest does not match: part path, line number, algorithm, expected digest, computed digest. |
```

with:

```markdown
| `verify` | `STORE` | Replay the store with integrity checking (§15) and print one line per segment whose digest does not match: the part path; the line number, which is the 1-based line, within that part, of the `#_checksum_<algo>_#` marker that closes the segment; the algorithm name in lowercase; the expected digest; and the computed digest. Both digests are printed in lowercase hexadecimal. |
```

- [ ] **Step 14: Decision 3 — its cases**

`TSVZ/conformance/cases/reader/15-verify-lines.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "15.3-mismatch-is-reported", "spec": ["15.3", "20.2", "20.6"], "level": "MUST", "requires": ["crc32"],
 "note": "verify prints the part, the 1-based line of the failing marker, the algorithm and both digests, and exits 4.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\nb\ty\n#_checksum_crc32_#\t00000000\n"},
 "expect": {"verify": {"lines": [["{store}", "4", "crc32", "00000000", "c1ec8234"]], "exit": 4}}},
{"id": "15.3-every-mismatch-is-a-line", "spec": ["15.3", "20.2"], "level": "MUST", "requires": ["crc32"],
 "note": "Each failing segment gets its own line, in order.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\n#_checksum_crc32_#\t00000000\nk\tz\n#_checksum_crc32_#\t11111111\n"},
 "expect": {"verify": {"lines": [["{store}", "3", "crc32", "00000000", "aa3c6e15"], ["{store}", "5", "crc32", "11111111", "f7b7ecf3"]], "exit": 4}}},
{"id": "15.2-checksum-key-ignores-case", "spec": ["15.2", "12.4.1"], "level": "MUST", "requires": ["crc32"],
 "note": "The algorithm name is matched ignoring case and printed in lowercase.",
 "files": {"s.tsvz": "#_CHECKSUM_CRC32_#\na\tx\n#_Checksum_Crc32_#\t00000000\n"},
 "expect": {"verify": {"lines": [["{store}", "3", "crc32", "00000000", "aa3c6e15"]], "exit": 4}}},
{"id": "20.2-verify-line-is-counted-in-its-part", "spec": ["20.2", "15.4"], "level": "MUST", "requires": ["crc32"],
 "note": "The line number is counted within the part the failing marker is in.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\n", "s.tsvz.1": "b\ty\n#_checksum_crc32_#\t00000000\n"},
 "expect": {"verify": {"lines": [["{store}.1", "2", "crc32", "00000000", "c1ec8234"]], "exit": 4}}},
{"id": "4.7-bom-is-not-in-a-segment", "spec": ["4.7", "15.4"], "level": "MUST", "requires": ["crc32"],
 "note": "A part's byte-order mark is not digested and does not shift line numbers.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\n", "s.tsvz.1": "\ufeffb\ty\n#_checksum_crc32_#\t00000000\n"},
 "expect": {"verify": {"lines": [["{store}.1", "2", "crc32", "00000000", "c1ec8234"]], "exit": 4}}},
{"id": "4.8-digest-covers-the-raw-bytes", "spec": ["4.8", "15.4"], "level": "MUST", "requires": ["crc32"],
 "note": "Digests are computed over the bytes on disk, not the text after U+FFFD replacement.",
 "files": {"s.tsvz": {"b64": "I19jaGVja3N1bV9jcmMzMl8jCmEJ/wojX2NoZWNrc3VtX2NyYzMyXyMJMDAwMDAwMDAK"}},
 "expect": {"verify": {"lines": [["{store}", "3", "crc32", "00000000", "defe6099"]], "exit": 4}}}
]}
```


- [ ] **Step 15: Decision 3 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `241 passed, 5 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `16.5-unsupported-zstd-part-fails`. Nothing else fails.

- [ ] **Step 16: Decision 3 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/15-verify-lines.json && git commit -m "Define verify's line number and digests (spec §20.2), with the mismatch cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 17: Decision 6 — spec §15.2: digests are written lowercase and compared ignoring case**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
inner character class of §12.2.1. The value is the lowercase hexadecimal expected
digest, or empty.
```

with:

```markdown
inner character class of §12.2.1. The value is the expected digest in hexadecimal,
or empty. A writer **MUST** write it in lowercase; a reader compares it ignoring
ASCII case.
```

- [ ] **Step 18: Decision 6 — its cases**

`TSVZ/conformance/cases/reader/15-digest-case.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "15.2-uppercase-digest-matches", "spec": ["15.2"], "level": "MUST", "requires": ["crc32"],
 "note": "A reader compares the digest ignoring ASCII case.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\nb\ty\n#_checksum_crc32_#\tC1EC8234\n"},
 "expect": {"verify": {"lines": [], "exit": 0}}},
{"id": "15.2-digests-are-printed-in-lowercase", "spec": ["15.2", "20.2"], "level": "MUST", "requires": ["crc32"],
 "note": "verify prints the expected and computed digests in lowercase, whatever the marker holds.",
 "files": {"s.tsvz": "#_checksum_crc32_#\na\tx\nb\ty\n#_checksum_crc32_#\tABCDEF01\n"},
 "expect": {"verify": {"lines": [["{store}", "4", "crc32", "abcdef01", "c1ec8234"]], "exit": 4}}}
]}
```


- [ ] **Step 19: Decision 6 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `243 passed, 5 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `16.5-unsupported-zstd-part-fails`. Nothing else fails.

- [ ] **Step 20: Decision 6 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/15-digest-case.json && git commit -m "Compare checksum digests ignoring ASCII case (spec §15.2), with its cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 12: Writer cases, with decisions 9, 10, 12 and 13

**Files:**
- Create: `TSVZ/conformance/cases/writer/13-encoding.json`
- Create: `TSVZ/conformance/cases/writer/19-clear.json`
- Create: `TSVZ/conformance/cases/writer/19-snapshot.json`
- Create: `TSVZ/conformance/cases/reader/13-carriage-return.json`
- Create: `TSVZ/conformance/cases/writer/13-carriage-return.json`
- Create: `TSVZ/conformance/cases/writer/19-fidelity.json`
- Create: `TSVZ/conformance/cases/reader/16-multi-member.json`
- Create: `TSVZ/conformance/cases/writer/17-active-part.json`
- Modify: `TSVZ/tsvz-spec-v1.md` (decisions 9, 10, 12, 13)

**Interfaces:**
- Consumes: Task 9.
- Produces: Spec §13.1–§13.5 (`<CR>`), §16.4, §16.6, §17.7, §19.2.3, §19.4, §19.6.

- [ ] **Step 1: Write the case files**

Write these files exactly. Each expectation follows from the clause its `note` gives.

`TSVZ/conformance/cases/writer/13-encoding.json`:

```json
{"format": 1, "class": "writer", "cases": [
{"id": "13.3-set-encodes-delimiter-newline-and-lt", "spec": ["13.3", "20.1.4"], "level": "MUST",
 "note": "A tab, a newline and a < in a value are written as tokens, so they read back unchanged.",
 "steps": [{"op": ["set", "k", "a\tb", "c\nd", "e<f"]},
  {"op": ["get", "k"], "stdout": {"rows": [["k", "a\tb", "c\nd", "e<f"]]}}],
 "after": {"rows": [["k", "a\tb", "c\nd", "e<f"]], "bytes": ["encoded"]}},
{"id": "13.3-literal-token-text-round-trips", "spec": ["13.3", "13.5"], "level": "MUST",
 "note": "Values that look like tokens are written with <lt> and read back as typed.",
 "steps": [{"op": ["set", "k", "<sep>", "<LF>", "<lt>", "<#>"]}],
 "after": {"rows": [["k", "<sep>", "<LF>", "<lt>", "<#>"]], "bytes": ["encoded"]}},
{"id": "13.3-key-starting-with-hash", "spec": ["13.3", "8.3"], "level": "MUST",
 "note": "A data key starting with # is written <#>…, so it is not read as a comment.",
 "steps": [{"op": ["set", "#k", "v"]}, {"op": ["keys"], "stdout": {"lines": [["#k"]]}}],
 "after": {"rows": [["#k", "v"]], "bytes": ["encoded"]}},
{"id": "13.3-hash-inside-keys-and-values", "spec": ["13.3"], "level": "MUST",
 "note": "A # anywhere but the start of a key needs no token and reads back.",
 "steps": [{"op": ["set", "k#", "#v", "a#b"]}],
 "after": {"rows": [["k#", "#v", "a#b"]], "bytes": ["encoded"]}},
{"id": "13.3-csvz-encodes-commas", "spec": ["13.3", "2.1"], "level": "MUST",
 "note": "In .csvz a comma in a value is written <sep>; a tab needs no token.",
 "store": "s.csvz",
 "steps": [{"op": ["set", "k", "a,b", "c\td"]}],
 "after": {"rows": [["k", "a,b", "c\td"]], "bytes": ["encoded"]}},
{"id": "13.3-psvz-encodes-pipes", "spec": ["13.3", "2.1"], "level": "MUST",
 "note": "In .psvz a pipe in a value is written <sep>.",
 "store": "s.psvz",
 "steps": [{"op": ["set", "k", "a|b", "c,d"]}],
 "after": {"rows": [["k", "a|b", "c,d"]], "bytes": ["encoded"]}},
{"id": "13.3-nsvz-encodes-nul", "spec": ["13.3", "2.1", "20.3.1"], "level": "MUST",
 "note": "In .nsvz a NUL in a value is written <sep>; it arrives through set - because arguments cannot hold NUL.",
 "store": "s.nsvz",
 "steps": [{"op": ["set", "-"], "stdin": "k\u0000a<sep>b\u0000c\td\n", "body": ["k\ta\u0000b\tc<sep>d"]}],
 "after": {"rows": [["k", "a\u0000b", "c\td"]], "bytes": ["encoded"]}},
{"id": "13.3-non-ascii-round-trips", "spec": ["13.3", "4.1"], "level": "MUST",
 "note": "Non-ASCII text is written as UTF-8 and reads back unchanged.",
 "steps": [{"op": ["set", "é", "ß", "漢字", "😀"]}],
 "after": {"rows": [["é", "ß", "漢字", "😀"]], "bytes": ["encoded"]}},
{"id": "13.3-marker-like-value-is-data", "spec": ["13.3", "12.2.1"], "level": "MUST",
 "note": "A value spelled like a marker is only data; it sets no state.",
 "steps": [{"op": ["set", "k", "#_defaults_#", "d"]}],
 "after": {"rows": [["k", "#_defaults_#", "d"]], "missing": []}},
{"id": "10.1-trailing-space-is-not-kept-while-stripping", "spec": ["10.1", "10.2"], "level": "MUST",
 "note": "No token stands for a space, so a trailing space written while stripping is on reads back stripped.",
 "steps": [{"op": ["set", "k", "v "]}],
 "after": {"rows": [["k", "v"]]}},
{"id": "10.3-trailing-space-is-kept-without-stripping", "spec": ["10.3", "10.2"], "level": "MUST",
 "note": "With stripping off, a trailing space written by set reads back.",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tfalse\n"},
 "steps": [{"op": ["set", "k", "v "]}],
 "after": {"rows": [["k", "v "]]}},
{"id": "3.1-set-only-appends", "spec": ["3.1", "18.2"], "level": "MUST",
 "note": "A write appends a record; no earlier byte changes.",
 "files": {"s.tsvz": "#hdr\na\tx\n"},
 "steps": [{"op": ["set", "b", "y"]}, {"op": ["delete", "a"]}],
 "after": {"rows": [["b", "y"]], "bytes": ["appended", "encoded"]}},
{"id": "4.4-written-records-end-with-a-newline", "spec": ["4.4", "18.2"], "level": "SHOULD",
 "note": "A writer terminates every record, the last included, with \\n.",
 "steps": [{"op": ["set", "k", "v"]}],
 "after": {"bytes": ["lf-terminated"]}},
{"id": "4.1-new-store-has-no-bom", "spec": ["4.1"], "level": "MUST",
 "note": "A store created by a write does not start with a byte-order mark.",
 "steps": [{"op": ["set", "k", "v"]}],
 "after": {"rows": [["k", "v"]], "bytes": ["encoded"]}}
]}
```

`TSVZ/conformance/cases/writer/19-clear.json`:

```json
{"format": 1, "class": "writer", "cases": [
{"id": "19.8-clear-keeps-the-header-and-markers", "spec": ["19.8", "20.2"], "level": "MUST",
 "note": "Clearing one part leaves no rows, the header comment first, and the same marker state, in the same file.",
 "files": {"s.tsvz": "#hdr\n#_defaults_#\td\n#_strip_trailing_whites_#\tfalse\na\tx\n"},
 "steps": [{"op": ["clear"], "stdout": "empty"}],
 "after": {"rows": [], "fidelity": "markers", "bytes": ["header-kept", "same-file", "encoded"]}},
{"id": "19.8-clear-without-a-header", "spec": ["19.8", "20.2"], "level": "MUST",
 "note": "Clearing a part without a header keeps its markers.",
 "files": {"s.tsvz": "#_defaults_#\td\n#_return_defaults_when_missing_#\tfalse\na\tx\n"},
 "steps": [{"op": ["clear"]}],
 "after": {"rows": [], "fidelity": "markers", "bytes": ["same-file"]}}
]}
```


- [ ] **Step 2: Run the tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `243 passed, 5 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `16.5-unsupported-zstd-part-fails`. Nothing else fails.

- [ ] **Step 3: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add conformance/cases/writer/13-encoding.json conformance/cases/writer/19-clear.json && git commit -m "Add the writer cases for encoding and clear.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Decision 9 — spec §19.2.3c, §19.6: a snapshot may keep the header comment**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
      c. no tombstones, no comments, no superseded values.
```

with:

```markdown
      c. no tombstones and no superseded values, and no comments, except that the
         snapshot MAY begin with the store's header comment — the first committed
         line of its first part, when that line is a comment — placed before the
         markers of (a).
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
19.6 **Lossiness.** A snapshot is lossy with respect to history: comments,
intermediate values, and tombstone records are discarded.
```

with:

```markdown
19.6 **Lossiness.** A snapshot is lossy with respect to history: comments (other
than a header comment kept by §19.2), intermediate values, and tombstone records
are discarded.
```

- [ ] **Step 5: Decision 9 — its cases**

`TSVZ/conformance/cases/writer/19-snapshot.json`:

```json
{"format": 1, "class": "writer", "cases": [
{"id": "19.2-scrub-layout-and-fidelity", "spec": ["19.2", "19.4", "19.8", "20.2"], "level": "MUST",
 "note": "scrub rewrites the part as markers then one row per live key, drops tombstones and other comments, and reads the same.",
 "files": {"s.tsvz": "#key\tval\n#_version_#\t1\n#_defaults_#\td1\td2\na\tx\nb\ty\n#note\na\tz\nb\nc\t\t\n"},
 "steps": [{"op": ["scrub"], "stdout": "empty"}],
 "after": {"rows": [["a", "z", "d2"], ["c", "", ""]], "fidelity": "full", "bytes": ["snapshot-layout", "same-file", "encoded"]}},
{"id": "19.2-scrub-without-a-header", "spec": ["19.2"], "level": "MUST",
 "note": "Without a header comment the snapshot starts with #_version_#; a later comment is dropped.",
 "files": {"s.tsvz": "a\tx\n#c\nb\ty\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "19.4-scrub-keeps-not-found", "spec": ["19.4", "14.5"], "level": "MUST",
 "note": "#_return_defaults_when_missing_# false survives a scrub.",
 "files": {"s.tsvz": "#_defaults_#\td\n#_return_defaults_when_missing_#\tfalse\na\tx\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"missing": null, "fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "19.2-scrub-keeps-first-appearance-order", "spec": ["19.2", "3.4"], "level": "MUST",
 "note": "Rows are written in first-appearance order, a rewritten-after-delete key last.",
 "files": {"s.tsvz": "b\t1\na\t2\nb\t3\nc\t4\nb\nb\t5\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["a", "2"], ["c", "4"], ["b", "5"]], "fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "19.2-scrub-keeps-escaped-content", "spec": ["19.2", "13.3"], "level": "MUST",
 "note": "Keys and values needing tokens are written encoded.",
 "files": {"s.tsvz": "<#>k\tx<sep>y\tl<LF>f\na\t<lt>sep>\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["#k", "x\ty", "l\nf"], ["a", "<sep>"]], "fidelity": "full", "bytes": ["snapshot-layout", "same-file", "encoded"]}},
{"id": "19.2-scrub-of-a-store-with-no-live-key", "spec": ["19.2"], "level": "MUST",
 "note": "A store whose keys are all deleted scrubs to its markers alone.",
 "files": {"s.tsvz": "#_defaults_#\td\na\tx\na\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [], "fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "19.2-scrub-csvz", "spec": ["19.2", "2.1"], "level": "MUST",
 "note": "A .csvz store scrubs in its own variant.",
 "store": "s.csvz", "files": {"s.csvz": "#_defaults_#,d1,d2\na,x\na,y<sep>z\nb,w\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["a", "y,z", "d2"], ["b", "w", "d2"]], "fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "20.2-scrub-of-a-multipart-store-is-refused", "spec": ["20.2", "20.6"], "level": "MUST",
 "requires": ["!scrub-multipart"],
 "note": "A tool that does not scrub multi-part stores exits 1 and writes nothing.",
 "files": {"s.tsvz": "a\t1\n", "s.tsvz.1": "b\t2\na\n"},
 "steps": [{"op": ["scrub"], "exit": 1}],
 "after": {"bytes": ["unchanged"]}},
{"id": "20.2-scrub-of-a-multipart-store", "spec": ["20.2", "19.4"], "level": "MUST",
 "requires": ["scrub-multipart"],
 "note": "A tool that scrubs multi-part stores keeps what a reader sees.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\na\t1\n", "s.tsvz.1": "b\t2\na\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["b", "2", "d2"]], "fidelity": "full"}}
]}
```


- [ ] **Step 6: Decision 9 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `243 passed, 5 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `16.5-unsupported-zstd-part-fails`. Nothing else fails.

- [ ] **Step 7: Decision 9 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/writer/19-snapshot.json && git commit -m "Let a snapshot keep the store's header comment (spec §19.2.3c, §19.6), with the scrub cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 8: Decision 10 — spec §13: the `<CR>` token; writers encode every carriage return**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
13.1 The variant delimiter, the line feed `\n`, and the escape introducer `<`
cannot appear literally inside a field; they are represented by reversible
**control tokens** of the form `<...>`. The number sign `#` is represented by a
token only where required by §8.3. Reading inverts writing.
```

with:

```markdown
13.1 The variant delimiter, the line feed `\n`, the carriage return `\r`, and the
escape introducer `<` cannot appear literally inside a field; they are represented
by reversible **control tokens** of the form `<...>`. The number sign `#` is
represented by a token only where required by §8.3. Reading inverts writing.
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
| line feed `U+000A`      | `<LF>`  | line feed |
```

with:

```markdown
| line feed `U+000A`      | `<LF>`  | line feed |
| carriage return `U+000D` | `<CR>` | carriage return |
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
`<lt>`, every delimiter character as `<sep>`, and every line feed as `<LF>`. A
```

with:

```markdown
`<lt>`, every delimiter character as `<sep>`, every line feed as `<LF>`, and every
carriage return as `<CR>`. A
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
   - `a<b`: written `a<lt>b` ⟶ reads as `a<b`
```

with:

```markdown
   - `a<b`: written `a<lt>b` ⟶ reads as `a<b`
   - a value ending in a carriage return: written `a<CR>` ⟶ reads as `a`‹CR› (a raw
     `\r` there would be removed as part of a CRLF line ending, §4.2)
```

- [ ] **Step 9: Decision 10 — its cases**

`TSVZ/conformance/cases/reader/13-carriage-return.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "13.2-cr-token-decodes", "spec": ["13.2", "13.4"], "level": "MUST",
 "note": "<CR> reads back as a carriage return.",
 "files": {"s.tsvz": "a\tc<CR>r\tx\n"},
 "expect": {"read": [["a", "c\rr", "x"]]}},
{"id": "13.5-cr-token-at-the-end-of-a-row", "spec": ["13.5", "13.2"], "level": "MUST",
 "note": "<CR> at the end of the last cell reads back as a carriage return.",
 "files": {"s.tsvz": "a\tx<CR>\n"},
 "expect": {"read": [["a", "x\r"]]}}
]}
```

`TSVZ/conformance/cases/writer/13-carriage-return.json`:

```json
{"format": 1, "class": "writer", "cases": [
{"id": "13.3-set-encodes-carriage-returns", "spec": ["13.3", "13.5"], "level": "MUST",
 "note": "Every carriage return is written <CR>, so one inside a value and one ending the last value both read back.",
 "steps": [{"op": ["set", "k", "e\rf", "x\r"]},
  {"op": ["get", "k"], "stdout": {"rows": [["k", "e\rf", "x\r"]]}}],
 "after": {"rows": [["k", "e\rf", "x\r"]], "bytes": ["encoded"]}},
{"id": "19.2-scrub-keeps-a-carriage-return", "spec": ["19.2", "13.3"], "level": "MUST",
 "note": "A scrub writes a carriage return as <CR>, so one ending the last value survives.",
 "files": {"s.tsvz": "a\tc<CR>\nb\tx<CR>y\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["a", "c\r"], ["b", "x\ry"]], "fidelity": "full", "bytes": ["snapshot-layout", "same-file", "encoded"]}}
]}
```


- [ ] **Step 10: Decision 10 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `245 passed, 5 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `13.2-cr-token-decodes`, `13.3-set-encodes-carriage-returns`, `13.5-cr-token-at-the-end-of-a-row`, `16.5-unsupported-zstd-part-fails`, `19.2-scrub-keeps-a-carriage-return`. Nothing else fails.

- [ ] **Step 11: Decision 10 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/13-carriage-return.json conformance/cases/writer/13-carriage-return.json && git commit -m "Add the <CR> token and make writers encode every carriage return (spec §13), with its cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 12: Decision 12 — spec §19.2.3d, §19.4, §19.6: a snapshot keeps the final marker state**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
         markers of (a).
```

with:

```markdown
         markers of (a);
      d. when the rows of (b) are written under marker state other than the final
         state (§19.4), the final markers again, after the rows, so that the
         store's final marker state is preserved.
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
19.4 **Fidelity (normative).** A snapshot **MUST** reproduce the exact observable
read result of the store it replaces.
```

with:

```markdown
19.4 **Fidelity (normative).** A snapshot **MUST** reproduce the exact observable
read result of the store it replaces, and **MUST** leave the same final marker
state, so that records appended after it are read as they would have been without
it.
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
strategy is to emit `#_strip_trailing_whites_#` `false` and
`#_fill_empty_with_default_#` `false` and write fully resolved value cells, leaving
`#_defaults_#` only as a fallback for absent trailing columns.
```

with:

```markdown
strategy is to emit `#_strip_trailing_whites_#` `false` and
`#_fill_empty_with_default_#` `false`, write fully resolved value cells, leaving
`#_defaults_#` only as a fallback for absent trailing columns, and then emit the
final markers after the rows (§19.2.3d).
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
are discarded. The live data state and
the active marker state are preserved exactly (subject to §19.4).
```

with:

```markdown
are discarded. The live data state and the final marker state are preserved
exactly (§19.4).
```

- [ ] **Step 13: Decision 12 — its cases**

`TSVZ/conformance/cases/writer/19-fidelity.json`:

```json
{"format": 1, "class": "writer", "cases": [
{"id": "19.4-scrub-with-changed-defaults", "spec": ["19.4", "14.4"], "level": "MUST",
 "note": "Rows bound to superseded defaults keep their values after a scrub.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\na\tx\n#_defaults_#\te1\te2\nb\ty\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["a", "x", "d2"], ["b", "y", "e2"]], "fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "19.4-scrub-with-defaults-widened", "spec": ["19.4", "14.4"], "level": "MUST",
 "note": "A row bound to narrower defaults must not pick up the wider final defaults.",
 "files": {"s.tsvz": "#_defaults_#\td1\na\tx\n#_defaults_#\te1\te2\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["a", "x"]], "missing": ["e1", "e2"], "fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "19.4-scrub-with-stripping-toggled", "spec": ["19.4", "10.3"], "level": "MUST",
 "note": "Rows read with and without stripping keep their values, and stripping stays on for later appends.",
 "files": {"s.tsvz": "#_strip_trailing_whites_#\tfalse\na\tx \n#_strip_trailing_whites_#\ttrue\nb\ty \n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["a", "x "], ["b", "y"]], "fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "19.4-scrub-with-fill-empty-toggled", "spec": ["19.4", "14.4"], "level": "MUST",
 "note": "A filled cell and an empty cell both survive, and fill-empty stays off for later appends.",
 "files": {"s.tsvz": "#_fill_empty_with_default_#\ttrue\n#_defaults_#\td\na\t\n#_fill_empty_with_default_#\tfalse\nb\t\n"},
 "steps": [{"op": ["scrub"]}],
 "after": {"rows": [["a", "d"], ["b", ""]], "fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}},
{"id": "19.4-scrub-twice", "spec": ["19.4"], "level": "MUST",
 "note": "Scrubbing a snapshot again changes nothing a reader can see.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\na\tx\n#_strip_trailing_whites_#\tfalse\nb\ty \n"},
 "steps": [{"op": ["scrub"]}, {"op": ["scrub"]}],
 "after": {"fidelity": "full", "bytes": ["snapshot-layout", "same-file"]}}
]}
```


- [ ] **Step 14: Decision 12 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `245 passed, 5 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `13.2-cr-token-decodes`, `13.3-set-encodes-carriage-returns`, `13.5-cr-token-at-the-end-of-a-row`, `16.5-unsupported-zstd-part-fails`, `19.2-scrub-keeps-a-carriage-return`. Nothing else fails.

- [ ] **Step 15: Decision 12 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/writer/19-fidelity.json && git commit -m "Make a snapshot keep the final marker state (spec §19.2.3d, §19.4, §19.6), with the fidelity cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 16: Decision 13 — spec §16.4, §16.6, §17.7: commands append to the last part; compressed parts take new members**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
prior immutable parts to a later pass (§17.7). Where a compressed part is not held
open as a stream, appending requires rewriting it as a whole.
```

with:

```markdown
prior immutable parts to a later pass (§17.7). A writer that does not hold a
compressed part open appends to it by writing a new compressed member holding the
new records after the part's existing bytes: a gzip member, a bzip2 stream, an xz
stream or a Zstandard frame. It does not rewrite the part (§3.1).
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
its bytes as uncompressed text. A command-line tool reports this as a failure
(§20.6, status 1); a handler answers as §21.8.5 says.
```

with:

```markdown
its bytes as uncompressed text. A command-line tool reports this as a failure
(§20.6, status 1); a handler answers as §21.8.5 says.

16.6 **Multi-member parts.** Each codec of §16.2 lets a file hold several
compressed members one after another. A reader **MUST** read all the members of a
part, in order, as one stream. When a part's compressed data ends early, as a
final member cut short by a crash does, the reader keeps what it decoded before
that point; §4.3 then discards any bytes after the last `\n`. This is not an
error, though a reader MAY warn about it.
```

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
   - On startup, a writer **SHOULD** open a fresh part named with a new ordinal and
     leave existing parts untouched (treating them as immutable).
```

with:

```markdown
   - On startup, a long-running writer, such as a handler (§21), **SHOULD** open a
     fresh part named with a new ordinal and leave existing parts untouched
     (treating them as immutable).
   - A command-line tool (§20) writes once per invocation, so it appends to the
     store's last part in replay order (§17.3), which is the unnumbered file when
     that is the only part. That part is the store's **active part** for `parts`
     and `clear` (§20.2).
```

- [ ] **Step 17: Decision 13 — its cases**

`TSVZ/conformance/cases/reader/16-multi-member.json`:

```json
{"format": 1, "class": "reader", "cases": [
{"id": "16.6-gzip-members-read-as-one-stream", "spec": ["16.6", "16.4"], "level": "MUST",
 "note": "A gzip part holding two members reads as their concatenation.",
 "store": "s.tsvz.gz", "files": {"s.tsvz.gz": {"b64": "H4sIAAAAAAAA/0vkrOACABVuPKoEAAAAH4sIAAAAAAAA/0virOQCALrwkqEEAAAA"}},
 "expect": {"read": [["a", "x"], ["b", "y"]], "len": 2}},
{"id": "16.6-bzip2-members-read-as-one-stream", "spec": ["16.6", "16.4"], "level": "MUST", "requires": ["bzip2"],
 "note": "A bzip2 part holding two members reads as their concatenation.",
 "store": "s.tsvz.bz2", "files": {"s.tsvz.bz2": {"b64": "QlpoOTFBWSZTWQQH8tUAAAFBgAAwIAAAQCAAIZpoM00yvF3JFOFCQBAfy1RCWmg5MUFZJlNZtmmBZwAAAUGAADAQAAAgIAAhmmgzTTK8XckU4UJC2aYFnA=="}},
 "expect": {"read": [["a", "x"], ["b", "y"]], "len": 2}},
{"id": "16.6-xz-members-read-as-one-stream", "spec": ["16.6", "16.4"], "level": "MUST", "requires": ["xz"],
 "note": "A xz part holding two members reads as their concatenation.",
 "store": "s.tsvz.xz", "files": {"s.tsvz.xz": {"b64": "/Td6WFoAAATm1rRGAgAhARYAAAB0L+WjAQADYQl4CgAqJo2HZDZldAABHARvLJzBH7bzfQEAAAAABFla/Td6WFoAAATm1rRGAgAhARYAAAB0L+WjAQADYgl5CgBDNSi9FNG/BgABHARvLJzBH7bzfQEAAAAABFla"}},
 "expect": {"read": [["a", "x"], ["b", "y"]], "len": 2}},
{"id": "16.6-zstd-members-read-as-one-stream", "spec": ["16.6", "16.4"], "level": "MUST", "requires": ["zstd"],
 "note": "A zstd part holding two members reads as their concatenation.",
 "store": "s.tsvz.zst", "files": {"s.tsvz.zst": {"b64": "KLUv/SAEIQAAYQl4Cii1L/0gBCEAAGIJeQo="}},
 "expect": {"read": [["a", "x"], ["b", "y"]], "len": 2}},
{"id": "16.6-member-cut-short-is-a-torn-tail", "spec": ["16.6", "4.5"], "level": "MUST",
 "note": "A final member cut short after its header adds nothing, and the store still reads without error.",
 "store": "s.tsvz.gz", "files": {"s.tsvz.gz": {"b64": "H4sIAAAAAAAA/0vkrOACABVuPKoEAAAAH4sIAAAAAAAA/w=="}},
 "expect": {"read": [["a", "x"]], "len": 1}}
]}
```

`TSVZ/conformance/cases/writer/17-active-part.json`:

```json
{"format": 1, "class": "writer", "cases": [
{"id": "16.4-append-to-a-gzip-store-adds-a-member", "spec": ["16.4", "3.1", "16.6"], "level": "MUST",
 "note": "A write to a compressed part appends a new compressed member; the earlier bytes stay as they were.",
 "store": "s.tsvz.gz", "files": {"s.tsvz.gz": {"b64": "H4sIAAAAAAAA/0vkrOACABVuPKoEAAAA"}},
 "steps": [{"op": ["set", "b", "y"]}, {"op": ["read"], "stdout": {"rows": [["a", "x"], ["b", "y"]]}}],
 "after": {"rows": [["a", "x"], ["b", "y"]], "bytes": ["appended", "encoded"]}},
{"id": "17.7-set-appends-to-the-last-part", "spec": ["17.7", "3.1"], "level": "MUST",
 "note": "A command appends to the last part in replay order and changes no other part.",
 "files": {"s.tsvz": "a\t1\n", "s.tsvz.f": "b\t2\n", "s.tsvz.10": "c\t3\n"},
 "steps": [{"op": ["set", "d", "4"]}, {"op": ["delete", "a"]}],
 "after": {"rows": [["b", "2"], ["c", "3"], ["d", "4"]], "bytes": ["last-part-only", "encoded"]}},
{"id": "17.7-set-appends-to-a-compressed-last-part", "spec": ["17.7", "16.4"], "level": "MUST",
 "note": "When the last part is compressed, the record goes into it as a new member.",
 "files": {"s.tsvz": "a\t1\n", "s.tsvz.1.gz": {"b64": "H4sIAAAAAAAA/0viNOICAHRmH7IEAAAA"}},
 "steps": [{"op": ["set", "c", "3"]}],
 "after": {"rows": [["a", "1"], ["b", "2"], ["c", "3"]], "bytes": ["last-part-only", "encoded"]}},
{"id": "18.8-clear-multipart-appends-tombstones", "spec": ["18.8", "20.2", "17.7"], "level": "MUST",
 "note": "Clearing a multi-part store appends tombstones to its active part, the last one, and changes no earlier byte.",
 "files": {"s.tsvz": "#_defaults_#\td\na\t1\n", "s.tsvz.1": "b\t2\nc\t3\n"},
 "steps": [{"op": ["clear"]}],
 "after": {"rows": [], "fidelity": "markers", "bytes": ["last-part-only", "encoded"]}}
]}
```


- [ ] **Step 18: Decision 13 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `249 passed, 6 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `13.2-cr-token-decodes`, `13.3-set-encodes-carriage-returns`, `13.5-cr-token-at-the-end-of-a-row`, `16.5-unsupported-zstd-part-fails`, `19.2-scrub-keeps-a-carriage-return`. Nothing else fails.

- [ ] **Step 19: Decision 13 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/reader/16-multi-member.json conformance/cases/writer/17-active-part.json && git commit -m "Make commands append to the last part and compressed parts take new members (spec §16.4, §16.6, §17.7), with their cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 13: Command-line cases, with decision 14

**Files:**
- Create: `TSVZ/conformance/cases/cli/20-invocation.json`
- Create: `TSVZ/conformance/cases/cli/20-operations.json`
- Create: `TSVZ/conformance/cases/cli/20-bulk.json`
- Create: `TSVZ/conformance/cases/cli/20-parts.json`
- Modify: `TSVZ/tsvz-spec-v1.md` (decisions 14)

**Interfaces:**
- Consumes: Task 9.
- Produces: The `parts` row of spec §20.2.

- [ ] **Step 1: Write the case files**

Write these files exactly. Each expectation follows from the clause its `note` gives.

`TSVZ/conformance/cases/cli/20-invocation.json`:

```json
{"format": 1, "class": "cli", "cases": [
{"id": "20.7.1-help", "spec": ["20.7.1"], "level": "MUST",
 "note": "-h and --help print usage to standard output and exit 0.",
 "steps": [{"argv": ["-h"], "stdout": "nonempty"}, {"argv": ["--help"], "stdout": "nonempty"}]},
{"id": "20.7.1-version", "spec": ["20.7.1"], "level": "MUST",
 "note": "-V and --version print the tool's name and version and exit 0.",
 "steps": [{"argv": ["-V"], "stdout": "nonempty"}, {"argv": ["--version"], "stdout": "nonempty"}]},
{"id": "20.7.3-unknown-operation-exits-2", "spec": ["20.7.3", "20.6"], "level": "MUST",
 "note": "An operation the spec does not define is a usage error.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["frobnicate", "{store}"], "exit": 2, "stdout": "empty"}]},
{"id": "20.7.3-unknown-option-exits-2", "spec": ["20.7.3", "20.6"], "level": "MUST",
 "note": "An option the tool does not know is a usage error, extension-namespace options included.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["--no-such-option", "read", "{store}"], "exit": 2, "stdout": "empty"},
  {"argv": ["--x-no-such-extension", "read", "{store}"], "exit": 2, "stdout": "empty"}]},
{"id": "20.7.3-unknown-format-exits-2", "spec": ["20.7.3", "20.6", "20.7.1"], "level": "MUST",
 "note": "--format takes table or records; anything else is a usage error.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["--format", "xml", "read", "{store}"], "exit": 2, "stdout": "empty"}]},
{"id": "20.6-missing-arguments-exit-2", "spec": ["20.6", "20.2"], "level": "MUST",
 "note": "An operation without the arguments it needs is a usage error.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": [], "exit": 2}, {"argv": ["read"], "exit": 2}, {"argv": ["get", "{store}"], "exit": 2},
  {"argv": ["has", "{store}"], "exit": 2}, {"argv": ["pop", "{store}"], "exit": 2}, {"argv": ["setdefault", "{store}", "k"], "exit": 2}]},
{"id": "20.1.2-options-after-operands", "spec": ["20.1.2"], "level": "MUST",
 "note": "Options may follow the operation and the store.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["read", "{store}", "--format", "records"], "stdout": {"rows": [["a", "x"]]}},
  {"argv": ["get", "{store}", "--format", "records", "a"], "stdout": {"rows": [["a", "x"]]}}]},
{"id": "20.1.2-double-dash-ends-options", "spec": ["20.1.2"], "level": "MUST",
 "note": "Every argument after -- is positional, even one that looks like an option.",
 "steps": [{"argv": ["--format", "records", "set", "{store}", "--", "k", "--format", "-q"]},
  {"op": ["get", "k"], "stdout": {"rows": [["k", "--format", "-q"]]}}]},
{"id": "20.1.2-negative-numbers-are-positional", "spec": ["20.1.2"], "level": "MUST",
 "note": "An argument that is - followed by a digit or a dot is a value, not an option.",
 "steps": [{"argv": ["set", "{store}", "k", "-5", "-.5", "-1e3"]},
  {"op": ["get", "k"], "stdout": {"rows": [["k", "-5", "-.5", "-1e3"]]}}]},
{"id": "20.1.2-lone-dash-is-a-value", "spec": ["20.1.2", "20.3.1"], "level": "MUST",
 "note": "- is positional; it means standard input only when it is the only ARG.",
 "steps": [{"argv": ["set", "{store}", "k", "v", "-"]},
  {"op": ["get", "k"], "stdout": {"rows": [["k", "v", "-"]]}}]},
{"id": "20.7.1-option-with-equals", "spec": ["20.7.1"], "level": "MUST",
 "note": "A long option's value may be given as --option=VALUE.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["--format=records", "read", "{store}"], "stdout": {"rows": [["a", "x"]]}}]},
{"id": "20.7.1-quiet-and-verbose", "spec": ["20.7.1"], "level": "MUST",
 "note": "-q, --quiet, -v and --verbose are accepted.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["-q", "--format", "records", "read", "{store}"], "stdout": {"rows": [["a", "x"]]}},
  {"argv": ["--quiet", "--format", "records", "read", "{store}"], "stdout": {"rows": [["a", "x"]]}},
  {"argv": ["-v", "--format", "records", "read", "{store}"], "stdout": {"rows": [["a", "x"]]}},
  {"argv": ["--verbose", "--format", "records", "read", "{store}"], "stdout": {"rows": [["a", "x"]]}}]},
{"id": "20.1.4-arguments-are-literal", "spec": ["20.1.4", "13.3"], "level": "MUST",
 "note": "Token text in an argument is not decoded; it is encoded when written.",
 "steps": [{"argv": ["set", "{store}", "k", "<sep>", "<LF>", "<#>"]},
  {"op": ["get", "k"], "stdout": {"rows": [["k", "<sep>", "<LF>", "<#>"]]}}],
 "after": {"rows": [["k", "<sep>", "<LF>", "<#>"]], "bytes": ["encoded"]}},
{"id": "20.1.5-file-first-reads", "spec": ["20.1.5"], "level": "MUST", "requires": ["file-first"],
 "note": "With the file-first form, a store with no operation is read.",
 "files": {"s.tsvz": "a\tx\nb\ty\n"},
 "steps": [{"argv": ["--format", "records", "{store}"], "stdout": {"rows": [["a", "x"], ["b", "y"]]}}]},
{"id": "20.1.5-file-first-with-an-operation", "spec": ["20.1.5"], "level": "MUST", "requires": ["file-first"],
 "note": "With the file-first form, the operation may follow the store.",
 "files": {"s.tsvz": "a\tx\nb\ty\n"},
 "steps": [{"argv": ["--format", "records", "{store}", "get", "b"], "stdout": {"rows": [["b", "y"]]}},
  {"argv": ["{store}", "set", "c", "z"]},
  {"op": ["get", "c"], "stdout": {"rows": [["c", "z"]]}}]},
{"id": "20.7.1-delimiter-option-does-not-change-a-strict-variant", "spec": ["20.7.1", "5.1"], "level": "MUST",
 "note": "For a strict variant the extension decides the delimiter; -d comma is overridden.",
 "files": {"s.tsvz": "a\tx,y\n"},
 "steps": [{"argv": ["-q", "-d", "comma", "--format", "records", "read", "{store}"], "stdout": {"rows": [["a", "x,y"]]}}]},
{"id": "20.7.1-conflicting-delimiter-warns", "spec": ["20.7.1"], "level": "SHOULD",
 "note": "A -d that conflicts with a strict extension produces a warning.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["-d", "comma", "--format", "records", "read", "{store}"], "stdout": {"rows": [["a", "x"]]}, "stderr": "nonempty"}]},
{"id": "20.4.3-records-when-not-a-terminal", "spec": ["20.4.3", "20.4.1"], "level": "MUST",
 "note": "Without --format, output to a file is records.",
 "files": {"s.tsvz": "a\tx\nb\t\n"},
 "steps": [{"argv": ["read", "{store}"], "stdout": {"rows": [["a", "x"], ["b", ""]]}}]},
{"id": "20.4.2-table-format-is-accepted", "spec": ["20.4.2", "20.7.1"], "level": "MUST",
 "note": "--format table prints something and exits 0; its layout is the tool's own.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["--format", "table", "read", "{store}"], "stdout": "nonempty"}]},
{"id": "20.4.1-records-use-the-store-delimiter", "spec": ["20.4.1"], "level": "MUST",
 "note": "Records print in the store's own variant: split on commas, with a comma in a value written <sep>.",
 "store": "s.csvz", "files": {"s.csvz": "a,x,y\nb,p<sep>q\n"},
 "steps": [{"argv": ["--format", "records", "read", "{store}"], "stdout": {"rows": [["a", "x", "y"], ["b", "p,q"]]}}]},
{"id": "20.4.1-records-encode-a-leading-hash", "spec": ["20.4.1", "13.3"], "level": "MUST",
 "note": "A key starting with # prints as <#>…, and every other special character as its token.",
 "files": {"s.tsvz": "<#>k\tv<lt>w\tx<LF>y\n"},
 "steps": [{"argv": ["--format", "records", "read", "{store}"], "stdout": {"text": "<#>k\tv<lt>w\tx<LF>y\n"}}]},
{"id": "20.5.1-warnings-stay-off-stdout", "spec": ["20.5.1", "20.5.2"], "level": "MUST",
 "note": "A store with an unterminated tail prints only its rows on stdout, whatever is reported.",
 "files": {"s.tsvz": "a\tx\nb"},
 "steps": [{"argv": ["-v", "--format", "records", "read", "{store}"], "stdout": {"rows": [["a", "x"]]}}]}
]}
```

`TSVZ/conformance/cases/cli/20-operations.json`:

```json
{"format": 1, "class": "cli", "cases": [
{"id": "20.2.2-reads-do-not-create-a-missing-store", "spec": ["20.2.2", "20.6"], "level": "MUST", "modes": ["files"],
 "note": "Operations that only read exit 1 for a missing store and do not create it.",
 "steps": [{"op": ["read"], "exit": 1, "stdout": "empty"}, {"op": ["get", "k"], "exit": 1, "stdout": "empty"},
  {"op": ["scrub"], "exit": 1}, {"op": ["verify"], "exit": 1, "stdout": "empty"}, {"op": ["parts"], "exit": 1, "stdout": "empty"},
  {"op": ["has", "k"], "exit": 1}, {"op": ["len"], "exit": 1, "stdout": "empty"}, {"op": ["keys"], "exit": 1, "stdout": "empty"},
  {"op": ["pop", "k"], "exit": 1, "stdout": "empty"}, {"op": ["popitem"], "exit": 1, "stdout": "empty"}],
 "after": {"absent": ["s.tsvz"]}},
{"id": "20.2.2-set-creates-the-store", "spec": ["20.2.2"], "level": "MUST",
 "note": "set creates a missing store.",
 "steps": [{"op": ["set", "k", "v"]}],
 "after": {"exists": ["s.tsvz"], "rows": [["k", "v"]]}},
{"id": "20.2.2-append-creates-the-store", "spec": ["20.2.2"], "level": "MUST",
 "note": "append creates a missing store.",
 "steps": [{"op": ["append", "k", "v"]}],
 "after": {"exists": ["s.tsvz"], "rows": [["k", "v"]]}},
{"id": "20.2.2-delete-creates-the-store", "spec": ["20.2.2"], "level": "MUST",
 "note": "delete creates a missing store and succeeds.",
 "steps": [{"op": ["delete", "k"]}],
 "after": {"exists": ["s.tsvz"], "rows": []}},
{"id": "20.2.2-clear-creates-the-store", "spec": ["20.2.2"], "level": "MUST",
 "note": "clear creates a missing store and succeeds.",
 "steps": [{"op": ["clear"]}],
 "after": {"exists": ["s.tsvz"], "rows": []}},
{"id": "20.2.2-setdefault-creates-the-store", "spec": ["20.2.2", "20.2"], "level": "MUST",
 "note": "setdefault creates a missing store, writes the record and prints it.",
 "steps": [{"op": ["setdefault", "k", "v"], "stdout": {"rows": [["k", "v"]]}}],
 "after": {"exists": ["s.tsvz"], "rows": [["k", "v"]]}},
{"id": "20.2-set-of-a-lone-key-deletes", "spec": ["20.2", "9.2"], "level": "MUST",
 "note": "set with a key and no value writes a tombstone.",
 "files": {"s.tsvz": "a\tx\nb\ty\n"},
 "steps": [{"op": ["set", "a"], "stdout": "empty"}, {"op": ["get", "a"], "exit": 3, "stdout": {"rows": [["a"]]}}],
 "after": {"rows": [["b", "y"]]}},
{"id": "20.2-set-of-an-empty-value", "spec": ["20.2", "20.4.1"], "level": "MUST",
 "note": "An empty-string value writes a present-empty cell; read prints it with its delimiter.",
 "steps": [{"op": ["set", "a", ""]}, {"op": ["read"], "stdout": {"rows": [["a", ""]]}}],
 "after": {"rows": [["a", ""]]}},
{"id": "20.2-set-of-a-marker-key", "spec": ["20.2", "12.2.1"], "level": "MUST",
 "note": "A key matching the reserved pattern writes a marker line.",
 "steps": [{"op": ["set", "#_defaults_#", "d1", "d2"]}, {"op": ["set", "a", "x"]},
  {"op": ["get", "a", "m"], "exit": 3, "stdout": {"rows": [["a", "x", "d2"], ["m", "d1", "d2"]]}}],
 "after": {"rows": [["a", "x", "d2"]], "missing": ["d1", "d2"]}},
{"id": "20.2-set-of-a-lone-marker-key-resets-it", "spec": ["20.2", "12.1"], "level": "MUST",
 "note": "A marker key with no value writes a lone marker line, which resets it.",
 "files": {"s.tsvz": "#_defaults_#\td\n"},
 "steps": [{"op": ["set", "#_defaults_#"]}, {"op": ["get", "m"], "exit": 3, "stdout": {"rows": [["m"]]}}],
 "after": {"missing": []}},
{"id": "20.2-append-is-set", "spec": ["20.2"], "level": "MUST",
 "note": "append behaves exactly like set.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"op": ["append", "b", "y", "z"], "stdout": "empty"}, {"op": ["append", "a"]}],
 "after": {"rows": [["b", "y", "z"]]}},
{"id": "20.2-delete-several-keys", "spec": ["20.2"], "level": "MUST",
 "note": "delete appends one tombstone per key and prints nothing.",
 "files": {"s.tsvz": "a\t1\nb\t2\nc\t3\n"},
 "steps": [{"op": ["delete", "a", "c"], "stdout": "empty"}],
 "after": {"rows": [["b", "2"]]}},
{"id": "20.2-delete-of-an-absent-key-succeeds", "spec": ["20.2"], "level": "MUST",
 "note": "delete must not fail because a key is absent.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"op": ["delete", "zz"]}],
 "after": {"rows": [["a", "x"]]}},
{"id": "20.2.3-verify-without-markers-succeeds", "spec": ["20.2.3"], "level": "MUST",
 "note": "A store with no checksum markers verifies, printing nothing.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"op": ["verify"], "stdout": "empty"}]},
{"id": "20.2-parts-of-a-single-file", "spec": ["20.2", "20.4.1"], "level": "MUST",
 "note": "parts lists the one file as index 0, no ordinal, its path, and active.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"op": ["parts"], "stdout": {"lines": [["0", "", "{store}", "active"]]}}]},
{"id": "20.2-has", "spec": ["20.2"], "level": "MUST",
 "note": "has prints nothing and exits 0 for a live key, 3 for a deleted or absent one.",
 "files": {"s.tsvz": "a\tx\nb\ty\nb\n"},
 "steps": [{"op": ["has", "a"], "stdout": "empty"}, {"op": ["has", "b"], "exit": 3, "stdout": "empty"},
  {"op": ["has", "zz"], "exit": 3, "stdout": "empty"}]},
{"id": "20.2-keys-are-encoded-like-a-first-field", "spec": ["20.2", "13.3"], "level": "MUST",
 "note": "keys prints each key encoded as the first field of a record.",
 "files": {"s.tsvz": "<#>k\tv\na<sep>b\tw\n"},
 "steps": [{"op": ["keys"], "stdout": {"text": "<#>k\na<sep>b\n"}}]},
{"id": "20.2-len", "spec": ["20.2"], "level": "MUST",
 "note": "len prints the number of live keys.",
 "files": {"s.tsvz": "a\t1\nb\t2\na\nc\t3\n"},
 "steps": [{"op": ["len"], "stdout": {"text": "2\n"}}]},
{"id": "20.2-pop-prints-and-deletes", "spec": ["20.2"], "level": "MUST",
 "note": "pop prints the key's resolved row and deletes it.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\na\tx\nb\ty\n"},
 "steps": [{"op": ["pop", "a"], "stdout": {"rows": [["a", "x", "d2"]]}}],
 "after": {"rows": [["b", "y", "d2"]]}},
{"id": "20.2-pop-of-a-missing-key", "spec": ["20.2"], "level": "MUST",
 "note": "pop of a missing key prints nothing, writes nothing and exits 3.",
 "files": {"s.tsvz": "a\tx\nb\ty\nb\n"},
 "steps": [{"op": ["pop", "zz"], "exit": 3, "stdout": "empty"}, {"op": ["pop", "b"], "exit": 3, "stdout": "empty"}],
 "after": {"bytes": ["unchanged"]}},
{"id": "20.2-popitem-takes-the-last-key", "spec": ["20.2"], "level": "MUST",
 "note": "popitem with no argument, or last, pops the last live key.",
 "files": {"s.tsvz": "a\t1\nb\t2\nc\t3\n"},
 "steps": [{"op": ["popitem"], "stdout": {"rows": [["c", "3"]]}}, {"op": ["popitem", "last"], "stdout": {"rows": [["b", "2"]]}}],
 "after": {"rows": [["a", "1"]]}},
{"id": "20.2-popitem-first", "spec": ["20.2"], "level": "MUST",
 "note": "popitem first pops the first live key.",
 "files": {"s.tsvz": "a\t1\nb\t2\n"},
 "steps": [{"op": ["popitem", "first"], "stdout": {"rows": [["a", "1"]]}}],
 "after": {"rows": [["b", "2"]]}},
{"id": "20.2-popitem-of-an-empty-store", "spec": ["20.2"], "level": "MUST",
 "note": "popitem of a store with no live key exits 3 and writes nothing.",
 "files": {"s.tsvz": "a\tx\na\n"},
 "steps": [{"op": ["popitem"], "exit": 3, "stdout": "empty"}],
 "after": {"bytes": ["unchanged"]}},
{"id": "20.2-setdefault-of-a-live-key", "spec": ["20.2"], "level": "MUST",
 "note": "setdefault of a live key prints its row and writes nothing.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"op": ["setdefault", "a", "y"], "stdout": {"rows": [["a", "x"]]}}],
 "after": {"bytes": ["unchanged"]}},
{"id": "20.2-setdefault-prints-the-resolved-row", "spec": ["20.2", "14.3"], "level": "MUST",
 "note": "setdefault of a missing key appends it and prints the row a reader then returns, defaults included.",
 "files": {"s.tsvz": "#_defaults_#\td1\td2\n"},
 "steps": [{"op": ["setdefault", "k", "v"], "stdout": {"rows": [["k", "v", "d2"]]}}],
 "after": {"rows": [["k", "v", "d2"]]}},
{"id": "20.2-setdefault-of-a-deleted-key", "spec": ["20.2"], "level": "MUST",
 "note": "A deleted key is missing, so setdefault writes it.",
 "files": {"s.tsvz": "a\tx\na\n"},
 "steps": [{"op": ["setdefault", "a", "y"], "stdout": {"rows": [["a", "y"]]}}],
 "after": {"rows": [["a", "y"]]}},
{"id": "20.5.1-writes-print-nothing", "spec": ["20.5.1"], "level": "MUST",
 "note": "Operations without output leave standard output empty.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"op": ["set", "k", "v"], "stdout": "empty"}, {"op": ["append", "j", "w"], "stdout": "empty"},
  {"op": ["delete", "k"], "stdout": "empty"}, {"op": ["scrub"], "stdout": "empty"}, {"op": ["clear"], "stdout": "empty"}]},
{"id": "20.2.6-serve-without-a-handler", "spec": ["20.2.6"], "level": "MUST", "requires": ["!handler"],
 "note": "A tool without a handler exits 1 with a diagnostic for serve.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["serve", "{store}"], "exit": 1, "stderr": "nonempty"}]},
{"id": "21.13.2-stop-without-a-handler", "spec": ["21.13.2", "20.2.6"], "level": "MUST",
 "note": "stop exits 1 when no handler answers.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"argv": ["stop", "{store}"], "exit": 1}]},
{"id": "20.2.4-loose-files-accept-every-operation", "spec": ["20.2.4"], "level": "MUST",
 "note": "Operations work on a .tsv file, whose data semantics are the tool's own; verify finds nothing to check.",
 "store": "s.tsv", "files": {"s.tsv": "a\tx\n"},
 "steps": [{"op": ["set", "k", "v"]}, {"op": ["append", "j", "w"]}, {"op": ["read"]}, {"op": ["len"], "stdout": "nonempty"},
  {"op": ["keys"]}, {"op": ["verify"], "stdout": "empty"}, {"op": ["parts"], "stdout": "nonempty"}, {"op": ["delete", "k"]},
  {"op": ["scrub"]}, {"op": ["clear"]}]}
]}
```

`TSVZ/conformance/cases/cli/20-bulk.json`:

```json
{"format": 1, "class": "cli", "cases": [
{"id": "20.3.1-set-reads-records-from-stdin", "spec": ["20.3.1"], "level": "MUST",
 "note": "set - takes records from standard input.",
 "steps": [{"op": ["set", "-"], "stdin": "a\tx\nb\ty\tz\n", "stdout": "empty"}],
 "after": {"rows": [["a", "x"], ["b", "y", "z"]]}},
{"id": "20.3.1-lone-key-line-is-a-tombstone", "spec": ["20.3.1", "9.2"], "level": "MUST",
 "note": "A line with no delimiter deletes its key.",
 "files": {"s.tsvz": "a\tx\nb\ty\n"},
 "steps": [{"op": ["set", "-"], "stdin": "a\nc\t1\n"}],
 "after": {"rows": [["b", "y"], ["c", "1"]]}},
{"id": "20.3.1-marker-lines-are-written", "spec": ["20.3.1", "12.2.1"], "level": "MUST",
 "note": "A line whose first field is a marker key writes that marker.",
 "steps": [{"op": ["set", "-"], "stdin": "#_defaults_#\td1\td2\na\tx\n"}],
 "after": {"rows": [["a", "x", "d2"]], "missing": ["d1", "d2"]}},
{"id": "20.3.1-comments-and-empty-lines-are-skipped", "spec": ["20.3.1"], "level": "MUST",
 "note": "Comment lines and empty lines are skipped.",
 "steps": [{"op": ["set", "-"], "stdin": "#c\tz\n\na\tx\n"}],
 "after": {"rows": [["a", "x"]]}},
{"id": "20.3.1-fields-are-decoded", "spec": ["20.3.1", "13.4"], "level": "MUST",
 "note": "Input fields are §13-decoded, and written encoded again.",
 "steps": [{"op": ["set", "-"], "stdin": "a\tx<sep>y\t<lt>z\n<#>h\tv\n"}],
 "after": {"rows": [["a", "x\ty", "<z"], ["#h", "v"]], "bytes": ["encoded"]}},
{"id": "20.3.1-unterminated-line-is-ignored", "spec": ["20.3.1", "4.3"], "level": "MUST",
 "note": "Only committed lines count; an unterminated final line is ignored.",
 "steps": [{"op": ["set", "-"], "stdin": "a\tx\nb\ty"}],
 "after": {"rows": [["a", "x"]]}},
{"id": "20.3.1-unterminated-line-warns", "spec": ["20.3.1"], "level": "SHOULD",
 "note": "The tool warns about an unterminated final line of input.",
 "steps": [{"argv": ["set", "{store}", "-"], "stdin": "a\tx\nb\ty", "stderr": "nonempty"}]},
{"id": "20.3.1-crlf-input", "spec": ["20.3.1", "4.2"], "level": "MUST",
 "note": "Input lines are framed like file records, so one \\r before \\n is removed.",
 "steps": [{"op": ["set", "-"], "stdin": "a\tx\r\n"}],
 "modes": ["files", "forwarded"],
 "after": {"rows": [["a", "x"]]}},
{"id": "20.3.1-delete-reads-keys-from-stdin", "spec": ["20.3.1"], "level": "MUST",
 "note": "delete - deletes the first field of each line.",
 "files": {"s.tsvz": "a\t1\nb\t2\nc\t3\n"},
 "steps": [{"op": ["delete", "-"], "stdin": "a\nc\tignored\n", "stdout": "empty"}],
 "after": {"rows": [["b", "2"]]}},
{"id": "20.3.1-delete-of-a-marker-line-resets-it", "spec": ["20.3.1"], "level": "MUST",
 "note": "A marker key on delete's input resets that marker.",
 "files": {"s.tsvz": "#_defaults_#\td\na\tx\n"},
 "steps": [{"op": ["delete", "-"], "stdin": "#_defaults_#\n"}],
 "after": {"rows": [["a", "x"]], "missing": []}},
{"id": "20.3.1-delete-skips-comments", "spec": ["20.3.1"], "level": "MUST",
 "note": "A comment line on delete's input deletes nothing.",
 "files": {"s.tsvz": "a\tx\n<#>a\ty\n"},
 "steps": [{"op": ["delete", "-"], "stdin": "#a\n"}],
 "after": {"rows": [["a", "x"], ["#a", "y"]]}},
{"id": "20.3.1-delete-decodes-keys", "spec": ["20.3.1", "13.4"], "level": "MUST",
 "note": "Keys on delete's input are decoded.",
 "files": {"s.tsvz": "<#>k\tv\na<sep>b\tw\nc\tz\n"},
 "steps": [{"op": ["delete", "-"], "stdin": "<#>k\na<sep>b\n"}],
 "after": {"rows": [["c", "z"]]}},
{"id": "20.3.1-input-uses-the-store-variant", "spec": ["20.3.1", "2.1"], "level": "MUST",
 "note": "Standard input is read in the target store's variant.",
 "store": "s.csvz",
 "steps": [{"op": ["set", "-"], "stdin": "a,x,y\nb,p<sep>q\n", "body": ["a\tx\ty", "b\tp,q"]}],
 "after": {"rows": [["a", "x", "y"], ["b", "p,q"]]}},
{"id": "20.3.1-append-reads-stdin", "spec": ["20.3.1", "20.2"], "level": "MUST",
 "note": "append - reads records from standard input like set -.",
 "steps": [{"op": ["append", "-"], "stdin": "a\tx\n"}],
 "after": {"rows": [["a", "x"]]}}
]}
```


- [ ] **Step 2: Run the tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `249 passed, 6 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `13.2-cr-token-decodes`, `13.3-set-encodes-carriage-returns`, `13.5-cr-token-at-the-end-of-a-row`, `16.5-unsupported-zstd-part-fails`, `19.2-scrub-keeps-a-carriage-return`. Nothing else fails.

- [ ] **Step 3: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add conformance/cases/cli/20-invocation.json conformance/cases/cli/20-operations.json conformance/cases/cli/20-bulk.json && git commit -m "Add the command-line cases (§20).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Decision 14 — spec §20.2: `parts` names codecs**

In `TSVZ/tsvz-spec-v1.md`, replace:

```markdown
path, and flags (`active` for the part appends go to, and the compression codec if any, separated by commas).
```

with:

```markdown
path, and flags: `active` for the part appends go to (§17.7), then the codec name `gzip`, `bzip2`, `xz` or `zstd` (§16.2) if the part is compressed, separated by commas.
```

- [ ] **Step 5: Decision 14 — its cases**

`TSVZ/conformance/cases/cli/20-parts.json`:

```json
{"format": 1, "class": "cli", "cases": [
{"id": "20.2-parts-in-replay-order", "spec": ["20.2", "17.3", "17.7"], "level": "MUST",
 "note": "parts lists index, ordinal, path and flags in replay order, skips .rotated parts, and flags the last part active with its codec.",
 "files": {"s.tsvz": "a\t1\n", "s.tsvz.10.gz": {"b64": "H4sIAAAAAAAA/0vmNOYCAFAwuBMEAAAA"}, "s.tsvz.f": "b\t2\n", "s.tsvz.5.rotated": "r\t9\n"},
 "steps": [{"op": ["parts"], "stdout": {"lines": [["0", "", "{store}", ""], ["1", "f", "{store}.f", ""], ["2", "10", "{store}.10.gz", "active,gzip"]]}}]},
{"id": "20.2-parts-names-the-codec", "spec": ["20.2", "16.2"], "level": "MUST",
 "note": "The codec flag is the codec name, gzip, whichever suffix spelling the file uses.",
 "store": "s.tsvz.gzip", "files": {"s.tsvz.gzip": {"b64": "H4sIAAAAAAAA/0vkNOQCAFmah4sEAAAA"}},
 "steps": [{"op": ["parts"], "stdout": {"lines": [["0", "", "{store}", "active,gzip"]]}}]},
{"id": "20.2-parts-names-the-bzip2-codec", "spec": ["20.2", "16.2"], "level": "MUST", "requires": ["bzip2"],
 "note": "A .bz2 part is flagged bzip2.",
 "files": {"s.tsvz": "a\t1\n", "s.tsvz.1.bz2": {"b64": "QlpoOTFBWSZTWQ+M8aoAAAHJAAAwEAAQACAAIZpoM00XPF3JFOFCQD4zxqg="}},
 "steps": [{"op": ["parts"], "stdout": {"lines": [["0", "", "{store}", ""], ["1", "1", "{store}.1.bz2", "active,bzip2"]]}}]}
]}
```


- [ ] **Step 6: Decision 14 — run the tests, the site tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `249 passed, 6 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the spec still renders cleanly).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `13.2-cr-token-decodes`, `13.3-set-encodes-carriage-returns`, `13.5-cr-token-at-the-end-of-a-row`, `16.5-unsupported-zstd-part-fails`, `19.2-scrub-keeps-a-carriage-return`, `20.2-parts-in-replay-order`, `20.2-parts-names-the-bzip2-codec`. Nothing else fails.

- [ ] **Step 7: Decision 14 — commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add tsvz-spec-v1.md conformance/cases/cli/20-parts.json && git commit -m "Name codecs in parts output (spec §20.2), with the parts cases.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 14: Handler cases

**Files:**
- Create: `TSVZ/conformance/cases/handler/21-protocol.json`

**Interfaces:**
- Consumes: Task 9.
- Produces: Nothing new.

- [ ] **Step 1: Write the case files**

Write these files exactly. Each expectation follows from the clause its `note` gives.

`TSVZ/conformance/cases/handler/21-protocol.json`:

```json
{"format": 1, "class": "handler", "cases": [
{"id": "21.6.1-a-status-line-ends-each-response", "spec": ["21.6.1", "21.3.2"], "level": "MUST",
 "note": "Each response is its output lines and then #N; one connection carries many requests.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"request": ["len"], "stdout": {"text": "1\n"}}, {"request": ["get", "a"], "stdout": {"text": "a\tx\n"}},
  {"request": ["get", "zz"], "exit": 3, "stdout": {"rows": [["zz"]]}}]},
{"id": "21.4.2-a-csvz-store-answers-in-tsv", "spec": ["21.4.2", "21.6.1"], "level": "MUST",
 "note": "The protocol is TAB-delimited whatever the store's variant, so a comma in a value is plain.",
 "store": "s.csvz", "files": {"s.csvz": "a,x,y\nb,p<sep>q\n"},
 "steps": [{"request": ["get", "b"], "stdout": {"rows": [["b", "p,q"]]}}, {"request": ["read"], "stdout": {"rows": [["a", "x", "y"], ["b", "p,q"]]}}]},
{"id": "21.4.1-crlf-request", "spec": ["21.4.1"], "level": "SHOULD",
 "note": "A handler removes one \\r before the \\n of a request line.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"raw": "len\r\n", "stdout": {"text": "1\n"}}]},
{"id": "21.4.3-marker-key-and-escaped-data-key", "spec": ["21.4.3"], "level": "MUST",
 "note": "#_defaults_# written raw is a marker key; written <#>_defaults_# it is a data key.",
 "steps": [{"raw": "set\t#_defaults_#\td1\td2\n"}, {"raw": "set\t<#>_defaults_#\tq\n"},
  {"raw": "get\tm\n", "exit": 3, "stdout": {"rows": [["m", "d1", "d2"]]}},
  {"raw": "get\t<#>_defaults_#\n", "stdout": {"rows": [["#_defaults_#", "q", "d2"]]}}]},
{"id": "21.6.2-output-encodes-a-leading-hash", "spec": ["21.6.2", "21.6.1"], "level": "MUST",
 "note": "Output lines never begin with an unencoded #.",
 "files": {"s.tsvz": "<#>k\tv\n"},
 "steps": [{"request": ["read"], "stdout": {"text": "<#>k\tv\n"}}, {"request": ["keys"], "stdout": {"text": "<#>k\n"}}]},
{"id": "21.7.2-bulk-set-ends-with-a-hash-line", "spec": ["21.7.2"], "level": "MUST",
 "note": "set - is followed by record lines and a line of exactly #.",
 "steps": [{"request": ["set", "-"], "body": ["a\tx", "b\ty", "a"]}, {"request": ["read"], "stdout": {"rows": [["b", "y"]]}}],
 "after": {"rows": [["b", "y"]]}},
{"id": "21.7.2-bulk-delete", "spec": ["21.7.2"], "level": "MUST",
 "note": "delete - takes one key per body line.",
 "files": {"s.tsvz": "a\t1\nb\t2\nc\t3\n"},
 "steps": [{"request": ["delete", "-"], "body": ["a", "c"]}, {"request": ["len"], "stdout": {"text": "1\n"}}],
 "after": {"rows": [["b", "2"]]}},
{"id": "21.7.3-setdefault-with-a-marker-key", "spec": ["21.7.3", "21.10.1"], "level": "MUST",
 "note": "setdefault of a marker key is a usage error, and the connection stays usable.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"raw": "setdefault\t#_defaults_#\td\n", "exit": 2}, {"request": ["len"], "stdout": {"text": "1\n"}}]},
{"id": "21.10.1-unknown-operation", "spec": ["21.10.1"], "level": "MUST",
 "note": "An unknown operation gets status 2 and the connection stays usable.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"raw": "frobnicate\n", "exit": 2}, {"request": ["len"], "stdout": {"text": "1\n"}}]},
{"id": "21.10.1-wrong-arguments", "spec": ["21.10.1"], "level": "MUST",
 "note": "An operation without its arguments gets status 2 and the connection stays usable.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"raw": "get\n", "exit": 2}, {"raw": "has\n", "exit": 2}, {"request": ["len"], "stdout": {"text": "1\n"}}]},
{"id": "21.10.1-unknown-option", "spec": ["21.10.1", "21.5.2"], "level": "MUST",
 "note": "An unknown request option gets status 2 and the connection stays usable.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"raw": "--bogus\tlen\n", "exit": 2}, {"request": ["len"], "stdout": {"text": "1\n"}}]},
{"id": "21.9.2-sync-write-is-on-disk-when-answered", "spec": ["21.9.2", "21.5.2"], "level": "MUST",
 "note": "A write sent with --sync is in the files when its answer arrives.",
 "steps": [{"request": ["set", "k", "v"], "options": ["--sync"]}, {"check": {"rows": [["k", "v"]]}}]},
{"id": "21.8.1-direct-appends-are-seen", "spec": ["21.8.1", "21.1.2"], "level": "MUST",
 "note": "A record another writer appended before a request is in its answer.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"request": ["len"], "stdout": {"text": "1\n"}}, {"append": "b\ty\n", "path": "s.tsvz"},
  {"request": ["get", "b"], "stdout": {"rows": [["b", "y"]]}}]},
{"id": "21.8.1-a-new-part-is-seen", "spec": ["21.8.1", "17.4"], "level": "MUST",
 "note": "A part another writer created before a request is in its answer.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"request": ["len"], "stdout": {"text": "1\n"}}, {"write": "c\tz\n", "path": "s.tsvz.1"},
  {"request": ["read"], "stdout": {"rows": [["a", "x"], ["c", "z"]]}}]},
{"id": "21.8.3-writes-apply-in-order", "spec": ["21.8.3"], "level": "MUST",
 "note": "Writes on one connection apply in the order received.",
 "steps": [{"request": ["set", "k", "1"]}, {"request": ["set", "k", "2"]}, {"request": ["delete", "j"]},
  {"request": ["get", "k"], "stdout": {"rows": [["k", "2"]]}}],
 "after": {"rows": [["k", "2"]]}},
{"id": "21.13.1-serve-creates-a-missing-store", "spec": ["21.13.1"], "level": "MUST",
 "note": "serve creates a missing store as set does.",
 "steps": [{"request": ["len"], "stdout": {"text": "0\n"}}],
 "after": {"exists": ["s.tsvz"]}},
{"id": "21.13.1-clean-stop", "spec": ["21.13.1", "21.2.3", "21.7.1"], "level": "MUST",
 "note": "After stop the handler exits 0 and removes its pointer file.",
 "files": {"s.tsvz": "a\tx\n"},
 "steps": [{"request": ["len"], "stdout": {"text": "1\n"}}],
 "after": {"serve_exit": 0, "absent": ["s.tsvz.serve"]}},
{"id": "21.9.3-stop-makes-writes-durable", "spec": ["21.9.3", "21.9.1"], "level": "MUST",
 "note": "Every acknowledged write is on disk once the handler has stopped.",
 "steps": [{"request": ["set", "k", "v"]}, {"request": ["set", "-"], "body": ["j\tw"]}],
 "after": {"rows": [["k", "v"], ["j", "w"]]}}
]}
```


- [ ] **Step 2: Run the tests and the reference**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `249 passed, 6 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --impl 'python3 TSVZ.py' --feature bzip2,xz,crc32,sha256,file-first,handler -j 16 | grep -E '^(FAIL|warn|error) |^conformant'`
Expected: exit status 1, and `FAIL` lines for exactly these cases, each in modes files, socket and forwarded (the reference's deviations, pinned in Task 15): `12.4.5-marker-value-is-not-stripped`, `13.2-cr-token-decodes`, `13.3-set-encodes-carriage-returns`, `13.5-cr-token-at-the-end-of-a-row`, `16.5-unsupported-zstd-part-fails`, `19.2-scrub-keeps-a-carriage-return`, `20.2-parts-in-replay-order`, `20.2-parts-names-the-bzip2-codec`. Nothing else fails.

- [ ] **Step 3: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add conformance/cases/handler/21-protocol.json && git commit -m "Add the handler cases (§21).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 15: The reference baseline

**Files:**
- Create: `TSVZ/conformance/reference-baseline.json`
- Modify: `TSVZ/tsvzconform_test.py` (append)

**Interfaces:**
- Consumes: Tasks 1–14 (the runner and the whole corpus).
- Produces: `reference-baseline.json` — `{"impl", "features", "note", "not_passing": [{"id", "modes", "result", "reason", "python_below"?}]}` — and the end-to-end test that keeps it true.

- [ ] **Step 1: Write the failing tests**

Append this section to `TSVZ/tsvzconform_test.py`:

```python
# --- the reference ----------------------------------------------------------------------------

def test_a_long_work_directory_path(tmp_path, capsys):
	work = tmp_path / ('long' * 30)
	code, results = run_main(work, REF_COMMAND, '--only', '21.2.4-refuse-wrong-first-line,21.13.1-clean-stop', '--feature', 'handler')
	assert code == 0 and len(results) == 2 and all(r['result'] == 'pass' for r in results.values())


def test_the_reference_matches_its_baseline(tmp_path, capsys):
	"""Every case the reference does not pass is listed in conformance/reference-baseline.json, and nothing else (design section 8.3)."""
	with open(BASELINE, encoding='utf-8') as f:
		baseline = json.load(f)
	features = list(baseline['features'])
	if sys.version_info >= (3, 14):
		features.append('zstd')
	code, results = run_main(tmp_path, REF_COMMAND, '--feature', ','.join(features), '-j', str(os.cpu_count() or 4))
	found = {key: r['result'] for key, r in results.items() if r['result'] not in ('pass', 'skip')}
	expected = {}
	for entry in baseline['not_passing']:
		below = entry.get('python_below')
		if below and sys.version_info >= tuple(below):
			continue
		for mode in entry['modes']:
			expected[(entry['id'], mode)] = entry['result']
	assert found == expected
	assert code == (1 if expected else 0)
```

- [ ] **Step 2: Run them to see the baseline test fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py -k 'baseline or long_work'`
Expected: `1 failed, 1 passed` — `test_the_reference_matches_its_baseline` fails with FileNotFoundError for `conformance/reference-baseline.json`; `test_a_long_work_directory_path` passes (the decoy socket of Task 7 already lives in a short directory).

- [ ] **Step 3: Write the baseline**

`TSVZ/conformance/reference-baseline.json` lists every case the reference does not pass, and why. Each reason is a deviation Tasks 9–13 found; fixing them is a follow-up project (design section 10).

```json
{"impl": "python3 TSVZ.py",
 "features": ["bzip2", "xz", "crc32", "sha256", "file-first", "handler"],
 "note": "The reference also claims zstd on Python 3.14 and later. Every case it does not pass, and why; remove an entry when the reference is fixed.",
 "not_passing": [
  {"id": "12.4.5-marker-value-is-not-stripped", "modes": ["files", "socket", "forwarded"], "result": "FAIL",
   "reason": "Marker values are stripped, so \"false \" turns stripping off; §10.4 says marker lines are not stripped, which makes the value invalid (§12.4.5)."},
  {"id": "13.2-cr-token-decodes", "modes": ["files", "socket", "forwarded"], "result": "FAIL",
   "reason": "<CR> is not decoded (decision 10)."},
  {"id": "13.5-cr-token-at-the-end-of-a-row", "modes": ["files", "socket", "forwarded"], "result": "FAIL",
   "reason": "<CR> is not decoded (decision 10)."},
  {"id": "16.5-unsupported-zstd-part-fails", "modes": ["files", "socket", "forwarded"], "result": "FAIL", "python_below": [3, 14],
   "reason": "Without compression.zstd a .zst part is read as plain bytes instead of failing (decision 2)."},
  {"id": "13.3-set-encodes-carriage-returns", "modes": ["files", "socket", "forwarded"], "result": "FAIL",
   "reason": "A carriage return is written raw, not as <CR> (decision 10)."},
  {"id": "19.2-scrub-keeps-a-carriage-return", "modes": ["files", "socket", "forwarded"], "result": "FAIL",
   "reason": "<CR> is not decoded, so a scrub keeps it as text (decision 10)."},
  {"id": "20.2-parts-in-replay-order", "modes": ["files", "socket", "forwarded"], "result": "FAIL",
   "reason": "parts prints the suffix gz instead of the codec name gzip (decision 14)."},
  {"id": "20.2-parts-names-the-bzip2-codec", "modes": ["files", "socket", "forwarded"], "result": "FAIL",
   "reason": "parts prints the suffix bz2 instead of the codec name bzip2 (decision 14)."}
 ]}
```

- [ ] **Step 4: Run the whole suite on three Pythons**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `251 passed, 6 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && uv run -q --no-project --python 3.8 --with pytest python -m pytest -q tsvzconform_test.py`
Expected: `251 passed, 6 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && uv run -q --no-project --python 3.14 --with pytest python -m pytest -q tsvzconform_test.py`
Expected: `254 passed, 3 skipped` (with `compression.zstd`, the zstd cases run and the unsupported-zstd case is skipped).

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add conformance/reference-baseline.json tsvzconform_test.py && git commit -m "Pin the reference's conformance results in conformance/reference-baseline.json.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 16: Documentation

**Files:**
- Create: `TSVZ/conformance/README.md`
- Modify: `TSVZ/README.md`, `TSVZ/website/index.md`, `TSVZ/website/spec-glance.md`
- Modify: `/mnt/klein/work/cas/CLAUDE.md` (not in git)

**Interfaces:**
- Consumes: the runner's options and output (Tasks 1–8), the case format (Task 3), the corpus and baseline (Tasks 9–15).
- Produces: the implementer documentation and the pointers to it.

- [ ] **Step 1: Write `TSVZ/conformance/README.md`**

````markdown
# TSVZ conformance suite

The cases in `cases/` test how closely an implementation follows
[tsvz-spec-v1](../tsvz-spec-v1.md). Each case names the § clauses it tests and
says in one sentence why its outcome is the right one. `../tsvzconform.py` runs
them against any implementation, in any language, through its command line
(§20) and its handler socket (§21). You can also load the cases straight into
your own test framework.

## Running the suite

```bash
python3 tsvzconform.py --impl ./target/release/tsvz --feature crc32,sha256,handler
python3 tsvzconform.py --impl ./tsvz --class reader --only 13      # §13 reader cases only
python3 tsvzconform.py --list --only 9                             # what would run
```

Python 3.8 or later on Linux or macOS; standard library only. Run it from
the `TSVZ/` directory, or anywhere: it finds `conformance/cases/` beside itself.

| Option | Meaning |
|---|---|
| `--impl COMMAND` | The command that runs your tool, split like a shell would. A relative path resolves from where you start the runner. |
| `--class reader,writer,cli,handler` | The conformance classes to run (default all). Naming `handler` also claims the handler. |
| `--only PATTERNS` | Case ids, clauses (`9.2` matches `9.2-…` and `9.2.1-…`) or globs (`13.*`), comma-separated. |
| `--feature NAMES` | The optional features you implement (below), or `all`. |
| `--mode files,socket,forwarded` | Restrict the modes (below). |
| `--level must` | Skip SHOULD cases. |
| `-j N`, `-x`, `--timeout S` | Parallel cases (default: CPUs), stop at the first failure, limit per step (default 10 s). |
| `--work-dir DIR`, `--json PATH` | Where case directories go; a JSON record of every result. |

### Classes

| Class | What it tests | Operations it calls |
|---|---|---|
| reader | §4–§17: framing, variants, markers, escaping, defaults, integrity, compression, multi-part stores | `read`, `get`, `keys`, `len`, `verify`, `-V` |
| writer | §13.3 encoding, append-only writes, scrub and clear (§19) | the reader's, plus `set`, `set -`, `delete`, `delete -`, `clear`, `scrub` |
| cli | the whole of §20: arguments, every operation, output, exit statuses | everything in §20 |
| handler | §21: the pointer file, the protocol, consistency, `serve` and `stop` | `serve`, `stop`, the socket |

A class is conformant when none of its cases fails; the cli and handler
classes also need reader and writer to be conformant (§2.1).

### Features

Some things the spec lets you leave out look exactly like a bug from outside: a
reader without crc32 must ignore its markers (§15.3). So you claim what you
implement:

| Feature | Meaning |
|---|---|
| `bzip2`, `xz`, `zstd` | Reads parts in that codec (gzip is required, §16.5) |
| `crc32`, `crc32c`, `sha256`, `blake3` | Implements that digest (§15.2.1) |
| `file-first` | Accepts `tsvz STORE [OPERATION]` (§20.1.5) |
| `handler` | Provides `serve` and the socket (§21) |
| `scrub-multipart` | Scrubs a multi-part store instead of refusing (§20.2) |

A case marked `requires: ["x"]` runs only when you claim `x`; one marked
`["!x"]` runs only when you do not, and checks what the spec asks of an
implementation without `x`.

### Modes

Every reader case, and every case made only of plain operations, runs up to
three ways:

- `files`: your command on the files;
- `socket`: requests to your handler serving the store (§21.7.1 says it answers
  every §20 operation the same way);
- `forwarded`: your command while your handler serves the store (§20.8.1).

`socket` and `forwarded` need the `handler` feature, and their results count
toward the handler class.

### Reading the report

Each failure shows the case, its clauses, its note, the input files as JSON
strings (so tabs, `\r` and NUL are visible), the step that failed, what was
expected and what came back, the end of standard error, the directory it ran
in (kept for you), and the command line that reruns it alone:

```
FAIL 9.2-trailing-delimiter-is-not-a-tombstone   MUST §9.2 §14.2
  key<TAB> sets a present-empty cell; only a lone key deletes.
  files   s.tsvz = "a\tx\na\t\n"
  step 1  ./tsvz --format records -q read /tmp/tsvzconform-k3x9/9.2-…/s.tsvz
          row 1 missing: expected ["a", ""] (got 0 rows, expected 1)
  kept    /tmp/tsvzconform-k3x9/9.2-trailing-delimiter-is-not-a-tombstone
  rerun   python3 tsvzconform.py --impl ./tsvz --only 9.2-trailing-delimiter-is-not-a-tombstone
```

`FAIL` is a MUST that did not hold, `warn` a SHOULD, `skip` a case your claims
or the environment leave out, and `error` a bug in the suite itself. The runner
exits 0 when nothing failed, 1 when something did, 2 for a usage error and 130
after Ctrl-C.

## Testing a library that has no command line

Write a small adapter program. The runner needs only this of it for
`--class reader`:

- `ADAPTER -V` prints one line naming the implementation and exits 0;
- `ADAPTER --format records -q OP STORE [ARG ...]` for `OP` in `read`, `get`,
  `keys`, `len` and `verify`, printing what §20.2 says in the records format of
  §20.4.1 (the store's own delimiter, §13 escaping, one `\n`-terminated line per
  row) and exiting as §20.6 says: 0, 1 when the store is missing or cannot be
  read, 3 when `get` finds a key missing, 4 when `verify` finds a mismatch.

## Loading the cases yourself

Each file in `cases/<class>/` is a JSON object `{"format": 1, "class": ...,
"cases": [...]}`. Strings are exact: write them to disk as UTF-8. A value
`{"b64": "..."}` is raw bytes in base64 (compressed parts, invalid UTF-8).

| Field | Meaning |
|---|---|
| `id` | Unique; starts with the main § clause |
| `spec` | The § clauses the case tests |
| `level` | `MUST` or `SHOULD` |
| `requires` | Features the case needs (`!x`: only without `x`) |
| `note` | Why the expected outcome is right |
| `store` | The store's path in the case directory (default `s.tsvz`) |
| `files` | Path → content, written before the case starts |
| `modes` | Restricts the modes the case runs in |
| `expect` | Reader cases: what reading the store gives |
| `steps`, `after` | Other cases: what to run, and what the files must hold afterwards |

### Reader cases

Write `files` into an empty directory, open `store`, and check `expect`:

- `read`: the live rows, key first, in first-appearance order. Compare after
  removing trailing empty cells from each row: §3.6 lets a reader pad or trim.
  `{"rows": null, "exit": 1}` means reading must fail.
- `keys`, `len`: the live keys in order; how many there are.
- `get`: each query's `args` are keys looked up in order. A live key gives its
  row. A missing key gives the key and the current defaults, or nothing when
  `#_return_defaults_when_missing_#` is false (§14.5); `exit` 3 means at least
  one key was missing.
- `verify`: the mismatches, each `[part path, line, algorithm, expected,
  computed]`, where `{store}` stands for the store's path and `{dir}` for the
  case directory.

### Writer, CLI and handler cases

`steps` run in order. Each step is one of:

| Step | Meaning |
|---|---|
| `{"op": [OP, ARG...]}` | One operation on the store: `tsvz --format records -q OP STORE ARG...`, or the request `OP⇥ARG...` on a socket |
| `{"argv": [...]}` | Exactly these arguments after the command (`{store}`, `{dir}` substituted) |
| `{"request": [...], "options": [...]}`, `{"raw": "..."}` | One §21 request, by fields or as exact bytes |
| `{"append": DATA, "path": P}`, `{"write": DATA, "path": P}` | Append to, or create, a file directly: another writer |
| `{"check": {...}}` | An `after` block checked at that point |

A step may carry `stdin` (on a socket, the request body; `body` gives it
explicitly), `exit` (default 0), `stdout` and `stderr` (`"empty"` or
`"nonempty"`; never compared by content). `stdout` is `"empty"`,
`"nonempty"`, `{"text": ...}` (exact), `{"rows": [...]}` (split on the
delimiter, decoded, trailing empty cells ignored; a live row printed by `read`
must still contain a delimiter, or it would read back as a tombstone), or
`{"lines": [...]}` (split and decoded, exact; `verify` and `parts` lines split
on TAB).

An `after` block holds any of:

- `rows`, `missing`: the files read back give these live rows, and this
  missing-key result (`null` for not-found);
- `fidelity`: `"full"` means everything a reader can observe is unchanged since
  before the first step; `"markers"` the same except that no rows are left. What
  a reader observes is the rows, the missing-key result, and how two probe lines
  appended to the store would read (`tsvzconform-probe-1⇥v␠␠⇥⇥` and
  `tsvzconform-probe-2␠⇥w`, in the store's delimiter), which shows the final
  marker state;
- `exists`, `absent`: paths in the case directory;
- `serve_exit`: the handler's exit status;
- `bytes`: rules on the bytes written: `encoded` (every `<` begins a token, no
  raw `\r`, no byte-order mark at the start of a new part), `lf-terminated`,
  `appended` (no earlier byte changed), `last-part-only` (only the last part
  grew), `unchanged`, `same-file` (same inode and mode), `header-kept`, and
  `snapshot-layout` (§19.2.3).

## The reference implementation

`reference-baseline.json` lists every case the Python reference does not pass
yet, and why. `tsvzconform_test.py` checks that list against a real run, so it
changes only when the reference does.
````

- [ ] **Step 2: Update `TSVZ/README.md`**

Make these four replacements. The deviation list loses the five entries that decisions 5, 7, 8 and 9 made into spec rules, its `<CR>` entry is rewritten for decision 10, and the three deviations Tasks 10, 11 and 13 found are added.

Replace:

```markdown
## Spec deviations and interpretations

- §19.2.3c: scrub keeps the header comment (3.39 header verification needs it).
- §17.5: a numbered part opened directly is read alone, with a warning.
- §4.1: a leading byte order mark is tolerated (stripped, with a warning).
- A leading byte order mark is not part of any `#_checksum_*_#` segment.
- §12.4.1: boolean marker values also accept `yes/no/on/off/1/0`; TSVZ writes `true`/`false`.
- §14.5: the defaults-on-missing read applies to `t[key]` only.
```

with:

```markdown
## Spec deviations and interpretations

The conformance cases these deviations make fail are listed in
`conformance/reference-baseline.json` (see Conformance testing).

- §17.5: a numbered part opened directly is read alone, with a warning.
- §14.5: the defaults-on-missing read applies to `t[key]` only.
```

Replace:

```markdown
- Invalid UTF-8 is replaced with U+FFFD (and reported).
- v1 has no `<CR>` token, so a value whose last cell ends in `\r` loses it; values set through `TSVZed` are right-stripped, so this only affects rows passed raw to the append helpers.
```

with:

```markdown
- §13: `<CR>` is neither written nor read yet. A carriage return is written raw, so one that ends a row's last value is lost, and `<CR>` in a file reads as text.
- §10.4: marker values have trailing whitespace stripped, so `#_strip_trailing_whites_#` with `false ` turns stripping off; the spec makes that value invalid (§12.4.5).
- §16.5: before Python 3.14 a `.zst` part is read as plain bytes instead of failing.
- §20.2: `parts` names a compressed part's codec by its suffix (`gz`), not the codec name (`gzip`).
```

Replace:

```markdown
## Benchmarking implementations

```

with:

````markdown
## Conformance testing

`tsvzconform.py` tests how closely an implementation of tsvz-spec-v1, in any
language, follows the spec. Its cases are JSON files in `conformance/cases/`,
each naming the § clauses it tests, and it drives an implementation only
through the spec's command line (§20) and handler socket (§21).

```bash
python3 tsvzconform.py --impl ./tsvz --feature crc32,sha256,handler
python3 tsvzconform.py --impl ./tsvz --class reader --only 13
```

Each failure is printed with its input, what was expected, what came back and
a command line that reruns it alone; the summary gives a verdict for each
conformance class (reader, writer, cli, handler). `conformance/README.md`
explains the options and the features to claim, and how to load the cases into
another language's tests.

## Benchmarking implementations

````

Replace:

```markdown
python3 -m pytest tsvzbench_test.py -q                    # the benchmark harness

```

with:

```markdown
python3 -m pytest tsvzbench_test.py -q                    # the benchmark harness
python3 -m pytest tsvzconform_test.py -q                  # the conformance runner and cases, against the reference

```

- [ ] **Step 3: Update the site text**

In `TSVZ/website/index.md`, replace:

```markdown
Writing one in another language? The spec is one document with worked
examples and pseudocode. Test against the reference implementation, then
[open an issue](https://github.com/yufei-pan/TSVZ/issues) to be listed here.
```

with:

```markdown
Writing one in another language? The spec is one document with worked
examples and pseudocode, and the
[conformance suite](https://github.com/yufei-pan/TSVZ/tree/main/conformance)
tests an implementation against it, class by class: the Read, Write, CLI and
Handler columns above. Run it, then
[open an issue](https://github.com/yufei-pan/TSVZ/issues) with its report to be
listed here.
```

In `TSVZ/website/spec-glance.md`, replace:

```markdown
- `<sep>`, `<LF>`, `<lt>` and `<#>` stand for the delimiter, a newline, `<`
  and `#`. §13
```

with:

```markdown
- `<sep>`, `<LF>`, `<CR>`, `<lt>` and `<#>` stand for the delimiter, a newline, a
  carriage return, `<` and `#`. §13
```

Do not restart `tsvz-site.service`: publishing is the user's call.

- [ ] **Step 4: Update `/mnt/klein/work/cas/CLAUDE.md`**

Replace:

```markdown
Benchmarks (not tests): `TSVZ/benchTSVZ.py`, `TSVZ/tsvzbench.py` (compares tsvz-spec-v1 implementations as black boxes; tests in `TSVZ/tsvzbench_test.py`), `tee-logger/benchmark*.py`.
```

with:

```markdown
Benchmarks (not tests): `TSVZ/benchTSVZ.py`, `TSVZ/tsvzbench.py` (compares tsvz-spec-v1 implementations as black boxes; tests in `TSVZ/tsvzbench_test.py`), `tee-logger/benchmark*.py`.

Conformance: `TSVZ/tsvzconform.py --impl CMD` tests any tsvz-spec-v1 implementation against the JSON cases in `TSVZ/conformance/cases/`, each tied to a spec clause. Its tests, `TSVZ/tsvzconform_test.py`, run the reference and compare it with `TSVZ/conformance/reference-baseline.json`.
```

- [ ] **Step 5: Run every affected test**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py`
Expected: `90 passed` (the README's spec links and the edited pages render).

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q tsvzconform_test.py`
Expected: `251 passed, 6 skipped`.

Run: `cd /mnt/klein/work/cas/TSVZ && python3 tsvzconform.py --list | wc -l`
Expected: `291` (272 cases and 19 procedural checks).

- [ ] **Step 6: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ && git add conformance/README.md README.md website/index.md website/spec-glance.md && git commit -m "Document tsvzconform in conformance/README.md, the README and tsvz.org, and bring the deviation list in line with the spec.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

The cas `CLAUDE.md` edit stays uncommitted (the cas directory is not a repository). Report to the
user: the plan is done, the site edits are committed but not live, and the eight cases the
reference fails (four deviations: `<CR>`, stripped marker values, the zstd fallback and codec
names in `parts`), listed in `conformance/reference-baseline.json`, are the input to the
follow-up project.
