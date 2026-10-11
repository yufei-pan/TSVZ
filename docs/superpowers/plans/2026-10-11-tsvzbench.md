# tsvzbench Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standard-library benchmark harness, `TSVZ/tsvzbench.py`, that compares
tsvz-spec-v1 implementations in any language through the spec's command line
(§20) and handler socket (§21) — speed, CPU, memory and disk footprint — with a
correctness gate on every result.

**Architecture:** One self-contained module grown in ten sections, one per task:
command line → §13 codec and §7 replay model → deterministic fixtures → gate →
measuring launcher → §21 client and handler lifecycle → statistics and tables →
implementations and workload cases → the benchmark runner → report and entry
point. Each implementation is a black-box command. Every command runs under a
tiny `python -S -I` launcher that times it and reports its `wait4` rusage, so a
child's peak RSS is its own and not the harness's.

**Tech Stack:** Python ≥ 3.8 standard library (argparse, subprocess, `os.wait4`,
`os.fork`, AF_UNIX sockets, gzip, hashlib, zlib, statistics); pytest for tests.

**Spec:** `TSVZ/docs/superpowers/specs/2026-10-10-tsvz-bench-design.md`. Here, as
there, `§N` means tsvz-spec-v1 and "section N" means the design.

**Provenance.** Every code block below was written and run as a prototype before
this plan: each task's tests were checked to fail before its code (with the
error quoted in its Step 2) and to pass after it, on Python 3.8 and 3.13, and the
finished harness ran the `smoke` and `quick` suites against the reference. Copy
the blocks exactly; they are **tab-indented**.

## Global Constraints

- One file `TSVZ/tsvzbench.py`; tests in `TSVZ/tsvzbench_test.py`. Standard library only, Python ≥ 3.8, POSIX only (Linux primary, macOS works, Windows refused at start).
- Tab indentation, as in `TSVZ.py` and `benchTSVZ.py`.
- `version = '1.0'` and `__version__ = version` near the top; `-V` prints `tsvzbench 1.0`.
- The harness never imports `TSVZ`: it stays neutral between implementations.
- Not added to `setup.py`'s `py_modules` (like `benchTSVZ.py`): a development tool, not part of the wheel.
- Tests are plain pytest `test_*` functions, run from `TSVZ/` with `python3 -m pytest -q tsvzbench_test.py`. They use `TSVZ.py` beside them as the reference implementation (`TSVZBENCH_REFERENCE=/path/TSVZ.py` overrides it).
- Benchmark runs (not tests) on this machine use a work directory under `/mnt/gen5` (f2fs directly on the T700), e.g. `--work-dir /mnt/gen5/tsvzbench/work` — never `/tmp` (tmpfs) or anything under `/mnt/klein` (mergerfs).
- Commit on `main` of the TSVZ repo, as the design was; sentence-style messages like the history, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Do not push.

## Review Focus

Inputs and conditions the spec implies but its sections do not spell out, most
likely to bite first; each has a test in the task that owns the code:

1. **A relative implementation command**, e.g. `--impl ref='python3 TSVZ.py'` run from `TSVZ/`: it must run from the directory tsvzbench was started in, as it would in the shell — not from the run directory, where `TSVZ.py` does not exist. (Task 8, `test_a_relative_command_runs_from_the_starting_directory`; the code is `Impl.cwd` in Task 6.)
2. **A `read` of millions of rows** (10M in `large`, ~760 MB of output): the gate must stream the output file, not hold it whole next to the 5 GB model. (Task 4, `test_output_rows_stream_from_a_file`.)
3. **A handler that answers but ignores `stop`**: it must be killed (SIGTERM, then SIGKILL) within the wait, with nothing left running. (Task 6, `test_a_handler_that_ignores_stop_is_killed`.)
4. **A work directory whose path has spaces or non-ASCII characters**: every case must still pass. (Task 9, `test_a_work_directory_with_spaces_and_non_ascii`.)
5. **Ctrl-C in the middle of a handler workload**: exit 130 with a partial report, and no handler or dummy server left running. (Task 10, `test_an_interrupt_during_a_handler_workload_leaves_no_process`.)

## File map

- Create `TSVZ/tsvzbench.py`, built section by section (Tasks 1–10).
- Create `TSVZ/tsvzbench_test.py`, one section per task.
- Modify `TSVZ/README.md` (Task 10): a "Benchmarking implementations" section before "## Tests", and one line in the Tests block.
- Modify `/mnt/klein/work/cas/CLAUDE.md` (Task 10; the cas directory is not a git repo, so this edit is not committed).

---

### Task 1: Command line, suites and constants

**Files:**
- Create: `TSVZ/tsvzbench.py` (module docstring, imports, constants, section "Command line")
- Create: `TSVZ/tsvzbench_test.py` (imports, reference path, section "command line")

**Interfaces:**
- Consumes: nothing.
- Produces: `version`; `GENERATOR_VERSION = 1`; `SUITES` — name → dict with keys `sizes` (tuple of row counts), `mid`, `calls`, `seq`, `sync`, `clients` (tuple), `per_client`, `repeat`, `timeout`; `FAMILIES = ('cold', 'bulk', 'perop', 'maint')`; `FEATURE_SHAPES`; `OPTIONAL_SHAPES`; key index bases `NEW_KEY_BASE`, `ROTATED_KEY_BASE`, `MISSING_INDEX`; `HANDLER_WAIT`; `ROW_BYTES`; gate marks `OK, WRONG, EXIT, TIMEOUT, NA, REFUSED` (strings `'ok'`, `'wrong'`, `'exit'`, `'timeout'`, `'n/a'`, `'refused'`) and `FAILED = (WRONG, EXIT, TIMEOUT)`; `eprint(*args)`; `parse_args(argv=None)` → `argparse.Namespace(impls=[(name, command)], suite, params, families, seed, work_dir, keep, json)` where `params` is a copy of the suite's dict with `--repeat`/`--timeout` applied and `families` keeps `FAMILIES` order.

Usage errors go through `parser.error`, which exits with status 2 (design section 6.2). The imports cover the whole module so later tasks only append code.

- [ ] **Step 1: Write the failing tests**

Create `TSVZ/tsvzbench_test.py` with:

```python
import collections
import hashlib
import io
import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import zlib

import pytest

import tsvzbench as tb

HERE = os.path.dirname(os.path.abspath(__file__))
REFERENCE = os.environ.get('TSVZBENCH_REFERENCE', os.path.join(HERE, 'TSVZ.py'))
REF_COMMAND = '%s %s' % (shlex.quote(sys.executable), shlex.quote(REFERENCE))


# --- command line ------------------------------------------------------------------

@pytest.mark.parametrize('argv', [
	[],
	['--impl', 'noequals'],
	['--impl', 'bad name=x'],
	['--impl', 'a=x', '--impl', 'a=y'],
	['--impl', 'a=x', '--only', 'nope'],
	['--impl', 'a=x', '--repeat', '0'],
	['--impl', 'a=x', '--timeout', '0'],
	['--impl', 'a=x', '--suite', 'nope'],
])
def test_usage_errors_exit_2(argv, capsys):
	with pytest.raises(SystemExit) as info:
		tb.parse_args(argv)
	assert info.value.code == 2


def test_version(capsys):
	with pytest.raises(SystemExit) as info:
		tb.parse_args(['-V'])
	assert info.value.code == 0
	assert capsys.readouterr().out.strip() == 'tsvzbench ' + tb.version


def test_suite_and_overrides():
	opts = tb.parse_args(['--impl', 'a=x y', '--suite', 'quick', '--repeat', '2', '--timeout', '9', '--only', 'bulk,cold'])
	assert opts.impls == [('a', 'x y')]
	assert opts.params['repeat'] == 2 and opts.params['timeout'] == 9.0
	assert opts.params['sizes'] == tb.SUITES['quick']['sizes']
	assert opts.families == ('cold', 'bulk')
	assert tb.SUITES['quick']['repeat'] == 3
	defaults = tb.parse_args(['--impl', 'a=x'])
	assert defaults.suite == 'standard' and defaults.families == tb.FAMILIES and defaults.seed == 1


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `ModuleNotFoundError: No module named 'tsvzbench'`.

- [ ] **Step 3: Implement**

Create `TSVZ/tsvzbench.py` with:

```python
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


```

- [ ] **Step 4: Make the script executable, like `benchTSVZ.py`**

Make the script executable, like `benchTSVZ.py`: `chmod +x /mnt/klein/work/cas/TSVZ/tsvzbench.py`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `10 passed`.

- [ ] **Step 6: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Add tsvzbench: its command line, suites and constants.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Field codec and the replay model

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append section "Spec §13 field codec and the replay model (§7)")
- Modify: `TSVZ/tsvzbench_test.py` (append section "codec and replay model")

**Interfaces:**
- Consumes: nothing beyond Task 1.
- Produces: `DELIM = '\t'`; `decode_field(text, delimiter=DELIM)`; `encode_field(value, delimiter=DELIM, is_key=False)`; `record(key, cells)` → one record line with its `'\n'`; `trim(cells)`; `class Model` with `rows` (dict key → `(raw value fields, state index)`, in replay order), `apply_line(raw)`, `replay(data: bytes)` → self, `replay_file(path)` → self (`.gz` read through gzip), `resolve(key)` → trimmed resolved cells, `missing()` → trimmed current defaults or `None`, `iter_rows()` → `[key] + resolve(key)` per live key; `store_parts(store)` → part paths in replay order; `read_store(store)` → `Model`; `store_bytes(store)` → int.

The model keeps each live key's raw value fields plus the index of the marker state in force at its row, and resolves §14 defaults only when a row is read: that is how forward-only binding (§12.4.4) costs nothing at replay. A plain `dict` gives the order rules: overwriting keeps a key's position; `pop` then re-adding moves it to the end, as §3.4 (clarified in commit `736b883`) and Appendix B say. Marker keys are matched case-insensitively on raw bytes; marker values are decoded but never stripped (§10.4).

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- codec and replay model ------------------------------------------------------------

def replay_text(text):
	return tb.Model().replay(text.encode('utf-8'))


def test_codec_round_trips():
	for value in ['plain', 'a\tb', 'line\nbreak', 'a<b', '<sep>', '<LF>', '#x', 'é漢😀']:
		assert tb.decode_field(tb.encode_field(value)) == value
	assert tb.encode_field('#key', is_key=True) == '<#>key'
	assert tb.encode_field('#value') == '#value'
	assert tb.encode_field('<sep>') == '<lt>sep>'
	assert tb.decode_field('<unknown>') == '<unknown>'
	assert tb.record('#k', ['a\tb', '']) == '<#>k\ta<sep>b\t\n'


def test_appendix_c_worked_example():
	model = replay_text('#_version_#\t1\n#_defaults_#\tguest\t0\nalice\tAlice\t30\nbob\tBob\ncarol\t\t25\nalice\tAlice\t31\nbob\n')
	assert list(model.iter_rows()) == [['alice', 'Alice', '31'], ['carol', '', '25']]
	assert model.missing() == ['guest', '0']


def test_tombstones_and_empty_keys():
	model = replay_text('a\t1\n\tignored\n \t\nb\t\nc\n')
	assert list(model.iter_rows()) == [['a', '1'], ['b']]


def test_overwrite_keeps_position_and_rewrite_after_tombstone_moves_to_end():
	assert list(replay_text('a\t1\nb\t2\na\t3\n').iter_rows()) == [['a', '3'], ['b', '2']]
	assert [row[0] for row in replay_text('a\t1\nb\t2\na\nc\t3\na\t4\n').iter_rows()] == ['b', 'c', 'a']


def test_defaults_bind_to_the_row_position():
	model = replay_text('#_defaults_#\tx\ty\n#_fill_empty_with_default_#\ttrue\nk1\t\nk2\tv\n#_defaults_#\tX\tY\nk3\t\n')
	assert model.resolve('k1') == ['x', 'y']
	assert model.resolve('k2') == ['v', 'y']
	assert model.resolve('k3') == ['X', 'Y']


def test_strip_toggle_crlf_markers_and_commit_rule():
	model = replay_text('a \tv \n#_strip_trailing_whites_#\tfalse\nb\tw \r\n#_DEFAULTS_#\td\r\n#_Fill_Empty_With_Default_#\tTRUE\nc\t\nz\tunterminated')
	assert list(model.iter_rows()) == [['a', 'v'], ['b', 'w '], ['c', 'd']]


def test_missing_key_without_return_defaults():
	assert replay_text('#_return_defaults_when_missing_#\tfalse\n').missing() is None


def test_store_parts_order_and_bytes(tmp_path):
	for name in ('s.tsvz', 's.tsvz.f', 's.tsvz.10', 's.tsvz.2.rotated', 's.tsvz.a.gz', 'other.tsvz.1', 's.tsvz.serve'):
		(tmp_path / name).write_bytes(b'k\tv\n')
	parts = tb.store_parts(str(tmp_path / 's.tsvz'))
	assert [os.path.basename(p) for p in parts] == ['s.tsvz', 's.tsvz.a.gz', 's.tsvz.f', 's.tsvz.10']
	assert tb.store_bytes(str(tmp_path / 's.tsvz')) == 16


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: module 'tsvzbench' has no attribute 'decode_field'`.

- [ ] **Step 3: Implement**

Append to `TSVZ/tsvzbench.py`:

```python
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


```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `18 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Add the tsvzbench replay model of spec §7, §13 and §14, and multi-part store assembly.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Deterministic fixtures and bulk inputs

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append section "Fixtures")
- Modify: `TSVZ/tsvzbench_test.py` (append section "fixtures")

**Interfaces:**
- Consumes: `record`, `encode_field`, `read_store`, `Model` (Task 2); `NEW_KEY_BASE`, `ROTATED_KEY_BASE`, `FEATURE_SHAPES`, `ROW_BYTES`, `GENERATOR_VERSION` (Task 1).
- Produces: `key_of(index)` → 12 hex digits; `random_values(rng, count=5)`; generators `gen_plain(rng, rows, base=0)`, `gen_churn`, `gen_escaped`, `gen_markers`, `gen_checksum` (each yields lines); `write_lines(path, lines, compress=False)`; `HEADER`; `MULTIPART_ORDINALS`; `_MAKERS` — kind → maker for `plain`, `churn`, `escaped`, `markers`, `checksum`, `gz`, `multipart`, `bulk-new`, `bulk-add`, `bulk-del`; `class Fixture(kind, rows, directory, store_name)` with `files`, `path`, `copy_to(run_dir)` → store path, `model()`; `class Fixtures(directory, seed)` with `cached(kind, rows)`, `get(kind, rows)` → `Fixture`, `estimate(kind, rows)` → bytes still to generate; `planned_fixtures(params, families)` → ordered unique `[(kind, rows)]`.

Each fixture lives in its own subdirectory named by kind, rows, seed and `GENERATOR_VERSION`, with an empty `complete` file written last, so an interrupted generation is redone. `random.Random(stem)` seeds from the directory name: the same seed gives the same bytes. Keys are a permutation of the row index (unique without a set, unsorted). `churn` arithmetic: a pool of 1.6N keys, 0.8N tombstones, 0.2N re-inserts and overwrites up to 4N lines leave exactly N live keys with tombstones at 20%. `gen_checksum` feeds every line to the armed accumulators exactly as §7.4 says, including the other algorithm's marker lines. `gzip` is written with `mtime=0` and no file name, so `gz` is deterministic too.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- fixtures ------------------------------------------------------------------------

SHAPES = ('plain', 'churn', 'escaped', 'markers', 'checksum', 'gz', 'multipart')


def _files(fixture):
	return {name: open(os.path.join(fixture.directory, name), 'rb').read() for name in fixture.files}


@pytest.mark.parametrize('kind', sorted(tb._MAKERS))
def test_fixtures_are_deterministic(tmp_path, kind):
	first = tb.Fixtures(str(tmp_path / 'a'), 7).get(kind, 300)
	second = tb.Fixtures(str(tmp_path / 'b'), 7).get(kind, 300)
	other = tb.Fixtures(str(tmp_path / 'c'), 8).get(kind, 300)
	assert _files(first) == _files(second)
	assert _files(first) != _files(other)


def test_fixture_shapes(tmp_path):
	fixtures = tb.Fixtures(str(tmp_path), 1)
	for shape in SHAPES:
		assert len(fixtures.get(shape, 1000).model().rows) == 1000, shape
	lines = open(fixtures.get('churn', 1000).path, 'rb').read().split(b'\n')[1:-1]
	tombstones = [line for line in lines if b'\t' not in line]
	assert 3900 <= len(lines) <= 4100 and len(tombstones) >= 0.2 * len(lines)
	text = open(fixtures.get('escaped', 1000).path, encoding='utf-8').read()
	for token in ('<sep>', '<LF>', '<lt>', '<#>', '漢字'):
		assert token in text
	multipart = fixtures.get('multipart', 1000)
	ordinals = [name.split('.')[2] for name in multipart.files if name.count('.') == 2]
	assert sorted(ordinals) != sorted(ordinals, key=lambda o: int(o, 16))
	assert tb.key_of(tb.ROTATED_KEY_BASE) not in multipart.model().rows
	assert fixtures.cached('plain', 1000) and not fixtures.cached('plain', 999)
	assert fixtures.estimate('plain', 1000) == 0 and fixtures.estimate('churn', 10) == 4 * 10 * tb.ROW_BYTES


def test_bulk_inputs(tmp_path):
	fixtures = tb.Fixtures(str(tmp_path), 1)
	plain = fixtures.get('plain', 1000).model().rows
	new = tb.Model().replay_file(fixtures.get('bulk-new', 1000).path).rows
	assert len(new) == 1000 and not set(new) & set(plain)
	assert len(tb.Model().replay_file(fixtures.get('bulk-add', 1000).path).rows) == 10
	deleted = open(fixtures.get('bulk-del', 1000).path).read().split()
	assert len(deleted) == 100 and set(deleted) <= set(plain)


def test_checksum_digests_are_correct(tmp_path):
	lines = open(tb.Fixtures(str(tmp_path), 1).get('checksum', 3000).path, 'rb').read().split(b'\n')[:-1]
	for algo, digest in (('crc32', lambda data: '%08x' % (zlib.crc32(data) & 0xffffffff)),
			('sha256', lambda data: hashlib.sha256(data).hexdigest())):
		marker = ('#_checksum_%s_#' % algo).encode()
		at = [i for i, line in enumerate(lines) if line.split(b'\t')[0] == marker]
		assert len(at) >= 3 and lines[at[0]] == marker + b'\t'
		for previous, current in zip(at, at[1:]):
			segment = b''.join(line + b'\n' for line in lines[previous + 1:current])
			assert lines[current].split(b'\t')[1].decode() == digest(segment)


def test_planned_fixtures():
	params = tb.SUITES['quick']
	assert tb.planned_fixtures(params, ('perop',)) == [('plain', 10000)]
	planned = tb.planned_fixtures(params, tb.FAMILIES)
	assert len(planned) == len(set(planned))
	assert ('bulk-new', 1000) in planned and ('multipart', 10000) in planned


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: module 'tsvzbench' has no attribute '_MAKERS'`.

- [ ] **Step 3: Implement**

Append to `TSVZ/tsvzbench.py`:

```python
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


```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `32 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Generate tsvzbench's fixtures: seven store shapes and three bulk inputs, cached by seed.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The correctness gate

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append section "The gate")
- Modify: `TSVZ/tsvzbench_test.py` (append section "gate")

**Interfaces:**
- Consumes: `decode_field`, `trim`, `Model`, `read_store`, `store_bytes`, `DELIM` (Task 2); `Fixtures` (Task 3, in tests); marks (Task 1).
- Produces: `output_rows(data: bytes)` → list of `[key, cells...]`; `iter_output_rows(path)` → the same, streamed; `compare_rows(got, expected)` → `None` or the first difference (`'row N: got …, expected …'`, `'row N missing: …'`, `'extra row N: …'`); `_read(path)`; checks `check_exit_only`, `check_read`, `check_len`, `check_get`, `check_no_output`, `check_post`, `check_scrub`, `check_clear`, `check_scrub_parts` — each `(ctx)` → `(mark, detail)`; `expected_post(ctx)`; `_hashes(run_dir, names)`. A `ctx` is a `RunContext` (Task 8) with `case`, `sample`, `run_dir`, `store`, `stdout`, `fixture`, `stdin`, `before_bytes`, `before_hashes`; the checks only read those attributes, and Task 9 exercises them end to end.

The normal form drops trailing empty cells, so the padding §3.6 allows (`b⇥3⇥` vs `b⇥3`) compares equal while a row that leaves out a resolved default does not. The cross-check test is what keeps the model honest: for every shape the reference's `read` must equal the model. If it ever disagrees, find out which side is wrong before changing either — the reference can be the one at fault.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- gate ------------------------------------------------------------------------------

@pytest.mark.parametrize('shape', SHAPES)
def test_model_agrees_with_the_reference(tmp_path, shape):
	fixture = tb.Fixtures(str(tmp_path), 1).get(shape, 1000)
	done = subprocess.run([sys.executable, REFERENCE, '--format', 'records', '-q', 'read', fixture.path],
		stdout=subprocess.PIPE, check=True)
	assert tb.compare_rows(tb.output_rows(done.stdout), fixture.model().iter_rows()) is None


def test_gate_accepts_padding_but_not_a_missing_default():
	expected = list(replay_text('#_defaults_#\tx\ty\nk\tv\n').iter_rows())
	assert expected == [['k', 'v', 'y']]
	assert tb.compare_rows(tb.output_rows(b'k\tv\ty\t\t\n'), expected) is None
	assert tb.compare_rows(tb.output_rows(b'k\tv\ty'), expected) is None
	assert tb.compare_rows(tb.output_rows(b'k\tv\n'), expected).startswith('row 1:')
	assert tb.compare_rows([], expected).startswith('row 1 missing')
	assert tb.compare_rows(expected + [['z']], expected).startswith('extra row 2')
	assert tb.output_rows(b'') == [] and tb.output_rows(b'<#>k\ta<sep>b\n') == [['#k', 'a\tb']]


def test_output_rows_stream_from_a_file(tmp_path):
	import tracemalloc
	path = str(tmp_path / 'out')
	sample = b'a\t1\t\n<#>b\tx<sep>y\n\nc'
	open(path, 'wb').write(sample)
	assert list(tb.iter_output_rows(path)) == tb.output_rows(sample)
	with open(path, 'wb') as f:
		for i in range(200000):
			f.write(b'key%06d\tvalue\tmore\n' % i)
	tracemalloc.start()
	try:
		count = sum(1 for _ in tb.iter_output_rows(path))
		peak = tracemalloc.get_traced_memory()[1]
	finally:
		tracemalloc.stop()
	assert count == 200000 and peak < 5 * 1024 * 1024


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: module 'tsvzbench' has no attribute 'compare_rows'`.

- [ ] **Step 3: Implement**

Append to `TSVZ/tsvzbench.py`:

```python
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


```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `41 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Add the tsvzbench gate: compare output and post-state with the replay model.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The measuring launcher and child runner

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append section "Running children")
- Modify: `TSVZ/tsvzbench_test.py` (append section "children")

**Interfaces:**
- Consumes: nothing beyond Task 1.
- Produces: `_RSS_SCALE`; `LIVE_GROUPS` (process-group ids still running); `_LAUNCHER` (source run as `python -S -I -c`); `class Sample` with fields `wall, utime, stime, maxrss, rss_floor, inblock, oublock, nvcsw, nivcsw, exit, timed_out, store_bytes`, property `cpu`, `as_dict()`; `exit_code(status)`; `kill_group(pid, sig=SIGKILL)`; `_read_all(fd)`; `class Child(argv, cwd, stdin_path=None, stdout_path=None, stderr_path=None)` with `pid`, `started`, `sample`, `poll()` → bool, `wait(timeout, first_signal=SIGKILL)` → `Sample`; `run_child(argv, cwd, timeout, stdin_path=None, stdout_path=None, stderr_path=None)` → `Sample`; `kill_live_groups()` (also registered with `atexit`).

Why a launcher (design section 4.1): on exec, Linux folds the old memory map's high-water mark into the new program's `ru_maxrss`, and a forked child's map is a copy of its parent's — measured, `true` reports 512 MiB when spawned from a 500 MB process. The launcher is a fresh small interpreter: its child reports its own peak, or about the launcher's size when its peak is lower. The launcher reads its floor from `VmHWM`, because its own `ru_maxrss` carries the harness's size the same way. The floor is approximate: a child that peaks at about the launcher's size can report up to ~250 KiB *below* it, which is why the test allows `rss_floor - 1 MiB`. The launcher writes ten space-separated numbers to an inherited pipe; anything else (killed, crashed) yields a `Sample` with only `wall`, `exit` and `timed_out`.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- children --------------------------------------------------------------------------

def test_run_child_measures_exit_output_and_timeout(tmp_path):
	stdin, out, err = str(tmp_path / 'in'), str(tmp_path / 'out'), str(tmp_path / 'err')
	open(stdin, 'w').write('hello')
	sample = tb.run_child([sys.executable, '-c', 'import sys; sys.stdout.write(sys.stdin.read().upper()); sys.exit(3)'],
		str(tmp_path), 30, stdin_path=stdin, stdout_path=out)
	assert sample.exit == 3 and not sample.timed_out and open(out).read() == 'HELLO'
	assert sample.maxrss > 1024 * 1024 and sample.wall > 0 and sample.cpu > 0
	slow = tb.run_child([sys.executable, '-c', 'import time; time.sleep(30)'], str(tmp_path), 0.5)
	assert slow.timed_out and slow.wall < 5 and slow.exit == -9
	missing = tb.run_child([str(tmp_path / 'no-such-tool')], str(tmp_path), 5, stderr_path=err)
	assert missing.exit == 127 and 'cannot run' in open(err).read()
	assert not tb.LIVE_GROUPS


def test_child_rss_does_not_include_the_harness(tmp_path):
	ballast = bytearray(300 * 1024 * 1024)
	for at in range(0, len(ballast), 4096):
		ballast[at] = 1
	sample = tb.run_child([sys.executable, '-c', 'pass'], str(tmp_path), 30)
	assert sample.rss_floor - 1024 * 1024 < sample.maxrss < 100 * 1024 * 1024
	big = tb.run_child([sys.executable, '-c', 'x = bytearray(200 * 1024 * 1024); x[::4096] = b"1" * len(x[::4096])'],
		str(tmp_path), 30)
	assert big.maxrss > 200 * 1024 * 1024


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: module 'tsvzbench' has no attribute 'run_child'`.

- [ ] **Step 3: Implement**

Append to `TSVZ/tsvzbench.py`:

```python
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


```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `43 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Run tsvzbench's commands under a measuring launcher, so a child's peak RSS is its own.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The §21 client, implementations and handler lifecycle

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append section "The handler protocol (§21)")
- Modify: `TSVZ/tsvzbench_test.py` (append section "the handler protocol")

**Interfaces:**
- Consumes: `Child`, `kill_group`, `LIVE_GROUPS`, `_read_all` (Task 5); `encode_field`, `decode_field` (Task 2); `output_rows`, `compare_rows` (Task 4).
- Produces: `HandlerError`; `read_pointer(store)` → `(socket path, pid or None)` or `None`; `request(op, args=(), options=())` → request bytes; `response_status(lines)` → int; `response_rows(lines)`; `class Client(path, timeout=30.0)` with `roundtrip(payload)` → response lines (bytes, status line last), `close()`, `sock`; `proc_rss(pid)` → bytes or `None`; `class Impl(name, command, cwd=None)` with `argv`, `cwd` (default: the current directory), `version`, `handler`, `problem`, `handler_problem`, `cli(op, store, *args)`; `class Handler(impl, store, run_dir, wait)` with `start()` (raises `HandlerError`), `stop(wait=10.0)` → `Sample`, and `path`, `pid`, `load_time`, `rss_after_load`, `sample`; `client_loop(client, payloads)` → `(latencies, responses, start, end)`; `check_responses(responses, expected)` → `None` or the first problem; `_write_all(fd, data)`; `run_clients(path, plans, timeout)` → `(latencies, span, problem)`; `start_dummy(path)` → pid; `stop_dummy(pid)`.

`Impl` lives here because `Handler` needs `cli()` and `cwd`. All of an implementation's commands run in `impl.cwd`, the directory tsvzbench was started from, so a relative COMMAND resolves as in the shell (Review Focus 1); store paths are always absolute. `Handler.start` polls every 5 ms until the pointer file is valid **and** a `len` request answers `#0`, which is the load time. `run_clients` forks one client per plan; they connect, report ready on a pipe, and all start on one byte each from a shared "go" pipe, so the measured span is the concurrent one. The dummy server calls `setsid` and forks one process per connection, so 16 clients are not limited by one Python process and `killpg` removes it all.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- the handler protocol --------------------------------------------------------------------

def _short_socket_dir():
	return tempfile.mkdtemp(prefix='tsvzb-')


def test_request_encoding_and_response_parsing():
	assert tb.request('set', ['#k', 'a\tb'], ('--sync',)) == b'--sync\tset\t<#>k\ta<sep>b\n'
	assert tb.request('len') == b'len\n'
	lines = [b'#!\tnote\n', b'k\tv\t\n', b'#3\tmissing\n']
	assert tb.response_status(lines) == 3
	assert tb.response_rows(lines) == [['k', 'v']]


def test_read_pointer(tmp_path):
	store = str(tmp_path / 'p.tsvz')
	assert tb.read_pointer(store) is None
	open(store + '.serve', 'w').write('tsvz-handler\t1\naddress\tunix:/run/x/s\nhost\th\npid\t42\nx-extra\t1\n')
	assert tb.read_pointer(store) == ('/run/x/s', 42)
	open(store + '.serve', 'w').write('tsvz-handler\t1\naddress\ttcp:1.2.3.4:5\n')
	assert tb.read_pointer(store) is None
	open(store + '.serve', 'w').write('something-else\t1\naddress\tunix:/s\n')
	assert tb.read_pointer(store) is None


def test_client_reads_diagnostics_and_bulk_requests():
	path = os.path.join(_short_socket_dir(), 's')
	server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	server.bind(path)
	server.listen(1)

	def serve():
		conn, _ = server.accept()
		rfile = conn.makefile('rb')
		for line in rfile:
			if line == b'get\tk\n':
				conn.sendall(b'#!\twarning\nk\tv\n#0\n')
			elif line == b'set\t-\n':
				body = []
				for record in rfile:
					if record == b'#\n':
						break
					body.append(record)
				conn.sendall(b'#0\t%d records\n' % len(body))
		conn.close()

	thread = threading.Thread(target=serve, daemon=True)
	thread.start()
	client = tb.Client(path)
	assert client.roundtrip(b'get\tk\n') == [b'#!\twarning\n', b'k\tv\n', b'#0\n']
	assert client.roundtrip(b'set\t-\na\t1\nb\t2\n#\n') == [b'#0\t2 records\n']
	client.close()
	thread.join(5)
	server.close()


def test_check_responses():
	assert tb.check_responses([[b'k\tv\n', b'#0\n']], [['k', 'v']]) is None
	assert tb.check_responses([[b'#1\tboom\n']], [None]) == 'request 1: status 1'
	assert tb.check_responses([[b'k\tw\n', b'#0\n']], [['k', 'v']]).startswith('request 1: row 1')


def test_concurrent_clients_against_the_dummy_server():
	path = os.path.join(_short_socket_dir(), 's')
	pid = tb.start_dummy(path)
	try:
		plans = [[(b'get\tk\n', None)] * 20 for _ in range(3)]
		latencies, span, problem = tb.run_clients(path, plans, 10)
		wrong, _, trouble = tb.run_clients(path, [[(b'get\tk\n', ['k', 'v'])]], 10)
	finally:
		tb.stop_dummy(pid)
	assert problem is None and len(latencies) == 60 and span > 0
	assert trouble.startswith('request 1: row 1 missing')


def test_impl_command_line():
	impl = tb.Impl('x', "python3 'my tool.py' --x-fast")
	assert impl.argv == ['python3', 'my tool.py', '--x-fast']
	assert impl.cli('get', 's.tsvz', 'k') == ['python3', 'my tool.py', '--x-fast', '--format', 'records', '-q', 'get', 's.tsvz', 'k']


def test_handler_lifecycle_with_the_reference(tmp_path):
	store = str(tmp_path / 'h.tsvz')
	handler = tb.Handler(tb.Impl('ref', REF_COMMAND), store, str(tmp_path), 10.0)
	handler.start()
	try:
		assert handler.load_time > 0 and handler.pid
		client = tb.Client(handler.path)
		assert tb.response_status(client.roundtrip(tb.request('set', ['k', 'v']))) == 0
		assert tb.response_rows(client.roundtrip(tb.request('get', ['k']))) == [['k', 'v']]
		assert tb.response_status(client.roundtrip(b'set\t-\na\t1\nb\t2\n#\n')) == 0
		client.close()
	finally:
		sample = handler.stop()
	assert sample.exit == 0 and sample.maxrss > 0 and not os.path.exists(store + '.serve')
	assert list(tb.read_store(store).iter_rows()) == [['k', 'v'], ['a', '1'], ['b', '2']]


def test_handler_that_never_starts(tmp_path):
	impl = tb.Impl('bad', '%s -c "import sys; sys.exit(1)"' % shlex.quote(sys.executable))
	handler = tb.Handler(impl, str(tmp_path / 'h.tsvz'), str(tmp_path), 10.0)
	with pytest.raises(tb.HandlerError):
		handler.start()
	assert handler.stop().exit == 1


STUCK = r'''
import os, socket, sys, tempfile
args = sys.argv[1:]
store = args[-1]
path = os.path.join(tempfile.mkdtemp(prefix='tsvzb-'), 's')
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(path)
server.listen(4)
with open(store + '.serve', 'w') as f:
	f.write('tsvz-handler\t1\naddress\tunix:%s\npid\t%d\n' % (path, os.getpid()))
while True:
	conn, _ = server.accept()
	for line in conn.makefile('rb'):
		conn.sendall(b'#0\n')
	conn.close()
'''


def test_a_handler_that_ignores_stop_is_killed(tmp_path):
	import time
	script = tmp_path / 'stuck.py'
	script.write_text(STUCK)
	impl = tb.Impl('stuck', '%s %s' % (shlex.quote(sys.executable), shlex.quote(str(script))))
	handler = tb.Handler(impl, str(tmp_path / 'h.tsvz'), str(tmp_path), 10.0)
	handler.start()
	began = time.perf_counter()
	sample = handler.stop(wait=0.5)
	assert sample.timed_out and time.perf_counter() - began < 5
	assert not tb.LIVE_GROUPS
	deadline = time.perf_counter() + 5
	while time.perf_counter() < deadline:
		try:
			os.kill(handler.pid, 0)
		except ProcessLookupError:
			break
		time.sleep(0.05)
	else:
		pytest.fail('the stuck handler survived')


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: module 'tsvzbench' has no attribute 'request'`.

- [ ] **Step 3: Implement**

Append to `TSVZ/tsvzbench.py`:

```python
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


```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `52 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Add tsvzbench's §21 client, handler lifecycle, concurrent clients and dummy server.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Statistics and table formatting

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append section "Statistics and formatting")
- Modify: `TSVZ/tsvzbench_test.py` (append section "statistics and formatting")

**Interfaces:**
- Consumes: marks (Task 1).
- Produces: `median(values)`, `spread_pct(values)` (MAD as % of the median), `percentile(values, q)`, `geomean(values)` — all `None` on empty input; `fmt_time`, `fmt_net` (`'≈0'` at or below zero), `fmt_bytes`, `fmt_rss(value, floor)` (`'≤floor'` within 256 KiB of the floor), `fmt_rows` (`100k`, `1M`), `fmt_rate`, `fmt_ratio` (`'×0.50'`), `mark_text(mark)`; `class Style(enabled)` with `wrap(text, code)`; `DROP_ORDER`; `render_table(columns, rows, width=None, style=None)` where `columns` is `[(title, group or None)]` and `rows` are lists of `(text, ansi code or None)`.

When a table is wider than `width`, `render_table` drops whole column groups in `DROP_ORDER` — every implementation's `cpu` columns at once — so the implementations stay comparable. `width=None` (output to a file or pipe) keeps everything.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- statistics and formatting -----------------------------------------------------------------

def test_statistics():
	assert tb.median([3, 1, 2]) == 2 and tb.median([]) is None and tb.median([None, 4]) == 4
	assert tb.spread_pct([10]) is None and tb.spread_pct([9, 10, 11]) == 10.0
	assert tb.percentile([5, 1, 3, 2, 4], 0.5) == 3 and tb.percentile([], 0.5) is None
	assert tb.percentile(list(range(101)), 0.95) == 95
	assert abs(tb.geomean([2, 8]) - 4) < 1e-9 and tb.geomean([]) is None


def test_formatting():
	assert [tb.fmt_time(v) for v in (1.5, 0.0123, 42e-6, None)] == ['1.50 s', '12.3 ms', '42 µs', '—']
	assert tb.fmt_net(-0.001) == '≈0' and tb.fmt_net(None) == '' and tb.fmt_net(0.002) == '2.0 ms'
	assert [tb.fmt_bytes(v) for v in (512, 1536, 3 * 1024 ** 3, None)] == ['512 B', '1.5 KiB', '3.0 GiB', '—']
	assert tb.fmt_rss(9 * 2 ** 20, 9 * 2 ** 20) == '≤9.0 MiB' and tb.fmt_rss(40 * 2 ** 20, 9 * 2 ** 20) == '40.0 MiB'
	assert [tb.fmt_rows(n) for n in (100, 1000, 10000, 1000000, 1500)] == ['100', '1k', '10k', '1M', '1500']
	assert [tb.fmt_rate(v) for v in (52200, 2.5e6, 28.4, None)] == ['52.2k/s', '2.50M/s', '28/s', '—']
	assert tb.fmt_ratio(0.5) == '×0.50' and tb.fmt_ratio(None) == '—'
	marks = [(tb.OK, ''), (tb.WRONG, 'x'), (tb.EXIT, '1'), (tb.TIMEOUT, ''), (tb.REFUSED, ''), (tb.NA, '')]
	assert [tb.mark_text(m) for m in marks] == ['✓', '✗', '! 1', '⏱', 'refused', 'n/a']


def test_tables_drop_whole_column_groups():
	columns = [('name', None), ('cpu', 'cpu'), ('a disk', 'disk'), ('b disk', 'disk')]
	rows = [[('a' * 10, None), ('c' * 10, None), ('d' * 10, None), ('e' * 10, '31')]]
	assert 'b disk' in tb.render_table(columns, rows)
	narrow = tb.render_table(columns, rows, 40).split('\n')[0]
	assert 'cpu' not in narrow and 'a disk' in narrow and 'b disk' in narrow
	assert 'disk' not in tb.render_table(columns, rows, 15)
	assert '\x1b[31m' in tb.render_table(columns, rows, style=tb.Style(True))
	assert '\x1b[' not in tb.render_table(columns, rows, style=tb.Style(False))


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: module 'tsvzbench' has no attribute 'median'`.

- [ ] **Step 3: Implement**

Append to `TSVZ/tsvzbench.py`:

```python
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


```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `55 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Add tsvzbench's statistics and table formatting.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Probes, workload cases and socket plans

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append section "Implementations and workloads")
- Modify: `TSVZ/tsvzbench_test.py` (append section "implementations and workloads")

**Interfaces:**
- Consumes: `run_child` (Task 5); `Handler`, `HandlerError`, `request`, `Impl` (Task 6); check functions and `_read` (Task 4); `key_of`, `random_values` (Task 3); `record` (Task 2); `fmt_rows` (Task 7).
- Produces: `probe(impl, directory, timeout)` — sets `impl.version` (first line of `-V`), `impl.problem` (`'-V failed: exit N'` or `'-V failed: timed out'`), `impl.handler` / `impl.handler_problem`; `class Case(family, name, kind, shape=None, rows=None, op=None, args=(), stdin=None, expect_exit=0, check=check_exit_only, via_handler=False, min_repeat=1)` with `optional`, `ident`, `label` (e.g. `'read gz 100k'`); `build_cases(params, families)` → cases in run order, `startup` always first; `class Result(case, impl)` with `mark`, `samples`, `series` (name → `{'lat', 'ops', 'rss'}`), `extra`, `log`, `drift`, `add_series(name, latencies, ops=None, rss=None)`, `add_extra(name, value)`; `class RunContext(case, sample, run_dir, store, stdout, fixture, stdin)` plus `before_bytes`, `before_hashes`; `class SocketPlan(fixture, params)` with `gets`, `sets`, `syncs` (lists of `(payload, expected row or None)`), `mix` (client count → list of per-client plans) and `final` (the expected `Model` after every request); `class _BlindPlan(plan)` (the same requests, no expectations, for the dummy server).

`case.kind` is `'cli'` (one command per run), `'calls'` (single-operation CLI calls, optionally through the implementation's own handler) or `'socket'` (raw §21 loads). `startup` gets `min_repeat=5` so the floor subtracted for *net* times is stable even in `smoke`. In a `SocketPlan` each concurrent client owns its own keys and every `get` follows that client's own `set` of the same key, so expected answers and the final state do not depend on interleaving.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- implementations and workloads --------------------------------------------------------------

def test_probe_the_reference(tmp_path):
	impl = tb.Impl('ref', REF_COMMAND)
	tb.probe(impl, str(tmp_path / 'probe'), 30)
	assert impl.problem is None and impl.version and impl.handler


def test_probe_a_broken_command(tmp_path):
	impl = tb.Impl('bad', '%s -c "import sys; sys.exit(2)"' % shlex.quote(sys.executable))
	tb.probe(impl, str(tmp_path / 'probe'), 30)
	assert impl.problem == '-V failed: exit 2' and not impl.handler


def test_a_relative_command_runs_from_the_starting_directory(tmp_path, monkeypatch):
	import shutil
	shutil.copyfile(REFERENCE, str(tmp_path / 'ref.py'))
	monkeypatch.chdir(tmp_path)
	impl = tb.Impl('rel', '%s ref.py' % shlex.quote(sys.executable))
	tb.probe(impl, str(tmp_path / 'probe'), 30)
	assert impl.problem is None and impl.version and impl.handler


def test_build_cases():
	params = tb.SUITES['standard']
	cases = tb.build_cases(params, tb.FAMILIES)
	assert cases[0].name == 'startup' and cases[0].min_repeat == 5
	labels = [(case.family, case.label) for case in cases]
	for wanted in (('cold', 'read 1M'), ('cold', 'read gz 100k'), ('bulk', 'set - new 1M'),
			('maint', 'scrub multipart 100k'), ('perop', 'socket 100k'), ('perop', 'cli via handler 100k')):
		assert wanted in labels
	assert len({case.ident for case in cases}) == len(cases)
	assert [case.family for case in tb.build_cases(params, ('bulk',))] == ['cold'] + ['bulk'] * 5
	get = next(case for case in cases if case.name == 'get')
	assert get.expect_exit == 3 and get.args == (tb.key_of(0), tb.key_of(tb.MISSING_INDEX))
	mid_reads = [case.optional for case in cases if case.name == 'read' and case.rows == params['mid']]
	assert mid_reads == [False, False, False, False, False, True, True]


def test_socket_plan_is_deterministic_and_tracks_the_final_state(tmp_path):
	params = tb.SUITES['smoke']
	fixture = tb.Fixtures(str(tmp_path), 1).get('plain', params['mid'])
	first, second = tb.SocketPlan(fixture, params), tb.SocketPlan(fixture, params)
	assert first.gets == second.gets and first.mix == second.mix
	assert len(first.gets) == len(first.sets) == params['seq'] and len(first.syncs) == params['sync']
	assert [len(plans) for plans in first.mix.values()] == list(params['clients'])
	assert all(payload.startswith(b'--sync\tset\t') for payload, _ in first.syncs)
	known = fixture.model().rows
	new_keys = sum(1 for payload, _ in first.sets if payload.split(b'\t')[1].decode() not in known)
	assert len(first.final.rows) == params['mid'] + new_keys
	for plans in first.mix.values():
		owned = [set(payload.split(b'\t')[1] for payload, _ in plan) for plan in plans]
		assert all(not (a & b) for i, a in enumerate(owned) for b in owned[i + 1:])
	blind = tb._BlindPlan(first)
	assert all(want is None for _, want in blind.gets) and len(blind.mix[4]) == 4


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: module 'tsvzbench' has no attribute 'probe'`.

- [ ] **Step 3: Implement**

Append to `TSVZ/tsvzbench.py`:

```python
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


```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `60 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Add tsvzbench's probes, workload cases and §21 request plans.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: The benchmark runner

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append section "The benchmark: running": `environment`, `fs_warning`, `class Bench` without its report methods)
- Modify: `TSVZ/tsvzbench_test.py` (append section "running a benchmark")

**Interfaces:**
- Consumes: everything from Tasks 1–8.
- Produces: `environment(work)` → dict `host, cpu, cpus, python, kernel, fs`; `fs_warning(fs)` → message for `fuse*` file systems or `None`; `class Bench(opts, out=None)` with `impls`, `results`, `ceiling`, `work`, `fixtures`, `params`, `opts`, and methods `setup()`, `preflight()` → message or `None`, `cleanup()`, `prepare()` → message or `None` (setup, preflight, FUSE warning, probes, fixtures), `run_cases(cases)`, `run_cli_case(case)`, `run_calls_case(case)`, `run_socket_case(case)`, `run_ceiling(plan)`, `judge(case, ctx)`, `exit_status()` → 0/1, `_startup(impl)`; the file ends inside `class Bench` (Task 10 appends methods to it).

`_rounds` runs every implementation's warm-up (the gated run) first, then the timed repetitions with the order rotated each repetition (rep r runs implementation `(i + r) mod n` in slot i); an implementation that timed out gets no timed runs of that workload. Every run starts from an empty run directory; copying the fixture and checking the result are outside the timed region. A timed run whose exit status differs from its warm-up's sets `drift`, shown as `*` next to the mark in Task 10. The default work directory is `./tsvzbench-*`, not the system temp dir (often tmpfs).

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- running a benchmark -----------------------------------------------------------------------

FAKE = r'''
import subprocess, sys, time
mode, reference, args = sys.argv[1], sys.argv[2], sys.argv[3:]
ops = ('read', 'len', 'get', 'set', 'delete', 'scrub', 'clear', 'verify', 'serve', 'stop')
op = next((a for a in args if a in ops), None)
if mode == 'exit1' and op == 'read':
	sys.exit(1)
if mode == 'sleep' and op == 'len':
	time.sleep(60)
if mode == 'noserve' and op == 'serve':
	sys.exit(1)
data = sys.stdin.buffer.read() if args[-1:] == ['-'] else b''
if mode == 'corrupt':
	data = data.replace(b'\t', b'\tX', 1)
done = subprocess.run([sys.executable, reference] + args, input=data, stdout=subprocess.PIPE)
out = done.stdout
if mode == 'drop' and op == 'read':
	out = b''.join(out.splitlines(True)[:-1])
sys.stdout.buffer.write(out)
sys.exit(done.returncode)
'''


def fake(tmp_path, mode):
	script = tmp_path / 'fake.py'
	script.write_text(FAKE)
	return '%s %s %s %s' % (shlex.quote(sys.executable), shlex.quote(str(script)), mode, shlex.quote(REFERENCE))


def bench_argv(tmp_path, impls, extra):
	argv = ['--suite', 'smoke', '--work-dir', str(tmp_path / 'work')] + list(extra)
	for name, command in impls:
		argv += ['--impl', '%s=%s' % (name, command)]
	return argv


def run_suite(tmp_path, impls, *extra):
	"""prepare() and every case of the selected families, without the report."""
	bench = tb.Bench(tb.parse_args(bench_argv(tmp_path, impls, extra)), out=io.StringIO())
	try:
		problem = bench.prepare()
		if problem is None:
			bench.run_cases(tb.build_cases(bench.params, bench.opts.families))
	finally:
		bench.cleanup()
	return bench, problem


def marks(bench, name):
	return {(r.case.family, r.case.label): r.mark for r in bench.results if r.impl.name == name}


def test_every_case_passes_with_the_reference(tmp_path):
	bench, problem = run_suite(tmp_path, [('ref', REF_COMMAND)])
	assert problem is None
	found = marks(bench, 'ref')
	assert {key: mark for key, mark in found.items() if mark[0] not in (tb.OK, tb.REFUSED)} == {}
	assert found[('maint', 'scrub multipart 100')][0] == tb.REFUSED
	assert bench.exit_status() == 0
	startup = next(r for r in bench.results if r.case.name == 'startup')
	assert len(startup.samples) == 5
	socket_result = next(r for r in bench.results if r.case.kind == 'socket')
	assert set(socket_result.series) == {'get', 'set', '--sync set', 'mix ×1', 'mix ×4'}
	assert socket_result.extra['load_time'] and set(bench.ceiling.series) == set(socket_result.series)
	if socket_result.extra.get('rss_after_load'):
		assert socket_result.extra['handler_maxrss'][0] >= socket_result.extra['rss_after_load'][0]


@pytest.mark.parametrize('mode, family, label, kind', [
	('drop', 'cold', 'read 100', tb.WRONG),
	('exit1', 'cold', 'read 100', tb.EXIT),
	('sleep', 'cold', 'len 100', tb.TIMEOUT),
	('corrupt', 'bulk', 'set - new 100', tb.WRONG),
])
def test_the_gate_catches_a_misbehaving_implementation(tmp_path, mode, family, label, kind):
	bench, _ = run_suite(tmp_path, [('bad', fake(tmp_path, mode))], '--only', family, '--timeout', '3')
	result = next(r for r in bench.results if (r.case.family, r.case.label) == (family, label))
	assert result.mark[0] == kind
	assert bench.exit_status() == 1
	if kind == tb.TIMEOUT:
		assert result.samples == []


def test_an_implementation_without_a_handler(tmp_path):
	bench, _ = run_suite(tmp_path, [('nos', fake(tmp_path, 'noserve'))], '--only', 'perop')
	found = marks(bench, 'nos')
	assert found[('perop', 'cli 100')][0] == tb.OK
	assert found[('perop', 'cli via handler 100')][0] == tb.NA
	assert found[('perop', 'socket 100')][0] == tb.NA
	assert bench.exit_status() == 0


def test_preflight_refuses_a_full_disk(tmp_path, monkeypatch):
	Stat = collections.namedtuple('Stat', 'f_bavail f_frsize')
	monkeypatch.setattr(tb.os, 'statvfs', lambda path: Stat(1, 1))
	bench, problem = run_suite(tmp_path, [('ref', REF_COMMAND)])
	assert 'free' in problem and bench.results == []


def test_work_directory_lifecycle(tmp_path, monkeypatch):
	monkeypatch.chdir(tmp_path)
	bench = tb.Bench(tb.parse_args(['--impl', 'ref=' + REF_COMMAND, '--suite', 'smoke']), out=io.StringIO())
	bench.setup()
	assert os.path.dirname(os.path.realpath(bench.work)) == os.path.realpath(str(tmp_path))
	assert os.path.basename(bench.work).startswith('tsvzbench-')
	bench.cleanup()
	assert not os.path.exists(bench.work)
	kept, _ = run_suite(tmp_path, [('ref', REF_COMMAND)], '--only', 'bulk', '--keep')
	assert os.listdir(os.path.join(kept.work, 'runs', 'ref'))
	again, _ = run_suite(tmp_path, [('ref', REF_COMMAND)], '--only', 'bulk')
	assert not os.path.exists(os.path.join(again.work, 'runs')) and os.listdir(again.fixtures.directory)


def test_a_work_directory_with_spaces_and_non_ascii(tmp_path):
	work = str(tmp_path / 'my bench é')
	bench = tb.Bench(tb.parse_args(['--impl', 'ref=' + REF_COMMAND, '--suite', 'smoke', '--only', 'cold,maint',
		'--work-dir', work]), out=io.StringIO())
	try:
		assert bench.prepare() is None
		bench.run_cases(tb.build_cases(bench.params, bench.opts.families))
	finally:
		bench.cleanup()
	assert all(r.mark[0] in (tb.OK, tb.REFUSED) for r in bench.results)


def test_fs_warning():
	assert 'mergerfs' in tb.fs_warning('fuse.mergerfs')
	assert tb.fs_warning('ext4') is None
	assert set(tb.environment(os.getcwd())) == {'host', 'cpu', 'cpus', 'python', 'kernel', 'fs'}


```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: module 'tsvzbench' has no attribute 'Bench'`.

- [ ] **Step 3: Implement**

Append to `TSVZ/tsvzbench.py`:

```python
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

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `70 passed`.

- [ ] **Step 5: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py
git commit -m "Run tsvzbench's workloads: gated warm-ups, rotated timed repetitions, handlers and the client ceiling.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Report, entry point and documentation

**Files:**
- Modify: `TSVZ/tsvzbench.py` (append the report methods to `class Bench`, then the section "Entry point")
- Modify: `TSVZ/tsvzbench_test.py` (append section "report and entry point")
- Modify: `TSVZ/README.md` (new section before "## Tests"; one line in the Tests block)
- Modify: `/mnt/klein/work/cas/CLAUDE.md` (Benchmarks line; not a git repo)

**Interfaces:**
- Consumes: `Bench` and everything before it (Tasks 1–9).
- Produces: `Bench.run()` → exit status (prepare, `run_cases(build_cases(...))`, `report()`, `exit_status()`); `Bench.report(partial=False)`; `Bench.cli_table(...)`, `Bench.perop_table(...)`, `Bench.footprint()`, `Bench.summary()`, `Bench.write_json(env)`; module-level `_us(seconds)`, `_interrupt(signum, frame)`, `main(argv=None)` → exit status (0 all passed, 1 a failure or an implementation that failed `-V`, 130 after an interrupt; usage errors raise `SystemExit(2)`), and the `__main__` guard.

`main` turns SIGTERM into `KeyboardInterrupt` for its duration and restores the previous handler on the way out. On an interrupt it kills every live process group, prints the report of what finished, marked "interrupted: partial results", and returns 130. The perop table shows the dummy server's ops/s as a `ceiling` column, so a fast handler limited by the harness client is visible as such.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ/tsvzbench_test.py`:

```python
# --- report and entry point --------------------------------------------------------------------

def run_bench(tmp_path, impls, *extra):
	out = io.StringIO()
	bench = tb.Bench(tb.parse_args(bench_argv(tmp_path, impls, extra)), out=out)
	try:
		code = bench.run()
	finally:
		bench.cleanup()
	return bench, code, out.getvalue()


def test_smoke_report_and_json(tmp_path):
	report = str(tmp_path / 'r.json')
	bench, code, out = run_bench(tmp_path, [('ref', REF_COMMAND)], '--json', report)
	assert code == 0
	assert out.startswith('tsvzbench %s' % tb.version)
	for section in ('[cold]', '[bulk]', '[perop]', '[maint]', '[footprint]', '[summary]', 'ceiling ops/s', 'refused'):
		assert section in out
	perop = out[out.index('[perop]'):out.index('[maint]')]
	assert ' rss ' in perop.split('\n')[1] and 'MiB' in perop
	assert '→' in out[out.index('[maint]'):out.index('[footprint]')]
	document = json.load(open(report))
	assert document['schema'] == 1 and document['impls'][0]['handler'] is True
	assert document['harness']['suite'] == 'smoke' and 'get' in document['ceiling']
	read = next(e for e in document['results'] if e['workload'] == 'read' and e['shape'] == 'plain')
	assert read['gate']['mark'] == 'ok' and read['samples'][0]['rss_floor'] > 0
	socket_entry = next(e for e in document['results'] if e['workload'] == 'socket')
	assert len(socket_entry['latency']['get']['latencies_us']) == socket_entry['latency']['get']['requests']


def test_report_compares_two_implementations(tmp_path):
	bench, code, out = run_bench(tmp_path, [('ref', REF_COMMAND), ('ref2', REF_COMMAND)], '--only', 'cold')
	assert code == 0 and 'ref2 wall' in out and '×' in out
	summary = out[out.index('[summary]'):]
	assert 'ref2' in summary and 'cold ×' in summary


def test_report_lists_failures_with_their_stderr(tmp_path):
	bench, code, out = run_bench(tmp_path, [('bad', fake(tmp_path, 'exit1'))], '--only', 'cold')
	summary = out[out.index('[summary]'):]
	assert code == 1 and '! 1 bad read 100' in summary


def test_main_exit_statuses(tmp_path, monkeypatch, capsys):
	argv = bench_argv(tmp_path, [('ref', REF_COMMAND)], ['--only', 'cold'])
	assert tb.main(argv) == 0
	with pytest.raises(SystemExit) as info:
		tb.main(['--suite', 'nope'])
	assert info.value.code == 2

	def interrupted(self, cases):
		raise KeyboardInterrupt

	monkeypatch.setattr(tb.Bench, 'run_cases', interrupted)
	capsys.readouterr()
	assert tb.main(argv) == 130
	assert 'partial results' in capsys.readouterr().out


def test_an_interrupt_during_a_handler_workload_leaves_no_process(tmp_path, monkeypatch):
	dummies, handlers = [], []
	real_dummy, real_loop, real_start = tb.start_dummy, tb.client_loop, tb.Handler.start

	def spy_dummy(path):
		dummies.append(path)
		return real_dummy(path)

	def spy_start(self):
		real_start(self)
		handlers.append(self.pid)

	def interrupting(client, payloads):
		if client.sock.getpeername() in dummies:
			return real_loop(client, payloads)
		raise KeyboardInterrupt

	monkeypatch.setattr(tb, 'start_dummy', spy_dummy)
	monkeypatch.setattr(tb, 'client_loop', interrupting)
	monkeypatch.setattr(tb.Handler, 'start', spy_start)
	assert tb.main(bench_argv(tmp_path, [('ref', REF_COMMAND)], ['--only', 'perop'])) == 130
	assert handlers and not tb.LIVE_GROUPS
	for pid in handlers:
		with pytest.raises(ProcessLookupError):
			os.kill(pid, 0)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: FAIL with `AttributeError: 'Bench' object has no attribute 'run'`.

- [ ] **Step 3: Add the report methods to `Bench`**

The file still ends inside `class Bench` (Task 9), so append these methods — indented one tab, as methods — to the end of `TSVZ/tsvzbench.py`:

```python
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


```

- [ ] **Step 4: Add the entry point**

Append to the end of `TSVZ/tsvzbench.py`, after a blank line:

```python
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q -x tsvzbench_test.py`

Expected: PASS, `75 passed` (about 30 s).

Then the same suite on the oldest supported Python:

Run: `cd /mnt/klein/work/cas/TSVZ && uv run --no-project --python 3.8 --with pytest python -m pytest -q tsvzbench_test.py`

Expected: PASS, the same count.

- [ ] **Step 6: Document it**

In `TSVZ/README.md`, insert this section immediately before `## Tests`:

````markdown
## Benchmarking implementations

`tsvzbench.py` compares implementations of tsvz-spec-v1, in any language, side
by side on one machine. It drives each one only through the spec's command
line (§20) and, when it has one, its handler socket (§21), so any conformant
`tsvz` tool can be measured as it is.

```bash
python3 tsvzbench.py --impl ref='python3 TSVZ.py' --impl new='../tsvz-rs/target/release/tsvz' \
                     --suite quick --work-dir /mnt/fast-disk/tsvzbench
```

- `--impl NAME=COMMAND`, repeatable: the first is the baseline, and every other
  implementation gets a `×` ratio against it (below 1 is faster). A relative
  COMMAND runs from the directory you start tsvzbench in.
- `--suite smoke|quick|standard|large`: `smoke` checks a setup in seconds,
  `quick` takes about a minute, `standard` (the default, up to 1M rows) about
  five, and `large` (up to 10M rows) twenty or more and ~5 GB of memory for the
  harness itself.
- `--only cold,bulk,perop,maint` picks families: one-shot replays, `set -` and
  `delete -` batches, single operations (CLI and handler), and scrub/clear.
- `--work-dir DIR` keeps the generated fixtures between runs. Put it on the disk
  you mean to measure: not a tmpfs, and not a FUSE file system such as mergerfs
  (the harness warns about those). The default is a `tsvzbench-*` directory here.
- `--json PATH` also writes every raw sample.

The harness writes its own input stores, so every implementation reads the same
bytes, and checks every result against its own replay model: `✓` right, `✗`
wrong output or files, `! N` an unexpected exit status, `⏱` timed out, `refused`
an allowed refusal. It exits 1 when any cell failed. Each command runs under a
small launcher that measures it with `wait4`; a peak RSS shown as `≤8.0 MiB` is
the launcher's own size, so the true peak is no higher. Compare an
implementation with itself to see the noise: a few percent per cell, up to
about 10% for concurrent socket loads.
````

In the `## Tests` code block of `TSVZ/README.md`, add after the `website/tsvz_site_test.py` line:

```bash
python3 -m pytest tsvzbench_test.py -q                     # the benchmark harness
```

In `/mnt/klein/work/cas/CLAUDE.md`, replace the line

```markdown
Benchmarks (not tests): `TSVZ/benchTSVZ.py`, `tee-logger/benchmark*.py`.
```

with

```markdown
Benchmarks (not tests): `TSVZ/benchTSVZ.py`, `TSVZ/tsvzbench.py` (compares tsvz-spec-v1 implementations as black boxes; tests in `TSVZ/tsvzbench_test.py`), `tee-logger/benchmark*.py`.
```

Then check the website tests still pass (they read `README.md`): `cd /mnt/klein/work/cas/TSVZ && python3 -m pytest -q website/tsvz_site_test.py` — expected PASS.

- [ ] **Step 7: Run real benchmarks against the reference**

```bash
cd /mnt/klein/work/cas/TSVZ
mkdir -p /mnt/gen5/tsvzbench/work
./tsvzbench.py --impl ref='python3 TSVZ.py' --suite smoke --work-dir /mnt/gen5/tsvzbench/work; echo "exit $?"
./tsvzbench.py --impl ref='python3 TSVZ.py' --impl ref2='python3 TSVZ.py' --suite quick --work-dir /mnt/gen5/tsvzbench/work --json /mnt/gen5/tsvzbench/quick.json; echo "exit $?"
```

Expected: both exit 0; every mark `✓` except `scrub multipart`, which is `refused`; the header shows `fs f2fs` and no FUSE warning; `ref2`'s ratios sit within about ±5% of `×1.00`, socket `mix` rows up to about ±10% (that spread is the noise floor), and its summary within ±2%; RSS grows with the store size (about 33 MiB at 1k rows, 40 MiB for `read` at 10k) instead of staying flat; the quick run takes about 50 s. Also run the relative command from another directory to see it fail cleanly: `cd /tmp && /mnt/klein/work/cas/TSVZ/tsvzbench.py --impl ref='python3 TSVZ.py' --suite smoke --work-dir /mnt/gen5/tsvzbench/work` must report `ref` as `-V failed: exit 2` and exit 1.

- [ ] **Step 8: Commit**

```bash
cd /mnt/klein/work/cas/TSVZ
git add tsvzbench.py tsvzbench_test.py README.md
git commit -m "Report tsvzbench results as tables, footprint, summary and JSON, and document it.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
