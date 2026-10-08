#! /usr/bin/env python3
"""Tests for TSVZ 4.1 (TSVZ.py).

Plain ``test_*`` functions (pytest-style, no class boilerplate), matching the
repo convention. Runnable with ``python -m pytest test_TSVZ.py`` or directly
with ``python test_TSVZ.py`` (a tiny runner at the bottom executes every
``test_*`` function and reports pass/fail).
"""
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import TSVZ


def _tmp(suffix='.tsv'):
	fd, path = tempfile.mkstemp(suffix=suffix)
	os.close(fd)
	os.remove(path)  # we only want a unique name; let TSVZ create it
	return path


# --------------------------------------------------------------------------
# Issue 1 + 3: deletion must survive a reload (tombstone correctness)
# --------------------------------------------------------------------------
def test_tsvzed_delete_persists_after_reload():
	f = _tmp()
	try:
		t = TSVZ.TSVZed(f, header='id\tname\tval', rewrite_on_load=False)
		t['a'] = ['a', 'alice', '1']
		t['b'] = ['b', 'bob', '2']
		time.sleep(0.05)
		del t['a']
		t.close()  # flushes the append queue

		t2 = TSVZ.TSVZed(f, header='id\tname\tval', rewrite_on_load=False)
		assert 'a' not in t2, f"deleted key resurrected as {t2.get('a')!r}"
		assert 'b' in t2
		t2.close()
	finally:
		os.path.exists(f) and os.remove(f)


# --------------------------------------------------------------------------
# Issue 2: `tsv[key] = key` (a lone key with no values) must delete, not re-add
# --------------------------------------------------------------------------
def test_tsvzed_setitem_lone_key_deletes():
	f = _tmp()
	try:
		t = TSVZ.TSVZed(f, header='id\tname\tval', rewrite_on_load=False)
		t['a'] = ['a', 'alice', '1']
		time.sleep(0.05)
		t['a'] = 'a'  # lone key -> delete
		assert 'a' not in t, f"expected delete, got {t.get('a')!r}"
		t.close()
	finally:
		os.path.exists(f) and os.remove(f)


def test_tsvzedlite_setitem_lone_key_deletes():
	f = _tmp()
	try:
		with open(f, 'w') as fh:
			fh.write('id\tname\tval\n')
		lite = TSVZ.TSVZedLite(f, header='id\tname\tval', strict=False)
		lite['a'] = ['a', 'alice', '1']
		lite['a'] = 'a'  # lone key -> delete
		assert 'a' not in lite, f"expected delete, got index {lite.indexes.get('a')!r}"
		lite.close()
	finally:
		os.path.exists(f) and os.remove(f)


# --------------------------------------------------------------------------
# Issue 1 (Lite variant): del lite[key] must persist across a reload
# --------------------------------------------------------------------------
def test_tsvzedlite_delete_persists_after_reload():
	f = _tmp()
	try:
		with open(f, 'w') as fh:
			fh.write('id\tname\tval\n')
		lite = TSVZ.TSVZedLite(f, header='id\tname\tval', strict=False)
		lite['a'] = ['a', 'alice', '1']
		lite['b'] = ['b', 'bob', '2']
		del lite['a']
		assert 'a' not in lite
		lite.close()

		lite2 = TSVZ.TSVZedLite(f, header='id\tname\tval', strict=False)
		assert 'a' not in lite2, f"deleted key resurrected as {lite2.get('a')!r}"
		assert 'b' in lite2
		lite2.close()
	finally:
		os.path.exists(f) and os.remove(f)


# --------------------------------------------------------------------------
# Issue 3: deleting must not raise when the column count is unknown
# --------------------------------------------------------------------------
def test_delete_no_keyerror_when_column_count_unknown():
	f = _tmp()
	try:
		t = TSVZ.TSVZed(f, header='', rewrite_on_load=False)
		t['a'] = ['a', 'b']
		time.sleep(0.05)
		t.correctColumnNum = 0  # simulate undetermined column count
		del t['a']  # must not raise KeyError
		assert 'a' not in t
		t.close()
	finally:
		os.path.exists(f) and os.remove(f)


# --------------------------------------------------------------------------
# Issue 4: get_delimiter must be pure (no module-global leakage)
# --------------------------------------------------------------------------
def test_get_delimiter_is_pure():
	# Resolving a csv name must not change what a later bare call returns.
	assert TSVZ.get_delimiter(..., 'a.csv') == ','
	assert TSVZ.get_delimiter(...) == '\t', "bare get_delimiter() leaked the csv delimiter"
	assert TSVZ.get_delimiter(..., 'b.psv') == '|'
	assert TSVZ.get_delimiter(...) == '\t'
	assert TSVZ.DEFAULT_DELIMITER == '\t'


def test_get_delimiter_compressed_extension():
	assert TSVZ.get_delimiter(..., 'data.csv.gz') == ','
	assert TSVZ.get_delimiter(..., 'data.psv.zst') == '|'
	assert TSVZ.get_delimiter(..., 'data.tsv.xz') == '\t'
	assert TSVZ.get_delimiter(..., 'DATA.CSV') == ','  # case-insensitive


# --------------------------------------------------------------------------
# Issue 9: backward last-line reader must report the correct byte offset
# --------------------------------------------------------------------------
def test_read_last_valid_line_offset_across_chunks():
	f = _tmp()
	try:
		header = 'id\tname\tval\n'
		dataline = 'k1\talice\t100\n'
		with open(f, 'wb') as fh:
			fh.write(header.encode())
			fh.write(dataline.encode())
			fh.write(b'\n' * 3000)  # force multiple backward chunks
		true_offset = len(header.encode())
		got = TSVZ.read_last_valid_line(f, {}, correctColumnNum=3, delimiter='\t', storeOffset=True)
		assert got == true_offset, f"offset {got} != {true_offset}"
		with open(f, 'rb') as fh:
			fh.seek(got)
			assert fh.readline() == dataline.encode()
		# non-offset path still returns the right line
		line = TSVZ.read_last_valid_line(f, {}, correctColumnNum=3, delimiter='\t')
		assert line == ['k1', 'alice', '100']
	finally:
		os.path.exists(f) and os.remove(f)


# --------------------------------------------------------------------------
# Issue 6: rewrite must not emit '#' comment keys but must keep #_defaults_#
# --------------------------------------------------------------------------
def test_hardmap_skips_comments_keeps_defaults():
	f = _tmp()
	try:
		t = TSVZ.TSVZed(f, header='id\tname\tval', rewrite_on_load=False, defaults='#_defaults_#\t\tNA')
		t['a'] = ['a', 'alice', '1']
		t['#note'] = ['#note', 'should', 'stay-in-ram']
		t.hardMapToFile()
		t.close()
		with open(f) as fh:
			raw = fh.read()
		assert '#note' not in raw, "comment key was written to file"
		assert '#_defaults_#' in raw, "defaults line missing after rewrite"
	finally:
		os.path.exists(f) and os.remove(f)


def test_mapto_skips_comments_keeps_defaults():
	f = _tmp()
	try:
		t = TSVZ.TSVZed(f, header='id\tname\tval', rewrite_on_load=False, defaults='#_defaults_#\t\tNA')
		t['a'] = ['a', 'alice', '1']
		t['b'] = ['b', 'bob', '2']
		t['#note'] = ['#note', 'x', 'y']
		t.dirty = True
		t.mapToFile()
		t.close()
		with open(f) as fh:
			raw = fh.read()
		assert '#note' not in raw
		assert '#_defaults_#' in raw
		# data still readable + intact after the in-place rewrite
		t2 = TSVZ.TSVZed(f, header='id\tname\tval', rewrite_on_load=False)
		assert t2['a'] == ['a', 'alice', '1']
		assert t2['b'] == ['b', 'bob', '2']
		t2.close()
	finally:
		os.path.exists(f) and os.remove(f)


# --------------------------------------------------------------------------
# Issue 7: appendLinesTabularFile must not mutate the caller's lists
# --------------------------------------------------------------------------
def test_append_does_not_mutate_input():
	f = _tmp()
	try:
		rows = [['a', 1, 2], ['b', 3, 4]]  # non-str values on purpose
		snapshot = [list(r) for r in rows]
		TSVZ.appendLinesTabularFile(f, rows, header='id\tx\ty', createIfNotExist=True)
		assert rows == snapshot, f"caller list mutated: {rows!r}"
	finally:
		os.path.exists(f) and os.remove(f)


# --------------------------------------------------------------------------
# Issue 8 / format spec: sanitize <-> unsanitize round-trips per definition
# --------------------------------------------------------------------------
def test_sanitize_roundtrip():
	d = '\t'
	# tab char and newline survive a round trip
	for value in ['plain', 'has\ttab', 'has\nlf', 'ends with spaces   ',
				  'literal<sep>here', 'literal<LF>here']:
		san = TSVZ._sanitize(value, delimiter=d)
		back = TSVZ._unsanitize(san, delimiter=d)
		# trailing whitespace is intentionally stripped by the format
		assert back == value.rstrip(), f"{value!r} -> {san!r} -> {back!r}"
	# reserved tokens in file map back to the bare token form
	assert TSVZ._unsanitize('</sep/>', delimiter=d) == '<sep>'
	assert TSVZ._unsanitize('</LF/>', delimiter=d) == '<LF>'


# --------------------------------------------------------------------------
# Issue 12: TSVZedLite with external indexes must create a missing file
# --------------------------------------------------------------------------
def test_tsvzedlite_external_indexes_creates_file():
	f = _tmp()
	try:
		# pass an explicit (empty) index dict so load() is skipped
		lite = TSVZ.TSVZedLite(f, header='id\tname\tval', createIfNotExist=True, indexes={})
		assert os.path.isfile(f)
		lite['a'] = ['a', 'alice', '1']
		assert lite['a'] == ['a', 'alice', '1']
		lite.close()
	finally:
		os.path.exists(f) and os.remove(f)


# --------------------------------------------------------------------------
# Sanity: a normal write/read cycle still works (no regressions)
# --------------------------------------------------------------------------
def test_basic_roundtrip():
	f = _tmp()
	try:
		t = TSVZ.TSVZed(f, header='id\tname\tval')
		t['a'] = ['a', 'alice', '1']
		t['b'] = 'b\tbob\t2'
		t.close()
		t2 = TSVZ.TSVZed(f, header='id\tname\tval')
		assert t2['a'] == ['a', 'alice', '1']
		assert t2['b'] == ['b', 'bob', '2']
		t2.close()
	finally:
		os.path.exists(f) and os.remove(f)


def test_version_is_4_1():
	assert TSVZ.version == '4.1'
	assert TSVZ.__version__ == '4.1'
	assert not hasattr(TSVZ, 'WalStore')


# ==========================================================================
# Shared helpers for the 4.1 tests
# ==========================================================================
import contextlib
import gzip
import io
import random
import subprocess
from collections import OrderedDict

import pytest

import TSVZ_old

HERE = os.path.dirname(os.path.abspath(__file__))
LEGACY_HEADER = ['id', 'c1', 'c2']
LEGACY_VALUES = ['a', 'b c', 'x<y', '#hash', '<sep>', '</sep/>', '<LF>', 'multi\nline',
				 'trail  ', ' lead', 'é ü 中', 'tab\there', 'comma,here', 'pipe|here', '']


def _touch(path, content=b''):
	with open(path, 'wb') as f:
		f.write(content)


def _tsvz_warnings(capsys):
	return [line for line in capsys.readouterr().err.splitlines() if line.startswith('TSVZ warning:')]


def _content(path):
	with open(path, 'rb') as f:
		data = f.read()
	return gzip.decompress(data) if path.endswith('.gz') else data


def _twin_paths(base, suffix):
	old_dir, new_dir = base / 'old', base / 'new'
	old_dir.mkdir(parents=True, exist_ok=True)
	new_dir.mkdir(parents=True, exist_ok=True)
	return str(old_dir / ('data' + suffix)), str(new_dir / ('data' + suffix))


def _legacy_ops(seed, count=40, hash_keys=True):
	rng = random.Random(seed)
	keys = ['k%d' % i for i in range(6)] + (['#memo'] if hash_keys else [])
	ops = []
	for _ in range(count):
		roll = rng.random()
		key = rng.choice(keys)
		if roll < 0.55:
			cells = [rng.choice(LEGACY_VALUES) for _ in range(rng.randint(1, 3))]
			cells[0] = cells[0] or 'v'  # never all-empty: that case is fix L2
			ops.append(('set', key, [key] + cells))
		elif roll < 0.75:
			ops.append(('del', key))
		elif roll < 0.85:
			ops.append(('lone', key))
		elif roll < 0.92:
			ops.append(('defaults', ['#_defaults_#', '', rng.choice(['NA', 'x y'])]))
		else:
			ops.append(('reopen',))
	return ops


def _apply_op(store, op):
	if op[0] == 'set':
		store[op[1]] = list(op[2])
	elif op[0] == 'del':
		del store[op[1]]
	elif op[0] == 'lone':
		store[op[1]] = op[1]
	elif op[0] == 'defaults':
		store['#_defaults_#'] = list(op[1])


# ==========================================================================
# Legacy dialect: differential tests against the frozen 3.39 module
# ==========================================================================
LEGACY_SUFFIXES = ['.tsv', '.csv', '.nsv', '.psv', '.tsv.gz']
SUFFIX_DELIMITERS = {'.tsv': '\t', '.csv': ',', '.nsv': '\x00', '.psv': '|'}


# 3.39's background append worker races the test's own writes, so these lines
# appear or vanish with thread timing. They are dropped before comparison; every
# other stdout line is still compared.
_RACY_STDOUT_PREFIXES = ('External changes detected in ', 'Time anomalies detected in ',
						 'Warning: Overwriting external changes in ')


def _observe(fn, twin_dir):
	"""Run fn with stdout captured; return (outcome, stdout) with the twin dir masked."""
	buf = io.StringIO()
	try:
		with contextlib.redirect_stdout(buf):
			outcome = ('ok', fn())
	except Exception as e:
		outcome = ('raise', type(e).__name__)
	text = buf.getvalue().replace(twin_dir, '<dir>')
	return outcome, ''.join(line for line in text.splitlines(True)
							if not line.startswith(_RACY_STDOUT_PREFIXES))


def _tsvzed_snapshot(mod, path):
	t = mod.TSVZed(path, header=LEGACY_HEADER, rewrite_on_load=False)
	snap = dict(t)
	t.close()
	return snap


def _lite_snapshot(mod, path):
	lite = mod.TSVZedLite(path, header=LEGACY_HEADER, strict=False)
	snap = {k: lite[k] for k in list(lite.indexes)}
	lite.close()
	return snap


def _legacy_read_observations(mod, path, delimiter, twin_dir, compressed):
	obs = []
	for header in (LEGACY_HEADER, ''):
		for strict in (True, False):
			for verify in (True, False):
				obs.append(_observe(lambda: mod.readTabularFile(
					path, header=header, strict=strict, verifyHeader=verify), twin_dir))
	if not compressed:
		obs.append(_observe(lambda: mod.readTabularFile(
			path, header=LEGACY_HEADER, storeOffset=True), twin_dir))
	obs.append(_observe(lambda: mod.read_last_valid_line(path, {}, -1, delimiter=delimiter), twin_dir))
	obs.append(_observe(lambda: _tsvzed_snapshot(mod, path), twin_dir))
	if not compressed:
		obs.append(_observe(lambda: _lite_snapshot(mod, path), twin_dir))
	return obs


def _legacy_corpora(d):
	"""Hand-written legacy files (bytes) using delimiter d."""
	def line(*cells):
		return d.join(cells).encode('utf-8')
	main = [
		line('id', 'c1', 'c2'),
		line('a', 'b c', 'x'),
		line('dup', 'one', 'two'),
		line('dup'),                       # lone key: delete
		line('dup', 'three', 'four'),      # re-set after the delete
		line('k2', 'z', 'w'),
		line('k3', '', ''),                # key followed by empty cells
		line('wide', 'a', 'b', 'c', 'd'),  # wider than header
		line('narrow', 'a'),               # narrower than header
		b'',
		b'   ',
		line('crlf', 'x', 'y') + b'\r',
		b'# a comment',
		line('#note', 'q', 'r'),
		line('#_defaults_#', '', 'NA'),
		line('#_defaults_#'),              # empty defaults line
		line('ts', 'trail  ', 'x '),
		line('tok', '</sep/>', '<sep>'),
		line('lf', '<LF>', 'a'),
		line('late', 'v', 'w'),            # last line, no trailing newline
	]
	return {
		'main': b'\n'.join(main),
		'badheader': b'\n'.join([line('ID', 'X', 'Y'), line('a', 'b', 'c'), line('b', 'c', 'd')]) + b'\n',
		'utf8': b'\n'.join([line('id', 'c1', 'c2'), line('u', 'ok', 'fine'),
		                    b'bad' + d.encode('utf-8') + b'\xff' + d.encode('utf-8') + b'z']) + b'\n',
		'nul': b'\n'.join([line('id', 'c1', 'c2'), line('nul', 'v', 'w') + b'\x00', line('n2', 'p', 'q')]) + b'\n',
	}




def _drive_tsvzed(mod, path, ops):
	seen = []
	kwargs = dict(header=LEGACY_HEADER, append_check_delay=0.001)
	t = mod.TSVZed(path, **kwargs)
	for op in ops:
		if op[0] == 'reopen':
			t.close()
			seen.append(dict(t))
			t = mod.TSVZed(path, **kwargs)
			seen.append(dict(t))
		else:
			_apply_op(t, op)
	t.close()
	seen.append(dict(t))
	t = mod.TSVZed(path, **kwargs)
	seen.append(dict(t))
	t.close()
	return seen


def _drive_lite(mod, path, ops):
	seen = []
	kwargs = dict(header=LEGACY_HEADER, strict=False)
	lite = mod.TSVZedLite(path, **kwargs)
	for op in ops:
		if op[0] == 'reopen':
			lite.close()
			lite = mod.TSVZedLite(path, **kwargs)
			seen.append({key: lite[key] for key in list(lite.indexes)})
		else:
			_apply_op(lite, op)
	seen.append({key: lite[key] for key in list(lite.indexes)})
	lite.close()
	return seen


def _drive_stateless(mod, path, seed):
	rng = random.Random(seed)
	out = []
	delimiter = mod.get_delimiter(..., path)
	for _ in range(6):
		rows = []
		for _ in range(rng.randint(1, 4)):
			key = 'k%d' % rng.randint(0, 5)
			rows.append([key] + [rng.choice(LEGACY_VALUES) or 'v' for _ in range(rng.randint(0, 3))])
		mod.appendLinesTabularFile(path, rows, header=LEGACY_HEADER, createIfNotExist=True)
		out.append(dict(mod.readTabularFile(path, header=LEGACY_HEADER, strict=False)))
		out.append(dict(mod.readTabularFile(path, header=LEGACY_HEADER, strict=True)))
		out.append(mod.read_last_valid_line(path, {}, -1, delimiter=delimiter))
	out.append(dict(mod.scrubTabularFile(path, header=LEGACY_HEADER)))
	out.append(_content(path))
	mod.clearTabularFile(path, header=LEGACY_HEADER)
	out.append(_content(path))
	return out


def test_legacy_tsvzed_matches_339(tmp_path):
	for suffix in LEGACY_SUFFIXES:
		for seed in range(4):
			old_path, new_path = _twin_paths(tmp_path / ('%s-%d' % (suffix.replace('.', ''), seed)), suffix)
			ops = _legacy_ops(seed)
			assert _observe(lambda: _drive_tsvzed(TSVZ, new_path, ops), os.path.dirname(new_path)) == \
				_observe(lambda: _drive_tsvzed(TSVZ_old, old_path, ops), os.path.dirname(old_path)), (suffix, seed)
			assert _content(new_path) == _content(old_path), (suffix, seed)


def test_legacy_tsvzedlite_matches_339(tmp_path):
	for suffix in ['.tsv', '.csv', '.nsv', '.psv']:
		for seed in range(4):
			old_path, new_path = _twin_paths(tmp_path / ('%s-%d' % (suffix.replace('.', ''), seed)), suffix)
			ops = _legacy_ops(seed, hash_keys=False)
			assert _observe(lambda: _drive_lite(TSVZ, new_path, ops), os.path.dirname(new_path)) == \
				_observe(lambda: _drive_lite(TSVZ_old, old_path, ops), os.path.dirname(old_path)), (suffix, seed)
			assert _content(new_path) == _content(old_path), (suffix, seed)


def test_legacy_stateless_helpers_match_339(tmp_path):
	for suffix in LEGACY_SUFFIXES:
		for seed in range(3):
			old_path, new_path = _twin_paths(tmp_path / ('%s-%d' % (suffix.replace('.', ''), seed)), suffix)
			assert _observe(lambda: _drive_stateless(TSVZ, new_path, seed), os.path.dirname(new_path)) == \
				_observe(lambda: _drive_stateless(TSVZ_old, old_path, seed), os.path.dirname(old_path)), (suffix, seed)

def test_legacy_read_corpus_matches_339(tmp_path):
	for suffix, d in SUFFIX_DELIMITERS.items():
		for name, content in _legacy_corpora(d).items():
			old_path, new_path = _twin_paths(tmp_path / ('%s-%s' % (name, suffix.replace('.', ''))), suffix)
			_touch(old_path, content)
			_touch(new_path, content)
			old_obs = _legacy_read_observations(TSVZ_old, old_path, d, os.path.dirname(old_path), False)
			new_obs = _legacy_read_observations(TSVZ, new_path, d, os.path.dirname(new_path), False)
			assert new_obs == old_obs, (name, suffix)
			assert _content(new_path) == _content(old_path), (name, suffix)
	# gzip-compressed intact copy of the main corpus file (tab-delimited)
	main = _legacy_corpora('\t')['main']
	old_path, new_path = _twin_paths(tmp_path / 'main-gz', '.tsv.gz')
	_touch(old_path, gzip.compress(main))
	_touch(new_path, gzip.compress(main))
	old_obs = _legacy_read_observations(TSVZ_old, old_path, '\t', os.path.dirname(old_path), True)
	new_obs = _legacy_read_observations(TSVZ, new_path, '\t', os.path.dirname(new_path), True)
	assert new_obs == old_obs
	assert _content(new_path) == _content(old_path)


# ==========================================================================
# Infrastructure
# ==========================================================================
def test_parse_part_name():
	P = TSVZ._parsePartName
	assert P('/a/b.c/data.tsvz.1a2b.rotated.gz') == ('/a/b.c/data.tsvz', 'tsvz', 0x1a2b, True, 'gz')
	assert P('data.tsvz') == ('data.tsvz', 'tsvz', None, False, '')
	assert P('data.TSVZ.GZ') == ('data.TSVZ', 'tsvz', None, False, 'gz')
	assert P('data.txt.gz') == ('data.txt', '', None, False, 'gz')
	assert P('data.add') == ('data.add', '', None, False, '')
	assert P('/x.tsv/data') == ('/x.tsv/data', '', None, False, '')
	assert P('tsv') == ('tsv', '', None, False, '')
	assert P('data.tsv.1') == ('data.tsv', 'tsv', 1, False, '')
	assert P('data.tsvz.f.xz').ordinal == 15


def test_is_spec_path():
	assert TSVZ._isSpecPath('a.tsvz') and TSVZ._isSpecPath('a.csvz.10.zst') and TSVZ._isSpecPath('A.NSVZ')
	assert not TSVZ._isSpecPath('a.tsv') and not TSVZ._isSpecPath('a.csv.gz') and not TSVZ._isSpecPath('a.txt')


def test_get_delimiter_strict_extensions():
	assert TSVZ.get_delimiter(..., 'a.tsvz') == '\t'
	assert TSVZ.get_delimiter(..., 'a.csvz') == ','
	assert TSVZ.get_delimiter(..., 'a.nsvz.gz') == '\0'
	assert TSVZ.get_delimiter(..., 'a.psvz.1f') == '|'
	assert TSVZ.get_delimiter(..., 'a.csv') == ','


def test_reporter_summarises_once_per_kind(capsys):
	reporter = TSVZ._Reporter('f.tsvz')
	reporter.note('utf8', 'line 3', 'invalid UTF-8 replaced with U+FFFD')
	reporter.note('utf8', 'line 9', 'invalid UTF-8 replaced with U+FFFD')
	reporter.note('tail', None, "ignored uncommitted bytes after the last newline: b'x'")
	reporter.flush()
	reporter.flush()
	assert _tsvz_warnings(capsys) == [
		'TSVZ warning: f.tsvz: invalid UTF-8 replaced with U+FFFD (2 occurrences, first at line 3)',
		"TSVZ warning: f.tsvz: ignored uncommitted bytes after the last newline: b'x'",
	]


def test_reporter_uses_tee_logger(capsys):
	class Logger(object):
		def __init__(self):
			self.calls = []

		def teelog(self, message, level):
			self.calls.append((message, level))
	log = Logger()
	reporter = TSVZ._Reporter('f.tsvz', log)
	reporter.note('x', 'line 1', 'detail')
	reporter.flush()
	assert log.calls == [('TSVZ warning: f.tsvz: detail (line 1)', 'warning')]
	assert capsys.readouterr().err == ''


def test_warn_once_per_owner(capsys):
	class Owner(object):
		pass
	owner = Owner()
	TSVZ._warnOnce(owner, 'k', 'p.tsv', 'said once')
	TSVZ._warnOnce(owner, 'k', 'p.tsv', 'said once')
	assert _tsvz_warnings(capsys) == ['TSVZ warning: p.tsv: said once']


def test_locked_append_newline_policy(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv')
	_touch(p, b'a\t1')
	reporter = TSVZ._Reporter(p)
	TSVZ._lockedAppend(p, b'b\t2\n', 'newline', reporter)
	reporter.flush()
	assert open(p, 'rb').read() == b'a\t1\nb\t2\n'
	assert _tsvz_warnings(capsys) == ['TSVZ warning: %s: last line had no trailing newline; added one before appending' % p]


def test_locked_append_truncate_policy(tmp_path, capsys):
	p = str(tmp_path / 'a.tsvz')
	_touch(p, b'a\t1\nbo')
	reporter = TSVZ._Reporter(p)
	TSVZ._lockedAppend(p, b'c\t3\n', 'truncate', reporter)
	reporter.flush()
	assert open(p, 'rb').read() == b'a\t1\nc\t3\n'
	assert _tsvz_warnings(capsys) == ["TSVZ warning: %s: removed uncommitted tail b'bo' before appending" % p]


def test_locked_append_whole_file_uncommitted_and_missing_file(tmp_path):
	p = str(tmp_path / 'a.tsvz')
	_touch(p, b'no newline at all')
	TSVZ._lockedAppend(p, b'c\t3\n', 'truncate', TSVZ._Reporter(p))
	assert open(p, 'rb').read() == b'c\t3\n'
	q = str(tmp_path / 'new.tsvz')
	TSVZ._lockedAppend(q, b'd\t4\n', 'truncate', TSVZ._Reporter(q))
	assert open(q, 'rb').read() == b'd\t4\n'


def test_last_newline_end_spans_blocks(tmp_path):
	p = str(tmp_path / 'big')
	_touch(p, b'x\n' + b'y' * 200000)
	with open(p, 'rb') as f:
		assert TSVZ._lastNewlineEnd(f, os.path.getsize(p)) == 2
	_touch(p, b'y' * 70000)
	with open(p, 'rb') as f:
		assert TSVZ._lastNewlineEnd(f, os.path.getsize(p)) == 0


def test_compress_bytes_roundtrip():
	import bz2
	import lzma
	assert gzip.decompress(TSVZ._compressBytes('gz', b'abc\n')) == b'abc\n'
	assert bz2.decompress(TSVZ._compressBytes('bz2', b'abc\n')) == b'abc\n'
	assert lzma.decompress(TSVZ._compressBytes('xz', b'abc\n')) == b'abc\n'


def test_path_lock_is_shared_per_real_path(tmp_path):
	p = str(tmp_path / 'a.tsvz')
	assert TSVZ._pathLock(p) is TSVZ._pathLock(os.path.join(str(tmp_path), '.', 'a.tsvz'))


# ==========================================================================
# Legacy fixes L1 and L6
# ==========================================================================
def test_l1_stateless_append_adds_missing_newline(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv')
	_touch(p, b'id\tv\nk1\tx')
	TSVZ.appendTabularFile(p, ['k2', 'y'], header='id\tv')
	assert open(p, 'rb').read() == b'id\tv\nk1\tx\nk2\ty\n'
	assert dict(TSVZ.readTabularFile(p, header='id\tv')) == {'k1': ['k1', 'x'], 'k2': ['k2', 'y']}
	assert _tsvz_warnings(capsys) == ['TSVZ warning: %s: last line had no trailing newline; added one before appending' % p]


def test_l1_tsvzed_append_adds_missing_newline(tmp_path):
	p = str(tmp_path / 'a.tsv')
	_touch(p, b'id\tv\nk1\tx')
	t = TSVZ.TSVZed(p, header='id\tv', rewrite_on_load=False)
	t['k2'] = ['k2', 'y']
	t.close()
	assert open(p, 'rb').read() == b'id\tv\nk1\tx\nk2\ty\n'


def test_l1_tsvzedlite_append_adds_missing_newline(tmp_path):
	p = str(tmp_path / 'a.tsv')
	_touch(p, b'id\tv\nk1\tx')
	lite = TSVZ.TSVZedLite(p, header='id\tv', strict=False)
	lite['k2'] = ['k2', 'y']
	assert lite['k2'] == ['k2', 'y'] and lite['k1'] == ['k1', 'x']
	lite.close()
	assert open(p, 'rb').read() == b'id\tv\nk1\tx\nk2\ty\n'


def test_l6_map_to_file_keeps_newline_at_end_of_file(tmp_path):
	p = str(tmp_path / 'a.tsv')
	_touch(p, b'id\ta\tb\nk1\txx\tyy\n')
	t = TSVZ.TSVZed(p, header='id\ta\tb', rewrite_on_load=False)
	t.memoryOnly = True  # keep these rows out of the append queue
	t['k1'] = ['k1', 'x2', 'y2']  # same length: rewritten in place
	t['k2'] = ['k2', 'p', 'q']    # beyond the end of the file
	t['k3'] = ['k3', 'r', 's']
	t.memoryOnly = False
	t.mapToFile()
	t.close()
	assert open(p, 'rb').read() == b'id\ta\tb\nk1\tx2\ty2\nk2\tp\tq\nk3\tr\ts\n'


# ==========================================================================
# Legacy fixes L4 and L5
# ==========================================================================
def test_l4_invalid_utf8_is_reported_once(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv')
	_touch(p, b'id\tv\nk1\t\xff\nk2\t\xfe\n')
	data = TSVZ.readTabularFile(p, header='id\tv')
	assert data['k1'] == ['k1', '�']
	assert _tsvz_warnings(capsys) == ['TSVZ warning: %s: invalid utf8 replaced with U+FFFD (2 occurrences, first at line 2)' % p]


def test_l4_strict_drops_are_reported(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv')
	_touch(p, b'id\tv\nk1\tx\nk2\ty\tEXTRA\n')
	data = TSVZ.readTabularFile(p, header='id\tv', strict=True)
	assert list(data) == ['k1']
	assert _tsvz_warnings(capsys) == ['TSVZ warning: %s: dropped a line whose column count is not 2 (strict mode)' % p]


def test_l5_damaged_gzip_reads_up_to_the_damage(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv.gz')
	_touch(p, gzip.compress(b'id\tv\nk1\tx\n') + gzip.compress(b'k2\ty\n')[:-6])
	data = TSVZ.readTabularFile(p, header='id\tv')
	assert data['k1'] == ['k1', 'x']
	warnings = _tsvz_warnings(capsys)
	assert len(warnings) == 1 and 'compressed stream is damaged' in warnings[0]


def test_l5_corrupt_uncompressed_errors_still_raise(tmp_path):
	with pytest.raises(FileNotFoundError):
		TSVZ.readTabularFile(str(tmp_path / 'missing.tsv'))


def test_l5_damaged_header_line_reads_up_to_the_damage(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv.gz')
	_touch(p, gzip.compress(b'id\tv\nk1\tx\n')[:12])
	data = TSVZ.readTabularFile(p, header='id\tv')
	assert data == {}
	warnings = _tsvz_warnings(capsys)
	assert len(warnings) == 1 and 'compressed stream is damaged' in warnings[0]


def test_l4_invalid_utf8_in_header_is_reported_and_matches_339(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv')
	_touch(p, b'id\tv\xff\nk1\tx\n')
	data = TSVZ.readTabularFile(p, header='id\tv')
	assert data == TSVZ_old.readTabularFile(p, header='id\tv')
	assert _tsvz_warnings(capsys) == ['TSVZ warning: %s: invalid utf8 replaced with U+FFFD (line 1)' % p]


if __name__ == '__main__':
	sys.exit(pytest.main([__file__] + sys.argv[1:]))
