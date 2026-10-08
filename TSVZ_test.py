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
import gzip
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
			assert _drive_tsvzed(TSVZ, new_path, ops) == _drive_tsvzed(TSVZ_old, old_path, ops), (suffix, seed)
			assert _content(new_path) == _content(old_path), (suffix, seed)


def test_legacy_tsvzedlite_matches_339(tmp_path):
	for suffix in ['.tsv', '.csv', '.nsv', '.psv']:
		for seed in range(4):
			old_path, new_path = _twin_paths(tmp_path / ('%s-%d' % (suffix.replace('.', ''), seed)), suffix)
			ops = _legacy_ops(seed, hash_keys=False)
			assert _drive_lite(TSVZ, new_path, ops) == _drive_lite(TSVZ_old, old_path, ops), (suffix, seed)
			assert _content(new_path) == _content(old_path), (suffix, seed)


def test_legacy_stateless_helpers_match_339(tmp_path):
	for suffix in LEGACY_SUFFIXES:
		for seed in range(3):
			old_path, new_path = _twin_paths(tmp_path / ('%s-%d' % (suffix.replace('.', ''), seed)), suffix)
			assert _drive_stateless(TSVZ, new_path, seed) == _drive_stateless(TSVZ_old, old_path, seed), (suffix, seed)

if __name__ == '__main__':
	funcs = [(n, f) for n, f in sorted(globals().items())
			 if n.startswith('test_') and callable(f)]
	failed = 0
	for name, fn in funcs:
		try:
			fn()
			print(f"PASS {name}")
		except Exception as e:
			failed += 1
			import traceback
			print(f"FAIL {name}: {type(e).__name__}: {e}")
			traceback.print_exc()
	print(f"\n{len(funcs) - failed}/{len(funcs)} passed")
	sys.exit(1 if failed else 0)
