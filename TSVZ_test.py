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


# ==========================================================================
# Legacy fixes L2 and L3
# ==========================================================================
def test_l2_tsvzed_all_empty_row_is_a_delete(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv')
	t = TSVZ.TSVZed(p, header='id\ta\tb', rewrite_on_load=False)
	t['k'] = ['k', 'x', 'y']
	t['k'] = ['k', '', '']
	assert 'k' not in t
	t['j'] = ['j', '', '']
	assert 'j' not in t
	t['#scratch'] = ['#scratch', '', '']  # '#' keys stay memory-only, untouched by L2
	assert '#scratch' in t
	t.close()
	assert 'k' not in TSVZ.readTabularFile(p, header='id\ta\tb')
	warnings = _tsvz_warnings(capsys)
	assert len(warnings) == 1 and 'values are all empty is a delete' in warnings[0]


def test_l2_tsvzedlite_all_empty_row_is_a_delete(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv')
	lite = TSVZ.TSVZedLite(p, header='id\ta\tb', strict=False)
	lite['k'] = ['k', 'x', 'y']
	lite['k'] = ['k', '', '']
	assert 'k' not in lite.indexes
	lite.close()
	assert 'k' not in TSVZ.readTabularFile(p, header='id\ta\tb')
	assert len(_tsvz_warnings(capsys)) == 1


def test_l3_tsvzedlite_on_gzip_keeps_rows_in_memory(tmp_path, capsys):
	p = str(tmp_path / 'a.tsv.gz')
	TSVZ.appendLinesTabularFile(p, [['k1', 'x']], header='id\tv', createIfNotExist=True)
	lite = TSVZ.TSVZedLite(p, header='id\tv', strict=False)
	assert lite['k1'] == ['k1', 'x']
	lite['k2'] = ['k2', 'y']
	assert lite['k2'] == ['k2', 'y']
	del lite['k1']
	repr(lite)
	lite.close()
	assert dict(TSVZ.readTabularFile(p, header='id\tv')) == {'k2': ['k2', 'y']}
	assert any('keeps rows in memory' in w for w in _tsvz_warnings(capsys))


def test_l3_tsvzedlite_gzip_clear(tmp_path):
	p = str(tmp_path / 'a.tsv.gz')
	TSVZ.appendLinesTabularFile(p, [['k1', 'x']], header='id\tv', createIfNotExist=True)
	lite = TSVZ.TSVZedLite(p, header='id\tv', strict=False)
	lite.clear()
	lite.close()
	assert gzip.decompress(open(p, 'rb').read()) == b'id\tv\n'


def test_l3_tsvzedlite_hash_keys_are_readable(tmp_path):
	p = str(tmp_path / 'a.tsv')
	lite = TSVZ.TSVZedLite(p, header='id\tv', strict=False)
	lite['#memo'] = ['#memo', 'ram only']
	assert lite['#memo'] == ['#memo', 'ram only']
	assert lite.pop('#memo') == ['#memo', 'ram only']
	lite['#memo'] = ['#memo', 'again']
	assert lite.popitem() == ('#memo', ['#memo', 'again'])
	lite.close()


# ==========================================================================
# Spec dialect: codec, reader state, record pipeline (spec §7–§14)
# ==========================================================================
def test_spec_field_codec_roundtrip_every_variant():
	for d in ('\t', ',', '\0', '|'):
		for value in ['a' + d + 'b', '<sep>', '<LF>', 'a<b', '#x', 'x\ny', '<lt>', '<#>', '', 'é<中>']:
			assert TSVZ._specDecodeField(TSVZ._specEncodeField(value, d), d) == value, (d, value)
			assert TSVZ._specDecodeField(TSVZ._specEncodeField(value, d, isKey=True), d) == value, (d, value)


def test_spec_13_5_examples():
	E, D = TSVZ._specEncodeField, TSVZ._specDecodeField
	assert E('a\tb', '\t') == 'a<sep>b'
	assert E('<sep>', '\t') == '<lt>sep>'
	assert E('<LF>', '\t') == '<lt>LF>'
	assert E('#foo', '\t', isKey=True) == '<#>foo'
	assert E('#foo', '\t') == '#foo'
	assert E('a<b', '\t') == 'a<lt>b'
	assert D('<future>', '\t') == '<future>'
	assert D('</sep/>', '\t') == '</sep/>'


def test_spec_format_record():
	F = TSVZ._specFormatRecord
	assert F(['#k', 'v'], '\t') == '<#>k\tv'
	assert F(['k'], '\t') == 'k'
	assert F(['#_defaults_#', 'a<b', ''], '\t', marker=True) == '#_defaults_#\ta<lt>b\t'
	assert F(['#_defaults_#', 'v'], '\t') == '<#>_defaults_#\tv'


def _process(lines, d='\t', state=None):
	state = state or TSVZ._SpecState()
	reporter = TSVZ._Reporter('t')
	return [TSVZ._specProcessRecord(text, state, d, reporter, None) for text in lines], state, reporter


def test_spec_tombstone_only_without_delimiter():
	out, _, _ = _process(['k', 'k\t', 'k\t\t', '\tv', '', '#c', ' #notcomment\tv', '<#>h\tv'])
	assert out == [('k', None), ('k', ['k', '']), ('k', ['k', '', '']), None, None, None,
				   (' #notcomment', [' #notcomment', 'v']), ('#h', ['#h', 'v'])]


def test_spec_strip_trailing_whites_marker():
	out, _, _ = _process(['k \tv \t', '#_strip_trailing_whites_#\tFALSE', 'k \tv ', '#_STRIP_TRAILING_WHITES_#', 'j\tv '])
	assert out[0] == ('k', ['k', 'v', ''])
	assert out[2] == ('k ', ['k ', 'v '])
	assert out[4] == ('j', ['j', 'v'])


def test_spec_fill_empty_and_defaults_binding():
	out, state, _ = _process(['#_defaults_#\tg\t0', 'a\t\t5', '#_fill_empty_with_default_#\ttrue', 'b\t\t5', '#_defaults_#', 'c\t\t5'])
	assert out[1] == ('a', ['a', '', '5'])
	assert out[3] == ('b', ['b', 'g', '5'])
	assert out[5] == ('c', ['c', '', '5'])
	assert state.defaults == ['#_defaults_#']


def test_spec_bad_marker_values_keep_state(capsys):
	_, state, reporter = _process(['#_fill_empty_with_default_#\tmaybe', '#_version_#\tx',
								   '#_return_defaults_when_missing_#\tno', '#_version_#\t3',
								   '#__custom__#\tanything', '#_unknown_#\t1', '#_rotate_#\tRENAME'])
	reporter.flush()
	assert state.fillEmpty is False and state.returnDefaults is False and state.version == 3
	assert state.rotate == 'rename'
	warnings = _tsvz_warnings(capsys)
	assert len(warnings) == 2
	assert 'invalid value' in warnings[0] and '2 occurrences' in warnings[0]
	assert 'declares spec version 3' in warnings[1]


def test_spec_marker_keys_are_case_insensitive_and_values_decoded():
	_, state, _ = _process(['#_DEFAULTS_#\ta<sep>b\t<lt>'])
	assert state.defaults == ['#_defaults_#', 'a\tb', '<']
	assert state.sawDefaults and state.rowState.defaults == ('#_defaults_#', 'a\tb', '<')


def test_spec_state_from_row_state():
	state = TSVZ._SpecState(['#_defaults_#', 'x'])
	state.strip = False
	state.refresh()
	copy = TSVZ._SpecState.fromRowState(state.rowState)
	assert copy.defaults == ['#_defaults_#', 'x'] and copy.strip is False and copy.fillEmpty is False


def test_spec_digests():
	crc = TSVZ._newDigest('crc32')
	crc.update(b'abc')
	assert crc.hexdigest() == '352441c2'
	assert TSVZ._newDigest('sha256') is not None
	assert TSVZ._newDigest('shake_128') is None and TSVZ._newDigest('crc32c') is None and TSVZ._newDigest('blake3') is None
	assert TSVZ._digestSupported('md5') and not TSVZ._digestSupported('nope')


def test_normalize_defaults():
	N = TSVZ._normalizeDefaults
	assert N(None, '\t') == ['#_defaults_#']
	assert N(..., '\t') == ['#_defaults_#']
	assert N('#_defaults_#\tNA\t0', '\t') == ['#_defaults_#', 'NA', '0']
	assert N(['NA', ' 0 '], '\t') == ['#_defaults_#', 'NA', ' 0']
	assert N(['', ''], '\t') == ['#_defaults_#']
	assert N('NA,0', ',') == ['#_defaults_#', 'NA', '0']


def test_spec_version_marker_with_unicode_digits_is_rejected(capsys):
	_, state, reporter = _process(['#_version_#\t²', '#_version_#\t٣'])
	assert state.version == 1
	reporter.flush()
	warnings = _tsvz_warnings(capsys)
	assert len(warnings) == 1
	assert 'invalid value' in warnings[0] and '2 occurrences' in warnings[0]


# ==========================================================================
# Spec dialect: part reader, checksums, multi-part assembly (§4, §15–§17)
# ==========================================================================
def _replay(paths, d='\t'):
	state = TSVZ._SpecState()
	reporter = TSVZ._Reporter('t')
	infos = []
	data = OrderedDict()
	for _, _, _, record in TSVZ._specReplay(paths, d, state, reporter, infos):
		if record is None:
			continue
		key, row = record
		if row is None:
			data.pop(key, None)
		else:
			data[key] = row
	reporter.flush()
	return data, state, infos


def test_spec_commit_rule_crlf_and_bom(tmp_path, capsys):
	p = str(tmp_path / 'a.tsvz')
	_touch(p, b'\xef\xbb\xbfa\t1\r\nb\t2\r\nc\t3')
	data, _, infos = _replay([p])
	assert dict(data) == {'a': ['a', '1'], 'b': ['b', '2']}
	assert infos[0].tail == b'c\t3'
	assert sorted(_tsvz_warnings(capsys)) == sorted([
		'TSVZ warning: t: stripped a UTF-8 byte order mark (line 1)',
		"TSVZ warning: t: ignored uncommitted bytes after the last newline: b'c\\t3'",
	])


def test_spec_invalid_utf8_is_replaced_and_reported(tmp_path, capsys):
	p = str(tmp_path / 'u.tsvz')
	_touch(p, b'a\t\xff\nb\t\xfe\n')
	data, _, _ = _replay([p])
	assert dict(data) == {'a': ['a', '�'], 'b': ['b', '�']}
	assert _tsvz_warnings(capsys) == ['TSVZ warning: t: invalid UTF-8 replaced with U+FFFD (2 occurrences, first at line 1)']


def test_spec_lines_split_across_chunks(tmp_path, monkeypatch):
	monkeypatch.setattr(TSVZ, '_CHUNK', 3)
	p = str(tmp_path / 'a.tsvz')
	_touch(p, 'ключ\tзначение\r\nk2\t中文\n'.encode('utf-8'))
	data, _, _ = _replay([p])
	assert dict(data) == {'ключ': ['ключ', 'значение'], 'k2': ['k2', '中文']}
	g = str(tmp_path / 'b.tsvz.gz')
	_touch(g, gzip.compress('ключ\tзначение\r\n'.encode('utf-8')) + gzip.compress(b'k2\tv\n'))
	data, _, infos = _replay([g])
	assert dict(data) == {'ключ': ['ключ', 'значение'], 'k2': ['k2', 'v']}
	assert not infos[0].damaged


def test_spec_gzip_damage(tmp_path, capsys):
	g = str(tmp_path / 'g.tsvz.gz')
	_touch(g, gzip.compress(b'a\t1\n') + b'\0\0\0' + gzip.compress(b'b\t2\n') + gzip.compress(b'c\t3\n')[:-6])
	data, _, infos = _replay([g])
	assert dict(data) == {'a': ['a', '1'], 'b': ['b', '2'], 'c': ['c', '3']}
	assert infos[0].damaged == 'truncated'
	_touch(g, gzip.compress(b'a\t1\n') + b'garbage!')
	data, _, infos = _replay([g])
	assert dict(data) == {'a': ['a', '1']} and infos[0].damaged.startswith('corrupt')
	assert len(_tsvz_warnings(capsys)) == 2


def test_spec_bz2_and_xz_parts(tmp_path):
	import bz2
	import lzma
	b = str(tmp_path / 'a.tsvz.bz2')
	x = str(tmp_path / 'b.tsvz.xz')
	_touch(b, bz2.compress(b'a\t1\n') + bz2.compress(b'b\t2\n'))
	_touch(x, lzma.compress(b'a\t1\n') + lzma.compress(b'b\t2\n'))
	assert dict(_replay([b])[0]) == dict(_replay([x])[0]) == {'a': ['a', '1'], 'b': ['b', '2']}


def test_spec_zst_without_support_is_skipped(tmp_path):
	try:
		from compression import zstd  # noqa: F401
		pytest.skip('compression.zstd is available')
	except ImportError:
		pass
	z = str(tmp_path / 'a.tsvz.zst')
	_touch(z, b'\x28\xb5\x2f\xfdjunk')
	data, _, infos = _replay([z])
	assert dict(data) == {} and infos[0].damaged.startswith('cannot decompress')


def test_spec_checksums_arm_verify_and_span_parts(tmp_path, capsys):
	import zlib
	base = str(tmp_path / 'm.tsvz')
	body = b'b\t2\n'
	crc = '%08x' % (zlib.crc32(b'a\t1\r\n' + body) & 0xffffffff)
	_touch(base, b'#_checksum_crc32_#\tIGNORED\na\t1\r\n')
	_touch(base + '.f', body + ('#_checksum_crc32_#\t' + crc.upper() + '\n').encode()
		   + b'#_checksum_crc32_#\tdeadbeef\n#_checksum_nope_#\t1\n')
	data, _, _ = _replay([base, base + '.f'])
	assert dict(data) == {'a': ['a', '1'], 'b': ['b', '2']}
	warnings = _tsvz_warnings(capsys)
	assert len(warnings) == 1
	assert 'mismatch: expected deadbeef, computed 00000000' in warnings[0] and 'm.tsvz.f line 3' in warnings[0]


def test_spec_checksum_covers_other_algorithms_markers(tmp_path, capsys):
	import zlib
	p = str(tmp_path / 'a.tsvz')
	sha_line = b'#_checksum_sha256_#\n'
	crc = '%08x' % (zlib.crc32(sha_line + b'a\t1\n') & 0xffffffff)
	_touch(p, b'#_checksum_crc32_#\n' + sha_line + b'a\t1\n' + ('#_checksum_crc32_#\t%s\n' % crc).encode())
	_replay([p])
	assert _tsvz_warnings(capsys) == []


def test_store_parts_order_and_exclusions(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	for name in ['m.tsvz', 'm.tsvz.10', 'm.tsvz.f', 'm.tsvz.0', 'm.tsvz.11.rotated', 'm.tsvz.12.rotated.gz',
				 'm.tsvz.zz', 'other.tsvz.1', 'm.tsv.2']:
		_touch(str(tmp_path / name))
	reporter = TSVZ._Reporter(base)
	parts, active = TSVZ._storeParts(base, reporter)
	assert [os.path.basename(p) for p in parts] == ['m.tsvz', 'm.tsvz.0', 'm.tsvz.f', 'm.tsvz.10']
	assert os.path.basename(active) == 'm.tsvz.10'
	reporter.flush()
	assert _tsvz_warnings(capsys) == []


def test_store_parts_without_part_zero_and_ties(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base + '.1a')
	_touch(base + '.1A.gz')
	reporter = TSVZ._Reporter(base)
	parts, active = TSVZ._storeParts(base, reporter)
	assert [os.path.basename(p) for p in parts] == ['m.tsvz.1A.gz', 'm.tsvz.1a']
	assert active == parts[-1]
	reporter.flush()
	assert len(_tsvz_warnings(capsys)) == 1


def test_store_parts_ambiguous_part_zero(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base)
	_touch(base + '.gz')
	reporter = TSVZ._Reporter(base)
	assert TSVZ._storeParts(base, reporter) == ([base], base)
	reporter.flush()
	assert len(_tsvz_warnings(capsys)) == 1


def test_store_parts_fragment_and_missing(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base)
	_touch(base + '.1')
	reporter = TSVZ._Reporter(base + '.1')
	assert TSVZ._storeParts(base + '.1', reporter) == ([base + '.1'], base + '.1')
	reporter.flush()
	assert 'one part of a multi-part store' in _tsvz_warnings(capsys)[0]
	missing = str(tmp_path / 'none.tsvz')
	assert TSVZ._storeParts(missing) == ([], missing)


def test_spec_unreadable_part_is_skipped(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'a\t1\n')
	os.mkdir(base + '.1')  # a directory named like a part cannot be read
	parts, _ = TSVZ._storeParts(base)
	data, _, _ = _replay(parts)
	assert dict(data) == {'a': ['a', '1']}
	assert 'skipped unreadable part' in _tsvz_warnings(capsys)[0]


# ==========================================================================
# Spec dialect: loading a store through readTabularFile
# ==========================================================================
APPENDIX_C = b'#_version_#\t1\n#_defaults_#\tguest\t0\nalice\tAlice\t30\nbob\tBob\ncarol\t\t25\nalice\tAlice\t31\nbob\n'


def test_spec_appendix_c_via_read_tabular_file(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, APPENDIX_C)
	data = TSVZ.readTabularFile(p)
	assert list(data) == ['alice', 'carol']
	assert data['alice'] == ['alice', 'Alice', '31']
	assert data['carol'] == ['carol', '', '25']


def test_spec_absent_columns_use_row_bound_defaults(tmp_path):
	p = str(tmp_path / 'd.tsvz')
	_touch(p, b'a\t1\n#_defaults_#\tX\tY\tZ\nb\t2\n#_defaults_#\tQ\nc\t3\n')
	data = TSVZ.readTabularFile(p)
	assert data['a'] == ['a', '1']
	assert data['b'] == ['b', '2', 'Y', 'Z']
	assert data['c'] == ['c', '3']


def test_spec_wide_rows_are_kept_and_strict_does_not_drop(tmp_path):
	p = str(tmp_path / 'w.tsvz')
	_touch(p, b'#id\tv\na\t1\nb\t2\t3\t4\nc\n')
	assert TSVZ.readTabularFile(p, header='id\tv', strict=True) == {'a': ['a', '1'], 'b': ['b', '2', '3', '4']}


def test_spec_header_comment_after_markers(tmp_path):
	p = str(tmp_path / 'h.tsvz')
	_touch(p, b'#_version_#\t1\n#id\tv\na\t1\n')
	assert TSVZ.readTabularFile(p, header='id\tv', strict=True) == {'a': ['a', '1']}
	with pytest.raises(ValueError):
		TSVZ.readTabularFile(p, header='other\tcols', strict=True)


def test_spec_header_written_as_data_is_data(tmp_path, capsys):
	p = str(tmp_path / 'h.tsvz')
	_touch(p, b'id\tv\na\t1\n')
	assert TSVZ.readTabularFile(p, header='id\tv') == {'id': ['id', 'v'], 'a': ['a', '1']}
	assert any('not a # comment' in w for w in _tsvz_warnings(capsys))


def test_spec_read_creates_store(tmp_path):
	p = str(tmp_path / 'n.tsvz')
	assert TSVZ.readTabularFile(p, header='id\tv', createIfNotExist=True) == {}
	assert open(p, 'rb').read() == b'#id\tv\n'
	q = str(tmp_path / 'e.tsvz')
	TSVZ.readTabularFile(q, createIfNotExist=True, defaults=['NA'])
	assert open(q, 'rb').read() == b'#_defaults_#\tNA\n'
	z = str(tmp_path / 'z.tsvz.gz')
	TSVZ.readTabularFile(z, header='id\tv', createIfNotExist=True)
	assert gzip.decompress(open(z, 'rb').read()) == b'#id\tv\n'
	e = str(tmp_path / 'empty.tsvz')
	TSVZ.readTabularFile(e, createIfNotExist=True)
	assert open(e, 'rb').read() == b''
	with pytest.raises(FileNotFoundError):
		TSVZ.readTabularFile(str(tmp_path / 'missing.tsvz'))
	assert TSVZ.readTabularFile(str(tmp_path / 'missing.tsvz'), strict=False) == {}


def test_spec_conflicting_delimiter_and_encoding(tmp_path, capsys):
	p = str(tmp_path / 'c.csvz')
	_touch(p, 'k,v<sep>w\n'.encode())
	assert TSVZ.readTabularFile(p, delimiter='\t', encoding='latin-1') == {'k': ['k', 'v,w']}
	warnings = _tsvz_warnings(capsys)
	assert any("delimiter '\\t' conflicts" in w for w in warnings) and any('encoding' in w for w in warnings)
	assert TSVZ.readTabularFile(p, delimiter='comma', encoding='UTF-8') == {'k': ['k', 'v,w']}
	assert _tsvz_warnings(capsys) == []


def test_spec_defaults_argument_is_a_preamble_updated_in_place(tmp_path):
	p = str(tmp_path / 'p.tsvz')
	_touch(p, b'a\t1\n#_defaults_#\tX\tY\nb\t2\n')
	defaults = ['#_defaults_#', 'P', 'Q']
	data = TSVZ.readTabularFile(p, defaults=defaults)
	assert data['a'] == ['a', '1', 'Q'] and data['b'] == ['b', '2', 'Y']
	assert defaults == ['#_defaults_#', 'X', 'Y']


def test_spec_last_line_only_and_offsets(tmp_path):
	p = str(tmp_path / 'l.tsvz')
	_touch(p, b'#_defaults_#\tD\na\t1\nb\nb\t2\n#_defaults_#\tE\nc\n')
	assert TSVZ.readTabularFile(p, lastLineOnly=True) == ['b', '2']
	assert TSVZ.read_last_valid_line(p, {}, -1) == ['b', '2']
	b_offset = len(b'#_defaults_#\tD\na\t1\nb\n')
	assert TSVZ.readTabularFile(p, lastLineOnly=True, storeOffset=True) == b_offset
	assert TSVZ.readTabularFile(p, storeOffset=True) == {'a': len(b'#_defaults_#\tD\n'), 'b': b_offset}


def test_spec_multipart_read_and_fragment(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'#_defaults_#\tD\na\t1\n')
	_touch(base + '.f', b'b\t2\n')
	_touch(base + '.10', b'a\nc\n')
	_touch(base + '.11.rotated', b'z\t9\n')
	assert TSVZ.readTabularFile(base) == {'b': ['b', '2']}
	assert TSVZ.readTabularFile(base + '.f') == {'b': ['b', '2']}
	assert any('one part of a multi-part store' in w for w in _tsvz_warnings(capsys))


def test_spec_empty_and_marker_only_stores(tmp_path):
	for content in (b'', b'#only a comment\n', b'#_defaults_#\tD\n#_version_#\t1\n'):
		p = str(tmp_path / 'e.tsvz')
		_touch(p, content)
		assert TSVZ.readTabularFile(p) == {}
		assert TSVZ.readTabularFile(p, lastLineOnly=True) == []


# ==========================================================================
# Spec dialect: appends, compressed repair, clear
# ==========================================================================
def test_spec_append_writes_rows_as_given(tmp_path):
	p = str(tmp_path / 'a.tsvz')
	TSVZ.appendLinesTabularFile(p, [['k1', 'x'], ['k2', '', ''], ['#hash', 'a<b'], ['k3'], 'k4\tv4'],
								header='id\tv\tw', createIfNotExist=True)
	assert open(p, 'rb').read() == b'#id\tv\tw\nk1\tx\nk2\t\t\n<#>hash\ta<lt>b\nk3\nk4\tv4\n'
	assert TSVZ.readTabularFile(p, header='id\tv\tw') == {
		'k1': ['k1', 'x', ''], 'k2': ['k2', '', ''], '#hash': ['#hash', 'a<b', ''], 'k4': ['k4', 'v4', '']}


def test_spec_append_dict_and_markers(tmp_path):
	p = str(tmp_path / 'a.tsvz')
	TSVZ.appendLinesTabularFile(p, OrderedDict([('k1', ['x']), ('#_defaults_#', ['D'])]), createIfNotExist=True)
	assert open(p, 'rb').read() == b'k1\tx\n#_defaults_#\tD\n'


def test_spec_append_truncates_torn_tail(tmp_path, capsys):
	p = str(tmp_path / 'a.tsvz')
	_touch(p, b'a\t1\nb\t')
	TSVZ.appendTabularFile(p, ['c', '3'])
	assert open(p, 'rb').read() == b'a\t1\nc\t3\n'
	assert _tsvz_warnings(capsys)[-1].endswith("removed uncommitted tail b'b\\t' before appending")


def test_spec_append_goes_to_active_part(tmp_path):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'a\t1\n')
	_touch(base + '.2', b'b\t2\n')
	TSVZ.appendTabularFile(base, ['c', '3'])
	assert open(base + '.2', 'rb').read() == b'b\t2\nc\t3\n'
	assert open(base, 'rb').read() == b'a\t1\n'


def test_spec_append_compressed_adds_member(tmp_path):
	p = str(tmp_path / 'a.tsvz.gz')
	TSVZ.appendLinesTabularFile(p, [['a', '1']], createIfNotExist=True)
	TSVZ.appendLinesTabularFile(p, [['b', '2']])
	assert gzip.decompress(open(p, 'rb').read()) == b'a\t1\nb\t2\n'


def test_spec_append_header_mismatch_strict(tmp_path):
	p = str(tmp_path / 'a.tsvz')
	_touch(p, b'#id\tv\n')
	with pytest.raises(ValueError):
		TSVZ.appendTabularFile(p, ['k', 'v'], header='x\ty', strict=True)
	TSVZ.appendTabularFile(p, ['k', 'v'], header='id\tv', strict=True)
	assert open(p, 'rb').read() == b'#id\tv\nk\tv\n'


def test_spec_compressed_repair_keeps_records_and_backs_up(tmp_path, capsys):
	g = str(tmp_path / 'g.tsvz.gz')
	_touch(g, gzip.compress(b'a\t1\n') + gzip.compress(b'b\t2\nbr')[:-6])
	reporter = TSVZ._Reporter(g)
	TSVZ._specAppendPayload(g, b'c\t3\n', reporter, repair=True)
	reporter.flush()
	assert TSVZ.readTabularFile(g) == {'a': ['a', '1'], 'b': ['b', '2'], 'c': ['c', '3']}
	assert len([n for n in os.listdir(str(tmp_path)) if '.damaged-' in n]) == 1
	assert any('repaired a damaged compressed part' in w for w in _tsvz_warnings(capsys))


def test_spec_rewrite_in_place_keeps_inode_and_skips_identical(tmp_path):
	p = str(tmp_path / 'r.tsvz')
	_touch(p, b'a\t1\nb\t2\n')
	ino = os.stat(p).st_ino
	assert TSVZ._specRewriteInPlace(p, b'a\t1\n') is True
	assert open(p, 'rb').read() == b'a\t1\n' and os.stat(p).st_ino == ino
	assert TSVZ._specRewriteInPlace(p, b'a\t1\n') is False


def test_spec_clear_keeps_header_and_markers(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, b'#id\tv\n#_defaults_#\tD\n#_fill_empty_with_default_#\ttrue\na\t1\n# note\nb\t2\n')
	ino = os.stat(p).st_ino
	TSVZ.clearTabularFile(p)
	assert open(p, 'rb').read() == b'#id\tv\n#_fill_empty_with_default_#\ttrue\n#_defaults_#\tD\n'
	assert os.stat(p).st_ino == ino


def test_spec_clear_multipart_appends_tombstones(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'a\t1\n')
	_touch(base + '.2', b'b\t2\n')
	TSVZ.clearTabularFile(base)
	assert TSVZ.readTabularFile(base) == {}
	assert open(base, 'rb').read() == b'a\t1\n'
	assert open(base + '.2', 'rb').read() == b'b\t2\na\nb\n'
	assert any('older parts are not compacted' in w for w in _tsvz_warnings(capsys))


def test_spec_clear_creates_missing_and_empty_files(tmp_path):
	p = str(tmp_path / 'n.tsvz')
	TSVZ.clearTabularFile(p, header='id\tv')
	assert open(p, 'rb').read() == b'#id\tv\n'
	e = str(tmp_path / 'e.tsvz')
	_touch(e)
	TSVZ.clearTabularFile(e)
	assert open(e, 'rb').read() == b''



if __name__ == '__main__':
	sys.exit(pytest.main([__file__] + sys.argv[1:]))
