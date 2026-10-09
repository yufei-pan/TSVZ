#! /usr/bin/env python3
"""Tests for TSVZ 4.1 (TSVZ.py).

Plain ``test_*`` functions (pytest-style, no class boilerplate), matching the
repo convention. Run them with ``python3 -m pytest TSVZ_test.py -q``, or with
``python3 TSVZ_test.py``, which hands its arguments to pytest. The
differential tests compare the legacy dialect against the frozen 3.39
reference ``TSVZ_old.py`` (``-k 339`` selects them).
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
import threading
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
	# monitor_external_changes=False: with it on, the append worker can mistake this
	# instance's own write for an external change and rewrite queued rows that the
	# same tick then appends again -- a timing race 3.39 has too, so the two modules'
	# bytes could differ by luck. Both modules get the same settings.
	kwargs = dict(header=LEGACY_HEADER, append_check_delay=0.001, monitor_external_changes=False)
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



# ==========================================================================
# Spec dialect: archival scrub (design §5.7)
# ==========================================================================
def test_spec_scrub_compacts_in_place(tmp_path):
	p = str(tmp_path / 's.tsvz')
	_touch(p, b'#id\tv\n#_defaults_#\tD\na\t1\nb\t2\n# comment\na\t3\nb\n#__tool__#\tmeta\n#_checksum_crc32_#\n')
	ino = os.stat(p).st_ino
	before = TSVZ.readTabularFile(p, header='id\tv')
	assert TSVZ.scrubTabularFile(p, header='id\tv') == before
	assert open(p, 'rb').read() == b'#id\tv\n#_version_#\t1\n#_defaults_#\tD\na\t3\n'
	assert os.stat(p).st_ino == ino
	assert TSVZ.readTabularFile(p, header='id\tv') == before
	mtime = os.stat(p).st_mtime_ns
	TSVZ.scrubTabularFile(p, header='id\tv')
	assert os.stat(p).st_mtime_ns == mtime  # identical content is not rewritten


def test_spec_scrub_fidelity_when_markers_changed(tmp_path):
	p = str(tmp_path / 'f.tsvz')
	_touch(p, b'#id\tname\tage\n#_strip_trailing_whites_#\tfalse\na\tx  \t1\n#_strip_trailing_whites_#\ttrue\n'
			  b'b\tq\n#_defaults_#\tD1\tD2\tD3\n#_fill_empty_with_default_#\ttrue\nc\t\t5\n')
	before = TSVZ.readTabularFile(p, header='id\tname\tage')
	TSVZ.scrubTabularFile(p, header='id\tname\tage')
	after = TSVZ.readTabularFile(p, header='id\tname\tage')
	assert list(after) == list(before)
	for key, row in before.items():
		assert after[key][:len(row)] == row and not any(after[key][len(row):]), (key, row, after[key])
	text = open(p, 'rb').read()
	assert text.index(b'#_fill_empty_with_default_#\tfalse') < text.index(b'a\tx  \t1') < text.index(b'#_fill_empty_with_default_#\ttrue')
	load = TSVZ._specLoad(p, '\t')
	assert load.state.strip and load.state.fillEmpty and load.state.defaults == ['#_defaults_#', 'D1', 'D2', 'D3']


def test_spec_scrub_keeps_marker_like_data_keys_as_data(tmp_path):
	p = str(tmp_path / 'k.tsvz')
	_touch(p, b'<#>_defaults_#\tdata\n<#>plain\tv\n')
	TSVZ.scrubTabularFile(p)
	assert open(p, 'rb').read() == b'#_version_#\t1\n<#>_defaults_#\tdata\n<#>plain\tv\n'
	assert TSVZ.readTabularFile(p) == {'#_defaults_#': ['#_defaults_#', 'data'], '#plain': ['#plain', 'v']}


def test_spec_scrub_refuses_multipart_and_fragments(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'a\t1\na\n')
	_touch(base + '.2', b'b\t2\n')
	assert TSVZ.scrubTabularFile(base) == {'b': ['b', '2']}
	assert TSVZ.scrubTabularFile(base + '.2') == {'b': ['b', '2']}
	assert open(base, 'rb').read() == b'a\t1\na\n' and open(base + '.2', 'rb').read() == b'b\t2\n'
	assert sum('scrub skipped' in w for w in _tsvz_warnings(capsys)) == 2


def test_spec_scrub_compressed_in_place(tmp_path):
	p = str(tmp_path / 's.tsvz.gz')
	_touch(p, gzip.compress(b'a\t1\n') + gzip.compress(b'a\t2\nb\t3\nb\n'))
	ino = os.stat(p).st_ino
	TSVZ.scrubTabularFile(p)
	assert gzip.decompress(open(p, 'rb').read()) == b'#_version_#\t1\na\t2\n'
	assert os.stat(p).st_ino == ino


def test_spec_scrub_empty_stores(tmp_path):
	for content in (b'', b'#id\tv\n', b'a\t1\na\n'):
		p = str(tmp_path / 'e.tsvz')
		_touch(p, content)
		assert TSVZ.scrubTabularFile(p) == {}
		assert TSVZ.readTabularFile(p) == {}


# ==========================================================================
# TSVZed on spec stores: loading and reading (design §5.6, §6)
# ==========================================================================
def _wait_for(predicate, timeout=3.0):
	deadline = time.time() + timeout
	while not predicate() and time.time() < deadline:
		time.sleep(0.01)
	return predicate()


def test_tsvzed_spec_loads_without_rewriting(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, APPENDIX_C)
	mtime = os.stat(p).st_mtime_ns
	t = TSVZ.TSVZed(p)
	assert t.dialect == 'tsvz'
	assert list(t) == ['alice', 'carol'] and t['alice'] == ['alice', 'Alice', '31']
	assert t.defaults == ['#_defaults_#', 'guest', '0']
	t.close()
	assert open(p, 'rb').read() == APPENDIX_C and os.stat(p).st_mtime_ns == mtime


def test_tsvzed_dialect_attribute_for_tsv(tmp_path):
	t = TSVZ.TSVZed(str(tmp_path / 'a.tsv'))
	assert t.dialect == 'tsv'
	t.close()


def test_tsvzed_spec_missing_key_returns_defaults(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, APPENDIX_C)
	t = TSVZ.TSVZed(p)
	assert t['bob'] == ['bob', 'guest', '0']
	assert 'bob' not in t and t.get('bob') is None and t.get('bob', 7) == 7
	assert t.pop('bob', 'x') == 'x'
	t.close()
	_touch(p, APPENDIX_C + b'#_return_defaults_when_missing_#\tfalse\n')
	t = TSVZ.TSVZed(p)
	with pytest.raises(KeyError):
		t['bob']
	t.close()


def test_tsvzed_spec_empty_stores(tmp_path):
	t = TSVZ.TSVZed(str(tmp_path / 'e.tsvz'), header='id\ta\tb')
	assert dict(t) == {} and t['nobody'] == ['nobody', '', '']
	t.close()
	for content in (b'', b'#id\tv\n', b'#_defaults_#\tD\n'):
		p = str(tmp_path / 'm.tsvz')
		_touch(p, content)
		t = TSVZ.TSVZed(p)
		assert dict(t) == {}
		t.close()


def test_tsvzed_legacy_missing_key_still_raises(tmp_path):
	t = TSVZ.TSVZed(str(tmp_path / 'a.tsv'))
	with pytest.raises(KeyError):
		t['nobody']
	t.close()


def test_tsvzed_spec_rewrite_features_warn_and_do_nothing(tmp_path, capsys):
	p = str(tmp_path / 'r.tsvz')
	_touch(p, b'a\t1\na\t2\n')
	t = TSVZ.TSVZed(p, rewrite_on_load=True, rewrite_on_exit=True, rewrite_interval=5)
	assert t.rewrite(force=True) is False
	t.mapToFile()
	t.hardMapToFile()
	t.close()
	assert open(p, 'rb').read() == b'a\t1\na\t2\n'
	warnings = _tsvz_warnings(capsys)
	assert len(warnings) == 6 and all('ignored for .tsvz' in w for w in warnings)


def test_tsvzed_spec_default_constructor_is_silent(tmp_path, capsys):
	t = TSVZ.TSVZed(str(tmp_path / 'q.tsvz'))
	t.close()
	assert _tsvz_warnings(capsys) == []


def test_tsvzed_spec_move_to_end_is_memory_only(tmp_path, capsys):
	p = str(tmp_path / 'o.tsvz')
	_touch(p, b'a\t1\nb\t2\n')
	t = TSVZ.TSVZed(p)
	t.move_to_end('a')
	assert list(t) == ['b', 'a'] and t.rewrite_on_exit is False
	t.close()
	assert open(p, 'rb').read() == b'a\t1\nb\t2\n'
	assert any('move_to_end only reorders memory' in w for w in _tsvz_warnings(capsys))


def test_tsvzed_spec_reloads_on_external_append(tmp_path):
	p = str(tmp_path / 'x.tsvz')
	_touch(p, b'a\t1\n')
	t = TSVZ.TSVZed(p, append_check_delay=0.01)
	time.sleep(0.05)
	with open(p, 'ab') as f:
		f.write(b'b\t2\n')
	st = os.stat(p)
	os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 10 ** 9))
	assert _wait_for(lambda: 'b' in t)
	assert t['b'] == ['b', '2']
	t.close()


def test_tsvzed_spec_reloads_when_a_new_part_appears(tmp_path):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'a\t1\n')
	t = TSVZ.TSVZed(base, append_check_delay=0.01)
	time.sleep(0.05)
	_touch(base + '.5', b'b\t2\n')
	assert _wait_for(lambda: 'b' in t)
	assert t._activePath == base + '.5'
	t.close()


def test_tsvzed_spec_header_and_defaults_arguments(tmp_path):
	p = str(tmp_path / 'h.tsvz')
	_touch(p, b'#id\tname\tage\nk\tv\n')
	t = TSVZ.TSVZed(p, header='id\tname\tage', defaults=['x', 'y'])
	assert t.correctColumnNum == 3 and t['k'] == ['k', 'v', 'y']
	t.close()


# ==========================================================================
# TSVZed on spec stores: writing (design §5.2–§5.5, §5.8)
# ==========================================================================
def test_tsvzed_spec_writes_rows_tombstones_and_hash_keys(tmp_path):
	p = str(tmp_path / 'w.tsvz')
	t = TSVZ.TSVZed(p, header='id\ta\tb')
	t['k1'] = ['k1', 'x']
	t['k2'] = ['k2', '', '']
	t['#hash'] = 'v<1>'
	t['k3'] = 'k3\ty\tz'
	del t['k3']
	t['k1'] = 'k1'
	assert dict(t) == {'k2': ['k2', '', ''], '#hash': ['#hash', 'v<1>', '']}
	t.close()
	assert open(p, 'rb').read() == b'#id\ta\tb\nk1\tx\nk2\t\t\n<#>hash\tv<lt>1>\nk3\ty\tz\nk3\nk1\n'
	t = TSVZ.TSVZed(p, header='id\ta\tb')
	assert dict(t) == {'k2': ['k2', '', ''], '#hash': ['#hash', 'v<1>', '']}
	t.close()


def test_tsvzed_spec_markers_through_the_api(tmp_path):
	p = str(tmp_path / 'm.tsvz')
	t = TSVZ.TSVZed(p)
	t['#_defaults_#'] = ['G', '0']
	t['a'] = ['a', '', '']
	t['#_return_defaults_when_missing_#'] = 'false'
	t['#__tool__#'] = 'meta'
	t['#_fill_empty_with_default_#'] = 'maybe'  # rejected: not written
	assert t['a'] == ['a', 'G', '0'] and '#__tool__#' not in t
	with pytest.raises(KeyError):
		t['missing']
	del t['#_defaults_#']
	assert t.defaults == ['#_defaults_#']
	t.close()
	assert open(p, 'rb').read() == (b'#_defaults_#\tG\t0\na\tG\t0\n#_return_defaults_when_missing_#\tfalse\n'
									b'#__tool__#\tmeta\n#_defaults_#\n')


def test_tsvzed_spec_wide_values_are_kept(tmp_path):
	p = str(tmp_path / 'w.tsvz')
	t = TSVZ.TSVZed(p, header='id\tv')
	t['k'] = ['k', '1', 'extra']
	assert t['k'] == ['k', '1', 'extra']
	t.close()
	assert TSVZ.readTabularFile(p, header='id\tv') == {'k': ['k', '1', 'extra']}
	s = TSVZ.TSVZed(p, header='id\tv', strict=True)
	s['j'] = ['j', '1', 'extra']  # strict keeps 3.39's refusal
	assert 'j' not in s
	s.close()


def test_tsvzed_spec_constructor_defaults_are_persisted_once(tmp_path):
	p = str(tmp_path / 'd.tsvz')
	_touch(p, b'a\t1\n')
	for _ in range(2):
		t = TSVZ.TSVZed(p, defaults=['X', 'Y'])
		t.close()
	assert open(p, 'rb').read() == b'a\t1\n#_defaults_#\tX\tY\n'
	n = str(tmp_path / 'n.tsvz')
	t = TSVZ.TSVZed(n, header='id\tv', defaults=['X'])
	t.close()
	assert open(n, 'rb').read() == b'#id\tv\n#_defaults_#\tX\n'


def test_tsvzed_spec_append_truncates_torn_tail(tmp_path, capsys):
	p = str(tmp_path / 't.tsvz')
	_touch(p, b'a\t1\nhalf')
	t = TSVZ.TSVZed(p)
	t['b'] = ['b', '2']
	t.close()
	assert open(p, 'rb').read() == b'a\t1\nb\t2\n'
	warnings = _tsvz_warnings(capsys)
	assert any('ignored uncommitted bytes' in w for w in warnings)
	assert any("removed uncommitted tail b'half'" in w for w in warnings)


def test_tsvzed_spec_repairs_damaged_gzip_before_appending(tmp_path):
	g = str(tmp_path / 'g.tsvz.gz')
	_touch(g, gzip.compress(b'a\t1\n') + gzip.compress(b'b\t2\nbr')[:-6])
	t = TSVZ.TSVZed(g)
	t['c'] = ['c', '3']
	t.close()
	assert TSVZ.readTabularFile(g) == {'a': ['a', '1'], 'b': ['b', '2'], 'c': ['c', '3']}
	assert any('.damaged-' in name for name in os.listdir(str(tmp_path)))


def test_tsvzed_spec_appends_go_to_the_active_part(tmp_path):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'a\t1\n')
	_touch(base + '.2', b'b\t2\n')
	t = TSVZ.TSVZed(base)
	t['c'] = ['c', '3']
	del t['a']
	t.close()
	assert open(base, 'rb').read() == b'a\t1\n'
	assert open(base + '.2', 'rb').read() == b'b\t2\nc\t3\na\n'


def test_tsvzed_spec_clear_keeps_markers_queued_before_it(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	t = TSVZ.TSVZed(p, header='id\tv')
	t['a'] = ['a', '1']
	t['#_defaults_#'] = ['D']
	t.clear()
	t['b'] = ['b', '']
	t.close()
	assert open(p, 'rb').read() == b'#id\tv\n#_defaults_#\tD\nb\tD\n'


def test_tsvzed_spec_keeps_queue_when_writes_fail(tmp_path, capsys):
	d = tmp_path / 'sub'
	d.mkdir()
	p = str(d / 'f.tsvz')
	t = TSVZ.TSVZed(p, append_check_delay=0.01)
	t._activePath = str(d / 'missing-dir' / 'f.tsvz')  # every write now fails
	t['a'] = ['a', '1']
	time.sleep(0.1)
	assert _wait_for(lambda: len(t.appendQueue) == 1)
	assert open(p, 'rb').read() == b''
	t._activePath = p
	assert _wait_for(lambda: not t.appendQueue)
	t.close()
	assert open(p, 'rb').read() == b'a\t1\n'
	out = capsys.readouterr()
	assert out.out.count('will retry') == 1 and 'writes recovered' in out.err


def test_two_tsvzed_handles_on_one_tsvz_file(tmp_path):
	p = str(tmp_path / 'shared.tsvz')
	a = TSVZ.TSVZed(p, append_check_delay=0.002)
	b = TSVZ.TSVZed(p, append_check_delay=0.002)
	for i in range(50):
		a['a%d' % i] = ['a%d' % i, str(i)]
		b['b%d' % i] = ['b%d' % i, str(i)]
	a.close()
	b.close()
	data = TSVZ.readTabularFile(p)
	assert len(data) == 100 and data['a7'] == ['a7', '7'] and data['b49'] == ['b49', '49']
	assert open(p, 'rb').read().count(b'\n') == 100


def test_tsvzed_spec_recreates_a_deleted_file_on_append(tmp_path):
	p = str(tmp_path / 'gone.tsvz')
	t = TSVZ.TSVZed(p, monitor_external_changes=False)
	os.remove(p)
	t['a'] = ['a', '1']
	t.close()
	assert open(p, 'rb').read() == b'a\t1\n'


def test_tsvzed_spec_survives_a_failed_external_reload(tmp_path, capsys):
	p = str(tmp_path / 'x.tsvz')
	_touch(p, b'#id\tv\n')
	t = TSVZ.TSVZed(p, header='id\tv', strict=True, append_check_delay=0.01)
	time.sleep(0.05)
	_touch(p, b'#other\tcols\n')
	st = os.stat(p)
	os.utime(p, (st.st_atime + 1, st.st_mtime + 1))
	assert _wait_for(lambda: not t.deSynced and t._specLoaded)
	time.sleep(0.3)
	t['k'] = ['k', 'v']
	assert t.appendThread.is_alive()
	assert _wait_for(lambda: b'k\tv\n' in open(p, 'rb').read())
	t.close()
	assert capsys.readouterr().out.count('Failed to reload') == 1


def test_tsvzed_spec_lone_surrogate_does_not_kill_the_worker(tmp_path):
	p = str(tmp_path / 's.tsvz')
	t = TSVZ.TSVZed(p, append_check_delay=0.01)
	t['bad'] = ['bad', b'\xff'.decode('utf-8', 'surrogateescape')]
	assert _wait_for(lambda: b'bad\t?\n' in open(p, 'rb').read())
	t['c'] = ['c', '1']
	assert _wait_for(lambda: b'c\t1\n' in open(p, 'rb').read())
	assert t.appendThread.is_alive()
	t.close()


def test_tsvzed_spec_clear_after_set_leaves_nothing(tmp_path):
	p = str(tmp_path / 'o.tsvz')
	for _ in range(20):
		t = TSVZ.TSVZed(p, header='id\tv', append_check_delay=0.001)
		t['a'] = ['a', '1']
		t.clear()
		t.close()
		assert TSVZ.readTabularFile(p, header='id\tv') == {}


def test_tsvzed_spec_width_matches_a_reload(tmp_path):
	p = str(tmp_path / 'wd.tsvz')
	t = TSVZ.TSVZed(p, defaults=['X'])
	t['a'] = ['a', '1', '2']
	t['b'] = ['b', '5']
	mem = dict(t)
	t.close()
	assert mem == dict(TSVZ.TSVZed(p, defaults=['X']))


def test_tsvzed_spec_sync_keeps_memory_while_writes_fail(tmp_path):
	d = tmp_path / 'sub'
	d.mkdir()
	p = str(d / 'f.tsvz')
	t = TSVZ.TSVZed(p, append_check_delay=0.01, monitor_external_changes=False)
	t._activePath = str(d / 'nodir' / 'f.tsvz')
	t['a'] = ['a', '1']
	t.deSynced = True
	assert t._specSync(True) is False
	assert t['a'] == ['a', '1'] and t.deSynced
	t._activePath = p
	t.close()


# ==========================================================================
# TSVZedLite on spec stores
# ==========================================================================
def test_lite_spec_reads_and_writes(tmp_path):
	p = str(tmp_path / 'l.tsvz')
	_touch(p, APPENDIX_C)
	lite = TSVZ.TSVZedLite(p, strict=False)
	assert lite.dialect == 'tsvz'
	assert list(lite) == ['alice', 'carol']
	assert lite['carol'] == ['carol', '', '25'] and lite['alice'] == ['alice', 'Alice', '31']
	assert lite['bob'] == ['bob', 'guest', '0']
	assert 'bob' not in lite and lite.get('bob') is None and lite.get('alice') == ['alice', 'Alice', '31']
	assert lite.setdefault('carol', ['x']) == ['carol', '', '25']
	lite['dave'] = ['dave', 'Dave']
	assert lite['dave'] == ['dave', 'Dave', '0']
	lite['#_defaults_#'] = ['G', '9']
	lite['erin'] = ['erin', 'Erin']
	assert lite['erin'] == ['erin', 'Erin', '9'] and lite['dave'] == ['dave', 'Dave', '0']
	del lite['alice']
	lite.close()
	assert TSVZ.readTabularFile(p) == {'carol': ['carol', '', '25'], 'dave': ['dave', 'Dave', '0'],
									   'erin': ['erin', 'Erin', '9']}


def test_lite_spec_multipart_older_parts_in_memory(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'a\t1\nb\t2\n')
	_touch(base + '.3', b'c\t3\n')
	lite = TSVZ.TSVZedLite(base, strict=False)
	assert lite.indexes['a'] == ['a', '1'] and isinstance(lite.indexes['c'], int)
	lite['d'] = ['d', '4']
	del lite['a']
	lite.close()
	assert open(base + '.3', 'rb').read() == b'c\t3\nd\t4\na\n'
	assert any('held in memory' in w for w in _tsvz_warnings(capsys))


def test_lite_spec_gzip(tmp_path):
	g = str(tmp_path / 'g.tsvz.gz')
	TSVZ.appendLinesTabularFile(g, [['a', '1']], createIfNotExist=True)
	lite = TSVZ.TSVZedLite(g, strict=False)
	lite['b'] = ['b', '2']
	assert lite['a'] == ['a', '1'] and lite['b'] == ['b', '2']
	lite.close()
	assert TSVZ.readTabularFile(g) == {'a': ['a', '1'], 'b': ['b', '2']}


def test_lite_spec_external_indexes(tmp_path):
	p = str(tmp_path / 'x.tsvz')
	_touch(p, b'a\t1\n#_defaults_#\tQ\tR\nb\t2\n')
	offsets = TSVZ.readTabularFile(p, storeOffset=True)
	lite = TSVZ.TSVZedLite(p, indexes=offsets, strict=False)
	assert lite['b'] == ['b', '2', 'R'] and lite['a'] == ['a', '1']
	lite.close()


def test_lite_spec_torn_tail_and_clear(tmp_path):
	p = str(tmp_path / 't.tsvz')
	_touch(p, b'#id\tv\na\t1\nzz')
	lite = TSVZ.TSVZedLite(p, header='id\tv', strict=False)
	lite['b'] = ['b', '2']
	assert open(p, 'rb').read() == b'#id\tv\na\t1\nb\t2\n'
	lite.clear()
	assert open(p, 'rb').read() == b'#id\tv\n'
	lite['c'] = ['c', '3']
	assert lite['c'] == ['c', '3'] and list(lite) == ['c']
	lite.close()


def test_lite_spec_constructor_defaults_persisted(tmp_path):
	p = str(tmp_path / 'd.tsvz')
	_touch(p, b'a\t1\n')
	lite = TSVZ.TSVZedLite(p, defaults=['X', 'Y'], strict=False)
	lite.close()
	assert open(p, 'rb').read() == b'a\t1\n#_defaults_#\tX\tY\n'


def test_lite_switch_file_between_dialects(tmp_path):
	tsv = str(tmp_path / 'a.tsv')
	tsvz = str(tmp_path / 'b.tsvz')
	_touch(tsv, b'k\tv\n')
	_touch(tsvz, b'k\tw\n')
	lite = TSVZ.TSVZedLite(tsv, strict=False)
	assert lite['k'] == ['k', 'v'] and lite.dialect == 'tsv'
	lite.switchFile(tsvz)
	assert lite['k'] == ['k', 'w'] and lite.dialect == 'tsvz'
	lite.close()

# ==========================================================================
# CLI
# ==========================================================================
def _cli(*args, script='TSVZ.py'):
	return subprocess.run([sys.executable, os.path.join(HERE, script)] + list(args),
						  stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)


def test_cli_spec_operations(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	assert _cli(p, 'append', 'alice', 'Alice', '30').returncode == 0
	assert _cli(p, 'append', 'bob', 'Bob', '7').returncode == 0
	assert _cli(p, 'delete', 'bob').returncode == 0
	out = _cli(p, 'read')
	assert out.returncode == 0 and 'Alice' in out.stdout and 'Bob' not in out.stdout
	assert open(p, 'rb').read() == b'alice\tAlice\t30\nbob\tBob\t7\nbob\n'
	assert _cli(p, 'scrub').returncode == 0
	assert open(p, 'rb').read() == b'#_version_#\t1\nalice\tAlice\t30\n'
	assert _cli(p, 'clear').returncode == 0
	assert open(p, 'rb').read() == b''


def test_cli_reads_a_store_made_of_numbered_parts_only(tmp_path):
	base = str(tmp_path / 'm.tsvz')
	_touch(base + '.1', b'key1\tvalue1\n')
	out = _cli(base, 'read')
	assert out.returncode == 0 and 'File not found' not in out.stdout and 'value1' in out.stdout


def test_cli_legacy_matches_339(tmp_path):
	"""3.39 file-first command lines leave 3.39's bytes and print 3.39's text (spec §20.1.5).

	Two listed CLI changes apply: 4.1 prints diagnostics on stderr, and prints
	records into a pipe (``--format table`` asks for 3.39's table). Options
	follow the positionals: 3.39's argparse rejects ``STORE -c H append ...``
	on Python 3.6 and 3.7.
	"""
	steps = (['append', 'k', 'v', '-c', 'id\\tval'], ['append', 'j', 'w'], ['delete', 'k'], ['read'],
			 ['scrub'], ['read'], ['clear', '-c', 'id\\tval'])
	results = []
	for script in ('TSVZ.py', 'TSVZ_old.py'):
		d = tmp_path / script.split('.')[0]
		d.mkdir()
		p = str(d / 'c.tsv')
		outputs = []
		for args in steps:
			if script == 'TSVZ.py':
				r = _cli(p, *(args + ['--format', 'table']), script=script)
				assert 'Created' not in r.stdout
				text = r.stdout + r.stderr
			else:
				r = _cli(p, *args, script=script)
				text = r.stdout
			outputs.append((r.returncode, text.replace(str(d), 'D')))
		results.append((outputs, open(p, 'rb').read()))
	assert results[0] == results[1]


def test_cli_version():
	out = _cli('-V')
	assert out.returncode == 0 and '4.1' in out.stdout


# ==========================================================================
# Round-trip fuzz: memory == reload == scrub + reload (design §8.6)
# ==========================================================================
SPEC_ALPHABET = ['a', 'b', ' ', '\t', ',', '|', '\0', '\n', '<', '>', '#', '_', 'é', '中', '\U0001F600',
				 'sep', 'LF', 'lt', '\r']
LEGACY_ALPHABET = ['a', 'b', ' ', '\t', ',', '|', '\n', '<', '>', 'é', '中', 'sep', 'LF', '\r']


def _fuzz_text(rng, alphabet):
	return ''.join(rng.choice(alphabet) for _ in range(rng.randint(0, 6))).rstrip()


def _trim_empty_tail(row):
	row = list(row)
	while len(row) > 2 and row[-1] == '':
		row.pop()
	return row


def _normalised(items):
	return [(key, _trim_empty_tail(row)) for key, row in items]


def _fuzz_session(path, seed, alphabet, legacy):
	rng = random.Random(seed)
	t = TSVZ.TSVZed(path, append_check_delay=0.001)
	for _ in range(40):
		key = _fuzz_text(rng, alphabet) or 'k'
		if legacy:
			key = key.lstrip('#') or 'k'  # '#' keys are memory-only in 3.39 files
		elif key.startswith('#_') and key.endswith('_#'):
			key = 'x' + key  # the reserved marker namespace is not data
		roll = rng.random()
		if roll < 0.6:
			t[key] = [key] + [_fuzz_text(rng, alphabet) for _ in range(rng.randint(1, 3))]
		elif roll < 0.85:
			del t[key]
		else:
			t[key] = key
	t.close()
	return list(t.items())


def _reload_items(path):
	t = TSVZ.TSVZed(path)
	items = list(t.items())
	t.close()
	return items


def test_spec_roundtrip_fuzz(tmp_path):
	for suffix in ('.tsvz', '.csvz', '.nsvz', '.psvz', '.tsvz.gz'):
		for seed in range(6):
			p = str(tmp_path / ('f%d%s' % (seed, suffix)))
			memory = _normalised(_fuzz_session(p, seed, SPEC_ALPHABET, legacy=False))
			assert _normalised(_reload_items(p)) == memory, (suffix, seed)
			lite = TSVZ.TSVZedLite(p, strict=False)
			assert _normalised((key, lite[key]) for key in list(lite.indexes)) == memory, (suffix, seed)
			lite.close()
			TSVZ.scrubTabularFile(p)
			assert _normalised(_reload_items(p)) == memory, (suffix, seed)


def test_legacy_roundtrip_fuzz(tmp_path):
	for suffix in ('.tsv', '.csv', '.nsv', '.psv', '.tsv.gz'):
		for seed in range(6):
			p = str(tmp_path / ('f%d%s' % (seed, suffix)))
			memory = _fuzz_session(p, seed, LEGACY_ALPHABET, legacy=True)
			assert _reload_items(p) == memory, (suffix, seed)
			TSVZ.scrubTabularFile(p)
			assert _reload_items(p) == memory, (suffix, seed)


# ==========================================================================
# Final review fixes: concurrent writers during scrub / clear, TSVZedLite
# locking, setDefaults persistence, part-path clear, huge version, backups
# ==========================================================================
def _append_after_loads(monkeypatch, path, records, times):
	"""Make the first ``times`` calls of ``_specLoad`` commit ``records`` right after they read."""
	real = TSVZ._specLoad
	calls = []

	def loadThenAppend(*args, **kwargs):
		result = real(*args, **kwargs)
		calls.append(1)
		if len(calls) <= times:
			TSVZ.appendLinesTabularFile(path, records)  # another writer commits after the read
		return result
	monkeypatch.setattr(TSVZ, '_specLoad', loadThenAppend)
	return calls


def test_spec_scrub_keeps_a_record_committed_while_it_runs(tmp_path, monkeypatch):
	p = str(tmp_path / 'live.tsvz')
	_touch(p, b'a\t1\na\t2\nb\t1\n')
	calls = _append_after_loads(monkeypatch, p, [['c', 'during']], times=1)
	assert TSVZ.scrubTabularFile(p) == {'a': ['a', '2'], 'b': ['b', '1'], 'c': ['c', 'during']}
	assert len(calls) == 2  # the first attempt saw the store change and was retried
	monkeypatch.undo()
	assert open(p, 'rb').read() == b'#_version_#\t1\na\t2\nb\t1\nc\tduring\n'


def test_spec_scrub_skips_when_the_store_keeps_changing(tmp_path, monkeypatch, capsys):
	p = str(tmp_path / 'busy.tsvz')
	_touch(p, b'a\t1\na\t2\n')
	calls = _append_after_loads(monkeypatch, p, [['c', 'x']], times=99)
	TSVZ.scrubTabularFile(p)
	monkeypatch.undo()
	assert len(calls) == 3
	assert open(p, 'rb').read() == b'a\t1\na\t2\n' + b'c\tx\n' * 3  # nothing was rewritten
	warnings = _tsvz_warnings(capsys)
	assert any('scrub skipped: the store kept changing while it was being compacted; nothing was written' in w
			   for w in warnings)


def test_spec_clear_retries_when_the_store_changes_after_its_read(tmp_path, monkeypatch):
	# The record and the marker committed after the first read are not lost
	# silently: the clear re-reads, so the record is cleared too and the new
	# #_defaults_# survives the clear (a stale preamble would have dropped it).
	p = str(tmp_path / 'live.tsvz')
	_touch(p, b'#id\tv\na\t1\n')
	calls = _append_after_loads(monkeypatch, p, [['c', 'during'], ['#_defaults_#', 'NEW']], times=1)
	TSVZ.clearTabularFile(p)
	monkeypatch.undo()
	assert len(calls) == 2
	assert open(p, 'rb').read() == b'#id\tv\n#_defaults_#\tNEW\n'
	assert TSVZ.readTabularFile(p) == {}


def test_spec_clear_skips_when_the_store_keeps_changing(tmp_path, monkeypatch, capsys):
	p = str(tmp_path / 'busy.tsvz')
	_touch(p, b'a\t1\n')
	calls = _append_after_loads(monkeypatch, p, [['c', 'x']], times=99)
	TSVZ.clearTabularFile(p)
	monkeypatch.undo()
	assert len(calls) == 3
	assert open(p, 'rb').read() == b'a\t1\n' + b'c\tx\n' * 3
	assert any('clear skipped: the store kept changing while it was being cleared; nothing was written' in w
			   for w in _tsvz_warnings(capsys))


def test_tsvzed_spec_clear_reloads_when_the_clear_was_skipped(tmp_path, monkeypatch, capsys):
	p = str(tmp_path / 'busy.tsvz')
	t = TSVZ.TSVZed(p, append_check_delay=0.002)
	t['a'] = ['a', '1']
	t.commitAppendToFile()
	_append_after_loads(monkeypatch, p, [['c', 'x']], times=99)
	t.clear()
	monkeypatch.undo()
	assert _wait_for(lambda: 'a' in t and 'c' in t)  # memory follows the store the clear left alone
	t.close()
	assert TSVZ.readTabularFile(p) == {'a': ['a', '1'], 'c': ['c', 'x']}


def test_lite_spec_and_another_process_append_without_losing_records(tmp_path):
	p = str(tmp_path / 'race.tsvz')
	_touch(p, b'seed\tx\n')
	go = str(tmp_path / 'go')
	n = 200
	code = ('import os, sys, time\nsys.path.insert(0, {!r})\nimport TSVZ\n'
			'deadline = time.time() + 20\n'
			'while not os.path.exists({!r}) and time.time() < deadline:\n\tpass\n'
			'for i in range({}):\n\tTSVZ.appendLinesTabularFile({!r}, [["H%d" % i, "v" * 50]])\n').format(HERE, go, n, p)
	helper = subprocess.Popen([sys.executable, '-c', code], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
	written = 0
	try:
		lite = TSVZ.TSVZedLite(p, strict=False)
		_touch(go)  # both writers start now
		# Keep writing for as long as the helper does, so the two writers overlap.
		while written < n or (helper.poll() is None and written < 100000):
			lite['L%d' % written] = ['L%d' % written, 'v' * 50]
			written += 1
		lite.close()
	finally:
		helper.wait()
	data = TSVZ.readTabularFile(p, strict=False)
	missing = [key for key in ['H%d' % i for i in range(n)] + ['L%d' % i for i in range(written)] if key not in data]
	assert not missing, (len(missing), missing[:10])


def test_tsvzed_spec_set_defaults_is_persisted(tmp_path):
	p = str(tmp_path / 'sd.tsvz')
	t = TSVZ.TSVZed(p, header='id\tname\tscore')
	t.setDefaults(['#_defaults_#', 'NONAME', '0'])
	t['k'] = ['k', 'alice']
	t['j'] = ['j', '', '']
	memory = (t['k'], t['j'], list(t.defaults))
	t.close()
	assert memory == (['k', 'alice', '0'], ['j', 'NONAME', '0'], ['#_defaults_#', 'NONAME', '0'])
	assert open(p, 'rb').read() == b'#id\tname\tscore\n#_defaults_#\tNONAME\t0\nk\talice\nj\tNONAME\t0\n'
	t = TSVZ.TSVZed(p, header='id\tname\tscore')
	assert (t['k'], t['j'], list(t.defaults)) == memory
	t.setDefaults(None)  # a reset is written as a lone marker
	assert t.defaults == ['#_defaults_#'] and t['z'] == ['z', '', '']
	t.close()
	t = TSVZ.TSVZed(p, header='id\tname\tscore')
	assert t.defaults == ['#_defaults_#'] and t['k'] == ['k', 'alice', '0']
	t.close()


def test_lite_spec_set_defaults_is_persisted(tmp_path):
	p = str(tmp_path / 'sd.tsvz')
	lite = TSVZ.TSVZedLite(p, header='id\tname\tscore', strict=False)
	lite.setDefaults(['NONAME', '0'])
	lite['k'] = ['k', 'alice']
	lite['j'] = ['j', '', '']
	memory = (lite['k'], lite['j'], list(lite.defaults))
	lite.close()
	assert memory == (['k', 'alice', '0'], ['j', 'NONAME', '0'], ['#_defaults_#', 'NONAME', '0'])
	lite = TSVZ.TSVZedLite(p, header='id\tname\tscore', strict=False)
	assert (lite['k'], lite['j'], list(lite.defaults)) == memory
	lite.close()
	assert open(p, 'rb').read() == b'#id\tname\tscore\n#_defaults_#\tNONAME\t0\nk\talice\nj\tNONAME\t0\n'


def test_set_defaults_during_construction_is_not_written(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, b'#_defaults_#\tF\na\t1\n')
	t = TSVZ.TSVZed(p, defaults=['X'])  # the file's own #_defaults_# overrides the constructor's
	t.close()
	lite = TSVZ.TSVZedLite(p, defaults=['Y'], strict=False)
	lite.close()
	assert open(p, 'rb').read() == b'#_defaults_#\tF\na\t1\n'


def test_spec_clear_refuses_a_part_path(tmp_path, capsys):
	base = str(tmp_path / 'm.tsvz')
	_touch(base, b'k\tv1\n')
	_touch(base + '.1', b'k\tv2\n')
	_touch(base + '.rotated', b'k\tv0\n')
	TSVZ.clearTabularFile(base + '.1')
	TSVZ.clearTabularFile(base + '.rotated')
	TSVZ.clearTabularFile(base + '.5')  # a part that does not exist is not created
	assert open(base + '.1', 'rb').read() == b'k\tv2\n' and open(base + '.rotated', 'rb').read() == b'k\tv0\n'
	assert not os.path.exists(base + '.5')
	assert TSVZ.readTabularFile(base) == {'k': ['k', 'v2']}
	warnings = [w for w in _tsvz_warnings(capsys) if base in w]
	assert len(warnings) == 3
	assert all('clear skipped: ' in w and 'names one part of a multi-part store; clear the store path instead; nothing was written' in w
			   for w in warnings)


def test_spec_version_marker_with_too_many_digits_does_not_crash(tmp_path, capsys):
	p = str(tmp_path / 'v.tsvz')
	_touch(p, b'#_version_#\t' + b'1' * 5000 + b'\nk\tv\n')
	assert TSVZ.readTabularFile(p) == {'k': ['k', 'v']}
	_, state, reporter = _process(['#_version_#\t' + '1' * 5000])
	reporter.flush()
	warnings = [w for w in _tsvz_warnings(capsys) if w.startswith(('TSVZ warning: t:', 'TSVZ warning: ' + p))]
	limit = getattr(sys, 'get_int_max_str_digits', lambda: 0)()
	if limit and limit < 5000:  # int() refuses it: the marker is ignored and the state kept
		assert state.version == 1
		assert len(warnings) == 2 and all('ignored #_version_# with invalid value' in w for w in warnings)
	else:
		assert len(warnings) == 2 and all('declares spec version' in w for w in warnings)


def test_spec_compressed_repair_never_overwrites_a_backup(tmp_path, monkeypatch, capsys):
	g = str(tmp_path / 'g.tsvz.gz')
	monkeypatch.setattr(TSVZ.time, 'strftime', lambda *args: '20261008T120000')
	synced = []
	realFsync = os.fsync
	me = threading.current_thread()

	def fsync(fd):
		if threading.current_thread() is me:  # not a worker left over from another test
			synced.append(os.fstat(fd).st_ino)
		return realFsync(fd)
	monkeypatch.setattr(TSVZ.os, 'fsync', fsync)
	first = gzip.compress(b'a\t1\n') + gzip.compress(b'b\t2\nbr')[:-6]
	second = gzip.compress(b'a\t1\n') + gzip.compress(b'c\t3\ncr')[:-6]
	for damaged in (first, second):
		_touch(g, damaged)
		reporter = TSVZ._Reporter(g)
		assert TSVZ._specRepairCompressed(g, reporter) is True
		reporter.flush()
	monkeypatch.undo()
	backup = g + '.damaged-20261008T120000'
	assert open(backup, 'rb').read() == first and open(backup + '.1', 'rb').read() == second
	assert synced == [os.stat(backup).st_ino, os.stat(g).st_ino, os.stat(backup + '.1').st_ino, os.stat(g).st_ino]
	warnings = [w for w in _tsvz_warnings(capsys) if g in w]
	assert len(warnings) == 2 and warnings[0].endswith(backup) and warnings[1].endswith(backup + '.1')


def test_lite_spec_write_locks_and_unlocks_at_raw_offset_zero(tmp_path, monkeypatch):
	# msvcrt.locking locks from the raw fd position, so the lock and the unlock in
	# TSVZedLite._specWrite must both start at raw offset 0, even after a read has
	# filled the buffered file object's read buffer from offset 0.
	p = str(tmp_path / 'w.tsvz')
	_touch(p, b'k0\t' + b'v' * 200 + b'\nk1\tx\n')
	positions = []
	real_lock, real_unlock = TSVZ._lockFile, TSVZ._unlockFile

	def lock(f):
		positions.append(('lock', os.lseek(f.fileno(), 0, os.SEEK_CUR)))
		real_lock(f)

	def unlock(f):
		positions.append(('unlock', os.lseek(f.fileno(), 0, os.SEEK_CUR)))
		real_unlock(f)
	monkeypatch.setattr(TSVZ, '_lockFile', lock)
	monkeypatch.setattr(TSVZ, '_unlockFile', unlock)
	lite = TSVZ.TSVZedLite(p, strict=False)
	assert lite['k0'][0] == 'k0'
	lite['k2'] = ['k2', 'y']
	lite.close()
	assert positions == [('lock', 0), ('unlock', 0)]


# ==========================================================================
# CLI (spec §20): parser, logger, writers
# ==========================================================================
def test_cli_parse_canonical_and_file_first_forms():
	P = TSVZ._cliParseArgs
	a = P(['get', 'data.tsvz', 'alice', 'bob'])
	assert (a.operation, a.store, a.args) == ('get', 'data.tsvz', ['alice', 'bob'])
	a = P(['data.tsvz', 'append', 'k', 'v'])
	assert (a.operation, a.store, a.args) == ('append', 'data.tsvz', ['k', 'v'])
	a = P(['data.tsvz'])
	assert (a.operation, a.store, a.args) == ('read', 'data.tsvz', [])
	a = P(['read', './read'])
	assert (a.operation, a.store) == ('read', './read')
	a = P(['./read'])
	assert (a.operation, a.store) == ('read', './read')
	a = P(['set', 'my store.tsvz', 'k', 'v'])
	assert (a.operation, a.store, a.args) == ('set', 'my store.tsvz', ['k', 'v'])


def test_cli_parse_options_anywhere_and_double_dash():
	P = TSVZ._cliParseArgs
	a = P(['-q', 'set', '--format', 'records', 'd.tsvz', 'k', '-v', 'v1', '--x-header=id\\tv'])
	assert a.quiet and a.verbose and a.format == 'records' and a.header == 'id\\tv'
	assert (a.operation, a.store, a.args) == ('set', 'd.tsvz', ['k', 'v1'])
	a = P(['set', 'd.tsvz', 'k', '-5', '-.5', '-'])
	assert a.args == ['k', '-5', '-.5', '-']
	a = P(['set', 'd.tsvz', '--', 'k', '-v', '--format'])
	assert a.args == ['k', '-v', '--format'] and not a.verbose and a.format is None
	assert P(['-dcomma', 'read', 'd.csv']).delimiter == 'comma'
	assert P(['read', 'd.csv', '--delimiter', '|']).delimiter == '|'
	a = P(['-qv', 'read', 'd.tsvz'])
	assert a.quiet and a.verbose
	assert P(['set', 'd.tsvz', '-']).args == ['-']
	assert P(['set', 'd.tsvz', 'k', '']).args == ['k', '']


def test_cli_parse_legacy_aliases():
	P = TSVZ._cliParseArgs
	a = P(['d.tsv', '-c', 'id\\tv', '-s', 'append', 'k', 'v', '--defaults', 'NA'])
	assert a.header == 'id\\tv' and a.strict is True and a.defaults == 'NA'
	assert P(['d.tsv', '-s', '-f', 'read']).strict is False
	assert P(['d.tsv', '--x-strict', 'read']).strict is True
	assert P(['-cid', 'read', 'd.tsv']).header == 'id'


def test_cli_parse_usage_errors():
	P = TSVZ._cliParseArgs
	bad = ([], ['read'], ['frob', 'd.tsvz'], ['d.tsvz', 'frob'], ['read', 'd.tsvz', 'extra'],
		   ['get', 'd.tsvz'], ['set', 'd.tsvz'], ['delete', 'd.tsvz'], ['--bogus', 'read', 'd.tsvz'],
		   ['x-export', 'd.tsvz'], ['read', 'd.tsvz', '--x-bogus'], ['read', 'd.tsvz', '--format', 'json'],
		   ['read', 'd.tsvz', '--format'], ['read', 'd.tsvz', '--quiet=yes'], ['set', 'd.tsvz', '', 'v'],
		   ['get', 'd.tsvz', 'a', ''], ['delete', 'd.tsvz', ''])
	for argv in bad:
		with pytest.raises(TSVZ._CliUsageError):
			P(argv)
	assert P(['-h']).help and P(['read', '--version']).version


def test_cli_logger_and_writers():
	err = io.StringIO()
	log = TSVZ._CliLogger(err)
	log.teelog('w', 'warning')
	log.teelog('i', callerStackDepth=3)
	assert err.getvalue() == 'w\ni\n'
	err = io.StringIO()
	log = TSVZ._CliLogger(err, quiet=True)
	log.teelog('w', 'warning')
	log.teelog('e', 'error')
	log.teelog('i')
	assert err.getvalue() == 'e\n'
	args = TSVZ._CliArgs()
	out = io.StringIO()
	TSVZ._cliEmitRows([['#k', 'a<b', 'x\ty']], args, out, True, '\t')
	assert out.getvalue() == '<#>k\ta<lt>b\tx<sep>y\n'
	out = io.StringIO()
	TSVZ._cliEmitRows([['k', '<sep>']], args, out, False, '\t')
	assert out.getvalue() == 'k\t</sep/>\n'
	out = io.StringIO()
	TSVZ._cliEmitFields([['0', '', 'a\tb.tsvz', 'active']], ['index', 'ordinal', 'path', 'flags'], args, out)
	assert out.getvalue() == '0\t\ta<sep>b.tsvz\tactive\n'
	args.format = 'table'
	out = io.StringIO()
	TSVZ._cliEmitRows([['k', 'v']], args, out, True, '\t')
	assert out.getvalue() == 'k | v\n--+--\n\n'
	assert TSVZ._cliFormat(TSVZ._CliArgs(), io.StringIO()) == 'records'


# ==========================================================================
# CLI (spec §20): operations
# ==========================================================================
def _run(*argv, stdin=''):
	"""Run the CLI in-process; return (exit status, stdout, stderr)."""
	out, err = io.StringIO(), io.StringIO()
	status = TSVZ._cliMain(list(argv), stdin=io.StringIO(stdin), stdout=out, stderr=err)
	return status, out.getvalue(), err.getvalue()


def test_cli_read_and_get_a_spec_store(tmp_path):
	p = str(tmp_path / 'g.tsvz')
	_touch(p, b'#_defaults_#\t\tNA\nalice\tAlice\t30\nbob\tBob\n<#>tag\ta<sep>b\n')
	assert _run('read', p) == (0, 'alice\tAlice\t30\nbob\tBob\tNA\n<#>tag\ta<sep>b\tNA\n', '')
	assert _run(p) == _run('read', p)  # file-first form; read is the default
	assert _run('get', p, 'bob', 'alice  ', '#tag') == (0, 'bob\tBob\tNA\nalice\tAlice\t30\n<#>tag\ta<sep>b\tNA\n', '')
	# Spec §14.5: a missing key prints the key and the active defaults; exit 3.
	assert _run('get', p, 'carol', 'alice') == (3, 'carol\t\tNA\nalice\tAlice\t30\n', '')
	with open(p, 'ab') as f:
		f.write(b'#_return_defaults_when_missing_#\tfalse\n')
	assert _run('get', p, 'carol') == (3, '', '')
	q = str(tmp_path / 'd.tsvz')
	_touch(q, b'a\tx\n')
	assert _run('read', q, '--x-defaults', 'key\\tD1\\tD2') == (0, 'a\tx\tD2\n', '')
	assert _run('read', q, '--defaults', 'key\\tD1\\tD2') == (0, 'a\tx\tD2\n', '')


def test_cli_read_and_get_a_loose_file_and_missing_stores(tmp_path):
	q = str(tmp_path / 'g.csv')
	_touch(q, b'alice,Alice,30\nbob,B<sep>ob,\n')
	assert _run('read', q) == (0, 'alice,Alice,30\nbob,B<sep>ob,\n', '')
	assert _run('get', q, 'bob ', 'nobody') == (3, 'bob,B<sep>ob,\n', '')
	for argv in (['read', 'none.tsvz'], ['get', 'none.tsvz', 'k'], ['scrub', 'none.tsvz'],
				 ['read', 'none.tsv'], ['get', 'none.tsv', 'k'], ['scrub', 'none.tsv']):
		argv[1] = str(tmp_path / argv[1])
		status, out, err = _run(*argv)
		assert (status, out) == (1, '') and 'no such store' in err
	assert sorted(os.listdir(str(tmp_path))) == ['g.csv']


def test_cli_set_append_and_delete(tmp_path):
	p = str(tmp_path / 's.tsvz')
	for argv in (['set', p, 'alice', 'Alice', '30'], ['append', p, 'bob', ''], ['set', p, '#_defaults_#', '', 'NA'],
				 ['set', p, '#tag', 'a\tb', 'x<y'], ['set', p, 'carol', '-5'], ['set', p, 'dave'],
				 ['delete', p, 'alice', 'nobody']):
		status, out, _ = _run(*argv)
		assert (status, out) == (0, '')
	assert open(p, 'rb').read() == (b'alice\tAlice\t30\nbob\t\n#_defaults_#\t\tNA\n<#>tag\ta<sep>b\tx<lt>y\n'
									b'carol\t-5\ndave\nalice\nnobody\n')
	assert _run('read', p) == (0, 'bob\t\t\n<#>tag\ta<sep>b\tx<lt>y\ncarol\t-5\tNA\n', '')
	fresh = str(tmp_path / 'fresh.tsvz')
	assert _run('delete', fresh, 'k')[0] == 0  # delete creates a missing store (spec §20.2.2)
	assert open(fresh, 'rb').read() == b'k\n'
	q = str(tmp_path / 's.tsv')
	assert _run(q, 'append', 'k', 'v', '-c', 'id\\tval')[0] == 0  # 3.39 file-first form and -c
	assert _run(q, 'delete', 'k', '-c', 'id\\tval')[0] == 0
	assert open(q, 'rb').read() == b'id\tval\nk\tv\nk\t\n'


def test_cli_store_paths_and_values_that_look_like_options(tmp_path, monkeypatch):
	"""Review focus 2 and 3: values after '--', paths with spaces, a store named like an operation."""
	monkeypatch.chdir(str(tmp_path))
	assert _run('set', 'my store.tsvz', 'k', 'v')[0] == 0
	assert _run('set', 'my store.tsvz', '--', 'e', '-v', '--x')[0] == 0
	assert _run('read', 'my store.tsvz') == (0, 'k\tv\ne\t-v\t--x\n', '')
	assert _run('set', './read', 'k', 'v')[0] == 0
	# [:2]: 3.39 warns on stderr that the name does not end with .tsv
	assert _run('./read')[:2] == (0, 'k\tv\n')
	assert _run('read', './read')[:2] == (0, 'k\tv\n')


def test_cli_clear_and_scrub(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, b'#id\tval\n#_strip_trailing_whites_#\tfalse\na\t1\nb\t2\na\n')
	assert _run('scrub', p) == (0, '', '')
	assert open(p, 'rb').read() == b'#id\tval\n#_version_#\t1\n#_strip_trailing_whites_#\tfalse\nb\t2\n'
	assert _run('clear', p) == (0, '', '')
	assert open(p, 'rb').read() == b'#id\tval\n#_strip_trailing_whites_#\tfalse\n'
	fresh = str(tmp_path / 'fresh.tsvz')
	assert _run('clear', fresh)[0] == 0 and open(fresh, 'rb').read() == b''
	m = str(tmp_path / 'm.tsvz')
	_touch(m, b'a\t1\n')
	_touch(m + '.1', b'b\t2\n')
	status, out, err = _run('scrub', m)  # 4.1 refuses to compact a multi-part store
	assert (status, out) == (1, '') and 'scrub skipped' in err and 'scrub refused' in err
	assert open(m, 'rb').read() == b'a\t1\n' and open(m + '.1', 'rb').read() == b'b\t2\n'
	assert _run('-q', 'scrub', m) == (1, '', 'tsvz: {}: scrub refused; nothing was written\n'.format(m))
	status, _, err = _run('clear', m + '.1')  # one part of a multi-part store
	assert status == 1 and 'clear skipped' in err
	assert _run('clear', m)[0] == 0  # multi-part: a tombstone for every live key, appended
	assert open(m + '.1', 'rb').read() == b'b\t2\na\nb\n'


def test_cli_usage_errors_help_and_version(tmp_path):
	p = str(tmp_path / 'u.tsvz')
	for argv in (['frob', p], ['x-export', p], ['read', p, '--x-bogus'], ['--format', 'json', 'read', p],
				 ['get', p], ['set', p, '', 'v']):
		r = _cli(*argv)
		assert (r.returncode, r.stdout) == (2, '') and 'usage: tsvz' in r.stderr
	assert not os.path.exists(p)
	r = _cli('-h')
	assert (r.returncode, r.stderr) == (0, '') and r.stdout.startswith('usage: tsvz')
	r = _cli('read', p, '--version')
	assert r.returncode == 0 and '4.1' in r.stdout


def test_cli_streams_and_verbosity(tmp_path):
	p = str(tmp_path / 'o.tsvz')
	r = _cli('set', p, 'k', 'v')
	assert (r.returncode, r.stdout) == (0, '') and 'Created' in r.stderr
	r = _cli('-q', 'set', p, 'j', 'w')
	assert (r.returncode, r.stdout, r.stderr) == (0, '', '')
	r = _cli('-v', 'set', p, 'i', 'x')
	assert (r.returncode, r.stdout) == (0, '') and 'Appended 1 lines' in r.stderr
	with open(p, 'ab') as f:
		f.write(b'torn')
	r = _cli('read', p)
	assert (r.returncode, r.stdout) == (0, 'k\tv\nj\tw\ni\tx\n') and 'TSVZ warning' in r.stderr and 'torn' in r.stderr
	r = _cli('-q', 'read', p)
	assert (r.returncode, r.stdout, r.stderr) == (0, 'k\tv\nj\tw\ni\tx\n', '')
	status, out, err = _run('read', p, '-d', 'comma')  # in-process: the warning text is not ASCII
	assert (status, out) == (0, 'k\tv\nj\tw\ni\tx\n') and 'conflicts with the file extension' in err


def test_cli_output_into_a_closed_pipe(tmp_path):
	"""Review focus 5: a reader that closes the pipe early gets no traceback; the status is 1."""
	p = str(tmp_path / 'big.tsvz')
	_touch(p, b''.join(b'k%d\tv%d\n' % (i, i) for i in range(200000)))
	proc = subprocess.Popen([sys.executable, os.path.join(HERE, 'TSVZ.py'), 'read', p],
							stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	first = proc.stdout.readline()
	proc.stdout.close()
	err = proc.stderr.read()
	proc.wait()
	assert first == b'k0\tv0\n'
	assert proc.returncode == 1 and b'Traceback' not in err and b'Exception ignored' not in err


@pytest.mark.skipif(os.name != 'posix', reason='needs a pseudo-terminal')
def test_cli_prints_a_table_on_a_terminal(tmp_path):
	"""Spec §20.4.3: without --format, a terminal gets the table and a pipe gets records."""
	import pty
	p = str(tmp_path / 't.tsvz')
	_touch(p, b'alice\tAlice\t30\n')
	master, slave = pty.openpty()
	try:
		proc = subprocess.Popen([sys.executable, os.path.join(HERE, 'TSVZ.py'), 'read', p],
								stdout=slave, stderr=subprocess.DEVNULL)
		os.close(slave)
		out = b''
		while True:
			try:
				chunk = os.read(master, 4096)
			except OSError:  # EIO: the child closed the terminal
				break
			if not chunk:
				break
			out += chunk
		proc.wait()
	finally:
		os.close(master)
	assert proc.returncode == 0
	assert b'Alice' in out and b'|' in out and b'\t' not in out
	assert _cli('read', p).stdout == 'alice\tAlice\t30\n'


@pytest.mark.skipif(os.name != 'posix', reason='POSIX locales')
def test_cli_non_ascii_under_the_c_locale(tmp_path):
	"""Review focus 1: UTF-8 arguments, files and output survive LANG=C (ASCII argv and stdio on Python 3.6).

	PYTHONUTF8=0 and PYTHONCOERCECLOCALE=0 give Python 3.7+ the 3.6 behaviour.
	"""
	env = dict(os.environ, LANG='C', LC_ALL='C', PYTHONUTF8='0', PYTHONCOERCECLOCALE='0')
	env.pop('PYTHONIOENCODING', None)
	p = str(tmp_path / 'u.tsvz')

	def run(*args):
		argv = [sys.executable, os.path.join(HERE, 'TSVZ.py')] + [a.encode('utf-8') for a in args]
		return subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
	r = run('set', p, 'é', '中', '\U0001F600')
	assert r.returncode == 0 and b'Traceback' not in r.stderr
	assert open(p, 'rb').read() == 'é\t中\t\U0001F600\n'.encode('utf-8')
	r = run('get', p, 'é')
	assert (r.returncode, r.stdout) == (0, 'é\t中\t\U0001F600\n'.encode('utf-8'))
	r = run('read', p, '--format', 'table')
	assert r.returncode == 0 and '中'.encode('utf-8') in r.stdout and b'Traceback' not in r.stderr


# ==========================================================================
# CLI (spec §20.3): bulk input
# ==========================================================================
def test_cli_bulk_set_and_delete(tmp_path):
	p = str(tmp_path / 'b.tsvz')
	data = ('# a comment\n'
			'\n'
			'alice\tAlice\t30\n'
			'<#>_x_#\tdata, not a marker\n'
			'#_defaults_#\t\tNA\n'
			'bob\n'
			'carol\ta<sep>b\tx<lt>y\n'
			'\tno key\n'
			'dave\tunterminated')
	status, out, err = _run('set', p, '-', stdin=data)
	assert (status, out) == (0, '')
	assert open(p, 'rb').read() == (b'alice\tAlice\t30\n<#>_x_#\tdata, not a marker\n#_defaults_#\t\tNA\nbob\n'
									b'carol\ta<sep>b\tx<lt>y\n')
	assert 'unterminated' in err and 'empty key' in err
	status, out, err = _run('delete', p, '-', stdin='alice\tignored\n# comment\n<#>_x_#\n#_defaults_#\n')
	assert (status, out, err) == (0, '', '')
	assert open(p, 'rb').read().endswith(b'carol\ta<sep>b\tx<lt>y\nalice\n<#>_x_#\n#_defaults_#\n')
	assert _run('read', p) == (0, 'carol\ta<sep>b\tx<lt>y\n', '')
	q = str(tmp_path / 'b.csv')
	assert _run('set', q, '-', stdin='k,a<sep>b\n# c\nj,x\n')[0] == 0
	assert open(q, 'rb').read() == b'\nk,a<sep>b\nj,x\n'  # 3.39 starts a header-less file with '\n'
	assert _run('delete', q, '-', stdin='k,whatever\n')[0] == 0
	assert _run('read', q) == (0, 'j,x\n', '')
	fresh = str(tmp_path / 'fresh.tsvz')
	assert _run('set', fresh, '-', stdin='')[0] == 0 and open(fresh, 'rb').read() == b''


def test_cli_bulk_set_is_one_batch(tmp_path, monkeypatch):
	"""Review focus 4: 10 000 records on stdin reach the store in one append (spec §18.2, §20.3)."""
	p = str(tmp_path / 'big.tsvz')
	payloads = []
	real = TSVZ._specAppendPayload

	def spy(path, payload, reporter, **kwargs):
		payloads.append(payload)
		return real(path, payload, reporter, **kwargs)
	monkeypatch.setattr(TSVZ, '_specAppendPayload', spy)
	data = ''.join('k{}\tv{}\n'.format(i, i) for i in range(10000))
	assert _run('set', p, '-', stdin=data)[0] == 0
	assert len(payloads) == 1 and payloads[0] == data.encode()
	assert open(p, 'rb').read() == data.encode()
	assert _run('delete', p, '-', stdin=data)[0] == 0
	assert len(payloads) == 2 and _run('read', p) == (0, '', '')


@pytest.mark.parametrize('ext', ['tsvz', 'csvz', 'nsvz', 'psvz'])
def test_cli_read_piped_into_set_copies_the_store(tmp_path, ext):
	"""Spec §20.3.2: `tsvz read A | tsvz set B -` copies every live row of a same-variant store."""
	src = str(tmp_path / ('a.' + ext))
	dst = str(tmp_path / ('b.' + ext))
	rows = [['alice', 'A, l|i\tce', '30'], ['#hash', '<sep>', 'multi\nline'], ['k', '', 'x<y'],
			['nul', 'a\0b', 'é 中 \U0001F600'], ['#_x_#', 'a key, not a marker', '']]
	TSVZ.appendLinesTabularFile(src, rows[:4], createIfNotExist=True)
	with open(src, 'ab') as f:  # a '#_x_#' data key can only be written in its encoded form
		f.write(TSVZ._specFormatRecord(rows[4], TSVZ._EXTENSION_DELIMITERS[ext]).encode('utf-8') + b'\n')
	status, out, _ = _run('read', src)
	assert status == 0
	assert _run('set', dst, '-', stdin=out)[0] == 0
	expected = list(TSVZ.readTabularFile(src, verifyHeader=False).items())
	assert '#_x_#' in dict(expected)
	assert list(TSVZ.readTabularFile(dst, verifyHeader=False).items()) == expected


# ==========================================================================
# CLI (spec §20.2): verify and parts
# ==========================================================================
def _crc32(data):
	import zlib
	return '%08x' % (zlib.crc32(data) & 0xffffffff)


def test_cli_verify(tmp_path):
	p = str(tmp_path / 'v.tsvz')
	_touch(p, ('#_checksum_crc32_#\na\t1\nb\t2\n#_checksum_crc32_#\t' + _crc32(b'a\t1\nb\t2\n') + '\n'
			   'c\t3\n#_checksum_crc32_#\tdeadbeef\n').encode())
	assert _run('verify', p) == (4, '{}\t6\tcrc32\tdeadbeef\t{}\n'.format(p, _crc32(b'c\t3\n')), '')
	status, out, _ = _run('verify', p, '--format', 'table')
	assert status == 4 and 'expected' in out and 'deadbeef' in out
	m = str(tmp_path / 'm.tsvz')  # a digest runs across parts (spec §17.4); lines count per part
	_touch(m, b'#_checksum_crc32_#\na\t1\n')
	_touch(m + '.1', b'b\t2\n#_checksum_crc32_#\t00000000\n')
	assert _run('verify', m) == (4, '{}.1\t2\tcrc32\t00000000\t{}\n'.format(m, _crc32(b'a\t1\nb\t2\n')), '')
	clean = str(tmp_path / 'clean.tsvz')
	_touch(clean, ('#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\t' + _crc32(b'a\t1\n') + '\n').encode())
	assert _run('verify', clean) == (0, '', '')
	plain = str(tmp_path / 'plain.tsvz')
	_touch(plain, b'a\t1\n')
	assert _run('verify', plain) == (0, '', '')  # no markers: nothing to check (spec §20.2.3)
	loose = str(tmp_path / 'loose.tsv')
	_touch(loose, b'#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\t00000000\n')
	assert _run('verify', loose) == (0, '', '')  # a loose file has no checksums (spec §20.2.4)
	odd = str(tmp_path / 'odd.tsvz')
	_touch(odd, b'#_checksum_nosuchalgo_#\na\t1\n#_checksum_nosuchalgo_#\tff\n')
	status, out, err = _run('verify', odd)
	assert (status, out) == (0, '') and 'not verified' in err
	status, out, err = _run('verify', str(tmp_path / 'none.tsvz'))
	assert (status, out) == (1, '') and 'no such store' in err


def test_cli_parts(tmp_path):
	base = str(tmp_path / 'e.tsvz')
	_touch(base, b'a\t1\n')
	_touch(base + '.0190c3a1', b'b\t2\n')
	_touch(base + '.0a.gz', gzip.compress(b'c\t3\n'))
	_touch(base + '.2.rotated', b'')
	assert _run('parts', base) == (0, '0\t\t{0}\t\n1\t0a\t{0}.0a.gz\tgz\n2\t0190c3a1\t{0}.0190c3a1\tactive\n'.format(base), '')
	status, out, _ = _run('parts', base, '--format', 'table')
	assert status == 0 and 'ordinal' in out and '|' in out
	numbered = str(tmp_path / 'n.tsvz')
	_touch(numbered + '.1', b'k\tv\n')
	assert _run('parts', numbered) == (0, '0\t1\t{}.1\tactive\n'.format(numbered), '')
	loose = str(tmp_path / 'x.tsv')
	_touch(loose, b'k\tv\n')
	assert _run('parts', loose) == (0, '0\t\t{}\tactive\n'.format(loose), '')
	packed = str(tmp_path / 'y.tsv.gz')
	_touch(packed, gzip.compress(b'k\tv\n'))
	assert _run('parts', packed) == (0, '0\t\t{}\tactive,gz\n'.format(packed), '')
	status, out, err = _run('parts', str(tmp_path / 'none.tsvz'))
	assert (status, out) == (1, '') and 'no such store' in err


# ==========================================================================
# CLI: final-review fixes
# ==========================================================================
def test_cli_write_that_cannot_create_the_store_fails(tmp_path):
	"""I1: a write whose store cannot be created exits 1 with an error that -q keeps (spec §20.6)."""
	(tmp_path / 'd.tsv').mkdir()
	for store in (str(tmp_path / 'nodir' / 's.tsvz'), str(tmp_path / 'nodir' / 's.tsv'), str(tmp_path / 'd.tsv')):
		for argv, stdin in ((['set', store, 'k', 'v'], ''), (['append', store, 'k'], ''), (['delete', store, 'k'], ''),
							(['set', store, '-'], 'k\tv\n'), (['delete', store, '-'], 'k\n')):
			status, out, err = _run('-q', *argv, stdin=stdin)
			assert (status, out) == (1, '') and 'could not be created' in err, (store, argv)
	assert not os.path.exists(str(tmp_path / 'nodir'))


def test_cli_unreadable_part_fails(tmp_path, monkeypatch):
	"""I2: read, get and verify exit 1 when a part cannot be read; the other parts' rows are still printed."""
	m = str(tmp_path / 'm.tsvz')
	_touch(m, b'a\t1\n')
	os.mkdir(m + '.1')  # a numbered part that cannot be opened, even by root
	for argv, rows in ((['read', m], 'a\t1\n'), (['get', m, 'a'], 'a\t1\n'), (['verify', m], '')):
		status, out, err = _run('-q', *argv)
		assert (status, out) == (1, rows) and 'could not read' in err, argv
	p = str(tmp_path / 'p.tsvz')
	_touch(p, b'a\t1\n')
	real = open

	def deny(path, *args, **kwargs):
		if path == p:
			raise PermissionError(13, 'Permission denied', path)
		return real(path, *args, **kwargs)
	monkeypatch.setattr(TSVZ, 'open', deny, raising=False)
	status, out, err = _run('read', p)
	assert (status, out) == (1, '') and 'Permission denied' in err and 'could not read' in err


@pytest.mark.skipif(os.name != 'posix', reason='POSIX locales')
def test_cli_non_ascii_store_name_under_the_c_locale(tmp_path):
	"""I3: a non-ASCII STORE path keeps working where the locale is ASCII (Python 3.6 under LANG=C)."""
	env = dict(os.environ, LANG='C', LC_ALL='C', PYTHONUTF8='0', PYTHONCOERCECLOCALE='0')
	env.pop('PYTHONIOENCODING', None)
	store = os.path.join(str(tmp_path), 'stör.tsvz')

	def run(*args):
		argv = [sys.executable, os.path.join(HERE, 'TSVZ.py')] + [a.encode('utf-8') for a in args]
		return subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
	r = run('set', store, 'k', 'é')
	assert r.returncode == 0 and b'Traceback' not in r.stderr
	assert os.path.exists(store.encode('utf-8'))
	r = run('get', store, 'k')
	assert (r.returncode, r.stdout) == (0, 'k\té\n'.encode('utf-8'))
	r = run('parts', store)
	assert (r.returncode, r.stdout) == (0, b'0\t\t' + store.encode('utf-8') + b'\tactive\n')


def test_cli_parse_short_option_equals_and_long_prefixes():
	"""I4, I5: 3.39's argparse spellings -d=, and unique long-option prefixes still parse."""
	P = TSVZ._cliParseArgs
	assert P(['-d=,', 'read', 'x.csv']).delimiter == ','
	assert P(['read', 'x.tsv', '-c=id']).header == 'id'
	a = P(['--verb', '--delim', 'comma', '--head=id', '--form', 'records', 'read', 'x.csv'])
	assert a.verbose and a.delimiter == 'comma' and a.header == 'id' and a.format == 'records'
	assert P(['--x-s', 'read', 'x.tsv']).strict is True
	for argv in (['--he', 'read', 'x.tsv'], ['--ver', 'read', 'x.tsv'], ['--de', ',', 'read', 'x.tsv'],
				 ['--x-', 'read', 'x.tsv'], ['--bogus', 'read', 'x.tsv']):
		with pytest.raises(TSVZ._CliUsageError):
			P(argv)


def test_cli_delimiter_option_is_checked(tmp_path):
	"""I4: -d=, writes commas; a long -d is warned about; an undecodable -d is a usage error."""
	y = str(tmp_path / 'y.txt')
	assert _run('-d=,', y, 'append', 'a', 'b')[0] == 0
	assert open(y, 'rb').read().endswith(b'a,b\n')
	status, out, err = _run('read', y, '-d', '::')
	assert status == 0 and 'one character' in err
	status, out, err = _run('read', y, '-d', '\\')
	assert (status, out) == (2, '') and 'usage: tsvz' in err


# ==========================================================================
# Write handler (spec §21): library helpers
# ==========================================================================
def test_part_info_counts_committed_bytes(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, b'a\t1\nb\t2\ntorn')
	info = TSVZ._PartInfo(p)
	reporter = TSVZ._Reporter(p)
	assert [raw for raw, _ in TSVZ._iterPartLines(info, reporter)] == [b'a\t1\n', b'b\t2\n']
	assert (info.committed, info.tail) == (8, b'torn')
	assert TSVZ._specLoad(p, '\t', reporter=TSVZ._Reporter(p)).infos[0].lines == 2


def test_spec_replay_line_matches_spec_replay(tmp_path):
	p = str(tmp_path / 'r.tsvz')
	good = '%08x' % (__import__('zlib').crc32(b'a\t1\n') & 0xffffffff)
	_touch(p, ('\ufeff#_defaults_#\t\tD\n#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\t' + good + '\n'
			   '<#>h\tx<sep>y\nb\n#c\n\xe9\t2\n').encode('utf-8') + b'bad\xff\t3\n')
	whole = TSVZ._SpecState()
	expected = [(text, record) for _, _, text, record in
				TSVZ._specReplay([p], '\t', whole, TSVZ._Reporter(p), [])]
	single = TSVZ._SpecState()
	reporter = TSVZ._Reporter(p)
	got = []
	for lineNo, (raw, _) in enumerate(TSVZ._iterPartLines(TSVZ._PartInfo(p), reporter), 1):
		replayed = TSVZ._specReplayLine(raw, lineNo, '', p, single, '\t', reporter)
		if replayed is not None:
			got.append(replayed)
	assert got == expected
	assert single.defaults == whole.defaults == ['#_defaults_#', '', 'D']
	assert single.mismatches == whole.mismatches == []


def test_spec_pad_row():
	state = TSVZ._SpecState(['#_defaults_#', 'x', 'y'])
	row = ['k', '1']
	assert TSVZ._specPadRow(row, state, -1) == 3 and row == ['k', '1', 'y']
	row = ['k', '1', '2', '3']
	assert TSVZ._specPadRow(row, state, 3) == 3 and row == ['k', '1', '2', '3']


def test_legacy_read_loop_reports_width_and_committed_offset(tmp_path):
	p = str(tmp_path / 'l.tsv')
	_touch(p, b'k\tv\nj\tw\nlast\tx')
	data = OrderedDict()
	with open(p, 'rb') as f:
		width, committed, lines = TSVZ._legacyReadLoop(f, p, data, -1, 0, False, '\t', [], False, 'utf8', TSVZ._Reporter(p))
	assert (width, committed, lines) == (2, 8, 3)
	assert list(data) == ['k', 'j', 'last']  # 3.39 reads an unterminated last line as a record


def test_legacy_format_payload_is_what_append_writes(tmp_path):
	p = str(tmp_path / 'f.tsv')
	rows = [['k', 'a\tb'], ['j'], ['i', 'x', 'y']]
	payload, count = TSVZ._legacyFormatPayload(p, rows, None, [''], True, False, 'utf8', False, '\t')
	TSVZ.appendLinesTabularFile(p, rows, createIfNotExist=True)
	assert count == 3 and open(p, 'rb').read() == b'\n' + payload
	assert payload == b'k\ta<sep>b\t\nj\t\t\ni\tx\ty\n'


@pytest.mark.skipif(os.name != 'posix', reason='flock')
def test_try_lock_file_excludes_a_second_handle(tmp_path):
	p = str(tmp_path / 'x.serve')
	first = open(p, 'a+b')
	second = open(p, 'a+b')
	try:
		assert TSVZ._tryLockFile(first) is True
		assert TSVZ._tryLockFile(second) is False
		open(p, 'rb').close()  # closing another descriptor keeps a flock
		assert TSVZ._tryLockFile(second) is False
	finally:
		first.close()
	assert TSVZ._tryLockFile(second) is True
	second.close()


def test_parse_record_lines():
	reporter = TSVZ._Reporter('<test>')
	texts = ['# comment', '', 'k\tv<sep>w', '<#>_x_#\tdata', '#_defaults_#\t\tD<lt>', '\tno key', 'j']
	assert TSVZ._parseRecordLines(texts, '\t', True, False, reporter) == [
		(['k', 'v\tw'], False), (['#_x_#', 'data'], False), (['#_defaults_#', '', 'D<'], True), (['j'], False)]
	assert TSVZ._parseRecordLines(texts, '\t', True, True, reporter) == [
		(['k'], False), (['#_x_#'], False), (['#_defaults_#'], True), (['j'], False)]
	assert TSVZ._parseRecordLines(['k,a<sep>b', '#c', 'j,x'], ',', False, False, reporter) == [
		(['k', 'a,b'], False), (['j', 'x'], False)]
	assert reporter._events['empty-key'][0] == 2


# ==========================================================================
# Write handler (spec §21): the store engine
# ==========================================================================
def _view(path):
	"""Rows a direct reader returns for ``path``, in order (what ``tsvz read`` prints)."""
	return [list(row) for row in TSVZ.readTabularFile(path, verifyHeader=False, strict=False).values()]


@pytest.fixture
def engines():
	"""Open ``_ServeStore``s through this fixture so their writer threads stop."""
	opened = []

	def make(path, **kwargs):
		store = TSVZ._ServeStore(path, **kwargs)
		opened.append(store)
		return store
	yield make
	for store in opened:
		store.close()


@pytest.mark.parametrize('name', ['e.tsvz', 'e.psvz', 'e.tsv', 'e.csv'])
def test_engine_follows_direct_appends(tmp_path, engines, name):
	p = str(tmp_path / name)
	d = TSVZ._EXTENSION_DELIMITERS[name.rpartition('.')[2]].encode()
	_touch(p, b'a' + d + b'1\nb' + d + b'2\n')
	store = engines(p)
	assert store.read(None) == _view(p) == [['a', '1'], ['b', '2']]
	TSVZ.appendLinesTabularFile(p, [['c', '3'], ['a']])
	assert store.read(None) == _view(p) == [['b', '2'], ['c', '3']]
	TSVZ.appendLinesTabularFile(p, [['a', '9'], ['b', 'x' + d.decode() + 'y']])
	assert store.read(None) == _view(p)
	assert store.keys(None) == ['b', 'c', 'a'] and store.length(None) == 3
	assert store.has('a', None) and not store.has('z', None)


def test_engine_ignores_a_torn_tail_until_it_is_completed(tmp_path, engines, capsys):
	p = str(tmp_path / 't.tsvz')
	_touch(p, b'a\t1\n')
	store = engines(p)
	with open(p, 'ab') as f:
		f.write(b'b\t2')
	assert store.read(None) == [['a', '1']]
	assert store.read(None) == [['a', '1']]
	assert capsys.readouterr().err.count('ignored uncommitted bytes') == 1  # noted once, not per request
	with open(p, 'ab') as f:
		f.write(b'2\n')
	assert store.read(None) == _view(p) == [['a', '1'], ['b', '22']]


def test_engine_reloads_when_the_store_changes_other_than_by_appending(tmp_path, engines):
	p = str(tmp_path / 'r.tsvz')
	_touch(p, b'a\t1\nb\t2\na\n')
	store = engines(p)
	assert store.read(None) == [['b', '2']]
	TSVZ.scrubTabularFile(p, verifyHeader=False)  # shrinks the file in place
	TSVZ.appendTabularFile(p, ['c', '3'])
	assert store.read(None) == _view(p) == [['b', '2'], ['c', '3']]
	other = str(tmp_path / 'other')
	_touch(other, b'x\t1\ny\t2\n')
	os.replace(other, p)  # a new inode
	assert store.read(None) == [['x', '1'], ['y', '2']]
	with open(p, 'r+b') as f:  # rewritten in place, same size
		f.write(b'q\t7\nz\t8\n')
	st = os.stat(p)  # file times can be coarser than these steps; make the rewrite visible to stat()
	os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 10 ** 9))
	assert store.read(None) == [['q', '7'], ['z', '8']]
	_touch(p + '.1', b'n\t1\n')  # a new part
	assert store.read(None) == _view(p) == [['q', '7'], ['z', '8'], ['n', '1']]


def test_engine_follows_compressed_and_multi_part_stores(tmp_path, engines):
	g = str(tmp_path / 'g.tsvz.gz')
	_touch(g, gzip.compress(b'a\t1\n'))
	store = engines(g)
	TSVZ.appendTabularFile(g, ['b', '2'])
	assert store.read(None) == _view(g) == [['a', '1'], ['b', '2']]
	m = str(tmp_path / 'm.tsvz')
	_touch(m, b'a\t1\n')
	_touch(m + '.1', b'b\t2\n')
	store = engines(m)
	TSVZ.appendTabularFile(m, ['c', '3'])  # goes to the active part, m.tsvz.1
	assert store.read(None) == _view(m) == [['a', '1'], ['b', '2'], ['c', '3']]
	assert open(m + '.1', 'rb').read() == b'b\t2\nc\t3\n'


def test_engine_sees_a_new_part_within_one_directory_timestamp(tmp_path, engines, monkeypatch):
	"""A part created in the same timestamp tick as the last directory change is still found."""
	m = str(tmp_path / 'n.tsvz')
	_touch(m, b'a\t1\n')
	frozen = TSVZ._dirMtimeNs(m)
	monkeypatch.setattr(TSVZ, '_dirMtimeNs', lambda path: frozen)  # the new part leaves the mtime as it was
	store = engines(m)
	_touch(m + '.1', b'b\t2\n')
	assert store.read(None) == [['a', '1'], ['b', '2']]

def test_engine_get_resolves_keys_and_missing_rows(tmp_path, engines):
	p = str(tmp_path / 'g.tsvz')
	_touch(p, b'#_defaults_#\t\tNA\nalice\tAlice\t30\nbob\tBob\n')
	store = engines(p)
	assert store.get(['bob', 'alice  ', 'carol'], None) == ([['bob', 'Bob', 'NA'], ['alice', 'Alice', '30'],
															['carol', '', 'NA']], True)
	assert store.get(['alice'], None) == ([['alice', 'Alice', '30']], False)


def test_engine_writes_what_the_direct_writers_write(tmp_path, engines):
	for name in ('w.tsvz', 'w.tsv'):
		direct, served = str(tmp_path / ('d' + name)), str(tmp_path / ('s' + name))
		store = engines(served, create=True)
		_touch(direct, open(served, 'rb').read())
		rows = [(['k', 'a\tb', ''], False), (['#tag', 'x<y'], False), (['#_defaults_#', '', 'D'], True), (['gone'], False)]
		store.write(rows, False, None)
		TSVZ.appendLinesTabularFile(direct, [cells for cells, marker in rows])
		assert store.read(None) == _view(direct)  # read-your-writes: the read waits for the queued batch
		assert open(served, 'rb').read() == open(direct, 'rb').read()


def test_engine_acknowledges_disk_writes_after_fsync(tmp_path, engines, monkeypatch):
	synced = []
	real = os.fsync
	monkeypatch.setattr(os, 'fsync', lambda fd: (synced.append(fd), real(fd)))
	p = str(tmp_path / 'a.tsvz')
	_touch(p, b'a\t1\n')
	store = engines(p)
	store.write([(['b', '2'], False)], False, None)  # memory: no fsync needed to answer
	store.write([(['c', '3'], False)], True, None)  # --sync
	assert synced
	synced[:] = []
	store.write([(['#_write_ack_#', 'disk'], True)], False, None)
	store.read(None)
	store.write([(['d', '4'], False)], False, None)
	assert synced  # the store's own marker asks for disk


def test_engine_pop_popitem_and_setdefault(tmp_path, engines):
	p = str(tmp_path / 'p.tsvz')
	_touch(p, b'a\t1\nb\t2\nc\t3\n')
	store = engines(p)
	assert store.pop('b', False, None) == ['b', '2'] and store.pop('b', False, None) is None
	assert store.popitem(True, False, None) == ['c', '3'] and store.popitem(False, False, None) == ['a', '1']
	assert store.popitem(True, False, None) is None
	assert store.setdefault(['k', 'v'], False, None) == ['k', 'v']
	assert store.setdefault(['k', 'other'], False, None) == ['k', 'v']
	assert open(p, 'rb').read() == b'a\t1\nb\t2\nc\t3\nb\nc\na\nk\tv\n'


def test_engine_pop_is_atomic_under_concurrency(tmp_path, engines):
	p = str(tmp_path / 'race.tsvz')
	_touch(p, b''.join(b'k%d\t%d\n' % (i, i) for i in range(200)))
	store = engines(p)
	popped = []

	def worker():
		while True:
			row = store.popitem(True, False, None)
			if row is None:
				return
			popped.append(row[0])
	threads = [threading.Thread(target=worker) for _ in range(8)]
	for t in threads:
		t.start()
	for t in threads:
		t.join()
	assert sorted(popped) == sorted('k%d' % i for i in range(200))
	assert store.read(None) == [] == _view(p)


def test_engine_clear_scrub_verify_and_parts(tmp_path, engines):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, b'#id\tval\na\t1\nb\t2\na\n')
	store = engines(p)
	assert store.scrub(None) is True and open(p, 'rb').read() == b'#id\tval\n#_version_#\t1\nb\t2\n'
	assert store.read(None) == [['b', '2']]
	assert store.clear(None) is True and store.read(None) == []
	m = str(tmp_path / 'm.tsvz')
	_touch(m, b'a\t1\n')
	_touch(m + '.1', b'#_checksum_crc32_#\nb\t2\n#_checksum_crc32_#\t00000000\n')
	store = engines(m)
	assert store.scrub(None) is False  # 4.1 refuses to compact a multi-part store
	mismatches, readable = store.verify(None)
	assert readable and [entry[:3] for entry in mismatches] == [(m + '.1', 3, 'crc32')]
	assert store.partRows(None) == [['0', '', m, ''], ['1', '1', m + '.1', 'active']]


def test_engine_reports_an_unreadable_part(tmp_path, engines):
	m = str(tmp_path / 'u.tsvz')
	_touch(m, b'a\t1\n')
	os.mkdir(m + '.1')
	store = engines(m)
	assert store.unreadable and store.read(None) == [['a', '1']]


def test_engine_close_writes_and_fsyncs_queued_writes(tmp_path, monkeypatch):
	synced = []
	real = os.fsync
	monkeypatch.setattr(os, 'fsync', lambda fd: (synced.append(fd), real(fd)))
	p = str(tmp_path / 'q.tsvz')
	store = TSVZ._ServeStore(p, create=True)
	for i in range(50):
		store.write([(['k%d' % i, 'v'], False)], False, None)
	store.close()
	assert synced and _view(p) == [['k%d' % i, 'v'] for i in range(50)]

def test_engine_survives_the_store_being_deleted(tmp_path, engines):
	"""Review focus 4: a store removed under the server is recreated by the next write."""
	p = str(tmp_path / 'x.tsvz')
	_touch(p, b'a\t1\n')
	store = engines(p)
	os.remove(p)
	assert store.read(None) == []
	store.write([(['b', '2'], False)], False, None)
	assert store.read(None) == _view(p) == [['b', '2']] and open(p, 'rb').read() == b'b\t2\n'


# ==========================================================================
# Write handler (spec §21): requests and responses
# ==========================================================================
def test_serve_responses(tmp_path, engines):
	p = str(tmp_path / 'r.tsvz')
	_touch(p, b'#_defaults_#\t\tNA\nalice\tAlice\t30\n<#>tag\ta<sep>b\tx\n')
	store = engines(p)
	R = lambda line, bulk=None: TSVZ._serveRespond(store, line, bulk)
	assert R('read') == ['alice\tAlice\t30', '<#>tag\ta<sep>b\tx', '#0']
	assert R('get\talice\tcarol') == ['alice\tAlice\t30', 'carol\t\tNA', '#3']
	assert R('get\t#tag') == ['<#>tag\ta<sep>b\tx', '#0'] == R('get\t<#>tag')
	assert R('has\talice') == ['#0'] and R('has\tnobody') == ['#3']
	assert R('len') == ['2', '#0'] and R('keys') == ['alice', '<#>tag', '#0']
	assert R('set\tbob\tB<lt>b\tline<LF>two') == ['#0']
	assert R('get\tbob') == ['bob\tB<lt>b\tline<LF>two', '#0']
	assert R('setdefault\tbob\tother') == ['bob\tB<lt>b\tline<LF>two', '#0']
	assert R('pop\tbob') == ['bob\tB<lt>b\tline<LF>two', '#0'] and R('pop\tbob') == ['#3']
	assert R('popitem\tfirst') == ['alice\tAlice\t30', '#0']
	assert R('set\t-', ['# comment', 'k\tv', '<#>_x_#\tdata', '#_defaults_#\t\tD']) == ['#0']
	# rows bind the defaults active where they are written: the marker comes after them
	assert R('get\t<#>_x_#\tk') == ['<#>_x_#\tdata\tNA', 'k\tv\tNA', '#0']
	assert R('delete\t-', ['k\tignored', '<#>_x_#']) == ['#0'] and R('has\tk') == ['#3']
	assert R('--sync\tset\tz\t1') == ['#0']
	assert open(p, 'rb').read().endswith(b'k\tv\n<#>_x_#\tdata\n#_defaults_#\t\tD\nk\n<#>_x_#\nz\t1\n')


def test_serve_usage_errors_keep_the_connection_usable(tmp_path, engines):
	store = engines(str(tmp_path / 'u.tsvz'), create=True)
	for line in ('', 'frob', 'x-export', '--bogus\tread', 'read\textra', 'get', 'has\ta\tb', 'set\t\tv',
				 'setdefault\tk', 'setdefault\t#_defaults_#\tx', 'popitem\tmiddle', 'stop'):
		response = TSVZ._serveRespond(store, line)
		assert response[-1].startswith('#2\t') or (line == 'stop' and response[-1].startswith('#1\t')), line
	assert TSVZ._serveRespond(store, 'read') == ['#0']


def test_serve_responses_carry_diagnostics_and_failures(tmp_path, engines, monkeypatch):
	p = str(tmp_path / 'd.tsvz')
	_touch(p, b'a\t1\n')
	store = engines(p)
	with open(p, 'ab') as f:
		f.write(b'torn')
	response = TSVZ._serveRespond(store, 'read')
	assert response[0].startswith('#!\tTSVZ warning: ') and 'torn' in response[0]
	assert response[1:] == ['a\t1', '#0']
	m = str(tmp_path / 'm.tsvz')
	_touch(m, b'a\t1\n')
	_touch(m + '.1', b'b\t2\n')
	store = engines(m)
	assert TSVZ._serveRespond(store, 'scrub')[-1] == '#1\tscrub refused; nothing was written'
	monkeypatch.chdir(str(tmp_path))
	_touch('#h.tsvz', b'#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\t0\n')  # a path starting with '#'
	store = engines('#h.tsvz')
	lines, _, status, _ = TSVZ._serveParseResponse(TSVZ._serveRespond(store, 'verify'))
	assert status == 4 and lines[0].startswith('<#>h.tsvz\t3\tcrc32\t0\t')


def test_serve_parse_response():
	assert TSVZ._serveParseResponse(['#!\tw<LF>x', 'k\tv', '#1\tbad<sep>thing']) == (['k\tv'], ['w\nx'], 1, 'bad\tthing')
	with pytest.raises(TSVZ._ServeLost):
		TSVZ._serveParseResponse(['k\tv'])


def test_cli_has_len_keys_pop_popitem_setdefault(tmp_path):
	p = str(tmp_path / 'o.tsvz')
	_touch(p, b'a\t1\n<#>h\t2\nb\t3\n')
	assert _run('has', p, 'a') == (0, '', '') and _run('has', p, 'z') == (3, '', '')
	assert _run('len', p) == (0, '3\n', '') and _run('keys', p) == (0, 'a\n<#>h\nb\n', '')
	assert _run('pop', p, 'a') == (0, 'a\t1\n', '') and _run('pop', p, 'a') == (3, '', '')
	assert _run('popitem', p) == (0, 'b\t3\n', '') and _run('popitem', p, 'first') == (0, '<#>h\t2\n', '')
	assert _run('popitem', p) == (3, '', '')
	assert _run('setdefault', p, 'k', 'v', '-5') == (0, 'k\tv\t-5\n', '')
	assert _run('setdefault', p, 'k', 'other') == (0, 'k\tv\t-5\n', '')
	assert open(p, 'rb').read() == b'a\t1\n<#>h\t2\nb\t3\na\nb\n<#>h\nk\tv\t-5\n'
	fresh = str(tmp_path / 'fresh.tsvz')
	status, out, err = _run('setdefault', fresh, 'k', 'v')
	assert (status, out) == (0, 'k\tv\n') and open(fresh, 'rb').read() == b'k\tv\n'
	q = str(tmp_path / 'o.csv')
	_touch(q, b'a,x<sep>y\nb,2\n')
	assert _run('keys', q) == (0, 'a\nb\n', '') and _run('pop', q, 'a') == (0, 'a,x<sep>y\n', '')
	for argv in (['has', 'none.tsvz', 'k'], ['len', 'none.tsvz'], ['keys', 'none.tsv'], ['pop', 'none.tsvz', 'k'],
				 ['popitem', 'none.tsvz']):
		argv[1] = str(tmp_path / argv[1])
		status, out, err = _run(*argv)
		assert (status, out) == (1, '') and 'no such store' in err
	for argv in (['has', p], ['has', p, 'a', 'b'], ['len', p, 'x'], ['popitem', p, 'middle'], ['setdefault', p, 'k'],
				 ['pop', p, '']):
		with pytest.raises(TSVZ._CliUsageError):
			TSVZ._cliParseArgs(argv)


# ==========================================================================
# Write handler (spec §21): the server
# ==========================================================================
@pytest.fixture
def served():
	"""Start in-thread handlers: ``served(path, **handleOptions)`` returns the ``_ServeHandle``."""
	running = []

	def start(path, **options):
		handle = TSVZ._ServeHandle(path, **options)
		handle.lock()
		handle.start(TSVZ._ServeStore(path, create=True))
		thread = threading.Thread(target=handle.serve)
		thread.start()
		handle.thread = thread
		running.append((handle, thread))
		return handle
	yield start
	for handle, thread in running:
		handle.stop()
		thread.join(10)
		handle.close()


def _raw(address, data):
	"""Send raw bytes to a handler and return everything it answers until it closes or goes quiet."""
	import socket
	sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	sock.settimeout(2)
	sock.connect(address[len('unix:'):])
	try:
		sock.sendall(data)
		sock.shutdown(socket.SHUT_WR)
		chunks = []
		while True:
			chunk = sock.recv(65536)
			if not chunk:
				return b''.join(chunks)
			chunks.append(chunk)
	finally:
		sock.close()


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_over_a_unix_socket(tmp_path, served):
	p = str(tmp_path / 's.tsvz')
	handle = served(p)
	info = TSVZ._serveFind(p)
	assert info['address'] == handle.address and info['address'].startswith('unix:')
	assert info['pid'] == str(os.getpid()) and info['host'] == __import__('socket').gethostname()
	connection = TSVZ._ServeConnection(info)
	try:
		assert connection.request('set\talice\tAlice') == ([], [], 0, '')
		assert connection.request('set\t-', ['bob\tBob', 'carol']) == ([], [], 0, '')
		assert connection.request('read') == (['alice\tAlice', 'bob\tBob'], [], 0, '')
		assert connection.request('frob')[2:] == (2, 'unknown operation frob')
		assert connection.request('get\tbob') == (['bob\tBob'], [], 0, '')  # still usable after #2
	finally:
		connection.close()
	assert _raw(handle.address, b'len\nkeys\nbogus\n') == b'2\n#0\nalice\nbob\n#0\n#2\tunknown operation bogus\n'


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_closes_a_connection_with_an_overlong_line(tmp_path, served, monkeypatch):
	monkeypatch.setattr(TSVZ, '_SERVE_LINE_LIMIT', 100)
	handle = served(str(tmp_path / 'l.tsvz'))
	assert _raw(handle.address, b'get\t' + b'k' * 200 + b'\nlen\n') == b''
	assert _raw(handle.address, b'len\n') == b'0\n#0\n'


def test_serve_over_tcp_requires_the_token(tmp_path, served):
	p = str(tmp_path / 't.tsvz')
	handle = served(p, tcp=True)
	info = TSVZ._serveFind(p)
	assert info['address'].startswith('tcp:127.0.0.1:') and len(info['token']) == 32
	assert os.stat(p + '.serve').st_mode & 0o777 == 0o600  # the token is in it
	connection = TSVZ._ServeConnection(info)
	try:
		assert connection.request('set\tk\tv')[2] == 0 and connection.request('get\tk')[0] == ['k\tv']
	finally:
		connection.close()
	import socket
	host, port = info['address'][4:].rsplit(':', 1)
	for first in (b'read\n', b'auth\twrong\n'):
		sock = socket.create_connection((host, int(port)), 2)
		try:
			sock.sendall(first)
			assert sock.makefile('rb').read().startswith(b'#1\t')
		finally:
			sock.close()
	with pytest.raises(TSVZ._ServeUnreachable):
		TSVZ._ServeConnection(dict(info, token='0' * 32))


@pytest.mark.skipif(os.name != 'posix', reason='POSIX permissions')
def test_serve_socket_permissions(tmp_path, served):
	def modes(handle):
		path = handle.address[len('unix:'):]
		return os.stat(path).st_mode & 0o777, os.stat(os.path.dirname(path)).st_mode & 0o777
	assert modes(served(str(tmp_path / 'a.tsvz'))) == (0o600, 0o700)
	assert os.stat(str(tmp_path / 'a.tsvz.serve')).st_mode & 0o777 == 0o644
	assert modes(served(str(tmp_path / 'b.tsvz'), group=str(os.getgid()))) == (0o660, 0o710)
	assert modes(served(str(tmp_path / 'c.tsvz'), mode=0o666)) == (0o666, 0o711)


def test_serve_refuses_a_second_handler_for_the_same_store(tmp_path, served):
	p = str(tmp_path / 'b.tsvz')
	served(p)
	second = TSVZ._ServeHandle(p)
	with pytest.raises(TSVZ._ServeBusy) as busy:
		second.lock()
	assert busy.value.args[0]['pid'] == str(os.getpid())


def _serve_process(store, *options):
	"""Start ``tsvz serve STORE`` in the background; return the Popen once its pointer file is written."""
	proc = subprocess.Popen([sys.executable, os.path.join(HERE, 'TSVZ.py'), 'serve', store] + list(options),
							stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	deadline = time.time() + 10
	while TSVZ._serveFind(store) is None:
		if proc.poll() is not None or time.time() > deadline:
			proc.kill()
			raise AssertionError(proc.communicate()[1])
		time.sleep(0.02)
	return proc


def test_tsvz_serve_and_stop(tmp_path):
	p = str(tmp_path / 'p.tsvz')
	proc = _serve_process(p, '--x-header', 'id\\tval')
	try:
		address = TSVZ._serveFind(p)['address']
		r = _cli('serve', p)
		assert r.returncode == 1 and 'already served by pid {}'.format(proc.pid) in r.stderr
		connection = TSVZ._ServeConnection(TSVZ._serveFind(p))
		assert connection.request('set\tk\tv')[2] == 0
		connection.close()
		r = _cli('stop', p)
		assert (r.returncode, r.stdout) == (0, '')
		out, err = proc.communicate(timeout=10)
	finally:
		if proc.poll() is None:
			proc.kill()
	assert proc.returncode == 0 and out == b'' and b'serving' in err and b'stopped serving' in err
	assert open(p, 'rb').read() == b'#id\tval\nk\tv\n'  # the header of a store serve created; the write
	assert not os.path.exists(p + '.serve') and not os.path.exists(os.path.dirname(address[len('unix:'):]))
	r = _cli('stop', p)
	assert r.returncode == 1 and 'no handler is running' in r.stderr


@pytest.mark.skipif(os.name != 'posix', reason='signals')
def test_tsvz_serve_stops_on_sigterm_and_when_idle(tmp_path):
	import signal
	p = str(tmp_path / 'g.tsvz')
	proc = _serve_process(p)
	proc.send_signal(signal.SIGTERM)
	assert proc.wait(10) == 0 and not os.path.exists(p + '.serve')
	proc = _serve_process(p, '--x-idle-timeout', '0.3')
	assert proc.wait(10) == 0 and b'idle for 0.3 s' in proc.stderr.read()
	assert not os.path.exists(p + '.serve')


def test_tsvz_serve_usage_errors(tmp_path):
	p = str(tmp_path / 'u.tsvz')
	for options in (['--x-idle-timeout', 'soon'], ['--x-mode', '9x']):
		r = _cli('serve', p, *options)
		assert r.returncode == 2 and 'serve: bad' in r.stderr and 'usage: tsvz' in r.stderr
	assert _cli('serve', p, 'extra').returncode == 2

@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_close_ends_open_connections(tmp_path, served):
	p = str(tmp_path / 'e.tsvz')
	handle = served(p)
	connection = TSVZ._ServeConnection(TSVZ._serveFind(p))
	try:
		assert connection.request('len')[2] == 0
		handle.stop()
		handle.thread.join(10)
		handle.close()
		with pytest.raises(TSVZ._ServeLost):
			connection.request('len')
	finally:
		connection.close()
	assert TSVZ._serveFind(p) is None

@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_many_concurrent_clients(tmp_path, served):
	"""Review focus 1: interleaved requests from many connections all land, each answered on its own."""
	p = str(tmp_path / 'c.tsvz')
	served(p)
	errors = []

	def client(n):
		connection = TSVZ._ServeConnection(TSVZ._serveFind(p))
		try:
			for i in range(50):
				key = 'c{}k{}'.format(n, i)
				if connection.request('set\t{}\t{}'.format(key, i))[2] != 0 or \
						connection.request('get\t' + key)[0] != ['{}\t{}'.format(key, i)]:
					errors.append(key)
		finally:
			connection.close()
	threads = [threading.Thread(target=client, args=(n,)) for n in range(8)]
	for t in threads:
		t.start()
	for t in threads:
		t.join()
	assert errors == []
	assert len(_view(p)) == 400


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_ignores_bulk_input_cut_off_before_its_end(tmp_path, served):
	"""Review focus 2: a client that disconnects before the closing '#' writes nothing."""
	p = str(tmp_path / 'b.tsvz')
	handle = served(p)
	assert _raw(handle.address, b'set\t-\nk\tv\nj\tw\n') == b''
	assert _raw(handle.address, b'len\n') == b'0\n#0\n' and open(p, 'rb').read() == b''


def test_serve_takes_over_a_stale_pointer(tmp_path, served):
	"""Review focus 3: the pointer file of a crashed handler is replaced by the next one."""
	import socket
	p = str(tmp_path / 's.tsvz')
	_touch(p + '.serve', 'tsvz-handler\t1\naddress\tunix:/gone\nhost\t{}\npid\t1\n'.format(socket.gethostname()).encode())
	assert _run('get', p, 'k')[0] == 1  # no store yet; the stale pointer is reported and skipped
	handle = served(p)
	assert TSVZ._serveFind(p)['address'] == handle.address


# ==========================================================================
# CLI through a handler (spec §20.8)
# ==========================================================================
_ROUTED_STEPS = (
	(['read'], ''), (['get', 'alice', 'nobody'], ''), (['has', 'alice'], ''), (['len'], ''), (['keys'], ''),
	(['set', 'bob', 'B<b', 'two words'], ''), (['set', '#tag', 'v'], ''), (['set', '#_defaults_#', '', 'NA'], ''),
	(['get', 'carol', 'bob'], ''), (['delete', 'alice', 'nobody'], ''),
	(['set', '-'], 'k1\tv1\n# comment\nk2\tv<sep>2\n'), (['delete', '-'], 'k1\tignored\n'),
	(['pop', 'bob'], ''), (['pop', 'bob'], ''), (['popitem', 'first'], ''), (['setdefault', 'k', 'v', '-5'], ''),
	(['setdefault', 'k', 'other'], ''), (['read', '--format', 'table'], ''), (['verify'], ''),
	(['scrub'], ''), (['read'], ''), (['clear'], ''), (['read'], ''), (['popitem'], ''),
)


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
@pytest.mark.parametrize('name', ['d.tsvz', 'd.tsv'])
def test_cli_gives_the_same_results_through_a_handler(tmp_path, served, name):
	"""Spec §20.8: every operation prints the same and exits the same with or without a handler."""
	direct, routed = str(tmp_path / ('direct-' + name)), str(tmp_path / ('routed-' + name))
	start = b'alice\tAlice\t30\n<#>h\tx\ty\n' if name.endswith('z') else b'alice\tAlice\t30\nh\tx\ty\n'
	for path in (direct, routed):
		_touch(path, start)
	served(routed)
	for argv, stdin in _ROUTED_STEPS:
		first = _run(argv[0], direct, *argv[1:], stdin=stdin)
		second = _run(argv[0], routed, *argv[1:], stdin=stdin)
		assert first[:2] == second[:2], argv
	assert open(direct, 'rb').read() == open(routed, 'rb').read()


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_cli_routes_only_what_the_handler_can_answer(tmp_path, served, monkeypatch):
	p = str(tmp_path / 'v.tsvz')
	_touch(p, b'a\t1\n')
	served(p)
	requests = []
	real = TSVZ._serveRespond

	def spy(store, line, bulk=None, logger=None):
		requests.append(line)
		return real(store, line, bulk, logger)
	monkeypatch.setattr(TSVZ, '_serveRespond', spy)
	assert _run('get', p, 'a') == (0, 'a\t1\n', '')
	assert _run('--x-direct', 'get', p, 'a') == (0, 'a\t1\n', '')
	assert _run('get', p, 'a', '--x-defaults', 'k\\tD') == (0, 'a\t1\n', '')  # an option the handler lacks
	q = str(tmp_path / 'v.txt')
	_touch(q, b'a|1\n')
	served(q)  # a loose file served with the delimiter its name implies (tab)
	assert _run('get', q, 'a', '-d', 'pipe')[:2] == (0, 'a|1\n')  # 3.39 warns about the name on stderr
	assert requests == ['get\ta']


def test_cli_falls_back_to_the_files_when_the_handler_is_gone(tmp_path):
	import socket
	p = str(tmp_path / 'f.tsvz')
	_touch(p, b'a\t1\n')
	_touch(p + '.serve', 'tsvz-handler\t1\naddress\tunix:{}\nhost\t{}\npid\t1\n'.format(
		str(tmp_path / 'gone.sock'), socket.gethostname()).encode())
	status, out, err = _run('get', p, 'a')
	assert (status, out) == (0, 'a\t1\n') and 'does not answer' in err
	_touch(p + '.serve', b'tsvz-handler\t1\naddress\tunix:/nowhere\nhost\tsome-other-host\npid\t1\n')
	assert _run('get', p, 'a') == (0, 'a\t1\n', '')  # another host's handler: skipped quietly
	assert _run('stop', p)[0] == 1


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_cli_when_the_handler_drops_the_connection(tmp_path):
	"""A read falls back to the files; a write reports that its outcome is unknown."""
	import socket
	path = str(tmp_path / 'drop.sock')
	listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	listener.bind(path)
	listener.listen(8)

	def drop():
		while True:
			try:
				conn, _ = listener.accept()
			except OSError:
				return
			conn.recv(1024)
			conn.close()
	thread = threading.Thread(target=drop)
	thread.daemon = True
	thread.start()
	p = str(tmp_path / 'd.tsvz')
	_touch(p, b'a\t1\n')
	_touch(p + '.serve', 'tsvz-handler\t1\naddress\tunix:{}\nhost\t{}\npid\t1\n'.format(
		path, socket.gethostname()).encode())
	try:
		status, out, err = _run('read', p)
		assert (status, out) == (0, 'a\t1\n') and 'lost the handler' in err
		status, out, err = _run('set', p, 'b', '2')
		assert (status, out) == (1, '') and 'may or may not have been applied' in err
	finally:
		listener.close()


def test_tsvz_commands_through_tsvz_serve(tmp_path):
	p = str(tmp_path / 'e.tsvz')
	proc = _serve_process(p)
	try:
		assert _cli('set', p, 'k', 'v').returncode == 0
		r = _cli('get', p, 'k')
		assert (r.returncode, r.stdout) == (0, 'k\tv\n')
		r = subprocess.run([sys.executable, os.path.join(HERE, 'TSVZ.py'), 'set', p, '-'], input='j\tw\n',
						   stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
		assert r.returncode == 0 and _cli('read', p).stdout == 'k\tv\nj\tw\n'
		assert _cli('stop', p).returncode == 0
		assert proc.wait(10) == 0
	finally:
		if proc.poll() is None:
			proc.kill()
	assert open(p, 'rb').read() == b'k\tv\nj\tw\n'


# ==========================================================================
# TSVZClient: a served store from Python (spec §21)
# ==========================================================================
def _mapping_story(t):
	"""Run the same dict operations on a TSVZed or a TSVZClient; return what they returned."""
	seen = []
	t['alice'] = ['alice', 'Alice', '30']
	t['bob'] = 'bob\tBob\t7'
	t['carol'] = ['Carol', '5']  # the key is put first
	seen += [t['bob'], t.get('nobody', 'missing'), 'alice' in t, 'zed' in t, len(t), list(t)]
	del t['bob']
	seen += [t.pop('carol'), t.pop('carol', 'gone')]
	seen += [t.setdefault('dave', ['dave', 'D', '1']), t.setdefault('dave', ['dave', 'X', '2'])]
	t.update({'erin': ['erin', 'E', '2'], 'fay': 'fay\tF\t3'})
	seen += [sorted(t.keys()), t.popitem(), t.popitem(last=False)]
	del t['nobody']  # a missing key is not an error
	return seen


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
@pytest.mark.parametrize('ext', ['tsvz', 'tsv'])
def test_tsvz_client_behaves_like_tsvzed(tmp_path, served, ext):
	header = 'id\tname\tn' if ext == 'tsvz' else ''  # a .tsv header line is a row when served, as in `tsvz read`
	paths = [str(tmp_path / '{}.{}'.format(name, ext)) for name in ('tsvzed', 'served', 'local')]
	for path in paths[1:]:
		TSVZ.appendLinesTabularFile(path, [], header=header, createIfNotExist=True)
	served(paths[1])
	with TSVZ.TSVZed(paths[0], header=header) as t:
		expected = _mapping_story(t)
	results = []
	for path in paths[1:]:
		with TSVZ.TSVZClient(path) as c:
			results.append(_mapping_story(c))
			assert c.items() == [(row[0], row) for row in _view(path)]
	assert results == [expected, expected]
	assert expected[:6] == [['bob', 'Bob', '7'], 'missing', True, False, 3, ['alice', 'bob', 'carol']]
	views = [[row for row in _view(path) if row[0] != 'id'] for path in paths]
	assert views[0] == views[1] == views[2] == [['dave', 'D', '1'], ['erin', 'E', '2']]


def test_tsvz_client_missing_keys_follow_the_store(tmp_path):
	p = str(tmp_path / 'm.tsvz')
	_touch(p, b'#_defaults_#\t\tNA\na\t1\t2\n')
	with TSVZ.TSVZClient(p) as c:
		assert c['zed'] == ['zed', '', 'NA'] and c.get('zed') is None and 'zed' not in c
		c['#_return_defaults_when_missing_#'] = ['false']  # a marker write
		with pytest.raises(KeyError):
			c['zed']
		c.setDefaults(['', 'X'])
		assert c['a'] == ['a', '1', '2'] and open(p, 'rb').read().endswith(
			b'#_return_defaults_when_missing_#\tfalse\n#_defaults_#\t\tX\n')
		for method in ('move_to_end', 'rewrite', 'mapToFile', 'hardMapToFile'):
			with pytest.raises(NotImplementedError):
				getattr(c, method)('a')


def test_tsvz_client_warns_once_when_no_handler_runs(tmp_path, capsys):
	p = str(tmp_path / 'w.tsvz')
	with TSVZ.TSVZClient(p) as c:
		c['k'] = ['k', 'v']
		assert c['k'] == ['k', 'v'] and len(c) == 1
		assert repr(c) == 'TSVZClient({!r}, local)'.format(p)
	err = capsys.readouterr().err
	assert err.count('working on the files in this process') == 1
	assert open(p, 'rb').read() == b'k\tv\n'


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_tsvz_client_sync_asks_for_disk_acknowledgement(tmp_path, served, monkeypatch):
	p = str(tmp_path / 's.tsvz')
	served(p)
	lines = []
	real = TSVZ._serveRespond
	monkeypatch.setattr(TSVZ, '_serveRespond', lambda store, line, bulk=None, logger=None: (
		lines.append(line), real(store, line, bulk, logger))[1])
	with TSVZ.TSVZClient(p, sync=True) as c:
		c['k'] = 'k\tv'
		assert c['k'] == ['k', 'v']
	assert lines == ['--sync\tset\tk\tv', 'get\tk']


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_tsvz_client_reconnects_or_falls_back(tmp_path, served):
	p = str(tmp_path / 'r.tsvz')
	first = served(p)
	client = TSVZ.TSVZClient(p)
	try:
		client['k'] = ['k', 'v']
		first.stop()
		first.thread.join(10)
		first.close()
		second = served(p)
		assert client['k'] == ['k', 'v']  # the lost connection was replaced by one to the new handler
		assert repr(client).endswith('served)')
		second.stop()
		second.thread.join(10)
		second.close()
		with pytest.raises(ConnectionError):
			client.pop('k')  # not retried: it may have been applied
		assert repr(client).endswith('local)') and client['k'] == ['k', 'v']
	finally:
		client.close()


if __name__ == '__main__':
	sys.exit(pytest.main([__file__] + sys.argv[1:]))
