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
	('gzip', lambda d: tc._gzip_member(d)),
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
	([{'append': 'x', 'path': '../escape.tsvz'}], 'relative path'),
	([{'write': 'x', 'path': '/tmp/abs.tsvz'}], 'relative path'),
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
	('#hdr\n#_snapshot_#\n#_version_#\t1\na\tx\n', None),  # §19.2.3e
	('#_snapshot_#\n#_version_#\t1\na\tx\n', None),
	('#_snapshot_#\tx\n#_version_#\t1\na\tx\n', '#_version_#'),  # with a value: not the marker
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


def test_two_threads_can_reap_the_same_process(tmp_path):
	"""Ctrl-C reaps live handlers from the main thread while a worker stops the same one; neither may raise."""
	proc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(0.5)'], start_new_session=True)
	got = []

	def reap():
		try:
			got.append(tc.reap(proc, 10))
		except Exception as e:
			got.append(e)

	threads = [threading.Thread(target=reap) for _ in range(2)]
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join(15)
	assert got == [(0, False), (0, False)]


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


def test_a_status_line_with_non_ascii_digits_is_malformed():
	path = os.path.join(_short_socket_dir(), 's')
	server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	server.bind(path)
	server.listen(1)

	def serve():
		conn, _ = server.accept()
		conn.makefile('rb').readline()
		conn.sendall('#\u00b2\n'.encode('utf-8'))
		conn.close()

	thread = threading.Thread(target=serve)
	thread.daemon = True
	thread.start()
	client = tc.Client(path, 5)
	try:
		with pytest.raises(ConnectionError, match='not #N'):
			client.request(['len'])
	finally:
		client.close()
		thread.join(5)
		server.close()


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


def test_a_serve_that_forks_and_exits_leaves_nothing_running(tmp_path):
	store = tmp_path / 's.tsvz'
	store.write_text('')
	pidfile = tmp_path / 'child.pid'
	code = ('import os, sys, time\nif os.fork() == 0:\n\topen(%r, "w").write(str(os.getpid()))\n\ttime.sleep(30)\n\tos._exit(0)\n'
		'time.sleep(0.5)\nsys.exit(0)\n' % str(pidfile))
	script = tmp_path / 'forky.py'
	script.write_text(code)
	impl = tc.Impl('%s %s' % (shlex.quote(sys.executable), shlex.quote(str(script))))
	with pytest.raises(tc.HandlerError, match='exited with status 0'):
		tc.Handler(impl, str(store), str(tmp_path)).start()
	pid = int(pidfile.read_text())
	time.sleep(0.2)
	with pytest.raises(ProcessLookupError):
		os.kill(pid, 0)


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
if mode == 'ttyhang' and op == 'read' and os.isatty(1):
	time.sleep(40)
if mode == 'canon' and store:
	args[args.index(store)] = os.path.realpath(store)
if mode == 'freshpart' and op == 'serve':
	open(store, 'ab').close()
	open(store + '.0192a3b4c5d67e8f9a0b1c2d3e4f5099', 'ab').close()
if mode == 'refuse' and store and os.path.lexists(store + '.serve'):
	sys.stderr.write('refusing: there is a pointer file\n')
	sys.exit(1)
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


FAKE_SLOW_SERVE = r'''
import os, socket, sys, tempfile, threading, time
args = sys.argv[1:]
if '-V' in args:
	print('slow 1.0')
	sys.exit(0)
store = args[-1]
path = os.path.join(tempfile.mkdtemp(prefix='tc-', dir='/tmp'), 's')
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(path)
server.listen(8)
with open(store + '.serve', 'w') as f:
	f.write('tsvz-handler\t1\naddress\tunix:%s\nhost\t%s\npid\t%d\n' % (path, socket.gethostname(), os.getpid()))

def handle(conn):
	for line in conn.makefile('rb'):
		if line.startswith(b'stop'):
			conn.sendall(b'#0\n')
			os.remove(store + '.serve')
			os._exit(0)
		if line.startswith(b'get'):
			time.sleep(30)
		conn.sendall(b'0\n#0\n')

while True:
	conn, _ = server.accept()
	threading.Thread(target=handle, args=(conn,), daemon=True).start()
'''


def test_socket_steps_obey_the_timeout(tmp_path):
	script = tmp_path / 'slow.py'
	script.write_text(FAKE_SLOW_SERVE)
	case = tc.validate_case({'id': '21.6.1-x', 'spec': ['21.6.1'], 'level': 'MUST', 'note': 'n', 'files': {'s.tsvz': 'k\tv\n'},
		'steps': [{'request': ['get', 'k'], 'stdout': {'rows': [['k', 'v']]}}]}, 'handler', 'x')
	ctx = tc.Context(tc.Impl('%s %s' % (shlex.quote(sys.executable), shlex.quote(str(script)))), str(tmp_path), 1.0, frozenset(['handler']))
	started = time.time()
	result = tc.run_case(case, 'socket', ctx)
	assert result.result == tc.FAIL and 'no response' in ' '.join(result.detail)
	assert time.time() - started < 8


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


def test_a_case_directory_that_cannot_be_prepared_is_reported(tmp_path):
	work = tmp_path / 'work'
	work.mkdir()
	(work / '9.2-lone-key-deletes').write_text('in the way')
	result = tc.run_case(tc.validate_case(DELETES, 'reader', 'x'), 'files', context(work))
	assert result.result == tc.ERROR and 'cannot prepare' in result.detail[0]


def test_a_missing_tool_fails_and_a_runner_bug_is_an_error(tmp_path, monkeypatch):
	result = tc.run_case(tc.validate_case(DELETES, 'reader', 'x'), 'files', context(tmp_path / 'work', '/nonexistent/tsvz'))
	assert result.result == tc.FAIL and 'cannot run' in ' '.join(result.detail)

	def boom(store):
		raise RuntimeError('a bug in the runner')

	monkeypatch.setattr(tc, 'observe', boom)
	faithful = dict(WRITES, after={'fidelity': 'full'})
	result = tc.run_case(tc.validate_case(faithful, 'writer', 'x'), 'files', context(tmp_path / 'w2'))
	assert result.result == tc.ERROR and 'a bug in the runner' in result.detail[-1]


# --- procedural checks -----------------------------------------------------------------------

def _python_has_zstd():
	try:
		from compression import zstd  # noqa: F401 (Python 3.14+)
	except ImportError:
		return False
	return True


@pytest.mark.parametrize('check', tc.CHECKS, ids=[c.id for c in tc.CHECKS])
def test_every_procedural_check_passes_with_the_reference(check, tmp_path):
	features = ['handler'] + (['zstd'] if _python_has_zstd() else [])
	unclaimed = [r for r in check.requires if not r.startswith('!') and r not in features]
	if unclaimed:
		pytest.skip('the reference does not claim %s on this Python' % ', '.join(unclaimed))
	result = tc.run_case(check, 'files', context(tmp_path / 'work', features=features))
	assert result.result in (tc.PASS, tc.SKIP), result.detail


def test_the_terminal_check_obeys_the_timeout(tmp_path):
	check = next(c for c in tc.CHECKS if c.id == '20.4.3-terminal-output-is-a-table')
	ctx = tc.Context(tc.Impl(fake(tmp_path, 'ttyhang')), str(tmp_path), 2.0, frozenset())
	started = time.time()
	result = tc.run_case(check, 'files', ctx)
	assert time.time() - started < 15
	assert result.result == tc.WARN and 'timed out' in ' '.join(result.detail)


def test_refusing_a_bad_pointer_is_conformant_but_falls_short_of_the_should(tmp_path):
	"""Not connecting is the MUST (§21.2.4); answering from the files instead is a SHOULD (§20.8.2)."""
	checks = {c.id: c for c in tc.CHECKS}
	must = tc.run_case(checks['21.2.4-refuse-wrong-first-line'], 'files', context(tmp_path / 'w1', fake(tmp_path, 'refuse')))
	assert must.result == tc.PASS, must.detail
	should = tc.run_case(checks['20.8.2-files-answer-past-a-bad-pointer'], 'files', context(tmp_path / 'w2', fake(tmp_path, 'refuse')))
	assert should.result == tc.WARN and 'did not answer from the files' in should.detail[0]


def test_a_command_that_follows_a_bad_pointer_fails(tmp_path):
	check = next(c for c in tc.CHECKS if c.id == '21.2.4-refuse-wrong-first-line')
	result = tc.run_case(check, 'files', context(tmp_path / 'work', fake(tmp_path, 'decoy')))
	assert result.result == tc.FAIL and 'must not use' in result.detail[0]


def test_a_read_that_prints_an_empty_row_as_a_lone_key_breaks_the_copy(tmp_path):
	"""§20.3.2: a row of only empty cells printed without a delimiter is copied as a tombstone."""
	check = next(c for c in tc.CHECKS if c.id == '20.3.2-read-piped-into-set-copies-the-rows')
	result = tc.run_case(check, 'files', context(tmp_path / 'work', fake(tmp_path, 'nodelim')))
	assert result.result == tc.FAIL and len(result.detail) == 1 and result.detail[0].startswith('.tsvz:')


# --- selection, report and main --------------------------------------------------------------

def test_the_rerun_line_names_the_mode():
	case = tc.validate_case(DELETES, 'reader', 'x')
	impl = tc.Impl('tsvz')
	assert '--mode files' in tc.rerun_command(impl, tc.Result(case, 'files', tc.FAIL), frozenset())
	assert '--mode socket' in tc.rerun_command(impl, tc.Result(case, 'socket', tc.FAIL), frozenset(['handler']))
	check = next(c for c in tc.CHECKS if c.id == '21.2.3-pointer-is-locked')
	assert '--mode' not in tc.rerun_command(impl, tc.Result(check, 'files', tc.FAIL), frozenset())


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


def test_a_work_directory_reached_through_a_symlink(tmp_path, capsys):
	"""An implementation may print canonical part paths; {store} must match them (macOS: /var is a symlink)."""
	real = tmp_path / 'real'
	real.mkdir()
	link = tmp_path / 'link'
	link.symlink_to(real)
	code, results = run_main(link, fake(tmp_path, 'canon'), '--only', '20.2-parts-of-a-single-file', '--mode', 'files')
	assert results[('20.2-parts-of-a-single-file', 'files')]['result'] == 'pass' and code == 0


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
		'--only', '20.2-len,20.4.3-terminal-output-is-a-table', '-j', '1', '--timeout', '60', '--work-dir', str(tmp_path / 'work')],
		cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
	deadline = time.time() + 20
	while not (tmp_path / 'pid').exists() and time.time() < deadline:
		time.sleep(0.1)
	proc.send_signal(signal.SIGINT)
	out, _ = proc.communicate(timeout=30)
	assert proc.returncode == 130
	assert b'tsvzconform ' + tc.version.encode() in out
	assert re.search(rb'^cli +1 +0 +0 +0 +0$', out, re.M), out
	with pytest.raises(ProcessLookupError):
		os.kill(int((tmp_path / 'pid').read_text()), 0)


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
	if 'snapshot-skip' in case.requires:
		pytest.skip('the oracle replays every part: it does not skip history (spec §19.10.3 is a MAY)')
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


# --- the reference ----------------------------------------------------------------------------

def test_a_long_work_directory_path(tmp_path, capsys):
	work = tmp_path / ('long' * 30)
	code, results = run_main(work, REF_COMMAND, '--only', '21.2.4-refuse-wrong-first-line,21.13.1-clean-stop', '--feature', 'handler')
	assert code == 0 and len(results) == 2 and all(r['result'] == 'pass' for r in results.values())


def test_a_handler_that_opens_a_fresh_part_fails(tmp_path, capsys):
	"""§17.7: a handler appends to the store's last part, so a one-file store stays one file."""
	code, results = run_main(tmp_path, fake(tmp_path, 'freshpart'), '--class', 'handler',
		'--only', '17.7,20.2-parts-of-a-single-file,21.8.2')
	assert results[('17.7-a-handler-keeps-a-one-file-store-one-file', 'socket')]['result'] == 'FAIL'
	assert results[('17.7-a-handler-appends-to-the-last-part', 'socket')]['result'] == 'FAIL'
	assert results[('20.2-parts-of-a-single-file', 'socket')]['result'] == 'FAIL'
	assert results[('21.8.2-concurrent-pop-is-atomic', 'files')]['result'] == 'pass'
	assert code == 1


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
