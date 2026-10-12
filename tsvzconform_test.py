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


# --- procedural checks -----------------------------------------------------------------------

@pytest.mark.parametrize('check', tc.CHECKS, ids=[c.id for c in tc.CHECKS])
def test_every_procedural_check_passes_with_the_reference(check, tmp_path):
	result = tc.run_case(check, 'files', context(tmp_path / 'work', features=['handler']))
	assert result.result in (tc.PASS, tc.SKIP), result.detail


def test_a_command_that_follows_a_bad_pointer_fails(tmp_path):
	check = next(c for c in tc.CHECKS if c.id == '21.2.4-refuse-wrong-first-line')
	result = tc.run_case(check, 'files', context(tmp_path / 'work', fake(tmp_path, 'decoy')))
	assert result.result == tc.FAIL and 'must not use' in result.detail[0]
