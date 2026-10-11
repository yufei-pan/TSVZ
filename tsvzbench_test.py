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


