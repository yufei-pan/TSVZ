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


