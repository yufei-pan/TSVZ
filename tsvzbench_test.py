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


