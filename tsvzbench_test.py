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


