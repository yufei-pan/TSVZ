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
