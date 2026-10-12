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
