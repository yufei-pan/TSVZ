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
