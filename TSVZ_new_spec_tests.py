#!/usr/bin/env python3
"""Tests for TSVZ_new.py (tsvz-spec-v1.md core conformance)."""
import gzip
import os
import tempfile
import unittest
from collections import OrderedDict

import TSVZ_new as TSVZ


class TempFile:
	def __init__(self, suffix='.tsvz', content=None, encoding='utf8'):
		self._tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
		self.path = self._tmp.name
		if content is not None:
			self._tmp.write(content if isinstance(content, bytes) else content.encode(encoding))
		self._tmp.close()

	def __enter__(self):
		return self.path

	def __exit__(self, *exc):
		try:
			os.unlink(self.path)
		except FileNotFoundError:
			pass


def replay(content, delimiter='\t'):
	if isinstance(content, str):
		content = content.encode()
	store, state = TSVZ.replay_bytes(content, delimiter)
	rows = OrderedDict((k, list(v.row)) for k, v in store.items())
	return rows, state


def decode_header_line(line, delimiter='\t'):
	fields = [TSVZ.decode_field(f, delimiter) for f in line.split(delimiter)]
	if fields[0].startswith('#'):
		fields[0] = fields[0][1:]
	return fields


class TestDelimiterForPath(unittest.TestCase):
	def test_loose_extensions(self):
		self.assertEqual(TSVZ.delimiter_for_path('x.tsv'), '\t')
		self.assertEqual(TSVZ.delimiter_for_path('x.csv'), ',')
		self.assertEqual(TSVZ.delimiter_for_path('x.nsv'), '\0')
		self.assertEqual(TSVZ.delimiter_for_path('x.psv'), '|')

	def test_strict_extensions(self):
		self.assertEqual(TSVZ.delimiter_for_path('x.tsvz'), '\t')
		self.assertEqual(TSVZ.delimiter_for_path('x.csvz'), ',')
		self.assertEqual(TSVZ.delimiter_for_path('x.nsvz'), '\0')
		self.assertEqual(TSVZ.delimiter_for_path('x.psvz'), '|')

	def test_compression_suffix_stripped(self):
		self.assertEqual(TSVZ.delimiter_for_path('data.csv.gz'), ',')
		self.assertEqual(TSVZ.delimiter_for_path('data.tsvz.zst'), '\t')

	def test_explicit_delimiter(self):
		self.assertEqual(TSVZ.delimiter_for_path('x.unknown', delimiter='|'), '|')


class TestIsStrictStore(unittest.TestCase):
	def test_strict_vs_loose(self):
		self.assertTrue(TSVZ.is_strict_store('data.tsvz'))
		self.assertTrue(TSVZ.is_strict_store('data.csvz.gz'))
		self.assertFalse(TSVZ.is_strict_store('data.tsv'))
		self.assertFalse(TSVZ.is_strict_store('data.csv'))


class TestEncoding(unittest.TestCase):
	def test_spec_examples_roundtrip(self):
		d = '\t'
		cases = [
			('a\tb', 'a<sep>b'),
			('<sep>', '<lt>sep>'),
			('<LF>', '<lt>LF>'),
			('#foo', '<#>foo'),
			('a<b', 'a<lt>b'),
		]
		for raw, encoded in cases:
			is_key = raw.startswith('#')
			self.assertEqual(TSVZ.encode_field(raw, d, is_key=is_key), encoded)
			self.assertEqual(TSVZ.decode_field(encoded, d), raw)

	def test_newline_in_field(self):
		raw = 'line1\nline2'
		enc = TSVZ.encode_field(raw, '\t')
		self.assertIn('<LF>', enc)
		self.assertEqual(TSVZ.decode_field(enc, '\t'), raw)

	def test_unknown_token_passes_through(self):
		self.assertEqual(TSVZ.decode_field('<future>', '\t'), '<future>')

	def test_format_data_row(self):
		self.assertEqual(TSVZ.format_data_row(['k', 'a\tb'], '\t'), 'k\ta<sep>b')


class TestClassification(unittest.TestCase):
	def test_data_comment_marker(self):
		self.assertEqual(TSVZ.classify_record('alice'), 'data')
		self.assertEqual(TSVZ.classify_record('# header'), 'comment')
		self.assertEqual(TSVZ.classify_record('#_version_#'), 'marker')
		self.assertEqual(TSVZ.classify_record('#_checksum_sha256_#'), 'ignore')
		self.assertEqual(TSVZ.classify_record('#__custom__#'), 'ignore')

	def test_leading_space_is_data(self):
		rows, _ = replay(' #notacomment\tval\n')
		self.assertIn(' #notacomment', rows)


class TestMarkers(unittest.TestCase):
	def test_defaults_and_fill_empty(self):
		rows, _ = replay(
			'#_defaults_#\tguest\n'
			'#_fill_empty_with_default_#\ttrue\n'
			'k\t\n'
		)
		self.assertEqual(rows['k'], ['k', 'guest'])

	def test_return_defaults_when_missing_false(self):
		_, state = replay('#_return_defaults_when_missing_#\tfalse\n')
		with self.assertRaises(KeyError):
			TSVZ.resolve_missing_key('missing', state)

	def test_defaults_not_retroactive(self):
		rows, _ = replay('#_defaults_#\tA\nk\t\n#_defaults_#\tB\n')
		self.assertEqual(rows['k'], ['k', ''])

	def test_appendix_c(self):
		content = (
			"#_version_#\t1\n#_defaults_#\tguest\t0\n"
			"alice\tAlice\t30\nbob\tBob\ncarol\t\t25\n"
			"alice\tAlice\t31\nbob\n"
		)
		rows, state = replay(content)
		self.assertEqual(rows['alice'], ['alice', 'Alice', '31'])
		self.assertEqual(rows['carol'], ['carol', '', '25'])
		self.assertNotIn('bob', rows)
		self.assertEqual(TSVZ.resolve_missing_key('bob', state)[1:], ['guest', '0'])


class TestTombstones(unittest.TestCase):
	def test_lone_key_deletes(self):
		rows, _ = replay('mykey\told\nmykey\n')
		self.assertNotIn('mykey', rows)

	def test_key_with_empty_col_not_tombstone(self):
		rows, _ = replay('mykey\t\n')
		self.assertEqual(rows['mykey'], ['mykey', ''])

	def test_empty_key_ignored(self):
		rows, _ = replay('\tval\nk\tx\n')
		self.assertNotIn('', rows)


class TestFraming(unittest.TestCase):
	def test_discard_torn_tail(self):
		rows, _ = replay(b'alice\t1\npartial')
		self.assertEqual(rows, OrderedDict([('alice', ['alice', '1'])]))

	def test_crlf(self):
		rows, _ = replay('k\tv\r\n')
		self.assertEqual(rows['k'], ['k', 'v'])

	def test_committed_payload(self):
		self.assertEqual(TSVZ.committed_payload(b'a\nb\n'), b'a\nb\n')
		self.assertEqual(TSVZ.committed_payload(b'a\nb'), b'a\n')
		self.assertEqual(TSVZ.committed_payload(b'torn'), b'')


class TestHeaderComment(unittest.TestCase):
	def test_format_and_decode(self):
		line = TSVZ.format_header_comment(['id', 'name'], '\t')
		self.assertTrue(line.startswith('#id\t'))
		self.assertEqual(decode_header_line(line), ['id', 'name'])

	def test_truncate_writes_header(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.truncate_part(path, header=['id', 'name'])
			with open(path) as f:
				self.assertTrue(f.readline().startswith('#id'))


class TestReadStore(unittest.TestCase):
	def test_create_and_read(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			data = TSVZ.read_store(path, create=True)
			self.assertTrue(os.path.isfile(path))
			self.assertEqual(data, OrderedDict())

	def test_missing_raises(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			with self.assertRaises(FileNotFoundError):
				TSVZ.read_store(path, create=False)

	def test_last_record(self):
		with TempFile(suffix='.tsv', content='a\t1\nb\t2\nc\t3\n') as path:
			self.assertEqual(TSVZ.read_last_record(path), ['c', '3'])

	def test_last_record_offset(self):
		with TempFile(suffix='.tsv', content='a\t1\nb\t2\n') as path:
			off = TSVZ.read_last_record(path, store_offset=True)
			with open(path, 'rb') as f:
				f.seek(off)
				self.assertTrue(f.readline().startswith(b'b'))

	def test_store_offset(self):
		with TempFile(suffix='.tsv', content='k\tv\n') as path:
			idx = OrderedDict()
			TSVZ.read_store(path, store=idx, store_offset=True)
			self.assertIsInstance(idx['k'], int)
			self.assertEqual(idx._values_cache['k'], ['k', 'v'])


class TestAppendAPI(unittest.TestCase):
	def test_append_and_tombstone(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_record(path, ['a', 'a', 'Ann'], create=True)
			TSVZ.append_record(path, ['a', 'a', 'Amy'])
			self.assertEqual(TSVZ.read_store(path)['a'], ['a', 'a', 'Amy'])
			TSVZ.append_record(path, ['a'])
			self.assertNotIn('a', TSVZ.read_store(path))

	def test_append_records_dict(self):
		with TempFile(suffix='.tsv') as path:
			TSVZ.append_records(path, {'a': ['a', '1'], 'b': ['b', '2']}, create=True)
			data = TSVZ.read_store(path)
			self.assertEqual(data['a'], ['a', '1'])

	def test_escape_in_value(self):
		with TempFile(suffix='.tsv') as path:
			TSVZ.append_record(path, ['k', 'a\tb'], create=True)
			with open(path) as f:
				self.assertIn('<sep>', f.read())
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'a\tb'])


class TestSnapshot(unittest.TestCase):
	def test_snapshot_collapses_history(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_record(path, ['a', '1'], create=True)
			TSVZ.append_record(path, ['a', '2'])
			TSVZ.append_record(path, ['b', '9'])
			TSVZ.snapshot_part(path)
			data = TSVZ.read_store(path)
			self.assertEqual(data['a'], ['a', '2'])
			self.assertEqual(data['b'], ['b', '9'])
			with open(path) as f:
				data_rows = [ln for ln in f if ln.strip() and not ln.startswith('#')]
			self.assertEqual(len(data_rows), 2)

	def test_snapshot_emits_version_marker(self):
		with TempFile(suffix='.tsv') as path:
			TSVZ.append_record(path, ['k', 'v'], create=True)
			TSVZ.snapshot_part(path)
			with open(path) as f:
				self.assertIn('#_version_#', f.read())


class TestDelimiterVariants(unittest.TestCase):
	def test_csv_psv_nsv(self):
		for suffix, delim in (('.csv', ','), ('.psv', '|'), ('.nsv', '\0')):
			with TempFile(suffix=suffix) as path:
				TSVZ.append_record(path, ['k', 'v'], create=True)
				self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v'])


class TestCompression(unittest.TestCase):
	def test_gzip_roundtrip(self):
		with TempFile(suffix='.tsv.gz') as path:
			TSVZ.append_record(path, ['k', 'v'], create=True)
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v'])


class TestWalStore(unittest.TestCase):
	def _store(self, path, **kw):
		return TSVZ.WalStore(path, create=True, flush_interval=0.001, **kw)

	def test_append_delete_close(self):
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path, header='id\tname')
			db['x'] = ['x', 'Xavier']
			db['y'] = ['y', 'Yvonne']
			del db['x']
			db.close()
			data = TSVZ.read_store(path)
			self.assertNotIn('x', data)
			self.assertEqual(data['y'], ['y', 'Yvonne'])

	def test_last_wins(self):
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path)
			db['k'] = ['k', '1']
			db['k'] = ['k', '2']
			db.flush()
			db.close()
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', '2'])

	def test_hash_key_not_persisted(self):
		with TempFile(suffix='.tsv') as path:
			db = self._store(path)
			db['#scratch'] = ['#scratch', 'tmp']
			db.flush()
			db.close()
			self.assertNotIn('#scratch', open(path).read())

	def test_missing_key_defaults(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.truncate_part(path, defaults=['guest', '0'])
			db = self._store(path)
			self.assertEqual(db['nobody'][1:], ['guest', '0'])
			db.close()

	def test_tombstone_line(self):
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path)
			db['k'] = ['k', 'v']
			del db['k']
			db.flush()
			db.close()
			self.assertIn('k\n', open(path).read() or 'k')


class TestOffsetStore(unittest.TestCase):
	def _store(self, path, **kw):
		return TSVZ.OffsetStore(path, create=True, **kw)

	def test_read_write_delete(self):
		with TempFile(suffix='.tsv') as path:
			s = self._store(path)
			s['k'] = ['k', 'val']
			self.assertEqual(s['k'], ['k', 'val'])
			s['k'] = ['k', 'val2']
			self.assertEqual(s['k'], ['k', 'val2'])
			del s['k']
			self.assertNotIn('k', s)
			s.close()

	def test_reload(self):
		with TempFile(suffix='.tsv') as path:
			s = self._store(path)
			s['k'] = ['k', 'v']
			s.close()
			s2 = self._store(path)
			s2.reload()
			self.assertEqual(s2['k'], ['k', 'v'])
			s2.close()

	def test_hash_key_in_memory_only(self):
		with TempFile(suffix='.tsv') as path:
			s = self._store(path)
			s['#note'] = ['#note', 'x']
			self.assertEqual(s['#note'], ['#note', 'x'])
			self.assertNotIn('#note', open(path).read())
			s.close()


class TestWriterHelpers(unittest.TestCase):
	def test_snapshot_preamble(self):
		lines = TSVZ.build_snapshot_preamble(TSVZ.ReaderState(), '\t')
		self.assertTrue(any('#_version_#' in ln for ln in lines))

	def test_queue_bytes(self):
		b = TSVZ._queue_item_to_bytes((TSVZ._TOMBSTONE, 'key'), '\t', 'utf8')
		self.assertEqual(b, b'key\n')
		b = TSVZ._queue_item_to_bytes([TSVZ.MARKER_DEFAULTS, 'a', 'b'], '\t', 'utf8')
		self.assertIn(b'#_defaults_#', b)


if __name__ == '__main__':
	unittest.main()
