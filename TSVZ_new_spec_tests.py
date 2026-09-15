"""Tests for TSVZ.py (tsvz-spec-v1 core conformance)."""
import gzip
import hashlib
import io
import os
import shutil
import sys
import tempfile
import threading
import time
import types
import unittest
import warnings
from collections import OrderedDict

import TSVZ


class TempFile:
	def __init__(self, suffix='.tsvz', content=None, encoding='utf8'):
		# delete=False: we own the file past close(), hence no context manager.
		self._tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)  # noqa: SIM115
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


def read_text(path, encoding='utf8'):
	"""Return a part's full contents as text."""
	with open(path, encoding=encoding) as f:
		return f.read()


def replay(content, delimiter='\t'):
	if isinstance(content, str):
		content = content.encode()
	store, state = TSVZ.replay_bytes(content, delimiter)
	rows = OrderedDict((k, list(v.row)) for k, v in store.items())
	return rows, state


def decode_header_line(line, delimiter='\t'):
	fields = [TSVZ.decode_field(f, delimiter) for f in line.split(delimiter)]
	fields[0] = fields[0].removeprefix('#')
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
		self.assertEqual(TSVZ.classify_record('#_checksum_sha256_#'), 'checksum')
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

	def test_read_offsets(self):
		with TempFile(suffix='.tsv', content='a\t1\nk\tv\n') as path:
			offsets, values, state = TSVZ.read_offsets(path)
			self.assertIsInstance(offsets['k'], int)
			self.assertEqual(values['k'], ['k', 'v'])
			self.assertEqual(state.defaults, [])
			with open(path, 'rb') as f:
				f.seek(offsets['k'])
				self.assertTrue(f.readline().startswith(b'k'))

	def test_read_store_returns_only_rows(self):
		with TempFile(suffix='.tsv', content='k\tv\n') as path:
			data = TSVZ.read_store(path)
			self.assertTrue(all(isinstance(v, list) for v in data.values()))


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
		with TempFile(suffix='.tsvz') as path:
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
		with TempFile(suffix='.tsvz') as path:
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

	def test_hash_key_is_persisted_as_escaped_data(self):
		# §8.3: a key beginning with '#' is real data, stored as <#>key.
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path)
			db['#scratch'] = ['#scratch', 'tmp']
			db.flush()
			db.close()
			raw = read_text(path)
			self.assertIn('<#>scratch', raw)
			self.assertEqual(TSVZ.read_store(path)['#scratch'], ['#scratch', 'tmp'])

	def test_lookup_honors_strip_false_on_trailing_space_key(self):
		# §10: with stripping off, 'foo ' and 'foo' are distinct keys.
		content = '#_strip_trailing_whites_#\tfalse\nfoo \tbar\n'
		with TempFile(suffix='.tsvz', content=content) as path:
			db = TSVZ.WalStore(path, create=False, flush_interval=1000)
			try:
				self.assertIn('foo ', db)
				self.assertNotIn('foo', db)
				self.assertEqual(db['foo '], ['foo ', 'bar'])
			finally:
				db.close()

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
			self.assertIn('k\n', read_text(path) or 'k')


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

	def test_hash_key_is_persisted_as_escaped_data(self):
		# §8.3: a key beginning with '#' is real data, stored as <#>key.
		with TempFile(suffix='.tsvz') as path:
			s = self._store(path)
			s['#note'] = ['#note', 'x']
			self.assertEqual(s['#note'], ['#note', 'x'])
			s.close()
			self.assertIn('<#>note', read_text(path))
			self.assertEqual(TSVZ.read_store(path)['#note'], ['#note', 'x'])

	def test_lookup_honors_strip_false_on_trailing_space_key(self):
		content = '#_strip_trailing_whites_#\tfalse\nfoo \tbar\n'
		with TempFile(suffix='.tsvz', content=content) as path:
			s = TSVZ.OffsetStore(path, create=False, cache_size=0)
			try:
				self.assertIn('foo ', s)
				self.assertNotIn('foo', s)
				self.assertEqual(s['foo '], ['foo ', 'bar'])
				key = next(iter(s))
				self.assertEqual(s[key], ['foo ', 'bar'])
			finally:
				s.close()

	def test_forward_only_binding_of_defaults(self):
		# §12.4.4 / §14.4: OffsetStore must resolve a row under the markers
		# in force at that row's offset, not the end-of-file ReaderState.
		content = '#_defaults_#\tA1\tA2\nk\tv\n#_defaults_#\tB1\tB2\tB3\n'
		with TempFile(suffix='.tsvz', content=content) as path:
			want = TSVZ.read_store(path)['k']
			self.assertEqual(want, ['k', 'v', 'A2'])
			s = TSVZ.OffsetStore(path, create=False, cache_size=0)
			try:
				self.assertEqual(s['k'], want)
			finally:
				s.close()

	def test_forward_only_binding_of_fill_empty(self):
		content = (
			'#_defaults_#\tD1\n#_fill_empty_with_default_#\ttrue\n'
			'k\t\tz\n#_fill_empty_with_default_#\tfalse\n'
		)
		with TempFile(suffix='.tsvz', content=content) as path:
			want = TSVZ.read_store(path)['k']
			self.assertEqual(want, ['k', 'D1', 'z'])
			s = TSVZ.OffsetStore(path, create=False, cache_size=0)
			try:
				self.assertEqual(s['k'], want)
			finally:
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


class TestAbsentColumnDefaults(unittest.TestCase):
	"""§14.3 — a value column beyond a row's written width takes its default."""

	def test_materialize_row_pads_to_defaults_width(self):
		self.assertEqual(
			TSVZ.materialize_row(TSVZ.StoreEntry(['k', 'v1'], ['D1', 'D2'], False)),
			['k', 'v1', 'D2'])

	def test_materialize_row_never_trims(self):
		self.assertEqual(
			TSVZ.materialize_row(TSVZ.StoreEntry(['k', 'a', 'b', 'c'], ['D1'], False)),
			['k', 'a', 'b', 'c'])

	def test_read_store_resolves_absent_column(self):
		with TempFile(suffix='.tsvz', content='#_defaults_#\tD1\tD2\nk\tv1\n') as path:
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v1', 'D2'])

	def test_forward_only_binding_of_defaults(self):
		# §12.4.4: the row binds the defaults in force at its own position.
		content = '#_defaults_#\tA1\tA2\nk\tv\n#_defaults_#\tB1\tB2\tB3\n'
		with TempFile(suffix='.tsvz', content=content) as path:
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v', 'A2'])

	def test_no_defaults_leaves_width_untouched(self):
		with TempFile(suffix='.tsvz', content='k\tv\n') as path:
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v'])


class TestSnapshotFidelity(unittest.TestCase):
	"""§19.4 — a snapshot must reproduce the exact observable read result."""

	def _roundtrip(self, content):
		with TempFile(suffix='.tsvz', content=content) as path:
			before = dict(TSVZ.read_store(path))
			TSVZ.snapshot_part(path)
			return before, dict(TSVZ.read_store(path))

	def test_preserves_trailing_whitespace(self):
		before, after = self._roundtrip('#_strip_trailing_whites_#\tfalse\nk\tval  \n')
		self.assertEqual(before['k'], ['k', 'val  '])
		self.assertEqual(after, before)

	def test_preserves_fill_empty_result(self):
		before, after = self._roundtrip(
			'#_defaults_#\tD1\tD2\n#_fill_empty_with_default_#\ttrue\nk\t\tz\n')
		self.assertEqual(before['k'], ['k', 'D1', 'z'])
		self.assertEqual(after, before)

	def test_preserves_hash_prefixed_key_on_disk(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['#hash', 'v'], ['plain', 'w']], create=True)
			before = dict(TSVZ.read_store(path))
			TSVZ.snapshot_part(path)
			self.assertEqual(dict(TSVZ.read_store(path)), before)
			self.assertIn('#hash', before)

	def test_preamble_pins_resolution_markers(self):
		lines = TSVZ.build_snapshot_preamble(TSVZ.ReaderState(), '\t')
		self.assertEqual(lines[0], '#_version_#\t1')
		self.assertIn('#_strip_trailing_whites_#\tfalse', lines)
		self.assertIn('#_fill_empty_with_default_#\tfalse', lines)

	def test_defaults_marker_emitted_last(self):
		st = TSVZ.ReaderState()
		st.defaults = ['x']
		st.return_on_missing = False
		self.assertTrue(TSVZ.build_snapshot_preamble(st, '\t')[-1].startswith('#_defaults_#'))

	def test_empty_store_is_still_compacted(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1'], ['a']], create=True)
			TSVZ.snapshot_part(path)
			body = [ln for ln in read_text(path).splitlines() if not ln.startswith('#')]
			self.assertEqual(body, [])

	def test_rewrite_is_atomic(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			original = read_text(path)
			real = TSVZ.format_data_row

			def boom(*a, **k):
				raise RuntimeError('simulated crash mid-snapshot')
			TSVZ.format_data_row = boom
			try:
				with self.assertRaises(RuntimeError):
					TSVZ.snapshot_part(path)
			finally:
				TSVZ.format_data_row = real
			self.assertEqual(read_text(path), original)
			siblings = os.listdir(os.path.dirname(path))
			self.assertFalse([n for n in siblings if n.endswith('.tmp')])


class TestWriterCoercion(unittest.TestCase):
	def test_falsy_values_survive(self):
		self.assertEqual(TSVZ._coerce_row(['a', 0, False], '\t'), ['a', '0', 'False'])

	def test_trailing_whitespace_is_not_stripped_on_write(self):
		self.assertEqual(TSVZ._coerce_row(['a', 'keep  '], '\t'), ['a', 'keep  '])

	def test_zero_round_trips(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['k', 0]], create=True)
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', '0'])

	def test_trailing_space_round_trips_when_stripping_off(self):
		with TempFile(suffix='.tsvz', content='#_strip_trailing_whites_#\tfalse\n') as path:
			TSVZ.append_records(path, [['k', 'v  ']])
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v  '])


class TestCompressionSuffixes(unittest.TestCase):
	def test_lzma_suffix_writes(self):
		with TempFile(suffix='.tsvz.lzma') as path:
			os.unlink(path)
			TSVZ.append_records(path, [['k', 'v']], create=True)
			self.assertEqual(dict(TSVZ.read_store(path)), {'k': ['k', 'v']})

	def test_xz_suffix_writes(self):
		with TempFile(suffix='.tsvz.xz') as path:
			os.unlink(path)
			TSVZ.append_records(path, [['k', 'v']], create=True)
			self.assertEqual(dict(TSVZ.read_store(path)), {'k': ['k', 'v']})

	def test_compression_suffix_helpers(self):
		self.assertTrue(TSVZ.is_compressed_path('a.tsvz.gz'))
		self.assertFalse(TSVZ.is_compressed_path('a.tsvz'))

	def test_snapshot_keeps_compression(self):
		with TempFile(suffix='.tsvz.gz') as path:
			os.unlink(path)
			TSVZ.append_records(path, [['a', '1'], ['a', '2']], create=True)
			TSVZ.snapshot_part(path)
			with gzip.open(path, 'rt') as f:
				self.assertIn('#_version_#', f.read())
			self.assertEqual(dict(TSVZ.read_store(path)), {'a': ['a', '2']})

	def test_atomic_rewrite_fsyncs_complete_gzip(self):
		# fsync must happen after the codec footer is written, otherwise a
		# crash can leave a truncated gzip that gzip.open rejects.
		import stat as _stat
		with TempFile(suffix='.tsvz.gz') as path:
			os.unlink(path)
			TSVZ.append_records(path, [['a', '1']], create=True)
			sizes = []
			real = os.fsync

			def spy(fd):
				info = os.fstat(fd)
				if _stat.S_ISREG(info.st_mode):
					sizes.append(info.st_size)
				return real(fd)

			os.fsync = spy
			try:
				TSVZ.snapshot_part(path)
			finally:
				os.fsync = real
			self.assertTrue(sizes, 'snapshot never fsynced the temp part')
			self.assertEqual(sizes[-1], os.path.getsize(path))


class TestWalStoreDurability(unittest.TestCase):
	def _store(self, path, **kw):
		kw.setdefault('flush_interval', 0.01)
		return TSVZ.WalStore(path, create=True, **kw)

	def test_opening_existing_store_does_not_truncate(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1'], ['b', '2']], create=True)
			on_disk = read_text(path)
			db = self._store(path)
			try:
				self.assertEqual(dict(db), {'a': ['a', '1'], 'b': ['b', '2']})
				self.assertEqual(read_text(path), on_disk)
				db.reload()
				self.assertEqual(dict(db), {'a': ['a', '1'], 'b': ['b', '2']})
				self.assertEqual(read_text(path), on_disk)
			finally:
				db.close()

	def test_reload_does_not_requeue_replayed_rows(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			db = self._store(path)
			try:
				db.reload()
				self.assertEqual(len(db._pending), 0)
			finally:
				db.close()

	def test_constructor_defaults_take_effect_and_persist(self):
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path, defaults=['n/a', '0'])
			try:
				self.assertEqual(db._reader_state.defaults, ['n/a', '0'])
				self.assertEqual(db['nobody'], ['nobody', 'n/a', '0'])
				db.flush()
			finally:
				db.close()
			self.assertIn('#_defaults_#', read_text(path))
			db2 = self._store(path)
			try:
				self.assertEqual(db2._reader_state.defaults, ['n/a', '0'])
			finally:
				db2.close()

	def test_flush_failure_requeues_instead_of_dropping(self):
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path, flush_interval=1000)
			try:
				db['a'] = ['a', '1']
				db.path = os.path.join(path + '.nodir', 'x.tsvz')
				with self.assertRaises(OSError):
					db.flush()
				self.assertEqual(len(db._pending), 1)
				self.assertFalse(db._lock.locked())  # lock released on failure
			finally:
				db.path = path
				db.close()

	def test_clear_does_not_resurrect_an_in_flight_flush(self):
		# Drain must hold _lock through the write; otherwise clear() truncates
		# and the popped batch is appended afterwards.
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path, flush_interval=1000)
			entered = threading.Event()
			release = threading.Event()
			real_open = db._open_locked
			try:
				db['gone'] = ['gone', '2']

				def delayed(mode, **kw):
					entered.set()
					if not release.wait(3.0):
						raise RuntimeError('test gate timed out')
					return real_open(mode, **kw)

				db._open_locked = delayed
				flusher = threading.Thread(target=db.flush)
				clearer = threading.Thread(target=db.clear)
				flusher.start()
				self.assertTrue(entered.wait(3.0), 'flush never reached the write')
				clearer.start()
				# If drain is outside the lock, clear runs now and the write
				# that follows resurrected the row. If drain holds the lock,
				# clear blocks until the write finishes, then truncates it.
				time.sleep(0.05)
				release.set()
				flusher.join(timeout=5)
				clearer.join(timeout=5)
				self.assertEqual(dict(db), {})
				self.assertEqual(dict(TSVZ.read_store(path)), {})
			finally:
				release.set()
				db._open_locked = real_open
				db.close()

	def test_close_does_not_wait_a_whole_interval(self):
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path, flush_interval=30)
			db['a'] = ['a', '1']
			start = time.monotonic()
			db.close()
			self.assertLess(time.monotonic() - start, 5)
			self.assertEqual(TSVZ.read_store(path)['a'], ['a', '1'])

	def test_flush_terminates_under_sustained_writes(self):
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path, flush_interval=1000)
			try:
				stop = threading.Event()

				def writer():
					i = 0
					while not stop.is_set():
						db[f'k{i % 10}'] = [f'k{i % 10}', str(i)]
						i += 1
				th = threading.Thread(target=writer, daemon=True)
				th.start()
				try:
					done = threading.Event()

					def flusher():
						db.flush()
						done.set()
					threading.Thread(target=flusher, daemon=True).start()
					self.assertTrue(done.wait(20), 'flush() never terminated')
				finally:
					stop.set()
					th.join(timeout=5)
			finally:
				db.close()

	def test_delete_missing_key_raises(self):
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path)
			try:
				with self.assertRaises(KeyError):
					del db['nope']
			finally:
				db.close()

	def test_closed_store_is_not_pinned_by_atexit(self):
		import gc
		import weakref
		with TempFile(suffix='.tsvz') as path:
			db = self._store(path)
			ref = weakref.ref(db)
			db.close()
			del db
			gc.collect()
			self.assertIsNone(ref(), 'closed WalStore still reachable')


class TestOffsetStoreHardening(unittest.TestCase):
	def test_rejects_compressed_path(self):
		with self.assertRaises(ValueError):
			TSVZ.OffsetStore('store.tsvz.gz', create=True)

	def test_flush_makes_writes_visible(self):
		with TempFile(suffix='.tsvz') as path:
			s = TSVZ.OffsetStore(path, create=True)
			try:
				s['a'] = ['a', '1']
				s.flush()
				self.assertEqual(TSVZ.read_store(path)['a'], ['a', '1'])
			finally:
				s.close()

	def test_hash_key_indexed_by_real_offset(self):
		with TempFile(suffix='.tsvz') as path:
			s = TSVZ.OffsetStore(path, create=True)
			try:
				s['#note'] = ['#note', 'x']
				self.assertIsInstance(s._offsets['#note'], int)
				s._values.clear()  # force a real seek+read at the stored offset
				self.assertEqual(s['#note'], ['#note', 'x'])
			finally:
				s.close()

	def test_delete_missing_key_raises(self):
		with TempFile(suffix='.tsvz') as path:
			s = TSVZ.OffsetStore(path, create=True)
			try:
				with self.assertRaises(KeyError):
					del s['nope']
			finally:
				s.close()

	def test_read_at_offset_after_write(self):
		with TempFile(suffix='.tsvz') as path:
			s = TSVZ.OffsetStore(path, create=True)
			try:
				s['a'] = ['a', '1']
				s['b'] = ['b', '2']
				s._values.clear()  # force a real seek+read
				self.assertEqual(s['b'], ['b', '2'])
			finally:
				s.close()


class TestMarkerConformance(unittest.TestCase):
	def test_marker_key_is_case_insensitive(self):
		_rows, state = replay('#_DEFAULTS_#\tD1\nk\n')
		self.assertEqual(state.defaults, ['D1'])

	def test_checksum_marker_classified_either_case(self):
		# §12.2.1 case-insensitive; §15.2 the algo is part of the key.
		self.assertEqual(TSVZ.classify_record('#_CHECKSUM_SHA256_#'), 'checksum')
		self.assertEqual(TSVZ.classify_record('#_checksum_sha256_#'), 'checksum')
		self.assertEqual(TSVZ.checksum_algorithm('#_CHECKSUM_SHA256_#'), 'sha256')

	def test_lone_marker_resets_to_builtin_default(self):
		# §12.1: a value-less stateful marker resets to its built-in default.
		_, state = replay('#_fill_empty_with_default_#\ttrue\n#_fill_empty_with_default_#\n')
		self.assertFalse(state.fill_empty)
		_, state = replay('#_strip_trailing_whites_#\tfalse\n#_strip_trailing_whites_#\n')
		self.assertTrue(state.strip_trailing)

	def test_version_clamped_to_reader_max(self):
		# §6.3: effective version = min(declared, reader max).
		_, state = replay('#_version_#\t9\n')
		self.assertEqual(state.version, TSVZ.MAX_SPEC_VERSION)

	def test_custom_double_underscore_marker_is_inert(self):
		# §12.2.2/§12.3.1: extension markers are ignored by this reader.
		rows, state = replay('#__mytool_meta__#\tx\nk\tv\n')
		self.assertEqual(list(rows), ['k'])
		self.assertEqual(state.defaults, [])

	def test_empty_key_row_is_ignored_not_data(self):
		self.assertEqual(
			TSVZ.process_record('\tval', TSVZ.ReaderState(), OrderedDict(), '\t'),
			('ignore', None))

	def test_store_offset_requires_an_offset(self):
		with self.assertRaises(ValueError):
			TSVZ.process_record('k\tv', TSVZ.ReaderState(), {}, '\t', store_offset=True)


class TestLegacyHardening(unittest.TestCase):
	def test_non_ascii_delimiter_not_mangled(self):
		self.assertEqual(TSVZ._legacy_delimiter(delimiter='§'), '§')

	def test_ascii_escape_still_expands(self):
		self.assertEqual(TSVZ._legacy_delimiter(delimiter='\\t'), '\t')

	def test_fit_row_does_not_append_to_a_live_store(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			db = TSVZ.WalStore(path, create=True, flush_interval=1000)
			try:
				with warnings.catch_warnings():
					warnings.simplefilter('ignore', DeprecationWarning)
					TSVZ.readTabularFile(path, taskDic=db, header=['id', 'v', 'w'])
				self.assertEqual(len(db._pending), 0)
			finally:
				db.close()


class TestDeprecations(unittest.TestCase):
	"""Legacy entry points warn but keep working (removal target: v5.0)."""

	def test_legacy_functions_warn(self):
		with TempFile(suffix='.tsvz') as path:
			cases = [
				(TSVZ.appendTabularFile, (path, ['k', 'v']), {'createIfNotExist': True}),
				(TSVZ.readTabularFile, (path,), {}),
				(TSVZ.scrubTabularFile, (path,), {}),
				(TSVZ.clearTabularFile, (path,), {}),
				(TSVZ.get_delimiter, (), {'file_name': path}),
			]
			for fn, args, kw in cases:
				with self.subTest(fn=fn.__name__), self.assertWarns(DeprecationWarning) as ctx:
					fn(*args, **kw)
				self.assertIn('TSVZ_old', str(ctx.warning))

	def test_legacy_classes_warn(self):
		with TempFile(suffix='.tsvz') as path:
			with self.assertWarns(DeprecationWarning):
				db = TSVZ.TSVZed(path, createIfNotExist=True, append_check_delay=0.01)
			db.close()
			with self.assertWarns(DeprecationWarning):
				lite = TSVZ.TSVZedLite(path, createIfNotExist=True)
			lite.close()

	def test_modern_api_does_not_warn(self):
		with TempFile(suffix='.tsvz') as path, warnings.catch_warnings():
			warnings.simplefilter('error', DeprecationWarning)
			TSVZ.append_records(path, [['k', 'v']], create=True)
			TSVZ.read_store(path)
			TSVZ.snapshot_part(path)
			TSVZ.truncate_part(path)
			db = TSVZ.WalStore(path, create=True, flush_interval=1000)
			db.close()
			s = TSVZ.OffsetStore(path, create=True)
			s.close()

	def test_legacy_still_functional(self):
		with warnings.catch_warnings():
			warnings.simplefilter('ignore', DeprecationWarning)
			with TempFile(suffix='.tsvz') as path:
				TSVZ.appendTabularFile(path, ['k', 'v'], createIfNotExist=True)
				self.assertEqual(dict(TSVZ.readTabularFile(path))['k'], ['k', 'v'])


class TestHashKeysArePersisted(unittest.TestCase):
	"""§8.3 — a '#'-prefixed key is ordinary data at every layer."""

	def test_walstore_offsetstore_and_functions_agree(self):
		for make in ('functions', 'wal', 'offset'):
			with self.subTest(layer=make), TempFile(suffix='.tsvz') as path:
				if make == 'functions':
					TSVZ.append_records(path, [['#k', 'v']], create=True)
				elif make == 'wal':
					db = TSVZ.WalStore(path, create=True, flush_interval=1000)
					db['#k'] = ['#k', 'v']
					db.close()
				else:
					s = TSVZ.OffsetStore(path, create=True)
					s['#k'] = ['#k', 'v']
					s.close()
				self.assertIn('<#>k', read_text(path))
				self.assertEqual(TSVZ.read_store(path)['#k'], ['#k', 'v'])

	def test_survives_snapshot(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['#k', 'v'], ['plain', 'w']], create=True)
			before = dict(TSVZ.read_store(path))
			TSVZ.snapshot_part(path)
			self.assertEqual(dict(TSVZ.read_store(path)), before)


class TestApiSplit(unittest.TestCase):
	"""read_store returns rows only; the old flag modes still work, loudly."""

	def test_store_offset_flag_warns_and_still_works(self):
		with TempFile(suffix='.tsv', content='k\tv\n') as path:
			idx = OrderedDict()
			with self.assertWarns(DeprecationWarning):
				TSVZ.read_store(path, store=idx, store_offset=True)
			self.assertIsInstance(idx['k'], int)
			self.assertEqual(idx._values_cache['k'], ['k', 'v'])

	def test_last_record_only_flag_warns_and_still_works(self):
		with TempFile(suffix='.tsv', content='a\t1\nb\t2\n') as path:
			with self.assertWarns(DeprecationWarning):
				got = TSVZ.read_store(path, last_record_only=True)
			self.assertEqual(got, ['b', '2'])

	def test_read_store_does_not_warn(self):
		with TempFile(suffix='.tsv', content='k\tv\n') as path, \
				warnings.catch_warnings():
			warnings.simplefilter('error', DeprecationWarning)
			TSVZ.read_store(path)
			TSVZ.read_offsets(path)
			TSVZ.read_last_record(path)

	def test_offset_store_reload_does_not_warn(self):
		with TempFile(suffix='.tsvz') as path, warnings.catch_warnings():
			warnings.simplefilter('error', DeprecationWarning)
			s = TSVZ.OffsetStore(path, create=True)
			s['k'] = ['k', 'v']
			s.reload()
			s.close()


class TestDeleteApi(unittest.TestCase):
	def test_delete_records(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1'], ['b', '2'], ['c', '3']], create=True)
			TSVZ.delete_records(path, ['a', 'c'])
			self.assertEqual(dict(TSVZ.read_store(path)), {'b': ['b', '2']})

	def test_delete_record_single(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			TSVZ.delete_record(path, 'a')
			self.assertEqual(dict(TSVZ.read_store(path)), {})

	def test_delete_absent_key_is_a_noop(self):
		# §9.3: a tombstone for an absent key has no effect.
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			TSVZ.delete_record(path, 'nope')
			self.assertEqual(dict(TSVZ.read_store(path)), {'a': ['a', '1']})

	def test_delete_writes_a_lone_key_row(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			TSVZ.delete_record(path, 'a')
			self.assertEqual(read_text(path).splitlines()[-1], 'a')

	def test_delete_escapes_the_key(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a\tb', 'v'], ['#h', 'v']], create=True)
			TSVZ.delete_records(path, ['a\tb', '#h'])
			self.assertEqual(dict(TSVZ.read_store(path)), {})


class TestPublicSurface(unittest.TestCase):
	def test_all_names_exist(self):
		self.assertEqual([n for n in TSVZ.__all__ if not hasattr(TSVZ, n)], [])

	def test_star_import_leaks_no_stdlib(self):
		ns = {}
		exec('from TSVZ import *', ns)  # noqa: S102
		leaked = [n for n in ('os', 'sys', 're', 'time', 'threading', 'atexit',
							  'contextlib', 'functools', 'warnings', 'deque',
							  'OrderedDict', 'MutableMapping') if n in ns]
		self.assertEqual(leaked, [])


class TestTsvzNewShim(unittest.TestCase):
	def test_preview_import_warns_and_reexports(self):
		sys.modules.pop('TSVZ_new', None)
		with self.assertWarns(DeprecationWarning) as ctx:
			import TSVZ_new  # noqa: PLC0415
		try:
			self.assertIn('import TSVZ', str(ctx.warning))
			self.assertIn('TSVZ_old', str(ctx.warning))
			self.assertIs(TSVZ_new.WalStore, TSVZ.WalStore)
		finally:
			sys.modules.pop('TSVZ_new', None)


class TestFlusherIsEventDriven(unittest.TestCase):
	def test_idle_store_does_not_busy_poll(self):
		with TempFile(suffix='.tsvz') as path:
			calls = [0]
			real = TSVZ.WalStore.flush

			def counted(self):
				calls[0] += 1
				return real(self)
			TSVZ.WalStore.flush = counted
			try:
				db = TSVZ.WalStore(path, create=True, flush_interval=0.01)
				time.sleep(1.0)
				db.close()
			finally:
				TSVZ.WalStore.flush = real
			# The old poll loop did ~100 in a second; allow generous slack.
			self.assertLess(calls[0], 15, f'idle store flushed {calls[0]}x in 1s')

	def test_write_is_flushed_without_waiting_for_the_interval(self):
		with TempFile(suffix='.tsvz') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=0.01)
			try:
				db['k'] = ['k', 'v']
				deadline = time.monotonic() + 5
				while time.monotonic() < deadline and not read_text(path):
					time.sleep(0.01)
				self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v'])
			finally:
				db.close()

	def test_close_is_prompt_with_a_long_interval(self):
		with TempFile(suffix='.tsvz') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=600)
			db['k'] = ['k', 'v']
			start = time.monotonic()
			db.close()
			self.assertLess(time.monotonic() - start, 5)
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v'])


class TestGetAlignment(unittest.TestCase):
	def _pairs(self, path):
		return [TSVZ.WalStore(path, create=True, flush_interval=600),
				TSVZ.OffsetStore(path, create=True)]

	def test_get_matches_getitem_on_missing(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.truncate_part(path, defaults=['guest', '0'])
			for store in self._pairs(path):
				with self.subTest(store=type(store).__name__):
					try:
						self.assertEqual(store['nobody'], store.get('nobody'))
						self.assertEqual(store.get('nobody'), ['nobody', 'guest', '0'])
					finally:
						store.close()

	def test_get_returns_default_when_return_on_missing_is_off(self):
		content = '#_return_defaults_when_missing_#\tfalse\n'
		with TempFile(suffix='.tsvz', content=content) as path:
			for store in self._pairs(path):
				with self.subTest(store=type(store).__name__):
					try:
						with self.assertRaises(KeyError):
							store['nobody']
						self.assertEqual(store.get('nobody', 'S'), 'S')
					finally:
						store.close()

	def test_membership_stays_literal(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.truncate_part(path, defaults=['guest'])
			for store in self._pairs(path):
				with self.subTest(store=type(store).__name__):
					try:
						store['real'] = ['real', 'v']
						self.assertIn('real', store)
						self.assertNotIn('nobody', store)
						self.assertEqual(len(store), 1)
					finally:
						store.close()


class TestWriteAck(unittest.TestCase):
	def test_default_is_memory(self):
		with TempFile(suffix='.tsvz') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=600)
			self.assertEqual(db.write_ack, TSVZ.WRITE_ACK_MEMORY)
			db.close()

	def test_marker_sets_the_mode(self):
		with TempFile(suffix='.tsvz', content='#_write_ack_#\tdisk\n') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=600)
			self.assertEqual(db.write_ack, TSVZ.WRITE_ACK_DISK)
			db.close()
			s = TSVZ.OffsetStore(path, create=True)
			self.assertEqual(s.write_ack, TSVZ.WRITE_ACK_DISK)
			s.close()

	def test_constructor_overrides_the_marker(self):
		with TempFile(suffix='.tsvz', content='#_write_ack_#\tdisk\n') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=600, write_ack='memory')
			self.assertEqual(db.write_ack, TSVZ.WRITE_ACK_MEMORY)
			db.close()

	def test_bad_mode_rejected(self):
		with TempFile(suffix='.tsvz') as path, self.assertRaises(ValueError):
			TSVZ.WalStore(path, create=True, flush_interval=600, write_ack='sometimes')

	def test_marker_never_affects_data(self):
		# §12.4.3: advisory markers have no effect on reconstructed data.
		a = 'k\tv\n'
		b = '#_write_ack_#\tdisk\nk\tv\n'
		with TempFile(suffix='.tsvz', content=a) as pa, TempFile(suffix='.tsvz', content=b) as pb:
			self.assertEqual(dict(TSVZ.read_store(pa)), dict(TSVZ.read_store(pb)))

	def test_both_modes_are_durable_after_close(self):
		for mode in (TSVZ.WRITE_ACK_MEMORY, TSVZ.WRITE_ACK_DISK):
			with self.subTest(mode=mode), TempFile(suffix='.tsvz') as path:
				db = TSVZ.WalStore(path, create=True, flush_interval=0.01, write_ack=mode)
				db['k'] = ['k', mode]
				db.close()
				self.assertEqual(TSVZ.read_store(path)['k'], ['k', mode])

	def test_snapshot_reemits_non_default_mode(self):
		with TempFile(suffix='.tsvz', content='#_write_ack_#\tdisk\nk\tv\n') as path:
			TSVZ.snapshot_part(path)
			self.assertIn('#_write_ack_#\tdisk', read_text(path))

	def test_snapshot_reemits_non_default_rotate(self):
		st = TSVZ.ReaderState()
		st.rotate = TSVZ.ROTATE_RENAME
		lines = TSVZ.build_snapshot_preamble(st, '\t')
		self.assertIn('#_rotate_#\trename', lines)
		self.assertNotIn('#_rotate_#\tkeep', lines)
		st.defaults = ['x']
		self.assertTrue(TSVZ.build_snapshot_preamble(st, '\t')[-1].startswith('#_defaults_#'))

	def test_disk_ack_propagates_fsync_failure(self):
		with TempFile(suffix='.tsvz') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=1000, write_ack='disk')
			real = os.fsync

			def boom(_fd):
				raise OSError(28, 'No space left on device')

			try:
				db['k'] = ['k', 'v']
				os.fsync = boom
				with self.assertRaises(OSError):
					db.flush()
				self.assertEqual(len(db._pending), 1)
				self.assertFalse(db._lock.locked())
			finally:
				os.fsync = real
				db.close()


class TestRecordIterator(unittest.TestCase):
	"""§4 framing, implemented once in _iter_records."""

	def _recs(self, data, **kw):
		"""Return (offset, text) pairs, dropping the blank records."""
		import io as _io
		return [(o, t) for o, t, _ in
				TSVZ._iter_records(_io.BytesIO(data), 'utf8', **kw) if t]

	def _raw(self, data, **kw):
		import io as _io
		return list(TSVZ._iter_records(_io.BytesIO(data), 'utf8', **kw))

	def test_offsets_and_crlf_and_blanks(self):
		# 'a\n'=2, blank '\n'=1, 'b\r\n'=3 -> 'c' starts at byte 6
		self.assertEqual(self._recs(b'a\n\nb\r\nc\n'),
						 [(0, 'a'), (3, 'b'), (6, 'c')])

	def test_torn_tail_discarded(self):
		self.assertEqual(self._recs(b'a\ntorn'), [(0, 'a')])

	def test_cr_mid_line_is_data(self):
		# §4.2: only a \r immediately before the terminator is stripped.
		self.assertEqual(self._recs(b'a\rb\n'), [(0, 'a\rb')])

	def test_offsets_address_the_real_bytes(self):
		data = b'alpha\tone\nbeta\ttwo\n'
		for offset, text in self._recs(data):
			self.assertTrue(data[offset:].startswith(text.encode()))

	def test_blank_records_are_yielded_with_their_bytes(self):
		# §15.4: a blank line's bytes still belong to an open digest segment.
		self.assertEqual(self._raw(b'a\n\nb\n'),
						 [(0, 'a', b'a\n'), (2, '', b'\n'), (3, 'b', b'b\n')])

	def test_raw_bytes_are_untouched(self):
		self.assertEqual([r for _, _, r in self._raw(b'a\r\nb\n')],
						 [b'a\r\n', b'b\n'])

	def test_bad_utf8_warns_once_and_replaces(self):
		data = b'a\tok\nb\t\xff\xfe\nc\t\xff\n'
		with self.assertWarns(UnicodeWarning):
			recs = self._recs(data, source='part.tsvz')
		self.assertEqual(len(recs), 3)
		self.assertIn('\ufffd', recs[1][1])

	def test_strict_errors_raise(self):
		with self.assertRaises(UnicodeDecodeError):
			self._recs(b'a\t\xff\n', errors='strict')

	def test_read_store_warns_on_corrupt_part(self):
		with TempFile(suffix='.tsvz', content=b'k\tv\nbad\t\xff\n') as path:
			with self.assertWarns(UnicodeWarning):
				data = TSVZ.read_store(path)
			self.assertEqual(data['k'], ['k', 'v'])


class TestOffsetStoreIsLazy(unittest.TestCase):
	def _fill(self, path, n=500):
		TSVZ.append_records(path, [[f'k{i}', f'v{i}'] for i in range(n)], create=True)

	def test_reload_does_not_materialize_every_row(self):
		with TempFile(suffix='.tsvz') as path:
			self._fill(path)
			s = TSVZ.OffsetStore(path, create=True, cache_size=16)
			try:
				self.assertEqual(len(s._offsets), 500)
				self.assertEqual(len(s._values), 0, 'replay should build the index only')
			finally:
				s.close()

	def test_reads_come_from_disk_and_are_correct(self):
		with TempFile(suffix='.tsvz') as path:
			self._fill(path)
			s = TSVZ.OffsetStore(path, create=True, cache_size=16)
			try:
				for i in (0, 1, 250, 499):
					self.assertEqual(s[f'k{i}'], [f'k{i}', f'v{i}'])
			finally:
				s.close()

	def test_cache_is_bounded(self):
		with TempFile(suffix='.tsvz') as path:
			self._fill(path)
			s = TSVZ.OffsetStore(path, create=True, cache_size=16)
			try:
				for i in range(500):
					s[f'k{i}']
				self.assertLessEqual(len(s._values), 16)
			finally:
				s.close()

	def test_cache_size_zero_disables_caching(self):
		with TempFile(suffix='.tsvz') as path:
			self._fill(path, 20)
			s = TSVZ.OffsetStore(path, create=True, cache_size=0)
			try:
				self.assertEqual(s['k5'], ['k5', 'v5'])
				self.assertEqual(len(s._values), 0)
			finally:
				s.close()

	def test_listview_sees_every_row_not_just_cached(self):
		with TempFile(suffix='.tsvz') as path:
			self._fill(path, 100)
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				lite = TSVZ.TSVZedLite(path, createIfNotExist=True)
			try:
				lite._cache_size = 8
				self.assertEqual(len(lite.getListView()), 100)
			finally:
				lite.close()


class TestEscapeFastPath(unittest.TestCase):
	"""The '<' guard must be indistinguishable from the character walk."""

	def _slow_decode(self, raw, delimiter):
		out, i = [], 0
		while i < len(raw):
			if raw[i] == '<':
				end = raw.find('>', i + 1)
				if end == -1:
					out.append(raw[i:])
					break
				name = raw[i + 1:end]
				out.append({'sep': delimiter, 'LF': '\n', 'lt': '<',
							'#': '#'}.get(name, raw[i:end + 1]))
				i = end + 1
			else:
				out.append(raw[i])
				i += 1
		return ''.join(out)

	def test_fast_path_matches_character_walk(self):
		import random
		import string
		rnd = random.Random(7)
		alphabet = list(string.printable) + ['<', '>', '#', '\t', '\n']
		for _ in range(20000):
			s = ''.join(rnd.choice(alphabet) for _ in range(rnd.randint(0, 10)))
			self.assertEqual(TSVZ.decode_field(s, '\t'), self._slow_decode(s, '\t'))

	def test_encode_decode_round_trip_still_total(self):
		import random
		import string
		rnd = random.Random(11)
		alphabet = list(string.printable) + ['<', '>', '#', '\t', '\n']
		for _ in range(20000):
			s = ''.join(rnd.choice(alphabet) for _ in range(rnd.randint(0, 10)))
			for is_key in (False, True):
				enc = TSVZ.encode_field(s, '\t', is_key=is_key)
				self.assertEqual(TSVZ.decode_field(enc, '\t'), s)
				self.assertNotIn('\n', enc)
				self.assertNotIn('\t', enc)

	def test_fast_path_still_escapes_leading_hash_key(self):
		self.assertEqual(TSVZ.encode_field('#k', '\t', is_key=True), '<#>k')
		self.assertEqual(TSVZ.encode_field('#k', '\t', is_key=False), '#k')

	def test_fast_path_respects_a_non_tab_delimiter(self):
		self.assertEqual(TSVZ.encode_field('a,b', ',', is_key=False), 'a<sep>b')
		self.assertEqual(TSVZ.encode_field('a,b', '\t', is_key=False), 'a,b')

	def test_plain_field_is_returned_unchanged(self):
		plain = 'ordinary_value_123'
		self.assertIs(TSVZ.decode_field(plain, '\t'), plain)


class TestLooseExtensionWarning(unittest.TestCase):
	"""§5.2 — loose extensions promise generic-tool readability."""

	def test_plain_rows_to_loose_ext_are_silent(self):
		with TempFile(suffix='.tsv') as path, warnings.catch_warnings():
			warnings.simplefilter('error', UserWarning)
			TSVZ.append_records(path, [['k', 'plain'], ['j', 'also plain']],
								create=True)

	def test_escape_token_to_loose_ext_warns(self):
		with TempFile(suffix='.tsv') as path, self.assertWarns(UserWarning):
			TSVZ.append_records(path, [['k', 'a\tb']], create=True)

	def test_marker_to_loose_ext_warns(self):
		with TempFile(suffix='.tsv') as path, self.assertWarns(UserWarning):
			TSVZ.truncate_part(path, defaults=['guest'])

	def test_strict_ext_never_warns(self):
		for suffix in ('.tsvz', '.csvz', '.psvz'):
			with self.subTest(suffix=suffix), TempFile(suffix=suffix) as path, \
					warnings.catch_warnings():
				warnings.simplefilter('error', UserWarning)
				TSVZ.append_records(path, [['k', 'a\tb'], ['#h', 'v']], create=True)
				TSVZ.truncate_part(path, defaults=['guest'])
				TSVZ.snapshot_part(path)

	def test_store_warns_once_then_stays_quiet(self):
		with TempFile(suffix='.tsv') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=600)
			try:
				db['k'] = ['k', 'a\tb']
				with self.assertWarns(UserWarning):
					db.flush()
				with warnings.catch_warnings():
					warnings.simplefilter('error', UserWarning)
					db['j'] = ['j', 'c\td']
					db.flush()
			finally:
				db.close()

	def test_offsetstore_warns_on_loose_ext(self):
		with TempFile(suffix='.tsv') as path:
			s = TSVZ.OffsetStore(path, create=True)
			try:
				with self.assertWarns(UserWarning):
					s['k'] = ['k', 'a\tb']
			finally:
				s.close()

	def test_warning_does_not_change_what_is_written(self):
		with TempFile(suffix='.tsv') as loose, TempFile(suffix='.tsvz') as strict:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', UserWarning)
				for p in (loose, strict):
					TSVZ.append_records(p, [['k', 'a\tb']], create=True)
			self.assertEqual(read_text(loose), read_text(strict))


class TestAtomicRewriteConcurrency(unittest.TestCase):
	def test_parallel_snapshots_do_not_collide(self):
		paths = []
		try:
			for i in range(6):
				tf = TempFile(suffix=f'.p{i}.tsvz')
				paths.append(tf.path)
				TSVZ.append_records(tf.path, [[f'k{j}', str(j)] for j in range(50)],
									create=True)
			errors = []

			def snap(p):
				try:
					TSVZ.snapshot_part(p)
				except Exception as exc:  # noqa: BLE001
					errors.append(exc)
			threads = [threading.Thread(target=snap, args=(p,)) for p in paths]
			for t in threads:
				t.start()
			for t in threads:
				t.join()
			self.assertEqual(errors, [])
			for p in paths:
				self.assertEqual(len(TSVZ.read_store(p)), 50)
			leftovers = [n for n in os.listdir(os.path.dirname(paths[0]))
						 if n.endswith('.tmp')]
			self.assertEqual(leftovers, [])
		finally:
			for p in paths:
				try:
					os.unlink(p)
				except FileNotFoundError:
					pass


class TestCli(unittest.TestCase):
	MOD = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'TSVZ.py')

	def run_cli(self, *args):
		import subprocess
		return subprocess.run([sys.executable, self.MOD, *args],
							  capture_output=True, text=True, check=False)

	def test_missing_part_is_lenient_by_default(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			r = self.run_cli(path, 'read')
			self.assertEqual(r.returncode, 0)

	def test_missing_part_with_strict_exits_nonzero(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			r = self.run_cli(path, 'read', '--strict')
			self.assertEqual(r.returncode, 1)
			self.assertIn('no such part', r.stderr)

	def test_delete_without_key_is_a_usage_error(self):
		with TempFile(suffix='.tsvz') as path:
			r = self.run_cli(path, 'delete')
			self.assertEqual(r.returncode, 2)

	def test_round_trip(self):
		with TempFile(suffix='.tsvz') as path:
			self.assertEqual(self.run_cli(path, 'append', 'k', 'v1', 'v2').returncode, 0)
			self.assertEqual(self.run_cli(path, 'append', '#h', 'z').returncode, 0)
			out = self.run_cli(path, 'read').stdout
			self.assertIn('v1', out)
			self.assertIn('#h', out)
			self.assertEqual(self.run_cli(path, 'delete', 'k').returncode, 0)
			self.assertEqual(self.run_cli(path, 'scrub').returncode, 0)
			self.assertEqual(dict(TSVZ.read_store(path)), {'#h': ['#h', 'z']})

	def test_no_verbose_flag(self):
		r = self.run_cli('--help')
		self.assertNotIn('--verbose', r.stdout)

	def test_help_lists_breaking_changes(self):
		r = self.run_cli('--help')
		self.assertIn('TSVZ_old', r.stdout)
		self.assertIn('not compatible with TSVZ 3.x', r.stdout)
		self.assertIn('tombstone', r.stdout.lower())

	def test_field_escapes_are_expanded(self):
		with TempFile(suffix='.tsvz') as path:
			self.run_cli(path, 'append', 'k', 'a\\tb')
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'a\tb'])

	def _numbered_store(self):
		d = tempfile.mkdtemp()
		base = os.path.join(d, 'st.tsvz')
		ords = sorted(TSVZ.new_ordinal() for _ in range(2))
		TSVZ.append_records(TSVZ.part_path(base, ords[0]), [['a', '1']], create=True)
		TSVZ.append_records(TSVZ.part_path(base, ords[1]), [['b', '2']], create=True)
		return d, base, ords

	def test_append_to_numbered_store_without_m(self):
		import shutil
		d, base, ords = self._numbered_store()
		try:
			r = self.run_cli(base, 'append', 'c', '3')
			self.assertEqual(r.returncode, 0, r.stderr)
			self.assertFalse(os.path.isfile(base), 'must not create a ghost unnumbered file')
			self.assertEqual(dict(TSVZ.read_multipart(base))['c'], ['c', '3'])
			self.assertIn('c\t3', read_text(TSVZ.part_path(base, ords[-1])))
		finally:
			shutil.rmtree(d, ignore_errors=True)

	def test_read_numbered_store_without_m(self):
		import shutil
		d, base, _ords = self._numbered_store()
		try:
			out = self.run_cli(base, 'read').stdout
			self.assertIn('1', out)
			self.assertIn('2', out)
		finally:
			shutil.rmtree(d, ignore_errors=True)

	def test_scrub_numbered_store_without_m(self):
		import shutil
		d, base, _ords = self._numbered_store()
		try:
			r = self.run_cli(base, 'scrub')
			self.assertEqual(r.returncode, 0, r.stderr)
			self.assertIn('subsumed', r.stdout)
			self.assertEqual(sorted(TSVZ.read_multipart(base)), ['a', 'b'])
		finally:
			shutil.rmtree(d, ignore_errors=True)

	def test_clear_numbered_store_tombstones_newest_part(self):
		import shutil
		d, base, ords = self._numbered_store()
		try:
			r = self.run_cli(base, 'clear')
			self.assertEqual(r.returncode, 0, r.stderr)
			self.assertFalse(os.path.isfile(base))
			self.assertEqual(dict(TSVZ.read_multipart(base)), {})
			self.assertTrue(os.path.isfile(TSVZ.part_path(base, ords[0])))
		finally:
			shutil.rmtree(d, ignore_errors=True)


class TestFlusherSurvivesFailures(unittest.TestCase):
	def test_non_oserror_in_flush_does_not_kill_the_worker(self):
		with TempFile(suffix='.tsvz') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=0.01)
			try:
				boom = [3]
				real = TSVZ.WalStore.flush

				def flaky(self):
					if boom[0] > 0:
						boom[0] -= 1
						raise RuntimeError('not an OSError')
					return real(self)
				TSVZ.WalStore.flush = flaky
				try:
					db['k'] = ['k', 'v']
					deadline = time.monotonic() + 10
					while time.monotonic() < deadline and boom[0] > 0:
						time.sleep(0.02)
				finally:
					TSVZ.WalStore.flush = real
				self.assertTrue(db._worker.is_alive(), 'flusher thread died')
				deadline = time.monotonic() + 10
				while time.monotonic() < deadline and db._pending:
					time.sleep(0.02)
				self.assertEqual(len(db._pending), 0, 'queue never drained')
			finally:
				db.close()
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'v'])


class TestGetListViewDeprecated(unittest.TestCase):
	def test_public_name_warns(self):
		with self.assertWarns(DeprecationWarning):
			TSVZ.getListView({'a': ['a', '1']})

	def test_internal_helper_is_silent(self):
		with warnings.catch_warnings():
			warnings.simplefilter('error', DeprecationWarning)
			self.assertEqual(TSVZ._list_view({'a': ['a', '1']}), [['a', '1']])

	def test_store_listviews_do_not_warn(self):
		with TempFile(suffix='.tsvz') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				db = TSVZ.TSVZed(path, createIfNotExist=True, append_check_delay=0.01)
			db['k'] = ['k', 'v']
			try:
				with warnings.catch_warnings():
					warnings.simplefilter('error', DeprecationWarning)
					self.assertEqual(db.getListView(), [['k', 'v']])
			finally:
				db.close()


class TestIntegrity(unittest.TestCase):
	"""§15 — opt-in, detection-only integrity."""

	def test_crc_vectors(self):
		for algo, want in (('crc32', 'cbf43926'), ('crc32c', 'e3069283')):
			with self.subTest(algo=algo):
				d = TSVZ.new_digest(algo)
				d.update(b'123456789')
				self.assertEqual(d.hexdigest(), want)

	def test_unknown_algorithm_has_no_accumulator(self):
		self.assertIsNone(TSVZ.new_digest('definitely-not-real'))
		self.assertIsNone(TSVZ.new_digest('shake_128'))

	def test_no_markers_means_no_checking(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			d = TSVZ.verify_part(path)
			self.assertEqual((d.verified, d.mismatches, bool(d)), (0, [], False))

	def test_first_marker_arms_and_ignores_its_value(self):
		# §15.3: nothing precedes it, so a value would have nothing to verify.
		content = 'a\t1\n#_checksum_sha256_#\tdeadbeef\nb\t2\n'
		with TempFile(suffix='.tsvz', content=content) as path:
			d = TSVZ.verify_part(path)
			self.assertEqual((d.verified, d.mismatches), (0, []))

	def test_segment_excludes_content_before_arming(self):
		seg = b'b\t2\nc\t3\n'
		digest = hashlib.sha256(seg).hexdigest().encode()
		body = b'a\t1\n#_checksum_sha256_#\n' + seg + b'#_checksum_sha256_#\t' + digest + b'\n'
		with TempFile(suffix='.tsvz', content=body) as path:
			d = TSVZ.verify_part(path)
			self.assertEqual((d.verified, d.mismatches), (1, []))

	def test_mismatch_is_detected(self):
		body = b'#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\t00000000\n'
		with TempFile(suffix='.tsvz', content=body) as path:
			d = TSVZ.verify_part(path)
			self.assertEqual(len(d.mismatches), 1)
			self.assertEqual(d.mismatches[0][0], 'crc32')

	def test_empty_value_resets_without_verifying(self):
		body = b'#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\nb\t2\n'
		with TempFile(suffix='.tsvz', content=body) as path:
			d = TSVZ.verify_part(path)
			self.assertEqual((d.verified, d.mismatches), (0, []))

	def test_unimplemented_algorithm_is_inert(self):
		body = b'#_checksum_notreal_#\na\t1\n#_checksum_notreal_#\tffffffff\n'
		with TempFile(suffix='.tsvz', content=body) as path:
			d = TSVZ.verify_part(path)
			self.assertEqual((d.armed, d.mismatches), (frozenset(), []))
			self.assertEqual(dict(TSVZ.read_store(path)), {'a': ['a', '1']})

	def test_other_algorithms_markers_are_ordinary_content(self):
		# §15.4: a checksum line is withheld only from its OWN accumulator.
		inner, crc_line = b'x\t1\n', b'#_checksum_crc32_#\n'
		digest = hashlib.sha256(inner + crc_line).hexdigest().encode()
		body = (b'#_checksum_sha256_#\n' + inner + crc_line
				+ b'#_checksum_sha256_#\t' + digest + b'\n')
		with TempFile(suffix='.tsvz', content=body) as path:
			d = TSVZ.verify_part(path)
			self.assertEqual((d.verified, d.mismatches), (1, []))

	def test_blank_and_cr_bytes_are_in_the_segment(self):
		for seg in (b'a\t1\n\nb\t2\n', b'a\t1\r\n'):
			with self.subTest(seg=seg):
				acc = TSVZ.new_digest('crc32')
				acc.update(seg)
				body = (b'#_checksum_crc32_#\n' + seg
						+ b'#_checksum_crc32_#\t' + acc.hexdigest().encode() + b'\n')
				with TempFile(suffix='.tsvz', content=body) as path:
					d = TSVZ.verify_part(path)
					self.assertEqual((d.verified, d.mismatches), (1, []))

	def test_append_checksum_round_trip(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			self.assertEqual(TSVZ.append_checksum(path, 'crc32'), '')
			TSVZ.append_records(path, [['b', '2']])
			self.assertNotEqual(TSVZ.append_checksum(path, 'crc32'), '')
			d = TSVZ.verify_part(path)
			self.assertEqual((d.verified, d.mismatches), (1, []))

	def test_staggered_algorithms_cover_each_others_digest_lines(self):
		# §15.6: a digest cannot cover its own marker; two algorithms can.
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			TSVZ.append_checksum(path, 'crc32')
			TSVZ.append_records(path, [['b', '2']])
			TSVZ.append_checksum(path, 'sha256')
			TSVZ.append_records(path, [['c', '3']])
			TSVZ.append_checksum(path, 'crc32')
			TSVZ.append_records(path, [['d', '4']])
			TSVZ.append_checksum(path, 'sha256')
			self.assertEqual(TSVZ.verify_part(path).verified, 2)
			raw = bytearray(read_text(path).encode())
			i = raw.index(b'#_checksum_sha256_#\t')
			j = raw.index(b'\n', i)
			raw[j - 1] = ord('0') if raw[j - 1] != ord('0') else ord('1')
			with open(path, 'wb') as f:
				f.write(bytes(raw))
			self.assertTrue(TSVZ.verify_part(path).mismatches)

	def test_append_checksum_rejects_inert_algorithm(self):
		with TempFile(suffix='.tsvz') as path, self.assertRaises(ValueError):
			TSVZ.append_checksum(path, 'not-an-algo')

	def test_policies(self):
		body = b'#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\tbadbad00\n'
		with TempFile(suffix='.tsvz', content=body) as path:
			with self.assertWarns(TSVZ.IntegrityWarning):
				TSVZ.read_store(path)
			with self.assertRaises(TSVZ.IntegrityError):
				TSVZ.read_store(path, policy='raise')
			with warnings.catch_warnings():
				warnings.simplefilter('error')
				data = TSVZ.read_store(path, policy='ignore')
			self.assertTrue(data._digests.mismatches)

	def test_works_over_compression(self):
		with TempFile(suffix='.tsvz.gz') as path:
			os.unlink(path)
			TSVZ.append_records(path, [['a', '1']], create=True)
			TSVZ.append_checksum(path, 'sha256')
			TSVZ.append_records(path, [['b', '2']])
			TSVZ.append_checksum(path, 'sha256')
			self.assertEqual(TSVZ.verify_part(path).verified, 1)


class TestMultiPart(unittest.TestCase):
	"""§17 — a store split across ordinal-suffixed parts."""

	def setUp(self):
		self.dir = tempfile.mkdtemp()

	def base(self, name='ev.tsvz'):
		return os.path.join(self.dir, name)

	def test_filename_grammar(self):
		pn = TSVZ.parse_part_name('/d/events.tsvz.1f')
		self.assertEqual((pn.store_path, pn.format_ext, pn.ordinal_value,
						  pn.rotated, pn.codec), ('/d/events', 'tsvz', 31, False, ''))
		pn = TSVZ.parse_part_name('/d/events.tsvz.0a.rotated.zst')
		self.assertEqual((pn.ordinal_value, pn.rotated, pn.codec), (10, True, 'zst'))
		self.assertIsNone(TSVZ.parse_part_name('/d/events.tsvz'))
		self.assertIsNone(TSVZ.parse_part_name('/d/events.tsvz.gz'))

	def test_ordinals_sort_as_integers_not_strings(self):
		# §17.3: .f is 15 and precedes .10 (16), though it follows it lexically.
		base = self.base()
		TSVZ.append_records(TSVZ.part_path(base, 0xF), [['k', 'from-f']], create=True)
		TSVZ.append_records(TSVZ.part_path(base, 0x10), [['k', 'from-10']], create=True)
		self.assertEqual([os.path.basename(p) for p in TSVZ.store_part_paths(base)],
						 ['ev.tsvz.f', 'ev.tsvz.10'])
		self.assertEqual(dict(TSVZ.read_multipart(base))['k'], ['k', 'from-10'])

	def test_marker_state_spans_parts(self):
		base = self.base('m.tsvz')
		with open(TSVZ.part_path(base, 1), 'w') as f:
			f.write('#_defaults_#\tD1\tD2\n')
		with open(TSVZ.part_path(base, 2), 'w') as f:
			f.write('k\tv\n')
		self.assertEqual(dict(TSVZ.read_multipart(base))['k'], ['k', 'v', 'D2'])

	def test_digest_segment_spans_parts(self):
		base = self.base('c.tsvz')
		seg = b'x\t1\n'
		with open(TSVZ.part_path(base, 1), 'wb') as f:
			f.write(b'#_checksum_sha256_#\n' + seg)
		with open(TSVZ.part_path(base, 2), 'wb') as f:
			f.write(b'#_checksum_sha256_#\t' + hashlib.sha256(seg).hexdigest().encode() + b'\n')
		data = TSVZ.read_multipart(base, policy='ignore')
		self.assertEqual((data._digests.verified, data._digests.mismatches), (1, []))

	def test_tombstone_in_a_later_part_deletes(self):
		base = self.base()
		TSVZ.append_records(TSVZ.part_path(base, 1), [['a', '1'], ['b', '2']], create=True)
		TSVZ.append_records(TSVZ.part_path(base, 2), [['a']], create=True)
		self.assertEqual(dict(TSVZ.read_multipart(base)), {'b': ['b', '2']})

	def test_rotated_parts_excluded(self):
		base = self.base()
		TSVZ.append_records(TSVZ.part_path(base, 1), [['a', '1']], create=True)
		TSVZ.append_records(TSVZ.part_path(base, 2, rotated=True), [['z', 'ghost']], create=True)
		self.assertNotIn('z', TSVZ.read_multipart(base))
		self.assertIn('z', TSVZ.read_multipart(base, include_rotated=True))

	def test_uuid7_ordinals(self):
		o = TSVZ.new_ordinal()
		self.assertEqual(len(o), 32)
		self.assertEqual((int(o, 16) >> 76) & 0xF, 7)
		self.assertNotEqual(TSVZ.new_ordinal(), TSVZ.new_ordinal())
		# Fixed 32-wide zero-padded hex, so §17.3's integer order and plain
		# string order agree -- and UUIDv7 makes both chronological.
		batch = [TSVZ.new_ordinal() for _ in range(50)]
		self.assertEqual(sorted(batch), sorted(batch, key=lambda o: int(o, 16)))
		self.assertEqual(len({len(o) for o in batch}), 1)

	def test_format_inference_understands_part_names(self):
		# §16.3: strip codec, .rotated and the ordinal before inferring.
		self.assertEqual(TSVZ.delimiter_for_path('ev.csvz.1f'), ',')
		self.assertEqual(TSVZ.delimiter_for_path('ev.psvz.0a.rotated'), '|')
		self.assertEqual(TSVZ.delimiter_for_path('ev.nsvz.3.gz'), '\0')
		self.assertTrue(TSVZ.is_strict_store('ev.csvz.1f'))
		self.assertFalse(TSVZ.is_strict_store('ev.csv.1f'))

	def test_csvz_multipart_round_trip(self):
		base = self.base('ev.csvz')
		TSVZ.append_records(TSVZ.part_path(base, 1), [['k', 'a,b']], create=True)
		self.assertIn('<sep>', read_text(TSVZ.part_path(base, 1)))
		self.assertEqual(dict(TSVZ.read_multipart(base))['k'], ['k', 'a,b'])

	def test_single_file_is_the_one_part_case(self):
		base = self.base()
		TSVZ.append_records(base, [['a', '1']], create=True)
		self.assertEqual(dict(TSVZ.read_multipart(base)), {'a': ['a', '1']})

	def test_missing_store_raises(self):
		with self.assertRaises(FileNotFoundError):
			TSVZ.read_multipart(self.base('nope.tsvz'))

	def test_checksum_segment_closes_across_parts(self):
		# §15.4 + §17.4: a marker in a later part closes a segment armed in an
		# earlier one, so append_checksum must replay the preceding parts.
		base = self.base()
		o = sorted(TSVZ.new_ordinal() for _ in range(2))
		p0, p1 = TSVZ.part_path(base, o[0]), TSVZ.part_path(base, o[1])
		TSVZ.append_records(p0, [['a', '1']], create=True)
		TSVZ.append_checksum(p0, 'sha256')
		TSVZ.append_records(p0, [['b', '2']])
		TSVZ.append_records(p1, [['c', '3']], create=True)
		self.assertNotEqual(TSVZ.append_checksum(p1, 'sha256'), '',
							'a later part must close, not re-arm, the segment')
		data = TSVZ.read_multipart(base, policy='ignore')
		self.assertEqual((data._digests.verified, data._digests.mismatches), (1, []))

	def test_cross_part_tampering_is_detected(self):
		base = self.base()
		o = sorted(TSVZ.new_ordinal() for _ in range(2))
		p0, p1 = TSVZ.part_path(base, o[0]), TSVZ.part_path(base, o[1])
		TSVZ.append_records(p0, [['a', '1']], create=True)
		TSVZ.append_checksum(p0, 'sha256')
		TSVZ.append_records(p0, [['b', '2']])
		TSVZ.append_records(p1, [['c', '3']], create=True)
		TSVZ.append_checksum(p1, 'sha256')
		with open(p0, 'rb') as f:
			raw = bytearray(f.read())
		raw[raw.index(b'b\t2') + 2] = ord('9')
		with open(p0, 'wb') as f:
			f.write(bytes(raw))
		with self.assertRaises(TSVZ.IntegrityError):
			TSVZ.read_multipart(base, policy='raise')

	def test_standalone_checksum_ignores_other_parts(self):
		base = self.base()
		o = sorted(TSVZ.new_ordinal() for _ in range(2))
		p0, p1 = TSVZ.part_path(base, o[0]), TSVZ.part_path(base, o[1])
		TSVZ.append_records(p0, [['a', '1']], create=True)
		TSVZ.append_checksum(p0, 'sha256')
		TSVZ.append_records(p1, [['b', '2']], create=True)
		# preceding=() opts out of the cross-part scan: this arms afresh.
		self.assertEqual(TSVZ.append_checksum(p1, 'sha256', preceding=()), '')

	def test_walstore_opens_a_fresh_part(self):
		base = self.base()
		TSVZ.append_records(TSVZ.part_path(base, 1), [['a', '1']], create=True)
		db = TSVZ.WalStore(base, multipart=True, create=True, flush_interval=600)
		try:
			self.assertEqual(dict(db), {'a': ['a', '1']})
			self.assertNotEqual(db.path, TSVZ.part_path(base, 1))
			db['b'] = ['b', '2']
		finally:
			db.close()
		self.assertEqual(read_text(TSVZ.part_path(base, 1)), 'a\t1\n')
		self.assertEqual(dict(TSVZ.read_multipart(base)),
						 {'a': ['a', '1'], 'b': ['b', '2']})

	def test_multipart_clear_uses_tombstones(self):
		base = self.base()
		TSVZ.append_records(TSVZ.part_path(base, 1), [['a', '1']], create=True)
		db = TSVZ.WalStore(base, multipart=True, create=True, flush_interval=600)
		try:
			db.clear()
		finally:
			db.close()
		self.assertEqual(read_text(TSVZ.part_path(base, 1)), 'a\t1\n')
		self.assertEqual(dict(TSVZ.read_multipart(base)), {})


class TestSnapshotStore(unittest.TestCase):
	"""§19.2 — the race-free multi-part snapshot procedure."""

	def setUp(self):
		self.dir = tempfile.mkdtemp()
		self.base = os.path.join(self.dir, 'ev.tsvz')
		self.ords = sorted(TSVZ.new_ordinal() for _ in range(4))
		for i, o in enumerate(self.ords[:3]):
			TSVZ.append_records(TSVZ.part_path(self.base, o),
								[[f'k{i}', f'v{i}'], ['shared', f'from{i}']], create=True)
		TSVZ.append_records(TSVZ.part_path(self.base, self.ords[3]),
							[['live', 'active']], create=True)

	def test_reads_are_unchanged(self):
		before = dict(TSVZ.read_multipart(self.base))
		TSVZ.snapshot_store(self.base)
		self.assertEqual(dict(TSVZ.read_multipart(self.base)), before)

	def test_ordinal_slots_between_prefix_and_active(self):
		res = TSVZ.snapshot_store(self.base)
		self.assertLess(int(self.ords[2], 16), int(res.ordinal, 16))
		self.assertLess(int(res.ordinal, 16), int(self.ords[3], 16))

	def test_snapshot_contents(self):
		res = TSVZ.snapshot_store(self.base)
		body = read_text(res.path).splitlines()
		rows = [ln for ln in body if not ln.startswith('#')]
		markers = [ln for ln in body if ln.startswith('#')]
		self.assertEqual(body[:len(markers)], markers)        # §19.2.3a top
		self.assertTrue(body[0].startswith('#_version_#'))
		self.assertEqual([ln for ln in markers if not ln.startswith('#_')], [])  # §19.2.3c
		self.assertEqual([ln for ln in rows if '\t' not in ln], [])              # no tombstones
		self.assertEqual([ln.split('\t')[0] for ln in rows],
						 ['k0', 'shared', 'k1', 'k2'])                           # §19.2.3b

	def test_active_part_untouched(self):
		TSVZ.snapshot_store(self.base)
		self.assertEqual(read_text(TSVZ.part_path(self.base, self.ords[3])),
						 'live\tactive\n')

	def test_concurrent_writer_loses_nothing(self):
		stop = threading.Event()
		written = []

		def writer():
			i = 0
			while not stop.is_set():
				TSVZ.append_records(TSVZ.part_path(self.base, self.ords[3]),
									[[f'w{i}', str(i)]])
				written.append(f'w{i}')
				i += 1
				time.sleep(0.002)
		th = threading.Thread(target=writer, daemon=True)
		th.start()
		try:
			time.sleep(0.05)
			TSVZ.snapshot_store(self.base)
			time.sleep(0.05)
		finally:
			stop.set()
			th.join(timeout=5)
		final = TSVZ.read_multipart(self.base)
		self.assertTrue(all(k in final for k in written))
		self.assertTrue(all(f'k{i}' in final for i in range(3)))

	def test_rotate_rename(self):
		res = TSVZ.snapshot_store(self.base, rotate='rename')
		self.assertTrue(all(os.path.exists(p + '.rotated') for p in res.subsumed))
		self.assertFalse(any(os.path.exists(p) for p in res.subsumed))
		self.assertEqual(sorted(TSVZ.read_multipart(self.base)),
						 ['k0', 'k1', 'k2', 'live', 'shared'])

	def test_delete_is_downgraded_unless_allowed(self):
		res = TSVZ.snapshot_store(self.base, rotate='delete')
		self.assertEqual(res.rotate_action, 'rename')

	def test_delete_honoured_on_request(self):
		res = TSVZ.snapshot_store(self.base, rotate='delete', allow_delete=True)
		self.assertEqual(res.rotate_action, 'delete')
		self.assertFalse(any(os.path.exists(p) for p in res.subsumed))
		self.assertEqual(sorted(TSVZ.read_multipart(self.base)),
						 ['k0', 'k1', 'k2', 'live', 'shared'])

	def test_rotate_marker_selects_the_action(self):
		with open(TSVZ.part_path(self.base, self.ords[0]), 'a') as f:
			f.write('#_rotate_#\trename\n')
		self.assertEqual(TSVZ.snapshot_store(self.base).rotate_action, 'rename')

	def test_part_path_argument_writes_into_the_store(self):
		part = TSVZ.part_path(self.base, self.ords[0])
		res = TSVZ.snapshot_store(part)
		parsed = TSVZ.parse_part_name(res.path)
		self.assertIsNotNone(parsed, res.path)
		self.assertEqual(f'{parsed.store_path}.{parsed.format_ext}', self.base)
		self.assertIn(res.path, TSVZ.store_part_paths(self.base))

	def test_refuses_when_no_ordinal_fits(self):
		tight = os.path.join(self.dir, 'tight.tsvz')
		TSVZ.append_records(TSVZ.part_path(tight, 1), [['a', '1']], create=True)
		TSVZ.append_records(TSVZ.part_path(tight, 2), [['b', '2']], create=True)
		with self.assertRaises(ValueError):
			TSVZ.snapshot_store(tight)
		res = TSVZ.snapshot_store(tight, quiesce=True)
		self.assertEqual(len(res.subsumed), 2)
		self.assertEqual(dict(TSVZ.read_multipart(tight)),
						 {'a': ['a', '1'], 'b': ['b', '2']})

	def test_nothing_to_compact(self):
		one = os.path.join(self.dir, 'one.tsvz')
		TSVZ.append_records(TSVZ.part_path(one, 1), [['a', '1']], create=True)
		self.assertIsNone(TSVZ.snapshot_store(one))

	def test_unnumbered_file_points_at_snapshot_part(self):
		plain = os.path.join(self.dir, 'plain.tsvz')
		TSVZ.append_records(plain, [['a', '1']], create=True)
		with self.assertRaises(ValueError):
			TSVZ.snapshot_store(plain)

	def test_compensates_for_varying_defaults(self):
		# §19.4: hoisting #_defaults_# must not change how earlier rows resolve.
		base = os.path.join(self.dir, 'vary.csvz')
		o = sorted(TSVZ.new_ordinal() for _ in range(3))
		with open(TSVZ.part_path(base, o[0]), 'w') as f:
			f.write('#_strip_trailing_whites_#,false\nk1,keep  \n')
		with open(TSVZ.part_path(base, o[1]), 'w') as f:
			f.write('#_defaults_#,D1,D2\nk2,v\n')
		TSVZ.append_records(TSVZ.part_path(base, o[2]), [['live', 'x']], create=True)
		before = dict(TSVZ.read_multipart(base))
		self.assertEqual(before['k1'], ['k1', 'keep  '])
		self.assertEqual(before['k2'], ['k2', 'v', 'D2'])
		TSVZ.snapshot_store(base)
		after = dict(TSVZ.read_multipart(base))
		self.assertEqual(set(after), set(before))
		for key, was in before.items():
			now = after[key]
			width = max(len(was), len(now))
			# §3.6 lets width vary; every column *value* must be identical.
			self.assertEqual(now + [''] * (width - len(now)),
							 was + [''] * (width - len(was)), key)

	def test_snapshot_is_a_fixed_point(self):
		TSVZ.snapshot_store(self.base)
		once = dict(TSVZ.read_multipart(self.base))
		TSVZ.snapshot_store(self.base, quiesce=True)
		self.assertEqual(dict(TSVZ.read_multipart(self.base)), once)


class TestUtf8Bom(unittest.TestCase):
	"""§4.1 — UTF-8 with no byte-order mark."""

	def test_writer_does_not_emit_bom(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['k', 'v']], create=True)
			with open(path, 'rb') as f:
				self.assertFalse(f.read().startswith(b'\xef\xbb\xbf'))

	def test_leading_bom_is_field_data_not_stripped(self):
		# Files must not have a BOM; a leading U+FEFF is therefore a key char.
		rows, _ = replay('\ufeffk\tv\n')
		self.assertEqual(list(rows), ['\ufeffk'])
		self.assertNotIn('k', rows)


class TestVersionSwitching(unittest.TestCase):
	"""§6.3–§6.4 — effective version is per-line and degrades gracefully."""

	def test_empty_version_resets_to_one(self):
		_, state = replay('#_version_#\t9\n#_version_#\n')
		self.assertEqual(state.version, 1)

	def test_blank_version_value_resets_to_one(self):
		_, state = replay('#_version_#\t9\n#_version_#\t\n')
		self.assertEqual(state.version, 1)

	def test_invalid_version_resets_to_one(self):
		_, state = replay('#_version_#\tnot-a-number\n')
		self.assertEqual(state.version, 1)

	def test_mid_file_switch_keeps_every_row(self):
		# §6.4: 1 → 99 → 1; each row is read under the version in force there.
		# This reader implements only v1, so 99 clamps (§6.3) and still yields data.
		rows, state = replay(
			'#_version_#\t1\na\t1\n#_version_#\t99\nb\t2\n#_version_#\t1\nc\t3\n')
		self.assertEqual(dict(rows), {'a': ['a', '1'], 'b': ['b', '2'], 'c': ['c', '3']})
		self.assertEqual(state.version, 1)

	def test_unknown_token_under_clamped_future_version_passes_through(self):
		# §6.4 + §13.4: a construct newer than the effective version is literal.
		rows, _ = replay('#_version_#\t99\nk\t<future-token>\n')
		self.assertEqual(rows['k'], ['k', '<future-token>'])


class TestBoolSynonyms(unittest.TestCase):
	"""Marker booleans accept the documented synonyms, not only true/false."""

	def test_truthy_synonyms_enable_fill_empty(self):
		for word in ('true', 'YES', 'on', '1'):
			with self.subTest(word=word):
				_, state = replay(f'#_fill_empty_with_default_#\t{word}\n')
				self.assertTrue(state.fill_empty)

	def test_falsy_synonyms_disable_strip(self):
		for word in ('false', 'NO', 'off', '0'):
			with self.subTest(word=word):
				_, state = replay(f'#_strip_trailing_whites_#\t{word}\n')
				self.assertFalse(state.strip_trailing)

	def test_unrecognized_value_keeps_the_builtin_default(self):
		_, state = replay('#_fill_empty_with_default_#\tmaybe\n')
		self.assertFalse(state.fill_empty)


class TestBz2AndZstd(unittest.TestCase):
	"""§16 — bz2 round-trip; zstd refuses a plaintext fallback."""

	def test_bz2_roundtrip(self):
		with TempFile(suffix='.tsvz.bz2') as path:
			os.unlink(path)
			TSVZ.append_records(path, [['k', 'v']], create=True)
			self.assertEqual(dict(TSVZ.read_store(path)), {'k': ['k', 'v']})

	def test_bzip2_suffix_roundtrip(self):
		with TempFile(suffix='.tsvz.bzip2') as path:
			os.unlink(path)
			TSVZ.append_records(path, [['k', 'v']], create=True)
			self.assertEqual(dict(TSVZ.read_store(path)), {'k': ['k', 'v']})

	def test_gzip_alias_suffix_roundtrip(self):
		with TempFile(suffix='.tsvz.gzip') as path:
			os.unlink(path)
			TSVZ.append_records(path, [['k', 'v']], create=True)
			self.assertEqual(dict(TSVZ.read_store(path)), {'k': ['k', 'v']})

	def test_zst_refuses_plaintext_when_stdlib_is_missing(self):
		try:
			from compression import zstd  # noqa: F401
		except ImportError:
			pass
		else:
			self.skipTest('compression.zstd is available; cannot probe the refusal')
		with TempFile(suffix='.tsvz.zst') as path:
			with self.assertRaises(ImportError) as cm:
				TSVZ.open_part(path, 'wb')
			self.assertIn('plaintext', str(cm.exception).lower())

	def test_zst_does_not_use_third_party_zstandard(self):
		try:
			from compression import zstd  # noqa: F401
		except ImportError:
			pass
		else:
			self.skipTest('compression.zstd is available; third-party probe is dead code')
		sys.modules['zstandard'] = types.ModuleType('zstandard')
		try:
			with TempFile(suffix='.tsvz.zst') as path:
				with self.assertRaises(ImportError) as cm:
					TSVZ.open_part(path, 'wb')
				self.assertIn('not used', str(cm.exception).lower())
		finally:
			sys.modules.pop('zstandard', None)


class TestStreamAppendCompressed(unittest.TestCase):
	"""§16.4 — repeated appends to a compressed active part stay readable."""

	def test_walstore_gz_flushes_remain_one_readable_store(self):
		with TempFile(suffix='.tsvz.gz') as path:
			os.unlink(path)
			db = TSVZ.WalStore(path, create=True, flush_interval=600)
			try:
				db['a'] = ['a', '1']
				db.flush()
				db['b'] = ['b', '2']
				db.flush()
			finally:
				db.close()
			self.assertEqual(dict(TSVZ.read_store(path)),
							 {'a': ['a', '1'], 'b': ['b', '2']})
			with gzip.open(path, 'rt') as f:
				text = f.read()
			self.assertIn('a\t1', text)
			self.assertIn('b\t2', text)

	def test_open_close_per_write_gz_still_roundtrips(self):
		# The spec prefers a held stream; open/close per write must not lose rows.
		with TempFile(suffix='.tsvz.gz') as path:
			os.unlink(path)
			TSVZ.append_record(path, ['a', '1'], create=True)
			TSVZ.append_record(path, ['b', '2'])
			self.assertEqual(dict(TSVZ.read_store(path)),
							 {'a': ['a', '1'], 'b': ['b', '2']})

	def test_walstore_bz2_stream_append(self):
		with TempFile(suffix='.tsvz.bz2') as path:
			os.unlink(path)
			db = TSVZ.WalStore(path, create=True, flush_interval=600)
			try:
				db['a'] = ['a', '1']
				db.flush()
				db['b'] = ['b', '2']
				db.flush()
			finally:
				db.close()
			self.assertEqual(dict(TSVZ.read_store(path)),
							 {'a': ['a', '1'], 'b': ['b', '2']})


class TestBlake3(unittest.TestCase):
	"""§15 — blake3 is optional; missing it must not affect reconstructed data."""

	def test_missing_module_makes_the_marker_inert(self):
		injected = 'blake3' not in sys.modules
		if TSVZ.new_digest('blake3') is not None:
			self.skipTest('blake3 is installed; inert path is not taken')
		self.assertIsNone(TSVZ.new_digest('blake3'))
		body = b'#_checksum_blake3_#\na\t1\n#_checksum_blake3_#\tdeadbeef\n'
		with TempFile(suffix='.tsvz', content=body) as path:
			d = TSVZ.verify_part(path)
			self.assertEqual((d.armed, d.mismatches), (frozenset(), []))
			self.assertEqual(dict(TSVZ.read_store(path)), {'a': ['a', '1']})
		_ = injected

	def test_round_trip_when_an_accumulator_exists(self):
		injected = False
		if TSVZ.new_digest('blake3') is None:
			class Acc:
				def __init__(self):
					self._b = b''

				def update(self, data):
					self._b += bytes(data)

				def hexdigest(self):
					return hashlib.sha256(self._b).hexdigest()

			fake = types.ModuleType('blake3')
			fake.blake3 = Acc
			sys.modules['blake3'] = fake
			injected = True
		try:
			self.assertIsNotNone(TSVZ.new_digest('blake3'))
			with TempFile(suffix='.tsvz') as path:
				TSVZ.append_records(path, [['a', '1']], create=True)
				TSVZ.append_checksum(path, 'blake3')
				TSVZ.append_records(path, [['b', '2']])
				self.assertNotEqual(TSVZ.append_checksum(path, 'blake3'), '')
				self.assertEqual(TSVZ.verify_part(path).verified, 1)
				self.assertEqual(dict(TSVZ.read_store(path)),
								 {'a': ['a', '1'], 'b': ['b', '2']})
		finally:
			if injected:
				sys.modules.pop('blake3', None)


class TestSnapshotHeaderComment(unittest.TestCase):
	"""snapshot_part(header=) writes a # comment; live data must not change."""

	def test_optional_header_is_a_comment_not_a_key(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['k', 'v']], create=True)
			TSVZ.snapshot_part(path, header=['id', 'val'])
			text = read_text(path)
			self.assertIn('#id\tval', text.splitlines())
			self.assertEqual(dict(TSVZ.read_store(path)), {'k': ['k', 'v']})
			self.assertNotIn('id', TSVZ.read_store(path))


class TestIntegrityPolicyAndRotate(unittest.TestCase):
	def test_report_corruption_rejects_unknown_policy(self):
		with self.assertRaises(ValueError):
			TSVZ.report_corruption(TSVZ.DigestSet(), 'x', policy='nope')

	def test_report_corruption_noop_when_clean(self):
		self.assertEqual(TSVZ.report_corruption(None, 'x'), [])
		self.assertEqual(TSVZ.report_corruption(TSVZ.DigestSet(), 'x'), [])

	def test_rotate_parts_rejects_unknown_action(self):
		with self.assertRaises(ValueError):
			TSVZ.rotate_parts([], 'explode')

	def test_rotate_rename_skips_already_rotated_parts(self):
		d = tempfile.mkdtemp()
		self.addCleanup(shutil.rmtree, d, ignore_errors=True)
		base = os.path.join(d, 'ev.tsvz')
		live = TSVZ.part_path(base, 1)
		rotated = TSVZ.part_path(base, 2, rotated=True)
		TSVZ.append_records(live, [['a', '1']], create=True)
		TSVZ.append_records(rotated, [['z', 'ghost']], create=True)
		parts = TSVZ.store_parts(base, include_rotated=True)
		action, changes = TSVZ.rotate_parts(parts, 'rename')
		self.assertEqual(action, 'rename')
		self.assertTrue(os.path.isfile(rotated), 'already-rotated part must stay put')
		self.assertFalse(os.path.isfile(live))
		self.assertTrue(any(new and new.endswith('.rotated') for _, new in changes))

	def test_failed_atomic_rewrite_does_not_leave_a_temp(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			directory = os.path.dirname(os.path.abspath(path))
			before = set(os.listdir(directory))
			real = os.replace

			def boom(*_a, **_k):
				raise OSError('injected replace failure')

			os.replace = boom
			try:
				with self.assertRaises(OSError):
					TSVZ.snapshot_part(path)
			finally:
				os.replace = real
			leftovers = [n for n in set(os.listdir(directory)) - before if '.tmp' in n]
			self.assertEqual(leftovers, [])
			self.assertEqual(dict(TSVZ.read_store(path)), {'a': ['a', '1']})

	def test_snapshot_store_missing_raises(self):
		with self.assertRaises(FileNotFoundError):
			TSVZ.snapshot_store(os.path.join(tempfile.gettempdir(),
											 'no-such-tsvz-store-xyz.tsvz'))

	def test_store_parts_missing_directory_is_empty(self):
		self.assertEqual(TSVZ.store_parts('/no/such/tsvz-dir/x.tsvz'), [])

	def test_unnumbered_file_beside_parts_is_ignored_with_a_warning(self):
		d = tempfile.mkdtemp()
		self.addCleanup(shutil.rmtree, d, ignore_errors=True)
		base = os.path.join(d, 'st.tsvz')
		TSVZ.append_records(TSVZ.part_path(base, 1), [['a', '1']], create=True)
		with open(base, 'w') as f:
			f.write('ghost\t9\n')
		with warnings.catch_warnings(record=True) as caught:
			warnings.simplefilter('always', UserWarning)
			paths = TSVZ.store_part_paths(base)
			data = dict(TSVZ.read_multipart(base))
		self.assertTrue(any('numbered parts' in str(w.message) for w in caught))
		self.assertNotIn(base, paths)
		self.assertEqual(data, {'a': ['a', '1']})

	def test_replay_of_a_missing_part_is_empty(self):
		store, state = TSVZ.replay_part(
			os.path.join(tempfile.gettempdir(), 'no-such-tsvz-part.tsvz'), '\t')
		self.assertEqual(len(store), 0)
		self.assertEqual(state.version, 1)

	def test_read_last_record_missing_file(self):
		missing = os.path.join(tempfile.gettempdir(), 'no-such-tsvz-part.tsvz')
		self.assertEqual(TSVZ.read_last_record(missing), [])
		self.assertEqual(TSVZ.read_last_record(missing, store_offset=True), -1)


class TestPopAndContext(unittest.TestCase):
	def _open(self, path, cls):
		if cls is TSVZ.WalStore:
			return cls(path, create=True, flush_interval=600)
		return cls(path, create=True)

	def test_pop_persists_a_tombstone(self):
		for cls in (TSVZ.WalStore, TSVZ.OffsetStore):
			with self.subTest(cls=cls.__name__), TempFile(suffix='.tsvz') as path:
				db = self._open(path, cls)
				try:
					db['a'] = ['a', '1']
					db['b'] = ['b', '2']
					self.assertEqual(db.pop('a'), ['a', '1'])
					self.assertNotIn('a', db)
					self.assertEqual(db.pop('missing', 'd'), 'd')
					with self.assertRaises(KeyError):
						db.pop('missing')
				finally:
					db.close()
				self.assertEqual(dict(TSVZ.read_store(path)), {'b': ['b', '2']})

	def test_popitem_last_and_first(self):
		for cls in (TSVZ.WalStore, TSVZ.OffsetStore):
			with self.subTest(cls=cls.__name__), TempFile(suffix='.tsvz') as path:
				db = self._open(path, cls)
				try:
					db['a'] = ['a', '1']
					db['b'] = ['b', '2']
					key, row = db.popitem()
					self.assertEqual((key, row), ('b', ['b', '2']))
					key, row = db.popitem(last=False)
					self.assertEqual((key, row), ('a', ['a', '1']))
					with self.assertRaises(KeyError):
						db.popitem()
				finally:
					db.close()
				self.assertEqual(dict(TSVZ.read_store(path)), {})

	def test_walstore_context_manager_flushes(self):
		with TempFile(suffix='.tsvz') as path:
			with TSVZ.WalStore(path, create=True, flush_interval=600) as db:
				db['k'] = ['k', 'v']
			self.assertEqual(dict(TSVZ.read_store(path)), {'k': ['k', 'v']})

	def test_empty_key_cannot_be_stored(self):
		for cls in (TSVZ.WalStore, TSVZ.OffsetStore):
			with self.subTest(cls=cls.__name__), TempFile(suffix='.tsvz') as path:
				db = self._open(path, cls)
				try:
					with self.assertRaises(KeyError):
						db[''] = ['', 'x']
				finally:
					db.close()

	def test_lone_key_assignment_is_a_delete(self):
		for cls in (TSVZ.WalStore, TSVZ.OffsetStore):
			with self.subTest(cls=cls.__name__), TempFile(suffix='.tsvz') as path:
				db = self._open(path, cls)
				try:
					db['k'] = ['k', 'v']
					db['k'] = ['k']
					self.assertNotIn('k', db)
				finally:
					db.close()
				self.assertEqual(dict(TSVZ.read_store(path)), {})

	def test_defaults_mapping_key_updates_marker_state(self):
		for cls in (TSVZ.WalStore, TSVZ.OffsetStore):
			with self.subTest(cls=cls.__name__), TempFile(suffix='.tsvz') as path:
				db = self._open(path, cls)
				try:
					db[TSVZ.MARKER_DEFAULTS] = [TSVZ.MARKER_DEFAULTS, 'NA']
					self.assertEqual(db['missing'], ['missing', 'NA'])
					del db[TSVZ.MARKER_DEFAULTS]
					self.assertEqual(db.get('missing'), ['missing'])
				finally:
					db.close()


class TestOffsetStoreExtras(unittest.TestCase):
	def test_clear_keeps_header_and_defaults(self):
		with TempFile(suffix='.tsvz') as path:
			s = TSVZ.OffsetStore(path, header=['id', 'v'], defaults=['NA'], create=True)
			try:
				s['k'] = ['k', '1']
				s.clear()
				self.assertEqual(len(s), 0)
			finally:
				s.close()
			text = read_text(path)
			self.assertIn('#id\tv', text)
			self.assertIn('#_defaults_#\tNA', text)
			self.assertNotIn('k\t1', text)

	def test_create_false_on_missing_file_is_empty(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			s = TSVZ.OffsetStore(path, create=False)
			try:
				self.assertEqual(len(s), 0)
			finally:
				s.close()

	def test_crlf_row_is_readable_by_offset(self):
		with TempFile(suffix='.tsvz', content=b'k\tv\r\n') as path:
			s = TSVZ.OffsetStore(path, create=False)
			try:
				self.assertEqual(s['k'], ['k', 'v'])
			finally:
				s.close()

	def test_flush_on_a_closed_store_is_a_noop(self):
		with TempFile(suffix='.tsvz') as path:
			s = TSVZ.OffsetStore(path, create=True)
			s.close()
			self.assertIs(s.flush(), s)

	def test_walstore_create_false_missing_file_is_empty(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			db = TSVZ.WalStore(path, create=False, flush_interval=600)
			try:
				self.assertEqual(len(db), 0)
			finally:
				db.close()

	def test_open_part_text_mode_roundtrips(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['k', 'v']], create=True)
			with TSVZ.open_part(path, 'r') as f:
				self.assertIn('k\tv', f.read())

	def test_ensure_part_exists_writes_defaults_marker(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			self.assertTrue(TSVZ.ensure_part_exists(path, create=True, defaults=['NA']))
			self.assertIn('#_defaults_#\tNA', read_text(path))


class TestTsvzedHardMapAndSwitch(unittest.TestCase):
	def test_hardmap_collapses_superseded_rows(self):
		with TempFile(suffix='.tsvz') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				db = TSVZ.TSVZed(path, createIfNotExist=True, append_check_delay=600)
			try:
				db['a'] = ['a', '1']
				db['a'] = ['a', '2']
				db.flush()
				self.assertTrue(db.hardMapToFile())
				self.assertEqual(read_text(path).count('a\t'), 1)
				self.assertIn('a\t2', read_text(path))
				self.assertTrue(db.rewrite())
			finally:
				db.close()

	def test_hardmap_returns_false_when_already_rewriting(self):
		with TempFile(suffix='.tsvz') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				db = TSVZ.TSVZed(path, createIfNotExist=True, append_check_delay=600)
			try:
				db._rewriting = True
				self.assertFalse(db.hardMapToFile())
			finally:
				db._rewriting = False
				db.close()

	def test_rewrite_on_load_compacts(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1'], ['a', '2']], create=True)
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				db = TSVZ.TSVZed(path, rewrite_on_load=True, append_check_delay=600)
			try:
				self.assertEqual(db['a'], ['a', '2'])
				self.assertEqual(read_text(path).count('a\t'), 1)
			finally:
				db.close()

	def test_rewrite_on_exit_compacts(self):
		with TempFile(suffix='.tsvz') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				db = TSVZ.TSVZed(path, createIfNotExist=True, rewrite_on_exit=True,
								 append_check_delay=600)
				db['a'] = ['a', '1']
				db['a'] = ['a', '2']
				db.flush()
				db.close()
			self.assertEqual(read_text(path).count('a\t'), 1)

	def test_flush_honours_rewrite_interval(self):
		with TempFile(suffix='.tsvz') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				db = TSVZ.TSVZed(path, createIfNotExist=True, rewrite_interval=0.001,
								 append_check_delay=600)
			try:
				db['a'] = ['a', '1']
				db['a'] = ['a', '2']
				db._last_rewrite = 0
				db.flush()
				self.assertEqual(read_text(path).count('a\t'), 1)
			finally:
				db.close()

	def test_legacy_aliases_still_work(self):
		with TempFile(suffix='.tsvz') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				db = TSVZ.TSVZed(path, createIfNotExist=True, append_check_delay=600)
			try:
				db['k'] = ['k', 'v']
				self.assertIs(db.commitAppendToFile(), db)
				self.assertIs(db.load(), db)
				self.assertIs(db.checkExternalChanges(), db)
				self.assertEqual(db.getListView(), [['k', 'v']])
				self.assertIs(db.clear_file(), db)
			finally:
				db.stopAppendThread()
			self.assertEqual(dict(TSVZ.read_store(path)), {})

	def test_lite_switch_file(self):
		with TempFile(suffix='.tsvz') as p1, TempFile(suffix='.tsvz') as p2:
			TSVZ.append_records(p2, [['x', '9']], create=True)
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				lite = TSVZ.TSVZedLite(p1, createIfNotExist=True)
			try:
				lite['a'] = ['a', '1']
				lite.switchFile(p2)
				self.assertEqual(lite['x'], ['x', '9'])
				self.assertNotIn('a', lite)
				with self.assertRaises(ValueError):
					lite.switchFile(p2 + '.gz')
			finally:
				lite.close()

	def test_lite_adopts_an_open_handle(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['k', 'v']], create=True)
			fh = open(path, 'r+b')  # noqa: SIM115
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				lite = TSVZ.TSVZedLite(path, fileObj=fh)
			try:
				self.assertEqual(lite['k'], ['k', 'v'])
			finally:
				lite.close()


class TestLegacyMore(unittest.TestCase):
	def test_symbolic_delimiter_names(self):
		self.assertEqual(TSVZ._legacy_delimiter(delimiter='comma'), ',')
		self.assertEqual(TSVZ._legacy_delimiter(delimiter='tab'), '\t')
		self.assertEqual(TSVZ._legacy_delimiter(delimiter='pipe'), '|')
		self.assertEqual(TSVZ._legacy_delimiter(delimiter='null'), '\0')
		self.assertEqual(TSVZ._legacy_delimiter(delimiter=''), '\t')
		self.assertEqual(TSVZ._legacy_delimiter(delimiter=...), '\t')

	def test_read_last_valid_line(self):
		with TempFile(suffix='.tsvz', content='a\t1\nb\t2\n') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				self.assertEqual(
					TSVZ.read_last_valid_line(path, {}, -1), ['b', '2'])

	def test_read_tabular_missing_is_lenient_when_not_strict(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				self.assertEqual(dict(TSVZ.readTabularFile(path, strict=False)), {})
				self.assertEqual(
					TSVZ.readTabularFile(path, lastLineOnly=True, strict=False), [])

	def test_read_tabular_last_line_pads_to_header_width(self):
		with TempFile(suffix='.tsvz', content='k\tv\n') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				row = TSVZ.readTabularFile(
					path, lastLineOnly=True, header=['id', 'v', 'extra'])
			self.assertEqual(row, ['k', 'v', ''])

	def test_read_tabular_store_offset(self):
		with TempFile(suffix='.tsvz', content='k\tv\n') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				offsets = TSVZ.readTabularFile(path, storeOffset=True)
			self.assertEqual(offsets['k'], 0)

	def test_append_lines_tabular_file(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				TSVZ.appendLinesTabularFile(
					path, [['a', '1'], ['b', '2']], createIfNotExist=True)
			self.assertEqual(dict(TSVZ.read_store(path)),
							 {'a': ['a', '1'], 'b': ['b', '2']})

	def test_list_view_with_header_and_empty_store(self):
		self.assertEqual(TSVZ._list_view({}, header=['id', 'v']), [['id', 'v']])
		self.assertEqual(TSVZ._list_view({}), [])
		self.assertEqual(
			TSVZ._list_view({'a': ['a', '1']}, header=['id', 'v']),
			[['id', 'v'], ['a', '1']])


class TestCliInProcess(unittest.TestCase):
	"""Drive __main__ in-process so coverage.py sees the CLI."""

	def run_main(self, *argv):
		buf_out, buf_err = io.StringIO(), io.StringIO()
		old = sys.argv, sys.stdout, sys.stderr
		sys.argv = ['TSVZ.py', *argv]
		sys.stdout, sys.stderr = buf_out, buf_err
		try:
			code = TSVZ.__main__()
		except SystemExit as exc:
			code = exc.code if isinstance(exc.code, int) else 1
		finally:
			sys.argv, sys.stdout, sys.stderr = old
		return code, buf_out.getvalue(), buf_err.getvalue()

	def _numbered(self):
		d = tempfile.mkdtemp()
		self.addCleanup(shutil.rmtree, d, ignore_errors=True)
		base = os.path.join(d, 'st.tsvz')
		ords = sorted(TSVZ.new_ordinal() for _ in range(2))
		TSVZ.append_records(TSVZ.part_path(base, ords[0]), [['a', '1']], create=True)
		TSVZ.append_records(TSVZ.part_path(base, ords[1]), [['b', '2']], create=True)
		return base, ords

	def test_version_flag(self):
		code, out, _err = self.run_main('--version')
		self.assertEqual(code, 0)
		self.assertIn(TSVZ.version, out)
		self.assertIn('TSVZ_old', out)

	def test_round_trip_and_default_read(self):
		with TempFile(suffix='.tsvz') as path:
			self.assertEqual(self.run_main(path, 'append', 'k', 'v1', 'v2')[0], 0)
			code, out, _err = self.run_main(path)
			self.assertEqual(code, 0)
			self.assertIn('v1', out)
			self.assertEqual(self.run_main(path, 'delete', 'k')[0], 0)
			self.assertEqual(dict(TSVZ.read_store(path)), {})

	def test_header_on_append_and_defaults_on_clear(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			code, _out, err = self.run_main(
				path, 'append', '-c', 'id\\tval', 'k', 'v')
			self.assertEqual(code, 0, err)
			self.assertIn('#id\tval', read_text(path))
			code, _out, err = self.run_main(path, 'clear', '--defaults', 'NA')
			self.assertEqual(code, 0, err)
			self.assertIn('#_defaults_#\tNA', read_text(path))

	def test_checksum_append_and_verify(self):
		with TempFile(suffix='.tsvz') as path:
			self.assertEqual(self.run_main(path, 'append', 'a', '1')[0], 0)
			self.assertEqual(self.run_main(path, 'append', '--checksum', 'crc32', 'b', '2')[0], 0)
			code, out, err = self.run_main(path, 'verify')
			self.assertEqual(code, 0, err)
			self.assertIn('verified', out)

	def test_verify_mismatch_exits_nonzero(self):
		body = b'#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\t00000000\n'
		with TempFile(suffix='.tsvz', content=body) as path:
			code, _out, err = self.run_main(path, 'verify')
			self.assertEqual(code, 1)
			self.assertIn('mismatch', err)

	def test_verify_missing_part_reports_zero_segments(self):
		# Unnumbered missing path: replay is empty, so verify is a no-op success.
		code, out, _err = self.run_main(
			os.path.join(tempfile.gettempdir(), 'no-such-tsvz-cli.tsvz'), 'verify')
		self.assertEqual(code, 0)
		self.assertIn('0 segment', out)

	def test_verify_missing_multipart_store_exits_nonzero(self):
		code, _out, err = self.run_main(
			os.path.join(tempfile.gettempdir(), 'no-such-tsvz-cli.tsvz'),
			'verify', '-m')
		self.assertEqual(code, 1)
		self.assertIn('no such store', err)

	def test_clear_and_scrub(self):
		with TempFile(suffix='.tsvz') as path:
			self.run_main(path, 'append', 'a', '1')
			self.run_main(path, 'append', 'a', '2')
			self.assertEqual(self.run_main(path, 'scrub')[0], 0)
			self.assertEqual(read_text(path).count('a\t'), 1)
			self.assertEqual(self.run_main(path, 'clear')[0], 0)
			self.assertEqual(dict(TSVZ.read_store(path)), {})

	def test_force_missing_read(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			code, _out, _err = self.run_main(path, 'read', '--force')
			self.assertEqual(code, 0)

	def test_non_ascii_field_is_not_unescaped(self):
		with TempFile(suffix='.tsvz') as path:
			self.assertEqual(self.run_main(path, 'append', 'k', 'café')[0], 0)
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'café'])

	def test_parts_lists_rotated(self):
		base, ords = self._numbered()
		TSVZ.append_records(TSVZ.part_path(base, ords[0], rotated=True),
							[['z', 'ghost']], create=True)
		code, out, err = self.run_main(base, 'parts')
		self.assertEqual(code, 0, err)
		self.assertIn(ords[0], out)
		self.assertIn('rotated', out)

	def test_multipart_verify_and_explicit_m(self):
		base, _ords = self._numbered()
		code, out, err = self.run_main(base, 'verify', '-m')
		self.assertEqual(code, 0, err)
		self.assertIn('store', out)

	def test_multipart_scrub_and_delete(self):
		base, ords = self._numbered()
		code, out, err = self.run_main(base, 'scrub')
		self.assertEqual(code, 0, err)
		self.assertIn('subsumed', out)
		code, _out, err = self.run_main(base, 'delete', 'a')
		self.assertEqual(code, 0, err)
		self.assertNotIn('a', TSVZ.read_multipart(base))
		self.assertTrue(os.path.isfile(TSVZ.part_path(base, ords[-1])))

	def test_multipart_scrub_with_no_prefix_is_quiet(self):
		d = tempfile.mkdtemp()
		self.addCleanup(shutil.rmtree, d, ignore_errors=True)
		base = os.path.join(d, 'one.tsvz')
		TSVZ.append_records(TSVZ.part_path(base, 1), [['a', '1']], create=True)
		code, _out, err = self.run_main(base, 'scrub')
		self.assertEqual(code, 0)
		self.assertIn('nothing to compact', err)

	def test_csv_delimiter_flag(self):
		with TempFile(suffix='.csvz') as path:
			code, _out, err = self.run_main(path, 'append', '-d', 'comma', 'k', 'a,b')
			self.assertEqual(code, 0, err)
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'a,b'])

	def test_strict_missing_read_in_process(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			code, _out, err = self.run_main(path, 'read', '--strict')
			self.assertEqual(code, 1)
			self.assertIn('no such part', err)

	def test_pretty_table_empty_and_scalars(self):
		self.assertEqual(TSVZ._cli_pretty_format_table([]), '')
		self.assertEqual(TSVZ._cli_pretty_format_table(['solo']), 'solo\n')


class TestMorePublicEdges(unittest.TestCase):
	def test_store_parts_normalizes_a_part_path(self):
		d = tempfile.mkdtemp()
		self.addCleanup(shutil.rmtree, d, ignore_errors=True)
		base = os.path.join(d, 'ev.tsvz')
		p0 = TSVZ.part_path(base, 1)
		p1 = TSVZ.part_path(base, 2)
		TSVZ.append_records(p0, [['a', '1']], create=True)
		TSVZ.append_records(p1, [['b', '2']], create=True)
		self.assertEqual([pn.path for pn in TSVZ.store_parts(p0)], [p0, p1])

	def test_snapshot_part_fills_a_caller_store(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1'], ['a', '2']], create=True)
			plain = {}
			out = TSVZ.snapshot_part(path, store=plain)
			self.assertIs(out, plain)
			self.assertEqual(plain, {'a': ['a', '2']})

	def test_append_records_from_a_mapping_and_skips_empty(self):
		with TempFile(suffix='.tsvz') as path:
			os.unlink(path)
			TSVZ.append_records(path, {'k': ['v'], 'u': ['u', '2']}, create=True)
			TSVZ.append_records(path, [[], ['z', '9']])
			self.assertEqual(dict(TSVZ.read_store(path)),
							 {'k': ['k', 'v'], 'u': ['u', '2'], 'z': ['z', '9']})

	def test_read_store_store_offset_builds_an_index(self):
		with TempFile(suffix='.tsvz', content='k\tv\n') as path:
			with self.assertWarns(DeprecationWarning):
				idx = TSVZ.read_store(path, store_offset=True)
			self.assertEqual(idx['k'], 0)

	def test_tombstone_drops_the_offset_cache_entry(self):
		with TempFile(suffix='.tsvz', content='a\t1\na\n') as path:
			offsets, values, _state = TSVZ.read_offsets(path)
			self.assertNotIn('a', offsets)
			self.assertNotIn('a', values)

	def test_last_flush_error_starts_clear(self):
		with TempFile(suffix='.tsvz') as path:
			db = TSVZ.WalStore(path, create=True, flush_interval=600)
			try:
				self.assertIsNone(db.last_flush_error)
			finally:
				db.close()

	def test_fit_row_truncates_and_leaves_non_lists(self):
		with TempFile(suffix='.tsvz', content='k\ta\tb\tc\n') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				row = TSVZ.readTabularFile(path, lastLineOnly=True, correctColumnNum=2)
				wide = TSVZ.readTabularFile(
					path, storeOffset=True, header=['id', 'v', 'extra'])
		self.assertEqual(row, ['k', 'a'])
		self.assertEqual(wide._values_cache['k'], ['k', 'a', 'b'])

	def test_list_view_header_shapes(self):
		self.assertEqual(
			TSVZ._list_view({'a': ['id', 'v']}, header=['id', 'v']),
			[['id', 'v']])
		self.assertEqual(
			TSVZ._list_view({'a': ['a', '1']}, header=('id', 'v')),
			[['id', 'v'], ['a', '1']])
		self.assertEqual(TSVZ._list_view({'a': ['a']}, header=1), [['a']])

	def test_legacy_delimiter_incomplete_escape_stays_literal(self):
		self.assertEqual(TSVZ._legacy_delimiter(delimiter='\\x'), '\\x')

	def test_lite_indexes_override_and_listview(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['k', 'v']], create=True)
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				lite = TSVZ.TSVZedLite(path, indexes={})
			try:
				self.assertEqual(len(lite), 0)
				self.assertEqual(lite.getListView(), [])
				self.assertIs(lite.clear_file(), lite)
			finally:
				lite.close()

	def test_lite_switch_file_overrides_create_flag(self):
		with TempFile(suffix='.tsvz') as p1, TempFile(suffix='.tsvz') as p2:
			os.unlink(p2)
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				lite = TSVZ.TSVZedLite(p1, createIfNotExist=True)
			try:
				lite.switchFile(p2, createIfNotExist=True, verifyHeader=False)
				self.assertFalse(lite.verifyHeader)
				self.assertTrue(os.path.isfile(p2))
			finally:
				lite.close()

	def test_scrub_tabular_last_line_only(self):
		with TempFile(suffix='.tsvz', content='a\t1\nb\t2\n') as path:
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				self.assertEqual(TSVZ.scrubTabularFile(path, lastLineOnly=True),
								 ['b', '2'])


if __name__ == '__main__':
	unittest.main()
