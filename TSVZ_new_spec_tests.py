"""Tests for TSVZ_new.py (tsvz-spec-v1.md core conformance)."""
import gzip
import os
import sys
import tempfile
import threading
import time
import unittest
import warnings
from collections import OrderedDict

import TSVZ_new as TSVZ


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

	def test_checksum_marker_ignored_either_case(self):
		self.assertEqual(TSVZ.classify_record('#_CHECKSUM_SHA256_#'), 'ignore')
		self.assertEqual(TSVZ.classify_record('#_checksum_sha256_#'), 'ignore')

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
				with self.subTest(fn=fn.__name__), self.assertWarns(DeprecationWarning):
					fn(*args, **kw)

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
		exec('from TSVZ_new import *', ns)  # noqa: S102
		leaked = [n for n in ('os', 'sys', 're', 'time', 'threading', 'atexit',
							  'contextlib', 'functools', 'warnings', 'deque',
							  'OrderedDict', 'MutableMapping') if n in ns]
		self.assertEqual(leaked, [])


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


class TestRecordIterator(unittest.TestCase):
	"""§4 framing, implemented once in _iter_records."""

	def _recs(self, data, **kw):
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
	MOD = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'TSVZ_new.py')

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

	def test_field_escapes_are_expanded(self):
		with TempFile(suffix='.tsvz') as path:
			self.run_cli(path, 'append', 'k', 'a\\tb')
			self.assertEqual(TSVZ.read_store(path)['k'], ['k', 'a\tb'])


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


if __name__ == '__main__':
	unittest.main()
