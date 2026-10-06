# Marker watch implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a caller register one function per custom marker key and have that function run with the logical line whenever a full replay reads a match.

**Architecture:** A `MarkerWatch` holds the registrations. `process_record` is the only place that calls them, after the line has been classified and fed to any armed checksum. Public full replays take a keyword-only `watch=None` and pass it through. `WalStore` and `OffsetStore` keep the object and pass it on open and on `reload()`. Snapshot, checksum, `clear()`, `read_last_record`, and the CLI do not.

**Tech Stack:** Python 3.6+, `unittest` via pytest, doctest via `python TSVZ.py --doctest`. Single module `TSVZ.py`.

**Spec:** `docs/superpowers/specs/2026-10-05-marker-watch-design.md`

## Global Constraints

- Python 3.6+. No syntax newer than 3.6 (no walrus, no `list[str]` annotations, no `str.removeprefix`).
- `TSVZ.py` and `TSVZ_new_spec_tests.py` indent with tabs. Code in this plan uses tabs.
- One function per key. Adding the same key again replaces the function. There is no unregister method.
- `fn` is called as `fn(line)`. The return value is discarded.
- `line` is the logical line: text-decoded, committing newline removed, one trailing CR removed when the terminator was CRLF. Escape tokens stay literal. The line keeps the casing stored in the file.
- Matching uses the raw first field, split on the read's delimiter, compared case-insensitively over ASCII. The registry stores the key lowercased.
- `watch` is keyword-only and defaults to `None`. `None` and an empty `MarkerWatch` leave the scan unchanged.
- `add` raises `TypeError` when `fn` is not callable or the key is not a `str`. It raises `ValueError` when the key is not a whole-field `#_[A-Za-z0-9_-]+_#` marker, when it is in `OFFICIAL_MARKERS`, or when it matches `#_checksum_<algo>_#`. A failed `add` leaves previous registrations in place.
- A public replay entry, each store constructor, and `process_record` raise `TypeError` when `watch` is not `None` and not a `MarkerWatch`, before scanning, including when the part is empty or missing.
- An unrecognized marker stays `ignore`. The store is not updated and `apply_marker` is not called. Official markers in the same file still update reader state.
- Any exception from `fn` propagates. Nothing is written back. `read_store`, `read_offsets`, and `read_multipart` publish into the caller's mapping only after the scan finishes.
- `WalStore.reload` leaves in-memory rows and the unflushed pending queue in place when the scan raises. `OffsetStore.reload` builds the new index privately and installs it only on success, including when the failure is an `IntegrityError` from that scan.
- A raising callback during `WalStore(...)` aborts construction before the flush thread starts.
- The callback runs on the thread that started the read. It must not reload or mutate the store being read. `add` from inside a callback is visible to later lines in that same scan. Concurrent `add` from another thread during a scan is unsupported. `MarkerWatch` has no lock.
- These paths keep `watch=None`: `read_last_record`, `snapshot_part`, `snapshot_store`, `verify_part`, `append_checksum`, `WalStore.clear` (including the busy-path replay), and the CLI.
- A snapshot still drops custom marker lines and does not call the function.
- Do not change `tsvz-spec-v1.md`. No CLI flag. No version bump.

## File structure

- Modify `TSVZ.py`: add `MarkerWatch` and `_require_watch`; call them from `process_record` and the public replay entry points; store `watch` on `WalStore` and `OffsetStore`.
- Modify `TSVZ_new_spec_tests.py`: new `TestMarkerWatch` class, appended at the end of the file.
- Modify `README.md`: one sentence on the Markers bullet.
- Do not split `TSVZ.py`. The module is one file on purpose.

## Review Focus

- A CRLF terminator: the function sees `#__meta__#\tpayload`, not a line that still ends in `\r`.
- A torn tail: `#__meta__#\tpayload` with no committing newline does not call the function.
- A non-tab delimiter: `#__meta__#,payload` with delimiter `,` calls the function once with that whole line.
- An armed checksum: the watched line is still fed, so a digest closed over a span that contains the marker still verifies.
- `OffsetStore.reload` after `report_corruption` raises `IntegrityError`: the key that was readable before the reload is still readable.

---

### Task 1: MarkerWatch registry

**Files:**
- Modify: `TSVZ.py` (insert the class after `OFFICIAL_MARKERS`, around line 189; add the name to `__all__`)
- Test: `TSVZ_new_spec_tests.py` (append class `TestMarkerWatch`)

**Interfaces:**
- Consumes: `MARKER_RE`, `CHECKSUM_MARKER_RE`, `OFFICIAL_MARKERS` already defined in `TSVZ.py`.
- Produces:
  - `MarkerWatch.add(marker_key: str, fn: callable) -> MarkerWatch`
  - `MarkerWatch.notify(field0: str, line: str) -> None` — calls `fn(line)` when `field0.lower()` is registered, otherwise does nothing.
  - `__all__` includes `'MarkerWatch'`.

- [ ] **Step 1: Write the failing test**

Append this class to `TSVZ_new_spec_tests.py`:

```python
class TestMarkerWatch(unittest.TestCase):
	def test_add_chains_and_notify_passes_the_line(self):
		seen = []
		watch = TSVZ.MarkerWatch()
		self.assertIs(watch.add('#__meta__#', seen.append), watch)
		watch.notify('#__META__#', '#__META__#\tpayload')
		self.assertEqual(seen, ['#__META__#\tpayload'])

	def test_second_key_and_replace(self):
		seen = []
		watch = TSVZ.MarkerWatch()
		watch.add('#__meta__#', lambda line: seen.append(('first', line)))
		watch.add('#__other__#', lambda line: seen.append(('other', line)))
		watch.add('#__Meta__#', lambda line: seen.append(('second', line)))
		watch.notify('#__meta__#', 'meta-line')
		watch.notify('#__other__#', 'other-line')
		watch.notify('#__nope__#', 'absent')
		self.assertEqual(seen, [('second', 'meta-line'), ('other', 'other-line')])

	def test_future_marker_key_is_allowed(self):
		seen = []
		watch = TSVZ.MarkerWatch()
		watch.add('#_future_marker_#', seen.append)
		watch.notify('#_future_marker_#', '#_future_marker_#')
		self.assertEqual(seen, ['#_future_marker_#'])

	def test_failed_add_keeps_the_previous_registration(self):
		seen = []
		watch = TSVZ.MarkerWatch()
		watch.add('#__meta__#', seen.append)
		with self.assertRaises(TypeError):
			watch.add('#__meta__#', None)
		with self.assertRaises(TypeError):
			watch.add(1, seen.append)
		for bad in ('# comment', '#_defaults_#', '#_DEFAULTS_#',
					'#_checksum_sha256_#', '#_checksum_SHA256_#', 'alice'):
			with self.assertRaises(ValueError):
				watch.add(bad, seen.append)
		watch.notify('#__meta__#', 'still-there')
		self.assertEqual(seen, ['still-there'])

	def test_empty_watch_notifies_nothing(self):
		watch = TSVZ.MarkerWatch()
		watch.notify('#__meta__#', '#__meta__#')
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest TSVZ_new_spec_tests.py::TestMarkerWatch -v`

Expected: FAIL with `AttributeError: module 'TSVZ' has no attribute 'MarkerWatch'`

- [ ] **Step 3: Write the minimal implementation**

Insert this immediately after the `OFFICIAL_MARKERS` frozenset:

```python
class MarkerWatch:
	"""Observational callbacks for unrecognized marker lines.

	Register one function per marker key with :meth:`add`. A full replay
	that is given this object calls ``fn(line)`` for each matching
	committed line. The call does not change reconstructed rows.
	"""

	def __init__(self):
		self._fns = {}

	def add(self, marker_key, fn):
		"""Register ``fn`` for one marker key.

		Adding the same key again, ignoring ASCII case, replaces the
		function. Official markers and checksum markers are rejected.

		Args:
			marker_key: Whole-field marker key, for example ``#__meta__#``.
			fn: Called as ``fn(line)`` with the logical line.

		Returns:
			MarkerWatch: ``self``, so registrations can chain.

		Raises:
			TypeError: If ``marker_key`` is not a ``str`` or ``fn`` is not
				callable.
			ValueError: If ``marker_key`` is not a reserved marker key, or
				is an official or checksum marker.

		Examples:
			>>> watch = MarkerWatch()
			>>> watch.add('#__meta__#', lambda line: None) is watch
			True
		"""
		if not isinstance(marker_key, str):
			raise TypeError('marker_key must be a str')
		if not callable(fn):
			raise TypeError('fn must be callable')
		if (not MARKER_RE.match(marker_key)
				or marker_key.lower() in OFFICIAL_MARKERS
				or CHECKSUM_MARKER_RE.match(marker_key.lower())):
			raise ValueError(
				'marker_key must be an unrecognized #_..._# marker, '
				'not an official or checksum marker')
		self._fns[marker_key.lower()] = fn
		return self

	def notify(self, field0, line):
		"""Call the function registered for ``field0``, if any."""
		fn = self._fns.get(field0.lower())
		if fn is not None:
			fn(line)
```

Add `'MarkerWatch'` to `__all__` in the stores group, next to `'WalStore'`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest TSVZ_new_spec_tests.py::TestMarkerWatch -v`

Expected: PASS

Run: `python TSVZ.py --doctest`

Expected: all doctests pass, including the new `MarkerWatch.add` example.

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_new_spec_tests.py
git commit -m "$(cat <<'EOF'
Add MarkerWatch as a registry of one callback per marker key.

EOF
)"
```

---

### Task 2: process_record calls the watch

**Files:**
- Modify: `TSVZ.py` `process_record` (signature near line 1024, ignore branch near line 1084)
- Test: `TSVZ_new_spec_tests.py` class `TestMarkerWatch`

**Interfaces:**
- Consumes: `MarkerWatch.notify(field0, line)` and `MarkerWatch.add` from Task 1.
- Produces:
  - `_require_watch(watch) -> None`
  - `process_record(..., *, watch=None)` — keyword-only, after `raw_bytes`. When classification is `'ignore'` and `watch` is a `MarkerWatch`, calls `watch.notify(f0, raw_line)` after any checksum feed and before returning `('ignore', None)`.

- [ ] **Step 1: Write the failing test**

Add these methods to `TestMarkerWatch`:

```python
	def test_process_record_calls_with_the_raw_logical_line(self):
		seen = []
		watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
		state, store = TSVZ.ReaderState(), OrderedDict()
		kind, payload = TSVZ.process_record(
			'#__META__#\ta<sep>b', state, store, '\t', watch=watch)
		self.assertEqual((kind, payload, seen, list(store), state.defaults),
						 ('ignore', None, ['#__META__#\ta<sep>b'], [], []))

	def test_process_record_skips_comments_empty_keys_and_other_markers(self):
		seen = []
		watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
		state, store = TSVZ.ReaderState(), OrderedDict()
		TSVZ.process_record('# a comment', state, store, '\t', watch=watch)
		TSVZ.process_record('\tval', state, store, '\t', watch=watch)
		TSVZ.process_record('#__other__#\tz', state, store, '\t', watch=watch)
		TSVZ.process_record('#_defaults_#\tD1', state, store, '\t', watch=watch)
		self.assertEqual(seen, [])
		self.assertEqual(state.defaults, ['D1'])
		self.assertEqual(list(store), [])

	def test_process_record_discards_the_return_value(self):
		watch = TSVZ.MarkerWatch().add('#__meta__#', lambda line: 'nope')
		kind, payload = TSVZ.process_record(
			'#__meta__#\tx', TSVZ.ReaderState(), OrderedDict(), '\t', watch=watch)
		self.assertEqual((kind, payload), ('ignore', None))

	def test_process_record_propagates_callback_errors(self):
		def boom(line):
			raise RuntimeError('stop')
		watch = TSVZ.MarkerWatch().add('#__meta__#', boom)
		with self.assertRaises(RuntimeError):
			TSVZ.process_record(
				'#__meta__#\tx', TSVZ.ReaderState(), {}, '\t', watch=watch)

	def test_process_record_rejects_a_foreign_watch(self):
		with self.assertRaises(TypeError):
			TSVZ.process_record(
				'#__meta__#\tx', TSVZ.ReaderState(), {}, '\t', watch=object())

	def test_process_record_without_a_watch_stays_inert(self):
		state, store = TSVZ.ReaderState(), OrderedDict()
		self.assertEqual(
			TSVZ.process_record('#__meta__#\tx', state, store, '\t'),
			('ignore', None))
		self.assertEqual(list(store), [])
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest TSVZ_new_spec_tests.py::TestMarkerWatch -v`

Expected: FAIL with `TypeError: process_record() got an unexpected keyword argument 'watch'`

- [ ] **Step 3: Write the minimal implementation**

Add this function immediately after `MarkerWatch`:

```python
def _require_watch(watch):
	"""Raise TypeError unless ``watch`` is None or a MarkerWatch."""
	if watch is not None and not isinstance(watch, MarkerWatch):
		raise TypeError('watch must be None or a MarkerWatch')
```

Change the `process_record` signature to:

```python
def process_record(raw_line, state, store, delimiter, *, offset=None,
				   store_offset=False, values_cache=None, bound_states=None,
				   digests=None, raw_bytes=None, watch=None):
```

Add this to its docstring Args list:

```
		watch: Optional :class:`MarkerWatch`. An unrecognized marker whose
			key is registered calls ``fn(line)`` and stays ignored.
```

As the first statement of the body, before the line is split:

```python
	_require_watch(watch)
```

Replace the ignore/comment return so the checksum feed above it still runs first:

```python
	if kind in ('comment', 'ignore'):
		if kind == 'ignore' and watch is not None:
			watch.notify(f0, raw_line)
		return kind, None
```

Do not notify on the later empty-key `return 'ignore', None`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest TSVZ_new_spec_tests.py::TestMarkerWatch TSVZ_new_spec_tests.py::TestMarkerConformance -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_new_spec_tests.py
git commit -m "$(cat <<'EOF'
Invoke MarkerWatch from process_record for unrecognized markers.

EOF
)"
```

---

### Task 3: Thread watch through full replays

**Files:**
- Modify: `TSVZ.py` functions `replay_bytes`, `replay_part`, `_replay_stream`, `replay_parts`, `read_multipart`, `_replay_into`, `read_store`, `read_offsets`
- Test: `TSVZ_new_spec_tests.py` class `TestMarkerWatch`

**Interfaces:**
- Consumes: `process_record(..., watch=None)` and `_require_watch` from Task 2.
- Produces: keyword-only `watch=None` on `replay_bytes`, `replay_part`, `replay_parts`, `read_multipart`, `read_store`, and `read_offsets`. Private `_replay_stream` and `_replay_into` take the same argument and pass it to `process_record` / `replay_part`. `read_store(last_record_only=True)` type-checks `watch` and does not forward it to `read_last_record`.

- [ ] **Step 1: Write the failing tests**

Add these methods to `TestMarkerWatch`:

```python
	def test_replay_bytes_delivers_every_match_in_order(self):
		seen = []
		watch = (TSVZ.MarkerWatch()
				 .add('#__meta__#', seen.append)
				 .add('#__side__#', seen.append))
		body = b'#__meta__#\told\n#__side__#\ts\nk\tv1\n#__META__#\tnew\nk\tv2\n'
		store, state = TSVZ.replay_bytes(body, '\t', watch=watch)
		self.assertEqual(seen, ['#__meta__#\told', '#__side__#\ts', '#__META__#\tnew'])
		self.assertEqual(list(store['k'].row), ['k', 'v2'])
		self.assertEqual(state.defaults, [])

	def test_crlf_torn_tail_and_comma_delimiter(self):
		seen = []
		watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
		TSVZ.replay_bytes(b'#__meta__#\tpayload\r\n', '\t', watch=watch)
		TSVZ.replay_bytes(b'#__meta__#\ttorn', '\t', watch=watch)
		TSVZ.replay_bytes(b'#__meta__#,payload\n', ',', watch=watch)
		self.assertEqual(seen, ['#__meta__#\tpayload', '#__meta__#,payload'])

	def test_add_during_callback_sees_a_later_line(self):
		seen = []
		watch = TSVZ.MarkerWatch()

		def first(line):
			seen.append(line)
			watch.add('#__later__#', seen.append)

		watch.add('#__meta__#', first)
		TSVZ.replay_bytes(b'#__meta__#\ta\n#__later__#\tb\n', '\t', watch=watch)
		self.assertEqual(seen, ['#__meta__#\ta', '#__later__#\tb'])

	def test_watched_line_still_feeds_an_armed_checksum(self):
		with TempFile(suffix='.tsvz') as path:
			TSVZ.append_records(path, [['a', '1']], create=True)
			TSVZ.append_checksum(path, 'crc32')
			with open(path, 'a') as handle:
				handle.write('#__meta__#\tx\n')
			TSVZ.append_records(path, [['b', '2']])
			TSVZ.append_checksum(path, 'crc32')
			seen = []
			watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
			data = TSVZ.read_store(path, watch=watch)
			self.assertEqual(seen, ['#__meta__#\tx'])
			self.assertEqual((data._digests.verified, data._digests.mismatches), (1, []))
			self.assertEqual(data['b'], ['b', '2'])

	def test_callback_exception_leaves_the_caller_mapping_and_the_file(self):
		with TempFile(suffix='.tsvz', content=b'k\tv\n#__meta__#\tx\n') as path:
			before = read_text(path)
			caller = OrderedDict([('old', ['old', 'row'])])

			def boom(line):
				raise RuntimeError('stop')

			watch = TSVZ.MarkerWatch().add('#__meta__#', boom)
			with self.assertRaises(RuntimeError):
				TSVZ.read_store(path, watch=watch, store=caller)
			self.assertEqual(list(caller.items()), [('old', ['old', 'row'])])
			self.assertEqual(read_text(path), before)

	def test_bad_watch_on_a_missing_or_empty_read_is_type_error(self):
		with self.assertRaises(TypeError):
			TSVZ.replay_bytes(b'', '\t', watch=object())
		with self.assertRaises(TypeError):
			TSVZ.replay_part('/no/such/tsvz-marker-watch.tsvz', '\t', watch=object())
		with self.assertRaises(TypeError):
			TSVZ.read_multipart('/no/such/tsvz-marker-watch.tsvz', watch=object())
		with self.assertRaises(TypeError):
			TSVZ.read_store('/no/such/tsvz-marker-watch.tsvz', watch=object())
		with self.assertRaises(TypeError):
			TSVZ.read_offsets('/no/such/tsvz-marker-watch.tsvz', watch=object())
		empty = TSVZ.MarkerWatch()
		store, state = TSVZ.replay_bytes(b'', '\t', watch=empty)
		self.assertEqual(list(store), [])
		self.assertIsInstance(state, TSVZ.ReaderState)

	def test_read_offsets_and_read_multipart_notify_in_ordinal_order(self):
		with TempFile(suffix='.tsv', content=b'#__meta__#\tx\na\t1\n') as path:
			seen = []
			watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
			offsets, _values, _state = TSVZ.read_offsets(path, watch=watch)
			self.assertEqual(seen, ['#__meta__#\tx'])
			self.assertIn('a', offsets)
		directory = tempfile.mkdtemp()
		try:
			base = os.path.join(directory, 'ev.tsvz')
			with open(TSVZ.part_path(base, 1), 'w') as handle:
				handle.write('#__meta__#\tone\n')
			with open(TSVZ.part_path(base, 2), 'w') as handle:
				handle.write('#__meta__#\ttwo\nk\tv\n')
			seen = []
			watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
			rows = TSVZ.read_multipart(base, watch=watch)
			self.assertEqual(seen, ['#__meta__#\tone', '#__meta__#\ttwo'])
			self.assertEqual(rows['k'], ['k', 'v'])
		finally:
			shutil.rmtree(directory)

	def test_last_record_snapshot_and_verify_do_not_notify(self):
		with TempFile(suffix='.tsvz', content=b'#__meta__#\tx\na\t1\n') as path:
			seen = []
			watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
			with warnings.catch_warnings():
				warnings.simplefilter('ignore', DeprecationWarning)
				self.assertEqual(
					TSVZ.read_store(path, watch=watch, last_record_only=True),
					['a', '1'])
			self.assertEqual(seen, [])
			calls = []
			real = TSVZ.process_record

			def spy(raw_line, state, store, delimiter, **kwargs):
				calls.append(kwargs.get('watch'))
				return real(raw_line, state, store, delimiter, **kwargs)

			TSVZ.process_record = spy
			try:
				TSVZ.snapshot_part(path)
				TSVZ.verify_part(path)
			finally:
				TSVZ.process_record = real
			self.assertTrue(calls)
			self.assertTrue(all(item is None for item in calls))
			text = read_text(path)
			self.assertNotIn('#__meta__#', text)
			self.assertIn('a\t1', text)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest TSVZ_new_spec_tests.py::TestMarkerWatch -v`

Expected: FAIL with `TypeError: replay_bytes() got an unexpected keyword argument 'watch'`

- [ ] **Step 3: Write the minimal implementation**

Add `watch=None` as the last keyword-only parameter of `replay_bytes`, `replay_part`, `_replay_stream`, `replay_parts`, `read_multipart`, `_replay_into`, `read_store`, and `read_offsets`.

Call `_require_watch(watch)` as the first statement of each of those functions, before any file is opened and before a missing path raises `FileNotFoundError`.

Pass `watch=watch` into every `process_record`, `replay_part`, `replay_parts`, and `_replay_into` call that belongs to these functions. The exact call updates:

`replay_bytes` loop:

```python
		process_record(
			line, state, store, delimiter,
			offset=offset, store_offset=store_offset, values_cache=values_cache,
			bound_states=bound_states,
			digests=digests, raw_bytes=raw, watch=watch,
		)
```

`_replay_stream` loop:

```python
		process_record(
			line, state, store, delimiter, offset=offset,
			store_offset=store_offset, values_cache=values_cache,
			bound_states=bound_states, digests=digests, raw_bytes=raw,
			watch=watch,
		)
```

`replay_part` passes `watch=watch` into `_replay_stream`. Its `FileNotFoundError` return stays after `_require_watch`.

```python
		_replay_stream(f, path, delimiter, encoding=encoding, store=store, state=state,
					   store_offset=store_offset, values_cache=values_cache,
					   bound_states=bound_states, errors=errors, digests=digests,
					   watch=watch)
```

`replay_parts`:

```python
		store, state = replay_part(
			path, delimiter, encoding=encoding, store=store, state=state,
			digests=digests, errors=errors, watch=watch,
		)
```

`read_multipart` passes `watch=watch` into `replay_parts`. Its "no parts" `FileNotFoundError` stays after `_require_watch`.

```python
	replayed, state, digests = replay_parts(
		paths, delimiter, encoding=encoding, errors=errors, watch=watch)
```

`_replay_into`:

```python
	replayed, state = replay_part(
		path, delimiter, encoding=encoding, store=OrderedDict(),
		store_offset=store_offset, values_cache=values_cache,
		bound_states=bound_states, digests=digests, watch=watch,
	)
```

`read_store`:

```python
	if not (last_record_only or store_offset) and _promoted_parts(path):
		return read_multipart(path, encoding=encoding, delimiter=delimiter,
							  store=store, policy=policy, watch=watch)
```

Leave the `read_last_record(...)` call unchanged so `last_record_only=True` does not forward `watch`. Pass `watch=watch` into both `_replay_into` calls:

```python
		store, state, values_cache, digests = _replay_into(
			path, store, create=create, encoding=encoding, delimiter=delimiter,
			defaults=defaults, header=header, store_offset=True, policy=policy,
			watch=watch)
```

```python
	store, state, _values, digests = _replay_into(
		path, store, create=create, encoding=encoding, delimiter=delimiter,
		defaults=defaults, header=header, store_offset=False, policy=policy,
		watch=watch)
```

`read_offsets`:

```python
	store, state, values_cache, digests = _replay_into(
		path, store, create=create, encoding=encoding, delimiter=delimiter,
		defaults=defaults, header=header, store_offset=True,
		cache_values=cache_values, policy=policy, bound_states=bound_states,
		watch=watch)
```

Add this Args line to each of the six public docstrings (`replay_bytes`, `replay_part`, `replay_parts`, `read_multipart`, `read_store`, `read_offsets`):

```
		watch: Optional :class:`MarkerWatch` invoked for unrecognized markers
			during this scan. ``None`` leaves those lines ignored.
```

Do not add `watch` to `read_last_record`, `verify_part`, `append_checksum`, `_replay_through`, or `_compact_in_place`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest TSVZ_new_spec_tests.py::TestMarkerWatch TSVZ_new_spec_tests.py::TestMarkerConformance -v`

Expected: PASS. `test_last_record_snapshot_and_verify_do_not_notify` passes because snapshot and verify still call `process_record` without `watch`.

Run: `python TSVZ.py --doctest`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_new_spec_tests.py
git commit -m "$(cat <<'EOF'
Pass MarkerWatch through the public full-replay readers.

EOF
)"
```

---

### Task 4: Stores keep the watch, and the README names it

**Files:**
- Modify: `TSVZ.py` `WalStore.__init__` (near line 3624), `WalStore.reload` (near line 3669), `OffsetStore.__init__` (near line 4062), `OffsetStore.reload` (near line 4102)
- Modify: `README.md` Markers bullet (near line 150)
- Test: `TSVZ_new_spec_tests.py` class `TestMarkerWatch`

**Interfaces:**
- Consumes: `read_store(..., watch=None)`, `read_offsets(..., watch=None)`, `read_multipart(..., watch=None)`, and `_require_watch` from Task 3.
- Produces:
  - `WalStore(path, *, ..., watch=None)` stores the object on `self._watch` and passes it to `read_store` / `read_multipart` from `reload`. `reload()` grows no new parameter.
  - `OffsetStore(path, *, ..., watch=None)` does the same via `read_offsets`. `reload` installs the new index only after `read_offsets` returns.

- [ ] **Step 1: Write the failing tests**

Add these methods to `TestMarkerWatch`:

```python
	def test_walstore_open_and_reload_notify_again(self):
		with TempFile(suffix='.tsvz', content=b'#__meta__#\tx\na\t1\n') as path:
			seen = []
			watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
			store = TSVZ.WalStore(path, watch=watch, flush_interval=1000)
			try:
				self.assertEqual(seen, ['#__meta__#\tx'])
				self.assertEqual(store['a'], ['a', '1'])
				store.reload()
				self.assertEqual(seen, ['#__meta__#\tx', '#__meta__#\tx'])
			finally:
				store.close()

	def test_walstore_reload_failure_keeps_rows_and_pending(self):
		with TempFile(suffix='.tsvz', content=b'#__meta__#\tx\na\t1\n') as path:
			calls = {'n': 0}

			def maybe(line):
				calls['n'] += 1
				if calls['n'] > 1:
					raise RuntimeError('reload')

			watch = TSVZ.MarkerWatch().add('#__meta__#', maybe)
			store = TSVZ.WalStore(path, watch=watch, flush_interval=1000)
			try:
				store['b'] = ['b', 'queued']
				with self.assertRaises(RuntimeError):
					store.reload()
				self.assertEqual(store['a'], ['a', '1'])
				self.assertEqual(list(store._pending), [['b', 'queued']])
				self.assertEqual(read_text(path), '#__meta__#\tx\na\t1\n')
			finally:
				store.close()

	def test_walstore_construction_failure_starts_no_thread(self):
		with TempFile(suffix='.tsvz', content=b'#__meta__#\tx\n') as path:
			def boom(line):
				raise RuntimeError('open')
			watch = TSVZ.MarkerWatch().add('#__meta__#', boom)
			before = threading.active_count()
			with self.assertRaises(RuntimeError):
				TSVZ.WalStore(path, watch=watch, flush_interval=1000)
			self.assertEqual(threading.active_count(), before)

	def test_multipart_walstore_notifies_in_ordinal_order(self):
		directory = tempfile.mkdtemp()
		try:
			base = os.path.join(directory, 'ev.tsvz')
			with open(TSVZ.part_path(base, 1), 'w') as handle:
				handle.write('#__meta__#\tone\n')
			with open(TSVZ.part_path(base, 2), 'w') as handle:
				handle.write('#__meta__#\ttwo\n')
			seen = []
			watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
			store = TSVZ.WalStore(base, multipart=True, watch=watch, flush_interval=1000)
			try:
				self.assertEqual(seen, ['#__meta__#\tone', '#__meta__#\ttwo'])
			finally:
				store.close()
		finally:
			shutil.rmtree(directory)

	def test_offset_reload_failure_keeps_the_previous_row(self):
		with TempFile(suffix='.tsv', content=b'#__meta__#\tx\na\t1\n') as path:
			calls = {'n': 0}

			def maybe(line):
				calls['n'] += 1
				if calls['n'] > 1:
					raise RuntimeError('reload')

			watch = TSVZ.MarkerWatch().add('#__meta__#', maybe)
			store = TSVZ.OffsetStore(path, watch=watch)
			try:
				self.assertEqual(store['a'], ['a', '1'])
				with self.assertRaises(RuntimeError):
					store.reload()
				self.assertEqual(store['a'], ['a', '1'])
			finally:
				store.close()

	def test_offset_reload_integrity_error_keeps_the_index(self):
		with TempFile(suffix='.tsv', content=b'a\t1\n') as path:
			store = TSVZ.OffsetStore(path)
			real = TSVZ.report_corruption

			def boom(*_args, **_kwargs):
				raise TSVZ.IntegrityError('injected')

			try:
				TSVZ.report_corruption = boom
				with self.assertRaises(TSVZ.IntegrityError):
					store.reload()
				self.assertEqual(store['a'], ['a', '1'])
			finally:
				TSVZ.report_corruption = real
				store.close()

	def test_clear_busy_path_does_not_notify(self):
		with TempFile(suffix='.tsvz', content=b'#__meta__#\tx\na\t1\n') as path:
			seen = []
			watch = TSVZ.MarkerWatch().add('#__meta__#', seen.append)
			store = TSVZ.WalStore(path, watch=watch, flush_interval=1000, lock_timeout=0)
			other = TSVZ.WalStore(path, flush_interval=1000, lock_timeout=0)
			try:
				seen.clear()
				store.clear()
				self.assertEqual(seen, [])
				self.assertIn('#__meta__#', read_text(path))
			finally:
				other.close()
				store.close()

	def test_bad_watch_is_rejected_by_constructors(self):
		with TempFile(suffix='.tsvz', content=b'a\t1\n') as path:
			with self.assertRaises(TypeError):
				TSVZ.WalStore(path, watch=object())
			with self.assertRaises(TypeError):
				TSVZ.OffsetStore(path, watch=object())
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest TSVZ_new_spec_tests.py::TestMarkerWatch -v`

Expected: FAIL with `TypeError: __init__() got an unexpected keyword argument 'watch'`

- [ ] **Step 3: Write the minimal implementation**

`WalStore.__init__`: add `watch=None` after `lock_timeout`. First statement of the body:

```python
		_require_watch(watch)
		self._watch = watch
```

`WalStore.reload`: pass `watch=self._watch` into both reads. Do not clear `self` or `_pending` before that call. The existing `except FileNotFoundError` and the later `super().clear()` stay where they are, so a callback exception still skips the clear.

```python
				read_multipart(
					self.store_path, encoding=self.encoding,
					delimiter=self.delimiter, store=loaded, watch=self._watch)
```

```python
				read_store(
					self.path, create=self.create, encoding=self.encoding,
					delimiter=self.delimiter, store=loaded,
					header=self.header or None, watch=self._watch,
				)
```

`OffsetStore.__init__`: add `watch=None` after `lock_timeout`. First statement of the body, before the compressed-path check:

```python
		_require_watch(watch)
		self._watch = watch
```

Replace `OffsetStore.reload` with:

```python
	def reload(self):
		"""Rebuild the offset index by replaying the part from disk.

		The new index is installed only after replay returns, so a failure
		leaves the previous offsets, value cache, bindings, and reader
		state in place.

		Returns:
			OffsetStore: ``self``, after a successful replay.
		"""
		bindings = {}
		offsets, _values, state = read_offsets(
			self.path, create=self.create, encoding=self.encoding,
			delimiter=self.delimiter, cache_values=False,
			bound_states=bindings, watch=self._watch)
		intern = {}
		for key, binding in bindings.items():
			bindings[key] = intern.setdefault(binding, binding)
		self._offsets.clear()
		self._values.clear()
		self._bindings.clear()
		self._offsets.update(offsets)
		self._bindings.update(bindings)
		self._reader_state = state
		return self
```

Leave `WalStore.clear`'s `replay_part(...)` call without `watch`.

In `README.md`, extend the Markers bullet so it reads:

```markdown
- **Markers.** Reserved lines matching `#_[A-Za-z0-9_-]+_#` set reader state
  (`#_version_#`, `#_defaults_#`, `#_strip_trailing_whites_#`, and others — spec
  §12). Official markers use `#_name_#`; custom extensions should use
  `#__name__#` to avoid collision. Pass a `MarkerWatch` to a read to invoke a
  function with the logical line when a matching custom marker is seen. The
  marker still does not affect reconstructed rows, and a snapshot still drops
  the line.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest TSVZ_new_spec_tests.py -v`

Expected: PASS, including `TestMarkerWatch` and the existing marker, checksum, and store tests.

Run: `python TSVZ.py --doctest`

Expected: PASS

`test_clear_busy_path_does_not_notify` requires the second open handle to block exclusive upgrade (Linux open-file-description locks). The assertion that `#__meta__#` is still in the file is what proves `clear` took the busy path. If that assertion fails because the part was rewritten, the second `WalStore` did not block the upgrade; hold a shared `_PartHandle(path)` across `store.clear()` instead, and keep both assertions.

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_new_spec_tests.py README.md
git commit -m "$(cat <<'EOF'
Let WalStore and OffsetStore replay through a MarkerWatch.

EOF
)"
```
