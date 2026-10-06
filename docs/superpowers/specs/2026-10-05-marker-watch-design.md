# Marker watch

A caller of TSVZ 4.0 can register one function per marker key. When a full
replay reads a matching line, that function is called with the logical line.
The call is observational. Reconstructed rows, reader state, and snapshots
stay as they are today when no watch is registered.

This is allowed by tsvz-spec-v1 §12.3.1: an unrecognized marker has no effect
on reconstructed state, and a processor may act on its own markers. This
design does not change `tsvz-spec-v1.md`.

## API

`MarkerWatch` is a new public class in `TSVZ.py`, exported from `__all__`.

```python
watch = TSVZ.MarkerWatch()
watch.add('#__mytool_meta__#', on_meta)   # on_meta(line)
watch.add('#__other__#', on_other)        # a second key is a second add
rows = TSVZ.read_store(path, watch=watch)
store = TSVZ.WalStore(path, watch=watch)  # also runs during open and reload()
```

`add(marker_key, fn)` stores one function for that key and returns the watch,
so calls can chain. Adding the same key again replaces the function. There is
no unregister method. The caller keeps the `MarkerWatch` instance; a store
holds that same object and uses it on later reloads. `add` on the caller's
object is how a registration changes. Replacing a private attribute on the
store is not part of the API.

`fn` is called as `fn(line)`. `line` is the logical line the reader already
uses: the committed record, text-decoded, with the committing newline removed
and with one trailing CR removed when the terminator was CRLF. Escape tokens
are still literal. A line such as `#__mytool_meta__#\tpayload` is passed
whole. The return value is discarded.

Matching uses the raw first field, split on the read's delimiter, compared
case-insensitively over ASCII. The registry key is stored lowercased. The
line passed to `fn` keeps the casing stored in the file.

`watch` is a keyword-only argument and defaults to `None`. `None` and an
empty `MarkerWatch` leave the scan unchanged.

### Where `watch` is accepted

Threaded through the public full replays:

- `process_record`
- `replay_bytes`
- `replay_part`
- `replay_parts`
- `read_store`
- `read_offsets`
- `read_multipart`
- `WalStore` and `OffsetStore` constructors

`WalStore` and `OffsetStore` keep the object and pass it on open and on every
`reload()`. A multi-part `WalStore` scans parts in ordinal order. `reload()`
itself grows no new parameter.

These paths keep `watch=None`:

- `read_last_record`
- snapshot (`snapshot_part`, `snapshot_store`, and their internal replay)
- checksum (`verify_part`, `append_checksum`, and their internal replay)
- `WalStore.clear`, including its busy-path replay
- the CLI

`process_record` is the single place that invokes the function. The public
replays pass `watch` through. The excluded paths call the same replay helpers
without it.

## Data flow

`add` stores `marker_key.lower()` → `fn`.

During a scan, `_iter_records` yields each committed record. Uncommitted
trailing bytes are not records and do not call the function. `process_record`
classifies the line, feeds any armed checksum as it does today, then handles
the line. When the line is an unrecognized marker and its first field is
registered, it calls `fn(line)` and then returns `('ignore', None)`.

The store is not updated. `apply_marker` is not called. Comments, empty-key
rows, and unregistered custom markers do not call the function. An official
marker in the same file still updates reader state.

Each matching line calls the function, in file order, including a line that a
later data row or tombstone has superseded. A later data row does not call
the function again. `replay_parts` and a multi-part `WalStore` use one watch
for every part, so a marker in a later part is seen after the earlier parts.

`reload()` scans the whole store again and calls the function again for every
matching line still on disk. A `WalStore` queue that has not been flushed is
not visible. The flush thread does not replay, so it does not call the
function. The function runs on the thread that started the read.

Snapshot, checksum, and `clear()` do not receive the watch. A snapshot still
drops custom marker lines and does not call the function while it replays.

## Registration errors

`add` raises `TypeError` when `fn` is not callable or `marker_key` is not a
`str`. It raises `ValueError` when the key is not a whole-field marker
matching `#_[A-Za-z0-9_-]+_#`, when the key is in `OFFICIAL_MARKERS`, or when
the key matches `#_checksum_<algo>_#`. After either failure the watch still
holds exactly the registrations it held before the call.

A public replay entry, each store constructor, and `process_record` raise
`TypeError` when `watch` is not `None` and not a `MarkerWatch`. Replay
entries and constructors do this before scanning, including when the part is
empty or missing. Private helpers (`_replay_stream`, `_replay_into`) take
the same `watch` argument so every public path shares one call into
`process_record`.

## Callback errors

Any exception from `fn` propagates out of `process_record` and out of the
read that started the scan. Nothing is caught. Nothing is written back to
the file. When a checksum accumulator is active, the line has already been
fed to it. Abandoning the scan discards that accumulator, so the feed has
no effect.

`read_store`, `read_offsets`, and `read_multipart` replay into a private
mapping and publish into the caller's mapping only after the scan finishes.
A failure leaves the caller's mapping as it was.

`WalStore.reload` already replays into a private mapping. A failure leaves
both the in-memory rows and the unflushed pending queue as they were.
`OffsetStore.reload` today clears its index and then replays. That sequence
changes: it builds the new offset index, value cache, bindings, and reader
state privately, and swaps them in only on success. A failure leaves the
previous index, cache, bindings, and reader state in place. This holds for
every `OffsetStore.reload`, including an integrity error from that same scan.

A callback that raises during `WalStore(...)` or `OffsetStore(...)` aborts
construction. `WalStore` starts its flush thread only after a successful
initial reload, so a failure during construction leaves no flush thread
running.

The callback runs inside the scan. It must not reload or mutate the store
being read. `add` from inside a callback is visible to later lines in that
same scan. Concurrent `add` from another thread during a scan is unsupported.
`MarkerWatch` has no lock.

## Documentation

The README markers bullet gains one sentence: a caller can pass a
`MarkerWatch` so a matching custom marker invokes a function with the logical
line; the marker still does not affect reconstructed rows, and a snapshot
still drops the line.

`MarkerWatch.add` carries a short doctest in the style of the other public
functions in `TSVZ.py`.

## Tests

New cases live in `TSVZ_new_spec_tests.py` next to the existing marker tests.
They assert:

- A registered `#__name__#` line calls the function with the logical line,
  once per occurrence, in file order, including a line a later row has
  superseded. Reconstructed rows and reader state match a scan with no watch.
- A second `add` watches a second key. Adding the same key again keeps only
  the new function. Matching is case-insensitive, and the line the function
  sees keeps the casing stored in the file. A value field is still on that
  line, escape tokens included.
- `add` raises `TypeError` for a non-callable or a non-string key, and
  `ValueError` for a non-marker, an official marker, or a checksum marker.
  After a failed `add`, the previous registration is still there. A public
  read rejects a `watch` that is not a `MarkerWatch` before scanning.
- A comment, an empty-key row, and an unregistered custom marker do not call
  the function. An official marker in the same file still updates reader
  state.
- The function's return value is discarded. An exception from the function
  leaves the caller's mapping and the file as they were. `WalStore.reload`
  and `OffsetStore.reload` keep the previously loaded contents. A raising
  callback during `WalStore` construction leaves no flush thread running.
- Opening a `WalStore` or `OffsetStore` runs the function, and `reload()`
  runs it again for every matching line still on disk. A multi-part store
  runs it in ordinal order across parts.
- `snapshot_part` does not call the function, and the snapshot file does not
  contain the custom marker line.

## Out of scope

- Writing custom markers through the store API. Callers that want a line on
  disk already have `format_marker_line`.
- Retaining marker payload on the store after the scan.
- Re-emitting custom markers from a snapshot.
- A CLI flag.
- A watch on `read_last_record`, snapshot, checksum, or `clear`.
- A version bump. Release versioning stays on the existing release process.
