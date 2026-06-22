# TSVZ

TSVZ is a tiny, dependency-free library and CLI for treating a **tab-separated
values file as a key-value store**. The first column of every row is a unique
key; the file behaves like an ordered dictionary that is transparently
persisted to disk.

For the strict `.tsvz` WAL format, see **[tsvz-spec-v1.md](tsvz-spec-v1.md)**.

Two front-ends are provided:

- **`TSVZed`** — an in-memory `OrderedDict` backed by the file, with a
  background worker that does **non-blocking, append-only** writes, optional
  periodic / on-exit rewrites, file locking for multi-process access, and
  transparent (de)compression.
- **`TSVZedLite`** — a `MutableMapping` that keeps only a **key → byte-offset
  index** in memory and reads values from disk on demand. Minimal footprint,
  single-process, append-only.

It is a single self-contained module (`TSVZ.py`) — drop it next to your code and
`import TSVZ`, or install it.

```bash
pip install tsvz          # from PyPI
pip install -e .          # editable, from this directory
```

---

## Quick start

### As a library

```python
import TSVZ

# Opens (creating if needed), loads into memory, starts the async writer.
# Per tsvz-spec-v1.md the column header is a # comment; the reference
# implementation still accepts a legacy non-# first line (see Implementation notes).
db = TSVZ.TSVZed('data.tsv', header='id\tname\tscore')

db['alice'] = ['alice', 'Alice', '10']   # value as a list
db['bob']   = 'bob\tBob\t20'             # ...or a delimited string
db['alice'] = ['alice', 'Alice', '11']   # last write wins
del db['bob']                            # deletes (and persists the deletion)

print(db['alice'])        # ['alice', 'Alice', '11']
db.close()                # flush the queue and stop the worker (also runs atexit)
```

There are also stateless helper functions if you just want to touch a file once:
`readTabularFile`, `appendTabularFile`, `appendLinesTabularFile`,
`clearTabularFile`, `scrubTabularFile` (and `readTSV` / `appendTSV` / … aliases).

### From the command line

```bash
tsvz data.tsv                          # read + pretty-print
tsvz data.tsv append alice Alice 10    # append/update a row keyed by "alice"
tsvz data.tsv delete alice             # delete the row keyed by "alice"
tsvz data.tsv clear                    # truncate to just the header
tsvz data.tsv scrub                    # compact (snapshot) — drops comments, applies last-wins

tsvz data.csv -d comma append k v1 v2  # pick a delimiter explicitly
tsvz -h
```

---

## The TSVZ file format

The strict `.tsvz` / `.csvz` / `.nsvz` / `.psvz` formats are defined in
**[tsvz-spec-v1.md](tsvz-spec-v1.md)** (format specification version 1, draft).
That document is the canonical reference for any conformant reader or writer — not
just this library.

> **Status:** The spec is draft; the reference implementation in `TSVZ.py` does not
> fully conform yet (see *Implementation notes*).

### What the format is

`.tsvz` and its siblings are a **format in their own right** — not merely a `.tsv`
that TSVZ happens to manage. They define a **strict, append-only,
write-ahead-log (WAL) key–value store** in plain UTF-8 text: replay the file
forward with **last-wins** semantics to reconstruct current state.

The plain extensions `.tsv` / `.csv` / `.nsv` / `.psv` are a **loose tabular
fallback** with no semantic guarantees. The same delimiter mapping applies to both;
only the `z` extensions carry the full contract in
[tsvz-spec-v1.md](tsvz-spec-v1.md).

| Variant | Strict ext. | Loose ext. | Delimiter |
|---------|-------------|------------|-----------|
| TSVZ    | `.tsvz`     | `.tsv`     | tab `\t`  |
| CSVZ    | `.csvz`     | `.csv`     | comma `,` |
| NSVZ    | `.nsvz`     | `.nsv`     | NUL `\0`  |
| PSVZ    | `.psvz`     | `.psv`     | pipe `\|` |

There is no RFC 4180-style quoting; delimiters and newlines inside field data are
represented by escape tokens (see the spec §13).

### At a glance

These are summaries only — normative rules, edge cases, and the full reading
pipeline are in [tsvz-spec-v1.md](tsvz-spec-v1.md).

- **Append-only WAL.** Normal operation only appends; updates and deletes are new
  lines at the end. **Snapshot / compaction** (spec §19) is the one sanctioned
  rewrite.
- **Key–value model.** Field 0 is the key; value columns follow. Column count is
  not fixed — absent trailing columns default per the active `#_defaults_#` marker
  (spec §14).
- **Tombstones.** A delete is a data row with **no delimiter** — just the key and
  `\n` (e.g. `mykey\n`). A row like `mykey\t\n` is *not* a tombstone; it stores an
  empty value column.
- **Commit marker.** The trailing `\n` commits a record; bytes after the last `\n`
  in a part are discarded (crash / torn-tail recovery, spec §4).
- **Comments and header.** A line whose raw first field begins with `#` (and is not
  a recognized marker) is a comment. There is no formal header — document columns
  with a comment line such as `#id\tname\tscore`.
- **Markers.** Reserved lines matching `#_[A-Za-z0-9_-]+_#` set reader state
  (`#_version_#`, `#_defaults_#`, `#_strip_trailing_whites_#`, and others — spec
  §12). Official markers use `#_name_#`; custom extensions should use
  `#__name__#` to avoid collision.
- **Escaping.** Reversible control tokens: `<sep>`, `<LF>`, `<lt>`, `<#>` (spec
  §13). Every literal `<` in field data is encoded; there are no lossy escape
  cases.
- **Optional integrity.** `#_checksum_<algo>_#` markers enable segment digest
  verification (spec §15).
- **Multi-part stores.** `store.tsvz.<ordinal>` parts are ordered by hexadecimal
  ordinal, ascending; replay spans part boundaries (spec §17). Snapshots can slot
  a new part between an immutable prefix and the active writer (spec §19).
- **Compression.** An orthogonal per-part stream filter (`.gz`, `.zst`, …; spec
  §16).

A worked replay example appears in Appendix C of
[tsvz-spec-v1.md](tsvz-spec-v1.md).

---

## API

### `TSVZed(fileName, …)`

An `OrderedDict` subclass that auto-syncs to `fileName`. Selected options:

| Option | Default | Meaning |
|---|---|---|
| `header` | `''` | Header line (string or list) for creation/validation. |
| `createIfNotExist` | `True` | Create the file if missing. |
| `verifyHeader` | `True` | Check the file's first line against `header`. |
| `delimiter` | auto | Delimiter; inferred from the extension if omitted. |
| `defaults` | `None` | Per-column default values. |
| `strict` | `False` | Enforce column counts / header; reject malformed rows. |
| `rewrite_on_load` | `True` | Compact the file once when opened. |
| `rewrite_on_exit` | `False` | Compact the file when closed. |
| `rewrite_interval` | `0` | Min seconds between periodic compactions (`0` = never). |
| `monitor_external_changes` | `True` | Detect / merge other processes' writes. |
| `encoding` | `'utf8'` | File encoding. |

Writes are queued and flushed by a background thread; call `close()` (or use it
as a context manager) to flush and stop cleanly. Keys starting with `#` are kept
in memory only (handy for scratch state) and are never written.

### `TSVZedLite(fileName, …)`

A `MutableMapping` that stores only a key→offset index in RAM and seeks into the
file for each read. Append-only; single-process; no compression, no background
thread, no external-change monitoring. Good for very large, mostly-write,
seek-cheap (SSD) workloads. An external index dict can be supplied to skip the
initial full scan (turning it into a small key-value store).

### Module functions

`readTabularFile`, `appendTabularFile`, `appendLinesTabularFile`,
`clearTabularFile`, `scrubTabularFile`, `get_delimiter`, `pretty_format_table`,
plus the legacy `readTSV` / `appendTSV` / `clearTSV` / `scrubTSV` aliases.

---

## Implementation notes (reference behavior vs. [tsvz-spec-v1.md](tsvz-spec-v1.md))

[tsvz-spec-v1.md](tsvz-spec-v1.md) is the target definition. The current
`TSVZed` / `TSVZedLite` implementation differs or only partially implements it in
these areas — useful to know, and candidates for a future alignment pass:

- **Tombstones.** The spec (§9) deletes only on a delimiter-free row (`key\n`).
  The reference code also treats all-empty value columns as deletion.
- **Escaping.** The spec (§13) uses `<sep>`, `<LF>`, `<lt>`, and `<#>` with no
  lossy cases. The reference code uses the older `</sep/>` / `</LF/>` scheme and
  does not encode `<` or leading `#` in keys.
- **Markers.** Only `#_defaults_#` is partially recognized today. The spec (§12)
  defines `#_version_#`, `#_strip_trailing_whites_#`, `#_fill_empty_with_default_#`,
  `#_return_defaults_when_missing_#`, `#_rotate_#`, `#_write_ack_#`, and
  `#_checksum_<algo>_#` — none of which are implemented yet.
- **Column normalization.** Rather than variable-length rows with absent trailing
  columns resolved at read time (spec §14), the classes detect a column count
  (from the header or first row) and, in non-strict mode, pad/trim rows to it;
  `strict` mode rejects mismatches.
- **Header.** The reference classes still use a *non*-`#` first line as the
  header (validated via `verifyHeader`), whereas spec §11 defines the header as an
  ordinary `#` comment. Existing files written by older versions use the non-`#`
  header.
- **Strict vs. loose extensions.** `get_delimiter` infers from `.tsv`/`.csv`/`.nsv`/`.psv`
  only; `.csvz`/`.nsvz`/`.psvz` currently fall back to tab (`.tsvz` happens to
  land on tab). The code does not yet key behavior off the `z` suffix or enforce
  the strict WAL contract separately for strict extensions.
- **Multi-part, snapshots, integrity.** No multi-part read/write, no spec §19
  snapshot procedure (ordinal slotting, `.rotated` exclusion), and no checksum
  markers (spec §15).
- **Rewrite-on-load.** `rewrite_on_load` / `mapToFile` perform in-place compaction
  on open, which conflicts with the strict append-only model for `.tsvz` files
  (spec §3.1, §19).

### Open implementation questions

Raised by aligning the reference code with the spec; not yet decided in
`TSVZ.py`:

1. **Strict contract gating.** Should `TSVZed`/`TSVZedLite` key append-only WAL
   behavior off the `z` extension (`.tsv` = lenient table, `.tsvz` = spec §3), or
   treat all extensions the same?
2. **Rewrite machinery.** A strict `.tsvz` arguably forbids in-place rewrite
   (`rewrite_on_load` / `mapToFile`) except via the spec §19 snapshot procedure.
3. **History retention.** If keeping the full append log matters, compaction must
   be an explicit, separate action — and may emit a snapshot part rather than
   mutating the active log in place.

## Tests

```bash
python -m pytest test_TSVZ.py            # regression suite
python test_TSVZ.py                      # or run directly
python benchTSVZ.py data.tsv -n 100000   # throughput benchmark (not a test)
```

## License

GPL-3.0-or-later © Yufei Pan (pan@zopyr.us)
