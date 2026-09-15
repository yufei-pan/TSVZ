# TSVZ

TSVZ is a tiny, dependency-free library and CLI for treating a **tab-separated
values file as a key-value store**. The first column of every row is a unique
key; the file behaves like an ordered dictionary that is transparently
persisted to disk.

**Version 4.0** is the [tsvz-spec-v1.md](tsvz-spec-v1.md) reference
implementation. It requires **Python 3.8+** and is **not a drop-in replacement
for 3.x**. The frozen 3.39 API remains as `TSVZ_old` (that module still runs on
3.6).

Preferred 4.0 front-ends:

- **`WalStore`** — in-memory `OrderedDict` with a background append-only writer
  (§18), optional `#_write_ack_#` fsync, multi-part stores, and compression.
- **`OffsetStore`** — key → byte-offset index; values are read from disk on
  demand. Uncompressed single-part only.

`TSVZed` / `TSVZedLite` still exist as thin wrappers around those classes. They
emit `DeprecationWarning` and are scheduled for removal in 5.0. They follow
**4.0 file semantics**, not 3.x.

It is a self-contained module (`TSVZ.py`) — drop it next to your code and
`import TSVZ`, or install it. `TSVZ_old.py` ships in the same package.

```bash
pip install tsvz          # from PyPI
pip install -e .          # editable, from this directory
```

---

## Breaking changes in 4.0

TSVZ 4.0 reads and writes **tsvz-spec-v1** only. Existing 3.x files will
silently reconstruct **wrong**:

| 3.x | 4.0 |
|---|---|
| Non-`#` first line is a header | It is a **data key** (`id` in `id\tname\tval`) |
| `key\t\t` (empty value columns) is a delete | Only a **lone** `key\n` is a tombstone; `key\t\t` is live empty cells, so 3.x deletes **resurrect** |
| `</sep/>` / `</LF/>` escapes | Unknown tokens pass through; spec tokens are `<sep>`, `<LF>`, `<lt>`, `<#>` |
| `#` keys were RAM-only scratch | They persist as `<#>key` |
| `defaults='#_defaults_#\tNA'` is one TSV line | A string is split **character-by-character** — pass a list |
| `rewrite_on_load=True` by default | **`False`** |
| CLI `-s/--strict` = column checks; `-v` verbose | `-s` = missing file is an error; **`-v` is gone** |

To keep 3.x behaviour:

```python
import TSVZ_old as TSVZ
```

`import TSVZ` itself is silent. The CLI (`tsvz --help` / `--version`) and the
legacy wrappers (`TSVZed`, `readTabularFile`, …) print this warning.

---

## Quick start

### As a library

```python
import TSVZ

db = TSVZ.WalStore('data.tsvz', header=['id', 'name', 'score'], create=True)
db['alice'] = ['alice', 'Alice', '10']
db['bob'] = ['bob', 'Bob', '20']
db['alice'] = ['alice', 'Alice', '11']   # last write wins
del db['bob']                            # appends a tombstone (lone key)
print(db['alice'])                       # ['alice', 'Alice', '11']
db.close()
```

Stateless helpers: `read_store`, `append_records`, `delete_records`,
`snapshot_part`, `read_multipart`. The 3.x names (`readTabularFile`, …) still
work and warn.

### From the command line

```bash
tsvz data.tsvz                         # read + pretty-print
tsvz data.tsvz append alice Alice 10   # append/update
tsvz data.tsvz delete alice            # tombstone
tsvz data.tsvz clear                   # truncate
tsvz data.tsvz scrub                   # compact (snapshot)
tsvz data.tsvz verify                  # §15 checksums
tsvz data.tsvz parts                   # list §17 parts

tsvz data.csvz -d comma append k v1 v2
tsvz -h
tsvz -V
```

---

## The TSVZ file format

The strict `.tsvz` / `.csvz` / `.nsvz` / `.psvz` formats are defined in
**[tsvz-spec-v1.md](tsvz-spec-v1.md)** (format specification version 1, draft).
That document is the canonical reference for any conformant reader or writer — not
just this library.

> **Status:** Format spec v1 is draft; `TSVZ.py` 4.0 is the reference
> implementation (see *Known deviations* below). TSVZ 3.39 is `TSVZ_old.py`.

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

### `WalStore(path, …)`

An `OrderedDict` subclass that auto-syncs to `path` with a background append
worker. Selected options: `header`, `create`, `delimiter`, `defaults`,
`flush_interval`, `write_ack` (`'memory'` or `'disk'`), `multipart`.

`#` keys are ordinary data, persisted as `<#>key`.

### `OffsetStore(path, …)`

A `MutableMapping` that stores only a key→offset index and seeks for each read.
Uncompressed single-part only; rejects `.gz` / `.bz2` / `.xz` / `.zst`.

### Legacy wrappers

`TSVZed` → `WalStore`; `TSVZedLite` → `OffsetStore`.
`readTabularFile`, `appendTabularFile`, `appendLinesTabularFile`,
`clearTabularFile`, `scrubTabularFile`, `get_delimiter`, plus `readTSV` /
`appendTSV` / `clearTSV` / `scrubTSV`. All warn; removal target 5.0.

---

## Known deviations (4.0 vs [tsvz-spec-v1.md](tsvz-spec-v1.md))

Documented in the `TSVZ.py` module docstring. The important ones:

- `snapshot_part(header=)` re-emits a `#` header comment (§19.2.3c says no comments).
- Boolean markers accept `yes`/`no`/`on`/`off`/`1`/`0` on read; writers emit `true`/`false`.
- No `<CR>` escape token.
- Loose extensions write the same bytes as strict ones, but warn once if a write
  actually emits an escape token or marker.
- Invalid UTF-8 is replaced (and warned) unless `errors='strict'`.
- `#_rotate_# delete` is downgraded to `rename` unless `allow_delete=True`.
- `.zst` needs Python 3.14 `compression.zstd`; it never falls back to plaintext.

## Tests

```bash
python -m pytest TSVZ_new_spec_tests.py  # spec v1 / TSVZ 4.0
python TSVZ.py --doctest
python -m pytest test_TSVZ.py            # frozen 3.39 (TSVZ_old)
python benchTSVZ.py data.tsvz -n 100000  # throughput benchmark (not a test)
```

## License

GPL-3.0-or-later © Yufei Pan (pan@zopyr.us)
