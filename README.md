# TSVZ

TSVZ is a tiny, dependency-free library and CLI for treating a **tab-separated
values file as a key-value store**. The first column of every row is a unique
key; the file behaves like an ordered dictionary that is transparently
persisted to disk.

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
tsvz data.tsv scrub                    # rewrite compactly (drops comments, applies last-wins)

tsvz data.csv -d comma append k v1 v2  # pick a delimiter explicitly
tsvz -h
```

---

## The TSVZ file format

> **Status: draft / evolving spec.** The reference code in this repo does not
> fully implement it yet (see *Implementation notes*).

`.tsvz` / `.csvz` / `.nsvz` / `.psvz` are a **format in their own right** — not
merely a `.tsv` that TSVZ happens to manage. They define a **strict,
append-only, write-ahead-log (WAL) style key-value store** that is *also*
readable by ordinary CSV/TSV tools (which see the rows fine but may
misinterpret escaped values; see *Escaping*).

The plain, un-suffixed `.tsv` / `.csv` / `.nsv` / `.psv` extensions are **not**
this format: they remain loose delimited tables with no semantic guarantees. The
same delimiter mapping carries over to both; only the `z` forms carry the
stricter contract below.

The format is defined by the rules below; they are written to be implementable by
any reader/writer, not just this library.

### 0. Model — the file *is* a write-ahead log

- **Every mutation is an append.** There is no in-place update during normal
  operation. Current state is reconstructed by **replaying the file forward**,
  **last-wins**.
- **Key-value, not relational.** First column is the globally-unique key; the
  remaining columns are the value (variable width; a virtual infinite tail of
  empty columns; a defaults line as per-column fallback). No schema, no header
  semantics beyond "a `#` comment."
- **Delete is a tombstone** (a lone key / all-empty values), never a physical
  removal — consistent with a log.
- **Minimal overhead vs. an embedded DB.** Plain UTF-8 text, line-oriented,
  human-readable and greppable; no page cache, b-tree, or separate journal. You
  trade random-update efficiency and bounded file size for near-zero write
  overhead and trivial crash recovery (truncate the last partial line). Fits
  logging / event streams / transactional state where overwrites are rare, write
  latency matters, or the full history is worth keeping.
- **Compaction is occasional maintenance, not normal operation** — the one place
  the append-only rule is deliberately, explicitly broken. See *Compaction*.

### 1. Encoding & lines

1. The file is **UTF-8** encoded (no BOM).
2. Records are separated by a line feed (`\n`). A reader **must** tolerate a
   trailing `\r` (i.e. read CRLF files); writers **should** emit bare `\n`.
3. The file **should** end with a trailing newline. A reader **must** tolerate a
   missing final newline.
4. An empty file (zero bytes) — or a file containing only comments — is a valid,
   empty TSVZ.

### 2. Delimiter & variants

5. The variants differ only in the field delimiter. The `z` extension marks the
   strict WAL format; the plain extension is the loose tabular fallback:

   | Variant | Format ext. | Plain ext. | Delimiter |
   |---------|-------------|------------|-----------|
   | TSVZ    | `.tsvz`     | `.tsv`     | tab `\t`  |
   | CSVZ    | `.csvz`     | `.csv`     | comma `,` |
   | NSVZ    | `.nsvz`     | `.nsv`     | NUL `\0`  |
   | PSVZ    | `.psvz`     | `.psv`     | pipe `\|` |

6. Each line is a list of fields joined by the delimiter. **Column count is not
   fixed** — a reader must accept arbitrarily short or long lines. Conceptually
   every row is its values followed by an infinite tail of empty-string columns,
   so reading a field beyond a row's length yields `''` (subject to the defaults
   rule below). A returned row is therefore **not guaranteed to be a fixed
   length**; a reader is free to additionally validate or pad/trim if it wants.

### 3. Keys & values

7. The **first column is the key** and is **globally unique** within the file.
8. **Last-wins:** when the same key appears on multiple lines, the **last**
   occurrence determines the current value. (This is what makes append-only
   updates work.)
9. A key's **logical position** is the position of its *first* appearance; later
   updates change its value but not its order.
10. **Append-only:** during normal operation a TSVZ file is only ever appended
    to — an update or a delete is a new line at the end of the file, never an
    in-place edit. (Compaction is a separate, explicit operation; see *scrub*.)

### 4. Deletion (tombstones)

11. A line consisting of **a lone key with no following values, or whose
    following values are all empty**, is a **tombstone**: it removes that key if
    it currently exists, and is tolerated (no-op) if it does not.
    - e.g. `mykey\n` or `mykey\t\t\n` both delete `mykey`.
    - Edge case: in a strictly single-column file a lone key *is* the value, so
      deletion cannot be represented; such files cannot tombstone.

### 5. Whitespace

12. **Trailing whitespace is stripped from every field on read** (including the
    key) — trailing whitespace is treated as visual padding and is not
    significant. Leading whitespace **is** significant and preserved. (This does
    not apply to comment lines, which are not parsed.)

### 6. Comments & header

13. Any line whose first field starts with `#` is a **comment** and is discarded
    by the reader (with the exception of the reserved `#_..._#` keys below).
14. There is **no formal header**. A header row, if present, is simply a comment
    (a line starting with `#`, e.g. `#id\tname\tscore`) and carries no semantics
    for the data — it is purely human documentation.

### 7. Defaults line

15. A line whose key is **`#_defaults_#`** (case-insensitive — `#_DEFAULTS_#` is
    equally valid) is a **defaults line**. It is not data; it supplies fallback
    values per column index.
16. Defaults have an **unbounded** number of columns and are applied **forward
    only**: as a reader streams the file, a field that is empty (or absent) in a
    data row takes the value at the same column index from the **currently
    active** defaults line.
    - Rows that appear *before* a defaults line are unaffected by it (the reader
      does not backtrack).
    - Example: rows have 5 fields and there is no defaults line ⇒ reading field 6
      yields `''`. Then a defaults line with 10 fields appears ⇒ for *subsequent*
      5-field rows, reading field 6 yields the defaults line's field 6.
17. **Multiple defaults lines are allowed.** A later defaults line **completely
    replaces** the previous one — if the later line is shorter, the extra columns
    of the previous line are discarded, not merged.
18. A `#_defaults_#` line with **no values** clears the active defaults.

### 8. Reserved namespace

19. Keys matching **`#_<ascii letters/digits>_#`** are **reserved** for current
    and future internal TSVZ semantics (`#_defaults_#` is the first). Don't use
    that pattern for your own comments. (Conformance is only enforced where a
    given reserved key is actually defined; unknown `#_..._#` keys are treated as
    ordinary comments today.)

### 9. Escaping / sanitization

The delimiter and newline can't appear literally inside a field, so they are
encoded with reversible tokens. Reading inverts writing:

| In a field (in memory) | Stored on disk | Read back as |
|---|---|---|
| the delimiter char (e.g. `\t`) | `<sep>` | the delimiter char |
| line feed `\n` | `<LF>` | line feed `\n` |
| literal `<sep>` | `</sep/>` | literal `<sep>` |
| literal `<LF>` | `</LF/>` | literal `<LF>` |

20. Consequently `<sep>` / `<LF>` appearing **in the file** are interpreted as a
    delimiter / line feed on read. The double-wrapped forms `</sep/>` / `</LF/>`
    exist so that the literal strings `<sep>` / `<LF>` (e.g. HTML-ish tags) can
    round-trip.
21. To keep the mapping finite, the literal strings **`</sep/>` and `</LF/>` are
    invalid as field data**: if they occur in a file they are read back as the
    literal `<sep>` / `<LF>` (i.e. they have no escape of their own and are
    *lossy* — they collapse one level). Writers should avoid emitting them as
    real data.

### 10. Compression

22. Compression is an **orthogonal stream filter**, not part of the format
    semantics: any TSVZ file (or any single part of a multi-part set, below) may
    be wrapped by appending a compression extension — `.gz`/`.gzip`,
    `.bz2`/`.bzip2`, `.xz`/`.lzma`, `.zst`/`.zstd` (e.g. `log.tsvz.zst`).
    Delimiter/format inference ignores the compression suffix. A compressed part
    is rewritten as a whole, so the append-only optimization applies only to
    uncompressed parts.

### 11. Multi-part files

23. A logical TSVZ store may be **split across multiple parts**. Parts share the
    same path up to the format extension and add a `.<hex>` ordinal suffix, e.g.
    `events.tsvz.0`, `events.tsvz.1`, … or with timestamps
    `events.tsvz.6650a3c0`, `events.tsvz.66518f10`, ….
24. Parts are ordered by interpreting the suffix as a **hexadecimal (and hence
    also plain-integer) number, ascending** — smaller = earlier in the log. The
    logical store is the concatenation of the parts in that order; replay forward
    / last-wins simply spans part boundaries.
25. **Each part is itself a complete, valid TSVZ file** (comments, defaults, etc.
    all legal per part). Multi-part is purely an addressing convention layered on
    top; a single-file store is just the one-part case.
26. **Recommended suffix:** the hex (or integer) Unix timestamp of when the part
    was created — naturally chronological and collision-resistant.

Operational recommendations for multi-part writers:

- On startup, **open a fresh part** named with the current timestamp and append
  new records there; leave older parts untouched (they become immutable).
- **Do not compress the current (active) part** — it is being appended to.
- Provide a processor/flag that, on the next launch, **compresses prior parts**
  that aren't already compressed (now immutable, so it's safe and frees space).
  Different parts may use different codecs.

### Additional ideas worth considering (not yet normative)

These came up while writing the spec; flagging them for your call:

- **Format-version marker.** Reserve a `#_tsvz1_#`-style first-line comment so
  future readers can detect the format/version explicitly. Cheap insurance.
- **Key constraints.** State explicitly that a *data* key must be non-empty
  (after trailing-whitespace strip) and must not start with `#` (that prefix is
  reserved for comments/internal keys).
- **Append atomicity.** Recommend that a conformant appender write each record
  as a *single* `write()` of a complete, newline-terminated line (open with
  `O_APPEND`) so concurrent appenders never interleave partial lines. This is
  what makes multi-writer last-wins safe without a global rewrite.
- **Compaction definition.** Define "scrub/compact" formally: produce an
  equivalent file containing, for each live key, exactly one line in
  first-appearance order, dropping comments and tombstones, optionally
  re-emitting a single `#_defaults_#` line. (This is lossy w.r.t. comments — say
  so.)
- **NUL caveat.** Note that NSVZ (`\0` delimiter) is not friendly to most text
  tools; it's meant for programmatic use.

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

## Implementation notes (reference behavior vs. the format spec)

The format above is the target definition. A few places where the current
`TSVZed` / `TSVZedLite` implementation differs or only partially implements it —
useful to know, and candidates for a future alignment pass:

- **Column normalization.** Rather than treating rows as variable-length with an
  infinite empty tail (rule 6), the classes detect a column count (from the
  header or first row) and, in non-strict mode, pad/trim rows to it; `strict`
  mode rejects mismatches. Reads therefore return fixed-width rows in practice.
- **Defaults application.** Empty cells *present* in a row are filled from the
  active defaults at parse time, but rows are not virtually extended, so "read a
  field beyond the row length" (rule 16) isn't exposed as an API yet.
- **Header.** The reference classes still use a *non*-`#` first line as the
  header (validated via `verifyHeader`), whereas rule 14 defines the header as an
  ordinary `#` comment. Existing files written by older versions use the non-`#`
  header.
- **`#_DEFAULTS_#` alias.** Only the lowercase `#_defaults_#` key is currently
  recognized by the parser.
- **No `z` vs. plain distinction, no multi-part.** `get_delimiter` infers from
  `.tsv`/`.csv`/`.nsv`/`.psv` only, so the `z` extensions aren't recognized for
  delimiter inference yet (`.csvz`/`.nsvz`/`.psvz` currently fall back to tab;
  `.tsvz` happens to land on tab correctly). The code does not yet enforce the
  strict append-only WAL contract differently for the `z` forms, and there is no
  multi-part read/write support. These are spec items, not implemented.

### Open design questions

Raised by the spec clarification, not yet decided:

1. **Where does the strict contract live?** Should `TSVZed`/`TSVZedLite` key the
   append-only WAL behavior off the `z` extension (`.tsv` = lenient table,
   `.tsvz` = this format), or treat all forms the same and let the suffix be
   purely documentary?
2. **Append-only vs. the existing rewrite machinery.** A strict `.tsvz` arguably
   forbids in-place rewrite (`rewrite_on_load` / `mapToFile`) entirely except via
   explicit compaction — which would simplify the class considerably.
3. **Compaction for history-retention users.** If "keep the full log" is a goal,
   compaction must be an explicit, separate action — and perhaps emit a `.tsv`
   snapshot rather than mutating the `.tsvz` log in place.

## Tests

```bash
python -m pytest test_TSVZ.py            # regression suite
python test_TSVZ.py                      # or run directly
python benchTSVZ.py data.tsv -n 100000   # throughput benchmark (not a test)
```

## License

GPL-3.0-or-later © Yufei Pan (pan@zopyr.us)
