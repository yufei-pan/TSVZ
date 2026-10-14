# TSVZ

**[tsvz.org](https://tsvz.org)**: the format, its specification and its implementations.

TSVZ is a small, dependency-free library and CLI that keeps an **ordered
key-value store in a delimiter-separated text file**. The first column of each
row is the key; the file behaves like an ordered dictionary that is persisted to
disk as you change it.

**Version 4.7**, Python **3.6+**, one self-contained module (`TSVZ.py`): drop it
next to your code and `import TSVZ`, or install it.

```bash
pip install tsvz          # from PyPI
pip install -e .          # editable, from this directory
```

## Two file dialects, one API

| Extension | Rules |
|---|---|
| `.tsv` `.csv` `.nsv` `.psv` (and any other) | The TSVZ **3.39** format, unchanged. A 3.39 host can read every file 4.x writes. |
| `.tsvz` `.csvz` `.nsvz` `.psvz` | The **[tsvz-spec-v1](tsvz-spec-v1.md)** append-only key-value log. |

The delimiter follows the extension: tab, comma, NUL, pipe. A trailing
compression suffix (`.gz`, `.bz2`, `.xz`, `.zst`) is allowed on both; `.zst`
needs Python 3.14. For important or sensitive data, keep the part you write to
uncompressed and compress parts once they no longer change: a damaged
compressed member can lose every record in it (spec §16.4.2).

`TSVZed`, `TSVZedLite`, `readTabularFile`, `appendTabularFile`,
`appendLinesTabularFile`, `clearTabularFile`, `scrubTabularFile` (and the
`*TSV` aliases) work on both dialects. `t.dialect` tells you which one a store
uses.

## Quick start

```python
import TSVZ

t = TSVZ.TSVZed('people.tsvz', header='id\tname\tscore')
t['alice'] = ['alice', 'Alice', '10']
t['bob'] = 'bob\tBob\t20'
t['alice'] = ['alice', 'Alice', '11']   # last write wins
del t['bob']                            # appends a tombstone
print(t['alice'])                       # ['alice', 'Alice', '11']
t.close()
```

```bash
tsvz set people.tsvz carol Carol 5        # add or update a row (append is the same)
tsvz get people.tsvz carol                # one row; exit 3 if carol is missing
tsvz read people.tsvz                     # every row: a table on a terminal, records in a pipe
tsvz delete people.tsvz carol             # tombstone
tsvz read people.tsvz | tsvz set copy.tsvz -   # copy every row
tsvz clear people.tsvz                    # empty it, keeping the header and markers
tsvz scrub people.tsvz                    # compact it (for .tsvz: archival maintenance)
tsvz verify people.tsvz                   # check #_checksum_*_# segments; exit 4 on a mismatch
tsvz parts events.tsvz                    # list the parts of a multi-part store
tsvz has people.tsvz carol                # exit 0 if carol is live, 3 if not
tsvz pop people.tsvz carol                # print carol's row and delete it
tsvz serve people.tsvz &                  # keep the store loaded; tsvz commands then use it
tsvz stop people.tsvz
tsvz people.tsvz append carol Carol 5     # the 3.39 form still works
tsvz -V
```

## What TSVZ 4 does with `.tsvz` files

- **Reads** everything a conformant writer can produce: markers
  (`#_defaults_#`, `#_strip_trailing_whites_#`, `#_fill_empty_with_default_#`,
  `#_return_defaults_when_missing_#`, ...), escapes (`<sep>`, `<LF>`, `<lt>`,
  `<#>`), tombstones, `#_checksum_<algo>_#` segments (crc32 and every
  fixed-length hashlib algorithm), compressed parts, and **multi-part stores**
  (`store.tsvz.<hex>`, part 0 first, `.rotated` parts skipped).
- **Writes** one file: the named file, or the highest-numbered part of a
  multi-part store. Rows are written as given (no padding); a lone key is a
  tombstone; a row of empty cells (`k\t\t`) is a live row.
- **Compaction is manual.** `scrubTabularFile` / `tsvz scrub f.tsvz` rewrites a
  single-part store in place (same inode). If another writer commits while it
  runs, it reads the store again; after three tries it writes nothing, with a
  warning (`clearTabularFile` does the same). The automatic rewrite options of
  3.39 (`rewrite_on_load`, `rewrite_on_exit`, `rewrite_interval`, `rewrite()`,
  `mapToFile()`, `hardMapToFile()`) are ignored for `.tsvz`, with a warning.
- **Missing keys:** `t[key]` returns the defaults row while
  `#_return_defaults_when_missing_#` is true (its default); `in`, `get`,
  `setdefault` and `pop` behave like a dict.
- **`#` keys** are stored (`<#>key`). Keys of the form `#_name_#` are marker
  writes: `t['#_defaults_#'] = [...]` sets the defaults, and so does
  `setDefaults([...])` once the object is constructed.

## Command line

`tsvz` follows [tsvz-spec-v1 §20](tsvz-spec-v1.md#20-command-line-interface):
`tsvz [OPTION ...] OPERATION STORE [ARG ...]`, with options anywhere before a
lone `--`. The 3.39 form `tsvz STORE [OPERATION] [ARG ...]` works too; a store
named like an operation then needs a path prefix (`./read`).

- **stdout carries only rows.** It gets a table on a terminal and records
  otherwise; `--format table|records` overrides. Records use the store's
  delimiter and escaping, so `tsvz read A | tsvz set B -` copies a store of
  the same variant.
- **stderr carries every message.** `-q` prints errors only; `-v` adds detail.
- **Exit status:**
  - 0: done.
  - 1: failed or refused. Also a missing store for `read`, `get`, `scrub`, `verify` and `parts`,
    a store that `set`, `append` or `delete` could not create, and a `.tsvz` part that could not
    be read (the rows of the other parts are still printed).
  - 2: usage error.
  - 3: `get` found a missing key.
  - 4: `verify` found a checksum mismatch.
- **Arguments are literal.** `tsvz set s.tsvz k 'a<b'` stores `a<b`. Values may
  begin with `-` (`-5`); after `--` every argument is positional. `set KEY`
  alone deletes KEY, as in 3.39. `set KEY ''` stores an empty cell in a `.tsvz`
  store; in a `.tsv` file it deletes KEY, as in 3.39.
- **Bulk input.** `set STORE -` and `delete STORE -` read records or keys from
  stdin and append them in one write. An unterminated last line is ignored,
  with a warning.
- **TSVZ extensions:**
  - `--x-header H` and `--x-defaults KEY<d>D1<d>...` decode backslash escapes such as `\t`;
  - `--x-strict` / `--x-force` turn 3.39's column and header checks on or off.

## Serving a store

`tsvz serve STORE` keeps a store loaded in one process and answers requests on a
local socket ([tsvz-spec-v1 §21](tsvz-spec-v1.md#21-write-handler-protocol)).
`tsvz` commands and `TSVZ.TSVZClient` find it through the pointer file
`STORE.serve` and use it instead of re-reading the store. Their output and exit
status stay the same.

- **One appender.** The server batches the writes it receives into single appends.
  Other writers may still append to the files; the server reads what they add
  before it answers.
- **Acknowledgement.** A write is acknowledged once it is queued (`#_write_ack_#`
  `memory`, the default), or after `fsync` (`#_write_ack_#` `disk`, or
  `TSVZClient(..., sync=True)`). Reads always see acknowledged writes.
- **Access.** Only the user who started the server can connect (socket mode 0600).
  `--x-group G` or `--x-mode 0660` opens it on purpose. The server listens on a
  Unix-domain socket only; reaching it from other hosts is left to a separate
  adapter (for example TCP with TLS) that would relay to the socket.
- **Trust.** Clients use `STORE.serve` only when it is a regular file owned by
  them or by the store's owner, not writable by anyone else, naming a socket that
  belongs to that owner. Otherwise they warn and use the
  files. `serve` never writes through a symbolic link or into a file it did not
  create.
- **Lifetime.** It runs in the foreground until `tsvz stop STORE`, SIGINT or
  SIGTERM, or `--x-idle-timeout S` seconds without connections. It then writes
  and `fsync`s everything and removes `STORE.serve`.
- **Bypassing it.** `--x-direct` makes a `tsvz` command use the files. Command
  lines with `--x-header`, `--x-defaults` or `--x-strict`, or another delimiter
  than the server's, use the files too.

```python
import TSVZ

with TSVZ.TSVZClient('people.tsvz') as people:  # served, or in this process when no server runs
    people['dave'] = ['dave', 'Dave', '41']
    print(people['dave'], len(people), 'alice' in people)
    print(people.pop('dave'))
```

`TSVZClient` has `TSVZed`'s semantics for reading and setting keys, `del`, `in`,
`len`, iteration, `pop`, `popitem`, `setdefault`, `update`, `clear` and
`setDefaults`. Its writes return once they are written (raising `OSError` if they
cannot be), one client may be shared between threads, and without a server it
warns once and works on the files in-process, finishing its writes when the
program exits.

## Fault tolerance

TSVZ keeps your data and keeps going when a file is not what it expects, and
says so on stderr (or through `teeLogger`) with one summarised line per problem:

```
TSVZ warning: data.tsvz: invalid UTF-8 replaced with U+FFFD (3 occurrences, first at line 17)
```

Examples: bytes after the last newline (ignored on read; truncated before the
next append, and quoted), invalid UTF-8, a byte order mark, a newer
`#_version_#`, a marker with a bad value, a checksum mismatch, a damaged
compressed member or a zero-filled line (a member cut short by a crash keeps
the whole lines it decoded; a corrupt member or a zero-filled line is
recovered, skipped or stops the read, as the store's `#_on_corruption_#` says,
`recover` by default, spec §4.9, §4.10, §16.6; a scrub that rewrites such a
part keeps the original as `<part>.damaged-<time>`), an unreadable part, and a
delimiter or encoding argument that conflicts with the extension.

## Breaking changes from 3.39

### `.tsv`, `.csv`, `.nsv`, `.psv`: bug fixes only

| # | Change |
|---|---|
| L1 | Appending after an unterminated last line writes `\n` first; 3.39 merged the two lines. |
| L2 | Setting a row whose values are all empty deletes it in memory too (it already meant delete on disk), with a warning. |
| L3 | `TSVZedLite` on a compressed file keeps rows in memory; 3.39 read compressed bytes as text. Rows held in memory (also `#` keys) are returned directly. |
| L4 | New warnings on stderr (invalid encoding, lines dropped by `strict`); data unchanged. |
| L5 | A damaged compressed file is read up to the damage instead of raising. |
| L6 | `mapToFile` no longer glues a row it adds at the end of the file to the next one. |
| L7 | `TSVZed`'s `in` and `get` find a key as `t[key]` does: the live key equal to it, else the key right-stripped. 3.39 matched them exactly, so `'x ' in t` was false while `t['x ']` read `x`, and `t.setdefault('x ', …)` overwrote `x`. |

### `.tsvz`, `.csvz`, `.nsvz`, `.psvz`: now tsvz-spec-v1

| # | Change |
|---|---|
| S1 | `.csvz/.nsvz/.psvz` use `, \0 \|` (3.39: tab). A conflicting `delimiter=` or `encoding=` is overridden, with a warning. |
| S2 | A file written by 3.39 under one of these names reads differently (header line becomes data, `key\t\t` deletes come back, `</sep/>` stays literal, an unterminated last line is dropped, single-column files read as empty, empty cells are not defaulted, NULs are not stripped). **Rename such a file to `.tsv`.** |
| S3 | `t[missing]` returns the defaults row instead of raising `KeyError` (see above). |
| S4 | `#` keys are stored; `#_name_#` keys are marker writes. |
| S5 | Automatic rewrites are ignored with a warning; `rewrite_on_load` effectively defaults to `False`; `move_to_end` only reorders memory. |
| S6 | Rows are never trimmed or dropped for their width; `strict` no longer drops rows; `appendLinesTabularFile` does not pad. |
| S7 | The header is written as a `#` comment; a new file without a header is empty. |
| S8 | An unterminated tail is ignored on read and truncated on append. |
| S9 | `clear` keeps the active markers; scrub keeps the header comment and drops other comments, custom markers and checksums. |
| S10 | Files named `x.tsvz.<hex>` beside `x.tsvz` are read as parts of the store. |
| S11 | `lastLineOnly` reads forward (slower on large files). |
| S12 | `TSVZedLite.clear()` and `clear_file()` raise `OSError` when the clear is refused (a part could not be read, the path names one part of a store, or the store kept changing). Memory is reloaded from the unchanged store first. |
| S13 | Compressed parts are written in members of at most 1 MiB of records (`#_member_size_#`), Zstandard frames with a checksum; an append never reads or repairs the part. |
| S14 | A zero byte in a value is written `<NUL>`, and a raw zero byte in a record is read as zero-fill damage (spec §4.9): under the default `recover` only the bytes after it are read. **A value that 4.2 or 4.3 wrote with a raw zero byte therefore reads differently in 4.4.** Such values are rare; before upgrading, `grep -c -P '\x00' store.tsvz` (any `.tsvz`, `.csvz` or `.psvz` part) finds them. Stop 4.2 and 4.3 handlers before upgrading: they read `<NUL>` as text, as they read `<CR>`. |
| S15 | A lookup (`t[key]`, `in`, `get`, `del`, `pop`, `setdefault`) takes the live key equal to the one given before the right-stripped one, so a key ending in whitespace (written while `#_strip_trailing_whites_#` was false) can be read and deleted by name (spec §20.2.7). Setting a key still right-strips it. |

### The `tsvz` command

3.39 command lines still parse, including abbreviated long options (`--delim`)
and `-d=,`, except the ones C5 and C9 list.

| # | Change |
|---|---|
| C1 | `read` into a pipe or a file prints records instead of the table; `--format table` gives the table. |
| C2 | Every message goes to stderr: "Created …", "File not found", header mismatches, warnings. |
| C3 | `read` and `scrub` of a missing store exit 1 with a message (3.39: exit 0, and `read` printed "File not found" on stdout). |
| C4 | `delete` deletes every KEY it is given (3.39: only the first). |
| C5 | `append` or `delete` without a KEY, or with an empty KEY, is a usage error with exit 2 (3.39 wrote an empty line). |
| C6 | A `clear` or `scrub` that writes nothing (one part of a multi-part store, a store that kept changing, a part that declares a spec version newer than 1, per spec §19.4 and §19.8) exits 1. |
| C7 | `-c/--header`, `--defaults`, `-s/--strict` and `-f/--force` are undocumented aliases of the `--x-` options. `--x-header` and `--x-defaults` keep non-ASCII text. |
| C8 | Usage errors print tsvz's own message, exit 2; the `-h` text is new. |
| C9 | Arguments after STORE for `read`, `clear` and `scrub` are a usage error, exit 2 (3.39 ignored them, so `clear junk` cleared the file). |
| C10 | The table (a terminal, or `--format table`) of `get`, `keys`, `pop`, `popitem`, `setdefault`, and of `read` on a `.tsvz`-family store, shows the rows only: no row is drawn as a header, and no row is cut to the first row's or the terminal's width. `read` of a `.tsv`-family file keeps 3.39's table, whose first row is the file's header line. |
| C11 | The table shows control characters, bidi controls and line separators as escapes (`\r`, `\n`, `\t`, `\xNN`, `\uNNNN`), so no value can move the cursor, reorder the line or restyle the terminal. It sizes columns in terminal columns, so CJK text (two columns a character) lines up. The table is for reading: a backslash in a value is shown as is, so use records for exact values. |
| C12 | While stripping is on, `get`, `has`, `pop`, `delete` and `setdefault` take the live key equal to a KEY ending in a space or tab before the stripped one (spec §20.2.7), so `tsvz keys s \| tsvz delete s -` deletes every key. `delete` of such a KEY reads the store first, and exits 1 without writing when the store cannot be read whole. |

## Spec deviations and interpretations

None of these fails a conformance case: `conformance/reference-baseline.json`,
which lists the cases this implementation does not pass, is empty (see
Conformance testing).

- §17.5: a numbered part opened directly is read alone, with a warning.
- §14.5: the defaults-on-missing read applies to `t[key]` only.
- §18.6: locking is one exclusive whole-file lock per write plus an in-process lock; readers do not lock. On Linux it is an open file description lock, which still excludes 3.39's `lockf`; elsewhere it is `lockf`.
- §19.4: a scrub whose rows would read differently under the final markers writes them with stripping and fill-empty off, then restores the final markers after the rows.- §20.7: `-q` also silences informational messages ("Created …"); only errors remain.
- §20.2: `scrub` of a multi-part store, and `clear` of one part of one, are refused with exit 1; `read`, `get` and `parts` of a numbered part read that part alone, with a warning.
- §20.2.4: on `.tsv`-family files `get` prints nothing for a missing key, `verify` checks nothing, and `parts` lists the file.
- §20.3: a bulk input line with an empty key is skipped, with a warning.
- §20.6: a write whose store cannot be created, and a `read`, `get` or `verify` that cannot read every part, exit 1 with an error that `-q` keeps.
- §21: a served `.tsv` file with a header line shows that line as a row, as `tsvz read` does (`TSVZed` hides it when given the header).
- §21.8: the server notices changes with `stat()`. It re-reads the part list while the store's directory changed less than 2 s ago, and reloads a part whose size stayed the same but whose inode or modification time changed. A rewrite in place that keeps a part's size and modification time is not seen until the part changes again.
- §21.9: `tsvz` commands and `TSVZClient` ask the server to answer writes once they are written (`--x-written`), so a write the server cannot make fails as it would on the files. While writes fail, every answer carries a warning.
- §21.3: `tsvz serve` needs Unix-domain sockets. Where Python has none (Windows), it exits 1, and `tsvz` commands and `TSVZClient` work on the files.
- §20.7: on `.tsv`-family files a `-d` longer than one character is used, as in 3.39, with a warning; a `-d` that does not decode is a usage error.

## Website

`website/` holds the tsvz.org site. `website/tsvz_site.py` (standard library
only, Python 3.8+) renders `website/index.md`, `website/spec-glance.md` and
`tsvz-spec-v1.md`, and serves HTML to browsers and markdown to agents.

```bash
python3 website/tsvz_site.py serve -p 8765     # http://127.0.0.1:8765/
python3 website/tsvz_site.py build /tmp/site   # static files for any host
```

`deploy/tsvz-site.service` runs it under systemd; restart it to publish an edit.

## Conformance testing

`tsvzconform.py` tests how closely an implementation of tsvz-spec-v1, in any
language, follows the spec. Its cases are JSON files in `conformance/cases/`,
each naming the § clauses it tests, and it drives an implementation only
through the spec's command line (§20) and handler socket (§21).

```bash
python3 tsvzconform.py --impl ./tsvz --feature crc32,sha256,handler
python3 tsvzconform.py --impl ./tsvz --class reader --only 13
```

Each failure is printed with its input, what was expected, what came back and
a command line that reruns it alone; the summary gives a verdict for each
conformance class (reader, writer, cli, handler). `conformance/README.md`
explains the options and the features to claim, and how to load the cases into
another language's tests.

## Benchmarking implementations

`tsvzbench.py` compares implementations of tsvz-spec-v1, in any language, side
by side on one machine. It drives each one only through the spec's command
line (§20) and, when it has one, its handler socket (§21), so any conformant
`tsvz` tool can be measured as it is.

```bash
python3 tsvzbench.py --impl ref='python3 TSVZ.py' --impl new='../tsvz-rs/target/release/tsvz' \
                     --suite quick --work-dir /mnt/fast-disk/tsvzbench
```

- `--impl NAME=COMMAND`, repeatable: the first is the baseline, and every other
  implementation gets a `×` ratio against it (below 1 is faster). A relative
  COMMAND runs from the directory you start tsvzbench in.
- `--suite smoke|quick|standard|large`: `smoke` checks a setup in seconds,
  `quick` takes about a minute, `standard` (the default, up to 1M rows) about
  five, and `large` (up to 10M rows) twenty or more and ~5 GB of memory for the
  harness itself.
- `--only cold,bulk,perop,maint` picks families: one-shot replays, `set -` and
  `delete -` batches, single operations (CLI and handler), and scrub/clear.
- `--work-dir DIR` keeps the generated fixtures between runs. Put it on the disk
  you mean to measure: not a tmpfs, and not a FUSE file system such as mergerfs
  (the harness warns about those). The default is a `tsvzbench-*` directory here.
- `--json PATH` also writes every raw sample.

The harness writes its own input stores, so every implementation reads the same
bytes, and checks every result against its own replay model: `✓` right, `✗`
wrong output or files, `! N` an unexpected exit status, `⏱` timed out, `refused`
an allowed refusal. It exits 1 when any cell failed. Each command runs under a
small launcher that measures it with `wait4`; a peak RSS shown as `≤8.0 MiB` is
the launcher's own size, so the true peak is no higher. Compare an
implementation with itself to see the noise: a few percent per cell, but the
socket rows, whose latencies are tens of microseconds, can move 10–20% between
runs, because a single run can land in a slower mode as a whole.

## Tests

```bash
python3 -m pytest TSVZ_test.py -q                         # everything, incl. 3.39 differential tests
python3 -m pytest website/tsvz_site_test.py -q            # the tsvz.org site
python3 -m pytest tsvzbench_test.py -q                    # the benchmark harness
python3 -m pytest tsvzconform_test.py -q                  # the conformance runner and cases, against the reference
python3 benchTSVZ.py /tmp/b.tsv -n 100000 --compare-old    # benchmark (not a test)
```

`TSVZ_old.py` is the frozen 3.39 module the differential tests compare against;
it is not installed.

## License

GPL-3.0-or-later © Yufei Pan (pan@zopyr.us)
