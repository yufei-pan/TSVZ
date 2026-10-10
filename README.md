# TSVZ

**[tsvz.org](https://tsvz.org)**: the format, its specification and its implementations.

TSVZ is a small, dependency-free library and CLI that keeps an **ordered
key-value store in a delimiter-separated text file**. The first column of each
row is the key; the file behaves like an ordered dictionary that is persisted to
disk as you change it.

**Version 4.2**, Python **3.6+**, one self-contained module (`TSVZ.py`): drop it
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
needs Python 3.14.

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
compressed part (read up to the damage; repaired in place before the next
append, with the original saved as `<part>.damaged-<time>`), an unreadable
part, and a delimiter or encoding argument that conflicts with the extension.

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
| C6 | A `clear` or `scrub` that writes nothing (one part of a multi-part store, a store that kept changing) exits 1. |
| C7 | `-c/--header`, `--defaults`, `-s/--strict` and `-f/--force` are undocumented aliases of the `--x-` options. `--x-header` and `--x-defaults` keep non-ASCII text. |
| C8 | Usage errors print tsvz's own message, exit 2; the `-h` text is new. |
| C9 | Arguments after STORE for `read`, `clear` and `scrub` are a usage error, exit 2 (3.39 ignored them, so `clear junk` cleared the file). |

## Spec deviations and interpretations

- §19.2.3c: scrub keeps the header comment (3.39 header verification needs it).
- §17.5: a numbered part opened directly is read alone, with a warning.
- §4.1: a leading byte order mark is tolerated (stripped, with a warning).
- A leading byte order mark is not part of any `#_checksum_*_#` segment.
- §12.4.1: boolean marker values also accept `yes/no/on/off/1/0`; TSVZ writes `true`/`false`.
- §14.5: the defaults-on-missing read applies to `t[key]` only.
- §18.6: locking is the 3.39 exclusive `lockf` per write plus an in-process lock; readers do not lock.
- §19.4: a scrub whose rows would read differently under the final markers writes them with stripping and fill-empty off, then restores the final markers after the rows.
- Invalid UTF-8 is replaced with U+FFFD (and reported).
- v1 has no `<CR>` token, so a value whose last cell ends in `\r` loses it; values set through `TSVZed` are right-stripped, so this only affects rows passed raw to the append helpers.
- §20.7: `-q` also silences informational messages ("Created …"); only errors remain.
- §20.2: `scrub` of a multi-part store, and `clear` of one part of one, are refused with exit 1; `read`, `get` and `parts` of a numbered part read that part alone, with a warning.
- §20.2.4: on `.tsv`-family files `get` prints nothing for a missing key, `verify` checks nothing, and `parts` lists the file.
- §20.3: a bulk input line with an empty key is skipped, with a warning.
- §20.6: a write whose store cannot be created, and a `read`, `get` or `verify` that cannot read every part, exit 1 with an error that `-q` keeps.
- §21: a served `.tsv` file with a header line shows that line as a row, as `tsvz read` does (`TSVZed` hides it when given the header).
- §21.8: the server notices changes with `stat()`. It re-reads the part list while the store's directory changed less than 2 s ago, and reloads a part whose size stayed the same but whose inode or modification time changed. A rewrite in place that keeps a part's size and modification time is not seen until the part changes again.
- §21.9: `tsvz` commands and `TSVZClient` ask the server to answer writes once they are written (`--x-written`), so a write the server cannot make fails as it would on the files. While writes fail, every answer carries a warning.
- §17.7: the server appends to the store's current part; it does not start a new part when it starts.
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

## Tests

```bash
python3 -m pytest TSVZ_test.py -q                         # everything, incl. 3.39 differential tests
python3 -m pytest website/tsvz_site_test.py -q            # the tsvz.org site
python3 benchTSVZ.py /tmp/b.tsv -n 100000 --compare-old    # benchmark (not a test)
```

`TSVZ_old.py` is the frozen 3.39 module the differential tests compare against;
it is not installed.

## License

GPL-3.0-or-later © Yufei Pan (pan@zopyr.us)
