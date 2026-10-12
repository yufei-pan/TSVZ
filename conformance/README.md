# TSVZ conformance suite

The cases in `cases/` test how closely an implementation follows
[tsvz-spec-v1](../tsvz-spec-v1.md). Each case names the § clauses it tests and
says in one sentence why its outcome is the right one. `../tsvzconform.py` runs
them against any implementation, in any language, through its command line
(§20) and its handler socket (§21). You can also load the cases straight into
your own test framework.

## Running the suite

```bash
python3 tsvzconform.py --impl ./target/release/tsvz --feature crc32,sha256,handler
python3 tsvzconform.py --impl ./tsvz --class reader --only 13      # §13 reader cases only
python3 tsvzconform.py --list --only 9                             # what would run
```

Python 3.8 or later on Linux or macOS; standard library only. Run it from
the `TSVZ/` directory, or anywhere: it finds `conformance/cases/` beside itself.

| Option | Meaning |
|---|---|
| `--impl COMMAND` | The command that runs your tool, split like a shell would. A relative path resolves from where you start the runner. |
| `--class reader,writer,cli,handler` | The conformance classes to run (default all). Naming `handler` also claims the handler. |
| `--only PATTERNS` | Case ids, clauses (`9.2` matches `9.2-…` and `9.2.1-…`) or globs (`13.*`), comma-separated. |
| `--feature NAMES` | The optional features you implement (below), or `all`. |
| `--mode files,socket,forwarded` | Restrict the modes (below). |
| `--level must` | Skip SHOULD cases. |
| `-j N`, `-x`, `--timeout S` | Parallel cases (default: CPUs), stop at the first failure, limit per step (default 10 s). |
| `--work-dir DIR`, `--json PATH` | Where case directories go; a JSON record of every result. |

### Classes

| Class | What it tests | Operations it calls |
|---|---|---|
| reader | §4–§17: framing, variants, markers, escaping, defaults, integrity, compression, multi-part stores | `read`, `get`, `keys`, `len`, `verify`, `-V` |
| writer | §13.3 encoding, append-only writes, scrub and clear (§19) | the reader's, plus `set`, `set -`, `delete`, `delete -`, `clear`, `scrub` |
| cli | the whole of §20: arguments, every operation, output, exit statuses | everything in §20 |
| handler | §21: the pointer file, the protocol, consistency, `serve` and `stop` | `serve`, `stop`, the socket |

A class is conformant when none of its cases fails; the cli and handler
classes also need reader and writer to be conformant (§2.1).

### Features

Some things the spec lets you leave out look exactly like a bug from outside: a
reader without crc32 must ignore its markers (§15.3). So you claim what you
implement:

| Feature | Meaning |
|---|---|
| `bzip2`, `xz`, `zstd` | Reads parts in that codec (gzip is required, §16.5) |
| `crc32`, `crc32c`, `sha256`, `blake3` | Implements that digest (§15.2.1) |
| `file-first` | Accepts `tsvz STORE [OPERATION]` (§20.1.5) |
| `handler` | Provides `serve` and the socket (§21) |
| `scrub-multipart` | Scrubs a multi-part store instead of refusing (§20.2) |

A case marked `requires: ["x"]` runs only when you claim `x`; one marked
`["!x"]` runs only when you do not, and checks what the spec asks of an
implementation without `x`.

### Modes

Every reader case, and every case made only of plain operations, runs up to
three ways:

- `files`: your command on the files;
- `socket`: requests to your handler serving the store (§21.7.1 says it answers
  every §20 operation the same way);
- `forwarded`: your command while your handler serves the store (§20.8.1).

`socket` and `forwarded` need the `handler` feature, and their results count
toward the handler class.

### Reading the report

Each failure shows the case, its clauses, its note, the input files as JSON
strings (so tabs, `\r` and NUL are visible), the step that failed, what was
expected and what came back, the end of standard error, the directory it ran
in (kept for you), and the command line that reruns it alone:

```
FAIL 9.2-trailing-delimiter-is-not-a-tombstone   MUST §9.2 §14.2
  key<TAB> sets a present-empty cell; only a lone key deletes.
  files   s.tsvz = "a\tx\na\t\n"
  step 1  ./tsvz --format records -q read /tmp/tsvzconform-k3x9/9.2-…/s.tsvz
          row 1 missing: expected ["a"] (got 0 rows, expected 1)
  kept    /tmp/tsvzconform-k3x9/9.2-trailing-delimiter-is-not-a-tombstone
  rerun   python3 tsvzconform.py --impl ./tsvz --only 9.2-trailing-delimiter-is-not-a-tombstone
```

`FAIL` is a MUST that did not hold, `warn` a SHOULD, `skip` a case your claims
or the environment leave out, and `error` a bug in the suite itself. The runner
exits 0 when nothing failed, 1 when something did, 2 for a usage error and 130
after Ctrl-C.

## Testing a library that has no command line

Write a small adapter program. The runner needs only this of it for
`--class reader`:

- `ADAPTER -V` prints one line naming the implementation and exits 0;
- `ADAPTER --format records -q OP STORE [ARG ...]` for `OP` in `read`, `get`,
  `keys`, `len` and `verify`, printing what §20.2 says in the records format of
  §20.4.1 (the store's own delimiter, §13 escaping, one `\n`-terminated line per
  row) and exiting as §20.6 says: 0, 1 when the store is missing or cannot be
  read, 3 when `get` finds a key missing, 4 when `verify` finds a mismatch.

## Loading the cases yourself

Each file in `cases/<class>/` is a JSON object `{"format": 1, "class": ...,
"cases": [...]}`. Strings are exact: write them to disk as UTF-8. A value
`{"b64": "..."}` is raw bytes in base64 (compressed parts, invalid UTF-8).

| Field | Meaning |
|---|---|
| `id` | Unique; starts with the main § clause |
| `spec` | The § clauses the case tests |
| `level` | `MUST` or `SHOULD` |
| `requires` | Features the case needs (`!x`: only without `x`) |
| `note` | Why the expected outcome is right |
| `store` | The store's path in the case directory (default `s.tsvz`) |
| `files` | Path → content, written before the case starts |
| `modes` | Restricts the modes the case runs in |
| `expect` | Reader cases: what reading the store gives |
| `steps`, `after` | Other cases: what to run, and what the files must hold afterwards |

### Reader cases

Write `files` into an empty directory, open `store`, and check `expect`:

- `read`: the live rows, key first, in first-appearance order. Compare after
  removing trailing empty cells from each row: §3.6 lets a reader pad or trim.
  `{"rows": null, "exit": 1}` means reading must fail.
- `keys`, `len`: the live keys in order; how many there are.
- `get`: each query's `args` are keys looked up in order. A live key gives its
  row. A missing key gives the key and the current defaults, or nothing when
  `#_return_defaults_when_missing_#` is false (§14.5); `exit` 3 means at least
  one key was missing.
- `verify`: the mismatches, each `[part path, line, algorithm, expected,
  computed]`, where `{store}` stands for the store's path and `{dir}` for the
  case directory.

### Writer, CLI and handler cases

`steps` run in order. Each step is one of:

| Step | Meaning |
|---|---|
| `{"op": [OP, ARG...]}` | One operation on the store: `tsvz --format records -q OP STORE ARG...`, or the request `OP⇥ARG...` on a socket |
| `{"argv": [...]}` | Exactly these arguments after the command (`{store}`, `{dir}` substituted) |
| `{"request": [...], "options": [...]}`, `{"raw": "..."}` | One §21 request, by fields or as exact bytes |
| `{"append": DATA, "path": P}`, `{"write": DATA, "path": P}` | Append to, or create, a file directly: another writer |
| `{"check": {...}}` | An `after` block checked at that point |

A step may carry `stdin` (on a socket, the request body; `body` gives it
explicitly), `exit` (default 0), `stdout` and `stderr` (`"empty"` or
`"nonempty"`; never compared by content). `stdout` is `"empty"`,
`"nonempty"`, `{"text": ...}` (exact), `{"rows": [...]}` (split on the
delimiter, decoded, trailing empty cells ignored; a live row printed by `read`
must still contain a delimiter, or it would read back as a tombstone), or
`{"lines": [...]}` (split and decoded, exact; `verify` and `parts` lines split
on TAB).

An `after` block holds any of:

- `rows`, `missing`: the files read back give these live rows, and this
  missing-key result (`null` for not-found);
- `fidelity`: `"full"` means everything a reader can observe is unchanged since
  before the first step; `"markers"` the same except that no rows are left. What
  a reader observes is the rows, the missing-key result, and how two probe lines
  appended to the store would read (`tsvzconform-probe-1⇥v␠␠⇥⇥` and
  `tsvzconform-probe-2␠⇥w`, in the store's delimiter), which shows the final
  marker state;
- `exists`, `absent`: paths in the case directory;
- `serve_exit`: the handler's exit status;
- `bytes`: rules on the bytes written: `encoded` (every `<` begins a token, no
  raw `\r`, no byte-order mark at the start of a new part), `lf-terminated`,
  `appended` (no earlier byte changed), `last-part-only` (only the last part
  grew), `unchanged`, `same-file` (same inode and mode), `header-kept`, and
  `snapshot-layout` (§19.2.3).

## The reference implementation

`reference-baseline.json` lists every case the Python reference does not pass
yet, and why. `tsvzconform_test.py` checks that list against a real run, so it
changes only when the reference does.
