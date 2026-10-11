# TSVZ conformance suite (`tsvzconform.py` and `conformance/`)

## Intent

A standardized, language-neutral test of how closely an implementation of
tsvz-spec-v1 follows the spec. Its main job is the **implementer's dev loop**:
someone writing TSVZ in Rust, Go or any other language runs it on every change,
filters it to the sections they are working on, and gets each failure as a
minimal reproducible case with the § clause it breaks.

It covers all four conformance classes of §2.1 (reader, writer, command-line
tool, handler). Its per-class verdicts are the Read / Write / CLI / Handler
columns of the implementations table on tsvz.org.

Expected results come from the spec text, never from what the reference
implementation happens to do. Writing the suite settles twelve gaps in the spec
(section 5); each lands in the spec together with the cases that test it.

In this document `§N` refers to tsvz-spec-v1; "section N" refers to this
design's own sections.

### Decisions taken during design

| Question | Decision |
|---|---|
| Main job | Implementer's dev loop: fast, filterable, minimal failing cases with § clauses; the site listing and keeping the reference honest are served where cheap |
| Classes covered | All four: reader, writer, CLI §20, handler §21 |
| Location | The TSVZ repo: `TSVZ/tsvzconform.py` (runner) and `TSVZ/conformance/` (cases), beside the spec and `tsvzbench.py` |
| Approach | Hybrid: a declarative JSON case corpus for everything that is data in, data out; a few procedural checks in the runner for what data cannot express (pointer-file safety, races, kills) |
| Oracle | The runner carries its own reader, which must pass the reader corpus; it judges what writers leave on disk |
| Comparison | Decoded rows, not bytes, except where a MUST fixes bytes |
| Optional features | Claimed by the implementer (`--feature`), never guessed |
| Spec gaps | Settled in the spec as part of this project, one commit per decision with its cases (section 5) |
| Reference deviations | Recorded in a baseline file; fixing the reference is a follow-up project |

## 1. Layout and case format

### 1.1 Layout

```
TSVZ/
  tsvzconform.py              the runner: standard library only, Python >= 3.8, POSIX
  tsvzconform_test.py         tests of the runner and the corpus
  conformance/
    README.md                 the case format, for implementers who load cases natively
    reference-baseline.json   the reference's expected non-passing cases (section 8.3)
    cases/
      reader/NN-topic.json    one file per spec area: 04-framing.json, 09-tombstones.json, ...
      writer/NN-topic.json
      cli/NN-topic.json
      handler/NN-topic.json
```

### 1.2 Case files

1.2.1 Each case file is a JSON object:

```json
{"format": 1, "class": "reader", "cases": [ ... ]}
```

`format` versions the case schema (this document defines format 1). `class` is
one of `reader`, `writer`, `cli`, `handler`, and every case in the file belongs
to it.

1.2.2 JSON is the case language because every language parses it and its string
escapes keep invisible bytes visible (`\t`, `\r`, `\u0000`, a missing final
`\n`). TSVZ itself is not used: a reader bug would corrupt the tests meant to
catch it.

1.2.3 **File content** is a JSON string, written to disk as UTF-8, or
`{"b64": "..."}` for exact bytes (compressed parts, invalid UTF-8, a BOM). Each
case is self-contained; there is no shared fixture directory.

### 1.3 Case fields

| Field | Required | Meaning |
|---|---|---|
| `id` | yes | Unique across the corpus. Form `CLAUSE-slug`, where CLAUSE is the main § clause (`9.2`, `20.2`, `21.8.1`) and slug is lowercase kebab-case |
| `spec` | yes | List of § clauses the case tests, e.g. `["9.2", "14.2"]` |
| `level` | yes | `MUST` or `SHOULD`. Rules the spec states as definitions without a keyword are `MUST`. A MAY in the spec becomes a feature (section 2.4); its cases check the MUSTs that apply once the feature is provided |
| `requires` | no | Features (section 2.4) the case needs; `!name` means "only when `name` is not claimed". Default `[]` |
| `note` | yes | One sentence: what the case shows and why that is the expected outcome |
| `store` | no | The store path relative to the case directory. Default `s.tsvz` |
| `files` | no | Map of relative path to content (section 1.2.3): the files present before the case runs. Default `{}` |
| `modes` | no | Modes the case may run in (section 1.6). Default: every mode its steps allow |
| `expect` | reader | Reader expectations (section 1.4) |
| `steps` | writer, cli, handler | Ordered steps (section 1.5) |
| `after` | no | Checks on the files once the steps are done (section 1.7) |

A case never uses a key that starts with `tsvzconform-probe` (section 3.2).

### 1.4 Reader expectations

`expect` holds any of:

| Key | Value | Runs | Expected exit |
|---|---|---|---|
| `read` | rows, or `{"rows": rows or null, "exit": N}` | `read` | 0 unless given |
| `keys` | list of keys | `keys` | 0 |
| `len` | integer | `len` | 0 |
| `get` | list of `{"args": [KEY, ...], "rows": rows, "exit": N}` | `get KEY ...` | as given |
| `verify` | `{"lines": [[path, line, algo, expected, computed], ...], "exit": N}` | `verify` | as given |

Rows are lists of decoded strings, key first. `"rows": null` leaves output
unchecked (used where the spec fixes only the exit status, as for an unreadable
part). A native consumer needs only `files`, `store` and `expect`: write the
files, call its own reader, compare.

### 1.5 Steps

Writer, CLI and handler cases list `steps`, run in order:

| Step | Meaning | Modes |
|---|---|---|
| `{"op": [OP, ARG, ...], ...}` | One operation on the store. CLI: `COMMAND --format records -q OP STORE ARG ...`. Socket: the request `OP<TAB>ARG...` | all |
| `{"argv": [...], ...}` | CLI only: exactly these arguments after `COMMAND`, nothing added; `{store}` and `{dir}` are substituted | `files` |
| `{"request": [OP, ARG, ...], "options": [...], ...}` | Socket only: one §21 request with these fields and options | `socket` |
| `{"raw": "text"}` | Socket only: these exact bytes, for malformed requests | `socket` |
| `{"append": DATA, "path": P}` | The runner appends raw bytes to file P: a direct writer (§21.2.1, §21.8.1) | all |
| `{"write": DATA, "path": P}` | The runner creates or replaces file P (for example a new part) | all |
| `{"check": AFTER}` | An `after` block (section 1.7) checked at this point | all |

`op`, `argv`, `request` and `raw` steps may carry:

- `stdin`: content for bulk input (§20.3). On a socket the same content is the
  request body (§21.7.2) when the store is `.tsvz`, because the TSVZ variant is
  the protocol encoding; only its committed lines are sent, followed by `#`.
  For a `.csvz`, `.nsvz` or `.psvz` store a socket run needs an explicit
  `body` (list of protocol lines), otherwise the case runs in `files` mode only.
- `exit`: the expected exit status, or on a socket the status line's code.
  Default 0.
- `stdout`: a matcher (section 1.6.2). Default: unchecked.
- `stderr`: `"empty"`, `"nonempty"`, or absent (unchecked). Never compared by
  content. Ignored in `socket` and `forwarded` modes.

### 1.6 Modes and matchers

1.6.1 **Modes.** A case whose steps are all `op`, `append`, `write` and `check`
(and every reader case, whose expectations become `op` steps) runs once per
mode its claims allow:

| Mode | How operations run | Counts toward |
|---|---|---|
| `files` | the CLI on the files, no handler running | the case's class |
| `socket` | §21 requests to a handler serving the store | `handler` |
| `forwarded` | the CLI while a handler serves the store; §20.8.1 says stdout and exit status must be what `files` gives | `handler` |

`socket` and `forwarded` need the `handler` feature. A case's `modes` field can
narrow the set. Results are reported per case and mode: `9.2-x`, `9.2-x@socket`,
`9.2-x@forwarded`.

1.6.2 **Matchers** for `stdout`:

| Matcher | Comparison |
|---|---|
| `{"rows": [...]}` | Each line is split on the delimiter (the store's in `files` and `forwarded`, TAB in `socket`, §21.6.1) and each field §13-decoded. Rows are compared in order after removing trailing empty cells from both sides (§3.6 lets a reader pad or trim). Each printed line must contain at least one delimiter, or `read A \| set B -` would turn it into a tombstone (§20.3.2) |
| `{"lines": [[...], ...]}` | Each line split and decoded, fields compared exactly. `verify` and `parts` lines split on TAB (§20.4.1); `keys` lines on the same delimiter as rows |
| `{"text": "..."}` | Exact stdout, e.g. `"3\n"` for `len` |
| `"empty"` / `"nonempty"` | As named |

Expected strings may contain `{dir}` (the case directory) and `{store}` (the
store's absolute path).

### 1.7 After blocks and byte rules

An `after` block (or a `check` step) holds any of:

| Key | Check |
|---|---|
| `rows` | The oracle reads the files and its live rows equal these, in order |
| `missing` | The oracle's missing-key read: a list (the defaults row's cells) or `null` (not-found) |
| `fidelity` | `"full"`: the observable state (section 3.2) equals the state before the first step. `"markers"`: as `"full"` but the live rows are empty (for `clear`) |
| `exists` / `absent` | Paths that must or must not exist |
| `bytes` | A list of byte rules, below |

Byte rules apply to the **written bytes**: what the steps appended to a part, or
the whole part when a step rewrote it.

| Rule | § | Check |
|---|---|---|
| `encoded` | 13.3 | Every `<` begins `<sep>`, `<LF>`, `<lt>`, `<#>` or `<CR>`; no raw `\r` (section 5, decision 10). A key starting with `#` written without `<#>` reads as a comment, which `rows` catches |
| `lf-terminated` | 4.4 | The written bytes end with `\n` |
| `appended` | 3.1 | Every part's earlier bytes are an unchanged prefix of its new bytes |
| `unchanged` | 20.2, 20.6 | Every part is byte-identical to before (refusals) |
| `same-file` | 18.7 | Every part that existed keeps its inode and permission bits |
| `snapshot-layout` | 19.2.3 | Optionally the original header comment first (decision 9); then a block of marker lines with `#_version_#` first and, if present, `#_defaults_#` last; then one data row per live key, in first-appearance order, each with at least one delimiter; then optionally another block of marker lines restoring the final state (decision 12). No other comment, no tombstone, no key twice |

A case has a single level, so a SHOULD rule (such as `lf-terminated`) gets its
own case.

### 1.8 Procedural checks

What data cannot express is written as Python functions in `tsvzconform.py`,
each registered with the same fields as a case (`id`, `spec`, `level`,
`class`, `requires`, `note`). They are listed with `--list`, selected by
`--only`, and reported like any case. Section 4.3 lists them.

## 2. Invocation and the implementation contract

### 2.1 Command line

```
python3 tsvzconform.py --impl COMMAND
       [--class reader,writer,cli,handler] [--only PATTERN[,PATTERN...]]
       [--mode files,socket,forwarded] [--feature NAME[,NAME...] | --feature all]
       [--level must|all] [-j N] [-x] [--timeout SECONDS]
       [--work-dir DIR] [--json PATH] [--list] [-v] [-h] [-V]
```

- `--impl COMMAND`, exactly once: one implementation per run. Split with
  `shlex.split`. A relative COMMAND resolves from the directory the runner was
  started in.
- `--class` selects classes (default all). Classes not selected are not run and
  their operations are never called.
- `--only` patterns match a case id exactly, a clause (`9.2` matches ids
  starting `9.2-` and `9.2.`), or a glob (`13.*`).
- `--mode` selects modes (section 1.6.1; default: every mode the claims allow).
- `--level must` drops SHOULD cases; `all` is the default.
- `-j N` runs N cases in parallel (default: the CPU count). `-x` stops at the
  first FAIL.
- `--timeout` is per step (default 10 s).
- `--list` prints the selected cases (id, class, level, § clauses, requires)
  and exits 0 without running anything.
- `-V` prints `tsvzconform` and its version (`version = '1.0'`,
  `__version__ = version`).

### 2.2 Probe

Before any case, the runner calls `COMMAND -V`. It must exit 0; the first line
of its stdout is recorded as the implementation's version. If it fails, the
runner reports it and exits 1 without running cases.

### 2.3 What each class needs

| Class | Operations the runner calls |
|---|---|
| reader | `read`, `get`, `keys`, `len`, `verify` (and `-V`) |
| writer | the reader's, plus `set`, `set -`, `delete`, `delete -`, `clear`, `scrub` |
| cli | all of §20 |
| handler | `serve`, `stop`, and the §21 socket |

A library-only implementation can write a small adapter that implements the
reader's five operations in the canonical form of §20.1 with records output and
the §20.6 exit statuses, then run `--class reader`. Loading the JSON cases
natively is the other route. `conformance/README.md` states the adapter
contract.

### 2.4 Features

2.4.1 Some behaviour the spec lets an implementation leave out cannot be told
apart from a wrong implementation: a reader that does not implement `crc32`
must treat its marker as inert (§15.3), which looks exactly like a broken
`crc32`. So the implementer **claims** features with `--feature`:

| Feature | Meaning |
|---|---|
| `bzip2`, `xz`, `zstd` | Reads (and `parts` reports) parts in that codec (§16.2; gzip is required, decision 2) |
| `crc32`, `crc32c`, `sha256`, `blake3` | Implements that digest algorithm (§15.2, decision 11) |
| `file-first` | Accepts the file-first form (§20.1.5) |
| `handler` | Provides a handler (§21); implied by `--class handler` |
| `scrub-multipart` | Scrubs a multi-part store instead of refusing (§20.2) |

`--feature all` claims everything.

2.4.2 A case with `requires: ["crc32"]` runs only when `crc32` is claimed; one
with `["!crc32"]` only when it is not, and checks the unclaimed behaviour (the
marker is inert; a `.zst` part makes reads fail; serve exits 1 with a
diagnostic, §20.2.6; a multi-part scrub is refused with nothing written).
Skipped cases are counted per reason.

2.4.3 The four strict variants are not features: every conformant reader and
writer supports them (decision 1). Loose variants have implementation-defined
semantics (§20.2.4); only the availability of their operations is tested.

### 2.5 Isolation and placeholders

Each case and mode runs in a fresh directory, `WORK/<case-id>[@mode]/`. The
runner writes `files` there and passes the store as an absolute path. Steps run
with the runner's start directory as their working directory. Expected strings
use `{dir}` and `{store}` for paths (`parts`, `verify`).

### 2.6 Processes and timeouts

Each step's child runs in a session of its own. On timeout its whole process
group gets SIGKILL and the step fails as `timeout`. Standard input comes from a
file; stdout and stderr go to files in the case directory.

### 2.7 Handler lifecycle

In `socket` and `forwarded` modes, and in handler cases, the runner writes the
files, starts `COMMAND serve STORE`, and waits up to 10 s for a valid pointer
file (§21.2: first line `tsvz-handler<TAB>1`, an `address` of `unix:PATH`).
After the steps it sends `stop`, which must answer `#0`; the process must exit
0 within 10 s and its pointer file must be gone (§21.2.3, §21.13). A handler
that does not stop gets SIGTERM to its process group, then SIGKILL after 5 s.
The same cleanup runs for every live handler at exit and on SIGINT/SIGTERM.
`after` checks run once the handler has stopped, so every acknowledged write is
on disk (§21.9.3).

### 2.8 Work directory and repro

The default work directory is a fresh temporary directory. Passing cases'
directories are removed as they finish. A failed case's directory is kept, and
its report shows the path and the exact command line that failed. `--work-dir
DIR` uses DIR instead (created if missing). When nothing failed the work
directory is removed at exit.

## 3. The oracle and judging

### 3.1 The oracle

3.1.1 `tsvzconform.py` contains a complete reader for §4–§17, written from the
spec independently of the cases:

- framing, BOM removal and U+FFFD decoding (§4, decisions 7 and 8);
- all four strict variants;
- marker classification, stateful markers with the value rules of decision 5,
  tombstones, empty keys, stripping, §13 tokens including `<CR>`, and §14
  resolution with forward-bound defaults;
- multi-part assembly (§17): part 0, hexadecimal ordinals, `.rotated` skipped;
  gzip, bz2 and xz parts through the standard library, zstd where the Python
  provides `compression.zstd`.

It does not compute digests: `verify` results are written into each case.

3.1.2 The oracle must pass every reader case; `tsvzconform_test.py` enforces it.
It is then trusted to read what writers leave on disk, so a writer bug and a
matching bug in the implementation's own reader cannot cancel out.

3.1.3 A writer case whose post-state needs a codec the runner's Python lacks
(zstd before 3.14) is skipped with that reason.

### 3.2 Observable state

The observable state of a store is what a reader can tell apart:

1. the live rows, resolved, in first-appearance order;
2. the missing-key read at the end of replay: the current defaults row, or
   not-found;
3. the **probe**: the rows a reader returns for the keys starting
   `tsvzconform-probe` after these lines, using the store's delimiter D, are
   replayed at the end of the store:

   ```
   tsvzconform-probe-1⇥v␠␠⇥⇥
   tsvzconform-probe-2␠⇥w
   ```

   (⇥ is D, ␠ a space.) The first has a cell with trailing spaces, two
   present-empty cells and absent cells from column 4; the second has a key
   with a trailing space.

The probe makes the final marker state observable: stripping, fill-empty and
the bound defaults all show in its rows. A scrub or clear that keeps the rows
but changes how later appends read fails the probe (decision 12).

### 3.3 Results

| Result | Meaning |
|---|---|
| `pass` | As expected |
| `FAIL` | A MUST case did not hold |
| `warn` | A SHOULD case did not hold |
| `skip` | Class or mode not selected, feature not claimed (or `!feature` claimed), or the environment cannot run it (needs root, runner lacks zstd); the reason is recorded |
| `error` | The runner itself raised: a bug in the suite, not the implementation |

Optional behaviour enters only through claims, so a case of a claimed feature
that does not hold is a `FAIL`. A crash, a timeout or an unexpected exit status of the
implementation is a `FAIL` (or `warn`), never an `error`.

## 4. Coverage and authoring

### 4.1 Authoring rules

4.1.1 Every expectation is written by hand from the clause it cites; its `note`
says why. No expectation is generated from an implementation's output.

4.1.2 The corpus, the oracle and the reference check one another. When two
disagree, the cause is found before anything changes: a wrong case, a reference
bug (recorded in the baseline, section 8.3), or a spec question.

4.1.3 A case is written only where the spec decides the outcome. A gap found
while writing cases is collected and put to the user one question at a time at
a checkpoint in the plan; its cases wait for the answer, which lands in the spec
as in section 5.

4.1.4 Digests in `verify` cases are computed when the case is written. The
corpus tests recompute crc32 and sha256 with the standard library, crc32c with
a small pure-Python routine, and blake3 with the `blake3` package when it is
installed (skipped otherwise).

### 4.2 Coverage

| Class | § | Cases (examples) | ~N |
|---|---|---|---|
| reader | 4 | `\n` and CRLF; exactly one `\r` stripped; `\r` elsewhere is data; an unterminated tail is discarded per part, not joined to the next part; empty file; BOM stripped per part; invalid UTF-8 to U+FFFD | 20 |
| | 5–6 | each delimiter; other delimiters are data; NUL in `.nsvz`; no version line is v1; `#_version_#` 2 still reads; 1→2→1 | 15 |
| | 7–11 | tombstone decided before stripping (`a␠` deletes `a`); empty-key rows ignored even without a delimiter; `<#>foo` is data; ` #x` is data; `#x␠` is a comment; delete-then-rewrite moves to the end; single-column store | 30 |
| | 12 | marker keys case-insensitive and matched raw; `#_a_#x` is a comment; empty value resets; later replaces; values decoded; advisory markers inert; invalid values ignored; the eight boolean spellings | 25 |
| | 13 | every token incl. `<CR>`; `<lt>sep>`; unknown tokens and a bare `<` pass through; names case-sensitive (`<lf>`); `<sep>` is each variant's delimiter | 15 |
| | 14 | Appendix C; defaults bound at the row's position; fill-empty; the missing-key read and not-found (`get` exit 3, nothing printed) | 20 |
| | 15 | arming ignores the value; matching segment; mismatch line (1-based marker line in its part, lowercase digests); empty value resets; segments span parts; other algorithms' lines covered; uppercase digest accepted; each registered algorithm; unclaimed algorithm inert | 25 |
| | 16–17 | gzip (`.gz`, `.gzip`); each optional codec; unclaimed codec makes reads fail; `f` before `10`; part 0 before ordinal `0`; uppercase hex ordinals; `.rotated` and `.rotated.gz` excluded; markers span parts | 20 |
| writer | 13, 3.1 | round trip of every special character in every variant (delimiter, LF, CR, `<`, leading `#`, the text `<sep>`, NUL in non-NSVZ variants, non-ASCII); `encoded`; `appended`; marker writes; tombstone vs. present-empty | 25 |
| | 19, 18.7 | scrub layout and fidelity across changed defaults, stripping and fill-empty; return-missing kept; header comment kept or dropped; clear keeps the header and markers; `same-file`; multi-part clear appends tombstones to the active part; multi-part scrub refused (`unchanged`) or done (`scrub-multipart`) | 30 |
| cli | 20.1 | options anywhere before `--`; `-`, `-5`, `-.5` positional; `--opt=V`; arguments literal (`<sep>` stays text); unknown operation or option exits 2; `-h`, `-V`; file-first form | 20 |
| | 20.2–20.7 | every operation's output and exit status; creates vs. must-not-create; `set -` and `delete -` lines (markers, comments, empty lines, unterminated final line); `verify` and `parts` lines TAB-joined even for `.csvz`; `-d` on a strict variant; loose files accept every operation | 70 |
| handler | 21 | pointer format; CSVZ store answered in TAB; `\r\n` requests; marker key vs. `<#>` data key; `set -` body ending in `#`; follows direct appends; `--sync` writes are on disk at the ack; malformed request is status 2 and the connection stays usable; `setdefault` with a marker key is 2; `stop`; serve creates a missing store | 30 |

Every reader case and every `op`-only CLI case also runs in `socket` and
`forwarded` modes when a handler is claimed, so §21.7.1 ("every operation of
§20.2 ... with the effect, output and exit status that §20 gives it") is tested
without writing the cases twice.

### 4.3 Procedural checks

| Id (clause) | Class | Level | Check |
|---|---|---|---|
| 20.4.3 terminal default | cli | SHOULD | Under a pty, `read` exits 0 and its output differs from the records output (the table form is implementation-defined, so only the difference is observable) |
| 21.2.3 lock held | handler | MUST | While serving, `flock(LOCK_EX\|LOCK_NB)` on the pointer file fails |
| 21.2.3 pointer mode | handler | MUST | The pointer file is not writable by group or others |
| 21.13.1 second serve | handler | MUST | While the runner holds the pointer lock, `serve` exits 1 |
| 21.2.3 stale takeover | handler | MUST | A stale pointer file (no lock holder) is replaced and `serve` starts |
| 21.2.3 no symlink write | handler | MUST | A pointer path that is a symlink to a victim file: the victim is unchanged afterwards |
| 21.2.4 client refuses pointer | cli | MUST | The runner listens on a decoy socket and plants a bad pointer: a symlink, group-writable, wrong first line, control character in a value, another host, a non-`unix:` address, or (as root only) a socket owned by another user. The CLI must not connect to the decoy, and its result must equal the `files` result |
| 21.10.2 line limit | handler | MUST | A 1 MiB request line gets a response, not a closed connection |
| 21.7.2 cut-off bulk | handler | MUST | A `set -` whose connection closes before `#` writes nothing |
| 21.8.2 atomic pop | handler | MUST | Eight clients `pop` one key at once: exactly one gets the row, seven get status 3, and one tombstone is written |
| 21.11.1 socket access | handler | SHOULD | The socket, or a directory on its path, is closed to group and others |

## 5. Spec decisions

Each decision is one spec commit carrying the cases that test it. v1 is still a
Draft, so these are clarifications without a version bump.
`website/spec-glance.md` is updated where it repeats a changed rule.

| # | Clauses | Decision | Reference |
|---|---|---|---|
| 1 | §2.1 | Every conformant reader and writer supports all four strict variants | conforms |
| 2 | §16.2 (§16.1, §21.8.5) | gzip (`.gz`, `.gzip`) is required; bz2, xz/lzma and zst/zstd are optional. A reader that meets a part in a codec it lacks MUST fail, never skip or misread the part; the CLI prints a diagnostic and exits 1 | deviates: before Python 3.14 it reads a `.zst` part as plain bytes |
| 3 | §20.2 `verify` | The line number is the 1-based line, within the printed part, of the `#_checksum_<algo>_#` marker whose segment failed | conforms |
| 4 | §13.2, §13.4 | Token names are case-sensitive; `<lf>`, `<SEP>` and the like are unrecognized and pass through | conforms |
| 5 | §12.1, §12.4.1 | A marker line with an invalid value is ignored and the marker keeps its previous state; a reader MAY warn. Boolean values are `true`/`false`/`yes`/`no`/`on`/`off`/`1`/`0`, compared ignoring ASCII case; writers SHOULD write `true`/`false`. `#_version_#` takes ASCII decimal digits with a value ≥ 1 | conforms |
| 6 | §15.2 | Writers MUST write lowercase hex digests; readers compare ignoring ASCII case; `verify` prints both digests in lowercase | conforms |
| 7 | §4.1, §15.4 | A reader MUST remove one UTF-8 BOM at the start of each part; it is not part of the first line or of any checksum segment, and does not change line numbering; a reader MAY warn. Writers MUST NOT write one | conforms |
| 8 | §4.1 | A reader MUST decode invalid UTF-8 to U+FFFD by Unicode's maximal-subpart rule and keep reading; a reader MAY warn. Checksums cover the raw bytes. Because every delimiter and `\n` is ASCII, decoding per line or per part gives the same result | conforms |
| 9 | §19.2.3c, §19.8 | A snapshot MAY begin with the store's header comment (the first committed line of its first part, when that line is a comment), before the markers; it contains no other comment | conforms |
| 10 | §13.2, §13.3, §13.5, §6.1 | Add the token `<CR>` for U+000D. Writers MUST encode every carriage return as `<CR>`; readers decode it. A raw `\r` before `\n` is still stripped (§4.2). §13.5 gains the example | deviates: writes raw `\r`, does not decode `<CR>` |
| 11 | §15.2 | A registry: `crc32` (CRC-32/ISO-HDLC, as zlib's `crc32`), `crc32c` (CRC-32C, Castagnoli), `sha256` (FIPS 180-4), `blake3` (default 32-byte output). A CRC is written as its 32-bit value in 8 hex digits, most significant first; a hash as its output bytes in order. Other names MAY be supported; their meaning is implementation-defined | conforms for crc32 and sha256; has no crc32c or blake3 (optional) |
| 12 | §19.2.3, §19.4, §19.6 | A snapshot MUST preserve the live rows and the final marker state. When it writes its rows under different marker state to keep them exact, it MUST re-emit the final markers after the rows; the layout of §19.2.3 and the strategy suggested in §19.4 say so | conforms |

## 6. Report and exit status

6.1 **Failures** print as blocks, in case order:

```
FAIL 9.2-trailing-delimiter-is-not-a-tombstone   MUST §9.2 §14.2
  key<TAB> sets a present-empty cell; only a lone key deletes
  files   s.tsvz = "a\tx\na\t\n"
  step 1  read → exit 0 (ok)
  rows    expected ["a", ""]   got (no rows)   first difference: row 1 missing
  stderr  (last 5 lines)
  kept    /tmp/tsvzconform-k3x9/9.2-trailing-delimiter-is-not-a-tombstone/
  rerun   python3 tsvzconform.py --impl 'tsvz' --only 9.2-trailing-delimiter-is-not-a-tombstone
```

Inputs and values are shown as JSON strings. Each `warn` gets one line; skips
are counted by reason, and `-v` lists them.

6.2 **The summary:**

```
tsvzconform 1.0 · cases format 1 · impl: tsvz 4.2.3 (python3 TSVZ.py)
claimed: bzip2, xz, crc32, sha256, file-first, handler
           pass  FAIL  warn  skip
reader      162     0     1    14
writer       48     1     0     2
cli          88     0     2     0
handler     310     0     0     0
conformant: reader ✓   writer ✗ (1 MUST)   cli ✗ (needs writer)   handler ✗ (needs writer)
```

A class is conformant when none of its results is `FAIL`; CLI and handler also
need reader and writer to be conformant (§2.1). A class that was not run shows
`–`.

6.3 `--json PATH` writes one record per (case, mode): id, mode, class, spec,
level, result, detail, seconds; plus the runner version, the cases format, the
implementation's `-V` line and its claims.

6.4 **Exit status:** 0 when nothing is `FAIL` or `error` (warns allowed); 1
when anything is `FAIL` or `error`, or the `-V` probe failed; 2 for a usage
error (including an unknown feature name); 130 after Ctrl-C, with the results
so far printed.

## 7. Error handling

7.1 A failure never aborts the run (except with `-x`): a crash, timeout or
unexpected exit marks that (case, mode) and the runner moves on. The first
failing step ends its case; later steps are not run.

7.2 A case file that fails the schema stops the run before any case, with the
file and the problem, and exit status 1. So does an unknown feature in a
`requires`.

7.3 An exception inside the runner while running one case makes that case
`error`, with its traceback in the report, and the run continues.

7.4 Handlers never outlive the runner (section 2.7). Ctrl-C stops the running
cases, cleans up, and prints the results so far.

7.5 Platforms: Linux is primary; macOS works. Without `AF_UNIX`, `os.setsid` or
`pty` (Windows) the runner exits 1 at start with a message.

## 8. Testing the suite

`tsvzconform_test.py`: plain pytest `test_*` functions; passes on Python 3.8
and on the current Python.

8.1 **Corpus lint.** Every case file matches the schema. Ids are unique and
start with a clause. Every `spec` entry names a clause that exists in
`tsvz-spec-v1.md`. Every `requires` names a known feature. No case key starts
with `tsvzconform-probe`. Digests recompute (section 4.1.4).

8.2 **Oracle and judging.**

- The oracle passes every reader case.
- Fake implementations, each a small script that wraps the reference and breaks
  one thing, get the right result through the real runner: a dropped row →
  FAIL; a live row printed without a delimiter → FAIL; a wrong exit status →
  FAIL; a hang → FAIL with its process group killed; a crash → FAIL, not
  `error`; a scrub that renames a new file over the part → FAIL `same-file`; a
  scrub that drops the final markers → FAIL fidelity; a CLI that connects to the
  decoy pointer socket → FAIL.
- Claims: unclaimed features skip, `!feature` cases run only when unclaimed,
  modes are selected from claims.
- `--list`, `--only` patterns, `--level`, usage errors (exit 2), and Ctrl-C
  (exit 130 with partial results).

8.3 **The reference baseline.** The end-to-end test runs the reference
(`python3 TSVZ.py`) with its claims: `bzip2`, `xz`, `crc32`, `sha256`,
`handler`, `file-first`, and `zstd` on Python 3.14+. Its
non-passing results must equal `conformance/reference-baseline.json`, which
lists each non-passing (case, mode) with its result and a reason. A new failure
or an unexpected pass fails the test, so the file changes only on purpose: when
the reference is fixed, entries are removed. Entries may depend on the Python
version (the zstd fallback), recorded as a condition on the entry.

## 9. Documentation and packaging

- `conformance/README.md`: the case format (sections 1.2–1.7), the feature
  names, the reader-adapter contract, how to run the runner, and how to load
  the cases natively.
- `TSVZ/README.md`: a "Conformance testing" section beside "Benchmarking
  implementations", and the "Spec deviations and interpretations" list brought
  in line with section 5 (deviations that became spec rules are removed).
- `website/index.md`: "Test against the reference implementation" becomes
  "Run the conformance suite", linking it, and the table's four columns are
  explained as its verdicts. The site goes live only when its service restarts,
  which needs the user's go-ahead.
- `website/spec-glance.md`: the token line gains `<CR>`.
- cas `CLAUDE.md`: `tsvzconform.py` under Tests.
- `setup.py` is unchanged: like `tsvzbench.py`, the runner and the corpus are
  not in the wheel.

## 10. Out of scope and follow-ups

- **Fixing the reference** (`<CR>`, the zstd fallback, and whatever the first
  run finds): a follow-up project with its own TSVZ release, driven by the
  baseline.
- Comparing implementations or measuring speed (that is `tsvzbench.py`).
- The data semantics of loose variants.
- Crash and power-loss durability, and the locks of §18.6.
- Remote (TCP) handler access, fuzzing or generated cases, Windows, and badges.
- Switching `tsvzbench.py` to the new oracle.
