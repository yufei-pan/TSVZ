# TSVZ command-line interface — standardization in tsvz-spec-v1 §20

## Intent

Make the `tsvz` command line part of the format specification, so any
conformant tool that exposes TSVZ stores on the command line behaves the same
and shell scripts are portable between implementations. TSVZ 4.1's own `tsvz`
command is then brought into conformance.

This is the first of two related specifications:

- **Spec A (this document):** the command-line interface, §20.
- **Spec B (next):** the dedicated handler protocol of §18.3 — `tsvz serve`, a
  long-running process that owns a store and answers requests over a local
  socket, so repeated commands do not re-read the store. Spec B MUST support
  every normal `TSVZed` operation, especially getting and setting single values.
  Spec A's operation names and record encoding are chosen so that the protocol
  can reuse them unchanged ("the CLI over a socket").

The design rules of TSVZ 4.1 still apply: fault tolerance first (keep the data,
keep going, report ambiguity on stderr), and compatibility with TSVZ 3.39 unless
the spec forbids it.

### Decisions taken during design

| Question | Decision |
|---|---|
| Role of the CLI text | Normative core (MUST/SHOULD) plus a reserved extension namespace |
| Invocation grammar | Operation first in the spec; tools MAY also accept the 3.39 file-first form |
| Required operations | `read`, `get`, `set` (+ `append` alias), `delete`, `clear`, `scrub`, `verify`, `parts` |
| Default output | Table on a terminal, records when piped; `--format` overrides |
| Exit status | 0 done (warnings allowed), 1 failed/refused, 2 usage, 3 missing key, 4 checksum mismatch |
| Missing keys | `get` prints the §14.5 result and exits 3; `delete` is idempotent and exits 0 |
| Normative options | `-h`, `-V`, `--format`, `-q`, `-v`, `-d` (loose files only) |
| Extensions | Operations `x-…` and long options `--x-…` only; 3.39 flags become TSVZ extensions |
| `set KEY` with no value | Writes the tombstone (deletes KEY), as 3.39's `append KEY` did |
| Bulk input | `set STORE -` / `delete STORE -` read from stdin |
| Placement | New §20 of `tsvz-spec-v1.md`, new conformance class in §2.1, examples in Appendix D |

## 1. Spec changes outside §20

**§2.1** gains a third conformance class:

> A **conformant command-line tool** implements §20. It MUST also be a
> conformant reader and a conformant writer for the strict variants. A reader or
> writer library is conformant without providing a command-line tool.

The spec's version (`**Version:** 1`) and the file-format version
(`#_version_#`) are unchanged: §20 adds a conformance class and does not alter
how files are read or written.

**Appendix D. Command-line examples (informative)** — worked examples of every
operation, bulk copying, exit-status use in a shell script, and the file-first
form.

## 2. §20 Command-line interface (normative content)

The subsections below are the requirements §20 states. The implementation plan
turns them into spec prose; the wording may change, the requirements may not.

### 20.1 Invocation

- The canonical form is

  ```
  tsvz [OPTION ...] OPERATION STORE [ARG ...]
  ```

  The program name `tsvz` is used in this specification for illustration; it is
  not normative.
- Options MAY appear anywhere among the arguments before a lone `--`. Every
  argument after `--` is positional.
- An argument consisting of exactly `-`, or of `-` followed by a digit or `.`
  (a negative number), is positional, not an option.
- `STORE` names a store as in §17.1: the path of the unnumbered file, whether or
  not that file exists alongside numbered parts. The behaviour when `STORE` names
  a single numbered or rotated part is implementation-defined.
- `ARG` values are literal strings. A tool MUST NOT interpret escape tokens in
  command-line arguments; it applies §13 encoding when it writes them.
- **File-first form (optional).** A tool MAY additionally accept

  ```
  tsvz [OPTION ...] STORE [OPERATION] [ARG ...]
  ```

  A tool that accepts it MUST decide the form by the first positional argument:
  if it is the name of an operation defined in §20.2, the canonical form applies;
  otherwise the file-first form applies, and an omitted `OPERATION` means `read`.
  (A store whose path equals an operation name must then be given with a path
  prefix, e.g. `./read`.)

### 20.2 Operations

A conformant tool MUST implement every operation below with the stated effect
and exit status (§20.6).

| Operation | Arguments | Effect |
|---|---|---|
| `read` | `STORE` | Print every live key's resolved row (§7, §14) in first-appearance order (§3.4). |
| `get` | `STORE KEY [KEY ...]` | Print each requested key's resolved row, in argument order. For a missing key, print what §14.5 specifies a read returns: the key followed by the active defaults while `#_return_defaults_when_missing_#` is `true`; nothing while it is `false`. A tool SHOULD resolve a requested key as §7.7 resolves a key (trailing space and tab removed while stripping is in force) before looking it up. |
| `set` | `STORE KEY [VALUE ...]` or `STORE -` | Append one record whose first field is `KEY` and whose value fields are the `VALUE`s. A `KEY` with no `VALUE` writes a tombstone (§9.2). An empty-string `VALUE` writes a present-empty cell. A `KEY` matching the reserved pattern (§12.2) writes a marker line with that key. `-` reads records from stdin (§20.3). |
| `append` | as `set` | Alias of `set`; identical behaviour. |
| `delete` | `STORE KEY [KEY ...]` or `STORE -` | Append one tombstone per `KEY`. MUST NOT fail because a key is absent (idempotent). `-` reads keys from stdin (§20.3). |
| `clear` | `STORE` | Make the store empty. For a single-part store: rewrite the part in place keeping the header comment and the active markers (§19.8). For a multi-part store: append a tombstone for every live key to the active part (§18.8). |
| `scrub` | `STORE` | Compact a single-part store in place (§19.8, §19.4 fidelity). A tool MAY refuse to scrub a multi-part store; it then writes nothing and exits 1. |
| `verify` | `STORE` | Replay the store with integrity checking (§15) and print one line per segment whose digest does not match: part path, line number, algorithm, expected digest, computed digest. |
| `parts` | `STORE` | Print the parts of the store in replay order (§17.3), one per line: index (0-based), ordinal in hexadecimal (empty for the unnumbered part 0), path, and flags (`active` for the part appends go to; the compression codec, if any). Parts carrying `.rotated` are not listed. |

- `set`, `append`, `delete` and `clear` MUST create a missing store (an empty
  part at the `STORE` path). `read`, `get`, `scrub`, `verify` and `parts` on a
  missing store MUST exit 1.
- A `verify` of a store with no `#_checksum_<algo>_#` markers succeeds (exit 0).
- **Loose variants.** All operations are available for loose extensions (§5.2);
  their data semantics are implementation-defined. On a loose file `verify` has
  nothing to check, and `parts` lists the file itself.

### 20.3 Bulk input

When the only `ARG` of `set` or `delete` is `-`, the tool reads stdin as a TSVZ
stream in the target store's variant:

- Only committed lines count (§4.3): an unterminated final line is ignored, and
  the tool SHOULD warn about it.
- Lines are framed and split as in §7.1–§7.2 and fields are decoded per §13.
- For `set`: each data line is handled exactly as `set` with those fields (a
  lone key is a tombstone). A line whose first field matches the reserved
  pattern is a marker write. Comment lines and empty lines are skipped.
- For `delete`: the first field of each non-empty line is one key, decoded per
  §13; a line whose first field matches the reserved pattern resets that marker;
  comment lines are skipped.
- All records from one invocation MUST be appended as a single batch (§18.2).

Because `read` prints records in the store's own variant (§20.4),
`tsvz read A | tsvz set B -` copies every live row of `A` into `B` when both
use the same variant. Conversion between variants is out of scope.

### 20.4 Output formats

- `--format records` — the machine format: one record per line, each terminated
  by `\n`, using the store's delimiter and §13 escaping (a key beginning with `#`
  is written `<#>…`). Rows are the resolved rows a reader returns. No header,
  comments or markers are printed. `verify` and `parts` records are their
  fields joined by TAB.
- `--format table` — a human-readable layout. Its form is implementation-defined;
  it MAY truncate and MUST NOT be relied on for parsing.
- Without `--format`, a tool MUST use `table` when standard output is a terminal
  and `records` otherwise.
- For loose variants, records use the file's own escaping (implementation-defined).

### 20.5 Output streams

- Standard output carries only the operation's output.
- Every diagnostic — tolerance warnings, errors, informational messages — goes
  to standard error. The text of diagnostics is implementation-defined.
- `get` that exits 3 still prints its rows (§20.2) to standard output.

### 20.6 Exit status

| Code | Meaning |
|---|---|
| 0 | The operation completed. Tolerance warnings may have been printed. |
| 1 | The operation failed or was refused. When a maintenance operation (`clear`, `scrub`) is refused, nothing was written. |
| 2 | Usage error: invalid arguments, unknown operation, unknown option. |
| 3 | `get`: at least one requested key is missing. |
| 4 | `verify`: at least one checksum mismatch. |
| 5–63 | Reserved for future versions of this specification. |
| 64–125 | Available to extensions (§20.7). |

### 20.7 Options and extensions

A conformant tool MUST accept:

| Option | Meaning |
|---|---|
| `-h`, `--help` | Print usage to standard output and exit 0. |
| `-V`, `--version` | Print the tool's name and version to standard output and exit 0. |
| `--format table\|records` | Select the output format (§20.4). |
| `-q`, `--quiet` | Suppress tolerance warnings; errors are still reported. |
| `-v`, `--verbose` | Report informational messages on standard error. |
| `-d`, `--delimiter D` | For loose variants only: the field delimiter, as one character or one of `tab`, `comma`, `pipe`, `null`. For a strict variant the extension determines the delimiter (§5.1); a conflicting `-d` is overridden and SHOULD produce a warning. |

- **Extension namespace.** This specification will never define an operation
  whose name begins with `x-`, nor a long option whose name begins with `--x-`.
  Implementations MUST put their own operations and long options in that
  namespace.
- An unknown operation or option MUST produce exit status 2.
- A tool MAY keep pre-existing spellings outside the namespace as undocumented
  aliases; this specification gives such aliases no protection from future
  definitions.

## 3. TSVZ 4.1 changes

`TSVZ.py`'s `__main__` is rewritten to conform:

1. Canonical op-first grammar plus the file-first form (§20.1), so every 3.39
   invocation still parses.
2. New operations `get`, `set` (with `append` as its alias), `verify`, `parts`;
   `-` bulk input for `set` and `delete`.
3. `--format` with automatic table/records selection; `-q`.
4. All diagnostics on stderr; the exit codes of §20.6.
5. 3.39 options become extensions: `-c/--header` → `--x-header`,
   `--defaults` → `--x-defaults`, `-s/--strict` → `--x-strict`,
   `-f/--force` → `--x-force`. The old spellings remain as undocumented aliases.
6. `table` output keeps 3.39's `pretty_format_table`.
7. A key matching the reserved pattern passed to `set` follows `TSVZed`'s rule
   (marker write) for `.tsvz`; for loose files the 3.39 rules apply.

Library behaviour (`TSVZed`, `TSVZedLite`, the stateless helpers) does not
change. Messages that the library prints to stdout for 3.39 compatibility
(for example "Created …") are redirected to stderr by the CLI only.

### Breaking changes from the 3.39 CLI

| Change |
|---|
| `read` output piped or redirected is records instead of the table. |
| A missing store on `read` exits 1 with the message on stderr (3.39: stdout, exit 0). |
| Informational messages ("Created …", "Failed to decode …") move to stderr. |
| `-c`, `--defaults`, `-s`, `-f` are no longer documented (they still work). |
| Unchanged: `append KEY` with no values still deletes KEY. |

These are added to the README's breaking-changes section.

## 4. Testing

- CLI conformance tests through `subprocess`, Python 3.6-safe: every operation
  and its exit codes; both formats, including the terminal path (forced with a
  pseudo-terminal, e.g. `pty`, or an equivalent test hook); `-` bulk input for
  `set` and `delete`, including an unterminated final line and marker lines;
  `--`; the file-first form and the `./read` rule; `x-` handling and unknown
  options (exit 2); `-q` and `-v`; stdout carrying no diagnostics.
- The existing 3.39 CLI differential test keeps comparing the file-first form
  against `TSVZ_old.py`, with expectations changed only where a listed breaking
  change applies (stdout vs stderr, exit status).
- Round trip: `tsvz read A | tsvz set B -` reproduces every live row for each
  strict variant.
- The full suite runs on Python 3.6 (Docker) as before.

## 5. Out of scope

- The handler protocol, `tsvz serve`, socket discovery and CLI auto-routing
  (spec B).
- Conversion between variants, multiple stores per invocation, filtering or
  querying beyond `get`.
- Remote (network) access.
