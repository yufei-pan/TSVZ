# TSVZ Write Handler (`tsvz serve`, spec §21) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Specify a dedicated write handler as `tsvz-spec-v1` §21 (with §20 additions) and build it into TSVZ 4.1. `tsvz serve` keeps a store loaded and answers requests on a local socket; `tsvz` commands and a new `TSVZClient` use it, and fall back to the files when no handler runs.

**Architecture:** Five layers in `TSVZ.py`, each built on the one before:
- **Engine.** `_ServeStore` keeps a view of the store that is always a replay of its files: it follows appends incrementally and reloads on any other change. `_ServeWriter` is the single appender, batching writes.
- **Protocol.** `_serveRespond` turns one request line into response lines, in §20's vocabulary and record encoding.
- **Server.** `_ServeHandle` owns the pointer file `STORE.serve` and a threaded socket server.
- **CLI.** The six new operations, `serve`, `stop`, and routing of command lines through a live handler.
- **Python client.** `TSVZClient`, which uses the same requests over the socket, or in-process when no handler runs.

**Tech Stack:** Python ≥ 3.6 standard library (`socket`, `socketserver`, `threading`), imported lazily; pytest; Docker `python:3.6-slim` / `python:3.7-slim`.

**Spec:** `docs/superpowers/specs/2026-10-09-tsvz-serve-design.md` is the approved design; its §2 is the normative content of spec §21. Background: `tsvz-spec-v1.md` (§17, §18, §20), `docs/superpowers/specs/2026-10-08-tsvz-cli-design.md`.

All the code in this plan was built and run in a scratch copy of the repository before the plan was written. Each task's code passed its tests there: 233 tests in the end, on Python 3.13, 3.6, 3.7, and 3.6 under `LANG=C`.

## Global Constraints

- Python floor is **3.6**. Do not use 3.7+ APIs: no `time.time_ns`, `reversed(dict)` (use `OrderedDict`), `subprocess.run(capture_output=...)`, `contextlib.nullcontext`, walrus, dataclasses.
- `TSVZ.py` stays one self-contained file with no runtime dependencies; tab-indented; camelCase private helpers. `socket`, `socketserver`, `signal`, `tempfile` and `hmac` are imported inside the functions that use them, so `import TSVZ` does not load them.
- `TSVZed`, `TSVZedLite` and the stateless helpers do not change behaviour. Task 2's refactors keep every public signature and return value.
- Every existing test keeps passing, including `python3 -m pytest TSVZ_test.py -q -k 339`.
- A command line answered through a handler prints the same stdout and exits with the same status as on the files (spec §20.8).
- Text the CLI and the server print is ASCII. Tests that check non-ASCII stderr run in-process (`_run`).
- Commit messages: one imperative sentence ending in a period, a blank line, then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew`.
- Run every command from the repository (or worktree) root. In this environment `rm` is aliased to `rm -i`; use `rm -f` in scripts.

## Deviations from the design

These were decided while building the plan. Task 1 writes them into the design doc so the two agree.

1. **`del client[key]` on a missing key is a no-op**, not `KeyError`. `TSVZed` treats it so (3.39 tolerance), and the design's governing rule is "`TSVZed`'s semantics".
2. **`TSVZClient(..., timeout=None)`** by default, meaning no limit per answer. A 5 s limit would cut off a long `clear` and leave the connection out of step. Connecting always gives up after 0.5 s.
3. **`serve --x-header` and `--x-defaults` apply only when `serve` creates the store.** The served view never uses initial defaults or 3.39 strict mode, so routed output equals direct output.
4. **Routing rule.** A command line is forwarded only when it has no `--x-direct`, `--x-header`, `--x-defaults` or `--x-strict`. For a loose file, its delimiter must also equal the server's, which the pointer records as `x-delimiter`.
5. **`--x-tcp`** (TSVZ extension) makes `serve` use the TCP transport on any platform, which lets the tests exercise it on Linux.
6. **Racy directory timestamps.** While the store's directory changed less than 2 s ago, the engine re-lists the parts on every sync, so a part created within one timestamp tick is not missed. A rewrite in place that keeps a part's size and mtime is not detected (README).

## Review Focus

1. **Many clients at once.** 8 connections × 50 interleaved `set`/`get` all land, each answered on its own connection. Test in Task 5.
2. **A client that disconnects in the middle of bulk input** (before the closing `#`) writes nothing. Test in Task 5.
3. **A crashed handler's stale pointer.** Clients warn and use the files, and the next `tsvz serve` takes the lock and replaces the pointer. Tests in Tasks 5 and 6.
4. **The store file deleted while served.** The next write recreates it, and the view follows. Test in Task 3.
5. **Changes inside one timestamp tick.** A new part created without moving the directory's mtime is still found. Test in Task 3.

## File Structure

| File | Change |
|---|---|
| `tsvz-spec-v1.md` | §1 scope; §2.1 handler class; §18.3 pointer; §20.2 rows, 20.2.2, 20.2.5, 20.2.6; §20.8; new §21; Appendix D examples (Task 1). |
| `docs/superpowers/specs/2026-10-09-tsvz-serve-design.md` | The deviations above (Task 1). |
| `TSVZ.py` | Task 2 helpers in the reader/writer section. A "Write handler" section directly above the CLI block (Tasks 3, 4, 5, 7). CLI additions (Tasks 4–6). |
| `TSVZ_test.py` | One test group per task, appended above the final `if __name__ == '__main__':` block. |
| `README.md` | Quick start lines, "Serving a store", spec interpretations (Task 8). |

### Names introduced (reference for every task)

- **Task 2:**
  - `_PartInfo.committed`, `_PartInfo.lines`;
  - `_specReplayLine`, `_specPadRow`;
  - `_legacyReadLoop`, `_legacyFormatPayload`;
  - `_tryLockFile`, `_parseRecordLines`.
- **Task 3:**
  - `_SERVE_CHECK_BYTES`, `_SERVE_RACY_SECONDS`, `_partRows`;
  - `_ServeWriter`, `_ServeStore`.
- **Task 4:**
  - constants: `_SERVE_PROTOCOL_VERSION`, `_SERVE_LINE_LIMIT`, `_SERVE_ARITY`, `_SERVE_UNREADABLE`, `_ServeUsage`;
  - encoding: `_serveEncode`, `_serveRecordLine`, `_serveFieldsLine`, `_serveDecodeLine`;
  - requests: `_serveParseRequest`, `_serveNeedsBulk`, `_ServeLog`, `_serveDispatch`, `_serveRespond`;
  - responses: `_serveParseResponse`, `_ServeLost`, `_ServeLocal`;
  - CLI: `_cliRequestLine`, `_cliEmitServed`, `_cliEngineOp`.
- **Task 5:**
  - exceptions: `_ServeBusy`, `_ServeUnreachable`, `_ServeSignal`;
  - pointer and client: `_servePointerPath`, `_serveFind`, `_ServeConnection`;
  - server: `_SERVE_CLASSES`, `_serveClasses`, `_serveGroup`, `_serveIsStop`, `_ServeHandle`, `_serveSignals`;
  - CLI: `_cliServe`, `_cliStop`.
- **Task 6:** `_CLI_READ_ONLY`, `_cliHandler`, `_cliRouted`.
- **Task 7:** `TSVZClient`.
- **Test helpers:**
  - Task 3: `_view(path)` and the `engines` fixture.
  - Task 5: the `served` fixture (handles carry `.thread`), `_raw(address, data)` and `_serve_process(store, *options)`.
  - Task 6: `_ROUTED_STEPS`.
  - Task 7: `_mapping_story(t)`.

---


### Task 1: Spec text: §2.1 handler class, §20 additions and §20.8, §21, Appendix D

**Files:**
- Modify: `tsvz-spec-v1.md`
- Modify: `docs/superpowers/specs/2026-10-09-tsvz-serve-design.md`

**Interfaces:**
- Consumes: the design doc's §1 and §2, and the planning decisions listed under *Deviations from the design*.
- Produces: the normative text Tasks 2–8 implement.

The design doc's §2 becomes spec prose here. The last four edits bring the design doc in line with the planning decisions.

- [ ] **Step 1: Edit the spec**

In `tsvz-spec-v1.md`, replace

```markdown
integrity checking, compression, multi-part stores, the snapshot (compaction)
procedure, and a command-line interface for tools (§20).
```

with

```markdown
integrity checking, compression, multi-part stores, the snapshot (compaction)
procedure, a command-line interface for tools (§20), and a protocol for a
dedicated write handler (§21).
```

In `tsvz-spec-v1.md`, replace

```markdown
or writer is conformant without providing a command-line tool, and §20 does not
change how files are read or written.
```

with

```markdown
or writer is conformant without providing a command-line tool, and §20 does not
change how files are read or written.

A **conformant handler** is software that implements §21. It MUST also be a
conformant reader and a conformant writer for the strict variants.
```

In `tsvz-spec-v1.md`, replace

```markdown
append (and a single fsync). This preserves single-writer-per-part while allowing
many concurrent in-memory producers.
```

with

```markdown
append (and a single fsync). This preserves single-writer-per-part while allowing
many concurrent in-memory producers. §21 specifies a protocol for such a handler,
reached over a local socket.
```

In `tsvz-spec-v1.md`, replace

```markdown
| `parts` | `STORE` | Print the parts of the store in replay order (§17.3), one per line: index (from 0), ordinal in hexadecimal (empty for the unnumbered part 0), path, and flags (`active` for the part appends go to, and the compression codec if any, separated by commas). Parts carrying `.rotated` are not listed. |
```

with

```markdown
| `parts` | `STORE` | Print the parts of the store in replay order (§17.3), one per line: index (from 0), ordinal in hexadecimal (empty for the unnumbered part 0), path, and flags (`active` for the part appends go to, and the compression codec if any, separated by commas). Parts carrying `.rotated` are not listed. |
| `has` | `STORE KEY` | Print nothing. Exit with status 0 when KEY is live and 3 when it is not. |
| `len` | `STORE` | Print the number of live keys. |
| `keys` | `STORE` | Print each live key on a line of its own, in first-appearance order, encoded as the first field of a record (§13, `<#>` for a leading `#`). |
| `pop` | `STORE KEY` | Print the resolved row of KEY and append a tombstone for it. When KEY is missing, print nothing, write nothing and exit with status 3. |
| `popitem` | `STORE [first\|last]` | As `pop`, for the first or the last live key (default `last`). Exit with status 3 when the store has no live key. |
| `setdefault` | `STORE KEY VALUE [VALUE ...]` | When KEY is live, print its resolved row. Otherwise append the record `KEY VALUE ...` and print the row a reader then returns for KEY. |
```

In `tsvz-spec-v1.md`, replace

```markdown
20.2.2 `set`, `append`, `delete` and `clear` MUST create a missing store as a part
at the `STORE` path. `read`, `get`, `scrub`, `verify` and `parts` on a missing
store MUST NOT create it and MUST exit with status 1.
```

with

```markdown
20.2.2 `set`, `append`, `delete`, `clear` and `setdefault` MUST create a missing
store as a part at the `STORE` path. `read`, `get`, `scrub`, `verify`, `parts`,
`has`, `len`, `keys`, `pop` and `popitem` on a missing store MUST NOT create it and
MUST exit with status 1.
```

In `tsvz-spec-v1.md`, replace

```markdown
(§5.2); their data semantics are implementation-defined. On a loose file, `verify`
has nothing to check and `parts` lists the file itself.
```

with

```markdown
(§5.2); their data semantics are implementation-defined. On a loose file, `verify`
has nothing to check and `parts` lists the file itself.

20.2.5 `pop`, `popitem` and `setdefault` read and then write. A tool that performs
them on the files need not make them atomic with respect to other writers; through
a handler they are atomic (§21.8).

20.2.6 `serve` and `stop` are defined by §21.13. A tool that does not provide a
handler MUST exit with status 1 and a diagnostic for them.
```

In `tsvz-spec-v1.md`, replace

```markdown
20.7.4 A tool MAY keep pre-existing spellings outside the namespace as
undocumented aliases. This specification gives such aliases no protection from
future definitions.

---
```

with

```markdown
20.7.4 A tool MAY keep pre-existing spellings outside the namespace as
undocumented aliases. This specification gives such aliases no protection from
future definitions.

### 20.8 Routing through a handler

20.8.1 A tool MAY forward an operation to the handler (§21) that a store's pointer
file names (§21.2). Standard output and the exit status MUST then be what the
operation produces on the files without the handler, given the same store
contents; diagnostics MAY differ.

20.8.2 A tool SHOULD warn and operate on the files when the pointer file is stale
or its handler does not answer. It SHOULD NOT forward a command line whose result
depends on options the handler does not share.

---

## 21. Write handler protocol

This section defines how a dedicated write handler (§18.3) is found and talked to
over a local socket, so that many producers and command-line invocations share one
loaded store and one appender. It is a separate conformance class (§2.1). Its
operations, record encoding and exit statuses are those of §20.

### 21.1 Purpose

21.1.1 A **handler** is a process that serves exactly one store: it keeps the
store's resolved state, answers requests about it, and appends every write it
receives (§18.3).

21.1.2 A handler is not the only possible writer. Other writers MAY append to the
store directly, and the handler MUST follow them (§21.8).

### 21.2 Pointer file

21.2.1 The **pointer file** of a store is the store path (§17.1) as given,
including any compression suffix, followed by `.serve`; for example
`data.tsvz.serve`. It is not a part: Appendix A does not match it.

21.2.2 Its content is UTF-8 lines of `NAME<TAB>VALUE`, each VALUE encoded per §13
with TAB as the delimiter. The first line is `tsvz-handler<TAB>1`, naming the
format and the protocol version. The other lines may come in any order:

| Name | Value |
|---|---|
| `address` | `unix:PATH` for a Unix-domain socket, or `tcp:HOST:PORT` with a loopback HOST (`127.0.0.1` or `[::1]`). |
| `host` | The name of the host the handler runs on. |
| `pid` | The handler's process id. |
| `token` | Present with a TCP address: the token of §21.11. |

Readers MUST ignore names they do not know. Names beginning with `x-` belong to
implementations.

21.2.3 A handler MUST hold an exclusive advisory lock on its pointer file (on
POSIX, `flock(2)` on the whole file) for as long as it serves, and MUST refuse to
start while another process holds that lock. It writes the pointer file in place
under the lock, never by renaming another file over it, and removes it when it
stops cleanly.

21.2.4 A client MUST NOT use a pointer file whose first line is not
`tsvz-handler<TAB>1`, that names another host, or whose handler does not accept a
connection. Such a pointer is stale until a handler obtains the lock and rewrites
it.

### 21.3 Transport

21.3.1 A handler listens on a stream socket on the local host: a Unix-domain socket
where the platform has one, else TCP on a loopback address.

21.3.2 A connection carries any number of requests, one at a time: the client sends
a request and reads its whole response before it sends the next one.

### 21.4 Framing and encoding

21.4.1 Requests and responses are UTF-8 text lines, each ending in `\n`. A handler
SHOULD remove one `\r` before the `\n` of a request line.

21.4.2 Fields are separated by TAB and encoded per §13 with TAB as the delimiter,
whatever the store's variant.

21.4.3 Each field of a request is decoded per §13 before use. A key whose written
form matches the reserved pattern (§12.2) is a marker key, as in §20.2; a key
written with `<#>` is a data key.

### 21.5 Requests

21.5.1 A request line is `[OPTION ...] OPERATION [ARG ...]`: the form of §20.1
without `STORE`, which the connection implies.

21.5.2 Fields before the operation that begin with `--` are options. Version 1
defines one: `--sync`, which makes the handler acknowledge the request's writes
only after they are on durable storage (§21.9).

### 21.6 Responses

21.6.1 A response is zero or more lines followed by exactly one status line:

- **Output lines** carry the operation's output in the TSVZ variant: rows as
  `read --format records` prints them (§20.4.1), and the lines §20 defines for
  `len`, `keys`, `verify` and `parts`. A first field that begins with `#` is
  written `<#>…`.
- **Diagnostic lines** are `#!<TAB>TEXT`, with TEXT encoded per §13. A
  command-line tool copies TEXT to standard error.
- **The status line** is `#` followed by the exit status of §20.6 in decimal,
  optionally followed by `<TAB>MESSAGE` encoded per §13. It ends the response.

21.6.2 Output lines never begin with an unencoded `#`, so the first two
characters of a line decide its kind.

### 21.7 Operations

21.7.1 A handler MUST implement every operation of §20.2 except `serve`, with the
effect, output and exit status that §20 gives it, and these two:

| Operation | Effect |
|---|---|
| `stop` | Make every acknowledged write durable, reply `#0`, then stop serving (§21.13). |
| `auth TOKEN` | Authenticate the connection (§21.11). |

21.7.2 **Bulk input.** A `set -`, `append -` or `delete -` request is followed by
record lines in the protocol encoding, handled as §20.3 handles standard input,
and ended by a line consisting of exactly `#`. They are appended as a single
batch. Nothing is written when the connection ends before that line.

21.7.3 `setdefault` with a marker key is a usage error (status 2).

### 21.8 Consistency

21.8.1 A response reflects every write the handler acknowledged before it received
the request, and every record committed to the store's files (§4.3) before it
received the request, subject to the file system's visibility rules.

21.8.2 `pop`, `popitem` and `setdefault` are atomic: no other request's write is
applied between their read and their write.

21.8.3 Writes received on one connection are applied in the order received.

21.8.4 The handler appends with whole-record writes (§18.2) to one part of the
store, the part its appends go to (§17, §17.7), using the same locks as any other
writer (§18.6).

21.8.5 The handler's state is what a conformant reader returns for the store's
files at that moment. When a part cannot be read, `read`, `get`, `has` and
`verify` answer with status 1 and still return what could be read.

### 21.9 Acknowledgement

21.9.1 While `#_write_ack_#` is `memory` (§18.4), the handler acknowledges a write
once it is in its batch buffer, and SHOULD write the batch without deliberate
delay.

21.9.2 While `#_write_ack_#` is `disk`, or when the request carries `--sync`, the
handler acknowledges a write only after its batch is written and `fsync`'d.

21.9.3 `stop` and a clean shutdown make every acknowledged write durable first.

### 21.10 Errors

21.10.1 A request that does not parse, names an unknown operation or option, or
has the wrong arguments gets status 2, and the connection stays usable.

21.10.2 A handler MAY close a connection whose line exceeds an implementation
limit; the limit MUST be at least 1 MiB.

21.10.3 Any other failure of a request gets status 1 with a message; the handler
keeps serving.

### 21.11 Access

21.11.1 A handler SHOULD by default accept connections only from the user it runs
as, for example through a Unix socket of mode 0600 in a directory only that user
can enter. Wider access MUST be an explicit choice of whoever starts it.

21.11.2 A handler on TCP MUST require authentication: the first line of a
connection is `auth<TAB>TOKEN`, with TOKEN from the pointer file, and the handler
answers `#0`, or answers status 1 and closes the connection. The pointer file's
permissions then govern who may connect.

### 21.12 Extensions

Operations beginning with `x-`, options beginning with `--x-`, and exit statuses
64–125 belong to implementations, as in §20.7.2.

### 21.13 `serve` and `stop`

21.13.1 `serve STORE` starts a handler for STORE in the foreground. It creates a
missing store as `set` does, exits with status 1 when another handler holds the
pointer lock, and exits with status 0 after a clean stop.

21.13.2 `stop STORE` sends `stop` to the handler of STORE and exits with status 0
once it has replied. It exits with status 1 when no handler answers.

---
```

In `tsvz-spec-v1.md`, replace

````markdown
    echo "tsvz failed" >&2
    exit 1
fi
```
````

with

````markdown
    echo "tsvz failed" >&2
    exit 1
fi
```

A handler (§21), driven from the shell:

```
$ tsvz serve people.tsvz &
tsvz: serving people.tsvz at unix:/run/user/1000/tsvz-k3x9/s (pid 41234)
$ tsvz get people.tsvz alice              # answered by the handler
alice	Alice	30
$ printf 'get\talice\nlen\n' | socat - UNIX-CONNECT:/run/user/1000/tsvz-k3x9/s
alice	Alice	30
#0
2
#0
$ tsvz stop people.tsvz
```

From Python, with TSVZ's client:

```python
import TSVZ

with TSVZ.TSVZClient('people.tsvz') as people:
    people['carol'] = ['carol', 'Carol', '25']
    print(people['alice'], len(people))
```
````

- [ ] **Step 2: Bring the design doc in line**

In `docs/superpowers/specs/2026-10-09-tsvz-serve-design.md`, replace

```markdown
**Server lifecycle.** `tsvz serve STORE [--x-idle-timeout S] [--x-group G]
[--x-mode MODE] [--x-header H] [--x-defaults D]`: lock, load, bind, write
pointer, log the address on stderr, serve.
```

with

```markdown
**Server lifecycle.** `tsvz serve STORE [--x-idle-timeout S] [--x-group G]
[--x-mode MODE] [--x-tcp] [--x-header H] [--x-defaults D]`: lock, load, bind,
write pointer, log the address on stderr, serve. `--x-header` and `--x-defaults`
apply only when `serve` creates the store; the served view never uses initial
defaults or 3.39 strict mode, so routed output equals direct output (§20.8).
`--x-tcp` uses the TCP transport on any platform.
```

In `docs/superpowers/specs/2026-10-09-tsvz-serve-design.md`, replace

```markdown
**CLI routing.** In `_cliMain`, before running a store operation (everything but
`serve`), unless `--x-direct`: find the pointer; if live, forward.
```

with

```markdown
**CLI routing.** In `_cliMain`, before running a store operation (everything but
`serve` and `stop`): find the pointer; if live, forward. A command line is
forwarded only when it has none of `--x-direct`, `--x-header`, `--x-defaults`,
`--x-strict`, and, for a loose file, the server's delimiter (the pointer records
it as `x-delimiter`).
```

In `docs/superpowers/specs/2026-10-09-tsvz-serve-design.md`, replace

```markdown
**`TSVZClient(fileName, sync=False, timeout=5, teeLogger=None)`.** A
```

with

```markdown
**`TSVZClient(fileName, sync=False, timeout=None, teeLogger=None)`.** A
```

In `docs/superpowers/specs/2026-10-09-tsvz-serve-design.md`, replace

```markdown
  `del c[k]` uses `pop`, so a missing key raises `KeyError`.
```

with

```markdown
  `del c[k]` writes a tombstone; a missing key is not an error, as in `TSVZed`.
- `timeout` is the time to wait for each answer (None: no limit); connecting
  gives up after 0.5 s.
```

- [ ] **Step 3: Check the documents**

Run: `grep -n "^## 21\.\|^### 21\.\|^### 20\.8\|conformant handler\|^20\.2\.[56]" tsvz-spec-v1.md`
Expected: the §2.1 sentence, `20.2.5`, `20.2.6`, `### 20.8 Routing through a handler`, `## 21. Write handler protocol` and the thirteen `### 21.N` headings in order.

Run: `grep -n "timeout=None\|x-delimiter\|--x-tcp\|is not an error" docs/superpowers/specs/2026-10-09-tsvz-serve-design.md`
Expected: one match each.

- [ ] **Step 4: Commit**

```bash
git add tsvz-spec-v1.md docs/superpowers/specs/2026-10-09-tsvz-serve-design.md
git commit -q -F - <<'EOF'
Specify the tsvz write handler protocol as tsvz-spec-v1 §21.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
EOF
```

---

### Task 2: Library helpers for the engine

**Files:**
- Modify: `TSVZ.py` (reader and writer internals; no behaviour change)
- Modify: `TSVZ_test.py`

**Interfaces:**
- Consumes: existing `_PartInfo`, `_iterPartLines`, `_specReplay`, `_specLoad`, `readTabularFile`, `appendLinesTabularFile`, `_cliStdinBatch`.
- Produces:
  - `_PartInfo.committed` (bytes of committed lines read) and `_PartInfo.lines` (lines replayed, set by `_specReplay`).
  - `_specReplayLine(raw, lineNo, label, path, state, delimiter, reporter) -> (text, record) | None`: one line as `_specReplay` replays it. `_specReplay` keeps its loop inline for speed.
  - `_specPadRow(row, state, width) -> width`.
  - `_legacyReadLoop(file, fileName, taskDic, correctColumnNum, lineNo, strict, delimiter, defaults, storeOffset, encoding, reporter) -> (correctColumnNum, committed, lineNo)`.
  - `_legacyFormatPayload(fileName, linesToAppend, teeLogger, header, verifyHeader, verbose, encoding, strict, delimiter) -> (payload, count)`.
  - `_tryLockFile(f) -> bool`.
  - `_parseRecordLines(texts, delimiter, spec, keysOnly, reporter) -> [(cells, marker), ...]`.

These are refactors that later tasks build on. Every existing test, including the 3.39 differential tests, must keep passing unchanged. `_specReplay` keeps its per-line loop inline: routing every line through `_specReplayLine` measured about 6% slower on a 500 000-line `.tsvz` load, while `_specPadRow` (one call per data row) measured within noise.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ_test.py`, above the final `if __name__ == '__main__':` block:

```python

# ==========================================================================
# Write handler (spec §21): library helpers
# ==========================================================================
def test_part_info_counts_committed_bytes(tmp_path):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, b'a\t1\nb\t2\ntorn')
	info = TSVZ._PartInfo(p)
	reporter = TSVZ._Reporter(p)
	assert [raw for raw, _ in TSVZ._iterPartLines(info, reporter)] == [b'a\t1\n', b'b\t2\n']
	assert (info.committed, info.tail) == (8, b'torn')
	assert TSVZ._specLoad(p, '\t', reporter=TSVZ._Reporter(p)).infos[0].lines == 2


def test_spec_replay_line_matches_spec_replay(tmp_path):
	p = str(tmp_path / 'r.tsvz')
	good = '%08x' % (__import__('zlib').crc32(b'a\t1\n') & 0xffffffff)
	_touch(p, ('\ufeff#_defaults_#\t\tD\n#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\t' + good + '\n'
			   '<#>h\tx<sep>y\nb\n#c\n\xe9\t2\n').encode('utf-8') + b'bad\xff\t3\n')
	whole = TSVZ._SpecState()
	expected = [(text, record) for _, _, text, record in
				TSVZ._specReplay([p], '\t', whole, TSVZ._Reporter(p), [])]
	single = TSVZ._SpecState()
	reporter = TSVZ._Reporter(p)
	got = []
	for lineNo, (raw, _) in enumerate(TSVZ._iterPartLines(TSVZ._PartInfo(p), reporter), 1):
		replayed = TSVZ._specReplayLine(raw, lineNo, '', p, single, '\t', reporter)
		if replayed is not None:
			got.append(replayed)
	assert got == expected
	assert single.defaults == whole.defaults == ['#_defaults_#', '', 'D']
	assert single.mismatches == whole.mismatches == []


def test_spec_pad_row():
	state = TSVZ._SpecState(['#_defaults_#', 'x', 'y'])
	row = ['k', '1']
	assert TSVZ._specPadRow(row, state, -1) == 3 and row == ['k', '1', 'y']
	row = ['k', '1', '2', '3']
	assert TSVZ._specPadRow(row, state, 3) == 3 and row == ['k', '1', '2', '3']


def test_legacy_read_loop_reports_width_and_committed_offset(tmp_path):
	p = str(tmp_path / 'l.tsv')
	_touch(p, b'k\tv\nj\tw\nlast\tx')
	data = OrderedDict()
	with open(p, 'rb') as f:
		width, committed, lines = TSVZ._legacyReadLoop(f, p, data, -1, 0, False, '\t', [], False, 'utf8', TSVZ._Reporter(p))
	assert (width, committed, lines) == (2, 8, 3)
	assert list(data) == ['k', 'j', 'last']  # 3.39 reads an unterminated last line as a record


def test_legacy_format_payload_is_what_append_writes(tmp_path):
	p = str(tmp_path / 'f.tsv')
	rows = [['k', 'a\tb'], ['j'], ['i', 'x', 'y']]
	payload, count = TSVZ._legacyFormatPayload(p, rows, None, [''], True, False, 'utf8', False, '\t')
	TSVZ.appendLinesTabularFile(p, rows, createIfNotExist=True)
	assert count == 3 and open(p, 'rb').read() == b'\n' + payload
	assert payload == b'k\ta<sep>b\t\nj\t\t\ni\tx\ty\n'


@pytest.mark.skipif(os.name != 'posix', reason='flock')
def test_try_lock_file_excludes_a_second_handle(tmp_path):
	p = str(tmp_path / 'x.serve')
	first = open(p, 'a+b')
	second = open(p, 'a+b')
	try:
		assert TSVZ._tryLockFile(first) is True
		assert TSVZ._tryLockFile(second) is False
		open(p, 'rb').close()  # closing another descriptor keeps a flock
		assert TSVZ._tryLockFile(second) is False
	finally:
		first.close()
	assert TSVZ._tryLockFile(second) is True
	second.close()


def test_parse_record_lines():
	reporter = TSVZ._Reporter('<test>')
	texts = ['# comment', '', 'k\tv<sep>w', '<#>_x_#\tdata', '#_defaults_#\t\tD<lt>', '\tno key', 'j']
	assert TSVZ._parseRecordLines(texts, '\t', True, False, reporter) == [
		(['k', 'v\tw'], False), (['#_x_#', 'data'], False), (['#_defaults_#', '', 'D<'], True), (['j'], False)]
	assert TSVZ._parseRecordLines(texts, '\t', True, True, reporter) == [
		(['k'], False), (['#_x_#'], False), (['#_defaults_#'], True), (['j'], False)]
	assert TSVZ._parseRecordLines(['k,a<sep>b', '#c', 'j,x'], ',', False, False, reporter) == [
		(['k', 'a,b'], False), (['j', 'x'], False)]
	assert reporter._events['empty-key'][0] == 2
```

- [ ] **Step 2: Run them to verify they fail**

Run: `python3 -m pytest TSVZ_test.py -q -k "part_info or replay_line or pad_row or legacy_read_loop or format_payload or try_lock or parse_record"`
Expected: 7 FAIL, each with `AttributeError: module 'TSVZ' has no attribute ...` (or `'_PartInfo' object has no attribute 'committed'`).

- [ ] **Step 3: Refactor**

In `TSVZ.py`, replace

```python
	def __init__(self, path):
		self.path = path
		self.codec = _parsePartName(path).codec
		self.tail = b''
		self.damaged = ''
		self.goodRawEnd = 0
```

with

```python
	def __init__(self, path):
		self.path = path
		self.codec = _parsePartName(path).codec
		self.tail = b''
		self.damaged = ''
		self.goodRawEnd = 0
		self.committed = 0  # bytes of committed lines read (decompressed bytes for a compressed part)
		self.lines = 0  # committed lines replayed (set by _specReplay)
```

In `TSVZ.py`, replace

```python
		except OSError as e:
			reporter.note('unreadable', None, 'stopped reading part {} ({})'.format(info.path, e))
			return
	if rest:
		info.tail = rest
```

with

```python
		except OSError as e:
			reporter.note('unreadable', None, 'stopped reading part {} ({})'.format(info.path, e))
			info.committed = offset
			return
	info.committed = offset
	if rest:
		info.tail = rest
```

In `TSVZ.py`, replace

```python
			if algo:
				_specChecksum(state, algo, text, delimiter, reporter, where, (path, lineNo))
				continue
			yield partIndex, offset, text, _specProcessRecord(text, state, delimiter, reporter, where)
```

with

```python
			if algo:
				_specChecksum(state, algo, text, delimiter, reporter, where, (path, lineNo))
				continue
			yield partIndex, offset, text, _specProcessRecord(text, state, delimiter, reporter, where)
		info.lines = lineNo
```

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
def _specReplayLine(raw, lineNo, label, path, state, delimiter, reporter):
	"""Replay one committed line of the part ``path``, as ``_specReplay`` does for each line.

	``raw`` is the line with its b'\\n'; ``lineNo`` counts from 1 within the
	part and ``label`` prefixes locations in reports. Returns ``(text,
	record)`` as ``_specProcessRecord`` sees it, or None for the checksum
	marker of a supported algorithm. (``_specReplay`` keeps this logic inline
	for speed; the two must stay in step.)
	"""
	where = '{}line {}'.format(label, lineNo)
	if lineNo == 1 and raw.startswith(b'\xef\xbb\xbf'):
		raw = raw[3:]
		reporter.note('bom', where, 'stripped a UTF-8 byte order mark')
	text = _decodeLine(raw, reporter, where)
	check = _CHECKSUM_RE.match(text.split(delimiter, 1)[0]) if text.startswith('#_') else None
	algo = check.group(1).lower() if check and _digestSupported(check.group(1).lower()) else None
	for name, digest in state.digests.items():
		if name != algo:
			digest.update(raw)
	if algo:
		_specChecksum(state, algo, text, delimiter, reporter, where, (path, lineNo))
		return None
	return text, _specProcessRecord(text, state, delimiter, reporter, where)
```

```python
def _storeParts(path, reporter=None):
```

In `TSVZ.py`, replace

```python
		key, row = record
		if row is None:
			data.pop(key, None)
			continue
		if width == -1:
			width = len(state.defaults) if len(state.defaults) > 1 else len(row)
		target = max(width, len(state.defaults))
		if len(row) < target:
			bound = state.defaults
			row.extend(bound[j] if j < len(bound) else '' for j in range(len(row), target))
		result.lastLine = (offset, row)
```

with

```python
		key, row = record
		if row is None:
			data.pop(key, None)
			continue
		width = _specPadRow(row, state, width)
		result.lastLine = (offset, row)
```

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
def _specPadRow(row, state, width):
	"""Pad a data row in place to the store width with its row-bound defaults (design §4.7).

	``width`` is the store width so far (-1 before the first data row); the
	width after this row is returned. Nothing is trimmed.
	"""
	if width == -1:
		width = len(state.defaults) if len(state.defaults) > 1 else len(row)
	target = max(width, len(state.defaults))
	if len(row) < target:
		bound = state.defaults
		row.extend(bound[j] if j < len(bound) else '' for j in range(len(row), target))
	return width
```

```python
def _specOptions(fileName, delimiter, encoding, reporter):
```

In `TSVZ.py`, replace

```python
			lines = iter(file)
			while True:
				try:
					line = next(lines)
				except StopIteration:
					break
				except Exception as e:
					# L5: a damaged compressed stream ends the read instead of raising.
					if not _isCompressedFile(fileName):
						raise
					reporter.note('damaged', None, 'compressed stream is damaged ({}: {}); read up to the damage'.format(type(e).__name__, e))
					break
				lineNo += 1
				try:
					text = line.decode(encoding=encoding)
				except UnicodeDecodeError:
					text = line.decode(encoding=encoding,errors='replace')
					reporter.note('decode', 'line {}'.format(lineNo), 'invalid {} replaced with U+FFFD'.format(encoding))
				correctColumnNum, _ = _processLine(text,taskDic,correctColumnNum,strict = strict,delimiter=delimiter,defaults = defaults,storeOffset=storeOffset,offset=file.tell()-len(line),reporter=reporter)
	finally:
		reporter.flush()
	return taskDic
```

with

```python
			_legacyReadLoop(file, fileName, taskDic, correctColumnNum, lineNo, strict, delimiter, defaults,
							storeOffset, encoding, reporter)
	finally:
		reporter.flush()
	return taskDic


def _legacyReadLoop(file, fileName, taskDic, correctColumnNum, lineNo, strict, delimiter, defaults, storeOffset,
					encoding, reporter):
	"""3.39's read loop over the rest of an open legacy file (``readTabularFile``).

	Returns ``(correctColumnNum, committed, lineNo)``: the column count after
	the last line, the offset just past the last line that ends in b'\\n'
	(decompressed bytes for a compressed file) and the number of lines read.
	"""
	committed = file.tell()
	lines = iter(file)
	while True:
		try:
			line = next(lines)
		except StopIteration:
			break
		except Exception as e:
			# L5: a damaged compressed stream ends the read instead of raising.
			if not _isCompressedFile(fileName):
				raise
			reporter.note('damaged', None, 'compressed stream is damaged ({}: {}); read up to the damage'.format(type(e).__name__, e))
			break
		lineNo += 1
		try:
			text = line.decode(encoding=encoding)
		except UnicodeDecodeError:
			text = line.decode(encoding=encoding,errors='replace')
			reporter.note('decode', 'line {}'.format(lineNo), 'invalid {} replaced with U+FFFD'.format(encoding))
		position = file.tell()
		correctColumnNum, _ = _processLine(text,taskDic,correctColumnNum,strict = strict,delimiter=delimiter,defaults = defaults,storeOffset=storeOffset,offset=position-len(line),reporter=reporter)
		if line.endswith(b'\n'):
			committed = position
	return correctColumnNum, committed, lineNo
```

In `TSVZ.py`, replace

```python
	delimiter = get_delimiter(delimiter,file_name=fileName)
	header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger,delimiter=delimiter)
	if not _verifyFileExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict,delimiter=delimiter):
		return
	formatedLines = []
```

with

```python
	delimiter = get_delimiter(delimiter,file_name=fileName)
	header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger,delimiter=delimiter)
	if not _verifyFileExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict,delimiter=delimiter):
		return
	payload, count = _legacyFormatPayload(fileName, linesToAppend, teeLogger, header, verifyHeader, verbose, encoding,
										  strict, delimiter)
	if not count:
		if verbose:
			__teePrintOrNot(f"No lines to append to {fileName}",teeLogger=teeLogger)
		return
	if _isCompressedFile(fileName):
		with openFileAsCompressed(fileName, mode ='ab',encoding=encoding,teeLogger=teeLogger)as file:
			file.write(payload)
	else:
		# L1: a last line without '\n' is a record to the 3.39 reader; terminate it
		# instead of gluing the new record onto it.
		reporter = _Reporter(fileName, teeLogger)
		_lockedAppend(fileName, payload, 'newline', reporter)
		reporter.flush()
	if verbose:
		__teePrintOrNot(f"Appended {count} lines to {fileName}",teeLogger=teeLogger)


def _legacyFormatPayload(fileName, linesToAppend, teeLogger, header, verifyHeader, verbose, encoding, strict, delimiter):
	"""Format rows as 3.39's ``appendLinesTabularFile`` writes them; return ``(payload, count)``.

	``header`` is a formatted header list and ``delimiter`` a resolved
	delimiter. Rows are sanitised and padded or cut to the column count: the
	header's when the file's first line matches it, else the longest row's.
	"""
	formatedLines = []
```

In `TSVZ.py`, replace

```python
	if not formatedLines:
		if verbose:
			__teePrintOrNot(f"No lines to append to {fileName}",teeLogger=teeLogger)
		return
	correctColumnNum = max([len(line) for line in formatedLines])
```

with

```python
	if not formatedLines:
		return b'', 0
	correctColumnNum = max([len(line) for line in formatedLines])
```

In `TSVZ.py`, replace

```python
	payload = b'\n'.join([delimiter.join(line).encode(encoding=encoding,errors='replace') for line in formatedLines]) + b'\n'
	if _isCompressedFile(fileName):
		with openFileAsCompressed(fileName, mode ='ab',encoding=encoding,teeLogger=teeLogger)as file:
			file.write(payload)
	else:
		# L1: a last line without '\n' is a record to the 3.39 reader; terminate it
		# instead of gluing the new record onto it.
		reporter = _Reporter(fileName, teeLogger)
		_lockedAppend(fileName, payload, 'newline', reporter)
		reporter.flush()
	if verbose:
		__teePrintOrNot(f"Appended {len(formatedLines)} lines to {fileName}",teeLogger=teeLogger)
```

with

```python
	payload = b'\n'.join([delimiter.join(line).encode(encoding=encoding,errors='replace') for line in formatedLines]) + b'\n'
	return payload, len(formatedLines)
```

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
def _tryLockFile(f):
	"""Take an exclusive whole-file lock without waiting; return False when another handle holds it.

	POSIX uses ``flock``: it belongs to the open file description, so closing
	another descriptor of the same file in this process does not drop it.
	Windows locks are mandatory, so there one byte far past the content is
	locked and readers can still read the file.
	"""
	try:
		if os.name == 'posix':
			fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
		elif os.name == 'nt':
			os.lseek(f.fileno(), 1 << 30, os.SEEK_SET)
			msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
		return True
	except OSError:
		return False
```

```python
def _unlockFile(f):
```

In `TSVZ.py`, replace

```python
	spec = _isSpecPath(args.store)
	source = _Reporter('<stdin>', logger)
	batch = []
	try:
		for lineNo, text in enumerate(_cliStdinLines(stdin, source), 1):
			fields = text.split(delimiter)
			if keysOnly:
				fields = fields[:1]
			first = fields[0]
			if _MARKER_RE.match(first):
				batch.append(delimiter.join(fields) if spec else _unsanitize(fields, delimiter))
				continue
			if first.startswith('#') or not text:
				continue
			cells = [_specDecodeField(field, delimiter) for field in fields] if spec else _unsanitize(fields, delimiter)
			if not cells[0]:
				source.note('empty-key', 'line {}'.format(lineNo), 'skipped a line with an empty key')
				continue
			batch.append(_specFormatRecord(cells, delimiter) if spec else cells)
	finally:
		source.flush()
	return batch
```

with

```python
	spec = _isSpecPath(args.store)
	source = _Reporter('<stdin>', logger)
	try:
		rows = _parseRecordLines(_cliStdinLines(stdin, source), delimiter, spec, keysOnly, source)
	finally:
		source.flush()
	if not spec:
		return [cells for cells, marker in rows]
	return [_specFormatRecord(cells, delimiter, marker=marker) for cells, marker in rows]


def _parseRecordLines(texts, delimiter, spec, keysOnly, reporter):
	"""Parse record lines of a stream (spec §20.3) into ``[(cells, marker), ...]``.

	A line whose first field matches the reserved pattern is a marker line
	(``marker`` True, key kept as written). Other fields are decoded per §13
	for a spec store, by 3.39's rules otherwise. Comment lines and empty lines
	are skipped, and so is a line with an empty key (reported on
	``reporter``). ``keysOnly`` keeps the first field only.
	"""
	rows = []
	for lineNo, text in enumerate(texts, 1):
		fields = text.split(delimiter)
		if keysOnly:
			fields = fields[:1]
		first = fields[0]
		marker = bool(_MARKER_RE.match(first))
		if not marker and (first.startswith('#') or not text):
			continue
		if spec:
			cells = [first] + [_specDecodeField(field, delimiter) for field in fields[1:]] if marker else \
				[_specDecodeField(field, delimiter) for field in fields]
		else:
			cells = _unsanitize(fields, delimiter)
		if not cells[0]:
			reporter.note('empty-key', 'line {}'.format(lineNo), 'skipped a line with an empty key')
			continue
		rows.append((cells, marker))
	return rows
```

- [ ] **Step 4: Run the tests**

Run: `python3 -m pytest TSVZ_test.py -q`
Expected: all pass.


- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_test.py
git commit -q -F - <<'EOF'
Expose committed offsets, single-line replay and 3.39 formatting for the write handler.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
EOF
```

---

### Task 3: The store engine (`_ServeStore`, `_ServeWriter`)

**Files:**
- Modify: `TSVZ.py` (new block directly above the CLI block; `_cliParts` uses `_partRows`)
- Modify: `TSVZ_test.py`

**Interfaces:**
- Consumes: Task 2's helpers; `_specLoad`, `_storeParts`, `_specStamp`, `_dirMtimeNs`, `_specEnsureStore`, `_verifyFileExistence`, `_specAppendPayload`, `_lockedAppend`, `_specClearTabularFile`, `_specScrub`, `clearTabularFile`, `scrubTabularFile`, `_specVerify`.
- Produces:
  - `_partRows(store, reporter) -> rows`, shared with `_cliParts`.
  - `_ServeWriter(store)`: `submit(payload, durable) -> seq`, `wait(seq, durable=False)` (raises the batch's write error), `barrier()`, `close()`, `failures`.
  - `_ServeStore(fileName, delimiter=..., header='', defaults=None, strict=False, teeLogger=None, verbose=False, create=False, createDefaults=None)`, with:
    - attributes: `path`, `spec`, `delimiter`, `data` (OrderedDict key -> row), `unreadable`, `writer`, `teeLogger`;
    - reading: `reload(log)`, `sync(log)`, `resolve(key)`;
    - reads: `read(log) -> rows`, `get(keys, log) -> (rows, missing)`, `has(key, log)`, `length(log)`, `keys(log)`, `verify(log) -> (mismatches, readable)`, `partRows(log)`;
    - writes: `write(rows, sync, log)` with `rows = [(cells, marker), ...]`, `pop(key, sync, log) -> row|None`, `popitem(last, sync, log) -> row|None`, `setdefault(cells, sync, log) -> row|None`, `clear(log) -> bool`, `scrub(log) -> bool`;
    - lifecycle: `flush()`, `close()`.
  - `log` is any teeLogger (`.teelog(message, level)`) or None.

**The rule the engine keeps:** its view is always a replay of the store's files. Writes are formatted exactly as the direct writers format them, appended by the writer thread, and read back by `sync()` like anyone else's appends. Every public method takes the state lock; the writer thread never does, so a read can wait for queued writes (`barrier`) while holding the lock.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ_test.py`, above the final `if __name__ == '__main__':` block:

```python

# ==========================================================================
# Write handler (spec §21): the store engine
# ==========================================================================
def _view(path):
	"""Rows a direct reader returns for ``path``, in order (what ``tsvz read`` prints)."""
	return [list(row) for row in TSVZ.readTabularFile(path, verifyHeader=False, strict=False).values()]


@pytest.fixture
def engines():
	"""Open ``_ServeStore``s through this fixture so their writer threads stop."""
	opened = []

	def make(path, **kwargs):
		store = TSVZ._ServeStore(path, **kwargs)
		opened.append(store)
		return store
	yield make
	for store in opened:
		store.close()


@pytest.mark.parametrize('name', ['e.tsvz', 'e.psvz', 'e.tsv', 'e.csv'])
def test_engine_follows_direct_appends(tmp_path, engines, name):
	p = str(tmp_path / name)
	d = TSVZ._EXTENSION_DELIMITERS[name.rpartition('.')[2]].encode()
	_touch(p, b'a' + d + b'1\nb' + d + b'2\n')
	store = engines(p)
	assert store.read(None) == _view(p) == [['a', '1'], ['b', '2']]
	TSVZ.appendLinesTabularFile(p, [['c', '3'], ['a']])
	assert store.read(None) == _view(p) == [['b', '2'], ['c', '3']]
	TSVZ.appendLinesTabularFile(p, [['a', '9'], ['b', 'x' + d.decode() + 'y']])
	assert store.read(None) == _view(p)
	assert store.keys(None) == ['b', 'c', 'a'] and store.length(None) == 3
	assert store.has('a', None) and not store.has('z', None)


def test_engine_ignores_a_torn_tail_until_it_is_completed(tmp_path, engines, capsys):
	p = str(tmp_path / 't.tsvz')
	_touch(p, b'a\t1\n')
	store = engines(p)
	with open(p, 'ab') as f:
		f.write(b'b\t2')
	assert store.read(None) == [['a', '1']]
	assert store.read(None) == [['a', '1']]
	assert capsys.readouterr().err.count('ignored uncommitted bytes') == 1  # noted once, not per request
	with open(p, 'ab') as f:
		f.write(b'2\n')
	assert store.read(None) == _view(p) == [['a', '1'], ['b', '22']]


def test_engine_reloads_when_the_store_changes_other_than_by_appending(tmp_path, engines):
	p = str(tmp_path / 'r.tsvz')
	_touch(p, b'a\t1\nb\t2\na\n')
	store = engines(p)
	assert store.read(None) == [['b', '2']]
	TSVZ.scrubTabularFile(p, verifyHeader=False)  # shrinks the file in place
	TSVZ.appendTabularFile(p, ['c', '3'])
	assert store.read(None) == _view(p) == [['b', '2'], ['c', '3']]
	other = str(tmp_path / 'other')
	_touch(other, b'x\t1\ny\t2\n')
	os.replace(other, p)  # a new inode
	assert store.read(None) == [['x', '1'], ['y', '2']]
	with open(p, 'r+b') as f:  # rewritten in place, same size
		f.write(b'q\t7\nz\t8\n')
	st = os.stat(p)  # file times can be coarser than these steps; make the rewrite visible to stat()
	os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 10 ** 9))
	assert store.read(None) == [['q', '7'], ['z', '8']]
	_touch(p + '.1', b'n\t1\n')  # a new part
	assert store.read(None) == _view(p) == [['q', '7'], ['z', '8'], ['n', '1']]


def test_engine_follows_compressed_and_multi_part_stores(tmp_path, engines):
	g = str(tmp_path / 'g.tsvz.gz')
	_touch(g, gzip.compress(b'a\t1\n'))
	store = engines(g)
	TSVZ.appendTabularFile(g, ['b', '2'])
	assert store.read(None) == _view(g) == [['a', '1'], ['b', '2']]
	m = str(tmp_path / 'm.tsvz')
	_touch(m, b'a\t1\n')
	_touch(m + '.1', b'b\t2\n')
	store = engines(m)
	TSVZ.appendTabularFile(m, ['c', '3'])  # goes to the active part, m.tsvz.1
	assert store.read(None) == _view(m) == [['a', '1'], ['b', '2'], ['c', '3']]
	assert open(m + '.1', 'rb').read() == b'b\t2\nc\t3\n'


def test_engine_sees_a_new_part_within_one_directory_timestamp(tmp_path, engines, monkeypatch):
	"""A part created in the same timestamp tick as the last directory change is still found."""
	m = str(tmp_path / 'n.tsvz')
	_touch(m, b'a\t1\n')
	frozen = TSVZ._dirMtimeNs(m)
	monkeypatch.setattr(TSVZ, '_dirMtimeNs', lambda path: frozen)  # the new part leaves the mtime as it was
	store = engines(m)
	_touch(m + '.1', b'b\t2\n')
	assert store.read(None) == [['a', '1'], ['b', '2']]

def test_engine_get_resolves_keys_and_missing_rows(tmp_path, engines):
	p = str(tmp_path / 'g.tsvz')
	_touch(p, b'#_defaults_#\t\tNA\nalice\tAlice\t30\nbob\tBob\n')
	store = engines(p)
	assert store.get(['bob', 'alice  ', 'carol'], None) == ([['bob', 'Bob', 'NA'], ['alice', 'Alice', '30'],
															['carol', '', 'NA']], True)
	assert store.get(['alice'], None) == ([['alice', 'Alice', '30']], False)


def test_engine_writes_what_the_direct_writers_write(tmp_path, engines):
	for name in ('w.tsvz', 'w.tsv'):
		direct, served = str(tmp_path / ('d' + name)), str(tmp_path / ('s' + name))
		store = engines(served, create=True)
		_touch(direct, open(served, 'rb').read())
		rows = [(['k', 'a\tb', ''], False), (['#tag', 'x<y'], False), (['#_defaults_#', '', 'D'], True), (['gone'], False)]
		store.write(rows, False, None)
		TSVZ.appendLinesTabularFile(direct, [cells for cells, marker in rows])
		assert store.read(None) == _view(direct)  # read-your-writes: the read waits for the queued batch
		assert open(served, 'rb').read() == open(direct, 'rb').read()


def test_engine_acknowledges_disk_writes_after_fsync(tmp_path, engines, monkeypatch):
	synced = []
	real = os.fsync
	monkeypatch.setattr(os, 'fsync', lambda fd: (synced.append(fd), real(fd)))
	p = str(tmp_path / 'a.tsvz')
	_touch(p, b'a\t1\n')
	store = engines(p)
	store.write([(['b', '2'], False)], False, None)  # memory: no fsync needed to answer
	store.write([(['c', '3'], False)], True, None)  # --sync
	assert synced
	synced[:] = []
	store.write([(['#_write_ack_#', 'disk'], True)], False, None)
	store.read(None)
	store.write([(['d', '4'], False)], False, None)
	assert synced  # the store's own marker asks for disk


def test_engine_pop_popitem_and_setdefault(tmp_path, engines):
	p = str(tmp_path / 'p.tsvz')
	_touch(p, b'a\t1\nb\t2\nc\t3\n')
	store = engines(p)
	assert store.pop('b', False, None) == ['b', '2'] and store.pop('b', False, None) is None
	assert store.popitem(True, False, None) == ['c', '3'] and store.popitem(False, False, None) == ['a', '1']
	assert store.popitem(True, False, None) is None
	assert store.setdefault(['k', 'v'], False, None) == ['k', 'v']
	assert store.setdefault(['k', 'other'], False, None) == ['k', 'v']
	assert open(p, 'rb').read() == b'a\t1\nb\t2\nc\t3\nb\nc\na\nk\tv\n'


def test_engine_pop_is_atomic_under_concurrency(tmp_path, engines):
	p = str(tmp_path / 'race.tsvz')
	_touch(p, b''.join(b'k%d\t%d\n' % (i, i) for i in range(200)))
	store = engines(p)
	popped = []

	def worker():
		while True:
			row = store.popitem(True, False, None)
			if row is None:
				return
			popped.append(row[0])
	threads = [threading.Thread(target=worker) for _ in range(8)]
	for t in threads:
		t.start()
	for t in threads:
		t.join()
	assert sorted(popped) == sorted('k%d' % i for i in range(200))
	assert store.read(None) == [] == _view(p)


def test_engine_clear_scrub_verify_and_parts(tmp_path, engines):
	p = str(tmp_path / 'c.tsvz')
	_touch(p, b'#id\tval\na\t1\nb\t2\na\n')
	store = engines(p)
	assert store.scrub(None) is True and open(p, 'rb').read() == b'#id\tval\n#_version_#\t1\nb\t2\n'
	assert store.read(None) == [['b', '2']]
	assert store.clear(None) is True and store.read(None) == []
	m = str(tmp_path / 'm.tsvz')
	_touch(m, b'a\t1\n')
	_touch(m + '.1', b'#_checksum_crc32_#\nb\t2\n#_checksum_crc32_#\t00000000\n')
	store = engines(m)
	assert store.scrub(None) is False  # 4.1 refuses to compact a multi-part store
	mismatches, readable = store.verify(None)
	assert readable and [entry[:3] for entry in mismatches] == [(m + '.1', 3, 'crc32')]
	assert store.partRows(None) == [['0', '', m, ''], ['1', '1', m + '.1', 'active']]


def test_engine_reports_an_unreadable_part(tmp_path, engines):
	m = str(tmp_path / 'u.tsvz')
	_touch(m, b'a\t1\n')
	os.mkdir(m + '.1')
	store = engines(m)
	assert store.unreadable and store.read(None) == [['a', '1']]


def test_engine_close_writes_and_fsyncs_queued_writes(tmp_path, monkeypatch):
	synced = []
	real = os.fsync
	monkeypatch.setattr(os, 'fsync', lambda fd: (synced.append(fd), real(fd)))
	p = str(tmp_path / 'q.tsvz')
	store = TSVZ._ServeStore(p, create=True)
	for i in range(50):
		store.write([(['k%d' % i, 'v'], False)], False, None)
	store.close()
	assert synced and _view(p) == [['k%d' % i, 'v'] for i in range(50)]

def test_engine_survives_the_store_being_deleted(tmp_path, engines):
	"""Review focus 4: a store removed under the server is recreated by the next write."""
	p = str(tmp_path / 'x.tsvz')
	_touch(p, b'a\t1\n')
	store = engines(p)
	os.remove(p)
	assert store.read(None) == []
	store.write([(['b', '2'], False)], False, None)
	assert store.read(None) == _view(p) == [['b', '2']] and open(p, 'rb').read() == b'b\t2\n'
```

- [ ] **Step 2: Run them to verify they fail**

Run: `python3 -m pytest TSVZ_test.py -q -k engine`
Expected: every test FAILs with `AttributeError: module 'TSVZ' has no attribute '_ServeStore'`.

- [ ] **Step 3: Add the engine**

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
# ===========================================================================
# Write handler (tsvz-spec-v1 §21): the store engine
# ===========================================================================
#: Bytes before the read offset that must be unchanged for an incremental read.
_SERVE_CHECK_BYTES = 64
#: A directory modified less than this long ago may change again within the same
#: timestamp tick, so its part list is checked on every sync until it is older.
_SERVE_RACY_SECONDS = 2.0


def _partRows(store, reporter):
	"""Rows for ``parts`` (spec §20.2): index, ordinal as the file name spells it, path, flags."""
	if _isSpecPath(store):
		parts, active = _storeParts(store, reporter)
	else:
		parts, active = [store], store  # a loose file is its only part (spec §20.2.4)
	rows = []
	for index, path in enumerate(parts):
		name = _parsePartName(path)
		ordinal = ''
		if name.ordinal is not None:
			base = os.path.basename(path)
			if name.codec:
				base = base[:-len(name.codec) - 1]
			ordinal = base.rpartition('.')[2]
		flags = (['active'] if path == active else []) + ([name.codec] if name.codec else [])
		rows.append([str(index), ordinal, path, ','.join(flags)])
	return rows


class _ServeWriter(object):
	"""The single appender of a store (spec §18.3): each batch of queued payloads is one write.

	``submit`` returns a sequence number; ``wait`` blocks until that payload
	is written (or written and fsync'd). The thread never takes the store's
	state lock.
	"""

	def __init__(self, store):
		self.store = store
		self.cond = threading.Condition()
		self.queue = []
		self.submitted = 0
		self.written = 0
		self.durable = 0
		self.failures = deque(maxlen=64)
		self.unsynced = False
		self.closing = False
		self.thread = threading.Thread(target=self._run, name='tsvz-writer')
		self.thread.daemon = True
		self.thread.start()

	def submit(self, payload, durable):
		with self.cond:
			if self.closing:
				raise RuntimeError('the store is closing')
			self.submitted += 1
			self.queue.append((self.submitted, payload, durable))
			self.cond.notify_all()
			return self.submitted

	def wait(self, seq, durable=False):
		"""Block until payload ``seq`` is written (``durable``: and fsync'd); raise its write error."""
		with self.cond:
			while (self.durable if durable else self.written) < seq:
				self.cond.wait()
			for first, last, error in self.failures:
				if first <= seq <= last:
					raise error

	def barrier(self):
		"""Block until everything submitted so far is written (spec §21.8 read-your-writes)."""
		with self.cond:
			while self.written < self.submitted:
				self.cond.wait()

	def close(self):
		"""Write everything queued, fsync it, and stop the thread."""
		with self.cond:
			self.closing = True
			self.cond.notify_all()
		self.thread.join()

	def _run(self):
		while True:
			with self.cond:
				while not self.queue and not self.closing:
					self.cond.wait()
				if not self.queue:
					break
				batch, self.queue = self.queue, []
			durable = any(entry[2] for entry in batch)
			error = None
			try:
				self.store._append(b''.join(entry[1] for entry in batch), durable)
			except Exception as e:
				error = e
				_warn('TSVZ error: {}: a batch of {} writes failed: {}'.format(self.store.path, len(batch), e),
					  self.store.teeLogger)
			with self.cond:
				last = batch[-1][0]
				self.written = last
				if durable or error is not None:
					self.durable = last
				if error is not None:
					self.failures.append((batch[0][0], last, error))
				elif not durable:
					self.unsynced = True
				self.cond.notify_all()
		if self.unsynced:
			try:
				self.store._fsync()
			except Exception as e:
				_warn('TSVZ error: {}: fsync failed: {}'.format(self.store.path, e), self.store.teeLogger)


class _ServeStore(object):
	"""One store's live view for the write handler (spec §21, design §3).

	``data`` maps each live key to its resolved row, in first-appearance
	order, and is always a replay of the store's files: writes are appended
	through ``_ServeWriter`` and come back through ``sync()`` like any other
	writer's. ``sync()`` replays the newly committed bytes of the active part
	and reloads everything when anything else changed. Every public method
	takes ``self.lock``; ``log`` is the teeLogger its messages go to.
	"""

	def __init__(self, fileName, delimiter=..., header='', defaults=None, strict=False, teeLogger=None,
				 verbose=False, create=False, createDefaults=None):
		self.path = fileName
		self.spec = _isSpecPath(fileName)
		if self.spec:
			self.delimiter = _EXTENSION_DELIMITERS[_parsePartName(fileName).ext]
		else:
			self.delimiter = get_delimiter(delimiter, file_name=fileName)
		self.defaults = list(defaults) if defaults else []
		self.strict = strict
		self.teeLogger = teeLogger
		self.verbose = verbose
		self.lock = threading.RLock()
		self.data = OrderedDict()
		self.unreadable = False
		if create:
			self._create(header, self.defaults if createDefaults is None else createDefaults)
		self.reload(teeLogger)
		self.writer = _ServeWriter(self)

	def _create(self, header, defaults):
		header = _formatHeader(header, teeLogger=self.teeLogger, delimiter=self.delimiter)
		if self.spec:
			_specEnsureStore(self.path, True, header, _normalizeDefaults(defaults, self.delimiter), False,
							 self.teeLogger, self.delimiter)
		else:
			_verifyFileExistence(self.path, createIfNotExist=True, teeLogger=self.teeLogger, header=header,
								 strict=False, delimiter=self.delimiter)

	# -- reading the files ---------------------------------------------------
	def reload(self, log):
		"""Replay the whole store."""
		with self.lock:
			reporter = _Reporter(self.path, log)
			try:
				self._noteDirMtime(_dirMtimeNs(self.path))
				if self.spec:
					self._loadSpec(reporter, log)
				else:
					self._loadLegacy(reporter)
				self.unreadable = reporter.has('unreadable')
			finally:
				reporter.flush()

	def _noteDirMtime(self, dirMtime):
		self.dirMtime = dirMtime
		self.dirRacy = time.time() - dirMtime / 1e9 < _SERVE_RACY_SECONDS

	def _stamps(self, parts):
		stamps = {}
		for path in parts:
			try:
				stamps[path] = _specStamp(os.stat(path))
			except OSError:
				stamps[path] = None
		return stamps

	def _loadSpec(self, reporter, log):
		self.parts = _storeParts(self.path)[0]
		self.stamps = self._stamps(self.parts)
		load = _specLoad(self.path, self.delimiter, defaults=_normalizeDefaults(self.defaults, self.delimiter),
						 strict=self.strict, taskDic=OrderedDict(), reporter=reporter, teeLogger=log,
						 verbose=self.verbose)
		self.data, self.state, self.width = load.data, load.state, load.correctColumnNum
		if load.parts != self.parts:  # the part list changed while it was read
			self.parts = load.parts
			self.stamps = {}
		info = load.infos[-1] if load.infos else None
		self.offset = info.committed if info is not None else 0
		self.lineNo = info.lines if info is not None else 0
		self.tailSeen = info.tail if info is not None else b''
		self.label = os.path.basename(self.parts[-1]) + ' ' if len(self.parts) > 1 else ''
		self.check = self._readCheck()

	def _loadLegacy(self, reporter):
		self.parts = [self.path] if os.path.isfile(self.path) else []
		self.stamps = self._stamps(self.parts)
		self.data = OrderedDict()
		self.legacyDefaults = list(self.defaults)
		self.width, self.offset, self.lineNo, self.tailSeen = -1, 0, 0, b''
		if self.parts:
			try:
				with openFileAsCompressed(self.path, mode='rb', teeLogger=self.teeLogger) as f:
					self.width, self.offset, self.lineNo = _legacyReadLoop(
						f, self.path, self.data, -1, 0, self.strict, self.delimiter, self.legacyDefaults, False,
						'utf8', reporter)
			except OSError as e:
				reporter.note('unreadable', None, 'could not read {} ({})'.format(self.path, e))
		self.label = ''
		self.check = self._readCheck()

	def _readCheck(self):
		"""The bytes just before the read offset of the active part ('' when compressed or empty)."""
		if not self.parts or _parsePartName(self.parts[-1]).codec or not self.offset:
			return b''
		try:
			with open(self.parts[-1], 'rb') as f:
				start = max(0, self.offset - _SERVE_CHECK_BYTES)
				f.seek(start)
				return f.read(self.offset - start)
		except OSError:
			return b''

	def sync(self, log):
		"""Bring the view up to date with the files: follow appends, reload on anything else."""
		with self.lock:
			dirMtime = _dirMtimeNs(self.path)
			if dirMtime != self.dirMtime or self.dirRacy:
				parts = _storeParts(self.path)[0] if self.spec else ([self.path] if os.path.isfile(self.path) else [])
				if parts != self.parts:
					return self.reload(log)
				self._noteDirMtime(dirMtime)
			for path in self.parts:
				try:
					stamp = _specStamp(os.stat(path))
				except OSError:
					return self.reload(log)
				if stamp == self.stamps.get(path):
					continue
				if path != self.parts[-1] or not self._follow(path, stamp, log):
					return self.reload(log)

	def _follow(self, path, stamp, log):
		"""Replay what was appended to the active part; False when it changed some other way."""
		old = self.stamps.get(path)
		if _parsePartName(path).codec or old is None or stamp[0] != old[0] or stamp[1] < self.offset:
			return False
		try:
			with open(path, 'rb') as f:
				f.seek(self.offset - len(self.check))
				if f.read(len(self.check)) != self.check:
					return False
				data = f.read()
		except OSError:
			return False
		end = data.rfind(b'\n') + 1
		reporter = _Reporter(self.path, log)
		try:
			if end:
				for raw in data[:end].split(b'\n')[:-1]:
					self._apply(raw + b'\n', reporter)
			tail = data[end:]
			if tail and tail != self.tailSeen:
				reporter.note('tail', None, 'ignored uncommitted bytes after the last newline: {!r}'.format(tail[:80]))
			self.tailSeen = tail
		finally:
			reporter.flush()
		self.offset += end
		self.check = (self.check + data[:end])[-_SERVE_CHECK_BYTES:]
		self.stamps[path] = (stamp[0], self.offset + len(data) - end, stamp[2])
		return True

	def _apply(self, raw, reporter):
		"""Replay one appended committed line of the active part into the view."""
		self.lineNo += 1
		if not self.spec:
			try:
				text = raw.decode('utf8')
			except UnicodeDecodeError:
				text = raw.decode('utf8', errors='replace')
				reporter.note('decode', 'line {}'.format(self.lineNo), 'invalid utf8 replaced with U+FFFD')
			self.width, _ = _processLine(text, self.data, self.width, strict=self.strict, delimiter=self.delimiter,
										 defaults=self.legacyDefaults, reporter=reporter)
			return
		replayed = _specReplayLine(raw, self.lineNo, self.label, self.parts[-1], self.state, self.delimiter,
								   reporter)
		if replayed is None or replayed[1] is None:
			return
		key, row = replayed[1]
		if row is None:
			self.data.pop(key, None)
			return
		self.width = _specPadRow(row, self.state, self.width)
		self.data[key] = row

	def _fresh(self, log):
		"""Read-your-writes (spec §21.8): wait for queued writes, then follow the files."""
		self.writer.barrier()
		self.sync(log)

	# -- reads ----------------------------------------------------------------
	def resolve(self, key):
		"""A requested key as a reader resolves it (spec §7.7; 3.39 rstrip for loose files)."""
		if self.spec:
			return key.rstrip(' \t') if self.state.strip else key
		return key.rstrip()

	def read(self, log):
		with self.lock:
			self._fresh(log)
			return [list(row) for row in self.data.values()]

	def get(self, keys, log):
		"""Rows for ``keys`` and whether one was missing (spec §20.2 ``get``, §14.5)."""
		with self.lock:
			self._fresh(log)
			rows = []
			missing = False
			for key in keys:
				key = self.resolve(key)
				row = self.data.get(key)
				if row is None:
					missing = True
					if not (self.spec and self.state.returnDefaults):
						continue
					row = [key] + list(self.state.defaults[1:])
					row += [''] * (self.width - len(row))
				rows.append(list(row))
			return rows, missing

	def has(self, key, log):
		with self.lock:
			self._fresh(log)
			return self.resolve(key) in self.data

	def length(self, log):
		with self.lock:
			self._fresh(log)
			return len(self.data)

	def keys(self, log):
		with self.lock:
			self._fresh(log)
			return list(self.data)

	def verify(self, log):
		"""Checksum mismatches (spec §15) and whether every part could be read."""
		with self.lock:
			self.writer.barrier()
			if not self.spec:
				return [], True
			reporter = _Reporter(self.path, log)
			try:
				mismatches = _specVerify(self.path, self.delimiter, reporter)
				return mismatches, not reporter.has('unreadable')
			finally:
				reporter.flush()

	def partRows(self, log):
		with self.lock:
			self.writer.barrier()
			reporter = _Reporter(self.path, log)
			try:
				return _partRows(self.path, reporter)
			finally:
				reporter.flush()

	# -- writes ---------------------------------------------------------------
	def _payload(self, rows, log):
		"""Bytes for ``rows`` (``[(cells, marker), ...]``), formatted as the direct writers format them."""
		if self.spec:
			return ''.join(_specFormatRecord(cells, self.delimiter, marker=marker) + '\n'
						   for cells, marker in rows).encode('utf-8')
		return _legacyFormatPayload(self.path, [cells for cells, marker in rows], log, [''], True, self.verbose,
									'utf8', self.strict, self.delimiter)[0]

	def _durable(self, sync):
		return sync or (self.spec and self.state.writeAck == 'disk')

	def _submit(self, rows, sync, log):
		"""Queue ``rows`` (caller holds the lock); return ``(seq, durable)`` or None when there is nothing to write."""
		payload = self._payload(rows, log)
		if not payload:
			return None
		durable = self._durable(sync)
		return self.writer.submit(payload, durable), durable

	def _ack(self, queued):
		"""Acknowledge per spec §21.9: at once for ``memory``, after fsync for ``disk``."""
		if queued is not None and queued[1]:
			self.writer.wait(queued[0], durable=True)

	def write(self, rows, sync, log):
		"""Append ``rows`` (``[(cells, marker), ...]``) as one batch: ``set``, ``delete`` and bulk input."""
		with self.lock:
			queued = self._submit(rows, sync, log)
		self._ack(queued)

	def pop(self, key, sync, log):
		"""Atomically return the row of ``key`` and append its tombstone; None when it is missing."""
		with self.lock:
			self._fresh(log)
			key = self.resolve(key)
			row = self.data.get(key)
			if row is None:
				return None
			row = list(row)
			queued = self._submit([([key], False)], sync, log)
		self._ack(queued)
		return row

	def popitem(self, last, sync, log):
		"""Atomically return the row of the last (or first) live key and delete it; None when empty."""
		with self.lock:
			self._fresh(log)
			if not self.data:
				return None
			key = next(reversed(self.data)) if last else next(iter(self.data))
			row = list(self.data[key])
			queued = self._submit([([key], False)], sync, log)
		self._ack(queued)
		return row

	def setdefault(self, cells, sync, log):
		"""Atomically return the row of ``cells[0]``, appending ``cells`` first when the key is missing."""
		with self.lock:
			self._fresh(log)
			key = self.resolve(cells[0])
			row = self.data.get(key)
			if row is not None:
				return list(row)
			queued = self._submit([(cells, False)], sync, log)
			if queued is not None:
				self.writer.wait(queued[0])
			self.sync(log)
			row = self.data.get(key)
			row = list(row) if row is not None else None
		self._ack(queued)
		return row

	def clear(self, log):
		"""``clear`` (spec §20.2) through the library; True when the store was cleared."""
		with self.lock:
			self.writer.barrier()
			try:
				if self.spec:
					return _specClearTabularFile(self.path, teeLogger=log, verbose=self.verbose,
												 delimiter=self.delimiter)
				clearTabularFile(self.path, teeLogger=log, verbose=self.verbose, delimiter=self.delimiter)
				return True
			finally:
				self.reload(log)

	def scrub(self, log):
		"""``scrub`` (spec §20.2) through the library; True when it was not refused."""
		with self.lock:
			self.writer.barrier()
			try:
				if self.spec:
					return _specScrub(self.path, teeLogger=log, verifyHeader=False, verbose=self.verbose,
									  strict=self.strict, delimiter=self.delimiter, defaults=self.defaults)[1]
				scrubTabularFile(self.path, teeLogger=log, verifyHeader=False, verbose=self.verbose,
								 strict=self.strict, delimiter=self.delimiter, defaults=self.defaults)
				return True
			finally:
				self.reload(log)

	def flush(self):
		"""Write and fsync everything submitted so far (``stop``, spec §21.9)."""
		self.writer.barrier()
		with self.lock:
			self._fsync()

	def close(self):
		"""Write and fsync everything queued and stop the writer."""
		self.writer.close()

	# -- called by the writer thread -----------------------------------------------
	def _append(self, payload, fsync):
		reporter = _Reporter(self.path, self.teeLogger)
		try:
			if self.spec:
				_specAppendPayload(_storeParts(self.path)[1], payload, reporter, fsync=fsync)
			elif _isCompressedFile(self.path):
				with openFileAsCompressed(self.path, mode='ab', teeLogger=self.teeLogger) as f:
					f.write(payload)
			else:
				_lockedAppend(self.path, payload, 'newline', reporter, fsync=fsync)
		finally:
			reporter.flush()

	def _fsync(self):
		path = _storeParts(self.path)[1] if self.spec else self.path
		if os.path.exists(path):
			with open(path, 'ab') as f:
				os.fsync(f.fileno())
```

```python
# ===========================================================================
# Command-line interface (tsvz-spec-v1 §20)
```

Then make `_cliParts` use `_partRows`:

In `TSVZ.py`, replace

```python
	if not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	if _isSpecPath(args.store):
		reporter = _Reporter(args.store, logger)
		try:
			parts, active = _storeParts(args.store, reporter)
		finally:
			reporter.flush()
	else:
		parts, active = [args.store], args.store  # a loose file is its only part (spec §20.2.4)
	rows = []
	for index, path in enumerate(parts):
		name = _parsePartName(path)
		ordinal = ''
		if name.ordinal is not None:
			base = os.path.basename(path)
			if name.codec:
				base = base[:-len(name.codec) - 1]
			ordinal = base.rpartition('.')[2]  # as the file name spells it
		flags = (['active'] if path == active else []) + ([name.codec] if name.codec else [])
		rows.append([str(index), ordinal, path, ','.join(flags)])
	_cliEmitFields(rows, ['index', 'ordinal', 'path', 'flags'], args, stdout)
```

with

```python
	if not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	reporter = _Reporter(args.store, logger)
	try:
		rows = _partRows(args.store, reporter)
	finally:
		reporter.flush()
	_cliEmitFields(rows, ['index', 'ordinal', 'path', 'flags'], args, stdout)
```

- [ ] **Step 4: Run the tests**

Run: `python3 -m pytest TSVZ_test.py -q`
Expected: all pass. Run it three times: the engine tests use threads and file timestamps, and must not be flaky.

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_test.py
git commit -q -F - <<'EOF'
Add the write handler engine: a replayed view of a store and its single appender.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
EOF
```

---

### Task 4: The request protocol, and `has`, `len`, `keys`, `pop`, `popitem`, `setdefault` on the command line

**Files:**
- Modify: `TSVZ.py` (protocol block above the CLI block; CLI parser, help, handlers)
- Modify: `TSVZ_test.py`

**Interfaces:**
- Consumes: Task 3's `_ServeStore`; Task 2's `_parseRecordLines`; the CLI's `_cliEmitRows`, `_cliEmitFields`, `_cliWrite`, `_CliStoreMissing`, `_cliStoreExists`.
- Produces:
  - Protocol constants: `_SERVE_PROTOCOL_VERSION`, `_SERVE_LINE_LIMIT`, `_SERVE_ARITY`, `_ServeUsage`.
  - Encoding helpers: `_serveEncode(value)`, `_serveRecordLine(cells, marker)`, `_serveFieldsLine(fields)`, `_serveDecodeLine(line)`.
  - Requests: `_serveParseRequest(line) -> (options, operation, args, rawArgs)`, `_serveNeedsBulk(line)`.
  - Running a request: `_ServeLog(logger)`, `_serveDispatch(store, line, bulk, log) -> (status, lines, message)`, `_serveRespond(store, line, bulk=None, logger=None) -> response lines`.
  - Responses: `_serveParseResponse(lines) -> (lines, diagnostics, status, message)`, `_ServeLost`.
  - `_ServeLocal(store)`: `.request(line, bulk=None)` and `.close()`. Task 5's `_ServeConnection` has the same interface.
  - CLI: `_cliRequestLine(args)`, `_cliEmitServed(args, delimiter, logger, stdout, response) -> status`, `_cliEngineOp(...)`, and the six new operations in `_CLI_OPERATIONS` / `_CLI_HANDLERS`.

The CLI runs the new operations through an in-process engine and prints the protocol response, so the direct path and Task 6's routed path print the same way.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ_test.py`, above the final `if __name__ == '__main__':` block:

```python

# ==========================================================================
# Write handler (spec §21): requests and responses
# ==========================================================================
def test_serve_responses(tmp_path, engines):
	p = str(tmp_path / 'r.tsvz')
	_touch(p, b'#_defaults_#\t\tNA\nalice\tAlice\t30\n<#>tag\ta<sep>b\tx\n')
	store = engines(p)
	R = lambda line, bulk=None: TSVZ._serveRespond(store, line, bulk)
	assert R('read') == ['alice\tAlice\t30', '<#>tag\ta<sep>b\tx', '#0']
	assert R('get\talice\tcarol') == ['alice\tAlice\t30', 'carol\t\tNA', '#3']
	assert R('get\t#tag') == ['<#>tag\ta<sep>b\tx', '#0'] == R('get\t<#>tag')
	assert R('has\talice') == ['#0'] and R('has\tnobody') == ['#3']
	assert R('len') == ['2', '#0'] and R('keys') == ['alice', '<#>tag', '#0']
	assert R('set\tbob\tB<lt>b\tline<LF>two') == ['#0']
	assert R('get\tbob') == ['bob\tB<lt>b\tline<LF>two', '#0']
	assert R('setdefault\tbob\tother') == ['bob\tB<lt>b\tline<LF>two', '#0']
	assert R('pop\tbob') == ['bob\tB<lt>b\tline<LF>two', '#0'] and R('pop\tbob') == ['#3']
	assert R('popitem\tfirst') == ['alice\tAlice\t30', '#0']
	assert R('set\t-', ['# comment', 'k\tv', '<#>_x_#\tdata', '#_defaults_#\t\tD']) == ['#0']
	# rows bind the defaults active where they are written: the marker comes after them
	assert R('get\t<#>_x_#\tk') == ['<#>_x_#\tdata\tNA', 'k\tv\tNA', '#0']
	assert R('delete\t-', ['k\tignored', '<#>_x_#']) == ['#0'] and R('has\tk') == ['#3']
	assert R('--sync\tset\tz\t1') == ['#0']
	assert open(p, 'rb').read().endswith(b'k\tv\n<#>_x_#\tdata\n#_defaults_#\t\tD\nk\n<#>_x_#\nz\t1\n')


def test_serve_usage_errors_keep_the_connection_usable(tmp_path, engines):
	store = engines(str(tmp_path / 'u.tsvz'), create=True)
	for line in ('', 'frob', 'x-export', '--bogus\tread', 'read\textra', 'get', 'has\ta\tb', 'set\t\tv',
				 'setdefault\tk', 'setdefault\t#_defaults_#\tx', 'popitem\tmiddle', 'stop'):
		response = TSVZ._serveRespond(store, line)
		assert response[-1].startswith('#2\t') or (line == 'stop' and response[-1].startswith('#1\t')), line
	assert TSVZ._serveRespond(store, 'read') == ['#0']


def test_serve_responses_carry_diagnostics_and_failures(tmp_path, engines, monkeypatch):
	p = str(tmp_path / 'd.tsvz')
	_touch(p, b'a\t1\n')
	store = engines(p)
	with open(p, 'ab') as f:
		f.write(b'torn')
	response = TSVZ._serveRespond(store, 'read')
	assert response[0].startswith('#!\tTSVZ warning: ') and 'torn' in response[0]
	assert response[1:] == ['a\t1', '#0']
	m = str(tmp_path / 'm.tsvz')
	_touch(m, b'a\t1\n')
	_touch(m + '.1', b'b\t2\n')
	store = engines(m)
	assert TSVZ._serveRespond(store, 'scrub')[-1] == '#1\tscrub refused; nothing was written'
	monkeypatch.chdir(str(tmp_path))
	_touch('#h.tsvz', b'#_checksum_crc32_#\na\t1\n#_checksum_crc32_#\t0\n')  # a path starting with '#'
	store = engines('#h.tsvz')
	lines, _, status, _ = TSVZ._serveParseResponse(TSVZ._serveRespond(store, 'verify'))
	assert status == 4 and lines[0].startswith('<#>h.tsvz\t3\tcrc32\t0\t')


def test_serve_parse_response():
	assert TSVZ._serveParseResponse(['#!\tw<LF>x', 'k\tv', '#1\tbad<sep>thing']) == (['k\tv'], ['w\nx'], 1, 'bad\tthing')
	with pytest.raises(TSVZ._ServeLost):
		TSVZ._serveParseResponse(['k\tv'])


def test_cli_has_len_keys_pop_popitem_setdefault(tmp_path):
	p = str(tmp_path / 'o.tsvz')
	_touch(p, b'a\t1\n<#>h\t2\nb\t3\n')
	assert _run('has', p, 'a') == (0, '', '') and _run('has', p, 'z') == (3, '', '')
	assert _run('len', p) == (0, '3\n', '') and _run('keys', p) == (0, 'a\n<#>h\nb\n', '')
	assert _run('pop', p, 'a') == (0, 'a\t1\n', '') and _run('pop', p, 'a') == (3, '', '')
	assert _run('popitem', p) == (0, 'b\t3\n', '') and _run('popitem', p, 'first') == (0, '<#>h\t2\n', '')
	assert _run('popitem', p) == (3, '', '')
	assert _run('setdefault', p, 'k', 'v', '-5') == (0, 'k\tv\t-5\n', '')
	assert _run('setdefault', p, 'k', 'other') == (0, 'k\tv\t-5\n', '')
	assert open(p, 'rb').read() == b'a\t1\n<#>h\t2\nb\t3\na\nb\n<#>h\nk\tv\t-5\n'
	fresh = str(tmp_path / 'fresh.tsvz')
	status, out, err = _run('setdefault', fresh, 'k', 'v')
	assert (status, out) == (0, 'k\tv\n') and open(fresh, 'rb').read() == b'k\tv\n'
	q = str(tmp_path / 'o.csv')
	_touch(q, b'a,x<sep>y\nb,2\n')
	assert _run('keys', q) == (0, 'a\nb\n', '') and _run('pop', q, 'a') == (0, 'a,x<sep>y\n', '')
	for argv in (['has', 'none.tsvz', 'k'], ['len', 'none.tsvz'], ['keys', 'none.tsv'], ['pop', 'none.tsvz', 'k'],
				 ['popitem', 'none.tsvz']):
		argv[1] = str(tmp_path / argv[1])
		status, out, err = _run(*argv)
		assert (status, out) == (1, '') and 'no such store' in err
	for argv in (['has', p], ['has', p, 'a', 'b'], ['len', p, 'x'], ['popitem', p, 'middle'], ['setdefault', p, 'k'],
				 ['pop', p, '']):
		with pytest.raises(TSVZ._CliUsageError):
			TSVZ._cliParseArgs(argv)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `python3 -m pytest TSVZ_test.py -q -k "serve_ or cli_has"`
Expected: FAIL: `AttributeError: module 'TSVZ' has no attribute '_serveRespond'` (and `_serveParseResponse`); the CLI test fails on the unknown operation `has`.

- [ ] **Step 3: Add the protocol and the CLI operations**

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
# ===========================================================================
# Write handler (tsvz-spec-v1 §21): the protocol
# ===========================================================================
_SERVE_PROTOCOL_VERSION = 1
#: A request or bulk line longer than this closes the connection (spec §21.10).
_SERVE_LINE_LIMIT = 64 << 20
#: operation -> (minimum, maximum) number of arguments (None: no maximum).
_SERVE_ARITY = {
	'read': (0, 0), 'len': (0, 0), 'keys': (0, 0), 'clear': (0, 0), 'scrub': (0, 0), 'verify': (0, 0),
	'parts': (0, 0), 'stop': (0, 0), 'get': (1, None), 'set': (1, None), 'append': (1, None),
	'delete': (1, None), 'has': (1, 1), 'pop': (1, 1), 'popitem': (0, 1), 'setdefault': (2, None),
}
_SERVE_UNREADABLE = 'could not read every part of the store'


class _ServeUsage(Exception):
	"""A request that does not follow spec §21.5 / §21.7 (status 2)."""


def _serveEncode(value):
	"""A protocol field (spec §21.4): §13 with TAB, a leading '#' left as written."""
	return _specEncodeField(value, '\t')


def _serveRecordLine(cells, marker):
	"""A record as a protocol line: a marker key as written, a data key with ``<#>`` (spec §21.4)."""
	if marker:
		return '\t'.join([cells[0]] + [_serveEncode(cell) for cell in cells[1:]])
	return _specFormatRecord(cells, '\t')


def _serveFieldsLine(fields):
	"""An output line of plain fields (``verify``, ``parts``): §13 with TAB, a leading '#' as ``<#>``."""
	return '\t'.join([_specEncodeField(str(fields[0]), '\t', isKey=True)] + [_serveEncode(str(field)) for field in fields[1:]])


def _serveDecodeLine(line):
	"""The fields of a protocol record line, decoded per §13."""
	return [_specDecodeField(field, '\t') for field in line.split('\t')]


def _serveParseRequest(line):
	"""Split a request line (spec §21.5) into ``(options, operation, args, rawArgs)``."""
	raw = line.split('\t')
	i = 0
	while i < len(raw) and raw[i].startswith('--'):
		i += 1
	if i >= len(raw) or not raw[i]:
		raise _ServeUsage('missing operation')
	return raw[:i], raw[i], [_specDecodeField(field, '\t') for field in raw[i + 1:]], raw[i + 1:]


def _serveNeedsBulk(line):
	"""True when ``line`` is ``set -`` / ``append -`` / ``delete -``: record lines up to ``#`` follow."""
	try:
		_, operation, _, raw = _serveParseRequest(line)
	except _ServeUsage:
		return False
	return operation in ('set', 'append', 'delete') and raw == ['-']


class _ServeLog(object):
	"""Per-request teeLogger: warnings and errors become ``#!`` lines and also go to ``logger``."""

	def __init__(self, logger=None):
		self.logger = logger
		self.lines = []

	def teelog(self, message, level='info', callerStackDepth=None):
		if level in ('warning', 'error', 'critical'):
			self.lines.append(str(message))
		if self.logger is not None:
			self.logger.teelog(message, level)


def _serveDispatch(store, line, bulk, log):
	"""Run one request against ``store`` (spec §21.7); return ``(status, lines, message)``."""
	options, operation, args, raw = _serveParseRequest(line)
	for option in options:
		if option != '--sync':
			raise _ServeUsage('unknown option {}'.format(option))
	sync = '--sync' in options
	if operation not in _SERVE_ARITY:
		raise _ServeUsage('unknown operation {}'.format(operation))
	low, high = _SERVE_ARITY[operation]
	if len(args) < low or (high is not None and len(args) > high):
		raise _ServeUsage('wrong number of arguments for {}'.format(operation))
	isBulk = operation in ('set', 'append', 'delete') and raw == ['-']
	keys = args if operation in ('get', 'delete', 'has', 'pop') else args[:1]
	if operation not in ('read', 'len', 'keys', 'clear', 'scrub', 'verify', 'parts', 'stop', 'popitem') \
			and not isBulk and '' in keys:
		raise _ServeUsage('a KEY must not be empty')
	unreadable = (1, _SERVE_UNREADABLE)
	if operation == 'read':
		rows = store.read(log)
		status = unreadable if store.unreadable else (0, '')
		return status[0], [_specFormatRecord(row, '\t') for row in rows], status[1]
	if operation == 'get':
		rows, missing = store.get(args, log)
		status = unreadable if store.unreadable else (3 if missing else 0, '')
		return status[0], [_specFormatRecord(row, '\t') for row in rows], status[1]
	if operation == 'has':
		found = store.has(args[0], log)
		return (1, [], _SERVE_UNREADABLE) if store.unreadable else (0 if found else 3, [], '')
	if operation == 'len':
		return 0, [str(store.length(log))], ''
	if operation == 'keys':
		return 0, [_specEncodeField(key, '\t', isKey=True) for key in store.keys(log)], ''
	if operation in ('set', 'append', 'delete'):
		keysOnly = operation == 'delete'
		if isBulk:
			reporter = _Reporter('<request>', log)
			try:
				rows = _parseRecordLines(bulk or [], '\t', True, keysOnly, reporter)
			finally:
				reporter.flush()
		elif keysOnly:
			rows = [([key], bool(_MARKER_RE.match(field))) for key, field in zip(args, raw)]
		else:
			rows = [(args, bool(_MARKER_RE.match(raw[0])))]
		store.write(rows, sync, log)
		return 0, [], ''
	if operation in ('pop', 'popitem'):
		if operation == 'pop':
			row = store.pop(args[0], sync, log)
		else:
			if args and args[0] not in ('first', 'last'):
				raise _ServeUsage('popitem takes first or last')
			row = store.popitem(not args or args[0] == 'last', sync, log)
		return (3, [], '') if row is None else (0, [_specFormatRecord(row, '\t')], '')
	if operation == 'setdefault':
		if _MARKER_RE.match(raw[0]):
			raise _ServeUsage('setdefault takes a data KEY, not a marker')
		row = store.setdefault(args, sync, log)
		return (3, [], '') if row is None else (0, [_specFormatRecord(row, '\t')], '')
	if operation in ('clear', 'scrub'):
		done = store.clear(log) if operation == 'clear' else store.scrub(log)
		return (0, [], '') if done else (1, [], '{} refused; nothing was written'.format(operation))
	if operation == 'verify':
		mismatches, readable = store.verify(log)
		lines = [_serveFieldsLine(entry) for entry in mismatches]
		return (1 if not readable else 4 if lines else 0), lines, '' if readable else _SERVE_UNREADABLE
	if operation == 'parts':
		return 0, [_serveFieldsLine(row) for row in store.partRows(log)], ''
	return 1, [], 'not served by a handler'  # stop, sent to an in-process store


def _serveRespond(store, line, bulk=None, logger=None):
	"""The response to one request (spec §21.6), as lines without their '\\n'."""
	log = _ServeLog(logger)
	try:
		status, lines, message = _serveDispatch(store, line, bulk, log)
	except _ServeUsage as e:
		status, lines, message = 2, [], str(e)
	except Exception as e:
		status, lines, message = 1, [], str(e) or type(e).__name__
		if logger is not None:
			logger.teelog('tsvz: {}: request {!r} failed: {}'.format(store.path, line[:80], message), 'error')
	response = ['#!\t' + _serveEncode(text) for text in log.lines] + list(lines)
	response.append('#{}\t{}'.format(status, _serveEncode(message)) if message else '#{}'.format(status))
	return response


def _serveParseResponse(lines):
	"""``(lines, diagnostics, status, message)`` from a response's lines (spec §21.6)."""
	out, diagnostics = [], []
	for line in lines:
		if line.startswith('#!'):
			diagnostics.append(_specDecodeField(line[3:], '\t'))
		elif line.startswith('#'):
			head, _, message = line[1:].partition('\t')
			status = int(head) if _ASCII_DIGITS_RE.match(head) else 1
			return out, diagnostics, status, _specDecodeField(message, '\t')
		else:
			out.append(line)
	raise _ServeLost('the response ended without a status line')


class _ServeLost(Exception):
	"""The connection to a handler broke during a request; whether it was applied is unknown."""


class _ServeLocal(object):
	"""The in-process stand-in for a handler connection: the same engine and requests, no socket."""

	def __init__(self, store):
		self.store = store

	def request(self, line, bulk=None):
		return _serveParseResponse(_serveRespond(self.store, line, bulk))

	def close(self):
		self.store.close()
```

```python
# ===========================================================================
# Command-line interface (tsvz-spec-v1 §20)
```

In `TSVZ.py`, replace

```python
_CLI_OPERATIONS = ('read', 'get', 'set', 'append', 'delete', 'clear', 'scrub', 'verify', 'parts')
```

with

```python
_CLI_OPERATIONS = ('read', 'get', 'set', 'append', 'delete', 'clear', 'scrub', 'verify', 'parts',
				   'has', 'len', 'keys', 'pop', 'popitem', 'setdefault')
```

In `TSVZ.py`, replace

```python
  parts STORE                  list the parts of a multi-part store
```

with

```python
  parts STORE                  list the parts of a multi-part store
  has STORE KEY                exit 0 if KEY is live, 3 if not
  len STORE                    print the number of live keys
  keys STORE                   print the live keys
  pop STORE KEY                print the row of KEY and delete it (exit 3 if missing)
  popitem STORE [first|last]   pop the first or last live key (default: last)
  setdefault STORE KEY VALUE [VALUE ...]
                               print the row of KEY, setting it to the VALUEs if missing
```

In `TSVZ.py`, replace

```python
	if args.operation in ('read', 'clear', 'scrub', 'verify', 'parts') and args.args:
		raise _CliUsageError('{} takes no arguments after STORE'.format(args.operation))
	if args.operation in ('get', 'set', 'append', 'delete'):
		if not args.args:
			raise _CliUsageError('{} needs a KEY'.format(args.operation))
		keys = args.args if args.operation in ('get', 'delete') else args.args[:1]
		if '' in keys:
			raise _CliUsageError('{}: a KEY must not be empty'.format(args.operation))
	return args
```

with

```python
	if args.operation in ('read', 'clear', 'scrub', 'verify', 'parts', 'len', 'keys') and args.args:
		raise _CliUsageError('{} takes no arguments after STORE'.format(args.operation))
	if args.operation == 'popitem' and args.args not in ([], ['first'], ['last']):
		raise _CliUsageError('popitem takes first or last')
	if args.operation in ('has', 'pop') and len(args.args) > 1:
		raise _CliUsageError('{} takes one KEY'.format(args.operation))
	if args.operation == 'setdefault' and len(args.args) == 1:
		raise _CliUsageError('setdefault needs a KEY and a VALUE')
	if args.operation in ('get', 'set', 'append', 'delete', 'has', 'pop', 'setdefault'):
		if not args.args:
			raise _CliUsageError('{} needs a KEY'.format(args.operation))
		keys = args.args if args.operation in ('get', 'delete') else args.args[:1]
		if '' in keys:
			raise _CliUsageError('{}: a KEY must not be empty'.format(args.operation))
	return args
```

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
def _cliRequestLine(args):
	"""The protocol request (spec §21.5) for a parsed command line: its arguments are literal."""
	return '\t'.join([args.operation] + [_serveEncode(arg) for arg in args.args])


def _cliEmitServed(args, delimiter, logger, stdout, response):
	"""Print a response (spec §21.6) as the operation prints on the files (§20.8); return the exit status."""
	lines, diagnostics, status, message = response
	for text in diagnostics:
		logger.teelog(text, 'warning')
	spec = _isSpecPath(args.store)
	operation = args.operation
	if operation in ('read', 'get', 'pop', 'popitem', 'setdefault'):
		_cliEmitRows([_serveDecodeLine(line) for line in lines], args, stdout, spec, delimiter)
	elif operation == 'keys':
		_cliEmitRows([[_specDecodeField(line, '\t')] for line in lines], args, stdout, spec, delimiter)
	elif operation == 'len':
		_cliWrite(stdout, ''.join(line + '\n' for line in lines))
	elif operation == 'verify':
		_cliEmitFields([_serveDecodeLine(line) for line in lines], ['path', 'line', 'algorithm', 'expected', 'computed'],
					   args, stdout)
	elif operation == 'parts':
		_cliEmitFields([_serveDecodeLine(line) for line in lines], ['index', 'ordinal', 'path', 'flags'], args, stdout)
	if message and status in (1, 2):
		logger.teelog('tsvz: {}: {}'.format(args.store, message), 'error')
	return status


def _cliEngineOp(args, delimiter, logger, stdin, stdout):
	"""``has``, ``len``, ``keys``, ``pop``, ``popitem``, ``setdefault`` (spec §20.2) on the files.

	They run through an in-process write-handler engine, so they behave as
	they do through a handler, except that ``pop``, ``popitem`` and
	``setdefault`` are not atomic against other writers.
	"""
	create = args.operation == 'setdefault'
	if not create and not _cliStoreExists(args.store):
		raise _CliStoreMissing()
	store = _ServeStore(args.store, delimiter=delimiter, header=args.header, defaults=args.defaults,
						strict=args.strict, teeLogger=logger, verbose=args.verbose, create=create)
	try:
		response = _ServeLocal(store).request(_cliRequestLine(args))
	finally:
		store.close()
	status = _cliEmitServed(args, delimiter, logger, stdout, response)
	if store.writer.failures:
		return 1  # the write was not made; the writer reported why
	return status
```

```python
_CLI_HANDLERS = {'read': _cliRead,
```

In `TSVZ.py`, replace

```python
				 'clear': _cliClear, 'scrub': _cliScrub, 'verify': _cliVerify, 'parts': _cliParts}
```

with

```python
				 'clear': _cliClear, 'scrub': _cliScrub, 'verify': _cliVerify, 'parts': _cliParts,
				 'has': _cliEngineOp, 'len': _cliEngineOp, 'keys': _cliEngineOp, 'pop': _cliEngineOp,
				 'popitem': _cliEngineOp, 'setdefault': _cliEngineOp}
```

- [ ] **Step 4: Run the tests**

Run: `python3 -m pytest TSVZ_test.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_test.py
git commit -q -F - <<'EOF'
Add the write handler protocol and tsvz has, len, keys, pop, popitem and setdefault.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
EOF
```

---

### Task 5: The server, its pointer file, `tsvz serve` and `tsvz stop`

**Files:**
- Modify: `TSVZ.py` (server block above the CLI block; CLI options, help, `_cliServe`, `_cliStop`)
- Modify: `TSVZ_test.py`

**Interfaces:**
- Consumes: Task 4's `_serveRespond`, `_serveParseRequest`, `_serveNeedsBulk`, `_serveParseResponse`, `_ServeLost`; Task 3's `_ServeStore`; Task 2's `_tryLockFile`.
- Produces:
  - Exceptions: `_ServeBusy` (`args[0]` is the holder's pointer dict), `_ServeUnreachable(message, quiet)`, `_ServeSignal`.
  - Pointer files: `_servePointerPath(store)`, `_serveFind(store) -> dict|None`.
  - Client side: `_ServeConnection(info, timeout=None)` with `.request(line, bulk=None)` and `.close()`.
  - Server side: `_serveClasses()`, `_serveGroup(group)`, `_serveIsStop(line)`, `_serveSignals() -> restore`.
  - `_ServeHandle(storePath, logger=None, group=None, mode=None, tcp=False, idleTimeout=0)`:
    - methods: `.lock()`, `.start(store)`, `.serve()`, `.stop()`, `.close()`, `.connection(rfile, wfile, sock=None)`;
    - attributes: `.address`, `.token`, `.store`.
  - CLI:
    - options `--x-direct`, `--x-idle-timeout`, `--x-group`, `--x-mode` and `--x-tcp` (`_CliArgs.direct`, `idleTimeout`, `group`, `mode`, `tcp`);
    - operations `serve` and `stop`.

`tsvz serve` takes the pointer lock before it touches the store. It installs its SIGTERM handler before the pointer file announces it, so a stop that arrives at once is still clean.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ_test.py`, above the final `if __name__ == '__main__':` block:

```python

# ==========================================================================
# Write handler (spec §21): the server
# ==========================================================================
@pytest.fixture
def served():
	"""Start in-thread handlers: ``served(path, **handleOptions)`` returns the ``_ServeHandle``."""
	running = []

	def start(path, **options):
		handle = TSVZ._ServeHandle(path, **options)
		handle.lock()
		handle.start(TSVZ._ServeStore(path, create=True))
		thread = threading.Thread(target=handle.serve)
		thread.start()
		handle.thread = thread
		running.append((handle, thread))
		return handle
	yield start
	for handle, thread in running:
		handle.stop()
		thread.join(10)
		handle.close()


def _raw(address, data):
	"""Send raw bytes to a handler and return everything it answers until it closes or goes quiet."""
	import socket
	sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	sock.settimeout(2)
	sock.connect(address[len('unix:'):])
	try:
		sock.sendall(data)
		sock.shutdown(socket.SHUT_WR)
		chunks = []
		while True:
			chunk = sock.recv(65536)
			if not chunk:
				return b''.join(chunks)
			chunks.append(chunk)
	finally:
		sock.close()


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_over_a_unix_socket(tmp_path, served):
	p = str(tmp_path / 's.tsvz')
	handle = served(p)
	info = TSVZ._serveFind(p)
	assert info['address'] == handle.address and info['address'].startswith('unix:')
	assert info['pid'] == str(os.getpid()) and info['host'] == __import__('socket').gethostname()
	connection = TSVZ._ServeConnection(info)
	try:
		assert connection.request('set\talice\tAlice') == ([], [], 0, '')
		assert connection.request('set\t-', ['bob\tBob', 'carol']) == ([], [], 0, '')
		assert connection.request('read') == (['alice\tAlice', 'bob\tBob'], [], 0, '')
		assert connection.request('frob')[2:] == (2, 'unknown operation frob')
		assert connection.request('get\tbob') == (['bob\tBob'], [], 0, '')  # still usable after #2
	finally:
		connection.close()
	assert _raw(handle.address, b'len\nkeys\nbogus\n') == b'2\n#0\nalice\nbob\n#0\n#2\tunknown operation bogus\n'


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_closes_a_connection_with_an_overlong_line(tmp_path, served, monkeypatch):
	monkeypatch.setattr(TSVZ, '_SERVE_LINE_LIMIT', 100)
	handle = served(str(tmp_path / 'l.tsvz'))
	assert _raw(handle.address, b'get\t' + b'k' * 200 + b'\nlen\n') == b''
	assert _raw(handle.address, b'len\n') == b'0\n#0\n'


def test_serve_over_tcp_requires_the_token(tmp_path, served):
	p = str(tmp_path / 't.tsvz')
	handle = served(p, tcp=True)
	info = TSVZ._serveFind(p)
	assert info['address'].startswith('tcp:127.0.0.1:') and len(info['token']) == 32
	assert os.stat(p + '.serve').st_mode & 0o777 == 0o600  # the token is in it
	connection = TSVZ._ServeConnection(info)
	try:
		assert connection.request('set\tk\tv')[2] == 0 and connection.request('get\tk')[0] == ['k\tv']
	finally:
		connection.close()
	import socket
	host, port = info['address'][4:].rsplit(':', 1)
	for first in (b'read\n', b'auth\twrong\n'):
		sock = socket.create_connection((host, int(port)), 2)
		try:
			sock.sendall(first)
			assert sock.makefile('rb').read().startswith(b'#1\t')
		finally:
			sock.close()
	with pytest.raises(TSVZ._ServeUnreachable):
		TSVZ._ServeConnection(dict(info, token='0' * 32))


@pytest.mark.skipif(os.name != 'posix', reason='POSIX permissions')
def test_serve_socket_permissions(tmp_path, served):
	def modes(handle):
		path = handle.address[len('unix:'):]
		return os.stat(path).st_mode & 0o777, os.stat(os.path.dirname(path)).st_mode & 0o777
	assert modes(served(str(tmp_path / 'a.tsvz'))) == (0o600, 0o700)
	assert os.stat(str(tmp_path / 'a.tsvz.serve')).st_mode & 0o777 == 0o644
	assert modes(served(str(tmp_path / 'b.tsvz'), group=str(os.getgid()))) == (0o660, 0o710)
	assert modes(served(str(tmp_path / 'c.tsvz'), mode=0o666)) == (0o666, 0o711)


def test_serve_refuses_a_second_handler_for_the_same_store(tmp_path, served):
	p = str(tmp_path / 'b.tsvz')
	served(p)
	second = TSVZ._ServeHandle(p)
	with pytest.raises(TSVZ._ServeBusy) as busy:
		second.lock()
	assert busy.value.args[0]['pid'] == str(os.getpid())


def _serve_process(store, *options):
	"""Start ``tsvz serve STORE`` in the background; return the Popen once its pointer file is written."""
	proc = subprocess.Popen([sys.executable, os.path.join(HERE, 'TSVZ.py'), 'serve', store] + list(options),
							stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	deadline = time.time() + 10
	while TSVZ._serveFind(store) is None:
		if proc.poll() is not None or time.time() > deadline:
			proc.kill()
			raise AssertionError(proc.communicate()[1])
		time.sleep(0.02)
	return proc


def test_tsvz_serve_and_stop(tmp_path):
	p = str(tmp_path / 'p.tsvz')
	proc = _serve_process(p, '--x-header', 'id\\tval')
	try:
		address = TSVZ._serveFind(p)['address']
		r = _cli('serve', p)
		assert r.returncode == 1 and 'already served by pid {}'.format(proc.pid) in r.stderr
		connection = TSVZ._ServeConnection(TSVZ._serveFind(p))
		assert connection.request('set\tk\tv')[2] == 0
		connection.close()
		r = _cli('stop', p)
		assert (r.returncode, r.stdout) == (0, '')
		out, err = proc.communicate(timeout=10)
	finally:
		if proc.poll() is None:
			proc.kill()
	assert proc.returncode == 0 and out == b'' and b'serving' in err and b'stopped serving' in err
	assert open(p, 'rb').read() == b'#id\tval\nk\tv\n'  # the header of a store serve created; the write
	assert not os.path.exists(p + '.serve') and not os.path.exists(os.path.dirname(address[len('unix:'):]))
	r = _cli('stop', p)
	assert r.returncode == 1 and 'no handler is running' in r.stderr


@pytest.mark.skipif(os.name != 'posix', reason='signals')
def test_tsvz_serve_stops_on_sigterm_and_when_idle(tmp_path):
	import signal
	p = str(tmp_path / 'g.tsvz')
	proc = _serve_process(p)
	proc.send_signal(signal.SIGTERM)
	assert proc.wait(10) == 0 and not os.path.exists(p + '.serve')
	proc = _serve_process(p, '--x-idle-timeout', '0.3')
	assert proc.wait(10) == 0 and b'idle for 0.3 s' in proc.stderr.read()
	assert not os.path.exists(p + '.serve')


def test_tsvz_serve_usage_errors(tmp_path):
	p = str(tmp_path / 'u.tsvz')
	for options in (['--x-idle-timeout', 'soon'], ['--x-mode', '9x']):
		r = _cli('serve', p, *options)
		assert r.returncode == 2 and 'serve: bad' in r.stderr and 'usage: tsvz' in r.stderr
	assert _cli('serve', p, 'extra').returncode == 2

@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_close_ends_open_connections(tmp_path, served):
	p = str(tmp_path / 'e.tsvz')
	handle = served(p)
	connection = TSVZ._ServeConnection(TSVZ._serveFind(p))
	try:
		assert connection.request('len')[2] == 0
		handle.stop()
		handle.thread.join(10)
		handle.close()
		with pytest.raises(TSVZ._ServeLost):
			connection.request('len')
	finally:
		connection.close()
	assert TSVZ._serveFind(p) is None

@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_many_concurrent_clients(tmp_path, served):
	"""Review focus 1: interleaved requests from many connections all land, each answered on its own."""
	p = str(tmp_path / 'c.tsvz')
	served(p)
	errors = []

	def client(n):
		connection = TSVZ._ServeConnection(TSVZ._serveFind(p))
		try:
			for i in range(50):
				key = 'c{}k{}'.format(n, i)
				if connection.request('set\t{}\t{}'.format(key, i))[2] != 0 or \
						connection.request('get\t' + key)[0] != ['{}\t{}'.format(key, i)]:
					errors.append(key)
		finally:
			connection.close()
	threads = [threading.Thread(target=client, args=(n,)) for n in range(8)]
	for t in threads:
		t.start()
	for t in threads:
		t.join()
	assert errors == []
	assert len(_view(p)) == 400


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_serve_ignores_bulk_input_cut_off_before_its_end(tmp_path, served):
	"""Review focus 2: a client that disconnects before the closing '#' writes nothing."""
	p = str(tmp_path / 'b.tsvz')
	handle = served(p)
	assert _raw(handle.address, b'set\t-\nk\tv\nj\tw\n') == b''
	assert _raw(handle.address, b'len\n') == b'0\n#0\n' and open(p, 'rb').read() == b''


def test_serve_takes_over_a_stale_pointer(tmp_path, served):
	"""Review focus 3: the pointer file of a crashed handler is replaced by the next one."""
	import socket
	p = str(tmp_path / 's.tsvz')
	_touch(p + '.serve', 'tsvz-handler\t1\naddress\tunix:/gone\nhost\t{}\npid\t1\n'.format(socket.gethostname()).encode())
	assert _run('get', p, 'k')[0] == 1  # no store yet; the stale pointer is reported and skipped
	handle = served(p)
	assert TSVZ._serveFind(p)['address'] == handle.address
```

- [ ] **Step 2: Run them to verify they fail**

Run: `python3 -m pytest TSVZ_test.py -q -k "serve_over or serve_closes or serve_socket or serve_refuses or tsvz_serve or serve_many or serve_ignores or serve_takes or close_ends"`
Expected: FAIL with `AttributeError` (`_ServeHandle`, `_serveFind`); the subprocess tests fail because `serve` is not an operation yet.

- [ ] **Step 3: Add the server and the two operations**

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
# ===========================================================================
# Write handler (tsvz-spec-v1 §21): pointer files, connections and the server
# ===========================================================================
class _ServeBusy(Exception):
	"""Another handler holds the store's pointer file (spec §21.2); ``args[0]`` is its pointer, as a dict."""


class _ServeUnreachable(Exception):
	"""A pointer file whose handler cannot be used; ``quiet`` when that is expected (other host, no permission)."""

	def __init__(self, message, quiet=False):
		Exception.__init__(self, message)
		self.quiet = quiet


class _ServeSignal(Exception):
	"""SIGTERM arrived while ``tsvz serve`` was serving."""


def _servePointerPath(store):
	"""The pointer file of a store (spec §21.2): the store path plus ``.serve``."""
	return store + '.serve'


def _serveFind(store):
	"""Read the pointer file of ``store`` (spec §21.2); return its fields as a dict, or None.

	None when there is no pointer file, or it does not parse even after one
	short retry (a handler may be writing it).
	"""
	path = _servePointerPath(store)
	for attempt in range(2):
		if attempt:
			time.sleep(0.05)
		try:
			with open(path, 'rb') as f:
				lines = f.read().decode('utf-8', 'replace').split('\n')
		except OSError:
			return None
		if lines[0] != 'tsvz-handler\t{}'.format(_SERVE_PROTOCOL_VERSION):
			continue
		info = {}
		for line in lines[1:]:
			name, tab, value = line.partition('\t')
			if tab:
				info[name] = _specDecodeField(value, '\t')
		if 'address' in info and 'host' in info:
			return info
	return None


class _ServeConnection(object):
	"""A connection to the handler a pointer file names (spec §21.3–§21.6, §21.11)."""

	def __init__(self, info, timeout=None):
		import errno
		import socket
		if info.get('host') != socket.gethostname():
			raise _ServeUnreachable('it runs on host {}'.format(info.get('host')), quiet=True)
		address = info.get('address', '')
		try:
			if address.startswith('unix:') and hasattr(socket, 'AF_UNIX'):
				sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
				try:
					sock.settimeout(0.5)
					sock.connect(address[5:])
				except Exception:
					sock.close()
					raise
			elif address.startswith('tcp:'):
				host, _, port = address[4:].rpartition(':')
				sock = socket.create_connection((host.strip('[]'), int(port)), 0.5)
			else:
				raise _ServeUnreachable('unusable address {!r}'.format(address), quiet=True)
		except (OSError, ValueError) as e:
			raise _ServeUnreachable(str(e), quiet=getattr(e, 'errno', None) in (errno.EACCES, errno.EPERM))
		sock.settimeout(timeout)
		self.sock = sock
		self.rfile = sock.makefile('rb')
		if info.get('token'):
			try:
				status = self.request('auth\t' + info['token'])[2]
			except _ServeLost:
				status = 1
			if status != 0:
				self.close()
				raise _ServeUnreachable('the handler refused the token')

	def request(self, line, bulk=None):
		"""Send one request, followed by ``bulk`` record lines and ``#`` when given; return the parsed response."""
		text = line + '\n'
		if bulk is not None:
			text += ''.join(record + '\n' for record in bulk) + '#\n'
		try:
			self.sock.sendall(text.encode('utf-8', 'replace'))
			lines = []
			while True:
				raw = self.rfile.readline()
				if not raw.endswith(b'\n'):
					raise _ServeLost('the handler closed the connection')
				line = raw[:-1].decode('utf-8', 'replace')
				lines.append(line)
				if line.startswith('#') and not line.startswith('#!'):
					return _serveParseResponse(lines)
		except OSError as e:
			raise _ServeLost(str(e))

	def close(self):
		for thing in (self.rfile, self.sock):
			try:
				thing.close()
			except Exception:
				pass


_SERVE_CLASSES = {}


def _serveClasses():
	"""The socketserver classes of the handler; ``socketserver`` is imported on first use."""
	if not _SERVE_CLASSES:
		import socketserver

		class Handler(socketserver.StreamRequestHandler):
			def handle(self):
				self.server.serving.connection(self.rfile, self.wfile, self.request)

		class TCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
			daemon_threads = True
			allow_reuse_address = True

		_SERVE_CLASSES.update(handler=Handler, tcp=TCPServer)
		if hasattr(socketserver, 'UnixStreamServer'):
			class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
				daemon_threads = True

			_SERVE_CLASSES['unix'] = UnixServer
	return _SERVE_CLASSES


def _serveGroup(group):
	"""A group name or number as a gid (None stays None)."""
	if group is None or group == '':
		return None
	if _ASCII_DIGITS_RE.match(str(group)):
		return int(group)
	import grp
	return grp.getgrnam(group).gr_gid


def _serveIsStop(line):
	"""True when ``line`` is a well-formed ``stop`` request."""
	try:
		options, operation, args, _ = _serveParseRequest(line)
	except _ServeUsage:
		return False
	return operation == 'stop' and not args and all(option == '--sync' for option in options)


class _ServeHandle(object):
	"""A running write handler for one store (spec §21; design §3).

	``lock()`` takes the pointer file, ``start(store)`` binds the socket and
	writes the pointer, ``serve()`` answers requests until ``stop()`` (or an
	exception in the serving thread), ``close()`` writes everything and
	removes the socket and the pointer file.
	"""

	def __init__(self, storePath, logger=None, group=None, mode=None, tcp=False, idleTimeout=0):
		self.storePath = storePath
		self.logger = logger
		self.group = group
		self.mode = mode
		self.tcp = tcp
		self.idleTimeout = idleTimeout
		self.pointer = _servePointerPath(storePath)
		self.pointerFile = None
		self.store = None
		self.server = None
		self.socketDir = None
		self.address = None
		self.token = None
		self.guard = threading.Lock()
		self.active = 0
		self.sockets = set()
		self.lastActivity = time.monotonic()
		self.stopping = threading.Event()

	def lock(self):
		"""Take the pointer file's lock (spec §21.2); raise ``_ServeBusy`` when another handler has it."""
		f = os.fdopen(os.open(self.pointer, os.O_RDWR | os.O_CREAT, 0o644), 'r+b')
		if not _tryLockFile(f):
			f.close()
			raise _ServeBusy(_serveFind(self.storePath) or {})
		self.pointerFile = f

	def start(self, store):
		"""Bind the socket for ``store`` and write the pointer file."""
		import socket
		import tempfile
		self.store = store
		classes = _serveClasses()
		gid = _serveGroup(self.group)
		mode = self.mode if self.mode is not None else (0o660 if gid is not None else 0o600)
		if self.tcp or 'unix' not in classes:
			self.server = classes['tcp'](('127.0.0.1', 0), classes['handler'])
			self.address = 'tcp:127.0.0.1:{}'.format(self.server.server_address[1])
			self.token = ''.join('%02x' % b for b in os.urandom(16))
		else:
			base = os.environ.get('XDG_RUNTIME_DIR', '')
			if not base or not os.path.isdir(base) or not os.access(base, os.W_OK):
				base = tempfile.gettempdir()
			self.socketDir = tempfile.mkdtemp(prefix='tsvz-', dir=base)
			path = os.path.join(self.socketDir, 's')
			self.server = classes['unix'](path, classes['handler'])
			if gid is not None:
				os.chown(self.socketDir, -1, gid)
				os.chown(path, -1, gid)
			os.chmod(path, mode)
			os.chmod(self.socketDir, 0o700 | (0o010 if mode & 0o070 else 0) | (0o001 if mode & 0o007 else 0))
			self.address = 'unix:' + path
		self.server.serving = self
		lines = ['tsvz-handler\t{}'.format(_SERVE_PROTOCOL_VERSION), 'address\t' + _serveEncode(self.address),
				 'host\t' + _serveEncode(socket.gethostname()), 'pid\t{}'.format(os.getpid())]
		if self.token:
			lines.append('token\t' + self.token)
		if not store.spec:
			lines.append('x-delimiter\t' + _serveEncode(store.delimiter))
		try:
			os.chmod(self.pointer, mode if self.token else 0o644)
		except OSError:
			pass
		f = self.pointerFile
		f.seek(0)
		f.truncate()
		f.write(''.join(line + '\n' for line in lines).encode('utf-8'))
		f.flush()
		os.fsync(f.fileno())

	def serve(self):
		"""Answer requests in this thread until ``stop()`` or the idle timeout."""
		if self.idleTimeout:
			watcher = threading.Thread(target=self._watchIdle, name='tsvz-idle')
			watcher.daemon = True
			watcher.start()
		self.server.serve_forever(poll_interval=0.2)

	def stop(self):
		"""Make ``serve()`` return (from any other thread)."""
		self.stopping.set()
		if self.server is not None:
			stopper = threading.Thread(target=self.server.shutdown, name='tsvz-stop')
			stopper.daemon = True
			stopper.start()

	def close(self):
		"""Stop listening, end open connections, write and fsync everything, remove the socket and the pointer."""
		import socket
		if self.server is not None:
			self.server.server_close()
		with self.guard:
			sockets = list(self.sockets)
		for sock in sockets:
			try:
				sock.shutdown(socket.SHUT_RDWR)
			except OSError:
				pass
		if self.store is not None:
			self.store.close()
		if self.socketDir is not None:
			shutil.rmtree(self.socketDir, ignore_errors=True)
		if self.pointerFile is not None:
			try:
				if os.stat(self.pointer).st_ino == os.fstat(self.pointerFile.fileno()).st_ino:
					os.unlink(self.pointer)
			except OSError:
				pass
			self.pointerFile.close()
			self.pointerFile = None

	def _watchIdle(self):
		while not self.stopping.wait(min(1.0, self.idleTimeout / 4.0)):
			with self.guard:
				idle = not self.active and time.monotonic() - self.lastActivity >= self.idleTimeout
			if idle:
				if self.logger is not None:
					self.logger.teelog('tsvz: {}: idle for {:g} s; stopping'.format(self.storePath, self.idleTimeout))
				self.stop()
				return

	def connection(self, rfile, wfile, sock=None):
		"""Answer the requests of one connection (spec §21.3–§21.11)."""
		with self.guard:
			self.active += 1
			if sock is not None:
				self.sockets.add(sock)
		try:
			authed = self.token is None
			while True:
				line = self._readLine(rfile)
				if line is None:
					return
				fields = line.split('\t')
				if fields[0] == 'auth':
					import hmac
					ok = len(fields) == 2 and (self.token is None or hmac.compare_digest(fields[1], self.token))
					self._send(wfile, ['#0'] if ok else ['#1\tauthentication failed'])
					if not ok:
						return
					authed = True
					continue
				if not authed:
					self._send(wfile, ['#1\tauthentication required'])
					return
				bulk = None
				if _serveNeedsBulk(line):
					bulk = []
					while True:
						record = self._readLine(rfile)
						if record is None:
							return
						if record == '#':
							break
						bulk.append(record)
				if _serveIsStop(line):
					self.store.flush()
					self._send(wfile, ['#0'])
					self.stop()
					return
				self._send(wfile, _serveRespond(self.store, line, bulk, self.logger))
		except OSError:
			return  # the client went away
		finally:
			with self.guard:
				self.active -= 1
				self.sockets.discard(sock)
				self.lastActivity = time.monotonic()

	def _readLine(self, rfile):
		"""One request line without its terminator; None at the end of the connection or past the limit."""
		raw = rfile.readline(_SERVE_LINE_LIMIT + 1)
		if not raw.endswith(b'\n'):
			return None
		text = raw[:-1].decode('utf-8', 'replace')
		return text[:-1] if text.endswith('\r') else text

	def _send(self, wfile, lines):
		wfile.write(''.join(line + '\n' for line in lines).encode('utf-8', 'replace'))
		wfile.flush()


def _serveSignals():
	"""Make SIGTERM stop ``serve`` as SIGINT does; return a function that restores the old handler."""
	import signal
	if threading.current_thread() is not threading.main_thread() or not hasattr(signal, 'SIGTERM'):
		return lambda: None

	def handler(signum, frame):
		raise _ServeSignal()
	old = signal.signal(signal.SIGTERM, handler)
	return lambda: signal.signal(signal.SIGTERM, old)
```

```python
# ===========================================================================
# Command-line interface (tsvz-spec-v1 §20)
```

In `TSVZ.py`, replace

```python
				   'has', 'len', 'keys', 'pop', 'popitem', 'setdefault')
```

with

```python
				   'has', 'len', 'keys', 'pop', 'popitem', 'setdefault', 'serve', 'stop')
```

In `TSVZ.py`, replace

```python
	'--x-force': ('force', False), '-f': ('force', False), '--force': ('force', False),
```

with

```python
	'--x-force': ('force', False), '-f': ('force', False), '--force': ('force', False),
	'--x-direct': ('direct', False), '--x-idle-timeout': ('idleTimeout', True),
	'--x-group': ('group', True), '--x-mode': ('mode', True), '--x-tcp': ('tcp', False),
```

In `TSVZ.py`, replace

```python
		self.operation = None
		self.store = None
		self.args = []
```

with

```python
		self.direct = False
		self.idleTimeout = None
		self.group = None
		self.mode = None
		self.tcp = False
		self.operation = None
		self.store = None
		self.args = []
```

In `TSVZ.py`, replace

```python
  setdefault STORE KEY VALUE [VALUE ...]
                               print the row of KEY, setting it to the VALUEs if missing
```

with

```python
  setdefault STORE KEY VALUE [VALUE ...]
                               print the row of KEY, setting it to the VALUEs if missing
  serve STORE                  keep STORE loaded and answer requests on a local socket
  stop STORE                   stop the server of STORE
```

In `TSVZ.py`, replace

```python
  --                           every argument after this is positional
```

with

```python
  --x-direct                   work on the files even when a server runs
  --x-idle-timeout S           serve: stop after S seconds without connections
  --x-group G, --x-mode M      serve: let group G connect / set the socket's octal mode
  --x-tcp                      serve: listen on 127.0.0.1 instead of a Unix socket
  --                           every argument after this is positional
```

In `TSVZ.py`, replace

```python
	if args.operation in ('read', 'clear', 'scrub', 'verify', 'parts', 'len', 'keys') and args.args:
```

with

```python
	if args.operation in ('read', 'clear', 'scrub', 'verify', 'parts', 'len', 'keys', 'serve', 'stop') and args.args:
```

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
def _cliServe(args, delimiter, logger, stdin, stdout):
	"""``serve STORE`` (spec §21.13): run a write handler for STORE in the foreground until it is stopped."""
	try:
		idle = float(args.idleTimeout) if args.idleTimeout else 0.0
		mode = int(args.mode, 8) if args.mode else None
		_serveGroup(args.group)
	except (ValueError, KeyError) as e:
		raise _CliUsageError('serve: bad --x-idle-timeout, --x-mode or --x-group ({})'.format(e))
	handle = _ServeHandle(args.store, logger=logger, group=args.group, mode=mode, tcp=args.tcp, idleTimeout=idle)
	try:
		handle.lock()
	except _ServeBusy as e:
		info = e.args[0]
		logger.teelog('tsvz: {}: already served by pid {} on host {}'.format(
			args.store, info.get('pid', '?'), info.get('host', '?')), 'error')
		return 1
	restore = _serveSignals()  # before the pointer file announces this handler
	try:
		store = _ServeStore(args.store, delimiter=delimiter, header=args.header, createDefaults=args.defaults,
							teeLogger=logger, verbose=args.verbose, create=True)
		handle.start(store)
		logger.teelog('tsvz: serving {} at {} (pid {})'.format(args.store, handle.address, os.getpid()))
		handle.serve()
	except (KeyboardInterrupt, _ServeSignal):
		pass
	finally:
		restore()
		handle.close()
	logger.teelog('tsvz: stopped serving {}'.format(args.store))
	return 0


def _cliStop(args, delimiter, logger, stdin, stdout):
	"""``stop STORE`` (spec §21.13): ask the handler of STORE to stop, and wait until it has."""
	info = _serveFind(args.store)
	if info is None:
		logger.teelog('tsvz: {}: no handler is running'.format(args.store), 'error')
		return 1
	try:
		connection = _ServeConnection(info)
	except _ServeUnreachable as e:
		logger.teelog('tsvz: {}: the handler does not answer ({})'.format(args.store, e), 'error')
		return 1
	try:
		status = connection.request('stop')[2]
	except _ServeLost as e:
		logger.teelog('tsvz: {}: {}'.format(args.store, e), 'error')
		return 1
	finally:
		connection.close()
	deadline = time.monotonic() + 10
	while time.monotonic() < deadline and (_serveFind(args.store) or {}).get('pid') == info.get('pid'):
		time.sleep(0.05)
	return 0 if status == 0 else 1
```

```python
_CLI_HANDLERS = {'read': _cliRead,
```

In `TSVZ.py`, replace

```python
				 'popitem': _cliEngineOp, 'setdefault': _cliEngineOp}
```

with

```python
				 'popitem': _cliEngineOp, 'setdefault': _cliEngineOp, 'serve': _cliServe, 'stop': _cliStop}
```

- [ ] **Step 4: Run the tests**

Run: `python3 -m pytest TSVZ_test.py -q` three times.
Expected: all pass every time (the SIGTERM test starts a server and signals it at once).

Run: `python3 TSVZ.py serve /tmp/served-check.tsvz --x-idle-timeout 1; echo "exit $?"; ls /tmp/served-check.tsvz*`
Expected: `tsvz: serving /tmp/served-check.tsvz at unix:... (pid N)`, then after a second `idle for 1 s; stopping`, `stopped serving`, `exit 0`; only `/tmp/served-check.tsvz` is left (no `.serve` file). Remove it with `rm -f /tmp/served-check.tsvz`.

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_test.py
git commit -q -F - <<'EOF'
Serve a store over a local socket with tsvz serve, and stop it with tsvz stop.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
EOF
```

---

### Task 6: Route `tsvz` commands through a running server (spec §20.8)

**Files:**
- Modify: `TSVZ.py` (`_cliHandler`, `_cliRouted`, `_cliMain`)
- Modify: `TSVZ_test.py`

**Interfaces:**
- Consumes: Task 5's `_serveFind`, `_ServeConnection`, `_ServeUnreachable`, `_servePointerPath`; Task 4's `_cliRequestLine`, `_cliEmitServed`, `_serveRecordLine`, `_ServeLost`; Task 2's `_parseRecordLines`.
- Produces: `_CLI_READ_ONLY`, `_cliHandler(args, delimiter, logger) -> connection|None`, `_cliRouted(connection, args, delimiter, logger, stdin, stdout) -> status`.

The routing rule: forward only when the result cannot depend on what the server does not share. That means no `--x-direct`, `--x-header`, `--x-defaults` or `--x-strict`, and for a loose file the server's own delimiter (`x-delimiter` in the pointer).

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ_test.py`, above the final `if __name__ == '__main__':` block:

```python

# ==========================================================================
# CLI through a handler (spec §20.8)
# ==========================================================================
_ROUTED_STEPS = (
	(['read'], ''), (['get', 'alice', 'nobody'], ''), (['has', 'alice'], ''), (['len'], ''), (['keys'], ''),
	(['set', 'bob', 'B<b', 'two words'], ''), (['set', '#tag', 'v'], ''), (['set', '#_defaults_#', '', 'NA'], ''),
	(['get', 'carol', 'bob'], ''), (['delete', 'alice', 'nobody'], ''),
	(['set', '-'], 'k1\tv1\n# comment\nk2\tv<sep>2\n'), (['delete', '-'], 'k1\tignored\n'),
	(['pop', 'bob'], ''), (['pop', 'bob'], ''), (['popitem', 'first'], ''), (['setdefault', 'k', 'v', '-5'], ''),
	(['setdefault', 'k', 'other'], ''), (['read', '--format', 'table'], ''), (['verify'], ''),
	(['scrub'], ''), (['read'], ''), (['clear'], ''), (['read'], ''), (['popitem'], ''),
)


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
@pytest.mark.parametrize('name', ['d.tsvz', 'd.tsv'])
def test_cli_gives_the_same_results_through_a_handler(tmp_path, served, name):
	"""Spec §20.8: every operation prints the same and exits the same with or without a handler."""
	direct, routed = str(tmp_path / ('direct-' + name)), str(tmp_path / ('routed-' + name))
	start = b'alice\tAlice\t30\n<#>h\tx\ty\n' if name.endswith('z') else b'alice\tAlice\t30\nh\tx\ty\n'
	for path in (direct, routed):
		_touch(path, start)
	served(routed)
	for argv, stdin in _ROUTED_STEPS:
		first = _run(argv[0], direct, *argv[1:], stdin=stdin)
		second = _run(argv[0], routed, *argv[1:], stdin=stdin)
		assert first[:2] == second[:2], argv
	assert open(direct, 'rb').read() == open(routed, 'rb').read()


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_cli_routes_only_what_the_handler_can_answer(tmp_path, served, monkeypatch):
	p = str(tmp_path / 'v.tsvz')
	_touch(p, b'a\t1\n')
	served(p)
	requests = []
	real = TSVZ._serveRespond

	def spy(store, line, bulk=None, logger=None):
		requests.append(line)
		return real(store, line, bulk, logger)
	monkeypatch.setattr(TSVZ, '_serveRespond', spy)
	assert _run('get', p, 'a') == (0, 'a\t1\n', '')
	assert _run('--x-direct', 'get', p, 'a') == (0, 'a\t1\n', '')
	assert _run('get', p, 'a', '--x-defaults', 'k\\tD') == (0, 'a\t1\n', '')  # an option the handler lacks
	q = str(tmp_path / 'v.txt')
	_touch(q, b'a|1\n')
	served(q)  # a loose file served with the delimiter its name implies (tab)
	assert _run('get', q, 'a', '-d', 'pipe')[:2] == (0, 'a|1\n')  # 3.39 warns about the name on stderr
	assert requests == ['get\ta']


def test_cli_falls_back_to_the_files_when_the_handler_is_gone(tmp_path):
	import socket
	p = str(tmp_path / 'f.tsvz')
	_touch(p, b'a\t1\n')
	_touch(p + '.serve', 'tsvz-handler\t1\naddress\tunix:{}\nhost\t{}\npid\t1\n'.format(
		str(tmp_path / 'gone.sock'), socket.gethostname()).encode())
	status, out, err = _run('get', p, 'a')
	assert (status, out) == (0, 'a\t1\n') and 'does not answer' in err
	_touch(p + '.serve', b'tsvz-handler\t1\naddress\tunix:/nowhere\nhost\tsome-other-host\npid\t1\n')
	assert _run('get', p, 'a') == (0, 'a\t1\n', '')  # another host's handler: skipped quietly
	assert _run('stop', p)[0] == 1


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_cli_when_the_handler_drops_the_connection(tmp_path):
	"""A read falls back to the files; a write reports that its outcome is unknown."""
	import socket
	path = str(tmp_path / 'drop.sock')
	listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	listener.bind(path)
	listener.listen(8)

	def drop():
		while True:
			try:
				conn, _ = listener.accept()
			except OSError:
				return
			conn.recv(1024)
			conn.close()
	thread = threading.Thread(target=drop)
	thread.daemon = True
	thread.start()
	p = str(tmp_path / 'd.tsvz')
	_touch(p, b'a\t1\n')
	_touch(p + '.serve', 'tsvz-handler\t1\naddress\tunix:{}\nhost\t{}\npid\t1\n'.format(
		path, socket.gethostname()).encode())
	try:
		status, out, err = _run('read', p)
		assert (status, out) == (0, 'a\t1\n') and 'lost the handler' in err
		status, out, err = _run('set', p, 'b', '2')
		assert (status, out) == (1, '') and 'may or may not have been applied' in err
	finally:
		listener.close()


def test_tsvz_commands_through_tsvz_serve(tmp_path):
	p = str(tmp_path / 'e.tsvz')
	proc = _serve_process(p)
	try:
		assert _cli('set', p, 'k', 'v').returncode == 0
		r = _cli('get', p, 'k')
		assert (r.returncode, r.stdout) == (0, 'k\tv\n')
		r = subprocess.run([sys.executable, os.path.join(HERE, 'TSVZ.py'), 'set', p, '-'], input='j\tw\n',
						   stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
		assert r.returncode == 0 and _cli('read', p).stdout == 'k\tv\nj\tw\n'
		assert _cli('stop', p).returncode == 0
		assert proc.wait(10) == 0
	finally:
		if proc.poll() is None:
			proc.kill()
	assert open(p, 'rb').read() == b'k\tv\nj\tw\n'
```

- [ ] **Step 2: Run them to verify they fail**

Run: `python3 -m pytest TSVZ_test.py -q -k "handler or routes or through"`
Expected: `test_cli_routes_only_what_the_handler_can_answer`, `test_cli_falls_back_to_the_files_when_the_handler_is_gone` and `test_cli_when_the_handler_drops_the_connection` FAIL (no request reaches the server; no warning). The differential tests may already pass, because without routing both sides use the files.

- [ ] **Step 3: Route**

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
_CLI_READ_ONLY = ('read', 'get', 'has', 'len', 'keys', 'verify', 'parts')


def _cliHandler(args, delimiter, logger):
	"""A connection to STORE's handler when this command line may go through it (spec §20.8), else None.

	TSVZ routes a command line only when its result does not depend on
	options the handler does not share: no --x-direct, --x-header,
	--x-defaults or --x-strict, and for a loose file the handler's own
	delimiter. A pointer whose handler does not answer is reported and the
	files are used; another host's handler, or one this user may not
	connect to, is skipped quietly.
	"""
	if args.operation in ('serve', 'stop') or args.direct or args.header or args.defaults or args.strict:
		return None
	info = _serveFind(args.store)
	if info is None:
		return None
	if not _isSpecPath(args.store) and info.get('x-delimiter', delimiter) != delimiter:
		return None
	try:
		return _ServeConnection(info)
	except _ServeUnreachable as e:
		if not e.quiet:
			logger.teelog('TSVZ warning: {}: the handler in {} does not answer ({}); using the files directly'.format(
				args.store, _servePointerPath(args.store), e), 'warning')
		return None


def _cliRouted(connection, args, delimiter, logger, stdin, stdout):
	"""Run a command line through STORE's handler; print the response as the files would give it (§20.8)."""
	bulk = None
	if args.operation in ('set', 'append', 'delete') and args.args == ['-']:
		source = _Reporter('<stdin>', logger)
		try:
			rows = _parseRecordLines(_cliStdinLines(stdin, source), delimiter, _isSpecPath(args.store),
									 args.operation == 'delete', source)
		finally:
			source.flush()
		bulk = [_serveRecordLine(cells, marker) for cells, marker in rows]
	try:
		response = connection.request(_cliRequestLine(args), bulk)
	except _ServeLost as e:
		if args.operation in _CLI_READ_ONLY:
			logger.teelog('TSVZ warning: {}: lost the handler ({}); using the files directly'.format(args.store, e),
						  'warning')
			return _CLI_HANDLERS[args.operation](args, delimiter, logger, stdin, stdout)
		logger.teelog('tsvz: {}: lost the handler ({}); the {} may or may not have been applied'.format(
			args.store, e, args.operation), 'error')
		return 1
	return _cliEmitServed(args, delimiter, logger, stdout, response)
```

```python
_CLI_HANDLERS = {'read': _cliRead,
```

In `TSVZ.py`, replace

```python
			return _CLI_HANDLERS[args.operation](args, delimiter, logger, stdin, stdout)
	except _CliUsageError as e:
```

with

```python
			connection = _cliHandler(args, delimiter, logger)
			if connection is not None:
				try:
					return _cliRouted(connection, args, delimiter, logger, stdin, stdout)
				finally:
					connection.close()
			return _CLI_HANDLERS[args.operation](args, delimiter, logger, stdin, stdout)
	except _CliUsageError as e:
```

- [ ] **Step 4: Run the tests**

Run: `python3 -m pytest TSVZ_test.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_test.py
git commit -q -F - <<'EOF'
Route tsvz command lines through a running write handler.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
EOF
```

---

### Task 7: `TSVZClient`

**Files:**
- Modify: `TSVZ.py` (new class above the CLI block)
- Modify: `TSVZ_test.py`

**Interfaces:**
- Consumes: Task 5's `_serveFind`, `_ServeConnection`, `_ServeUnreachable`; Task 4's `_ServeLocal`, `_serveEncode`, `_serveDecodeLine`, `_serveRecordLine`, `_ServeLost`; Task 3's `_ServeStore`.
- Produces: public `TSVZ.TSVZClient(fileName, sync=False, timeout=None, teeLogger=None)`, a MutableMapping:
  - reads: `[]`, `get`, `in`, `len`, iteration, `keys()`, `values()`, `items()` (lists);
  - writes: `[]=`, `del`, `pop`, `popitem`, `setdefault`, `update`, `clear`, `setDefaults`;
  - other: `dialect`, `close`, context manager;
  - `move_to_end`, `rewrite`, `mapToFile` and `hardMapToFile` raise `NotImplementedError`.

Retries: a request whose connection is lost is sent once more, to a new connection or to the in-process engine. The exceptions are `pop`, `popitem` and `setdefault`, which may already have been applied; those raise `ConnectionError`.

- [ ] **Step 1: Write the failing tests**

Append to `TSVZ_test.py`, above the final `if __name__ == '__main__':` block:

```python

# ==========================================================================
# TSVZClient: a served store from Python (spec §21)
# ==========================================================================
def _mapping_story(t):
	"""Run the same dict operations on a TSVZed or a TSVZClient; return what they returned."""
	seen = []
	t['alice'] = ['alice', 'Alice', '30']
	t['bob'] = 'bob\tBob\t7'
	t['carol'] = ['Carol', '5']  # the key is put first
	seen += [t['bob'], t.get('nobody', 'missing'), 'alice' in t, 'zed' in t, len(t), list(t)]
	del t['bob']
	seen += [t.pop('carol'), t.pop('carol', 'gone')]
	seen += [t.setdefault('dave', ['dave', 'D', '1']), t.setdefault('dave', ['dave', 'X', '2'])]
	t.update({'erin': ['erin', 'E', '2'], 'fay': 'fay\tF\t3'})
	seen += [sorted(t.keys()), t.popitem(), t.popitem(last=False)]
	del t['nobody']  # a missing key is not an error
	return seen


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
@pytest.mark.parametrize('ext', ['tsvz', 'tsv'])
def test_tsvz_client_behaves_like_tsvzed(tmp_path, served, ext):
	header = 'id\tname\tn' if ext == 'tsvz' else ''  # a .tsv header line is a row when served, as in `tsvz read`
	paths = [str(tmp_path / '{}.{}'.format(name, ext)) for name in ('tsvzed', 'served', 'local')]
	for path in paths[1:]:
		TSVZ.appendLinesTabularFile(path, [], header=header, createIfNotExist=True)
	served(paths[1])
	with TSVZ.TSVZed(paths[0], header=header) as t:
		expected = _mapping_story(t)
	results = []
	for path in paths[1:]:
		with TSVZ.TSVZClient(path) as c:
			results.append(_mapping_story(c))
			assert c.items() == [(row[0], row) for row in _view(path)]
	assert results == [expected, expected]
	assert expected[:6] == [['bob', 'Bob', '7'], 'missing', True, False, 3, ['alice', 'bob', 'carol']]
	views = [[row for row in _view(path) if row[0] != 'id'] for path in paths]
	assert views[0] == views[1] == views[2] == [['dave', 'D', '1'], ['erin', 'E', '2']]


def test_tsvz_client_missing_keys_follow_the_store(tmp_path):
	p = str(tmp_path / 'm.tsvz')
	_touch(p, b'#_defaults_#\t\tNA\na\t1\t2\n')
	with TSVZ.TSVZClient(p) as c:
		assert c['zed'] == ['zed', '', 'NA'] and c.get('zed') is None and 'zed' not in c
		c['#_return_defaults_when_missing_#'] = ['false']  # a marker write
		with pytest.raises(KeyError):
			c['zed']
		c.setDefaults(['', 'X'])
		assert c['a'] == ['a', '1', '2'] and open(p, 'rb').read().endswith(
			b'#_return_defaults_when_missing_#\tfalse\n#_defaults_#\t\tX\n')
		for method in ('move_to_end', 'rewrite', 'mapToFile', 'hardMapToFile'):
			with pytest.raises(NotImplementedError):
				getattr(c, method)('a')


def test_tsvz_client_warns_once_when_no_handler_runs(tmp_path, capsys):
	p = str(tmp_path / 'w.tsvz')
	with TSVZ.TSVZClient(p) as c:
		c['k'] = ['k', 'v']
		assert c['k'] == ['k', 'v'] and len(c) == 1
		assert repr(c) == 'TSVZClient({!r}, local)'.format(p)
	err = capsys.readouterr().err
	assert err.count('working on the files in this process') == 1
	assert open(p, 'rb').read() == b'k\tv\n'


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_tsvz_client_sync_asks_for_disk_acknowledgement(tmp_path, served, monkeypatch):
	p = str(tmp_path / 's.tsvz')
	served(p)
	lines = []
	real = TSVZ._serveRespond
	monkeypatch.setattr(TSVZ, '_serveRespond', lambda store, line, bulk=None, logger=None: (
		lines.append(line), real(store, line, bulk, logger))[1])
	with TSVZ.TSVZClient(p, sync=True) as c:
		c['k'] = 'k\tv'
		assert c['k'] == ['k', 'v']
	assert lines == ['--sync\tset\tk\tv', 'get\tk']


@pytest.mark.skipif(not hasattr(__import__('socket'), 'AF_UNIX'), reason='Unix sockets')
def test_tsvz_client_reconnects_or_falls_back(tmp_path, served):
	p = str(tmp_path / 'r.tsvz')
	first = served(p)
	client = TSVZ.TSVZClient(p)
	try:
		client['k'] = ['k', 'v']
		first.stop()
		first.thread.join(10)
		first.close()
		second = served(p)
		assert client['k'] == ['k', 'v']  # the lost connection was replaced by one to the new handler
		assert repr(client).endswith('served)')
		second.stop()
		second.thread.join(10)
		second.close()
		with pytest.raises(ConnectionError):
			client.pop('k')  # not retried: it may have been applied
		assert repr(client).endswith('local)') and client['k'] == ['k', 'v']
	finally:
		client.close()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `python3 -m pytest TSVZ_test.py -q -k tsvz_client`
Expected: 6 FAIL with `AttributeError: module 'TSVZ' has no attribute 'TSVZClient'`.

- [ ] **Step 3: Add the client**

In `TSVZ.py`, insert this directly above the lines below, followed by two blank lines:

```python
class TSVZClient(MutableMapping):
	"""A store kept by ``tsvz serve`` (spec §21), as a mapping with ``TSVZed``'s semantics.

	Every operation is one request to the handler that the store's pointer
	file names, so many processes share one loaded store and one writer.
	Without a running handler (or when it goes away) the client warns once
	and runs the same engine in this process: the code keeps working, it just
	does not share the loaded store.

	- ``c[key]`` returns the row; for a missing key of a ``.tsvz`` store the
	  defaults row while ``#_return_defaults_when_missing_#`` is true (spec
	  §14.5), else ``KeyError``. ``get``, ``in``, ``len`` and iteration behave
	  like a dict; ``keys()``, ``values()`` and ``items()`` return lists.
	- ``c[key] = value`` normalises ``value`` as ``TSVZed`` does (a string is
	  split on the delimiter, cells are right-stripped, the key goes first).
	- ``sync=True`` acknowledges every write only after it is on disk.
	- ``timeout``: seconds to wait for each answer (None: no limit).
	"""

	def __init__(self, fileName, sync=False, timeout=None, teeLogger=None):
		self._fileName = fileName
		self._sync = sync
		self._timeout = timeout
		self.teeLogger = teeLogger
		self._spec = _isSpecPath(fileName)
		if self._spec:
			self.delimiter = _EXTENSION_DELIMITERS[_parsePartName(fileName).ext]
		else:
			self.delimiter = get_delimiter(..., file_name=fileName)
		self._backend = None
		self._connect()

	def _connect(self):
		info = _serveFind(self._fileName)
		reason = 'no handler is running'
		if info is not None:
			try:
				self._backend = _ServeConnection(info, self._timeout)
				return
			except _ServeUnreachable as e:
				reason = 'its handler cannot be used ({})'.format(e)
		_warnOnce(self, 'local', self._fileName, '{}; working on the files in this process'.format(reason),
				  self.teeLogger)
		self._backend = _ServeLocal(_ServeStore(self._fileName, teeLogger=self.teeLogger, create=True))

	def _request(self, fields, write=False, bulk=None, retry=True):
		"""Send a request; return ``(lines, status, message)``. Usage errors raise ``ValueError``."""
		line = '\t'.join((['--sync'] if write and self._sync else []) + fields)
		try:
			lines, diagnostics, status, message = self._backend.request(line, bulk)
		except _ServeLost as e:
			self._backend.close()
			self._connect()
			if not retry:
				raise ConnectionError('the tsvz handler of {} went away ({}); {} may or may not have been applied'
									  .format(self._fileName, e, fields[0]))
			lines, diagnostics, status, message = self._backend.request(line, bulk)
		for text in diagnostics:
			_warn(text, self.teeLogger)
		if status == 2:
			raise ValueError(message)
		if status == 1 and write:
			raise OSError(message)
		if status == 1:
			_warn('TSVZ warning: {}: {}'.format(self._fileName, message), self.teeLogger)
		return lines, status, message

	def _cells(self, key, value):
		"""``value`` normalised as ``TSVZed.__setitem__`` does, with ``key`` first."""
		key = str(key).rstrip()
		if isinstance(value, str):
			value = value.split(self.delimiter)
		cells = [str(cell).rstrip() if cell else '' for cell in value]
		if not cells or cells[0] != key:
			cells = [key] + cells
		return cells

	@staticmethod
	def _fields(cells):
		return [_serveEncode(cell) for cell in cells]

	@property
	def dialect(self):
		"""'tsvz' for tsvz-spec-v1 files (.tsvz/.csvz/.nsvz/.psvz), 'tsv' for 3.39-format files."""
		return 'tsvz' if self._spec else 'tsv'

	def __getitem__(self, key):
		lines = self._request(['get', _serveEncode(str(key))])[0]
		if not lines:
			raise KeyError(key)
		return _serveDecodeLine(lines[0])

	def get(self, key, default=None):
		lines, status, _ = self._request(['get', _serveEncode(str(key))])
		return _serveDecodeLine(lines[0]) if status == 0 and lines else default

	def __contains__(self, key):
		return self._request(['has', _serveEncode(str(key))])[1] == 0

	def __len__(self):
		return int(self._request(['len'])[0][0])

	def __iter__(self):
		return iter(self.keys())

	def keys(self):
		return [_specDecodeField(line, '\t') for line in self._request(['keys'])[0]]

	def values(self):
		return [_serveDecodeLine(line) for line in self._request(['read'])[0]]

	def items(self):
		return [(row[0], row) for row in self.values()]

	def __setitem__(self, key, value):
		cells = self._cells(key, value)
		if not cells[0]:
			_warn('TSVZ error: {}: a key cannot be empty'.format(self._fileName), self.teeLogger)
			return
		self._request(['set'] + self._fields(cells), write=True)

	def __delitem__(self, key):
		"""Delete ``key``; a missing key is not an error (as in ``TSVZed``)."""
		self._request(['delete', _serveEncode(str(key))], write=True)

	_marker = object()

	def pop(self, key, default=_marker):
		lines, status, _ = self._request(['pop', _serveEncode(str(key))], write=True, retry=False)
		if lines:
			return _serveDecodeLine(lines[0])
		if default is TSVZClient._marker:
			raise KeyError(key)
		return default

	def popitem(self, last=True):
		lines = self._request(['popitem', 'last' if last else 'first'], write=True, retry=False)[0]
		if not lines:
			raise KeyError('popitem(): {} is empty'.format(self._fileName))
		row = _serveDecodeLine(lines[0])
		return row[0], row

	def setdefault(self, key, default=None):
		"""The row of ``key``; when it is missing, first set it to ``default`` (None: only look it up)."""
		if default is None:
			return self.get(key)
		cells = self._cells(key, default)
		if len(cells) < 2:
			return self.get(key)
		lines = self._request(['setdefault'] + self._fields(cells), write=True, retry=False)[0]
		return _serveDecodeLine(lines[0]) if lines else None

	def update(self, other=(), **kwds):
		"""Set many keys with one request (one batch append)."""
		pairs = list(other.items()) if hasattr(other, 'items') else list(other)
		pairs += list(kwds.items())
		bulk = []
		for key, value in pairs:
			cells = self._cells(key, value)
			if cells[0]:
				bulk.append(_serveRecordLine(cells, bool(_MARKER_RE.match(cells[0]))))
		if bulk:
			self._request(['set', '-'], write=True, bulk=bulk)

	def clear(self):
		self._request(['clear'], write=True)

	def setDefaults(self, defaults):
		"""Write ``defaults`` as the store's ``#_defaults_#`` marker (as ``TSVZed.setDefaults``)."""
		if isinstance(defaults, str):
			defaults = defaults.split(self.delimiter)
		cells = [str(cell).rstrip() if cell else '' for cell in (defaults or [])]
		if not cells or cells[0] != DEFAULTS_INDICATOR_KEY:
			cells = [DEFAULTS_INDICATOR_KEY] + cells
		self._request(['set'] + self._fields(cells), write=True)

	def move_to_end(self, key, last=True):
		raise NotImplementedError('a served store keeps first-appearance order (spec §3.4)')

	def rewrite(self, *args, **kwargs):
		raise NotImplementedError('a served store is compacted with `tsvz scrub`')

	mapToFile = hardMapToFile = rewrite

	def close(self):
		if self._backend is not None:
			self._backend.close()
			self._backend = None

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.close()

	def __repr__(self):
		return 'TSVZClient({!r}, {})'.format(self._fileName, 'local' if isinstance(self._backend, _ServeLocal)
											   else 'served')
```

```python
# ===========================================================================
# Command-line interface (tsvz-spec-v1 §20)
```

- [ ] **Step 4: Run the tests**

Run: `python3 -m pytest TSVZ_test.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add TSVZ.py TSVZ_test.py
git commit -q -F - <<'EOF'
Add TSVZClient, a mapping over a served store with an in-process fallback.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
EOF
```

---

### Task 8: README, Python 3.6 runs and the final check

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: everything above.
- Produces: user documentation of `tsvz serve`, the new operations and `TSVZClient`.

- [ ] **Step 1: README**

In `README.md`, replace

```markdown
tsvz parts events.tsvz                    # list the parts of a multi-part store
```

with

```markdown
tsvz parts events.tsvz                    # list the parts of a multi-part store
tsvz has people.tsvz carol                # exit 0 if carol is live, 3 if not
tsvz pop people.tsvz carol                # print carol's row and delete it
tsvz serve people.tsvz &                  # keep the store loaded; tsvz commands then use it
tsvz stop people.tsvz
```

In `README.md`, insert this directly above the lines below, followed by one blank line:

````markdown
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
  `--x-group G` or `--x-mode 0660` opens it on purpose. `--x-tcp` listens on
  127.0.0.1 with a token in the pointer file instead.
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
`setDefaults`. Without a server it warns once and works on the files in-process.
````

```markdown
## Fault tolerance
```

In `README.md`, replace

```markdown
- §20.6: a write whose store cannot be created, and a `read`, `get` or `verify` that cannot read every part, exit 1 with an error that `-q` keeps.
```

with

```markdown
- §20.6: a write whose store cannot be created, and a `read`, `get` or `verify` that cannot read every part, exit 1 with an error that `-q` keeps.
- §21: a served `.tsv` file with a header line shows that line as a row, as `tsvz read` does (`TSVZed` hides it when given the header).
- §21.8: the server notices changes with `stat()`. It re-reads the part list while the store's directory changed less than 2 s ago, but a rewrite in place that keeps a part's size and modification time is not seen until the part changes again.
- §17.7: the server appends to the store's current part; it does not start a new part when it starts.
- §21.3: where Python has no Unix sockets (Windows), the server uses loopback TCP with a token. The test suite exercises the TCP transport on Linux only.
```

- [ ] **Step 2: Run the suite on Python 3.6 and 3.7**

```bash
for image in python:3.6-slim python:3.7-slim; do
  docker run --rm -v "$PWD":/w -w /w "$image" sh -c 'pip install -q "pytest<7.1" && python -m pytest TSVZ_test.py -q -p no:cacheprovider' || echo "FAILED on $image"
done
docker run --rm -v "$PWD":/w -w /w -e LANG=C -e LC_ALL=C python:3.6-slim sh -c 'pip install -q "pytest<7.1" && python -m pytest TSVZ_test.py -q -p no:cacheprovider' || echo "FAILED under LANG=C"
```

Expected: three passing runs, no `FAILED` line. Run the 3.6 image twice more: the server tests use threads and timestamps.

- [ ] **Step 3: Final check**

Run: `python3 -m pytest TSVZ_test.py -q && git status --short`
Expected: all tests pass; `git status` lists only `README.md` (and `.coverage` if it was already there).

Run: `python3 -X importtime -c "import TSVZ" 2>&1 | grep -E "socket|TSVZ" | tail -3`
Expected: no `socketserver` line: importing TSVZ does not load the server modules.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -q -F - <<'EOF'
Document tsvz serve, the new operations and TSVZClient.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
EOF
```
