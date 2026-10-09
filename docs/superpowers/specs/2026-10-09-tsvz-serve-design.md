# TSVZ write handler (`tsvz serve`) — tsvz-spec-v1 §21

## Intent

Give a TSVZ store a long-running owner process, the "dedicated write handler"
that spec §18.3 recommends, reachable over a local socket. Repeated `tsvz`
commands and many Python processes then share one in-memory copy of the store
instead of re-reading it on every call, and all their writes go through one
appender that batches them.

This is spec B of two. Spec A, the command line (§20,
`2026-10-08-tsvz-cli-design.md`), is merged; its operation names and record
encoding were chosen so this protocol could reuse them. The user's requirement:
the handler supports every normal `TSVZed` operation, especially getting and
setting single values.

The design rules of TSVZ 4.1 still apply: fault tolerance first (keep the data,
keep going, report ambiguity on stderr), compatibility with TSVZ 3.39 unless the
spec forbids it, Python ≥ 3.6, standard library only, one file.

### Decisions taken during design

| Question | Decision |
|---|---|
| Clients | The `tsvz` CLI, Python processes (`TSVZClient`), and any language through a plain-text protocol. Local machine only. |
| Discovery | A pointer file `STORE.serve` beside the store, holding the address; locked while the server runs |
| Writers that bypass the server | Followed: the server replays new committed bytes before answering |
| Lifecycle | Explicit `tsvz serve STORE` in the foreground; optional idle timeout; clients never start servers |
| Python API | A new `TSVZClient` MutableMapping; `TSVZed` and `TSVZedLite` unchanged |
| Access | Socket mode 0600 by default; `--x-group` / `--x-mode` open it on purpose |
| Write acknowledgement | `#_write_ack_#` decides (`memory` default, `disk`); a request can ask for `--sync` |
| Dialects | Both: strict variants per §21, `.tsv`-family files under 3.39 rules (implementation-defined) |
| Engine | A dedicated replay engine: the server's view is always a replay of the file |
| Protocol | §20's vocabulary over TAB-separated, §13-encoded text lines; a `#<status>` line ends each response |
| New operations | `has`, `len`, `keys`, `pop`, `popitem`, `setdefault` (added to §20 too); `serve`, `stop` |
| Placement | New §21 of `tsvz-spec-v1.md`, a "conformant handler" class in §2.1, examples in Appendix D |

## 1. Spec changes outside §21

**§2.1** gains a fourth conformance class:

> A **conformant handler** implements §21. It MUST also be a conformant reader and
> a conformant writer for the strict variants.

**§18.3** gains a pointer: "§21 specifies a protocol for such a handler."

**§20** changes (all additive; nothing in §20 as merged changes meaning):

- §20.2 gains six required operations. A CLI tool performs them on the files
  directly when no handler answers, and through the handler (§21) when one does.

  | Operation | Arguments | Effect |
  |---|---|---|
  | `has` | `STORE KEY` | Print nothing. Exit 0 if KEY is live, 3 if not. |
  | `len` | `STORE` | Print the number of live keys. |
  | `keys` | `STORE` | Print each live key, one per line, in first-appearance order, encoded as a record's first field (§13, `<#>` for a leading `#`). |
  | `pop` | `STORE KEY` | Print KEY's row and append a tombstone for it. If KEY is missing, print nothing, write nothing and exit 3. |
  | `popitem` | `STORE [first\|last]` | As `pop`, for the first or last live key (default `last`). Exit 3 if the store is empty. |
  | `setdefault` | `STORE KEY VALUE [VALUE ...]` | If KEY is live, print its row. Otherwise append the record `KEY VALUE ...` and print the row a reader now returns for KEY. |

  `pop`, `popitem` and `setdefault` are read-modify-write. Done directly on the
  files they are not atomic with respect to other writers; through a handler
  they are (§21.8).
- `serve STORE` and `stop STORE` are defined by §21.13. A CLI tool that does not
  provide a handler MUST exit 1 with a diagnostic for them (not 2: the names are
  defined).
- New §20.8 **Routing through a handler.** A CLI tool MAY forward an operation to
  a handler it finds through §21.2. Standard output and the exit status MUST then
  be what the operation would produce without the handler, given the same store
  contents; diagnostics MAY differ. A tool SHOULD fall back to operating on the
  files, with a warning, when the pointer is stale or the handler does not
  answer.

**Appendix D** gains `tsvz serve`, a `socat` session and a `TSVZClient` example.

The spec's version and the file-format version (`#_version_#`) are unchanged.

## 2. §21 Write handler protocol (normative content)

The subsections below are the requirements §21 states. The implementation plan
turns them into spec prose; the wording may change, the requirements may not.

### 21.1 Purpose

- A handler is a process that owns a store: it keeps the store's resolved state,
  answers requests about it, and appends every write it receives (§18.3).
- A handler MUST NOT be the only possible writer: other writers MAY append to the
  store directly, and the handler MUST follow them (§21.8).
- A handler serves exactly one store.

### 21.2 Pointer file

- The pointer file of a store is the store path (§17.1, as given, including any
  compression suffix) followed by `.serve`, e.g. `data.tsvz.serve`. It is not a
  part (Appendix A does not match it).
- Its content is UTF-8 lines of `name<TAB>value`, values §13-encoded:

  ```
  tsvz-handler	1
  address	unix:/run/user/1000/tsvz-k3x9/s
  host	node17
  pid	41234
  ```

  The first line names the format and the protocol version (1). `address` is
  `unix:PATH` or `tcp:127.0.0.1:PORT` (`tcp:[::1]:PORT`); a TCP address MUST be a
  loopback address and the file MUST then also hold `token<TAB>HEX` (§21.11).
  Unknown names MUST be ignored.
- A handler MUST hold an exclusive advisory lock on its pointer file for as long
  as it serves, and MUST refuse to start when that lock is held.
- A handler writes the pointer file in place under its lock (it does not rename
  over it), and removes it when it stops cleanly.
- A client MUST treat the pointer as stale, and not use it, when its `host` is not
  the client's host, when it does not parse, or when connecting fails. A stale
  pointer is replaced by the next handler that obtains the lock.

### 21.3 Transport

- A stream socket on the local host: a Unix-domain socket where available, else
  TCP on a loopback address.
- A connection carries any number of requests, one at a time: a client sends a
  request and reads its whole response before sending the next.

### 21.4 Framing and encoding

- UTF-8 text lines, each ending in `\n`.
- Fields are separated by TAB and encoded per §13 with TAB as the delimiter,
  whatever the store's variant: `<sep>` is TAB, plus `<LF>`, `<lt>`, `<#>`.
- In a request, a field is decoded per §13 before use. A first field (KEY) that
  matches the reserved pattern (§12.2) unencoded is a marker key, as in §20.2;
  `<#>` makes a leading `#` part of a data key.

### 21.5 Requests

- `[OPTION ...] OPERATION [ARG ...]`: the §20.1 form without STORE.
- Fields before the operation that begin with `--` are options. Version 1
  defines one: `--sync`, which makes the handler acknowledge this request's
  writes only after they are on durable storage (§21.9).

### 21.6 Responses

A response is zero or more lines followed by exactly one status line:

- **Record lines** — rows, encoded as `read --format records` prints them in the
  TSVZ variant (§20.4.1). Other outputs (`len`, `keys`, `verify`, `parts`) use
  the line formats of §20.
- **Diagnostic lines** — `#!<TAB>TEXT`. A CLI copies TEXT to standard error.
- **Status line** — `#` followed by the §20.6 exit status in decimal, optionally
  `<TAB>MESSAGE` (§13-encoded): `#0`, `#3`, `#1<TAB>scrub refused: …`. It ends the
  response.

Record and output lines never begin with an unencoded `#` (§13: `<#>`; markers
are not printed), so the line type is decided by the first two characters.

### 21.7 Operations

Every operation of §20.2 (`read`, `get`, `set`, `append`, `delete`, `clear`,
`scrub`, `verify`, `parts`, `has`, `len`, `keys`, `pop`, `popitem`,
`setdefault`) with the effect, output and status that §20 gives it, plus:

| Operation | Effect |
|---|---|
| `stop` | Write and make durable everything acknowledged, reply `#0`, then stop serving (§21.13). |

- **Bulk input.** `set -` and `delete -`: the request line is followed by lines
  in the protocol encoding, handled as §20.3 handles standard input, and ended
  by a line that is exactly `#`. All of them are appended as a single batch.
- `setdefault` with no VALUE is a usage error (`#2`).

### 21.8 Consistency

- A response reflects every write the handler acknowledged before it received
  the request, and every record committed to the store's files (§4.3) before it
  received the request, subject to the file system's visibility rules.
- `pop`, `popitem` and `setdefault` are atomic: no other request's write is
  applied between their read and their write.
- Writes received on one connection are applied in the order received.
- The handler appends with whole-record `write()`s (§18.2) to one part of the
  store, the part its appends go to (§17, §17.7), using the same locks as any
  other writer (§18.6).
- The handler's state is what a conformant reader would return for the store's
  files at that moment. A handler that cannot read a part reports that request
  with status 1 (and still returns what it could read).

### 21.9 Acknowledgement

- `#_write_ack_#` `memory` (the default): a write is acknowledged once it is in
  the handler's batch buffer. The handler SHOULD write the batch without
  deliberate delay.
- `#_write_ack_#` `disk`, or the request option `--sync`: a write is acknowledged
  only after its batch is written and `fsync`'d.
- `stop` and a clean shutdown write and `fsync` every acknowledged write first.

### 21.10 Errors

- A line that does not parse, an unknown operation or an unknown option: `#2`;
  the connection stays usable.
- A handler MAY close a connection whose line exceeds an implementation limit of
  at least 1 MiB.
- Any other failure of a request: `#1` with a message; the handler keeps serving.

### 21.11 Access

- A handler SHOULD by default accept connections only from the user it runs as
  (e.g. a Unix socket of mode 0600 in a directory only that user can enter).
  Wider access MUST be an explicit choice of whoever starts it.
- A TCP handler MUST require authentication: the client's first line is
  `auth<TAB>TOKEN`, with TOKEN from the pointer file; the handler answers `#0`,
  or `#1` and closes. The pointer file's permissions then govern access.

### 21.12 Extensions

As §20.7.2: operations `x-…`, options `--x-…`, statuses 64–125 belong to
implementations.

### 21.13 `serve` and `stop` (CLI)

- `tsvz serve STORE` starts a handler for STORE in the foreground. It creates a
  missing store (as `set` does), exits 1 when the pointer lock is held (another
  handler serves STORE), and exits 0 after a clean stop.
- `tsvz stop STORE` asks the handler of STORE to stop (`stop` request) and exits
  0 once it has replied; exit 1 when no live handler is found.

## 3. TSVZ implementation

New code in `TSVZ.py`, next to the CLI block.

**`_ServeStore`, the engine.** One store's live view:

- State: the rows dict (insertion order = first appearance), the spec reader
  state (`_SpecState`), or for `.tsv` 3.39's column count and defaults; per part,
  the read offset, inode and size.
- `sync()`: checks the store directory's mtime (part list) and the active part's
  inode and size. Growth of the active (uncompressed) part: replay the newly
  committed bytes through the reader's own record processing (`_specProcessRecord`
  / `_processLine`); an unterminated tail is left for later. A shrink, an inode
  change, a change in a compressed part, or a changed part list: full reload.
- Every operation of §21.7, under one state lock. Reads: barrier, `sync()`,
  answer. Writes: format records exactly as the direct writers do (spec:
  `_specFormatRecord`, marker keys raw; `.tsv`: 3.39 sanitising and padding,
  shared with `appendLinesTabularFile`), submit. `pop`, `popitem`, `setdefault`:
  barrier, `sync()`, decide, submit, all under the lock.
- `clear` / `scrub`: barrier, call the existing library functions (keeping their
  optimistic in-place rewrite and their refusals), full reload.
- Its own appended records come back through `sync()` like anyone else's, so the
  view is a replay of the file by construction.

**The writer thread.** A queue of `(sequence, payload, wantsDisk)`. It takes
everything queued, appends it with one locked write (`_specAppendPayload` /
the 3.39 append path), `fsync`s when any entry or the marker asks for disk,
publishes "written up to N" (and "durable up to N") on a condition variable, and
loops. It never takes the state lock. A write request waits for "written" (or
"durable") as §21.9 says; the barrier of a read waits for "written up to the last
sequence submitted".

**Connections.** `socketserver.ThreadingMixIn` with `UnixStreamServer`, or
`TCPServer` on `127.0.0.1` when `socket.AF_UNIX` is missing; a
`StreamRequestHandler` reads lines, parses, calls the engine, writes the
response. The socket is `s` inside `tempfile.mkdtemp(prefix='tsvz-')` under
`$XDG_RUNTIME_DIR` (else the temp directory): directory 0700, socket 0600; with
`--x-group G` the directory is 0710 and the socket 0660, group G; `--x-mode`
sets the socket mode explicitly.

**Pointer file.** Created with `O_CREAT`, locked non-blocking with the 4.1 lock
helpers, written in place, fsync'd, kept open; unlinked then closed on clean
stop. A TCP handler adds a random `token` and gives the pointer file the
socket's mode.

**Server lifecycle.** `tsvz serve STORE [--x-idle-timeout S] [--x-group G]
[--x-mode MODE] [--x-tcp] [--x-header H] [--x-defaults D]`: lock, load, bind,
write pointer, log the address on stderr, serve. `--x-header` and `--x-defaults`
apply only when `serve` creates the store; the served view never uses initial
defaults or 3.39 strict mode, so routed output equals direct output (§20.8).
`--x-tcp` uses the TCP transport on any platform. SIGINT/SIGTERM or `stop`: stop
accepting, drain the queue, `fsync`, remove socket, socket directory and pointer,
exit 0. Idle timeout: the same after S seconds with no connection. Tolerance
events are reported once per kind on stderr (as everywhere in 4.1) and as `#!`
lines to the client whose request met them.

**Client side.** `_serveFind(store) -> address | None` reads and checks the
pointer (one retry after a short pause if it does not parse yet);
`_ServeConnection(address, token)` connects (0.5 s connect timeout), authenticates
over TCP, and `request(fields, options, bulkLines=None) -> (lines, diagnostics,
status)`.

**CLI routing.** In `_cliMain`, before running a store operation (everything but
`serve` and `stop`): find the pointer; if live, forward. A command line is
forwarded only when it has none of `--x-direct`, `--x-header`, `--x-defaults`,
`--x-strict`, and, for a loose file, the server's delimiter (the pointer records
it as `x-delimiter`). Output
re-encodes protocol rows into the store's own delimiter for records, or prints
the table; diagnostics go to stderr (`-q` drops warnings); the status is the
exit code. A stale or unreachable pointer: one warning, then the direct path.

**`TSVZClient(fileName, sync=False, timeout=None, teeLogger=None)`.** A
MutableMapping with `TSVZed`'s semantics:

- `c[k]`: the row; for a missing key of a spec store the §14.5 defaults row while
  `#_return_defaults_when_missing_#` is true; otherwise `KeyError` (as
  `TSVZed.__missing__`).
- `get`, `in` (`has`), `len`, iteration (`keys`), `items()` / `values()` (one
  `read`), `pop`, `popitem`, `setdefault`, `update` (one bulk `set -`),
  `clear()`, `setDefaults()` (a `#_defaults_#` marker write), `close()`, `with`.
- `c[k] = v` normalises `v` as `TSVZed.__setitem__` does (a string is split on
  the store's delimiter; the key is put first when missing), then `set`s it.
  `del c[k]` writes a tombstone; a missing key is not an error, as in `TSVZed`.
- `timeout` is the time to wait for each answer (None: no limit); connecting
  gives up after 0.5 s.
- `sync=True` adds `--sync` to every write.
- `move_to_end`, `rewrite`, `mapToFile`, `hardMapToFile` raise
  `NotImplementedError` (order is first appearance, §3.4; rewriting is `scrub`).
- **No server:** a warning once, then the same engine in-process (a private
  `_ServeStore` with its own writer thread): identical semantics, no shared
  cache. **Connection lost:** re-read the pointer once and reconnect, else switch
  to the in-process engine with a warning. Writes a crashed server had
  acknowledged in `memory` mode but not yet written are lost (§18.4's stated
  trade-off).

**Dialects.** Spec stores follow §21. `.tsv`-family files are served under the
3.39 rules the library uses (§20.2.4: implementation-defined): the view is a
3.39 replay, writes use 3.39 formatting, and the protocol carries rows in the
§13 encoding regardless.

### Changes users can see

| Change |
|---|
| New `tsvz` operations: `has`, `len`, `keys`, `pop`, `popitem`, `setdefault`, `serve`, `stop`; new option `--x-direct`. |
| While a server runs, a `STORE.serve` file sits beside the store. |
| With a live server, `tsvz` commands are answered by it (same output and exit status). |
| New `TSVZ.TSVZClient`. `TSVZed`, `TSVZedLite` and the stateless helpers are unchanged. |

No 3.39 behaviour changes.

## 4. Testing

- **Engine (in-process `_ServeStore`):** after any mix of engine writes and
  direct appends (both dialects, single- and multi-part, compressed), the view
  equals `readTabularFile`; every reload trigger (shrink, inode change, new part,
  compressed part changed); a torn tail completed later; read-your-writes under
  `memory`; `disk` and `--sync` waiting for `fsync` (spy); threaded `pop` /
  `setdefault` races; unreadable part → status 1 with partial rows.
- **Protocol:** a real handler on a temporary socket driven by raw lines:
  framing, `#!` lines, each status, `#2` for malformed lines and unknown
  operations, the line limit, bulk input ended by `#`, TCP `auth`.
- **CLI:** `tsvz serve` as a background subprocess; the CLI operation tests run
  a second time against a live server and must give identical stdout and exit
  status (direct-vs-served differential); stale pointer fallback with a warning;
  `--x-direct`; `stop`; idle timeout; a second `serve` refused; SIGTERM removes
  the pointer and the socket; the new operations both direct and served.
- **`TSVZClient`:** the same operation sequence on `TSVZed` and on `TSVZClient`
  reads back the same rows; in-process fallback warns once; reconnect after a
  server restart.
- **Access:** socket and directory modes by default and with `--x-group`.
- **Pythons:** the suite on Python 3.6 and 3.7 (Docker) and 3.6 under `LANG=C`.
  The TCP transport is exercised by forcing it in a test on Linux; Windows itself
  is not exercised (stated in the README).

## 5. Out of scope

- Clients on other hosts, network authentication beyond the loopback token.
- One handler for several stores.
- Auto-starting handlers, `--daemon`.
- Handler-driven snapshot, rotation and promotion (§19.2, §19.9).
- HTTP.
- Moving `TSVZed` or `TSVZedLite` onto the handler.
