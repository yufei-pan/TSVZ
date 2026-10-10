# An append-only CSV that's also a key-value store.

TSVZ files are plain CSV or TSV where every write is one new line. Read one
with `cat`, open it in a spreadsheet, hand it to an agent. Replay the lines and
you have the current state.

[Read the spec](/spec) [Get an implementation](#implementations)

`pip install tsvz` — Python, the reference implementation

```csvz people.csvz
#id,name,score  ← header (a comment)
alice,Alice,30
bob,Bob,25
alice,Alice,31  ← update: last line wins
bob             ← delete: just the key
```

Current state: alice is Alice, 31. bob is deleted.

## Why TSVZ

- **Append-only, crash-safe.** Every change is a new line, and a line counts
  only once its newline is written. A crash can cut off the line being
  written; lines already committed are never rewritten.
- **Readable as-is.** Plain UTF-8 text, no binary parts, no quoting rules.
  What you see in the file is the data.
- **Works with your tools.** A `.csvz` file is plain CSV: open it in a
  spreadsheet, load it with pandas, grep it. You'll see the log, every write
  in order; `tsvz scrub` compacts it to one row per key.
- **Built for agents.** LLMs read the file as it is. `tsvz get` and `tsvz set`
  work from any shell tool call, and this site serves agents markdown.

## How it works

1. **Append.** Writers only add lines: a row sets a key, a key alone deletes
   it, and `#_markers_#` set defaults and options.
2. **Replay.** Readers go top to bottom. The last line for a key wins; a key
   keeps the position where it first appeared.
3. **Compact.** When you don't need the history, `tsvz scrub` rewrites the
   file with one row per key.

## Quick start

### Command line

```console
$ pip install tsvz
$ tsvz set people.csvz alice Alice 30
Created people.csvz
$ tsvz set people.csvz bob Bob 25
$ tsvz set people.csvz alice Alice 31
$ tsvz delete people.csvz bob
$ cat people.csvz
alice,Alice,30
bob,Bob,25
alice,Alice,31
bob
$ tsvz get people.csvz alice    # → alice, Alice, 31
```

### Python

```python
import TSVZ

t = TSVZ.TSVZed('people.csvz',
                header='id,name,score')
t['alice'] = ['alice', 'Alice', '30']
t['bob'] = ['bob', 'Bob', '25']
t['alice'] = ['alice', 'Alice', '31']
del t['bob']       # appends "bob": a key alone
print(t['alice'])  # ['alice', 'Alice', '31']
t.close()
```

## Implementations

| Language | Package | Read | Write | CLI §20 | Handler §21 | Version | Links |
|---|---|---|---|---|---|---|---|
| Python (reference) | `tsvz` on PyPI | ✓ | ✓ | ✓ | ✓ | 4.2+ | [GitHub](https://github.com/yufei-pan/TSVZ) · [PyPI](https://pypi.org/project/TSVZ/) · [Notes](https://github.com/yufei-pan/TSVZ#spec-deviations-and-interpretations) |

TSVZ 4.2 is the first Python release that follows spec v1. Earlier releases
(3.x) read `.tsvz` files under older rules.

Writing one in another language? The spec is one document with worked
examples and pseudocode. Test against the reference implementation, then
[open an issue](https://github.com/yufei-pan/TSVZ/issues) to be listed here.

## When TSVZ isn't the right fit

- **The file grows** with every write until you compact it.
- **Reads replay the log.** There are no indexes and no in-place updates.
- **Key-value, not relational.** No queries or joins.
- **One machine.** `tsvz serve` listens on a Unix socket only.
- **Spreadsheets show the log,** not the current table.
