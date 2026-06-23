# TSVZ — Format Specification

**Version:** 1
**Status:** Draft
**Family:** TSVZ / CSVZ / NSVZ / PSVZ

A line-oriented, append-only, key–value write-ahead-log format in plain UTF-8 text.

---

## 1. Scope and overview

TSVZ is a text file format for durably recording a key–value store as an
**append-only log**. Current state is reconstructed by replaying the file forward
with **last-wins** semantics. The format is designed for near-zero write overhead,
trivial crash recovery, and direct inspection with ordinary text tools, in
exchange for giving up random-update efficiency and bounded file size.

The format defines four delimiter variants (§5) that are identical except for the
field delimiter. The strict variants (extension ending in `z`) carry the full
semantics of this document; the loose variants (plain extension) are a tabular
fallback with no semantic guarantees.

This document specifies the file encoding, the reading procedure, the deletion and
defaulting model, the reserved marker namespace, field escaping, optional
integrity checking, compression, multi-part stores, and the snapshot (compaction)
procedure.

---

## 2. Conformance and terminology

### 2.1 Requirement levels

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are
to be interpreted as described in RFC 2119 and RFC 8174.

A **conformant reader** is software that reconstructs store state from a TSVZ file
in accordance with §6–§15. A **conformant writer** is software that produces TSVZ
files in accordance with this document. Some requirements apply only to one role
and are marked accordingly.

### 2.2 Definitions

- **Store** — the logical key–value mapping represented by a file or a multi-part
  set of files (§17).
- **Part** — one physical file. A single-file store is the one-part case.
- **Record / row / line** — one logical line of the file, terminated by `\n`.
- **Field** — a maximal run of bytes within a row not containing a raw delimiter,
  obtained by splitting the row on the raw delimiter.
- **Key** — the value of field index 0 (the first field) after processing.
- **Value column** _j_ (_j_ ≥ 1) — field index _j_ of a row; the value of a key is
  its sequence of value columns.
- **Marker** — a reserved line that sets reader state or carries metadata (§12).
- **Tombstone** — a record that deletes a key (§9).
- **Live key** — a key whose most recent record is not a tombstone.
- **Missing key** — a key that has no record, or whose most recent record is a
  tombstone.
- **Effective version** — the spec version a reader applies to a given line (§6.2).

---

## 3. Data model

3.1 Every mutation is an **append**. During normal operation a part is only ever
appended to; no byte previously written is modified. The sole exception is the
snapshot procedure (§19).

3.2 The store is **key–value**, not relational. Field 0 is the key; value columns
0..∞ follow. There is no schema and no header semantics beyond the comment rule
(§11).

3.3 State is reconstructed by **replaying the file forward**. For each key, the
**last** record in file order determines its current value (**last-wins**).

3.4 A key's **logical position** is the position of its first appearance. Later
records for that key change its value, not its order. First-appearance order is
the iteration order a reader SHOULD expose and the order a snapshot MUST emit
(§19).

3.5 Deletion is represented by a **tombstone** (§9), never by physical removal of
prior bytes.

3.6 Conceptually, every row is its written value columns followed by an infinite
tail of empty value columns; reading a value column beyond a row's written width
yields the empty string, subject to defaulting (§14). A reconstructed row is **not
guaranteed** to be of any fixed width; a reader MAY validate, pad, or trim.

---

## 4. File encoding and framing

4.1 A file is encoded in **UTF-8** with no byte-order mark.

4.2 Records are separated by a line feed, `U+000A` (`\n`). A reader **MUST**
tolerate a single carriage return (`U+000D`, `\r`) immediately preceding the
terminating `\n` (i.e. CRLF line endings). A writer **SHOULD** emit bare `\n`.
CRLF tolerance strips at most one `\r` immediately before the record terminator; a
`\r` elsewhere in a row is ordinary field data.

4.3 **The trailing `\n` is the commit marker.** A record is *committed* if and
only if it is followed by `\n`. A reader **MUST** discard any bytes after the last
`\n` in a part. A reader **MUST NOT** treat unterminated trailing bytes as a
record.

4.4 A reader **MUST** tolerate the absence of a final newline: it does not error,
but per §4.3 the unterminated trailing bytes are discarded. A writer **SHOULD**
terminate every record, including the last, with `\n`.

4.5 **Crash and torn-tail recovery is unconditional**: everything after the last
`\n` is discarded, whether or not it would parse. This is what makes recovery from
a partial write at end-of-file safe. (In this format a torn write usually yields a
parseable but short record rather than a parse error, so any recovery rule
conditioned on "fails to parse" would admit corruption.)

4.6 An empty file (zero bytes), and a file containing only comments and markers,
are valid and represent an empty store.

> Recovery at §4.3–§4.5 handles truncation at end-of-file. Avoiding torn records
> in the *middle* of a part is the writer's responsibility; see §18.

---

## 5. Variants and delimiters

5.1 The variants differ only in the field delimiter. A file's variant is
determined by its extension; the delimiter MUST match the variant.

| Variant | Strict ext. | Loose ext. | Delimiter |
|---------|-------------|------------|-----------|
| TSVZ    | `.tsvz`     | `.tsv`     | TAB `\t` (`U+0009`) |
| CSVZ    | `.csvz`     | `.csv`     | comma `,` (`U+002C`) |
| NSVZ    | `.nsvz`     | `.nsv`     | NUL `\0` (`U+0000`) |
| PSVZ    | `.psvz`     | `.psv`     | pipe `\|` (`U+007C`) |

5.2 The strict extension (ending in `z`) denotes a file carrying the full
semantics of this document. The loose extension denotes a tabular fallback with no
semantic guarantees (it may, for example, be read by generic delimiter-separated
tooling that does not honor markers, escaping, or tombstones).

5.3 The variants do **not** use RFC 4180-style quoting. A raw delimiter always
terminates a field and a raw `\n` always terminates a record; delimiters and
newlines that are part of field data are represented by escape tokens (§13). This
keeps record and field boundaries trivially locatable by simple tools.

5.4 The NUL delimiter (NSVZ) is intended for stores whose values may contain any
other delimiter character — for example arbitrary filesystem paths or opaque user
data — since NUL cannot occur in such values. NUL-delimited parts remain
greppable and open in pager tools; they may not render cleanly in a terminal.

---

## 6. The frozen meta-grammar

6.1 The following are **version-invariant** and MUST be parseable before any
`#_version_#` line is interpreted:

- record splitting on `\n` (with `\r` tolerance, §4.2) and the commit rule (§4.3);
- field splitting on the variant's delimiter (§5);
- the escape-token *framing* and the literal-`<` rule (§13);
- the reserved-key pattern and the comment rule (§11, §12.2);
- the empty-key rule (§8.4).

Versioned rules (defaulting, whitespace handling, marker semantics, the set of
recognized escape-token names, etc.) **MUST NOT** alter this meta-grammar. This
guarantees that a reader can always locate marker lines and record/field
boundaries regardless of the active version.

6.2 **Versioning is strictly additive.** Each version is a superset of all earlier
versions: it MAY add markers, escape-token names, or semantics, but MUST NOT
remove or redefine existing constructs.

6.3 The **effective version** a reader applies to a line is
`min(declared_version, highest_version_the_reader_implements)`:

- If the declared version is ≤ the reader's maximum, the reader applies the
  declared version exactly.
- If the declared version is greater than the reader's maximum, the reader applies
  its own highest implemented version. (A version-2 reader encountering a
  version-3 line uses version 2, not version 1.)

6.4 Constructs newer than the effective version **degrade gracefully**:
unrecognized markers are ignored (§12.3); unrecognized escape tokens are passed
through literally (§13.4). Absence of any `#_version_#` line means version 1.
`#_version_#` is itself stateful (§12.4): the effective version is recomputed at
each `#_version_#` line. A file MAY switch versions (e.g. `1 → 2 → 1`); each row is
interpreted under the version in force at its position. Switching never affects the
meta-grammar of §6.1.

---

## 7. The reading pipeline

A conformant reader processes a part as a forward stream of records. For each
record, in order:

7.1 **Frame.** Read the record's raw bytes up to, but not including, the
terminating `\n`, removing a single trailing `\r` if present (§4.2). Bytes after
the final `\n` of the part are not a record (§4.3).

7.2 **Split.** Split the raw record into fields on the raw delimiter. The field
count equals the number of raw delimiters plus one.

7.3 **Classify.** Let `F0` be the raw, unmodified first field.

   - If the first character of `F0` is `#` and `F0` matches the reserved-marker
     pattern (§12.2) under a case-insensitive ASCII comparison:
     - if it names an **integrity** marker (`#_checksum_<algo>_#`, §15) recognized
       under the effective version, the line is an integrity-marker line for that
       algorithm;
     - else if it names another marker the reader recognizes as active under the
       effective version, the line is an **official-marker** line;
     - else the line is an unrecognized/custom marker.
   - Otherwise, if the first character of `F0` is `#`, the line is a **comment**.
   - Otherwise the line is a **data row**.

7.4 **Update integrity accumulators.** Feed this record's full raw bytes into every
currently armed integrity accumulator (§15), **except** the accumulator whose
algorithm matches this line when this line is an integrity-marker line — a digest
cannot cover its own marker line (§15.6). A line that is not an integrity-marker
line is fed into every armed accumulator (including, for any algorithm, the marker
lines of *other* algorithms). Then, if this line is an integrity-marker line,
perform its arm-or-verify action (§15.3) using the accumulator state as just
updated.

7.5 **Act on non-data lines.** A comment, an unrecognized/custom marker, and an
integrity-marker line (already handled in §7.4) have no further effect; an
official-marker line is applied (§12). In all of these cases, proceed to the next
record.

7.6 **Tombstone detection (raw, pre-stripping).** For a data row, determine whether
it is a **tombstone**: it is one if and only if it contains no raw delimiter (it
split into exactly one field). This determination is made on the raw row, before
stripping or defaulting (§9.1); the deletion it implies is applied in §7.8, after
the key is resolved.

7.7 **Resolve the key.** Apply trailing-whitespace stripping to `F0` if enabled
(§10), then decode escape tokens (§13). If the resulting key is the empty string,
the row is ignored (§8.4); proceed to the next record. (The empty-key check
precedes the tombstone deletion, so an empty-key row — even one with no delimiter —
is ignored rather than treated as a deletion.)

7.8 **Tombstone deletion.** If the row was found to be a tombstone in §7.6, delete
the resolved key (§9) and proceed to the next record.

7.9 **Resolve value columns.** For each written value column _j_ ≥ 1: apply
trailing-whitespace stripping if enabled, then decode escape tokens, then apply
the empty-cell rule of §14.2. Absent value columns are resolved at read time per
§14.

7.10 **Store.** Record `key → value` with last-wins semantics, preserving the key's
first-appearance position (§3.4).

A non-normative pseudocode rendering of this pipeline appears in Appendix B.

---

## 8. Keys, values, and columns

8.1 Field 0 is the **key**. After reconstruction, each key is unique within the
store; physical duplicates in the file are the update mechanism and are resolved by
last-wins (§3.3).

8.2 The **value** of a key is its sequence of value columns (fields 1..),
post-processing (§7.9, §14).

8.3 The raw first field of a **data row** MUST NOT begin with `#`; such lines are
comments or markers (§11, §12). To store a key whose value begins with `#`, encode
the leading `#` as the escape token `<#>` (§13); the row's raw first field then
begins with `<` and is correctly classified as data.

8.4 **Empty-key rows are ignored.** If, after the processing of §7.7, the key is
the empty string, the entire row is discarded with no effect on state — exactly
like a comment.

---

## 9. Deletion (tombstones)

9.1 A **tombstone** removes a key from the store. Whether a row is a tombstone is
determined on the **raw** row, **before** any whitespace stripping or defaulting
(§7.6). The key it removes, however, is the **resolved** key — field 0 after
trailing-whitespace stripping and escape decoding (§7.7) — so that a tombstone
matches the same key identity under which the key was stored.

9.2 A data row is a tombstone **if and only if** it contains no raw field
delimiter — that is, it consists of the key alone, with **zero** value fields.

   - `mykey\n` deletes `mykey`.
   - `mykey\t\n`, `mykey\t\t\n`, and so on are **not** tombstones; each sets
     `mykey` to a value whose written columns are present and empty (≥ one value
     field exists). See §14.

9.3 A tombstone deletes its resolved key if present and is a no-op if absent. An
empty-key row is ignored rather than treated as a deletion (§7.7, §8.4).

9.4 Active defaults never resurrect a tombstoned key, because tombstone detection
precedes defaulting (§9.1). Whether a now-missing key *reads back* as defaults is
governed by `#_return_defaults_when_missing_#` (§12, §14.3). When that marker is
`true` (its built-in default), a deletion is observationally equivalent to a reset
to the active defaults.

9.5 **Single-column edge case.** In a store whose rows never contain a delimiter,
every row is of the form `key\n` and is therefore a tombstone; such a store can
represent only deletions. To represent presence-with-empty-value, a row MUST
include at least one delimiter (`key\t\n`).

---

## 10. Whitespace

10.1 When trailing-whitespace stripping is enabled, a reader **strips trailing
whitespace from every field** (including the key) during processing (§7.7, §7.9).
**Leading whitespace is significant and preserved.**

10.2 Stripping removes a trailing run of the ASCII characters space (`U+0020`) and
horizontal tab (`U+0009`). (In TSVZ, tab is the delimiter and so cannot occur
inside a field; in the other variants a literal trailing tab within a field is
subject to stripping.) Stripping is applied to a field's raw bytes before escape
decoding (§13); a trailing whitespace character that must be preserved is retained
only when stripping is disabled.

10.3 Stripping is controlled by `#_strip_trailing_whites_#` (§12). Its built-in
default is `true`.

10.4 Comment and marker lines are **not** subject to field stripping. Marker
*values* are decoded as ordinary fields (§13), but marker *keys* are matched
literally on their raw bytes (§12.2).

---

## 11. Comments and header

11.1 A line is a **comment** if and only if the first character of its raw first
field is `#` **and** the line is not a recognized active marker (§12) and not a
recognized integrity marker (§15). Comment classification is performed on the raw
bytes, before any stripping; consequently a leading space defeats comment
classification, and trailing whitespace is irrelevant to it.

   - `" #foo"` (leading space) is a **data key**, not a comment.
   - `"#foo "` is a **comment**, regardless of `#_strip_trailing_whites_#`.

11.2 A comment has no effect on reconstructed state (its raw bytes still count
toward integrity, §15).

11.3 There is **no formal header**. A header row, if present, is merely a comment
and carries no data semantics.

---

## 12. Markers

12.1 Markers are reserved lines that either set reader state or carry metadata.
Markers are **stateful and forward-only** unless stated otherwise:

   - A marker takes effect from its line onward and affects only **subsequent**
     records. A reader does not backtrack.
   - A later marker line of the same key **completely replaces** the prior state
     for that marker (no merge).
   - For a **stateful** marker, an empty or lone-key (value-less) marker line
     **resets** that marker to its built-in default for the effective version.
   - Marker state carries across part boundaries (§17): a multi-part store is
     replayed as one concatenation.

### 12.2 Reserved-key pattern and namespace

12.2.1 A marker key matches the **reserved pattern**, anchored to the whole field
and compared case-insensitively over ASCII:

```
#_[A-Za-z0-9_-]+_#
```

This pattern is part of the frozen meta-grammar (§6.1). Lines whose first field
does not match it, but begins with `#`, are ordinary comments (§11).

12.2.2 **Namespace convention.** Official markers defined by this specification use
the single-underscore form `#_name_#` (one underscore adjacent to each `#`).
**This specification will never define a marker of the form `#__name__#`** (two
underscores adjacent to each `#`). Implementations and processors that define their
own markers **SHOULD** use the double-underscore form `#__name__#` to avoid
collision with current and future official markers.

12.2.3 The pattern of §12.2.1 matches both forms; the distinction in §12.2.2 is a
naming convention enforced by reserving the double-underscore space for extensions.

### 12.3 Unrecognized markers

12.3.1 A line that matches the reserved pattern but is not a marker the reader
recognizes as active under the effective version is **ignored**: it has no effect
on reconstructed state and is not surfaced to the application. A processor MAY
recognize and act on its own custom (`#__name__#`) markers; to all other readers
those markers are inert.

### 12.4 Defined markers (version 1)

| Marker | Class | Purpose | Value form | Built-in default |
|--------|-------|---------|-----------|------------------|
| `#_version_#` | stateful | active spec version | a single integer ≥ 1 | `1` |
| `#_defaults_#` | stateful | per-value-column fallback (§14) | unbounded fields | all empty |
| `#_strip_trailing_whites_#` | stateful | trailing-whitespace stripping (§10) | `true` / `false` | `true` |
| `#_fill_empty_with_default_#` | stateful | present-empty cells take the default (§14) | `true` / `false` | `false` |
| `#_return_defaults_when_missing_#` | stateful | a missing-key read returns the defaults (§14) | `true` / `false` | `true` |
| `#_rotate_#` | advisory | recommended action for superseded parts after a snapshot (§19) | `keep` / `rename` / `delete` | `keep` |
| `#_write_ack_#` | advisory | when a writer/handler acknowledges a record (§18) | `memory` / `disk` | `memory` |
| `#_checksum_<algo>_#` | integrity | verify a segment digest (§15) | hex digest, or empty | off |

12.4.1 Boolean values (`true`/`false`), enumerated values, and `<algo>` names are
matched case-insensitively over ASCII. An empty value resets a stateful marker to
its built-in default; for `#_checksum_<algo>_#`, an empty value begins a new
segment without verification (§15.3).

12.4.2 **`#_version_#`** — see §6. **`#_defaults_#`, `#_fill_empty_with_default_#`,
`#_return_defaults_when_missing_#`** — see §14. **`#_strip_trailing_whites_#`** —
see §10. **`#_rotate_#`** — see §19. **`#_write_ack_#`** — see §18.
**`#_checksum_<algo>_#`** — see §15.

12.4.3 **Advisory** markers (`#_rotate_#`, `#_write_ack_#`) are hints to writers
and processors and have **no effect on reconstructed data**. **Integrity** markers
(`#_checksum_<algo>_#`) affect only verification and have no effect on
reconstructed data (§15).

12.4.4 The forward-only binding of stateful markers (§12.1) applies to
`#_defaults_#`, `#_strip_trailing_whites_#`, and `#_fill_empty_with_default_#` as
well: each data row is resolved under the state in force at that row's position. A
later change to any of these markers does not retroactively alter rows already
processed under the earlier state. See §14.4 and §19.4 for the consequences for
defaulting and snapshots.

---

## 13. Field escaping

13.1 The variant delimiter, the line feed `\n`, and the escape introducer `<`
cannot appear literally inside a field; they are represented by reversible
**control tokens** of the form `<...>`. The number sign `#` is represented by a
token only where required by §8.3. Reading inverts writing.

### 13.2 Token table (version 1)

| In a field (in memory) | Stored on disk | Read back as |
|------------------------|----------------|--------------|
| the delimiter character | `<sep>` | the delimiter character |
| line feed `U+000A`      | `<LF>`  | line feed |
| less-than `<` `U+003C`  | `<lt>`  | `<` |
| number sign `#` `U+0023`| `<#>`   | `#` |

13.3 **Writer rules.** A conformant writer **MUST** encode every literal `<` as
`<lt>`, every delimiter character as `<sep>`, and every line feed as `<LF>`. A
writer **MUST** encode a `#` as `<#>` when it is the first character of a data key
(§8.3); elsewhere `#` MAY be written literally. A writer **MUST NOT** emit a bare
`<`.

13.4 **Reader rules.** Because every literal `<` is encoded, a raw `<` in a
conformant file always begins a control token; decoding is therefore unambiguous
and total over all UTF-8 (and arbitrary byte) field content. A reader maps the
tokens of §13.2; a `<...>` token it does not recognize (for example, one
introduced by a higher version) is **passed through literally**. **Field splitting
(§7.2) occurs before token decoding**, so an encoded `<sep>` or `<LF>` never causes
a split.

13.5 **Reversibility examples** (no lossy cases):

   - delimiter inside a field: `a<sep>b` ⟶ reads as `a`‹delim›`b`
   - the literal five-character string `<sep>`: written `<lt>sep>` ⟶ reads as `<sep>`
   - the literal four-character string `<LF>`: written `<lt>LF>` ⟶ reads as `<LF>`
   - a data key whose value is `#foo`: written `<#>foo` (raw first field begins
     `<`, classified as data) ⟶ reads as `#foo`
   - `a<b`: written `a<lt>b` ⟶ reads as `a<b`

13.6 The token *framing* (`<...>`) and the literal-`<` rule (§13.3–§13.4) are part
of the frozen meta-grammar (§6.1). The *set of recognized token names* MAY grow in
later versions; unrecognized tokens degrade per §13.4.

> Marker keys are matched on raw bytes (§12.2); only field *values*, including
> marker values, are token-decoded.

---

## 14. Defaults and empty-cell resolution

14.1 **`#_defaults_#`** supplies per-value-column fallback values. On a defaults
line, field index _j_ (_j_ ≥ 1) is the default for value column _j_; field 0 is
the marker key and has no defaulting role (the key itself is never defaulted). A
column with no corresponding default field defaults to the empty string.

14.2 **Resolution of a present value column.** For a value column _j_ that is
written in a row:

   - if non-empty (after stripping and decoding) — its value;
   - if empty and `#_fill_empty_with_default_#` is `true` — the default for column
     _j_ active at this row's position;
   - if empty and `#_fill_empty_with_default_#` is `false` (the built-in default) —
     the empty string.

14.3 **Resolution of an absent value column** (index ≥ the row's written width):
the default for that column active at this row's position.

14.4 **Forward-only binding.** Per §12.4.4, the defaults and
`#_fill_empty_with_default_#` state used to resolve a row are those active at the
**row's position**, not a later or "current" state. Authors **SHOULD** declare
`#_defaults_#` (and the related toggles) once, near the top of a part or via the
snapshot preamble (§19), and **SHOULD NOT** change them in a way that leaves live
sparse rows bound to superseded defaults. Changing them mid-stream is permitted but
imposes obligations on snapshots (§19.4).

14.5 **Missing keys.** For a key with no live record (§2.2):

   - if `#_return_defaults_when_missing_#` is `true` (the built-in default), a read
     returns the defaults active at the end of replay (the current active
     defaults), as a full value row;
   - if `false`, a read signals not-found (the precise signal — error, null,
     sentinel — is implementation-defined).

   A missing key has no position, so its defaulting uses the current active
   defaults rather than a row-bound state.

14.6 **Observable read states.** The model yields three distinguishable outcomes:

   - **missing** → the active defaults (if §14.5 enabled) or not-found;
   - **present with explicit empty cells** → empty cells (unless
     `#_fill_empty_with_default_#` is `true`, in which case they take their
     defaults);
   - **present with values** → those values, with absent and (optionally)
     present-empty columns drawn from defaults.

14.7 **Note on column ordering.** Because a row is a delimiter-joined sequence (in
every variant, §5), only *trailing* value columns can be absent; to set value
column _j_, all columns 1..(_j_−1) must be written, and any of them you wish to
leave unset are written as present-empty. With `#_fill_empty_with_default_#` set to
`true`, such present-empty cells take their defaults — the intended behavior for
configuration-style files where a blank cell means "use the default."

---

## 15. Integrity (optional)

15.1 Integrity checking is **opt-in and detection-only**. In the absence of any
`#_checksum_<algo>_#` marker, a reader performs no integrity checking. The action
taken on a detected mismatch (warn, refuse the affected segment, halt) is
implementation-defined.

15.2 **Marker.** `#_checksum_<algo>_#`, where `<algo>` names a digest algorithm
(for example `crc32`, `crc32c`, `sha256`, `blake3`). `<algo>` is part of the
case-insensitive reserved key and matches the inner character class of §12.2.1. The
value is the lowercase hexadecimal expected digest, or empty.

15.3 **Semantics.** A reader does not begin computing a digest automatically; an
algorithm's accumulator is armed only by an explicit marker. For each
`#_checksum_<algo>_#` line, a reader that implements `<algo>` proceeds as follows:

   - **First marker for `<algo>`** (no prior `#_checksum_<algo>_#` has been seen
     across the parts replayed so far): arm the accumulator and begin a new segment
     from after this line. **No verification is performed, and any value the marker
     carries is ignored** — there is no prior content to verify, so this case
     behaves exactly like a marker with an empty value.
   - **Subsequent marker with a non-empty value:** verify that the digest of the
     covered segment (§15.4) equals the value; a mismatch is a detected corruption.
     Then reset the accumulator and begin a new segment from after this line.
   - **Subsequent marker with an empty value:** reset the accumulator and begin a
     new segment from after this line, with no verification (the preceding segment
     is left unverified).
   - A reader that does **not** implement `<algo>` treats the line as inert
     (equivalent to an unrecognized marker, §12.3): no arming, no verification, no
     effect on reconstructed state.

15.4 **Covered segment.** The covered segment verified by a `#_checksum_<algo>_#`
marker is the concatenation of the raw on-disk bytes of every line strictly after
the previous `#_checksum_<algo>_#` marker and strictly before this one. **There is
no implicit "start of part" boundary**: content before the first
`#_checksum_<algo>_#` marker is not covered (§15.3). Each included line contributes
its full raw bytes, including field delimiters and its terminating `\n` (and a
preceding `\r` if present). A `#_checksum_<algo>_#` marker line is **never** part of
its own algorithm's segment — its bytes are not fed into the `<algo>` accumulator
(§7.4); checksum marker lines of *other* algorithms **are** included as ordinary
content. Because parts are replayed as a single concatenation (§17.4), a segment
**may span part boundaries**: a marker near the end of one part and the next marker
of the same algorithm in a later part verify the content between them across the
intervening part boundary.

15.5 **Multiple algorithms** MAY be interleaved; each maintains an independent
accumulator and independent segmentation, and segments MAY overlap. Content
following the last marker of an algorithm is unverified, which is expected for an
append-only tail.

15.6 **Coverage of digest lines.** A digest cannot cover its own marker line: the
stored value would depend on itself. Consequently a checksum marker line of
algorithm A is unprotected by A. To protect digest lines, use **two algorithms with
staggered injection points**: because each algorithm's segments include the other
algorithm's marker lines (§15.4), the two algorithms cover each other's digest
lines. Choosing algorithms with different digest lengths, or injecting them at
different intervals, ensures the markers interleave so that each falls within the
other's segment.

15.7 **Scope.** Integrity is strictly marker-to-marker. It is **not** a file-level
or per-part checksum — file- and storage-level integrity are left to the underlying
storage layer. Because parts are treated as a single data-wise concatenation
(§17.4), integrity segments are not scoped to individual parts and may cross part
boundaries (§15.4). This differs from compression, which is applied per part (§16).
The concatenation view applies during normal replay; during a concurrent snapshot
(§19), the immutable prefix being snapshotted and the separate active part are
processed as independent streams.

---

## 16. Compression

16.1 Compression is an **orthogonal stream filter**, not part of the format
semantics. A part (or, in a multi-part store, any individual part) MAY be wrapped
by a supported codec.

16.2 The compression suffix is the **outermost** filename suffix, so that tools
which determine the codec from the filename (rather than from magic bytes) work
correctly. Supported suffixes: `.gz`/`.gzip`, `.bz2`/`.bzip2`, `.xz`/`.lzma`,
`.zst`/`.zstd`. Example: `events.tsvz.<ordinal>.zst`.

16.3 Delimiter/format inference and the parsing of the ordinal and the `.rotated`
component (§17) strip the compression suffix first.

16.4 A long-running writer **MAY** hold a streaming codec open over the active part
and append to it incrementally; in that case the append-only model is preserved at
the compressed-stream level, and over a long run compression can be effective. By
contrast, a writer that is **not** continuously running **SHOULD NOT** open and
close a compressed active part for each individual write: per-open/close overhead is
high, and compression ratio suffers when each write becomes its own small compressed
unit. Such a writer SHOULD keep the active part uncompressed and leave compression of
prior immutable parts to a later pass (§17.7). Where a compressed part is not held
open as a stream, appending requires rewriting it as a whole.

---

## 17. Multi-part files

17.1 A store **MAY** be split across multiple parts sharing the same path up to the
format extension, distinguished by an ordinal suffix. Multi-part is purely an
addressing convention; a single-file store is the one-part case.

17.2 **Filename grammar.**

```
<store-path>.<format-ext>.<ordinal>[.rotated][.<compression>]
```

   - `<format-ext>` is one of the strict or loose extensions of §5.
   - `<ordinal>` is a hexadecimal integer (§17.3).
   - `.rotated` (optional) marks a part as excluded from normal loading (§19.5). It
     is an infix, placed before any compression suffix.
   - `.<compression>` (optional) is the outermost suffix (§16.2).

   A formal grammar appears in Appendix A.

17.3 **Ordering.** Parts are ordered by interpreting `<ordinal>` as a
**hexadecimal integer, ascending** — smaller is earlier. A reader **MUST** parse
the suffix as an integer and **MUST NOT** order parts by lexicographic comparison
of filenames (for example `.f` is 15 and precedes `.10`, which is 16, though it
follows it lexically). Leading zeros are insignificant.

17.4 **Logical store.** The logical store is the concatenation of parts in ordinal
order. Replay-forward, last-wins, and marker state (§12) all span part boundaries.

17.5 Each part is itself a **complete, valid TSVZ file**. Parts **MUST NOT** be
read independently unless the producer specifically designed each part as a
standalone snapshot (§19).

17.6 **Recommended ordinal: the hexadecimal of a UUIDv7**, emitted full width (32
hex characters). UUIDv7 is time-ordered, so parts sort chronologically, and
includes random bits, so it is collision-resistant across processes that begin in
the same millisecond. A hexadecimal Unix timestamp remains acceptable where
simultaneous part creation is impossible. A store **SHOULD** use a single ordinal
scheme throughout.

17.7 **Writer recommendations.**

   - On startup, a writer **SHOULD** open a fresh part named with a new ordinal and
     leave existing parts untouched (treating them as immutable).
   - A writer that is not continuously running **SHOULD** keep the active part
     uncompressed; a long-running writer **MAY** stream-append to a compressed
     active part (§16.4).
   - As an optional add-on, a producer **MAY** provide a processor that, on a later
     run, compresses prior immutable parts; different parts MAY use different
     codecs.

---

## 18. Concurrency and durability (recommendations)

The following are recommendations, not normative format rules; they are what make
the end-of-file recovery guarantee of §4 sound.

18.1 **Single writer per part.** A part SHOULD have exactly one appending writer.
Concurrency SHOULD be expressed as multiple parts (§17), not as concurrent appends
to one part. Concurrent `O_APPEND` writers can, on a crash, leave a torn record in
the *middle* of a part — offsets are assigned in order, so an earlier writer's
record may be torn while a later one is complete — which the "discard after the
last `\n`" recovery does not catch.

18.2 **Whole-record appends.** Every append `write()` SHOULD contain only complete
records and end with `\n`; a record MUST NOT be split across `write()` calls. A
record larger than the platform's atomic-write bound may still tear at end-of-file
on a crash; the trailing-bytes discard of §4.3 handles that case.

18.3 **Multiprocess: a dedicated write handler.** In a multi-process environment,
all writes SHOULD be routed through a single dedicated handler process that owns
the append. Producer threads or processes submit records to the handler over
in-memory IPC; the handler **batches** them and commits each batch as a single
append (and a single fsync). This preserves single-writer-per-part while allowing
many concurrent in-memory producers.

18.4 **`#_write_ack_#`** controls when the handler acknowledges a submitted record
back to its producer:

   - `memory` (built-in default) — acknowledge once the record is in the in-memory
     batch buffer. Lowest latency; not durable against power loss before fsync.
   - `disk` — acknowledge only after the batch is fsync'd to durable storage.
     Durable; higher latency.

18.5 **Independent parts** (separate writers without a shared handler) SHOULD be
used only when either (a) keys are partitioned so that no key is ever written from
two parts (no cross-part overwrite of a key), or (b) the application tolerates the
last-wins-across-parts **regression** that independent appends can produce, since
cross-writer append order may not reflect real time. Otherwise, writes SHOULD be
funneled through one handler (§18.3).

---

## 19. Snapshot / compaction

19.1 **Definition.** A *snapshot* (equivalently, compaction or scrub) emits a
single new part that **materializes the reader's final state** after replaying a
contiguous prefix of the store. It is the one sanctioned departure from
append-only (§3.1).

19.2 **Recommended procedure** (race-free and non-blocking):

   1. Identify an **immutable prefix** of parts — a contiguous set, lowest ordinal
      through some ordinal _N_, to none of which any writer is appending. The live
      writer is on an **active part** whose ordinal is strictly greater than every
      part in the prefix, and continues appending to it throughout. The writer is
      never paused and is never notified of the snapshot.
   2. Spawn a worker that performs **no writes** (for example, a `fork()`ed child).
      It replays the immutable prefix to obtain the final live key set
      (first-appearance order) and the final marker state.
   3. Write the snapshot as a **new part** whose ordinal _S_ lies strictly between
      the prefix maximum and the active part: _N_ < _S_ < (active ordinal) —
      newer than everything it subsumes, older than the live part. Write it
      durably (write + fsync), containing, in order:
      a. all currently active markers, dumped at the top: `#_version_#` first, then
         any other non-default stateful markers, then `#_defaults_#` last (subject
         to §19.4);
      b. one line per live key, in first-appearance order;
      c. no tombstones, no comments, no superseded values.
   4. Once _S_ is durable, apply the `#_rotate_#` action (§19.5) to the subsumed
      prefix.
   5. The worker exits. No coordination with the live process is required: the live
      process's in-memory state was already correct, and its part remains newest,
      so **no regression is possible** — the snapshot only materialized older,
      immutable data into an older slot.

19.3 **Ordinal room.** Slotting _S_ into the open interval (_N_, active) requires
sparse ordinals; UUIDv7 and timestamp ordinals leave numeric gaps and so are
recommended (§17.6). If the interval cannot be guaranteed, a producer MAY instead
quiesce writes for the duration of the snapshot.

19.4 **Fidelity (normative).** A snapshot **MUST** reproduce the exact observable
read result of the store it replaces. When forward-only marker state
(`#_defaults_#`, `#_strip_trailing_whites_#`, `#_fill_empty_with_default_#`) varied
over the life of the subsumed prefix such that re-emitting only the final state
would change any live key's resolved value, the snapshot **MUST** compensate — by
baking resolved values into the affected rows and/or by choosing marker state that
preserves them. In the common case where this state is set once and never changed,
the snapshot re-emits that state and keeps rows sparse. A simple always-correct
strategy is to emit `#_strip_trailing_whites_#` `false` and
`#_fill_empty_with_default_#` `false` and write fully resolved value cells, leaving
`#_defaults_#` only as a fallback for absent trailing columns.

19.5 **`#_rotate_#` action** for the now-superseded prefix:

   - `keep` (built-in default) — leave prior parts as they are.
   - `rename` — append the `.rotated` component to each prior part filename (placed
     before any compression suffix). A `.rotated` part is **excluded from normal
     loading**; a reader MUST ignore parts carrying a `.rotated` component when
     assembling the store.
   - `delete` — remove the prior parts.

   The action is advisory: a conservative reader or processor MAY always downgrade
   `delete` → `rename` → `keep` for safety.

19.6 **Lossiness.** A snapshot is lossy with respect to history: comments,
intermediate values, and tombstone records are discarded. The live data state and
the active marker state are preserved exactly (subject to §19.4).

19.7 **Lazy / offset-based readers.** Because the prefix is immutable, the live
process and the snapshot worker MAY read it concurrently with no coordination.
Readers that retain only a part identifier and byte offset per key (rather than
materialized values) work unchanged: both can serve reads from the immutable parts
while the snapshot is produced.

---

## Appendix A. Filename grammar (ABNF)

```abnf
part-file     = store-path "." format-ext "." ordinal [ ".rotated" ] [ "." codec ]
format-ext    = "tsvz" / "csvz" / "nsvz" / "psvz"   ; strict
              / "tsv"  / "csv"  / "nsv"  / "psv"     ; loose
ordinal       = 1*HEXDIG                              ; parsed as a hex integer
codec         = "gz" / "gzip" / "bz2" / "bzip2" / "xz" / "lzma" / "zst" / "zstd"
store-path    = <any valid path up to the format extension>
```

Parts are ordered by the integer value of `ordinal`, ascending. Parts bearing the
`.rotated` component are excluded from loading.

---

## Appendix B. Reading procedure (non-normative pseudocode)

```
state.version            = 1
state.defaults           = []        # value-column fallbacks, index 1..
state.strip              = true
state.fill_empty         = false
state.return_on_missing  = true
store                    = ordered map: key -> resolved value (first-appearance order)
digests                  = {}        # armed per-algorithm accumulators (optional)

# Parts are read in ordinal order as ONE concatenation; marker state and digest
# accumulators persist across part boundaries (§17.4, §15.4).
for raw in split_records(concatenated_parts):   # split on '\n'; tolerate trailing
                                                 # '\r'; discard bytes after last '\n'
    fields = split(raw, DELIM)        # raw split; count = (#DELIM) + 1
    f0     = fields[0]                # raw, unmodified

    # --- classify (§7.3) ---
    kind = "data"; algo = none
    if starts_with(f0, '#') and matches_reserved(f0):
        if is_checksum(f0):                                kind = "checksum"; algo = algo_of(f0)
        elif recognized_active_marker(f0, state.version):  kind = "marker"
        else:                                              kind = "ignore"  # unrecognized/custom
    elif starts_with(f0, '#'):
        kind = "ignore"                                    # comment

    # --- feed integrity (§7.4): a checksum line is excluded from its OWN
    #     accumulator; everything else (incl. other algos' markers) is fed ---
    for a in digests:                 # armed algorithms only
        if not (kind == "checksum" and algo == a):
            feed(digests[a], raw)     # full raw bytes incl. delimiters and '\n'

    # --- act ---
    if kind == "checksum":
        if algo not in digests:       # FIRST marker for algo: arm, ignore any value
            digests[algo] = new_accumulator()
        else:                         # subsequent marker
            dval = fields[1] if len(fields) > 1 else ""
            if dval != "" and digest(digests[algo]) != dval:
                report_corruption(algo)          # detection only
            digests[algo] = new_accumulator()    # reset; begin next segment
        continue
    if kind == "marker":
        apply_marker(state, f0, fields[1:])      # values decoded; keys raw
        continue
    if kind == "ignore":
        continue

    # --- data row ---
    is_tombstone = (len(fields) == 1) # no delimiter in the raw row
    key = decode(strip_if(state.strip, f0))
    if key == "":                     # empty-key rows are ignored
        continue
    if is_tombstone:
        store.delete(key)
        continue

    value = []
    for j in 1 .. len(fields) - 1:
        cell = decode(strip_if(state.strip, fields[j]))
        if cell == "" and state.fill_empty:
            cell = default_at(state.defaults, j)
        value[j] = cell
    # absent value columns are resolved at read time, bound to this row's state
    store.set(key, value, effective_defaults = state.defaults)

# read(key):
#   if key is live:
#       for column j: present-nonempty -> value[j];
#                     present-empty    -> (default if row was fill_empty else "");
#                     absent           -> key.effective_defaults[j]
#   else (missing):
#       return current_active_defaults  if state.return_on_missing
#       else NOT_FOUND
```

---

## Appendix C. Worked example (TSVZ)

Raw file (TAB shown as `→`):

```
#_version_#→1
#_defaults_#→guest→0
alice→Alice→30
bob→Bob
carol→→25
alice→Alice→31
bob
```

Replay:

- `#_version_#→1` — version 1 active.
- `#_defaults_#→guest→0` — value column 1 defaults to `guest`, column 2 to `0`.
- `alice→Alice→30` — alice = `["Alice", "30"]` (first appearance: position 0).
- `bob→Bob` — bob written with column 1 = `Bob`, column 2 absent → default `0`,
  giving `["Bob", "0"]` (position 1).
- `carol→→25` — column 1 present and empty, column 2 = `25`. With
  `#_fill_empty_with_default_#` at its default (`false`), the empty column 1 stays
  empty: carol = `["", "25"]` (position 2). (Had the marker been `true`, column 1
  would be `guest`, giving `["guest", "25"]`.)
- `alice→Alice→31` — last-wins update; alice = `["Alice", "31"]`, position
  unchanged (0).
- `bob` — a lone key with no value field: a tombstone. bob is deleted.

Final reads (iteration order alice, carol; bob is missing):

- `alice` → `["Alice", "31"]`
- `carol` → `["", "25"]`
- `bob`   → missing; with `#_return_defaults_when_missing_#` at its default
  (`true`), returns the active defaults `["guest", "0"]`. (A deletion here is thus
  observationally a reset to the defaults.)
