# tsvz.org website

## Intent

Replace the current site, `serve_spec.py`, with a two-page site at
https://tsvz.org/ that sells TSVZ. Today's site serves one page: the 1,421-line
spec, turned into HTML in the browser by marked.js from a CDN. It has no pitch,
no quick start and no links to an implementation.

The user's requirements: simple, fast, modern; easy to read for people and for
agents; works on every screen size; the important things first and the details
later; market what is great about TSVZ; link to the reference implementations.
Python is the only one today, and more may follow.

The site describes TSVZ 4.2 and later, the first Python release that follows
tsvz-spec-v1. It goes live once 4.2 is on PyPI.

### Decisions taken during design

| Question | Decision |
|---|---|
| Positioning | Format first. TSVZ is an open, plain-text format with a written spec. Python is the first entry in a list of implementations. |
| Pitch | "An append-only CSV that's also a key-value store": an append-only, WAL-style CSV/TSV store that people and agents read as-is, that works with existing CSV tools and spreadsheets, and that suits LLM tools and agentic workflows. |
| Spreadsheet claim | Made, with its caveat: a spreadsheet shows the log, every write in order; `tsvz scrub` compacts it to one row per key. |
| Pages | A landing page (`/`) and the spec (`/spec`), each also served as markdown, plus `/llms.txt` and `/llms-full.txt`. |
| Hosting | The user's own server, as today: systemd behind a reverse proxy. |
| Rendering | Our own markdown renderer, standard library only, run once at startup. Reading a page needs no JavaScript. |
| Visual style | "Plain text": warm paper, one teal accent, monospace where it matters, and the annotated file as the hero image. |
| Spec page | A sticky contents sidebar, and an "At a glance" box written for the site only. `tsvz-spec-v1.md` is not changed. |
| Domain | `https://tsvz.org/` |
| Spec terms | The spec text is CC BY 4.0, and anyone may implement TSVZ under any license. The Python implementation stays GPL-3.0-or-later. |
| Versions | The site describes TSVZ 4.2+. |
| Dark mode | Follows the operating system. No toggle. |

The mockups shown during design (`visual-direction.html`, `landing-layout.html`,
`spec-layout.html`) are under `.superpowers/brainstorm/`, which is not
committed.

## 1. Files

```
TSVZ/
  tsvz-spec-v1.md            unchanged; the only copy of the spec
  website/
    tsvz_site.py             renderer, page templates, server, static export
    index.md                 landing page content (Appendix A); served as-is to agents
    spec-glance.md           the "At a glance" box (Appendix B)
    style.css                inlined into every HTML page
    tsvz_site_test.py        tests (section 6)
  deploy/tsvz-site.service   replaces deploy/tsvz-spec.service
```

The directory is `website/`, not `site/`, so it cannot be confused with
Python's `site` module. `setup.py` ships only `TSVZ.py`, so none of this reaches
PyPI.

`website/tsvz_site.py` is one file that needs Python 3.8 or later and the
standard library only. It has two subcommands:

```
tsvz_site.py serve [-H HOST] [-p PORT] [--base-url URL] [--spec PATH]
tsvz_site.py build OUTDIR          [--base-url URL] [--spec PATH]
```

- `--base-url` defaults to `https://tsvz.org/`. `--spec` defaults to
  `../tsvz-spec-v1.md`, relative to the script.
- At startup it reads `index.md`, `spec-glance.md`, `style.css` and the spec,
  renders every response body once, and keeps a gzip copy of each. Requests
  only look up bytes. A missing input file exits with status 1 and a message.
- `serve` defaults to `127.0.0.1:8765`, the address and port of
  `serve_spec.py`.
- `serve` exits with status 1 and one line,
  `tsvz_site: cannot listen on HOST:PORT: <reason>`, when it cannot listen.
- `build OUTDIR` writes the same bodies as files (section 4.1), for any static
  host.
- The spec's date is the output of `git log -1 --format=%cs -- <spec>`, run in
  the spec's directory with a 2-second timeout. If git fails or is missing, the
  date is left out.

Where each kind of edit goes:

- The pitch, quick start, trade-offs or the implementations list: `index.md`.
  A new implementation is one new row in its table.
- The spec: `tsvz-spec-v1.md`, as today.
- The look: `style.css`.

A content edit is published by restarting the service.

## 2. Pages

Both pages share the template: a skip link, the nav, `<main>`, and the footer.

- **Nav.** The `tsvz` wordmark (links to `/`), then Spec, Implementations
  (`/#implementations`, hidden below 760px) and GitHub
  (`https://github.com/yufei-pan/TSVZ`). The current page's link is marked
  with `aria-current="page"`.
- **Footer.**
  - Spec v1 (draft) · CC BY 4.0, anyone may implement TSVZ under any license.
  - Python implementation: GPL-3.0-or-later.
  - Links: GitHub · PyPI · "This page as markdown" (`/index.md` or
    `/spec.md`) · `llms.txt`.

### 2.1 Landing page (`/`)

The content is `index.md` (Appendix A). Each `##` section is wrapped in
`<section class="sec-<heading id>">`, and the `<h2>` inside keeps the id, so
`/#implementations` lands on the heading. Everything before the first `##` is
wrapped in `<section class="hero">`, split into `<div class="hero-text">` and
`<div class="hero-art">` at the annotated file. Within a section, each `###`
starts a `<div class="sub">`, and the subs share one `<div class="subs">`. The
layout comes from `style.css` selecting on those classes. A test checks that
every `sec-` class the CSS uses exists.

| # | Section | Layout at 760px and wider | Below 760px |
|---|---|---|---|
| ① | Hero: h1, lead, action links, install line, annotated file, caption | Two columns: text on the left; file and caption on the right | One column; the action links fill the width; each file note moves under its line |
| ② | Why TSVZ: a 4-item list | 2 × 2 cards; each item's leading bold text is the card title | One card per row |
| ③ | How it works: a 3-item ordered list | 3 columns with numbered circles | Stacked |
| ④ | Quick start: two `###` headings, each followed by a code block | 2 columns | Stacked |
| ⑤ | Implementations: a table, then two short paragraphs | Table | One card per row; each cell labelled with its column name |
| ⑥ | When TSVZ isn't the right fit: a 5-item list | 2 text columns | 1 column |
| ⑦ | Footer (template) | One row | Wraps |

The hero and the Quick start split into two columns from 1000px, not 760px;
between 760px and 1000px they stack. At 768px a split hero squeezed the
headline into five lines.

### 2.2 Spec page (`/spec`)

- **Header.**
  - The title is the spec's H1.
  - Below it: "Version 1 · Draft · updated YYYY-MM-DD", using the spec's
    `**Version:**` and `**Status:**` lines and the git date.
  - A "Markdown" link to `/spec.md`, and the line "CC BY 4.0 · anyone may
    implement".
  - The spec's H1 and its Version, Status and Family lines appear only in this
    header. The body starts with the spec's one-line summary.
- **At a glance.** `spec-glance.md`, rendered in spec mode (section 3.3) so its
  `§` references are links. It is shown first, in a box with a teal left
  border.
- **Contents** (1000px and wider). A sticky sidebar listing the 21 sections and
  the appendices.
  - With JavaScript, the subsections of the section being read open under it,
    and the current entry is highlighted.
  - Without JavaScript, it lists the top-level sections only.
- **Contents** (below 1000px). A `<details>` "Contents" box above the glance
  box.
- **Body.** The spec, rendered in spec mode.

### 2.3 Not found

A short page in the template: "No page here", with links to `/` and `/spec`.
Agents get the one line `Not found. See https://tsvz.org/llms.txt`.

## 3. Markdown renderer

### 3.1 What it supports

This covers what `index.md`, `spec-glance.md` and `tsvz-spec-v1.md` use.

- **Blocks:**
  - ATX headings (`#` to `####`) and paragraphs;
  - bullet lists (`-`, `*`) and ordered lists (`1.`), nested by indentation;
  - fenced code blocks with an info string;
  - blockquotes;
  - GFM tables, including `\|` inside a cell;
  - `---` rules.
- **Inline:**
  - code spans and backslash escapes;
  - `**strong**`;
  - `*em*`, and `_em_` only at word boundaries, so `#_defaults_#` is untouched;
  - `[text](url)` links.
- **Escaping.** Every other character is HTML-escaped, which matters for
  `<sep>`, `<LF>`, `<lt>`, `&` and `"`.
- **Not supported:** raw HTML, setext headings, indented code blocks, reference
  links, images and footnotes. These come out as escaped text, never as markup.
- **Heading ids** follow GitHub's rule: lower-case; drop everything except
  letters, digits, spaces and hyphens; spaces become hyphens; repeated ids get
  `-1`, `-2` and so on. So `/spec#20-command-line-interface` keeps working, as
  the README already links it.
- **Output.** The renderer never writes `style="..."` attributes, so the CSP
  (section 4.3) can stay strict.

### 3.2 Site conventions

- **Annotated store files.** A fenced block whose info string is
  `<variant> <filename>`, with the variant one of `tsvz`, `csvz` or `psvz`, is
  shown as a file:
  - the filename appears as a title bar, and each line gets a line number;
  - the text after the first run of two or more spaces followed by `← ` is that
    line's note;
  - a line whose first field matches `#_..._#` is coloured as a marker;
  - any other line starting with `#` is coloured as a comment;
  - a line with no delimiter is a delete: struck through, and coloured;
  - otherwise the first field (the key) is bold.

  In the markdown copy the block stays as written, so the notes read as
  "← note".
- **Action links.** A paragraph made only of links separated by spaces gets
  `class="actions"`. CSS makes its first link the primary button.
- **Console blocks.** In a `console` block, lines starting with `$ ` are
  commands, and their `$` is drawn as the prompt. The Copy button copies only
  the commands, without the `$ `.
- **Tables.** Each `<td>` gets `data-label="<column heading>"`, which the
  phone layout uses.

### 3.3 Spec mode (spec page and glance box only)

- **Numbered paragraphs.** A paragraph or list item that starts with a number
  such as `4.3 ` or `12.2.1 ` shows that number in a muted
  `<span class="num">` and gets the anchor `id="s4.3"`. Headings get the
  anchors `s4`, `s12.2`, and `sA` to `sD` for the appendices. These sit on an
  empty `<span>` inside the heading, which keeps the GitHub-style id on the
  heading itself.
- **Section references.** `§4.3`, `§12.2.1`, `§A` and similar become links to
  their anchor.
  - Code is skipped; a trailing full stop is not part of the reference; both
    ends of a range like `§4.3–§4.5` are linked.
  - A reference with no anchor of its own links to the closest numbered parent
    that has one. In the current spec only `§19.2.3`, an item in a list, needs
    this; it links to `s19.2`.
  - The spec has 242 references today.
- **RFC 2119 keywords.** Upper-case MUST NOT, MUST, SHOULD NOT, SHOULD, MAY,
  REQUIRED, RECOMMENDED and OPTIONAL, outside code, get `<span class="rfc">`.
- **Notes.** Blockquotes get `class="note"` and appear as callout boxes.
- **Heading links.** Every heading gets a `¶` link to itself.

## 4. URLs, agents and serving

### 4.1 URLs

| URL | Browsers | Agents and command-line clients | `build` writes |
|---|---|---|---|
| `/`, `/index.html` | landing HTML | `index.md` | `index.html` |
| `/spec`, `/spec/` | spec HTML | `tsvz-spec-v1.md`, unchanged | `spec/index.html` |
| `/index.md` | `index.md` | same | `index.md` |
| `/spec.md`, `/tsvz-spec-v1.md` | the spec | same | `spec.md`, `tsvz-spec-v1.md` |
| `/llms.txt` | generated (4.4) | same | `llms.txt` |
| `/llms-full.txt` | `index.md` + `spec-glance.md` + spec | same | `llms-full.txt` |
| `/robots.txt` | allow all, plus a `Sitemap:` line | same | `robots.txt` |
| `/sitemap.xml` | `/` and `/spec`; `/spec` carries the git date | same | `sitemap.xml` |
| anything else | 404 page | one-line 404 | `404.html` |

`?format=md` and `?format=html` work on `/`, `/index.html` and `/spec`.
`serve_spec.py`'s URLs keep working. The one change is that `/` now returns
the landing page instead of the spec; its markdown links to `/spec.md` in its
first lines.

### 4.2 Choosing HTML or markdown

`wants_markdown()` is copied from `serve_spec.py` without change:

1. `?format=` decides, when present.
2. Otherwise, User-Agents of known command-line tools and AI agents get
   markdown.
3. Otherwise, `Accept: text/markdown` gets markdown.
4. Otherwise, a browser's page load (`Sec-Fetch-Mode: navigate` with
   `text/html`) gets HTML.
5. Otherwise, the `Accept` header is weighed.

One site rule runs first, without `?format=`. Link-preview fetchers and search
crawlers (Slackbot, Twitterbot, facebookexternalhit, LinkedInBot, Discordbot,
Googlebot, bingbot and similar) get HTML unless they also match an AI-agent
pattern. They read `<title>` and the Open Graph tags (4.3) and often send
`Accept: */*`, which `wants_markdown()` answers with markdown. This was added
after the final review; `wants_markdown()` itself is unchanged.

### 4.3 Responses

- **Methods.** GET and HEAD. Any other method gets 405 with
  `Allow: GET, HEAD`.
- **Content types:**
  - HTML: `text/html; charset=utf-8`.
  - Markdown: `text/markdown; charset=utf-8`, with
    `Content-Disposition: inline; filename="index.md"` or `"tsvz-spec-v1.md"`.
  - `llms.txt`, `llms-full.txt` and `robots.txt`: `text/plain; charset=utf-8`.
  - `sitemap.xml`: `application/xml`.
- **`Vary`.** `Accept-Encoding` on every response. `Accept` and `User-Agent`
  as well on the URLs where HTML or markdown is chosen.
- **`Link`.** HTML responses carry
  `Link: <https://tsvz.org/index.md>; rel="alternate"; type="text/markdown"`
  (or the `/spec.md` equivalent), and the same as a `<link>` in `<head>`.
- **Caching.**
  - The ETag is the first 16 hex digits of the body's SHA-256, in quotes.
  - A matching `If-None-Match` gets 304.
  - Every response has `Cache-Control: public, max-age=300`.
- **gzip.** Used when `Accept-Encoding` includes `gzip`.
- **Security headers.** `X-Content-Type-Options: nosniff` and
  `Referrer-Policy: strict-origin-when-cross-origin` on everything.
- **CSP.** HTML responses also get:

  ```
  Content-Security-Policy: default-src 'none'; style-src 'sha256-…';
    script-src 'sha256-…'; img-src data:; base-uri 'none';
    form-action 'none'; frame-ancestors 'none'
  ```

  The hashes are computed at startup from the page's one inline `<style>` and
  its inline `<script>`.
- **`<head>`:**
  - `<title>`: "TSVZ — an append-only CSV that's also a key-value store" for
    `/`, and "TSVZ Format Specification (v1)" for `/spec`;
  - a description, a canonical URL, and `color-scheme: light dark`;
  - Open Graph tags (title, description, URL, type, site name) and
    `twitter:card` `summary`;
  - the markdown alternate link;
  - the favicon: a small inline SVG as a data URI, a teal rounded square with
    a white monospace "z".
- **Logging.** One line per request on stderr, as `serve_spec.py` does.

### 4.4 `llms.txt`

This follows the llmstxt.org layout. It is generated, so it follows
`index.md`.

```
# TSVZ

> An append-only CSV that's also a key-value store: an open, plain-text
> key-value log format (spec v1) that people, agents and CSV tools read as-is.

<the bullets of spec-glance.md, with § references as absolute links>

## Docs

- [Overview](https://tsvz.org/index.md): what TSVZ is, quick start, trade-offs
- [Specification v1](https://tsvz.org/spec.md): the full format specification

## Implementations

- [Python (reference), 4.2+](https://github.com/yufei-pan/TSVZ): one line per row of the implementations table in index.md

## Optional

- [Everything in one file](https://tsvz.org/llms-full.txt)
```

## 5. Look and behaviour

### 5.1 Colour tokens

These are defined once on `:root`. Dark values apply under
`@media (prefers-color-scheme: dark)`. Every text colour has a contrast of
4.5:1 or more against both the page and card backgrounds; the ratios below
were measured.

| Token | Light | Dark | Use |
|---|---|---|---|
| `--bg` | `#faf9f6` | `#161513` | page |
| `--surface` | `#ffffff` | `#1f1e1b` | cards, file, tables |
| `--line` | `#e7e3d9` | `#34322d` | borders, rules |
| `--ink` | `#1b1b1a` (16.4) | `#ece9e2` (15.1) | text |
| `--muted` | `#57534b` (7.3) | `#bdb8ad` (9.2) | secondary text |
| `--faint` | `#6b675e` (5.4) | `#a39d91` (6.8) | meta, line numbers, captions |
| `--accent` | `#0f766e` (5.2) | `#2dd4bf` (9.8) | links, notes, highlights |
| `--on-accent` | `#ffffff` (5.5 on accent) | `#04201c` (9.2 on accent) | primary button text |
| `--marker` | `#b45309` (4.8) | `#f2a33a` (8.8) | `#_markers_#` in the file |
| `--delete` | `#b91c1c` (6.2) | `#f87171` (6.6) | deleted line in the file |
| `--accent-soft` | `#e3f1ee` | `#123a35` | step-number circles (accent on it: 4.7 / 6.7) |
| `--rfc` | `#9a3412` (6.9) | `#fb923c` (8.1) | MUST, SHOULD, MAY in the spec |
| `--inline-code` | `#efece4` | `#2a2925` | inline code background (ink on it: 14.6 / 12.0) |
| `--note-bg` | `#f1efe8` | `#24231f` | note callouts (muted on it: 6.7 / 8.0) |
| `--code-bg`, `--code-fg`, `--code-dim`, `--code-prompt` | `#1f1f1d`, `#e9e6df` (13.3), `#9a958b` (5.5), `#5eead4` (11.2) | same | code blocks (dark in both themes) |

### 5.2 Type, layout and screen sizes

- **Fonts.** `system-ui` for text and `ui-monospace` (SF Mono, Menlo,
  Consolas) for code. No web fonts.
- **Width.** Text measures at most about 70 characters. The landing page is at
  most 1100px wide.
- **Below 760px.** One column with a 16px gutter. "Implementations" leaves
  the nav.
- **760px and wider.** Cards, steps and file notes sit side by side.
- **1000px and wider.** The hero and the Quick start split into two columns,
  and the spec's sticky contents sidebar appears.
- **No sideways scrolling.** The page never scrolls horizontally. Tables and
  code blocks scroll inside their own box.

### 5.3 JavaScript

One inline script, about 1.6 KB, does two optional things:

- **The spec's contents sidebar.** An `IntersectionObserver` marks the
  section being read and opens its subsections.
- **Copy buttons.** One on every code block (commands only, for `console`
  blocks), added by the script, so no button appears without JavaScript.

With JavaScript off, every page reads and works the same apart from these.

### 5.4 Accessibility and print

- **Structure.** Proper landmarks (header, nav, main, section, footer),
  headings in order, and `lang="en"`.
- **Keyboard.** A skip link, and a visible focus ring.
- **Colour.** Never the only signal: a deleted line is also struck through
  and labelled.
- **Motion.** None.
- **Print.** The spec prints without the nav, sidebar or Copy buttons.

### 5.5 Size budget

- **Landing HTML.** 30 KB or less before compression, in one request.
- **Spec page.** About 150 KB, around 35 KB gzipped, in one request.
- **Other requests.** None: no images, no fonts, no third-party requests.

## 6. Testing

`website/tsvz_site_test.py` holds plain pytest functions, matching the repo's
style. It is named like `TSVZ_test.py` because `.gitignore` ignores `test*`:

```bash
python3 -m pytest website/tsvz_site_test.py -q
```

- **Renderer.**
  - One test for each construct in 3.1, including nested lists, `\|` in a
    table cell, `_em_` against `#_defaults_#`, and escaping of `<sep>`,
    `<lt>`, `&` and `"`.
  - Unsupported syntax comes out as escaped text.
  - GitHub-style heading ids, including repeats.
- **Conventions.** Annotated store files (notes, marker, comment, delete and
  key classes), action links, console blocks, and table `data-label`s.
- **Whole spec.** Rendering `tsvz-spec-v1.md` gives:
  - no leftover markdown (`**`, three backticks, `|---`);
  - an id on every heading;
  - every `§` reference as a link to an id that exists;
  - an existing id for every `tsvz-spec-v1.md#…` anchor in `README.md`;
  - balanced tags (checked with `html.parser`).
- **Landing.**
  - Every `sec-` class that `style.css` selects exists.
  - The HTML is within 30 KB.
  - `llms.txt` lists every row of the implementations table.
- **Quick start runs.** Extract the two Quick start code blocks from
  `index.md` and run them in a temporary directory against `../TSVZ.py`. The
  `pip install` line is skipped and `tsvz` maps to `python3 TSVZ.py`. Then:
  - the Python block writes exactly the hero file, minus its notes;
  - the console block's `cat` output and the `Created people.csvz` message
    match what the block shows;
  - its final `tsvz get` prints `alice,Alice,31`, the row its comment names.
- **Server.** Started on a free port in a thread, then checked for:
  - HTML or markdown for Chrome's page-load headers, curl, ClaudeBot,
    `Accept: text/markdown`, and both `?format=` overrides;
  - the `Vary`, `Link`, CSP, nosniff and Content-Type headers;
  - gzip, ETag → 304, HEAD, 404 (HTML and text) and 405.
- **Build.** `build OUTDIR` writes exactly the file list in 4.1.
- **Visual check.** A step in the plan, not an automated test:
  - headless Chrome screenshots of `/` and `/spec` at 375, 768 and 1280px
    wide, in light and dark;
  - a check that `document.documentElement.scrollWidth` is never larger than
    the window width.

## 7. Deployment and switch-over

- **Service.** `deploy/tsvz-site.service` replaces `deploy/tsvz-spec.service`.
  It keeps the same port, bind address and hardening, so the proxy
  configuration does not change. `ExecStart` runs
  `website/tsvz_site.py serve`.
- **Same commit.**
  - Remove `serve_spec.py` and `deploy/tsvz-spec.service`.
  - Add `https://tsvz.org` to the top of `README.md`.
  - Add `.superpowers/` to `.gitignore`, along with the missing final newline.
- **Branch.** The work is done on a `website` branch in `.worktrees/website`.
- **Release order.**
  1. Merge the site.
  2. The user releases TSVZ 4.2 to PyPI.
  3. The user deploys the site.

  Nothing in this work deploys anything or touches the server.

## 8. Out of scope and follow-ups

- **Out of scope:**
  - search;
  - an archive of earlier spec versions (worth adding once a v2 exists);
  - analytics;
  - a link-preview image;
  - a theme toggle;
  - translations;
  - a blog.
- **Follow-up (separate from this work).** On a terminal, `tsvz get` and
  `tsvz read` print a `-----+-----+--` rule under the first row, as if it were
  a header. To reproduce:

  ```
  tsvz set people.csvz alice Alice 31
  script -qc "tsvz get people.csvz alice" /dev/null
  ```

  The quick start avoids showing that output. The user decides whether to fix
  it before the 4.2 release.

## Appendix A. `website/index.md`

````markdown
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
````

## Appendix B. `website/spec-glance.md`

```markdown
- One record per line; a line counts only once its `\n` is written. §4.3
- Field 0 is the key; the last line for a key wins. §3.3
- A key alone, with no delimiter, deletes it. §9
- `#` lines are comments; `#_name_#` lines are markers. §11, §12
- `<sep>`, `<LF>`, `<lt>` and `<#>` stand for the delimiter, a newline, `<`
  and `#`. §13
- The extension picks the delimiter: `.tsvz` tab, `.csvz` comma, `.nsvz` NUL,
  `.psvz` pipe. Plain `.tsv` and `.csv` files are loose tables. §5
```
