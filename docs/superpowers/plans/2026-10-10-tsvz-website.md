# tsvz.org Website Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `serve_spec.py` with the two-page tsvz.org site described in the design: a landing page that sells TSVZ and the full spec, both served as HTML to browsers and as markdown to agents, from one standard-library module.

**Architecture:** One file, `website/tsvz_site.py`, built in five layers, one task each:
- **Renderer.** A markdown parser and `Renderer` that know the site's conventions (annotated store files, action links, console blocks, labelled table cells).
- **Spec mode.** `SpecRenderer`, a subclass that adds numbered anchors, `§` links, RFC 2119 keywords, note callouts and ¶ links.
- **Pages.** The HTML template and the landing and spec page bodies.
- **Site.** `Site` renders every response body once (HTML, markdown, `llms.txt`, `robots.txt`, `sitemap.xml`, gzip copies, ETags), plus `build OUTDIR`.
- **Serving.** A `ThreadingHTTPServer` handler that picks HTML or markdown per client (the `serve_spec.py` rules), plus `serve`.

The content lives beside it in `website/index.md`, `website/spec-glance.md` and `website/style.css`. Task 6 adds the systemd unit, removes `serve_spec.py`, updates the README and runs the browser check.

**Tech Stack:** Python ≥ 3.8 standard library (`http.server`, `re`, `gzip`, `hashlib`); pytest; ruff for linting; Docker `python:3.8-slim`; headless Chrome driven by `playwright-core` (from the local npx cache) for the visual check.

**Spec:** `docs/superpowers/specs/2026-10-10-tsvz-website-design.md` is the approved design. Its Appendix A and B are the content of `index.md` and `spec-glance.md`. Background: `tsvz-spec-v1.md` (the document the spec page renders), `serve_spec.py` (the server being replaced).

All the code in this plan was built and run in a scratch copy of the repository before the plan was written. Each task's code passed its tests there, cumulatively 17, 22, 30, 40 and 65 tests, on Python 3.13 and on 3.8 in Docker. Each task's new tests failed as stated in its step 2 when run against the previous task's code. The browser check passed at 375, 768 and 1280px in light and dark.

## Global Constraints

- `website/tsvz_site.py` needs only Python **3.8+** and the standard library. Do not use 3.9+ features: no `str.removeprefix`, no `dict | dict`, no `zoneinfo`; type hints only under `from __future__ import annotations`.
- `website/tsvz_site.py` stays one file. It uses 4-space indentation and snake_case, like the `serve_spec.py` it replaces.
- **Do not modify `tsvz-spec-v1.md` or `TSVZ.py`.** The spec file is rendered as it is; the glance box is site-only.
- Pages make **no requests to other sites**: no CDN, no web fonts, no images. CSS and JS are inline. The CSP is `default-src 'none'` plus hashes.
- The renderer never writes `style="..."` attributes, so the strict CSP holds.
- The landing page's HTML stays at **30 KB or less** before compression.
- Copy and colours follow the design (Appendix A and B, §5.1) except where "Deviations" below says otherwise. Public URL `https://tsvz.org/`. The spec text is CC BY 4.0 and anyone may implement it; the Python implementation is GPL-3.0-or-later.
- Work on branch `website` in the worktree `.worktrees/website`, created from `main` at execution time.
- Commit messages: one imperative sentence ending in a period, a blank line, then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew`.
- Run every command from the worktree root. In this environment `rm` is aliased to `rm -i`; use `rm -f`.
- **Do not deploy anything, restart services or touch any server.** The user deploys after releasing TSVZ 4.2 to PyPI.

## Deviations from the design

These were decided while building the plan. The task that introduces each one also records it in the design doc, so the two agree.

1. **Test file name** (Task 1). The tests are in `website/tsvz_site_test.py`, not `test_tsvz_site.py`, because `.gitignore` ignores `test*`. The name matches `TSVZ_test.py`.
2. **Section markup** (Task 3). Landing sections are `<section class="sec-<heading id>">`, and the `<h2>` keeps its id, so `/#implementations` lands on the heading. The hero is `<section class="hero">` with no id. CSS selects the classes.
3. **One script** (Task 3). A single inline script of about 1.6 KB does both jobs (Copy buttons, contents highlighting), so the CSP carries one script hash.
4. **Breakpoint for the hero and Quick start** (Task 3). They split into two columns from **1000px**, not 760px. At 768px a split hero squeezed the headline into five lines and cut off the code. The other 760px layouts are unchanged.
5. **Quick start copy** (Task 3). The code lines are shortened to at most 52 characters so they fit the columns:
   - `header=` moves onto its own line in the Python block;
   - the Python comments are shortened;
   - the `tsvz get` comment becomes `# → alice, Alice, 31`.

   Appendix A of the design is updated to match.
6. **Four extra colour tokens** (Task 3). `--accent-soft`, `--rfc`, `--inline-code` and `--note-bg` are added, each measured at 4.5:1 or better, and added to the design's §5.1.
7. **Serving details** (Task 5).
   - `serve` exits 1 with `tsvz_site: cannot listen on HOST:PORT: <reason>` when it cannot bind, instead of printing a traceback.
   - `/spec/` is served like `/spec`, since static hosts serve `spec/index.html` there.

## Review Focus

1. **The port is taken, or binding is not allowed, when systemd starts the service.** Expect one line on stderr and exit 1, not a traceback. Test: `test_cli_serve_port_in_use` (Task 5).
2. **A gzipping proxy (nginx) turns ETags weak**, so browsers send `If-None-Match: W/"…"`. Expect that to still get a 304. Test: `test_etags_and_304` (Task 5).
3. **A bad paste puts invalid UTF-8 into `index.md` or the spec.** Expect `SiteError` naming the file, and exit 1 from the CLI. Test: `test_non_utf8_source_is_a_site_error` (Task 4).
4. **systemd starts the script from a different working directory.** Expect the sources to be found relative to the script. Tests: `test_cli_build_and_missing_input` (Task 4) and `test_cli_serve_starts_and_answers` (Task 5) both run from a temporary directory.
5. **A later spec edit adds a `§` reference with no anchor, or markdown the renderer does not support.** Expect the whole-spec test to fail loudly rather than the site rendering it silently, and image syntax to stay text. Tests: `test_whole_spec_renders_cleanly` (Task 2) and `test_unsupported_syntax_stays_text` (Task 1).

## File Structure

| File | Change |
|---|---|
| `website/tsvz_site.py` | New. Renderer (Task 1), spec mode (Task 2), pages (Task 3), site and `build` (Task 4), serving and `serve` (Task 5). |
| `website/tsvz_site_test.py` | New. One test group per task, appended in order. |
| `website/index.md`, `website/spec-glance.md`, `website/style.css` | New (Task 3). |
| `deploy/tsvz-site.service` | New (Task 6). |
| `serve_spec.py`, `deploy/tsvz-spec.service` | Removed (Task 6). |
| `README.md`, `.gitignore` | Website link, a "Website" section and test command; `.superpowers/` (Task 6). |
| `docs/superpowers/specs/2026-10-10-tsvz-website-design.md` | Deviations 1, 2–6 and 7 (Tasks 1, 3, 5). |

### Names introduced (reference for every task)

- **Task 1:**
  - data: `Heading(level, text, id, number)`, `Block(kind, html, raw, heading)`;
  - functions: `escape`, `slugify(text, seen)`, `heading_number(text)`, `split_row(line)`, `parse_blocks(lines)`, `plain_text(markdown)`, `render(text) -> (blocks, anchors)`;
  - `class Renderer`: `render(text)`, and the hooks `prepare(nodes)`, `heading_inner(text, info)`, `para(text)` and `decorate(text)`, plus the class attribute `quote_class`.
  - Block kinds: `h1`–`h6`, `para`, `actions`, `code`, `file`, `table`, `list`, `quote`, `hr`.
- **Task 2:**
  - `resolve_ref(number, anchors)`;
  - `class SpecRenderer(Renderer)`, with `anchors` and `section_link(m)`;
  - `render(text, spec=False, anchors=None) -> (blocks, anchors)`.
- **Task 3:**
  - constants: `SCRIPT`, `FAVICON`, `GITHUB_URL`, `PYPI_URL`, `CC_BY_URL`, `LANDING_TITLE`, `SPEC_TITLE`, `NOT_FOUND_MAIN`;
  - functions: `landing_main(blocks)`, `spec_main(blocks, glance_html, date)`, and `page(*, css, title, description, canonical, md_url, current, main, body_class, md_link_text)`.
- **Task 4:**
  - constants: `HERE`, `DEFAULT_SPEC`, `DEFAULT_BASE_URL`, `LLMS_SUMMARY`, `BUILD_FILES`;
  - helpers: `_csp_hash(text)`, `_git_date(path)`, `implementations(index_md)`;
  - errors and data: `SiteError`, `Body(data, gz, ctype, etag, disposition, link)`;
  - `class Site(base_url, spec_path, root)`, with `.bodies`, `.csp`, `.anchors`, `.spec_date`, `llms_txt()`, `sitemap_xml()` and `build(outdir)`;
  - `main(argv)`.
- **Task 5:**
  - constants: `DEFAULT_PORT`, `NEGOTIATED`, `FIXED`;
  - negotiation: `wants_markdown(...)`, copied from `serve_spec.py`;
  - helpers: `accepts_gzip(header)`, `etag_matches(header, etag)`;
  - `class Handler`, and `make_server(site, host, port)`;
  - `main(argv)` with `serve`.

---

### Task 1: Markdown renderer and site conventions

**Files:**
- Create: `website/tsvz_site.py`
- Create: `website/tsvz_site_test.py`
- Modify: `docs/superpowers/specs/2026-10-10-tsvz-website-design.md` (deviation 1)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `render(text) -> (list[Block], set)`. The set is empty until Task 2.
  - `Block(kind, html, raw, heading)`, where `heading` is a `Heading(level, text, id, number)` for headings and `None` otherwise. `raw` is the markdown source of the block: paragraph text, code body, or table lines joined by `\n`.
  - `split_row(line) -> list[str]`, `plain_text(markdown) -> str`, `escape(text) -> str`, `heading_number(text) -> str | None`.
  - `class Renderer` with the hooks Task 2 overrides: `prepare(nodes)`, `heading_inner(text, info)`, `para(text)`, `decorate(text)` and `quote_class`.

- [ ] **Step 1: Write the failing tests**

Create `website/tsvz_site_test.py`:

````python
"""Tests for website/tsvz_site.py.  Run: python3 -m pytest website/tsvz_site_test.py -q"""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

import tsvz_site  # noqa: E402

SPEC = (REPO / "tsvz-spec-v1.md").read_text(encoding="utf-8")


def html_of(text, **kw):
    blocks, _ = tsvz_site.render(text, **kw)
    return "".join(b.html for b in blocks)


# -- Task 1: renderer and site conventions ------------------------------------


def test_escape():
    assert tsvz_site.escape('a<b & "c">') == "a&lt;b &amp; &quot;c&quot;&gt;"


def test_headings_get_github_ids():
    out = html_of("# TSVZ — Format Specification\n\n## 20. Command-line interface\n\n"
                  "### 21.13 `serve` and `stop`\n\n## Notes\n\n## Notes\n")
    assert '<h1 id="tsvz--format-specification">' in out
    assert '<h2 id="20-command-line-interface">20. Command-line interface</h2>' in out
    assert '<h3 id="2113-serve-and-stop">21.13 <code>serve</code> and <code>stop</code></h3>' in out
    assert '<h2 id="notes">' in out and '<h2 id="notes-1">' in out


def test_heading_blocks_carry_heading_info():
    blocks, _ = tsvz_site.render("## 4. File encoding\n\ntext\n")
    assert blocks[0].kind == "h2"
    assert blocks[0].heading == tsvz_site.Heading(2, "4. File encoding", "4-file-encoding", "4")
    assert blocks[1].kind == "para" and blocks[1].raw == "text"


def test_inline_formatting():
    out = html_of("**bold** *em* _j_ `#_defaults_#` and #_defaults_# and snake_case_name "
                  "[link](https://x.org/a?b=1&c=2) \\*not em\\*")
    assert "<strong>bold</strong>" in out
    assert "<em>em</em>" in out and "<em>j</em>" in out
    assert "<code>#_defaults_#</code>" in out
    assert "and #_defaults_# and snake_case_name" in out
    assert '<a href="https://x.org/a?b=1&amp;c=2">link</a>' in out
    assert "*not em*" in out


def test_html_specials_are_escaped():
    out = html_of('A `<sep>` token, a <lt> and & and "q" <b>not html</b>\n')
    assert "<code>&lt;sep&gt;</code>" in out
    assert "a &lt;lt&gt; and &amp; and &quot;q&quot; &lt;b&gt;not html&lt;/b&gt;" in out


def test_code_span_with_double_backticks():
    assert "<code>a`b</code>" in html_of("``a`b``\n")


def test_lists_tight_nested_and_ordered():
    out = html_of("- one\n  continued\n- two\n  - inner\n\n3. three\n4. four\n")
    assert "<ul>\n<li>one\ncontinued</li>\n<li>two<ul>\n<li>inner</li>\n</ul>\n</li>\n</ul>" in out
    assert '<ol start="3">\n<li>three</li>\n<li>four</li>\n</ol>' in out


def test_loose_list_wraps_paragraphs():
    out = html_of("- one\n\n- two\n")
    assert "<li><p>one</p>\n</li>" in out and "<li><p>two</p>\n</li>" in out


def test_ordered_siblings_are_not_continuation_lines():
    out = html_of("19.2 Steps:\n\n   1. Identify a prefix\n      of parts.\n   2. Spawn a worker\n"
                  "   3. Write the snapshot\n")
    assert out.count("<li>") == 3
    assert "<li>Identify a prefix\nof parts.</li>" in out


def test_list_interrupts_paragraph_and_lazy_lines_continue():
    out = html_of("Intro line\n- item one\nlazy continuation\n- item two\n")
    assert "<p>Intro line</p>" in out
    assert "<li>item one\nlazy continuation</li>" in out


def test_table_with_escaped_pipe():
    out = html_of("| Variant | Delimiter |\n|---|---|\n| PSVZ | pipe `\\|` |\n| X |\n")
    assert "<thead><tr><th>Variant</th><th>Delimiter</th></tr></thead>" in out
    assert "<code>|</code>" in out
    assert out.count("<tr>") == 3  # a short row is padded


def test_fenced_code_blockquote_and_rule():
    out = html_of("```sh\n$ tsvz -V\n<x> & y\n```\n\n> a note\n> on two lines\n\n---\n")
    assert '<pre><code class="language-sh">$ tsvz -V\n&lt;x&gt; &amp; y</code></pre>' in out
    assert "<blockquote>\n<p>a note\non two lines</p>\n</blockquote>" in out
    assert "<hr>" in out


def test_unsupported_syntax_stays_text():
    out = html_of("<div>raw</div>\n\n![alt](img.png)\n\nTitle\n===\n\n[ref][1]\n")
    assert "<div>" not in out and "&lt;div&gt;raw&lt;/div&gt;" in out
    assert "<img" not in out and "<a" not in out
    assert "![alt](img.png)" in out and "[ref][1]" in out


STORE = ("```csvz people.csvz\n#id,name,score  ← header (a comment)\n#_defaults_#,guest,0\n"
         "alice,Alice,30\nbob             ← delete: just the key\n```\n")


def test_store_file_block():
    blocks, _ = tsvz_site.render(STORE)
    assert blocks[0].kind == "file"
    out = blocks[0].html
    assert out.startswith('<figure class="file"><figcaption>people.csvz</figcaption><ol>')
    assert '<li class="cm"><code>#id,name,score</code><span class="note">header (a comment)</span></li>' in out
    assert '<li class="mk"><code>#_defaults_#,guest,0</code></li>' in out
    assert "<li><code><b>alice</b>,Alice,30</code></li>" in out
    assert '<li class="del"><code>bob</code><span class="note">delete: just the key</span></li>' in out


def test_actions_paragraph():
    blocks, _ = tsvz_site.render("[Read the spec](/spec) [Get one](#implementations)\n\nNot [only](/x) links\n")
    assert blocks[0].kind == "actions"
    assert blocks[0].html.startswith('<p class="actions"><a href="/spec">Read the spec</a>')
    assert blocks[1].kind == "para" and "<p>Not" in blocks[1].html


def test_console_block():
    out = html_of("```console\n$ tsvz get s.csvz alice    # → the row\nalice,Alice,31\n```\n")
    assert ('<span class="prompt">$ </span><span class="cmd">tsvz get s.csvz alice</span>'
            '<span class="c">    # → the row</span>') in out
    assert '<span class="out">alice,Alice,31</span>' in out


def test_table_cells_carry_data_labels():
    out = html_of("| Language | CLI §20 |\n|---|---|\n| Python | ✓ |\n")
    assert '<td data-label="Language">Python</td><td data-label="CLI §20">✓</td>' in out
````

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m pytest website/tsvz_site_test.py -q`
Expected: collection error `ModuleNotFoundError: No module named 'tsvz_site'`.

- [ ] **Step 3: Write the renderer**

Create `website/tsvz_site.py`:

````python
#!/usr/bin/env python3
"""The tsvz.org website: a landing page and the TSVZ format specification.

Reads website/index.md, website/spec-glance.md, website/style.css and
tsvz-spec-v1.md once at startup, renders them, and serves the result: HTML to
browsers, the markdown sources to agents and command-line clients.
Standard library only; Python 3.8 or later.

Usage:
    python3 website/tsvz_site.py serve [-H HOST] [-p PORT]
    python3 website/tsvz_site.py build OUTDIR
"""

from __future__ import annotations

import re
from collections import namedtuple

# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------

Heading = namedtuple("Heading", "level text id number")
Block = namedtuple("Block", "kind html raw heading")

_FENCE = re.compile(r"^( {0,3})(`{3,})[ \t]*([^`]*)$")
_FENCE_CLOSE = re.compile(r"^ {0,3}(`{3,})[ \t]*$")
_ATX = re.compile(r"^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$")
_HR = re.compile(r"^ {0,3}([-*_])(?:[ \t]*\1){2,}[ \t]*$")
_QUOTE = re.compile(r"^ {0,3}> ?")
_LIST_ITEM = re.compile(r"^( *)([-*+]|\d{1,9}[.)])( +|$)")
_TABLE_DELIM = re.compile(r"^ {0,3}\|?[ \t]*:?-+:?[ \t]*(\|[ \t]*:?-+:?[ \t]*)*\|?[ \t]*$")
_SECTION_NUMBER = re.compile(r"^(\d+(?:\.\d+)*)\.?[ \t]")
_APPENDIX = re.compile(r"^Appendix ([A-Z])\.")
_STORE_VARIANTS = {"tsvz": "\t", "csvz": ",", "psvz": "|"}
_NOTE = re.compile(r"^(.*?\S)[ \t]{2,}← (.*)$")
_MARKER_KEY = re.compile(r"^#_[A-Za-z0-9_-]+_#$")
_ACTIONS = re.compile(r"^(?:\[[^\]]+\]\([^)\s]+\)\s*)+$")


def escape(text):
    """Escape &, <, > and " for HTML text and attribute values."""
    return (text.replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def slugify(text, seen):
    """GitHub-style heading id; repeats get -1, -2, ... (tracked in seen)."""
    slug = "".join(ch for ch in text.lower() if ch.isalnum() or ch in "-_ ")
    slug = slug.replace(" ", "-")
    count = seen.get(slug, 0)
    seen[slug] = count + 1
    return slug if count == 0 else "%s-%d" % (slug, count)


def heading_number(text):
    """The section number a heading starts with ("4", "12.2", "A"), or None."""
    m = _SECTION_NUMBER.match(text) or _APPENDIX.match(text)
    return m.group(1) if m else None


def _indent(line):
    return len(line) - len(line.lstrip(" "))


def _list_marker(line):
    """(ordered, marker, indent, content_offset) for a list item line, or None."""
    m = _LIST_ITEM.match(line)
    if not m or len(m.group(1)) > 3 or _HR.match(line):
        return None
    indent = len(m.group(1))
    marker = m.group(2)
    spaces = len(m.group(3)) if m.group(3) else 1
    if spaces > 4:
        spaces = 1
    return marker[-1] in ".)", marker, indent, indent + len(marker) + spaces


def _is_table_start(lines, i):
    return (i + 1 < len(lines) and "|" in lines[i] and "|" in lines[i + 1]
            and _TABLE_DELIM.match(lines[i + 1]) is not None)


def _starts_block(lines, i, in_paragraph=True):
    """True when lines[i] starts a block that ends a paragraph.

    Inside a paragraph only a bullet item or an ordered item numbered 1 does
    (as in CommonMark); outside one (in_paragraph=False), any list item does.
    """
    line = lines[i]
    if _FENCE.match(line) or _ATX.match(line) or _HR.match(line) or _QUOTE.match(line):
        return True
    mk = _list_marker(line)
    if mk:
        if not in_paragraph:
            return True
        ordered, marker = mk[0], mk[1]
        rest = line[mk[3]:].strip()
        return bool(rest) and (not ordered or int(marker[:-1]) == 1)
    return _is_table_start(lines, i)


def split_row(line):
    """Split a GFM table row into cells; \\| stays inside its cell as |."""
    s = line.strip()
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|") and not s.endswith("\\|"):
        s = s[:-1]
    return [c.strip().replace("\\|", "|") for c in re.split(r"(?<!\\)\|", s)]


def parse_blocks(lines):
    """Parse markdown lines into nodes: tuples whose first item is the kind."""
    nodes = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        m = _FENCE.match(line)
        if m:
            indent, fence, info = len(m.group(1)), m.group(2), m.group(3).strip()
            body = []
            i += 1
            while i < n:
                close = _FENCE_CLOSE.match(lines[i])
                if close and len(close.group(1)) >= len(fence):
                    i += 1
                    break
                body.append(lines[i][min(indent, _indent(lines[i])):])
                i += 1
            nodes.append(("code", info, "\n".join(body)))
            continue
        m = _ATX.match(line)
        if m:
            nodes.append(("heading", len(m.group(1)), (m.group(2) or "").strip()))
            i += 1
            continue
        if _HR.match(line):
            nodes.append(("hr",))
            i += 1
            continue
        if _QUOTE.match(line):
            quoted = []
            while i < n and lines[i].strip():
                if _QUOTE.match(lines[i]):
                    quoted.append(_QUOTE.sub("", lines[i], count=1))
                elif not _starts_block(lines, i):
                    quoted.append(lines[i])  # lazy continuation
                else:
                    break
                i += 1
            nodes.append(("quote", parse_blocks(quoted)))
            continue
        if _list_marker(line):
            node, i = _parse_list(lines, i)
            nodes.append(node)
            continue
        if _is_table_start(lines, i):
            rows = [lines[i], lines[i + 1]]
            i += 2
            while i < n and lines[i].strip() and "|" in lines[i]:
                rows.append(lines[i])
                i += 1
            nodes.append(("table", rows))
            continue
        para = [line.strip()]
        i += 1
        while i < n and lines[i].strip() and not _starts_block(lines, i):
            para.append(lines[i].strip())
            i += 1
        nodes.append(("para", "\n".join(para)))
    return nodes


def _parse_list(lines, i):
    """Parse the list starting at lines[i]; returns (node, next index)."""
    ordered, marker, _, _ = _list_marker(lines[i])
    kind = marker[-1]
    start = int(marker[:-1]) if ordered else 1
    items, loose, n = [], False, len(lines)
    while i < n:
        mk = _list_marker(lines[i])
        if not mk or mk[0] != ordered or mk[1][-1] != kind:
            break
        offset = mk[3]
        body = [lines[i][offset:]]
        i += 1
        while i < n:
            line = lines[i]
            if not line.strip():
                body.append("")
            elif _indent(line) >= offset:
                body.append(line[offset:])
            elif body[-1].strip() and not _starts_block(lines, i, in_paragraph=False):
                body.append(line.strip())  # lazy continuation of a paragraph
            else:
                break
            i += 1
        trailing = 0
        while body and not body[-1].strip():
            body.pop()
            trailing += 1
        for k in range(len(body) - 1):
            if not body[k].strip() and _indent(body[k + 1]) == 0 and not _list_marker(body[k + 1]):
                loose = True  # a blank line between two blocks of this item
        items.append(parse_blocks(body))
        if trailing:
            nxt = _list_marker(lines[i]) if i < n else None
            if not (nxt and nxt[0] == ordered and nxt[1][-1] == kind):
                break
            loose = True  # a blank line between two items
    return ("list", ordered, start, items, loose), i


class Renderer:
    """Turns markdown into a list of top-level HTML Blocks.

    Besides plain markdown it knows the site's conventions: annotated store
    files (```csvz people.csvz), paragraphs made only of links (actions),
    console blocks, and data-label attributes on table cells.
    """

    quote_class = ""

    def __init__(self):
        self.seen = {}
        self.ids = set()

    def render(self, text):
        nodes = parse_blocks(text.split("\n"))
        self.prepare(nodes)
        return [self.block(node) for node in nodes]

    def prepare(self, nodes):
        """Called with the parsed nodes before any is rendered."""

    # -- blocks ------------------------------------------------------------

    def block(self, node):
        kind = node[0]
        if kind == "heading":
            return self.heading(node[1], node[2])
        if kind == "para":
            kind = "actions" if _ACTIONS.match(node[1]) else "para"
            return Block(kind, self.para(node[1]), node[1], None)
        if kind == "code":
            info = node[1].split()
            if len(info) >= 2 and info[0] in _STORE_VARIANTS:
                return Block("file", self.store_file(info[0], info[1], node[2]), node[2], None)
            return Block("code", self.code(info[0] if info else "", node[2]), node[2], None)
        if kind == "table":
            return Block("table", self.table(node[1]), "\n".join(node[1]), None)
        return Block(kind, self.node(node), "", None)

    def node(self, node, tight=False):
        kind = node[0]
        if kind == "para":
            return self.inline(node[1]) if tight else self.para(node[1])
        if kind == "list":
            return self.list_block(node)
        if kind == "hr":
            return "<hr>\n"
        if kind == "quote":
            inner = "".join(self.node(child) for child in node[1])
            return "<blockquote%s>\n%s</blockquote>\n" % (self.quote_class, inner)
        return self.block(node).html

    def heading(self, level, text):
        plain = re.sub(r"`([^`]*)`", r"\1", text)
        hid = slugify(plain, self.seen)
        self.ids.add(hid)
        info = Heading(level, plain, hid, heading_number(text))
        html = '<h%d id="%s">%s</h%d>\n' % (level, hid, self.heading_inner(text, info), level)
        return Block("h%d" % level, html, text, info)

    def heading_inner(self, text, info):
        return self.inline(text)

    def para(self, text):
        if _ACTIONS.match(text):
            return '<p class="actions">%s</p>\n' % self.inline(text)
        return "<p>%s</p>\n" % self.inline(text)

    def list_block(self, node):
        _, ordered, start, items, loose = node
        tag = "ol" if ordered else "ul"
        attr = ' start="%d"' % start if ordered and start != 1 else ""
        parts = []
        for item in items:
            parts.append("<li>%s</li>\n" % "".join(self.node(child, tight=not loose) for child in item))
        return "<%s%s>\n%s</%s>\n" % (tag, attr, "".join(parts), tag)

    def code(self, lang, text):
        cls = ' class="language-%s"' % escape(lang) if lang else ""
        if lang == "console":
            lines = []
            for line in text.split("\n"):
                if not line.startswith("$ "):
                    lines.append('<span class="out">%s</span>' % escape(line))
                    continue
                cmd, comment = line[2:], ""
                m = re.match(r"^(.*?\S)(\s+#\s.*)$", cmd)
                if m:
                    cmd, comment = m.group(1), '<span class="c">%s</span>' % escape(m.group(2))
                lines.append('<span class="prompt">$ </span><span class="cmd">%s</span>%s'
                             % (escape(cmd), comment))
            body = "\n".join(lines)
        else:
            body = escape(text)
        return '<div class="code"><pre><code%s>%s</code></pre></div>\n' % (cls, body)

    def store_file(self, variant, filename, text):
        delim = _STORE_VARIANTS[variant]
        rows = []
        for line in text.rstrip("\n").split("\n"):
            m = _NOTE.match(line)
            content, note = (m.group(1), m.group(2)) if m else (line.rstrip(), "")
            first = content.split(delim, 1)[0]
            if content.startswith("#") and _MARKER_KEY.match(first):
                cls, code = ' class="mk"', escape(content)
            elif content.startswith("#"):
                cls, code = ' class="cm"', escape(content)
            elif delim not in content:
                cls, code = ' class="del"', escape(content)
            else:
                cls, code = "", "<b>%s</b>%s" % (escape(first), escape(content[len(first):]))
            note_html = '<span class="note">%s</span>' % escape(note) if note else ""
            rows.append("<li%s><code>%s</code>%s</li>\n" % (cls, code, note_html))
        return '<figure class="file"><figcaption>%s</figcaption><ol>\n%s</ol></figure>\n' % (
            escape(filename), "".join(rows))

    def table(self, rows):
        head = split_row(rows[0])
        out = ['<div class="table-wrap"><table>\n<thead><tr>']
        out.extend("<th>%s</th>" % self.inline(cell) for cell in head)
        out.append("</tr></thead>\n<tbody>\n")
        for row in rows[2:]:
            cells = split_row(row)
            cells += [""] * (len(head) - len(cells))
            out.append("<tr>")
            for label, cell in zip(head, cells):
                out.append('<td data-label="%s">%s</td>' % (escape(plain_text(label)), self.inline(cell)))
            out.append("</tr>\n")
        out.append("</tbody></table></div>\n")
        return "".join(out)

    # -- inline ------------------------------------------------------------

    def inline(self, text):
        store = []

        def keep(fragment):
            store.append(fragment)
            return "\ue000%d\ue001" % (len(store) - 1)

        text = self.code_spans(text, keep)
        text = re.sub(r"\\([!-/:-@\[-`{-~])", lambda m: keep(escape(m.group(1))), text)
        text = re.sub(r"(?<!!)\[([^\]]+)\]\(([^)\s]+)\)", lambda m: keep('<a href="%s">%s</a>' % (
            escape(m.group(2)), self.finish(m.group(1)))), text)
        text = self.finish(text)
        while "\ue000" in text:
            text = re.sub(r"\ue000(\d+)\ue001", lambda m: store[int(m.group(1))], text)
        return text

    @staticmethod
    def code_spans(text, keep):
        out, i = [], 0
        while True:
            j = text.find("`", i)
            if j < 0:
                out.append(text[i:])
                return "".join(out)
            run = re.match(r"`+", text[j:]).group(0)
            k = j + len(run)
            close = re.compile(r"(?<!`)%s(?!`)" % run).search(text, k)
            if not close:
                out.append(text[i:k])  # unmatched backticks are literal
                i = k
                continue
            code = text[k:close.start()].replace("\n", " ")
            if len(code) > 2 and code[0] == " " and code[-1] == " " and code.strip():
                code = code[1:-1]
            out.append(text[i:j])
            out.append(keep("<code>%s</code>" % escape(code)))
            i = close.end()

    def finish(self, text):
        """Escape text outside code spans and links, then apply emphasis."""
        text = self.decorate(escape(text))
        text = re.sub(r"\*\*(?=\S)(.+?)(?<=\S)\*\*", r"<strong>\1</strong>", text, flags=re.S)
        text = re.sub(r"(?<![\w*])\*(?=[^\s*])(.+?)(?<=[^\s*])\*(?![\w*])", r"<em>\1</em>", text, flags=re.S)
        text = re.sub(r"(?<![\w#])_(?=[^\s_])(.+?)(?<=[^\s_])_(?![\w#])", r"<em>\1</em>", text, flags=re.S)
        return text

    def decorate(self, text):
        """Hook for escaped text before emphasis; the plain renderer keeps it."""
        return text


def plain_text(markdown):
    """Inline markdown reduced to plain text (for titles, labels, descriptions)."""
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", markdown)
    text = text.replace("`", "").replace("**", "")
    return re.sub(r"\s+", " ", text).strip()


def render(text):
    """Render markdown; returns (blocks, anchors).  anchors is always empty here."""
    return Renderer().render(text), set()
````

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m pytest website/tsvz_site_test.py -q && ruff check --select E,F,W --line-length 120 website/`
Expected: `17 passed`; `All checks passed!`

- [ ] **Step 5: Record deviation 1 in the design doc**

In `docs/superpowers/specs/2026-10-10-tsvz-website-design.md`, make these replacements (each text occurs once):

1. Replace:

   ````text
       test_tsvz_site.py        tests (section 6)
   ````

   with:

   ````text
       tsvz_site_test.py        tests (section 6)
   ````

2. Replace:

   ````text
   `website/test_tsvz_site.py` holds plain pytest functions, matching the repo's
   style:
   ````

   with:

   ````text
   `website/tsvz_site_test.py` holds plain pytest functions, matching the repo's
   style. It is named like `TSVZ_test.py` because `.gitignore` ignores `test*`:
   ````

3. Replace:

   ````text
   python3 -m pytest website/test_tsvz_site.py -q
   ````

   with:

   ````text
   python3 -m pytest website/tsvz_site_test.py -q
   ````

- [ ] **Step 6: Commit**

```bash
git add website/tsvz_site.py website/tsvz_site_test.py docs/superpowers/specs/2026-10-10-tsvz-website-design.md
git commit -F - <<'MSG'
Add the tsvz.org markdown renderer with the site's block conventions.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
MSG
```

---

### Task 2: Spec mode (`SpecRenderer`)

**Files:**
- Modify: `website/tsvz_site.py` (replace `render()`, the last function)
- Modify: `website/tsvz_site_test.py` (imports; append the Task 2 group)

**Interfaces:**
- Consumes (Task 1):
  - the class: `Renderer` and its hooks;
  - functions: `heading_number`, `render`;
  - data: `Block`, `Heading`.
- Produces:
  - `render(text, spec=False, anchors=None) -> (blocks, anchors)`. With `spec=True`, `anchors` is every section number found (`"4"`, `"4.3"`, `"A"`, …) plus the seed `anchors`.
  - `resolve_ref(number, anchors) -> str | None`.
  - `SpecRenderer(anchors=None)`.
  - Spec HTML hooks used by Tasks 3 and 5: the ids `s<number>`, and the classes `sref`, `num`, `rfc`, `pilcrow` and `note`.

- [ ] **Step 1: Write the failing tests**

In `website/tsvz_site_test.py`, make the import lines at the top read:

````python
import re
import sys
from html.parser import HTMLParser
from pathlib import Path
````

Append to `website/tsvz_site_test.py`:

````python
# -- Task 2: spec mode ------------------------------------------------------


def test_spec_mode_numbered_paragraphs_and_section_links():
    text = ("## 4. Framing\n\n4.3 A reader **MUST** discard bytes; see §4.3–§4.5, §19.2.3.\n\n"
            "4.5 Recovery is unconditional (§9) and `§4.3` in code stays.\n\n## 19. Snapshot\n\n"
            "19.2 Steps.\n")
    blocks, anchors = tsvz_site.render(text, spec=True)
    out = "".join(b.html for b in blocks)
    assert {"4", "4.3", "4.5", "19", "19.2"} <= anchors
    assert '<span class="anchor" id="s4"></span>' in out
    assert '<p id="s4.3"><span class="num">4.3</span> A reader' in out
    assert '<a class="sref" href="#s4.3">§4.3</a>–<a class="sref" href="#s4.5">§4.5</a>' in out
    assert '<a class="sref" href="#s19.2">§19.2.3</a>.' in out  # closest parent; full stop outside
    assert "§9" in out and 'href="#s9"' not in out  # no anchor anywhere: left as text
    assert "<code>§4.3</code>" in out
    assert '<strong><span class="rfc">MUST</span></strong>' in out


def test_spec_mode_notes_pilcrows_and_external_anchors():
    out = html_of("## 2. Terms\n\n> A note with MAY.\n", spec=True)
    assert ('<h2 id="2-terms"><span class="anchor" id="s2"></span>2. Terms '
            '<a class="pilcrow" href="#2-terms" aria-label="Link to this section">¶</a></h2>') in out
    assert '<blockquote class="note">' in out and '<span class="rfc">MAY</span>' in out
    glance = html_of("- Last line wins. §3.3\n", spec=True, anchors={"3", "3.3"})
    assert '<a class="sref" href="#s3.3">§3.3</a>' in glance


def test_plain_mode_has_no_spec_features():
    out = html_of("## 2. Terms\n\n2.1 A reader MUST see §2.\n\n> note\n")
    assert "pilcrow" not in out and "sref" not in out and "rfc" not in out
    assert 'class="note"' not in out and "<p>2.1 A reader MUST see §2.</p>" in out


class _TagChecker(HTMLParser):
    VOID = {"meta", "link", "br", "hr", "img", "input"}

    def __init__(self):
        super().__init__()
        self.stack, self.errors = [], []

    def handle_starttag(self, tag, attrs):
        if tag not in self.VOID:
            self.stack.append(tag)

    def handle_endtag(self, tag):
        if not self.stack or self.stack[-1] != tag:
            self.errors.append("unexpected </%s> with open %s" % (tag, self.stack[-3:]))
        else:
            self.stack.pop()


def assert_balanced(html):
    checker = _TagChecker()
    checker.feed(html)
    checker.close()
    assert not checker.errors, checker.errors[:3]
    assert not checker.stack, checker.stack


def test_whole_spec_renders_cleanly():
    out = html_of(SPEC, spec=True)
    outside_code = re.sub(r"<code[^>]*>.*?</code>", "", out, flags=re.S)
    assert "**" not in outside_code and "```" not in out and "|---" not in out
    assert not re.search(r"<h[1-4](?! id=)", out)  # every heading has an id
    ids = re.findall(r'id="([^"]+)"', out)
    assert len(ids) == len(set(ids)), "duplicate ids"
    missing = set(re.findall(r'href="#([^"]+)"', out)) - set(ids)
    assert not missing, sorted(missing)[:5]
    unlinked = re.sub(r'<a class="sref"[^>]*>§[^<]*</a>', "", outside_code)
    assert "§" not in unlinked
    assert_balanced(out)


def test_readme_spec_anchors_exist():
    ids = set(re.findall(r'id="([^"]+)"', html_of(SPEC, spec=True)))
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    anchors = re.findall(r"tsvz-spec-v1\.md#([A-Za-z0-9_-]+)", readme)
    assert anchors and set(anchors) <= ids
````

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m pytest website/tsvz_site_test.py -q`
Expected: failures with `TypeError: render() got an unexpected keyword argument 'spec'`; the 17 Task 1 tests still pass.

- [ ] **Step 3: Add spec mode**

In `website/tsvz_site.py`, replace the last function, `render(text)`, with:

````python
_PARA_NUMBER = re.compile(r"^(\d+\.\d+(?:\.\d+)*)[ \t]+")
_SECTION_REF = re.compile(r"§(\d+(?:\.\d+)*|[A-Z](?![A-Za-z]))")
_RFC = re.compile(r"\b(MUST\s+NOT|MUST|SHOULD\s+NOT|SHOULD|MAY|REQUIRED|RECOMMENDED|OPTIONAL)\b")


def resolve_ref(number, anchors):
    """The closest numbered anchor at or above number ("19.2.3" -> "19.2"), or None."""
    while number:
        if number in anchors:
            return number
        number = number.rpartition(".")[0]
    return None


class SpecRenderer(Renderer):
    """The renderer for the spec page.

    Adds anchors for numbered headings and paragraphs (id="s4.3"), links every
    §-reference to its anchor, marks RFC 2119 keywords, styles blockquotes as
    notes and gives each heading a ¶ link.  anchors starts with the numbers
    passed in and gains every number found in the rendered text.
    """

    quote_class = ' class="note"'

    def __init__(self, anchors=None):
        super().__init__()
        self.anchors = set(anchors or ())

    def prepare(self, nodes):
        for node in nodes:
            if node[0] == "heading":
                number = heading_number(node[2])
            elif node[0] == "para":
                m = _PARA_NUMBER.match(node[1])
                number = m.group(1) if m else None
            else:
                number = None
            if number:
                self.anchors.add(number)

    def heading_inner(self, text, info):
        anchor = ""
        if info.number and "s" + info.number not in self.ids:
            self.ids.add("s" + info.number)
            anchor = '<span class="anchor" id="s%s"></span>' % info.number
        return '%s%s <a class="pilcrow" href="#%s" aria-label="Link to this section">¶</a>' % (
            anchor, self.inline(text), info.id)

    def para(self, text):
        m = _PARA_NUMBER.match(text)
        if m and "s" + m.group(1) not in self.ids:
            number = m.group(1)
            self.ids.add("s" + number)
            return '<p id="s%s"><span class="num">%s</span> %s</p>\n' % (
                number, number, self.inline(text[m.end():]))
        return super().para(text)

    def decorate(self, text):
        text = _SECTION_REF.sub(self.section_link, text)
        return _RFC.sub(lambda m: '<span class="rfc">%s</span>' % m.group(1), text)

    def section_link(self, m):
        target = resolve_ref(m.group(1), self.anchors)
        if not target:
            return m.group(0)
        return '<a class="sref" href="#s%s">%s</a>' % (target, m.group(0))


def render(text, spec=False, anchors=None):
    """Render markdown; returns (blocks, anchors).

    spec=True uses SpecRenderer; anchors seeds the section numbers that
    §-references may link to (the spec's, when rendering the glance box).
    """
    renderer = SpecRenderer(anchors) if spec else Renderer()
    blocks = renderer.render(text)
    return blocks, getattr(renderer, "anchors", set())
````

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m pytest website/tsvz_site_test.py -q && ruff check --select E,F,W --line-length 120 website/`
Expected: `22 passed`; `All checks passed!`. `test_whole_spec_renders_cleanly` proves that all of the spec's §-references outside code are links to anchors that exist, and that the tags are balanced.

- [ ] **Step 5: Commit**

```bash
git add website/tsvz_site.py website/tsvz_site_test.py
git commit -F - <<'MSG'
Render the spec with section anchors, § links, RFC 2119 keywords and note callouts.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
MSG
```

---

### Task 3: Content, stylesheet and page templates

**Files:**
- Create: `website/index.md`, `website/spec-glance.md`, `website/style.css`
- Modify: `website/tsvz_site.py` (append the "Pages" section)
- Modify: `website/tsvz_site_test.py` (imports; append the Task 3 group)
- Modify: `docs/superpowers/specs/2026-10-10-tsvz-website-design.md` (deviations 2–6)

**Interfaces:**
- Consumes (Tasks 1–2):
  - functions: `render`, `escape`, `plain_text`;
  - data: `Block`, `Heading`, the spec `anchors`.
- Produces:
  - `landing_main(blocks) -> str` and `spec_main(blocks, glance_html, date) -> str`, each returning `<main>` content;
  - `page(*, css, title, description, canonical, md_url, current, main, body_class, md_link_text) -> str`, a whole HTML document. `canonical=None` drops the canonical and Open Graph tags; `md_url=None` drops the markdown links.
  - `SCRIPT`, whose hash Task 4 puts in the CSP, and the URL and title constants.

- [ ] **Step 1: Add the content files**

Create `website/index.md` (Appendix A of the design, with deviation 5's shorter Quick start lines):

`````markdown
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
`````

Create `website/spec-glance.md`:

````markdown
- One record per line; a line counts only once its `\n` is written. §4.3
- Field 0 is the key; the last line for a key wins. §3.3
- A key alone, with no delimiter, deletes it. §9
- `#` lines are comments; `#_name_#` lines are markers. §11, §12
- `<sep>`, `<LF>`, `<lt>` and `<#>` stand for the delimiter, a newline, `<`
  and `#`. §13
- The extension picks the delimiter: `.tsvz` tab, `.csvz` comma, `.nsvz` NUL,
  `.psvz` pipe. Plain `.tsv` and `.csv` files are loose tables. §5
````

Create `website/style.css`:

````css
/* tsvz.org: one stylesheet, inlined into every page by tsvz_site.py. */

:root {
  --bg: #faf9f6;
  --surface: #ffffff;
  --line: #e7e3d9;
  --ink: #1b1b1a;
  --muted: #57534b;
  --faint: #6b675e;
  --accent: #0f766e;
  --on-accent: #ffffff;
  --accent-soft: #e3f1ee;
  --marker: #b45309;
  --delete: #b91c1c;
  --rfc: #9a3412;
  --inline-code: #efece4;
  --note-bg: #f1efe8;
  --code-bg: #1f1f1d;
  --code-fg: #e9e6df;
  --code-dim: #9a958b;
  --code-prompt: #5eead4;
  --sans: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --mono: ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  color-scheme: light dark;
}

@media (prefers-color-scheme: dark) {
  :root {
    --bg: #161513;
    --surface: #1f1e1b;
    --line: #34322d;
    --ink: #ece9e2;
    --muted: #bdb8ad;
    --faint: #a39d91;
    --accent: #2dd4bf;
    --on-accent: #04201c;
    --accent-soft: #123a35;
    --marker: #f2a33a;
    --delete: #f87171;
    --rfc: #fb923c;
    --inline-code: #2a2925;
    --note-bg: #24231f;
  }
}

/* Base */

*, *::before, *::after { box-sizing: border-box; }
html { -webkit-text-size-adjust: 100%; text-size-adjust: 100%; }
body { margin: 0; background: var(--bg); color: var(--ink); font: 16px/1.6 var(--sans); }
a { color: var(--accent); text-underline-offset: 2px; }
a:focus-visible, button:focus-visible, summary:focus-visible {
  outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 3px;
}
code { font-family: var(--mono); font-size: .875em; background: var(--inline-code); padding: .1em .35em; border-radius: 4px; }
h1, h2, h3 { line-height: 1.2; letter-spacing: -.01em; }
.skip { position: absolute; left: -9999px; top: 8px; z-index: 10; background: var(--surface); color: var(--ink); padding: 6px 12px; border: 1px solid var(--line); border-radius: 6px; }
.skip:focus { left: 8px; }

/* Site header and footer */

.site-head { border-bottom: 1px solid var(--line); }
.site-head nav { max-width: 1100px; margin: 0 auto; padding: 12px 16px; display: flex; align-items: center; justify-content: space-between; gap: 16px; }
.brand { font: 700 17px var(--mono); color: var(--ink); text-decoration: none; }
.nav-links { display: flex; gap: 18px; font-size: 15px; }
.nav-links a { color: var(--muted); text-decoration: none; }
.nav-links a:hover, .nav-links a[aria-current] { color: var(--ink); }
.nav-links a[aria-current] { font-weight: 600; }
.nav-impl { display: none; }
.site-foot { max-width: 1100px; margin: 0 auto; padding: 28px 16px 40px; font-size: .875rem; color: var(--faint); }
.site-foot p { margin: 0 0 6px; }
.site-foot a { color: var(--muted); }

/* Landing page */

.page-landing main { max-width: 1100px; margin: 0 auto; padding: 0 16px; }
.page-landing section { padding: 40px 0; border-bottom: 1px solid var(--line); }
.page-landing section h2 { font-size: 1.5rem; margin: 0 0 20px; }
.page-landing section > p { max-width: 70ch; color: var(--muted); }
.hero { display: grid; gap: 28px; }
.hero h1 { font-size: clamp(2rem, 7vw, 2.6rem); line-height: 1.1; letter-spacing: -.02em; margin: 0 0 16px; font-weight: 800; text-wrap: balance; }
.hero-text > p:first-of-type { font-size: 1.125rem; color: var(--muted); margin: 0 0 24px; max-width: 34em; }
.actions { display: flex; flex-wrap: wrap; gap: 10px; margin: 0 0 16px; }
.actions a { flex: 1 1 auto; text-align: center; padding: 10px 18px; border: 1px solid var(--line); border-radius: 8px; color: var(--ink); font-weight: 600; text-decoration: none; background: var(--surface); }
.actions a:first-child { background: var(--accent); border-color: var(--accent); color: var(--on-accent); }
.actions + p { margin: 0; font-size: .9rem; color: var(--faint); }
.hero-art > p { margin: 12px 2px 0; font-size: .9rem; color: var(--muted); }

.file { margin: 0; background: var(--surface); border: 1px solid var(--line); border-radius: 10px; overflow: hidden; }
.file figcaption { padding: 8px 14px; border-bottom: 1px solid var(--line); font: 13px var(--mono); color: var(--faint); }
.file ol { list-style: none; margin: 0; padding: 10px 0; counter-reset: line; }
.file li { counter-increment: line; display: grid; grid-template-columns: 2.6em minmax(0, 1fr); padding-right: 14px; font: 14px/1.8 var(--mono); }
.file li::before { content: counter(line); color: var(--faint); text-align: right; padding-right: 12px; }
.file li code { background: none; padding: 0; font-size: inherit; white-space: pre; overflow-x: auto; }
.file .note { grid-column: 2; margin: -4px 0 4px; font: 12.5px/1.5 var(--sans); color: var(--accent); }
.file .cm code { color: var(--faint); }
.file .mk code { color: var(--marker); }
.file .del code { color: var(--delete); text-decoration: line-through; }

.sec-why-tsvz ul { list-style: none; margin: 0; padding: 0; display: grid; gap: 14px; }
.sec-why-tsvz li { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 16px 18px; color: var(--muted); }
.sec-why-tsvz li > strong:first-child { display: block; margin-bottom: 4px; color: var(--ink); font-size: 1.05rem; }

.sec-how-it-works ol { list-style: none; margin: 0; padding: 0; display: grid; gap: 18px; counter-reset: step; }
.sec-how-it-works li { counter-increment: step; color: var(--muted); }
.sec-how-it-works li > strong:first-child { display: flex; align-items: center; gap: 10px; margin-bottom: 4px; color: var(--ink); font-size: 1.05rem; }
.sec-how-it-works li > strong:first-child::before { content: counter(step); display: inline-grid; place-items: center; width: 26px; height: 26px; border-radius: 50%; background: var(--accent-soft); color: var(--accent); font-size: .85rem; }

.subs { display: grid; gap: 4px 20px; }
.sub { min-width: 0; }
.sub h3 { margin: 0 0 8px; font-size: .8rem; text-transform: uppercase; letter-spacing: .06em; color: var(--faint); }

.sec-implementations table { background: var(--surface); }
.sec-implementations th { font-size: .8rem; text-transform: uppercase; letter-spacing: .04em; color: var(--faint); }
.sec-when-tsvz-isnt-the-right-fit ul { margin: 0; padding-left: 1.2em; color: var(--muted); }
.sec-when-tsvz-isnt-the-right-fit li { break-inside: avoid; margin-bottom: 4px; }
.sec-when-tsvz-isnt-the-right-fit li strong { color: var(--ink); }
.notfound { padding: 64px 0; }

/* Code blocks and tables (both pages) */

.code { position: relative; margin: 0 0 16px; }
.code pre { margin: 0; padding: 14px 16px; overflow-x: auto; background: var(--code-bg); color: var(--code-fg); border: 1px solid var(--line); border-radius: 10px; font: 13.5px/1.65 var(--mono); }
.code pre code { background: none; padding: 0; font-size: inherit; color: inherit; }
.code .prompt { color: var(--code-prompt); user-select: none; }
.code .c { color: var(--code-dim); }
.copy { position: absolute; top: 8px; right: 8px; padding: 3px 10px; border: 1px solid #45433e; border-radius: 6px; background: #2b2a27; color: var(--code-fg); font: 12px var(--sans); cursor: pointer; opacity: 0; }
.code:hover .copy, .copy:focus-visible { opacity: 1; }
@media (hover: none) { .copy { opacity: 1; } }
.table-wrap { overflow-x: auto; margin: 0 0 16px; }
table { border-collapse: collapse; width: 100%; font-size: .94rem; }
th, td { text-align: left; vertical-align: top; padding: 8px 12px; border: 1px solid var(--line); }
th { font-weight: 600; background: var(--bg); }

/* Spec page */

.doc-head { max-width: 1100px; margin: 0 auto; padding: 28px 16px 18px; border-bottom: 1px solid var(--line); }
.doc-head h1 { margin: 0 0 6px; font-size: clamp(1.6rem, 5vw, 2rem); }
.doc-head .meta { margin: 0; font-size: .9rem; color: var(--faint); }
.doc-layout { max-width: 1100px; margin: 0 auto; padding: 24px 16px 48px; }
.toc { display: none; }
.toc ul, .toc-mobile ul { list-style: none; margin: 0; padding: 0; }
.toc li > ul { display: none; padding-left: 12px; }
.toc li.open > ul { display: block; }
.toc a { display: block; padding: 3px 0 3px 10px; border-left: 2px solid transparent; color: var(--muted); text-decoration: none; }
.toc a:hover { color: var(--ink); }
.toc a.on { border-left-color: var(--accent); color: var(--accent); font-weight: 600; }
.toc-title { margin: 0 0 8px 12px; font-size: .72rem; text-transform: uppercase; letter-spacing: .08em; color: var(--faint); }
.toc-mobile { margin: 0 0 20px; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; }
.toc-mobile summary { padding: 10px 14px; font-weight: 600; cursor: pointer; }
.toc-mobile ul { padding: 0 14px 12px; font-size: .92rem; line-height: 1.8; }
.toc-mobile a { text-decoration: none; }
.doc { min-width: 0; max-width: 72ch; }
.doc [id] { scroll-margin-top: 16px; }
.glance { margin: 0 0 28px; padding: 14px 18px; background: var(--surface); border: 1px solid var(--line); border-left: 4px solid var(--accent); border-radius: 8px; }
.glance-title { margin: 0 0 6px; font-weight: 700; }
.glance ul { margin: 0; padding-left: 1.2em; color: var(--muted); }
.doc h2 { margin: 2.2em 0 .7em; padding-bottom: .3em; border-bottom: 1px solid var(--line); font-size: 1.4rem; }
.doc h3 { margin: 1.8em 0 .6em; font-size: 1.1rem; }
.doc p { margin: 0 0 1em; }
.doc ul, .doc ol { padding-left: 1.5em; }
.doc li + li { margin-top: .25em; }
.doc hr { border: 0; border-top: 1px solid var(--line); margin: 2em 0; }
.num { margin-right: .2em; font: .82em var(--mono); color: var(--faint); }
.rfc { font-size: .86em; font-weight: 700; letter-spacing: .03em; color: var(--rfc); }
.sref { text-decoration: none; }
.sref:hover { text-decoration: underline; }
.pilcrow { margin-left: .3em; color: var(--faint); font-weight: 400; text-decoration: none; opacity: 0; }
h2:hover .pilcrow, h3:hover .pilcrow, .pilcrow:focus { opacity: 1; }
@media (hover: none) { .pilcrow { opacity: .6; } }
blockquote { margin: 1em 0; padding: 10px 14px; background: var(--note-bg); border-radius: 8px; color: var(--muted); }
blockquote > :last-child { margin-bottom: 0; }

/* Screen sizes */

@media (max-width: 759.98px) {
  .sec-implementations thead { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); }
  .sec-implementations table, .sec-implementations tbody, .sec-implementations tr, .sec-implementations td { display: block; }
  .sec-implementations tr { padding: 10px 14px; border: 1px solid var(--line); border-radius: 10px; background: var(--surface); }
  .sec-implementations td { display: flex; gap: 12px; padding: 3px 0; border: 0; }
  .sec-implementations td::before { content: attr(data-label); flex: 0 0 7.5em; color: var(--faint); font-size: .85rem; }
  .sec-implementations table { background: none; }
}

@media (min-width: 760px) {
  .site-head nav { padding: 14px 24px; }
  .nav-impl { display: inline; }
  .page-landing main { padding: 0 24px; }
  .site-foot { padding: 28px 24px 40px; }
  .actions a { flex: 0 0 auto; }
  .file li { grid-template-columns: 2.6em minmax(0, 1fr) auto; }
  .file .note { grid-column: 3; align-self: center; margin: 0; padding-left: 14px; text-align: right; }
  .sec-why-tsvz ul { grid-template-columns: 1fr 1fr; }
  .sec-how-it-works ol { grid-template-columns: repeat(3, 1fr); gap: 28px; }
  .sec-when-tsvz-isnt-the-right-fit ul { columns: 2; column-gap: 40px; }
  .doc-head { padding: 32px 24px 20px; }
}

@media (min-width: 1000px) {
  .hero { grid-template-columns: minmax(0, 1fr) minmax(0, 1.05fr); align-items: center; gap: 40px; padding: 64px 0; }
  .subs { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); }
  .doc-layout { display: grid; grid-template-columns: 220px minmax(0, 1fr); gap: 48px; padding: 32px 24px 64px; }
  .toc { display: block; position: sticky; top: 16px; align-self: start; max-height: calc(100vh - 32px); overflow-y: auto; font-size: .84rem; line-height: 1.45; }
  .toc-mobile { display: none; }
}

@media print {
  .site-head, .toc, .toc-mobile, .copy, .skip, .pilcrow { display: none !important; }
  body { background: #fff; color: #000; font-size: 11pt; }
  .doc-layout { display: block; padding: 0; }
  .doc { max-width: none; }
  .code pre { background: #f5f5f5; color: #000; border-color: #ccc; white-space: pre-wrap; }
  a { color: inherit; }
}
````

- [ ] **Step 2: Write the failing tests**

In `website/tsvz_site_test.py`, make the import lines at the top read:

````python
import os
import re
import shlex
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path
````

Append to `website/tsvz_site_test.py`:

````python
# -- Task 3: content, stylesheet and page templates ---------------------------

INDEX_MD = (HERE / "index.md").read_text(encoding="utf-8")
CSS = (HERE / "style.css").read_text(encoding="utf-8").strip()


def landing_html():
    blocks, _ = tsvz_site.render(INDEX_MD)
    return tsvz_site.page(css=CSS, title=tsvz_site.LANDING_TITLE, description="d",
                          canonical="https://tsvz.org/", md_url="/index.md", current="home",
                          main=tsvz_site.landing_main(blocks), body_class="page-landing",
                          md_link_text="This page as markdown")


def test_landing_sections():
    blocks, _ = tsvz_site.render(INDEX_MD)
    main = tsvz_site.landing_main(blocks)
    hero = main.split("</section>", 1)[0]
    assert hero.startswith('<section class="hero">\n<div class="hero-text">\n<h1')
    text, art = hero.split('<div class="hero-art">')
    assert '<p class="actions">' in text and "pip install tsvz" in text
    assert art.lstrip().startswith('<figure class="file">') and "Current state" in art
    for cls in ("why-tsvz", "how-it-works", "quick-start", "implementations",
                "when-tsvz-isnt-the-right-fit"):
        assert '<section class="sec-%s">' % cls in main
    quick = main.split('<section class="sec-quick-start">', 1)[1].split("</section>", 1)[0]
    assert quick.count('<div class="sub">') == 2 and '<div class="subs">' in quick
    assert_balanced(main)


def test_every_css_section_class_exists():
    page = landing_html()
    for cls in set(re.findall(r"\.(sec-[a-z0-9-]+)", CSS)):
        assert 'class="%s"' % cls in page, cls


def test_landing_page_template():
    out = landing_html()
    assert out.startswith('<!doctype html>\n<html lang="en">')
    assert "<title>TSVZ — an append-only CSV that's also a key-value store</title>" in out
    assert '<link rel="canonical" href="https://tsvz.org/">' in out
    assert '<link rel="alternate" type="text/markdown" href="/index.md">' in out
    assert '<meta property="og:url" content="https://tsvz.org/">' in out
    assert '<a class="skip" href="#main">Skip to content</a>' in out
    assert '<a href="/spec">Spec</a>' in out and 'class="nav-impl" href="/#implementations"' in out
    assert "<style>%s</style>" % CSS in out and "<script>%s</script>" % tsvz_site.SCRIPT in out
    assert 'CC BY 4.0</a>, anyone may implement TSVZ under any license' in out
    assert '<a href="/index.md">This page as markdown</a>' in out
    assert_balanced(out)


def test_landing_size_budget():
    assert len(landing_html().encode("utf-8")) <= 30 * 1024


def test_spec_page_parts():
    blocks, anchors = tsvz_site.render(SPEC, spec=True)
    glance_blocks, _ = tsvz_site.render((HERE / "spec-glance.md").read_text(encoding="utf-8"),
                                        spec=True, anchors=anchors)
    main = tsvz_site.spec_main(blocks, "".join(b.html for b in glance_blocks), "2026-10-09")
    assert '<header class="doc-head">\n<h1>TSVZ — Format Specification</h1>' in main
    assert '<p class="meta">Version 1 · Draft · updated 2026-10-09</p>' in main
    assert "**Version:**" not in main and "<strong>Version:</strong>" not in main
    assert main.count("<h1") == 1
    toc = main.split('<nav class="toc" id="toc" aria-label="Contents">', 1)[1].split("</nav>", 1)[0]
    assert '<li><a href="#at-a-glance">At a glance</a></li>' in toc
    assert '<a href="#20-command-line-interface">20. Command-line interface</a><ul>' in toc
    assert '<li><a href="#201-invocation">20.1 Invocation</a></li>' in toc
    mobile = main.split('<details class="toc-mobile">', 1)[1].split("</details>", 1)[0]
    assert '<a href="#20-command-line-interface">' in mobile and "201-invocation" not in mobile
    assert '<aside class="glance" id="at-a-glance">' in main
    assert '<a class="sref" href="#s4.3">§4.3</a>' in main.split("</aside>", 1)[0]
    assert_balanced(main)


def test_spec_page_without_date_or_meta():
    blocks, _ = tsvz_site.render("# Title\n\nTagline.\n\n## 1. One\n", spec=True)
    main = tsvz_site.spec_main(blocks, "", None)
    assert '<p class="meta"></p>' in main and "<h1>Title</h1>" in main


def _quick_start_blocks():
    blocks, _ = tsvz_site.render(INDEX_MD)
    console = next(b.raw for b in blocks if 'class="language-console"' in b.html)
    python = next(b.raw for b in blocks if 'class="language-python"' in b.html)
    hero = next(b.raw for b in blocks if b.kind == "file")
    return console, python, hero


def test_quick_start_python_writes_the_hero_file(tmp_path):
    _, python, hero = _quick_start_blocks()
    env = dict(os.environ, PYTHONPATH=str(REPO))
    run = subprocess.run([sys.executable, "-c", python], cwd=str(tmp_path), env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    assert run.returncode == 0, run.stderr
    # TSVZed also prints "Created people.csvz" first, as 3.39 did.
    assert run.stdout.decode().splitlines()[-1] == "['alice', 'Alice', '31']"
    expected = "\n".join(re.sub(r"\s{2,}← .*$", "", line).rstrip() for line in hero.split("\n")) + "\n"
    assert (tmp_path / "people.csvz").read_text(encoding="utf-8") == expected


def test_quick_start_console_shows_real_output(tmp_path):
    console, _, _ = _quick_start_blocks()
    steps, current = [], None
    for line in console.split("\n"):
        if line.startswith("$ "):
            current = [line[2:], []]
            steps.append(current)
        else:
            current[1].append(line)
    ran = 0
    for command, shown in steps:
        argv = shlex.split(command, comments=True)
        if argv[:2] == ["pip", "install"]:
            continue
        if argv[0] == "tsvz":
            argv = [sys.executable, str(REPO / "TSVZ.py")] + argv[1:]
        run = subprocess.run(argv, cwd=str(tmp_path), stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, timeout=30)
        assert run.returncode == 0, (command, run.stdout)
        output = run.stdout.decode().splitlines()
        if command.startswith("tsvz get"):
            assert output == ["alice,Alice,31"] and "alice, Alice, 31" in command
        else:
            assert output == shown, command
        ran += 1
    assert ran == 6
````

- [ ] **Step 3: Run the tests to verify they fail**

Run: `python3 -m pytest website/tsvz_site_test.py -q`
Expected: failures with `AttributeError: module 'tsvz_site' has no attribute 'landing_main'` (and `'page'`, `'spec_main'`). The two Quick start tests already pass, because they only run `index.md`'s code against `../TSVZ.py`.

- [ ] **Step 4: Write the page templates**

Append to `website/tsvz_site.py`, after two blank lines:

````python
# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

SCRIPT = """(function () {
  var d = document;
  if (navigator.clipboard) {
    d.querySelectorAll('.code').forEach(function (box) {
      var code = box.querySelector('code');
      var b = d.createElement('button');
      b.type = 'button';
      b.className = 'copy';
      b.textContent = 'Copy';
      b.addEventListener('click', function () {
        var cmds = code.querySelectorAll('.cmd');
        var text = cmds.length ? Array.prototype.map.call(cmds, function (c) {
          return c.textContent;
        }).join('\\n') : code.textContent;
        navigator.clipboard.writeText(text).then(function () {
          b.textContent = 'Copied';
          setTimeout(function () { b.textContent = 'Copy'; }, 1500);
        });
      });
      box.appendChild(b);
    });
  }
  var toc = d.getElementById('toc');
  if (!toc || !window.IntersectionObserver) return;
  var links = {}, current = null;
  toc.querySelectorAll('a').forEach(function (a) { links[a.hash.slice(1)] = a; });
  function mark(id) {
    var a = links[id];
    if (!a || a === current) return;
    if (current) current.classList.remove('on');
    toc.querySelectorAll('li.open').forEach(function (li) { li.classList.remove('open'); });
    a.classList.add('on');
    current = a;
    for (var el = a.parentNode; el && el !== toc; el = el.parentNode) {
      if (el.tagName === 'LI') el.classList.add('open');
    }
  }
  var seen = new IntersectionObserver(function (entries) {
    entries.forEach(function (e) { if (e.isIntersecting) mark(e.target.id); });
  }, { rootMargin: '0px 0px -75% 0px' });
  d.querySelectorAll('#at-a-glance, .doc h2[id], .doc h3[id]').forEach(function (h) {
    if (links[h.id]) seen.observe(h);
  });
})();
"""

FAVICON = ("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E"
           "%3Crect width='32' height='32' rx='7' fill='%230f766e'/%3E"
           "%3Ctext x='16' y='23' font-family='ui-monospace,Menlo,monospace' font-size='20' "
           "font-weight='700' fill='white' text-anchor='middle'%3Ez%3C/text%3E%3C/svg%3E")

GITHUB_URL = "https://github.com/yufei-pan/TSVZ"
PYPI_URL = "https://pypi.org/project/TSVZ/"
CC_BY_URL = "https://creativecommons.org/licenses/by/4.0/"
LANDING_TITLE = "TSVZ — an append-only CSV that's also a key-value store"
SPEC_TITLE = "TSVZ Format Specification (v1)"


def landing_main(blocks):
    """The landing page's <main> content from the rendered blocks of index.md."""
    groups, current = [], (None, [])
    for block in blocks:
        if block.kind == "h2":
            groups.append(current)
            current = (block, [])
        else:
            current[1].append(block)
    groups.append(current)
    hero = groups[0][1]
    split = next((k for k, b in enumerate(hero) if b.kind == "file"), len(hero))
    out = ['<section class="hero">\n<div class="hero-text">\n']
    out.extend(b.html for b in hero[:split])
    out.append('</div>\n<div class="hero-art">\n')
    out.extend(b.html for b in hero[split:])
    out.append("</div>\n</section>\n")
    for heading, body in groups[1:]:
        out.append('<section class="sec-%s">\n' % heading.heading.id)
        out.append(heading.html)
        first_sub = next((k for k, b in enumerate(body) if b.kind == "h3"), len(body))
        out.extend(b.html for b in body[:first_sub])
        if first_sub < len(body):
            out.append('<div class="subs">\n')
            for k, b in enumerate(body[first_sub:]):
                if b.kind == "h3":
                    out.append("</div>\n" if k else "")
                    out.append('<div class="sub">\n')
                out.append(b.html)
            out.append("</div>\n</div>\n")
        out.append("</section>\n")
    return "".join(out)


def _toc_items(headings, nested):
    out, open_h2 = [], False
    for h in headings:
        if h.level == 2:
            if open_h2:
                out.append("</ul></li>\n" if nested == "sub" else "</li>\n")
            out.append('<li><a href="#%s">%s</a>' % (h.id, escape(h.text)))
            open_h2 = True
            if nested == "sub":
                out.append("<ul>")
        elif h.level == 3 and nested == "sub":
            out.append('<li><a href="#%s">%s</a></li>' % (h.id, escape(h.text)))
    if open_h2:
        out.append("</ul></li>\n" if nested == "sub" else "</li>\n")
    return "".join(out).replace("<ul></ul>", "")


def spec_main(blocks, glance_html, date):
    """The spec page's <main> content: header, contents, glance box and body."""
    title = next(b.heading.text for b in blocks if b.kind == "h1")
    meta = next((b for b in blocks if b.kind == "para" and b.raw.startswith("**Version:**")), None)
    version = status = None
    if meta:
        m = re.search(r"\*\*Version:\*\*\s*(\S+)", meta.raw)
        version = m.group(1) if m else None
        m = re.search(r"\*\*Status:\*\*\s*(\S+)", meta.raw)
        status = m.group(1) if m else None
    body = [b for b in blocks if b.kind != "h1" and b is not meta]
    headings = [b.heading for b in body if b.heading and b.heading.level in (2, 3)]
    facts = []
    if version:
        facts.append("Version %s" % escape(version))
    if status:
        facts.append(escape(status))
    if date:
        facts.append("updated %s" % date)
    glance_item = '<li><a href="#at-a-glance">At a glance</a></li>\n'
    return "".join([
        '<header class="doc-head">\n<h1>%s</h1>\n' % escape(title),
        '<p class="meta">%s</p>\n' % " · ".join(facts),
        '<p class="meta"><a href="/spec.md">Markdown</a> · <a href="%s">CC BY 4.0</a> · '
        "anyone may implement</p>\n</header>\n" % CC_BY_URL,
        '<div class="doc-layout">\n',
        '<nav class="toc" id="toc" aria-label="Contents">\n<p class="toc-title">Contents</p>\n<ul>\n',
        glance_item, _toc_items(headings, "sub"), "</ul>\n</nav>\n",
        '<article class="doc">\n',
        '<details class="toc-mobile"><summary>Contents</summary>\n<ul>\n',
        glance_item, _toc_items(headings, "flat"), "</ul>\n</details>\n",
        '<aside class="glance" id="at-a-glance">\n<p class="glance-title">At a glance</p>\n',
        glance_html, "</aside>\n",
        "".join(b.html for b in body),
        "</article>\n</div>\n",
    ])


NOT_FOUND_MAIN = ('<section class="notfound">\n<h1>No page here</h1>\n'
                  '<p><a href="/">Home</a> · <a href="/spec">The specification</a></p>\n</section>\n')


def page(*, css, title, description, canonical, md_url, current, main, body_class, md_link_text):
    """A complete HTML page in the site template."""
    def nav_link(href, label, name, extra=""):
        here = ' aria-current="page"' if name == current else ""
        return '<a%s href="%s"%s>%s</a>' % (extra, href, here, label)

    head = [
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n",
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n',
        '<meta name="color-scheme" content="light dark">\n',
        "<title>%s</title>\n" % escape(title),
        '<meta name="description" content="%s">\n' % escape(description),
    ]
    if canonical:
        head += [
            '<link rel="canonical" href="%s">\n' % canonical,
            '<meta property="og:type" content="website">\n',
            '<meta property="og:site_name" content="TSVZ">\n',
            '<meta property="og:title" content="%s">\n' % escape(title),
            '<meta property="og:description" content="%s">\n' % escape(description),
            '<meta property="og:url" content="%s">\n' % canonical,
            '<meta name="twitter:card" content="summary">\n',
        ]
    if md_url:
        head.append('<link rel="alternate" type="text/markdown" href="%s">\n' % md_url)
    head += ['<link rel="icon" href="%s">\n' % FAVICON, "<style>%s</style>\n" % css, "</head>\n"]
    footer_links = ['<a href="%s">GitHub</a>' % GITHUB_URL, '<a href="%s">PyPI</a>' % PYPI_URL]
    if md_url:
        footer_links.append('<a href="%s">%s</a>' % (md_url, md_link_text))
    footer_links.append('<a href="/llms.txt">llms.txt</a>')
    body = [
        '<body class="%s">\n<a class="skip" href="#main">Skip to content</a>\n' % body_class,
        '<header class="site-head"><nav aria-label="Site">',
        '<a class="brand" href="/">tsvz</a><span class="nav-links">',
        nav_link("/spec", "Spec", "spec"),
        nav_link("/#implementations", "Implementations", "", ' class="nav-impl"'),
        nav_link(GITHUB_URL, "GitHub", ""),
        "</span></nav></header>\n",
        '<main id="main">\n', main, "</main>\n",
        '<footer class="site-foot">\n',
        '<p>Spec v1 (draft) · <a href="%s">CC BY 4.0</a>, anyone may implement TSVZ under any license'
        " · Python implementation: GPL-3.0-or-later</p>\n" % CC_BY_URL,
        "<p>%s</p>\n</footer>\n" % " · ".join(footer_links),
        "<script>%s</script>\n</body>\n</html>\n" % SCRIPT,
    ]
    return "".join(head + body)
````

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python3 -m pytest website/tsvz_site_test.py -q && ruff check --select E,F,W --line-length 120 website/`
Expected: `30 passed`; `All checks passed!`

The Quick start tests run the Python block and every `tsvz` command of the console block against `../TSVZ.py`:
- `TSVZed` prints `Created people.csvz` before the row, as 3.39 did. The test checks the last line.
- The `tsvz get` line must print `alice,Alice,31`, the row its comment names.

- [ ] **Step 6: Record deviations 2–6 in the design doc**

In `docs/superpowers/specs/2026-10-10-tsvz-website-design.md`, make these replacements (each text occurs once):

1. Replace:

   ````text
   The content is `index.md` (Appendix A). Each `##` section is wrapped in
   `<section id="<heading id>">`, and everything before the first `##` is wrapped
   in `<section id="top" class="hero">`. The layout comes from `style.css`
   selecting on those ids. A test checks that every id the CSS uses exists.
   ````

   with:

   ````text
   The content is `index.md` (Appendix A). Each `##` section is wrapped in
   `<section class="sec-<heading id>">`, and the `<h2>` inside keeps the id, so
   `/#implementations` lands on the heading. Everything before the first `##` is
   wrapped in `<section class="hero">`, split into `<div class="hero-text">` and
   `<div class="hero-art">` at the annotated file. Within a section, each `###`
   starts a `<div class="sub">`, and the subs share one `<div class="subs">`. The
   layout comes from `style.css` selecting on those classes. A test checks that
   every `sec-` class the CSS uses exists.
   ````

2. Replace:

   ````text
   | ⑦ | Footer (template) | One row | Wraps |
   ````

   with:

   ````text
   | ⑦ | Footer (template) | One row | Wraps |

   The hero and the Quick start split into two columns from 1000px, not 760px;
   between 760px and 1000px they stack. At 768px a split hero squeezed the
   headline into five lines.
   ````

3. Replace:

   ````text
   | `--code-bg`, `--code-fg`, `--code-dim`, `--code-prompt` |
   ````

   with:

   ````text
   | `--accent-soft` | `#e3f1ee` | `#123a35` | step-number circles (accent on it: 4.7 / 6.7) |
   | `--rfc` | `#9a3412` (6.9) | `#fb923c` (8.1) | MUST, SHOULD, MAY in the spec |
   | `--inline-code` | `#efece4` | `#2a2925` | inline code background (ink on it: 14.6 / 12.0) |
   | `--note-bg` | `#f1efe8` | `#24231f` | note callouts (muted on it: 6.7 / 8.0) |
   | `--code-bg`, `--code-fg`, `--code-dim`, `--code-prompt` |
   ````

4. Replace:

   ````text
   - **760px and wider.** The hero splits; cards and code blocks sit side by
     side.
   - **1000px and wider.** The spec's sticky contents sidebar appears.
   ````

   with:

   ````text
   - **760px and wider.** Cards, steps and file notes sit side by side.
   - **1000px and wider.** The hero and the Quick start split into two columns,
     and the spec's sticky contents sidebar appears.
   ````

5. Replace:

   ````text
   There are two inline scripts, each under 1 KB, and both are optional:
   ````

   with:

   ````text
   One inline script, about 1.6 KB, does two optional things:
   ````

6. Replace:

   ````text
     - Every section id that `style.css` selects exists.
   ````

   with:

   ````text
     - Every `sec-` class that `style.css` selects exists.
   ````

7. Replace:

   ````text
   $ tsvz get people.csvz alice      # the current row: alice, Alice, 31
   ````

   with:

   ````text
   $ tsvz get people.csvz alice    # → alice, Alice, 31
   ````

8. Replace:

   ````text
   t = TSVZ.TSVZed('people.csvz', header='id,name,score')
   t['alice'] = ['alice', 'Alice', '30']
   t['bob'] = ['bob', 'Bob', '25']
   t['alice'] = ['alice', 'Alice', '31']   # last write wins
   del t['bob']                            # appends "bob": a key alone
   print(t['alice'])                       # ['alice', 'Alice', '31']
   ````

   with:

   ````text
   t = TSVZ.TSVZed('people.csvz',
                   header='id,name,score')
   t['alice'] = ['alice', 'Alice', '30']
   t['bob'] = ['bob', 'Bob', '25']
   t['alice'] = ['alice', 'Alice', '31']
   del t['bob']       # appends "bob": a key alone
   print(t['alice'])  # ['alice', 'Alice', '31']
   ````

- [ ] **Step 7: Commit**

```bash
git add website/ docs/superpowers/specs/2026-10-10-tsvz-website-design.md
git commit -F - <<'MSG'
Add the tsvz.org landing copy, stylesheet and page templates.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
MSG
```

---

### Task 4: The site's bodies, agent files and `build`

**Files:**
- Modify: `website/tsvz_site.py` (imports; append the "Site" section and `main`)
- Modify: `website/tsvz_site_test.py` (imports; append the Task 4 group)

**Interfaces:**
- Consumes (Tasks 1–3):
  - rendering: `render`, `split_row`, `resolve_ref`, `_SECTION_REF`, `plain_text`;
  - pages: `landing_main`, `spec_main`, `page`, `NOT_FOUND_MAIN`;
  - constants: `SCRIPT`, `LANDING_TITLE`, `SPEC_TITLE`, `GITHUB_URL`.
- Produces:
  - `Site(base_url=DEFAULT_BASE_URL, spec_path=DEFAULT_SPEC, root=HERE)`. Its `.bodies` is keyed by `index.html`, `spec.html`, `404.html`, `index.md`, `spec.md`, `llms.txt`, `llms-full.txt`, `robots.txt`, `sitemap.xml` and `404.txt`.
  - `Body(data, gz, ctype, etag, disposition, link)`.
  - `Site.csp`, `Site.build(outdir) -> list[Path]`, `BUILD_FILES`.
  - `SiteError`, and `main(argv)`, which Task 5 replaces.

- [ ] **Step 1: Write the failing tests**

In `website/tsvz_site_test.py`, make the import lines at the top read:

````python
import gzip
import os
import re
import shlex
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path
````

Append to `website/tsvz_site_test.py`:

````python
# -- Task 4: the site's bodies and static export ------------------------------

SITE = tsvz_site.Site(base_url="https://tsvz.org/", spec_path=REPO / "tsvz-spec-v1.md", root=HERE)


def test_site_bodies():
    names = {"index.html", "spec.html", "404.html", "index.md", "spec.md", "llms.txt",
             "llms-full.txt", "robots.txt", "sitemap.xml", "404.txt"}
    assert set(SITE.bodies) == names
    index = SITE.bodies["index.html"]
    assert index.ctype == "text/html; charset=utf-8"
    assert index.link == '<https://tsvz.org/index.md>; rel="alternate"; type="text/markdown"'
    assert re.match(r'^"[0-9a-f]{16}"$', index.etag)
    assert gzip.decompress(index.gz) == index.data
    assert SITE.bodies["spec.md"].data == (REPO / "tsvz-spec-v1.md").read_bytes()
    assert SITE.bodies["spec.md"].disposition == 'inline; filename="tsvz-spec-v1.md"'
    assert SITE.bodies["index.md"].data == (HERE / "index.md").read_bytes()
    assert SITE.bodies["sitemap.xml"].ctype == "application/xml"
    assert len(index.data) <= 30 * 1024


def test_site_pages_have_descriptions_and_csp_hashes():
    landing = SITE.bodies["index.html"].data.decode()
    spec = SITE.bodies["spec.html"].data.decode()
    assert '<meta name="description" content="TSVZ files are plain CSV or TSV where every write' in landing
    assert ('<meta name="description" content="A line-oriented, append-only, key–value '
            'write-ahead-log format in plain UTF-8 text.">') in spec
    assert '<link rel="canonical" href="https://tsvz.org/spec">' in spec
    assert '<a href="/spec" aria-current="page">Spec</a>' in spec
    assert "<style>" in SITE.bodies["404.html"].data.decode()
    style = re.search(r"<style>(.*?)</style>", landing, re.S).group(1)
    script = re.search(r"<script>(.*?)</script>", landing, re.S).group(1)
    assert tsvz_site._csp_hash(style) in SITE.csp and tsvz_site._csp_hash(script) in SITE.csp
    assert "default-src 'none'" in SITE.csp and "frame-ancestors 'none'" in SITE.csp


def test_llms_txt_lists_glance_docs_and_every_implementation():
    text = SITE.bodies["llms.txt"].data.decode()
    assert text.startswith("# TSVZ\n\n> An append-only CSV that's also a key-value store")
    assert "[§4.3](https://tsvz.org/spec#s4.3)" in text
    assert "- [Specification v1](https://tsvz.org/spec.md)" in text
    rows = tsvz_site.implementations(INDEX_MD)
    assert rows and rows[0]["Language"] == "Python (reference)"
    for row in rows:
        assert "- [%s, %s](" % (row["Language"], row["Version"]) in text
    assert "Read, Write, CLI §20, Handler §21" in text
    assert text.rstrip().endswith("- [Everything in one file](https://tsvz.org/llms-full.txt)")


def test_llms_full_robots_sitemap():
    full = SITE.bodies["llms-full.txt"].data.decode()
    assert full.startswith(INDEX_MD.rstrip("\n")) and "# At a glance" in full
    assert full.rstrip("\n").endswith(SPEC.rstrip("\n"))
    assert SITE.bodies["robots.txt"].data == b"User-agent: *\nAllow: /\n\nSitemap: https://tsvz.org/sitemap.xml\n"
    sitemap = SITE.bodies["sitemap.xml"].data.decode()
    assert "<loc>https://tsvz.org/</loc>" in sitemap and "<loc>https://tsvz.org/spec</loc>" in sitemap


def test_base_url_is_normalised():
    site = tsvz_site.Site(base_url="http://localhost:8765", spec_path=REPO / "tsvz-spec-v1.md", root=HERE)
    assert site.base_url == "http://localhost:8765/"
    assert b'href="http://localhost:8765/spec"' in site.bodies["spec.html"].data


def test_missing_input_is_a_site_error(tmp_path):
    try:
        tsvz_site.Site(spec_path=tmp_path / "nope.md", root=HERE)
    except tsvz_site.SiteError as e:
        assert "nope.md" in str(e)
    else:
        raise AssertionError("no SiteError")


def test_non_utf8_source_is_a_site_error(tmp_path):
    bad = tmp_path / "spec.md"
    bad.write_bytes(b"# Spec\n\xff\xfe\n")
    try:
        tsvz_site.Site(spec_path=bad, root=HERE)
    except tsvz_site.SiteError as e:
        assert "spec.md" in str(e)
    else:
        raise AssertionError("no SiteError")


def test_git_date(tmp_path):
    date = tsvz_site._git_date(REPO / "tsvz-spec-v1.md")
    assert date is None or re.match(r"^\d{4}-\d{2}-\d{2}$", date)
    (tmp_path / "x.md").write_text("x")
    assert tsvz_site._git_date(tmp_path / "x.md") is None


def test_build_writes_the_file_list(tmp_path):
    written = SITE.build(tmp_path)
    rel = sorted(str(p.relative_to(tmp_path)) for p in written)
    assert rel == sorted(tsvz_site.BUILD_FILES)
    assert (tmp_path / "spec" / "index.html").read_bytes() == SITE.bodies["spec.html"].data


def test_cli_build_and_missing_input(tmp_path):
    script = str(HERE / "tsvz_site.py")
    run = subprocess.run([sys.executable, script, "build", str(tmp_path / "out")], cwd=str(tmp_path),
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    assert run.returncode == 0, run.stderr
    assert (tmp_path / "out" / "llms.txt").is_file()
    run = subprocess.run([sys.executable, script, "build", str(tmp_path / "o2"), "--spec",
                          str(tmp_path / "missing.md")], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    assert run.returncode == 1
    assert run.stderr.decode().startswith("tsvz_site: cannot read ")
````

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m pytest website/tsvz_site_test.py -q`
Expected: collection error `AttributeError: module 'tsvz_site' has no attribute 'Site'`.

- [ ] **Step 3: Write the site**

In `website/tsvz_site.py`, replace the import lines at the top (from `from __future__ import annotations` to `from collections import namedtuple`) with:

````python
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import re
import subprocess
import sys
from collections import namedtuple
from pathlib import Path
````

Append to `website/tsvz_site.py`, after two blank lines:

````python
# ---------------------------------------------------------------------------
# Site: every response body, built once
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent
DEFAULT_SPEC = HERE.parent / "tsvz-spec-v1.md"
DEFAULT_BASE_URL = "https://tsvz.org/"
LLMS_SUMMARY = ("An append-only CSV that's also a key-value store: an open, plain-text "
                "key-value log format (spec v1) that people, agents and CSV tools read as-is.")

Body = namedtuple("Body", "data gz ctype etag disposition link")

BUILD_FILES = {
    "index.html": "index.html",
    "index.md": "index.md",
    "spec/index.html": "spec.html",
    "spec.md": "spec.md",
    "tsvz-spec-v1.md": "spec.md",
    "llms.txt": "llms.txt",
    "llms-full.txt": "llms-full.txt",
    "robots.txt": "robots.txt",
    "sitemap.xml": "sitemap.xml",
    "404.html": "404.html",
}


def _csp_hash(text):
    """A CSP source expression for an inline <style> or <script> body."""
    return "'sha256-%s'" % base64.b64encode(hashlib.sha256(text.encode("utf-8")).digest()).decode("ascii")


def _git_date(path):
    """The date of path's last git commit (YYYY-MM-DD), or None."""
    try:
        out = subprocess.run(["git", "log", "-1", "--format=%cs", "--", path.name],
                             cwd=str(path.parent), stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, timeout=2, check=True)
    except (OSError, subprocess.SubprocessError):
        return None
    date = out.stdout.decode("ascii", "replace").strip()
    return date if re.match(r"^\d{4}-\d{2}-\d{2}$", date) else None


class SiteError(Exception):
    """An input file is missing or unreadable."""


def _read(path):
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        raise SiteError("cannot read %s: %s" % (path, e))


def implementations(index_md):
    """The rows of the Implementations table in index.md, as dicts."""
    blocks, _ = render(index_md)
    in_section = False
    for block in blocks:
        if block.kind == "h2":
            in_section = block.heading.id == "implementations"
        elif in_section and block.kind == "table":
            lines = block.raw.split("\n")
            head = split_row(lines[0])
            return [dict(zip(head, split_row(line))) for line in lines[2:]]
    return []


class Site:
    """Reads the sources and renders every response body once."""

    def __init__(self, base_url=DEFAULT_BASE_URL, spec_path=DEFAULT_SPEC, root=HERE):
        self.base_url = base_url.rstrip("/") + "/"
        self.index_md = _read(root / "index.md")
        self.glance_md = _read(root / "spec-glance.md")
        self.css = _read(root / "style.css").strip()
        self.spec_md = _read(spec_path)
        self.spec_date = _git_date(spec_path)
        self.csp = ("default-src 'none'; style-src %s; script-src %s; img-src data:; "
                    "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
                    % (_csp_hash(self.css), _csp_hash(SCRIPT)))
        self.bodies = {}
        self._build_pages()
        self._build_texts()

    def _add(self, name, text, ctype, disposition=None, link=None):
        data = text.encode("utf-8")
        etag = '"%s"' % hashlib.sha256(data).hexdigest()[:16]
        self.bodies[name] = Body(data, gzip.compress(data, mtime=0), ctype, etag, disposition, link)

    def _link(self, md_path):
        return '<%s%s>; rel="alternate"; type="text/markdown"' % (self.base_url, md_path)

    def _build_pages(self):
        index_blocks, _ = render(self.index_md)
        lead = next(b for b in index_blocks if b.kind == "para")
        spec_blocks, self.anchors = render(self.spec_md, spec=True)
        glance_blocks, _ = render(self.glance_md, spec=True, anchors=self.anchors)
        tagline = next(b for b in spec_blocks if b.kind == "para" and not b.raw.startswith("**Version:**"))
        common = {"css": self.css}
        self._add("index.html", page(
            title=LANDING_TITLE, description=plain_text(lead.raw), canonical=self.base_url,
            md_url="/index.md", current="home", main=landing_main(index_blocks),
            body_class="page-landing", md_link_text="This page as markdown", **common),
            "text/html; charset=utf-8", link=self._link("index.md"))
        self._add("spec.html", page(
            title=SPEC_TITLE, description=plain_text(tagline.raw), canonical=self.base_url + "spec",
            md_url="/spec.md", current="spec",
            main=spec_main(spec_blocks, "".join(b.html for b in glance_blocks), self.spec_date),
            body_class="page-spec", md_link_text="This page as markdown", **common),
            "text/html; charset=utf-8", link=self._link("spec.md"))
        self._add("404.html", page(
            title="Not found · TSVZ", description="No page at this address.", canonical=None,
            md_url=None, current="", main=NOT_FOUND_MAIN, body_class="page-404",
            md_link_text="", **common), "text/html; charset=utf-8")

    def _build_texts(self):
        md = "text/markdown; charset=utf-8"
        text = "text/plain; charset=utf-8"
        self._add("index.md", self.index_md, md, disposition='inline; filename="index.md"')
        self._add("spec.md", self.spec_md, md, disposition='inline; filename="tsvz-spec-v1.md"')
        self._add("llms.txt", self.llms_txt(), text)
        self._add("llms-full.txt", "\n\n---\n\n".join(
            [self.index_md.rstrip("\n"), "# At a glance\n\n" + self.glance_md.rstrip("\n"),
             self.spec_md.rstrip("\n")]) + "\n", text)
        self._add("robots.txt", "User-agent: *\nAllow: /\n\nSitemap: %ssitemap.xml\n" % self.base_url, text)
        self._add("sitemap.xml", self.sitemap_xml(), "application/xml")
        self._add("404.txt", "Not found. See %sllms.txt\n" % self.base_url, text)

    def llms_txt(self):
        """The llmstxt.org index, generated from index.md and spec-glance.md."""
        def absolute(m):
            target = resolve_ref(m.group(1), self.anchors)
            if not target:
                return m.group(0)
            return "[%s](%sspec#s%s)" % (m.group(0), self.base_url, target)

        lines = ["# TSVZ", "", "> " + LLMS_SUMMARY, "",
                 _SECTION_REF.sub(absolute, self.glance_md.strip()), "",
                 "## Docs", "",
                 "- [Overview](%sindex.md): what TSVZ is, quick start, trade-offs" % self.base_url,
                 "- [Specification v1](%sspec.md): the full format specification" % self.base_url,
                 "", "## Implementations", ""]
        for row in implementations(self.index_md):
            url = re.search(r"\]\(([^)\s]+)\)", row.get("Links", ""))
            features = [name for name, value in row.items() if value == "✓"]
            lines.append("- [%s, %s](%s): %s; %s" % (
                row.get("Language", ""), row.get("Version", ""), url.group(1) if url else GITHUB_URL,
                row.get("Package", ""), ", ".join(features)))
        lines += ["", "## Optional", "",
                  "- [Everything in one file](%sllms-full.txt)" % self.base_url, ""]
        return "\n".join(lines)

    def sitemap_xml(self):
        lastmod = "<lastmod>%s</lastmod>" % self.spec_date if self.spec_date else ""
        return ('<?xml version="1.0" encoding="UTF-8"?>\n'
                '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
                "  <url><loc>%s</loc></url>\n"
                "  <url><loc>%sspec</loc>%s</url>\n"
                "</urlset>\n" % (self.base_url, self.base_url, lastmod))

    def build(self, outdir):
        """Write every page and text file under outdir; returns the paths written."""
        written = []
        for rel, name in BUILD_FILES.items():
            path = Path(outdir) / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(self.bodies[name].data)
            written.append(path)
        return written


def main(argv=None):
    parser = argparse.ArgumentParser(prog="tsvz_site.py", description="The tsvz.org website.")
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build", help="write the site as static files")
    build.add_argument("outdir", help="directory to write into")
    build.add_argument("--base-url", default=DEFAULT_BASE_URL, help="public URL (default: %(default)s)")
    build.add_argument("--spec", default=str(DEFAULT_SPEC), help="the spec's markdown file")
    args = parser.parse_args(argv)
    try:
        site = Site(base_url=args.base_url, spec_path=Path(args.spec))
    except SiteError as e:
        print("tsvz_site: %s" % e, file=sys.stderr)
        return 1
    for path in site.build(Path(args.outdir)):
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
````

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m pytest website/tsvz_site_test.py -q && ruff check --select E,F,W --line-length 120 website/`
Expected: `40 passed`; `All checks passed!`

Then look at the generated agent index:

```bash
python3 website/tsvz_site.py build /tmp/tsvz-site-out && cat /tmp/tsvz-site-out/llms.txt && rm -rf /tmp/tsvz-site-out
```

Expected: the ten paths, then an `llms.txt` with:
- the summary line;
- the six glance bullets, with links such as `[§4.3](https://tsvz.org/spec#s4.3)`;
- Docs;
- ``- [Python (reference), 4.2+](https://github.com/yufei-pan/TSVZ): `tsvz` on PyPI; Read, Write, CLI §20, Handler §21``;
- Optional.

- [ ] **Step 5: Commit**

```bash
git add website/tsvz_site.py website/tsvz_site_test.py
git commit -F - <<'MSG'
Build every tsvz.org response body once, with llms.txt and a static export.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
MSG
```

---

### Task 5: Serving HTML to browsers and markdown to agents

**Files:**
- Modify: `website/tsvz_site.py` (imports; replace `main` and what follows with the "Serving" section and the new `main`)
- Modify: `website/tsvz_site_test.py` (imports; append the Task 5 group)
- Modify: `docs/superpowers/specs/2026-10-10-tsvz-website-design.md` (deviation 7)

**Interfaces:**
- Consumes (Task 4): `Site` (`.bodies`, `.csp`, `.base_url`), `SiteError`, `DEFAULT_BASE_URL`, `DEFAULT_SPEC`.
- Produces:
  - `make_server(site, host, port) -> ThreadingHTTPServer` (port 0 picks a free port), and `Handler`;
  - `wants_markdown(user_agent, accept, headers, *, query_format=None)`, the same function as in `serve_spec.py`;
  - `accepts_gzip(header)` and `etag_matches(header, etag)`;
  - `NEGOTIATED`, `FIXED`, `DEFAULT_PORT`;
  - `main(argv)` with `serve` and `build`.

- [ ] **Step 1: Write the failing tests**

In `website/tsvz_site_test.py`, make the import lines at the top read:

````python
import gzip
import http.client
import os
import re
import shlex
import socket
import subprocess
import sys
import threading
from html.parser import HTMLParser
from pathlib import Path

import pytest
````

Append to `website/tsvz_site_test.py`:

````python
# -- Task 5: serving -----------------------------------------------------------

CHROME = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/140.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
}
CURL = {"User-Agent": "curl/8.5.0", "Accept": "*/*"}
CLAUDEBOT = {"User-Agent": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; "
                           "ClaudeBot/1.0; +claudebot@anthropic.com)", "Accept": "*/*"}
MD_ACCEPT = {"User-Agent": "SomeAgent/1.0", "Accept": "text/markdown, text/html;q=0.9"}
HTML = "text/html; charset=utf-8"
MD = "text/markdown; charset=utf-8"


@pytest.fixture(scope="module")
def port():
    server = tsvz_site.make_server(SITE, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server.server_address[1]
    server.shutdown()
    server.server_close()


def fetch(port, path, headers=None, method="GET"):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request(method, path, headers=dict(headers or {}))
    resp = conn.getresponse()
    body = resp.read()
    conn.close()
    return resp.status, {k.lower(): v for k, v in resp.getheaders()}, body


@pytest.mark.parametrize("path,headers,ctype,name", [
    ("/", CHROME, HTML, "index.html"),
    ("/index.html", CHROME, HTML, "index.html"),
    ("/spec", CHROME, HTML, "spec.html"),
    ("/spec/", CHROME, HTML, "spec.html"),
    ("/", CURL, MD, "index.md"),
    ("/spec", CURL, MD, "spec.md"),
    ("/spec", CLAUDEBOT, MD, "spec.md"),
    ("/", MD_ACCEPT, MD, "index.md"),
    ("/?format=md", CHROME, MD, "index.md"),
    ("/spec?format=html", CURL, HTML, "spec.html"),
    ("/index.md", CHROME, MD, "index.md"),
    ("/spec.md", CHROME, MD, "spec.md"),
    ("/tsvz-spec-v1.md", CHROME, MD, "spec.md"),
    ("/llms.txt", CHROME, "text/plain; charset=utf-8", "llms.txt"),
    ("/llms-full.txt", CURL, "text/plain; charset=utf-8", "llms-full.txt"),
    ("/robots.txt", CURL, "text/plain; charset=utf-8", "robots.txt"),
    ("/sitemap.xml", CURL, "application/xml", "sitemap.xml"),
])
def test_who_gets_what(port, path, headers, ctype, name):
    status, head, body = fetch(port, path, headers)
    assert status == 200
    assert head["content-type"] == ctype
    assert body == SITE.bodies[name].data
    assert head["content-length"] == str(len(body))


def test_headers(port):
    _, head, _ = fetch(port, "/", CHROME)
    assert head["vary"] == "Accept, User-Agent, Accept-Encoding"
    assert head["link"] == '<https://tsvz.org/index.md>; rel="alternate"; type="text/markdown"'
    assert head["content-security-policy"] == SITE.csp
    assert head["x-content-type-options"] == "nosniff"
    assert head["referrer-policy"] == "strict-origin-when-cross-origin"
    assert head["cache-control"] == "public, max-age=300"
    assert head["etag"] == SITE.bodies["index.html"].etag
    _, head, _ = fetch(port, "/spec", CURL)
    assert head["content-disposition"] == 'inline; filename="tsvz-spec-v1.md"'
    assert "content-security-policy" not in head and "link" not in head
    _, head, _ = fetch(port, "/llms.txt", CHROME)
    assert head["vary"] == "Accept-Encoding"


def test_gzip(port):
    _, head, body = fetch(port, "/spec", dict(CHROME, **{"Accept-Encoding": "gzip, deflate, br"}))
    assert head["content-encoding"] == "gzip"
    assert gzip.decompress(body) == SITE.bodies["spec.html"].data
    for refused in ("identity", "gzip;q=0", "br"):
        _, head, body = fetch(port, "/spec", dict(CHROME, **{"Accept-Encoding": refused}))
        assert "content-encoding" not in head and body == SITE.bodies["spec.html"].data


def test_etags_and_304(port):
    etag = SITE.bodies["spec.html"].etag
    for tag in (etag, "W/" + etag, '"other", ' + etag, "*"):
        status, head, body = fetch(port, "/spec", dict(CHROME, **{"If-None-Match": tag}))
        assert status == 304 and body == b"" and head["etag"] == etag
    status, _, _ = fetch(port, "/spec", dict(CHROME, **{"If-None-Match": '"stale"'}))
    assert status == 200


def test_head_matches_get(port):
    status, get_head, get_body = fetch(port, "/spec", CHROME)
    status2, head_head, head_body = fetch(port, "/spec", CHROME, method="HEAD")
    assert status == status2 == 200 and head_body == b""
    assert head_head["content-length"] == get_head["content-length"] == str(len(get_body))


def test_not_found(port):
    status, head, body = fetch(port, "/nope", CHROME)
    assert status == 404 and head["content-type"] == HTML and b"No page here" in body
    status, head, body = fetch(port, "/nope", CURL)
    assert status == 404 and body == b"Not found. See https://tsvz.org/llms.txt\n"
    status, _, _ = fetch(port, "/nope", dict(CHROME, **{"If-None-Match": "*"}))
    assert status == 404


def test_other_methods(port):
    for method in ("POST", "PUT", "DELETE", "OPTIONS", "PROPFIND"):
        status, head, body = fetch(port, "/", CHROME, method=method)
        assert status == 405 and head["allow"] == "GET, HEAD" and body == b""


def test_cli_serve_starts_and_answers(tmp_path):
    proc = subprocess.Popen([sys.executable, str(HERE / "tsvz_site.py"), "serve", "-p", "0"],
                            cwd=str(tmp_path), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        line = proc.stdout.readline().decode()
        m = re.search(r"on http://127\.0\.0\.1:(\d+)/", line)
        assert m, line
        status, head, _ = fetch(int(m.group(1)), "/", CURL)
        assert status == 200 and head["content-type"] == MD
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        proc.stdout.close()
        proc.stderr.close()


def test_cli_serve_port_in_use():
    busy = socket.socket()
    busy.bind(("127.0.0.1", 0))
    busy.listen(1)
    try:
        port_number = busy.getsockname()[1]
        run = subprocess.run([sys.executable, str(HERE / "tsvz_site.py"), "serve", "-p", str(port_number)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    finally:
        busy.close()
    assert run.returncode == 1
    assert run.stderr.decode().startswith("tsvz_site: cannot listen on 127.0.0.1:%d: " % port_number)
````

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m pytest website/tsvz_site_test.py -q`
Expected:
- errors in the fixture: `AttributeError: module 'tsvz_site' has no attribute 'make_server'`;
- `test_cli_serve_*` fail, because `serve` is not a command yet;
- the 40 earlier tests pass.

- [ ] **Step 3: Write the server**

In `website/tsvz_site.py`, replace the import lines at the top with:

````python
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import re
import subprocess
import sys
from collections import namedtuple
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
````

Then replace everything from `def main(argv=None):` to the end of the file with:

````python
# ---------------------------------------------------------------------------
# Serving: HTML to browsers, markdown to agents (logic from serve_spec.py)
# ---------------------------------------------------------------------------

DEFAULT_PORT = 8765

NEGOTIATED = {
    "/": ("index.html", "index.md"),
    "/index.html": ("index.html", "index.md"),
    "/spec": ("spec.html", "spec.md"),
    "/spec/": ("spec.html", "spec.md"),
}
FIXED = {
    "/index.md": "index.md",
    "/spec.md": "spec.md",
    "/tsvz-spec-v1.md": "spec.md",
    "/llms.txt": "llms.txt",
    "/llms-full.txt": "llms-full.txt",
    "/robots.txt": "robots.txt",
    "/sitemap.xml": "sitemap.xml",
}

# curl, wget, common CLI / library clients
_CLI_UA = re.compile(
    r"(curl/|wget/|httpie/|Go-http-client/|python-requests/|"
    r"Python-urllib/|libwww-perl/|aiohttp/|httpx/|okhttp/|"
    r"Java/|node-fetch/|axios/|PostmanRuntime/)",
    re.I,
)

# Known AI / LLM crawlers and fetch agents
_AI_UA = re.compile(
    r"(GPTBot|ChatGPT-User|OAI-SearchBot|ClaudeBot|Claude-Web|"
    r"anthropic-ai|Google-Extended|GoogleOther|PerplexityBot|"
    r"Bytespider|CCBot|Amazonbot|FacebookBot|Meta-ExternalAgent|"
    r"Applebot-Extended|cohere-ai|Diffbot|YouBot|AI2Bot|"
    r"ImagesiftBot|anthropic|OpenAI|Claude|Perplexity|Gemini|"
    r"bingbot.*chat|CopilotBot|MetaAI|meta-externalfetch|"
    r"aiagent|fetcher|llm|langchain|LlamaIndex)",
    re.I,
)


def _accept_prefers_markdown(accept: str) -> bool:
    if not accept or accept.strip() == "*/*":
        return True
    parts = [p.strip() for p in accept.split(",") if p.strip()]
    scored: list[tuple[float, str]] = []
    for part in parts:
        if ";" in part:
            media, *params = part.split(";")
            q = 1.0
            for param in params:
                param = param.strip()
                if param.startswith("q="):
                    try:
                        q = float(param[2:])
                    except ValueError:
                        pass
        else:
            media, q = part, 1.0
        media = media.strip().lower()
        scored.append((q, media))
    scored.sort(key=lambda x: -x[0])
    for q, media in scored:
        if media in ("text/markdown", "text/x-markdown", "text/plain"):
            return True
        if media == "text/html":
            return False
        if media == "*/*":
            return True
    return False


def wants_markdown(
    user_agent: str,
    accept: str,
    headers: dict[str, str],
    *,
    query_format: str | None = None,
) -> bool:
    if query_format == "md":
        return True
    if query_format == "html":
        return False

    ua = user_agent or ""
    if _CLI_UA.search(ua) or _AI_UA.search(ua):
        return True

    accept_l = (accept or "").lower()
    if "text/markdown" in accept_l or "text/x-markdown" in accept_l:
        return True

    # Modern browsers send Sec-Fetch-Mode: navigate on top-level loads.
    sec_mode = headers.get("Sec-Fetch-Mode", headers.get("sec-fetch-mode", ""))
    sec_dest = headers.get("Sec-Fetch-Dest", headers.get("sec-fetch-dest", ""))
    if sec_mode == "navigate" and "text/html" in accept_l:
        return False
    if sec_dest == "document" and "text/html" in accept_l:
        return False

    if _accept_prefers_markdown(accept):
        return True

    if "text/html" in accept_l:
        return False

    return True


def accepts_gzip(header):
    """True when an Accept-Encoding header allows gzip."""
    for part in (header or "").split(","):
        name, _, params = part.partition(";")
        if name.strip().lower() == "gzip":
            m = re.search(r"q\s*=\s*([0-9.]+)", params)
            try:
                return not m or float(m.group(1)) > 0
            except ValueError:
                return True
    return False


def etag_matches(header, etag):
    """True when an If-None-Match header names etag (or is *)."""
    if not header:
        return False
    tags = [t.strip() for t in header.split(",")]
    return "*" in tags or etag in tags or "W/" + etag in tags


class Handler(BaseHTTPRequestHandler):
    """Serves one Site's bodies.  GET and HEAD only."""

    site = None
    server_version = "tsvz.org"
    sys_version = ""

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt % args))

    def __getattr__(self, name):
        if name.startswith("do_"):
            return self._not_allowed
        raise AttributeError(name)

    def _not_allowed(self):
        self.send_response(405)
        self.send_header("Allow", "GET, HEAD")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._serve(send_body=True)

    def do_HEAD(self):
        self._serve(send_body=False)

    def _wants_markdown(self, query_format):
        headers = {k: v for k, v in self.headers.items()}
        return wants_markdown(self.headers.get("User-Agent", ""), self.headers.get("Accept", ""),
                              headers, query_format=query_format)

    def _serve(self, send_body):
        parts = urlsplit(self.path)
        status, vary = 200, "Accept, User-Agent, Accept-Encoding"
        if parts.path in NEGOTIATED:
            fmt = parse_qs(parts.query).get("format", [None])[0]
            html_name, md_name = NEGOTIATED[parts.path]
            name = md_name if self._wants_markdown(fmt) else html_name
        elif parts.path in FIXED:
            name, vary = FIXED[parts.path], "Accept-Encoding"
        else:
            status = 404
            name = "404.txt" if self._wants_markdown(None) else "404.html"
        body = self.site.bodies[name]
        if status == 200 and etag_matches(self.headers.get("If-None-Match"), body.etag):
            self.send_response(304)
            self._common_headers(body, vary)
            self.end_headers()
            return
        use_gzip = accepts_gzip(self.headers.get("Accept-Encoding"))
        data = body.gz if use_gzip else body.data
        self.send_response(status)
        self.send_header("Content-Type", body.ctype)
        self.send_header("Content-Length", str(len(data)))
        if use_gzip:
            self.send_header("Content-Encoding", "gzip")
        self._common_headers(body, vary)
        if body.ctype.startswith("text/html"):
            self.send_header("Content-Security-Policy", self.site.csp)
        if body.link:
            self.send_header("Link", body.link)
        if body.disposition:
            self.send_header("Content-Disposition", body.disposition)
        self.end_headers()
        if send_body:
            self.wfile.write(data)

    def _common_headers(self, body, vary):
        self.send_header("Vary", vary)
        self.send_header("ETag", body.etag)
        self.send_header("Cache-Control", "public, max-age=300")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")


def make_server(site, host, port):
    """A ThreadingHTTPServer that serves site (port 0 picks a free port)."""
    handler = type("SiteHandler", (Handler,), {"site": site})
    return ThreadingHTTPServer((host, port), handler)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="tsvz_site.py", description="The tsvz.org website.")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="serve the site over HTTP")
    serve.add_argument("-H", "--host", default="127.0.0.1", help="bind address (default: 127.0.0.1)")
    serve.add_argument("-p", "--port", type=int, default=DEFAULT_PORT,
                       help="port (default: %d)" % DEFAULT_PORT)
    build = sub.add_parser("build", help="write the site as static files")
    build.add_argument("outdir", help="directory to write into")
    for p in (serve, build):
        p.add_argument("--base-url", default=DEFAULT_BASE_URL, help="public URL (default: %(default)s)")
        p.add_argument("--spec", default=str(DEFAULT_SPEC), help="the spec's markdown file")
    args = parser.parse_args(argv)
    try:
        site = Site(base_url=args.base_url, spec_path=Path(args.spec))
    except SiteError as e:
        print("tsvz_site: %s" % e, file=sys.stderr)
        return 1
    if args.command == "build":
        for path in site.build(Path(args.outdir)):
            print(path)
        return 0
    try:
        server = make_server(site, args.host, args.port)
    except OSError as e:
        print("tsvz_site: cannot listen on %s:%d: %s" % (args.host, args.port, e.strerror or e), file=sys.stderr)
        return 1
    print("Serving %s on http://%s:%d/" % (site.base_url, args.host, server.server_address[1]), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
````

`wants_markdown`, `_accept_prefers_markdown`, `_CLI_UA` and `_AI_UA` are the functions and patterns of `serve_spec.py`, copied without change. The design requires the same rules for choosing HTML or markdown.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m pytest website/tsvz_site_test.py -q && ruff check --select E,F,W --line-length 120 website/`
Expected: `65 passed`; `All checks passed!`

- [ ] **Step 5: Record deviation 7 in the design doc**

In `docs/superpowers/specs/2026-10-10-tsvz-website-design.md`, make these replacements (each text occurs once):

1. Replace:

   ````text
   - `serve` defaults to `127.0.0.1:8765`, the address and port of
     `serve_spec.py`.
   ````

   with:

   ````text
   - `serve` defaults to `127.0.0.1:8765`, the address and port of
     `serve_spec.py`.
   - `serve` exits with status 1 and one line,
     `tsvz_site: cannot listen on HOST:PORT: <reason>`, when it cannot listen.
   ````

2. Replace:

   ````text
   | `/spec` | spec HTML |
   ````

   with:

   ````text
   | `/spec`, `/spec/` | spec HTML |
   ````

- [ ] **Step 6: Commit**

```bash
git add website/tsvz_site.py website/tsvz_site_test.py docs/superpowers/specs/2026-10-10-tsvz-website-design.md
git commit -F - <<'MSG'
Serve tsvz.org with HTML for browsers, markdown for agents, gzip and ETags.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
MSG
```

---

### Task 6: Deployment, switch-over and the browser check

**Files:**
- Create: `deploy/tsvz-site.service`
- Delete: `serve_spec.py`, `deploy/tsvz-spec.service`
- Modify: `README.md`, `.gitignore`

**Interfaces:**
- Consumes: `website/tsvz_site.py serve` and `build` (Task 5).
- Produces: the deployable unit and the final state of the branch.

- [ ] **Step 1: Add the systemd unit and remove the old server**

Create `deploy/tsvz-site.service`:

````ini
# Sample systemd unit for website/tsvz_site.py, the tsvz.org website.
#
# Install (adjust paths and user first):
#   sudo cp deploy/tsvz-site.service /etc/systemd/system/
#   sudo systemctl daemon-reload
#   sudo systemctl enable --now tsvz-site.service
#
# It replaces tsvz-spec.service (serve_spec.py) on the same port; if that unit
# is installed:  sudo systemctl disable --now tsvz-spec.service
#
# Publish an edit to website/*.md, website/style.css or tsvz-spec-v1.md:
#   sudo systemctl restart tsvz-site.service
#
# The spec page's "updated" date comes from `git log`; it is left out when git
# cannot read the checkout (for example, when the tsvz user does not own it).
#
# Logs: journalctl -u tsvz-site.service -f

[Unit]
Description=tsvz.org website (landing page and TSVZ format specification)
Documentation=https://tsvz.org/
After=network-online.target
Wants=network-online.target

[Service]
Type=simple

# Clone or copy the repo here (must contain website/ and tsvz-spec-v1.md).
WorkingDirectory=/opt/tsvz

# Listen on all interfaces; put nginx/caddy in front for TLS in production.
# For localhost-only (reverse proxy on same host), use: -H 127.0.0.1
ExecStart=/usr/bin/python3 /opt/tsvz/website/tsvz_site.py serve -H 0.0.0.0 -p 8765

# Dedicated unprivileged account (create with: useradd --system --home /opt/tsvz --shell /usr/sbin/nologin tsvz)
User=tsvz
Group=tsvz

Restart=on-failure
RestartSec=5s

# Hardening (read-only service; only needs the repo directory)
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=
ReadOnlyPaths=/opt/tsvz

[Install]
WantedBy=multi-user.target
````

Then:

```bash
git rm -q serve_spec.py deploy/tsvz-spec.service
```

- [ ] **Step 2: Update the README and .gitignore**

In `README.md`, make these replacements (each text occurs once):

1. Replace:

   ````text
   # TSVZ

   TSVZ is a small, dependency-free library
   ````

   with:

   ````text
   # TSVZ

   **[tsvz.org](https://tsvz.org)**: the format, its specification and its implementations.

   TSVZ is a small, dependency-free library
   ````

2. Replace:

   ````text
   ## Tests
   ````

   with:

   ````text
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
   ````

3. Replace:

   ````text
   python3 -m pytest TSVZ_test.py -q                         # everything, incl. 3.39 differential tests
   ````

   with:

   ````text
   python3 -m pytest TSVZ_test.py -q                         # everything, incl. 3.39 differential tests
   python3 -m pytest website/tsvz_site_test.py -q            # the tsvz.org site
   ````

Replace the whole of `.gitignore` with (this also adds the final newline it lacked):

````
dist
*.egg-info
__pycache__
test*
.worktrees/
.superpowers/
````

- [ ] **Step 3: Run every test, on 3.13 and on 3.8**

```bash
python3 -m pytest website/tsvz_site_test.py -q
python3 -m pytest TSVZ_test.py -q
ruff check --select E,F,W --line-length 120 website/
docker run --rm -v "$PWD":/w -w /w python:3.8-slim sh -c \
  "pip install -q pytest >/dev/null 2>&1; python -m pytest website/tsvz_site_test.py -q -p no:cacheprovider"
```

Expected:
- `65 passed` (website tests);
- the TSVZ suite passes unchanged (`TSVZ.py` was not touched);
- `All checks passed!`;
- `65 passed` on 3.8. git is absent in the image, so the spec page omits its date there.

- [ ] **Step 4: Browser check (manual; not committed)**

Save this script as `shots.js` in your scratchpad directory, not in the repository:

````javascript
// Screenshot pages at several widths in light and dark; report horizontal overflow.
// usage: node shots.js BASE_URL OUTDIR
const { chromium } = require('playwright-core');
const [base, outdir] = process.argv.slice(2);
(async () => {
  const browser = await chromium.launch({ executablePath: '/opt/google/chrome/chrome', args: ['--no-sandbox'] });
  let bad = 0;
  for (const scheme of ['light', 'dark']) {
    for (const width of [375, 768, 1280]) {
      const page = await browser.newPage({ viewport: { width, height: 900 }, colorScheme: scheme });
      const errors = [];
      page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });
      for (const [name, path] of [['landing', '/'], ['spec', '/spec']]) {
        await page.goto(base + path, { waitUntil: 'load' });
        const over = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
        if (over > 0) bad++;
        const file = `${outdir}/${name}-${width}-${scheme}.png`;
        await page.screenshot({ path: file, fullPage: name === 'landing' });
        console.log(`${name} ${width} ${scheme}: overflow=${over}px ${file}`);
      }
      if (errors.length) { console.log('console errors:', errors); bad++; }
      await page.close();
    }
  }
  await browser.close();
  process.exit(bad ? 1 : 0);
})();
````

Serve the site and run the check. Do not use `pkill -f` with a pattern that also appears in your own command line: it kills the shell running it.

```bash
SHOTS=<your scratchpad>/shots && mkdir -p "$SHOTS"
python3 website/tsvz_site.py serve -p 18765 > "$SHOTS/serve.log" 2>&1 &
for i in $(seq 1 20); do curl -s -o /dev/null http://127.0.0.1:18765/ && break; sleep 0.3; done
PW=$(dirname "$(find /root/.npm/_npx -maxdepth 3 -name playwright-core -type d | head -1)")
NODE_PATH="$PW" node <your scratchpad>/shots.js http://127.0.0.1:18765 "$SHOTS"; echo "exit=$?"
pgrep -f '^python3 website/tsvz_site.py serve' | xargs -r kill
```

Expected: 12 lines (`landing` and `spec` × 375/768/1280 × light/dark), each `overflow=0px`, and `exit=0`. No console errors appear; a CSP hash mismatch would show up as one.

Then open the screenshots and confirm:
- **1280 landing:**
  - the hero is two columns;
  - the headline breaks as "An append-only / CSV that's also a / key-value store.";
  - the file notes are right-aligned beside their lines;
  - the Quick start blocks sit side by side with no cut-off comments.
- **768 landing:** the hero and Quick start are stacked, and the cards are 2 × 2.
- **375 landing:**
  - the buttons fill the width;
  - each file note sits under its line;
  - the implementations table is a labelled card.
- **1280 spec:** the sidebar, the glance box with teal `§` links, and `MUST` in rust.
- **375 spec:** a "Contents" box above the glance box.
- **Dark:** warm near-black with teal accents; the code blocks stay dark.

If `playwright-core` is missing from the npx cache, run `npx -y playwright-core --version` once to fetch it.

- [ ] **Step 5: Commit**

```bash
git add deploy/tsvz-site.service README.md .gitignore
git commit -F - <<'MSG'
Replace serve_spec.py with the tsvz.org site and its systemd unit.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01FGorN6GFsPEZYEG592m1ew
MSG
git status --short
```

Expected: `git status --short` shows nothing, or only the untracked `.coverage`.
