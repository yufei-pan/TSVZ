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


# ---------------------------------------------------------------------------
# Serving: HTML to browsers, markdown to agents (logic from serve_spec.py)
# ---------------------------------------------------------------------------

DEFAULT_PORT = 8765
REQUEST_TIMEOUT = 30  # seconds a connection may stay silent

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


def make_server(site, host, port, timeout=REQUEST_TIMEOUT):
    """A ThreadingHTTPServer that serves site (port 0 picks a free port).

    A connection that sends nothing for timeout seconds is closed, so idle
    clients cannot hold its threads and file descriptors forever.
    """
    handler = type("SiteHandler", (Handler,), {"site": site, "timeout": timeout})
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
