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
