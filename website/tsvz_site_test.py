"""Tests for website/tsvz_site.py.  Run: python3 -m pytest website/tsvz_site_test.py -q"""

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


# References the design lets link to their closest numbered parent: (reference, target).
FALLBACK_REFS = {("19.2.3", "19.2")}

# Markdown the renderer does not support: it would show up as literal text on the site.
UNSUPPORTED = {
    "image": r"!\[",
    "reference link": r"\]\[",
    "strikethrough": r"~~",
    "autolink": r"<https?://",
    "raw HTML": r"</?(?:a|b|i|p|br|em|div|img|pre|sub|sup|code|span|table|strong|details|summary)\b[^>]*>",
    "setext heading": r"^[^\n]*\S[^\n]*\n[ \t]{0,3}(?:=+|-+)[ \t]*$",
    "indented code": r"(?:^|\n)[ \t]*\n(?:    |\t)(?![ \t]*(?:[-*+]|\d+[.)])[ \t])\S",
}


def spec_problems(markdown):
    """What would go wrong on the site if markdown were the spec; empty when nothing would."""
    problems = []
    source = re.sub(r"^```.*?^```[ \t]*$", "", markdown, flags=re.S | re.M)
    source = re.sub(r"(`+)(?!`).*?(?<!`)\1(?!`)", "", source, flags=re.S)
    for name, pattern in UNSUPPORTED.items():
        if re.search(pattern, source, re.M):
            problems.append("unsupported markdown: %s" % name)
    out = html_of(markdown, spec=True)
    outside_code = re.sub(r"<code[^>]*>.*?</code>", "", out, flags=re.S)
    if "**" in outside_code or "```" in out or "|---" in out:
        problems.append("leftover markdown")
    if re.search(r"<h[1-4](?! id=)", out):
        problems.append("heading without an id")
    ids = re.findall(r'id="([^"]+)"', out)
    if len(ids) != len(set(ids)):
        problems.append("duplicate ids")
    missing = set(re.findall(r'href="#([^"]+)"', out)) - set(ids)
    if missing:
        problems.append("links to missing ids: %s" % sorted(missing)[:5])
    if "§" in re.sub(r'<a class="sref"[^>]*>§[^<]*</a>', "", outside_code):
        problems.append("a § reference that is not a link")
    for target, ref in re.findall(r'<a class="sref" href="#s([^"]+)">§([^<]+)</a>', out):
        if ref != target and (ref, target) not in FALLBACK_REFS:
            problems.append("§%s links to its parent §%s" % (ref, target))
    checker = _TagChecker()
    checker.feed(out)
    checker.close()
    if checker.errors or checker.stack:
        problems.append("unbalanced tags")
    return problems


def test_whole_spec_renders_cleanly():
    assert spec_problems(SPEC) == []
    assert_balanced(html_of(SPEC, spec=True))


@pytest.mark.parametrize("addition", [
    "See §4.30 for details.",          # a subsection that does not exist links to §4
    "See §21.99.",
    "See §4.3.7.",
    "![diagram](x.png)",                # unsupported markdown renders as literal text
    "Some <b>raw</b> HTML.",
    "Setext\n===",
    "A [reference link][1].",
    "~~struck~~ text",
    "An autolink <https://x.org>.",
    "Para.\n\n    indented code\n",
])
def test_spec_guard_catches_regressions(addition):
    assert spec_problems(SPEC + "\n\n" + addition + "\n")


def test_readme_spec_anchors_exist():
    ids = set(re.findall(r'id="([^"]+)"', html_of(SPEC, spec=True)))
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    anchors = re.findall(r"tsvz-spec-v1\.md#([A-Za-z0-9_-]+)", readme)
    assert anchors and set(anchors) <= ids


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


# -- Final review fixes ---------------------------------------------------------


def test_idle_connections_are_closed():
    assert tsvz_site.make_server(SITE, "127.0.0.1", 0).RequestHandlerClass.timeout == 30
    server = tsvz_site.make_server(SITE, "127.0.0.1", 0, timeout=1)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        idle = socket.create_connection(server.server_address, timeout=5)
        idle.sendall(b"GET / HTTP/1.1\r\n")  # never finishes its headers
        assert idle.recv(1024) == b""  # closed by the server, not by our 5 s timeout
        idle.close()
    finally:
        server.shutdown()
        server.server_close()


def test_private_use_characters_render_as_text():
    # The renderer's own placeholders are U+E000..U+E001; text containing them must not
    # hang it, crash it or be swapped for other content.  Run in a child: a hang times out.
    code = ("import sys; sys.path.insert(0, %r); import tsvz_site; "
            "b, _ = tsvz_site.render('a\\ue000b `c\\ue001` \\ue0000\\ue001 `x`'); print(b[0].html)" % str(HERE))
    run = subprocess.run([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
    assert run.returncode == 0, run.stderr
    assert run.stdout.decode() == "<p>a&#xE000;b <code>c&#xE001;</code> &#xE000;0&#xE001; <code>x</code></p>\n\n"


@pytest.mark.parametrize("user_agent", [
    "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)",
    "Twitterbot/1.0",
    "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
    "LinkedInBot/1.0 (compatible; Mozilla/5.0; Apache-HttpClient +http://www.linkedin.com)",
    "Mozilla/5.0 (compatible; Discordbot/2.0; +https://discordapp.com)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
])
def test_link_previews_and_search_crawlers_get_html(port, user_agent):
    # They read <title> and the Open Graph tags, and often send Accept: */*.
    status, head, _ = fetch(port, "/", {"User-Agent": user_agent, "Accept": "*/*"})
    assert status == 200 and head["content-type"] == HTML


def test_ai_agents_and_format_still_win_over_the_preview_rule(port):
    _, head, _ = fetch(port, "/", {"User-Agent": "Mozilla/5.0 (compatible; bingbot/2.0) chat", "Accept": "*/*"})
    assert head["content-type"] == MD
    _, head, _ = fetch(port, "/?format=md", {"User-Agent": "Twitterbot/1.0", "Accept": "*/*"})
    assert head["content-type"] == MD
