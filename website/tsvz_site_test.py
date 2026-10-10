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
