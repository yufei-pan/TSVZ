#!/usr/bin/env python3
"""Serve tsvz-spec-v1.md as a responsive website or raw markdown.

Browsers receive a styled HTML page. curl, wget, download requests, and AI
fetch clients receive the markdown source.

Usage:
    python serve_spec.py              # http://127.0.0.1:8765/
    python serve_spec.py -p 8080
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

SPEC_PATH = Path(__file__).resolve().parent / "tsvz-spec-v1.md"
DEFAULT_PORT = 8765

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


HTML_SHELL = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TSVZ — Format Specification</title>
<meta name="description" content="TSVZ format specification (version 1)">
<link rel="alternate" type="text/markdown" href="/?format=md" title="Markdown source">
<style>
  :root {
    color-scheme: light dark;
    --bg: #ffffff;
    --fg: #1f2328;
    --muted: #656d76;
    --border: #d0d7de;
    --code-bg: #f6f8fa;
    --link: #0969da;
    --toc-bg: #f6f8fa;
    --max: 48rem;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0d1117;
      --fg: #e6edf3;
      --muted: #8b949e;
      --border: #30363d;
      --code-bg: #161b22;
      --link: #58a6ff;
      --toc-bg: #161b22;
    }
  }
  * { box-sizing: border-box; }
  html { scroll-behavior: smooth; }
  body {
    margin: 0;
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    font-size: 16px;
    line-height: 1.6;
    color: var(--fg);
    background: var(--bg);
  }
  header.site {
    border-bottom: 1px solid var(--border);
    padding: 1rem 1.25rem;
    position: sticky;
    top: 0;
    background: var(--bg);
    z-index: 10;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 0.75rem;
  }
  header.site h1 {
    margin: 0;
    font-size: 1.125rem;
    font-weight: 600;
  }
  header.site .meta {
    font-size: 0.875rem;
    color: var(--muted);
  }
  header.site a.raw {
    font-size: 0.875rem;
    color: var(--link);
    text-decoration: none;
    white-space: nowrap;
  }
  header.site a.raw:hover { text-decoration: underline; }
  .layout {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 0;
    max-width: calc(var(--max) + 18rem);
    margin: 0 auto;
    padding: 0 1.25rem 3rem;
  }
  @media (min-width: 960px) {
    .layout {
      grid-template-columns: 14rem minmax(0, var(--max));
      gap: 2.5rem;
      padding-top: 1.5rem;
    }
    nav#toc {
      position: sticky;
      top: 4.5rem;
      align-self: start;
      max-height: calc(100vh - 5.5rem);
      overflow: auto;
    }
  }
  nav#toc {
    display: none;
    font-size: 0.8125rem;
    line-height: 1.4;
    background: var(--toc-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.75rem 1rem;
  }
  @media (min-width: 960px) { nav#toc { display: block; } }
  nav#toc strong {
    display: block;
    margin-bottom: 0.5rem;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
  }
  nav#toc ul {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  nav#toc li { margin: 0.2rem 0; }
  nav#toc a {
    color: var(--muted);
    text-decoration: none;
  }
  nav#toc a:hover { color: var(--link); }
  nav#toc li.depth-2 { padding-left: 0.75rem; }
  nav#toc li.depth-3 { padding-left: 1.5rem; }
  main#content {
    min-width: 0;
    padding-top: 1.5rem;
  }
  main#content h1, main#content h2, main#content h3, main#content h4 {
    line-height: 1.25;
    margin-top: 1.75em;
    margin-bottom: 0.75em;
    scroll-margin-top: 5rem;
  }
  main#content h1 { font-size: 2rem; border-bottom: 1px solid var(--border); padding-bottom: 0.3em; }
  main#content h2 { font-size: 1.5rem; border-bottom: 1px solid var(--border); padding-bottom: 0.25em; }
  main#content h3 { font-size: 1.25rem; }
  main#content h4 { font-size: 1rem; }
  main#content p, main#content ul, main#content ol, main#content dl, main#content blockquote {
    margin: 0 0 1em;
  }
  main#content ul, main#content ol { padding-left: 1.5em; }
  main#content li + li { margin-top: 0.25em; }
  main#content a { color: var(--link); }
  main#content hr {
    border: none;
    border-top: 1px solid var(--border);
    margin: 2em 0;
  }
  main#content code {
    font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
    font-size: 0.875em;
    background: var(--code-bg);
    padding: 0.15em 0.35em;
    border-radius: 4px;
  }
  main#content pre {
    overflow: auto;
    background: var(--code-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
    line-height: 1.45;
  }
  main#content pre code {
    background: none;
    padding: 0;
    font-size: 0.8125rem;
  }
  main#content table {
    display: block;
    width: 100%;
    overflow: auto;
    border-collapse: collapse;
    margin: 1em 0;
    font-size: 0.9375rem;
  }
  main#content th, main#content td {
    border: 1px solid var(--border);
    padding: 0.45em 0.75em;
    text-align: left;
  }
  main#content th { background: var(--code-bg); font-weight: 600; }
  main#content blockquote {
    border-left: 4px solid var(--border);
    margin-left: 0;
    padding: 0.25em 0 0.25em 1em;
    color: var(--muted);
  }
  main#content img { max-width: 100%; height: auto; }
  .loading { color: var(--muted); font-style: italic; }
</style>
</head>
<body>
<header class="site">
  <div>
    <h1>TSVZ Format Specification</h1>
    <div class="meta">Version 1 · Draft</div>
  </div>
  <a class="raw" href="/?format=md">Raw markdown</a>
</header>
<div class="layout">
  <nav id="toc" aria-label="Table of contents"><strong>Contents</strong><ul></ul></nav>
  <main id="content"><p class="loading">Loading specification…</p></main>
</div>
<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<script>
(function () {
  const md = __SPEC_JSON__;
  const content = document.getElementById("content");
  const tocRoot = document.querySelector("#toc ul");
  marked.setOptions({ gfm: true, breaks: false, headerIds: true, mangle: false });
  content.innerHTML = marked.parse(md);
  content.querySelectorAll("h2, h3, h4").forEach(function (heading) {
    if (!heading.id) {
      heading.id = heading.textContent.trim().toLowerCase()
        .replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
    }
    const depth = heading.tagName === "H2" ? 2 : heading.tagName === "H3" ? 3 : 4;
    const li = document.createElement("li");
    li.className = "depth-" + depth;
    const a = document.createElement("a");
    a.href = "#" + heading.id;
    a.textContent = heading.textContent;
    li.appendChild(a);
    tocRoot.appendChild(li);
  });
})();
</script>
</body>
</html>
"""


def build_html(spec_text: str) -> bytes:
    payload = json.dumps(spec_text)
    page = HTML_SHELL.replace("__SPEC_JSON__", payload)
    return page.encode("utf-8")


class SpecHandler(BaseHTTPRequestHandler):
    spec_bytes: bytes

    server_version = "TSVZSpec/1.0"

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("%s - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt % args))

    def _path_and_query(self) -> tuple[str, dict[str, list[str]]]:
        parsed = urlparse(self.path)
        return parsed.path, parse_qs(parsed.query)

    def _resolve_format(self) -> str:
        path, query = self._path_and_query()
        if path.endswith(".md"):
            return "md"
        fmt = query.get("format", [None])[0]
        headers = {k: v for k, v in self.headers.items()}
        if wants_markdown(
            self.headers.get("User-Agent", ""),
            self.headers.get("Accept", ""),
            headers,
            query_format=fmt,
        ):
            return "md"
        return "html"

    def _send(self, code: int, body: bytes, content_type: str, *, disposition: str | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        if disposition:
            self.send_header("Content-Disposition", disposition)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path, _ = self._path_and_query()
        if path not in ("/", "/index.html", "/tsvz-spec-v1.md", "/spec"):
            body = b"Not found"
            self._send(404, body, "text/plain; charset=utf-8")
            return

        fmt = self._resolve_format()
        if fmt == "md":
            self._send(
                200,
                self.spec_bytes,
                "text/markdown; charset=utf-8",
                disposition='inline; filename="tsvz-spec-v1.md"',
            )
        else:
            self._send(200, build_html(self.spec_bytes.decode("utf-8")), "text/html; charset=utf-8")

    def do_HEAD(self) -> None:
        path, _ = self._path_and_query()
        if path not in ("/", "/index.html", "/tsvz-spec-v1.md", "/spec"):
            self.send_response(404)
            self.end_headers()
            return
        fmt = self._resolve_format()
        if fmt == "md":
            self._send(200, b"", "text/markdown; charset=utf-8")
        else:
            self._send(200, b"", "text/html; charset=utf-8")


def load_spec() -> tuple[bytes, float]:
    if not SPEC_PATH.is_file():
        raise SystemExit("Spec file not found: %s" % SPEC_PATH)
    data = SPEC_PATH.read_bytes()
    return data, SPEC_PATH.stat().st_mtime


def make_handler(spec_bytes: bytes) -> type[SpecHandler]:
    class Handler(SpecHandler):
        pass

    Handler.spec_bytes = spec_bytes
    return Handler


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Serve the TSVZ spec as HTML or markdown.")
    parser.add_argument("-H", "--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="Port (default: %d)" % DEFAULT_PORT)
    args = parser.parse_args(argv)

    spec_bytes, _ = load_spec()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(spec_bytes))
    url = "http://%s:%d/" % (args.host if args.host != "0.0.0.0" else "127.0.0.1", args.port)
    print("Serving %s" % SPEC_PATH)
    print("  Browser:  %s" % url)
    print("  Markdown: curl %s" % url)
    print("  Explicit: %s?format=md  |  %s?format=html" % (url, url))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()


if __name__ == "__main__":
    main()
