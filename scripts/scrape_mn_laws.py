"""Scrape the Minnesota Statutes from revisor.mn.gov.

Source layout
-------------
The Minnesota Statutes live at ``https://www.revisor.mn.gov/statutes/``.
The index has three layers:

1. Root ``/statutes/`` lists "parts" (ranges of chapters by topic), each linking
   to a part page at ``/statutes/part/<NAME>``.
2. Each part page lists its chapters as ``/statutes/cite/<chapter>`` links
   (chapter is a number, sometimes with a letter suffix like ``2A``).
3. Each chapter page ``/statutes/cite/<chapter>`` is a "Table of Sections"
   whose rows link to individual section pages ``/statutes/cite/<section>``
   where section is ``<chapter>.<nn>`` (e.g. ``1.01``, ``1.0431``).

Each section page contains a block like::

    <div class="section" id="stat.1.01">
      <h1 class="shn">1.01 EXTENT.</h1>
      <p>The sovereignty and jurisdiction of this state ...</p>
    </div>
    <div class="history" id="stat.1.01.history...">
      <h2>History: </h2>
      <p class="first">(1) <a href="...">RL s 1</a>; ...</p>
    </div>

Sections with subdivisions contain nested ``<div class="subd">`` wrappers
with ``<h2 class="subd_no">Subd. N.<span class="headnote">...</span></h2>``.

Repealed sections render as ``<div class="sr" id="stat.X.Y">...</div>`` and
carry no body; they are skipped.

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state mn`` expects.

Usage
-----
::

    uv run python scripts/scrape_mn_laws.py --out /tmp/rules-us-mn
    uv run python scripts/scrape_mn_laws.py --out /tmp/rules-us-mn --chapters 1,2
"""

from __future__ import annotations

import argparse
import html as _html
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

BASE = "https://www.revisor.mn.gov"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> str | None:
    """GET a URL. Return None on 404, decoded text otherwise."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace."""
    # Drop pilcrow permalinks the site inserts before every subdivision.
    s = re.sub(
        r'<a[^>]*class="permalink"[^>]*>.*?</a>',
        "",
        s,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # Give headnote spans a leading space so "Subd. 1." meets "Title." cleanly.
    s = re.sub(
        r'<span\s+class="headnote"[^>]*>',
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|li|h[1-6])>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n[ \t]*", "\n", s)
    s = re.sub(r"\n\n+", "\n\n", s).strip()
    return s


def list_parts() -> list[str]:
    """Return the list of part URLs from the root statutes index."""
    html = _http_get(f"{BASE}/statutes/")
    if html is None:
        raise RuntimeError("statutes root 404")
    # Part links: /statutes/part/<NAME> (absolute or relative).
    urls = re.findall(
        r'href="(https?://www\.revisor\.mn\.gov/statutes/part/[^"]+)"', html
    )
    return sorted(set(urls))


def list_chapters() -> list[str]:
    """Return the full list of chapter tokens (e.g. ``1``, ``2A``, ``13D``)."""
    chapters: set[str] = set()
    for part_url in list_parts():
        html = _http_get(part_url)
        if html is None:
            continue
        # Chapter links look like /statutes/cite/1, /statutes/cite/2A.
        # Avoid section links like /statutes/cite/1.01 (contain a dot).
        for m in re.finditer(
            r'href="(?:https?://www\.revisor\.mn\.gov)?/statutes/cite/([0-9]+[A-Z]?)"',
            html,
        ):
            chapters.add(m.group(1))
    # Sort numerically-then-alpha: 1, 2, 2A, 3, ...
    def _sort_key(tok: str) -> tuple[int, str]:
        m = re.match(r"(\d+)([A-Z]*)", tok)
        return (int(m.group(1)), m.group(2)) if m else (0, tok)

    return sorted(chapters, key=_sort_key)


def list_sections(chapter: str) -> list[str]:
    """Return the section tokens (e.g. ``1.01``) linked from a chapter page."""
    url = f"{BASE}/statutes/cite/{chapter}"
    html = _http_get(url)
    if html is None:
        return []
    # Section links: /statutes/cite/<chapter>.<nn>.
    # Escape regex special chars in chapter token (just letters + digits here).
    pat = rf'href="(?:https?://www\.revisor\.mn\.gov)?/statutes/cite/({re.escape(chapter)}\.[0-9A-Za-z]+)"'
    found = re.findall(pat, html)
    # De-dup while keeping order.
    seen: set[str] = set()
    ordered: list[str] = []
    for s in found:
        if s not in seen:
            seen.add(s)
            ordered.append(s)
    return ordered


# Section body: <div class="section" id="stat.<section>">...</div>.
# A "section" div spans one top-level <div class="section"> up to, but not
# including, the following <div class="history"> (or end of #xtend).
_SECTION_BLOCK = re.compile(
    r'<div\s+class="section"\s+id="stat\.(?P<section>[0-9A-Za-z.]+)"\s*>(?P<body>.*?)</div>\s*(?=<div\s+class="(?:history|sr)"|</div>\s*(?:<!--|$))',
    re.DOTALL,
)
# Repealed-section marker used where the whole section is struck.
_REPEALED_BLOCK = re.compile(
    r'<div\s+class="sr"\s+id="stat\.(?P<section>[0-9A-Za-z.]+)"\s*>.*?</div>',
    re.DOTALL,
)
# Subdivision block inside a section.
_SUBD_BLOCK = re.compile(
    r'<div\s+class="subd"[^>]*>(?P<body>.*?)</div>',
    re.DOTALL,
)


def parse_section_page(html: str, section: str) -> tuple[str, str] | None:
    """Return ``(heading, body)`` for ``section``, or ``None`` if not present.

    Body is newline-joined paragraphs (subdivisions appear as their own
    paragraph groups preceded by the ``Subd. N. Headnote.`` marker).
    """
    # Repealed — skip.
    if _REPEALED_BLOCK.search(html):
        return None

    m = _SECTION_BLOCK.search(html)
    if not m:
        # Fallback: looser match anchored on the id attribute only.
        m2 = re.search(
            rf'<div\s+class="section"\s+id="stat\.{re.escape(section)}"\s*>(.*?)(?=<div\s+class="history"|</div>\s*</div>\s*<)',
            html,
            re.DOTALL,
        )
        if not m2:
            return None
        body_html = m2.group(1)
    else:
        body_html = m.group("body")

    # Pull the heading out of <h1 class="shn">1.01 EXTENT.</h1>.
    shn_m = re.search(r'<h1\s+class="shn"[^>]*>(.*?)</h1>', body_html, re.DOTALL)
    heading = ""
    if shn_m:
        raw = _clean_text(shn_m.group(1))
        # Remove the leading section number (e.g. "1.01 ").
        raw = re.sub(rf"^{re.escape(section)}\s*", "", raw)
        heading = raw.rstrip(".").strip()
        # Drop the <h1> from body_html.
        body_html = body_html[: shn_m.start()] + body_html[shn_m.end() :]

    # Handle subdivisions: replace each <div class="subd"> with a labeled block.
    def _subd_repl(match: re.Match[str]) -> str:
        inner = match.group("body")
        hdr_m = re.search(
            r'<h2\s+class="subd_no"[^>]*>(.*?)</h2>', inner, re.DOTALL
        )
        hdr_text = _clean_text(hdr_m.group(1)) if hdr_m else ""
        body_inner = (
            inner[: hdr_m.start()] + inner[hdr_m.end() :] if hdr_m else inner
        )
        body_inner_clean = _clean_text(body_inner)
        if hdr_text and body_inner_clean:
            return f"\n\n{hdr_text}\n{body_inner_clean}"
        if body_inner_clean:
            return f"\n\n{body_inner_clean}"
        return ""

    body_html = _SUBD_BLOCK.sub(_subd_repl, body_html)

    body = _clean_text(body_html)
    # Normalize double blank lines.
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    return heading, body


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"Minn. Stat. \u00a7 {section}"
    safe_section = section.replace(".", "_").replace("-", "_")
    eid = f"sec_{chapter}_{safe_section}"
    paras = [p for p in re.split(r"\n\n+", body) if p.strip()]
    paras_xml = "\n            ".join(
        f"<p>{xml_escape(p)}</p>" for p in paras
    ) or "<p/>"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="{AKN_NS}">
  <act name="section">
    <meta>
      <identification source="#axiom">
        <FRBRWork>
          <FRBRthis value="/akn/us-mn/act/statutes/{section}"/>
          <FRBRuri value="/akn/us-mn/act/statutes/{section}"/>
          <FRBRauthor href="#mn-legislature"/>
          <FRBRcountry value="us-mn"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="Minn. Stat."/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-mn/act/statutes/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-mn/act/statutes/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-mn/act/statutes/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-mn/act/statutes/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="mn-legislature" href="https://www.revisor.mn.gov" showAs="Minnesota Legislature"/>
        <TLCOrganization eId="axiom" href="https://axiom-foundation.org" showAs="Axiom Foundation"/>
      </references>
    </meta>
    <body>
      <section eId="{eid}">
        <num>{xml_escape(citation)}</num>
        <heading>{xml_escape(heading or f"Section {section}")}</heading>
        <content>
            {paras_xml}
        </content>
      </section>
    </body>
  </act>
</akomaNtoso>
"""


def scrape_chapter(
    chapter: str, out_root: Path
) -> tuple[int, int, str]:
    """Scrape one chapter. Return ``(ok, skipped, chapter_token)``."""
    try:
        sections = list_sections(chapter)
    except RuntimeError as exc:
        print(f"  WARN ch-{chapter} (TOC): {exc}", file=sys.stderr)
        return (0, 0, chapter)
    if not sections:
        return (0, 0, chapter)

    ok = 0
    skipped = 0
    for section in sections:
        url = f"{BASE}/statutes/cite/{section}"
        try:
            html = _http_get(url)
        except RuntimeError as exc:
            print(f"  WARN {section}: {exc}", file=sys.stderr)
            skipped += 1
            continue
        if html is None:  # 404 → skip silently.
            skipped += 1
            continue
        parsed = parse_section_page(html, section)
        if parsed is None:
            skipped += 1
            continue
        heading, body = parsed
        if not body:
            skipped += 1
            continue
        xml = build_akn_xml(chapter, section, heading, body)
        safe_section = section.replace("/", "_")
        dest = (
            out_root
            / "statutes"
            / f"ch-{chapter}"
            / f"ch-{chapter}-sec-{safe_section}.xml"
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(xml, encoding="utf-8")
        ok += 1
    return (ok, skipped, chapter)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-mn"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '1,2,2A').",
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="Parallel chapter fetches."
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Stop after N chapters."
    )
    args = parser.parse_args(argv)

    chapter_filter: set[str] | None = None
    if args.chapters:
        chapter_filter = {
            c.strip() for c in args.chapters.split(",") if c.strip()
        }

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)

    if chapter_filter:
        chapters = sorted(chapter_filter)
    else:
        chapters = list_chapters()
    if args.limit:
        chapters = chapters[: args.limit]
    print(f"Scraping {len(chapters)} chapters", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_chapter, ch, args.out): ch for ch in chapters
        }
        for fut in as_completed(futures):
            ok, skipped, ch = fut.result()
            total_ok += ok
            total_skipped += skipped
            if ok + skipped > 0:
                elapsed = (time.time() - started) / 60
                print(
                    f"  ch-{ch}: {ok} ok, {skipped} skip  "
                    f"(running: {total_ok} ok / {total_skipped} skip, {elapsed:.1f} min)",
                    flush=True,
                )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE — {total_ok} sections scraped, "
        f"{total_skipped} skipped, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
