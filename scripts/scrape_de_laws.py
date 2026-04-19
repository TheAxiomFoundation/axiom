"""Scrape the Delaware Code from delcode.delaware.gov.

Source layout
-------------
The Delaware Code Online is a static-ish HTML site hosted at
``https://delcode.delaware.gov``. The top-level index lists 31 titles at
``/title{N}/index.html`` (e.g. ``/title1/index.html``). Each title page lists
its chapters, e.g. ``/title1/c001/index.html``. Chapter slugs are usually
``c001`` .. ``c999``; some carry a letter suffix (``c020a``).

Two kinds of chapter pages appear:

1. **Leaf chapter.** The ``/title{N}/c{CCC}/index.html`` page contains the
   sections inline. Each section is::

       <div class="Section">
         <div class="SectionHead" id="{sec}">
           §  {sec}. {heading}.</div>
         <p class="subsection">{body paragraph 1}</p>
         <p class="subsection">{body paragraph 2}</p>
         ...
         {history links: <a href="https://legis.delaware.gov/...">...</a>}
       </div>

   The section id (``{sec}``) is typically numeric (``101``) but may carry a
   letter suffix (``101A``).

2. **Chapter split into subchapters.** The chapter index is then just a TOC
   of links to ``/title{N}/c{CCC}/sc{SC}/index.html`` pages, each of which
   follows the leaf chapter shape.

Output
------
AKN-3.0 XML files at
``{out}/statutes/title-{N}/ch-{CCC}/ch-{CCC}-sec-{section}.xml``
(or with a subchapter segment). Output shape matches what
``ingest_state_laws.py --state de`` expects.

Usage
-----
::

    uv run python scripts/scrape_de_laws.py --out /tmp/rules-us-de
    uv run python scripts/scrape_de_laws.py --out /tmp/rules-us-de --titles 1
    uv run python scripts/scrape_de_laws.py --out /tmp/rules-us-de --titles 1,8,30
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

BASE = "https://delcode.delaware.gov"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Titles 1..31 cover the whole Delaware Code.
DEFAULT_TITLES = tuple(range(1, 32))


def _http_get(url: str, retries: int = 3) -> str | None:
    """GET a URL as UTF-8 text. Return None on 404. Raise on other errors."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                # Site declares cp16 in <meta> but serves UTF-8; fall back if needed.
                try:
                    return raw.decode("utf-8")
                except UnicodeDecodeError:
                    return raw.decode("cp1252", errors="replace")
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
    """Strip HTML, normalize whitespace, decode entities."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span|li)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n\n", s).strip()
    return s


# Find a chapter link on a title-index page. Accepts c001, c020a, etc.
_CHAPTER_LINK = re.compile(
    r'href="[^"]*/title(?P<title>\d+)/c(?P<chapter>[0-9a-z]+)/index\.html"',
    re.IGNORECASE,
)

# Find a subchapter link on a chapter-index page.
_SUBCHAPTER_LINK = re.compile(
    r'href="[^"]*/title(?P<title>\d+)/c(?P<chapter>[0-9a-z]+)'
    r'/sc(?P<sub>[0-9a-z]+)/index\.html"',
    re.IGNORECASE,
)

# One complete <div class="Section">...</div> holding one statute section.
# Use a non-greedy capture stopping at the *next* Section div or </div>\n        </div>.
_SECTION_BLOCK = re.compile(
    r'<div\s+class="Section"\s*>(.*?)(?=<div\s+class="Section"\s*>|</div>\s*</div>\s*</div>)',
    re.DOTALL | re.IGNORECASE,
)

# SectionHead carries the section number + heading.
_SECTION_HEAD = re.compile(
    r'<div\s+class="SectionHead"\s+id="(?P<id>[^"]+)"\s*>(?P<inner>.*?)</div>',
    re.DOTALL | re.IGNORECASE,
)


def list_title_chapters(title_num: int) -> list[str]:
    """Return chapter slugs (e.g. ``c001``, ``c020a``) in title order."""
    url = f"{BASE}/title{title_num}/index.html"
    html = _http_get(url)
    if html is None:
        return []
    slugs: list[str] = []
    seen: set[str] = set()
    for m in _CHAPTER_LINK.finditer(html):
        if int(m.group("title")) != title_num:
            continue
        slug = f"c{m.group('chapter').lower()}"
        if slug not in seen:
            seen.add(slug)
            slugs.append(slug)
    return slugs


def list_chapter_subchapters(title_num: int, chapter: str) -> list[str]:
    """Return subchapter slugs (e.g. ``sc01``) if the chapter is split."""
    url = f"{BASE}/title{title_num}/{chapter}/index.html"
    html = _http_get(url)
    if html is None:
        return []
    # If chapter page has sections inline, it's a leaf chapter (no subchapters).
    if re.search(r'<div\s+class="SectionHead"', html, re.IGNORECASE):
        return []
    slugs: list[str] = []
    seen: set[str] = set()
    for m in _SUBCHAPTER_LINK.finditer(html):
        if (
            int(m.group("title")) != title_num
            or m.group("chapter").lower() != chapter[1:]
        ):
            continue
        slug = f"sc{m.group('sub').lower()}"
        if slug not in seen:
            seen.add(slug)
            slugs.append(slug)
    return slugs


def parse_sections(html: str) -> list[tuple[str, str, str]]:
    """Return ``(section_num, heading, body)`` for each section in ``html``."""
    out: list[tuple[str, str, str]] = []
    for block_m in _SECTION_BLOCK.finditer(html):
        block = block_m.group(1)
        head_m = _SECTION_HEAD.search(block)
        if not head_m:
            continue
        section_id = head_m.group("id").strip()
        head_inner = _clean_text(head_m.group("inner"))
        # Head text looks like: "§ 101. Designation and citation of Code."
        # Strip the leading "§ <num>." prefix and the trailing ".".
        heading = re.sub(
            r"^\s*§\s*" + re.escape(section_id) + r"\s*\.\s*",
            "",
            head_inner,
        ).strip().rstrip(".")

        # Body: everything after the SectionHead, minus the history anchors.
        body_html = block[head_m.end():]
        # Drop trailing history anchors (`<a href="...SessionLaws...">`) with
        # surrounding text like ``1 Del. C. 1953, § 101;`` and semicolons.
        # The subsection paragraphs are what we keep; history appears *after*
        # the last </p>. Split at the last </p> and keep only up to it.
        last_p = body_html.rfind("</p>")
        if last_p != -1:
            body_html = body_html[: last_p + len("</p>")]
        # Otherwise (section with no <p>), strip all <a> tags to suppress
        # history links before _clean_text runs.
        else:
            body_html = re.sub(
                r'<a\s[^>]*>.*?</a>', "", body_html, flags=re.DOTALL | re.IGNORECASE
            )
        body = _clean_text(body_html)
        # Some sections include a bracketed "[Repealed]" marker only.
        out.append((section_id, heading, body))
    return out


def _safe_segment(s: str) -> str:
    """Make a string filesystem-safe."""
    return re.sub(r"[^0-9A-Za-z_-]+", "_", s)


def build_akn_xml(
    title_num: int,
    chapter: str,
    section: str,
    heading: str,
    body: str,
) -> str:
    """AKN-3.0 XML for one Delaware Code section."""
    citation = f"{title_num} Del. C. § {section}"
    safe_chapter = _safe_segment(chapter)
    safe_section = _safe_segment(section)
    eid = f"sec_t{title_num}_{safe_chapter}_{safe_section}"
    paras = [p for p in re.split(r"\n\n+", body) if p.strip()]
    paras_xml = "\n            ".join(
        f"<p>{xml_escape(p)}</p>" for p in paras
    ) or "<p/>"
    frbr_path = f"/akn/us-de/act/delcode/{title_num}/{section}"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="{AKN_NS}">
  <act name="section">
    <meta>
      <identification source="#axiom">
        <FRBRWork>
          <FRBRthis value="{frbr_path}"/>
          <FRBRuri value="{frbr_path}"/>
          <FRBRauthor href="#de-legislature"/>
          <FRBRcountry value="us-de"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="Del. C."/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="{frbr_path}/eng@2026-01-01"/>
          <FRBRuri value="{frbr_path}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="{frbr_path}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="{frbr_path}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="de-legislature" href="https://legis.delaware.gov" showAs="Delaware General Assembly"/>
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


def _write_section(
    out_root: Path,
    title_num: int,
    chapter: str,
    section: str,
    heading: str,
    body: str,
) -> bool:
    """Write one section XML. Return True if written, False if skipped (no body)."""
    if not body:
        return False
    xml = build_akn_xml(title_num, chapter, section, heading, body)
    safe_chapter = _safe_segment(chapter)
    safe_section = _safe_segment(section)
    dest = (
        out_root
        / "statutes"
        / f"title-{title_num}"
        / f"ch-{safe_chapter}"
        / f"ch-{safe_chapter}-sec-{safe_section}.xml"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(xml, encoding="utf-8")
    return True


def scrape_chapter(
    title_num: int, chapter: str, out_root: Path
) -> tuple[int, int, str]:
    """Scrape one chapter (handling subchapters). Return (ok, skipped, label)."""
    label = f"t{title_num}-{chapter}"
    chapter_url = f"{BASE}/title{title_num}/{chapter}/index.html"
    try:
        html = _http_get(chapter_url)
    except RuntimeError as exc:
        print(f"  WARN {label}: {exc}", file=sys.stderr)
        return (0, 0, label)
    if html is None:
        return (0, 0, label)

    ok = 0
    skipped = 0

    # If the chapter page has sections inline, scrape them.
    sections = parse_sections(html)
    for section, heading, body in sections:
        if _write_section(out_root, title_num, chapter, section, heading, body):
            ok += 1
        else:
            skipped += 1

    # If no sections found inline, look for subchapter pages.
    if not sections:
        subs = list_chapter_subchapters(title_num, chapter)
        for sub in subs:
            sub_url = f"{BASE}/title{title_num}/{chapter}/{sub}/index.html"
            try:
                sub_html = _http_get(sub_url)
            except RuntimeError as exc:
                print(f"  WARN {label}/{sub}: {exc}", file=sys.stderr)
                continue
            if sub_html is None:
                continue
            for section, heading, body in parse_sections(sub_html):
                if _write_section(
                    out_root, title_num, chapter, section, heading, body
                ):
                    ok += 1
                else:
                    skipped += 1
    return (ok, skipped, label)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-de"))
    parser.add_argument(
        "--titles",
        default="",
        help="Comma-separated title numbers (e.g. '1,8,30'). Default: all (1-31).",
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="Parallel chapter fetches."
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Stop after N chapters."
    )
    args = parser.parse_args(argv)

    titles: list[int]
    if args.titles:
        titles = sorted(
            {int(t.strip()) for t in args.titles.split(",") if t.strip()}
        )
    else:
        titles = list(DEFAULT_TITLES)

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)

    # Build the full (title, chapter) worklist.
    jobs: list[tuple[int, str]] = []
    for tnum in titles:
        chapters = list_title_chapters(tnum)
        if not chapters:
            print(f"  WARN title {tnum}: no chapters found", file=sys.stderr)
            continue
        for ch in chapters:
            jobs.append((tnum, ch))

    if args.limit:
        jobs = jobs[: args.limit]
    print(f"Scraping {len(jobs)} chapters across {len(titles)} titles", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_chapter, tnum, ch, args.out): (tnum, ch)
            for (tnum, ch) in jobs
        }
        for fut in as_completed(futures):
            ok, skipped, label = fut.result()
            total_ok += ok
            total_skipped += skipped
            if ok + skipped > 0:
                elapsed = (time.time() - started) / 60
                print(
                    f"  {label}: {ok} ok, {skipped} skip  "
                    f"(running: {total_ok} ok / {total_skipped} skip, "
                    f"{elapsed:.1f} min)",
                    flush=True,
                )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE - {total_ok} sections scraped, "
        f"{total_skipped} skipped, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
