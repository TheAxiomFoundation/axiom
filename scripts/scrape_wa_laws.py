"""Scrape the Washington Revised Code (RCW) from app.leg.wa.gov/RCW.

Source layout
-------------
The RCW is served as an ASP.NET site at
``https://app.leg.wa.gov/RCW/default.aspx`` with three nested pages keyed off
the ``cite`` query string:

  * Index (``/RCW/``): links ``default.aspx?Cite=<title>`` for each title token
    (e.g. ``1``, ``9A``, ``28A``).
  * Title page (``?Cite=<title>``): block
    ``<div id='contentWrapper' class='title-page'>`` listing chapter tokens as
    ``<a href='...cite=1.04'>1.04</a>`` plus a human-readable description.
  * Chapter page (``?cite=<chapter>``): block
    ``<div id='contentWrapper' class='chapter-page'>`` listing section tokens
    as ``<a href='...cite=1.04.010'>1.04.010</a>`` with headings.
  * Section page (``?cite=<section>``): ``<title>RCW <section>: <heading></title>``
    and body inside ``<div id='contentWrapper' class='section-page'>...</div>``.
    Trailing ``[ ... ]`` block carries session law history; skipped from body.

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state wa`` expects.

Usage
-----
::

    uv run python scripts/scrape_wa_laws.py --out /tmp/rules-us-wa
    uv run python scripts/scrape_wa_laws.py --out /tmp/rules-us-wa --chapters 1.04,9A.04
    uv run python scripts/scrape_wa_laws.py --out /tmp/rules-us-wa --titles 9A --limit 10
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

BASE = "https://app.leg.wa.gov/RCW"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> str:
    """GET a URL decoded as UTF-8 with a cp1252 fallback."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                try:
                    return raw.decode("utf-8")
                except UnicodeDecodeError:
                    return raw.decode("cp1252", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(min(8.0, 2.0**attempt))
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span|li)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# Matches the content wrapper block on any RCW page.
_CONTENT_WRAPPER = re.compile(
    r"<div\s+id=['\"]contentWrapper['\"][^>]*>(?P<body>.*?)</div>\s*"
    r"<div\s+id=['\"]ContentPlaceHolder1_pnlExpanded['\"]",
    re.DOTALL | re.IGNORECASE,
)

# Missing-citation pages render "Citation not found" instead of a contentWrapper.
_CITATION_NOT_FOUND = re.compile(r"Citation\s+not\s+found", re.IGNORECASE)


def _extract_content_wrapper(html: str) -> str | None:
    m = _CONTENT_WRAPPER.search(html)
    return m.group("body") if m else None


def list_titles() -> list[str]:
    """Return all RCW title tokens from the index page (e.g. '1', '9A', '28A')."""
    html = _http_get(f"{BASE}/")
    # Only pull links from the titles listing (ContentPlaceHolder1_dgSections).
    # Fallback: scan the whole HTML for ``default.aspx?Cite=<token>`` links, which
    # are unique to the titles table.
    m = re.search(
        r'<table[^>]*id="ContentPlaceHolder1_dgSections"[^>]*>(.*?)</table>',
        html,
        re.DOTALL,
    )
    scope = m.group(1) if m else html
    tokens = re.findall(r'default\.aspx\?Cite=([0-9]+[A-Z]?)"', scope)
    # Preserve order, de-dupe.
    seen: set[str] = set()
    out: list[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def list_chapters_for_title(title: str) -> list[str]:
    """Return chapter tokens for a title, e.g. '1.04', '9A.04'."""
    url = f"{BASE}/default.aspx?Cite={urllib.parse.quote(title)}"
    html = _http_get(url)
    body = _extract_content_wrapper(html)
    if body is None:
        return []
    # Chapter links: href='...cite=1.04'.
    tokens = re.findall(
        r"default\.aspx\?cite=([0-9]+[A-Z]?\.[0-9]+[A-Z]?)(?=['\"])",
        body,
        re.IGNORECASE,
    )
    # Keep only chapters that belong to this title (guard against Notes).
    prefix = f"{title}."
    seen: set[str] = set()
    out: list[str] = []
    for t in tokens:
        if not t.startswith(prefix):
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def list_sections_for_chapter(chapter: str) -> list[str]:
    """Return section tokens for a chapter, e.g. '1.04.010'."""
    url = f"{BASE}/default.aspx?cite={urllib.parse.quote(chapter)}"
    html = _http_get(url)
    body = _extract_content_wrapper(html)
    if body is None:
        return []
    # Section links: href='...cite=1.04.010' (three-segment citation).
    tokens = re.findall(
        r"default\.aspx\?cite=([0-9]+[A-Z]?\.[0-9]+[A-Z]?\.[0-9A-Za-z]+)"
        r"(?=['\"&])",
        body,
        re.IGNORECASE,
    )
    prefix = f"{chapter}."
    seen: set[str] = set()
    out: list[str] = []
    for t in tokens:
        if not t.startswith(prefix):
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def fetch_section(section: str) -> tuple[str, str, str] | None:
    """Fetch a section page; return (section, heading, body) or None if missing."""
    url = f"{BASE}/default.aspx?cite={urllib.parse.quote(section)}"
    try:
        html = _http_get(url)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    except RuntimeError as exc:
        print(f"  WARN {section}: {exc}", file=sys.stderr)
        return None

    if _CITATION_NOT_FOUND.search(html):
        return None

    # Heading lives in <h2><!-- field: CaptionsTitles -->{heading}<!-- field: --></h2>
    # right below the citation banner. The <title> tag only carries "RCW {section}:"
    # with an empty trailing segment, so prefer the h2 source.
    heading = ""
    head_m = re.search(
        r"<h2>\s*<!--\s*field:\s*CaptionsTitles\s*-->(.*?)<!--\s*field:",
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if head_m:
        heading = _clean_text(head_m.group(1))
    if not heading:
        # Fallback: pull whatever the <title> tag says after the colon.
        tm = re.search(r"<title[^>]*>\s*(.*?)\s*</title>", html, re.DOTALL)
        if tm:
            tt = _clean_text(tm.group(1))
            if ":" in tt:
                heading = tt.split(":", 1)[1].strip()
    heading = heading.rstrip(".")

    # Guard against the page actually being a chapter index (if the cite the
    # server resolves to is two-segment) before we extract the wrapper body.
    if "class='section-page'" not in html and 'class="section-page"' not in html:
        return None

    body_html = _extract_content_wrapper(html)
    if body_html is None:
        return None

    # Drop the trailing session-law brackets: ``[ 1951 c 5 s 2; ... ]`` and the
    # "Notes:" heading plus everything after it. History and cross-reference
    # annotations aren't statute text.
    body_html = re.split(r"<h3[^>]*>\s*Notes:\s*</h3>", body_html, maxsplit=1)[0]
    body_html = re.sub(
        r"<div[^>]*style=\"[^\"]*margin-top:\s*15pt[^\"]*\"[^>]*>\s*\[.*?\]\s*</div>",
        "",
        body_html,
        flags=re.DOTALL,
    )
    # Fallback: remove any remaining standalone bracketed session-law lines.
    body_html = re.sub(r"\[\s*(?:19|20)\d{2}[^\[\]]*?\]", "", body_html)

    # Split paragraphs on <div> / <p> boundaries so paragraph structure survives.
    paragraphs: list[str] = []
    for chunk in re.split(r"</(?:div|p)>", body_html, flags=re.IGNORECASE):
        text = _clean_text(chunk)
        if text:
            paragraphs.append(text)
    body = "\n\n".join(paragraphs).strip()
    if not body:
        return None
    return (section, heading, body)


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"RCW {section}"
    safe_section = section.replace(".", "_").replace("-", "_")
    safe_chapter = chapter.replace(".", "_").replace("-", "_")
    eid = f"sec_{safe_chapter}_{safe_section}"
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
          <FRBRthis value="/akn/us-wa/act/rcw/{section}"/>
          <FRBRuri value="/akn/us-wa/act/rcw/{section}"/>
          <FRBRauthor href="#wa-legislature"/>
          <FRBRcountry value="us-wa"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="RCW"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-wa/act/rcw/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-wa/act/rcw/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-wa/act/rcw/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-wa/act/rcw/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="wa-legislature" href="https://leg.wa.gov" showAs="Washington State Legislature"/>
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


def _title_of_chapter(chapter: str) -> str:
    """'1.04' -> '1', '9A.04' -> '9A'."""
    return chapter.split(".", 1)[0]


def scrape_section(
    chapter: str, section: str, out_root: Path
) -> tuple[int, int, str]:
    """Fetch and write one section. Return (ok, skipped, section)."""
    result = fetch_section(section)
    if result is None:
        return (0, 1, section)
    _, heading, body = result
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
    return (1, 0, section)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-wa"))
    parser.add_argument(
        "--titles",
        default="",
        help="Comma-separated title tokens (e.g. '1,9A'). Default: all.",
    )
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '1.04,9A.04'). "
        "Overrides --titles if set.",
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="Parallel section fetches."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N sections written (for smoke tests).",
    )
    args = parser.parse_args(argv)

    title_filter: set[str] | None = None
    if args.titles:
        title_filter = {t.strip() for t in args.titles.split(",") if t.strip()}

    chapter_filter: set[str] | None = None
    if args.chapters:
        chapter_filter = {c.strip() for c in args.chapters.split(",") if c.strip()}

    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()

    # Build the (chapter, section) work list.
    chapters: list[str] = []
    if chapter_filter:
        chapters = sorted(chapter_filter)
        print(f"Using {len(chapters)} chapters from --chapters", flush=True)
    else:
        print("Listing titles...", flush=True)
        titles = list_titles()
        if title_filter:
            titles = [t for t in titles if t in title_filter]
        print(f"  {len(titles)} titles", flush=True)
        for title in titles:
            try:
                ch = list_chapters_for_title(title)
            except RuntimeError as exc:
                print(f"  WARN title {title}: {exc}", file=sys.stderr)
                continue
            chapters.extend(ch)
        print(f"  {len(chapters)} chapters total", flush=True)

    work: list[tuple[str, str]] = []
    for chapter in chapters:
        try:
            sections = list_sections_for_chapter(chapter)
        except RuntimeError as exc:
            print(f"  WARN chapter {chapter}: {exc}", file=sys.stderr)
            continue
        for s in sections:
            work.append((chapter, s))
            if args.limit and len(work) >= args.limit:
                break
        if args.limit and len(work) >= args.limit:
            break

    print(f"Scraping {len(work)} sections", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_section, ch, sec, args.out): (ch, sec)
            for (ch, sec) in work
        }
        for fut in as_completed(futures):
            ok, skipped, sec = fut.result()
            total_ok += ok
            total_skipped += skipped
            if (total_ok + total_skipped) % 25 == 0 or ok + skipped > 0:
                elapsed = (time.time() - started) / 60
                print(
                    f"  sec {sec}: {ok} ok, {skipped} skip  "
                    f"(running: {total_ok} ok / {total_skipped} skip, "
                    f"{elapsed:.1f} min)",
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
