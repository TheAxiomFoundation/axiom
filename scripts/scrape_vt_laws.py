"""Scrape the Vermont Statutes Annotated from legislature.vermont.gov.

Source layout
-------------
The VSA is served on the Vermont Legislature SilverStripe site:

    https://legislature.vermont.gov/statutes/                         (title index)
    https://legislature.vermont.gov/statutes/title/{T}                (chapter list)
    https://legislature.vermont.gov/statutes/chapter/{T}/{C}          (section list)
    https://legislature.vermont.gov/statutes/section/{T}/{C}/{S}      (full section)

Title tokens look like ``01``, ``32``, ``09A``, ``10APPENDIX``. Chapters are
zero-padded numerics (``151``, ``001``). Section tokens are zero-padded with an
optional trailing letter (``05811``, ``05825a``, ``05862c``). Section pages have
a ``<b>(Cite as: 32 V.S.A. § 5811)</b>`` marker immediately above a
``<ul class="item-list statutes-detail">`` block containing the section body in
``<p>`` tags. The trailing history note ``(Added 1966, No. 61 ...)`` is part of
the last paragraph of the body.

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{title}/ch-{title}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state vt`` expects. Citation uses
the canonical ``{title} V.S.A. § {section}`` form.

Usage
-----
::

    uv run python scripts/scrape_vt_laws.py --out /tmp/rules-us-vt
    uv run python scripts/scrape_vt_laws.py --out /tmp/rules-us-vt --titles 32
    uv run python scripts/scrape_vt_laws.py --out /tmp/rules-us-vt --titles 32 --chapters 151 --limit-sections 5
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

BASE = "https://legislature.vermont.gov"
ROOT = f"{BASE}/statutes/"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# HTTP status codes that mean "gone/missing" — skip quietly, don't retry.
_SKIP_STATUSES = {307, 404, 410}


def _http_get(url: str, retries: int = 3) -> str | None:
    """GET a URL as UTF-8. Returns None on any HTTP/URL error (SOFT-FAIL)."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code in _SKIP_STATUSES:
                return None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(min(8.0, 2.0**attempt))
    print(f"  WARN GET {url}: {last_exc}", file=sys.stderr)
    return None


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace, preserve paragraph breaks."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    # Preserve paragraph/list-item boundaries as blank lines so callers can
    # split on ``\n\n+`` to get one ``<p>`` per source paragraph.
    s = re.sub(r"</(p|li|tr|div)>", "\n\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(td|span)>", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    # Collapse runs of spaces/tabs but keep newlines.
    s = re.sub(r"[ \t]+", " ", s)
    # Normalize line-internal whitespace but preserve paragraph breaks.
    s = re.sub(r" *\n *", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


_TITLE_LINK = re.compile(
    r'href="/?statutes/title/(?P<title>[0-9A-Za-z]+)"',
)
_CHAPTER_LINK = re.compile(
    r'href="/?statutes/chapter/(?P<title>[0-9A-Za-z]+)/(?P<chapter>[0-9A-Za-z]+)"',
)
_SECTION_LINK = re.compile(
    r'href="/?statutes/section/'
    r'(?P<title>[0-9A-Za-z]+)/(?P<chapter>[0-9A-Za-z]+)/(?P<section>[0-9A-Za-z]+)"',
)


def list_titles() -> list[str]:
    """Return title tokens from the statutes root page."""
    html = _http_get(ROOT)
    if html is None:
        return []
    seen: list[str] = []
    got: set[str] = set()
    for m in _TITLE_LINK.finditer(html):
        tok = m.group("title")
        if tok in got:
            continue
        got.add(tok)
        seen.append(tok)
    return seen


def list_chapters(title: str) -> list[str]:
    """Return chapter tokens for a title."""
    html = _http_get(f"{BASE}/statutes/title/{title}")
    if html is None:
        return []
    seen: list[str] = []
    got: set[str] = set()
    for m in _CHAPTER_LINK.finditer(html):
        if m.group("title") != title:
            continue
        tok = m.group("chapter")
        if tok in got:
            continue
        got.add(tok)
        seen.append(tok)
    return seen


def list_sections(title: str, chapter: str) -> list[str]:
    """Return section tokens for a chapter."""
    html = _http_get(f"{BASE}/statutes/chapter/{title}/{chapter}")
    if html is None:
        return []
    seen: list[str] = []
    got: set[str] = set()
    for m in _SECTION_LINK.finditer(html):
        if m.group("title") != title or m.group("chapter") != chapter:
            continue
        tok = m.group("section")
        if tok in got:
            continue
        got.add(tok)
        seen.append(tok)
    return seen


# Section page shape: citation marker, then <ul class="item-list statutes-detail">
# containing <li>...<p>body paragraphs...</p>...</li>.
_CITE_RE = re.compile(
    r"\(\s*Cite as\s*:\s*(?P<title>[0-9A-Za-z]+)\s+V\.?S\.?A\.?\s*§+\s*"
    r"(?P<section>[0-9A-Za-z\-]+)\s*\)",
    re.IGNORECASE,
)
_DETAIL_UL_RE = re.compile(
    r'<ul[^>]*class="[^"]*statutes-detail[^"]*"[^>]*>(?P<inner>.*?)</ul>',
    re.DOTALL | re.IGNORECASE,
)
# Leading "§ 5811. Definitions" paragraph carries the heading.
_HEADING_RE = re.compile(
    r"<p[^>]*>\s*(?:<b>\s*)?§+\s*[0-9A-Za-z\-]+\s*\.\s*"
    r"(?P<heading>[^<]*)"
    r"(?:</b>\s*)?</p>",
    re.IGNORECASE,
)


def parse_section_page(html: str) -> tuple[str, str, str, str] | None:
    """Parse a section HTML page.

    Return ``(title, section, heading, body)`` or ``None`` if the page does not
    look like a normal section (no cite marker / no detail block).
    """
    cite = _CITE_RE.search(html)
    if not cite:
        return None
    title = cite.group("title")
    section = cite.group("section")

    m = _DETAIL_UL_RE.search(html)
    if not m:
        return None
    inner = m.group("inner")

    heading = ""
    head_m = _HEADING_RE.search(inner)
    if head_m:
        heading = _clean_text(head_m.group("heading")).rstrip(". ").strip()
        # Drop the heading paragraph from the body so we don't duplicate it.
        inner = inner[: head_m.start()] + inner[head_m.end() :]

    body = _clean_text(inner)
    # Collapse stray bullets and trim.
    body = body.strip().strip("—–-").strip()
    return (title, section, heading, body)


def build_akn_xml(
    title: str, section: str, heading: str, body: str
) -> str:
    citation = f"{title} V.S.A. § {section}"
    safe_section = section.replace(".", "_").replace("-", "_")
    eid = f"sec_{title}_{safe_section}"
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
          <FRBRthis value="/akn/us-vt/act/vsa/{title}/{section}"/>
          <FRBRuri value="/akn/us-vt/act/vsa/{title}/{section}"/>
          <FRBRauthor href="#vt-legislature"/>
          <FRBRcountry value="us-vt"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="V.S.A."/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-vt/act/vsa/{title}/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-vt/act/vsa/{title}/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-vt/act/vsa/{title}/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-vt/act/vsa/{title}/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="vt-legislature" href="https://legislature.vermont.gov" showAs="Vermont General Assembly"/>
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


def scrape_section(
    title: str, chapter: str, section: str, out_root: Path
) -> tuple[bool, bool]:
    """Scrape one section. Return (ok, skipped)."""
    url = f"{BASE}/statutes/section/{title}/{chapter}/{section}"
    html = _http_get(url)
    if html is None:
        return (False, True)
    parsed = parse_section_page(html)
    if parsed is None:
        return (False, True)
    _title, sec_num, heading, body = parsed
    # Prefer the cite marker's title for citation correctness, but keep the URL
    # token for filesystem pathing so each title directory is self-contained.
    if not body:
        return (False, True)
    xml = build_akn_xml(_title, sec_num, heading, body)
    safe_section = sec_num.replace("/", "_").replace(".", "_")
    dest = (
        out_root
        / "statutes"
        / f"ch-{title}"
        / f"ch-{title}-sec-{safe_section}.xml"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(xml, encoding="utf-8")
    return (True, False)


def _collect_work(
    title_filter: set[str] | None,
    chapter_filter: set[str] | None,
    limit_sections: int | None,
) -> list[tuple[str, str, str]]:
    """Walk title -> chapter -> section listings and return tuples to fetch."""
    titles = list_titles()
    if not titles:
        print("  WARN: no titles found on statutes root", file=sys.stderr)
        return []
    if title_filter:
        titles = [t for t in titles if t in title_filter]
    work: list[tuple[str, str, str]] = []
    for title in titles:
        chapters = list_chapters(title)
        if chapter_filter:
            chapters = [c for c in chapters if c in chapter_filter]
        for chapter in chapters:
            sections = list_sections(title, chapter)
            if limit_sections:
                sections = sections[:limit_sections]
            for section in sections:
                work.append((title, chapter, section))
    return work


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-vt"))
    parser.add_argument(
        "--titles",
        default="",
        help="Comma-separated title tokens (e.g. '32,09A'). Default: all.",
    )
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '151'). Default: all.",
    )
    parser.add_argument(
        "--limit-sections",
        type=int,
        default=None,
        help="Stop after N sections per chapter (for smoke tests).",
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="Parallel section fetches."
    )
    args = parser.parse_args(argv)

    title_filter: set[str] | None = None
    if args.titles:
        title_filter = {t.strip() for t in args.titles.split(",") if t.strip()}
    chapter_filter: set[str] | None = None
    if args.chapters:
        chapter_filter = {c.strip() for c in args.chapters.split(",") if c.strip()}

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)

    work = _collect_work(title_filter, chapter_filter, args.limit_sections)
    print(f"Scraping {len(work)} sections", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_section, t, c, s, args.out): (t, c, s)
            for (t, c, s) in work
        }
        done = 0
        for fut in as_completed(futures):
            t, c, s = futures[fut]
            ok, skipped = fut.result()
            total_ok += int(ok)
            total_skipped += int(skipped)
            done += 1
            if done % 25 == 0 or done == len(futures):
                elapsed = (time.time() - started) / 60
                print(
                    f"  {done}/{len(futures)}  "
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
