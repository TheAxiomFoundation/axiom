"""Scrape the Nebraska Revised Statutes from nebraskalegislature.gov.

Source layout
-------------
The Nebraska Revised Statutes are published one section per page at
``https://nebraskalegislature.gov/laws/statutes.php?statute={chapter}-{section}``
(e.g. ``?statute=2-1201``). Each chapter has a TOC at
``https://nebraskalegislature.gov/laws/browse-chapters.php?chapter={N}``
which links to every section in that chapter via::

    <a href="/laws/statutes.php?statute=1-101"><span class="sr-only">View Statute </span>1-101</a>

The master chapter index lives at
``https://nebraskalegislature.gov/laws/browse-statutes.php`` and lists
all 90 numeric chapters.

Each section page wraps its body in::

    <div class="statute">
      <h2>{chapter}-{section}.</h2>
      <h3>{heading}</h3>
      <p class="text-justify">{body para 1}</p>
      <p class="text-justify">{body para 2}</p>
      ...
      <div>
        <h2>Source</h2>
        <ul>...cite history...</ul>
      </div>
    </div>

Repealed sections have no body paragraphs and an H3 like
``Repealed. Laws 1957, c. 1, § 65.`` — they are skipped (body empty).
Invalid / nonexistent statute numbers return a 200 response whose body
is literally ``Invalid statute number format: '...'`` (no HTML); those
are treated as 404-equivalent skips.

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state ne`` expects.

Usage
-----
::

    uv run python scripts/scrape_ne_laws.py --out /tmp/rules-us-ne
    uv run python scripts/scrape_ne_laws.py --out /tmp/rules-us-ne --chapters 90
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

BASE = "https://nebraskalegislature.gov/laws"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Nebraska's Revised Statutes have 90 numbered chapters (some have decimal
# subsections like 1-105.01, but chapter tokens themselves are integer).
NE_CHAPTERS = list(range(1, 91))


def _http_get(url: str, retries: int = 4) -> str | None:
    """GET a URL as UTF-8; return None for 404/410 so callers skip.

    NE's server returns 200 with a plain-text error body for malformed
    statute numbers; _parse_section detects that via the missing
    ``<div class="statute">`` wrapper and skips those too.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code in (404, 410):
                return None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(min(8.0, 2.0**attempt))
    print(f"  WARN skip {url}: {last_exc}", file=sys.stderr, flush=True)
    return None


def _clean_text(s: str) -> str:
    """Strip HTML tags, normalize whitespace."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span|li)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# Each chapter TOC exposes every section via this exact anchor shape.
_SECTION_LINK_RE = re.compile(
    r'<a\s+href="/laws/statutes\.php\?statute=(?P<stat>[0-9A-Za-z.\-]+)"'
    r'>\s*<span class="sr-only">View Statute\s*</span>\s*(?P=stat)\s*</a>',
    re.IGNORECASE,
)


def list_sections_for_chapter(chapter: int) -> list[str]:
    """Return the list of ``{chapter}-{section}`` statute tokens for a chapter.

    Deduped and in discovery order so the scraper traverses sections
    in the order the legislature renders them.
    """
    url = f"{BASE}/browse-chapters.php?chapter={chapter}"
    html = _http_get(url)
    if html is None:
        return []
    seen: set[str] = set()
    tokens: list[str] = []
    prefix = f"{chapter}-"
    for m in _SECTION_LINK_RE.finditer(html):
        tok = m.group("stat")
        if not tok.startswith(prefix):
            # Skip cross-chapter refs that slip into TOC markup.
            continue
        if tok in seen:
            continue
        seen.add(tok)
        tokens.append(tok)
    return tokens


# Section page body anatomy.
_STATUTE_DIV_RE = re.compile(
    r'<div\s+class="statute"[^>]*>(.*?)(?=<div\s+class="card-footer"|'
    r'</div>\s*</div>\s*<nav|</div>\s*</div>\s*</div>)',
    re.IGNORECASE | re.DOTALL,
)
_H2_NUM_RE = re.compile(
    r'<h2[^>]*>\s*(?P<num>[0-9A-Za-z.\-,]+?)\s*\.?\s*</h2>',
    re.IGNORECASE | re.DOTALL,
)
_H3_HEAD_RE = re.compile(
    r'<h3[^>]*>(?P<heading>.*?)</h3>', re.IGNORECASE | re.DOTALL
)
_BODY_PARA_RE = re.compile(
    r'<p\s+class="text-justify"[^>]*>(?P<para>.*?)</p>',
    re.IGNORECASE | re.DOTALL,
)


def parse_section(html: str, expected_token: str) -> tuple[str, str, str] | None:
    """Extract ``(section, heading, body)`` from a section page.

    Returns None if:
    - The page has no ``<div class="statute">`` (invalid / error body).
    - The H3 heading begins with ``Repealed.`` AND there are no body paras
      (pure repeal marker — nothing substantive to archive).
    """
    div_m = _STATUTE_DIV_RE.search(html)
    if not div_m:
        return None
    slab = div_m.group(1)

    # Drop the inner Source <div>, which contains cite history, not statute text.
    slab_body = re.sub(
        r'<div[^>]*>\s*<h2[^>]*>\s*Source\s*</h2>.*?</div>',
        "",
        slab,
        flags=re.IGNORECASE | re.DOTALL,
    )

    num_m = _H2_NUM_RE.search(slab_body)
    section = num_m.group("num").strip().rstrip(".") if num_m else expected_token

    head_m = _H3_HEAD_RE.search(slab_body)
    heading = ""
    if head_m:
        heading = _clean_text(head_m.group("heading")).rstrip(".")

    paras: list[str] = []
    for m in _BODY_PARA_RE.finditer(slab_body):
        para = _clean_text(m.group("para"))
        if para:
            paras.append(para)

    body = "\n\n".join(paras)
    if not body and heading.lower().startswith("repealed"):
        return None
    return section, heading, body


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"Neb. Rev. Stat. \u00a7 {section}"
    safe_section = section.replace(".", "_").replace("-", "_").replace(",", "_")
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
          <FRBRthis value="/akn/us-ne/act/nrs/{section}"/>
          <FRBRuri value="/akn/us-ne/act/nrs/{section}"/>
          <FRBRauthor href="#ne-legislature"/>
          <FRBRcountry value="us-ne"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="NebRevStat"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-ne/act/nrs/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-ne/act/nrs/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-ne/act/nrs/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-ne/act/nrs/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="ne-legislature" href="https://nebraskalegislature.gov" showAs="Nebraska Legislature"/>
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


def _section_suffix(stat_token: str, chapter: int) -> str:
    """Return the within-chapter section id, e.g. ``'1-105.01' → '105.01'``."""
    prefix = f"{chapter}-"
    return stat_token[len(prefix):] if stat_token.startswith(prefix) else stat_token


def scrape_section(
    stat_token: str, chapter: int, out_root: Path
) -> tuple[bool, str]:
    """Fetch + parse one section, write AKN XML. Return (ok, msg)."""
    url = f"{BASE}/statutes.php?statute={urllib.parse.quote(stat_token)}"
    html = _http_get(url)
    if html is None:
        return (False, "404")
    parsed = parse_section(html, stat_token)
    if parsed is None:
        return (False, "no body / repealed")
    section_num, heading, body = parsed
    if not body:
        return (False, "empty body")

    safe_section = (
        _section_suffix(section_num, chapter)
        .replace("/", "_")
    )
    dest = (
        out_root
        / "statutes"
        / f"ch-{chapter}"
        / f"ch-{chapter}-sec-{safe_section}.xml"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        build_akn_xml(str(chapter), section_num, heading, body),
        encoding="utf-8",
    )
    return (True, section_num)


def scrape_chapter(
    chapter: int, out_root: Path, workers: int
) -> tuple[int, int, int]:
    """Scrape one chapter. Return (ok, skipped, section_count)."""
    tokens = list_sections_for_chapter(chapter)
    if not tokens:
        return (0, 0, 0)
    ok = 0
    skipped = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(scrape_section, t, chapter, out_root): t for t in tokens
        }
        for fut in as_completed(futures):
            ok_flag, _msg = fut.result()
            if ok_flag:
                ok += 1
            else:
                skipped += 1
    return (ok, skipped, len(tokens))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-ne"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter numbers (e.g. '1,90').",
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="Parallel section fetches per chapter."
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Stop after N chapters."
    )
    args = parser.parse_args(argv)

    chapters: list[int]
    if args.chapters:
        chapters = [int(c.strip()) for c in args.chapters.split(",") if c.strip()]
    else:
        chapters = NE_CHAPTERS
    if args.limit:
        chapters = chapters[: args.limit]

    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    total_ok = 0
    total_skipped = 0
    print(f"Scraping {len(chapters)} Nebraska chapters", flush=True)

    for chapter in chapters:
        ok, skipped, n = scrape_chapter(chapter, args.out, args.workers)
        total_ok += ok
        total_skipped += skipped
        elapsed = (time.time() - started) / 60
        print(
            f"  ch-{chapter}: {ok} ok, {skipped} skip / {n} listed  "
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
