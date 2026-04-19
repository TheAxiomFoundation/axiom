"""Scrape the Missouri Revised Statutes from revisor.mo.gov.

Source layout
-------------
The RSMo is served via ASP.NET pages at ``https://www.revisor.mo.gov``:

* ``Home.aspx`` contains the master navigation listing every chapter
  across every title via links of the form
  ``/main/OneChapter.aspx?chapter={chapter}``.
* ``OneChapter.aspx?chapter={chapter}`` is a chapter index listing each
  section as ``/main/PageSelect.aspx?section={chapter}.{sec}&bid=...``.
  The page contains no section body text, only the list of sections.
* ``OneSection.aspx?section={chapter}.{sec}`` renders the section body.
  The heading is in ``<meta property="og:description" content="...">``
  and the body text is in ``<p class="norm">`` paragraphs inside the
  ``<div class="norm" ...>`` block, followed by a ``<div class="foot">``
  containing the enactment history (stripped).

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state mo`` expects. Citations
use ``§ {section}, RSMo``.

Usage
-----
::

    uv run python scripts/scrape_mo_laws.py --out /tmp/rules-us-mo
    uv run python scripts/scrape_mo_laws.py --out /tmp/rules-us-mo --chapters 1,32
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

BASE = "https://www.revisor.mo.gov"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> str:
    """GET a URL decoded as UTF-8 (MO pages declare charset=UTF-8)."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            # Treat 404 as a skip, not a retryable error.
            if exc.code == 404:
                raise
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


def list_chapters() -> list[str]:
    """Return every chapter token (e.g. ``'1'``, ``'143'``) from the home page.

    The Home.aspx navigation lists every chapter across every title.
    """
    html = _http_get(f"{BASE}/main/Home.aspx")
    chapters = re.findall(
        r"/main/OneChapter\.aspx\?chapter=([0-9A-Za-z]+)", html
    )
    # Preserve numeric ordering when possible, else lex.
    def _key(s: str) -> tuple[int, int | str, str]:
        m = re.match(r"^(\d+)([A-Za-z]*)$", s)
        if m:
            return (0, int(m.group(1)), m.group(2))
        return (1, s, "")

    return sorted(set(chapters), key=_key)


_SECTION_HREF = re.compile(
    r'PageSelect\.aspx\?section=([0-9A-Za-z.]+)(?:&amp;|&)bid=\d+'
)


def list_chapter_sections(chapter: str) -> list[str]:
    """Return ordered list of section tokens in the given chapter."""
    url = f"{BASE}/main/OneChapter.aspx?chapter={chapter}"
    html = _http_get(url)
    # Preserve first-occurrence order (the page lists sections in code order).
    seen: dict[str, None] = {}
    for m in _SECTION_HREF.finditer(html):
        sec = m.group(1)
        if sec not in seen:
            seen[sec] = None
    return list(seen)


def _clean_text(s: str) -> str:
    """Strip HTML tags, collapse whitespace."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n\n", s).strip()
    return s


# The section body sits in a <div class="norm" ...> ... </div> block that
# contains <p class="norm"> paragraphs. A trailing <div class="foot"> holds
# the enactment history, which we drop.
_NORM_BLOCK = re.compile(
    r'<div\s+class="norm"[^>]*>(?P<body>.*?)</div>\s*<hr\s*/?>',
    re.DOTALL | re.IGNORECASE,
)
_FOOT_BLOCK = re.compile(
    r'<div\s+class="foot"[^>]*>.*?</div>', re.DOTALL | re.IGNORECASE
)
# The lead <span class="bold"> contains a nested whitespace <span> before
# the heading text, so a naive non-greedy match stops too early. Match the
# outer span by allowing one nested balanced <span>...</span> inside.
_LEAD_BOLD = re.compile(
    r'<span\s+class="bold"[^>]*>'
    r'(?P<lead>(?:[^<]|<span[^>]*>[^<]*</span>|<(?!/?span))*?)'
    r'</span>',
    re.DOTALL | re.IGNORECASE,
)
_OG_DESC = re.compile(
    r'<meta\s+property="og:description"\s+content="(?P<d>[^"]*)"',
    re.IGNORECASE,
)


def parse_section(html: str, section: str) -> tuple[str, str]:
    """Return ``(heading, body)`` for the section; empty strings if missing.

    The ``<span class="bold">`` near the top of the body carries both the
    section number and the section heading, separated by ``—``. We also
    fall back to the ``og:description`` meta tag for the heading.
    """
    m = _NORM_BLOCK.search(html)
    if not m:
        return ("", "")
    body_html = m.group("body")
    # Drop the foot (history / annotations).
    body_html = _FOOT_BLOCK.sub("", body_html)

    # Extract heading from leading <span class="bold">; text is like
    # "  1.010.  Common law in force — effect on statutes — ... — ".
    heading = ""
    lb = _LEAD_BOLD.search(body_html)
    if lb:
        lead = _clean_text(lb.group("lead"))
        # Strip leading section number "1.010." if present.
        lead = re.sub(rf"^\s*{re.escape(section)}\.\s*", "", lead)
        # The heading ends where the actual section text begins; MO uses
        # " — " as the trailing separator. The final " — " is included in
        # the bold span itself, so clean_text already trimmed it.
        heading = lead.strip().rstrip("—–-").strip().rstrip(".")
        # Remove the bold span from body — its text is heading/lead, not body.
        body_html = _LEAD_BOLD.sub("", body_html, count=1)

    if not heading:
        md = _OG_DESC.search(html)
        if md:
            heading = _clean_text(md.group("d")).rstrip(".")

    body = _clean_text(body_html)
    # Sometimes an initial " — " remnant lingers from the lead removal.
    body = body.lstrip("—–-").strip()
    return (heading, body)


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"\u00a7 {section}, RSMo"
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
          <FRBRthis value="/akn/us-mo/act/rsmo/{section}"/>
          <FRBRuri value="/akn/us-mo/act/rsmo/{section}"/>
          <FRBRauthor href="#mo-legislature"/>
          <FRBRcountry value="us-mo"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="RSMo"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-mo/act/rsmo/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-mo/act/rsmo/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-mo/act/rsmo/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-mo/act/rsmo/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="mo-legislature" href="https://www.revisor.mo.gov" showAs="Missouri General Assembly"/>
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
    chapter: str, out_root: Path, chapter_filter: set[str] | None
) -> tuple[int, int, str]:
    """Scrape one chapter. Return ``(ok, skipped, chapter)``."""
    if chapter_filter and chapter not in chapter_filter:
        return (0, 0, chapter)

    try:
        sections = list_chapter_sections(chapter)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return (0, 0, chapter)
        print(f"  WARN ch-{chapter}: {exc}", file=sys.stderr)
        return (0, 0, chapter)
    except RuntimeError as exc:
        print(f"  WARN ch-{chapter}: {exc}", file=sys.stderr)
        return (0, 0, chapter)

    ok = 0
    skipped = 0
    for section in sections:
        # Sections are named like "1.010", "143.121"; the chapter prefix
        # should match the page's chapter to avoid cross-chapter pollution.
        sec_chapter = section.split(".", 1)[0]
        if sec_chapter != chapter:
            skipped += 1
            continue

        url = f"{BASE}/main/OneSection.aspx?section={section}"
        try:
            html = _http_get(url)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                skipped += 1
                continue
            print(f"  WARN {section}: {exc}", file=sys.stderr)
            skipped += 1
            continue
        except RuntimeError as exc:
            print(f"  WARN {section}: {exc}", file=sys.stderr)
            skipped += 1
            continue

        heading, body = parse_section(html, section)
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
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-mo"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '1,32,143').",
    )
    parser.add_argument("--workers", type=int, default=6,
                        help="Parallel chapter fetches.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N chapters.")
    args = parser.parse_args(argv)

    chapter_filter: set[str] | None = None
    if args.chapters:
        chapter_filter = {c.strip() for c in args.chapters.split(",") if c.strip()}

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)
    chapters = list_chapters()
    if chapter_filter:
        chapters = [c for c in chapters if c in chapter_filter]
    if args.limit:
        chapters = chapters[: args.limit]
    print(f"Scraping {len(chapters)} chapters", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_chapter, c, args.out, None): c for c in chapters
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
