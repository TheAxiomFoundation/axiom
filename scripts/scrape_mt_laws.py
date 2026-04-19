"""Scrape the Montana Code Annotated from archive.legmt.gov.

Source layout
-------------
The MCA is served as a four-level static HTML tree at
``https://archive.legmt.gov/bills/mca/``::

    index.html                              -> list of titles
    title_NNNN/chapters_index.html          -> list of chapters
    title_NNNN/chapter_NNNN/parts_index.html -> list of parts
    title_NNNN/chapter_NNNN/part_NNNN/sections_index.html
                                             -> list of sections
    title_NNNN/chapter_NNNN/part_NNNN/section_NNNN/
        NNNN-NNNN-NNNN-NNNN.html            -> one section

Each section page contains a ``<div class="section-doc" ...>`` with
``<p class="line-indent">`` paragraphs. The first paragraph starts with a
``<span class="catchline"><span class="citation">NN-NN-NNNN</span>.&#8195;
Heading.</span>`` followed by the body. A separate ``<div class="history-doc"
...>`` holds the session-law history; we drop it.

Citations are ``MCA § {title}-{chapter}-{section}`` where the section token
is the dashed number printed in the citation span (e.g. ``15-30-2101``).

Output
------
AKN-3.0 XML files at
``{out}/statutes/ch-{title}-{chapter}/ch-{title}-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state mt`` expects.

Usage
-----
::

    uv run python scripts/scrape_mt_laws.py --out /tmp/rules-us-mt
    uv run python scripts/scrape_mt_laws.py --out /tmp/rules-us-mt --titles 1,2
    uv run python scripts/scrape_mt_laws.py --out /tmp/rules-us-mt --limit 1
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

BASE = "https://archive.legmt.gov/bills/mca"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> str | None:
    """GET a URL as UTF-8. Return ``None`` on 404. Raise on other errors."""
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
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|li)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ").replace("\u2003", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n[ \t]+", "\n", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# title_0010 -> "1", title_0100 -> "10", title_0150 -> "15". The four-digit
# tail encodes the number times ten (the trailing zero is a reserved slot for
# future subdivisions). So "0150" -> 15, "0010" -> 1, "0030" -> 3.
_DIR_TOKEN = re.compile(r"_(\d+)$")


def _token_to_num(token: str) -> str:
    """Convert ``title_0150`` / ``chapter_0030`` -> ``15`` / ``3``."""
    m = _DIR_TOKEN.search(token)
    if not m:
        return token
    raw = int(m.group(1))
    # Trailing digit is always 0 for normal titles/chapters/parts; divide it
    # out to recover the canonical number.
    if raw % 10 == 0:
        return str(raw // 10)
    # Fallback for anything that doesn't follow the pattern.
    return str(raw)


def list_titles() -> list[str]:
    """Return ``['title_0000', 'title_0010', ...]`` by scraping the TOC."""
    html = _http_get(f"{BASE}/index.html")
    if html is None:
        raise RuntimeError(f"no MCA TOC at {BASE}/index.html")
    # Only follow active (linked) titles. Reserved ones are <span>s.
    hrefs = re.findall(r'href="\.?/?(title_\d+)/chapters_index\.html"', html)
    return sorted(set(hrefs))


def list_chapters(title_dir: str) -> list[str]:
    """Return chapter dir tokens (e.g. ``chapter_0010``) for a title."""
    url = f"{BASE}/{title_dir}/chapters_index.html"
    html = _http_get(url)
    if html is None:
        return []
    hrefs = re.findall(r'href="\.?/?(chapter_\d+)/parts_index\.html"', html)
    return sorted(set(hrefs))


def list_parts(title_dir: str, chapter_dir: str) -> list[str]:
    """Return part dir tokens (e.g. ``part_0010``) for a chapter."""
    url = f"{BASE}/{title_dir}/{chapter_dir}/parts_index.html"
    html = _http_get(url)
    if html is None:
        return []
    hrefs = re.findall(r'href="\.?/?(part_\d+)/sections_index\.html"', html)
    return sorted(set(hrefs))


# Each row in sections_index looks like:
#   <a href="./section_0010/0010-0010-0010-0010.html">
#     <span class="citation">1-1-101</span>&nbsp;Definition of law</a>
_SECTION_ROW = re.compile(
    r'<a[^>]+href="\.?/?(?P<dir>section_\d+)/(?P<file>[\d-]+\.html)"[^>]*>\s*'
    r'<span\s+class="citation"[^>]*>\s*(?P<cite>[^<]+?)\s*</span>'
    r'(?P<rest>.*?)</a>',
    re.DOTALL,
)


def list_sections(
    title_dir: str, chapter_dir: str, part_dir: str
) -> list[tuple[str, str, str, str]]:
    """Return ``(section_dir, filename, citation, row_heading)`` for each section
    listed on the part's sections_index.
    """
    url = f"{BASE}/{title_dir}/{chapter_dir}/{part_dir}/sections_index.html"
    html = _http_get(url)
    if html is None:
        return []
    out: list[tuple[str, str, str, str]] = []
    for m in _SECTION_ROW.finditer(html):
        cite = _clean_text(m.group("cite"))
        rest = _clean_text(m.group("rest"))
        out.append((m.group("dir"), m.group("file"), cite, rest))
    return out


# Catchline in a section page: span class="catchline" wraps the citation and
# heading text. The body text follows in the same and subsequent <p> tags.
_CATCHLINE = re.compile(
    r'<span\s+class="catchline"[^>]*>\s*'
    r'<span\s+class="citation"[^>]*>\s*(?P<cite>[^<]+?)\s*</span>'
    r'\s*\.?\s*(?P<heading>.*?)</span>',
    re.DOTALL,
)
_SECTION_DOC = re.compile(
    r'<div\s+class="section-doc"[^>]*>(?P<body>.*?)</div>\s*'
    r'(?=<div\s+class="history-doc"|</div>\s*<footer|</div>\s*$)',
    re.DOTALL,
)


def parse_section_page(html: str) -> tuple[str, str, str] | None:
    """Return ``(citation, heading, body)`` or ``None`` if the page is not a
    readable section (e.g. stub / renumbered entry with no substantive body).
    """
    doc_m = _SECTION_DOC.search(html)
    if not doc_m:
        return None
    doc_html = doc_m.group("body")

    cat_m = _CATCHLINE.search(doc_html)
    if not cat_m:
        return None
    citation = _clean_text(cat_m.group("cite"))
    heading = _clean_text(cat_m.group("heading")).rstrip(".")

    # Body = everything inside section-doc minus the catchline span. The
    # first paragraph's lead-in text after the catchline survives intact.
    body_html = doc_html[: cat_m.start()] + doc_html[cat_m.end() :]
    # Split into <p class="line-indent"> blocks so paragraph breaks survive.
    blocks = re.findall(
        r'<p[^>]*>(.*?)</p>', body_html, re.DOTALL | re.IGNORECASE
    )
    paras = [_clean_text(b) for b in blocks]
    paras = [p for p in paras if p]
    body = "\n\n".join(paras).strip()
    body = body.strip("—–-").strip()
    return (citation, heading, body)


def build_akn_xml(
    title: str, chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"MCA \u00a7 {section}"
    safe_section = section.replace(".", "_").replace("-", "_")
    safe_chapter = f"{title}_{chapter}"
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
          <FRBRthis value="/akn/us-mt/act/mca/{section}"/>
          <FRBRuri value="/akn/us-mt/act/mca/{section}"/>
          <FRBRauthor href="#mt-legislature"/>
          <FRBRcountry value="us-mt"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="MCA"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-mt/act/mca/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-mt/act/mca/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-mt/act/mca/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-mt/act/mca/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="mt-legislature" href="https://archive.legmt.gov" showAs="Montana Legislature"/>
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


def scrape_title(
    title_dir: str, out_root: Path
) -> tuple[int, int, int, str]:
    """Walk one title: chapters -> parts -> sections. Return
    ``(ok, skipped, not_found, title_token)``.
    """
    title_num = _token_to_num(title_dir)
    ok = 0
    skipped = 0
    not_found = 0

    chapters = list_chapters(title_dir)
    for chapter_dir in chapters:
        chapter_num = _token_to_num(chapter_dir)
        parts = list_parts(title_dir, chapter_dir)
        for part_dir in parts:
            sections = list_sections(title_dir, chapter_dir, part_dir)
            for section_dir, filename, cite, row_heading in sections:
                url = (
                    f"{BASE}/{title_dir}/{chapter_dir}/"
                    f"{part_dir}/{section_dir}/{filename}"
                )
                try:
                    page = _http_get(url)
                except RuntimeError as exc:
                    print(f"  WARN {cite}: {exc}", file=sys.stderr)
                    skipped += 1
                    continue
                if page is None:
                    not_found += 1
                    continue
                parsed = parse_section_page(page)
                if parsed is None:
                    skipped += 1
                    continue
                citation, heading, body = parsed
                if not body:
                    skipped += 1
                    continue
                if not heading:
                    heading = row_heading
                section_num = citation or cite
                xml = build_akn_xml(
                    title_num, chapter_num, section_num, heading, body
                )
                safe_section = section_num.replace("/", "_")
                dest = (
                    out_root
                    / "statutes"
                    / f"ch-{title_num}-{chapter_num}"
                    / f"ch-{title_num}-{chapter_num}-sec-{safe_section}.xml"
                )
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(xml, encoding="utf-8")
                ok += 1
    return (ok, skipped, not_found, title_num)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-mt"))
    parser.add_argument(
        "--titles",
        default="",
        help="Comma-separated title numbers (e.g. '1,2,15'). Empty = all.",
    )
    parser.add_argument("--workers", type=int, default=6,
                        help="Parallel title fetches.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N titles.")
    args = parser.parse_args(argv)

    title_filter: set[str] | None = None
    if args.titles:
        title_filter = {
            t.strip() for t in args.titles.split(",") if t.strip()
        }

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)
    titles = list_titles()
    if title_filter:
        titles = [t for t in titles if _token_to_num(t) in title_filter]
    if args.limit:
        titles = titles[: args.limit]
    print(f"Scraping {len(titles)} MCA titles", flush=True)

    total_ok = 0
    total_skipped = 0
    total_nf = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(scrape_title, t, args.out): t for t in titles}
        for fut in as_completed(futures):
            ok, skipped, nf, title_num = fut.result()
            total_ok += ok
            total_skipped += skipped
            total_nf += nf
            elapsed = (time.time() - started) / 60
            print(
                f"  title {title_num}: {ok} ok, {skipped} skip, {nf} 404  "
                f"(running: {total_ok} ok / {total_skipped} skip / "
                f"{total_nf} 404, {elapsed:.1f} min)",
                flush=True,
            )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE - {total_ok} sections scraped, "
        f"{total_skipped} skipped, {total_nf} 404, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
