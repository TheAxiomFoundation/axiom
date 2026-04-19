"""Scrape the Oregon Revised Statutes from oregonlegislature.gov.

Source layout
-------------
The ORS is served one page per chapter at
``https://www.oregonlegislature.gov/bills_laws/ors/ors{NNN}.html`` where
``NNN`` is the zero-padded chapter token (``001`` for chapter 1, ``244`` for
chapter 244, ``285A`` for chapter 285A, etc.). Chapters have gaps
(e.g. chapter 6 is not published) so 404 is treated as skip.

Each page is a Word-exported HTML bundle where each section starts with a
bolded lead-in of the form::

    <b><span style='font-family:"Times New Roman",serif'>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 1.020 Contempt of court.
    </span></b>
    <span style='font-family:"Times New Roman",serif'> {body paragraphs}</span>
    ...

The body continues through subsequent ``<p class=MsoNormal>`` paragraphs
until the next bolded lead-in, which may be another section heading or an
annotation block starting with ``Note:`` or ``Sec. N.`` (legislative
history). Annotation blocks are dropped as they are not operative text.

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state or`` expects.

Usage
-----
::

    uv run python scripts/scrape_or_laws.py --out /tmp/rules-us-or
    uv run python scripts/scrape_or_laws.py --out /tmp/rules-us-or --chapters 1,244
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

BASE = "https://www.oregonlegislature.gov/bills_laws/ors"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Chapter tokens are upper-bounded by ~830; probe every integer and known
# letter suffixes. The index page at /bills_laws/ors/ is behind auth, so we
# enumerate file names directly.
MAX_CHAPTER_NUM = 840
LETTER_SUFFIXES = ("", "A", "B", "C", "D", "E")


def _http_get(url: str, retries: int = 3) -> tuple[int, str]:
    """GET a URL decoded as cp1252 (ORS pages are Windows-1252).

    Returns (status_code, body). 404 is returned as (404, "").
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return (resp.status, resp.read().decode("cp1252", errors="replace"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return (404, "")
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


def enumerate_chapter_files() -> list[str]:
    """Return candidate ``ors{NNN}{suffix}.html`` filenames to probe."""
    files: list[str] = []
    for n in range(1, MAX_CHAPTER_NUM + 1):
        token = f"{n:03d}"
        for suf in LETTER_SUFFIXES:
            files.append(f"ors{token}{suf}.html")
    return files


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace, preserve paragraph breaks.

    Word-exported HTML has arbitrary line breaks inside tags and text that
    carry no semantic meaning — only ``<p>`` and ``<br>`` are real breaks.
    We normalize by:
      1. Converting ``<br>`` and ``</p>`` to a sentinel paragraph marker.
      2. Stripping remaining tags.
      3. Collapsing all whitespace (including raw newlines) to single spaces.
      4. Replacing the sentinel with ``\n\n`` paragraph breaks.
    """
    sentinel = "@@PARABREAK@@"
    s = re.sub(r"<br\s*/?>", sentinel, s, flags=re.IGNORECASE)
    s = re.sub(r"</p\s*>", sentinel, s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.replace(sentinel, "\n\n")
    s = re.sub(r" *\n *", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


# Section-heading lead-in: <b><span ...>[nbsp/space]* {section} {heading}.</span></b>
# Section is {chapter-token}.{subsec} where chapter token = digits plus optional letter.
_SECTION_HEAD = re.compile(
    r'<b>\s*<span[^>]*>\s*[\xa0\s]*'
    r'(?P<section>\d+[A-Za-z]?\.\d+[A-Za-z]?)'
    r'\s+(?P<heading>.*?)'
    r'</span>\s*</b>',
    re.DOTALL,
)

# Terminator — any bolded lead-in that ends the previous section body.
# Matches section headings OR annotation markers ("Note:", "Sec. N.") that
# appear between sections.
_ANY_LEADIN = re.compile(
    r'<b>\s*<span[^>]*>\s*[\xa0\s]*'
    r'(?:'
    r'(?:\d+[A-Za-z]?\.\d+[A-Za-z]?\s)|'  # another section
    r'Note:|'
    r'Sec\.\s*\d+[A-Za-z]?\.'  # "Sec. 3." historical annotation
    r')',
    re.DOTALL,
)


def split_sections(html: str) -> list[tuple[str, str, str]]:
    """Return ``(section, heading, body)`` for each section in the page."""
    heads = list(_SECTION_HEAD.finditer(html))
    terms = [m.start() for m in _ANY_LEADIN.finditer(html)]
    sections: list[tuple[str, str, str]] = []
    for i, m in enumerate(heads):
        section_num = m.group("section")
        heading_raw = m.group("heading")
        start = m.end()
        # Body ends at the next lead-in whose start > start.
        body_end = len(html)
        for ts in terms:
            if ts > start:
                body_end = ts
                break
        body_html = html[start:body_end]
        body = _clean_text(body_html)
        # Lead-in is typically followed by a space and then the body — strip any
        # leading orphan punctuation (e.g. the sentence-terminal period that
        # belonged to the heading but fell on the body side due to formatting).
        body = body.lstrip(". \t\n").strip()
        # Drop trailing history bracket — e.g. "[1981 c.1 §4; 1995 c.658 §7]".
        # ORS appends bracketed session-law citations to the end of the last
        # paragraph; they aren't operative text.
        body = re.sub(r"\s*\[[^\[\]]*\]\s*$", "", body).strip()
        heading = _clean_text(heading_raw)
        heading = re.sub(r"\s+", " ", heading).strip().rstrip(".")
        sections.append((section_num, heading, body))
    return sections


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"ORS {section}"
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
          <FRBRthis value="/akn/us-or/act/ors/{section}"/>
          <FRBRuri value="/akn/us-or/act/ors/{section}"/>
          <FRBRauthor href="#or-legislature"/>
          <FRBRcountry value="us-or"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="ORS"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-or/act/ors/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-or/act/ors/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-or/act/ors/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-or/act/ors/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="or-legislature" href="https://www.oregonlegislature.gov" showAs="Oregon Legislative Assembly"/>
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


def _chapter_key(filename: str) -> str:
    """Extract chapter token from ``ors001.html`` → ``1`` or ``ors285A.html`` → ``285A``.

    Strips leading zeros from the numeric portion.
    """
    m = re.match(r"ors(\d+)([A-Za-z]?)\.html$", filename)
    if not m:
        return filename
    digits, suffix = m.group(1), m.group(2)
    return f"{int(digits)}{suffix}"


def scrape_chapter(
    filename: str, out_root: Path, chapter_filter: set[str] | None
) -> tuple[int, int, str, bool]:
    """Scrape one chapter page. Return (ok, skipped, chapter_token, found).

    ``found`` is False when the chapter file returned 404 (expected for gaps).
    """
    chapter_token = _chapter_key(filename)
    if chapter_filter and chapter_token not in chapter_filter:
        return (0, 0, chapter_token, True)

    try:
        status, html = _http_get(f"{BASE}/{filename}")
    except RuntimeError as exc:
        print(f"  WARN {chapter_token}: {exc}", file=sys.stderr)
        return (0, 0, chapter_token, True)

    if status == 404 or not html:
        return (0, 0, chapter_token, False)

    sections = split_sections(html)
    ok = 0
    skipped = 0
    for section, heading, body in sections:
        if not body:
            skipped += 1
            continue
        # Verify section belongs to this chapter — guard against stray matches.
        sec_chapter_raw = re.match(r"(\d+)([A-Za-z]?)", section)
        sec_chapter_norm = (
            f"{int(sec_chapter_raw.group(1))}{sec_chapter_raw.group(2)}"
        )
        if sec_chapter_norm != chapter_token:
            skipped += 1
            continue
        xml = build_akn_xml(chapter_token, section, heading, body)
        safe_section = section.replace("/", "_")
        dest = (
            out_root
            / "statutes"
            / f"ch-{chapter_token}"
            / f"ch-{chapter_token}-sec-{safe_section}.xml"
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(xml, encoding="utf-8")
        ok += 1
    return (ok, skipped, chapter_token, True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-or"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '1,244,285A').",
    )
    parser.add_argument("--workers", type=int, default=6,
                        help="Parallel chapter fetches.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N chapter pages (any 200 or 404).")
    args = parser.parse_args(argv)

    chapter_filter: set[str] | None = None
    if args.chapters:
        chapter_filter = {c.strip() for c in args.chapters.split(",") if c.strip()}

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)
    files = enumerate_chapter_files()
    if chapter_filter:
        files = [f for f in files if _chapter_key(f) in chapter_filter]
    if args.limit:
        files = files[: args.limit]
    print(f"Probing {len(files)} candidate chapter pages", flush=True)

    total_ok = 0
    total_skipped = 0
    total_found = 0
    total_missing = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_chapter, f, args.out, chapter_filter): f for f in files
        }
        for fut in as_completed(futures):
            ok, skipped, ch, found = fut.result()
            total_ok += ok
            total_skipped += skipped
            if found:
                total_found += 1
            else:
                total_missing += 1
            if ok + skipped > 0:
                elapsed = (time.time() - started) / 60
                print(
                    f"  ch-{ch}: {ok} ok, {skipped} skip  "
                    f"(running: {total_ok} ok / {total_skipped} skip, "
                    f"{total_found} found / {total_missing} missing, "
                    f"{elapsed:.1f} min)",
                    flush=True,
                )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE — {total_ok} sections scraped, "
        f"{total_skipped} skipped, "
        f"{total_found} chapters found, {total_missing} missing (404), "
        f"{elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
