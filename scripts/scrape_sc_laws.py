"""Scrape the South Carolina Code of Laws from scstatehouse.gov.

Source layout
-------------
The SC Code is served at ``https://www.scstatehouse.gov/code``. The master
index at ``/code/statmast.php`` lists 63 titles as links to
``/code/title{N}.php``. Each title page is a table whose only links are to
chapter HTML pages at ``/code/t{TT}c{CCC}.php`` (zero-padded, e.g.
``t01c001.php``, ``t12c006.php``).

Each chapter page renders all its sections inline. Per-section markup:

    <span style="font-weight: bold;"> SECTION 1-1-10.</span> Jurisdiction ...<br /><br />
        {body paragraphs separated by <br /><br />} ...
    HISTORY: 1962 Code SECTION 39-1; ... ; 2016 Act No. 270 ... <br /><br />

Some sections use a letter-suffixed number (e.g. ``1-1-713A``) and may not
be wrapped in the bold ``<span>``. Section numbers uniquely use the
triple-dashed form ``{title}-{chapter}-{section}[letter]``, distinguishing
them from history references like ``SECTION 2, eff June 4, 2008``.

Output
------
AKN-3.0 XML files at
``{out}/statutes/ch-{title}/ch-{title}-sec-{section}.xml``
(where ``{section}`` is the full dashed section number, e.g. ``1-1-10``),
matching what ``ingest_state_laws.py --state sc`` expects.

Usage
-----
::

    uv run python scripts/scrape_sc_laws.py --out /tmp/rules-us-sc
    uv run python scripts/scrape_sc_laws.py --out /tmp/rules-us-sc --titles 1
    uv run python scripts/scrape_sc_laws.py --out /tmp/rules-us-sc --titles 1,12,44
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

BASE = "https://www.scstatehouse.gov/code"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# SC has titles 1..63 (some gaps possible, but the master index is the source
# of truth).
DEFAULT_TITLES = tuple(range(1, 64))


def _http_get(url: str, retries: int = 3) -> str | None:
    """GET a URL as text. Return None on 404/307/410 or any connection error.

    Soft-fails on HTTPError (5xx), URLError, TimeoutError, and
    ConnectionResetError so a single flaky chapter does not tank the run.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                # SC pages declare iso-8859-1 in <meta> but the server
                # Content-Type is UTF-8. Try UTF-8 first, fall back to
                # iso-8859-1, then cp1252 with replace.
                try:
                    return raw.decode("utf-8")
                except UnicodeDecodeError:
                    try:
                        return raw.decode("iso-8859-1")
                    except UnicodeDecodeError:
                        return raw.decode("cp1252", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code in (404, 307, 410):
                return None
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
        except (urllib.error.URLError, TimeoutError, ConnectionResetError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
        except Exception as exc:  # noqa: BLE001 — defensive soft-fail
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
    print(f"  WARN giving up on {url}: {last_exc}", file=sys.stderr)
    return None


# Title index link: /code/title{N}.php
_TITLE_LINK = re.compile(
    r'href="/code/title(?P<title>\d+)\.php"',
    re.IGNORECASE,
)

# Chapter link inside a title page: /code/t{TT}c{CCC}.php
_CHAPTER_LINK = re.compile(
    r'href="/code/t(?P<title>\d+)c(?P<chapter>\d+[A-Za-z]*)\.php"',
    re.IGNORECASE,
)


def list_titles() -> list[int]:
    """Return the list of title numbers from the SC code master index."""
    html = _http_get(f"{BASE}/statmast.php")
    if html is None:
        return []
    seen: set[int] = set()
    for m in _TITLE_LINK.finditer(html):
        seen.add(int(m.group("title")))
    return sorted(seen)


def list_title_chapters(title_num: int) -> list[str]:
    """Return chapter tokens (e.g. ``001``, ``006``) for a title, in order."""
    url = f"{BASE}/title{title_num}.php"
    html = _http_get(url)
    if html is None:
        return []
    tokens: list[str] = []
    seen: set[str] = set()
    for m in _CHAPTER_LINK.finditer(html):
        if int(m.group("title")) != title_num:
            continue
        tok = m.group("chapter")
        if tok not in seen:
            seen.add(tok)
            tokens.append(tok)
    return tokens


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace, decode entities."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span|li)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n\n", s).strip()
    return s


# Match a section marker: ``SECTION {title}-{chapter}-{section}[letter].``
# Allows optional alphabetic suffix on any of the three segments (rare but
# present — e.g. ``12-21-2710A``). Requires all three dashes so history-note
# references like ``SECTION 2`` are not matched.
_SECTION_MARKER = re.compile(
    r"SECTION\s+"
    r"(?P<full>(?P<t>\d+[A-Za-z]?)-(?P<c>\d+[A-Za-z]?)-(?P<s>\d+[A-Za-z]?))"
    r"\s*\.",
)


def _extract_heading_and_body(slab: str) -> tuple[str, str]:
    """From the raw HTML slab after a SECTION marker, extract heading + body.

    The slab format is::

        {heading up to first <br />} <br /><br />
        {body paragraphs separated by <br /><br />}
        HISTORY: ... <br /><br />
        {editor notes, effect-of-amendment, etc.}

    We take everything up to the first ``HISTORY:`` marker as the section's
    content, then split heading (first <br/> delimiter) from body.
    """
    # End of this section is at "HISTORY:" (case-insensitive).
    history_m = re.search(r"HISTORY\s*:", slab, re.IGNORECASE)
    content = slab[: history_m.start()] if history_m else slab

    # The heading ends at the first <br /> (single) before body paragraphs.
    head_end = re.search(r"<br\s*/?>", content, re.IGNORECASE)
    if head_end is None:
        return ("", _clean_text(content))
    heading = _clean_text(content[: head_end.start()]).rstrip(".")
    body = _clean_text(content[head_end.end():])
    return (heading, body)


def parse_sections(
    html: str, title_num: int, chapter_token: str
) -> list[tuple[str, str, str]]:
    """Return ``(section_num, heading, body)`` for sections on the page.

    ``section_num`` is the full dashed identifier (e.g. ``1-1-10``,
    ``12-6-3650``). Only sections whose title matches ``title_num`` are
    returned, as a safety guard against cross-title references bleeding in.
    """
    matches = list(_SECTION_MARKER.finditer(html))
    out: list[tuple[str, str, str]] = []
    for i, m in enumerate(matches):
        section_num = m.group("full")
        # Safety: only accept this as a real section if its title+chapter
        # match the page we're on. Otherwise it's a cross-reference in a
        # history/editor note.
        t = m.group("t")
        c = m.group("c").lstrip("0") or "0"
        page_title = str(title_num)
        page_chapter = chapter_token.lstrip("0") or "0"
        if t != page_title or c != page_chapter:
            continue

        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(html)
        slab = html[start:end]
        heading, body = _extract_heading_and_body(slab)
        out.append((section_num, heading, body))
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
    """AKN-3.0 XML for one South Carolina Code section."""
    citation = f"S.C. Code \u00a7 {section}"
    safe_section = _safe_segment(section)
    eid = f"sec_t{title_num}_{section.replace('-', '_')}"
    paras = [p for p in re.split(r"\n\n+", body) if p.strip()]
    paras_xml = "\n            ".join(
        f"<p>{xml_escape(p)}</p>" for p in paras
    ) or "<p/>"
    frbr_path = f"/akn/us-sc/act/sccode/{section}"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="{AKN_NS}">
  <act name="section">
    <meta>
      <identification source="#axiom">
        <FRBRWork>
          <FRBRthis value="{frbr_path}"/>
          <FRBRuri value="{frbr_path}"/>
          <FRBRauthor href="#sc-legislature"/>
          <FRBRcountry value="us-sc"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="S.C. Code"/>
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
        <TLCOrganization eId="sc-legislature" href="https://www.scstatehouse.gov" showAs="South Carolina General Assembly"/>
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
    section: str,
    heading: str,
    body: str,
) -> bool:
    """Write one section XML. Return True if written, False if skipped."""
    if not body:
        return False
    xml = build_akn_xml(title_num, "", section, heading, body)
    safe_section = _safe_segment(section)
    dest = (
        out_root
        / "statutes"
        / f"ch-{title_num}"
        / f"ch-{title_num}-sec-{safe_section}.xml"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(xml, encoding="utf-8")
    return True


def scrape_chapter(
    title_num: int, chapter_token: str, out_root: Path
) -> tuple[int, int, str]:
    """Scrape one chapter page. Return (ok, skipped, label)."""
    label = f"t{title_num}-c{chapter_token}"
    # URL: zero-padded title (2 digits) + zero-padded chapter as seen on site.
    tt = f"{title_num:02d}"
    url = f"{BASE}/t{tt}c{chapter_token}.php"
    html = _http_get(url)
    if html is None:
        return (0, 0, label)

    ok = 0
    skipped = 0
    for section, heading, body in parse_sections(html, title_num, chapter_token):
        if _write_section(out_root, title_num, section, heading, body):
            ok += 1
        else:
            skipped += 1
    return (ok, skipped, label)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-sc"))
    parser.add_argument(
        "--titles",
        default="",
        help="Comma-separated title numbers (e.g. '1,12,44'). Default: from index.",
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
        titles = list_titles()
        if not titles:
            print("ERROR: could not load title index; using default range",
                  file=sys.stderr)
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
