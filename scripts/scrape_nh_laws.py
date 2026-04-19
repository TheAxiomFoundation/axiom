"""Scrape the New Hampshire Revised Statutes Annotated (RSA).

Source layout
-------------
The RSA is served at ``https://www.gencourt.state.nh.us/rsa/html/`` with this
shape:

- Top TOC (``nhtoc.htm``) lists titles (roman numerals) linking to
  ``NHTOC/NHTOC-{title}.htm``.
- Each title TOC lists chapters linking to
  ``NHTOC/NHTOC-{title}-{chapter}.htm`` (chapters may have letter suffixes
  like ``1-A``, ``21-V``).
- Each chapter TOC lists sections linking to
  ``../{title}/{chapter}/{chapter}-{section}.htm`` (e.g. ``I/1/1-1.htm``).
- Each section page has this shape::

    <center><h1>TITLE I<br>THE STATE AND ITS GOVERNMENT</h1></center>
    <center><h2>CHAPTER 1<br>STATE BOUNDARIES</h2></center>
    <center><h3>Section 1:1</h3></center>
    &nbsp;&nbsp;&nbsp;<b> 1:1 Perambulation of ... &#150;</b>
    <codesect>
    The boundary lines between the state of New Hampshire...
    </codesect>
    <sourcenote>
    <p><b>Source.</b> 2000, 35:1, eff. Jan. 1, 2001.</p>
    </sourcenote>

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state nh`` expects.

Usage
-----
::

    uv run python scripts/scrape_nh_laws.py --out /tmp/rules-us-nh
    uv run python scripts/scrape_nh_laws.py --out /tmp/rules-us-nh --chapters 1,1-A
"""

from __future__ import annotations

import argparse
import html as _html
import http.client
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

BASE = "https://www.gencourt.state.nh.us/rsa/html"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 8) -> str | None:
    """GET a URL decoded as utf-8. Return None on 404. Retries on transient errors."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Connection": "close",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last_exc = exc
            if attempt < retries:
                time.sleep(min(30.0, 2.0**attempt))
                continue
        except (
            urllib.error.URLError,
            TimeoutError,
            http.client.RemoteDisconnected,
            http.client.HTTPException,
            ConnectionResetError,
            OSError,
        ) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(30.0, 2.0**attempt))
                continue
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span|li)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    # &#150; is the Win-1252 en dash NH uses as a separator
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = re.sub(r"^[ \t]+", "", s, flags=re.MULTILINE)
    s = re.sub(r"[ \t]+$", "", s, flags=re.MULTILINE)
    return s.strip()


def list_titles() -> list[str]:
    """Return list of title roman numerals (``I``, ``II``, ...)."""
    html = _http_get(f"{BASE}/nhtoc.htm")
    if not html:
        return []
    names = re.findall(r'href="NHTOC/NHTOC-([A-Z\-]+)\.htm"', html)
    return sorted(set(names))


def list_chapters_in_title(title: str) -> list[tuple[str, str]]:
    """Return ``(title, chapter)`` pairs by scraping the title TOC."""
    html = _http_get(f"{BASE}/NHTOC/NHTOC-{title}.htm")
    if not html:
        return []
    # href="NHTOC-I-1.htm"  or  "NHTOC-I-1-A.htm"
    pairs: list[tuple[str, str]] = []
    for m in re.finditer(r'href="NHTOC-([A-Z\-]+?)-([0-9][0-9A-Z\-]*)\.htm"', html):
        t, ch = m.group(1), m.group(2)
        if t == title:
            pairs.append((t, ch))
    # Preserve document order but deduplicate.
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for p in pairs:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def list_sections_in_chapter(title: str, chapter: str) -> list[str]:
    """Return section tokens (e.g. ``1``, ``14``) for a chapter TOC."""
    html = _http_get(f"{BASE}/NHTOC/NHTOC-{title}-{chapter}.htm")
    if not html:
        return []
    # href="../I/1/1-14.htm"  or  "../I/1-A/1-A-5.htm"
    pattern = rf'href="\.\./{re.escape(title)}/{re.escape(chapter)}/{re.escape(chapter)}-([0-9][0-9A-Za-z\-]*)\.htm"'
    sections = re.findall(pattern, html)
    seen: set[str] = set()
    out: list[str] = []
    for s in sections:
        if s == "mrg":
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


# Page structure: heading line then <codesect>body</codesect>.
_CODESECT_RE = re.compile(r"<codesect[^>]*>(.*?)</codesect>", re.DOTALL | re.IGNORECASE)
_HEADING_RE = re.compile(
    r"<b>\s*[0-9A-Za-z:\-]+\s+(.*?)\s*(?:&#150;|-|\u2013|\u2014)\s*</b>",
    re.DOTALL,
)


def parse_section_page(
    html: str, chapter: str, section: str
) -> tuple[str, str]:
    """Return ``(heading, body)`` for a section page."""
    head_m = _HEADING_RE.search(html)
    heading = _clean_text(head_m.group(1)) if head_m else ""
    heading = heading.rstrip(".")

    code_m = _CODESECT_RE.search(html)
    body = _clean_text(code_m.group(1)) if code_m else ""
    return heading, body


def build_akn_xml(chapter: str, section: str, heading: str, body: str) -> str:
    full_cite = f"{chapter}:{section}"
    citation = f"RSA {full_cite}"
    safe = full_cite.replace(":", "_").replace("-", "_")
    eid = f"sec_{safe}"
    akn_value = f"{chapter}-{section}"
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
          <FRBRthis value="/akn/us-nh/act/rsa/{akn_value}"/>
          <FRBRuri value="/akn/us-nh/act/rsa/{akn_value}"/>
          <FRBRauthor href="#nh-legislature"/>
          <FRBRcountry value="us-nh"/>
          <FRBRnumber value="{full_cite}"/>
          <FRBRname value="RSA"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-nh/act/rsa/{akn_value}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-nh/act/rsa/{akn_value}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-nh/act/rsa/{akn_value}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-nh/act/rsa/{akn_value}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="nh-legislature" href="https://www.gencourt.state.nh.us" showAs="New Hampshire General Court"/>
        <TLCOrganization eId="axiom" href="https://axiom-foundation.org" showAs="Axiom Foundation"/>
      </references>
    </meta>
    <body>
      <section eId="{eid}">
        <num>{xml_escape(citation)}</num>
        <heading>{xml_escape(heading or f"Section {full_cite}")}</heading>
        <content>
            {paras_xml}
        </content>
      </section>
    </body>
  </act>
</akomaNtoso>
"""


def scrape_chapter(
    title: str,
    chapter: str,
    out_root: Path,
) -> tuple[int, int, str]:
    """Scrape one chapter. Return (ok, skipped, chapter)."""
    try:
        sections = list_sections_in_chapter(title, chapter)
    except RuntimeError as exc:
        print(f"  WARN ch-{chapter} TOC: {exc}", file=sys.stderr)
        return (0, 0, chapter)
    ok = 0
    skipped = 0
    for section in sections:
        url = f"{BASE}/{title}/{chapter}/{chapter}-{section}.htm"
        try:
            html = _http_get(url)
        except RuntimeError as exc:
            print(f"  WARN {chapter}:{section}: {exc}", file=sys.stderr)
            skipped += 1
            continue
        if html is None:
            skipped += 1
            continue
        heading, body = parse_section_page(html, chapter, section)
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


def list_all_chapters(
    chapter_filter: set[str] | None = None,
) -> list[tuple[str, str]]:
    """Walk all titles and return ``(title, chapter)`` pairs.

    Skips title TOCs that fail to fetch — a transient server outage on one
    title should not kill the whole run. If ``chapter_filter`` is set, stops
    once every filter value has been located.
    """
    try:
        titles = list_titles()
    except RuntimeError as exc:
        print(f"WARN top TOC: {exc}", file=sys.stderr)
        return []
    pairs: list[tuple[str, str]] = []
    remaining = set(chapter_filter) if chapter_filter else None
    for t in titles:
        try:
            chunk = list_chapters_in_title(t)
        except RuntimeError as exc:
            print(f"WARN title {t}: {exc}", file=sys.stderr)
            continue
        pairs.extend(chunk)
        if remaining is not None:
            for _, ch in chunk:
                remaining.discard(ch)
            if not remaining:
                break
    return pairs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-nh"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '1,1-A,21-V').",
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
    print(f"Discovering chapters (filter={sorted(chapter_filter) if chapter_filter else 'none'})...", flush=True)
    pairs = list_all_chapters(chapter_filter)
    if chapter_filter:
        pairs = [(t, c) for t, c in pairs if c in chapter_filter]
    if args.limit:
        pairs = pairs[: args.limit]
    print(f"Scraping {len(pairs)} chapters", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_chapter, t, c, args.out): (t, c) for t, c in pairs
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
        f"\nDONE - {total_ok} sections scraped, "
        f"{total_skipped} skipped, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
