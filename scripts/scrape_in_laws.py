"""Scrape the Indiana Code from iga.in.gov.

Source layout
-------------
``iga.in.gov`` is a React SPA whose ``/laws/{year}/ic/titles/{N}`` route loads
two companion assets served from the same origin:

1. ``https://iga.in.gov/ic/{year}/Title_{N}.json`` — structured menu with one
   entry per title / article / chapter / section::

       [
         {"type":"title",   "number":"2",       "name":"TITLE 2. ..."},
         {"type":"article", "number":"2-1",     "name":"ARTICLE 1. ..."},
         {"type":"chapter", "number":"2-1-1",   "name":"Chapter 1. Repealed"},
         ...
       ]

2. ``https://iga.in.gov/ic/{year}/Title_{N}.html`` — the full title as a
   WordPerfect-exported HTML bundle. Each section begins with::

       <div class="section" id="2-2.2-2-1" ...>
         <span id="ic_number">IC 2-2.2-2-1</span>
         <span id="shortdescription">Deadline for filing ...</span>
       </div>
       <p>Sec. 1. (a) ...body paragraphs...</p>
       <p><i>As added by P.L.123-2015, SEC.2.</i></p>
       ...next <div class="section"> or <div class="chapter"> boundary.

   The trailing ``<i>As added by ...</i>`` / ``<i>Amended by ...</i>`` paragraph
   is publication-history metadata we drop like NV's ``SourceLine``.

Indiana Code titles 1-36 exist (with gaps — requesting a non-existent title
returns the 691-byte SPA shell instead of real HTML/JSON).

Output
------
AKN-3.0 XML at ``{out}/statutes/ch-{title}/ch-{title}-sec-{article}-{chapter}-{section}.xml``,
shape matching what ``ingest_state_laws.py --state in`` expects. The filename
stem after ``sec-`` is the portion of the IC citation *after* the title
(e.g. ``IC 2-2.2-2-1`` -> ``sec-2.2-2-1``); this keeps filenames unique across
articles/chapters within a title while staying close to the user's spec of
``ch-{title}-sec-{section}.xml``.

Usage
-----
::

    uv run python scripts/scrape_in_laws.py --out /tmp/rules-us-in
    uv run python scripts/scrape_in_laws.py --out /tmp/rules-us-in --titles 2,6
"""

from __future__ import annotations

import argparse
import html as _html
import json
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

BASE = "https://iga.in.gov/ic"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Indiana Code has titles 1-36; several numbers are unused (the site returns a
# ~691-byte SPA shell for those rather than a 404). We try every number in
# 1..36 and skip anything that isn't real content.
IN_MAX_TITLE = 36
DEFAULT_YEAR = "2024"
# The SPA shell served for non-existent titles is ~691 bytes. Use a small
# generous floor so we don't mistake a tiny but valid title for the shell.
SHELL_SIZE_THRESHOLD = 2048


def _http_get(url: str, retries: int = 4) -> bytes | None:
    """GET a URL as bytes; soft-fail (return None) on giveup.

    Mirrors the soft-fail pattern from ``scrape_ky_laws.py`` so one flaky
    title doesn't kill a whole run.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=45) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code in (404, 410):
                return None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(min(12.0, 2.0**attempt))
    print(f"  WARN skip {url}: {last_exc}", file=sys.stderr, flush=True)
    return None


def _http_get_text(url: str, retries: int = 4) -> str | None:
    data = _http_get(url, retries=retries)
    if data is None:
        return None
    return data.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def list_titles(year: str) -> list[tuple[str, str]]:
    """Probe Title_{N}.json for N=1..36; return [(title_num, title_name)].

    Silently skips titles whose JSON response is the SPA shell or a stub.
    """
    out: list[tuple[str, str]] = []
    for n in range(1, IN_MAX_TITLE + 1):
        url = f"{BASE}/{year}/Title_{n}.json"
        data = _http_get(url)
        if data is None or len(data) < SHELL_SIZE_THRESHOLD:
            continue
        try:
            entries = json.loads(data.decode("utf-8", errors="replace"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(entries, list) or not entries:
            continue
        first = entries[0]
        if not isinstance(first, dict) or first.get("type") != "title":
            continue
        name = first.get("name") or first.get("shortdescription") or ""
        out.append((str(n), name))
    return out


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span|li)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# Each section is opened by <div class="section" id="{title}-...">. The body
# runs to the next <div class="section"> or <div class="chapter"> (or
# <div class="article">, <div class="title">) or end-of-document.
_SECTION_OPEN = re.compile(
    r'<div\s+class="section"\s+id="(?P<id>[^"]+)"[^>]*>(?P<inner>.*?)</div>',
    re.DOTALL | re.IGNORECASE,
)
_ANY_STRUCTURE_OPEN = re.compile(
    r'<div\s+class="(?:section|chapter|article|title)"\s+id="[^"]+"',
    re.IGNORECASE,
)
_IC_NUMBER = re.compile(
    r'<span\s+id="ic_number"[^>]*>\s*IC\s+(?P<num>[0-9A-Za-z.\-]+)\s*</span>',
    re.IGNORECASE,
)
_SHORT_DESCRIPTION = re.compile(
    r'<span\s+id="shortdescription"[^>]*>(?P<desc>.*?)</span>',
    re.DOTALL | re.IGNORECASE,
)
# Trailing <p><i>As added by .../Amended by .../Formerly: ...</i></p> blocks
# are publication history - drop them. We match the <i>...</i> content
# starting with any of these history prefixes and remove the whole <p> wrap.
_HISTORY_PARA = re.compile(
    r"<p[^>]*>\s*<span[^>]*>?\s*<i>\s*"
    r"(?:As added by|Amended by|Formerly:|As amended by)"
    r"[^<]*?</i>\s*</span>?\s*</p>",
    re.DOTALL | re.IGNORECASE,
)
# Some trailers just wrap <i> directly in <p> without an inner <span>.
_HISTORY_PARA_SIMPLE = re.compile(
    r"<p[^>]*>\s*<i>\s*"
    r"(?:As added by|Amended by|Formerly:|As amended by)"
    r"[^<]*?</i>\s*</p>",
    re.DOTALL | re.IGNORECASE,
)


def split_sections(
    html: str, title_num: str
) -> list[tuple[str, str, str, str]]:
    """Return ``(div_id, section_num, heading, body)`` for every section.

    ``section_num`` is the full IC citation number (e.g. ``2-2.2-2-1``) from
    the ``<span id="ic_number">`` tag. ``div_id`` is the ``<div class="section"
    id="...">`` value, which may carry a version suffix like ``-b`` for
    successor-version sections that share the citation with a preceding
    ``effective until {date}`` variant. Callers use ``div_id`` as the unique
    filename stem so both versions survive, while ``section_num`` becomes the
    AKN ``<num>`` / ``FRBRnumber`` display.
    """
    out: list[tuple[str, str, str, str]] = []
    # Locate each <div class="section" id="..."> opener
    section_opens = [
        m
        for m in re.finditer(
            r'<div\s+class="section"\s+id="(?P<id>[^"]+)"',
            html,
            re.IGNORECASE,
        )
    ]
    if not section_opens:
        return out

    for i, open_m in enumerate(section_opens):
        sec_id = open_m.group("id")
        # Only keep sections belonging to this title (defensive — the title
        # HTML should already be a single title).
        if not (
            sec_id == title_num
            or sec_id.startswith(f"{title_num}-")
        ):
            continue
        # The body runs from end of this <div class="section" ...></div>
        # opening block through the next section/chapter/article/title
        # structure anchor or EOF.
        start = open_m.start()
        end = section_opens[i + 1].start() if i + 1 < len(section_opens) else None
        # Also stop at the next chapter/article/title divider, if any.
        next_struct = _ANY_STRUCTURE_OPEN.search(html, pos=open_m.end())
        if next_struct and (end is None or next_struct.start() < end):
            end = next_struct.start()
        slab = html[start:end] if end is not None else html[start:]

        # IC number from the <span id="ic_number">
        num_m = _IC_NUMBER.search(slab)
        section_num = num_m.group("num") if num_m else sec_id

        # Heading from the <span id="shortdescription">
        head_m = _SHORT_DESCRIPTION.search(slab)
        heading = _clean_text(head_m.group("desc")) if head_m else ""
        heading = heading.rstrip(".").strip()

        # Body: everything *after* the opening <div class="section">...</div>
        # header. Strip the header itself by cutting past its closing tag.
        # The header div contains two spans then "</div>". Find that first
        # balancing </div>.
        header_end_m = re.search(
            r'</div>\s*(?:<p[^>]*>\s*</p>\s*)*', slab, re.IGNORECASE
        )
        body_html = slab[header_end_m.end():] if header_end_m else slab
        # Drop trailing publication-history paragraphs.
        body_html = _HISTORY_PARA.sub("", body_html)
        body_html = _HISTORY_PARA_SIMPLE.sub("", body_html)

        # Split on </p> boundaries so each paragraph becomes its own <p>.
        # Also treat </li> boundaries as paragraph breaks.
        raw_paras = re.split(r"</p>|</li>", body_html, flags=re.IGNORECASE)
        paras: list[str] = []
        for raw in raw_paras:
            txt = _clean_text(raw)
            if txt:
                paras.append(txt)
        body = "\n\n".join(paras)
        # Trim leftover em-dashes and whitespace.
        body = body.strip().strip("—–-").strip()

        out.append((sec_id, section_num, heading, body))

    return out


# ---------------------------------------------------------------------------
# AKN serialization
# ---------------------------------------------------------------------------


def build_akn_xml(
    title_num: str, section_num: str, heading: str, body: str
) -> str:
    """Build AKN-3.0 XML for one IC section.

    ``section_num`` is the full IC citation number, e.g. ``2-2.2-2-1``.
    """
    citation = f"IC {section_num}"
    safe = section_num.replace(".", "_").replace("-", "_")
    eid = f"sec_{safe}"
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
          <FRBRthis value="/akn/us-in/act/ic/{section_num}"/>
          <FRBRuri value="/akn/us-in/act/ic/{section_num}"/>
          <FRBRauthor href="#in-legislature"/>
          <FRBRcountry value="us-in"/>
          <FRBRnumber value="{section_num}"/>
          <FRBRname value="IC"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-in/act/ic/{section_num}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-in/act/ic/{section_num}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-in/act/ic/{section_num}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-in/act/ic/{section_num}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="in-legislature" href="https://iga.in.gov" showAs="Indiana General Assembly"/>
        <TLCOrganization eId="axiom" href="https://axiom-foundation.org" showAs="Axiom Foundation"/>
      </references>
    </meta>
    <body>
      <section eId="{eid}">
        <num>{xml_escape(citation)}</num>
        <heading>{xml_escape(heading or f"Section {section_num}")}</heading>
        <content>
            {paras_xml}
        </content>
      </section>
    </body>
  </act>
</akomaNtoso>
"""


# ---------------------------------------------------------------------------
# Scrape driver
# ---------------------------------------------------------------------------


def scrape_title(
    title_num: str, year: str, out_root: Path
) -> tuple[int, int, str]:
    """Scrape one IC title's HTML bundle. Return ``(ok, skipped, title_num)``."""
    url = f"{BASE}/{year}/Title_{title_num}.html"
    data = _http_get(url)
    if data is None or len(data) < SHELL_SIZE_THRESHOLD:
        print(
            f"  WARN title {title_num}: no HTML bundle",
            file=sys.stderr,
            flush=True,
        )
        return (0, 0, title_num)
    html = data.decode("utf-8", errors="replace")
    sections = split_sections(html, title_num)
    ok = 0
    skipped = 0
    for div_id, section_num, heading, body in sections:
        if not body:
            skipped += 1
            continue
        # Build the filename stem from the portion of the div id that
        # follows the title number (e.g. div_id "2-2.2-2-1" -> "2.2-2-1",
        # "6-1.1-12-10.1-b" -> "1.1-12-10.1-b"). Using the div id preserves
        # version-variant suffixes so post-effective-date pairs don't clobber
        # each other.
        if div_id.startswith(f"{title_num}-"):
            filename_sec = div_id[len(title_num) + 1 :]
        else:
            filename_sec = div_id
        filename_sec = filename_sec.replace("/", "_")
        xml = build_akn_xml(title_num, section_num, heading, body)
        dest = (
            out_root
            / "statutes"
            / f"ch-{title_num}"
            / f"ch-{title_num}-sec-{filename_sec}.xml"
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(xml, encoding="utf-8")
        ok += 1
    return (ok, skipped, title_num)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-in"))
    parser.add_argument(
        "--year",
        default=DEFAULT_YEAR,
        help=f"IC year (default {DEFAULT_YEAR}).",
    )
    parser.add_argument(
        "--titles",
        default="",
        help="Comma-separated title numbers (e.g. '2,6'). Default: auto-discover.",
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="Parallel title fetches."
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Stop after N titles."
    )
    args = parser.parse_args(argv)

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)

    if args.titles:
        title_nums = [t.strip() for t in args.titles.split(",") if t.strip()]
        titles = [(t, f"Title {t}") for t in title_nums]
    else:
        print(
            f"Discovering IC titles for year {args.year}...",
            file=sys.stderr,
            flush=True,
        )
        titles = list_titles(args.year)
        print(
            f"Found {len(titles)} titles: {', '.join(t for t, _ in titles)}",
            file=sys.stderr,
            flush=True,
        )

    if args.limit:
        titles = titles[: args.limit]
    print(f"Scraping {len(titles)} titles", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_title, t, args.year, args.out): t
            for t, _ in titles
        }
        for fut in as_completed(futures):
            ok, skipped, t = fut.result()
            total_ok += ok
            total_skipped += skipped
            if ok + skipped > 0:
                elapsed = (time.time() - started) / 60
                print(
                    f"  ch-{t}: {ok} ok, {skipped} skip  "
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
