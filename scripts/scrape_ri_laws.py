"""Scrape the Rhode Island General Laws from webserver.rilegislature.gov.

Source layout
-------------
The RIGL is served by a LexisNexis-generated static site rooted at
``https://webserver.rilegislature.gov/Statutes/``. The hierarchy is:

1. Root index at ``/Statutes/`` lists each title as
   ``TITLE{N}/INDEX.HTM`` (e.g. ``TITLE1``, ``TITLE6A``, ``TITLE40.1``).
2. Each title index at ``/Statutes/TITLE{N}/INDEX.HTM`` links to chapter
   indexes as ``{N}-{CHAP}/INDEX.htm`` where ``{CHAP}`` can be a plain
   integer (``1``, ``2``) or a decimal-suffixed token (``4.1``, ``16.1``).
3. Each chapter index at ``/Statutes/TITLE{N}/{N}-{CHAP}/INDEX.htm`` links
   to per-section HTML files as ``{N}-{CHAP}-{SEC}.htm``. ``{SEC}`` can be
   a plain integer (``1``, ``101``) or a decimal-suffixed token (``1.1``,
   ``17.2``).
4. Each section page is a tiny (~4 KB) static HTML doc with the body
   structured as::

       <h1>...Title X...</h1>
       <h2>...Chapter Y...</h2>
       <h3>R.I. Gen. Laws § {section}</h3>
       <div>
         <p><b>§ {section}. {heading}.</b></p>
         <p><b>(a)</b>&nbsp;Body text...</p>
         ...
         <div><p>History of Section.<br>...</p></div>
       </div>

Repealed sections publish a body that reads only "History of Section."
with no substantive text; those are skipped as empty bodies.

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{title}/ch-{title}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state ri`` expects. Here
``{title}`` is the RI title token (e.g. ``1``, ``6A``, ``40.1``) and
``{section}`` is the full section id with dashes replaced by underscores
for filename safety (e.g. ``1-2-17_1`` for § 1-2-17.1).

Usage
-----
::

    uv run python scripts/scrape_ri_laws.py --out /tmp/rules-us-ri
    uv run python scripts/scrape_ri_laws.py --out /tmp/rules-us-ri --titles 1,2
    uv run python scripts/scrape_ri_laws.py --out /tmp/rules-us-ri --titles 1 --limit 5
"""

from __future__ import annotations

import argparse
import html as _html
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

BASE = "https://webserver.rilegislature.gov/Statutes"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> str | None:
    """GET a URL as UTF-8 text. Returns None on 404/410 so callers can skip."""
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


def list_titles() -> list[str]:
    """Return RI title tokens (``1``, ``6A``, ``40.1``, ...) from the root index."""
    html = _http_get(f"{BASE}/")
    if html is None:
        return []
    # Anchor hrefs look like TITLE1/INDEX.HTM, TITLE6A/INDEX.HTM,
    # TITLE40.1/INDEX.HTM (case-insensitive).
    tokens = set(
        re.findall(r'href="TITLE([0-9A-Za-z.]+)/INDEX\.HTM"', html, re.IGNORECASE)
    )
    # Natural sort: numeric prefix then any alphanumeric/decimal suffix.
    def key(t: str) -> tuple[int, str]:
        m = re.match(r"(\d+)(.*)", t)
        return (int(m.group(1)), m.group(2)) if m else (999999, t)

    return sorted(tokens, key=key)


def list_chapter_tokens(title: str) -> list[str]:
    """Return chapter tokens (e.g. ``1``, ``4.1``, ``18.1``) under a title."""
    html = _http_get(f"{BASE}/TITLE{title}/INDEX.HTM")
    if html is None:
        return []
    # Chapter index hrefs look like href="1-2/INDEX.htm" (lowercase .htm) or
    # href="44-18.1/INDEX.htm". The chapter portion is what follows the
    # "{title}-" prefix.
    prefix = re.escape(title) + r"-"
    tokens = set(
        re.findall(
            rf'href="{prefix}([0-9A-Za-z.]+)/INDEX\.htm"', html, re.IGNORECASE
        )
    )

    def key(t: str) -> tuple[int, str]:
        m = re.match(r"(\d+)(.*)", t)
        return (int(m.group(1)), m.group(2)) if m else (999999, t)

    return sorted(tokens, key=key)


def list_section_tokens(title: str, chapter: str) -> list[str]:
    """Return section tokens (e.g. ``1``, ``17.1``) under a title-chapter pair."""
    html = _http_get(f"{BASE}/TITLE{title}/{title}-{chapter}/INDEX.htm")
    if html is None:
        return []
    # Section links look like href="1-2-1.htm", href="1-2-17.1.htm".
    prefix = re.escape(f"{title}-{chapter}-")
    tokens = set(
        re.findall(
            rf'href="{prefix}([0-9A-Za-z.]+)\.htm"', html, re.IGNORECASE
        )
    )

    def key(t: str) -> tuple[int, float]:
        # Sort by integer part then decimal suffix (so 17 < 17.1 < 17.2 < 18).
        m = re.match(r"(\d+)(?:\.(\d+))?(.*)", t)
        if not m:
            return (999999, 0.0)
        base = int(m.group(1))
        dec = float(f"0.{m.group(2)}") if m.group(2) else 0.0
        return (base, dec)

    return sorted(tokens, key=key)


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# RIGL pages are delivered in UTF-8 and use the literal § character rather
# than &sect;. We also tolerate &nbsp; / U+00A0 padding around the section
# number, which LexisNexis inserts for visual spacing.
_SECT = r"(?:§|&sect;|&#167;|&#xa7;)"
_SPACE = r"(?:\s|&nbsp;|\xa0)+"

# The section page pins the citation in its own <h3>.
_CITATION_RE = re.compile(
    rf"<h3[^>]*>\s*R\.I\.\s*Gen\.\s*Laws\s*{_SECT}{_SPACE}"
    r"([0-9A-Za-z.\-]+)\s*</h3>",
    re.IGNORECASE,
)

# Header paragraph: <p ...><b>§ {section}. {heading}.</b></p>. The section
# number is repeated from the citation <h3> and the heading follows an
# &nbsp;-padded period.
_HEADER_RE = re.compile(
    rf"<p[^>]*>\s*<b>\s*{_SECT}{_SPACE}(?P<section>[0-9A-Za-z.\-]+)\s*\."
    rf"{_SPACE}?(?P<heading>.*?)\s*</b>\s*</p>",
    re.DOTALL | re.IGNORECASE,
)


def parse_section(html: str) -> tuple[str, str, str] | None:
    """Extract ``(section, heading, body)`` from one section page.

    Returns None if we can't find a bold header line (non-section pages).
    Returns an empty body for repealed sections — callers decide to skip.
    """
    header = _HEADER_RE.search(html)
    if header is None:
        # Fall back to the citation header and leave heading blank.
        cit = _CITATION_RE.search(html)
        if cit is None:
            return None
        section = cit.group(1).strip()
        heading = ""
        body_start = cit.end()
    else:
        section = header.group("section").strip()
        heading = _clean_text(header.group("heading")).rstrip(".")
        body_start = header.end()

    body_html = html[body_start:]
    # Strip the trailing "History of Section" block. RIGL always wraps it in
    # a <div><p>History of Section.<br>...</p></div> at the tail of the
    # body <div>. We drop everything from the first "History of Section"
    # onward because it's provenance metadata, not statute text.
    body_html = re.sub(
        r"<div[^>]*>\s*<p[^>]*>\s*History of Section\..*?</body>",
        "</body>",
        body_html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # Belt-and-suspenders: if the wrapping <div> form doesn't match (e.g.
    # rare pages that don't wrap the history), still strip the paragraph
    # by itself.
    body_html = re.sub(
        r"<p[^>]*>\s*History of Section\..*?</p>",
        "",
        body_html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # Drop closing </body></html>.
    body_html = re.sub(
        r"</body>.*", "", body_html, flags=re.IGNORECASE | re.DOTALL
    )
    body = _clean_text(body_html)
    # Trim leading lone periods / em-dashes that HTML whitespace leaves.
    body = body.strip().strip("—–-").strip()
    return section, heading, body


def build_akn_xml(
    title: str, section: str, heading: str, body: str
) -> str:
    citation = f"R.I. Gen. Laws § {section}"
    safe_section = section.replace(".", "_").replace("-", "_")
    eid = f"sec_{title.replace('.', '_')}_{safe_section}"
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
          <FRBRthis value="/akn/us-ri/act/rigl/{section}"/>
          <FRBRuri value="/akn/us-ri/act/rigl/{section}"/>
          <FRBRauthor href="#ri-legislature"/>
          <FRBRcountry value="us-ri"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="RIGL"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-ri/act/rigl/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-ri/act/rigl/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-ri/act/rigl/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-ri/act/rigl/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="ri-legislature" href="https://webserver.rilegislature.gov" showAs="Rhode Island General Assembly"/>
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
    title: str, chapter: str, section_token: str, out_root: Path
) -> tuple[bool, str]:
    """Fetch and write one section. Returns (ok, reason-or-section-id)."""
    url = f"{BASE}/TITLE{title}/{title}-{chapter}/{title}-{chapter}-{section_token}.htm"
    html = _http_get(url)
    if html is None:
        return (False, "404")
    parsed = parse_section(html)
    if parsed is None:
        return (False, "no header")
    section, heading, body = parsed
    if not body:
        return (False, "empty body")
    safe_section = section.replace(".", "_").replace("-", "_").replace("/", "_")
    # Title folder uses the RI title token verbatim but with "." swapped to "_"
    # so paths like ch-40_1/... remain filesystem-safe.
    title_dir = title.replace(".", "_")
    dest = (
        out_root
        / "statutes"
        / f"ch-{title_dir}"
        / f"ch-{title_dir}-sec-{safe_section}.xml"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        build_akn_xml(title, section, heading, body), encoding="utf-8"
    )
    return (True, section)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-ri"))
    parser.add_argument(
        "--titles",
        default="",
        help="Comma-separated title tokens (e.g. '1,2,6A,40.1'). "
        "Default: all titles discovered from the root index.",
    )
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N sections per title (smoke-test convenience).",
    )
    args = parser.parse_args(argv)

    if args.titles:
        titles = [t.strip() for t in args.titles.split(",") if t.strip()]
    else:
        titles = list_titles()

    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    total_ok = 0
    total_failed = 0

    for title in titles:
        chapters = list_chapter_tokens(title)
        if not chapters:
            print(f"  title {title}: 0 chapters (skipping)", flush=True)
            continue

        # Expand (chapter, section) pairs.
        pairs: list[tuple[str, str]] = []
        for chapter in chapters:
            sections = list_section_tokens(title, chapter)
            for section_token in sections:
                pairs.append((chapter, section_token))
            if args.limit is not None and len(pairs) >= args.limit:
                pairs = pairs[: args.limit]
                break

        if not pairs:
            print(f"  title {title}: 0 sections (skipping)", flush=True)
            continue

        ok = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(scrape_section, title, ch, sec, args.out): (ch, sec)
                for (ch, sec) in pairs
            }
            for fut in as_completed(futures):
                ok_flag, _msg = fut.result()
                if ok_flag:
                    ok += 1
                else:
                    failed += 1

        total_ok += ok
        total_failed += failed
        elapsed = (time.time() - started) / 60
        print(
            f"  title {title}: {ok} ok, {failed} skip  "
            f"(running: {total_ok} ok, {elapsed:.1f} min)",
            flush=True,
        )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE — {total_ok} sections scraped, "
        f"{total_failed} skipped, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
