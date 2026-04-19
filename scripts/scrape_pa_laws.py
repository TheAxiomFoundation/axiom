"""Scrape Pennsylvania Consolidated Statutes from legis.state.pa.us.

Source layout
-------------
Each title lives at ``https://www.legis.state.pa.us/WU01/LI/LI/CT/HTM/{NN}/{NN}.HTM``.
PA distinguishes consolidated titles (body text embedded in the TOC HTML)
from unconsolidated titles (TOC-only, body text not online as HTML).
We pick out only titles whose page contains substantive body text.

Sections in consolidated titles render as ``&#167; {N}. &nbsp;Heading.``
followed by body paragraphs until the next ``&#167;`` marker.

Output
------
AKN-3.0 XML at ``{out}/statutes/tt-{title}/tt-{title}-sec-{section}.xml``.
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

BASE = "https://www.legis.state.pa.us"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# PA uses 1-82 for consolidated titles plus a few alpha-suffix variants
# we can't easily enumerate; iterate 0-82 and let missing titles 404.
PA_TITLES = list(range(1, 83))

# If a title page's visible text is below this threshold, treat it as
# unconsolidated TOC-only and skip. Empirically 1.8 MB visible for Title
# 18 (content-rich) vs ~200 KB for TOC-only titles.
MIN_VISIBLE_CHARS = 300_000


def _http_get(url: str, retries: int = 3) -> str | None:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(min(8.0, 2.0**attempt))
    print(f"  WARN {url}: {last_exc}", file=sys.stderr, flush=True)
    return None


def _clean_text(s: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# Section marker: &#167; N. Heading... body text... until next &#167;.
# Section ids look like 101, 901, 1301, 5523.1, etc.
_SECTION_SPLIT = re.compile(r"&#167;\s*(?P<num>\d+[\w.\-]*)\s*\.")


def split_sections(html: str, title: str) -> list[tuple[str, str, str]]:
    """Return ``(section, heading, body)`` tuples from a title's HTML.

    PA title HTML contains each section TWICE — once in the TOC at top,
    once as the body element lower down. Dedupe by section number
    keeping the LAST occurrence (which has the real body text); TOC
    entries are back-to-back short headings, bodies are longer and
    come later in the document.
    """
    all_matches = list(_SECTION_SPLIT.finditer(html))
    # Keep only the last occurrence of each section number.
    last: dict[str, int] = {}
    for i, m in enumerate(all_matches):
        last[m.group("num")] = i
    matches = [all_matches[i] for i in sorted(last.values())]
    results: list[tuple[str, str, str]] = []
    for i, m in enumerate(matches):
        section = m.group("num")
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(html)
        slab = html[body_start:body_end]
        # Heading = first chunk until the first "." or <br>; body = rest.
        # In PA's layout the heading ends at "." then &nbsp;&nbsp; body.
        # Extract until the heading-ending period.
        clean = _clean_text(slab)
        # Strip leading "&nbsp;" artifact & whitespace
        clean = clean.lstrip()
        if not clean:
            continue
        # Heading: everything up to the first period followed by whitespace.
        h_m = re.match(r"(.+?)\.\s+", clean)
        if h_m:
            heading = h_m.group(1).strip().rstrip(".").strip()
            body = clean[h_m.end() :].strip()
        else:
            # No body — TOC-only entry for this section.
            heading = clean.rstrip(".").strip()
            body = ""

        # Trim trailing TOC accumulations (next section's heading might
        # leak in via the next iteration — guard by stripping any trailing
        # reference to the next section).
        # Also PA body text often ends with "(Source note...)" but not
        # always; leave as-is.
        results.append((section, heading, body))
    return results


def build_akn_xml(
    title: str, section: str, heading: str, body: str
) -> str:
    citation = f"{title} Pa.C.S. § {section}"
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
          <FRBRthis value="/akn/us-pa/act/pacs/{title}-{section}"/>
          <FRBRuri value="/akn/us-pa/act/pacs/{title}-{section}"/>
          <FRBRauthor href="#pa-legislature"/>
          <FRBRcountry value="us-pa"/>
          <FRBRnumber value="{title}-{section}"/>
          <FRBRname value="PaCS"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-pa/act/pacs/{title}-{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-pa/act/pacs/{title}-{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-pa/act/pacs/{title}-{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-pa/act/pacs/{title}-{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="pa-legislature" href="https://www.legis.state.pa.us" showAs="Pennsylvania General Assembly"/>
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


def scrape_title(title: int, out_root: Path) -> tuple[int, int]:
    url = f"{BASE}/WU01/LI/LI/CT/HTM/{title:02d}/{title:02d}.HTM"
    html = _http_get(url)
    if html is None:
        return (0, 0)
    visible_len = len(re.sub(r"<[^>]+>", " ", html))
    if visible_len < MIN_VISIBLE_CHARS:
        print(
            f"  title {title}: unconsolidated (visible {visible_len:,} < {MIN_VISIBLE_CHARS:,}) — skip",
            flush=True,
        )
        return (0, 0)

    sections = split_sections(html, str(title))
    ok = 0
    empty = 0
    title_token = f"{title:02d}"
    for section, heading, body in sections:
        if not body or len(body) < 30:
            empty += 1
            continue
        safe = section.replace("/", "_")
        dest = (
            out_root
            / "statutes"
            / f"tt-{title_token}"
            / f"tt-{title_token}-sec-{safe}.xml"
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(
            build_akn_xml(title_token, section, heading, body), encoding="utf-8"
        )
        ok += 1
    print(
        f"  title {title}: {ok} ok, {empty} toc-only  "
        f"(html={len(html):,}B)",
        flush=True,
    )
    return (ok, empty)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-pa"))
    parser.add_argument("--titles", default="", help="Comma-separated title numbers.")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)

    titles: list[int]
    if args.titles:
        titles = [int(t) for t in args.titles.split(",") if t.strip()]
    else:
        titles = PA_TITLES

    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    total_ok = 0
    total_skipped = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(scrape_title, t, args.out): t for t in titles}
        for fut in as_completed(futures):
            ok, skipped = fut.result()
            total_ok += ok
            total_skipped += skipped

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE — {total_ok} sections scraped, "
        f"{total_skipped} toc-only, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
