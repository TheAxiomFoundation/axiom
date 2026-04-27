#!/usr/bin/env python3
"""Convert Alaska Statutes to Akoma Ntoso XML.

This script fetches Alaska Statutes from akleg.gov using their AJAX API
and converts them to Akoma Ntoso XML format in /tmp/rules-us-ak-akn/.

API Endpoints discovered:
- TOC: statutes.asp?media=js&type=TOC&title=43
- Chapter TOC: statutes.asp?media=js&type=TOC&title=43.23
- Section content: statutes.asp?media=print&secStart=43.23.005&secEnd=43.23.005

Usage:
    python scripts/ak_to_akn.py

By default, converts all titles (1-47).
"""

import re
import time
from datetime import date
from pathlib import Path
from xml.dom import minidom
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-ak-akn")

# Base URL for Alaska Legislature
BASE_URL = "https://www.akleg.gov/basis/statutes.asp"

# All Alaska titles
AK_TITLES = {
    1: "General Provisions",
    2: "Aeronautics",
    3: "Agriculture, Animals, and Food",
    4: "Alcoholic Beverages",
    5: "Amusements and Sports",
    6: "Banks and Financial Institutions",
    7: "Boroughs",
    8: "Business and Professions",
    9: "Code of Civil Procedure",
    10: "Corporations and Associations",
    11: "Criminal Law",
    12: "Code of Criminal Procedure",
    13: "Decedents' Estates, Guardianships, Transfers, Trusts, and Health Care Decisions",
    14: "Education, Libraries, and Museums",
    15: "Elections",
    16: "Fish and Game",
    17: "Food and Drugs",
    18: "Health, Safety, Housing, Human Rights, and Public Defender",
    19: "Highways and Ferries",
    20: "Infants and Incompetents",
    21: "Insurance",
    22: "Judiciary",
    23: "Labor and Workers' Compensation",
    24: "Legislature and Lobbying",
    25: "Marital and Domestic Relations",
    26: "Military Affairs, Veterans, Disasters, and Aerospace",
    27: "Mining",
    28: "Motor Vehicles",
    29: "Municipal Government",
    30: "Navigation, Harbors, Shipping, and Transportation Facilities",
    31: "Oil and Gas",
    32: "Partnership",
    33: "Probation, Prisons, Pardons, and Prisoners",
    34: "Property",
    35: "Public Buildings, Works, and Improvements",
    36: "Public Contracts",
    37: "Public Finance",
    38: "Public Land",
    39: "Public Officers and Employees",
    40: "Public Records and Recorders",
    41: "Public Resources",
    42: "Public Utilities and Carriers and Energy Programs",
    43: "Revenue and Taxation",
    44: "State Government",
    45: "Trade and Commerce",
    46: "Water, Air, Energy, and Environmental Conservation",
    47: "Welfare, Social Services, and Institutions",
}


class AKFetcher:
    """Fetches Alaska Statutes from akleg.gov AJAX API."""

    def __init__(self, rate_limit: float = 0.5):
        self.rate_limit = rate_limit
        self.last_request = 0.0
        self.client = httpx.Client(
            timeout=60.0,
            headers={"User-Agent": "Arch/1.0 (Statute Research; contact@axiom-foundation.org)"},
            follow_redirects=True,
        )

    def _rate_limit_wait(self):
        elapsed = time.time() - self.last_request
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request = time.time()

    def get_title_chapters(self, title: int) -> list[dict]:
        """Get list of chapters in a title.

        Returns list of dicts with 'chapter' and 'title' keys.
        """
        self._rate_limit_wait()
        url = f"{BASE_URL}?media=js&type=TOC&title={title}"
        response = self.client.get(url)
        response.raise_for_status()

        chapters = []
        # Parse: loadTOC("43.05"); href=javascript:void(0) ><b>Chapter 05. Administration of Revenue Laws
        pattern = re.compile(r'loadTOC\("(\d+\.\d+)"\).*?Chapter\s+(\d+[A-Za-z]?)\.\s*([^<]+)<')
        for match in pattern.finditer(response.text):
            chapter_id = match.group(1)  # e.g., "43.05"
            chapter_num = match.group(2)  # e.g., "05"
            chapter_title = match.group(3).strip().rstrip(".")
            chapters.append(
                {
                    "chapter_id": chapter_id,
                    "chapter_num": chapter_num,
                    "chapter_title": chapter_title,
                }
            )

        return chapters

    def get_chapter_sections(self, chapter_id: str) -> list[dict]:
        """Get list of sections in a chapter.

        Args:
            chapter_id: e.g., "43.23"

        Returns list of dicts with 'section_num' and 'section_title' keys.
        """
        self._rate_limit_wait()
        url = f"{BASE_URL}?media=js&type=TOC&title={chapter_id}"
        response = self.client.get(url)
        response.raise_for_status()

        sections = []
        # Parse: Sec. 43.23.005.   Eligibility.
        pattern = re.compile(r"#(\d+\.\d+\.\d+[A-Za-z]?)\s*>Sec\.\s+[\d.]+[A-Za-z]?\.\s+([^<]+)<")
        for match in pattern.finditer(response.text):
            section_num = match.group(1)  # e.g., "43.23.005"
            section_title = match.group(2).strip().rstrip(".")

            # Skip repealed sections
            if "[Repealed" in section_title or "[Renumbered" in section_title:
                continue

            sections.append(
                {
                    "section_num": section_num,
                    "section_title": section_title,
                }
            )

        return sections

    def get_section_content(self, section_num: str) -> str:
        """Get the HTML content of a section.

        Args:
            section_num: e.g., "43.23.005"

        Returns raw HTML content.
        """
        self._rate_limit_wait()
        url = f"{BASE_URL}?media=print&secStart={section_num}&secEnd={section_num}"
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    def close(self):
        self.client.close()


def parse_section_html(html: str) -> dict:
    """Parse section HTML into structured data.

    Returns dict with:
    - section_num: e.g., "43.23.005"
    - section_title: e.g., "Eligibility"
    - text: Full text content
    - subsections: List of subsection dicts
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the statute div
    statute_div = soup.find("div", class_="statute")
    if not statute_div:
        return None

    # Get section number and title from the anchor
    anchor = statute_div.find("a", attrs={"name": True})
    if not anchor:
        return None

    section_num = anchor.get("name", "").strip()

    # Get title from bold text
    bold = statute_div.find("b")
    if bold:
        title_text = bold.get_text(strip=True)
        # Parse "Sec. 43.23.005.   Eligibility."
        title_match = re.search(r"Sec\.\s+[\d.]+[A-Za-z]?\.\s+(.+)", title_text)
        section_title = title_match.group(1).strip() if title_match else ""
    else:
        section_title = ""

    # Get full text content
    text = statute_div.get_text(separator="\n", strip=True)

    # Parse subsections
    subsections = parse_subsections(text)

    return {
        "section_num": section_num,
        "section_title": section_title,
        "text": text,
        "subsections": subsections,
    }


def parse_subsections(text: str) -> list[dict]:
    """Parse hierarchical subsections from text.

    Alaska uses:
    - (a), (b), (c) for primary divisions
    - (1), (2), (3) for secondary
    - (A), (B), (C) for tertiary
    """
    subsections = []

    # Split by top-level subsections (a), (b), etc.
    parts = re.split(r"(?=\n\s*\([a-z]\)\s)", text)

    for part in parts[1:]:  # Skip content before first (a)
        match = re.match(r"\s*\(([a-z])\)\s*", part)
        if not match:
            continue

        identifier = match.group(1)
        content = part[match.end() :]

        # Get text before first child
        first_num = re.search(r"\n\s*\(\d+\)", content)
        if first_num:
            direct_text = content[: first_num.start()].strip()
            rest = content[first_num.start() :]
        else:
            direct_text = content.strip()
            rest = ""

        # Parse level 2 children
        children = []
        if rest:
            level2_parts = re.split(r"(?=\n\s*\(\d+\)\s)", rest)
            for l2_part in level2_parts[1:]:
                l2_match = re.match(r"\s*\((\d+)\)\s*", l2_part)
                if l2_match:
                    l2_id = l2_match.group(1)
                    l2_content = l2_part[l2_match.end() :].strip()

                    # Stop at next alpha subsection
                    next_alpha = re.search(r"\n\s*\([a-z]\)", l2_content)
                    if next_alpha:
                        l2_content = l2_content[: next_alpha.start()]

                    children.append(
                        {
                            "identifier": l2_id,
                            "text": l2_content[:2000],
                            "children": [],
                        }
                    )

        subsections.append(
            {
                "identifier": identifier,
                "text": direct_text[:2000],
                "children": children,
            }
        )

    return subsections


def create_akn_xml(
    title_num: int,
    title_name: str,
    chapter_num: str,
    chapter_title: str,
    sections: list[dict],
) -> str:
    """Create Akoma Ntoso XML from parsed sections."""
    # Register namespace
    ET.register_namespace("akn", AKN_NS)
    ET.register_namespace("", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "AS")  # Alaska Statutes

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#arch")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-ak/act/as/title-{title_num}/chapter-{chapter_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-ak/act/as/title-{title_num}/chapter-{chapter_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#alaska-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ak")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", f"{title_num}.{chapter_num}")
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "AS")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value",
        f"/akn/us-ak/act/as/title-{title_num}/chapter-{chapter_num}/eng@{date.today().isoformat()}",
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value",
        f"/akn/us-ak/act/as/title-{title_num}/chapter-{chapter_num}/eng@{date.today().isoformat()}",
    )
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", date.today().isoformat())
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#arch")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set(
        "value",
        f"/akn/us-ak/act/as/title-{title_num}/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value",
        f"/akn/us-ak/act/as/title-{title_num}/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", date.today().isoformat())
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#arch")

    # References
    refs = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    refs.set("source", "#arch")

    # TLC references
    arch_ref = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    arch_ref.set("eId", "arch")
    arch_ref.set("href", "https://axiom-foundation.org")
    arch_ref.set("showAs", "Atlas")

    ak_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    ak_leg.set("eId", "alaska-legislature")
    ak_leg.set("href", "https://www.akleg.gov")
    ak_leg.set("showAs", "Alaska State Legislature")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Title container
    title_elem = ET.SubElement(body, f"{{{AKN_NS}}}title")
    title_elem.set("eId", f"title_{title_num}")

    # Title number
    num = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
    num.text = str(title_num)

    # Title heading
    heading = ET.SubElement(title_elem, f"{{{AKN_NS}}}heading")
    heading.text = title_name

    # Chapter container
    chapter_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}chapter")
    chapter_elem.set("eId", f"title_{title_num}__chp_{chapter_num}")

    # Chapter number
    chp_num = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}num")
    chp_num.text = chapter_num

    # Chapter heading
    chp_heading = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}heading")
    chp_heading.text = chapter_title

    # Add sections
    for section in sections:
        add_section_to_xml(chapter_elem, section, title_num, chapter_num)

    # Convert to string with pretty print
    xml_str = ET.tostring(root, encoding="unicode")

    try:
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ", encoding="UTF-8")
        lines = pretty_xml.decode("utf-8").split("\n")
        cleaned = [line for line in lines if line.strip()]
        return "\n".join(cleaned)
    except Exception:
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str


def add_section_to_xml(parent: ET.Element, section: dict, title_num: int, chapter_num: str) -> None:
    """Add a section element to the parent XML element."""
    sec_id = section["section_num"].replace(".", "-")

    sec_elem = ET.SubElement(parent, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", f"sec_{sec_id}")

    # Section number
    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = section["section_num"]

    # Section heading
    heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    heading.text = section["section_title"]

    # Content
    if section.get("subsections"):
        # Add subsections
        for sub in section["subsections"]:
            add_subsection_to_xml(sec_elem, sub, sec_id, level=1)
    elif section.get("text"):
        # Plain content
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = section["text"][:10000] if section["text"] else ""


def add_subsection_to_xml(parent: ET.Element, subsection: dict, parent_id: str, level: int) -> None:
    """Add a subsection element to the parent XML element."""
    identifier = subsection["identifier"]
    sub_id = f"{parent_id}__subsec_{identifier}"

    if level == 1:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    # Number/identifier
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"({identifier})"

    # Text content
    if subsection.get("text"):
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection["text"][:10000]

    # Children
    for child in subsection.get("children", []):
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def convert_chapter(
    fetcher: AKFetcher,
    title_num: int,
    chapter_info: dict,
    output_dir: Path,
) -> dict:
    """Convert a single chapter to Akoma Ntoso XML."""
    chapter_id = chapter_info["chapter_id"]
    chapter_num = chapter_info["chapter_num"]
    chapter_title = chapter_info["chapter_title"]

    try:
        # Get sections in this chapter
        section_list = fetcher.get_chapter_sections(chapter_id)

        if not section_list:
            return {
                "title": title_num,
                "chapter": chapter_num,
                "sections": 0,
                "success": True,
                "error": None,
            }

        # Fetch content for each section
        sections = []
        for sec_info in section_list:
            try:
                html = fetcher.get_section_content(sec_info["section_num"])
                parsed = parse_section_html(html)
                if parsed:
                    sections.append(parsed)
            except Exception as e:
                print(f"    Warning: Could not fetch {sec_info['section_num']}: {e}")
                continue

        if not sections:
            return {
                "title": title_num,
                "chapter": chapter_num,
                "sections": 0,
                "success": True,
                "error": None,
            }

        # Get title name
        title_name = AK_TITLES.get(title_num, f"Title {title_num}")

        # Create AKN XML
        xml_content = create_akn_xml(title_num, title_name, chapter_num, chapter_title, sections)

        # Write output
        output_path = output_dir / f"as-title-{title_num:02d}-chapter-{chapter_num}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {
            "title": title_num,
            "chapter": chapter_num,
            "sections": len(sections),
            "success": True,
            "error": None,
        }

    except Exception as e:
        return {
            "title": title_num,
            "chapter": chapter_num,
            "sections": 0,
            "success": False,
            "error": str(e),
        }


def main(start_title: int = 1):
    """Convert Alaska Statutes to Akoma Ntoso XML."""
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Alaska Statutes to Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Starting from title: {start_title}")
    print()

    total_sections = 0
    successful_chapters = 0
    failed_chapters = 0
    empty_chapters = 0

    fetcher = AKFetcher(rate_limit=0.5)

    try:
        for title_num in sorted(AK_TITLES.keys()):
            if title_num < start_title:
                continue
            title_name = AK_TITLES[title_num]
            print(f"\nTitle {title_num}: {title_name}")
            print("-" * 40)

            # Get chapters for this title
            try:
                chapters = fetcher.get_title_chapters(title_num)
            except Exception as e:
                print(f"  [FAIL] Could not get chapters: {e}")
                continue

            if not chapters:
                print("  [--] No chapters found")
                continue

            for chapter_info in chapters:
                result = convert_chapter(fetcher, title_num, chapter_info, OUTPUT_DIR)

                if result["success"]:
                    if result["sections"] > 0:
                        successful_chapters += 1
                        total_sections += result["sections"]
                        print(f"  [OK] Chapter {result['chapter']}: {result['sections']} sections")
                    else:
                        empty_chapters += 1
                        print(f"  [--] Chapter {result['chapter']}: no sections found")
                else:
                    failed_chapters += 1
                    print(f"  [FAIL] Chapter {result['chapter']}: {result['error']}")
    finally:
        fetcher.close()

    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Successful chapters:   {successful_chapters}")
    print(f"  Empty chapters:        {empty_chapters}")
    print(f"  Failed chapters:       {failed_chapters}")
    print(f"  Total sections:        {total_sections}")
    print(f"  Output directory:      {OUTPUT_DIR}")

    # List output files
    output_files = list(OUTPUT_DIR.glob("*.xml"))
    print(f"  Output XML files:      {len(output_files)}")


if __name__ == "__main__":
    import sys

    start = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    main(start)
