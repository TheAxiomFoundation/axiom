#!/usr/bin/env python3
"""Convert Pennsylvania Consolidated Statutes to Akoma Ntoso XML.

This script fetches PA statutes from palegis.us and converts them
to Akoma Ntoso XML format in /tmp/rules-us-pa-akn/.

Pennsylvania Statute Structure:
- Titles (e.g., Title 72: Taxation and Fiscal Affairs)
- Parts
- Chapters
- Subchapters
- Sections (e.g., S 3116. Microenterprise loans.)

Source: https://www.palegis.us/statutes/consolidated

Usage:
    python scripts/pa_to_akn.py              # Convert all titles
    python scripts/pa_to_akn.py --title 72   # Convert specific title
    python scripts/pa_to_akn.py --tax        # Convert tax titles only
"""

import argparse
import re
import sys
import time
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

import httpx
from bs4 import BeautifulSoup

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-pa-akn")

# Base URL for PA statutes
BASE_URL = "https://www.palegis.us/statutes/consolidated"

# Title mapping for Pennsylvania Consolidated Statutes
PA_TITLES = {
    1: "General Provisions",
    2: "Administrative Law and Procedure",
    3: "Agriculture",
    4: "Amusements",
    5: "Athletics and Sports",
    7: "Banks and Banking",
    8: "Boroughs and Incorporated Towns",
    9: "Burial Grounds",
    10: "Charitable Organizations",
    11: "Cities",
    12: "Commerce and Trade",
    13: "Commercial Code",
    15: "Corporations and Unincorporated Associations",
    16: "Counties",
    17: "Credit Unions",
    18: "Crimes and Offenses",
    19: "Decedents, Estates and Fiduciaries",
    20: "Decedents, Estates and Fiduciaries",
    22: "Detectives and Private Police",
    23: "Domestic Relations",
    24: "Education",
    25: "Elections",
    27: "Environmental Resources",
    30: "Fish",
    32: "Forests, Waters and State Parks",
    34: "Game",
    35: "Health and Safety",
    37: "Historical and Museums",
    38: "Holidays and Observances",
    40: "Insurance",
    42: "Judiciary and Judicial Procedure",
    44: "Law and Justice",
    45: "Legal Notices",
    46: "Legislature",
    48: "Lodges",
    51: "Military Affairs",
    53: "Municipalities Generally",
    54: "Names",
    58: "Oil and Gas",
    61: "Prisons and Parole",
    62: "Procurement",
    63: "Professions and Occupations (State Licensed)",
    64: "Public Authorities and Quasi-Public Corporations",
    65: "Public Officers",
    66: "Public Utilities",
    67: "Public Welfare",
    68: "Real and Personal Property",
    69: "Savings Associations",
    71: "State Government",
    72: "Taxation and Fiscal Affairs",
    73: "Trade and Commerce",
    74: "Transportation",
    75: "Vehicles",
    76: "Veterans and War Veterans' Organizations",
    77: "Workmen's Compensation",
    79: "Zoning and Planning",
}

# Tax-related titles
TAX_TITLES = [72]

# Rate limiting
RATE_LIMIT_DELAY = 0.5
last_request_time = 0.0


def rate_limit():
    """Enforce rate limiting between requests."""
    global last_request_time
    elapsed = time.time() - last_request_time
    if elapsed < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - elapsed)
    last_request_time = time.time()


def fetch_title_html(title: int) -> str:
    """Fetch HTML for a title from palegis.us."""
    rate_limit()
    url = f"{BASE_URL}/view-statute?txtType=HTM&ttl={title}&iFrame=true"

    client = httpx.Client(
        timeout=60.0,
        headers={"User-Agent": "Arch/1.0 (Statute Research; contact@axiom-foundation.org)"},
    )

    try:
        response = client.get(url)
        response.raise_for_status()
        return response.text
    finally:
        client.close()


def extract_sections_from_html(html: str, title: int) -> list[dict]:
    """Extract all sections from title HTML.

    Returns:
        List of section dicts with keys:
        - section_number: e.g., "3116"
        - title: Section title text
        - text: Full text content
        - chapter_number: Optional chapter number
        - chapter_title: Optional chapter title
        - subsections: List of subsection dicts
    """
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text(separator="\n", strip=True)

    sections = []

    # Find all section patterns: S 3116. Title text
    section_pattern = re.compile(r"(?:S|\u00a7)\s*(\d+[A-Za-z]?)\.\s*([^.]+?)\.?\s*[-—]")

    current_chapter = None
    current_chapter_title = None

    # Track chapter context
    for chapter_match in re.finditer(
        r"CHAPTER\s+(\d+[A-Z]?)\s*\n\s*([^\n]+)", full_text, re.IGNORECASE
    ):
        ch_num = chapter_match.group(1)
        ch_title = chapter_match.group(2).strip()
        ch_pos = chapter_match.start()

        # Find sections after this chapter
        next_chapter = re.search(r"CHAPTER\s+\d+", full_text[chapter_match.end() :], re.IGNORECASE)
        end_pos = chapter_match.end() + next_chapter.start() if next_chapter else len(full_text)

        chapter_text = full_text[ch_pos:end_pos]

        for sec_match in section_pattern.finditer(chapter_text):
            section_num = sec_match.group(1)
            section_title = sec_match.group(2).strip()

            # Find section text (until next section or end of chapter)
            sec_start = sec_match.end()
            next_sec = section_pattern.search(chapter_text[sec_start:])
            sec_end = sec_start + next_sec.start() if next_sec else len(chapter_text)

            section_text = chapter_text[sec_start:sec_end].strip()

            # Parse subsections
            subsections = parse_subsections(section_text)

            sections.append(
                {
                    "section_number": section_num,
                    "title": section_title,
                    "text": section_text[:50000],
                    "chapter_number": ch_num,
                    "chapter_title": ch_title,
                    "subsections": subsections,
                }
            )

    # If no chapters found, try to parse sections directly
    if not sections:
        for sec_match in section_pattern.finditer(full_text):
            section_num = sec_match.group(1)
            section_title = sec_match.group(2).strip()

            sec_start = sec_match.end()
            next_sec = section_pattern.search(full_text[sec_start:])
            sec_end = sec_start + next_sec.start() if next_sec else len(full_text)

            section_text = full_text[sec_start:sec_end].strip()
            subsections = parse_subsections(section_text)

            sections.append(
                {
                    "section_number": section_num,
                    "title": section_title,
                    "text": section_text[:50000],
                    "chapter_number": None,
                    "chapter_title": None,
                    "subsections": subsections,
                }
            )

    return sections


def parse_subsections(text: str) -> list[dict]:
    """Parse hierarchical subsections from text.

    Pennsylvania statutes typically use:
    - (a), (b), (c) for primary divisions with optional heading
    - (1), (2), (3) for secondary divisions
    - (i), (ii), (iii) for tertiary divisions
    """
    subsections = []

    # Split by primary subsections (a), (b), etc.
    parts = re.split(r"(?=\([a-z]\)\s)", text)

    for part in parts[1:]:  # Skip content before first (a)
        match = re.match(r"\(([a-z])\)\s*", part)
        if not match:
            continue

        identifier = match.group(1)
        content = part[match.end() :]

        # Check for heading pattern: (a) Heading.--
        heading = None
        heading_match = re.match(r"([A-Z][^.]+?)\.\s*[-—]+\s*", content)
        if heading_match:
            heading = heading_match.group(1).strip()
            content = content[heading_match.end() :]

        # Parse second-level children (1), (2), etc.
        children = parse_level2(content)

        # Get text before first child
        if children:
            first_child_match = re.search(r"\(\d+\)", content)
            direct_text = (
                content[: first_child_match.start()].strip()
                if first_child_match
                else content.strip()
            )
        else:
            direct_text = content.strip()

        # Clean up text - remove trailing subsections
        next_subsection = re.search(r"\([a-z]\)", direct_text)
        if next_subsection:
            direct_text = direct_text[: next_subsection.start()].strip()

        subsections.append(
            {
                "identifier": identifier,
                "heading": heading,
                "text": direct_text[:5000],
                "children": children,
            }
        )

    return subsections


def parse_level2(text: str) -> list[dict]:
    """Parse level 2 subsections (1), (2), etc."""
    subsections = []
    parts = re.split(r"(?=\(\d+\)\s)", text)

    for part in parts[1:]:
        match = re.match(r"\((\d+)\)\s*", part)
        if not match:
            continue

        identifier = match.group(1)
        content = part[match.end() :]

        # Parse level 3 children (i), (ii), etc.
        children = parse_level3(content)

        # Limit to reasonable size and stop at next letter subsection
        next_letter = re.search(r"\([a-z]\)", content)
        if next_letter:
            content = content[: next_letter.start()]

        # Get text before first child
        if children:
            first_child_match = re.search(r"\([ivxlc]+\)", content, re.IGNORECASE)
            direct_text = (
                content[: first_child_match.start()].strip()
                if first_child_match
                else content.strip()
            )
        else:
            direct_text = content.strip()

        subsections.append(
            {
                "identifier": identifier,
                "heading": None,
                "text": direct_text[:5000],
                "children": children,
            }
        )

    return subsections


def parse_level3(text: str) -> list[dict]:
    """Parse level 3 subsections (i), (ii), etc."""
    subsections = []
    # Match roman numerals: i, ii, iii, iv, v, vi, vii, viii, ix, x
    parts = re.split(r"(?=\((?:i{1,3}|iv|vi{0,3}|ix|x)\)\s)", text, flags=re.IGNORECASE)

    for part in parts[1:]:
        match = re.match(r"\((i{1,3}|iv|vi{0,3}|ix|x)\)\s*", part, re.IGNORECASE)
        if not match:
            continue

        identifier = match.group(1).lower()
        content = part[match.end() :]

        # Limit size and stop at next subsection
        next_num = re.search(r"\(\d+\)", content)
        if next_num:
            content = content[: next_num.start()]

        next_letter = re.search(r"\([a-z]\)", content)
        if next_letter:
            content = content[: next_letter.start()]

        subsections.append(
            {
                "identifier": identifier,
                "heading": None,
                "text": content.strip()[:5000],
                "children": [],
            }
        )

    return subsections


def create_akn_xml(title_num: int, title_name: str, sections: list[dict]) -> str:
    """Create Akoma Ntoso XML from parsed sections.

    Args:
        title_num: Title number (e.g., 72)
        title_name: Title name (e.g., "Taxation and Fiscal Affairs")
        sections: List of parsed section dicts

    Returns:
        XML string
    """
    # Register namespace
    ET.register_namespace("akn", AKN_NS)
    ET.register_namespace("", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "PaCS")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#arch")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-pa/act/pacs/title-{title_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-pa/act/pacs/title-{title_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#pennsylvania-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-pa")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", str(title_num))
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "PaCS")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/akn/us-pa/act/pacs/title-{title_num}/eng@{date.today().isoformat()}")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-pa/act/pacs/title-{title_num}/eng@{date.today().isoformat()}")
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
        "value", f"/akn/us-pa/act/pacs/title-{title_num}/eng@{date.today().isoformat()}/main.xml"
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value", f"/akn/us-pa/act/pacs/title-{title_num}/eng@{date.today().isoformat()}/main.xml"
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

    pa_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    pa_leg.set("eId", "pennsylvania-legislature")
    pa_leg.set("href", "https://www.palegis.us")
    pa_leg.set("showAs", "Pennsylvania General Assembly")

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

    # Group sections by chapter
    chapters = {}
    for sec in sections:
        ch_num = sec.get("chapter_number") or "0"
        if ch_num not in chapters:
            chapters[ch_num] = {
                "title": sec.get("chapter_title") or f"Chapter {ch_num}",
                "sections": [],
            }
        chapters[ch_num]["sections"].append(sec)

    # Add chapters and sections
    for ch_num in sorted(chapters.keys(), key=lambda x: int(x) if x.isdigit() else 0):
        ch_data = chapters[ch_num]

        if ch_num != "0":
            # Create chapter element
            chapter = ET.SubElement(title_elem, f"{{{AKN_NS}}}chapter")
            chapter.set("eId", f"title_{title_num}__chp_{ch_num}")

            ch_heading = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
            ch_heading.text = ch_num

            ch_title = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
            ch_title.text = ch_data["title"]

            parent = chapter
        else:
            parent = title_elem

        for sec in ch_data["sections"]:
            add_section_to_xml(parent, sec, title_num, ch_num)

    # Convert to string with pretty print
    xml_str = ET.tostring(root, encoding="unicode")

    # Pretty print using minidom
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
    sec_num = section["section_number"]
    sec_id = f"title_{title_num}__sec_{sec_num}"

    sec_elem = ET.SubElement(parent, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", sec_id)

    # Section number
    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = f"{sec_num}"

    # Section heading
    heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    heading.text = section["title"]

    # Content
    if section["subsections"]:
        # Add subsections
        for sub in section["subsections"]:
            add_subsection_to_xml(sec_elem, sub, sec_id, level=1)
    elif section["text"]:
        # Plain content
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = section["text"][:10000] if section["text"] else ""


def add_subsection_to_xml(parent: ET.Element, subsection: dict, parent_id: str, level: int) -> None:
    """Add a subsection element to the parent XML element."""
    identifier = subsection["identifier"]
    sub_id = f"{parent_id}__subsec_{identifier}"

    if level == 1:
        # Top-level subsection (a), (b), etc.
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        # Second-level (1), (2)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        # Third-level (i), (ii)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    # Number/identifier
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"({identifier})"

    # Heading if present
    if subsection.get("heading"):
        heading = ET.SubElement(elem, f"{{{AKN_NS}}}heading")
        heading.text = subsection["heading"]

    # Text content
    if subsection["text"]:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection["text"][:10000]

    # Children
    for child in subsection.get("children", []):
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def convert_title(title_num: int, output_dir: Path) -> dict:
    """Convert a single title to Akoma Ntoso XML.

    Returns:
        Dict with stats: {title, sections, success, error}
    """
    title_name = PA_TITLES.get(title_num, f"Title {title_num}")

    try:
        print(f"  Fetching Title {title_num}: {title_name}...")
        html = fetch_title_html(title_num)

        print(f"  Parsing sections...")
        sections = extract_sections_from_html(html, title_num)

        if not sections:
            return {"title": title_num, "sections": 0, "success": True, "error": None}

        print(f"  Creating AKN XML for {len(sections)} sections...")
        xml_content = create_akn_xml(title_num, title_name, sections)

        # Write output
        output_path = output_dir / f"pacs-title-{str(title_num).zfill(2)}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {"title": title_num, "sections": len(sections), "success": True, "error": None}

    except Exception as e:
        return {"title": title_num, "sections": 0, "success": False, "error": str(e)}


def main():
    """Convert Pennsylvania statutes to Akoma Ntoso XML."""
    parser = argparse.ArgumentParser(description="Convert PA statutes to Akoma Ntoso XML")
    parser.add_argument("--title", type=int, help="Convert specific title number")
    parser.add_argument("--tax", action="store_true", help="Convert tax titles only (72)")
    args = parser.parse_args()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Determine which titles to convert
    if args.title:
        titles = [args.title]
    elif args.tax:
        titles = TAX_TITLES
    else:
        titles = list(PA_TITLES.keys())

    print(f"Pennsylvania Consolidated Statutes to Akoma Ntoso Converter")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Titles to convert: {len(titles)}")
    print()

    total_sections = 0
    successful_titles = 0
    failed_titles = 0
    empty_titles = 0

    for title_num in titles:
        print(f"Converting Title {title_num}...")
        result = convert_title(title_num, OUTPUT_DIR)

        if result["success"]:
            if result["sections"] > 0:
                successful_titles += 1
                total_sections += result["sections"]
                print(f"  [OK] Title {result['title']}: {result['sections']} sections")
            else:
                empty_titles += 1
                print(f"  [--] Title {result['title']}: no sections found")
        else:
            failed_titles += 1
            print(f"  [FAIL] Title {result['title']}: {result['error']}")

        print()

    print("=" * 60)
    print("Summary:")
    print(f"  Total titles requested:  {len(titles)}")
    print(f"  Successful titles:       {successful_titles}")
    print(f"  Empty titles:            {empty_titles}")
    print(f"  Failed titles:           {failed_titles}")
    print(f"  Total sections:          {total_sections}")
    print(f"  Output directory:        {OUTPUT_DIR}")

    # List output files
    output_files = list(OUTPUT_DIR.glob("*.xml"))
    print(f"  Output XML files:        {len(output_files)}")


if __name__ == "__main__":
    main()
