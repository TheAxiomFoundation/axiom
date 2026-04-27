#!/usr/bin/env python3
"""Convert Nevada Revised Statutes HTML files to Akoma Ntoso XML.

This script reads NRS HTML files from data/statutes/us-nv/ and converts them
to Akoma Ntoso XML format in /tmp/rules-us-nv-akn/.

HTML Structure:
- Section anchors: <a name=NRS361Sec010>
- Section number: <span class="Section">361.015</span>
- Section title: <span class="Leadline">...</span>
- Body text: <p class="SectBody">...</p>
- History: <p class="SourceNote">...</p>
- Chapter title: <p class="Chapter">CHAPTER 32 - RECEIVERS</p>

Usage:
    python scripts/nv_to_akn.py
"""

import re
import os
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

from bs4 import BeautifulSoup


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Input and output paths
INPUT_DIR = Path(__file__).parent.parent / "data" / "statutes" / "us-nv"
OUTPUT_DIR = Path("/tmp/rules-us-nv-akn")


def extract_chapter_number(filename: str) -> str:
    """Extract chapter number from filename like NRS_NRS-032.html.html."""
    match = re.search(r"NRS-(\d+[A-Za-z]?)\.html", filename)
    if match:
        return match.group(1).lstrip("0") or "0"
    return ""


def extract_chapter_info(soup: BeautifulSoup) -> tuple[str, str]:
    """Extract chapter number and title from HTML.

    Returns:
        Tuple of (chapter_number, chapter_title)
    """
    # Try to find <p class="Chapter">CHAPTER 32 - RECEIVERS</p>
    chapter_elem = soup.find("p", class_="Chapter")
    if chapter_elem:
        text = chapter_elem.get_text(strip=True)
        # Parse "CHAPTER 32 - RECEIVERS" or "CHAPTER 32A - RECEIVERS"
        match = re.match(r"CHAPTER\s+(\d+[A-Za-z]?)\s*[-–]\s*(.+)", text, re.IGNORECASE)
        if match:
            return match.group(1), match.group(2).strip()
    return "", ""


def parse_sections(soup: BeautifulSoup, chapter_num: str) -> list[dict]:
    """Parse all sections from a chapter HTML.

    Returns:
        List of section dicts with keys:
        - section_number (e.g., "32.010")
        - title (e.g., "Cases in which receiver may be appointed")
        - text (full text content)
        - history (source note if present)
        - subsections (list of subsection dicts)
    """
    sections = []

    # Find all section anchors: <a name=NRS032Sec010>
    anchor_pattern = re.compile(rf"NRS{re.escape(chapter_num.zfill(3))}Sec(\d+[A-Za-z]?)")

    for anchor in soup.find_all("a", attrs={"name": anchor_pattern}):
        name = anchor.get("name", "")
        match = anchor_pattern.match(name)
        if not match:
            continue

        section_suffix = match.group(1)
        section_number = f"{chapter_num}.{section_suffix}"

        # Navigate to parent paragraph
        section_start = anchor.find_parent("p")
        if not section_start:
            continue

        # Extract section title from Leadline span
        leadline = section_start.find("span", class_="Leadline")
        section_title = leadline.get_text(strip=True) if leadline else ""
        section_title = section_title.strip("'\"")

        # Collect content paragraphs until next section
        content_parts = []
        history = None

        # First paragraph content
        first_text = section_start.get_text(strip=True)
        # Remove section header and leadline
        if leadline and section_title:
            idx = first_text.find(section_title)
            if idx >= 0:
                content_after = first_text[idx + len(section_title) :].strip()
                if content_after:
                    content_parts.append(content_after)

        # Iterate through siblings
        current = section_start.find_next_sibling()
        while current:
            if current.name == "p":
                # Check for next section anchor
                has_section_anchor = current.find("a", attrs={"name": anchor_pattern})
                if has_section_anchor:
                    break

                # Check for source note
                if "SourceNote" in (current.get("class") or []):
                    history = current.get_text(strip=True)
                    break

                # Regular content paragraph
                text = current.get_text(strip=True)
                if text:
                    content_parts.append(text)

            current = current.find_next_sibling() if current else None

        full_text = "\n".join(content_parts)

        # Parse subsections
        subsections = parse_subsections(full_text)

        sections.append(
            {
                "section_number": section_number,
                "title": section_title or f"Section {section_number}",
                "text": full_text,
                "history": history,
                "subsections": subsections,
            }
        )

    return sections


def parse_subsections(text: str) -> list[dict]:
    """Parse hierarchical subsections from text.

    Nevada uses:
    - 1., 2., 3. for primary divisions
    - (a), (b), (c) for secondary
    - (1), (2), (3) for tertiary
    """
    subsections = []

    # Split by top-level numbered subsections
    parts = re.split(r"(?=\s+\d+\.\s{2,})", text)

    for part in parts:
        if not part.strip():
            continue

        match = re.match(r"\s*(\d+)\.\s{2,}(.*)$", part, re.DOTALL)
        if not match:
            continue

        identifier = match.group(1)
        content = match.group(2).strip()

        # Parse second-level (a), (b)
        children = parse_subsection_level2(content)

        # Get direct text before children
        if children:
            first_child_match = re.search(r"\([a-z]\)", content)
            direct_text = (
                content[: first_child_match.start()].strip() if first_child_match else content
            )
        else:
            direct_text = content

        # Clean up - remove next numbered subsection
        next_sub = re.search(r"\s+\d+\.\s{2,}", direct_text)
        if next_sub:
            direct_text = direct_text[: next_sub.start()]

        subsections.append(
            {
                "identifier": identifier,
                "text": direct_text[:5000],
                "children": children,
            }
        )

    return subsections


def parse_subsection_level2(text: str) -> list[dict]:
    """Parse level 2 subsections (a), (b), etc."""
    subsections = []
    parts = re.split(r"(?=\([a-z]\)\s*)", text)

    for part in parts:
        if not part.strip():
            continue

        match = re.match(r"\(([a-z])\)\s*(.*)$", part, re.DOTALL)
        if not match:
            continue

        identifier = match.group(1)
        content = match.group(2).strip()

        # Limit to reasonable size
        next_num = re.search(r"\s+\d+\.\s{2,}", content)
        if next_num:
            content = content[: next_num.start()]

        # Parse level 3
        children = parse_subsection_level3(content)

        if children:
            first_match = re.search(r"\(\d+\)", content)
            direct_text = content[: first_match.start()].strip() if first_match else content
        else:
            direct_text = content

        subsections.append(
            {
                "identifier": identifier,
                "text": direct_text[:5000],
                "children": children,
            }
        )

    return subsections


def parse_subsection_level3(text: str) -> list[dict]:
    """Parse level 3 subsections (1), (2), etc."""
    subsections = []
    parts = re.split(r"(?=\(\d+\)\s*)", text)

    for part in parts:
        if not part.strip():
            continue

        match = re.match(r"\((\d+)\)\s*(.*)$", part, re.DOTALL)
        if not match:
            continue

        identifier = match.group(1)
        content = match.group(2).strip()

        # Stop at next alphabetic
        next_alpha = re.search(r"\([a-z]\)", content)
        if next_alpha:
            content = content[: next_alpha.start()]

        subsections.append(
            {
                "identifier": identifier,
                "text": content[:5000],
                "children": [],
            }
        )

    return subsections


def create_akn_xml(chapter_num: str, chapter_title: str, sections: list[dict]) -> str:
    """Create Akoma Ntoso XML from parsed sections.

    Args:
        chapter_num: e.g., "32"
        chapter_title: e.g., "RECEIVERS"
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
    act.set("name", "NRS")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#arch")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-nv/act/nrs/chapter-{chapter_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-nv/act/nrs/chapter-{chapter_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#nevada-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-nv")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", chapter_num)
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "NRS")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-nv/act/nrs/chapter-{chapter_num}/eng@{date.today().isoformat()}"
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value", f"/akn/us-nv/act/nrs/chapter-{chapter_num}/eng@{date.today().isoformat()}"
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
        "value", f"/akn/us-nv/act/nrs/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml"
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value", f"/akn/us-nv/act/nrs/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml"
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

    nv_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    nv_leg.set("eId", "nevada-legislature")
    nv_leg.set("href", "https://www.leg.state.nv.us")
    nv_leg.set("showAs", "Nevada State Legislature")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Chapter container
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{chapter_num}")

    # Chapter number
    num = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    num.text = chapter_num

    # Chapter heading
    heading = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    heading.text = chapter_title or f"Chapter {chapter_num}"

    # Add sections
    for sec in sections:
        add_section_to_xml(chapter, sec, chapter_num)

    # Convert to string with pretty print
    xml_str = ET.tostring(root, encoding="unicode")

    # Pretty print using minidom
    try:
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ", encoding="UTF-8")
        # Remove extra blank lines and fix declaration
        lines = pretty_xml.decode("utf-8").split("\n")
        cleaned = [line for line in lines if line.strip()]
        return "\n".join(cleaned)
    except Exception:
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str


def add_section_to_xml(parent: ET.Element, section: dict, chapter_num: str) -> None:
    """Add a section element to the parent XML element."""
    sec_num = section["section_number"].replace(".", "-")

    sec_elem = ET.SubElement(parent, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", f"sec_{sec_num}")

    # Section number
    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = section["section_number"]

    # Section heading
    heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    heading.text = section["title"]

    # Content
    if section["subsections"]:
        # Add subsections
        for sub in section["subsections"]:
            add_subsection_to_xml(sec_elem, sub, sec_num, level=1)
    elif section["text"]:
        # Plain content
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = section["text"][:10000] if section["text"] else ""

    # Add history/source note as authorialNote
    if section.get("history"):
        note = ET.SubElement(sec_elem, f"{{{AKN_NS}}}authorialNote")
        note.set("marker", "history")
        p = ET.SubElement(note, f"{{{AKN_NS}}}p")
        p.text = section["history"]


def add_subsection_to_xml(parent: ET.Element, subsection: dict, parent_id: str, level: int) -> None:
    """Add a subsection element to the parent XML element."""
    identifier = subsection["identifier"]
    sub_id = f"{parent_id}__subsec_{identifier}"

    if level == 1:
        # Top-level subsection (1., 2., etc.)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        # Second-level (a), (b)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        # Third-level (1), (2)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    # Number/identifier
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    if level == 1:
        num.text = f"{identifier}."
    else:
        num.text = f"({identifier})"

    # Text content
    if subsection["text"]:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection["text"][:10000]

    # Children
    for child in subsection.get("children", []):
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def convert_file(html_path: Path, output_dir: Path) -> dict:
    """Convert a single HTML file to Akoma Ntoso XML.

    Returns:
        Dict with stats: {chapter, sections, success, error}
    """
    filename = html_path.name
    chapter_num = extract_chapter_number(filename)

    if not chapter_num:
        return {
            "chapter": filename,
            "sections": 0,
            "success": False,
            "error": "Could not extract chapter number",
        }

    try:
        with open(html_path, "r", encoding="cp1252", errors="replace") as f:
            html = f.read()

        soup = BeautifulSoup(html, "html.parser")

        # Get chapter info
        _, chapter_title = extract_chapter_info(soup)
        if not chapter_title:
            chapter_title = f"Chapter {chapter_num}"

        # Parse sections
        sections = parse_sections(soup, chapter_num)

        if not sections:
            return {"chapter": chapter_num, "sections": 0, "success": True, "error": None}

        # Create AKN XML
        xml_content = create_akn_xml(chapter_num, chapter_title, sections)

        # Write output
        output_path = output_dir / f"nrs-chapter-{chapter_num.zfill(3)}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {"chapter": chapter_num, "sections": len(sections), "success": True, "error": None}

    except Exception as e:
        return {"chapter": chapter_num, "sections": 0, "success": False, "error": str(e)}


def main():
    """Convert all Nevada HTML files to Akoma Ntoso XML."""
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Find all HTML files
    html_files = sorted(INPUT_DIR.glob("NRS_NRS-*.html.html"))

    print(f"Found {len(html_files)} HTML files in {INPUT_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    total_sections = 0
    successful_chapters = 0
    failed_chapters = 0
    empty_chapters = 0

    for html_path in html_files:
        result = convert_file(html_path, OUTPUT_DIR)

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

    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Total HTML files:      {len(html_files)}")
    print(f"  Successful chapters:   {successful_chapters}")
    print(f"  Empty chapters:        {empty_chapters}")
    print(f"  Failed chapters:       {failed_chapters}")
    print(f"  Total sections:        {total_sections}")
    print(f"  Output directory:      {OUTPUT_DIR}")

    # List output files
    output_files = list(OUTPUT_DIR.glob("*.xml"))
    print(f"  Output XML files:      {len(output_files)}")


if __name__ == "__main__":
    main()
