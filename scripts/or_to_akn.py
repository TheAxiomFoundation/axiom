#!/usr/bin/env python3
"""Convert Oregon Revised Statutes to Akoma Ntoso XML.

This script uses the ORConverter to fetch ORS chapters and converts them
to Akoma Ntoso XML format in /tmp/rules-us-or-akn/.

Oregon Statute Structure:
- Chapters (e.g., Chapter 316: Personal Income Tax)
- Sections (e.g., 316.037: Imposition and rate of tax)
- Subsections using (1), (a), (A), (i) notation

Usage:
    python scripts/or_to_akn.py
"""

import re
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from axiom.converters.us_states.or_ import (
    ORConverter,
    OR_TAX_CHAPTERS,
    OR_WELFARE_CHAPTERS,
)
from axiom.models import Section


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output path
OUTPUT_DIR = Path("/tmp/rules-us-or-akn")


def section_to_akn_dict(section: Section) -> dict:
    """Convert Axiom section model to dict for AKN conversion."""
    return {
        "section_number": section.citation.section.replace("OR-", ""),
        "title": section.section_title,
        "text": section.text,
        "history": None,  # History is embedded in text for OR
        "subsections": [subsection_to_dict(sub) for sub in section.subsections],
    }


def subsection_to_dict(sub) -> dict:
    """Convert Axiom subsection to dict."""
    return {
        "identifier": sub.identifier,
        "text": sub.text,
        "children": [subsection_to_dict(c) for c in sub.children],
    }


def create_akn_xml(chapter_num: str, chapter_title: str, sections: list[dict]) -> str:
    """Create Akoma Ntoso XML from parsed sections.

    Args:
        chapter_num: e.g., "316"
        chapter_title: e.g., "Personal Income Tax"
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
    act.set("name", "ORS")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-or/act/ors/chapter-{chapter_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-or/act/ors/chapter-{chapter_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#oregon-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-or")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", chapter_num)
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "ORS")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-or/act/ors/chapter-{chapter_num}/eng@{date.today().isoformat()}"
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value", f"/akn/us-or/act/ors/chapter-{chapter_num}/eng@{date.today().isoformat()}"
    )
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", date.today().isoformat())
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#axiom")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set(
        "value", f"/akn/us-or/act/ors/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml"
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value", f"/akn/us-or/act/ors/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml"
    )
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", date.today().isoformat())
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom")

    # References
    refs = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    refs.set("source", "#axiom")

    # TLC references
    axiom_ref = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    axiom_ref.set("eId", "axiom")
    axiom_ref.set("href", "https://axiom-foundation.org")
    axiom_ref.set("showAs", "Axiom")

    or_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    or_leg.set("eId", "oregon-legislature")
    or_leg.set("href", "https://www.oregonlegislature.gov")
    or_leg.set("showAs", "Oregon State Legislature")

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
        # Top-level subsection (1), (2), etc.
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        # Second-level (a), (b)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    elif level == 3:
        # Third-level (A), (B)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")
    else:
        # Fourth-level (i), (ii)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}clause")

    elem.set("eId", sub_id)

    # Number/identifier
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"({identifier})"

    # Text content
    if subsection["text"]:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection["text"][:10000]

    # Children
    for child in subsection.get("children", []):
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def convert_chapter(
    converter: ORConverter, chapter_num: int, chapter_title: str, output_dir: Path
) -> dict:
    """Convert a single chapter to Akoma Ntoso XML.

    Returns:
        Dict with stats: {chapter, sections, success, error}
    """
    try:
        # Fetch sections from Oregon legislature website
        print(f"    Fetching chapter {chapter_num}...")
        sections = converter.fetch_chapter(chapter_num)

        if not sections:
            return {"chapter": str(chapter_num), "sections": 0, "success": True, "error": None}

        # Convert to AKN format
        section_dicts = [section_to_akn_dict(s) for s in sections]

        # Create AKN XML
        xml_content = create_akn_xml(str(chapter_num), chapter_title, section_dicts)

        # Write output
        output_path = output_dir / f"ors-chapter-{str(chapter_num).zfill(3)}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {
            "chapter": str(chapter_num),
            "sections": len(sections),
            "success": True,
            "error": None,
        }

    except Exception as e:
        return {"chapter": str(chapter_num), "sections": 0, "success": False, "error": str(e)}


def main():
    """Convert Oregon tax and welfare chapters to Akoma Ntoso XML."""
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Combine all chapters
    all_chapters = {**OR_TAX_CHAPTERS, **OR_WELFARE_CHAPTERS}

    print(f"Converting {len(all_chapters)} Oregon Revised Statutes chapters")
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    total_sections = 0
    successful_chapters = 0
    failed_chapters = 0
    empty_chapters = 0

    with ORConverter(rate_limit_delay=0.5) as converter:
        for chapter_str, chapter_title in all_chapters.items():
            # Skip chapters with letter suffixes (308A, 317A) for now
            if not chapter_str.isdigit():
                print(
                    f"  [SKIP] Chapter {chapter_str}: non-numeric chapter numbers not yet supported"
                )
                continue

            chapter_num = int(chapter_str)
            print(f"  Processing Chapter {chapter_num}: {chapter_title}")

            result = convert_chapter(converter, chapter_num, chapter_title, OUTPUT_DIR)

            if result["success"]:
                if result["sections"] > 0:
                    successful_chapters += 1
                    total_sections += result["sections"]
                    print(f"    [OK] {result['sections']} sections")
                else:
                    empty_chapters += 1
                    print(f"    [--] No sections found")
            else:
                failed_chapters += 1
                print(f"    [FAIL] {result['error']}")

    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Total chapters:        {len(all_chapters)}")
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
