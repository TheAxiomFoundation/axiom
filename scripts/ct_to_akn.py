#!/usr/bin/env python3
"""Convert Connecticut General Statutes to Akoma Ntoso XML.

This script fetches Connecticut statutes from cga.ct.gov and converts them
to Akoma Ntoso XML format in /tmp/rules-us-ct-akn/.

Usage:
    python scripts/ct_to_akn.py                     # Convert all tax/welfare chapters
    python scripts/ct_to_akn.py --chapters 203 211  # Convert specific chapters
    python scripts/ct_to_akn.py --title 12          # Convert all Title 12 chapters
"""

import argparse
import re
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

from axiom.converters.us_states.ct import (
    CTConverter,
    CT_TAX_CHAPTERS,
    CT_WELFARE_CHAPTERS,
    CT_TITLES,
)
from axiom.models import Section


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-ct-akn")


def section_to_akn_xml(section: Section, chapter_num: str, chapter_title: str) -> str:
    """Convert an Axiom section model to Akoma Ntoso XML.

    Args:
        section: The Axiom section model
        chapter_num: Connecticut chapter number
        chapter_title: Connecticut chapter title

    Returns:
        Pretty-printed XML string
    """
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Extract section number from citation (e.g., "CT-12-41" -> "12-41")
    section_num = section.citation.section.replace("CT-", "")

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#ct-legislature")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-ct/act/cgs/sec-{section_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-ct/act/cgs/sec-{section_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", str(date.today()))
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#ct-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ct")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", section_num)
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "CGS")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/akn/us-ct/act/cgs/sec-{section_num}/eng@{date.today().isoformat()}")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-ct/act/cgs/sec-{section_num}/eng@{date.today().isoformat()}")
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", str(date.today()))
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#axiom")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set(
        "value", f"/akn/us-ct/act/cgs/sec-{section_num}/eng@{date.today().isoformat()}/main.xml"
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value", f"/akn/us-ct/act/cgs/sec-{section_num}/eng@{date.today().isoformat()}/main.xml"
    )
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", str(date.today()))
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom")

    org_axiom = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_axiom.set("eId", "axiom")
    org_axiom.set("href", "https://axiom-foundation.org")
    org_axiom.set("showAs", "Axiom")

    org_ct = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_ct.set("eId", "ct-legislature")
    org_ct.set("href", "https://www.cga.ct.gov")
    org_ct.set("showAs", "Connecticut General Assembly")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Chapter container
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{chapter_num}")

    # Chapter number and heading
    chp_num = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    chp_num.text = chapter_num
    chp_heading = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    chp_heading.text = chapter_title

    # Section element
    sec_elem = ET.SubElement(chapter, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", f"sec_{section_num.replace('-', '_')}")

    # Section number
    sec_num_elem = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    sec_num_elem.text = section_num

    # Section heading
    if section.section_title:
        sec_heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
        sec_heading.text = section.section_title

    # Add subsections or content
    if section.subsections:
        for subsec in section.subsections:
            add_subsection_to_xml(sec_elem, subsec, section_num)
    elif section.text:
        # Plain content
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        # Split text into paragraphs
        for para in section.text.split("\n\n"):
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para.strip()[:10000]  # Limit size

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    try:
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ", encoding="UTF-8")
        # Remove extra blank lines
        lines = pretty_xml.decode("utf-8").split("\n")
        cleaned = [line for line in lines if line.strip()]
        return "\n".join(cleaned)
    except Exception:
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str


def add_subsection_to_xml(parent: ET.Element, subsection, parent_id: str, level: int = 1) -> None:
    """Add a subsection element to the parent XML element."""
    identifier = subsection.identifier
    sub_id = f"{parent_id.replace('-', '_')}__subsec_{identifier}"

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

    # Heading if present
    if subsection.heading:
        heading = ET.SubElement(elem, f"{{{AKN_NS}}}heading")
        heading.text = subsection.heading

    # Text content
    if subsection.text:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection.text[:10000]

    # Children
    for child in subsection.children:
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def convert_chapter(converter: CTConverter, chapter: str, output_dir: Path) -> tuple[int, int]:
    """Convert a single chapter to AKN format.

    Returns:
        Tuple of (sections_found, sections_converted)
    """
    chapter_title = CT_TAX_CHAPTERS.get(chapter) or CT_WELFARE_CHAPTERS.get(
        chapter, f"Chapter {chapter}"
    )

    print(f"  Fetching chapter {chapter}: {chapter_title}...", end=" ", flush=True)

    try:
        sections = converter.fetch_chapter(chapter)
    except Exception as e:
        print(f"ERROR: {e}")
        return 0, 0

    if not sections:
        print("no sections found")
        return 0, 0

    print(f"{len(sections)} sections")

    # Create chapter directory
    chapter_dir = output_dir / f"chapter-{chapter}"
    chapter_dir.mkdir(exist_ok=True)

    converted = 0
    for section_num, section in sections.items():
        try:
            xml_content = section_to_akn_xml(section, chapter, chapter_title)

            # Create safe filename
            safe_num = section_num.replace("/", "-").replace(".", "-")
            section_path = chapter_dir / f"sec-{safe_num}.xml"

            with open(section_path, "w", encoding="utf-8") as f:
                f.write(xml_content)

            converted += 1
        except Exception as e:
            print(f"    WARNING: Could not convert section {section_num}: {e}")

    return len(sections), converted


def main():
    parser = argparse.ArgumentParser(
        description="Convert Connecticut General Statutes to Akoma Ntoso XML"
    )
    parser.add_argument(
        "--chapters",
        nargs="+",
        help="Specific chapter numbers to convert (e.g., 203 211)",
    )
    parser.add_argument(
        "--title",
        choices=["12", "17b"],
        help="Convert all chapters for a title (12=Taxation, 17b=Social Services)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Convert all tax and welfare chapters",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )

    args = parser.parse_args()

    # Determine which chapters to convert
    if args.chapters:
        chapters = args.chapters
    elif args.title == "12":
        chapters = list(CT_TAX_CHAPTERS.keys())
    elif args.title == "17b":
        chapters = list(CT_WELFARE_CHAPTERS.keys())
    elif args.all:
        chapters = list(CT_TAX_CHAPTERS.keys()) + list(CT_WELFARE_CHAPTERS.keys())
    else:
        # Default: all tax and welfare chapters
        chapters = list(CT_TAX_CHAPTERS.keys()) + list(CT_WELFARE_CHAPTERS.keys())

    output_dir = args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Converting Connecticut General Statutes to Akoma Ntoso")
    print(f"Output directory: {output_dir}")
    print(f"Chapters to convert: {len(chapters)}")
    print()

    total_found = 0
    total_converted = 0
    successful_chapters = 0
    failed_chapters = 0

    with CTConverter(rate_limit_delay=0.5) as converter:
        for chapter in chapters:
            found, converted = convert_chapter(converter, chapter, output_dir)

            if found > 0:
                successful_chapters += 1
                total_found += found
                total_converted += converted
            else:
                failed_chapters += 1

    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Chapters processed:     {len(chapters)}")
    print(f"  Successful chapters:    {successful_chapters}")
    print(f"  Failed chapters:        {failed_chapters}")
    print(f"  Total sections found:   {total_found}")
    print(f"  Total sections converted: {total_converted}")
    print(f"  Output directory:       {output_dir}")

    # List output files
    xml_files = list(output_dir.rglob("*.xml"))
    print(f"  Output XML files:       {len(xml_files)}")


if __name__ == "__main__":
    main()
