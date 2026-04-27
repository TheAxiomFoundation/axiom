#!/usr/bin/env python3
"""Convert Wisconsin Statutes to Akoma Ntoso XML.

This script fetches Wisconsin Statutes from docs.legis.wisconsin.gov
using the WIConverter and converts them to Akoma Ntoso 3.0 XML format.

Output is saved to /tmp/rules-us-wi-akn/ with one file per chapter.

Usage:
    python scripts/wi_to_akn.py

Options:
    --chapters: Comma-separated chapter numbers (default: tax+welfare chapters)
    --all-tax: Fetch all tax chapters (71-79)
    --all-welfare: Fetch all welfare chapters (46-52)
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.wi import (
    WIConverter,
    WI_TAX_CHAPTERS,
    WI_WELFARE_CHAPTERS,
)
from atlas.models import Section

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-wi-akn")


def create_akn_xml(chapter_num: int, chapter_title: str, sections: list[Section]) -> str:
    """Create Akoma Ntoso XML from parsed sections.

    Args:
        chapter_num: e.g., 71
        chapter_title: e.g., "Income and Franchise Taxes..."
        sections: List of Section objects

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
    act.set("name", "WI-Statutes")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#arch")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-wi/act/statutes/chapter-{chapter_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-wi/act/statutes/chapter-{chapter_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#wisconsin-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-wi")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", str(chapter_num))
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "WI-Statutes")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-wi/act/statutes/chapter-{chapter_num}/eng@{date.today().isoformat()}"
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value", f"/akn/us-wi/act/statutes/chapter-{chapter_num}/eng@{date.today().isoformat()}"
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
        f"/akn/us-wi/act/statutes/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value",
        f"/akn/us-wi/act/statutes/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml",
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

    wi_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    wi_leg.set("eId", "wisconsin-legislature")
    wi_leg.set("href", "https://docs.legis.wisconsin.gov")
    wi_leg.set("showAs", "Wisconsin State Legislature")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Chapter container
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{chapter_num}")

    # Chapter number
    num = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    num.text = str(chapter_num)

    # Chapter heading
    heading = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    heading.text = chapter_title or f"Chapter {chapter_num}"

    # Add sections
    for sec in sections:
        add_section_to_xml(chapter, sec)

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


def add_section_to_xml(parent: ET.Element, section: Section) -> None:
    """Add a section element to the parent XML element."""
    # Extract section number from citation (e.g., "WI-71.01" -> "71-01")
    sec_num = section.citation.section.replace("WI-", "").replace(".", "-")

    sec_elem = ET.SubElement(parent, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", f"sec_{sec_num}")

    # Section number
    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = section.citation.section.replace("WI-", "")

    # Section heading
    heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    heading.text = section.section_title or f"Section {section.citation.section}"

    # Content with subsections
    if section.subsections:
        # Add subsections
        for sub in section.subsections:
            add_subsection_to_xml(sec_elem, sub, sec_num, level=1)
    elif section.text:
        # Plain content
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        # Limit text to reasonable size
        p.text = section.text[:10000] if section.text else ""


def add_subsection_to_xml(parent: ET.Element, subsection, parent_id: str, level: int) -> None:
    """Add a subsection element to the parent XML element."""
    identifier = subsection.identifier
    sub_id = f"{parent_id}__subsec_{identifier}"

    if level == 1:
        # Top-level subsection (1), (2), etc.
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        # Second-level (a), (b)
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        # Third-level
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    # Number/identifier
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"({identifier})"

    # Text content
    if subsection.text:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection.text[:10000]

    # Children
    for child in subsection.children:
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def convert_chapter(converter: WIConverter, chapter: int, chapter_title: str) -> dict:
    """Convert a single chapter to Akoma Ntoso XML.

    Returns:
        Dict with stats: {chapter, sections, success, error}
    """
    try:
        sections = list(converter.iter_chapter(chapter))

        if not sections:
            return {"chapter": chapter, "sections": 0, "success": True, "error": None}

        # Create AKN XML
        xml_content = create_akn_xml(chapter, chapter_title, sections)

        # Write output
        output_path = OUTPUT_DIR / f"wi-chapter-{str(chapter).zfill(3)}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {"chapter": chapter, "sections": len(sections), "success": True, "error": None}

    except Exception as e:
        return {"chapter": chapter, "sections": 0, "success": False, "error": str(e)}


def main():
    """Convert Wisconsin Statutes to Akoma Ntoso XML."""
    parser = argparse.ArgumentParser(description="Convert Wisconsin Statutes to Akoma Ntoso XML")
    parser.add_argument("--chapters", type=str, help="Comma-separated chapter numbers")
    parser.add_argument("--all-tax", action="store_true", help="Fetch all tax chapters (71-79)")
    parser.add_argument(
        "--all-welfare", action="store_true", help="Fetch all welfare chapters (46-52)"
    )
    parser.add_argument(
        "--rate-limit", type=float, default=0.5, help="Delay between requests in seconds"
    )
    args = parser.parse_args()

    # Determine which chapters to fetch
    chapters_to_fetch = {}

    if args.chapters:
        for ch in args.chapters.split(","):
            ch_num = int(ch.strip())
            title = (
                WI_TAX_CHAPTERS.get(ch_num)
                or WI_WELFARE_CHAPTERS.get(ch_num)
                or f"Chapter {ch_num}"
            )
            chapters_to_fetch[ch_num] = title
    elif args.all_tax:
        chapters_to_fetch = dict(WI_TAX_CHAPTERS)
    elif args.all_welfare:
        chapters_to_fetch = dict(WI_WELFARE_CHAPTERS)
    else:
        # Default: all tax and welfare chapters
        chapters_to_fetch = {**WI_TAX_CHAPTERS, **WI_WELFARE_CHAPTERS}

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {len(chapters_to_fetch)} Wisconsin Statute chapters")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Rate limit: {args.rate_limit}s between requests")
    print()

    total_sections = 0
    successful_chapters = 0
    failed_chapters = 0
    empty_chapters = 0

    with WIConverter(rate_limit_delay=args.rate_limit) as converter:
        for chapter, title in sorted(chapters_to_fetch.items()):
            print(f"Processing Chapter {chapter}: {title}...")
            result = convert_chapter(converter, chapter, title)

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
    print(f"  Total chapters requested: {len(chapters_to_fetch)}")
    print(f"  Successful chapters:      {successful_chapters}")
    print(f"  Empty chapters:           {empty_chapters}")
    print(f"  Failed chapters:          {failed_chapters}")
    print(f"  Total sections:           {total_sections}")
    print(f"  Output directory:         {OUTPUT_DIR}")

    # List output files
    output_files = list(OUTPUT_DIR.glob("*.xml"))
    print(f"  Output XML files:         {len(output_files)}")


if __name__ == "__main__":
    main()
