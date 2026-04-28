#!/usr/bin/env python3
"""Convert Minnesota Statutes HTML to Akoma Ntoso XML.

This script fetches Minnesota Statutes from revisor.mn.gov using the MNConverter
and converts them to Akoma Ntoso XML format in /tmp/rules-us-mn-akn/.

Minnesota Statute Structure:
- Chapters (e.g., Chapter 290: Individual Income Tax)
- Sections (e.g., 290.01: Definitions)
- Subdivisions (e.g., Subd. 1: Tax base)
- Clauses (e.g., (a), (b), (c) within subdivisions)

Usage:
    python scripts/mn_to_akn.py [--chapters 290,291,256J] [--all-tax] [--all-welfare]
"""

import argparse
import re
from datetime import date
from pathlib import Path
from xml.dom import minidom
from xml.etree import ElementTree as ET

from axiom_corpus.converters.us_states.mn import (
    MNConverter,
    MN_TAX_CHAPTERS,
    MN_WELFARE_CHAPTERS,
    MNConverterError,
)


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output path
OUTPUT_DIR = Path("/tmp/rules-us-mn-akn")


def create_akn_xml(chapter_num: str, chapter_title: str, sections: list[dict]) -> str:
    """Create Akoma Ntoso XML from parsed sections.

    Args:
        chapter_num: e.g., "290" or "290A"
        chapter_title: e.g., "Individual Income Tax"
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
    act.set("name", "MN-Statutes")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-mn/act/statute/chapter-{chapter_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-mn/act/statute/chapter-{chapter_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#mn-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-mn")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", chapter_num)
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "Minnesota-Statutes")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-mn/act/statute/chapter-{chapter_num}/eng@{date.today().isoformat()}"
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value", f"/akn/us-mn/act/statute/chapter-{chapter_num}/eng@{date.today().isoformat()}"
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
        "value",
        f"/akn/us-mn/act/statute/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value",
        f"/akn/us-mn/act/statute/chapter-{chapter_num}/eng@{date.today().isoformat()}/main.xml",
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

    mn_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    mn_leg.set("eId", "mn-legislature")
    mn_leg.set("href", "https://www.revisor.mn.gov")
    mn_leg.set("showAs", "Minnesota State Legislature")

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
    if section["subdivisions"]:
        # Add subdivisions
        for sub in section["subdivisions"]:
            add_subdivision_to_xml(sec_elem, sub, sec_num)
    elif section["text"]:
        # Plain content
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = section["text"][:10000] if section["text"] else ""

    # Add history as authorialNote
    if section.get("history"):
        note = ET.SubElement(sec_elem, f"{{{AKN_NS}}}authorialNote")
        note.set("marker", "history")
        p = ET.SubElement(note, f"{{{AKN_NS}}}p")
        p.text = section["history"]


def add_subdivision_to_xml(parent: ET.Element, subdivision: dict, parent_id: str) -> None:
    """Add a subdivision element to the parent XML element.

    Minnesota uses subdivisions (Subd. 1, 2, etc.) instead of numbered subsections.
    """
    identifier = subdivision["identifier"]
    sub_id = f"{parent_id}__subd_{identifier}"

    # Minnesota subdivisions map to AKN subsection
    elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elem.set("eId", sub_id)

    # Number
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"Subd. {identifier}."

    # Heading if present
    if subdivision.get("heading"):
        heading = ET.SubElement(elem, f"{{{AKN_NS}}}heading")
        heading.text = subdivision["heading"]

    # Text content
    if subdivision["text"]:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subdivision["text"][:10000]

    # Clauses (a), (b), etc.
    for clause in subdivision.get("clauses", []):
        add_clause_to_xml(elem, clause, sub_id)


def add_clause_to_xml(parent: ET.Element, clause: dict, parent_id: str) -> None:
    """Add a clause element (a), (b), etc. to the parent."""
    identifier = clause["identifier"]
    clause_id = f"{parent_id}__para_{identifier}"

    # Clauses map to AKN paragraph
    elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    elem.set("eId", clause_id)

    # Number
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"({identifier})"

    # Text content
    if clause["text"]:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = clause["text"][:10000]

    # Children (1), (2), etc.
    for child in clause.get("children", []):
        add_subclause_to_xml(elem, child, clause_id)


def add_subclause_to_xml(parent: ET.Element, subclause: dict, parent_id: str) -> None:
    """Add a subclause element (1), (2), etc. to the parent."""
    identifier = subclause["identifier"]
    sub_id = f"{parent_id}__subpara_{identifier}"

    # Subclauses map to AKN subparagraph
    elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")
    elem.set("eId", sub_id)

    # Number
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"({identifier})"

    # Text content
    if subclause["text"]:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subclause["text"][:10000]


def convert_section_to_dict(section) -> dict:
    """Convert an axiom_corpus.models.Section to a dict for XML generation."""
    subdivisions = []
    for sub in section.subsections:
        clauses = []
        for child in sub.children:
            sub_children = []
            for grandchild in child.children:
                sub_children.append(
                    {
                        "identifier": grandchild.identifier,
                        "text": grandchild.text or "",
                        "children": [],
                    }
                )
            clauses.append(
                {
                    "identifier": child.identifier,
                    "text": child.text or "",
                    "children": sub_children,
                }
            )
        subdivisions.append(
            {
                "identifier": sub.identifier,
                "heading": sub.heading,
                "text": sub.text or "",
                "clauses": clauses,
            }
        )

    return {
        "section_number": section.citation.section.replace("MN-", ""),
        "title": section.section_title,
        "text": section.text or "",
        "subdivisions": subdivisions,
        "history": None,  # Could extract from text if needed
    }


def convert_chapter(converter: MNConverter, chapter: str) -> dict:
    """Convert a chapter to AKN XML.

    Returns:
        Dict with stats: {chapter, sections, success, error, output_path}
    """
    # Get chapter title
    chapter_title = (
        MN_TAX_CHAPTERS.get(chapter) or MN_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
    )

    sections = []
    errors = []

    try:
        # Fetch all section numbers in the chapter
        section_numbers = converter.get_chapter_section_numbers(chapter)
        print(f"    Found {len(section_numbers)} section numbers")

        for sec_num in section_numbers:
            try:
                section = converter.fetch_section(sec_num)
                sections.append(convert_section_to_dict(section))
            except MNConverterError as e:
                errors.append(f"{sec_num}: {e}")
            except Exception as e:
                errors.append(f"{sec_num}: {type(e).__name__}: {e}")

        if not sections:
            return {
                "chapter": chapter,
                "sections": 0,
                "success": True,
                "error": "No sections found" if not errors else "; ".join(errors[:3]),
                "output_path": None,
            }

        # Create AKN XML
        xml_content = create_akn_xml(chapter, chapter_title, sections)

        # Write output
        output_path = OUTPUT_DIR / f"mn-chapter-{chapter}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {
            "chapter": chapter,
            "sections": len(sections),
            "success": True,
            "error": None,
            "output_path": str(output_path),
        }

    except Exception as e:
        return {
            "chapter": chapter,
            "sections": 0,
            "success": False,
            "error": str(e),
            "output_path": None,
        }


def main():
    """Convert Minnesota Statutes chapters to Akoma Ntoso XML."""
    parser = argparse.ArgumentParser(description="Convert Minnesota Statutes to Akoma Ntoso XML")
    parser.add_argument(
        "--chapters",
        type=str,
        help="Comma-separated list of chapter numbers (e.g., 290,291,256J)",
    )
    parser.add_argument(
        "--all-tax",
        action="store_true",
        help="Convert all tax-related chapters (270-297I)",
    )
    parser.add_argument(
        "--all-welfare",
        action="store_true",
        help="Convert all human services chapters (256-261)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.5,
        help="Seconds between HTTP requests (default: 0.5)",
    )
    args = parser.parse_args()

    # Determine which chapters to convert
    chapters = []

    if args.chapters:
        chapters = [c.strip() for c in args.chapters.split(",")]
    elif args.all_tax:
        chapters = list(MN_TAX_CHAPTERS.keys())
    elif args.all_welfare:
        chapters = list(MN_WELFARE_CHAPTERS.keys())
    else:
        # Default: main tax chapters
        chapters = ["290", "290A", "291", "295", "297A"]

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Converting {len(chapters)} Minnesota Statute chapters to Akoma Ntoso XML")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Rate limit: {args.rate_limit}s between requests")
    print()

    total_sections = 0
    successful_chapters = 0
    failed_chapters = 0
    empty_chapters = 0

    with MNConverter(rate_limit_delay=args.rate_limit) as converter:
        for chapter in chapters:
            print(f"  Processing Chapter {chapter}...")
            result = convert_chapter(converter, chapter)

            if result["success"]:
                if result["sections"] > 0:
                    successful_chapters += 1
                    total_sections += result["sections"]
                    print(f"    [OK] Chapter {result['chapter']}: {result['sections']} sections")
                else:
                    empty_chapters += 1
                    print(f"    [--] Chapter {result['chapter']}: no sections found")
                    if result["error"]:
                        print(f"         {result['error']}")
            else:
                failed_chapters += 1
                print(f"    [FAIL] Chapter {result['chapter']}: {result['error']}")

    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Chapters requested:    {len(chapters)}")
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
