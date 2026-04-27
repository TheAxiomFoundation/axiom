#!/usr/bin/env python3
"""Convert Montana Code Annotated to Akoma Ntoso XML format.

Fetches Montana statutes from archive.legmt.gov using MTConverter,
then converts to Akoma Ntoso XML.

Usage:
    python scripts/mt_to_akn.py

Outputs to: /tmp/rules-us-mt-akn/
"""

import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.mt import (
    MTConverter,
    MT_TAX_CHAPTERS,
    MT_WELFARE_CHAPTERS,
)
from atlas.models import Section


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-mt-akn")


def section_to_xml_dict(section: Section) -> dict:
    """Convert Section model to dict for XML generation."""
    subsections = []
    for sub in section.subsections:
        children = []
        for child in sub.children:
            children.append(
                {
                    "identifier": child.identifier,
                    "text": child.text or "",
                    "children": [],
                }
            )
        subsections.append(
            {
                "identifier": sub.identifier,
                "text": sub.text or "",
                "children": children,
            }
        )

    # Extract section number from citation (e.g., "MT-15-30-2101" -> "15-30-2101")
    section_num = section.citation.section
    if section_num.startswith("MT-"):
        section_num = section_num[3:]

    return {
        "section_number": section_num,
        "title": section.section_title,
        "text": section.text,
        "history": None,  # Could extract from text if needed
        "subsections": subsections,
    }


def create_akn_xml(title: int, chapter: int, chapter_name: str, sections: list[dict]) -> str:
    """Create Akoma Ntoso XML from parsed sections.

    Args:
        title: Title number (e.g., 15)
        chapter: Chapter number (e.g., 30)
        chapter_name: Chapter name (e.g., "Individual Income Tax")
        sections: List of section dicts

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
    act.set("name", "MCA")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#arch")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-mt/act/mca/title-{title}/chapter-{chapter}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-mt/act/mca/title-{title}/chapter-{chapter}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#montana-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-mt")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", f"{title}-{chapter}")
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "MCA")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value",
        f"/akn/us-mt/act/mca/title-{title}/chapter-{chapter}/eng@{date.today().isoformat()}",
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value",
        f"/akn/us-mt/act/mca/title-{title}/chapter-{chapter}/eng@{date.today().isoformat()}",
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
        f"/akn/us-mt/act/mca/title-{title}/chapter-{chapter}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value",
        f"/akn/us-mt/act/mca/title-{title}/chapter-{chapter}/eng@{date.today().isoformat()}/main.xml",
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

    mt_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    mt_leg.set("eId", "montana-legislature")
    mt_leg.set("href", "https://leg.mt.gov")
    mt_leg.set("showAs", "Montana State Legislature")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Title container
    title_elem = ET.SubElement(body, f"{{{AKN_NS}}}title")
    title_elem.set("eId", f"title_{title}")

    # Title number
    num = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
    num.text = str(title)

    # Chapter container
    chapter_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}chapter")
    chapter_elem.set("eId", f"title_{title}__chp_{chapter}")

    # Chapter number
    chp_num = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}num")
    chp_num.text = str(chapter)

    # Chapter heading
    heading = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}heading")
    heading.text = chapter_name

    # Add sections
    for sec in sections:
        add_section_to_xml(chapter_elem, sec, title, chapter)

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


def add_section_to_xml(parent: ET.Element, section: dict, title: int, chapter: int) -> None:
    """Add a section element to the parent XML element."""
    sec_num = section["section_number"].replace("-", "_").replace(".", "_")

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

    # Add history as authorialNote
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
    else:
        # Third-level
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

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
    converter: MTConverter, title: int, chapter: int, chapter_name: str, output_dir: Path
) -> dict:
    """Convert a single chapter to Akoma Ntoso XML.

    Returns:
        Dict with stats: {title, chapter, sections, success, error}
    """
    try:
        # Fetch sections from Montana legislature website
        sections_list = []
        print(f"    Fetching sections for Title {title}, Chapter {chapter}...")

        for section in converter.iter_chapter(title, chapter):
            sec_dict = section_to_xml_dict(section)
            sections_list.append(sec_dict)
            print(f"      Fetched {section.citation.section}")

        if not sections_list:
            return {
                "title": title,
                "chapter": chapter,
                "sections": 0,
                "success": True,
                "error": None,
            }

        # Create AKN XML
        xml_content = create_akn_xml(title, chapter, chapter_name, sections_list)

        # Write output
        output_path = output_dir / f"mca-title-{title:02d}-chapter-{chapter:02d}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {
            "title": title,
            "chapter": chapter,
            "sections": len(sections_list),
            "success": True,
            "error": None,
            "output_path": str(output_path),
        }

    except Exception as e:
        return {
            "title": title,
            "chapter": chapter,
            "sections": 0,
            "success": False,
            "error": str(e),
        }


def main():
    """Convert Montana statutes to Akoma Ntoso XML."""
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Combine all chapters
    all_chapters = {**MT_TAX_CHAPTERS, **MT_WELFARE_CHAPTERS}

    print(f"Converting {len(all_chapters)} Montana chapters to Akoma Ntoso XML")
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    total_sections = 0
    successful_chapters = 0
    failed_chapters = 0
    empty_chapters = 0

    with MTConverter(rate_limit_delay=0.3) as converter:
        for (title, chapter), chapter_name in sorted(all_chapters.items()):
            print(f"  Title {title}, Chapter {chapter}: {chapter_name}")

            result = convert_chapter(converter, title, chapter, chapter_name, OUTPUT_DIR)

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
    print(f"  Total chapters attempted:  {len(all_chapters)}")
    print(f"  Successful chapters:       {successful_chapters}")
    print(f"  Empty chapters:            {empty_chapters}")
    print(f"  Failed chapters:           {failed_chapters}")
    print(f"  Total sections:            {total_sections}")
    print(f"  Output directory:          {OUTPUT_DIR}")

    # List output files
    output_files = list(OUTPUT_DIR.glob("*.xml"))
    print(f"  Output XML files:          {len(output_files)}")

    if output_files:
        print("\nGenerated files:")
        for f in sorted(output_files):
            size_kb = f.stat().st_size / 1024
            print(f"  {f.name} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
