#!/usr/bin/env python3
"""Convert Oklahoma Statutes to Akoma Ntoso XML.

This script uses the OKConverter to fetch Oklahoma statutes from OSCN
(Oklahoma State Courts Network) and converts them to Akoma Ntoso XML format.

Usage:
    python scripts/ok_to_akn.py

Output:
    /tmp/rules-us-ok-akn/title-{num}/section-{num}.xml
"""

import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from axiom.converters.us_states.ok import (
    OKConverter,
    OK_TITLES,
    OK_SECTIONS,
)
from axiom.models import Section


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-ok-akn")


def create_akn_xml(section: Section, title_num: int, title_name: str) -> str:
    """Create Akoma Ntoso XML for a section.

    Args:
        section: The Section model from OKConverter
        title_num: Oklahoma title number (e.g., 68)
        title_name: Title name (e.g., "Revenue and Taxation")

    Returns:
        XML string
    """
    # Register namespace
    ET.register_namespace("akn", AKN_NS)
    ET.register_namespace("", AKN_NS)

    # Extract section number from citation (e.g., "OK-68-101" -> "68-101")
    section_id = section.citation.section.replace("OK-", "")

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "Oklahoma Statutes")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-ok/act/okst/section-{section_id}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-ok/act/okst/section-{section_id}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#oklahoma-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ok")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", section_id)
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "Oklahoma Statutes")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-ok/act/okst/section-{section_id}/eng@{date.today().isoformat()}"
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value", f"/akn/us-ok/act/okst/section-{section_id}/eng@{date.today().isoformat()}"
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
        "value", f"/akn/us-ok/act/okst/section-{section_id}/eng@{date.today().isoformat()}/main.xml"
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value", f"/akn/us-ok/act/okst/section-{section_id}/eng@{date.today().isoformat()}/main.xml"
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

    ok_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    ok_leg.set("eId", "oklahoma-legislature")
    ok_leg.set("href", "https://www.oklegislature.gov")
    ok_leg.set("showAs", "Oklahoma State Legislature")

    oscn_ref = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    oscn_ref.set("eId", "oscn")
    oscn_ref.set("href", "https://www.oscn.net")
    oscn_ref.set("showAs", "Oklahoma State Courts Network")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Title container
    title_elem = ET.SubElement(body, f"{{{AKN_NS}}}title")
    title_elem.set("eId", f"title_{title_num}")

    title_num_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
    title_num_elem.text = str(title_num)

    title_heading = ET.SubElement(title_elem, f"{{{AKN_NS}}}heading")
    title_heading.text = title_name

    # Section element
    sec_id = section_id.replace("-", "_").replace(".", "_")
    sec_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", f"sec_{sec_id}")

    # Section number
    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = section_id

    # Section heading
    heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    heading.text = section.section_title

    # Add subsections if present
    if section.subsections:
        for subsec in section.subsections:
            add_subsection_to_xml(sec_elem, subsec, sec_id, level=1)
    elif section.text:
        # Plain content
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        # Split into paragraphs
        for para in section.text.split("\n\n"):
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para.strip()[:10000]

    # Add source URL as reference
    if section.source_url:
        note = ET.SubElement(sec_elem, f"{{{AKN_NS}}}authorialNote")
        note.set("marker", "source")
        p = ET.SubElement(note, f"{{{AKN_NS}}}p")
        p.text = f"Source: {section.source_url}"

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


def add_subsection_to_xml(parent: ET.Element, subsection, parent_id: str, level: int) -> None:
    """Add a subsection element to the parent XML element."""
    identifier = subsection.identifier
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
    if level == 1:
        num.text = f"{identifier}."
    else:
        num.text = f"({identifier})"

    # Text content
    if subsection.text:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection.text[:10000]

    # Children
    for child in subsection.children:
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def main():
    """Convert Oklahoma statutes to Akoma Ntoso XML."""
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Oklahoma Statutes to Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    # Group sections by title
    titles_to_fetch = {}
    for section_num, (cite_id, section_title) in OK_SECTIONS.items():
        title_num = int(section_num.split("-")[0])
        if title_num not in titles_to_fetch:
            titles_to_fetch[title_num] = []
        titles_to_fetch[title_num].append((section_num, cite_id, section_title))

    print(f"Titles to process: {sorted(titles_to_fetch.keys())}")
    print(f"Total sections in registry: {len(OK_SECTIONS)}")
    print()

    total_sections = 0
    successful = 0
    failed = 0

    with OKConverter(rate_limit_delay=0.5) as converter:
        for title_num in sorted(titles_to_fetch.keys()):
            title_name = OK_TITLES.get(title_num, f"Title {title_num}")
            sections_list = titles_to_fetch[title_num]

            print(f"Title {title_num}: {title_name}")
            print(f"  Sections to fetch: {len(sections_list)}")

            # Create title directory
            title_dir = OUTPUT_DIR / f"title-{title_num}"
            title_dir.mkdir(exist_ok=True)

            for section_num, cite_id, section_title in sections_list:
                total_sections += 1

                try:
                    # Fetch section from OSCN
                    section = converter.fetch_by_cite_id(cite_id, section_num)

                    # Create AKN XML
                    xml_content = create_akn_xml(section, title_num, title_name)

                    # Write output file
                    safe_section_num = section_num.replace(".", "-")
                    output_path = title_dir / f"section-{safe_section_num}.xml"

                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(xml_content)

                    successful += 1
                    print(f"    [OK] {section_num}: {section_title}")

                except Exception as e:
                    failed += 1
                    print(f"    [FAIL] {section_num}: {e}")

            print()

    print("=" * 60)
    print("Summary:")
    print(f"  Total sections:      {total_sections}")
    print(f"  Successful:          {successful}")
    print(f"  Failed:              {failed}")
    print(f"  Output directory:    {OUTPUT_DIR}")

    # List output files
    output_files = list(OUTPUT_DIR.rglob("*.xml"))
    print(f"  Output XML files:    {len(output_files)}")


if __name__ == "__main__":
    main()
