#!/usr/bin/env python3
"""Convert California statutes to Akoma Ntoso XML.

This script uses the CAStateConverter to fetch California Code sections
and converts them to Akoma Ntoso XML format.

California has 29 codes. We focus on tax/welfare relevant codes:
- RTC: Revenue and Taxation Code (state tax law)
- WIC: Welfare and Institutions Code (benefits programs)
- UIC: Unemployment Insurance Code (UI, disability insurance)

Key sections for PolicyEngine work:
- RTC 17052: California EITC (CalEITC)
- RTC 17041: Personal income tax rates
- RTC 17041.5: Additional tax rates
- WIC 11320.3: CalWORKs provisions
- UIC 2655: State Disability Insurance

Usage:
    python scripts/ca_to_akn.py
    python scripts/ca_to_akn.py --sections rtc/17052,rtc/17041
    python scripts/ca_to_akn.py --code RTC --max 50
"""

import argparse
import os
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from axiom.converters.us_states.ca import CAStateConverter, CA_CODES
from axiom.models_statute import Statute, StatuteSubsection


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-ca-akn")

# Key sections for tax/benefit policy work
KEY_SECTIONS = {
    "RTC": [
        # Personal income tax
        "17041",
        "17041.5",
        "17043",
        "17044",
        # California EITC (CalEITC)
        "17052",
        # Young child tax credit
        "17052.1",
        # Exemptions and deductions
        "17054",
        "17054.5",
        "17054.6",
        "17054.7",
        # Standard deduction
        "17073.5",
        # Credits
        "17052",
        "17052.5",
        "17052.6",
        "17052.10",
        "17052.12",
        "17052.25",
        # Child care credit
        "17052.17",
        "17052.18",
        # Renter's credit
        "17053.5",
        # Senior head of household credit
        "17054.7",
    ],
    "WIC": [
        # CalWORKs
        "11320",
        "11320.1",
        "11320.3",
        "11322",
        "11322.8",
        "11450",
        "11450.5",
        "11451",
        "11453",
        # CalFresh/SNAP
        "18900",
        "18901",
        "18904",
        # Child care
        "10207",
        "10209",
        "10210",
        "10211",
        # Medi-Cal
        "14005",
        "14005.7",
        "14007",
    ],
    "UIC": [
        # State Disability Insurance
        "2655",
        "2656",
        "2656.1",
        # Paid Family Leave
        "3301",
        "3302",
        # Unemployment Insurance
        "1252",
        "1253",
        "1253.5",
    ],
}


def statute_to_akn_xml(statute: Statute) -> str:
    """Convert a Statute model to Akoma Ntoso XML.

    Args:
        statute: Parsed Statute object

    Returns:
        XML string in Akoma Ntoso format
    """
    # Register namespace
    ET.register_namespace("akn", AKN_NS)
    ET.register_namespace("", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", statute.code)

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom")

    # Work URI
    work_uri = f"/akn/{statute.jurisdiction}/act/{statute.code.lower()}/{statute.section}"

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", work_uri)
    work_uri_elem = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri_elem.set("value", work_uri)
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#california-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ca")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", statute.section)
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", statute.code)

    # FRBRExpression
    expr_uri = f"{work_uri}/eng@{date.today().isoformat()}"
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", expr_uri)
    expr_uri_elem = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri_elem.set("value", expr_uri)
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", date.today().isoformat())
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#axiom")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif_uri = f"{expr_uri}/main.xml"
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", manif_uri)
    manif_uri_elem = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri_elem.set("value", manif_uri)
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

    ca_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    ca_leg.set("eId", "california-legislature")
    ca_leg.set("href", "https://leginfo.legislature.ca.gov")
    ca_leg.set("showAs", "California State Legislature")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Add structural hierarchy if present
    parent_elem = body

    if statute.division:
        div_elem = ET.SubElement(parent_elem, f"{{{AKN_NS}}}division")
        div_elem.set("eId", f"div_{statute.division}")
        div_num = ET.SubElement(div_elem, f"{{{AKN_NS}}}num")
        div_num.text = statute.division
        parent_elem = div_elem

    if statute.part:
        part_elem = ET.SubElement(parent_elem, f"{{{AKN_NS}}}part")
        part_elem.set("eId", f"part_{statute.part}")
        part_num = ET.SubElement(part_elem, f"{{{AKN_NS}}}num")
        part_num.text = statute.part
        parent_elem = part_elem

    if statute.chapter:
        chap_elem = ET.SubElement(parent_elem, f"{{{AKN_NS}}}chapter")
        chap_elem.set("eId", f"chp_{statute.chapter}")
        chap_num = ET.SubElement(chap_elem, f"{{{AKN_NS}}}num")
        chap_num.text = statute.chapter
        parent_elem = chap_elem

    if statute.article:
        art_elem = ET.SubElement(parent_elem, f"{{{AKN_NS}}}article")
        art_elem.set("eId", f"art_{statute.article}")
        art_num = ET.SubElement(art_elem, f"{{{AKN_NS}}}num")
        art_num.text = statute.article
        parent_elem = art_elem

    # Section
    sec_id = f"sec_{statute.section.replace('.', '-')}"
    section = ET.SubElement(parent_elem, f"{{{AKN_NS}}}section")
    section.set("eId", sec_id)

    # Section number
    num = ET.SubElement(section, f"{{{AKN_NS}}}num")
    num.text = statute.section

    # Section heading
    heading = ET.SubElement(section, f"{{{AKN_NS}}}heading")
    heading.text = statute.title

    # Add subsections or plain content
    if statute.subsections:
        for sub in statute.subsections:
            add_subsection_to_xml(section, sub, sec_id, level=1)
    elif statute.text:
        content = ET.SubElement(section, f"{{{AKN_NS}}}content")
        # Split text into paragraphs
        paragraphs = statute.text.split("\n\n")
        for para in paragraphs[:20]:  # Limit paragraphs
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para.strip()[:5000]

    # Add history as authorialNote
    if statute.history:
        note = ET.SubElement(section, f"{{{AKN_NS}}}authorialNote")
        note.set("marker", "history")
        p = ET.SubElement(note, f"{{{AKN_NS}}}p")
        p.text = statute.history

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


def add_subsection_to_xml(
    parent: ET.Element, sub: StatuteSubsection, parent_id: str, level: int
) -> None:
    """Add a subsection element to the XML tree.

    Args:
        parent: Parent XML element
        sub: StatuteSubsection object
        parent_id: Parent element ID for constructing child IDs
        level: Nesting level (1=subsection, 2=paragraph, 3+=subparagraph)
    """
    identifier = sub.identifier
    sub_id = f"{parent_id}__subsec_{identifier}"

    # Choose element type based on level
    if level == 1:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    # Number
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"({identifier})"

    # Heading if present
    if sub.heading:
        heading = ET.SubElement(elem, f"{{{AKN_NS}}}heading")
        heading.text = sub.heading

    # Content
    if sub.text:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = sub.text[:5000]

    # Recursively add children
    for child in sub.children:
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def fetch_and_convert(converter: CAStateConverter, ref: str, output_dir: Path) -> dict:
    """Fetch a section and convert to AKN XML.

    Args:
        converter: CAStateConverter instance
        ref: Section reference (e.g., "rtc/17052")
        output_dir: Directory to write XML files

    Returns:
        Dict with status info: {ref, success, error, path}
    """
    try:
        # Fetch the statute
        statute = converter.fetch(ref, cache=True)

        # Convert to AKN XML
        xml_content = statute_to_akn_xml(statute)

        # Write to file
        code = statute.code.lower()
        section = statute.section.replace(".", "-")
        output_path = output_dir / f"{code}-{section}.xml"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {
            "ref": ref,
            "success": True,
            "error": None,
            "path": str(output_path),
            "title": statute.title,
        }

    except Exception as e:
        return {
            "ref": ref,
            "success": False,
            "error": str(e),
            "path": None,
            "title": None,
        }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Convert California statutes to Akoma Ntoso XML")
    parser.add_argument(
        "--sections",
        type=str,
        help="Comma-separated list of sections (e.g., rtc/17052,wic/11320)",
    )
    parser.add_argument(
        "--code",
        type=str,
        choices=list(CA_CODES.keys()),
        help="Process all key sections from a specific code",
    )
    parser.add_argument(
        "--all-key",
        action="store_true",
        help="Process all key tax/benefit sections",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_DIR),
        help="Output directory for XML files",
    )

    args = parser.parse_args()

    # Determine output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build list of sections to fetch
    sections_to_fetch = []

    if args.sections:
        sections_to_fetch = [s.strip() for s in args.sections.split(",")]
    elif args.code:
        code = args.code.upper()
        if code in KEY_SECTIONS:
            sections_to_fetch = [f"{code.lower()}/{sec}" for sec in KEY_SECTIONS[code]]
        else:
            print(f"No key sections defined for {code}, fetching nothing.")
            return
    elif args.all_key:
        for code, sections in KEY_SECTIONS.items():
            sections_to_fetch.extend([f"{code.lower()}/{sec}" for sec in sections])
    else:
        # Default: fetch all key sections
        for code, sections in KEY_SECTIONS.items():
            sections_to_fetch.extend([f"{code.lower()}/{sec}" for sec in sections])

    print(f"California Statute to Akoma Ntoso Converter")
    print(f"=" * 60)
    print(f"Output directory: {output_dir}")
    print(f"Sections to fetch: {len(sections_to_fetch)}")
    print()

    # Create converter
    converter = CAStateConverter()

    # Track results
    success_count = 0
    fail_count = 0

    for ref in sections_to_fetch:
        print(f"  Fetching {ref}...", end=" ")
        result = fetch_and_convert(converter, ref, output_dir)

        if result["success"]:
            success_count += 1
            title_preview = result["title"][:40] if result["title"] else ""
            print(f"OK - {title_preview}")
        else:
            fail_count += 1
            print(f"FAIL - {result['error']}")

    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Total sections:   {len(sections_to_fetch)}")
    print(f"  Successful:       {success_count}")
    print(f"  Failed:           {fail_count}")
    print(f"  Output directory: {output_dir}")

    # List output files
    output_files = list(output_dir.glob("*.xml"))
    print(f"  Output XML files: {len(output_files)}")


if __name__ == "__main__":
    main()
