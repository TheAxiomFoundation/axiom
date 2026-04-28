#!/usr/bin/env python3
"""Convert New York state statutes to Akoma Ntoso XML.

This script fetches NY statutes via the NY Open Legislation API and converts
them to Akoma Ntoso XML format.

Requirements:
    - Set NY_LEGISLATION_API_KEY environment variable
    - Get a free key at https://legislation.nysenate.gov/

Usage:
    export NY_LEGISLATION_API_KEY="your-api-key"
    python scripts/ny_to_akn.py TAX           # Convert Tax Law
    python scripts/ny_to_akn.py TAX SOS SCL   # Convert multiple laws
    python scripts/ny_to_akn.py --all         # Convert all laws (slow!)
    python scripts/ny_to_akn.py --list        # List available law codes

The converted files are written to /tmp/rules-us-ny-akn/ by default.
"""

import argparse
import os
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path and use uv run for proper environment
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from axiom.converters.us_states.ny import NYStateConverter, NYFetchResult, NY_LAW_CODES
except ImportError:
    # Try direct import for standalone use
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "ny", Path(__file__).parent.parent / "src" / "axiom" / "converters" / "us_states" / "ny.py"
    )
    ny_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ny_module)
    NYStateConverter = ny_module.NYStateConverter
    NYFetchResult = ny_module.NYFetchResult
    NY_LAW_CODES = ny_module.NY_LAW_CODES


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-ny-akn")


def section_to_akn(result: NYFetchResult, converter: NYStateConverter) -> str:
    """Convert a NY section to Akoma Ntoso XML.

    Args:
        result: The fetch result containing section data
        converter: The NY converter (for utility methods)

    Returns:
        Akoma Ntoso XML string
    """
    section = result.section
    law_info = result.law_info

    # Get law name
    law_name = (
        law_info.name if law_info else NY_LAW_CODES.get(section.law_id, f"{section.law_id} Law")
    )

    # Extract section number
    section_num = converter._extract_section_number(section.location_id)
    article_num = converter._extract_article_number(section.location_id)

    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", section.law_id)

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#ny-legislature")

    # Build URIs
    base_uri = f"/akn/us-ny/act/{section.law_id.lower()}/section-{section_num}"

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", base_uri)
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", base_uri)
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", section.active_date or date.today().isoformat())
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#ny-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ny")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", section_num)
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", section.law_id)

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"{base_uri}/eng@{date.today().isoformat()}")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"{base_uri}/eng@{date.today().isoformat()}")
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
    manif_this.set("value", f"{base_uri}/eng@{date.today().isoformat()}/main.xml")
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"{base_uri}/eng@{date.today().isoformat()}/main.xml")
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

    ny_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    ny_leg.set("eId", "ny-legislature")
    ny_leg.set("href", "https://legislation.nysenate.gov")
    ny_leg.set("showAs", "New York State Legislature")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Optional: Add article container if present
    parent_elem = body
    if article_num:
        article = ET.SubElement(body, f"{{{AKN_NS}}}article")
        article.set("eId", f"art_{article_num}")
        art_num = ET.SubElement(article, f"{{{AKN_NS}}}num")
        art_num.text = article_num
        parent_elem = article

    # Section element
    sec_id = f"sec_{section_num}".replace(".", "_").replace("-", "_")
    sec_elem = ET.SubElement(parent_elem, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", sec_id)

    # Section number
    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = section_num

    # Section heading
    heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    heading.text = section.title or f"Section {section_num}"

    # Parse subsections
    subsections = converter._parse_subsections(section.text)

    if subsections:
        add_subsections_to_xml(sec_elem, subsections, sec_id)
    elif section.text:
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        # Limit text size
        p.text = section.text[:10000] if len(section.text) > 10000 else section.text

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    try:
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ", encoding="UTF-8")
        lines = pretty_xml.decode("utf-8").split("\n")
        cleaned = [line for line in lines if line.strip()]
        return "\n".join(cleaned)
    except Exception:
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str


def add_subsections_to_xml(
    parent: ET.Element, subsections: list, parent_id: str, level: int = 0
) -> None:
    """Add subsections recursively to XML element.

    Args:
        parent: Parent XML element
        subsections: List of (identifier, level, text, children) tuples
        parent_id: Parent element ID
        level: Nesting level
    """
    level_tags = ["subsection", "paragraph", "subparagraph", "clause", "subclause"]

    for identifier, _, text, children in subsections:
        tag = level_tags[min(level, len(level_tags) - 1)]
        sub_id = f"{parent_id}__subsec_{identifier}"

        elem = ET.SubElement(parent, f"{{{AKN_NS}}}{tag}")
        elem.set("eId", sub_id)

        # Number
        num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
        num.text = f"({identifier})"

        # Content
        if text:
            content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
            p = ET.SubElement(content, f"{{{AKN_NS}}}p")
            # Limit text size
            p.text = text[:5000] if len(text) > 5000 else text

        # Recurse for children
        if children:
            add_subsections_to_xml(elem, children, sub_id, level + 1)


def convert_law(converter: NYStateConverter, law_id: str, output_dir: Path) -> dict:
    """Convert all sections of a law to AKN.

    Args:
        converter: The NY converter
        law_id: Law code (e.g., "TAX")
        output_dir: Output directory

    Returns:
        Dict with conversion stats
    """
    law_id = law_id.upper()
    law_name = NY_LAW_CODES.get(law_id, f"{law_id} Law")

    print(f"\nConverting {law_name} ({law_id})...")

    # Create law directory
    law_dir = output_dir / law_id.lower()
    law_dir.mkdir(parents=True, exist_ok=True)

    section_count = 0
    error_count = 0

    try:
        for result in converter.iter_sections(law_id):
            try:
                section = result.section
                section_num = converter._extract_section_number(section.location_id)

                # Generate AKN XML
                xml_content = section_to_akn(result, converter)

                # Write to file
                filename = f"{section_num.replace('.', '-').replace('/', '-')}.xml"
                filepath = law_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(xml_content)

                section_count += 1
                if section_count % 100 == 0:
                    print(f"  ... processed {section_count} sections")

            except Exception as e:
                error_count += 1
                print(f"  Error processing section {section.location_id}: {e}")

    except Exception as e:
        print(f"  Error fetching law tree: {e}")
        return {"law": law_id, "sections": section_count, "errors": error_count, "success": False}

    print(f"  Completed: {section_count} sections, {error_count} errors")
    return {"law": law_id, "sections": section_count, "errors": error_count, "success": True}


def main():
    parser = argparse.ArgumentParser(description="Convert NY statutes to Akoma Ntoso XML")
    parser.add_argument("laws", nargs="*", help="Law codes to convert (e.g., TAX SOS SCL)")
    parser.add_argument("--all", action="store_true", help="Convert all available laws")
    parser.add_argument("--list", action="store_true", help="List available law codes")
    parser.add_argument("--output", "-o", type=Path, default=OUTPUT_DIR, help="Output directory")

    args = parser.parse_args()

    # List law codes
    if args.list:
        print("Available NY law codes:")
        print("-" * 60)
        for code, name in sorted(NY_LAW_CODES.items()):
            print(f"  {code:4s}  {name}")
        print()
        print("Usage: python ny_to_akn.py TAX SOS SCL")
        return

    # Check for API key
    api_key = os.environ.get("NY_LEGISLATION_API_KEY")
    if not api_key:
        print("Error: NY_LEGISLATION_API_KEY environment variable not set.")
        print()
        print("To get a free API key:")
        print("  1. Visit https://legislation.nysenate.gov/")
        print("  2. Sign up for a free developer account")
        print("  3. Export your key: export NY_LEGISLATION_API_KEY='your-key'")
        print()
        sys.exit(1)

    # Determine which laws to convert
    if args.all:
        laws = list(NY_LAW_CODES.keys())
        print(f"Converting all {len(laws)} NY laws...")
    elif args.laws:
        laws = [law.upper() for law in args.laws]
    else:
        parser.print_help()
        return

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {args.output}")

    # Initialize converter
    converter = NYStateConverter(api_key=api_key)

    # Convert each law
    results = []
    for law_id in laws:
        if law_id not in NY_LAW_CODES:
            print(f"Warning: Unknown law code '{law_id}', skipping...")
            continue
        result = convert_law(converter, law_id, args.output)
        results.append(result)

    # Close converter
    converter.close()

    # Summary
    print("\n" + "=" * 60)
    print("Conversion Summary")
    print("=" * 60)

    total_sections = sum(r["sections"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    successful_laws = sum(1 for r in results if r["success"])

    for r in results:
        status = "OK" if r["success"] else "FAILED"
        print(f"  [{status}] {r['law']}: {r['sections']} sections, {r['errors']} errors")

    print("-" * 60)
    print(f"Total: {len(results)} laws, {total_sections} sections, {total_errors} errors")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
