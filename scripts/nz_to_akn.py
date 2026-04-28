#!/usr/bin/env python3
"""Convert New Zealand statutes to Akoma Ntoso XML.

This script fetches New Zealand legislation from the official PCO RSS feed
and converts it to Akoma Ntoso XML format.

New Zealand legislation is published by the Parliamentary Counsel Office (PCO)
at https://www.legislation.govt.nz/

Key tax/benefit legislation:
- Income Tax Act 2007 (No 97)
- Tax Administration Act 1994 (No 166)
- Social Security Act 2018 (No 32)
- Working for Families Tax Credits Act 2004
- Child Support Act 1991 (No 142)

Usage:
    python scripts/nz_to_akn.py
    python scripts/nz_to_akn.py --output /tmp/rules-nz-akn
    python scripts/nz_to_akn.py --rss-only  # Just fetch RSS feed items
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import directly to avoid dependency chain issues
from axiom.converters.nz_pco import NZPCOConverter, NZLegislation, NZProvision, NZLabeledParagraph


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-nz-akn")


def nz_legislation_to_akn_xml(legislation: NZLegislation) -> str:
    """Convert an NZLegislation model to Akoma Ntoso XML.

    Args:
        legislation: Parsed NZLegislation object

    Returns:
        XML string in Akoma Ntoso format
    """
    # Register namespace
    ET.register_namespace("akn", AKN_NS)
    ET.register_namespace("", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container (or bill if it's a bill)
    doc_type = "act" if legislation.legislation_type in ("act", "regulation") else "bill"
    act = ET.SubElement(root, f"{{{AKN_NS}}}{doc_type}")
    act.set("name", legislation.title or "")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom")

    # Work URI - following Akoma Ntoso naming convention
    # /akn/{country}/{docType}/{year}/{number}
    work_uri = f"/akn/nz/{legislation.legislation_type}/{legislation.year}/{legislation.number}"

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", work_uri)
    work_uri_elem = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri_elem.set("value", work_uri)
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", (legislation.assent_date or date.today()).isoformat())
    work_date.set("name", "assent")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#nz-parliament")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "nz")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", str(legislation.number))
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", legislation.legislation_type)

    # FRBRExpression
    version_date = legislation.version_date or legislation.assent_date or date.today()
    expr_uri = f"{work_uri}/eng@{version_date.isoformat()}"
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", expr_uri)
    expr_uri_elem = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri_elem.set("value", expr_uri)
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", version_date.isoformat())
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

    nz_parl = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    nz_parl.set("eId", "nz-parliament")
    nz_parl.set("href", "https://www.parliament.nz")
    nz_parl.set("showAs", "New Zealand Parliament")

    pco = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    pco.set("eId", "pco")
    pco.set("href", "https://www.legislation.govt.nz")
    pco.set("showAs", "Parliamentary Counsel Office")

    # Preface with long title if present
    if legislation.long_title:
        preface = ET.SubElement(act, f"{{{AKN_NS}}}preface")
        long_title_elem = ET.SubElement(preface, f"{{{AKN_NS}}}longTitle")
        p = ET.SubElement(long_title_elem, f"{{{AKN_NS}}}p")
        p.text = legislation.long_title

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Add provisions (sections)
    for provision in legislation.provisions:
        add_provision_to_xml(body, provision)

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


def add_provision_to_xml(parent: ET.Element, provision: NZProvision) -> None:
    """Add a provision (section) element to the XML tree.

    Args:
        parent: Parent XML element
        provision: NZProvision object
    """
    # Create section element
    sec_id = (
        f"sec_{provision.label.replace('.', '-')}" if provision.label else f"sec_{provision.id}"
    )
    section = ET.SubElement(parent, f"{{{AKN_NS}}}section")
    section.set("eId", sec_id)

    # Section number
    if provision.label:
        num = ET.SubElement(section, f"{{{AKN_NS}}}num")
        num.text = provision.label

    # Section heading
    if provision.heading:
        heading = ET.SubElement(section, f"{{{AKN_NS}}}heading")
        heading.text = provision.heading

    # Add content if there's direct text
    if provision.text:
        content = ET.SubElement(section, f"{{{AKN_NS}}}content")
        # Split into paragraphs
        paragraphs = provision.text.split("\n\n")
        for para in paragraphs[:20]:  # Limit paragraphs
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para.strip()[:5000]

    # Add subprovisions (subsections)
    for i, subprov in enumerate(provision.subprovisions):
        add_subprovision_to_xml(section, subprov, sec_id, level=1, index=i)

    # Add labeled paragraphs
    for i, para in enumerate(provision.paragraphs):
        add_labeled_para_to_xml(section, para, sec_id, level=1, index=i)


def add_subprovision_to_xml(
    parent: ET.Element, subprov: NZProvision, parent_id: str, level: int, index: int
) -> None:
    """Add a subprovision element to the XML tree.

    Args:
        parent: Parent XML element
        subprov: NZProvision subprovision
        parent_id: Parent element ID
        level: Nesting level
        index: Index in parent's list
    """
    sub_id = f"{parent_id}__subsec_{subprov.label or index}"

    # Choose element type based on level
    if level == 1:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    # Number
    if subprov.label:
        num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
        num.text = f"({subprov.label})"

    # Content
    if subprov.text:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subprov.text[:5000]

    # Recursively add children
    for i, para in enumerate(subprov.paragraphs):
        add_labeled_para_to_xml(elem, para, sub_id, level + 1, i)


def add_labeled_para_to_xml(
    parent: ET.Element, para: NZLabeledParagraph, parent_id: str, level: int, index: int
) -> None:
    """Add a labeled paragraph element to the XML tree.

    Args:
        parent: Parent XML element
        para: NZLabeledParagraph object
        parent_id: Parent element ID
        level: Nesting level
        index: Index in parent's list
    """
    para_id = f"{parent_id}__para_{para.label or index}"

    # Choose element type based on level
    if level <= 2:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", para_id)

    # Number/label
    if para.label:
        num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
        num.text = f"({para.label})"

    # Content
    if para.text:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = para.text[:5000]

    # Recursively add children
    for i, child in enumerate(para.children):
        add_labeled_para_to_xml(elem, child, para_id, level + 1, i)


def fetch_rss_and_convert(output_dir: Path, limit: int = 100) -> dict:
    """Fetch legislation from RSS feed and convert to AKN.

    Args:
        output_dir: Directory to write XML files
        limit: Maximum number of items to process

    Returns:
        Dict with summary statistics
    """
    converter = NZPCOConverter()

    print("Fetching NZ legislation RSS feed...")
    try:
        rss_items = converter.fetch_rss_feed()
        print(f"Found {len(rss_items)} items in RSS feed")
    except Exception as e:
        print(f"Error fetching RSS feed: {e}")
        print("\nNote: The NZ legislation RSS feed may be temporarily unavailable.")
        print("Creating sample AKN files from known legislation structure...")
        return create_sample_akn_files(output_dir)

    results = {
        "total": min(len(rss_items), limit),
        "success": 0,
        "failed": 0,
        "files": [],
    }

    for i, item in enumerate(rss_items[:limit]):
        print(f"  [{i + 1}/{results['total']}] Processing {item.title}...", end=" ")

        try:
            # Create a minimal legislation object from RSS item
            legislation = NZLegislation(
                id=item.id,
                legislation_type=item.legislation_type,
                subtype=item.subtype,
                year=item.year,
                number=item.number,
                title=item.title,
                provisions=[],  # RSS doesn't include full content
            )

            # Convert to AKN
            xml_content = nz_legislation_to_akn_xml(legislation)

            # Write to file
            filename = f"{item.legislation_type}-{item.year}-{item.number:04d}.xml"
            output_path = output_dir / filename

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(xml_content)

            results["success"] += 1
            results["files"].append(str(output_path))
            print("OK")

        except Exception as e:
            results["failed"] += 1
            print(f"FAIL - {e}")

    return results


def create_sample_akn_files(output_dir: Path) -> dict:
    """Create sample AKN files for key NZ legislation.

    This is used when the RSS feed is unavailable. Creates placeholder
    AKN files for key tax and benefit legislation.

    Args:
        output_dir: Directory to write XML files

    Returns:
        Dict with summary statistics
    """
    # Key NZ tax/benefit legislation
    key_legislation = [
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 2007,
            "number": 97,
            "title": "Income Tax Act 2007",
            "assent_date": date(2007, 11, 1),
            "long_title": "An Act to consolidate and amend the law relating to income tax",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 1994,
            "number": 166,
            "title": "Tax Administration Act 1994",
            "assent_date": date(1994, 12, 20),
            "long_title": "An Act to provide for the administration of income tax and other taxes",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 2018,
            "number": 32,
            "title": "Social Security Act 2018",
            "assent_date": date(2018, 11, 26),
            "long_title": "An Act to establish a social security system for New Zealand",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 2002,
            "number": 40,
            "title": "Parental Leave and Employment Protection Act 1987",
            "assent_date": date(1987, 7, 17),
            "long_title": "An Act to provide for parental leave and employment protection",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 1991,
            "number": 142,
            "title": "Child Support Act 1991",
            "assent_date": date(1991, 12, 18),
            "long_title": "An Act to provide for the assessment, collection, and enforcement of child support",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 2023,
            "number": 42,
            "title": "Taxation (Annual Rates for 2023-24, Multinational Tax, and Remedial Matters) Act 2024",
            "assent_date": date(2024, 3, 26),
            "long_title": "An Act to impose annual rates of income tax, make changes concerning multinational tax, and make other amendments",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 2004,
            "number": 52,
            "title": "Taxation (Working for Families) Act 2004",
            "assent_date": date(2004, 6, 21),
            "long_title": "An Act to assist families through the tax system",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 2001,
            "number": 84,
            "title": "New Zealand Superannuation and Retirement Income Act 2001",
            "assent_date": date(2001, 10, 11),
            "long_title": "An Act to provide for New Zealand superannuation",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 2022,
            "number": 21,
            "title": "Cost of Living Payment Act 2022",
            "assent_date": date(2022, 5, 24),
            "long_title": "An Act to provide for cost of living payments",
        },
        {
            "legislation_type": "act",
            "subtype": "public",
            "year": 2000,
            "number": 36,
            "title": "Paid Parental Leave and Employment Protection (Paid Parental Leave) Amendment Act 2002",
            "assent_date": date(2002, 8, 30),
            "long_title": "An Act to provide for paid parental leave",
        },
    ]

    results = {
        "total": len(key_legislation),
        "success": 0,
        "failed": 0,
        "files": [],
    }

    for item in key_legislation:
        print(f"  Creating {item['title']}...", end=" ")

        try:
            legislation = NZLegislation(
                id=f"DLM{item['year']}{item['number']:04d}",
                legislation_type=item["legislation_type"],
                subtype=item["subtype"],
                year=item["year"],
                number=item["number"],
                title=item["title"],
                assent_date=item.get("assent_date"),
                long_title=item.get("long_title", ""),
                provisions=[],
            )

            # Convert to AKN
            xml_content = nz_legislation_to_akn_xml(legislation)

            # Write to file
            filename = f"{item['legislation_type']}-{item['year']}-{item['number']:04d}.xml"
            output_path = output_dir / filename

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(xml_content)

            results["success"] += 1
            results["files"].append(str(output_path))
            print("OK")

        except Exception as e:
            results["failed"] += 1
            print(f"FAIL - {e}")

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Convert New Zealand statutes to Akoma Ntoso XML")
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_DIR),
        help="Output directory for XML files",
    )
    parser.add_argument(
        "--rss-only",
        action="store_true",
        help="Only fetch and display RSS feed items (no conversion)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of items to process from RSS",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Create sample AKN files for key legislation (skip RSS)",
    )

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("New Zealand Statute to Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output directory: {output_dir}")
    print()

    if args.rss_only:
        # Just display RSS feed items
        converter = NZPCOConverter()
        try:
            items = converter.fetch_rss_feed()
            print(f"Found {len(items)} items in RSS feed:\n")
            for item in items[:20]:
                print(f"  {item.title}")
                print(f"    Type: {item.legislation_type}/{item.subtype}")
                print(f"    Year: {item.year}, Number: {item.number}")
                print(f"    Published: {item.published}")
                print()
        except Exception as e:
            print(f"Error fetching RSS: {e}")
        return

    if args.sample:
        # Create sample files directly
        results = create_sample_akn_files(output_dir)
    else:
        # Try RSS feed, fall back to sample files
        results = fetch_rss_and_convert(output_dir, limit=args.limit)

    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Total processed:  {results['total']}")
    print(f"  Successful:       {results['success']}")
    print(f"  Failed:           {results['failed']}")
    print(f"  Output directory: {output_dir}")
    print(f"  Output files:     {len(results.get('files', []))}")


if __name__ == "__main__":
    main()
