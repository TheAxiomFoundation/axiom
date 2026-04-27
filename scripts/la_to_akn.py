#!/usr/bin/env python3
"""Convert Louisiana Revised Statutes to Akoma Ntoso XML.

This script fetches Louisiana statutes from legis.la.gov using the LAConverter
and converts them to Akoma Ntoso XML format.

Louisiana uses document IDs rather than direct section URLs. The script parses
the table of contents HTML files to discover document IDs, then fetches each
section and converts to AKN format.

Usage:
    python scripts/la_to_akn.py              # Convert all discovered sections
    python scripts/la_to_akn.py --title 47   # Convert only Title 47 (Revenue)
    python scripts/la_to_akn.py --limit 100  # Limit to first 100 sections
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from xml.dom import minidom
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.la import LAConverter, LA_TITLES

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Input and output paths
INPUT_DIR = Path(__file__).parent.parent / "data" / "statutes" / "us-la"
OUTPUT_DIR = Path("/tmp/rules-us-la-akn")


def parse_toc_for_doc_ids(html_path: Path) -> list[dict]:
    """Parse a table of contents HTML file to extract section document IDs.

    Returns list of dicts with keys:
    - doc_id: Louisiana document ID
    - citation: e.g., "RS 47:21" or "SRULE 1.1"
    - title: Section title text
    """
    sections = []

    try:
        with open(html_path, "r", encoding="utf-8", errors="replace") as f:
            html = f.read()
    except Exception as e:
        print(f"  Error reading {html_path}: {e}")
        return sections

    soup = BeautifulSoup(html, "html.parser")

    # Find all links with d= parameter (document IDs)
    # Pattern: href="~/Law.aspx?d=180351" or href="./Law.aspx?d=180351"
    for link in soup.find_all("a", href=re.compile(r"Law\.aspx\?d=\d+")):
        href = link.get("href", "")
        doc_id_match = re.search(r"d=(\d+)", href)
        if not doc_id_match:
            continue

        doc_id = int(doc_id_match.group(1))
        link_text = link.get_text(strip=True)

        # Try to find the title from sibling or parent elements
        title = ""
        parent = link.find_parent("td")
        if parent:
            # Look for next sibling with title
            next_sibling = parent.find_next_sibling("td")
            if next_sibling:
                title_link = next_sibling.find("a")
                if title_link:
                    title = title_link.get_text(strip=True)

        sections.append(
            {
                "doc_id": doc_id,
                "citation": link_text,
                "title": title,
            }
        )

    return sections


def discover_all_sections(input_dir: Path, title_filter: int | None = None) -> list[dict]:
    """Discover all section document IDs from table of contents files.

    Args:
        input_dir: Directory containing ToC HTML files
        title_filter: Optional title number to filter (e.g., 47 for Revenue)

    Returns:
        List of section dicts with doc_id, citation, title
    """
    all_sections = []
    seen_doc_ids = set()

    # Find ToC files with actual content (larger files)
    toc_files = sorted(input_dir.glob("Legis_Laws_Toc.aspx_*.html"))

    print(f"Found {len(toc_files)} ToC files")

    for toc_file in toc_files:
        # Skip small files (probably just navigation)
        if toc_file.stat().st_size < 50000:
            continue

        print(f"  Parsing {toc_file.name}...")
        sections = parse_toc_for_doc_ids(toc_file)

        for section in sections:
            doc_id = section["doc_id"]
            citation = section["citation"]

            # Skip if already seen
            if doc_id in seen_doc_ids:
                continue
            seen_doc_ids.add(doc_id)

            # Filter by title if specified
            if title_filter:
                # Check if citation starts with RS {title}: pattern
                if not re.match(rf"RS\s+{title_filter}:", citation):
                    continue

            all_sections.append(section)

    return all_sections


def section_to_akn_xml(
    section, converter: LAConverter, original_citation: str = ""
) -> tuple[str | None, str, str]:
    """Convert a Section model to Akoma Ntoso XML.

    Args:
        section: Section model from LAConverter
        converter: LAConverter instance (for context)
        original_citation: Original citation from ToC (e.g., "SRULE 1.1", "RS 47:32")

    Returns:
        Tuple of (XML string or None, title_num, section_num)
    """
    try:
        # Register namespace
        ET.register_namespace("akn", AKN_NS)
        ET.register_namespace("", AKN_NS)

        # Parse citation to get title and section number
        # Format: LA-47:287.445 or LA-0:SRULE 1.1
        citation_str = section.citation.section
        title_num = "0"
        section_num = original_citation or citation_str

        if citation_str.startswith("LA-"):
            parts = citation_str[3:].split(":", 1)
            if len(parts) == 2:
                title_num = parts[0]
                section_num = parts[1] if parts[1] else original_citation

        # If section_num is still empty, use original citation
        if not section_num or section_num == "LA-0:":
            section_num = original_citation

        # Create safe ID from section number
        safe_id = re.sub(r"[^\w\-]", "-", section_num).strip("-")

        # Root element
        root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

        # Act container
        act = ET.SubElement(root, f"{{{AKN_NS}}}act")
        act.set("name", "LRS")

        # Meta section
        meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

        # Identification
        identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
        identification.set("source", "#arch")

        # FRBRWork
        work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
        work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
        work_this.set("value", f"/akn/us-la/act/lrs/title-{title_num}/sec-{safe_id}")
        work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
        work_uri.set("value", f"/akn/us-la/act/lrs/title-{title_num}/sec-{safe_id}")
        work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
        work_date.set("date", date.today().isoformat())
        work_date.set("name", "generation")
        work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
        work_author.set("href", "#louisiana-legislature")
        work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
        work_country.set("value", "us-la")
        work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
        work_number.set("value", section_num)
        work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
        work_name.set("value", "LRS")

        # FRBRExpression
        expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
        expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
        expr_this.set(
            "value",
            f"/akn/us-la/act/lrs/title-{title_num}/sec-{safe_id}/eng@{date.today().isoformat()}",
        )
        expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
        expr_uri.set(
            "value",
            f"/akn/us-la/act/lrs/title-{title_num}/sec-{safe_id}/eng@{date.today().isoformat()}",
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
            f"/akn/us-la/act/lrs/title-{title_num}/sec-{safe_id}/eng@{date.today().isoformat()}/main.xml",
        )
        manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
        manif_uri.set(
            "value",
            f"/akn/us-la/act/lrs/title-{title_num}/sec-{safe_id}/eng@{date.today().isoformat()}/main.xml",
        )
        manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
        manif_date.set("date", date.today().isoformat())
        manif_date.set("name", "generation")
        manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
        manif_author.set("href", "#arch")

        # References
        refs = ET.SubElement(meta, f"{{{AKN_NS}}}references")
        refs.set("source", "#arch")

        arch_ref = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
        arch_ref.set("eId", "arch")
        arch_ref.set("href", "https://axiom-foundation.org")
        arch_ref.set("showAs", "Atlas")

        la_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
        la_leg.set("eId", "louisiana-legislature")
        la_leg.set("href", "https://www.legis.la.gov")
        la_leg.set("showAs", "Louisiana State Legislature")

        # Body
        body = ET.SubElement(act, f"{{{AKN_NS}}}body")

        # Section element
        sec_elem = ET.SubElement(body, f"{{{AKN_NS}}}section")
        sec_elem.set("eId", f"sec_{safe_id}")

        # Section number
        num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
        num.text = section_num

        # Section heading
        if section.section_title:
            heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
            heading.text = section.section_title

        # Add subsections or content
        if section.subsections:
            for subsec in section.subsections:
                add_subsection_to_xml(sec_elem, subsec, safe_id, level=1)
        elif section.text:
            content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
            # Split text into paragraphs
            paragraphs = section.text.split("\n\n")
            for para in paragraphs:
                if para.strip():
                    p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                    p.text = para.strip()[:10000]  # Limit size

        # Add source URL as note
        if section.source_url:
            note = ET.SubElement(sec_elem, f"{{{AKN_NS}}}authorialNote")
            note.set("marker", "source")
            p = ET.SubElement(note, f"{{{AKN_NS}}}p")
            p.text = f"Source: {section.source_url}"

        # Convert to pretty XML string
        xml_str = ET.tostring(root, encoding="unicode")
        try:
            dom = minidom.parseString(xml_str)
            pretty_xml = dom.toprettyxml(indent="  ", encoding="UTF-8")
            lines = pretty_xml.decode("utf-8").split("\n")
            cleaned = [line for line in lines if line.strip()]
            return "\n".join(cleaned), title_num, section_num
        except Exception:
            return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str, title_num, section_num

    except Exception as e:
        print(f"    Error converting section: {e}")
        return None, "0", original_citation


def add_subsection_to_xml(parent: ET.Element, subsec, parent_id: str, level: int) -> None:
    """Add a subsection element to the parent XML element."""
    identifier = subsec.identifier
    sub_id = f"{parent_id}__subsec_{identifier}"

    # Choose element type based on level
    if level == 1:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    # Number/identifier
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    if level == 1 and identifier.isalpha():
        num.text = f"{identifier}."
    else:
        num.text = f"({identifier})"

    # Heading if available
    if subsec.heading:
        heading = ET.SubElement(elem, f"{{{AKN_NS}}}heading")
        heading.text = subsec.heading

    # Text content
    if subsec.text:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsec.text[:10000]

    # Children
    for child in subsec.children:
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def convert_sections(sections: list[dict], output_dir: Path, limit: int | None = None) -> dict:
    """Convert discovered sections to Akoma Ntoso XML.

    Args:
        sections: List of section dicts with doc_id, citation, title
        output_dir: Output directory for XML files
        limit: Optional limit on number of sections to convert

    Returns:
        Stats dict with counts
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    stats = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "skipped": 0,
    }

    # Apply limit
    if limit:
        sections = sections[:limit]

    print(f"\nConverting {len(sections)} sections...")

    with LAConverter(rate_limit_delay=0.5) as converter:
        for i, section_info in enumerate(sections):
            doc_id = section_info["doc_id"]
            citation = section_info["citation"]
            stats["total"] += 1

            try:
                # Fetch section from legis.la.gov
                section = converter.fetch_section_by_id(doc_id)

                # Convert to AKN XML
                xml_content, title_num, section_num = section_to_akn_xml(
                    section, converter, original_citation=citation
                )

                if xml_content:
                    # Create title directory
                    title_dir = output_dir / f"title-{title_num}"
                    title_dir.mkdir(exist_ok=True)

                    # Safe filename from section_num
                    safe_filename = re.sub(r"[^\w\-]", "-", section_num).strip("-")
                    if not safe_filename:
                        safe_filename = f"doc-{doc_id}"
                    output_path = title_dir / f"sec-{safe_filename}.xml"

                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(xml_content)

                    stats["success"] += 1
                    print(f"  [{i + 1}/{len(sections)}] OK: {citation} -> {output_path.name}")
                else:
                    stats["failed"] += 1
                    print(f"  [{i + 1}/{len(sections)}] FAIL: {citation} - No XML generated")

            except Exception as e:
                stats["failed"] += 1
                print(f"  [{i + 1}/{len(sections)}] FAIL: {citation} - {e}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Convert Louisiana Revised Statutes to Akoma Ntoso XML"
    )
    parser.add_argument(
        "--title",
        type=int,
        help="Filter by title number (e.g., 47 for Revenue and Taxation)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of sections to convert",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    print("Louisiana Statutes to Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Input directory:  {INPUT_DIR}")
    print(f"Output directory: {args.output}")

    if args.title:
        title_name = LA_TITLES.get(args.title, f"Title {args.title}")
        print(f"Filtering:        Title {args.title} - {title_name}")

    # Discover sections from ToC files
    print("\nDiscovering sections from table of contents...")
    sections = discover_all_sections(INPUT_DIR, title_filter=args.title)
    print(f"Found {len(sections)} unique sections")

    if not sections:
        print("No sections found. Exiting.")
        return

    # Convert sections
    stats = convert_sections(sections, args.output, limit=args.limit)

    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Total sections:   {stats['total']}")
    print(f"  Successful:       {stats['success']}")
    print(f"  Failed:           {stats['failed']}")
    print(f"  Output directory: {args.output}")

    # List output files
    output_files = list(args.output.rglob("*.xml"))
    print(f"  Output XML files: {len(output_files)}")


if __name__ == "__main__":
    main()
