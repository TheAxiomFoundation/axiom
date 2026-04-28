#!/usr/bin/env python
"""Download and ingest Title 26 (Internal Revenue Code).

Usage:
    python scripts/ingest_title26.py
"""

from pathlib import Path

from axiom.archive import AxiomArchive
from axiom.parsers.us.statutes import download_title


def main():
    data_dir = Path("data/uscode")
    data_dir.mkdir(parents=True, exist_ok=True)

    # Download Title 26 if not present
    xml_path = data_dir / "usc26.xml"
    if not xml_path.exists():
        print("Downloading Title 26 (Internal Revenue Code)...")
        xml_path = download_title(26, data_dir)
    else:
        print(f"Using existing {xml_path}")

    # Ingest into database
    archive = AxiomArchive(db_path="axiom.db")
    count = archive.ingest_title(xml_path)
    print(f"\nDone! Ingested {count} sections into axiom.db")

    # Test a lookup
    print("\nTest lookup: 26 USC 32 (EITC)")
    eitc = archive.get("26 USC 32")
    if eitc:
        print(f"  Title: {eitc.section_title}")
        print(f"  Text length: {len(eitc.text)} chars")
        print(f"  Subsections: {len(eitc.subsections)}")
    else:
        print("  Not found!")


if __name__ == "__main__":
    main()
