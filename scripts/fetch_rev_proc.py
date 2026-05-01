#!/usr/bin/env python3
"""Fetch and store IRS Revenue Procedures in the axiom database.

This script downloads Rev. Procs from IRS.gov and stores them in the axiom
database with full-text search and structured sections.

Usage:
    python scripts/fetch_rev_proc.py 2023-34
    python scripts/fetch_rev_proc.py 2024-40 2023-34 2022-38  # Multiple docs
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher
from axiom_corpus.storage.guidance import GuidanceStorage


def fetch_and_store_rev_proc(doc_number: str, db_path: str = "axiom.db") -> None:
    """Fetch a Revenue Procedure and store it in the database.

    Args:
        doc_number: Document number like "2023-34"
        db_path: Path to SQLite database
    """
    print(f"\n{'='*60}")
    print(f"Fetching Rev. Proc. {doc_number}")
    print(f"{'='*60}\n")

    storage = GuidanceStorage(db_path)

    with IRSGuidanceFetcher() as fetcher:
        try:
            # Fetch the document
            print("Downloading from IRS.gov...")
            rev_proc = fetcher.fetch_revenue_procedure(doc_number)

            print("\nDocument Details:")
            print(f"  Title: {rev_proc.title}")
            print(f"  IRB Citation: {rev_proc.irb_citation}")
            print(f"  Published: {rev_proc.published_date}")
            print(f"  Tax Years: {rev_proc.tax_years}")
            print(f"  Subject Areas: {rev_proc.subject_areas}")
            print(f"  Sections: {len(rev_proc.sections)}")
            print(f"  Full Text Length: {len(rev_proc.full_text)} chars")

            # Store in database
            print("\nStoring in database...")
            storage.store_revenue_procedure(rev_proc)

            # Link to EITC statute if applicable
            if "EITC" in rev_proc.subject_areas or "earned income" in rev_proc.title.lower():
                print("Linking to IRC § 32 (EITC)...")
                year, num = doc_number.split("-")
                doc_id = f"rp-{year}-{num}"
                storage.link_guidance_to_statute(
                    doc_id=doc_id,
                    statute_title=26,
                    statute_section="32",
                    ref_type="implements",
                    excerpt="Provides inflation-adjusted amounts for EITC",
                )

            print(f"\n✓ Successfully stored Rev. Proc. {doc_number}")

            # Test retrieval
            print("\nVerifying storage...")
            retrieved = storage.get_revenue_procedure(doc_number)
            if retrieved:
                print("✓ Successfully retrieved from database")
                print(f"  Title: {retrieved.title}")
            else:
                print("✗ Failed to retrieve from database")

        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python scripts/fetch_rev_proc.py <doc_number> [doc_number...]")
        print("\nExample:")
        print("  python scripts/fetch_rev_proc.py 2023-34")
        print("  python scripts/fetch_rev_proc.py 2024-40 2023-34 2022-38")
        sys.exit(1)

    doc_numbers = sys.argv[1:]
    db_path = "axiom.db"

    print(f"Database: {db_path}")
    print(f"Documents to fetch: {len(doc_numbers)}")

    for doc_number in doc_numbers:
        fetch_and_store_rev_proc(doc_number, db_path)

    print(f"\n{'='*60}")
    print("All documents processed successfully!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
