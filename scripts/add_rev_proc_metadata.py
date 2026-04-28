#!/usr/bin/env python3
"""Add IRS Revenue Procedure metadata to the axiom database.

This script adds Rev. Proc. metadata records that can be populated with full text later.
The metadata includes document numbers, IRB citations, and links to source documents.

Usage:
    python scripts/add_rev_proc_metadata.py
"""

import sys
from datetime import date
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from axiom.models_guidance import GuidanceType, RevenueProcedure
from axiom.storage.guidance import GuidanceStorage


# EITC-related Revenue Procedures with metadata
EITC_REV_PROCS = [
    {
        "doc_number": "2023-34",
        "title": "2024 Earned Income Credit Tables",
        "irb_citation": "2023-48 IRB",
        "published_date": date(2023, 11, 27),
        "source_url": "https://www.irs.gov/irb/2023-48_IRB",
        "pdf_url": "https://www.irs.gov/pub/irs-drop/rp-23-34.pdf",
        "tax_years": [2024],
        "subject_areas": ["EITC", "Income Tax", "Inflation Adjustment"],
        "full_text_placeholder": """Rev. Proc. 2023-34 - 2024 Earned Income Credit Tables

This Revenue Procedure provides the inflation-adjusted amounts for the Earned Income Tax Credit (EITC)
for taxable years beginning in 2024, as required by § 32(j) of the Internal Revenue Code.

SECTION 1. PURPOSE
This revenue procedure sets forth inflation-adjusted items for 2024 for the Earned Income Tax Credit
under § 32 of the Internal Revenue Code of 1986 (Code).

SECTION 2. CHANGES
The adjusted items are listed in Tables 1 through 8 at the end of this revenue procedure.

[Full text would be extracted from PDF]

Key parameters for 2024:
- Maximum credit amounts by family size
- Phase-in and phase-out percentages
- Earned income and AGI limits
- Investment income limit

See https://www.irs.gov/pub/irs-drop/rp-23-34.pdf for complete tables.
""",
    },
    {
        "doc_number": "2024-40",
        "title": "2025 Earned Income Credit Tables",
        "irb_citation": "2024-50 IRB",
        "published_date": date(2024, 12, 9),
        "source_url": "https://www.irs.gov/irb/2024-50_IRB",
        "pdf_url": "https://www.irs.gov/pub/irs-drop/rp-24-40.pdf",
        "tax_years": [2025],
        "subject_areas": ["EITC", "Income Tax", "Inflation Adjustment"],
        "full_text_placeholder": """Rev. Proc. 2024-40 - 2025 Earned Income Credit Tables

This Revenue Procedure provides the inflation-adjusted amounts for the Earned Income Tax Credit (EITC)
for taxable years beginning in 2025, as required by § 32(j) of the Internal Revenue Code.

See https://www.irs.gov/pub/irs-drop/rp-24-40.pdf for complete tables.
""",
    },
    {
        "doc_number": "2022-38",
        "title": "2023 Earned Income Credit Tables",
        "irb_citation": "2022-45 IRB",
        "published_date": date(2022, 10, 18),
        "source_url": "https://www.irs.gov/irb/2022-45_IRB",
        "pdf_url": "https://www.irs.gov/pub/irs-drop/rp-22-38.pdf",
        "tax_years": [2023],
        "subject_areas": ["EITC", "Income Tax", "Inflation Adjustment"],
        "full_text_placeholder": """Rev. Proc. 2022-38 - 2023 Earned Income Credit Tables

This Revenue Procedure provides the inflation-adjusted amounts for the Earned Income Tax Credit (EITC)
for taxable years beginning in 2023, as required by § 32(j) of the Internal Revenue Code.

See https://www.irs.gov/pub/irs-drop/rp-22-38.pdf for complete tables.
""",
    },
]


def add_rev_proc_metadata(db_path: str = "axiom.db") -> None:
    """Add Revenue Procedure metadata to the database."""
    storage = GuidanceStorage(db_path)

    for rp_data in EITC_REV_PROCS:
        print(f"\nAdding Rev. Proc. {rp_data['doc_number']}")
        print(f"  Title: {rp_data['title']}")
        print(f"  Tax Years: {rp_data['tax_years']}")
        print(f"  PDF: {rp_data['pdf_url']}")

        # Create RevenueProcedure object
        rev_proc = RevenueProcedure(
            doc_number=rp_data["doc_number"],
            doc_type=GuidanceType.REV_PROC,
            title=rp_data["title"],
            irb_citation=rp_data["irb_citation"],
            published_date=rp_data["published_date"],
            full_text=rp_data["full_text_placeholder"],
            sections=[],  # Would be populated by full parser
            effective_date=None,
            tax_years=rp_data["tax_years"],
            subject_areas=rp_data["subject_areas"],
            parameters={},  # Would be extracted from full text
            source_url=rp_data["source_url"],
            pdf_url=rp_data["pdf_url"],
            retrieved_at=date.today(),
        )

        # Store in database
        storage.store_revenue_procedure(rev_proc)

        # Link to EITC statute (26 USC § 32)
        year, num = rp_data["doc_number"].split("-")
        doc_id = f"rp-{year}-{num}"
        storage.link_guidance_to_statute(
            doc_id=doc_id,
            statute_title=26,
            statute_section="32",
            ref_type="implements",
            excerpt="Provides inflation-adjusted EITC amounts per IRC § 32(j)",
        )

        print(f"  ✓ Stored and linked to IRC § 32")


def main():
    """Main entry point."""
    db_path = "axiom.db"

    print("="*60)
    print("Adding EITC Revenue Procedure Metadata")
    print("="*60)

    add_rev_proc_metadata(db_path)

    print("\n" + "="*60)
    print("Complete!")
    print("="*60)

    # Show what was added
    storage = GuidanceStorage(db_path)
    print("\nVerifying entries...")

    for rp_data in EITC_REV_PROCS:
        doc_num = rp_data["doc_number"]
        retrieved = storage.get_revenue_procedure(doc_num)
        if retrieved:
            print(f"  ✓ {doc_num}: {retrieved.title}")
        else:
            print(f"  ✗ {doc_num}: NOT FOUND")

    print("\nSearching for 'EITC'...")
    results = storage.search_guidance("EITC", limit=5)
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r.doc_number}: {r.title}")

    print("\nGuidance for IRC § 32:")
    guidance = storage.get_guidance_for_statute(26, "32")
    print(f"  Found {len(guidance)} documents:")
    for g in guidance:
        print(f"    - Rev. Proc. {g.doc_number} ({g.tax_years[0] if g.tax_years else 'N/A'})")


if __name__ == "__main__":
    main()
