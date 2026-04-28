#!/usr/bin/env python3
"""Query IRS guidance documents from the axiom database.

This script demonstrates querying Rev. Procs and other guidance documents.

Usage:
    python scripts/query_guidance.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from axiom_corpus.storage.guidance import GuidanceStorage


def main():
    """Query and display guidance documents."""
    storage = GuidanceStorage("axiom.db")

    print("="*70)
    print("IRS GUIDANCE DOCUMENTS IN AXIOM")
    print("="*70)

    # Get all documents
    all_docs = storage.db.execute(
        """
        SELECT doc_number, title, published_date, tax_years_json, pdf_url
        FROM guidance_documents
        ORDER BY published_date DESC
        """
    ).fetchall()

    print(f"\nTotal documents: {len(all_docs)}\n")

    for doc_num, title, pub_date, tax_years, pdf_url in all_docs:
        print(f"Rev. Proc. {doc_num}")
        print(f"  Title: {title}")
        print(f"  Published: {pub_date}")
        print(f"  Tax Years: {tax_years}")
        print(f"  PDF: {pdf_url}")
        print()

    # Get guidance for IRC § 32 (EITC)
    print("="*70)
    print("GUIDANCE FOR IRC § 32 (EARNED INCOME TAX CREDIT)")
    print("="*70)
    print()

    guidance_for_eitc = storage.get_guidance_for_statute(26, "32")
    print(f"Found {len(guidance_for_eitc)} documents:\n")

    for doc in guidance_for_eitc:
        print(f"Rev. Proc. {doc.doc_number} - {doc.title}")
        print(f"  Applies to: {', '.join(str(y) for y in doc.tax_years)}")
        print(f"  IRB: {doc.irb_citation}")
        print(f"  Source: {doc.source_url}")
        print(f"  PDF: {doc.pdf_url}")
        print(f"  Subject Areas: {', '.join(doc.subject_areas)}")
        print()

    # Retrieve specific document
    print("="*70)
    print("SPECIFIC DOCUMENT DETAILS: Rev. Proc. 2023-34")
    print("="*70)
    print()

    rp_2023_34 = storage.get_revenue_procedure("2023-34")
    if rp_2023_34:
        print(f"Title: {rp_2023_34.title}")
        print(f"Document Type: {rp_2023_34.doc_type.value}")
        print(f"Published: {rp_2023_34.published_date}")
        print(f"Tax Years: {rp_2023_34.tax_years}")
        print(f"Subject Areas: {rp_2023_34.subject_areas}")
        print(f"\nFull Text Preview (first 500 chars):")
        print("-"*70)
        print(rp_2023_34.full_text[:500])
        print("-"*70)
        print(f"\nPDF Available: {rp_2023_34.pdf_url}")
        print(f"IRB Page: {rp_2023_34.source_url}")
    else:
        print("Document not found!")

    print("\n" + "="*70)
    print("USAGE FOR AI AGENTS")
    print("="*70)
    print("""
An AI encoding agent can use this data to:

1. Find the correct Rev. Proc. for a given tax year:
   guidance = storage.get_guidance_for_statute(26, "32")
   eitc_2024 = [g for g in guidance if 2024 in g.tax_years][0]

2. Download the PDF for full parameter extraction:
   pdf_url = eitc_2024.pdf_url
   # Use PDF parser to extract tables and parameters

3. Link parameters to source documents:
   # When encoding EITC max credit amount for 2024,
   # cite Rev. Proc. 2023-34 as the authority

4. Track historical changes:
   # Compare parameters across multiple years
   # Rev. Proc. 2022-38 for 2023, 2023-34 for 2024, etc.
""")

if __name__ == "__main__":
    main()
