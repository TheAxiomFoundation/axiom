# Quick Start - IRS Guidance Documents

## 30-Second Overview

Atlas now stores IRS Revenue Procedures (and other guidance) with links to statute sections. This enables AI agents to find authoritative parameter sources.

## Quick Examples

### Get EITC guidance for 2024

```python
from lawarchive.storage.guidance import GuidanceStorage

storage = GuidanceStorage("lawarchive.db")

# Find all EITC guidance
eitc_docs = storage.get_guidance_for_statute(26, "32")

# Get 2024 document
doc_2024 = [d for d in eitc_docs if 2024 in d.tax_years][0]

print(doc_2024.title)
# → "2024 Earned Income Credit Tables"

print(doc_2024.pdf_url)
# → "https://www.irs.gov/pub/irs-drop/rp-23-34.pdf"
```

### Get specific Rev. Proc.

```python
rp = storage.get_revenue_procedure("2023-34")

print(f"{rp.title}")
print(f"Tax Years: {rp.tax_years}")
print(f"PDF: {rp.pdf_url}")
print(f"IRB: {rp.irb_citation}")
```

### Direct SQL queries

```bash
# List all documents
sqlite3 lawarchive.db "
  SELECT doc_number, title, tax_years_json
  FROM guidance_documents
"

# Find guidance for a statute
sqlite3 lawarchive.db "
  SELECT g.doc_number, g.title
  FROM guidance_documents g
  JOIN guidance_statute_refs r ON r.guidance_id = g.id
  WHERE r.statute_title = 26 AND r.statute_section = '32'
"
```

## What's Available

Currently in database:
- **Rev. Proc. 2022-38** - 2023 EITC parameters
- **Rev. Proc. 2023-34** - 2024 EITC parameters
- **Rev. Proc. 2024-40** - 2025 EITC parameters

All linked to IRC § 32 (26 USC 32)

## Add More Documents

```python
from lawarchive.models_guidance import RevenueProcedure, GuidanceType
from lawarchive.storage.guidance import GuidanceStorage
from datetime import date

storage = GuidanceStorage("lawarchive.db")

new_doc = RevenueProcedure(
    doc_number="2025-XX",
    title="Your Title",
    irb_citation="2025-YY IRB",
    published_date=date(2025, 1, 1),
    full_text="Document content...",
    tax_years=[2026],
    subject_areas=["Subject"],
    source_url="https://www.irs.gov/irb/...",
    pdf_url="https://www.irs.gov/pub/irs-drop/...",
    retrieved_at=date.today(),
)

storage.store_revenue_procedure(new_doc)

# Link to statute
storage.link_guidance_to_statute(
    doc_id="rp-2025-XX",
    statute_title=26,
    statute_section="32",
    ref_type="implements"
)
```

## Scripts

```bash
# Populate EITC Rev. Procs
python scripts/add_rev_proc_metadata.py

# Query and demonstrate
python scripts/query_guidance.py
```

## For AI Agents

When encoding EITC parameters in rules-us:

```python
# 1. Find source document
storage = GuidanceStorage("lawarchive.db")
eitc_docs = storage.get_guidance_for_statute(26, "32")
doc = [d for d in eitc_docs if 2024 in d.tax_years][0]

# 2. Note PDF URL for parameter extraction
pdf_url = doc.pdf_url  # Download and parse tables

# 3. Cite in encoding
# rules-us/26/32/eitc.yaml
#   max_credit: 7830  # Rev. Proc. 2023-34, Table 1
```

## Full Documentation

See `GUIDANCE_DOCS.md` for complete API reference and `SUMMARY.md` for implementation details.

## Database Location

`/Users/maxghenis/TheAxiomFoundation/atlas/atlas.db`

Schema: `schema/002_guidance_documents.sql`
