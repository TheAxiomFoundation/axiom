# IRS Guidance Documents in Lawarchive

This document describes the IRS guidance document feature added to Atlas.

## Overview

The lawarchive database now stores IRS guidance documents (Revenue Procedures, Revenue Rulings, Notices) alongside statute text. This enables AI encoding agents to:

1. Find authoritative parameter sources for encoded tax rules
2. Link parameters to their source documents
3. Track historical changes in inflation-adjusted amounts
4. Access structured guidance metadata for policy research

## Database Schema

### Tables

**guidance_documents** - Main table for IRS guidance
- `id` (TEXT, PRIMARY KEY) - Document ID (e.g., "rp-2023-34")
- `doc_type` - Type: 'revenue_procedure', 'revenue_ruling', 'notice', 'announcement'
- `doc_number` - Document number (e.g., "2023-34")
- `title` - Full title
- `irb_citation` - Internal Revenue Bulletin citation (e.g., "2023-48 IRB")
- `published_date` - Publication date
- `full_text` - Complete document text
- `sections_json` - Structured sections (JSON)
- `effective_date` - When the guidance takes effect
- `tax_years_json` - Applicable tax years (JSON array)
- `subject_areas_json` - Subject areas (JSON array, e.g., ["EITC", "Income Tax"])
- `parameters_json` - Extracted parameters (JSON object)
- `source_url` - URL to IRB page
- `pdf_url` - URL to PDF version
- `retrieved_at` - Retrieval date

**guidance_statute_refs** - Links guidance to statute sections
- `guidance_id` - References guidance_documents(id)
- `statute_title` - Title number (e.g., 26 for IRC)
- `statute_section` - Section number (e.g., "32" for EITC)
- `ref_type` - Relationship: 'implements', 'interprets', 'modifies', 'cites'
- `excerpt` - Relevant excerpt

**guidance_fts** - Full-text search (FTS5)
- Indexes: title, full_text, subject_areas

## Python API

### Storage

```python
from lawarchive.storage.guidance import GuidanceStorage

storage = GuidanceStorage("lawarchive.db")

# Get a specific document
rp = storage.get_revenue_procedure("2023-34")
print(rp.title)  # "2024 Earned Income Credit Tables"
print(rp.pdf_url)  # URL to PDF

# Get all guidance for a statute section
eitc_guidance = storage.get_guidance_for_statute(26, "32")  # IRC § 32
for doc in eitc_guidance:
    print(f"{doc.doc_number}: {doc.title} (Tax Year {doc.tax_years[0]})")

# Link a guidance document to a statute
storage.link_guidance_to_statute(
    doc_id="rp-2023-34",
    statute_title=26,
    statute_section="32",
    ref_type="implements",
    excerpt="Provides inflation-adjusted EITC amounts per IRC § 32(j)"
)
```

### Models

```python
from lawarchive.models_guidance import RevenueProcedure, GuidanceType

rev_proc = RevenueProcedure(
    doc_number="2023-34",
    doc_type=GuidanceType.REV_PROC,
    title="2024 Earned Income Credit Tables",
    irb_citation="2023-48 IRB",
    published_date=date(2023, 11, 27),
    full_text="...",
    tax_years=[2024],
    subject_areas=["EITC", "Income Tax"],
    source_url="https://www.irs.gov/irb/2023-48_IRB",
    pdf_url="https://www.irs.gov/pub/irs-drop/rp-23-34.pdf",
    retrieved_at=date.today(),
)

# Access document properties
print(rev_proc.path)  # "us/guidance/irs/rp-2023-34"
```

## Scripts

### add_rev_proc_metadata.py

Adds EITC-related Revenue Procedures to the database.

```bash
python scripts/add_rev_proc_metadata.py
```

This script populates:
- Rev. Proc. 2022-38 (2023 EITC parameters)
- Rev. Proc. 2023-34 (2024 EITC parameters)
- Rev. Proc. 2024-40 (2025 EITC parameters)

Each document includes:
- Metadata (title, IRB citation, dates)
- Subject areas
- PDF URLs for full text extraction
- Links to IRC § 32

### query_guidance.py

Demonstrates querying guidance documents.

```bash
python scripts/query_guidance.py
```

Shows:
- All documents in database
- Guidance for specific statute sections
- Individual document details
- Usage examples for AI agents

## EITC Revenue Procedures

### Currently Available

| Rev. Proc. | Tax Year | IRB | PDF |
|------------|----------|-----|-----|
| 2022-38 | 2023 | 2022-45 | [PDF](https://www.irs.gov/pub/irs-drop/rp-22-38.pdf) |
| 2023-34 | 2024 | 2023-48 | [PDF](https://www.irs.gov/pub/irs-drop/rp-23-34.pdf) |
| 2024-40 | 2025 | 2024-50 | [PDF](https://www.irs.gov/pub/irs-drop/rp-24-40.pdf) |

### Key EITC Parameters

Each Rev. Proc. provides inflation-adjusted amounts for:
- Maximum credit amounts (by family size: 0, 1, 2, 3+ children)
- Phase-in percentages
- Phase-out thresholds and percentages
- Earned income limits
- AGI phase-out limits
- Investment income limit

Example from Rev. Proc. 2023-34 (2024 tax year):
- Max credit (3+ children): $7,830
- Phase-out begins: $20,330 (single), $26,960 (married)
- Investment income limit: $11,600

## Usage for AI Encoding Agents

### 1. Find the Right Document

```python
from lawarchive.storage.guidance import GuidanceStorage

storage = GuidanceStorage("lawarchive.db")

# Get EITC guidance for 2024
eitc_docs = storage.get_guidance_for_statute(26, "32")
eitc_2024 = [doc for doc in eitc_docs if 2024 in doc.tax_years][0]

print(f"Source: Rev. Proc. {eitc_2024.doc_number}")
print(f"PDF: {eitc_2024.pdf_url}")
```

### 2. Extract Parameters

```python
# Download PDF from eitc_2024.pdf_url
# Use PDF parser to extract tables
# Store parameters in database

# Example: Maximum EITC credit for 3+ children in 2024
max_credit_3plus = 7830  # from Rev. Proc. 2023-34 Table 1

# Store with citation
params = {
    "26/32/max_credit/3": {
        "value": 7830,
        "year": 2024,
        "source": "Rev. Proc. 2023-34",
        "table": "Table 1"
    }
}
```

### 3. Link Parameters to Source

When encoding in rules-us:

```python
# rules-us/26/32/eitc.yaml

Variable:
  path: 26/32/eitc_max_credit
  entity: TaxUnit
  period: Year
  dtype: Money

Formula:
  year >= 2024:
    n_children >= 3: 7830  # Rev. Proc. 2023-34, Table 1
    n_children == 2: 6960  # Rev. Proc. 2023-34, Table 1
    n_children == 1: 4213  # Rev. Proc. 2023-34, Table 1
    n_children == 0: 632   # Rev. Proc. 2023-34, Table 1
```

### 4. Query Historical Changes

```python
# Compare EITC max credit across years
eitc_docs = storage.get_guidance_for_statute(26, "32")

for doc in sorted(eitc_docs, key=lambda d: d.tax_years[0] if d.tax_years else 0):
    year = doc.tax_years[0] if doc.tax_years else "N/A"
    print(f"{year}: Rev. Proc. {doc.doc_number}")
    print(f"  PDF: {doc.pdf_url}")
```

## Future Enhancements

### Automated PDF Parsing

Create `scripts/extract_rev_proc_params.py` to:
1. Download PDF from `pdf_url`
2. Extract tables using pdfplumber or tabula
3. Parse EITC parameter tables
4. Store in `parameters_json` field
5. Generate RuleSpec DSL snippets

### Full IRB Scraper

Create `src/lawarchive/fetchers/irb_crawler.py` to:
1. Crawl IRS IRB index
2. Discover all Rev. Procs automatically
3. Extract full text from HTML or PDF
4. Parse structured sections
5. Update database with complete content

### Cross-Reference Detection

Enhance to automatically:
1. Detect statute citations in guidance text
2. Create links in `guidance_statute_refs`
3. Find superseded guidance (e.g., Rev. Proc. 2024-40 supersedes 2023-34)
4. Build dependency graph

### Parameter Extraction Rules

Add structured rules for common parameters:
- EITC amounts → `26/32/*`
- Standard deduction → `26/63/*`
- Child Tax Credit → `26/24/*`
- Income brackets → `26/1/*`

## Testing

### Verify Installation

```bash
# Check schema
sqlite3 lawarchive.db ".schema guidance_documents"

# Count documents
sqlite3 lawarchive.db "SELECT COUNT(*) FROM guidance_documents"

# List all
sqlite3 lawarchive.db "SELECT doc_number, title FROM guidance_documents"
```

### Test Queries

```bash
# Run demonstration
python scripts/query_guidance.py

# Check links
sqlite3 lawarchive.db "
  SELECT g.doc_number, r.statute_section, r.ref_type
  FROM guidance_statute_refs r
  JOIN guidance_documents g ON g.id = r.guidance_id
"
```

## Files Added

```
atlas/
├── schema/
│   └── 002_guidance_documents.sql       # Database schema
├── src/atlas/
│   ├── models_guidance.py               # Pydantic models
│   ├── storage/
│   │   └── guidance.py                  # Storage backend
│   └── fetchers/
│       ├── __init__.py
│       └── irs_guidance.py             # IRS.gov fetcher (WIP)
└── scripts/
    ├── add_rev_proc_metadata.py        # Populate EITC Rev. Procs
    └── query_guidance.py               # Query demonstration
```

## Contributing

To add more guidance documents:

1. Update `EITC_REV_PROCS` in `scripts/add_rev_proc_metadata.py`
2. Run the script to populate the database
3. Or use the Python API directly:

```python
from lawarchive.models_guidance import RevenueProcedure, GuidanceType
from lawarchive.storage.guidance import GuidanceStorage
from datetime import date

storage = GuidanceStorage("lawarchive.db")

new_rp = RevenueProcedure(
    doc_number="2025-XX",
    title="Your Title Here",
    irb_citation="2025-YY IRB",
    published_date=date(2025, 1, 1),
    full_text="Document text...",
    tax_years=[2026],
    subject_areas=["Subject"],
    source_url="https://...",
    pdf_url="https://...",
    retrieved_at=date.today(),
)

storage.store_revenue_procedure(new_rp)
```

## License

Same as Atlas (Apache 2.0).
