# IRS Revenue Procedures - Implementation Summary

## What Was Done

Successfully added IRS Revenue Procedure support to Atlas. The system now stores guidance documents alongside statute text, enabling AI encoding agents to access authoritative parameter sources.

## Key Accomplishments

### 1. Database Schema (schema/002_guidance_documents.sql)
- **guidance_documents** table for storing Rev. Procs, Rev. Rulings, Notices
- **guidance_statute_refs** table linking guidance to specific statute sections
- **guidance_fts** full-text search index for querying documents
- Automatic triggers to keep FTS index synchronized

### 2. Data Models (src/lawarchive/models_guidance.py)
- `RevenueProcedure` - Complete document with metadata, sections, parameters
- `GuidanceSection` - Structured section hierarchy
- `GuidanceType` - Enum for document types (Rev. Proc, Rev. Rul, Notice, etc.)
- `GuidanceSearchResult` - Search result model

### 3. Storage Backend (src/lawarchive/storage/guidance.py)
- `GuidanceStorage` class extending SQLite functionality
- Methods:
  - `store_revenue_procedure()` - Store documents with metadata
  - `get_revenue_procedure(doc_number)` - Retrieve by document number
  - `search_guidance(query)` - Full-text search
  - `get_guidance_for_statute(title, section)` - Find guidance for specific statutes
  - `link_guidance_to_statute()` - Create cross-references

### 4. Data Population (scripts/add_rev_proc_metadata.py)
- Populated 3 EITC Revenue Procedures:
  - **Rev. Proc. 2022-38** - 2023 EITC parameters
  - **Rev. Proc. 2023-34** - 2024 EITC parameters
  - **Rev. Proc. 2024-40** - 2025 EITC parameters
- Each includes:
  - Document metadata (title, IRB citation, dates)
  - Subject areas (EITC, Income Tax, Inflation Adjustment)
  - Tax years covered
  - PDF and IRB URLs
  - Links to IRC § 32

### 5. Query Tools (scripts/query_guidance.py)
- Demonstration script showing:
  - Listing all documents
  - Finding guidance for specific statutes
  - Retrieving individual documents
  - Usage examples for AI agents

### 6. Fetcher Framework (src/lawarchive/fetchers/irs_guidance.py)
- Started framework for automated fetching from IRS.gov
- Includes URL lookup table for known Rev. Procs
- HTML parsing infrastructure (needs refinement)
- Designed for future expansion

## Database Contents

```sql
-- 3 Revenue Procedures stored
SELECT doc_number, title, tax_years_json FROM guidance_documents;

rp-2024-40|2025 Earned Income Credit Tables|[2025]
rp-2023-34|2024 Earned Income Credit Tables|[2024]
rp-2022-38|2023 Earned Income Credit Tables|[2023]

-- 3 statute cross-references
SELECT guidance_id, statute_section, ref_type FROM guidance_statute_refs;

rp-2024-40|32|implements
rp-2023-34|32|implements
rp-2022-38|32|implements
```

## Usage for AI Agents

An AI encoding agent can now:

```python
from lawarchive.storage.guidance import GuidanceStorage

storage = GuidanceStorage("lawarchive.db")

# 1. Find the right Rev. Proc for a tax year
eitc_docs = storage.get_guidance_for_statute(26, "32")
eitc_2024 = [d for d in eitc_docs if 2024 in d.tax_years][0]

# 2. Access the PDF for parameter extraction
pdf_url = eitc_2024.pdf_url
# → "https://www.irs.gov/pub/irs-drop/rp-23-34.pdf"

# 3. Use in encoding workflow
print(f"Source: Rev. Proc. {eitc_2024.doc_number}")
print(f"Applies to: {eitc_2024.tax_years}")
print(f"IRB Citation: {eitc_2024.irb_citation}")

# 4. Link parameters to authority
# When encoding max EITC credit for 2024:
# Citation: Rev. Proc. 2023-34, Table 1
# Value: $7,830 (3+ children)
```

## Files Created/Modified

### New Files
```
schema/002_guidance_documents.sql           - Database schema
src/lawarchive/models_guidance.py           - Pydantic models
src/lawarchive/storage/guidance.py          - Storage backend
src/lawarchive/fetchers/__init__.py         - Fetcher package
src/lawarchive/fetchers/irs_guidance.py     - IRS.gov fetcher
scripts/add_rev_proc_metadata.py            - Data population script
scripts/query_guidance.py                   - Query demonstration
GUIDANCE_DOCS.md                            - Complete documentation
SUMMARY.md                                  - This file
```

### Modified Files
```
lawarchive.db                               - SQLite database (schema + data)
```

## Verification

Run these commands to verify the installation:

```bash
# Check schema
sqlite3 lawarchive.db ".schema guidance_documents"

# List documents
python scripts/query_guidance.py

# Count records
sqlite3 lawarchive.db "SELECT COUNT(*) FROM guidance_documents"
# Expected: 3

# Check links
sqlite3 lawarchive.db "
  SELECT g.doc_number, r.statute_section
  FROM guidance_statute_refs r
  JOIN guidance_documents g ON g.id = r.guidance_id
"
# Expected: 3 rows linking to IRC § 32
```

## Next Steps

### Immediate Enhancements

1. **PDF Parameter Extraction**
   - Create `scripts/extract_eitc_params.py`
   - Use pdfplumber to extract tables from Rev. Proc. PDFs
   - Populate `parameters_json` field automatically
   - Example structure:
     ```json
     {
       "26/32/max_credit/3": {"value": 7830, "year": 2024},
       "26/32/phase_out_start/single": {"value": 20330, "year": 2024}
     }
     ```

2. **Full IRB Scraper**
   - Enhance `irs_guidance.py` to scrape full text from IRB pages
   - Or download PDFs directly and extract text
   - Parse structured sections automatically

3. **Historical Parameters**
   - Add more years of EITC Rev. Procs (2020-2021, 2019, etc.)
   - Build time series of parameter changes
   - Enable comparative analysis

### Future Expansions

1. **Other Tax Credits**
   - Child Tax Credit Rev. Procs
   - Standard Deduction inflation adjustments
   - Income tax brackets

2. **Other Guidance Types**
   - Revenue Rulings
   - Notices
   - Announcements
   - Private Letter Rulings (if available)

3. **Integration with RuleSpec**
   - Add `@source` decorator to reference guidance docs
   - Automatic parameter syncing from lawarchive
   - Validation against official sources

4. **API Endpoints**
   - REST API for guidance queries
   - GraphQL interface
   - Integration with RuleSpec API

## Benefits for The Axiom Foundation

1. **Source Documentation**
   - Every parameter can cite its authoritative source
   - Rev. Proc. 2023-34 for 2024 EITC amounts
   - Builds trust and verifiability

2. **Historical Tracking**
   - Compare how parameters change over time
   - Understand inflation adjustments
   - Validate encoded values against official guidance

3. **Automation Potential**
   - AI agents can automatically fetch latest Rev. Procs
   - Extract parameters from PDFs
   - Update rules-us encodings
   - Flag when new guidance is published

4. **Research Capabilities**
   - Query all guidance for a specific statute
   - Search across guidance text
   - Track policy changes over time

## Technical Notes

### SQLite Implementation
- Used sqlite_utils for Python API
- Required explicit `commit()` for writes
- FTS5 for full-text search (may need rebuilding if issues)

### Data Model Design
- Follows existing patterns in lawarchive
- JSON fields for structured data (sections, parameters, arrays)
- ISO date formats for consistency
- Document IDs follow pattern: `rp-YYYY-NN`

### PDF URLs
- IRS publishes PDFs at: `https://www.irs.gov/pub/irs-drop/rp-YY-NN.pdf`
- IRB pages at: `https://www.irs.gov/irb/YYYY-NN_IRB`
- Pattern is consistent and reliable

## Conclusion

The IRS guidance document feature is now fully operational in Atlas. The system provides:

- ✅ Database schema for guidance documents
- ✅ Python models and storage API
- ✅ 3 EITC Revenue Procedures (2023-2025)
- ✅ Cross-references to IRC § 32
- ✅ Query and retrieval functionality
- ✅ Foundation for automated parameter extraction
- ✅ Complete documentation

AI encoding agents can now access authoritative parameter sources, enabling fully-cited and verifiable tax and benefit encodings in RuleSpec.
