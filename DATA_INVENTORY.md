# Arch Data Inventory

This document catalogs all data sources that have been downloaded and scraped into the arch repository. **DO NOT re-scrape or re-crawl** - use the existing data.

## Data Locations

### ~/.arch/ (User Cache Directory)
Pre-scraped data cached by arch CLI commands.

| Directory | Contents | Count | Format | Status |
|-----------|----------|-------|--------|--------|
| `~/.arch/canada/` | Canada federal statutes | 958 files | LIMS XML | Complete |
| `~/.arch/federal/` | US federal agency guidance | varies | HTML/PDF | Partial |
| `~/.arch/irs/` | IRS guidance documents | varies | PDF | Partial |
| `~/.arch/policyengine-us/` | State tax forms/instructions by state | ~40 states | PDF | Reference docs |
| `~/.arch/uk/ukpga/` | UK Public General Acts | 3,239 files | CLML XML | Complete (1916-2025) |
| `~/.arch/us-ca/` | California statutes | 3 files | HTML | Partial |

### data/ (Repository Data Directory)

| Directory | Contents | Count | Format | Status |
|-----------|----------|-------|--------|--------|
| `data/uscode/` | US Code titles (USLM XML) | 53 titles | USLM XML | Complete |
| `data/statutes/us-*` | State statute downloads | 48 states | Mixed | Varies (some 404s) |
| `data/microdata/census_blocks/` | Census PL 94-171 data | 1 file | Parquet | Complete |
| `data/us/guidance/irs/` | IRS guidance by year/title | varies | Mixed | Partial |
| `data/ca/` | California statutes/guidance | ~33 items | Mixed | Partial |
| `data/texas_cache/` | Texas statutes | empty | - | Failed |

### sources/ (Source Document Archives)

| Directory | Contents | Count | Format | Status |
|-----------|----------|-------|--------|--------|
| `sources/dc/dc-law-xml/` | DC Council Law Library | 28,432 files | DC XML | Complete |
| `sources/policyengine-us/` | PolicyEngine state data | varies | Mixed | Reference |

## Data Quality Notes

### Complete and Usable
- **US Code**: All 54 titles in USLM XML format (`data/uscode/`)
- **DC Code**: 21,163 sections from DC Council (`sources/dc/dc-law-xml/`)
- **UK Acts**: 3,239 acts in CLML XML (`~/.arch/uk/ukpga/`)
- **Canada**: 958 federal statutes in LIMS XML (`~/.arch/canada/`)
- **7 CFR 271–283 (SNAP)**: 210 rows (parts, subparts, sections) in
  `arch.rules` as of 2026-04-16 (eCFR `2024-04-16` snapshot). Ingested via
  `scripts/ingest_cfr_parts.py`.

### Partial or Problematic
- **State statutes** (`data/statutes/us-*`): Quality varies significantly
  - Some states have 404 error pages (failed downloads)
  - Some have PDFs saved with wrong extensions
  - Check file contents before using

### Not Yet Scraped
- Most US state statutes need better sources
- International jurisdictions (except CA, UK, NZ)

## Rules Repositories Status

Akoma Ntoso XML files pushed to GitHub:

### Complete Repositories

| Repository | Sections | Source |
|------------|----------|--------|
| `RulesFoundation/rules-us` | 60,204 | US Code USLM XML |
| `RulesFoundation/rules-ca` | 601 | Canada LIMS XML |
| `RulesFoundation/rules-us-dc` | 21,163 | DC Law XML |
| `RulesFoundation/rules-uk` | ~3,236 | UK CLML XML |

### State Repositories with Content

| Repository | Sections | Notes |
|------------|----------|-------|
| `rules-us-al` | varies | Alabama Code |
| `rules-us-az` | varies | Arizona Revised Statutes |
| `rules-us-de` | varies | Delaware Code |
| `rules-us-ga` | varies | Georgia Code |
| `rules-us-ia` | varies | Iowa Code |
| `rules-us-ky` | varies | Kentucky Revised Statutes |
| `rules-us-me` | varies | Maine Revised Statutes |
| `rules-us-nc` | varies | North Carolina General Statutes |
| `rules-us-nd` | varies | North Dakota Century Code |
| `rules-us-ne` | varies | Nebraska Revised Statutes |
| `rules-us-nh` | varies | New Hampshire Revised Statutes |
| `rules-us-nv` | 11,842 | Nevada Revised Statutes |
| `rules-us-ny` | 10,976 | NY Consolidated Laws (Tax, Social Services, Education, etc.) |
| `rules-us-oh` | varies | Ohio Revised Code |
| `rules-us-ri` | varies | Rhode Island General Laws |
| `rules-us-sc` | varies | South Carolina Code of Laws |
| `rules-us-tn` | varies | Tennessee Code |
| `rules-us-tx` | varies | Texas Statutes |
| `rules-us-va` | varies | Virginia Code |
| `rules-us-vt` | varies | Vermont Statutes |
| `rules-us-wy` | varies | Wyoming Statutes |

### Empty Scaffolds (30 repos)

Remaining state repos created but awaiting quality source data.

### Source Data Issues

States with unusable source data in `data/statutes/`:
- **AK**: 404 error pages (download failures)
- **LA**: Table of Contents pages only
- **MN, WA, WI, PA**: Navigation/UI pages only
- **OK**: Index pages only

## Adding New Data

When adding new data sources:

1. **Check this inventory first** - data may already exist
2. **Use existing scrapers** in `src/arch/parsers/` and `src/arch/converters/`
3. **Cache to ~/.arch/** for reuse
4. **Document in this file** after scraping

## Converter Reference

Converters in `src/arch/converters/`:

| Converter | Input Format | Output Format |
|-----------|-------------|---------------|
| `uk_clml.py` | UK CLML XML | Arch models |
| `ca_laws.py` | Canada LIMS XML | Arch models |
| `ecfr.py` | eCFR XML | Arch models |
| `nz_pco.py` | NZ PCO XML | Arch models |
| `us_states/*.py` | State HTML/XML | USLM XML |

---
Last updated: 2025-12-31
