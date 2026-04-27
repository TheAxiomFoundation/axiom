# DC Law XML Import Guide

This document describes the structure of the DC Council's law-xml repository and how to extract tax provisions for use with RuleSpec.

## Repository Overview

- **Source Repository**: https://github.com/DCCouncil/dc-law-xml
- **Local Clone**: `sources/dc/dc-law-xml/`
- **Format**: XML with custom DC Council schema
- **Scope**: District of Columbia statutes and code

## XML Schema

The DC Law XML uses custom namespaces defined in the `schemas/` directory:

| Schema File | Namespace | Purpose |
|-------------|-----------|---------|
| `dc-library.xsd` | `https://code.dccouncil.us/schemas/dc-library` | Main library schema |
| `codified.xsd` | `https://code.dccouncil.us/schemas/codified` | Codification attributes |
| `codify.xsd` | `https://code.dccouncil.us/schemas/codify` | Codification operations |
| `annotation-types.xsd` | N/A | Annotation type definitions |
| `XInclude.xsd` | `http://www.w3.org/2001/XInclude` | XInclude support |

### Key XML Elements

| Element | Description | Example |
|---------|-------------|---------|
| `<library>` | Root element for the entire law library | Top-level container |
| `<document>` | A complete legal document (law or code section) | A single statute |
| `<container>` | Organizational unit (Title, Chapter, Subchapter) | Title 47, Chapter 18 |
| `<section>` | A code section with legal content | Section 47-1806.03 |
| `<para>` | Paragraph within a section | Subsection (a)(1) |
| `<num>` | Section/paragraph number | "47-1806.03" or "(a)" |
| `<heading>` | Title of section or container | "Tax on residents..." |
| `<text>` | Prose content | Legal text |
| `<table>` | Tabular data (tax brackets, etc.) | Rate tables |
| `<annotation>` | Editorial notes, history, references | Amendment history |
| `<meta>` | Metadata (effective dates, citations) | Dates, law numbers |

### Hierarchical Structure

```
library
  |-- collection (dclaws, fedlaws)
  |     |-- period (Council period)
  |           |-- document (individual laws)
  |-- document (DC Code)
        |-- container (Title)
              |-- container (Chapter)
                    |-- container (Subchapter) [optional]
                          |-- section
                                |-- num
                                |-- heading
                                |-- para
                                      |-- num
                                      |-- text
                                      |-- para (nested)
```

## Code Organization

### Directory Structure

```
us/dc/council/code/
  |-- index.xml          (Code table of contents with XIncludes)
  |-- titles/
        |-- 1/           (Title 1)
        |     |-- index.xml
        |     |-- sections/
        |           |-- 1-101.xml
        |           |-- 1-102.xml
        |-- ...
        |-- 47/          (Title 47 - Taxation)
              |-- index.xml
              |-- sections/
                    |-- 47-1801.01.xml  (Definitions)
                    |-- 47-1806.03.xml  (Tax rates)
                    |-- 47-1806.04.xml  (Credits including EITC)
```

### Title 47: Taxation, Licensing, Permits, Assessments, and Fees

Title 47 contains all DC tax law. Key chapters for individual income tax:

| Chapter | Sections | Description |
|---------|----------|-------------|
| 18 | 47-1801.xx - 47-1819.xx | Income and Franchise Taxes |

#### Chapter 18 Subchapters

| Subchapter | Sections | Description |
|------------|----------|-------------|
| I | 47-1801.01 - 47-1801.05 | Definitions |
| II | 47-1802.01 - 47-1802.04 | Jurisdiction |
| III | 47-1803.01 - 47-1803.03 | Gross Income |
| IV | 47-1804.01 - 47-1804.07 | Net Income |
| V | 47-1805.01 - 47-1805.05 | Partnerships |
| VI | 47-1806.01 - 47-1806.12 | **Tax on Residents and Nonresidents** |
| VII | 47-1807.01 - 47-1807.11 | Corporations |
| VIII | 47-1808.01 - 47-1808.11 | Unincorporated Businesses |

## Key Tax Sections for RuleSpec

### Individual Income Tax

| Section | Description | RuleSpec Concept |
|---------|-------------|------------------|
| 47-1801.04(26) | Standard deduction definition | `dc_standard_deduction` parameter |
| 47-1801.04(43) | Personal exemption definition | `dc_personal_exemption` parameter |
| 47-1806.02 | Personal exemptions | `dc_personal_exemptions` variable |
| 47-1806.03 | **Tax rates (brackets)** | `dc_income_tax_brackets` parameter |
| 47-1806.04(f) | **DC EITC (40% of federal)** | `dc_eitc` variable |
| 47-1806.04(e) | Low income credit | `dc_low_income_credit` variable |
| 47-1806.04(c) | Child care credit | `dc_child_care_credit` variable |

### Current Tax Rates (Section 47-1806.03)

As of taxable years beginning after December 31, 2015 (paragraph 10):

**For taxable income (all filers):**

| Bracket | Rate | Base Tax |
|---------|------|----------|
| $0 - $10,000 | 4% | $0 |
| $10,000 - $40,000 | 6% | $400 |
| $40,000 - $60,000 | 6.5% (subject to funding) | $2,200 |
| $60,000 - $350,000 | 8.5% | $3,500 |
| $350,000 - $1,000,000 | 8.75% | $28,150 |
| Over $1,000,000 | 8.95% | $85,025 |

Note: DC uses a single rate schedule for all filers (no separate married/single brackets).

### DC EITC (Section 47-1806.04(f))

- **With qualifying child**: 40% of federal EITC
- **Without qualifying child**: Custom calculation in subsection (f)(1)(C)
  - Uses modified phase-in and phase-out from federal
  - Phaseout percentage: 8.48%
  - Phaseout amount: $17,235 (adjusted for cost-of-living)
- **Refundable**: Yes

### Standard Deduction (Section 47-1801.04(26))

For taxable years beginning after December 31, 2014:

| Filing Status | Base Amount |
|--------------|-------------|
| Single | $5,200 (adjusted for COLA) |
| Head of Household | $6,500 (adjusted for COLA) |
| Married Filing Jointly / Surviving Spouse | $8,350 (adjusted for COLA) |
| Married Filing Separately | Half of single amount |

## Parsing XML for Tax Provisions

### Sample XPath Queries

```python
# Get all sections in Title 47
//container[@num='47']//section

# Get income tax rate table
//section[@num='47-1806.03']//table

# Get EITC provisions
//section[@num='47-1806.04']//para[@num='(f)']

# Get standard deduction definition
//section[@num='47-1801.04']//para[@num='(26)']
```

### Extracting Values from Tables

DC tax rate tables are embedded in `<table>` elements:

```xml
<table>
  <tbody>
    <tr>
      <td>Not over $10,000</td>
      <td>4% of the taxable income.</td>
    </tr>
    <tr>
      <td>Over $10,000 but not over $40,000</td>
      <td>$400, plus 6% of the excess over $10,000.</td>
    </tr>
    <!-- ... -->
  </tbody>
</table>
```

### Parsing Bracket Text

The rate description follows a pattern:
- **Base amount**: "$X, plus"
- **Rate**: "Y% of the excess over"
- **Threshold**: "$Z"

Regex pattern:
```python
r'\$?([\d,]+)(?:,\s*plus)?\s*([\d.]+)%\s*(?:of the (?:taxable income|excess over \$?([\d,]+)))?'
```

## Mapping to RuleSpec Concepts

### XML Element to RuleSpec Mapping

| DC XML | RuleSpec | Notes |
|--------|----------|-------|
| `<section>/@num` | Variable/Parameter name | e.g., "47-1806.03" -> `dc_income_tax_rates` |
| `<table>` in rate section | Bracket parameter | Parse to threshold/rate arrays |
| `<meta>/<effective>` | Parameter effective dates | When law took effect |
| `<annotation type="History">` | Reference metadata | Legislative history |
| `<cite path="...">` | Cross-references | Links between sections |

### Example RuleSpec Parameter

From Section 47-1806.03(10):

```yaml
# parameters/dc/income_tax/brackets.yaml
dc_income_tax_brackets:
  description: DC individual income tax rate brackets
  reference:
    - title: "D.C. Code Section 47-1806.03(a)(10)"
      href: "https://code.dccouncil.us/us/dc/council/code/sections/47-1806.03.html"
  values:
    2016-01-01:
      - threshold: 0
        rate: 0.04
        base: 0
      - threshold: 10_000
        rate: 0.06
        base: 400
      - threshold: 40_000
        rate: 0.065  # subject to funding
        base: 2_200
      - threshold: 60_000
        rate: 0.085
        base: 3_500
      - threshold: 350_000
        rate: 0.0875
        base: 28_150
      - threshold: 1_000_000
        rate: 0.0895
        base: 85_025
```

### Example RuleSpec Variable

From Section 47-1806.04(f):

```python
# variables/dc/credits/eitc.py
class dc_eitc(Variable):
    """DC Earned Income Tax Credit"""
    value_type = float
    entity = TaxUnit
    definition_period = YEAR
    reference = "https://code.dccouncil.us/us/dc/council/code/sections/47-1806.04.html#(f)"

    def formula(tax_unit, period, parameters):
        federal_eitc = tax_unit("eitc", period)
        has_qualifying_child = tax_unit.any(
            tax_unit.members("is_eitc_qualifying_child", period)
        )

        # With qualifying child: 40% of federal EITC
        with_child = federal_eitc * 0.40

        # Without qualifying child: custom calculation
        # (See subsection (f)(1)(C) for details)
        without_child = calculate_childless_dc_eitc(tax_unit, period, parameters)

        return where(has_qualifying_child, with_child, without_child)
```

## Import Strategy

### Phase 1: Core Tax Provisions

1. Extract Title 47 sections
2. Parse income tax rate tables
3. Extract EITC percentages
4. Extract standard deduction amounts
5. Generate RuleSpec parameter files

### Phase 2: Credits and Deductions

1. Low income credit (47-1806.04(e))
2. Child care credit (47-1806.04(c))
3. Personal exemptions (47-1806.02)
4. Itemized deduction limitations

### Phase 3: Full Code Import

1. All Title 47 sections
2. Historical rate changes
3. Amendment tracking
4. Cross-reference resolution

## Using the Extraction Script

```bash
# Extract DC tax provisions
python scripts/extract_dc_tax.py

# Output files will be in:
# - output/dc/brackets.yaml
# - output/dc/eitc.yaml
# - output/dc/standard_deduction.yaml
```

## References

- **DC Code Online**: https://code.dccouncil.us/
- **DC Law XML GitHub**: https://github.com/DCCouncil/dc-law-xml
- **DC Law HTML GitHub**: https://github.com/DCCouncil/law-html
- **Schema Documentation**: See `schemas/` directory in repository
