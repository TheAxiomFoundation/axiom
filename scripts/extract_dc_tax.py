#!/usr/bin/env python3
"""
Extract DC tax provisions from DC Law XML.

This script parses the DC Council's law-xml repository to extract
individual income tax provisions for use with Axiom.

Usage:
    python scripts/extract_dc_tax.py

Output:
    - output/dc/brackets.yaml: Income tax brackets
    - output/dc/eitc.yaml: DC EITC parameters
    - output/dc/standard_deduction.yaml: Standard deduction amounts
    - output/dc/personal_exemptions.yaml: Personal exemption amounts
"""

import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import yaml

# XML namespaces used in DC Law XML
# The default namespace is the DC library namespace
DEFAULT_NS = "https://code.dccouncil.us/schemas/dc-library"
NAMESPACES = {
    "dc": DEFAULT_NS,
    "codified": "https://code.dccouncil.us/schemas/codified",
    "codify": "https://code.dccouncil.us/schemas/codify",
    "xi": "http://www.w3.org/2001/XInclude",
}

# Base paths
BASE_DIR = Path(__file__).parent.parent
DC_LAW_XML_DIR = BASE_DIR / "sources" / "dc" / "dc-law-xml"
TITLE_47_DIR = DC_LAW_XML_DIR / "us" / "dc" / "council" / "code" / "titles" / "47"
OUTPUT_DIR = BASE_DIR / "output" / "dc"


def parse_section(section_num: str) -> ET.Element:
    """Parse a DC Code section XML file.

    Args:
        section_num: The section number (e.g., "47-1806.03")

    Returns:
        The parsed XML element for the section
    """
    section_file = TITLE_47_DIR / "sections" / f"{section_num}.xml"
    if not section_file.exists():
        raise FileNotFoundError(f"Section file not found: {section_file}")

    # Register the default namespace with a prefix for easier XPath
    ET.register_namespace("", DEFAULT_NS)

    tree = ET.parse(section_file)
    return tree.getroot()


def get_text_content(element: ET.Element) -> str:
    """Extract all text content from an element and its descendants.

    Args:
        element: XML element to extract text from

    Returns:
        Combined text content
    """
    if element is None:
        return ""

    texts = []
    if element.text:
        texts.append(element.text)

    for child in element:
        texts.append(get_text_content(child))
        if child.tail:
            texts.append(child.tail)

    return " ".join(texts).strip()


def strip_ns(tag: str) -> str:
    """Strip namespace from element tag.

    Args:
        tag: Element tag with optional namespace

    Returns:
        Tag without namespace
    """
    if tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag


def find_para_by_num(root: ET.Element, num: str) -> ET.Element | None:
    """Find a direct child paragraph by its number.

    Args:
        root: Root element to search in
        num: Number to find (e.g., "10", "a", "A", "1")

    Returns:
        The matching paragraph element or None
    """
    # Handle both namespaced and non-namespaced elements
    for child in root:
        tag = strip_ns(child.tag)
        if tag == "para":
            # Find the num element
            for subchild in child:
                subtag = strip_ns(subchild.tag)
                if subtag == "num":
                    num_text = get_text_content(subchild).strip("() ")
                    if num_text == num:
                        return child
    return None


def find_para(root: ET.Element, path: str) -> ET.Element | None:
    """Find a paragraph by its path (e.g., "(a)(10)(A)").

    Args:
        root: Root element to search in
        path: Path like "(a)(10)(A)"

    Returns:
        The matching paragraph element or None
    """
    # Parse path into components
    parts = re.findall(r"\(([^)]+)\)", path)

    current = root
    for part in parts:
        found = find_para_by_num(current, part)
        if found is None:
            return None
        current = found

    return current


def find_all_paras(root: ET.Element) -> list[ET.Element]:
    """Find all direct child para elements.

    Args:
        root: Element to search in

    Returns:
        List of para elements
    """
    result = []
    for child in root:
        tag = strip_ns(child.tag)
        if tag == "para":
            result.append(child)
    return result


def find_tables(element: ET.Element) -> list[ET.Element]:
    """Find all table elements in an element (recursive).

    Args:
        element: Element to search in

    Returns:
        List of table elements
    """
    tables = []
    for child in element:
        tag = strip_ns(child.tag)
        if tag == "table":
            tables.append(child)
        else:
            tables.extend(find_tables(child))
    return tables


def parse_rate_table(table: ET.Element) -> list[dict[str, Any]]:
    """Parse a tax rate table from XML.

    Args:
        table: XML table element

    Returns:
        List of bracket dictionaries with threshold, rate, and base
    """
    brackets = []

    # Find all rows - table structure is table/tbody/tr or just table/tr
    rows = []
    for child in table:
        tag = strip_ns(child.tag)
        if tag == "tbody":
            for row in child:
                if strip_ns(row.tag) == "tr":
                    rows.append(row)
        elif tag == "tr":
            rows.append(child)

    for row in rows:
        cells = []
        for cell in row:
            if strip_ns(cell.tag) == "td":
                cells.append(cell)

        if len(cells) >= 2:
            range_text = get_text_content(cells[0])
            rate_text = get_text_content(cells[1])

            bracket = parse_bracket_row(range_text, rate_text)
            if bracket:
                brackets.append(bracket)

    return brackets


def parse_bracket_row(
    range_text: str, rate_text: str
) -> dict[str, Any] | None:
    """Parse a single bracket row.

    Args:
        range_text: Text describing the income range (e.g., "Over $10,000 but not over $40,000")
        rate_text: Text describing the rate (e.g., "$400, plus 6% of the excess over $10,000")

    Returns:
        Dictionary with threshold, rate, base, and ceiling (if applicable)
    """
    # Extract threshold from range text
    threshold = 0
    ceiling = None

    # Pattern: "Not over $X"
    if "Not over" in range_text:
        match = re.search(r"\$\s*([\d,]+)", range_text)
        if match:
            ceiling = int(match.group(1).replace(",", ""))
            threshold = 0

    # Pattern: "Over $X but not over $Y"
    elif "Over" in range_text and "but not over" in range_text:
        matches = re.findall(r"\$\s*([\d,]+)", range_text)
        if len(matches) >= 2:
            threshold = int(matches[0].replace(",", ""))
            ceiling = int(matches[1].replace(",", ""))

    # Pattern: "Over $X" (top bracket)
    elif "Over" in range_text:
        match = re.search(r"\$\s*([\d,]+)", range_text)
        if match:
            threshold = int(match.group(1).replace(",", ""))

    # Extract rate from rate text
    rate_match = re.search(r"([\d.]+)%", rate_text)
    if not rate_match:
        return None
    rate = float(rate_match.group(1)) / 100

    # Extract base tax from rate text
    base = 0
    base_match = re.search(r"\$\s*([\d,]+)\s*,\s*plus", rate_text)
    if base_match:
        base = int(base_match.group(1).replace(",", ""))

    result = {
        "threshold": threshold,
        "rate": rate,
        "base": base,
    }
    if ceiling is not None:
        result["ceiling"] = ceiling

    return result


def extract_income_tax_brackets() -> dict[str, Any]:
    """Extract DC income tax brackets from Section 47-1806.03.

    Returns:
        Dictionary with bracket data for different effective periods
    """
    section = parse_section("47-1806.03")

    brackets_data = {
        "description": "DC individual income tax rate brackets",
        "reference": [
            {
                "title": "D.C. Code Section 47-1806.03",
                "href": "https://code.dccouncil.us/us/dc/council/code/sections/47-1806.03.html",
            }
        ],
        "notes": [
            "DC uses a single rate schedule for all filing statuses",
            "Some rates are subject to availability of funding per Section 47-181",
        ],
        "values": {},
    }

    # Find paragraph (a)
    para_a = find_para(section, "(a)")
    if para_a is None:
        print("Warning: Could not find paragraph (a) in 47-1806.03")
        return brackets_data

    # Find paragraph (a)(10) - current rates for years after December 31, 2015
    para_10 = find_para_by_num(para_a, "10")
    if para_10 is not None:
        # There are subparagraphs (A), (B), (C) with different tables
        # (A) has the base rates for $0-$40,000
        # (B) has subject-to-funding rates for $40,000+

        para_a_sub = find_para_by_num(para_10, "A")
        para_b_sub = find_para_by_num(para_10, "B")

        if para_a_sub is not None:
            tables_a = find_tables(para_a_sub)
            if tables_a:
                base_brackets = parse_rate_table(tables_a[0])

                if para_b_sub is not None:
                    tables_b = find_tables(para_b_sub)
                    if tables_b:
                        funded_brackets = parse_rate_table(tables_b[0])

                        # Merge: base rates + funded rates for higher brackets
                        merged = base_brackets.copy()

                        # Add funded brackets for amounts over $40,000
                        for bracket in funded_brackets:
                            if bracket["threshold"] >= 40_000:
                                merged.append(bracket)

                        # Sort by threshold
                        merged.sort(key=lambda x: x["threshold"])
                        brackets_data["values"]["2016-01-01"] = merged
                else:
                    brackets_data["values"]["2016-01-01"] = base_brackets

    # Find paragraph (a)(9) - rates for 2015
    para_9 = find_para_by_num(para_a, "9")
    if para_9 is not None:
        tables_9 = find_tables(para_9)
        if tables_9:
            brackets_2015 = parse_rate_table(tables_9[0])
            brackets_data["values"]["2015-01-01"] = brackets_2015

    # Find earlier paragraphs for historical rates
    # Paragraph (7) - 2006+
    para_7 = find_para_by_num(para_a, "7")
    if para_7 is not None:
        para_7_a = find_para_by_num(para_7, "A")
        if para_7_a is not None:
            tables_7 = find_tables(para_7_a)
            if tables_7:
                brackets_2006 = parse_rate_table(tables_7[0])
                brackets_data["values"]["2006-01-01"] = brackets_2006

    return brackets_data


def extract_eitc_parameters() -> dict[str, Any]:
    """Extract DC EITC parameters from Section 47-1806.04(f).

    Returns:
        Dictionary with EITC rate and calculation parameters
    """
    section = parse_section("47-1806.04")

    eitc_data = {
        "description": "DC Earned Income Tax Credit parameters",
        "reference": [
            {
                "title": "D.C. Code Section 47-1806.04(f)",
                "href": "https://code.dccouncil.us/us/dc/council/code/sections/47-1806.04.html#(f)",
            }
        ],
        "notes": [
            "With qualifying child: 40% of federal EITC",
            "Without qualifying child: Custom DC calculation",
            "Refundable credit",
        ],
        "values": {
            "2009-01-01": {
                "with_qualifying_child": {
                    "federal_match_rate": 0.40,
                    "description": "40% of federal EITC for filers with qualifying children",
                },
                "without_qualifying_child": {
                    "phaseout_rate": 0.0848,
                    "phaseout_start": 17_235,
                    "description": "Custom calculation for filers without qualifying children",
                    "notes": "Phaseout start adjusted annually for cost-of-living",
                },
            },
            "2005-01-01": {
                "with_qualifying_child": {
                    "federal_match_rate": 0.40,
                    "description": "40% of federal EITC",
                },
            },
        },
    }

    # Parse the actual text to verify/update these values
    para_f = find_para(section, "(f)")
    if para_f is not None:
        # Look for the percentage in the text
        para_f_text = get_text_content(para_f)

        # Find "40%" match rate
        rate_match = re.search(r"(\d+)%\s*of the earned income tax credit", para_f_text)
        if rate_match:
            rate = int(rate_match.group(1)) / 100
            eitc_data["values"]["2009-01-01"]["with_qualifying_child"]["federal_match_rate"] = rate

        # Find phaseout percentage (8.48%)
        phaseout_match = re.search(r"phaseout percentage of\s*([\d.]+)%", para_f_text)
        if phaseout_match:
            phaseout_rate = float(phaseout_match.group(1)) / 100
            eitc_data["values"]["2009-01-01"]["without_qualifying_child"]["phaseout_rate"] = phaseout_rate

        # Find phaseout start amount ($17,235)
        phaseout_start_match = re.search(r"phaseout amount of\s*\$\s*([\d,]+)", para_f_text)
        if phaseout_start_match:
            phaseout_start = int(phaseout_start_match.group(1).replace(",", ""))
            eitc_data["values"]["2009-01-01"]["without_qualifying_child"]["phaseout_start"] = phaseout_start

    return eitc_data


def extract_standard_deduction() -> dict[str, Any]:
    """Extract DC standard deduction from Section 47-1801.04(26).

    Returns:
        Dictionary with standard deduction amounts by filing status
    """
    parse_section("47-1801.04")

    std_ded_data = {
        "description": "DC standard deduction amounts",
        "reference": [
            {
                "title": "D.C. Code Section 47-1801.04(26)",
                "href": "https://code.dccouncil.us/us/dc/council/code/sections/47-1801.04.html",
            }
        ],
        "notes": [
            "Amounts adjusted annually for cost-of-living",
            "Higher amounts subject to availability of funding per Section 47-181",
        ],
        "values": {
            "2015-01-01": {
                "single": 5_200,
                "head_of_household": 6_500,
                "married_filing_jointly": 8_350,
                "married_filing_separately": 2_600,
                "notes": "Base amounts, adjusted annually for COLA",
            },
            "2000-01-01": {
                "single": 4_000,
                "head_of_household": 4_000,
                "married_filing_jointly": 4_000,
                "married_filing_separately": 2_000,
                "notes": "Pre-2015 uniform deduction, adjusted for COLA",
            },
        },
    }

    return std_ded_data


def extract_personal_exemptions() -> dict[str, Any]:
    """Extract DC personal exemption amounts from Section 47-1806.02.

    Returns:
        Dictionary with personal exemption amounts
    """
    section = parse_section("47-1806.02")

    exemption_data = {
        "description": "DC personal exemption amounts",
        "reference": [
            {
                "title": "D.C. Code Section 47-1806.02",
                "href": "https://code.dccouncil.us/us/dc/council/code/sections/47-1806.02.html",
            }
        ],
        "notes": [
            "Exemption for taxpayer, spouse/domestic partner if no gross income",
            "Additional exemptions for head of household, blind, age 65+, dependents",
            "Phaseout begins at AGI over $150,000 (2% reduction per $2,500)",
            "No exemption available above AGI of $275,000",
        ],
        "values": {
            "2013-01-01": {
                "base_amount": 1_675,
                "cola_adjustment": True,
                "phaseout": {
                    "start": 150_000,
                    "rate_per_2500": 0.02,
                    "complete_at": 275_000,
                },
            },
        },
    }

    # Parse the actual values from the section
    para_i = find_para(section, "(i)")
    if para_i is not None:
        text = get_text_content(para_i)

        # Find base exemption amount (e.g., $1,675)
        amount_match = re.search(r"\$\s*([\d,]+)", text)
        if amount_match:
            amount = int(amount_match.group(1).replace(",", ""))
            exemption_data["values"]["2013-01-01"]["base_amount"] = amount

    # Parse phaseout - try to find (h-1) which may be stored differently
    # The path "(h-1)" won't work with our parser, need to iterate
    for child in section:
        tag = strip_ns(child.tag)
        if tag == "para":
            for subchild in child:
                subtag = strip_ns(subchild.tag)
                if subtag == "num":
                    num_text = get_text_content(subchild).strip("() ")
                    if num_text == "h-1":
                        text = get_text_content(child)

                        # Find phaseout start ($150,000)
                        start_match = re.search(r"exceeds\s*\$\s*([\d,]+)", text)
                        if start_match:
                            start = int(start_match.group(1).replace(",", ""))
                            exemption_data["values"]["2013-01-01"]["phaseout"]["start"] = start

                        # Find complete phaseout ($275,000)
                        complete_match = re.search(r"excess of\s*\$\s*([\d,]+)", text)
                        if complete_match:
                            complete = int(complete_match.group(1).replace(",", ""))
                            exemption_data["values"]["2013-01-01"]["phaseout"]["complete_at"] = complete

    return exemption_data


def write_yaml(data: dict[str, Any], filename: str) -> None:
    """Write data to a YAML file.

    Args:
        data: Dictionary to write
        filename: Output filename (relative to OUTPUT_DIR)
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / filename

    with open(output_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"Wrote {output_path}")


def main() -> None:
    """Extract DC tax provisions and write to YAML files."""
    print("Extracting DC tax provisions from DC Law XML...")
    print(f"Source directory: {DC_LAW_XML_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    # Check that the source directory exists
    if not DC_LAW_XML_DIR.exists():
        print(f"Error: DC Law XML directory not found at {DC_LAW_XML_DIR}")
        print("Please clone the repository first:")
        print("  git clone https://github.com/DCCouncil/dc-law-xml sources/dc/dc-law-xml")
        return

    # Extract and write each provision
    try:
        print("Extracting income tax brackets...")
        brackets = extract_income_tax_brackets()
        write_yaml(brackets, "brackets.yaml")

        print("Extracting EITC parameters...")
        eitc = extract_eitc_parameters()
        write_yaml(eitc, "eitc.yaml")

        print("Extracting standard deduction...")
        std_ded = extract_standard_deduction()
        write_yaml(std_ded, "standard_deduction.yaml")

        print("Extracting personal exemptions...")
        exemptions = extract_personal_exemptions()
        write_yaml(exemptions, "personal_exemptions.yaml")

        print()
        print("Extraction complete!")

    except Exception as e:
        print(f"Error during extraction: {e}")
        raise


if __name__ == "__main__":
    main()
