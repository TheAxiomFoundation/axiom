"""Verification tool: Compare DSL encodings against PolicyEngine.

Similar to policyengine-taxsim validation - run test cases through both
our DSL encodings and PolicyEngine's Python package, compare results.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console
from rich.table import Table

console = Console()

# Try to import policyengine-us, fall back to API if not available
try:
    from policyengine_us import Simulation

    USE_PACKAGE = True  # pragma: no cover
except ImportError:  # pragma: no cover
    USE_PACKAGE = False
    POLICYENGINE_API_URL = "https://api.policyengine.org/us/calculate"


@dataclass
class TestCase:
    """A single test case from the DSL encoding."""

    name: str
    inputs: dict[str, Any]
    expected: dict[str, float]


@dataclass
class ComparisonResult:
    """Result of comparing one test case."""

    test_case: TestCase
    policyengine_value: float | None
    expected_value: float
    variable_name: str
    matches: bool
    difference: float
    error: str | None = None


@dataclass
class VerificationReport:
    """Full verification report for a section encoding."""

    citation: str
    timestamp: datetime
    test_cases: list[TestCase]
    results: list[ComparisonResult]
    policyengine_variable: str

    @property
    def match_count(self) -> int:
        return sum(1 for r in self.results if r.matches)

    @property
    def total_count(self) -> int:
        return len(self.results)

    @property
    def match_percentage(self) -> float:
        if self.total_count == 0:
            return 0.0
        return (self.match_count / self.total_count) * 100

    @property
    def passed(self) -> bool:
        return self.match_count == self.total_count


# Mapping from DSL test input names to PolicyEngine variable names
INPUT_MAPPINGS = {
    "adjusted_gross_income": "adjusted_gross_income",
    "earned_income": "earned_income",
    "filing_status": "filing_status",
    "eitc_qualifying_children_count": "eitc_child_count",
    "age_at_end_of_year": "age",
    "disqualified_income": "eitc_relevant_investment_income",
    "has_required_identification": "meets_eitc_identification_requirements",
    "months_principal_place_abode_us": "months_in_us",
    "taxable_year_months": None,  # Not a PE variable
    "not_dependent_of_another": None,  # Derived from other vars
    "is_married": "is_married",
    "separated_from_spouse_6_months": None,  # Complex eligibility
    "taxable_year_closed_by_death": None,  # Edge case
}

# Mapping from DSL expected output to PolicyEngine variable
OUTPUT_MAPPINGS = {
    "statute/26/32/earned_income_credit": "eitc",
    "statute/26/24/child_tax_credit": "ctc",
    "statute/26/63/taxable_income": "taxable_income",
    "statute/26/1/income_tax": "income_tax",
}

# Filing status enum conversion
FILING_STATUS_MAP = {
    "SINGLE": "SINGLE",
    "JOINT": "JOINT",
    "HEAD_OF_HOUSEHOLD": "HEAD_OF_HOUSEHOLD",
    "SEPARATE": "SEPARATE",
    "WIDOW": "SURVIVING_SPOUSE",
}


def load_test_cases(tests_path: Path) -> list[TestCase]:
    """Load test cases from a tests.yaml file."""
    if not tests_path.exists():
        return []

    content = yaml.safe_load(tests_path.read_text())

    test_cases = []
    # Handle both flat list and nested format
    if isinstance(content, list):
        for item in content:
            if "test_cases" in item:
                for tc in item["test_cases"]:
                    test_cases.append(
                        TestCase(
                            name=tc.get("name", "unnamed"),
                            inputs=tc.get("input", {}),
                            expected=tc.get("expected", {}),
                        )
                    )
            elif "input" in item:
                test_cases.append(
                    TestCase(
                        name=item.get("name", "unnamed"),
                        inputs=item.get("input", {}),
                        expected=item.get("expected", {}),
                    )
                )

    return test_cases


def build_policyengine_situation(test_case: TestCase, year: int = 2024) -> dict:
    """Convert DSL test inputs to PolicyEngine API situation format."""
    # Start with basic household structure
    situation = {
        "people": {
            "adult": {
                "age": {str(year): 30},  # Default
            }
        },
        "tax_units": {
            "tax_unit": {
                "members": ["adult"],
            }
        },
        "spm_units": {
            "spm_unit": {
                "members": ["adult"],
            }
        },
        "households": {
            "household": {
                "members": ["adult"],
                "state_name": {str(year): "CA"},  # Default state
            }
        },
        "families": {
            "family": {
                "members": ["adult"],
            }
        },
        "marital_units": {
            "marital_unit": {
                "members": ["adult"],
            }
        },
    }

    inputs = test_case.inputs

    # Handle age
    if "age_at_end_of_year" in inputs:
        situation["people"]["adult"]["age"] = {str(year): inputs["age_at_end_of_year"]}

    # Handle earned income (split into employment income for PE)
    if "earned_income" in inputs:
        situation["people"]["adult"]["employment_income"] = {str(year): inputs["earned_income"]}

    # Handle filing status - affects household structure
    filing_status = inputs.get("filing_status", "SINGLE")
    if filing_status in ["JOINT"]:
        # Add spouse
        situation["people"]["spouse"] = {
            "age": {str(year): 30},
        }
        situation["tax_units"]["tax_unit"]["members"].append("spouse")
        situation["spm_units"]["spm_unit"]["members"].append("spouse")
        situation["households"]["household"]["members"].append("spouse")
        situation["families"]["family"]["members"].append("spouse")
        situation["marital_units"]["marital_unit"]["members"].append("spouse")

    # Handle children for EITC
    num_children = inputs.get("eitc_qualifying_children_count", 0)
    for i in range(num_children):
        child_id = f"child_{i}"
        situation["people"][child_id] = {
            "age": {str(year): 5},  # Young child
            "is_tax_unit_dependent": {str(year): True},
        }
        situation["tax_units"]["tax_unit"]["members"].append(child_id)
        situation["spm_units"]["spm_unit"]["members"].append(child_id)
        situation["households"]["household"]["members"].append(child_id)
        situation["families"]["family"]["members"].append(child_id)

    # Investment income (for EITC disqualification)
    if "disqualified_income" in inputs:
        situation["people"]["adult"]["taxable_interest_income"] = {
            str(year): inputs["disqualified_income"]
        }

    return situation


def call_policyengine(
    situation: dict, output_variable: str, year: int = 2024
) -> tuple[float | None, str | None]:
    """Calculate a variable using PolicyEngine.

    Uses the Python package if available (faster, offline), otherwise
    falls back to the web API.
    """
    if USE_PACKAGE:
        return _call_policyengine_package(situation, output_variable, year)
    else:
        return _call_policyengine_api(situation, output_variable, year)


def _call_policyengine_package(
    situation: dict, output_variable: str, year: int = 2024
) -> tuple[float | None, str | None]:
    """Calculate using policyengine-us Python package."""
    try:
        sim = Simulation(situation=situation)
        value = sim.calculate(output_variable, year)
        # Handle array output (sum for tax unit level)
        if hasattr(value, "__len__") and len(value) > 0:  # pragma: no cover
            return float(value[0]), None
        return float(value), None
    except Exception as e:
        return None, str(e)


def _call_policyengine_api(
    situation: dict, output_variable: str, year: int = 2024
) -> tuple[float | None, str | None]:
    """Call PolicyEngine web API (fallback when package not installed).

    Note: PolicyEngine API requires you to request output variables by
    including them in the situation with a null value.
    """
    import requests

    # Add the output variable to the tax_unit with null value to request it
    if "tax_units" in situation and "tax_unit" in situation["tax_units"]:
        situation["tax_units"]["tax_unit"][output_variable] = {str(year): None}

    payload = {
        "household": situation,
    }

    try:
        response = requests.post(
            POLICYENGINE_API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()

        # Extract value from response
        if "result" in result:
            tax_units = result["result"].get("tax_units", {})
            if "tax_unit" in tax_units:
                var_data = tax_units["tax_unit"].get(output_variable, {})
                if str(year) in var_data:
                    return var_data[str(year)], None

        return None, f"Variable {output_variable} not found in response"

    except requests.exceptions.RequestException as e:
        return None, str(e)
    except (KeyError, TypeError) as e:  # pragma: no cover
        return None, f"Failed to parse response: {e}"


def verify_encoding(
    section_dir: Path, pe_variable: str, tolerance: float = 15.0
) -> VerificationReport:
    """Verify a DSL encoding against PolicyEngine.

    Args:
        section_dir: Directory containing rules.yaml and tests.yaml
        pe_variable: PolicyEngine variable name to compare (e.g., "eitc")
        tolerance: Dollar tolerance for matching (default $15)

    Returns:
        VerificationReport with comparison results
    """
    tests_path = section_dir / "tests.yaml"
    metadata_path = section_dir / "metadata.json"

    # Get citation from metadata
    citation = "Unknown"
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text())
        citation = metadata.get("citation", citation)

    # Load test cases
    test_cases = load_test_cases(tests_path)

    results = []
    for tc in test_cases:
        # Find expected output variable
        expected_value = None
        expected_var = None
        for var, value in tc.expected.items():
            expected_var = var
            expected_value = value
            break

        if expected_value is None:
            continue

        # Build PE situation
        situation = build_policyengine_situation(tc)

        # Call PolicyEngine
        pe_value, error = call_policyengine(situation, pe_variable)

        # Compare
        if pe_value is not None and expected_value is not None:
            diff = abs(pe_value - expected_value)
            matches = diff <= tolerance
        else:
            diff = float("inf")
            matches = False

        results.append(
            ComparisonResult(
                test_case=tc,
                policyengine_value=pe_value,
                expected_value=expected_value,
                variable_name=expected_var or pe_variable,
                matches=matches,
                difference=diff,
                error=error,
            )
        )

    return VerificationReport(
        citation=citation,
        timestamp=datetime.now(),
        test_cases=test_cases,
        results=results,
        policyengine_variable=pe_variable,
    )


def print_verification_report(report: VerificationReport):
    """Print a formatted verification report."""
    console.print()
    console.print(f"[bold]Verification Report: {report.citation}[/bold]")
    mode = "policyengine-us package" if USE_PACKAGE else "PolicyEngine API"
    console.print(
        f"[dim]Compared against PolicyEngine variable: {report.policyengine_variable} (via {mode})[/dim]"
    )
    console.print(f"[dim]Timestamp: {report.timestamp.isoformat()}[/dim]")
    console.print()

    # Summary
    if report.passed:
        console.print(
            f"[green]✓ PASSED[/green] {report.match_count}/{report.total_count} "
            f"test cases match ({report.match_percentage:.1f}%)"
        )
    else:
        console.print(
            f"[red]✗ FAILED[/red] {report.match_count}/{report.total_count} "
            f"test cases match ({report.match_percentage:.1f}%)"
        )
    console.print()

    # Results table
    table = Table(title="Test Case Results")
    table.add_column("Test Case", style="cyan")
    table.add_column("Expected", justify="right")
    table.add_column("PolicyEngine", justify="right")
    table.add_column("Diff", justify="right")
    table.add_column("Status")

    for r in report.results:
        pe_str = f"${r.policyengine_value:,.0f}" if r.policyengine_value else "ERROR"
        exp_str = f"${r.expected_value:,.0f}"
        diff_str = f"${r.difference:,.0f}" if r.difference != float("inf") else "N/A"

        if r.error:
            status = f"[red]✗ {r.error[:30]}[/red]"
        elif r.matches:
            status = "[green]✓ Match[/green]"
        else:
            status = "[red]✗ Mismatch[/red]"

        table.add_row(
            r.test_case.name[:40],
            exp_str,
            pe_str,
            diff_str,
            status,
        )

    console.print(table)


def save_verification_report(report: VerificationReport, output_path: Path):
    """Save verification report to JSON."""
    data = {
        "citation": report.citation,
        "timestamp": report.timestamp.isoformat(),
        "policyengine_variable": report.policyengine_variable,
        "summary": {
            "total_tests": report.total_count,
            "matches": report.match_count,
            "match_percentage": report.match_percentage,
            "passed": report.passed,
        },
        "results": [
            {
                "test_name": r.test_case.name,
                "expected": r.expected_value,
                "policyengine": r.policyengine_value,
                "difference": r.difference if r.difference != float("inf") else None,
                "matches": r.matches,
                "error": r.error,
            }
            for r in report.results
        ],
    }

    output_path.write_text(json.dumps(data, indent=2))
