"""Tests for the verifier module.

Tests cover test case loading, situation building, PolicyEngine calls,
verification reports, and report output/saving.
"""

import json
from datetime import datetime
from unittest.mock import patch

from axiom.verifier import (
    FILING_STATUS_MAP,
    INPUT_MAPPINGS,
    OUTPUT_MAPPINGS,
    ComparisonResult,
    TestCase,
    VerificationReport,
    build_policyengine_situation,
    load_test_cases,
    print_verification_report,
    save_verification_report,
    verify_encoding,
)

# =============================================================================
# Dataclass Tests
# =============================================================================


class TestTestCase:
    def test_create(self):
        tc = TestCase(
            name="basic",
            inputs={"earned_income": 10000},
            expected={"eitc": 500},
        )
        assert tc.name == "basic"
        assert tc.inputs["earned_income"] == 10000


class TestComparisonResult:
    def test_create_match(self):
        tc = TestCase(name="test1", inputs={}, expected={"eitc": 500})
        result = ComparisonResult(
            test_case=tc,
            policyengine_value=510.0,
            expected_value=500.0,
            variable_name="eitc",
            matches=True,
            difference=10.0,
        )
        assert result.matches is True
        assert result.error is None

    def test_create_with_error(self):
        tc = TestCase(name="test1", inputs={}, expected={})
        result = ComparisonResult(
            test_case=tc,
            policyengine_value=None,
            expected_value=500.0,
            variable_name="eitc",
            matches=False,
            difference=float("inf"),
            error="API error",
        )
        assert result.error == "API error"


class TestVerificationReport:
    def _make_report(self, match_count=2, total=3):
        tc = TestCase(name="test", inputs={}, expected={"eitc": 100})
        results = []
        for i in range(total):
            results.append(
                ComparisonResult(
                    test_case=tc,
                    policyengine_value=100.0 if i < match_count else 999.0,
                    expected_value=100.0,
                    variable_name="eitc",
                    matches=i < match_count,
                    difference=0.0 if i < match_count else 899.0,
                )
            )
        return VerificationReport(
            citation="26 USC 32",
            timestamp=datetime(2024, 1, 1),
            test_cases=[tc] * total,
            results=results,
            policyengine_variable="eitc",
        )

    def test_match_count(self):
        report = self._make_report(match_count=2, total=3)
        assert report.match_count == 2

    def test_total_count(self):
        report = self._make_report(match_count=2, total=3)
        assert report.total_count == 3

    def test_match_percentage(self):
        report = self._make_report(match_count=2, total=4)
        assert report.match_percentage == 50.0

    def test_match_percentage_zero(self):
        report = self._make_report(match_count=0, total=0)
        assert report.match_percentage == 0.0

    def test_passed_true(self):
        report = self._make_report(match_count=3, total=3)
        assert report.passed is True

    def test_passed_false(self):
        report = self._make_report(match_count=2, total=3)
        assert report.passed is False


# =============================================================================
# Mapping Tests
# =============================================================================


class TestMappings:
    def test_input_mappings(self):
        assert "adjusted_gross_income" in INPUT_MAPPINGS
        assert "earned_income" in INPUT_MAPPINGS
        assert "filing_status" in INPUT_MAPPINGS

    def test_output_mappings(self):
        assert "statute/26/32/earned_income_credit" in OUTPUT_MAPPINGS
        assert OUTPUT_MAPPINGS["statute/26/32/earned_income_credit"] == "eitc"

    def test_filing_status_map(self):
        assert FILING_STATUS_MAP["SINGLE"] == "SINGLE"
        assert FILING_STATUS_MAP["WIDOW"] == "SURVIVING_SPOUSE"


# =============================================================================
# Load Test Cases
# =============================================================================


class TestLoadTestCases:
    def test_nonexistent_file(self, tmp_path):
        result = load_test_cases(tmp_path / "nonexistent.yaml")
        assert result == []

    def test_flat_list_format(self, tmp_path):
        yaml_content = """
- name: test1
  input:
    earned_income: 10000
  expected:
    eitc: 500
- name: test2
  input:
    earned_income: 20000
  expected:
    eitc: 1000
"""
        yaml_file = tmp_path / "tests.yaml"
        yaml_file.write_text(yaml_content)
        result = load_test_cases(yaml_file)
        assert len(result) == 2
        assert result[0].name == "test1"
        assert result[0].inputs["earned_income"] == 10000

    def test_nested_format(self, tmp_path):
        yaml_content = """
- test_cases:
    - name: test1
      input:
        earned_income: 10000
      expected:
        eitc: 500
    - name: test2
      input:
        earned_income: 20000
      expected:
        eitc: 1000
"""
        yaml_file = tmp_path / "tests.yaml"
        yaml_file.write_text(yaml_content)
        result = load_test_cases(yaml_file)
        assert len(result) == 2

    def test_empty_yaml(self, tmp_path):
        yaml_file = tmp_path / "tests.yaml"
        yaml_file.write_text("")
        result = load_test_cases(yaml_file)
        assert result == []

    def test_non_list_yaml(self, tmp_path):
        yaml_file = tmp_path / "tests.yaml"
        yaml_file.write_text("key: value")
        result = load_test_cases(yaml_file)
        assert result == []

    def test_unnamed_test(self, tmp_path):
        yaml_content = """
- input:
    earned_income: 10000
  expected:
    eitc: 500
"""
        yaml_file = tmp_path / "tests.yaml"
        yaml_file.write_text(yaml_content)
        result = load_test_cases(yaml_file)
        assert result[0].name == "unnamed"


# =============================================================================
# Build PolicyEngine Situation
# =============================================================================


class TestBuildPolicyEngineSituation:
    def test_basic_single(self):
        tc = TestCase(
            name="basic",
            inputs={"earned_income": 10000},
            expected={"eitc": 500},
        )
        sit = build_policyengine_situation(tc)
        assert "people" in sit
        assert "adult" in sit["people"]
        assert sit["people"]["adult"]["employment_income"]["2024"] == 10000

    def test_with_age(self):
        tc = TestCase(
            name="age",
            inputs={"age_at_end_of_year": 25},
            expected={},
        )
        sit = build_policyengine_situation(tc, year=2024)
        assert sit["people"]["adult"]["age"]["2024"] == 25

    def test_joint_filing(self):
        tc = TestCase(
            name="joint",
            inputs={"filing_status": "JOINT"},
            expected={},
        )
        sit = build_policyengine_situation(tc)
        assert "spouse" in sit["people"]
        assert "spouse" in sit["tax_units"]["tax_unit"]["members"]
        assert "spouse" in sit["spm_units"]["spm_unit"]["members"]
        assert "spouse" in sit["households"]["household"]["members"]
        assert "spouse" in sit["families"]["family"]["members"]
        assert "spouse" in sit["marital_units"]["marital_unit"]["members"]

    def test_with_children(self):
        tc = TestCase(
            name="kids",
            inputs={"eitc_qualifying_children_count": 2},
            expected={},
        )
        sit = build_policyengine_situation(tc)
        assert "child_0" in sit["people"]
        assert "child_1" in sit["people"]
        assert sit["people"]["child_0"]["is_tax_unit_dependent"]["2024"] is True

    def test_with_disqualified_income(self):
        tc = TestCase(
            name="invest",
            inputs={"disqualified_income": 5000},
            expected={},
        )
        sit = build_policyengine_situation(tc)
        assert sit["people"]["adult"]["taxable_interest_income"]["2024"] == 5000

    def test_custom_year(self):
        tc = TestCase(
            name="custom_year",
            inputs={"earned_income": 10000},
            expected={},
        )
        sit = build_policyengine_situation(tc, year=2023)
        assert sit["people"]["adult"]["employment_income"]["2023"] == 10000


# =============================================================================
# Verify Encoding
# =============================================================================


class TestVerifyEncoding:
    @patch("axiom.verifier.call_policyengine")
    def test_verify_with_tests(self, mock_call, tmp_path):
        # Create test files
        tests_yaml = """
- name: test1
  input:
    earned_income: 10000
  expected:
    eitc: 500.0
"""
        (tmp_path / "tests.yaml").write_text(tests_yaml)
        metadata = {"citation": "26 USC 32"}
        (tmp_path / "metadata.json").write_text(json.dumps(metadata))

        mock_call.return_value = (510.0, None)

        report = verify_encoding(tmp_path, "eitc", tolerance=15.0)
        assert report.citation == "26 USC 32"
        assert report.total_count == 1
        assert report.results[0].matches is True

    @patch("axiom.verifier.call_policyengine")
    def test_verify_no_metadata(self, mock_call, tmp_path):
        tests_yaml = """
- name: test1
  input:
    earned_income: 10000
  expected:
    eitc: 500.0
"""
        (tmp_path / "tests.yaml").write_text(tests_yaml)
        mock_call.return_value = (500.0, None)

        report = verify_encoding(tmp_path, "eitc")
        assert report.citation == "Unknown"

    @patch("axiom.verifier.call_policyengine")
    def test_verify_api_error(self, mock_call, tmp_path):
        tests_yaml = """
- name: test1
  input:
    earned_income: 10000
  expected:
    eitc: 500.0
"""
        (tmp_path / "tests.yaml").write_text(tests_yaml)
        mock_call.return_value = (None, "API error")

        report = verify_encoding(tmp_path, "eitc")
        assert report.results[0].matches is False
        assert report.results[0].error == "API error"

    @patch("axiom.verifier.call_policyengine")
    def test_verify_empty_expected(self, mock_call, tmp_path):
        tests_yaml = """
- name: test1
  input:
    earned_income: 10000
  expected: {}
"""
        (tmp_path / "tests.yaml").write_text(tests_yaml)
        mock_call.return_value = (500.0, None)

        report = verify_encoding(tmp_path, "eitc")
        # Should skip test cases with no expected values
        assert report.total_count == 0


# =============================================================================
# Print Report
# =============================================================================


class TestPrintReport:
    def test_print_passed_report(self, capsys):
        tc = TestCase(name="test1", inputs={}, expected={"eitc": 100})
        result = ComparisonResult(
            test_case=tc,
            policyengine_value=100.0,
            expected_value=100.0,
            variable_name="eitc",
            matches=True,
            difference=0.0,
        )
        report = VerificationReport(
            citation="26 USC 32",
            timestamp=datetime(2024, 1, 1),
            test_cases=[tc],
            results=[result],
            policyengine_variable="eitc",
        )
        print_verification_report(report)
        # No assertions on captured output since Rich uses its own console

    def test_print_failed_report(self, capsys):
        tc = TestCase(name="test1", inputs={}, expected={"eitc": 100})
        result = ComparisonResult(
            test_case=tc,
            policyengine_value=999.0,
            expected_value=100.0,
            variable_name="eitc",
            matches=False,
            difference=899.0,
        )
        report = VerificationReport(
            citation="26 USC 32",
            timestamp=datetime(2024, 1, 1),
            test_cases=[tc],
            results=[result],
            policyengine_variable="eitc",
        )
        print_verification_report(report)

    def test_print_error_report(self, capsys):
        tc = TestCase(name="test1", inputs={}, expected={"eitc": 100})
        result = ComparisonResult(
            test_case=tc,
            policyengine_value=None,
            expected_value=100.0,
            variable_name="eitc",
            matches=False,
            difference=float("inf"),
            error="Connection error occurred during API call",
        )
        report = VerificationReport(
            citation="26 USC 32",
            timestamp=datetime(2024, 1, 1),
            test_cases=[tc],
            results=[result],
            policyengine_variable="eitc",
        )
        print_verification_report(report)


# =============================================================================
# Save Report
# =============================================================================


class TestSaveReport:
    def test_save_report(self, tmp_path):
        tc = TestCase(name="test1", inputs={}, expected={"eitc": 100})
        result = ComparisonResult(
            test_case=tc,
            policyengine_value=100.0,
            expected_value=100.0,
            variable_name="eitc",
            matches=True,
            difference=0.0,
        )
        report = VerificationReport(
            citation="26 USC 32",
            timestamp=datetime(2024, 1, 1),
            test_cases=[tc],
            results=[result],
            policyengine_variable="eitc",
        )
        out = tmp_path / "report.json"
        save_verification_report(report, out)

        data = json.loads(out.read_text())
        assert data["citation"] == "26 USC 32"
        assert data["summary"]["total_tests"] == 1
        assert data["summary"]["passed"] is True
        assert data["results"][0]["matches"] is True

    def test_save_report_with_inf_difference(self, tmp_path):
        tc = TestCase(name="test1", inputs={}, expected={"eitc": 100})
        result = ComparisonResult(
            test_case=tc,
            policyengine_value=None,
            expected_value=100.0,
            variable_name="eitc",
            matches=False,
            difference=float("inf"),
            error="error",
        )
        report = VerificationReport(
            citation="26 USC 32",
            timestamp=datetime(2024, 1, 1),
            test_cases=[tc],
            results=[result],
            policyengine_variable="eitc",
        )
        out = tmp_path / "report.json"
        save_verification_report(report, out)
        data = json.loads(out.read_text())
        assert data["results"][0]["difference"] is None
