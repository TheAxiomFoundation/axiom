"""Tests for the eCFR converter module.

Tests cover ECFRMetadata, FetchResult, PRIORITY_TITLES, and ECFRConverter init.
Network calls are NOT made.
"""

from datetime import date

from axiom.converters.ecfr import (
    ECFR_API_BASE,
    PRIORITY_TITLES,
    ECFRConverter,
    ECFRMetadata,
    FetchResult,
)


class TestConstants:
    def test_ecfr_api_base(self):
        assert "ecfr.gov" in ECFR_API_BASE
        assert "versioner" in ECFR_API_BASE

    def test_priority_titles(self):
        assert 26 in PRIORITY_TITLES  # IRS
        assert 7 in PRIORITY_TITLES  # SNAP
        assert 20 in PRIORITY_TITLES  # SSA
        assert 42 in PRIORITY_TITLES  # Medicare/Medicaid
        assert len(PRIORITY_TITLES) >= 5


class TestECFRMetadata:
    def test_create(self):
        meta = ECFRMetadata(title=26, name="Internal Revenue")
        assert meta.title == 26
        assert meta.name == "Internal Revenue"
        assert meta.latest_issue_date is None
        assert meta.part_count == 0

    def test_with_all_fields(self):
        meta = ECFRMetadata(
            title=26,
            name="Internal Revenue",
            latest_issue_date=date(2024, 1, 1),
            amendment_date=date(2024, 3, 15),
            part_count=10,
            section_count=500,
        )
        assert meta.section_count == 500


class TestFetchResult:
    def test_success(self):
        result = FetchResult(success=True)
        assert result.success is True
        assert result.citation is None
        assert result.error is None
        assert result.regulations == []

    def test_failure(self):
        result = FetchResult(success=False, error="Connection timeout")
        assert result.success is False
        assert result.error == "Connection timeout"


class TestECFRConverter:
    def test_init(self):
        converter = ECFRConverter()
        assert converter is not None
