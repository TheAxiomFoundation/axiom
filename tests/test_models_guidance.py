"""Tests for the guidance models module."""

from datetime import date

from axiom.models_guidance import (
    GuidanceSearchResult,
    GuidanceSection,
    GuidanceType,
    RevenueProcedure,
)


class TestGuidanceType:
    def test_values(self):
        assert GuidanceType.REV_PROC == "revenue_procedure"
        assert GuidanceType.REV_RUL == "revenue_ruling"
        assert GuidanceType.NOTICE == "notice"
        assert GuidanceType.ANNOUNCEMENT == "announcement"


class TestGuidanceSection:
    def test_create(self):
        section = GuidanceSection(
            section_num=".01",
            heading="Purpose",
            text="This procedure provides...",
        )
        assert section.section_num == ".01"
        assert section.heading == "Purpose"

    def test_no_heading(self):
        section = GuidanceSection(section_num="1", text="Text content")
        assert section.heading is None

    def test_with_children(self):
        child = GuidanceSection(section_num=".01", text="Child")
        parent = GuidanceSection(
            section_num="3",
            text="Parent",
            children=[child],
        )
        assert len(parent.children) == 1


class TestRevenueProcedure:
    def _make_rev_proc(self, **kwargs):
        defaults = {
            "doc_number": "2023-34",
            "doc_type": GuidanceType.REV_PROC,
            "title": "Inflation Adjustments Under Section 1(f)",
            "irb_citation": "2023-48 IRB",
            "published_date": date(2023, 11, 9),
            "full_text": "This procedure provides inflation adjustments...",
            "source_url": "https://www.irs.gov/irb/2023-48_IRB#RP-2023-34",
            "retrieved_at": date(2024, 1, 15),
        }
        defaults.update(kwargs)
        return RevenueProcedure(**defaults)

    def test_create(self):
        rp = self._make_rev_proc()
        assert rp.doc_number == "2023-34"
        assert rp.doc_type == GuidanceType.REV_PROC

    def test_path(self):
        rp = self._make_rev_proc()
        assert rp.path == "us/guidance/irs/rp-2023-34"

    def test_optional_fields(self):
        rp = self._make_rev_proc(
            effective_date=date(2024, 1, 1),
            tax_years=[2024],
            subject_areas=["EITC", "Income Tax"],
            parameters={"eitc_max": {"value": 7830}},
            pdf_url="https://www.irs.gov/pub/irs-irbs/irb23-48.pdf",
        )
        assert rp.effective_date == date(2024, 1, 1)
        assert rp.tax_years == [2024]
        assert len(rp.subject_areas) == 2

    def test_with_sections(self):
        sections = [
            GuidanceSection(section_num=".01", text="Purpose"),
            GuidanceSection(section_num=".02", text="Scope"),
        ]
        rp = self._make_rev_proc(sections=sections)
        assert len(rp.sections) == 2


class TestGuidanceSearchResult:
    def test_create(self):
        result = GuidanceSearchResult(
            doc_number="2023-34",
            doc_type=GuidanceType.REV_PROC,
            title="Inflation Adjustments",
            snippet="earned income credit amounts...",
            score=0.85,
            published_date=date(2023, 11, 9),
        )
        assert result.score == 0.85
        assert result.doc_type == GuidanceType.REV_PROC
