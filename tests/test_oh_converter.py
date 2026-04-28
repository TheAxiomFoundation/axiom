"""Tests for Ohio Revised Code converter.

Tests the OHConverter which fetches from codes.ohio.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.oh import (
    OH_TAX_CHAPTERS,
    OH_WELFARE_CHAPTERS,
    OHConverter,
    OHConverterError,
    download_oh_chapter,
    fetch_oh_section,
)
from axiom.models import Section

# Sample HTML from codes.ohio.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 5747.01 | Definitions</title></head>
<body>
<nav>
  <a href="/ohio-revised-code">Ohio Revised Code</a> /
  <a href="/ohio-revised-code/title-57">Title 57 Taxation</a> /
  <a href="/ohio-revised-code/chapter-5747">Chapter 5747 Income Tax</a>
</nav>
<main>
<h1>Section 5747.01 | Definitions.</h1>
<p>Effective: September 30, 2025</p>
<p>Latest Legislation: House Bill 96 - 136th General Assembly</p>
<div class="section-content">
<p>(A) As used in this chapter:</p>
<p>(1) "Adjusted gross income" means a taxpayer's adjusted gross income as defined in section 62 of the Internal Revenue Code.</p>
<p>(2) "Business income" means income arising from transactions and activity in the regular course of the taxpayer's trade or business.</p>
<p>(a) Business income includes income from tangible and intangible property if the acquisition, management, and disposition of the property constitute integral parts of the taxpayer's regular trade or business operations.</p>
<p>(b) Business income includes income from the sale of a partnership interest, S corporation shares, or membership interest in a limited liability company.</p>
<p>(B) "Compensation" means any form of remuneration paid to an employee for personal services.</p>
<p>(C) "Domicile" means the place where a person has their true, fixed, and permanent home.</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 5101.16 | Paying county share of public assistance expenditures.</title></head>
<body>
<nav>
  <a href="/ohio-revised-code">Ohio Revised Code</a> /
  <a href="/ohio-revised-code/title-51">Title 51 Public Welfare</a> /
  <a href="/ohio-revised-code/chapter-5101">Chapter 5101</a>
</nav>
<main>
<h1>Section 5101.16 | Paying county share of public assistance expenditures.</h1>
<p>Effective: December 31, 2017</p>
<p>Latest Legislation: House Bill 49 - 132nd General Assembly</p>
<div class="section-content">
<p>(A) Except as otherwise provided in division (B) of this section, the department of job and family services shall determine the county share of expenditures for public assistance.</p>
<p>(1) The department shall certify to the county auditor the amount of the county share.</p>
<p>(2) The county auditor shall draw a warrant on the county treasury for the amount certified.</p>
<p>(B) The county share shall be paid from the county's general fund unless otherwise provided by law.</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 5747 | Income Tax - Ohio Revised Code</title></head>
<body>
<h1>Chapter 5747 | Income Tax</h1>
<div id="contents">
<ul>
<li><a href="/ohio-revised-code/section-5747.01">5747.01 Definitions</a></li>
<li><a href="/ohio-revised-code/section-5747.02">5747.02 Tax rates</a></li>
<li><a href="/ohio-revised-code/section-5747.025">5747.025 Joint filers</a></li>
<li><a href="/ohio-revised-code/section-5747.03">5747.03 Withholding</a></li>
</ul>
</div>
</body>
</html>
"""


class TestOHChaptersRegistry:
    """Test Ohio chapter registries."""

    def test_chapter_5747_in_tax_chapters(self):
        """Chapter 5747 (Income Tax) is in tax chapters."""
        assert 5747 in OH_TAX_CHAPTERS
        assert "Income Tax" in OH_TAX_CHAPTERS[5747]

    def test_chapter_5739_in_tax_chapters(self):
        """Chapter 5739 (Sales Tax) is in tax chapters."""
        assert 5739 in OH_TAX_CHAPTERS
        assert "Sales Tax" in OH_TAX_CHAPTERS[5739]

    def test_chapter_5101_in_welfare_chapters(self):
        """Chapter 5101 (DJFS) is in welfare chapters."""
        assert 5101 in OH_WELFARE_CHAPTERS
        assert "Job and Family Services" in OH_WELFARE_CHAPTERS[5101]

    def test_chapter_5107_in_welfare_chapters(self):
        """Chapter 5107 (Ohio Works First) is in welfare chapters."""
        assert 5107 in OH_WELFARE_CHAPTERS
        assert "Ohio Works First" in OH_WELFARE_CHAPTERS[5107]

    def test_tax_chapters_in_title_57(self):
        """Tax chapters are in the expected range 5701-5751."""
        for chapter in OH_TAX_CHAPTERS:
            assert 5701 <= chapter <= 5799

    def test_welfare_chapters_in_title_51(self):
        """Welfare chapters are in the expected range 5101-5199."""
        for chapter in OH_WELFARE_CHAPTERS:
            assert 5101 <= chapter <= 5199


class TestOHConverter:
    """Test OHConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = OHConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = OHConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = OHConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = OHConverter()
        url = converter._build_section_url("5747.01")
        assert "codes.ohio.gov" in url
        assert "ohio-revised-code/section-5747.01" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = OHConverter()
        url = converter._build_chapter_url(5747)
        assert "ohio-revised-code/chapter-5747" in url

    def test_get_title_for_chapter_tax(self):
        """Get title for tax chapter."""
        converter = OHConverter()
        title_num, title_name = converter._get_title_for_chapter(5747)
        assert title_num == 57
        assert title_name == "Taxation"

    def test_get_title_for_chapter_welfare(self):
        """Get title for welfare chapter."""
        converter = OHConverter()
        title_num, title_name = converter._get_title_for_chapter(5101)
        assert title_num == 51
        assert title_name == "Public Welfare"

    def test_context_manager(self):
        """Converter works as context manager."""
        with OHConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestOHConverterParsing:
    """Test OHConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedOHSection."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "5747.01", "https://codes.ohio.gov/ohio-revised-code/section-5747.01"
        )

        assert parsed.section_number == "5747.01"
        assert parsed.section_title == "Definitions"
        assert parsed.chapter_number == 5747
        assert parsed.chapter_title == "Income Tax"
        assert parsed.title_number == 57
        assert parsed.title_name == "Taxation"
        assert "adjusted gross income" in parsed.text.lower()

    def test_parse_effective_date(self):
        """Parse effective date from section HTML."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "5747.01", "https://example.com"
        )

        assert parsed.effective_date is not None
        assert parsed.effective_date.year == 2025
        assert parsed.effective_date.month == 9
        assert parsed.effective_date.day == 30

    def test_parse_history(self):
        """Parse history/legislation info from section HTML."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "5747.01", "https://example.com"
        )

        assert parsed.history is not None
        assert "House Bill 96" in parsed.history

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "5747.01", "https://example.com"
        )

        # Should have subsections (A), (B), (C)
        assert len(parsed.subsections) >= 3
        identifiers = [s.identifier for s in parsed.subsections]
        assert "A" in identifiers
        assert "B" in identifiers
        assert "C" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (A)."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "5747.01", "https://example.com"
        )

        # Find subsection (A)
        sub_a = next((s for s in parsed.subsections if s.identifier == "A"), None)
        assert sub_a is not None
        # Should have children (1) and (2)
        assert len(sub_a.children) >= 2
        child_ids = [c.identifier for c in sub_a.children]
        assert "1" in child_ids
        assert "2" in child_ids

    def test_parse_level3_subsections(self):
        """Parse level 3 subsections (a), (b) under (2)."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "5747.01", "https://example.com"
        )

        # Find subsection (A) -> (2)
        sub_a = next((s for s in parsed.subsections if s.identifier == "A"), None)
        assert sub_a is not None
        sub_2 = next((c for c in sub_a.children if c.identifier == "2"), None)
        assert sub_2 is not None
        # Should have children (a) and (b)
        assert len(sub_2.children) >= 2
        grandchild_ids = [g.identifier for g in sub_2.children]
        assert "a" in grandchild_ids
        assert "b" in grandchild_ids

    def test_parse_welfare_section(self):
        """Parse a welfare section HTML."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "5101.16", "https://example.com"
        )

        assert parsed.section_number == "5101.16"
        assert "county share" in parsed.section_title.lower()
        assert parsed.chapter_number == 5101
        assert parsed.title_number == 51
        assert parsed.title_name == "Public Welfare"
        assert parsed.effective_date is not None
        assert parsed.effective_date.year == 2017

    def test_to_section_model(self):
        """Convert ParsedOHSection to Section model."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "5747.01", "https://codes.ohio.gov/ohio-revised-code/section-5747.01"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "OH-5747.01"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "Ohio Revised Code" in section.title_name
        assert section.uslm_id == "oh/5747/5747.01"
        assert "codes.ohio.gov" in section.source_url

    def test_to_section_model_welfare(self):
        """Convert welfare ParsedOHSection to Section model."""
        converter = OHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "5101.16", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "OH-5101.16"
        assert "Public Welfare" in section.title_name
        assert section.uslm_id == "oh/5101/5101.16"


class TestOHConverterFetching:
    """Test OHConverter HTTP fetching with mocks."""

    @patch.object(OHConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = OHConverter()
        section = converter.fetch_section("5747.01")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "OH-5747.01"
        assert "Definitions" in section.section_title

    @patch.object(OHConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section cannot be found</body></html>"

        converter = OHConverter()
        with pytest.raises(OHConverterError) as exc_info:
            converter.fetch_section("9999.99")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(OHConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = OHConverter()
        sections = converter.get_chapter_section_numbers(5747)

        assert len(sections) == 4
        assert "5747.01" in sections
        assert "5747.02" in sections
        assert "5747.025" in sections
        assert "5747.03" in sections

    @patch.object(OHConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = OHConverter()
        sections = list(converter.iter_chapter(5747))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(OHConverter, "_get")
    def test_fetch_oh_section(self, mock_get):
        """Test fetch_oh_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_oh_section("5747.01")

        assert section is not None
        assert section.citation.section == "OH-5747.01"

    @patch.object(OHConverter, "_get")
    def test_download_oh_chapter(self, mock_get):
        """Test download_oh_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_oh_chapter(5747)

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestOHConverterIntegration:
    """Integration tests that hit real codes.ohio.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Ohio Income Tax section 5747.01."""
        converter = OHConverter()
        section = converter.fetch_section("5747.01")

        assert section is not None
        assert section.citation.section == "OH-5747.01"
        assert "definition" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_rates_section(self):
        """Fetch Ohio Income Tax rates section 5747.02."""
        converter = OHConverter()
        section = converter.fetch_section("5747.02")

        assert section is not None
        assert section.citation.section == "OH-5747.02"
        assert "rate" in section.section_title.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_5747_sections(self):
        """Get list of sections in Chapter 5747."""
        converter = OHConverter()
        sections = converter.get_chapter_section_numbers(5747)

        assert len(sections) > 0
        assert all(s.startswith("5747.") for s in sections)
        assert "5747.01" in sections  # Definitions

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Ohio welfare section 5101.16."""
        converter = OHConverter()
        try:
            section = converter.fetch_section("5101.16")
            assert section.citation.section == "OH-5101.16"
        except OHConverterError:
            # Section may not exist, which is acceptable
            pytest.skip("Section 5101.16 not found")
