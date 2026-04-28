"""Tests for South Carolina state statute converter.

Tests the SCConverter which fetches from scstatehouse.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.sc import (
    SC_TAX_CHAPTERS,
    SC_TITLES,
    SC_WELFARE_CHAPTERS,
    SCConverter,
    SCConverterError,
    download_sc_chapter,
    fetch_sc_section,
)
from axiom_corpus.models import Section

# Sample HTML from scstatehouse.gov for testing
# Based on actual SC Code structure: sections use <p><b>SECTION X-X-X. Title.</b></p>
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>SC Code Title 12, Chapter 6</title></head>
<body>
<div id="content">
<h1>CHAPTER 6 - SOUTH CAROLINA INCOME TAX ACT</h1>

<p><b>SECTION 12-6-10. Short title.</b></p>
<p>This chapter may be cited as the "South Carolina Income Tax Act".</p>
<p><b>HISTORY: 1995 Act No. 76, Section 1.</b></p>

<p>* * *</p>

<p><b>SECTION 12-6-20. Administration of chapter.</b></p>
<p>(A) This chapter shall be administered by the Department of Revenue.</p>
<p>(B) The department shall have the power to:</p>
<p>(1) prescribe forms and procedures;</p>
<p>(2) interpret and apply the provisions of this chapter;</p>
<p>(a) in a manner consistent with the Internal Revenue Code;</p>
<p>(b) as modified by this chapter.</p>
<p><b>HISTORY: 1995 Act No. 76, Section 1; 2020 Act No. 135, Section 5.</b></p>

</div>
</body>
</html>
"""

# Single section HTML (fetched individually)
SAMPLE_SINGLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>SC Code Section 12-6-510</title></head>
<body>
<div id="content">

<p><b>SECTION 12-6-510. Tax rates for individuals, estates, and trusts.</b></p>
<p>(A) A tax is imposed for each taxable year on the South Carolina taxable income of every individual, estate, or trust, computed at the following rates with the income brackets indexed for inflation:</p>
<p>(1) Zero percent on the first two thousand five hundred dollars;</p>
<p>(2) Three percent on the next two thousand five hundred dollars;</p>
<p>(3) Four percent on the next two thousand five hundred dollars;</p>
<p>(4) Five percent on the next two thousand five hundred dollars;</p>
<p>(5) Six percent on the next five thousand dollars;</p>
<p>(6) Seven percent on all taxable income in excess of fifteen thousand dollars.</p>
<p>(B) The income brackets shall be indexed annually for inflation.</p>
<p><b>HISTORY: 1995 Act No. 76, Section 1.</b></p>

</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Title 12 - Chapter 6</title></head>
<body>
<div id="content">
<h1>Title 12 - Taxation</h1>
<h2>CHAPTER 6 - SOUTH CAROLINA INCOME TAX ACT</h2>

<p><b>SECTION 12-6-10. Short title.</b></p>
<p><b>SECTION 12-6-20. Administration of chapter.</b></p>
<p><b>SECTION 12-6-30. Definitions.</b></p>
<p><b>SECTION 12-6-40. Conformity with Internal Revenue Code.</b></p>
<p><b>SECTION 12-6-50. Legislative intent.</b></p>

</div>
</body>
</html>
"""


class TestSCTitlesRegistry:
    """Test South Carolina title and chapter registries."""

    def test_title_12_in_titles(self):
        """Title 12 (Taxation) is in titles."""
        assert 12 in SC_TITLES
        assert "Taxation" in SC_TITLES[12]

    def test_title_43_in_titles(self):
        """Title 43 (Social Services) is in titles."""
        assert 43 in SC_TITLES
        assert "Social" in SC_TITLES[43]

    def test_chapter_6_in_tax_chapters(self):
        """Chapter 6 (Income Tax Act) is in tax chapters."""
        assert 6 in SC_TAX_CHAPTERS
        assert "Income Tax" in SC_TAX_CHAPTERS[6]

    def test_welfare_chapters_exist(self):
        """Welfare chapters are defined."""
        assert len(SC_WELFARE_CHAPTERS) > 0


class TestSCConverter:
    """Test SCConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = SCConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = SCConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = SCConverter(year=2024)
        assert converter.year == 2024

    def test_build_chapter_url(self):
        """Build correct URL for chapter fetch."""
        converter = SCConverter()
        url = converter._build_chapter_url(12, 6)
        assert "scstatehouse.gov" in url
        assert "t12c006" in url

    def test_build_chapter_url_single_digit(self):
        """Build correct URL for single-digit chapter."""
        converter = SCConverter()
        url = converter._build_chapter_url(43, 1)
        assert "t43c001" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with SCConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None

    def test_get_title_info(self):
        """Get title information for known titles."""
        converter = SCConverter()
        title_name = converter._get_title_name(12)
        assert title_name is not None
        assert "Taxation" in title_name


class TestSCConverterParsing:
    """Test SCConverter HTML parsing."""

    def test_parse_section_from_chapter_html(self):
        """Parse section from chapter HTML."""
        converter = SCConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_SECTION_HTML, 12, 6, "https://example.com"
        )

        assert len(sections) >= 2
        # Check first section
        sec_10 = next(
            (s for s in sections if s.section_number == "12-6-10"), None
        )
        assert sec_10 is not None
        assert sec_10.section_title == "Short title"
        assert "South Carolina Income Tax Act" in sec_10.text

    def test_parse_section_with_subsections(self):
        """Parse section with hierarchical subsections."""
        converter = SCConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_SECTION_HTML, 12, 6, "https://example.com"
        )

        sec_20 = next(
            (s for s in sections if s.section_number == "12-6-20"), None
        )
        assert sec_20 is not None
        assert sec_20.section_title == "Administration of chapter"
        # Should have subsections (A), (B)
        assert len(sec_20.subsections) >= 2

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (B)."""
        converter = SCConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_SECTION_HTML, 12, 6, "https://example.com"
        )

        sec_20 = next(
            (s for s in sections if s.section_number == "12-6-20"), None
        )
        assert sec_20 is not None
        # Find subsection (B)
        sub_b = next(
            (s for s in sec_20.subsections if s.identifier == "B"), None
        )
        assert sub_b is not None
        # Should have children (1), (2)
        assert len(sub_b.children) >= 2

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = SCConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_SECTION_HTML, 12, 6, "https://example.com"
        )

        sec_10 = next(
            (s for s in sections if s.section_number == "12-6-10"), None
        )
        assert sec_10 is not None
        assert sec_10.history is not None
        assert "1995 Act No. 76" in sec_10.history

    def test_to_section_model(self):
        """Convert ParsedSCSection to Section model."""
        converter = SCConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_SECTION_HTML, 12, 6, "https://example.com"
        )

        sec_10 = next(
            (s for s in sections if s.section_number == "12-6-10"), None
        )
        assert sec_10 is not None

        section = converter._to_section(sec_10)

        assert isinstance(section, Section)
        assert section.citation.section == "SC-12-6-10"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Short title"
        assert "South Carolina" in section.title_name
        assert section.uslm_id == "sc/12/6/12-6-10"
        assert section.source_url == "https://example.com"


class TestSCConverterFetching:
    """Test SCConverter HTTP fetching with mocks."""

    @patch.object(SCConverter, "_get")
    def test_fetch_chapter(self, mock_get):
        """Fetch and parse a chapter."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = SCConverter()
        sections = converter.fetch_chapter(12, 6)

        assert len(sections) >= 2
        assert all(isinstance(s, Section) for s in sections)
        assert any("12-6-10" in s.citation.section for s in sections)

    @patch.object(SCConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SINGLE_SECTION_HTML

        converter = SCConverter()
        section = converter.fetch_section("12-6-510")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "SC-12-6-510"
        assert "Tax rates" in section.section_title

    @patch.object(SCConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section cannot be found</body></html>"

        converter = SCConverter()
        with pytest.raises(SCConverterError) as exc_info:
            converter.fetch_section("99-99-999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(SCConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = SCConverter()
        sections = list(converter.iter_chapter(12, 6))

        assert len(sections) >= 2
        assert all(isinstance(s, Section) for s in sections)


class TestSCConverterSubsections:
    """Test subsection parsing specifically."""

    def test_parse_tax_rate_subsections(self):
        """Parse tax rate section with numeric subsections."""
        converter = SCConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_SINGLE_SECTION_HTML, 12, 6, "https://example.com"
        )

        assert len(sections) == 1
        sec = sections[0]

        # Should have (A) and (B) subsections
        assert len(sec.subsections) >= 2

        # Find (A) subsection
        sub_a = next(
            (s for s in sec.subsections if s.identifier == "A"), None
        )
        assert sub_a is not None
        # (A) should have children (1) through (6)
        assert len(sub_a.children) >= 6


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(SCConverter, "_get")
    def test_fetch_sc_section(self, mock_get):
        """Test fetch_sc_section function."""
        mock_get.return_value = SAMPLE_SINGLE_SECTION_HTML

        section = fetch_sc_section("12-6-510")

        assert section is not None
        assert section.citation.section == "SC-12-6-510"

    @patch.object(SCConverter, "_get")
    def test_download_sc_chapter(self, mock_get):
        """Test download_sc_chapter function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        sections = download_sc_chapter(12, 6)

        assert len(sections) >= 2
        assert all(isinstance(s, Section) for s in sections)


class TestSCConverterIntegration:
    """Integration tests that hit real scstatehouse.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_chapter(self):
        """Fetch South Carolina Income Tax chapter 12-6."""
        converter = SCConverter()
        sections = converter.fetch_chapter(12, 6)

        assert sections is not None
        assert len(sections) > 0
        assert any("12-6" in s.citation.section for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_specific_section(self):
        """Fetch South Carolina section 12-6-10."""
        converter = SCConverter()
        section = converter.fetch_section("12-6-10")

        assert section is not None
        assert section.citation.section == "SC-12-6-10"
        assert "income" in section.text.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_chapter(self):
        """Fetch a South Carolina social services chapter."""
        converter = SCConverter()
        try:
            sections = converter.fetch_chapter(43, 1)
            assert len(sections) > 0
        except SCConverterError:
            # Chapter may not exist or have different structure
            pytest.skip("Chapter 43-1 not found or unavailable")
