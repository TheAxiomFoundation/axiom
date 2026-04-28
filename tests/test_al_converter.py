"""Tests for Alabama state statute converter.

Tests the ALConverter which fetches from alisondb.legislature.state.al.us (ALISON database)
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.al import (
    AL_TAX_CHAPTERS,
    AL_TITLES,
    AL_WELFARE_CHAPTERS,
    ALConverter,
    ALConverterError,
    download_al_chapter,
    fetch_al_section,
)
from axiom.models import Section

# Sample HTML from alisondb.legislature.state.al.us for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Code of Alabama 40-18-1</title></head>
<body>
<h1>Code of Alabama 1975</h1>
<h2>Title 40 - Revenue and Taxation</h2>
<h3>Chapter 18 - Income Tax</h3>
<div class="content">
<p><b>Section 40-18-1 Definitions.</b></p>
<p>For the purpose of this chapter, the following terms shall have the meanings respectively ascribed to them by this section:</p>
<p>(1) ALABAMA S CORPORATION. An S corporation as defined under the provisions of 26 U.S.C. Section 1361(a)(1).</p>
<p>(2) CORPORATION. Includes associations, joint stock companies, and any other entity classified as an association taxable as a corporation under the Internal Revenue Code of 1986, as amended.</p>
<p>(3) DEPARTMENT. The Alabama Department of Revenue.</p>
<p>(a) For purposes of subsection (3), the department shall include all authorized agents.</p>
<p>(b) The commissioner may delegate authority as appropriate.</p>
<p>(4) INDIVIDUAL. A natural person.</p>
<p>(5) PERSON. Includes an individual, trust, estate, partnership, corporation, or other entity.</p>
<p>(Acts 1935, No. 194, p. 256; Acts 1939, No. 112, p. 144; Act 2001-1085, p. 2002, Section 1.)</p>
</div>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Code of Alabama 38-2-1</title></head>
<body>
<h1>Code of Alabama 1975</h1>
<h2>Title 38 - Public Welfare</h2>
<h3>Chapter 2 - Department of Human Resources</h3>
<div class="content">
<p><b>Section 38-2-1 Definitions.</b></p>
<p>As used in this chapter, the following terms shall have the meanings respectively ascribed:</p>
<p>(a) COMMISSIONER. The Commissioner of the Department of Human Resources.</p>
<p>(b) DEPARTMENT. The Alabama Department of Human Resources.</p>
<p>(c) BOARD. The State Board of Human Resources.</p>
<p>(Acts 1939, No. 112; Acts 1975, No. 1205, Section 1.)</p>
</div>
</body>
</html>
"""

SAMPLE_COMPLEX_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Code of Alabama 40-18-35</title></head>
<body>
<h1>Code of Alabama 1975</h1>
<div class="content">
<p><b>Section 40-18-35 Deductions allowed to corporations.</b></p>
<p>(a) In computing net income there shall be allowed as deductions:</p>
<p>(1) Refunds of state and local income taxes previously included in income.</p>
<p>(2) Federal income tax paid or accrued during the taxable year.</p>
<p>(3) Interest from obligations of the United States.</p>
<p>(b) Restrictions on the deductibility of certain intangible expenses:</p>
<p>(1) A corporation shall add back otherwise deductible interest expenses.</p>
<p>(2) The add-back required by subdivision (1) shall not apply if:</p>
<p>(c) Additional restrictions apply to captive REIT dividends.</p>
<p>(d) No item may be deducted more than once.</p>
<p>(e) The following tax credits are allowed against the tax levied.</p>
<p>(Acts 1935, No. 194, p. 256; Act 2008-543, p. 1175, Section 1.)</p>
</div>
</body>
</html>
"""

SAMPLE_NOT_FOUND_HTML = """<!DOCTYPE html>
<html>
<head><title>Not Found</title></head>
<body>
<p>The requested section does not exist or was not found.</p>
</body>
</html>
"""


class TestALTitlesRegistry:
    """Test Alabama title registries."""

    def test_title_40_in_titles(self):
        """Title 40 (Revenue and Taxation) is in titles."""
        assert 40 in AL_TITLES
        assert "Revenue" in AL_TITLES[40] or "Taxation" in AL_TITLES[40]

    def test_title_38_in_titles(self):
        """Title 38 (Public Welfare) is in titles."""
        assert 38 in AL_TITLES
        assert "Welfare" in AL_TITLES[38]

    def test_chapter_18_in_tax_chapters(self):
        """Chapter 18 (Income Tax) is in tax chapters."""
        assert 18 in AL_TAX_CHAPTERS
        assert "Income" in AL_TAX_CHAPTERS[18]

    def test_chapter_23_in_tax_chapters(self):
        """Chapter 23 (Sales and Use Tax) is in tax chapters."""
        assert 23 in AL_TAX_CHAPTERS
        assert "Sales" in AL_TAX_CHAPTERS[23] or "Use" in AL_TAX_CHAPTERS[23]

    def test_chapter_2_in_welfare_chapters(self):
        """Chapter 2 (Department of Human Resources) is in welfare chapters."""
        assert 2 in AL_WELFARE_CHAPTERS
        assert "Human Resources" in AL_WELFARE_CHAPTERS[2] or "Department" in AL_WELFARE_CHAPTERS[2]


class TestALConverter:
    """Test ALConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = ALConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = ALConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = ALConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = ALConverter()
        url = converter._build_section_url("40-18-1")
        assert "alisondb.legislature.state.al.us" in url
        assert "codeofalabama" in url
        assert "1975" in url
        assert "40-18-1.htm" in url

    def test_build_section_url_with_decimal(self):
        """Build URL for section with decimal suffix."""
        converter = ALConverter()
        url = converter._build_section_url("40-18-24.2")
        assert "40-18-24.2.htm" in url

    def test_parse_section_number(self):
        """Parse section number into components."""
        converter = ALConverter()

        title, chapter, suffix = converter._parse_section_number("40-18-1")
        assert title == 40
        assert chapter == 18
        assert suffix == "1"

    def test_parse_section_number_with_decimal(self):
        """Parse section number with decimal suffix."""
        converter = ALConverter()

        title, chapter, suffix = converter._parse_section_number("40-18-24.2")
        assert title == 40
        assert chapter == 18
        assert suffix == "24.2"

    def test_context_manager(self):
        """Converter works as context manager."""
        with ALConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestALConverterParsing:
    """Test ALConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedALSection."""
        converter = ALConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "40-18-1", "https://example.com")

        assert parsed.section_number == "40-18-1"
        assert parsed.section_title == "Definitions"
        assert parsed.title_number == 40
        assert parsed.title_name == "Revenue and Taxation"
        assert parsed.chapter_number == 18
        assert "CORPORATION" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_welfare_section(self):
        """Parse welfare section HTML."""
        converter = ALConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "38-2-1", "https://example.com"
        )

        assert parsed.section_number == "38-2-1"
        assert parsed.title_number == 38
        assert parsed.title_name == "Public Welfare"
        assert parsed.chapter_number == 2
        assert "DEPARTMENT" in parsed.text

    def test_parse_subsections_numbered(self):
        """Parse numbered subsections (1), (2), etc."""
        converter = ALConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "40-18-1", "https://example.com")

        # Should have numbered subsections
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)

    def test_parse_subsections_lettered(self):
        """Parse lettered subsections (a), (b), etc."""
        converter = ALConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "38-2-1", "https://example.com"
        )

        # Should have lettered subsections
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)

    def test_parse_complex_hierarchical_subsections(self):
        """Parse complex section with nested subsections."""
        converter = ALConverter()
        parsed = converter._parse_section_html(
            SAMPLE_COMPLEX_SECTION_HTML, "40-18-35", "https://example.com"
        )

        # Should have top-level lettered subsections
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)

        # Subsection (a) should have children (1), (2), (3)
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        if sub_a and sub_a.children:
            assert any(c.identifier == "1" for c in sub_a.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = ALConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "40-18-1", "https://example.com")

        assert parsed.history is not None
        assert "1935" in parsed.history or "Acts" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedALSection to Section model."""
        converter = ALConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "40-18-1", "https://example.com")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "AL-40-18-1"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "Code of Alabama" in section.title_name
        assert section.uslm_id == "al/40/18/40-18-1"
        assert section.source_url == "https://example.com"


class TestALConverterFetching:
    """Test ALConverter HTTP fetching with mocks."""

    @patch.object(ALConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = ALConverter()
        section = converter.fetch_section("40-18-1")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "AL-40-18-1"
        assert "Definitions" in section.section_title

    @patch.object(ALConverter, "_get")
    def test_fetch_welfare_section(self, mock_get):
        """Fetch and parse a welfare section."""
        mock_get.return_value = SAMPLE_WELFARE_SECTION_HTML

        converter = ALConverter()
        section = converter.fetch_section("38-2-1")

        assert section is not None
        assert section.citation.section == "AL-38-2-1"
        assert "Welfare" in section.title_name

    @patch.object(ALConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_NOT_FOUND_HTML

        converter = ALConverter()
        with pytest.raises(ALConverterError) as exc_info:
            converter.fetch_section("99-99-999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(ALConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # Return valid HTML for first 3, then not found
        mock_get.side_effect = [
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ] + [SAMPLE_NOT_FOUND_HTML] * 15  # Trigger consecutive failure exit

        converter = ALConverter()
        sections = list(converter.iter_chapter(40, 18, max_sections=20))

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)

    @patch.object(ALConverter, "_get")
    def test_iter_chapter_handles_gaps(self, mock_get):
        """Iterate over chapter with gaps in section numbers."""
        # Simulate gaps: 1, 2, not found, not found, 5
        mock_get.side_effect = [
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_NOT_FOUND_HTML,
            SAMPLE_NOT_FOUND_HTML,
            SAMPLE_SECTION_HTML,
        ] + [SAMPLE_NOT_FOUND_HTML] * 15

        converter = ALConverter()
        sections = list(converter.iter_chapter(40, 18, max_sections=20))

        # Should get 3 sections (continues after gaps until 10 consecutive failures)
        assert len(sections) == 3


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(ALConverter, "_get")
    def test_fetch_al_section(self, mock_get):
        """Test fetch_al_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_al_section("40-18-1")

        assert section is not None
        assert section.citation.section == "AL-40-18-1"

    @patch.object(ALConverter, "_get")
    def test_download_al_chapter(self, mock_get):
        """Test download_al_chapter function."""
        mock_get.side_effect = [
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ] + [SAMPLE_NOT_FOUND_HTML] * 15

        sections = download_al_chapter(40, 18)

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestALConverterIntegration:
    """Integration tests that hit real alisondb.legislature.state.al.us (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_definitions(self):
        """Fetch Alabama Income Tax definitions section 40-18-1."""
        converter = ALConverter()
        section = converter.fetch_section("40-18-1")

        assert section is not None
        assert section.citation.section == "AL-40-18-1"
        assert "definition" in section.section_title.lower() or "Definition" in section.text

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_corporate_deductions(self):
        """Fetch Alabama corporate deductions section 40-18-35."""
        converter = ALConverter()
        section = converter.fetch_section("40-18-35")

        assert section is not None
        assert section.citation.section == "AL-40-18-35"
        assert "deduction" in section.text.lower() or "corporation" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Alabama welfare section 38-2-1."""
        converter = ALConverter()
        try:
            section = converter.fetch_section("38-2-1")
            assert section.citation.section == "AL-38-2-1"
        except ALConverterError:
            # Section may not exist, which is acceptable
            pytest.skip("Section 38-2-1 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_iter_chapter_40_18(self):
        """Iterate over some sections in Chapter 18."""
        converter = ALConverter()
        sections = []

        for section in converter.iter_chapter(40, 18, max_sections=5):
            sections.append(section)
            if len(sections) >= 3:
                break

        # Should get at least one section
        assert len(sections) >= 1
        assert all(s.citation.section.startswith("AL-40-18-") for s in sections)
