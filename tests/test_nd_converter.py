"""Tests for North Dakota state statute converter.

Tests the NDConverter which fetches from ndlegis.gov (North Dakota Legislative Branch)
and converts North Dakota Century Code sections to the internal Section model.

The ND Legislature provides:
- Title listing at /cencode/t{title}.html
- Chapter section listings at /cencode/t{title}c{chapter}.html
- Full section text in PDFs at /cencode/t{title}c{chapter}.pdf with named destinations
"""

from unittest.mock import patch

import pytest

from axiom.converters.us_states.nd import (
    ND_TAX_CHAPTERS,
    ND_WELFARE_CHAPTERS,
    NDConverter,
    NDConverterError,
    download_nd_chapter,
    fetch_nd_section,
)
from axiom.models import Section

# Sample HTML from ndlegis.gov for testing
SAMPLE_TITLE_HTML = """<!DOCTYPE html>
<html lang="en">
<head><title>North Dakota Century Code | North Dakota Legislative Branch</title></head>
<body>
<h1>Title 57</h1>
<h2>Taxation</h2>
<table>
    <tr>
        <td class="no-wrap"><a href="t57c01.pdf">57-01</a></td>
        <td><a href="t57c01.html">57-01 Sections</a></td>
        <td>Tax Commissioner</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t57c02.pdf">57-02</a></td>
        <td><a href="t57c02.html">57-02 Sections</a></td>
        <td>General Property Assessment</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t57c38.pdf">57-38</a></td>
        <td><a href="t57c38.html">57-38 Sections</a></td>
        <td>Income Tax</td>
    </tr>
</table>
</body>
</html>
"""

SAMPLE_CHAPTER_HTML = """<!DOCTYPE html>
<html lang="en">
<head><title>North Dakota Century Code | North Dakota Legislative Branch</title></head>
<body>
<div class="views-row">
    <h1>Chapter 57-38</h1>
</div>
<table>
    <tr>
        <td class="no-wrap"><a href="t57c38.pdf#nameddest=57-38-01">57-38-01</a></td>
        <td>Definitions</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t57c38.pdf#nameddest=57-38-01p1">57-38-01.1</a></td>
        <td>Individual income tax - Tax rates for individuals, estates, and trusts</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t57c38.pdf#nameddest=57-38-01p2">57-38-01.2</a></td>
        <td>Tax credits for individuals [Repealed]</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t57c38.pdf#nameddest=57-38-01p3">57-38-01.3</a></td>
        <td>Corporations - Tax imposed - Rates</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t57c38.pdf#nameddest=57-38-02">57-38-02</a></td>
        <td>Tax commissioner - Powers and duties</td>
    </tr>
</table>
</body>
</html>
"""

SAMPLE_WELFARE_CHAPTER_HTML = """<!DOCTYPE html>
<html lang="en">
<head><title>North Dakota Century Code | North Dakota Legislative Branch</title></head>
<body>
<div class="views-row">
    <h1>Chapter 50-06</h1>
</div>
<table>
    <tr>
        <td class="no-wrap"><a href="t50c06.pdf#nameddest=50-06-01">50-06-01</a></td>
        <td>Definitions</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t50c06.pdf#nameddest=50-06-01p1">50-06-01.1</a></td>
        <td>Department of health and human services to be substituted for department of human services</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t50c06.pdf#nameddest=50-06-02">50-06-02</a></td>
        <td>Director - Appointment - Term - Salary</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t50c06.pdf#nameddest=50-06-05p1">50-06-05.1</a></td>
        <td>Medicaid expansion - Legislative intent</td>
    </tr>
    <tr>
        <td class="no-wrap"><a href="t50c06.pdf#nameddest=50-06-05p2">50-06-05.2</a></td>
        <td>Medicaid program - Federal waiver - Requirements (Effective through June 30, 2027)</td>
    </tr>
</table>
</body>
</html>
"""


class TestNDChaptersRegistry:
    """Test North Dakota chapter registries."""

    def test_chapter_57_38_in_tax_chapters(self):
        """Chapter 57-38 (Income Tax) is in tax chapters."""
        assert "57-38" in ND_TAX_CHAPTERS
        assert "Income Tax" in ND_TAX_CHAPTERS["57-38"]

    def test_chapter_57_01_in_tax_chapters(self):
        """Chapter 57-01 (Tax Commissioner) is in tax chapters."""
        assert "57-01" in ND_TAX_CHAPTERS
        assert "Tax Commissioner" in ND_TAX_CHAPTERS["57-01"]

    def test_chapter_50_06_in_welfare_chapters(self):
        """Chapter 50-06 (Human Services) is in welfare chapters."""
        assert "50-06" in ND_WELFARE_CHAPTERS
        assert "Human Services" in ND_WELFARE_CHAPTERS["50-06"] or "Health" in ND_WELFARE_CHAPTERS["50-06"]

    def test_tax_chapters_in_title_57(self):
        """Tax chapters are in Title 57."""
        for chapter in ND_TAX_CHAPTERS:
            assert chapter.startswith("57-")


class TestNDConverter:
    """Test NDConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = NDConverter()
        assert converter.rate_limit_delay == 0.5

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = NDConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_build_title_url(self):
        """Build correct URL for title page."""
        converter = NDConverter()
        url = converter._build_title_url(57)
        assert "ndlegis.gov" in url
        assert "t57.html" in url

    def test_build_chapter_url_simple(self):
        """Build correct URL for chapter with simple number."""
        converter = NDConverter()
        url = converter._build_chapter_url("57-01")
        assert "ndlegis.gov" in url
        assert "t57c01.html" in url

    def test_build_chapter_url_with_decimal(self):
        """Build correct URL for chapter with decimal (e.g., 57-02.1)."""
        converter = NDConverter()
        url = converter._build_chapter_url("57-02.1")
        assert "t57c02-1.html" in url

    def test_build_pdf_url(self):
        """Build correct URL for chapter PDF."""
        converter = NDConverter()
        url = converter._build_pdf_url("57-38")
        assert "t57c38.pdf" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with NDConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None

    def test_parse_section_number_simple(self):
        """Parse simple section number."""
        converter = NDConverter()
        title, chapter, section = converter._parse_section_number("57-38-01")
        assert title == 57
        assert chapter == "57-38"
        assert section == "01"

    def test_parse_section_number_with_decimal(self):
        """Parse section number with decimal."""
        converter = NDConverter()
        title, chapter, section = converter._parse_section_number("57-38-01.1")
        assert title == 57
        assert chapter == "57-38"
        assert section == "01.1"

    def test_parse_section_number_welfare(self):
        """Parse welfare section number."""
        converter = NDConverter()
        title, chapter, section = converter._parse_section_number("50-06-05.2")
        assert title == 50
        assert chapter == "50-06"
        assert section == "05.2"


class TestNDConverterParsing:
    """Test NDConverter HTML parsing."""

    def test_parse_chapter_html(self):
        """Parse chapter HTML into section list."""
        converter = NDConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, "57-38")

        assert len(sections) == 5
        assert sections[0]["section_number"] == "57-38-01"
        assert sections[0]["section_title"] == "Definitions"
        assert sections[1]["section_number"] == "57-38-01.1"
        assert "Individual income tax" in sections[1]["section_title"]

    def test_parse_chapter_html_with_repealed(self):
        """Parse chapter HTML including repealed sections."""
        converter = NDConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, "57-38")

        # Find the repealed section
        repealed = next((s for s in sections if "Repealed" in s["section_title"]), None)
        assert repealed is not None
        assert repealed["section_number"] == "57-38-01.2"

    def test_parse_welfare_chapter_html(self):
        """Parse welfare chapter HTML."""
        converter = NDConverter()
        sections = converter._parse_chapter_html(SAMPLE_WELFARE_CHAPTER_HTML, "50-06")

        assert len(sections) == 5
        assert sections[0]["section_number"] == "50-06-01"
        assert "Definitions" in sections[0]["section_title"]
        # Check Medicaid section
        medicaid = next((s for s in sections if "Medicaid expansion" in s["section_title"]), None)
        assert medicaid is not None

    def test_parse_title_html(self):
        """Parse title HTML to get list of chapters."""
        converter = NDConverter()
        chapters = converter._parse_title_html(SAMPLE_TITLE_HTML, 57)

        assert len(chapters) == 3
        assert "57-01" in chapters
        assert "57-02" in chapters
        assert "57-38" in chapters
        assert chapters["57-01"] == "Tax Commissioner"


class TestNDConverterFetching:
    """Test NDConverter HTTP fetching with mocks."""

    @patch.object(NDConverter, "_get")
    def test_get_chapter_sections(self, mock_get):
        """Get list of sections from chapter HTML."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = NDConverter()
        sections = converter.get_chapter_sections("57-38")

        assert len(sections) == 5
        assert sections[0]["section_number"] == "57-38-01"

    @patch.object(NDConverter, "_get")
    def test_get_title_chapters(self, mock_get):
        """Get list of chapters from title HTML."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        converter = NDConverter()
        chapters = converter.get_title_chapters(57)

        assert len(chapters) == 3
        assert "57-38" in chapters

    @patch.object(NDConverter, "_get")
    def test_fetch_section_metadata(self, mock_get):
        """Fetch section metadata (without PDF text)."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = NDConverter()
        section = converter.fetch_section_metadata("57-38-01")

        assert isinstance(section, Section)
        assert section.citation.section == "ND-57-38-01"
        assert section.section_title == "Definitions"
        assert "ndlegis.gov" in section.source_url

    @patch.object(NDConverter, "_get")
    def test_iter_chapter_metadata(self, mock_get):
        """Iterate over section metadata in a chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = NDConverter()
        sections = list(converter.iter_chapter("57-38", include_text=False))

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)
        assert sections[0].citation.section == "ND-57-38-01"


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(NDConverter, "_get")
    def test_fetch_nd_section(self, mock_get):
        """Test fetch_nd_section function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        section = fetch_nd_section("57-38-01")

        assert section is not None
        assert section.citation.section == "ND-57-38-01"
        assert "Definitions" in section.section_title

    @patch.object(NDConverter, "_get")
    def test_download_nd_chapter(self, mock_get):
        """Test download_nd_chapter function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        sections = download_nd_chapter("57-38")

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestNDConverterIntegration:
    """Integration tests that hit real ndlegis.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch North Dakota Income Tax section 57-38-01."""
        converter = NDConverter()
        section = converter.fetch_section_metadata("57-38-01")

        assert section is not None
        assert section.citation.section == "ND-57-38-01"
        assert "definitions" in section.section_title.lower() or section.section_title != ""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_commissioner_section(self):
        """Fetch Tax Commissioner section 57-01-01."""
        converter = NDConverter()
        section = converter.fetch_section_metadata("57-01-01")

        assert section is not None
        assert section.citation.section == "ND-57-01-01"

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_57_38_sections(self):
        """Get list of sections in Chapter 57-38 (Income Tax)."""
        converter = NDConverter()
        sections = converter.get_chapter_sections("57-38")

        assert len(sections) > 0
        assert all(s["section_number"].startswith("57-38-") for s in sections)
        assert any("57-38-01" in s["section_number"] for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_57_chapters(self):
        """Get list of chapters in Title 57 (Taxation)."""
        converter = NDConverter()
        chapters = converter.get_title_chapters(57)

        assert len(chapters) > 0
        assert "57-01" in chapters
        assert "57-38" in chapters

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch welfare section 50-06-01."""
        converter = NDConverter()
        try:
            section = converter.fetch_section_metadata("50-06-01")
            assert section.citation.section == "ND-50-06-01"
        except NDConverterError:
            pytest.skip("Section 50-06-01 not found")
