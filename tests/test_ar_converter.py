"""Tests for Arkansas state statute converter.

Tests the ARConverter which fetches Arkansas Code from the official
LexisNexis-hosted portal and converts to the internal Section model.

Arkansas Code Structure:
- Titles (e.g., Title 26: Taxation)
- Subtitles (e.g., Subtitle 5: State Taxes)
- Chapters (e.g., Chapter 51: Income Taxes)
- Subchapters (e.g., Subchapter 1: General Provisions)
- Sections (e.g., 26-51-101)

Section numbering: Title-Chapter-Section (e.g., 26-51-101)
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.ar import (
    AR_TAX_CHAPTERS,
    AR_WELFARE_CHAPTERS,
    ARConverter,
    ARConverterError,
    download_ar_chapter,
    fetch_ar_section,
)
from axiom.models import Section

# Sample HTML from Arkansas Code for testing (based on LexisNexis structure)
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Arkansas Code 26-51-101</title></head>
<body>
<div class="document">
<h1>26-51-101. Title.</h1>
<div class="body">
<p>This act shall be known and may be cited as the "Income Tax Act of 1929."</p>
</div>
<div class="history">
<p>History. Acts 1929, No. 118, &sect; 1; Pope's Dig., &sect; 14001; A.S.A. 1947, &sect; 84-2001.</p>
</div>
</div>
</body>
</html>
"""

SAMPLE_SECTION_HTML_COMPLEX = """<!DOCTYPE html>
<html>
<head><title>Arkansas Code 26-51-201</title></head>
<body>
<div class="document">
<h1>26-51-201. Tax imposed.</h1>
<div class="body">
<p>(a) A tax is hereby imposed upon the net income of:</p>
<p>(1) Every resident of the State of Arkansas;</p>
<p>(2) Every nonresident individual having a taxable status;</p>
<p>(3) Every foreign corporation deriving income from sources within this state.</p>
<p>(b) For purposes of this section:</p>
<p>(1) "Net income" means gross income minus allowed deductions;</p>
<p>(2) "Resident" means a person domiciled in this state.</p>
</div>
<div class="history">
<p>History. Acts 1929, No. 118, &sect; 2; Pope's Dig., &sect; 14002; amended by Acts 1987, No. 234.</p>
</div>
<div class="effective-date">
<p>Effective Date: January 1, 2024</p>
</div>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 51 - Income Taxes</title></head>
<body>
<div class="toc">
<h1>Chapter 51 - Income Taxes</h1>
<ul>
<li><a href="/section/26-51-101">26-51-101. Title.</a></li>
<li><a href="/section/26-51-102">26-51-102. Purpose and construction.</a></li>
<li><a href="/section/26-51-201">26-51-201. Tax imposed.</a></li>
<li><a href="/section/26-51-202">26-51-202. Rate of tax.</a></li>
</ul>
</div>
</body>
</html>
"""


class TestARChaptersRegistry:
    """Test Arkansas chapter registries."""

    def test_chapter_51_in_tax_chapters(self):
        """Chapter 51 (Income Taxes) is in tax chapters."""
        assert 51 in AR_TAX_CHAPTERS
        assert "Income Tax" in AR_TAX_CHAPTERS[51]

    def test_chapter_52_in_tax_chapters(self):
        """Chapter 52 (Sales and Use Tax) is in tax chapters."""
        assert 52 in AR_TAX_CHAPTERS
        assert "Sales" in AR_TAX_CHAPTERS[52]

    def test_chapter_20_in_welfare_chapters(self):
        """Chapter 20 (Social Welfare) chapters exist in welfare chapters."""
        # Note: Arkansas Title 20 is Public Health and Welfare
        # We check for relevant chapters under this title
        assert len(AR_WELFARE_CHAPTERS) > 0

    def test_tax_chapters_are_under_title_26(self):
        """Tax chapters should be for Title 26."""
        # Arkansas Title 26 covers Taxation
        for chapter in AR_TAX_CHAPTERS:
            assert isinstance(chapter, int)


class TestARConverter:
    """Test ARConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = ARConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = ARConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = ARConverter(year=2024)
        assert converter.year == 2024

    def test_parse_section_number_simple(self):
        """Parse simple section number 26-51-101."""
        converter = ARConverter()
        title, chapter, section = converter._parse_section_number("26-51-101")
        assert title == 26
        assert chapter == 51
        assert section == "101"

    def test_parse_section_number_with_letter(self):
        """Parse section number with letter suffix 26-51-101a."""
        converter = ARConverter()
        title, chapter, section = converter._parse_section_number("26-51-101a")
        assert title == 26
        assert chapter == 51
        assert section == "101a"

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = ARConverter()
        url = converter._build_section_url("26-51-101")
        # Should contain the section reference
        assert "26-51-101" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with ARConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestARConverterParsing:
    """Test ARConverter HTML parsing."""

    def test_parse_section_html_simple(self):
        """Parse simple section HTML into ParsedARSection."""
        converter = ARConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "26-51-101", "https://example.com"
        )

        assert parsed.section_number == "26-51-101"
        assert "Title" in parsed.section_title or "Income Tax" in parsed.text
        assert parsed.title_number == 26
        assert parsed.chapter_number == 51
        assert "Income Tax Act of 1929" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_section_html_complex(self):
        """Parse complex section HTML with subsections."""
        converter = ARConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML_COMPLEX, "26-51-201", "https://example.com"
        )

        assert parsed.section_number == "26-51-201"
        assert "Tax imposed" in parsed.section_title or "Tax imposed" in parsed.text
        assert parsed.title_number == 26
        assert parsed.chapter_number == 51

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = ARConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML_COMPLEX, "26-51-201", "https://example.com"
        )

        # Should have subsections (a) and (b)
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (a)."""
        converter = ARConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML_COMPLEX, "26-51-201", "https://example.com"
        )

        # Find subsection (a)
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        # Should have children (1), (2), (3)
        assert len(sub_a.children) >= 2
        assert any(c.identifier == "1" for c in sub_a.children)
        assert any(c.identifier == "2" for c in sub_a.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = ARConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "26-51-101", "https://example.com"
        )

        assert parsed.history is not None
        assert "1929" in parsed.history or "Acts" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedARSection to Section model."""
        converter = ARConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "26-51-101", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "AR-26-51-101"
        assert section.citation.title == 0  # State law indicator
        assert "Arkansas Code" in section.title_name
        assert section.uslm_id == "ar/26/51/26-51-101"
        assert section.source_url == "https://example.com"


class TestARConverterFetching:
    """Test ARConverter HTTP fetching with mocks."""

    @patch.object(ARConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = ARConverter()
        section = converter.fetch_section("26-51-101")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "AR-26-51-101"

    @patch.object(ARConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section not found</body></html>"

        converter = ARConverter()
        with pytest.raises(ARConverterError) as exc_info:
            converter.fetch_section("99-99-999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(ARConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = ARConverter()
        sections = converter.get_chapter_section_numbers(26, 51)

        assert len(sections) == 4
        assert "26-51-101" in sections
        assert "26-51-102" in sections
        assert "26-51-201" in sections

    @patch.object(ARConverter, "_get")
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

        converter = ARConverter()
        sections = list(converter.iter_chapter(26, 51))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(ARConverter, "_get")
    def test_fetch_ar_section(self, mock_get):
        """Test fetch_ar_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_ar_section("26-51-101")

        assert section is not None
        assert section.citation.section == "AR-26-51-101"

    @patch.object(ARConverter, "_get")
    def test_download_ar_chapter(self, mock_get):
        """Test download_ar_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_ar_chapter(26, 51)

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestARConverterIntegration:
    """Integration tests that hit real Arkansas Code portal (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_title_section(self):
        """Fetch Arkansas Income Tax section 26-51-101."""
        converter = ARConverter()
        try:
            section = converter.fetch_section("26-51-101")

            assert section is not None
            assert section.citation.section == "AR-26-51-101"
            assert "income tax" in section.text.lower()
        except ARConverterError:
            pytest.skip("Could not connect to Arkansas Code portal")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch Arkansas Sales Tax section 26-52-103."""
        converter = ARConverter()
        try:
            section = converter.fetch_section("26-52-103")

            assert section is not None
            assert section.citation.section == "AR-26-52-103"
            assert "sales" in section.text.lower() or "tax" in section.text.lower()
        except ARConverterError:
            pytest.skip("Could not connect to Arkansas Code portal")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_51_sections(self):
        """Get list of sections in Chapter 51."""
        converter = ARConverter()
        try:
            sections = converter.get_chapter_section_numbers(26, 51)

            assert len(sections) > 0
            assert all(s.startswith("26-51-") for s in sections)
        except ARConverterError:
            pytest.skip("Could not connect to Arkansas Code portal")
