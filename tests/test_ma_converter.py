"""Tests for Massachusetts state statute converter.

Tests the MAConverter which fetches from malegislature.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.ma import (
    MA_TAX_CHAPTERS,
    MA_WELFARE_CHAPTERS,
    MAConverter,
    MAConverterError,
    download_ma_chapter,
    fetch_ma_section,
)
from axiom.models import Section

# Sample HTML from malegislature.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head>
<title>Section 2: Gross income, adjusted gross income and taxable income defined - Massachusetts General Laws</title>
</head>
<body>
<nav>Navigation</nav>
<main>
<h2>Section 2: Gross income, adjusted gross income and taxable income defined; classes</h2>
<div class="lawContent">
<p>[Text of section applicable as provided by 2021, 9, Secs. 8, 12 and 26.]</p>
<p>(a) "Gross income" shall mean gross income as defined under the provisions of the Federal Internal Revenue Code, as amended and in effect for the taxable year.</p>
<p>(b) "Adjusted gross income" shall mean adjusted gross income as defined under the provisions of the Federal Internal Revenue Code, with the following modifications:</p>
<p>(1) There shall be added interest income from obligations of any state other than Massachusetts;</p>
<p>(2) There shall be added income taxes imposed by this chapter;</p>
<p>(A) This includes state taxes withheld;</p>
<p>(B) This includes estimated tax payments;</p>
<p>(c) "Taxable income" shall mean the sum of the taxable income separately determined for each class of income.</p>
<p>Amended by 2021, 9, Sec. 8</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 62 - Taxation of Incomes</title></head>
<body>
<h1>Chapter 62 - Taxation of Incomes</h1>
<div id="contents">
<ul>
<li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section1">Section 1: Definitions</a></li>
<li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section2">Section 2: Gross income defined</a></li>
<li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section3">Section 3: Deductions</a></li>
<li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section4">Section 4: Tax rates</a></li>
<li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section5A">Section 5A: Exempt income</a></li>
</ul>
</div>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head>
<title>Section 1: Definitions - Massachusetts General Laws</title>
</head>
<body>
<main>
<h2>Section 1: Definitions</h2>
<div class="lawContent">
<p>As used in this chapter, the following words shall have the following meanings:</p>
<p>(a) "Department", the department of transitional assistance.</p>
<p>(b) "Commissioner", the commissioner of the department of transitional assistance.</p>
<p>(c) "Assistance", cash assistance, food assistance, or other benefits provided under this chapter.</p>
</div>
</main>
</body>
</html>
"""


class TestMAChaptersRegistry:
    """Test Massachusetts chapter registries."""

    def test_chapter_62_in_tax_chapters(self):
        """Chapter 62 (Income Tax) is in tax chapters."""
        assert "62" in MA_TAX_CHAPTERS
        assert "Income" in MA_TAX_CHAPTERS["62"]

    def test_chapter_64h_in_tax_chapters(self):
        """Chapter 64H (Sales Tax) is in tax chapters."""
        assert "64H" in MA_TAX_CHAPTERS
        assert "Sales" in MA_TAX_CHAPTERS["64H"]

    def test_chapter_118e_in_welfare_chapters(self):
        """Chapter 118E (Medical Assistance) is in welfare chapters."""
        assert "118E" in MA_WELFARE_CHAPTERS
        assert "Medical" in MA_WELFARE_CHAPTERS["118E"]

    def test_chapter_121_in_welfare_chapters(self):
        """Chapter 121 (Transitional Assistance) is in welfare chapters."""
        assert "121" in MA_WELFARE_CHAPTERS
        assert "Transitional" in MA_WELFARE_CHAPTERS["121"]

    def test_tax_chapters_include_key_chapters(self):
        """Tax chapters include key income and sales tax chapters."""
        assert "62" in MA_TAX_CHAPTERS  # Income tax
        assert "63" in MA_TAX_CHAPTERS  # Corporate tax
        assert "64H" in MA_TAX_CHAPTERS  # Sales tax
        assert "65C" in MA_TAX_CHAPTERS  # Estate tax


class TestMAConverter:
    """Test MAConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = MAConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = MAConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = MAConverter(year=2024)
        assert converter.year == 2024

    def test_get_chapter_info_tax(self):
        """Get correct part and title for tax chapters."""
        converter = MAConverter()
        part, title_roman, title_name = converter._get_chapter_info(62)
        assert part == "I"
        assert title_roman == "IX"
        assert title_name == "Taxation"

    def test_get_chapter_info_welfare(self):
        """Get correct part and title for welfare chapters."""
        converter = MAConverter()
        part, title_roman, title_name = converter._get_chapter_info(118)
        assert part == "I"
        assert title_roman == "XVII"
        assert title_name == "Public Welfare"

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = MAConverter()
        url = converter._build_section_url(62, 2)
        assert "malegislature.gov" in url
        assert "PartI" in url
        assert "TitleIX" in url
        assert "Chapter62" in url
        assert "Section2" in url

    def test_build_section_url_with_letter(self):
        """Build correct URL for section with letter suffix."""
        converter = MAConverter()
        url = converter._build_section_url("62B", "5A")
        assert "Chapter62B" in url
        assert "Section5A" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = MAConverter()
        url = converter._build_chapter_url(62)
        assert "Chapter62" in url
        assert "Section" not in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with MAConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestMAConverterParsing:
    """Test MAConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedMASection."""
        converter = MAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "62", "2", "https://example.com"
        )

        assert parsed.chapter_number == "62"
        assert parsed.section_number == "2"
        assert "Gross income" in parsed.section_title
        assert parsed.part == "I"
        assert parsed.title_roman == "IX"
        assert parsed.title_name == "Taxation"
        assert "Federal Internal Revenue Code" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = MAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "62", "2", "https://example.com"
        )

        # Should have subsections (a), (b), (c)
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)
        assert any(s.identifier == "c" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (b)."""
        converter = MAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "62", "2", "https://example.com"
        )

        # Find subsection (b)
        sub_b = next((s for s in parsed.subsections if s.identifier == "b"), None)
        assert sub_b is not None
        # Should have children (1) and (2)
        assert len(sub_b.children) >= 2
        assert any(c.identifier == "1" for c in sub_b.children)
        assert any(c.identifier == "2" for c in sub_b.children)

    def test_parse_level3_subsections(self):
        """Parse level 3 subsections (A), (B) under (2)."""
        converter = MAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "62", "2", "https://example.com"
        )

        # Find subsection (b)
        sub_b = next((s for s in parsed.subsections if s.identifier == "b"), None)
        assert sub_b is not None
        # Find subsection (2) under (b)
        sub_2 = next((c for c in sub_b.children if c.identifier == "2"), None)
        assert sub_2 is not None
        # Should have children (A) and (B)
        assert len(sub_2.children) >= 2
        assert any(c.identifier == "A" for c in sub_2.children)
        assert any(c.identifier == "B" for c in sub_2.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = MAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "62", "2", "https://example.com"
        )

        assert parsed.history is not None
        assert "2021" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedMASection to Section model."""
        converter = MAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "62", "2", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "MA-62-2"
        assert section.citation.title == 0  # State law indicator
        assert "Gross income" in section.section_title
        assert "Massachusetts General Laws" in section.title_name
        assert section.uslm_id == "ma/62/2"
        assert section.source_url == "https://example.com"


class TestMAConverterFetching:
    """Test MAConverter HTTP fetching with mocks."""

    @patch.object(MAConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = MAConverter()
        section = converter.fetch_section(62, 2)

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "MA-62-2"
        assert "Gross income" in section.section_title

    @patch.object(MAConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section not found</body></html>"

        converter = MAConverter()
        with pytest.raises(MAConverterError) as exc_info:
            converter.fetch_section(999, 999)

        assert "not found" in str(exc_info.value).lower()

    @patch.object(MAConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = MAConverter()
        sections = converter.get_chapter_section_numbers(62)

        assert len(sections) == 5
        assert "1" in sections
        assert "2" in sections
        assert "5A" in sections

    @patch.object(MAConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = MAConverter()
        sections = list(converter.iter_chapter(62))

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(MAConverter, "_get")
    def test_fetch_ma_section(self, mock_get):
        """Test fetch_ma_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_ma_section(62, 2)

        assert section is not None
        assert section.citation.section == "MA-62-2"

    @patch.object(MAConverter, "_get")
    def test_download_ma_chapter(self, mock_get):
        """Test download_ma_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_ma_chapter(62)

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestMAConverterWelfare:
    """Test MAConverter with welfare chapters."""

    @patch.object(MAConverter, "_get")
    def test_fetch_welfare_section(self, mock_get):
        """Fetch and parse a welfare chapter section."""
        mock_get.return_value = SAMPLE_WELFARE_SECTION_HTML

        converter = MAConverter()
        section = converter.fetch_section(121, 1)

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "MA-121-1"
        assert "Public Welfare" in section.title_name

    @patch.object(MAConverter, "_get")
    def test_welfare_chapter_url(self, mock_get):
        """Welfare chapters use correct Title XVII URL."""
        mock_get.return_value = SAMPLE_WELFARE_SECTION_HTML

        converter = MAConverter()
        url = converter._build_section_url(121, 1)

        assert "TitleXVII" in url
        assert "Chapter121" in url


class TestMAConverterIntegration:
    """Integration tests that hit real malegislature.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Massachusetts Income Tax section 62.2."""
        converter = MAConverter()
        section = converter.fetch_section(62, 2)

        assert section is not None
        assert section.citation.section == "MA-62-2"
        assert "gross income" in section.section_title.lower()
        assert "income" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_definitions_section(self):
        """Fetch Massachusetts Income Tax section 62.1 (Definitions)."""
        converter = MAConverter()
        section = converter.fetch_section(62, 1)

        assert section is not None
        assert section.citation.section == "MA-62-1"
        assert "definition" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_62_sections(self):
        """Get list of sections in Chapter 62."""
        converter = MAConverter()
        sections = converter.get_chapter_section_numbers(62)

        assert len(sections) > 0
        assert "1" in sections  # Definitions
        assert "2" in sections  # Gross income

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Massachusetts welfare section."""
        import httpx

        converter = MAConverter()
        try:
            section = converter.fetch_section(121, 1)
            assert section.citation.section == "MA-121-1"
        except (MAConverterError, httpx.HTTPStatusError):
            # Section may not exist or URL pattern may differ
            pytest.skip("Section 121.1 not found or URL pattern differs")
