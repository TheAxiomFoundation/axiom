"""Tests for Montana state statute converter.

Tests the MTConverter which fetches from archive.legmt.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.mt import (
    MT_TAX_CHAPTERS,
    MT_WELFARE_CHAPTERS,
    MTConverter,
    MTConverterError,
    download_mt_chapter,
    fetch_mt_section,
)
from axiom_corpus.models import Section

# Sample HTML from archive.legmt.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>15-30-2101. Definitions, MCA</title>
</head>
<body class="section-doc">
    <ol class="breadcrumb">
        <li><a href="../../../../index.html" title="MCA Table of Contents">MCA Contents</a></li>
        <li><a href="../../../chapters_index.html" title="TITLE 15. TAXATION">TITLE 15</a></li>
        <li><a href="../../parts_index.html" title="CHAPTER 30. INDIVIDUAL INCOME TAX">CHAPTER 30</a></li>
        <li><a href="../sections_index.html" title="Part 21. Rate and General Provisions">Part 21</a></li>
        <li class="active"><span title="15-30-2101 Definitions">15-30-2101 Definitions</span></li>
    </ol>
    <div class="mca-content mca-toc">
        <h1>Montana Code Annotated 2023</h1>
        <div class="section-header">
            <h4 class="section-title-title">TITLE 15. TAXATION</h4>
            <h3 class="section-chapter-title">CHAPTER 30. INDIVIDUAL INCOME TAX</h3>
            <h2 class="section-part-title">Part 21. Rate and General Provisions</h2>
            <h1 class="section-section-title">Definitions</h1>
        </div>
        <div class="section-doc" id="mca_0150-0300-0210-0010">
            <div class="section-content">
                <p class="line-indent">
                    <span class="catchline"><span class="citation">15-30-2101</span>.&#8195;Definitions.</span> For the purpose of this chapter, unless otherwise required by the context, the following definitions apply:
                </p>
                <p class="line-indent">
                    (1) "Consumer price index" means the consumer price index, United States city average, for all items, for all urban consumers (CPI-U).
                </p>
                <p class="line-indent">
                    (2) "Corporation" or "C. corporation" means a corporation, limited liability company, or other entity:
                </p>
                <p class="line-indent">
                    (a) that is treated as an association for federal income tax purposes;
                </p>
                <p class="line-indent">
                    (b) for which a valid election under section 1362 of the Internal Revenue Code (26 U.S.C. 1362) is not in effect; and
                </p>
                <p class="line-indent">
                    (c) that is not a disregarded entity.
                </p>
                <p class="line-indent">
                    (3) "Department" means the department of revenue.
                </p>
            </div>
        </div>
        <div class="history-doc" id="mca_0150-0300-0210-0010_hist">
            <div class="history-content">
                <p class="line-indent">
                    <span class="header">History:</span>&#8195;En. Sec. 1, Ch. 181, L. 1933; amd. Sec. 8, Ch. 503, L. 2021.
                </p>
            </div>
        </div>
    </div>
</body>
</html>
"""

SAMPLE_SECTIONS_INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Part 21. Rate and General Provisions - Table of Contents, Title 15, Chapter 30, MCA</title>
</head>
<body class="section-doc">
    <div class="mca-content mca-toc">
        <h1>Montana Code Annotated 2023</h1>
        <div class="section-index-header">
            <h3 class="section-title-title">TITLE 15. TAXATION</h3>
            <h2 class="section-chapter-title">CHAPTER 30. INDIVIDUAL INCOME TAX</h2>
            <h1 class="section-part-title">Part 21. Rate and General Provisions</h1>
        </div>
        <div class="section-toc-content">
            <ul class="section-list">
                <li class="line">
                    <a href="./section_0010/0150-0300-0210-0010.html"><span class="citation">15-30-2101</span>&nbsp;Definitions</a>
                </li>
                <li class="line">
                    <a href="./section_0020/0150-0300-0210-0020.html"><span class="citation">15-30-2102</span>&nbsp;Construction of income</a>
                </li>
                <li class="line">
                    <a href="./section_0030/0150-0300-0210-0030.html"><span class="citation">15-30-2103</span>&nbsp;Tax rate on capital gains</a>
                </li>
            </ul>
        </div>
    </div>
</body>
</html>
"""

SAMPLE_PARTS_INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>CHAPTER 30. INDIVIDUAL INCOME TAX - Table of Contents, Title 15, MCA</title>
</head>
<body>
    <div class="mca-content mca-toc">
        <h1>Montana Code Annotated 2023</h1>
        <div class="chapter-toc-content">
            <ul class="part-list">
                <li class="line">
                    <a href="./part_0210/sections_index.html">Part 21. Rate and General Provisions</a>
                </li>
                <li class="line">
                    <a href="./part_0230/sections_index.html">Part 23. Tax Credits</a>
                </li>
                <li class="line">
                    <a href="./part_0250/sections_index.html">Part 25. Estimated Tax</a>
                </li>
            </ul>
        </div>
    </div>
</body>
</html>
"""


class TestMTChaptersRegistry:
    """Test Montana chapter registries."""

    def test_chapter_15_30_in_tax_chapters(self):
        """Chapter 15-30 (Individual Income Tax) is in tax chapters."""
        assert (15, 30) in MT_TAX_CHAPTERS
        assert "Individual Income Tax" in MT_TAX_CHAPTERS[(15, 30)]

    def test_chapter_15_31_in_tax_chapters(self):
        """Chapter 15-31 (Corporate Income Tax) is in tax chapters."""
        assert (15, 31) in MT_TAX_CHAPTERS
        assert "Corporate" in MT_TAX_CHAPTERS[(15, 31)]

    def test_chapter_53_2_in_welfare_chapters(self):
        """Chapter 53-2 (Public Assistance) is in welfare chapters."""
        assert (53, 2) in MT_WELFARE_CHAPTERS
        assert "Public Assistance" in MT_WELFARE_CHAPTERS[(53, 2)]


class TestMTConverter:
    """Test MTConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = MTConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = MTConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = MTConverter(year=2024)
        assert converter.year == 2024

    def test_parse_section_citation(self):
        """Parse section citation like '15-30-2101' correctly."""
        converter = MTConverter()
        title, chapter, part, section = converter._parse_section_citation("15-30-2101")
        assert title == 15
        assert chapter == 30
        assert part == 21
        assert section == 1

    def test_parse_section_citation_three_digit(self):
        """Parse three-digit section number."""
        converter = MTConverter()
        title, chapter, part, section = converter._parse_section_citation("15-30-2120")
        assert title == 15
        assert chapter == 30
        assert part == 21
        assert section == 20

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = MTConverter()
        url = converter._build_section_url("15-30-2101")
        assert "archive.legmt.gov/bills/mca" in url
        assert "title_0150" in url
        assert "chapter_0300" in url
        assert "part_0210" in url
        assert "section_0010" in url
        assert "0150-0300-0210-0010.html" in url

    def test_build_parts_index_url(self):
        """Build correct URL for chapter parts index."""
        converter = MTConverter()
        url = converter._build_parts_index_url(15, 30)
        assert "title_0150/chapter_0300/parts_index.html" in url

    def test_build_sections_index_url(self):
        """Build correct URL for part sections index."""
        converter = MTConverter()
        url = converter._build_sections_index_url(15, 30, 21)
        assert "title_0150/chapter_0300/part_0210/sections_index.html" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with MTConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestMTConverterParsing:
    """Test MTConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedMTSection."""
        converter = MTConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "15-30-2101", "https://example.com")

        assert parsed.section_number == "15-30-2101"
        assert parsed.section_title == "Definitions"
        assert parsed.title_number == 15
        assert parsed.title_name == "TAXATION"
        assert parsed.chapter_number == 30
        assert parsed.chapter_title == "INDIVIDUAL INCOME TAX"
        assert parsed.part_number == 21
        assert parsed.part_title == "Rate and General Provisions"
        assert "definitions apply" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = MTConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "15-30-2101", "https://example.com")

        # Should have subsections (1), (2), (3)
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)
        assert any(s.identifier == "3" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b), (c) under (2)."""
        converter = MTConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "15-30-2101", "https://example.com")

        # Find subsection (2)
        sub_2 = next((s for s in parsed.subsections if s.identifier == "2"), None)
        assert sub_2 is not None
        # Should have children (a), (b), (c)
        assert len(sub_2.children) >= 3
        assert any(c.identifier == "a" for c in sub_2.children)
        assert any(c.identifier == "b" for c in sub_2.children)
        assert any(c.identifier == "c" for c in sub_2.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = MTConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "15-30-2101", "https://example.com")

        assert parsed.history is not None
        assert "Ch. 181" in parsed.history
        assert "L. 1933" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedMTSection to Section model."""
        converter = MTConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "15-30-2101", "https://example.com")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "MT-15-30-2101"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "Montana Code Annotated" in section.title_name
        assert section.uslm_id == "mt/15/30/15-30-2101"
        assert section.source_url == "https://example.com"


class TestMTConverterFetching:
    """Test MTConverter HTTP fetching with mocks."""

    @patch.object(MTConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = MTConverter()
        section = converter.fetch_section("15-30-2101")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "MT-15-30-2101"
        assert "Definitions" in section.section_title

    @patch.object(MTConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><head><title>Page Not Found</title></head><body>The requested page could not be found.</body></html>"

        converter = MTConverter()
        with pytest.raises(MTConverterError) as exc_info:
            converter.fetch_section("99-99-9999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(MTConverter, "_get")
    def test_get_part_section_numbers(self, mock_get):
        """Get list of section numbers from part index."""
        mock_get.return_value = SAMPLE_SECTIONS_INDEX_HTML

        converter = MTConverter()
        sections = converter.get_part_section_numbers(15, 30, 21)

        assert len(sections) == 3
        assert "15-30-2101" in sections
        assert "15-30-2102" in sections
        assert "15-30-2103" in sections

    @patch.object(MTConverter, "_get")
    def test_get_chapter_parts(self, mock_get):
        """Get list of parts in a chapter."""
        mock_get.return_value = SAMPLE_PARTS_INDEX_HTML

        converter = MTConverter()
        parts = converter.get_chapter_parts(15, 30)

        assert len(parts) == 3
        assert 21 in parts
        assert 23 in parts
        assert 25 in parts

    @patch.object(MTConverter, "_get")
    def test_iter_part(self, mock_get):
        """Iterate over sections in a part."""
        # First call returns sections index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_SECTIONS_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = MTConverter()
        sections = list(converter.iter_part(15, 30, 21))

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(MTConverter, "_get")
    def test_fetch_mt_section(self, mock_get):
        """Test fetch_mt_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_mt_section("15-30-2101")

        assert section is not None
        assert section.citation.section == "MT-15-30-2101"

    @patch.object(MTConverter, "_get")
    def test_download_mt_chapter(self, mock_get):
        """Test download_mt_chapter function."""
        # Parts index returns 3 parts (21, 23, 25), each part needs sections index + sections
        mock_get.side_effect = [
            SAMPLE_PARTS_INDEX_HTML,  # get_chapter_parts
            # Part 21
            SAMPLE_SECTIONS_INDEX_HTML,  # get_part_section_numbers for part 21
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            # Part 23
            SAMPLE_SECTIONS_INDEX_HTML,  # get_part_section_numbers for part 23
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            # Part 25
            SAMPLE_SECTIONS_INDEX_HTML,  # get_part_section_numbers for part 25
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_mt_chapter(15, 30)

        # 3 parts x 3 sections each = 9 sections
        assert len(sections) == 9
        assert all(isinstance(s, Section) for s in sections)


class TestMTConverterIntegration:
    """Integration tests that hit real archive.legmt.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_definitions(self):
        """Fetch Montana Income Tax definitions section 15-30-2101."""
        converter = MTConverter()
        section = converter.fetch_section("15-30-2101")

        assert section is not None
        assert section.citation.section == "MT-15-30-2101"
        assert "definitions" in section.section_title.lower()
        assert "income" in section.text.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_rate_section(self):
        """Fetch Montana tax rate section 15-30-2103."""
        converter = MTConverter()
        section = converter.fetch_section("15-30-2103")

        assert section is not None
        assert section.citation.section == "MT-15-30-2103"

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_30_parts(self):
        """Get list of parts in Chapter 30."""
        converter = MTConverter()
        parts = converter.get_chapter_parts(15, 30)

        assert len(parts) > 0
        assert 21 in parts  # Rate and General Provisions

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_part_21_sections(self):
        """Get list of sections in Part 21."""
        converter = MTConverter()
        sections = converter.get_part_section_numbers(15, 30, 21)

        assert len(sections) > 0
        assert any("15-30-2101" in s for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Montana welfare section from Title 53."""
        converter = MTConverter()
        try:
            # 53-2-201 is "Departmental duties"
            section = converter.fetch_section("53-2-201")
            assert section.citation.section == "MT-53-2-201"
        except MTConverterError:
            # Section may not exist in expected format
            pytest.skip("Section 53-2-201 not found or format changed")
