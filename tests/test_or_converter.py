"""Tests for Oregon state statute converter.

Tests the ORConverter which fetches from oregonlegislature.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.or_ import (
    OR_TAX_CHAPTERS,
    OR_WELFARE_CHAPTERS,
    ORConverter,
    ORConverterError,
    download_or_chapter,
    fetch_or_section,
)
from axiom.models import Section

# Sample HTML from oregonlegislature.gov for testing
# This mimics the actual structure: MsoNormal paragraphs with bold section headers
SAMPLE_CHAPTER_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 316 - Personal Income Tax</title></head>
<body>
<div class=WordSection1>

<p class=MsoNormal align=center><span style='font-size:12.0pt;
font-family:"Times New Roman",serif'>Chapter 316 - Personal Income Tax</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>&nbsp;</span></p>

<p class=MsoNormal><b><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     316.002 Short title.</span></b><span
style='font-size:12.0pt;font-family:"Times New Roman",serif'> This chapter may be cited as the Personal Income Tax Act of 1969. [1969 c.493 s1; 1995 c.79 s164]</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>&nbsp;</span></p>

<p class=MsoNormal><b><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     316.003 Goals.</span></b><span
style='font-size:12.0pt;font-family:"Times New Roman",serif'> (1) The goals of the Legislative Assembly are to achieve for the people of this state a tax system that recognizes:</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (a) Fairness and equity as its basic values; and</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (b) That the total tax system should use seven guiding principles as measures by which to evaluate tax proposals.</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (2) The seven guiding principles are:</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (a) Reliability;</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (b) Broad base; and</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (c) Administrative simplicity. [1995 c.779 s1]</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>&nbsp;</span></p>

<p class=MsoNormal><b><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     316.005</span></b><span
style='font-size:12.0pt;font-family:"Times New Roman",serif'> [1953 c.304 s2; repealed by 1969 c.493 s99]</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>&nbsp;</span></p>

<p class=MsoNormal><b><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     316.037 Imposition and rate of tax.</span></b><span
style='font-size:12.0pt;font-family:"Times New Roman",serif'> (1)(a) A tax is imposed for each taxable year on the entire taxable income of every resident of this state. The amount of the tax shall be determined in accordance with the following table:</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>Not over $2,000 - 4.75% of taxable income</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>Over $2,000 but not over $5,000 - $95 plus 6.75% of the excess over $2,000</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>Over $5,000 but not over $125,000 - $298 plus 8.75% of the excess over $5,000</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>Over $125,000 - $10,798 plus 9.9% of the excess over $125,000</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (b) The Department of Revenue shall adjust the bracket amounts annually.</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (2) A tax is imposed for each taxable year on the taxable income of every part-year resident that is derived from sources within this state.</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (3) A tax is imposed for each taxable year on the taxable income of every full-year nonresident that is derived from sources within this state. [1969 c.493 s11; 1975 c.674 s1; 2019 c.122 s56]</span></p>

</div>
</body>
</html>
"""

# Sample HTML for welfare chapter (411)
SAMPLE_WELFARE_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 411 - Public Assistance</title></head>
<body>
<div class=WordSection1>

<p class=MsoNormal align=center><span style='font-size:12.0pt;
font-family:"Times New Roman",serif'>Chapter 411 - Public Assistance</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>&nbsp;</span></p>

<p class=MsoNormal><b><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     411.010 Definitions.</span></b><span
style='font-size:12.0pt;font-family:"Times New Roman",serif'> As used in this chapter, unless the context requires otherwise:</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (1) "Assistance" or "public assistance" means benefits that the Department of Human Services is authorized to provide to eligible persons.</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (2) "Department" means the Department of Human Services.</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (3) "Recipient" means a person who receives public assistance. [1941 c.356 s1; 2016 c.93 s3]</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>&nbsp;</span></p>

<p class=MsoNormal><b><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     411.020 Powers and duties of department.</span></b><span
style='font-size:12.0pt;font-family:"Times New Roman",serif'> The department shall:</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (1) Administer all public assistance programs authorized by this chapter;</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (2) Adopt rules necessary for the administration of this chapter; and</span></p>

<p class=MsoNormal><span style='font-size:12.0pt;font-family:"Times New Roman",serif'>     (3) Determine eligibility for public assistance. [1941 c.356 s2; 2001 c.900 s123]</span></p>

</div>
</body>
</html>
"""


class TestORChaptersRegistry:
    """Test Oregon chapter registries."""

    def test_chapter_316_in_tax_chapters(self):
        """Chapter 316 (Personal Income Tax) is in tax chapters."""
        assert "316" in OR_TAX_CHAPTERS
        assert "Personal Income Tax" in OR_TAX_CHAPTERS["316"]

    def test_chapter_317_in_tax_chapters(self):
        """Chapter 317 (Corporation Excise Tax) is in tax chapters."""
        assert "317" in OR_TAX_CHAPTERS
        assert "Corporation" in OR_TAX_CHAPTERS["317"]

    def test_chapter_411_in_welfare_chapters(self):
        """Chapter 411 (Public Assistance) is in welfare chapters."""
        assert "411" in OR_WELFARE_CHAPTERS
        assert "Public Assistance" in OR_WELFARE_CHAPTERS["411"]

    def test_tax_chapters_range(self):
        """Tax chapters are in expected range 305-323."""
        for chapter in OR_TAX_CHAPTERS:
            # Skip letter-suffix chapters like "308A", "317A"
            if chapter.isdigit():
                assert 305 <= int(chapter) <= 323


class TestORConverter:
    """Test ORConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = ORConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = ORConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = ORConverter(year=2024)
        assert converter.year == 2024

    def test_build_chapter_url(self):
        """Build correct URL for chapter fetch."""
        converter = ORConverter()
        url = converter._build_chapter_url(316)
        assert "oregonlegislature.gov" in url
        assert "ors316.html" in url

    def test_build_chapter_url_welfare(self):
        """Build correct URL for welfare chapter."""
        converter = ORConverter()
        url = converter._build_chapter_url(411)
        assert "ors411.html" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with ORConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None

    def test_normalize_text(self):
        """Normalize text handles unicode spaces and special chars."""
        converter = ORConverter()
        # Unicode non-breaking spaces
        text = "Hello\xa0\xa0World"
        assert converter._normalize_text(text) == "Hello World"
        # Section symbol
        text = "See ORS 316.037 s1"
        normalized = converter._normalize_text(text)
        assert "s1" in normalized


class TestORConverterParsing:
    """Test ORConverter HTML parsing."""

    def test_parse_chapter_html(self):
        """Parse chapter HTML into sections."""
        converter = ORConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_CHAPTER_HTML, 316, "https://example.com"
        )

        # Should have multiple sections (including repealed)
        assert len(sections) >= 3

        # Check for specific sections
        section_nums = [s.section_number for s in sections]
        assert "316.002" in section_nums
        assert "316.003" in section_nums
        assert "316.037" in section_nums

    def test_parse_section_title(self):
        """Parse section title from HTML."""
        converter = ORConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_CHAPTER_HTML, 316, "https://example.com"
        )

        # Find 316.002
        section_002 = next(s for s in sections if s.section_number == "316.002")
        assert section_002.section_title == "Short title"

        # Find 316.037
        section_037 = next(s for s in sections if s.section_number == "316.037")
        assert section_037.section_title == "Imposition and rate of tax"

    def test_parse_section_text(self):
        """Parse section body text."""
        converter = ORConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_CHAPTER_HTML, 316, "https://example.com"
        )

        section_002 = next(s for s in sections if s.section_number == "316.002")
        assert "Personal Income Tax Act" in section_002.text

        section_037 = next(s for s in sections if s.section_number == "316.037")
        assert "taxable income" in section_037.text
        assert "4.75%" in section_037.text

    def test_parse_repealed_section(self):
        """Parse repealed section."""
        converter = ORConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_CHAPTER_HTML, 316, "https://example.com"
        )

        # Find repealed section 316.005
        section_005 = next(
            (s for s in sections if s.section_number == "316.005"), None
        )
        assert section_005 is not None
        assert section_005.is_repealed is True
        assert "repealed" in section_005.history.lower()

    def test_parse_subsections(self):
        """Parse subsections from section text."""
        converter = ORConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_CHAPTER_HTML, 316, "https://example.com"
        )

        # 316.003 has subsections (1) and (2)
        section_003 = next(s for s in sections if s.section_number == "316.003")
        assert len(section_003.subsections) >= 2
        assert any(s.identifier == "1" for s in section_003.subsections)
        assert any(s.identifier == "2" for s in section_003.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under (1)."""
        converter = ORConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_CHAPTER_HTML, 316, "https://example.com"
        )

        section_003 = next(s for s in sections if s.section_number == "316.003")

        # Find subsection (1)
        sub_1 = next((s for s in section_003.subsections if s.identifier == "1"), None)
        assert sub_1 is not None
        # Should have children (a) and (b)
        assert len(sub_1.children) >= 2
        assert any(c.identifier == "a" for c in sub_1.children)
        assert any(c.identifier == "b" for c in sub_1.children)

    def test_parse_history(self):
        """Parse history note from section."""
        converter = ORConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_CHAPTER_HTML, 316, "https://example.com"
        )

        section_002 = next(s for s in sections if s.section_number == "316.002")
        assert section_002.history is not None
        assert "1969" in section_002.history

    def test_parse_welfare_chapter(self):
        """Parse welfare chapter HTML."""
        converter = ORConverter()
        sections = converter._parse_chapter_html(
            SAMPLE_WELFARE_HTML, 411, "https://example.com"
        )

        assert len(sections) >= 2
        section_nums = [s.section_number for s in sections]
        assert "411.010" in section_nums
        assert "411.020" in section_nums

        # Check definitions section
        section_010 = next(s for s in sections if s.section_number == "411.010")
        assert section_010.section_title == "Definitions"
        assert "Department of Human Services" in section_010.text

    def test_to_section_model(self):
        """Convert ParsedORSection to Section model."""
        converter = ORConverter()
        parsed = converter._parse_chapter_html(
            SAMPLE_CHAPTER_HTML, 316, "https://example.com"
        )
        section_037 = next(p for p in parsed if p.section_number == "316.037")
        section = converter._to_section(section_037)

        assert isinstance(section, Section)
        assert section.citation.section == "OR-316.037"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Imposition and rate of tax"
        assert "Oregon Revised Statutes" in section.title_name
        assert section.uslm_id == "or/316/316.037"
        assert section.source_url == "https://example.com"


class TestORConverterFetching:
    """Test ORConverter HTTP fetching with mocks."""

    @patch.object(ORConverter, "_get")
    def test_fetch_chapter(self, mock_get):
        """Fetch and parse an entire chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = ORConverter()
        sections = converter.fetch_chapter(316)

        # Should exclude repealed sections by default
        assert len(sections) >= 3
        assert all(isinstance(s, Section) for s in sections)
        # Should not include repealed 316.005
        assert all("316.005" not in s.citation.section for s in sections)

    @patch.object(ORConverter, "_get")
    def test_fetch_chapter_with_repealed(self, mock_get):
        """Fetch chapter including repealed sections."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = ORConverter()
        sections = converter.fetch_chapter(316, include_repealed=True)

        # Should include repealed section
        section_nums = [s.citation.section for s in sections]
        assert any("316.005" in num for num in section_nums)

    @patch.object(ORConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch a single section."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = ORConverter()
        section = converter.fetch_section("316.037")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "OR-316.037"
        assert "Imposition" in section.section_title

    @patch.object(ORConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = ORConverter()
        with pytest.raises(ORConverterError) as exc_info:
            converter.fetch_section("316.999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(ORConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = ORConverter()
        sections = converter.get_chapter_section_numbers(316)

        assert len(sections) >= 3
        assert "316.002" in sections
        assert "316.003" in sections
        assert "316.037" in sections
        # Should exclude repealed sections
        assert "316.005" not in sections

    @patch.object(ORConverter, "_get")
    def test_chapter_caching(self, mock_get):
        """Chapter data is cached to avoid repeated fetches."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = ORConverter()

        # First fetch
        converter.fetch_chapter(316)
        assert mock_get.call_count == 1

        # Second fetch should use cache
        converter.fetch_chapter(316)
        assert mock_get.call_count == 1  # Still 1

        # Different chapter triggers new fetch
        mock_get.return_value = SAMPLE_WELFARE_HTML
        converter.fetch_chapter(411)
        assert mock_get.call_count == 2

    @patch.object(ORConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = ORConverter()
        sections = list(converter.iter_chapter(316))

        assert len(sections) >= 3
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(ORConverter, "_get")
    def test_fetch_or_section(self, mock_get):
        """Test fetch_or_section function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        section = fetch_or_section("316.037")

        assert section is not None
        assert section.citation.section == "OR-316.037"

    @patch.object(ORConverter, "_get")
    def test_download_or_chapter(self, mock_get):
        """Test download_or_chapter function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        sections = download_or_chapter(316)

        assert len(sections) >= 3
        assert all(isinstance(s, Section) for s in sections)


class TestORConverterIntegration:
    """Integration tests that hit real oregonlegislature.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_chapter_316(self):
        """Fetch Oregon Personal Income Tax chapter 316."""
        converter = ORConverter()
        sections = converter.fetch_chapter(316)

        assert len(sections) > 0
        # All sections should be from chapter 316
        assert all("316." in s.citation.section for s in sections)
        # Should find the imposition of tax section
        assert any("316.037" in s.citation.section for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_section_316_037(self):
        """Fetch specific Oregon income tax rate section."""
        converter = ORConverter()
        section = converter.fetch_section("316.037")

        assert section is not None
        assert section.citation.section == "OR-316.037"
        assert "Imposition" in section.section_title.lower() or "rate" in section.section_title.lower()
        # Should contain tax rate information
        assert any(x in section.text.lower() for x in ["percent", "%", "tax"])

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_chapter_411(self):
        """Fetch Oregon Public Assistance chapter 411."""
        converter = ORConverter()
        sections = converter.fetch_chapter(411)

        assert len(sections) > 0
        assert all("411." in s.citation.section for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_section_numbers(self):
        """Get list of sections in Chapter 316."""
        converter = ORConverter()
        section_nums = converter.get_chapter_section_numbers(316)

        assert len(section_nums) > 0
        assert all(s.startswith("316.") for s in section_nums)
        # Should include key sections
        assert "316.037" in section_nums  # Tax imposition
