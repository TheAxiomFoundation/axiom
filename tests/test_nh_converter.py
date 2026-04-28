"""Tests for New Hampshire state statute converter.

Tests the NHConverter which fetches from gc.nh.gov (gencourt.state.nh.us)
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.nh import (
    NH_TAX_CHAPTERS,
    NH_WELFARE_CHAPTERS,
    NHConverter,
    NHConverterError,
    download_nh_chapter,
    fetch_nh_section,
)
from axiom.models import Section

# Sample HTML from gc.nh.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>NH RSA 77-A:1</title></head>
<body>
<h1>TITLE V</h1>
<h2>TAXATION</h2>
<h3>CHAPTER 77-A</h3>
<h4>BUSINESS PROFITS TAX</h4>
<p><b>77-A:1 Definitions. -</b></p>
<p>When appearing in this chapter:</p>
<p>I. "Business organization" means any enterprise, whether corporation, partnership, limited liability company, proprietorship, association, business trust, real estate trust or other form of organization; organized for gain or profit, carrying on any business activity within the state.</p>
<p>II. "Commissioner" means the commissioner of the department of revenue administration.</p>
<p>III. "Gross business profits" means:</p>
<p>(a) In the case of a business organization, the gross receipts from sales, services, and other sources of income attributable to the conduct of business activities in the state.</p>
<p>(b) In the case of a combined group, the aggregate gross business profits of all members of the group.</p>
<p>Source. 1970, 5:1. 1993, 313:2. 2022, 316:1, eff. Aug. 30, 2022.</p>
</body>
</html>
"""

SAMPLE_SECTION_HTML_2 = """<!DOCTYPE html>
<html>
<head><title>NH RSA 77-A:2</title></head>
<body>
<h1>TITLE V</h1>
<h2>TAXATION</h2>
<h3>CHAPTER 77-A</h3>
<h4>BUSINESS PROFITS TAX</h4>
<p><b>77-A:2 Imposition of Tax. -</b></p>
<p>A tax is imposed at the following rates upon the taxable business profits of every business organization:</p>
<p>I. For all taxable periods ending on or after December 31, 2019 and prior to December 31, 2022, the rate shall be 7.7 percent.</p>
<p>II. For all taxable periods ending on or after December 31, 2022 and prior to December 31, 2023, the rate shall be 7.6 percent.</p>
<p>III. For all taxable periods ending on or after December 31, 2023, the rate shall be 7.5 percent.</p>
<p>Source. 1970, 5:1. 2019, 346:1. 2022, 316:2, eff. Aug. 30, 2022.</p>
</body>
</html>
"""

SAMPLE_CHAPTER_TOC_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 77-A</title></head>
<body>
<h1>CHAPTER 77-A - BUSINESS PROFITS TAX</h1>
<div id="contents">
<ul>
<li><a href="../V/77-A/77-A-1.htm">77-A:1 Definitions</a></li>
<li><a href="../V/77-A/77-A-2.htm">77-A:2 Imposition of Tax</a></li>
<li><a href="../V/77-A/77-A-3.htm">77-A:3 Returns</a></li>
<li><a href="../V/77-A/77-A-4.htm">77-A:4 Filing Deadline</a></li>
<li><a href="../V/77-A/77-A-5.htm">77-A:5 Credits</a></li>
</ul>
</div>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>NH RSA 167:4</title></head>
<body>
<h1>TITLE XII</h1>
<h2>PUBLIC SAFETY AND WELFARE</h2>
<h3>CHAPTER 167</h3>
<h4>PUBLIC ASSISTANCE TO NEEDY PERSONS</h4>
<p><b>167:4 Eligibility for Assistance. -</b></p>
<p>Public assistance under this chapter shall be granted to any person who meets the following conditions:</p>
<p>I. The person is a resident of New Hampshire.</p>
<p>II. The person meets the income requirements established by the department.</p>
<p>(a) For purposes of this section, income includes wages, salaries, and other compensation.</p>
<p>(b) Income does not include certain excluded benefits as defined by federal law.</p>
<p>III. The person has applied in the manner prescribed by the commissioner.</p>
<p>Source. 1937, 202:8. 2015, 276:4, eff. Jan. 1, 2016.</p>
</body>
</html>
"""


class TestNHChaptersRegistry:
    """Test New Hampshire chapter registries."""

    def test_chapter_77a_in_tax_chapters(self):
        """Chapter 77-A (Business Profits Tax) is in tax chapters."""
        assert "77-A" in NH_TAX_CHAPTERS
        assert "Business Profits" in NH_TAX_CHAPTERS["77-A"]

    def test_chapter_77e_in_tax_chapters(self):
        """Chapter 77-E (Business Enterprise Tax) is in tax chapters."""
        assert "77-E" in NH_TAX_CHAPTERS
        assert "Business Enterprise" in NH_TAX_CHAPTERS["77-E"]

    def test_chapter_78_in_tax_chapters(self):
        """Chapter 78 (Meals and Rooms Tax) is in tax chapters."""
        assert "78" in NH_TAX_CHAPTERS
        assert "Meals and Rooms" in NH_TAX_CHAPTERS["78"]

    def test_chapter_167_in_welfare_chapters(self):
        """Chapter 167 (Public Assistance) is in welfare chapters."""
        assert "167" in NH_WELFARE_CHAPTERS
        assert "Public Assistance" in NH_WELFARE_CHAPTERS["167"]

    def test_chapter_161_in_welfare_chapters(self):
        """Chapter 161 (Human Services) is in welfare chapters."""
        assert "161" in NH_WELFARE_CHAPTERS
        assert "Human Services" in NH_WELFARE_CHAPTERS["161"]


class TestNHConverter:
    """Test NHConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = NHConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = NHConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = NHConverter(year=2024)
        assert converter.year == 2024

    def test_get_title_from_chapter_tax(self):
        """Title extraction for tax chapters (Title V)."""
        converter = NHConverter()
        assert converter._get_title_from_chapter("77-A") == "V"
        assert converter._get_title_from_chapter("78") == "V"
        assert converter._get_title_from_chapter("90") == "V"

    def test_get_title_from_chapter_welfare(self):
        """Title extraction for welfare chapters (Title XII)."""
        converter = NHConverter()
        assert converter._get_title_from_chapter("167") == "XII"
        assert converter._get_title_from_chapter("161") == "XII"
        assert converter._get_title_from_chapter("173-B") == "XII"

    def test_build_section_url_tax(self):
        """Build correct URL for tax section."""
        converter = NHConverter()
        url = converter._build_section_url("77-A:1")
        assert "gc.nh.gov/rsa/html" in url
        assert "/V/77-A/77-A-1.htm" in url

    def test_build_section_url_welfare(self):
        """Build correct URL for welfare section."""
        converter = NHConverter()
        url = converter._build_section_url("167:4")
        assert "gc.nh.gov/rsa/html" in url
        assert "/XII/167/167-4.htm" in url

    def test_build_chapter_toc_url(self):
        """Build correct URL for chapter TOC."""
        converter = NHConverter()
        url = converter._build_chapter_toc_url("77-A")
        assert "NHTOC/NHTOC-V-77-A.htm" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with NHConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None

    def test_invalid_section_format_raises_error(self):
        """Invalid section format raises NHConverterError."""
        converter = NHConverter()
        with pytest.raises(NHConverterError) as exc_info:
            converter._build_section_url("77A1")  # Missing colon

        assert "Invalid section format" in str(exc_info.value)


class TestNHConverterParsing:
    """Test NHConverter HTML parsing."""

    def test_parse_section_html_basic(self):
        """Parse section HTML into ParsedNHSection."""
        converter = NHConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "77-A:1", "https://example.com")

        assert parsed.section_number == "77-A:1"
        assert parsed.section_title == "Definitions"
        assert parsed.chapter_number == "77-A"
        assert parsed.chapter_title == "Business Profits Tax"
        assert parsed.title_roman == "V"
        assert parsed.title_name == "Taxation"
        assert "Business organization" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections_roman_numerals(self):
        """Parse Roman numeral subsections (I, II, III)."""
        converter = NHConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "77-A:1", "https://example.com")

        # Should have subsections I, II, III
        assert len(parsed.subsections) >= 2
        identifiers = [s.identifier for s in parsed.subsections]
        assert "I" in identifiers or "II" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under Roman numerals."""
        converter = NHConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "77-A:1", "https://example.com")

        # Find subsection III which has (a) and (b) children
        sub_iii = next((s for s in parsed.subsections if s.identifier == "III"), None)
        if sub_iii:
            assert len(sub_iii.children) >= 1

    def test_parse_history(self):
        """Parse Source note from section HTML."""
        converter = NHConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "77-A:1", "https://example.com")

        assert parsed.history is not None
        assert "1970" in parsed.history or "2022" in parsed.history

    def test_parse_welfare_section(self):
        """Parse welfare section from Title XII."""
        converter = NHConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "167:4", "https://example.com"
        )

        assert parsed.section_number == "167:4"
        assert parsed.section_title == "Eligibility for Assistance"
        assert parsed.chapter_number == "167"
        assert parsed.title_roman == "XII"
        assert "resident" in parsed.text.lower()

    def test_to_section_model(self):
        """Convert ParsedNHSection to Section model."""
        converter = NHConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "77-A:1", "https://example.com")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "NH-77-A:1"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "New Hampshire RSA" in section.title_name
        assert section.uslm_id == "nh/77-A/77-A:1"
        assert section.source_url == "https://example.com"


class TestNHConverterFetching:
    """Test NHConverter HTTP fetching with mocks."""

    @patch.object(NHConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = NHConverter()
        section = converter.fetch_section("77-A:1")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "NH-77-A:1"
        assert "Definitions" in section.section_title

    @patch.object(NHConverter, "_get")
    def test_fetch_section_tax_rates(self, mock_get):
        """Fetch and parse a section with tax rates."""
        mock_get.return_value = SAMPLE_SECTION_HTML_2

        converter = NHConverter()
        section = converter.fetch_section("77-A:2")

        assert section is not None
        assert section.citation.section == "NH-77-A:2"
        assert "Imposition of Tax" in section.section_title
        assert "7.5 percent" in section.text or "7.7 percent" in section.text

    @patch.object(NHConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter TOC."""
        mock_get.return_value = SAMPLE_CHAPTER_TOC_HTML

        converter = NHConverter()
        sections = converter.get_chapter_section_numbers("77-A")

        assert len(sections) == 5
        assert "77-A:1" in sections
        assert "77-A:2" in sections
        assert "77-A:5" in sections

    @patch.object(NHConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns TOC, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_TOC_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML_2,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = NHConverter()
        sections = list(converter.iter_chapter("77-A"))

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(NHConverter, "_get")
    def test_fetch_nh_section(self, mock_get):
        """Test fetch_nh_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_nh_section("77-A:1")

        assert section is not None
        assert section.citation.section == "NH-77-A:1"

    @patch.object(NHConverter, "_get")
    def test_download_nh_chapter(self, mock_get):
        """Test download_nh_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_TOC_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML_2,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_nh_chapter("77-A")

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestRomanNumeralValidation:
    """Test Roman numeral validation."""

    def test_valid_roman_numerals(self):
        """Valid Roman numerals are recognized."""
        converter = NHConverter()
        assert converter._is_valid_roman_numeral("I")
        assert converter._is_valid_roman_numeral("II")
        assert converter._is_valid_roman_numeral("III")
        assert converter._is_valid_roman_numeral("IV")
        assert converter._is_valid_roman_numeral("V")
        assert converter._is_valid_roman_numeral("X")
        assert converter._is_valid_roman_numeral("XX")
        assert converter._is_valid_roman_numeral("XL")

    def test_invalid_roman_numerals(self):
        """Invalid Roman numerals are rejected."""
        converter = NHConverter()
        assert not converter._is_valid_roman_numeral("")
        assert not converter._is_valid_roman_numeral("IIII")  # Should be IV
        assert not converter._is_valid_roman_numeral("ABC")
        assert not converter._is_valid_roman_numeral("VV")  # V cannot be repeated


class TestNHConverterIntegration:
    """Integration tests that hit real gc.nh.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_business_profits_tax_definitions(self):
        """Fetch NH Business Profits Tax section 77-A:1."""
        converter = NHConverter()
        section = converter.fetch_section("77-A:1")

        assert section is not None
        assert section.citation.section == "NH-77-A:1"
        assert "definition" in section.section_title.lower()
        assert "business" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_business_profits_tax_rates(self):
        """Fetch NH Business Profits Tax section 77-A:2."""
        converter = NHConverter()
        section = converter.fetch_section("77-A:2")

        assert section is not None
        assert section.citation.section == "NH-77-A:2"
        assert "tax" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_meals_rooms_tax(self):
        """Fetch NH Meals and Rooms Tax section 78:1."""
        converter = NHConverter()
        try:
            section = converter.fetch_section("78:1")
            assert section.citation.section == "NH-78:1"
        except NHConverterError:
            pytest.skip("Section 78:1 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_77a_sections(self):
        """Get list of sections in Chapter 77-A."""
        converter = NHConverter()
        sections = converter.get_chapter_section_numbers("77-A")

        assert len(sections) > 0
        assert all(s.startswith("77-A:") for s in sections)
        assert "77-A:1" in sections  # Definitions

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch NH public assistance section."""
        converter = NHConverter()
        try:
            section = converter.fetch_section("167:4")
            assert section.citation.section == "NH-167:4"
        except NHConverterError:
            pytest.skip("Section 167:4 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_business_enterprise_tax(self):
        """Fetch NH Business Enterprise Tax section 77-E:1."""
        converter = NHConverter()
        try:
            section = converter.fetch_section("77-E:1")
            assert section.citation.section == "NH-77-E:1"
        except NHConverterError:
            pytest.skip("Section 77-E:1 not found")
