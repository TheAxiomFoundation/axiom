"""Tests for Texas state statute converter.

Tests the TXConverter which fetches from texas.public.law
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.tx import (
    TX_CODES,
    TX_TAX_CHAPTERS,
    TX_WELFARE_CHAPTERS,
    TXConverter,
    TXConverterError,
    download_tx_chapter,
    fetch_tx_section,
)
from axiom.models import Section

# Sample HTML from texas.public.law for testing
SAMPLE_TAX_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Tex. Tax Code Section 151.001</title></head>
<body>
<nav class="breadcrumb">
<ol itemtype="BreadcrumbList">
<li>Statutes</li>
<li>Tax Code</li>
<li>Title 2</li>
<li>Subtitle E</li>
<li>Chapter 151</li>
</ol>
</nav>
<main>
<h1>Tex. Tax Code Section 151.001 Short Title</h1>
<p>This chapter may be cited as the Limited Sales, Excise, and Use Tax Act.</p>
<hr>
<p>Acts 1981, 67th Leg., p. 1545, ch. 389, Sec. 1, eff. Jan. 1, 1982.</p>
</main>
</body>
</html>
"""

SAMPLE_HR_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Tex. Human Resources Code Section 31.001</title></head>
<body>
<nav class="breadcrumb">
<ol itemtype="BreadcrumbList">
<li>Statutes</li>
<li>Human Resources Code</li>
<li>Title 2</li>
<li>Subtitle C</li>
<li>Chapter 31</li>
</ol>
</nav>
<main>
<h1>Tex. Human Resources Code Section 31.001 Temporary Assistance for Needy Families</h1>
<p>The commission shall provide financial assistance and services to families with dependent children in accordance with federal and state law and rules adopted by the executive commissioner.</p>
<p>(a) The commission shall operate the Temporary Assistance for Needy Families program to provide:</p>
<p>(1) time-limited financial assistance to families with dependent children; and</p>
<p>(2) support services to help families achieve self-sufficiency.</p>
<p>(b) The commission shall determine eligibility for benefits based on:</p>
<p>(1) income and resources;</p>
<p>(2) citizenship or immigration status; and</p>
<p>(3) work participation requirements.</p>
<hr>
<p>Acts 1979, 66th Leg., p. 2343, ch. 842, art. 1, Sec. 1, eff. Sept. 1, 1979.</p>
<p>Amended by:</p>
<p>Acts 2015, 84th Leg., R.S., Ch. 1 (S.B. 219), Sec. 4.207, eff. April 2, 2015.</p>
</main>
</body>
</html>
"""

SAMPLE_FRANCHISE_TAX_HTML = """<!DOCTYPE html>
<html>
<head><title>Tex. Tax Code Section 171.001</title></head>
<body>
<nav class="breadcrumb">
<ol itemtype="BreadcrumbList">
<li>Statutes</li>
<li>Tax Code</li>
<li>Title 2</li>
<li>Subtitle F</li>
<li>Chapter 171</li>
</ol>
</nav>
<main>
<h1>Tex. Tax Code Section 171.001 Tax Imposed</h1>
<p>(a) A franchise tax is imposed on each taxable entity that does business in this state or that is chartered or organized in this state.</p>
<p>(b) The rate of the franchise tax is:</p>
<p>(1) for taxable entities primarily engaged in retail or wholesale trade, 0.375 percent; and</p>
<p>(2) for other taxable entities, 0.75 percent.</p>
<p>(c) The tax is imposed on the taxable entity's taxable margin.</p>
<p>(d) The tax may not exceed the amount computed by:</p>
<p>(1) determining the taxable entity's taxable margin; and</p>
<p>(2) multiplying the taxable margin by the applicable tax rate.</p>
<hr>
<p>Acts 1981, 67th Leg., p. 1686, ch. 389, Sec. 1, eff. Jan. 1, 1982.</p>
<p>Amended by:</p>
<p>Acts 2006, 79th Leg., 3rd C.S., Ch. 1 (H.B. 3), Sec. 2, eff. Jan. 1, 2008.</p>
</main>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 151 - Limited Sales, Excise, and Use Tax</title></head>
<body>
<h1>Chapter 151 - Limited Sales, Excise, and Use Tax</h1>
<nav class="breadcrumb">
<ol>
<li>Tax Code</li>
<li>Title 2</li>
<li>Subtitle E</li>
<li>Chapter 151</li>
</ol>
</nav>
<main>
<ul>
<li><a href="/statutes/tex._tax_code_section_151.001">151.001 Short Title</a></li>
<li><a href="/statutes/tex._tax_code_section_151.002">151.002 Construction of Code</a></li>
<li><a href="/statutes/tex._tax_code_section_151.003">151.003 Purpose</a></li>
<li><a href="/statutes/tex._tax_code_section_151.004">151.004 Taxable Entity</a></li>
<li><a href="/statutes/tex._tax_code_section_151.0045">151.0045 Temporary Tax Exemption</a></li>
<li><a href="/statutes/tex._tax_code_section_151.005">151.005 Definitions</a></li>
</ul>
</main>
</body>
</html>
"""


class TestTXCodesRegistry:
    """Test Texas code registries."""

    def test_tax_code_in_registry(self):
        """Tax Code (TX) is in the codes registry."""
        assert "TX" in TX_CODES
        assert TX_CODES["TX"] == "tax_code"

    def test_human_resources_code_in_registry(self):
        """Human Resources Code (HR) is in the codes registry."""
        assert "HR" in TX_CODES
        assert TX_CODES["HR"] == "human_resources_code"

    def test_chapter_151_in_tax_chapters(self):
        """Chapter 151 (Sales Tax) is in tax chapters."""
        assert 151 in TX_TAX_CHAPTERS
        assert "Sales" in TX_TAX_CHAPTERS[151]

    def test_chapter_171_in_tax_chapters(self):
        """Chapter 171 (Franchise Tax) is in tax chapters."""
        assert 171 in TX_TAX_CHAPTERS
        assert "Franchise" in TX_TAX_CHAPTERS[171]

    def test_chapter_31_in_welfare_chapters(self):
        """Chapter 31 (Financial Assistance) is in welfare chapters."""
        assert 31 in TX_WELFARE_CHAPTERS
        assert "Financial" in TX_WELFARE_CHAPTERS[31] or "Assistance" in TX_WELFARE_CHAPTERS[31]

    def test_chapter_32_in_welfare_chapters(self):
        """Chapter 32 (Medical Assistance) is in welfare chapters."""
        assert 32 in TX_WELFARE_CHAPTERS
        assert "Medical" in TX_WELFARE_CHAPTERS[32]


class TestTXConverter:
    """Test TXConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = TXConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = TXConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = TXConverter(year=2024)
        assert converter.year == 2024

    def test_get_code_url_name_tax(self):
        """URL name for Tax Code."""
        converter = TXConverter()
        assert converter._get_code_url_name("TX") == "tax_code"
        assert converter._get_code_url_name("tx") == "tax_code"

    def test_get_code_url_name_hr(self):
        """URL name for Human Resources Code."""
        converter = TXConverter()
        assert converter._get_code_url_name("HR") == "human_resources_code"

    def test_get_code_url_name_invalid(self):
        """Invalid code raises error."""
        converter = TXConverter()
        with pytest.raises(TXConverterError) as exc_info:
            converter._get_code_url_name("INVALID")
        assert "Unknown Texas code" in str(exc_info.value)

    def test_build_section_url_tax(self):
        """Build correct URL for Tax Code section."""
        converter = TXConverter()
        url = converter._build_section_url("TX", "151.001")
        assert "texas.public.law" in url
        assert "tex._tax_code_section_151.001" in url

    def test_build_section_url_hr(self):
        """Build correct URL for Human Resources Code section."""
        converter = TXConverter()
        url = converter._build_section_url("HR", "31.001")
        assert "texas.public.law" in url
        assert "tex._human_resources_code_section_31.001" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = TXConverter()
        url = converter._build_chapter_url("TX", 151)
        assert "tex._tax_code_chapter_151" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with TXConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestTXConverterParsing:
    """Test TXConverter HTML parsing."""

    def test_parse_tax_section_html(self):
        """Parse Tax Code section HTML into ParsedTXSection."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "TX", "151.001", "https://example.com"
        )

        assert parsed.code == "TX"
        assert parsed.section_number == "151.001"
        assert parsed.section_title == "Short Title"
        assert parsed.chapter_number == 151
        assert parsed.chapter_title == "Limited Sales, Excise, and Use Tax"
        assert "Limited Sales" in parsed.text or "cited as" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_hr_section_html(self):
        """Parse Human Resources Code section HTML."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_HR_SECTION_HTML, "HR", "31.001", "https://example.com"
        )

        assert parsed.code == "HR"
        assert parsed.section_number == "31.001"
        assert "Temporary Assistance" in parsed.section_title
        assert parsed.chapter_number == 31
        assert "Financial" in parsed.chapter_title or "Assistance" in parsed.chapter_title

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_HR_SECTION_HTML, "HR", "31.001", "https://example.com"
        )

        # Should have subsections (a) and (b)
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (a), (b)."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_HR_SECTION_HTML, "HR", "31.001", "https://example.com"
        )

        # Find subsection (a)
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        # Should have children (1) and (2)
        assert len(sub_a.children) >= 2
        assert any(c.identifier == "1" for c in sub_a.children)
        assert any(c.identifier == "2" for c in sub_a.children)

    def test_parse_franchise_tax_subsections(self):
        """Parse franchise tax section with multiple levels."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_FRANCHISE_TAX_HTML, "TX", "171.001", "https://example.com"
        )

        # Should have subsections (a), (b), (c), (d)
        assert len(parsed.subsections) >= 4
        identifiers = [s.identifier for s in parsed.subsections]
        assert "a" in identifiers
        assert "b" in identifiers
        assert "c" in identifiers
        assert "d" in identifiers

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "TX", "151.001", "https://example.com"
        )

        assert parsed.history is not None
        assert "Acts 1981" in parsed.history or "67th Leg" in parsed.history

    def test_parse_amended_history(self):
        """Parse amended history notes."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_FRANCHISE_TAX_HTML, "TX", "171.001", "https://example.com"
        )

        assert parsed.history is not None
        # Should include both original and amendment info
        assert "Acts" in parsed.history

    def test_to_section_model_tax(self):
        """Convert ParsedTXSection to Section model for Tax Code."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "TX", "151.001", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "TX-TX-151.001"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Short Title"
        assert "Texas" in section.title_name
        assert "Tax" in section.title_name
        assert section.uslm_id == "tx/tx/151/151.001"
        assert section.source_url == "https://example.com"

    def test_to_section_model_hr(self):
        """Convert ParsedTXSection to Section model for Human Resources Code."""
        converter = TXConverter()
        parsed = converter._parse_section_html(
            SAMPLE_HR_SECTION_HTML, "HR", "31.001", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "TX-HR-31.001"
        assert "Human Resources" in section.title_name
        assert section.uslm_id == "tx/hr/31/31.001"


class TestTXConverterFetching:
    """Test TXConverter HTTP fetching with mocks."""

    @patch.object(TXConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_TAX_SECTION_HTML

        converter = TXConverter()
        section = converter.fetch_section("TX", "151.001")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "TX-TX-151.001"
        assert "Short Title" in section.section_title

    @patch.object(TXConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><head><title>404 Not Found</title></head><body>Page not found</body></html>"

        converter = TXConverter()
        with pytest.raises(TXConverterError) as exc_info:
            converter.fetch_section("TX", "999.999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(TXConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = TXConverter()
        sections = converter.get_chapter_section_numbers("TX", 151)

        assert len(sections) == 6
        assert "151.001" in sections
        assert "151.002" in sections
        assert "151.0045" in sections  # Test alphanumeric section

    @patch.object(TXConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
        ]

        converter = TXConverter()
        sections = list(converter.iter_chapter("TX", 151))

        assert len(sections) == 6
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(TXConverter, "_get")
    def test_fetch_tx_section(self, mock_get):
        """Test fetch_tx_section function."""
        mock_get.return_value = SAMPLE_TAX_SECTION_HTML

        section = fetch_tx_section("TX", "151.001")

        assert section is not None
        assert section.citation.section == "TX-TX-151.001"

    @patch.object(TXConverter, "_get")
    def test_download_tx_chapter(self, mock_get):
        """Test download_tx_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
            SAMPLE_TAX_SECTION_HTML,
        ]

        sections = download_tx_chapter("TX", 151)

        assert len(sections) == 6
        assert all(isinstance(s, Section) for s in sections)


class TestTXConverterIntegration:
    """Integration tests that hit real texas.public.law (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch Texas Sales Tax section 151.001."""
        converter = TXConverter()
        section = converter.fetch_section("TX", "151.001")

        assert section is not None
        assert section.citation.section == "TX-TX-151.001"
        assert "short title" in section.section_title.lower() or "151.001" in section.section_title

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_franchise_tax_section(self):
        """Fetch Texas Franchise Tax section 171.001."""
        converter = TXConverter()
        section = converter.fetch_section("TX", "171.001")

        assert section is not None
        assert section.citation.section == "TX-TX-171.001"
        assert "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Texas Human Resources Code section 31.001."""
        converter = TXConverter()
        section = converter.fetch_section("HR", "31.001")

        assert section is not None
        assert section.citation.section == "TX-HR-31.001"
        assert "assistance" in section.text.lower() or "families" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_151_sections(self):
        """Get list of sections in Chapter 151."""
        converter = TXConverter()
        sections = converter.get_chapter_section_numbers("TX", 151)

        assert len(sections) > 0
        assert all(s.startswith("151.") for s in sections)
        assert "151.001" in sections  # Short Title
