"""Tests for South Dakota state statute converter.

Tests the SDConverter which fetches from sdlegislature.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.sd import (
    SD_TAX_CHAPTERS,
    SD_WELFARE_CHAPTERS,
    SDConverter,
    download_sd_chapter,
    fetch_sd_section,
)
from axiom.models import Section

# Sample HTML from sdlegislature.gov for testing (modern format)
SAMPLE_SECTION_HTML = """<html xmlns="http://www.w3.org/1999/xhtml"><head>
<meta charset="UTF-8" />
<meta http-equiv='content-language' content='en-us'/>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>SDLRC - Codified Law 10-1-1 - Department created--Seal of department. </title>
<meta name="Generator" content="PowerTools for Open XML" />
<meta property="og:title" content="SD 10-1-1 - Department created--Seal of department.">
<style>span { white-space: pre-wrap; }
p.s2037406Normal { margin-top: 0; margin-bottom: 0; text-align: justify; font-family: 'Times New Roman', 'serif'; font-size: 12pt; line-height: 108%; margin-left: 0; margin-right: 0; }
span.s2037406SENU { font-family: 'Times New Roman', 'serif'; font-size: 12pt; font-style: normal; font-weight: bold; margin: 0; padding: 0; }
span.s2037406CL { font-family: 'Times New Roman', 'serif'; font-size: 12pt; font-style: normal; font-weight: bold; margin: 0; padding: 0; }
span.s2037406DefaultParagraphFont { font-family: 'Times New Roman', 'serif'; font-size: 12pt; font-style: normal; font-weight: normal; margin: 0; padding: 0; }
span.s2037406SCL { font-family: 'Times New Roman', 'serif'; font-size: 12pt; font-style: normal; font-weight: bold; margin: 0; padding: 0; }
span.s2037406SCL-000002 { font-family: 'Times New Roman', 'serif'; font-size: 12pt; font-style: normal; font-weight: normal; margin: 0; padding: 0; }
</style></head><body><div>
<p dir="ltr" class="s2037406Normal">
<a target="_top" href="https://sdlegislature.gov/Statutes/Codified_Laws/DisplayStatute.aspx?Type=Statute&amp;Statute=10-1-1">
<span class="s2037406SENU">10-1-1</span></a>
<span xml:space="preserve" class="s2037406SENU">. </span>
<span class="s2037406CL">Department created--Seal of department.</span></p>
<p dir="ltr" class="s2037406Normal-000000">
<span class="s2037406DefaultParagraphFont">There is hereby constituted an executive department of the state government to be known as the Department of Revenue, with the powers, duties, and functions established by this chapter. The department shall have a seal which shall contain the words "Seal of the Department of Revenue."</span></p>
<p dir="ltr" class="s2037406Normal"><span xml:space="preserve" class="s2037406000001"> </span></p>
<p dir="ltr" class="s2037406Normal">
<span class="s2037406SCL">Source:</span>
<span xml:space="preserve" class="s2037406SCL-000002">  SL 1955, ch 246, § 1; SDC Supp 1960, § 57.01A01; SL 2003, ch 272 (Ex. Ord. 03-1), § 82; SL 2011, ch 1 (Ex. Ord. 11-1), § 161, eff. Apr. 12, 2011.</span></p>
</div></body></html>
"""

# Sample HTML with subsections
SAMPLE_SECTION_WITH_SUBSECTIONS_HTML = """<html xmlns="http://www.w3.org/1999/xhtml"><head>
<meta charset="UTF-8">
<title>SDLRC - Codified Law 28-6-1 - Provision of medical services and remedial care authorized--Rules.</title>
<meta property="og:title" content="SD 28-6-1 - Provision of medical services and remedial care authorized--Rules.">
</head><body><div>
<p dir="ltr" class="Normal">
<a target="_top" href="https://sdlegislature.gov/Statutes?Statute=28-6-1">
<span class="SENU">28-6-1</span></a>
<span xml:space="preserve" class="SENU">. </span>
<span class="CL">Provision of medical services and remedial care authorized--Rules.</span></p>
<p dir="ltr" class="Normal-000000">
<span class="DefaultParagraphFont">The Department of Social Services may provide medical services and medical or remedial care on behalf of persons having insufficient income and resources. The rules shall specify:</span></p>
<p dir="ltr" class="Normal-000001">
<span class="DefaultParagraphFont">(1)    The amount, scope, and duration of medical and remedial services;</span></p>
<p dir="ltr" class="Normal-000001">
<span class="DefaultParagraphFont">(2)    The basis for and extent of provider payments on behalf of an eligible person;</span></p>
<p dir="ltr" class="Normal-000001">
<span class="DefaultParagraphFont">(3)    The establishment and collection of copayments, premiums, fees, or charges;</span></p>
<p dir="ltr" class="Normal-000001">
<span class="DefaultParagraphFont">(a)    First subdivision under subsection 3;</span></p>
<p dir="ltr" class="Normal-000001">
<span class="DefaultParagraphFont">(b)    Second subdivision under subsection 3;</span></p>
<p dir="ltr" class="Normal">
<span class="SCL">Source:</span>
<span class="SCL-000003">  SL 1966, ch 191, § 1; SL 2019, ch 127, § 9.</span></p>
</div></body></html>
"""

# Sample chapter index HTML
SAMPLE_CHAPTER_INDEX_HTML = """<html xmlns="http://www.w3.org/1999/xhtml" lang="en-US"><head>
<meta charset="UTF-8">
<title>SDLRC - Codified Law 10-1 - DEPARTMENT OF REVENUE</title>
</head><body><div>
<p dir="ltr" class="Normal"><span xml:space="preserve">CHAPTER </span>
<a target="_top" href="https://sdlegislature.gov/Statutes?Statute=10-1" rel="noopener"><span>10-1</span></a></p>
<p dir="ltr" class="Normal"><span>DEPARTMENT OF REVENUE</span></p>
<p dir="ltr" class="B">
<a target="_top" href="https://sdlegislature.gov/Statutes?Statute=10-1-1" rel="noopener"><span>10-1-1</span></a>
<span xml:space="preserve">    Department created--Seal of department.</span></p>
<p dir="ltr" class="B">
<a target="_top" href="https://sdlegislature.gov/Statutes?Statute=10-1-3" rel="noopener"><span>10-1-3</span></a>
<span xml:space="preserve">    Secretary's full time service required.</span></p>
<p dir="ltr" class="B">
<a target="_top" href="https://sdlegislature.gov/Statutes?Statute=10-1-5" rel="noopener"><span>10-1-5</span></a>
<span xml:space="preserve">    Deputy secretary of revenue.</span></p>
<p dir="ltr" class="B">
<a target="_top" href="https://sdlegislature.gov/Statutes?Statute=10-1-6" rel="noopener"><span>10-1-6</span></a>
<span xml:space="preserve">    Divisions within department.</span></p>
</div></body></html>
"""

# Sample repealed section HTML
SAMPLE_REPEALED_SECTION_HTML = """<HTML>
<HEAD>
<HTML>
<HEAD>
<meta http-equiv='content-language' content='en-us'/>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>SDLRC - Codified Law 10-45-3 - 10-45-3. Repealed by SL 2006, ch 58, § 15, eff. April 1, 2006.10-45-3 </title>
</HEAD>
<BODY>
<Div align="full">
<span>10-45-3.</span>
Repealed by SL 2006, ch 58, § 15, eff. April 1, 2006.<p>
</BODY>
</HTML>
"""


class TestSDChaptersRegistry:
    """Test South Dakota chapter registries."""

    def test_chapter_10_1_in_tax_chapters(self):
        """Chapter 10-1 (Department of Revenue) is in tax chapters."""
        assert "10-1" in SD_TAX_CHAPTERS
        assert "Revenue" in SD_TAX_CHAPTERS["10-1"]

    def test_chapter_10_45_in_tax_chapters(self):
        """Chapter 10-45 (Sales and Use Tax) is in tax chapters."""
        assert "10-45" in SD_TAX_CHAPTERS
        assert "Sales" in SD_TAX_CHAPTERS["10-45"]

    def test_chapter_28_7a_in_welfare_chapters(self):
        """Chapter 28-7A (TANF) is in welfare chapters."""
        assert "28-7A" in SD_WELFARE_CHAPTERS
        assert "Temporary Assistance" in SD_WELFARE_CHAPTERS["28-7A"]

    def test_chapter_28_6_in_welfare_chapters(self):
        """Chapter 28-6 (Medical Services) is in welfare chapters."""
        assert "28-6" in SD_WELFARE_CHAPTERS
        assert "Medical" in SD_WELFARE_CHAPTERS["28-6"]


class TestSDConverter:
    """Test SDConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = SDConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = SDConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = SDConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = SDConverter()
        url = converter._build_section_url("10-1-1")
        assert "sdlegislature.gov/api/Statutes" in url
        assert "10-1-1.html" in url

    def test_build_section_url_with_letter(self):
        """Build correct URL for section with letter suffix."""
        converter = SDConverter()
        url = converter._build_section_url("28-7A-1")
        assert "28-7A-1.html" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = SDConverter()
        url = converter._build_chapter_url("10-1")
        assert "10-1.html" in url

    def test_parse_section_number(self):
        """Parse section number into title and chapter."""
        converter = SDConverter()

        title, chapter = converter._parse_section_number("10-1-1")
        assert title == 10
        assert chapter == "10-1"

        title, chapter = converter._parse_section_number("28-7A-1")
        assert title == 28
        assert chapter == "28-7A"

    def test_context_manager(self):
        """Converter works as context manager."""
        with SDConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestSDConverterParsing:
    """Test SDConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedSDSection."""
        converter = SDConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "10-1-1", "https://example.com")

        assert parsed.section_number == "10-1-1"
        assert "Department created" in parsed.section_title
        assert parsed.title_number == 10
        assert parsed.title_name == "Taxation"
        assert parsed.chapter_number == "10-1"
        assert "Department of Revenue" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_section_history(self):
        """Parse history/source note from section HTML."""
        converter = SDConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "10-1-1", "https://example.com")

        assert parsed.history is not None
        assert "SL 1955" in parsed.history or "1955" in str(parsed.history)

    def test_parse_repealed_section(self):
        """Parse repealed section HTML."""
        converter = SDConverter()
        parsed = converter._parse_section_html(
            SAMPLE_REPEALED_SECTION_HTML, "10-45-3", "https://example.com"
        )

        assert parsed.is_repealed is True
        assert "Repealed" in parsed.text

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = SDConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "28-6-1", "https://example.com"
        )

        # Should have numbered subsections
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under numbered subsections."""
        converter = SDConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "28-6-1", "https://example.com"
        )

        # Find subsection (3) which has children
        sub_3 = next((s for s in parsed.subsections if s.identifier == "3"), None)
        if sub_3:
            # Should have children (a) and (b)
            assert len(sub_3.children) >= 2
            assert any(c.identifier == "a" for c in sub_3.children)
            assert any(c.identifier == "b" for c in sub_3.children)

    def test_to_section_model(self):
        """Convert ParsedSDSection to Section model."""
        converter = SDConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "10-1-1", "https://example.com")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "SD-10-1-1"
        assert section.citation.title == 0  # State law indicator
        assert "Department created" in section.section_title
        assert "South Dakota Codified Laws" in section.title_name
        assert section.uslm_id == "sd/10/10-1-1"
        assert section.source_url == "https://example.com"


class TestSDConverterFetching:
    """Test SDConverter HTTP fetching with mocks."""

    @patch.object(SDConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = SDConverter()
        section = converter.fetch_section("10-1-1")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "SD-10-1-1"
        assert "Department created" in section.section_title

    @patch.object(SDConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = SDConverter()
        sections = converter.get_chapter_section_numbers("10-1")

        assert len(sections) == 4
        assert "10-1-1" in sections
        assert "10-1-3" in sections
        assert "10-1-5" in sections

    @patch.object(SDConverter, "_get")
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

        converter = SDConverter()
        sections = list(converter.iter_chapter("10-1"))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(SDConverter, "_get")
    def test_fetch_sd_section(self, mock_get):
        """Test fetch_sd_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_sd_section("10-1-1")

        assert section is not None
        assert section.citation.section == "SD-10-1-1"

    @patch.object(SDConverter, "_get")
    def test_download_sd_chapter(self, mock_get):
        """Test download_sd_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_sd_chapter("10-1")

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestSDConverterIntegration:
    """Integration tests that hit real sdlegislature.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_section(self):
        """Fetch South Dakota tax section 10-1-1."""
        converter = SDConverter()
        section = converter.fetch_section("10-1-1")

        assert section is not None
        assert section.citation.section == "SD-10-1-1"
        assert "department" in section.section_title.lower() or "department" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch South Dakota Sales Tax section 10-45-2."""
        converter = SDConverter()
        section = converter.fetch_section("10-45-2")

        assert section is not None
        assert section.citation.section == "SD-10-45-2"
        assert "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch South Dakota welfare section 28-6-1."""
        converter = SDConverter()
        section = converter.fetch_section("28-6-1")

        assert section is not None
        assert section.citation.section == "SD-28-6-1"
        assert "medical" in section.text.lower() or "services" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tanf_section(self):
        """Fetch South Dakota TANF section 28-7A-1."""
        converter = SDConverter()
        section = converter.fetch_section("28-7A-1")

        assert section is not None
        assert section.citation.section == "SD-28-7A-1"

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_10_1_sections(self):
        """Get list of sections in Chapter 10-1."""
        converter = SDConverter()
        sections = converter.get_chapter_section_numbers("10-1")

        assert len(sections) > 0
        assert all(s.startswith("10-1-") for s in sections)
        assert "10-1-1" in sections
