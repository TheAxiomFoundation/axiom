"""Tests for District of Columbia statute converter.

Tests the DCConverter which fetches from the dccouncil/law-xml-codified
GitHub repository and converts to the internal Section model.
"""

from unittest.mock import patch

import pytest

from axiom.converters.us_states.dc import (
    DC_TAX_CHAPTERS,
    DC_TITLES,
    DC_WELFARE_CHAPTERS,
    DCConverter,
    DCConverterError,
    download_dc_title,
    fetch_dc_section,
)
from axiom.models import Section

# Sample XML from dccouncil/law-xml-codified for testing
# Based on structure from https://raw.githubusercontent.com/dccouncil/law-xml-codified/master/
SAMPLE_SECTION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<section xmlns="https://code.dccouncil.us/schemas/dc-library"
         xmlns:codified="https://code.dccouncil.us/schemas/codified"
         xmlns:codify="https://code.dccouncil.us/schemas/codify"
         xmlns:xi="http://www.w3.org/2001/XInclude">
  <num>47-1801.04</num>
  <heading>General definitions.</heading>
  <text>For the purposes of this chapter, unless otherwise required by the context, the term:</text>
  <para>
    <num>(1)</num>
    <text>"Affiliated group" means one or more chains of corporations connected through stock ownership.</text>
  </para>
  <para>
    <num>(2)</num>
    <text>"Base year" means the calendar year preceding the taxable year.</text>
  </para>
  <para>
    <num>(3)</num>
    <heading>Basic standard deduction</heading>
    <text>means the amount specified in subsection (A).</text>
    <para>
      <num>(A)</num>
      <text>For the taxable year ending December 31, 2025, the basic standard deduction is $12,000.</text>
    </para>
    <para>
      <num>(B)</num>
      <text>For subsequent years, the amount shall be adjusted for inflation.</text>
    </para>
  </para>
  <annotations>
    <annotation doc="D.C. Law 1-23" type="History">Oct. 21, 1975, D.C. Law 1-23, title VI, § 601</annotation>
    <annotation type="Effect of Amendments">D.C. Law 23-45 revised the definition of "basic standard deduction".</annotation>
    <annotation type="Editor's Notes">Implementation guidance for tax year 2025.</annotation>
  </annotations>
</section>
"""

SAMPLE_WELFARE_SECTION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<section xmlns="https://code.dccouncil.us/schemas/dc-library">
  <num>4-205.11</num>
  <heading>TANF need determination.</heading>
  <text>The Mayor shall determine the need for assistance by applying the following standards:</text>
  <para>
    <num>(1)</num>
    <text>Earned income disregards as specified by regulation.</text>
  </para>
  <para>
    <num>(2)</num>
    <text>Child care expense deductions up to the maximum amount.</text>
  </para>
  <annotations>
    <annotation type="History">Apr. 6, 1982, D.C. Law 4-101, § 511</annotation>
  </annotations>
</section>
"""

SAMPLE_TITLE_INDEX_XML = """<?xml version="1.0" encoding="UTF-8"?>
<container xmlns="https://code.dccouncil.us/schemas/dc-library"
           xmlns:xi="http://www.w3.org/2001/XInclude">
  <prefix>Title</prefix>
  <num>47</num>
  <heading>Taxation, Licensing, Permits, Assessments, and Fees.</heading>
  <xi:include href="./sections/47-101.xml"/>
  <xi:include href="./sections/47-102.xml"/>
  <xi:include href="./sections/47-1801.01.xml"/>
  <xi:include href="./sections/47-1801.04.xml"/>
  <xi:include href="./sections/47-1806.01.xml"/>
</container>
"""


class TestDCTitlesRegistry:
    """Test DC Code title registries."""

    def test_title_47_in_titles(self):
        """Title 47 (Taxation) is in titles dict."""
        assert 47 in DC_TITLES
        assert "Taxation" in DC_TITLES[47]

    def test_title_4_in_titles(self):
        """Title 4 (Public Care Systems) is in titles dict."""
        assert 4 in DC_TITLES
        assert "Public Care" in DC_TITLES[4]

    def test_title_46_in_titles(self):
        """Title 46 (Domestic Relations) is in titles dict."""
        assert 46 in DC_TITLES
        assert "Domestic Relations" in DC_TITLES[46]

    def test_tax_chapters_exist(self):
        """Tax chapters registry has entries."""
        assert len(DC_TAX_CHAPTERS) > 0
        assert 18 in DC_TAX_CHAPTERS  # Income and Franchise Taxes
        assert "Income" in DC_TAX_CHAPTERS[18]

    def test_welfare_chapters_exist(self):
        """Welfare chapters registry has entries."""
        assert len(DC_WELFARE_CHAPTERS) > 0
        assert 2 in DC_WELFARE_CHAPTERS  # Public Assistance
        assert "Assistance" in DC_WELFARE_CHAPTERS[2]


class TestDCConverter:
    """Test DCConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = DCConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.use_web_fallback is False

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = DCConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_web_fallback(self):
        """Converter accepts web fallback option."""
        converter = DCConverter(use_web_fallback=True)
        assert converter.use_web_fallback is True

    def test_parse_section_number_valid(self):
        """Parse valid section numbers."""
        converter = DCConverter()

        title, section = converter._parse_section_number("47-1801.04")
        assert title == 47
        assert section == "47-1801.04"

        title, section = converter._parse_section_number("4-205.11")
        assert title == 4
        assert section == "4-205.11"

    def test_parse_section_number_invalid(self):
        """Handle invalid section number format."""
        converter = DCConverter()

        with pytest.raises(DCConverterError):
            converter._parse_section_number("invalid")

        with pytest.raises(DCConverterError):
            converter._parse_section_number("abc-123")

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = DCConverter()
        url = converter._build_section_url("47-1801.04")

        assert "raw.githubusercontent.com" in url
        assert "dccouncil/law-xml-codified" in url
        assert "titles/47" in url
        assert "sections/47-1801.04.xml" in url

    def test_build_section_url_title_4(self):
        """Build correct URL for Title 4 section."""
        converter = DCConverter()
        url = converter._build_section_url("4-205.11")

        assert "titles/4" in url
        assert "sections/4-205.11.xml" in url

    def test_build_web_url(self):
        """Build correct web interface URL."""
        converter = DCConverter()
        url = converter._build_web_url("47-1801.04")

        assert "code.dccouncil.gov" in url
        assert "sections/47-1801.04" in url

    def test_build_title_index_url(self):
        """Build correct URL for title index."""
        converter = DCConverter()
        url = converter._build_title_index_url(47)

        assert "titles/47/index.xml" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with DCConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestDCConverterParsing:
    """Test DCConverter XML parsing."""

    def test_parse_xml(self):
        """Parse section XML into ParsedDCSection."""
        converter = DCConverter()
        parsed = converter._parse_xml(SAMPLE_SECTION_XML, "47-1801.04", "https://example.com")

        assert parsed.section_number == "47-1801.04"
        assert parsed.section_title == "General definitions."
        assert parsed.title_number == 47
        assert parsed.title_name == "Taxation, Licensing, Permits, Assessments, and Fees"
        assert "purposes of this chapter" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section XML."""
        converter = DCConverter()
        parsed = converter._parse_xml(SAMPLE_SECTION_XML, "47-1801.04", "https://example.com")

        # Should have subsections (1), (2), (3)
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)
        assert any(s.identifier == "3" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (A), (B) under (3)."""
        converter = DCConverter()
        parsed = converter._parse_xml(SAMPLE_SECTION_XML, "47-1801.04", "https://example.com")

        # Find subsection (3) which has "Basic standard deduction" heading
        sub_3 = next((s for s in parsed.subsections if s.identifier == "3"), None)
        assert sub_3 is not None
        assert sub_3.heading == "Basic standard deduction"

        # Should have children (A) and (B)
        assert len(sub_3.children) == 2
        assert any(c.identifier == "A" for c in sub_3.children)
        assert any(c.identifier == "B" for c in sub_3.children)

    def test_parse_subsection_text(self):
        """Verify subsection text content."""
        converter = DCConverter()
        parsed = converter._parse_xml(SAMPLE_SECTION_XML, "47-1801.04", "https://example.com")

        sub_1 = next((s for s in parsed.subsections if s.identifier == "1"), None)
        assert sub_1 is not None
        assert "Affiliated group" in sub_1.text
        assert "stock ownership" in sub_1.text

    def test_parse_annotations(self):
        """Parse annotations from section XML."""
        converter = DCConverter()
        parsed = converter._parse_xml(SAMPLE_SECTION_XML, "47-1801.04", "https://example.com")

        assert len(parsed.annotations) > 0
        # Check for history annotation
        assert parsed.history is not None
        assert "D.C. Law 1-23" in parsed.history

    def test_parse_welfare_section(self):
        """Parse welfare section (Title 4) XML."""
        converter = DCConverter()
        parsed = converter._parse_xml(SAMPLE_WELFARE_SECTION_XML, "4-205.11", "https://example.com")

        assert parsed.section_number == "4-205.11"
        assert parsed.section_title == "TANF need determination."
        assert parsed.title_number == 4
        assert "Public Care" in parsed.title_name

    def test_to_section_model(self):
        """Convert ParsedDCSection to Section model."""
        converter = DCConverter()
        parsed = converter._parse_xml(SAMPLE_SECTION_XML, "47-1801.04", "https://example.com")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "DC-47-1801.04"
        assert section.citation.title == 0  # District law indicator
        assert section.section_title == "General definitions."
        assert "DC Code" in section.title_name
        assert section.uslm_id == "dc/47/47-1801.04"
        assert section.source_url == "https://example.com"

    def test_to_section_model_subsections(self):
        """Verify subsections are converted correctly."""
        converter = DCConverter()
        parsed = converter._parse_xml(SAMPLE_SECTION_XML, "47-1801.04", "https://example.com")
        section = converter._to_section(parsed)

        assert len(section.subsections) >= 3
        # Check nested structure is preserved
        sub_3 = next((s for s in section.subsections if s.identifier == "3"), None)
        assert sub_3 is not None
        assert len(sub_3.children) == 2


class TestDCConverterFetching:
    """Test DCConverter HTTP fetching with mocks."""

    @patch.object(DCConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_XML

        converter = DCConverter()
        section = converter.fetch_section("47-1801.04")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "DC-47-1801.04"
        assert "General definitions" in section.section_title

    @patch.object(DCConverter, "_get")
    def test_fetch_section_welfare(self, mock_get):
        """Fetch and parse a welfare section."""
        mock_get.return_value = SAMPLE_WELFARE_SECTION_XML

        converter = DCConverter()
        section = converter.fetch_section("4-205.11")

        assert section is not None
        assert section.citation.section == "DC-4-205.11"
        assert "TANF" in section.section_title

    @patch.object(DCConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        import httpx

        mock_response = httpx.Response(404)
        mock_get.side_effect = httpx.HTTPStatusError(
            "Not Found", request=httpx.Request("GET", "http://test"), response=mock_response
        )

        converter = DCConverter()
        with pytest.raises(DCConverterError) as exc_info:
            converter.fetch_section("99-999.99")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(DCConverter, "_get")
    def test_get_title_section_numbers(self, mock_get):
        """Get list of section numbers from title index."""
        mock_get.return_value = SAMPLE_TITLE_INDEX_XML

        converter = DCConverter()
        sections = converter.get_title_section_numbers(47)

        assert len(sections) == 5
        assert "47-101" in sections
        assert "47-1801.04" in sections
        assert "47-1806.01" in sections

    @patch.object(DCConverter, "_get")
    def test_iter_title(self, mock_get):
        """Iterate over sections in a title."""
        # First call returns index, subsequent calls return section XML
        mock_get.side_effect = [
            SAMPLE_TITLE_INDEX_XML,
            SAMPLE_SECTION_XML,
            SAMPLE_SECTION_XML,
            SAMPLE_SECTION_XML,
            SAMPLE_SECTION_XML,
            SAMPLE_SECTION_XML,
        ]

        converter = DCConverter()
        sections = list(converter.iter_title(47))

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(DCConverter, "_get")
    def test_fetch_dc_section(self, mock_get):
        """Test fetch_dc_section function."""
        mock_get.return_value = SAMPLE_SECTION_XML

        section = fetch_dc_section("47-1801.04")

        assert section is not None
        assert section.citation.section == "DC-47-1801.04"

    @patch.object(DCConverter, "_get")
    def test_download_dc_title(self, mock_get):
        """Test download_dc_title function."""
        mock_get.side_effect = [
            SAMPLE_TITLE_INDEX_XML,
            SAMPLE_SECTION_XML,
            SAMPLE_SECTION_XML,
            SAMPLE_SECTION_XML,
            SAMPLE_SECTION_XML,
            SAMPLE_SECTION_XML,
        ]

        sections = download_dc_title(47)

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestDCConverterIntegration:
    """Integration tests that hit real GitHub (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_section(self):
        """Fetch DC Tax section 47-1801.04 (definitions)."""
        converter = DCConverter()
        section = converter.fetch_section("47-1801.04")

        assert section is not None
        assert section.citation.section == "DC-47-1801.04"
        assert "definitions" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch DC income tax rate section."""
        converter = DCConverter()
        try:
            section = converter.fetch_section("47-1806.03")
            assert section.citation.section == "DC-47-1806.03"
        except DCConverterError:
            pytest.skip("Section 47-1806.03 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch DC TANF section 4-205.11."""
        converter = DCConverter()
        try:
            section = converter.fetch_section("4-205.11")
            assert section.citation.section == "DC-4-205.11"
            assert "TANF" in section.section_title or "need" in section.section_title.lower()
        except DCConverterError:
            pytest.skip("Section 4-205.11 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_47_sections(self):
        """Get list of sections in Title 47."""
        converter = DCConverter()
        sections = converter.get_title_section_numbers(47)

        # Title 47 should have many sections
        assert len(sections) > 10
        # Most sections should start with "47-" (some repealed sections have brackets)
        valid_sections = [s for s in sections if s.startswith("47-") or s.startswith("[47-")]
        assert len(valid_sections) == len(sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_4_sections(self):
        """Get list of sections in Title 4 (Public Care)."""
        converter = DCConverter()
        sections = converter.get_title_section_numbers(4)

        assert len(sections) > 0
        assert all(s.startswith("4-") for s in sections)
