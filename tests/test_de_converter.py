"""Tests for Delaware state statute converter.

Tests the DEConverter which fetches from delcode.delaware.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.de import (
    DE_TAX_CHAPTERS,
    DE_WELFARE_CHAPTERS,
    DEConverter,
    DEConverterError,
    download_de_chapter,
    fetch_de_section,
)
from axiom.models import Section

# Sample HTML from delcode.delaware.gov for testing
SAMPLE_CHAPTER_HTML = """<html>
  <head><title>Delaware Code Online</title></head>
  <body>
    <div class="page-container">
      <div class="code-container">
        <div id="content" class="container container-home" role="main">
          <ul class="chaptersections">
            <li><a href="#1101">§ 1101</a></li>
            <li><a href="#1102">§ 1102</a></li>
            <li><a href="#1103">§ 1103</a></li>
          </ul>
          <div id="TitleHead">
            <h1>TITLE 30</h1>
            <h4>State Taxes</h4>
            <h2>Income, Inheritance and Estate Taxes</h2>
            <h3>CHAPTER 11. Personal Income Tax</h3>
            <h4>Subchapter I. General Provisions</h4>
          </div>
          <div id="CodeBody">
            <div class="Section">
              <div class="SectionHead" id="1101">§ 1101. Meaning of terms.</div>
              <p class="subsection">Any term used in this chapter shall have the same meaning as when used in a comparable context in the laws of the United States referring to federal income taxes, unless a different meaning is clearly required. Any reference to the laws of the United States shall mean the Internal Revenue Code of 1986 [26 U.S.C. § 1 et seq.] and amendments thereto and other laws of the United States relating to federal income taxes, as the same may have been or shall become effective, for any taxable year.</p>
              30 Del. C. 1953, § 1101;
              <a href="https://legis.delaware.gov/SessionLaws?volume=57&amp;chapter=737">57 Del. Laws, c. 737, § 1</a>;
              <a href="https://legis.delaware.gov/SessionLaws?volume=67&amp;chapter=408">67 Del. Laws, c. 408, § 1</a>;
            </div><br>
            <div class="Section">
              <div class="SectionHead" id="1102">§ 1102. Imposition and rate of tax; separate tax on lump-sum distributions.</div>
              <p class="subsection">(a) (1) For taxable years beginning after December 31, 2024, the amount of tax shall be determined as follows:</p>
              <p class="indent-3">2.2% of taxable income not in excess of $2,000;</p>
              <p class="indent-3">3.9% of taxable income in excess of $2,000 but not in excess of $5,000;</p>
              <p class="indent-3">4.8% of taxable income in excess of $5,000 but not in excess of $10,000;</p>
              <p class="indent-3">5.2% of taxable income in excess of $10,000 but not in excess of $20,000;</p>
              <p class="indent-3">5.55% of taxable income in excess of $20,000 but not in excess of $25,000;</p>
              <p class="indent-3">6.6% of taxable income in excess of $25,000 but not in excess of $60,000;</p>
              <p class="indent-3">6.6% of taxable income in excess of $60,000.</p>
              <p class="indent-2">(2) For taxable years beginning after December 31, 1999, and before January 1, 2025, see historical tax rate schedules.</p>
              <p class="subsection">(b) Every taxpayer required to file a federal return shall file a return with the Director.</p>
              30 Del. C. 1953, § 1102;
              <a href="https://legis.delaware.gov/SessionLaws?volume=57&amp;chapter=737">57 Del. Laws, c. 737, § 1</a>;
            </div><br>
            <div class="Section">
              <div class="SectionHead" id="1103">§ 1103. Credits against tax.</div>
              <p class="subsection">(a) Personal credits. — Every resident individual shall be allowed a credit against the tax imposed by this chapter in the amount of $110.</p>
              <p class="subsection">(b) Additional credits for dependents. — In addition to the credit allowed under subsection (a) of this section, a resident individual shall be allowed a credit of $110 for each dependent.</p>
              30 Del. C. 1953, § 1103;
            </div>
          </div>
        </div>
      </div>
    </div>
  </body>
</html>
"""

SAMPLE_TITLE_INDEX_HTML = """<html>
  <head><title>Delaware Code Online</title></head>
  <body>
    <div class="page-container">
      <div class="code-container">
        <div id="content" class="container container-home" role="main">
          <div id="TitleHead">
            <h1>TITLE 30</h1>
            <h4>State Taxes</h4>
          </div>
          <h2>Part I. General Provisions</h2>
          <div class="title-links"><a href="../title30/c001/index.html">Chapter 1. General Provisions</a></div>
          <div class="title-links"><a href="../title30/c003/index.html">Chapter 3. Division of Revenue</a></div>
          <h2>Part II. Income Taxes</h2>
          <div class="title-links"><a href="../title30/c011/index.html">Chapter 11. Personal Income Tax</a></div>
          <div class="title-links"><a href="../title30/c019/index.html">Chapter 19. Corporation Income Tax</a></div>
        </div>
      </div>
    </div>
  </body>
</html>
"""

SAMPLE_CHAPTER_WITH_SUBCHAPTERS_HTML = """<html>
  <head><title>Delaware Code Online</title></head>
  <body>
    <div class="page-container">
      <div class="code-container">
        <div id="content" class="container container-home" role="main">
          <div id="TitleHead">
            <h1>TITLE 30</h1>
            <h4>State Taxes</h4>
            <h3>CHAPTER 11. Personal Income Tax</h3>
          </div>
          <div class="title-links"><a href="../../title30/c011/sc01/index.html">Subchapter I. General Provisions</a></div>
          <div class="title-links"><a href="../../title30/c011/sc02/index.html">Subchapter II. Resident Individuals</a></div>
          <div class="title-links"><a href="../../title30/c011/sc03/index.html">Subchapter III. Nonresident Individuals</a></div>
        </div>
      </div>
    </div>
  </body>
</html>
"""


class TestDEChaptersRegistry:
    """Test Delaware chapter registries."""

    def test_chapter_11_in_tax_chapters(self):
        """Chapter 11 (Personal Income Tax) is in tax chapters."""
        assert 11 in DE_TAX_CHAPTERS
        assert "Personal Income Tax" in DE_TAX_CHAPTERS[11]

    def test_chapter_19_in_tax_chapters(self):
        """Chapter 19 (Corporation Income Tax) is in tax chapters."""
        assert 19 in DE_TAX_CHAPTERS
        assert "Corporation" in DE_TAX_CHAPTERS[19]

    def test_chapter_5_in_welfare_chapters(self):
        """Chapter 5 (Public Assistance) is in welfare chapters."""
        assert 5 in DE_WELFARE_CHAPTERS
        assert "Assistance" in DE_WELFARE_CHAPTERS[5] or "Public" in DE_WELFARE_CHAPTERS[5]

    def test_tax_chapters_have_expected_chapters(self):
        """Tax chapters include key chapters for state taxes."""
        # Key chapters in Title 30
        assert 1 in DE_TAX_CHAPTERS  # General Provisions
        assert 11 in DE_TAX_CHAPTERS  # Personal Income Tax


class TestDEConverter:
    """Test DEConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = DEConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = DEConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = DEConverter(year=2024)
        assert converter.year == 2024

    def test_build_chapter_url(self):
        """Build correct URL for chapter fetch."""
        converter = DEConverter()
        url = converter._build_chapter_url(30, 11)
        assert "delcode.delaware.gov" in url
        assert "title30" in url
        assert "c011" in url

    def test_build_subchapter_url(self):
        """Build correct URL for subchapter fetch."""
        converter = DEConverter()
        url = converter._build_subchapter_url(30, 11, 1)
        assert "delcode.delaware.gov" in url
        assert "title30" in url
        assert "c011" in url
        assert "sc01" in url

    def test_build_title_url(self):
        """Build correct URL for title index."""
        converter = DEConverter()
        url = converter._build_title_url(30)
        assert "delcode.delaware.gov" in url
        assert "title30/index.html" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with DEConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestDEConverterParsing:
    """Test DEConverter HTML parsing."""

    def test_parse_chapter_html(self):
        """Parse chapter HTML into list of ParsedDESection."""
        converter = DEConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, 30, 11, "https://example.com")

        assert len(sections) == 3
        assert sections[0].section_number == "1101"
        assert sections[0].section_title == "Meaning of terms"
        assert sections[1].section_number == "1102"
        assert "Imposition" in sections[1].section_title
        assert sections[2].section_number == "1103"

    def test_parse_section_title(self):
        """Parse section number and title from SectionHead."""
        converter = DEConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, 30, 11, "https://example.com")

        # Section 1101
        assert sections[0].section_number == "1101"
        assert sections[0].section_title == "Meaning of terms"
        assert sections[0].chapter_number == 11

    def test_parse_section_text(self):
        """Parse section text content."""
        converter = DEConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, 30, 11, "https://example.com")

        # Section 1101 should have text about federal income taxes
        assert "Internal Revenue Code" in sections[0].text
        assert "federal income taxes" in sections[0].text

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = DEConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, 30, 11, "https://example.com")

        # Section 1102 should have subsections (a) and (b)
        sec_1102 = sections[1]
        assert len(sec_1102.subsections) >= 2
        assert any(s.identifier == "a" for s in sec_1102.subsections)
        assert any(s.identifier == "b" for s in sec_1102.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (a)."""
        converter = DEConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, 30, 11, "https://example.com")

        # Section 1102(a) should have children (1) and (2)
        sec_1102 = sections[1]
        sub_a = next((s for s in sec_1102.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        assert len(sub_a.children) >= 1

    def test_parse_history(self):
        """Parse history citations from section HTML."""
        converter = DEConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, 30, 11, "https://example.com")

        # Section 1101 should have history citations
        assert sections[0].history is not None
        assert "Del. Laws" in sections[0].history or "Del. C." in sections[0].history

    def test_to_section_model(self):
        """Convert ParsedDESection to Section model."""
        converter = DEConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, 30, 11, "https://example.com")
        section = converter._to_section(sections[0], 30)

        assert isinstance(section, Section)
        assert section.citation.section == "DE-30-1101"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Meaning of terms"
        assert "Delaware Code" in section.title_name
        assert section.uslm_id == "de/30/11/1101"
        assert section.source_url == "https://example.com#1101"


class TestDEConverterFetching:
    """Test DEConverter HTTP fetching with mocks."""

    @patch.object(DEConverter, "_get")
    def test_fetch_chapter(self, mock_get):
        """Fetch and parse a chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = DEConverter()
        sections = converter.fetch_chapter(30, 11)

        assert sections is not None
        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)
        assert sections[0].citation.section == "DE-30-1101"

    @patch.object(DEConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch a specific section from a chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = DEConverter()
        section = converter.fetch_section(30, 11, "1101")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "DE-30-1101"
        assert "Meaning of terms" in section.section_title

    @patch.object(DEConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = DEConverter()
        with pytest.raises(DEConverterError) as exc_info:
            converter.fetch_section(30, 11, "9999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(DEConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = DEConverter()
        section_numbers = converter.get_chapter_section_numbers(30, 11)

        assert len(section_numbers) == 3
        assert "1101" in section_numbers
        assert "1102" in section_numbers
        assert "1103" in section_numbers

    @patch.object(DEConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = DEConverter()
        sections = list(converter.iter_chapter(30, 11))

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(DEConverter, "_get")
    def test_fetch_de_section(self, mock_get):
        """Test fetch_de_section function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        section = fetch_de_section(30, 11, "1101")

        assert section is not None
        assert section.citation.section == "DE-30-1101"

    @patch.object(DEConverter, "_get")
    def test_download_de_chapter(self, mock_get):
        """Test download_de_chapter function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        sections = download_de_chapter(30, 11)

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestDEConverterIntegration:
    """Integration tests that hit real delcode.delaware.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_chapter(self):
        """Fetch Delaware Personal Income Tax chapter (30 Del. C. Chapter 11)."""
        converter = DEConverter()
        sections = converter.fetch_chapter(30, 11)

        assert sections is not None
        assert len(sections) > 0
        # Should have section 1101 (definitions)
        section_numbers = [s.citation.section for s in sections]
        assert any("1101" in sn for sn in section_numbers)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_specific_section(self):
        """Fetch a specific Delaware income tax section."""
        converter = DEConverter()
        section = converter.fetch_section(30, 11, "1101")

        assert section is not None
        assert "1101" in section.citation.section
        assert "income" in section.text.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_chapter(self):
        """Fetch Delaware welfare chapter (31 Del. C. Chapter 5)."""
        converter = DEConverter()
        try:
            sections = converter.fetch_chapter(31, 5)
            assert sections is not None
            assert len(sections) > 0
        except DEConverterError:
            # Chapter may be structured differently
            pytest.skip("Chapter 5 may have different structure")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_section_numbers(self):
        """Get list of sections in Chapter 11."""
        converter = DEConverter()
        sections = converter.get_chapter_section_numbers(30, 11)

        assert len(sections) > 0
        # Chapter 11 sections should be in the 11xx range
        assert any(s.startswith("11") for s in sections)
