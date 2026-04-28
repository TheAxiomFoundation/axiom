"""Tests for Mississippi state statute converter.

Tests the MSConverter which fetches from UniCourt's cic-code-ms GitHub Pages
and converts to the internal Section model.
"""

from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from axiom.converters.us_states.ms import (
    MS_TAX_CHAPTERS,
    MS_TITLES,
    MS_WELFARE_CHAPTERS,
    MSConverter,
    MSConverterError,
    download_ms_chapter,
    fetch_ms_section,
)
from axiom.models import Section

# Sample HTML from UniCourt CIC for testing (simplified structure)
SAMPLE_TITLE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <title>MSCODE</title>
  <meta name="description" content="Release 78 of the Official Code of Mississippi Annotated" />
</head>
<body>
  <nav>
    <h1><b>Title 27. Taxation and Finance</b></h1>
    <ul class="leaders">
      <li id="t27c07-cnav05">
        <a href="#t27c07">Chapter 7. Income Tax and Withholding</a>
      </li>
    </ul>
  </nav>
  <main>
    <div>
      <h2 id="t27c07"><b>Chapter 7. Income Tax and Withholding</b></h2>
      <nav>
        <ul class="leaders">
          <li id="t27c07s27-7-1-snav01">
            <a href="#t27c07s27-7-1">§ 27-7-1. Definitions.</a>
          </li>
          <li id="t27c07s27-7-3-snav02">
            <a href="#t27c07s27-7-3">§ 27-7-3. Levy of tax.</a>
          </li>
          <li id="t27c07s27-7-5-snav03">
            <a href="#t27c07s27-7-5">§ 27-7-5. Rate of tax.</a>
          </li>
        </ul>
      </nav>
    </div>
    <div>
      <h3 id="t27c07s27-7-1"><b>§ 27-7-1. Definitions.</b></h3>
      <p>As used in this chapter:</p>
      <p>(1) "Adjusted gross income" means the adjusted gross income as defined in the Internal Revenue Code.</p>
      <p>(2) "Department" means the Department of Revenue.</p>
      <p>(a) The Department shall administer this chapter.</p>
      <p>(b) The Department shall promulgate rules and regulations.</p>
      <p>(3) "Individual" means a natural person.</p>
      <p>HISTORY: Codes, 1942, § 9220; Laws, 1934, ch. 120; Laws, 1983, ch. 475, § 1.</p>
    </div>
    <div>
      <h3 id="t27c07s27-7-3"><b>§ 27-7-3. Levy of tax.</b></h3>
      <p>There is hereby levied and there shall be collected annual taxes on the entire net income of every resident individual.</p>
      <p>HISTORY: Codes, 1942, § 9221; Laws, 1934, ch. 120.</p>
    </div>
    <div>
      <h3 id="t27c07s27-7-5"><b>§ 27-7-5. Rate of tax.</b></h3>
      <p>(1) The tax imposed by this chapter shall be computed at the following rates:</p>
      <p>(a) Three percent (3%) on the first Five Thousand Dollars ($5,000.00) of taxable income.</p>
      <p>(b) Four percent (4%) on the next Five Thousand Dollars ($5,000.00) of taxable income.</p>
      <p>(c) Five percent (5%) on all taxable income over Ten Thousand Dollars ($10,000.00).</p>
      <p>HISTORY: Codes, 1942, § 9222; Laws, 1934, ch. 120; Laws, 2016, ch. 428, § 1.</p>
    </div>
  </main>
</body>
</html>
"""

SAMPLE_WELFARE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <title>MSCODE</title>
</head>
<body>
  <nav>
    <h1><b>Title 43. Public Welfare</b></h1>
  </nav>
  <main>
    <div>
      <h2 id="t43c17"><b>Chapter 17. Medicaid</b></h2>
    </div>
    <div>
      <h3 id="t43c17s43-17-1"><b>§ 43-17-1. Definitions.</b></h3>
      <p>As used in this chapter:</p>
      <p>(1) "Assistance" means money payments to, or medical care in behalf of, or any type of remedial care recognized under state law in behalf of, a needy individual.</p>
      <p>(2) "Department" means the Division of Medicaid.</p>
      <p>HISTORY: Laws, 1969, ch. 445, § 1, eff from and after July 1, 1969.</p>
    </div>
  </main>
</body>
</html>
"""


class TestMSTitlesRegistry:
    """Test Mississippi title registries."""

    def test_title_27_is_taxation(self):
        """Title 27 is Taxation and Finance."""
        assert 27 in MS_TITLES
        assert "Taxation" in MS_TITLES[27]

    def test_title_43_is_welfare(self):
        """Title 43 is Public Welfare."""
        assert 43 in MS_TITLES
        assert "Welfare" in MS_TITLES[43]

    def test_tax_chapters_have_income_tax(self):
        """Tax chapters include Income Tax chapter 7."""
        assert 7 in MS_TAX_CHAPTERS
        assert "Income Tax" in MS_TAX_CHAPTERS[7]

    def test_tax_chapters_have_sales_tax(self):
        """Tax chapters include Sales Tax chapter 65."""
        assert 65 in MS_TAX_CHAPTERS
        assert "Sales Tax" in MS_TAX_CHAPTERS[65]

    def test_welfare_chapters_have_medicaid(self):
        """Welfare chapters include Medicaid chapter 17."""
        assert 17 in MS_WELFARE_CHAPTERS
        assert "Medicaid" in MS_WELFARE_CHAPTERS[17]

    def test_welfare_chapters_have_tanf(self):
        """Welfare chapters include TANF chapter 11."""
        assert 11 in MS_WELFARE_CHAPTERS
        assert "Temporary Assistance" in MS_WELFARE_CHAPTERS[11]


class TestMSConverter:
    """Test MSConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = MSConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.release == "r78"

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = MSConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_release(self):
        """Converter accepts custom release version."""
        converter = MSConverter(release="r77")
        assert converter.release == "r77"

    def test_build_title_url(self):
        """Build correct URL for title fetch."""
        converter = MSConverter()
        url = converter._build_title_url(27)
        assert "unicourt.github.io/cic-code-ms" in url
        assert "r78" in url
        assert "gov.ms.code.title.27.html" in url

    def test_build_title_url_single_digit(self):
        """Build correct URL for single-digit title (zero-padded)."""
        converter = MSConverter()
        url = converter._build_title_url(1)
        assert "gov.ms.code.title.01.html" in url

    def test_parse_section_number(self):
        """Parse section number into components."""
        converter = MSConverter()

        title, chapter, suffix = converter._parse_section_number("27-7-1")
        assert title == 27
        assert chapter == 7
        assert suffix == "1"

    def test_parse_section_number_multipart(self):
        """Parse multi-part section number."""
        converter = MSConverter()

        title, chapter, suffix = converter._parse_section_number("27-7-15-1")
        assert title == 27
        assert chapter == 7
        assert suffix == "15-1"

    def test_parse_section_number_invalid(self):
        """Raise error for invalid section number."""
        converter = MSConverter()

        with pytest.raises(ValueError) as exc_info:
            converter._parse_section_number("27-7")

        assert "Invalid section number" in str(exc_info.value)

    def test_context_manager(self):
        """Converter works as context manager."""
        with MSConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None
        assert len(converter._title_cache) == 0


class TestMSConverterParsing:
    """Test MSConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedMSSection."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        parsed = converter._parse_section_html(
            soup, "27-7-1", "https://example.com"
        )

        assert parsed.section_number == "27-7-1"
        assert parsed.section_title == "Definitions"
        assert parsed.title_number == 27
        assert parsed.title_name == "Taxation and Finance"
        assert parsed.chapter_number == 7
        assert parsed.chapter_title == "Income Tax and Withholding"
        assert "adjusted gross income" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        parsed = converter._parse_section_html(
            soup, "27-7-1", "https://example.com"
        )

        # Should have subsections (1), (2), (3)
        assert len(parsed.subsections) >= 3
        identifiers = [s.identifier for s in parsed.subsections]
        assert "1" in identifiers
        assert "2" in identifiers
        assert "3" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under (2)."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        parsed = converter._parse_section_html(
            soup, "27-7-1", "https://example.com"
        )

        # Find subsection (2)
        sub_2 = next((s for s in parsed.subsections if s.identifier == "2"), None)
        assert sub_2 is not None
        # Should have children (a) and (b)
        assert len(sub_2.children) >= 2
        child_ids = [c.identifier for c in sub_2.children]
        assert "a" in child_ids
        assert "b" in child_ids

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        parsed = converter._parse_section_html(
            soup, "27-7-1", "https://example.com"
        )

        assert parsed.history is not None
        assert "1942" in parsed.history or "1934" in parsed.history

    def test_parse_welfare_section(self):
        """Parse a welfare section HTML."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_WELFARE_HTML, "html.parser")

        parsed = converter._parse_section_html(
            soup, "43-17-1", "https://example.com"
        )

        assert parsed.section_number == "43-17-1"
        assert parsed.section_title == "Definitions"
        assert parsed.title_number == 43
        assert parsed.title_name == "Public Welfare"
        assert parsed.chapter_number == 17
        assert "Medicaid" in parsed.chapter_title

    def test_to_section_model(self):
        """Convert ParsedMSSection to Section model."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        parsed = converter._parse_section_html(
            soup, "27-7-1", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "MS-27-7-1"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "Mississippi Code" in section.title_name
        assert section.uslm_id == "ms/27/7/27-7-1"


class TestMSConverterSectionFinding:
    """Test MSConverter section finding in HTML."""

    def test_find_section_element(self):
        """Find section element by ID."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        elem = converter._find_section_element(soup, "27-7-1")

        assert elem is not None
        assert elem.name == "h3"
        assert "27-7-1" in elem.get_text()

    def test_find_section_element_not_found(self):
        """Return None for non-existent section."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        elem = converter._find_section_element(soup, "99-99-99")

        assert elem is None

    def test_get_chapter_section_numbers(self):
        """Get list of section numbers in a chapter."""
        converter = MSConverter()
        converter._title_cache[27] = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        sections = converter.get_chapter_section_numbers(27, 7)

        assert len(sections) >= 3
        assert "27-7-1" in sections
        assert "27-7-3" in sections
        assert "27-7-5" in sections


class TestMSConverterFetching:
    """Test MSConverter HTTP fetching with mocks."""

    @patch.object(MSConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        converter = MSConverter()
        section = converter.fetch_section("27-7-1")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "MS-27-7-1"
        assert "Definitions" in section.section_title

    @patch.object(MSConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        converter = MSConverter()
        with pytest.raises(MSConverterError) as exc_info:
            converter.fetch_section("99-99-99")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(MSConverter, "_get")
    def test_fetch_section_caches_title(self, mock_get):
        """Fetching multiple sections from same title uses cache."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        converter = MSConverter()
        converter.fetch_section("27-7-1")
        converter.fetch_section("27-7-3")

        # Should only fetch the title HTML once
        assert mock_get.call_count == 1
        assert 27 in converter._title_cache

    @patch.object(MSConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        converter = MSConverter()
        sections = list(converter.iter_chapter(27, 7))

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)
        section_nums = [s.citation.section for s in sections]
        assert "MS-27-7-1" in section_nums
        assert "MS-27-7-3" in section_nums
        assert "MS-27-7-5" in section_nums


class TestMSConverterSubsectionParsing:
    """Test MSConverter subsection parsing."""

    def test_parse_tax_rate_subsections(self):
        """Parse tax rate section with nested subsections."""
        converter = MSConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_HTML, "html.parser")

        parsed = converter._parse_section_html(
            soup, "27-7-5", "https://example.com"
        )

        # Should have top-level subsection (1)
        assert len(parsed.subsections) >= 1
        sub_1 = next((s for s in parsed.subsections if s.identifier == "1"), None)
        assert sub_1 is not None

        # Should have children (a), (b), (c)
        assert len(sub_1.children) >= 3
        child_ids = [c.identifier for c in sub_1.children]
        assert "a" in child_ids
        assert "b" in child_ids
        assert "c" in child_ids


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(MSConverter, "_get")
    def test_fetch_ms_section(self, mock_get):
        """Test fetch_ms_section function."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        section = fetch_ms_section("27-7-1")

        assert section is not None
        assert section.citation.section == "MS-27-7-1"

    @patch.object(MSConverter, "_get")
    def test_download_ms_chapter(self, mock_get):
        """Test download_ms_chapter function."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        sections = download_ms_chapter(27, 7)

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestMSConverterIntegration:
    """Integration tests that hit real UniCourt site (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_definition(self):
        """Fetch Mississippi Income Tax definitions section 27-7-1."""
        converter = MSConverter()
        section = converter.fetch_section("27-7-1")

        assert section is not None
        assert section.citation.section == "MS-27-7-1"
        assert "definition" in section.section_title.lower() or "27-7-1" in section.citation.section

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_rate_section(self):
        """Fetch Mississippi Income Tax rate section."""
        converter = MSConverter()
        try:
            section = converter.fetch_section("27-7-5")
            assert section.citation.section == "MS-27-7-5"
        except MSConverterError:
            pytest.skip("Section 27-7-5 not found in current release")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_7_sections(self):
        """Get list of sections in Chapter 7."""
        converter = MSConverter()
        sections = converter.get_chapter_section_numbers(27, 7)

        assert len(sections) > 0
        assert all(s.startswith("27-7-") for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Mississippi welfare section."""
        converter = MSConverter()
        try:
            section = converter.fetch_section("43-17-1")
            assert section.citation.section == "MS-43-17-1"
            assert "Welfare" in section.title_name
        except MSConverterError:
            pytest.skip("Section 43-17-1 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_title_caching(self):
        """Verify title caching works for multiple fetches."""
        converter = MSConverter()

        # Fetch two sections from same title
        section1 = converter.fetch_section("27-7-1")
        section2 = converter.fetch_section("27-7-3")

        # Both should succeed and title should be cached
        assert section1 is not None
        assert section2 is not None
        assert 27 in converter._title_cache
