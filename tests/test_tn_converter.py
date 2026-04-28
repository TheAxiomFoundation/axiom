"""Tests for Tennessee state statute converter.

Tests the TNConverter which parses Tennessee Code Annotated HTML from
Public.Resource.Org and converts to the internal Section model.
"""

from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.tn import (
    TN_TAX_CHAPTERS,
    TN_TITLES,
    TN_WELFARE_CHAPTERS,
    TNConverter,
    TNConverterError,
    download_tn_chapter,
    fetch_tn_section,
)
from axiom_corpus.models import Section

# Sample HTML from Public.Resource.Org's TCA for testing
SAMPLE_TITLE_67_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta content="text/html; charset=utf-8" http-equiv="Content-Type" />
  <title>tncode</title>
  <meta content="Tennessee Code Annotated" name="Subject" />
</head>
<body>
  <nav>
    <h1>
      <span class="boldspan">
        Title 67
        <br />
        Taxes And Licenses
      </span>
    </h1>
    <ul class="leaders">
      <li id="t67c01-cnav01">
        <a aria-describedby="t67c01" href="#t67c01">
          Chapter 1 General Provisions
        </a>
      </li>
    </ul>
  </nav>
  <main>
    <div>
      <h2 id="t67c01">
        <span class="boldspan">
          Chapter 1
          <br />
          General Provisions
        </span>
      </h2>
      <nav>
        <ul class="leaders">
          <li id="t67c01p01-cnav01">
            <a aria-describedby="t67c01p01" href="#t67c01p01">
              Part 1 Miscellaneous Provisions
            </a>
          </li>
        </ul>
      </nav>
      <div>
        <h2 class="parth2" id="t67c01p01">
          <span class="boldspan">
            Part 1
            <br />
            Miscellaneous Provisions
          </span>
        </h2>
        <nav>
          <ul class="leaders">
            <li id="t67c01s67-1-101-snav01">
              <a aria-describedby="t67c01s67-1-101" href="#t67c01s67-1-101">
                67-1-101. Liberal construction of title - Incidental powers of commissioner - Chapter definitions.
              </a>
            </li>
            <li id="t67c01s67-1-102-snav02">
              <a aria-describedby="t67c01s67-1-102" href="#t67c01s67-1-102">
                67-1-102. Powers and duties of commissioner and department of revenue.
              </a>
            </li>
          </ul>
        </nav>
        <div>
          <h3 id="t67c01s67-1-101">
            <span class="boldspan">
              67-1-101. Liberal construction of title - Incidental powers of commissioner - Chapter definitions.
            </span>
          </h3>
          <ol class="alpha">
            <li id="t67c01s67-1-101ol1a">
              It is declared to be the legislative intent that this title be liberally construed in favor of the jurisdiction and powers conferred upon the commissioner of revenue.
            </li>
            <li id="t67c01s67-1-101ol1b">
              The commissioner shall have and exercise all such incidental powers as may be necessary to carry out and effectuate the objects and purposes of this title.
            </li>
            <li id="t67c01s67-1-101ol1c">
              As used in this chapter, unless the context otherwise requires:
              <ol>
                <li id="t67c01s67-1-101ol1c1">
                  "Commissioner" means the commissioner of revenue; and
                </li>
                <li id="t67c01s67-1-101ol1c2">
                  "Department" means the department of revenue.
                </li>
              </ol>
            </li>
          </ol>
          <p>
            Acts 1919, ch. 1, sec. 16; 1921, ch. 113, sec. 20; T.C.A. (orig. ed.), sec. 67-107.
          </p>
          <p>
            Cross-References. Rulemaking by commissioner, sec. <cite class="octn"><a href="#t67c01s67-1-1439" target="_self">67-1-1439</a></cite>.
          </p>
        </div>
        <div>
          <h3 id="t67c01s67-1-102">
            <span class="boldspan">
              67-1-102. Powers and duties of commissioner and department of revenue.
            </span>
          </h3>
          <ol class="alpha">
            <li id="t67c01s67-1-102ol1a">
              The commissioner has the powers and shall perform the duties conferred and imposed in this chapter.
            </li>
            <li id="t67c01s67-1-102ol1b">
              The department has the power to:
              <ol>
                <li id="t67c01s67-1-102ol1b1">
                  Administer the assessment and collection of all state taxes;
                </li>
                <li id="t67c01s67-1-102ol1b2">
                  Investigate the tax systems of other states.
                </li>
              </ol>
            </li>
          </ol>
          <p>
            Acts 1921, ch. 113, sec. 2; T.C.A. (orig. ed.), sec. 67-101.
          </p>
        </div>
      </div>
    </div>
  </main>
</body>
</html>
"""

SAMPLE_TITLE_71_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <title>tncode</title>
</head>
<body>
  <nav>
    <h1>
      <span class="boldspan">
        Title 71
        <br />
        Welfare
      </span>
    </h1>
  </nav>
  <main>
    <div>
      <h2 id="t71c01">
        <span class="boldspan">
          Chapter 1
          <br />
          Administration
        </span>
      </h2>
      <div>
        <h2 class="parth2" id="t71c01p01">
          <span class="boldspan">
            Part 1
            <br />
            Department of Human Services
          </span>
        </h2>
        <div>
          <h3 id="t71c01s71-1-101">
            <span class="boldspan">
              71-1-101. Short title.
            </span>
          </h3>
          <p>
            This part may be cited as the "Welfare Organization Law of 1937."
          </p>
          <p>
            Acts 1937, ch. 48, sec. 15; T.C.A. (orig. ed.), sec. 14-101.
          </p>
        </div>
        <div>
          <h3 id="t71c01s71-1-105">
            <span class="boldspan">
              71-1-105. Powers and duties.
            </span>
          </h3>
          <p>
            The department of human services has general supervision over all matters relating to welfare in the state.
          </p>
          <p>
            Acts 1937, ch. 48, sec. 4.
          </p>
        </div>
      </div>
    </div>
  </main>
</body>
</html>
"""


class TestTNTitlesRegistry:
    """Test Tennessee title registries."""

    def test_title_67_is_taxes(self):
        """Title 67 is Taxes and Licenses."""
        assert 67 in TN_TITLES
        assert "Taxes" in TN_TITLES[67]

    def test_title_71_is_welfare(self):
        """Title 71 is Welfare."""
        assert 71 in TN_TITLES
        assert "Welfare" in TN_TITLES[71]


class TestTNChaptersRegistry:
    """Test Tennessee chapter registries."""

    def test_chapter_1_in_tax_chapters(self):
        """Chapter 1 (General Provisions) is in tax chapters."""
        assert 1 in TN_TAX_CHAPTERS
        assert "General" in TN_TAX_CHAPTERS[1]

    def test_chapter_6_in_tax_chapters(self):
        """Chapter 6 (Sales and Use Taxes) is in tax chapters."""
        assert 6 in TN_TAX_CHAPTERS
        assert "Sales" in TN_TAX_CHAPTERS[6]

    def test_chapter_1_in_welfare_chapters(self):
        """Chapter 1 (Administration) is in welfare chapters."""
        assert 1 in TN_WELFARE_CHAPTERS
        assert "Administration" in TN_WELFARE_CHAPTERS[1]

    def test_chapter_5_in_welfare_chapters(self):
        """Chapter 5 (Programs for Poor Persons) is in welfare chapters."""
        assert 5 in TN_WELFARE_CHAPTERS
        assert "Poor" in TN_WELFARE_CHAPTERS[5]


class TestTNConverter:
    """Test TNConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = TNConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.cache_title_html is True

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = TNConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_no_cache(self):
        """Converter can disable HTML caching."""
        converter = TNConverter(cache_title_html=False)
        assert converter.cache_title_html is False

    def test_build_title_url(self):
        """Build correct URL for title HTML."""
        converter = TNConverter()
        url = converter._build_title_url(67)
        assert "unicourt.github.io/cic-code-tn" in url
        assert "gov.tn.tca.title.67.html" in url

    def test_build_title_url_71(self):
        """Build correct URL for title 71."""
        converter = TNConverter()
        url = converter._build_title_url(71)
        assert "gov.tn.tca.title.71.html" in url

    def test_parse_section_number_valid(self):
        """Parse valid section number."""
        converter = TNConverter()
        title, chapter, section = converter._parse_section_number("67-1-101")
        assert title == 67
        assert chapter == 1
        assert section == 101

    def test_parse_section_number_invalid(self):
        """Raise error for invalid section number."""
        converter = TNConverter()
        with pytest.raises(TNConverterError):
            converter._parse_section_number("67-1")  # Missing section

        with pytest.raises(TNConverterError):
            converter._parse_section_number("invalid")

    def test_build_section_id(self):
        """Build correct HTML ID for section."""
        converter = TNConverter()
        section_id = converter._build_section_id("67-1-101")
        assert section_id == "t67c01s67-1-101"

    def test_build_section_id_two_digit_chapter(self):
        """Build correct HTML ID for section in chapter 10+."""
        converter = TNConverter()
        section_id = converter._build_section_id("67-10-101")
        assert section_id == "t67c10s67-10-101"

    def test_context_manager(self):
        """Converter works as context manager."""
        with TNConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None
        assert len(converter._title_cache) == 0


class TestTNConverterParsing:
    """Test TNConverter HTML parsing."""

    def test_parse_section_html_title_67(self):
        """Parse section HTML for Title 67."""
        from bs4 import BeautifulSoup

        converter = TNConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_67_HTML, "html.parser")
        parsed = converter._parse_section_html(soup, "67-1-101", "https://example.com")

        assert parsed.section_number == "67-1-101"
        assert "Liberal construction" in parsed.section_title
        assert parsed.title_number == 67
        assert parsed.title_name == "Taxes and Licenses"
        assert parsed.chapter_number == 1
        assert "General Provisions" in parsed.chapter_name
        assert parsed.part_number == 1
        assert "Miscellaneous" in parsed.part_name
        assert "legislative intent" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_section_html_title_71(self):
        """Parse section HTML for Title 71."""
        from bs4 import BeautifulSoup

        converter = TNConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_71_HTML, "html.parser")
        parsed = converter._parse_section_html(soup, "71-1-101", "https://example.com")

        assert parsed.section_number == "71-1-101"
        assert "Short title" in parsed.section_title
        assert parsed.title_number == 71
        assert parsed.title_name == "Welfare"
        assert "Welfare Organization Law" in parsed.text

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        from bs4 import BeautifulSoup

        converter = TNConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_67_HTML, "html.parser")
        parsed = converter._parse_section_html(soup, "67-1-101", "https://example.com")

        # Should have subsections (a), (b), (c)
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)
        assert any(s.identifier == "c" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (c)."""
        from bs4 import BeautifulSoup

        converter = TNConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_67_HTML, "html.parser")
        parsed = converter._parse_section_html(soup, "67-1-101", "https://example.com")

        # Find subsection (c)
        sub_c = next((s for s in parsed.subsections if s.identifier == "c"), None)
        assert sub_c is not None
        # Should have children (1) and (2)
        assert len(sub_c.children) >= 2
        assert any(c.identifier == "1" for c in sub_c.children)
        assert any(c.identifier == "2" for c in sub_c.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        from bs4 import BeautifulSoup

        converter = TNConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_67_HTML, "html.parser")
        parsed = converter._parse_section_html(soup, "67-1-101", "https://example.com")

        assert parsed.history is not None
        assert "Acts 1919" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedTNSection to Section model."""
        from bs4 import BeautifulSoup

        converter = TNConverter()
        soup = BeautifulSoup(SAMPLE_TITLE_67_HTML, "html.parser")
        parsed = converter._parse_section_html(soup, "67-1-101", "https://example.com")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "TN-67-1-101"
        assert section.citation.title == 0  # State law indicator
        assert "Liberal construction" in section.section_title
        assert "Tennessee Code" in section.title_name
        assert section.uslm_id == "tn/67/1/67-1-101"
        assert section.source_url == "https://example.com"


class TestTNConverterFetching:
    """Test TNConverter HTTP fetching with mocks."""

    @patch.object(TNConverter, "_get_title_html")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_TITLE_67_HTML

        converter = TNConverter()
        section = converter.fetch_section("67-1-101")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "TN-67-1-101"
        assert "Liberal construction" in section.section_title

    @patch.object(TNConverter, "_get_title_html")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_TITLE_67_HTML

        converter = TNConverter()
        with pytest.raises(TNConverterError) as exc_info:
            converter.fetch_section("67-1-999")  # Non-existent section

        assert "not found" in str(exc_info.value).lower()

    @patch.object(TNConverter, "_get_title_html")
    def test_fetch_section_title_71(self, mock_get):
        """Fetch section from Title 71."""
        mock_get.return_value = SAMPLE_TITLE_71_HTML

        converter = TNConverter()
        section = converter.fetch_section("71-1-101")

        assert section is not None
        assert section.citation.section == "TN-71-1-101"
        assert "Short title" in section.section_title
        assert "Welfare" in section.title_name

    @patch.object(TNConverter, "_get_title_html")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter."""
        mock_get.return_value = SAMPLE_TITLE_67_HTML

        converter = TNConverter()
        sections = converter.get_chapter_section_numbers(67, 1)

        assert len(sections) == 2
        assert "67-1-101" in sections
        assert "67-1-102" in sections

    @patch.object(TNConverter, "_get_title_html")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.return_value = SAMPLE_TITLE_67_HTML

        converter = TNConverter()
        sections = list(converter.iter_chapter(67, 1))

        assert len(sections) == 2
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(TNConverter, "_get_title_html")
    def test_fetch_tn_section(self, mock_get):
        """Test fetch_tn_section function."""
        mock_get.return_value = SAMPLE_TITLE_67_HTML

        section = fetch_tn_section("67-1-101")

        assert section is not None
        assert section.citation.section == "TN-67-1-101"

    @patch.object(TNConverter, "_get_title_html")
    def test_download_tn_chapter(self, mock_get):
        """Test download_tn_chapter function."""
        mock_get.return_value = SAMPLE_TITLE_67_HTML

        sections = download_tn_chapter(67, 1)

        assert len(sections) == 2
        assert all(isinstance(s, Section) for s in sections)


class TestTNConverterIntegration:
    """Integration tests that hit real Public.Resource.Org (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_section(self):
        """Fetch Tennessee Tax section 67-1-101."""
        converter = TNConverter()
        section = converter.fetch_section("67-1-101")

        assert section is not None
        assert section.citation.section == "TN-67-1-101"
        assert "commissioner" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Tennessee Welfare section 71-1-101."""
        converter = TNConverter()
        section = converter.fetch_section("71-1-101")

        assert section is not None
        assert section.citation.section == "TN-71-1-101"

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_67_1_sections(self):
        """Get list of sections in Title 67, Chapter 1."""
        converter = TNConverter()
        sections = converter.get_chapter_section_numbers(67, 1)

        assert len(sections) > 0
        assert all(s.startswith("67-1-") for s in sections)
        assert "67-1-101" in sections

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch Tennessee Sales Tax section 67-6-101."""
        converter = TNConverter()
        try:
            section = converter.fetch_section("67-6-101")
            assert section.citation.section == "TN-67-6-101"
        except TNConverterError:
            pytest.skip("Section 67-6-101 not found in cached version")
