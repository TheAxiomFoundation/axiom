"""Tests for Wisconsin state statute converter.

Tests the WIConverter which fetches from docs.legis.wisconsin.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.wi import (
    WI_TAX_CHAPTERS,
    WI_WELFARE_CHAPTERS,
    WIConverter,
    WIConverterError,
    download_wi_chapter,
    fetch_wi_section,
)
from axiom_corpus.models import Section

# Sample HTML from docs.legis.wisconsin.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Wisconsin Legislature: 71.01</title>
</head>
<body>
<div class="bar noprint">
  <div class="container">
    <ul class="breadcrumb rounded">
      <li><a href="/">Menu</a></li>
      <li><span class='divider'>></span><a href="/statutes">Statutes Related</a></li>
      <li><span class='divider'>></span><a href="/statutes/statutes">Statutes</a></li>
      <li><span class='divider'>></span><a href="/statutes/statutes/71">Chapter 71</a></li>
    </ul>
  </div>
</div>
<div id="document" class="statutes">
    <div class="qsnum_subchap" data-cites='["statutes/subch. I of ch. 71"]'>
      <span class="qstr">SUBCHAPTER I</span>
    </div>
    <div class="qstitle_subchap">
      <span class="qstr">TAXATION OF INDIVIDUALS AND FIDUCIARIES</span>
    </div>
    <div class="qsatxt_1sect level3" data-section="71.01">
      <a class="reference" href="/document/statutes/71.01">71.01</a>
      <span class="qsnum_sect"><span class="qstr">71.01</span></span>
      <span class="qstitle_sect"><span class="qstr">Definitions.</span></span>
      <span class="qstr">In this chapter in regard to natural persons and fiduciaries, except fiduciaries of nuclear decommissioning trust or reserve funds:</span>
    </div>
    <div class="qsatxt_2subsect level4" data-section="71.01">
      <a class="reference" href="/document/statutes/71.01(1)">71.01(1)</a>
      <span class="qsnum_subsect"><span>(1)</span></span>
      <span class="qstr">"Adjusted gross income", when not preceded by the word "federal", means Wisconsin adjusted gross income, unless otherwise defined or the context plainly requires otherwise.</span>
    </div>
    <div class="qsatxt_2subsect level4" data-section="71.01">
      <a class="reference" href="/document/statutes/71.01(2)">71.01(2)</a>
      <span class="qsnum_subsect"><span>(2)</span></span>
      <span class="qstr">"Claimant" means an individual who files a claim for credit under this chapter and who was a resident of this state during the entire taxable year for which a claim for credit is filed.</span>
    </div>
    <div class="qsatxt_2subsect level4" data-section="71.01">
      <a class="reference" href="/document/statutes/71.01(3)">71.01(3)</a>
      <span class="qsnum_subsect"><span>(3)</span></span>
      <span class="qstr">"Department" means the department of revenue.</span>
    </div>
    <div class="qsatxt_2subsect level4" data-section="71.01">
      <a class="reference" href="/document/statutes/71.01(3m)">71.01(3m)</a>
      <span class="qsnum_subsect"><span>(3m)</span></span>
      <span class="qstr">"Dependent" means any of the following:</span>
    </div>
    <div class="qsatxt_3par level5" data-section="71.01">
      <a class="reference" href="/document/statutes/71.01(3m)(a)">71.01(3m)(a)</a>
      <span class="qsnum_subsect"><span>(a)</span></span>
      <span class="qstr">A qualifying child of the claimant.</span>
    </div>
    <div class="qsatxt_3par level5" data-section="71.01">
      <a class="reference" href="/document/statutes/71.01(3m)(b)">71.01(3m)(b)</a>
      <span class="qsnum_subsect"><span>(b)</span></span>
      <span class="qstr">A qualifying relative of the claimant.</span>
    </div>
    <div class="qshistory" data-section="71.01">
      <span class="qstr">71.01 HistoryHistory: 1973 c. 147; 1975 c. 39, 224; 1979 c. 1; 1981 c. 20; 1983 a. 27; 1985 a. 29; 1987 a. 312, 411; 1989 a. 31.</span>
    </div>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Wisconsin Legislature: Chapter 71</title>
</head>
<body>
<div id="document" class="statutes">
    <div class="qsnum_chap"><span class="qstr">CHAPTER 71</span></div>
    <div class="qstitle_chap"><span>INCOME AND FRANCHISE TAXES FOR STATE AND LOCAL REVENUES</span></div>
    <div class="qstoc_subchap"><span class="qstr">SUBCHAPTER I</span></div>
    <div class="qstoc_subchap"><span class="qstr">TAXATION OF INDIVIDUALS AND FIDUCIARIES</span></div>
    <div class="qstoc_entry">
      <span class="qstr"><a rel="statutes/71.01" href="/document/statutes/71.01" title="Statutes 71.01">71.01</a>
      <span class="qstab"></span>Definitions.</span>
    </div>
    <div class="qstoc_entry">
      <span class="qstr"><a rel="statutes/71.02" href="/document/statutes/71.02" title="Statutes 71.02">71.02</a>
      <span class="qstab"></span>Imposition of tax.</span>
    </div>
    <div class="qstoc_entry">
      <span class="qstr"><a rel="statutes/71.03" href="/document/statutes/71.03" title="Statutes 71.03">71.03</a>
      <span class="qstab"></span>Filing returns; certain claims.</span>
    </div>
    <div class="qstoc_entry">
      <span class="qstr"><a rel="statutes/71.04" href="/document/statutes/71.04" title="Statutes 71.04">71.04</a>
      <span class="qstab"></span>Situs of income; allocation and apportionment.</span>
    </div>
    <div class="qstoc_entry">
      <span class="qstr"><a rel="statutes/71.05" href="/document/statutes/71.05" title="Statutes 71.05">71.05</a>
      <span class="qstab"></span>Income computation.</span>
    </div>
    <div class="qstoc_entry">
      <span class="qstr"><a rel="statutes/71.06" href="/document/statutes/71.06" title="Statutes 71.06">71.06</a>
      <span class="qstab"></span>Rates of taxation.</span>
    </div>
    <div class="qstoc_entry">
      <span class="qstr"><a rel="statutes/71.07" href="/document/statutes/71.07" title="Statutes 71.07">71.07</a>
      <span class="qstab"></span>Credits.</span>
    </div>
</div>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Wisconsin Legislature: 49.01</title>
</head>
<body>
<div id="document" class="statutes">
    <div class="qsnum_chap"><span class="qstr">CHAPTER 49</span></div>
    <div class="qstitle_chap"><span>PUBLIC ASSISTANCE</span></div>
    <div class="qsatxt_1sect level3" data-section="49.01">
      <a class="reference" href="/document/statutes/49.01">49.01</a>
      <span class="qsnum_sect"><span class="qstr">49.01</span></span>
      <span class="qstitle_sect"><span class="qstr">Definitions.</span></span>
      <span class="qstr">In this chapter, unless the context requires otherwise:</span>
    </div>
    <div class="qsatxt_2subsect level4" data-section="49.01">
      <a class="reference" href="/document/statutes/49.01(1)">49.01(1)</a>
      <span class="qsnum_subsect"><span>(1)</span></span>
      <span class="qstr">"Caretaker" means a person who resides with and provides care and maintenance for a dependent child.</span>
    </div>
    <div class="qsatxt_2subsect level4" data-section="49.01">
      <a class="reference" href="/document/statutes/49.01(1g)">49.01(1g)</a>
      <span class="qsnum_subsect"><span>(1g)</span></span>
      <span class="qstr">"County department" means a county department under s. 46.215, 46.22 or 46.23.</span>
    </div>
    <div class="qsatxt_2subsect level4" data-section="49.01">
      <a class="reference" href="/document/statutes/49.01(1m)">49.01(1m)</a>
      <span class="qsnum_subsect"><span>(1m)</span></span>
      <span class="qstr">"Dependent child" means any person under the age of 18 who is not married or who is in need of parental care.</span>
    </div>
</div>
</body>
</html>
"""


class TestWIChaptersRegistry:
    """Test Wisconsin chapter registries."""

    def test_chapter_71_in_tax_chapters(self):
        """Chapter 71 (Income and Franchise Taxes) is in tax chapters."""
        assert 71 in WI_TAX_CHAPTERS
        assert "Income" in WI_TAX_CHAPTERS[71]

    def test_chapter_77_in_tax_chapters(self):
        """Chapter 77 (Sales Tax) is in tax chapters."""
        assert 77 in WI_TAX_CHAPTERS
        assert "Sales" in WI_TAX_CHAPTERS[77]

    def test_chapter_49_in_welfare_chapters(self):
        """Chapter 49 (Public Assistance) is in welfare chapters."""
        assert 49 in WI_WELFARE_CHAPTERS
        assert "Public Assistance" in WI_WELFARE_CHAPTERS[49]

    def test_chapter_46_in_welfare_chapters(self):
        """Chapter 46 (Social Services) is in welfare chapters."""
        assert 46 in WI_WELFARE_CHAPTERS
        assert "Social Services" in WI_WELFARE_CHAPTERS[46]


class TestWIConverter:
    """Test WIConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = WIConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = WIConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = WIConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = WIConverter()
        url = converter._build_section_url("71.01")
        assert "docs.legis.wisconsin.gov" in url
        assert "document/statutes/71.01" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = WIConverter()
        url = converter._build_chapter_url(71)
        assert "statutes/statutes/71" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with WIConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestWIConverterParsing:
    """Test WIConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedWISection."""
        converter = WIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "71.01", "https://example.com"
        )

        assert parsed.section_number == "71.01"
        assert parsed.section_title == "Definitions"
        assert parsed.chapter_number == 71
        assert "Income" in parsed.chapter_title
        assert "Adjusted gross income" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subchapter(self):
        """Parse subchapter info from section HTML."""
        converter = WIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "71.01", "https://example.com"
        )

        assert parsed.subchapter is not None
        assert "TAXATION OF INDIVIDUALS" in parsed.subchapter.upper()

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = WIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "71.01", "https://example.com"
        )

        # Should have subsections (1), (2), (3), (3m)
        assert len(parsed.subsections) >= 3
        identifiers = [s.identifier for s in parsed.subsections]
        assert "1" in identifiers
        assert "2" in identifiers
        assert "3" in identifiers

    def test_parse_variant_subsection_numbers(self):
        """Parse Wisconsin variant subsection numbers like (1m), (1g)."""
        converter = WIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "49.01", "https://example.com"
        )

        identifiers = [s.identifier for s in parsed.subsections]
        # Should recognize (1g) and (1m) variants
        assert "1" in identifiers or "1g" in identifiers or "1m" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under numbered subsections."""
        converter = WIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "71.01", "https://example.com"
        )

        # Find subsection (3m) which has children (a), (b)
        sub_3m = next((s for s in parsed.subsections if s.identifier == "3m"), None)
        if sub_3m:
            assert len(sub_3m.children) >= 2
            child_ids = [c.identifier for c in sub_3m.children]
            assert "a" in child_ids
            assert "b" in child_ids

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = WIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "71.01", "https://example.com"
        )

        assert parsed.history is not None
        assert "1973 c. 147" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedWISection to Section model."""
        converter = WIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "71.01", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "WI-71.01"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "Wisconsin Statutes" in section.title_name
        assert section.uslm_id == "wi/71/71.01"
        assert section.source_url == "https://example.com"

    def test_welfare_section_parsing(self):
        """Parse welfare chapter section (Chapter 49)."""
        converter = WIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "49.01", "https://example.com"
        )

        assert parsed.chapter_number == 49
        assert "Public Assistance" in parsed.chapter_title
        assert parsed.section_title == "Definitions"


class TestWIConverterFetching:
    """Test WIConverter HTTP fetching with mocks."""

    @patch.object(WIConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = WIConverter()
        section = converter.fetch_section("71.01")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "WI-71.01"
        assert "Definitions" in section.section_title

    @patch.object(WIConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section not found</body></html>"

        converter = WIConverter()
        with pytest.raises(WIConverterError) as exc_info:
            converter.fetch_section("999.99")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(WIConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = WIConverter()
        sections = converter.get_chapter_section_numbers(71)

        assert len(sections) >= 5
        assert "71.01" in sections
        assert "71.02" in sections
        assert "71.06" in sections

    @patch.object(WIConverter, "_get")
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
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = WIConverter()
        sections = list(converter.iter_chapter(71))

        assert len(sections) >= 5
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(WIConverter, "_get")
    def test_fetch_wi_section(self, mock_get):
        """Test fetch_wi_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_wi_section("71.01")

        assert section is not None
        assert section.citation.section == "WI-71.01"

    @patch.object(WIConverter, "_get")
    def test_download_wi_chapter(self, mock_get):
        """Test download_wi_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_wi_chapter(71)

        assert len(sections) >= 5
        assert all(isinstance(s, Section) for s in sections)


class TestWIConverterIntegration:
    """Integration tests that hit real docs.legis.wisconsin.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Wisconsin Income Tax section 71.01."""
        converter = WIConverter()
        section = converter.fetch_section("71.01")

        assert section is not None
        assert section.citation.section == "WI-71.01"
        assert "definition" in section.section_title.lower()
        assert "income" in section.text.lower() or "adjusted gross" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_rates_section(self):
        """Fetch Wisconsin tax rates section 71.06."""
        converter = WIConverter()
        section = converter.fetch_section("71.06")

        assert section is not None
        assert section.citation.section == "WI-71.06"
        assert "rate" in section.text.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_71_sections(self):
        """Get list of sections in Chapter 71."""
        converter = WIConverter()
        sections = converter.get_chapter_section_numbers(71)

        assert len(sections) > 0
        assert all(s.startswith("71.") for s in sections)
        assert "71.01" in sections  # Definitions

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_public_assistance_section(self):
        """Fetch Wisconsin public assistance section 49.01."""
        converter = WIConverter()
        section = converter.fetch_section("49.01")

        assert section is not None
        assert section.citation.section == "WI-49.01"
        assert "Public Assistance" in section.title_name
