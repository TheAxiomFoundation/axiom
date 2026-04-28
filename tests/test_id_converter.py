"""Tests for Idaho state statute converter.

Tests the IDConverter which fetches from legislature.idaho.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.id_ import (
    ID_TAX_CHAPTERS,
    ID_TITLES,
    ID_WELFARE_CHAPTERS,
    IDConverter,
    IDConverterError,
    download_id_chapter,
    fetch_id_section,
)
from axiom_corpus.models import Section

# Sample HTML from legislature.idaho.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html lang="en-US">
<head>
    <title>Section 63-3002 - Idaho State Legislature</title>
</head>
<body>
<div style="line-height: 12pt; text-align: center"><span class="f11s" style="font-family: Courier New;">TITLE 63</span></div>
<div style="line-height: 12pt; text-align: center"><span class="f11s" style="font-family: Courier New;">REVENUE AND TAXATION</span></div>
<div style="line-height: 12pt; text-align: center; padding-top: 12pt"><span class="f11s" style="font-family: Courier New;">CHAPTER 30</span></div>
<div style="line-height: 12pt; text-align: center"><span class="f11s" style="font-family: Courier New;">INCOME TAX</span></div>
<div style="line-height: 12pt; text-align: justify; text-indent: 5.9%; padding-top: 12pt">
    <span class="f11s" style="font-family: Courier New;">
        63-3002.  <span style="text-transform: uppercase">Declaration of intent. </span>
        It is the intent of the legislature by the adoption of this act, insofar as possible
        to make the provisions of the Idaho act identical to the provisions of the Federal
        Internal Revenue Code relating to the measurement of taxable income, to the end that
        the taxable income reported each taxable year by a taxpayer to the internal revenue
        service shall be the identical sum reported to this state, subject only to modifications
        contained in the Idaho law.
    </span>
</div>
<div style="line-height: 12pt; text-align: justify">
    <span style="font-size: 11pt; font-family: Courier New;">History:</span>
</div>
<div style="line-height: 12pt; text-align: justify; text-indent: 5.9%">
    <span class="f11s" style="font-family: Courier New;">
        [63-3002, added 1959, ch. 299, sec. 2, p. 613; am. 1969, ch. 319, sec. 1, p. 982.]
    </span>
</div>
</body>
</html>
"""

SAMPLE_SECTION_WITH_SUBSECTIONS_HTML = """<!DOCTYPE html>
<html lang="en-US">
<head>
    <title>Section 63-3022 - Idaho State Legislature</title>
</head>
<body>
<div style="line-height: 12pt; text-align: center"><span class="f11s" style="font-family: Courier New;">TITLE 63</span></div>
<div style="line-height: 12pt; text-align: center"><span class="f11s" style="font-family: Courier New;">REVENUE AND TAXATION</span></div>
<div style="line-height: 12pt; text-align: justify; text-indent: 5.9%; padding-top: 12pt">
    <span class="f11s" style="font-family: Courier New;">
        63-3022.  <span style="text-transform: uppercase">Adjustments to taxable income. </span>
        The additions and subtractions set forth in this section are to be applied in computing Idaho taxable income:
    </span>
</div>
<div style="line-height: 12pt; text-align: justify; text-indent: 5.9%">
    <span class="f11s" style="font-family: Courier New;">
        (a)  Add any state and local taxes, as defined in section 164 of the Internal Revenue Code
        that are measured by net income.
    </span>
</div>
<div style="line-height: 12pt; text-align: justify; text-indent: 5.9%">
    <span class="f11s" style="font-family: Courier New;">
        (b)  Add the net operating loss deduction used in arriving at taxable income.
    </span>
</div>
<div style="line-height: 12pt; text-align: justify; padding-left: 5.9%">
    <span class="f11s" style="font-family: Courier New;">
        (c)(1)  A net operating loss for any taxable year shall be a net operating loss carryback.
    </span>
</div>
<div style="line-height: 12pt; text-align: justify; padding-left: 5.9%">
    <span class="f11s" style="font-family: Courier New;">
        (2)  Any portion of the net operating loss not subtracted may be carried forward.
    </span>
</div>
<div style="line-height: 12pt; text-align: justify">
    <span style="font-size: 11pt; font-family: Courier New;">History:</span>
</div>
<div style="line-height: 12pt; text-align: justify; text-indent: 5.9%">
    <span class="f11s" style="font-family: Courier New;">
        [63-3022, added 1959, ch. 299, sec. 22, p. 613; am. 2025, ch. 13, sec. 1, p. 39.]
    </span>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html lang="en-US">
<head>
    <title>Chapter 30 - Idaho State Legislature</title>
</head>
<body>
<h1>Chapter 30 - Income Tax</h1>
<table>
<tr>
    <td valign="top" nowrap="true">
        <a href="/statutesrules/idstat/Title63/T63CH30/SECT63-3001">63-3001</a>
    </td>
    <td>Short title</td>
</tr>
<tr>
    <td valign="top" nowrap="true">
        <a href="/statutesrules/idstat/Title63/T63CH30/SECT63-3002">63-3002</a>
    </td>
    <td>Declaration of intent</td>
</tr>
<tr>
    <td valign="top" nowrap="true">
        <a href="/statutesrules/idstat/Title63/T63CH30/SECT63-3003">63-3003</a>
    </td>
    <td>Definitions</td>
</tr>
<tr>
    <td valign="top" nowrap="true">
        <a href="/statutesrules/idstat/Title63/T63CH30/SECT63-3022A">63-3022A</a>
    </td>
    <td>Additional adjustments</td>
</tr>
</table>
</body>
</html>
"""

SAMPLE_TITLE_INDEX_HTML = """<!DOCTYPE html>
<html lang="en-US">
<head>
    <title>Title 63 - Idaho State Legislature</title>
</head>
<body>
<h1>Title 63 - Revenue and Taxation</h1>
<ul>
    <li><a href="/statutesrules/idstat/Title63/T63CH1">Chapter 1 - Property Taxes</a></li>
    <li><a href="/statutesrules/idstat/Title63/T63CH30">Chapter 30 - Income Tax</a></li>
    <li><a href="/statutesrules/idstat/Title63/T63CH35">Chapter 35 - Sales Tax</a></li>
</ul>
</body>
</html>
"""


class TestIDTitlesRegistry:
    """Test Idaho title registries."""

    def test_title_63_in_titles(self):
        """Title 63 (Revenue and Taxation) is in titles."""
        assert 63 in ID_TITLES
        assert "Revenue" in ID_TITLES[63]

    def test_title_56_in_titles(self):
        """Title 56 (Public Assistance and Welfare) is in titles."""
        assert 56 in ID_TITLES
        assert "Public Assistance" in ID_TITLES[56]

    def test_chapter_30_in_tax_chapters(self):
        """Chapter 30 (Income Tax) is in tax chapters."""
        assert 30 in ID_TAX_CHAPTERS
        assert "Income Tax" in ID_TAX_CHAPTERS[30]

    def test_chapter_35_in_tax_chapters(self):
        """Chapter 35 (Sales Tax) is in tax chapters."""
        assert 35 in ID_TAX_CHAPTERS
        assert "Sales Tax" in ID_TAX_CHAPTERS[35]

    def test_chapter_2_in_welfare_chapters(self):
        """Chapter 2 (Public Assistance Law) is in welfare chapters."""
        assert 2 in ID_WELFARE_CHAPTERS
        assert "Public Assistance" in ID_WELFARE_CHAPTERS[2]


class TestIDConverter:
    """Test IDConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = IDConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = IDConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = IDConverter(year=2024)
        assert converter.year == 2024

    def test_parse_section_number_simple(self):
        """Parse simple section number."""
        converter = IDConverter()
        title, chapter, section = converter._parse_section_number("63-3002")
        assert title == 63
        assert chapter == 30
        assert section == "3002"

    def test_parse_section_number_with_letter(self):
        """Parse section number with letter suffix."""
        converter = IDConverter()
        title, chapter, section = converter._parse_section_number("63-3022A")
        assert title == 63
        assert chapter == 30
        assert section == "3022A"

    def test_parse_section_number_title_56(self):
        """Parse section number from Title 56."""
        converter = IDConverter()
        title, chapter, section = converter._parse_section_number("56-202")
        assert title == 56
        # 202 // 100 = 2
        assert chapter == 2
        assert section == "202"

    def test_parse_section_number_chapter_derivation(self):
        """Test chapter derivation from section number."""
        converter = IDConverter()
        # 3002 // 100 = 30
        _, chapter, _ = converter._parse_section_number("63-3002")
        assert chapter == 30
        # 201 // 100 = 2
        _, chapter, _ = converter._parse_section_number("56-201")
        assert chapter == 2
        # 101 // 100 = 1
        _, chapter, _ = converter._parse_section_number("56-101")
        assert chapter == 1

    def test_parse_section_number_invalid(self):
        """Parse invalid section number raises error."""
        converter = IDConverter()
        with pytest.raises(IDConverterError):
            converter._parse_section_number("invalid")

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = IDConverter()
        url = converter._build_section_url("63-3002")
        assert "legislature.idaho.gov" in url
        assert "Title63" in url
        assert "T63CH30" in url
        assert "SECT63-3002" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = IDConverter()
        url = converter._build_chapter_url(63, 30)
        assert "Title63" in url
        assert "T63CH30" in url

    def test_build_title_url(self):
        """Build correct URL for title contents."""
        converter = IDConverter()
        url = converter._build_title_url(63)
        assert "Title63" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with IDConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestIDConverterParsing:
    """Test IDConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedIDSection."""
        converter = IDConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "63-3002", "https://example.com"
        )

        assert parsed.section_number == "63-3002"
        assert "declaration" in parsed.section_title.lower() or "intent" in parsed.section_title.lower()
        assert parsed.title_number == 63
        assert parsed.chapter_number == 30
        assert "Internal Revenue Code" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = IDConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "63-3022", "https://example.com"
        )

        # Should have subsections - may be parsed as letters or numbers
        # depending on which pattern dominates
        assert len(parsed.subsections) >= 2
        # Verify we got some identifiers
        identifiers = [s.identifier for s in parsed.subsections]
        assert len(identifiers) >= 2

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = IDConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "63-3002", "https://example.com"
        )

        assert parsed.history is not None
        assert "1959" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedIDSection to Section model."""
        converter = IDConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "63-3002", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "ID-63-3002"
        assert section.citation.title == 0  # State law indicator
        assert "Idaho Statutes" in section.title_name
        assert section.uslm_id == "id/63/63-3002"
        assert section.source_url == "https://example.com"


class TestIDConverterFetching:
    """Test IDConverter HTTP fetching with mocks."""

    @patch.object(IDConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = IDConverter()
        section = converter.fetch_section("63-3002")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "ID-63-3002"

    @patch.object(IDConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Page not found</body></html>"

        converter = IDConverter()
        with pytest.raises(IDConverterError) as exc_info:
            converter.fetch_section("99-9999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(IDConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = IDConverter()
        sections = converter.get_chapter_section_numbers(63, 30)

        assert len(sections) == 4
        assert "63-3001" in sections
        assert "63-3002" in sections
        assert "63-3003" in sections
        assert "63-3022A" in sections

    @patch.object(IDConverter, "_get")
    def test_get_title_chapters(self, mock_get):
        """Get list of chapters from title index."""
        mock_get.return_value = SAMPLE_TITLE_INDEX_HTML

        converter = IDConverter()
        chapters = converter.get_title_chapters(63)

        assert len(chapters) == 3
        assert 1 in chapters
        assert 30 in chapters
        assert 35 in chapters

    @patch.object(IDConverter, "_get")
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

        converter = IDConverter()
        sections = list(converter.iter_chapter(63, 30))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(IDConverter, "_get")
    def test_fetch_id_section(self, mock_get):
        """Test fetch_id_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_id_section("63-3002")

        assert section is not None
        assert section.citation.section == "ID-63-3002"

    @patch.object(IDConverter, "_get")
    def test_download_id_chapter(self, mock_get):
        """Test download_id_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_id_chapter(63, 30)

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestIDConverterIntegration:
    """Integration tests that hit real legislature.idaho.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Idaho Income Tax section 63-3002."""
        converter = IDConverter()
        section = converter.fetch_section("63-3002")

        assert section is not None
        assert section.citation.section == "ID-63-3002"
        assert "intent" in section.section_title.lower() or "declaration" in section.section_title.lower()
        assert "income" in section.text.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_section_with_subsections(self):
        """Fetch Idaho Income Tax section 63-3022 with subsections."""
        converter = IDConverter()
        section = converter.fetch_section("63-3022")

        assert section is not None
        assert section.citation.section == "ID-63-3022"
        # Should have subsections
        assert len(section.subsections) > 0

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_30_sections(self):
        """Get list of sections in Chapter 30 (Income Tax)."""
        converter = IDConverter()
        sections = converter.get_chapter_section_numbers(63, 30)

        assert len(sections) > 0
        assert all(s.startswith("63-30") for s in sections)
        assert "63-3002" in sections  # Declaration of intent

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_63_chapters(self):
        """Get list of chapters in Title 63."""
        converter = IDConverter()
        chapters = converter.get_title_chapters(63)

        assert len(chapters) > 0
        assert 30 in chapters  # Income Tax
        assert 35 in chapters  # Sales Tax

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Idaho welfare section 56-202."""
        converter = IDConverter()
        try:
            section = converter.fetch_section("56-202")
            assert section.citation.section == "ID-56-202"
        except IDConverterError:
            # Section may not exist, which is acceptable
            pytest.skip("Section 56-202 not found")
