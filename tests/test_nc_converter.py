"""Tests for North Carolina state statute converter.

Tests the NCConverter which fetches from ncleg.gov and converts to the
internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.nc import (
    NC_SOCIAL_CHAPTERS,
    NC_TAX_CHAPTERS,
    NCConverter,
    NCConverterError,
    download_nc_chapter,
    fetch_nc_section,
)
from axiom.models import Section

# Sample HTML from ncleg.gov for testing (based on actual structure)
SAMPLE_SECTION_HTML = """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" /><title>
            G.S. 105-130.2</title>
        <style type="text/css">
            .cs8E357F70{text-align:justify;text-indent:-54pt;margin:0pt 0pt 0pt 54pt}
            .cs72F7C9C5{color:#000000;background-color:transparent;font-family:'Times New Roman';font-size:12pt;font-weight:bold;font-style:normal;}
            .cs4817DA29{text-align:justify;text-indent:18pt;margin:0pt 0pt 0pt 0pt}
            .cs9D249CCB{color:#000000;background-color:transparent;font-family:'Times New Roman';font-size:12pt;font-weight:normal;font-style:normal;}
            .cs10EB6B29{text-align:justify;text-indent:-36pt;margin:0pt 0pt 0pt 90pt}
        </style>
    </head>
    <body>
        <p class="cs8E357F70"><span class="cs72F7C9C5">&sect; 105-130.2. &nbsp;Definitions.</span></p>
        <p class="cs4817DA29" style="tab-stops:left 54pt;"><span class="cs9D249CCB">The following definitions apply in this Part:</span></p>
        <p class="cs10EB6B29"><span class="cs9D249CCB">(1)	Affiliate. - A corporation is an affiliate of another corporation when both are directly or indirectly controlled by the same parent corporation.</span></p>
        <p class="cs10EB6B29"><span class="cs9D249CCB">(2)	Code. - Defined in G.S. 105-228.90.</span></p>
        <p class="cs10EB6B29"><span class="cs9D249CCB">(3)	Corporation. - A joint-stock company or association, an insurance company, a domestic corporation, a foreign corporation, or a limited liability company.</span></p>
        <p class="cs10EB6B29"><span class="cs9D249CCB">(4)	C Corporation. - A corporation that is not an S Corporation.</span></p>
        <p class="cs10EB6B29"><span class="cs9D249CCB">(5)	Department. - The Department of Revenue.  (1939, c. 158, s. 302; 2013-157, s. 27.)</span></p>
    </body>
</html>
"""

SAMPLE_SECTION_HTML_LETTER = """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" /><title>
            G.S. 108A-25</title>
        <style type="text/css">
            .cs8E357F70{text-align:justify;text-indent:-54pt;margin:0pt 0pt 0pt 54pt}
            .cs72F7C9C5{color:#000000;background-color:transparent;font-family:'Times New Roman';font-size:12pt;font-weight:bold;font-style:normal;}
            .cs4817DA29{text-align:justify;text-indent:18pt;margin:0pt 0pt 0pt 0pt}
            .cs9D249CCB{color:#000000;background-color:transparent;font-family:'Times New Roman';font-size:12pt;font-weight:normal;font-style:normal;}
            .cs10EB6B29{text-align:justify;text-indent:-36pt;margin:0pt 0pt 0pt 90pt}
        </style>
    </head>
    <body>
        <p class="cs8E357F70"><span class="cs72F7C9C5">&sect; 108A-25. &nbsp;Creation of programs; assumption by federally recognized tribe of programs.</span></p>
        <p class="cs4817DA29" style="tab-stops:left 54pt;"><span class="cs9D249CCB">(a) The following programs of public assistance are established:</span></p>
        <p class="cs10EB6B29"><span class="cs9D249CCB">(1)	Repealed by S.L. 1997-443.</span></p>
        <p class="cs10EB6B29"><span class="cs9D249CCB">(2)	State-county special assistance.</span></p>
        <p class="cs10EB6B29"><span class="cs9D249CCB">(3)	Food and Nutrition Services.</span></p>
        <p class="cs4817DA29" style="tab-stops:left 54pt;"><span class="cs9D249CCB">(b) The program of medical assistance is established as a program of public assistance.</span></p>
        <p class="cs4817DA29" style="tab-stops:left 54pt;"><span class="cs9D249CCB">(c) The Department may accept all grants-in-aid.  (1937, c. 135, s. 1; 2023-7, s. 1.8(b).)</span></p>
    </body>
</html>
"""

SAMPLE_CHAPTER_HTML = """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" /><title>
            Chapter 105</title>
        <style type="text/css">
            .cs72F7C9C5{color:#000000;background-color:transparent;font-family:'Times New Roman';font-size:12pt;font-weight:bold;font-style:normal;}
            .cs9D249CCB{color:#000000;background-color:transparent;font-family:'Times New Roman';font-size:12pt;font-weight:normal;font-style:normal;}
        </style>
    </head>
    <body>
        <h3><span class="cs72F7C9C5">Chapter 105. </span></h3>
        <h3><span class="cs72F7C9C5">Taxation. </span></h3>
        <p><span class="cs72F7C9C5">&sect; 105-1. &nbsp;Title and purpose of Subchapter.</span></p>
        <p><span class="cs9D249CCB">The title of this Subchapter shall be "The Revenue Act."</span></p>
        <p><span class="cs72F7C9C5">&sect; 105-1.1. &nbsp;Supremacy of State Constitution.</span></p>
        <p><span class="cs9D249CCB">The State's power of taxation is vested in the General Assembly.</span></p>
        <p><span class="cs72F7C9C5">&sect; 105-33. &nbsp;Taxes under this Article.</span></p>
        <p><span class="cs9D249CCB">Taxes in this Article are imposed for the privilege.</span></p>
        <p><span class="cs72F7C9C5">&sect;&sect; 105-2 through 105-32: &nbsp;Repealed by Session Laws 1998-212.</span></p>
    </body>
</html>
"""


class TestNCChaptersRegistry:
    """Test North Carolina chapter registries."""

    def test_chapter_105_in_tax_chapters(self):
        """Chapter 105 (Taxation) is in tax chapters."""
        assert 105 in NC_TAX_CHAPTERS
        assert "Taxation" in NC_TAX_CHAPTERS[105]

    def test_chapter_108a_in_social_chapters(self):
        """Chapter 108A (Social Services) is in social chapters."""
        assert "108A" in NC_SOCIAL_CHAPTERS
        assert "Social Services" in NC_SOCIAL_CHAPTERS["108A"]

    def test_chapter_108d_in_social_chapters(self):
        """Chapter 108D (NC Health Choice) is in social chapters."""
        assert "108D" in NC_SOCIAL_CHAPTERS
        assert "Health" in NC_SOCIAL_CHAPTERS["108D"]


class TestNCConverter:
    """Test NCConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = NCConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = NCConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = NCConverter(year=2024)
        assert converter.year == 2024

    def test_get_chapter_from_section_numeric(self):
        """Extract chapter from numeric section number."""
        converter = NCConverter()
        assert converter._get_chapter_from_section("105-130.2") == "105"
        assert converter._get_chapter_from_section("105-1") == "105"

    def test_get_chapter_from_section_alphanumeric(self):
        """Extract chapter from alphanumeric section number."""
        converter = NCConverter()
        assert converter._get_chapter_from_section("108A-25") == "108A"
        assert converter._get_chapter_from_section("108D-1") == "108D"

    def test_build_section_url_numeric(self):
        """Build correct URL for numeric chapter section."""
        converter = NCConverter()
        url = converter._build_section_url("105-130.2")
        assert "ncleg.gov" in url
        assert "Chapter_105" in url
        assert "GS_105-130.2.html" in url

    def test_build_section_url_alphanumeric(self):
        """Build correct URL for alphanumeric chapter section."""
        converter = NCConverter()
        url = converter._build_section_url("108A-25")
        assert "ncleg.gov" in url
        assert "Chapter_108A" in url
        assert "GS_108A-25.html" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter page."""
        converter = NCConverter()
        url = converter._build_chapter_url(105)
        assert "Chapter_105.html" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with NCConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestNCConverterParsing:
    """Test NCConverter HTML parsing."""

    def test_parse_section_html_numbered(self):
        """Parse section HTML with numbered subsections."""
        converter = NCConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "105-130.2", "https://example.com"
        )

        assert parsed.section_number == "105-130.2"
        assert parsed.section_title == "Definitions"
        assert parsed.chapter == "105"
        assert parsed.chapter_title == "Taxation"
        assert "following definitions" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections_numbered(self):
        """Parse numbered subsections from section HTML."""
        converter = NCConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "105-130.2", "https://example.com"
        )

        # Should have subsections (1) through (5)
        assert len(parsed.subsections) >= 4
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)

    def test_parse_section_html_letter(self):
        """Parse section HTML with letter subsections."""
        converter = NCConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML_LETTER, "108A-25", "https://example.com"
        )

        assert parsed.section_number == "108A-25"
        assert "programs" in parsed.section_title.lower() or "creation" in parsed.section_title.lower()
        assert parsed.chapter == "108A"
        assert parsed.chapter_title == "Social Services"

    def test_parse_subsections_letter_with_numbers(self):
        """Parse letter subsections with numbered children."""
        converter = NCConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML_LETTER, "108A-25", "https://example.com"
        )

        # Should have subsections (a), (b), (c)
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)

        # Subsection (a) should have numbered children
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        # May have children (1), (2), (3)
        if sub_a.children:
            assert any(c.identifier == "2" for c in sub_a.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = NCConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "105-130.2", "https://example.com"
        )

        assert parsed.history is not None
        assert "1939" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedNCSection to Section model."""
        converter = NCConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "105-130.2", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "NC-105-130.2"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "North Carolina" in section.title_name
        assert section.uslm_id == "nc/105/105-130.2"
        assert section.source_url == "https://example.com"


class TestNCConverterFetching:
    """Test NCConverter HTTP fetching with mocks."""

    @patch.object(NCConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = NCConverter()
        section = converter.fetch_section("105-130.2")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "NC-105-130.2"
        assert "Definitions" in section.section_title

    @patch.object(NCConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section cannot be found</body></html>"

        converter = NCConverter()
        with pytest.raises(NCConverterError) as exc_info:
            converter.fetch_section("999-99")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(NCConverter, "_get")
    def test_fetch_section_empty(self, mock_get):
        """Handle empty page error."""
        mock_get.return_value = "<html><body></body></html>"

        converter = NCConverter()
        with pytest.raises(NCConverterError) as exc_info:
            converter.fetch_section("105-999")

        assert "empty" in str(exc_info.value).lower() or "invalid" in str(exc_info.value).lower()

    @patch.object(NCConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = NCConverter()
        sections = converter.get_chapter_section_numbers(105)

        assert len(sections) == 3  # 105-1, 105-1.1, 105-33 (not repealed ones)
        assert "105-1" in sections
        assert "105-1.1" in sections
        assert "105-33" in sections

    @patch.object(NCConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns chapter index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = NCConverter()
        sections = list(converter.iter_chapter(105))

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(NCConverter, "_get")
    def test_fetch_nc_section(self, mock_get):
        """Test fetch_nc_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_nc_section("105-130.2")

        assert section is not None
        assert section.citation.section == "NC-105-130.2"

    @patch.object(NCConverter, "_get")
    def test_download_nc_chapter(self, mock_get):
        """Test download_nc_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_nc_chapter(105)

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestNCConverterIntegration:
    """Integration tests that hit real ncleg.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_section(self):
        """Fetch NC tax section 105-130.2."""
        converter = NCConverter()
        section = converter.fetch_section("105-130.2")

        assert section is not None
        assert section.citation.section == "NC-105-130.2"
        assert "definition" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_social_services_section(self):
        """Fetch NC social services section 108A-25."""
        converter = NCConverter()
        section = converter.fetch_section("108A-25")

        assert section is not None
        assert section.citation.section == "NC-108A-25"
        assert "program" in section.section_title.lower() or "creation" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_105_sections(self):
        """Get list of sections in Chapter 105."""
        converter = NCConverter()
        sections = converter.get_chapter_section_numbers(105)

        assert len(sections) > 0
        assert all(s.startswith("105-") for s in sections)
        assert "105-1" in sections  # Title and purpose
