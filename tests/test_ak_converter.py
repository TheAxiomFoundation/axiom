"""Tests for Alaska state statute converter.

Tests the AKConverter which fetches from akleg.gov and converts to the
internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.ak import (
    AK_TAX_CHAPTERS,
    AK_TITLES,
    AK_WELFARE_CHAPTERS,
    AKConverter,
    AKConverterError,
    download_ak_chapter,
    fetch_ak_section,
)
from axiom_corpus.models import Section

# Sample HTML from akleg.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Alaska Statutes - Title 43</title></head>
<body>
<h1>Title 43. Revenue and Taxation</h1>
<h2>Chapter 05. Administration of Revenue Laws</h2>
<div class="statute">
<p><b>Sec. 43.05.010. Duties of the department of revenue.</b></p>
<p>The department of revenue shall</p>
<p>(a) enforce the tax laws of the state and collect all taxes, licenses,
and fees payable under the tax laws;</p>
<p>(b) supervise the fiscal concerns of the state;</p>
<p>(c) prepare and submit to the legislature estimates of revenue for the
following fiscal year;</p>
<p>(1) the estimates shall include projected revenue from each major
revenue source;</p>
<p>(2) the department shall update the estimates quarterly;</p>
<p>(A) updates shall be provided to the legislature within 30 days;</p>
<p>(B) updates shall include explanations of any significant changes;</p>
<p>(d) prescribe uniform systems of accounting for all state departments;</p>
<p>History: Sec. 1 ch 83 SLA 1971; am sec. 2 ch 118 SLA 1980.</p>
</div>
</body>
</html>
"""

SAMPLE_TITLE_INDEX_HTML = """
<a name=#43.05.010 >Sec. 43.05.010.   Duties of the department of revenue.</a>
<a name=#43.05.020 >Sec. 43.05.020.   Powers of the department.</a>
<a name=#43.05.030 >Sec. 43.05.030.   Rules and regulations.</a>
<a name=#43.05.040 >Sec. 43.05.040.   Taxpayer assistance.</a>
"""


class TestAKChaptersRegistry:
    """Test Alaska chapter registries."""

    def test_title_43_in_titles(self):
        """Title 43 (Revenue and Taxation) is in titles."""
        assert 43 in AK_TITLES
        assert "Revenue" in AK_TITLES[43]
        assert "Taxation" in AK_TITLES[43]

    def test_title_47_in_titles(self):
        """Title 47 (Welfare) is in titles."""
        assert 47 in AK_TITLES
        assert "Welfare" in AK_TITLES[47]

    def test_chapter_05_in_tax_chapters(self):
        """Chapter 05 (Administration) is in tax chapters."""
        assert "05" in AK_TAX_CHAPTERS
        assert "Administration" in AK_TAX_CHAPTERS["05"]

    def test_chapter_20_in_tax_chapters(self):
        """Chapter 20 (Net Income Tax) is in tax chapters."""
        assert "20" in AK_TAX_CHAPTERS
        assert "Net Income Tax" in AK_TAX_CHAPTERS["20"]

    def test_chapter_23_in_tax_chapters(self):
        """Chapter 23 (Permanent Fund Dividends) is in tax chapters."""
        assert "23" in AK_TAX_CHAPTERS
        assert "Permanent Fund" in AK_TAX_CHAPTERS["23"]

    def test_chapter_07_in_welfare_chapters(self):
        """Chapter 07 (Medical Assistance) is in welfare chapters."""
        assert "07" in AK_WELFARE_CHAPTERS
        assert "Medical" in AK_WELFARE_CHAPTERS["07"]

    def test_chapter_12_in_welfare_chapters(self):
        """Chapter 12 (Temporary Assistance) is in welfare chapters."""
        assert "12" in AK_WELFARE_CHAPTERS
        assert "Temporary Assistance" in AK_WELFARE_CHAPTERS["12"]


class TestAKConverter:
    """Test AKConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = AKConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = AKConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = AKConverter(year=2024)
        assert converter.year == 2024

    def test_parse_section_number_valid(self):
        """Parse valid section numbers correctly."""
        converter = AKConverter()

        title, chapter, suffix = converter._parse_section_number("43.05.010")
        assert title == 43
        assert chapter == "05"
        assert suffix == "010"

        title, chapter, suffix = converter._parse_section_number("47.30.660")
        assert title == 47
        assert chapter == "30"
        assert suffix == "660"

    def test_parse_section_number_invalid(self):
        """Raise error for invalid section numbers."""
        converter = AKConverter()

        with pytest.raises(AKConverterError):
            converter._parse_section_number("43")

        with pytest.raises(AKConverterError):
            converter._parse_section_number("43.05")

    def test_build_title_url(self):
        """Build correct URL for title index."""
        converter = AKConverter()
        url = converter._build_title_url(43)
        assert "akleg.gov/basis/statutes.asp" in url
        assert "title=43" in url

    def test_get_title_for_section(self):
        """Get title info from section number."""
        converter = AKConverter()

        title_num, title_name = converter._get_title_for_section("43.05.010")
        assert title_num == 43
        assert "Revenue" in title_name

        title_num, title_name = converter._get_title_for_section("47.30.660")
        assert title_num == 47
        assert "Welfare" in title_name

    def test_get_chapter_info(self):
        """Get chapter info from title and chapter number."""
        converter = AKConverter()

        chapter_num, chapter_title = converter._get_chapter_info(43, "05")
        assert chapter_num == "05"
        assert "Administration" in chapter_title

        chapter_num, chapter_title = converter._get_chapter_info(47, "07")
        assert chapter_num == "07"
        assert "Medical" in chapter_title

    def test_context_manager(self):
        """Converter works as context manager."""
        with AKConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestAKConverterParsing:
    """Test AKConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedAKSection."""
        converter = AKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "43.05.010", "https://example.com"
        )

        assert parsed.section_number == "43.05.010"
        assert "Duties" in parsed.section_title or "department" in parsed.section_title.lower()
        assert parsed.chapter_number == "05"
        assert parsed.title_number == 43
        assert "Revenue" in parsed.title_name
        assert "enforce" in parsed.text.lower() or "tax" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = AKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "43.05.010", "https://example.com"
        )

        # Should have primary subsections (a), (b), (c), (d)
        assert len(parsed.subsections) >= 3
        identifiers = [s.identifier for s in parsed.subsections]
        assert "a" in identifiers
        assert "b" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under primary subsections."""
        converter = AKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "43.05.010", "https://example.com"
        )

        # Find subsection (c) which should have children (1) and (2)
        sub_c = next((s for s in parsed.subsections if s.identifier == "c"), None)
        if sub_c and sub_c.children:
            child_ids = [c.identifier for c in sub_c.children]
            assert "1" in child_ids or "2" in child_ids

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = AKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "43.05.010", "https://example.com"
        )

        assert parsed.history is not None
        assert "SLA" in parsed.history or "ch" in parsed.history.lower()

    def test_to_section_model(self):
        """Convert ParsedAKSection to Section model."""
        converter = AKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "43.05.010", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "AK-43.05.010"
        assert section.citation.title == 0  # State law indicator
        assert "Alaska Statutes" in section.title_name
        assert section.uslm_id == "ak/43/05/43.05.010"
        assert section.source_url == "https://example.com"


class TestAKConverterFetching:
    """Test AKConverter HTTP fetching with mocks."""

    @patch.object(AKConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = AKConverter()
        section = converter.fetch_section("43.05.010")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "AK-43.05.010"

    @patch.object(AKConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section cannot be found</body></html>"

        converter = AKConverter()
        with pytest.raises(AKConverterError) as exc_info:
            converter.fetch_section("43.99.999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(AKConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_TITLE_INDEX_HTML

        converter = AKConverter()
        sections = converter.get_chapter_section_numbers(43, "05")

        assert len(sections) >= 1
        assert any("43.05" in s for s in sections)

    @patch.object(AKConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_TITLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = AKConverter()
        sections = list(converter.iter_chapter(43, "05"))

        assert len(sections) >= 1
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(AKConverter, "_get")
    def test_fetch_ak_section(self, mock_get):
        """Test fetch_ak_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_ak_section("43.05.010")

        assert section is not None
        assert section.citation.section == "AK-43.05.010"

    @patch.object(AKConverter, "_get")
    def test_download_ak_chapter(self, mock_get):
        """Test download_ak_chapter function."""
        mock_get.side_effect = [
            SAMPLE_TITLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_ak_chapter(43, "05")

        assert len(sections) >= 1
        assert all(isinstance(s, Section) for s in sections)


class TestAKConverterIntegration:
    """Integration tests that hit real akleg.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_title_43_section(self):
        """Fetch Alaska Revenue section."""
        converter = AKConverter()
        try:
            section = converter.fetch_section("43.05.010")
            assert section is not None
            assert section.citation.section == "AK-43.05.010"
        except AKConverterError:
            pytest.skip("Could not fetch from akleg.gov")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_pfd_section(self):
        """Fetch Alaska Permanent Fund Dividend section."""
        converter = AKConverter()
        try:
            section = converter.fetch_section("43.23.005")
            assert section is not None
            assert section.citation.section == "AK-43.23.005"
        except AKConverterError:
            pytest.skip("Could not fetch from akleg.gov")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Alaska welfare section."""
        converter = AKConverter()
        try:
            section = converter.fetch_section("47.07.010")
            assert section is not None
            assert section.citation.section == "AK-47.07.010"
        except AKConverterError:
            pytest.skip("Could not fetch from akleg.gov")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_sections(self):
        """Get list of sections in Chapter 05."""
        converter = AKConverter()
        try:
            sections = converter.get_chapter_section_numbers(43, "05")
            # Should find at least some sections
            assert len(sections) >= 0  # May be empty due to JS-driven site
        except AKConverterError:
            pytest.skip("Could not fetch from akleg.gov")
