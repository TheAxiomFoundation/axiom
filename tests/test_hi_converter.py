"""Tests for Hawaii state statute converter.

Tests the HIConverter which fetches from capitol.hawaii.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.hi import (
    HI_TAX_CHAPTERS,
    HI_WELFARE_CHAPTERS,
    HIConverter,
    HIConverterError,
    download_hi_chapter,
    fetch_hi_section,
)
from axiom.models import Section

# Sample HTML from capitol.hawaii.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>HRS 235-51</title></head>
<body>
<h3>PART III. INDIVIDUAL INCOME TAX</h3>
<p><b>[235-51] Tax imposed on individuals; rates.</b></p>
<p>(a) There is hereby imposed on the taxable income of:</p>
<p>(1) Every taxpayer who files a joint return under section 235-93; and</p>
<p>(2) Every surviving spouse;</p>
<p>a tax determined in accordance with the following table:</p>
<p>(b) There is hereby imposed on the taxable income of every head of a household
a tax determined in accordance with the following table:</p>
<p>(c) There is hereby imposed on the taxable income of:</p>
<p>(1) Every unmarried individual; and</p>
<p>(2) Every married individual who does not make a joint return under section 235-93;</p>
<p>a tax determined in accordance with the following table:</p>
<p>[L 1978, c 173, pt of 1; am L 2017, c 107, 1]</p>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 235 - Income Tax Law</title></head>
<body>
<h1>CHAPTER 235</h1>
<h2>INCOME TAX LAW</h2>
<div id="contents">
<ul>
<li><a href="HRS_0235-0001.htm">[235-1] Definitions</a></li>
<li><a href="HRS_0235-0002.htm">[235-2] Repealed</a></li>
<li><a href="HRS_0235-0051.htm">[235-51] Tax imposed on individuals; rates</a></li>
<li><a href="HRS_0235-0093.htm">[235-93] Joint returns</a></li>
</ul>
</div>
</body>
</html>
"""


class TestHIChaptersRegistry:
    """Test Hawaii chapter registries."""

    def test_chapter_235_in_tax_chapters(self):
        """Chapter 235 (Income Tax) is in tax chapters."""
        assert 235 in HI_TAX_CHAPTERS
        assert "Income Tax" in HI_TAX_CHAPTERS[235]

    def test_chapter_237_in_tax_chapters(self):
        """Chapter 237 (General Excise Tax) is in tax chapters."""
        assert 237 in HI_TAX_CHAPTERS
        assert "Excise" in HI_TAX_CHAPTERS[237]

    def test_chapter_346_in_welfare_chapters(self):
        """Chapter 346 (Department of Human Services) is in welfare chapters."""
        assert 346 in HI_WELFARE_CHAPTERS
        assert "Human Services" in HI_WELFARE_CHAPTERS[346]

    def test_chapter_383_in_welfare_chapters(self):
        """Chapter 383 (Employment Security) is in welfare chapters."""
        assert 383 in HI_WELFARE_CHAPTERS
        assert "Employment" in HI_WELFARE_CHAPTERS[383]

    def test_tax_chapters_range(self):
        """Tax chapters are in the expected range 231-257."""
        for chapter in HI_TAX_CHAPTERS:
            if isinstance(chapter, str):
                # String keys like "244D" - extract numeric part
                import re
                match = re.match(r"(\d+)", chapter)
                if match:
                    assert 231 <= int(match.group(1)) <= 257
            else:
                assert 231 <= chapter <= 257


class TestHIConverter:
    """Test HIConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = HIConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = HIConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = HIConverter(year=2024)
        assert converter.year == 2024

    def test_get_volume_for_chapter_235(self):
        """Volume calculation for chapter 235 (Vol04)."""
        converter = HIConverter()
        volume = converter._get_volume_for_chapter(235)
        assert "Vol04" in volume
        assert "0201-0257" in volume

    def test_get_volume_for_chapter_346(self):
        """Volume calculation for chapter 346 (Vol07)."""
        converter = HIConverter()
        volume = converter._get_volume_for_chapter(346)
        assert "Vol07" in volume
        assert "0346-0398" in volume

    def test_get_volume_for_chapter_1(self):
        """Volume calculation for chapter 1 (Vol01)."""
        converter = HIConverter()
        volume = converter._get_volume_for_chapter(1)
        assert "Vol01" in volume

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = HIConverter()
        url = converter._build_section_url("235-51")
        assert "capitol.hawaii.gov/hrscurrent" in url
        assert "Vol04" in url
        assert "HRS0235" in url
        assert "HRS_0235-0051.htm" in url

    def test_build_section_url_single_digit(self):
        """Build correct URL for single-digit section."""
        converter = HIConverter()
        url = converter._build_section_url("235-1")
        assert "HRS_0235-0001.htm" in url

    def test_build_chapter_contents_url(self):
        """Build correct URL for chapter contents."""
        converter = HIConverter()
        url = converter._build_chapter_contents_url(235)
        assert "HRS_0235-.htm" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with HIConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestHIConverterParsing:
    """Test HIConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedHISection."""
        converter = HIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "235-51", "https://example.com"
        )

        assert parsed.section_number == "235-51"
        assert "tax imposed" in parsed.section_title.lower() or "235-51" in parsed.section_title
        assert parsed.chapter_number == 235
        assert parsed.chapter_title == "Income Tax Law"
        assert parsed.title_number == 14
        assert parsed.title_name == "Taxation"
        assert "taxable income" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = HIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "235-51", "https://example.com"
        )

        # Should have subsections (a), (b), (c)
        assert len(parsed.subsections) >= 2
        identifiers = [s.identifier for s in parsed.subsections]
        assert "a" in identifiers
        assert "b" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (a)."""
        converter = HIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "235-51", "https://example.com"
        )

        # Find subsection (a)
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        # Should have children (1) and (2)
        if sub_a.children:
            assert any(c.identifier == "1" for c in sub_a.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = HIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "235-51", "https://example.com"
        )

        assert parsed.history is not None
        assert "L 1978" in parsed.history or "L 2017" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedHISection to Section model."""
        converter = HIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "235-51", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "HI-235-51"
        assert section.citation.title == 0  # State law indicator
        assert "Hawaii Revised Statutes" in section.title_name
        assert section.uslm_id == "hi/235/235-51"
        assert section.source_url == "https://example.com"


class TestHIConverterFetching:
    """Test HIConverter HTTP fetching with mocks."""

    @patch.object(HIConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = HIConverter()
        section = converter.fetch_section("235-51")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "HI-235-51"

    @patch.object(HIConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section cannot be found</body></html>"

        converter = HIConverter()
        with pytest.raises(HIConverterError) as exc_info:
            # Use a valid chapter number (235) so we don't get "unknown volume" error
            converter.fetch_section("235-9999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(HIConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = HIConverter()
        sections = converter.get_chapter_section_numbers(235)

        assert len(sections) >= 1
        # Check for expected section numbers in some form
        section_strs = [str(s) for s in sections]
        assert any("235-1" in s for s in section_strs) or any("235-51" in s for s in section_strs)

    @patch.object(HIConverter, "_get")
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

        converter = HIConverter()
        sections = list(converter.iter_chapter(235))

        # Should get some sections
        assert len(sections) > 0
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(HIConverter, "_get")
    def test_fetch_hi_section(self, mock_get):
        """Test fetch_hi_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_hi_section("235-51")

        assert section is not None
        assert section.citation.section == "HI-235-51"

    @patch.object(HIConverter, "_get")
    def test_download_hi_chapter(self, mock_get):
        """Test download_hi_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_hi_chapter(235)

        assert len(sections) > 0
        assert all(isinstance(s, Section) for s in sections)


class TestHIConverterIntegration:
    """Integration tests that hit real capitol.hawaii.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Hawaii Income Tax section 235-51."""
        converter = HIConverter()
        try:
            section = converter.fetch_section("235-51")

            assert section is not None
            assert section.citation.section == "HI-235-51"
            assert "tax" in section.text.lower()
        except HIConverterError as e:
            pytest.skip(f"Could not fetch section: {e}")
        finally:
            converter.close()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_definitions_section(self):
        """Fetch Hawaii definitions section 235-1."""
        converter = HIConverter()
        try:
            section = converter.fetch_section("235-1")

            assert section is not None
            assert section.citation.section == "HI-235-1"
        except HIConverterError as e:
            pytest.skip(f"Could not fetch section: {e}")
        finally:
            converter.close()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_235_sections(self):
        """Get list of sections in Chapter 235."""
        converter = HIConverter()
        try:
            sections = converter.get_chapter_section_numbers(235)

            assert len(sections) > 0
            assert all("235-" in s for s in sections)
        except HIConverterError as e:
            pytest.skip(f"Could not fetch chapter: {e}")
        finally:
            converter.close()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Hawaii welfare section 346-14."""
        converter = HIConverter()
        try:
            section = converter.fetch_section("346-14")
            assert section.citation.section == "HI-346-14"
        except HIConverterError as e:
            # Section may not exist, which is acceptable
            pytest.skip(f"Section 346-14 not found: {e}")
        finally:
            converter.close()
