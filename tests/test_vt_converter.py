"""Tests for Vermont state statute converter.

Tests the VTConverter which fetches from legislature.vermont.gov
and converts to the internal Section model.
"""

from unittest.mock import patch

import pytest

from axiom.converters.us_states.vt import (
    VT_HUMAN_SERVICES_CHAPTERS,
    VT_TAX_CHAPTERS,
    VT_TITLES,
    VTConverter,
    VTConverterError,
    download_vt_chapter,
    fetch_vt_section,
)
from axiom.models import Section

# Sample HTML from legislature.vermont.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Vermont Laws</title>
</head>
<body class="StatutesPage StatutesPage-handleSection">
    <div class="main" id="main-content" tabindex="-1">
        <h2>The Vermont Statutes Online</h2>
        <h2 class="statute-title">
            <a href="/statutes/title/32">
                Title
                <span class="dirty">32</span>
                : <span class="caps">Taxation and Finance</span>
            </a>
        </h2>
        <h3 class="statute-chapter">
            <a href="/statutes/chapter/32/151">
                Chapter
                <span class="dirty">151</span>
                : <span class="caps">Income Taxes</span>
            </a>
        </h3>
        <h4 class="statute-section">
            Subchapter
            <span class="dirty">001</span>: <span class="caps">DEFINITIONS; GENERAL PROVISIONS</span>
        </h4>
        <b>(Cite as: 32 V.S.A. § 5811)</b>
        <ul class="item-list statutes-detail">
            <li>
                <p><b>§ 5811. Definitions</b></p>
                <p style="text-indent:19.2px">As used in this chapter unless the context requires otherwise:</p>
                <p style="text-indent:38.4px">(1) [Repealed.]</p>
                <p style="text-indent:38.4px">(2) "Commissioner" means the Commissioner of Taxes appointed under section 3101 of this title.</p>
                <p style="text-indent:38.4px">(3) "Corporation" means any business entity subject to income taxation as a corporation.</p>
                <p style="text-indent:57.6px">(A) railroad and insurance companies that are taxed under chapter 211 of this title;</p>
                <p style="text-indent:57.6px">(B) credit unions organized under 8 V.S.A. chapter 221 and federal credit unions;</p>
                <p style="text-indent:38.4px">(4) [Repealed.]</p>
                <p style="text-indent:38.4px">(5) "Fiscal year" means an accounting period of 12 months ending on the last day of any month except December.</p>
                <p>(Added 1966, No. 61 (Sp. Sess.), § 1, eff. Jan. 1, 1966; amended 2023, No. 85 (Adj. Sess.), §§ 467, 468, eff. July 1, 2024.)</p>
            </li>
        </ul>
    </div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Income Taxes</title>
</head>
<body class="StatutesPage StatutesPage-handleChapter">
    <div class="main" id="main-content" tabindex="-1">
        <h2>The Vermont Statutes Online</h2>
        <h2 class="statute-title"><a href="/statutes/title/32">Title <span class="dirty">32</span>: Taxation and Finance</a></h2>
        <h3 class="statute-chapter">Chapter  <span class="dirty">151</span>: <span class="caps">Income Taxes</span></h3>
        <ul class="item-list statutes-list">
            <li>
                <strong>Subchapter <span class="dirty">001</span>: <span class="caps">DEFINITIONS; GENERAL PROVISIONS</span></strong>
            </li>
            <li>
                <a href="/statutes/section/32/151/05811"> § 5811.  Definitions</a>
                <p><i></i></p>
            </li>
            <li>
                <a href="/statutes/section/32/151/05812"> § 5812.  Income taxation of parties to a civil union</a>
                <p><i></i></p>
            </li>
            <li>
                <a href="/statutes/section/32/151/05822"> § 5822.  Tax on income of individuals, estates, and trusts</a>
                <p><i></i></p>
            </li>
            <li>
                <a href="/statutes/section/32/151/05828b"> § 5828b.  Earned income tax credit</a>
                <p><i></i></p>
            </li>
        </ul>
    </div>
</body>
</html>
"""

SAMPLE_TITLE_INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Taxation and Finance</title>
</head>
<body class="StatutesPage StatutesPage-handleTitle">
    <div class="main" id="main-content" tabindex="-1">
        <h2>The Vermont Statutes Online</h2>
        <h2 class="statute-title"><a href="/statutes/title/32">Title <span class="dirty">32</span>: <span class="caps">Taxation and Finance</span></a></h2>
        <ul class="item-list statutes-list">
            <li>
                <a href="/statutes/chapter/32/001">Chapter  <span class="dirty">001</span>: <span class="caps">General Provisions</span></a>
                <ul class="item-list"><li>Contains: §§ 1 - 6</li></ul>
            </li>
            <li>
                <a href="/statutes/chapter/32/151">Chapter  <span class="dirty">151</span>: <span class="caps">Income Taxes</span></a>
                <ul class="item-list"><li>Contains: §§ 5811 - 5930</li></ul>
            </li>
            <li>
                <a href="/statutes/chapter/32/205">Chapter  <span class="dirty">205</span>: <span class="caps">Sales and Use Tax</span></a>
                <ul class="item-list"><li>Contains: §§ 9701 - 9784</li></ul>
            </li>
        </ul>
    </div>
</body>
</html>
"""


class TestVTTitlesRegistry:
    """Test Vermont titles registry."""

    def test_title_32_is_taxation(self):
        """Title 32 is Taxation and Finance."""
        assert 32 in VT_TITLES
        assert "Taxation" in VT_TITLES[32]

    def test_title_33_is_human_services(self):
        """Title 33 is Human Services."""
        assert 33 in VT_TITLES
        assert "Human Services" in VT_TITLES[33]

    def test_all_titles_present(self):
        """All 33 titles should be present."""
        assert len(VT_TITLES) == 33


class TestVTChaptersRegistry:
    """Test Vermont chapter registries."""

    def test_chapter_151_in_tax_chapters(self):
        """Chapter 151 (Income Taxes) is in tax chapters."""
        assert 151 in VT_TAX_CHAPTERS
        assert "Income" in VT_TAX_CHAPTERS[151]

    def test_chapter_205_in_tax_chapters(self):
        """Chapter 205 (Sales and Use Tax) is in tax chapters."""
        assert 205 in VT_TAX_CHAPTERS
        assert "Sales" in VT_TAX_CHAPTERS[205]

    def test_chapter_19_in_human_services_chapters(self):
        """Chapter 19 (Medical Assistance) is in human services chapters."""
        assert 19 in VT_HUMAN_SERVICES_CHAPTERS
        assert "Medical" in VT_HUMAN_SERVICES_CHAPTERS[19]

    def test_chapter_11_in_human_services_chapters(self):
        """Chapter 11 (Public Assistance Programs) is in human services chapters."""
        assert 11 in VT_HUMAN_SERVICES_CHAPTERS
        assert "Assistance" in VT_HUMAN_SERVICES_CHAPTERS[11]


class TestVTConverter:
    """Test VTConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = VTConverter()
        assert converter.rate_limit_delay == 0.5

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = VTConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = VTConverter()
        url = converter._build_section_url(32, 151, 5811)
        assert "legislature.vermont.gov/statutes" in url
        assert "/section/32/151/05811" in url

    def test_build_section_url_with_string(self):
        """Build URL with section number as string."""
        converter = VTConverter()
        url = converter._build_section_url(32, 151, "5811")
        assert "/section/32/151/05811" in url

    def test_build_section_url_with_letter_suffix(self):
        """Build URL for sections with letter suffixes (e.g., 5828b)."""
        converter = VTConverter()
        url = converter._build_section_url(32, 151, "5828b")
        assert "/section/32/151/05828b" in url

    def test_build_section_url_with_letter_suffix_uppercase(self):
        """Build URL for sections with uppercase letter suffixes."""
        converter = VTConverter()
        url = converter._build_section_url(32, 151, "5825A")
        assert "/section/32/151/05825a" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = VTConverter()
        url = converter._build_chapter_url(32, 151)
        assert "/chapter/32/151" in url

    def test_build_title_url(self):
        """Build correct URL for title chapters."""
        converter = VTConverter()
        url = converter._build_title_url(32)
        assert "/title/32" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with VTConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestVTConverterParsing:
    """Test VTConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedVTSection."""
        converter = VTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 32, 151, 5811, "https://example.com"
        )

        assert parsed.title_number == 32
        assert parsed.title_name == "Taxation and Finance"
        assert parsed.chapter_number == 151
        assert parsed.chapter_title == "Income Taxes"
        assert parsed.subchapter_number == "001"
        assert "DEFINITIONS" in parsed.subchapter_title.upper()
        assert parsed.section_number == "5811"
        assert parsed.section_title == "Definitions"
        assert "Commissioner" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = VTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 32, 151, 5811, "https://example.com"
        )

        # Should have multiple subsections
        assert len(parsed.subsections) >= 3
        # Check for specific subsection identifiers
        identifiers = [s.identifier for s in parsed.subsections]
        assert "2" in identifiers
        assert "3" in identifiers
        assert "5" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (A), (B) under (3)."""
        converter = VTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 32, 151, 5811, "https://example.com"
        )

        # Find subsection (3) which has children
        sub_3 = next((s for s in parsed.subsections if s.identifier == "3"), None)
        assert sub_3 is not None
        # Should have children (A) and (B)
        assert len(sub_3.children) >= 2
        child_ids = [c.identifier for c in sub_3.children]
        assert "A" in child_ids
        assert "B" in child_ids

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = VTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 32, 151, 5811, "https://example.com"
        )

        assert parsed.history is not None
        assert "1966" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedVTSection to Section model."""
        converter = VTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 32, 151, 5811, "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "VT-32-5811"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "Vermont Statutes Annotated" in section.title_name
        assert section.uslm_id == "vt/32/151/5811"
        assert section.source_url == "https://example.com"


class TestVTConverterFetching:
    """Test VTConverter HTTP fetching with mocks."""

    @patch.object(VTConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = VTConverter()
        section = converter.fetch_section(32, 151, 5811)

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "VT-32-5811"
        assert "Definitions" in section.section_title

    @patch.object(VTConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>404 Not Found</body></html>"

        converter = VTConverter()
        with pytest.raises(VTConverterError) as exc_info:
            converter.fetch_section(32, 999, 99999)

        assert "not found" in str(exc_info.value).lower()

    @patch.object(VTConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = VTConverter()
        sections = converter.get_chapter_section_numbers(32, 151)

        assert len(sections) == 4
        assert "5811" in sections
        assert "5812" in sections
        assert "5822" in sections
        assert "5828b" in sections

    @patch.object(VTConverter, "_get")
    def test_get_title_chapters(self, mock_get):
        """Get list of chapters in a title."""
        mock_get.return_value = SAMPLE_TITLE_INDEX_HTML

        converter = VTConverter()
        chapters = converter.get_title_chapters(32)

        assert len(chapters) == 3
        assert 1 in chapters
        assert 151 in chapters
        assert 205 in chapters

    @patch.object(VTConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns chapter index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = VTConverter()
        sections = list(converter.iter_chapter(32, 151))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(VTConverter, "_get")
    def test_fetch_vt_section(self, mock_get):
        """Test fetch_vt_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_vt_section(32, 151, 5811)

        assert section is not None
        assert section.citation.section == "VT-32-5811"

    @patch.object(VTConverter, "_get")
    def test_download_vt_chapter(self, mock_get):
        """Test download_vt_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_vt_chapter(32, 151)

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestVTConverterIntegration:
    """Integration tests that hit real legislature.vermont.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_definitions(self):
        """Fetch Vermont Income Tax section 5811 (Definitions)."""
        converter = VTConverter()
        section = converter.fetch_section(32, 151, 5811)

        assert section is not None
        assert section.citation.section == "VT-32-5811"
        assert "definitions" in section.section_title.lower()
        assert "commissioner" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_eitc_section(self):
        """Fetch Vermont Earned Income Tax Credit section 5828b."""
        converter = VTConverter()
        section = converter.fetch_section(32, 151, "5828b")

        assert section is not None
        assert "earned income" in section.section_title.lower() or "earned income" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_rates(self):
        """Fetch Vermont Income Tax section 5822 (Tax rates)."""
        converter = VTConverter()
        section = converter.fetch_section(32, 151, 5822)

        assert section is not None
        assert "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_151_sections(self):
        """Get list of sections in Chapter 151 (Income Taxes)."""
        converter = VTConverter()
        sections = converter.get_chapter_section_numbers(32, 151)

        assert len(sections) > 0
        assert "5811" in sections  # Definitions

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_32_chapters(self):
        """Get list of chapters in Title 32 (Taxation and Finance)."""
        converter = VTConverter()
        chapters = converter.get_title_chapters(32)

        assert len(chapters) > 0
        assert 151 in chapters  # Income Taxes

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_human_services_section(self):
        """Fetch a Human Services section from Title 33."""
        converter = VTConverter()
        try:
            # Try to fetch a section from chapter 11 (Public Assistance Programs)
            sections = converter.get_chapter_section_numbers(33, 11)
            if sections:
                section = converter.fetch_section(33, 11, sections[0])
                assert section.citation.section.startswith("VT-33-")
        except VTConverterError:
            # Chapter may not have sections, which is acceptable
            pytest.skip("No sections found in Title 33 Chapter 11")
