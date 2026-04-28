"""Tests for Washington state statute converter.

Tests the WAConverter which fetches from app.leg.wa.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.wa import (
    WA_EXCISE_TAX_CHAPTERS,
    WA_PUBLIC_ASSISTANCE_CHAPTERS,
    WA_TITLES,
    WAConverter,
    WAConverterError,
    download_wa_chapter,
    fetch_wa_section,
)
from axiom.models import Section

# Sample HTML from app.leg.wa.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>RCW 82.04.290</title></head>
<body>
<nav>Navigation here</nav>
<main>
<h3>RCW 82.04.290: Tax on service and other activities</h3>
<p>(Effective until October 1, 2025.)</p>
<div class="content">
<p>(1) Upon every person engaging within this state in the business of providing international investment management services, as to such persons, the amount of tax with respect to such business is equal to the gross income or gross proceeds of sales of the business multiplied by a rate of 0.275 percent.</p>
<p>(2) Upon every person engaging within this state in any business activity other than or in addition to those enumerated in RCW 82.04.230, 82.04.240, 82.04.250, 82.04.255, 82.04.260, and 82.04.270:</p>
<p>(a) The amount of tax on such business is equal to the gross income of the business multiplied by the rate of 1.5 percent; or</p>
<p>(b) For persons engaged in performing aerospace product development for others:</p>
<p>(i) The amount of tax is equal to the gross income of the business multiplied by the rate of 0.9 percent; and</p>
<p>(ii) This subsection (2)(b) expires July 1, 2040.</p>
<p>(3) This section does not apply to any persons subject to the tax under RCW 82.04.272.</p>
<p>NOTES:</p>
<p>Effective date--2023 c 195 ss 1-5 and 7-10: See note following RCW 82.04.261.</p>
<p>[2023 c 195 s 2; 2020 c 139 s 31; 2019 c 423 s 201; 2017 3rd sp.s. c 37 s 101.]</p>
</div>
</main>
<footer>Footer here</footer>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>RCW 74.04.005</title></head>
<body>
<main>
<h3>RCW 74.04.005: Definitions -- Eligibility</h3>
<div class="content">
<p>For the purposes of this title, unless the context indicates otherwise, the following definitions shall apply:</p>
<p>(1) "Applicant" means any person who has made a request, or on behalf of whom a request has been made, to any county or local office for assistance.</p>
<p>(2) "Department" means the department of social and health services.</p>
<p>(3) "Director" means the director of the department or their designee.</p>
<p>(a) The director has authority to delegate powers and duties to assistant directors.</p>
<p>(b) Such delegation must be in writing.</p>
<p>(4) "Federal aid assistance" means the specific categories of assistance for which the state receives federal aid.</p>
<p>(5) "Income" means:</p>
<p>(a) All wages, salaries, and commissions;</p>
<p>(b) Interest, dividends, and other investment income;</p>
<p>(c) Retirement benefits; and</p>
<p>(d) Any other money received.</p>
<p>[2021 c 245 s 1; 2019 c 324 s 2; 2017 c 283 s 1.]</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 82.04 RCW</title></head>
<body>
<main>
<h1>Chapter 82.04 RCW - Business and Occupation Tax</h1>
<div id="contents">
<ul>
<li><a href="default.aspx?cite=82.04.010">82.04.010 Introductory</a></li>
<li><a href="default.aspx?cite=82.04.020">82.04.020 "Person" defined</a></li>
<li><a href="default.aspx?cite=82.04.030">82.04.030 "Sale" defined</a></li>
<li><a href="default.aspx?cite=82.04.290">82.04.290 Tax on service and other activities</a></li>
</ul>
</div>
</main>
</body>
</html>
"""

SAMPLE_TITLE_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Title 82 RCW</title></head>
<body>
<main>
<h1>Title 82 RCW - Excise Taxes</h1>
<div id="contents">
<ul>
<li><a href="default.aspx?cite=82.04">Chapter 82.04 - Business and Occupation Tax</a></li>
<li><a href="default.aspx?cite=82.08">Chapter 82.08 - Retail Sales Tax</a></li>
<li><a href="default.aspx?cite=82.12">Chapter 82.12 - Use Tax</a></li>
<li><a href="default.aspx?cite=82.14">Chapter 82.14 - Local Retail Sales and Use Taxes</a></li>
</ul>
</div>
</main>
</body>
</html>
"""


class TestWAChaptersRegistry:
    """Test Washington chapter registries."""

    def test_chapter_82_04_in_excise_chapters(self):
        """Chapter 82.04 (B&O Tax) is in excise tax chapters."""
        assert "82.04" in WA_EXCISE_TAX_CHAPTERS
        assert "Business" in WA_EXCISE_TAX_CHAPTERS["82.04"]

    def test_chapter_82_08_in_excise_chapters(self):
        """Chapter 82.08 (Retail Sales Tax) is in excise tax chapters."""
        assert "82.08" in WA_EXCISE_TAX_CHAPTERS
        assert "Retail Sales" in WA_EXCISE_TAX_CHAPTERS["82.08"]

    def test_chapter_82_87_in_excise_chapters(self):
        """Chapter 82.87 (Capital Gains Tax) is in excise tax chapters."""
        assert "82.87" in WA_EXCISE_TAX_CHAPTERS
        assert "Capital Gains" in WA_EXCISE_TAX_CHAPTERS["82.87"]

    def test_chapter_74_04_in_assistance_chapters(self):
        """Chapter 74.04 (General Provisions) is in public assistance chapters."""
        assert "74.04" in WA_PUBLIC_ASSISTANCE_CHAPTERS
        assert "General" in WA_PUBLIC_ASSISTANCE_CHAPTERS["74.04"]

    def test_chapter_74_62_in_assistance_chapters(self):
        """Chapter 74.62 (Working Families Tax Credit) is in public assistance chapters."""
        assert "74.62" in WA_PUBLIC_ASSISTANCE_CHAPTERS
        assert "Working Families" in WA_PUBLIC_ASSISTANCE_CHAPTERS["74.62"]

    def test_title_82_in_titles(self):
        """Title 82 (Excise Taxes) is in titles registry."""
        assert "82" in WA_TITLES
        assert "Excise" in WA_TITLES["82"]

    def test_title_74_in_titles(self):
        """Title 74 (Public Assistance) is in titles registry."""
        assert "74" in WA_TITLES
        assert "Public Assistance" in WA_TITLES["74"]


class TestWAConverter:
    """Test WAConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = WAConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = WAConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = WAConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = WAConverter()
        url = converter._build_section_url("82.04.290")
        assert "app.leg.wa.gov/rcw" in url
        assert "cite=82.04.290" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = WAConverter()
        url = converter._build_chapter_url("82.04")
        assert "cite=82.04" in url

    def test_build_title_url(self):
        """Build correct URL for title contents."""
        converter = WAConverter()
        url = converter._build_title_url(82)
        assert "cite=82" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with WAConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestWAConverterParsing:
    """Test WAConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedWASection."""
        converter = WAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "82.04.290", "https://example.com"
        )

        assert parsed.section_number == "82.04.290"
        assert parsed.section_title == "Tax on service and other activities"
        assert parsed.chapter_number == "82.04"
        assert parsed.chapter_title == "Business and Occupation Tax"
        assert parsed.title_number == 82
        assert parsed.title_name == "Excise Taxes"
        assert "international investment management" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = WAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "82.04.290", "https://example.com"
        )

        # Should have subsections (1), (2), (3)
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)
        assert any(s.identifier == "3" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under (2)."""
        converter = WAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "82.04.290", "https://example.com"
        )

        # Find subsection (2)
        sub_2 = next((s for s in parsed.subsections if s.identifier == "2"), None)
        assert sub_2 is not None
        # Should have children (a) and (b)
        assert len(sub_2.children) >= 2
        assert any(c.identifier == "a" for c in sub_2.children)
        assert any(c.identifier == "b" for c in sub_2.children)

    def test_parse_level3_subsections(self):
        """Parse level 3 subsections (ii), (iii) when clearly Roman numerals."""
        # Level 3 parsing works when Roman numerals are unambiguous (ii, iii, iv, etc.)
        # Note: (i) is ambiguous with level 2 letter 'i', so we test with (ii), (iii)
        html_with_level3 = """<!DOCTYPE html>
        <html><body><main>
        <h3>RCW 82.04.290: Tax on service and other activities</h3>
        <div class="content">
        <p>(1) First subsection.</p>
        <p>(2) Second subsection intro:</p>
        <p>(a) First part with nested roman: (ii) Second roman. (iii) Third roman.</p>
        </div>
        </main></body></html>
        """
        converter = WAConverter()
        parsed = converter._parse_section_html(
            html_with_level3, "82.04.290", "https://example.com"
        )

        # Find subsection (2)(a)
        sub_2 = next((s for s in parsed.subsections if s.identifier == "2"), None)
        assert sub_2 is not None
        sub_2a = next((c for c in sub_2.children if c.identifier == "a"), None)
        assert sub_2a is not None
        # Should have children (ii) and (iii) - unambiguous Roman numerals
        assert len(sub_2a.children) >= 2
        assert any(c.identifier == "ii" for c in sub_2a.children)
        assert any(c.identifier == "iii" for c in sub_2a.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = WAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "82.04.290", "https://example.com"
        )

        assert parsed.history is not None
        assert "2023 c 195" in parsed.history

    def test_parse_welfare_section(self):
        """Parse public assistance section."""
        converter = WAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "74.04.005", "https://example.com"
        )

        assert parsed.section_number == "74.04.005"
        assert "Definitions" in parsed.section_title or "Eligibility" in parsed.section_title
        assert parsed.chapter_number == "74.04"
        assert parsed.title_number == 74
        assert parsed.title_name == "Public Assistance"
        assert "Applicant" in parsed.text

    def test_parse_welfare_subsections(self):
        """Parse subsections from welfare section."""
        converter = WAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "74.04.005", "https://example.com"
        )

        # Should have subsections (1), (2), (3), (4), (5)
        assert len(parsed.subsections) >= 5

        # Subsection 3 should have children (a) and (b)
        sub_3 = next((s for s in parsed.subsections if s.identifier == "3"), None)
        assert sub_3 is not None
        assert len(sub_3.children) >= 2

        # Subsection 5 should have children (a), (b), (c), (d)
        sub_5 = next((s for s in parsed.subsections if s.identifier == "5"), None)
        assert sub_5 is not None
        assert len(sub_5.children) >= 4

    def test_to_section_model(self):
        """Convert ParsedWASection to Section model."""
        converter = WAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "82.04.290", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "WA-82.04.290"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Tax on service and other activities"
        assert "Washington RCW" in section.title_name
        assert section.uslm_id == "wa/82/82.04.290"
        assert section.source_url == "https://example.com"


class TestWAConverterFetching:
    """Test WAConverter HTTP fetching with mocks."""

    @patch.object(WAConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = WAConverter()
        section = converter.fetch_section("82.04.290")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "WA-82.04.290"
        assert "service" in section.section_title.lower()

    @patch.object(WAConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Page not found</body></html>"

        converter = WAConverter()
        with pytest.raises(WAConverterError) as exc_info:
            converter.fetch_section("99.99.999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(WAConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = WAConverter()
        sections = converter.get_chapter_section_numbers("82.04")

        assert len(sections) == 4
        assert "82.04.010" in sections
        assert "82.04.020" in sections
        assert "82.04.290" in sections

    @patch.object(WAConverter, "_get")
    def test_get_title_chapters(self, mock_get):
        """Get list of chapters from title index."""
        mock_get.return_value = SAMPLE_TITLE_INDEX_HTML

        converter = WAConverter()
        chapters = converter.get_title_chapters(82)

        assert len(chapters) == 4
        assert "82.04" in chapters
        assert "82.08" in chapters
        assert "82.12" in chapters

    @patch.object(WAConverter, "_get")
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

        converter = WAConverter()
        sections = list(converter.iter_chapter("82.04"))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(WAConverter, "_get")
    def test_fetch_wa_section(self, mock_get):
        """Test fetch_wa_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_wa_section("82.04.290")

        assert section is not None
        assert section.citation.section == "WA-82.04.290"

    @patch.object(WAConverter, "_get")
    def test_download_wa_chapter(self, mock_get):
        """Test download_wa_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_wa_chapter("82.04")

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestWAConverterIntegration:
    """Integration tests that hit real app.leg.wa.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_bo_tax_section(self):
        """Fetch Washington B&O Tax section 82.04.290."""
        converter = WAConverter()
        section = converter.fetch_section("82.04.290")

        assert section is not None
        assert section.citation.section == "WA-82.04.290"
        assert "tax" in section.text.lower() or "business" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch Washington Sales Tax section 82.08.020."""
        converter = WAConverter()
        section = converter.fetch_section("82.08.020")

        assert section is not None
        assert section.citation.section == "WA-82.08.020"
        assert "tax" in section.text.lower() or "sale" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_capital_gains_section(self):
        """Fetch Washington Capital Gains Tax section 82.87.040."""
        converter = WAConverter()
        try:
            section = converter.fetch_section("82.87.040")
            assert section.citation.section == "WA-82.87.040"
        except WAConverterError:
            pytest.skip("Section 82.87.040 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_82_04_sections(self):
        """Get list of sections in Chapter 82.04."""
        converter = WAConverter()
        sections = converter.get_chapter_section_numbers("82.04")

        assert len(sections) > 0
        assert all(s.startswith("82.04.") for s in sections)
        assert "82.04.010" in sections  # Introductory

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_working_families_credit_section(self):
        """Fetch Washington Working Families Tax Credit section."""
        converter = WAConverter()
        try:
            section = converter.fetch_section("74.62.030")
            assert section.citation.section == "WA-74.62.030"
        except WAConverterError:
            pytest.skip("Section 74.62.030 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_public_assistance_section(self):
        """Fetch Washington public assistance section 74.04.005."""
        converter = WAConverter()
        section = converter.fetch_section("74.04.005")

        assert section.citation.section == "WA-74.04.005"
        assert "definition" in section.text.lower() or "purpose" in section.text.lower()
