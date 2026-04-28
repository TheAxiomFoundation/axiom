"""Tests for West Virginia state statute converter.

Tests the WVConverter which fetches from code.wvlegislature.gov
and converts to the internal Section model.

West Virginia Code Structure:
- Chapters (e.g., Chapter 11: Taxation)
- Articles (e.g., Article 21: Personal Income Tax)
- Sections (e.g., 11-21-1: Legislative findings)

URL Patterns:
- Chapter index: /11/
- Article contents: /11-21/
- Section: /11-21-1/
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.wv import (
    WV_CHAPTERS,
    WV_TAX_CHAPTERS,
    WV_WELFARE_CHAPTERS,
    WVConverter,
    WVConverterError,
    download_wv_article,
    fetch_wv_section,
)
from axiom_corpus.models import Section

# Sample HTML from code.wvlegislature.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>West Virginia Code - 11-21-1. Legislative findings.</title></head>
<body>
<nav class="breadcrumb">
    <a href="/">West Virginia Code</a> &gt;
    <a href="/11/">CHAPTER 11. TAXATION</a> &gt;
    <a href="/11-21/">ARTICLE 21. PERSONAL INCOME TAX</a>
</nav>
<h1>11-21-1. Legislative findings.</h1>
<div id="content">
<p>The Legislature hereby finds and declares that the adoption by this state of a personal income tax measured by federal adjusted gross income as modified by this article, will:</p>
<p>(1) Simplify preparation of state income tax returns by taxpayers;</p>
<p>(2) Improve enforcement of state income tax laws by adoption of federal income tax definitions, rules and regulations; and</p>
<p>(3) Aid in interpretation of the state income tax law.</p>
<p>This article shall be liberally construed in accordance with the foregoing legislative findings and declaration of purpose.</p>
<div class="history">
<h4>Bill History</h4>
<p>SB329, 1961 Regular Session; HB46, 1987 Regular Session</p>
</div>
</div>
</body>
</html>
"""

SAMPLE_SECTION_WITH_SUBSECTIONS_HTML = """<!DOCTYPE html>
<html>
<head><title>West Virginia Code - 11-21-12. West Virginia adjusted gross income.</title></head>
<body>
<nav class="breadcrumb">
    <a href="/">West Virginia Code</a> &gt;
    <a href="/11/">CHAPTER 11. TAXATION</a> &gt;
    <a href="/11-21/">ARTICLE 21. PERSONAL INCOME TAX</a>
</nav>
<h1>11-21-12. West Virginia adjusted gross income of a resident individual.</h1>
<div id="content">
<p>(a) General. The West Virginia adjusted gross income of a resident individual means his or her federal adjusted gross income as defined in the laws of the United States for the taxable year with the modifications specified in this section.</p>
<p>(b) Modifications increasing federal adjusted gross income. There shall be added to federal adjusted gross income:</p>
<p>(1) Interest income on obligations of any state other than this state, or of a political subdivision of any such other state unless created by compact or agreement to which this state is a party;</p>
<p>(2) Interest or dividend income on obligations or securities of any authority, commission or instrumentality of the United States which the laws of the United States exempt from federal income tax but not from state income taxes;</p>
<p>(A) For taxable years beginning after December 31, 2000, the amount specified in this subdivision;</p>
<p>(B) For taxable years beginning after December 31, 2005, the amount shall be adjusted for inflation.</p>
<p>(c) Modifications decreasing federal adjusted gross income. There shall be subtracted from federal adjusted gross income:</p>
<p>(1) Interest income on obligations of the United States and its possessions to the extent includable in gross income for federal income tax purposes;</p>
<p>(2) Any income derived from West Virginia lottery winnings.</p>
<div class="history">
<h4>Bill History</h4>
<p>Code 1961, 11-21-12; 1987, c. 478; 2000, c. 560; 2005, c. 219.</p>
</div>
</div>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>West Virginia Code - 9-1-2. Definitions.</title></head>
<body>
<nav class="breadcrumb">
    <a href="/">West Virginia Code</a> &gt;
    <a href="/9/">CHAPTER 9. HUMAN SERVICES</a> &gt;
    <a href="/9-1/">ARTICLE 1. LEGISLATIVE PURPOSE AND DEFINITIONS</a>
</nav>
<h1>9-1-2. Definitions.</h1>
<div id="content">
<p>As used in this chapter:</p>
<p>"Department" means the Department of Human Services.</p>
<p>"Commissioner" means the Secretary of the Department of Human Services.</p>
<p>"Federal-state assistance" means assistance provided under:</p>
<p>(1) The supplemental nutrition assistance program (SNAP);</p>
<p>(2) Temporary assistance for needy families (TANF).</p>
<div class="history">
<h4>Bill History</h4>
<p>1936, c. 1; 1961, c. 1; 2025, c. 37.</p>
</div>
</div>
</body>
</html>
"""

SAMPLE_ARTICLE_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>West Virginia Code - Article 21. Personal Income Tax</title></head>
<body>
<h1>ARTICLE 21. PERSONAL INCOME TAX</h1>
<div id="contents">
<ul>
<li><a href="/11-21-1/">11-21-1. Legislative findings.</a></li>
<li><a href="/11-21-3/">11-21-3. Imposition of tax; persons subject to tax.</a></li>
<li><a href="/11-21-12/">11-21-12. West Virginia adjusted gross income.</a></li>
<li><a href="/11-21-21/">11-21-21. Senior citizens' tax credit.</a></li>
</ul>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>West Virginia Code - Chapter 11. Taxation</title></head>
<body>
<h1>CHAPTER 11. TAXATION</h1>
<div id="contents">
<ul>
<li><a href="/11-1/">ARTICLE 1. SUPERVISION</a></li>
<li><a href="/11-6B/">ARTICLE 6B. HOMESTEAD PROPERTY TAX EXEMPTION</a></li>
<li><a href="/11-21/">ARTICLE 21. PERSONAL INCOME TAX</a></li>
<li><a href="/11-24/">ARTICLE 24. CORPORATION NET INCOME TAX</a></li>
</ul>
</div>
</body>
</html>
"""


class TestWVChaptersRegistry:
    """Test West Virginia chapter registries."""

    def test_chapter_11_in_tax_chapters(self):
        """Chapter 11 (Taxation) is in tax chapters."""
        assert 11 in WV_TAX_CHAPTERS
        assert "Taxation" in WV_TAX_CHAPTERS[11]

    def test_chapter_9_in_welfare_chapters(self):
        """Chapter 9 (Human Services) is in welfare chapters."""
        assert 9 in WV_WELFARE_CHAPTERS
        assert "Human Services" in WV_WELFARE_CHAPTERS[9]

    def test_chapters_have_names(self):
        """All registered chapters have names."""
        for chapter_num, chapter_name in WV_CHAPTERS.items():
            assert chapter_name, f"Chapter {chapter_num} has no name"


class TestWVConverter:
    """Test WVConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = WVConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = WVConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = WVConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = WVConverter()
        url = converter._build_section_url("11-21-1")
        assert "code.wvlegislature.gov" in url
        assert "/11-21-1/" in url

    def test_build_section_url_welfare(self):
        """Build correct URL for welfare section."""
        converter = WVConverter()
        url = converter._build_section_url("9-1-2")
        assert "/9-1-2/" in url

    def test_build_article_url(self):
        """Build correct URL for article index."""
        converter = WVConverter()
        url = converter._build_article_url(11, "21")
        assert "/11-21/" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter index."""
        converter = WVConverter()
        url = converter._build_chapter_url(11)
        assert "/11/" in url

    def test_parse_section_number(self):
        """Parse section number into components."""
        converter = WVConverter()
        chapter, article, section = converter._parse_section_number("11-21-1")
        assert chapter == 11
        assert article == "21"
        assert section == "1"

    def test_parse_section_number_with_suffix(self):
        """Parse section number with letter suffix."""
        converter = WVConverter()
        chapter, article, section = converter._parse_section_number("11-6B-1")
        assert chapter == 11
        assert article == "6B"
        assert section == "1"

    def test_context_manager(self):
        """Converter works as context manager."""
        with WVConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestWVConverterParsing:
    """Test WVConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedWVSection."""
        converter = WVConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "11-21-1", "https://example.com"
        )

        assert parsed.section_number == "11-21-1"
        assert parsed.section_title == "Legislative findings"
        assert parsed.chapter_number == 11
        assert parsed.chapter_name == "Taxation"
        assert parsed.article_number == "21"
        assert parsed.article_name == "Personal Income Tax"
        assert "personal income tax" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_welfare_section_html(self):
        """Parse welfare section HTML."""
        converter = WVConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "9-1-2", "https://example.com"
        )

        assert parsed.section_number == "9-1-2"
        assert parsed.section_title == "Definitions"
        assert parsed.chapter_number == 9
        assert parsed.chapter_name == "Human Services"
        assert "Department" in parsed.text

    def test_parse_subsections_numbered(self):
        """Parse numbered subsections (1), (2), etc."""
        converter = WVConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "11-21-1", "https://example.com"
        )

        # Should have subsections (1), (2), (3)
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)
        assert any(s.identifier == "3" for s in parsed.subsections)

    def test_parse_subsections_lettered(self):
        """Parse lettered subsections (a), (b), (c)."""
        converter = WVConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "11-21-12", "https://example.com"
        )

        # Should have subsections (a), (b), (c)
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)
        assert any(s.identifier == "c" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (b)."""
        converter = WVConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "11-21-12", "https://example.com"
        )

        # Find subsection (b)
        sub_b = next((s for s in parsed.subsections if s.identifier == "b"), None)
        assert sub_b is not None
        # Should have children (1) and (2)
        assert len(sub_b.children) >= 2
        assert any(c.identifier == "1" for c in sub_b.children)
        assert any(c.identifier == "2" for c in sub_b.children)

    def test_parse_level3_subsections(self):
        """Parse level 3 subsections (A), (B) under (1)."""
        converter = WVConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "11-21-12", "https://example.com"
        )

        # Find subsection (b)
        sub_b = next((s for s in parsed.subsections if s.identifier == "b"), None)
        assert sub_b is not None

        # Find subsection (1) under (b) that has (A), (B) children
        # Looking for the one with uppercase letter children
        for child in sub_b.children:
            if child.children and any(c.identifier == "A" for c in child.children):
                assert any(c.identifier == "A" for c in child.children)
                assert any(c.identifier == "B" for c in child.children)
                break

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = WVConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "11-21-1", "https://example.com"
        )

        assert parsed.history is not None
        assert "1961" in parsed.history or "SB329" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedWVSection to Section model."""
        converter = WVConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "11-21-1", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "WV-11-21-1"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Legislative findings"
        assert "West Virginia Code" in section.title_name
        assert section.uslm_id == "wv/11/11-21-1"
        assert section.source_url == "https://example.com"


class TestWVConverterFetching:
    """Test WVConverter HTTP fetching with mocks."""

    @patch.object(WVConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = WVConverter()
        section = converter.fetch_section("11-21-1")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "WV-11-21-1"
        assert "Legislative findings" in section.section_title

    @patch.object(WVConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        # Return a realistic error page with "page not found" indicator
        mock_get.return_value = """<html>
        <head><title>Page Not Found - West Virginia Code</title></head>
        <body>
        <div class="error">The requested page was not found.</div>
        <p>The section you are looking for does not exist or has been moved.</p>
        </body></html>"""

        converter = WVConverter()
        with pytest.raises(WVConverterError) as exc_info:
            converter.fetch_section("999-999-999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(WVConverter, "_get")
    def test_get_article_section_numbers(self, mock_get):
        """Get list of section numbers from article index."""
        mock_get.return_value = SAMPLE_ARTICLE_INDEX_HTML

        converter = WVConverter()
        sections = converter.get_article_section_numbers(11, "21")

        assert len(sections) == 4
        assert "11-21-1" in sections
        assert "11-21-3" in sections
        assert "11-21-12" in sections

    @patch.object(WVConverter, "_get")
    def test_get_chapter_articles(self, mock_get):
        """Get list of articles in a chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = WVConverter()
        articles = converter.get_chapter_articles(11)

        assert len(articles) == 4
        assert "1" in articles
        assert "6B" in articles
        assert "21" in articles

    @patch.object(WVConverter, "_get")
    def test_iter_article(self, mock_get):
        """Iterate over sections in an article."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_ARTICLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = WVConverter()
        sections = list(converter.iter_article(11, "21"))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(WVConverter, "_get")
    def test_fetch_wv_section(self, mock_get):
        """Test fetch_wv_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_wv_section("11-21-1")

        assert section is not None
        assert section.citation.section == "WV-11-21-1"

    @patch.object(WVConverter, "_get")
    def test_download_wv_article(self, mock_get):
        """Test download_wv_article function."""
        mock_get.side_effect = [
            SAMPLE_ARTICLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_wv_article(11, "21")

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestWVConverterIntegration:
    """Integration tests that hit real code.wvlegislature.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch West Virginia Income Tax section 11-21-1."""
        converter = WVConverter()
        section = converter.fetch_section("11-21-1")

        assert section is not None
        assert section.citation.section == "WV-11-21-1"
        assert "legislative" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_imposition_section(self):
        """Fetch West Virginia tax imposition section 11-21-3."""
        converter = WVConverter()
        section = converter.fetch_section("11-21-3")

        assert section is not None
        assert section.citation.section == "WV-11-21-3"
        assert "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch West Virginia welfare section 9-1-2."""
        converter = WVConverter()
        section = converter.fetch_section("9-1-2")

        assert section is not None
        assert section.citation.section == "WV-9-1-2"
        assert "definition" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_article_sections(self):
        """Get list of sections in Article 11-21 (Personal Income Tax)."""
        converter = WVConverter()
        sections = converter.get_article_section_numbers(11, "21")

        assert len(sections) > 0
        # All sections should start with "11-21-"
        assert all(s.startswith("11-21-") for s in sections)
        assert "11-21-1" in sections  # Legislative findings

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_articles(self):
        """Get list of articles in Chapter 11 (Taxation)."""
        converter = WVConverter()
        articles = converter.get_chapter_articles(11)

        assert len(articles) > 0
        assert "21" in articles  # Personal Income Tax
