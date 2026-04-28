"""Tests for Indiana state statute converter.

Tests the INConverter which fetches from Justia (as iga.in.gov is a JavaScript SPA)
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.in_ import (
    IN_TAX_ARTICLES,
    IN_TITLES,
    IN_WELFARE_ARTICLES,
    INConverter,
    INConverterError,
    download_in_article,
    fetch_in_section,
)
from axiom.models import Section

# Sample HTML from Justia for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head>
<title>IC 6-3-1-3.5 - Adjusted Gross Income :: 2024 Indiana Code</title>
<meta name="description" content="Section 6-3-1-3.5 - Adjusted Gross Income">
</head>
<body>
<main>
<h1>6-3-1-3.5. "Adjusted Gross Income"</h1>
<div class="codes-content">
<p>Sec. 3.5. (a) As used in this article, "adjusted gross income" means the following:</p>
<p>(1) In the case of a corporation, the term means the corporation's taxable income as defined in Section 63 of the Internal Revenue Code.</p>
<p>(2) In the case of an individual, the term means the individual's adjusted gross income as defined in Section 62 of the Internal Revenue Code.</p>
<p>(A) For purposes of this subdivision, the term includes all items of gross income.</p>
<p>(B) The term does not include exempt interest income.</p>
<p>(b) The following modifications shall be made to adjusted gross income:</p>
<p>(1) Subtract income that is exempt from taxation under Indiana law.</p>
<p>(2) Add back any deductions not allowed under Indiana law.</p>
<p>History: As amended through P.L. 123-2024.</p>
<p>Effective: July 1, 2024</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_TANF_SECTION_HTML = """<!DOCTYPE html>
<html>
<head>
<title>IC 12-14-1-1 - Eligibility :: 2024 Indiana Code</title>
</head>
<body>
<main>
<h1>12-14-1-1. Eligibility</h1>
<div class="codes-content">
<p>Sec. 1. (a) Assistance under the TANF program shall be given to a dependent child who otherwise qualifies for assistance if the child:</p>
<p>(1) is living in a family home of a person who is at least eighteen (18) years of age; and</p>
<p>(2) is the child's relative.</p>
<p>(b) A family must meet income and resource requirements established by the division.</p>
<p>History: As amended through P.L. 45-2024.</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 1 - Definitions :: Indiana Code Title 6</title></head>
<body>
<main>
<h1>Chapter 1 - Definitions</h1>
<div id="codes">
<ul>
<li><a href="/codes/indiana/title-6/article-3/chapter-1/section-6-3-1-1/">6-3-1-1. Application of article</a></li>
<li><a href="/codes/indiana/title-6/article-3/chapter-1/section-6-3-1-2/">6-3-1-2. "Corporation"</a></li>
<li><a href="/codes/indiana/title-6/article-3/chapter-1/section-6-3-1-3.5/">6-3-1-3.5. "Adjusted gross income"</a></li>
<li><a href="/codes/indiana/title-6/article-3/chapter-1/section-6-3-1-4/">6-3-1-4. "Resident"</a></li>
</ul>
</div>
</main>
</body>
</html>
"""

SAMPLE_ARTICLE_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Article 3 - State Income Taxes :: Indiana Code Title 6</title></head>
<body>
<main>
<h1>Article 3 - State Income Taxes</h1>
<div id="codes">
<ul>
<li><a href="/codes/indiana/title-6/article-3/chapter-1/">Chapter 1 - Definitions</a></li>
<li><a href="/codes/indiana/title-6/article-3/chapter-2/">Chapter 2 - Imposition of Tax</a></li>
<li><a href="/codes/indiana/title-6/article-3/chapter-3/">Chapter 3 - Credits</a></li>
</ul>
</div>
</main>
</body>
</html>
"""


class TestINTitlesRegistry:
    """Test Indiana title and article registries."""

    def test_title_6_is_taxation(self):
        """Title 6 is Taxation."""
        assert 6 in IN_TITLES
        assert IN_TITLES[6] == "Taxation"

    def test_title_12_is_human_services(self):
        """Title 12 is Human Services."""
        assert 12 in IN_TITLES
        assert IN_TITLES[12] == "Human Services"

    def test_article_6_3_in_tax_articles(self):
        """Article 6-3 (State Income Taxes) is in tax articles."""
        assert "6-3" in IN_TAX_ARTICLES
        assert "Income" in IN_TAX_ARTICLES["6-3"]

    def test_article_12_14_in_welfare_articles(self):
        """Article 12-14 (Family Assistance/TANF) is in welfare articles."""
        assert "12-14" in IN_WELFARE_ARTICLES
        assert "Family" in IN_WELFARE_ARTICLES["12-14"]


class TestINConverter:
    """Test INConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = INConverter()
        assert converter.rate_limit_delay == 1.0
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = INConverter(rate_limit_delay=2.0)
        assert converter.rate_limit_delay == 2.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = INConverter(year=2024)
        assert converter.year == 2024

    def test_parse_section_number(self):
        """Parse section number into components."""
        converter = INConverter()
        title, article, chapter, section = converter._parse_section_number("6-3-1-3.5")
        assert title == 6
        assert article == "3"
        assert chapter == "1"
        assert section == "3.5"

    def test_parse_section_number_with_suffix(self):
        """Parse section number with suffix."""
        converter = INConverter()
        title, article, chapter, section = converter._parse_section_number("12-14-1-1-b")
        assert title == 12
        assert article == "14"
        assert chapter == "1"
        assert section == "1-b"

    def test_parse_section_number_invalid(self):
        """Invalid section number raises ValueError."""
        converter = INConverter()
        with pytest.raises(ValueError):
            converter._parse_section_number("invalid")

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = INConverter()
        url = converter._build_section_url("6-3-1-3.5")
        assert "law.justia.com/codes/indiana" in url
        assert "title-6" in url
        assert "article-3" in url
        assert "chapter-1" in url
        assert "section-6-3-1-3.5" in url

    def test_build_article_url(self):
        """Build correct URL for article index."""
        converter = INConverter()
        url = converter._build_article_url("6-3")
        assert "title-6" in url
        assert "article-3" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter index."""
        converter = INConverter()
        url = converter._build_chapter_url(6, "3", "1")
        assert "title-6" in url
        assert "article-3" in url
        assert "chapter-1" in url

    def test_get_title_name(self):
        """Get title name from number."""
        converter = INConverter()
        assert converter._get_title_name(6) == "Taxation"
        assert converter._get_title_name(12) == "Human Services"
        assert converter._get_title_name(999) is None

    def test_get_article_name(self):
        """Get article name from code."""
        converter = INConverter()
        assert converter._get_article_name("6-3") == "State Income Taxes"
        assert converter._get_article_name("12-14") == "Family Assistance Services"
        assert converter._get_article_name("99-99") is None

    def test_context_manager(self):
        """Converter works as context manager."""
        with INConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestINConverterParsing:
    """Test INConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedINSection."""
        converter = INConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "6-3-1-3.5", "https://example.com"
        )

        assert parsed.section_number == "6-3-1-3.5"
        assert "Adjusted Gross Income" in parsed.section_title
        assert parsed.title_number == 6
        assert parsed.article_number == "3"
        assert parsed.chapter_number == "1"
        assert parsed.title_name == "Taxation"
        assert parsed.article_name == "State Income Taxes"
        assert "Internal Revenue Code" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_effective_date(self):
        """Parse effective date from section HTML."""
        converter = INConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "6-3-1-3.5", "https://example.com"
        )

        assert parsed.effective_date == date(2024, 7, 1)

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = INConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "6-3-1-3.5", "https://example.com"
        )

        # Should have subsections (a) and (b)
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (a)."""
        converter = INConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "6-3-1-3.5", "https://example.com"
        )

        # Find subsection (a)
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        # Should have children (1) and (2)
        assert len(sub_a.children) >= 2
        assert any(c.identifier == "1" for c in sub_a.children)
        assert any(c.identifier == "2" for c in sub_a.children)

    def test_parse_level3_subsections(self):
        """Parse level 3 subsections (A), (B) under (2)."""
        converter = INConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "6-3-1-3.5", "https://example.com"
        )

        # Find subsection (a) -> (2) -> should have (A), (B)
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None

        sub_2 = next((c for c in sub_a.children if c.identifier == "2"), None)
        assert sub_2 is not None
        assert len(sub_2.children) >= 2
        assert any(gc.identifier == "A" for gc in sub_2.children)
        assert any(gc.identifier == "B" for gc in sub_2.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = INConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "6-3-1-3.5", "https://example.com"
        )

        assert parsed.history is not None
        assert "P.L." in parsed.history

    def test_parse_tanf_section(self):
        """Parse TANF eligibility section."""
        converter = INConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TANF_SECTION_HTML, "12-14-1-1", "https://example.com"
        )

        assert parsed.section_number == "12-14-1-1"
        assert "Eligibility" in parsed.section_title
        assert parsed.title_number == 12
        assert parsed.article_number == "14"
        assert parsed.title_name == "Human Services"
        assert parsed.article_name == "Family Assistance Services"
        assert "TANF" in parsed.text

    def test_to_section_model(self):
        """Convert ParsedINSection to Section model."""
        converter = INConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "6-3-1-3.5", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "IN-6-3-1-3.5"
        assert section.citation.title == 0  # State law indicator
        assert "Adjusted Gross Income" in section.section_title
        assert "Indiana Code" in section.title_name
        assert "Taxation" in section.title_name
        assert section.uslm_id == "in/6/6-3/6-3-1-3.5"
        assert section.source_url == "https://example.com"


class TestINConverterFetching:
    """Test INConverter HTTP fetching with mocks."""

    @patch.object(INConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = INConverter()
        section = converter.fetch_section("6-3-1-3.5")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "IN-6-3-1-3.5"
        assert "Adjusted Gross Income" in section.section_title

    @patch.object(INConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>404 - Not Found</body></html>"

        converter = INConverter()
        with pytest.raises(INConverterError) as exc_info:
            converter.fetch_section("99-99-99-99")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(INConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = INConverter()
        sections = converter.get_chapter_section_numbers(6, "3", "1")

        assert len(sections) == 4
        assert "6-3-1-1" in sections
        assert "6-3-1-3.5" in sections

    @patch.object(INConverter, "_get")
    def test_get_article_chapters(self, mock_get):
        """Get list of chapters from article index."""
        mock_get.return_value = SAMPLE_ARTICLE_INDEX_HTML

        converter = INConverter()
        chapters = converter.get_article_chapters("6-3")

        assert len(chapters) == 3
        assert ("6", "3", "1") in chapters
        assert ("6", "3", "2") in chapters

    @patch.object(INConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = INConverter()
        sections = list(converter.iter_chapter(6, "3", "1"))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(INConverter, "_get")
    def test_fetch_in_section(self, mock_get):
        """Test fetch_in_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_in_section("6-3-1-3.5")

        assert section is not None
        assert section.citation.section == "IN-6-3-1-3.5"

    @patch.object(INConverter, "_get")
    def test_download_in_article(self, mock_get):
        """Test download_in_article function."""
        mock_get.side_effect = [
            SAMPLE_ARTICLE_INDEX_HTML,
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_in_article("6-3")

        assert len(sections) > 0
        assert all(isinstance(s, Section) for s in sections)


class TestINConverterIntegration:
    """Integration tests that hit real Justia (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Indiana Income Tax section 6-3-1-3.5."""
        converter = INConverter()
        try:
            section = converter.fetch_section("6-3-1-3.5")

            assert section is not None
            assert section.citation.section == "IN-6-3-1-3.5"
            assert "income" in section.text.lower()
        except INConverterError:
            pytest.skip("Could not fetch section from Justia")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tanf_section(self):
        """Fetch Indiana TANF eligibility section 12-14-1-1."""
        converter = INConverter()
        try:
            section = converter.fetch_section("12-14-1-1")

            assert section is not None
            assert section.citation.section == "IN-12-14-1-1"
            # Should contain TANF-related content
            assert (
                "tanf" in section.text.lower()
                or "assistance" in section.text.lower()
                or "eligib" in section.text.lower()
            )
        except INConverterError:
            pytest.skip("Could not fetch section from Justia")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_article_6_3_chapters(self):
        """Get list of chapters in Article 6-3 (Income Tax)."""
        converter = INConverter()
        try:
            chapters = converter.get_article_chapters("6-3")

            assert len(chapters) > 0
            # All should be from Title 6, Article 3
            assert all(ch[0] == "6" and ch[1] == "3" for ch in chapters)
        except Exception:
            pytest.skip("Could not fetch article index from Justia")
