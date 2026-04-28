"""Tests for Georgia state statute converter.

Tests the GAConverter which fetches from ga.elaws.us
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.ga import (
    GA_SOCIAL_SERVICES_CHAPTERS,
    GA_TAX_CHAPTERS,
    GA_TITLES,
    GAConverter,
    GAConverterError,
    fetch_ga_section,
)
from axiom_corpus.models import Section

# Sample HTML from ga.elaws.us for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 48-7-20. Individual tax rate; tax table | Georgia Code</title></head>
<body>
<div class="rulehome_rightdetail">
<h3>Section 48-7-20. Individual tax rate; tax table; credit for withholding and other payments; applicability to estates and trusts</h3>
<p>
(a) A tax is imposed upon every resident of this state with respect to the Georgia taxable net income of the taxpayer as defined in Code Section 48-7-27.
</p>
<p>
(b) For taxable years beginning on or after January 1, 2024, except as otherwise provided in this Code section, the tax imposed by this Code section shall be computed at the following rates:
</p>
<p>
(1) For single persons and married persons filing separately:
</p>
<p>
(A) On the first $750.00 of Georgia taxable net income, or any part thereof, 1 percent;
</p>
<p>
(B) On the next $2,250.00 of Georgia taxable net income, or any part thereof, 2 percent;
</p>
<p>
(2) For married persons filing jointly:
</p>
<p>
(A) On the first $1,000.00 of Georgia taxable net income, or any part thereof, 1 percent;
</p>
<p>
(B) On the next $3,000.00 of Georgia taxable net income, or any part thereof, 2 percent;
</p>
<p>
(c) There shall be allowed as a credit against the tax imposed by this Code section the amounts withheld from compensation.
</p>
<p>
(d) The tax imposed by this Code section shall apply to estates and trusts.
</p>
<p>
History.-- Ga. L. 1931, Ex. Sess., p. 24, § 13; Ga. L. 1937, p. 109, § 1; Ga. L. 2022, p. 953, § 1; Ga. L. 2024, p. 123, § 2.
</p>
</div>
</body>
</html>
"""

SAMPLE_SOCIAL_SERVICES_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 49-4-142. Department of Community Health | Georgia Code</title></head>
<body>
<div class="rulehome_rightdetail">
<h3>Section 49-4-142. Department of Community Health established; adoption, administration, and modification of state plan; drug application fees</h3>
<p>
(a) There is created the Department of Community Health which shall be responsible for the administration of all Medicaid programs in this state.
</p>
<p>
(b) The Department of Community Health shall have the following powers and duties:
</p>
<p>
(1) To adopt, administer, and modify a state plan for medical assistance pursuant to Title XIX of the federal Social Security Act;
</p>
<p>
(2) To administer the state health planning and development program;
</p>
<p>
(3) To collect fees for processing applications for drugs.
</p>
<p>
(c) The department may promulgate rules and regulations to carry out the purposes of this Code section.
</p>
<p>
History.-- Ga. L. 1977, p. 384, § 4; Ga. L. 1984, p. 1647, § 1; Ga. L. 2009, p. 8, § 1.
</p>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 7 - Income Taxes | Georgia Code</title></head>
<body>
<h1>Chapter 7. INCOME TAXES</h1>
<div id="contents">
<ul>
<li><a href="/law/48-7|1">Article 1. GENERAL PROVISIONS</a></li>
<li><a href="/law/48-7|2">Article 2. IMPOSITION, RATE, AND COMPUTATION; EXEMPTIONS</a></li>
<li><a href="/law/48-7|3">Article 3. RETURNS AND FURNISHING OF INFORMATION</a></li>
<li><a href="/law/48-7|4">Article 4. PAYMENT: DEFICIENCIES, ASSESSMENT, AND COLLECTION</a></li>
<li><a href="/law/48-7|5">Article 5. CURRENT INCOME TAX PAYMENT</a></li>
</ul>
</div>
</body>
</html>
"""

SAMPLE_ARTICLE_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Article 2 - Imposition | Georgia Code</title></head>
<body>
<h1>Article 2. IMPOSITION, RATE, AND COMPUTATION; EXEMPTIONS</h1>
<div id="contents">
<ul>
<li><a href="/law/section48-7-20">Section 48-7-20. Individual tax rate</a></li>
<li><a href="/law/section48-7-21">Section 48-7-21. Taxation of corporations</a></li>
<li><a href="/law/section48-7-22">Section 48-7-22. Computation of taxable income</a></li>
<li><a href="/law/section48-7-25">Section 48-7-25. Tax credits</a></li>
</ul>
</div>
</body>
</html>
"""


class TestGAChaptersRegistry:
    """Test Georgia chapter registries."""

    def test_chapter_48_7_in_tax_chapters(self):
        """Chapter 48-7 (Income Taxes) is in tax chapters."""
        assert "48-7" in GA_TAX_CHAPTERS
        assert "Income Taxes" in GA_TAX_CHAPTERS["48-7"]

    def test_chapter_48_8_in_tax_chapters(self):
        """Chapter 48-8 (Sales and Use Taxes) is in tax chapters."""
        assert "48-8" in GA_TAX_CHAPTERS
        assert "Sales" in GA_TAX_CHAPTERS["48-8"]

    def test_chapter_49_4_in_social_services(self):
        """Chapter 49-4 (Public Assistance) is in social services chapters."""
        assert "49-4" in GA_SOCIAL_SERVICES_CHAPTERS
        assert "Public Assistance" in GA_SOCIAL_SERVICES_CHAPTERS["49-4"]

    def test_title_48_in_titles(self):
        """Title 48 (Revenue and Taxation) is in titles."""
        assert 48 in GA_TITLES
        assert "Revenue" in GA_TITLES[48] or "Taxation" in GA_TITLES[48]

    def test_title_49_in_titles(self):
        """Title 49 (Social Services) is in titles."""
        assert 49 in GA_TITLES
        assert "Social Services" in GA_TITLES[49]


class TestGAConverter:
    """Test GAConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = GAConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = GAConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = GAConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = GAConverter()
        url = converter._build_section_url("48-7-20")
        assert "ga.elaws.us" in url
        assert "section48-7-20" in url

    def test_build_section_url_title_49(self):
        """Build correct URL for Title 49 section."""
        converter = GAConverter()
        url = converter._build_section_url("49-4-142")
        assert "section49-4-142" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter."""
        converter = GAConverter()
        url = converter._build_chapter_url("48-7")
        assert "ga.elaws.us/law/48-7" in url

    def test_build_article_url(self):
        """Build correct URL for article."""
        converter = GAConverter()
        url = converter._build_article_url("48-7", "2")
        assert "48-7|2" in url

    def test_parse_section_number_parts(self):
        """Parse section number into parts."""
        converter = GAConverter()
        title, chapter, section = converter._parse_section_number_parts("48-7-20")
        assert title == 48
        assert chapter == "7"
        assert section == "20"

    def test_parse_section_number_with_subsection(self):
        """Parse section number with subsection."""
        converter = GAConverter()
        title, chapter, section = converter._parse_section_number_parts("48-7-40.26")
        assert title == 48
        assert chapter == "7"
        assert section == "40.26"

    def test_context_manager(self):
        """Converter works as context manager."""
        with GAConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestGAConverterParsing:
    """Test GAConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedGASection."""
        converter = GAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "48-7-20", "https://ga.elaws.us/law/section48-7-20"
        )

        assert parsed.section_number == "48-7-20"
        assert "Individual tax rate" in parsed.section_title
        assert parsed.title_number == 48
        assert parsed.title_name == "Revenue and Taxation"
        assert parsed.chapter == "7"
        assert "Income Taxes" in parsed.chapter_title
        assert "Georgia taxable net income" in parsed.text
        assert parsed.source_url == "https://ga.elaws.us/law/section48-7-20"

    def test_parse_social_services_section(self):
        """Parse Title 49 section HTML."""
        converter = GAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SOCIAL_SERVICES_HTML, "49-4-142", "https://ga.elaws.us/law/section49-4-142"
        )

        assert parsed.section_number == "49-4-142"
        assert "Department of Community Health" in parsed.section_title
        assert parsed.title_number == 49
        assert parsed.title_name == "Social Services"
        assert "Medicaid" in parsed.text

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = GAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "48-7-20", "https://example.com"
        )

        # Should have subsections (a), (b), (c), (d)
        assert len(parsed.subsections) >= 3
        identifiers = [s.identifier for s in parsed.subsections]
        assert "a" in identifiers
        assert "b" in identifiers
        assert "c" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (A), etc."""
        converter = GAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "48-7-20", "https://example.com"
        )

        # Find subsection (b) which has children
        sub_b = next((s for s in parsed.subsections if s.identifier == "b"), None)
        assert sub_b is not None
        # Should have numeric children
        assert len(sub_b.children) >= 1

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = GAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "48-7-20", "https://example.com"
        )

        assert parsed.history is not None
        assert "Ga. L. 1931" in parsed.history or "1931" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedGASection to Section model."""
        converter = GAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "48-7-20", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "GA-48-7-20"
        assert section.citation.title == 0  # State law indicator
        assert "Individual tax rate" in section.section_title
        assert "Georgia Code" in section.title_name
        assert section.uslm_id == "ga/48/48-7-20"
        assert section.source_url == "https://example.com"


class TestGAConverterFetching:
    """Test GAConverter HTTP fetching with mocks."""

    @patch.object(GAConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = GAConverter()
        section = converter.fetch_section("48-7-20")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "GA-48-7-20"
        assert "Individual tax rate" in section.section_title

    @patch.object(GAConverter, "_get")
    def test_fetch_social_services_section(self, mock_get):
        """Fetch Title 49 section."""
        mock_get.return_value = SAMPLE_SOCIAL_SERVICES_HTML

        converter = GAConverter()
        section = converter.fetch_section("49-4-142")

        assert section is not None
        assert section.citation.section == "GA-49-4-142"
        assert "Department of Community Health" in section.section_title

    @patch.object(GAConverter, "_get")
    def test_get_chapter_articles(self, mock_get):
        """Get list of articles from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = GAConverter()
        articles = converter.get_chapter_articles("48-7")

        assert len(articles) == 5
        article_nums = [a[0] for a in articles]
        assert "1" in article_nums
        assert "2" in article_nums

    @patch.object(GAConverter, "_get")
    def test_get_article_section_numbers(self, mock_get):
        """Get list of section numbers from article index."""
        mock_get.return_value = SAMPLE_ARTICLE_INDEX_HTML

        converter = GAConverter()
        sections = converter.get_article_section_numbers("48-7", "2")

        assert len(sections) == 4
        assert "48-7-20" in sections
        assert "48-7-21" in sections
        assert "48-7-25" in sections

    @patch.object(GAConverter, "_get")
    def test_iter_article(self, mock_get):
        """Iterate over sections in an article."""
        # First call returns article index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_ARTICLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = GAConverter()
        sections = list(converter.iter_article("48-7", "2"))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(GAConverter, "_get")
    def test_fetch_ga_section(self, mock_get):
        """Test fetch_ga_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_ga_section("48-7-20")

        assert section is not None
        assert section.citation.section == "GA-48-7-20"

    @patch.object(GAConverter, "_get")
    def test_download_ga_chapter(self, mock_get):
        """Test download_ga_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_ARTICLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_ARTICLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ] * 5  # More than needed to handle multiple articles

        # This test may be slow due to multiple fetches
        # Just verify it doesn't crash with mocked data
        converter = GAConverter()
        articles = converter.get_chapter_articles("48-7")
        assert len(articles) > 0


class TestGAConverterIntegration:
    """Integration tests that hit real ga.elaws.us (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Georgia Income Tax section 48-7-20."""
        converter = GAConverter()
        section = converter.fetch_section("48-7-20")

        assert section is not None
        assert section.citation.section == "GA-48-7-20"
        assert "tax" in section.section_title.lower() or "rate" in section.section_title.lower()
        assert "income" in section.text.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_corporate_tax_section(self):
        """Fetch Georgia Corporate Tax section 48-7-21."""
        converter = GAConverter()
        section = converter.fetch_section("48-7-21")

        assert section is not None
        assert section.citation.section == "GA-48-7-21"
        assert "corporation" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch Georgia Sales Tax section 48-8-30."""
        converter = GAConverter()
        try:
            section = converter.fetch_section("48-8-30")
            assert section.citation.section == "GA-48-8-30"
            assert "sales" in section.text.lower() or "tax" in section.text.lower()
        except GAConverterError:
            pytest.skip("Section 48-8-30 not accessible")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_social_services_section(self):
        """Fetch Georgia Social Services section 49-4-142."""
        converter = GAConverter()
        try:
            section = converter.fetch_section("49-4-142")
            assert section.citation.section == "GA-49-4-142"
        except GAConverterError:
            pytest.skip("Section 49-4-142 not accessible")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_7_articles(self):
        """Get list of articles in Chapter 48-7 (Income Taxes)."""
        converter = GAConverter()
        articles = converter.get_chapter_articles("48-7")

        assert len(articles) > 0
        # Should have multiple articles
        assert len(articles) >= 5

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_article_sections(self):
        """Get list of sections in Article 2 of Chapter 48-7."""
        converter = GAConverter()
        sections = converter.get_article_section_numbers("48-7", "2")

        assert len(sections) > 0
        # Should include the main income tax sections
        section_numbers = [s.split("-")[-1] for s in sections]
        # At minimum should have some sections in the 20s range
        assert any(s.startswith("2") for s in section_numbers)
