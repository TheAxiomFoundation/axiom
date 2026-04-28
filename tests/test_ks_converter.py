"""Tests for Kansas state statute converter.

Tests the KSConverter which fetches from kslegislature.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.ks import (
    KS_TAX_ARTICLES,
    KS_WELFARE_ARTICLES,
    KSConverter,
    KSConverterError,
    download_ks_article,
    fetch_ks_section,
)
from axiom.models import Section

# Sample HTML from kslegislature.gov for testing - simple section
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Statute | Kansas State Legislature</title></head>
<body>
<h1>2024 Statute</h1>
<table>
<tr>
<td>Prev</td>
<td>Article 32. - INCOME TAX</td>
<td>Next</td>
</tr>
</table>
<table>
<tr>
<td colspan="3">79-3201. Title. The title of this act shall be "Kansas income tax act."
History: L. 1933, ch. 320, § 1; March 29.</td>
</tr>
</table>
</body>
</html>
"""

# Sample HTML with subsections
SAMPLE_SECTION_WITH_SUBSECTIONS_HTML = """<!DOCTYPE html>
<html>
<head><title>Statute | Kansas State Legislature</title></head>
<body>
<h1>2024 Statute</h1>
<table>
<tr>
<td>Prev</td>
<td>Article 32. - INCOME TAX</td>
<td>Next</td>
</tr>
</table>
<table>
<tr>
<td colspan="3">
79-3220. Requirements for individuals, corporations, fiduciaries and partnerships with regard to returns.
(a) (1) Each individual required to file a federal income tax return and any other individual whose gross income exceeds the sum of such individual's applicable Kansas standard deduction amount and Kansas personal exemption amount shall each make and sign a return or statement stating specifically such items as are required by the forms and rules and regulations of the secretary of revenue.
(2) In accordance with the provisions of K.S.A. 75-5151a, and amendments thereto, an individual who is required to file a return may file such return by electronic means in a manner approved by the secretary of revenue.
(3) For purposes of this subsection, a nonresident individual or fiduciary whose only source of income from this state is income from an electing pass-through entity under the salt parity act shall not be required to file a return.
(b) Every corporation subject to taxation under this act shall make a return, or statement stating specifically such items as may be required by the forms and regulations of the secretary of revenue.
(c) Every fiduciary, except a receiver appointed by authority of law in possession of part only of the property of an individual shall make and sign a return.
(d) Every partnership shall make a return for each taxable year, stating specifically such items as may be required by the forms and regulations of the secretary of revenue.
</td>
</tr>
</table>
<table>
<tr>
<td>History:</td>
<td>L. 1933, ch. 320, § 20; L. 2022, ch. 63, § 38; July 1.</td>
</tr>
</table>
</body>
</html>
"""

# Sample article index HTML
SAMPLE_ARTICLE_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Article 32 - Income Tax</title></head>
<body>
<h1>2024 Statute</h1>
<div>
<a href="079_032_0001_section/079_032_0001_k/">79-3201 - Title.</a>
<a href="079_032_0020_section/079_032_0020_k/">79-3220 - Requirements for individuals, corporations, fiduciaries and partnerships with regard to returns.</a>
<a href="079_032_0021_section/079_032_0021_k/">79-3221 - Returns; form, place and time of filing.</a>
<a href="079_032_0025_section/079_032_0025_k/">79-3225 - Time for payment of tax.</a>
</div>
</body>
</html>
"""


class TestKSArticlesRegistry:
    """Test Kansas article registries."""

    def test_article_32_in_tax_articles(self):
        """Article 32 (Income Tax) is in tax articles."""
        assert 32 in KS_TAX_ARTICLES
        assert "Income Tax" in KS_TAX_ARTICLES[32]

    def test_article_36_in_tax_articles(self):
        """Article 36 (Retailers' Sales Tax) is in tax articles."""
        assert 36 in KS_TAX_ARTICLES
        assert "Sales Tax" in KS_TAX_ARTICLES[36]

    def test_article_7_in_welfare_articles(self):
        """Article 7 (General Assistance) is in welfare articles."""
        assert 7 in KS_WELFARE_ARTICLES
        assert "General Assistance" in KS_WELFARE_ARTICLES[7]


class TestKSConverter:
    """Test KSConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = KSConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = KSConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = KSConverter(year=2024)
        assert converter.year == 2024

    def test_format_chapter(self):
        """Format chapter number correctly."""
        converter = KSConverter()
        assert converter._format_chapter(79) == "079"
        assert converter._format_chapter(1) == "001"
        assert converter._format_chapter(100) == "100"

    def test_format_article(self):
        """Format article number correctly."""
        converter = KSConverter()
        assert converter._format_article(32) == "032"
        assert converter._format_article(1) == "001"

    def test_format_section(self):
        """Format section number correctly."""
        converter = KSConverter()
        assert converter._format_section(1) == "0001"
        assert converter._format_section(20) == "0020"
        assert converter._format_section(3201) == "3201"

    def test_parse_section_number(self):
        """Parse section number into components."""
        converter = KSConverter()

        chapter, article, section = converter._parse_section_number("79-3201")
        assert chapter == 79
        assert article == 32
        assert section == 1

        chapter, article, section = converter._parse_section_number("79-3220")
        assert chapter == 79
        assert article == 32
        assert section == 20

    def test_build_chapter_url(self):
        """Build correct URL for chapter."""
        converter = KSConverter()
        url = converter._build_chapter_url(79)
        assert "kslegislature.gov" in url
        assert "079_000_0000_chapter" in url

    def test_build_article_url(self):
        """Build correct URL for article."""
        converter = KSConverter()
        url = converter._build_article_url(79, 32)
        assert "079_000_0000_chapter" in url
        assert "079_032_0000_article" in url

    def test_build_section_url(self):
        """Build correct URL for section."""
        converter = KSConverter()
        url = converter._build_section_url(79, 32, 1)
        assert "079_000_0000_chapter" in url
        assert "079_032_0000_article" in url
        assert "079_032_0001_section" in url
        assert "079_032_0001_k" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with KSConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestKSConverterParsing:
    """Test KSConverter HTML parsing."""

    def test_parse_simple_section_html(self):
        """Parse simple section HTML into ParsedKSSection."""
        converter = KSConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "79-3201", "https://example.com"
        )

        assert parsed.section_number == "79-3201"
        assert parsed.section_title == "Title"
        assert parsed.chapter_number == 79
        assert parsed.chapter_title == "Taxation"
        assert parsed.article_number == 32
        assert parsed.article_title == "Income Tax"
        assert "Kansas income tax act" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = KSConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "79-3201", "https://example.com"
        )

        assert parsed.history is not None
        assert "1933" in parsed.history

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = KSConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "79-3220", "https://example.com"
        )

        # Should have subsections (a), (b), (c), (d)
        assert len(parsed.subsections) >= 3
        identifiers = [s.identifier for s in parsed.subsections]
        assert "a" in identifiers
        assert "b" in identifiers

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (a)."""
        converter = KSConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "79-3220", "https://example.com"
        )

        # Find subsection (a)
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        # Should have children (1), (2), (3)
        assert len(sub_a.children) >= 2
        child_ids = [c.identifier for c in sub_a.children]
        assert "1" in child_ids
        assert "2" in child_ids

    def test_to_section_model(self):
        """Convert ParsedKSSection to Section model."""
        converter = KSConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "79-3201", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "KS-79-3201"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Title"
        assert "Kansas Statutes" in section.title_name
        assert section.uslm_id == "ks/79/32/79-3201"
        assert section.source_url == "https://example.com"


class TestKSConverterFetching:
    """Test KSConverter HTTP fetching with mocks."""

    @patch.object(KSConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = KSConverter()
        section = converter.fetch_section("79-3201")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "KS-79-3201"
        assert "Title" in section.section_title

    @patch.object(KSConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section cannot be found</body></html>"

        converter = KSConverter()
        with pytest.raises(KSConverterError) as exc_info:
            converter.fetch_section("79-9999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(KSConverter, "_get")
    def test_get_article_section_numbers(self, mock_get):
        """Get list of section numbers from article index."""
        mock_get.return_value = SAMPLE_ARTICLE_INDEX_HTML

        converter = KSConverter()
        sections = converter.get_article_section_numbers(79, 32)

        assert len(sections) >= 3
        assert "79-3201" in sections
        assert "79-3220" in sections

    @patch.object(KSConverter, "_get")
    def test_iter_article(self, mock_get):
        """Iterate over sections in an article."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_ARTICLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = KSConverter()
        sections = list(converter.iter_article(79, 32))

        assert len(sections) >= 3
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(KSConverter, "_get")
    def test_fetch_ks_section(self, mock_get):
        """Test fetch_ks_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_ks_section("79-3201")

        assert section is not None
        assert section.citation.section == "KS-79-3201"

    @patch.object(KSConverter, "_get")
    def test_download_ks_article(self, mock_get):
        """Test download_ks_article function."""
        mock_get.side_effect = [
            SAMPLE_ARTICLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_ks_article(79, 32)

        assert len(sections) >= 3
        assert all(isinstance(s, Section) for s in sections)


class TestKSConverterIntegration:
    """Integration tests that hit real kslegislature.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_title_section(self):
        """Fetch Kansas Income Tax section 79-3201."""
        converter = KSConverter()
        section = converter.fetch_section("79-3201")

        assert section is not None
        assert section.citation.section == "KS-79-3201"
        assert "title" in section.section_title.lower() or "kansas income tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_requirements_section(self):
        """Fetch Kansas Income Tax section 79-3220."""
        converter = KSConverter()
        section = converter.fetch_section("79-3220")

        assert section is not None
        assert section.citation.section == "KS-79-3220"
        assert "return" in section.text.lower() or "individual" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_article_32_sections(self):
        """Get list of sections in Article 32 (Income Tax)."""
        converter = KSConverter()
        sections = converter.get_article_section_numbers(79, 32)

        assert len(sections) > 0
        assert all(s.startswith("79-32") for s in sections)
        assert "79-3201" in sections  # Title section
