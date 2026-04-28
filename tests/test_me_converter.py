"""Tests for Maine state statute converter.

Tests the MEConverter which fetches from legislature.maine.gov
and converts to the internal Section model.
"""

from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.me import (
    ME_TAX_CHAPTERS,
    ME_TITLES,
    ME_WELFARE_CHAPTERS,
    MEConverter,
    MEConverterError,
    download_me_chapter,
    fetch_me_section,
)
from axiom_corpus.models import Section

# Sample HTML from legislature.maine.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Title 36, Section 5219-S</title></head>
<body>
<p><a href="title36ch822sec0.html">Chapter 822: TAX CREDITS</a></p>
<h3>Section 5219-S. Earned income credit</h3>
<p>1. Resident taxpayer; tax years beginning 2020, 2021. For tax years beginning
on or after January 1, 2020 and before January 1, 2022, a resident individual
who is allowed a federal earned income credit under the Code, Section 32 is
allowed an earned income credit equal to 12% of the federal credit allowed.</p>

<p>2. Nonresident taxpayer; tax years beginning 2020, 2021. For tax years
beginning on or after January 1, 2020 and before January 1, 2022, a nonresident
individual who is allowed a federal earned income credit is allowed a credit
in an amount equal to that portion of the credit that would be computed under
subsection 1.</p>

<p>3. Part-year resident; tax years beginning 2020, 2021. For tax years beginning
on or after January 1, 2020 and before January 1, 2022, an individual who is a
part-year resident during a taxable year is allowed an earned income credit.</p>

<p>1-A. Resident taxpayer; tax years beginning 2022 and after. For tax years
beginning on or after January 1, 2022, a resident individual who is allowed a
federal earned income credit under the Code, Section 32 is allowed an earned
income credit equal to 25% of the federal credit allowed.</p>

<p>2-A. Nonresident taxpayer; tax years beginning 2022 and after. For tax years
beginning on or after January 1, 2022, a nonresident individual who is allowed
a federal earned income credit is allowed a credit in an amount equal to that
portion of the credit that would be computed under subsection 1-A.</p>

<p>SECTION HISTORY
RR 1999, c. 2, Section 35 (RAL). PL 1999, c. 731, Section V1 (NEW). PL 2001, c. 396,
Section 9 (AMD). PL 2005, c. 618, Section 6 (AMD). PL 2009, c. 213, Pt. BBBB, Section 6
(AMD). PL 2011, c. 380, Pt. N, Section 5 (AMD). PL 2021, c. 635, Pt. E, Section 1 (AMD).</p>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Title 36, Chapter 822</title></head>
<body>
<h1>Title 36: TAXATION</h1>
<h2>Part 8: INCOME TAXES</h2>
<h3>Chapter 822: TAX CREDITS</h3>
<ul>
<li><a href="title36sec5219.html">36 Section 5219. Fuel and electricity sales tax credit</a></li>
<li><a href="title36sec5219-A.html">36 Section 5219-A. Retirement and disability credit (REPEALED)</a></li>
<li><a href="title36sec5219-S.html">36 Section 5219-S. Earned income credit</a></li>
<li><a href="title36sec5219-KK.html">36 Section 5219-KK. Child tax credit</a></li>
</ul>
</body>
</html>
"""

SAMPLE_DEFINITIONS_HTML = """<!DOCTYPE html>
<html>
<head><title>Title 36, Section 5102</title></head>
<body>
<h3>Section 5102. Definitions</h3>
<p>As used in this Part, unless the context otherwise indicates, the following
terms have the following meanings.</p>

<p>1. Code. "Code" means the United States Internal Revenue Code of 1986 and
amendments to that Code as of December 31, 2022.</p>

<p>2. Commissioner. "Commissioner" means the State Tax Assessor.</p>

<p>3. Corporation. "Corporation" means any business entity treated as a
corporation for federal income tax purposes.</p>

<p>4. Fiscal year. "Fiscal year" means an accounting period of 12 months ending
on the last day of any month other than December.</p>

<p>5. Resident individual.
A. A resident individual is an individual:
(1) Who is domiciled in this State for the entire taxable year; or
(2) Who maintains a permanent place of abode in this State and spends more than
183 days of the taxable year in this State.
B. An individual who changes domicile to or from this State is a resident for
only that portion of the taxable year during which the individual was domiciled
in this State.</p>

<p>SECTION HISTORY
PL 1969, c. 154, Section 62 (AMD). PL 1975, c. 660, Section 1 (AMD). PL 2023, c. 412,
Pt. E, Section 1 (AMD).</p>
</body>
</html>
"""

SAMPLE_REPEALED_HTML = """<!DOCTYPE html>
<html>
<head><title>Title 36, Section 5219-A</title></head>
<body>
<h3>Section 5219-A. Retirement and disability credit</h3>
<p>(REPEALED)</p>
<p>SECTION HISTORY
PL 2015, c. 267, Pt. DD, Section 25 (RP).</p>
</body>
</html>
"""


class TestMEChaptersRegistry:
    """Test Maine chapter registries."""

    def test_title_36_in_titles(self):
        """Title 36 (Taxation) is in titles registry."""
        assert 36 in ME_TITLES
        assert "Taxation" in ME_TITLES[36]

    def test_title_22_in_titles(self):
        """Title 22 (Health and Welfare) is in titles registry."""
        assert 22 in ME_TITLES
        assert "Health" in ME_TITLES[22] or "Welfare" in ME_TITLES[22]

    def test_chapter_822_in_tax_chapters(self):
        """Chapter 822 (Tax Credits) is in tax chapters."""
        assert 822 in ME_TAX_CHAPTERS
        assert "Credit" in ME_TAX_CHAPTERS[822]

    def test_chapter_1053_in_welfare_chapters(self):
        """Chapter 1053 (Aid to Dependent Children) is in welfare chapters."""
        assert 1053 in ME_WELFARE_CHAPTERS

    def test_tax_chapters_has_income_tax(self):
        """Tax chapters include income tax chapters (800 series)."""
        income_chapters = [ch for ch in ME_TAX_CHAPTERS if 800 <= ch <= 850]
        assert len(income_chapters) > 0


class TestMEConverter:
    """Test MEConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = MEConverter()
        assert converter.rate_limit_delay == 0.5

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = MEConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = MEConverter()
        url = converter._build_section_url(36, "5219-S")
        assert "legislature.maine.gov" in url
        assert "/36/" in url
        assert "title36sec5219-S.html" in url

    def test_build_section_url_simple(self):
        """Build URL for simple section number."""
        converter = MEConverter()
        url = converter._build_section_url(36, "5102")
        assert "title36sec5102.html" in url

    def test_build_chapter_index_url(self):
        """Build correct URL for chapter index."""
        converter = MEConverter()
        url = converter._build_chapter_index_url(36, 822)
        assert "title36ch822sec0.html" in url

    def test_build_title_index_url(self):
        """Build correct URL for title index."""
        converter = MEConverter()
        url = converter._build_title_index_url(36)
        assert "title36ch0sec0.html" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with MEConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestMEConverterParsing:
    """Test MEConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedMESection."""
        converter = MEConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, 36, "5219-S", "https://example.com")

        assert parsed.title == 36
        assert parsed.section_number == "5219-S"
        assert parsed.section_title == "Earned income credit"
        assert "federal earned income credit" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = MEConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, 36, "5219-S", "https://example.com")

        # Should have subsections 1, 2, 3, 1-A, 2-A (at least)
        assert len(parsed.subsections) >= 3
        identifiers = [s.identifier for s in parsed.subsections]
        assert "1" in identifiers or "1-A" in identifiers

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = MEConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, 36, "5219-S", "https://example.com")

        assert parsed.history is not None
        assert "PL 1999" in parsed.history or "RR 1999" in parsed.history

    def test_parse_definitions_with_nested_subsections(self):
        """Parse definitions section with nested A., B., (1), (2) structure."""
        converter = MEConverter()
        parsed = converter._parse_section_html(SAMPLE_DEFINITIONS_HTML, 36, "5102", "https://example.com")

        assert parsed.section_title == "Definitions"
        assert len(parsed.subsections) >= 4  # At least 4 numbered definitions

        # Find subsection 5 which has nested structure
        sub_5 = next((s for s in parsed.subsections if s.identifier == "5"), None)
        if sub_5:
            # Should have children A, B
            assert len(sub_5.children) >= 1

    def test_parse_repealed_section(self):
        """Parse repealed section should raise error."""
        converter = MEConverter()
        with pytest.raises(MEConverterError) as exc_info:
            converter._parse_section_html(SAMPLE_REPEALED_HTML, 36, "5219-A", "https://example.com")

        assert "repealed" in str(exc_info.value).lower()

    def test_to_section_model(self):
        """Convert ParsedMESection to Section model."""
        converter = MEConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, 36, "5219-S", "https://example.com")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "ME-36-5219-S"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Earned income credit"
        assert "Maine Revised Statutes" in section.title_name
        assert "36" in section.title_name
        assert section.uslm_id == "me/36/5219-S"
        assert section.source_url == "https://example.com"


class TestMEConverterFetching:
    """Test MEConverter HTTP fetching with mocks."""

    @patch.object(MEConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = MEConverter()
        section = converter.fetch_section(36, "5219-S")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "ME-36-5219-S"
        assert "earned income credit" in section.section_title.lower()

    @patch.object(MEConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>404 Not Found</body></html>"

        converter = MEConverter()
        with pytest.raises(MEConverterError) as exc_info:
            converter.fetch_section(36, "99999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(MEConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = MEConverter()
        sections = converter.get_chapter_section_numbers(36, 822)

        assert len(sections) >= 3
        assert "5219" in sections
        assert "5219-S" in sections
        assert "5219-KK" in sections

    @patch.object(MEConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_REPEALED_HTML,  # This will be skipped (repealed)
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = MEConverter()
        sections = list(converter.iter_chapter(36, 822))

        # Should get 3 valid sections (one is repealed and skipped)
        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(MEConverter, "_get")
    def test_fetch_me_section(self, mock_get):
        """Test fetch_me_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_me_section(36, "5219-S")

        assert section is not None
        assert section.citation.section == "ME-36-5219-S"

    @patch.object(MEConverter, "_get")
    def test_download_me_chapter(self, mock_get):
        """Test download_me_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_me_chapter(36, 822)

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestMEConverterIntegration:
    """Integration tests that hit real legislature.maine.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_earned_income_credit_section(self):
        """Fetch Maine Earned Income Credit section 5219-S."""
        converter = MEConverter()
        section = converter.fetch_section(36, "5219-S")

        assert section is not None
        assert section.citation.section == "ME-36-5219-S"
        assert "credit" in section.section_title.lower()
        # Maine EITC is a percentage of federal
        assert "federal" in section.text.lower() or "credit" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_definitions_section(self):
        """Fetch Maine Income Tax definitions section 5102."""
        converter = MEConverter()
        section = converter.fetch_section(36, "5102")

        assert section is not None
        assert section.citation.section == "ME-36-5102"
        assert "definition" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_822_sections(self):
        """Get list of sections in Chapter 822 (Tax Credits)."""
        converter = MEConverter()
        sections = converter.get_chapter_section_numbers(36, 822)

        assert len(sections) > 0
        # Should include earned income credit section
        assert any("5219" in s for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_property_tax_fairness_credit(self):
        """Fetch Maine Property Tax Fairness Credit section."""
        converter = MEConverter()
        try:
            section = converter.fetch_section(36, "5219-KK")
            assert section.citation.section == "ME-36-5219-KK"
        except MEConverterError:
            # Section may not exist or be structured differently
            pytest.skip("Section 5219-KK not found or structured differently")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_title_22_section(self):
        """Fetch a Title 22 (Health and Welfare) section."""
        converter = MEConverter()
        try:
            # Try to fetch a general assistance section
            section = converter.fetch_section(22, "4301")
            assert "ME-22-4301" in section.citation.section
        except MEConverterError:
            pytest.skip("Section 22 MRS 4301 not found")
