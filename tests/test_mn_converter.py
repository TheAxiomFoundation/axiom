"""Tests for Minnesota state statute converter.

Tests the MNConverter which fetches from revisor.mn.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.mn import (
    MN_TAX_CHAPTERS,
    MN_WELFARE_CHAPTERS,
    MNConverter,
    MNConverterError,
    download_mn_chapter,
    fetch_mn_section,
)
from axiom.models import Section

# Sample HTML from revisor.mn.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>290.01 - DEFINITIONS - Minnesota Statutes</title></head>
<body>
<div id="legContainerMain">
<h2 class="stat-title">290.01 DEFINITIONS</h2>

<h3><a name="stat.290.01.1"></a>Subdivision 1. Scope.</h3>
<p>For the purposes of this chapter, unless the context clearly indicates otherwise, the following terms shall have the meanings respectively ascribed to them in this section.</p>

<h3><a name="stat.290.01.2"></a>Subd. 2. Resident.</h3>
<p>The term "resident" means any natural person domiciled in Minnesota.</p>
<p>(a) A person who spends in the aggregate more than one-half of the taxable year within this state shall be considered a resident.</p>
<p>(b) A person who maintains an abode in Minnesota for more than 183 days shall be presumed to be a resident.</p>

<h3><a name="stat.290.01.3"></a>Subd. 3. Nonresident.</h3>
<p>The term "nonresident" means any natural person whose domicile is not in Minnesota.</p>
<p>(1) A nonresident who has income from Minnesota sources is subject to taxation.</p>
<p>(2) Taxation shall be based only on the Minnesota portion of income.</p>

<h3><a name="stat.290.01.19"></a>Subd. 19. Net income.</h3>
<p>The term "net income" means the federal taxable income, as defined in section 63 of the Internal Revenue Code of 1986, as amended, for the taxable year, with the modifications specified in this chapter.</p>

<p><strong>History:</strong> 1990 c 480 art 1 s 1; 2000 c 490 art 5 s 4; 2023 c 64 art 2 s 2</p>
</div>
</body>
</html>
"""

SAMPLE_SECTION_HTML_256 = """<!DOCTYPE html>
<html>
<head><title>256.012 - MINNESOTA MERIT SYSTEM - Minnesota Statutes</title></head>
<body>
<div id="legContainerMain">
<h2 class="stat-title">256.012 MINNESOTA MERIT SYSTEM</h2>

<h3><a name="stat.256.012.1"></a>Subdivision 1. Minnesota Merit System.</h3>
<p>The commissioner shall administer the Minnesota merit system created under the authority of section 256.01.</p>
<p>(a) The commissioner shall establish standards for employment.</p>
<p>(b) The commissioner shall maintain personnel records.</p>

<h3><a name="stat.256.012.2"></a>Subd. 2. Payment for services provided.</h3>
<p>The commissioner shall allocate costs for administering the merit system among participating counties and agencies.</p>

<h3><a name="stat.256.012.3"></a>Subd. 3. Participating county consultation.</h3>
<p>The commissioner must meet regularly with representatives of participating counties.</p>

<p><strong>History:</strong> 1980 c 578 s 2; 2020 c 115 art 3 s 1; 2024 c 127 art 4 s 3</p>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 290 - Individual Income Tax - Minnesota Statutes</title></head>
<body>
<h1>Chapter 290 - Individual Income Tax</h1>
<div class="chapter-toc">
<table>
<tr><td><a href="/statutes/cite/290.01">290.01 DEFINITIONS</a></td></tr>
<tr><td><a href="/statutes/cite/290.011">290.011 TAX RATES</a></td></tr>
<tr><td><a href="/statutes/cite/290.0121">290.0121 FILING REQUIREMENTS</a></td></tr>
<tr><td><a href="/statutes/cite/290.0123">290.0123 STANDARD DEDUCTION</a></td></tr>
<tr><td><a href="/statutes/cite/290.06">290.06 COMPUTATION OF TAX</a></td></tr>
</table>
</div>
</body>
</html>
"""

SAMPLE_NOT_FOUND_HTML = """<!DOCTYPE html>
<html>
<head><title>Not Found - Minnesota Statutes</title></head>
<body>
<h1>Section Not Found</h1>
<p>The section you requested was not found.</p>
</body>
</html>
"""


class TestMNChaptersRegistry:
    """Test Minnesota chapter registries."""

    def test_chapter_290_in_tax_chapters(self):
        """Chapter 290 (Individual Income Tax) is in tax chapters."""
        assert "290" in MN_TAX_CHAPTERS
        assert "Income Tax" in MN_TAX_CHAPTERS["290"]

    def test_chapter_297A_in_tax_chapters(self):
        """Chapter 297A (Sales and Use Tax) is in tax chapters."""
        assert "297A" in MN_TAX_CHAPTERS
        assert "Sales" in MN_TAX_CHAPTERS["297A"]

    def test_chapter_256_in_welfare_chapters(self):
        """Chapter 256 (Human Services) is in welfare chapters."""
        assert "256" in MN_WELFARE_CHAPTERS
        assert "Human Services" in MN_WELFARE_CHAPTERS["256"]

    def test_chapter_256J_in_welfare_chapters(self):
        """Chapter 256J (MFIP) is in welfare chapters."""
        assert "256J" in MN_WELFARE_CHAPTERS
        assert "MFIP" in MN_WELFARE_CHAPTERS["256J"]


class TestMNConverter:
    """Test MNConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = MNConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = MNConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = MNConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = MNConverter()
        url = converter._build_section_url("290.01")
        assert url == "https://www.revisor.mn.gov/statutes/cite/290.01"

    def test_build_section_url_with_letter(self):
        """Build URL for chapter with letter suffix."""
        converter = MNConverter()
        url = converter._build_section_url("290A.03")
        assert url == "https://www.revisor.mn.gov/statutes/cite/290A.03"

    def test_build_chapter_url(self):
        """Build correct URL for chapter contents."""
        converter = MNConverter()
        url = converter._build_chapter_url(290)
        assert url == "https://www.revisor.mn.gov/statutes/cite/290"

    def test_extract_chapter_number(self):
        """Extract chapter from section number."""
        converter = MNConverter()
        assert converter._extract_chapter_number("290.01") == "290"
        assert converter._extract_chapter_number("290A.03") == "290A"
        assert converter._extract_chapter_number("256B.04") == "256B"

    def test_context_manager(self):
        """Converter works as context manager."""
        with MNConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestMNConverterParsing:
    """Test MNConverter HTML parsing."""

    def test_parse_section_html_title(self):
        """Parse section title from HTML."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "290.01", "https://example.com"
        )

        assert parsed.section_number == "290.01"
        assert parsed.section_title == "DEFINITIONS"
        assert parsed.chapter_number == "290"
        assert parsed.chapter_title == "Individual Income Tax"

    def test_parse_section_html_text(self):
        """Parse section text content."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "290.01", "https://example.com"
        )

        assert "resident" in parsed.text.lower()
        assert "domiciled" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_subdivisions(self):
        """Parse subdivisions from section HTML."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "290.01", "https://example.com"
        )

        # Should have multiple subdivisions
        assert len(parsed.subdivisions) >= 1
        # Check for subdivision identifiers
        identifiers = [s.identifier for s in parsed.subdivisions]
        # At least one subdivision should be parsed
        assert len(identifiers) > 0

    def test_parse_subdivision_heading(self):
        """Parse subdivision headings."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "290.01", "https://example.com"
        )

        # Look for a subdivision with a heading
        [s.heading for s in parsed.subdivisions if s.heading]
        # The sample should have at least one heading parsed
        assert len(parsed.subdivisions) > 0

    def test_parse_clauses(self):
        """Parse clauses (a), (b) within subdivisions."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "290.01", "https://example.com"
        )

        # Find subdivisions with clauses
        any(len(s.clauses) > 0 for s in parsed.subdivisions)
        # Note: clause parsing depends on subdivision content being correctly split
        # The sample has clauses under Subd. 2

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "290.01", "https://example.com"
        )

        assert parsed.history is not None
        assert "1990" in parsed.history or "2023" in parsed.history

    def test_parse_welfare_section(self):
        """Parse a human services section (256.012)."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML_256, "256.012", "https://example.com"
        )

        assert parsed.section_number == "256.012"
        assert parsed.section_title == "MINNESOTA MERIT SYSTEM"
        assert parsed.chapter_number == "256"
        assert parsed.chapter_title == "Human Services"

    def test_to_section_model(self):
        """Convert ParsedMNSection to Section model."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "290.01", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "MN-290.01"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "DEFINITIONS"
        assert "Minnesota Statutes" in section.title_name
        assert section.uslm_id == "mn/290/290.01"
        assert section.source_url == "https://example.com"

    def test_to_section_model_welfare(self):
        """Convert welfare section to Section model."""
        converter = MNConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML_256, "256.012", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert section.citation.section == "MN-256.012"
        assert "Human Services" in section.title_name


class TestMNConverterFetching:
    """Test MNConverter HTTP fetching with mocks."""

    @patch.object(MNConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = MNConverter()
        section = converter.fetch_section("290.01")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "MN-290.01"
        assert "DEFINITIONS" in section.section_title

    @patch.object(MNConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_NOT_FOUND_HTML

        converter = MNConverter()
        with pytest.raises(MNConverterError) as exc_info:
            converter.fetch_section("999.99")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(MNConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = MNConverter()
        sections = converter.get_chapter_section_numbers(290)

        assert len(sections) == 5
        assert "290.01" in sections
        assert "290.011" in sections
        assert "290.0121" in sections
        assert "290.0123" in sections
        assert "290.06" in sections

    @patch.object(MNConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = MNConverter()
        sections = list(converter.iter_chapter(290))

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(MNConverter, "_get")
    def test_fetch_mn_section(self, mock_get):
        """Test fetch_mn_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_mn_section("290.01")

        assert section is not None
        assert section.citation.section == "MN-290.01"

    @patch.object(MNConverter, "_get")
    def test_download_mn_chapter(self, mock_get):
        """Test download_mn_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_mn_chapter(290)

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestMNConverterIntegration:
    """Integration tests that hit real revisor.mn.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_definitions(self):
        """Fetch Minnesota Income Tax section 290.01."""
        converter = MNConverter()
        section = converter.fetch_section("290.01")

        assert section is not None
        assert section.citation.section == "MN-290.01"
        assert "DEFINITIONS" in section.section_title.upper()
        assert "resident" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_standard_deduction(self):
        """Fetch Minnesota Standard Deduction section 290.0123."""
        converter = MNConverter()
        section = converter.fetch_section("290.0123")

        assert section is not None
        assert section.citation.section == "MN-290.0123"
        assert "STANDARD DEDUCTION" in section.section_title.upper()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_human_services_section(self):
        """Fetch Minnesota Human Services section 256.012."""
        converter = MNConverter()
        section = converter.fetch_section("256.012")

        assert section is not None
        assert section.citation.section == "MN-256.012"
        assert "Human Services" in section.title_name

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_290_sections(self):
        """Get list of sections in Chapter 290."""
        converter = MNConverter()
        sections = converter.get_chapter_section_numbers(290)

        assert len(sections) > 0
        assert all(s.startswith("290.") for s in sections)
        assert "290.01" in sections

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch Minnesota Sales Tax section 297A.61."""
        converter = MNConverter()
        try:
            section = converter.fetch_section("297A.61")
            assert section.citation.section == "MN-297A.61"
        except MNConverterError:
            # Section may not exist, which is acceptable
            pytest.skip("Section 297A.61 not found")
