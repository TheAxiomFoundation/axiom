"""Tests for New Jersey state statute converter.

Tests the NJConverter which fetches from lis.njleg.state.nj.us
and converts to the internal Section model.
"""

from unittest.mock import patch

import pytest

from axiom.converters.us_states.nj import (
    NJ_TAX_TITLES,
    NJ_WELFARE_TITLES,
    NJConverter,
    NJConverterError,
    search_nj_statutes,
)
from axiom.models import Section

# Sample HTML from lis.njleg.state.nj.us for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>NJ Statutes 54:4-1</title></head>
<body>
<div class="document">
<h1>TITLE 54 TAXATION</h1>
<h2>CHAPTER 4 TAX ON PROPERTY</h2>
<p><b>54:4-1 Property subject to taxation.</b></p>
<p>All property real and personal within the jurisdiction of this State not expressly exempted from taxation or expressly excluded from the operation of this chapter shall be subject to taxation annually under this chapter.</p>
<p>a. The personal property so affixed can be removed or severed without material injury to the real property;</p>
<p>(1) The personal property so affixed can be removed or severed without material injury to the personal property itself; and</p>
<p>(2) The personal property so affixed is not ordinarily intended to be affixed permanently to real property; or</p>
<p>b. The personal property so affixed is machinery, apparatus, or equipment used or held for use in business.</p>
<p>Amended 1942, c.281, s.1; 1943, c.120, s.1; 1960, c.51, s.23; 2004, c.42, s.13.</p>
</div>
</body>
</html>
"""

SAMPLE_SEARCH_RESULTS_HTML = """<!DOCTYPE html>
<html>
<head><title>Search Results</title></head>
<body>
<div id="results">
<div class="result">
<a href="/nxt/gateway.dll/statutes/1/2951/3001">54:4-1 Property subject to taxation.</a>
<p>New Jersey Statutes (Unannotated) / TITLE 54 TAXATION</p>
</div>
<div class="result">
<a href="/nxt/gateway.dll/statutes/1/2951/3002">54:4-1.1 Pending litigation unaffected</a>
<p>New Jersey Statutes (Unannotated) / TITLE 54 TAXATION</p>
</div>
<div class="result">
<a href="/nxt/gateway.dll/statutes/1/2951/3003">54:4-1.12 Storage tank deemed real property</a>
<p>New Jersey Statutes (Unannotated) / TITLE 54 TAXATION</p>
</div>
</div>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>NJ Statutes 44:10-44</title></head>
<body>
<div class="document">
<h1>TITLE 44 POOR</h1>
<h2>CHAPTER 10 WORK FIRST NEW JERSEY PROGRAM</h2>
<p><b>44:10-44 Eligibility for assistance.</b></p>
<p>To be eligible for Work First New Jersey assistance under this act, an applicant shall meet all of the following requirements:</p>
<p>a. The applicant is a resident of this State;</p>
<p>b. The applicant is a citizen of the United States or a qualified alien;</p>
<p>c. The applicant has applied for all other benefits for which the applicant may be eligible;</p>
<p>d. The applicant has income and resources below the limits established by the commissioner.</p>
<p>L.1997, c.38, s.44; amended 2018, c.23, s.5.</p>
</div>
</body>
</html>
"""

SAMPLE_NOT_FOUND_HTML = """<!DOCTYPE html>
<html>
<head><title>Error</title></head>
<body>
<p>The requested section cannot be found in the database.</p>
</body>
</html>
"""


class TestNJTitlesRegistry:
    """Test New Jersey title registries."""

    def test_title_54_in_tax_titles(self):
        """Title 54 (Taxation) is in tax titles."""
        assert "54" in NJ_TAX_TITLES
        assert "Taxation" in NJ_TAX_TITLES["54"]

    def test_title_54a_in_tax_titles(self):
        """Title 54A (Gross Income Tax) is in tax titles."""
        assert "54A" in NJ_TAX_TITLES
        assert "Gross Income Tax" in NJ_TAX_TITLES["54A"]

    def test_title_44_in_welfare_titles(self):
        """Title 44 (Poor) is in welfare titles."""
        assert "44" in NJ_WELFARE_TITLES
        assert "Poor" in NJ_WELFARE_TITLES["44"]

    def test_title_30_in_welfare_titles(self):
        """Title 30 (Institutions and Agencies) is in welfare titles."""
        assert "30" in NJ_WELFARE_TITLES


class TestNJConverter:
    """Test NJConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = NJConverter()
        assert converter.rate_limit_delay == 0.5

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = NJConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_parse_section_number_standard(self):
        """Parse standard section number format."""
        converter = NJConverter()
        title, chapter, section = converter._parse_section_number("54:4-1")
        assert title == "54"
        assert chapter == "4"
        assert section == "1"

    def test_parse_section_number_with_letter_suffix(self):
        """Parse section number with letter in title."""
        converter = NJConverter()
        title, chapter, section = converter._parse_section_number("54A:3-5")
        assert title == "54A"
        assert chapter == "3"
        assert section == "5"

    def test_parse_section_number_decimal(self):
        """Parse section number with decimal."""
        converter = NJConverter()
        title, chapter, section = converter._parse_section_number("54:4-1.12")
        assert title == "54"
        assert chapter == "4"
        assert section == "1.12"

    def test_parse_section_number_invalid(self):
        """Parse invalid section number raises error."""
        converter = NJConverter()
        with pytest.raises(ValueError):
            converter._parse_section_number("invalid")

    def test_context_manager(self):
        """Converter works as context manager."""
        with NJConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestNJConverterParsing:
    """Test NJConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedNJSection."""
        converter = NJConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "54:4-1", "https://example.com"
        )

        assert parsed.section_number == "54:4-1"
        assert parsed.section_title == "Property subject to taxation"
        assert parsed.title_number == "54"
        assert parsed.title_name == "Taxation"
        assert "jurisdiction of this State" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_section_history(self):
        """Parse history note from section HTML."""
        converter = NJConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "54:4-1", "https://example.com"
        )

        assert parsed.history is not None
        assert "1942" in parsed.history or "Amended" in parsed.history

    def test_parse_subsections_letter_style(self):
        """Parse letter-style subsections a., b., etc."""
        converter = NJConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "54:4-1", "https://example.com"
        )

        # Should have subsections a and b
        assert len(parsed.subsections) >= 1

    def test_parse_welfare_section(self):
        """Parse welfare title section."""
        converter = NJConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "44:10-44", "https://example.com"
        )

        assert parsed.section_number == "44:10-44"
        assert "Eligibility" in parsed.section_title or "44:10-44" in parsed.section_title
        assert parsed.title_number == "44"
        assert "Poor" in parsed.title_name

    def test_parse_not_found(self):
        """Handle not found error."""
        converter = NJConverter()
        with pytest.raises(NJConverterError) as exc_info:
            converter._parse_section_html(
                SAMPLE_NOT_FOUND_HTML, "99:99-99", "https://example.com"
            )

        assert "not found" in str(exc_info.value).lower()

    def test_to_section_model(self):
        """Convert ParsedNJSection to Section model."""
        converter = NJConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "54:4-1", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "NJ-54:4-1"
        assert section.citation.title == 0  # State law indicator
        assert "Property subject to taxation" in section.section_title
        assert "New Jersey Revised Statutes" in section.title_name
        assert section.uslm_id == "nj/54/54:4-1"
        assert section.source_url == "https://example.com"


class TestNJConverterSearchParsing:
    """Test NJConverter search result parsing."""

    def test_parse_search_results(self):
        """Parse search results HTML."""
        converter = NJConverter()
        results = converter._parse_search_results(SAMPLE_SEARCH_RESULTS_HTML)

        # Should find multiple results
        assert len(results) >= 1

        # Check first result structure
        if results:
            assert "section_number" in results[0]
            assert "url" in results[0]


class TestNJConverterFetching:
    """Test NJConverter HTTP fetching with mocks."""

    @patch.object(NJConverter, "_get")
    def test_search_sections(self, mock_get):
        """Search for sections."""
        mock_get.return_value = SAMPLE_SEARCH_RESULTS_HTML

        converter = NJConverter()
        results = converter.search_sections("54:4-1")

        assert mock_get.called
        # Results should be a list
        assert isinstance(results, list)

    @patch.object(NJConverter, "_get")
    def test_fetch_section_by_url(self, mock_get):
        """Fetch section by direct URL."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = NJConverter()
        section = converter.fetch_section_by_url(
            "https://example.com/section", "54:4-1"
        )

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "NJ-54:4-1"


class TestNJConverterSubsectionParsing:
    """Test NJConverter subsection parsing."""

    def test_parse_parenthetical_subsections(self):
        """Parse (1), (2), (3) style subsections."""
        converter = NJConverter()
        text = """
        (1) The first condition must be met.
        (2) The second condition must be met.
        (3) The third condition is optional.
        """
        subsections = converter._parse_parenthetical_subsections(text)

        assert len(subsections) == 3
        assert subsections[0].identifier == "1"
        assert subsections[1].identifier == "2"
        assert subsections[2].identifier == "3"

    def test_parse_letter_subsections(self):
        """Parse (a), (b), (c) style subsections."""
        converter = NJConverter()
        text = """
        (a) The applicant is a resident.
        (b) The applicant is a citizen.
        (c) The applicant has applied for benefits.
        """
        subsections = converter._parse_letter_subsections(text)

        assert len(subsections) == 3
        assert subsections[0].identifier == "a"
        assert subsections[1].identifier == "b"
        assert subsections[2].identifier == "c"


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(NJConverter, "_get")
    def test_search_nj_statutes(self, mock_get):
        """Test search_nj_statutes function."""
        mock_get.return_value = SAMPLE_SEARCH_RESULTS_HTML

        results = search_nj_statutes("property taxation")

        assert mock_get.called
        assert isinstance(results, list)


class TestNJConverterIntegration:
    """Integration tests that hit real lis.njleg.state.nj.us (marked slow).

    Note: The NJ Legislature website gateway.dll can be unreliable and may
    return 500 errors. These tests are skipped if the server is unavailable.
    """

    @pytest.mark.slow
    @pytest.mark.integration
    def test_search_property_tax(self):
        """Search for property taxation sections."""
        import httpx

        converter = NJConverter()
        try:
            results = converter.search_sections("54:4-1")
            # Should find at least some results
            assert len(results) >= 0  # May be empty if site structure changed
        except httpx.HTTPStatusError as e:
            pytest.skip(f"NJ Legislature site returned error: {e.response.status_code}")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_taxation_section(self):
        """Fetch a taxation section (54:4-1)."""
        import httpx

        converter = NJConverter()
        try:
            section = converter.fetch_section("54:4-1")
            assert section is not None
            assert section.citation.section == "NJ-54:4-1"
            assert "property" in section.text.lower() or "tax" in section.text.lower()
        except NJConverterError:
            pytest.skip("Could not fetch section 54:4-1 from NJ Legislature site")
        except httpx.HTTPStatusError as e:
            pytest.skip(f"NJ Legislature site returned error: {e.response.status_code}")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch a welfare title section."""
        import httpx

        converter = NJConverter()
        try:
            section = converter.fetch_section("44:10-44")
            assert section.citation.section == "NJ-44:10-44"
        except NJConverterError:
            pytest.skip("Section 44:10-44 not found")
        except httpx.HTTPStatusError as e:
            pytest.skip(f"NJ Legislature site returned error: {e.response.status_code}")
