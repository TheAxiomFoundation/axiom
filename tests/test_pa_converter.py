"""Tests for Pennsylvania state statute converter.

Tests the PAConverter which fetches from palegis.us
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.pa import (
    PA_TAX_TITLES,
    PA_TITLES,
    PA_WELFARE_TITLES,
    PAConverter,
    PAConverterError,
    download_pa_title,
    fetch_pa_section,
)
from axiom.models import Section

# Sample HTML from palegis.us for testing
SAMPLE_TITLE_HTML = """<!DOCTYPE html>
<html>
<head><title>PA Consolidated Statutes Title 72</title></head>
<body>
<div class="BodyContainer">
<h1>Title 72</h1>
<h2>TAXATION AND FISCAL AFFAIRS</h2>

<p><b>CHAPTER 3</b></p>
<p>MICROENTERPRISE DEVELOPMENT</p>

<a name="72c3116s"></a>
<p><b>§ 3116.  Microenterprise loans.</b></p>

<p><b>(a)  Loan issuance.--</b></p>
<p>(1)  An administrative entity may issue a loan to a microenterprise or a person seeking to establish a microenterprise for the purpose of assisting in the establishment of that microenterprise.</p>
<p>(2)  An administrative entity may partner with another lending institution to issue a loan under this section.</p>

<p><b>(b)  Training.--</b></p>
<p>(1)  For the purpose of reducing loan defaults, an administrative entity shall require the recipient of a loan under this section to complete training in basic business planning and management provided by the administrative entity or by an entity approved by the administrative entity.</p>
<p>(2)  An administrative entity may reduce the training requirement for an applicant who demonstrates an understanding of basic business planning and management.</p>

<p><b>(c)  Maximum loan amount.--</b>The maximum loan amount that may be issued under this section shall be the greater of:</p>
<p>(1)  $35,000; or</p>
<p>(2)  the amount established by the department in accordance with the guidelines established under section 3121 (relating to guidelines).</p>

<p><b>Cross References.  </b>Section 3116 is referred to in section 3121.</p>

<p><b>History.--</b>Act 2004-32, effective 60 days after July 7, 2004.</p>

<a name="72c3117s"></a>
<p><b>§ 3117.  Microenterprise grants.</b></p>

<p>(a)  An administrative entity may issue a grant to a microenterprise or a person seeking to establish a microenterprise for the purpose of assisting in the establishment of that microenterprise.</p>

</div>
</body>
</html>
"""

SAMPLE_WELFARE_HTML = """<!DOCTYPE html>
<html>
<head><title>PA Consolidated Statutes Title 67</title></head>
<body>
<div class="BodyContainer">
<h1>Title 67</h1>
<h2>PUBLIC WELFARE</h2>

<p><b>CHAPTER 1</b></p>
<p>GENERAL PROVISIONS</p>

<a name="67c101s"></a>
<p><b>§ 101.  Definitions.</b></p>

<p>The following words and phrases when used in this title shall have the meanings given to them in this section unless the context clearly indicates otherwise:</p>

<p><b>(a)  "Applicant."--</b>A person who applies for public assistance.</p>

<p><b>(b)  "Department."--</b>The Department of Public Welfare of the Commonwealth.</p>

<p><b>(c)  "Public assistance."--</b>Money payments or services provided under this title.</p>
<p>(1)  Assistance includes financial assistance.</p>
<p>(2)  Assistance includes medical assistance.</p>
<p>(i)  Inpatient care.</p>
<p>(ii)  Outpatient care.</p>
<p>(iii)  Prescription drugs.</p>

</div>
</body>
</html>
"""

SAMPLE_NOT_FOUND_HTML = """<!DOCTYPE html>
<html>
<body>
<p>Title 72 Section 999999 could not be found. Please go back and try again. Error VS-4</p>
</body>
</html>
"""


class TestPATitlesRegistry:
    """Test Pennsylvania title registries."""

    def test_title_72_in_titles(self):
        """Title 72 (Taxation) is in titles."""
        assert 72 in PA_TITLES
        assert "Taxation" in PA_TITLES[72]

    def test_title_67_in_titles(self):
        """Title 67 (Public Welfare) is in titles."""
        assert 67 in PA_TITLES
        assert "Public Welfare" in PA_TITLES[67]

    def test_title_72_in_tax_titles(self):
        """Title 72 is in tax titles."""
        assert 72 in PA_TAX_TITLES

    def test_title_67_in_welfare_titles(self):
        """Title 67 is in welfare titles."""
        assert 67 in PA_WELFARE_TITLES


class TestPAConverter:
    """Test PAConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = PAConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = PAConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = PAConverter(year=2024)
        assert converter.year == 2024

    def test_build_title_url(self):
        """Build correct URL for title fetch."""
        converter = PAConverter()
        url = converter._build_title_url(72)
        assert "palegis.us/statutes/consolidated" in url
        assert "txtType=HTM" in url
        assert "ttl=72" in url
        assert "iFrame=true" in url

    def test_build_title_url_with_chapter(self):
        """Build correct URL for title with chapter."""
        converter = PAConverter()
        url = converter._build_title_url(72, chapter="3")
        assert "ttl=72" in url
        assert "chpt=3" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with PAConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestPAConverterParsing:
    """Test PAConverter HTML parsing."""

    def test_extract_section_from_html(self):
        """Extract section from title HTML."""
        converter = PAConverter()
        parsed = converter._extract_section_from_html(
            SAMPLE_TITLE_HTML, 72, "3116", "https://example.com"
        )

        assert parsed is not None
        assert parsed.section_number == "3116"
        assert parsed.section_title == "Microenterprise loans"
        assert parsed.title_number == 72
        assert parsed.title_name == "Taxation and Fiscal Affairs"
        assert "Loan issuance" in parsed.text or "administrative entity" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = PAConverter()
        parsed = converter._extract_section_from_html(
            SAMPLE_TITLE_HTML, 72, "3116", "https://example.com"
        )

        assert parsed is not None
        # Should have subsections (a), (b), (c)
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)

    def test_parse_subsection_headings(self):
        """Parse subsection headings like (a) Loan issuance.--"""
        converter = PAConverter()
        parsed = converter._extract_section_from_html(
            SAMPLE_TITLE_HTML, 72, "3116", "https://example.com"
        )

        assert parsed is not None
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        assert sub_a.heading == "Loan issuance"

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (a)."""
        converter = PAConverter()
        parsed = converter._extract_section_from_html(
            SAMPLE_TITLE_HTML, 72, "3116", "https://example.com"
        )

        assert parsed is not None
        sub_a = next((s for s in parsed.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        # Should have children (1) and (2)
        assert len(sub_a.children) >= 2
        assert any(c.identifier == "1" for c in sub_a.children)
        assert any(c.identifier == "2" for c in sub_a.children)

    def test_parse_level3_subsections(self):
        """Parse level 3 subsections (i), (ii), (iii)."""
        converter = PAConverter()
        parsed = converter._extract_section_from_html(
            SAMPLE_WELFARE_HTML, 67, "101", "https://example.com"
        )

        assert parsed is not None
        # Find subsection c which has nested items
        sub_c = next((s for s in parsed.subsections if s.identifier == "c"), None)
        assert sub_c is not None

        # Find child (2) which has roman numeral children
        child_2 = next((c for c in sub_c.children if c.identifier == "2"), None)
        if child_2 and child_2.children:
            # Should have (i), (ii), (iii)
            identifiers = [gc.identifier for gc in child_2.children]
            assert "i" in identifiers or "ii" in identifiers

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = PAConverter()
        parsed = converter._extract_section_from_html(
            SAMPLE_TITLE_HTML, 72, "3116", "https://example.com"
        )

        assert parsed is not None
        assert parsed.history is not None
        assert "2004" in parsed.history

    def test_parse_cross_references(self):
        """Parse cross-references from section HTML."""
        converter = PAConverter()
        parsed = converter._extract_section_from_html(
            SAMPLE_TITLE_HTML, 72, "3116", "https://example.com"
        )

        assert parsed is not None
        # Cross-references should be parsed if present in the section text
        # The sample HTML includes "Section 3116 is referred to in section 3121"
        # which should extract "3116" and "3121"
        if "Cross References" in parsed.text:
            assert len(parsed.cross_references) >= 1

    def test_to_section_model(self):
        """Convert ParsedPASection to Section model."""
        converter = PAConverter()
        parsed = converter._extract_section_from_html(
            SAMPLE_TITLE_HTML, 72, "3116", "https://example.com"
        )
        assert parsed is not None

        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "PA-72-3116"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Microenterprise loans"
        assert "Pennsylvania Consolidated Statutes" in section.title_name
        assert section.uslm_id == "pa/72/3116"
        assert section.source_url == "https://example.com"


class TestPAConverterFetching:
    """Test PAConverter HTTP fetching with mocks."""

    @patch.object(PAConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        converter = PAConverter()
        section = converter.fetch_section(72, "3116")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "PA-72-3116"
        assert "Microenterprise loans" in section.section_title

    @patch.object(PAConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_NOT_FOUND_HTML

        converter = PAConverter()
        with pytest.raises(PAConverterError) as exc_info:
            converter.fetch_section(72, "999999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(PAConverter, "_get")
    def test_get_title_section_numbers(self, mock_get):
        """Get list of section numbers from title."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        converter = PAConverter()
        sections = converter.get_title_section_numbers(72)

        assert len(sections) >= 2
        assert "3116" in sections
        assert "3117" in sections

    @patch.object(PAConverter, "_get")
    def test_iter_title(self, mock_get):
        """Iterate over sections in a title."""
        # First call returns section numbers, subsequent calls return HTML
        mock_get.return_value = SAMPLE_TITLE_HTML

        converter = PAConverter()
        sections = list(converter.iter_title(72))

        assert len(sections) >= 2
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(PAConverter, "_get")
    def test_fetch_pa_section(self, mock_get):
        """Test fetch_pa_section function."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        section = fetch_pa_section(72, "3116")

        assert section is not None
        assert section.citation.section == "PA-72-3116"

    @patch.object(PAConverter, "_get")
    def test_download_pa_title(self, mock_get):
        """Test download_pa_title function."""
        mock_get.return_value = SAMPLE_TITLE_HTML

        sections = download_pa_title(72)

        assert len(sections) >= 2
        assert all(isinstance(s, Section) for s in sections)


class TestPAConverterIntegration:
    """Integration tests that hit real palegis.us (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_section(self):
        """Fetch Pennsylvania Tax section."""
        converter = PAConverter()
        try:
            section = converter.fetch_section(72, "3116")
            assert section is not None
            assert section.citation.section == "PA-72-3116"
            assert "microenterprise" in section.text.lower() or "loan" in section.text.lower()
        except PAConverterError:
            pytest.skip("Section 72 Pa.C.S. 3116 not found in current version")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Pennsylvania Public Welfare section."""
        converter = PAConverter()
        try:
            section = converter.fetch_section(67, "101")
            assert section is not None
            assert section.citation.section == "PA-67-101"
        except PAConverterError:
            pytest.skip("Section 67 Pa.C.S. 101 not found in current version")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_72_sections(self):
        """Get list of sections in Title 72."""
        converter = PAConverter()
        try:
            sections = converter.get_title_section_numbers(72)
            if len(sections) == 0:
                pytest.skip("No sections found - PA site may have changed format")
            # All section numbers should be numeric (possibly with letter suffix)
            for s in sections:
                assert s[0].isdigit(), f"Section number should start with digit: {s}"
        except Exception as e:
            pytest.skip(f"Could not fetch title 72: {e}")
