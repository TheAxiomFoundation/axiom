"""Tests for Oklahoma state statute converter.

Tests the OKConverter which fetches from OSCN (Oklahoma State Courts Network)
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.ok import (
    OK_SECTIONS,
    OK_TAX_SECTIONS,
    OK_TITLES,
    OK_WELFARE_SECTIONS,
    OKConverter,
    OKConverterError,
    download_ok_title,
    fetch_ok_section,
)
from axiom.models import Section

# Sample HTML from OSCN for testing - Tax Code definition section
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 101 - Tax Code | Oklahoma Statutes | OSCN</title></head>
<body>
<div class="document_header">
<a href="index.asp?ftdb=STOKST68&level=1">Title 68</a>
<a href="#">Chapter 1</a>
<a href="#">Article 1</a>
</div>
<div class="paragraphs">
<p>Cite as: O.S. 68, Section 101</p>
<h3>Section 101. Tax Code</h3>
<p>The several tax laws recodified as Tax Codes, together with this act, shall be known as the Oklahoma Tax Code.</p>
<p><strong>Historical Data</strong></p>
<p>Laws 1965, SB 453, c. 235, Section 1</p>
</div>
</body>
</html>
"""

# Sample HTML with subsections (numbered pattern)
SAMPLE_SECTION_WITH_SUBSECTIONS_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 450.1 - Definitions | Oklahoma Statutes | OSCN</title></head>
<body>
<div class="document_header">
<a href="index.asp?ftdb=STOKST68&level=1">Title 68</a>
<a href="#">Chapter 1</a>
<a href="#">Article 4B</a>
</div>
<div class="paragraphs">
<p>Cite as: O.S. 68, Section 450.1</p>
<h3>Section 450.1. Definitions</h3>
<p>As used in this article:</p>
<p>1. "Controlled dangerous substance" means a drug or substance as defined in the Uniform Controlled Dangerous Substances Act.</p>
<p>2. "Dealer" means a person who:</p>
<p>a. manufactures or produces a controlled dangerous substance;</p>
<p>b. possesses forty-two and one-half (42.5) or more grams of marijuana; or</p>
<p>c. possesses seven (7) or more grams of any other controlled dangerous substance.</p>
<p>3. "Commission" means the Oklahoma Tax Commission.</p>
<p><strong>Historical Data</strong></p>
<p>Laws 1990, HB 1694, c. 25, Section 1, emerg. eff. July 1, 1990</p>
</div>
</body>
</html>
"""

# Sample HTML with lettered major divisions
SAMPLE_SECTION_LETTERED_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 54 - Burial of Indigent Persons | Oklahoma Statutes | OSCN</title></head>
<body>
<div class="document_header">
<a href="index.asp?ftdb=STOKST56&level=1">Title 56</a>
<a href="#">Chapter 3</a>
</div>
<div class="paragraphs">
<p>Cite as: O.S. 56, Section 54</p>
<h3>Section 54. Burial of Indigent Persons</h3>
<p>A. It shall be the duty of the overseers of the poor to provide for the burial of all dead bodies of persons dying in the county.</p>
<p>B. The term "public cemeteries" as used herein means any cemetery owned by the state or any subdivision thereof.</p>
<p><strong>Historical Data</strong></p>
<p>Laws 1951, p. 144, Section 1</p>
</div>
</body>
</html>
"""

# Sample title index HTML
SAMPLE_TITLE_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Title 68 - Revenue and Taxation</title></head>
<body>
<h1>Title 68. Revenue and Taxation</h1>
<div id="contents">
<p class="Index2"><a href="DeliverDocument.asp?CiteID=91842">Section 101. Tax Code</a></p>
<p class="Index2"><a href="DeliverDocument.asp?CiteID=91843">Section 102. Creation of Oklahoma Tax Commission</a></p>
<p class="Index2"><a href="DeliverDocument.asp?CiteID=91844">Section 103. Qualifications of Members</a></p>
</div>
</body>
</html>
"""


class TestOKTitlesRegistry:
    """Test Oklahoma title registries."""

    def test_title_68_in_titles(self):
        """Title 68 (Revenue and Taxation) is registered."""
        assert 68 in OK_TITLES
        assert "Revenue" in OK_TITLES[68]
        assert "Taxation" in OK_TITLES[68]

    def test_title_56_in_titles(self):
        """Title 56 (Poor Persons) is registered."""
        assert 56 in OK_TITLES
        assert "Poor" in OK_TITLES[56]

    def test_tax_sections_have_title_68(self):
        """Tax sections are from Title 68."""
        for section in OK_TAX_SECTIONS:
            assert section.startswith("68-")

    def test_welfare_sections_have_title_56(self):
        """Welfare sections are from Title 56."""
        for section in OK_WELFARE_SECTIONS:
            assert section.startswith("56-")

    def test_combined_sections_registry(self):
        """Combined registry contains tax and welfare sections."""
        assert len(OK_SECTIONS) >= len(OK_TAX_SECTIONS) + len(OK_WELFARE_SECTIONS)
        assert "68-101" in OK_SECTIONS
        assert "56-54" in OK_SECTIONS


class TestOKConverter:
    """Test OKConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = OKConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = OKConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = OKConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = OKConverter()
        url = converter._build_section_url(91842)
        assert "oscn.net" in url
        assert "DeliverDocument.asp" in url
        assert "CiteID=91842" in url

    def test_build_title_index_url(self):
        """Build correct URL for title index."""
        converter = OKConverter()
        url = converter._build_title_index_url(68)
        assert "STOKST68" in url
        assert "level=1" in url

    def test_get_cite_id_known_section(self):
        """Get CiteID for known section."""
        converter = OKConverter()
        cite_id = converter._get_cite_id("68-101")
        assert cite_id == 91842

    def test_get_cite_id_unknown_section(self):
        """Unknown section raises error."""
        converter = OKConverter()
        with pytest.raises(OKConverterError) as exc_info:
            converter._get_cite_id("68-999999")
        assert "not found in registry" in str(exc_info.value).lower()

    def test_context_manager(self):
        """Converter works as context manager."""
        with OKConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestOKConverterParsing:
    """Test OKConverter HTML parsing."""

    def test_parse_section_html_basic(self):
        """Parse basic section HTML into ParsedOKSection."""
        converter = OKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "68-101", "https://example.com", 91842
        )

        assert parsed.section_number == "68-101"
        assert parsed.section_title == "Tax Code"
        assert parsed.title_number == 68
        assert "Revenue" in parsed.title_name
        assert "Oklahoma Tax Code" in parsed.text
        assert parsed.source_url == "https://example.com"
        assert parsed.cite_id == 91842

    def test_parse_section_html_with_subsections(self):
        """Parse section HTML with numbered subsections."""
        converter = OKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML,
            "68-450.1",
            "https://example.com",
            92028,
        )

        assert parsed.section_number == "68-450.1"
        assert "Definitions" in parsed.section_title
        # Should have subsections 1, 2, 3
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)
        assert any(s.identifier == "3" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1 -> a, b, c)."""
        converter = OKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML,
            "68-450.1",
            "https://example.com",
            92028,
        )

        # Find subsection 2 which has children a, b, c
        sub_2 = next((s for s in parsed.subsections if s.identifier == "2"), None)
        assert sub_2 is not None
        # Should have children a, b, c
        assert len(sub_2.children) >= 2
        assert any(c.identifier == "a" for c in sub_2.children)
        assert any(c.identifier == "b" for c in sub_2.children)

    def test_parse_lettered_major_divisions(self):
        """Parse sections with major letter divisions (A, B)."""
        converter = OKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_LETTERED_HTML, "56-54", "https://example.com", 83200
        )

        assert parsed.section_number == "56-54"
        # Should have A and B subsections
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "A" for s in parsed.subsections)
        assert any(s.identifier == "B" for s in parsed.subsections)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = OKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "68-101", "https://example.com", 91842
        )

        assert parsed.history is not None
        assert "1965" in parsed.history or "Laws" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedOKSection to Section model."""
        converter = OKConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "68-101", "https://example.com", 91842
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "OK-68-101"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Tax Code"
        assert "Oklahoma Statutes" in section.title_name
        assert section.uslm_id == "ok/68/68-101"
        assert section.source_url == "https://example.com"


class TestOKConverterFetching:
    """Test OKConverter HTTP fetching with mocks."""

    @patch.object(OKConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = OKConverter()
        section = converter.fetch_section("68-101")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "OK-68-101"
        assert "Tax Code" in section.section_title

    @patch.object(OKConverter, "_get")
    def test_fetch_by_cite_id(self, mock_get):
        """Fetch section by CiteID directly."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = OKConverter()
        section = converter.fetch_by_cite_id(91842, "68-101")

        assert section is not None
        assert section.citation.section == "OK-68-101"

    @patch.object(OKConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Document cannot be found</body></html>"

        converter = OKConverter()
        with pytest.raises(OKConverterError) as exc_info:
            converter.fetch_by_cite_id(99999, "99-999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(OKConverter, "_get")
    def test_iter_title(self, mock_get):
        """Iterate over sections in a title."""
        # Return section HTML for each fetch
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = OKConverter()
        # Get only first few sections to avoid long test
        sections = []
        for i, section in enumerate(converter.iter_title(68)):
            sections.append(section)
            if i >= 2:
                break

        assert len(sections) >= 1
        assert all(isinstance(s, Section) for s in sections)

    def test_get_title_sections(self):
        """Get list of sections for a title."""
        converter = OKConverter()
        sections = converter.get_title_sections(68)

        assert len(sections) > 0
        assert all(s[0].startswith("68-") for s in sections)
        assert ("68-101", 91842) in sections


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(OKConverter, "_get")
    def test_fetch_ok_section(self, mock_get):
        """Test fetch_ok_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_ok_section("68-101")

        assert section is not None
        assert section.citation.section == "OK-68-101"

    @patch.object(OKConverter, "_get")
    def test_download_ok_title(self, mock_get):
        """Test download_ok_title function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        # This will iterate over all known sections, so use a small sample
        sections = download_ok_title(68)

        assert len(sections) > 0
        assert all(isinstance(s, Section) for s in sections)


class TestOKConverterIntegration:
    """Integration tests that hit real OSCN (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_code_section(self):
        """Fetch Oklahoma Tax Code section 68-101."""
        converter = OKConverter()
        section = converter.fetch_section("68-101")

        assert section is not None
        assert section.citation.section == "OK-68-101"
        assert "tax" in section.section_title.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_definitions_section(self):
        """Fetch Oklahoma definitions section with subsections."""
        converter = OKConverter()
        section = converter.fetch_by_cite_id(92028, "68-450.1")

        assert section is not None
        assert section.citation.section == "OK-68-450.1"
        # Should have subsections
        assert len(section.subsections) > 0

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Oklahoma welfare section 56-54."""
        converter = OKConverter()
        section = converter.fetch_by_cite_id(83200, "56-54")

        assert section is not None
        assert section.citation.section == "OK-56-54"
        assert "indigent" in section.text.lower() or "burial" in section.text.lower()
