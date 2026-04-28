"""Tests for Iowa state statute converter.

Tests the IAConverter which fetches from legis.iowa.gov and converts
to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.ia import (
    IA_TAX_CHAPTERS,
    IA_TITLES,
    IA_WELFARE_CHAPTERS,
    IAConverter,
    IAConverterError,
    download_ia_chapter,
    fetch_ia_section,
)
from axiom_corpus.models import Section

# Sample RTF content for testing (simplified RTF format)
SAMPLE_RTF_CONTENT = rb"""{\\rtf1\\ansi\\deff0
{\\fonttbl{\\f0 Times New Roman;}}
{\\colortbl;}
\\f0\\fs24
422.5  Tax imposed.
1.  A tax is hereby imposed upon every resident and nonresident of this state which tax shall be levied, collected, and paid annually upon and with respect to the entire taxable income at the following rates:
a.  On taxable income from zero through five hundred dollars, thirty-six hundredths percent.
b.  On taxable income from five hundred one dollars through one thousand dollars, seventy-two hundredths percent.
c.  On taxable income from one thousand one dollars through two thousand dollars, two and forty-three hundredths percent.
2.  For purposes of computing the tax, taxable income shall be rounded to the nearest dollar.
3.  The tax shall be computed on the taxable income of residents and nonresidents as determined under this chapter.
[2023 Acts, ch 19, sec 2401]
History: C97, sec 1379; C24, 27, 31, sec 6943; C35, sec 6943-c5]
}
"""

# Sample chapter index HTML for testing
SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Iowa Code Chapter 422</title></head>
<body>
<h1>Chapter 422 - INDIVIDUAL INCOME, CORPORATE, AND FRANCHISE TAXES</h1>
<table>
<tr>
<td><a href="/docs/code/2025/422.1.pdf">section;422.1 - Classification of chapter</a></td>
<td><a href="/docs/code/2025/422.1.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/422.2.pdf">section;422.2 - Short title</a></td>
<td><a href="/docs/code/2025/422.2.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/422.3.pdf">section;422.3 - Definitions</a></td>
<td><a href="/docs/code/2025/422.3.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/422.4.pdf">section;422.4 - Definitions relating to individuals</a></td>
<td><a href="/docs/code/2025/422.4.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/422.5.pdf">section;422.5 - Tax imposed</a></td>
<td><a href="/docs/code/2025/422.5.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/422.6.pdf">section;422.6 - Deductions</a></td>
<td><a href="/docs/code/2025/422.6.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/422.7.pdf">section;422.7 - Additions to income</a></td>
<td><a href="/docs/code/2025/422.7.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/422.11A.pdf">section;422.11A - Earned income tax credit</a></td>
<td><a href="/docs/code/2025/422.11A.rtf">RTF</a></td>
</tr>
</table>
</body>
</html>
"""

# Sample welfare chapter index HTML
SAMPLE_WELFARE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Iowa Code Chapter 239B</title></head>
<body>
<h1>Chapter 239B - FAMILY INVESTMENT PROGRAM</h1>
<table>
<tr>
<td><a href="/docs/code/2025/239B.1.pdf">section;239B.1 - Definitions</a></td>
<td><a href="/docs/code/2025/239B.1.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/239B.2.pdf">section;239B.2 - Eligibility</a></td>
<td><a href="/docs/code/2025/239B.2.rtf">RTF</a></td>
</tr>
<tr>
<td><a href="/docs/code/2025/239B.3.pdf">section;239B.3 - Assistance amounts</a></td>
<td><a href="/docs/code/2025/239B.3.rtf">RTF</a></td>
</tr>
</table>
</body>
</html>
"""


class TestIATitlesRegistry:
    """Test Iowa title and chapter registries."""

    def test_title_x_is_financial_resources(self):
        """Title X is Financial Resources."""
        assert "X" in IA_TITLES
        assert IA_TITLES["X"] == "Financial Resources"

    def test_title_vi_is_human_services(self):
        """Title VI is Human Services."""
        assert "VI" in IA_TITLES
        assert IA_TITLES["VI"] == "Human Services"

    def test_all_16_titles_present(self):
        """All 16 Iowa Code titles are registered."""
        expected_titles = [
            "I",
            "II",
            "III",
            "IV",
            "V",
            "VI",
            "VII",
            "VIII",
            "IX",
            "X",
            "XI",
            "XII",
            "XIII",
            "XIV",
            "XV",
            "XVI",
        ]
        for title in expected_titles:
            assert title in IA_TITLES

    def test_chapter_422_in_tax_chapters(self):
        """Chapter 422 (Income Tax) is in tax chapters."""
        assert "422" in IA_TAX_CHAPTERS
        assert "Income" in IA_TAX_CHAPTERS["422"]

    def test_chapter_425_in_tax_chapters(self):
        """Chapter 425 (Homestead Tax Credits) is in tax chapters."""
        assert "425" in IA_TAX_CHAPTERS
        assert "Homestead" in IA_TAX_CHAPTERS["425"]

    def test_chapter_239B_in_welfare_chapters(self):
        """Chapter 239B (Family Investment Program) is in welfare chapters."""
        assert "239B" in IA_WELFARE_CHAPTERS
        assert "Family Investment" in IA_WELFARE_CHAPTERS["239B"]

    def test_chapter_249A_in_welfare_chapters(self):
        """Chapter 249A (Medical Assistance) is in welfare chapters."""
        assert "249A" in IA_WELFARE_CHAPTERS
        assert "Medical Assistance" in IA_WELFARE_CHAPTERS["249A"]


class TestIAConverter:
    """Test IAConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = IAConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = IAConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = IAConverter(year=2024)
        assert converter.year == 2024

    def test_get_chapter_from_section_simple(self):
        """Extract chapter number from simple section."""
        converter = IAConverter()
        assert converter._get_chapter_from_section("422.5") == "422"
        assert converter._get_chapter_from_section("425.1") == "425"

    def test_get_chapter_from_section_with_letter(self):
        """Extract chapter number from section with letter suffix."""
        converter = IAConverter()
        assert converter._get_chapter_from_section("422A.5") == "422A"
        assert converter._get_chapter_from_section("239B.1") == "239B"

    def test_get_title_for_chapter_tax(self):
        """Get title info for tax chapters."""
        converter = IAConverter()
        roman, name = converter._get_title_for_chapter("422")
        assert roman == "X"
        assert name == "Financial Resources"

    def test_get_title_for_chapter_welfare(self):
        """Get title info for welfare chapters."""
        converter = IAConverter()
        roman, name = converter._get_title_for_chapter("239B")
        assert roman == "VI"
        assert name == "Human Services"

    def test_get_chapter_title_known(self):
        """Get known chapter title."""
        converter = IAConverter()
        assert "Income" in converter._get_chapter_title("422")
        assert "Family Investment" in converter._get_chapter_title("239B")

    def test_get_chapter_title_unknown(self):
        """Get generic title for unknown chapter."""
        converter = IAConverter()
        title = converter._get_chapter_title("999")
        assert "Chapter 999" in title

    def test_build_chapter_sections_url(self):
        """Build correct URL for chapter section listing."""
        converter = IAConverter(year=2025)
        url = converter._build_chapter_sections_url("422")
        assert "legis.iowa.gov" in url
        assert "codeChapter=422" in url
        assert "year=2025" in url

    def test_build_section_rtf_url(self):
        """Build correct URL for section RTF file."""
        converter = IAConverter(year=2025)
        url = converter._build_section_rtf_url("422.5")
        assert "legis.iowa.gov" in url
        assert "/docs/code/2025/422.5.rtf" in url

    def test_build_section_pdf_url(self):
        """Build correct URL for section PDF file."""
        converter = IAConverter(year=2025)
        url = converter._build_section_pdf_url("422.5")
        assert "legis.iowa.gov" in url
        assert "/docs/code/2025/422.5.pdf" in url

    def test_build_title_chapters_url(self):
        """Build correct URL for title chapters listing."""
        converter = IAConverter(year=2025)
        url = converter._build_title_chapters_url("X")
        assert "legis.iowa.gov" in url
        assert "title=X" in url
        assert "year=2025" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with IAConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestIAConverterParsing:
    """Test IAConverter parsing methods."""

    def test_extract_text_from_rtf(self):
        """Extract plain text from RTF content."""
        converter = IAConverter()
        text = converter._extract_text_from_rtf(SAMPLE_RTF_CONTENT)

        assert "422.5" in text
        assert "Tax imposed" in text
        assert "taxable income" in text.lower()
        # RTF control codes should be removed
        assert "\\rtf" not in text
        assert "\\fonttbl" not in text

    def test_extract_section_title(self):
        """Extract section title from text."""
        converter = IAConverter()
        text = converter._extract_text_from_rtf(SAMPLE_RTF_CONTENT)
        title = converter._extract_section_title(text, "422.5")

        assert title == "Tax imposed"

    def test_extract_section_title_not_found(self):
        """Return generic title when not found."""
        converter = IAConverter()
        title = converter._extract_section_title("Some random text", "422.5")

        assert "Section 422.5" in title

    def test_extract_history(self):
        """Extract history note from text."""
        converter = IAConverter()
        text = converter._extract_text_from_rtf(SAMPLE_RTF_CONTENT)
        history = converter._extract_history(text)

        assert history is not None
        # Should find either the Acts reference or History line
        assert "Acts" in history or "C97" in history or "ch" in history

    def test_parse_subsections(self):
        """Parse subsections from text."""
        converter = IAConverter()
        text = converter._extract_text_from_rtf(SAMPLE_RTF_CONTENT)
        subsections = converter._parse_subsections(text)

        # Should have at least subsection 1
        assert len(subsections) >= 1
        assert any(s.identifier == "1" for s in subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a, b, c under 1)."""
        converter = IAConverter()
        text = converter._extract_text_from_rtf(SAMPLE_RTF_CONTENT)
        subsections = converter._parse_subsections(text)

        # Find subsection 1
        sub_1 = next((s for s in subsections if s.identifier == "1"), None)
        if sub_1:
            # Should have children a, b, c
            assert len(sub_1.children) >= 2
            assert any(c.identifier == "a" for c in sub_1.children)
            assert any(c.identifier == "b" for c in sub_1.children)

    def test_parse_rtf_content(self):
        """Parse full RTF content into ParsedIASection."""
        converter = IAConverter()
        parsed = converter._parse_rtf_content(SAMPLE_RTF_CONTENT, "422.5")

        assert parsed.section_number == "422.5"
        assert parsed.section_title == "Tax imposed"
        assert parsed.chapter_number == "422"
        assert parsed.title_roman == "X"
        assert parsed.title_name == "Financial Resources"
        assert "taxable income" in parsed.text.lower()

    def test_to_section_model(self):
        """Convert ParsedIASection to Section model."""
        converter = IAConverter()
        parsed = converter._parse_rtf_content(SAMPLE_RTF_CONTENT, "422.5")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "IA-422.5"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Tax imposed"
        assert "Iowa Code" in section.title_name
        assert section.uslm_id == "ia/422/422.5"

    def test_parse_chapter_index_html(self):
        """Parse chapter index HTML for section listings."""
        converter = IAConverter(year=2025)
        sections = converter._parse_chapter_index_html(SAMPLE_CHAPTER_INDEX_HTML, "422")

        assert len(sections) >= 6
        assert any(s["section_number"] == "422.1" for s in sections)
        assert any(s["section_number"] == "422.5" for s in sections)
        assert any(s["section_number"] == "422.11A" for s in sections)

        # Check titles are extracted
        sec_422_5 = next((s for s in sections if s["section_number"] == "422.5"), None)
        assert sec_422_5 is not None
        assert sec_422_5["section_title"] == "Tax imposed"


class TestIAConverterFetching:
    """Test IAConverter HTTP fetching with mocks."""

    @patch.object(IAConverter, "_get_bytes")
    def test_fetch_section(self, mock_get_bytes):
        """Fetch and parse a single section."""
        mock_get_bytes.return_value = SAMPLE_RTF_CONTENT

        converter = IAConverter()
        section = converter.fetch_section("422.5")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "IA-422.5"
        assert section.section_title == "Tax imposed"

    @patch.object(IAConverter, "_get_bytes")
    def test_fetch_section_not_found(self, mock_get_bytes):
        """Handle section not found error."""
        import httpx

        mock_response = httpx.Response(404, request=httpx.Request("GET", "http://test"))
        mock_get_bytes.side_effect = httpx.HTTPStatusError(
            "Not Found", request=mock_response.request, response=mock_response
        )

        converter = IAConverter()
        with pytest.raises(IAConverterError) as exc_info:
            converter.fetch_section("999.999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(IAConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = IAConverter(year=2025)
        sections = converter.get_chapter_section_numbers("422")

        assert len(sections) >= 6
        assert "422.1" in sections
        assert "422.5" in sections
        assert "422.11A" in sections

    @patch.object(IAConverter, "_get")
    def test_get_chapter_sections_with_titles(self, mock_get):
        """Get section numbers and titles from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = IAConverter(year=2025)
        sections = converter.get_chapter_sections_with_titles("422")

        assert len(sections) >= 6
        sec_422_5 = next((s for s in sections if s["section_number"] == "422.5"), None)
        assert sec_422_5 is not None
        assert sec_422_5["section_title"] == "Tax imposed"

    @patch.object(IAConverter, "_get")
    @patch.object(IAConverter, "_get_bytes")
    def test_iter_chapter(self, mock_get_bytes, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns index, subsequent calls return RTF
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML
        mock_get_bytes.return_value = SAMPLE_RTF_CONTENT

        converter = IAConverter(year=2025)
        sections = list(converter.iter_chapter("422"))

        # Should have fetched all sections from index
        assert len(sections) >= 1
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(IAConverter, "_get_bytes")
    def test_fetch_ia_section(self, mock_get_bytes):
        """Test fetch_ia_section function."""
        mock_get_bytes.return_value = SAMPLE_RTF_CONTENT

        section = fetch_ia_section("422.5")

        assert section is not None
        assert section.citation.section == "IA-422.5"

    @patch.object(IAConverter, "_get")
    @patch.object(IAConverter, "_get_bytes")
    def test_download_ia_chapter(self, mock_get_bytes, mock_get):
        """Test download_ia_chapter function."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML
        mock_get_bytes.return_value = SAMPLE_RTF_CONTENT

        sections = download_ia_chapter("422")

        assert len(sections) >= 1
        assert all(isinstance(s, Section) for s in sections)


class TestIAConverterWelfare:
    """Test IAConverter with welfare/human services chapters."""

    @patch.object(IAConverter, "_get")
    @patch.object(IAConverter, "_get_bytes")
    def test_welfare_chapter_iteration(self, mock_get_bytes, mock_get):
        """Iterate over welfare chapter sections."""
        mock_get.return_value = SAMPLE_WELFARE_CHAPTER_INDEX_HTML

        # RTF content for welfare section
        welfare_rtf = rb"""{\\rtf1\\ansi
239B.1  Definitions.
1.  "Assistance unit" means the group of individuals whose needs are considered as a unit.
2.  "Family investment program" or "FIP" means the program established under this chapter.
[History: 2023 Acts, ch 45]
}
"""
        mock_get_bytes.return_value = welfare_rtf

        converter = IAConverter(year=2025)
        sections = list(converter.iter_chapter("239B"))

        assert len(sections) >= 1
        # Check welfare chapter detection
        for section in sections:
            assert "IA-239B" in section.citation.section
            assert "Iowa Code" in section.title_name


class TestIAConverterIntegration:
    """Integration tests that hit real legis.iowa.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Iowa Income Tax section 422.5."""
        converter = IAConverter()
        try:
            section = converter.fetch_section("422.5")

            assert section is not None
            assert section.citation.section == "IA-422.5"
            # Basic content validation
            assert len(section.text) > 100
        except IAConverterError as e:
            pytest.skip(f"Could not fetch section: {e}")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_homestead_credit_section(self):
        """Fetch Iowa Homestead Tax Credit section 425.1."""
        converter = IAConverter()
        try:
            section = converter.fetch_section("425.1")

            assert section is not None
            assert section.citation.section == "IA-425.1"
        except IAConverterError as e:
            pytest.skip(f"Could not fetch section: {e}")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_422_sections(self):
        """Get list of sections in Chapter 422."""
        converter = IAConverter()
        sections = converter.get_chapter_section_numbers("422")

        assert len(sections) > 0
        assert all(s.startswith("422.") for s in sections)
        assert "422.1" in sections or "422.2" in sections  # Should have early sections

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Iowa welfare section 239B.1."""
        converter = IAConverter()
        try:
            section = converter.fetch_section("239B.1")
            assert section.citation.section == "IA-239B.1"
        except IAConverterError:
            pytest.skip("Section 239B.1 not found")
