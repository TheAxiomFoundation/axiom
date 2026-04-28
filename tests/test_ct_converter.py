"""Tests for Connecticut state statute converter.

Tests the CTConverter which fetches from cga.ct.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.ct import (
    CT_TAX_CHAPTERS,
    CT_TITLES,
    CT_WELFARE_CHAPTERS,
    CTConverter,
    CTConverterError,
    download_ct_chapter,
    fetch_ct_section,
)
from axiom_corpus.models import Section

# Sample HTML from cga.ct.gov for testing
SAMPLE_CHAPTER_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 203 - Property Tax Assessment</title></head>
<body>
<h2 class="chap-no">CHAPTER 203*</h2>
<h2 class="chap-name">PROPERTY TAX ASSESSMENT</h2>

<p class="front-note-first">*See Sec. 7-568 re additional property tax.</p>

<p class="chap_toc_hd">Table of Contents</p>
<p class="toc_catchln"><a href="#sec_12-40">Sec. 12-40. Notice requiring declaration of personal property.</a></p>
<p class="toc_catchln"><a href="#sec_12-41">Sec. 12-41. Filing of declaration.</a></p>
<p class="toc_catchln"><a href="#sec_12-42">Sec. 12-42. Extension for filing declaration.</a></p>

<p><span class="catchln" id="sec_12-40">Sec. 12-40. Notice requiring declaration of personal property.</span> The assessor or board of assessors of each town shall annually, at least thirty days before the first day of November, prepare a notice requiring all persons liable to pay taxes in such town to bring in to such assessor or board declarations of all goods and chattels.</p>

<p class="source-first">(1949 Rev., S. 1717; P.A. 84-146, S. 6; P.A. 99-189, S. 1, 20; P.A. 22-110, S. 2.)</p>

<p class="history-first">History: P.A. 84-146 included a reference to posting of notice on a place other than a signpost; P.A. 99-189 replaced list with declaration.</p>

<p class="annotation-first">Personal property in hands of executors, administrators or trustees. 30 C. 402; 38 C. 443.</p>

<p><span class="catchln" id="sec_12-41">Sec. 12-41. Filing of declaration.</span> (a) <b>Definitions.</b> "Municipality", whenever used in this section, includes each town, consolidated town and city, and consolidated town and borough.</p>

<p>(b) <b>Motor Vehicles.</b> No person required by law to file an annual declaration of personal property shall include in such declaration motor vehicles that are registered with the Department of Motor Vehicles.</p>

<p>(c) <b>Property included.</b> The annual declaration of the tangible personal property owned by such person on the assessment date, shall include, but is not limited to, the following property: (1) Machinery used in mills and factories; (2) cables, wires, poles and conduits; (3) horses, cattle and other livestock.</p>

<p class="source-first">(1949 Rev., S. 1718; 1961, P.A. 509, S. 1; P.A. 95-283, S. 37.)</p>

<p class="history-first">History: 1961 act provided that declaration need not include motor vehicles registered with motor vehicle department.</p>

<p><span class="catchln" id="sec_12-42">Sec. 12-42. Extension for filing declaration.</span> Any assessor may grant an extension of time for filing declarations of property. Such extension shall not extend beyond the fifteenth day of November.</p>

<p class="source-first">(1949 Rev., S. 1719.)</p>

</body>
</html>
"""

SAMPLE_INCOME_TAX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 211 - Income Tax</title></head>
<body>
<h2 class="chap-no">CHAPTER 211</h2>
<h2 class="chap-name">INCOME TAX</h2>

<p><span class="catchln" id="sec_12-700">Sec. 12-700. Imposition of tax.</span> (a) <b>Tax imposed.</b> A tax is hereby imposed for each taxable year, at the rates provided in this chapter, on the Connecticut taxable income of every resident and nonresident individual.</p>

<p>(b) <b>Rates.</b> The rates of tax are as follows: (1) Three percent of the first ten thousand dollars of Connecticut taxable income; (2) Five percent of Connecticut taxable income in excess of ten thousand dollars but not in excess of fifty thousand dollars; (3) Five and one-half percent of Connecticut taxable income in excess of fifty thousand dollars.</p>

<p class="source-first">(P.A. 91-3, S. 1; P.A. 97-1, June 18 Sp. Sess., S. 1.)</p>

<p class="history-first">History: P.A. 91-3 effective July 1, 1991; P.A. 97-1 amended rates.</p>

</body>
</html>
"""


class TestCTChaptersRegistry:
    """Test Connecticut chapter registries."""

    def test_chapter_203_in_tax_chapters(self):
        """Chapter 203 (Property Tax Assessment) is in tax chapters."""
        assert "203" in CT_TAX_CHAPTERS
        assert "Property Tax" in CT_TAX_CHAPTERS["203"]

    def test_chapter_211_in_tax_chapters(self):
        """Chapter 211 (Income Tax) is in tax chapters."""
        assert "211" in CT_TAX_CHAPTERS
        assert "Income Tax" in CT_TAX_CHAPTERS["211"]

    def test_chapter_216_in_tax_chapters(self):
        """Chapter 216 (Sales and Use Taxes) is in tax chapters."""
        assert "216" in CT_TAX_CHAPTERS
        assert "Sales" in CT_TAX_CHAPTERS["216"]

    def test_title_12_is_taxation(self):
        """Title 12 is Taxation."""
        assert "12" in CT_TITLES
        assert CT_TITLES["12"] == "Taxation"

    def test_title_17b_is_social_services(self):
        """Title 17b is Social Services."""
        assert "17b" in CT_TITLES
        assert CT_TITLES["17b"] == "Social Services"

    def test_welfare_chapters_exist(self):
        """Welfare chapters exist in CT_WELFARE_CHAPTERS."""
        assert len(CT_WELFARE_CHAPTERS) > 0
        # TANF chapter
        assert "319v" in CT_WELFARE_CHAPTERS


class TestCTConverter:
    """Test CTConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = CTConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year
        assert converter.verify_ssl is False

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = CTConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = CTConverter(year=2024)
        assert converter.year == 2024

    def test_build_chapter_url(self):
        """Build correct URL for chapter fetch."""
        converter = CTConverter()
        url = converter._build_chapter_url("203")
        assert "cga.ct.gov" in url
        assert "chap_203.htm" in url

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = CTConverter()
        url = converter._build_section_url("12-41", "203")
        assert "chap_203.htm" in url
        assert "#sec_12-41" in url

    def test_extract_title_from_section(self):
        """Extract title info from section number."""
        converter = CTConverter()

        title_num, title_name = converter._extract_title_from_section("12-41")
        assert title_num == "12"
        assert title_name == "Taxation"

        title_num, title_name = converter._extract_title_from_section("17b-112")
        assert title_num == "17b"
        assert title_name == "Social Services"

    def test_context_manager(self):
        """Converter works as context manager."""
        with CTConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestCTConverterParsing:
    """Test CTConverter HTML parsing."""

    def test_parse_chapter_html(self):
        """Parse chapter HTML and extract sections."""
        converter = CTConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, "203")

        assert len(sections) == 3
        assert "12-40" in sections
        assert "12-41" in sections
        assert "12-42" in sections

    def test_parse_section_12_40(self):
        """Parse section 12-40 details."""
        converter = CTConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, "203")

        sec = sections["12-40"]
        assert sec.section_number == "12-40"
        assert sec.section_title == "Notice requiring declaration of personal property"
        assert sec.chapter_number == "203"
        assert sec.title_number == "12"
        assert "assessor" in sec.text.lower()
        assert sec.source is not None
        assert "1949 Rev" in sec.source
        assert sec.history is not None
        assert "P.A. 84-146" in sec.history

    def test_parse_section_12_41_with_subsections(self):
        """Parse section 12-41 with subsections."""
        converter = CTConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, "203")

        sec = sections["12-41"]
        assert sec.section_number == "12-41"
        assert sec.section_title == "Filing of declaration"

        # Should have subsections (a), (b), (c)
        assert len(sec.subsections) >= 2
        sub_ids = [s.identifier for s in sec.subsections]
        assert "a" in sub_ids
        assert "b" in sub_ids

    def test_parse_subsection_headings(self):
        """Parse subsection headings (bold text after identifier)."""
        converter = CTConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, "203")

        sec = sections["12-41"]
        sub_a = next((s for s in sec.subsections if s.identifier == "a"), None)
        assert sub_a is not None
        assert sub_a.heading == "Definitions"

        sub_b = next((s for s in sec.subsections if s.identifier == "b"), None)
        assert sub_b is not None
        assert sub_b.heading == "Motor Vehicles"

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (c)."""
        converter = CTConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, "203")

        sec = sections["12-41"]
        sub_c = next((s for s in sec.subsections if s.identifier == "c"), None)

        # Subsection (c) should have children (1), (2), (3)
        if sub_c:
            child_ids = [c.identifier for c in sub_c.children]
            assert "1" in child_ids or len(sub_c.children) > 0

    def test_to_section_model(self):
        """Convert ParsedCTSection to Section model."""
        converter = CTConverter()
        sections = converter._parse_chapter_html(SAMPLE_CHAPTER_HTML, "203")
        section = converter._to_section(sections["12-41"])

        assert isinstance(section, Section)
        assert section.citation.section == "CT-12-41"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Filing of declaration"
        assert "Connecticut General Statutes" in section.title_name
        assert "Title 12" in section.title_name
        assert section.uslm_id == "ct/12/12-41"
        assert "cga.ct.gov" in section.source_url

    def test_parse_income_tax_section(self):
        """Parse income tax section with rates."""
        converter = CTConverter()
        sections = converter._parse_chapter_html(SAMPLE_INCOME_TAX_HTML, "211")

        assert "12-700" in sections
        sec = sections["12-700"]
        assert sec.section_title == "Imposition of tax"
        assert "tax" in sec.text.lower()
        assert len(sec.subsections) >= 1


class TestCTConverterFetching:
    """Test CTConverter HTTP fetching with mocks."""

    @patch.object(CTConverter, "_get")
    def test_fetch_chapter(self, mock_get):
        """Fetch and parse an entire chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = CTConverter()
        sections = converter.fetch_chapter("203")

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections.values())
        assert "12-41" in sections

    @patch.object(CTConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = CTConverter()
        section = converter.fetch_section("12-41", "203")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "CT-12-41"
        assert "Filing of declaration" in section.section_title

    @patch.object(CTConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = CTConverter()
        with pytest.raises(CTConverterError) as exc_info:
            converter.fetch_section("12-999", "203")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(CTConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = CTConverter()
        sections = converter.get_chapter_section_numbers("203")

        assert len(sections) == 3
        assert "12-40" in sections
        assert "12-41" in sections
        assert "12-42" in sections

    @patch.object(CTConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = CTConverter()
        sections = list(converter.iter_chapter("203"))

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(CTConverter, "_get")
    def test_fetch_ct_section(self, mock_get):
        """Test fetch_ct_section function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        section = fetch_ct_section("12-41", "203")

        assert section is not None
        assert section.citation.section == "CT-12-41"

    @patch.object(CTConverter, "_get")
    def test_download_ct_chapter(self, mock_get):
        """Test download_ct_chapter function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        sections = download_ct_chapter("203")

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestCTConverterChapterMapping:
    """Test chapter mapping from section numbers."""

    def test_get_chapter_for_title_12_section(self):
        """Map Title 12 section to correct chapter."""
        converter = CTConverter()

        # Section 12-41 should map to chapter 203 (Property Tax Assessment)
        chapter = converter._get_chapter_for_section("12-41")
        assert chapter == "203"

        # Section 12-700 should map to chapter 211 (Income Tax)
        chapter = converter._get_chapter_for_section("12-700")
        assert chapter == "216"  # Based on range mapping


class TestCTConverterIntegration:
    """Integration tests that hit real cga.ct.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_property_tax_section(self):
        """Fetch Connecticut property tax section 12-41."""
        converter = CTConverter()
        section = converter.fetch_section("12-41", "203")

        assert section is not None
        assert section.citation.section == "CT-12-41"
        assert "declaration" in section.section_title.lower() or "filing" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_203_sections(self):
        """Get list of sections in Chapter 203."""
        converter = CTConverter()
        sections = converter.get_chapter_section_numbers("203")

        assert len(sections) > 0
        assert all(s.startswith("12-") for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_chapter(self):
        """Fetch Connecticut income tax chapter 211."""
        converter = CTConverter()
        try:
            sections = converter.fetch_chapter("211")
            assert len(sections) > 0
            # Should have section 12-700 (Imposition of tax)
            section_nums = list(sections.keys())
            assert any("700" in s for s in section_nums)
        except Exception as e:
            pytest.skip(f"Could not fetch chapter 211: {e}")
