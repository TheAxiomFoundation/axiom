"""Tests for Kentucky Revised Statutes converter.

Tests the KYConverter which fetches PDFs from apps.legislature.ky.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from axiom.converters.us_states.ky import (
    KY_CHAPTER_IDS,
    KY_TAX_CHAPTERS,
    KY_WELFARE_CHAPTERS,
    KYConverter,
    KYConverterError,
    download_ky_chapter,
    fetch_ky_section,
)
from axiom.models import Section

# Sample HTML from chapter.aspx for testing (chapter 141 - Income Taxes)
SAMPLE_CHAPTER_HTML = """<!DOCTYPE html>
<html>
<head><title>Kentucky Revised Statutes - Chapter 141</title></head>
<body>
<h1>Chapter 141 - Income Taxes</h1>
<div class="content">
<p>The KRS database was last updated on 12/30/2025</p>
<ul>
<li><a href="statute.aspx?id=56352">.010 Definitions</a></li>
<li><a href="statute.aspx?id=47430">.011 Taxable year</a></li>
<li><a href="statute.aspx?id=56339">.020 Tax imposed on individuals</a></li>
<li><a href="statute.aspx?id=29043">.030 Gross income defined</a></li>
<li><a href="statute.aspx?id=55406">.039 Exclusion of combat zone pay</a></li>
</ul>
</div>
</body>
</html>
"""

# Sample PDF text content for testing
SAMPLE_PDF_TEXT = """141.010 Definitions for KRS Chapter 141.

As used in this chapter, unless the context requires otherwise:

(1) "Commissioner" means the commissioner of the Department of Revenue or the
commissioner's designated representative;

(2) "Corporation" means any corporation, joint-stock company, or association
organized for profit, or any bank, trust company, savings and loan association,
or building and loan association organized under the laws of any state;

(a) For purposes of this section, a limited liability company shall be
classified in accordance with its federal income tax classification;

(b) A partnership shall not be considered a corporation for purposes of
this chapter;

(3) "Fiduciary" means a guardian, trustee, executor, administrator, receiver,
conservator, or any person, whether individual or corporate, acting in any
fiduciary capacity for any person, trust, or estate;

(4) "Fiscal year" means an accounting period of twelve (12) months ending on
the last day of any month other than December;

(5) "Gross income" means gross income as defined in Section 61 of the Internal
Revenue Code;

(6) "Individual" means a natural person;

Effective: June 27, 2019
History: Amended 2019 Ky. Acts ch. 151, sec. 13, effective June 27, 2019.
"""

# Sample PDF text for section with nested subsections
SAMPLE_NESTED_PDF_TEXT = """141.020 Tax imposed on individuals.

(1) There is hereby imposed on the Kentucky taxable income of every individual
a tax as follows:

(a) For taxable years beginning after December 31, 2017, but before January 1,
2019, the rate shall be five percent (5%);

(b) For taxable years beginning on or after January 1, 2019, the rate shall
be four percent (4%);

(2) Every resident individual shall be allowed a credit against the tax imposed
by this section equal to the lesser of:

(a) The amount of income tax imposed for the taxable year by another state
on income subject to tax under this chapter; or

(b) The tax under this chapter attributable to the income taxed by the
other state.

1. The credit shall be claimed on the annual return;

2. Documentation of the tax paid to another state shall be attached to
the return.

(3) Nonresident individuals shall be subject to tax on income derived from
sources within this Commonwealth.

Effective: January 1, 2019
History: Amended 2018 Ky. Acts ch. 171, sec. 45.
"""


class TestKYChaptersRegistry:
    """Test Kentucky chapter registries."""

    def test_chapter_141_in_tax_chapters(self):
        """Chapter 141 (Income Taxes) is in tax chapters."""
        assert 141 in KY_TAX_CHAPTERS
        assert "Income" in KY_TAX_CHAPTERS[141]

    def test_chapter_139_in_tax_chapters(self):
        """Chapter 139 (Sales and Use Taxes) is in tax chapters."""
        assert 139 in KY_TAX_CHAPTERS
        assert "Sales" in KY_TAX_CHAPTERS[139]

    def test_chapter_205_in_welfare_chapters(self):
        """Chapter 205 (Public Assistance) is in welfare chapters."""
        assert 205 in KY_WELFARE_CHAPTERS
        assert "Assistance" in KY_WELFARE_CHAPTERS[205]

    def test_tax_chapters_range(self):
        """Tax chapters are in the expected range 131-143."""
        for chapter in KY_TAX_CHAPTERS:
            assert 131 <= chapter <= 143

    def test_chapter_ids_exist(self):
        """All tax and welfare chapters have IDs."""
        for chapter in KY_TAX_CHAPTERS:
            assert chapter in KY_CHAPTER_IDS, f"Missing ID for chapter {chapter}"
        for chapter in KY_WELFARE_CHAPTERS:
            assert chapter in KY_CHAPTER_IDS, f"Missing ID for chapter {chapter}"


class TestKYConverter:
    """Test KYConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = KYConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = KYConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = KYConverter(year=2024)
        assert converter.year == 2024

    def test_build_chapter_url(self):
        """Build correct URL for chapter fetch."""
        converter = KYConverter()
        url = converter._build_chapter_url(141)
        assert "apps.legislature.ky.gov" in url
        assert "chapter.aspx" in url
        assert "id=37674" in url  # Chapter 141 ID

    def test_build_chapter_url_unknown(self):
        """Raise error for unknown chapter."""
        converter = KYConverter()
        with pytest.raises(KYConverterError) as exc_info:
            converter._build_chapter_url(999)
        assert "Unknown chapter ID" in str(exc_info.value)

    def test_build_section_url(self):
        """Build correct URL for section PDF fetch."""
        converter = KYConverter()
        url = converter._build_section_url(56352)
        assert "statute.aspx" in url
        assert "id=56352" in url

    def test_get_title_for_chapter_tax(self):
        """Get title info for tax chapter."""
        converter = KYConverter()
        roman, name = converter._get_title_for_chapter(141)
        assert roman == "XI"
        assert name == "Revenue and Taxation"

    def test_get_title_for_chapter_welfare(self):
        """Get title info for welfare chapter."""
        converter = KYConverter()
        roman, name = converter._get_title_for_chapter(205)
        assert roman == "XVII"
        assert name == "Public Assistance"

    def test_get_title_for_unknown_chapter(self):
        """Get None for unknown chapter."""
        converter = KYConverter()
        roman, name = converter._get_title_for_chapter(999)
        assert roman is None
        assert name is None

    def test_context_manager(self):
        """Converter works as context manager."""
        with KYConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestKYConverterParsing:
    """Test KYConverter PDF parsing."""

    def test_parse_section_pdf_basic(self):
        """Parse section PDF into ParsedKYSection."""
        converter = KYConverter()
        # Mock the PDF extractor to return sample text
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = SAMPLE_PDF_TEXT

        parsed = converter._parse_section_pdf(
            b"fake pdf content",
            "141.010",
            "https://example.com/statute.aspx?id=56352",
        )

        assert parsed.section_number == "141.010"
        assert "Definitions" in parsed.section_title
        assert parsed.chapter_number == 141
        assert parsed.chapter_title == "Income Taxes"
        assert parsed.title_roman == "XI"
        assert parsed.title_name == "Revenue and Taxation"
        assert "Commissioner" in parsed.text
        assert parsed.source_url == "https://example.com/statute.aspx?id=56352"

    def test_parse_subsections(self):
        """Parse subsections from PDF text."""
        converter = KYConverter()
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = SAMPLE_PDF_TEXT

        parsed = converter._parse_section_pdf(
            b"fake pdf content",
            "141.010",
            "https://example.com",
        )

        # Should have subsections (1), (2), (3), etc.
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under (2)."""
        converter = KYConverter()
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = SAMPLE_PDF_TEXT

        parsed = converter._parse_section_pdf(
            b"fake pdf content",
            "141.010",
            "https://example.com",
        )

        # Find subsection (2)
        sub_2 = next((s for s in parsed.subsections if s.identifier == "2"), None)
        assert sub_2 is not None
        # Should have children (a) and (b)
        assert len(sub_2.children) >= 2
        assert any(c.identifier == "a" for c in sub_2.children)
        assert any(c.identifier == "b" for c in sub_2.children)

    def test_parse_effective_date(self):
        """Parse effective date from PDF text."""
        converter = KYConverter()
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = SAMPLE_PDF_TEXT

        parsed = converter._parse_section_pdf(
            b"fake pdf content",
            "141.010",
            "https://example.com",
        )

        assert parsed.effective_date is not None
        assert parsed.effective_date.year == 2019
        assert parsed.effective_date.month == 6
        assert parsed.effective_date.day == 27

    def test_parse_history(self):
        """Parse history note from PDF text."""
        converter = KYConverter()
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = SAMPLE_PDF_TEXT

        parsed = converter._parse_section_pdf(
            b"fake pdf content",
            "141.010",
            "https://example.com",
        )

        assert parsed.history is not None
        assert "2019" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedKYSection to Section model."""
        converter = KYConverter()
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = SAMPLE_PDF_TEXT

        parsed = converter._parse_section_pdf(
            b"fake pdf content",
            "141.010",
            "https://example.com",
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "KY-141.010"
        assert section.citation.title == 0  # State law indicator
        assert "Definitions" in section.section_title
        assert "Kentucky Revised Statutes" in section.title_name
        assert section.uslm_id == "ky/141/141.010"

    def test_empty_pdf_raises_error(self):
        """Empty PDF raises error."""
        converter = KYConverter()
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = ""

        with pytest.raises(KYConverterError) as exc_info:
            converter._parse_section_pdf(
                b"fake pdf content",
                "141.010",
                "https://example.com",
            )

        assert "appears empty" in str(exc_info.value).lower()


class TestKYConverterChapterParsing:
    """Test KYConverter chapter HTML parsing."""

    def test_load_chapter_section_ids(self):
        """Load section IDs from chapter HTML."""
        converter = KYConverter()

        with patch.object(converter, "_get_text", return_value=SAMPLE_CHAPTER_HTML):
            converter._load_chapter_section_ids(141)

        assert "141.010" in converter._section_id_cache
        assert converter._section_id_cache["141.010"] == 56352
        assert "141.011" in converter._section_id_cache
        assert "141.020" in converter._section_id_cache

    def test_get_chapter_section_numbers(self):
        """Get list of section numbers from chapter."""
        converter = KYConverter()

        with patch.object(converter, "_get_text", return_value=SAMPLE_CHAPTER_HTML):
            sections = converter.get_chapter_section_numbers(141)

        assert len(sections) == 5
        assert "141.010" in sections
        assert "141.011" in sections
        assert "141.020" in sections
        # Should be sorted
        assert sections == sorted(sections, key=lambda s: (int(s.split(".")[1].rstrip("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")), s.split(".")[1]))


class TestKYConverterFetching:
    """Test KYConverter HTTP fetching with mocks."""

    @patch.object(KYConverter, "_get_text")
    @patch.object(KYConverter, "_get")
    def test_fetch_section(self, mock_get, mock_get_text):
        """Fetch and parse a single section."""
        mock_get_text.return_value = SAMPLE_CHAPTER_HTML

        # Mock PDF extractor
        converter = KYConverter()
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = SAMPLE_PDF_TEXT

        section = converter.fetch_section("141.010")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "KY-141.010"
        assert "Definitions" in section.section_title

    @patch.object(KYConverter, "_get_text")
    def test_fetch_section_not_found(self, mock_get_text):
        """Handle section not found error."""
        mock_get_text.return_value = SAMPLE_CHAPTER_HTML

        converter = KYConverter()

        with pytest.raises(KYConverterError) as exc_info:
            converter.fetch_section("141.999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(KYConverter, "_get_text")
    @patch.object(KYConverter, "_get")
    def test_iter_chapter(self, mock_get, mock_get_text):
        """Iterate over sections in a chapter."""
        mock_get_text.return_value = SAMPLE_CHAPTER_HTML
        mock_get.return_value = b"fake pdf"

        converter = KYConverter()
        converter._pdf_extractor = MagicMock()
        converter._pdf_extractor.extract_text.return_value = SAMPLE_PDF_TEXT

        sections = list(converter.iter_chapter(141))

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(KYConverter, "_get_text")
    @patch.object(KYConverter, "_get")
    def test_fetch_ky_section(self, mock_get, mock_get_text):
        """Test fetch_ky_section function."""
        mock_get_text.return_value = SAMPLE_CHAPTER_HTML
        mock_get.return_value = b"fake pdf"

        with patch("axiom.converters.us_states.ky.PDFTextExtractor") as mock_extractor:
            mock_extractor.return_value.extract_text.return_value = SAMPLE_PDF_TEXT
            section = fetch_ky_section("141.010")

        assert section is not None
        assert section.citation.section == "KY-141.010"

    @patch.object(KYConverter, "_get_text")
    @patch.object(KYConverter, "_get")
    def test_download_ky_chapter(self, mock_get, mock_get_text):
        """Test download_ky_chapter function."""
        mock_get_text.return_value = SAMPLE_CHAPTER_HTML
        mock_get.return_value = b"fake pdf"

        with patch("axiom.converters.us_states.ky.PDFTextExtractor") as mock_extractor:
            mock_extractor.return_value.extract_text.return_value = SAMPLE_PDF_TEXT
            sections = download_ky_chapter(141)

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)


class TestKYConverterIntegration:
    """Integration tests that hit real apps.legislature.ky.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_definitions(self):
        """Fetch Kentucky Income Tax definitions section 141.010."""
        converter = KYConverter()
        section = converter.fetch_section("141.010")

        assert section is not None
        assert section.citation.section == "KY-141.010"
        assert "definition" in section.section_title.lower() or "141.010" in section.section_title
        assert len(section.text) > 100

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_rate(self):
        """Fetch Kentucky Income Tax rate section 141.020."""
        converter = KYConverter()
        section = converter.fetch_section("141.020")

        assert section is not None
        assert section.citation.section == "KY-141.020"
        assert "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_141_sections(self):
        """Get list of sections in Chapter 141."""
        converter = KYConverter()
        sections = converter.get_chapter_section_numbers(141)

        assert len(sections) > 10
        assert all(s.startswith("141.") for s in sections)
        assert "141.010" in sections  # Definitions

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_public_assistance_section(self):
        """Fetch a public assistance section from chapter 205."""
        converter = KYConverter()
        try:
            sections = converter.get_chapter_section_numbers(205)
            if sections:
                section = converter.fetch_section(sections[0])
                assert section.citation.section.startswith("KY-205.")
        except KYConverterError:
            # Chapter may not be available, which is acceptable
            pytest.skip("Chapter 205 not available")
