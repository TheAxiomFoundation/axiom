"""Tests for Rhode Island state statute converter.

Tests the RIConverter which fetches from rilegislature.gov
and converts to the internal Section model.
"""

from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.ri import (
    RI_TAX_CHAPTERS,
    RI_WELFARE_CHAPTERS,
    RIConverter,
    RIConverterError,
    download_ri_chapter,
    fetch_ri_section,
)
from axiom_corpus.models import Section

# Sample HTML from rilegislature.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>R.I. Gen. Laws § 44-30-1</title></head>
<body>
<h1>Title 44 Taxation</h1>
<h2>Chapter 30 Personal Income Tax</h2>
<h3>Part I General</h3>
<h4>R.I. Gen. Laws § 44-30-1</h4>
<h5>§ 44-30-1. Persons subject to tax.</h5>
<div class="statute">
<p><strong>(a) Imposition of tax.</strong> A Rhode Island personal income tax is hereby imposed on the Rhode Island income of every individual, estate, and trust.</p>
<p><strong>(b) Partners and partnerships.</strong> A partnership as such shall not be subject to the taxes imposed by this chapter. Persons carrying on business as partners shall be liable for the tax imposed by this chapter only in their separate or individual capacities.</p>
<p><strong>(c) Associations taxable as corporations.</strong> An association which is taxable as a corporation under the laws of the United States shall not be subject to the tax imposed by this chapter.</p>
<p><strong>(d) Exempt trusts and organizations.</strong> A trust or other unincorporated organization which by reason of its purposes or activities is exempt from federal income tax shall be exempt from the tax imposed by this chapter.</p>
<p><strong>(e) Cross references.</strong></p>
<p>(1) For tax on residents, see § 44-30-12.</p>
<p>(2) For tax on nonresidents, see § 44-30-16.</p>
<p>(3) For tax on estates and trusts, see § 44-30-32.</p>
<p>(4) For withholding, see § 44-30-35.</p>
<p>History of Section. P.L. 1971, ch. 8, art. 1, § 1; P.L. 2006, ch. 246, art. 38, § 10.</p>
</div>
</body>
</html>
"""

SAMPLE_SIMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>R.I. Gen. Laws § 44-1-1</title></head>
<body>
<h1>Title 44 Taxation</h1>
<h2>Chapter 1 State Tax Officials</h2>
<h4>R.I. Gen. Laws § 44-1-1</h4>
<h5>§ 44-1-1. Tax administrator — Appointment.</h5>
<div class="statute">
<p>There shall be a tax administrator within the department of revenue appointed by the director of revenue with the approval of the governor.</p>
<p>History of Section. P.L. 1939, ch. 660, § 70; impl. am. P.L. 1951, ch. 2727, art. 1, § 3; G.L. 1956, § 44-1-1; P.L. 2006, ch. 246, art. 38, § 10.</p>
</div>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>R.I. Gen. Laws § 40-1-1</title></head>
<body>
<h1>Title 40 Human Services</h1>
<h2>Chapter 1 Department of Human Services</h2>
<h4>R.I. Gen. Laws § 40-1-1</h4>
<h5>§ 40-1-1. Advisory council — Appointment of members.</h5>
<div class="statute">
<p>(a) There shall be an advisory council to the department of human services to consist of fifteen (15) members appointed by the governor with the advice and consent of the senate.</p>
<p>(b) The members shall be appointed for terms of three (3) years and until their successors are appointed and qualified.</p>
<p>(c) The council shall advise the department on matters relating to policies, programs, and services.</p>
<p>History of Section. P.L. 1939, ch. 660, § 1; G.L. 1956, § 40-1-1; P.L. 1997, ch. 326, § 1.</p>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Chapter 44-1 Index</title></head>
<body>
<h1>Chapter 44-1 - State Tax Officials</h1>
<h3>Index of Sections</h3>
<ul>
<li><a href="44-1-1.htm">§ 44-1-1. Tax administrator — Appointment</a></li>
<li><a href="44-1-2.htm">§ 44-1-2. Deputy tax administrator</a></li>
<li><a href="44-1-3.htm">§ 44-1-3. Powers and duties</a></li>
<li><a href="44-1-4.htm">§ 44-1-4. Rules and regulations</a></li>
</ul>
</body>
</html>
"""


class TestRIChaptersRegistry:
    """Test Rhode Island chapter registries."""

    def test_chapter_44_30_in_tax_chapters(self):
        """Chapter 44-30 (Personal Income Tax) is in tax chapters."""
        assert "44-30" in RI_TAX_CHAPTERS
        assert "Personal Income Tax" in RI_TAX_CHAPTERS["44-30"]

    def test_chapter_44_18_in_tax_chapters(self):
        """Chapter 44-18 (Sales Tax) is in tax chapters."""
        assert "44-18" in RI_TAX_CHAPTERS
        assert "Sales" in RI_TAX_CHAPTERS["44-18"]

    def test_chapter_40_1_in_welfare_chapters(self):
        """Chapter 40-1 (Department of Human Services) is in welfare chapters."""
        assert "40-1" in RI_WELFARE_CHAPTERS
        assert "Human Services" in RI_WELFARE_CHAPTERS["40-1"]

    def test_chapter_40_6_3_in_welfare_chapters(self):
        """Chapter 40-6.3 (RI Works Program) is in welfare chapters."""
        assert "40-6.3" in RI_WELFARE_CHAPTERS
        assert "Works" in RI_WELFARE_CHAPTERS["40-6.3"]

    def test_tax_chapters_start_with_44(self):
        """All tax chapters start with title 44."""
        for chapter in RI_TAX_CHAPTERS:
            assert chapter.startswith("44-")

    def test_welfare_chapters_start_with_40(self):
        """All welfare chapters start with title 40."""
        for chapter in RI_WELFARE_CHAPTERS:
            assert chapter.startswith("40-")


class TestRIConverter:
    """Test RIConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = RIConverter()
        assert converter.rate_limit_delay == 0.5

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = RIConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_extract_title_from_section(self):
        """Extract title number from section number."""
        converter = RIConverter()
        assert converter._extract_title_from_section("44-30-1") == 44
        assert converter._extract_title_from_section("40-1-1") == 40

    def test_extract_chapter_from_section(self):
        """Extract chapter number from section number."""
        converter = RIConverter()
        assert converter._extract_chapter_from_section("44-30-1") == "44-30"
        assert converter._extract_chapter_from_section("40-1-1") == "40-1"
        assert converter._extract_chapter_from_section("44-6.3-1") == "44-6.3"  # decimal chapters preserved

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = RIConverter()
        url = converter._build_section_url("44-30-1")
        assert "rilegislature.gov/Statutes" in url
        assert "TITLE44" in url
        assert "44-30" in url
        assert "44-30-1.htm" in url

    def test_build_chapter_index_url(self):
        """Build correct URL for chapter index."""
        converter = RIConverter()
        url = converter._build_chapter_index_url("44-30")
        assert "TITLE44/44-30/INDEX.htm" in url

    def test_build_title_index_url(self):
        """Build correct URL for title index."""
        converter = RIConverter()
        url = converter._build_title_index_url(44)
        assert "TITLE44/INDEX.HTM" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with RIConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestRIConverterParsing:
    """Test RIConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedRISection."""
        converter = RIConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "44-30-1", "https://example.com")

        assert parsed.section_number == "44-30-1"
        assert parsed.section_title == "Persons subject to tax"
        assert parsed.title_number == 44
        assert parsed.title_name == "Taxation"
        assert parsed.chapter_number == "44-30"
        assert "Personal Income Tax" in (parsed.chapter_title or "")
        assert "Rhode Island personal income tax" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_simple_section(self):
        """Parse a simpler section without nested subsections."""
        converter = RIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SIMPLE_SECTION_HTML, "44-1-1", "https://example.com"
        )

        assert parsed.section_number == "44-1-1"
        assert "Tax administrator" in parsed.section_title
        # Note: The title parsing captures text before the em-dash
        assert "tax administrator" in parsed.text.lower()

    def test_parse_welfare_section(self):
        """Parse a human services section."""
        converter = RIConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "40-1-1", "https://example.com"
        )

        assert parsed.section_number == "40-1-1"
        assert parsed.title_number == 40
        assert parsed.title_name == "Human Services"
        assert "advisory council" in parsed.text.lower()

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = RIConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "44-30-1", "https://example.com")

        # Should have subsections (a), (b), (c), (d), (e)
        assert len(parsed.subsections) >= 4
        assert any(s.identifier == "a" for s in parsed.subsections)
        assert any(s.identifier == "b" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under (e)."""
        converter = RIConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "44-30-1", "https://example.com")

        # Find subsection (e) which has cross references
        sub_e = next((s for s in parsed.subsections if s.identifier == "e"), None)
        assert sub_e is not None
        # Should have children (1), (2), (3), (4)
        assert len(sub_e.children) >= 3

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = RIConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "44-30-1", "https://example.com")

        assert parsed.history is not None
        assert "P.L. 1971" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedRISection to Section model."""
        converter = RIConverter()
        parsed = converter._parse_section_html(SAMPLE_SECTION_HTML, "44-30-1", "https://example.com")
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "RI-44-30-1"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Persons subject to tax"
        assert "Rhode Island General Laws" in section.title_name
        assert section.uslm_id == "ri/44/44-30-1"
        assert section.source_url == "https://example.com"


class TestRIConverterFetching:
    """Test RIConverter HTTP fetching with mocks."""

    @patch.object(RIConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = RIConverter()
        section = converter.fetch_section("44-30-1")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "RI-44-30-1"
        assert "Persons subject to tax" in section.section_title

    @patch.object(RIConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>404 Not Found</body></html>"

        converter = RIConverter()
        with pytest.raises(RIConverterError) as exc_info:
            converter.fetch_section("99-99-99")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(RIConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = RIConverter()
        sections = converter.get_chapter_section_numbers("44-1")

        assert len(sections) == 4
        assert "44-1-1" in sections
        assert "44-1-2" in sections
        assert "44-1-3" in sections

    @patch.object(RIConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SIMPLE_SECTION_HTML,
            SAMPLE_SIMPLE_SECTION_HTML,
            SAMPLE_SIMPLE_SECTION_HTML,
            SAMPLE_SIMPLE_SECTION_HTML,
        ]

        converter = RIConverter()
        sections = list(converter.iter_chapter("44-1"))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(RIConverter, "_get")
    def test_fetch_ri_section(self, mock_get):
        """Test fetch_ri_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_ri_section("44-30-1")

        assert section is not None
        assert section.citation.section == "RI-44-30-1"

    @patch.object(RIConverter, "_get")
    def test_download_ri_chapter(self, mock_get):
        """Test download_ri_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SIMPLE_SECTION_HTML,
            SAMPLE_SIMPLE_SECTION_HTML,
            SAMPLE_SIMPLE_SECTION_HTML,
            SAMPLE_SIMPLE_SECTION_HTML,
        ]

        sections = download_ri_chapter("44-1")

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestRIConverterIntegration:
    """Integration tests that hit real rilegislature.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_personal_income_tax_section(self):
        """Fetch Rhode Island Personal Income Tax section 44-30-1."""
        converter = RIConverter()
        section = converter.fetch_section("44-30-1")

        assert section is not None
        assert section.citation.section == "RI-44-30-1"
        assert "income" in section.text.lower() or "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_admin_section(self):
        """Fetch Rhode Island Tax Administrator section 44-1-1."""
        converter = RIConverter()
        section = converter.fetch_section("44-1-1")

        assert section is not None
        assert section.citation.section == "RI-44-1-1"
        assert "tax administrator" in section.text.lower() or "department" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_human_services_section(self):
        """Fetch Rhode Island Human Services section 40-1-1."""
        converter = RIConverter()
        section = converter.fetch_section("40-1-1")

        assert section is not None
        assert section.citation.section == "RI-40-1-1"

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_44_1_sections(self):
        """Get list of sections in Chapter 44-1."""
        converter = RIConverter()
        sections = converter.get_chapter_section_numbers("44-1")

        assert len(sections) > 0
        assert all(s.startswith("44-1-") for s in sections)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_44_chapters(self):
        """Get list of chapters in Title 44."""
        converter = RIConverter()
        chapters = converter.get_title_chapters(44)

        assert len(chapters) > 0
        assert all(c.startswith("44-") for c in chapters)
        assert "44-30" in chapters  # Personal Income Tax
