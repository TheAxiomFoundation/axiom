"""Tests for Nevada state statute converter.

Tests the NVConverter which fetches from leg.state.nv.us
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.nv import (
    NV_TAX_CHAPTERS,
    NV_WELFARE_CHAPTERS,
    NVConverter,
    NVConverterError,
    download_nv_chapter,
    fetch_nv_section,
)
from axiom_corpus.models import Section

# Sample HTML from leg.state.nv.us for testing
SAMPLE_CHAPTER_HTML = """<!DOCTYPE html>
<html>
<head>
<meta http-equiv=Content-Type content="text/html; charset=windows-1252">
<title>NRS: CHAPTER 361 - PROPERTY TAX</title>
</head>
<body>
<p class="MsoTitle">CHAPTER 361 - PROPERTY TAX</p>

<p class="COHead1">DEFINITIONS</p>

<p class="COLeadline"><a href="#NRS361Sec010">NRS 361.010</a> Definitions.</p>
<p class="COLeadline"><a href="#NRS361Sec015">NRS 361.015</a> "Bona fide resident" defined.</p>
<p class="COLeadline"><a href="#NRS361Sec020">NRS 361.020</a> "Fiscal year" defined.</p>

<p class="COHead1">GENERAL PROVISIONS</p>

<p class="SectBody"><span class="Empty">     <a name=NRS361Sec010></a>NRS </span><span
class="Section">361.010</span><span class="Empty">  </span><span
class="Leadline">Definitions.</span><span class="Empty">  </span>As used in NRS 361.013 to 361.043, inclusive, unless the context otherwise requires, the words and terms defined in NRS 361.013 to 361.043, inclusive, have the meanings ascribed to them in those sections.</p>

<p class="SourceNote">     [Part 1:344:1953]</p>

<p class="SectBody"><span class="Empty">     <a name=NRS361Sec015></a>NRS </span><span
class="Section">361.015</span><span class="Empty">  </span><span
class="Leadline">"Bona fide resident" defined.</span><span class="Empty">  </span>"Bona
fide resident" means a person who:</p>

<p class="SectBody">     1.  Has established a residence in the
State of Nevada; and</p>

<p class="SectBody">     2.  Has:</p>

<p class="SectBody">     (a) Actually resided in this state for at least 6
months; or</p>

<p class="SectBody">     (b) A valid driver's license or identification
card issued by the Department of Motor Vehicles of this state.</p>

<p class="SourceNote">     [Part 3:344:1953]-(NRS A 2003, 2749; 2011, 3514)</p>

<p class="SectBody"><span class="Empty">     <a name=NRS361Sec020></a>NRS </span><span
class="Section">361.020</span><span class="Empty">  </span><span
class="Leadline">"Fiscal year" defined.</span><span class="Empty">  </span>"Fiscal year" means:</p>

<p class="SectBody">     1.  For the purposes of ad valorem taxation, the period commencing on July 1 of any year and ending on June 30 of the following year.</p>

<p class="SectBody">     2.  For all other purposes, the period determined by the appropriate governing body.</p>

<p class="SourceNote">     [Part 2:344:1953]-(NRS A 1979, 1871)</p>

</body>
</html>
"""

# Sample HTML for welfare chapter (Title 38)
SAMPLE_WELFARE_CHAPTER_HTML = """<!DOCTYPE html>
<html>
<head>
<title>NRS: CHAPTER 422 - HEALTH CARE FINANCING AND POLICY</title>
</head>
<body>
<p class="MsoTitle">CHAPTER 422 - HEALTH CARE FINANCING AND POLICY</p>

<p class="COHead1">GENERAL PROVISIONS</p>

<p class="COLeadline"><a href="#NRS422Sec001">NRS 422.001</a> Definitions.</p>
<p class="COLeadline"><a href="#NRS422Sec050">NRS 422.050</a> Powers and duties of Administrator.</p>

<p class="SectBody"><span class="Empty">     <a name=NRS422Sec001></a>NRS </span><span
class="Section">422.001</span><span class="Empty">  </span><span
class="Leadline">Definitions.</span><span class="Empty">  </span>As used in this chapter, unless the context otherwise requires:</p>

<p class="SectBody">     1.  "Administrator" means the Administrator of the Division of Health Care Financing and Policy.</p>

<p class="SectBody">     2.  "Division" means the Division of Health Care Financing and Policy of the Department.</p>

<p class="SourceNote">     (Added to NRS by 1991, 1883)</p>

<p class="SectBody"><span class="Empty">     <a name=NRS422Sec050></a>NRS </span><span
class="Section">422.050</span><span class="Empty">  </span><span
class="Leadline">Powers and duties of Administrator.</span><span class="Empty">  </span>The Administrator shall:</p>

<p class="SectBody">     1.  Administer and supervise all programs of medical assistance.</p>

<p class="SectBody">     2.  Prepare and submit reports as required by law.</p>

<p class="SourceNote">     [1:65:1967]-(NRS A 1991, 1884)</p>

</body>
</html>
"""

# HTML with section not found
SAMPLE_EMPTY_HTML = """<!DOCTYPE html>
<html>
<head><title>NRS: CHAPTER 999</title></head>
<body>
<p class="MsoTitle">CHAPTER 999</p>
</body>
</html>
"""


class TestNVChaptersRegistry:
    """Test Nevada chapter registries."""

    def test_chapter_361_in_tax_chapters(self):
        """Chapter 361 (Property Tax) is in tax chapters."""
        assert "361" in NV_TAX_CHAPTERS
        assert "Property Tax" in NV_TAX_CHAPTERS["361"]

    def test_chapter_372_in_tax_chapters(self):
        """Chapter 372 (Sales and Use Taxes) is in tax chapters."""
        assert "372" in NV_TAX_CHAPTERS
        assert "Sales" in NV_TAX_CHAPTERS["372"]

    def test_chapter_422_in_welfare_chapters(self):
        """Chapter 422 (Health Care Financing) is in welfare chapters."""
        assert "422" in NV_WELFARE_CHAPTERS
        assert "Health Care" in NV_WELFARE_CHAPTERS["422"]

    def test_chapter_432b_in_welfare_chapters(self):
        """Chapter 432B (Protection of Children) is in welfare chapters."""
        assert "432B" in NV_WELFARE_CHAPTERS
        assert "Children" in NV_WELFARE_CHAPTERS["432B"]

    def test_tax_chapters_in_title_32_range(self):
        """Tax chapters are in Title 32 range (360-377)."""
        for chapter in NV_TAX_CHAPTERS:
            # Extract numeric part
            num = int("".join(c for c in chapter if c.isdigit()))
            assert 360 <= num <= 377, f"Chapter {chapter} outside Title 32 range"


class TestNVConverter:
    """Test NVConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = NVConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = NVConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = NVConverter(year=2024)
        assert converter.year == 2024

    def test_build_chapter_url(self):
        """Build correct URL for chapter."""
        converter = NVConverter()
        url = converter._build_chapter_url("361")
        assert url == "https://www.leg.state.nv.us/NRS/NRS-361.html"

    def test_build_chapter_url_with_letter(self):
        """Build correct URL for chapter with letter suffix."""
        converter = NVConverter()
        url = converter._build_chapter_url("361A")
        assert url == "https://www.leg.state.nv.us/NRS/NRS-361A.html"

    def test_extract_chapter_from_section(self):
        """Extract chapter number from section number."""
        converter = NVConverter()
        assert converter._extract_chapter_from_section("361.010") == "361"
        assert converter._extract_chapter_from_section("361A.100") == "361A"
        assert converter._extract_chapter_from_section("422.001") == "422"

    def test_extract_chapter_invalid(self):
        """Raise error for invalid section number."""
        converter = NVConverter()
        with pytest.raises(ValueError):
            converter._extract_chapter_from_section("invalid")

    def test_get_anchor_name(self):
        """Build correct anchor name for section."""
        converter = NVConverter()
        assert converter._get_anchor_name("361", "010") == "NRS361Sec010"
        assert converter._get_anchor_name("361", "0435") == "NRS361Sec0435"
        assert converter._get_anchor_name("422", "001") == "NRS422Sec001"

    def test_context_manager(self):
        """Converter works as context manager."""
        with NVConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestNVConverterParsing:
    """Test NVConverter HTML parsing."""

    def test_parse_section_definitions(self):
        """Parse definitions section from HTML."""
        converter = NVConverter()
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_CHAPTER_HTML, "html.parser")
        parsed = converter._parse_section_from_soup(
            soup, "361.010", "https://www.leg.state.nv.us/NRS/NRS-361.html"
        )

        assert parsed.section_number == "361.010"
        assert parsed.section_title == "Definitions."
        assert parsed.chapter_number == "361"
        assert parsed.chapter_title == "Property Tax"
        assert parsed.title_number == 32
        assert parsed.title_name == "Revenue and Taxation"
        assert "NRS 361.013" in parsed.text

    def test_parse_section_with_subsections(self):
        """Parse section with numbered and lettered subsections."""
        converter = NVConverter()
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_CHAPTER_HTML, "html.parser")
        parsed = converter._parse_section_from_soup(
            soup, "361.015", "https://www.leg.state.nv.us/NRS/NRS-361.html"
        )

        assert parsed.section_number == "361.015"
        assert "Bona fide resident" in parsed.section_title
        assert "residence" in parsed.text.lower()

        # Should have subsections 1 and 2
        assert len(parsed.subsections) >= 1

    def test_parse_section_fiscal_year(self):
        """Parse fiscal year section with numbered subsections."""
        converter = NVConverter()
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_CHAPTER_HTML, "html.parser")
        parsed = converter._parse_section_from_soup(
            soup, "361.020", "https://www.leg.state.nv.us/NRS/NRS-361.html"
        )

        assert parsed.section_number == "361.020"
        assert "Fiscal year" in parsed.section_title

    def test_parse_history_note(self):
        """Parse history/source note from section."""
        converter = NVConverter()
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_CHAPTER_HTML, "html.parser")
        parsed = converter._parse_section_from_soup(
            soup, "361.015", "https://www.leg.state.nv.us/NRS/NRS-361.html"
        )

        assert parsed.history is not None
        assert "1953" in parsed.history or "2003" in parsed.history

    def test_parse_welfare_section(self):
        """Parse welfare chapter section."""
        converter = NVConverter()
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_WELFARE_CHAPTER_HTML, "html.parser")
        parsed = converter._parse_section_from_soup(
            soup, "422.001", "https://www.leg.state.nv.us/NRS/NRS-422.html"
        )

        assert parsed.section_number == "422.001"
        assert parsed.section_title == "Definitions."
        assert parsed.chapter_number == "422"
        assert parsed.title_number == 38
        assert parsed.title_name == "Public Welfare"
        assert "Administrator" in parsed.text

    def test_to_section_model(self):
        """Convert ParsedNVSection to Section model."""
        converter = NVConverter()
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_CHAPTER_HTML, "html.parser")
        parsed = converter._parse_section_from_soup(
            soup, "361.010", "https://www.leg.state.nv.us/NRS/NRS-361.html"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "NV-361.010"
        assert section.citation.title == 0  # State law indicator
        assert "Definitions" in section.section_title
        assert "Nevada Revised Statutes" in section.title_name
        assert section.uslm_id == "nv/361/361.010"
        assert "NRS361Sec010" in section.source_url

    def test_section_not_found(self):
        """Raise error when section anchor not found."""
        converter = NVConverter()
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(SAMPLE_CHAPTER_HTML, "html.parser")

        with pytest.raises(NVConverterError) as exc_info:
            converter._parse_section_from_soup(
                soup, "361.999", "https://www.leg.state.nv.us/NRS/NRS-361.html"
            )

        assert "not found" in str(exc_info.value).lower()


class TestNVConverterFetching:
    """Test NVConverter HTTP fetching with mocks."""

    @patch.object(NVConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = NVConverter()
        section = converter.fetch_section("361.010")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "NV-361.010"
        assert "Definitions" in section.section_title

    @patch.object(NVConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = SAMPLE_EMPTY_HTML

        converter = NVConverter()
        with pytest.raises(NVConverterError) as exc_info:
            converter.fetch_section("999.999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(NVConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = NVConverter()
        sections = converter.get_chapter_section_numbers("361")

        assert len(sections) == 3
        assert "361.010" in sections
        assert "361.015" in sections
        assert "361.020" in sections

    @patch.object(NVConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = NVConverter()
        sections = list(converter.iter_chapter("361"))

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)

    @patch.object(NVConverter, "_get")
    def test_chapter_caching(self, mock_get):
        """Chapter HTML is cached to avoid redundant requests."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        converter = NVConverter()
        # Fetch two sections from same chapter
        converter.fetch_section("361.010")
        converter.fetch_section("361.015")

        # Should only make one HTTP request (chapter is cached)
        assert mock_get.call_count == 1


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(NVConverter, "_get")
    def test_fetch_nv_section(self, mock_get):
        """Test fetch_nv_section function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        section = fetch_nv_section("361.010")

        assert section is not None
        assert section.citation.section == "NV-361.010"

    @patch.object(NVConverter, "_get")
    def test_download_nv_chapter(self, mock_get):
        """Test download_nv_chapter function."""
        mock_get.return_value = SAMPLE_CHAPTER_HTML

        sections = download_nv_chapter("361")

        assert len(sections) == 3
        assert all(isinstance(s, Section) for s in sections)


class TestSubsectionParsing:
    """Test subsection parsing logic."""

    def test_parse_numbered_subsections(self):
        """Parse numbered subsections 1., 2., etc."""
        converter = NVConverter()

        text = """
        1.  First subsection content here.
        2.  Second subsection content here.
        3.  Third subsection with more detail.
        """

        subsections = converter._parse_subsections(text)

        assert len(subsections) == 3
        assert subsections[0].identifier == "1"
        assert "First subsection" in subsections[0].text
        assert subsections[1].identifier == "2"
        assert subsections[2].identifier == "3"

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under numbered."""
        converter = NVConverter()

        text = """
        1.  First subsection:
        (a) Sub-item A content.
        (b) Sub-item B content.
        2.  Second subsection standalone.
        """

        subsections = converter._parse_subsections(text)

        assert len(subsections) == 2
        assert subsections[0].identifier == "1"
        assert len(subsections[0].children) >= 2
        assert subsections[0].children[0].identifier == "a"
        assert subsections[0].children[1].identifier == "b"

    def test_parse_deeply_nested_subsections(self):
        """Parse three-level nesting with (1), (2) inside (a)."""
        converter = NVConverter()

        text = """
        1.  First:
        (a) Item with sub-items:
        (1) Numeric sub one.
        (2) Numeric sub two.
        (b) Another item.
        """

        subsections = converter._parse_subsections(text)

        assert len(subsections) == 1
        assert subsections[0].identifier == "1"
        assert len(subsections[0].children) >= 2

        # Check level 3 under (a)
        child_a = subsections[0].children[0]
        assert child_a.identifier == "a"
        assert len(child_a.children) >= 2
        assert child_a.children[0].identifier == "1"
        assert child_a.children[1].identifier == "2"


class TestNVConverterIntegration:
    """Integration tests that hit real leg.state.nv.us (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_property_tax_section(self):
        """Fetch Nevada Property Tax section 361.010."""
        converter = NVConverter()
        section = converter.fetch_section("361.010")

        assert section is not None
        assert section.citation.section == "NV-361.010"
        assert "definition" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch Nevada Sales Tax section 372.105."""
        converter = NVConverter()
        try:
            section = converter.fetch_section("372.105")
            assert section is not None
            assert section.citation.section == "NV-372.105"
        except NVConverterError:
            pytest.skip("Section 372.105 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_361_sections(self):
        """Get list of sections in Chapter 361."""
        converter = NVConverter()
        sections = converter.get_chapter_section_numbers("361")

        assert len(sections) > 0
        assert all(s.startswith("361.") for s in sections)
        assert "361.010" in sections

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Nevada welfare section 422.001."""
        converter = NVConverter()
        try:
            section = converter.fetch_section("422.001")
            assert section.citation.section == "NV-422.001"
            assert "health" in section.title_name.lower() or "welfare" in section.title_name.lower()
        except NVConverterError:
            pytest.skip("Section 422.001 not found")
