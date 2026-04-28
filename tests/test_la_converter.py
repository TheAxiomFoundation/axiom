"""Tests for Louisiana state statute converter.

Tests the LAConverter which fetches from legis.la.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.la import (
    LA_KNOWN_DOC_IDS,
    LA_TAX_SECTIONS,
    LA_TITLES,
    LA_WELFARE_SECTIONS,
    LAConverter,
    LAConverterError,
    fetch_la_section,
)
from axiom.models import Section

# Sample HTML from legis.la.gov for testing (based on RS 47:287.445)
# Note: The actual website returns HTML with newlines between paragraph elements
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head><title>Louisiana Laws - Louisiana State Legislature</title></head>
<body>
<form name="aspnetForm" method="post" action="./Law.aspx?d=101682">
<div id="ctl00_PageBody_UpdatePanelLawDoc">
    <div id="ctl00_PageBody_divLaw" style="margin:30px 50px 0px 250px;">
        <div style="text-align:center;">
            <span id="ctl00_PageBody_LabelName" class="title" style="font-size:Large;">RS 47:287.445</span>
        </div>
        <br />
        <input type="hidden" name="ctl00$PageBody$HiddenDocId" value="101682" />
        <div style="padding:10px;">
           <span id="ctl00_PageBody_LabelDocument">
<P class=A0001 align=justify>&#167;287.445. Special adjustment for long-term contracts</P>
<P class=A0002 align=justify>A. General. Notwithstanding any provision to the contrary in this Chapter, any corporation that uses the percentage of completion method prescribed in 26 U.S.C.A. &#167;460 shall upon completion of the contract, pay or shall be entitled to receive interest computed under the look-back method of Subsection B.</P>
<P class=A0002 align=justify>B. Look-back method. The interest computed under the look-back method of this Subsection shall be determined as follows:</P>
<P class=A0002 align=justify>(1) First, allocating income under the contract among taxable years in accordance with the provisions of 26 U.S.C.A. &#167;460(b)(2)(A).</P>
<P class=A0002 align=justify>(2) Second, determine solely for purposes of computing such interest, the overpayment or underpayment of Louisiana corporate income tax for each taxable year.</P>
<P class=A0002 align=justify>(3) Then, applying the rate of interest established by R.S. 47:1624 to the overpayment or underpayment determined under Paragraph (2).</P>
<P class=A0002 align=justify>C. S corporations. With respect to a corporation which for a taxable year is classified as an S corporation, the principles of I.R.C. Section 460(b)(4)(A) shall apply.</P>
<P class=A0002 align=justify>Acts 1992, No. 588, &#167;1; Acts 2002, No. 51, &#167;1, eff. Jan. 1, 2003.</P>
<P class=A0003 align=justify><BR></P></span>
        </div>
    </div>
</div>
</form>
</body>
</html>
"""

# Simple section with no subsections
SAMPLE_SIMPLE_HTML = """<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head><title>Louisiana Laws</title></head>
<body>
<form name="aspnetForm" method="post" action="./Law.aspx?d=101478">
<div id="ctl00_PageBody_UpdatePanelLawDoc">
    <div id="ctl00_PageBody_divLaw">
        <span id="ctl00_PageBody_LabelName">RS 47:2061</span>
        <span id="ctl00_PageBody_LabelDocument">
<P>&#167;2061. Deputy tax collectors</P>
<P>The tax collector of any parish may appoint one or more deputies to assist him in the performance of his duties. Each deputy shall give bond in the amount fixed by the tax collector.</P>
<P>H.C.R. No. 88, 1993 R.S., eff. May 30, 1993.</P>
</span>
    </div>
</div>
</form>
</body>
</html>
"""

# Section with numeric subsections (1), (2)
SAMPLE_NUMERIC_HTML = """<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head><title>Louisiana Laws</title></head>
<body>
<form name="aspnetForm" method="post" action="./Law.aspx?d=99999">
<div id="ctl00_PageBody_UpdatePanelLawDoc">
    <div id="ctl00_PageBody_divLaw">
        <span id="ctl00_PageBody_LabelName">RS 46:231</span>
        <span id="ctl00_PageBody_LabelDocument">
<P>&#167;231. Louisiana Workforce Commission</P>
<P>(1) The Louisiana Workforce Commission is hereby created within the executive branch of state government.</P>
<P>(2) The commission shall administer all workforce development programs as provided by law.</P>
<P>(a) This includes job training programs.</P>
<P>(b) This includes employment services.</P>
<P>Acts 2008, No. 743, &#167;1.</P>
</span>
    </div>
</div>
</form>
</body>
</html>
"""


class TestLATitlesRegistry:
    """Test Louisiana title registries."""

    def test_title_47_in_titles(self):
        """Title 47 (Revenue and Taxation) is in titles."""
        assert 47 in LA_TITLES
        assert "Revenue" in LA_TITLES[47] or "Tax" in LA_TITLES[47]

    def test_title_46_in_titles(self):
        """Title 46 (Public Welfare) is in titles."""
        assert 46 in LA_TITLES
        assert "Welfare" in LA_TITLES[46] or "Public" in LA_TITLES[46]

    def test_tax_sections_exist(self):
        """Tax sections registry has entries."""
        assert len(LA_TAX_SECTIONS) > 0
        assert "47:32" in LA_TAX_SECTIONS or any("32" in k for k in LA_TAX_SECTIONS)

    def test_welfare_sections_exist(self):
        """Welfare sections registry has entries."""
        assert len(LA_WELFARE_SECTIONS) > 0


class TestLAConverter:
    """Test LAConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = LAConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = LAConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = LAConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = LAConverter()
        url = converter._build_section_url(101682)
        assert "legis.la.gov/legis" in url
        assert "Law.aspx" in url
        assert "d=101682" in url

    def test_context_manager(self):
        """Converter works as context manager."""
        with LAConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestLAConverterParsing:
    """Test LAConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedLASection."""
        converter = LAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 101682, "https://legis.la.gov/legis/Law.aspx?d=101682"
        )

        assert parsed.doc_id == 101682
        assert parsed.citation == "RS 47:287.445"
        assert parsed.title_number == 47
        assert parsed.section_number == "287.445"
        assert parsed.section_title == "Special adjustment for long-term contracts"
        assert parsed.source_url == "https://legis.la.gov/legis/Law.aspx?d=101682"

    def test_parse_citation(self):
        """Parse citation from section HTML."""
        converter = LAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 101682, "https://example.com"
        )

        assert parsed.citation == "RS 47:287.445"
        assert parsed.title_number == 47
        assert parsed.section_number == "287.445"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = LAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 101682, "https://example.com"
        )

        # Should have subsections A, B, C
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "A" for s in parsed.subsections)
        assert any(s.identifier == "B" for s in parsed.subsections)
        assert any(s.identifier == "C" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under B."""
        converter = LAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 101682, "https://example.com"
        )

        # Find subsection B
        sub_b = next((s for s in parsed.subsections if s.identifier == "B"), None)
        assert sub_b is not None

        # B should have children (1), (2), (3) parsed from the text content
        assert len(sub_b.children) >= 3, f"Expected >= 3 children, got {len(sub_b.children)}: {sub_b}"
        assert any(c.identifier == "1" for c in sub_b.children)
        assert any(c.identifier == "2" for c in sub_b.children)
        assert any(c.identifier == "3" for c in sub_b.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = LAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 101682, "https://example.com"
        )

        assert parsed.history is not None
        assert "Acts 1992" in parsed.history
        assert "No. 588" in parsed.history

    def test_parse_simple_section(self):
        """Parse simple section with no subsections."""
        converter = LAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SIMPLE_HTML, 101478, "https://example.com"
        )

        assert parsed.citation == "RS 47:2061"
        assert parsed.title_number == 47
        assert parsed.section_number == "2061"
        assert "Deputy tax collectors" in parsed.section_title

    def test_parse_numeric_subsections(self):
        """Parse sections with (1), (2) subsections."""
        converter = LAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_NUMERIC_HTML, 99999, "https://example.com"
        )

        assert parsed.citation == "RS 46:231"
        assert parsed.title_number == 46
        # Should have subsections (1) and (2)
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)

    def test_to_section_model(self):
        """Convert ParsedLASection to Section model."""
        converter = LAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, 101682, "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "LA-47:287.445"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Special adjustment for long-term contracts"
        assert "Louisiana Revised Statutes" in section.title_name
        assert "Revenue" in section.title_name
        assert section.uslm_id == "la/47/287.445"
        assert section.source_url == "https://example.com"


class TestLAConverterFetching:
    """Test LAConverter HTTP fetching with mocks."""

    @patch.object(LAConverter, "_get")
    def test_fetch_section_by_id(self, mock_get):
        """Fetch and parse a single section by document ID."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = LAConverter()
        section = converter.fetch_section_by_id(101682)

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "LA-47:287.445"
        assert "Special adjustment" in section.section_title

    @patch.object(LAConverter, "_get")
    def test_fetch_section_requires_doc_id(self, mock_get):
        """fetch_section raises error if doc_id not provided."""
        converter = LAConverter()
        with pytest.raises(LAConverterError) as exc_info:
            converter.fetch_section("47:287.445")

        assert "Document ID required" in str(exc_info.value)

    @patch.object(LAConverter, "_get")
    def test_fetch_section_with_doc_id(self, mock_get):
        """fetch_section works when doc_id provided."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = LAConverter()
        section = converter.fetch_section("47:287.445", doc_id=101682)

        assert section is not None
        assert section.citation.section == "LA-47:287.445"


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(LAConverter, "_get")
    def test_fetch_la_section(self, mock_get):
        """Test fetch_la_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_la_section(101682)

        assert section is not None
        assert section.citation.section == "LA-47:287.445"


class TestKnownDocIDs:
    """Test the known document IDs registry."""

    def test_known_doc_ids_exist(self):
        """Known document IDs registry has entries."""
        assert len(LA_KNOWN_DOC_IDS) > 0
        assert "47:287.445" in LA_KNOWN_DOC_IDS

    def test_known_doc_id_value(self):
        """Known document ID has correct value."""
        assert LA_KNOWN_DOC_IDS["47:287.445"] == 101682


class TestLAConverterIntegration:
    """Integration tests that hit real legis.la.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_section(self):
        """Fetch Louisiana tax section RS 47:287.445."""
        converter = LAConverter()
        section = converter.fetch_section_by_id(101682)

        assert section is not None
        assert section.citation.section == "LA-47:287.445"
        assert "long-term contracts" in section.section_title.lower()
        assert "corporation" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_deputy_collectors(self):
        """Fetch Louisiana section RS 47:2061."""
        converter = LAConverter()
        section = converter.fetch_section_by_id(101478)

        assert section is not None
        assert "47:2061" in section.citation.section
        assert "tax collector" in section.text.lower() or "deputy" in section.text.lower()
