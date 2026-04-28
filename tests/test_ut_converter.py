"""Tests for Utah state statute converter.

Tests the UTConverter which fetches from le.utah.gov (Utah State Legislature)
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom_corpus.converters.us_states.ut import (
    UT_ALPHA_TITLES,
    UT_TAX_CHAPTERS,
    UT_TITLES,
    UT_WELFARE_CHAPTERS,
    UTConverter,
    UTConverterError,
    download_ut_chapter,
    fetch_ut_section,
)
from axiom_corpus.models import Section

# Sample wrapper HTML that contains version info
SAMPLE_WRAPPER_HTML = """<!DOCTYPE html>
<html lang="en-US">
<head>
<title>Utah Code Section 59-10-104</title>
<script type="text/javascript">
var affectURL = "59-10-S104_bills.html";
var versionArr = [
['C59-10-S104_2025010120250507.html','Current Version','C59-10-S104_2025010120250507']
];
var versionDefault="C59-10-S104_2025010120250507";
</script>
</head>
<body>
<div id="content">Loading...</div>
</body>
</html>
"""

# Sample content HTML from le.utah.gov for testing
SAMPLE_SECTION_HTML = """<html>
<head>
<META http-equiv="Content-Type" content="text/html; charset=UTF-8">
</head>
<body style="margin-left:50px; margin-right:50px; margin-top:50px; margin-bottom:50px">
<div id="content">
<ul id="breadcrumb">
<li><a href="/">Home</a></li>
<li><a href="../../code.html">Utah Code</a></li>
<li><a href="../../Title59/59.html">Title 59</a></li>
<li><a href="../../Title59/Chapter10/59-10.html">Chapter 10</a></li>
<li><a href="../../Title59/Chapter10/59-10-P1.html">Part 1</a></li>
<li>Section 104</li>
</ul>
<h3 class="heading">Title 59 Chapter 10 Part 1 Section 104</h3>
<table id="parenttbl">
<tr>
<td style="text-align:right; font-weight:bold"><a href="../../code.html">Index</a></td>
<td style="font-weight:bold">Utah Code</td>
</tr>
<tr>
<td style="text-align:right; font-weight:bold"><a href="../../Title59/59.html">Title 59</a></td>
<td style="font-weight:bold">Revenue and Taxation</td>
</tr>
<tr>
<td style="text-align:right; font-weight:bold"><a href="../../Title59/Chapter10/59-10.html">Chapter 10</a></td>
<td style="font-weight:bold">Individual Income Tax Act</td>
</tr>
<tr>
<td style="text-align:right; font-weight:bold"><a href="../../Title59/Chapter10/59-10-P1.html">Part 1</a></td>
<td style="font-weight:bold">Determination and Reporting of Tax Liability and Information</td>
</tr>
<tr>
<td style="text-align:right; font-weight:bold">Section 104</td>
<td style="font-weight:bold">Tax basis -- Tax rate -- Exemption.</td>
</tr>
</table>
<hr>
<br>
<div id="secdiv">
<b><i>Effective 1/1/2025</i></b>
<br>
<b>59-10-104.&nbsp;</b><b>Tax basis -- Tax rate -- Exemption.</b>
<br>
<a id="59-10-104(1)" name="59-10-104(1)"></a>
<table width="100%">
<tr>
<td style="vertical-align:text-top">(1)</td>
<td style="width:99%">A tax is imposed on the state taxable income of a resident individual as provided in this section.</td>
</tr>
</table>
<a id="59-10-104(2)" name="59-10-104(2)"></a>
<table width="100%">
<tr>
<td style="vertical-align:text-top">(2)</td>
<td style="width:99%">For purposes of Subsection (1), for a taxable year, the tax is an amount equal to the product of:
<a id="59-10-104(2)(a)" name="59-10-104(2)(a)"></a>
<table width="100%">
<tr>
<td style="vertical-align:text-top">(a)</td>
<td style="width:99%">the resident individual's state taxable income for that taxable year; and</td>
</tr>
</table>
<a id="59-10-104(2)(b)" name="59-10-104(2)(b)"></a>
<table width="100%">
<tr>
<td style="vertical-align:text-top">(b)</td>
<td style="width:99%">4.5%.</td>
</tr>
</table>
</td>
</tr>
</table>
<a id="59-10-104(3)" name="59-10-104(3)"></a>
<table width="100%">
<tr>
<td style="vertical-align:text-top">(3)</td>
<td style="width:99%">This section does not apply to a resident individual exempt from taxation under Section 59-10-104.1.</td>
</tr>
</table>
<br>
<br>Amended by Chapter 407, 2025 General Session<br>
</div>
</div>
</body>
</html>
"""

SAMPLE_PART_INDEX_HTML = """<html>
<head>
<META http-equiv="Content-Type" content="text/html; charset=UTF-8">
</head>
<body>
<div id="content">
<ul id="breadcrumb">
<li><a href="/">Home</a></li>
<li><a href="../../code.html">Utah Code</a></li>
<li><a href="../../Title59/59.html">Title 59</a></li>
<li><a href="../../Title59/Chapter10/59-10.html">Chapter 10</a></li>
<li>Part 1</li>
</ul>
<h3 class="heading">Title 59 Chapter 10 Part 1</h3>
<table id="parenttbl">
<tr>
<td style="text-align:right; font-weight:bold">Part 1</td>
<td style="font-weight:bold">Determination and Reporting of Tax Liability and Information</td>
</tr>
</table>
<hr>
<table id="childtbl">
<tr>
<td style="vertical-align:text-top; white-space: nowrap"><a href="../../Title59/Chapter10/59-10-S103.html?v=C59-10-S103_2025050720250507">Section 103</a></td>
<td>Definitions.</td>
</tr>
<tr>
<td style="vertical-align:text-top; white-space: nowrap"><a href="../../Title59/Chapter10/59-10-S103.1.html?v=C59-10-S103.1_2025050720250507">Section 103.1</a></td>
<td>Information to be contained on individual income tax returns or booklets.</td>
</tr>
<tr>
<td style="vertical-align:text-top; white-space: nowrap"><a href="../../Title59/Chapter10/59-10-S104.html?v=C59-10-S104_2025010120250507">Section 104</a></td>
<td>Tax basis -- Tax rate -- Exemption.</td>
</tr>
<tr>
<td style="vertical-align:text-top; white-space: nowrap"><a href="../../Title59/Chapter10/59-10-S104.1.html?v=C59-10-S104.1_1800010118000101">Section 104.1</a></td>
<td>Exemption from taxation.</td>
</tr>
</table>
<hr>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<html>
<head>
<META http-equiv="Content-Type" content="text/html; charset=UTF-8">
</head>
<body>
<div id="content">
<ul id="breadcrumb">
<li><a href="/">Home</a></li>
<li><a href="../../code.html">Utah Code</a></li>
<li><a href="../../Title59/59.html">Title 59</a></li>
<li>Chapter 10</li>
</ul>
<h3 class="heading">Title 59 Chapter 10</h3>
<table id="parenttbl">
<tr>
<td style="text-align:right; font-weight:bold">Chapter 10</td>
<td style="font-weight:bold">Individual Income Tax Act</td>
</tr>
</table>
<hr>
<table id="childtbl">
<tr>
<td style="vertical-align:text-top; white-space: nowrap"><a href="../../Title59/Chapter10/59-10-P1.html?v=C59-10-P1_1800010118000101">Part 1</a></td>
<td>Determination and Reporting of Tax Liability and Information</td>
</tr>
<tr>
<td style="vertical-align:text-top; white-space: nowrap"><a href="../../Title59/Chapter10/59-10-P2.html?v=C59-10-P2_1800010118000101">Part 2</a></td>
<td>Trusts and Estates</td>
</tr>
<tr>
<td style="vertical-align:text-top; white-space: nowrap"><a href="../../Title59/Chapter10/59-10-P10.html?v=C59-10-P10_1800010118000101">Part 10</a></td>
<td>Nonrefundable Tax Credit Act</td>
</tr>
</table>
<hr>
</div>
</body>
</html>
"""

# Sample for alphanumeric title (35A - Workforce Services)
SAMPLE_35A_SECTION_HTML = """<html>
<head>
<META http-equiv="Content-Type" content="text/html; charset=UTF-8">
</head>
<body>
<div id="content">
<ul id="breadcrumb">
<li><a href="/">Home</a></li>
<li><a href="../../code.html">Utah Code</a></li>
<li><a href="../../Title35A/35A.html">Title 35A</a></li>
<li><a href="../../Title35A/Chapter3/35A-3.html">Chapter 3</a></li>
<li><a href="../../Title35A/Chapter3/35A-3-P3.html">Part 3</a></li>
<li>Section 302</li>
</ul>
<table id="parenttbl">
<tr>
<td style="text-align:right; font-weight:bold">Title 35A</td>
<td style="font-weight:bold">Utah Workforce Services Code</td>
</tr>
<tr>
<td style="text-align:right; font-weight:bold">Chapter 3</td>
<td style="font-weight:bold">Employment Support Act</td>
</tr>
<tr>
<td style="text-align:right; font-weight:bold">Part 3</td>
<td style="font-weight:bold">General Assistance</td>
</tr>
<tr>
<td style="text-align:right; font-weight:bold">Section 302</td>
<td style="font-weight:bold">General assistance -- Eligibility.</td>
</tr>
</table>
<hr>
<div id="secdiv">
<b><i>Effective 5/14/2019</i></b>
<br>
<b>35A-3-302.&nbsp;</b><b>General assistance -- Eligibility.</b>
<br>
<a id="35A-3-302(1)" name="35A-3-302(1)"></a>
<table width="100%">
<tr>
<td style="vertical-align:text-top">(1)</td>
<td style="width:99%">General assistance may be provided to an individual who is not eligible for other assistance programs.</td>
</tr>
</table>
<br>
<br>Enacted by Chapter 174, 2019 General Session<br>
</div>
</div>
</body>
</html>
"""


class TestUTChaptersRegistry:
    """Test Utah chapter registries."""

    def test_title_59_in_titles(self):
        """Title 59 (Revenue and Taxation) is in titles."""
        assert 59 in UT_TITLES
        assert "Revenue" in UT_TITLES[59]

    def test_chapter_59_10_in_tax_chapters(self):
        """Chapter 59-10 (Individual Income Tax) is in tax chapters."""
        assert "59-10" in UT_TAX_CHAPTERS
        assert "Individual Income Tax" in UT_TAX_CHAPTERS["59-10"]

    def test_chapter_59_12_in_tax_chapters(self):
        """Chapter 59-12 (Sales Tax) is in tax chapters."""
        assert "59-12" in UT_TAX_CHAPTERS
        assert "Sales" in UT_TAX_CHAPTERS["59-12"]

    def test_chapter_35A_3_in_welfare_chapters(self):
        """Chapter 35A-3 (Employment Support Act) is in welfare chapters."""
        assert "35A-3" in UT_WELFARE_CHAPTERS
        assert "Employment Support" in UT_WELFARE_CHAPTERS["35A-3"]

    def test_alpha_title_35A(self):
        """Title 35A is in alpha titles."""
        assert "35A" in UT_ALPHA_TITLES
        assert "Workforce" in UT_ALPHA_TITLES["35A"]


class TestUTConverter:
    """Test UTConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = UTConverter()
        assert converter.rate_limit_delay == 0.5

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = UTConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_parse_section_number(self):
        """Parse section number into components."""
        converter = UTConverter()

        # Standard numeric title
        title, chapter, section = converter._parse_section_number("59-10-104")
        assert title == "59"
        assert chapter == "10"
        assert section == "104"

        # Alphanumeric title
        title, chapter, section = converter._parse_section_number("35A-3-302")
        assert title == "35A"
        assert chapter == "3"
        assert section == "302"

        # Section with decimal
        title, chapter, section = converter._parse_section_number("59-10-104.1")
        assert title == "59"
        assert chapter == "10"
        assert section == "104.1"

    def test_parse_section_number_invalid(self):
        """Invalid section numbers raise error."""
        converter = UTConverter()

        with pytest.raises(UTConverterError):
            converter._parse_section_number("invalid")

        with pytest.raises(UTConverterError):
            converter._parse_section_number("59-10")  # Missing section

    def test_build_section_wrapper_url(self):
        """Build correct URL for section wrapper page."""
        converter = UTConverter()

        url = converter._build_section_wrapper_url("59-10-104")
        assert "le.utah.gov/xcode" in url
        assert "Title59/Chapter10/59-10-S104.html" in url

        url = converter._build_section_wrapper_url("35A-3-302")
        assert "Title35A/Chapter3/35A-3-S302.html" in url

    def test_build_section_content_url(self):
        """Build correct URL for section content page."""
        converter = UTConverter()

        url = converter._build_section_content_url("59-10-104", "2025010120250507")
        assert "C59-10-S104_2025010120250507.html" in url

        url = converter._build_section_content_url("59-10-104", None)
        assert "C59-10-S104_1800010118000101.html" in url

    def test_get_current_version(self):
        """Extract version from wrapper page."""
        converter = UTConverter()
        version = converter._get_current_version(SAMPLE_WRAPPER_HTML)
        assert version == "2025010120250507"

    def test_context_manager(self):
        """Converter works as context manager."""
        with UTConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestUTConverterParsing:
    """Test UTConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedUTSection."""
        converter = UTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "59-10-104", "https://example.com", "2025010120250507"
        )

        assert parsed.section_number == "59-10-104"
        assert "Tax basis" in parsed.section_title or "Tax rate" in parsed.section_title
        assert parsed.title_number == "59"
        assert parsed.title_name == "Revenue and Taxation"
        assert parsed.chapter_number == "10"
        assert parsed.chapter_title == "Individual Income Tax Act"
        assert "tax is imposed" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_subsections(self):
        """Parse subsections from section HTML."""
        converter = UTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "59-10-104", "https://example.com"
        )

        # Should have subsections (1), (2), (3)
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)
        assert any(s.identifier == "3" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (a), (b) under (2)."""
        converter = UTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "59-10-104", "https://example.com"
        )

        # Find subsection (2)
        sub_2 = next((s for s in parsed.subsections if s.identifier == "2"), None)
        assert sub_2 is not None
        # Should have children (a) and (b)
        assert len(sub_2.children) >= 2
        assert any(c.identifier == "a" for c in sub_2.children)
        assert any(c.identifier == "b" for c in sub_2.children)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = UTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "59-10-104", "https://example.com"
        )

        assert parsed.history is not None
        assert "Chapter 407" in parsed.history

    def test_parse_effective_date(self):
        """Parse effective date from section HTML."""
        converter = UTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "59-10-104", "https://example.com"
        )

        assert parsed.effective_date is not None
        assert parsed.effective_date == date(2025, 1, 1)

    def test_parse_35A_section(self):
        """Parse section from alphanumeric title 35A."""
        converter = UTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_35A_SECTION_HTML, "35A-3-302", "https://example.com"
        )

        assert parsed.section_number == "35A-3-302"
        assert parsed.title_number == "35A"
        assert parsed.title_name == "Utah Workforce Services Code"
        assert parsed.chapter_number == "3"
        assert parsed.chapter_title == "Employment Support Act"

    def test_to_section_model(self):
        """Convert ParsedUTSection to Section model."""
        converter = UTConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "59-10-104", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "UT-59-10-104"
        assert section.citation.title == 0  # State law indicator
        assert "Tax basis" in section.section_title or "Tax rate" in section.section_title
        assert "Utah Code" in section.title_name
        assert section.uslm_id == "ut/59/10/59-10-104"
        assert section.source_url == "https://example.com"


class TestUTConverterFetching:
    """Test UTConverter HTTP fetching with mocks."""

    @patch.object(UTConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.side_effect = [SAMPLE_WRAPPER_HTML, SAMPLE_SECTION_HTML]

        converter = UTConverter()
        section = converter.fetch_section("59-10-104")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "UT-59-10-104"

    @patch.object(UTConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        import httpx

        mock_get.side_effect = httpx.HTTPStatusError(
            "Not Found", request=None, response=None
        )

        converter = UTConverter()
        with pytest.raises(UTConverterError) as exc_info:
            converter.fetch_section("99-99-999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(UTConverter, "_get")
    def test_get_part_section_numbers(self, mock_get):
        """Get list of section numbers from part index."""
        mock_get.return_value = SAMPLE_PART_INDEX_HTML

        converter = UTConverter()
        sections = converter.get_part_section_numbers("59", "10", "1")

        assert len(sections) == 4
        assert "59-10-103" in sections
        assert "59-10-103.1" in sections
        assert "59-10-104" in sections
        assert "59-10-104.1" in sections

    @patch.object(UTConverter, "_get")
    def test_get_chapter_parts(self, mock_get):
        """Get list of parts from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = UTConverter()
        parts = converter.get_chapter_parts("59", "10")

        assert len(parts) == 3
        assert "1" in parts
        assert "2" in parts
        assert "10" in parts

    @patch.object(UTConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        # Calls: chapter index, part index, wrapper+content for each section
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,  # get_chapter_parts
            SAMPLE_PART_INDEX_HTML,  # get_part_section_numbers for Part 1
            SAMPLE_WRAPPER_HTML,  # fetch_section wrapper
            SAMPLE_SECTION_HTML,  # fetch_section content
            SAMPLE_WRAPPER_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_WRAPPER_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_WRAPPER_HTML,
            SAMPLE_SECTION_HTML,
            # Part 2 - empty for this test
            """<html><body><div id="content"><table id="childtbl"></table></div></body></html>""",
            # Part 10 - empty for this test
            """<html><body><div id="content"><table id="childtbl"></table></div></body></html>""",
        ]

        converter = UTConverter()
        sections = list(converter.iter_chapter("59", "10"))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(UTConverter, "_get")
    def test_fetch_ut_section(self, mock_get):
        """Test fetch_ut_section function."""
        mock_get.side_effect = [SAMPLE_WRAPPER_HTML, SAMPLE_SECTION_HTML]

        section = fetch_ut_section("59-10-104")

        assert section is not None
        assert section.citation.section == "UT-59-10-104"

    @patch.object(UTConverter, "_get")
    def test_download_ut_chapter(self, mock_get):
        """Test download_ut_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_PART_INDEX_HTML,
            SAMPLE_WRAPPER_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_WRAPPER_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_WRAPPER_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_WRAPPER_HTML,
            SAMPLE_SECTION_HTML,
            """<html><body><div id="content"><table id="childtbl"></table></div></body></html>""",
            """<html><body><div id="content"><table id="childtbl"></table></div></body></html>""",
        ]

        sections = download_ut_chapter("59", "10")

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestUTConverterIntegration:
    """Integration tests that hit real le.utah.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Utah Income Tax section 59-10-104."""
        converter = UTConverter()
        section = converter.fetch_section("59-10-104")

        assert section is not None
        assert section.citation.section == "UT-59-10-104"
        assert "tax" in section.section_title.lower()
        assert "tax" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_sales_tax_section(self):
        """Fetch Utah Sales Tax section 59-12-103."""
        converter = UTConverter()
        section = converter.fetch_section("59-12-103")

        assert section is not None
        assert section.citation.section == "UT-59-12-103"

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_workforce_services_section(self):
        """Fetch Utah Workforce Services section 35A-3-302."""
        converter = UTConverter()
        try:
            section = converter.fetch_section("35A-3-302")
            assert section.citation.section == "UT-35A-3-302"
            assert "35A" in section.uslm_id
        except UTConverterError:
            # Section may not exist in current code
            pytest.skip("Section 35A-3-302 not found")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_chapter_59_10_parts(self):
        """Get list of parts in Chapter 59-10."""
        converter = UTConverter()
        parts = converter.get_chapter_parts("59", "10")

        assert len(parts) > 0
        assert "1" in parts  # Part 1 should exist

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_part_sections(self):
        """Get list of sections in Chapter 59-10 Part 1."""
        converter = UTConverter()
        sections = converter.get_part_section_numbers("59", "10", "1")

        assert len(sections) > 0
        assert any("59-10-104" in s for s in sections)
