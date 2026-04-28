"""Tests for Arizona state statute converter.

Tests the AZConverter which fetches from azleg.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.az import (
    AZ_TAX_CHAPTERS,
    AZ_TITLES,
    AZ_WELFARE_CHAPTERS,
    AZConverter,
    AZConverterError,
    download_az_title,
    fetch_az_section,
)
from axiom.models import Section

# Sample HTML from azleg.gov for testing
SAMPLE_SECTION_HTML = """<HTML>

<HEAD>
<TITLE>42-1001 - Definitions</TITLE>
<!Creation Date: 09/21/25>
<!Author: Arizona Legislative Council>
<!Typist: dbupdate>
<meta http-equiv="Content-type" content="text/html;charset=UTF-8">
</HEAD>

<BODY>

<p><font color=GREEN>42-1001</font>. <font color=PURPLE><u>Definitions</u></font></p>

<p>In this title, unless the context otherwise requires:</p>

<p>1. &quot;Board&quot; or &quot;state board&quot; means either the state board of tax appeals or the state board of equalization, as applicable.</p>

<p>2. &quot;Court&quot; means the tax court or superior court, whichever is applicable.</p>

<p>3. &quot;Department&quot; means the department of revenue.</p>

<p>4. &quot;Director&quot; means the director of the department.</p>

<p>5. &quot;Electronically send&quot; or &quot;send electronically&quot; means to send by either email or the use of an electronic portal.</p>

<p>6. &quot;Electronic portal&quot; means a secure location on a website established by the department that requires the receiver to enter a password to access.</p>

<p>7. &quot;Email&quot; means:</p>

<p>(a) An electronic transmission of a message to an email address.</p>

<p>(b) If the message contains confidential information, the electronic transmission of a message to an email address using encryption software that requires the receiver to enter a password before the message can be retrieved and viewed. </p>

<p>8. &quot;Internal revenue code&quot; means the United States internal revenue code of 1986, as amended and in effect as of January 1, 2025, including those provisions that became effective during 2024 with the specific adoption of their retroactive effective dates but excluding all changes to the code enacted after January 1, 2025. </p>

</BODY>

</HTML>
"""

SAMPLE_SECTION_WITH_SUBSECTIONS_HTML = """<HTML>

<HEAD>
<TITLE>42-5006 - Taxpayer bonds; out of state licensed contractors</TITLE>
<!Author: Arizona Legislative Council>
<meta http-equiv="Content-type" content="text/html;charset=UTF-8">
</HEAD>

<BODY>

<p><font color=GREEN>42-5006</font>. <font color=PURPLE><u>Taxpayer bonds; out of state licensed contractors and manufactured building dealers</u></font></p>

<p>A. Notwithstanding section 42-1102, the department shall require a surety bond for each taxpayer who is required to be licensed under title 32, chapter 10 or who is regulated under title 41, chapter 37, article 3, if the taxpayer's principal place of business is outside this state or if the taxpayer has conducted business in this state for less than one year. The department shall prescribe the form of the bond. The bond shall be maintained for a period of at least two years.</p>

<p>B. The bond, duly executed by the applicant as principal and with a corporation duly authorized to execute and write bonds in this state as surety, shall be payable to this state and conditioned on the payment of all transaction privilege taxes incurred and imposed on the taxpayer by this state and its political subdivisions. The bond shall be in such amount, but not less than two thousand dollars, as will assure the payment of the transaction privilege taxes which may reasonably be expected to be incurred by the licensed establishment for a period of one hundred fifty days.</p>

<p>C. The director, by rule, may establish classes of expected tax liability in five thousand dollar increments, beginning with the minimum bond amount prescribed in subsection B of this section. The bond shall provide that after notice and a hearing the director may order forfeited to this state and any affected political subdivision part or all of the bond for nonpayment of taxes, interest and penalties.</p>

<p>D. A licensee on application for a new license covered by subsection A of this section, renewal of a license covered by subsection A of this section or transfer of a license covered by subsection A of this section is exempt from posting a bond if the licensee has for at least two years immediately preceding the application made timely payment of all transaction privilege taxes incurred.</p>

<p>E. If a licensee is not exempt from this section, the director may exempt the licensee if the director finds that the surety bond is not necessary to insure payment of such taxes to the state and any affected political subdivision or the licensee had good cause for the late or insufficient payment of the transaction privilege tax and affiliated excise taxes incurred. </p>

</BODY>

</HTML>
"""

SAMPLE_WELFARE_SECTION_HTML = """<HTML>

<HEAD>
<TITLE>46-101 - Definitions</TITLE>
<!Author: Arizona Legislative Council>
<meta http-equiv="Content-type" content="text/html;charset=UTF-8">
</HEAD>

<BODY>

<p><font color=GREEN>46-101</font>. <font color=PURPLE><u>Definitions</u></font></p>

<p>In this title, unless the context otherwise requires:</p>

<p>1. &quot;Aid to families with dependent children&quot; means assistance granted under section 403 of title IV of the social security act as it existed before August 22, 1996.</p>

<p>2. &quot;Applicant&quot; means a person who has applied for assistance or services under this title.</p>

<p>3. &quot;Assistance&quot; means payments in cash or kind to or on behalf of a person or persons in need as provided for in this title.</p>

<p>4. &quot;Cash assistance&quot; means temporary assistance for needy families paid to a recipient for the purpose of meeting basic living expenses.</p>

<p>5. &quot;Department&quot; means the department of economic security.</p>

<p>6. &quot;Needy family&quot;:</p>

<p>(a) Means a family that resides in the same home and includes a dependent child.</p>

<p>(b) Does not include a child only case.</p>

</BODY>

</HTML>
"""

SAMPLE_TITLE_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Title 42 Index</title></head>
<body>
<h1>Title 42 - Taxation</h1>
<div id="contents">
<ul>
<li><a href="/ars/42/01001.htm">42-1001 Definitions</a></li>
<li><a href="/ars/42/01002.htm">42-1002 Department of revenue</a></li>
<li><a href="/ars/42/05001.htm">42-5001 Definitions</a></li>
<li><a href="/ars/42/05006.htm">42-5006 Taxpayer bonds</a></li>
</ul>
</div>
</body>
</html>
"""


class TestAZTitlesRegistry:
    """Test Arizona title registries."""

    def test_title_42_in_titles(self):
        """Title 42 (Taxation) is in the title registry."""
        assert 42 in AZ_TITLES
        assert "Taxation" in AZ_TITLES[42]

    def test_title_43_in_titles(self):
        """Title 43 (Taxation of Income) is in the title registry."""
        assert 43 in AZ_TITLES
        assert "Income" in AZ_TITLES[43]

    def test_title_46_in_titles(self):
        """Title 46 (Welfare) is in the title registry."""
        assert 46 in AZ_TITLES
        assert "Welfare" in AZ_TITLES[46]

    def test_tax_chapters_exist(self):
        """Tax chapters registry is populated."""
        assert len(AZ_TAX_CHAPTERS) > 0
        assert 421 in AZ_TAX_CHAPTERS  # Administration

    def test_welfare_chapters_exist(self):
        """Welfare chapters registry is populated."""
        assert len(AZ_WELFARE_CHAPTERS) > 0
        assert 461 in AZ_WELFARE_CHAPTERS  # General Provisions


class TestAZConverter:
    """Test AZConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = AZConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = AZConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = AZConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = AZConverter()
        url = converter._build_section_url("42-1001")
        assert "azleg.gov" in url
        assert "/ars/42/01001.htm" in url

    def test_build_section_url_short(self):
        """Build correct URL for short section numbers."""
        converter = AZConverter()
        url = converter._build_section_url("46-101")
        assert "/ars/46/00101.htm" in url

    def test_build_section_url_long(self):
        """Build correct URL for long section numbers."""
        converter = AZConverter()
        url = converter._build_section_url("42-11001")
        assert "/ars/42/11001.htm" in url

    def test_build_section_url_invalid(self):
        """Raise error for invalid section number format."""
        converter = AZConverter()
        with pytest.raises(AZConverterError):
            converter._build_section_url("invalid")

    def test_context_manager(self):
        """Converter works as context manager."""
        with AZConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestAZConverterParsing:
    """Test AZConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedAZSection."""
        converter = AZConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "42-1001", "https://example.com"
        )

        assert parsed.section_number == "42-1001"
        assert parsed.section_title == "Definitions"
        assert parsed.title_number == 42
        assert parsed.title_name == "Taxation"
        assert "internal revenue code" in parsed.text.lower()
        assert parsed.source_url == "https://example.com"

    def test_parse_section_with_subsections(self):
        """Parse section with A., B., C. subsections."""
        converter = AZConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_WITH_SUBSECTIONS_HTML, "42-5006", "https://example.com"
        )

        assert parsed.section_number == "42-5006"
        assert "Taxpayer bonds" in parsed.section_title
        # Should have subsections A, B, C, D, E
        assert len(parsed.subsections) >= 5
        assert any(s.identifier == "A" for s in parsed.subsections)
        assert any(s.identifier == "B" for s in parsed.subsections)
        assert any(s.identifier == "E" for s in parsed.subsections)

    def test_parse_welfare_section(self):
        """Parse welfare section from Title 46."""
        converter = AZConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "46-101", "https://example.com"
        )

        assert parsed.section_number == "46-101"
        assert parsed.section_title == "Definitions"
        assert parsed.title_number == 46
        assert parsed.title_name == "Welfare"
        assert "needy family" in parsed.text.lower()

    def test_parse_nested_subsections(self):
        """Parse nested (a), (b) subsections."""
        converter = AZConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "46-101", "https://example.com"
        )

        # Definition 6 has (a) and (b) subsections
        # These may appear as level-3 children depending on parsing
        assert "Means a family" in parsed.text

    def test_to_section_model(self):
        """Convert ParsedAZSection to Section model."""
        converter = AZConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "42-1001", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "AZ-42-1001"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "Arizona Revised Statutes" in section.title_name
        assert section.uslm_id == "az/42/42-1001"
        assert section.source_url == "https://example.com"


class TestAZConverterFetching:
    """Test AZConverter HTTP fetching with mocks."""

    @patch.object(AZConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = AZConverter()
        section = converter.fetch_section("42-1001")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "AZ-42-1001"
        assert "Definitions" in section.section_title

    @patch.object(AZConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>404 not found</body></html>"

        converter = AZConverter()
        with pytest.raises(AZConverterError) as exc_info:
            converter.fetch_section("99-9999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(AZConverter, "_get")
    def test_get_title_sections(self, mock_get):
        """Get list of section numbers from title index."""
        mock_get.return_value = SAMPLE_TITLE_INDEX_HTML

        converter = AZConverter()
        sections = converter.get_title_sections(42)

        assert len(sections) == 4
        assert "42-1001" in sections
        assert "42-1002" in sections
        assert "42-5001" in sections
        assert "42-5006" in sections

    @patch.object(AZConverter, "_get")
    def test_iter_title(self, mock_get):
        """Iterate over sections in a title."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_TITLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = AZConverter()
        sections = list(converter.iter_title(42))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(AZConverter, "_get")
    def test_fetch_az_section(self, mock_get):
        """Test fetch_az_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_az_section("42-1001")

        assert section is not None
        assert section.citation.section == "AZ-42-1001"

    @patch.object(AZConverter, "_get")
    def test_download_az_title(self, mock_get):
        """Test download_az_title function."""
        mock_get.side_effect = [
            SAMPLE_TITLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_az_title(42)

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestAZConverterIntegration:
    """Integration tests that hit real azleg.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_definitions_section(self):
        """Fetch Arizona Tax section 42-1001."""
        converter = AZConverter()
        section = converter.fetch_section("42-1001")

        assert section is not None
        assert section.citation.section == "AZ-42-1001"
        assert "definitions" in section.section_title.lower()
        assert "department" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_transaction_privilege_section(self):
        """Fetch Arizona Transaction Privilege Tax section 42-5001."""
        converter = AZConverter()
        section = converter.fetch_section("42-5001")

        assert section is not None
        assert section.citation.section == "AZ-42-5001"
        assert "tax" in section.text.lower() or "privilege" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Arizona welfare section 46-101."""
        converter = AZConverter()
        section = converter.fetch_section("46-101")

        assert section is not None
        assert section.citation.section == "AZ-46-101"
        assert "welfare" in section.title_name.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_42_sections(self):
        """Get list of sections in Title 42."""
        converter = AZConverter()
        sections = converter.get_title_sections(42)

        # Title 42 should have many sections
        assert len(sections) > 0
        assert all(s.startswith("42-") for s in sections)
