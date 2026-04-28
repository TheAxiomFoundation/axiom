"""Tests for eCFR converter."""

from datetime import date
from unittest.mock import Mock, patch

from axiom.converters.ecfr import (
    PRIORITY_TITLES,
    ECFRConverter,
    FetchResult,
    fetch_regulation,
)
from axiom.models_regulation import CFRCitation

# Sample XML fragments for testing
SAMPLE_SECTION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ECFR>
<DIV5 N="1" NODE="26:1.0.1.1.1" TYPE="PART">
<HEAD>PART 1-INCOME TAXES</HEAD>
<AUTH>
<HED>Authority:</HED>
<PSPACE>26 U.S.C. 7805, unless otherwise noted.</PSPACE>
<P>Section 1.32-1 also issued under 26 U.S.C. 32;</P>
</AUTH>
<DIV8 N="§ 1.32-1" NODE="26:1.0.1.1.1.0.1.100" TYPE="SECTION">
<HEAD>§ 1.32-1   Earned income.</HEAD>
<P>(a) <I>In general.</I> For purposes of section 32, earned income means-
</P>
<P>(1) wages, salaries, tips, and other employee compensation, plus
</P>
<P>(2) the amount of the taxpayer's net earnings from self-employment.
</P>
<P>(b) <I>Special rules.</I> The following rules apply for purposes of this section:
</P>
<P>(1) <I>Combat pay.</I> A taxpayer may elect to include combat pay.
</P>
<CITA TYPE="N">[T.D. 9954, 86 FR 12345, Mar. 15, 2021]
</CITA>
</DIV8>
</DIV5>
</ECFR>
"""

SAMPLE_MULTIPLE_SECTIONS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ECFR>
<DIV5 N="1" NODE="26:1.0.1.1.1" TYPE="PART">
<HEAD>PART 1-INCOME TAXES</HEAD>
<AUTH>
<HED>Authority:</HED>
<PSPACE>26 U.S.C. 7805</PSPACE>
</AUTH>
<DIV8 N="§ 1.32-1" NODE="26:1.0.1.1.1.0.1.100" TYPE="SECTION">
<HEAD>§ 1.32-1   Earned income.</HEAD>
<P>(a) Test content for section 32-1.</P>
<CITA TYPE="N">[T.D. 9954, 86 FR 12345, Mar. 15, 2021]</CITA>
</DIV8>
<DIV8 N="§ 1.32-2" NODE="26:1.0.1.1.1.0.1.101" TYPE="SECTION">
<HEAD>§ 1.32-2   Qualifying child.</HEAD>
<P>(a) Test content for section 32-2.</P>
<CITA TYPE="N">[T.D. 9955, 86 FR 12346, Mar. 16, 2021]</CITA>
</DIV8>
</DIV5>
</ECFR>
"""


class TestECFRConverter:
    """Tests for ECFRConverter initialization."""

    def test_converter_init_defaults(self):
        """Initialize converter with default settings."""
        converter = ECFRConverter()
        assert converter.api_base == "https://www.ecfr.gov/api/versioner/v1"
        assert converter.data_dir.name == "ecfr"
        assert converter.timeout == 120.0

    def test_converter_init_custom(self, tmp_path):
        """Initialize converter with custom settings."""
        converter = ECFRConverter(
            data_dir=tmp_path,
            api_base="https://custom.api/",
            timeout=60.0,
        )
        assert converter.data_dir == tmp_path
        assert converter.api_base == "https://custom.api/"
        assert converter.timeout == 60.0

    def test_converter_context_manager(self):
        """Converter works as context manager."""
        with ECFRConverter() as converter:
            assert converter is not None
        # Client should be closed after context


class TestECFRUrls:
    """Tests for URL building."""

    def test_get_title_url_current(self):
        """Build URL for current date."""
        converter = ECFRConverter()
        url = converter.get_title_url(26)
        assert "title-26.xml" in url
        assert date.today().isoformat() in url

    def test_get_title_url_with_date(self):
        """Build URL for specific date."""
        converter = ECFRConverter()
        test_date = date(2024, 1, 15)
        url = converter.get_title_url(26, as_of=test_date)
        assert "2024-01-15" in url
        assert "title-26.xml" in url

    def test_get_title_url_with_part(self):
        """Build URL with part filter."""
        converter = ECFRConverter()
        url = converter.get_title_url(26, part=1)
        assert "part=1" in url
        assert "title-26.xml" in url


class TestECFRParsing:
    """Tests for XML parsing."""

    def test_parse_section_basic(self):
        """Parse a basic CFR section."""
        converter = ECFRConverter()
        reg = converter._parse_section(
            SAMPLE_SECTION_XML, 26, 1, "32-1",
            "https://example.com", date(2024, 1, 1)
        )

        assert reg is not None
        assert reg.citation.title == 26
        assert reg.citation.part == 1
        assert reg.citation.section == "32-1"
        assert reg.heading == "Earned income"

    def test_parse_section_full_text(self):
        """Section full text is extracted."""
        converter = ECFRConverter()
        reg = converter._parse_section(
            SAMPLE_SECTION_XML, 26, 1, "32-1",
            "https://example.com", None
        )

        assert reg is not None
        assert "earned income means" in reg.full_text
        assert "wages, salaries, tips" in reg.full_text

    def test_parse_section_subsections(self):
        """Subsections are parsed."""
        converter = ECFRConverter()
        reg = converter._parse_section(
            SAMPLE_SECTION_XML, 26, 1, "32-1",
            "https://example.com", None
        )

        assert reg is not None
        assert len(reg.subsections) >= 2
        assert reg.subsections[0].id == "a"
        assert reg.subsections[0].heading == "In general"

    def test_parse_section_source(self):
        """Source citation is extracted."""
        converter = ECFRConverter()
        reg = converter._parse_section(
            SAMPLE_SECTION_XML, 26, 1, "32-1",
            "https://example.com", None
        )

        assert reg is not None
        assert "T.D. 9954" in reg.source
        assert "86 FR 12345" in reg.source

    def test_parse_section_not_found(self):
        """Returns None when section not found."""
        converter = ECFRConverter()
        reg = converter._parse_section(
            SAMPLE_SECTION_XML, 26, 1, "999",
            "https://example.com", None
        )

        assert reg is None

    def test_parse_part_multiple_sections(self):
        """Parse all sections in a part."""
        converter = ECFRConverter()
        regulations = list(converter._parse_part(
            SAMPLE_MULTIPLE_SECTIONS_XML, 26, 1,
            "https://example.com", None
        ))

        assert len(regulations) == 2
        assert regulations[0].citation.section == "32-1"
        assert regulations[1].citation.section == "32-2"


class TestECFRFetch:
    """Tests for fetching with mocked HTTP."""

    def test_fetch_success(self):
        """Fetch a section successfully."""
        converter = ECFRConverter()

        # Mock the HTTP client
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = SAMPLE_SECTION_XML

        mock_client = Mock()
        mock_client.get.return_value = mock_response
        converter._client = mock_client

        result = converter.fetch("26/1.32-1")

        assert result.success
        assert result.regulation is not None
        assert result.regulation.citation.section == "32-1"
        assert result.error is None

    def test_fetch_invalid_citation(self):
        """Fetch with invalid citation format."""
        converter = ECFRConverter()
        result = converter.fetch("invalid")

        assert not result.success
        assert "Invalid citation format" in result.error

    def test_fetch_http_error(self):
        """Fetch handles HTTP errors."""
        import httpx

        converter = ECFRConverter()

        mock_client = Mock()
        mock_client.get.side_effect = httpx.HTTPStatusError(
            "Not found",
            request=Mock(),
            response=Mock(status_code=404, text="Not found"),
        )
        converter._client = mock_client

        result = converter.fetch("26/1.32-1")

        assert not result.success
        assert "HTTP error 404" in result.error

    def test_fetch_part_success(self):
        """Fetch all sections in a part."""
        converter = ECFRConverter()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = SAMPLE_MULTIPLE_SECTIONS_XML

        mock_client = Mock()
        mock_client.get.return_value = mock_response
        converter._client = mock_client

        result = converter.fetch_part(26, 1)

        assert result.success
        assert len(result.regulations) == 2


class TestECFRConvenienceMethods:
    """Tests for convenience methods."""

    def test_fetch_irs_section(self):
        """Fetch IRS regulation by section."""
        converter = ECFRConverter()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = SAMPLE_SECTION_XML

        mock_client = Mock()
        mock_client.get.return_value = mock_response
        converter._client = mock_client

        result = converter.fetch_irs(1, "32-1")

        assert result.success
        assert result.regulation is not None
        mock_client.get.assert_called_once()
        call_url = mock_client.get.call_args[0][0]
        assert "title-26.xml" in call_url
        assert "part=1" in call_url

    def test_fetch_irs_part(self):
        """Fetch entire IRS part."""
        converter = ECFRConverter()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = SAMPLE_MULTIPLE_SECTIONS_XML

        mock_client = Mock()
        mock_client.get.return_value = mock_response
        converter._client = mock_client

        result = converter.fetch_irs(1)

        assert result.success
        assert len(result.regulations) >= 1


class TestFetchResult:
    """Tests for FetchResult dataclass."""

    def test_fetch_result_success(self):
        """Create successful FetchResult."""
        citation = CFRCitation(title=26, part=1, section="32")
        result = FetchResult(
            success=True,
            citation=citation,
            source_url="https://example.com",
        )

        assert result.success
        assert result.citation.cfr_cite == "26 CFR 1.32"
        assert result.error is None

    def test_fetch_result_error(self):
        """Create error FetchResult."""
        result = FetchResult(
            success=False,
            error="Connection failed",
            source_url="https://example.com",
        )

        assert not result.success
        assert result.error == "Connection failed"
        assert result.regulation is None


class TestPriorityTitles:
    """Tests for priority titles configuration."""

    def test_priority_titles_exist(self):
        """Priority titles are defined."""
        assert 26 in PRIORITY_TITLES  # IRS
        assert 7 in PRIORITY_TITLES   # SNAP
        assert 20 in PRIORITY_TITLES  # SSA
        assert 42 in PRIORITY_TITLES  # Medicare/Medicaid

    def test_priority_titles_descriptions(self):
        """Priority titles have descriptions."""
        assert "IRS" in PRIORITY_TITLES[26] or "Internal Revenue" in PRIORITY_TITLES[26]
        assert "SNAP" in PRIORITY_TITLES[7]
        assert "SSA" in PRIORITY_TITLES[20]


class TestModuleFunctions:
    """Tests for module-level convenience functions."""

    def test_fetch_regulation(self):
        """Module function fetches regulation."""
        with patch('axiom.converters.ecfr.ECFRConverter') as MockConverter:
            mock_converter = Mock()
            mock_converter.fetch.return_value = FetchResult(
                success=True,
                citation=CFRCitation(title=26, part=1, section="32"),
            )
            MockConverter.return_value.__enter__ = Mock(return_value=mock_converter)
            MockConverter.return_value.__exit__ = Mock(return_value=None)

            result = fetch_regulation("26/1.32")

            assert result.success
            mock_converter.fetch.assert_called_once_with("26/1.32", as_of=None)
