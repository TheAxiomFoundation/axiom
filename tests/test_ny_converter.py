"""Tests for New York state converter (converters/us_states/ny.py)."""

import os
from unittest.mock import MagicMock, patch
from xml.etree import ElementTree as ET

import pytest

from axiom.converters.us_states.ny import (
    NY_LAW_CODES,
    USLM_NS,
    NYFetchResult,
    NYLawInfo,
    NYLegislationAPIError,
    NYSection,
    NYStateConverter,
)


class TestNYLawCodes:
    """Tests for NY law code constants."""

    def test_tax_law_present(self):
        """Tax Law should be in the codes."""
        assert "TAX" in NY_LAW_CODES
        assert NY_LAW_CODES["TAX"] == "Tax Law"

    def test_social_services_present(self):
        """Social Services Law should be in the codes."""
        assert "SOS" in NY_LAW_CODES
        assert NY_LAW_CODES["SOS"] == "Social Services Law"

    def test_education_law_present(self):
        """Education Law should be in the codes."""
        assert "EDN" in NY_LAW_CODES
        assert NY_LAW_CODES["EDN"] == "Education Law"


class TestNYSection:
    """Tests for NYSection dataclass."""

    def test_basic_section(self):
        """Test basic section creation."""
        section = NYSection(
            law_id="TAX",
            location_id="606",
            title="Credits against tax",
            text="Sample text...",
            doc_type="SECTION",
            doc_level_id="SECTION",
        )
        assert section.law_id == "TAX"
        assert section.location_id == "606"
        assert section.active_date is None

    def test_section_with_metadata(self):
        """Test section with all metadata."""
        section = NYSection(
            law_id="TAX",
            location_id="A22S606",
            title="Credits against tax",
            text="Sample text...",
            doc_type="SECTION",
            doc_level_id="SECTION",
            active_date="2024-01-01",
            parent_location_ids=["A22"],
            prev_sibling="605",
            next_sibling="607",
        )
        assert section.active_date == "2024-01-01"
        assert section.parent_location_ids == ["A22"]


class TestNYStateConverterExtraction:
    """Tests for NYStateConverter extraction methods."""

    def test_extract_section_number_simple(self):
        """Test simple section number extraction."""
        assert NYStateConverter._extract_section_number("606") == "606"

    def test_extract_section_number_article_format(self):
        """Test article-section format like A22S606."""
        assert NYStateConverter._extract_section_number("A22S606") == "606"

    def test_extract_section_number_short(self):
        """Test short format like A1S1."""
        assert NYStateConverter._extract_section_number("A1S1") == "1"

    def test_extract_section_number_empty(self):
        """Test empty location ID."""
        assert NYStateConverter._extract_section_number("") == ""

    def test_extract_section_number_complex(self):
        """Test complex section number."""
        assert NYStateConverter._extract_section_number("A9TS151-A") == "151-A"

    def test_extract_article_number_simple(self):
        """Test that simple sections have no article."""
        assert NYStateConverter._extract_article_number("606") is None

    def test_extract_article_number_present(self):
        """Test extracting article number."""
        assert NYStateConverter._extract_article_number("A22S606") == "22"

    def test_extract_article_number_short(self):
        """Test short article format."""
        assert NYStateConverter._extract_article_number("A1S1") == "1"


class TestNYStateConverterUSLMConversion:
    """Tests for USLM XML generation."""

    def test_section_to_uslm_basic(self):
        """Test basic USLM conversion."""
        section = NYSection(
            law_id="TAX",
            location_id="606",
            title="Credits against tax",
            text="Earned income credit provisions.",
            doc_type="SECTION",
            doc_level_id="SECTION",
            active_date="2024-01-01",
        )
        law_info = NYLawInfo(
            law_id="TAX",
            chapter="60",
            name="Tax Law",
            law_type="CONSOLIDATED",
        )

        xml_str = NYStateConverter._section_to_uslm(section, law_info)

        # Parse and verify structure
        root = ET.fromstring(xml_str)
        assert root.tag == f"{{{USLM_NS}}}lawDoc"
        assert root.get("identifier") == "/us/ny/tax"

        # Find section
        section_elem = root.find(f".//{{{USLM_NS}}}section")
        assert section_elem is not None
        assert section_elem.get("identifier") == "/us/ny/tax/s606"

        # Check heading
        heading = section_elem.find(f"{{{USLM_NS}}}heading")
        assert heading is not None
        assert heading.text == "Credits against tax"

    def test_section_to_uslm_with_article(self):
        """Test USLM conversion with article structure."""
        section = NYSection(
            law_id="TAX",
            location_id="A22S606",
            title="Credits against tax",
            text="Earned income credit.",
            doc_type="SECTION",
            doc_level_id="SECTION",
        )

        xml_str = NYStateConverter._section_to_uslm(section, None)
        root = ET.fromstring(xml_str)

        # Check article element exists
        article = root.find(f".//{{{USLM_NS}}}article")
        assert article is not None
        assert article.get("identifier") == "/us/ny/tax/a22"

        # Check section is under article
        section_elem = article.find(f"{{{USLM_NS}}}section")
        assert section_elem is not None
        assert section_elem.get("identifier") == "/us/ny/tax/s606"

    def test_section_to_uslm_with_subsections(self):
        """Test USLM conversion parses subsections."""
        section = NYSection(
            law_id="TAX",
            location_id="606",
            title="Credits against tax",
            text="""(a) General rule. A resident individual taxpayer shall be allowed a credit.
(b) Amount. The credit shall be calculated as follows:
(1) For taxpayers with income under $30,000...
(2) For taxpayers with income over $30,000...""",
            doc_type="SECTION",
            doc_level_id="SECTION",
        )

        xml_str = NYStateConverter._section_to_uslm(section, None)
        root = ET.fromstring(xml_str)

        # Should have subsection elements
        subsections = root.findall(f".//{{{USLM_NS}}}subsection")
        assert len(subsections) >= 2  # (a) and (b)


class TestNYStateConverterErrors:
    """Tests for error handling."""

    def test_missing_api_key_raises(self):
        """Missing API key should raise ValueError."""
        env = os.environ.copy()
        env.pop("NY_LEGISLATION_API_KEY", None)
        with patch.dict(os.environ, env, clear=True), pytest.raises(
            ValueError, match="NY API key required"
        ):
            NYStateConverter()

    def test_invalid_path_format(self):
        """Invalid path format should raise ValueError."""
        with patch.dict(os.environ, {"NY_LEGISLATION_API_KEY": "test_key"}):
            converter = NYStateConverter(api_key="test_key")
            with pytest.raises(ValueError, match="Invalid path format"):
                converter.fetch("TAX606")  # Missing /
            converter.close()


class TestNYStateConverterMocked:
    """Tests with mocked HTTP responses."""

    @pytest.fixture
    def mock_converter(self):
        """Create a converter with mocked HTTP client."""
        with patch.dict(os.environ, {"NY_LEGISLATION_API_KEY": "test_key"}):
            converter = NYStateConverter(api_key="test_key")
            converter._client = MagicMock()
            yield converter
            converter.close()

    def test_fetch_section(self, mock_converter):
        """Test fetching a section."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "success": True,
            "result": {
                "lawId": "TAX",
                "locationId": "606",
                "title": "Credits against tax",
                "text": "Section text here...",
                "docType": "SECTION",
                "docLevelId": "SECTION",
                "activeDate": "2024-01-01",
            },
        }
        mock_converter._client.get.return_value = mock_response

        result = mock_converter.fetch("TAX/606")

        assert isinstance(result, NYFetchResult)
        assert result.section.law_id == "TAX"
        assert result.section.location_id == "606"
        assert result.section.title == "Credits against tax"

    def test_fetch_with_law_info(self, mock_converter):
        """Test that law info is fetched when available."""
        # First call is for section, second is for law info
        mock_response_section = MagicMock()
        mock_response_section.json.return_value = {
            "success": True,
            "result": {
                "lawId": "TAX",
                "locationId": "606",
                "title": "Credits",
                "text": "Text...",
                "docType": "SECTION",
                "docLevelId": "SECTION",
            },
        }
        mock_response_info = MagicMock()
        mock_response_info.json.return_value = {
            "success": True,
            "result": {
                "info": {
                    "lawId": "TAX",
                    "chapter": "60",
                    "name": "Tax Law",
                    "lawType": "CONSOLIDATED",
                }
            },
        }
        mock_converter._client.get.side_effect = [
            mock_response_section,
            mock_response_info,
        ]

        result = mock_converter.fetch("TAX/606")

        assert result.law_info is not None
        assert result.law_info.law_id == "TAX"
        assert result.law_info.name == "Tax Law"

    def test_list_laws(self, mock_converter):
        """Test listing all law codes."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "success": True,
            "result": {
                "items": [
                    {"lawId": "TAX", "chapter": "60", "name": "Tax", "lawType": "CONSOLIDATED"},
                    {"lawId": "SOS", "chapter": "55", "name": "Social Services", "lawType": "CONSOLIDATED"},
                ]
            },
        }
        mock_converter._client.get.return_value = mock_response

        laws = mock_converter.list_laws()

        assert len(laws) == 2
        assert laws[0].law_id == "TAX"
        assert laws[1].law_id == "SOS"

    def test_search(self, mock_converter):
        """Test search functionality."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "success": True,
            "result": {
                "items": [
                    {"lawId": "TAX", "locationId": "606", "title": "Credits"},
                ]
            },
        }
        mock_converter._client.get.return_value = mock_response

        results = mock_converter.search("earned income credit", law_id="TAX")

        assert len(results) == 1
        assert results[0]["lawId"] == "TAX"

    def test_api_error_response(self, mock_converter):
        """Test handling of API error response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "success": False,
            "message": "Invalid API key",
            "errorCode": 701,
        }
        mock_converter._client.get.return_value = mock_response

        with pytest.raises(NYLegislationAPIError) as exc_info:
            mock_converter.fetch("TAX/606")

        assert exc_info.value.error_code == 701
        assert "Invalid API key" in str(exc_info.value)


class TestNYFetchResult:
    """Tests for NYFetchResult."""

    def test_to_uslm_xml(self):
        """Test converting fetch result to USLM XML."""
        from datetime import datetime

        section = NYSection(
            law_id="TAX",
            location_id="606",
            title="Credits against tax",
            text="Sample text",
            doc_type="SECTION",
            doc_level_id="SECTION",
        )
        result = NYFetchResult(
            section=section,
            law_info=None,
            raw_response={},
            fetched_at=datetime.now(),
        )

        xml_str = result.to_uslm_xml()

        assert "<?xml version" in xml_str
        assert "lawDoc" in xml_str
        assert "/us/ny/tax" in xml_str


class TestNYStateConverterIntegration:
    """Integration tests requiring real API key.

    These tests are skipped if NY_LEGISLATION_API_KEY is not set.
    Run with: pytest tests/test_ny_converter.py -v -k integration
    """

    @pytest.fixture
    def real_converter(self):
        """Create a converter with real API key if available."""
        api_key = os.environ.get("NY_LEGISLATION_API_KEY")
        if not api_key:
            pytest.skip("NY_LEGISLATION_API_KEY not set")
        converter = NYStateConverter(api_key=api_key, rate_limit_delay=0.5)
        yield converter
        converter.close()

    @pytest.mark.integration
    def test_list_laws_real(self, real_converter):
        """Test listing laws with real API."""
        laws = real_converter.list_laws()
        assert len(laws) > 0

        # TAX should be in the list
        law_ids = [law.law_id for law in laws]
        assert "TAX" in law_ids

    @pytest.mark.integration
    def test_fetch_tax_section_606_real(self, real_converter):
        """Test fetching NY EITC section (TAX 606)."""
        result = real_converter.fetch("TAX/606")

        assert result.section.law_id == "TAX"
        assert result.section.location_id == "606"
        assert len(result.section.text) > 0
        # Should contain earned income credit language
        text_lower = result.section.text.lower()
        assert "credit" in text_lower or "tax" in text_lower

    @pytest.mark.integration
    def test_to_uslm_xml_real(self, real_converter):
        """Test USLM conversion with real data."""
        result = real_converter.fetch("TAX/606")
        xml_str = result.to_uslm_xml()

        # Parse to verify it's valid XML
        root = ET.fromstring(xml_str)
        assert root.tag == f"{{{USLM_NS}}}lawDoc"

        # Check we have section content
        content = root.find(f".//{{{USLM_NS}}}content")
        # Content or subsections should exist
        subsections = root.findall(f".//{{{USLM_NS}}}subsection")
        assert content is not None or len(subsections) > 0

    @pytest.mark.integration
    def test_search_earned_income_real(self, real_converter):
        """Test searching for earned income credit."""
        results = real_converter.search("earned income credit", law_id="TAX", limit=10)
        # May or may not find results depending on search index state
        assert isinstance(results, list)
