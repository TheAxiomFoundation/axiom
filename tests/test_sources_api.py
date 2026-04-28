"""Tests for the API source adapter module."""

from unittest.mock import MagicMock, patch

import pytest

from axiom.sources.api import APISource, LegiScanSource, NYLegislationSource
from axiom.sources.base import SourceConfig


class TestAPISource:
    def test_init(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="api",
            base_url="https://api.example.com",
        )
        source = APISource(config)
        assert source.config.source_type == "api"

    def test_get_section_not_implemented(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="api",
            base_url="https://api.example.com",
        )
        source = APISource(config)
        with pytest.raises(NotImplementedError):
            source.get_section("TAX", "601")

    def test_list_sections_not_implemented(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="api",
            base_url="https://api.example.com",
        )
        source = APISource(config)
        with pytest.raises(NotImplementedError):
            list(source.list_sections("TAX"))

    @patch.object(APISource, "_get")
    def test_api_get(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"result": "data"}
        mock_get.return_value = mock_response

        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="api",
            base_url="https://api.example.com",
            api_key="test-key",
        )
        source = APISource(config)
        result = source._api_get("/endpoint", params={"q": "test"})

        assert result == {"result": "data"}
        mock_get.assert_called_once()

    @patch.object(APISource, "_get")
    def test_api_get_no_key(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"result": "data"}
        mock_get.return_value = mock_response

        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="api",
            base_url="https://api.example.com",
        )
        source = APISource(config)
        result = source._api_get("/endpoint")
        assert result == {"result": "data"}


class TestNYLegislationSource:
    def test_init(self):
        source = NYLegislationSource(api_key="test-key")
        assert source.config.jurisdiction == "us-ny"
        assert source.config.api_key == "test-key"
        assert "TAX" in source.NY_CODES

    def test_init_no_key(self):
        source = NYLegislationSource()
        assert source.config.api_key is None

    @patch.object(NYLegislationSource, "_api_get")
    def test_get_section_success(self, mock_api_get):
        mock_api_get.return_value = {
            "result": {
                "text": "(a) General rule. Tax is imposed...",
                "title": "Imposition of tax",
            }
        }

        source = NYLegislationSource()
        result = source.get_section("TAX", "601")

        assert result is not None
        assert result.section == "601"
        assert result.jurisdiction == "us-ny"

    @patch.object(NYLegislationSource, "_api_get")
    def test_get_section_not_found(self, mock_api_get):
        mock_api_get.return_value = {"result": {}}

        source = NYLegislationSource()
        result = source.get_section("TAX", "999")
        assert result is None

    @patch.object(NYLegislationSource, "_api_get")
    def test_get_section_http_error(self, mock_api_get):
        import httpx
        mock_api_get.side_effect = httpx.HTTPError("Connection error")

        source = NYLegislationSource()
        result = source.get_section("TAX", "601")
        assert result is None

    def test_parse_subsections(self):
        source = NYLegislationSource()
        text = "(a) General rule applies. (b) Exceptions exist."
        subs = source._parse_subsections(text)
        assert len(subs) >= 1

    @patch.object(NYLegislationSource, "_api_get")
    def test_list_sections(self, mock_api_get):
        mock_api_get.return_value = {
            "result": {
                "locationId": "TAX",
                "documents": {
                    "items": [
                        {"locationId": "TAX601"},
                        {"locationId": "TAX602", "documents": {"items": []}},
                    ]
                },
            }
        }

        source = NYLegislationSource()
        sections = list(source.list_sections("TAX"))
        assert len(sections) >= 1

    @patch.object(NYLegislationSource, "_api_get")
    def test_list_sections_http_error(self, mock_api_get):
        import httpx
        mock_api_get.side_effect = httpx.HTTPError("Connection error")

        source = NYLegislationSource()
        sections = list(source.list_sections("TAX"))
        assert sections == []


class TestLegiScanSource:
    def test_init(self):
        source = LegiScanSource(api_key="test-key")
        assert source.config.api_key == "test-key"
        assert source.config.rate_limit == 1.0

    def test_get_section_raises(self):
        source = LegiScanSource(api_key="test-key")
        with pytest.raises(NotImplementedError, match="bills"):
            source.get_section("TAX", "601")

    def test_list_sections_raises(self):
        source = LegiScanSource(api_key="test-key")
        with pytest.raises(NotImplementedError, match="bills"):
            list(source.list_sections("TAX"))

    @patch.object(LegiScanSource, "_api_get")
    def test_search_bills(self, mock_api_get):
        mock_api_get.return_value = {
            "searchresult": {
                "bills": [
                    {"bill_id": 1, "title": "Tax Reform Act"},
                ]
            }
        }

        source = LegiScanSource(api_key="test-key")
        results = source.search_bills("CA", "tax reform")
        assert len(results) == 1

    @patch.object(LegiScanSource, "_api_get")
    def test_search_bills_with_year(self, mock_api_get):
        mock_api_get.return_value = {
            "searchresult": {"bills": []}
        }

        source = LegiScanSource(api_key="test-key")
        results = source.search_bills("NY", "income", year=2024)
        assert results == []
