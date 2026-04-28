"""Tests for the IRS guidance fetcher module."""

from unittest.mock import MagicMock, patch

import pytest

from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher


class TestIRSGuidanceFetcher:
    def test_init(self):
        fetcher = IRSGuidanceFetcher()
        assert fetcher.base_url == "https://www.irs.gov"
        assert fetcher.client is not None

    @patch.object(IRSGuidanceFetcher, "_find_irb_url")
    @patch.object(IRSGuidanceFetcher, "_parse_revenue_procedure")
    def test_fetch_revenue_procedure(self, mock_parse, mock_find_url):
        mock_find_url.return_value = "https://www.irs.gov/irb/2023-48"

        mock_rp = MagicMock()
        mock_parse.return_value = mock_rp

        fetcher = IRSGuidanceFetcher()
        with patch.object(fetcher.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.text = "<html><body>Rev. Proc.</body></html>"
            mock_get.return_value = mock_response

            result = fetcher.fetch_revenue_procedure("2023-34")
            assert result is mock_rp

    @patch.object(IRSGuidanceFetcher, "_find_irb_url")
    def test_fetch_revenue_procedure_not_found(self, mock_find_url):
        mock_find_url.return_value = None

        fetcher = IRSGuidanceFetcher()
        with pytest.raises(ValueError, match="Could not find IRB URL"):
            fetcher.fetch_revenue_procedure("9999-99")
