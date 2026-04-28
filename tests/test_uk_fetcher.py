"""Tests for UK legislation fetcher."""

from unittest.mock import AsyncMock, patch

import pytest

# Sample XML response
SAMPLE_SECTION_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<Legislation xmlns="http://www.legislation.gov.uk/namespaces/legislation"
             xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata"
             xmlns:dc="http://purl.org/dc/elements/1.1/"
             DocumentURI="http://www.legislation.gov.uk/ukpga/2003/1/section/62">
<ukm:Metadata>
    <dc:title>Income Tax (Earnings and Pensions) Act 2003</dc:title>
    <ukm:PrimaryMetadata>
        <ukm:Year Value="2003"/>
        <ukm:Number Value="1"/>
    </ukm:PrimaryMetadata>
    <ukm:EnactmentDate Date="2003-04-10"/>
</ukm:Metadata>
<Primary>
    <Body>
        <P1 id="section-62">
            <Pnumber>62</Pnumber>
            <P1para>
                <Text>"Earnings" means any salary, wages or fee.</Text>
            </P1para>
        </P1>
    </Body>
</Primary>
</Legislation>
"""


class TestUKLegislationFetcher:
    """Tests for UK legislation fetcher."""

    def test_fetcher_init(self):
        """Initialize fetcher with default settings."""
        from axiom_corpus.fetchers.legislation_uk import UKLegislationFetcher

        fetcher = UKLegislationFetcher()
        assert fetcher.base_url == "https://www.legislation.gov.uk"

    def test_fetcher_custom_data_dir(self, tmp_path):
        """Initialize fetcher with custom data directory."""
        from axiom_corpus.fetchers.legislation_uk import UKLegislationFetcher

        fetcher = UKLegislationFetcher(data_dir=tmp_path)
        assert fetcher.data_dir == tmp_path

    def test_build_section_url(self):
        """Build URL for fetching a section."""
        from axiom_corpus.fetchers.legislation_uk import UKLegislationFetcher
        from axiom_corpus.models_uk import UKCitation

        fetcher = UKLegislationFetcher()
        citation = UKCitation(type="ukpga", year=2003, number=1, section="62")
        url = fetcher.build_url(citation)
        assert url == "https://www.legislation.gov.uk/ukpga/2003/1/section/62/data.xml"

    def test_build_act_url(self):
        """Build URL for fetching an entire Act."""
        from axiom_corpus.fetchers.legislation_uk import UKLegislationFetcher
        from axiom_corpus.models_uk import UKCitation

        fetcher = UKLegislationFetcher()
        citation = UKCitation(type="ukpga", year=2003, number=1)
        url = fetcher.build_url(citation)
        assert url == "https://www.legislation.gov.uk/ukpga/2003/1/data.xml"


class TestUKLegislationDownload:
    """Tests for downloading UK legislation."""

    @pytest.mark.asyncio
    async def test_fetch_section(self, tmp_path):
        """Fetch a single section."""
        from axiom_corpus.fetchers.legislation_uk import UKLegislationFetcher
        from axiom_corpus.models_uk import UKCitation

        fetcher = UKLegislationFetcher(data_dir=tmp_path)

        with patch.object(fetcher, '_fetch_xml', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_SECTION_RESPONSE

            citation = UKCitation(type="ukpga", year=2003, number=1, section="62")
            section = await fetcher.fetch_section(citation)

            assert section is not None
            assert section.citation.section == "62"
            assert "salary" in section.text

    @pytest.mark.asyncio
    async def test_fetch_with_caching(self, tmp_path):
        """Fetched XML is cached to disk."""
        from axiom_corpus.fetchers.legislation_uk import UKLegislationFetcher
        from axiom_corpus.models_uk import UKCitation

        fetcher = UKLegislationFetcher(data_dir=tmp_path)

        with patch.object(fetcher, '_fetch_xml', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_SECTION_RESPONSE

            citation = UKCitation(type="ukpga", year=2003, number=1, section="62")
            await fetcher.fetch_section(citation, cache=True)

            # Check cache file exists
            cache_path = tmp_path / "ukpga" / "2003" / "1" / "section-62.xml"
            assert cache_path.exists()


class TestUKLegislationSearch:
    """Tests for searching UK legislation."""

    def test_build_search_url(self):
        """Build search API URL."""
        from axiom_corpus.fetchers.legislation_uk import UKLegislationFetcher

        fetcher = UKLegislationFetcher()
        url = fetcher.build_search_url("earnings", type="ukpga", year=2003)
        assert "text=earnings" in url
        assert "type=ukpga" in url
        assert "year=2003" in url


class TestPriorityActs:
    """Tests for priority legislation list."""

    def test_priority_acts_defined(self):
        """Priority Acts are defined for PolicyEngine."""
        from axiom_corpus.fetchers.legislation_uk import UK_PRIORITY_ACTS

        # Should include key tax/benefits legislation
        assert any("2003/1" in act for act in UK_PRIORITY_ACTS)  # ITEPA
        assert any("2007/3" in act for act in UK_PRIORITY_ACTS)  # ITA


class TestRateLimiting:
    """Tests for rate limiting compliance."""

    def test_rate_limit_delay(self):
        """Fetcher respects rate limit delay."""
        from axiom_corpus.fetchers.legislation_uk import UKLegislationFetcher

        # Default should be reasonable (legislation.gov.uk allows 3000/5min = 10/sec)
        fetcher = UKLegislationFetcher()
        assert fetcher.rate_limit_delay >= 0.1  # At least 100ms between requests
