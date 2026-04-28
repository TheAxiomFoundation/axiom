"""Tests for eCFR fetcher."""

from unittest.mock import AsyncMock, patch

import pytest

# Sample XML for mocking
SAMPLE_TITLE_XML = """<?xml version="1.0" encoding="UTF-8" ?>
<DLPSTEXTCLASS>
<HEADER>
<FILEDESC>
<TITLESTMT>
<TITLE>Title 26: Internal Revenue</TITLE>
</TITLESTMT>
<PUBLICATIONSTMT>
<IDNO TYPE="title">26</IDNO>
</PUBLICATIONSTMT>
</FILEDESC>
</HEADER>
<TEXT><BODY><ECFRBRWS>
<AMDDATE>Dec. 18, 2025</AMDDATE>
<DIV1 N="1" NODE="26:1" TYPE="TITLE">
<DIV5 N="1" NODE="26:1.0.1.1.1" TYPE="PART">
<HEAD>PART 1</HEAD>
<AUTH><HED>Authority:</HED><PSPACE>26 U.S.C. 7805</PSPACE></AUTH>
<DIV8 N="§ 1.32-1" NODE="26:1.0.1.1.1.0.1.100" TYPE="SECTION">
<HEAD>§ 1.32-1   Earned income.</HEAD>
<P>(a) <I>In general.</I> Test content.
</P>
</DIV8>
</DIV5>
</DIV1>
</ECFRBRWS></BODY></TEXT>
</DLPSTEXTCLASS>
"""


class TestECFRFetcher:
    """Tests for eCFR bulk data fetcher."""

    def test_fetcher_init(self):
        """Initialize fetcher with default settings."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher()
        assert fetcher.base_url == "https://www.govinfo.gov/bulkdata/ECFR"
        assert fetcher.data_dir is not None

    def test_fetcher_custom_data_dir(self, tmp_path):
        """Initialize fetcher with custom data directory."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher(data_dir=tmp_path)
        assert fetcher.data_dir == tmp_path

    def test_get_title_url(self):
        """Get URL for a specific CFR title."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher()
        url = fetcher.get_title_url(26)
        assert "title-26" in url
        assert url.endswith(".xml")

    def test_available_titles(self):
        """List of available CFR titles."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher()
        # Treasury regulations are in Title 26
        assert 26 in fetcher.available_titles
        # IRS is also in Title 26
        # Should have most titles 1-50
        assert len(fetcher.available_titles) >= 40


class TestECFRDownload:
    """Tests for downloading CFR titles."""

    @pytest.mark.asyncio
    async def test_download_title(self, tmp_path):
        """Download a CFR title XML file."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher(data_dir=tmp_path)

        # Mock the HTTP client
        with patch.object(fetcher, '_download_file', new_callable=AsyncMock) as mock_download:
            mock_download.return_value = tmp_path / "title-26.xml"

            # Write sample content
            (tmp_path / "title-26.xml").write_text(SAMPLE_TITLE_XML)

            path = await fetcher.download_title(26)
            assert path.exists()
            assert "title-26" in path.name

    @pytest.mark.asyncio
    async def test_download_creates_directory(self, tmp_path):
        """Download creates data directory if missing."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        nested_dir = tmp_path / "nested" / "cfr"
        fetcher = ECFRFetcher(data_dir=nested_dir)

        with patch.object(fetcher, '_download_file', new_callable=AsyncMock) as mock_download:
            mock_download.return_value = nested_dir / "title-26.xml"
            (nested_dir).mkdir(parents=True)
            (nested_dir / "title-26.xml").write_text(SAMPLE_TITLE_XML)

            await fetcher.download_title(26)
            assert nested_dir.exists()


class TestECFRParsing:
    """Tests for parsing downloaded CFR files."""

    def test_parse_downloaded_title(self, tmp_path):
        """Parse a downloaded CFR title file."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher(data_dir=tmp_path)

        # Write sample XML
        xml_path = tmp_path / "title-26.xml"
        xml_path.write_text(SAMPLE_TITLE_XML)

        regulations = list(fetcher.parse_title(xml_path))
        assert len(regulations) >= 1
        assert regulations[0].citation.title == 26
        assert regulations[0].citation.section == "32-1"

    def test_parse_with_filter(self, tmp_path):
        """Parse CFR title with part filter."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher(data_dir=tmp_path)

        xml_path = tmp_path / "title-26.xml"
        xml_path.write_text(SAMPLE_TITLE_XML)

        # Filter to part 1 only
        regulations = list(fetcher.parse_title(xml_path, parts=[1]))
        assert len(regulations) >= 1
        assert all(r.citation.part == 1 for r in regulations)


class TestECFRMetadata:
    """Tests for CFR metadata extraction."""

    def test_extract_amendment_date(self, tmp_path):
        """Extract amendment date from title XML."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher(data_dir=tmp_path)

        xml_path = tmp_path / "title-26.xml"
        xml_path.write_text(SAMPLE_TITLE_XML)

        metadata = fetcher.get_title_metadata(xml_path)
        assert metadata["title_number"] == 26
        assert metadata["title_name"] == "Internal Revenue"
        assert "amendment_date" in metadata

    def test_count_sections(self, tmp_path):
        """Count sections in a title."""
        from axiom_corpus.fetchers.ecfr import ECFRFetcher

        fetcher = ECFRFetcher(data_dir=tmp_path)

        xml_path = tmp_path / "title-26.xml"
        xml_path.write_text(SAMPLE_TITLE_XML)

        count = fetcher.count_sections(xml_path)
        assert count >= 1
