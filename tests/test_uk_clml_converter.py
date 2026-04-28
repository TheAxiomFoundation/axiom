"""Tests for UK CLML converter."""

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from axiom.converters.uk_clml import UKCLMLConverter
from axiom.models_uk import UKAct, UKSection

# Sample CLML XML for testing (Finance Act 2024 section 1)
SAMPLE_SECTION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Legislation xmlns="http://www.legislation.gov.uk/namespaces/legislation"
             xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata"
             xmlns:dc="http://purl.org/dc/elements/1.1/"
             DocumentURI="http://www.legislation.gov.uk/ukpga/2024/3/section/1"
             RestrictExtent="E+W+S+N.I.">
<ukm:Metadata>
    <dc:title>Finance Act 2024</dc:title>
    <ukm:PrimaryMetadata>
        <ukm:DocumentClassification>
            <ukm:DocumentCategory Value="primary"/>
            <ukm:DocumentMainType Value="UnitedKingdomPublicGeneralAct"/>
        </ukm:DocumentClassification>
        <ukm:Year Value="2024"/>
        <ukm:Number Value="3"/>
        <ukm:EnactmentDate Date="2024-02-22"/>
    </ukm:PrimaryMetadata>
</ukm:Metadata>
<Primary>
    <Body>
        <Part id="part-1">
            <Number>Part 1</Number>
            <Title>Income tax and corporation tax</Title>
            <Chapter id="part-1-chapter-1">
                <Number>Chapter 1</Number>
                <Title>Reliefs for businesses etc</Title>
                <P1group>
                    <Title>Permanent full expensing etc for expenditure on plant or machinery</Title>
                    <P1 DocumentURI="http://www.legislation.gov.uk/ukpga/2024/3/section/1" id="section-1">
                        <Pnumber>1</Pnumber>
                        <P1para>
                            <P2 id="section-1-1">
                                <Pnumber>1</Pnumber>
                                <P2para>
                                    <Text>In section 7 of F(No.2)A 2023, on the inserted section 45S of CAA 2001, omit "but before 1 April 2026".</Text>
                                </P2para>
                            </P2>
                            <P2 id="section-1-2">
                                <Pnumber>2</Pnumber>
                                <P2para>
                                    <Text>In consequence of the provision made by subsection (1):</Text>
                                    <P3 id="section-1-2-a">
                                        <Pnumber>a</Pnumber>
                                        <P3para>
                                            <Text>the amendments made by subsections (2) to (6) of section 7 are to operate as textual amendments, and</Text>
                                        </P3para>
                                    </P3>
                                    <P3 id="section-1-2-b">
                                        <Pnumber>b</Pnumber>
                                        <P3para>
                                            <Text>accordingly, in subsection (1) substitute "is amended as follows".</Text>
                                        </P3para>
                                    </P3>
                                </P2para>
                            </P2>
                        </P1para>
                    </P1>
                </P1group>
            </Chapter>
        </Part>
    </Body>
</Primary>
</Legislation>
"""

SAMPLE_ACT_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Legislation xmlns="http://www.legislation.gov.uk/namespaces/legislation"
             xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata"
             xmlns:dc="http://purl.org/dc/elements/1.1/"
             DocumentURI="http://www.legislation.gov.uk/ukpga/2024/3"
             NumberOfProvisions="671"
             RestrictExtent="E+W+S+N.I.">
<ukm:Metadata>
    <dc:title>Finance Act 2024</dc:title>
    <ukm:PrimaryMetadata>
        <ukm:Year Value="2024"/>
        <ukm:Number Value="3"/>
        <ukm:EnactmentDate Date="2024-02-22"/>
    </ukm:PrimaryMetadata>
    <ukm:ComingIntoForce>
        <ukm:DateTime Date="2024-02-22"/>
    </ukm:ComingIntoForce>
</ukm:Metadata>
<Primary>
    <Body>
        <Part id="part-1">
            <Number>Part 1</Number>
            <Title>Income tax and corporation tax</Title>
        </Part>
        <Part id="part-2">
            <Number>Part 2</Number>
            <Title>Capital gains tax</Title>
        </Part>
    </Body>
</Primary>
</Legislation>
"""


class TestUKCLMLConverter:
    """Tests for UKCLMLConverter."""

    def test_converter_initialization(self):
        """Initialize converter with default settings."""
        converter = UKCLMLConverter()
        assert converter is not None
        assert converter.base_url == "https://www.legislation.gov.uk"

    def test_converter_custom_settings(self, tmp_path):
        """Initialize converter with custom data directory."""
        converter = UKCLMLConverter(data_dir=tmp_path)
        assert converter.data_dir == tmp_path

    def test_build_url_for_act(self):
        """Build URL for fetching an entire Act."""
        converter = UKCLMLConverter()
        url = converter.build_url("ukpga/2024/3")
        assert url == "https://www.legislation.gov.uk/ukpga/2024/3/data.xml"

    def test_build_url_for_section(self):
        """Build URL for fetching a specific section."""
        converter = UKCLMLConverter()
        url = converter.build_url("ukpga/2024/3/section/1")
        assert url == "https://www.legislation.gov.uk/ukpga/2024/3/section/1/data.xml"

    def test_parse_citation_from_ref(self):
        """Parse citation from reference string."""
        converter = UKCLMLConverter()
        citation = converter.parse_reference("ukpga/2024/3")
        assert citation.type == "ukpga"
        assert citation.year == 2024
        assert citation.number == 3
        assert citation.section is None

    def test_parse_citation_with_section(self):
        """Parse citation with section from reference string."""
        converter = UKCLMLConverter()
        citation = converter.parse_reference("ukpga/2024/3/section/1")
        assert citation.type == "ukpga"
        assert citation.year == 2024
        assert citation.number == 3
        assert citation.section == "1"


class TestUKCLMLParseSection:
    """Tests for parsing section XML."""

    def test_parse_section_from_xml(self):
        """Parse a section from CLML XML string."""
        converter = UKCLMLConverter()
        section = converter.parse_section_xml(SAMPLE_SECTION_XML)

        assert section is not None
        assert isinstance(section, UKSection)
        assert section.citation.year == 2024
        assert section.citation.number == 3
        assert section.citation.section == "1"

    def test_parse_section_text(self):
        """Extract section text content."""
        converter = UKCLMLConverter()
        section = converter.parse_section_xml(SAMPLE_SECTION_XML)

        assert "section 7" in section.text.lower() or "CAA 2001" in section.text
        assert "omit" in section.text.lower()

    def test_parse_section_subsections(self):
        """Parse subsection structure."""
        converter = UKCLMLConverter()
        section = converter.parse_section_xml(SAMPLE_SECTION_XML)

        # Should have at least 2 subsections (P2 elements)
        assert len(section.subsections) >= 2

    def test_parse_section_nested_paragraphs(self):
        """Parse nested P3 paragraphs."""
        converter = UKCLMLConverter()
        section = converter.parse_section_xml(SAMPLE_SECTION_XML)

        # Find subsection 2 which has P3 children
        subsection_2 = next((s for s in section.subsections if s.id in ("2", "section-1-2")), None)
        if subsection_2:
            assert len(subsection_2.children) >= 2

    def test_parse_section_enacted_date(self):
        """Extract enactment date."""
        converter = UKCLMLConverter()
        section = converter.parse_section_xml(SAMPLE_SECTION_XML)

        assert section.enacted_date == date(2024, 2, 22)

    def test_parse_section_extent(self):
        """Parse territorial extent."""
        converter = UKCLMLConverter()
        section = converter.parse_section_xml(SAMPLE_SECTION_XML)

        assert "E" in section.extent
        assert "W" in section.extent
        assert "S" in section.extent
        assert "N.I." in section.extent

    def test_parse_section_source_url(self):
        """Extract source URL."""
        converter = UKCLMLConverter()
        section = converter.parse_section_xml(SAMPLE_SECTION_XML)

        assert section.source_url == "http://www.legislation.gov.uk/ukpga/2024/3/section/1"


class TestUKCLMLParseAct:
    """Tests for parsing Act metadata."""

    def test_parse_act_metadata(self):
        """Parse Act-level metadata."""
        converter = UKCLMLConverter()
        act = converter.parse_act_xml(SAMPLE_ACT_XML)

        assert act is not None
        assert isinstance(act, UKAct)
        assert act.title == "Finance Act 2024"
        assert act.citation.year == 2024
        assert act.citation.number == 3

    def test_parse_act_dates(self):
        """Parse enactment and commencement dates."""
        converter = UKCLMLConverter()
        act = converter.parse_act_xml(SAMPLE_ACT_XML)

        assert act.enacted_date == date(2024, 2, 22)
        assert act.commencement_date == date(2024, 2, 22)

    def test_parse_act_parts(self):
        """Parse parts structure."""
        converter = UKCLMLConverter()
        act = converter.parse_act_xml(SAMPLE_ACT_XML)

        assert len(act.parts) >= 2
        assert act.parts[0].title == "Income tax and corporation tax"
        assert act.parts[1].title == "Capital gains tax"

    def test_parse_act_section_count(self):
        """Extract section count from NumberOfProvisions."""
        converter = UKCLMLConverter()
        act = converter.parse_act_xml(SAMPLE_ACT_XML)

        assert act.section_count == 671


class TestUKCLMLFetch:
    """Tests for fetching legislation from API."""

    @pytest.mark.asyncio
    async def test_fetch_section(self, tmp_path):
        """Fetch a single section from the API."""
        converter = UKCLMLConverter(data_dir=tmp_path)

        with patch.object(converter, "_fetch_xml", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_SECTION_XML

            section = await converter.fetch("ukpga/2024/3/section/1")

            assert section is not None
            assert isinstance(section, UKSection)
            assert section.citation.section == "1"
            mock_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_act(self, tmp_path):
        """Fetch Act metadata from the API."""
        converter = UKCLMLConverter(data_dir=tmp_path)

        with patch.object(converter, "_fetch_xml", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_ACT_XML

            act = await converter.fetch("ukpga/2024/3")

            assert act is not None
            assert isinstance(act, UKAct)
            assert act.title == "Finance Act 2024"
            mock_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_with_caching(self, tmp_path):
        """Fetched XML is cached to disk."""
        converter = UKCLMLConverter(data_dir=tmp_path)

        with patch.object(converter, "_fetch_xml", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_SECTION_XML

            await converter.fetch("ukpga/2024/3/section/1", cache=True)

            # Check cache file exists
            cache_path = tmp_path / "ukpga" / "2024" / "3" / "section-1.xml"
            assert cache_path.exists()

    @pytest.mark.asyncio
    async def test_fetch_uses_cache(self, tmp_path):
        """Fetch uses cached file if available."""
        converter = UKCLMLConverter(data_dir=tmp_path)

        # Pre-create cache file
        cache_path = tmp_path / "ukpga" / "2024" / "3" / "section-1.xml"
        cache_path.parent.mkdir(parents=True)
        cache_path.write_text(SAMPLE_SECTION_XML)

        with patch.object(converter, "_fetch_xml", new_callable=AsyncMock) as mock_fetch:
            section = await converter.fetch("ukpga/2024/3/section/1", cache=True)

            # Should not call API since cache exists
            mock_fetch.assert_not_called()
            assert section.citation.section == "1"


class TestUKCLMLLegislationTypes:
    """Tests for different legislation types."""

    def test_parse_ukpga(self):
        """Parse UK Public General Act."""
        converter = UKCLMLConverter()
        citation = converter.parse_reference("ukpga/2024/3")
        assert citation.type == "ukpga"

    def test_parse_uksi(self):
        """Parse UK Statutory Instrument."""
        converter = UKCLMLConverter()
        citation = converter.parse_reference("uksi/2024/832")
        assert citation.type == "uksi"
        assert citation.year == 2024
        assert citation.number == 832

    def test_parse_asp(self):
        """Parse Act of Scottish Parliament."""
        converter = UKCLMLConverter()
        citation = converter.parse_reference("asp/2020/13")
        assert citation.type == "asp"

    def test_parse_asc(self):
        """Parse Act of Senedd Cymru (Welsh Parliament)."""
        converter = UKCLMLConverter()
        citation = converter.parse_reference("asc/2021/4")
        assert citation.type == "asc"


class TestUKCLMLSyncFetch:
    """Tests for synchronous fetch wrapper."""

    def test_fetch_sync(self, tmp_path):
        """Synchronous fetch wrapper."""
        converter = UKCLMLConverter(data_dir=tmp_path)

        with patch.object(converter, "_fetch_xml", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_SECTION_XML

            # Use the sync wrapper
            section = converter.fetch_sync("ukpga/2024/3/section/1")

            assert section is not None
            assert section.citation.section == "1"
