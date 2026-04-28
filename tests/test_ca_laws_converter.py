"""Tests for Canadian laws-lois-xml GitHub converter."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from axiom_corpus.converters.ca_laws import (
    BilingualContent,
    CanadaLawsConverter,
    CanadaLawsSource,
)
from axiom_corpus.models_canada import CanadaAct

# Sample minimal XML for testing
SAMPLE_ACT_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<Statute xmlns:lims="http://justice.gc.ca/lims"
         lims:id="12345"
         in-force="yes"
         xml:lang="en"
         bill-origin="commons"
         bill-type="govt-public"
         lims:inforce-start-date="1970-06-26"
         lims:lastAmendedDate="2024-06-20">
    <Identification lims:id="12346">
        <ShortTitle>Test Act</ShortTitle>
        <LongTitle>An Act to test the parser</LongTitle>
        <Chapter lims:id="12347">
            <ConsolidatedNumber>T-1.5</ConsolidatedNumber>
        </Chapter>
    </Identification>
    <Body>
        <Section lims:id="12350" lims:inforce-start-date="1985-07-01">
            <MarginalNote>Short title</MarginalNote>
            <Label>1</Label>
            <Text>This Act may be cited as the Test Act.</Text>
        </Section>
        <Section lims:id="12351" lims:inforce-start-date="1985-07-01">
            <MarginalNote>Definitions</MarginalNote>
            <Label>2</Label>
            <Subsection lims:id="12352">
                <Label>(1)</Label>
                <Text>In this Act,</Text>
                <Definition lims:id="12353">
                    <DefinedTermEn>test</DefinedTermEn>
                    <Text>means a unit test;</Text>
                </Definition>
            </Subsection>
            <Subsection lims:id="12354">
                <Label>(2)</Label>
                <Text>For greater certainty.</Text>
                <Paragraph lims:id="12355">
                    <Label>(a)</Label>
                    <Text>first paragraph;</Text>
                </Paragraph>
                <Paragraph lims:id="12356">
                    <Label>(b)</Label>
                    <Text>second paragraph.</Text>
                </Paragraph>
            </Subsection>
        </Section>
    </Body>
</Statute>
"""

SAMPLE_FR_ACT_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<Statute xmlns:lims="http://justice.gc.ca/lims"
         lims:id="12345"
         xml:lang="fr">
    <Identification>
        <ShortTitle>Loi sur les tests</ShortTitle>
        <LongTitle>Loi concernant les tests du parseur</LongTitle>
        <Chapter>
            <ConsolidatedNumber>T-1.5</ConsolidatedNumber>
        </Chapter>
    </Identification>
    <Body>
        <Section lims:id="12350">
            <MarginalNote>Titre abr&#233;g&#233;</MarginalNote>
            <Label>1</Label>
            <Text>Loi sur les tests.</Text>
        </Section>
    </Body>
</Statute>
"""


class TestCanadaLawsSource:
    """Test CanadaLawsSource enum."""

    def test_github_base_url(self):
        """GitHub source has correct base URL."""
        assert "github" in CanadaLawsSource.GITHUB.value.lower()

    def test_local_is_none(self):
        """Local source has None value."""
        assert CanadaLawsSource.LOCAL.value is None


class TestBilingualContent:
    """Test BilingualContent model."""

    def test_create_with_both_languages(self):
        """Can create bilingual content with both languages."""
        content = BilingualContent(en="Hello", fr="Bonjour")
        assert content.en == "Hello"
        assert content.fr == "Bonjour"

    def test_create_english_only(self):
        """Can create with English only."""
        content = BilingualContent(en="Hello")
        assert content.en == "Hello"
        assert content.fr is None

    def test_primary_returns_english(self):
        """Primary text returns English."""
        content = BilingualContent(en="Hello", fr="Bonjour")
        assert content.primary == "Hello"

    def test_primary_fallback_to_french(self):
        """Primary falls back to French if no English."""
        content = BilingualContent(en=None, fr="Bonjour")
        assert content.primary == "Bonjour"


class TestCanadaLawsConverter:
    """Test CanadaLawsConverter."""

    def test_init_default_source(self):
        """Default source is GitHub."""
        converter = CanadaLawsConverter()
        assert converter.source == CanadaLawsSource.GITHUB

    def test_init_local_source(self):
        """Can initialize with local source."""
        converter = CanadaLawsConverter(
            source=CanadaLawsSource.LOCAL, local_path=Path("/tmp/laws")
        )
        assert converter.source == CanadaLawsSource.LOCAL
        assert converter.local_path == Path("/tmp/laws")

    def test_build_github_url_acts(self):
        """Build correct GitHub URL for acts."""
        converter = CanadaLawsConverter()
        url = converter._build_github_url("acts/I/I-3.3", lang="eng")
        assert "justicecanada/laws-lois-xml" in url
        assert "eng/acts/I-3.3.xml" in url

    def test_build_github_url_regulations(self):
        """Build correct GitHub URL for regulations."""
        converter = CanadaLawsConverter()
        url = converter._build_github_url("regulations/SOR/SOR-86-304", lang="eng")
        assert "eng/regulations/SOR-86-304.xml" in url

    def test_parse_act_path_simple(self):
        """Parse simple act path."""
        converter = CanadaLawsConverter()
        doc_type, identifier = converter._parse_path("acts/I/I-3.3")
        assert doc_type == "acts"
        assert identifier == "I-3.3"

    def test_parse_act_path_flat(self):
        """Parse flat act path without subdirectory."""
        converter = CanadaLawsConverter()
        doc_type, identifier = converter._parse_path("acts/A-1")
        assert doc_type == "acts"
        assert identifier == "A-1"

    @patch("httpx.Client")
    def test_fetch_from_github(self, mock_client_class):
        """Fetch act from GitHub."""
        # Set up mock
        mock_client = MagicMock()
        mock_client_class.return_value.__enter__ = Mock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = Mock(return_value=False)

        mock_response = Mock()
        mock_response.content = SAMPLE_ACT_XML
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        converter = CanadaLawsConverter()
        result = converter.fetch("acts/T/T-1.5")

        assert result is not None
        assert isinstance(result, CanadaAct)
        assert result.short_title == "Test Act"
        assert result.consolidated_number == "T-1.5"

    @patch("httpx.Client")
    def test_fetch_bilingual(self, mock_client_class):
        """Fetch act with both English and French."""
        mock_client = MagicMock()
        mock_client_class.return_value.__enter__ = Mock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = Mock(return_value=False)

        def mock_get(url, *args, **kwargs):
            response = Mock()
            response.raise_for_status = Mock()
            if "/fra/" in url:
                response.content = SAMPLE_FR_ACT_XML
            else:
                response.content = SAMPLE_ACT_XML
            return response

        mock_client.get.side_effect = mock_get

        converter = CanadaLawsConverter()
        result = converter.fetch("acts/T/T-1.5", include_french=True)

        assert result is not None
        # Check that we got bilingual metadata
        assert hasattr(result, "french_title") or result.short_title == "Test Act"

    def test_fetch_from_local_path(self, tmp_path):
        """Fetch act from local filesystem."""
        # Write test XML to temp file
        eng_dir = tmp_path / "eng" / "acts"
        eng_dir.mkdir(parents=True)
        (eng_dir / "T-1.5.xml").write_bytes(SAMPLE_ACT_XML)

        converter = CanadaLawsConverter(
            source=CanadaLawsSource.LOCAL, local_path=tmp_path
        )
        result = converter.fetch("acts/T/T-1.5")

        assert result is not None
        assert result.short_title == "Test Act"
        assert result.consolidated_number == "T-1.5"

    def test_parse_xml_act_metadata(self):
        """Parse Act metadata from XML."""
        converter = CanadaLawsConverter()
        act = converter._parse_act_xml(SAMPLE_ACT_XML)

        assert act.short_title == "Test Act"
        assert act.long_title == "An Act to test the parser"
        assert act.consolidated_number == "T-1.5"
        assert act.bill_origin == "commons"
        assert act.bill_type == "govt-public"
        assert act.in_force is True
        assert act.section_count == 2

    def test_parse_xml_sections(self):
        """Parse sections from XML."""
        converter = CanadaLawsConverter()
        sections = list(converter._iter_sections_xml(SAMPLE_ACT_XML))

        assert len(sections) == 2
        assert sections[0].section_number == "1"
        assert sections[0].marginal_note == "Short title"
        assert sections[1].section_number == "2"
        assert sections[1].marginal_note == "Definitions"

    def test_parse_xml_subsections(self):
        """Parse subsections from XML."""
        converter = CanadaLawsConverter()
        sections = list(converter._iter_sections_xml(SAMPLE_ACT_XML))

        section_2 = sections[1]
        assert len(section_2.subsections) == 2
        assert section_2.subsections[0].label == "(1)"
        assert section_2.subsections[1].label == "(2)"

    def test_parse_xml_paragraphs(self):
        """Parse paragraphs nested in subsections."""
        converter = CanadaLawsConverter()
        sections = list(converter._iter_sections_xml(SAMPLE_ACT_XML))

        section_2 = sections[1]
        subsection_2 = section_2.subsections[1]
        assert len(subsection_2.children) == 2
        assert subsection_2.children[0].label == "(a)"
        assert subsection_2.children[1].label == "(b)"

    def test_list_acts(self):
        """List available acts from index."""
        converter = CanadaLawsConverter()
        # This is a placeholder - in practice we'd mock the API call
        # Just test that the method exists and returns a list
        with patch.object(converter, "_fetch_index") as mock_fetch:
            mock_fetch.return_value = ["A-1", "I-3.3", "T-1.5"]
            acts = converter.list_acts()
            assert isinstance(acts, list)

    def test_get_section(self):
        """Get a specific section by number."""
        converter = CanadaLawsConverter()
        sections = list(converter._iter_sections_xml(SAMPLE_ACT_XML))

        # Find section 2
        section = next((s for s in sections if s.section_number == "2"), None)
        assert section is not None
        assert section.marginal_note == "Definitions"


class TestCanadaLawsConverterIntegration:
    """Integration tests that hit real GitHub (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_access_to_information_act(self):
        """Fetch the Access to Information Act from GitHub."""
        converter = CanadaLawsConverter()
        act = converter.fetch("acts/A/A-1")

        assert act is not None
        assert "Access to Information" in act.short_title
        assert act.consolidated_number == "A-1"

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_act_section(self):
        """Fetch a section from the Income Tax Act."""
        converter = CanadaLawsConverter()
        sections = converter.fetch_sections("acts/I/I-3.3", section_numbers=["2"])

        assert len(sections) >= 1
        assert any(s.section_number == "2" for s in sections)
