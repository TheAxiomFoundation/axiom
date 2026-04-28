"""Tests for Michigan Compiled Laws XML converter."""

from datetime import date
from unittest.mock import MagicMock, Mock, patch

import pytest

from axiom.converters.us_states.mi import (
    MCLChapter,
    MCLCitation,
    MCLHistory,
    MCLSection,
    MichiganConverter,
    parse_body_text,
)

# Sample XML from Michigan legislature.mi.gov
SAMPLE_CHAPTER_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<MCLChapterInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <DocumentID>11034</DocumentID>
  <Repealed>false</Repealed>
  <Name>206</Name>
  <Title>INCOME TAX ACT OF 1967</Title>
  <MultiChapter>false</MultiChapter>
  <MCLDocumentInfoCollection>
    <MCLStatuteInfo>
      <DocumentID>11035</DocumentID>
      <Repealed>false</Repealed>
      <Name>Act 281 of 1967</Name>
      <Heading>INCOME TAX ACT OF 1967</Heading>
      <LongTitle>AN ACT to meet deficiencies in state funds...</LongTitle>
      <ShortTitle>Income tax act of 1967</ShortTitle>
      <MCLDocumentInfoCollection>
        <MCLDivisionInfo>
          <DocumentID>47903</DocumentID>
          <Repealed>false</Repealed>
          <Name>206.1.D10</Name>
          <DivisionNumber>1</DivisionNumber>
          <DivisionType>PART</DivisionType>
          <MCLDocumentInfoCollection>
            <MCLDivisionInfo>
              <DocumentID>47902</DocumentID>
              <Name>206.1.D20.new</Name>
              <DivisionNumber>1</DivisionNumber>
              <DivisionType>Chapter</DivisionType>
              <MCLDocumentInfoCollection>
                <MCLSectionInfo>
                  <DocumentID>11037</DocumentID>
                  <Repealed>false</Repealed>
                  <HistoryText>&lt;HistoryData&gt;1967, Act 281, Eff. Oct. 1, 1967&lt;/HistoryData&gt;</HistoryText>
                  <History>
                    <HistoryInfo>
                      <EffectiveDate>1967-10-01</EffectiveDate>
                      <Action>New</Action>
                      <Legislation>
                        <Type>PA</Type>
                        <Number>281</Number>
                        <Year>1967</Year>
                      </Legislation>
                    </HistoryInfo>
                  </History>
                  <MCLNumber>206.1</MCLNumber>
                  <CatchLine>Income tax act of 1967; short title.</CatchLine>
                  <Label>1</Label>
                  <SectRef>206.1</SectRef>
                  <BodyText>&lt;Section-Number&gt;Sec. 1.&lt;/Section-Number&gt;&lt;Paragraph&gt;&lt;P&gt;This act is for the purpose of meeting deficiencies in state funds and shall be known and may be cited as the "income tax act of 1967".&lt;/P&gt;&lt;/Paragraph&gt;</BodyText>
                </MCLSectionInfo>
                <MCLSectionInfo>
                  <DocumentID>47825</DocumentID>
                  <Repealed>false</Repealed>
                  <MCLNumber>206.2</MCLNumber>
                  <CatchLine>Rules of construction; internal revenue code.</CatchLine>
                  <Label>2</Label>
                  <SectRef>206.2</SectRef>
                  <BodyText>&lt;Section-Body&gt;&lt;Section-Number&gt;Sec. 2.&lt;/Section-Number&gt;&lt;Paragraph&gt;&lt;P&gt;(1) For the purposes of this part, the words have the meaning given.&lt;/P&gt;&lt;/Paragraph&gt;&lt;Paragraph&gt;&lt;P&gt;(2) Any term used in this part shall have the same meaning.&lt;/P&gt;&lt;/Paragraph&gt;&lt;/Section-Body&gt;</BodyText>
                </MCLSectionInfo>
              </MCLDocumentInfoCollection>
            </MCLDivisionInfo>
          </MCLDocumentInfoCollection>
        </MCLDivisionInfo>
      </MCLDocumentInfoCollection>
    </MCLStatuteInfo>
  </MCLDocumentInfoCollection>
</MCLChapterInfo>
"""

# Sample XML for a section with subsections (a)(b)(c) style
SAMPLE_SECTION_WITH_SUBSECTIONS = b"""<?xml version="1.0" encoding="utf-8"?>
<MCLSectionInfo>
  <MCLNumber>206.30</MCLNumber>
  <CatchLine>Taxable income defined.</CatchLine>
  <Label>30</Label>
  <BodyText>&lt;Section-Body&gt;&lt;Section-Number&gt;Sec. 30.&lt;/Section-Number&gt;&lt;Paragraph&gt;&lt;P&gt;(1) "Taxable income" means adjusted gross income.&lt;/P&gt;&lt;/Paragraph&gt;&lt;Paragraph&gt;&lt;P&gt;(a) Add gross interest income.&lt;/P&gt;&lt;/Paragraph&gt;&lt;Paragraph&gt;&lt;P&gt;(b) Add taxes on income.&lt;/P&gt;&lt;/Paragraph&gt;&lt;Paragraph&gt;&lt;P&gt;(2) Personal exemption allowed.&lt;/P&gt;&lt;/Paragraph&gt;&lt;/Section-Body&gt;</BodyText>
</MCLSectionInfo>
"""


class TestMCLCitation:
    """Test MCL citation parsing and formatting."""

    def test_from_mcl_number_simple(self):
        """Parse simple MCL number like 206.1."""
        citation = MCLCitation.from_mcl_number("206.1")
        assert citation.chapter == 206
        assert citation.section == "1"
        assert citation.subsection is None

    def test_from_mcl_number_complex(self):
        """Parse MCL number with letters like 206.30a."""
        citation = MCLCitation.from_mcl_number("206.30a")
        assert citation.chapter == 206
        assert citation.section == "30a"

    def test_cite_string(self):
        """Format citation as MCL string."""
        citation = MCLCitation(chapter=206, section="30")
        assert citation.cite_string == "MCL 206.30"

    def test_cite_string_with_subsection(self):
        """Format citation with subsection."""
        citation = MCLCitation(chapter=206, section="30", subsection="1")
        assert citation.cite_string == "MCL 206.30(1)"

    def test_path(self):
        """Generate filesystem path from citation."""
        citation = MCLCitation(chapter=206, section="30")
        assert citation.path == "state/mi/mcl/206/30"

    def test_path_with_subsection(self):
        """Generate path with subsection."""
        citation = MCLCitation(chapter=206, section="30", subsection="1/a")
        assert citation.path == "state/mi/mcl/206/30/1/a"


class TestBodyTextParser:
    """Test parsing of Michigan's HTML-in-XML body text."""

    def test_parse_simple_text(self):
        """Parse simple section text."""
        body = '<Section-Number>Sec. 1.</Section-Number><Paragraph><P>This is the text.</P></Paragraph>'
        text, subsections = parse_body_text(body)
        assert "This is the text." in text

    def test_parse_numbered_subsections(self):
        """Parse (1), (2) style subsections."""
        body = '<Section-Body><Paragraph><P>(1) First subsection.</P></Paragraph><Paragraph><P>(2) Second subsection.</P></Paragraph></Section-Body>'
        text, subsections = parse_body_text(body)
        assert len(subsections) == 2
        assert subsections[0].identifier == "1"
        assert subsections[1].identifier == "2"

    def test_parse_lettered_paragraphs(self):
        """Parse (a), (b) style paragraphs within subsection."""
        body = '<Section-Body><Paragraph><P>(1) Intro text:</P></Paragraph><Paragraph><P>(a) First item.</P></Paragraph><Paragraph><P>(b) Second item.</P></Paragraph></Section-Body>'
        text, subsections = parse_body_text(body)
        assert len(subsections) >= 1  # At least the (1) subsection
        # The (a) and (b) might be parsed as children or separate entries

    def test_preserve_text_content(self):
        """Ensure text content is preserved correctly."""
        body = '<P>The income tax act of 1967.</P>'
        text, _ = parse_body_text(body)
        assert "income tax act of 1967" in text.lower()


class TestMCLSection:
    """Test MCL section model."""

    def test_create_section(self):
        """Create a basic section."""
        section = MCLSection(
            document_id="11037",
            mcl_number="206.1",
            catch_line="Income tax act; short title.",
            label="1",
            body_text="This act shall be known as the income tax act.",
            repealed=False,
        )
        assert section.mcl_number == "206.1"
        assert section.catch_line == "Income tax act; short title."

    def test_section_with_history(self):
        """Create section with legislative history."""
        history = MCLHistory(
            effective_date=date(1967, 10, 1),
            action="New",
            public_act_number=281,
            public_act_year=1967,
        )
        section = MCLSection(
            document_id="11037",
            mcl_number="206.1",
            catch_line="Short title.",
            label="1",
            body_text="Text here.",
            repealed=False,
            history=[history],
        )
        assert len(section.history) == 1
        assert section.history[0].public_act_number == 281


class TestMCLChapter:
    """Test MCL chapter model."""

    def test_create_chapter(self):
        """Create a basic chapter."""
        chapter = MCLChapter(
            document_id="11034",
            chapter_number=206,
            title="INCOME TAX ACT OF 1967",
            repealed=False,
            sections=[],
        )
        assert chapter.chapter_number == 206
        assert chapter.title == "INCOME TAX ACT OF 1967"


class TestMichiganConverter:
    """Test Michigan XML converter."""

    def test_parse_chapter_xml(self):
        """Parse full chapter XML from Michigan legislature."""
        converter = MichiganConverter()
        chapter = converter.parse_chapter_xml(SAMPLE_CHAPTER_XML)

        assert chapter.chapter_number == 206
        assert chapter.title == "INCOME TAX ACT OF 1967"
        assert chapter.repealed is False

    def test_extract_sections(self):
        """Extract sections from chapter XML."""
        converter = MichiganConverter()
        chapter = converter.parse_chapter_xml(SAMPLE_CHAPTER_XML)

        # Should find sections 206.1 and 206.2
        assert len(chapter.sections) >= 2
        mcl_numbers = [s.mcl_number for s in chapter.sections]
        assert "206.1" in mcl_numbers
        assert "206.2" in mcl_numbers

    def test_section_text_extraction(self):
        """Extract and clean section body text."""
        converter = MichiganConverter()
        chapter = converter.parse_chapter_xml(SAMPLE_CHAPTER_XML)

        # Find section 206.1
        section = next(s for s in chapter.sections if s.mcl_number == "206.1")
        assert "income tax act of 1967" in section.body_text.lower()

    def test_chapter_url(self):
        """Generate correct download URL for chapter."""
        converter = MichiganConverter()
        url = converter.get_chapter_url(206)
        assert "legislature.mi.gov" in url
        assert "206" in url

    def test_list_chapters(self):
        """List available chapters from directory listing."""
        converter = MichiganConverter()
        # This would require mocking the HTTP response
        # For now just test the method exists
        assert hasattr(converter, "list_chapters")

    @patch("httpx.Client")
    def test_fetch_chapter(self, mock_client_class):
        """Test fetching a chapter from legislature.mi.gov."""
        mock_client = MagicMock()
        mock_response = Mock()
        mock_response.content = SAMPLE_CHAPTER_XML
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=False)
        mock_client_class.return_value = mock_client

        converter = MichiganConverter()
        chapter = converter.fetch_chapter(206)

        assert chapter.chapter_number == 206
        mock_client.get.assert_called_once()


class TestMichiganConverterIntegration:
    """Integration tests for Michigan converter (require network)."""

    @pytest.mark.integration
    def test_fetch_real_chapter_206(self):
        """Fetch real Chapter 206 (Income Tax Act) from Michigan."""
        converter = MichiganConverter()
        chapter = converter.fetch_chapter(206)

        assert chapter.chapter_number == 206
        assert "INCOME TAX" in chapter.title.upper()
        assert len(chapter.sections) > 10  # Should have many sections

    @pytest.mark.integration
    def test_fetch_section_206_30(self):
        """Fetch and parse the taxable income section."""
        converter = MichiganConverter()
        chapter = converter.fetch_chapter(206)

        # Find section 206.30 (Taxable income)
        section = next(
            (s for s in chapter.sections if s.mcl_number == "206.30"),
            None
        )
        assert section is not None
        assert "taxable income" in section.catch_line.lower()


class TestMCLToArchSection:
    """Test conversion from MCL models to Axiom section model."""

    def test_convert_to_arch_section(self):
        """Convert MCL section to Axiom section model."""

        converter = MichiganConverter()
        chapter = converter.parse_chapter_xml(SAMPLE_CHAPTER_XML)

        # Convert to Axiom sections
        arch_sections = list(converter.to_arch_sections(chapter))

        assert len(arch_sections) >= 2
        # Verify first section
        sec1 = next(s for s in arch_sections if "206.1" in s.citation.section)
        assert sec1.title_name == "Michigan Compiled Laws"
        assert "income tax" in sec1.section_title.lower()

    def test_arch_section_source_url(self):
        """Ensure source URL is set correctly."""
        converter = MichiganConverter()
        chapter = converter.parse_chapter_xml(SAMPLE_CHAPTER_XML)
        arch_sections = list(converter.to_arch_sections(chapter))

        for section in arch_sections:
            assert "legislature.mi.gov" in section.source_url
