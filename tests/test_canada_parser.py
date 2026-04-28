"""Tests for Canadian statute parser."""

from unittest.mock import patch

import pytest

from axiom_corpus.parsers.canada.statutes import CanadaStatuteParser, download_act

SAMPLE_CANADA_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Statute xmlns:lims="http://justice.gc.ca/lims"
         lims:inforce-start-date="1985-01-01"
         lims:lastAmendedDate="2024-06-15"
         bill-origin="commons"
         bill-type="Government"
         in-force="yes">
  <Identification>
    <ConsolidatedNumber>I-3.3</ConsolidatedNumber>
    <ShortTitle>Income Tax Act</ShortTitle>
    <LongTitle>An Act respecting income taxes</LongTitle>
  </Identification>
  <Body>
    <Section lims:id="sec-1"
             lims:inforce-start-date="1985-01-01"
             lims:lastAmendedDate="2023-12-01">
      <Label>1</Label>
      <MarginalNote>Short title</MarginalNote>
      <Text>This Act may be cited as the Income Tax Act.</Text>
    </Section>
    <Section lims:id="sec-2"
             lims:inforce-start-date="1985-01-01">
      <Label>2</Label>
      <MarginalNote>Definitions</MarginalNote>
      <Text>In this Act, the following definitions apply.</Text>
      <Subsection>
        <Label>(1)</Label>
        <Text>amount means any amount.</Text>
        <Paragraph>
          <Label>(a)</Label>
          <Text>First paragraph.</Text>
          <Subparagraph>
            <Label>(i)</Label>
            <Text>First subparagraph.</Text>
            <Clause>
              <Label>(A)</Label>
              <Text>First clause.</Text>
            </Clause>
          </Subparagraph>
        </Paragraph>
      </Subsection>
      <Subsection>
        <Label>(2)</Label>
        <MarginalNote>Interpretation</MarginalNote>
        <Text>For greater certainty.</Text>
      </Subsection>
      <HistoricalNoteSubItem>R.S., 1985, c. 1 (5th Supp.).</HistoricalNoteSubItem>
      <HistoricalNoteSubItem>[NOTE: This is ignored]</HistoricalNoteSubItem>
      <XRefExternal link="C-46">Criminal Code</XRefExternal>
      <XRefInternal>3</XRefInternal>
    </Section>
    <Section>
      <Label>3</Label>
      <Text>Binding on Her Majesty. See also reference to <XRefInternal>section 2</XRefInternal>.</Text>
    </Section>
  </Body>
</Statute>
"""


@pytest.fixture
def xml_file(tmp_path):
    """Create a temporary XML file with sample Canada statute."""
    xml_path = tmp_path / "I-3.3.xml"
    xml_path.write_text(SAMPLE_CANADA_XML, encoding="utf-8")
    return xml_path


@pytest.fixture
def parser(xml_file):
    return CanadaStatuteParser(xml_file)


class TestCanadaStatuteParser:
    def test_init(self, xml_file):
        parser = CanadaStatuteParser(xml_file)
        assert parser.xml_path == xml_file

    def test_init_with_str(self, xml_file):
        parser = CanadaStatuteParser(str(xml_file))
        assert parser.xml_path == xml_file

    def test_tree_lazy_load(self, parser):
        assert parser._tree is None
        _ = parser.tree
        assert parser._tree is not None

    def test_get_consolidated_number(self, parser):
        assert parser.get_consolidated_number() == "I-3.3"

    def test_get_consolidated_number_fallback(self, tmp_path):
        xml = "<Statute><Identification></Identification></Statute>"
        path = tmp_path / "C-46.xml"
        path.write_text(xml, encoding="utf-8")
        p = CanadaStatuteParser(path)
        assert p.get_consolidated_number() == "C-46"

    def test_get_short_title(self, parser):
        assert parser.get_short_title() == "Income Tax Act"

    def test_get_short_title_missing(self, tmp_path):
        xml = "<Statute><Identification></Identification></Statute>"
        path = tmp_path / "test.xml"
        path.write_text(xml, encoding="utf-8")
        p = CanadaStatuteParser(path)
        assert p.get_short_title() == ""

    def test_get_long_title(self, parser):
        assert parser.get_long_title() == "An Act respecting income taxes"

    def test_get_long_title_missing(self, tmp_path):
        xml = "<Statute><Identification></Identification></Statute>"
        path = tmp_path / "test.xml"
        path.write_text(xml, encoding="utf-8")
        p = CanadaStatuteParser(path)
        assert p.get_long_title() == ""

    def test_get_act_metadata(self, parser):
        act = parser.get_act_metadata()
        assert act.consolidated_number == "I-3.3"
        assert act.short_title == "Income Tax Act"
        assert act.long_title == "An Act respecting income taxes"
        assert act.bill_origin == "commons"
        assert act.bill_type == "Government"
        assert act.in_force is True
        assert act.section_count == 3
        assert "I-3.3" in act.source_url

    def test_get_act_metadata_dates(self, parser):
        act = parser.get_act_metadata()
        from datetime import date
        assert act.in_force_date == date(1985, 1, 1)
        assert act.last_amended_date == date(2024, 6, 15)

    def test_get_act_metadata_bad_dates(self, tmp_path):
        xml = '<Statute xmlns:lims="http://justice.gc.ca/lims" lims:inforce-start-date="bad" lims:lastAmendedDate="also-bad"><Identification><ConsolidatedNumber>X-1</ConsolidatedNumber></Identification><Body></Body></Statute>'
        path = tmp_path / "test.xml"
        path.write_text(xml, encoding="utf-8")
        p = CanadaStatuteParser(path)
        act = p.get_act_metadata()
        assert act.in_force_date is None
        assert act.last_amended_date is None

    def test_iter_sections(self, parser):
        sections = list(parser.iter_sections())
        assert len(sections) == 3
        assert sections[0].section_number == "1"
        assert sections[0].marginal_note == "Short title"
        assert "Income Tax Act" in sections[0].text

    def test_iter_sections_with_subsections(self, parser):
        sections = list(parser.iter_sections())
        section2 = sections[1]
        assert section2.section_number == "2"
        assert len(section2.subsections) >= 2
        assert section2.subsections[0].label == "(1)"

    def test_iter_sections_nested_subsections(self, parser):
        sections = list(parser.iter_sections())
        section2 = sections[1]
        # Check paragraph inside subsection
        sub1 = section2.subsections[0]
        assert len(sub1.children) >= 1  # has paragraph

    def test_iter_sections_historical_notes(self, parser):
        sections = list(parser.iter_sections())
        section2 = sections[1]
        # Should have one historical note (the [NOTE:] one is filtered)
        assert len(section2.historical_notes) >= 1
        assert "R.S." in section2.historical_notes[0]

    def test_iter_sections_references(self, parser):
        sections = list(parser.iter_sections())
        section2 = sections[1]
        # Should have external reference (C-46) and internal (s. 3)
        assert any("C-46" in r for r in section2.references_to)
        assert any("s. 3" in r for r in section2.references_to)

    def test_get_section(self, parser):
        section = parser.get_section("1")
        assert section is not None
        assert section.section_number == "1"

    def test_get_section_not_found(self, parser):
        section = parser.get_section("999")
        assert section is None

    def test_section_dates(self, parser):
        from datetime import date
        sections = list(parser.iter_sections())
        section1 = sections[0]
        assert section1.in_force_date == date(1985, 1, 1)
        assert section1.last_amended_date == date(2023, 12, 1)

    def test_section_bad_dates(self, tmp_path):
        xml = """\
<Statute xmlns:lims="http://justice.gc.ca/lims">
  <Identification><ConsolidatedNumber>T-1</ConsolidatedNumber></Identification>
  <Body>
    <Section lims:inforce-start-date="bad" lims:lastAmendedDate="bad">
      <Label>1</Label>
      <Text>Test</Text>
    </Section>
  </Body>
</Statute>"""
        path = tmp_path / "test.xml"
        path.write_text(xml, encoding="utf-8")
        p = CanadaStatuteParser(path)
        sections = list(p.iter_sections())
        assert len(sections) == 1
        assert sections[0].in_force_date is None

    def test_section_without_label(self, tmp_path):
        xml = """\
<Statute xmlns:lims="http://justice.gc.ca/lims">
  <Identification><ConsolidatedNumber>T-1</ConsolidatedNumber></Identification>
  <Body>
    <Section><Text>No label section</Text></Section>
  </Body>
</Statute>"""
        path = tmp_path / "test.xml"
        path.write_text(xml, encoding="utf-8")
        p = CanadaStatuteParser(path)
        sections = list(p.iter_sections())
        assert len(sections) == 0

    def test_section_source_url(self, parser):
        sections = list(parser.iter_sections())
        assert "I-3.3" in sections[0].source_url
        assert "section-1" in sections[0].source_url


class TestDownloadAct:
    @patch("httpx.Client")
    def test_download_act(self, mock_client_cls, tmp_path):
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.get.return_value.content = b"<Statute>test</Statute>"
        mock_client.get.return_value.raise_for_status = lambda: None

        result = download_act("I-3.3", tmp_path)

        assert result == tmp_path / "I-3.3.xml"
        assert result.read_bytes() == b"<Statute>test</Statute>"
