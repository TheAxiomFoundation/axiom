"""Tests for Minnesota Statutes to AKN converter."""

from xml.etree import ElementTree as ET

import pytest

from axiom_corpus.converters.mn_statutes import MNSection, MNStatutesToAKN, MNSubsection

SAMPLE_MN_HTML = """\
<html>
<body>
<div id="breadcrumb">
  <a href="/statutes/">2025 Minnesota Statutes</a>
  <a href="/statutes/part/CRIMES">CRIMES; EXPUNGEMENT; VICTIMS</a>
</div>
<div id="xtend" class="statute">
  <div class="section">
    <h1 class="shn">609.75 GAMBLING; DEFINITIONS.</h1>
    <p class="para">General text paragraph.</p>
    <div class="subd" id="stat.609.75.1">
      <h2 class="subd_no">Subd. 1. <span class="headnote">Acts constituting.</span></h2>
      <p>A person who makes a bet is gambling.</p>
    </div>
    <div class="subd" id="stat.609.75.2">
      <h2 class="subd_no">Subd. 2. <span class="headnote">Bet.</span></h2>
      <p>A bet is a bargain whereby the parties mutually agree.</p>
      <p>Additional paragraph for subdivision 2.</p>
    </div>
  </div>
  <div class="history">
    <p class="first">1963 c 753 art 1 s 609.75</p>
  </div>
</div>
</body>
</html>
"""

REPEALED_MN_HTML = """\
<html>
<body>
<div id="breadcrumb">
  <a href="/statutes/">2025 Minnesota Statutes</a>
</div>
<div id="xtend" class="statute">
  <div class="sr">105.63 [Repealed, 1990 c 391 art 10 s 4]</div>
</div>
</body>
</html>
"""

MINIMAL_MN_HTML = """\
<html>
<body>
<div id="xtend" class="statute">
  <div class="section">
    <h1 class="shn">Simple Text.</h1>
    <p>Direct text content only.</p>
  </div>
</div>
</body>
</html>
"""


@pytest.fixture
def converter():
    return MNStatutesToAKN()


class TestParseCitationFromFilename:
    def test_standard_filename(self, converter):
        assert converter.parse_citation_from_filename("statutes_cite_609.75.html") == "609.75"

    def test_chapter_only(self, converter):
        assert converter.parse_citation_from_filename("statutes_cite_105.html") == "105"

    def test_no_match(self, converter):
        assert converter.parse_citation_from_filename("other_file.html") == ""


class TestParseHtml:
    def test_basic_section(self, converter):
        section = converter.parse_html(SAMPLE_MN_HTML, filename="statutes_cite_609.75.html")
        assert section.citation == "609.75"
        assert section.chapter == "609"
        assert section.title == "GAMBLING; DEFINITIONS."
        assert section.year == "2025"
        assert section.part_name == "CRIMES; EXPUNGEMENT; VICTIMS"

    def test_subdivisions(self, converter):
        section = converter.parse_html(SAMPLE_MN_HTML, filename="statutes_cite_609.75.html")
        assert len(section.subdivisions) == 2
        assert section.subdivisions[0].identifier == "1"
        assert section.subdivisions[0].headnote == "Acts constituting."
        assert section.subdivisions[1].identifier == "2"
        assert section.subdivisions[1].headnote == "Bet."

    def test_subdivision_paragraphs(self, converter):
        section = converter.parse_html(SAMPLE_MN_HTML, filename="statutes_cite_609.75.html")
        assert len(section.subdivisions[1].paragraphs) >= 2

    def test_history(self, converter):
        section = converter.parse_html(SAMPLE_MN_HTML, filename="statutes_cite_609.75.html")
        assert "1963" in section.history

    def test_repealed_section(self, converter):
        section = converter.parse_html(REPEALED_MN_HTML, filename="statutes_cite_105.63.html")
        assert section.is_repealed is True
        assert "1990 c 391 art 10 s 4" in section.repealed_by

    def test_minimal_html(self, converter):
        section = converter.parse_html(MINIMAL_MN_HTML, filename="statutes_cite_1.html")
        assert section.citation == "1"

    def test_source_url_preserved(self, converter):
        section = converter.parse_html(
            SAMPLE_MN_HTML,
            source_url="https://revisor.mn.gov/statutes/cite/609.75",
            filename="statutes_cite_609.75.html",
        )
        assert section.source_url == "https://revisor.mn.gov/statutes/cite/609.75"

    def test_not_repealed(self, converter):
        section = converter.parse_html(SAMPLE_MN_HTML, filename="statutes_cite_609.75.html")
        assert section.is_repealed is False


class TestToAknXml:
    def test_basic_xml_output(self, converter):
        section = MNSection(
            citation="609.75",
            chapter="609",
            title="GAMBLING; DEFINITIONS.",
            part_name="CRIMES",
            year="2025",
        )
        xml = converter.to_akn_xml(section)
        assert "akomaNtoso" in xml
        assert "609.75" in xml
        assert "GAMBLING; DEFINITIONS." in xml

    def test_repealed_section_xml(self, converter):
        section = MNSection(
            citation="105.63",
            chapter="105",
            title="",
            part_name="",
            year="2025",
            is_repealed=True,
            repealed_by="1990 c 391 art 10 s 4",
        )
        xml = converter.to_akn_xml(section)
        assert "Repealed" in xml
        assert "status" in xml

    def test_xml_with_subdivisions(self, converter):
        section = MNSection(
            citation="609.75",
            chapter="609",
            title="GAMBLING",
            part_name="",
            year="2025",
            subdivisions=[
                MNSubsection(identifier="1", headnote="Definitions", paragraphs=["Text."]),
                MNSubsection(identifier="2", headnote="Penalty", paragraphs=["More text."]),
            ],
        )
        xml = converter.to_akn_xml(section)
        assert "subsection" in xml
        assert "Subd. 1" in xml
        assert "Subd. 2" in xml

    def test_xml_with_direct_text(self, converter):
        section = MNSection(
            citation="1.01",
            chapter="1",
            title="General Provisions",
            part_name="",
            year="2025",
            text="The state shall be governed by these statutes.\n\nSecond paragraph.",
        )
        xml = converter.to_akn_xml(section)
        assert "The state shall be governed" in xml
        assert "Second paragraph" in xml

    def test_xml_with_part_name(self, converter):
        section = MNSection(
            citation="609.75",
            chapter="609",
            title="Test",
            part_name="CRIMES; EXPUNGEMENT; VICTIMS",
            year="2025",
        )
        xml = converter.to_akn_xml(section)
        assert "CRIMES" in xml

    def test_xml_is_valid(self, converter):
        section = MNSection(
            citation="609.75",
            chapter="609",
            title="Test",
            part_name="",
            year="2025",
        )
        xml = converter.to_akn_xml(section)
        # Remove XML declaration for parsing
        xml_body = xml.split("?>", 1)[1] if "?>" in xml else xml
        root = ET.fromstring(xml_body.strip())
        assert root is not None

    def test_frbr_elements(self, converter):
        section = MNSection(
            citation="609.75",
            chapter="609",
            title="Test",
            part_name="",
            year="2025",
        )
        xml = converter.to_akn_xml(section)
        assert "FRBRWork" in xml
        assert "FRBRExpression" in xml
        assert "FRBRManifestation" in xml

    def test_lifecycle_repeal_event(self, converter):
        section = MNSection(
            citation="105.63",
            chapter="105",
            title="",
            part_name="",
            year="2025",
            is_repealed=True,
            repealed_by="1990 c 391",
        )
        xml = converter.to_akn_xml(section)
        assert "repeal" in xml

    def test_repealed_subdivision(self, converter):
        section = MNSection(
            citation="609.75",
            chapter="609",
            title="Test",
            part_name="",
            year="2025",
            subdivisions=[
                MNSubsection(
                    identifier="1",
                    headnote="Old subd",
                    paragraphs=["[Repealed, 2020 c 1]"],
                    status="repealed",
                ),
            ],
        )
        xml = converter.to_akn_xml(section)
        assert "repealed" in xml


class TestConvertHtml:
    def test_convert_html(self, converter):
        xml = converter.convert_html(SAMPLE_MN_HTML, filename="statutes_cite_609.75.html")
        assert "akomaNtoso" in xml
        assert "609.75" in xml


class TestConvertFile:
    def test_convert_file(self, converter, tmp_path):
        input_path = tmp_path / "statutes_cite_609.75.html"
        input_path.write_text(SAMPLE_MN_HTML)

        xml = converter.convert_file(input_path)
        assert "akomaNtoso" in xml

    def test_convert_file_with_output(self, converter, tmp_path):
        input_path = tmp_path / "statutes_cite_609.75.html"
        input_path.write_text(SAMPLE_MN_HTML)
        output_path = tmp_path / "output" / "mn_609.75.xml"

        converter.convert_file(input_path, output_path)
        assert output_path.exists()
        assert "akomaNtoso" in output_path.read_text()


class TestConvertDirectory:
    def test_convert_directory(self, converter, tmp_path):
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()

        # Create two test HTML files
        (input_dir / "statutes_cite_609.75.html").write_text(SAMPLE_MN_HTML)
        (input_dir / "statutes_cite_105.63.html").write_text(REPEALED_MN_HTML)
        (input_dir / "other_file.txt").write_text("ignore me")

        stats = converter.convert_directory(input_dir, output_dir)

        assert stats["total"] == 2
        assert stats["success"] == 2
        assert stats["failed"] == 0
