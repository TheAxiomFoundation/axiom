"""Tests for the state_to_uslm converter module."""


import pytest

from axiom.converters.state_to_uslm import (
    USLM_NS,
    ParsedSection,
    ParsedSubsection,
    StateToUSLMConverter,
)


class TestParsedSubsection:
    def test_create(self):
        sub = ParsedSubsection(identifier="A", level=0, text="Tax imposed.")
        assert sub.identifier == "A"
        assert sub.level == 0
        assert sub.children == []

    def test_with_children(self):
        child = ParsedSubsection(identifier="1", level=1, text="Rate is 5%.")
        parent = ParsedSubsection(
            identifier="A", level=0, text="Rates.", children=[child]
        )
        assert len(parent.children) == 1
        assert parent.children[0].identifier == "1"


class TestParsedSection:
    def test_create(self):
        section = ParsedSection(
            state="oh",
            code="orc",
            title_num="57",
            title_name="Taxation",
            chapter_num="5747",
            chapter_name="Income Tax",
            section_num="5747.02",
            section_title="Tax rates",
        )
        assert section.state == "oh"
        assert section.section_num == "5747.02"
        assert section.text == ""
        assert section.subsections == []

    def test_with_optional_fields(self):
        section = ParsedSection(
            state="oh",
            code="orc",
            title_num="57",
            title_name="Taxation",
            chapter_num="5747",
            chapter_name="Income Tax",
            section_num="5747.02",
            section_title="Tax rates",
            effective_date="2024-01-01",
            legislation="HB 1",
            text="Tax imposed...",
            source_url="https://codes.ohio.gov/orc/5747.02",
        )
        assert section.effective_date == "2024-01-01"
        assert section.legislation == "HB 1"


class TestStateToUSLMConverter:
    def test_convert_html_calls_parse_and_to_xml(self):
        converter = StateToUSLMConverter()
        converter.state_code = "oh"

        # Override parse_html since it's abstract
        parsed = ParsedSection(
            state="oh",
            code="orc",
            title_num="57",
            title_name="Taxation",
            chapter_num="5747",
            chapter_name="Income Tax",
            section_num="5747.02",
            section_title="Tax rates",
            text="Tax imposed at the following rates.",
        )
        converter.parse_html = lambda html, url: parsed

        xml = converter.convert_html("<html>test</html>")
        assert "lawDoc" in xml
        assert "5747.02" in xml

    def test_parse_html_not_implemented(self):
        converter = StateToUSLMConverter()
        with pytest.raises(NotImplementedError):
            converter.parse_html("<html>test</html>")

    def test_to_uslm_xml_basic(self):
        converter = StateToUSLMConverter()
        parsed = ParsedSection(
            state="oh",
            code="orc",
            title_num="57",
            title_name="Taxation",
            chapter_num="5747",
            chapter_name="Income Tax",
            section_num="5747.02",
            section_title="Tax rates",
            text="Tax imposed.",
        )
        xml = converter.to_uslm_xml(parsed)
        assert "lawDoc" in xml
        assert "5747" in xml

    def test_to_uslm_xml_with_subsections(self):
        converter = StateToUSLMConverter()
        subsections = [
            ParsedSubsection(identifier="A", level=0, text="Rate is 5%."),
            ParsedSubsection(identifier="B", level=0, text="Rate is 3%."),
        ]
        parsed = ParsedSection(
            state="oh",
            code="orc",
            title_num="57",
            title_name="Taxation",
            chapter_num="5747",
            chapter_name="Income Tax",
            section_num="5747.02",
            section_title="Tax rates",
            subsections=subsections,
        )
        xml = converter.to_uslm_xml(parsed)
        assert "lawDoc" in xml

    def test_convert_file(self, tmp_path):
        converter = StateToUSLMConverter()
        converter.state_code = "oh"

        parsed = ParsedSection(
            state="oh",
            code="orc",
            title_num="57",
            title_name="Taxation",
            chapter_num="5747",
            chapter_name="Income Tax",
            section_num="5747.02",
            section_title="Tax rates",
            text="Tax imposed.",
        )
        converter.parse_html = lambda html, url: parsed

        input_file = tmp_path / "input.html"
        input_file.write_text("<html>test</html>")
        output_file = tmp_path / "output.xml"

        converter.convert_file(input_file, output_file)
        assert output_file.exists()
        content = output_file.read_text()
        assert "lawDoc" in content

    def test_uslm_namespace(self):
        assert "house.gov" in USLM_NS
