"""Tests for the Tennessee HTML to AKN converter module."""


from axiom_corpus.converters.tn_html_to_akn import (
    AKN_NS,
    ParsedSection,
    ParsedSubsection,
    TennesseeToAKN,
)


class TestParsedSubsection:
    def test_create(self):
        sub = ParsedSubsection(
            identifier="a", eId="sec_67-1-101__subsec_a", text="Tax imposed."
        )
        assert sub.identifier == "a"
        assert sub.eId == "sec_67-1-101__subsec_a"
        assert sub.children == []

    def test_with_children(self):
        child = ParsedSubsection(
            identifier="1",
            eId="sec_67-1-101__subsec_a__para_1",
            text="Rate is 5%.",
        )
        parent = ParsedSubsection(
            identifier="a",
            eId="sec_67-1-101__subsec_a",
            text="Rates:",
            children=[child],
        )
        assert len(parent.children) == 1


class TestParsedSection:
    def test_create(self):
        section = ParsedSection(
            section_num="67-1-101",
            heading="Liberal construction of title",
            eId="sec_67-1-101",
        )
        assert section.section_num == "67-1-101"
        assert section.text == ""
        assert section.subsections == []
        assert section.history == ""

    def test_with_content(self):
        section = ParsedSection(
            section_num="67-1-101",
            heading="Liberal construction",
            eId="sec_67-1-101",
            text="This title shall be liberally construed.",
            history="Acts 1990, ch. 1, sec. 1.",
        )
        assert section.text != ""
        assert "Acts" in section.history


class TestTennesseeToAKN:
    def test_init(self):
        converter = TennesseeToAKN()
        assert converter is not None

    def test_akn_namespace(self):
        assert "akn/3.0" in AKN_NS
