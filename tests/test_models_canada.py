"""Tests for Canadian statute models."""

from datetime import date

from axiom_corpus.models_canada import CanadaAct, CanadaCitation, CanadaSection, CanadaSubsection


class TestCanadaCitation:
    def test_short_cite_number_only(self):
        c = CanadaCitation(consolidated_number="I-3.3")
        assert c.short_cite == "I-3.3"

    def test_short_cite_with_section(self):
        c = CanadaCitation(consolidated_number="I-3.3", section="2")
        assert c.short_cite == "I-3.3, s. 2"

    def test_short_cite_with_subsection(self):
        c = CanadaCitation(consolidated_number="I-3.3", section="2", subsection="1")
        assert c.short_cite == "I-3.3, s. 2(1)"

    def test_short_cite_with_paragraph(self):
        c = CanadaCitation(
            consolidated_number="I-3.3", section="2", subsection="1", paragraph="a"
        )
        assert c.short_cite == "I-3.3, s. 2(1)(a)"

    def test_path_number_only(self):
        c = CanadaCitation(consolidated_number="I-3.3")
        assert c.path == "canada/I-3.3"

    def test_path_with_section(self):
        c = CanadaCitation(consolidated_number="I-3.3", section="2")
        assert c.path == "canada/I-3.3/2"

    def test_path_with_subsection(self):
        c = CanadaCitation(consolidated_number="I-3.3", section="2", subsection="1")
        assert c.path == "canada/I-3.3/2/1"

    def test_path_with_paragraph(self):
        c = CanadaCitation(
            consolidated_number="I-3.3", section="2", subsection="1", paragraph="a"
        )
        assert c.path == "canada/I-3.3/2/1/a"


class TestCanadaSubsection:
    def test_basic_subsection(self):
        sub = CanadaSubsection(label="(1)", text="First subsection.")
        assert sub.label == "(1)"
        assert sub.text == "First subsection."
        assert sub.children == []
        assert sub.level == "subsection"

    def test_nested_children(self):
        child = CanadaSubsection(label="(a)", text="Paragraph a.", level="paragraph")
        parent = CanadaSubsection(label="(1)", text="Parent.", children=[child])
        assert len(parent.children) == 1
        assert parent.children[0].label == "(a)"


class TestCanadaSection:
    def test_basic_section(self):
        sec = CanadaSection(
            citation=CanadaCitation(consolidated_number="I-3.3", section="2"),
            section_number="2",
            marginal_note="Definitions",
            text="Full text of section.",
            source_url="https://laws-lois.justice.gc.ca/eng/acts/I-3.3/page-2.html",
        )
        assert sec.section_number == "2"
        assert sec.marginal_note == "Definitions"
        assert sec.subsections == []
        assert sec.historical_notes == []
        assert sec.references_to == []

    def test_section_with_dates(self):
        sec = CanadaSection(
            citation=CanadaCitation(consolidated_number="I-3.3", section="1"),
            section_number="1",
            marginal_note="Short title",
            text="Income Tax Act.",
            source_url="https://laws-lois.justice.gc.ca",
            in_force_date=date(1985, 1, 1),
            last_amended_date=date(2024, 6, 15),
        )
        assert sec.in_force_date == date(1985, 1, 1)
        assert sec.last_amended_date == date(2024, 6, 15)


class TestCanadaAct:
    def test_basic_act(self):
        act = CanadaAct(
            citation=CanadaCitation(consolidated_number="I-3.3"),
            short_title="Income Tax Act",
            long_title="An Act respecting income taxes",
            consolidated_number="I-3.3",
            source_url="https://laws-lois.justice.gc.ca/eng/acts/I-3.3/",
        )
        assert act.short_title == "Income Tax Act"
        assert act.in_force is True
        assert act.bill_origin is None

    def test_act_with_all_fields(self):
        act = CanadaAct(
            citation=CanadaCitation(consolidated_number="C-46"),
            short_title="Criminal Code",
            long_title="An Act respecting the criminal law",
            consolidated_number="C-46",
            bill_origin="commons",
            bill_type="Government",
            in_force_date=date(1985, 1, 1),
            last_amended_date=date(2024, 1, 1),
            in_force=True,
            section_count=850,
            source_url="https://laws-lois.justice.gc.ca/eng/acts/C-46/",
            source_path="/data/canada/C-46.xml",
        )
        assert act.section_count == 850
        assert act.bill_origin == "commons"
