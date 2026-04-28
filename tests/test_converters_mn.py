"""Tests for the Minnesota statutes converter module."""


from axiom_corpus.converters.mn_statutes import (
    AKN_NS,
    MNSection,
    MNStatutesToAKN,
    MNSubsection,
)


class TestMNSubsection:
    def test_create(self):
        sub = MNSubsection(identifier="1", headnote="Definitions")
        assert sub.identifier == "1"
        assert sub.headnote == "Definitions"
        assert sub.paragraphs == []
        assert sub.status == ""

    def test_with_paragraphs(self):
        sub = MNSubsection(
            identifier="1",
            headnote="Definitions",
            paragraphs=["For purposes of this section:", "Term means..."],
        )
        assert len(sub.paragraphs) == 2

    def test_with_status(self):
        sub = MNSubsection(
            identifier="2", headnote="Repealed", status="Repealed"
        )
        assert sub.status == "Repealed"


class TestMNSection:
    def test_create(self):
        section = MNSection(
            citation="609.75",
            chapter="609",
            title="GAMBLING; DEFINITIONS.",
            part_name="CRIMES; EXPUNGEMENT; VICTIMS",
            year="2025",
        )
        assert section.citation == "609.75"
        assert section.chapter == "609"
        assert section.is_repealed is False

    def test_repealed_section(self):
        section = MNSection(
            citation="105.63",
            chapter="105",
            title="Old section",
            part_name="PART",
            year="2025",
            is_repealed=True,
            repealed_by="1990 c 391 art 10 s 4",
        )
        assert section.is_repealed is True
        assert "1990" in section.repealed_by

    def test_with_subdivisions(self):
        subs = [
            MNSubsection(identifier="1", headnote="Definitions"),
            MNSubsection(identifier="2", headnote="Application"),
        ]
        section = MNSection(
            citation="609.75",
            chapter="609",
            title="GAMBLING; DEFINITIONS.",
            part_name="CRIMES",
            year="2025",
            subdivisions=subs,
        )
        assert len(section.subdivisions) == 2


class TestMNStatutesToAKN:
    def test_init(self):
        converter = MNStatutesToAKN()
        assert converter.country == "us-mn"

    def test_akn_namespace(self):
        assert "akn/3.0" in AKN_NS
