"""Tests for UK legislation data models."""

from datetime import date

import pytest


class TestUKCitation:
    """Tests for UK legislation citation parsing."""

    def test_parse_primary_act(self):
        """Parse UK Public General Act citation."""
        from axiom.models_uk import UKCitation

        cite = UKCitation.from_string("ukpga/2003/1")
        assert cite.type == "ukpga"
        assert cite.year == 2003
        assert cite.number == 1
        assert cite.section is None

    def test_parse_act_with_section(self):
        """Parse citation with section number."""
        from axiom.models_uk import UKCitation

        cite = UKCitation.from_string("ukpga/2007/3/section/1")
        assert cite.type == "ukpga"
        assert cite.year == 2007
        assert cite.number == 3
        assert cite.section == "1"

    def test_parse_statutory_instrument(self):
        """Parse UK Statutory Instrument citation."""
        from axiom.models_uk import UKCitation

        cite = UKCitation.from_string("uksi/2024/832")
        assert cite.type == "uksi"
        assert cite.year == 2024
        assert cite.number == 832

    def test_parse_scottish_act(self):
        """Parse Acts of the Scottish Parliament."""
        from axiom.models_uk import UKCitation

        cite = UKCitation.from_string("asp/2020/13")
        assert cite.type == "asp"
        assert cite.year == 2020
        assert cite.number == 13

    def test_parse_welsh_act(self):
        """Parse Acts of Senedd Cymru (Welsh Parliament)."""
        from axiom.models_uk import UKCitation

        cite = UKCitation.from_string("asc/2021/4")
        assert cite.type == "asc"
        assert cite.year == 2021
        assert cite.number == 4

    def test_parse_ni_act(self):
        """Parse Acts of the Northern Ireland Assembly."""
        from axiom.models_uk import UKCitation

        cite = UKCitation.from_string("nia/2022/7")
        assert cite.type == "nia"
        assert cite.year == 2022
        assert cite.number == 7

    def test_parse_human_readable_citation(self):
        """Parse human-readable citation format."""
        from axiom.models_uk import UKCitation

        # Common short forms
        cite = UKCitation.from_string("ITEPA 2003 s.1")
        assert cite.year == 2003
        assert cite.section == "1"

    def test_parse_section_with_subsection(self):
        """Parse citation with subsection."""
        from axiom.models_uk import UKCitation

        cite = UKCitation.from_string("ukpga/2003/1/section/1/1")
        assert cite.section == "1"
        assert cite.subsection == "1"

    def test_legislation_url(self):
        """Generate legislation.gov.uk URL."""
        from axiom.models_uk import UKCitation

        cite = UKCitation(type="ukpga", year=2003, number=1)
        assert cite.legislation_url == "https://www.legislation.gov.uk/ukpga/2003/1"

    def test_legislation_url_with_section(self):
        """Generate URL for specific section."""
        from axiom.models_uk import UKCitation

        cite = UKCitation(type="ukpga", year=2003, number=1, section="1")
        assert cite.legislation_url == "https://www.legislation.gov.uk/ukpga/2003/1/section/1"

    def test_data_xml_url(self):
        """Generate XML data URL."""
        from axiom.models_uk import UKCitation

        cite = UKCitation(type="ukpga", year=2007, number=3, section="23")
        assert cite.data_xml_url == "https://www.legislation.gov.uk/ukpga/2007/3/section/23/data.xml"

    def test_short_cite(self):
        """Generate short citation format."""
        from axiom.models_uk import UKCitation

        cite = UKCitation(type="ukpga", year=2003, number=1, section="62")
        # e.g., "2003 c. 1 s. 62"
        assert "2003" in cite.short_cite
        assert "62" in cite.short_cite

    def test_path(self):
        """Generate filesystem path."""
        from axiom.models_uk import UKCitation

        cite = UKCitation(type="ukpga", year=2003, number=1, section="62", subsection="1/a")
        assert cite.path == "uk/ukpga/2003/1/62/1/a"

    def test_invalid_citation_raises(self):
        """Invalid citation raises ValueError."""
        from axiom.models_uk import UKCitation

        with pytest.raises(ValueError):
            UKCitation.from_string("not a citation")

    def test_type_names(self):
        """Legislation type names are correct."""
        from axiom.models_uk import UK_LEGISLATION_TYPES

        assert UK_LEGISLATION_TYPES["ukpga"] == "UK Public General Act"
        assert UK_LEGISLATION_TYPES["uksi"] == "UK Statutory Instrument"
        assert UK_LEGISLATION_TYPES["asp"] == "Act of Scottish Parliament"


class TestUKSection:
    """Tests for UK legislation section model."""

    def test_create_section(self):
        """Create a basic UK section."""
        from axiom.models_uk import UKCitation, UKSection

        section = UKSection(
            citation=UKCitation(type="ukpga", year=2003, number=1, section="1"),
            title="Overview of Parts 2 to 7",
            text="This Act imposes charges to income tax on employment income...",
            enacted_date=date(2003, 4, 10),
        )
        assert section.citation.year == 2003
        assert section.title == "Overview of Parts 2 to 7"

    def test_section_with_extent(self):
        """Section includes territorial extent."""
        from axiom.models_uk import UKCitation, UKSection

        section = UKSection(
            citation=UKCitation(type="ukpga", year=2003, number=1, section="1"),
            title="Test",
            text="...",
            enacted_date=date(2003, 4, 10),
            extent=["E", "W", "S", "N.I."],  # England, Wales, Scotland, NI
        )
        assert "E" in section.extent
        assert len(section.extent) == 4

    def test_section_with_amendments(self):
        """Section tracks amendment history."""
        from axiom.models_uk import UKAmendment, UKCitation, UKSection

        amendment = UKAmendment(
            type="substitution",
            amending_act="ukpga/2017/32",
            description="Words substituted",
            effective_date=date(2017, 11, 16),
        )
        section = UKSection(
            citation=UKCitation(type="ukpga", year=2003, number=1, section="1"),
            title="Test",
            text="...",
            enacted_date=date(2003, 4, 10),
            amendments=[amendment],
        )
        assert len(section.amendments) == 1
        assert section.amendments[0].type == "substitution"

    def test_section_with_subsections(self):
        """Section contains subsections."""
        from axiom.models_uk import UKCitation, UKSection, UKSubsection

        subsec = UKSubsection(
            id="1",
            text="(1) This Act imposes charges to income tax on—",
        )
        section = UKSection(
            citation=UKCitation(type="ukpga", year=2003, number=1, section="1"),
            title="Overview",
            text="...",
            enacted_date=date(2003, 4, 10),
            subsections=[subsec],
        )
        assert len(section.subsections) == 1

    def test_section_path(self):
        """Section has path property."""
        from axiom.models_uk import UKCitation, UKSection

        section = UKSection(
            citation=UKCitation(type="ukpga", year=2003, number=1, section="62"),
            title="Earnings",
            text="...",
            enacted_date=date(2003, 4, 10),
        )
        assert section.path == "uk/ukpga/2003/1/62"


class TestUKAct:
    """Tests for complete UK Act model."""

    def test_create_act(self):
        """Create a UK Act with metadata."""
        from axiom.models_uk import UKAct, UKCitation

        act = UKAct(
            citation=UKCitation(type="ukpga", year=2003, number=1),
            title="Income Tax (Earnings and Pensions) Act 2003",
            short_title="ITEPA 2003",
            enacted_date=date(2003, 4, 10),
            commencement_date=date(2003, 4, 6),
            extent=["E", "W", "S", "N.I."],
            section_count=725,
        )
        assert act.title == "Income Tax (Earnings and Pensions) Act 2003"
        assert act.short_title == "ITEPA 2003"

    def test_act_with_parts(self):
        """Act can have parts structure."""
        from axiom.models_uk import UKAct, UKCitation, UKPart

        part = UKPart(
            number="1",
            title="Overview",
            section_range="1-12",
        )
        act = UKAct(
            citation=UKCitation(type="ukpga", year=2003, number=1),
            title="Income Tax (Earnings and Pensions) Act 2003",
            enacted_date=date(2003, 4, 10),
            parts=[part],
        )
        assert len(act.parts) == 1
        assert act.parts[0].title == "Overview"


class TestUKSubsection:
    """Tests for UK subsection model."""

    def test_nested_subsections(self):
        """Subsections can be nested."""
        from axiom.models_uk import UKSubsection

        para_a = UKSubsection(id="a", text="(a) employment income (see Part 2),")
        para_b = UKSubsection(id="b", text="(b) pension income (see Part 9),")

        subsec = UKSubsection(
            id="1",
            text="(1) This Act imposes charges to income tax on—",
            children=[para_a, para_b],
        )
        assert len(subsec.children) == 2
        assert subsec.children[0].id == "a"


class TestUKSearchResult:
    """Tests for UK legislation search results."""

    def test_create_search_result(self):
        """Create a search result."""
        from axiom.models_uk import UKSearchResult

        result = UKSearchResult(
            citation="ukpga/2003/1/section/62",
            title="Earnings",
            snippet="...the <em>earnings</em> from an employment...",
            score=0.95,
            act_title="Income Tax (Earnings and Pensions) Act 2003",
        )
        assert result.score == 0.95
        assert "earnings" in result.snippet.lower()
