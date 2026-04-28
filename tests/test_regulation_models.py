"""Tests for regulation data models."""

from datetime import date

import pytest


class TestCFRCitation:
    """Tests for CFR citation parsing."""

    def test_parse_simple_citation(self):
        """Parse simple CFR citation like '26 CFR 1.32'."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation.from_string("26 CFR 1.32")
        assert cite.title == 26
        assert cite.part == 1
        assert cite.section == "32"
        assert cite.subsection is None

    def test_parse_citation_with_hyphen(self):
        """Parse CFR citation with hyphenated section like '26 CFR 1.32-1'."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation.from_string("26 CFR 1.32-1")
        assert cite.title == 26
        assert cite.part == 1
        assert cite.section == "32-1"

    def test_parse_citation_with_subsection(self):
        """Parse CFR citation with subsection like '26 CFR 1.32-1(a)'."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation.from_string("26 CFR 1.32-1(a)")
        assert cite.title == 26
        assert cite.part == 1
        assert cite.section == "32-1"
        assert cite.subsection == "a"

    def test_parse_citation_with_nested_subsections(self):
        """Parse CFR citation with nested subsections like '26 CFR 1.32-1(a)(1)(i)'."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation.from_string("26 CFR 1.32-1(a)(1)(i)")
        assert cite.title == 26
        assert cite.section == "32-1"
        assert cite.subsection == "a/1/i"

    def test_parse_citation_with_periods(self):
        """Parse CFR citation with C.F.R. format."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation.from_string("26 C.F.R. 1.32-1")
        assert cite.title == 26
        assert cite.part == 1
        assert cite.section == "32-1"

    def test_parse_citation_with_section_symbol(self):
        """Parse CFR citation with section symbol."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation.from_string("26 CFR § 1.32-1")
        assert cite.title == 26
        assert cite.section == "32-1"

    def test_cfr_cite_format(self):
        """Test CFR citation string output."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation(title=26, part=1, section="32-1", subsection="a/1")
        assert cite.cfr_cite == "26 CFR 1.32-1(a)(1)"

    def test_path_format(self):
        """Test filesystem path format."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation(title=26, part=1, section="32-1", subsection="a/1")
        assert cite.path == "regulation/26/1/32-1/a/1"

    def test_path_format_no_subsection(self):
        """Test path format without subsection."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation(title=26, part=1, section="32-1")
        assert cite.path == "regulation/26/1/32-1"

    def test_invalid_citation_raises(self):
        """Invalid CFR citation string raises ValueError."""
        from axiom.models_regulation import CFRCitation

        with pytest.raises(ValueError):
            CFRCitation.from_string("not a citation")

    def test_part_only_citation(self):
        """Parse CFR citation with just part number like '26 CFR 1'."""
        from axiom.models_regulation import CFRCitation

        cite = CFRCitation.from_string("26 CFR 1")
        assert cite.title == 26
        assert cite.part == 1
        assert cite.section is None


class TestRegulationSubsection:
    """Tests for regulation subsection model."""

    def test_create_subsection(self):
        """Create a regulation subsection."""
        from axiom.models_regulation import RegulationSubsection

        subsec = RegulationSubsection(
            id="a",
            heading="General rule",
            text="The earned income credit is allowed...",
        )
        assert subsec.id == "a"
        assert subsec.heading == "General rule"
        assert "earned income" in subsec.text

    def test_nested_subsections(self):
        """Subsections can have children."""
        from axiom.models_regulation import RegulationSubsection

        child = RegulationSubsection(id="1", text="First sub-requirement")
        parent = RegulationSubsection(
            id="a",
            heading="Requirements",
            text="The following requirements apply:",
            children=[child],
        )
        assert len(parent.children) == 1
        assert parent.children[0].id == "1"


class TestRegulation:
    """Tests for the main Regulation model."""

    def test_create_regulation(self):
        """Create a basic regulation."""
        from axiom.models_regulation import CFRCitation, Regulation

        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Earned income",
            authority="26 U.S.C. 32",
            source="T.D. 9954, 86 FR 12345",
            full_text="(a) In general. For purposes of section 32...",
            effective_date=date(2021, 1, 1),
        )
        assert reg.citation.title == 26
        assert reg.heading == "Earned income"
        assert reg.authority == "26 U.S.C. 32"

    def test_regulation_with_source_statutes(self):
        """Regulation can reference source statutes."""
        from axiom.models_regulation import CFRCitation, Regulation

        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Earned income",
            authority="26 U.S.C. 32",
            source="T.D. 9954",
            full_text="...",
            effective_date=date(2021, 1, 1),
            source_statutes=["26 USC 32", "26 USC 32(a)"],
        )
        assert "26 USC 32" in reg.source_statutes
        assert len(reg.source_statutes) == 2

    def test_regulation_with_subsections(self):
        """Regulation can have structured subsections."""
        from axiom.models_regulation import CFRCitation, Regulation, RegulationSubsection

        subsec = RegulationSubsection(id="a", text="General rule text")
        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Earned income",
            authority="26 U.S.C. 32",
            source="T.D. 9954",
            full_text="...",
            effective_date=date(2021, 1, 1),
            subsections=[subsec],
        )
        assert len(reg.subsections) == 1
        assert reg.subsections[0].id == "a"

    def test_regulation_path(self):
        """Regulation has path property."""
        from axiom.models_regulation import CFRCitation, Regulation

        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Earned income",
            authority="26 U.S.C. 32",
            source="T.D. 9954",
            full_text="...",
            effective_date=date(2021, 1, 1),
        )
        assert reg.path == "regulation/26/1/32-1"

    def test_regulation_cfr_cite(self):
        """Regulation has cfr_cite property."""
        from axiom.models_regulation import CFRCitation, Regulation

        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Earned income",
            authority="26 U.S.C. 32",
            source="T.D. 9954",
            full_text="...",
            effective_date=date(2021, 1, 1),
        )
        assert reg.cfr_cite == "26 CFR 1.32-1"


class TestRegulationSearchResult:
    """Tests for regulation search results."""

    def test_create_search_result(self):
        """Create a regulation search result."""
        from axiom.models_regulation import RegulationSearchResult

        result = RegulationSearchResult(
            cfr_cite="26 CFR 1.32-1",
            heading="Earned income",
            snippet="...the <em>earned income</em> credit is allowed...",
            score=0.95,
            effective_date=date(2021, 1, 1),
        )
        assert result.cfr_cite == "26 CFR 1.32-1"
        assert result.score == 0.95


class TestAmendment:
    """Tests for regulation amendment tracking."""

    def test_create_amendment(self):
        """Create an amendment record."""
        from axiom.models_regulation import Amendment

        amendment = Amendment(
            document="T.D. 9954",
            federal_register_citation="86 FR 12345",
            published_date=date(2021, 3, 15),
            effective_date=date(2021, 4, 1),
            description="Updated definition of earned income",
        )
        assert amendment.document == "T.D. 9954"
        assert "86 FR" in amendment.federal_register_citation
