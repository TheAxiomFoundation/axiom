"""Tests for the regulation models module."""

from datetime import date

import pytest

from axiom.models_regulation import (
    CFR_CITATION_PATTERN,
    Amendment,
    CFRCitation,
    Regulation,
    RegulationSearchResult,
    RegulationSubsection,
)


class TestCFRCitationPattern:
    def test_basic_match(self):
        match = CFR_CITATION_PATTERN.match("26 CFR 1")
        assert match is not None
        assert match.group(1) == "26"
        assert match.group(2) == "1"

    def test_with_section(self):
        match = CFR_CITATION_PATTERN.match("26 CFR 1.32")
        assert match is not None
        assert match.group(3) == "32"

    def test_with_section_dash(self):
        match = CFR_CITATION_PATTERN.match("26 CFR 1.32-1")
        assert match is not None
        assert match.group(3) == "32-1"

    def test_with_subsection(self):
        match = CFR_CITATION_PATTERN.match("26 CFR 1.32-1(a)(1)")
        assert match is not None
        assert match.group(4) == "(a)(1)"

    def test_dotted_cfr(self):
        match = CFR_CITATION_PATTERN.match("26 C.F.R. 1.32")
        assert match is not None

    def test_with_section_symbol(self):
        match = CFR_CITATION_PATTERN.match("26 CFR \u00a7 1.32")
        assert match is not None


class TestCFRCitation:
    def test_create(self):
        cite = CFRCitation(title=26, part=1)
        assert cite.title == 26
        assert cite.part == 1
        assert cite.section is None
        assert cite.subsection is None

    def test_create_full(self):
        cite = CFRCitation(title=26, part=1, section="32-1", subsection="a/1")
        assert cite.section == "32-1"
        assert cite.subsection == "a/1"

    def test_from_string_basic(self):
        cite = CFRCitation.from_string("26 CFR 1")
        assert cite.title == 26
        assert cite.part == 1

    def test_from_string_with_section(self):
        cite = CFRCitation.from_string("26 CFR 1.32-1")
        assert cite.title == 26
        assert cite.part == 1
        assert cite.section == "32-1"

    def test_from_string_with_subsection(self):
        cite = CFRCitation.from_string("26 CFR 1.32-1(a)(1)")
        assert cite.subsection == "a/1"

    def test_from_string_invalid(self):
        with pytest.raises(ValueError, match="Invalid CFR citation"):
            CFRCitation.from_string("not a citation")

    def test_cfr_cite_basic(self):
        cite = CFRCitation(title=26, part=1)
        assert cite.cfr_cite == "26 CFR 1"

    def test_cfr_cite_with_section(self):
        cite = CFRCitation(title=26, part=1, section="32-1")
        assert cite.cfr_cite == "26 CFR 1.32-1"

    def test_cfr_cite_with_subsection(self):
        cite = CFRCitation(title=26, part=1, section="32-1", subsection="a/1")
        assert cite.cfr_cite == "26 CFR 1.32-1(a)(1)"

    def test_path_basic(self):
        cite = CFRCitation(title=26, part=1)
        assert cite.path == "regulation/26/1"

    def test_path_with_section(self):
        cite = CFRCitation(title=26, part=1, section="32-1")
        assert cite.path == "regulation/26/1/32-1"

    def test_path_with_subsection(self):
        cite = CFRCitation(title=26, part=1, section="32-1", subsection="a/1")
        assert cite.path == "regulation/26/1/32-1/a/1"


class TestRegulationSubsection:
    def test_create(self):
        sub = RegulationSubsection(id="a", text="General rule.")
        assert sub.id == "a"
        assert sub.heading is None
        assert sub.children == []

    def test_with_heading(self):
        sub = RegulationSubsection(id="a", heading="In general", text="Rule.")
        assert sub.heading == "In general"

    def test_with_children(self):
        child = RegulationSubsection(id="1", text="Rate.")
        parent = RegulationSubsection(id="a", text="Rule.", children=[child])
        assert len(parent.children) == 1


class TestAmendment:
    def test_create(self):
        amendment = Amendment(
            document="T.D. 9954",
            federal_register_citation="86 FR 12345",
            published_date=date(2024, 1, 15),
            effective_date=date(2024, 3, 1),
        )
        assert amendment.document == "T.D. 9954"
        assert amendment.description is None

    def test_with_description(self):
        amendment = Amendment(
            document="T.D. 9954",
            federal_register_citation="86 FR 12345",
            published_date=date(2024, 1, 15),
            effective_date=date(2024, 3, 1),
            description="Updated earned income thresholds",
        )
        assert "thresholds" in amendment.description


class TestRegulation:
    def _make_regulation(self, **kwargs):
        defaults = {
            "citation": CFRCitation(title=26, part=1, section="32-1"),
            "heading": "Earned income credit",
            "authority": "26 U.S.C. 32",
            "source": "T.D. 9954, 86 FR 12345",
            "full_text": "The earned income credit...",
            "effective_date": date(2024, 1, 1),
        }
        defaults.update(kwargs)
        return Regulation(**defaults)

    def test_create(self):
        reg = self._make_regulation()
        assert reg.heading == "Earned income credit"
        assert reg.subsections == []
        assert reg.amendments == []

    def test_path_property(self):
        reg = self._make_regulation()
        assert reg.path == "regulation/26/1/32-1"

    def test_cfr_cite_property(self):
        reg = self._make_regulation()
        assert reg.cfr_cite == "26 CFR 1.32-1"

    def test_with_subsections(self):
        subs = [
            RegulationSubsection(id="a", text="General rule."),
        ]
        reg = self._make_regulation(subsections=subs)
        assert len(reg.subsections) == 1

    def test_with_amendments(self):
        amendments = [
            Amendment(
                document="T.D. 9954",
                federal_register_citation="86 FR 12345",
                published_date=date(2024, 1, 15),
                effective_date=date(2024, 3, 1),
            ),
        ]
        reg = self._make_regulation(amendments=amendments)
        assert len(reg.amendments) == 1

    def test_with_cross_references(self):
        reg = self._make_regulation(
            source_statutes=["26 USC 32"],
            cross_references=["26 CFR 1.24-1"],
        )
        assert reg.source_statutes == ["26 USC 32"]
        assert reg.cross_references == ["26 CFR 1.24-1"]


class TestRegulationSearchResult:
    def test_create(self):
        result = RegulationSearchResult(
            cfr_cite="26 CFR 1.32-1",
            heading="Earned income credit",
            snippet="The credit is allowed...",
            score=0.85,
            effective_date=date(2024, 1, 1),
        )
        assert result.score == 0.85
