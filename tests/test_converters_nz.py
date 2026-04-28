"""Tests for the New Zealand PCO converter module."""

from datetime import date

from axiom_corpus.converters.nz_pco import (
    NZLabeledParagraph,
    NZLegislation,
    NZProvision,
)


class TestNZProvision:
    def test_create(self):
        prov = NZProvision(
            id="DLM407936",
            label="1",
            heading="Short Title",
        )
        assert prov.id == "DLM407936"
        assert prov.label == "1"
        assert prov.text == ""
        assert prov.subprovisions == []
        assert prov.paragraphs == []

    def test_with_content(self):
        prov = NZProvision(
            id="DLM407936",
            label="1",
            heading="Short Title",
            text="This Act may be cited as the Tax Act.",
        )
        assert "Tax Act" in prov.text


class TestNZLabeledParagraph:
    def test_create(self):
        para = NZLabeledParagraph(label="a", text="Income includes...")
        assert para.label == "a"
        assert para.children == []

    def test_with_children(self):
        child = NZLabeledParagraph(label="i", text="Salary")
        parent = NZLabeledParagraph(
            label="a", text="Income:", children=[child]
        )
        assert len(parent.children) == 1


class TestNZLegislation:
    def _make_legislation(self, **kwargs):
        defaults = {
            "id": "DLM407930",
            "legislation_type": "act",
            "subtype": "public",
            "year": 2007,
            "number": 97,
            "title": "Income Tax Act",
        }
        defaults.update(kwargs)
        return NZLegislation(**defaults)

    def test_create(self):
        leg = self._make_legislation()
        assert leg.title == "Income Tax Act"
        assert leg.year == 2007
        assert leg.number == 97

    def test_citation(self):
        leg = self._make_legislation()
        assert "2007" in leg.citation
        assert "97" in leg.citation

    def test_citation_sop(self):
        leg = self._make_legislation(
            legislation_type="sop",
            title="SOP 1",
        )
        # SOP uses .title() on the type string, giving "Sop"
        assert "2007" in leg.citation
        assert "97" in leg.citation

    def test_url(self):
        leg = self._make_legislation()
        url = leg.url
        assert "legislation.govt.nz" in url
        assert "act" in url
        assert "public" in url
        assert "2007" in url

    def test_optional_fields(self):
        leg = self._make_legislation(
            short_title="ITA 2007",
            assent_date=date(2007, 11, 1),
            commencement_date=date(2008, 4, 1),
            stage="in-force",
            administering_ministry="Inland Revenue",
            version_date=date(2024, 1, 1),
        )
        assert leg.short_title == "ITA 2007"
        assert leg.assent_date == date(2007, 11, 1)

    def test_with_provisions(self):
        provs = [
            NZProvision(id="DLM1", label="1", heading="Short Title"),
            NZProvision(id="DLM2", label="2", heading="Interpretation"),
        ]
        leg = self._make_legislation(provisions=provs)
        assert len(leg.provisions) == 2
