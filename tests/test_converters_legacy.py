"""Tests for the legacy converters module (converters.py at axiom package root).

Tests cover conversion functions from state-specific models to unified Statute model.
Note: converters.py at axiom root is shadowed by converters/ package, so we load it
directly from the file path using importlib.
"""

import importlib.util
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from axiom.models import Citation, Section, Subsection

# Load the shadowed converters.py module directly
_spec = importlib.util.spec_from_file_location(
    "axiom_converters_legacy",
    Path(__file__).parent.parent / "src" / "axiom" / "converters.py",
)
if _spec and _spec.loader:
    _mod = importlib.util.module_from_spec(_spec)
    try:
        _spec.loader.exec_module(_mod)
        from_ca_section = _mod.from_ca_section
        from_fl_section = _mod.from_fl_section
        from_tx_section = _mod.from_tx_section
        from_ny_section = _mod.from_ny_section
        from_generic_state_section = _mod.from_generic_state_section
        from_usc_section = _mod.from_usc_section
        _IMPORT_OK = True
    except Exception:
        _IMPORT_OK = False
else:
    _IMPORT_OK = False

pytestmark = pytest.mark.skipif(not _IMPORT_OK, reason="Legacy converters.py not loadable")


def _mock_subsection(identifier="a", text="Sub text", children=None):
    sub = SimpleNamespace(identifier=identifier, text=text)
    if children:
        sub.children = children
    return sub


class TestFromCASection:
    def test_basic_conversion(self):
        ca_section = SimpleNamespace(
            code="RTC",
            code_name="Revenue and Taxation Code",
            section_num="17041",
            title="Tax rates",
            text="Tax imposed at rates...",
            subsections=[_mock_subsection("a", "Rate for individuals")],
            division="Part 10",
            part="1",
            chapter="1",
            article="1",
            history="Added by Stats. 2024, ch. 1",
            url="https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml",
        )

        result = from_ca_section(ca_section)
        assert result.jurisdiction == "us-ca"
        assert result.code == "RTC"
        assert result.section == "17041"
        assert len(result.subsections) == 1

    def test_with_nested_children(self):
        child = _mock_subsection("1", "Child text")
        ca_section = SimpleNamespace(
            code="RTC",
            code_name="Revenue and Taxation Code",
            section_num="17041",
            title="Tax rates",
            text="Tax imposed...",
            subsections=[_mock_subsection("a", "Rate", children=[child])],
            division=None,
            part=None,
            chapter=None,
            article=None,
            history=None,
            url="https://example.com",
        )

        result = from_ca_section(ca_section)
        assert len(result.subsections[0].children) == 1


class TestFromFLSection:
    def test_basic_conversion(self):
        fl_section = SimpleNamespace(
            chapter=220,
            chapter_title="Income on Corporations",
            number="220.02",
            title="Definitions",
            text="Definitions for this chapter...",
            subsections=[_mock_subsection("1", "Corporation defined")],
            url="https://leg.state.fl.us/statutes/220.02",
        )

        result = from_fl_section(fl_section)
        assert result.jurisdiction == "us-fl"
        assert result.code == "220"
        assert result.section == "220.02"
        assert result.chapter == "220"


class TestFromTXSection:
    def test_basic_conversion(self):
        tx_section = SimpleNamespace(
            code="TX",
            code_name="Tax Code",
            section="171.001",
            title="Tax imposed",
            text="A franchise tax is imposed...",
            subsections=[_mock_subsection("a", "Rate")],
            url="https://statutes.capitol.texas.gov",
        )

        result = from_tx_section(tx_section)
        assert result.jurisdiction == "us-tx"
        assert result.code == "TX"
        assert result.section == "171.001"

    def test_no_subsections(self):
        tx_section = SimpleNamespace(
            code="TX",
            code_name="Tax Code",
            section="171.001",
            title="Tax imposed",
            text="A franchise tax...",
            url="https://example.com",
        )

        result = from_tx_section(tx_section)
        assert result.subsections == []


class TestFromNYSection:
    def test_basic_conversion(self):
        ny_section = SimpleNamespace(
            law_code="TAX",
            law_name="Tax Law",
            section="601",
            title="Imposition of tax",
            text="A tax is imposed...",
            subsections=[_mock_subsection("a", "Tax rate")],
            url="https://legislation.nysenate.gov",
        )

        result = from_ny_section(ny_section)
        assert result.jurisdiction == "us-ny"
        assert result.code == "TAX"
        assert result.section == "601"

    def test_no_subsections(self):
        ny_section = SimpleNamespace(
            law_code="TAX",
            law_name="Tax Law",
            section="601",
            title="Imposition of tax",
            text="A tax is imposed...",
            url="https://example.com",
        )

        result = from_ny_section(ny_section)
        assert result.subsections == []


class TestFromGenericStateSection:
    def test_known_state(self):
        state_section = SimpleNamespace(
            state="OH",
            code="ORC",
            code_name="Ohio Revised Code",
            section_num="5747.02",
            title="Tax rates",
            text="Tax imposed...",
            chapter="5747",
            subsections=[_mock_subsection("A", "Rate")],
            history="Amended 2024",
            url="https://codes.ohio.gov/orc/5747.02",
        )

        result = from_generic_state_section(state_section)
        assert result.jurisdiction == "us-oh"

    def test_unknown_state(self):
        state_section = SimpleNamespace(
            state="ZZ",
            code="ZZC",
            code_name="ZZ Code",
            section_num="1.01",
            title="Test",
            text="Test text",
            chapter="1",
            subsections=[],
            history="",
            url="https://example.com",
        )

        result = from_generic_state_section(state_section)
        assert result.jurisdiction == "us-zz"


class TestFromUSCSection:
    def test_basic_conversion(self):
        section = Section(
            citation=Citation(title=26, section="32"),
            title_name="Internal Revenue Code",
            section_title="Earned income tax credit",
            text="A tax credit is allowed...",
            subsections=[
                Subsection(identifier="a", text="Allowance of credit"),
            ],
            source_url="https://uscode.house.gov",
            retrieved_at=date(2024, 1, 1),
        )

        result = from_usc_section(section)
        assert result.jurisdiction == "us"
        assert result.code == "26"
        assert result.section == "32"
        assert result.title == "Earned income tax credit"
        assert len(result.subsections) == 1

    def test_with_subsection_children(self):
        section = Section(
            citation=Citation(title=26, section="32"),
            title_name="Internal Revenue Code",
            section_title="EITC",
            text="Tax credit...",
            subsections=[
                Subsection(
                    identifier="a",
                    text="General rule",
                    children=[
                        Subsection(identifier="1", text="Credit amount"),
                    ],
                ),
            ],
            source_url="https://uscode.house.gov",
            retrieved_at=date(2024, 1, 1),
        )

        result = from_usc_section(section)
        assert len(result.subsections[0].children) == 1

    def test_with_cross_references(self):
        section = Section(
            citation=Citation(title=26, section="32"),
            title_name="IRC",
            section_title="EITC",
            text="See also section 24.",
            source_url="https://uscode.house.gov",
            retrieved_at=date(2024, 1, 1),
            references_to=["26 USC 24"],
            referenced_by=["26 USC 1"],
        )

        result = from_usc_section(section)
        assert "26 USC 24" in result.references_to
        assert "26 USC 1" in result.referenced_by
