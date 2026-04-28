"""Tests for the converters base module.

Tests cover the AKN models, converter registry, and base converter class.
"""

from datetime import date

import pytest

from axiom.converters.base import (
    CONVERTERS,
    AknSection,
    AknSubsection,
    AkomaNtoso,
    LegalDocConverter,
    _deterministic_id,
    get_converter,
    register_converter,
)


class TestAknSubsection:
    def test_create(self):
        sub = AknSubsection(
            id="sec_32__subsec_a",
            identifier="a",
            text="Allowance of credit.",
        )
        assert sub.id == "sec_32__subsec_a"
        assert sub.identifier == "a"
        assert sub.heading is None
        assert sub.children == []

    def test_with_heading(self):
        sub = AknSubsection(
            id="sec_32__subsec_a",
            identifier="a",
            text="Credit allowed.",
            heading="In general",
        )
        assert sub.heading == "In general"

    def test_with_children(self):
        child = AknSubsection(
            id="sec_32__subsec_a__para_1",
            identifier="1",
            text="Credit percentage.",
        )
        parent = AknSubsection(
            id="sec_32__subsec_a",
            identifier="a",
            text="Credit allowed.",
            children=[child],
        )
        assert len(parent.children) == 1

    def test_extra_forbidden(self):
        with pytest.raises(Exception):
            AknSubsection(
                id="test",
                identifier="a",
                text="Test",
                invalid="field",
            )


class TestAknSection:
    def test_create(self):
        section = AknSection(
            id="sec_32",
            jurisdiction="us",
            doc_type="statute",
            title="Earned income tax credit",
            text="Credit is allowed.",
            source_url="https://uscode.house.gov",
        )
        assert section.id == "sec_32"
        assert section.jurisdiction == "us"

    def test_with_subsections(self):
        subs = [
            AknSubsection(id="sub_a", identifier="a", text="General rule."),
        ]
        section = AknSection(
            id="sec_32",
            jurisdiction="us",
            doc_type="statute",
            title="EITC",
            text="Credit allowed.",
            source_url="https://example.com",
            subsections=subs,
        )
        assert len(section.subsections) == 1

    def test_optional_hierarchy(self):
        section = AknSection(
            id="sec_32",
            jurisdiction="us",
            doc_type="statute",
            title="EITC",
            text="Credit.",
            source_url="https://example.com",
            division="A",
            part="1",
            chapter="1",
            subchapter="A",
            article="1",
        )
        assert section.division == "A"
        assert section.chapter == "1"


class TestAkomaNtoso:
    def test_create(self):
        doc = AkomaNtoso(
            uri="/us/statute/26/32",
            jurisdiction="us",
            doc_type="statute",
            source_format="uslm",
            source_url="https://uscode.house.gov",
        )
        assert doc.uri == "/us/statute/26/32"
        assert doc.sections == []

    def test_with_sections(self):
        sections = [
            AknSection(
                id="sec_32",
                jurisdiction="us",
                doc_type="statute",
                title="EITC",
                text="Credit.",
                source_url="https://example.com",
            ),
        ]
        doc = AkomaNtoso(
            uri="/us/statute/26/32",
            jurisdiction="us",
            doc_type="statute",
            source_format="uslm",
            source_url="https://uscode.house.gov",
            sections=sections,
        )
        assert len(doc.sections) == 1

    def test_metadata(self):
        doc = AkomaNtoso(
            uri="/us/statute/26/32",
            jurisdiction="us",
            doc_type="statute",
            source_format="uslm",
            source_url="https://uscode.house.gov",
            title="Title 26 - IRC",
            published_date=date(2024, 1, 1),
        )
        assert doc.title == "Title 26 - IRC"
        assert doc.published_date == date(2024, 1, 1)


class TestDeterministicId:
    def test_same_input_same_output(self):
        id1 = _deterministic_id("us/statute/26/32")
        id2 = _deterministic_id("us/statute/26/32")
        assert id1 == id2

    def test_different_input_different_output(self):
        id1 = _deterministic_id("us/statute/26/32")
        id2 = _deterministic_id("us/statute/26/24")
        assert id1 != id2

    def test_is_valid_uuid(self):
        import uuid
        result = _deterministic_id("test/path")
        uuid.UUID(result)  # Should not raise


class TestConverterRegistry:
    def test_register_converter(self):
        # Save existing converters
        saved = dict(CONVERTERS)

        class TestConverter(LegalDocConverter):
            jurisdiction = "test-reg"
            source_format = "test"
            doc_type = "statute"

            def fetch(self, citation):
                return b""

            def parse(self, raw, source_url=""):
                return None

        register_converter(TestConverter)
        assert "test-reg:test" in CONVERTERS

        # Clean up
        CONVERTERS.clear()
        CONVERTERS.update(saved)

    def test_get_converter_specific(self):
        saved = dict(CONVERTERS)

        class TestConverter2(LegalDocConverter):
            jurisdiction = "test-get"
            source_format = "html"
            doc_type = "statute"

            def fetch(self, citation):
                return b""

            def parse(self, raw, source_url=""):
                return None

        CONVERTERS["test-get:html"] = TestConverter2

        result = get_converter("test-get", "html")
        assert result is not None
        assert isinstance(result, TestConverter2)

        CONVERTERS.clear()
        CONVERTERS.update(saved)

    def test_get_converter_any_format(self):
        saved = dict(CONVERTERS)

        class TestConverter3(LegalDocConverter):
            jurisdiction = "test-any"
            source_format = "xml"
            doc_type = "statute"

            def fetch(self, citation):
                return b""

            def parse(self, raw, source_url=""):
                return None

        CONVERTERS["test-any:xml"] = TestConverter3

        result = get_converter("test-any")
        assert result is not None

        CONVERTERS.clear()
        CONVERTERS.update(saved)

    def test_get_converter_not_found(self):
        result = get_converter("nonexistent-jurisdiction", "html")
        assert result is None

    def test_get_converter_no_format_not_found(self):
        result = get_converter("nonexistent-jurisdiction")
        assert result is None


class TestLegalDocConverter:
    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            LegalDocConverter()

    def test_concrete_subclass(self):
        class ConcreteConverter(LegalDocConverter):
            jurisdiction = "test"
            source_format = "html"
            doc_type = "statute"

            def fetch(self, citation):
                return b"<html>test</html>"

            def parse(self, raw, source_url=""):
                return AkomaNtoso(
                    uri="/test",
                    jurisdiction="test",
                    doc_type="statute",
                    source_format="html",
                    source_url=source_url,
                )

        converter = ConcreteConverter()
        assert converter.jurisdiction == "test"
        raw = converter.fetch("test citation")
        assert raw == b"<html>test</html>"
