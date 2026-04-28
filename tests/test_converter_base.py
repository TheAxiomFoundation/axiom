"""Tests for LegalDocConverter base class and registry."""

import sys
from pathlib import Path

# Add src to path for direct import without triggering full axiom package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest

# Import directly from the module to avoid full package import
from axiom.converters.base import (
    CONVERTERS,
    AknSection,
    AknSubsection,
    AkomaNtoso,
    LegalDocConverter,
    get_converter,
    register_converter,
)


class TestAkomaNtosoModels:
    """Test Akoma Ntoso Pydantic models."""

    def test_akn_subsection_basic(self):
        """Test basic AknSubsection creation."""
        sub = AknSubsection(
            id="sec-1-a",
            identifier="a",
            text="This is subsection (a).",
        )
        assert sub.id == "sec-1-a"
        assert sub.identifier == "a"
        assert sub.text == "This is subsection (a)."
        assert sub.children == []

    def test_akn_subsection_with_children(self):
        """Test AknSubsection with nested children."""
        child = AknSubsection(
            id="sec-1-a-1",
            identifier="1",
            text="Paragraph 1",
        )
        parent = AknSubsection(
            id="sec-1-a",
            identifier="a",
            text="Introduction",
            children=[child],
        )
        assert len(parent.children) == 1
        assert parent.children[0].identifier == "1"

    def test_akn_section_basic(self):
        """Test basic AknSection creation."""
        section = AknSection(
            id="/us/statute/26/32",
            jurisdiction="us",
            doc_type="statute",
            title="Earned income",
            text="The earned income credit is...",
            source_url="https://uscode.house.gov/...",
        )
        assert section.id == "/us/statute/26/32"
        assert section.jurisdiction == "us"
        assert section.doc_type == "statute"

    def test_akoma_ntoso_document(self):
        """Test full AkomaNtoso document creation."""
        section = AknSection(
            id="/us/statute/26/32",
            jurisdiction="us",
            doc_type="statute",
            title="Earned income",
            text="The earned income credit is...",
            source_url="https://uscode.house.gov/...",
        )
        doc = AkomaNtoso(
            uri="/us/statute/26/32",
            jurisdiction="us",
            doc_type="statute",
            source_format="uslm",
            source_url="https://uscode.house.gov/...",
            sections=[section],
        )
        assert doc.uri == "/us/statute/26/32"
        assert len(doc.sections) == 1


class TestConverterRegistry:
    """Test converter registry functionality."""

    def test_register_converter_decorator(self):
        """Test that @register_converter adds to registry."""
        # Clear registry for clean test
        CONVERTERS.clear()

        @register_converter
        class TestConverter(LegalDocConverter):
            jurisdiction = "test"
            source_format = "html"
            doc_type = "statute"

            def fetch(self, citation: str) -> bytes:
                return b"test"

            def parse(self, raw: bytes, source_url: str = "") -> AkomaNtoso:
                return AkomaNtoso(
                    uri="/test",
                    jurisdiction="test",
                    doc_type="statute",
                    source_format="html",
                    source_url=source_url,
                    sections=[],
                )

        assert "test:html" in CONVERTERS
        assert CONVERTERS["test:html"] is TestConverter

    def test_get_converter_exact_match(self):
        """Test getting converter by jurisdiction and format."""
        CONVERTERS.clear()

        @register_converter
        class USLMConverter(LegalDocConverter):
            jurisdiction = "us"
            source_format = "uslm"
            doc_type = "statute"

            def fetch(self, citation: str) -> bytes:
                return b""

            def parse(self, raw: bytes, source_url: str = "") -> AkomaNtoso:
                return AkomaNtoso(
                    uri="/us",
                    jurisdiction="us",
                    doc_type="statute",
                    source_format="uslm",
                    source_url=source_url,
                    sections=[],
                )

        converter = get_converter("us", "uslm")
        assert converter is not None
        assert isinstance(converter, USLMConverter)

    def test_get_converter_not_found(self):
        """Test that None is returned for unknown converter."""
        CONVERTERS.clear()
        converter = get_converter("unknown", "unknown")
        assert converter is None


class TestLegalDocConverter:
    """Test LegalDocConverter base class behavior."""

    def test_convert_method(self):
        """Test the convert() pipeline: fetch -> parse."""

        class MockConverter(LegalDocConverter):
            jurisdiction = "mock"
            source_format = "xml"
            doc_type = "statute"

            def fetch(self, citation: str) -> bytes:
                return f"<doc>{citation}</doc>".encode()

            def parse(self, raw: bytes, source_url: str = "") -> AkomaNtoso:
                return AkomaNtoso(
                    uri=f"/mock/{raw.decode()}",
                    jurisdiction="mock",
                    doc_type="statute",
                    source_format="xml",
                    source_url=source_url,
                    sections=[],
                )

        converter = MockConverter()
        doc = converter.convert("section-1")
        assert "/mock/<doc>section-1</doc>" in doc.uri

    def test_to_rules_default_implementation(self):
        """Test default to_rules() yields section records."""

        class SimpleConverter(LegalDocConverter):
            jurisdiction = "simple"
            source_format = "text"
            doc_type = "statute"

            def fetch(self, citation: str) -> bytes:
                return b""

            def parse(self, raw: bytes, source_url: str = "") -> AkomaNtoso:
                section = AknSection(
                    id="/simple/1",
                    jurisdiction="simple",
                    doc_type="statute",
                    title="Test Section",
                    text="Section text here.",
                    source_url="https://example.com",
                )
                return AkomaNtoso(
                    uri="/simple/1",
                    jurisdiction="simple",
                    doc_type="statute",
                    source_format="text",
                    source_url="https://example.com",
                    sections=[section],
                )

        converter = SimpleConverter()
        doc = converter.parse(b"", "https://example.com")
        rules = list(converter.to_rules(doc))

        assert len(rules) >= 1
        rule = rules[0]
        assert rule["jurisdiction"] == "simple"
        assert rule["doc_type"] == "statute"
        assert rule["heading"] == "Test Section"
        assert rule["body"] == "Section text here."
        assert rule["citation_path"] == "/simple/1"

    def test_to_rules_with_subsections(self):
        """Test to_rules() handles nested subsections."""

        class NestedConverter(LegalDocConverter):
            jurisdiction = "nested"
            source_format = "xml"
            doc_type = "statute"

            def fetch(self, citation: str) -> bytes:
                return b""

            def parse(self, raw: bytes, source_url: str = "") -> AkomaNtoso:
                child_sub = AknSubsection(
                    id="/nested/1/a/1",
                    identifier="1",
                    text="Paragraph 1 text.",
                )
                sub = AknSubsection(
                    id="/nested/1/a",
                    identifier="a",
                    text="Subsection (a) text.",
                    children=[child_sub],
                )
                section = AknSection(
                    id="/nested/1",
                    jurisdiction="nested",
                    doc_type="statute",
                    title="Section 1",
                    text="Main section text.",
                    subsections=[sub],
                    source_url="https://example.com",
                )
                return AkomaNtoso(
                    uri="/nested/1",
                    jurisdiction="nested",
                    doc_type="statute",
                    source_format="xml",
                    source_url="https://example.com",
                    sections=[section],
                )

        converter = NestedConverter()
        doc = converter.parse(b"", "")
        rules = list(converter.to_rules(doc))

        # Should have 3 rules: section, subsection (a), paragraph (1)
        assert len(rules) == 3
        citation_paths = [r["citation_path"] for r in rules]
        assert "/nested/1" in citation_paths
        assert "/nested/1/a" in citation_paths
        assert "/nested/1/a/1" in citation_paths

    def test_abstract_methods_raise(self):
        """Test that instantiating without implementing abstract methods fails."""
        with pytest.raises(TypeError):
            LegalDocConverter()  # type: ignore
