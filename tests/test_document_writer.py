"""Tests for DocumentWriter - stores original + canonical documents."""

import json
import tempfile
from datetime import date
from pathlib import Path

import pytest

from axiom_corpus.writer import (
    CanonicalDocument,
    DocumentWriter,
    LocalBackend,
)


class TestCanonicalDocument:
    """Tests for CanonicalDocument schema."""

    def test_required_fields(self):
        """Test that required fields must be provided."""
        doc = CanonicalDocument(
            jurisdiction="us",
            doc_type="statute",
            citation="26 USC § 32",
            title=26,
            section="32",
            heading="Earned income",
            effective_date=date(2024, 1, 1),
            accessed_date=date(2025, 6, 15),
            source_url="https://uscode.house.gov/...",
            content_text="The credit shall be allowed.",
        )

        assert doc.jurisdiction == "us"
        assert doc.doc_type == "statute"
        assert doc.citation == "26 USC § 32"
        assert doc.effective_date == date(2024, 1, 1)
        assert doc.accessed_date == date(2025, 6, 15)

    def test_optional_fields_default(self):
        """Test that optional fields have sensible defaults."""
        doc = CanonicalDocument(
            jurisdiction="us",
            doc_type="statute",
            citation="26 USC § 32",
            title=26,
            section="32",
            heading="Earned income",
            effective_date=date(2024, 1, 1),
            accessed_date=date(2025, 6, 15),
            source_url="https://example.com",
            content_text="Text here.",
        )

        assert doc.subsections == []
        assert doc.release_point is None

    def test_to_dict_serializable(self):
        """Test that to_dict produces JSON-serializable output."""
        doc = CanonicalDocument(
            jurisdiction="us",
            doc_type="statute",
            citation="26 USC § 32",
            title=26,
            section="32",
            heading="Earned income",
            effective_date=date(2024, 1, 1),
            accessed_date=date(2025, 6, 15),
            source_url="https://example.com",
            content_text="Text here.",
        )

        d = doc.to_dict()
        json_str = json.dumps(d)  # Should not raise
        parsed = json.loads(json_str)

        assert parsed["citation"] == "26 USC § 32"
        assert parsed["effective_date"] == "2024-01-01"

    def test_storage_path_statute(self):
        """Test storage path generation for statutes."""
        doc = CanonicalDocument(
            jurisdiction="us",
            doc_type="statute",
            citation="26 USC § 32",
            title=26,
            section="32",
            heading="Earned income",
            effective_date=date(2024, 1, 1),
            accessed_date=date(2025, 6, 15),
            source_url="https://example.com",
            content_text="Text here.",
        )

        path = doc.storage_path()
        assert path == "us/statute/26/32/2024-01-01"

    def test_storage_path_guidance(self):
        """Test storage path generation for guidance documents."""
        doc = CanonicalDocument(
            jurisdiction="us",
            doc_type="guidance",
            citation="IRS Pub 596",
            title=None,
            section="pub-596",
            heading="Earned Income Credit",
            effective_date=date(2024, 1, 1),
            accessed_date=date(2025, 6, 15),
            source_url="https://irs.gov/...",
            content_text="Text here.",
            agency="irs",
        )

        path = doc.storage_path()
        assert path == "us/guidance/irs/pub-596/2024-01-01"

    def test_storage_path_uk(self):
        """Test storage path for UK legislation."""
        doc = CanonicalDocument(
            jurisdiction="uk",
            doc_type="statute",
            citation="Income Tax Act 2007 s.1",
            title=None,
            section="ita-2007-s1",
            heading="Overview",
            effective_date=date(2024, 1, 1),
            accessed_date=date(2025, 6, 15),
            source_url="https://legislation.gov.uk/...",
            content_text="Text here.",
        )

        path = doc.storage_path()
        assert path == "uk/statute/ita-2007-s1/2024-01-01"


class TestLocalBackend:
    """Tests for local filesystem backend."""

    def test_write_creates_directory(self):
        """Test that write creates the correct directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))

            doc = CanonicalDocument(
                jurisdiction="us",
                doc_type="statute",
                citation="26 USC § 32",
                title=26,
                section="32",
                heading="Earned income",
                effective_date=date(2024, 1, 1),
                accessed_date=date(2025, 6, 15),
                source_url="https://example.com",
                content_text="Text here.",
            )

            backend.write(doc, b"<section>raw xml</section>", "xml")

            expected_dir = Path(tmpdir) / "us" / "statute" / "26" / "32" / "2024-01-01"
            assert expected_dir.exists()

    def test_write_saves_original(self):
        """Test that original file is saved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))

            doc = CanonicalDocument(
                jurisdiction="us",
                doc_type="statute",
                citation="26 USC § 32",
                title=26,
                section="32",
                heading="Earned income",
                effective_date=date(2024, 1, 1),
                accessed_date=date(2025, 6, 15),
                source_url="https://example.com",
                content_text="Text here.",
            )

            original_content = b"<section>raw xml content</section>"
            backend.write(doc, original_content, "xml")

            original_file = Path(tmpdir) / "us" / "statute" / "26" / "32" / "2024-01-01" / "original.xml"
            assert original_file.exists()
            assert original_file.read_bytes() == original_content

    def test_write_saves_canonical_json(self):
        """Test that canonical.json is saved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))

            doc = CanonicalDocument(
                jurisdiction="us",
                doc_type="statute",
                citation="26 USC § 32",
                title=26,
                section="32",
                heading="Earned income",
                effective_date=date(2024, 1, 1),
                accessed_date=date(2025, 6, 15),
                source_url="https://example.com",
                content_text="The credit shall be allowed.",
            )

            backend.write(doc, b"<xml/>", "xml")

            json_file = Path(tmpdir) / "us" / "statute" / "26" / "32" / "2024-01-01" / "canonical.json"
            assert json_file.exists()

            data = json.loads(json_file.read_text())
            assert data["citation"] == "26 USC § 32"
            assert data["content_text"] == "The credit shall be allowed."

    def test_write_different_formats(self):
        """Test writing different original formats (xml, pdf, html)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))

            for fmt, content in [("xml", b"<xml/>"), ("pdf", b"%PDF-1.4"), ("html", b"<html>")]:
                doc = CanonicalDocument(
                    jurisdiction="us",
                    doc_type="guidance",
                    citation=f"Test {fmt}",
                    title=None,
                    section=f"test-{fmt}",
                    heading="Test",
                    effective_date=date(2024, 1, 1),
                    accessed_date=date(2025, 6, 15),
                    source_url="https://example.com",
                    content_text="Text.",
                )

                backend.write(doc, content, fmt)

                original_file = Path(tmpdir) / "us" / "guidance" / f"test-{fmt}" / "2024-01-01" / f"original.{fmt}"
                assert original_file.exists(), f"original.{fmt} should exist"

    def test_read_returns_document(self):
        """Test reading a previously written document."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))

            doc = CanonicalDocument(
                jurisdiction="us",
                doc_type="statute",
                citation="26 USC § 32",
                title=26,
                section="32",
                heading="Earned income",
                effective_date=date(2024, 1, 1),
                accessed_date=date(2025, 6, 15),
                source_url="https://example.com",
                content_text="Text here.",
            )

            backend.write(doc, b"<xml/>", "xml")

            # Read it back
            read_doc = backend.read("us/statute/26/32/2024-01-01")
            assert read_doc is not None
            assert read_doc.citation == "26 USC § 32"

    def test_read_nonexistent_returns_none(self):
        """Test reading a nonexistent document returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))

            result = backend.read("us/statute/99/999/2024-01-01")
            assert result is None

    def test_list_versions(self):
        """Test listing all versions of a document."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))

            # Write multiple versions
            for year in [2022, 2023, 2024]:
                doc = CanonicalDocument(
                    jurisdiction="us",
                    doc_type="statute",
                    citation="26 USC § 32",
                    title=26,
                    section="32",
                    heading="Earned income",
                    effective_date=date(year, 1, 1),
                    accessed_date=date(2025, 6, 15),
                    source_url="https://example.com",
                    content_text=f"Version {year}.",
                )
                backend.write(doc, b"<xml/>", "xml")

            versions = backend.list_versions("us/statute/26/32")
            assert len(versions) == 3
            assert "2022-01-01" in versions
            assert "2023-01-01" in versions
            assert "2024-01-01" in versions


class TestDocumentWriter:
    """Tests for high-level DocumentWriter interface."""

    def test_write_with_validation(self):
        """Test that writer validates document before writing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))
            writer = DocumentWriter(backend=backend)

            doc = CanonicalDocument(
                jurisdiction="us",
                doc_type="statute",
                citation="26 USC § 32",
                title=26,
                section="32",
                heading="Earned income",
                effective_date=date(2024, 1, 1),
                accessed_date=date(2025, 6, 15),
                source_url="https://example.com",
                content_text="Text here.",
            )

            # Should succeed
            path = writer.write(doc, b"<xml/>", "xml")
            assert path == "us/statute/26/32/2024-01-01"

    def test_write_returns_storage_path(self):
        """Test that write returns the storage path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))
            writer = DocumentWriter(backend=backend)

            doc = CanonicalDocument(
                jurisdiction="us",
                doc_type="statute",
                citation="26 USC § 32",
                title=26,
                section="32",
                heading="Earned income",
                effective_date=date(2024, 1, 1),
                accessed_date=date(2025, 6, 15),
                source_url="https://example.com",
                content_text="Text here.",
            )

            path = writer.write(doc, b"<xml/>", "xml")
            assert path == "us/statute/26/32/2024-01-01"

    def test_invalid_format_raises(self):
        """Test that invalid format raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalBackend(root=Path(tmpdir))
            writer = DocumentWriter(backend=backend)

            doc = CanonicalDocument(
                jurisdiction="us",
                doc_type="statute",
                citation="26 USC § 32",
                title=26,
                section="32",
                heading="Earned income",
                effective_date=date(2024, 1, 1),
                accessed_date=date(2025, 6, 15),
                source_url="https://example.com",
                content_text="Text here.",
            )

            with pytest.raises(ValueError, match="Unsupported format"):
                writer.write(doc, b"content", "exe")  # Invalid format
