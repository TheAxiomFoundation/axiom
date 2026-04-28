"""Document writer for storing original + canonical documents.

Provides a standard interface for scrapers to store documents with:
- Original file (raw XML, PDF, HTML as fetched)
- Canonical JSON (normalized schema for encoding)

Usage:
    from axiom_corpus.writer import CanonicalDocument, DocumentWriter, LocalBackend

    backend = LocalBackend(root=Path("/path/to/storage"))
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
        source_url="https://uscode.house.gov/...",
        content_text="The text of the statute...",
    )

    path = writer.write(doc, original_xml_bytes, "xml")
    # Returns: "us/statute/26/32/2024-01-01"
"""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


class StorageError(Exception):
    """Error during document storage."""


# Allowed original file formats
ALLOWED_FORMATS = {"xml", "pdf", "html", "json", "txt"}


@dataclass
class CanonicalDocument:
    """Canonical document schema for storage.

    This is the standard format that all scrapers must produce.
    It ensures consistency across jurisdictions and document types.
    """

    # Required fields
    jurisdiction: str  # e.g., "us", "uk", "us-ca" (California)
    doc_type: str  # "statute", "regulation", "guidance"
    citation: str  # Human-readable citation
    section: str  # Section identifier for path
    heading: str  # Document/section heading
    effective_date: date  # When this version took effect
    accessed_date: date  # When we fetched it
    source_url: str  # Original source URL
    content_text: str  # Full text content

    # Optional fields
    title: int | None = None  # Title number (for USC)
    agency: str | None = None  # Agency (for guidance)
    release_point: str | None = None  # e.g., "119-46" for USC
    subsections: list[dict] = field(default_factory=list)

    def storage_path(self) -> str:
        """Generate the storage path for this document.

        Format: {jurisdiction}/{doc_type}/{title|agency|}/{section}/{effective_date}

        Examples:
            - us/statute/26/32/2024-01-01
            - us/guidance/irs/pub-596/2024-01-01
            - uk/statute/ita-2007-s1/2024-01-01
        """
        parts = [self.jurisdiction, self.doc_type]

        if self.title is not None:
            parts.append(str(self.title))
        elif self.agency:
            parts.append(self.agency)

        parts.append(self.section)
        parts.append(self.effective_date.isoformat())

        return "/".join(parts)

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dictionary."""
        return {
            "jurisdiction": self.jurisdiction,
            "doc_type": self.doc_type,
            "citation": self.citation,
            "title": self.title,
            "section": self.section,
            "heading": self.heading,
            "effective_date": self.effective_date.isoformat(),
            "accessed_date": self.accessed_date.isoformat(),
            "source_url": self.source_url,
            "content_text": self.content_text,
            "agency": self.agency,
            "release_point": self.release_point,
            "subsections": self.subsections,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CanonicalDocument":
        """Create from dictionary (e.g., loaded from JSON)."""
        return cls(
            jurisdiction=data["jurisdiction"],
            doc_type=data["doc_type"],
            citation=data["citation"],
            title=data.get("title"),
            section=data["section"],
            heading=data["heading"],
            effective_date=date.fromisoformat(data["effective_date"]),
            accessed_date=date.fromisoformat(data["accessed_date"]),
            source_url=data["source_url"],
            content_text=data["content_text"],
            agency=data.get("agency"),
            release_point=data.get("release_point"),
            subsections=data.get("subsections", []),
        )


class StorageBackendBase(ABC):
    """Abstract base for document storage backends."""

    @abstractmethod
    def write(self, doc: CanonicalDocument, original: bytes, original_format: str) -> str:
        """Write document to storage.

        Args:
            doc: Canonical document metadata
            original: Raw original content bytes
            original_format: File extension (xml, pdf, html)

        Returns:
            Storage path where document was written
        """
        pass

    @abstractmethod
    def read(self, path: str) -> CanonicalDocument | None:
        """Read canonical document from storage.

        Args:
            path: Storage path (e.g., "us/statute/26/32/2024-01-01")

        Returns:
            CanonicalDocument if found, None otherwise
        """
        pass

    @abstractmethod
    def read_original(self, path: str) -> bytes | None:
        """Read original file content.

        Args:
            path: Storage path

        Returns:
            Original file bytes if found, None otherwise
        """
        pass

    @abstractmethod
    def list_versions(self, base_path: str) -> list[str]:
        """List all versions (effective dates) for a document.

        Args:
            base_path: Path without effective date (e.g., "us/statute/26/32")

        Returns:
            List of effective dates as strings
        """
        pass


class LocalBackend(StorageBackendBase):
    """Local filesystem storage backend."""

    def __init__(self, root: Path):
        """Initialize with root directory.

        Args:
            root: Root directory for storage
        """
        self.root = root

    def write(self, doc: CanonicalDocument, original: bytes, original_format: str) -> str:
        """Write document to local filesystem."""
        path = doc.storage_path()
        dir_path = self.root / path

        # Create directory
        dir_path.mkdir(parents=True, exist_ok=True)

        # Write original file
        original_file = dir_path / f"original.{original_format}"
        original_file.write_bytes(original)

        # Write canonical JSON
        json_file = dir_path / "canonical.json"
        json_file.write_text(json.dumps(doc.to_dict(), indent=2))

        return path

    def read(self, path: str) -> CanonicalDocument | None:
        """Read canonical document from local filesystem."""
        json_file = self.root / path / "canonical.json"

        if not json_file.exists():
            return None

        data = json.loads(json_file.read_text())
        return CanonicalDocument.from_dict(data)

    def read_original(self, path: str) -> bytes | None:
        """Read original file from local filesystem."""
        dir_path = self.root / path

        if not dir_path.exists():
            return None

        # Find original file (could be .xml, .pdf, .html, etc.)
        for ext in ALLOWED_FORMATS:
            original_file = dir_path / f"original.{ext}"
            if original_file.exists():
                return original_file.read_bytes()

        return None

    def list_versions(self, base_path: str) -> list[str]:
        """List all versions for a document."""
        dir_path = self.root / base_path

        if not dir_path.exists():
            return []

        versions = []
        for child in dir_path.iterdir():
            if child.is_dir():
                # Check if it looks like a date (YYYY-MM-DD)
                name = child.name
                if len(name) == 10 and name[4] == "-" and name[7] == "-":
                    versions.append(name)

        return sorted(versions)


class DocumentWriter:
    """High-level document writer with validation."""

    def __init__(self, backend: StorageBackendBase):
        """Initialize with storage backend.

        Args:
            backend: Storage backend (LocalBackend, R2Backend, etc.)
        """
        self.backend = backend

    def write(self, doc: CanonicalDocument, original: bytes, original_format: str) -> str:
        """Write document with validation.

        Args:
            doc: Canonical document
            original: Original content bytes
            original_format: File extension (xml, pdf, html)

        Returns:
            Storage path

        Raises:
            ValueError: If format is not allowed
            StorageError: If write fails
        """
        # Validate format
        if original_format not in ALLOWED_FORMATS:
            raise ValueError(
                f"Unsupported format: {original_format}. "
                f"Allowed: {', '.join(sorted(ALLOWED_FORMATS))}"
            )

        # Write to backend
        return self.backend.write(doc, original, original_format)

    def read(self, path: str) -> CanonicalDocument | None:
        """Read canonical document.

        Args:
            path: Storage path

        Returns:
            CanonicalDocument if found
        """
        return self.backend.read(path)
