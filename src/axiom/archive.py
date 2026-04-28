"""Main AxiomArchive class - the public API."""

from datetime import date
from pathlib import Path

from axiom.models import Citation, SearchResult, Section, TitleInfo
from axiom.storage.base import StorageBackend
from axiom.storage.sqlite import SQLiteStorage


class AxiomArchive:
    """Main interface for accessing the law archive.

    Example:
        >>> axiom = AxiomArchive()
        >>> eitc = axiom.get("26 USC 32")
        >>> print(eitc.section_title)
        "Earned income"

        >>> results = axiom.search("child tax credit")
        >>> for r in results:
        ...     print(r.citation.usc_cite, r.section_title)
    """

    def __init__(
        self,
        db_path: Path | str = "axiom.db",
        storage: StorageBackend | None = None,
    ):
        """Initialize AxiomArchive.

        Args:
            db_path: Path to SQLite database (ignored if storage is provided)
            storage: Optional custom storage backend
        """
        self.storage = storage or SQLiteStorage(db_path)

    def get(
        self,
        citation: str | Citation,
        as_of: date | None = None,
    ) -> Section | None:
        """Get a section by citation.

        Args:
            citation: USC citation string (e.g., "26 USC 32") or Citation object
            as_of: Optional date for historical version

        Returns:
            Section object or None if not found

        Example:
            >>> axiom.get("26 USC 32")
            >>> axiom.get("26 USC 32(a)(1)")
            >>> axiom.get("26 USC 32", as_of=date(2020, 1, 1))
        """
        if isinstance(citation, str):
            citation = Citation.from_string(citation)

        return self.storage.get_section(
            title=citation.title,
            section=citation.section,
            subsection=citation.subsection,
            as_of=as_of,
        )

    def search(
        self,
        query: str,
        title: int | None = None,
        limit: int = 20,
    ) -> list[SearchResult]:
        """Search for sections matching a query.

        Args:
            query: Search query (supports FTS5 syntax)
            title: Optional title number to limit search
            limit: Maximum results to return

        Returns:
            List of SearchResult objects

        Example:
            >>> axiom.search("earned income credit")
            >>> axiom.search("child", title=26, limit=10)
        """
        return self.storage.search(query, title=title, limit=limit)

    def list_titles(self) -> list[TitleInfo]:
        """List all available US Code titles.

        Returns:
            List of TitleInfo objects with metadata
        """
        return self.storage.list_titles()

    def get_references(self, citation: str | Citation) -> dict[str, list[str]]:
        """Get cross-references for a section.

        Args:
            citation: USC citation string or Citation object

        Returns:
            Dict with 'references_to' and 'referenced_by' lists

        Example:
            >>> refs = axiom.get_references("26 USC 32")
            >>> print(refs["references_to"])  # What this section cites
            >>> print(refs["referenced_by"])  # What cites this section
        """
        if isinstance(citation, str):
            citation = Citation.from_string(citation)

        return {
            "references_to": self.storage.get_references_to(citation.title, citation.section),
            "referenced_by": self.storage.get_referenced_by(citation.title, citation.section),
        }

    def ingest_title(self, xml_path: Path | str) -> int:
        """Ingest a US Code title from USLM XML.

        Args:
            xml_path: Path to USLM XML file

        Returns:
            Number of sections ingested

        Example:
            >>> axiom.ingest_title("data/uscode/usc26.xml")
            2345  # sections ingested
        """
        from axiom.parsers.us.statutes import USLMParser

        parser = USLMParser(xml_path)
        count = 0

        title_num = parser.get_title_number()
        title_name = parser.get_title_name()

        print(f"Ingesting Title {title_num}: {title_name}")

        for section in parser.iter_sections():
            self.storage.store_section(section)
            count += 1
            if count % 100 == 0:
                print(f"  Processed {count} sections...")

        # Update title metadata
        # Positive law titles (enacted into law directly, not just prima facie evidence)
        positive_law_titles = {
            1,
            3,
            4,
            5,
            9,
            10,
            11,
            13,
            14,
            17,
            18,
            23,
            28,
            31,
            32,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            44,
            46,
            49,
            51,
            54,
        }
        is_positive_law = title_num in positive_law_titles

        self.storage.update_title_metadata(title_num, title_name, is_positive_law)

        print(f"Completed: {count} sections from Title {title_num}")
        return count
