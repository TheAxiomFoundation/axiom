"""SQLite storage backend with full-text search."""

import json
from datetime import date
from pathlib import Path

import sqlite_utils

from axiom.models import Citation, SearchResult, Section, Subsection, TitleInfo
from axiom.storage.base import StorageBackend


class SQLiteStorage(StorageBackend):
    """SQLite-based storage with FTS5 full-text search."""

    def __init__(self, db_path: Path | str = "axiom.db"):
        """Initialize SQLite storage.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db = sqlite_utils.Database(str(self.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        """Create database tables if they don't exist."""
        # Main sections table
        if "sections" not in self.db.table_names():
            self.db["sections"].create(
                {
                    "id": str,  # USLM identifier
                    "title": int,
                    "section": str,
                    "title_name": str,
                    "section_title": str,
                    "text": str,
                    "subsections_json": str,  # JSON-serialized subsections
                    "enacted_date": str,
                    "last_amended": str,
                    "public_laws_json": str,
                    "effective_date": str,
                    "references_to_json": str,
                    "referenced_by_json": str,
                    "source_url": str,
                    "retrieved_at": str,
                },
                pk="id",
            )
            # Create indexes
            self.db["sections"].create_index(["title", "section"], unique=True, if_not_exists=True)

            # Enable FTS5 full-text search
            self.db.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS sections_fts USING fts5(
                    section_title,
                    text,
                    content='sections',
                    content_rowid='rowid'
                )
            """
            )

            # Triggers to keep FTS in sync
            self.db.execute(
                """
                CREATE TRIGGER IF NOT EXISTS sections_ai AFTER INSERT ON sections BEGIN
                    INSERT INTO sections_fts(rowid, section_title, text)
                    VALUES (new.rowid, new.section_title, new.text);
                END
            """
            )
            self.db.execute(
                """
                CREATE TRIGGER IF NOT EXISTS sections_ad AFTER DELETE ON sections BEGIN
                    INSERT INTO sections_fts(sections_fts, rowid, section_title, text)
                    VALUES ('delete', old.rowid, old.section_title, old.text);
                END
            """
            )
            self.db.execute(
                """
                CREATE TRIGGER IF NOT EXISTS sections_au AFTER UPDATE ON sections BEGIN
                    INSERT INTO sections_fts(sections_fts, rowid, section_title, text)
                    VALUES ('delete', old.rowid, old.section_title, old.text);
                    INSERT INTO sections_fts(rowid, section_title, text)
                    VALUES (new.rowid, new.section_title, new.text);
                END
            """
            )

        # Cross-references table for efficient lookups
        if "cross_references" not in self.db.table_names():
            self.db["cross_references"].create(
                {
                    "from_title": int,
                    "from_section": str,
                    "to_title": int,
                    "to_section": str,
                },
                pk=("from_title", "from_section", "to_title", "to_section"),
            )
            self.db["cross_references"].create_index(["to_title", "to_section"], if_not_exists=True)

        # Title metadata
        if "titles" not in self.db.table_names():
            self.db["titles"].create(
                {
                    "number": int,
                    "name": str,
                    "section_count": int,
                    "last_updated": str,
                    "is_positive_law": bool,
                },
                pk="number",
            )

    def store_section(self, section: Section) -> None:
        """Store a section in the database."""
        # Serialize subsections to JSON
        subsections_json = json.dumps([self._subsection_to_dict(s) for s in section.subsections])

        record = {
            "id": section.uslm_id or f"{section.citation.title}/{section.citation.section}",
            "title": section.citation.title,
            "section": section.citation.section,
            "title_name": section.title_name,
            "section_title": section.section_title,
            "text": section.text,
            "subsections_json": subsections_json,
            "enacted_date": section.enacted_date.isoformat() if section.enacted_date else None,
            "last_amended": section.last_amended.isoformat() if section.last_amended else None,
            "public_laws_json": json.dumps(section.public_laws),
            "effective_date": (
                section.effective_date.isoformat() if section.effective_date else None
            ),
            "references_to_json": json.dumps(section.references_to),
            "referenced_by_json": json.dumps(section.referenced_by),
            "source_url": section.source_url,
            "retrieved_at": section.retrieved_at.isoformat(),
        }

        # Use INSERT OR REPLACE to handle duplicate (title, section) pairs
        # This can occur when the same section number appears multiple times
        # in the XML with different USLM IDs (e.g., parsing anomalies in Title 10)
        self.db.execute(
            """
            INSERT OR REPLACE INTO sections (
                id, title, section, title_name, section_title, text,
                subsections_json, enacted_date, last_amended, public_laws_json,
                effective_date, references_to_json, referenced_by_json,
                source_url, retrieved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record["id"],
                record["title"],
                record["section"],
                record["title_name"],
                record["section_title"],
                record["text"],
                record["subsections_json"],
                record["enacted_date"],
                record["last_amended"],
                record["public_laws_json"],
                record["effective_date"],
                record["references_to_json"],
                record["referenced_by_json"],
                record["source_url"],
                record["retrieved_at"],
            ],
        )
        # Commit after raw execute (sqlite_utils methods auto-commit, but execute doesn't)
        self.db.conn.commit()

        # Update cross-references
        self._update_cross_references(section)

    def _subsection_to_dict(self, sub: Subsection) -> dict:
        """Convert Subsection to dictionary for JSON serialization."""
        return {
            "identifier": sub.identifier,
            "heading": sub.heading,
            "text": sub.text,
            "children": [self._subsection_to_dict(c) for c in sub.children],
        }

    def _dict_to_subsection(self, d: dict) -> Subsection:
        """Convert dictionary to Subsection."""
        return Subsection(
            identifier=d["identifier"],
            heading=d.get("heading"),
            text=d["text"],
            children=[self._dict_to_subsection(c) for c in d.get("children", [])],
        )

    def _update_cross_references(self, section: Section) -> None:
        """Update cross-reference table for a section."""
        # Remove existing references from this section
        self.db.execute(
            "DELETE FROM cross_references WHERE from_title = ? AND from_section = ?",
            [section.citation.title, section.citation.section],
        )

        # Add new references
        for ref in section.references_to:
            try:
                ref_citation = Citation.from_string(ref)
                self.db["cross_references"].insert(
                    {
                        "from_title": section.citation.title,
                        "from_section": section.citation.section,
                        "to_title": ref_citation.title,
                        "to_section": ref_citation.section,
                    },
                    ignore=True,
                )
            except ValueError:
                pass  # Skip malformed references

    def get_section(
        self,
        title: int,
        section: str,
        subsection: str | None = None,
        as_of: date | None = None,
    ) -> Section | None:
        """Retrieve a section by citation.

        Note: the ``as_of`` parameter is accepted but currently ignored by
        this backend. There is no version table; the most recently
        ingested row is always returned. See
        ``docs/historical-versioning.md`` for the roadmap.
        """
        # TODO: Implement historical versions (as_of parameter) —
        # see docs/historical-versioning.md.
        row = self.db.execute(
            "SELECT * FROM sections WHERE title = ? AND section = ?", [title, section]
        ).fetchone()

        if not row:
            return None

        # Convert row to Section
        return self._row_to_section(row)

    def _row_to_section(self, row: tuple) -> Section:
        """Convert a database row to a Section model."""
        # Get column names
        cursor = self.db.execute("SELECT * FROM sections LIMIT 0")
        columns = [desc[0] for desc in cursor.description]
        record = dict(zip(columns, row, strict=True))

        subsections = [
            self._dict_to_subsection(d) for d in json.loads(record["subsections_json"] or "[]")
        ]

        return Section(
            citation=Citation(title=record["title"], section=record["section"]),
            title_name=record["title_name"],
            section_title=record["section_title"],
            text=record["text"],
            subsections=subsections,
            enacted_date=(
                date.fromisoformat(record["enacted_date"]) if record["enacted_date"] else None
            ),
            last_amended=(
                date.fromisoformat(record["last_amended"]) if record["last_amended"] else None
            ),
            public_laws=json.loads(record["public_laws_json"] or "[]"),
            effective_date=(
                date.fromisoformat(record["effective_date"]) if record["effective_date"] else None
            ),
            references_to=json.loads(record["references_to_json"] or "[]"),
            referenced_by=json.loads(record["referenced_by_json"] or "[]"),
            source_url=record["source_url"],
            retrieved_at=date.fromisoformat(record["retrieved_at"]),
            uslm_id=record["id"],
        )

    def search(
        self,
        query: str,
        title: int | None = None,
        limit: int = 20,
    ) -> list[SearchResult]:
        """Full-text search across sections."""
        if title is not None:
            sql = """
                SELECT s.title, s.section, s.section_title,
                       snippet(sections_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
                       bm25(sections_fts) as score
                FROM sections_fts
                JOIN sections s ON sections_fts.rowid = s.rowid
                WHERE sections_fts MATCH ? AND s.title = ?
                ORDER BY score
                LIMIT ?
            """
            rows = self.db.execute(sql, [query, title, limit]).fetchall()
        else:
            sql = """
                SELECT s.title, s.section, s.section_title,
                       snippet(sections_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
                       bm25(sections_fts) as score
                FROM sections_fts
                JOIN sections s ON sections_fts.rowid = s.rowid
                WHERE sections_fts MATCH ?
                ORDER BY score
                LIMIT ?
            """
            rows = self.db.execute(sql, [query, limit]).fetchall()

        results = []
        for row in rows:
            title_num, section, section_title, snippet, score = row
            results.append(
                SearchResult(
                    citation=Citation(title=title_num, section=section),
                    section_title=section_title,
                    snippet=snippet,
                    score=abs(score),  # BM25 returns negative scores
                )
            )

        return results

    def list_titles(self) -> list[TitleInfo]:
        """List all available titles with metadata."""
        rows = self.db.execute("SELECT * FROM titles ORDER BY number").fetchall()
        return [
            TitleInfo(
                number=row[0],
                name=row[1],
                section_count=row[2],
                last_updated=date.fromisoformat(row[3]),
                is_positive_law=bool(row[4]),
            )
            for row in rows
        ]

    def get_references_to(self, title: int, section: str) -> list[str]:
        """Get sections that this section references."""
        rows = self.db.execute(
            "SELECT to_title, to_section FROM cross_references WHERE from_title = ? AND from_section = ?",
            [title, section],
        ).fetchall()
        return [f"{row[0]} USC {row[1]}" for row in rows]

    def get_referenced_by(self, title: int, section: str) -> list[str]:
        """Get sections that reference this section."""
        rows = self.db.execute(
            "SELECT from_title, from_section FROM cross_references WHERE to_title = ? AND to_section = ?",
            [title, section],
        ).fetchall()
        return [f"{row[0]} USC {row[1]}" for row in rows]

    def update_title_metadata(self, title_num: int, name: str, is_positive_law: bool) -> None:
        """Update metadata for a title."""
        # Count sections
        count = self.db.execute(
            "SELECT COUNT(*) FROM sections WHERE title = ?", [title_num]
        ).fetchone()[0]

        self.db["titles"].upsert(
            {
                "number": title_num,
                "name": name,
                "section_count": count,
                "last_updated": date.today().isoformat(),
                "is_positive_law": is_positive_law,
            },
            pk="number",
        )
