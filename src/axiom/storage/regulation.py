"""SQLite storage backend for CFR regulations."""

import json
from datetime import date
from pathlib import Path

import sqlite_utils

from axiom.models_regulation import (
    CFRCitation,
    Regulation,
    RegulationSearchResult,
    RegulationSubsection,
)


class RegulationStorage:
    """SQLite-based storage for CFR regulations with FTS5 full-text search."""

    def __init__(self, db_path: Path | str = "axiom.db"):
        """Initialize regulation storage.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db = sqlite_utils.Database(str(self.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        """Create database tables if they don't exist."""
        # Main regulations table
        if "regulations" not in self.db.table_names():
            self.db["regulations"].create(
                {
                    "id": str,  # "26/1/32-1" format
                    "title": int,
                    "part": int,
                    "section": str,
                    "heading": str,
                    "authority": str,
                    "source": str,
                    "full_text": str,
                    "subsections_json": str,
                    "effective_date": str,
                    "source_statutes_json": str,
                    "cross_references_json": str,
                    "amendments_json": str,
                    "source_url": str,
                    "retrieved_at": str,
                },
                pk="id",
            )
            # Create indexes
            self.db["regulations"].create_index(
                ["title", "part", "section"], unique=True, if_not_exists=True
            )
            self.db["regulations"].create_index(["title"], if_not_exists=True)
            self.db["regulations"].create_index(["title", "part"], if_not_exists=True)

            # Enable FTS5 full-text search
            self.db.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS regulations_fts USING fts5(
                    heading,
                    full_text,
                    content='regulations',
                    content_rowid='rowid'
                )
            """
            )

            # Triggers to keep FTS in sync
            self.db.execute(
                """
                CREATE TRIGGER IF NOT EXISTS regulations_ai AFTER INSERT ON regulations BEGIN
                    INSERT INTO regulations_fts(rowid, heading, full_text)
                    VALUES (new.rowid, new.heading, new.full_text);
                END
            """
            )
            self.db.execute(
                """
                CREATE TRIGGER IF NOT EXISTS regulations_ad AFTER DELETE ON regulations BEGIN
                    INSERT INTO regulations_fts(regulations_fts, rowid, heading, full_text)
                    VALUES ('delete', old.rowid, old.heading, old.full_text);
                END
            """
            )
            self.db.execute(
                """
                CREATE TRIGGER IF NOT EXISTS regulations_au AFTER UPDATE ON regulations BEGIN
                    INSERT INTO regulations_fts(regulations_fts, rowid, heading, full_text)
                    VALUES ('delete', old.rowid, old.heading, old.full_text);
                    INSERT INTO regulations_fts(rowid, heading, full_text)
                    VALUES (new.rowid, new.heading, new.full_text);
                END
            """
            )

        # CFR title metadata
        if "cfr_titles" not in self.db.table_names():
            self.db["cfr_titles"].create(
                {
                    "number": int,
                    "name": str,
                    "regulation_count": int,
                    "last_updated": str,
                    "amendment_date": str,
                },
                pk="number",
            )

    def store_regulation(self, regulation: Regulation) -> None:
        """Store a regulation in the database.

        Args:
            regulation: Regulation object to store
        """
        reg_id = f"{regulation.citation.title}/{regulation.citation.part}/{regulation.citation.section}"

        # Serialize nested objects
        subsections_json = json.dumps(
            [self._subsection_to_dict(s) for s in regulation.subsections]
        )
        amendments_json = json.dumps(
            [a.model_dump(mode="json") for a in regulation.amendments]
        )

        record = {
            "id": reg_id,
            "title": regulation.citation.title,
            "part": regulation.citation.part,
            "section": regulation.citation.section,
            "heading": regulation.heading,
            "authority": regulation.authority,
            "source": regulation.source,
            "full_text": regulation.full_text,
            "subsections_json": subsections_json,
            "effective_date": regulation.effective_date.isoformat(),
            "source_statutes_json": json.dumps(regulation.source_statutes),
            "cross_references_json": json.dumps(regulation.cross_references),
            "amendments_json": amendments_json,
            "source_url": regulation.source_url,
            "retrieved_at": (
                regulation.retrieved_at.isoformat() if regulation.retrieved_at else None
            ),
        }

        # Upsert
        self.db.execute(
            """
            INSERT OR REPLACE INTO regulations (
                id, title, part, section, heading, authority, source, full_text,
                subsections_json, effective_date, source_statutes_json,
                cross_references_json, amendments_json, source_url, retrieved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record["id"],
                record["title"],
                record["part"],
                record["section"],
                record["heading"],
                record["authority"],
                record["source"],
                record["full_text"],
                record["subsections_json"],
                record["effective_date"],
                record["source_statutes_json"],
                record["cross_references_json"],
                record["amendments_json"],
                record["source_url"],
                record["retrieved_at"],
            ],
        )
        self.db.conn.commit()

    def _subsection_to_dict(self, subsec: RegulationSubsection) -> dict:
        """Convert RegulationSubsection to dictionary for JSON."""
        return {
            "id": subsec.id,
            "heading": subsec.heading,
            "text": subsec.text,
            "children": [self._subsection_to_dict(c) for c in subsec.children],
        }

    def _dict_to_subsection(self, d: dict) -> RegulationSubsection:
        """Convert dictionary to RegulationSubsection."""
        return RegulationSubsection(
            id=d["id"],
            heading=d.get("heading"),
            text=d["text"],
            children=[self._dict_to_subsection(c) for c in d.get("children", [])],
        )

    def get_regulation(
        self,
        title: int,
        part: int,
        section: str,
    ) -> Regulation | None:
        """Retrieve a regulation by title, part, and section.

        Args:
            title: CFR title number
            part: Part number within title
            section: Section number

        Returns:
            Regulation if found, None otherwise
        """
        row = self.db.execute(
            "SELECT * FROM regulations WHERE title = ? AND part = ? AND section = ?",
            [title, part, section],
        ).fetchone()

        if not row:
            return None

        return self._row_to_regulation(row)

    def get_by_citation(self, citation: CFRCitation) -> Regulation | None:
        """Retrieve a regulation by CFR citation.

        Args:
            citation: CFRCitation object

        Returns:
            Regulation if found, None otherwise
        """
        if citation.section is None:
            return None  # pragma: no cover
        return self.get_regulation(citation.title, citation.part, citation.section)

    def _row_to_regulation(self, row: tuple) -> Regulation:
        """Convert a database row to a Regulation model."""
        # Get column names
        cursor = self.db.execute("SELECT * FROM regulations LIMIT 0")
        columns = [desc[0] for desc in cursor.description]
        record = dict(zip(columns, row, strict=True))

        subsections = [
            self._dict_to_subsection(d)
            for d in json.loads(record["subsections_json"] or "[]")
        ]

        return Regulation(
            citation=CFRCitation(
                title=record["title"],
                part=record["part"],
                section=record["section"],
            ),
            heading=record["heading"],
            authority=record["authority"],
            source=record["source"],
            full_text=record["full_text"],
            subsections=subsections,
            effective_date=date.fromisoformat(record["effective_date"]),
            source_statutes=json.loads(record["source_statutes_json"] or "[]"),
            cross_references=json.loads(record["cross_references_json"] or "[]"),
            source_url=record["source_url"],
            retrieved_at=(
                date.fromisoformat(record["retrieved_at"])
                if record["retrieved_at"]
                else None
            ),
        )

    def search(
        self,
        query: str,
        title: int | None = None,
        limit: int = 20,
    ) -> list[RegulationSearchResult]:
        """Full-text search across regulations.

        Args:
            query: Search query
            title: Optional CFR title to filter by
            limit: Maximum results to return

        Returns:
            List of search results
        """
        if title is not None:
            sql = """
                SELECT r.title, r.part, r.section, r.heading,
                       snippet(regulations_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
                       bm25(regulations_fts) as score,
                       r.effective_date
                FROM regulations_fts
                JOIN regulations r ON regulations_fts.rowid = r.rowid
                WHERE regulations_fts MATCH ? AND r.title = ?
                ORDER BY score
                LIMIT ?
            """
            rows = self.db.execute(sql, [query, title, limit]).fetchall()
        else:
            sql = """
                SELECT r.title, r.part, r.section, r.heading,
                       snippet(regulations_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
                       bm25(regulations_fts) as score,
                       r.effective_date
                FROM regulations_fts
                JOIN regulations r ON regulations_fts.rowid = r.rowid
                WHERE regulations_fts MATCH ?
                ORDER BY score
                LIMIT ?
            """
            rows = self.db.execute(sql, [query, limit]).fetchall()

        results = []
        for row in rows:
            title_num, part, section, heading, snippet, score, eff_date = row
            cfr_cite = f"{title_num} CFR {part}.{section}"
            results.append(
                RegulationSearchResult(
                    cfr_cite=cfr_cite,
                    heading=heading,
                    snippet=snippet,
                    score=abs(score),  # BM25 returns negative scores
                    effective_date=date.fromisoformat(eff_date),
                )
            )

        return results

    def list_cfr_titles(self) -> list[dict]:
        """List all CFR titles with metadata.

        Returns:
            List of title metadata dicts
        """
        rows = self.db.execute(
            "SELECT * FROM cfr_titles ORDER BY number"
        ).fetchall()

        return [
            {
                "number": row[0],
                "name": row[1],
                "regulation_count": row[2],
                "last_updated": row[3],
                "amendment_date": row[4],
            }
            for row in rows
        ]

    def update_cfr_title_metadata(
        self,
        title_num: int,
        name: str,
        amendment_date: date | None = None,
    ) -> None:
        """Update metadata for a CFR title.

        Args:
            title_num: CFR title number
            name: Title name
            amendment_date: Date of last amendment
        """
        count = self.count_regulations(title=title_num)

        self.db["cfr_titles"].upsert(
            {
                "number": title_num,
                "name": name,
                "regulation_count": count,
                "last_updated": date.today().isoformat(),
                "amendment_date": (
                    amendment_date.isoformat() if amendment_date else None
                ),
            },
            pk="number",
        )

    def count_regulations(self, title: int | None = None) -> int:
        """Count regulations, optionally filtered by title.

        Args:
            title: Optional CFR title to filter by

        Returns:
            Number of regulations
        """
        if title is not None:
            return self.db.execute(
                "SELECT COUNT(*) FROM regulations WHERE title = ?", [title]
            ).fetchone()[0]
        else:
            return self.db.execute("SELECT COUNT(*) FROM regulations").fetchone()[0]  # pragma: no cover

    def list_regulations_in_part(
        self,
        title: int,
        part: int,
    ) -> list[Regulation]:
        """List all regulations in a CFR part.

        Args:
            title: CFR title number
            part: Part number

        Returns:
            List of Regulation objects
        """
        rows = self.db.execute(
            "SELECT * FROM regulations WHERE title = ? AND part = ? ORDER BY section",
            [title, part],
        ).fetchall()

        return [self._row_to_regulation(row) for row in rows]
