"""Storage backend extension for IRS guidance documents."""

import json
from datetime import date
from pathlib import Path

import sqlite_utils

from axiom.models_guidance import (
    GuidanceSearchResult,
    GuidanceSection,
    GuidanceType,
    RevenueProcedure,
)


class GuidanceStorage:
    """SQLite storage extension for IRS guidance documents.

    This extends the base SQLiteStorage to handle Rev. Procs, Rev. Rulings, etc.
    """

    def __init__(self, db_path: Path | str = "axiom.db"):
        """Initialize guidance storage.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db = sqlite_utils.Database(str(self.db_path))

    def store_revenue_procedure(self, rev_proc: RevenueProcedure) -> None:
        """Store a Revenue Procedure (or other guidance document) in the database."""
        # Serialize sections to JSON
        sections_json = json.dumps([self._section_to_dict(s) for s in rev_proc.sections])

        # Generate ID based on document type (e.g., "rp-2023-34", "rr-2023-12", "notice-2024-45")
        prefix_map = {
            GuidanceType.REV_PROC: "rp",
            GuidanceType.REV_RUL: "rr",
            GuidanceType.NOTICE: "notice",
            GuidanceType.ANNOUNCEMENT: "announce",
        }
        year, num = rev_proc.doc_number.split("-")
        prefix = prefix_map.get(rev_proc.doc_type, "doc")
        doc_id = f"{prefix}-{year}-{num}"

        record = {
            "id": doc_id,
            "doc_type": rev_proc.doc_type.value,
            "doc_number": rev_proc.doc_number,
            "title": rev_proc.title,
            "irb_citation": rev_proc.irb_citation,
            "published_date": rev_proc.published_date.isoformat(),
            "full_text": rev_proc.full_text,
            "sections_json": sections_json,
            "effective_date": (
                rev_proc.effective_date.isoformat() if rev_proc.effective_date else None
            ),
            "tax_years_json": json.dumps(rev_proc.tax_years),
            "subject_areas_json": json.dumps(rev_proc.subject_areas),
            "parameters_json": json.dumps(rev_proc.parameters),
            "source_url": rev_proc.source_url,
            "pdf_url": rev_proc.pdf_url,
            "retrieved_at": rev_proc.retrieved_at.isoformat(),
        }

        # Use insert_all with replace=True for better control
        self.db["guidance_documents"].insert(record, replace=True)
        # Ensure changes are committed
        self.db.conn.commit()

    def get_revenue_procedure(self, doc_number: str) -> RevenueProcedure | None:
        """Retrieve a Revenue Procedure by document number.

        Args:
            doc_number: Document number like "2023-34"

        Returns:
            RevenueProcedure object or None if not found
        """
        year, num = doc_number.split("-")
        doc_id = f"rp-{year}-{num}"

        row = self.db.execute(
            "SELECT * FROM guidance_documents WHERE id = ?", [doc_id]
        ).fetchone()

        if not row:
            return None

        return self._row_to_revenue_procedure(row)

    def search_guidance(
        self,
        query: str,
        doc_type: GuidanceType | None = None,
        limit: int = 20,
    ) -> list[GuidanceSearchResult]:
        """Full-text search across guidance documents.

        Args:
            query: Search query (supports FTS5 syntax)
            doc_type: Optional filter by document type
            limit: Maximum results to return

        Returns:
            List of GuidanceSearchResult objects
        """
        if doc_type:
            sql = """  # pragma: no cover
                SELECT g.doc_number, g.doc_type, g.title, g.published_date,
                       snippet(guidance_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
                       bm25(guidance_fts) as score
                FROM guidance_fts
                JOIN guidance_documents g ON guidance_fts.rowid = g.rowid
                WHERE guidance_fts MATCH ? AND g.doc_type = ?
                ORDER BY score
                LIMIT ?
            """
            rows = self.db.execute(sql, [query, doc_type.value, limit]).fetchall()  # pragma: no cover
        else:
            sql = """
                SELECT g.doc_number, g.doc_type, g.title, g.published_date,
                       snippet(guidance_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
                       bm25(guidance_fts) as score
                FROM guidance_fts
                JOIN guidance_documents g ON guidance_fts.rowid = g.rowid
                WHERE guidance_fts MATCH ?
                ORDER BY score
                LIMIT ?
            """
            rows = self.db.execute(sql, [query, limit]).fetchall()

        results = []
        for row in rows:
            doc_number, doc_type_str, title, published_date_str, snippet, score = row  # pragma: no cover
            results.append(  # pragma: no cover
                GuidanceSearchResult(
                    doc_number=doc_number,
                    doc_type=GuidanceType(doc_type_str),
                    title=title,
                    snippet=snippet,
                    score=abs(score),
                    published_date=date.fromisoformat(published_date_str),
                )
            )

        return results

    def get_guidance_for_statute(
        self, title: int, section: str
    ) -> list[RevenueProcedure]:
        """Get all guidance documents that reference a specific statute section.

        Args:
            title: Title number (e.g., 26 for IRC)
            section: Section number (e.g., "32" for EITC)

        Returns:
            List of guidance documents
        """
        rows = self.db.execute(
            """
            SELECT DISTINCT g.*
            FROM guidance_documents g
            JOIN guidance_statute_refs r ON r.guidance_id = g.id
            WHERE r.statute_title = ? AND r.statute_section = ?
            ORDER BY g.published_date DESC
            """,
            [title, section],
        ).fetchall()

        return [self._row_to_revenue_procedure(row) for row in rows]

    def link_guidance_to_statute(
        self,
        doc_id: str,
        statute_title: int,
        statute_section: str,
        ref_type: str = "implements",
        excerpt: str | None = None,
    ) -> None:
        """Create a link between a guidance document and a statute section.

        Args:
            doc_id: Guidance document ID (e.g., "rp-2023-34")
            statute_title: Title number (e.g., 26)
            statute_section: Section number (e.g., "32")
            ref_type: Type of reference ('implements', 'interprets', 'modifies', 'cites')
            excerpt: Optional excerpt from the guidance
        """
        self.db["guidance_statute_refs"].insert(
            {
                "guidance_id": doc_id,
                "statute_title": statute_title,
                "statute_section": statute_section,
                "ref_type": ref_type,
                "excerpt": excerpt,
            },
            ignore=True,
        )

    def _section_to_dict(self, section: GuidanceSection) -> dict:
        """Convert GuidanceSection to dictionary for JSON serialization."""
        return {
            "section_num": section.section_num,
            "heading": section.heading,
            "text": section.text,
            "children": [self._section_to_dict(c) for c in section.children],
        }

    def _dict_to_section(self, d: dict) -> GuidanceSection:
        """Convert dictionary to GuidanceSection."""
        return GuidanceSection(
            section_num=d["section_num"],
            heading=d.get("heading"),
            text=d["text"],
            children=[self._dict_to_section(c) for c in d.get("children", [])],
        )

    def _row_to_revenue_procedure(self, row: tuple) -> RevenueProcedure:
        """Convert a database row to a RevenueProcedure model."""
        # Get column names
        cursor = self.db.execute("SELECT * FROM guidance_documents LIMIT 0")
        columns = [desc[0] for desc in cursor.description]
        record = dict(zip(columns, row, strict=True))

        sections = [
            self._dict_to_section(d) for d in json.loads(record["sections_json"] or "[]")
        ]

        return RevenueProcedure(
            doc_number=record["doc_number"],
            doc_type=GuidanceType(record["doc_type"]),
            title=record["title"],
            irb_citation=record["irb_citation"],
            published_date=date.fromisoformat(record["published_date"]),
            full_text=record["full_text"],
            sections=sections,
            effective_date=(
                date.fromisoformat(record["effective_date"])
                if record["effective_date"]
                else None
            ),
            tax_years=json.loads(record["tax_years_json"] or "[]"),
            subject_areas=json.loads(record["subject_areas_json"] or "[]"),
            parameters=json.loads(record["parameters_json"] or "{}"),
            source_url=record["source_url"],
            pdf_url=record.get("pdf_url"),
            retrieved_at=date.fromisoformat(record["retrieved_at"]),
        )
