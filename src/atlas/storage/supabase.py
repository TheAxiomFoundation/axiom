"""Supabase storage backend for statute archive.

Stores statutes in PostgreSQL via Supabase for production use.
Supports full-text search via tsvector/tsquery.

Connection URL format:
postgresql://postgres:[PASSWORD]@db.[PROJECT-ID].supabase.co:5432/postgres

Environment variable: COSILICO_SUPABASE_DB_URL
"""

import json
import os
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from atlas.models_statute import (
    JurisdictionInfo,
    Statute,
    StatuteSearchResult,
    StatuteSubsection,
)

# Optional dependency - only needed for Supabase storage
try:
    import psycopg
    from psycopg.rows import dict_row

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False


def get_db_url() -> str:
    """Get database URL from environment."""
    url = os.environ.get("COSILICO_SUPABASE_DB_URL")
    if not url:
        raise ValueError(
            "COSILICO_SUPABASE_DB_URL environment variable not set. "
            "Set it to your Supabase database URL."
        )
    return url


class SupabaseStorage:
    """Supabase/PostgreSQL storage backend for statutes.

    Schema:
    - arch.statutes: Main statute storage with tsvector for FTS
    - arch.cross_references: Citation cross-reference graph
    - arch.jurisdictions: Jurisdiction metadata
    """

    def __init__(self, db_url: str | None = None):
        """Initialize Supabase storage.

        Args:
            db_url: PostgreSQL connection URL. If None, reads from
                    COSILICO_SUPABASE_DB_URL environment variable.
        """
        if not HAS_PSYCOPG:
            raise ImportError(
                "psycopg is required for Supabase storage. "
                "Install with: pip install psycopg[binary]"
            )

        self.db_url = db_url or get_db_url()
        self._conn = None

    @property
    def conn(self):
        """Lazy-initialize database connection."""
        if self._conn is None:
            self._conn = psycopg.connect(self.db_url, row_factory=dict_row)
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()

    def _init_schema(self):
        """Create schema if it doesn't exist.

        Note: This is usually done via migrations.
        This method is for convenience/testing.
        """
        with self.conn.cursor() as cur:
            # Create schema
            cur.execute("CREATE SCHEMA IF NOT EXISTS arch")

            # Main statutes table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS arch.statutes (
                    id SERIAL PRIMARY KEY,
                    jurisdiction TEXT NOT NULL,
                    code TEXT NOT NULL,
                    code_name TEXT NOT NULL,
                    section TEXT NOT NULL,
                    subsection_path TEXT,
                    title TEXT NOT NULL,
                    text TEXT NOT NULL,
                    subsections_json JSONB DEFAULT '[]',
                    division TEXT,
                    part TEXT,
                    chapter TEXT,
                    subchapter TEXT,
                    article TEXT,
                    history TEXT,
                    enacted_date DATE,
                    last_amended DATE,
                    effective_date DATE,
                    public_laws JSONB DEFAULT '[]',
                    references_to JSONB DEFAULT '[]',
                    referenced_by JSONB DEFAULT '[]',
                    source_url TEXT NOT NULL,
                    source_id TEXT,
                    retrieved_at TIMESTAMPTZ DEFAULT NOW(),
                    fts_vector TSVECTOR,
                    UNIQUE(jurisdiction, code, section, subsection_path)
                )
            """)

            # Full-text search index
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_statutes_fts
                ON arch.statutes USING GIN(fts_vector)
            """)

            # Indexes for common queries
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_statutes_jurisdiction
                ON arch.statutes(jurisdiction)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_statutes_code
                ON arch.statutes(jurisdiction, code)
            """)

            # Trigger to update FTS vector
            cur.execute("""
                CREATE OR REPLACE FUNCTION arch.update_fts_vector()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.fts_vector := to_tsvector('english',
                        COALESCE(NEW.title, '') || ' ' ||
                        COALESCE(NEW.text, '')
                    );
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
            """)
            cur.execute("""
                DROP TRIGGER IF EXISTS trg_statutes_fts ON arch.statutes
            """)
            cur.execute("""
                CREATE TRIGGER trg_statutes_fts
                BEFORE INSERT OR UPDATE ON arch.statutes
                FOR EACH ROW EXECUTE FUNCTION arch.update_fts_vector()
            """)

            # Cross-references table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS arch.cross_references (
                    from_jurisdiction TEXT NOT NULL,
                    from_code TEXT NOT NULL,
                    from_section TEXT NOT NULL,
                    to_jurisdiction TEXT NOT NULL,
                    to_code TEXT NOT NULL,
                    to_section TEXT NOT NULL,
                    PRIMARY KEY (from_jurisdiction, from_code, from_section,
                                 to_jurisdiction, to_code, to_section)
                )
            """)

            # Jurisdictions metadata
            cur.execute("""
                CREATE TABLE IF NOT EXISTS arch.jurisdictions (
                    jurisdiction TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    codes JSONB DEFAULT '{}',
                    section_count INTEGER DEFAULT 0,
                    last_updated TIMESTAMPTZ
                )
            """)

            self.conn.commit()

    def store_statute(self, statute: Statute) -> None:
        """Store a statute in the database."""
        with self.conn.cursor() as cur:
            subsections_json = json.dumps(
                [self._subsection_to_dict(s) for s in statute.subsections]
            )

            cur.execute(
                """
                INSERT INTO arch.statutes (
                    jurisdiction, code, code_name, section, subsection_path,
                    title, text, subsections_json,
                    division, part, chapter, subchapter, article,
                    history, enacted_date, last_amended, effective_date,
                    public_laws, references_to, referenced_by,
                    source_url, source_id, retrieved_at
                ) VALUES (
                    %(jurisdiction)s, %(code)s, %(code_name)s, %(section)s, %(subsection_path)s,
                    %(title)s, %(text)s, %(subsections_json)s,
                    %(division)s, %(part)s, %(chapter)s, %(subchapter)s, %(article)s,
                    %(history)s, %(enacted_date)s, %(last_amended)s, %(effective_date)s,
                    %(public_laws)s, %(references_to)s, %(referenced_by)s,
                    %(source_url)s, %(source_id)s, %(retrieved_at)s
                )
                ON CONFLICT (jurisdiction, code, section, subsection_path)
                DO UPDATE SET
                    code_name = EXCLUDED.code_name,
                    title = EXCLUDED.title,
                    text = EXCLUDED.text,
                    subsections_json = EXCLUDED.subsections_json,
                    division = EXCLUDED.division,
                    part = EXCLUDED.part,
                    chapter = EXCLUDED.chapter,
                    subchapter = EXCLUDED.subchapter,
                    article = EXCLUDED.article,
                    history = EXCLUDED.history,
                    enacted_date = EXCLUDED.enacted_date,
                    last_amended = EXCLUDED.last_amended,
                    effective_date = EXCLUDED.effective_date,
                    public_laws = EXCLUDED.public_laws,
                    references_to = EXCLUDED.references_to,
                    referenced_by = EXCLUDED.referenced_by,
                    source_url = EXCLUDED.source_url,
                    source_id = EXCLUDED.source_id,
                    retrieved_at = EXCLUDED.retrieved_at
                """,
                {
                    "jurisdiction": statute.jurisdiction,
                    "code": statute.code,
                    "code_name": statute.code_name,
                    "section": statute.section,
                    "subsection_path": statute.subsection_path,
                    "title": statute.title,
                    "text": statute.text,
                    "subsections_json": subsections_json,
                    "division": statute.division,
                    "part": statute.part,
                    "chapter": statute.chapter,
                    "subchapter": statute.subchapter,
                    "article": statute.article,
                    "history": statute.history,
                    "enacted_date": statute.enacted_date,
                    "last_amended": statute.last_amended,
                    "effective_date": statute.effective_date,
                    "public_laws": json.dumps(statute.public_laws),
                    "references_to": json.dumps(statute.references_to),
                    "referenced_by": json.dumps(statute.referenced_by),
                    "source_url": statute.source_url,
                    "source_id": statute.source_id,
                    "retrieved_at": statute.retrieved_at,
                },
            )
            self.conn.commit()

    def _subsection_to_dict(self, sub: StatuteSubsection) -> dict:
        """Convert subsection to JSON-serializable dict."""
        return {
            "identifier": sub.identifier,
            "heading": sub.heading,
            "text": sub.text,
            "children": [self._subsection_to_dict(c) for c in sub.children],
        }

    def _dict_to_subsection(self, d: dict) -> StatuteSubsection:
        """Convert dict to subsection."""
        return StatuteSubsection(
            identifier=d["identifier"],
            heading=d.get("heading"),
            text=d["text"],
            children=[self._dict_to_subsection(c) for c in d.get("children", [])],
        )

    def get_statute(
        self,
        jurisdiction: str,
        code: str,
        section: str,
        subsection_path: str | None = None,
    ) -> Statute | None:
        """Retrieve a statute by citation."""
        with self.conn.cursor() as cur:
            if subsection_path:
                cur.execute(
                    """
                    SELECT * FROM arch.statutes
                    WHERE jurisdiction = %s AND code = %s AND section = %s
                    AND subsection_path = %s
                    """,
                    [jurisdiction, code, section, subsection_path],
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM arch.statutes
                    WHERE jurisdiction = %s AND code = %s AND section = %s
                    AND subsection_path IS NULL
                    """,
                    [jurisdiction, code, section],
                )

            row = cur.fetchone()
            if not row:
                return None

            return self._row_to_statute(row)

    def _row_to_statute(self, row: dict) -> Statute:
        """Convert database row to Statute model."""
        subsections = [self._dict_to_subsection(d) for d in (row["subsections_json"] or [])]

        return Statute(
            jurisdiction=row["jurisdiction"],
            code=row["code"],
            code_name=row["code_name"],
            section=row["section"],
            subsection_path=row["subsection_path"],
            title=row["title"],
            text=row["text"],
            subsections=subsections,
            division=row["division"],
            part=row["part"],
            chapter=row["chapter"],
            subchapter=row["subchapter"],
            article=row["article"],
            history=row["history"],
            enacted_date=row["enacted_date"],
            last_amended=row["last_amended"],
            effective_date=row["effective_date"],
            public_laws=row["public_laws"] or [],
            references_to=row["references_to"] or [],
            referenced_by=row["referenced_by"] or [],
            source_url=row["source_url"],
            source_id=row["source_id"],
            retrieved_at=row["retrieved_at"] or datetime.utcnow(),
        )

    def search(
        self,
        query: str,
        jurisdiction: str | None = None,
        code: str | None = None,
        limit: int = 20,
    ) -> list[StatuteSearchResult]:
        """Full-text search across statutes."""
        with self.conn.cursor() as cur:
            # Build query
            sql = """
                SELECT
                    jurisdiction, code, section, title,
                    ts_headline('english', text, plainto_tsquery('english', %s),
                               'MaxWords=50, MinWords=20, StartSel=<mark>, StopSel=</mark>') as snippet,
                    ts_rank(fts_vector, plainto_tsquery('english', %s)) as score
                FROM arch.statutes
                WHERE fts_vector @@ plainto_tsquery('english', %s)
            """
            params = [query, query, query]

            if jurisdiction:
                sql += " AND jurisdiction = %s"
                params.append(jurisdiction)
            if code:
                sql += " AND code = %s"
                params.append(code)

            sql += " ORDER BY score DESC LIMIT %s"
            params.append(limit)

            cur.execute(sql, params)
            rows = cur.fetchall()

        results = []
        for row in rows:
            # Construct RuleSpec path
            rulespec_path = (
                f"rules-{row['jurisdiction']}/statute/{row['code']}/{row['section']}.yaml"
            )

            results.append(
                StatuteSearchResult(
                    jurisdiction=row["jurisdiction"],
                    code=row["code"],
                    section=row["section"],
                    title=row["title"],
                    snippet=row["snippet"],
                    score=float(row["score"]),
                    rulespec_path=rulespec_path,
                )
            )

        return results

    def list_jurisdictions(self) -> list[JurisdictionInfo]:
        """List all jurisdictions with metadata."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    j.jurisdiction,
                    j.name,
                    j.type,
                    j.codes,
                    COALESCE(j.section_count, 0) as section_count,
                    j.last_updated
                FROM arch.jurisdictions j
                ORDER BY j.jurisdiction
            """)
            rows = cur.fetchall()

        from atlas.models_statute import JurisdictionType

        return [
            JurisdictionInfo(
                jurisdiction=row["jurisdiction"],
                name=row["name"],
                type=JurisdictionType(row["type"]),
                codes=[{"id": k, "name": v} for k, v in (row["codes"] or {}).items()],
                section_count=row["section_count"],
                last_updated=row["last_updated"],
            )
            for row in rows
        ]

    def update_jurisdiction_stats(self, jurisdiction: str) -> None:
        """Update section count for a jurisdiction."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE arch.jurisdictions
                SET
                    section_count = (
                        SELECT COUNT(*) FROM arch.statutes
                        WHERE jurisdiction = %s
                    ),
                    last_updated = NOW()
                WHERE jurisdiction = %s
                """,
                [jurisdiction, jurisdiction],
            )
            self.conn.commit()

    def get_section_count(self, jurisdiction: str | None = None) -> int:
        """Get total section count, optionally filtered by jurisdiction."""
        with self.conn.cursor() as cur:
            if jurisdiction:
                cur.execute(
                    "SELECT COUNT(*) as count FROM arch.statutes WHERE jurisdiction = %s",
                    [jurisdiction],
                )
            else:
                cur.execute("SELECT COUNT(*) as count FROM arch.statutes")

            row = cur.fetchone()
            return row["count"] if row else 0

    def batch_store(self, statutes: Iterator[Statute], batch_size: int = 100) -> int:
        """Store multiple statutes in batches for efficiency."""
        count = 0
        batch = []

        for statute in statutes:
            batch.append(statute)
            if len(batch) >= batch_size:
                for s in batch:
                    self.store_statute(s)
                count += len(batch)
                batch = []

        # Store remaining
        for s in batch:
            self.store_statute(s)
        count += len(batch)

        return count
