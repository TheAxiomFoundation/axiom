"""PostgreSQL storage backend for production scale.

Designed for Supabase or any PostgreSQL instance. Handles millions of
sections with full-text search using pg_trgm and GIN indexes.
"""

import json
import os
from datetime import date

from axiom_corpus.models import Citation, SearchResult, Section, Subsection, TitleInfo
from axiom_corpus.storage.base import StorageBackend

# Lazy import - only load if postgres extras installed
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import declarative_base, sessionmaker

    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


Base = declarative_base() if POSTGRES_AVAILABLE else None


def get_engine(database_url: str | None = None):
    """Create SQLAlchemy engine from URL or environment."""
    if not POSTGRES_AVAILABLE:
        raise ImportError("PostgreSQL support requires: pip install axiom[postgres]")

    url = database_url or os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise ValueError(
            "No database URL. Set DATABASE_URL or SUPABASE_DB_URL environment variable."
        )

    return create_engine(url)


class PostgresStorage(StorageBackend):
    """PostgreSQL-based storage with full-text search.

    Designed for scale:
    - 2M+ sections across federal/state statutes, regs, guidance
    - Full-text search with pg_trgm trigram matching
    - GIN indexes for fast lookups
    - Connection pooling for high concurrency
    """

    def __init__(self, database_url: str | None = None):
        """Initialize PostgreSQL storage.

        Args:
            database_url: PostgreSQL connection URL. If not provided, reads from
                         DATABASE_URL or SUPABASE_DB_URL environment variable.
        """
        self.engine = get_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tables and indexes if they don't exist."""
        with self.engine.connect() as conn:
            # Enable pg_trgm extension for fuzzy text search
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))

            # Main sections table
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS sections (
                    id TEXT PRIMARY KEY,
                    title INTEGER NOT NULL,
                    section TEXT NOT NULL,
                    jurisdiction TEXT DEFAULT 'federal',
                    doc_type TEXT DEFAULT 'statute',
                    title_name TEXT,
                    section_title TEXT,
                    text TEXT,
                    subsections JSONB,
                    enacted_date DATE,
                    last_amended DATE,
                    public_laws JSONB,
                    effective_date DATE,
                    references_to JSONB,
                    referenced_by JSONB,
                    source_url TEXT,
                    retrieved_at DATE,

                    UNIQUE(title, section, jurisdiction, doc_type)
                )
            """)
            )

            # Indexes for common queries
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_sections_title
                ON sections(title)
            """)
            )
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_sections_jurisdiction
                ON sections(jurisdiction)
            """)
            )
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_sections_doc_type
                ON sections(doc_type)
            """)
            )

            # GIN index for full-text search on section_title and text
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_sections_text_search
                ON sections USING GIN (
                    (to_tsvector('english', COALESCE(section_title, '') || ' ' || COALESCE(text, '')))
                )
            """)
            )

            # Trigram index for fuzzy matching
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_sections_title_trgm
                ON sections USING GIN (section_title gin_trgm_ops)
            """)
            )

            # Cross-references table
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS cross_references (
                    from_id TEXT REFERENCES sections(id),
                    to_title INTEGER,
                    to_section TEXT,
                    PRIMARY KEY (from_id, to_title, to_section)
                )
            """)
            )
            conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_xref_to
                ON cross_references(to_title, to_section)
            """)
            )

            # Title metadata
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS titles (
                    number INTEGER,
                    jurisdiction TEXT DEFAULT 'federal',
                    name TEXT,
                    section_count INTEGER,
                    last_updated DATE,
                    is_positive_law BOOLEAN,
                    PRIMARY KEY (number, jurisdiction)
                )
            """)
            )

            conn.commit()

    def store_section(
        self, section: Section, jurisdiction: str = "federal", doc_type: str = "statute"
    ) -> None:
        """Store a section in the database."""
        section_id = (
            section.uslm_id
            or f"{jurisdiction}/{doc_type}/{section.citation.title}/{section.citation.section}"
        )

        with self.Session() as session:
            # Upsert section
            session.execute(
                text("""
                INSERT INTO sections (
                    id, title, section, jurisdiction, doc_type,
                    title_name, section_title, text, subsections,
                    enacted_date, last_amended, public_laws, effective_date,
                    references_to, referenced_by, source_url, retrieved_at
                ) VALUES (
                    :id, :title, :section, :jurisdiction, :doc_type,
                    :title_name, :section_title, :text, :subsections,
                    :enacted_date, :last_amended, :public_laws, :effective_date,
                    :references_to, :referenced_by, :source_url, :retrieved_at
                )
                ON CONFLICT (id) DO UPDATE SET
                    title_name = EXCLUDED.title_name,
                    section_title = EXCLUDED.section_title,
                    text = EXCLUDED.text,
                    subsections = EXCLUDED.subsections,
                    enacted_date = EXCLUDED.enacted_date,
                    last_amended = EXCLUDED.last_amended,
                    public_laws = EXCLUDED.public_laws,
                    effective_date = EXCLUDED.effective_date,
                    references_to = EXCLUDED.references_to,
                    referenced_by = EXCLUDED.referenced_by,
                    source_url = EXCLUDED.source_url,
                    retrieved_at = EXCLUDED.retrieved_at
            """),
                {
                    "id": section_id,
                    "title": section.citation.title,
                    "section": section.citation.section,
                    "jurisdiction": jurisdiction,
                    "doc_type": doc_type,
                    "title_name": section.title_name,
                    "section_title": section.section_title,
                    "text": section.text,
                    "subsections": json.dumps(
                        [self._subsection_to_dict(s) for s in section.subsections]
                    ),
                    "enacted_date": section.enacted_date,
                    "last_amended": section.last_amended,
                    "public_laws": json.dumps(section.public_laws),
                    "effective_date": section.effective_date,
                    "references_to": json.dumps(section.references_to),
                    "referenced_by": json.dumps(section.referenced_by),
                    "source_url": section.source_url,
                    "retrieved_at": section.retrieved_at,
                },
            )

            # Update cross-references
            session.execute(
                text("DELETE FROM cross_references WHERE from_id = :id"), {"id": section_id}
            )
            for ref in section.references_to:
                try:
                    ref_citation = Citation.from_string(ref)
                    session.execute(
                        text("""
                        INSERT INTO cross_references (from_id, to_title, to_section)
                        VALUES (:from_id, :to_title, :to_section)
                        ON CONFLICT DO NOTHING
                    """),
                        {
                            "from_id": section_id,
                            "to_title": ref_citation.title,
                            "to_section": ref_citation.section,
                        },
                    )
                except ValueError:
                    pass

            session.commit()

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

    def get_section(
        self,
        title: int,
        section: str,
        subsection: str | None = None,
        as_of: date | None = None,
        jurisdiction: str = "federal",
    ) -> Section | None:
        """Retrieve a section by citation."""
        with self.Session() as session:
            result = session.execute(
                text("""
                SELECT * FROM sections
                WHERE title = :title AND section = :section AND jurisdiction = :jurisdiction
                LIMIT 1
            """),
                {"title": title, "section": section, "jurisdiction": jurisdiction},
            ).fetchone()

            if not result:
                return None

            return self._row_to_section(result._mapping)

    def _row_to_section(self, row: dict) -> Section:
        """Convert a database row to a Section model."""
        subsections = [
            self._dict_to_subsection(d)
            for d in (json.loads(row["subsections"]) if row["subsections"] else [])
        ]

        return Section(
            citation=Citation(title=row["title"], section=row["section"]),
            title_name=row["title_name"] or "",
            section_title=row["section_title"] or "",
            text=row["text"] or "",
            subsections=subsections,
            enacted_date=row["enacted_date"],
            last_amended=row["last_amended"],
            public_laws=json.loads(row["public_laws"]) if row["public_laws"] else [],
            effective_date=row["effective_date"],
            references_to=json.loads(row["references_to"]) if row["references_to"] else [],
            referenced_by=json.loads(row["referenced_by"]) if row["referenced_by"] else [],
            source_url=row["source_url"] or "",
            retrieved_at=row["retrieved_at"] or date.today(),
            uslm_id=row["id"],
        )

    def search(
        self,
        query: str,
        title: int | None = None,
        jurisdiction: str | None = None,
        doc_type: str | None = None,
        limit: int = 20,
    ) -> list[SearchResult]:
        """Full-text search across sections using PostgreSQL FTS."""
        with self.Session() as session:
            # Build dynamic WHERE clause
            conditions = ["TRUE"]
            params = {"query": query, "limit": limit}

            if title is not None:
                conditions.append("title = :title")
                params["title"] = title
            if jurisdiction:
                conditions.append("jurisdiction = :jurisdiction")
                params["jurisdiction"] = jurisdiction
            if doc_type:
                conditions.append("doc_type = :doc_type")
                params["doc_type"] = doc_type

            where_clause = " AND ".join(conditions)

            # Use ts_rank for relevance scoring
            results = session.execute(
                text(f"""
                SELECT
                    title, section, section_title,
                    ts_headline('english', text, plainto_tsquery('english', :query),
                        'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') as snippet,
                    ts_rank(
                        to_tsvector('english', COALESCE(section_title, '') || ' ' || COALESCE(text, '')),
                        plainto_tsquery('english', :query)
                    ) as score
                FROM sections
                WHERE {where_clause}
                    AND to_tsvector('english', COALESCE(section_title, '') || ' ' || COALESCE(text, ''))
                        @@ plainto_tsquery('english', :query)
                ORDER BY score DESC
                LIMIT :limit
            """),
                params,
            ).fetchall()

            return [
                SearchResult(
                    citation=Citation(title=row.title, section=row.section),
                    section_title=row.section_title or "",
                    snippet=row.snippet or "",
                    score=float(row.score),
                )
                for row in results
            ]

    def list_titles(self, jurisdiction: str = "federal") -> list[TitleInfo]:
        """List all available titles with metadata."""
        with self.Session() as session:
            results = session.execute(
                text("""
                SELECT * FROM titles
                WHERE jurisdiction = :jurisdiction
                ORDER BY number
            """),
                {"jurisdiction": jurisdiction},
            ).fetchall()

            return [
                TitleInfo(
                    number=row.number,
                    name=row.name or f"Title {row.number}",
                    section_count=row.section_count or 0,
                    last_updated=row.last_updated or date.today(),
                    is_positive_law=bool(row.is_positive_law),
                )
                for row in results
            ]

    def get_references_to(self, title: int, section: str) -> list[str]:
        """Get sections that this section references."""
        with self.Session() as session:
            results = session.execute(
                text("""
                SELECT to_title, to_section FROM cross_references cr
                JOIN sections s ON cr.from_id = s.id
                WHERE s.title = :title AND s.section = :section
            """),
                {"title": title, "section": section},
            ).fetchall()

            return [f"{row.to_title} USC {row.to_section}" for row in results]

    def get_referenced_by(self, title: int, section: str) -> list[str]:
        """Get sections that reference this section."""
        with self.Session() as session:
            results = session.execute(
                text("""
                SELECT s.title, s.section FROM cross_references cr
                JOIN sections s ON cr.from_id = s.id
                WHERE cr.to_title = :title AND cr.to_section = :section
            """),
                {"title": title, "section": section},
            ).fetchall()

            return [f"{row.title} USC {row.section}" for row in results]

    def update_title_metadata(
        self, title_num: int, name: str, is_positive_law: bool, jurisdiction: str = "federal"
    ) -> None:
        """Update metadata for a title."""
        with self.Session() as session:
            # Count sections
            count = session.execute(
                text("""
                SELECT COUNT(*) FROM sections
                WHERE title = :title AND jurisdiction = :jurisdiction
            """),
                {"title": title_num, "jurisdiction": jurisdiction},
            ).scalar()

            session.execute(
                text("""
                INSERT INTO titles (number, jurisdiction, name, section_count, last_updated, is_positive_law)
                VALUES (:number, :jurisdiction, :name, :count, :updated, :positive_law)
                ON CONFLICT (number, jurisdiction) DO UPDATE SET
                    name = EXCLUDED.name,
                    section_count = EXCLUDED.section_count,
                    last_updated = EXCLUDED.last_updated,
                    is_positive_law = EXCLUDED.is_positive_law
            """),
                {
                    "number": title_num,
                    "jurisdiction": jurisdiction,
                    "name": name,
                    "count": count,
                    "updated": date.today(),
                    "positive_law": is_positive_law,
                },
            )
            session.commit()

    def get_stats(self) -> dict:
        """Get database statistics."""
        with self.Session() as session:
            stats = {}

            # Total sections by type
            results = session.execute(
                text("""
                SELECT jurisdiction, doc_type, COUNT(*) as count
                FROM sections
                GROUP BY jurisdiction, doc_type
            """)
            ).fetchall()

            stats["sections_by_type"] = {f"{r.jurisdiction}/{r.doc_type}": r.count for r in results}

            # Total sections
            stats["total_sections"] = session.execute(
                text("SELECT COUNT(*) FROM sections")
            ).scalar()

            # Database size (PostgreSQL specific)
            try:
                stats["db_size"] = session.execute(
                    text("SELECT pg_size_pretty(pg_database_size(current_database()))")
                ).scalar()
            except Exception:
                stats["db_size"] = "unknown"

            return stats
