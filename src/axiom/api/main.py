"""FastAPI application for the law archive REST API."""

from datetime import date
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from axiom.archive import AxiomArchive
from axiom.models import Citation, SearchResult, Section


# Response models
class SectionResponse(BaseModel):
    """API response for a section."""

    citation: str
    title_name: str
    section_title: str
    text: str
    subsections: list[dict]
    source_url: str
    retrieved_at: date
    references_to: list[str]
    referenced_by: list[str]

    @classmethod
    def from_section(cls, section: Section) -> SectionResponse:
        return cls(
            citation=section.citation.usc_cite,
            title_name=section.title_name,
            section_title=section.section_title,
            text=section.text,
            subsections=[s.model_dump() for s in section.subsections],
            source_url=section.source_url,
            retrieved_at=section.retrieved_at,
            references_to=section.references_to,
            referenced_by=section.referenced_by,
        )


class SearchResultResponse(BaseModel):
    """API response for search results."""

    citation: str
    section_title: str
    snippet: str
    score: float

    @classmethod
    def from_result(cls, result: SearchResult) -> SearchResultResponse:
        return cls(
            citation=result.citation.usc_cite,
            section_title=result.section_title,
            snippet=result.snippet,
            score=result.score,
        )


class SearchResponse(BaseModel):
    """API response for search endpoint."""

    query: str
    total: int
    results: list[SearchResultResponse]


class TitleResponse(BaseModel):
    """API response for title info."""

    number: int
    name: str
    section_count: int
    last_updated: date
    is_positive_law: bool


class ReferencesResponse(BaseModel):
    """API response for cross-references."""

    citation: str
    references_to: list[str]
    referenced_by: list[str]


def create_app(db_path: Path | str = "axiom.db") -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        db_path: Path to SQLite database

    Returns:
        Configured FastAPI application
    """
    app = FastAPI(
        title="Axiom",
        description="Open source US statute text via API",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # CORS middleware for browser access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Initialize archive
    archive = AxiomArchive(db_path=db_path)

    @app.get("/")
    async def root():
        """API root - returns basic info."""
        return {
            "name": "Axiom",
            "version": "0.1.0",
            "docs": "/docs",
        }

    @app.get("/v1/titles", response_model=list[TitleResponse])
    async def list_titles():
        """List all available US Code titles."""
        titles = archive.list_titles()
        return [
            TitleResponse(
                number=t.number,
                name=t.name,
                section_count=t.section_count,
                last_updated=t.last_updated,
                is_positive_law=t.is_positive_law,
            )
            for t in titles
        ]

    @app.get("/v1/sections/{title}/{section}", response_model=SectionResponse)
    async def get_section(
        title: int,
        section: str,
        as_of: date | None = Query(None, description="Historical version date"),
    ):
        """Get a specific section by title and section number.

        Examples:
            - /v1/sections/26/32 - Get IRC § 32 (EITC)
            - /v1/sections/26/32?as_of=2020-01-01 - Historical version
        """
        result = archive.get(
            Citation(title=title, section=section),
            as_of=as_of,
        )
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Section {title} USC {section} not found",
            )
        return SectionResponse.from_section(result)

    @app.get("/v1/sections/{title}/{section}/{subsection:path}", response_model=SectionResponse)
    async def get_subsection(
        title: int,
        section: str,
        subsection: str,
        as_of: date | None = Query(None, description="Historical version date"),
    ):
        """Get a specific subsection.

        Examples:
            - /v1/sections/26/32/a/1 - Get IRC § 32(a)(1)
        """
        # For now, get the full section and let the client navigate
        # TODO: Return just the subsection
        result = archive.get(
            Citation(title=title, section=section, subsection=subsection),
            as_of=as_of,
        )
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Section {title} USC {section}({subsection}) not found",
            )
        return SectionResponse.from_section(result)

    @app.get("/v1/search", response_model=SearchResponse)
    async def search(
        q: str = Query(..., min_length=1, description="Search query"),
        title: int | None = Query(None, description="Limit to specific title"),
        limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    ):
        """Full-text search across sections.

        Supports FTS5 query syntax:
            - Simple terms: earned income
            - Phrases: "child tax credit"
            - Boolean: child AND credit
            - Prefix: tax*
        """
        results = archive.search(q, title=title, limit=limit)
        return SearchResponse(
            query=q,
            total=len(results),
            results=[SearchResultResponse.from_result(r) for r in results],
        )

    @app.get("/v1/references/{title}/{section}", response_model=ReferencesResponse)
    async def get_references(title: int, section: str):
        """Get cross-references for a section.

        Returns sections that this section references and sections that
        reference this section.
        """
        refs = archive.get_references(Citation(title=title, section=section))
        return ReferencesResponse(
            citation=f"{title} USC {section}",
            references_to=refs["references_to"],
            referenced_by=refs["referenced_by"],
        )

    @app.get("/v1/citation/{citation:path}", response_model=SectionResponse)
    async def get_by_citation(
        citation: str,
        as_of: date | None = Query(None, description="Historical version date"),
    ):
        """Get a section by full citation string.

        Examples:
            - /v1/citation/26 USC 32
            - /v1/citation/26 USC 32(a)(1)
        """
        try:
            parsed = Citation.from_string(citation)
        except ValueError as e:  # pragma: no cover
            raise HTTPException(status_code=400, detail=str(e)) from e  # pragma: no cover

        result = archive.get(parsed, as_of=as_of)
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Section {citation} not found",
            )
        return SectionResponse.from_section(result)

    return app


# Default app instance
app = create_app()
