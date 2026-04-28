"""Data models for IRS guidance documents (Revenue Procedures, Revenue Rulings, Notices)."""

from datetime import date
from enum import Enum

from pydantic import BaseModel, Field


class GuidanceType(str, Enum):
    """Types of IRS guidance documents."""

    REV_PROC = "revenue_procedure"
    REV_RUL = "revenue_ruling"
    NOTICE = "notice"
    ANNOUNCEMENT = "announcement"


class GuidanceSection(BaseModel):
    """A section within an IRS guidance document."""

    section_num: str = Field(..., description="Section number (e.g., '.01', '.02', '3.01')")
    heading: str | None = Field(None, description="Section heading if present")
    text: str = Field(..., description="Text content of this section")
    children: list["GuidanceSection"] = Field(
        default_factory=list, description="Child sections"
    )

    model_config = {"extra": "forbid"}


class RevenueProcedure(BaseModel):
    """An IRS Revenue Procedure document."""

    # Identifier
    doc_number: str = Field(..., description="Document number (e.g., '2023-34')")
    doc_type: GuidanceType = Field(
        default=GuidanceType.REV_PROC, description="Document type"
    )

    # Metadata
    title: str = Field(..., description="Full title of the document")
    irb_citation: str = Field(
        ..., description="Internal Revenue Bulletin citation (e.g., '2023-48 IRB')"
    )
    published_date: date = Field(..., description="Date published in IRB")

    # Content
    full_text: str = Field(..., description="Full text of the document")
    sections: list[GuidanceSection] = Field(
        default_factory=list, description="Structured sections"
    )

    # Applicability
    effective_date: date | None = Field(None, description="Effective date")
    tax_years: list[int] = Field(
        default_factory=list, description="Tax years this applies to (e.g., [2024])"
    )
    subject_areas: list[str] = Field(
        default_factory=list,
        description="Subject areas covered (e.g., ['EITC', 'Income Tax'])",
    )

    # Parameters extracted (for EITC-related Rev. Procs)
    parameters: dict[str, dict] = Field(
        default_factory=dict,
        description="Extracted parameters by variable path (e.g., {'26/32/eitc_max': {'value': 7830, 'year': 2024}})",
    )

    # Source tracking
    source_url: str = Field(..., description="URL to official source")
    pdf_url: str | None = Field(None, description="URL to PDF version if available")
    retrieved_at: date = Field(..., description="Date this version was retrieved")

    model_config = {"extra": "forbid"}

    @property
    def path(self) -> str:
        """Return filesystem-style path for axiom."""
        # e.g., "us/guidance/irs/rp-23-34"
        year, num = self.doc_number.split("-")
        return f"us/guidance/irs/rp-{year}-{num}"


class GuidanceSearchResult(BaseModel):
    """A search result for guidance documents."""

    doc_number: str
    doc_type: GuidanceType
    title: str
    snippet: str = Field(..., description="Relevant text snippet with highlights")
    score: float = Field(..., description="Relevance score (0-1)")
    published_date: date

    model_config = {"extra": "forbid"}
