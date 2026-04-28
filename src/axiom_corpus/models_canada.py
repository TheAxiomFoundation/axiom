"""Data models for Canadian federal statutes.

Canada uses a LIMS (Legal Information Management System) XML format
published by the Department of Justice at laws-lois.justice.gc.ca.

Key differences from US Code:
- Consolidated numbers (e.g., I-3.3 for Income Tax Act)
- French/English bilingual support
- Different hierarchy: Section → Subsection → Paragraph → Subparagraph → Clause
"""

from datetime import date

from pydantic import BaseModel, Field


class CanadaCitation(BaseModel):
    """Citation for a Canadian federal statute."""

    consolidated_number: str = Field(..., description="Consolidated number (e.g., 'I-3.3')")
    section: str | None = Field(None, description="Section number (e.g., '32')")
    subsection: str | None = Field(None, description="Subsection (e.g., '1')")
    paragraph: str | None = Field(None, description="Paragraph (e.g., 'a')")

    @property
    def short_cite(self) -> str:
        """Return short citation format."""
        base = self.consolidated_number
        if self.section:
            base = f"{base}, s. {self.section}"
            if self.subsection:
                base = f"{base}({self.subsection})"
            if self.paragraph:
                base = f"{base}({self.paragraph})"
        return base

    @property
    def path(self) -> str:
        """Return filesystem path for storage."""
        parts = ["canada", self.consolidated_number]
        if self.section:
            parts.append(self.section)
            if self.subsection:
                parts.append(self.subsection)
                if self.paragraph:
                    parts.append(self.paragraph)
        return "/".join(parts)


class CanadaSubsection(BaseModel):
    """A subsection/paragraph/clause within a Canadian statute section."""

    label: str = Field(..., description="Label (e.g., '(1)', '(a)', '(i)')")
    marginal_note: str | None = Field(None, description="Marginal note if present")
    text: str = Field(..., description="Text content")
    children: list["CanadaSubsection"] = Field(default_factory=list)
    level: str = Field(
        "subsection",
        description="Level: subsection, paragraph, subparagraph, clause",
    )


class CanadaSection(BaseModel):
    """A complete section of a Canadian statute."""

    citation: CanadaCitation
    section_number: str = Field(..., description="Section number (e.g., '2')")
    marginal_note: str = Field(..., description="Section title/marginal note")
    text: str = Field(..., description="Full text of the section")
    subsections: list[CanadaSubsection] = Field(default_factory=list)

    # Temporal
    in_force_date: date | None = Field(None, description="When section came into force")
    last_amended_date: date | None = Field(None, description="Last amendment date")

    # Historical notes
    historical_notes: list[str] = Field(
        default_factory=list, description="Amendment history"
    )

    # Cross-references
    references_to: list[str] = Field(
        default_factory=list, description="Acts/sections referenced"
    )

    # Provenance
    source_url: str = Field(..., description="URL to laws-lois.justice.gc.ca")
    source_path: str | None = Field(None, description="Local file path in Axiom")
    lims_id: str | None = Field(None, description="LIMS ID from XML")


class CanadaAct(BaseModel):
    """Metadata for a Canadian federal act."""

    citation: CanadaCitation
    short_title: str = Field(..., description="Official short title")
    long_title: str = Field(..., description="Full long title")
    consolidated_number: str = Field(..., description="e.g., 'I-3.3'")

    # Bill info
    bill_origin: str | None = Field(None, description="commons or senate")
    bill_type: str | None = Field(None, description="e.g., govt-public")

    # Temporal
    in_force_date: date | None = Field(None, description="When act came into force")
    last_amended_date: date | None = Field(None, description="Last amendment date")
    in_force: bool = Field(True, description="Whether act is currently in force")

    # Stats
    section_count: int | None = Field(None, description="Number of sections")

    # Provenance
    source_url: str = Field(..., description="URL to laws-lois.justice.gc.ca")
    source_path: str | None = Field(None, description="Local file path in Axiom")
