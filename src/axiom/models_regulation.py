"""Data models for federal regulations (Code of Federal Regulations)."""

import re
from datetime import date

from pydantic import BaseModel, Field

# Regex pattern for CFR citations - defined outside class to avoid Pydantic issues
# Matches: "26 CFR 1.32-1(a)(1)" or "26 C.F.R. § 1.32-1" etc.
CFR_CITATION_PATTERN = re.compile(
    r"(\d+)\s*"  # Title number
    r"C\.?F\.?R\.?\s*"  # CFR or C.F.R.
    r"§?\s*"  # Optional section symbol
    r"(\d+)"  # Part number
    r"(?:\.(\d+(?:-\d+)?))?"  # Optional section (e.g., .32 or .32-1)
    r"((?:\([a-zA-Z0-9]+\))*)?",  # Optional subsections like (a)(1)(i)
    re.IGNORECASE,
)


class CFRCitation(BaseModel):
    """A citation to the Code of Federal Regulations.

    Examples:
        - 26 CFR 1.32-1 (Title 26, Part 1, Section 32-1)
        - 26 CFR 1.32-1(a)(1) (with subsection)
        - 26 C.F.R. § 1.32-1 (alternate format)
    """

    title: int = Field(..., description="CFR title number (e.g., 26 for Treasury/IRS)")
    part: int = Field(..., description="Part number within the title")
    section: str | None = Field(None, description="Section number (e.g., '32-1')")
    subsection: str | None = Field(None, description="Subsection path in a/1/i format")

    model_config = {"extra": "forbid"}

    @classmethod
    def from_string(cls, citation_str: str) -> "CFRCitation":
        """Parse a CFR citation string.

        Args:
            citation_str: Citation like "26 CFR 1.32-1(a)(1)"

        Returns:
            CFRCitation object

        Raises:
            ValueError: If the citation cannot be parsed
        """
        match = CFR_CITATION_PATTERN.match(citation_str.strip())
        if not match:
            raise ValueError(f"Invalid CFR citation: {citation_str}")

        title = int(match.group(1))
        part = int(match.group(2))
        section = match.group(3)  # May be None
        subsection_str = match.group(4)  # May be None or "(a)(1)(i)"

        # Parse subsection path
        subsection = None
        if subsection_str:
            # Extract content from parentheses: "(a)(1)(i)" -> ["a", "1", "i"]
            parts = re.findall(r"\(([^)]+)\)", subsection_str)
            if parts:
                subsection = "/".join(parts)

        return cls(title=title, part=part, section=section, subsection=subsection)

    @property
    def cfr_cite(self) -> str:
        """Return the standard CFR citation format.

        Returns:
            String like "26 CFR 1.32-1(a)(1)"
        """
        result = f"{self.title} CFR {self.part}"
        if self.section:
            result += f".{self.section}"
        if self.subsection:
            # Convert "a/1/i" back to "(a)(1)(i)"
            parts = self.subsection.split("/")
            result += "".join(f"({p})" for p in parts)
        return result

    @property
    def path(self) -> str:
        """Return filesystem-style path for storage.

        Returns:
            Path like "regulation/26/1/32-1/a/1"
        """
        parts = ["regulation", str(self.title), str(self.part)]
        if self.section:
            parts.append(self.section)
        if self.subsection:
            parts.extend(self.subsection.split("/"))
        return "/".join(parts)


class RegulationSubsection(BaseModel):
    """A subsection within a regulation."""

    id: str = Field(..., description="Subsection identifier (e.g., 'a', '1', 'i')")
    heading: str | None = Field(None, description="Subsection heading if present")
    text: str = Field(..., description="Text content of this subsection")
    children: list["RegulationSubsection"] = Field(
        default_factory=list, description="Child subsections"
    )

    model_config = {"extra": "forbid"}


class Amendment(BaseModel):
    """A record of an amendment to a regulation via Federal Register."""

    document: str = Field(
        ..., description="Treasury Decision or document number (e.g., 'T.D. 9954')"
    )
    federal_register_citation: str = Field(
        ..., description="Federal Register citation (e.g., '86 FR 12345')"
    )
    published_date: date = Field(..., description="Date published in Federal Register")
    effective_date: date = Field(..., description="Date the amendment took effect")
    description: str | None = Field(None, description="Brief description of changes")

    model_config = {"extra": "forbid"}


class Regulation(BaseModel):
    """A regulation from the Code of Federal Regulations.

    Represents a single CFR section with its full text, structure,
    authority (source statute), and amendment history.
    """

    # Citation
    citation: CFRCitation = Field(..., description="CFR citation for this regulation")

    # Content
    heading: str = Field(..., description="Section heading/title")
    authority: str = Field(..., description="Authority citation (e.g., '26 U.S.C. 32')")
    source: str = Field(..., description="Source document (e.g., 'T.D. 9954, 86 FR 12345')")
    full_text: str = Field(..., description="Full text of the regulation")
    subsections: list[RegulationSubsection] = Field(
        default_factory=list, description="Structured subsections"
    )

    # Dates
    effective_date: date = Field(..., description="Current effective date")

    # Cross-references
    source_statutes: list[str] = Field(
        default_factory=list,
        description="USC citations this regulation interprets (e.g., ['26 USC 32'])",
    )
    cross_references: list[str] = Field(
        default_factory=list,
        description="Other CFR sections referenced",
    )

    # Amendment history
    amendments: list[Amendment] = Field(
        default_factory=list, description="Amendment history from Federal Register"
    )

    # Source tracking
    source_url: str | None = Field(None, description="URL to eCFR or govinfo source")
    retrieved_at: date | None = Field(None, description="Date this version was retrieved")

    model_config = {"extra": "forbid"}

    @property
    def path(self) -> str:
        """Return filesystem-style path for storage."""
        return self.citation.path

    @property
    def cfr_cite(self) -> str:
        """Return the standard CFR citation format."""
        return self.citation.cfr_cite


class RegulationSearchResult(BaseModel):
    """A search result for regulations."""

    cfr_cite: str = Field(..., description="CFR citation string")
    heading: str = Field(..., description="Section heading")
    snippet: str = Field(..., description="Relevant text snippet with highlights")
    score: float = Field(..., description="Relevance score (0-1)")
    effective_date: date = Field(..., description="Current effective date")

    model_config = {"extra": "forbid"}
