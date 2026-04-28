"""Data models for statute representation."""

from datetime import date

from pydantic import BaseModel, Field


class Citation(BaseModel):
    """A legal citation to a specific statute section."""

    title: int = Field(..., description="Title number (e.g., 26 for IRC)")
    section: str = Field(..., description="Section number (e.g., '32' or '32A')")
    subsection: str | None = Field(None, description="Subsection path (e.g., 'a/1/A')")

    @property
    def usc_cite(self) -> str:
        """Return standard USC citation format."""
        base = f"{self.title} USC {self.section}"
        if self.subsection:
            # Convert a/1/A to (a)(1)(A)
            parts = self.subsection.split("/")
            formatted = "".join(f"({p})" for p in parts)
            return f"{base}{formatted}"
        return base

    @property
    def path(self) -> str:
        """Return filesystem-style path for RuleSpec YAML."""
        if self.subsection:
            return f"statute/{self.title}/{self.section}/{self.subsection}"
        return f"statute/{self.title}/{self.section}"

    @classmethod
    def from_string(cls, cite: str) -> "Citation":
        """Parse a citation string like '26 USC 32(a)(1)'."""
        import re

        # Match patterns like "26 USC 32" or "26 USC 32(a)(1)(A)"
        pattern = r"(\d+)\s*(?:U\.?S\.?C\.?|USC)\s*(?:§\s*)?(\d+[A-Za-z]?)(?:\(([^)]+)\))?"
        match = re.match(pattern, cite.strip(), re.IGNORECASE)
        if not match:
            raise ValueError(f"Cannot parse citation: {cite}")

        title = int(match.group(1))
        section = match.group(2)

        # Parse subsections like (a)(1)(A) into a/1/A
        subsection = None
        remainder = cite[match.end() :]
        if remainder or match.group(3):
            sub_pattern = r"\(([^)]+)\)"
            subs = re.findall(sub_pattern, cite)
            if subs:
                subsection = "/".join(subs)

        return cls(title=title, section=section, subsection=subsection)


class Subsection(BaseModel):
    """A subsection within a statute section."""

    identifier: str = Field(..., description="Subsection identifier (e.g., 'a', '1', 'A')")
    heading: str | None = Field(None, description="Subsection heading if present")
    text: str = Field(..., description="Text content of this subsection")
    children: list["Subsection"] = Field(default_factory=list, description="Child subsections")

    model_config = {"extra": "forbid"}

    def full_text(self) -> str:
        """Recursively aggregate heading + text + all children's text."""
        parts: list[str] = []
        if self.heading:
            parts.append(f"({self.identifier}) {self.heading}")
        if self.text:
            parts.append(self.text)
        for child in self.children:
            parts.append(child.full_text())
        return "\n".join(parts)


class Section(BaseModel):
    """A complete statute section with full metadata."""

    citation: Citation
    title_name: str = Field(..., description="Name of the title (e.g., 'Internal Revenue Code')")
    section_title: str = Field(..., description="Section heading (e.g., 'Earned income')")
    text: str = Field(..., description="Full text of the section")
    subsections: list[Subsection] = Field(
        default_factory=list, description="Hierarchical subsection structure"
    )

    # Metadata
    enacted_date: date | None = Field(None, description="Date section was enacted")
    last_amended: date | None = Field(None, description="Date of last amendment")
    public_laws: list[str] = Field(
        default_factory=list, description="Public law numbers that affected this section"
    )
    effective_date: date | None = Field(
        None, description="Effective date if different from enactment"
    )

    # Cross-references
    references_to: list[str] = Field(
        default_factory=list, description="Citations this section references"
    )
    referenced_by: list[str] = Field(
        default_factory=list, description="Citations that reference this section"
    )

    # Source tracking
    source_url: str = Field(..., description="URL to official source")
    retrieved_at: date = Field(..., description="Date this version was retrieved")
    uslm_id: str | None = Field(None, description="USLM identifier if from US Code")

    model_config = {"extra": "forbid"}

    def get_subsection(self, path: str) -> "Subsection | None":
        """Walk the subsection tree by slash-separated path (e.g., 'c', 'b/1/A')."""
        if not path:
            return None
        segments = path.split("/")
        children = self.subsections
        node = None
        for seg in segments:
            node = next((c for c in children if c.identifier == seg), None)
            if node is None:
                return None
            children = node.children
        return node

    def get_subsection_text(self, path: str) -> str | None:
        """Get the full recursive text for a subsection by path."""
        sub = self.get_subsection(path)
        return sub.full_text() if sub else None


class SearchResult(BaseModel):
    """A search result with relevance scoring."""

    citation: Citation
    section_title: str
    snippet: str = Field(..., description="Relevant text snippet with highlights")
    score: float = Field(..., description="Relevance score (0-1)")

    model_config = {"extra": "forbid"}


class TitleInfo(BaseModel):
    """Metadata about a US Code title."""

    number: int
    name: str
    section_count: int
    last_updated: date
    is_positive_law: bool = Field(
        ..., description="Whether this title has been enacted into positive law"
    )

    model_config = {"extra": "forbid"}
