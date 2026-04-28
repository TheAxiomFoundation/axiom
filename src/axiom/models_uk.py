"""Data models for UK legislation from legislation.gov.uk.

UK legislation uses a hierarchical citation system:
- ukpga: UK Public General Acts
- uksi: UK Statutory Instruments
- asp: Acts of Scottish Parliament
- asc: Acts of Senedd Cymru (Welsh Parliament)
- nia: Acts of Northern Ireland Assembly
- ukla: UK Local Acts
- ukppa: UK Private and Personal Acts

Source: https://www.legislation.gov.uk/developer/formats
"""

import re
from datetime import date

from pydantic import BaseModel, Field

# Legislation type codes and their full names
UK_LEGISLATION_TYPES = {
    "ukpga": "UK Public General Act",
    "uksi": "UK Statutory Instrument",
    "asp": "Act of Scottish Parliament",
    "ssi": "Scottish Statutory Instrument",
    "asc": "Act of Senedd Cymru",
    "wsi": "Wales Statutory Instrument",
    "nia": "Act of Northern Ireland Assembly",
    "nisr": "Northern Ireland Statutory Rules",
    "ukla": "UK Local Act",
    "ukppa": "UK Private and Personal Act",
    "ukmo": "UK Ministerial Order",
    "uksro": "UK Statutory Rules and Orders",
    "eudr": "EU Directive (Retained)",
    "eur": "EU Regulation (Retained)",
}

# Common short titles mapped to citations
UK_ACT_SHORT_TITLES = {
    "ITEPA": ("ukpga", 2003, 1),  # Income Tax (Earnings and Pensions) Act
    "ITA": ("ukpga", 2007, 3),  # Income Tax Act
    "TCGA": ("ukpga", 1992, 12),  # Taxation of Chargeable Gains Act
    "TCA": ("ukpga", 2002, 21),  # Tax Credits Act
    "SSCBA": ("ukpga", 1992, 4),  # Social Security Contributions and Benefits Act
    "WRA": ("ukpga", 2012, 5),  # Welfare Reform Act
    "CTA": ("ukpga", 2009, 4),  # Corporation Tax Act
    "VATA": ("ukpga", 1994, 23),  # Value Added Tax Act
    "IHTA": ("ukpga", 1984, 51),  # Inheritance Tax Act
    "FA": None,  # Finance Act (varies by year)
}

# Regex for parsing citations
UK_CITATION_PATTERN = re.compile(
    r"^([a-z]{2,5})"  # Type (ukpga, uksi, asp, etc.)
    r"/(\d{4})"  # Year
    r"/(\d+)"  # Number
    r"(?:/section/(\d+[A-Za-z]?))?"  # Optional section
    r"(?:/(\d+[a-z]?(?:/[a-z])?))?$",  # Optional subsection path
    re.IGNORECASE,
)

# Human-readable citation pattern (e.g., "ITEPA 2003 s.1")
UK_SHORT_CITE_PATTERN = re.compile(
    r"^([A-Z]{2,6})\s+"  # Short title
    r"(\d{4})\s+"  # Year
    r"s\.?\s*(\d+[A-Za-z]?)"  # Section
    r"(?:\((\d+)\))?$",  # Optional subsection
    re.IGNORECASE,
)


class UKCitation(BaseModel):
    """A citation to UK legislation.

    Examples:
        - ukpga/2003/1 (ITEPA 2003)
        - ukpga/2003/1/section/62 (Section 62 of ITEPA)
        - uksi/2024/832 (Statutory Instrument)
        - asp/2020/13 (Scottish Act)
    """

    type: str = Field(..., description="Legislation type (ukpga, uksi, asp, etc.)")
    year: int = Field(..., description="Year of enactment")
    number: int = Field(..., description="Act/SI number within the year")
    section: str | None = Field(None, description="Section number")
    subsection: str | None = Field(None, description="Subsection path (e.g., '1/a')")

    model_config = {"extra": "forbid"}

    @classmethod
    def from_string(cls, citation_str: str) -> "UKCitation":
        """Parse a UK legislation citation string.

        Args:
            citation_str: Citation like "ukpga/2003/1/section/62" or "ITEPA 2003 s.1"

        Returns:
            UKCitation object

        Raises:
            ValueError: If the citation cannot be parsed
        """
        citation_str = citation_str.strip()

        # Try standard format first
        match = UK_CITATION_PATTERN.match(citation_str)
        if match:
            leg_type = match.group(1).lower()
            year = int(match.group(2))
            number = int(match.group(3))
            section = match.group(4)
            subsection = match.group(5)

            return cls(
                type=leg_type,
                year=year,
                number=number,
                section=section,
                subsection=subsection,
            )

        # Try short citation format (e.g., "ITEPA 2003 s.1")
        short_match = UK_SHORT_CITE_PATTERN.match(citation_str)
        if short_match:
            short_title = short_match.group(1).upper()
            year = int(short_match.group(2))
            section = short_match.group(3)
            subsection = short_match.group(4)

            # Look up the act
            if short_title in UK_ACT_SHORT_TITLES:
                act_info = UK_ACT_SHORT_TITLES[short_title]
                if act_info:
                    return cls(
                        type=act_info[0],
                        year=act_info[1],
                        number=act_info[2],
                        section=section,
                        subsection=subsection,
                    )

            # Default to ukpga if not found
            return cls(  # pragma: no cover
                type="ukpga",
                year=year,
                number=1,
                section=section,
                subsection=subsection,
            )

        raise ValueError(f"Invalid UK citation: {citation_str}")

    @property
    def legislation_url(self) -> str:
        """Return the legislation.gov.uk URL.

        Returns:
            URL like "https://www.legislation.gov.uk/ukpga/2003/1/section/62"
        """
        url = f"https://www.legislation.gov.uk/{self.type}/{self.year}/{self.number}"
        if self.section:
            url += f"/section/{self.section}"
        return url

    @property
    def data_xml_url(self) -> str:
        """Return the XML data URL.

        Returns:
            URL like "https://www.legislation.gov.uk/ukpga/2003/1/section/62/data.xml"
        """
        return f"{self.legislation_url}/data.xml"

    @property
    def short_cite(self) -> str:
        """Return short citation format.

        Returns:
            String like "2003 c. 1 s. 62"
        """
        # Use chapter number format for Acts
        if self.type in ("ukpga", "ukla", "ukppa"):
            cite = f"{self.year} c. {self.number}"
        else:
            cite = f"{self.type.upper()} {self.year}/{self.number}"  # pragma: no cover

        if self.section:
            cite += f" s. {self.section}"
        return cite

    @property
    def path(self) -> str:
        """Return filesystem-style path for storage.

        Returns:
            Path like "uk/ukpga/2003/1/62/1/a"
        """
        parts = ["uk", self.type, str(self.year), str(self.number)]
        if self.section:
            parts.append(self.section)
        if self.subsection:
            parts.extend(self.subsection.split("/"))
        return "/".join(parts)

    @property
    def type_name(self) -> str:
        """Return full name of legislation type."""
        return UK_LEGISLATION_TYPES.get(self.type, self.type.upper())  # pragma: no cover


class UKSubsection(BaseModel):
    """A subsection or paragraph within UK legislation."""

    id: str | None = Field(None, description="Subsection identifier (e.g., '1', 'a', 'i')")
    heading: str | None = Field(None, description="Subsection heading if present")
    text: str = Field("", description="Text content")
    children: list["UKSubsection"] = Field(
        default_factory=list, description="Child subsections/paragraphs"
    )

    model_config = {"extra": "forbid"}


class UKAmendment(BaseModel):
    """A record of an amendment to UK legislation."""

    type: str = Field(..., description="Amendment type: substitution, repeal, insertion")
    amending_act: str = Field(..., description="Citation of the amending legislation")
    description: str | None = Field(None, description="Brief description of change")
    effective_date: date = Field(..., description="Date the amendment took effect")
    change_id: str | None = Field(None, description="Change ID from CLML")

    model_config = {"extra": "forbid"}


class UKSection(BaseModel):
    """A section from UK legislation.

    Represents a single section with its full text, structure,
    amendments, and territorial extent.
    """

    # Citation
    citation: UKCitation = Field(..., description="UK legislation citation")

    # Content
    title: str = Field(..., description="Section title/heading")
    text: str = Field(..., description="Full text of the section")
    subsections: list[UKSubsection] = Field(
        default_factory=list, description="Structured subsections"
    )

    # Dates
    enacted_date: date = Field(..., description="Date of Royal Assent")
    commencement_date: date | None = Field(None, description="Date section came into force")

    # Territorial extent
    extent: list[str] = Field(
        default_factory=list,
        description="Territorial extent: E (England), W (Wales), S (Scotland), N.I.",
    )

    # Amendments
    amendments: list[UKAmendment] = Field(default_factory=list, description="Amendment history")

    # Cross-references
    references_to: list[str] = Field(
        default_factory=list, description="Citations this section references"
    )
    referenced_by: list[str] = Field(
        default_factory=list, description="Citations that reference this section"
    )

    # Source tracking
    source_url: str | None = Field(None, description="legislation.gov.uk URL")
    retrieved_at: date | None = Field(None, description="Date this version was retrieved")

    model_config = {"extra": "forbid"}

    @property
    def path(self) -> str:
        """Return filesystem-style path for storage."""
        return self.citation.path


class UKPart(BaseModel):
    """A part or chapter within UK legislation."""

    number: str = Field(..., description="Part number")
    title: str = Field(..., description="Part title")
    section_range: str | None = Field(None, description="Section range (e.g., '1-12')")

    model_config = {"extra": "forbid"}


class UKAct(BaseModel):
    """Complete UK Act with metadata.

    Represents an entire Act's metadata without individual sections.
    """

    citation: UKCitation = Field(..., description="UK legislation citation")
    title: str = Field(..., description="Full title of the Act")
    short_title: str | None = Field(None, description="Short title (e.g., 'ITEPA 2003')")

    # Dates
    enacted_date: date = Field(..., description="Date of Royal Assent")
    commencement_date: date | None = Field(None, description="Default commencement date")

    # Structure
    parts: list[UKPart] = Field(default_factory=list, description="Parts/Chapters")
    section_count: int | None = Field(None, description="Total number of sections")

    # Territorial extent
    extent: list[str] = Field(
        default_factory=list,
        description="Territorial extent",
    )

    # Metadata
    subjects: list[str] = Field(default_factory=list, description="Subject classifications")

    # Source tracking
    source_url: str | None = Field(None, description="legislation.gov.uk URL")
    retrieved_at: date | None = Field(None, description="Date retrieved")

    model_config = {"extra": "forbid"}


class UKSearchResult(BaseModel):
    """A search result for UK legislation."""

    citation: str = Field(..., description="UK citation string")
    title: str = Field(..., description="Section title")
    snippet: str = Field(..., description="Relevant text snippet with highlights")
    score: float = Field(..., description="Relevance score (0-1)")
    act_title: str | None = Field(None, description="Title of the parent Act")

    model_config = {"extra": "forbid"}
