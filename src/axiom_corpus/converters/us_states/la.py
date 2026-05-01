"""Louisiana state statute converter.

Converts Louisiana Revised Statutes HTML from legis.la.gov
to the internal Section model for ingestion.

Louisiana Revised Statutes Structure:
- Titles (e.g., Title 47: Revenue and Taxation)
- Chapters (e.g., Chapter 1: Income Tax)
- Parts (e.g., Part I: General Provisions)
- Sections (e.g., RS 47:32: Tax on Louisiana taxable income)

URL Patterns:
- Law search: legis.la.gov/legis/LawSearch.aspx
- Table of contents: legis.la.gov/legis/Laws_Toc.aspx?folder=75&level=Parent
- Individual section: legis.la.gov/legis/Law.aspx?d=[doc_id]

Note: Louisiana uses document IDs (d=xxxxx) rather than section-based URLs.
The document IDs must be discovered through the table of contents.

Example:
    >>> from axiom_corpus.converters.us_states.la import LAConverter
    >>> converter = LAConverter()
    >>> section = converter.fetch_section_by_id(101682)
    >>> print(section.section_title)
    "Special adjustment for long-term contracts"
"""

import re
import time
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://www.legis.la.gov/legis"

# Louisiana Revised Statutes titles for tax/benefit analysis
LA_TITLES: dict[str, str] = {
    1: "General Provisions",
    6: "Banks and Banking",
    9: "Civil Code - Ancillaries",
    11: "Consolidated Public Retirement",
    12: "Corporations and Associations",
    14: "Criminal Law",
    15: "Criminal Procedure",
    17: "Education",
    18: "Elections",
    22: "Insurance",
    23: "Labor and Workers' Compensation",
    26: "Liquor - Alcoholic Beverages",
    28: "Mental Health",
    29: "Military, Naval and Veterans' Affairs",
    30: "Minerals, Oil and Gas and Environmental Quality",
    32: "Motor Vehicles and Traffic Regulation",
    33: "Municipalities and Parishes",
    34: "Navigation and Shipping",
    36: "Organization of Executive Branch of State Government",
    37: "Professions and Occupations",
    38: "Public Contracts, Works and Improvements",
    39: "Public Finance",
    40: "Public Health and Safety",
    42: "Public Officers and Employees",
    43: "Public Service Commission",
    44: "Public Records and Recorders",
    45: "Public Utilities and Carriers",
    46: "Public Welfare and Assistance",
    47: "Revenue and Taxation",
    48: "Roads, Bridges, Ferries and Tunnels",
    49: "State Administration",
    51: "Trade and Commerce",
    54: "Warehouses",
    56: "Wildlife and Fisheries",
}

# Key sections for tax analysis (Title 47)
LA_TAX_SECTIONS: dict[str, str] = {
    "47:21": "Personal income tax - General",
    "47:32": "Tax on Louisiana taxable income - Rates",
    "47:44": "Returns and payment of tax",
    "47:51": "Definitions for corporate income tax",
    "47:287.2": "Definitions for corporate income tax",
    "47:287.11": "Imposition of tax on corporations",
    "47:287.73": "Louisiana taxable income",
    "47:287.86": "Apportionment of business income",
    "47:287.440": "Tax credits",
    "47:287.445": "Special adjustment for long-term contracts",
    "47:293": "Federal adjusted gross income modification",
    "47:294": "Louisiana income tax brackets",
    "47:295": "Net income defined",
    "47:297": "Itemized deductions",
    "47:297.2": "Standard deduction",
    "47:297.4": "Personal exemption",
    "47:301": "Sales and use tax - Definitions",
    "47:302": "Imposition of sales tax",
    "47:321": "Sales and use tax - Agricultural purposes",
    "47:601": "Severance tax - Oil and gas",
    "47:1401": "Property tax - Assessment",
    "47:1624": "Interest rate on underpayments",
    "47:1675": "Tax credits - General",
}

# Key sections for public welfare (Title 46)
LA_WELFARE_SECTIONS: dict[str, str] = {
    "46:51": "Department of Children and Family Services",
    "46:56": "Administration of public assistance",
    "46:231": "Louisiana Workforce Commission",
    "46:236.1": "Child support enforcement",
    "46:460": "Medicaid program administration",
    "46:460.31": "Louisiana Children's Health Insurance Program",
    "46:932": "Food assistance program",
    "46:1301": "Developmental disabilities services",
    "46:2361": "Services for the aging",
}


@dataclass
class ParsedLASection:
    """Parsed Louisiana statute section."""

    doc_id: int  # Document ID (d parameter)
    citation: str  # e.g., "RS 47:287.445"
    title_number: int  # e.g., 47
    section_number: str  # e.g., "287.445"
    section_title: str  # e.g., "Special adjustment for long-term contracts"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedLASubsection] = field(default_factory=list)
    history: str | None = None  # History note (Acts yyyy, No. xxx)
    source_url: str = ""


@dataclass
class ParsedLASubsection:
    """A subsection within a Louisiana statute."""

    identifier: str  # e.g., "A", "B", "1", "a"
    text: str
    children: list[ParsedLASubsection] = field(default_factory=list)


class LAConverterError(Exception):
    """Error during Louisiana statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class LAConverter:
    """Converter for Louisiana Revised Statutes HTML to internal Section model.

    Louisiana uses document IDs rather than direct section URLs, so sections
    must be fetched by document ID. The document IDs can be discovered through
    the table of contents.

    Example:
        >>> converter = LAConverter()
        >>> section = converter.fetch_section_by_id(101682)
        >>> print(section.citation.section)
        "LA-47:287.445"

        >>> for section in converter.iter_title(47):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Louisiana statute converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
            year: Statute year (default: current year)
        """
        self.rate_limit_delay = rate_limit_delay
        self.year = year or date.today().year
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                timeout=60.0,
                headers={"User-Agent": "Axiom/1.0 (Statute Research; contact@axiom-foundation.org)"},
            )
        return self._client

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str) -> str:
        """Make a rate-limited GET request."""
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    def _build_section_url(self, doc_id: int) -> str:
        """Build the URL for a section by document ID.

        Args:
            doc_id: Louisiana document ID (d parameter)

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/Law.aspx?d={doc_id}"

    def _parse_section_html(
        self,
        html: str,
        doc_id: int,
        url: str,
    ) -> ParsedLASection:
        """Parse section HTML into ParsedLASection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "error" in html.lower():  # pragma: no cover
            error_div = soup.find("div", class_="error")
            if error_div:
                raise LAConverterError(f"Document {doc_id} not found", url)  # pragma: no cover

        # Extract citation from LabelName span (e.g., "RS 47:287.445")
        label_span = soup.find("span", id="ctl00_PageBody_LabelName")
        citation = ""
        if label_span:
            citation = label_span.get_text(strip=True)

        # Parse title and section number from citation
        # Format: "RS 47:287.445" or "CC 1234"
        title_number = 0
        section_number = ""
        if citation.startswith("RS "):
            citation_match = re.match(r"RS\s+(\d+):(.+)", citation)
            if citation_match:
                title_number = int(citation_match.group(1))
                section_number = citation_match.group(2)

        # Get the main content from LabelDocument span
        content_span = soup.find("span", id="ctl00_PageBody_LabelDocument")
        if not content_span:
            # Fallback to divLaw
            content_span = soup.find("div", id="ctl00_PageBody_divLaw")  # pragma: no cover

        text = ""
        html_content = ""
        section_title = ""
        history = None
        subsections: list[ParsedLASubsection] = []

        if content_span:
            html_content = str(content_span)

            # Extract all paragraph text
            paragraphs = content_span.find_all("p")
            text_parts = []

            for p in paragraphs:
                p_text = p.get_text(separator=" ", strip=True)
                if p_text:
                    text_parts.append(p_text)

            text = "\n".join(text_parts)

            # Parse section title from first paragraph
            # Format: "Section 287.445. Special adjustment for long-term contracts"
            if text_parts:
                first_para = text_parts[0]
                # Try pattern with section symbol
                title_match = re.match(
                    rf"[^\d]*{re.escape(section_number)}\.\s*(.+?)(?:\s*[A-Z]\.|\s*\(1\)|$)",
                    first_para,
                )
                if title_match:
                    section_title = title_match.group(1).strip().rstrip(".")

                # Fallback: Look for section symbol pattern
                if not section_title:
                    symbol_match = re.search(
                        r"\u00A7\d+\.\s*(.+?)(?:\s*[A-Z]\.|$)", first_para
                    )  # pragma: no cover
                    if symbol_match:  # pragma: no cover
                        section_title = symbol_match.group(1).strip()  # pragma: no cover

            # Extract history note (Acts at end)
            # Pattern: "Acts 1992, No. 588, Section 1; Acts 2002, No. 51, Section 1, eff. Jan. 1, 2003."
            history_match = re.search(r"(Acts\s+\d{4}.+?)(?:<|$)", text, re.DOTALL)
            if history_match:
                history = history_match.group(1).strip()
                # Remove history from main text
                text = text[: history_match.start()].strip()

            # Parse subsections
            subsections = self._parse_subsections(text)

        return ParsedLASection(
            doc_id=doc_id,
            citation=citation,
            title_number=title_number,
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedLASubsection]:
        """Parse hierarchical subsections from text.

        Louisiana statutes typically use:
        - A., B., C. for primary divisions
        - (1), (2), (3) for secondary divisions
        - (a), (b), (c) for tertiary divisions
        """
        subsections = []

        # Split by top-level subsections A., B., C.
        # Match only at start of line (after newline) to avoid matching
        # within text like "U.S.C.A." or "I.R.C."
        parts = re.split(r"(?=\n[A-Z]\.\s)", text)

        for part in parts[1:]:  # Skip content before first A.
            match = re.match(r"\n?([A-Z])\.\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse second-level children (1), (2), etc.
            children = self._parse_level2(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\(\d+\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Clean up text - remove trailing subsections
            next_subsection = re.search(r"\n[A-Z]\.\s", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedLASubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        # If no A., B. subsections, try (1), (2) pattern
        if not subsections:
            subsections = self._parse_numeric_subsections(text)

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedLASubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (a), (b), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([a-z]\)", content)  # pragma: no cover
                direct_text = (  # pragma: no cover
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit to reasonable size and stop at next lettered subsection
            next_letter = re.search(r"\n[A-Z]\.\s", direct_text)
            if next_letter:
                direct_text = direct_text[: next_letter.start()]  # pragma: no cover

            subsections.append(
                ParsedLASubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedLASubsection]:
        """Parse level 3 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedLASubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _parse_numeric_subsections(self, text: str) -> list[ParsedLASubsection]:
        """Parse (1), (2), (3) subsections when no A., B. present."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse nested (a), (b), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([a-z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            subsections.append(
                ParsedLASubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedLASection) -> Section:
        """Convert ParsedLASection to internal Section model."""
        # Create citation using state prefix
        # Format: LA-47:287.445
        section_id = f"LA-{parsed.title_number}:{parsed.section_number}"

        citation = Citation(
            title=0,  # State law indicator
            section=section_id,
        )

        # Get title name
        title_name = LA_TITLES.get(parsed.title_number, f"Title {parsed.title_number}")

        # Convert subsections
        subsections = [
            Subsection(
                identifier=sub.identifier,
                heading=None,
                text=sub.text,
                children=[
                    Subsection(
                        identifier=child.identifier,
                        heading=None,
                        text=child.text,
                        children=[
                            Subsection(
                                identifier=grandchild.identifier,
                                heading=None,
                                text=grandchild.text,
                                children=[],
                            )
                            for grandchild in child.children
                        ],
                    )
                    for child in sub.children
                ],
            )
            for sub in parsed.subsections
        ]

        return Section(
            citation=citation,
            title_name=f"Louisiana Revised Statutes - {title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"la/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_section_by_id(self, doc_id: int) -> Section:
        """Fetch and convert a single section by document ID.

        Args:
            doc_id: Louisiana document ID (e.g., 101682)

        Returns:
            Section model

        Raises:
            LAConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(doc_id)
        html = self._get(url)
        parsed = self._parse_section_html(html, doc_id, url)
        return self._to_section(parsed)

    def fetch_section(self, citation: str, doc_id: int | None = None) -> Section:
        """Fetch and convert a section by citation.

        Note: Louisiana uses document IDs, so either provide the doc_id
        or use fetch_section_by_id directly.

        Args:
            citation: e.g., "47:287.445" or "RS 47:287.445"
            doc_id: Optional document ID if known

        Returns:
            Section model

        Raises:
            LAConverterError: If doc_id not provided and cannot be determined
        """
        if doc_id is None:
            raise LAConverterError(
                "Document ID required for Louisiana statutes. "
                "Use fetch_section_by_id() or provide doc_id parameter.",
                None,
            )
        return self.fetch_section_by_id(doc_id)

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> LAConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_la_section(doc_id: int) -> Section:
    """Fetch a single Louisiana statute section by document ID.

    Args:
        doc_id: Louisiana document ID (e.g., 101682)

    Returns:
        Section model
    """
    with LAConverter() as converter:
        return converter.fetch_section_by_id(doc_id)


# Known document IDs for common sections
# These can be discovered via the table of contents
LA_KNOWN_DOC_IDS: dict[str, int] = {
    # Title 47: Revenue and Taxation
    "47:287.445": 101682,  # Special adjustment for long-term contracts
    "47:2061": 101478,  # Deputy tax collectors
    # Title 46: Public Welfare
    # Add more as discovered
}
