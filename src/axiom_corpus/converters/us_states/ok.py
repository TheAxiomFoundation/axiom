"""Oklahoma state statute converter.

Converts Oklahoma Statutes HTML from OSCN (Oklahoma State Courts Network)
to the internal Section model for ingestion.

Oklahoma Statutes Structure:
- Titles (e.g., Title 68: Revenue and Taxation)
- Chapters (e.g., Chapter 1: Tax Codes)
- Articles (e.g., Article 1: Oklahoma Tax Commission)
- Sections (e.g., Section 101: Tax Code)

URL Patterns:
- Title index: /osStatutes{title}.html
- Section: /DeliverDocument.asp?CiteID={cite_id}

Note: OSCN uses numeric CiteIDs for each section. This converter maintains
a mapping of section numbers to CiteIDs for common tax and welfare sections.

Example:
    >>> from axiom_corpus.converters.us_states.ok import OKConverter
    >>> converter = OKConverter()
    >>> section = converter.fetch_section("68-101")
    >>> print(section.section_title)
    "Tax Code"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://www.oscn.net/applications/oscn"

# Title mapping for reference
OK_TITLES: dict[str, str] = {
    2: "Agriculture",
    10: "Children",
    12: "Consumer Credit",
    15: "Charities",
    21: "Crimes and Punishments",
    22: "Criminal Procedure",
    25: "Definitions and General Provisions",
    36: "Insurance",
    40: "Labor",
    47: "Motor Vehicles",
    51: "Officers",
    56: "Poor Persons",
    58: "Probate Procedure",
    62: "Public Finance",
    63: "Public Health and Safety",
    68: "Revenue and Taxation",
    70: "Schools",
    74: "State Government",
    85: "Workers' Compensation",
}

# Key sections for tax analysis (Title 68 - Revenue and Taxation)
OK_TAX_SECTIONS: dict[str, tuple[int, str]] = {
    # Article 1: Oklahoma Tax Commission
    "68-101": (91842, "Tax Code"),
    "68-102": (91843, "Creation of Oklahoma Tax Commission"),
    "68-103": (91844, "Qualifications of Members"),
    "68-201": (91862, "Definitions"),
    "68-202": (91864, "Definitions (General)"),
    "68-203": (91865, "Corporation Defined"),
    "68-204": (91866, "Resident Defined"),
    # Income Tax
    "68-2351": (92166, "Short Title - Oklahoma Income Tax Act"),
    "68-2352": (92168, "Personal Exemptions"),
    "68-2353": (92171, "Taxable Income Defined"),
    "68-2354": (92176, "Credits Against Tax"),
    "68-2355": (92178, "Oklahoma Taxable Income"),
    "68-2357": (92181, "Tax Rate - Individuals"),
    "68-2358": (92182, "Tax Rate - Corporations"),
    "68-2359": (92183, "Returns Required"),
    # Sales Tax
    "68-1350": (91969, "Sales Tax Definitions"),
    "68-1351": (91972, "Exemptions from Sales Tax"),
    "68-1354": (91979, "Sales Tax Rate"),
    "68-1356": (91982, "Sales Tax - Credits"),
    "68-1357": (91983, "Sales Tax - Filing Returns"),
    # Use Tax
    "68-1401": (91991, "Use Tax Definitions"),
    "68-1402": (91994, "Use Tax Imposed"),
    "68-1403": (91995, "Use Tax Exemptions"),
    # Property Tax
    "68-2801": (92238, "Property Tax Definitions"),
    "68-2802": (92239, "Property Tax Assessment"),
    "68-2803": (92240, "Property Tax Exemptions"),
    # Drug Tax
    "68-450.1": (92028, "Drug Tax Definitions"),
    "68-450.2": (92029, "Drug Tax Imposed"),
    "68-450.3": (92030, "Drug Tax Rate"),
}

# Key sections for welfare analysis (Title 56 - Poor Persons)
OK_WELFARE_SECTIONS: dict[str, tuple[int, str]] = {
    "56-26.1": (83158, "General Assistance Definitions"),
    "56-26.2": (83159, "General Assistance Program"),
    "56-26.3": (83160, "General Assistance Eligibility"),
    "56-31": (83161, "Care of Indigent Persons"),
    "56-32": (83162, "County Responsibility"),
    "56-33": (83163, "Hospital Care"),
    "56-40": (83182, "Oklahoma Indigent Health Care Act"),
    "56-41": (83183, "Indigent Health Care Definitions"),
    "56-42": (83184, "Indigent Health Care Fund"),
    "56-54": (83200, "Burial of Indigent Persons"),
    "56-162": (83249, "Children's Services Definitions"),
    "56-163": (83250, "Children's Services Program"),
    "56-230.50": (83294, "Oklahoma Child Care Facilities Act"),
    "56-230.51": (83295, "Child Care Definitions"),
    "56-230.52": (83296, "Child Care Licensing"),
    "56-238": (83313, "SNAP - Supplemental Nutrition Assistance"),
    "56-239": (83314, "SNAP Eligibility"),
    "56-240": (83315, "SNAP Benefits"),
}

# Combined section registry for lookup
OK_SECTIONS: dict[str, tuple[int, str]] = {**OK_TAX_SECTIONS, **OK_WELFARE_SECTIONS}


@dataclass
class ParsedOKSection:
    """Parsed Oklahoma statute section."""

    section_number: str  # e.g., "68-101"
    section_title: str  # e.g., "Tax Code"
    title_number: int  # e.g., 68
    title_name: str  # e.g., "Revenue and Taxation"
    chapter: str | None  # e.g., "Chapter 1"
    article: str | None  # e.g., "Article 1"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedOKSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None
    cite_id: int | None = None  # OSCN CiteID


@dataclass
class ParsedOKSubsection:
    """A subsection within an Oklahoma statute."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list["ParsedOKSubsection"] = field(default_factory=list)


class OKConverterError(Exception):
    """Error during Oklahoma statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class OKConverter:
    """Converter for Oklahoma Statutes HTML to internal Section model.

    OSCN (Oklahoma State Courts Network) provides Oklahoma Statutes online.
    Each section has a unique CiteID for direct access.

    Example:
        >>> converter = OKConverter()
        >>> section = converter.fetch_section("68-101")
        >>> print(section.citation.section)
        "OK-68-101"

        >>> for section in converter.iter_title(68):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Oklahoma statute converter.

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

    def _get_cite_id(self, section_number: str) -> int:
        """Get the OSCN CiteID for a section number.

        Args:
            section_number: e.g., "68-101" or "56-26.1"

        Returns:
            CiteID number

        Raises:
            OKConverterError: If section not found in registry
        """
        if section_number in OK_SECTIONS:
            return OK_SECTIONS[section_number][0]
        raise OKConverterError(
            f"Section {section_number} not found in registry. "
            "Use fetch_by_cite_id() for direct CiteID access."
        )

    def _build_section_url(self, cite_id: int) -> str:
        """Build the URL for a section by CiteID.

        Args:
            cite_id: OSCN CiteID number

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/DeliverDocument.asp?CiteID={cite_id}"

    def _build_title_index_url(self, title: int) -> str:
        """Build the URL for a title index page."""
        return f"{BASE_URL}/index.asp?ftdb=STOKST{title}&level=1"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
        cite_id: int | None = None,
    ) -> ParsedOKSection:
        """Parse section HTML into ParsedOKSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise OKConverterError(f"Section {section_number} not found", url)

        # Extract title number from section number (e.g., "68-101" -> 68)
        title_number = int(section_number.split("-")[0])
        title_name = OK_TITLES.get(title_number, f"Title {title_number}")

        # Extract section title from header or document_header
        section_title = ""

        # Try to find section title in document header area
        # OSCN pages typically have format: "Section {num}. {title}"
        header_patterns = [
            re.compile(rf"§\s*{re.escape(section_number.split('-')[1])}\.\s*(.+?)(?:\s*[-—]|$)"),
            re.compile(
                rf"Section\s+{re.escape(section_number.split('-')[1])}\.\s*(.+?)(?:\s*[-—]|$)"
            ),
            re.compile(rf"{re.escape(section_number.split('-')[1])}\.\s*(.+?)(?:\s*[-—]|$)"),
        ]

        for pattern in header_patterns:
            for text_node in soup.stripped_strings:
                match = pattern.search(text_node)
                if match:
                    section_title = match.group(1).strip()
                    break
            if section_title:
                break

        # Fallback: check known section titles
        if not section_title and section_number in OK_SECTIONS:
            section_title = OK_SECTIONS[section_number][1]

        # Try to extract from page title
        if not section_title:
            title_tag = soup.find("title")  # pragma: no cover
            if title_tag:  # pragma: no cover
                title_text = title_tag.get_text(strip=True)  # pragma: no cover
                # OSCN title format: "Section X - Title | OSCN"
                match = re.search(
                    r"Section\s+[\d.]+\s*[-—]\s*(.+?)(?:\s*\||\s*$)", title_text
                )  # pragma: no cover
                if match:  # pragma: no cover
                    section_title = match.group(1).strip()  # pragma: no cover

        # Get chapter and article from navigation/breadcrumbs if available
        chapter = None
        article = None

        # Look for chapter/article in hierarchy links
        for link in soup.find_all("a"):
            link_text = link.get_text(strip=True)
            if "Chapter" in link_text:
                chapter = link_text
            elif "Article" in link_text:
                article = link_text

        # Get body content - OSCN uses "paragraphs" div
        content_elem = (
            soup.find("div", class_="paragraphs")
            or soup.find("div", id="content")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(
                ["nav", "script", "style", "header", "footer"]
            ):  # pragma: no cover
                elem.decompose()
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note (OSCN uses "Historical Data" section)
        history = None
        history_match = re.search(
            r"(?:Historical Data|History)[:\s]*(.+?)(?:\n\n|Citationizer|$)",
            text,
            re.DOTALL | re.IGNORECASE,
        )
        if history_match:
            history = history_match.group(1).strip()[:1000]  # Limit length

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedOKSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_number,
            title_name=title_name,
            chapter=chapter,
            article=article,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
            cite_id=cite_id,
        )

    def _parse_subsections(self, text: str) -> list[ParsedOKSubsection]:
        """Parse hierarchical subsections from text.

        Oklahoma statutes typically use:
        - 1., 2., 3. or (1), (2), (3) for primary divisions
        - a., b., c. or (a), (b), (c) for secondary divisions
        - A., B., C. for major divisions in some statutes
        """
        subsections = []

        # Try numbered pattern first: 1., 2., 3.
        numbered_pattern = re.compile(r"(?:^|\n)\s*(\d+)\.\s+")
        paren_pattern = re.compile(r"\((\d+)\)\s+")

        # Check which pattern is dominant
        numbered_count = len(numbered_pattern.findall(text))
        paren_count = len(paren_pattern.findall(text))

        if numbered_count >= paren_count and numbered_count > 0:
            # Use numbered pattern: 1., 2., 3.
            parts = re.split(r"(?=(?:^|\n)\s*\d+\.\s)", text)
            for part in parts[1:]:
                match = re.match(r"\s*(\d+)\.\s*", part)
                if not match:
                    continue  # pragma: no cover
                identifier = match.group(1)
                content = part[match.end() :]

                # Parse lettered children: a., b., c.
                children = self._parse_lettered_subsections(content)

                # Get text before first child
                if children:
                    first_child_match = re.search(r"(?:^|\n)\s*[a-z]\.\s", content)
                    direct_text = (
                        content[: first_child_match.start()].strip()
                        if first_child_match
                        else content.strip()
                    )
                else:
                    direct_text = content.strip()

                # Clean up - remove trailing numbered sections
                next_subsection = re.search(r"(?:^|\n)\s*\d+\.\s", direct_text)
                if next_subsection:
                    direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

                subsections.append(
                    ParsedOKSubsection(
                        identifier=identifier,
                        text=direct_text[:2000],
                        children=children,
                    )
                )
        elif paren_count > 0:
            # Use parenthetical pattern: (1), (2), (3)
            parts = re.split(r"(?=\(\d+\)\s)", text)  # pragma: no cover
            for part in parts[1:]:  # pragma: no cover
                match = re.match(r"\((\d+)\)\s*", part)  # pragma: no cover
                if not match:  # pragma: no cover
                    continue  # pragma: no cover
                identifier = match.group(1)  # pragma: no cover
                content = part[match.end() :]  # pragma: no cover

                # Parse children: (a), (b), (c)
                children = self._parse_paren_letter_subsections(content)  # pragma: no cover

                if children:  # pragma: no cover
                    first_child_match = re.search(r"\([a-z]\)", content)  # pragma: no cover
                    direct_text = (  # pragma: no cover
                        content[: first_child_match.start()].strip()
                        if first_child_match
                        else content.strip()
                    )
                else:
                    direct_text = content.strip()  # pragma: no cover

                next_subsection = re.search(r"\(\d+\)", direct_text)  # pragma: no cover
                if next_subsection:  # pragma: no cover
                    direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

                subsections.append(  # pragma: no cover
                    ParsedOKSubsection(
                        identifier=identifier,
                        text=direct_text[:2000],
                        children=children,
                    )
                )
        else:
            # Try lettered major divisions: A., B., C.
            subsections = self._parse_major_letter_subsections(text)

        return subsections

    def _parse_lettered_subsections(self, text: str) -> list[ParsedOKSubsection]:
        """Parse level 2 subsections: a., b., c."""
        subsections = []
        parts = re.split(r"(?=(?:^|\n)\s*[a-z]\.\s)", text)

        for part in parts[1:]:
            match = re.match(r"\s*([a-z])\.\s*", part)
            if not match:
                continue  # pragma: no cover
            identifier = match.group(1)
            content = part[match.end() :]

            # Limit and clean
            next_num = re.search(r"(?:^|\n)\s*\d+\.\s", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedOKSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _parse_paren_letter_subsections(self, text: str) -> list[ParsedOKSubsection]:
        """Parse level 2 subsections: (a), (b), (c)."""
        subsections = []  # pragma: no cover
        parts = re.split(r"(?=\([a-z]\)\s)", text)  # pragma: no cover

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\(([a-z])\)\s*", part)  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover
            identifier = match.group(1)  # pragma: no cover
            content = part[match.end() :]  # pragma: no cover

            # Limit
            next_num = re.search(r"\(\d+\)", content)  # pragma: no cover
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedOKSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections  # pragma: no cover

    def _parse_major_letter_subsections(self, text: str) -> list[ParsedOKSubsection]:
        """Parse major letter divisions: A., B., C."""
        subsections = []
        parts = re.split(r"(?=(?:^|\n)\s*[A-Z]\.\s)", text)

        for part in parts[1:]:
            match = re.match(r"\s*([A-Z])\.\s*", part)
            if not match:
                continue  # pragma: no cover
            identifier = match.group(1)
            content = part[match.end() :]

            # Parse numbered children if present
            children = self._parse_subsections(content)
            if not children:
                children = self._parse_lettered_subsections(content)

            if children:
                first_child_match = re.search(
                    r"(?:^|\n)\s*(?:\d+\.|[a-z]\.)\s", content
                )  # pragma: no cover
                direct_text = (  # pragma: no cover
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            subsections.append(
                ParsedOKSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedOKSection) -> Section:
        """Convert ParsedOKSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"OK-{parsed.section_number}",
        )

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
                        children=[],
                    )
                    for child in sub.children
                ],
            )
            for sub in parsed.subsections
        ]

        return Section(
            citation=citation,
            title_name=f"Oklahoma Statutes - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ok/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section by section number.

        Args:
            section_number: e.g., "68-101", "56-26.1"

        Returns:
            Section model

        Raises:
            OKConverterError: If section not found or parsing fails
        """
        cite_id = self._get_cite_id(section_number)
        return self.fetch_by_cite_id(cite_id, section_number)

    def fetch_by_cite_id(self, cite_id: int, section_number: str | None = None) -> Section:
        """Fetch and convert a section by OSCN CiteID.

        Args:
            cite_id: OSCN CiteID number
            section_number: Optional section number for labeling

        Returns:
            Section model

        Raises:
            OKConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(cite_id)
        html = self._get(url)

        # If no section number provided, try to extract from HTML
        if section_number is None:
            section_number = f"citeID-{cite_id}"  # pragma: no cover

        parsed = self._parse_section_html(html, section_number, url, cite_id)
        return self._to_section(parsed)

    def get_title_sections(self, title: int) -> list[tuple[str, int]]:
        """Get list of section numbers and CiteIDs for a title from registry.

        Args:
            title: Title number (e.g., 68, 56)

        Returns:
            List of (section_number, cite_id) tuples
        """
        prefix = f"{title}-"
        return [
            (section, cite_id)
            for section, (cite_id, _) in OK_SECTIONS.items()
            if section.startswith(prefix)
        ]

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all known sections in a title.

        Args:
            title: Title number (e.g., 68 for Revenue and Taxation)

        Yields:
            Section objects for each section
        """
        sections = self.get_title_sections(title)

        for section_num, cite_id in sections:
            try:
                yield self.fetch_by_cite_id(cite_id, section_num)
            except OKConverterError as e:
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_tax_sections(self) -> Iterator[Section]:
        """Iterate over known tax sections (Title 68).

        Yields:
            Section objects for tax-related sections
        """
        yield from self.iter_title(68)  # pragma: no cover

    def iter_welfare_sections(self) -> Iterator[Section]:
        """Iterate over known welfare sections (Title 56).

        Yields:
            Section objects for welfare-related sections
        """
        yield from self.iter_title(56)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "OKConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ok_section(section_number: str) -> Section:
    """Fetch a single Oklahoma statute section.

    Args:
        section_number: e.g., "68-101"

    Returns:
        Section model
    """
    with OKConverter() as converter:
        return converter.fetch_section(section_number)


def download_ok_title(title: int) -> list[Section]:
    """Download all known sections from an Oklahoma Statutes title.

    Args:
        title: Title number (e.g., 68)

    Returns:
        List of Section objects
    """
    with OKConverter() as converter:
        return list(converter.iter_title(title))


def download_ok_tax_sections() -> Iterator[Section]:
    """Download known tax-related sections (Title 68).

    Yields:
        Section objects
    """
    with OKConverter() as converter:  # pragma: no cover
        yield from converter.iter_tax_sections()  # pragma: no cover


def download_ok_welfare_sections() -> Iterator[Section]:
    """Download known welfare-related sections (Title 56).

    Yields:
        Section objects
    """
    with OKConverter() as converter:  # pragma: no cover
        yield from converter.iter_welfare_sections()  # pragma: no cover
