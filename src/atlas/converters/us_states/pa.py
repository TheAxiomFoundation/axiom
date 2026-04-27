"""Pennsylvania state statute converter.

Converts Pennsylvania Consolidated Statutes HTML from palegis.us
to the internal Section model for ingestion.

Pennsylvania Statute Structure:
- Titles (e.g., Title 72: Taxation and Fiscal Affairs)
- Parts (e.g., Part I: General)
- Chapters (e.g., Chapter 3: Microenterprise Development)
- Subchapters
- Sections (e.g., S 3116. Microenterprise loans.)

URL Patterns:
- Consolidated statutes index: /statutes/consolidated
- Title HTML: /statutes/consolidated/view-statute?txtType=HTM&ttl=[NUM]&iFrame=true
- Title with chapter: /statutes/consolidated/view-statute?txtType=HTM&ttl=[NUM]&chpt=[NUM]&iFrame=true
- Title PDF: /statutes/consolidated/view-statute?txtType=PDF&ttl=[NUM]

Note: Pennsylvania has comprehensive state income tax (Title 72) and public welfare (Title 67) codes.

Example:
    >>> from atlas.converters.us_states.pa import PAConverter
    >>> converter = PAConverter()
    >>> section = converter.fetch_section("72", "3116")
    >>> print(section.section_title)
    "Microenterprise loans"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://www.palegis.us/statutes/consolidated"

# Title mapping for Pennsylvania Consolidated Statutes
PA_TITLES: dict[str, str] = {
    1: "General Provisions",
    2: "Administrative Law and Procedure",
    3: "Agriculture",
    4: "Amusements",
    5: "Athletics and Sports",
    7: "Banks and Banking",
    8: "Boroughs and Incorporated Towns",
    9: "Burial Grounds",
    10: "Charitable Organizations",
    11: "Cities",
    12: "Commerce and Trade",
    13: "Commercial Code",
    15: "Corporations and Unincorporated Associations",
    16: "Counties",
    17: "Credit Unions",
    18: "Crimes and Offenses",
    19: "Decedents, Estates and Fiduciaries",
    20: "Decedents, Estates and Fiduciaries",
    22: "Detectives and Private Police",
    23: "Domestic Relations",
    24: "Education",
    25: "Elections",
    27: "Environmental Resources",
    30: "Fish",
    32: "Forests, Waters and State Parks",
    34: "Game",
    35: "Health and Safety",
    37: "Historical and Museums",
    38: "Holidays and Observances",
    40: "Insurance",
    42: "Judiciary and Judicial Procedure",
    44: "Law and Justice",
    45: "Legal Notices",
    46: "Legislature",
    48: "Lodges",
    51: "Military Affairs",
    53: "Municipalities Generally",
    54: "Names",
    58: "Oil and Gas",
    61: "Prisons and Parole",
    62: "Procurement",
    63: "Professions and Occupations (State Licensed)",
    64: "Public Authorities and Quasi-Public Corporations",
    65: "Public Officers",
    66: "Public Utilities",
    67: "Public Welfare",
    68: "Real and Personal Property",
    69: "Savings Associations",
    71: "State Government",
    72: "Taxation and Fiscal Affairs",
    73: "Trade and Commerce",
    74: "Transportation",
    75: "Vehicles",
    76: "Veterans and War Veterans' Organizations",
    77: "Workmen's Compensation",
    79: "Zoning and Planning",
}

# Key titles for tax/benefit analysis
PA_TAX_TITLES: dict[str, str] = {
    72: "Taxation and Fiscal Affairs",
}

PA_WELFARE_TITLES: dict[str, str] = {
    67: "Public Welfare",
    62: "Procurement",  # Includes welfare-related procurement
}


@dataclass
class ParsedPASection:
    """Parsed Pennsylvania statute section."""

    section_number: str  # e.g., "3116"
    section_title: str  # e.g., "Microenterprise loans"
    title_number: int  # e.g., 72
    title_name: str  # e.g., "Taxation and Fiscal Affairs"
    chapter_number: str | None  # e.g., "3"
    chapter_title: str | None  # e.g., "Microenterprise Development"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedPASubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None
    cross_references: list[str] = field(default_factory=list)


@dataclass
class ParsedPASubsection:
    """A subsection within a Pennsylvania statute."""

    identifier: str  # e.g., "a", "1", "i"
    heading: str | None  # e.g., "General rule"
    text: str
    children: list["ParsedPASubsection"] = field(default_factory=list)


class PAConverterError(Exception):
    """Error during Pennsylvania statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class PAConverter:
    """Converter for Pennsylvania Consolidated Statutes HTML to internal Section model.

    Example:
        >>> converter = PAConverter()
        >>> section = converter.fetch_section("72", "3116")
        >>> print(section.citation.section)
        "PA-72-3116"

        >>> for section in converter.iter_title(72):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Pennsylvania statute converter.

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
                headers={"User-Agent": "Arch/1.0 (Statute Research; contact@axiom-foundation.org)"},
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

    def _build_title_url(self, title: int, chapter: str | None = None) -> str:
        """Build the URL for a title or chapter.

        Args:
            title: Title number (e.g., 72)
            chapter: Optional chapter number (e.g., "3")

        Returns:
            Full URL to the title/chapter page
        """
        url = f"{BASE_URL}/view-statute?txtType=HTM&ttl={title}&iFrame=true"
        if chapter:
            url += f"&chpt={chapter}"
        return url

    def _build_section_url(
        self, title: int, chapter: str | None = None, section: str | None = None
    ) -> str:
        """Build the URL for a specific section.

        Args:
            title: Title number
            chapter: Chapter number
            section: Section number

        Returns:
            Full URL to section page
        """
        url = f"{BASE_URL}/view-statute?txtType=HTM&ttl={title}&iFrame=true"  # pragma: no cover
        if chapter:  # pragma: no cover
            url += f"&chpt={chapter}"  # pragma: no cover
        if section:  # pragma: no cover
            url += f"&sctn={section}"  # pragma: no cover
        return url  # pragma: no cover

    def _extract_section_from_html(
        self,
        html: str,
        title: int,
        section_number: str,
        source_url: str,
    ) -> ParsedPASection | None:
        """Extract a specific section from title HTML.

        Pennsylvania serves entire titles/chapters in one HTML page,
        so we need to locate and extract the specific section.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Look for section anchor patterns like "72c3116s"
        section_anchor = f"{title}c{section_number}s"
        anchor_elem = soup.find("a", {"name": section_anchor})

        # Also try finding by section text pattern
        section_pattern = rf"§\s*{re.escape(section_number)}\."

        # Get title info
        title_name = PA_TITLES.get(title, f"Title {title}")

        # Extract section text - search for the section pattern
        full_text = soup.get_text(separator="\n", strip=True)

        # Find the section start
        section_match = re.search(section_pattern, full_text)
        if not section_match:
            return None

        # Extract section title (text after section number, before period or dash)
        section_start = section_match.start()
        after_section = full_text[section_match.end() :]

        # Title ends at first period followed by newline or double-dash
        title_match = re.match(r"\s*([^.]+?)(?:\.?\s*[-—]|\.\s*\n)", after_section)
        section_title = title_match.group(1).strip() if title_match else f"Section {section_number}"

        # Find section end (next section or end of document)
        next_section = re.search(rf"§\s*\d+\.", after_section)
        if next_section:
            section_text = after_section[: next_section.start()]
        else:
            section_text = after_section

        # Extract chapter info if present
        chapter_number = None
        chapter_title = None
        chapter_match = re.search(
            rf"CHAPTER\s+(\d+[A-Z]?)\s*\n\s*([^\n]+)", full_text[:section_start], re.IGNORECASE
        )
        if chapter_match:
            chapter_number = chapter_match.group(1)
            chapter_title = chapter_match.group(2).strip()

        # Parse subsections
        subsections = self._parse_subsections(section_text)

        # Extract history
        history = None
        history_match = re.search(
            r"(?:History|(?:\d{4}\s+)?Amendment)\s*[.:]?\s*[-—]?\s*(.+?)(?=\n\n|§|Cross References|$)",
            section_text,
            re.IGNORECASE | re.DOTALL,
        )
        if history_match:
            history = history_match.group(1).strip()[:1000]

        # Extract cross-references
        cross_refs = []
        cross_ref_match = re.search(
            r"Cross References\.?\s*(.+?)(?=\n\n|History|$)",
            section_text,
            re.IGNORECASE | re.DOTALL,
        )
        if cross_ref_match:
            ref_text = cross_ref_match.group(1)
            # Extract section references (both "S 3116" and "section 3116" patterns)
            refs = re.findall(r"(?:§|section)\s*(\d+[A-Za-z]?)", ref_text, re.IGNORECASE)
            cross_refs.extend(refs)

        return ParsedPASection(
            section_number=section_number,
            section_title=section_title,
            title_number=title,
            title_name=title_name,
            chapter_number=chapter_number,
            chapter_title=chapter_title,
            text=section_text.strip(),
            html=html,
            subsections=subsections,
            history=history,
            source_url=source_url,
            cross_references=cross_refs,
        )

    def _parse_subsections(self, text: str) -> list[ParsedPASubsection]:
        """Parse hierarchical subsections from text.

        Pennsylvania statutes typically use:
        - (a), (b), (c) for primary divisions with optional heading
        - (1), (2), (3) for secondary divisions
        - (i), (ii), (iii) for tertiary divisions
        """
        subsections = []

        # Split by primary subsections (a), (b), etc.
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (a)
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Check for heading pattern: (a) Heading.--
            heading = None
            heading_match = re.match(r"([A-Za-z][^.]+?)\.\s*[-—]+\s*", content)
            if heading_match:
                heading = heading_match.group(1).strip()
                content = content[heading_match.end() :]

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
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedPASubsection(
                    identifier=identifier,
                    heading=heading,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedPASubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (i), (ii), etc.
            children = self._parse_level3(content)

            # Limit to reasonable size and stop at next letter subsection
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]  # pragma: no cover

            # Get text before first child
            if children:
                first_child_match = re.search(
                    r"\([ivxlc]+\)", content, re.IGNORECASE
                )  # pragma: no cover
                direct_text = (  # pragma: no cover
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            subsections.append(
                ParsedPASubsection(
                    identifier=identifier,
                    heading=None,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedPASubsection]:
        """Parse level 3 subsections (i), (ii), etc."""
        subsections = []
        # Match roman numerals: i, ii, iii, iv, v, vi, vii, viii, ix, x
        parts = re.split(r"(?=\((?:i{1,3}|iv|vi{0,3}|ix|x)\)\s)", text, flags=re.IGNORECASE)

        for part in parts[1:]:
            match = re.match(
                r"\((i{1,3}|iv|vi{0,3}|ix|x)\)\s*", part, re.IGNORECASE
            )  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover

            identifier = match.group(1).lower()  # pragma: no cover
            content = part[match.end() :]  # pragma: no cover

            # Limit size and stop at next subsection
            next_num = re.search(r"\(\d+\)", content)  # pragma: no cover
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]  # pragma: no cover

            next_letter = re.search(r"\([a-z]\)", content)  # pragma: no cover
            if next_letter:  # pragma: no cover
                content = content[: next_letter.start()]  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedPASubsection(
                    identifier=identifier,
                    heading=None,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedPASection) -> Section:
        """Convert ParsedPASection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"PA-{parsed.title_number}-{parsed.section_number}",
        )

        # Convert subsections
        subsections = [
            Subsection(
                identifier=sub.identifier,
                heading=sub.heading,
                text=sub.text,
                children=[
                    Subsection(
                        identifier=child.identifier,
                        heading=child.heading,
                        text=child.text,
                        children=[
                            Subsection(
                                identifier=grandchild.identifier,
                                heading=grandchild.heading,
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
            title_name=f"Pennsylvania Consolidated Statutes - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"pa/{parsed.title_number}/{parsed.section_number}",
            references_to=parsed.cross_references,
        )

    def fetch_section(self, title: str | int, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            title: Title number (e.g., 72 or "72")
            section_number: Section number (e.g., "3116")

        Returns:
            Section model

        Raises:
            PAConverterError: If section not found or parsing fails
        """
        title_num = int(title)
        url = self._build_title_url(title_num)
        html = self._get(url)

        parsed = self._extract_section_from_html(html, title_num, section_number, url)
        if not parsed:
            raise PAConverterError(f"Section {title_num} Pa.C.S. {section_number} not found", url)

        return self._to_section(parsed)

    def get_title_section_numbers(self, title: int) -> list[str]:
        """Get list of section numbers in a title.

        Args:
            title: Title number (e.g., 72)

        Returns:
            List of section numbers (e.g., ["101", "102", "3116", ...])
        """
        url = self._build_title_url(title)
        html = self._get(url)

        # Extract all section numbers from the HTML
        section_numbers = []
        pattern = re.compile(r"§\s*(\d+[A-Za-z]?)\.")

        for match in pattern.finditer(html):
            section_num = match.group(1)
            if section_num not in section_numbers:
                section_numbers.append(section_num)

        return section_numbers

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 72)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_title_section_numbers(title)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(title, section_num)
            except PAConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(
                    f"Warning: Could not fetch {title} Pa.C.S. {section_num}: {e}"
                )  # pragma: no cover
                continue  # pragma: no cover

    def iter_titles(
        self,
        titles: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple titles.

        Args:
            titles: List of title numbers (default: tax titles)

        Yields:
            Section objects
        """
        if titles is None:  # pragma: no cover
            titles = list(PA_TAX_TITLES.keys())  # pragma: no cover

        for title in titles:  # pragma: no cover
            yield from self.iter_title(title)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "PAConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_pa_section(title: int | str, section_number: str) -> Section:
    """Fetch a single Pennsylvania statute section.

    Args:
        title: Title number (e.g., 72)
        section_number: Section number (e.g., "3116")

    Returns:
        Section model
    """
    with PAConverter() as converter:
        return converter.fetch_section(title, section_number)


def download_pa_title(title: int) -> list[Section]:
    """Download all sections from a Pennsylvania Consolidated Statutes title.

    Args:
        title: Title number (e.g., 72)

    Returns:
        List of Section objects
    """
    with PAConverter() as converter:
        return list(converter.iter_title(title))


def download_pa_tax_titles() -> Iterator[Section]:
    """Download all sections from Pennsylvania tax-related titles.

    Yields:
        Section objects
    """
    with PAConverter() as converter:  # pragma: no cover
        yield from converter.iter_titles(list(PA_TAX_TITLES.keys()))  # pragma: no cover


def download_pa_welfare_titles() -> Iterator[Section]:
    """Download all sections from Pennsylvania welfare-related titles.

    Yields:
        Section objects
    """
    with PAConverter() as converter:  # pragma: no cover
        yield from converter.iter_titles(list(PA_WELFARE_TITLES.keys()))  # pragma: no cover
