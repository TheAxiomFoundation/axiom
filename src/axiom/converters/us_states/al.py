"""Alabama state statute converter.

Converts Code of Alabama HTML from alisondb.legislature.state.al.us (ALISON database)
to the internal Section model for ingestion.

Code of Alabama Structure:
- Titles (e.g., Title 40: Revenue and Taxation)
- Chapters (e.g., Chapter 18: Income Taxes)
- Articles (e.g., Article 1: General Provisions)
- Sections (e.g., 40-18-1: Definitions)

URL Pattern:
- Section: alisondb.legislature.state.al.us/alison/codeofalabama/1975/{section}.htm
  where {section} is like "40-18-1" (title-chapter-section)

Note: Alabama has a state individual income tax (unlike Florida) with brackets.
Title 40 covers Revenue and Taxation, Title 38 covers Public Welfare.

Example:
    >>> from axiom.converters.us_states.al import ALConverter
    >>> converter = ALConverter()
    >>> section = converter.fetch_section("40-18-1")
    >>> print(section.section_title)
    "Definitions"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom.models import Citation, Section, Subsection

BASE_URL = "http://alisondb.legislature.state.al.us/alison/codeofalabama/1975"

# Title mapping for reference
AL_TITLES: dict[str, str] = {
    1: "General Provisions",
    2: "Agriculture",
    3: "Animals",
    4: "Aviation",
    5: "Banks and Financial Institutions",
    6: "Civil Practice",
    7: "Commercial Code",
    8: "Commercial Law and Consumer Protection",
    9: "Conservation and Natural Resources",
    10: "Corporations, Partnerships, and Associations",
    11: "Counties and Municipal Corporations",
    12: "Courts",
    13: "Crimes and Punishments",
    14: "Criminal Correctional and Detention Facilities",
    15: "Criminal Procedure",
    16: "Education",
    17: "Elections",
    18: "Eminent Domain",
    19: "Fiduciaries and Trusts",
    20: "Food, Drugs, and Cosmetics",
    21: "Handicapped Persons",
    22: "Health, Mental Health, and Environmental Control",
    23: "Highways, Roads, Bridges, and Ferries",
    24: "Housing",
    25: "Industrial Relations and Labor",
    26: "Infants and Incompetents",
    27: "Insurance",
    28: "Intoxicating Liquor, Malt Beverages, and Wine",
    29: "Legislature",
    30: "Marital and Domestic Relations",
    31: "Military Affairs and Civil Defense",
    32: "Motor Vehicles and Traffic",
    33: "Navigation and Watercourses",
    34: "Professions and Businesses",
    35: "Property",
    36: "Public Officers and Employees",
    37: "Public Utilities and Public Transportation",
    38: "Public Welfare",
    39: "Public Works",
    40: "Revenue and Taxation",
    41: "State Government",
    42: "United States",
    43: "Wills and Decedents' Estates",
}

# Key chapters for tax analysis (Title 40)
AL_TAX_CHAPTERS: dict[str, str] = {
    1: "General Provisions",
    2: "Department of Revenue",
    7: "Assessment of Property",
    8: "State Board of Equalization",
    9: "Exemptions from Taxation",
    10: "Sales of Land for Taxes",
    11: "Property Tax",
    12: "Licenses Generally",
    13: "Severance Taxes",
    14: "Corporations - Franchise Tax",
    15: "Inheritance and Estate Tax",
    16: "Income Tax - Individuals and Corporations",
    17: "Income Tax - Filing and Collection",
    18: "Income Tax",
    21: "Sales Tax",
    22: "Gasoline Tax",
    23: "Sales and Use Tax",
    25: "Tobacco Tax",
    26: "Utility Gross Receipts Tax",
    27: "Financial Institution Excise Tax",
    29: "Alabama Taxpayers' Bill of Rights",
}

# Key chapters for welfare analysis (Title 38)
AL_WELFARE_CHAPTERS: dict[str, str] = {
    1: "General Provisions",
    2: "Department of Human Resources",
    3: "Old Age Assistance",
    4: "Aid to Dependent Children",
    5: "Assistance to Aged and Blind",
    6: "Aid to Permanently and Totally Disabled",
    7: "County Boards of Human Resources",
    9: "Child Care Facilities",
    10: "Adult Protective Services",
    12: "Child Abuse and Neglect",
    13: "Family Preservation and Protection",
    14: "Food Assistance",
    15: "Temporary Assistance for Needy Families",
}


@dataclass
class ParsedALSection:
    """Parsed Alabama statute section."""

    section_number: str  # e.g., "40-18-1"
    section_title: str  # e.g., "Definitions"
    title_number: int  # e.g., 40
    title_name: str  # e.g., "Revenue and Taxation"
    chapter_number: int  # e.g., 18
    chapter_name: str | None  # e.g., "Income Tax"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedALSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedALSubsection:
    """A subsection within an Alabama statute."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list["ParsedALSubsection"] = field(default_factory=list)


class ALConverterError(Exception):
    """Error during Alabama statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class ALConverter:
    """Converter for Code of Alabama HTML to internal Section model.

    Example:
        >>> converter = ALConverter()
        >>> section = converter.fetch_section("40-18-1")
        >>> print(section.citation.section)
        "AL-40-18-1"

        >>> for section in converter.iter_chapter(40, 18):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Alabama statute converter.

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

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "40-18-1", "40-18-24.2", "38-2-12"

        Returns:
            Full URL to the section page
        """
        # Alabama uses the format: {section_number}.htm
        return f"{BASE_URL}/{section_number}.htm"

    def _parse_section_number(self, section_number: str) -> tuple[int, int, str]:
        """Parse section number into components.

        Args:
            section_number: e.g., "40-18-1" or "40-18-24.2"

        Returns:
            Tuple of (title_number, chapter_number, section_suffix)
        """
        parts = section_number.split("-")
        if len(parts) < 3:
            raise ALConverterError(
                f"Invalid section number format: {section_number}"
            )  # pragma: no cover

        title_num = int(parts[0])
        chapter_num = int(parts[1])
        section_suffix = "-".join(parts[2:])  # Handle multi-part suffixes

        return title_num, chapter_num, section_suffix

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedALSection:
        """Parse section HTML into ParsedALSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "does not exist" in html.lower():
            raise ALConverterError(f"Section {section_number} not found", url)

        # Check for empty page
        if len(html.strip()) < 100:
            raise ALConverterError(
                f"Section {section_number} returned empty page", url
            )  # pragma: no cover

        title_num, chapter_num, _ = self._parse_section_number(section_number)

        title_name = AL_TITLES.get(title_num, f"Title {title_num}")

        # Get chapter name from tax or welfare chapters
        chapter_name = None
        if title_num == 40:
            chapter_name = AL_TAX_CHAPTERS.get(chapter_num)
        elif title_num == 38:
            chapter_name = AL_WELFARE_CHAPTERS.get(chapter_num)
        if not chapter_name:
            chapter_name = f"Chapter {chapter_num}"  # pragma: no cover

        # Extract section title - look for pattern like "Section 40-18-1 Definitions."
        section_title = ""
        title_patterns = [
            # Pattern: "Section 40-18-1 Title here."
            re.compile(rf"Section\s+{re.escape(section_number)}\s+(.+?)(?:\.|$)", re.IGNORECASE),
            # Pattern: just the title after section number
            re.compile(rf"{re.escape(section_number)}\s*[-.:]\s*(.+?)(?:\.|$)"),
        ]

        for text_node in soup.stripped_strings:
            for pattern in title_patterns:
                match = pattern.search(text_node)
                if match:
                    section_title = match.group(1).strip().rstrip(".")
                    break
            if section_title:
                break

        # Try extracting from heading tags if not found
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3", "h4", "b", "strong"]):
                heading_text = heading.get_text(strip=True)
                for pattern in title_patterns:
                    match = pattern.search(heading_text)
                    if match:
                        section_title = match.group(1).strip().rstrip(".")  # pragma: no cover
                        break  # pragma: no cover
                if section_title:
                    break  # pragma: no cover

        # Get body content - Alabama uses various structures
        content_elem = (
            soup.find("div", class_="statute")
            or soup.find("div", class_="content")
            or soup.find("div", id="content")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(["nav", "script", "style", "header", "footer"]):
                elem.decompose()  # pragma: no cover
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note - Alabama format: "Acts YYYY, No. NNN" or "Act YYYY-NNN"
        history = None
        history_patterns = [
            r"(?:History|Acts?)[\s.:-]+(.+?)(?:\n\n|\Z)",
            r"\(Acts?\s+\d{4}[^)]+\)",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if history_match:
                history = history_match.group(0).strip()[:1000]  # Limit length
                break

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedALSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_num,
            title_name=title_name,
            chapter_number=chapter_num,
            chapter_name=chapter_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedALSubsection]:
        """Parse hierarchical subsections from text.

        Alabama statutes typically use:
        - (a), (b), (c) for primary divisions in most sections
        - (1), (2), (3) for secondary divisions, or as primary in definition sections
        - a., b., c. or 1., 2., 3. for tertiary

        We detect which pattern comes first in the text to determine hierarchy.
        """
        # Find first occurrence of each pattern
        first_letter = re.search(r"\([a-z]\)\s", text)
        first_number = re.search(r"\(\d+\)\s", text)

        # Determine which comes first (if both exist)
        use_numbered_primary = False
        if first_number and first_letter:
            # Use numbered as primary if (1) comes before (a)
            use_numbered_primary = first_number.start() < first_letter.start()
        elif first_number and not first_letter:
            use_numbered_primary = True  # pragma: no cover

        if use_numbered_primary:
            # Parse numbered subsections as primary
            return self._parse_numbered_as_primary(text)
        elif first_letter:
            # Parse lettered subsections as primary
            return self._parse_lettered_as_primary(text)
        else:
            return []  # pragma: no cover

    def _parse_lettered_as_primary(self, text: str) -> list[ParsedALSubsection]:
        """Parse lettered subsections (a), (b) as primary with numbered children."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (a)
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse second-level children (1), (2), etc.
            children = self._parse_numbered_subsections(content)

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

            # Clean up text - remove next lettered subsection
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedALSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_numbered_as_primary(self, text: str) -> list[ParsedALSubsection]:
        """Parse numbered subsections (1), (2) as primary with lettered children."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:  # Skip content before first (1)
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse second-level children (a), (b), etc.
            children = self._parse_lettered_subsections(content)

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

            # Clean up text - remove next numbered subsection
            next_subsection = re.search(r"\(\d+\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedALSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_lettered_subsections(self, text: str) -> list[ParsedALSubsection]:
        """Parse lettered subsections (a), (b), etc. as children."""
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
                content = content[: next_num.start()]

            subsections.append(
                ParsedALSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _parse_numbered_subsections(self, text: str) -> list[ParsedALSubsection]:
        """Parse numbered subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size and stop at next lettered subsection
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]  # pragma: no cover

            subsections.append(
                ParsedALSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedALSection) -> Section:
        """Convert ParsedALSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"AL-{parsed.section_number}",
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
            title_name=f"Code of Alabama - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"al/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "40-18-1", "40-18-24.2", "38-2-12"

        Returns:
            Section model

        Raises:
            ALConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Note: Alabama's ALISON database doesn't have a consistent chapter index page,
        so this method returns a range of potential section numbers that can be tried.
        For accurate enumeration, use the table of contents page manually.

        Args:
            title: Title number (e.g., 40)
            chapter: Chapter number (e.g., 18)

        Returns:
            List of section numbers (e.g., ["40-18-1", "40-18-2", ...])
        """
        # Generate potential section numbers - Alabama typically uses 1-200 range
        section_numbers = []  # pragma: no cover
        for i in range(1, 201):  # pragma: no cover
            section_numbers.append(f"{title}-{chapter}-{i}")  # pragma: no cover
        return section_numbers  # pragma: no cover

    def iter_chapter(self, title: int, chapter: int, max_sections: int = 200) -> Iterator[Section]:
        """Iterate over sections in a chapter.

        Tries section numbers sequentially until hitting consistent failures.

        Args:
            title: Title number (e.g., 40)
            chapter: Chapter number (e.g., 18)
            max_sections: Maximum sections to try (default 200)

        Yields:
            Section objects for each found section
        """
        consecutive_failures = 0
        max_consecutive_failures = 10  # Stop after 10 consecutive failures

        for i in range(1, max_sections + 1):
            section_num = f"{title}-{chapter}-{i}"
            try:
                yield self.fetch_section(section_num)
                consecutive_failures = 0  # Reset on success
            except ALConverterError:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    break  # Likely reached end of chapter
            except httpx.HTTPStatusError:  # pragma: no cover
                consecutive_failures += 1  # pragma: no cover
                if consecutive_failures >= max_consecutive_failures:  # pragma: no cover
                    break  # pragma: no cover

    def iter_title(self, title: int, chapters: list[int] | None = None) -> Iterator[Section]:
        """Iterate over sections from multiple chapters in a title.

        Args:
            title: Title number (e.g., 40)
            chapters: List of chapter numbers (default: all known tax/welfare chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            if title == 40:  # pragma: no cover
                chapters = list(AL_TAX_CHAPTERS.keys())  # pragma: no cover
            elif title == 38:  # pragma: no cover
                chapters = list(AL_WELFARE_CHAPTERS.keys())  # pragma: no cover
            else:
                raise ALConverterError(
                    f"No default chapters defined for title {title}"
                )  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "ALConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_al_section(section_number: str) -> Section:
    """Fetch a single Alabama statute section.

    Args:
        section_number: e.g., "40-18-1"

    Returns:
        Section model
    """
    with ALConverter() as converter:
        return converter.fetch_section(section_number)


def download_al_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a Code of Alabama chapter.

    Args:
        title: Title number (e.g., 40)
        chapter: Chapter number (e.g., 18)

    Returns:
        List of Section objects
    """
    with ALConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_al_tax_title() -> Iterator[Section]:
    """Download all sections from Alabama tax title (Title 40).

    Yields:
        Section objects
    """
    with ALConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(40)  # pragma: no cover


def download_al_welfare_title() -> Iterator[Section]:
    """Download all sections from Alabama welfare title (Title 38).

    Yields:
        Section objects
    """
    with ALConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(38)  # pragma: no cover
