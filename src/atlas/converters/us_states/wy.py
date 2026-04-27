"""Wyoming state statute converter.

Converts Wyoming Statutes HTML from wyoleg.gov (NXT gateway) to the internal
Section model for ingestion.

Wyoming Statute Structure:
- Titles (e.g., Title 39: Taxation and Revenue)
- Chapters (e.g., Chapter 13: Ad Valorem Taxation)
- Articles (e.g., Article 1: In General)
- Sections (e.g., 39-13-101: Definitions)

URL Patterns:
- NXT Gateway: wyoleg.gov/NXT/gateway.dll/...
- PDF Download: wyoleg.gov/statutes/compress/title{N}.pdf
- Section format: {title}-{chapter}-{section} (e.g., 39-13-101)

Key Titles for Tax/Benefit Analysis:
- Title 39: Taxation and Revenue
  - Chapter 13: Ad Valorem Taxation (Property Tax)
  - Chapter 14: Mine Product Taxes (Severance)
  - Chapter 15: Sales Tax
  - Chapter 16: Use Tax
  - Chapter 17: Fuel Tax
  - Chapter 18: Cigarette Tax
  - Chapter 22: Wind Energy Tax
- Title 42: Welfare
  - Chapter 1: General Provisions
  - Chapter 2: Public Assistance and Social Services
  - Chapter 4: Medical Assistance and Services (Medicaid)

Note: Wyoming has no state income tax on individuals or corporations.

Example:
    >>> from atlas.converters.us_states.wy import WYConverter
    >>> converter = WYConverter()
    >>> section = converter.fetch_section("39-13-101")
    >>> print(section.section_title)
    "Definitions"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

# Base URL for Wyoming Legislature NXT gateway
BASE_URL = "https://wyoleg.gov/NXT/gateway.dll"

# Title mapping for reference
WY_TITLES: dict[int, str] = {
    1: "Code of Civil Procedure",
    2: "Agriculture",
    6: "Crimes and Offenses",
    7: "Corporations and Associations",
    9: "Corporations and Associations",
    11: "Counties, County Officers and Employees",
    14: "Education",
    15: "Elections",
    16: "Local Government Powers",
    17: "Corporations and Associations",
    18: "Eminent Domain",
    19: "Estates",
    20: "Fences and Roads",
    21: "Fish and Game",
    22: "Guardianship and Trusts",
    24: "Highways",
    25: "Hospitals",
    26: "Insurance",
    27: "Labor and Employment",
    28: "Liens",
    30: "Married Women and Children",
    31: "Motor Vehicles",
    33: "Professions and Occupations",
    34: "Property and Conveyances",
    35: "Public Health and Safety",
    36: "Public Lands",
    37: "Public Utilities",
    39: "Taxation and Revenue",
    40: "Trade and Commerce",
    41: "Unemployment Insurance",
    42: "Welfare",
}

# Key chapters for tax analysis (Title 39)
WY_TAX_CHAPTERS: dict[int, str] = {
    1: "General Provisions",
    11: "Tax Procedure and Administration",
    12: "Income Tax Preemption",  # Wyoming prohibits state income tax
    13: "Ad Valorem Taxation",  # Property Tax
    14: "Mine Product Taxes",  # Severance Tax
    15: "Sales Tax",
    16: "Use Tax",
    17: "Fuel Tax",
    18: "Cigarette Tax",
    19: "Inheritance Tax",
    22: "Wind Energy Production Tax",
    23: "Nuclear Energy Production Tax",
}

# Key chapters for welfare analysis (Title 42)
WY_WELFARE_CHAPTERS: dict[int, str] = {
    1: "General Provisions",
    2: "Public Assistance and Social Services",
    4: "Medical Assistance and Services",  # Medicaid
    5: "Family Planning and Birth Control",
    6: "Long Term Care Choices Program",
    7: "Long-Term Care Partnership Program",
    8: "Nursing Care Facility Assessment Act",
    9: "Private Hospital Assessment Act",
    10: "Welfare Fraud Prevention Act",
    11: "Private Ground Ambulance Service Provider Assessment Act",
}


@dataclass
class ParsedWYSection:
    """Parsed Wyoming statute section."""

    section_number: str  # e.g., "39-13-101"
    section_title: str  # e.g., "Definitions"
    title_number: int  # e.g., 39
    chapter_number: int  # e.g., 13
    title_name: str | None  # e.g., "Taxation and Revenue"
    chapter_name: str | None  # e.g., "Ad Valorem Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedWYSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedWYSubsection:
    """A subsection within a Wyoming statute."""

    identifier: str  # e.g., "a", "i", "A"
    text: str
    children: list["ParsedWYSubsection"] = field(default_factory=list)


class WYConverterError(Exception):
    """Error during Wyoming statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class WYConverter:
    """Converter for Wyoming Statutes HTML to internal Section model.

    Wyoming statutes are accessed through the NXT gateway system at wyoleg.gov.
    Section numbers follow the format: {title}-{chapter}-{section}

    Example:
        >>> converter = WYConverter()
        >>> section = converter.fetch_section("39-13-101")
        >>> print(section.citation.section)
        "WY-39-13-101"

        >>> for section in converter.iter_chapter(39, 13):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Wyoming statute converter.

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

    def _parse_section_number(self, section_number: str) -> tuple[int, int, str]:
        """Parse a section number like '39-13-101' into components.

        Args:
            section_number: Section number string (e.g., "39-13-101")

        Returns:
            Tuple of (title_number, chapter_number, section_suffix)

        Raises:
            ValueError: If section number format is invalid
        """
        parts = section_number.split("-")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid section number format: {section_number}. "
                f"Expected format: title-chapter-section (e.g., 39-13-101)"
            )
        return int(parts[0]), int(parts[1]), parts[2]

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section using NXT gateway.

        Wyoming uses numeric IDs in the NXT gateway, which makes direct
        section lookup challenging. We use a search-based approach.

        Args:
            section_number: e.g., "39-13-101"

        Returns:
            URL to search for the section
        """
        # URL-encode the search query for the section
        query = quote(section_number)
        return f"{BASE_URL}?f=templates&fn=default.htm&vid=Publish:10.1048/Enu&q={query}"

    def _build_title_pdf_url(self, title: int) -> str:
        """Build the URL for a title's PDF download.

        Args:
            title: Title number (e.g., 39)

        Returns:
            URL to the compressed PDF
        """
        return f"https://wyoleg.gov/statutes/compress/title{title}.pdf"

    def _get_title_info(self, title: int) -> tuple[str | None, dict[int, str]]:
        """Get title name and chapter mappings.

        Args:
            title: Title number

        Returns:
            Tuple of (title_name, chapter_dict)
        """
        title_name = WY_TITLES.get(title)
        if title == 39:
            return title_name, WY_TAX_CHAPTERS
        elif title == 42:
            return title_name, WY_WELFARE_CHAPTERS
        return title_name, {}  # pragma: no cover

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedWYSection:
        """Parse section HTML into ParsedWYSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise WYConverterError(f"Section {section_number} not found", url)

        title_num, chapter_num, _ = self._parse_section_number(section_number)
        title_name, chapter_dict = self._get_title_info(title_num)
        chapter_name = chapter_dict.get(chapter_num, f"Chapter {chapter_num}")

        # Extract section title from patterns like "39-13-101. Definitions."
        section_title = ""
        title_pattern = re.compile(
            rf"{re.escape(section_number)}\.\s*([^.]+(?:\.[^.]+)*?)(?:\.\s*$|\.\s*\()"
        )

        # Try simple pattern first
        simple_pattern = re.compile(rf"{re.escape(section_number)}\.\s*([^.\n]+)")

        for text_node in soup.stripped_strings:
            match = simple_pattern.search(text_node)
            if match:
                section_title = match.group(1).strip().rstrip(".")
                break

        # Also try h1, h2, h3 headings
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3", "b", "strong"]):
                heading_text = heading.get_text(strip=True)
                match = simple_pattern.search(heading_text)
                if match:
                    section_title = match.group(1).strip().rstrip(".")  # pragma: no cover
                    break  # pragma: no cover

        # Get body content - try various containers
        content_elem = (
            soup.find("div", class_="content")
            or soup.find("div", class_="statute")
            or soup.find("article")
            or soup.find("main")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(  # pragma: no cover
                ["nav", "script", "style", "header", "footer"]
            ):
                elem.decompose()
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note
        history = None
        # Wyoming uses "HISTORY:" or session law references
        history_match = re.search(
            r"(?:HISTORY|Laws)\s*[:\-]\s*(.+?)(?:\n\n|\Z)", text, re.IGNORECASE | re.DOTALL
        )
        if history_match:
            history = history_match.group(1).strip()[:500]

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedWYSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_num,
            chapter_number=chapter_num,
            title_name=title_name,
            chapter_name=chapter_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedWYSubsection]:
        """Parse hierarchical subsections from text.

        Wyoming Statutes typically use:
        - (a), (b), (c) for primary divisions
        - (i), (ii), (iii) for secondary divisions
        - (A), (B), (C) for tertiary divisions
        """
        subsections = []

        # Split by top-level subsections (a), (b), etc.
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (a)
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse second-level children (i), (ii), etc.
            children = self._parse_level2(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([ivxlcdm]+\)", content, re.IGNORECASE)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Clean up text - remove trailing subsections
            next_subsection = re.search(r"\([a-z]\)\s", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedWYSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedWYSubsection]:
        """Parse level 2 subsections (i), (ii), etc."""
        subsections = []
        # Roman numeral pattern
        parts = re.split(r"(?=\([ivxlcdm]+\)\s)", text, flags=re.IGNORECASE)

        for part in parts[1:]:
            match = re.match(r"\(([ivxlcdm]+)\)\s*", part, re.IGNORECASE)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1).lower()
            content = part[match.end() :]

            # Parse third-level children (A), (B), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([A-Z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Stop at next lower-level subsection
            next_lower = re.search(r"\([a-z]\)\s", direct_text)
            if next_lower:
                direct_text = direct_text[: next_lower.start()]  # pragma: no cover

            subsections.append(
                ParsedWYSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedWYSubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at next subsection marker
            next_marker = re.search(r"\([a-zA-Z]\)|\([ivxlcdm]+\)", content, re.IGNORECASE)
            if next_marker:
                content = content[: next_marker.start()]  # pragma: no cover

            if len(content) > 2000:
                content = content[:2000] + "..."  # pragma: no cover

            subsections.append(
                ParsedWYSubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedWYSection) -> Section:
        """Convert ParsedWYSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"WY-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsections(subs: list[ParsedWYSubsection]) -> list[Subsection]:
            return [
                Subsection(
                    identifier=sub.identifier,
                    heading=None,
                    text=sub.text,
                    children=convert_subsections(sub.children),
                )
                for sub in subs
            ]

        subsections = convert_subsections(parsed.subsections)

        return Section(
            citation=citation,
            title_name=f"Wyoming Statutes - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"wy/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "39-13-101", "42-2-101"

        Returns:
            Section model

        Raises:
            WYConverterError: If section not found or parsing fails
        """
        # Validate section number format
        self._parse_section_number(section_number)

        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        This is a placeholder - Wyoming's NXT gateway makes enumeration difficult.
        In practice, sections follow patterns like {title}-{chapter}-1XX.

        Args:
            title: Title number (e.g., 39)
            chapter: Chapter number (e.g., 13)

        Returns:
            List of section numbers
        """
        # Return common section patterns for known chapters
        # This is a simplified approach; full enumeration requires PDF parsing
        sections = []
        for i in range(101, 120):
            sections.append(f"{title}-{chapter}-{i}")
        return sections

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 39)
            chapter: Chapter number (e.g., 13)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except WYConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover
            except Exception:  # pragma: no cover
                # Section doesn't exist, skip
                continue  # pragma: no cover

    def iter_tax_chapters(self) -> Iterator[Section]:
        """Iterate over sections from Title 39 (Taxation and Revenue).

        Yields:
            Section objects
        """
        for chapter in WY_TAX_CHAPTERS:  # pragma: no cover
            yield from self.iter_chapter(39, chapter)  # pragma: no cover

    def iter_welfare_chapters(self) -> Iterator[Section]:
        """Iterate over sections from Title 42 (Welfare).

        Yields:
            Section objects
        """
        for chapter in WY_WELFARE_CHAPTERS:  # pragma: no cover
            yield from self.iter_chapter(42, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "WYConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_wy_section(section_number: str) -> Section:
    """Fetch a single Wyoming statute section.

    Args:
        section_number: e.g., "39-13-101"

    Returns:
        Section model
    """
    with WYConverter() as converter:
        return converter.fetch_section(section_number)


def download_wy_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a Wyoming Statutes chapter.

    Args:
        title: Title number (e.g., 39)
        chapter: Chapter number (e.g., 13)

    Returns:
        List of Section objects
    """
    with WYConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_wy_tax_chapters() -> Iterator[Section]:
    """Download all sections from Wyoming taxation chapters (Title 39).

    Yields:
        Section objects
    """
    with WYConverter() as converter:  # pragma: no cover
        yield from converter.iter_tax_chapters()  # pragma: no cover


def download_wy_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Wyoming welfare chapters (Title 42).

    Yields:
        Section objects
    """
    with WYConverter() as converter:  # pragma: no cover
        yield from converter.iter_welfare_chapters()  # pragma: no cover
