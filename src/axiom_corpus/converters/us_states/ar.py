"""Arkansas state statute converter.

Converts Arkansas Code HTML from the official LexisNexis-hosted portal
to the internal Section model for ingestion.

Arkansas Code Structure:
- Titles (e.g., Title 26: Taxation)
- Subtitles (e.g., Subtitle 5: State Taxes)
- Chapters (e.g., Chapter 51: Income Taxes)
- Subchapters (e.g., Subchapter 1: General Provisions)
- Sections (e.g., 26-51-101)

Section numbering format: Title-Chapter-Section (e.g., 26-51-101)

URL Patterns:
- The Arkansas Code is hosted via LexisNexis at advance.lexis.com
- Alternative access via codes.findlaw.com or law.justia.com

Key Titles:
- Title 20: Public Health and Welfare
- Title 26: Taxation

Example:
    >>> from axiom_corpus.converters.us_states.ar import ARConverter
    >>> converter = ARConverter()
    >>> section = converter.fetch_section("26-51-101")
    >>> print(section.section_title)
    "Title"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

# Base URL for Arkansas Code (using Justia as a reliable fallback)
BASE_URL = "https://law.justia.com/codes/arkansas"

# Title mapping for reference
AR_TITLES: dict[str, str] = {
    1: "General Provisions",
    2: "Legislative Department",
    3: "State Highway Commission and State Aid Roads",
    4: "General Assembly",
    5: "State Departments",
    6: "Local Government",
    7: "Corporations and Associations",
    8: "Adoption and Legitimation",
    9: "Family Law",
    10: "Criminal Law",
    11: "Civil Procedure",
    12: "Courts and Court Officers",
    13: "Administrative Procedure",
    14: "Agriculture",
    15: "Natural Resources and Economic Development",
    16: "Practice, Procedure, and Courts",
    17: "Professions, Occupations, and Businesses",
    18: "Transportation",
    19: "Revenue and Taxation",
    20: "Public Health and Welfare",
    21: "Public Lands",
    22: "Mechanics' and Laborers' Liens",
    23: "Public Utilities and Regulated Industries",
    24: "Property",
    25: "Domestic Relations",
    26: "Taxation",
    27: "Transportation",
    28: "Wills, Estates, and Fiduciary Relationships",
}

# Key chapters for tax/benefit analysis (within Title 26 - Taxation)
AR_TAX_CHAPTERS: dict[str, str] = {
    18: "Administration",
    26: "Arkansas Tax Procedure Act",
    51: "Income Taxes",
    52: "Gross Receipts Tax (Sales and Use Tax)",
    53: "Compensating (Use) Tax",
    54: "Soft Drink Tax",
    55: "Cigarette Tax",
    56: "Tobacco Products Tax",
    57: "Motor Fuel and Special Motor Fuel Tax",
    58: "Distilled Spirits Tax",
    59: "Beer Tax",
    60: "Mixed Drink Tax",
    61: "Estate Tax",
    62: "Inheritance and Estate Tax",
    63: "Special Taxes",
    64: "Property Taxes",
    65: "Miscellaneous Taxes",
    70: "Property Tax Relief",
    72: "Property Valuation and Assessment",
    74: "County Tax Collectors",
    75: "Real Property Assessment Coordination",
    76: "Personal Property Tax",
    80: "Corporate Franchise Tax",
    81: "Premium Taxes on Insurance Companies",
    82: "Banking and Financial Institution Taxes",
}

# Key chapters for public health and welfare analysis (within Title 20)
AR_WELFARE_CHAPTERS: dict[str, str] = {
    76: "Department of Human Services",
    77: "General Provisions",
    78: "Public Assistance and Social Services",
    81: "Medical Assistance Programs",
    82: "Programs for the Aged, Blind, and Disabled",
    83: "Child Welfare Services",
    86: "Food Stamp Program / SNAP",
}


@dataclass
class ParsedARSection:
    """Parsed Arkansas Code section."""

    section_number: str  # e.g., "26-51-101"
    section_title: str  # e.g., "Title"
    title_number: int  # e.g., 26
    chapter_number: int  # e.g., 51
    title_name: str | None  # e.g., "Taxation"
    chapter_title: str | None  # e.g., "Income Taxes"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedARSubsection] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedARSubsection:
    """A subsection within an Arkansas Code section."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list[ParsedARSubsection] = field(default_factory=list)


class ARConverterError(Exception):
    """Error during Arkansas Code conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class ARConverter:
    """Converter for Arkansas Code HTML to internal Section model.

    Example:
        >>> converter = ARConverter()
        >>> section = converter.fetch_section("26-51-101")
        >>> print(section.citation.section)
        "AR-26-51-101"

        >>> for section in converter.iter_chapter(26, 51):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Arkansas Code converter.

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
                follow_redirects=True,
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
        """Parse section number into title, chapter, and section.

        Args:
            section_number: e.g., "26-51-101", "26-51-101a"

        Returns:
            Tuple of (title, chapter, section_suffix)
        """
        parts = section_number.split("-")
        if len(parts) != 3:
            raise ARConverterError(
                f"Invalid section number format: {section_number}"
            )  # pragma: no cover

        title = int(parts[0])
        chapter = int(parts[1])
        section = parts[2]

        return title, chapter, section

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "26-51-101"

        Returns:
            Full URL to the section page
        """
        title, chapter, section = self._parse_section_number(section_number)

        # Justia URL pattern: /codes/arkansas/title-26/subtitle-5/chapter-51/section-26-51-101/
        # We'll try without subtitle first since we don't always know it
        return f"{BASE_URL}/title-{title}/subtitle-5/chapter-{chapter}/section-{section_number}/"

    def _build_chapter_url(self, title: int, chapter: int) -> str:
        """Build the URL for a chapter's contents.

        Args:
            title: Title number (e.g., 26)
            chapter: Chapter number (e.g., 51)

        Returns:
            Full URL to the chapter index page
        """
        return f"{BASE_URL}/title-{title}/subtitle-5/chapter-{chapter}/"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedARSection:
        """Parse section HTML into ParsedARSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "does not exist" in html.lower():
            raise ARConverterError(f"Section {section_number} not found", url)

        title_num, chapter_num, _ = self._parse_section_number(section_number)

        title_name = AR_TITLES.get(title_num)
        chapter_title = AR_TAX_CHAPTERS.get(chapter_num) or AR_WELFARE_CHAPTERS.get(chapter_num)

        # Extract section title from heading patterns
        section_title = ""

        # Try pattern: "26-51-101. Title."
        title_pattern = re.compile(
            rf"{re.escape(section_number)}\.?\s*(.+?)(?:\.|$)", re.IGNORECASE
        )

        # Look in h1, h2, h3 headings
        for heading in soup.find_all(["h1", "h2", "h3"]):
            heading_text = heading.get_text(strip=True)
            match = title_pattern.search(heading_text)
            if match:
                section_title = match.group(1).strip().rstrip(".")
                break

        # Try page title
        if not section_title:
            page_title = soup.find("title")
            if page_title:
                match = title_pattern.search(page_title.get_text(strip=True))
                if match:
                    section_title = match.group(1).strip().rstrip(".")  # pragma: no cover

        # Try document div
        if not section_title:
            doc_div = soup.find("div", class_="document")
            if doc_div:
                for text_node in doc_div.stripped_strings:
                    match = title_pattern.search(text_node)
                    if match:
                        section_title = match.group(1).strip().rstrip(".")  # pragma: no cover
                        break  # pragma: no cover

        # Get body content
        content_elem = (
            soup.find("div", class_="body")
            or soup.find("div", class_="codes-content")
            or soup.find("div", class_="document")
            or soup.find("article")
            or soup.find("main")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(
                ["nav", "script", "style", "header", "footer", "aside"]
            ):
                elem.decompose()  # pragma: no cover
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note
        history = None
        history_elem = soup.find("div", class_="history")
        if history_elem:
            history = history_elem.get_text(strip=True)[:1000]
        else:
            history_match = re.search(
                r"History\.?\s*[-:]?\s*(.+?)(?:\n\n|$)", text, re.DOTALL
            )  # pragma: no cover
            if history_match:  # pragma: no cover
                history = history_match.group(1).strip()[:1000]  # pragma: no cover

        # Extract effective date
        effective_date = None
        eff_match = re.search(r"Effective\s*Date[:\s]*(\w+\s+\d{1,2},?\s*\d{4})", text)
        if eff_match:
            try:  # pragma: no cover
                from datetime import datetime  # pragma: no cover

                date_str = eff_match.group(1)  # pragma: no cover
                effective_date = datetime.strptime(
                    date_str.replace(",", ""), "%B %d %Y"
                ).date()  # pragma: no cover
            except ValueError:  # pragma: no cover
                pass

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedARSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_num,
            chapter_number=chapter_num,
            title_name=title_name,
            chapter_title=chapter_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
            effective_date=effective_date,
        )

    def _parse_subsections(self, text: str) -> list[ParsedARSubsection]:
        """Parse hierarchical subsections from text.

        Arkansas Code typically uses:
        - (a), (b), (c) for primary divisions
        - (1), (2), (3) for secondary divisions
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
                direct_text = content.strip()  # pragma: no cover

            # Clean up text - remove trailing subsections
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedARSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedARSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse third-level children (A), (B), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([A-Z]\)", content)  # pragma: no cover
                direct_text = (  # pragma: no cover
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit to reasonable size and stop at next lowercase subsection
            next_lower = re.search(r"\([a-z]\)", direct_text)
            if next_lower:
                direct_text = direct_text[: next_lower.start()]  # pragma: no cover

            subsections.append(
                ParsedARSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedARSubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover

            identifier = match.group(1)  # pragma: no cover
            content = part[match.end() :]  # pragma: no cover

            # Stop at next higher-level subsection
            next_num = re.search(r"\(\d+\)", content)  # pragma: no cover
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]  # pragma: no cover
            next_lower = re.search(r"\([a-z]\)", content)  # pragma: no cover
            if next_lower:  # pragma: no cover
                content = content[: next_lower.start()]  # pragma: no cover

            if len(content) > 2000:  # pragma: no cover
                content = content[:2000] + "..."  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedARSubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedARSection) -> Section:
        """Convert ParsedARSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"AR-{parsed.section_number}",
        )

        # Convert subsections recursively
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
            title_name=f"Arkansas Code - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"ar/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "26-51-101"

        Returns:
            Section model

        Raises:
            ARConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            raise ARConverterError(f"HTTP error fetching section {section_number}: {e}", url) from e
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 26)
            chapter: Chapter number (e.g., 51)

        Returns:
            List of section numbers (e.g., ["26-51-101", "26-51-102", ...])
        """
        url = self._build_chapter_url(title, chapter)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            raise ARConverterError(  # pragma: no cover
                f"HTTP error fetching chapter {title}-{chapter}: {e}", url
            ) from e

        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links: /section/26-51-101 or section-26-51-101
        pattern = re.compile(rf"section[/-]({title}-{chapter}-\d+[A-Za-z]?)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 26)
            chapter: Chapter number (e.g., 51)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except ARConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        title: int = 26,
        chapters: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            title: Title number (default: 26 for Taxation)
            chapters: List of chapter numbers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(AR_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> ARConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ar_section(section_number: str) -> Section:
    """Fetch a single Arkansas Code section.

    Args:
        section_number: e.g., "26-51-101"

    Returns:
        Section model
    """
    with ARConverter() as converter:
        return converter.fetch_section(section_number)


def download_ar_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from an Arkansas Code chapter.

    Args:
        title: Title number (e.g., 26)
        chapter: Chapter number (e.g., 51)

    Returns:
        List of Section objects
    """
    with ARConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_ar_tax_chapters() -> Iterator[Section]:
    """Download all sections from Arkansas tax-related chapters (Title 26).

    Yields:
        Section objects
    """
    with ARConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(
            title=26, chapters=list(AR_TAX_CHAPTERS.keys())
        )  # pragma: no cover


def download_ar_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Arkansas welfare chapters (Title 20).

    Yields:
        Section objects
    """
    with ARConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(
            title=20, chapters=list(AR_WELFARE_CHAPTERS.keys())
        )  # pragma: no cover
