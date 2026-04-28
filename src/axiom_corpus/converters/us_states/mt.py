"""Montana state statute converter.

Converts Montana Code Annotated HTML from archive.legmt.gov
to the internal Section model for ingestion.

Montana Statute Structure:
- Titles (e.g., Title 15: Taxation)
- Chapters (e.g., Chapter 30: Individual Income Tax)
- Parts (e.g., Part 21: Rate and General Provisions)
- Sections (e.g., 15-30-2101: Definitions)

URL Patterns:
- Title index: /title_XXXX/chapters_index.html
- Chapter parts: /title_XXXX/chapter_XXXX/parts_index.html
- Part sections: /title_XXXX/chapter_XXXX/part_XXXX/sections_index.html
- Section: /title_XXXX/chapter_XXXX/part_XXXX/section_XXXX/XXXX-XXXX-XXXX-XXXX.html

Section Citation Format:
- Format: TT-CC-PPSS where TT=title, CC=chapter, PP=part, SS=section
- Example: 15-30-2101 means Title 15, Chapter 30, Part 21, Section 01

Example:
    >>> from axiom_corpus.converters.us_states.mt import MTConverter
    >>> converter = MTConverter()
    >>> section = converter.fetch_section("15-30-2101")
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

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://archive.legmt.gov/bills/mca"

# Key chapters for tax/benefit analysis
# Format: (title, chapter) -> name
MT_TAX_CHAPTERS: dict[tuple[int, int], str] = {
    (15, 1): "Administration",
    (15, 6): "Property Subject to Taxation",
    (15, 7): "Exempt Property",
    (15, 8): "Valuation",
    (15, 16): "Tax Levies",
    (15, 17): "Collection of Taxes",
    (15, 23): "Property Tax Assistance",
    (15, 24): "Property Tax Relief",
    (15, 30): "Individual Income Tax",
    (15, 31): "Corporate Income Tax",
    (15, 32): "Estate Tax",
    (15, 35): "Natural Resource Taxes",
    (15, 36): "Metalliferous Mines License Tax",
    (15, 37): "Oil and Gas Production Tax",
    (15, 38): "Coal Severance Tax",
    (15, 39): "Resource Indemnity Trust Tax",
    (15, 50): "Gasoline Tax",
    (15, 51): "Diesel Tax",
    (15, 53): "Alcohol Tax",
    (15, 64): "Tourism",
    (15, 65): "Local Government Fiscal Matters",
    (15, 68): "Nursing Facility Utilization Fee",
    (15, 70): "Lodging Facility Sales and Use Tax",
}

MT_WELFARE_CHAPTERS: dict[tuple[int, int], str] = {
    (53, 1): "Administration",
    (53, 2): "Public Assistance",
    (53, 3): "Relief",
    (53, 4): "Services for Children",
    (53, 5): "Adult Services",
    (53, 6): "Health Care",
    (53, 7): "Vocational Rehabilitation",
    (53, 9): "Crime Victims",
    (53, 10): "Human Services Administration",
    (53, 18): "Persons With Developmental Disabilities",
    (53, 19): "Mental Health",
    (53, 20): "Mental Health",
    (53, 21): "Substance Abuse",
    (53, 24): "Aging Services",
    (53, 25): "Assisted Living Facilities",
    (53, 30): "Corrections",
}


@dataclass
class ParsedMTSection:
    """Parsed Montana statute section."""

    section_number: str  # e.g., "15-30-2101"
    section_title: str  # e.g., "Definitions"
    title_number: int  # e.g., 15
    title_name: str  # e.g., "TAXATION"
    chapter_number: int  # e.g., 30
    chapter_title: str  # e.g., "INDIVIDUAL INCOME TAX"
    part_number: int  # e.g., 21
    part_title: str  # e.g., "Rate and General Provisions"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedMTSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""


@dataclass
class ParsedMTSubsection:
    """A subsection within a Montana statute."""

    identifier: str  # e.g., "1", "a", "i"
    text: str
    children: list["ParsedMTSubsection"] = field(default_factory=list)


class MTConverterError(Exception):
    """Error during Montana statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class MTConverter:
    """Converter for Montana Code Annotated HTML to internal Section model.

    Example:
        >>> converter = MTConverter()
        >>> section = converter.fetch_section("15-30-2101")
        >>> print(section.citation.section)
        "MT-15-30-2101"

        >>> for section in converter.iter_part(15, 30, 21):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Montana statute converter.

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

    def _parse_section_citation(self, citation: str) -> tuple[int, int, int, int]:
        """Parse a Montana section citation into components.

        Args:
            citation: e.g., "15-30-2101"

        Returns:
            Tuple of (title, chapter, part, section)

        Example:
            "15-30-2101" -> (15, 30, 21, 1)
            "15-30-2120" -> (15, 30, 21, 20)
        """
        parts = citation.split("-")
        if len(parts) != 3:
            raise MTConverterError(
                f"Invalid Montana citation format: {citation}"
            )  # pragma: no cover

        title = int(parts[0])
        chapter = int(parts[1])

        # The third part encodes part + section: PPSS
        # e.g., 2101 = Part 21, Section 01
        full_section = int(parts[2])
        part = full_section // 100  # First 2 digits
        section = full_section % 100  # Last 2 digits

        return title, chapter, part, section

    def _build_section_url(self, citation: str) -> str:
        """Build the URL for a section.

        Args:
            citation: e.g., "15-30-2101"

        Returns:
            Full URL to the section page
        """
        title, chapter, part, section = self._parse_section_citation(citation)

        # URL format uses 4-digit padded values multiplied by 10
        title_path = f"title_{title * 10:04d}"
        chapter_path = f"chapter_{chapter * 10:04d}"
        part_path = f"part_{part * 10:04d}"
        section_path = f"section_{section * 10:04d}"

        # File name uses the full citation encoded
        file_name = f"{title * 10:04d}-{chapter * 10:04d}-{part * 10:04d}-{section * 10:04d}.html"

        return f"{BASE_URL}/{title_path}/{chapter_path}/{part_path}/{section_path}/{file_name}"

    def _build_parts_index_url(self, title: int, chapter: int) -> str:
        """Build the URL for a chapter's parts index."""
        title_path = f"title_{title * 10:04d}"
        chapter_path = f"chapter_{chapter * 10:04d}"
        return f"{BASE_URL}/{title_path}/{chapter_path}/parts_index.html"

    def _build_sections_index_url(self, title: int, chapter: int, part: int) -> str:
        """Build the URL for a part's sections index."""
        title_path = f"title_{title * 10:04d}"
        chapter_path = f"chapter_{chapter * 10:04d}"
        part_path = f"part_{part * 10:04d}"
        return f"{BASE_URL}/{title_path}/{chapter_path}/{part_path}/sections_index.html"

    def _parse_section_html(
        self,
        html: str,
        citation: str,
        url: str,
    ) -> ParsedMTSection:
        """Parse section HTML into ParsedMTSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for section content first (most reliable indicator)
        section_content = soup.find("div", class_="section-content")
        if not section_content:
            # Check for "not found" error in the title or body
            title = soup.find("title")
            title_text = title.get_text() if title else ""
            if "not found" in title_text.lower() or "error" in title_text.lower():
                raise MTConverterError(f"Section {citation} not found", url)
            raise MTConverterError(f"Section {citation} content not found", url)  # pragma: no cover

        title, chapter, _, _ = self._parse_section_citation(citation)

        # Extract title name from header
        title_elem = soup.find("h4", class_="section-title-title")
        title_name = ""
        if title_elem:
            text = title_elem.get_text(strip=True)
            # Extract name after "TITLE XX. "
            match = re.search(r"TITLE\s+\d+\.\s*(.+)", text)
            if match:
                title_name = match.group(1).strip()

        # Extract chapter title
        chapter_elem = soup.find("h3", class_="section-chapter-title")
        chapter_title = ""
        if chapter_elem:
            text = chapter_elem.get_text(strip=True)
            match = re.search(r"CHAPTER\s+\d+\.\s*(.+)", text)
            if match:
                chapter_title = match.group(1).strip()

        # Extract part title and number
        part_elem = soup.find("h2", class_="section-part-title")
        part_title = ""
        part_number = 0
        if part_elem:
            text = part_elem.get_text(strip=True)
            match = re.search(r"Part\s+(\d+)\.\s*(.+)", text)
            if match:
                part_number = int(match.group(1))
                part_title = match.group(2).strip()

        # Extract section title
        section_title_elem = soup.find("h1", class_="section-section-title")
        section_title = ""
        if section_title_elem:
            section_title = section_title_elem.get_text(strip=True)

        # If not found in h1, try catchline
        if not section_title:
            catchline = soup.find("span", class_="catchline")  # pragma: no cover
            if catchline:  # pragma: no cover
                text = catchline.get_text(strip=True)  # pragma: no cover
                # Remove citation and period: "15-30-2101. Definitions."
                match = re.search(r"\d+-\d+-\d+\.\s*(.+?)\.?$", text)  # pragma: no cover
                if match:  # pragma: no cover
                    section_title = match.group(1).strip().rstrip(".")  # pragma: no cover

        # Get full text content
        if section_content:
            text = section_content.get_text(separator="\n", strip=True)
            html_content = str(section_content)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note
        history = None
        history_elem = soup.find("div", class_="history-content")
        if history_elem:
            history_text = history_elem.get_text(strip=True)
            # Remove "History:" prefix
            if "History:" in history_text:
                history = history_text.split("History:", 1)[1].strip()
            else:
                history = history_text  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedMTSection(
            section_number=citation,
            section_title=section_title or f"Section {citation}",
            title_number=title,
            title_name=title_name,
            chapter_number=chapter,
            chapter_title=chapter_title,
            part_number=part_number,
            part_title=part_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedMTSubsection]:
        """Parse hierarchical subsections from text.

        Montana statutes typically use:
        - (1), (2), (3) for primary divisions
        - (a), (b), (c) for secondary divisions
        - (i), (ii), (iii) for tertiary divisions (roman numerals)
        """
        subsections = []

        # Split by top-level subsections (1), (2), etc.
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:  # Skip content before first (1)
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse second-level children (a), (b), etc.
            children = self._parse_level2(content)

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

            # Clean up text - remove trailing subsections
            next_subsection = re.search(r"\(\d+\)", direct_text)
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedMTSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedMTSubsection]:
        """Parse level 2 subsections (a), (b), etc."""
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
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]

            subsections.append(
                ParsedMTSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedMTSection) -> Section:
        """Convert ParsedMTSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"MT-{parsed.section_number}",
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
            title_name=f"Montana Code Annotated - Title {parsed.title_number}. {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"mt/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, citation: str) -> Section:
        """Fetch and convert a single section.

        Args:
            citation: e.g., "15-30-2101"

        Returns:
            Section model

        Raises:
            MTConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(citation)
        html = self._get(url)
        parsed = self._parse_section_html(html, citation, url)
        return self._to_section(parsed)

    def get_chapter_parts(self, title: int, chapter: int) -> list[int]:
        """Get list of part numbers in a chapter.

        Args:
            title: Title number (e.g., 15)
            chapter: Chapter number (e.g., 30)

        Returns:
            List of part numbers (e.g., [21, 23, 25])
        """
        url = self._build_parts_index_url(title, chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        parts = []
        # Find links to parts: part_0210/sections_index.html
        pattern = re.compile(r"part_(\d+)/sections_index\.html")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                part_num = int(match.group(1)) // 10  # Convert 0210 -> 21
                if part_num not in parts:
                    parts.append(part_num)

        return parts

    def get_part_section_numbers(self, title: int, chapter: int, part: int) -> list[str]:
        """Get list of section citations in a part.

        Args:
            title: Title number (e.g., 15)
            chapter: Chapter number (e.g., 30)
            part: Part number (e.g., 21)

        Returns:
            List of section citations (e.g., ["15-30-2101", "15-30-2102"])
        """
        url = self._build_sections_index_url(title, chapter, part)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section citations in spans
        for span in soup.find_all("span", class_="citation"):
            citation = span.get_text(strip=True)
            if re.match(r"\d+-\d+-\d+", citation):
                if citation not in section_numbers:
                    section_numbers.append(citation)

        return section_numbers

    def iter_part(self, title: int, chapter: int, part: int) -> Iterator[Section]:
        """Iterate over all sections in a part.

        Args:
            title: Title number (e.g., 15)
            chapter: Chapter number (e.g., 30)
            part: Part number (e.g., 21)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_part_section_numbers(title, chapter, part)

        for citation in section_numbers:
            try:
                yield self.fetch_section(citation)
            except MTConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {citation}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 15)
            chapter: Chapter number (e.g., 30)

        Yields:
            Section objects for each section in all parts
        """
        parts = self.get_chapter_parts(title, chapter)

        for part in parts:
            yield from self.iter_part(title, chapter, part)

    def iter_chapters(
        self,
        chapters: list[tuple[int, int]] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of (title, chapter) tuples (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(MT_TAX_CHAPTERS.keys())  # pragma: no cover

        for title, chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "MTConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_mt_section(citation: str) -> Section:
    """Fetch a single Montana statute section.

    Args:
        citation: e.g., "15-30-2101"

    Returns:
        Section model
    """
    with MTConverter() as converter:
        return converter.fetch_section(citation)


def download_mt_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a Montana Code chapter.

    Args:
        title: Title number (e.g., 15)
        chapter: Chapter number (e.g., 30)

    Returns:
        List of Section objects
    """
    with MTConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_mt_tax_chapters() -> Iterator[Section]:
    """Download all sections from Montana tax-related chapters.

    Yields:
        Section objects
    """
    with MTConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(MT_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_mt_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Montana social services chapters.

    Yields:
        Section objects
    """
    with MTConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(MT_WELFARE_CHAPTERS.keys()))  # pragma: no cover
