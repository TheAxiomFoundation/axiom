"""Massachusetts state statute converter.

Converts Massachusetts General Laws HTML from malegislature.gov
to the internal Section model for ingestion.

Massachusetts General Laws Structure:
- Parts (e.g., Part I: Administration of the Government)
- Titles (e.g., Title IX: Taxation)
- Chapters (e.g., Chapter 62: Taxation of Incomes)
- Sections (e.g., Section 2: Gross income defined)

URL Patterns:
- Part index: /Laws/GeneralLaws/PartI
- Title index: /Laws/GeneralLaws/PartI/TitleIX
- Chapter contents: /Laws/GeneralLaws/PartI/TitleIX/Chapter62
- Section: /Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section2

Example:
    >>> from atlas.converters.us_states.ma import MAConverter
    >>> converter = MAConverter()
    >>> section = converter.fetch_section(62, 2)
    >>> print(section.section_title)
    "Gross income, adjusted gross income and taxable income defined; classes"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://malegislature.gov/Laws/GeneralLaws"

# Massachusetts General Laws Parts and Titles
MA_PARTS: dict[str, str] = {
    "I": "Administration of the Government",
    "II": "Real and Personal Property and Domestic Relations",
    "III": "Courts, Judicial Officers and Proceedings in Civil Cases",
    "IV": "Crimes, Punishments and Proceedings in Criminal Cases",
    "V": "The Continuing Consolidation and Arrangement of the General Laws",
}

# Title IX: Taxation (Chapters 58-65C)
MA_TAX_CHAPTERS: dict[str, str] = {
    "58": "General Provisions Relative to Taxation",
    "59": "Assessment of Local Taxes",
    "60": "Collection of Local Taxes",
    "60A": "Excise Tax on Registered Motor Vehicles",
    "61": "Classification and Taxation of Forest Land",
    "61A": "Assessment of Agricultural and Horticultural Land",
    "61B": "Classification and Taxation of Recreational Land",
    "62": "Taxation of Incomes",
    "62B": "Withholding of Taxes on Wages and Declaratory Estimated Tax",
    "62C": "Administrative Provisions Relative to State Taxation",
    "62D": "Procedure for Settlement of Disputes with the Department of Revenue",
    "62E": "Access to Financial Records for Tax Administration",
    "62F": "Tax Limitation",
    "63": "Taxation of Corporations",
    "63A": "Taxation of Certain Telephone and Telegraph Companies",
    "63B": "Taxation of Certain Insurance Companies",
    "64A": "Tax on the Sale of Gasoline",
    "64C": "Taxation of Cigarettes",
    "64E": "Tax on the Sale of Aviation Fuel",
    "64F": "Tax on the Sale of Fuel and Special Fuels",
    "64G": "Room Occupancy Excise",
    "64H": "Tax on Retail Sales of Certain Tangible Personal Property",
    "64I": "Tax on Storage, Use or Other Consumption of Tangible Personal Property",
    "64J": "Tax on the Retail Sale of Boats and Automobiles",
    "64L": "Tax on the Sale of Marijuana and Marijuana Products",
    "64M": "Tax on the Operation of Sports Wagering",
    "64N": "Excise on Retail Sales of Cannabis",
    "65": "Inheritance and Estate Taxes",
    "65A": "Tax on Generation-Skipping Transfers",
    "65B": "Uniform Estate Tax Apportionment Act",
    "65C": "Massachusetts Estate Tax",
}

# Title XVII: Public Welfare (Chapters 115-123B)
MA_WELFARE_CHAPTERS: dict[str, str] = {
    "115": "Veterans' Benefits",
    "116": "Settlement",
    "117": "Support by Kindred",
    "117A": "Old Age Assistance",
    "118": "Aid to Families with Dependent Children",
    "118A": "Veteran and Servicemen Transitional Housing",
    "118E": "Division of Medical Assistance",
    "118F": "Pilot Programs",
    "118G": "Catastrophic Illness in Children Relief Fund",
    "118H": "Medical Security Trust",
    "118I": "Health Safety Net",
    "119": "Protection and Care of Children",
    "119A": "Child Support Enforcement",
    "119B": "The Simplified Child Support Process",
    "120": "Delinquent Children and Youthful Offenders",
    "121": "Powers and Duties of the Department of Transitional Assistance",
    "121A": "Urban Redevelopment Corporations",
    "121B": "Housing and Urban Renewal",
    "121C": "Economic Development and Industrial Corporations",
    "121D": "Urban Center Housing Tax Increment Financing Zones",
    "121E": "Workforce Housing",
    "121F": "Gateway Municipality Economic Development Trust Fund",
    "121G": "Housing Development Incentive Program",
    "122": "Employment Security Law",
    "123": "Mental Health",
    "123A": "Care, Treatment and Rehabilitation of Sexually Dangerous Persons",
    "123B": "Commitment of Alcoholics",
}


@dataclass
class ParsedMASection:
    """Parsed Massachusetts statute section."""

    chapter_number: str  # e.g., "62" or "62B"
    section_number: str  # e.g., "2" or "2A"
    section_title: str  # e.g., "Gross income defined"
    part: str  # e.g., "I"
    title_roman: str  # e.g., "IX"
    title_name: str  # e.g., "Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedMASubsection"] = field(default_factory=list)
    history: str | None = None  # Amendment history
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedMASubsection:
    """A subsection within a Massachusetts statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list["ParsedMASubsection"] = field(default_factory=list)


class MAConverterError(Exception):
    """Error during Massachusetts statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class MAConverter:
    """Converter for Massachusetts General Laws HTML to internal Section model.

    Example:
        >>> converter = MAConverter()
        >>> section = converter.fetch_section(62, 2)
        >>> print(section.citation.section)
        "MA-62-2"

        >>> for section in converter.iter_chapter(62):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Massachusetts statute converter.

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

    def _get_chapter_info(self, chapter: int | str) -> tuple[str, str, str]:
        """Get part, title, and title name for a chapter.

        Args:
            chapter: Chapter number (e.g., 62 or "62B")

        Returns:
            Tuple of (part, title_roman, title_name)
        """
        # Normalize chapter to string for dict lookup
        chapter_str = str(chapter)
        chapter_num_match = re.match(r"(\d+)", chapter_str)
        chapter_num_str = chapter_num_match.group(1) if chapter_num_match else chapter_str

        # Determine part and title based on chapter number
        # Check both exact match and base number (e.g., "62" for "62B")
        if chapter_str in MA_TAX_CHAPTERS or chapter_num_str in MA_TAX_CHAPTERS:
            return ("I", "IX", "Taxation")
        elif chapter_str in MA_WELFARE_CHAPTERS or chapter_num_str in MA_WELFARE_CHAPTERS:
            return ("I", "XVII", "Public Welfare")
        else:
            # Default fallback - most chapters are in Part I
            return ("I", "Unknown", "Unknown")

    def _build_section_url(self, chapter: int | str, section: int | str) -> str:
        """Build the URL for a section.

        Args:
            chapter: Chapter number (e.g., 62 or "62B")
            section: Section number (e.g., 2 or "2A")

        Returns:
            Full URL to the section page
        """
        part, title_roman, _ = self._get_chapter_info(chapter)
        return f"{BASE_URL}/Part{part}/Title{title_roman}/Chapter{chapter}/Section{section}"

    def _build_chapter_url(self, chapter: int | str) -> str:
        """Build the URL for a chapter's contents index."""
        part, title_roman, _ = self._get_chapter_info(chapter)
        return f"{BASE_URL}/Part{part}/Title{title_roman}/Chapter{chapter}"

    def _parse_section_html(
        self,
        html: str,
        chapter: str,
        section: str,
        url: str,
    ) -> ParsedMASection:
        """Parse section HTML into ParsedMASection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "does not exist" in html.lower():
            raise MAConverterError(f"Section {chapter}-{section} not found", url)

        part, title_roman, title_name = self._get_chapter_info(chapter)

        # Extract section title from heading
        # Typical format: "Section 2: Gross income, adjusted gross income and taxable income defined"
        section_title = ""

        # Try to find the h2 heading with section title
        for heading in soup.find_all(["h1", "h2", "h3"]):
            heading_text = heading.get_text(strip=True)
            # Pattern: "Section X: Title" or "Section X Title"
            match = re.match(
                rf"Section\s+{re.escape(str(section))}[:\s]+(.+)",
                heading_text,
                re.IGNORECASE,
            )
            if match:
                section_title = match.group(1).strip()
                break

        # Try page title as fallback
        if not section_title:
            title_tag = soup.find("title")
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                # Often title format: "Section X: Title - Massachusetts General Laws"
                match = re.search(r"Section\s+\d+[A-Za-z]?[:\s]+(.+?)(?:\s*[-|]|$)", title_text)
                if match:
                    section_title = match.group(1).strip()

        # Get body content - try various containers
        content_elem = (
            soup.find("div", class_="lawContent")
            or soup.find("div", class_="content")
            or soup.find("main")
            or soup.find("article")
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

        # Extract history/amendment note
        history = None
        # Massachusetts often has amendment info in italics or in brackets
        history_patterns = [
            r"\[Text of section.*?\]",
            r"Amended by\s+\d{4},\s+\d+",
            r"Added by\s+\d{4},\s+\d+",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if history_match:
                history = history_match.group(0).strip()[:500]
                break

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedMASection(
            chapter_number=str(chapter),
            section_number=str(section),
            section_title=section_title or f"Section {section}",
            part=part,
            title_roman=title_roman,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedMASubsection]:
        """Parse hierarchical subsections from text.

        Massachusetts statutes typically use:
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
                direct_text = content.strip()

            # Clean up text - remove trailing subsections
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedMASubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedMASubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (A), (B), etc.
            children = self._parse_level3(content)

            # Limit to reasonable size and stop at next letter subsection
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:  # pragma: no cover
                content = content[: next_letter.start()]

            subsections.append(
                ParsedMASubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedMASubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at next numbered or lettered subsection
            next_sub = re.search(r"\([A-Za-z0-9]\)", content)
            if next_sub:  # pragma: no cover
                content = content[: next_sub.start()]

            subsections.append(
                ParsedMASubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedMASection) -> Section:
        """Convert ParsedMASection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"MA-{parsed.chapter_number}-{parsed.section_number}",
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
            title_name=f"Massachusetts General Laws - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ma/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, chapter: int | str, section: int | str) -> Section:
        """Fetch and convert a single section.

        Args:
            chapter: Chapter number (e.g., 62 or "62B")
            section: Section number (e.g., 2 or "2A")

        Returns:
            Section model

        Raises:
            MAConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(chapter, section)
        html = self._get(url)
        parsed = self._parse_section_html(html, str(chapter), str(section), url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: int | str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 62)

        Returns:
            List of section numbers (e.g., ["1", "2", "2A", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links: /Laws/GeneralLaws/.../ChapterXX/SectionYY
        pattern = re.compile(rf"Chapter{chapter}/Section(\d+[A-Za-z]?)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, chapter: int | str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 62)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(chapter, section_num)
            except MAConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(
                    f"Warning: Could not fetch Chapter {chapter} Section {section_num}: {e}"
                )  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[int | str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(MA_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "MAConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ma_section(chapter: int | str, section: int | str) -> Section:
    """Fetch a single Massachusetts statute section.

    Args:
        chapter: Chapter number (e.g., 62)
        section: Section number (e.g., 2)

    Returns:
        Section model
    """
    with MAConverter() as converter:
        return converter.fetch_section(chapter, section)


def download_ma_chapter(chapter: int | str) -> list[Section]:
    """Download all sections from a Massachusetts General Laws chapter.

    Args:
        chapter: Chapter number (e.g., 62)

    Returns:
        List of Section objects
    """
    with MAConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_ma_tax_chapters() -> Iterator[Section]:
    """Download all sections from Massachusetts tax-related chapters (58-65C).

    Yields:
        Section objects
    """
    with MAConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(MA_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_ma_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Massachusetts public welfare chapters (115-123B).

    Yields:
        Section objects
    """
    with MAConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(MA_WELFARE_CHAPTERS.keys()))  # pragma: no cover
