"""Oregon state statute converter.

Converts Oregon Revised Statutes (ORS) HTML from oregonlegislature.gov
to the internal Section model for ingestion.

Oregon Statute Structure:
- Chapters (e.g., Chapter 316: Personal Income Tax)
- Sections (e.g., 316.037: Imposition and rate of tax)
- Subsections using (1), (a), (A), (i) notation

URL Patterns:
- Chapter page: oregonlegislature.gov/bills_laws/ors/ors{chapter}.html
  (e.g., ors316.html for Chapter 316)

Each chapter HTML file contains ALL sections for that chapter, making
it efficient to parse entire chapters at once.

Example:
    >>> from atlas.converters.us_states.or_ import ORConverter
    >>> converter = ORConverter()
    >>> sections = converter.fetch_chapter(316)
    >>> for section in sections:
    ...     print(section.section_title)
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://www.oregonlegislature.gov/bills_laws/ors"

# Key chapters for tax/benefit analysis
OR_TAX_CHAPTERS: dict[str, str] = {
    "305": "Administration of Revenue and Tax Laws; Tax Court",
    "306": "Miscellaneous Tax and Revenue Provisions",
    "307": "Property Subject to Taxation; Exemptions",
    "308": "Assessment of Property for Taxation",
    "308A": "Special Assessment of Farm, Forest and Other Properties",
    "309": "County Assessment Function; County Assessors",
    "310": "Property Tax Limitations; Local Government Taxes",
    "311": "Collection of Property Taxes",
    "312": "Foreclosure of State and County Tax Liens",
    "314": "Taxes Imposed Upon or Measured by Net Income",
    "315": "Personal and Corporate Income or Excise Tax Credits",
    "316": "Personal Income Tax",
    "317": "Corporation Excise Tax",
    "317A": "Corporate Activity Tax",
    "318": "Corporation Income Tax",
    "319": "Motor Vehicle and Aircraft Fuel Taxes",
    "320": "Other Taxes on Motor Vehicles and Aircraft",
    "321": "Timber and Forestland Tax",
    "323": "Cigarettes and Tobacco Products",
}

OR_WELFARE_CHAPTERS: dict[str, str] = {
    "411": "Public Assistance",
    "412": "Financial Assistance and Grants to Clients",
    "413": "Child Welfare Services",
    "414": "Oregon Health Authority",
    "415": "Mental Health and Developmental Disability Services",
    "416": "Community Action Agencies",
    "417": "Youth Development Council; Positive Youth Development",
    "418": "Children's Commission",
}


@dataclass
class ParsedORSection:
    """Parsed Oregon statute section."""

    section_number: str  # e.g., "316.037"
    section_title: str  # e.g., "Imposition and rate of tax"
    chapter_number: int  # e.g., 316
    chapter_title: str  # e.g., "Personal Income Tax"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedORSubsection"] = field(default_factory=list)
    history: str | None = None  # History note like "[1969 c.493 §11; ...]"
    source_url: str = ""
    is_repealed: bool = False  # Section was repealed


@dataclass
class ParsedORSubsection:
    """A subsection within an Oregon statute."""

    identifier: str  # e.g., "1", "a", "A", "i"
    text: str
    children: list["ParsedORSubsection"] = field(default_factory=list)


class ORConverterError(Exception):
    """Error during Oregon statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class ORConverter:
    """Converter for Oregon Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = ORConverter()
        >>> sections = converter.fetch_chapter(316)
        >>> for s in sections:
        ...     print(s.citation.section, s.section_title)

        >>> section = converter.fetch_section("316.037")
        >>> print(section.section_title)
        "Imposition and rate of tax"
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Oregon statute converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
            year: Statute year (default: current year)
        """
        self.rate_limit_delay = rate_limit_delay
        self.year = year or date.today().year
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None
        self._chapter_cache: dict[int, list[ParsedORSection]] = {}

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
        """Make a rate-limited GET request.

        Oregon statutes are served as Windows-1252 encoded HTML (Microsoft Word export).
        The Content-Type header doesn't specify charset, so we must decode explicitly.
        """
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        # Oregon statutes are Windows-1252 encoded (see meta tag in HTML)
        return response.content.decode("windows-1252")

    def _build_chapter_url(self, chapter: int) -> str:
        """Build the URL for a chapter.

        Args:
            chapter: Chapter number (e.g., 316)

        Returns:
            Full URL to the chapter page
        """
        return f"{BASE_URL}/ors{chapter}.html"

    def _normalize_text(self, text: str) -> str:
        """Normalize whitespace and special characters in text.

        Oregon statute HTML uses various unicode spaces and special chars.
        """
        # Replace various unicode spaces with regular space
        text = re.sub(r"[\xa0\u00a0\u2003\u2002\u2009]+", " ", text)
        # Replace en-dash, em-dash with regular hyphen
        text = text.replace("\u2013", "-").replace("\u2014", "-")
        # Collapse multiple spaces
        text = re.sub(r" +", " ", text)
        # Clean up section symbol variations
        text = text.replace("§", "s")
        return text.strip()

    def _parse_chapter_html(
        self,
        html: str,
        chapter: int,
        url: str,
    ) -> list[ParsedORSection]:
        """Parse chapter HTML into list of ParsedORSection objects.

        Oregon chapter HTML contains all sections for that chapter.
        Sections are identified by bold text containing the section number.
        """
        soup = BeautifulSoup(html, "html.parser")
        sections: list[ParsedORSection] = []

        # Get chapter title from page heading
        chapter_title = self._get_chapter_title(chapter)

        # Find all paragraph elements
        paragraphs = soup.find_all("p", class_="MsoNormal")

        current_section: dict | None = None
        current_text_parts: list[str] = []

        # Section heading pattern: "316.037 Imposition and rate of tax."
        # The section number and title are in bold. The title may be on a new line.
        # Uses re.DOTALL to match across newlines
        section_pattern = re.compile(
            rf"^\s*({chapter}\.\d{{3}}[A-Za-z]?)\s+(.+?)\.?\s*$", re.DOTALL
        )
        # Repealed section pattern: "316.035 [1953 c.304 ...]"
        repealed_pattern = re.compile(
            rf"^\s*({chapter}\.\d{{3}}[A-Za-z]?)\s*(\[.+\])\s*$", re.DOTALL
        )

        for p in paragraphs:
            # Get full text and check for bold section headers
            bold_elem = p.find("b")
            full_text = self._normalize_text(p.get_text())

            if bold_elem:
                bold_text = self._normalize_text(bold_elem.get_text())

                # Check for section header in bold
                match = section_pattern.match(bold_text)
                repealed_match = repealed_pattern.match(full_text)

                if match or repealed_match:
                    # Save previous section if exists
                    if current_section:
                        sections.append(
                            self._finalize_section(
                                current_section,
                                current_text_parts,
                                chapter,
                                chapter_title,
                                url,
                            )
                        )
                        current_text_parts = []

                    if repealed_match:
                        # Repealed section - just note it
                        section_num = repealed_match.group(1)
                        history = repealed_match.group(2)
                        current_section = {
                            "section_number": section_num,
                            "section_title": "(Repealed)",
                            "is_repealed": True,
                            "history": history,
                        }
                        # Get any remaining text after the bold
                        remaining = full_text[len(bold_text) :].strip()
                        if remaining and not remaining.startswith("["):
                            current_text_parts.append(remaining)  # pragma: no cover
                    else:
                        # Active section
                        section_num = match.group(1)
                        # Clean up title - replace newlines and multiple spaces with single space
                        section_title = re.sub(r"\s+", " ", match.group(2)).strip()
                        current_section = {
                            "section_number": section_num,
                            "section_title": section_title,
                            "is_repealed": False,
                            "history": None,
                        }
                        # Get any remaining text after the bold (the body starts)
                        remaining = full_text[len(bold_text) :].strip()
                        if remaining:
                            current_text_parts.append(remaining)
                    continue

            # Not a section header - accumulate text for current section
            if current_section and full_text and full_text != "\xa0":
                current_text_parts.append(full_text)

        # Don't forget the last section
        if current_section:
            sections.append(
                self._finalize_section(
                    current_section,
                    current_text_parts,
                    chapter,
                    chapter_title,
                    url,
                )
            )

        return sections

    def _finalize_section(
        self,
        section_data: dict,
        text_parts: list[str],
        chapter: int,
        chapter_title: str,
        url: str,
    ) -> ParsedORSection:
        """Create a ParsedORSection from accumulated data."""
        full_text = "\n".join(text_parts)

        # Extract history note from end of text
        history = section_data.get("history")
        if not history:
            history_match = re.search(r"\[(\d{4}\s+c\.\d+[^]]*)\]\s*$", full_text)
            if history_match:
                history = history_match.group(0)
                full_text = full_text[: history_match.start()].strip()

        # Parse subsections from text
        subsections = self._parse_subsections(full_text)

        return ParsedORSection(
            section_number=section_data["section_number"],
            section_title=section_data["section_title"],
            chapter_number=chapter,
            chapter_title=chapter_title,
            text=full_text,
            html="",  # We don't preserve HTML
            subsections=subsections,
            history=history,
            source_url=url,
            is_repealed=section_data.get("is_repealed", False),
        )

    def _get_chapter_title(self, chapter: int) -> str:
        """Get the title for a chapter number."""
        chapter_str = str(chapter)
        if chapter_str in OR_TAX_CHAPTERS:
            return OR_TAX_CHAPTERS[chapter_str]
        if chapter_str in OR_WELFARE_CHAPTERS:
            return OR_WELFARE_CHAPTERS[chapter_str]
        return f"Chapter {chapter}"  # pragma: no cover

    def _parse_subsections(self, text: str) -> list[ParsedORSubsection]:
        """Parse hierarchical subsections from text.

        Oregon statutes typically use:
        - (1), (2), (3) for primary divisions
        - (a), (b), (c) for secondary divisions
        - (A), (B), (C) for tertiary divisions
        - (i), (ii), (iii) for quaternary divisions
        """
        subsections: list[ParsedORSubsection] = []

        # Split by top-level subsections (1), (2), etc.
        parts = re.split(r"(?=\(\d+\))", text)

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
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedORSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedORSubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections: list[ParsedORSubsection] = []
        parts = re.split(r"(?=\([a-z]\))", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (A), (B), etc.
            children = self._parse_level3(content)

            # Get direct text
            if children:  # pragma: no cover
                first_child_match = re.search(r"\([A-Z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                # Stop at next numbered subsection or (a)-(z)
                next_match = re.search(r"\(\d+\)|\([a-z]\)", content)
                direct_text = (
                    content[: next_match.start()].strip() if next_match else content.strip()
                )

            subsections.append(
                ParsedORSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedORSubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections: list[ParsedORSubsection] = []
        parts = re.split(r"(?=\([A-Z]\))", text)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at parent-level markers
            next_match = re.search(r"\(\d+\)|\([a-z]\)|\([A-Z]\)", content)
            if next_match:
                content = content[: next_match.start()]

            subsections.append(
                ParsedORSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedORSection) -> Section:
        """Convert ParsedORSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"OR-{parsed.section_number}",
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
                                identifier=gc.identifier,
                                heading=None,
                                text=gc.text,
                                children=[],
                            )
                            for gc in child.children
                        ],
                    )
                    for child in sub.children
                ],
            )
            for sub in parsed.subsections
        ]

        return Section(
            citation=citation,
            title_name=f"Oregon Revised Statutes - {parsed.chapter_title}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"or/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_chapter(self, chapter: int, include_repealed: bool = False) -> list[Section]:
        """Fetch and convert all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 316)
            include_repealed: Whether to include repealed sections

        Returns:
            List of Section models
        """
        # Check cache first
        if chapter in self._chapter_cache:
            parsed_sections = self._chapter_cache[chapter]
        else:
            url = self._build_chapter_url(chapter)
            html = self._get(url)
            parsed_sections = self._parse_chapter_html(html, chapter, url)
            self._chapter_cache[chapter] = parsed_sections

        sections = []
        for parsed in parsed_sections:
            if parsed.is_repealed and not include_repealed:
                continue
            sections.append(self._to_section(parsed))

        return sections

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "316.037", "411.010"

        Returns:
            Section model

        Raises:
            ORConverterError: If section not found
        """
        # Extract chapter from section number
        chapter = int(section_number.split(".")[0])

        # Fetch entire chapter (cached)
        url = self._build_chapter_url(chapter)
        if chapter not in self._chapter_cache:
            html = self._get(url)
            self._chapter_cache[chapter] = self._parse_chapter_html(html, chapter, url)

        # Find the specific section
        for parsed in self._chapter_cache[chapter]:
            if parsed.section_number == section_number:
                return self._to_section(parsed)

        raise ORConverterError(f"Section {section_number} not found", url)

    def get_chapter_section_numbers(self, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 316)

        Returns:
            List of section numbers (e.g., ["316.002", "316.003", ...])
        """
        # Fetch chapter (cached)
        if chapter not in self._chapter_cache:
            url = self._build_chapter_url(chapter)
            html = self._get(url)
            self._chapter_cache[chapter] = self._parse_chapter_html(html, chapter, url)

        return [p.section_number for p in self._chapter_cache[chapter] if not p.is_repealed]

    def iter_chapter(self, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 316)

        Yields:
            Section objects for each non-repealed section
        """
        yield from self.fetch_chapter(chapter)

    def iter_chapters(
        self,
        chapters: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            # Convert string keys to int, skipping any with letter suffixes like "308A"
            chapters = [int(c) for c in OR_TAX_CHAPTERS.keys() if c.isdigit()]  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "ORConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_or_section(section_number: str) -> Section:
    """Fetch a single Oregon statute section.

    Args:
        section_number: e.g., "316.037"

    Returns:
        Section model
    """
    with ORConverter() as converter:
        return converter.fetch_section(section_number)


def download_or_chapter(chapter: int) -> list[Section]:
    """Download all sections from an Oregon Revised Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 316)

    Returns:
        List of Section objects
    """
    with ORConverter() as converter:
        return converter.fetch_chapter(chapter)


def download_or_tax_chapters() -> Iterator[Section]:
    """Download all sections from Oregon tax-related chapters (305-323).

    Yields:
        Section objects
    """
    with ORConverter() as converter:  # pragma: no cover
        # Convert string keys to int, skipping any with letter suffixes like "308A"
        chapters = [int(c) for c in OR_TAX_CHAPTERS.keys() if c.isdigit()]  # pragma: no cover
        yield from converter.iter_chapters(chapters)  # pragma: no cover


def download_or_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Oregon public assistance chapters (411-418).

    Yields:
        Section objects
    """
    with ORConverter() as converter:  # pragma: no cover
        chapters = [int(c) for c in OR_WELFARE_CHAPTERS.keys()]  # pragma: no cover
        yield from converter.iter_chapters(chapters)  # pragma: no cover
