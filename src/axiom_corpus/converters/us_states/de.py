"""Delaware state statute converter.

Converts Delaware Code HTML from delcode.delaware.gov to the internal
Section model for ingestion.

Delaware Code Structure:
- Titles (e.g., Title 30: State Taxes, Title 31: Welfare)
- Chapters (e.g., Chapter 11: Personal Income Tax)
- Subchapters (e.g., Subchapter I: General Provisions) - optional
- Sections (e.g., 1101: Meaning of terms)

URL Patterns:
- Title index: /title30/index.html
- Chapter index: /title30/c011/index.html
- Subchapter: /title30/c011/sc01/index.html
- Authenticated PDF: /title30/Title30.pdf

Note: Delaware sections are embedded within chapter/subchapter pages (no separate
section URLs). Sections are identified by anchor IDs like #1101.

Example:
    >>> from axiom_corpus.converters.us_states.de import DEConverter
    >>> converter = DEConverter()
    >>> sections = converter.fetch_chapter(30, 11)
    >>> print(sections[0].section_title)
    "Meaning of terms"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup, Tag

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://delcode.delaware.gov"

# Title 30: State Taxes - Key chapters
DE_TAX_CHAPTERS: dict[str, str] = {
    1: "General Provisions",
    3: "Division of Revenue",
    5: "Tax Appeal Board",
    11: "Personal Income Tax",
    13: "Withholding Tax",
    14: "Estimated Income Tax",
    15: "Fiduciary Income Tax",
    16: "Pass-Through Entities",
    17: "Joint Returns",
    18: "Reciprocity",
    19: "Corporation Income Tax",
    20: "Bank Franchise Tax",
    21: "Inheritance Tax",
    23: "Realty Transfer Tax",
    25: "Occupational and Business License Tax",
    27: "Alcoholic Beverage License Tax",
    29: "Public Utility Tax",
    30: "Gross Receipts Tax",
    31: "Motor Fuel Tax",
    33: "Cigarette and Tobacco Products Tax",
    35: "Hotel Accommodations Tax",
    51: "Public Accommodations Tax",
    52: "Sports Lottery Tax",
    53: "Casino Gaming Tax",
    54: "Sports Wagering Tax",
    55: "Online Gaming Tax",
}

# Title 31: Welfare - Key chapters
DE_WELFARE_CHAPTERS: dict[str, str] = {
    1: "General Provisions",
    3: "Department of Health and Social Services",
    4: "Food Stamp Program",
    5: "Public Assistance",
    6: "Delaware Economic Development Office",
    9: "Disaster Assistance",
    10: "Aid to the Aged",
    11: "Aid to Dependent Children",
    13: "Medical Assistance",
    15: "Child Care",
    21: "Services for the Aging",
    23: "Services for Children, Youth and Their Families",
    25: "Division for Visually Impaired",
    27: "Vocational Rehabilitation",
    28: "Delaware School for the Deaf",
    29: "Delaware Psychiatric Center",
    30: "Developmental Disabilities Services",
    35: "Delaware Kidney Disease Commission",
    36: "Child Abuse Registry",
    37: "Child Abuse Prevention",
    38: "Child Death Review Commission",
    39: "Child Protection Registry",
    40: "Housing Authority",
    41: "Housing Code",
    42: "Manufactured Home Communities",
    43: "Delaware State Housing Authority",
    44: "Neighborhood Assistance",
    45: "Community Development",
    46: "Affordable Housing",
    47: "Low-Income Housing Tax Credit",
    51: "Juvenile Services",
    52: "Youth Rehabilitation Services",
    53: "Juvenile Justice",
    54: "Juvenile Court",
}


@dataclass
class ParsedDESubsection:
    """A subsection within a Delaware statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list[ParsedDESubsection] = field(default_factory=list)


@dataclass
class ParsedDESection:
    """Parsed Delaware statute section."""

    section_number: str  # e.g., "1101"
    section_title: str  # e.g., "Meaning of terms"
    title_number: int  # e.g., 30
    chapter_number: int  # e.g., 11
    chapter_title: str | None  # e.g., "Personal Income Tax"
    subchapter: str | None  # e.g., "I" or "General Provisions"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedDESubsection] = field(default_factory=list)
    history: str | None = None  # History note with citations
    source_url: str = ""
    effective_date: date | None = None


class DEConverterError(Exception):
    """Error during Delaware statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class DEConverter:
    """Converter for Delaware Code HTML to internal Section model.

    Example:
        >>> converter = DEConverter()
        >>> sections = converter.fetch_chapter(30, 11)
        >>> print(sections[0].citation.section)
        "DE-30-1101"

        >>> for section in converter.iter_chapter(30, 11):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Delaware statute converter.

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

    def _build_title_url(self, title: int) -> str:
        """Build URL for a title index page."""
        return f"{BASE_URL}/title{title}/index.html"

    def _build_chapter_url(self, title: int, chapter: int) -> str:
        """Build URL for a chapter index/content page.

        Args:
            title: Title number (e.g., 30)
            chapter: Chapter number (e.g., 11)

        Returns:
            Full URL to the chapter page
        """
        # Delaware uses c### format with leading zeros for single/double digit chapters
        chapter_str = f"c{chapter:03d}" if chapter < 100 else f"c{chapter}"
        return f"{BASE_URL}/title{title}/{chapter_str}/index.html"

    def _build_subchapter_url(self, title: int, chapter: int, subchapter: int) -> str:
        """Build URL for a subchapter page.

        Args:
            title: Title number (e.g., 30)
            chapter: Chapter number (e.g., 11)
            subchapter: Subchapter number (e.g., 1 for Subchapter I)

        Returns:
            Full URL to the subchapter page
        """
        chapter_str = f"c{chapter:03d}" if chapter < 100 else f"c{chapter}"
        subchapter_str = f"sc{subchapter:02d}"
        return f"{BASE_URL}/title{title}/{chapter_str}/{subchapter_str}/index.html"

    def _parse_section_head(self, section_head: str) -> tuple[str, str]:
        """Parse section number and title from SectionHead text.

        Args:
            section_head: Text like "1101. Meaning of terms."

        Returns:
            Tuple of (section_number, section_title)
        """
        # Pattern: section number followed by title
        # Examples: "1101. Meaning of terms." or "101. Definitions."
        match = re.match(r"§?\s*(\d+[A-Za-z]?)\.\s*(.+?)\.?\s*$", section_head.strip())
        if match:
            return match.group(1), match.group(2).strip().rstrip(".")
        # Fallback: just extract numbers
        num_match = re.search(r"(\d+[A-Za-z]?)", section_head)  # pragma: no cover
        if num_match:  # pragma: no cover
            return num_match.group(1), section_head.strip()
        return "", section_head.strip()  # pragma: no cover

    def _parse_subsections(self, text: str) -> list[ParsedDESubsection]:
        """Parse hierarchical subsections from text.

        Delaware statutes use:
        - (a), (b), (c) for primary divisions
        - (1), (2), (3) for secondary divisions
        - a., b., c. or 1., 2., 3. for tertiary (sometimes)
        """
        subsections = []

        # Split by top-level subsections (a), (b), etc.
        # Look for pattern at start of line or after whitespace
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts:
            if not part.strip():
                continue

            # Try to match (a), (b), etc.
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue

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
                ParsedDESubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedDESubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts:
            if not part.strip():
                continue

            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size and stop at next lettered subsection
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:  # pragma: no cover
                content = content[: next_letter.start()]

            subsections.append(
                ParsedDESubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _extract_history(self, section_div: Tag) -> str | None:
        """Extract history citations from a section div.

        Delaware history notes appear after the section text, containing
        citations like "30 Del. C. 1953, 1101; 57 Del. Laws, c. 737, 1"
        """
        # Get all text after the last subsection paragraph
        text = section_div.get_text(separator="\n", strip=True)

        # Look for Del. C. or Del. Laws patterns
        history_pattern = re.search(
            r"(\d+\s+Del\.\s+C\.[^\n]+(?:;\s*\d+\s+Del\.\s+Laws[^\n]*)*)",
            text,
            re.IGNORECASE,
        )
        if history_pattern:
            return history_pattern.group(1).strip()[:500]

        # Alternative: look for links to SessionLaws
        history_links = section_div.find_all(
            "a", href=re.compile(r"SessionLaws")
        )  # pragma: no cover
        if history_links:  # pragma: no cover
            history_parts = []
            for link in history_links[:5]:  # Limit to first 5
                history_parts.append(link.get_text(strip=True))
            return "; ".join(history_parts)[:500]

        return None  # pragma: no cover

    def _parse_chapter_html(
        self,
        html: str,
        title: int,
        chapter: int,
        url: str,
    ) -> list[ParsedDESection]:
        """Parse chapter HTML into list of ParsedDESection."""
        soup = BeautifulSoup(html, "html.parser")
        sections = []

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise DEConverterError(
                f"Chapter {chapter} not found in Title {title}", url
            )  # pragma: no cover

        # Get chapter title from TitleHead
        chapter_title = None
        title_head = soup.find("div", id="TitleHead")
        if title_head:
            h3 = title_head.find("h3")
            if h3:
                h3_text = h3.get_text(strip=True)
                # Extract title after "CHAPTER XX."
                chapter_match = re.search(r"CHAPTER\s+\d+[A-Za-z]?\.\s*(.+)", h3_text)
                if chapter_match:
                    chapter_title = chapter_match.group(1)

        # Get subchapter name if present
        subchapter = None
        if title_head:
            # Subchapter is in h4 after h3
            h4s = title_head.find_all("h4")
            for h4 in h4s:
                h4_text = h4.get_text(strip=True)
                if "Subchapter" in h4_text:
                    subchapter = h4_text

        # Find CodeBody which contains sections
        code_body = soup.find("div", id="CodeBody")
        if not code_body:
            # Some pages have sections directly in content
            code_body = soup.find("div", id="content")  # pragma: no cover

        if not code_body:
            return sections  # pragma: no cover

        # Find all section divs
        section_divs = code_body.find_all("div", class_="Section")

        for section_div in section_divs:
            # Get section header
            section_head = section_div.find("div", class_="SectionHead")
            if not section_head:
                continue  # pragma: no cover

            section_head_text = section_head.get_text(strip=True)
            section_number, section_title = self._parse_section_head(section_head_text)

            if not section_number:
                continue  # pragma: no cover

            # Get section ID from anchor
            section_id = section_head.get("id", section_number)

            # Get full text content
            text_parts = []
            for p in section_div.find_all("p", class_=re.compile(r"subsection|indent")):
                text_parts.append(p.get_text(strip=True))

            # If no paragraphs with those classes, get all text
            if not text_parts:  # pragma: no cover
                text = section_div.get_text(separator="\n", strip=True)
                # Remove the section header from text
                text = text.replace(section_head_text, "", 1).strip()
            else:
                text = "\n".join(text_parts)

            # Parse subsections
            subsections = self._parse_subsections(text)

            # Extract history
            history = self._extract_history(section_div)

            sections.append(
                ParsedDESection(
                    section_number=section_number,
                    section_title=section_title,
                    title_number=title,
                    chapter_number=chapter,
                    chapter_title=chapter_title,
                    subchapter=subchapter,
                    text=text,
                    html=str(section_div),
                    subsections=subsections,
                    history=history,
                    source_url=f"{url}#{section_id}",
                )
            )

        return sections

    def _to_section(self, parsed: ParsedDESection, title: int) -> Section:
        """Convert ParsedDESection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"DE-{title}-{parsed.section_number}",
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

        # Build title name
        title_name = f"Delaware Code Title {title}"
        if title == 30:
            title_name += " - State Taxes"
        elif title == 31:  # pragma: no cover
            title_name += " - Welfare"

        return Section(
            citation=citation,
            title_name=title_name,
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"de/{title}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_chapter(self, title: int, chapter: int) -> list[Section]:
        """Fetch and convert all sections in a chapter.

        Args:
            title: Title number (e.g., 30 for State Taxes)
            chapter: Chapter number (e.g., 11 for Personal Income Tax)

        Returns:
            List of Section models
        """
        url = self._build_chapter_url(title, chapter)
        html = self._get(url)

        # Check if this is a chapter index with subchapters
        soup = BeautifulSoup(html, "html.parser")
        subchapter_links = soup.find_all("a", href=re.compile(r"/sc\d+/index\.html"))

        if subchapter_links:  # pragma: no cover
            # Chapter has subchapters - fetch each one
            all_sections = []
            seen_subchapters = set()

            for link in subchapter_links:
                href = link.get("href", "")
                # Extract subchapter number from href
                sc_match = re.search(r"sc(\d+)", href)
                if sc_match:
                    sc_num = int(sc_match.group(1))
                    if sc_num in seen_subchapters:
                        continue  # pragma: no cover
                    seen_subchapters.add(sc_num)

                    try:
                        sc_url = self._build_subchapter_url(title, chapter, sc_num)
                        sc_html = self._get(sc_url)
                        parsed = self._parse_chapter_html(sc_html, title, chapter, sc_url)
                        all_sections.extend(self._to_section(p, title) for p in parsed)
                    except (httpx.HTTPError, DEConverterError) as e:  # pragma: no cover
                        print(
                            f"Warning: Could not fetch subchapter {sc_num}: {e}"
                        )  # pragma: no cover
                        continue  # pragma: no cover

            return all_sections
        else:
            # Chapter content is directly on this page
            parsed = self._parse_chapter_html(html, title, chapter, url)
            return [self._to_section(p, title) for p in parsed]

    def fetch_section(self, title: int, chapter: int, section_number: str) -> Section:
        """Fetch a specific section from a chapter.

        Args:
            title: Title number (e.g., 30)
            chapter: Chapter number (e.g., 11)
            section_number: Section number (e.g., "1101")

        Returns:
            Section model

        Raises:
            DEConverterError: If section not found
        """
        sections = self.fetch_chapter(title, chapter)

        for section in sections:
            if section_number in section.citation.section:
                return section

        raise DEConverterError(
            f"Section {section_number} not found in Title {title}, Chapter {chapter}"
        )

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 30)
            chapter: Chapter number (e.g., 11)

        Returns:
            List of section numbers (e.g., ["1101", "1102", ...])
        """
        url = self._build_chapter_url(title, chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # First check if there are subchapters
        subchapter_links = soup.find_all("a", href=re.compile(r"/sc\d+/index\.html"))

        if subchapter_links:  # pragma: no cover
            # Need to fetch each subchapter
            seen_subchapters = set()
            for link in subchapter_links:
                href = link.get("href", "")
                sc_match = re.search(r"sc(\d+)", href)
                if sc_match:
                    sc_num = int(sc_match.group(1))
                    if sc_num in seen_subchapters:
                        continue  # pragma: no cover
                    seen_subchapters.add(sc_num)

                    try:
                        sc_url = self._build_subchapter_url(title, chapter, sc_num)
                        sc_html = self._get(sc_url)
                        sc_soup = BeautifulSoup(sc_html, "html.parser")

                        # Find section links in chaptersections list
                        section_list = sc_soup.find("ul", class_="chaptersections")
                        if section_list:
                            for li in section_list.find_all("li"):
                                link_elem = li.find("a")
                                if link_elem:
                                    href = link_elem.get("href", "")
                                    if href.startswith("#"):
                                        section_numbers.append(href[1:])
                    except httpx.HTTPError, DEConverterError:  # pragma: no cover
                        continue  # pragma: no cover
        else:
            # Sections listed directly on chapter page
            section_list = soup.find("ul", class_="chaptersections")
            if section_list:
                for li in section_list.find_all("li"):
                    link = li.find("a")
                    if link:
                        href = link.get("href", "")
                        if href.startswith("#"):
                            section_numbers.append(href[1:])

        return section_numbers

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 30)
            chapter: Chapter number (e.g., 11)

        Yields:
            Section objects for each section
        """
        sections = self.fetch_chapter(title, chapter)
        yield from sections

    def iter_chapters(
        self,
        title: int,
        chapters: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            title: Title number (30 for taxes, 31 for welfare)
            chapters: List of chapter numbers (default: all tax chapters for 30)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            if title == 30:  # pragma: no cover
                chapters = list(DE_TAX_CHAPTERS.keys())  # pragma: no cover
            elif title == 31:  # pragma: no cover
                chapters = list(DE_WELFARE_CHAPTERS.keys())  # pragma: no cover
            else:
                chapters = []  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            try:  # pragma: no cover
                yield from self.iter_chapter(title, chapter)  # pragma: no cover
            except DEConverterError as e:  # pragma: no cover
                print(f"Warning: Could not fetch chapter {chapter}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> DEConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_de_section(title: int, chapter: int, section_number: str) -> Section:
    """Fetch a single Delaware statute section.

    Args:
        title: Title number (e.g., 30)
        chapter: Chapter number (e.g., 11)
        section_number: Section number (e.g., "1101")

    Returns:
        Section model
    """
    with DEConverter() as converter:
        return converter.fetch_section(title, chapter, section_number)


def download_de_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a Delaware Code chapter.

    Args:
        title: Title number (e.g., 30)
        chapter: Chapter number (e.g., 11)

    Returns:
        List of Section objects
    """
    with DEConverter() as converter:
        return converter.fetch_chapter(title, chapter)


def download_de_tax_chapters() -> Iterator[Section]:
    """Download all sections from Delaware tax-related chapters (Title 30).

    Yields:
        Section objects
    """
    with DEConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(30, list(DE_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_de_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Delaware welfare chapters (Title 31).

    Yields:
        Section objects
    """
    with DEConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(31, list(DE_WELFARE_CHAPTERS.keys()))  # pragma: no cover
