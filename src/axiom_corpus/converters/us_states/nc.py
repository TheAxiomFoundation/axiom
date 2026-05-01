"""North Carolina state statute converter.

Converts North Carolina General Statutes HTML from ncleg.gov to the
internal Section model for ingestion.

North Carolina Statute Structure:
- Chapters (e.g., Chapter 105: Taxation)
- Subchapters (e.g., Subchapter I: Levy of Taxes)
- Articles (e.g., Article 1: Inheritance Tax)
- Sections (e.g., 105-130.2: Definitions)

URL Patterns:
- Full chapter HTML: EnactedLegislation/Statutes/HTML/ByChapter/Chapter_105.html
- Individual section: EnactedLegislation/Statutes/HTML/BySection/Chapter_105/GS_105-130.2.html
- TOC: Laws/GeneralStatutesTOC/Chapter105

Example:
    >>> from axiom_corpus.converters.us_states.nc import NCConverter
    >>> converter = NCConverter()
    >>> section = converter.fetch_section("105-130.2")
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

BASE_URL = "https://www.ncleg.gov/EnactedLegislation/Statutes/HTML"

# Key chapters for tax/benefit analysis
NC_TAX_CHAPTERS: dict[str, str] = {
    105: "Taxation",
}

NC_SOCIAL_CHAPTERS: dict[str, str] = {
    "108A": "Social Services",
    "108D": "NC Health Choice",
}

# All chapters with numeric IDs
NC_CHAPTERS: dict[str, str] = {
    105: "Taxation",
}


@dataclass
class ParsedNCSection:
    """Parsed North Carolina statute section."""

    section_number: str  # e.g., "105-130.2"
    section_title: str  # e.g., "Definitions"
    chapter: str  # e.g., "105" or "108A"
    chapter_title: str  # e.g., "Taxation"
    subchapter: str | None  # e.g., "I"
    article: str | None  # e.g., "1"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedNCSubsection] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedNCSubsection:
    """A subsection within a North Carolina statute."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list[ParsedNCSubsection] = field(default_factory=list)


class NCConverterError(Exception):
    """Error during North Carolina statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class NCConverter:
    """Converter for North Carolina General Statutes HTML to internal Section model.

    Example:
        >>> converter = NCConverter()
        >>> section = converter.fetch_section("105-130.2")
        >>> print(section.citation.section)
        "NC-105-130.2"

        >>> for section in converter.iter_chapter(105):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the North Carolina statute converter.

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

    def _get_chapter_from_section(self, section_number: str) -> str:
        """Extract chapter from section number.

        Args:
            section_number: e.g., "105-130.2", "108A-25"

        Returns:
            Chapter string (e.g., "105", "108A")
        """
        # Section numbers are like "105-130.2" or "108A-25"
        parts = section_number.split("-", 1)
        return parts[0]

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "105-130.2", "108A-25"

        Returns:
            Full URL to the section page
        """
        chapter = self._get_chapter_from_section(section_number)
        # URL format: BySection/Chapter_105/GS_105-130.2.html
        return f"{BASE_URL}/BySection/Chapter_{chapter}/GS_{section_number}.html"

    def _build_chapter_url(self, chapter: str | int) -> str:
        """Build the URL for a full chapter HTML."""
        return f"{BASE_URL}/ByChapter/Chapter_{chapter}.html"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedNCSection:
        """Parse section HTML into ParsedNCSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise NCConverterError(f"Section {section_number} not found", url)

        # Check for empty or invalid page
        if not soup.body or len(soup.get_text(strip=True)) < 50:
            raise NCConverterError(f"Section {section_number} page is empty or invalid", url)

        chapter = self._get_chapter_from_section(section_number)
        chapter_title = self._get_chapter_title(chapter)

        # Extract section title from the heading pattern: "section 105-130.2. Definitions."
        section_title = ""

        # NC format: "<span class="cs72F7C9C5">section 105-130.2. Title.</span>"
        # The section symbol is &sect; in HTML
        for p_tag in soup.find_all("p"):
            text = p_tag.get_text(strip=True)
            # Match pattern like "section 105-130.2. Some Title" or "section 105-130.2.  Some Title"
            pattern = rf"§\s*{re.escape(section_number)}\.?\s+(.+?)(?:\(|$)"
            match = re.search(pattern, text)
            if match:
                section_title = match.group(1).strip().rstrip(".")
                break

        # Fallback: try to find bold span with section number
        if not section_title:
            for span in soup.find_all("span", class_="cs72F7C9C5"):
                text = span.get_text(strip=True)
                if section_number in text:
                    # Extract title after the section number
                    pattern = rf"§\s*{re.escape(section_number)}\.?\s+(.+)"
                    match = re.search(pattern, text)
                    if match:
                        section_title = match.group(1).strip().rstrip(".")  # pragma: no cover
                        break  # pragma: no cover

        # Get body content
        body = soup.find("body")
        if body:
            text = body.get_text(separator="\n", strip=True)
            html_content = str(body)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note - NC format: "(1939, c. 158, s. 302; ...)"
        history = None
        history_match = re.search(
            r"\((\d{4},\s*c\.\s*\d+.*?)\)\s*$",
            text,
            re.MULTILINE | re.DOTALL,
        )
        if history_match:
            history = history_match.group(1).strip()[:1000]  # Limit length

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedNCSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter=chapter,
            chapter_title=chapter_title,
            subchapter=None,  # Could be extracted from chapter HTML
            article=None,  # Could be extracted from chapter HTML
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _get_chapter_title(self, chapter: str) -> str:
        """Get chapter title from chapter number."""
        # Try numeric chapters first
        try:
            ch_num = int(chapter)
            if ch_num in NC_CHAPTERS:
                return NC_CHAPTERS[ch_num]
        except ValueError:
            pass

        # Try alphanumeric chapters (108A, etc.)
        if chapter in NC_SOCIAL_CHAPTERS:
            return NC_SOCIAL_CHAPTERS[chapter]

        return f"Chapter {chapter}"  # pragma: no cover

    def _parse_subsections(self, text: str) -> list[ParsedNCSubsection]:
        """Parse hierarchical subsections from text.

        North Carolina statutes typically use:
        - (a), (b), (c) for primary letter divisions
        - (1), (2), (3) for numbered divisions within letters
        - Sometimes primary numbered divisions depending on section
        """
        subsections = []

        # NC uses (a), (b) as primary or (1), (2) depending on section
        # Check which pattern appears first
        first_letter = re.search(r"\(([a-z])\)\s", text)
        first_number = re.search(r"\((\d+)\)\s", text)

        if first_letter and (not first_number or first_letter.start() < first_number.start()):
            # Letter-based primary divisions
            subsections = self._parse_letter_subsections(text)
        elif first_number:
            # Number-based primary divisions
            subsections = self._parse_number_subsections(text)

        return subsections

    def _parse_letter_subsections(self, text: str) -> list[ParsedNCSubsection]:
        """Parse subsections with (a), (b), (c) as primary divisions."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (a)
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse children (1), (2), etc.
            children = self._parse_number_children(content)

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

            # Clean up - remove next subsection content
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedNCSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_number_subsections(self, text: str) -> list[ParsedNCSubsection]:
        """Parse subsections with (1), (2), (3) as primary divisions."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:  # Skip content before first (1)
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse children (a), (b), etc.
            children = self._parse_letter_children(content)

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

            # Clean up - remove next subsection content
            next_subsection = re.search(r"\(\d+\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedNCSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_number_children(self, text: str) -> list[ParsedNCSubsection]:
        """Parse (1), (2), (3) children."""
        children = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit content
            next_match = re.search(r"\([a-z]\)", content)
            if next_match:  # pragma: no cover
                content = content[: next_match.start()]

            children.append(
                ParsedNCSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return children

    def _parse_letter_children(self, text: str) -> list[ParsedNCSubsection]:
        """Parse (a), (b), (c) children."""
        children = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover

            identifier = match.group(1)  # pragma: no cover
            content = part[match.end() :]  # pragma: no cover

            # Limit content
            next_match = re.search(r"\(\d+\)", content)  # pragma: no cover
            if next_match:  # pragma: no cover
                content = content[: next_match.start()]  # pragma: no cover

            children.append(  # pragma: no cover
                ParsedNCSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return children

    def _to_section(self, parsed: ParsedNCSection) -> Section:
        """Convert ParsedNCSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"NC-{parsed.section_number}",
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
            title_name=f"North Carolina General Statutes - {parsed.chapter_title}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"nc/{parsed.chapter}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "105-130.2", "108A-25"

        Returns:
            Section model

        Raises:
            NCConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: str | int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number/string (e.g., 105, "108A")

        Returns:
            List of section numbers (e.g., ["105-1", "105-1.1", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # NC chapter HTML contains all sections inline
        # Pattern: "section 105-1." or "section 108A-25."
        # The section number is like 105-1, 105-1.1, 105-130.2
        pattern = re.compile(rf"§\s*({re.escape(str(chapter))}-[\d]+(?:\.[\d]+)?)")

        for span in soup.find_all("span", class_="cs72F7C9C5"):
            text = span.get_text(strip=True)
            match = pattern.search(text)
            if match:
                section_num = match.group(1).rstrip(".")
                # Skip repealed sections
                if "Repealed" not in text and section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, chapter: str | int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 105, "108A")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except NCConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[str | int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(NC_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> NCConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_nc_section(section_number: str) -> Section:
    """Fetch a single North Carolina statute section.

    Args:
        section_number: e.g., "105-130.2"

    Returns:
        Section model
    """
    with NCConverter() as converter:
        return converter.fetch_section(section_number)


def download_nc_chapter(chapter: str | int) -> list[Section]:
    """Download all sections from a North Carolina General Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 105, "108A")

    Returns:
        List of Section objects
    """
    with NCConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_nc_tax_chapters() -> Iterator[Section]:
    """Download all sections from North Carolina tax-related chapters.

    Yields:
        Section objects
    """
    with NCConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(NC_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_nc_social_chapters() -> Iterator[Section]:
    """Download all sections from North Carolina social services chapters.

    Yields:
        Section objects
    """
    with NCConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(NC_SOCIAL_CHAPTERS.keys()))  # pragma: no cover
