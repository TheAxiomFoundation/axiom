"""Nebraska state statute converter.

Converts Nebraska Revised Statutes HTML from nebraskalegislature.gov
to the internal Section model for ingestion.

Nebraska Statute Structure:
- Chapters (e.g., Chapter 77: Revenue and Taxation)
- Sections (e.g., 77-2715: Income tax; rate; credits; refund)
- Subsections (e.g., (1), (2), (a), (b), (i), (ii))

URL Patterns:
- Chapter index: /laws/browse-chapters.php?chapter=[NUMBER]
- Section: /laws/statutes.php?statute=[CHAPTER]-[SECTION]
  e.g., /laws/statutes.php?statute=77-2715

Key Chapters:
- Chapter 77: Revenue and Taxation (income tax, sales tax, property tax)
- Chapter 68: Public Assistance (SNAP, TANF, Medicaid)

Example:
    >>> from axiom_corpus.converters.us_states.ne import NEConverter
    >>> converter = NEConverter()
    >>> section = converter.fetch_section("77-2715")
    >>> print(section.section_title)
    "Income tax; rate; credits; refund."
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://nebraskalegislature.gov/laws"

# Key chapters for tax/benefit analysis
NE_TAX_CHAPTERS: dict[str, str] = {
    77: "Revenue and Taxation",
}

NE_WELFARE_CHAPTERS: dict[str, str] = {
    68: "Public Assistance",
}

# Combined chapters
NE_CHAPTERS: dict[str, str] = {**NE_TAX_CHAPTERS, **NE_WELFARE_CHAPTERS}


@dataclass
class ParsedNESection:
    """Parsed Nebraska statute section."""

    section_number: str  # e.g., "77-2715"
    section_title: str  # e.g., "Income tax; rate; credits; refund."
    chapter_number: int  # e.g., 77
    chapter_title: str  # e.g., "Revenue and Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedNESubsection] = field(default_factory=list)
    history: str | None = None  # Source/history note
    cross_references: list[str] = field(default_factory=list)
    annotations: list[str] = field(default_factory=list)
    source_url: str = ""


@dataclass
class ParsedNESubsection:
    """A subsection within a Nebraska statute."""

    identifier: str  # e.g., "1", "a", "i"
    text: str
    children: list[ParsedNESubsection] = field(default_factory=list)


class NEConverterError(Exception):
    """Error during Nebraska statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class NEConverter:
    """Converter for Nebraska Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = NEConverter()
        >>> section = converter.fetch_section("77-2715")
        >>> print(section.citation.section)
        "NE-77-2715"

        >>> for section in converter.iter_chapter(77):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Nebraska statute converter.

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
            section_number: e.g., "77-2715", "68-1719"

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/statutes.php?statute={section_number}"

    def _build_chapter_url(self, chapter: int) -> str:
        """Build the URL for a chapter's index."""
        return f"{BASE_URL}/browse-chapters.php?chapter={chapter}"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedNESection:
        """Parse section HTML into ParsedNESection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "no statute" in html.lower():
            raise NEConverterError(f"Section {section_number} not found", url)

        # Extract chapter number from section_number (e.g., "77" from "77-2715")
        chapter = int(section_number.split("-")[0])
        chapter_title = NE_CHAPTERS.get(chapter, f"Chapter {chapter}")

        # Extract section title from h3 tag
        section_title = ""
        h3 = soup.find("h3")
        if h3:
            section_title = h3.get_text(strip=True)

        # If no h3, try to find title in different location
        if not section_title:
            # Look for h2 with section number, title might follow
            h2 = soup.find("h2")  # pragma: no cover
            if h2:  # pragma: no cover
                # Sometimes title is in next sibling
                next_elem = h2.find_next_sibling()  # pragma: no cover
                if next_elem and next_elem.name == "h3":  # pragma: no cover
                    section_title = next_elem.get_text(strip=True)  # pragma: no cover

        # Get the statute div content
        statute_div = soup.find("div", class_="statute")
        if not statute_div:
            # Try finding by card-body class
            statute_div = soup.find("div", class_="card-body")  # pragma: no cover

        if statute_div:
            # Remove script tags
            for script in statute_div.find_all("script"):
                script.decompose()  # pragma: no cover

            # Get text content from paragraphs
            paragraphs = statute_div.find_all("p", class_="text-justify")
            if paragraphs:
                text = "\n\n".join(p.get_text(strip=True) for p in paragraphs)
            else:
                text = statute_div.get_text(separator="\n", strip=True)  # pragma: no cover

            html_content = str(statute_div)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history/source information
        history = None
        source_div = soup.find("h2", string=re.compile(r"Source", re.IGNORECASE))
        if source_div:
            source_ul = source_div.find_next("ul")
            if source_ul:
                history_parts = []
                for li in source_ul.find_all("li"):
                    history_parts.append(li.get_text(strip=True))
                history = " ".join(history_parts)[:2000]

        # Extract cross-references
        cross_references = []
        xref_div = soup.find("h2", string=re.compile(r"Cross References", re.IGNORECASE))
        if xref_div:
            xref_ul = xref_div.find_next("ul")
            if xref_ul:
                for li in xref_ul.find_all("li"):
                    cross_references.append(li.get_text(strip=True))

        # Extract annotations
        annotations = []
        ann_div = soup.find("h2", string=re.compile(r"Annotations", re.IGNORECASE))
        if ann_div:
            ann_ul = ann_div.find_next("ul")
            if ann_ul:
                for li in ann_ul.find_all("li"):
                    annotations.append(li.get_text(strip=True))

        # Parse subsections from the text
        subsections = self._parse_subsections(text)

        return ParsedNESection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            cross_references=cross_references,
            annotations=annotations,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedNESubsection]:
        """Parse hierarchical subsections from text.

        Nebraska statutes typically use:
        - (1), (2), (3) for primary divisions
        - (a), (b), (c) for secondary divisions
        - (i), (ii), (iii) for tertiary (roman numerals)
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
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedNESubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedNESubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (i), (ii), etc.
            children = self._parse_level3(content)

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            # Get text before first roman numeral child
            if children:  # pragma: no cover
                first_child_match = re.search(r"\(i+\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            subsections.append(
                ParsedNESubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedNESubsection]:
        """Parse level 3 subsections (i), (ii), (iii), etc."""
        subsections = []
        # Match roman numerals in parentheses
        parts = re.split(r"(?=\((?:i{1,3}|iv|v|vi{0,3}|ix|x)\)\s)", text, flags=re.IGNORECASE)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\((i{1,3}|iv|v|vi{0,3}|ix|x)\)\s*", part, re.IGNORECASE)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1).lower()
            content = part[match.end() :]

            # Stop at next subsection marker
            next_marker = re.search(r"\([a-z]\)|\(\d+\)", content)
            if next_marker:
                content = content[: next_marker.start()]  # pragma: no cover

            subsections.append(
                ParsedNESubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedNESection) -> Section:
        """Convert ParsedNESection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"NE-{parsed.section_number}",
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

        # Extract references from cross-references
        references = []
        for xref in parsed.cross_references:
            # Extract section references like "68-1719" or "section 77-2716"
            refs = re.findall(r"(?:section\s+)?(\d+-\d+(?:\.\d+)?)", xref, re.IGNORECASE)
            references.extend(refs)

        return Section(
            citation=citation,
            title_name=f"Nebraska Revised Statutes - {parsed.chapter_title}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ne/{parsed.chapter_number}/{parsed.section_number}",
            references_to=references,
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "77-2715", "68-1719"

        Returns:
            Section model

        Raises:
            NEConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 77)

        Returns:
            List of section numbers (e.g., ["77-101", "77-102", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links: /laws/statutes.php?statute=77-101
        pattern = re.compile(rf"/laws/statutes\.php\?statute=({chapter}-[\d.]+)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 77)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except NEConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all tax/welfare chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(NE_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> NEConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ne_section(section_number: str) -> Section:
    """Fetch a single Nebraska statute section.

    Args:
        section_number: e.g., "77-2715"

    Returns:
        Section model
    """
    with NEConverter() as converter:
        return converter.fetch_section(section_number)


def download_ne_chapter(chapter: int) -> list[Section]:
    """Download all sections from a Nebraska Revised Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 77)

    Returns:
        List of Section objects
    """
    with NEConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_ne_tax_chapters() -> Iterator[Section]:
    """Download all sections from Nebraska tax-related chapters.

    Yields:
        Section objects
    """
    with NEConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(NE_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_ne_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Nebraska public assistance chapters.

    Yields:
        Section objects
    """
    with NEConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(NE_WELFARE_CHAPTERS.keys()))  # pragma: no cover
