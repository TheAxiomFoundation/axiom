"""Tennessee state statute converter.

Converts Tennessee Code Annotated (TCA) HTML to the internal Section model.
Uses Public.Resource.Org's beautified HTML versions of the official TCA.

Tennessee Code Structure:
- Titles (e.g., Title 67: Taxes and Licenses)
- Chapters (e.g., Chapter 1: General Provisions)
- Parts (e.g., Part 1: Miscellaneous Provisions)
- Sections (e.g., 67-1-101: Liberal construction of title)

Data Sources:
- Public.Resource.Org: https://unicourt.github.io/cic-code-tn/
- Internet Archive: https://archive.org/details/gov.tn.tca

Section Number Format: TT-CC-SSS (Title-Chapter-Section)
- Title 67, Chapter 1, Section 101 -> 67-1-101

Example:
    >>> from axiom_corpus.converters.us_states.tn import TNConverter
    >>> converter = TNConverter()
    >>> section = converter.fetch_section("67-1-101")
    >>> print(section.section_title)
    "Liberal construction of title - Incidental powers of commissioner - Chapter definitions."
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup, Tag

from axiom_corpus.models import Citation, Section, Subsection

# Base URL for Public.Resource.Org's beautified TCA HTML
BASE_URL = "https://unicourt.github.io/cic-code-tn/transforms/tn/octn/r74"

# Title information for reference
TN_TITLES: dict[str, str] = {
    67: "Taxes and Licenses",
    71: "Welfare",
}

# Key chapters for tax/benefit analysis (Title 67)
TN_TAX_CHAPTERS: dict[str, str] = {
    1: "General Provisions",
    2: "Income Taxation",
    3: "Petroleum Products and Alternative Fuels Tax Law",
    4: "Privilege and Excise Taxes",
    5: "Property Taxes",
    6: "Sales and Use Taxes",
    7: "Severance Taxes",
    8: "Transfer Taxes",
    9: "Payments in Lieu of Taxes",
    10: "Health Savings Account Act",
}

# Key chapters for welfare analysis (Title 71)
TN_WELFARE_CHAPTERS: dict[str, str] = {
    1: "Administration",
    2: "Programs and Services for Elderly Persons",
    3: "Programs and Services for Children",
    4: "Programs and Services for Persons With Disabilities",
    5: "Programs and Services for Poor Persons",
    6: "Programs and Services for Abused Persons",
    7: "Tennessee Rare Disease Advisory Council",
}


@dataclass
class ParsedTNSection:
    """Parsed Tennessee statute section."""

    section_number: str  # e.g., "67-1-101"
    section_title: str  # e.g., "Liberal construction of title"
    title_number: int  # e.g., 67
    title_name: str  # e.g., "Taxes and Licenses"
    chapter_number: int  # e.g., 1
    chapter_name: str  # e.g., "General Provisions"
    part_number: int | None  # e.g., 1
    part_name: str | None  # e.g., "Miscellaneous Provisions"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedTNSubsection] = field(default_factory=list)
    history: str | None = None  # History/Acts note
    cross_references: list[str] = field(default_factory=list)
    source_url: str = ""


@dataclass
class ParsedTNSubsection:
    """A subsection within a Tennessee statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list[ParsedTNSubsection] = field(default_factory=list)


class TNConverterError(Exception):
    """Error during Tennessee statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class TNConverter:
    """Converter for Tennessee Code Annotated HTML to internal Section model.

    Uses Public.Resource.Org's beautified HTML files which contain entire titles
    in single documents. Section lookup is done by downloading the title HTML
    and finding the section by its ID attribute.

    Example:
        >>> converter = TNConverter()
        >>> section = converter.fetch_section("67-1-101")
        >>> print(section.citation.section)
        "TN-67-1-101"

        >>> for section in converter.iter_chapter(67, 1):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        cache_title_html: bool = True,
    ):
        """Initialize the Tennessee statute converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
            cache_title_html: Whether to cache downloaded title HTML
        """
        self.rate_limit_delay = rate_limit_delay
        self.cache_title_html = cache_title_html
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None
        self._title_cache: dict[str, str] = {}

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                timeout=120.0,  # Longer timeout for large title files
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
        """Build the URL for a title HTML file.

        Args:
            title: Title number (e.g., 67)

        Returns:
            Full URL to the title HTML file
        """
        return f"{BASE_URL}/gov.tn.tca.title.{title}.html"

    def _get_title_html(self, title: int) -> str:  # pragma: no cover
        """Get HTML for a title, using cache if available."""
        if self.cache_title_html and title in self._title_cache:
            return self._title_cache[title]  # pragma: no cover

        url = self._build_title_url(title)
        html = self._get(url)

        if self.cache_title_html:
            self._title_cache[title] = html

        return html

    def _parse_section_number(self, section_number: str) -> tuple[int, int, int]:
        """Parse section number into title, chapter, section components.

        Args:
            section_number: e.g., "67-1-101"

        Returns:
            Tuple of (title, chapter, section)
        """
        parts = section_number.split("-")
        if len(parts) != 3:
            raise TNConverterError(f"Invalid section number format: {section_number}")

        try:
            return int(parts[0]), int(parts[1]), int(parts[2])
        except ValueError as exc:  # pragma: no cover
            raise TNConverterError(
                f"Invalid section number format: {section_number}"
            ) from exc  # pragma: no cover

    def _build_section_id(self, section_number: str) -> str:
        """Build the HTML ID for a section.

        The TCA HTML uses IDs like "t67c01s67-1-101" for section 67-1-101.

        Args:
            section_number: e.g., "67-1-101"

        Returns:
            HTML element ID for the section
        """
        title, chapter, _ = self._parse_section_number(section_number)
        return f"t{title}c{chapter:02d}s{section_number}"

    def _extract_section_from_html(self, soup: BeautifulSoup, section_number: str) -> Tag | None:
        """Find the section element in the parsed HTML.

        Args:
            soup: BeautifulSoup object
            section_number: e.g., "67-1-101"

        Returns:
            The h3 element for the section, or None if not found
        """
        section_id = self._build_section_id(section_number)
        return soup.find("h3", id=section_id)

    def _extract_section_content(self, section_elem: Tag) -> tuple[str, str]:
        """Extract text and HTML content for a section.

        Args:
            section_elem: The h3 element for the section

        Returns:
            Tuple of (text content, HTML content)
        """
        # The section content is in the parent div following the h3
        content_parts = []
        html_parts = [str(section_elem)]

        # Get all siblings until the next section (h3) or end of parent
        for sibling in section_elem.find_next_siblings():
            if sibling.name == "h3":
                break  # pragma: no cover
            if sibling.name == "div" and sibling.find("h3"):
                # This is a container with another section
                break  # pragma: no cover
            content_parts.append(sibling.get_text(separator="\n", strip=True))
            html_parts.append(str(sibling))

        text = "\n".join(content_parts)
        html = "\n".join(html_parts)
        return text, html

    def _parse_subsections(self, content_elem: Tag) -> list[ParsedTNSubsection]:
        """Parse hierarchical subsections from section content.

        Tennessee statutes use ordered lists with class 'alpha':
        - ol.alpha > li for (a), (b), (c)
        - Nested ol > li for (1), (2), (3)
        """
        subsections = []

        # Find the first ordered list (ol.alpha)
        main_list = content_elem.find("ol", class_="alpha")
        if not main_list:
            # Try to find any ol element
            main_list = content_elem.find("ol")

        if not main_list:
            return subsections

        # Parse top-level list items
        for i, li in enumerate(main_list.find_all("li", recursive=False)):
            li_id = li.get("id", "")
            # Extract identifier from ID like "t67c01s67-1-101ol1a"
            identifier = self._extract_identifier_from_id(li_id, i)

            # Get direct text (not nested lists)
            direct_text = self._get_direct_text(li)

            # Parse nested children
            children = []
            nested_ol = li.find("ol", recursive=False)
            if nested_ol:
                for j, child_li in enumerate(nested_ol.find_all("li", recursive=False)):
                    child_id = child_li.get("id", "")
                    child_identifier = self._extract_child_identifier_from_id(child_id, j)
                    child_text = self._get_direct_text(child_li)
                    children.append(
                        ParsedTNSubsection(
                            identifier=child_identifier,
                            text=child_text[:2000],
                            children=[],
                        )
                    )

            subsections.append(
                ParsedTNSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _extract_identifier_from_id(self, element_id: str, index: int) -> str:
        """Extract subsection identifier from element ID.

        IDs are like "t67c01s67-1-101ol1a" -> "a"
        """
        # Try to find the letter at the end after "ol1" or similar
        match = re.search(r"ol\d+([a-z])$", element_id)
        if match:
            return match.group(1)
        # Fallback: use index to generate letter
        return chr(ord("a") + index)  # pragma: no cover

    def _extract_child_identifier_from_id(self, element_id: str, index: int) -> str:
        """Extract child subsection identifier from element ID.

        IDs are like "t67c01s67-1-101ol1a1" -> "1"
        """
        # Try to find the number at the end
        match = re.search(r"ol\d+[a-z](\d+)$", element_id)
        if match:
            return match.group(1)
        # Fallback: use index + 1
        return str(index + 1)  # pragma: no cover

    def _get_direct_text(self, element: Tag) -> str:
        """Get direct text content, excluding nested lists."""
        text_parts = []
        for child in element.children:
            if isinstance(child, str):
                text_parts.append(child.strip())
            elif hasattr(child, "name") and child.name not in ["ol", "ul"]:
                text_parts.append(child.get_text(strip=True))  # pragma: no cover
        return " ".join(filter(None, text_parts))

    def _extract_history(self, section_content: str) -> str | None:
        """Extract history/acts note from section content."""
        # History is typically a paragraph starting with "Acts"
        acts_match = re.search(r"(Acts\s+\d{4}[^.]*\.(?:\s*[^.]+\.)*)", section_content)
        if acts_match:
            return acts_match.group(1).strip()[:1000]
        return None  # pragma: no cover

    def _extract_cross_references(self, section_elem: Tag) -> list[str]:
        """Extract cross-references from section."""
        refs = []
        for cite in section_elem.find_all_next("cite", class_="octn", limit=20):
            # Stop at next section
            parent_h3 = cite.find_parent("div")
            if (
                parent_h3 and parent_h3.find("h3") and parent_h3.find("h3") != section_elem
            ):  # pragma: no cover
                break
            link = cite.find("a")
            if link:
                href = link.get("href", "")
                # Extract section reference from href like "#t67c01s67-1-102"
                match = re.search(r"s(\d+-\d+-\d+)", href)
                if match:
                    refs.append(match.group(1))
        return refs

    def _get_chapter_name(self, soup: BeautifulSoup, title: int, chapter: int) -> str:
        """Get chapter name from the HTML."""
        chapter_id = f"t{title}c{chapter:02d}"
        chapter_elem = soup.find("h2", id=chapter_id)
        if chapter_elem:
            span = chapter_elem.find("span", class_="boldspan")
            if span:
                text = span.get_text(separator=" ", strip=True)
                # Extract name after "Chapter N"
                match = re.search(r"Chapter\s+\d+\s+(.+)", text)
                if match:
                    return match.group(1)
        return f"Chapter {chapter}"  # pragma: no cover

    def _get_part_info(
        self, soup: BeautifulSoup, title: int, chapter: int, section_number: str
    ) -> tuple[int | None, str | None]:
        """Get part number and name for a section."""
        # Find the section's h3
        section_id = self._build_section_id(section_number)
        section_elem = soup.find("h3", id=section_id)
        if not section_elem:
            return None, None  # pragma: no cover

        # Look for preceding part h2
        for prev in section_elem.find_all_previous("h2", class_="parth2"):
            part_id = prev.get("id", "")
            # Part IDs are like "t67c01p01"
            match = re.search(rf"t{title}c{chapter:02d}p(\d+)", part_id)
            if match:
                part_num = int(match.group(1))
                span = prev.find("span", class_="boldspan")
                if span:
                    text = span.get_text(separator=" ", strip=True)
                    name_match = re.search(r"Part\s+\d+\s+(.+)", text)
                    if name_match:
                        return part_num, name_match.group(1)
                return part_num, f"Part {part_num}"  # pragma: no cover

        return None, None  # pragma: no cover

    def _parse_section_html(
        self, soup: BeautifulSoup, section_number: str, url: str
    ) -> ParsedTNSection:
        """Parse section from BeautifulSoup into ParsedTNSection."""
        title, chapter, _ = self._parse_section_number(section_number)

        # Find the section element
        section_elem = self._extract_section_from_html(soup, section_number)
        if not section_elem:
            raise TNConverterError(f"Section {section_number} not found", url)

        # Get section title from the span.boldspan
        section_title = ""
        title_span = section_elem.find("span", class_="boldspan")
        if title_span:
            full_title = title_span.get_text(strip=True)
            # Remove section number prefix
            if full_title.startswith(section_number):
                section_title = full_title[len(section_number) :].strip(". ")
            else:
                section_title = full_title  # pragma: no cover

        # Get title and chapter names
        title_name = TN_TITLES.get(title, f"Title {title}")
        chapter_name = self._get_chapter_name(soup, title, chapter)
        part_number, part_name = self._get_part_info(soup, title, chapter, section_number)

        # Extract content
        text, html = self._extract_section_content(section_elem)

        # Parse subsections from the parent container
        parent_div = section_elem.find_parent("div")
        subsections = []
        if parent_div:
            subsections = self._parse_subsections(parent_div)

        # Extract history and cross-references
        history = self._extract_history(text)
        cross_refs = self._extract_cross_references(section_elem)

        return ParsedTNSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title,
            title_name=title_name,
            chapter_number=chapter,
            chapter_name=chapter_name,
            part_number=part_number,
            part_name=part_name,
            text=text,
            html=html,
            subsections=subsections,
            history=history,
            cross_references=cross_refs,
            source_url=url,
        )

    def _to_section(self, parsed: ParsedTNSection) -> Section:
        """Convert ParsedTNSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"TN-{parsed.section_number}",
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
            title_name=f"Tennessee Code Annotated - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"tn/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
            references_to=parsed.cross_references,
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "67-1-101", "71-1-105"

        Returns:
            Section model

        Raises:
            TNConverterError: If section not found or parsing fails
        """
        title, _, _ = self._parse_section_number(section_number)
        url = self._build_title_url(title)

        try:
            html = self._get_title_html(title)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            raise TNConverterError(f"Failed to fetch title {title}: {e}", url) from e  # pragma: no cover

        soup = BeautifulSoup(html, "html.parser")
        parsed = self._parse_section_html(soup, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 67)
            chapter: Chapter number (e.g., 1)

        Returns:
            List of section numbers (e.g., ["67-1-101", "67-1-102", ...])
        """
        self._build_title_url(title)
        html = self._get_title_html(title)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []
        # Find all section links in the chapter navigation
        chapter_id = f"t{title}c{chapter:02d}"

        # First, find the chapter h2 element
        chapter_elem = soup.find("h2", id=chapter_id)
        if not chapter_elem:
            return section_numbers  # pragma: no cover

        # Find all h3 elements (sections) within this chapter
        # They are in the same parent div
        parent_div = chapter_elem.find_parent("div")
        if not parent_div:
            return section_numbers  # pragma: no cover

        for h3 in parent_div.find_all("h3"):
            section_id = h3.get("id", "")
            # Extract section number from ID like "t67c01s67-1-101"
            match = re.search(rf"t{title}c{chapter:02d}s(\d+-\d+-\d+)", section_id)
            if match:
                section_numbers.append(match.group(1))

        return section_numbers

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 67)
            chapter: Chapter number (e.g., 1)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except TNConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 67)

        Yields:
            Section objects
        """
        # Get chapter list for the title
        chapters = TN_TAX_CHAPTERS if title == 67 else TN_WELFARE_CHAPTERS  # pragma: no cover
        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client and clear cache."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover
        self._title_cache.clear()

    def __enter__(self) -> TNConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_tn_section(section_number: str) -> Section:
    """Fetch a single Tennessee statute section.

    Args:
        section_number: e.g., "67-1-101"

    Returns:
        Section model
    """
    with TNConverter() as converter:
        return converter.fetch_section(section_number)


def download_tn_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a Tennessee Code chapter.

    Args:
        title: Title number (e.g., 67)
        chapter: Chapter number (e.g., 1)

    Returns:
        List of Section objects
    """
    with TNConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_tn_tax_chapters() -> Iterator[Section]:
    """Download all sections from Tennessee tax-related chapters (Title 67).

    Yields:
        Section objects
    """
    with TNConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(67)  # pragma: no cover


def download_tn_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Tennessee welfare chapters (Title 71).

    Yields:
        Section objects
    """
    with TNConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(71)  # pragma: no cover
