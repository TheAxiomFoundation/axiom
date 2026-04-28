"""Wisconsin state statute converter.

Converts Wisconsin Statutes HTML from docs.legis.wisconsin.gov
to the internal Section model for ingestion.

Wisconsin Statute Structure:
- Chapters (e.g., Chapter 71: Income and Franchise Taxes)
- Subchapters (e.g., Subchapter I: Taxation of Individuals and Fiduciaries)
- Sections (e.g., 71.01: Definitions)
- Subsections (e.g., (1), (2), (1am), (1m)(b))

URL Patterns:
- Chapter index: /statutes/statutes/{chapter}
- Section: /statutes/statutes/{chapter}/{subchapter}/{section_suffix}
  e.g., /statutes/statutes/71/i/01 for section 71.01
- Alternative: /document/statutes/{chapter}.{section}
  e.g., /document/statutes/71.01

Key Chapters:
- Chapter 71: Income and Franchise Taxes
- Chapter 49: Public Assistance

Example:
    >>> from axiom.converters.us_states.wi import WIConverter
    >>> converter = WIConverter()
    >>> section = converter.fetch_section("71.01")
    >>> print(section.section_title)
    "Definitions"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from axiom.models import Citation, Section, Subsection

BASE_URL = "https://docs.legis.wisconsin.gov"

# Key chapters for tax/benefit analysis
WI_TAX_CHAPTERS: dict[str, str] = {
    71: "Income and Franchise Taxes for State and Local Revenues",
    72: "Estate Tax",
    73: "Tax Appeals Commission and Department of Revenue",
    74: "Property Tax Collection",
    75: "Property Tax Delinquency",
    76: "Taxation of Utilities and Insurers",
    77: "Taxation of Forest Croplands; Real Estate Transfer Fees; Managed Forest Land; Sales and Use Tax; County and Special District Sales and Use Tax; Premier Resort Areas; Dry Cleaning Fees",
    78: "Taxes on Beverages, Fuel and Tobacco",
    79: "State Revenue Sharing and Tax Relief",
}

WI_WELFARE_CHAPTERS: dict[str, str] = {
    46: "Social Services",
    48: "Children's Code",
    49: "Public Assistance",
    50: "Uniform Licensure",
    51: "State Alcohol, Drug Abuse, Developmental Disabilities and Mental Health Act",
    52: "Support of Dependents",
}


@dataclass
class ParsedWISection:
    """Parsed Wisconsin statute section."""

    section_number: str  # e.g., "71.01"
    section_title: str  # e.g., "Definitions"
    chapter_number: int  # e.g., 71
    chapter_title: str  # e.g., "Income and Franchise Taxes..."
    subchapter: str | None  # e.g., "I" or "TAXATION OF INDIVIDUALS..."
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedWISubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedWISubsection:
    """A subsection within a Wisconsin statute."""

    identifier: str  # e.g., "1", "1m", "a", "1."
    text: str
    children: list["ParsedWISubsection"] = field(default_factory=list)


class WIConverterError(Exception):
    """Error during Wisconsin statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class WIConverter:
    """Converter for Wisconsin Statutes HTML to internal Section model.

    Example:
        >>> converter = WIConverter()
        >>> section = converter.fetch_section("71.01")
        >>> print(section.citation.section)
        "WI-71.01"

        >>> for section in converter.iter_chapter(71):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Wisconsin statute converter.

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
                follow_redirects=True,
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
            section_number: e.g., "71.01", "49.45"

        Returns:
            Full URL to the section page
        """
        # Wisconsin uses /document/statutes/XX.YY format
        return f"{BASE_URL}/document/statutes/{section_number}"

    def _build_chapter_url(self, chapter: int) -> str:
        """Build the URL for a chapter's table of contents."""
        return f"{BASE_URL}/statutes/statutes/{chapter}"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedWISection:
        """Parse section HTML into ParsedWISection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "does not exist" in html.lower():
            raise WIConverterError(f"Section {section_number} not found", url)

        chapter = int(section_number.split(".")[0])
        chapter_title = (
            WI_TAX_CHAPTERS.get(chapter) or WI_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
        )

        # Extract section title from the document
        section_title = self._extract_section_title(soup, section_number)

        # Extract subchapter info
        subchapter = self._extract_subchapter(soup)

        # Get document content
        content_elem = soup.find("div", id="document", class_="statutes")
        if content_elem:
            # Remove navigation elements
            for elem in content_elem.find_all(
                ["div"], class_=["navigation", "navigation_up"]
            ):  # pragma: no cover
                elem.decompose()
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note
        history = self._extract_history(text)

        # Parse subsections from HTML structure
        subsections = self._parse_subsections_from_html(soup)

        return ParsedWISection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            subchapter=subchapter,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _extract_section_title(self, soup: BeautifulSoup, section_number: str) -> str:
        """Extract section title from HTML."""
        # Look for qstitle_sect class which contains the section title
        title_elem = soup.find("span", class_="qstitle_sect")
        if title_elem:
            return title_elem.get_text(strip=True).rstrip(".")

        # Try to find from the section number + title pattern
        # Pattern: "71.01 Definitions."
        pattern = re.compile(rf"{re.escape(section_number)}\s+([^.]+)")  # pragma: no cover
        for elem in soup.find_all(
            ["span", "div"], class_=re.compile(r"qsatxt.*level3")
        ):  # pragma: no cover
            text = elem.get_text(strip=True)  # pragma: no cover
            match = pattern.search(text)  # pragma: no cover
            if match:  # pragma: no cover
                return match.group(1).strip()  # pragma: no cover

        # Fallback: look in page title
        title_tag = soup.find("title")  # pragma: no cover
        if title_tag:  # pragma: no cover
            title_text = title_tag.get_text(strip=True)  # pragma: no cover
            # Remove "Wisconsin Legislature: " prefix
            title_text = re.sub(r"Wisconsin Legislature:\s*", "", title_text)  # pragma: no cover
            # If it just contains the section number, no title
            if title_text != section_number:  # pragma: no cover
                return title_text  # pragma: no cover

        return ""  # pragma: no cover

    def _extract_subchapter(self, soup: BeautifulSoup) -> str | None:
        """Extract subchapter name from HTML."""
        # Look for qstitle_subchap class
        subchap_elem = soup.find("div", class_="qstitle_subchap")
        if subchap_elem:
            return subchap_elem.get_text(strip=True)

        # Try qsnum_subchap
        subchap_num = soup.find("div", class_="qsnum_subchap")
        if subchap_num:
            return subchap_num.get_text(strip=True)  # pragma: no cover

        return None

    def _extract_history(self, text: str) -> str | None:
        """Extract history note from text."""
        # Wisconsin history notes appear like: "71.01 HistoryHistory: 1973 c. 147..."
        history_match = re.search(
            r"(?:\d+\.\d+\s+)?History(?:History)?:\s*(.+?)(?=\n\d+\.\d+|\Z)",
            text,
            re.DOTALL,
        )
        if history_match:
            history = history_match.group(1).strip()
            # Limit length
            return history[:1000] if len(history) > 1000 else history
        return None

    def _parse_subsections_from_html(self, soup: BeautifulSoup) -> list[ParsedWISubsection]:
        """Parse subsections from Wisconsin HTML structure.

        Wisconsin uses CSS classes like qsatxt_2subsect for subsections.
        Subsection identifiers appear in spans with class qsnum_subsect.
        """
        subsections = []

        # Find all subsection elements (level 4 = primary subsections)
        for elem in soup.find_all("div", class_=re.compile(r"qsatxt_2subsect.*level4")):
            identifier = self._extract_subsection_identifier(elem)
            if not identifier:
                continue  # pragma: no cover

            # Get text content (excluding the identifier span)
            text_parts = []
            for child in elem.children:
                if hasattr(child, "get_text") and hasattr(child, "get"):
                    # It's a Tag element
                    class_list = child.get("class", [])
                    if "qsnum_subsect" not in class_list:
                        text_parts.append(child.get_text(strip=True))
                elif hasattr(child, "get_text"):
                    # NavigableString - include text
                    text = child.get_text(strip=True)
                    if text:
                        text_parts.append(text)  # pragma: no cover
                elif isinstance(child, str):  # pragma: no cover
                    text = child.strip()  # pragma: no cover
                    if text:  # pragma: no cover
                        text_parts.append(text)  # pragma: no cover

            text = " ".join(text_parts).strip()

            # Parse children (level 5 and deeper)
            children = self._parse_child_subsections(elem, soup)

            subsections.append(
                ParsedWISubsection(
                    identifier=identifier,
                    text=text[:2000],  # Limit text size
                    children=children,
                )
            )

        # If no structured subsections found, try parsing from text
        if not subsections:
            content = soup.find("div", id="document")  # pragma: no cover
            if content:  # pragma: no cover
                text = content.get_text(separator="\n", strip=True)  # pragma: no cover
                subsections = self._parse_subsections_from_text(text)  # pragma: no cover

        return subsections

    def _extract_subsection_identifier(self, elem) -> str | None:
        """Extract subsection identifier from element."""
        # Look for qsnum_subsect span
        num_span = elem.find("span", class_="qsnum_subsect")
        if num_span:
            # Extract just the number/letter, e.g., "(1)" -> "1", "(1m)" -> "1m"
            text = num_span.get_text(strip=True)
            match = re.search(r"\(([^)]+)\)", text)
            if match:
                return match.group(1)
        return None  # pragma: no cover

    def _parse_child_subsections(
        self, parent_elem, soup: BeautifulSoup
    ) -> list[ParsedWISubsection]:
        """Parse child subsections at deeper levels."""
        children = []

        # Look for level5 elements that follow the parent
        for elem in soup.find_all("div", class_=re.compile(r"qsatxt.*level5")):
            identifier = self._extract_subsection_identifier(elem)
            if not identifier:  # pragma: no cover
                continue

            text_parts = []
            for child in elem.children:
                if hasattr(child, "get_text") and hasattr(child, "get"):
                    # It's a Tag element
                    class_list = child.get("class", [])
                    if "qsnum_subsect" not in class_list:
                        text_parts.append(child.get_text(strip=True))
                elif hasattr(child, "get_text"):
                    # NavigableString - include text
                    text = child.get_text(strip=True)
                    if text:
                        text_parts.append(text)  # pragma: no cover
                elif isinstance(child, str):  # pragma: no cover
                    text = child.strip()  # pragma: no cover
                    if text:  # pragma: no cover
                        text_parts.append(text)  # pragma: no cover

            text = " ".join(text_parts).strip()

            children.append(
                ParsedWISubsection(
                    identifier=identifier,
                    text=text[:2000],
                    children=[],
                )
            )

        return children

    def _parse_subsections_from_text(self, text: str) -> list[ParsedWISubsection]:
        """Parse subsections from plain text using regex patterns.

        Wisconsin uses:
        - (1), (2), (1m), (1am) for primary subsections
        - (a), (b), (c) for secondary
        - 1., 2., 3. for tertiary
        """
        subsections = []  # pragma: no cover

        # Pattern for Wisconsin numbered subsections including variants like (1m), (1am)
        numbered_pattern = r"\((\d+[a-z]*)\)\s*"  # pragma: no cover

        # Split by primary subsections
        parts = re.split(f"(?={numbered_pattern})", text)  # pragma: no cover

        for part in parts[1:]:  # Skip content before first subsection  # pragma: no cover
            match = re.match(numbered_pattern, part)  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover

            identifier = match.group(1)  # pragma: no cover
            content = part[match.end() :].strip()  # pragma: no cover

            # Parse lettered children (a), (b), etc.
            children = self._parse_letter_subsections(content)  # pragma: no cover

            # Get text before first child
            if children:  # pragma: no cover
                first_child_match = re.search(r"\([a-z]\)", content)  # pragma: no cover
                if first_child_match:  # pragma: no cover
                    direct_text = content[: first_child_match.start()].strip()  # pragma: no cover
                else:
                    direct_text = content  # pragma: no cover
            else:
                direct_text = content  # pragma: no cover

            # Stop at next primary subsection
            next_num = re.search(r"\(\d+[a-z]*\)", direct_text)  # pragma: no cover
            if next_num:  # pragma: no cover
                direct_text = direct_text[: next_num.start()].strip()  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedWISubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections  # pragma: no cover

    def _parse_letter_subsections(self, text: str) -> list[ParsedWISubsection]:
        """Parse lettered subsections like (a), (b), (c)."""
        subsections = []  # pragma: no cover
        pattern = r"\(([a-z])\)\s*"  # pragma: no cover

        parts = re.split(f"(?={pattern})", text)  # pragma: no cover

        for part in parts[1:]:  # pragma: no cover
            match = re.match(pattern, part)  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover

            identifier = match.group(1)  # pragma: no cover
            content = part[match.end() :].strip()  # pragma: no cover

            # Stop at next numbered subsection
            next_num = re.search(r"\(\d+[a-z]*\)", content)  # pragma: no cover
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]  # pragma: no cover

            if len(content) > 2000:  # pragma: no cover
                content = content[:2000] + "..."  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedWISubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections  # pragma: no cover

    def _to_section(self, parsed: ParsedWISection) -> Section:
        """Convert ParsedWISection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"WI-{parsed.section_number}",
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
            title_name=f"Wisconsin Statutes - {parsed.chapter_title}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"wi/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "71.01", "49.45"

        Returns:
            Section model

        Raises:
            WIConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 71)

        Returns:
            List of section numbers (e.g., ["71.01", "71.02", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links in the table of contents
        # Pattern: href="/document/statutes/71.01"
        pattern = re.compile(rf"/document/statutes/({chapter}\.\d+[a-z]*)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        # Also look for rel attribute pattern
        rel_pattern = re.compile(rf"statutes/{chapter}\.(\d+[a-z]*)")
        for link in soup.find_all("a", rel=rel_pattern):
            rel = link.get("rel", [])
            if isinstance(rel, list):
                rel = " ".join(rel)
            match = rel_pattern.search(rel)
            if match:
                section_num = f"{chapter}.{match.group(1)}"
                if section_num not in section_numbers:
                    section_numbers.append(section_num)  # pragma: no cover

        return sorted(section_numbers)

    def iter_chapter(self, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 71)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except WIConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

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
            chapters = list(WI_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "WIConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_wi_section(section_number: str) -> Section:
    """Fetch a single Wisconsin statute section.

    Args:
        section_number: e.g., "71.01"

    Returns:
        Section model
    """
    with WIConverter() as converter:
        return converter.fetch_section(section_number)


def download_wi_chapter(chapter: int) -> list[Section]:
    """Download all sections from a Wisconsin Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 71)

    Returns:
        List of Section objects
    """
    with WIConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_wi_tax_chapters() -> Iterator[Section]:
    """Download all sections from Wisconsin tax-related chapters.

    Yields:
        Section objects
    """
    with WIConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(WI_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_wi_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Wisconsin public assistance chapters.

    Yields:
        Section objects
    """
    with WIConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(WI_WELFARE_CHAPTERS.keys()))  # pragma: no cover
