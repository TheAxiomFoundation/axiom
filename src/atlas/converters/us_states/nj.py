"""New Jersey state statute converter.

Converts New Jersey Revised Statutes HTML from lis.njleg.state.nj.us
to the internal Section model for ingestion.

New Jersey Statute Structure:
- Titles (e.g., Title 54 - Taxation)
- Subtitles (optional)
- Chapters (e.g., Chapter 4 - Tax on Property)
- Articles (e.g., Article 1 - Assessment of Property)
- Sections (e.g., 54:4-1 Property subject to taxation)

Citation Format:
- NJ statutes use format: TITLE:CHAPTER-SECTION (e.g., 54:4-1)
- Some sections have additional subsection markers (e.g., 54:4-1.12)

URL Patterns:
- Search: gateway.dll?f=xhitlist&xhitlist_q=...
- Direct section: gateway.dll/statutes/1/[path_num]/[section_num]
- Main portal: gateway.dll?f=templates&fn=default.htm

Example:
    >>> from atlas.converters.us_states.nj import NJConverter
    >>> converter = NJConverter()
    >>> section = converter.fetch_section("54:4-1")
    >>> print(section.section_title)
    "Property subject to taxation"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://lis.njleg.state.nj.us/nxt/gateway.dll"

# NJ Title mapping (selected titles relevant to tax/benefit analysis)
NJ_TITLES: dict[str, str] = {
    "2": "Administration of Civil and Criminal Justice",
    "2A": "Administration of Civil and Criminal Justice",
    "4": "Agriculture and Domestic Animals",
    "5": "Amusements, Public Exhibitions and Meetings",
    "9": "Children--Juvenile and Domestic Relations Courts",
    "17": "Corporations and Associations Not for Profit",
    "17B": "Commercial Banking",
    "18A": "Education",
    "19": "Elections",
    "26": "Health and Vital Statistics",
    "30": "Institutions and Agencies",
    "34": "Labor and Workmen's Compensation",
    "40": "Municipalities and Counties",
    "43": "Pensions and Retirement and Unemployment Compensation",
    "44": "Poor",
    "45": "Professions and Occupations",
    "46": "Property",
    "47": "Public Records",
    "52": "State Government, Departments and Officers",
    "54": "Taxation",
    "54A": "New Jersey Gross Income Tax Act",
    "55": "Tenement Houses and Public Housing",
    "56": "Trade Names, Trade-Marks and Unfair Trade Practices",
    "58": "Waters and Water Supply",
}

# Key titles for tax/benefit analysis
NJ_TAX_TITLES: dict[str, str] = {
    "54": "Taxation",
    "54A": "New Jersey Gross Income Tax Act",
}

NJ_WELFARE_TITLES: dict[str, str] = {
    "44": "Poor",
    "30": "Institutions and Agencies",
    "43": "Pensions and Retirement and Unemployment Compensation",
}


@dataclass
class ParsedNJSection:
    """Parsed New Jersey statute section."""

    section_number: str  # e.g., "54:4-1"
    section_title: str  # e.g., "Property subject to taxation"
    title_number: str  # e.g., "54"
    title_name: str  # e.g., "Taxation"
    chapter_number: str | None  # e.g., "4"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedNJSubsection"] = field(default_factory=list)
    history: str | None = None  # Amendment history note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedNJSubsection:
    """A subsection within a New Jersey statute."""

    identifier: str  # e.g., "a", "1", "i"
    text: str
    children: list["ParsedNJSubsection"] = field(default_factory=list)


class NJConverterError(Exception):
    """Error during New Jersey statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class NJConverter:
    """Converter for New Jersey Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = NJConverter()
        >>> section = converter.fetch_section("54:4-1")
        >>> print(section.citation.section)
        "NJ-54:4-1"

        >>> for section in converter.search_title(54):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the New Jersey statute converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
        """
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                timeout=60.0,
                headers={
                    "User-Agent": "Arch/1.0 (Statute Research; contact@axiom-foundation.org)",
                    "Accept": "text/html,application/xhtml+xml",
                },
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

    def _parse_section_number(self, section_number: str) -> tuple[str, str | None, str]:
        """Parse a section number into its components.

        Args:
            section_number: e.g., "54:4-1", "54A:3-5", "44:10-44"

        Returns:
            Tuple of (title, chapter, section_suffix)
            e.g., ("54", "4", "1") or ("54A", "3", "5")
        """
        # Pattern: TITLE:CHAPTER-SECTION (e.g., 54:4-1, 54A:3-5.1)
        match = re.match(r"(\d+[A-Za-z]?):(\d+[A-Za-z]?)-(.+)$", section_number)
        if match:
            return match.group(1), match.group(2), match.group(3)

        # Fallback: just title and section
        match = re.match(r"(\d+[A-Za-z]?):(.+)$", section_number)
        if match:
            return match.group(1), None, match.group(2)  # pragma: no cover

        raise ValueError(f"Cannot parse section number: {section_number}")

    def _build_search_url(self, query: str) -> str:
        """Build a search URL for the NJ statutes database.

        Args:
            query: Search query string

        Returns:
            Full search URL
        """
        # URL-encode the query for the NJ Legislature gateway
        encoded_query = query.replace(" ", "+").replace(":", "%3A")
        return (
            f"{BASE_URL}?f=xhitlist"
            f"&xhitlist_q={encoded_query}"
            f"&xhitlist_x=advanced"
            f"&xhitlist_s=relevance-weight"
            f"&xhitlist_mh=99999"
            f"&xhitlist_hc=%5BXML%5D%5BKwic,25%5D"
            f"&xhitlist_xsl=xhitlist.xsl"
            f"&xhitlist_vpc=first"
            f"&xhitlist_vps=100"
            f"&vid=Publish:10.1048/Enu"
        )

    def _parse_search_results(self, html: str) -> list[dict]:
        """Parse search results HTML to extract section links.

        Args:
            html: Search results HTML

        Returns:
            List of dicts with 'section_number', 'title', 'url'
        """
        soup = BeautifulSoup(html, "html.parser")
        results = []

        # Look for result links in the search results
        # NJ Legislature uses various link patterns
        for link in soup.find_all("a"):
            href = link.get("href", "")
            text = link.get_text(strip=True)

            # Pattern for section numbers like "54:4-1 Property subject to taxation"
            match = re.match(r"(\d+[A-Za-z]?:\d+[A-Za-z]?-[\d.]+[A-Za-z]?)\s+(.+)", text)
            if match:
                results.append(
                    {
                        "section_number": match.group(1),
                        "title": match.group(2),
                        "url": href if href.startswith("http") else f"{BASE_URL}{href}",
                    }
                )

        return results

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedNJSection:
        """Parse section HTML into ParsedNJSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise NJConverterError(f"Section {section_number} not found", url)

        # Parse section number to get title and chapter
        title_num, chapter_num, _ = self._parse_section_number(section_number)

        # Get title name
        title_name = NJ_TAX_TITLES.get(title_num) or NJ_WELFARE_TITLES.get(
            title_num, f"Title {title_num}"
        )

        # Extract section title - NJ format: "54:4-1 Property subject to taxation."
        section_title = ""

        # Try to find the section heading
        # Look for patterns like "54:4-1 Title text" or "54:4-1. Title text"
        title_pattern = re.compile(rf"{re.escape(section_number)}\.?\s+([^.]+(?:\.[^.]+)?)\.")

        # Search in text content
        for text_node in soup.stripped_strings:
            match = title_pattern.search(text_node)
            if match:
                section_title = match.group(1).strip()
                break

        # Try headings if not found
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3", "b", "strong"]):  # pragma: no cover
                heading_text = heading.get_text(strip=True)  # pragma: no cover
                if section_number in heading_text:  # pragma: no cover
                    # Extract title after section number
                    parts = heading_text.split(section_number, 1)  # pragma: no cover
                    if len(parts) > 1:  # pragma: no cover
                        section_title = parts[1].strip().lstrip(".").strip()  # pragma: no cover
                        # Remove trailing period if present
                        section_title = section_title.rstrip(".")  # pragma: no cover
                        break  # pragma: no cover

        # Get body content
        # NJ Legislature uses various content containers
        content_elem = (
            soup.find("div", class_="document")
            or soup.find("div", id="document")
            or soup.find("div", class_="content")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation, scripts, and other non-content elements
            for elem in content_elem.find_all(
                ["nav", "script", "style", "header", "footer", "iframe"]
            ):
                elem.decompose()  # pragma: no cover
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note - NJ uses "Amended YYYY, c.XXX, s.X"
        history = None
        history_match = re.search(r"(Amended\s+\d{4}[^.]*(?:\.\s*)?)+", text, re.IGNORECASE)
        if history_match:
            history = history_match.group(0).strip()[:1000]  # Limit length
        else:
            # Also look for "L.YYYY, c.XXX" pattern
            history_match = re.search(r"(L\.\s*\d{4}[^.]*(?:\.\s*)?)+", text)  # pragma: no cover
            if history_match:  # pragma: no cover
                history = history_match.group(0).strip()[:1000]  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedNJSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_num,
            title_name=title_name,
            chapter_number=chapter_num,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedNJSubsection]:
        """Parse hierarchical subsections from text.

        NJ statutes typically use:
        - a., b., c. for primary divisions (lowercase letters with period)
        - (1), (2), (3) for secondary divisions
        - (a), (b), (c) for tertiary divisions (in parentheses)
        - (i), (ii), (iii) for quaternary divisions
        """
        subsections = []

        # NJ often uses "a." style subsections at top level
        # Split by letter-period pattern: a. b. c.
        parts = re.split(r"(?=\b([a-z])\.\s+(?=[A-Z]))", text)

        for i in range(1, len(parts), 2):
            if i + 1 >= len(parts):
                break  # pragma: no cover

            identifier = parts[i]
            content = parts[i + 1] if i + 1 < len(parts) else ""

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

            # Clean up - remove content after next top-level subsection
            next_subsection = re.search(r"\b[a-z]\.\s+(?=[A-Z])", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedNJSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        # If no letter-period style found, try parenthetical style
        if not subsections:
            subsections = self._parse_parenthetical_subsections(text)  # pragma: no cover

        return subsections

    def _parse_parenthetical_subsections(self, text: str) -> list[ParsedNJSubsection]:
        """Parse (1), (2), (3) style subsections."""
        subsections = []

        # Split by numbered subsections
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:  # Skip content before first subsection
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse second-level children (a), (b), etc.
            children = self._parse_letter_subsections(content)

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

            # Clean up
            next_num = re.search(r"\(\d+\)", direct_text)
            if next_num:
                direct_text = direct_text[: next_num.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedNJSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedNJSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedNJSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _parse_letter_subsections(self, text: str) -> list[ParsedNJSubsection]:
        """Parse (a), (b), (c) style subsections."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]  # pragma: no cover

            subsections.append(
                ParsedNJSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedNJSection) -> Section:
        """Convert ParsedNJSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"NJ-{parsed.section_number}",
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
            title_name=f"New Jersey Revised Statutes - Title {parsed.title_number} {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"nj/{parsed.title_number}/{parsed.section_number}",
        )

    def search_sections(self, query: str) -> list[dict]:
        """Search for sections matching a query.

        Args:
            query: Search query (e.g., "54:4-1" or "property taxation")

        Returns:
            List of dicts with section_number, title, url
        """
        url = self._build_search_url(query)
        html = self._get(url)
        return self._parse_search_results(html)

    def fetch_section(self, section_number: str) -> Section:  # pragma: no cover
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "54:4-1", "44:10-44"

        Returns:
            Section model

        Raises:
            NJConverterError: If section not found or parsing fails
        """
        # Search for the section to get its URL
        results = self.search_sections(section_number)

        if not results:  # pragma: no cover
            raise NJConverterError(  # pragma: no cover
                f"Section {section_number} not found in search results"
            )

        # Find exact match
        exact_match = None  # pragma: no cover
        for result in results:  # pragma: no cover
            if result["section_number"] == section_number:  # pragma: no cover
                exact_match = result  # pragma: no cover
                break  # pragma: no cover

        if not exact_match:  # pragma: no cover
            # Use first result if no exact match
            exact_match = results[0]  # pragma: no cover

        # Fetch the section page
        url = exact_match.get("url", "")  # pragma: no cover
        if not url:  # pragma: no cover
            raise NJConverterError(f"No URL found for section {section_number}")  # pragma: no cover

        html = self._get(url)  # pragma: no cover
        parsed = self._parse_section_html(html, section_number, url)  # pragma: no cover
        return self._to_section(parsed)  # pragma: no cover

    def fetch_section_by_url(self, url: str, section_number: str) -> Section:
        """Fetch a section directly by URL.

        Args:
            url: Direct URL to the section
            section_number: Section number for citation purposes

        Returns:
            Section model
        """
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def iter_title(self, title: str) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., "54", "44")

        Yields:
            Section objects for each section in the title
        """
        # Search for all sections in the title
        query = f"TITLE {title}"  # pragma: no cover
        results = self.search_sections(query)  # pragma: no cover

        for result in results:  # pragma: no cover
            section_number = result.get("section_number", "")  # pragma: no cover
            # Filter to only sections in this title
            if section_number.startswith(f"{title}:"):  # pragma: no cover
                try:  # pragma: no cover
                    url = result.get("url", "")  # pragma: no cover
                    if url:  # pragma: no cover
                        yield self.fetch_section_by_url(url, section_number)  # pragma: no cover
                except NJConverterError as e:  # pragma: no cover
                    # Log but continue with other sections
                    print(f"Warning: Could not fetch {section_number}: {e}")  # pragma: no cover
                    continue  # pragma: no cover

    def iter_tax_titles(self) -> Iterator[Section]:
        """Iterate over sections from NJ tax-related titles.

        Yields:
            Section objects from Title 54 and 54A
        """
        for title in NJ_TAX_TITLES:  # pragma: no cover
            yield from self.iter_title(title)  # pragma: no cover

    def iter_welfare_titles(self) -> Iterator[Section]:
        """Iterate over sections from NJ welfare-related titles.

        Yields:
            Section objects from Title 44, 30, 43
        """
        for title in NJ_WELFARE_TITLES:  # pragma: no cover
            yield from self.iter_title(title)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "NJConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_nj_section(section_number: str) -> Section:
    """Fetch a single New Jersey statute section.

    Args:
        section_number: e.g., "54:4-1"

    Returns:
        Section model
    """
    with NJConverter() as converter:  # pragma: no cover
        return converter.fetch_section(section_number)  # pragma: no cover


def search_nj_statutes(query: str) -> list[dict]:
    """Search New Jersey statutes.

    Args:
        query: Search query

    Returns:
        List of matching sections with section_number, title, url
    """
    with NJConverter() as converter:
        return converter.search_sections(query)


def download_nj_title(title: str) -> list[Section]:
    """Download all sections from a New Jersey Revised Statutes title.

    Args:
        title: Title number (e.g., "54", "44")

    Returns:
        List of Section objects
    """
    with NJConverter() as converter:  # pragma: no cover
        return list(converter.iter_title(title))  # pragma: no cover
