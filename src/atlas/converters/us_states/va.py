"""Virginia state statute converter.

Converts Code of Virginia HTML from law.lis.virginia.gov
to the internal Section model for ingestion.

Virginia Code Structure:
- Titles (e.g., Title 58.1: Taxation)
- Chapters (e.g., Chapter 3: Income Tax)
- Articles (e.g., Article 2: Rates)
- Sections (e.g., § 58.1-320: Tax rates)

URL Patterns:
- Title index: /vacode/title58.1/
- Chapter contents: /vacode/title58.1/chapter3/
- Section: /vacode/58.1-320/

Section numbers use format: title.chapter-section (e.g., 58.1-320)

Example:
    >>> from atlas.converters.us_states.va import VAConverter
    >>> converter = VAConverter()
    >>> section = converter.fetch_section("58.1-301")
    >>> print(section.section_title)
    "Conformity to Internal Revenue Code"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://law.lis.virginia.gov"

# Virginia title mapping for reference
VA_TITLES: dict[str, str] = {
    "1": "General Provisions",
    "2.2": "Administration of Government",
    "3.2": "Agricultural, Animal Care, and Food",
    "4.1": "Alcoholic Beverage and Cannabis Control",
    "5.1": "Aviation",
    "6.2": "Financial Institutions and Services",
    "7.1": "Charitable and Philanthropic Institutions",
    "8.01": "Civil Remedies and Procedure",
    "10.1": "Conservation",
    "15.2": "Counties, Cities and Towns",
    "18.2": "Crimes and Offenses Generally",
    "19.2": "Criminal Procedure",
    "22.1": "Education",
    "23.1": "Institutions of Higher Education",
    "24.2": "Elections",
    "28.2": "Fisheries and Habitat of the Tidal Waters",
    "29.1": "Game, Inland Fisheries and Boating",
    "30": "General Assembly",
    "32.1": "Health",
    "33.2": "Highways and Other Surface Transportation Systems",
    "36": "Housing",
    "37.2": "Behavioral Health and Developmental Services",
    "38.2": "Insurance",
    "40.1": "Labor and Employment",
    "44": "Military and Emergency Laws",
    "45.2": "Mines and Mining",
    "46.2": "Motor Vehicles",
    "51.1": "Pensions, Benefits, and Retirement",
    "55.1": "Property and Conveyances",
    "58.1": "Taxation",
    "59.1": "Trade and Commerce",
    "60.2": "Workers' Compensation",
    "62.1": "Waters of the State, Ports and Harbors",
    "63.2": "Welfare (Social Services)",
    "64.2": "Wills, Trusts, and Fiduciaries",
    "65.2": "Workers' Compensation",
}

# Key titles for tax/benefit analysis
VA_TAX_TITLES: dict[str, str] = {
    "58.1": "Taxation",
}

VA_WELFARE_TITLES: dict[str, str] = {
    "63.2": "Welfare (Social Services)",
}


@dataclass
class ParsedVASection:
    """Parsed Virginia statute section."""

    section_number: str  # e.g., "58.1-320"
    section_title: str  # e.g., "Tax rates"
    title_number: str  # e.g., "58.1"
    title_name: str  # e.g., "Taxation"
    chapter_number: str | None  # e.g., "3"
    chapter_name: str | None  # e.g., "Income Tax"
    article_number: str | None  # e.g., "2"
    article_name: str | None  # e.g., "Rates"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedVASubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedVASubsection:
    """A subsection within a Virginia statute."""

    identifier: str  # e.g., "A", "1", "a"
    text: str
    children: list["ParsedVASubsection"] = field(default_factory=list)


class VAConverterError(Exception):
    """Error during Virginia statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class VAConverter:
    """Converter for Virginia Code HTML to internal Section model.

    Example:
        >>> converter = VAConverter()
        >>> section = converter.fetch_section("58.1-301")
        >>> print(section.citation.section)
        "VA-58.1-301"

        >>> for section in converter.iter_title("58.1"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Virginia statute converter.

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

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "58.1-320", "63.2-100"

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/vacode/{section_number}/"

    def _build_title_url(self, title_number: str) -> str:
        """Build the URL for a title's table of contents."""
        return f"{BASE_URL}/vacode/title{title_number}/"

    def _build_chapter_url(self, title_number: str, chapter_number: str) -> str:
        """Build the URL for a chapter's table of contents."""
        return f"{BASE_URL}/vacode/title{title_number}/chapter{chapter_number}/"

    def _extract_title_from_section(self, section_number: str) -> str:
        """Extract title number from section number.

        Args:
            section_number: e.g., "58.1-320" -> "58.1", "63.2-100" -> "63.2"

        Returns:
            Title number as string
        """
        if "-" in section_number:
            return section_number.split("-")[0]
        return section_number  # pragma: no cover

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedVASection:
        """Parse section HTML into ParsedVASection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        error_patterns = ["cannot be found", "not found", "does not exist", "404"]
        html_lower = html.lower()
        if any(pattern in html_lower for pattern in error_patterns):
            raise VAConverterError(f"Section {section_number} not found", url)

        title_number = self._extract_title_from_section(section_number)
        title_name = VA_TITLES.get(title_number, f"Title {title_number}")

        # Extract section title from the heading
        # Pattern: "§ 58.1-320. Tax rates" or similar
        section_title = ""

        # Try to find h1 or h2 with section heading
        for heading in soup.find_all(["h1", "h2", "h3"]):
            heading_text = heading.get_text(strip=True)
            # Match patterns like "§ 58.1-320. Tax rates" or "58.1-320. Tax rates"
            title_pattern = re.compile(
                rf"(?:§\s*)?{re.escape(section_number)}[.\s]+(.+?)(?:\s*$)", re.IGNORECASE
            )
            match = title_pattern.search(heading_text)
            if match:
                section_title = match.group(1).strip()
                break

        # Try finding from page title if not found
        if not section_title:
            title_tag = soup.find("title")
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                title_pattern = re.compile(
                    rf"(?:§\s*)?{re.escape(section_number)}[.\s]+(.+?)(?:\s*[-|]|$)", re.IGNORECASE
                )
                match = title_pattern.search(title_text)
                if match:
                    section_title = match.group(1).strip()  # pragma: no cover

        # Extract chapter/article info from breadcrumb or headings
        chapter_number = None
        chapter_name = None
        article_number = None
        article_name = None

        # Look for chapter info in breadcrumb or headings
        breadcrumb = soup.find(class_="breadcrumb") or soup.find(["nav", "ol", "ul"])
        if breadcrumb:
            breadcrumb_text = breadcrumb.get_text()
            chapter_match = re.search(r"Chapter\s+(\d+[A-Za-z]?)[.\s:]+([^»\n]+)", breadcrumb_text)
            if chapter_match:
                chapter_number = chapter_match.group(1)
                chapter_name = chapter_match.group(2).strip()

            article_match = re.search(r"Article\s+(\d+[A-Za-z]?)[.\s:]+([^»\n]+)", breadcrumb_text)
            if article_match:
                article_number = article_match.group(1)
                article_name = article_match.group(2).strip()

        # Get body content - try various containers
        content_elem = (
            soup.find("div", id="vacode")
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

        # Extract history note - Virginia uses "Code 1950..." or year citations
        history = None
        history_patterns = [
            # Match "Code 1950" through end of that paragraph/line
            r"(Code 1950[^\n§]*(?:;\s*\d{4}[^\n§]*)*)",
            # Match standalone year citations like "2024, cc. 37, 89"
            r"(\d{4},\s+cc?\.\s*\d+[^\n§]*(?:;\s*\d{4}[^\n§]*)*)",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.DOTALL)
            if history_match:
                history = history_match.group(1).strip()[:2000]  # Limit length
                break

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedVASection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_number,
            title_name=title_name,
            chapter_number=chapter_number,
            chapter_name=chapter_name,
            article_number=article_number,
            article_name=article_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedVASubsection]:
        """Parse hierarchical subsections from text.

        Virginia statutes typically use:
        - A., B., C. for primary divisions (uppercase letters with period)
        - 1., 2., 3. for secondary divisions
        - a., b., c. or (a), (b), (c) for tertiary divisions
        """
        subsections = []

        # Split by top-level subsections (A., B., etc.)
        # Look for patterns like "A. " at start of lines or after newline
        # Also handle cases where there's no newline (HTML paragraphs converted to text)
        parts = re.split(r"(?:^|\n)([A-Z])\.\s", text)

        # parts will be: [text_before, 'A', content_A, 'B', content_B, ...]
        if len(parts) > 1:
            i = 1
            while i < len(parts) - 1:
                identifier = parts[i]
                content = parts[i + 1]

                # Parse second-level children (1., 2., etc.)
                children = self._parse_level2(content)

                # Get text before first child
                if children:
                    first_child_match = re.search(r"(?:^|\n)\d+\.\s", content)
                    direct_text = (
                        content[: first_child_match.start()].strip()
                        if first_child_match
                        else content.strip()
                    )
                else:  # pragma: no cover
                    direct_text = content.strip()

                subsections.append(
                    ParsedVASubsection(
                        identifier=identifier,
                        text=direct_text[:2000],  # Limit text size
                        children=children,
                    )
                )
                i += 2

        # If no uppercase letter subsections found, try numbered format (1), (2), etc.
        if not subsections:
            subsections = self._parse_numbered_subsections(text)

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedVASubsection]:
        """Parse level 2 subsections (1., 2., etc.)."""
        subsections = []

        # Split by numbered subsections like "1. " at start or after newline
        parts = re.split(r"(?:^|\n)(\d+)\.\s", text)

        # parts will be: [text_before, '1', content_1, '2', content_2, ...]
        if len(parts) > 1:
            i = 1
            while i < len(parts) - 1:
                identifier = parts[i]
                content = parts[i + 1]

                # Parse level 3 children (a., b., etc.)
                children = self._parse_level3(content)

                if children:
                    first_child_match = re.search(r"(?:^|\n)[a-z]\.\s|\([a-z]\)", content)
                    direct_text = (
                        content[: first_child_match.start()].strip()
                        if first_child_match
                        else content.strip()
                    )
                else:
                    direct_text = content.strip()

                subsections.append(
                    ParsedVASubsection(
                        identifier=identifier,
                        text=direct_text[:2000],
                        children=children,
                    )
                )
                i += 2

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedVASubsection]:
        """Parse level 3 subsections (a., b., or (a), (b), etc.)."""
        subsections = []

        # Try "a. " format first (split capturing the letter)
        parts = re.split(r"(?:^|\n)([a-z])\.\s", text)

        # parts will be: [text_before, 'a', content_a, 'b', content_b, ...]
        if len(parts) > 2:  # More than just pre-text + one letter + content
            i = 1
            while i < len(parts) - 1:
                identifier = parts[i]
                content = parts[i + 1]

                subsections.append(
                    ParsedVASubsection(
                        identifier=identifier,
                        text=content.strip()[:2000],
                        children=[],
                    )
                )
                i += 2
        else:
            # Try (a), (b) format
            parts = re.split(r"\(([a-z])\)\s*", text)
            if len(parts) > 2:  # pragma: no cover
                i = 1
                while i < len(parts) - 1:
                    identifier = parts[i]
                    content = parts[i + 1]

                    subsections.append(
                        ParsedVASubsection(
                            identifier=identifier,
                            text=content.strip()[:2000],
                            children=[],
                        )
                    )
                    i += 2

        return subsections

    def _parse_numbered_subsections(self, text: str) -> list[ParsedVASubsection]:
        """Parse numbered subsections (1), (2), etc. as fallback."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit size and stop at next subsection
            next_subsection = re.search(r"\(\d+\)", content)
            if next_subsection:  # pragma: no cover
                content = content[: next_subsection.start()]

            subsections.append(
                ParsedVASubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedVASection) -> Section:
        """Convert ParsedVASection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"VA-{parsed.section_number}",
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
            title_name=f"Code of Virginia - Title {parsed.title_number}. {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"va/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "58.1-301", "63.2-100"

        Returns:
            Section model

        Raises:
            VAConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_title_section_numbers(self, title_number: str) -> list[str]:
        """Get list of section numbers in a title.

        Args:
            title_number: Title number (e.g., "58.1")

        Returns:
            List of section numbers (e.g., ["58.1-1", "58.1-2", ...])
        """
        url = self._build_title_url(title_number)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links: /vacode/58.1-320/
        pattern = re.compile(rf"/vacode/({re.escape(title_number)}-[\d.]+(?:[A-Za-z])?)/")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def get_chapter_section_numbers(self, title_number: str, chapter_number: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title_number: Title number (e.g., "58.1")
            chapter_number: Chapter number (e.g., "3")

        Returns:
            List of section numbers (e.g., ["58.1-300", "58.1-301", ...])
        """
        url = self._build_chapter_url(title_number, chapter_number)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links
        pattern = re.compile(rf"/vacode/({re.escape(title_number)}-[\d.]+(?:[A-Za-z])?)/")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_title(self, title_number: str) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title_number: Title number (e.g., "58.1")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_title_section_numbers(title_number)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except VAConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapter(
        self,
        title_number: str,
        chapter_number: str,
    ) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title_number: Title number (e.g., "58.1")
            chapter_number: Chapter number (e.g., "3")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title_number, chapter_number)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except VAConverterError as e:  # pragma: no cover
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_titles(
        self,
        title_numbers: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple titles.

        Args:
            title_numbers: List of title numbers (default: tax titles)

        Yields:
            Section objects
        """
        if title_numbers is None:  # pragma: no cover
            title_numbers = list(VA_TAX_TITLES.keys())  # pragma: no cover

        for title in title_numbers:  # pragma: no cover
            yield from self.iter_title(title)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "VAConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_va_section(section_number: str) -> Section:
    """Fetch a single Virginia statute section.

    Args:
        section_number: e.g., "58.1-301"

    Returns:
        Section model
    """
    with VAConverter() as converter:
        return converter.fetch_section(section_number)


def download_va_chapter(title_number: str, chapter_number: str) -> list[Section]:
    """Download all sections from a Virginia Code chapter.

    Args:
        title_number: Title number (e.g., "58.1")
        chapter_number: Chapter number (e.g., "3")

    Returns:
        List of Section objects
    """
    with VAConverter() as converter:
        return list(converter.iter_chapter(title_number, chapter_number))


def download_va_tax_title() -> Iterator[Section]:
    """Download all sections from Virginia tax title (58.1).

    Yields:
        Section objects
    """
    with VAConverter() as converter:  # pragma: no cover
        yield from converter.iter_title("58.1")  # pragma: no cover


def download_va_welfare_title() -> Iterator[Section]:
    """Download all sections from Virginia welfare title (63.2).

    Yields:
        Section objects
    """
    with VAConverter() as converter:  # pragma: no cover
        yield from converter.iter_title("63.2")  # pragma: no cover
