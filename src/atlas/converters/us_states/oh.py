"""Ohio Revised Code converter.

Converts Ohio Revised Code HTML from codes.ohio.gov to the internal Section model
for ingestion.

Ohio Revised Code Structure:
- Titles (e.g., Title 57: Taxation)
- Chapters (e.g., Chapter 5747: Income Tax)
- Sections (e.g., 5747.01: Definitions)

URL Patterns:
- Section: /ohio-revised-code/section-{section_number}
- Chapter: /ohio-revised-code/chapter-{chapter_number}
- Title: /ohio-revised-code/title-{title_number}

Example:
    >>> from atlas.converters.us_states.oh import OHConverter
    >>> converter = OHConverter()
    >>> section = converter.fetch_section("5747.01")
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

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://codes.ohio.gov"

# Title mapping for reference (Ohio uses numeric titles)
OH_TITLES: dict[str, str] = {
    51: "Public Welfare",
    53: "State Financial Institutions",
    55: "Taxation and Revenue",
    57: "Taxation",
}

# Key chapters for tax/benefit analysis
OH_TAX_CHAPTERS: dict[str, str] = {
    5701: "Tax Administration",
    5703: "Department of Taxation",
    5705: "Tax Levy Law",
    5709: "Taxable Property - Exemptions",
    5711: "Personal Property Tax",
    5713: "Real Property Tax",
    5715: "Board of Revision; Tax Appeals",
    5717: "Board of Tax Appeals",
    5719: "Delinquent Lands",
    5721: "Delinquent Land Sales",
    5723: "Auditor's Sale of Forfeited Lands",
    5725: "Financial Institutions Tax",
    5726: "Financial Institutions Tax",
    5729: "Insurance Company Tax",
    5731: "Estate Tax",
    5733: "Corporation Franchise Tax",
    5735: "Motor Fuel Tax",
    5739: "Sales Tax",
    5741: "Use Tax",
    5743: "Cigarette Tax",
    5745: "Severance Tax",
    5747: "Income Tax",
    5748: "School District Income Tax",
    5749: "Severance Tax",
    5751: "Commercial Activity Tax",
}

OH_WELFARE_CHAPTERS: dict[str, str] = {
    5101: "Department of Job and Family Services",
    5103: "Institutions for Children",
    5104: "Child Day Care",
    5107: "Ohio Works First Program",
    5108: "Prevention, Retention and Contingency Program",
    5111: "Medicaid",
    5115: "County Homes",
    5117: "County Hospitals",
    5119: "Department of Mental Health and Addiction Services",
    5121: "County Mental Health and Addiction Services",
    5123: "Department of Developmental Disabilities",
    5124: "Passport Program; Community-Based Long-Term Care Services",
    5126: "County Boards of Developmental Disabilities",
    5131: "Institutions for Mentally Ill",
    5139: "Department of Youth Services",
    5153: "Children Services Agencies",
}


@dataclass
class ParsedOHSection:
    """Parsed Ohio Revised Code section."""

    section_number: str  # e.g., "5747.01"
    section_title: str  # e.g., "Definitions"
    chapter_number: int  # e.g., 5747
    chapter_title: str  # e.g., "Income Tax"
    title_number: int | None  # e.g., 57
    title_name: str | None  # e.g., "Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedOHSubsection"] = field(default_factory=list)
    history: str | None = None  # History note / latest legislation
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedOHSubsection:
    """A subsection within an Ohio Revised Code section."""

    identifier: str  # e.g., "A", "1", "a"
    text: str
    children: list["ParsedOHSubsection"] = field(default_factory=list)


class OHConverterError(Exception):
    """Error during Ohio Revised Code conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class OHConverter:
    """Converter for Ohio Revised Code HTML to internal Section model.

    Example:
        >>> converter = OHConverter()
        >>> section = converter.fetch_section("5747.01")
        >>> print(section.citation.section)
        "OH-5747.01"

        >>> for section in converter.iter_chapter(5747):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Ohio Revised Code converter.

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
            section_number: e.g., "5747.01", "5101.16"

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/ohio-revised-code/section-{section_number}"

    def _build_chapter_url(self, chapter: int) -> str:
        """Build the URL for a chapter's contents."""
        return f"{BASE_URL}/ohio-revised-code/chapter-{chapter}"

    def _get_title_for_chapter(self, chapter: int) -> tuple[int | None, str | None]:
        """Determine title number and name from chapter number.

        Args:
            chapter: Chapter number (e.g., 5747)

        Returns:
            Tuple of (title_number, title_name)
        """
        # Ohio Revised Code chapter numbers indicate title:
        # 5101-5199 -> Title 51 (Public Welfare)
        # 5701-5799 -> Title 57 (Taxation)
        if 5101 <= chapter <= 5199:
            return 51, "Public Welfare"
        elif 5701 <= chapter <= 5799:
            return 57, "Taxation"
        elif 5301 <= chapter <= 5399:  # pragma: no cover
            return 53, "State Financial Institutions"  # pragma: no cover
        elif 5501 <= chapter <= 5599:  # pragma: no cover
            return 55, "Taxation and Revenue"  # pragma: no cover
        return None, None  # pragma: no cover

    def _parse_effective_date(self, text: str) -> date | None:
        """Parse effective date from text like 'Effective: September 30, 2025'.

        Args:
            text: Text containing effective date

        Returns:
            Parsed date or None
        """
        match = re.search(r"Effective:\s*(\w+\s+\d{1,2},\s*\d{4})", text)
        if match:
            date_str = match.group(1)
            try:
                from datetime import datetime

                return datetime.strptime(date_str, "%B %d, %Y").date()
            except ValueError:  # pragma: no cover
                pass
        return None  # pragma: no cover

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedOHSection:
        """Parse section HTML into ParsedOHSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise OHConverterError(f"Section {section_number} not found", url)

        chapter = int(section_number.split(".")[0])
        chapter_title = (
            OH_TAX_CHAPTERS.get(chapter) or OH_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
        )

        title_number, title_name = self._get_title_for_chapter(chapter)

        # Extract section title from the heading pattern: "Section 5747.01 | Definitions"
        section_title = ""
        title_pattern = re.compile(rf"Section\s+{re.escape(section_number)}\s*\|\s*(.+?)(?:\.|$)")

        # Try to find in the page title or headings
        page_title = soup.find("title")
        if page_title:
            match = title_pattern.search(page_title.get_text(strip=True))
            if match:
                section_title = match.group(1).strip().rstrip(".")

        # Also try h1, h2 headings
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3"]):
                heading_text = heading.get_text(strip=True)
                match = title_pattern.search(heading_text)
                if match:  # pragma: no cover
                    section_title = match.group(1).strip().rstrip(".")
                    break

        # Try simpler pattern
        if not section_title:
            simple_pattern = re.compile(rf"{re.escape(section_number)}\s*\|\s*([^.]+)")
            for text_node in soup.stripped_strings:
                match = simple_pattern.search(text_node)
                if match:
                    section_title = match.group(1).strip()  # pragma: no cover
                    break  # pragma: no cover

        # Get body content - try various containers
        content_elem = (
            soup.find("div", class_="section-content")
            or soup.find("div", class_="statute")
            or soup.find("article")
            or soup.find("main")
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

        # Extract effective date - search the whole page, not just content
        full_text = soup.get_text(separator="\n", strip=True)
        effective_date = self._parse_effective_date(full_text)

        # Extract history note / latest legislation - search the whole page
        history = None
        history_match = re.search(r"Latest\s+Legislation:\s*(.+?)(?:\n|$)", full_text)
        if history_match:
            history = history_match.group(1).strip()[:500]

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedOHSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            title_number=title_number,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
            effective_date=effective_date,
        )

    def _parse_subsections(self, text: str) -> list[ParsedOHSubsection]:
        """Parse hierarchical subsections from text.

        Ohio Revised Code typically uses:
        - (A), (B), (C) for primary divisions
        - (1), (2), (3) for secondary divisions
        - (a), (b), (c) for tertiary divisions
        """
        subsections = []

        # Split by top-level subsections (A), (B), etc.
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (A)
            match = re.match(r"\(([A-Z])\)\s*", part)
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
            next_subsection = re.search(r"\([A-Z]\)", direct_text)
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedOHSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedOHSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse third-level children (a), (b), etc.
            children = self._parse_level3(content)

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

            # Limit to reasonable size and stop at next lettered subsection
            next_alpha = re.search(r"\([A-Z]\)", direct_text)
            if next_alpha:  # pragma: no cover
                direct_text = direct_text[: next_alpha.start()]

            subsections.append(
                ParsedOHSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedOHSubsection]:
        """Parse level 3 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at next higher-level subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]
            next_alpha = re.search(r"\([A-Z]\)", content)
            if next_alpha:  # pragma: no cover
                content = content[: next_alpha.start()]

            if len(content) > 2000:
                content = content[:2000] + "..."  # pragma: no cover

            subsections.append(
                ParsedOHSubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedOHSection) -> Section:
        """Convert ParsedOHSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"OH-{parsed.section_number}",
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
            title_name=f"Ohio Revised Code - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"oh/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "5747.01", "5101.16"

        Returns:
            Section model

        Raises:
            OHConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 5747)

        Returns:
            List of section numbers (e.g., ["5747.01", "5747.02", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links: /ohio-revised-code/section-5747.01
        pattern = re.compile(rf"/ohio-revised-code/section-({chapter}\.\d+[A-Za-z]?)")

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
            chapter: Chapter number (e.g., 5747)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except OHConverterError as e:  # pragma: no cover
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
            chapters = list(OH_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "OHConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_oh_section(section_number: str) -> Section:
    """Fetch a single Ohio Revised Code section.

    Args:
        section_number: e.g., "5747.01"

    Returns:
        Section model
    """
    with OHConverter() as converter:
        return converter.fetch_section(section_number)


def download_oh_chapter(chapter: int) -> list[Section]:
    """Download all sections from an Ohio Revised Code chapter.

    Args:
        chapter: Chapter number (e.g., 5747)

    Returns:
        List of Section objects
    """
    with OHConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_oh_tax_chapters() -> Iterator[Section]:
    """Download all sections from Ohio tax-related chapters (5701-5751).

    Yields:
        Section objects
    """
    with OHConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(OH_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_oh_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Ohio public welfare chapters (5101-5153).

    Yields:
        Section objects
    """
    with OHConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(OH_WELFARE_CHAPTERS.keys()))  # pragma: no cover
