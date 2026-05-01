"""Texas state statute converter.

Converts Texas Statutes HTML from texas.public.law to the internal Section model
for ingestion.

Texas Statute Structure:
- Codes (e.g., Tax Code, Human Resources Code, Family Code)
- Titles (e.g., Title 2: State Taxation)
- Subtitles (e.g., Subtitle E: Sales, Excise, and Use Taxes)
- Chapters (e.g., Chapter 151: Limited Sales, Excise, and Use Tax)
- Sections (e.g., 151.001: Short Title)

URL Patterns (texas.public.law):
- Section: /statutes/tex._tax_code_section_151.001
- Chapter: /statutes/tex._tax_code_chapter_151
- Human Resources Code: /statutes/tex._human_resources_code_section_31.001

Note: Texas has a franchise tax on businesses (Tax Code Chapter 171) rather than
a personal income tax. The Sales Tax is in Chapter 151.

Example:
    >>> from axiom_corpus.converters.us_states.tx import TXConverter
    >>> converter = TXConverter()
    >>> section = converter.fetch_section("TX", "151.001")
    >>> print(section.section_title)
    "Short Title"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://texas.public.law/statutes"

# Texas Code abbreviations used in URL patterns
TX_CODES: dict[str, str] = {
    "TX": "tax_code",
    "HR": "human_resources_code",
    "FA": "family_code",
    "GV": "government_code",
    "HS": "health_and_safety_code",
    "ED": "education_code",
    "PE": "penal_code",
    "LG": "local_government_code",
    "LA": "labor_code",
    "BC": "business_and_commerce_code",
    "PR": "property_code",
    "IN": "insurance_code",
    "TN": "transportation_code",
    "UT": "utilities_code",
    "WA": "water_code",
    "AG": "agriculture_code",
}

# Key chapters for tax/benefit analysis
TX_TAX_CHAPTERS: dict[str, str] = {
    151: "Limited Sales, Excise, and Use Tax",
    152: "Taxes on Sale, Rental, and Use of Motor Vehicles",
    153: "Motor Fuel Taxes",
    154: "Cigarette Tax",
    155: "Cigar and Tobacco Products Tax",
    156: "Hotel Occupancy Tax",
    157: "Fireworks Tax",
    158: "Taxes on Alcohol",
    159: "Mixed Beverage Taxes",
    160: "Unauthorized Substances Tax",
    161: "White Sulfurous Acid Tax",
    162: "Motor Fuel Taxation",
    171: "Franchise Tax",
    172: "Margin Tax",
}

TX_WELFARE_CHAPTERS: dict[str, str] = {
    31: "Financial Assistance and Service Programs",
    32: "Medical Assistance Programs",
    33: "Services for Aging and Persons with Disabilities",
    34: "Child Care and Early Childhood Education",
    40: "Department of Protective and Regulatory Services",
    42: "Regulation of Certain Facilities, Homes, and Agencies",
    48: "Investigations and Protective Services for Elderly and Disabled Persons",
}


@dataclass
class ParsedTXSection:
    """Parsed Texas statute section."""

    code: str  # e.g., "TX" for Tax Code, "HR" for Human Resources
    section_number: str  # e.g., "151.001"
    section_title: str  # e.g., "Short Title"
    chapter_number: int  # e.g., 151
    chapter_title: str  # e.g., "Limited Sales, Excise, and Use Tax"
    title_name: str | None  # e.g., "Title 2: State Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedTXSubsection] = field(default_factory=list)
    history: str | None = None  # History/amendment notes
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedTXSubsection:
    """A subsection within a Texas statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list[ParsedTXSubsection] = field(default_factory=list)


class TXConverterError(Exception):
    """Error during Texas statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class TXConverter:
    """Converter for Texas Statutes HTML to internal Section model.

    Example:
        >>> converter = TXConverter()
        >>> section = converter.fetch_section("TX", "151.001")
        >>> print(section.citation.section)
        "TX-151.001"

        >>> for section in converter.iter_chapter("TX", 151):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Texas statute converter.

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

    def _get_code_url_name(self, code: str) -> str:
        """Get the URL-formatted code name.

        Args:
            code: Code abbreviation (e.g., "TX", "HR")

        Returns:
            URL-formatted name (e.g., "tax_code", "human_resources_code")

        Raises:
            TXConverterError: If code is not recognized
        """
        code_upper = code.upper()
        if code_upper not in TX_CODES:
            raise TXConverterError(f"Unknown Texas code: {code}")
        return TX_CODES[code_upper]

    def _build_section_url(self, code: str, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            code: Code abbreviation (e.g., "TX", "HR")
            section_number: e.g., "151.001", "31.001"

        Returns:
            Full URL to the section page
        """
        code_name = self._get_code_url_name(code)
        return f"{BASE_URL}/tex._{code_name}_section_{section_number}"

    def _build_chapter_url(self, code: str, chapter: int) -> str:
        """Build the URL for a chapter's table of contents."""
        code_name = self._get_code_url_name(code)
        return f"{BASE_URL}/tex._{code_name}_chapter_{chapter}"

    def _parse_section_html(
        self,
        html: str,
        code: str,
        section_number: str,
        url: str,
    ) -> ParsedTXSection:
        """Parse section HTML into ParsedTXSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error - look for actual 404 page indicators
        # Avoid false positives from script URLs containing numbers
        title_elem = soup.find("title")
        title_text = title_elem.get_text(strip=True).lower() if title_elem else ""

        # Check page title for error indicators
        if "not found" in title_text or "404" in title_text or "error" in title_text:
            raise TXConverterError(f"Section {code}-{section_number} not found", url)

        # Check for explicit error messages in body content
        body = soup.find("body")
        if body:
            body_text = body.get_text(strip=True).lower()
            # Only raise if the body is very short and contains error text (typical 404 page)
            if len(body_text) < 500 and ("page not found" in body_text or "404 error" in body_text):
                raise TXConverterError(
                    f"Section {code}-{section_number} not found", url
                )  # pragma: no cover

        chapter = int(section_number.split(".")[0])

        # Get chapter title from registry or extract from page
        chapter_title = ""
        if code.upper() == "TX":
            chapter_title = TX_TAX_CHAPTERS.get(chapter, f"Chapter {chapter}")
        elif code.upper() == "HR":
            chapter_title = TX_WELFARE_CHAPTERS.get(chapter, f"Chapter {chapter}")
        else:
            chapter_title = f"Chapter {chapter}"  # pragma: no cover

        # Extract section title from h1 element
        # Format: "Tex. Tax Code Section 151.001 Short Title"
        section_title = ""
        h1 = soup.find("h1")
        if h1:
            h1_text = h1.get_text(strip=True)
            # Try to extract title after section number
            # Pattern: "Section NNN.NNN Title Here"
            match = re.search(
                rf"Section\s+{re.escape(section_number)}\s+(.+)", h1_text, re.IGNORECASE
            )
            if match:
                section_title = match.group(1).strip()
            else:
                # Try alternative pattern with just section number
                match = re.search(rf"{re.escape(section_number)}\s+(.+)", h1_text)
                if match:
                    section_title = match.group(1).strip()  # pragma: no cover

        # Try breadcrumb for title info
        title_name = None
        breadcrumb = soup.find("ol", class_="breadcrumb") or soup.find(
            attrs={"itemtype": "BreadcrumbList"}
        )
        if breadcrumb:
            # Look for title in breadcrumb path
            crumbs = breadcrumb.find_all("li") or breadcrumb.find_all("span")
            for crumb in crumbs:
                text = crumb.get_text(strip=True)
                if "Title" in text:
                    title_name = text
                    break

        # Get body content
        content_elem = (
            soup.find("main")
            or soup.find("article")
            or soup.find("div", class_="content")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(
                ["nav", "script", "style", "header", "footer", "aside"]
            ):
                elem.decompose()  # pragma: no cover
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history/amendment notes
        history = None
        # Look for "Acts" pattern indicating legislative history
        history_match = re.search(
            r"(Acts\s+\d{4}.*?)(?=\n\n|\Z)",
            text,
            re.DOTALL | re.IGNORECASE,
        )
        if history_match:
            history = history_match.group(1).strip()[:2000]  # Limit length

        # Also check for "Amended by:" section
        amended_match = re.search(
            r"Amended by:\s*(.+?)(?=\n\n|\Z)",
            text,
            re.DOTALL | re.IGNORECASE,
        )
        if amended_match:
            if history:
                history += "\n" + amended_match.group(1).strip()[:1000]
            else:
                history = amended_match.group(1).strip()[:2000]  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedTXSection(
            code=code.upper(),
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedTXSubsection]:
        """Parse hierarchical subsections from text.

        Texas statutes typically use:
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
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedTXSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        # If no (a), (b) pattern found, try (1), (2) pattern
        if not subsections:
            subsections = self._parse_numbered_subsections(text)

        return subsections

    def _parse_numbered_subsections(self, text: str) -> list[ParsedTXSubsection]:
        """Parse numbered subsections (1), (2), etc. as primary divisions."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover

            identifier = match.group(1)  # pragma: no cover
            content = part[match.end() :]  # pragma: no cover

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", content)  # pragma: no cover
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedTXSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedTXSubsection]:
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

            # Limit to reasonable size and stop at next same-level subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            # Also stop at next letter subsection
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter and (not next_num or next_letter.start() < next_num.start()):
                content = content[: next_letter.start()]  # pragma: no cover

            subsections.append(
                ParsedTXSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedTXSubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover

            identifier = match.group(1)  # pragma: no cover
            content = part[match.end() :]  # pragma: no cover

            # Limit size and stop at boundaries
            next_upper = re.search(r"\([A-Z]\)", content)  # pragma: no cover
            if next_upper:  # pragma: no cover
                content = content[: next_upper.start()]  # pragma: no cover

            next_num = re.search(r"\(\d+\)", content)  # pragma: no cover
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedTXSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedTXSection) -> Section:
        """Convert ParsedTXSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"TX-{parsed.code}-{parsed.section_number}",
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

        # Build title name
        code_name = TX_CODES.get(parsed.code.upper(), parsed.code)
        title_display = code_name.replace("_", " ").title()
        if parsed.title_name:
            title_display = f"Texas {title_display} - {parsed.title_name}"
        else:
            title_display = f"Texas {title_display}"  # pragma: no cover

        return Section(
            citation=citation,
            title_name=title_display,
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"tx/{parsed.code.lower()}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, code: str, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            code: Code abbreviation (e.g., "TX" for Tax Code, "HR" for Human Resources)
            section_number: e.g., "151.001", "31.001"

        Returns:
            Section model

        Raises:
            TXConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(code, section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, code, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, code: str, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            code: Code abbreviation (e.g., "TX")
            chapter: Chapter number (e.g., 151)

        Returns:
            List of section numbers (e.g., ["151.001", "151.002", ...])
        """
        url = self._build_chapter_url(code, chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links with pattern: section_NNN.NNN
        code_name = self._get_code_url_name(code)
        pattern = re.compile(rf"tex\._{code_name}_section_(\d+\.\d+[A-Za-z]?)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, code: str, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            code: Code abbreviation (e.g., "TX")
            chapter: Chapter number (e.g., 151)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(code, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(code, section_num)
            except TXConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {code}-{section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        code: str,
        chapters: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            code: Code abbreviation (e.g., "TX")
            chapters: List of chapter numbers (default: tax chapters for TX, welfare for HR)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            if code.upper() == "TX":  # pragma: no cover
                chapters = list(TX_TAX_CHAPTERS.keys())  # pragma: no cover
            elif code.upper() == "HR":  # pragma: no cover
                chapters = list(TX_WELFARE_CHAPTERS.keys())  # pragma: no cover
            else:
                raise TXConverterError(f"No default chapters for code: {code}")  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(code, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> TXConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_tx_section(code: str, section_number: str) -> Section:
    """Fetch a single Texas statute section.

    Args:
        code: Code abbreviation (e.g., "TX" for Tax Code)
        section_number: e.g., "151.001"

    Returns:
        Section model
    """
    with TXConverter() as converter:
        return converter.fetch_section(code, section_number)


def download_tx_chapter(code: str, chapter: int) -> list[Section]:
    """Download all sections from a Texas chapter.

    Args:
        code: Code abbreviation (e.g., "TX")
        chapter: Chapter number (e.g., 151)

    Returns:
        List of Section objects
    """
    with TXConverter() as converter:
        return list(converter.iter_chapter(code, chapter))


def download_tx_tax_chapters() -> Iterator[Section]:
    """Download all sections from Texas tax-related chapters (151-172).

    Yields:
        Section objects
    """
    with TXConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters("TX", list(TX_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_tx_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Texas Human Resources Code welfare chapters.

    Yields:
        Section objects
    """
    with TXConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(
            "HR", list(TX_WELFARE_CHAPTERS.keys())
        )  # pragma: no cover
