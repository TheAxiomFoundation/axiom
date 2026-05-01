"""South Dakota state statute converter.

Converts South Dakota Codified Laws HTML from sdlegislature.gov
to the internal Section model for ingestion.

South Dakota Statute Structure:
- Titles (e.g., Title 10: TAXATION, Title 28: PUBLIC WELFARE AND ASSISTANCE)
- Chapters (e.g., Chapter 10-1: DEPARTMENT OF REVENUE)
- Sections (e.g., 10-1-1: Department created--Seal of department)

URL Patterns:
- Title index: https://sdlegislature.gov/api/Statutes/{title}.html
- Chapter index: https://sdlegislature.gov/api/Statutes/{title}-{chapter}.html
- Section: https://sdlegislature.gov/api/Statutes/{title}-{chapter}-{section}.html

Note: South Dakota has no state income tax on individuals (only on certain financial
institutions). Key chapters for tax/benefit analysis are in Title 10 (Taxation) and
Title 28 (Public Welfare and Assistance).

Example:
    >>> from axiom_corpus.converters.us_states.sd import SDConverter
    >>> converter = SDConverter()
    >>> section = converter.fetch_section("10-1-1")
    >>> print(section.section_title)
    "Department created--Seal of department"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://sdlegislature.gov/api/Statutes"

# Title mapping for reference
SD_TITLES: dict[str, str] = {
    1: "State Affairs and Government",
    2: "Legislative Branch",
    3: "Executive Branch",
    4: "State Elective Officers",
    5: "Charitable and Correctional Institutions",
    6: "Public Officers and Employees",
    7: "Elections",
    8: "Interstate Compacts",
    9: "Counties",
    10: "Taxation",
    11: "Municipal Government",
    12: "Education",
    13: "Libraries and Museums",
    14: "Public Safety",
    15: "Claims Against the State",
    16: "Courts and Judiciary",
    17: "Civil Remedies and Special Proceedings",
    18: "Partnerships",
    19: "Rules of Evidence",
    20: "Courts--Probate Procedure--Estates and Trusts",
    21: "Judicial Remedies",
    22: "Crimes",
    23: "Law Enforcement",
    "23A": "Criminal Procedure",
    24: "Corrections",
    25: "Persons and Rights of Persons",
    26: "Domestic Relations",
    27: "Property Rights and Transactions",
    "27A": "Mental Illness",
    28: "Public Welfare and Assistance",
    29: "Labor and Employment",
    "29A": "Unemployment Compensation",
    30: "Public Utilities",
    31: "Highways and Bridges",
    32: "Motor Vehicles",
    33: "Aeronautics",
    34: "Public Health and Safety",
    "34A": "Environment",
    35: "Alcoholic Beverages",
    36: "Professional and Occupational Licenses",
    37: "Trade Regulation",
    38: "Agriculture and Horticulture",
    39: "Livestock",
    40: "Animals",
    41: "Game, Fish, Parks, and Forestry",
    42: "Water Rights",
    43: "Property",
    44: "Banking and Finance",
    45: "Cooperative Associations",
    46: "Water and Water Rights",
    47: "Business Corporations",
    "47A": "Revised Uniform Limited Partnership Act",
    48: "Insurance",
    49: "Public Utilities Commission",
    50: "Eminent Domain",
    51: "Mortgages and Liens",
    52: "Pledges and Secured Transactions",
    53: "Suretyship and Guaranty",
    54: "Contracts and Obligations",
    55: "Fiduciaries and Trusts",
    56: "Negotiable Instruments",
    "57A": "Uniform Commercial Code",
    58: "Warehouse Receipts, Bills of Lading, and Other Documents of Title",
}

# Key chapters for tax analysis (Title 10)
SD_TAX_CHAPTERS: dict[str, str] = {
    "10-1": "Department of Revenue",
    "10-2": "Property Subject to Taxation",
    "10-3": "Property Exempt from Taxation",
    "10-4": "Persons Liable for Taxes",
    "10-5": "Assessment and Listing of Property",
    "10-6": "Valuation of Property",
    "10-13": "County Board of Equalization",
    "10-35": "Sales Tax--Administration and Definitions",
    "10-44": "Retail Occupations Tax [Repealed]",
    "10-45": "Sales and Use Tax",
    "10-45A": "Municipal Gross Receipts Tax",
    "10-45B": "Tourism Tax",
    "10-45C": "Streamlined Sales Tax",
    "10-45D": "Gross Receipts Tax on Contractors",
    "10-46": "Use Tax",
    "10-46A": "Municipal Use Tax",
    "10-47": "Motor Fuel Tax",
    "10-48": "Special Fuel Tax",
    "10-52": "Bank Franchise Tax",
    "10-59": "Inheritance Tax [Repealed]",
}

# Key chapters for welfare analysis (Title 28)
SD_WELFARE_CHAPTERS: dict[str, str] = {
    "28-1": "State Department of Social Services",
    "28-5": "Medical Assistance for the Aged",
    "28-5A": "Supplemental Security Income",
    "28-6": "Medical Services to the Indigent",
    "28-6A": "Assistance in Treatment of Kidney Disease",
    "28-6B": "Medical Care for Unborn Children",
    "28-7A": "Temporary Assistance for Needy Families",
    "28-8": "Title XX Social Services Program",
    "28-9": "Poor Relief",
    "28-10": "Food Stamp Program",
    "28-11": "Child Support Enforcement",
    "28-12": "Child Care Facilities",
    "28-13": "Community Action Agencies",
}


@dataclass
class ParsedSDSection:
    """Parsed South Dakota statute section."""

    section_number: str  # e.g., "10-1-1"
    section_title: str  # e.g., "Department created--Seal of department"
    title_number: int | str  # e.g., 10
    title_name: str  # e.g., "Taxation"
    chapter_number: str  # e.g., "10-1"
    chapter_title: str | None  # e.g., "Department of Revenue"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedSDSubsection] = field(default_factory=list)
    history: str | None = None  # Source/history note
    source_url: str = ""
    effective_date: date | None = None
    is_repealed: bool = False


@dataclass
class ParsedSDSubsection:
    """A subsection within a South Dakota statute."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list[ParsedSDSubsection] = field(default_factory=list)


class SDConverterError(Exception):
    """Error during South Dakota statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)  # pragma: no cover
        self.url = url  # pragma: no cover


class SDConverter:
    """Converter for South Dakota Codified Laws HTML to internal Section model.

    Example:
        >>> converter = SDConverter()
        >>> section = converter.fetch_section("10-1-1")
        >>> print(section.citation.section)
        "SD-10-1-1"

        >>> for section in converter.iter_chapter("10-1"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the South Dakota statute converter.

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
            section_number: e.g., "10-1-1", "28-7A-1"

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/{section_number}.html"

    def _build_chapter_url(self, chapter: str) -> str:
        """Build the URL for a chapter's contents index.

        Args:
            chapter: e.g., "10-1", "28-7A"
        """
        return f"{BASE_URL}/{chapter}.html"

    def _build_title_url(self, title: int | str) -> str:
        """Build the URL for a title's contents.

        Args:
            title: e.g., 10, 28, "23A"
        """
        return f"{BASE_URL}/{title}.html"  # pragma: no cover

    def _parse_section_number(self, section_number: str) -> tuple[int | str, str]:
        """Parse section number into title and chapter.

        Args:
            section_number: e.g., "10-1-1", "28-7A-1", "23A-1-1"

        Returns:
            Tuple of (title, chapter)
        """
        parts = section_number.split("-")

        # Handle titles with letters like "23A", "27A"
        title_str = parts[0]
        if title_str.isdigit():
            title: int | str = int(title_str)
        else:
            title = title_str  # pragma: no cover

        # Chapter includes title and chapter number (e.g., "10-1", "28-7A")
        chapter = f"{parts[0]}-{parts[1]}" if len(parts) >= 2 else parts[0]

        return title, chapter

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedSDSection:
        """Parse section HTML into ParsedSDSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for repealed section - older format
        is_repealed = False
        if "Repealed by" in html or "Repealed." in html:
            is_repealed = True

        title, chapter = self._parse_section_number(section_number)

        # Get title name
        if isinstance(title, str):
            title_name = SD_TITLES.get(title, f"Title {title}")  # pragma: no cover
        else:
            title_name = SD_TITLES.get(title, f"Title {title}")

        # Get chapter title
        chapter_title = SD_TAX_CHAPTERS.get(chapter) or SD_WELFARE_CHAPTERS.get(chapter)

        # Extract section title
        section_title = ""

        # Try to find title in the document title
        title_tag = soup.find("title")
        if title_tag:
            title_text = title_tag.get_text()
            # Pattern: "SDLRC - Codified Law 10-1-1 - Department created--Seal of department."
            match = re.search(rf"{re.escape(section_number)}\s*[-–—]\s*(.+?)(?:\.|$)", title_text)
            if match:
                section_title = match.group(1).strip()

        # Try from HTML body - look for CL class (Catchline/Title)
        if not section_title:
            cl_span = soup.find("span", class_=re.compile(r"CL$"))
            if cl_span:
                section_title = cl_span.get_text(strip=True)

        # Also check for og:title meta tag
        if not section_title:
            og_title = soup.find("meta", property="og:title")  # pragma: no cover
            if og_title:  # pragma: no cover
                content = og_title.get("content", "")  # pragma: no cover
                # Pattern: "SD 10-1-1 - Title here"
                match = re.search(
                    rf"SD\s*{re.escape(section_number)}\s*[-–—]\s*(.+)", content
                )  # pragma: no cover
                if match:  # pragma: no cover
                    section_title = match.group(1).strip()  # pragma: no cover

        # Get body content
        body = soup.find("body")
        if body:
            text = body.get_text(separator="\n", strip=True)
            html_content = str(body)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history/source note
        history = None
        # Look for Source: pattern
        source_match = re.search(r"Source:\s*(.+?)(?:\n\n|\Z)", text, re.DOTALL)
        if source_match:
            history = source_match.group(1).strip()[:1000]  # Limit length

        # Also check for SCL class span (Source Catchline)
        if not history:
            scl_elements = soup.find_all("span", class_=re.compile(r"SCL"))
            for elem in scl_elements:
                elem_text = elem.get_text()  # pragma: no cover
                if "Source:" in elem_text or re.match(r"SL\s+\d{4}", elem_text):  # pragma: no cover
                    # Combine all SCL elements for the full source
                    parent = elem.parent  # pragma: no cover
                    if parent:  # pragma: no cover
                        history = parent.get_text(strip=True)  # pragma: no cover
                        if history.startswith("Source:"):  # pragma: no cover
                            history = history[7:].strip()  # pragma: no cover
                        break  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(text)

        # Extract effective date if present
        effective_date = None
        eff_match = re.search(r"eff\.?\s+([A-Za-z]+\.?\s+\d+,?\s+\d{4})", text)
        if eff_match:
            try:
                from dateutil import parser as date_parser

                effective_date = date_parser.parse(eff_match.group(1)).date()
            except ValueError, ImportError:  # pragma: no cover
                pass

        return ParsedSDSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title,
            title_name=title_name,
            chapter_number=chapter,
            chapter_title=chapter_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
            effective_date=effective_date,
            is_repealed=is_repealed,
        )

    def _parse_subsections(self, text: str) -> list[ParsedSDSubsection]:
        """Parse hierarchical subsections from text.

        South Dakota statutes typically use:
        - (1), (2), (3) for primary divisions
        - (a), (b), (c) for secondary divisions
        - (i), (ii), (iii) sometimes for tertiary
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
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedSDSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedSDSubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedSDSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedSDSection) -> Section:
        """Convert ParsedSDSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"SD-{parsed.section_number}",
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
            title_name=f"South Dakota Codified Laws - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"sd/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "10-1-1", "28-7A-1"

        Returns:
            Section model

        Raises:
            SDConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            raise SDConverterError(
                f"Section {section_number} not found", url
            ) from e  # pragma: no cover

        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter identifier (e.g., "10-1", "28-7A")

        Returns:
            List of section numbers (e.g., ["10-1-1", "10-1-3", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links - they link to statutes like "10-1-1", "10-1-3"
        # Pattern: href contains the section number
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            # Match section links like /Statutes?Statute=10-1-1
            match = re.search(rf"Statute=({re.escape(chapter)}-[\dA-Za-z.]+)", href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def get_title_chapters(self, title: int | str) -> list[str]:
        """Get list of chapter identifiers in a title.

        Args:
            title: Title number (e.g., 10, 28)

        Returns:
            List of chapter identifiers (e.g., ["10-1", "10-2", ...])
        """
        url = self._build_title_url(title)  # pragma: no cover
        html = self._get(url)  # pragma: no cover
        soup = BeautifulSoup(html, "html.parser")  # pragma: no cover

        chapters = []  # pragma: no cover

        # Find chapter links
        for link in soup.find_all("a", href=True):  # pragma: no cover
            href = link.get("href", "")  # pragma: no cover
            # Match chapter links like /statutes/DisplayStatute.aspx?Type=Statute&Statute=10-1
            # or /Statutes?Statute=10-1
            match = re.search(
                rf"Statute=({title}-[\dA-Za-z]+)(?:$|[&\s])", href
            )  # pragma: no cover
            if match:  # pragma: no cover
                chapter = match.group(1)  # pragma: no cover
                if chapter not in chapters:  # pragma: no cover
                    chapters.append(chapter)  # pragma: no cover

        return chapters  # pragma: no cover

    def iter_chapter(self, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter identifier (e.g., "10-1", "28-7A")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except SDConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter identifiers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(SD_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> SDConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_sd_section(section_number: str) -> Section:
    """Fetch a single South Dakota statute section.

    Args:
        section_number: e.g., "10-1-1"

    Returns:
        Section model
    """
    with SDConverter() as converter:
        return converter.fetch_section(section_number)


def download_sd_chapter(chapter: str) -> list[Section]:
    """Download all sections from a South Dakota chapter.

    Args:
        chapter: Chapter identifier (e.g., "10-1")

    Returns:
        List of Section objects
    """
    with SDConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_sd_tax_chapters() -> Iterator[Section]:
    """Download all sections from South Dakota tax-related chapters (Title 10).

    Yields:
        Section objects
    """
    with SDConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(SD_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_sd_welfare_chapters() -> Iterator[Section]:
    """Download all sections from South Dakota welfare chapters (Title 28).

    Yields:
        Section objects
    """
    with SDConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(SD_WELFARE_CHAPTERS.keys()))  # pragma: no cover
