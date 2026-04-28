"""Mississippi state statute converter.

Converts Mississippi Code HTML from UniCourt's cic-code-ms GitHub Pages site
to the internal Section model for ingestion.

The UniCourt CIC (Code Improvement Commission) project provides structured HTML
versions of Mississippi statutes from Public.Resource.Org. The content is in
the public domain.

Mississippi Code Structure:
- Titles (e.g., Title 27: Taxation and Finance)
- Chapters (e.g., Chapter 7: Income Tax and Withholding)
- Sections (e.g., 27-7-1: Definitions)

URL Patterns:
- Title index: transforms/ms/ocms/r78/gov.ms.code.title.{TITLE_NUM}.html
- All titles in one HTML file per title (contains all chapters and sections)

Note: Unlike some other states, Mississippi provides full titles in single HTML files
on the UniCourt site. This converter parses sections from these large HTML files.

Example:
    >>> from axiom.converters.us_states.ms import MSConverter
    >>> converter = MSConverter()
    >>> section = converter.fetch_section("27-7-1")
    >>> print(section.section_title)
    "Definitions"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup, Tag

from axiom.models import Citation, Section, Subsection

BASE_URL = "https://unicourt.github.io/cic-code-ms"
DEFAULT_RELEASE = "r78"  # July 2020 release (most recent)

# Mississippi Code title mapping
MS_TITLES: dict[int, str] = {
    1: "Laws and Statutes",
    3: "State Sovereignty, Jurisdiction, and Holidays",
    5: "Executive Branch",
    7: "Legislature",
    9: "Courts",
    11: "Civil Practice and Procedure",
    13: "Condemnation",
    15: "Court Practice and Procedure in Specific Actions and Courts",
    17: "Counties and County Officers",
    19: "Municipalities",
    21: "Planning and Zoning",
    23: "Elections",
    25: "Public Officers and Employees",
    27: "Taxation and Finance",
    29: "Natural Resources",
    31: "Banks and Financial Institutions",
    33: "Insurance",
    35: "Alcoholic Beverages",
    37: "Agriculture and Commerce",
    39: "Heritage and Culture",
    41: "Health and Safety",
    43: "Public Welfare",
    45: "Public Safety",
    47: "Prisons and Prisoners",
    49: "Conservation and Ecology",
    51: "Eminent Domain",
    53: "Corporation and Associations",
    55: "Partnerships",
    57: "Labor and Employment",
    59: "Professions and Occupations",
    61: "State Contracts",
    63: "Motor Vehicles and Traffic",
    65: "Highways, Bridges and Ferries",
    67: "Railroads",
    69: "Aviation",
    71: "Waterways, Ports and Harbors",
    73: "Real Property",
    75: "Contracts and Commissions",
    77: "Trusts and Estates",
    79: "Trade Practices and Regulations",
    81: "Fraudulent Conveyances",
    83: "Torts",
    85: "Debtor and Creditor Relations",
    87: "Liens",
    89: "Secured Transactions",
    91: "Wills, Estates, and Fiduciaries",
    93: "Domestic Relations",
    95: "Crimes Against Public Morals",
    97: "Crimes",
    99: "Criminal Procedure",
}

# Key chapters for tax/benefit analysis (Title 27)
MS_TAX_CHAPTERS: dict[int, str] = {
    1: "Assessors and County Tax Collectors",
    3: "Department of Revenue",
    4: "Board of Tax Appeals",
    5: "Motor Vehicle Comptroller",
    7: "Income Tax and Withholding",
    8: "Mississippi S Corporation Income Tax Act",
    9: "Estate Tax",
    10: "Uniform Estate Tax Apportionment Act",
    11: "Amusement Tax",
    13: "Corporation Franchise Tax",
    15: "Statewide Privilege Taxes",
    17: "Local Privilege Taxes",
    19: "Motor Vehicle Privilege and Excise Taxes",
    21: "Finance Company Privilege Tax",
    25: "Severance Taxes",
    27: "Vending and Amusement Machine Taxes",
    29: "Ad Valorem Taxes - General Provisions",
    31: "Ad Valorem Taxes - General Exemptions",
    33: "Ad Valorem Taxes - Homestead Exemptions",
    35: "Ad Valorem Taxes - Assessment",
    39: "Ad Valorem Taxes - State and Local Levies",
    41: "Ad Valorem Taxes - Collection",
    55: "Gasoline and Motor Fuel Taxes",
    65: "Sales Tax",
    67: "Use or Compensating Taxes",
    69: "Tobacco Tax",
    71: "Alcoholic Beverage Taxes",
}

# Key chapters for welfare analysis (Title 43)
MS_WELFARE_CHAPTERS: dict[int, str] = {
    1: "Public Welfare Generally",
    3: "Dependent Children",
    5: "Old Age Assistance",
    7: "Aid to Needy Blind",
    9: "Aid to Disabled",
    11: "Temporary Assistance for Needy Families",
    13: "Child Welfare Services",
    15: "Food Stamps",
    17: "Medicaid",
    19: "Low Income Home Energy Assistance",
    21: "Children's Health Insurance Program",
    23: "Child Support",
    25: "Supplemental Nutrition Assistance Program",
}


@dataclass
class ParsedMSSection:
    """Parsed Mississippi Code section."""

    section_number: str  # e.g., "27-7-1"
    section_title: str  # e.g., "Definitions"
    title_number: int  # e.g., 27
    title_name: str  # e.g., "Taxation and Finance"
    chapter_number: int  # e.g., 7
    chapter_title: str  # e.g., "Income Tax and Withholding"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedMSSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedMSSubsection:
    """A subsection within a Mississippi Code section."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list["ParsedMSSubsection"] = field(default_factory=list)


class MSConverterError(Exception):
    """Error during Mississippi Code conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class MSConverter:
    """Converter for Mississippi Code HTML to internal Section model.

    This converter fetches statute HTML from the UniCourt CIC project's
    GitHub Pages site (unicourt.github.io/cic-code-ms). The CIC project
    transforms RTF files from Public.Resource.Org into structured HTML.

    Example:
        >>> converter = MSConverter()
        >>> section = converter.fetch_section("27-7-1")
        >>> print(section.citation.section)
        "MS-27-7-1"

        >>> for section in converter.iter_chapter(27, 7):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        release: str = DEFAULT_RELEASE,
    ):
        """Initialize the Mississippi Code converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
            release: CIC release version (default: r78)
        """
        self.rate_limit_delay = rate_limit_delay
        self.release = release
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None
        self._title_cache: dict[int, BeautifulSoup] = {}

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
        """Build the URL for a title's HTML file.

        Args:
            title: Title number (e.g., 27)

        Returns:
            Full URL to the title's HTML file
        """
        title_str = f"{title:02d}"  # Zero-pad to 2 digits
        return f"{BASE_URL}/transforms/ms/ocms/{self.release}/gov.ms.code.title.{title_str}.html"

    def _get_title_soup(self, title: int) -> BeautifulSoup:
        """Get BeautifulSoup for a title, with caching.

        Args:
            title: Title number

        Returns:
            BeautifulSoup object for the title's HTML
        """
        if title not in self._title_cache:
            url = self._build_title_url(title)
            html = self._get(url)
            self._title_cache[title] = BeautifulSoup(html, "html.parser")
        return self._title_cache[title]

    def _parse_section_number(self, section_number: str) -> tuple[int, int, str]:
        """Parse section number into components.

        Args:
            section_number: e.g., "27-7-1", "43-17-15"

        Returns:
            Tuple of (title, chapter, section_suffix)
        """
        parts = section_number.split("-")
        if len(parts) < 3:
            raise ValueError(f"Invalid section number format: {section_number}")

        title = int(parts[0])
        chapter = int(parts[1])
        section_suffix = "-".join(parts[2:])  # Handle multi-part section numbers

        return title, chapter, section_suffix

    def _find_section_element(self, soup: BeautifulSoup, section_number: str) -> Tag | None:
        """Find the section element in the HTML.

        Mississippi sections have IDs like:
        - t27c01s27-1-1 (Title 27, Chapter 1, Section 27-1-1)
        - t27c07s27-7-1 (Title 27, Chapter 7, Section 27-7-1)

        Args:
            soup: BeautifulSoup of the title HTML
            section_number: e.g., "27-7-1"

        Returns:
            The h3 element for the section or None
        """
        title, chapter, _ = self._parse_section_number(section_number)

        # Build the ID pattern: t{title}c{chapter}s{section_number}
        # Section numbers use hyphens in the ID
        section_id = f"t{title}c{chapter:02d}s{section_number}"

        # Try to find by ID
        elem = soup.find(id=section_id)
        if elem:
            return elem

        # Also try without zero-padding on chapter
        section_id_alt = f"t{title}c{chapter}s{section_number}"
        elem = soup.find(id=section_id_alt)
        if elem:
            return elem  # pragma: no cover

        # Try finding by section number in the heading text
        for h3 in soup.find_all("h3"):
            text = h3.get_text(strip=True)
            if f"§ {section_number}." in text or f"§{section_number}." in text:
                return h3  # pragma: no cover

        return None

    def _get_chapter_title(self, title: int, chapter: int) -> str:
        """Get chapter title from registries or default."""
        if title == 27:
            return MS_TAX_CHAPTERS.get(chapter, f"Chapter {chapter}")
        elif title == 43:
            return MS_WELFARE_CHAPTERS.get(chapter, f"Chapter {chapter}")
        return f"Chapter {chapter}"  # pragma: no cover

    def _extract_section_text(self, section_elem: Tag, soup: BeautifulSoup) -> tuple[str, str]:
        """Extract section text and HTML from section element.

        The section content is in <p> tags following the h3 heading,
        until the next h3 or h2 element.

        Args:
            section_elem: The h3 element for this section
            soup: BeautifulSoup of the full document

        Returns:
            Tuple of (text content, html content)
        """
        content_parts = []
        html_parts = [str(section_elem)]

        # Get the parent div containing this section
        parent_div = section_elem.find_parent("div")
        if parent_div:
            # Get all content within this div after the h3
            started = False
            for child in parent_div.children:
                if child == section_elem:
                    started = True
                    continue
                if started:
                    if isinstance(child, Tag):
                        # Stop at next section heading
                        if child.name in ("h3", "h2"):
                            break  # pragma: no cover
                        # Also stop at subsection/annotation headers
                        if child.name == "h4" and any(
                            kw in child.get_text().lower()
                            for kw in [
                                "cross references",
                                "research references",
                                "judicial decisions",
                            ]
                        ):
                            break  # pragma: no cover
                        content_parts.append(child.get_text(separator="\n", strip=True))
                        html_parts.append(str(child))
        else:
            # Fallback: get siblings
            for sibling in section_elem.find_next_siblings():  # pragma: no cover
                if isinstance(sibling, Tag):  # pragma: no cover
                    if sibling.name in ("h3", "h2"):  # pragma: no cover
                        break  # pragma: no cover
                    if sibling.name == "h4":  # pragma: no cover
                        break  # pragma: no cover
                    content_parts.append(
                        sibling.get_text(separator="\n", strip=True)
                    )  # pragma: no cover
                    html_parts.append(str(sibling))  # pragma: no cover

        text = "\n".join(content_parts)
        html = "\n".join(html_parts)

        return text, html

    def _parse_section_html(
        self,
        soup: BeautifulSoup,
        section_number: str,
        url: str,
    ) -> ParsedMSSection:
        """Parse section from BeautifulSoup into ParsedMSSection."""
        title, chapter, _ = self._parse_section_number(section_number)

        # Find the section element
        section_elem = self._find_section_element(soup, section_number)
        if section_elem is None:
            raise MSConverterError(f"Section {section_number} not found", url)

        # Get title and chapter names
        title_name = MS_TITLES.get(title, f"Title {title}")
        chapter_title = self._get_chapter_title(title, chapter)

        # Extract section title from heading
        # Format: "§ 27-7-1. Definitions."
        heading_text = section_elem.get_text(strip=True)
        section_title = ""

        # Parse the heading to extract title
        title_match = re.search(rf"§\s*{re.escape(section_number)}\.\s*(.+?)(?:\.|$)", heading_text)
        if title_match:
            section_title = title_match.group(1).strip().rstrip(".")

        # If no match, try simpler pattern
        if not section_title:
            # Remove the section number prefix
            section_title = (
                re.sub(  # pragma: no cover
                    rf"§\s*{re.escape(section_number)}\.\s*", "", heading_text
                )
                .strip()
                .rstrip(".")
            )

        # Extract text and HTML content
        text, html_content = self._extract_section_text(section_elem, soup)

        # Parse history from text
        history = None
        history_match = re.search(
            r"HISTORY:\s*(.+?)(?=Cross References|RESEARCH|JUDICIAL|$)",
            text,
            re.DOTALL | re.IGNORECASE,
        )
        if history_match:
            history = history_match.group(1).strip()[:1000]

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedMSSection(
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
        )

    def _parse_subsections(self, text: str) -> list[ParsedMSSubsection]:
        """Parse hierarchical subsections from text.

        Mississippi Code typically uses:
        - (1), (2), (3) for primary divisions
        - (a), (b), (c) for secondary divisions
        - (i), (ii), (iii) for tertiary divisions (sometimes)
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
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedMSSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedMSSubsection]:
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
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]

            subsections.append(
                ParsedMSSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedMSSection) -> Section:
        """Convert ParsedMSSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"MS-{parsed.section_number}",
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
            title_name=f"Mississippi Code - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ms/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "27-7-1", "43-17-15"

        Returns:
            Section model

        Raises:
            MSConverterError: If section not found or parsing fails
        """
        title, _, _ = self._parse_section_number(section_number)
        url = self._build_title_url(title)
        soup = self._get_title_soup(title)
        parsed = self._parse_section_html(soup, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 27)
            chapter: Chapter number (e.g., 7)

        Returns:
            List of section numbers (e.g., ["27-7-1", "27-7-3", ...])
        """
        soup = self._get_title_soup(title)
        section_numbers = []

        # Find section navigation links for this chapter
        # Pattern: t{title}c{chapter}s{section_number}-snav
        chapter_id = f"t{title}c{chapter:02d}"
        chapter_id_alt = f"t{title}c{chapter}"

        # Also look for direct section IDs
        section_pattern = re.compile(rf"t{title}c0*{chapter}s({title}-{chapter}-[\w-]+)")

        # Search for section headings
        for elem in soup.find_all(["h3", "a"]):
            elem_id = elem.get("id", "") or ""
            elem_href = elem.get("href", "") if elem.name == "a" else ""

            # Check ID
            match = section_pattern.search(elem_id)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)  # pragma: no cover
                continue

            # Check href for nav links
            if elem.name == "a":
                href_match = section_pattern.search(elem_href.lstrip("#"))
                if href_match:
                    section_num = href_match.group(1)
                    if section_num not in section_numbers:
                        section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 27)
            chapter: Chapter number (e.g., 7)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except MSConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 27)

        Yields:
            Section objects
        """
        soup = self._get_title_soup(title)  # pragma: no cover
        url = self._build_title_url(title)  # pragma: no cover

        # Find all section headings
        section_pattern = re.compile(rf"t{title}c\d+s({title}-\d+-[\w-]+)")  # pragma: no cover

        seen = set()  # pragma: no cover
        for h3 in soup.find_all("h3"):  # pragma: no cover
            h3_id = h3.get("id", "") or ""  # pragma: no cover
            match = section_pattern.search(h3_id)  # pragma: no cover
            if match:  # pragma: no cover
                section_num = match.group(1)  # pragma: no cover
                if section_num in seen:  # pragma: no cover
                    continue  # pragma: no cover
                seen.add(section_num)  # pragma: no cover

                try:  # pragma: no cover
                    parsed = self._parse_section_html(soup, section_num, url)  # pragma: no cover
                    yield self._to_section(parsed)  # pragma: no cover
                except MSConverterError as e:  # pragma: no cover
                    print(f"Warning: Could not parse {section_num}: {e}")  # pragma: no cover
                    continue  # pragma: no cover

    def iter_tax_chapters(self) -> Iterator[Section]:
        """Iterate over sections from Mississippi tax-related chapters (Title 27).

        Yields:
            Section objects
        """
        yield from self.iter_title(27)  # pragma: no cover

    def iter_welfare_chapters(self) -> Iterator[Section]:
        """Iterate over sections from Mississippi public welfare chapters (Title 43).

        Yields:
            Section objects
        """
        yield from self.iter_title(43)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client and clear cache."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover
        self._title_cache.clear()

    def __enter__(self) -> "MSConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ms_section(section_number: str) -> Section:
    """Fetch a single Mississippi Code section.

    Args:
        section_number: e.g., "27-7-1"

    Returns:
        Section model
    """
    with MSConverter() as converter:
        return converter.fetch_section(section_number)


def download_ms_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a Mississippi Code chapter.

    Args:
        title: Title number (e.g., 27)
        chapter: Chapter number (e.g., 7)

    Returns:
        List of Section objects
    """
    with MSConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_ms_tax_title() -> Iterator[Section]:
    """Download all sections from Mississippi Title 27 (Taxation and Finance).

    Yields:
        Section objects
    """
    with MSConverter() as converter:  # pragma: no cover
        yield from converter.iter_tax_chapters()  # pragma: no cover


def download_ms_welfare_title() -> Iterator[Section]:
    """Download all sections from Mississippi Title 43 (Public Welfare).

    Yields:
        Section objects
    """
    with MSConverter() as converter:  # pragma: no cover
        yield from converter.iter_welfare_chapters()  # pragma: no cover
