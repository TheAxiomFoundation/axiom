"""Alaska state statute converter.

Converts Alaska Statutes from akleg.gov to the internal Section model for ingestion.

Alaska Statute Structure:
- Titles (e.g., Title 43: Revenue and Taxation)
- Chapters (e.g., Chapter 05: Administration of Revenue Laws)
- Sections (e.g., 43.05.010: Duties of the department of revenue)

URL Patterns:
- Main page: https://www.akleg.gov/basis/statutes.asp
- Title index: https://www.akleg.gov/basis/statutes.asp?title=43
- Document: https://www.akleg.gov/basis/get_documents.asp?session=XX&docid=YYYYY

Note: The akleg.gov site uses JavaScript navigation and returns PDFs for individual
sections. This converter uses the title-based index to discover sections and fetches
them via the statutes.asp API.

Example:
    >>> from atlas.converters.us_states.ak import AKConverter
    >>> converter = AKConverter()
    >>> section = converter.fetch_section("43.05.010")
    >>> print(section.section_title)
    "Duties of the department of revenue"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://www.akleg.gov/basis"

# Title mapping for reference
AK_TITLES: dict[int, str] = {
    1: "General Provisions",
    2: "Aeronautics",
    3: "Agriculture, Animals, and Food",
    4: "Alcoholic Beverages",
    5: "Amusements and Sports",
    6: "Banking and Financial Institutions",
    8: "Business and Professions",
    9: "Code of Civil Procedure",
    10: "Elections",
    11: "Criminal Law",
    12: "Criminal Procedure",
    13: "Decedents' Estates, Guardianships, Transfers, and Trusts",
    14: "Education, Libraries, and Museums",
    15: "Elections",
    16: "Fish and Game",
    17: "Food and Drugs",
    18: "Health, Safety, Housing, Human Rights, and Public Defender",
    19: "Labor and Workers' Compensation",
    21: "Liens",
    22: "Judiciary",
    23: "Labor and Workers' Compensation",
    24: "Legislative Affairs Agency",
    25: "Marital and Domestic Relations",
    26: "Military Affairs and Civil Emergencies",
    28: "Motor Vehicles",
    29: "Municipal Government",
    30: "Notaries Public and Commissioners",
    31: "Partnerships and Associations",
    32: "Penal Institutions",
    33: "Probation and Parole",
    34: "Property",
    35: "Public Buildings, Works, and Improvements",
    36: "Public Contracts",
    37: "Public Finance",
    38: "Public Land",
    39: "Public Officers and Employees",
    40: "Public Records and Recorders",
    41: "Public Resources",
    42: "Public Utilities and Carriers and Energy Programs",
    43: "Revenue and Taxation",
    44: "State Government",
    45: "Trade and Commerce",
    46: "Water, Air, Energy, and Environmental Conservation",
    47: "Welfare, Social Services, and Institutions",
}

# Key chapters for tax/benefit analysis - Title 43 (Revenue and Taxation)
AK_TAX_CHAPTERS: dict[str, str] = {
    "05": "Administration of Revenue Laws",
    "08": "Borrowing in Anticipation of Revenues",
    "10": "Enforcement and Collection of Taxes",
    "18": "State Aid to Local Governments",
    "19": "Multistate Tax Compact",
    "20": "Alaska Net Income Tax Act",
    "23": "Permanent Fund Dividends",
    "31": "Estate Tax Law of Alaska",
    "40": "Motor Fuel Tax",
    "55": "Oil and Gas Production Tax and Oil Surcharge",
    "56": "Oil and Gas Exploration, Production, and Pipeline Transportation Property Taxes",
    "60": "Excise Tax on Alcoholic Beverages",
    "61": "Excise Tax on Marijuana",
    "65": "Mining License Tax",
    "70": "Alaska Business License Act",
    "75": "Fisheries Business License and Taxes",
    "77": "Salmon Enhancement Tax",
    "80": "Dive Fishery Management Assessment",
    "82": "Regional Seafood Development Tax",
    "90": "Alaska Gasline Inducement Act",
    "98": "Miscellaneous Provisions",
}

# Key chapters for welfare analysis - Title 47 (Welfare, Social Services, and Institutions)
AK_WELFARE_CHAPTERS: dict[str, str] = {
    "05": "Administration of Welfare, Social Services, and Institutions",
    "06": "Child Welfare Services",
    "07": "Medical Assistance for Needy Persons",
    "08": "Assistance for Catastrophic Illness and Chronic or Acute Medical Conditions",
    "10": "General Relief Assistance",
    "12": "Alaska Temporary Assistance Program",
    "14": "Child Care Assistance Program",
    "17": "Public Assistance Programs",
    "20": "Child Support Enforcement",
    "25": "Child Support Services Agency",
    "27": "Paternity",
    "30": "Mental Health",
    "32": "Commitment of Intoxicated Persons",
    "33": "Persons with Developmental Disabilities",
    "37": "Uniform Alcoholism and Intoxication Treatment Act",
    "40": "Institutions",
    "45": "Older Alaskans and Long-Term Care Facility Residents",
    "55": "Alaska Pioneers' Home and Alaska Veterans' Home",
    "60": "Multipurpose Senior Centers",
    "62": "Office of the Long Term Care Ombudsman",
    "65": "Service Programs for Older Alaskans and Other Adults",
    "70": "Interstate Compact on the Placement of Children",
    "75": "Social Services Planning",
    "80": "Persons with Disabilities",
    "90": "Displaced Homemakers",
}


@dataclass
class ParsedAKSection:
    """Parsed Alaska statute section."""

    section_number: str  # e.g., "43.05.010"
    section_title: str  # e.g., "Duties of the department of revenue"
    chapter_number: str  # e.g., "05"
    chapter_title: str  # e.g., "Administration of Revenue Laws"
    title_number: int  # e.g., 43
    title_name: str  # e.g., "Revenue and Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedAKSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedAKSubsection:
    """A subsection within an Alaska statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list["ParsedAKSubsection"] = field(default_factory=list)


class AKConverterError(Exception):
    """Error during Alaska statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class AKConverter:
    """Converter for Alaska Statutes to internal Section model.

    Example:
        >>> converter = AKConverter()
        >>> section = converter.fetch_section("43.05.010")
        >>> print(section.citation.section)
        "AK-43.05.010"

        >>> for section in converter.iter_chapter(43, "05"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Alaska statute converter.

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

    def _parse_section_number(self, section_number: str) -> tuple[int, str, str]:
        """Parse section number into components.

        Args:
            section_number: e.g., "43.05.010" or "47.30.660"

        Returns:
            Tuple of (title_number, chapter_number, section_suffix)
            e.g., (43, "05", "010")
        """
        parts = section_number.split(".")
        if len(parts) < 3:
            raise AKConverterError(f"Invalid section number format: {section_number}")
        title = int(parts[0])
        chapter = parts[1]
        section_suffix = ".".join(parts[2:])  # Handle cases like "43.05.010"
        return title, chapter, section_suffix

    def _build_title_url(self, title: int) -> str:
        """Build the URL for a title's chapter list.

        Args:
            title: Title number (e.g., 43)

        Returns:
            URL to the title index page
        """
        return f"{BASE_URL}/statutes.asp?title={title}"

    def _build_chapter_url(self, title: int, chapter: str) -> str:
        """Build the URL for a chapter's section list.

        The akleg.gov site uses fragment identifiers for chapters.

        Args:
            title: Title number (e.g., 43)
            chapter: Chapter number (e.g., "05")

        Returns:
            URL to the chapter page
        """
        return f"{BASE_URL}/statutes.asp?title={title}#chapter{chapter}"  # pragma: no cover

    def _get_title_for_section(self, section_number: str) -> tuple[int, str]:
        """Get title number and name from section number.

        Args:
            section_number: e.g., "43.05.010"

        Returns:
            Tuple of (title_number, title_name)
        """
        title_num, _, _ = self._parse_section_number(section_number)
        title_name = AK_TITLES.get(title_num, f"Title {title_num}")
        return title_num, title_name

    def _get_chapter_info(self, title: int, chapter: str) -> tuple[str, str]:
        """Get chapter number and name.

        Args:
            title: Title number (e.g., 43)
            chapter: Chapter number (e.g., "05")

        Returns:
            Tuple of (chapter_number, chapter_title)
        """
        if title == 43:
            chapter_title = AK_TAX_CHAPTERS.get(chapter, f"Chapter {chapter}")
        elif title == 47:
            chapter_title = AK_WELFARE_CHAPTERS.get(chapter, f"Chapter {chapter}")
        else:
            chapter_title = f"Chapter {chapter}"  # pragma: no cover
        return chapter, chapter_title

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedAKSection:
        """Parse section HTML into ParsedAKSection.

        Args:
            html: Raw HTML content
            section_number: e.g., "43.05.010"
            url: Source URL

        Returns:
            ParsedAKSection object
        """
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise AKConverterError(f"Section {section_number} not found", url)

        title_num, chapter, section_suffix = self._parse_section_number(section_number)
        title_name = AK_TITLES.get(title_num, f"Title {title_num}")
        chapter_num, chapter_title = self._get_chapter_info(title_num, chapter)

        # Extract section title - Alaska uses pattern: "Sec. 43.05.010. Title."
        section_title = ""

        # Try various patterns for finding the section title
        title_patterns = [
            # Pattern: "Sec. 43.05.010. Title here."
            re.compile(rf"Sec\.\s*{re.escape(section_number)}\.\s*([^.]+)"),
            # Pattern: "43.05.010 Title here"
            re.compile(rf"{re.escape(section_number)}\s+([^.]+)"),
            # Pattern with section symbol
            re.compile(rf"AS\s*{re.escape(section_number)}\s*[.:]\s*([^.]+)"),
        ]

        for pattern in title_patterns:
            for text_node in soup.stripped_strings:
                match = pattern.search(text_node)
                if match:
                    section_title = match.group(1).strip().rstrip(".")
                    break
            if section_title:
                break

        # Try headings if not found in text
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
                heading_text = heading.get_text(strip=True)
                for pattern in title_patterns:
                    match = pattern.search(heading_text)
                    if match:
                        section_title = match.group(1).strip().rstrip(".")  # pragma: no cover
                        break  # pragma: no cover
                if section_title:
                    break  # pragma: no cover

        # Get body content
        content_elem = (
            soup.find("div", class_="statute")
            or soup.find("div", class_="section")
            or soup.find("div", id="content")
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

        # Extract history note - Alaska uses "History:" or "(previous text...)"
        history = None
        history_patterns = [
            r"History[.:]\s*(.+?)(?:\n|$)",
            r"\(([Ss]ec\.\s+\d+.*?ch\.\s+\d+.*?SLA.*?)\)",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.DOTALL)
            if history_match:
                history = history_match.group(1).strip()[:1000]
                break

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedAKSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter_num,
            chapter_title=chapter_title,
            title_number=title_num,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedAKSubsection]:
        """Parse hierarchical subsections from text.

        Alaska statutes typically use:
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
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedAKSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedAKSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse third-level children (A), (B), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([A-Z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit to reasonable size and stop at next lowercase letter subsection
            next_alpha = re.search(r"\([a-z]\)", direct_text)
            if next_alpha:  # pragma: no cover
                direct_text = direct_text[: next_alpha.start()]

            subsections.append(
                ParsedAKSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedAKSubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at next higher-level subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover
            next_alpha_lower = re.search(r"\([a-z]\)", content)
            if next_alpha_lower:
                content = content[: next_alpha_lower.start()]  # pragma: no cover

            if len(content) > 2000:
                content = content[:2000] + "..."  # pragma: no cover

            subsections.append(
                ParsedAKSubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedAKSection) -> Section:
        """Convert ParsedAKSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"AK-{parsed.section_number}",
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
            title_name=f"Alaska Statutes - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"ak/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section's print page.

        Args:
            section_number: e.g., "43.05.010"

        Returns:
            URL to the section print page
        """
        return (
            f"{BASE_URL}/statutes.asp?media=print&secStart={section_number}&secEnd={section_number}"
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Uses the akleg.gov print API to get section content.

        Args:
            section_number: e.g., "43.05.010", "47.30.660"

        Returns:
            Section model

        Raises:
            AKConverterError: If section not found or parsing fails
        """
        # Build URL using the print API endpoint
        url = self._build_section_url(section_number)

        try:
            html = self._get(url)
        except httpx.HTTPError as e:  # pragma: no cover
            raise AKConverterError(
                f"Failed to fetch section {section_number}: {e}", url
            )  # pragma: no cover

        # Parse the HTML for the specific section
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Uses the akleg.gov AJAX API to get section list.

        Args:
            title: Title number (e.g., 43)
            chapter: Chapter number (e.g., "05")

        Returns:
            List of section numbers (e.g., ["43.05.010", "43.05.020", ...])
        """
        # Use the AJAX API endpoint to get chapter sections
        chapter_id = f"{title}.{chapter}"
        url = f"{BASE_URL}/statutes.asp?media=js&type=TOC&title={chapter_id}"
        html = self._get(url)

        section_numbers = []

        # Parse: #43.23.005 >Sec. 43.23.005.   Eligibility.
        pattern = re.compile(r"#(\d+\.\d+\.\d+[A-Za-z]?)\s*>Sec\.\s+[\d.]+[A-Za-z]?\.\s+([^<]+)<")

        for match in pattern.finditer(html):
            section_num = match.group(1)  # e.g., "43.23.005"
            section_title = match.group(2).strip().rstrip(".")

            # Skip repealed/renumbered sections
            if "[Repealed" in section_title or "[Renumbered" in section_title:  # pragma: no cover
                continue

            if section_num not in section_numbers:
                section_numbers.append(section_num)

        return sorted(section_numbers)

    def iter_chapter(self, title: int, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 43)
            chapter: Chapter number (e.g., "05")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except AKConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 43 or 47)

        Yields:
            Section objects for each section
        """
        # Get chapters for this title
        if title == 43:  # pragma: no cover
            chapters = list(AK_TAX_CHAPTERS.keys())  # pragma: no cover
        elif title == 47:  # pragma: no cover
            chapters = list(AK_WELFARE_CHAPTERS.keys())  # pragma: no cover
        else:
            raise AKConverterError(f"No chapter list defined for title {title}")  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def iter_tax_chapters(self) -> Iterator[Section]:
        """Iterate over all sections in Title 43 (Revenue and Taxation).

        Yields:
            Section objects
        """
        yield from self.iter_title(43)  # pragma: no cover

    def iter_welfare_chapters(self) -> Iterator[Section]:
        """Iterate over all sections in Title 47 (Welfare).

        Yields:
            Section objects
        """
        yield from self.iter_title(47)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "AKConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ak_section(section_number: str) -> Section:
    """Fetch a single Alaska statute section.

    Args:
        section_number: e.g., "43.05.010"

    Returns:
        Section model
    """
    with AKConverter() as converter:
        return converter.fetch_section(section_number)


def download_ak_chapter(title: int, chapter: str) -> list[Section]:
    """Download all sections from an Alaska Statutes chapter.

    Args:
        title: Title number (e.g., 43)
        chapter: Chapter number (e.g., "05")

    Returns:
        List of Section objects
    """
    with AKConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_ak_tax_chapters() -> Iterator[Section]:
    """Download all sections from Alaska Title 43 (Revenue and Taxation).

    Yields:
        Section objects
    """
    with AKConverter() as converter:  # pragma: no cover
        yield from converter.iter_tax_chapters()  # pragma: no cover


def download_ak_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Alaska Title 47 (Welfare).

    Yields:
        Section objects
    """
    with AKConverter() as converter:  # pragma: no cover
        yield from converter.iter_welfare_chapters()  # pragma: no cover
