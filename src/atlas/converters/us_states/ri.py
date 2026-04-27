"""Rhode Island state statute converter.

Converts Rhode Island General Laws HTML from rilegislature.gov
to the internal Section model for ingestion.

Rhode Island Statute Structure:
- Titles (e.g., Title 44: Taxation)
- Chapters (e.g., Chapter 44-30: Personal Income Tax)
- Parts (some chapters have parts, e.g., Part I: General)
- Sections (e.g., 44-30-1: Persons subject to tax)

URL Patterns:
- Title index: TITLE[NUM]/INDEX.HTM
- Chapter index: TITLE[NUM]/[NUM-NUM]/INDEX.htm
- Section: TITLE[NUM]/[NUM-NUM]/[NUM-NUM-NUM].htm

Example:
    >>> from atlas.converters.us_states.ri import RIConverter
    >>> converter = RIConverter()
    >>> section = converter.fetch_section("44-30-1")
    >>> print(section.section_title)
    "Persons subject to tax"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://webserver.rilegislature.gov/Statutes"

# Title mapping for reference
RI_TITLES: dict[str, str] = {
    1: "Aeronautics",
    2: "Agriculture and Forestry",
    3: "Alcoholic Beverages",
    4: "Animals and Animal Husbandry",
    5: "Businesses and Professions",
    6: "Commercial Law — General Regulatory Provisions",
    7: "Corporations, Associations, and Partnerships",
    8: "Courts and Civil Procedure — Courts",
    9: "Courts and Civil Procedure — Procedure Generally",
    10: "Courts and Civil Procedure — Actions and Proceedings",
    11: "Criminal Offenses",
    12: "Criminal Procedure",
    13: "Delinquent and Dependent Children",
    14: "Domestic Relations",
    15: "Education",
    16: "Elections",
    17: "Estates, Trusts, and Fiduciaries of Incapacitated Persons",
    18: "Fish and Wildlife",
    19: "Financial Institutions",
    20: "Food and Drugs",
    21: "Highways",
    22: "Hospitals and Sanitaria",
    23: "Health and Safety",
    24: "Insurers and Insurance",
    25: "State Lands, Buildings, and Property",
    26: "Libraries",
    27: "Lobbying",
    28: "Labor and Labor Relations",
    29: "Liens",
    30: "Military Affairs and Defense",
    31: "Motor and Other Vehicles",
    32: "Parks, Recreation, and Conservation",
    33: "Probate Practice and Procedure",
    34: "Property",
    35: "Public Debt and Finance",
    36: "Public Finance",
    37: "Public Officers and Employees",
    38: "Public Utilities and Carriers",
    39: "Regulatory and Administrative Provisions Relating to State Departments",
    40: "Human Services",
    41: "State Affairs and Government",
    42: "State Affairs and Government",
    43: "Sports and Entertainment",
    44: "Taxation",
    45: "Towns and Cities",
    46: "Waters and Navigation",
    47: "Weights and Measures",
}

# Key chapters for tax/benefit analysis
RI_TAX_CHAPTERS: dict[str, str] = {
    "44-1": "State Tax Officials",
    "44-2": "Limitations on Rate of Tax Levy",
    "44-3": "Levy and Assessment of Local Taxes",
    "44-4": "Taxation of Tangible Personal Property of Manufacturing, Commercial, and Financial Businesses",
    "44-5": "Levy and Assessment of Local Taxes",
    "44-7": "Taxation of Corporate Excess",
    "44-11": "Business Corporation Tax",
    "44-13": "Estate and Transfer Taxes",
    "44-14": "Personal and Corporate Income Tax Administration",
    "44-17": "Levy and Collection of Taxes",
    "44-18": "Sales and Use Tax — Imposition and Collection",
    "44-18.1": "Sales and Use Taxes — Enforcement and Collection",
    "44-19": "Sales and Use Taxes — Returns and Administration",
    "44-20": "Cigarette Tax",
    "44-22": "Meals and Beverage Tax",
    "44-23": "Tax Sales",
    "44-25": "Hotel Tax",
    "44-26": "Inheritance Tax",
    "44-27": "Legacy and Succession Tax",
    "44-30": "Personal Income Tax",
    "44-30.1": "Personal Income Tax — Adjustments",
    "44-31": "Taxation of Banks",
    "44-32": "Pass-Through Entity Tax",
    "44-33": "Taxation of Insurance Companies",
    "44-34": "Income Tax Credit",
    "44-35": "Rhode Island Economic Development Corporation",
    "44-42": "Property Revaluation Act",
    "44-43": "Lottery",
    "44-44": "Property Tax Relief",
    "44-45": "Tangible Personal Property Relief",
    "44-46": "Elderly Property Tax Relief",
    "44-48": "Tax Increment Financing",
    "44-49": "Rhode Island Investment Tax Credit",
    "44-52": "Tax Credits for Employers",
    "44-53": "Residential Lead Abatement Income Tax Credit",
    "44-55": "Rhode Island Urban Enterprise Zones",
    "44-57": "Historic Preservation Tax Credits",
    "44-61": "Tax Amnesty",
    "44-62": "Motion Picture Production Tax Credits",
    "44-63": "Historic Homeownership Assistance Act",
    "44-64": "Rhode Island Qualified Jobs Incentive Act",
    "44-65": "Rebuild Rhode Island Tax Credit Act",
    "44-66": "Small Business Development Fund Tax Credit",
    "44-67": "Stay Invested in RI Wavemaker Fellowship",
    "44-68": "First Wave Closing Fund Tax Credit",
    "44-69": "Rhode Island Tax Treaty Act",
    "44-70": "Municipal Property Tax Levy Stabilization",
    "44-72": "Non-Owner Occupied Property Tax Act",
}

RI_WELFARE_CHAPTERS: dict[str, str] = {
    "40-1": "Department of Human Services",
    "40-2": "State Institutions",
    "40-3": "Local Directors of Human Services",
    "40-4": "General Provisions",
    "40-5": "Medical Assistance",
    "40-5.1": "Medical Assistance — Services",
    "40-5.2": "Medical Assistance — Long-Term Care",
    "40-6": "General Public Assistance",
    "40-6.1": "Public Assistance — Support for Needy",
    "40-6.2": "Work Training and Work Incentive Programs",
    "40-6.3": "Rhode Island Works Program",
    "40-7": "Temporary Disability Insurance",
    "40-8": "Child Welfare",
    "40-8.1": "Child Day Care",
    "40-8.2": "Child Abuse and Neglect",
    "40-8.3": "Children's Trust",
    "40-8.4": "Healthy Families America Program",
    "40-8.5": "Pregnancy Prevention",
    "40-8.6": "Child Support Enforcement",
    "40-9": "Assistance to Blind, Aged, and Disabled",
    "40-9.1": "Assistance Programs for Elderly",
    "40-10": "Food Stamps Program",
    "40-11": "Abuse of Children",
    "40-12": "Assistance to Mothers",
    "40-12.1": "Women, Infants and Children (WIC) Program",
    "40-13": "Adult Services",
    "40-14": "Mental Health Facilities",
    "40-15": "Community Health Centers",
    "40-16": "Access to Health Care",
    "40-17": "Homeless Assistance",
    "40-18": "Low Income Home Energy Assistance",
    "40-20": "Catastrophic Health Insurance",
    "40-21": "RIte Care",
    "40-22": "Pharmacy Assistance to the Elderly",
}


@dataclass
class ParsedRISection:
    """Parsed Rhode Island statute section."""

    section_number: str  # e.g., "44-30-1"
    section_title: str  # e.g., "Persons subject to tax"
    title_number: int  # e.g., 44
    title_name: str  # e.g., "Taxation"
    chapter_number: str  # e.g., "44-30"
    chapter_title: str | None  # e.g., "Personal Income Tax"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedRISubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""


@dataclass
class ParsedRISubsection:
    """A subsection within a Rhode Island statute."""

    identifier: str  # e.g., "a", "1", "i"
    text: str
    children: list["ParsedRISubsection"] = field(default_factory=list)


class RIConverterError(Exception):
    """Error during Rhode Island statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class RIConverter:
    """Converter for Rhode Island General Laws HTML to internal Section model.

    Example:
        >>> converter = RIConverter()
        >>> section = converter.fetch_section("44-30-1")
        >>> print(section.citation.section)
        "RI-44-30-1"

        >>> for section in converter.iter_chapter("44-30"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the Rhode Island statute converter.

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

    def _extract_title_from_section(self, section_number: str) -> int:
        """Extract title number from section number.

        Args:
            section_number: e.g., "44-30-1" -> 44

        Returns:
            Title number as integer
        """
        parts = section_number.split("-")
        return int(parts[0])

    def _extract_chapter_from_section(self, section_number: str) -> str:
        """Extract chapter number from section number.

        Args:
            section_number: e.g., "44-30-1" -> "44-30"

        Returns:
            Chapter number (e.g., "44-30")
        """
        parts = section_number.split("-")
        if len(parts) >= 2:
            return f"{parts[0]}-{parts[1]}"
        return parts[0]  # pragma: no cover

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "44-30-1", "40-1-1"

        Returns:
            Full URL to the section page
        """
        title = self._extract_title_from_section(section_number)
        chapter = self._extract_chapter_from_section(section_number)

        return f"{BASE_URL}/TITLE{title}/{chapter}/{section_number}.htm"

    def _build_chapter_index_url(self, chapter: str) -> str:
        """Build the URL for a chapter's index page.

        Args:
            chapter: e.g., "44-30", "40-1"

        Returns:
            Full URL to the chapter index page
        """
        title = int(chapter.split("-")[0])
        return f"{BASE_URL}/TITLE{title}/{chapter}/INDEX.htm"

    def _build_title_index_url(self, title: int) -> str:
        """Build the URL for a title's index page.

        Args:
            title: Title number (e.g., 44)

        Returns:
            Full URL to the title index page
        """
        return f"{BASE_URL}/TITLE{title}/INDEX.HTM"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedRISection:
        """Parse section HTML into ParsedRISection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if (
            "404" in html.lower()
            or "not found" in html.lower()
            or "cannot be found" in html.lower()
        ):
            raise RIConverterError(f"Section {section_number} not found", url)

        title = self._extract_title_from_section(section_number)
        chapter = self._extract_chapter_from_section(section_number)

        title_name = RI_TITLES.get(title, f"Title {title}")
        chapter_title = RI_TAX_CHAPTERS.get(chapter) or RI_WELFARE_CHAPTERS.get(chapter)

        # Extract section title from heading
        # Rhode Island uses format: "§ 44-30-1. Persons subject to tax."
        section_title = ""

        # Try to find the section title in the text
        title_pattern = re.compile(
            rf"§?\s*{re.escape(section_number)}\.?\s*[-—.]?\s*([^.]+?)(?:\.|[-—]|$)",
            re.IGNORECASE,
        )

        for text_node in soup.stripped_strings:
            match = title_pattern.search(text_node)
            if match:
                section_title = match.group(1).strip()
                break

        # Try headings if not found
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
                heading_text = heading.get_text(strip=True)
                match = title_pattern.search(heading_text)
                if match:
                    section_title = match.group(1).strip()  # pragma: no cover
                    break  # pragma: no cover

        # Fallback: look for bold text with section number
        if not section_title:
            for bold in soup.find_all(["b", "strong"]):
                bold_text = bold.get_text(strip=True)  # pragma: no cover
                if section_number in bold_text:  # pragma: no cover
                    # Extract everything after the section number
                    after_num = bold_text.split(section_number, 1)[-1]  # pragma: no cover
                    # Clean up and extract title
                    after_num = re.sub(r"^[\s.—-]+", "", after_num)  # pragma: no cover
                    if after_num:  # pragma: no cover
                        section_title = after_num.split(".")[0].strip()  # pragma: no cover
                        break  # pragma: no cover

        # Get full text content
        body = soup.find("body")
        if body:
            # Remove navigation, scripts, styles
            for elem in body.find_all(["nav", "script", "style", "header", "footer"]):
                elem.decompose()  # pragma: no cover
            text = body.get_text(separator="\n", strip=True)
            html_content = str(body)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note
        history = None
        history_match = re.search(
            r"History\s*(?:of\s*Section)?\.?\s*[-—:]?\s*(.+?)(?:\n\n|\Z)",
            text,
            re.DOTALL | re.IGNORECASE,
        )
        if history_match:
            history = history_match.group(1).strip()[:2000]  # Limit length

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedRISection(
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

    def _parse_subsections(self, text: str) -> list[ParsedRISubsection]:
        """Parse hierarchical subsections from text.

        Rhode Island statutes typically use:
        - (a), (b), (c) for primary divisions
        - (1), (2), (3) for secondary divisions
        - (i), (ii), (iii) for tertiary (Roman numerals)
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
                ParsedRISubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        # If no (a)/(b) subsections found, try (1)/(2) at top level
        if not subsections:
            parts = re.split(r"(?=\(\d+\)\s)", text)
            for part in parts[1:]:
                match = re.match(r"\((\d+)\)\s*", part)  # pragma: no cover
                if not match:  # pragma: no cover
                    continue  # pragma: no cover

                identifier = match.group(1)  # pragma: no cover
                content = part[match.end() :]  # pragma: no cover

                # Clean up - stop at next top-level
                next_num = re.search(r"\(\d+\)", content)  # pragma: no cover
                if next_num:  # pragma: no cover
                    content = content[: next_num.start()]  # pragma: no cover

                subsections.append(  # pragma: no cover
                    ParsedRISubsection(
                        identifier=identifier,
                        text=content.strip()[:2000],
                        children=[],
                    )
                )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedRISubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size and stop at next letter subsection
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]  # pragma: no cover

            subsections.append(
                ParsedRISubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedRISection) -> Section:
        """Convert ParsedRISection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"RI-{parsed.section_number}",
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
            title_name=f"Rhode Island General Laws - Title {parsed.title_number}: {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ri/{parsed.title_number}/{parsed.section_number}",
        )

    def _build_section_urls(self, section_number: str) -> list[str]:
        """Build possible URLs for a section.

        Some chapters have Parts (e.g., 44-30 has Parts I-VI),
        so we need to try multiple URL patterns.

        Args:
            section_number: e.g., "44-30-1", "40-1-1"

        Returns:
            List of possible URLs to try
        """
        title = self._extract_title_from_section(section_number)
        chapter = self._extract_chapter_from_section(section_number)
        title_num = chapter.split("-")[0]

        urls = [
            # Standard pattern: TITLE44/44-30/44-30-1.htm
            f"{BASE_URL}/TITLE{title}/{chapter}/{section_number}.htm",
        ]

        # Add Part patterns (I through VI) for chapters that use them
        for part in ["I", "II", "III", "IV", "V", "VI"]:
            urls.append(
                f"{BASE_URL}/TITLE{title}/{chapter}/{title_num}-{part}/{section_number}.htm"
            )

        return urls

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "44-30-1", "40-1-1"

        Returns:
            Section model

        Raises:
            RIConverterError: If section not found or parsing fails
        """
        urls = self._build_section_urls(section_number)
        last_error = None

        for url in urls:
            try:
                html = self._get(url)
                parsed = self._parse_section_html(html, section_number, url)
                return self._to_section(parsed)
            except httpx.HTTPStatusError as e:  # pragma: no cover
                last_error = e
                continue
            except RIConverterError:
                continue

        raise RIConverterError(
            f"Section {section_number} not found after trying {len(urls)} URLs",
            urls[0],
        ) from last_error

    def get_chapter_section_numbers(self, chapter: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., "44-30", "40-1")

        Returns:
            List of section numbers (e.g., ["44-30-1", "44-30-2", ...])
        """
        url = self._build_chapter_index_url(chapter)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError:  # pragma: no cover
            # Some chapters have parts - try to get those
            return self._get_chapter_sections_with_parts(chapter)  # pragma: no cover

        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links: like "44-30-1.htm"
        pattern = re.compile(rf"({re.escape(chapter)}-[\d.]+)\.htm", re.IGNORECASE)

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def _get_chapter_sections_with_parts(self, chapter: str) -> list[str]:
        """Get sections for chapters that have parts (like 44-30).

        Some RI chapters are subdivided into parts (Part I, Part II, etc.)
        with their own index pages.
        """
        title = int(chapter.split("-")[0])  # pragma: no cover
        base_url = f"{BASE_URL}/TITLE{title}/{chapter}"  # pragma: no cover

        section_numbers = []  # pragma: no cover

        # Try parts I through VI (most chapters don't have more)
        for part in ["I", "II", "III", "IV", "V", "VI"]:  # pragma: no cover
            part_url = f"{base_url}/{chapter.split('-')[0]}-{part}/INDEX.htm"  # pragma: no cover
            try:  # pragma: no cover
                html = self._get(part_url)  # pragma: no cover
                soup = BeautifulSoup(html, "html.parser")  # pragma: no cover

                # Find section links
                pattern = re.compile(
                    rf"({re.escape(chapter)}-[\d.]+)\.htm", re.IGNORECASE
                )  # pragma: no cover
                for link in soup.find_all("a", href=True):  # pragma: no cover
                    href = link.get("href", "")  # pragma: no cover
                    match = pattern.search(href)  # pragma: no cover
                    if match:  # pragma: no cover
                        section_num = match.group(1)  # pragma: no cover
                        if section_num not in section_numbers:  # pragma: no cover
                            section_numbers.append(section_num)  # pragma: no cover
            except httpx.HTTPStatusError, Exception:  # pragma: no cover
                # Part doesn't exist, continue
                continue  # pragma: no cover

        return section_numbers  # pragma: no cover

    def get_title_chapters(self, title: int) -> list[str]:  # pragma: no cover
        """Get list of chapter numbers in a title.

        Args:
            title: Title number (e.g., 44)

        Returns:
            List of chapter numbers (e.g., ["44-1", "44-2", ...])
        """
        url = self._build_title_index_url(title)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        chapters = []

        # Find chapter links: like "44-30/INDEX.htm"
        pattern = re.compile(rf"({title}-[\d.]+)/INDEX\.htm", re.IGNORECASE)

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                chapter_num = match.group(1)
                if chapter_num not in chapters:
                    chapters.append(chapter_num)

        return chapters

    def iter_chapter(self, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., "44-30", "40-1")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except RIConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(RI_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 44)

        Yields:
            Section objects
        """
        chapters = self.get_title_chapters(title)  # pragma: no cover
        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "RIConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ri_section(section_number: str) -> Section:
    """Fetch a single Rhode Island statute section.

    Args:
        section_number: e.g., "44-30-1"

    Returns:
        Section model
    """
    with RIConverter() as converter:
        return converter.fetch_section(section_number)


def download_ri_chapter(chapter: str) -> list[Section]:
    """Download all sections from a Rhode Island General Laws chapter.

    Args:
        chapter: Chapter number (e.g., "44-30")

    Returns:
        List of Section objects
    """
    with RIConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_ri_tax_chapters() -> Iterator[Section]:
    """Download all sections from Rhode Island tax-related chapters (Title 44).

    Yields:
        Section objects
    """
    with RIConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(RI_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_ri_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Rhode Island human services chapters (Title 40).

    Yields:
        Section objects
    """
    with RIConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(RI_WELFARE_CHAPTERS.keys()))  # pragma: no cover
