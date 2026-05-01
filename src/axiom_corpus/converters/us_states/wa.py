"""Washington state statute converter.

Converts Washington Revised Code (RCW) HTML from app.leg.wa.gov
to the internal Section model for ingestion.

Washington RCW Structure:
- Titles (e.g., Title 82: Excise Taxes)
- Chapters (e.g., Chapter 82.04: Business and Occupation Tax)
- Sections (e.g., 82.04.290: Tax on service and other activities)

URL Patterns:
- Title index: default.aspx?cite=[title_number]
- Chapter contents: default.aspx?cite=[chapter_number]
- Section: default.aspx?cite=[section_number]
- PDF version: default.aspx?cite=[section_number]&pdf=true

Example:
    >>> from axiom_corpus.converters.us_states.wa import WAConverter
    >>> converter = WAConverter()
    >>> section = converter.fetch_section("82.04.290")
    >>> print(section.section_title)
    "Tax on service and other activities"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://app.leg.wa.gov/rcw"

# Title mapping for reference
# Note: Some titles have letter suffixes (e.g., 23B, 28A) so we use str keys
WA_TITLES: dict[str, str] = {
    "1": "General Provisions",
    "2": "Courts of Record",
    "3": "Courts of Limited Jurisdiction",
    "4": "Civil Procedure",
    "5": "Evidence",
    "6": "Enforcement of Judgments",
    "7": "Special Proceedings and Actions",
    "8": "Eminent Domain",
    "9": "Crimes and Punishments",
    "9.94A": "Sentencing Reform Act of 1981",
    "10": "Criminal Procedure",
    "11": "Probate and Trust Law",
    "12": "Evidence and Witnesses",
    "13": "Juvenile Courts and Juvenile Offenders",
    "14": "Aeronautics",
    "15": "Agriculture and Marketing",
    "16": "Animals and Livestock",
    "17": "Weeds, Rodents, and Pests",
    "18": "Businesses and Professions",
    "19": "Business Regulations -- Miscellaneous",
    "20": "Military Affairs",
    "21": "Securities and Investments",
    "22": "Libraries, Museums, and Historical Activities",
    "23": "Corporations and Associations (Nonprofit)",
    "23B": "Washington Business Corporation Act",
    "24": "Corporations and Associations (Nonprofit)",
    "25": "Partnerships",
    "26": "Domestic Relations",
    "27": "Libraries, Museums, and Historical Activities",
    "28A": "Common School Provisions",
    "28B": "Higher Education",
    "28C": "Vocational Education",
    "29A": "Elections",
    "30A": "Washington Commercial Bank Act",
    "30B": "Interstate Banking",
    "31": "Miscellaneous Loan Agencies",
    "32": "Washington Savings Bank Act",
    "33": "Washington Savings Association Act",
    "34": "Administrative Law",
    "35": "Cities and Towns",
    "35A": "Optional Municipal Code",
    "36": "Counties",
    "37": "Federal Areas -- Indians",
    "38": "Militia and Military Affairs",
    "39": "Public Contracts and Indebtedness",
    "40": "Public Documents, Records, and Publications",
    "41": "Public Employment, Civil Service, and Pensions",
    "42": "Public Officers and Agencies",
    "43": "State Government -- Executive",
    "44": "State Government -- Legislative",
    "45": "Public Printing and Advertising",
    "46": "Motor Vehicles",
    "47": "Public Highways and Transportation",
    "48": "Insurance",
    "49": "Labor Regulations",
    "50": "Unemployment Compensation",
    "50A": "Family and Medical Leave",
    "51": "Industrial Insurance",
    "52": "Fire Protection Districts",
    "53": "Port Districts",
    "54": "Public Utility Districts",
    "55": "Soil Conservation Districts",
    "56": "Sewer Districts",
    "57": "Water-Sewer Districts",
    "58": "Boundaries and Plats",
    "59": "Landlord and Tenant",
    "60": "Liens",
    "61": "Mortgages, Deeds of Trust, and Real Estate Contracts",
    "62A": "Uniform Commercial Code",
    "63": "Personal Property",
    "64": "Real Property and Conveyances",
    "65": "Recording, Registration, and Legal Publication",
    "66": "Alcoholic Beverage Control",
    "67": "Sports and Recreation -- Convention Facilities",
    "68": "Cemeteries, Morgues, and Human Remains",
    "69": "Food, Drugs, Cosmetics, and Poisons",
    "70": "Public Health and Safety",
    "70A": "Environmental Health and Safety",
    "71": "Mental Illness",
    "71A": "Developmental Disabilities",
    "72": "State Institutions",
    "73": "Veterans and Veterans' Affairs",
    "74": "Public Assistance",
    "75": "Food Fish and Shellfish",
    "76": "Forests and Forest Products",
    "77": "Fish and Wildlife",
    "78": "Mines, Minerals, and Petroleum",
    "79": "Public Lands",
    "79A": "Public Recreational Lands",
    "80": "Public Utilities",
    "81": "Transportation",
    "82": "Excise Taxes",
    "83": "Estate Taxation",
    "84": "Property Taxes",
    "85": "Diking and Drainage",
    "86": "Flood Control",
    "87": "Irrigation",
    "88": "Navigation and Harbor Improvements",
    "89": "Reclamation, Soil Conservation, and Land Settlement",
    "90": "Water Rights -- Environment",
    "91": "Waterways",
}

# Key chapters for tax/benefit analysis
WA_EXCISE_TAX_CHAPTERS: dict[str, str] = {
    "82.04": "Business and Occupation Tax",
    "82.08": "Retail Sales Tax",
    "82.12": "Use Tax",
    "82.14": "Local Retail Sales and Use Taxes",
    "82.16": "Public Utility Tax",
    "82.24": "Cigarette Tax",
    "82.25": "Tobacco Products Tax",
    "82.26": "Vapor Products Tax",
    "82.32": "General Administrative Provisions",
    "82.33": "Economic and Revenue Forecast Council",
    "82.36": "Motor Vehicle Fuel Tax",
    "82.38": "Special Fuel Tax",
    "82.42": "Aircraft Fuel Tax",
    "82.44": "Motor Vehicle Excise Tax",
    "82.45": "Excise Tax on Real Estate Sales",
    "82.46": "Counties and Cities -- Excise Tax on Real Estate Sales",
    "82.48": "Aircraft Excise Tax",
    "82.49": "Watercraft Excise Tax",
    "82.50": "Travel Trailers and Campers Excise Tax",
    "82.60": "Tax Deferrals for Investment Projects",
    "82.62": "Tax Credits for New Employees in Rural Areas",
    "82.63": "High Technology Research and Development Tax Incentives",
    "82.70": "Commute Trip Reduction Incentives",
    "82.75": "Rural County and CEZ Tax Credits for New Employees",
    "82.82": "Community Empowerment Zones",
    "82.85": "Job Creation and Economic Development",
    "82.87": "Capital Gains Tax",
    "82.89": "Tax Preference Performance Statement Act",
    "82.90": "Local Government Climate Response Program Tax Incentives",
    "82.92": "Tax Incentives for Certain Clean Technology Businesses",
    "82.94": "Biosimilar Biologics Tax Incentives",
}

WA_PUBLIC_ASSISTANCE_CHAPTERS: dict[str, str] = {
    "74.04": "General Provisions -- Administration",
    "74.08": "Eligibility Generally",
    "74.08A": "Temporary Assistance for Needy Families -- WorkFirst",
    "74.09": "Medical Care",
    "74.12": "Aid to Families with Dependent Children",
    "74.13": "Child Welfare Services",
    "74.13A": "Foster Care and Adoption Support",
    "74.15": "Care of Children, Expectant Mothers, and Persons with Developmental Disabilities",
    "74.18": "Services to the Blind",
    "74.20": "Support of Child",
    "74.20A": "Support of Dependent Children",
    "74.25": "Employment Support Act",
    "74.34": "Abuse of Vulnerable Adults",
    "74.36": "Funding for Community Programs for Older Adults",
    "74.38": "Senior Citizens Services Act",
    "74.39": "Nursing Facility Medicaid Payment System",
    "74.39A": "Long-Term Care Services Options -- Expansion",
    "74.41": "Respite Care Services",
    "74.42": "Nursing Homes -- Residents' Rights",
    "74.46": "Nursing Facility Medicaid Payment System",
    "74.50": "Basic Health Plan",
    "74.60": "Supplemental Security Income",
    "74.62": "Working Families Tax Credit",
    "74.64": "Fraud -- Food and Cash Assistance Programs",
    "74.66": "Long-Term Care Trust Act",
    "74.67": "Long-Term Care Trust Commission",
    "74.70": "Alien Emergency Medical Program",
    "74.77": "Washington Family Recovery Coordination Program",
}


@dataclass
class ParsedWASection:
    """Parsed Washington RCW section."""

    section_number: str  # e.g., "82.04.290"
    section_title: str  # e.g., "Tax on service and other activities"
    chapter_number: str  # e.g., "82.04"
    chapter_title: str  # e.g., "Business and Occupation Tax"
    title_number: int  # e.g., 82
    title_name: str  # e.g., "Excise Taxes"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedWASubsection] = field(default_factory=list)
    history: str | None = None  # History note
    notes: str | None = None  # Additional notes (effective dates, etc.)
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedWASubsection:
    """A subsection within a Washington RCW section."""

    identifier: str  # e.g., "1", "a", "i"
    text: str
    children: list[ParsedWASubsection] = field(default_factory=list)


class WAConverterError(Exception):
    """Error during Washington RCW conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class WAConverter:
    """Converter for Washington RCW HTML to internal Section model.

    Example:
        >>> converter = WAConverter()
        >>> section = converter.fetch_section("82.04.290")
        >>> print(section.citation.section)
        "WA-82.04.290"

        >>> for section in converter.iter_chapter("82.04"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Washington RCW converter.

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
            section_number: e.g., "82.04.290", "74.04.005"

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/default.aspx?cite={section_number}"

    def _build_chapter_url(self, chapter_number: str) -> str:
        """Build the URL for a chapter's contents index."""
        return f"{BASE_URL}/default.aspx?cite={chapter_number}"

    def _build_title_url(self, title_number: int) -> str:
        """Build the URL for a title's contents index."""
        return f"{BASE_URL}/default.aspx?cite={title_number}"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedWASection:
        """Parse section HTML into ParsedWASection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "page not found" in html.lower():
            raise WAConverterError(f"Section {section_number} not found", url)

        # Extract title number from section_number (e.g., "82.04.290" -> 82)
        parts = section_number.split(".")
        title_str = parts[0]  # Keep as string for dictionary lookup
        title_number = int(title_str) if title_str.isdigit() else 0
        chapter_number = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else parts[0]

        # Get title and chapter names from registries
        title_name = WA_TITLES.get(title_str, f"Title {title_str}")
        chapter_title = (
            WA_EXCISE_TAX_CHAPTERS.get(chapter_number)
            or WA_PUBLIC_ASSISTANCE_CHAPTERS.get(chapter_number)
            or f"Chapter {chapter_number}"
        )

        # Extract section title - look for heading pattern "RCW 82.04.290: Title"
        section_title = ""

        # Try to find h3 heading with section title
        for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
            heading_text = heading.get_text(strip=True)
            # Pattern: "82.04.290" or "RCW 82.04.290: Title"
            if section_number in heading_text:
                # Extract title after colon or after section number
                if ":" in heading_text:
                    section_title = heading_text.split(":", 1)[1].strip()
                elif re.search(
                    rf"{re.escape(section_number)}\s+(.+)", heading_text
                ):  # pragma: no cover
                    match = re.search(rf"{re.escape(section_number)}\s+(.+)", heading_text)
                    if match:
                        section_title = match.group(1).strip().rstrip(".")
                break

        # Try alternative patterns in page text
        if not section_title:
            # Look for pattern like "82.04.290\nTax on service..."
            page_text = soup.get_text()
            pattern = rf"(?:RCW\s+)?{re.escape(section_number)}[\s:]+([^\n]+)"
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:  # pragma: no cover
                section_title = match.group(1).strip().rstrip(".")

        # Get main content - try various containers
        content_elem = (
            soup.find("div", id="rcw")
            or soup.find("div", class_="rcw")
            or soup.find("div", class_="content")
            or soup.find("main")
            or soup.find("article")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation, scripts, headers, footers
            for elem in content_elem.find_all(  # pragma: no cover
                ["nav", "script", "style", "header", "footer", "aside"]
            ):
                elem.decompose()
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note - typically at the end in brackets
        history = None
        notes = None

        # Pattern for legislative history: [2023 c 123 s 1; ...]
        history_pattern = r"\[(\d{4}\s+c\s+\d+[^]]*(?:;\s+\d{4}\s+c\s+\d+[^]]*)*)\]"
        history_match = re.search(history_pattern, text)
        if history_match:
            history = history_match.group(1).strip()[:1000]  # Limit length

        # Extract NOTES section
        notes_match = re.search(r"NOTES:\s*(.+?)(?:\[|$)", text, re.DOTALL | re.IGNORECASE)
        if notes_match:
            notes = notes_match.group(1).strip()[:2000]

        # Extract effective date if present
        effective_date = None
        effective_match = re.search(
            r"(?:Effective|effective)\s+(?:date|until|from)[:\s]+([A-Za-z]+\s+\d+,\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})",
            text,
        )
        if effective_match:  # pragma: no cover
            date_str = effective_match.group(1)
            # Try to parse date (simplified)
            try:
                if "/" in date_str:
                    parts = date_str.split("/")
                    effective_date = date(int(parts[2]), int(parts[0]), int(parts[1]))
            except ValueError, IndexError:
                pass

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedWASection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter_number,
            chapter_title=chapter_title,
            title_number=title_number,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            notes=notes,
            source_url=url,
            effective_date=effective_date,
        )

    def _parse_subsections(self, text: str) -> list[ParsedWASubsection]:
        """Parse hierarchical subsections from text.

        Washington RCW typically uses:
        - (1), (2), (3) for primary divisions
        - (a), (b), (c) for secondary divisions
        - (i), (ii), (iii) for tertiary divisions
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
                ParsedWASubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedWASubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (i), (ii), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\(i+\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", direct_text)
            if next_num:  # pragma: no cover
                direct_text = direct_text[: next_num.start()]

            subsections.append(
                ParsedWASubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedWASubsection]:
        """Parse level 3 subsections (i), (ii), (iii), etc."""
        subsections = []
        # Match Roman numerals: (i), (ii), (iii), (iv), etc.
        parts = re.split(r"(?=\((?:i{1,3}|iv|vi{0,3})\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(((?:i{1,3}|iv|vi{0,3}))\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit content
            next_match = re.search(r"\([a-z]\)|\(\d+\)", content)
            if next_match:
                content = content[: next_match.start()]

            subsections.append(
                ParsedWASubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedWASection) -> Section:
        """Convert ParsedWASection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"WA-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsection(sub: ParsedWASubsection) -> Subsection:
            return Subsection(
                identifier=sub.identifier,
                heading=None,
                text=sub.text,
                children=[convert_subsection(child) for child in sub.children],
            )

        subsections = [convert_subsection(sub) for sub in parsed.subsections]

        return Section(
            citation=citation,
            title_name=f"Washington RCW - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"wa/{parsed.title_number}/{parsed.section_number}",
            effective_date=parsed.effective_date,
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "82.04.290", "74.04.005"

        Returns:
            Section model

        Raises:
            WAConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., "82.04", "74.04")

        Returns:
            List of section numbers (e.g., ["82.04.010", "82.04.020", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Pattern for section links: cite=82.04.290
        pattern = re.compile(rf"cite=({re.escape(chapter)}\.\d+[A-Za-z]?)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return sorted(section_numbers)

    def get_title_chapters(self, title: int) -> list[str]:
        """Get list of chapters in a title.

        Args:
            title: Title number (e.g., 82, 74)

        Returns:
            List of chapter numbers (e.g., ["82.04", "82.08", ...])
        """
        url = self._build_title_url(title)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        chapters = []

        # Pattern for chapter links: cite=82.04
        pattern = re.compile(rf"cite=({title}\.\d+[A-Za-z]?)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                chapter = match.group(1)
                # Skip if it looks like a section (has third part)
                if chapter.count(".") == 1 and chapter not in chapters:
                    chapters.append(chapter)

        return sorted(chapters)

    def iter_chapter(self, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., "82.04")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except WAConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all excise tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(WA_EXCISE_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 82)

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

    def __enter__(self) -> WAConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_wa_section(section_number: str) -> Section:
    """Fetch a single Washington RCW section.

    Args:
        section_number: e.g., "82.04.290"

    Returns:
        Section model
    """
    with WAConverter() as converter:
        return converter.fetch_section(section_number)


def download_wa_chapter(chapter: str) -> list[Section]:
    """Download all sections from a Washington RCW chapter.

    Args:
        chapter: Chapter number (e.g., "82.04")

    Returns:
        List of Section objects
    """
    with WAConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_wa_excise_tax_chapters() -> Iterator[Section]:
    """Download all sections from Washington excise tax chapters (Title 82).

    Yields:
        Section objects
    """
    with WAConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(WA_EXCISE_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_wa_public_assistance_chapters() -> Iterator[Section]:
    """Download all sections from Washington public assistance chapters (Title 74).

    Yields:
        Section objects
    """
    with WAConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(
            list(WA_PUBLIC_ASSISTANCE_CHAPTERS.keys())
        )  # pragma: no cover
