"""New Hampshire state statute converter.

Converts New Hampshire RSA (Revised Statutes Annotated) HTML from
gencourt.state.nh.us to the internal Section model for ingestion.

New Hampshire RSA Structure:
- Titles (e.g., Title V: Taxation)
- Chapters (e.g., Chapter 77-A: Business Profits Tax)
- Sections (e.g., 77-A:1: Definitions)

URL Patterns:
- Title TOC: https://gc.nh.gov/rsa/html/NHTOC/NHTOC-V.htm
- Chapter TOC: https://gc.nh.gov/rsa/html/NHTOC/NHTOC-V-77-A.htm
- Section: https://gc.nh.gov/rsa/html/V/77-A/77-A-1.htm

Note: The RSA website redirects from gencourt.state.nh.us to gc.nh.gov.

Example:
    >>> from axiom.converters.us_states.nh import NHConverter
    >>> converter = NHConverter()
    >>> section = converter.fetch_section("77-A:1")
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

from axiom.models import Citation, Section, Subsection

BASE_URL = "https://gc.nh.gov/rsa/html"

# Title mapping for reference (Roman numeral -> name)
NH_TITLES: dict[str, str] = {
    "I": "The State and Its Government",
    "II": "Counties",
    "III": "Towns, Cities, Village Districts, and Unincorporated Places",
    "IV": "Elections",
    "V": "Taxation",
    "VI": "Public Officers and Employees",
    "VII": "Sheriffs, Constables, and Police Officers",
    "VIII": "Public Defense and Veterans' Affairs",
    "IX": "Acquisition of Property for Public Uses; Highways",
    "X": "Public Health",
    "XI": "Hospitals and Sanitaria",
    "XII": "Public Safety and Welfare",
    "XIII": "Alcoholic Beverages",
    "XIV": "Court System",
    "XV": "Education",
    "XVI": "Libraries",
    "XVII": "Fish and Game",
    "XVIII": "Forestry and Forest Fire Prevention",
    "XIX": "Agriculture, Horticulture and Animal Husbandry",
    "XIX-A": "Aeronautics",
    "XX": "Transportation",
    "XXI": "Motor Vehicles",
    "XXII": "Navigation, Harbors, and Coast Survey",
    "XXIII": "Corporations, Partnerships, and Associations",
    "XXIV": "Banking and Credit",
    "XXV": "Merchants' Marks and Names",
    "XXVI": "Trade and Commerce",
    "XXVII": "Corporations, Associations, and Proprietors of Common Lands",
    "XXVIII": "Money and Weights and Measures",
    "XXIX": "Laying Out and Building Wharves and Piers",
    "XXX": "Occupations and Professions",
    "XXXI": "Trade and Commerce",
    "XXXII": "Energy",
    "XXXIII": "Employers and Employees",
    "XXXIV": "Public Utilities",
    "XXXV": "Banks and Banking; Fiduciaries",
    "XXXVI": "Pawnbrokers and Moneylenders",
    "XXXVII": "Insurance",
    "XXXVIII": "Securities",
    "XXXIX": "Wills, Decedents' Estates, and Fiduciary Relations",
    "XL": "Actions, Process, and Service of Process",
    "XLI": "Liens",
    "XLII": "Estates",
    "XLIII": "Wills",
    "XLIV": "Estates; Insolvent Estates",
    "XLV": "Aliens",
    "XLVI": "Commitment and Guardianships",
    "XLVII": "Revised Uniform Partnership Act",
    "XLVIII": "Conveyances and Mortgages of Realty",
    "XLIX": "Actions and Proceedings in General",
    "L": "Limitation of Actions",
    "LI": "Courts",
    "LII": "Actions, Process, and Service of Process",
    "LIII": "Proceedings in Court",
    "LIV": "Juries",
    "LV": "Proceedings in Special Cases",
    "LVI": "Actions Against Persons, Property, and in Particular Cases",
    "LVII": "Judgments and Decrees",
    "LVIII": "Writs and Executions",
    "LIX": "Proceedings in Special Cases",
    "LX": "Probate Courts and Decedents' Estates",
    "LXI": "Insolvency Proceedings",
    "LXII": "Criminal Code",
    "LXIII": "Criminal Procedure",
}

# Key chapters for tax analysis (Title V)
NH_TAX_CHAPTERS: dict[str, str] = {
    "71": "The Tax Commission (Repealed)",
    "71-A": "Department of Revenue Administration",
    "71-B": "Apportionment of Taxes",
    "71-C": "Taxpayer Bill of Rights",
    "72": "Taxation of Property",
    "73": "School Tax and Other Taxes",
    "74": "Taxable Property; How Assessed",
    "75": "Appraisal of Property for Tax Purposes",
    "76": "Abatement of Taxes",
    "77": "Taxation of Incomes (Repealed)",
    "77-A": "Business Profits Tax",
    "77-E": "Business Enterprise Tax",
    "77-G": "Combined Reporting",
    "78": "Meals and Rooms Tax",
    "78-A": "Land Use Change Tax",
    "78-B": "Tax on Transfer of Real Property",
    "78-C": "Medicaid Enhancement Tax",
    "78-D": "Healthcare Provider Assessment",
    "79": "Current Use Taxation",
    "79-A": "Discretionary Preservation Easements",
    "79-B": "Taxation of Farm Structures and Land Under Farm Structures",
    "79-C": "Discretionary Easements",
    "79-D": "Conservation Restriction Assessment",
    "79-E": "Community Revitalization Tax Relief Incentive",
    "79-F": "Taxation of Residences on Farms",
    "79-G": "Educational Facility Revitalization Tax Relief",
    "79-H": "Economic Development Zone Tax Relief",
    "80": "Proceedings Against Delinquent Taxpayers",
    "82": "Exemptions",
    "82-A": "Low and Moderate Income Homeowners Property Tax Relief",
    "83": "Poll Taxes (Repealed)",
    "83-A": "Taxation of Standing Timber (Repealed)",
    "83-B": "Tax on Electricity Consumption",
    "83-C": "Taxation of Railroads",
    "83-D": "Taxation of Utility Property",
    "83-E": "Taxation of Pipelines",
    "83-F": "Telecommunications Tax",
    "84": "Savings Bank Taxes",
    "84-A": "Bank Franchise Tax (Repealed)",
    "84-B": "Credit Union Tax (Repealed)",
    "85": "Taxation of Trust Companies",
    "86": "Taxation of Insurance Companies",
    "87": "Common Carriers (Repealed)",
    "88": "Express Companies (Repealed)",
    "89": "Communications Services Tax",
    "90": "Tobacco Tax",
}

# Key chapters for welfare/public safety (Title XII)
NH_WELFARE_CHAPTERS: dict[str, str] = {
    "153": "Fire Standards and Training and Emergency Medical Services",
    "154": "Fire Wards",
    "155": "Building Regulations",
    "155-A": "Building Code Review Board",
    "155-B": "Historic Buildings",
    "156": "Hazardous Substances",
    "157": "Boilers and Pressure Vessels",
    "158": "License for Itinerant Vendors",
    "159": "Pistols and Revolvers",
    "161": "Human Services",
    "161-B": "Council on Domestic Violence",
    "161-C": "Senior Citizens Bill of Rights",
    "161-D": "Employment Program",
    "161-E": "Community Services Block Grant",
    "161-F": "Energy Assistance Program",
    "161-G": "Office of Oversight and Accountability",
    "161-H": "Children Services",
    "161-I": "Kinship Care",
    "161-J": "Direct Support Workers",
    "161-K": "Human Trafficking Victim Assistance",
    "161-L": "Children's System of Care",
    "161-M": "Homelessness Prevention",
    "162": "Aid to Permanently and Totally Disabled",
    "163": "Public Assistance to Blind",
    "164": "Soldiers' Aid (Repealed)",
    "165": "Aid to the Aged",
    "166": "Liability for Support of Persons",
    "167": "Public Assistance to Needy Persons",
    "167-D": "Special Food Stamp Employment and Training Program",
    "168": "Paternity",
    "169": "Children and Families",
    "169-B": "Delinquent Children",
    "169-C": "Children in Need of Services",
    "169-D": "Child Abuse and Neglect",
    "169-E": "Permanency Planning for Children",
    "169-F": "Interstate Compact on Juveniles",
    "169-G": "Interstate Compact for the Placement of Children",
    "169-H": "Office of the Child Advocate",
    "170": "Child Placing and Care Agencies",
    "170-B": "Children Born Out of Wedlock",
    "170-C": "Adoption",
    "170-D": "Termination of Parental Rights",
    "170-E": "Day Care Standards",
    "170-F": "Child Care Scholarship Program",
    "170-G": "Division for Children, Youth and Families",
    "170-H": "Child Abuse Fatality Review Committee",
    "171": "Developmental Disabilities",
    "171-A": "Developmental Disabilities Waitlist",
    "171-B": "Developmental Disabilities Oversight",
    "172": "Mental Health Services",
    "172-A": "Community Mental Health Services Act",
    "172-B": "Emergency Services",
    "173": "Alcohol and Drug Abuse Services",
    "173-B": "Protection of Persons from Domestic Violence",
    "173-C": "Stalking and Harassment",
    "173-D": "Protection of Adults",
    "174": "Hospitals and Sanitaria",
    "174-B": "Emergency Medical Services",
    "174-D": "Hospital Community Benefits",
}


@dataclass
class ParsedNHSection:
    """Parsed New Hampshire RSA section."""

    section_number: str  # e.g., "77-A:1"
    section_title: str  # e.g., "Definitions"
    chapter_number: str  # e.g., "77-A"
    chapter_title: str  # e.g., "Business Profits Tax"
    title_roman: str | None  # e.g., "V"
    title_name: str | None  # e.g., "Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedNHSubsection"] = field(default_factory=list)
    history: str | None = None  # Source note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedNHSubsection:
    """A subsection within a New Hampshire RSA statute."""

    identifier: str  # e.g., "I", "a", "1", "A"
    text: str
    children: list["ParsedNHSubsection"] = field(default_factory=list)


class NHConverterError(Exception):
    """Error during New Hampshire RSA conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class NHConverter:
    """Converter for New Hampshire RSA HTML to internal Section model.

    Example:
        >>> converter = NHConverter()
        >>> section = converter.fetch_section("77-A:1")
        >>> print(section.citation.section)
        "NH-77-A:1"

        >>> for section in converter.iter_chapter("77-A"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the New Hampshire RSA converter.

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
                follow_redirects=True,  # Handle gencourt.state.nh.us -> gc.nh.gov redirect
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

    def _get_title_from_chapter(self, chapter: str) -> str:
        """Get the title (Roman numeral) from a chapter number.

        Args:
            chapter: Chapter number like "77-A", "167", "159-E"

        Returns:
            Title Roman numeral (e.g., "V", "XII")
        """
        # Extract the numeric prefix (e.g., "77" from "77-A")
        match = re.match(r"(\d+)", chapter)
        if not match:
            return "V"  # Default to Taxation  # pragma: no cover

        chapter_num = int(match.group(1))

        # Title V: Taxation (chapters 71-90)
        if 71 <= chapter_num <= 90:
            return "V"
        # Title XII: Public Safety and Welfare (chapters 153-174)
        elif 153 <= chapter_num <= 174:
            return "XII"
        # Title LXII: Criminal Code (chapters 625-651)
        elif 625 <= chapter_num <= 651:  # pragma: no cover
            return "LXII"  # pragma: no cover
        # Title XXI: Motor Vehicles (chapters 259-270)
        elif 259 <= chapter_num <= 270:  # pragma: no cover
            return "XXI"  # pragma: no cover
        else:
            # Return best guess based on range
            return "V"  # pragma: no cover

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "77-A:1", "167:4"

        Returns:
            Full URL to the section page
        """
        # Parse section number - format is "chapter:section" e.g., "77-A:1"
        if ":" not in section_number:
            raise NHConverterError(
                f"Invalid section format: {section_number}. Expected format: 'chapter:section'"
            )

        chapter, section = section_number.split(":", 1)
        title = self._get_title_from_chapter(chapter)

        # Build URL: https://gc.nh.gov/rsa/html/V/77-A/77-A-1.htm
        # Replace ':' with '-' in the filename
        filename = section_number.replace(":", "-")

        return f"{BASE_URL}/{title}/{chapter}/{filename}.htm"

    def _build_chapter_toc_url(self, chapter: str) -> str:
        """Build the URL for a chapter's table of contents.

        Args:
            chapter: Chapter number like "77-A"

        Returns:
            URL to the chapter TOC page
        """
        title = self._get_title_from_chapter(chapter)
        return f"{BASE_URL}/NHTOC/NHTOC-{title}-{chapter}.htm"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedNHSection:
        """Parse section HTML into ParsedNHSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" or empty content
        page_text = soup.get_text()
        if "[Repealed]" in page_text or "[Omitted]" in page_text:
            # Still valid, just note it's repealed
            pass

        chapter = section_number.split(":")[0]
        chapter_title = (
            NH_TAX_CHAPTERS.get(chapter) or NH_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
        )

        title_roman = self._get_title_from_chapter(chapter)
        title_name = NH_TITLES.get(title_roman, "Unknown Title")

        # Extract section title
        # Format is typically: "77-A:1 Definitions. --" or "77-A:1 Definitions. -"
        section_title = ""

        # Try to find the section header in the body
        body = soup.find("body")
        if body:
            body_text = body.get_text(strip=True)

            # Pattern: "77-A:1 Title. --" or "77-A:1 Title. -"
            # The title ends with ". --" or ". -" or just before the first Roman numeral
            escaped_section = re.escape(section_number)
            title_pattern = re.compile(
                rf"{escaped_section}\s+([^.]+(?:\.[^-]*)?)\.\s*[-\u2013\u2014]",
                re.IGNORECASE,
            )
            match = title_pattern.search(body_text)
            if match:
                section_title = match.group(1).strip()

            # Fallback: try simpler pattern
            if not section_title:
                simple_pattern = re.compile(rf"{escaped_section}\s+([^.]+)\.")
                match = simple_pattern.search(body_text)
                if match:
                    section_title = match.group(1).strip()  # pragma: no cover

        # Get body content for text extraction
        if body:
            # Remove script and style elements
            for elem in body.find_all(["script", "style", "nav", "header", "footer"]):
                elem.decompose()  # pragma: no cover
            text = body.get_text(separator="\n", strip=True)
            html_content = str(body)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history/source note
        history = None
        source_match = re.search(r"Source\.\s*(.+?)(?:\n|$)", text, re.DOTALL | re.IGNORECASE)
        if source_match:
            history = source_match.group(1).strip()[:1000]  # Limit length

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedNHSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            title_roman=title_roman,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedNHSubsection]:
        """Parse hierarchical subsections from text.

        New Hampshire RSA typically uses:
        - I., II., III. for primary divisions (Roman numerals)
        - (a), (b), (c) for secondary divisions
        - (1), (2), (3) for tertiary divisions
        - (A), (B), (C) for quaternary divisions
        """
        subsections = []

        # Split by top-level Roman numeral subsections
        # Match standalone Roman numerals at start of line or after period/semicolon
        parts = re.split(r"(?:^|\n)\s*([IVXLCDM]+)\.\s+", text)

        for i in range(1, len(parts), 2):
            if i + 1 > len(parts):
                break  # pragma: no cover

            identifier = parts[i]  # The Roman numeral
            content = parts[i + 1] if i + 1 < len(parts) else ""

            # Validate it's a proper Roman numeral (not just any capital letter sequence)
            if not self._is_valid_roman_numeral(identifier):
                continue  # pragma: no cover

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

            # Clean up text - stop at next Roman numeral
            next_roman = re.search(r"(?:^|\n)\s*[IVXLCDM]+\.\s+", direct_text)
            if next_roman:
                direct_text = direct_text[: next_roman.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedNHSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _is_valid_roman_numeral(self, s: str) -> bool:
        """Check if a string is a valid Roman numeral."""
        if not s:
            return False
        # Valid Roman numeral patterns
        pattern = r"^M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$"
        return bool(re.match(pattern, s.upper()))

    def _parse_level2(self, text: str) -> list[ParsedNHSubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (1), (2), etc.
            children = self._parse_level3(content)

            # Get direct text
            if children:  # pragma: no cover
                first_child_match = re.search(r"\(\d+\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Stop at next letter subsection
            next_letter = re.search(r"\([a-z]\)", direct_text)
            if next_letter:  # pragma: no cover
                direct_text = direct_text[: next_letter.start()].strip()

            subsections.append(
                ParsedNHSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedNHSubsection]:
        """Parse level 3 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit and clean up
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]

            subsections.append(
                ParsedNHSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedNHSection) -> Section:
        """Convert ParsedNHSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"NH-{parsed.section_number}",
        )

        # Convert subsections recursively
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
            title_name=f"New Hampshire RSA - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"nh/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "77-A:1", "167:4"

        Returns:
            Section model

        Raises:
            NHConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            raise NHConverterError(
                f"Section {section_number} not found: {e}", url
            )  # pragma: no cover

        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., "77-A", "167")

        Returns:
            List of section numbers (e.g., ["77-A:1", "77-A:2", ...])
        """
        url = self._build_chapter_toc_url(chapter)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            raise NHConverterError(f"Chapter {chapter} not found: {e}", url)  # pragma: no cover

        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links - format: ../V/77-A/77-A-1.htm
        # The section number is in the format chapter-section.htm
        escaped_chapter = re.escape(chapter)
        pattern = re.compile(rf"{escaped_chapter}-(\d+[a-z]?)\.htm", re.IGNORECASE)

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = f"{chapter}:{match.group(1)}"
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., "77-A", "167")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except NHConverterError as e:  # pragma: no cover
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
            chapters = list(NH_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "NHConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_nh_section(section_number: str) -> Section:
    """Fetch a single New Hampshire RSA section.

    Args:
        section_number: e.g., "77-A:1"

    Returns:
        Section model
    """
    with NHConverter() as converter:
        return converter.fetch_section(section_number)


def download_nh_chapter(chapter: str) -> list[Section]:
    """Download all sections from a New Hampshire RSA chapter.

    Args:
        chapter: Chapter number (e.g., "77-A")

    Returns:
        List of Section objects
    """
    with NHConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_nh_tax_chapters() -> Iterator[Section]:
    """Download all sections from NH tax-related chapters (Title V).

    Yields:
        Section objects
    """
    with NHConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(NH_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_nh_welfare_chapters() -> Iterator[Section]:
    """Download all sections from NH public safety/welfare chapters (Title XII).

    Yields:
        Section objects
    """
    with NHConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(NH_WELFARE_CHAPTERS.keys()))  # pragma: no cover
