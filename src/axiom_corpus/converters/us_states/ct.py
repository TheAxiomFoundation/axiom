"""Connecticut state statute converter.

Converts Connecticut General Statutes HTML from cga.ct.gov
to the internal Section model for ingestion.

Connecticut Statute Structure:
- Titles (e.g., Title 12: Taxation)
- Chapters (e.g., Chapter 203: Property Tax Assessment)
- Sections (e.g., Sec. 12-41: Filing of declaration)

URL Patterns:
- Titles index: https://www.cga.ct.gov/current/pub/titles.htm
- Title index: https://www.cga.ct.gov/current/pub/title_12.htm
- Chapter: https://www.cga.ct.gov/current/pub/chap_203.htm
- Section anchor: chap_203.htm#sec_12-41

Key Titles for Tax/Benefit Analysis:
- Title 12: Taxation
- Title 17b: Social Services

Example:
    >>> from axiom_corpus.converters.us_states.ct import CTConverter
    >>> converter = CTConverter()
    >>> section = converter.fetch_section("12-41")
    >>> print(section.section_title)
    "Filing of declaration"
"""

import re
import ssl
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup, Tag

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://www.cga.ct.gov/current/pub"

# Connecticut Titles mapping
CT_TITLES: dict[str, str] = {
    "1": "Provisions of General Application",
    "2": "General Assembly and Legislative Agencies",
    "3": "State Elective Officers",
    "4": "Management of State Agencies",
    "5": "State Officers in General",
    "6": "State Board of Accountancy",
    "7": "Municipalities",
    "8": "Housing",
    "9": "Elections",
    "10": "Education",
    "10a": "Higher Education",
    "11": "Libraries",
    "12": "Taxation",
    "13": "Eminent Domain",
    "13a": "Roads, Bridges and Highways",
    "13b": "Transportation",
    "14": "Motor Vehicles, Use of the Highway and Mass Transportation",
    "15": "Navigation and Aeronautics",
    "16": "Public Service Companies",
    "16a": "Planning and Energy Policy",
    "17": "Social and Human Services and Resources",
    "17a": "Department of Developmental Services",
    "17b": "Social Services",
    "18": "Social and Human Services and Resources",
    "19": "Public Health and Safety",
    "19a": "Public Health and Well-Being",
    "20": "Professional and Occupational Licensing, Certification",
    "21": "Agriculture. Domestic Animals",
    "21a": "Consumer Protection",
    "22": "Agriculture. Domestic Animals",
    "22a": "Environmental Protection",
    "23": "Parks, Forests and Public Shade Trees",
    "24": "Burial",
    "25": "Water Resources",
    "26": "Fisheries and Game",
    "27": "Military Department",
    "28": "Civil Preparedness and Emergency Services",
    "29": "Public Safety and State Police",
    "30": "Intoxicating Liquors",
    "31": "Labor",
    "32": "Commerce and Economic and Community Development",
    "33": "Corporations",
    "34": "Commercial Law",
    "35": "Antitrust and Trade Regulations",
    "36": "Money and Credit",
    "36a": "Banks and Bank Holding Companies",
    "36b": "Securities, Credit Unions and Insurance Companies",
    "37": "Weights and Measures",
    "38a": "Insurance",
    "39": "Interest",
    "40": "Assignments",
    "41": "Auctions",
    "42": "Business, Selling, Trading and Collection Practices",
    "42a": "Uniform Commercial Code",
    "43": "Innkeepers",
    "44": "Liens",
    "45a": "Probate Courts and Procedure",
    "46a": "Human Rights",
    "46b": "Family Law",
    "47": "Land and Land Titles",
    "47a": "Landlord and Tenant",
    "48": "Mortgages",
    "49": "Pledges and Collateral Security",
    "50": "Partnerships",
    "51": "Courts",
    "52": "Civil Actions",
    "53": "Crimes",
    "53a": "Penal Code",
    "54": "Criminal Procedure",
}

# Key chapters in Title 12 (Taxation)
CT_TAX_CHAPTERS: dict[str, str] = {
    "201": "State and Local Revenue Services. Department of Revenue Services",
    "202": "Collection of State Taxes",
    "203": "Property Tax Assessment",
    "203a": "The Connecticut Appeals Board for Property Valuation",
    "204": "Local Levy and Collection of Taxes",
    "204a": "Property Tax Relief for Elderly Homeowners and Renters and Persons with Permanent Total Disability",
    "204b": "Property Tax Assessment Appeals",
    "205": "Tax Liens",
    "206": "Inheritance and Succession Taxes",
    "207": "Gift Tax",
    "208": "Estate Taxes",
    "209": "Taxes on Corporations",
    "210": "Corporations: Miscellaneous Tax Provisions",
    "211": "Income Tax",
    "212": "Business Entity Tax",
    "213": "Tax Amnesty Program",
    "214": "Business Tax Credits",
    "215": "Insurance Companies",
    "216": "Sales and Use Taxes",
    "217": "Admissions and Dues Taxes",
    "218": "Cigarettes and Tobacco Products",
    "219": "Motor Vehicle Fuels Tax",
    "220": "Controlled Substances Tax",
    "221": "Real Estate Conveyance Tax",
    "222": "Unrelated Business Income Tax",
    "223": "Miscellaneous Tax Provisions",
    "224": "Community Revitalization and Investment",
    "225": "Room Occupancy Tax",
    "226": "Prepared Food and Beverage Tax",
    "227": "Rental Surcharge on Leasing Companies",
    "228": "Passthrough Entity Tax",
}

# Key chapters in Title 17b (Social Services)
CT_WELFARE_CHAPTERS: dict[str, str] = {
    "301": "Department of Social Services. Commissioner",
    "302": "Family Support",
    "303": "Assistance Programs",
    "304": "Food Stamp Program",
    "305": "Housing Assistance",
    "306": "Medical Assistance Programs",
    "307": "Child Care",
    "308": "Adult Services",
    "309": "Disability Services",
    "310": "Miscellaneous",
    "319c": "Early Childhood Programs",
    "319d": "Refugee Assistance",
    "319e": "Employment Opportunity Act",
    "319f": "Temporary Family Assistance",
    "319g": "JOBS Program",
    "319h": "Energy Assistance",
    "319i": "Connecticut HUSKY Plan",
    "319j": "Health Care For Uninsured Children",
    "319k": "Connecticut Home Care Program for Elders",
    "319l": "Nursing Home Financial Assistance",
    "319m": "CT Home Care Program for Disabled Adults",
    "319n": "Community-Based Services for Elderly Persons",
    "319o": "Personal Care Assistance",
    "319p": "Cash Assistance Programs",
    "319q": "Employment Services",
    "319r": "Medicare Savings Program",
    "319s": "Connecticut Partnership for Long-Term Care",
    "319t": "State Supplement to Supplemental Security Income",
    "319u": "Aid to the Aged, Blind and Disabled",
    "319v": "Temporary Assistance for Needy Families",
    "319w": "Connecticut Works",
}


@dataclass
class ParsedCTSection:
    """Parsed Connecticut statute section."""

    section_number: str  # e.g., "12-41"
    section_title: str  # e.g., "Filing of declaration"
    chapter_number: str  # e.g., "203"
    chapter_title: str  # e.g., "Property Tax Assessment"
    title_number: str  # e.g., "12"
    title_name: str  # e.g., "Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedCTSubsection] = field(default_factory=list)
    history: str | None = None  # History note
    source: str | None = None  # Source/citation info
    annotations: list[str] = field(default_factory=list)
    source_url: str = ""


@dataclass
class ParsedCTSubsection:
    """A subsection within a Connecticut statute."""

    identifier: str  # e.g., "a", "1", "A"
    heading: str | None = None  # Optional subsection heading
    text: str = ""
    children: list[ParsedCTSubsection] = field(default_factory=list)


class CTConverterError(Exception):
    """Error during Connecticut statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class CTConverter:
    """Converter for Connecticut General Statutes HTML to internal Section model.

    Example:
        >>> converter = CTConverter()
        >>> section = converter.fetch_section("12-41")
        >>> print(section.citation.section)
        "CT-12-41"

        >>> for section in converter.iter_chapter("203"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
        verify_ssl: bool = False,
    ):
        """Initialize the Connecticut statute converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
            year: Statute year (default: current year)
            verify_ssl: Whether to verify SSL certificates (default: False due to CT site cert issues)
        """
        self.rate_limit_delay = rate_limit_delay
        self.year = year or date.today().year
        self.verify_ssl = verify_ssl
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            # Create SSL context that doesn't verify certificates
            # (CT website has certificate issues)
            if not self.verify_ssl:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                self._client = httpx.Client(
                    timeout=60.0,
                    headers={
                        "User-Agent": "Axiom/1.0 (Statute Research; contact@axiom-foundation.org)"
                    },
                    verify=ssl_context,
                )
            else:
                self._client = httpx.Client(
                    timeout=60.0,
                    headers={
                        "User-Agent": "Axiom/1.0 (Statute Research; contact@axiom-foundation.org)"
                    },
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

    def _get_chapter_for_section(self, section_number: str) -> str:
        """Determine which chapter contains a section number.

        Connecticut sections are numbered like "12-41" where 12 is the title
        and 41 is the section within that title. Chapters group sections.

        This requires looking up the chapter containing the section.
        For simplicity, we extract the title prefix and search known chapters.
        """
        title_num = section_number.split("-")[0]

        # For Title 12, map section ranges to chapters
        if title_num == "12":
            section_suffix = section_number.split("-")[1] if "-" in section_number else ""
            try:
                sec_num = int(re.match(r"(\d+)", section_suffix).group(1)) if section_suffix else 0
            except AttributeError, ValueError:  # pragma: no cover
                sec_num = 0  # pragma: no cover

            # Section number ranges for Title 12 chapters (approximate)
            if sec_num <= 39:
                return "201"  # pragma: no cover
            elif sec_num <= 39:
                return "202"  # pragma: no cover
            elif 40 <= sec_num <= 170:
                return "203"
            elif 171 <= sec_num <= 195:
                return "204"  # pragma: no cover
            elif 196 <= sec_num <= 220:
                return "204a"  # pragma: no cover
            elif 221 <= sec_num <= 299:
                return "205"  # pragma: no cover
            elif 300 <= sec_num <= 399:
                return "206"  # pragma: no cover
            elif 400 <= sec_num <= 499:
                return "208"  # pragma: no cover
            elif 500 <= sec_num <= 599:
                return "209"  # pragma: no cover
            elif 600 <= sec_num <= 699:
                return "211"  # pragma: no cover
            elif 700 <= sec_num <= 799:
                return "216"
            else:
                return "203"  # Default  # pragma: no cover

        # For Title 17b, similar mapping
        elif title_num == "17b":  # pragma: no cover
            return "319v"  # TANF chapter as default  # pragma: no cover

        return "203"  # Default fallback  # pragma: no cover

    def _build_chapter_url(self, chapter: str) -> str:
        """Build the URL for a chapter page."""
        return f"{BASE_URL}/chap_{chapter}.htm"

    def _build_section_url(self, section_number: str, chapter: str | None = None) -> str:
        """Build the URL for a specific section.

        Args:
            section_number: e.g., "12-41"
            chapter: Chapter number (if known), otherwise will be determined

        Returns:
            Full URL to the section (chapter page with anchor)
        """
        if chapter is None:
            chapter = self._get_chapter_for_section(section_number)  # pragma: no cover
        return f"{BASE_URL}/chap_{chapter}.htm#sec_{section_number}"

    def _extract_title_from_section(self, section_number: str) -> tuple[str, str]:
        """Extract title number and name from section number.

        Args:
            section_number: e.g., "12-41"

        Returns:
            Tuple of (title_number, title_name)
        """
        title_num = section_number.split("-")[0]
        title_name = CT_TITLES.get(title_num, f"Title {title_num}")
        return title_num, title_name

    def _parse_chapter_html(self, html: str, chapter: str) -> dict[str, ParsedCTSection]:
        """Parse chapter HTML and extract all sections.

        Returns dict mapping section_number to ParsedCTSection.
        """
        soup = BeautifulSoup(html, "html.parser")
        sections = {}

        # Get chapter info from header
        chapter_title = CT_TAX_CHAPTERS.get(chapter) or CT_WELFARE_CHAPTERS.get(
            chapter, f"Chapter {chapter}"
        )

        # Find chapter name from h2 element
        soup.find("h2", class_="chap-no")
        chap_name = soup.find("h2", class_="chap-name")
        if chap_name:
            chapter_title = chap_name.get_text(strip=True)

        # Find all section headers (span.catchln with id starting with sec_)
        section_spans = soup.find_all("span", class_="catchln", id=re.compile(r"^sec_"))

        for span in section_spans:
            section_id = span.get("id", "")
            section_number = section_id.replace("sec_", "")

            if not section_number:
                continue  # pragma: no cover

            # Parse section title from the span text
            # Format: "Sec. 12-41. Filing of declaration."
            span_text = span.get_text(strip=True)
            title_match = re.match(r"Sec\.\s*[\d\w-]+\.\s*(.+?)\.?$", span_text)
            section_title = title_match.group(1) if title_match else span_text

            # Get title info
            title_num, title_name = self._extract_title_from_section(section_number)

            # Collect all content for this section
            section_text, section_html, subsections, history, source, annotations = (
                self._extract_section_content(soup, span)
            )

            sections[section_number] = ParsedCTSection(
                section_number=section_number,
                section_title=section_title,
                chapter_number=chapter,
                chapter_title=chapter_title,
                title_number=title_num,
                title_name=title_name,
                text=section_text,
                html=section_html,
                subsections=subsections,
                history=history,
                source=source,
                annotations=annotations,
                source_url=f"{BASE_URL}/chap_{chapter}.htm#sec_{section_number}",
            )

        return sections

    def _extract_section_content(
        self, soup: BeautifulSoup, start_span: Tag
    ) -> tuple[str, str, list[ParsedCTSubsection], str | None, str | None, list[str]]:
        """Extract content for a section starting from its header span.

        Returns:
            Tuple of (text, html, subsections, history, source, annotations)
        """
        text_parts = []
        html_parts = []
        history = None
        source = None
        annotations = []

        # Start from the parent P element
        current = start_span.find_parent("p")
        if not current:
            return "", "", [], None, None, []  # pragma: no cover

        # Collect elements until we hit the next section
        while current:
            # Check if this element contains another section header (not our starting one)
            next_catchln = current.find("span", class_="catchln")
            if next_catchln and next_catchln != start_span:
                break

            # Get class to identify element type
            elem_class = current.get("class", [])
            if isinstance(elem_class, list):
                elem_class = " ".join(elem_class)

            text_content = current.get_text(strip=True)

            # Categorize content by class
            if "history-first" in elem_class or "history" in elem_class:
                history = text_content
            elif "source-first" in elem_class or "source" in elem_class:
                source = text_content
            elif "annotation-first" in elem_class or "annotation" in elem_class:
                annotations.append(text_content)
            else:
                # Regular section text
                text_parts.append(text_content)
                html_parts.append(str(current))

            current = current.find_next_sibling()

        full_text = "\n".join(text_parts)
        full_html = "\n".join(html_parts)

        # Parse subsections from text
        subsections = self._parse_subsections(full_text)

        return full_text, full_html, subsections, history, source, annotations

    def _parse_subsections(self, text: str) -> list[ParsedCTSubsection]:
        """Parse hierarchical subsections from text.

        Connecticut statutes typically use:
        - (a), (b), (c) for primary divisions (often with headings in bold)
        - (1), (2), (3) for secondary divisions
        - (A), (B), (C) for tertiary divisions
        """
        subsections = []

        # Split by top-level subsections (a), (b), etc.
        # Note: CT statutes may not have space after the closing paren
        parts = re.split(r"(?=\([a-z]\))", text)

        for part in parts[1:]:  # Skip content before first (a)
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Check for bold heading at start
            heading = None
            heading_match = re.match(r"([^.]+\.)\s*", content)
            if heading_match:
                potential_heading = heading_match.group(1).strip()
                # Headings are typically short phrases ending in period
                if len(potential_heading) < 100 and potential_heading[0].isupper():
                    heading = potential_heading.rstrip(".")
                    content = content[heading_match.end() :]

            # Parse level 2 children (1), (2), etc.
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

            # Clean up - stop at next top-level subsection
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedCTSubsection(
                    identifier=identifier,
                    heading=heading,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedCTSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        # Note: CT statutes may not have space after the closing paren
        parts = re.split(r"(?=\(\d+\))", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (A), (B), etc.
            children = self._parse_level3(content)

            # Limit content and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            # Also stop at next letter subsection
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]  # pragma: no cover

            subsections.append(
                ParsedCTSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedCTSubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        # Note: CT statutes may not have space after the closing paren
        parts = re.split(r"(?=\([A-Z]\))", text)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit content
            next_match = re.search(r"\([A-Z]\)|\(\d+\)|\([a-z]\)", content)
            if next_match:
                content = content[: next_match.start()]  # pragma: no cover

            subsections.append(
                ParsedCTSubsection(
                    identifier=identifier,
                    text=content.strip()[:1000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedCTSection) -> Section:
        """Convert ParsedCTSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"CT-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsection(sub: ParsedCTSubsection) -> Subsection:
            return Subsection(
                identifier=sub.identifier,
                heading=sub.heading,
                text=sub.text,
                children=[convert_subsection(child) for child in sub.children],
            )

        subsections = [convert_subsection(sub) for sub in parsed.subsections]

        return Section(
            citation=citation,
            title_name=f"Connecticut General Statutes - Title {parsed.title_number}: {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ct/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_chapter(self, chapter: str) -> dict[str, Section]:
        """Fetch and parse all sections from a chapter.

        Args:
            chapter: Chapter number (e.g., "203", "211")

        Returns:
            Dict mapping section number to Section model
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        parsed_sections = self._parse_chapter_html(html, chapter)

        return {sec_num: self._to_section(parsed) for sec_num, parsed in parsed_sections.items()}

    def fetch_section(self, section_number: str, chapter: str | None = None) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "12-41"
            chapter: Chapter number (optional, will be determined if not provided)

        Returns:
            Section model

        Raises:
            CTConverterError: If section not found or parsing fails
        """
        if chapter is None:
            chapter = self._get_chapter_for_section(section_number)  # pragma: no cover

        # Fetch the entire chapter and extract the specific section
        sections = self.fetch_chapter(chapter)

        if section_number not in sections:
            raise CTConverterError(
                f"Section {section_number} not found in chapter {chapter}",
                url=self._build_section_url(section_number, chapter),
            )

        return sections[section_number]

    def get_chapter_section_numbers(self, chapter: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., "203")

        Returns:
            List of section numbers (e.g., ["12-40", "12-41", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []
        section_spans = soup.find_all("span", class_="catchln", id=re.compile(r"^sec_"))

        for span in section_spans:
            section_id = span.get("id", "")
            section_number = section_id.replace("sec_", "")
            if section_number:
                section_numbers.append(section_number)

        return section_numbers

    def iter_chapter(self, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., "203")

        Yields:
            Section objects for each section
        """
        sections = self.fetch_chapter(chapter)
        yield from sections.values()

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
            chapters = list(CT_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            try:  # pragma: no cover
                yield from self.iter_chapter(chapter)  # pragma: no cover
            except Exception as e:  # pragma: no cover
                print(f"Warning: Could not fetch chapter {chapter}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> CTConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ct_section(section_number: str, chapter: str | None = None) -> Section:
    """Fetch a single Connecticut statute section.

    Args:
        section_number: e.g., "12-41"
        chapter: Optional chapter number

    Returns:
        Section model
    """
    with CTConverter() as converter:
        return converter.fetch_section(section_number, chapter)


def download_ct_chapter(chapter: str) -> list[Section]:
    """Download all sections from a Connecticut Statutes chapter.

    Args:
        chapter: Chapter number (e.g., "203")

    Returns:
        List of Section objects
    """
    with CTConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_ct_tax_chapters() -> Iterator[Section]:
    """Download all sections from Connecticut tax-related chapters (Title 12).

    Yields:
        Section objects
    """
    with CTConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(CT_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_ct_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Connecticut social services chapters (Title 17b).

    Yields:
        Section objects
    """
    with CTConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(CT_WELFARE_CHAPTERS.keys()))  # pragma: no cover
