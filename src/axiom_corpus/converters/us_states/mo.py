"""Missouri state statute converter.

Converts Missouri Revised Statutes HTML from revisor.mo.gov
to the internal Section model for ingestion.

Missouri Statute Structure:
- Titles (e.g., Title X: Taxation and Revenue)
- Chapters (e.g., Chapter 143: Income Tax)
- Sections (e.g., 143.011: Resident individuals -- tax rates)

URL Patterns:
- Section: main/OneSection.aspx?section=[section_number]
- Chapter: main/OneChapter.aspx?chapter=[chapter_number]
- Title index: main/OneTitle.aspx?title=[roman_numeral]

Note: Missouri has a progressive individual income tax with rates set by statute.
Key tax chapters: 143 (Income Tax), 144 (Sales and Use Tax), 135 (Tax Credits).
Key social services chapters: 208 (Social Services).

Example:
    >>> from axiom_corpus.converters.us_states.mo import MOConverter
    >>> converter = MOConverter()
    >>> section = converter.fetch_section("143.011")
    >>> print(section.section_title)
    "Resident individuals -- tax rates -- rate reductions, when"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup, NavigableString

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://revisor.mo.gov/main"

# Title mapping for reference (Roman numerals to title names)
MO_TITLES: dict[str, str] = {
    "I": "Laws in Force and Construction of Statutes",
    "II": "Sovereignty, Jurisdiction and Emblems",
    "III": "Legislative Branch",
    "IV": "Executive Branch",
    "V": "State Departments",
    "VI": "State Elective Executive Officials",
    "VII": "Elections and Voters",
    "VIII": "Judicial Department",
    "IX": "Counties, County Officers and Seats of Justice",
    "X": "Taxation and Revenue",
    "XI": "Education and Libraries",
    "XII": "Public Health and Welfare",
    "XIII": "Public Peace and Correctional Institutions",
    "XIV": "Roads and Waterways",
    "XV": "Agriculture and Animals",
    "XVI": "Conservation, Resources and Development",
    "XVII": "Credit Institutions",
    "XVIII": "Insurance",
    "XIX": "Corporations",
    "XX": "Business and Financial Institutions",
    "XXI": "Business Organizations",
    "XXII": "Occupations and Professions",
    "XXIII": "Labor and Industrial Relations",
    "XXIV": "Railroads, Warehouses and Carriers",
    "XXV": "Utilities and Carriers",
    "XXVI": "Trade and Commerce",
    "XXVII": "Amusements and Sports",
    "XXVIII": "Alcoholic Beverages",
    "XXIX": "Property",
    "XXX": "Trusts and Estates",
    "XXXI": "Actions, Process and Procedure",
    "XXXII": "Civil Procedure",
    "XXXIII": "Criminal Procedure",
    "XXXIV": "Crimes and Punishment; Peace Officers and Public Defenders",
    "XXXV": "Military and State Defense",
    "XXXVI": "Municipal Corporations",
    "XXXVII": "Special Districts",
    "XXXVIII": "Miscellaneous Provisions",
    "XXXIX": "Laws Not Assigned to Titles",
    "XL": "Statutes and Statutory Construction",
    "XLI": "Second Extraordinary Session - 96th General Assembly",
}

# Key chapters for tax analysis (Title X: Taxation and Revenue)
MO_TAX_CHAPTERS: dict[str, str] = {
    135: "Tax Relief",
    136: "State Tax Administration",
    137: "Assessment and Levy of Property Taxes",
    138: "State Tax Commission",
    139: "Collection of Current and Delinquent Taxes",
    140: "Collection of Delinquent Taxes and Tax Sales",
    141: "Limitation on Assessment and Collection",
    142: "Motor Fuel Tax",
    143: "Income Tax",
    144: "Sales and Use Tax",
    145: "Cigarette Tax",
    146: "Meat Inspection",
    147: "Corporations--Taxation",
    148: "Insurance Premium Taxes",
    149: "Motor Vehicle Sales Tax",
    150: "Estate Tax",
    151: "Property Tax Credits",
    153: "Financial Institutions Tax",
    154: "Missouri Works",
    155: "Contribution to Political Parties Tax Credit",
}

# Key chapters for social services (Title XII: Public Health and Welfare)
MO_WELFARE_CHAPTERS: dict[str, str] = {
    188: "Regulation of Abortion",
    189: "Vital Statistics",
    190: "Emergency Services",
    191: "Health",
    192: "Health and Welfare",
    193: "Death, Certification and Registration",
    194: "Donation of Bodily Organs",
    195: "Drugs, Controlled Substances and Drug Dealers",
    196: "Food, Drugs and Cosmetics",
    197: "Hospitals and Ambulatory Surgical Centers",
    198: "Care Facilities",
    199: "Communicable Diseases",
    200: "Local Public Health Services",
    201: "Governor's Advisory Council",
    205: "State Mental Health and Developmental Disabilities",
    206: "Mental Health Coordinating Commission",
    207: "Department of Social Services",
    208: "Social Services",
    209: "Commission for the Blind",
    210: "Children and Youth",
    211: "Juvenile Courts",
    212: "Delinquent, Mentally Ill, Blind or Deaf Persons",
    213: "Human Rights",
    214: "Dependent, Neglected and Delinquent Children",
    215: "Miscellaneous",
}


@dataclass
class ParsedMOSection:
    """Parsed Missouri statute section."""

    section_number: str  # e.g., "143.011"
    section_title: str  # e.g., "Resident individuals -- tax rates"
    chapter_number: int  # e.g., 143
    chapter_title: str  # e.g., "Income Tax"
    title_roman: str | None  # e.g., "X"
    title_name: str | None  # e.g., "Taxation and Revenue"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedMOSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedMOSubsection:
    """A subsection within a Missouri statute."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list["ParsedMOSubsection"] = field(default_factory=list)


class MOConverterError(Exception):
    """Error during Missouri statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class MOConverter:
    """Converter for Missouri Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = MOConverter()
        >>> section = converter.fetch_section("143.011")
        >>> print(section.citation.section)
        "MO-143.011"

        >>> for section in converter.iter_chapter(143):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Missouri statute converter.

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
            section_number: e.g., "143.011", "208.010"

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/OneSection.aspx?section={section_number}"

    def _build_chapter_url(self, chapter: int) -> str:
        """Build the URL for a chapter's contents page."""
        return f"{BASE_URL}/OneChapter.aspx?chapter={chapter}"

    def _determine_title_info(self, chapter: int) -> tuple[str | None, str | None]:
        """Determine the title Roman numeral and name from chapter number.

        Args:
            chapter: Chapter number

        Returns:
            Tuple of (title_roman, title_name)
        """
        # Title X: Taxation and Revenue (Chapters 135-155)
        if 135 <= chapter <= 155:
            return "X", "Taxation and Revenue"
        # Title XII: Public Health and Welfare (Chapters 188-215)
        elif 188 <= chapter <= 215:
            return "XII", "Public Health and Welfare"
        # Title XI: Education and Libraries (Chapters 160-187)
        elif 160 <= chapter <= 187:
            return "XI", "Education and Libraries"  # pragma: no cover
        # Title VIII: Judicial Department (Chapters 472-530)
        elif 472 <= chapter <= 530:
            return "VIII", "Judicial Department"  # pragma: no cover
        # Title XIV: Roads and Waterways (Chapters 226-238)
        elif 226 <= chapter <= 238:
            return "XIV", "Roads and Waterways"  # pragma: no cover
        else:
            return None, None

    def _parse_effective_date(self, text: str) -> date | None:
        """Parse effective date from text like 'Effective - 02 Jan 2023'.

        Args:
            text: Text potentially containing effective date

        Returns:
            Parsed date or None
        """
        # Pattern: "Effective - DD Mon YYYY"
        pattern = r"Effective\s*[-—]\s*(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})"
        match = re.search(pattern, text)
        if match:
            day = int(match.group(1))
            month_str = match.group(2)
            year = int(match.group(3))

            month_map = {
                "Jan": 1,
                "Feb": 2,
                "Mar": 3,
                "Apr": 4,
                "May": 5,
                "Jun": 6,
                "Jul": 7,
                "Aug": 8,
                "Sep": 9,
                "Oct": 10,
                "Nov": 11,
                "Dec": 12,
            }
            month = month_map.get(month_str)
            if month:
                try:
                    return date(year, month, day)
                except ValueError:  # pragma: no cover
                    pass
        return None

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedMOSection:
        """Parse section HTML into ParsedMOSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise MOConverterError(f"Section {section_number} not found", url)

        # Check for invalid section number response
        if "invalid section" in html.lower():
            raise MOConverterError(f"Section {section_number} is invalid", url)  # pragma: no cover

        chapter = int(section_number.split(".")[0])

        # Get chapter title from our registry
        chapter_title = (
            MO_TAX_CHAPTERS.get(chapter) or MO_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
        )

        # Determine title info from chapter number
        title_roman, title_name = self._determine_title_info(chapter)

        # Extract section title
        # Pattern: "143.011  Resident individuals -- tax rates -- rate reductions, when"
        section_title = ""

        # Try to find the section title in headings or strong/bold text
        # The title typically appears after the section number
        title_pattern = re.compile(rf"{re.escape(section_number)}\s+(.+?)(?:\.|$)", re.IGNORECASE)

        # First, try to find it in the page text near the top
        for elem in soup.find_all(["h1", "h2", "h3", "h4", "strong", "b"]):
            elem_text = elem.get_text(strip=True)
            match = title_pattern.search(elem_text)
            if match:
                section_title = match.group(1).strip().rstrip(".")
                break

        # If not found in headings, search in the first few text blocks
        if not section_title:
            page_text = soup.get_text()
            match = title_pattern.search(page_text)
            if match:  # pragma: no cover
                section_title = match.group(1).strip().rstrip(".")
                # Truncate if too long (title shouldn't be paragraphs)
                if len(section_title) > 200:
                    section_title = (
                        section_title[:200].rsplit(" ", 1)[0] + "..."
                    )  # pragma: no cover

        # Extract effective date
        effective_date = self._parse_effective_date(soup.get_text())

        # Find the main content area
        # Missouri uses various containers; try to find statute content
        content_elem = None
        for class_name in ["rsmo", "norm", "statute"]:
            content_elem = soup.find("div", class_=class_name)
            if content_elem:
                break

        if not content_elem:
            # Fall back to body
            content_elem = soup.find("body") or soup  # pragma: no cover

        # Remove navigation, scripts, styles
        for elem in content_elem.find_all(["nav", "script", "style", "header", "footer"]):
            elem.decompose()  # pragma: no cover

        text = content_elem.get_text(separator="\n", strip=True)
        html_content = str(content_elem)

        # Extract history note
        # Missouri format: "(RSMo 1939 ... A.L. 1943 ...)" or "History:..."
        history = None
        history_patterns = [
            r"\(RSMo\s+\d{4}[^)]+\)",
            r"History[.:][-—]?\s*(.+?)(?:\n|$)",
            r"L\.\s*\d{4}[^,)]+",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.DOTALL)
            if history_match:
                history = history_match.group(0).strip()[:1000]
                break

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedMOSection(
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
            effective_date=effective_date,
        )

    def _parse_subsections(self, text: str) -> list[ParsedMOSubsection]:
        """Parse hierarchical subsections from text.

        Missouri statutes typically use:
        - 1., 2., 3. for primary divisions
        - (1), (2), (3) for secondary divisions
        - (a), (b), (c) for tertiary divisions
        """
        subsections = []

        # Missouri uses "1." format for top-level
        # Split by top-level subsections 1., 2., etc.
        parts = re.split(r"(?=(?:^|\n)\s*(\d+)\.\s)", text, flags=re.MULTILINE)

        current_num = None
        current_content = ""

        for part in parts:
            # Check if this is a numbered identifier
            num_match = re.match(r"^(\d+)$", part.strip())
            if num_match:
                # Save previous subsection if exists
                if current_num is not None:
                    children = self._parse_level2(current_content)
                    direct_text = self._get_direct_text(current_content, children)
                    subsections.append(
                        ParsedMOSubsection(
                            identifier=current_num,
                            text=direct_text[:2000],
                            children=children,
                        )
                    )
                current_num = num_match.group(1)
                current_content = ""
            elif current_num is not None:
                current_content += part

        # Don't forget the last subsection
        if current_num is not None and current_content.strip():
            children = self._parse_level2(current_content)
            direct_text = self._get_direct_text(current_content, children)
            subsections.append(
                ParsedMOSubsection(
                    identifier=current_num,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        # If no "1." format found, try "(1)" format
        if not subsections:
            parts = re.split(r"(?=\(\d+\)\s)", text)  # pragma: no cover
            for part in parts[1:]:  # pragma: no cover
                match = re.match(r"\((\d+)\)\s*", part)  # pragma: no cover
                if not match:  # pragma: no cover
                    continue  # pragma: no cover

                identifier = match.group(1)  # pragma: no cover
                content = part[match.end() :]  # pragma: no cover

                children = self._parse_level2_parens(content)  # pragma: no cover
                direct_text = self._get_direct_text_parens(content, children)  # pragma: no cover

                subsections.append(  # pragma: no cover
                    ParsedMOSubsection(
                        identifier=identifier,
                        text=direct_text[:2000],
                        children=children,
                    )
                )

        return subsections

    def _get_direct_text(self, content: str, children: list[ParsedMOSubsection]) -> str:
        """Get text before first child subsection."""
        if children:
            # Find where first child starts
            first_child_match = re.search(r"\(\d+\)", content)
            if first_child_match:
                return content[: first_child_match.start()].strip()
        return content.strip()

    def _get_direct_text_parens(self, content: str, children: list[ParsedMOSubsection]) -> str:
        """Get text before first child subsection in parenthetical format."""
        if children:  # pragma: no cover
            first_child_match = re.search(r"\([a-z]\)", content)  # pragma: no cover
            if first_child_match:  # pragma: no cover
                return content[: first_child_match.start()].strip()  # pragma: no cover
        return content.strip()  # pragma: no cover

    def _parse_level2(self, text: str) -> list[ParsedMOSubsection]:
        """Parse level 2 subsections (1), (2), etc. from "1." numbered section."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (a), (b), etc.
            children = self._parse_level3(content)

            # Get direct text before children
            direct_text = content
            if children:
                first_child_match = re.search(r"\([a-z]\)", content)
                if first_child_match:
                    direct_text = content[: first_child_match.start()]

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", direct_text)
            if next_num:
                direct_text = direct_text[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedMOSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level2_parens(self, text: str) -> list[ParsedMOSubsection]:
        """Parse level 2 subsections (a), (b), etc. from "(1)" numbered section."""
        return self._parse_level3(text)  # pragma: no cover

    def _parse_level3(self, text: str) -> list[ParsedMOSubsection]:
        """Parse level 3 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size and stop at next subsection
            next_match = re.search(r"\([a-z0-9]\)", content)
            if next_match:
                content = content[: next_match.start()]  # pragma: no cover

            subsections.append(
                ParsedMOSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedMOSection) -> Section:
        """Convert ParsedMOSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"MO-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsection(sub: ParsedMOSubsection) -> Subsection:
            return Subsection(
                identifier=sub.identifier,
                heading=None,
                text=sub.text,
                children=[convert_subsection(child) for child in sub.children],
            )

        subsections = [convert_subsection(sub) for sub in parsed.subsections]

        return Section(
            citation=citation,
            title_name=f"Missouri Revised Statutes - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"mo/{parsed.chapter_number}/{parsed.section_number}",
            effective_date=parsed.effective_date,
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "143.011", "208.010"

        Returns:
            Section model

        Raises:
            MOConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 143)

        Returns:
            List of section numbers (e.g., ["143.011", "143.021", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Missouri links sections like: PageSelect.aspx?section=143.011
        # or directly: OneSection.aspx?section=143.011
        pattern = re.compile(rf"section=({chapter}\.\d+[A-Za-z]?)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        # Also try text-based matching for section numbers
        # Some pages list sections as plain text like "143.011"
        text_pattern = re.compile(rf"\b({chapter}\.\d{{3}}[A-Za-z]?)\b")
        for text in soup.stripped_strings:
            for match in text_pattern.finditer(text):
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)  # pragma: no cover

        return sorted(set(section_numbers))

    def iter_chapter(self, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 143)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except MOConverterError as e:  # pragma: no cover
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
            chapters = list(MO_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "MOConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_mo_section(section_number: str) -> Section:
    """Fetch a single Missouri statute section.

    Args:
        section_number: e.g., "143.011"

    Returns:
        Section model
    """
    with MOConverter() as converter:
        return converter.fetch_section(section_number)


def download_mo_chapter(chapter: int) -> list[Section]:
    """Download all sections from a Missouri Revised Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 143)

    Returns:
        List of Section objects
    """
    with MOConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_mo_tax_chapters() -> Iterator[Section]:
    """Download all sections from Missouri tax-related chapters (135-155).

    Yields:
        Section objects
    """
    with MOConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(MO_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_mo_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Missouri social services chapters (188-215).

    Yields:
        Section objects
    """
    with MOConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(MO_WELFARE_CHAPTERS.keys()))  # pragma: no cover
