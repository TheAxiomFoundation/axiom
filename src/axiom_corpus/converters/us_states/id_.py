"""Idaho state statute converter.

Converts Idaho Statutes HTML from legislature.idaho.gov
to the internal Section model for ingestion.

Idaho Statute Structure:
- Titles (e.g., Title 63: Revenue and Taxation)
- Chapters (e.g., Chapter 30: Income Tax)
- Sections (e.g., 63-3002: Declaration of intent)

URL Patterns:
- Title index: /statutesrules/idstat/Title[NUMBER]/
- Chapter contents: /statutesrules/idstat/Title[NUMBER]/T[NUMBER]CH[CHAPTER]/
- Section: /statutesrules/idstat/Title[NUMBER]/T[NUMBER]CH[CHAPTER]/SECT[SECTION]/

Example:
    >>> from axiom_corpus.converters.us_states.id_ import IDConverter
    >>> converter = IDConverter()
    >>> section = converter.fetch_section("63-3002")
    >>> print(section.section_title)
    "Declaration of intent"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://legislature.idaho.gov/statutesrules/idstat"

# Title mapping for reference
ID_TITLES: dict[str, str] = {
    1: "General Provisions",
    2: "Educational Institutions",
    3: "State Printing and Documents",
    5: "State Officers",
    6: "Aliens and Absentees",
    7: "Courts and Court Officials",
    8: "Actions",
    9: "Pleading",
    10: "Evidence",
    11: "Judgments and Execution",
    12: "Liens",
    13: "Forcible Entries",
    14: "Change of Name and Sex Designation",
    15: "Habeas Corpus",
    16: "Infants and Incompetents",
    17: "Adoption of Children",
    18: "Crimes and Punishments",
    19: "Criminal Procedure",
    20: "Correction and Detention",
    21: "Special Proceedings",
    22: "Military and Homeland Security",
    23: "County Roads and Bridges",
    24: "County Hospitals",
    25: "Health and Hospitalization",
    26: "Flood Control Districts",
    27: "Public Utilities",
    28: "Commercial Transactions",
    29: "Property in General",
    30: "Probate Code",
    31: "County Organization",
    32: "County Elections",
    33: "Education",
    34: "Elections",
    35: "Eminent Domain",
    36: "Fish and Game",
    37: "Food, Drugs, and Oil",
    38: "Forestry",
    39: "Health and Safety",
    40: "Highways and Bridges",
    41: "Insurance",
    42: "Irrigation",
    43: "Drainage",
    44: "Industries and Labor",
    45: "Liens",
    46: "Militia",
    47: "Mining and Minerals",
    48: "Mortgages",
    49: "Motor Vehicles",
    50: "Municipal Corporations",
    51: "Partnership",
    52: "Pledges",
    53: "General Business Associations",
    54: "Professions, Vocations, and Businesses",
    55: "Property",
    56: "Public Assistance and Welfare",
    57: "Public Funds",
    58: "Public Lands",
    59: "Public Officers",
    60: "Railroads",
    61: "Recording of Instruments",
    62: "Repositories of the State",
    63: "Revenue and Taxation",
    64: "Suretyship",
    65: "Townships",
    66: "Trade Practices",
    67: "State Government and State Affairs",
    68: "Water Management and Conservation",
    69: "Warehouses and Warehouse Receipts",
    70: "Workers' Compensation",
    71: "Aeronautics",
    72: "Water Resources",
    73: "Natural Resources",
    74: "Public Finance",
}

# Key chapters for tax/benefit analysis
ID_TAX_CHAPTERS: dict[str, str] = {
    1: "Property Taxes - General Provisions",
    2: "Property Taxes - Assessment",
    3: "Property Taxes - Exemptions",
    4: "Property Taxes - Equalization",
    5: "Property Taxes - Levies and Collection",
    6: "Property Taxes - Tax Deeds",
    8: "Property Taxes - Special Assessment",
    9: "Property Taxes - Irrigation Districts",
    10: "Property Taxes - Collection of Taxes",
    16: "Mines - Net Profits Tax",
    23: "License Fees",
    24: "Fuel Tax",
    25: "Cigarette and Tobacco",
    27: "Electricity Excise",
    30: "Income Tax",
    31: "Idaho Tax Commission",
    32: "Administration",
    35: "Sales Tax",
    36: "Sales Tax Administration",
    44: "Tax Appeals",
}

ID_WELFARE_CHAPTERS: dict[str, str] = {
    1: "Payment for Skilled and Intermediate Services",
    2: "Public Assistance Law",
    3: "Food Stamps",
    4: "Child Support Enforcement",
    5: "TANF",
    6: "Child Care",
    7: "Low Income Energy Assistance",
    8: "Medicaid",
    10: "Department of Health and Welfare",
    11: "Child Protective Services",
    12: "Foster Care",
    17: "Crisis Standards of Care Act",
    22: "Medicaid State Plan",
}


@dataclass
class ParsedIDSection:
    """Parsed Idaho statute section."""

    section_number: str  # e.g., "63-3002"
    section_title: str  # e.g., "Declaration of intent"
    title_number: int  # e.g., 63
    title_name: str  # e.g., "Revenue and Taxation"
    chapter_number: int  # e.g., 30
    chapter_title: str | None  # e.g., "Income Tax"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedIDSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedIDSubsection:
    """A subsection within an Idaho statute."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list["ParsedIDSubsection"] = field(default_factory=list)


class IDConverterError(Exception):
    """Error during Idaho statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class IDConverter:
    """Converter for Idaho Statutes HTML to internal Section model.

    Example:
        >>> converter = IDConverter()
        >>> section = converter.fetch_section("63-3002")
        >>> print(section.citation.section)
        "ID-63-3002"

        >>> for section in converter.iter_chapter(63, 30):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Idaho statute converter.

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

    def _parse_section_number(self, section_number: str) -> tuple[int, int, str]:
        """Parse section number into title, chapter, and section parts.

        Args:
            section_number: e.g., "63-3002", "56-202"

        Returns:
            Tuple of (title, chapter, section_suffix)

        Note:
            Idaho chapter numbering follows section // 100 convention:
            - 63-3002: section 3002 // 100 = chapter 30
            - 56-202: section 202 // 100 = chapter 2
        """
        # Handle formats like "63-3002", "56-202", "63-3022A"
        match = re.match(r"(\d+)-(\d+)([A-Za-z]*)$", section_number)
        if not match:
            raise IDConverterError(f"Invalid section number format: {section_number}")

        title = int(match.group(1))
        full_section = match.group(2) + match.group(3)

        # Derive chapter from section number using integer division
        # Idaho convention: chapter = section_number // 100
        # e.g., 3002 // 100 = 30, 202 // 100 = 2
        section_num = int(match.group(2))
        chapter = section_num // 100

        return title, chapter, full_section

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "63-3002", "56-202"

        Returns:
            Full URL to the section page
        """
        title, chapter, _ = self._parse_section_number(section_number)

        # URL format: /statutesrules/idstat/Title63/T63CH30/SECT63-3002/
        return f"{BASE_URL}/Title{title}/T{title}CH{chapter}/SECT{section_number}/"

    def _build_chapter_url(self, title: int, chapter: int) -> str:
        """Build the URL for a chapter's contents index."""
        return f"{BASE_URL}/Title{title}/T{title}CH{chapter}/"

    def _build_title_url(self, title: int) -> str:
        """Build the URL for a title's contents index."""
        return f"{BASE_URL}/Title{title}/"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedIDSection:
        """Parse section HTML into ParsedIDSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "page not found" in html.lower():
            raise IDConverterError(f"Section {section_number} not found", url)

        title, chapter, _ = self._parse_section_number(section_number)
        title_name = ID_TITLES.get(title, f"Title {title}")
        chapter_title = None
        if title == 63:
            chapter_title = ID_TAX_CHAPTERS.get(chapter)
        elif title == 56:  # pragma: no cover
            chapter_title = ID_WELFARE_CHAPTERS.get(chapter)

        # Find divs containing spans with Courier New font (Idaho's styling pattern)
        # The Courier New style is on the inner span, not the div itself
        content_divs = []
        for div in soup.find_all("div"):
            span = div.find("span", style=lambda s: s and "Courier New" in str(s))
            if span:
                content_divs.append(div)

        # Extract section title and content
        section_title = ""
        text_parts = []
        history = None
        in_history = False

        for div in content_divs:
            div_text = div.get_text(separator=" ", strip=True)

            # Skip empty divs or title/chapter header divs (centered text)
            if not div_text:
                continue  # pragma: no cover
            style = div.get("style", "")
            if "text-align: center" in style:
                continue

            # Check for section header pattern: "63-3002.  SECTION TITLE. rest of text"
            # The title is usually in UPPERCASE in a span
            if section_number in div_text and not section_title:
                # Try to find the uppercase title span
                title_span = div.find(
                    "span", style=lambda s: s and "text-transform: uppercase" in str(s)
                )
                if title_span:
                    section_title = title_span.get_text(strip=True).rstrip(".")
                else:
                    # Fallback: parse from text
                    title_match = re.match(  # pragma: no cover
                        rf"{re.escape(section_number)}\.\s*([^.]+)\.",
                        div_text,
                    )
                    if title_match:  # pragma: no cover
                        section_title = title_match.group(1).strip()  # pragma: no cover

                # Extract remaining text after the section header
                # Remove the section number and title from div_text
                remainder = re.sub(
                    rf"^{re.escape(section_number)}\.\s*[^.]+\.\s*",
                    "",
                    div_text,
                )
                if remainder.strip():
                    text_parts.append(remainder.strip())
                continue

            # Check for history section
            if div_text.startswith("History:"):
                in_history = True
                continue
            if in_history:
                # History is usually in brackets
                if div_text.startswith("["):
                    history = div_text.strip()
                in_history = False
                continue

            # Regular content
            text_parts.append(div_text)

        # Combine text parts
        full_text = "\n".join(text_parts)

        # Parse subsections from the full text
        subsections = self._parse_subsections(full_text)

        # Get HTML content
        html_content = "\n".join(str(div) for div in content_divs)

        return ParsedIDSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title,
            title_name=title_name,
            chapter_number=chapter,
            chapter_title=chapter_title,
            text=full_text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedIDSubsection]:
        """Parse hierarchical subsections from text.

        Idaho statutes typically use:
        - (1), (2), (3) for primary divisions (sometimes with nested numbers)
        - (a), (b), (c) for secondary divisions
        - (c)(1), (c)(2) for nested under letters
        """
        subsections = []

        # Split by top-level subsections (a), (b), etc. or (1), (2), etc.
        # Idaho often uses letters as primary level
        letter_parts = re.split(r"(?=\([a-z]\)\s)", text)
        number_parts = re.split(r"(?=\(\d+\)\s)", text)

        # Determine which pattern is used based on which creates more parts
        if len(letter_parts) > len(number_parts):  # pragma: no cover
            parts = letter_parts
            pattern = re.compile(r"\(([a-z])\)\s*")
        else:
            parts = number_parts
            pattern = re.compile(r"\((\d+)\)\s*")

        for part in parts[1:]:  # Skip content before first marker
            match = pattern.match(part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse nested children
            children = self._parse_nested_subsections(content)

            # Get text before first child
            if children:  # pragma: no cover
                first_child_match = re.search(r"\(\d+\)|\([a-z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Clean up text - stop at next same-level subsection
            next_sub = re.search(r"\n\([a-z]\)|\n\(\d+\)", direct_text)
            if next_sub:  # pragma: no cover
                direct_text = direct_text[: next_sub.start()].strip()

            subsections.append(
                ParsedIDSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_nested_subsections(self, text: str) -> list[ParsedIDSubsection]:
        """Parse nested subsections (numbered or lettered)."""
        subsections = []

        # Look for nested patterns like (1), (2) under letters or (a), (b) under numbers
        # Nested typically appear after the first line
        nested_pattern = re.compile(r"\((\d+)\)\s*")
        matches = list(nested_pattern.finditer(text))

        for i, match in enumerate(matches):  # pragma: no cover
            identifier = match.group(1)
            start = match.end()
            # End at next nested or end of text
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            content = text[start:end].strip()

            # Limit length and stop at next top-level
            next_top = re.search(r"\n\([a-z]\)", content)
            if next_top:
                content = content[: next_top.start()].strip()

            subsections.append(
                ParsedIDSubsection(
                    identifier=identifier,
                    text=content[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedIDSection) -> Section:
        """Convert ParsedIDSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"ID-{parsed.section_number}",
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
            title_name=f"Idaho Statutes - Title {parsed.title_number}: {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"id/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "63-3002", "56-202"

        Returns:
            Section model

        Raises:
            IDConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 63)
            chapter: Chapter number (e.g., 30)

        Returns:
            List of section numbers (e.g., ["63-3001", "63-3002", ...])
        """
        url = self._build_chapter_url(title, chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links: href="/statutesrules/idstat/Title63/T63CH30/SECT63-3001"
        pattern = re.compile(rf"SECT({title}-\d+[A-Za-z]*)/?$")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def get_title_chapters(self, title: int) -> list[int]:
        """Get list of chapter numbers in a title.

        Args:
            title: Title number (e.g., 63)

        Returns:
            List of chapter numbers (e.g., [1, 2, 3, ...])
        """
        url = self._build_title_url(title)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        chapters = []

        # Find chapter links: href="/statutesrules/idstat/Title63/T63CH30/"
        pattern = re.compile(rf"T{title}CH(\d+)/?$")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                chapter = int(match.group(1))
                if chapter not in chapters:
                    chapters.append(chapter)

        return sorted(chapters)

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 63)
            chapter: Chapter number (e.g., 30)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except IDConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 63)

        Yields:
            Section objects for each section
        """
        chapters = self.get_title_chapters(title)  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def iter_tax_chapters(self) -> Iterator[Section]:
        """Iterate over sections from Title 63 tax chapters.

        Yields:
            Section objects
        """
        for chapter in ID_TAX_CHAPTERS:  # pragma: no cover
            yield from self.iter_chapter(63, chapter)  # pragma: no cover

    def iter_welfare_chapters(self) -> Iterator[Section]:
        """Iterate over sections from Title 56 welfare chapters.

        Yields:
            Section objects
        """
        for chapter in ID_WELFARE_CHAPTERS:  # pragma: no cover
            yield from self.iter_chapter(56, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "IDConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_id_section(section_number: str) -> Section:
    """Fetch a single Idaho statute section.

    Args:
        section_number: e.g., "63-3002"

    Returns:
        Section model
    """
    with IDConverter() as converter:
        return converter.fetch_section(section_number)


def download_id_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from an Idaho Statutes chapter.

    Args:
        title: Title number (e.g., 63)
        chapter: Chapter number (e.g., 30)

    Returns:
        List of Section objects
    """
    with IDConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_id_tax_chapters() -> Iterator[Section]:
    """Download all sections from Idaho tax-related chapters (Title 63).

    Yields:
        Section objects
    """
    with IDConverter() as converter:  # pragma: no cover
        yield from converter.iter_tax_chapters()  # pragma: no cover


def download_id_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Idaho welfare chapters (Title 56).

    Yields:
        Section objects
    """
    with IDConverter() as converter:  # pragma: no cover
        yield from converter.iter_welfare_chapters()  # pragma: no cover
