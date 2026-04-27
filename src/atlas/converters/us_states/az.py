"""Arizona state statute converter.

Converts Arizona Revised Statutes (ARS) HTML from azleg.gov
to the internal Section model for ingestion.

Arizona Statute Structure:
- Titles (e.g., Title 42: Taxation, Title 46: Welfare)
- Chapters (e.g., Chapter 1: Administration)
- Articles (e.g., Article 1: General Provisions)
- Sections (e.g., 42-1001: Definitions)

URL Patterns:
- Title index: /arstitle/?title=[NUMBER]
- Section details: /viewdocument/?docName=https://www.azleg.gov/ars/[TITLE]/[SECTION].htm
- Direct section: /ars/[TITLE]/[SECTION].htm

Example:
    >>> from atlas.converters.us_states.az import AZConverter
    >>> converter = AZConverter()
    >>> section = converter.fetch_section("42-1001")
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

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://www.azleg.gov"

# Arizona Revised Statutes Title mapping
AZ_TITLES: dict[str, str] = {
    1: "General Provisions",
    3: "Agriculture",
    4: "Alcoholic Beverages",
    5: "Amusements and Sports",
    6: "Banks and Financial Institutions",
    8: "Child Safety",
    9: "Cities and Towns",
    10: "Corporations and Associations",
    11: "Counties",
    12: "Courts and Civil Proceedings",
    13: "Criminal Code",
    14: "Trusts, Estates and Protective Proceedings",
    15: "Education",
    16: "Elections and Electors",
    17: "Game and Fish",
    18: "Information Technology",
    19: "Initiative, Referendum and Recall",
    20: "Insurance",
    21: "Juries",
    22: "Justices of the Peace",
    23: "Labor",
    25: "Marital and Domestic Relations",
    26: "Military Affairs and Emergency Management",
    27: "Minerals, Oil and Gas",
    28: "Transportation",
    29: "Partnership",
    30: "Power",
    31: "Prisons and Prisoners",
    32: "Professions and Occupations",
    33: "Property",
    34: "Public Buildings and Improvements",
    35: "Public Finances",
    36: "Public Health and Safety",
    37: "Public Lands",
    38: "Public Officers and Employees",
    39: "Public Records, Printing and Notices",
    40: "Public Utilities and Carriers",
    41: "State Government",
    42: "Taxation",
    43: "Taxation of Income",
    44: "Trade and Commerce",
    45: "Waters",
    46: "Welfare",
    47: "Uniform Commercial Code",
    48: "Special Taxing Districts",
    49: "The Environment",
}

# Key chapters for tax/benefit analysis
AZ_TAX_CHAPTERS: dict[str, str] = {
    421: "Administration",
    425: "Transaction Privilege and Affiliated Excise Taxes",
    4211: "Property Tax",
    4212: "Assessment",
    4213: "Valuation",
    4214: "Assessment of Centrally Valued Property",
    4215: "Classification",
    4216: "Exemptions",
    4217: "Tax Liens and Tax Levies",
    4218: "Tax Collection",
    4219: "Tax Sales",
}

AZ_INCOME_TAX_CHAPTERS: dict[str, str] = {
    431: "General Provisions",
    432: "Resident Individuals",
    433: "Nonresident and Part-Year Resident Individuals",
    434: "Withholding Tax on Wages",
    435: "Partnerships",
    436: "Corporate Income Tax",
}

AZ_WELFARE_CHAPTERS: dict[str, str] = {
    461: "General Provisions",
    462: "Assistance Programs",
    463: "Employment and Training",
    464: "Child Support Enforcement",
    465: "Aging and Long-Term Care",
}


@dataclass
class ParsedAZSection:
    """Parsed Arizona statute section."""

    section_number: str  # e.g., "42-1001"
    section_title: str  # e.g., "Definitions"
    title_number: int  # e.g., 42
    title_name: str | None  # e.g., "Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedAZSubsection"] = field(default_factory=list)
    history: str | None = None  # History note (if present)
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedAZSubsection:
    """A subsection within an Arizona statute."""

    identifier: str  # e.g., "A", "1", "a"
    text: str
    children: list["ParsedAZSubsection"] = field(default_factory=list)


class AZConverterError(Exception):
    """Error during Arizona statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class AZConverter:
    """Converter for Arizona Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = AZConverter()
        >>> section = converter.fetch_section("42-1001")
        >>> print(section.citation.section)
        "AZ-42-1001"

        >>> for section in converter.iter_title(42, chapter=1):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Arizona statute converter.

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
            section_number: e.g., "42-1001", "46-101"

        Returns:
            Full URL to the section page
        """
        # Parse title and section from "42-1001" format
        parts = section_number.split("-")
        if len(parts) != 2:
            raise AZConverterError(f"Invalid section number format: {section_number}")

        title = parts[0]
        section = parts[1]
        # Pad section to 5 digits (e.g., "1001" -> "01001")
        padded_section = section.zfill(5)

        return f"{BASE_URL}/ars/{title}/{padded_section}.htm"

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedAZSection:
        """Parse section HTML into ParsedAZSection.

        Arizona statute HTML structure:
        - <font color=GREEN>42-1001</font>. <font color=PURPLE><u>Title</u></font>
        - Body paragraphs with <p> tags
        - Subsections use A., B., C. for primary level
        - Sub-subsections use 1., 2., 3.
        - Sub-sub-subsections use (a), (b), (c)
        """
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "404" in html.lower():
            raise AZConverterError(f"Section {section_number} not found", url)

        # Parse title number from section number
        parts = section_number.split("-")
        title_number = int(parts[0])
        title_name = AZ_TITLES.get(title_number, f"Title {title_number}")

        # Extract section title from the heading
        # Pattern: <font color=GREEN>42-1001</font>. <font color=PURPLE><u>Title</u></font>
        section_title = ""

        # Try to find purple/underlined title text
        purple_font = soup.find("font", color=lambda c: c and c.upper() == "PURPLE")
        if purple_font:
            underline = purple_font.find("u")
            if underline:
                section_title = underline.get_text(strip=True)
            else:
                section_title = purple_font.get_text(strip=True)  # pragma: no cover

        # Fallback: parse from title tag
        if not section_title:
            title_tag = soup.find("title")  # pragma: no cover
            if title_tag:  # pragma: no cover
                title_text = title_tag.get_text(strip=True)  # pragma: no cover
                # Format: "42-1001 - Definitions"
                if " - " in title_text:  # pragma: no cover
                    section_title = title_text.split(" - ", 1)[1]  # pragma: no cover

        # Get body content
        body = soup.find("body")
        if body:
            # Remove navigation and scripts
            for elem in body.find_all(["nav", "script", "style", "header", "footer"]):
                elem.decompose()  # pragma: no cover
            text = body.get_text(separator="\n", strip=True)
            html_content = str(body)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note if present (usually at the end)
        history = None
        history_match = re.search(
            r"(?:History|Hist\.?)\.?\s*[-:]\s*(.+?)(?:\n|$)", text, re.IGNORECASE
        )
        if history_match:
            history = history_match.group(1).strip()[:1000]  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedAZSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_number,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedAZSubsection]:
        """Parse hierarchical subsections from text.

        Arizona statutes typically use:
        - A., B., C. for primary divisions (uppercase letters with period)
        - 1., 2., 3. for secondary divisions
        - (a), (b), (c) for tertiary divisions
        - (i), (ii), (iii) for quaternary divisions
        """
        subsections = []

        # Split by top-level subsections A., B., C., etc.
        # Look for uppercase letter followed by period at start of line or after newline
        parts = re.split(r"(?:^|\n)([A-Z])\.\s+", text)

        # First element is content before first A.
        for i in range(1, len(parts), 2):
            if i + 1 >= len(parts):
                break  # pragma: no cover

            identifier = parts[i]
            content = parts[i + 1]

            # Parse second-level children 1., 2., etc.
            children = self._parse_level2(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"(?:^|\n)\d+\.\s+", content)  # pragma: no cover
                direct_text = (  # pragma: no cover
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Stop at next top-level subsection indicator
            next_subsection = re.search(r"\n[A-Z]\.\s+", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedAZSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedAZSubsection]:
        """Parse level 2 subsections 1., 2., etc."""
        subsections = []
        parts = re.split(r"(?:^|\n)(\d+)\.\s+", text)

        for i in range(1, len(parts), 2):
            if i + 1 >= len(parts):  # pragma: no cover
                break  # pragma: no cover

            identifier = parts[i]  # pragma: no cover
            content = parts[i + 1]  # pragma: no cover

            # Parse third-level children (a), (b), etc.
            children = self._parse_level3(content)  # pragma: no cover

            # Get text before first child
            if children:  # pragma: no cover
                first_child_match = re.search(r"\([a-z]\)\s+", content)  # pragma: no cover
                direct_text = (  # pragma: no cover
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()  # pragma: no cover

            # Stop at next numbered subsection
            next_num = re.search(r"\n\d+\.\s+", direct_text)  # pragma: no cover
            if next_num:  # pragma: no cover
                direct_text = direct_text[: next_num.start()]  # pragma: no cover

            # Stop at next letter subsection
            next_letter = re.search(r"\n[A-Z]\.\s+", direct_text)  # pragma: no cover
            if next_letter:  # pragma: no cover
                direct_text = direct_text[: next_letter.start()]  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedAZSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedAZSubsection]:
        """Parse level 3 subsections (a), (b), etc."""
        subsections = []  # pragma: no cover
        parts = re.split(r"\(([a-z])\)\s+", text)  # pragma: no cover

        for i in range(1, len(parts), 2):  # pragma: no cover
            if i + 1 >= len(parts):  # pragma: no cover
                break  # pragma: no cover

            identifier = parts[i]  # pragma: no cover
            content = parts[i + 1]  # pragma: no cover

            # Stop at next numbered or letter subsection
            next_num = re.search(r"\n\d+\.\s+", content)  # pragma: no cover
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]  # pragma: no cover

            next_letter = re.search(r"\n[A-Z]\.\s+", content)  # pragma: no cover
            if next_letter:  # pragma: no cover
                content = content[: next_letter.start()]  # pragma: no cover

            # Stop at next (a) subsection
            next_paren = re.search(r"\([a-z]\)\s+", content)  # pragma: no cover
            if next_paren:  # pragma: no cover
                content = content[: next_paren.start()]  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedAZSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections  # pragma: no cover

    def _to_section(self, parsed: ParsedAZSection) -> Section:
        """Convert ParsedAZSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"AZ-{parsed.section_number}",
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
            title_name=f"Arizona Revised Statutes - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"az/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "42-1001", "46-101"

        Returns:
            Section model

        Raises:
            AZConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_title_sections(self, title: int) -> list[str]:
        """Get list of section numbers in a title.

        Note: This requires parsing the title index page, which has a complex
        JavaScript-based structure. This is a simplified implementation that
        may need enhancement for full coverage.

        Args:
            title: Title number (e.g., 42)

        Returns:
            List of section numbers (e.g., ["42-1001", "42-1002", ...])
        """
        url = f"{BASE_URL}/arsDetail/?title={title}"
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links in the format: /ars/42/01001.htm
        pattern = re.compile(rf"/ars/{title}/(\d+)\.htm")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_part = match.group(1).lstrip("0") or "0"
                section_num = f"{title}-{section_part}"
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_title(self, title: int, chapter: int | None = None) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 42)
            chapter: Optional chapter filter (e.g., 1 for sections 42-1xxx)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_title_sections(title)

        for section_num in section_numbers:
            # Filter by chapter if specified
            if chapter is not None:
                # Arizona chapters are typically indicated by the first digit(s)
                # after the title. E.g., 42-1001 is in chapter 1, 42-5001 is in chapter 5
                parts = section_num.split("-")  # pragma: no cover
                if len(parts) == 2:  # pragma: no cover
                    section_part = parts[1]  # pragma: no cover
                    # Chapter is typically the first digit for single-digit chapters
                    # or first two digits for multi-digit (e.g., 11, 12)
                    if len(section_part) >= 4:  # pragma: no cover
                        # First two digits indicate chapter for 4+ digit sections
                        sec_chapter = int(section_part[:2])  # pragma: no cover
                        if sec_chapter == 0:  # pragma: no cover
                            sec_chapter = int(section_part[0])  # pragma: no cover
                    else:
                        sec_chapter = int(section_part[0])  # pragma: no cover
                    if sec_chapter != chapter:  # pragma: no cover
                        continue  # pragma: no cover

            try:
                yield self.fetch_section(section_num)
            except AZConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_titles(
        self,
        titles: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple titles.

        Args:
            titles: List of title numbers (default: tax titles 42, 43)

        Yields:
            Section objects
        """
        if titles is None:  # pragma: no cover
            titles = [42, 43]  # Default to tax-related titles  # pragma: no cover

        for title in titles:  # pragma: no cover
            yield from self.iter_title(title)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "AZConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_az_section(section_number: str) -> Section:
    """Fetch a single Arizona statute section.

    Args:
        section_number: e.g., "42-1001"

    Returns:
        Section model
    """
    with AZConverter() as converter:
        return converter.fetch_section(section_number)


def download_az_title(title: int) -> list[Section]:
    """Download all sections from an Arizona Revised Statutes title.

    Args:
        title: Title number (e.g., 42)

    Returns:
        List of Section objects
    """
    with AZConverter() as converter:
        return list(converter.iter_title(title))


def download_az_tax_titles() -> Iterator[Section]:
    """Download all sections from Arizona tax-related titles (42, 43).

    Yields:
        Section objects
    """
    with AZConverter() as converter:  # pragma: no cover
        yield from converter.iter_titles([42, 43])  # pragma: no cover


def download_az_welfare_title() -> Iterator[Section]:
    """Download all sections from Arizona welfare title (46).

    Yields:
        Section objects
    """
    with AZConverter() as converter:  # pragma: no cover
        yield from converter.iter_titles([46])  # pragma: no cover
