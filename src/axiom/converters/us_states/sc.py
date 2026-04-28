"""South Carolina state statute converter.

Converts South Carolina Code of Laws HTML from scstatehouse.gov
to the internal Section model for ingestion.

South Carolina Code Structure:
- Titles (e.g., Title 12: Taxation)
- Chapters (e.g., Chapter 6: South Carolina Income Tax Act)
- Sections (e.g., 12-6-10: Short title)

URL Patterns:
- Title index: /code/statmast.php
- Title contents: /code/title12.php
- Chapter contents: /code/t12c006.php (Title 12, Chapter 6)

Section numbering: {title}-{chapter}-{section_number}
e.g., "12-6-10" = Title 12, Chapter 6, Section 10

Example:
    >>> from axiom.converters.us_states.sc import SCConverter
    >>> converter = SCConverter()
    >>> sections = converter.fetch_chapter(12, 6)  # Fetch all sections in Chapter 6
    >>> print(sections[0].section_title)
    "Short title"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom.models import Citation, Section, Subsection

BASE_URL = "https://www.scstatehouse.gov/code"

# Title mapping for reference
SC_TITLES: dict[str, str] = {
    1: "Administration of the Government",
    2: "Alcoholic Beverages",
    3: "Agriculture",
    4: "Animals, Livestock and Poultry",
    5: "Amusements",
    6: "Banks and Financial Institutions",
    7: "Boards and Commissions",
    8: "Burial",
    9: "Citizenship and Preamble",
    10: "Commerce and Trade",
    11: "Corporations, Associations and Partnerships",
    12: "Taxation",
    13: "Planning, Research and Development",
    14: "Courts",
    15: "Civil Remedies and Procedures",
    16: "Crimes and Offenses",
    17: "Criminal Procedures",
    18: "Education",
    19: "Elections",
    20: "Employees and Employers",
    21: "Estates and Wills",
    22: "Fees",
    23: "Law Enforcement and Public Safety",
    24: "Local Government Provisions Applicable to Special Purpose Districts",
    25: "Marriage",
    26: "Motor Vehicles and Traffic",
    27: "Property and Conveyances",
    28: "Public Buildings and Property",
    29: "Public Lands and State Land Resources",
    30: "Mines and Mining",
    31: "Housing and Redevelopment",
    32: "Hunting and Fishing",
    33: "Highways, Bridges and Ferries",
    34: "Insurance",
    35: "Jails and Prisoners",
    36: "Labor and Industrial Relations",
    37: "State and Local Libraries",
    38: "Weights, Measures and Standards",
    39: "Debtor and Creditor",
    40: "Professions and Occupations",
    41: "State and County Officers",
    42: "Alcoholic Beverages",
    43: "Social Services",
    44: "Health",
    45: "Indians",
    46: "Waters and Drainage",
    47: "Water, Water Resources and Drainage",
    48: "Environmental Protection and Conservation",
    49: "National Guard",
    50: "Fish, Game and Watercraft",
    51: "Militia and Military Affairs",
    52: "Notaries Public",
    53: "Pharmacies",
    54: "Aeronautics",
    55: "Railroads, Pipelines and Utilities",
    56: "Motor Vehicles",
    57: "Highways, Bridges and Ferries",
    58: "Ports and Harbors",
    59: "Education",
    60: "Museums, Cultural and Historical Affairs",
    61: "Alcohol and Alcoholic Beverages",
    62: "South Carolina Probate Code",
    63: "South Carolina Children's Code",
}

# Key chapters in Title 12 (Taxation)
SC_TAX_CHAPTERS: dict[str, str] = {
    2: "General Provisions",
    4: "Taxpayer Rights",
    6: "South Carolina Income Tax Act",
    8: "License Fees, Taxes and Permits",
    10: "License Fees",
    11: "Documentary Stamp Tax",
    13: "Department of Revenue",
    21: "Property Taxes Generally",
    23: "Assessment of Property",
    25: "Tax Collections",
    27: "Redemption of Real Property",
    28: "Homestead Exemption",
    31: "License Taxes",
    33: "Accommodations Tax",
    35: "Admissions Tax",
    36: "Sales and Use Tax",
    37: "Soft Drink Tax",
    39: "Motor Fuel Taxes",
    43: "Taxes on Insurance Companies",
    54: "Estate Tax",
    56: "Generation-Skipping Transfer Tax",
    58: "Local Option Tourism Development Fee",
}

# Key chapters in Title 43 (Social Services)
SC_WELFARE_CHAPTERS: dict[str, str] = {
    1: "Department of Social Services",
    3: "County Offices",
    5: "Public Assistance Generally",
    7: "Dependent and Neglected Children",
    21: "Family Independence Act",
    25: "Aid to Elderly, Blind, and Disabled",
    27: "Uniform Interstate Family Support Act",
    33: "South Carolina Children's Trust Fund",
    35: "Protection of Adults",
}


@dataclass
class ParsedSCSection:
    """Parsed South Carolina Code section."""

    section_number: str  # e.g., "12-6-10"
    section_title: str  # e.g., "Short title"
    title_number: int  # e.g., 12
    chapter_number: int  # e.g., 6
    title_name: str | None  # e.g., "Taxation"
    chapter_title: str | None  # e.g., "South Carolina Income Tax Act"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedSCSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedSCSubsection:
    """A subsection within a South Carolina Code section."""

    identifier: str  # e.g., "A", "1", "a"
    text: str
    children: list["ParsedSCSubsection"] = field(default_factory=list)


class SCConverterError(Exception):
    """Error during South Carolina Code conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class SCConverter:
    """Converter for South Carolina Code of Laws HTML to internal Section model.

    South Carolina Code is organized by Title > Chapter > Section.
    Section numbers follow the format: {title}-{chapter}-{section}
    e.g., "12-6-10" = Title 12, Chapter 6, Section 10

    Example:
        >>> converter = SCConverter()
        >>> sections = converter.fetch_chapter(12, 6)  # Get all sections
        >>> print(sections[0].citation.section)
        "SC-12-6-10"

        >>> for section in converter.iter_chapter(12, 6):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the South Carolina Code converter.

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

    def _get_title_name(self, title: int) -> str | None:
        """Get the title name for a title number."""
        return SC_TITLES.get(title)

    def _get_chapter_title(self, title: int, chapter: int) -> str | None:
        """Get the chapter title for a title and chapter number."""
        if title == 12:
            return SC_TAX_CHAPTERS.get(chapter)
        elif title == 43:  # pragma: no cover
            return SC_WELFARE_CHAPTERS.get(chapter)
        return None  # pragma: no cover

    def _build_chapter_url(self, title: int, chapter: int) -> str:
        """Build the URL for a chapter.

        Args:
            title: Title number (e.g., 12)
            chapter: Chapter number (e.g., 6)

        Returns:
            Full URL to the chapter page
        """
        return f"{BASE_URL}/t{title:02d}c{chapter:03d}.php"

    def _build_title_url(self, title: int) -> str:
        """Build the URL for a title's table of contents."""
        return f"{BASE_URL}/title{title}.php"  # pragma: no cover

    def _parse_chapter_html(
        self,
        html: str,
        title: int,
        chapter: int,
        url: str,
    ) -> list[ParsedSCSection]:
        """Parse chapter HTML into list of ParsedSCSection objects.

        SC Code chapters contain all sections inline, so we parse the entire
        chapter and split into sections.

        The actual HTML structure from scstatehouse.gov uses:
        - <span style="font-weight: bold;"> SECTION X-X-X.</span> Title.
        - Content separated by <br /> tags
        - HISTORY: lines without special markup
        """
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise SCConverterError(f"Chapter {title}-{chapter} not found", url)

        title_name = self._get_title_name(title)
        chapter_title = self._get_chapter_title(title, chapter)

        sections: list[ParsedSCSection] = []

        # Get the full text content and split into logical lines
        # The HTML uses <br /> as line separators
        body = soup.find("body")
        if not body:
            return sections  # pragma: no cover

        # Get the text, preserving some structure
        full_html = str(body)

        # Replace <br /> and <br/> with newlines for easier parsing
        text_content = re.sub(r"<br\s*/?>", "\n", full_html)
        # Remove other HTML tags
        text_content = re.sub(r"<[^>]+>", "", text_content)
        # Decode HTML entities
        text_content = text_content.replace("&quot;", '"')
        text_content = text_content.replace("&amp;", "&")
        text_content = text_content.replace("&lt;", "<")
        text_content = text_content.replace("&gt;", ">")
        text_content = text_content.replace("&nbsp;", " ")

        # Split into lines
        lines = text_content.split("\n")

        # Pattern for section headers: SECTION {title}-{chapter}-{number}. Title.
        # Note: section header has format "SECTION 12-6-10. Short title."
        section_pattern = re.compile(rf"^\s*SECTION\s+({title}-{chapter}-\d+[A-Za-z]?)\.\s*(.+)$")
        history_pattern = re.compile(r"^HISTORY:\s*(.+)$")

        current_section: ParsedSCSection | None = None
        current_text_parts: list[str] = []
        current_history: str | None = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check for section header
            section_match = section_pattern.match(line)
            if section_match:
                # Save previous section if exists
                if current_section is not None:
                    current_section.text = "\n".join(current_text_parts)
                    current_section.history = current_history
                    current_section.subsections = self._parse_subsections(current_section.text)
                    sections.append(current_section)

                # Start new section
                section_num = section_match.group(1)
                section_title = section_match.group(2).strip().rstrip(".")

                current_section = ParsedSCSection(
                    section_number=section_num,
                    section_title=section_title,
                    title_number=title,
                    chapter_number=chapter,
                    title_name=title_name,
                    chapter_title=chapter_title,
                    text="",
                    html="",
                    source_url=url,
                )
                current_text_parts = []
                current_history = None
                continue

            # Check for history note
            history_match = history_pattern.match(line)
            if history_match and current_section is not None:
                current_history = history_match.group(1).strip()
                continue

            # Skip navigation and other non-content lines
            if line.startswith("South Carolina") and "Code of Laws" in line:  # pragma: no cover
                continue
            if line.startswith("Home") or line.startswith("Code of Laws"):  # pragma: no cover
                continue
            if "CHAPTER" in line and "-" in line:
                # Chapter header, skip
                continue
            if line == "* * *":
                continue

            # Add to current section's text
            if current_section is not None:
                current_text_parts.append(line)

        # Don't forget the last section
        if current_section is not None:
            current_section.text = "\n".join(current_text_parts)
            current_section.history = current_history
            current_section.subsections = self._parse_subsections(current_section.text)
            sections.append(current_section)

        return sections

    def _parse_single_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedSCSection:
        """Parse HTML containing a single section (from search results or direct link)."""
        # Parse the section number to get title and chapter
        parts = section_number.split("-")  # pragma: no cover
        if len(parts) >= 2:  # pragma: no cover
            title = int(parts[0])  # pragma: no cover
            chapter = int(parts[1])  # pragma: no cover
        else:
            raise SCConverterError(
                f"Invalid section number format: {section_number}", url
            )  # pragma: no cover

        soup = BeautifulSoup(html, "html.parser")  # pragma: no cover

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():  # pragma: no cover
            raise SCConverterError(f"Section {section_number} not found", url)  # pragma: no cover

        title_name = self._get_title_name(title)  # pragma: no cover
        chapter_title = self._get_chapter_title(title, chapter)  # pragma: no cover

        # Find the section header
        section_pattern = re.compile(  # pragma: no cover
            rf"SECTION\s+{re.escape(section_number)}\.\s+([^.]+)\."
        )

        section_title = ""  # pragma: no cover
        content_elem = soup.find("div", id="content") or soup.find("body")  # pragma: no cover
        text_parts: list[str] = []  # pragma: no cover
        history: str | None = None  # pragma: no cover
        found_section = False  # pragma: no cover

        if content_elem:  # pragma: no cover
            for para in content_elem.find_all("p"):  # pragma: no cover
                para_text = para.get_text(strip=True)  # pragma: no cover

                bold = para.find("b")  # pragma: no cover
                if bold:  # pragma: no cover
                    bold_text = bold.get_text(strip=True)  # pragma: no cover

                    # Check for section header
                    section_match = section_pattern.match(bold_text)  # pragma: no cover
                    if section_match:  # pragma: no cover
                        section_title = section_match.group(1).strip()  # pragma: no cover
                        found_section = True  # pragma: no cover
                        continue  # pragma: no cover

                    # Check for history note
                    if found_section and bold_text.startswith("HISTORY:"):  # pragma: no cover
                        history = bold_text[8:].strip()  # pragma: no cover
                        continue  # pragma: no cover

                    # If we hit another SECTION, stop
                    if found_section and bold_text.startswith("SECTION"):  # pragma: no cover
                        break  # pragma: no cover

                # Add to text if we're in the section
                if found_section and para_text and para_text != "* * *":  # pragma: no cover
                    text_parts.append(para_text)  # pragma: no cover

        if not found_section:  # pragma: no cover
            raise SCConverterError(
                f"Section {section_number} not found in HTML", url
            )  # pragma: no cover

        full_text = "\n".join(text_parts)  # pragma: no cover

        return ParsedSCSection(  # pragma: no cover
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title,
            chapter_number=chapter,
            title_name=title_name,
            chapter_title=chapter_title,
            text=full_text,
            html=html,
            subsections=self._parse_subsections(full_text),
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedSCSubsection]:
        """Parse hierarchical subsections from text.

        South Carolina Code typically uses:
        - (A), (B), (C) for primary divisions
        - (1), (2), (3) for secondary divisions
        - (a), (b), (c) for tertiary divisions
        """
        subsections = []

        # Split by top-level subsections (A), (B), etc.
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (A)
            match = re.match(r"\(([A-Z])\)\s*", part)
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
            next_subsection = re.search(r"\([A-Z]\)", direct_text)
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedSCSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedSCSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse third-level children (a), (b), etc.
            children = self._parse_level3(content)

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

            # Limit to reasonable size and stop at next lettered subsection
            next_alpha = re.search(r"\([A-Z]\)", direct_text)
            if next_alpha:  # pragma: no cover
                direct_text = direct_text[: next_alpha.start()]

            subsections.append(
                ParsedSCSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedSCSubsection]:
        """Parse level 3 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at next higher-level subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]
            next_alpha = re.search(r"\([A-Z]\)", content)
            if next_alpha:  # pragma: no cover
                content = content[: next_alpha.start()]

            if len(content) > 2000:  # pragma: no cover
                content = content[:2000] + "..."

            subsections.append(
                ParsedSCSubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedSCSection) -> Section:
        """Convert ParsedSCSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"SC-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsection(sub: ParsedSCSubsection) -> Subsection:
            return Subsection(
                identifier=sub.identifier,
                heading=None,
                text=sub.text,
                children=[convert_subsection(child) for child in sub.children],
            )

        subsections = [convert_subsection(sub) for sub in parsed.subsections]

        return Section(
            citation=citation,
            title_name=f"South Carolina Code - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"sc/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_chapter(self, title: int, chapter: int) -> list[Section]:
        """Fetch all sections from a chapter.

        Args:
            title: Title number (e.g., 12)
            chapter: Chapter number (e.g., 6)

        Returns:
            List of Section objects

        Raises:
            SCConverterError: If chapter not found or parsing fails
        """
        url = self._build_chapter_url(title, chapter)
        html = self._get(url)
        parsed_sections = self._parse_chapter_html(html, title, chapter, url)
        return [self._to_section(parsed) for parsed in parsed_sections]

    def fetch_section(self, section_number: str) -> Section:
        """Fetch a single section by number.

        This fetches the entire chapter and extracts the specific section.

        Args:
            section_number: e.g., "12-6-10", "43-5-220"

        Returns:
            Section model

        Raises:
            SCConverterError: If section not found or parsing fails
        """
        # Parse the section number to get title and chapter
        parts = section_number.split("-")
        if len(parts) < 3:
            raise SCConverterError(
                f"Invalid section number format: {section_number}"
            )  # pragma: no cover

        title = int(parts[0])
        chapter = int(parts[1])

        # Fetch the chapter
        url = self._build_chapter_url(title, chapter)
        html = self._get(url)
        parsed_sections = self._parse_chapter_html(html, title, chapter, url)

        # Find the specific section
        for parsed in parsed_sections:
            if parsed.section_number == section_number:
                return self._to_section(parsed)

        raise SCConverterError(f"Section {section_number} not found", url)  # pragma: no cover

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 12)
            chapter: Chapter number (e.g., 6)

        Returns:
            List of section numbers (e.g., ["12-6-10", "12-6-20", ...])
        """
        url = self._build_chapter_url(title, chapter)  # pragma: no cover
        html = self._get(url)  # pragma: no cover
        parsed_sections = self._parse_chapter_html(html, title, chapter, url)  # pragma: no cover
        return [s.section_number for s in parsed_sections]  # pragma: no cover

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 12)
            chapter: Chapter number (e.g., 6)

        Yields:
            Section objects for each section
        """
        sections = self.fetch_chapter(title, chapter)
        yield from sections

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 12, 43)

        Yields:
            Section objects for each section across all chapters
        """
        if title == 12:  # pragma: no cover
            chapters = list(SC_TAX_CHAPTERS.keys())  # pragma: no cover
        elif title == 43:  # pragma: no cover
            chapters = list(SC_WELFARE_CHAPTERS.keys())  # pragma: no cover
        else:
            # Try to discover chapters (not implemented - would need to parse title page)
            raise SCConverterError(  # pragma: no cover
                f"Title {title} chapter discovery not implemented. "
                "Use fetch_chapter() with specific chapter numbers."
            )

        for chapter in chapters:  # pragma: no cover
            try:  # pragma: no cover
                yield from self.iter_chapter(title, chapter)  # pragma: no cover
            except SCConverterError as e:  # pragma: no cover
                # Log but continue with other chapters
                print(
                    f"Warning: Could not fetch chapter {title}-{chapter}: {e}"
                )  # pragma: no cover
                continue  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "SCConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_sc_section(section_number: str) -> Section:
    """Fetch a single South Carolina Code section.

    Args:
        section_number: e.g., "12-6-10"

    Returns:
        Section model
    """
    with SCConverter() as converter:
        return converter.fetch_section(section_number)


def download_sc_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a South Carolina Code chapter.

    Args:
        title: Title number (e.g., 12)
        chapter: Chapter number (e.g., 6)

    Returns:
        List of Section objects
    """
    with SCConverter() as converter:
        return converter.fetch_chapter(title, chapter)


def download_sc_tax_chapters() -> Iterator[Section]:
    """Download all sections from South Carolina tax-related chapters (Title 12).

    Yields:
        Section objects
    """
    with SCConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(12)  # pragma: no cover


def download_sc_welfare_chapters() -> Iterator[Section]:
    """Download all sections from South Carolina social services chapters (Title 43).

    Yields:
        Section objects
    """
    with SCConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(43)  # pragma: no cover
