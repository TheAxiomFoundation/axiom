"""Vermont state statute converter.

Converts Vermont Statutes Annotated HTML from legislature.vermont.gov
to the internal Section model for ingestion.

Vermont Statute Structure:
- Titles (e.g., Title 32: Taxation and Finance)
- Chapters (e.g., Chapter 151: Income Taxes)
- Subchapters (e.g., Subchapter 001: Definitions; General Provisions)
- Sections (e.g., 5811: Definitions)

URL Patterns:
- Title index: /statutes/title/32
- Chapter contents: /statutes/chapter/32/151
- Section: /statutes/section/32/151/05811

Citation format: "32 V.S.A. section 5811" (Vermont Statutes Annotated)

Example:
    >>> from axiom_corpus.converters.us_states.vt import VTConverter
    >>> converter = VTConverter()
    >>> section = converter.fetch_section(32, 151, 5811)
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

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://legislature.vermont.gov/statutes"

# Title mapping for reference
VT_TITLES: dict[str, str] = {
    1: "General Provisions",
    2: "Courts and Judicial System",
    3: "Executive",
    4: "Judiciary",
    5: "Crimes",
    6: "Banks and Banking",
    7: "Corporations, Partnerships and Associations",
    8: "Insurance",
    9: "Commerce and Trade",
    10: "Conservation and Development",
    11: "Defense and Veterans Affairs",
    12: "Court Procedure",
    13: "Crimes",
    14: "Decedents' Estates and Fiduciary Relations",
    15: "Domestic Relations",
    16: "Education",
    17: "Elections",
    18: "Health",
    19: "Highways",
    20: "Internal Security and Public Safety",
    21: "Labor",
    22: "Libraries, History, Museums and Fine Arts",
    23: "Motor Vehicles",
    24: "Municipal and County Government",
    25: "Notaries Public and Records",
    26: "Professions and Occupations",
    27: "Property",
    28: "Public Institutions and Corrections",
    29: "Public Property and Buildings",
    30: "Public Service",
    31: "Special Proceedings",
    32: "Taxation and Finance",
    33: "Human Services",
}

# Key chapters for tax/benefit analysis - Title 32
VT_TAX_CHAPTERS: dict[str, str] = {
    1: "General Provisions",
    101: "Construction",
    103: "Department of Taxes; Commissioner of Taxes",
    105: "Vermont Employment Growth Incentive Program",
    121: "General Provisions",
    123: "How, Where, and to Whom Property Is Taxed",
    124: "Agricultural Lands and Forestlands",
    125: "Exemptions",
    127: "Quadrennial Appraisal of Real Estate",
    129: "Grand Tax Lists",
    131: "Appeals",
    133: "Assessment and Collection of Taxes",
    135: "Tax Sales",
    151: "Income Taxes",
    153: "Estate Taxes",
    155: "Property Transfer Taxes",
    157: "Land Gains Tax",
    181: "Meals and Rooms Taxes",
    205: "Sales and Use Tax",
    233: "Miscellaneous Tax Provisions",
    241: "Motor Fuel Tax",
}

# Key chapters for human services analysis - Title 33
VT_HUMAN_SERVICES_CHAPTERS: dict[str, str] = {
    1: "Department for Children and Families",
    3: "Agency of Human Services",
    7: "Child Support",
    9: "Office of Child Support",
    11: "Public Assistance Programs",
    19: "Medical Assistance",
    21: "Healthy Vermonters",
    35: "Long-Term Care",
    37: "Home Health Services",
    43: "Housing Assistance",
    49: "Child Welfare Services",
    51: "Child Care Services",
    52: "Juvenile Proceedings",
    55: "State Institutions",
    57: "Child Protection",
    65: "Public Assistance",
    69: "Mental Health",
    71: "Developmental Disabilities",
    75: "General Assistance",
    81: "Vermont Health Benefit Exchange",
    82: "Health Care Affordability",
}


@dataclass
class ParsedVTSection:
    """Parsed Vermont statute section."""

    title_number: int  # e.g., 32
    title_name: str  # e.g., "Taxation and Finance"
    chapter_number: int  # e.g., 151
    chapter_title: str  # e.g., "Income Taxes"
    subchapter_number: str | None  # e.g., "001"
    subchapter_title: str | None  # e.g., "DEFINITIONS; GENERAL PROVISIONS"
    section_number: str  # e.g., "5811"
    section_title: str  # e.g., "Definitions"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedVTSubsection"] = field(default_factory=list)
    history: str | None = None  # Amendment history
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedVTSubsection:
    """A subsection within a Vermont statute."""

    identifier: str  # e.g., "1", "a", "A", "i"
    text: str
    children: list["ParsedVTSubsection"] = field(default_factory=list)


class VTConverterError(Exception):
    """Error during Vermont statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class VTConverter:
    """Converter for Vermont Statutes Annotated HTML to internal Section model.

    Example:
        >>> converter = VTConverter()
        >>> section = converter.fetch_section(32, 151, 5811)
        >>> print(section.citation.section)
        "VT-32-5811"

        >>> for section in converter.iter_chapter(32, 151):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the Vermont statute converter.

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

    def _build_section_url(self, title: int, chapter: int, section: int | str) -> str:
        """Build the URL for a section.

        Args:
            title: Title number (e.g., 32)
            chapter: Chapter number (e.g., 151)
            section: Section number (e.g., 5811, "05811", or "5828b")

        Returns:
            Full URL to the section page
        """
        section_str = str(section)
        # Handle sections with letter suffixes (e.g., "5828b")
        match = re.match(r"(\d+)([a-z]*)$", section_str.lstrip("0") or "0", re.IGNORECASE)
        if match:
            num_part = int(match.group(1))
            letter_part = match.group(2).lower()
            section_padded = f"{num_part:05d}{letter_part}"
        else:
            section_padded = f"{int(section_str.lstrip('0') or 0):05d}"  # pragma: no cover
        return f"{BASE_URL}/section/{title}/{chapter}/{section_padded}"

    def _build_chapter_url(self, title: int, chapter: int) -> str:
        """Build the URL for a chapter's contents index."""
        return f"{BASE_URL}/chapter/{title}/{chapter}"

    def _build_title_url(self, title: int) -> str:
        """Build the URL for a title's chapters index."""
        return f"{BASE_URL}/title/{title}"

    def _parse_section_html(
        self,
        html: str,
        title: int,
        chapter: int,
        section: int | str,
        url: str,
    ) -> ParsedVTSection:
        """Parse section HTML into ParsedVTSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for errors
        if "not found" in html.lower() or "404" in html.lower():
            raise VTConverterError(f"Section {title} V.S.A. {section} not found", url)

        # Find main content
        main_content = soup.find("div", id="main-content")
        if not main_content:
            main_content = soup.find("div", class_="main")  # pragma: no cover
        if not main_content:
            main_content = soup  # pragma: no cover

        # Extract title info from h2.statute-title
        title_name = VT_TITLES.get(title, f"Title {title}")
        title_elem = soup.find("h2", class_="statute-title")
        if title_elem:
            caps_span = title_elem.find("span", class_="caps")
            if caps_span:
                title_name = caps_span.get_text(strip=True)

        # Extract chapter info from h3.statute-chapter
        chapter_title = f"Chapter {chapter}"
        chapter_elem = soup.find("h3", class_="statute-chapter")
        if chapter_elem:
            caps_span = chapter_elem.find("span", class_="caps")
            if caps_span:
                chapter_title = caps_span.get_text(strip=True)

        # Extract subchapter info from h4.statute-section
        subchapter_number = None
        subchapter_title = None
        subchapter_elem = soup.find("h4", class_="statute-section")
        if subchapter_elem:
            sub_text = subchapter_elem.get_text(strip=True)
            # Pattern: "Subchapter 001: DEFINITIONS; GENERAL PROVISIONS"
            match = re.search(r"Subchapter\s+(\d+)\s*:\s*(.+)", sub_text, re.IGNORECASE)
            if match:
                subchapter_number = match.group(1)  # pragma: no cover
                subchapter_title = match.group(2).strip()  # pragma: no cover
            else:
                # Try finding dirty span for number and caps span for title
                dirty_span = subchapter_elem.find("span", class_="dirty")
                caps_span = subchapter_elem.find("span", class_="caps")
                if dirty_span:
                    subchapter_number = dirty_span.get_text(strip=True)
                if caps_span:
                    subchapter_title = caps_span.get_text(strip=True)

        # Extract section title from the content
        section_title = f"Section {section}"
        # Look for pattern like "section 5811. Definitions" in li elements
        statutes_list = soup.find("ul", class_="statutes-detail")
        if statutes_list:
            li = statutes_list.find("li")
            if li:
                # Find the section heading - usually in a <b> tag
                bold = li.find("b")
                if bold:
                    bold_text = bold.get_text(strip=True)
                    # Pattern: "section 5811. Definitions" or "section 5811 Definitions"
                    match = re.search(r"§\s*\d+[a-z]*\.?\s*(.+)", bold_text, re.IGNORECASE)
                    if match:
                        section_title = match.group(1).strip()
                        # Remove trailing period if present
                        section_title = section_title.rstrip(".")
                    else:
                        # Try simpler extraction
                        parts = bold_text.split(".", 1)  # pragma: no cover
                        if len(parts) > 1:  # pragma: no cover
                            section_title = parts[1].strip()  # pragma: no cover

        # Get full text content
        text_parts = []
        if statutes_list:
            # Get all text from the statutes list
            for li in statutes_list.find_all("li"):
                for p in li.find_all("p"):
                    text_parts.append(p.get_text(strip=True))
            text = "\n".join(text_parts)
            html_content = str(statutes_list)
        else:
            text = main_content.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = str(main_content)  # pragma: no cover

        # Extract history note - typically at the end in parentheses starting with "(Added"
        history = None
        # Vermont format: "(Added 1966, No. 61 (Sp. Sess.), § 1, eff. Jan. 1, 1966; amended ...)"
        history_match = re.search(r"\(Added\s+\d+.+\)(?:\s*$)", text, re.DOTALL)
        if history_match:
            history = history_match.group(0).strip()
            # Limit length and clean up
            if len(history) > 2000:  # pragma: no cover
                history = history[:2000] + "..."

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedVTSection(
            title_number=title,
            title_name=title_name,
            chapter_number=chapter,
            chapter_title=chapter_title,
            subchapter_number=subchapter_number,
            subchapter_title=subchapter_title,
            section_number=str(section),
            section_title=section_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedVTSubsection]:
        """Parse hierarchical subsections from text.

        Vermont statutes typically use:
        - (1), (2), (3) for primary divisions
        - (A), (B), (C) for secondary divisions
        - (i), (ii), (iii) for tertiary divisions
        - (I), (II), (III) for quaternary divisions
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

            # Parse second-level children (A), (B), etc.
            children = self._parse_level2(content)

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

            # Clean up text - remove trailing subsections
            next_subsection = re.search(r"\(\d+\)", direct_text)
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedVTSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedVTSubsection]:
        """Parse level 2 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (i), (ii), etc.
            children = self._parse_level3(content)

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]

            # Get text before first child
            if children:  # pragma: no cover
                first_child_match = re.search(r"\([ivxlcdm]+\)", content, re.IGNORECASE)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            subsections.append(
                ParsedVTSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedVTSubsection]:
        """Parse level 3 subsections (i), (ii), etc."""
        subsections = []
        # Match Roman numerals
        parts = re.split(r"(?=\((?:i{1,3}|iv|vi{0,3})\)\s)", text, flags=re.IGNORECASE)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\(([ivxlcdm]+)\)\s*", part, re.IGNORECASE)
            if not match:
                continue

            identifier = match.group(1).lower()
            content = part[match.end() :]

            # Limit size
            next_match = re.search(r"\([A-Z]\)|\(\d+\)", content)
            if next_match:
                content = content[: next_match.start()]

            subsections.append(
                ParsedVTSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedVTSection) -> Section:
        """Convert ParsedVTSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"VT-{parsed.title_number}-{parsed.section_number}",
        )

        # Convert subsections
        def convert_subsection(sub: ParsedVTSubsection) -> Subsection:
            return Subsection(
                identifier=sub.identifier,
                heading=None,
                text=sub.text,
                children=[convert_subsection(c) for c in sub.children],
            )

        subsections = [convert_subsection(sub) for sub in parsed.subsections]

        return Section(
            citation=citation,
            title_name=f"Vermont Statutes Annotated - Title {parsed.title_number}: {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"vt/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, title: int, chapter: int, section: int | str) -> Section:
        """Fetch and convert a single section.

        Args:
            title: Title number (e.g., 32)
            chapter: Chapter number (e.g., 151)
            section: Section number (e.g., 5811)

        Returns:
            Section model

        Raises:
            VTConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(title, chapter, section)
        html = self._get(url)
        parsed = self._parse_section_html(html, title, chapter, section, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 32)
            chapter: Chapter number (e.g., 151)

        Returns:
            List of section numbers (e.g., ["5811", "5812", ...])
        """
        url = self._build_chapter_url(title, chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links in the statutes list
        # Pattern: /statutes/section/32/151/05811
        pattern = re.compile(rf"/statutes/section/{title}/{chapter}/(\d+[a-z]?)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1).lstrip("0") or "0"
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def get_title_chapters(self, title: int) -> list[int]:
        """Get list of chapter numbers in a title.

        Args:
            title: Title number (e.g., 32)

        Returns:
            List of chapter numbers (e.g., [1, 3, 5, ...])
        """
        url = self._build_title_url(title)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        chapter_numbers = []

        # Find chapter links
        # Pattern: /statutes/chapter/32/151
        pattern = re.compile(rf"/statutes/chapter/{title}/(\d+)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                chapter_num = int(match.group(1))
                if chapter_num not in chapter_numbers:
                    chapter_numbers.append(chapter_num)

        return sorted(chapter_numbers)

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 32)
            chapter: Chapter number (e.g., 151)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(title, chapter, section_num)
            except VTConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(
                    f"Warning: Could not fetch {title} V.S.A. {section_num}: {e}"
                )  # pragma: no cover
                continue  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 32)

        Yields:
            Section objects for each section
        """
        chapters = self.get_title_chapters(title)  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def iter_tax_chapters(self) -> Iterator[Section]:
        """Iterate over sections from key tax chapters in Title 32.

        Yields:
            Section objects from tax-related chapters
        """
        for chapter in VT_TAX_CHAPTERS:  # pragma: no cover
            yield from self.iter_chapter(32, chapter)  # pragma: no cover

    def iter_human_services_chapters(self) -> Iterator[Section]:
        """Iterate over sections from key human services chapters in Title 33.

        Yields:
            Section objects from human services chapters
        """
        for chapter in VT_HUMAN_SERVICES_CHAPTERS:  # pragma: no cover
            yield from self.iter_chapter(33, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "VTConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_vt_section(title: int, chapter: int, section: int | str) -> Section:
    """Fetch a single Vermont statute section.

    Args:
        title: Title number (e.g., 32)
        chapter: Chapter number (e.g., 151)
        section: Section number (e.g., 5811)

    Returns:
        Section model
    """
    with VTConverter() as converter:
        return converter.fetch_section(title, chapter, section)


def download_vt_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a Vermont Statutes chapter.

    Args:
        title: Title number (e.g., 32)
        chapter: Chapter number (e.g., 151)

    Returns:
        List of Section objects
    """
    with VTConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_vt_tax_chapters() -> Iterator[Section]:
    """Download all sections from Vermont tax-related chapters (Title 32).

    Yields:
        Section objects
    """
    with VTConverter() as converter:  # pragma: no cover
        yield from converter.iter_tax_chapters()  # pragma: no cover


def download_vt_human_services_chapters() -> Iterator[Section]:
    """Download all sections from Vermont human services chapters (Title 33).

    Yields:
        Section objects
    """
    with VTConverter() as converter:  # pragma: no cover
        yield from converter.iter_human_services_chapters()  # pragma: no cover
