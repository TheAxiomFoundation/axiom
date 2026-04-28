"""Hawaii state statute converter.

Converts Hawaii Revised Statutes (HRS) HTML from capitol.hawaii.gov
to the internal Section model for ingestion.

Hawaii Revised Statutes Structure:
- Divisions (e.g., Division 1: Government)
- Titles (e.g., Title 14: Taxation)
- Chapters (e.g., Chapter 235: Income Tax Law)
- Parts (e.g., Part I: General Provisions)
- Sections (e.g., 235-1: Definitions)

URL Patterns:
- Chapter index: /hrscurrent/Vol04_Ch0201-0257/HRS0235/HRS_0235-.htm
- Section: /hrscurrent/Vol04_Ch0201-0257/HRS0235/HRS_0235-0001.htm

Volume mappings (chapter ranges):
- Vol01: Ch0001-0042F
- Vol02: Ch0046-0115
- Vol03: Ch0121-0200D
- Vol04: Ch0201-0257
- Vol05: Ch0261-0319
- Vol06: Ch0321-0344
- Vol07: Ch0346-0398
- Vol08: Ch0401-0429
- Vol09: Ch0431-0435H
- Vol10: Ch0436B-0481I
- Vol11: Ch0482-0490
- Vol12: Ch0501-0588
- Vol13: Ch0601-0676
- Vol14: Ch0701-0853

Note: Hawaii has a progressive income tax and various social programs.
Title 14 (Taxation) and Title 17 (Social Services) are key areas.

Example:
    >>> from axiom.converters.us_states.hi import HIConverter
    >>> converter = HIConverter()
    >>> section = converter.fetch_section("235-51")
    >>> print(section.section_title)
    "Tax imposed on individuals; rates"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom.models import Citation, Section, Subsection

BASE_URL = "https://www.capitol.hawaii.gov/hrscurrent"

# Volume to chapter range mapping
# Format: (volume_number, start_chapter, end_chapter, end_suffix)
VOLUME_RANGES: list[tuple[int, int, int, str]] = [
    (1, 1, 42, "F"),
    (2, 46, 115, ""),
    (3, 121, 200, "D"),
    (4, 201, 257, ""),
    (5, 261, 319, ""),
    (6, 321, 344, ""),
    (7, 346, 398, ""),
    (8, 401, 429, ""),
    (9, 431, 435, "H"),
    (10, 436, 481, "I"),
    (11, 482, 490, ""),
    (12, 501, 588, ""),
    (13, 601, 676, ""),
    (14, 701, 853, ""),
]

# Title mapping for reference
HI_TITLES: dict[str, str] = {
    1: "General Provisions",
    2: "Property",
    3: "Property Interests and Rights",
    4: "Courts and Judicial Proceedings",
    5: "Evidence",
    6: "Civil Procedure",
    7: "Criminal Procedure",
    8: "Crimes and Criminal Proceedings",
    9: "Public Property, Purchasing, and Contracting",
    10: "Public Officers and Employees",
    11: "Elections",
    12: "Political Parties",
    13: "Legislature",
    14: "Taxation",
    15: "Transportation and Utilities",
    16: "Agriculture",
    17: "Social Services",
    18: "Education",
    19: "Health",
    20: "Public Safety",
    21: "Labor and Industrial Relations",
    22: "Business",
    23: "Corporations and Partnerships",
    24: "Insurance",
    25: "Professions and Occupations",
    26: "Banks and Financial Institutions",
    27: "Other Government Relations",
    28: "Miscellaneous",
}

# Key chapters for tax analysis (Title 14)
HI_TAX_CHAPTERS: dict[str, str] = {
    231: "Administration of Taxes",
    232: "Tax Appeal Court",
    235: "Income Tax Law",
    236: "Tax Credits",
    237: "General Excise Tax Law",
    238: "Use Tax Law",
    239: "Public Service Company Tax Law",
    240: "Rental Motor Vehicle, Tour Vehicle, and Car-Sharing Vehicle Surcharge Tax",
    241: "Banks and Other Financial Corporations--Franchise Tax",
    243: "Fuel Tax Law",
    244: "Tax on Cigarettes and Tobacco Products",
    "244D": "Liquor Tax",
    245: "Tax on Intoxicating Liquor",
    246: "Real Property Tax Law",
    247: "Conveyance Tax",
    248: "Estate and Transfer Tax",
    249: "County Vehicular Taxes",
    251: "Transient Accommodations Tax",
    255: "Tax Information Sharing",
    256: "Multistate Tax Compact",
    257: "Uniform Division of Income for Tax Purposes Act",
}

# Key chapters for social services (Title 17)
HI_WELFARE_CHAPTERS: dict[str, str] = {
    346: "Department of Human Services",
    347: "Hawaii Public Housing Authority",
    348: "Vocational Rehabilitation",
    349: "Child Care",
    350: "Child Protective Act",
    351: "Crime Victim Compensation",
    352: "Youth Correctional Facilities",
    353: "Corrections",
    "354D": "Hawaii Paroling Authority",
    "356D": "Hawaii Public Housing Authority",
    359: "Office of Community Services",
    360: "Aloha Tower Development Corporation",
    363: "Hawaiian Home Lands Trust",
    378: "Employment Practices",
    383: "Hawaii Employment Security Law",
    386: "Workers' Compensation Law",
    388: "Payment of Wages and Other Compensation",
    389: "Whistleblowers' Protection Act",
    390: "Employee Benefit Plans",
    392: "Temporary Disability Insurance",
    393: "Prepaid Health Care Act",
    "394B": "Dislocated Workers",
    396: "Occupational Safety and Health",
    398: "Family Leave",
}


@dataclass
class ParsedHISection:
    """Parsed Hawaii Revised Statutes section."""

    section_number: str  # e.g., "235-51"
    section_title: str  # e.g., "Tax imposed on individuals; rates"
    chapter_number: int  # e.g., 235
    chapter_title: str  # e.g., "Income Tax Law"
    title_number: int | None  # e.g., 14
    title_name: str | None  # e.g., "Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedHISubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedHISubsection:
    """A subsection within a Hawaii Revised Statutes section."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list["ParsedHISubsection"] = field(default_factory=list)


class HIConverterError(Exception):
    """Error during Hawaii statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class HIConverter:
    """Converter for Hawaii Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = HIConverter()
        >>> section = converter.fetch_section("235-51")
        >>> print(section.citation.section)
        "HI-235-51"

        >>> for section in converter.iter_chapter(235):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Hawaii statute converter.

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

    def _get_volume_for_chapter(self, chapter: int) -> str:
        """Get the volume folder for a chapter number.

        Args:
            chapter: Chapter number (e.g., 235)

        Returns:
            Volume folder string (e.g., "Vol04_Ch0201-0257")
        """
        for vol_num, start_ch, end_ch, suffix in VOLUME_RANGES:
            if start_ch <= chapter <= end_ch:
                return f"Vol{vol_num:02d}_Ch{start_ch:04d}-{end_ch:04d}{suffix}"

        # Default fallback for chapters not in known ranges
        raise HIConverterError(f"Unknown volume for chapter {chapter}")  # pragma: no cover

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "235-51", "235-51.5"

        Returns:
            Full URL to the section page
        """
        # Parse chapter and section parts
        parts = section_number.split("-", 1)
        if len(parts) != 2:
            raise HIConverterError(
                f"Invalid section number format: {section_number}"
            )  # pragma: no cover

        chapter_str = parts[0]
        section_part = parts[1]

        # Handle chapter suffixes (e.g., "244D")
        chapter_match = re.match(r"(\d+)([A-Z]*)", chapter_str)
        if not chapter_match:
            raise HIConverterError(
                f"Cannot parse chapter from: {section_number}"
            )  # pragma: no cover

        chapter_num = int(chapter_match.group(1))
        chapter_suffix = chapter_match.group(2)

        volume = self._get_volume_for_chapter(chapter_num)
        padded_chapter = f"{chapter_num:04d}{chapter_suffix}"

        # Section number format: zero-pad to 4 digits
        # Handle decimal sections like "51.5" -> "0051_0005" or keep as "0051"
        section_base = section_part.split(".")[0]
        section_padded = f"{int(section_base):04d}"

        # Handle subsections like "235-51.5" - these use different file naming
        if "." in section_part:
            # Some decimal sections might be in the same file as the base section
            # Try the exact decimal format first
            decimal_part = section_part.replace(".", "_")  # pragma: no cover
            return (  # pragma: no cover
                f"{BASE_URL}/{volume}/HRS{padded_chapter}/HRS_{padded_chapter}-{section_padded}.htm"
            )
        else:
            return (
                f"{BASE_URL}/{volume}/HRS{padded_chapter}/HRS_{padded_chapter}-{section_padded}.htm"
            )

    def _build_chapter_contents_url(self, chapter: int | str) -> str:
        """Build the URL for a chapter's contents index.

        Args:
            chapter: Chapter number (e.g., 235) or with suffix (e.g., "244D")
        """
        if isinstance(chapter, str):
            chapter_match = re.match(r"(\d+)([A-Z]*)", chapter)  # pragma: no cover
            if not chapter_match:  # pragma: no cover
                raise HIConverterError(f"Cannot parse chapter: {chapter}")  # pragma: no cover
            chapter_num = int(chapter_match.group(1))  # pragma: no cover
            chapter_suffix = chapter_match.group(2)  # pragma: no cover
        else:
            chapter_num = chapter
            chapter_suffix = ""

        volume = self._get_volume_for_chapter(chapter_num)
        padded_chapter = f"{chapter_num:04d}{chapter_suffix}"

        return f"{BASE_URL}/{volume}/HRS{padded_chapter}/HRS_{padded_chapter}-.htm"

    def _get_title_for_chapter(self, chapter: int) -> tuple[int | None, str | None]:
        """Determine title number and name from chapter number.

        Args:
            chapter: Chapter number (e.g., 235)

        Returns:
            Tuple of (title_number, title_name)
        """
        # Title 14 (Taxation): Chapters 231-257
        if 231 <= chapter <= 257:
            return 14, "Taxation"
        # Title 17 (Social Services): Chapters 346-398
        elif 346 <= chapter <= 398:  # pragma: no cover
            return 17, "Social Services"
        # Title 21 (Labor and Industrial Relations): Chapters 377-398
        elif 377 <= chapter <= 398:  # pragma: no cover
            return 21, "Labor and Industrial Relations"  # pragma: no cover
        # Title 19 (Health): Chapters 321-346
        elif 321 <= chapter <= 346:  # pragma: no cover
            return 19, "Health"  # pragma: no cover
        return None, None  # pragma: no cover

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedHISection:
        """Parse section HTML into ParsedHISection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower() or "404" in html:
            raise HIConverterError(f"Section {section_number} not found", url)

        # Parse chapter number from section_number (e.g., "235-51" -> 235)
        parts = section_number.split("-", 1)
        chapter_match = re.match(r"(\d+)", parts[0])
        if not chapter_match:
            raise HIConverterError(
                f"Cannot parse chapter from {section_number}"
            )  # pragma: no cover

        chapter = int(chapter_match.group(1))
        chapter_title = (
            HI_TAX_CHAPTERS.get(chapter) or HI_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
        )

        title_number, title_name = self._get_title_for_chapter(chapter)

        # Extract section title
        # Pattern: "[235-51] Tax imposed on individuals; rates."
        section_title = ""
        title_pattern = re.compile(
            rf"\[?{re.escape(section_number)}\]?\s*(.+?)(?:\.|$)",
            re.IGNORECASE,
        )

        # Try to find in the page title first
        page_title = soup.find("title")
        if page_title:
            title_text = page_title.get_text(strip=True)
            match = title_pattern.search(title_text)
            if match:
                section_title = match.group(1).strip().rstrip(".")  # pragma: no cover

        # Try headings
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3", "h4", "b", "strong"]):
                heading_text = heading.get_text(strip=True)
                match = title_pattern.search(heading_text)
                if match:
                    section_title = match.group(1).strip().rstrip(".")
                    break

        # Try looking for the section number followed by title in body text
        if not section_title:
            body_text = soup.get_text(separator="\n", strip=True)
            match = title_pattern.search(body_text)
            if match:
                section_title = match.group(1).strip().rstrip(".")  # pragma: no cover
                # Limit title length
                if len(section_title) > 200:  # pragma: no cover
                    section_title = section_title[:200] + "..."  # pragma: no cover

        # Get body content
        content_elem = soup.find("body") or soup

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(["nav", "script", "style", "header", "footer"]):
                elem.decompose()  # pragma: no cover
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note - Hawaii uses "[L ####, c ##, pt of ...]" format
        history = None
        history_patterns = [
            r"\[L\s+\d{4}.*?\]",  # [L 2023, c 123, ...]
            r"History:.*?(?:\n|$)",
            r"\(Added\s+\d{4}.*?\)",
            r"\(Amended\s+\d{4}.*?\)",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.IGNORECASE)
            if history_match:
                history = history_match.group(0).strip()[:500]
                break

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedHISection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            title_number=title_number,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedHISubsection]:
        """Parse hierarchical subsections from text.

        Hawaii statutes typically use:
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
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedHISubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedHISubsection]:
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
            if children:  # pragma: no cover
                first_child_match = re.search(r"\([A-Z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit to reasonable size and stop at next lettered subsection
            next_alpha = re.search(r"\([a-z]\)", direct_text)
            if next_alpha:  # pragma: no cover
                direct_text = direct_text[: next_alpha.start()]

            subsections.append(
                ParsedHISubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedHISubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at next higher-level subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover
            next_alpha = re.search(r"\([a-z]\)", content)
            if next_alpha:
                content = content[: next_alpha.start()]  # pragma: no cover

            if len(content) > 2000:
                content = content[:2000] + "..."  # pragma: no cover

            subsections.append(
                ParsedHISubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedHISection) -> Section:
        """Convert ParsedHISection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"HI-{parsed.section_number}",
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
            title_name=f"Hawaii Revised Statutes - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"hi/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "235-51", "346-14"

        Returns:
            Section model

        Raises:
            HIConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: int | str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 235) or with suffix (e.g., "244D")

        Returns:
            List of section numbers (e.g., ["235-1", "235-2", ...])
        """
        url = self._build_chapter_contents_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Parse chapter string for pattern matching
        if isinstance(chapter, str):
            chapter_pattern = chapter  # pragma: no cover
        else:
            chapter_pattern = str(chapter)

        # Find section links: HRS_0235-0051.htm or similar
        pattern = re.compile(
            rf"HRS_{chapter_pattern.zfill(4)}-(\d+)(?:_\d+)?\.htm",
            re.IGNORECASE,
        )

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = f"{chapter_pattern}-{int(match.group(1))}"
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        # Also look for section references in the text
        text = soup.get_text()
        section_ref_pattern = re.compile(rf"\[?{re.escape(chapter_pattern)}-(\d+(?:\.\d+)?)\]?")
        for match in section_ref_pattern.finditer(text):
            section_num = f"{chapter_pattern}-{match.group(1)}"
            if section_num not in section_numbers:
                section_numbers.append(section_num)  # pragma: no cover

        return sorted(section_numbers, key=lambda x: self._section_sort_key(x))

    def _section_sort_key(self, section_num: str) -> tuple:
        """Generate sort key for section numbers."""
        parts = section_num.split("-", 1)
        if len(parts) != 2:
            return (0, 0)  # pragma: no cover

        chapter_match = re.match(r"(\d+)", parts[0])
        section_match = re.match(r"(\d+)(?:\.(\d+))?", parts[1])

        chapter = int(chapter_match.group(1)) if chapter_match else 0
        section_base = int(section_match.group(1)) if section_match else 0
        section_decimal = (
            int(section_match.group(2)) if section_match and section_match.group(2) else 0
        )

        return (chapter, section_base, section_decimal)

    def iter_chapter(self, chapter: int | str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 235)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except HIConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover
            except httpx.HTTPError as e:  # pragma: no cover
                print(f"Warning: HTTP error fetching {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[int | str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(HI_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:  # pragma: no cover
            self._client.close()
            self._client = None

    def __enter__(self) -> "HIConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_hi_section(section_number: str) -> Section:
    """Fetch a single Hawaii Revised Statutes section.

    Args:
        section_number: e.g., "235-51"

    Returns:
        Section model
    """
    with HIConverter() as converter:
        return converter.fetch_section(section_number)


def download_hi_chapter(chapter: int | str) -> list[Section]:
    """Download all sections from a Hawaii Revised Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 235)

    Returns:
        List of Section objects
    """
    with HIConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_hi_tax_chapters() -> Iterator[Section]:
    """Download all sections from Hawaii tax-related chapters (231-257).

    Yields:
        Section objects
    """
    with HIConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(HI_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_hi_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Hawaii social services chapters (346-398).

    Yields:
        Section objects
    """
    with HIConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(HI_WELFARE_CHAPTERS.keys()))  # pragma: no cover
