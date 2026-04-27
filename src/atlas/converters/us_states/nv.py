"""Nevada state statute converter.

Converts Nevada Revised Statutes HTML from leg.state.nv.us
to the internal Section model for ingestion.

Nevada Statute Structure:
- Titles (e.g., Title 32: Revenue and Taxation)
- Chapters (e.g., Chapter 361: Property Tax)
- Sections (e.g., NRS 361.010: Definitions)

URL Patterns:
- Chapter list: https://www.leg.state.nv.us/NRS/
- Chapter page: https://www.leg.state.nv.us/NRS/NRS-361.html
- Section anchor: NRS361Sec010 (within chapter page)

HTML Structure:
- Section anchors: <a name=NRS361Sec010>
- Section number: <span class="Section">361.015</span>
- Section title: <span class="Leadline">...</span>
- Body text: <p class="SectBody">...</p>
- History: <p class="SourceNote">...</p>

Example:
    >>> from atlas.converters.us_states.nv import NVConverter
    >>> converter = NVConverter()
    >>> section = converter.fetch_section("361.010")
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

BASE_URL = "https://www.leg.state.nv.us/NRS"

# Title 32: Revenue and Taxation - chapters 360-377D
NV_TAX_CHAPTERS: dict[str, str] = {
    "360": "General Provisions",
    "360A": "Taxpayers' Bill of Rights",
    "360B": "Provisions Relating to Uniform Taxation",
    "361": "Property Tax",
    "361A": "Supersedure of Existing Constitutional Provisions",
    "362": "Mines and Mining Claims",
    "363A": "Business License Fee",
    "363B": "Bank Tax",
    "363C": "Commerce Tax",
    "363D": "Mining and Extraction Taxes",
    "364": "Surplus Lines Premiums Tax",
    "365": "Taxes on Motor Vehicle Fuel",
    "366": "Taxes on Special Fuel",
    "368A": "Liquor Tax",
    "369": "Wine",
    "370": "Cigarettes",
    "370A": "Other Tobacco Products",
    "371": "Tobacco Master Settlement Agreement",
    "372": "Sales and Use Taxes",
    "372A": "Local School Support Tax",
    "372B": "City-County Relief Tax",
    "373": "Supplemental City-County Relief Tax",
    "374": "Local Government Tax",
    "374A": "Neighborhood Housing Assistance",
    "375": "Real Property Transfer Tax",
    "375A": "Additional Tax on Transfers of Real Property",
    "375B": "Governmental Services Tax",
    "376A": "Live Entertainment Tax",
    "377": "Taxes for Support of Public Schools",
    "377A": "Taxes for Indigent Care",
    "377B": "Infrastructure Tax",
    "377C": "Tax for Improvement of Transportation",
    "377D": "Tax for Miscellaneous Local Purposes",
}

# Title 38: Public Welfare - chapters 422-432C
NV_WELFARE_CHAPTERS: dict[str, str] = {
    "422": "Health Care Financing and Policy",
    "422A": "Health Care Financing: Additional Provisions",
    "424": "Family Foster Homes",
    "425": "Welfare and Support of Children",
    "426": "Welfare: Blind Persons",
    "427A": "Welfare: Services for Aged and Disabled Persons",
    "428": "Welfare: County Responsibilities",
    "430A": "Substance Use Disorders",
    "432": "Child Welfare",
    "432A": "Facilities for Care of Children",
    "432B": "Protection of Children From Abuse and Neglect",
    "432C": "Protection of Children From Sexual Exploitation",
}


@dataclass
class ParsedNVSection:
    """Parsed Nevada statute section."""

    section_number: str  # e.g., "361.010"
    section_title: str  # e.g., "Definitions"
    chapter_number: str  # e.g., "361" or "361A"
    chapter_title: str  # e.g., "Property Tax"
    title_number: int | None  # e.g., 32
    title_name: str | None  # e.g., "Revenue and Taxation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedNVSubsection"] = field(default_factory=list)
    history: str | None = None  # Source note / history
    source_url: str = ""


@dataclass
class ParsedNVSubsection:
    """A subsection within a Nevada statute."""

    identifier: str  # e.g., "1", "a", "I"
    text: str
    children: list["ParsedNVSubsection"] = field(default_factory=list)


class NVConverterError(Exception):
    """Error during Nevada statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class NVConverter:
    """Converter for Nevada Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = NVConverter()
        >>> section = converter.fetch_section("361.010")
        >>> print(section.citation.section)
        "NV-361.010"

        >>> for section in converter.iter_chapter("361"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Nevada statute converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
            year: Statute year (default: current year)
        """
        self.rate_limit_delay = rate_limit_delay
        self.year = year or date.today().year
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None
        self._chapter_cache: dict[str, BeautifulSoup] = {}

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

    def _build_chapter_url(self, chapter: str) -> str:
        """Build the URL for a chapter page.

        Args:
            chapter: Chapter number (e.g., "361", "361A", "422")

        Returns:
            Full URL to the chapter page
        """
        return f"{BASE_URL}/NRS-{chapter}.html"

    def _get_chapter_soup(self, chapter: str) -> BeautifulSoup:
        """Get or fetch and cache the BeautifulSoup for a chapter."""
        if chapter not in self._chapter_cache:
            url = self._build_chapter_url(chapter)
            html = self._get(url)
            self._chapter_cache[chapter] = BeautifulSoup(html, "html.parser")
        return self._chapter_cache[chapter]

    def _extract_chapter_from_section(self, section_number: str) -> str:
        """Extract chapter number from section number.

        Args:
            section_number: e.g., "361.010", "361A.100", "422.270"

        Returns:
            Chapter number (e.g., "361", "361A", "422")
        """
        # Match pattern like "361" or "361A" before the dot
        match = re.match(r"^(\d+[A-Za-z]?)\.", section_number)
        if match:
            return match.group(1)
        raise ValueError(f"Cannot extract chapter from section: {section_number}")

    def _get_anchor_name(self, chapter: str, section_suffix: str) -> str:
        """Build the anchor name for a section.

        Args:
            chapter: Chapter number (e.g., "361")
            section_suffix: Section suffix after dot (e.g., "010", "0435")

        Returns:
            Anchor name (e.g., "NRS361Sec010")
        """
        # Remove letter suffix from chapter for anchor (e.g., "361A" -> "361A")
        return f"NRS{chapter}Sec{section_suffix}"

    def _parse_section_from_soup(
        self,
        soup: BeautifulSoup,
        section_number: str,
        url: str,
    ) -> ParsedNVSection:
        """Parse a section from the chapter soup.

        Nevada chapters contain all sections in one page, anchored by name attributes.
        """
        chapter = self._extract_chapter_from_section(section_number)
        section_suffix = (
            section_number.split(".", 1)[1] if "." in section_number else section_number
        )

        # Build anchor name: NRS361Sec010
        anchor_name = self._get_anchor_name(chapter, section_suffix)

        # Find the anchor element
        anchor = soup.find("a", attrs={"name": anchor_name})
        if not anchor:
            # Try without leading zeros for some older sections
            anchor_name_alt = f"NRS{chapter}Sec{section_suffix.lstrip('0') or '0'}"
            anchor = soup.find("a", attrs={"name": anchor_name_alt})

        if not anchor:
            raise NVConverterError(
                f"Section {section_number} not found (anchor {anchor_name})", url
            )

        # The anchor is inside a <p class="SectBody"> or <span>
        # Navigate to the parent paragraph
        section_start = anchor.find_parent("p")
        if not section_start:
            section_start = anchor.find_parent()  # pragma: no cover

        # Extract section title from Leadline span
        leadline = section_start.find("span", class_="Leadline") if section_start else None
        section_title = leadline.get_text(strip=True) if leadline else ""

        # Clean up title - remove surrounding quotes if present
        section_title = section_title.strip("'\"")

        # Get chapter title
        chapter_title = (
            NV_TAX_CHAPTERS.get(chapter) or NV_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
        )

        # Determine title number and name
        title_number = None
        title_name = None
        if chapter in NV_TAX_CHAPTERS or (
            chapter.rstrip("ABCD")
            and chapter.rstrip("ABCD").isdigit()
            and 360 <= int(chapter.rstrip("ABCD")) <= 377
        ):
            title_number = 32
            title_name = "Revenue and Taxation"
        elif chapter in NV_WELFARE_CHAPTERS or (
            chapter.rstrip("ABCD")
            and chapter.rstrip("ABCD").isdigit()
            and 422 <= int(chapter.rstrip("ABCD")) <= 432
        ):
            title_number = 38
            title_name = "Public Welfare"

        # Collect all content until the next section anchor or SourceNote
        content_parts = []
        html_parts = []
        current = section_start

        # First, add the initial paragraph content (after the leadline)
        if section_start:
            # Get text after the leadline span
            full_text = section_start.get_text(strip=True)
            # Remove the section number and leadline from the beginning
            # The text typically looks like: "NRS 361.010  Definitions.  Actual content..."
            # Or: "NRS 361.010  "Title" defined.  Actual content..."
            # We need to extract everything after the title/leadline

            # Find where the leadline ends - look for the leadline text followed by content
            leadline_text = section_title if section_title else ""
            if leadline_text:
                # Find where the leadline ends in the full text and get the content after it
                # The leadline is followed by the actual content
                idx = full_text.find(leadline_text)
                if idx >= 0:
                    content_after_leadline = full_text[idx + len(leadline_text) :].strip()
                    if content_after_leadline:
                        content_parts.append(content_after_leadline)
                else:
                    # Fallback: try regex pattern
                    content_match = re.search(
                        rf"NRS\s*{re.escape(section_number)}\s+.+?\.\s*(.+)$", full_text, re.DOTALL
                    )  # pragma: no cover
                    if content_match:  # pragma: no cover
                        content_parts.append(content_match.group(1))  # pragma: no cover
            else:
                # No leadline - extract content after section number
                content_match = re.search(
                    rf"NRS\s*{re.escape(section_number)}\s+(.*)$", full_text, re.DOTALL
                )  # pragma: no cover
                if content_match:  # pragma: no cover
                    content_parts.append(content_match.group(1))  # pragma: no cover

            html_parts.append(str(section_start))
            current = section_start.find_next_sibling()

        # Now iterate through siblings until we hit the next section or end
        history = None
        while current:
            # Check if this is a new section (has anchor with NRS###Sec pattern)
            if current.name == "p":
                # Check for section anchor in this paragraph
                has_section_anchor = current.find(
                    "a", attrs={"name": re.compile(r"NRS\d+[A-Za-z]?Sec")}
                )
                if has_section_anchor:
                    break  # pragma: no cover

                # Check if it's a source note (history)
                if "SourceNote" in (current.get("class") or []):
                    history = current.get_text(strip=True)
                    html_parts.append(str(current))
                    current = current.find_next_sibling()
                    break

                # Add this paragraph's content
                text = current.get_text(strip=True)
                if text:
                    content_parts.append(text)
                html_parts.append(str(current))

            current = current.find_next_sibling() if current else None

        # Join content
        full_text = "\n".join(content_parts)
        full_html = "\n".join(html_parts)

        # Parse subsections
        subsections = self._parse_subsections(full_text)

        return ParsedNVSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            title_number=title_number,
            title_name=title_name,
            text=full_text,
            html=full_html,
            subsections=subsections,
            history=history,
            source_url=f"{url}#{anchor_name}",
        )

    def _parse_subsections(self, text: str) -> list[ParsedNVSubsection]:
        """Parse hierarchical subsections from text.

        Nevada statutes typically use:
        - 1., 2., 3. for primary divisions (with period)
        - (a), (b), (c) for secondary divisions
        - (1), (2), (3) for tertiary divisions within (a), (b)
        - (I), (II), (III) for further nesting
        """
        subsections = []

        # Split by top-level numbered subsections: "1.  " pattern (with spaces after)
        # Nevada uses number followed by period and spaces
        parts = re.split(r"(?=\s+\d+\.\s{2,})", text)

        for part in parts:
            if not part.strip():
                continue

            # Match numbered subsection: "1.  content"
            match = re.match(r"\s*(\d+)\.\s{2,}(.*)$", part, re.DOTALL)
            if not match:
                continue

            identifier = match.group(1)
            content = match.group(2).strip()

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

            # Clean up text - remove next numbered subsection if present
            next_subsection = re.search(r"\s+\d+\.\s{2,}", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedNVSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedNVSubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s*)", text)

        for part in parts:
            if not part.strip():
                continue  # pragma: no cover

            match = re.match(r"\(([a-z])\)\s*(.*)$", part, re.DOTALL)
            if not match:
                continue

            identifier = match.group(1)
            content = match.group(2).strip()

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\s+\d+\.\s{2,}", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            # Parse level 3: (1), (2), etc.
            children = self._parse_level3(content)

            if children:
                first_child_match = re.search(r"\(\d+\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            subsections.append(
                ParsedNVSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedNVSubsection]:
        """Parse level 3 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s*)", text)

        for part in parts:
            if not part.strip():
                continue  # pragma: no cover

            match = re.match(r"\((\d+)\)\s*(.*)$", part, re.DOTALL)
            if not match:
                continue

            identifier = match.group(1)
            content = match.group(2).strip()

            # Stop at next alphabetic subsection
            next_alpha = re.search(r"\([a-z]\)", content)
            if next_alpha:
                content = content[: next_alpha.start()]  # pragma: no cover

            subsections.append(
                ParsedNVSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedNVSection) -> Section:
        """Convert ParsedNVSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"NV-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsections(subs: list[ParsedNVSubsection]) -> list[Subsection]:
            return [
                Subsection(
                    identifier=sub.identifier,
                    heading=None,
                    text=sub.text,
                    children=convert_subsections(sub.children),
                )
                for sub in subs
            ]

        subsections = convert_subsections(parsed.subsections)

        return Section(
            citation=citation,
            title_name=f"Nevada Revised Statutes - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"nv/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "361.010", "422.270"

        Returns:
            Section model

        Raises:
            NVConverterError: If section not found or parsing fails
        """
        chapter = self._extract_chapter_from_section(section_number)
        url = self._build_chapter_url(chapter)
        soup = self._get_chapter_soup(chapter)
        parsed = self._parse_section_from_soup(soup, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., "361", "361A")

        Returns:
            List of section numbers (e.g., ["361.010", "361.013", ...])
        """
        soup = self._get_chapter_soup(chapter)

        section_numbers = []
        # Find all section anchors: <a name=NRS361Sec010>
        pattern = re.compile(rf"NRS{re.escape(chapter)}Sec(\d+[A-Za-z]?)")

        for anchor in soup.find_all("a", attrs={"name": pattern}):
            name = anchor.get("name", "")
            match = pattern.match(name)
            if match:
                section_suffix = match.group(1)
                section_num = f"{chapter}.{section_suffix}"
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., "361")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except NVConverterError as e:  # pragma: no cover
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
            chapters = list(NV_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            # Clear cache between chapters to avoid memory bloat
            self._chapter_cache.clear()  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client and clear caches."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover
        self._chapter_cache.clear()

    def __enter__(self) -> "NVConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_nv_section(section_number: str) -> Section:
    """Fetch a single Nevada statute section.

    Args:
        section_number: e.g., "361.010"

    Returns:
        Section model
    """
    with NVConverter() as converter:
        return converter.fetch_section(section_number)


def download_nv_chapter(chapter: str) -> list[Section]:
    """Download all sections from a Nevada Revised Statutes chapter.

    Args:
        chapter: Chapter number (e.g., "361")

    Returns:
        List of Section objects
    """
    with NVConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_nv_tax_chapters() -> Iterator[Section]:
    """Download all sections from Nevada tax-related chapters (Title 32).

    Yields:
        Section objects
    """
    with NVConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(NV_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_nv_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Nevada welfare chapters (Title 38).

    Yields:
        Section objects
    """
    with NVConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(NV_WELFARE_CHAPTERS.keys()))  # pragma: no cover
