"""Kentucky Revised Statutes converter.

Converts Kentucky Revised Statutes PDFs from apps.legislature.ky.gov to the internal
Section model for ingestion.

Kentucky Revised Statutes Structure:
- Titles (e.g., Title XI: Revenue and Taxation)
- Chapters (e.g., Chapter 141: Income Taxes)
- Sections (e.g., 141.010: Definitions)

URL Patterns:
- Chapter index: /law/statutes/chapter.aspx?id={chapter_id}
- Section (PDF): /law/statutes/statute.aspx?id={section_id}

Note: Kentucky uses numeric IDs rather than section numbers in URLs. The chapter
page lists all sections with their IDs, which are used to fetch individual PDFs.

Example:
    >>> from atlas.converters.us_states.ky import KYConverter
    >>> converter = KYConverter()
    >>> section = converter.fetch_section("141.010")
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

from atlas.fetchers.pdf_extractor import PDFTextExtractor
from atlas.models import Citation, Section, Subsection

BASE_URL = "https://apps.legislature.ky.gov/law/statutes"

# Chapter ID mapping - Kentucky uses numeric IDs in URLs
# These are the chapter.aspx?id= values for each KRS chapter
KY_CHAPTER_IDS: dict[int, int] = {
    # Revenue and Taxation (Title XI, Chapters 131-143)
    131: 37623,  # Department of Revenue
    132: 37624,  # Levy, Assessment and Collection of State Taxes
    133: 37625,  # Agricultural Products
    134: 37626,  # Collection and Payment of Taxes
    135: 37627,  # Tax Credits
    136: 37628,  # Corporation and Utility Taxes
    137: 37629,  # Property Tax
    138: 37630,  # Gasoline and Special Fuels Taxes
    139: 37631,  # Sales and Use Taxes
    140: 37673,  # Inheritance and Estate Taxes
    141: 37674,  # Income Taxes
    142: 37675,  # Severance and Processing Taxes
    143: 37676,  # Unmined Coal Tax
    # Public Assistance (Title XVII, Chapter 205)
    205: 38043,  # Public Assistance and Welfare Programs
}

# Title mapping for reference
KY_TITLES: dict[str, str] = {
    "XI": "Revenue and Taxation",
    "XVII": "Public Assistance",
}

# Key chapters for tax/benefit analysis
KY_TAX_CHAPTERS: dict[str, str] = {
    131: "Department of Revenue",
    132: "Levy, Assessment and Collection of State Taxes",
    133: "Agricultural Products",
    134: "Collection and Payment of Taxes",
    135: "Tax Credits",
    136: "Corporation and Utility Taxes",
    137: "Property Tax",
    138: "Gasoline and Special Fuels Taxes",
    139: "Sales and Use Taxes",
    140: "Inheritance and Estate Taxes",
    141: "Income Taxes",
    142: "Severance and Processing Taxes",
    143: "Unmined Coal Tax",
}

KY_WELFARE_CHAPTERS: dict[str, str] = {
    205: "Public Assistance and Welfare Programs",
}


@dataclass
class ParsedKYSection:
    """Parsed Kentucky Revised Statutes section."""

    section_number: str  # e.g., "141.010"
    section_title: str  # e.g., "Definitions"
    chapter_number: int  # e.g., 141
    chapter_title: str  # e.g., "Income Taxes"
    title_roman: str | None  # e.g., "XI"
    title_name: str | None  # e.g., "Revenue and Taxation"
    text: str  # Full text content
    subsections: list["ParsedKYSubsection"] = field(default_factory=list)
    history: str | None = None  # History note / amendments
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedKYSubsection:
    """A subsection within a Kentucky Revised Statutes section."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list["ParsedKYSubsection"] = field(default_factory=list)


class KYConverterError(Exception):
    """Error during Kentucky Revised Statutes conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class KYConverter:
    """Converter for Kentucky Revised Statutes PDFs to internal Section model.

    Kentucky statutes are served as PDFs from apps.legislature.ky.gov. This converter:
    1. Fetches chapter HTML to get section-to-ID mappings
    2. Fetches individual section PDFs
    3. Extracts text using PyMuPDF
    4. Parses the text into structured Section models

    Example:
        >>> converter = KYConverter()
        >>> section = converter.fetch_section("141.010")
        >>> print(section.citation.section)
        "KY-141.010"

        >>> for section in converter.iter_chapter(141):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Kentucky Revised Statutes converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
            year: Statute year (default: current year)
        """
        self.rate_limit_delay = rate_limit_delay
        self.year = year or date.today().year
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None
        self._section_id_cache: dict[str, int] = {}
        self._pdf_extractor = PDFTextExtractor()

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

    def _get(self, url: str) -> bytes:
        """Make a rate-limited GET request returning bytes."""
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.content

    def _get_text(self, url: str) -> str:  # pragma: no cover
        """Make a rate-limited GET request returning text."""
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    def _build_chapter_url(self, chapter: int) -> str:
        """Build the URL for a chapter's contents index.

        Args:
            chapter: Chapter number (e.g., 141)

        Returns:
            Full URL to the chapter page

        Raises:
            KYConverterError: If chapter ID is not known
        """
        chapter_id = KY_CHAPTER_IDS.get(chapter)
        if not chapter_id:
            raise KYConverterError(f"Unknown chapter ID for chapter {chapter}")
        return f"{BASE_URL}/chapter.aspx?id={chapter_id}"

    def _build_section_url(self, section_id: int) -> str:
        """Build the URL for a section PDF.

        Args:
            section_id: The numeric ID for the section

        Returns:
            Full URL to the section PDF
        """
        return f"{BASE_URL}/statute.aspx?id={section_id}"

    def _get_title_for_chapter(self, chapter: int) -> tuple[str | None, str | None]:
        """Determine title number and name from chapter number.

        Args:
            chapter: Chapter number (e.g., 141)

        Returns:
            Tuple of (title_roman, title_name)
        """
        if 131 <= chapter <= 143:
            return "XI", "Revenue and Taxation"
        elif chapter == 205:
            return "XVII", "Public Assistance"
        return None, None

    def _parse_effective_date(self, text: str) -> date | None:
        """Parse effective date from text like 'Effective: June 27, 2019'.

        Args:
            text: Text containing effective date

        Returns:
            Parsed date or None
        """
        match = re.search(r"Effective[:\s]+(\w+\s+\d{1,2},?\s*\d{4})", text, re.IGNORECASE)
        if match:
            date_str = match.group(1).replace(",", "")
            try:
                from datetime import datetime

                return datetime.strptime(date_str, "%B %d %Y").date()
            except ValueError:  # pragma: no cover
                pass
        return None  # pragma: no cover

    def _get_section_id(self, section_number: str) -> int:
        """Get the numeric ID for a section number.

        Args:
            section_number: e.g., "141.010"

        Returns:
            Numeric section ID

        Raises:
            KYConverterError: If section not found
        """
        if section_number in self._section_id_cache:
            return self._section_id_cache[section_number]

        # Parse chapter from section number
        chapter = int(section_number.split(".")[0])

        # Fetch and parse chapter page to get section IDs
        self._load_chapter_section_ids(chapter)

        if section_number not in self._section_id_cache:
            raise KYConverterError(f"Section {section_number} not found in chapter {chapter}")

        return self._section_id_cache[section_number]

    def _load_chapter_section_ids(self, chapter: int) -> None:
        """Load section IDs from a chapter page into cache.

        Args:
            chapter: Chapter number (e.g., 141)
        """
        url = self._build_chapter_url(chapter)
        html = self._get_text(url)
        soup = BeautifulSoup(html, "html.parser")

        # Find section links: statute.aspx?id=12345
        # Links have text like ".010 Definitions" or "141.010 Definitions"
        pattern = re.compile(r"statute\.aspx\?id=(\d+)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if not match:
                continue  # pragma: no cover

            section_id = int(match.group(1))
            link_text = link.get_text(strip=True)

            # Parse section number from link text
            # Formats: ".010 Title" or "141.010 Title" or just ".010"
            section_match = re.match(r"\.?(\d{3}[A-Za-z]?)\s*", link_text)
            if section_match:
                section_suffix = section_match.group(1)
                section_number = f"{chapter}.{section_suffix}"
                self._section_id_cache[section_number] = section_id

    def _parse_section_pdf(
        self,
        pdf_content: bytes,
        section_number: str,
        url: str,
    ) -> ParsedKYSection:
        """Parse section PDF into ParsedKYSection.

        Args:
            pdf_content: PDF file bytes
            section_number: Section number (e.g., "141.010")
            url: Source URL

        Returns:
            ParsedKYSection with extracted data
        """
        # Extract text from PDF
        text = self._pdf_extractor.extract_text(pdf_content)

        if not text or len(text.strip()) < 50:
            raise KYConverterError(f"PDF appears empty for section {section_number}", url)

        chapter = int(section_number.split(".")[0])
        chapter_title = (
            KY_TAX_CHAPTERS.get(chapter) or KY_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
        )

        title_roman, title_name = self._get_title_for_chapter(chapter)

        # Extract section title from the text
        # Format: "141.010 Definitions" or "141.010 Definitions for KRS Chapter 141."
        section_title = ""
        title_pattern = re.compile(rf"{re.escape(section_number)}\s+([^\n.]+?)(?:\s*\.|\s*\n|$)")
        match = title_pattern.search(text)
        if match:
            section_title = match.group(1).strip()
            # Clean up "for KRS Chapter XXX" suffix
            section_title = re.sub(r"\s+for KRS\s+.*$", "", section_title, flags=re.IGNORECASE)

        # Try alternate patterns
        if not section_title:
            # Look for section number followed by text
            alt_pattern = re.compile(rf"{re.escape(section_number)}\s+(.+?)(?:\n|$)")
            match = alt_pattern.search(text)
            if match:
                section_title = match.group(1).strip()[:100]  # pragma: no cover

        # Extract effective date
        effective_date = self._parse_effective_date(text)

        # Extract history note (usually at end, starts with "History:")
        history = None
        history_match = re.search(r"History[:\s]+(.+?)(?:\n\n|$)", text, re.DOTALL)
        if history_match:
            history = history_match.group(1).strip()[:500]

        # Also look for "Effective:" as history
        if not history:
            eff_match = re.search(r"(Effective[:\s]+.+?)(?:\n\n|$)", text)  # pragma: no cover
            if eff_match:  # pragma: no cover
                history = eff_match.group(1).strip()[:500]  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedKYSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            title_roman=title_roman,
            title_name=title_name,
            text=text,
            subsections=subsections,
            history=history,
            source_url=url,
            effective_date=effective_date,
        )

    def _parse_subsections(self, text: str) -> list[ParsedKYSubsection]:
        """Parse hierarchical subsections from text.

        Kentucky Revised Statutes typically use:
        - (1), (2), (3) for primary divisions
        - (a), (b), (c) for secondary divisions
        - 1., 2., 3. for tertiary (sometimes)
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
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedKYSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedKYSubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse third-level children 1., 2., etc.
            children = self._parse_level3(content)

            # Get text before first child or next subsection
            if children:  # pragma: no cover
                first_child_match = re.search(r"\d+\.\s", content)
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
                ParsedKYSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedKYSubsection]:
        """Parse level 3 subsections 1., 2., etc."""
        subsections = []
        # Match "1. " at start of line or after whitespace
        parts = re.split(r"(?=(?:^|\s)(\d+)\.\s)", text)

        # The split pattern captures the digit, so we need to handle pairs
        i = 1
        while i < len(parts):  # pragma: no cover
            if i + 1 < len(parts):
                identifier = parts[i]
                content = parts[i + 1]
            else:
                break  # pragma: no cover

            # Stop at next higher-level subsection
            next_alpha = re.search(r"\([a-z]\)", content)
            if next_alpha:
                content = content[: next_alpha.start()]  # pragma: no cover
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            if len(content) > 2000:
                content = content[:2000] + "..."  # pragma: no cover

            if content.strip():
                subsections.append(
                    ParsedKYSubsection(
                        identifier=identifier,
                        text=content.strip(),
                        children=[],
                    )
                )

            i += 2

        return subsections

    def _to_section(self, parsed: ParsedKYSection) -> Section:
        """Convert ParsedKYSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"KY-{parsed.section_number}",
        )

        # Convert subsections
        def convert_subsections(subs: list[ParsedKYSubsection]) -> list[Subsection]:
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
            title_name=f"Kentucky Revised Statutes - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"ky/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "141.010", "205.010"

        Returns:
            Section model

        Raises:
            KYConverterError: If section not found or parsing fails
        """
        section_id = self._get_section_id(section_number)
        url = self._build_section_url(section_id)
        pdf_content = self._get(url)
        parsed = self._parse_section_pdf(pdf_content, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 141)

        Returns:
            List of section numbers (e.g., ["141.010", "141.020", ...])
        """
        # Load section IDs if not cached
        self._load_chapter_section_ids(chapter)

        # Filter cached sections for this chapter
        prefix = f"{chapter}."
        section_numbers = [s for s in self._section_id_cache.keys() if s.startswith(prefix)]

        # Sort by section number
        def sort_key(s: str) -> tuple[int, str]:
            parts = s.split(".")
            num = int(parts[1].rstrip("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"))
            suffix = re.sub(r"^\d+", "", parts[1])
            return (num, suffix)

        return sorted(section_numbers, key=sort_key)

    def iter_chapter(self, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 141)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except KYConverterError as e:  # pragma: no cover
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
            chapters = list(KY_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "KYConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ky_section(section_number: str) -> Section:
    """Fetch a single Kentucky Revised Statutes section.

    Args:
        section_number: e.g., "141.010"

    Returns:
        Section model
    """
    with KYConverter() as converter:
        return converter.fetch_section(section_number)


def download_ky_chapter(chapter: int) -> list[Section]:
    """Download all sections from a Kentucky Revised Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 141)

    Returns:
        List of Section objects
    """
    with KYConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_ky_tax_chapters() -> Iterator[Section]:
    """Download all sections from Kentucky tax-related chapters (131-143).

    Yields:
        Section objects
    """
    with KYConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(KY_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_ky_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Kentucky public assistance chapters (205).

    Yields:
        Section objects
    """
    with KYConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(KY_WELFARE_CHAPTERS.keys()))  # pragma: no cover
