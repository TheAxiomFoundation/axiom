"""Minnesota state statute converter.

Converts Minnesota Statutes HTML from revisor.mn.gov to the internal Section model
for ingestion.

Minnesota Statute Structure:
- Chapters (e.g., Chapter 290: Individual Income Tax)
- Sections (e.g., 290.01: Definitions)
- Subdivisions (e.g., Subd. 1: Tax base)
- Clauses (e.g., (a), (b), (c) within subdivisions)

URL Patterns:
- Chapter index: /statutes/?id=290
- Section: /statutes/cite/290.01
- Chapter text: /statutes/chapter/290

Key Tax Chapters (290-297A):
- 290: Individual Income Tax
- 290A: Property Tax Refund
- 290B: Senior Citizens Property Tax Deferral Program
- 290C: Sustainable Forest Incentive Act
- 291: Estate Tax
- 292: Property Tax Administration
- 296: Motor Fuel Taxation
- 297A: Sales and Use Tax

Human Services Chapter:
- 256: Human Services (MFIP, Child Care Assistance, etc.)

Example:
    >>> from atlas.converters.us_states.mn import MNConverter
    >>> converter = MNConverter()
    >>> section = converter.fetch_section("290.01")
    >>> print(section.section_title)
    "DEFINITIONS"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://www.revisor.mn.gov/statutes"

# Key chapters for tax/benefit analysis
MN_TAX_CHAPTERS: dict[str, str] = {
    "290": "Individual Income Tax",
    "270": "Revenue Department Duties",
    "270A": "Revenue Recapture",
    "270B": "Data Practices for Tax Matters",
    "270C": "Tax Administration",
    "271": "Tax Court",
    "272": "Property Taxes: General Provisions",
    "273": "Property Taxes: Classification and Valuation",
    "274": "Property Taxes: Board of Review and Equalization",
    "275": "Property Taxes: Levies, Extensions, Distribution",
    "276": "Property Taxes: Collection",
    "277": "Property Taxes: Delinquent Tax Collection",
    "278": "Property Taxes: Court Proceedings",
    "279": "Property Taxes: Delinquent Real Estate Taxes",
    "280": "Property Taxes: Forfeited Land",
    "281": "Property Taxes: State-Owned Land",
    "282": "Property Taxes: Tax-Forfeited Land",
    "284": "Property Taxes: Lawful Taxes",
    "289A": "Administration and Compliance",
    "290A": "Property Tax Refund",
    "290B": "Senior Citizens Property Tax Deferral",
    "290C": "Sustainable Forest Incentive Act",
    "291": "Estate Tax",
    "295": "MinnesotaCare Tax",
    "296A": "Motor Fuel Taxation",
    "297A": "Sales and Use Tax",
    "297B": "Motor Vehicle Sales Tax",
    "297E": "Lawful Gambling Taxes",
    "297F": "Tobacco Taxes",
    "297G": "Liquor Taxes",
    "297H": "Solid Waste Management Taxes",
    "297I": "Insurance Taxes",
}

MN_WELFARE_CHAPTERS: dict[str, str] = {
    "256": "Human Services",
    "256B": "Medical Assistance",
    "256D": "General Assistance",
    "256E": "Community Social Services",
    "256I": "Group Residential Housing",
    "256J": "Minnesota Family Investment Program (MFIP)",
    "256K": "Child Care Assistance",
    "256L": "MinnesotaCare",
    "256M": "Deaf and Hard of Hearing Services",
    "256N": "Housing Stabilization Services",
    "256P": "Eligibility and Assistance Standards",
    "256R": "Nursing Facility Rates",
    "256S": "Elderly Waiver",
    "257": "Children and Family Services",
    "259": "Adoption",
    "260": "Juvenile Court Act",
    "261": "Commitment and Guardianship",
}


@dataclass
class ParsedMNSection:
    """Parsed Minnesota statute section."""

    section_number: str  # e.g., "290.01"
    section_title: str  # e.g., "DEFINITIONS"
    chapter_number: str  # e.g., "290" or "290A"
    chapter_title: str  # e.g., "Individual Income Tax"
    text: str  # Full text content
    html: str  # Raw HTML
    subdivisions: list["ParsedMNSubdivision"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedMNSubdivision:
    """A subdivision within a Minnesota statute section.

    Minnesota uses subdivisions (Subd.) instead of subsections.
    Within subdivisions, clauses are marked with (a), (b), etc.
    """

    identifier: str  # e.g., "1", "2", "3"
    heading: str | None  # e.g., "Scope" (from "Subd. 1. Scope.")
    text: str
    clauses: list["ParsedMNClause"] = field(default_factory=list)


@dataclass
class ParsedMNClause:
    """A clause within a Minnesota subdivision."""

    identifier: str  # e.g., "a", "b", "c" or "1", "2", "3"
    text: str
    children: list["ParsedMNClause"] = field(default_factory=list)


class MNConverterError(Exception):
    """Error during Minnesota statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class MNConverter:
    """Converter for Minnesota Statutes HTML to internal Section model.

    Example:
        >>> converter = MNConverter()
        >>> section = converter.fetch_section("290.01")
        >>> print(section.citation.section)
        "MN-290.01"

        >>> for section in converter.iter_chapter(290):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Minnesota statute converter.

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

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "290.01", "256.012"

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/cite/{section_number}"

    def _build_chapter_url(self, chapter: str | int) -> str:
        """Build the URL for a chapter's table of contents."""
        return f"{BASE_URL}/cite/{chapter}"

    def _extract_chapter_number(self, section_number: str) -> str:
        """Extract chapter number from section number.

        Examples:
            "290.01" -> "290"
            "290A.03" -> "290A"
            "256B.04" -> "256B"
        """
        # Match chapter with optional letter suffix
        match = re.match(r"(\d+[A-Z]?)", section_number)
        if match:
            return match.group(1)
        return section_number.split(".")[0]  # pragma: no cover

    def _get_chapter_title(self, chapter: str) -> str:
        """Get the title for a chapter number."""
        # Try exact match first
        if chapter in MN_TAX_CHAPTERS:
            return MN_TAX_CHAPTERS[chapter]
        if chapter in MN_WELFARE_CHAPTERS:
            return MN_WELFARE_CHAPTERS[chapter]

        # Try integer lookup
        try:  # pragma: no cover
            chapter_int = int(chapter.rstrip("ABCDEFGHIJ"))  # pragma: no cover
            if chapter_int in MN_TAX_CHAPTERS_INT:  # pragma: no cover
                return MN_TAX_CHAPTERS_INT[chapter_int]  # pragma: no cover
        except ValueError:  # pragma: no cover
            pass

        return f"Chapter {chapter}"  # pragma: no cover

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedMNSection:
        """Parse section HTML into ParsedMNSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for page-level "not found" errors (not just any "not found" text on page)
        title_elem = soup.find("title")
        if title_elem:
            title_text = title_elem.get_text(strip=True).lower()
            if "not found" in title_text or "error" in title_text:
                raise MNConverterError(f"Section {section_number} not found", url)

        # Check for specific section error messages in the main content
        # Look for text like "Section 290.01 has been repealed" but not navigation text
        main_content = soup.find("div", id="legContainerMain") or soup.find("article") or soup
        if main_content:
            # Look for specific repealed/expired messages that include the section number
            error_pattern = re.compile(
                rf"(section\s+{re.escape(section_number)}\s+.{{0,30}}(repealed|expired|does not exist)|"
                rf"this\s+section\s+.{{0,30}}(repealed|expired|does not exist))",
                re.IGNORECASE,
            )
            error_text = main_content.get_text()
            if error_pattern.search(error_text):
                raise MNConverterError(
                    f"Section {section_number} has been repealed or expired", url
                )  # pragma: no cover

        chapter = self._extract_chapter_number(section_number)
        chapter_title = self._get_chapter_title(chapter)

        # Extract section title
        # Minnesota uses pattern like "290.01 DEFINITIONS" in h2
        section_title = ""

        # Try to find h2 with section number and title
        for heading in soup.find_all(["h1", "h2", "h3"]):
            heading_text = heading.get_text(strip=True)
            # Look for pattern: "290.01 TITLE" or "290.01TITLE"
            pattern = rf"{re.escape(section_number)}\s*([A-Z][A-Z\s,;:\-']+)"
            match = re.search(pattern, heading_text)
            if match:
                section_title = match.group(1).strip()
                break

        # Fallback: check title tag or page-title class
        if not section_title:
            title_elem = soup.find("title")
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                match = re.search(rf"{re.escape(section_number)}\s*[-–—]\s*(.+)", title_text)
                if match:
                    section_title = match.group(1).strip()  # pragma: no cover

        # Get main content area
        content_elem = (
            soup.find("div", id="legContainerMain")
            or soup.find("div", class_="statute-content")
            or soup.find("article")
            or soup.find("main")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(
                ["nav", "script", "style", "header", "footer"]
            ):  # pragma: no cover
                elem.decompose()
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note
        history = None
        history_match = re.search(r"History:\s*(.+?)(?:\n\n|\Z)", text, re.DOTALL)
        if history_match:
            history = history_match.group(1).strip()[:2000]  # Limit length

        # Parse subdivisions
        subdivisions = self._parse_subdivisions(soup, text)

        return ParsedMNSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            text=text,
            html=html_content,
            subdivisions=subdivisions,
            history=history,
            source_url=url,
        )

    def _parse_subdivisions(self, soup: BeautifulSoup, text: str) -> list[ParsedMNSubdivision]:
        """Parse subdivisions from statute text.

        Minnesota statutes use:
        - Subdivision 1., Subd. 1., or [§](#stat.X.X.1) for primary divisions
        - (a), (b), (c) for clauses within subdivisions
        - (1), (2), (3) for sub-clauses
        """
        subdivisions = []

        # Split by subdivision markers
        # Pattern: "Subdivision 1." or "Subd. 1." followed by optional heading
        subd_pattern = re.compile(
            r"(?:Subdivision|Subd\.?)\s*(\d+)\.?\s*([A-Za-z][^.]*\.)?", re.IGNORECASE
        )

        parts = subd_pattern.split(text)

        # Process parts - pattern produces groups: [prefix, num, heading, content, num, heading, content, ...]
        i = 1  # Skip content before first subdivision
        while i < len(parts) - 1:
            if i + 2 <= len(parts):
                identifier = parts[i] if i < len(parts) else None
                heading_raw = parts[i + 1] if i + 1 < len(parts) else None
                content = parts[i + 2] if i + 2 < len(parts) else ""

                if identifier:
                    heading = heading_raw.strip().rstrip(".") if heading_raw else None

                    # Find where next subdivision starts
                    next_subd = subd_pattern.search(content)
                    if next_subd:
                        content = content[: next_subd.start()]  # pragma: no cover

                    # Parse clauses within this subdivision
                    clauses = self._parse_clauses(content)

                    # Get text before first clause
                    first_clause_match = re.search(r"\([a-z]\)", content)
                    direct_text = (
                        content[: first_clause_match.start()].strip()
                        if first_clause_match
                        else content.strip()
                    )

                    subdivisions.append(
                        ParsedMNSubdivision(
                            identifier=identifier,
                            heading=heading,
                            text=direct_text[:4000],  # Limit text size
                            clauses=clauses,
                        )
                    )

                i += 3
            else:
                break  # pragma: no cover

        return subdivisions

    def _parse_clauses(self, text: str) -> list[ParsedMNClause]:
        """Parse clauses (a), (b), etc. from subdivision text."""
        clauses = []

        # Split by clause markers (a), (b), etc.
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first clause
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Find where next subdivision starts and truncate
            next_subd = re.search(r"(?:Subdivision|Subd\.?)\s*\d+", content, re.IGNORECASE)
            if next_subd:
                content = content[: next_subd.start()]  # pragma: no cover

            # Parse sub-clauses (1), (2), etc.
            children = self._parse_subclauses(content)

            # Get text before first sub-clause
            first_subclause_match = re.search(r"\(\d+\)", content)
            direct_text = (
                content[: first_subclause_match.start()].strip()
                if first_subclause_match
                else content.strip()
            )

            clauses.append(
                ParsedMNClause(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return clauses

    def _parse_subclauses(self, text: str) -> list[ParsedMNClause]:
        """Parse sub-clauses (1), (2), etc."""
        subclauses = []

        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit content - stop at next lettered clause
            next_clause = re.search(r"\([a-z]\)", content)
            if next_clause:
                content = content[: next_clause.start()]

            subclauses.append(
                ParsedMNClause(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subclauses

    def _to_section(self, parsed: ParsedMNSection) -> Section:
        """Convert ParsedMNSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"MN-{parsed.section_number}",
        )

        # Convert subdivisions to subsections
        subsections = []
        for subd in parsed.subdivisions:
            children = [
                Subsection(
                    identifier=clause.identifier,
                    heading=None,
                    text=clause.text,
                    children=[
                        Subsection(
                            identifier=child.identifier,
                            heading=None,
                            text=child.text,
                            children=[],
                        )
                        for child in clause.children
                    ],
                )
                for clause in subd.clauses
            ]

            subsections.append(
                Subsection(
                    identifier=subd.identifier,
                    heading=subd.heading,
                    text=subd.text,
                    children=children,
                )
            )

        return Section(
            citation=citation,
            title_name=f"Minnesota Statutes - {parsed.chapter_title}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"mn/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "290.01", "256.012"

        Returns:
            Section model

        Raises:
            MNConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: str | int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 290, "290A")

        Returns:
            List of section numbers (e.g., ["290.01", "290.011", ...])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []
        chapter_str = str(chapter)

        # Find section links: /statutes/cite/290.01
        pattern = re.compile(rf"/statutes/cite/({re.escape(chapter_str)}(?:\.\d+)+)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return sorted(
            section_numbers,
            key=lambda x: [int(p) if p.isdigit() else p for p in re.split(r"[.\-]", x)],
        )

    def iter_chapter(self, chapter: str | int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 290, "290A")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except MNConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[str | int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: tax chapters 290-297A)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            # Default to main tax chapters
            chapters = [290, 291, 295, "297A"]  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "MNConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_mn_section(section_number: str) -> Section:
    """Fetch a single Minnesota statute section.

    Args:
        section_number: e.g., "290.01"

    Returns:
        Section model
    """
    with MNConverter() as converter:
        return converter.fetch_section(section_number)


def download_mn_chapter(chapter: str | int) -> list[Section]:
    """Download all sections from a Minnesota Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 290, "290A")

    Returns:
        List of Section objects
    """
    with MNConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_mn_tax_chapters() -> Iterator[Section]:
    """Download sections from Minnesota tax-related chapters.

    Yields:
        Section objects from chapters 290, 291, 295, 297A
    """
    with MNConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters([290, 291, 295, "297A"])  # pragma: no cover


def download_mn_welfare_chapters() -> Iterator[Section]:
    """Download sections from Minnesota human services chapters (256, 256J, etc.).

    Yields:
        Section objects
    """
    with MNConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters([256, "256B", "256J", "256L"])  # pragma: no cover
