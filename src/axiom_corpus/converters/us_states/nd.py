"""North Dakota state statute converter.

Converts North Dakota Century Code HTML from ndlegis.gov (North Dakota Legislative Branch)
to the internal Section model for ingestion.

North Dakota Century Code Structure:
- Titles (e.g., Title 57: Taxation)
- Chapters (e.g., Chapter 57-38: Income Tax)
- Sections (e.g., 57-38-01: Definitions)

URL Patterns:
- Title index: /cencode/t{title}.html (e.g., /cencode/t57.html)
- Chapter sections: /cencode/t{title}c{chapter}.html (e.g., /cencode/t57c38.html)
- Chapter PDF: /cencode/t{title}c{chapter}.pdf (e.g., /cencode/t57c38.pdf)
- PDF with section anchor: t57c38.pdf#nameddest=57-38-01

Note: Full section text is only available in PDF format. The HTML pages provide
section listings with numbers and titles, linking to named destinations in PDFs.

Example:
    >>> from axiom_corpus.converters.us_states.nd import NDConverter
    >>> converter = NDConverter()
    >>> section = converter.fetch_section_metadata("57-38-01")
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

BASE_URL = "https://ndlegis.gov/cencode"

# Title mapping for reference
ND_TITLES: dict[int, str] = {
    1: "General Provisions",
    2: "Aeronautics",
    3: "Agency",
    4: "Agriculture",
    5: "Alcoholic Beverages",
    6: "Assignments",
    7: "Banks and Banking",
    8: "Bonds",
    9: "Building and Loan Associations",
    10: "Children and Domestic Relations",
    11: "Cities, Counties, and Other Political Subdivisions",
    12: "Contracts and Obligations",
    13: "Corporations",
    14: "Criminal Procedure",
    15: "Damages",
    16: "Decedents' Estates",
    17: "Drainage",
    18: "Easements",
    19: "Foods, Drugs, Oils, and Compounds",
    20: "Game, Fish, Predators, and Boating",
    21: "Governmental Finance",
    22: "Guaranty, Indemnity, and Suretyship",
    23: "Health and Safety",
    24: "Hospitals and Related Institutions",
    25: "Indians",
    26: "Infants and Incompetents",
    27: "Judicial System",
    28: "Judgments, Executions, and Exemptions from Process",
    29: "Judicial Procedure, Civil",
    30: "Judicial Proof",
    31: "Judicial Remedies",
    32: "Liability - Limitation of Actions",
    33: "Liens",
    34: "Logs and Lumber",
    35: "Mental Health",
    36: "Mines and Mining",
    37: "Military",
    38: "Motor Fuels",
    39: "Motor Vehicles",
    40: "Municipal Government",
    41: "Negotiable Instruments",
    42: "Notaries",
    43: "Occupations and Professions",
    44: "Offices and Officers",
    45: "Partnerships",
    46: "Prisons and Jails",
    47: "Property",
    48: "Public Printing and Blanks",
    49: "Public Utilities",
    50: "Public Welfare",
    51: "Railroads",
    52: "Religion",
    53: "Reward for Capture of Criminals",
    54: "State Government",
    55: "State Lands",
    56: "Statutes",
    57: "Taxation",
    58: "Towns",
    59: "Trusts",
    60: "Warehousing and Deposits",
    61: "Waters",
    62: "Weapons",
    63: "Weights, Measures, and Grades",
}

# Key chapters for tax analysis (Title 57)
ND_TAX_CHAPTERS: dict[str, str] = {
    "57-01": "Tax Commissioner",
    "57-02": "General Property Assessment",
    "57-02.1": "Taxation of Mobile Homes",
    "57-02.2": "Property Tax Assessment - Soil Survey",
    "57-02.3": "Property Tax Credits",
    "57-02.4": "Property Tax Reduction for Disabled Veterans",
    "57-06": "County Board of Equalization",
    "57-12": "Tax Collection",
    "57-15": "Tax Levies and Limitations",
    "57-20": "Supplemental Income Tax Collections",
    "57-35": "Building and Loan Association Tax",
    "57-35.1": "Financial Institutions Tax",
    "57-35.3": "Estate Tax",
    "57-36": "Cigarette and Tobacco Products Tax",
    "57-38": "Income Tax",
    "57-38.3": "Seed Capital Investment Tax Credit",
    "57-38.5": "Renaissance Zone Credits",
    "57-38.6": "Housing Incentive Fund Tax Credit",
    "57-39.2": "Sales Tax",
    "57-39.4": "Motor Vehicle Excise Tax",
    "57-39.5": "Agricultural Commodity Processing Facility Investment Tax Credit",
    "57-40.2": "Use Tax",
    "57-40.3": "Motor Vehicle Use Tax",
    "57-43.1": "Motor Fuel Tax",
    "57-43.2": "Special Fuels Tax",
    "57-51": "Oil and Gas Gross Production Tax",
    "57-51.1": "Oil Extraction Tax",
    "57-55": "Lodging Tax",
    "57-60": "Coal Severance Tax",
    "57-65": "Telecommunications Tax",
}

# Key chapters for welfare/public assistance (Title 50)
ND_WELFARE_CHAPTERS: dict[str, str] = {
    "50-01": "County Poor Relief - Administration",
    "50-01.1": "Foster Care Children",
    "50-01.2": "Services for At-Risk Children and Adults",
    "50-06": "Department of Health and Human Services",
    "50-06.1": "Social Service Block Grant",
    "50-06.2": "Food Stamps",
    "50-06.3": "Child Care Assistance",
    "50-06.4": "Supplemental Nutrition Assistance Program",
    "50-06.5": "Low-Income Home Energy Assistance Program",
    "50-09": "Determination of Paternity",
    "50-10.1": "Child Support Enforcement",
    "50-10.2": "Child Support Guidelines",
    "50-11": "Adoption Assistance and Subsidized Adoption",
    "50-11.1": "Adoption of Children",
    "50-12": "Aid to Families With Dependent Children",
    "50-24.1": "Medical Assistance",
    "50-24.4": "Medicaid Reform",
    "50-24.5": "Drug Utilization Review",
    "50-24.7": "Children's Health Insurance Program",
    "50-25": "Child Abuse and Neglect",
    "50-25.1": "Prevention and Treatment of Child Abuse and Neglect",
}


@dataclass
class ParsedNDSection:
    """Parsed North Dakota statute section from HTML listing."""

    section_number: str  # e.g., "57-38-01"
    section_title: str  # e.g., "Definitions"
    chapter: str  # e.g., "57-38"
    chapter_title: str | None  # e.g., "Income Tax"
    title_number: int  # e.g., 57
    title_name: str | None  # e.g., "Taxation"
    pdf_url: str  # URL to PDF with section anchor
    html_source_url: str  # URL to HTML chapter listing
    is_repealed: bool = False
    effective_date_note: str | None = None  # e.g., "(Effective through June 30, 2027)"


class NDConverterError(Exception):
    """Error during North Dakota statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)  # pragma: no cover
        self.url = url  # pragma: no cover


class NDConverter:
    """Converter for North Dakota Century Code HTML to internal Section model.

    The ND Legislature provides section metadata via HTML pages and full text
    via PDFs. This converter primarily extracts metadata from HTML listings.

    Example:
        >>> converter = NDConverter()
        >>> section = converter.fetch_section_metadata("57-38-01")
        >>> print(section.citation.section)
        "ND-57-38-01"

        >>> for section in converter.iter_chapter("57-38"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the North Dakota statute converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
        """
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None
        # Cache chapter data to avoid repeated fetches
        self._chapter_cache: dict[str, list[dict]] = {}

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                timeout=60.0,
                headers={
                    "User-Agent": "Axiom/1.0 (Statute Research; contact@axiom-foundation.org)",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
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

    def _build_title_url(self, title: int) -> str:
        """Build the URL for a title page.

        Args:
            title: Title number (e.g., 57)

        Returns:
            Full URL to the title page
        """
        return f"{BASE_URL}/t{title}.html"

    def _build_chapter_url(self, chapter: str) -> str:
        """Build the URL for a chapter's section listing.

        Args:
            chapter: Chapter identifier (e.g., "57-38", "57-02.1")

        Returns:
            Full URL to the chapter HTML page
        """
        # Parse chapter: "57-38" -> title=57, chapter_num=38
        # For decimal chapters: "57-02.1" -> t57c02-1.html
        parts = chapter.split("-")
        title = parts[0]
        chapter_part = parts[1] if len(parts) > 1 else "01"

        # Handle decimal chapters: 02.1 -> 02-1
        if "." in chapter_part:
            chapter_part = chapter_part.replace(".", "-")

        return f"{BASE_URL}/t{title}c{chapter_part}.html"

    def _build_pdf_url(self, chapter: str) -> str:
        """Build the URL for a chapter PDF.

        Args:
            chapter: Chapter identifier (e.g., "57-38")

        Returns:
            Full URL to the chapter PDF
        """
        parts = chapter.split("-")
        title = parts[0]
        chapter_part = parts[1] if len(parts) > 1 else "01"

        # Handle decimal chapters: 02.1 -> 02-1
        if "." in chapter_part:
            chapter_part = chapter_part.replace(".", "-")  # pragma: no cover

        return f"{BASE_URL}/t{title}c{chapter_part}.pdf"

    def _parse_section_number(self, section_number: str) -> tuple[int, str, str]:
        """Parse a section number into components.

        Args:
            section_number: e.g., "57-38-01", "57-38-01.1"

        Returns:
            Tuple of (title, chapter, section)
            e.g., (57, "57-38", "01") or (57, "57-38", "01.1")
        """
        parts = section_number.split("-")
        if len(parts) < 3:
            raise NDConverterError(
                f"Invalid section number format: {section_number}"
            )  # pragma: no cover

        title = int(parts[0])
        chapter = f"{parts[0]}-{parts[1]}"
        section = parts[2]

        return title, chapter, section

    def _parse_chapter_html(self, html: str, chapter: str) -> list[dict]:
        """Parse chapter HTML into a list of section info dictionaries.

        Args:
            html: Raw HTML content
            chapter: Chapter identifier for context

        Returns:
            List of dicts with section_number, section_title, pdf_anchor keys
        """
        soup = BeautifulSoup(html, "html.parser")
        sections = []

        # Extract title number from chapter
        title = int(chapter.split("-")[0])
        title_name = ND_TITLES.get(title, f"Title {title}")

        # Get chapter title from registry or heading
        chapter_title = ND_TAX_CHAPTERS.get(chapter) or ND_WELFARE_CHAPTERS.get(chapter) or None

        # Try to extract from page heading if not in registry
        if not chapter_title:
            heading = soup.find("h1")  # pragma: no cover
            if heading:  # pragma: no cover
                chapter_title = (
                    heading.get_text(strip=True).replace(f"Chapter {chapter}", "").strip()
                )  # pragma: no cover

        # Find section rows in tables
        # Pattern: <td class="no-wrap"><a href="t57c38.pdf#nameddest=57-38-01">57-38-01</a></td>
        #          <td>Definitions</td>
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:  # pragma: no cover
                continue

            # First cell should contain the section number link
            first_cell = cells[0]
            link = first_cell.find("a")
            if not link:
                continue  # pragma: no cover

            href = link.get("href", "")
            section_number = link.get_text(strip=True)

            # Validate section number format
            if not re.match(r"\d+-\d+-\d+", section_number):
                continue  # pragma: no cover

            # Second cell contains the title
            section_title = cells[1].get_text(strip=True) if len(cells) > 1 else ""

            # Check for repealed status
            is_repealed = "[Repealed]" in section_title

            # Check for effective date notes
            effective_note = None
            date_match = re.search(r"\(Effective[^)]+\)", section_title)
            if date_match:
                effective_note = date_match.group(0)

            # Build PDF URL with anchor
            pdf_url = f"{BASE_URL}/{href}" if not href.startswith("http") else href

            sections.append(
                {
                    "section_number": section_number,
                    "section_title": section_title,
                    "chapter": chapter,
                    "chapter_title": chapter_title,
                    "title_number": title,
                    "title_name": title_name,
                    "pdf_url": pdf_url,
                    "is_repealed": is_repealed,
                    "effective_date_note": effective_note,
                }
            )

        return sections

    def _parse_title_html(self, html: str, title: int) -> dict[str, str]:
        """Parse title HTML to get list of chapters.

        Args:
            html: Raw HTML content
            title: Title number

        Returns:
            Dict mapping chapter identifier to chapter name
        """
        soup = BeautifulSoup(html, "html.parser")
        chapters = {}

        # Pattern: <a href="t57c38.html">57-38 Sections</a>
        # OR: <td class="no-wrap"><a href="t57c38.pdf">57-38</a></td> + <td>Income Tax</td>
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:  # pragma: no cover
                continue

            # First cell contains the chapter number
            first_cell = cells[0]
            link = first_cell.find("a")
            if not link:
                continue  # pragma: no cover

            chapter_num = link.get_text(strip=True)
            if not re.match(rf"{title}-\d+", chapter_num):
                continue  # pragma: no cover

            # Third cell (if present) contains chapter name
            # Or look for the name in the row text
            chapter_name = ""
            for cell in cells[2:]:
                text = cell.get_text(strip=True)
                if text and not "Sections" in text:
                    chapter_name = text
                    break

            # If no name found in third cell, check for row text
            if not chapter_name:
                row_text = row.get_text(strip=True)  # pragma: no cover
                # Remove the chapter number and "Sections" links
                row_text = re.sub(
                    rf"{re.escape(chapter_num)}\s*Sections?\s*", "", row_text
                )  # pragma: no cover
                row_text = row_text.replace(chapter_num, "").strip()  # pragma: no cover
                chapter_name = (
                    row_text if row_text else f"Chapter {chapter_num}"
                )  # pragma: no cover

            chapters[chapter_num] = chapter_name

        return chapters

    def get_chapter_sections(self, chapter: str) -> list[dict]:
        """Get list of section info dicts for a chapter.

        Args:
            chapter: Chapter identifier (e.g., "57-38")

        Returns:
            List of dicts with section metadata
        """
        # Check cache first
        if chapter in self._chapter_cache:
            return self._chapter_cache[chapter]  # pragma: no cover

        url = self._build_chapter_url(chapter)
        html = self._get(url)
        sections = self._parse_chapter_html(html, chapter)

        if not sections:
            raise NDConverterError(
                f"No sections found in chapter {chapter}", url
            )  # pragma: no cover

        # Cache the result
        self._chapter_cache[chapter] = sections
        return sections

    def get_title_chapters(self, title: int) -> dict[str, str]:
        """Get list of chapters in a title.

        Args:
            title: Title number (e.g., 57)

        Returns:
            Dict mapping chapter identifier to chapter name
        """
        url = self._build_title_url(title)
        html = self._get(url)
        return self._parse_title_html(html, title)

    def _to_section(self, info: dict, html_url: str) -> Section:
        """Convert section info dict to Section model.

        Args:
            info: Section info dict from _parse_chapter_html
            html_url: URL of the chapter HTML page

        Returns:
            Section model
        """
        section_number = info["section_number"]

        # Create citation with state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"ND-{section_number}",
        )

        # Build title name
        title_name = f"North Dakota Century Code - {info.get('title_name', 'Unknown')}"
        if info.get("chapter_title"):
            title_name = f"{title_name} - {info['chapter_title']}"

        # Section text is only available in PDF; for metadata-only we use the title
        text = info["section_title"]
        if info.get("effective_date_note"):
            text = f"{text} {info['effective_date_note']}"  # pragma: no cover

        return Section(
            citation=citation,
            title_name=title_name,
            section_title=info["section_title"],
            text=text,
            subsections=[],  # Not available from HTML
            source_url=info["pdf_url"],
            retrieved_at=date.today(),
            uslm_id=f"nd/{info['title_number']}/{info['chapter']}/{section_number}",
        )

    def fetch_section_metadata(self, section_number: str) -> Section:
        """Fetch section metadata (without full text from PDF).

        Args:
            section_number: e.g., "57-38-01", "50-06-01.1"

        Returns:
            Section model with metadata from HTML listing

        Raises:
            NDConverterError: If section not found
        """
        title, chapter, section = self._parse_section_number(section_number)
        sections = self.get_chapter_sections(chapter)

        # Find the matching section
        for info in sections:
            if info["section_number"] == section_number:
                html_url = self._build_chapter_url(chapter)
                return self._to_section(info, html_url)

        raise NDConverterError(
            f"Section {section_number} not found in chapter {chapter}"
        )  # pragma: no cover

    def iter_chapter(
        self,
        chapter: str,
        include_text: bool = False,
    ) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter identifier (e.g., "57-38")
            include_text: If True, fetch full text from PDFs (not yet implemented)

        Yields:
            Section objects for each section
        """
        sections = self.get_chapter_sections(chapter)
        html_url = self._build_chapter_url(chapter)

        for info in sections:
            yield self._to_section(info, html_url)

    def iter_chapters(
        self,
        chapters: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter identifiers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(ND_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            try:  # pragma: no cover
                yield from self.iter_chapter(chapter)  # pragma: no cover
            except NDConverterError as e:  # pragma: no cover
                print(f"Warning: Could not fetch chapter {chapter}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "NDConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_nd_section(section_number: str) -> Section:
    """Fetch a single North Dakota Century Code section.

    Args:
        section_number: e.g., "57-38-01"

    Returns:
        Section model
    """
    with NDConverter() as converter:
        return converter.fetch_section_metadata(section_number)


def download_nd_chapter(chapter: str) -> list[Section]:
    """Download all sections from a North Dakota Century Code chapter.

    Args:
        chapter: Chapter identifier (e.g., "57-38")

    Returns:
        List of Section objects
    """
    with NDConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_nd_tax_chapters() -> Iterator[Section]:
    """Download all sections from North Dakota tax-related chapters (Title 57).

    Yields:
        Section objects
    """
    with NDConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(ND_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_nd_welfare_chapters() -> Iterator[Section]:
    """Download all sections from North Dakota public welfare chapters (Title 50).

    Yields:
        Section objects
    """
    with NDConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(ND_WELFARE_CHAPTERS.keys()))  # pragma: no cover
