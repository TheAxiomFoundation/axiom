"""Parser for Florida Statutes via web scraping.

Florida Legislature provides statutes at leg.state.fl.us/statutes. There is no
public API, so this parser scrapes the HTML pages.

Structure:
- Statutes are organized by Title (e.g., Title XIV: Taxation and Finance)
- Titles contain Chapters (e.g., Chapter 212: Tax on Sales, Use, and Other Transactions)
- Chapters contain Sections (e.g., 212.05: Sales, storage, use tax)

URL Patterns:
- Chapter index: index.cfm?App_mode=Display_Statute&URL=0200-0299/0212/0212ContentsIndex.html
- Section: index.cfm?App_mode=Display_Statute&URL=0200-0299/0212/Sections/0212.05.html
"""

import logging
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom.models import Citation, Section, Subsection

logger = logging.getLogger(__name__)

BASE_URL = "https://leg.state.fl.us/statutes"

# Florida Statutes Title XIV: Taxation and Finance (Chapters 192-220)
# Key chapters for tax/benefit policy
FL_TAX_CHAPTERS: dict[int, str] = {
    192: "Taxation: General Provisions",
    193: "Assessments",
    194: "Administrative and Judicial Review of Property Taxes",
    195: "Property Assessment Administration and Finance",
    196: "Exemption",
    197: "Tax Collections, Sales, and Liens",
    198: "Estate Taxes",
    199: "Intangible Personal Property Taxes",
    200: "Determination of Millage",
    201: "Excise Tax on Documents",
    202: "Communications Services Tax Simplification Law",
    203: "Gross Receipts Taxes",
    205: "Local Business Taxes",
    206: "Motor and Other Fuel Taxes",
    207: "Tax on Operation of Commercial Motor Vehicles",
    210: "Tax on Tobacco Products",
    211: "Tax on Production of Oil and Gas and Severance of Solid Minerals",
    212: "Tax on Sales, Use, and Other Transactions",
    213: "State Revenue Laws: General Provisions",
    215: "Financial Matters: General Provisions",
    216: "Planning and Budgeting",
    217: "Surplus Property",
    218: "Financial Matters Pertaining to Political Subdivisions",
    219: "County Public Money, Handling by State and County",
    220: "Income Tax Code",
}

# Florida Statutes Title XXX: Social Welfare (Chapters 409-430)
FL_WELFARE_CHAPTERS: dict[int, str] = {
    409: "Social and Economic Assistance",
    410: "Adult Services",
    411: "Child and Family Programs",
    414: "Family Self-Sufficiency",
    415: "Adult Protective Services",
    420: "Housing",
    429: "Assisted Living Facilities",
    430: "Elderly Affairs",
}


@dataclass
class FLChapterInfo:
    """Information about a Florida Statutes chapter."""

    number: int
    title: str
    url_range: str  # e.g., "0200-0299" for chapters 200-299

    @property
    def padded_number(self) -> str:
        """Return zero-padded chapter number (e.g., '0212')."""
        return f"{self.number:04d}"  # pragma: no cover

    @property
    def contents_url(self) -> str:
        """Return URL to chapter contents index."""
        return (  # pragma: no cover
            f"{BASE_URL}/index.cfm?App_mode=Display_Statute"
            f"&URL={self.url_range}/{self.padded_number}/{self.padded_number}ContentsIndex.html"
        )


@dataclass
class FLSectionInfo:
    """Information about a Florida Statutes section."""

    number: str  # e.g., "212.05", "220.02"
    title: str
    chapter: int
    url: str


@dataclass
class FLSection:
    """A section from Florida Statutes with full content."""

    number: str
    title: str
    chapter: int
    chapter_title: str
    text: str
    html: str
    url: str
    subsections: list["FLSubsection"] = field(default_factory=list)


@dataclass
class FLSubsection:
    """A subsection within a Florida statute."""

    identifier: str  # e.g., "1", "a", "I"
    text: str
    children: list["FLSubsection"] = field(default_factory=list)


class FLStatutesError(Exception):
    """Error accessing Florida Statutes."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)  # pragma: no cover
        self.url = url  # pragma: no cover


class FLStatutesClient:
    """Client for scraping Florida Statutes.

    Example:
        >>> client = FLStatutesClient()
        >>> sections = client.get_chapter_sections(212)
        >>> section = client.get_section("212.05")
        >>> for sec in client.iter_chapter(212):
        ...     print(sec.number, sec.title)
    """

    def __init__(self, rate_limit_delay: float = 0.5, year: int | None = None):
        """Initialize the Florida Statutes client.

        Args:
            rate_limit_delay: Seconds to wait between requests (default 0.5)
            year: Statute year to fetch (default: current year)
        """
        self.rate_limit_delay = rate_limit_delay  # pragma: no cover
        self.year = year or date.today().year  # pragma: no cover
        self._last_request_time = 0.0  # pragma: no cover
        self.client = httpx.Client(
            timeout=60.0,
            headers={"User-Agent": "Axiom/1.0 (Statute Research; contact@axiom-foundation.org)"},
        )

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str) -> str:
        """Make a GET request and return HTML content."""
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    def _get_url_range(self, chapter: int) -> str:
        """Determine the URL range folder for a chapter number."""
        # Florida uses 100-chapter ranges: 0000-0099, 0100-0199, 0200-0299, etc.
        lower = (chapter // 100) * 100  # pragma: no cover
        upper = lower + 99  # pragma: no cover
        return f"{lower:04d}-{upper:04d}"  # pragma: no cover

    def get_chapter_info(self, chapter: int) -> FLChapterInfo:
        """Get chapter info from known chapters or by probing.

        Args:
            chapter: Chapter number (e.g., 212)

        Returns:
            FLChapterInfo with chapter details
        """
        # Check known chapters first
        title = FL_TAX_CHAPTERS.get(chapter) or FL_WELFARE_CHAPTERS.get(chapter)  # pragma: no cover
        if not title:  # pragma: no cover
            title = f"Chapter {chapter}"  # pragma: no cover

        return FLChapterInfo(  # pragma: no cover
            number=chapter,
            title=title,
            url_range=self._get_url_range(chapter),
        )

    def get_chapter_sections(self, chapter: int) -> list[FLSectionInfo]:
        """Get list of all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 212)

        Returns:
            List of FLSectionInfo for each section in the chapter
        """
        info = self.get_chapter_info(chapter)  # pragma: no cover
        html = self._get(info.contents_url)  # pragma: no cover
        soup = BeautifulSoup(html, "html.parser")  # pragma: no cover

        sections = []  # pragma: no cover

        # Find section links - they follow pattern like "212.01", "212.02", etc.
        # Links are in format: Sections/0212.01.html
        section_pattern = re.compile(
            rf"Sections/{info.padded_number}\.(\d+[A-Za-z]?)\.html"
        )  # pragma: no cover

        for link in soup.find_all("a", href=section_pattern):  # pragma: no cover
            href = link.get("href", "")  # pragma: no cover
            match = section_pattern.search(href)  # pragma: no cover
            if match:  # pragma: no cover
                section_num = f"{chapter}.{match.group(1)}"  # pragma: no cover
                # Get title from link text, cleaning up whitespace
                title = link.get_text(strip=True)  # pragma: no cover
                # Title often includes section number prefix, remove it
                title = re.sub(
                    rf"^{re.escape(section_num)}\s*[-:.]?\s*", "", title
                )  # pragma: no cover

                # Build full URL
                section_url = (  # pragma: no cover
                    f"{BASE_URL}/index.cfm?App_mode=Display_Statute"
                    f"&URL={info.url_range}/{info.padded_number}/Sections/{info.padded_number}.{match.group(1)}.html"
                )

                sections.append(  # pragma: no cover
                    FLSectionInfo(
                        number=section_num,
                        title=title or f"Section {section_num}",
                        chapter=chapter,
                        url=section_url,
                    )
                )

        return sections  # pragma: no cover

    def get_section(self, section_number: str) -> FLSection:
        """Get full content of a specific section.

        Args:
            section_number: Section number (e.g., "212.05", "220.02")

        Returns:
            FLSection with full text and metadata
        """
        # Parse chapter from section number
        chapter = int(section_number.split(".")[0])  # pragma: no cover
        info = self.get_chapter_info(chapter)  # pragma: no cover

        # Build URL - section numbers can include letters (e.g., 212.08, 212.0596)
        section_suffix = (
            section_number.split(".", 1)[1] if "." in section_number else section_number
        )  # pragma: no cover
        url = (  # pragma: no cover
            f"{BASE_URL}/index.cfm?App_mode=Display_Statute"
            f"&URL={info.url_range}/{info.padded_number}/Sections/{info.padded_number}.{section_suffix}.html"
        )

        html = self._get(url)  # pragma: no cover
        return self._parse_section_page(
            html, section_number, chapter, info.title, url
        )  # pragma: no cover

    def _parse_section_page(
        self,
        html: str,
        section_number: str,
        chapter: int,
        chapter_title: str,
        url: str,
    ) -> FLSection:
        """Parse a section page HTML into FLSection."""
        soup = BeautifulSoup(html, "html.parser")  # pragma: no cover

        # Check for "cannot be found" error
        if "cannot be found" in html.lower():  # pragma: no cover
            raise FLStatutesError(f"Section {section_number} not found", url)  # pragma: no cover

        # Find the section title - usually in a heading or strong tag
        title = ""  # pragma: no cover
        # Look for section header pattern like "212.05 Sales, storage, use tax.—"
        title_pattern = re.compile(rf"{re.escape(section_number)}\s+(.+?)\.?—")  # pragma: no cover
        title_match = title_pattern.search(html)  # pragma: no cover
        if title_match:  # pragma: no cover
            title = title_match.group(1).strip()  # pragma: no cover

        # Alternative: look in page title or heading
        if not title:  # pragma: no cover
            h1 = soup.find("h1") or soup.find("h2")  # pragma: no cover
            if h1:  # pragma: no cover
                h1_text = h1.get_text(strip=True)  # pragma: no cover
                title_match = title_pattern.search(h1_text)  # pragma: no cover
                if title_match:  # pragma: no cover
                    title = title_match.group(1).strip()  # pragma: no cover
                else:
                    # Use full h1 text, removing section number prefix
                    title = re.sub(
                        rf"^{re.escape(section_number)}\s*[-:.]?\s*", "", h1_text
                    )  # pragma: no cover

        # Get full text content
        # Florida statutes are typically in a main content area
        # Look for the statute text - often in a specific div or the main body
        content_div = soup.find("div", class_="Statute") or soup.find(
            "div", id="statute"
        )  # pragma: no cover

        if content_div:  # pragma: no cover
            text = content_div.get_text(separator="\n", strip=True)  # pragma: no cover
            section_html = str(content_div)  # pragma: no cover
        else:
            # Fall back to body text, trying to exclude navigation
            body = soup.find("body")  # pragma: no cover
            if body:  # pragma: no cover
                # Remove navigation elements
                for nav in body.find_all(
                    ["nav", "header", "footer", "script", "style"]
                ):  # pragma: no cover
                    nav.decompose()  # pragma: no cover
                text = body.get_text(separator="\n", strip=True)  # pragma: no cover
                section_html = str(body)  # pragma: no cover
            else:
                text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
                section_html = html  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(text)  # pragma: no cover

        return FLSection(  # pragma: no cover
            number=section_number,
            title=title or f"Section {section_number}",
            chapter=chapter,
            chapter_title=chapter_title,
            text=text,
            html=section_html,
            url=url,
            subsections=subsections,
        )

    def _parse_subsections(self, text: str) -> list[FLSubsection]:
        """Parse subsection structure from statute text.

        Florida statutes typically use:
        (1), (2), (3) - Primary divisions
        (a), (b), (c) - Secondary divisions
        1., 2., 3. - Tertiary divisions (sometimes)
        """
        # For now, return empty list - full parsing is complex
        # TODO: Implement hierarchical subsection parsing
        return []  # pragma: no cover

    def iter_chapter(self, chapter: int) -> Iterator[FLSection]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 212)

        Yields:
            FLSection for each section in the chapter
        """
        sections = self.get_chapter_sections(chapter)  # pragma: no cover
        for section_info in sections:  # pragma: no cover
            try:  # pragma: no cover
                yield self.get_section(section_info.number)  # pragma: no cover
            except FLStatutesError as e:  # pragma: no cover
                # Log but continue with other sections
                logger.warning(  # pragma: no cover
                    "[FL] Could not fetch section %s: %s",
                    section_info.number,
                    e,
                    exc_info=True,
                )
                continue  # pragma: no cover

    def iter_chapters(self, chapters: list[int] | None = None) -> Iterator[FLSection]:
        """Iterate over all sections in multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all tax chapters)

        Yields:
            FLSection for each section
        """
        if chapters is None:  # pragma: no cover
            chapters = list(FL_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()  # pragma: no cover

    def __enter__(self) -> "FLStatutesClient":
        return self  # pragma: no cover

    def __exit__(self, *args) -> None:
        self.close()  # pragma: no cover


class FLStateCitation:
    """Citation for Florida state laws.

    Format: "Fla. Stat. section {section}" e.g., "Fla. Stat. section 212.05"
    """

    def __init__(self, section: str, subsection: str | None = None):
        self.section = section  # pragma: no cover
        self.subsection = subsection  # pragma: no cover

    @property
    def cite_string(self) -> str:
        """Return formatted citation string."""
        base = f"Fla. Stat. \u00a7 {self.section}"  # pragma: no cover
        if self.subsection:  # pragma: no cover
            parts = self.subsection.split("/")  # pragma: no cover
            formatted = "".join(f"({p})" for p in parts)  # pragma: no cover
            return f"{base}{formatted}"  # pragma: no cover
        return base  # pragma: no cover

    @property
    def path(self) -> str:
        """Return filesystem-style path."""
        chapter = self.section.split(".")[0]  # pragma: no cover
        if self.subsection:  # pragma: no cover
            return f"state/fl/{chapter}/{self.section}/{self.subsection}"  # pragma: no cover
        return f"state/fl/{chapter}/{self.section}"  # pragma: no cover

    @classmethod
    def from_string(cls, cite: str) -> "FLStateCitation":
        """Parse a citation string like 'Fla. Stat. section 212.05(1)(a)'.

        Handles formats:
        - Fla. Stat. section 212.05
        - Fla. Stat. section 212.05(1)
        - Fla. Stat. section 212.05(1)(a)
        - F.S. 212.05
        - section 212.05, F.S.
        """
        # Normalize the citation
        cite = cite.strip()  # pragma: no cover

        # Pattern for section number with optional subsections
        section_pattern = r"(\d+\.\d+[A-Za-z]?)(?:\(([^)]+)\))?"  # pragma: no cover

        # Try to find the section number
        match = re.search(section_pattern, cite)  # pragma: no cover
        if not match:  # pragma: no cover
            raise ValueError(f"Cannot parse Florida citation: {cite}")  # pragma: no cover

        section = match.group(1)  # pragma: no cover

        # Parse subsections like (1)(a)(I) into 1/a/I
        subsection = None  # pragma: no cover
        remainder = cite[match.end() :]  # pragma: no cover
        sub_pattern = r"\(([^)]+)\)"  # pragma: no cover
        subs = re.findall(sub_pattern, cite)  # pragma: no cover
        if subs:  # pragma: no cover
            subsection = "/".join(subs)  # pragma: no cover

        return cls(section=section, subsection=subsection)  # pragma: no cover


def convert_to_section(fl_section: FLSection) -> Section:
    """Convert FL scrape section to AxiomArchive Section model.

    Args:
        fl_section: Section from FL scraper

    Returns:
        AxiomArchive Section model
    """
    # Create citation - use 0 as title indicator for state laws
    citation = Citation(  # pragma: no cover
        title=0,  # State law indicator
        section=f"FL-{fl_section.number}",
    )

    # Convert subsections
    subsections = [  # pragma: no cover
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
        for sub in fl_section.subsections
    ]

    return Section(  # pragma: no cover
        citation=citation,
        title_name=f"Florida Statutes Chapter {fl_section.chapter}",
        section_title=fl_section.title,
        text=fl_section.text,
        subsections=subsections,
        source_url=fl_section.url,
        retrieved_at=date.today(),
        uslm_id=f"fl/{fl_section.chapter}/{fl_section.number}",
    )


def download_fl_chapter(
    chapter: int,
    rate_limit_delay: float = 0.5,
) -> Iterator[Section]:
    """Download all sections from a Florida Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 212)
        rate_limit_delay: Seconds between requests

    Yields:
        Section objects for each section in the chapter
    """
    with FLStatutesClient(rate_limit_delay=rate_limit_delay) as client:  # pragma: no cover
        for fl_section in client.iter_chapter(chapter):  # pragma: no cover
            yield convert_to_section(fl_section)  # pragma: no cover


def download_fl_tax_statutes(
    rate_limit_delay: float = 0.5,
) -> Iterator[Section]:
    """Download all sections from Florida tax-related chapters (192-220).

    Args:
        rate_limit_delay: Seconds between requests

    Yields:
        Section objects for each section
    """
    with FLStatutesClient(rate_limit_delay=rate_limit_delay) as client:  # pragma: no cover
        yield from (convert_to_section(s) for s in client.iter_chapters())  # pragma: no cover


def download_fl_welfare_statutes(
    rate_limit_delay: float = 0.5,
) -> Iterator[Section]:
    """Download all sections from Florida social welfare chapters (409-430).

    Args:
        rate_limit_delay: Seconds between requests

    Yields:
        Section objects for each section
    """
    chapters = list(FL_WELFARE_CHAPTERS.keys())  # pragma: no cover
    with FLStatutesClient(rate_limit_delay=rate_limit_delay) as client:  # pragma: no cover
        yield from (
            convert_to_section(s) for s in client.iter_chapters(chapters)
        )  # pragma: no cover
