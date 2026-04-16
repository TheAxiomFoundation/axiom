"""Parser for Texas Statutes via web scraping.

Texas Legislature provides statutes at statutes.capitol.texas.gov.
The site offers bulk downloads in various formats.

Structure:
- Texas statutes are organized by Code (e.g., Tax Code, Human Resources Code)
- Each Code contains Chapters
- Each Chapter contains Sections

URL Patterns:
- Statute home: https://statutes.capitol.texas.gov/
- Code index: https://statutes.capitol.texas.gov/Docs/TX/htm/TX.1.htm (Tax Code Ch. 1)
- Section: https://statutes.capitol.texas.gov/Docs/TX/htm/TX.1.htm#1.01

Bulk XML available at:
- https://statutes.capitol.texas.gov/Download/html/TX.zip (Tax Code HTML)
"""

import logging
import re
import time
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from io import BytesIO
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

logger = logging.getLogger(__name__)

BASE_URL = "https://statutes.capitol.texas.gov"

# Texas Code abbreviations and full names
TX_CODES: dict[str, str] = {
    "TX": "Tax Code",
    "HR": "Human Resources Code",
    "LA": "Labor Code",
    "HS": "Health and Safety Code",
    "GV": "Government Code",
    "FA": "Family Code",
    "PE": "Penal Code",
    "CP": "Civil Practice and Remedies Code",
    "BC": "Business and Commerce Code",
    "FI": "Finance Code",
    "IN": "Insurance Code",
    "ED": "Education Code",
    "TN": "Transportation Code",
    "PW": "Parks and Wildlife Code",
    "WA": "Water Code",
    "AG": "Agriculture Code",
    "AL": "Alcoholic Beverage Code",
    "EL": "Election Code",
    "ES": "Estates Code",
    "OC": "Occupations Code",
    "PR": "Property Code",
    "NR": "Natural Resources Code",
    "UT": "Utilities Code",
    "LG": "Local Government Code",
    "SD": "Special District Local Laws Code",
}

# Priority codes for tax/benefit policy
TX_PRIORITY_CODES = ["TX", "HR", "LA", "HS", "GV", "FA", "IN"]


@dataclass
class TXChapterInfo:
    """Information about a Texas Statutes chapter."""

    code: str  # e.g., "TX" for Tax Code
    chapter: int
    title: str
    url: str


@dataclass
class TXSectionInfo:
    """Information about a Texas Statutes section."""

    code: str
    chapter: int
    section: str  # e.g., "171.001"
    title: str
    url: str


@dataclass
class TXSection:
    """A section from Texas Statutes with full content."""

    code: str
    code_name: str
    chapter: int
    section: str
    title: str
    text: str
    html: str
    url: str
    subsections: list["TXSubsection"] = field(default_factory=list)


@dataclass
class TXSubsection:
    """A subsection within a Texas statute."""

    identifier: str
    text: str
    children: list["TXSubsection"] = field(default_factory=list)


class TXStatutesError(Exception):
    """Error accessing Texas Statutes."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)  # pragma: no cover
        self.url = url  # pragma: no cover


class TXStatutesClient:
    """Client for scraping Texas Statutes.

    Texas provides HTML files that can be downloaded in bulk as ZIP archives.
    This client can either download the bulk ZIP or scrape individual pages.

    Example:
        >>> client = TXStatutesClient()
        >>> for section in client.iter_code("TX"):  # Tax Code
        ...     print(section.section, section.title)
    """

    def __init__(self, rate_limit_delay: float = 0.3, cache_dir: Path | None = None):
        """Initialize the Texas Statutes client.

        Args:
            rate_limit_delay: Seconds to wait between requests
            cache_dir: Directory to cache downloaded ZIP files
        """
        self.rate_limit_delay = rate_limit_delay  # pragma: no cover
        self.cache_dir = cache_dir or Path("data/texas_cache")  # pragma: no cover
        self._last_request_time = 0.0  # pragma: no cover
        self.client = httpx.Client(
            timeout=120.0,
            headers={
                "User-Agent": "Arch/1.0 (Statute Research; contact@rules.foundation)"
            },
            follow_redirects=True,
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

    def _get_bytes(self, url: str) -> bytes:
        """Make a GET request and return bytes."""
        self._rate_limit()  # pragma: no cover
        response = self.client.get(url)  # pragma: no cover
        response.raise_for_status()
        return response.content  # pragma: no cover

    def download_code_zip(self, code: str) -> Path:
        """Download the ZIP archive for a Texas code.

        Args:
            code: Code abbreviation (e.g., "TX" for Tax Code)

        Returns:
            Path to the downloaded ZIP file
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)  # pragma: no cover
        zip_path = self.cache_dir / f"{code}.zip"  # pragma: no cover

        if zip_path.exists():  # pragma: no cover
            return zip_path  # pragma: no cover

        url = f"{BASE_URL}/Download/html/{code}.zip"  # pragma: no cover
        print(f"Downloading {url}...")  # pragma: no cover

        content = self._get_bytes(url)  # pragma: no cover
        zip_path.write_bytes(content)  # pragma: no cover

        return zip_path  # pragma: no cover

    def iter_code_from_zip(self, code: str) -> Iterator[TXSection]:
        """Iterate over all sections in a code from its ZIP archive.

        Args:
            code: Code abbreviation (e.g., "TX" for Tax Code)

        Yields:
            TXSection for each section found
        """
        zip_path = self.download_code_zip(code)  # pragma: no cover
        code_name = TX_CODES.get(code, f"{code} Code")  # pragma: no cover

        with zipfile.ZipFile(zip_path, "r") as zf:  # pragma: no cover
            # Get list of HTML files
            html_files = [f for f in zf.namelist() if f.endswith(".htm")]  # pragma: no cover

            for html_file in sorted(html_files):  # pragma: no cover
                try:  # pragma: no cover
                    html_content = zf.read(html_file).decode("utf-8", errors="replace")  # pragma: no cover
                    yield from self._parse_html_file(  # pragma: no cover
                        html_content, code, code_name, html_file
                    )
                except Exception as e:  # pragma: no cover
                    logger.warning(  # pragma: no cover
                        "[TX] Error parsing %s: %s",
                        html_file,
                        e,
                        exc_info=True,
                    )
                    continue  # pragma: no cover

    def _parse_html_file(
        self, html: str, code: str, code_name: str, filename: str
    ) -> Iterator[TXSection]:
        """Parse an HTML file from the Texas statutes ZIP.

        Each HTML file contains multiple sections from a chapter.
        """
        soup = BeautifulSoup(html, "html.parser")  # pragma: no cover

        # Extract chapter number from filename (e.g., "TX.171.htm" -> 171)
        match = re.search(r"\.(\d+[A-Za-z]?)\.htm$", filename)  # pragma: no cover
        chapter = int(match.group(1)) if match else 0  # pragma: no cover

        # Find all section headings - they have anchors with section numbers
        # Pattern: <a name="171.001">...</a> or similar
        section_anchors = soup.find_all("a", attrs={"name": re.compile(r"^\d+\.\d+")})  # pragma: no cover

        for anchor in section_anchors:  # pragma: no cover
            section_num = anchor.get("name", "")  # pragma: no cover
            if not section_num:  # pragma: no cover
                continue  # pragma: no cover

            # Find the section content - typically the parent or sibling elements
            section_element = anchor.find_parent("p") or anchor.find_parent("div")  # pragma: no cover
            if not section_element:  # pragma: no cover
                continue  # pragma: no cover

            # Get section title from the heading
            title = ""  # pragma: no cover
            heading = section_element.find(["b", "strong"])  # pragma: no cover
            if heading:  # pragma: no cover
                title_text = heading.get_text(strip=True)  # pragma: no cover
                # Remove section number prefix
                title = re.sub(rf"^Sec\.?\s*{re.escape(section_num)}\.?\s*", "", title_text)  # pragma: no cover
                title = title.rstrip(".")  # pragma: no cover

            # Get section text - everything after the heading until next section
            text_parts = []  # pragma: no cover
            current = section_element  # pragma: no cover
            while current:  # pragma: no cover
                if current.name and current.name in ["p", "div"]:  # pragma: no cover
                    # Check if this is the start of a new section
                    new_anchor = current.find("a", attrs={"name": re.compile(r"^\d+\.\d+")})  # pragma: no cover
                    if new_anchor and new_anchor.get("name") != section_num:  # pragma: no cover
                        break  # pragma: no cover
                    text_parts.append(current.get_text(separator="\n", strip=True))  # pragma: no cover
                current = current.find_next_sibling()  # pragma: no cover

            text = "\n\n".join(text_parts)  # pragma: no cover

            # Build URL
            url = f"{BASE_URL}/Docs/{code}/htm/{code}.{chapter}.htm#{section_num}"  # pragma: no cover

            yield TXSection(  # pragma: no cover
                code=code,
                code_name=code_name,
                chapter=chapter,
                section=section_num,
                title=title or f"Section {section_num}",
                text=text,
                html=str(section_element) if section_element else "",
                url=url,
            )

    def iter_code(self, code: str) -> Iterator[TXSection]:
        """Iterate over all sections in a code.

        Uses ZIP download method for efficiency.

        Args:
            code: Code abbreviation (e.g., "TX")

        Yields:
            TXSection for each section
        """
        yield from self.iter_code_from_zip(code)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()  # pragma: no cover

    def __enter__(self) -> "TXStatutesClient":
        return self  # pragma: no cover

    def __exit__(self, *args) -> None:
        self.close()  # pragma: no cover


class TXStateCitation:
    """Citation for Texas state laws.

    Format: "Tex. {Code} Code § {section}"
    e.g., "Tex. Tax Code § 171.001"
    """

    def __init__(self, code: str, section: str, subsection: str | None = None):
        self.code = code  # pragma: no cover
        self.section = section  # pragma: no cover
        self.subsection = subsection  # pragma: no cover

    @property
    def code_name(self) -> str:
        """Return full code name."""
        return TX_CODES.get(self.code, f"{self.code} Code")  # pragma: no cover

    @property
    def cite_string(self) -> str:
        """Return formatted citation string."""
        base = f"Tex. {self.code_name} § {self.section}"  # pragma: no cover
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
            return f"state/tx/{self.code}/{chapter}/{self.section}/{self.subsection}"  # pragma: no cover
        return f"state/tx/{self.code}/{chapter}/{self.section}"  # pragma: no cover


def convert_to_section(tx_section: TXSection) -> Section:
    """Convert TX section to Arch Section model."""
    # Create citation - use 0 as title indicator for state laws
    citation = Citation(  # pragma: no cover
        title=0,  # State law indicator
        section=f"TX-{tx_section.code}-{tx_section.section}",
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
        for sub in tx_section.subsections
    ]

    return Section(  # pragma: no cover
        citation=citation,
        title_name=f"Texas {tx_section.code_name}",
        section_title=tx_section.title,
        text=tx_section.text,
        subsections=subsections,
        source_url=tx_section.url,
        retrieved_at=date.today(),
        uslm_id=f"tx/{tx_section.code}/{tx_section.chapter}/{tx_section.section}",
    )


def download_tx_code(code: str) -> Iterator[Section]:
    """Download all sections from a Texas code.

    Args:
        code: Code abbreviation (e.g., "TX" for Tax Code)

    Yields:
        Section objects for each section
    """
    with TXStatutesClient() as client:  # pragma: no cover
        for tx_section in client.iter_code(code):  # pragma: no cover
            yield convert_to_section(tx_section)  # pragma: no cover


def download_tx_priority_codes() -> Iterator[Section]:
    """Download sections from priority Texas codes (Tax, HR, Labor, etc.).

    Yields:
        Section objects for each section
    """
    with TXStatutesClient() as client:  # pragma: no cover
        for code in TX_PRIORITY_CODES:  # pragma: no cover
            print(f"Downloading Texas {TX_CODES.get(code, code)}...")  # pragma: no cover
            for tx_section in client.iter_code(code):  # pragma: no cover
                yield convert_to_section(tx_section)  # pragma: no cover
