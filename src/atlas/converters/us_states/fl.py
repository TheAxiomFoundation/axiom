"""Florida state statute converter.

Converts Florida Statutes HTML from leg.state.fl.us ("Online Sunshine" portal)
to the internal Section model for ingestion.

Florida Statute Structure:
- Titles (e.g., Title XIV: Taxation and Finance)
- Chapters (e.g., Chapter 220: Income Tax Code)
- Parts (e.g., Part I: Legislative Intent; Definitions)
- Sections (e.g., 220.02: Legislative intent)

URL Patterns:
- Title index: index.cfm?App_mode=Display_Index&Title_Request=[ROMAN]
- Chapter contents: index.cfm?App_mode=Display_Statute&URL=0200-0299/0220/0220ContentsIndex.html
- Section: index.cfm?App_mode=Display_Statute&URL=0200-0299/0220/Sections/0220.02.html

Note: Florida has no state income tax on individuals (only corporate income tax via Chapter 220).

Example:
    >>> from atlas.converters.us_states.fl import FLConverter
    >>> converter = FLConverter()
    >>> section = converter.fetch_section("220.02")
    >>> print(section.section_title)
    "Legislative intent"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "https://www.leg.state.fl.us/statutes"

# Title mapping for reference
FL_TITLES: dict[str, str] = {
    "I": "Construction of Statutes",
    "II": "State Organization",
    "III": "Legislative Branch; Commissions",
    "IV": "Executive Branch",
    "V": "Judicial Branch",
    "VI": "Civil Practice and Procedure",
    "VII": "Evidence",
    "VIII": "Limitations",
    "IX": "Electors and Elections",
    "X": "Public Officers, Employees, and Records",
    "XI": "County Organization and Intergovernmental Relations",
    "XII": "Municipalities",
    "XIII": "Planning and Development",
    "XIV": "Taxation and Finance",
    "XV": "Homestead and Exemptions",
    "XVI": "Public Lands and Property",
    "XVII": "Military Affairs and Related Matters",
    "XVIII": "Public Lands and Property",
    "XIX": "Domestic Relations",
    "XX": "Motor Vehicles",
    "XXI": "Public Transportation",
    "XXII": "Ports and Harbors",
    "XXIII": "Motor Vehicles",
    "XXIV": "Vessels",
    "XXV": "Aviation",
    "XXVI": "Public Transportation",
    "XXVII": "Railroads and Other Regulated Utilities",
    "XXVIII": "Natural Resources; Conservation, Reclamation, and Use",
    "XXIX": "Public Health",
    "XXX": "Social Welfare",
    "XXXI": "Labor",
    "XXXII": "Regulation of Professions and Occupations",
    "XXXIII": "Regulation of Trade, Commerce, Investments, and Solicitations",
    "XXXIV": "Alcoholic Beverages and Tobacco",
    "XXXV": "Agriculture, Horticulture, and Animal Industry",
    "XXXVI": "Business Organizations",
    "XXXVII": "Insurance",
    "XXXVIII": "Banks and Banking",
    "XXXIX": "Commercial Relations",
    "XL": "Real and Personal Property",
    "XLI": "Statute of Frauds, Fraudulent Transfers, and General Assignments",
    "XLII": "Estates and Trusts",
    "XLIII": "Domestic Relations",
    "XLIV": "Civil Rights",
    "XLV": "Torts",
    "XLVI": "Crimes",
    "XLVII": "Criminal Procedure and Corrections",
    "XLVIII": "Early Learning-20 Education Code",
    "XLIX": "Florida Cultural Affairs Code",
}

# Key chapters for tax/benefit analysis
FL_TAX_CHAPTERS: dict[str, str] = {
    192: "Taxation: General Provisions",
    193: "Assessments",
    194: "Administrative and Judicial Review of Property Taxes",
    195: "Property Assessment Administration and Finance",
    196: "Exemption",
    197: "Tax Collections, Sales, and Liens",
    198: "Estate Taxes",
    199: "Intangible Personal Property Taxes (Repealed)",
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

FL_WELFARE_CHAPTERS: dict[str, str] = {
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
class ParsedFLSection:
    """Parsed Florida statute section."""

    section_number: str  # e.g., "220.02"
    section_title: str  # e.g., "Legislative intent"
    chapter_number: int  # e.g., 220
    chapter_title: str  # e.g., "Income Tax Code"
    title_roman: str | None  # e.g., "XIV"
    title_name: str | None  # e.g., "Taxation and Finance"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedFLSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedFLSubsection:
    """A subsection within a Florida statute."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list["ParsedFLSubsection"] = field(default_factory=list)


class FLConverterError(Exception):
    """Error during Florida statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class FLConverter:
    """Converter for Florida Statutes HTML to internal Section model.

    Example:
        >>> converter = FLConverter()
        >>> section = converter.fetch_section("220.02")
        >>> print(section.citation.section)
        "FL-220.02"

        >>> for section in converter.iter_chapter(220):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Florida statute converter.

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

    def _get_url_range(self, chapter: int) -> str:
        """Get the URL range folder for a chapter number.

        Florida uses 100-chapter ranges: 0000-0099, 0100-0199, 0200-0299, etc.
        """
        lower = (chapter // 100) * 100
        upper = lower + 99
        return f"{lower:04d}-{upper:04d}"

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "220.02", "212.05"

        Returns:
            Full URL to the section page
        """
        chapter = int(section_number.split(".")[0])
        suffix = section_number.split(".", 1)[1] if "." in section_number else section_number
        url_range = self._get_url_range(chapter)
        padded_chapter = f"{chapter:04d}"

        return (
            f"{BASE_URL}/index.cfm?App_mode=Display_Statute"
            f"&URL={url_range}/{padded_chapter}/Sections/{padded_chapter}.{suffix}.html"
        )

    def _build_chapter_contents_url(self, chapter: int) -> str:
        """Build the URL for a chapter's contents index."""
        url_range = self._get_url_range(chapter)
        padded = f"{chapter:04d}"
        return (
            f"{BASE_URL}/index.cfm?App_mode=Display_Statute"
            f"&URL={url_range}/{padded}/{padded}ContentsIndex.html"
        )

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedFLSection:
        """Parse section HTML into ParsedFLSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise FLConverterError(f"Section {section_number} not found", url)

        chapter = int(section_number.split(".")[0])
        chapter_title = (
            FL_TAX_CHAPTERS.get(chapter) or FL_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
        )

        # Extract section title from the heading pattern: "220.02 Legislative intent.--"
        section_title = ""
        # Pattern with trailing "--" delimiter
        title_pattern = re.compile(rf"{re.escape(section_number)}\s+(.+?)\.?\s*[-—]")
        # Fallback pattern without "--" (e.g., when HTML splits "220.02 Title." and "--")
        title_pattern_simple = re.compile(rf"{re.escape(section_number)}\s+([^.]+)")

        for text_node in soup.stripped_strings:
            match = title_pattern.search(text_node)
            if match:
                section_title = match.group(1).strip()  # pragma: no cover
                break  # pragma: no cover

        # Try simpler pattern if first pattern failed
        if not section_title:
            for text_node in soup.stripped_strings:
                match = title_pattern_simple.search(text_node)
                if match:
                    section_title = match.group(1).strip().rstrip(".")
                    break

        # Try to get title from h1/h2 if not found
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3"]):
                heading_text = heading.get_text(strip=True)
                match = title_pattern.search(heading_text)
                if match:
                    section_title = match.group(1).strip()  # pragma: no cover
                    break  # pragma: no cover

        # Determine title from chapter number
        title_roman = None
        title_name = None
        if 192 <= chapter <= 220:
            title_roman = "XIV"
            title_name = "Taxation and Finance"
        elif 409 <= chapter <= 430:  # pragma: no cover
            title_roman = "XXX"
            title_name = "Social Welfare"

        # Get body content - the actual statute content is in div.Section
        # The page includes multiple nested HTML documents, so we need to find the Section div
        content_elem = soup.find("div", class_="Section")

        if content_elem:  # pragma: no cover
            # Remove navigation and scripts
            for elem in content_elem.find_all(["nav", "script", "style", "header", "footer"]):
                elem.decompose()  # pragma: no cover

            # Extract section title from Catchline
            catchline = content_elem.find("span", class_="Catchline")
            if catchline:
                catchline_text = catchline.find("span", class_="CatchlineText")
                if catchline_text:
                    section_title = catchline_text.get_text(strip=True)

            # Get just the text content from SectionBody
            section_body = content_elem.find("span", class_="SectionBody")
            if section_body:
                text = self._extract_section_text(section_body)
                subsections = self._parse_subsections_from_html(section_body)
            else:
                text = content_elem.get_text(separator="\n", strip=True)  # pragma: no cover
                subsections = self._parse_subsections(text)  # pragma: no cover

            html_content = str(content_elem)
        else:
            # Fallback to original behavior for other formats
            text = soup.get_text(separator="\n", strip=True)
            html_content = html
            subsections = self._parse_subsections(text)

        # Extract history note (often in a History span or at the end)
        history = None
        history_elem = soup.find("span", class_="History") or soup.find("p", class_="History")
        if history_elem:
            history = history_elem.get_text(strip=True)[:1000]  # pragma: no cover
        else:
            history_match = re.search(r"History\.[-—](.+?)(?:\n|$)", text, re.DOTALL)
            if history_match:
                history = history_match.group(1).strip()[:1000]

        return ParsedFLSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            title_roman=title_roman,
            title_name=title_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _extract_section_text(self, section_body) -> str:  # pragma: no cover
        """Extract text from section body, handling the intro text before subsections."""
        # Get intro text (text before first subsection)
        intro_text = ""
        intro_elem = section_body.find("span", class_="Text")
        if intro_elem:
            intro_text = intro_elem.get_text(strip=True)

        return intro_text

    def _parse_subsections_from_html(
        self, section_body
    ) -> list[ParsedFLSubsection]:  # pragma: no cover
        """Parse subsections from the structured HTML with CSS classes.

        The HTML uses classes like Subsection, Paragraph, SubParagraph, etc.
        """
        subsections = []

        # Find top-level subsections (div.Subsection)
        for subsec_div in section_body.find_all("div", class_="Subsection", recursive=False):
            subsec = self._parse_subsection_div(subsec_div, 0)
            if subsec:
                subsections.append(subsec)

        return subsections

    def _parse_subsection_div(
        self, div, level: int
    ) -> ParsedFLSubsection | None:  # pragma: no cover
        """Parse a single subsection div recursively."""
        # Get the number/identifier
        num_elem = div.find("span", class_="Number")
        if not num_elem:
            return None  # pragma: no cover

        num_text = num_elem.get_text(strip=True)
        # Extract identifier from patterns like "(1)", "(a)", "1.", "a."
        match = re.match(r"\(?([a-zA-Z0-9]+)[\).\s]", num_text)
        if not match:
            return None

        identifier = match.group(1)

        # Get the text content
        text_elem = div.find("span", class_="Text")
        text = text_elem.get_text(strip=True) if text_elem else ""

        # Also check for direct content in content spans
        if not text:
            content_elem = div.find("span", class_="Content")  # pragma: no cover
            if content_elem:  # pragma: no cover
                text = content_elem.get_text(strip=True)  # pragma: no cover

        # Parse children based on level
        children = []
        child_classes = {
            0: "Paragraph",
            1: "SubParagraph",
            2: "SubSubParagraph",
            3: "SubSubSubParagraph",
        }

        child_class = child_classes.get(level, "SubSubSubParagraph")
        for child_div in div.find_all("div", class_=child_class, recursive=False):
            child = self._parse_subsection_div(child_div, level + 1)
            if child:
                children.append(child)

        return ParsedFLSubsection(
            identifier=identifier,
            text=text[:2000],  # Limit text size
            children=children,
        )

    def _parse_subsections(self, text: str) -> list[ParsedFLSubsection]:
        """Parse hierarchical subsections from text.

        Florida statutes typically use:
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
                ParsedFLSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedFLSubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedFLSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedFLSection) -> Section:
        """Convert ParsedFLSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"FL-{parsed.section_number}",
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
                        children=[],
                    )
                    for child in sub.children
                ],
            )
            for sub in parsed.subsections
        ]

        return Section(
            citation=citation,
            title_name=f"Florida Statutes - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"fl/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "220.02", "212.05"

        Returns:
            Section model

        Raises:
            FLConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., 220)

        Returns:
            List of section numbers (e.g., ["220.02", "220.03", ...])
        """
        url = self._build_chapter_contents_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []
        padded = f"{chapter:04d}"

        # Find section links: Sections/0220.02.html
        pattern = re.compile(rf"Sections/{padded}\.(\d+[A-Za-z]?)\.html")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = f"{chapter}.{match.group(1)}"
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 220)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except FLConverterError as e:  # pragma: no cover
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
            chapters = list(FL_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "FLConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_fl_section(section_number: str) -> Section:
    """Fetch a single Florida statute section.

    Args:
        section_number: e.g., "220.02"

    Returns:
        Section model
    """
    with FLConverter() as converter:
        return converter.fetch_section(section_number)


def download_fl_chapter(chapter: int) -> list[Section]:
    """Download all sections from a Florida Statutes chapter.

    Args:
        chapter: Chapter number (e.g., 220)

    Returns:
        List of Section objects
    """
    with FLConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_fl_tax_chapters() -> Iterator[Section]:
    """Download all sections from Florida tax-related chapters (192-220).

    Yields:
        Section objects
    """
    with FLConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(FL_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_fl_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Florida social welfare chapters (409-430).

    Yields:
        Section objects
    """
    with FLConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(FL_WELFARE_CHAPTERS.keys()))  # pragma: no cover
