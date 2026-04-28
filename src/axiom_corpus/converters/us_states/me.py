"""Maine state statute converter.

Converts Maine Revised Statutes HTML from legislature.maine.gov to the
internal Section model for ingestion.

Maine Statute Structure:
- Titles (e.g., Title 36: Taxation)
- Parts (e.g., Part 8: Income Taxes)
- Chapters (e.g., Chapter 822: Tax Credits)
- Sections (e.g., Section 5219-S: Earned income credit)

URL Patterns:
- Title index: /statutes/36/title36ch0sec0.html
- Chapter index: /statutes/36/title36ch822sec0.html
- Section: /statutes/36/title36sec5219-S.html

Maine uses numbered paragraphs (1, 2, 3) and lettered sub-paragraphs (A, B, C).
Some sections use suffixed numbers (1-A, 1-B, 2-A).

Example:
    >>> from axiom_corpus.converters.us_states.me import MEConverter
    >>> converter = MEConverter()
    >>> section = converter.fetch_section(36, "5219-S")
    >>> print(section.section_title)
    "Earned income credit"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://legislature.maine.gov/statutes"

# Maine Title mapping for reference
ME_TITLES: dict[str, str] = {
    1: "General Provisions",
    2: "Legislature",
    3: "Governor",
    4: "Judiciary",
    5: "Administrative Procedures and Services",
    7: "Agriculture and Animals",
    9: "Banks and Financial Institutions",
    10: "Commerce and Trade",
    11: "Uniform Commercial Code",
    12: "Conservation",
    13: "Corporations",
    14: "Court Procedure -- Civil",
    15: "Court Procedure -- Criminal",
    17: "Crimes",
    18: "Decedents' Estates and Fiduciary Relations",
    19: "Domestic Relations",
    20: "Education",
    21: "Elections",
    22: "Health and Welfare",
    23: "Highways",
    24: "Insurance",
    25: "Internal Security and Public Safety",
    26: "Labor and Industry",
    27: "Libraries, History, Culture and Art",
    28: "Liquor",
    29: "Motor Vehicles",
    30: "Municipalities and Counties",
    31: "Partnerships and Associations",
    32: "Professions and Occupations",
    33: "Property",
    34: "Public Utilities",
    35: "State Government",
    36: "Taxation",
    37: "Veterans",
    38: "Waters and Navigation",
    39: "Workers' Compensation",
}

# Key chapters for tax analysis (Title 36)
ME_TAX_CHAPTERS: dict[str, str] = {
    1: "Taxpayers' Rights",
    3: "State Tax Assessor",
    5: "General Administrative Provisions",
    7: "Uniform Administrative Provisions",
    9: "Taxpayer Relief",
    11: "Maine Tax Court",
    101: "General Provisions",
    105: "Assessment",
    111: "Exemptions",
    112: "Assessment of Certain Homesteads",
    115: "Tax Expenditure Reporting",
    211: "General Provisions",
    212: "Imposition of Tax",
    213: "Specific Exemptions",
    219: "Collection and Enforcement",
    225: "Refunds",
    351: "Franchise Tax",
    355: "Service Provider Tax",
    357: "Electrical Energy Excise Tax",
    358: "Commercial Forestry Excise Tax",
    361: "Real Estate Transfer Tax",
    369: "Insurance Premium Tax",
    375: "Telecommunications and Cable Television Excise Tax",
    377: "Tobacco Taxes",
    451: "General Provisions",
    453: "Licensing",
    455: "Inspection and Enforcement",
    457: "Reports and Records",
    459: "Fuel Taxes; Rates",
    461: "Fuel Tax Credits and Refunds",
    551: "Inheritance Tax (Repealed)",
    575: "Estate Tax",
    577: "Generation-Skipping Transfer Tax",
    701: "Pari-mutuel Pools and Racing (Repealed)",
    703: "Games of Chance",
    711: "Blueberry Tax",
    713: "Potato Tax",
    717: "Sardine Tax",
    719: "Telecommunications Tax (Repealed)",
    721: "Tax on Slaughtering Facilities",
    723: "Tax on Mining or Excavating on State Land",
    725: "Milk Handling Fee",
    801: "Definitions",
    803: "Imposition of Tax",
    805: "Adjusted Gross Income",
    806: "Maine Taxable Income",
    807: "Taxable Income of Trusts",
    811: "Returns, Declarations, Records and Payments",
    817: "Assessments, Penalties, Liens, Collections",
    819: "Refunds",
    821: "Withholding of Tax",
    822: "Tax Credits",
    823: "Tax Credits for Corporations",
    825: "Tax Credits for Partnerships, S Corporations and Limited Liability Companies",
    827: "Maine Capital Investment Credit",
    829: "Research Expense Tax Credit",
    831: "Fiduciary Adjustment",
    833: "Nonresident and Part-year Resident Taxpayers",
    835: "Composite Return for Electing Partnerships",
    837: "Estimated Tax Returns and Payments",
    839: "Administration",
    841: "Confidentiality of Tax Records",
    901: "Maine Residents Property Tax and Rent Refund Program",
    907: "Property Tax Deferral for Residents 65 and Older",
    913: "Property Tax Fairness Credit",
    917: "Sales Tax Fairness Credit",
    920: "Multistate Tax Compact",
    921: "New England Interstate Corrections Compact",
    931: "State Tax Policy Goals",
}

# Key chapters for welfare analysis (Title 22)
ME_WELFARE_CHAPTERS: dict[str, str] = {
    1: "Department of Health and Human Services",
    101: "General Provisions",
    401: "Tuberculosis",
    403: "Cancer Control",
    405: "Other Communicable Diseases",
    409: "Lead Poisoning Control",
    411: "Radiation",
    451: "Board of Licensure in Medicine",
    853: "Aid to Aged Persons",
    855: "Aid to Needy Persons",
    951: "Services for Aged Persons",
    1053: "Aid to Dependent Children",
    1161: "General Assistance",
    1251: "Municipal General Assistance",
}

# Special sections of interest for policy encoding
ME_POLICY_SECTIONS: dict[str, str] = {
    "36-5219-S": "Earned Income Credit",
    "36-5219-KK": "Child Tax Credit",
    "36-913": "Property Tax Fairness Credit",
    "36-917": "Sales Tax Fairness Credit",
    "22-3762": "TANF Program",
    "22-3104": "MaineCare (Medicaid)",
}


@dataclass
class ParsedMESection:
    """Parsed Maine statute section."""

    title: int  # e.g., 36
    section_number: str  # e.g., "5219-S"
    section_title: str  # e.g., "Earned income credit"
    chapter_number: int | None  # e.g., 822
    chapter_title: str | None  # e.g., "Tax Credits"
    part_number: int | None  # e.g., 8
    part_title: str | None  # e.g., "Income Taxes"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedMESubsection"] = field(default_factory=list)
    history: str | None = None  # Section history
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedMESubsection:
    """A subsection within a Maine statute."""

    identifier: str  # e.g., "1", "1-A", "A", "a"
    heading: str | None  # Optional heading text
    text: str
    children: list["ParsedMESubsection"] = field(default_factory=list)


class MEConverterError(Exception):
    """Error during Maine statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class MEConverter:
    """Converter for Maine Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = MEConverter()
        >>> section = converter.fetch_section(36, "5219-S")
        >>> print(section.citation.section)
        "ME-36-5219-S"

        >>> for section in converter.iter_chapter(36, 822):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the Maine statute converter.

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

    def _build_section_url(self, title: int, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            title: Title number (e.g., 36)
            section_number: Section number (e.g., "5219-S", "5102")

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/{title}/title{title}sec{section_number}.html"

    def _build_chapter_index_url(self, title: int, chapter: int) -> str:
        """Build the URL for a chapter's index page."""
        return f"{BASE_URL}/{title}/title{title}ch{chapter}sec0.html"

    def _build_title_index_url(self, title: int) -> str:
        """Build the URL for a title's index page."""
        return f"{BASE_URL}/{title}/title{title}ch0sec0.html"

    def _parse_section_html(
        self,
        html: str,
        title: int,
        section_number: str,
        url: str,
    ) -> ParsedMESection:
        """Parse section HTML into ParsedMESection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" or empty content
        body_text = soup.get_text().lower()
        if "not found" in body_text or "404" in body_text:
            raise MEConverterError(f"Section {title} MRS {section_number} not found", url)

        # Check if section is repealed
        if "(repealed)" in body_text and len(body_text.strip()) < 500:
            # Very short page with just "(REPEALED)" indicator
            raise MEConverterError(f"Section {title} MRS {section_number} has been repealed", url)

        # Extract section title from heading - usually in h3 or similar
        section_title = ""
        title_heading = None

        # Look for pattern like "5219-S. Earned income credit"
        for heading in soup.find_all(["h1", "h2", "h3", "h4", "b", "strong"]):
            heading_text = heading.get_text(strip=True)
            # Match patterns like "5219-S. Title" or "Section 5219-S. Title"
            pattern = rf"(?:Section\s*)?(?:§\s*)?{re.escape(section_number)}\.?\s+(.+?)(?:\s*\[|$)"
            match = re.search(pattern, heading_text, re.IGNORECASE)
            if match:
                section_title = match.group(1).strip()
                title_heading = heading
                break

        # Fallback: try to find the section number anywhere
        if not section_title:
            for text in soup.stripped_strings:
                pattern = rf"(?:§\s*)?{re.escape(section_number)}\.?\s+(.+?)(?:\s*\[|$)"
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    section_title = match.group(1).strip()  # pragma: no cover
                    break  # pragma: no cover

        # Get title info
        title_name = ME_TITLES.get(title, f"Title {title}")

        # Try to extract chapter info from navigation or breadcrumbs
        chapter_number = None
        chapter_title = None
        part_number = None
        part_title = None

        # Look for chapter info in the page
        for link in soup.find_all("a"):
            href = link.get("href", "")
            link_text = link.get_text(strip=True)
            # Match "Chapter 822" pattern
            if "ch" in href.lower():
                ch_match = re.search(r"ch(\d+)", href.lower())
                if ch_match:
                    chapter_number = int(ch_match.group(1))
                    if title == 36:
                        chapter_title = ME_TAX_CHAPTERS.get(chapter_number)
                    elif title == 22:  # pragma: no cover
                        chapter_title = ME_WELFARE_CHAPTERS.get(chapter_number)
                    break

        # Get body content - find main content area
        # Maine pages typically have the content in the body without specific containers
        content_elem = soup.find("body")
        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(
                ["nav", "script", "style", "header", "footer"]
            ):  # pragma: no cover
                elem.decompose()

            # Get text content
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note - look for "SECTION HISTORY" section
        history = None
        history_match = re.search(
            r"SECTION\s+HISTORY[:\s]*\n*(.+?)(?:\n\n|\Z)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if history_match:
            history = history_match.group(1).strip()[:2000]  # Limit length
        else:
            # Alternative pattern for inline history
            history_match = re.search(  # pragma: no cover
                r"(?:PL|RR)\s+\d{4},\s+c\.\s+\d+.+?(?:\((?:NEW|AMD|RPR|RP)\))",
                text,
                re.IGNORECASE,
            )
            if history_match:  # pragma: no cover
                # Find the full history block
                start = history_match.start()  # pragma: no cover
                history_block = text[start:]  # pragma: no cover
                # Find the end - usually a double newline or end of content
                end_match = re.search(r"\n\n", history_block)  # pragma: no cover
                if end_match:  # pragma: no cover
                    history = history_block[: end_match.start()].strip()[:2000]  # pragma: no cover
                else:
                    history = history_block.strip()[:2000]  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedMESection(
            title=title,
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter_number,
            chapter_title=chapter_title,
            part_number=part_number,
            part_title=part_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedMESubsection]:
        """Parse hierarchical subsections from text.

        Maine statutes typically use:
        - 1., 2., 3. for primary divisions (sometimes 1-A, 1-B)
        - A., B., C. for secondary divisions (uppercase)
        - (1), (2), (3) for tertiary
        - (a), (b), (c) for quaternary
        """
        subsections = []

        # Split by top-level subsections: numbered like "1.", "2.", "1-A."
        # Pattern matches: "1. ", "2. ", "1-A. ", etc. at start of line or after newline
        parts = re.split(r"(?:^|\n)(\d+(?:-[A-Z]+)?)\.\s+", text)

        # parts will be: [intro, "1", content1, "2", content2, ...]
        if len(parts) > 1:
            for i in range(1, len(parts), 2):
                if i + 1 >= len(parts):
                    break  # pragma: no cover
                identifier = parts[i]
                content = parts[i + 1]

                # Look for heading - text before first period or sentence
                heading = None
                heading_match = re.match(r"([A-Z][^.]+\.)\s*", content)
                if heading_match:
                    potential_heading = heading_match.group(1).rstrip(".")
                    # Only use as heading if it's short enough
                    if len(potential_heading) < 100:
                        heading = potential_heading

                # Parse second-level children (A., B., C.)
                children = self._parse_level2(content)

                # Get text before first child
                if children:
                    first_child_match = re.search(r"\n[A-Z]\.\s+", content)
                    direct_text = (
                        content[: first_child_match.start()].strip()
                        if first_child_match
                        else content.strip()
                    )
                else:
                    direct_text = content.strip()

                # Clean up text - remove following numbered subsections
                next_subsection = re.search(r"\n\d+(?:-[A-Z]+)?\.\s+", direct_text)
                if next_subsection:
                    direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

                subsections.append(
                    ParsedMESubsection(
                        identifier=identifier,
                        heading=heading,
                        text=direct_text[:2000],
                        children=children,
                    )
                )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedMESubsection]:
        """Parse level 2 subsections (A., B., C.)."""
        subsections = []

        # Split by uppercase letter markers: "A. ", "B. ", etc.
        parts = re.split(r"(?:^|\n)([A-Z])\.\s+", text)

        if len(parts) > 1:
            for i in range(1, len(parts), 2):
                if i + 1 >= len(parts):
                    break  # pragma: no cover
                identifier = parts[i]
                content = parts[i + 1]

                # Parse level 3 children (1), (2), etc.
                children = self._parse_level3(content)

                # Get direct text
                if children:
                    first_child = re.search(r"\(\d+\)", content)
                    direct_text = (
                        content[: first_child.start()].strip() if first_child else content.strip()
                    )
                else:
                    direct_text = content.strip()

                # Stop at next uppercase letter marker
                next_marker = re.search(r"\n[A-Z]\.\s+", direct_text)
                if next_marker:
                    direct_text = direct_text[: next_marker.start()].strip()  # pragma: no cover

                subsections.append(
                    ParsedMESubsection(
                        identifier=identifier,
                        heading=None,
                        text=direct_text[:1500],
                        children=children,
                    )
                )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedMESubsection]:
        """Parse level 3 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"\((\d+)\)\s*", text)

        if len(parts) > 1:
            for i in range(1, len(parts), 2):
                if i + 1 >= len(parts):
                    break  # pragma: no cover
                identifier = parts[i]
                content = parts[i + 1]

                # Get direct text - limit size
                direct_text = content.strip()
                next_num = re.search(r"\(\d+\)", direct_text)
                if next_num:
                    direct_text = direct_text[: next_num.start()].strip()  # pragma: no cover

                subsections.append(
                    ParsedMESubsection(
                        identifier=identifier,
                        heading=None,
                        text=direct_text[:1000],
                        children=[],
                    )
                )

        return subsections

    def _to_section(self, parsed: ParsedMESection) -> Section:
        """Convert ParsedMESection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"ME-{parsed.title}-{parsed.section_number}",
        )

        # Convert subsections
        def convert_subsection(sub: ParsedMESubsection) -> Subsection:
            return Subsection(
                identifier=sub.identifier,
                heading=sub.heading,
                text=sub.text,
                children=[convert_subsection(child) for child in sub.children],
            )

        subsections = [convert_subsection(sub) for sub in parsed.subsections]

        title_name = ME_TITLES.get(parsed.title, f"Title {parsed.title}")

        return Section(
            citation=citation,
            title_name=f"Maine Revised Statutes - Title {parsed.title}: {title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"me/{parsed.title}/{parsed.section_number}",
        )

    def fetch_section(self, title: int, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            title: Title number (e.g., 36 for Taxation)
            section_number: Section number (e.g., "5219-S", "5102")

        Returns:
            Section model

        Raises:
            MEConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(title, section_number)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            if e.response.status_code == 404:  # pragma: no cover
                raise MEConverterError(
                    f"Section {title} MRS {section_number} not found", url
                )  # pragma: no cover
            raise  # pragma: no cover
        parsed = self._parse_section_html(html, title, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(self, title: int, chapter: int) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 36)
            chapter: Chapter number (e.g., 822)

        Returns:
            List of section numbers (e.g., ["5219", "5219-A", "5219-S"])
        """
        url = self._build_chapter_index_url(title, chapter)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError:  # pragma: no cover
            return []  # pragma: no cover

        soup = BeautifulSoup(html, "html.parser")
        section_numbers = []

        # Look for links matching pattern: title36sec5219.html or title36sec5219-S.html
        pattern = re.compile(rf"title{title}sec([^.]+)\.html")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers and section_num != "0":
                    section_numbers.append(section_num)

        return section_numbers

    def iter_chapter(self, title: int, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 36)
            chapter: Chapter number (e.g., 822)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(title, section_num)
            except MEConverterError as e:
                # Log but continue with other sections
                print(f"Warning: Could not fetch {title} MRS {section_num}: {e}")
                continue

    def iter_title_chapters(
        self,
        title: int,
        chapters: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            title: Title number (e.g., 36)
            chapters: List of chapter numbers (default: all known chapters for title)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            if title == 36:  # pragma: no cover
                chapters = list(ME_TAX_CHAPTERS.keys())  # pragma: no cover
            elif title == 22:  # pragma: no cover
                chapters = list(ME_WELFARE_CHAPTERS.keys())  # pragma: no cover
            else:
                chapters = []  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "MEConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_me_section(title: int, section_number: str) -> Section:
    """Fetch a single Maine statute section.

    Args:
        title: Title number (e.g., 36)
        section_number: Section number (e.g., "5219-S")

    Returns:
        Section model
    """
    with MEConverter() as converter:
        return converter.fetch_section(title, section_number)


def download_me_chapter(title: int, chapter: int) -> list[Section]:
    """Download all sections from a Maine Revised Statutes chapter.

    Args:
        title: Title number (e.g., 36)
        chapter: Chapter number (e.g., 822)

    Returns:
        List of Section objects
    """
    with MEConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_me_tax_chapters() -> Iterator[Section]:
    """Download all sections from Maine tax-related chapters (Title 36).

    Yields:
        Section objects
    """
    with MEConverter() as converter:  # pragma: no cover
        yield from converter.iter_title_chapters(
            36, list(ME_TAX_CHAPTERS.keys())
        )  # pragma: no cover


def download_me_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Maine welfare chapters (Title 22).

    Yields:
        Section objects
    """
    with MEConverter() as converter:  # pragma: no cover
        yield from converter.iter_title_chapters(
            22, list(ME_WELFARE_CHAPTERS.keys())
        )  # pragma: no cover
