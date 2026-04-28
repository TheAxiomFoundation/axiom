"""Utah state statute converter.

Converts Utah Code HTML from le.utah.gov (Utah State Legislature)
to the internal Section model for ingestion.

Utah Code Structure:
- Titles (e.g., Title 59: Revenue and Taxation)
- Chapters (e.g., Chapter 10: Individual Income Tax Act)
- Parts (e.g., Part 1: Determination and Reporting of Tax Liability)
- Sections (e.g., 59-10-104: Tax basis -- Tax rate -- Exemption)

URL Patterns:
- Title: xcode/Title{N}/{N}.html
- Chapter: xcode/Title{N}/Chapter{X}/{N}-{X}.html
- Part: xcode/Title{N}/Chapter{X}/{N}-{X}-P{Y}.html
- Section (wrapper): xcode/Title{N}/Chapter{X}/{N}-{X}-S{Z}.html
- Section (content): xcode/Title{N}/Chapter{X}/C{N}-{X}-S{Z}_{version}.html

Note: Utah has a flat 4.5% state income tax (as of 2025).

Example:
    >>> from axiom.converters.us_states.ut import UTConverter
    >>> converter = UTConverter()
    >>> section = converter.fetch_section("59-10-104")
    >>> print(section.section_title)
    "Tax basis -- Tax rate -- Exemption."
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup, Tag

from axiom.models import Citation, Section, Subsection

BASE_URL = "https://le.utah.gov/xcode"

# Title mapping for reference
UT_TITLES: dict[int, str] = {
    35: "Utah Labor Code",
    59: "Revenue and Taxation",
    62: "Utah Human Services Code",  # Note: 35A is Workforce Services
    63: "Utah State Government Code",
    78: "Utah Code of Judicial Administration",
}

# Special alphanumeric titles (like 35A)
UT_ALPHA_TITLES: dict[str, str] = {
    "35A": "Utah Workforce Services Code",
    "17B": "Limited Purpose Local Government Entities",
    "17C": "Community Development and Renewal Agencies Act",
    "17D": "Local District Act",
    "26B": "Utah Health Code",
    "53B": "Utah System of Higher Education",
    "53E": "Utah Board of State Education",
    "53F": "Utah Education Funding",
    "53G": "Utah Education Code",
    "63A": "Utah Administrative Services Code",
    "63C": "Independent Entities Code",
    "63G": "Utah General Government Code",
    "63H": "Utah State Economic Development Act",
    "63I": "Utah Regulatory Sandbox Act",
    "63J": "Utah Budgetary Procedures Act",
    "63K": "Utah Emergency Management Act",
    "63L": "Utah Federal Land Management Act",
    "63M": "Utah Science Technology and Research Act",
    "63N": "Utah Economic Development Act",
}

# Key chapters for tax/benefit analysis
UT_TAX_CHAPTERS: dict[str, str] = {
    "59-1": "General Taxation Policies",
    "59-2": "Property Tax Act",
    "59-7": "Corporate Franchise and Income Taxes",
    "59-10": "Individual Income Tax Act",
    "59-12": "Sales and Use Tax Act",
    "59-13": "Motor and Special Fuel Tax",
    "59-14": "Tobacco Products",
    "59-15": "Cigarette Tax",
}

UT_WELFARE_CHAPTERS: dict[str, str] = {
    "35A-1": "Department of Workforce Services",
    "35A-3": "Employment Support Act",
    "35A-4": "Employment Security Act",
    "35A-8": "Housing and Community Development Division",
    "35A-9": "Intergenerational Poverty Mitigation Act",
    "35A-16": "Office of Homeless Services",
    "35A-17": "SNAP Benefits Waiver",
}


@dataclass
class ParsedUTSection:
    """Parsed Utah statute section."""

    section_number: str  # e.g., "59-10-104"
    section_title: str  # e.g., "Tax basis -- Tax rate -- Exemption."
    title_number: str  # e.g., "59" or "35A"
    title_name: str  # e.g., "Revenue and Taxation"
    chapter_number: str  # e.g., "10"
    chapter_title: str | None  # e.g., "Individual Income Tax Act"
    part_number: str | None  # e.g., "1"
    part_title: str | None  # e.g., "Determination and Reporting..."
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedUTSubsection"] = field(default_factory=list)
    history: str | None = None  # History/amendment note
    source_url: str = ""
    effective_date: date | None = None
    version: str | None = None  # Version string from URL


@dataclass
class ParsedUTSubsection:
    """A subsection within a Utah statute."""

    identifier: str  # e.g., "1", "a", "i"
    text: str
    children: list["ParsedUTSubsection"] = field(default_factory=list)


class UTConverterError(Exception):
    """Error during Utah statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class UTConverter:
    """Converter for Utah Code HTML to internal Section model.

    Example:
        >>> converter = UTConverter()
        >>> section = converter.fetch_section("59-10-104")
        >>> print(section.citation.section)
        "UT-59-10-104"

        >>> for section in converter.iter_chapter("59", "10"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the Utah statute converter.

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

    def _parse_section_number(self, section_number: str) -> tuple[str, str, str]:
        """Parse section number into title, chapter, section parts.

        Args:
            section_number: e.g., "59-10-104", "35A-3-302"

        Returns:
            Tuple of (title, chapter, section) e.g., ("59", "10", "104")
        """
        # Handle alphanumeric titles like "35A"
        match = re.match(r"^(\d+[A-Z]?)-(\d+)-(.+)$", section_number)
        if not match:
            raise UTConverterError(f"Invalid section number format: {section_number}")
        return match.group(1), match.group(2), match.group(3)

    def _build_section_wrapper_url(self, section_number: str) -> str:
        """Build the URL for a section wrapper page.

        Args:
            section_number: e.g., "59-10-104", "35A-3-302"

        Returns:
            Full URL to the section wrapper page
        """
        title, chapter, section = self._parse_section_number(section_number)
        return f"{BASE_URL}/Title{title}/Chapter{chapter}/{title}-{chapter}-S{section}.html"

    def _build_section_content_url(self, section_number: str, version: str | None = None) -> str:
        """Build the URL for actual section content.

        Args:
            section_number: e.g., "59-10-104"
            version: Version string (e.g., "2025010120250507")

        Returns:
            Full URL to the section content page
        """
        title, chapter, section = self._parse_section_number(section_number)
        if version:
            return f"{BASE_URL}/Title{title}/Chapter{chapter}/C{title}-{chapter}-S{section}_{version}.html"
        # Default version format - current version
        return f"{BASE_URL}/Title{title}/Chapter{chapter}/C{title}-{chapter}-S{section}_1800010118000101.html"

    def _build_part_url(self, title: str, chapter: str, part: str) -> str:
        """Build the URL for a part index page."""
        return f"{BASE_URL}/Title{title}/Chapter{chapter}/C{title}-{chapter}-P{part}_1800010118000101.html"

    def _build_chapter_url(self, title: str, chapter: str) -> str:
        """Build the URL for a chapter index page."""
        return f"{BASE_URL}/Title{title}/Chapter{chapter}/C{title}-{chapter}_1800010118000101.html"

    def _get_current_version(self, wrapper_html: str) -> str | None:
        """Extract current version string from wrapper page JavaScript.

        The wrapper page contains JavaScript like:
        var versionArr = [['C59-10-S104_2025010120250507.html','Current Version',...]]
        """
        match = re.search(r"versionDefault\s*=\s*['\"]C[^_]+_(\d+)['\"]", wrapper_html)
        if match:
            return match.group(1)
        # Fallback: try to find in versionArr
        match = re.search(
            r"\['C[^_]+_(\d+)\.html',\s*'Current Version'", wrapper_html
        )  # pragma: no cover
        if match:  # pragma: no cover
            return match.group(1)  # pragma: no cover
        return None  # pragma: no cover

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
        version: str | None = None,
    ) -> ParsedUTSection:
        """Parse section HTML into ParsedUTSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise UTConverterError(f"Section {section_number} not found", url)  # pragma: no cover

        title, chapter, section = self._parse_section_number(section_number)

        # Get title name
        if title in UT_ALPHA_TITLES:
            title_name = UT_ALPHA_TITLES[title]
        elif title.isdigit() and int(title) in UT_TITLES:
            title_name = UT_TITLES[int(title)]
        else:
            title_name = f"Title {title}"  # pragma: no cover

        # Get chapter/part titles from parent table
        parent_table = soup.find("table", id="parenttbl")
        chapter_title = None
        part_number = None
        part_title = None

        if parent_table:
            rows = parent_table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    if label.startswith("Chapter"):
                        chapter_title = value
                    elif label.startswith("Part"):
                        part_title = value
                        part_match = re.search(r"Part\s+(\d+)", label)
                        if part_match:
                            part_number = part_match.group(1)

        # Extract section title from the section div
        section_title = ""
        secdiv = soup.find("div", id="secdiv")

        if secdiv:
            # Look for bold elements containing section title
            for bold in secdiv.find_all("b"):
                bold_text = bold.get_text(strip=True)
                # Skip "Effective" date text
                if bold_text.startswith("Effective"):
                    continue
                # Skip section number itself
                if bold_text.startswith(section_number):
                    continue
                # This should be the title
                if bold_text and not bold_text.startswith("("):
                    section_title = bold_text
                    break

        # If not found, try pattern matching
        if not section_title:
            text_content = soup.get_text()  # pragma: no cover
            # Pattern: 59-10-104. Title text here.
            pattern = (
                rf"{re.escape(section_number)}\.\s*([^.]+(?:\s+--\s+[^.]+)*)"  # pragma: no cover
            )
            match = re.search(pattern, text_content)  # pragma: no cover
            if match:  # pragma: no cover
                section_title = match.group(1).strip()  # pragma: no cover

        # Parse effective date
        effective_date = None
        if secdiv:
            effective_text = secdiv.get_text()
            eff_match = re.search(r"Effective\s+(\d{1,2}/\d{1,2}/\d{4})", effective_text)
            if eff_match:
                try:
                    effective_date = date(
                        int(eff_match.group(1).split("/")[2]),
                        int(eff_match.group(1).split("/")[0]),
                        int(eff_match.group(1).split("/")[1]),
                    )
                except ValueError, IndexError:  # pragma: no cover
                    pass

        # Get content
        content_elem = secdiv or soup.find("div", id="content") or soup.find("body")

        if content_elem:
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history note
        history = None
        history_match = re.search(
            r"(?:Amended|Enacted|Repealed|Renumbered)\s+by\s+Chapter\s+.+",
            text,
            re.DOTALL,
        )
        if history_match:
            history = history_match.group(0).strip()[:1000]  # Limit length

        # Parse subsections from tables
        subsections = self._parse_subsections(secdiv) if secdiv else []

        return ParsedUTSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title,
            title_name=title_name,
            chapter_number=chapter,
            chapter_title=chapter_title,
            part_number=part_number,
            part_title=part_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
            effective_date=effective_date,
            version=version,
        )

    def _parse_subsections(self, secdiv: Tag) -> list[ParsedUTSubsection]:
        """Parse hierarchical subsections from section div.

        Utah statutes use nested tables for subsections:
        - (1), (2), (3) for primary divisions
        - (a), (b), (c) for secondary divisions
        - (i), (ii), (iii) for tertiary divisions
        """
        subsections = []

        # Find top-level subsection anchors
        # Utah uses anchors like: <a id="59-10-104(1)" name="59-10-104(1)"></a>
        top_level_pattern = re.compile(r"^[\d\w-]+\((\d+)\)$")

        # Get all top-level tables (direct children of secdiv)
        for anchor in secdiv.find_all("a", id=top_level_pattern):
            anchor_id = anchor.get("id", "")
            match = top_level_pattern.search(anchor_id)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)

            # Find the following table
            table = anchor.find_next("table")
            if not table:
                continue  # pragma: no cover

            # Get the text from the second cell
            cells = table.find_all("td")
            if len(cells) < 2:
                continue  # pragma: no cover

            # First cell is identifier "(1)", second is content
            content_cell = cells[1]
            text = self._get_direct_text(content_cell)

            # Parse child subsections (a), (b), etc.
            children = self._parse_child_subsections(content_cell, anchor_id)

            subsections.append(
                ParsedUTSubsection(
                    identifier=identifier,
                    text=text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_child_subsections(
        self, parent_cell: Tag, parent_anchor_base: str
    ) -> list[ParsedUTSubsection]:
        """Parse child subsections from a parent cell."""
        children = []

        # Look for child anchors like 59-10-104(1)(a)
        child_pattern = re.compile(rf"^{re.escape(parent_anchor_base)}\(([a-z])\)$")

        for anchor in parent_cell.find_all("a", id=child_pattern):
            anchor_id = anchor.get("id", "")
            match = child_pattern.search(anchor_id)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)

            # Find the following table
            table = anchor.find_next("table")
            if not table:
                continue  # pragma: no cover

            cells = table.find_all("td")
            if len(cells) < 2:
                continue  # pragma: no cover

            content_cell = cells[1]
            text = self._get_direct_text(content_cell)

            # Parse grandchild subsections (i), (ii), etc.
            grandchildren = self._parse_grandchild_subsections(content_cell, anchor_id)

            children.append(
                ParsedUTSubsection(
                    identifier=identifier,
                    text=text[:2000],
                    children=grandchildren,
                )
            )

        return children

    def _parse_grandchild_subsections(
        self, parent_cell: Tag, parent_anchor_base: str
    ) -> list[ParsedUTSubsection]:
        """Parse grandchild subsections (roman numerals) from a parent cell."""
        grandchildren = []

        # Look for anchors like 59-10-104(1)(a)(i)
        grandchild_pattern = re.compile(rf"^{re.escape(parent_anchor_base)}\(([ivxlcdm]+)\)$")

        for anchor in parent_cell.find_all("a", id=grandchild_pattern):  # pragma: no cover
            anchor_id = anchor.get("id", "")
            match = grandchild_pattern.search(anchor_id)
            if not match:
                continue

            identifier = match.group(1)

            table = anchor.find_next("table")
            if not table:
                continue

            cells = table.find_all("td")
            if len(cells) < 2:
                continue

            text = self._get_direct_text(cells[1])

            grandchildren.append(
                ParsedUTSubsection(
                    identifier=identifier,
                    text=text[:2000],
                    children=[],
                )
            )

        return grandchildren

    def _get_direct_text(self, element: Tag) -> str:
        """Get text content excluding nested tables."""
        text_parts = []
        for child in element.children:
            if isinstance(child, str):
                text_parts.append(child.strip())
            elif hasattr(child, "name") and child.name not in ("table", "a"):
                text_parts.append(child.get_text(strip=True))  # pragma: no cover
        return " ".join(filter(None, text_parts))

    def _to_section(self, parsed: ParsedUTSection) -> Section:
        """Convert ParsedUTSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"UT-{parsed.section_number}",
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
                                identifier=gc.identifier,
                                heading=None,
                                text=gc.text,
                                children=[],
                            )
                            for gc in child.children
                        ],
                    )
                    for child in sub.children
                ],
            )
            for sub in parsed.subsections
        ]

        return Section(
            citation=citation,
            title_name=f"Utah Code - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ut/{parsed.title_number}/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "59-10-104", "35A-3-302"

        Returns:
            Section model

        Raises:
            UTConverterError: If section not found or parsing fails
        """
        # First fetch wrapper page to get current version
        wrapper_url = self._build_section_wrapper_url(section_number)
        try:
            wrapper_html = self._get(wrapper_url)
        except httpx.HTTPStatusError as e:
            raise UTConverterError(f"Section {section_number} not found: {e}", wrapper_url)

        version = self._get_current_version(wrapper_html)

        # Fetch actual content
        content_url = self._build_section_content_url(section_number, version)
        try:
            content_html = self._get(content_url)
        except httpx.HTTPStatusError:  # pragma: no cover
            # Fall back to default version if specific version fails
            content_url = self._build_section_content_url(section_number, None)  # pragma: no cover
            content_html = self._get(content_url)  # pragma: no cover

        parsed = self._parse_section_html(content_html, section_number, content_url, version)
        return self._to_section(parsed)

    def get_part_section_numbers(self, title: str, chapter: str, part: str) -> list[str]:
        """Get list of section numbers in a part.

        Args:
            title: Title number (e.g., "59", "35A")
            chapter: Chapter number (e.g., "10")
            part: Part number (e.g., "1")

        Returns:
            List of section numbers (e.g., ["59-10-103", "59-10-104", ...])
        """
        url = self._build_part_url(title, chapter, part)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []
        child_table = soup.find("table", id="childtbl")

        if child_table:
            # Pattern: href=".../{title}-{chapter}-S{section}.html..."
            pattern = re.compile(rf"{re.escape(title)}-{re.escape(chapter)}-S([\d.]+)\.html")
            for link in child_table.find_all("a", href=pattern):
                href = link.get("href", "")
                match = pattern.search(href)
                if match:
                    section_num = f"{title}-{chapter}-{match.group(1)}"
                    if section_num not in section_numbers:
                        section_numbers.append(section_num)

        return section_numbers

    def get_chapter_parts(self, title: str, chapter: str) -> list[str]:
        """Get list of parts in a chapter.

        Args:
            title: Title number (e.g., "59", "35A")
            chapter: Chapter number (e.g., "10")

        Returns:
            List of part numbers (e.g., ["1", "2", "10"])
        """
        url = self._build_chapter_url(title, chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        parts = []
        child_table = soup.find("table", id="childtbl")

        if child_table:
            # Pattern: href=".../{title}-{chapter}-P{part}.html..."
            pattern = re.compile(rf"{re.escape(title)}-{re.escape(chapter)}-P(\d+)\.html")
            for link in child_table.find_all("a", href=pattern):
                href = link.get("href", "")
                match = pattern.search(href)
                if match:
                    part_num = match.group(1)
                    if part_num not in parts:
                        parts.append(part_num)

        return parts

    def iter_chapter(self, title: str, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., "59", "35A")
            chapter: Chapter number (e.g., "10")

        Yields:
            Section objects for each section
        """
        parts = self.get_chapter_parts(title, chapter)

        for part in parts:
            section_numbers = self.get_part_section_numbers(title, chapter, part)
            for section_num in section_numbers:
                try:
                    yield self.fetch_section(section_num)
                except UTConverterError as e:  # pragma: no cover
                    # Log but continue with other sections
                    print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                    continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter identifiers like "59-10" or "35A-3"
                (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(UT_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter_id in chapters:  # pragma: no cover
            parts = chapter_id.split("-")  # pragma: no cover
            if len(parts) != 2:  # pragma: no cover
                continue  # pragma: no cover
            title, chapter = parts[0], parts[1]  # pragma: no cover
            yield from self.iter_chapter(title, chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "UTConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ut_section(section_number: str) -> Section:
    """Fetch a single Utah Code section.

    Args:
        section_number: e.g., "59-10-104"

    Returns:
        Section model
    """
    with UTConverter() as converter:
        return converter.fetch_section(section_number)


def download_ut_chapter(title: str, chapter: str) -> list[Section]:
    """Download all sections from a Utah Code chapter.

    Args:
        title: Title number (e.g., "59", "35A")
        chapter: Chapter number (e.g., "10")

    Returns:
        List of Section objects
    """
    with UTConverter() as converter:
        return list(converter.iter_chapter(title, chapter))


def download_ut_tax_chapters() -> Iterator[Section]:
    """Download all sections from Utah tax-related chapters.

    Yields:
        Section objects
    """
    with UTConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(UT_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_ut_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Utah workforce services chapters.

    Yields:
        Section objects
    """
    with UTConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(UT_WELFARE_CHAPTERS.keys()))  # pragma: no cover
