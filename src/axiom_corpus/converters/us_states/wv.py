"""West Virginia state statute converter.

Converts West Virginia Code HTML from code.wvlegislature.gov
to the internal Section model for ingestion.

West Virginia Code Structure:
- Chapters (e.g., Chapter 11: Taxation)
- Articles (e.g., Article 21: Personal Income Tax)
- Sections (e.g., 11-21-1: Legislative findings)

URL Patterns:
- Chapter index: /11/
- Article contents: /11-21/
- Section: /11-21-1/

Section numbers follow format: chapter-article-section (e.g., 11-21-1)
Some articles have letter suffixes (e.g., 11-6B for Homestead Property Tax Exemption).

Example:
    >>> from axiom_corpus.converters.us_states.wv import WVConverter
    >>> converter = WVConverter()
    >>> section = converter.fetch_section("11-21-1")
    >>> print(section.section_title)
    "Legislative findings"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://code.wvlegislature.gov"

# West Virginia chapter mapping
WV_CHAPTERS: dict[int, str] = {
    1: "The State and Its Subdivisions",
    2: "Legislature",
    3: "Executive Department",
    4: "Crimes and Their Punishments",
    5: "General Powers and Authority of the Governor",
    6: "General Provisions Respecting Officers",
    7: "County Commissions and Officers",
    8: "Municipal Corporations",
    9: "Human Services",
    10: "Public Safety",
    11: "Taxation",
    12: "Public Moneys and Securities",
    13: "Loans and Bonds and Other Obligations of State Agencies",
    14: "Claims Due or Against the State",
    15: "Public Safety",
    16: "Public Health",
    17: "Roads and Highways",
    18: "Education",
    19: "Agriculture",
    20: "Natural Resources",
    21: "Labor",
    22: "Environmental Resources",
    23: "Workers' Compensation",
    24: "Public Service Commission",
    25: "Sundry Offices and Officers",
    29: "Miscellaneous Boards and Officers",
    30: "Professions and Occupations",
    31: "Corporations",
    33: "Insurance",
    36: "Estates and Trusts",
    37: "Real Property",
    38: "Liens",
    39: "Recordation",
    44: "Administration of Estates and Trusts",
    46: "Uniform Commercial Code",
    47: "Regulation of Trade",
    48: "Domestic Relations",
    49: "Child Welfare",
    50: "Magistrate Courts",
    51: "Courts and Their Officers",
    53: "Extraordinary Remedies",
    55: "Actions, Suits and Arbitration; Judicial Sale",
    56: "Pleading and Practice",
    57: "Evidence and Witnesses",
    58: "Appeals and Supersedeas",
    59: "Fees and Costs; Fines; Forfeitures",
    60: "State Control of Alcoholic Liquors",
    61: "Crimes and Their Punishment",
    62: "Criminal Procedure",
}

# Key chapters for tax/benefit analysis
WV_TAX_CHAPTERS: dict[int, str] = {
    11: "Taxation",
    12: "Public Moneys and Securities",
}

WV_WELFARE_CHAPTERS: dict[int, str] = {
    9: "Human Services",
    16: "Public Health",
    23: "Workers' Compensation",
    49: "Child Welfare",
}

# Article mapping for key chapters
WV_TAX_ARTICLES: dict[str, str] = {
    "11-1": "Supervision",
    "11-3": "Assessments Generally",
    "11-6B": "Homestead Property Tax Exemption",
    "11-6C": "Senior Citizen Property Tax Credit",
    "11-12": "Business Registration Tax",
    "11-13": "Severance Taxes",
    "11-13A": "Severance and Business Privilege Tax Act",
    "11-14": "Gasoline and Special Fuel Excise Tax",
    "11-15": "Consumers Sales and Service Tax",
    "11-15A": "Use Tax",
    "11-16": "Nonintoxicating Beer",
    "11-17": "Tobacco Products",
    "11-19": "Soft Drinks Tax",
    "11-21": "Personal Income Tax",
    "11-22": "Business and Occupation Tax",
    "11-23": "Privilege Tax on Banks and Savings and Loan Associations",
    "11-24": "Corporation Net Income Tax",
    "11-25": "Nondisclosure",
    "11-27": "Telecommunications Tax",
    "11-28": "Health Care Provider Tax",
}

WV_WELFARE_ARTICLES: dict[str, str] = {
    "9-1": "Legislative Purpose and Definitions",
    "9-2": "Commissioner of Human Services",
    "9-3": "Application for and Granting of Assistance",
    "9-4": "State Advisory Board; Medical Services Fund",
    "9-4A": "Medicaid Uncompensated Care Fund",
    "9-4B": "Physician/Medical Practitioner Provider Medicaid Act",
    "9-4C": "Health Care Provider Medicaid Enhancement Act",
    "9-4D": "Medicaid Buy-In Program",
    "9-4E": "Long-Term Care Partnership Program",
    "9-5": "Miscellaneous Provisions",
    "9-6": "Institutional Facilities",
    "9-7": "Child Welfare",
    "9-9": "Fraud and Abuse",
}


@dataclass
class ParsedWVSection:
    """Parsed West Virginia statute section."""

    section_number: str  # e.g., "11-21-1"
    section_title: str  # e.g., "Legislative findings"
    chapter_number: int  # e.g., 11
    chapter_name: str  # e.g., "Taxation"
    article_number: str  # e.g., "21" (can include letters like "6B")
    article_name: str | None  # e.g., "Personal Income Tax"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedWVSubsection] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedWVSubsection:
    """A subsection within a West Virginia statute."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list[ParsedWVSubsection] = field(default_factory=list)


class WVConverterError(Exception):
    """Error during West Virginia statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class WVConverter:
    """Converter for West Virginia Code HTML to internal Section model.

    Example:
        >>> converter = WVConverter()
        >>> section = converter.fetch_section("11-21-1")
        >>> print(section.citation.section)
        "WV-11-21-1"

        >>> for section in converter.iter_article(11, "21"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the West Virginia statute converter.

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
            section_number: e.g., "11-21-1", "9-1-2"

        Returns:
            Full URL to the section page
        """
        return f"{BASE_URL}/{section_number}/"

    def _build_article_url(self, chapter: int, article: str) -> str:
        """Build the URL for an article's table of contents."""
        return f"{BASE_URL}/{chapter}-{article}/"

    def _build_chapter_url(self, chapter: int) -> str:
        """Build the URL for a chapter's table of contents."""
        return f"{BASE_URL}/{chapter}/"

    def _parse_section_number(self, section_number: str) -> tuple[int, str, str]:
        """Parse section number into components.

        Args:
            section_number: e.g., "11-21-1" or "11-6B-1"

        Returns:
            Tuple of (chapter, article, section)
        """
        parts = section_number.split("-")
        if len(parts) < 3:
            raise WVConverterError(
                f"Invalid section number format: {section_number}"
            )  # pragma: no cover

        chapter = int(parts[0])
        article = parts[1]  # Can be "21" or "6B"
        section = "-".join(parts[2:])  # Handle cases like "11-21-4a"

        return chapter, article, section

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedWVSection:
        """Parse section HTML into ParsedWVSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error - look for specific error page patterns
        # We check for section number not appearing in h4 headings (valid pages have h4 with section number)
        section_heading = soup.find(
            "h4", string=re.compile(rf"§?\s*{re.escape(section_number)}", re.I)
        )
        if not section_heading:
            # Also check page title as fallback
            title_tag = soup.find("title")
            title_text = title_tag.get_text() if title_tag else ""
            if section_number not in title_text:
                # Check for explicit error messages
                error_elem = soup.find(class_="error") or soup.find(class_="not-found")
                if error_elem or "page not found" in html.lower():
                    raise WVConverterError(f"Section {section_number} not found", url)

        chapter, article, _ = self._parse_section_number(section_number)
        chapter_name = WV_CHAPTERS.get(chapter, f"Chapter {chapter}")
        article_key = f"{chapter}-{article}"
        article_name = WV_TAX_ARTICLES.get(article_key) or WV_WELFARE_ARTICLES.get(article_key)

        # Extract section title from the heading
        # Pattern: "11-21-1. Legislative findings." or "§11-21-1. Legislative findings."
        section_title = ""

        # Try to find heading tags (WV uses h4 for section headings)
        for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
            heading_text = heading.get_text(strip=True)
            # Match patterns like "11-21-1. Legislative findings" or "§11-21-1. Legislative findings"
            title_pattern = re.compile(
                rf"(?:§\s*)?{re.escape(section_number)}[.\s]+(.+?)(?:\s*$)", re.IGNORECASE
            )
            match = title_pattern.search(heading_text)
            if match:
                section_title = match.group(1).strip().rstrip(".")
                break

        # Try finding from page title if not found
        if not section_title:
            title_tag = soup.find("title")
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                title_pattern = re.compile(
                    rf"(?:§\s*)?{re.escape(section_number)}[.\s]+(.+?)(?:\s*[-|]|$)", re.IGNORECASE
                )
                match = title_pattern.search(title_text)
                if match:
                    section_title = match.group(1).strip().rstrip(".")  # pragma: no cover

        # Try breadcrumb if still not found
        if not section_title:
            breadcrumb = soup.find(class_="breadcrumb") or soup.find(["nav", "ol"])
            if breadcrumb:
                for link in breadcrumb.find_all("a"):
                    link_text = link.get_text(strip=True)
                    if article.upper() in link_text.upper():
                        # Extract article name from breadcrumb
                        article_match = re.search(
                            r"ARTICLE\s+\d+[A-Z]?\.\s*(.+)", link_text, re.IGNORECASE
                        )
                        if article_match:
                            article_name = article_name or article_match.group(1).strip()

        # Get body content - try various containers
        content_elem = (
            soup.find("div", id="content")
            or soup.find("div", class_="content")
            or soup.find("main")
            or soup.find("article")
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

        # Extract history note - WV uses "Bill History" section
        history = None
        history_section = soup.find(class_="history") or soup.find(
            "div", string=re.compile(r"Bill History", re.I)
        )
        if history_section:
            history = history_section.get_text(strip=True)[:2000]
        else:  # pragma: no cover
            # Try to find history in text
            history_patterns = [
                r"Bill History[:\s]*(.+?)(?:\n\n|\s*$)",
                r"(\d{4},\s+c+\.\s*\d+[^§]+?)(?=\n\n|\s*$)",
            ]
            for pattern in history_patterns:
                history_match = re.search(pattern, text, re.DOTALL)
                if history_match:
                    history = history_match.group(1).strip()[:2000]
                    break

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedWVSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_name=chapter_name,
            article_number=article,
            article_name=article_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedWVSubsection]:
        """Parse hierarchical subsections from text.

        West Virginia statutes typically use:
        - (a), (b), (c) for primary divisions
        - (1), (2), (3) for secondary divisions
        - (A), (B), (C) for tertiary divisions
        - (i), (ii), (iii) for quaternary divisions

        Some sections use numbered format first:
        - (1), (2), (3) for primary
        - (a), (b), (c) for secondary
        """
        subsections = []

        # First try lettered format (a), (b), etc.
        if re.search(r"\([a-z]\)\s", text):
            subsections = self._parse_lettered_subsections(text)

        # If no lettered subsections, try numbered format (1), (2), etc.
        if not subsections:
            subsections = self._parse_numbered_subsections(text)

        return subsections

    def _parse_lettered_subsections(self, text: str) -> list[ParsedWVSubsection]:
        """Parse lettered subsections (a), (b), etc. as primary level."""
        subsections = []

        # Split by (a), (b), etc. pattern
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (a)
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse nested (1), (2), etc.
            children = self._parse_nested_numbered(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\(\d+\)\s", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit size and stop at next subsection
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedWVSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_numbered_subsections(self, text: str) -> list[ParsedWVSubsection]:
        """Parse numbered subsections (1), (2), etc. as primary level."""
        subsections = []

        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse nested (a), (b), etc.
            children = self._parse_nested_lettered(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([a-z]\)\s", content)  # pragma: no cover
                direct_text = (  # pragma: no cover
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit size
            next_subsection = re.search(r"\(\d+\)", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedWVSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_nested_numbered(self, text: str) -> list[ParsedWVSubsection]:
        """Parse (1), (2), etc. as nested level under letters."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse (A), (B), etc. as third level
            children = self._parse_uppercase_letters(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([A-Z]\)\s", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit size
            next_num = re.search(r"\(\d+\)", direct_text)
            if next_num:
                direct_text = direct_text[: next_num.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedWVSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_nested_lettered(self, text: str) -> list[ParsedWVSubsection]:
        """Parse (a), (b), etc. as nested level under numbers."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)  # pragma: no cover
            if not match:  # pragma: no cover
                continue  # pragma: no cover

            identifier = match.group(1)  # pragma: no cover
            content = part[match.end() :]  # pragma: no cover

            # Limit size and stop at next subsection
            next_letter = re.search(r"\([a-z]\)", content)  # pragma: no cover
            if next_letter:  # pragma: no cover
                content = content[: next_letter.start()]  # pragma: no cover

            next_num = re.search(r"\(\d+\)", content)  # pragma: no cover
            if next_num:  # pragma: no cover
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(  # pragma: no cover
                ParsedWVSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _parse_uppercase_letters(self, text: str) -> list[ParsedWVSubsection]:
        """Parse (A), (B), etc. as tertiary level."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit size
            next_letter = re.search(r"\([A-Z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]  # pragma: no cover

            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedWVSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedWVSection) -> Section:
        """Convert ParsedWVSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"WV-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsections(subs: list[ParsedWVSubsection]) -> list[Subsection]:
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
            title_name=f"West Virginia Code - Chapter {parsed.chapter_number}. {parsed.chapter_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"wv/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "11-21-1", "9-1-2"

        Returns:
            Section model

        Raises:
            WVConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_article_section_numbers(self, chapter: int, article: str) -> list[str]:
        """Get list of section numbers in an article.

        Args:
            chapter: Chapter number (e.g., 11)
            article: Article number (e.g., "21" or "6B")

        Returns:
            List of section numbers (e.g., ["11-21-1", "11-21-3", ...])
        """
        url = self._build_article_url(chapter, article)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links: /11-21-1/, /11-21-3/, etc.
        pattern = re.compile(rf"/({chapter}-{re.escape(article)}-[\d\w]+)/")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def get_chapter_articles(self, chapter: int) -> list[str]:
        """Get list of articles in a chapter.

        Args:
            chapter: Chapter number (e.g., 11)

        Returns:
            List of article numbers (e.g., ["1", "6B", "21", "24"])
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        articles = []

        # Find article links: /11-1/, /11-6B/, /11-21/, etc.
        pattern = re.compile(rf"/{chapter}-(\d+[A-Za-z]*)/")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                article_num = match.group(1)
                if article_num not in articles:
                    articles.append(article_num)

        return articles

    def iter_article(self, chapter: int, article: str) -> Iterator[Section]:
        """Iterate over all sections in an article.

        Args:
            chapter: Chapter number (e.g., 11)
            article: Article number (e.g., "21")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_article_section_numbers(chapter, article)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except WVConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapter(self, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 11)

        Yields:
            Section objects for each section
        """
        articles = self.get_chapter_articles(chapter)  # pragma: no cover

        for article in articles:  # pragma: no cover
            yield from self.iter_article(chapter, article)  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[int] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(WV_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> WVConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_wv_section(section_number: str) -> Section:
    """Fetch a single West Virginia statute section.

    Args:
        section_number: e.g., "11-21-1"

    Returns:
        Section model
    """
    with WVConverter() as converter:
        return converter.fetch_section(section_number)


def download_wv_article(chapter: int, article: str) -> list[Section]:
    """Download all sections from a West Virginia Code article.

    Args:
        chapter: Chapter number (e.g., 11)
        article: Article number (e.g., "21")

    Returns:
        List of Section objects
    """
    with WVConverter() as converter:
        return list(converter.iter_article(chapter, article))


def download_wv_tax_chapters() -> Iterator[Section]:
    """Download all sections from West Virginia tax chapters.

    Yields:
        Section objects
    """
    with WVConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(WV_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_wv_welfare_chapters() -> Iterator[Section]:
    """Download all sections from West Virginia welfare chapters.

    Yields:
        Section objects
    """
    with WVConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(WV_WELFARE_CHAPTERS.keys()))  # pragma: no cover
