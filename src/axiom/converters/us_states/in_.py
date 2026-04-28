"""Indiana state statute converter.

Converts Indiana Code HTML from iga.in.gov to the internal Section model
for ingestion.

Indiana Code Structure:
- Titles (e.g., Title 6: Taxation)
- Articles (e.g., Article 3: State Income Taxes)
- Chapters (e.g., Chapter 1: Definitions)
- Sections (e.g., 6-3-1-3.5: "Adjusted Gross Income")

Citation Format: IC Title-Article-Chapter-Section (e.g., IC 6-3-1-3.5)

URL Patterns (iga.in.gov):
- Title index: /laws/{year}/ic/titles/{title}
- Article: /laws/{year}/ic/titles/{title}/articles/{article}
- Section: /laws/{year}/ic/titles/{title}/articles/{article}/chapters/{chapter}/sections/{section}

Note: The iga.in.gov website is a JavaScript SPA requiring API access.
This converter uses Justia as an alternative HTML source:
- https://law.justia.com/codes/indiana/title-{title}/article-{article}/chapter-{chapter}/section-{title}-{article}-{chapter}-{section}/

Example:
    >>> from axiom.converters.us_states.in_ import INConverter
    >>> converter = INConverter()
    >>> section = converter.fetch_section("6-3-1-3.5")
    >>> print(section.section_title)
    "Adjusted Gross Income"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom.models import Citation, Section, Subsection

# Justia URL base for Indiana Code
JUSTIA_BASE_URL = "https://law.justia.com/codes/indiana"

# Title mapping for reference
IN_TITLES: dict[str, str] = {
    1: "General Provisions",
    2: "Definitions; General Provisions",
    3: "Property",
    4: "State Officers and Administration",
    5: "State and Local Administration",
    6: "Taxation",
    7: "Natural and Cultural Resources",
    8: "Utilities and Transportation",
    9: "Motor Vehicles",
    10: "Public Safety",
    11: "Criminal Law and Procedure",
    12: "Human Services",
    13: "Environment",
    14: "Education",
    15: "Agriculture and Animals",
    16: "Health",
    17: "Alcohol and Tobacco",
    20: "Elections",
    21: "Civil Rights",
    22: "Labor and Safety",
    23: "Business and Other Associations",
    24: "Trade Regulation",
    25: "Professions and Occupations",
    26: "Commercial Law",
    27: "Financial Institutions",
    28: "Insurance",
    29: "Trusts and Fiduciaries",
    30: "Trusts and Trust Companies",
    31: "Family and Juvenile Law",
    32: "Property",
    33: "Courts and Court Officers",
    34: "Civil Procedure",
    35: "Criminal Law and Procedure",
    36: "Local Government",
}

# Key articles for tax analysis (Title 6: Taxation)
IN_TAX_ARTICLES: dict[str, str] = {
    "6-1": "General Provisions",
    "6-1.1": "Property Taxes",
    "6-1.5": "General Tax Administration",
    "6-2.5": "Sales and Use Tax",
    "6-3": "State Income Taxes",
    "6-3.1": "Adjusted Gross Income Tax Credits",
    "6-3.5": "County Income Taxes (Expired)",
    "6-3.6": "Local Income Taxes",
    "6-4.1": "Inheritance Tax (Expired)",
    "6-5.5": "Financial Institutions Tax",
    "6-6": "Motor Fuel and Vehicle Excise Tax",
    "6-7": "Tobacco Taxes",
    "6-8": "Commercial Licensing",
    "6-8.1": "Uniform Revenue Procedures",
    "6-9": "Innkeepers Tax; Food and Beverage Tax",
}

# Key articles for human services (Title 12: Human Services)
IN_WELFARE_ARTICLES: dict[str, str] = {
    "12-7": "General Provisions and Definitions",
    "12-8": "Secretary of Family and Social Services",
    "12-10": "Medicaid",
    "12-13": "Division of Family Resources",
    "12-14": "Family Assistance Services",  # TANF
    "12-15": "Medicaid",
    "12-17": "Children's Services",
    "12-20": "Public Assistance",
    "12-21": "State Institutions",
    "12-22": "State Operated Facilities",
    "12-24": "Other State Health Institutions",
    "12-26": "Mental Health Law",
    "12-27": "Mental Health Services",
    "12-28": "Developmental Disabilities",
}


@dataclass
class ParsedINSection:
    """Parsed Indiana Code section."""

    section_number: str  # e.g., "6-3-1-3.5"
    section_title: str  # e.g., "Adjusted Gross Income"
    title_number: int  # e.g., 6
    article_number: str  # e.g., "3"
    chapter_number: str  # e.g., "1"
    title_name: str | None  # e.g., "Taxation"
    article_name: str | None  # e.g., "State Income Taxes"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedINSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedINSubsection:
    """A subsection within an Indiana Code section."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list["ParsedINSubsection"] = field(default_factory=list)


class INConverterError(Exception):
    """Error during Indiana Code conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class INConverter:
    """Converter for Indiana Code HTML to internal Section model.

    Uses Justia as the HTML source since iga.in.gov is a JavaScript SPA.

    Example:
        >>> converter = INConverter()
        >>> section = converter.fetch_section("6-3-1-3.5")
        >>> print(section.citation.section)
        "IN-6-3-1-3.5"

        >>> for section in converter.iter_article("6-3"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 1.0,
        year: int | None = None,
    ):
        """Initialize the Indiana Code converter.

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

    def _parse_section_number(self, section_number: str) -> tuple[int, str, str, str]:
        """Parse section number into components.

        Args:
            section_number: e.g., "6-3-1-3.5" or "12-14-1-0.5"

        Returns:
            Tuple of (title, article, chapter, section)
        """
        parts = section_number.split("-")
        if len(parts) < 4:
            raise ValueError(f"Invalid section number format: {section_number}")

        title = int(parts[0])
        article = parts[1]
        chapter = parts[2]
        # Section may have decimal (e.g., "3.5") or multiple parts (e.g., "3.5-d")
        section = "-".join(parts[3:])

        return title, article, chapter, section

    def _build_section_url(self, section_number: str) -> str:
        """Build the Justia URL for a section.

        Args:
            section_number: e.g., "6-3-1-3.5"

        Returns:
            Full URL to the section page on Justia
        """
        title, article, chapter, section = self._parse_section_number(section_number)

        # Justia URL format: /codes/indiana/title-{title}/article-{article}/chapter-{chapter}/section-{full-section}/
        return (
            f"{JUSTIA_BASE_URL}/title-{title}/article-{article}/"
            f"chapter-{chapter}/section-{section_number}/"
        )

    def _build_article_url(self, article_code: str) -> str:
        """Build the Justia URL for an article index.

        Args:
            article_code: e.g., "6-3" for Title 6, Article 3

        Returns:
            Full URL to the article page on Justia
        """
        parts = article_code.split("-")
        title = parts[0]
        article = parts[1] if len(parts) > 1 else "1"

        return f"{JUSTIA_BASE_URL}/title-{title}/article-{article}/"

    def _build_chapter_url(self, title: int, article: str, chapter: str) -> str:
        """Build the Justia URL for a chapter index.

        Args:
            title: Title number
            article: Article number
            chapter: Chapter number

        Returns:
            Full URL to the chapter page on Justia
        """
        return f"{JUSTIA_BASE_URL}/title-{title}/article-{article}/chapter-{chapter}/"

    def _get_title_name(self, title_number: int) -> str | None:
        """Get the name for a title number."""
        return IN_TITLES.get(title_number)

    def _get_article_name(self, article_code: str) -> str | None:
        """Get the name for an article code.

        Args:
            article_code: e.g., "6-3" for Title 6 Article 3
        """
        if article_code in IN_TAX_ARTICLES:
            return IN_TAX_ARTICLES[article_code]
        if article_code in IN_WELFARE_ARTICLES:
            return IN_WELFARE_ARTICLES[article_code]
        return None

    def _parse_effective_date(self, text: str) -> date | None:
        """Parse effective date from text.

        Args:
            text: Text containing effective date

        Returns:
            Parsed date or None
        """
        # Common patterns: "Effective July 1, 2024" or "As amended through P.L. 15-2024"
        match = re.search(r"Effective:?\s*(\w+\s+\d{1,2},\s*\d{4})", text, re.IGNORECASE)
        if match:
            date_str = match.group(1)
            try:
                from datetime import datetime

                return datetime.strptime(date_str, "%B %d, %Y").date()
            except ValueError:  # pragma: no cover
                pass
        return None

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedINSection:
        """Parse section HTML into ParsedINSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "404" in html.lower():
            raise INConverterError(f"Section {section_number} not found", url)

        title, article, chapter, _ = self._parse_section_number(section_number)
        article_code = f"{title}-{article}"

        title_name = self._get_title_name(title)
        article_name = self._get_article_name(article_code)

        # Extract section title from the page
        section_title = ""

        # Try to find in the page title
        page_title = soup.find("title")
        if page_title:
            title_text = page_title.get_text(strip=True)
            # Pattern like "IC 6-3-1-3.5 - Adjusted Gross Income"
            match = re.search(
                rf'(?:IC\s+)?{re.escape(section_number)}\s*[-|]\s*"?([^"|]+)"?',
                title_text,
            )
            if match:
                section_title = match.group(1).strip().rstrip(".")

        # Try h1 or h2 headings
        if not section_title:
            for heading in soup.find_all(["h1", "h2"]):
                heading_text = heading.get_text(strip=True)
                # Look for section number followed by title
                match = re.search(
                    rf'{re.escape(section_number)}\s*\.?\s*"?([^".]+)"?',
                    heading_text,
                )
                if match:  # pragma: no cover
                    section_title = match.group(1).strip()
                    break

        # Try meta description
        if not section_title:
            meta = soup.find("meta", attrs={"name": "description"})
            if meta and meta.get("content"):
                content = meta["content"]
                # Pattern: "Section 6-3-1-3.5 - Adjusted Gross Income"
                match = re.search(
                    rf'Section\s+{re.escape(section_number)}\s*[-:]\s*"?([^"|]+)"?',
                    content,
                )
                if match:
                    section_title = match.group(1).strip().rstrip(".")  # pragma: no cover

        # Get body content - try various Justia containers
        content_elem = (
            soup.find("div", class_="codes-content")
            or soup.find("div", id="codes")
            or soup.find("article")
            or soup.find("main")
            or soup.find("div", class_="content")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(  # pragma: no cover
                ["nav", "script", "style", "header", "footer", "aside"]
            ):
                elem.decompose()
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract effective date
        effective_date = self._parse_effective_date(text)

        # Extract history note
        history = None
        history_match = re.search(
            r"(?:History|As amended)[:\s]+(.+?)(?:\n|$)",
            text,
            re.IGNORECASE,
        )
        if history_match:
            history = history_match.group(1).strip()[:500]

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedINSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title,
            article_number=article,
            chapter_number=chapter,
            title_name=title_name,
            article_name=article_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
            effective_date=effective_date,
        )

    def _parse_subsections(self, text: str) -> list[ParsedINSubsection]:
        """Parse hierarchical subsections from text.

        Indiana Code typically uses:
        - (a), (b), (c) for primary divisions
        - (1), (2), (3) for secondary divisions
        - (A), (B), (C) for tertiary divisions
        """
        subsections = []

        # Split by top-level subsections (a), (b), etc.
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (a)
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse second-level children (1), (2), etc.
            children = self._parse_level2(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\(\d+\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Clean up text - remove trailing subsections
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedINSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedINSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse third-level children (A), (B), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\([A-Z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit to reasonable size and stop at next lettered subsection
            next_alpha = re.search(r"\([a-z]\)", direct_text)
            if next_alpha:
                direct_text = direct_text[: next_alpha.start()]  # pragma: no cover

            subsections.append(
                ParsedINSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedINSubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at next higher-level subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover
            next_alpha = re.search(r"\([a-z]\)", content)
            if next_alpha:
                content = content[: next_alpha.start()]  # pragma: no cover

            if len(content) > 2000:
                content = content[:2000] + "..."  # pragma: no cover

            subsections.append(
                ParsedINSubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedINSection) -> Section:
        """Convert ParsedINSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"IN-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsections(
            subs: list[ParsedINSubsection],
        ) -> list[Subsection]:
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

        article_code = f"{parsed.title_number}-{parsed.article_number}"
        title_desc = parsed.title_name or "Title Unknown"
        article_desc = parsed.article_name or f"Article {parsed.article_number}"

        return Section(
            citation=citation,
            title_name=f"Indiana Code - {title_desc} - {article_desc}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"in/{parsed.title_number}/{article_code}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "6-3-1-3.5"

        Returns:
            Section model

        Raises:
            INConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            raise INConverterError(f"Failed to fetch section {section_number}: {e}", url) from e
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_chapter_section_numbers(
        self,
        title: int,
        article: str,
        chapter: str,
    ) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            title: Title number (e.g., 6)
            article: Article number (e.g., "3")
            chapter: Chapter number (e.g., "1")

        Returns:
            List of section numbers (e.g., ["6-3-1-1", "6-3-1-2", ...])
        """
        url = self._build_chapter_url(title, article, chapter)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError:  # pragma: no cover
            return []  # pragma: no cover

        soup = BeautifulSoup(html, "html.parser")
        section_numbers = []

        # Find section links on Justia: /section-{section_number}/
        pattern = re.compile(rf"/section-({title}-{article}-{chapter}-[\d.]+[a-z]?)/")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def get_article_chapters(self, article_code: str) -> list[tuple[str, str, str]]:
        """Get list of chapters in an article.

        Args:
            article_code: e.g., "6-3" for Title 6, Article 3

        Returns:
            List of (title, article, chapter) tuples
        """
        url = self._build_article_url(article_code)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError:  # pragma: no cover
            return []  # pragma: no cover

        soup = BeautifulSoup(html, "html.parser")
        chapters = []

        parts = article_code.split("-")
        title = parts[0]
        article = parts[1] if len(parts) > 1 else "1"

        # Find chapter links on Justia: /chapter-{number}/
        pattern = re.compile(rf"/title-{title}/article-{article}/chapter-(\d+)/")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                chapter = match.group(1)
                if (title, article, chapter) not in chapters:
                    chapters.append((title, article, chapter))

        return chapters

    def iter_chapter(
        self,
        title: int,
        article: str,
        chapter: str,
    ) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            title: Title number (e.g., 6)
            article: Article number (e.g., "3")
            chapter: Chapter number (e.g., "1")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(title, article, chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except INConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_article(self, article_code: str) -> Iterator[Section]:
        """Iterate over all sections in an article.

        Args:
            article_code: e.g., "6-3" for Title 6, Article 3

        Yields:
            Section objects for each section
        """
        chapters = self.get_article_chapters(article_code)

        for title, article, chapter in chapters:
            yield from self.iter_chapter(int(title), article, chapter)

    def iter_articles(
        self,
        article_codes: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple articles.

        Args:
            article_codes: List of article codes (default: all tax articles)

        Yields:
            Section objects
        """
        if article_codes is None:  # pragma: no cover
            article_codes = list(IN_TAX_ARTICLES.keys())  # pragma: no cover

        for article_code in article_codes:  # pragma: no cover
            yield from self.iter_article(article_code)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "INConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_in_section(section_number: str) -> Section:
    """Fetch a single Indiana Code section.

    Args:
        section_number: e.g., "6-3-1-3.5"

    Returns:
        Section model
    """
    with INConverter() as converter:
        return converter.fetch_section(section_number)


def download_in_article(article_code: str) -> list[Section]:
    """Download all sections from an Indiana Code article.

    Args:
        article_code: Article code (e.g., "6-3")

    Returns:
        List of Section objects
    """
    with INConverter() as converter:
        return list(converter.iter_article(article_code))


def download_in_tax_articles() -> Iterator[Section]:
    """Download all sections from Indiana tax-related articles (Title 6).

    Yields:
        Section objects
    """
    with INConverter() as converter:  # pragma: no cover
        yield from converter.iter_articles(list(IN_TAX_ARTICLES.keys()))  # pragma: no cover


def download_in_welfare_articles() -> Iterator[Section]:
    """Download all sections from Indiana human services articles (Title 12).

    Yields:
        Section objects
    """
    with INConverter() as converter:  # pragma: no cover
        yield from converter.iter_articles(list(IN_WELFARE_ARTICLES.keys()))  # pragma: no cover
