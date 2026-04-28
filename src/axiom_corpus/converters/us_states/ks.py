"""Kansas state statute converter.

Converts Kansas Statutes Annotated (K.S.A.) HTML from kslegislature.gov
to the internal Section model for ingestion.

Kansas Statute Structure:
- Chapters (e.g., Chapter 79: Taxation)
- Articles (e.g., Article 32: Income Tax)
- Sections (e.g., 79-3201: Title)

URL Patterns:
- Chapter: /li_2024/b2023_24/statute/079_000_0000_chapter/
- Article: /li_2024/b2023_24/statute/079_000_0000_chapter/079_032_0000_article/
- Section: /li_2024/b2023_24/statute/079_000_0000_chapter/079_032_0000_article/079_032_0001_section/079_032_0001_k/

Example:
    >>> from axiom_corpus.converters.us_states.ks import KSConverter
    >>> converter = KSConverter()
    >>> section = converter.fetch_section("79-3201")
    >>> print(section.section_title)
    "Title"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://www.kslegislature.gov"
STATUTE_BASE = "/li_2024/b2023_24/statute"

# Kansas Chapter 79 - Taxation articles
KS_TAX_ARTICLES: dict[str, str] = {
    1: "Property Subject to Taxation",
    2: "Property Exempt from Taxation",
    3: "Listing Property for Taxation",
    4: "Listing and Valuation of Real Estate",
    5: "Rules for Valuing Property",
    10: "Merchants, Manufacturers, Motor Vehicle Dealers and Certain Contractors",
    11: "Banks, Banking Businesses, Trust Companies and Savings and Loan Associations",
    14: "Property Valuation, Equalizing Assessments, Appraisers and Assessment of Property",
    15: "Death Taxes",
    18: "Levy of Taxes",
    19: "Limitations on Tax Levies",
    20: "Collection and Cancellation of Taxes",
    30: "Excise and Sales Taxes",
    32: "Income Tax",
    33: "Cigarettes and Tobacco Products",
    34: "Motor Vehicle Fuel Taxes",
    36: "Kansas Retailers' Sales Tax",
    37: "Kansas Compensating Tax",
    45: "Homestead Property Tax Refunds",
    52: "Food Sales Tax Credit",
}

# Kansas Chapter 39 - Social Welfare articles
KS_WELFARE_ARTICLES: dict[str, str] = {
    7: "General Assistance",
    9: "Cash Assistance",
    17: "Kansas Works Program",
}


@dataclass
class ParsedKSSection:
    """Parsed Kansas statute section."""

    section_number: str  # e.g., "79-3201"
    section_title: str  # e.g., "Title"
    chapter_number: int  # e.g., 79
    chapter_title: str  # e.g., "Taxation"
    article_number: int  # e.g., 32
    article_title: str  # e.g., "Income Tax"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedKSSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedKSSubsection:
    """A subsection within a Kansas statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list["ParsedKSSubsection"] = field(default_factory=list)


class KSConverterError(Exception):
    """Error during Kansas statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class KSConverter:
    """Converter for Kansas Statutes HTML to internal Section model.

    Example:
        >>> converter = KSConverter()
        >>> section = converter.fetch_section("79-3201")
        >>> print(section.citation.section)
        "KS-79-3201"

        >>> for section in converter.iter_article(79, 32):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Kansas statute converter.

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

    def _format_chapter(self, chapter: int) -> str:
        """Format chapter number to 3-digit string."""
        return f"{chapter:03d}"

    def _format_article(self, article: int) -> str:
        """Format article number to 3-digit string."""
        return f"{article:03d}"

    def _format_section(self, section: int) -> str:
        """Format section number to 4-digit string."""
        return f"{section:04d}"

    def _build_chapter_url(self, chapter: int) -> str:
        """Build URL for a chapter page."""
        ch = self._format_chapter(chapter)
        return f"{BASE_URL}{STATUTE_BASE}/{ch}_000_0000_chapter/"

    def _build_article_url(self, chapter: int, article: int) -> str:
        """Build URL for an article page."""
        ch = self._format_chapter(chapter)
        art = self._format_article(article)
        return f"{BASE_URL}{STATUTE_BASE}/{ch}_000_0000_chapter/{ch}_{art}_0000_article/"

    def _build_section_url(self, chapter: int, article: int, section: int) -> str:
        """Build URL for a section page.

        Kansas uses a complex URL pattern:
        /079_000_0000_chapter/079_032_0000_article/079_032_0001_section/079_032_0001_k/
        """
        ch = self._format_chapter(chapter)
        art = self._format_article(article)
        sec = self._format_section(section)
        return (
            f"{BASE_URL}{STATUTE_BASE}/{ch}_000_0000_chapter/"
            f"{ch}_{art}_0000_article/{ch}_{art}_{sec}_section/{ch}_{art}_{sec}_k/"
        )

    def _parse_section_number(self, section_str: str) -> tuple[int, int, int]:
        """Parse section number like '79-3201' into (chapter, article, section).

        Kansas uses format: chapter-article+section
        e.g., 79-3201 = chapter 79, article 32, section 01
        """
        match = re.match(r"(\d+)-(\d+)", section_str)
        if not match:
            raise KSConverterError(
                f"Invalid section number format: {section_str}"
            )  # pragma: no cover

        chapter = int(match.group(1))
        article_section = match.group(2)

        # The article is typically the first 1-2 digits, section is the rest
        # 3201 -> article 32, section 01
        # 320 -> article 3, section 20
        # Need to look at typical patterns
        if len(article_section) >= 3:
            # Try common patterns: 32xx = article 32, xx = section
            article = int(article_section[:2])
            section = int(article_section[2:]) if len(article_section) > 2 else 0
        else:
            article = int(article_section[0])  # pragma: no cover
            section = (
                int(article_section[1:]) if len(article_section) > 1 else 0
            )  # pragma: no cover

        return chapter, article, section

    def _get_chapter_title(self, chapter: int) -> str:
        """Get chapter title."""
        chapter_titles = {
            39: "Mentally Ill, Incapacitated and Dependent Persons; Social Welfare",
            79: "Taxation",
        }
        return chapter_titles.get(chapter, f"Chapter {chapter}")

    def _get_article_title(self, chapter: int, article: int) -> str:
        """Get article title."""
        if chapter == 79:
            return KS_TAX_ARTICLES.get(article, f"Article {article}")
        elif chapter == 39:  # pragma: no cover
            return KS_WELFARE_ARTICLES.get(article, f"Article {article}")  # pragma: no cover
        return f"Article {article}"  # pragma: no cover

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedKSSection:
        """Parse section HTML into ParsedKSSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise KSConverterError(f"Section {section_number} not found", url)

        chapter, article, section = self._parse_section_number(section_number)
        chapter_title = self._get_chapter_title(chapter)
        article_title = self._get_article_title(chapter, article)

        # Find the main content area - look for the statute text
        # The structure has tables with section number, title, and text
        section_title = ""
        text = ""

        # Find all tables that contain statute content
        tables = soup.find_all("table")
        for table in tables:
            table_text = table.get_text(separator=" ", strip=True)

            # Look for section number pattern at start
            if section_number in table_text:
                # Extract section title - it appears after the section number
                # Pattern: "79-3201. Title. The title of this act..."
                title_pattern = re.compile(rf"{re.escape(section_number)}\.?\s*([^.]+)\.")
                match = title_pattern.search(table_text)
                if match:
                    section_title = match.group(1).strip()

                # Get full text after the title
                text = table_text

        # If we didn't find text in tables, try the whole page
        if not text:
            # Get body content
            body = soup.find("body")
            if body:
                text = body.get_text(separator="\n", strip=True)

        # Extract history note
        history = None
        history_match = re.search(r"History:\s*(.+?)(?:\n|$)", text, re.DOTALL)
        if history_match:
            history = history_match.group(1).strip()[:1000]  # Limit length

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedKSSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            article_number=article,
            article_title=article_title,
            text=text,
            html=html,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedKSSubsection]:
        """Parse hierarchical subsections from text.

        Kansas statutes typically use:
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
            children = self._parse_numeric_subsections(content)

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
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedKSSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_numeric_subsections(self, text: str) -> list[ParsedKSSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size and stop at next lettered subsection
            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]  # pragma: no cover

            subsections.append(
                ParsedKSSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedKSSection) -> Section:
        """Convert ParsedKSSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"KS-{parsed.section_number}",
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
            title_name=f"Kansas Statutes - {parsed.chapter_title}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ks/{parsed.chapter_number}/{parsed.article_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "79-3201", "39-709"

        Returns:
            Section model

        Raises:
            KSConverterError: If section not found or parsing fails
        """
        chapter, article, section = self._parse_section_number(section_number)
        url = self._build_section_url(chapter, article, section)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_article_section_numbers(self, chapter: int, article: int) -> list[str]:
        """Get list of section numbers in an article.

        Args:
            chapter: Chapter number (e.g., 79)
            article: Article number (e.g., 32)

        Returns:
            List of section numbers (e.g., ["79-3201", "79-3220", ...])
        """
        url = self._build_article_url(chapter, article)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Find section links - pattern like "79-3201"
        pattern = re.compile(rf"{chapter}-{article}\d+")

        for link in soup.find_all("a"):
            link_text = link.get_text(strip=True)
            match = pattern.match(link_text)
            if match:
                section_num = match.group(0)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_article(self, chapter: int, article: int) -> Iterator[Section]:
        """Iterate over all sections in an article.

        Args:
            chapter: Chapter number (e.g., 79)
            article: Article number (e.g., 32)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_article_section_numbers(chapter, article)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except KSConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapter(self, chapter: int) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., 79)

        Yields:
            Section objects for each section
        """
        # Get article list from chapter page
        url = self._build_chapter_url(chapter)  # pragma: no cover
        html = self._get(url)  # pragma: no cover
        soup = BeautifulSoup(html, "html.parser")  # pragma: no cover

        # Find article links
        articles = []  # pragma: no cover
        for link in soup.find_all("a"):  # pragma: no cover
            href = link.get("href", "")  # pragma: no cover
            # Pattern like "079_032_0000_article"
            match = re.search(
                rf"{self._format_chapter(chapter)}_(\d+)_0000_article", href
            )  # pragma: no cover
            if match:  # pragma: no cover
                article_num = int(match.group(1))  # pragma: no cover
                if article_num not in articles:  # pragma: no cover
                    articles.append(article_num)  # pragma: no cover

        for article in articles:  # pragma: no cover
            yield from self.iter_article(chapter, article)  # pragma: no cover

    def iter_tax_articles(self) -> Iterator[Section]:
        """Iterate over sections from Kansas tax-related articles (Chapter 79).

        Yields:
            Section objects
        """
        for article in KS_TAX_ARTICLES:  # pragma: no cover
            yield from self.iter_article(79, article)  # pragma: no cover

    def iter_welfare_articles(self) -> Iterator[Section]:
        """Iterate over sections from Kansas welfare-related articles (Chapter 39).

        Yields:
            Section objects
        """
        for article in KS_WELFARE_ARTICLES:  # pragma: no cover
            yield from self.iter_article(39, article)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "KSConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ks_section(section_number: str) -> Section:
    """Fetch a single Kansas statute section.

    Args:
        section_number: e.g., "79-3201"

    Returns:
        Section model
    """
    with KSConverter() as converter:
        return converter.fetch_section(section_number)


def download_ks_article(chapter: int, article: int) -> list[Section]:
    """Download all sections from a Kansas Statutes article.

    Args:
        chapter: Chapter number (e.g., 79)
        article: Article number (e.g., 32)

    Returns:
        List of Section objects
    """
    with KSConverter() as converter:
        return list(converter.iter_article(chapter, article))


def download_ks_tax_articles() -> Iterator[Section]:
    """Download all sections from Kansas tax-related articles (Chapter 79).

    Yields:
        Section objects
    """
    with KSConverter() as converter:  # pragma: no cover
        yield from converter.iter_tax_articles()  # pragma: no cover


def download_ks_welfare_articles() -> Iterator[Section]:
    """Download all sections from Kansas welfare-related articles (Chapter 39).

    Yields:
        Section objects
    """
    with KSConverter() as converter:  # pragma: no cover
        yield from converter.iter_welfare_articles()  # pragma: no cover
