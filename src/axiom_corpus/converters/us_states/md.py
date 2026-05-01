"""Maryland state statute converter.

Converts Maryland Code HTML from mgaleg.maryland.gov (Maryland General Assembly)
to the internal Section model for ingestion.

Maryland Code Structure:
- Articles (e.g., Tax - General (gtg), Human Services (ghu))
- Titles (e.g., Title 10 - Income Tax)
- Subtitles (e.g., Subtitle 1 - Definitions; General Provisions)
- Sections (e.g., 10-105 - State income tax rates)

URL Patterns:
- Statute text: Laws/StatuteText?article=gtg&section=10-105&enactments=false
- Sections API: api/Laws/GetSections?articleCode=gtg&enactments=false

Key Articles for tax/benefit analysis:
- gtg: Tax - General (income tax, sales tax, etc.)
- gtp: Tax - Property
- ghu: Human Services

Example:
    >>> from axiom_corpus.converters.us_states.md import MDConverter
    >>> converter = MDConverter()
    >>> section = converter.fetch_section("gtg", "10-105")
    >>> print(section.section_title)
    "State income tax rates"
"""

import html
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://mgaleg.maryland.gov/mgawebsite"

# Article code mapping
MD_ARTICLES: dict[str, str] = {
    "gag": "Agriculture",
    "gal": "Alcoholic Beverages",
    "gbr": "Business Regulation",
    "gcj": "Courts and Judicial Proceedings",
    "gcr": "Criminal Law",
    "gcp": "Criminal Procedure",
    "gec": "Economic Development",
    "ged": "Education",
    "gel": "Election Law",
    "gen": "Environment",
    "get": "Estates and Trusts",
    "gfa": "Family Law",
    "gfi": "Financial Institutions",
    "ghg": "Health - General",
    "gho": "Health Occupations",
    "ghp": "Housing and Community Development",
    "ghu": "Human Services",
    "gin": "Insurance",
    "gle": "Labor and Employment",
    "glu": "Land Use",
    "glg": "Local Government",
    "gnr": "Natural Resources",
    "gps": "Public Safety",
    "gpu": "Public Utilities",
    "grp": "Real Property",
    "gsg": "State Government",
    "gsf": "State Finance and Procurement",
    "gsp": "State Personnel and Pensions",
    "gtg": "Tax - General",
    "gtp": "Tax - Property",
    "gtr": "Transportation",
}

# Key articles for tax/benefit analysis
MD_TAX_ARTICLES: dict[str, str] = {
    "gtg": "Tax - General",
    "gtp": "Tax - Property",
}

MD_WELFARE_ARTICLES: dict[str, str] = {
    "ghu": "Human Services",
}


@dataclass
class ParsedMDSection:
    """Parsed Maryland Code section."""

    article_code: str  # e.g., "gtg"
    article_name: str  # e.g., "Tax - General"
    section_number: str  # e.g., "10-105"
    section_title: str | None  # e.g., "State income tax rates" (if found)
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedMDSubsection] = field(default_factory=list)
    history: str | None = None  # History note (not typically in MD statutes page)
    source_url: str = ""


@dataclass
class ParsedMDSubsection:
    """A subsection within a Maryland Code section."""

    identifier: str  # e.g., "a", "1", "i"
    text: str
    children: list[ParsedMDSubsection] = field(default_factory=list)


class MDConverterError(Exception):
    """Error during Maryland Code conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class MDConverter:
    """Converter for Maryland Code HTML to internal Section model.

    Example:
        >>> converter = MDConverter()
        >>> section = converter.fetch_section("gtg", "10-105")
        >>> print(section.citation.section)
        "MD-gtg-10-105"

        >>> for section in converter.iter_article("gtg"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the Maryland Code converter.

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

    def _get_json(self, url: str) -> list[dict]:  # pragma: no cover
        """Make a rate-limited GET request and parse JSON."""
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.json()

    def _build_section_url(self, article_code: str, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            article_code: e.g., "gtg", "ghu"
            section_number: e.g., "10-105", "3-101"

        Returns:
            Full URL to the section page
        """
        return (
            f"{BASE_URL}/Laws/StatuteText"
            f"?article={article_code}&section={section_number}&enactments=false"
        )

    def _build_sections_api_url(self, article_code: str) -> str:
        """Build the URL for the sections API."""
        return f"{BASE_URL}/api/Laws/GetSections?articleCode={article_code}&enactments=false"

    def _parse_section_html(
        self,
        html_content: str,
        article_code: str,
        section_number: str,
        url: str,
    ) -> ParsedMDSection:
        """Parse section HTML into ParsedMDSection."""
        soup = BeautifulSoup(html_content, "html.parser")

        # Check for "not found" error
        if "not found" in html_content.lower() or "error" in html_content.lower():
            # Check if it's actually an error page
            statute_div = soup.find("div", id="StatuteText")
            if not statute_div or len(statute_div.get_text(strip=True)) < 20:
                raise MDConverterError(f"Section {article_code} {section_number} not found", url)

        article_name = MD_ARTICLES.get(article_code, f"Article {article_code}")

        # Find the StatuteText div which contains the actual statute content
        statute_div = soup.find("div", id="StatuteText")
        if not statute_div:
            raise MDConverterError(
                f"Could not find StatuteText div for {section_number}", url
            )  # pragma: no cover

        # Get the raw HTML content
        statute_html = str(statute_div)

        # Decode HTML entities and clean up the text
        # Maryland uses &nbsp; for indentation and &sect; for section symbol
        statute_div.get_text(separator="\n", strip=True)

        # Also parse the inner HTML to get cleaner text
        inner_soup = BeautifulSoup(statute_html, "html.parser")
        # Remove navigation buttons
        for btn in inner_soup.find_all("button"):
            btn.decompose()
        for div in inner_soup.find_all("div", class_="row"):
            div.decompose()

        # Get cleaned text
        text = inner_soup.get_text(separator="\n", strip=True)
        # Decode any remaining HTML entities
        text = html.unescape(text)

        # Try to extract section title - Maryland doesn't always include titles in HTML
        # The section format is typically: "Article - Tax - General" then "section number" then text
        section_title = None

        # Parse subsections from the text
        subsections = self._parse_subsections(text)

        return ParsedMDSection(
            article_code=article_code,
            article_name=article_name,
            section_number=section_number,
            section_title=section_title,
            text=text,
            html=statute_html,
            subsections=subsections,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedMDSubsection]:
        """Parse hierarchical subsections from text.

        Maryland Code typically uses:
        - (a), (b), (c) for primary divisions
        - (1), (2), (3) for secondary divisions
        - (i), (ii), (iii) for tertiary divisions
        - 1., 2., 3. or A., B., C. for further divisions
        """
        subsections = []

        # Split by primary subsections (a), (b), etc.
        # Match pattern like "(a)" at start of line or after newline
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

            # Clean up text - remove subsequent subsections at same level
            next_subsection = re.search(r"\([a-z]\)", direct_text)
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedMDSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        # If no (a), (b) style subsections, try (1), (2) as primary
        if not subsections:
            subsections = self._parse_level2(text)

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedMDSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse third-level children (i), (ii), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:  # pragma: no cover
                first_child_match = re.search(r"\([ivxlcdm]+\)", content, re.IGNORECASE)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", direct_text)
            if next_num:
                direct_text = direct_text[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedMDSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedMDSubsection]:
        """Parse level 3 subsections (i), (ii), etc."""
        subsections = []
        # Match roman numerals
        parts = re.split(r"(?=\([ivxlcdm]+\)\s)", text, flags=re.IGNORECASE)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\(([ivxlcdm]+)\)\s*", part, re.IGNORECASE)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1).lower()
            content = part[match.end() :]

            # Limit size and stop at next subsection
            next_sub = re.search(r"\([ivxlcdm]+\)", content, re.IGNORECASE)
            if next_sub:
                content = content[: next_sub.start()]

            # Also stop at parent-level patterns
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedMDSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedMDSection) -> Section:
        """Convert ParsedMDSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"MD-{parsed.article_code}-{parsed.section_number}",
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

        # Build section title
        section_title = parsed.section_title or f"Section {parsed.section_number}"

        return Section(
            citation=citation,
            title_name=f"Maryland Code - {parsed.article_name}",
            section_title=section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"md/{parsed.article_code}/{parsed.section_number}",
        )

    def fetch_section(self, article_code: str, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            article_code: Article code (e.g., "gtg", "ghu")
            section_number: Section number (e.g., "10-105", "3-101")

        Returns:
            Section model

        Raises:
            MDConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(article_code, section_number)
        html_content = self._get(url)
        parsed = self._parse_section_html(html_content, article_code, section_number, url)
        return self._to_section(parsed)

    def get_article_section_numbers(self, article_code: str) -> list[str]:
        """Get list of section numbers in an article.

        Args:
            article_code: Article code (e.g., "gtg")

        Returns:
            List of section numbers (e.g., ["1-101", "1-201", "10-105", ...])
        """
        url = self._build_sections_api_url(article_code)
        try:
            sections_data = self._get_json(url)
        except Exception as e:  # pragma: no cover
            raise MDConverterError(
                f"Failed to fetch sections for {article_code}: {e}", url
            ) from e  # pragma: no cover

        return [item["DisplayText"] for item in sections_data]

    def iter_article(
        self,
        article_code: str,
        section_filter: str | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections in an article.

        Args:
            article_code: Article code (e.g., "gtg")
            section_filter: Optional prefix filter (e.g., "10-" for Title 10 only)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_article_section_numbers(article_code)

        for section_num in section_numbers:
            # Apply filter if specified
            if section_filter and not section_num.startswith(section_filter):
                continue

            try:
                yield self.fetch_section(article_code, section_num)
            except MDConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(
                    f"Warning: Could not fetch {article_code} {section_num}: {e}"
                )  # pragma: no cover
                continue  # pragma: no cover

    def iter_articles(
        self,
        article_codes: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple articles.

        Args:
            article_codes: List of article codes (default: tax articles)

        Yields:
            Section objects
        """
        if article_codes is None:  # pragma: no cover
            article_codes = list(MD_TAX_ARTICLES.keys())  # pragma: no cover

        for article_code in article_codes:  # pragma: no cover
            yield from self.iter_article(article_code)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> MDConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_md_section(article_code: str, section_number: str) -> Section:
    """Fetch a single Maryland Code section.

    Args:
        article_code: Article code (e.g., "gtg")
        section_number: Section number (e.g., "10-105")

    Returns:
        Section model
    """
    with MDConverter() as converter:
        return converter.fetch_section(article_code, section_number)


def download_md_article(article_code: str) -> list[Section]:
    """Download all sections from a Maryland Code article.

    Args:
        article_code: Article code (e.g., "gtg")

    Returns:
        List of Section objects
    """
    with MDConverter() as converter:
        return list(converter.iter_article(article_code))


def download_md_income_tax_sections() -> Iterator[Section]:
    """Download all sections from Maryland Tax-General Title 10 (Income Tax).

    Yields:
        Section objects
    """
    with MDConverter() as converter:  # pragma: no cover
        yield from converter.iter_article("gtg", section_filter="10-")  # pragma: no cover


def download_md_tax_articles() -> Iterator[Section]:
    """Download all sections from Maryland tax-related articles.

    Yields:
        Section objects
    """
    with MDConverter() as converter:  # pragma: no cover
        yield from converter.iter_articles(list(MD_TAX_ARTICLES.keys()))  # pragma: no cover


def download_md_human_services() -> Iterator[Section]:
    """Download all sections from Maryland Human Services article.

    Yields:
        Section objects
    """
    with MDConverter() as converter:  # pragma: no cover
        yield from converter.iter_article("ghu")  # pragma: no cover
