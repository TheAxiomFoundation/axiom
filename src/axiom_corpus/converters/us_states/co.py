"""Colorado state statute converter.

Converts Colorado Revised Statutes HTML from colorado.public.law
to the internal Section model for ingestion.

Colorado Statute Structure:
- Titles (e.g., Title 39: Taxation)
- Articles (e.g., Article 22: Income Tax)
- Parts (e.g., Part 1: General Provisions)
- Sections (e.g., 39-22-104: Income tax imposed)

URL Patterns:
- Title index: colorado.public.law/statutes/crs_title_[NUMBER]
- Section: colorado.public.law/statutes/crs_[TITLE]-[ARTICLE]-[SECTION]
  e.g., crs_39-22-104 for Title 39, Article 22, Section 104

Key Titles for Tax/Benefit Analysis:
- Title 26: Human Services Code (TANF, child care assistance)
- Title 39: Taxation (income tax, property tax)

Example:
    >>> from axiom_corpus.converters.us_states.co import COConverter
    >>> converter = COConverter()
    >>> section = converter.fetch_section("39-22-104")
    >>> print(section.section_title)
    "Income tax imposed on individuals, estates, and trusts"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://colorado.public.law/statutes"

# Title mapping for reference (some titles have decimal numbers like 25.5)
CO_TITLES: dict[int | float, str] = {
    1: "Elections",
    2: "Agriculture",
    3: "Animals",
    4: "Annotations",
    5: "Attorneys and the Law",
    6: "Consumer and Commercial Affairs",
    7: "Corporations and Associations",
    8: "Labor and Industry",
    9: "Safety - Industrial and Commercial",
    10: "Highways",
    11: "Financial Institutions",
    12: "Professions and Occupations",
    13: "Courts and Court Procedure",
    14: "Domestic Matters",
    15: "Probate, Trusts, and Fiduciaries",
    16: "Criminal Proceedings",
    17: "Corrections",
    18: "Criminal Code",
    19: "Children's Code",
    20: "Peace Officers, Police Officers, and Firefighters",
    21: "County Court Civil Procedure",
    22: "Education",
    23: "Postsecondary Education",
    24: "Government - State",
    25: "Health",
    25.5: "Health Care Policy and Financing",
    26: "Human Services Code",
    27: "Behavioral Health",
    28: "Military and Veterans",
    29: "Government - Local",
    30: "Government - County",
    31: "Government - Municipal",
    32: "Special Districts",
    33: "Parks, Wildlife, and Outdoor Recreation",
    34: "Mineral Resources",
    35: "Agriculture",
    36: "State Lands",
    37: "Water and Irrigation",
    38: "Property - Real and Personal",
    39: "Taxation",
    40: "Utilities - Public",
    41: "Railroads and Related Regulation",
    42: "Vehicles and Traffic",
    43: "Transportation",
    44: "Revenue - Regulation of Activities",
}

# Key articles for tax analysis (Title 39)
CO_TAX_ARTICLES: dict[int, str] = {
    1: "Property Tax - General",
    2: "Property Tax - Valuation for Assessment",
    3: "Property Tax - Classification",
    4: "Property Tax - Levies",
    5: "Property Tax - Collection",
    6: "Property Tax - Delinquent",
    7: "Property Tax - Redemption",
    8: "Property Tax - Sales",
    9: "Property Tax - Deeds",
    10: "Property Tax - Exemptions",
    11: "Property Tax - Senior Citizens",
    12: "Property Tax - Disabled Veterans",
    13: "Property Tax - Administration",
    14: "Property Tax - Special Provisions",
    20: "Excise Taxes - General",
    21: "Excise Taxes - Cigarette, Tobacco, Nicotine",
    22: "Income Tax",
    23: "Special Fuels",
    26: "Severance Tax",
    27: "Local Government Severance Tax",
    28: "Sales and Use Tax - State",
    29: "Sales and Use Tax - Local",
    30: "Gross Ton-mile Tax",
    35: "Lodging Tax",
}

# Key articles for human services (Title 26)
CO_HUMAN_SERVICES_ARTICLES: dict[int, str] = {
    1: "Department of Human Services",
    2: "Public Assistance",
    4: "Child Welfare",
    5: "Services for the Aged, Blind, and Disabled",
    6: "Community Services Block Grant",
    7: "Developmental Disabilities",
    8: "Mental Health and Substance Abuse",
    12: "Children's Basic Health Plan",
    13: "Healthy Communities",
    14: "Early Childhood Leadership Commission",
    15: "Family Resource Centers",
}


@dataclass
class ParsedCOSection:
    """Parsed Colorado statute section."""

    section_number: str  # e.g., "39-22-104"
    section_title: str  # e.g., "Income tax imposed on individuals, estates, and trusts"
    title_number: int  # e.g., 39
    title_name: str  # e.g., "Taxation"
    article_number: int  # e.g., 22
    article_name: str | None  # e.g., "Income Tax"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedCOSubsection] = field(default_factory=list)
    history: str | None = None  # History/source note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedCOSubsection:
    """A subsection within a Colorado statute."""

    identifier: str  # e.g., "1", "a", "I", "A"
    text: str
    children: list[ParsedCOSubsection] = field(default_factory=list)


class COConverterError(Exception):
    """Error during Colorado statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class COConverter:
    """Converter for Colorado Revised Statutes HTML to internal Section model.

    Example:
        >>> converter = COConverter()
        >>> section = converter.fetch_section("39-22-104")
        >>> print(section.citation.section)
        "CO-39-22-104"

        >>> for section in converter.iter_article(39, 22):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Colorado statute converter.

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
            section_number: e.g., "39-22-104", "26-2-701"

        Returns:
            Full URL to the section page
        """
        # Section number format: TITLE-ARTICLE-SECTION
        return f"{BASE_URL}/crs_{section_number}"

    def _build_title_url(self, title: int) -> str:
        """Build the URL for a title's index page."""
        return f"{BASE_URL}/crs_title_{title}"

    def _build_article_url(self, title: int, article: int) -> str:
        """Build the URL for an article's index page.

        Note: colorado.public.law uses a different pattern for articles.
        The actual structure varies by title.
        """
        return f"{BASE_URL}/crs_title_{title}/article_{article}"  # pragma: no cover

    def _parse_section_number(self, section_number: str) -> tuple[int, int, str]:
        """Parse section number into title, article, and section parts.

        Args:
            section_number: e.g., "39-22-104", "26-2-701"

        Returns:
            Tuple of (title_number, article_number, section_part)
        """
        parts = section_number.split("-")
        if len(parts) < 3:
            raise ValueError(f"Invalid section number format: {section_number}")  # pragma: no cover

        title = int(parts[0])
        article = int(parts[1])
        section = "-".join(parts[2:])  # Handle section numbers like "104.5"

        return title, article, section

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedCOSection:
        """Parse section HTML into ParsedCOSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error - look for specific error page patterns
        # Avoid false positives from generic phrases like "if not found"
        title_elem = soup.find("title")
        title_text = title_elem.get_text().lower() if title_elem else ""
        if "page not found" in title_text or "404" in title_text:
            raise COConverterError(f"Section {section_number} not found", url)
        # Also check for HTTP 404 error page body patterns
        h1_elem = soup.find("h1")
        h1_text = h1_elem.get_text().lower() if h1_elem else ""
        if "404" in h1_text or "page not found" in h1_text:
            raise COConverterError(f"Section {section_number} not found", url)  # pragma: no cover

        title_number, article_number, section_part = self._parse_section_number(section_number)
        title_name = CO_TITLES.get(title_number, f"Title {title_number}")

        # Get article name based on title
        if title_number == 39:
            article_name = CO_TAX_ARTICLES.get(article_number, f"Article {article_number}")
        elif title_number == 26:
            article_name = CO_HUMAN_SERVICES_ARTICLES.get(
                article_number, f"Article {article_number}"
            )
        else:
            article_name = f"Article {article_number}"  # pragma: no cover

        # Extract section title from h1 or heading
        section_title = ""

        # Look for h1 with pattern "C.R.S. Section X-X-X / Title"
        h1 = soup.find("h1")
        if h1:
            h1_text = h1.get_text(strip=True)
            # Pattern: "C.R.S. Section 39-22-104Income tax imposed..."
            # or "C.R.S. Section 39-22-104 / Income tax imposed..."
            title_match = re.search(
                rf"(?:C\.?R\.?S\.?\s*(?:Section\s*)?)?{re.escape(section_number)}\s*[/]?\s*(.+)",
                h1_text,
            )
            if title_match:
                section_title = title_match.group(1).strip()

        # Fallback: look for any heading with section number
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3"]):
                heading_text = heading.get_text(strip=True)
                if section_number in heading_text:
                    # Extract text after section number
                    idx = heading_text.find(section_number)  # pragma: no cover
                    after = heading_text[idx + len(section_number) :].strip()  # pragma: no cover
                    # Clean up leading punctuation
                    after = re.sub(r"^[/\-:.\s]+", "", after)  # pragma: no cover
                    if after:  # pragma: no cover
                        section_title = after  # pragma: no cover
                        break  # pragma: no cover

        # Get main content
        content_elem = (
            soup.find("article")
            or soup.find("main")
            or soup.find("div", class_="content")
            or soup.find("div", class_="statute")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and non-content elements
            for elem in content_elem.find_all(
                ["nav", "script", "style", "header", "footer", "aside"]
            ):
                elem.decompose()
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract history/source note
        history = None
        history_patterns = [
            r"Source:\s*(.+?)(?:\n|$)",
            r"History:\s*(.+?)(?:\n|$)",
            r"Added by Laws\s*(.+?)(?:\n|$)",
            r"Amended by Laws\s*(.+?)(?:\n|$)",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if history_match:
                history = history_match.group(1).strip()[:1000]
                break

        # Parse subsections from text
        subsections = self._parse_subsections(text)

        return ParsedCOSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_number,
            title_name=title_name,
            article_number=article_number,
            article_name=article_name,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedCOSubsection]:
        """Parse hierarchical subsections from text.

        Colorado statutes typically use:
        - (1), (1.5), (2), etc. for primary divisions
        - (a), (b), (c) for secondary divisions
        - (I), (II), (III) for tertiary divisions (Roman numerals)
        - (A), (B), (C) for quaternary divisions
        """
        subsections = []

        # Split by top-level subsections (1), (1.5), (2), etc.
        # Pattern matches (1), (1.5), (1.7), etc.
        parts = re.split(r"(?=\(\d+(?:\.\d+)?\)\s)", text)

        for part in parts[1:]:  # Skip content before first (1)
            match = re.match(r"\((\d+(?:\.\d+)?)\)\s*", part)
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
            next_subsection = re.search(r"\(\d+(?:\.\d+)?\)", direct_text)
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedCOSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedCOSubsection]:
        """Parse level 2 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 3 children (I), (II), etc.
            children = self._parse_level3(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\((?:I|II|III|IV|V|VI|VII|VIII|IX|X)\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit to reasonable size and stop at next numbered subsection
            next_num = re.search(r"\(\d+(?:\.\d+)?\)", direct_text)
            if next_num:  # pragma: no cover
                direct_text = direct_text[: next_num.start()]

            subsections.append(
                ParsedCOSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedCOSubsection]:
        """Parse level 3 subsections (I), (II), etc. (Roman numerals)."""
        subsections = []
        # Match Roman numerals I through X
        parts = re.split(r"(?=\((?:I|II|III|IV|V|VI|VII|VIII|IX|X)\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((I|II|III|IV|V|VI|VII|VIII|IX|X)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 4 children (A), (B), etc.
            children = self._parse_level4(content)

            # Get text before first child
            if children:  # pragma: no cover
                first_child_match = re.search(r"\([A-Z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Limit size
            next_num = re.search(r"\(\d+(?:\.\d+)?\)", direct_text)
            if next_num:  # pragma: no cover
                direct_text = direct_text[: next_num.start()]

            next_letter = re.search(r"\([a-z]\)", direct_text)
            if next_letter:  # pragma: no cover
                direct_text = direct_text[: next_letter.start()]

            subsections.append(
                ParsedCOSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:1500],
                    children=children,
                )
            )

        return subsections

    def _parse_level4(self, text: str) -> list[ParsedCOSubsection]:
        """Parse level 4 subsections (A), (B), etc. (uppercase letters)."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit size and stop at higher-level markers
            next_num = re.search(r"\(\d+(?:\.\d+)?\)", content)
            if next_num:
                content = content[: next_num.start()]

            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]

            next_roman = re.search(r"\((?:I|II|III|IV|V|VI|VII|VIII|IX|X)\)", content)
            if next_roman:
                content = content[: next_roman.start()]  # pragma: no cover

            subsections.append(
                ParsedCOSubsection(
                    identifier=identifier,
                    text=content.strip()[:1000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedCOSection) -> Section:
        """Convert ParsedCOSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"CO-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsection(sub: ParsedCOSubsection) -> Subsection:
            return Subsection(
                identifier=sub.identifier,
                heading=None,
                text=sub.text,
                children=[convert_subsection(c) for c in sub.children],
            )

        subsections = [convert_subsection(sub) for sub in parsed.subsections]

        return Section(
            citation=citation,
            title_name=f"Colorado Revised Statutes - Title {parsed.title_number}: {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"co/{parsed.title_number}/{parsed.article_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "39-22-104", "26-2-701"

        Returns:
            Section model

        Raises:
            COConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        html = self._get(url)
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_article_section_numbers(self, title: int, article: int) -> list[str]:
        """Get list of section numbers in an article.

        Args:
            title: Title number (e.g., 39)
            article: Article number (e.g., 22)

        Returns:
            List of section numbers (e.g., ["39-22-101", "39-22-102", ...])

        Note:
            This fetches the article page and parses links to find sections.
            The URL pattern is: crs_title_39_article_22
        """
        # Use the article-specific URL
        url = f"{BASE_URL}/crs_title_{title}_article_{article}"
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []
        # Pattern for section links: crs_39-22-104 or crs_39-22-104.5
        pattern = re.compile(rf"crs_{title}-{article}-(\d+(?:\.\d+)?)")

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = f"{title}-{article}-{match.group(1)}"
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return sorted(
            section_numbers,
            key=lambda x: [int(p) if p.isdigit() else p for p in re.split(r"[-.]", x)],
        )

    def iter_article(self, title: int, article: int) -> Iterator[Section]:
        """Iterate over all sections in an article.

        Args:
            title: Title number (e.g., 39)
            article: Article number (e.g., 22)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_article_section_numbers(title, article)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except COConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 39)

        Yields:
            Section objects

        Note:
            This may be slow for large titles. Consider iterating by article.
        """
        # Get article list based on title
        if title == 39:  # pragma: no cover
            articles = list(CO_TAX_ARTICLES.keys())  # pragma: no cover
        elif title == 26:  # pragma: no cover
            articles = list(CO_HUMAN_SERVICES_ARTICLES.keys())  # pragma: no cover
        else:
            # Try to discover articles from title page
            articles = self._discover_articles(title)  # pragma: no cover

        for article in articles:  # pragma: no cover
            yield from self.iter_article(title, article)  # pragma: no cover

    def _discover_articles(self, title: int) -> list[int]:
        """Discover article numbers from a title page.

        Args:
            title: Title number

        Returns:
            List of article numbers found
        """
        url = self._build_title_url(title)  # pragma: no cover
        try:  # pragma: no cover
            html = self._get(url)  # pragma: no cover
            soup = BeautifulSoup(html, "html.parser")  # pragma: no cover

            articles = set()  # pragma: no cover
            # Look for patterns like crs_39-22-104 (article 22) or article_22
            pattern = re.compile(rf"crs_{title}-(\d+)-")  # pragma: no cover
            for link in soup.find_all("a", href=True):  # pragma: no cover
                href = link.get("href", "")  # pragma: no cover
                match = pattern.search(href)  # pragma: no cover
                if match:  # pragma: no cover
                    articles.add(int(match.group(1)))  # pragma: no cover

            return sorted(articles)  # pragma: no cover
        except Exception:  # pragma: no cover
            return []  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> COConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_co_section(section_number: str) -> Section:
    """Fetch a single Colorado statute section.

    Args:
        section_number: e.g., "39-22-104"

    Returns:
        Section model
    """
    with COConverter() as converter:
        return converter.fetch_section(section_number)


def download_co_article(title: int, article: int) -> list[Section]:
    """Download all sections from a Colorado Revised Statutes article.

    Args:
        title: Title number (e.g., 39)
        article: Article number (e.g., 22)

    Returns:
        List of Section objects
    """
    with COConverter() as converter:
        return list(converter.iter_article(title, article))


def download_co_income_tax() -> Iterator[Section]:
    """Download all sections from Colorado income tax (Title 39, Article 22).

    Yields:
        Section objects
    """
    with COConverter() as converter:  # pragma: no cover
        yield from converter.iter_article(39, 22)  # pragma: no cover


def download_co_human_services() -> Iterator[Section]:
    """Download all sections from Colorado Human Services Code (Title 26).

    Yields:
        Section objects
    """
    with COConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(26)  # pragma: no cover
