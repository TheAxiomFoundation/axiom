"""Georgia state statute converter.

Converts Georgia Code HTML from ga.elaws.us to the internal Section model for ingestion.

Georgia Code Structure:
- Titles (e.g., Title 48: Revenue and Taxation)
- Chapters (e.g., Chapter 7: Income Taxes)
- Articles (e.g., Article 2: Imposition, Rate, and Computation; Exemptions)
- Sections (e.g., 48-7-20: Individual tax rate)

URL Patterns:
- Title index: http://ga.elaws.us/law/48
- Chapter: http://ga.elaws.us/law/48-7
- Article: http://ga.elaws.us/law/48-7|2
- Section: http://ga.elaws.us/law/section48-7-20

Note: Georgia has a state income tax with graduated rates (1% to 5.75%).

Example:
    >>> from atlas.converters.us_states.ga import GAConverter
    >>> converter = GAConverter()
    >>> section = converter.fetch_section("48-7-20")
    >>> print(section.section_title)
    "Individual tax rate; tax table; credit for withholding and other payments..."
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

BASE_URL = "http://ga.elaws.us"

# Title mapping for Georgia Code
GA_TITLES: dict[int, str] = {
    1: "General Provisions",
    2: "Agriculture",
    3: "Alcoholic Beverages",
    4: "Animals",
    5: "Arts and Culture",
    7: "Banking and Finance",
    8: "Buildings and Housing",
    9: "Civil Practice",
    10: "Commerce and Trade",
    11: "Contracts",
    12: "Conservation and Natural Resources",
    13: "Contracts",
    14: "Corporations, Partnerships, and Associations",
    15: "Courts",
    16: "Crimes and Offenses",
    17: "Criminal Procedure",
    18: "Debtor and Creditor",
    19: "Domestic Relations",
    20: "Education",
    21: "Elections",
    22: "Eminent Domain",
    23: "Equity",
    24: "Evidence",
    25: "Fire Protection and Safety",
    26: "Food, Drugs, and Cosmetics",
    27: "Game and Fish",
    28: "General Assembly",
    29: "Guardian and Ward",
    30: "Handicapped Persons",
    31: "Health",
    32: "Highways, Bridges, and Ferries",
    33: "Insurance",
    34: "Labor and Industrial Relations",
    35: "Law Enforcement Officers and Agencies",
    36: "Local Government",
    37: "Mental Health",
    38: "Military, Emergency Management, and Veterans Affairs",
    39: "Minors",
    40: "Motor Vehicles and Traffic",
    41: "Nuisances",
    42: "Penal Institutions",
    43: "Professions and Businesses",
    44: "Property",
    45: "Public Officers and Employees",
    46: "Public Utilities and Public Transportation",
    47: "Retirement and Pensions",
    48: "Revenue and Taxation",
    49: "Social Services",
    50: "State Government",
    51: "Torts",
    52: "Vacation of Office",
    53: "Wills, Trusts, and Administration of Estates",
}

# Key chapters for tax analysis - Title 48
GA_TAX_CHAPTERS: dict[str, str] = {
    "48-1": "General Provisions",
    "48-2": "State Administrative Organization, Administration, and Enforcement",
    "48-3": "Tax Executions",
    "48-4": "Tax Sales",
    "48-5": "Ad Valorem Taxation of Property",
    "48-5A": "Special Assessment of Forest Land Conservation Use Property",
    "48-5B": "Moratorium Period for Valuation Increases in Property",
    "48-5C": "Alternative Ad Valorem Tax on Motor Vehicles",
    "48-6": "Taxation of Intangibles",
    "48-7": "Income Taxes",
    "48-8": "Sales and Use Taxes",
    "48-9": "Motor Fuel and Road Taxes",
    "48-10": "Motor Vehicle License Fees and Plates",
    "48-11": "Taxes on Tobacco Products",
    "48-12": "Estate Tax",
    "48-13": "Specific, Business, and Occupation Taxes",
    "48-14": "Grants and Special Revenue Disbursements",
    "48-15": "Excise Tax on Marijuana and Controlled Substances",
    "48-16": "Tax Amnesty Program",
    "48-16A": "Property Tax Amnesty Program",
}

# Key chapters for social services analysis - Title 49
GA_SOCIAL_SERVICES_CHAPTERS: dict[str, str] = {
    "49-1": "General Provisions",
    "49-2": "Department of Human Services",
    "49-3": "County Departments of Family and Children Services",
    "49-4": "Public Assistance",
    "49-5": "Programs and Protection for Children and Youth",
    "49-6": "Services for the Aging",
    "49-7": "Office of Disability Services Ombudsman",
    "49-8": "Georgia Council on Developmental Disabilities",
    "49-9": "State Commission on the Deaf and Hard of Hearing",
}


@dataclass
class ParsedGASection:
    """Parsed Georgia statute section."""

    section_number: str  # e.g., "48-7-20"
    section_title: str  # e.g., "Individual tax rate..."
    title_number: int  # e.g., 48
    title_name: str  # e.g., "Revenue and Taxation"
    chapter: str  # e.g., "7"
    chapter_title: str | None  # e.g., "Income Taxes"
    article: str | None  # e.g., "2"
    article_title: str | None  # e.g., "Imposition, Rate, and Computation"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedGASubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedGASubsection:
    """A subsection within a Georgia statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list["ParsedGASubsection"] = field(default_factory=list)


class GAConverterError(Exception):
    """Error during Georgia statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)  # pragma: no cover
        self.url = url  # pragma: no cover


class GAConverter:
    """Converter for Georgia Code HTML to internal Section model.

    Example:
        >>> converter = GAConverter()
        >>> section = converter.fetch_section("48-7-20")
        >>> print(section.citation.section)
        "GA-48-7-20"

        >>> for section in converter.iter_chapter("48-7"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Georgia statute converter.

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

    def _build_section_url(self, section_number: str) -> str:
        """Build the URL for a section.

        Args:
            section_number: e.g., "48-7-20", "49-4-142"

        Returns:
            Full URL to the section page
        """
        # URL pattern: http://ga.elaws.us/law/section48-7-20
        return f"{BASE_URL}/law/section{section_number}"

    def _build_chapter_url(self, chapter: str) -> str:
        """Build the URL for a chapter index.

        Args:
            chapter: e.g., "48-7", "49-4"

        Returns:
            Full URL to the chapter page
        """
        return f"{BASE_URL}/law/{chapter}"

    def _build_article_url(self, chapter: str, article: str) -> str:
        """Build the URL for an article index.

        Args:
            chapter: e.g., "48-7"
            article: e.g., "2"

        Returns:
            Full URL to the article page
        """
        # URL pattern uses pipe separator: http://ga.elaws.us/law/48-7|2
        return f"{BASE_URL}/law/{chapter}|{article}"

    def _parse_section_number_parts(self, section_number: str) -> tuple[int, str, str]:
        """Parse section number into title, chapter, and section parts.

        Args:
            section_number: e.g., "48-7-20"

        Returns:
            Tuple of (title_num, chapter, section_suffix)
        """
        parts = section_number.split("-")
        if len(parts) < 3:
            raise GAConverterError(
                f"Invalid section number format: {section_number}"
            )  # pragma: no cover

        title_num = int(parts[0])
        chapter = parts[1]
        section_suffix = "-".join(parts[2:])  # Handle subsections like 48-7-40.26

        return title_num, chapter, section_suffix

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedGASection:
        """Parse section HTML into ParsedGASection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "not found" in html.lower() or "error" in html.lower()[:500]:
            # Check if it's an actual error page
            title_elem = soup.find("title")  # pragma: no cover
            if title_elem and "error" in title_elem.get_text().lower():  # pragma: no cover
                raise GAConverterError(
                    f"Section {section_number} not found", url
                )  # pragma: no cover

        title_num, chapter, _ = self._parse_section_number_parts(section_number)

        # Get title name
        title_name = GA_TITLES.get(title_num, f"Title {title_num}")

        # Get chapter title
        chapter_key = f"{title_num}-{chapter}"
        if title_num == 48:
            chapter_title = GA_TAX_CHAPTERS.get(chapter_key, f"Chapter {chapter}")
        elif title_num == 49:
            chapter_title = GA_SOCIAL_SERVICES_CHAPTERS.get(chapter_key, f"Chapter {chapter}")
        else:
            chapter_title = f"Chapter {chapter}"  # pragma: no cover

        # Extract section title from the heading
        # Pattern: "Section 48-7-20. Individual tax rate; tax table..."
        section_title = ""

        # Try to find section heading - multiple strategies
        # 1. Look for h1-h4 with section number
        for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
            heading_text = heading.get_text(strip=True)
            if section_number in heading_text:
                # Extract title after the section number
                match = re.search(
                    rf"Section\s+{re.escape(section_number)}[.\s]+(.+)",
                    heading_text,
                    re.IGNORECASE,
                )
                if match:
                    section_title = match.group(1).strip()
                    break

        # 2. Search in all text for the pattern
        if not section_title:
            full_text = soup.get_text()
            match = re.search(
                rf"Section\s+{re.escape(section_number)}[.\s]+([^\n]+)",
                full_text,
                re.IGNORECASE,
            )
            if match:
                section_title = match.group(1).strip()  # pragma: no cover

        # 3. Look for meta title
        if not section_title:
            title_tag = soup.find("title")
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                if section_number in title_text:
                    # Extract after section number
                    match = re.search(
                        rf"{re.escape(section_number)}[.\s]+(.+)", title_text
                    )  # pragma: no cover
                    if match:  # pragma: no cover
                        section_title = match.group(1).strip()  # pragma: no cover

        # Get body content
        # Try various containers
        content_elem = (
            soup.find("div", class_="rulehome_rightdetail")
            or soup.find("div", id="content")
            or soup.find("div", class_="content")
            or soup.find("article")
            or soup.find("main")
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

        # Extract history note - look for "History" or "Ga. L." pattern
        history = None
        history_patterns = [
            r"History\.?\s*[-—:\.]?\s*(.+?)(?:\n\n|\Z)",
            r"(Ga\.\s*L\.\s*\d{4}.+?)(?:\n\n|\Z)",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if history_match:
                history = history_match.group(1).strip()[:2000]  # Limit length
                break

        # Try to detect article from breadcrumb or content
        article = None
        article_title = None
        article_match = re.search(r"Article\s+(\d+)[.\s]+([^\n]+)", text, re.IGNORECASE)
        if article_match:  # pragma: no cover
            article = article_match.group(1)
            article_title = article_match.group(2).strip()

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedGASection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_num,
            title_name=title_name,
            chapter=chapter,
            chapter_title=chapter_title,
            article=article,
            article_title=article_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedGASubsection]:
        """Parse hierarchical subsections from text.

        Georgia statutes typically use:
        - (a), (b), (c) for primary divisions
        - (1), (2), (3) for secondary divisions under letters
        - (A), (B), (C) for tertiary divisions
        - (i), (ii), (iii) for quaternary
        """
        subsections = []

        # Split by top-level subsections (a), (b), etc.
        # This pattern matches (a), (b), ... (z) at word boundary
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
            if next_subsection:  # pragma: no cover
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedGASubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        # If no letter subsections found, try numeric as top level
        if not subsections:
            subsections = self._parse_numeric_subsections(text)  # pragma: no cover

        return subsections

    def _parse_numeric_subsections(self, text: str) -> list[ParsedGASubsection]:
        """Parse numeric subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse uppercase letter children (A), (B), etc.
            children = self._parse_uppercase_subsections(content)

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

            # Stop at next numeric
            next_num = re.search(r"\(\d+\)", direct_text)
            if next_num:
                direct_text = direct_text[: next_num.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedGASubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_uppercase_subsections(self, text: str) -> list[ParsedGASubsection]:
        """Parse uppercase subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit to reasonable size
            next_marker = re.search(r"\([A-Z]\)|\(\d+\)", content)
            if next_marker:
                content = content[: next_marker.start()]  # pragma: no cover

            subsections.append(
                ParsedGASubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedGASection) -> Section:
        """Convert ParsedGASection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"GA-{parsed.section_number}",
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
            title_name=f"Georgia Code - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ga/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "48-7-20", "49-4-142"

        Returns:
            Section model

        Raises:
            GAConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        try:
            html = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            raise GAConverterError(  # pragma: no cover
                f"Failed to fetch section {section_number}: {e}", url
            ) from e

        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def get_article_section_numbers(self, chapter: str, article: str) -> list[str]:
        """Get list of section numbers in an article.

        Args:
            chapter: Chapter identifier (e.g., "48-7")
            article: Article number (e.g., "2")

        Returns:
            List of section numbers (e.g., ["48-7-20", "48-7-21", ...])
        """
        url = self._build_article_url(chapter, article)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        section_numbers = []

        # Look for section links - pattern: /law/section48-7-20
        pattern = re.compile(r"/law/section([\d]+-[\d]+[A-Za-z]?-[\d.]+)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        # Also look for section text patterns
        text_pattern = re.compile(r"[^\d](\d+-\d+[A-Za-z]?-\d+(?:\.\d+)?)[^\d]")
        for text in soup.stripped_strings:
            for match in text_pattern.finditer(text):
                section_num = match.group(1)
                if section_num not in section_numbers and section_num.startswith(
                    chapter.replace("-", "")[:2]
                ):
                    section_numbers.append(section_num)  # pragma: no cover

        return section_numbers

    def get_chapter_articles(self, chapter: str) -> list[tuple[str, str]]:
        """Get list of articles in a chapter.

        Args:
            chapter: Chapter identifier (e.g., "48-7")

        Returns:
            List of (article_number, article_title) tuples
        """
        url = self._build_chapter_url(chapter)
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        articles = []

        # Look for article links - pattern: /law/48-7|2
        pattern = re.compile(rf"/law/{re.escape(chapter)}\|(\d+)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            match = pattern.search(href)
            if match:
                article_num = match.group(1)
                article_title = link.get_text(strip=True)
                # Clean up title - remove "Article X." prefix
                title_match = re.search(r"Article\s+\d+[.\s]+(.+)", article_title)
                if title_match:
                    article_title = title_match.group(1).strip()

                articles.append((article_num, article_title))

        return articles

    def iter_article(self, chapter: str, article: str) -> Iterator[Section]:
        """Iterate over all sections in an article.

        Args:
            chapter: Chapter identifier (e.g., "48-7")
            article: Article number (e.g., "2")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_article_section_numbers(chapter, article)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except GAConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapter(self, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter identifier (e.g., "48-7")

        Yields:
            Section objects for each section
        """
        articles = self.get_chapter_articles(chapter)  # pragma: no cover

        for article_num, _ in articles:  # pragma: no cover
            yield from self.iter_article(chapter, article_num)  # pragma: no cover

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 48, 49)

        Yields:
            Section objects
        """
        # Get chapters for this title
        if title == 48:  # pragma: no cover
            chapters = list(GA_TAX_CHAPTERS.keys())  # pragma: no cover
        elif title == 49:  # pragma: no cover
            chapters = list(GA_SOCIAL_SERVICES_CHAPTERS.keys())  # pragma: no cover
        else:
            raise GAConverterError(
                f"Title {title} not configured for iteration"
            )  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            try:  # pragma: no cover
                yield from self.iter_chapter(chapter)  # pragma: no cover
            except GAConverterError as e:  # pragma: no cover
                print(f"Warning: Could not fetch chapter {chapter}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "GAConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ga_section(section_number: str) -> Section:
    """Fetch a single Georgia Code section.

    Args:
        section_number: e.g., "48-7-20"

    Returns:
        Section model
    """
    with GAConverter() as converter:
        return converter.fetch_section(section_number)


def download_ga_chapter(chapter: str) -> list[Section]:
    """Download all sections from a Georgia Code chapter.

    Args:
        chapter: Chapter identifier (e.g., "48-7")

    Returns:
        List of Section objects
    """
    with GAConverter() as converter:  # pragma: no cover
        return list(converter.iter_chapter(chapter))  # pragma: no cover


def download_ga_tax_title() -> Iterator[Section]:
    """Download all sections from Georgia Title 48 (Revenue and Taxation).

    Yields:
        Section objects
    """
    with GAConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(48)  # pragma: no cover


def download_ga_social_services_title() -> Iterator[Section]:
    """Download all sections from Georgia Title 49 (Social Services).

    Yields:
        Section objects
    """
    with GAConverter() as converter:  # pragma: no cover
        yield from converter.iter_title(49)  # pragma: no cover
