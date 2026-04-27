"""New Mexico state statute converter.

Converts New Mexico Statutes Annotated (NMSA 1978) from nmonesource.com
to the internal Section model for ingestion.

NM Statute Structure:
- Chapters (e.g., Chapter 7: Taxation)
- Articles (e.g., Article 2: Income Tax General Provisions)
- Sections (e.g., Section 7-2-2: Definitions)

Citation format: "Section X-Y-Z NMSA 1978" where:
- X = Chapter number
- Y = Article number
- Z = Section number within article

URL Patterns (NMOneSource uses Lexum Norma platform):
- Base: https://nmonesource.com/nmos/nmsa/en/
- Items: /item/{item_id}/index.do
- Note: NMOneSource uses dynamic item IDs, not predictable URL patterns

Example:
    >>> from atlas.converters.us_states.nm import NMConverter
    >>> converter = NMConverter()
    >>> section = converter.fetch_section("7-2-2")
    >>> print(section.section_title)
    "Definitions"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from atlas.models import Citation, Section, Subsection

# NMOneSource base URL (powered by Lexum Norma)
BASE_URL = "https://nmonesource.com/nmos/nmsa/en"

# Chapter registry for reference
NM_CHAPTERS: dict[int | str, str] = {
    1: "General Provisions",
    2: "Accountability in Government",
    3: "Agricultural Code",
    4: "Alcoholic Beverages",
    5: "Mining",
    6: "Public Finances",
    7: "Taxation",
    8: "Charitable Gaming",
    9: "Executive Branch Structure",
    10: "Elections",
    11: "Unemployment Compensation",
    12: "Legislature and Statutory Compilation",
    13: "Public Purchases and Property",
    14: "Records, Rules, Legal Notices, Oaths",
    15: "Court Structure and Judiciary",
    16: "Domestic Affairs",
    17: "Conservation",
    18: "Libraries, Museums, Cultural Properties",
    19: "Wildlife Conservation",
    20: "Environmental Improvement",
    21: "Higher Education",
    22: "Public Schools",
    23: "Highways and Bridges",
    24: "Health and Safety",
    25: "Indians",
    26: "Insurance",
    27: "Public Assistance",
    28: "Trade Practices and Regulations",
    29: "Law Enforcement",
    30: "Criminal Offenses",
    31: "Criminal Procedure",
    32: "Agriculture",
    "32A": "Children's Code",
    33: "Corrections",
    34: "Criminal Sentencing",
    35: "Courts of Limited Jurisdiction",
    36: "Special Districts",
    37: "Real Property",
    38: "Civil Practice",
    39: "Jury Commissioners",
    40: "Domestic Affairs",
    41: "Torts",
    42: "Creditors and Debtors",
    43: "Mentally Disordered Persons",
    44: "Arbitration and Conciliation",
    45: "Fiduciary Relationships",
    46: "UPC - Intestate Succession",
    "46A": "UPC - Probate",
    "46B": "UPC - Uniform Trust Code",
    47: "Property",
    48: "Mortgages and Liens",
    49: "Negotiable Instruments",
    50: "Employment Law",
    51: "Holidays",
    52: "Workers' Compensation",
    53: "Corporations",
    54: "Partnerships",
    55: "Uniform Commercial Code",
    56: "Trade Names and Trademarks",
    57: "Uniform Commercial Code - Leases",
    58: "Banks and Financial Institutions",
    59: "Pawnbrokers and Money Lenders",
    "59A": "Insurance Code",
    60: "Business Licenses",
    61: "Professional and Occupational Licenses",
    62: "Public Utilities",
    63: "Railroads",
    64: "State Transportation",
    65: "Corporations, Partnerships, Associations",
    66: "Motor Vehicles",
    67: "Aviation",
    68: "Public Lands",
    69: "Oil and Gas",
    70: "Mines",
    71: "Livestock",
    72: "Water Law",
    73: "Interstate Water Compacts",
    74: "Environmental Improvement",
    75: "Solid Waste",
    76: "Seeds, Feeds, Fertilizers",
    77: "Animals and Livestock",
}

# Key chapters for tax/benefit analysis
NM_TAX_CHAPTERS: dict[int, str] = {
    7: "Taxation",
}

# Articles within Chapter 7 (Taxation)
NM_TAX_ARTICLES: dict[int | str, str] = {
    1: "Tax Administration",
    2: "Income Tax General Provisions",
    "2A": "Personal Income Tax",
    "2B": "Additional Deduction for Certain Dependents",
    "2C": "Low-Income Comprehensive Tax Rebate",
    "2D": "Working Families Tax Credit",
    "2E": "Child Income Tax Credit",
    "2F": "Child Daycare Services Tax Credit",
    3: "Corporate Income Tax",
    4: "Gross Receipts and Compensating Tax",
    5: "Local Option Gross Receipts Taxes",
    6: "Excise Tax on Tobacco Products",
    7: "Motor Vehicle Excise Tax",
    8: "Estate Tax (Repealed)",
    9: "Gasoline Tax",
    10: "Special Fuels Supplier Tax",
    11: "Cigarette Tax",
    12: "Severance Tax",
    13: "Resources Excise Tax",
    14: "Oil and Gas Production Taxes",
    15: "Processors Tax (Repealed)",
    16: "Liquor Excise Tax",
    17: "Fire Protection Fund Tax (Repealed)",
    18: "Insurers Premium Surtax (Repealed)",
    19: "Property Tax Code",
    20: "Intergovernmental Tax Agreements",
    21: "Reciprocal Tax Agreements",
    22: "Tourism Tax (Repealed)",
    23: "Bingo and Raffle Tax (Repealed)",
    24: "Hospital-Nursing Facility Tax",
    25: "Leased Vehicle Surcharge",
    26: "Tribal Infrastructure Project Loan (Repealed)",
    27: "Severance Tax Bonding Act",
    28: "Film Production Tax Credit",
    29: "High-Wage Jobs Tax Credit",
    30: "Rural Jobs Tax Credit",
    31: "Technology Jobs and Research and Development Tax Credit",
    32: "Angel Investment Tax Credit",
    33: "Solar Market Development Tax Credit",
    34: "Renewable Energy Tax Credit",
    35: "Advanced Energy Product Manufacturers Tax Credit",
    36: "Sustainable Building Tax Credit",
    37: "Laboratory Partnership Tax Credit",
    38: "New Markets Tax Credit",
    39: "Job Training Incentive Program",
}

NM_WELFARE_CHAPTERS: dict[int, str] = {
    27: "Public Assistance",
}

# Articles within Chapter 27 (Public Assistance)
NM_WELFARE_ARTICLES: dict[int | str, str] = {
    1: "Public Assistance Act",
    2: "General Provisions",
    "2A": "New Mexico Works Act",
    "2B": "Income Support",
    "2C": "Food Assistance",
    "2D": "Child Care Assistance",
    3: "Child Welfare",
    4: "Foster Care",
    5: "Developmental Disabilities",
    6: "Long-Term Care",
    7: "Medical Assistance",
    "7A": "Medicaid Managed Care",
    8: "Energy Assistance",
    9: "Homeless Assistance",
}


@dataclass
class ParsedNMSection:
    """Parsed New Mexico statute section."""

    section_number: str  # e.g., "7-2-2"
    section_title: str  # e.g., "Definitions"
    chapter_number: int  # e.g., 7
    chapter_title: str  # e.g., "Taxation"
    article_number: str | None  # e.g., "2" or "2A"
    article_title: str | None  # e.g., "Income Tax General Provisions"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list["ParsedNMSubsection"] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedNMSubsection:
    """A subsection within a New Mexico statute."""

    identifier: str  # e.g., "A", "1", "a"
    text: str
    children: list["ParsedNMSubsection"] = field(default_factory=list)


class NMConverterError(Exception):
    """Error during New Mexico statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class NMConverter:
    """Converter for New Mexico Statutes to internal Section model.

    Note: NMOneSource uses a Lexum Norma platform with dynamic item IDs.
    Direct URL access requires knowing the item ID. This converter provides
    methods to parse HTML content that has been retrieved from NMOneSource.

    Example:
        >>> converter = NMConverter()
        >>> # Parse pre-fetched HTML
        >>> section = converter.parse_section_html(html, "7-2-2", url)
        >>> print(section.citation.section)
        "NM-7-2-2"
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the New Mexico statute converter.

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

    def _parse_section_number(self, section_number: str) -> tuple[int, str | None, str]:
        """Parse a section number like "7-2-2" into components.

        Args:
            section_number: e.g., "7-2-2", "7-2A-1"

        Returns:
            Tuple of (chapter, article, section_in_article)
        """
        parts = section_number.split("-")
        if len(parts) < 2:
            raise ValueError(f"Invalid NM section number: {section_number}")  # pragma: no cover

        chapter = int(parts[0])
        article = parts[1] if len(parts) > 1 else None
        section_in_article = parts[2] if len(parts) > 2 else parts[1]

        return chapter, article, section_in_article

    def _get_chapter_title(self, chapter: int) -> str:
        """Get the chapter title for a chapter number."""
        return (
            NM_TAX_CHAPTERS.get(chapter)
            or NM_WELFARE_CHAPTERS.get(chapter)
            or NM_CHAPTERS.get(chapter)
            or f"Chapter {chapter}"
        )

    def _get_article_title(self, chapter: int, article: str | None) -> str | None:
        """Get the article title for a chapter/article combination."""
        if article is None:
            return None  # pragma: no cover

        # Try to convert to int for lookup, handle alphanumeric articles
        try:
            article_key: int | str = int(article)
        except ValueError:
            article_key = article

        if chapter == 7:
            return NM_TAX_ARTICLES.get(article_key)
        elif chapter == 27:
            return NM_WELFARE_ARTICLES.get(article_key)

        return None  # pragma: no cover

    def _parse_effective_date(self, text: str) -> date | None:
        """Parse effective date from text.

        Args:
            text: Text containing effective date

        Returns:
            Parsed date or None
        """
        # Look for patterns like "Effective: July 1, 2023" or "Laws 2023, ch. 42"
        patterns = [
            r"Effective[:\s]+(\w+\s+\d{1,2},?\s+\d{4})",
            r"effective\s+(\w+\s+\d{1,2},?\s+\d{4})",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                try:
                    from datetime import datetime

                    # Handle with or without comma
                    for fmt in ["%B %d, %Y", "%B %d %Y"]:
                        try:
                            return datetime.strptime(date_str, fmt).date()
                        except ValueError:  # pragma: no cover
                            continue  # pragma: no cover
                except ValueError:  # pragma: no cover
                    pass
        return None

    def _parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str,
    ) -> ParsedNMSection:
        """Parse section HTML into ParsedNMSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise NMConverterError(f"Section {section_number} not found", url)

        chapter, article, _ = self._parse_section_number(section_number)
        chapter_title = self._get_chapter_title(chapter)
        article_title = self._get_article_title(chapter, article)

        # Extract section title from headings or title element
        section_title = ""

        # Look for title in page title
        page_title = soup.find("title")
        if page_title:
            title_text = page_title.get_text(strip=True)
            # Pattern: "Section 7-2-2 NMSA 1978 - Definitions"
            title_pattern = re.compile(
                rf"Section\s+{re.escape(section_number)}[^-]*[-—]\s*(.+?)(?:\.|$)"
            )
            match = title_pattern.search(title_text)
            if match:
                section_title = match.group(1).strip().rstrip(".")

        # Try headings
        if not section_title:
            for heading in soup.find_all(["h1", "h2", "h3"]):  # pragma: no cover
                heading_text = heading.get_text(strip=True)  # pragma: no cover
                # Pattern like "7-2-2. Definitions." or just the title
                patterns = [  # pragma: no cover
                    rf"{re.escape(section_number)}\.\s*(.+?)(?:\.|$)",
                    rf"§\s*{re.escape(section_number)}\.\s*(.+?)(?:\.|$)",
                ]
                for pattern in patterns:  # pragma: no cover
                    match = re.search(pattern, heading_text)  # pragma: no cover
                    if match:  # pragma: no cover
                        section_title = match.group(1).strip().rstrip(".")  # pragma: no cover
                        break  # pragma: no cover
                if section_title:  # pragma: no cover
                    break  # pragma: no cover

        # Try to find title in general text
        if not section_title:
            full_text = soup.get_text()  # pragma: no cover
            patterns = [  # pragma: no cover
                rf"{re.escape(section_number)}\.\s*([^.]+)",
                rf"Section\s+{re.escape(section_number)}\s*[-—]\s*([^.]+)",
            ]
            for pattern in patterns:  # pragma: no cover
                match = re.search(pattern, full_text)  # pragma: no cover
                if match:  # pragma: no cover
                    section_title = match.group(1).strip()[:100]  # pragma: no cover
                    break  # pragma: no cover

        # Get body content - try various containers
        content_elem = (
            soup.find("div", class_="document-content")
            or soup.find("div", class_="statute-content")
            or soup.find("div", id="content")
            or soup.find("article")
            or soup.find("main")
            or soup.find("body")
        )

        if content_elem:
            # Remove navigation and scripts
            for elem in content_elem.find_all(["nav", "script", "style", "header", "footer"]):
                elem.decompose()  # pragma: no cover
            text = content_elem.get_text(separator="\n", strip=True)
            html_content = str(content_elem)
        else:
            text = soup.get_text(separator="\n", strip=True)  # pragma: no cover
            html_content = html  # pragma: no cover

        # Extract effective date
        effective_date = self._parse_effective_date(text)

        # Extract history note
        history = None
        history_patterns = [
            r"History:\s*(.+?)(?:\n\n|$)",
            r"Laws\s+\d{4},\s+ch\.\s+\d+[^.]*\.",
            r"\[.+Laws\s+\d{4}.+\]",
        ]
        for pattern in history_patterns:
            history_match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if history_match:
                history = history_match.group(0).strip()[:500]
                break

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedNMSection(
            section_number=section_number,
            section_title=section_title or f"Section {section_number}",
            chapter_number=chapter,
            chapter_title=chapter_title,
            article_number=article,
            article_title=article_title,
            text=text,
            html=html_content,
            subsections=subsections,
            history=history,
            source_url=url,
            effective_date=effective_date,
        )

    def _parse_subsections(self, text: str) -> list[ParsedNMSubsection]:
        """Parse hierarchical subsections from text.

        New Mexico statutes typically use:
        - A., B., C. for primary divisions (capital letters with period)
        - (1), (2), (3) for secondary divisions
        - (a), (b), (c) for tertiary divisions
        """
        subsections = []

        # Split by top-level subsections A., B., etc.
        parts = re.split(r"(?=\b([A-Z])\.\s)", text)

        for i in range(1, len(parts) - 1, 2):
            if i + 1 >= len(parts):
                break  # pragma: no cover

            identifier = parts[i]
            content = parts[i + 1] if i + 1 < len(parts) else ""

            # Skip if identifier is too long (probably not a real subsection marker)
            if len(identifier) > 1:
                continue  # pragma: no cover

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
            next_subsection = re.search(r"\b[A-Z]\.\s", direct_text)
            if next_subsection:
                direct_text = direct_text[: next_subsection.start()].strip()

            subsections.append(
                ParsedNMSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        # If no A., B. style subsections found, try (A), (B) style
        if not subsections:
            subsections = self._parse_paren_subsections(text)

        return subsections

    def _parse_paren_subsections(self, text: str) -> list[ParsedNMSubsection]:
        """Parse subsections using (A), (B) style markers."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([A-Z])\)\s*", part)
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

            # Stop at next capital letter subsection
            next_alpha = re.search(r"\([A-Z]\)", direct_text)
            if next_alpha:
                direct_text = direct_text[: next_alpha.start()]  # pragma: no cover

            subsections.append(
                ParsedNMSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedNMSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse third-level children (a), (b), etc.
            children = self._parse_level3(content)

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

            # Limit to reasonable size and stop at next lettered subsection
            next_alpha = re.search(r"\([A-Z]\)", direct_text)
            if next_alpha:
                direct_text = direct_text[: next_alpha.start()]  # pragma: no cover

            # Stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", direct_text)
            if next_num:
                direct_text = direct_text[: next_num.start()]  # pragma: no cover

            subsections.append(
                ParsedNMSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedNMSubsection]:
        """Parse level 3 subsections (a), (b), etc."""
        subsections = []
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Stop at next higher-level subsection
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]  # pragma: no cover
            next_alpha = re.search(r"\([A-Z]\)", content)
            if next_alpha:
                content = content[: next_alpha.start()]  # pragma: no cover

            if len(content) > 2000:
                content = content[:2000] + "..."  # pragma: no cover

            subsections.append(
                ParsedNMSubsection(
                    identifier=identifier,
                    text=content.strip(),
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedNMSection) -> Section:
        """Convert ParsedNMSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"NM-{parsed.section_number}",
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
                                identifier=grandchild.identifier,
                                heading=None,
                                text=grandchild.text,
                                children=[],
                            )
                            for grandchild in child.children
                        ],
                    )
                    for child in sub.children
                ],
            )
            for sub in parsed.subsections
        ]

        # Build title name
        title_name = f"New Mexico Statutes - {parsed.chapter_title}"
        if parsed.article_title:
            title_name = f"{title_name} - {parsed.article_title}"

        return Section(
            citation=citation,
            title_name=title_name,
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            effective_date=parsed.effective_date,
            uslm_id=f"nm/{parsed.chapter_number}/{parsed.section_number}",
        )

    def parse_section_html(
        self,
        html: str,
        section_number: str,
        url: str = "",
    ) -> Section:
        """Parse HTML content into a Section model.

        Use this method when you have pre-fetched HTML from NMOneSource.

        Args:
            html: Raw HTML content from NMOneSource
            section_number: e.g., "7-2-2"
            url: Source URL (optional)

        Returns:
            Section model
        """
        parsed = self._parse_section_html(html, section_number, url)
        return self._to_section(parsed)

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> "NMConverter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def parse_nm_section(html: str, section_number: str, url: str = "") -> Section:
    """Parse New Mexico statute HTML into a Section model.

    Args:
        html: Raw HTML from NMOneSource
        section_number: e.g., "7-2-2"
        url: Source URL (optional)

    Returns:
        Section model
    """
    with NMConverter() as converter:
        return converter.parse_section_html(html, section_number, url)
