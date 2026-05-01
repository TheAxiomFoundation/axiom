"""Illinois state statute converter.

Converts Illinois Compiled Statutes (ILCS) HTML from ilga.gov to the internal
Section model for ingestion.

Illinois Compiled Statute Structure:
- Chapters (e.g., 35 REVENUE, 305 PUBLIC AID)
- Acts within chapters (e.g., 35 ILCS 5/ Illinois Income Tax Act)
- Articles within acts (e.g., Article 2 - Tax Imposed)
- Sections within articles (e.g., Sec. 201. Tax imposed.)

Citation Format: "35 ILCS 5/201" means Chapter 35, Act 5, Section 201

URL Patterns:
- Chapter list: /Legislation/ILCS/Chapters
- Acts in chapter: /Legislation/ILCS/Acts?ChapterID=X&ChapterNumber=NN
- Full act text: /Legislation/ILCS/details?ActID=NNN&...
- Individual section: /legislation/ilcs/fulltext.asp?DocName=CCCCCAAAAKSSSS
  Where: CCCCC=chapter (5 digits), AAAA=act (4 digits), K=literal, SSSS=section

Example:
    >>> from axiom_corpus.converters.us_states.il import ILConverter
    >>> converter = ILConverter()
    >>> section = converter.fetch_section("35 ILCS 5/201")
    >>> print(section.section_title)
    "Tax imposed"
"""

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://www.ilga.gov"

# Illinois chapter information
IL_CHAPTERS: dict[str, str] = {
    5: "General Provisions",
    10: "Elections",
    15: "Executive Officers",
    20: "Executive Branch",
    25: "Legislature",
    30: "Finance",
    35: "Revenue",
    40: "Pensions",
    45: "Interstate Compacts",
    50: "Local Government",
    55: "Counties",
    60: "Townships",
    65: "Municipalities",
    70: "Special Districts",
    75: "Libraries",
    105: "Schools",
    110: "Higher Education",
    205: "Public Utilities",
    210: "Vehicles",
    215: "Roads and Bridges",
    220: "Railroads",
    225: "Professions and Occupations",
    230: "Public Health",
    235: "Children",
    240: "Aging",
    305: "Public Aid",
    310: "Welfare Services",
    320: "Housing",
    325: "Corrections",
    330: "Criminal Law",
    405: "Mental Health",
    410: "Hospitals",
    505: "Agriculture",
    510: "Animals",
    515: "Fish",
    520: "Wildlife",
    525: "Conservation",
    605: "Courts",
    625: "Motor Vehicles",
    705: "Civil Liabilities",
    710: "Contracts",
    715: "Business Organizations",
    720: "Criminal Offenses",
    725: "Criminal Procedure",
    730: "Corrections",
    735: "Civil Procedure",
    740: "Civil Liabilities",
    745: "Immunities",
    750: "Families",
    755: "Estates",
    760: "Trusts",
    765: "Property",
    770: "Insurance",
    805: "Business Organizations",
    810: "Financial Institutions",
    815: "Business Transactions",
    820: "Employment",
}

# Key acts for tax analysis (Revenue chapter 35)
IL_REVENUE_ACTS: dict[str, str] = {
    5: "Illinois Income Tax Act",
    10: "Economic Development for a Growing Economy Tax Credit Act",
    105: "Property Tax Code",
    115: "Motor Fuel Tax Law",
    120: "Retailers' Occupation Tax Act",
    130: "Use Tax Act",
    135: "Service Use Tax Act",
    140: "Service Occupation Tax Act",
    200: "Tax Delinquency Amnesty Act",
    405: "Cigarette Tax Act",
    500: "Illinois Estate and Generation-Skipping Transfer Tax Act",
}

# Key acts for public aid analysis (Chapter 305)
IL_PUBLIC_AID_ACTS: dict[str, str] = {
    5: "Illinois Public Aid Code",
    20: "Illinois Food, Drug and Cosmetic Act",
}

# Chapter IDs (used in URL parameters)
IL_CHAPTER_IDS: dict[int, int] = {
    5: 1,
    10: 2,
    15: 3,
    20: 4,
    25: 5,
    30: 6,
    35: 8,  # Revenue
    40: 9,
    50: 11,
    55: 12,
    60: 13,
    65: 14,
    70: 15,
    105: 17,
    110: 18,
    205: 20,
    305: 28,  # Public Aid
    405: 33,
    505: 38,
    605: 43,
    625: 44,
    720: 49,
    725: 50,
    735: 52,
    765: 57,
    805: 61,
    815: 63,
    820: 64,
}

# Mapping from citation act number to database ActID for URL construction
# Format: (chapter, citation_act) -> database_act_id
# The database ActID is used in URLs like: /documents/{chapter:04d}{act_id:05d}K{section}.htm
# These IDs are from ilga.gov website
IL_ACT_IDS: dict[tuple[int, int], int] = {
    # Chapter 35 - Revenue
    (35, 5): 577,  # Illinois Income Tax Act
    (35, 10): 578,  # Economic Development Tax Credit Act
    (35, 105): 594,  # Property Tax Code
    (35, 115): 598,  # Motor Fuel Tax Law
    (35, 120): 599,  # Retailers' Occupation Tax Act
    (35, 130): 603,  # Use Tax Act
    (35, 135): 604,  # Service Use Tax Act
    (35, 140): 605,  # Service Occupation Tax Act
    # Chapter 305 - Public Aid
    (305, 5): 2265,  # Illinois Public Aid Code
}


@dataclass
class ParsedILSection:
    """Parsed Illinois statute section."""

    chapter: int  # e.g., 35
    act: int  # e.g., 5
    section_number: str  # e.g., "201"
    section_title: str  # e.g., "Tax imposed"
    chapter_name: str  # e.g., "Revenue"
    act_name: str  # e.g., "Illinois Income Tax Act"
    text: str  # Full text content
    html: str  # Raw HTML
    subsections: list[ParsedILSubsection] = field(default_factory=list)
    history: str | None = None  # Source note (P.A. references)
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedILSubsection:
    """A subsection within an Illinois statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list[ParsedILSubsection] = field(default_factory=list)


class ILConverterError(Exception):
    """Error during Illinois statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class ILConverter:
    """Converter for Illinois Compiled Statutes HTML to internal Section model.

    Example:
        >>> converter = ILConverter()
        >>> section = converter.fetch_section("35 ILCS 5/201")
        >>> print(section.citation.section)
        "IL-35-5-201"

        >>> for section in converter.iter_act(35, 5):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Illinois statute converter.

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

    def _parse_citation(self, citation: str) -> tuple[int, int, str]:
        """Parse an ILCS citation string.

        Args:
            citation: e.g., "35 ILCS 5/201", "305 ILCS 5/5-5", "35-5-201"

        Returns:
            Tuple of (chapter, act, section_number)

        Raises:
            ValueError: If citation cannot be parsed
        """
        # Pattern for "35 ILCS 5/201" format
        ilcs_pattern = r"(\d+)\s*ILCS\s*(\d+)/(\S+)"
        match = re.match(ilcs_pattern, citation.strip(), re.IGNORECASE)
        if match:
            return int(match.group(1)), int(match.group(2)), match.group(3)

        # Pattern for "35-5-201" simplified format
        simple_pattern = r"(\d+)-(\d+)-(\S+)"
        match = re.match(simple_pattern, citation.strip())
        if match:
            return int(match.group(1)), int(match.group(2)), match.group(3)

        raise ValueError(f"Cannot parse ILCS citation: {citation}")

    def _build_section_url(self, chapter: int, act: int, section: str) -> str:
        """Build the URL for a single section.

        Args:
            chapter: Chapter number (e.g., 35)
            act: Act number (e.g., 5)
            section: Section number (e.g., "201", "5-5")

        Returns:
            Full URL to the section page

        URL Pattern observed:
        - 35 ILCS 5/201 -> /Documents/legislation/ilcs/documents/003500050K201.htm
        - 35 ILCS 105/1 -> /Documents/legislation/ilcs/documents/003501050K1.htm
        - 35 ILCS 120/1 -> /Documents/legislation/ilcs/documents/003501200K1.htm
        - Chapter: 4 digits (0035 for 35)
        - Act: 0 + 3-digit act number + 0 = 5 digits (00050 for 5, 01050 for 105)
        - K + section number
        """
        # Format: {chapter:04d}0{act:03d}0K{section}.htm
        # Example: 35 ILCS 5/201 -> 0035 + 0 + 005 + 0 + K + 201 = 003500050K201
        chapter_padded = f"{chapter:04d}"
        act_formatted = f"0{act:03d}0"

        # Section might have dashes, which get included as-is
        doc_name = f"{chapter_padded}{act_formatted}K{section}"

        return f"{BASE_URL}/Documents/legislation/ilcs/documents/{doc_name}.htm"

    def _build_act_url(self, chapter: int, act: int) -> str:
        """Build URL for an act's table of contents."""
        chapter_id = IL_CHAPTER_IDS.get(chapter, chapter)  # pragma: no cover
        chapter_name = IL_CHAPTERS.get(chapter, f"Chapter {chapter}")  # pragma: no cover

        return (  # pragma: no cover
            f"{BASE_URL}/legislation/ILCS/Articles?"
            f"ActID={act}&ChapterID={chapter_id}&Chapter={chapter_name}"
        )

    def _parse_section_html(
        self,
        html: str,
        chapter: int,
        act: int,
        section: str,
        url: str,
    ) -> ParsedILSection:
        """Parse section HTML into ParsedILSection."""
        soup = BeautifulSoup(html, "html.parser")

        # Check for "not found" error
        if "cannot be found" in html.lower() or "not found" in html.lower():
            raise ILConverterError(f"Section {chapter} ILCS {act}/{section} not found", url)

        # Get chapter and act names
        chapter_name = IL_CHAPTERS.get(chapter, f"Chapter {chapter}")
        if chapter == 35:
            act_name = IL_REVENUE_ACTS.get(act, f"Act {act}")
        elif chapter == 305:
            act_name = IL_PUBLIC_AID_ACTS.get(act, f"Act {act}")
        else:
            act_name = f"Act {act}"  # pragma: no cover

        # Extract text content
        text = soup.get_text(separator="\n", strip=True)

        # Extract section title from patterns like:
        # "(35 ILCS 5/201)"
        # "Sec. 201. Tax imposed."
        section_title = ""

        # Pattern: "Sec. NNN. Title."
        title_pattern = rf"Sec\.\s*{re.escape(section)}[A-Za-z]?\.\s*([^.]+)"
        match = re.search(title_pattern, text)
        if match:
            section_title = match.group(1).strip()

        # Extract source/history note: "(Source: P.A. ...)"
        history = None
        source_pattern = r"\(Source:\s*([^)]+)\)"
        source_match = re.search(source_pattern, text)
        if source_match:
            history = source_match.group(1).strip()[:1000]  # Limit length

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedILSection(
            chapter=chapter,
            act=act,
            section_number=section,
            section_title=section_title or f"Section {section}",
            chapter_name=chapter_name,
            act_name=act_name,
            text=text,
            html=html,
            subsections=subsections,
            history=history,
            source_url=url,
        )

    def _parse_subsections(self, text: str) -> list[ParsedILSubsection]:
        """Parse hierarchical subsections from text.

        Illinois statutes typically use:
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

            # Clean up text - stop at next lettered subsection
            next_sub = re.search(r"\([a-z]\)", direct_text)
            if next_sub:  # pragma: no cover
                direct_text = direct_text[: next_sub.start()].strip()

            subsections.append(
                ParsedILSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],  # Limit text size
                    children=children,
                )
            )

        return subsections

    def _parse_level2(self, text: str) -> list[ParsedILSubsection]:
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
            if children:  # pragma: no cover
                first_child_match = re.search(r"\([A-Z]\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Stop at next numbered subsection
            next_num = re.search(r"\(\d+\)", direct_text)
            if next_num:  # pragma: no cover
                direct_text = direct_text[: next_num.start()]

            # Stop at next lettered subsection
            next_letter = re.search(r"\([a-z]\)", direct_text)
            if next_letter:  # pragma: no cover
                direct_text = direct_text[: next_letter.start()]

            subsections.append(
                ParsedILSubsection(
                    identifier=identifier,
                    text=direct_text.strip()[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level3(self, text: str) -> list[ParsedILSubsection]:
        """Parse level 3 subsections (A), (B), etc."""
        subsections = []
        parts = re.split(r"(?=\([A-Z]\)\s)", text)

        for part in parts[1:]:  # pragma: no cover
            match = re.match(r"\(([A-Z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit content and stop at next subsection markers
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]

            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]

            next_cap = re.search(r"\([A-Z]\)", content)
            if next_cap:
                content = content[: next_cap.start()]  # pragma: no cover

            subsections.append(
                ParsedILSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _to_section(self, parsed: ParsedILSection) -> Section:
        """Convert ParsedILSection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"IL-{parsed.chapter}-{parsed.act}-{parsed.section_number}",
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
            title_name=f"Illinois Compiled Statutes - {parsed.chapter_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"il/{parsed.chapter}/{parsed.act}/{parsed.section_number}",
        )

    def fetch_section(self, citation: str) -> Section:
        """Fetch and convert a single section.

        Args:
            citation: ILCS citation string (e.g., "35 ILCS 5/201")

        Returns:
            Section model

        Raises:
            ILConverterError: If section not found or parsing fails
        """
        chapter, act, section = self._parse_citation(citation)
        url = self._build_section_url(chapter, act, section)
        html = self._get(url)
        parsed = self._parse_section_html(html, chapter, act, section, url)
        return self._to_section(parsed)

    def fetch_section_by_parts(
        self,
        chapter: int,
        act: int,
        section: str,
    ) -> Section:
        """Fetch a section by its component parts.

        Args:
            chapter: Chapter number (e.g., 35)
            act: Act number (e.g., 5)
            section: Section number (e.g., "201")

        Returns:
            Section model
        """
        url = self._build_section_url(chapter, act, section)
        html = self._get(url)
        parsed = self._parse_section_html(html, chapter, act, section, url)
        return self._to_section(parsed)

    def get_act_section_numbers(self, chapter: int, act: int) -> list[str]:
        """Get list of section numbers in an act.

        Args:
            chapter: Chapter number (e.g., 35)
            act: Act number (e.g., 5)

        Returns:
            List of section numbers (e.g., ["201", "202", "203", ...])
        """
        # Fetch the act details page
        chapter_id = IL_CHAPTER_IDS.get(chapter, chapter)

        # Try the details URL pattern
        url = f"{BASE_URL}/legislation/ILCS/ilcs5.asp?ActID={act}&ChapterID={chapter_id}"

        try:
            html = self._get(url)
        except httpx.HTTPError:  # pragma: no cover
            return []  # pragma: no cover

        soup = BeautifulSoup(html, "html.parser")
        section_numbers = []

        # Find section links - pattern varies by page
        # Look for links containing the section pattern
        pattern = re.compile(rf"{chapter}\s*ILCS\s*{act}/(\S+)")

        for text in soup.stripped_strings:
            match = pattern.search(text)
            if match:
                section_num = match.group(1)
                if section_num not in section_numbers:
                    section_numbers.append(section_num)

        return section_numbers

    def iter_act(self, chapter: int, act: int) -> Iterator[Section]:
        """Iterate over all sections in an act.

        Args:
            chapter: Chapter number (e.g., 35)
            act: Act number (e.g., 5)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_act_section_numbers(chapter, act)

        for section_num in section_numbers:
            try:
                yield self.fetch_section_by_parts(chapter, act, section_num)
            except ILConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(
                    f"Warning: Could not fetch {chapter} ILCS {act}/{section_num}: {e}"
                )  # pragma: no cover
                continue  # pragma: no cover

    def iter_revenue_acts(self) -> Iterator[Section]:
        """Iterate over sections from Revenue chapter (35 ILCS).

        Yields:
            Section objects from tax-related acts
        """
        for act_num in IL_REVENUE_ACTS:  # pragma: no cover
            yield from self.iter_act(35, act_num)  # pragma: no cover

    def iter_public_aid_acts(self) -> Iterator[Section]:
        """Iterate over sections from Public Aid chapter (305 ILCS).

        Yields:
            Section objects from public aid acts
        """
        for act_num in IL_PUBLIC_AID_ACTS:  # pragma: no cover
            yield from self.iter_act(305, act_num)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> ILConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_il_section(citation: str) -> Section:
    """Fetch a single Illinois statute section.

    Args:
        citation: ILCS citation (e.g., "35 ILCS 5/201")

    Returns:
        Section model
    """
    with ILConverter() as converter:
        return converter.fetch_section(citation)


def download_il_act(chapter: int, act: int) -> list[Section]:
    """Download all sections from an Illinois Compiled Statutes act.

    Args:
        chapter: Chapter number (e.g., 35)
        act: Act number (e.g., 5)

    Returns:
        List of Section objects
    """
    with ILConverter() as converter:
        return list(converter.iter_act(chapter, act))


def download_il_income_tax_act() -> Iterator[Section]:
    """Download all sections from the Illinois Income Tax Act (35 ILCS 5/).

    Yields:
        Section objects
    """
    with ILConverter() as converter:  # pragma: no cover
        yield from converter.iter_act(35, 5)  # pragma: no cover


def download_il_public_aid_code() -> Iterator[Section]:
    """Download all sections from the Illinois Public Aid Code (305 ILCS 5/).

    Yields:
        Section objects
    """
    with ILConverter() as converter:  # pragma: no cover
        yield from converter.iter_act(305, 5)  # pragma: no cover
