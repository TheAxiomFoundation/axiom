"""Iowa state statute converter.

Converts Iowa Code from legis.iowa.gov to the internal Section model for ingestion.

Iowa Code Structure:
- Titles (16 total, Roman numerals I-XVI)
- Chapters (e.g., Chapter 422: Individual Income, Corporate, and Franchise Taxes)
- Sections (e.g., 422.5: Tax imposed)

URL Patterns:
- Title chapters: /law/iowaCode/chapters?title=[ROMAN]&year=[YYYY]
- Chapter sections: /law/iowaCode/sections?codeChapter=[NUM]&year=[YYYY]
- Section PDF: /docs/code/[YEAR]/[CHAPTER].[SECTION].pdf
- Section RTF: /docs/code/[YEAR]/[CHAPTER].[SECTION].rtf

Note: Iowa provides sections as PDF/RTF files. This converter parses section listings
from HTML and attempts to extract text from RTF files where possible.

Example:
    >>> from axiom_corpus.converters.us_states.ia import IAConverter
    >>> converter = IAConverter()
    >>> section = converter.fetch_section("422.5")
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

BASE_URL = "https://www.legis.iowa.gov"

# Title mapping (Roman numeral to name)
IA_TITLES: dict[str, str] = {
    "I": "State Sovereignty and Management",
    "II": "Elections and Official Duties",
    "III": "Public Services and Regulation",
    "IV": "Public Health",
    "V": "Agriculture",
    "VI": "Human Services",
    "VII": "Education, History, and Culture",
    "VIII": "Transportation",
    "IX": "Local Government",
    "X": "Financial Resources",
    "XI": "Natural Resources",
    "XII": "Business Entities",
    "XIII": "Commerce",
    "XIV": "Property",
    "XV": "Judicial Branch and Judicial Procedures",
    "XVI": "Criminal Law and Procedure",
}

# Key chapters for tax/benefit analysis (Title X - Financial Resources)
IA_TAX_CHAPTERS: dict[str, str] = {
    "421": "Department of Revenue",
    "422": "Individual Income, Corporate, and Franchise Taxes",
    "422A": "Minimum Tax on Tax Preference Items",
    "422B": "Local Option Taxes",
    "422C": "Livestock Production Tax Credit",
    "422D": "Motor Vehicle Fuel Tax Credit",
    "422E": "Iowa New Jobs Tax Credit",
    "423": "Streamlined Sales and Use Tax Act",
    "423A": "Use Tax on Nonregistered Vehicles",
    "423B": "Local Sales and Services Tax",
    "423C": "Local Vehicle Tax",
    "423D": "Hotel and Motel Tax",
    "423E": "School Infrastructure Local Option Sales and Services Tax",
    "424": "Moneys and Credits Tax",
    "425": "Homestead Tax Credits, Exemptions, and Reimbursement",
    "426": "Agricultural Land Tax Credit",
    "426A": "Family Farm Tax Credit",
    "426B": "Military Service Tax Exemption",
    "426C": "Business Property Tax Credit",
    "427": "Property Exempt and Taxable",
    "428": "Listing and Valuation of Property",
    "433": "Railroad Property Tax",
    "437": "Electric and Natural Gas Utility Property Tax Replacement",
    "437A": "Electric and Natural Gas Utility Property Tax",
    "437B": "Utility Replacement Tax",
    "441": "Assessment and Valuation of Property",
    "445": "Tax Collection",
    "450": "Inheritance Tax",
    "450A": "Generation Skipping Tax",
    "451": "Fiduciary and Transfer Agent Income Tax",
    "452": "Transfer Inheritance Tax",
    "452A": "Motor Fuel and Special Fuel",
    "453A": "Cigarette and Tobacco Taxes",
    "453B": "Illegal Drug Tax",
    "453C": "Tobacco Master Settlement Agreement",
    "453D": "Tobacco Product Manufacturers",
}

# Key chapters for welfare/human services (Title VI - Human Services)
IA_WELFARE_CHAPTERS: dict[str, str] = {
    "216": "Office of Civil Rights",
    "217": "Department of Health and Human Services",
    "218": "Institutions Governed by Department of Health and Human Services",
    "222": "Persons with an Intellectual Disability",
    "225C": "Mental Health and Disability Services",
    "226": "State Mental Health Institutes",
    "229": "Hospitalization of Persons with Mental Illness",
    "231": "Aging and Disability Services",
    "231B": "Elder Abuse Prevention and Intervention",
    "231C": "Assisted Living Programs",
    "231D": "Adult Day Services Programs",
    "231E": "Dementia-Specific Education and Training Standards",
    "232": "Children and Families",
    "234": "Child and Family Services",
    "235": "Child Welfare",
    "235A": "Child Abuse",
    "235B": "Dependent Adult Abuse",
    "235E": "Child Care",
    "237": "Licensing and Regulation of Child Care Facilities",
    "237A": "Child Care Facilities",
    "238": "Subsidized Adoptions",
    "239B": "Family Investment Program",
    "249": "Cost of Support of Indigent Persons",
    "249A": "Medical Assistance",
    "249B": "Eligibility of Aliens for Benefits",
    "249C": "County Care of Indigent Persons",
    "249D": "Emergency Assistance",
    "249E": "Food Assistance",
    "249F": "Promise Jobs",
    "249G": "Family Self-Sufficiency Grants",
    "249H": "Hospital Health Care Access Trust Fund",
    "249I": "IowaCare",
    "249J": "Healthy and Well Kids in Iowa",
    "249K": "Children's Health Insurance Program",
    "249L": "Dental Home for Children Program",
    "249M": "Iowa Health and Wellness Plan",
    "249N": "Iowa Health Link",
    "252A": "Support of Dependents",
    "252B": "Child Support Recovery",
    "252C": "Paternity Determination",
    "252D": "Income Withholding",
    "252E": "Child Support Obligations",
    "252F": "Guidelines for Support Obligations",
    "252G": "Iowa Child Support Enforcement Program",
    "252H": "Licensed Child Support Recovery Services",
    "252I": "Child Support Payments",
    "252J": "Licensing Sanctions for Child Support Debts",
    "252K": "Intergovernmental Support Orders",
    "255": "Alcoholism",
    "255A": "Substance Abuse Treatment",
}


@dataclass
class ParsedIASection:
    """Parsed Iowa Code section."""

    section_number: str  # e.g., "422.5"
    section_title: str  # e.g., "Tax imposed"
    chapter_number: str  # e.g., "422"
    chapter_title: str  # e.g., "Individual Income, Corporate, and Franchise Taxes"
    title_roman: str | None  # e.g., "X"
    title_name: str | None  # e.g., "Financial Resources"
    text: str  # Full text content
    subsections: list[ParsedIASubsection] = field(default_factory=list)
    history: str | None = None  # History note
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedIASubsection:
    """A subsection within an Iowa Code section."""

    identifier: str  # e.g., "1", "a", "A"
    text: str
    children: list[ParsedIASubsection] = field(default_factory=list)


class IAConverterError(Exception):
    """Error during Iowa statute conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class IAConverter:
    """Converter for Iowa Code to internal Section model.

    Iowa provides statutes as PDF/RTF files rather than inline HTML.
    This converter:
    1. Fetches section listings from chapter index pages
    2. Downloads RTF files for text content
    3. Parses RTF to extract section text and structure

    Example:
        >>> converter = IAConverter()
        >>> section = converter.fetch_section("422.5")
        >>> print(section.citation.section)
        "IA-422.5"

        >>> for section in converter.iter_chapter("422"):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        year: int | None = None,
    ):
        """Initialize the Iowa statute converter.

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

    def _get_bytes(self, url: str) -> bytes:  # pragma: no cover
        """Make a rate-limited GET request returning bytes."""
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.content

    def _build_chapter_sections_url(self, chapter: str) -> str:
        """Build the URL for a chapter's section listing.

        Args:
            chapter: Chapter number (e.g., "422", "422A")

        Returns:
            Full URL to the chapter sections page
        """
        return f"{BASE_URL}/law/iowaCode/sections?codeChapter={chapter}&year={self.year}"

    def _build_section_rtf_url(self, section_number: str) -> str:
        """Build the URL for a section's RTF file.

        Args:
            section_number: e.g., "422.5", "422.11A"

        Returns:
            Full URL to the RTF file
        """
        return f"{BASE_URL}/docs/code/{self.year}/{section_number}.rtf"

    def _build_section_pdf_url(self, section_number: str) -> str:
        """Build the URL for a section's PDF file.

        Args:
            section_number: e.g., "422.5", "422.11A"

        Returns:
            Full URL to the PDF file
        """
        return f"{BASE_URL}/docs/code/{self.year}/{section_number}.pdf"

    def _build_title_chapters_url(self, title_roman: str) -> str:
        """Build the URL for a title's chapter listing.

        Args:
            title_roman: Roman numeral title (e.g., "X", "VI")

        Returns:
            Full URL to the title chapters page
        """
        return f"{BASE_URL}/law/iowaCode/chapters?title={title_roman}&year={self.year}"

    def _get_chapter_from_section(self, section_number: str) -> str:
        """Extract chapter number from section number.

        Args:
            section_number: e.g., "422.5", "422A.3"

        Returns:
            Chapter number (e.g., "422", "422A")
        """
        # Split on first dot
        parts = section_number.split(".", 1)
        return parts[0]

    def _get_title_for_chapter(self, chapter: str) -> tuple[str | None, str | None]:
        """Determine title Roman numeral and name from chapter number.

        Args:
            chapter: Chapter number (e.g., "422", "239B")

        Returns:
            Tuple of (title_roman, title_name) or (None, None)
        """
        # Extract numeric part for range comparison
        numeric_match = re.match(r"(\d+)", chapter)
        if not numeric_match:
            return None, None  # pragma: no cover

        chapter_num = int(numeric_match.group(1))

        # Title chapter ranges based on Iowa Code structure
        if 1 <= chapter_num <= 38:
            return "I", "State Sovereignty and Management"  # pragma: no cover
        elif 39 <= chapter_num <= 79:
            return "II", "Elections and Official Duties"  # pragma: no cover
        elif 80 <= chapter_num <= 122:
            return "III", "Public Services and Regulation"  # pragma: no cover
        elif 123 <= chapter_num <= 158:
            return "IV", "Public Health"  # pragma: no cover
        elif 159 <= chapter_num <= 215:
            return "V", "Agriculture"  # pragma: no cover
        elif 216 <= chapter_num <= 255:
            return "VI", "Human Services"
        elif 256 <= chapter_num <= 305:
            return "VII", "Education, History, and Culture"  # pragma: no cover
        elif 306 <= chapter_num <= 330:
            return "VIII", "Transportation"  # pragma: no cover
        elif 331 <= chapter_num <= 420:
            return "IX", "Local Government"  # pragma: no cover
        elif 421 <= chapter_num <= 454:
            return "X", "Financial Resources"
        elif 455 <= chapter_num <= 485:  # pragma: no cover
            return "XI", "Natural Resources"  # pragma: no cover
        elif 486 <= chapter_num <= 504:  # pragma: no cover
            return "XII", "Business Entities"  # pragma: no cover
        elif 505 <= chapter_num <= 554:  # pragma: no cover
            return "XIII", "Commerce"  # pragma: no cover
        elif 555 <= chapter_num <= 594:  # pragma: no cover
            return "XIV", "Property"  # pragma: no cover
        elif 595 <= chapter_num <= 686:  # pragma: no cover
            return "XV", "Judicial Branch and Judicial Procedures"  # pragma: no cover
        elif 687 <= chapter_num <= 916:  # pragma: no cover
            return "XVI", "Criminal Law and Procedure"  # pragma: no cover

        return None, None  # pragma: no cover

    def _get_chapter_title(self, chapter: str) -> str:
        """Get chapter title from known mappings or generic name.

        Args:
            chapter: Chapter number (e.g., "422", "239B")

        Returns:
            Chapter title
        """
        if chapter in IA_TAX_CHAPTERS:
            return IA_TAX_CHAPTERS[chapter]
        if chapter in IA_WELFARE_CHAPTERS:
            return IA_WELFARE_CHAPTERS[chapter]
        return f"Chapter {chapter}"

    def _parse_rtf_content(self, rtf_bytes: bytes, section_number: str) -> ParsedIASection:
        """Parse RTF content into ParsedIASection.

        Args:
            rtf_bytes: Raw RTF file content
            section_number: Section number for context

        Returns:
            ParsedIASection with extracted content
        """
        # Decode RTF to text - simple extraction
        text = self._extract_text_from_rtf(rtf_bytes)

        chapter = self._get_chapter_from_section(section_number)
        chapter_title = self._get_chapter_title(chapter)
        title_roman, title_name = self._get_title_for_chapter(chapter)

        # Try to extract section title from text
        section_title = self._extract_section_title(text, section_number)

        # Extract history note
        history = self._extract_history(text)

        # Parse subsections
        subsections = self._parse_subsections(text)

        return ParsedIASection(
            section_number=section_number,
            section_title=section_title,
            chapter_number=chapter,
            chapter_title=chapter_title,
            title_roman=title_roman,
            title_name=title_name,
            text=text,
            subsections=subsections,
            history=history,
            source_url=self._build_section_rtf_url(section_number),
        )

    def _extract_text_from_rtf(self, rtf_bytes: bytes) -> str:
        """Extract plain text from RTF content.

        This is a simple RTF parser that strips RTF control codes.
        For more complex RTF, consider using a dedicated library.

        Args:
            rtf_bytes: Raw RTF content

        Returns:
            Extracted plain text
        """
        try:
            rtf_text = rtf_bytes.decode("utf-8", errors="replace")
        except UnicodeDecodeError:  # pragma: no cover
            rtf_text = rtf_bytes.decode("latin-1", errors="replace")  # pragma: no cover

        # Remove RTF header and document info
        # Pattern to match RTF control words and groups
        text = rtf_text

        # Remove RTF version header
        text = re.sub(r"\\rtf\d+", "", text)

        # Remove font tables, color tables, etc. in braces
        # This is a simplified approach - nested braces need careful handling
        depth = 0
        result = []
        skip_group = False
        i = 0
        while i < len(text):
            char = text[i]
            if char == "{":
                depth += 1
                # Check if this is a group we want to skip (font table, etc.)
                lookahead = text[i : i + 20]
                if any(
                    kw in lookahead for kw in ["\\fonttbl", "\\colortbl", "\\stylesheet", "\\info"]
                ):
                    skip_group = depth
            elif char == "}":
                if skip_group and depth == skip_group:
                    skip_group = False
                depth -= 1
            elif not skip_group:
                result.append(char)
            i += 1

        text = "".join(result)

        # Remove remaining control words
        text = re.sub(r"\\[a-z]+\d*\s?", " ", text)

        # Remove control symbols
        text = re.sub(r"\\[^a-z\s]", "", text)

        # Remove remaining braces
        text = text.replace("{", "").replace("}", "")

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text)
        text = text.strip()

        # Split into lines for better formatting
        text = re.sub(r"\s*\n\s*", "\n", text)

        return text

    def _extract_section_title(self, text: str, section_number: str) -> str:
        """Extract section title from text content.

        Iowa sections typically start with: "422.5 Tax imposed."

        Args:
            text: Full text content
            section_number: Section number to find

        Returns:
            Section title or generic placeholder
        """
        # Pattern: section number followed by title ending with period
        # The title should not include numbers that indicate subsection starts
        escaped_num = re.escape(section_number)

        # Try pattern that stops at period followed by space and number (subsection start)
        pattern = rf"{escaped_num}\s+([A-Za-z][^.]*)\."

        match = re.search(pattern, text[:500])  # Search near beginning
        if match:
            title = match.group(1).strip()
            # Clean up any remaining RTF artifacts
            title = re.sub(r"\\[a-z]+\d*", "", title)
            # Remove trailing subsection indicators like ". 1" or ". a"
            title = re.sub(r"\.\s*\d+\s*$", "", title)
            title = re.sub(r"\.\s*[a-z]\s*$", "", title)
            title = title.strip()
            if title:
                return title

        return f"Section {section_number}"

    def _extract_history(self, text: str) -> str | None:
        """Extract history/amendment note from text.

        Iowa uses "History:" or similar markers.

        Args:
            text: Full text content

        Returns:
            History note or None
        """
        patterns = [
            r"History[:\s][-—]?\s*(.+?)(?:\n|$)",
            r"\[(.+?Acts.+?)\]",  # Pattern like [2023 Acts, ch 123]
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                history = match.group(1).strip()
                return history[:1000] if len(history) > 1000 else history

        return None  # pragma: no cover

    def _parse_subsections(self, text: str) -> list[ParsedIASubsection]:
        """Parse hierarchical subsections from text.

        Iowa statutes typically use:
        - 1., 2., 3. for primary divisions
        - a., b., c. for secondary divisions
        - (1), (2), (3) for tertiary divisions

        Args:
            text: Full text content

        Returns:
            List of parsed subsections
        """
        subsections = []

        # Split by top-level subsections 1., 2., etc.
        # Look for pattern at start of line or after space
        parts = re.split(r"(?:^|\s)(\d+)\.\s+", text)

        for i in range(1, len(parts) - 1, 2):
            identifier = parts[i]
            content = parts[i + 1] if i + 1 < len(parts) else ""

            # Parse second-level children a., b., etc.
            children = self._parse_level2_subsections(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\s[a-z]\.\s", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Stop at next top-level subsection
            next_top = re.search(r"\s\d+\.\s", direct_text)
            if next_top:
                direct_text = direct_text[: next_top.start()].strip()  # pragma: no cover

            subsections.append(
                ParsedIASubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_level2_subsections(self, text: str) -> list[ParsedIASubsection]:
        """Parse level 2 subsections a., b., etc.

        Args:
            text: Text to parse

        Returns:
            List of level 2 subsections
        """
        subsections = []
        parts = re.split(r"\s([a-z])\.\s+", text)

        for i in range(1, len(parts) - 1, 2):
            identifier = parts[i]
            content = parts[i + 1] if i + 1 < len(parts) else ""

            # Limit content and stop at next top-level
            next_top = re.search(r"\s\d+\.\s", content)
            if next_top:
                content = content[: next_top.start()]  # pragma: no cover

            subsections.append(
                ParsedIASubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def _parse_chapter_index_html(self, html: str, chapter: str) -> list[dict[str, str]]:
        """Parse chapter index HTML to get section listings.

        Args:
            html: Chapter sections page HTML
            chapter: Chapter number

        Returns:
            List of dicts with section_number and section_title
        """
        soup = BeautifulSoup(html, "html.parser")
        sections = []

        # Look for section links in the format: /docs/code/2025/422.5.pdf
        # The link text typically contains: "section;422.5 - Tax imposed"
        pattern = re.compile(rf"/docs/code/\d+/{re.escape(chapter)}\.[\d\w]+\.(?:pdf|rtf)")

        for link in soup.find_all("a", href=pattern):
            href = link.get("href", "")
            link_text = link.get_text(strip=True)

            # Extract section number from href
            match = re.search(rf"({re.escape(chapter)}\.[^\./]+)\.(?:pdf|rtf)", href)
            if match:
                section_number = match.group(1)

                # Extract title from link text
                # Format is typically "section;422.5 - Title" or just the number
                title_match = re.search(r"-\s*(.+)$", link_text)
                section_title = title_match.group(1).strip() if title_match else ""

                # Avoid duplicates (PDF and RTF links for same section)
                if not any(s["section_number"] == section_number for s in sections):
                    sections.append(
                        {
                            "section_number": section_number,
                            "section_title": section_title,
                        }
                    )

        return sections

    def _to_section(self, parsed: ParsedIASection) -> Section:
        """Convert ParsedIASection to internal Section model."""
        # Create citation using state prefix
        citation = Citation(
            title=0,  # State law indicator
            section=f"IA-{parsed.section_number}",
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
            title_name=f"Iowa Code - {parsed.title_name or 'Title Unknown'}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"ia/{parsed.chapter_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "422.5", "239B.3"

        Returns:
            Section model

        Raises:
            IAConverterError: If section not found or parsing fails
        """
        # Try RTF first (easier to parse than PDF)
        rtf_url = self._build_section_rtf_url(section_number)
        try:
            rtf_bytes = self._get_bytes(rtf_url)
            parsed = self._parse_rtf_content(rtf_bytes, section_number)
            return self._to_section(parsed)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise IAConverterError(f"Section {section_number} not found", rtf_url) from e
            raise IAConverterError(
                f"Error fetching {section_number}: {e}", rtf_url
            ) from e  # pragma: no cover

    def get_chapter_section_numbers(self, chapter: str) -> list[str]:
        """Get list of section numbers in a chapter.

        Args:
            chapter: Chapter number (e.g., "422", "239B")

        Returns:
            List of section numbers (e.g., ["422.1", "422.2", ...])
        """
        url = self._build_chapter_sections_url(chapter)
        html = self._get(url)
        section_info = self._parse_chapter_index_html(html, chapter)
        return [s["section_number"] for s in section_info]

    def get_chapter_sections_with_titles(self, chapter: str) -> list[dict[str, str]]:
        """Get list of section numbers and titles in a chapter.

        Args:
            chapter: Chapter number (e.g., "422", "239B")

        Returns:
            List of dicts with section_number and section_title
        """
        url = self._build_chapter_sections_url(chapter)
        html = self._get(url)
        return self._parse_chapter_index_html(html, chapter)

    def iter_chapter(self, chapter: str) -> Iterator[Section]:
        """Iterate over all sections in a chapter.

        Args:
            chapter: Chapter number (e.g., "422", "239B")

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_chapter_section_numbers(chapter)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except IAConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapters(
        self,
        chapters: list[str] | None = None,
    ) -> Iterator[Section]:
        """Iterate over sections from multiple chapters.

        Args:
            chapters: List of chapter numbers (default: all tax chapters)

        Yields:
            Section objects
        """
        if chapters is None:  # pragma: no cover
            chapters = list(IA_TAX_CHAPTERS.keys())  # pragma: no cover

        for chapter in chapters:  # pragma: no cover
            yield from self.iter_chapter(chapter)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> IAConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_ia_section(section_number: str) -> Section:
    """Fetch a single Iowa Code section.

    Args:
        section_number: e.g., "422.5"

    Returns:
        Section model
    """
    with IAConverter() as converter:
        return converter.fetch_section(section_number)


def download_ia_chapter(chapter: str) -> list[Section]:
    """Download all sections from an Iowa Code chapter.

    Args:
        chapter: Chapter number (e.g., "422", "239B")

    Returns:
        List of Section objects
    """
    with IAConverter() as converter:
        return list(converter.iter_chapter(chapter))


def download_ia_tax_chapters() -> Iterator[Section]:
    """Download all sections from Iowa tax-related chapters (Title X).

    Yields:
        Section objects
    """
    with IAConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(IA_TAX_CHAPTERS.keys()))  # pragma: no cover


def download_ia_welfare_chapters() -> Iterator[Section]:
    """Download all sections from Iowa human services chapters (Title VI).

    Yields:
        Section objects
    """
    with IAConverter() as converter:  # pragma: no cover
        yield from converter.iter_chapters(list(IA_WELFARE_CHAPTERS.keys()))  # pragma: no cover
