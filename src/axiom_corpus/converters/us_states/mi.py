"""Michigan Compiled Laws (MCL) XML converter.

This converter fetches and parses the official Michigan Compiled Laws XML archives
from the Michigan Legislature website.

Source: https://legislature.mi.gov/documents/mcl/
Archive: https://legislature.mi.gov/documents/mcl/archive/

Usage:
    >>> from axiom_corpus.converters.us_states.mi import MichiganConverter
    >>> converter = MichiganConverter()
    >>> chapter = converter.fetch_chapter(206)  # Income Tax Act
    >>> print(chapter.title)
    >>> for section in chapter.sections:
    ...     print(f"{section.mcl_number}: {section.catch_line}")

The Michigan XML format includes:
- MCLChapterInfo: Top-level container for a chapter
- MCLStatuteInfo: The Act information with divisions
- MCLDivisionInfo: Parts and Chapters within the Act
- MCLSectionInfo: Individual statute sections with body text

Body text uses HTML-like tags: <Section-Body>, <Paragraph>, <P>, etc.
"""

import contextlib
import html
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from xml.etree import ElementTree as ET

import httpx

from axiom_corpus.models import Citation, Section, Subsection

# Base URLs for Michigan Legislature
BASE_URL = "https://legislature.mi.gov/documents/mcl"
CHAPTER_URL = f"{BASE_URL}/Chapter%20{{chapter}}.xml"
SECTION_WEB_URL = "https://www.legislature.mi.gov/Laws/MCLSearch?objectName=mcl-{mcl_number}"


@dataclass
class MCLCitation:
    """Citation for Michigan Compiled Laws.

    Format: "MCL {chapter}.{section}" e.g., "MCL 206.30"
    """

    chapter: int
    section: str
    subsection: str | None = None

    @classmethod
    def from_mcl_number(cls, mcl_number: str) -> MCLCitation:
        """Parse MCL number like '206.30' or '206.30a'.

        Args:
            mcl_number: MCL number string (e.g., "206.30", "206.30a")

        Returns:
            MCLCitation instance
        """
        parts = mcl_number.split(".", 1)
        chapter = int(parts[0])
        section = parts[1] if len(parts) > 1 else ""
        return cls(chapter=chapter, section=section)

    @property
    def cite_string(self) -> str:
        """Return formatted MCL citation string."""
        base = f"MCL {self.chapter}.{self.section}"
        if self.subsection:
            return f"{base}({self.subsection})"
        return base

    @property
    def path(self) -> str:
        """Return filesystem-style path for storage."""
        base = f"state/mi/mcl/{self.chapter}/{self.section}"
        if self.subsection:
            # Convert 1/a/i format
            return f"{base}/{self.subsection}"
        return base


@dataclass
class MCLHistory:
    """Legislative history for a section."""

    effective_date: date
    action: str  # "New", "Amendatory", "Repealed"
    public_act_number: int
    public_act_year: int
    is_immediate_effect: bool = False


@dataclass
class MCLSubsection:
    """A subsection within a section."""

    identifier: str  # e.g., "1", "a", "i"
    text: str
    children: list[MCLSubsection] = field(default_factory=list)


@dataclass
class MCLSection:
    """A section from Michigan Compiled Laws."""

    document_id: str
    mcl_number: str  # e.g., "206.30"
    catch_line: str  # Short description / heading
    label: str  # Section number label (e.g., "30")
    body_text: str  # Full text content
    repealed: bool = False
    history: list[MCLHistory] = field(default_factory=list)
    subsections: list[MCLSubsection] = field(default_factory=list)
    editors_notes: str | None = None
    commentary: str | None = None


@dataclass
class MCLChapter:
    """A chapter from Michigan Compiled Laws."""

    document_id: str
    chapter_number: int
    title: str
    repealed: bool = False
    sections: list[MCLSection] = field(default_factory=list)
    act_name: str | None = None
    short_title: str | None = None
    long_title: str | None = None


def parse_body_text(body_text: str) -> tuple[str, list[MCLSubsection]]:
    """Parse Michigan's HTML-in-XML body text format.

    The body text contains escaped HTML with tags like:
    - <Section-Body>, <Section-Number>
    - <Paragraph>, <P>
    - <Emph EmphType="italic">

    Args:
        body_text: Raw body text with escaped HTML

    Returns:
        Tuple of (plain_text, subsections)
    """
    if not body_text:  # pragma: no cover
        return "", []

    # Unescape HTML entities
    text = html.unescape(body_text)

    # Extract plain text by removing XML tags
    plain_text = re.sub(r"<[^>]+>", " ", text)
    plain_text = re.sub(r"\s+", " ", plain_text).strip()

    # Parse subsections
    subsections = _parse_subsections_from_text(plain_text)

    return plain_text, subsections


def _parse_subsections_from_text(text: str) -> list[MCLSubsection]:
    """Parse subsections from plain text using regex patterns.

    Michigan uses:
    - (1), (2), ... for main subsections
    - (a), (b), ... for paragraphs within subsections
    - (i), (ii), ... for sub-paragraphs

    Args:
        text: Plain text content

    Returns:
        List of MCLSubsection objects
    """
    subsections = []

    # Pattern for numbered subsections like (1), (2)
    numbered_pattern = r'\((\d+)\)\s+'

    # Split by numbered subsections
    parts = re.split(f'(?={numbered_pattern})', text)

    for part in parts:
        if not part.strip():
            continue

        # Check if this part starts with a numbered subsection
        match = re.match(numbered_pattern, part)
        if match:
            identifier = match.group(1)
            content = part[match.end():].strip()

            # Parse lettered children (a), (b), etc.
            children = _parse_lettered_subsections(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r'\([a-z]\)', content)
                if first_child_match:
                    direct_text = content[:first_child_match.start()].strip()
                else:
                    direct_text = content  # pragma: no cover
            else:
                direct_text = content

            # Limit text length
            if len(direct_text) > 5000:
                direct_text = direct_text[:5000] + "..."  # pragma: no cover

            subsections.append(MCLSubsection(
                identifier=identifier,
                text=direct_text,
                children=children,
            ))

    return subsections


def _parse_lettered_subsections(text: str) -> list[MCLSubsection]:
    """Parse lettered subsections like (a), (b), (c).

    Args:
        text: Text potentially containing lettered subsections

    Returns:
        List of MCLSubsection for letters found
    """
    subsections = []
    pattern = r'\(([a-z])\)\s+'

    parts = re.split(f'(?={pattern})', text)

    for part in parts:
        if not part.strip():
            continue  # pragma: no cover

        match = re.match(pattern, part)
        if match:
            identifier = match.group(1)
            content = part[match.end():].strip()

            # Look for roman numeral children (i), (ii), etc.
            # For now, don't parse further levels
            if len(content) > 2000:  # pragma: no cover
                content = content[:2000] + "..."

            subsections.append(MCLSubsection(
                identifier=identifier,
                text=content,
                children=[],
            ))

    return subsections


class MichiganConverter:
    """Converter for Michigan Compiled Laws XML files.

    Fetches XML from the Michigan Legislature website and parses it into
    structured Python objects. Supports conversion to Axiom section models.

    Example:
        >>> converter = MichiganConverter()
        >>> chapter = converter.fetch_chapter(206)  # Income Tax Act
        >>> sections = list(converter.to_arch_sections(chapter))
    """

    def __init__(self, timeout: float = 120.0):
        """Initialize the converter.

        Args:
            timeout: HTTP request timeout in seconds
        """
        self.timeout = timeout

    def get_chapter_url(self, chapter: int) -> str:
        """Get the download URL for a chapter.

        Args:
            chapter: Chapter number (e.g., 206)

        Returns:
            Full URL to the chapter XML file
        """
        return CHAPTER_URL.format(chapter=chapter)

    def fetch_chapter(self, chapter: int) -> MCLChapter:
        """Fetch and parse a chapter from legislature.mi.gov.

        Args:
            chapter: Chapter number to fetch

        Returns:
            Parsed MCLChapter object

        Raises:
            httpx.HTTPError: If the request fails
        """
        url = self.get_chapter_url(chapter)

        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(url)
            response.raise_for_status()

            # The XML is UTF-16 encoded
            content = response.content

            # Try to decode as UTF-16, fall back to UTF-8
            try:
                if content.startswith(b'\xff\xfe') or content.startswith(b'\xfe\xff'):
                    xml_str = content.decode('utf-16')
                else:
                    xml_str = content.decode('utf-8')
            except UnicodeDecodeError:
                # Try removing BOM and decoding
                xml_str = content.decode('utf-16-le', errors='replace')

            return self.parse_chapter_xml(xml_str.encode('utf-8'))

    def parse_chapter_xml(self, xml_content: bytes) -> MCLChapter:
        """Parse chapter XML content.

        Args:
            xml_content: Raw XML bytes

        Returns:
            Parsed MCLChapter object
        """
        # Handle UTF-16 encoding in declaration
        xml_str = xml_content.decode('utf-8', errors='replace')
        # Remove UTF-16 declaration if present since we've already converted
        xml_str = re.sub(r'encoding="utf-16"', 'encoding="utf-8"', xml_str)

        root = ET.fromstring(xml_str.encode('utf-8'))

        # Extract chapter info
        document_id = self._get_text(root, "DocumentID")
        name = self._get_text(root, "Name")
        title = self._get_text(root, "Title")
        repealed = self._get_text(root, "Repealed") == "true"

        try:
            chapter_number = int(name) if name else 0
        except ValueError:  # pragma: no cover
            chapter_number = 0  # pragma: no cover

        # Find all sections recursively
        sections = self._extract_sections(root)

        # Extract Act info if present
        act_name = None
        short_title = None
        long_title = None

        statute_info = root.find(".//MCLStatuteInfo")
        if statute_info is not None:
            act_name = self._get_text(statute_info, "Name")
            short_title = self._get_text(statute_info, "ShortTitle")
            long_title = self._get_text(statute_info, "LongTitle")

        return MCLChapter(
            document_id=document_id or "",
            chapter_number=chapter_number,
            title=title or "",
            repealed=repealed,
            sections=sections,
            act_name=act_name,
            short_title=short_title,
            long_title=long_title,
        )

    def _extract_sections(self, root: ET.Element) -> list[MCLSection]:
        """Recursively extract all sections from XML.

        Args:
            root: XML element to search

        Returns:
            List of MCLSection objects
        """
        sections = []

        for section_elem in root.iter("MCLSectionInfo"):
            section = self._parse_section(section_elem)
            if section:
                sections.append(section)

        return sections

    def _parse_section(self, elem: ET.Element) -> MCLSection | None:
        """Parse a single section element.

        Args:
            elem: MCLSectionInfo XML element

        Returns:
            MCLSection or None if parsing fails
        """
        document_id = self._get_text(elem, "DocumentID")
        mcl_number = self._get_text(elem, "MCLNumber")
        catch_line = self._get_text(elem, "CatchLine")
        label = self._get_text(elem, "Label")
        body_text_raw = self._get_text(elem, "BodyText")
        repealed = self._get_text(elem, "Repealed") == "true"
        editors_notes = self._get_text(elem, "EditorsNotes")
        commentary = self._get_text(elem, "Commentary")

        if not mcl_number:
            return None  # pragma: no cover

        # Parse body text
        body_text, subsections = parse_body_text(body_text_raw or "")

        # Parse history
        history = self._parse_history(elem)

        return MCLSection(
            document_id=document_id or "",
            mcl_number=mcl_number,
            catch_line=catch_line or "",
            label=label or "",
            body_text=body_text,
            repealed=repealed,
            history=history,
            subsections=subsections,
            editors_notes=editors_notes if editors_notes else None,
            commentary=commentary if commentary else None,
        )

    def _parse_history(self, elem: ET.Element) -> list[MCLHistory]:
        """Parse legislative history from section.

        Args:
            elem: Section XML element

        Returns:
            List of MCLHistory entries
        """
        history_list = []

        history_elem = elem.find("History")
        if history_elem is None:
            return history_list

        for info in history_elem.findall("HistoryInfo"):
            eff_date_str = self._get_text(info, "EffectiveDate")
            action = self._get_text(info, "Action")

            # Parse effective date
            effective_date = None
            if eff_date_str and not eff_date_str.startswith("0001"):
                with contextlib.suppress(ValueError):
                    effective_date = date.fromisoformat(eff_date_str[:10])

            if not effective_date:  # pragma: no cover
                continue

            # Parse legislation info
            leg_elem = info.find("Legislation")
            if leg_elem is None:
                continue  # pragma: no cover

            pa_number_str = self._get_text(leg_elem, "Number")
            pa_year_str = self._get_text(leg_elem, "Year")
            is_immediate = self._get_text(leg_elem, "IsImmediateEffect") == "true"

            try:
                pa_number = int(pa_number_str) if pa_number_str else 0
                pa_year = int(pa_year_str) if pa_year_str else 0
            except ValueError:  # pragma: no cover
                continue  # pragma: no cover

            history_list.append(MCLHistory(
                effective_date=effective_date,
                action=action or "Unknown",
                public_act_number=pa_number,
                public_act_year=pa_year,
                is_immediate_effect=is_immediate,
            ))

        return history_list

    def _get_text(self, elem: ET.Element, tag: str) -> str | None:
        """Get text content of a child element.

        Args:
            elem: Parent element
            tag: Tag name to find

        Returns:
            Text content or None
        """
        child = elem.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return None

    def list_chapters(self) -> list[int]:
        """List available chapter numbers from the directory.

        Returns:
            List of chapter numbers available for download

        Note:
            This method fetches the directory listing from legislature.mi.gov
            and parses out the chapter numbers.
        """
        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(BASE_URL + "/")
            response.raise_for_status()

            # Parse chapter numbers from directory listing
            chapters = []
            pattern = r'Chapter\s+(\d+)\.xml'
            for match in re.finditer(pattern, response.text):
                chapters.append(int(match.group(1)))

            return sorted(chapters)

    def to_arch_sections(self, chapter: MCLChapter) -> Iterator[Section]:
        """Convert MCL chapter to Axiom section models.

        Args:
            chapter: Parsed MCLChapter

        Yields:
            Section objects compatible with Axiom storage
        """
        for mcl_section in chapter.sections:
            # Create citation
            citation = Citation(
                title=0,  # State law indicator
                section=f"MI-MCL-{mcl_section.mcl_number}",
            )

            # Get dates from history
            enacted_date = None
            last_amended = None
            public_laws = []

            if mcl_section.history:
                # First entry is usually the original enactment
                enacted_date = mcl_section.history[0].effective_date
                # Last entry is the most recent amendment
                last_amended = mcl_section.history[-1].effective_date
                # Collect all public act references
                public_laws = [
                    f"PA {h.public_act_number} of {h.public_act_year}"
                    for h in mcl_section.history
                ]

            # Build source URL
            source_url = SECTION_WEB_URL.format(mcl_number=mcl_section.mcl_number)

            # Convert subsections
            subsections = [
                self._convert_subsection(sub)
                for sub in mcl_section.subsections
            ]

            yield Section(
                citation=citation,
                title_name="Michigan Compiled Laws",
                section_title=mcl_section.catch_line,
                text=mcl_section.body_text,
                subsections=subsections,
                enacted_date=enacted_date,
                last_amended=last_amended,
                public_laws=public_laws,
                source_url=source_url,
                retrieved_at=date.today(),
                uslm_id=f"mi/mcl/{mcl_section.mcl_number}",
            )

    def _convert_subsection(self, sub: MCLSubsection) -> Subsection:
        """Convert MCL subsection to Axiom subsection model.

        Args:
            sub: MCLSubsection object

        Returns:
            Axiom subsection model
        """
        children = [
            self._convert_subsection(child)
            for child in sub.children
        ]

        return Subsection(
            identifier=sub.identifier,
            heading=None,
            text=sub.text,
            children=children,
        )


# Export main classes
__all__ = [
    "MichiganConverter",
    "MCLChapter",
    "MCLSection",
    "MCLHistory",
    "MCLCitation",
    "MCLSubsection",
    "parse_body_text",
]
