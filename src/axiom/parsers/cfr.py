"""Parser for eCFR (Code of Federal Regulations) XML.

The eCFR XML uses a hierarchical DIV structure:
- DIV1 TYPE="TITLE": Complete CFR title
- DIV3 TYPE="CHAPTER": Chapter groupings
- DIV4 TYPE="SUBCHAP": Subchapter sections
- DIV5 TYPE="PART": Parts (contain authority statements)
- DIV6 TYPE="SUBPART": Subparts
- DIV7 TYPE="SUBJGRP": Subject groups
- DIV8 TYPE="SECTION": Individual regulation sections

Source: https://github.com/usgpo/bulk-data/blob/main/ECFR-XML-User-Guide.md
"""

import logging
import re
from datetime import date
from typing import Iterator, Optional
from xml.etree import ElementTree as ET

from axiom.models_regulation import (
    CFRCitation,
    Regulation,
    RegulationSubsection,
)

logger = logging.getLogger(__name__)


def extract_subsection_id(text: str) -> Optional[str]:
    """Extract subsection ID from paragraph text.

    Args:
        text: Paragraph text like "(a) In general..."

    Returns:
        Subsection ID like "a", "1", "i" or None
    """
    match = re.match(r"^\s*\(([a-zA-Z0-9]+)\)", text)
    if match:
        return match.group(1)
    return None


def extract_heading(text: str) -> Optional[str]:
    """Extract heading from italic text in paragraph.

    Args:
        text: Paragraph text with potential italic heading

    Returns:
        Heading text without trailing period, or None
    """
    # Look for <I>heading.</I> pattern in the text
    match = re.search(r"<I>([^<]+?)\.</I>", text)
    if match:
        return match.group(1).strip()
    return None


def clean_text(text: str) -> str:
    """Clean text by removing XML tags and extra whitespace."""
    # Remove XML tags
    text = re.sub(r"<[^>]+>", "", text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_section(xml_str: str) -> Regulation:
    """Parse a CFR section from XML string.

    Args:
        xml_str: XML string containing a DIV8 section element

    Returns:
        Regulation object
    """
    # Parse XML
    root = ET.fromstring(f"<root>{xml_str}</root>")
    section_elem = root.find(".//DIV8[@TYPE='SECTION']")
    if section_elem is None:
        section_elem = root.find(".//DIV8")  # pragma: no cover
    if section_elem is None:
        raise ValueError("No DIV8 section element found in XML")  # pragma: no cover

    return _parse_section_element(section_elem)


def _parse_section_element(section_elem: ET.Element, authority: str = "") -> Regulation:
    """Parse a DIV8 section element into a Regulation.

    Args:
        section_elem: DIV8 XML element
        authority: Authority statement from parent PART

    Returns:
        Regulation object
    """
    # Extract section number from N attribute (e.g., "§ 1.32-1")
    section_n = section_elem.get("N", "")
    node = section_elem.get("NODE", "")

    # Parse citation from NODE (e.g., "26:1.0.1.1.1.0.1.100")
    title = 26  # Default
    part = 1
    section = ""

    if node:
        parts = node.split(":")
        if len(parts) >= 1:
            try:
                title = int(parts[0])
            except ValueError:  # pragma: no cover
                pass

    # Parse section number from N attribute
    section_match = re.search(r"§?\s*(\d+)\.(\d+(?:-\d+)?)", section_n)
    if section_match:
        part = int(section_match.group(1))
        section = section_match.group(2)

    # Extract heading from HEAD element
    head_elem = section_elem.find("HEAD")
    full_heading = head_elem.text if head_elem is not None and head_elem.text else ""
    # Remove section number prefix from heading
    heading = re.sub(r"^§\s*[\d.-]+\s*", "", full_heading).strip()
    # Remove trailing period from heading
    heading = heading.rstrip(".")

    # Extract all paragraph text
    paragraphs = []
    subsections = []

    for p_elem in section_elem.findall(".//P"):
        # Get full text including nested elements
        p_text = ET.tostring(p_elem, encoding="unicode", method="html")
        p_clean = clean_text(p_text)
        paragraphs.append(p_clean)

        # Check for subsection
        subsec_id = extract_subsection_id(p_clean)
        if subsec_id:
            subsec_heading = extract_heading(p_text)
            subsections.append(RegulationSubsection(
                id=subsec_id,
                heading=subsec_heading,
                text=p_clean,
            ))

    full_text = "\n".join(paragraphs)

    # Extract source citation from CITA element
    cita_elem = section_elem.find(".//CITA")
    source = ""
    if cita_elem is not None:
        source = clean_text(ET.tostring(cita_elem, encoding="unicode", method="html"))
        # Remove the CITA type annotation
        source = re.sub(r"^\[?TYPE=[^]]*\]?\s*", "", source)
        source = source.strip("[] \n")

    # Parse effective date from source if available
    effective_date = date.today()
    date_match = re.search(r"(\w+\.?\s+\d+,\s+\d{4})", source)
    if date_match:
        try:
            from dateutil.parser import parse as parse_date
            effective_date = parse_date(date_match.group(1)).date()
        except (ImportError, ValueError):  # pragma: no cover
            pass

    return Regulation(
        citation=CFRCitation(title=title, part=part, section=section),
        heading=heading,
        authority=authority or "26 U.S.C. 7805",
        source=source,
        full_text=full_text,
        effective_date=effective_date,
        subsections=subsections,
    )


def parse_part(xml_str: str) -> dict:
    """Parse a CFR part from XML string.

    Args:
        xml_str: XML string containing a DIV5 part element

    Returns:
        Dict with 'authority' and 'sections' list
    """
    root = ET.fromstring(f"<root>{xml_str}</root>")
    part_elem = root.find(".//DIV5[@TYPE='PART']")
    if part_elem is None:
        part_elem = root.find(".//DIV5")  # pragma: no cover
    if part_elem is None:
        raise ValueError("No DIV5 part element found in XML")  # pragma: no cover

    # Extract authority
    auth_elem = part_elem.find(".//AUTH")
    authority = ""
    if auth_elem is not None:
        authority = clean_text(ET.tostring(auth_elem, encoding="unicode", method="html"))

    # Parse all sections
    sections = []
    for section_elem in part_elem.findall(".//DIV8[@TYPE='SECTION']"):
        try:
            section = _parse_section_element(section_elem, authority)
            sections.append(section)
        except Exception as e:  # pragma: no cover
            section_id = (
                section_elem.get("N") or section_elem.get("TYPE") or "unknown"
            )
            logger.warning(  # pragma: no cover
                "[CFR] Failed to parse section %s: %s",
                section_id,
                e,
                exc_info=True,
            )
            continue  # pragma: no cover

    return {
        "authority": authority,
        "sections": sections,
    }


class CFRParser:
    """Parser for complete CFR title XML files.

    Handles the full document structure from govinfo.gov bulk downloads.
    """

    def __init__(self, xml_content: str):
        """Initialize parser with XML content.

        Args:
            xml_content: Full XML content of a CFR title
        """
        self.root = ET.fromstring(xml_content)
        self._parse_header()

    def _parse_header(self):
        """Parse title metadata from header."""
        # Extract title number
        idno_elem = self.root.find(".//IDNO[@TYPE='title']")
        if idno_elem is not None and idno_elem.text:
            try:
                self.title_number = int(idno_elem.text.strip())
            except ValueError:  # pragma: no cover
                self.title_number = 0  # pragma: no cover
        else:
            self.title_number = 0  # pragma: no cover

        # Extract title name
        title_elem = self.root.find(".//TITLESTMT/TITLE")
        if title_elem is not None and title_elem.text:
            # Parse "Title 26: Internal Revenue" -> "Internal Revenue"
            title_text = title_elem.text.strip()
            match = re.match(r"Title\s+\d+:\s*(.+)", title_text)
            if match:
                self.title_name = match.group(1)
            else:
                self.title_name = title_text  # pragma: no cover
        else:
            self.title_name = ""  # pragma: no cover

        # Extract amendment date
        amddate_elem = self.root.find(".//AMDDATE")
        self.amendment_date = None
        if amddate_elem is not None and amddate_elem.text:
            try:
                from dateutil.parser import parse as parse_date
                self.amendment_date = parse_date(amddate_elem.text.strip()).date()
            except (ImportError, ValueError):  # pragma: no cover
                pass

    def iter_parts(self) -> Iterator[dict]:
        """Iterate over all parts in the title.

        Yields:
            Dict with part metadata and sections
        """
        for part_elem in self.root.findall(".//DIV5[@TYPE='PART']"):
            # Get part number
            part_n = part_elem.get("N", "")

            # Extract authority
            auth_elem = part_elem.find(".//AUTH")
            authority = ""
            if auth_elem is not None:
                authority = clean_text(ET.tostring(auth_elem, encoding="unicode", method="html"))

            yield {
                "part_number": part_n,
                "authority": authority,
                "element": part_elem,
            }

    def iter_sections(self) -> Iterator[Regulation]:
        """Iterate over all sections in the title.

        Yields:
            Regulation objects for each section
        """
        for part_info in self.iter_parts():
            authority = part_info["authority"]
            part_elem = part_info["element"]

            for section_elem in part_elem.findall(".//DIV8[@TYPE='SECTION']"):
                try:
                    section = _parse_section_element(section_elem, authority)
                    # Override title number from parser
                    section.citation = CFRCitation(
                        title=self.title_number,
                        part=section.citation.part,
                        section=section.citation.section,
                        subsection=section.citation.subsection,
                    )
                    yield section
                except Exception as e:  # pragma: no cover
                    section_id = (
                        section_elem.get("N") or section_elem.get("TYPE") or "unknown"
                    )
                    logger.warning(  # pragma: no cover
                        "[CFR] Failed to parse section %s in title %s: %s",
                        section_id,
                        self.title_number,
                        e,
                        exc_info=True,
                    )
                    continue  # pragma: no cover

    def get_section(self, part: int, section: str) -> Optional[Regulation]:
        """Get a specific section by part and section number.

        Args:
            part: Part number (e.g., 1)
            section: Section number (e.g., "32-1")

        Returns:
            Regulation if found, None otherwise
        """
        for reg in self.iter_sections():  # pragma: no cover
            if reg.citation.part == part and reg.citation.section == section:  # pragma: no cover
                return reg  # pragma: no cover
        return None  # pragma: no cover
