"""Parser for CLML (Crown Legislation Markup Language) XML.

CLML is the XML format used by legislation.gov.uk for UK legislation.
Structure:
- <Legislation> root with namespaces
- <ukm:Metadata> contains Dublin Core and legislative metadata
- <Primary><Body> contains the legislation content
- <P1> = primary sections, <P2> = subsections, <P3> = paragraphs

Source: https://legislation.github.io/data-documentation/
"""

import re
from datetime import date
from typing import Optional
from xml.etree import ElementTree as ET

from axiom_corpus.models_uk import (
    UKAct,
    UKAmendment,
    UKCitation,
    UKPart,
    UKSection,
    UKSubsection,
)


# CLML namespaces
NAMESPACES = {
    "leg": "http://www.legislation.gov.uk/namespaces/legislation",
    "ukm": "http://www.legislation.gov.uk/namespaces/metadata",
    "dc": "http://purl.org/dc/elements/1.1/",
    "atom": "http://www.w3.org/2005/Atom",
}


def extract_text(xml_str: str) -> str:
    """Extract plain text from XML string, removing tags.

    Args:
        xml_str: XML string with potential tags

    Returns:
        Plain text with normalized whitespace
    """
    # Remove XML tags
    text = re.sub(r"<[^>]+>", " ", xml_str)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_extent(extent_str: str) -> list[str]:
    """Parse territorial extent string.

    Args:
        extent_str: String like "E+W+S+N.I."

    Returns:
        List of territory codes
    """
    if not extent_str:
        return []
    return [e.strip() for e in extent_str.split("+")]


def extract_citations(xml_str: str) -> list[str]:
    """Extract citation URIs from XML.

    Args:
        xml_str: XML string containing Citation elements

    Returns:
        List of citation URIs
    """
    # Find Citation URI attributes
    pattern = r'URI="([^"]+)"'
    matches = re.findall(pattern, xml_str)

    # Filter to legislation.gov.uk URIs
    citations = []
    for uri in matches:
        if "legislation.gov.uk" in uri:
            # Extract the citation part (e.g., ukpga/2017/32)
            match = re.search(r"legislation\.gov\.uk/([a-z]+/\d+/\d+)", uri)
            if match:
                citations.append(match.group(1))

    return citations


def _get_text_content(elem: ET.Element) -> str:
    """Get all text content from an element, including nested elements."""
    return "".join(elem.itertext())


def _parse_subsections(parent: ET.Element, ns: dict) -> list[UKSubsection]:
    """Parse P2/P3 elements into UKSubsection objects."""
    subsections = []

    # Find P2 elements (subsections like (a), (b))
    for p2 in parent.findall(".//leg:P2", ns):
        p2_id = p2.get("id", "")
        pnum = p2.find("leg:Pnumber", ns)
        pnum_text = pnum.text if pnum is not None else ""

        # Get text from P2para
        p2_text_parts = []
        for text_elem in p2.findall(".//leg:Text", ns):
            p2_text_parts.append(_get_text_content(text_elem))

        # Parse nested P3 elements
        children = []
        for p3 in p2.findall(".//leg:P3", ns):
            p3_pnum = p3.find("leg:Pnumber", ns)
            p3_id = p3_pnum.text if p3_pnum is not None else ""
            p3_text_parts = []
            for text_elem in p3.findall(".//leg:Text", ns):
                p3_text_parts.append(_get_text_content(text_elem))

            if p3_text_parts:
                children.append(UKSubsection(
                    id=p3_id,
                    text=" ".join(p3_text_parts),
                ))

        if p2_text_parts or children:
            subsections.append(UKSubsection(
                id=pnum_text or p2_id.split("-")[-1] if p2_id else "",
                text=" ".join(p2_text_parts),
                children=children,
            ))

    return subsections


def _parse_citation_from_uri(uri: str) -> Optional[UKCitation]:
    """Parse a UKCitation from a DocumentURI."""
    if not uri:
        return None

    # Extract type/year/number/section from URI
    # e.g., http://www.legislation.gov.uk/ukpga/2003/1/section/62
    match = re.search(
        r"legislation\.gov\.uk/([a-z]+)/(\d+)/(\d+)(?:/section/(\d+[A-Za-z]?))?",
        uri,
    )
    if match:
        return UKCitation(
            type=match.group(1),
            year=int(match.group(2)),
            number=int(match.group(3)),
            section=match.group(4),
        )
    return None  # pragma: no cover


def _parse_amendments(root: ET.Element, ns: dict) -> list[UKAmendment]:
    """Parse amendment information from Commentaries and Substitution elements."""
    amendments = []

    # Find Substitution/Addition/Repeal elements
    for sub in root.findall(".//leg:Substitution", ns):
        change_id = sub.get("ChangeId", "")
        commentary_ref = sub.get("CommentaryRef", "")

        # Try to find the referenced commentary
        if commentary_ref:
            commentary = root.find(f".//leg:Commentary[@id='{commentary_ref}']", ns)
            if commentary is not None:
                # Extract amending act from Citation
                citation_elem = commentary.find(".//leg:Citation", ns)
                amending_uri = citation_elem.get("URI", "") if citation_elem is not None else ""

                # Extract date from commentary text
                text = _get_text_content(commentary)
                date_match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
                eff_date = date.today()
                if date_match:
                    try:
                        eff_date = date(
                            int(date_match.group(3)),
                            int(date_match.group(2)),
                            int(date_match.group(1)),
                        )
                    except ValueError:  # pragma: no cover
                        pass

                # Extract citation path from URI
                amending_act = ""
                if amending_uri:
                    match = re.search(r"legislation\.gov\.uk/([a-z]+/\d+/\d+)", amending_uri)
                    if match:
                        amending_act = match.group(1)

                amendments.append(UKAmendment(
                    type="substitution",
                    amending_act=amending_act,
                    description=text[:200] if text else None,
                    effective_date=eff_date,
                    change_id=change_id,
                ))

    return amendments


def parse_section(xml_str: str) -> UKSection:
    """Parse a UK legislation section from CLML XML.

    Args:
        xml_str: XML string containing a section

    Returns:
        UKSection object
    """
    root = ET.fromstring(xml_str)

    # Use namespaces
    ns = NAMESPACES

    # Get DocumentURI - prefer P1 element's URI (has section) over root (Act-level)
    # Real API responses have DocumentURI on P1, but test fixtures may only have it on root
    p1 = root.find(".//leg:P1", ns)
    doc_uri = ""
    if p1 is not None:
        doc_uri = p1.get("DocumentURI", "")
    if not doc_uri:
        doc_uri = root.get("DocumentURI", "")

    # Parse citation from URI
    citation = _parse_citation_from_uri(doc_uri)
    if citation is None:
        # Try to extract from metadata
        year_elem = root.find(".//ukm:Year", ns)
        number_elem = root.find(".//ukm:Number", ns)
        year = int(year_elem.get("Value", "0")) if year_elem is not None else 0
        number = int(number_elem.get("Value", "0")) if number_elem is not None else 0

        # Try to find section number from P1
        section = None
        p1 = root.find(".//leg:P1", ns)
        if p1 is not None:
            pnum = p1.find("leg:Pnumber", ns)
            if pnum is not None:
                section = pnum.text

        citation = UKCitation(type="ukpga", year=year, number=number, section=section)

    # Extract title from dc:title or section heading
    title_elem = root.find(".//dc:title", ns)
    title = title_elem.text if title_elem is not None else ""

    # If this is a section-level doc, use section number as title
    if citation.section:
        title = f"Section {citation.section}"

    # Extract full text
    text_parts = []
    for text_elem in root.findall(".//leg:Text", ns):
        text_parts.append(_get_text_content(text_elem))
    full_text = "\n".join(text_parts)

    # Parse subsections
    body = root.find(".//leg:Body", ns)
    subsections = []
    if body is not None:
        for p1 in body.findall(".//leg:P1", ns):
            subsections.extend(_parse_subsections(p1, ns))

    # Get enactment date
    enacted_elem = root.find(".//ukm:EnactmentDate", ns)
    enacted_date = date.today()
    if enacted_elem is not None:
        date_str = enacted_elem.get("Date", "")
        if date_str:
            try:
                enacted_date = date.fromisoformat(date_str)
            except ValueError:  # pragma: no cover
                pass

    # Parse extent
    extent_str = root.get("RestrictExtent", "")
    extent = parse_extent(extent_str)

    # Parse amendments
    amendments = _parse_amendments(root, ns)

    # Extract cross-references
    references = extract_citations(xml_str)

    return UKSection(
        citation=citation,
        title=title,
        text=full_text,
        subsections=subsections,
        enacted_date=enacted_date,
        extent=extent,
        amendments=amendments,
        references_to=references,
        source_url=doc_uri,
    )


def parse_act_metadata(xml_str: str) -> UKAct:
    """Parse Act-level metadata from CLML XML.

    Args:
        xml_str: XML string containing Act metadata

    Returns:
        UKAct object
    """
    root = ET.fromstring(xml_str)
    ns = NAMESPACES

    # Get DocumentURI
    doc_uri = root.get("DocumentURI", "")

    # Parse citation from URI
    citation = _parse_citation_from_uri(doc_uri)
    if citation is None:
        year_elem = root.find(".//ukm:Year", ns)  # pragma: no cover
        number_elem = root.find(".//ukm:Number", ns)  # pragma: no cover
        year = int(year_elem.get("Value", "0")) if year_elem is not None else 0  # pragma: no cover
        number = int(number_elem.get("Value", "0")) if number_elem is not None else 0  # pragma: no cover
        citation = UKCitation(type="ukpga", year=year, number=number)  # pragma: no cover

    # Extract title
    title_elem = root.find(".//dc:title", ns)
    title = title_elem.text if title_elem is not None else ""

    # Get enactment date
    enacted_elem = root.find(".//ukm:EnactmentDate", ns)
    enacted_date = date.today()
    if enacted_elem is not None:
        date_str = enacted_elem.get("Date", "")
        if date_str:
            try:
                enacted_date = date.fromisoformat(date_str)
            except ValueError:  # pragma: no cover
                pass

    # Get commencement date
    commencement_elem = root.find(".//ukm:ComingIntoForce/ukm:DateTime", ns)
    commencement_date = None
    if commencement_elem is not None:
        date_str = commencement_elem.get("Date", "")
        if date_str:
            try:
                commencement_date = date.fromisoformat(date_str)
            except ValueError:  # pragma: no cover
                pass

    # Get section count from NumberOfProvisions
    section_count = None
    provisions = root.get("NumberOfProvisions")
    if provisions:
        try:
            section_count = int(provisions)
        except ValueError:  # pragma: no cover
            pass

    # Parse parts
    parts = []
    for part_elem in root.findall(".//leg:Part", ns):
        number_elem = part_elem.find("leg:Number", ns)
        title_elem = part_elem.find("leg:Title", ns)
        if number_elem is not None or title_elem is not None:
            parts.append(UKPart(
                number=number_elem.text if number_elem is not None else "",
                title=title_elem.text if title_elem is not None else "",
            ))

    # Parse extent
    extent_str = root.get("RestrictExtent", "")
    extent = parse_extent(extent_str)

    return UKAct(
        citation=citation,
        title=title,
        enacted_date=enacted_date,
        commencement_date=commencement_date,
        parts=parts,
        section_count=section_count,
        extent=extent,
        source_url=doc_uri,
    )
