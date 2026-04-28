"""Parser for Canadian federal statutes in LIMS XML format.

Canada's Department of Justice publishes consolidated statutes at
laws-lois.justice.gc.ca in a proprietary LIMS XML format.

Structure:
- <Statute> root with lims: namespace for metadata
- <Identification> contains titles and chapter info
- <Body> contains hierarchical sections
- <Section> → <Subsection> → <Paragraph> → <Subparagraph> → <Clause>

Source: https://laws-lois.justice.gc.ca
"""

import logging
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from lxml import etree

from axiom.models_canada import (
    CanadaAct,
    CanadaCitation,
    CanadaSection,
    CanadaSubsection,
)

logger = logging.getLogger(__name__)


# LIMS namespace
LIMS_NS = {"lims": "http://justice.gc.ca/lims"}


class CanadaStatuteParser:
    """Parser for Canadian federal statute XML files."""

    def __init__(self, xml_path: Path | str):
        """Initialize parser with path to statute XML file.

        Args:
            xml_path: Path to the LIMS XML file (e.g., I-3.3.xml)
        """
        self.xml_path = Path(xml_path)
        self._tree: etree._ElementTree | None = None

    @property
    def tree(self) -> etree._ElementTree:
        """Lazily load and return the XML tree."""
        if self._tree is None:
            self._tree = etree.parse(str(self.xml_path))
        return self._tree

    def get_consolidated_number(self) -> str:
        """Extract the consolidated number (e.g., 'I-3.3')."""
        root = self.tree.getroot()
        cons_num = root.find(".//ConsolidatedNumber")
        if cons_num is not None and cons_num.text:
            return cons_num.text.strip()
        # Fallback to filename
        return self.xml_path.stem

    def get_short_title(self) -> str:
        """Extract the short title (e.g., 'Income Tax Act')."""
        root = self.tree.getroot()
        short_title = root.find(".//ShortTitle")
        if short_title is not None and short_title.text:
            return short_title.text.strip()
        return ""

    def get_long_title(self) -> str:
        """Extract the long title."""
        root = self.tree.getroot()
        long_title = root.find(".//LongTitle")
        if long_title is not None and long_title.text:
            return long_title.text.strip()
        return ""

    def get_act_metadata(self) -> CanadaAct:
        """Extract Act-level metadata."""
        root = self.tree.getroot()
        cons_num = self.get_consolidated_number()

        # Parse dates from lims attributes
        in_force_str = root.get("{http://justice.gc.ca/lims}inforce-start-date", "")
        last_amended_str = root.get("{http://justice.gc.ca/lims}lastAmendedDate", "")

        in_force_date = None
        if in_force_str:
            try:
                in_force_date = date.fromisoformat(in_force_str)
            except ValueError:
                pass

        last_amended_date = None
        if last_amended_str:
            try:
                last_amended_date = date.fromisoformat(last_amended_str)
            except ValueError:
                pass

        # Bill info
        bill_origin = root.get("bill-origin")
        bill_type = root.get("bill-type")
        in_force = root.get("in-force", "yes") == "yes"

        # Count sections
        section_count = len(list(root.iter("Section")))

        return CanadaAct(
            citation=CanadaCitation(consolidated_number=cons_num),
            short_title=self.get_short_title(),
            long_title=self.get_long_title(),
            consolidated_number=cons_num,
            bill_origin=bill_origin,
            bill_type=bill_type,
            in_force_date=in_force_date,
            last_amended_date=last_amended_date,
            in_force=in_force,
            section_count=section_count,
            source_url=f"https://laws-lois.justice.gc.ca/eng/acts/{cons_num}/",
            source_path=str(self.xml_path),
        )

    def iter_sections(self) -> Iterator[CanadaSection]:
        """Iterate over all sections in the statute.

        Yields:
            CanadaSection objects for each section
        """
        root = self.tree.getroot()
        cons_num = self.get_consolidated_number()

        for section_elem in root.iter("Section"):
            try:
                section = self._parse_section(section_elem, cons_num)
                if section:
                    yield section
            except Exception as e:  # pragma: no cover
                lims_id = section_elem.get("{http://justice.gc.ca/lims}id", "unknown")  # pragma: no cover
                logger.warning(  # pragma: no cover
                    "[CA-LIMS] Failed to parse section %s: %s",
                    lims_id,
                    e,
                    exc_info=True,
                )

    def get_section(self, section_num: str) -> CanadaSection | None:
        """Get a specific section by number.

        Args:
            section_num: Section number (e.g., "32" or "32.1")

        Returns:
            CanadaSection object or None if not found
        """
        root = self.tree.getroot()
        cons_num = self.get_consolidated_number()

        for section_elem in root.iter("Section"):
            label = section_elem.find("Label")
            if label is not None and label.text and label.text.strip() == section_num:
                return self._parse_section(section_elem, cons_num)

        return None

    def _parse_section(
        self, elem: etree._Element, cons_num: str
    ) -> CanadaSection | None:
        """Parse a Section element into a CanadaSection model."""
        # Get section number
        label = elem.find("Label")
        if label is None or not label.text:
            return None
        section_num = label.text.strip()

        # Get marginal note (section title)
        marginal_note_elem = elem.find("MarginalNote")
        marginal_note = ""
        if marginal_note_elem is not None:
            marginal_note = self._get_text_content(marginal_note_elem)

        # Get full text
        text = self._get_section_text(elem)

        # Parse subsections
        subsections = self._parse_subsections(elem)

        # Get temporal info from lims attributes
        lims_ns = "{http://justice.gc.ca/lims}"
        in_force_str = elem.get(f"{lims_ns}inforce-start-date", "")
        last_amended_str = elem.get(f"{lims_ns}lastAmendedDate", "")
        lims_id = elem.get(f"{lims_ns}id")

        in_force_date = None
        if in_force_str:
            try:
                in_force_date = date.fromisoformat(in_force_str)
            except ValueError:
                pass

        last_amended_date = None
        if last_amended_str:
            try:
                last_amended_date = date.fromisoformat(last_amended_str)
            except ValueError:
                pass

        # Extract historical notes
        historical_notes = []
        for hist_note in elem.findall(".//HistoricalNoteSubItem"):
            note_text = self._get_text_content(hist_note)
            if note_text and not note_text.startswith("[NOTE:"):
                historical_notes.append(note_text)

        # Extract cross-references
        references = self._extract_references(elem)

        return CanadaSection(
            citation=CanadaCitation(
                consolidated_number=cons_num,
                section=section_num,
            ),
            section_number=section_num,
            marginal_note=marginal_note,
            text=text,
            subsections=subsections,
            in_force_date=in_force_date,
            last_amended_date=last_amended_date,
            historical_notes=historical_notes,
            references_to=references,
            source_url=f"https://laws-lois.justice.gc.ca/eng/acts/{cons_num}/section-{section_num}.html",
            source_path=str(self.xml_path),
            lims_id=lims_id,
        )

    def _parse_subsections(self, parent: etree._Element) -> list[CanadaSubsection]:
        """Parse subsection hierarchy."""
        subsections = []

        # Canada uses: Subsection → Paragraph → Subparagraph → Clause
        level_map = {
            "Subsection": "subsection",
            "Paragraph": "paragraph",
            "Subparagraph": "subparagraph",
            "Clause": "clause",
        }

        for tag, level_name in level_map.items():
            for sub_elem in parent.findall(tag):
                # Get label
                label_elem = sub_elem.find("Label")
                label = label_elem.text.strip() if label_elem is not None and label_elem.text else ""

                # Get marginal note if present
                marginal_note_elem = sub_elem.find("MarginalNote")
                marginal_note = None
                if marginal_note_elem is not None:
                    marginal_note = self._get_text_content(marginal_note_elem)

                # Get direct text content
                text = self._get_direct_text(sub_elem)

                # Recursively parse children
                children = self._parse_subsections(sub_elem)

                if label or text:
                    subsections.append(
                        CanadaSubsection(
                            label=label,
                            marginal_note=marginal_note,
                            text=text,
                            children=children,
                            level=level_name,
                        )
                    )

        return subsections

    def _get_text_content(self, elem: etree._Element) -> str:
        """Get all text content from an element."""
        return "".join(elem.itertext()).strip()

    def _get_direct_text(self, elem: etree._Element) -> str:
        """Get text content directly in this element, not in children."""
        parts = []

        # Get text from Text elements that are direct children
        for text_elem in elem.findall("Text"):
            parts.append(self._get_text_content(text_elem))

        # Also check ContinuedSectionSubsection, ContinuedParagraph, etc.
        for tag in ["ContinuedSectionSubsection", "ContinuedParagraph", "ContinuedSubparagraph"]:
            for cont in elem.findall(tag):
                for text_elem in cont.findall("Text"):  # pragma: no cover
                    parts.append(self._get_text_content(text_elem))  # pragma: no cover

        return " ".join(filter(None, parts))

    def _get_section_text(self, elem: etree._Element) -> str:
        """Get the full text of a section including all subsections."""
        return self._get_text_content(elem)

    def _extract_references(self, elem: etree._Element) -> list[str]:
        """Extract cross-references to other acts."""
        references = []

        # Find XRefExternal elements (references to other acts)
        for ref in elem.findall(".//XRefExternal"):
            link = ref.get("link", "")
            if link:
                references.append(link)

        # Find XRefInternal elements (references within same act)
        for ref in elem.findall(".//XRefInternal"):
            text = self._get_text_content(ref)
            if text:
                references.append(f"s. {text}")

        return list(set(references))


def download_act(consolidated_number: str, output_dir: Path, lang: str = "eng") -> Path:
    """Download a Canadian federal act from laws-lois.justice.gc.ca.

    Args:
        consolidated_number: Consolidated number (e.g., 'I-3.3')
        output_dir: Directory to save the XML file
        lang: Language code ('eng' or 'fra')

    Returns:
        Path to the downloaded XML file
    """
    import httpx

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{consolidated_number}.xml"

    # URL format for XML downloads
    url = f"https://laws-lois.justice.gc.ca/{lang}/XML/Acts/{consolidated_number}.xml"

    print(f"Downloading {consolidated_number} from {url}...")

    with httpx.Client(timeout=60.0) as client:
        response = client.get(url)
        response.raise_for_status()
        output_path.write_bytes(response.content)

    print(f"Saved to {output_path}")
    return output_path
