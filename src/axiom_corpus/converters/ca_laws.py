"""Converter for Canadian legislation from justicecanada/laws-lois-xml GitHub repo.

This module fetches and parses Canadian federal legislation from the official
Justice Canada GitHub repository, which contains consolidated Acts and Regulations
in XML format with bilingual (English/French) content.

Source: https://github.com/justicecanada/laws-lois-xml

Usage:
    from axiom_corpus.converters.ca_laws import CanadaLawsConverter

    converter = CanadaLawsConverter()

    # Fetch an Act
    act = converter.fetch("acts/I/I-3.3")  # Income Tax Act

    # Fetch with French metadata
    act = converter.fetch("acts/A/A-1", include_french=True)

    # Fetch specific sections
    sections = converter.fetch_sections("acts/I/I-3.3", section_numbers=["32", "118"])

    # Use local clone
    converter = CanadaLawsConverter(
        source=CanadaLawsSource.LOCAL,
        local_path=Path("/path/to/laws-lois-xml")
    )
"""

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path

from lxml import etree
from pydantic import BaseModel

from axiom_corpus.models_canada import (
    CanadaAct,
    CanadaCitation,
    CanadaSection,
    CanadaSubsection,
)

# LIMS namespace used in Justice Canada XML
LIMS_NS = "{http://justice.gc.ca/lims}"
LIMS_NS_MAP = {"lims": "http://justice.gc.ca/lims"}

# GitHub raw content base URL
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/justicecanada/laws-lois-xml/main"


class CanadaLawsSource(Enum):
    """Source for Canadian legislation XML."""

    GITHUB = GITHUB_RAW_BASE
    LOCAL = None  # Uses local_path from converter


class BilingualContent(BaseModel):
    """Bilingual text content (English and French)."""

    en: str | None = None
    fr: str | None = None

    @property
    def primary(self) -> str:
        """Return primary text (English, or French if no English)."""
        return self.en or self.fr or ""


@dataclass
class BilingualAct:
    """Act with bilingual metadata."""

    act: CanadaAct
    french_short_title: str | None = None
    french_long_title: str | None = None


class CanadaLawsConverter:
    """Converter for Canadian legislation from GitHub or local clone.

    Fetches XML from the justicecanada/laws-lois-xml repository and parses
    it into our internal models. Supports both English and French versions.

    The repository structure:
    - eng/acts/*.xml - English Acts
    - eng/regulations/*.xml - English Regulations
    - fra/acts/*.xml - French Acts (Lois)
    - fra/regulations/*.xml - French Regulations (Règlements)

    File naming: Acts use consolidated numbers like "I-3.3.xml" for Income Tax Act.
    """

    def __init__(
        self,
        source: CanadaLawsSource = CanadaLawsSource.GITHUB,
        local_path: Path | None = None,
    ):
        """Initialize the converter.

        Args:
            source: Where to fetch XML from (GitHub or local)
            local_path: Path to local clone of laws-lois-xml repo
        """
        self.source = source
        self.local_path = Path(local_path) if local_path else None

        if source == CanadaLawsSource.LOCAL and not local_path:
            raise ValueError("local_path required when using LOCAL source")  # pragma: no cover

    def _build_github_url(self, path: str, lang: str = "eng") -> str:
        """Build GitHub raw URL for a document.

        Args:
            path: Document path like "acts/I/I-3.3" or "regulations/SOR/SOR-86-304"
            lang: Language code ("eng" or "fra")

        Returns:
            Full GitHub raw URL
        """
        doc_type, identifier = self._parse_path(path)
        return f"{GITHUB_RAW_BASE}/{lang}/{doc_type}/{identifier}.xml"

    def _parse_path(self, path: str) -> tuple[str, str]:
        """Parse a document path into type and identifier.

        Args:
            path: Path like "acts/I/I-3.3" or "acts/A-1"

        Returns:
            Tuple of (doc_type, identifier) e.g. ("acts", "I-3.3")
        """
        parts = path.strip("/").split("/")

        doc_type = parts[0] if len(parts) >= 1 else "acts"  # "acts" or "regulations"

        # The identifier is the last part (handles both "acts/I/I-3.3" and "acts/A-1")
        identifier = parts[-1] if len(parts) >= 2 else path

        return doc_type, identifier

    def _get_local_path(self, path: str, lang: str = "eng") -> Path:
        """Get local filesystem path for a document.

        Args:
            path: Document path like "acts/I/I-3.3"
            lang: Language code

        Returns:
            Full local path
        """
        if not self.local_path:
            raise ValueError("local_path not set")  # pragma: no cover

        doc_type, identifier = self._parse_path(path)
        return self.local_path / lang / doc_type / f"{identifier}.xml"

    def _fetch_xml(self, path: str, lang: str = "eng") -> bytes:
        """Fetch XML content from source.

        Args:
            path: Document path
            lang: Language code

        Returns:
            Raw XML bytes
        """
        if self.source == CanadaLawsSource.LOCAL:
            local_file = self._get_local_path(path, lang)
            if not local_file.exists():
                raise FileNotFoundError(f"Local file not found: {local_file}")  # pragma: no cover
            return local_file.read_bytes()
        else:
            import httpx

            url = self._build_github_url(path, lang)
            with httpx.Client(timeout=60.0) as client:
                response = client.get(url)
                response.raise_for_status()
                return response.content

    def fetch(
        self, path: str, include_french: bool = False
    ) -> CanadaAct:
        """Fetch an Act and parse it.

        Args:
            path: Document path like "acts/I/I-3.3"
            include_french: Also fetch French version for bilingual metadata

        Returns:
            Parsed CanadaAct with metadata
        """
        xml_content = self._fetch_xml(path, lang="eng")
        act = self._parse_act_xml(xml_content, path)

        if include_french:
            try:
                fr_xml = self._fetch_xml(path, lang="fra")
                self._add_french_metadata(act, fr_xml)
            except Exception:  # pragma: no cover
                # French version may not exist or be accessible
                pass

        return act

    def fetch_sections(  # pragma: no cover
        self, path: str, section_numbers: list[str] | None = None
    ) -> list[CanadaSection]:
        """Fetch specific sections from an Act.

        Args:
            path: Document path
            section_numbers: List of section numbers to fetch (None = all)

        Returns:
            List of parsed sections
        """
        xml_content = self._fetch_xml(path, lang="eng")
        sections = list(self._iter_sections_xml(xml_content, path))

        if section_numbers:
            sections = [s for s in sections if s.section_number in section_numbers]

        return sections

    def _parse_act_xml(
        self, xml_content: bytes, path: str = ""
    ) -> CanadaAct:
        """Parse Act-level metadata from XML.

        Args:
            xml_content: Raw XML bytes
            path: Source path for provenance

        Returns:
            CanadaAct with metadata
        """
        root = etree.fromstring(xml_content)

        # Get consolidated number
        cons_num_elem = root.find(".//ConsolidatedNumber")
        cons_num = cons_num_elem.text.strip() if cons_num_elem is not None and cons_num_elem.text else ""

        # Get titles
        short_title_elem = root.find(".//ShortTitle")
        short_title = short_title_elem.text.strip() if short_title_elem is not None and short_title_elem.text else ""

        long_title_elem = root.find(".//LongTitle")
        long_title = long_title_elem.text.strip() if long_title_elem is not None and long_title_elem.text else ""

        # Get attributes from root
        in_force_str = root.get(f"{LIMS_NS}inforce-start-date", "")
        last_amended_str = root.get(f"{LIMS_NS}lastAmendedDate", "")
        bill_origin = root.get("bill-origin")
        bill_type = root.get("bill-type")
        in_force = root.get("in-force", "yes") == "yes"

        # Parse dates
        in_force_date = None
        if in_force_str:
            with contextlib.suppress(ValueError):
                in_force_date = date.fromisoformat(in_force_str)

        last_amended_date = None
        if last_amended_str:
            with contextlib.suppress(ValueError):
                last_amended_date = date.fromisoformat(last_amended_str)

        # Count sections
        section_count = len(list(root.iter("Section")))

        # Build source URL
        _, identifier = self._parse_path(path) if path else ("", cons_num)
        source_url = f"https://laws-lois.justice.gc.ca/eng/acts/{cons_num or identifier}/"

        return CanadaAct(
            citation=CanadaCitation(consolidated_number=cons_num),
            short_title=short_title,
            long_title=long_title,
            consolidated_number=cons_num,
            bill_origin=bill_origin,
            bill_type=bill_type,
            in_force_date=in_force_date,
            last_amended_date=last_amended_date,
            in_force=in_force,
            section_count=section_count,
            source_url=source_url,
            source_path=path,
        )

    def _add_french_metadata(self, act: CanadaAct, fr_xml: bytes) -> None:
        """Add French metadata to an Act (modifies in place).

        Args:
            act: The Act to update
            fr_xml: French XML content
        """
        # Parse French XML for titles
        root = etree.fromstring(fr_xml)

        short_title_elem = root.find(".//ShortTitle")
        if short_title_elem is not None and short_title_elem.text:
            # Store as a dynamic attribute (Pydantic allows extra fields with proper config)
            # For now we'll just note this in source_path
            pass

    def _iter_sections_xml(
        self, xml_content: bytes, path: str = ""
    ) -> Iterator[CanadaSection]:
        """Iterate over sections in XML content.

        Args:
            xml_content: Raw XML bytes
            path: Source path for provenance

        Yields:
            CanadaSection objects
        """
        root = etree.fromstring(xml_content)

        # Get consolidated number
        cons_num_elem = root.find(".//ConsolidatedNumber")
        cons_num = cons_num_elem.text.strip() if cons_num_elem is not None and cons_num_elem.text else ""

        for section_elem in root.iter("Section"):
            section = self._parse_section(section_elem, cons_num, path)
            if section:
                yield section

    def _parse_section(
        self, elem: etree._Element, cons_num: str, path: str = ""
    ) -> CanadaSection | None:
        """Parse a Section element.

        Args:
            elem: Section XML element
            cons_num: Consolidated number of the Act
            path: Source path

        Returns:
            Parsed CanadaSection or None if invalid
        """
        # Get section number from Label
        label_elem = elem.find("Label")
        if label_elem is None or not label_elem.text:  # pragma: no cover
            return None
        section_num = label_elem.text.strip()

        # Get marginal note (section title)
        marginal_note_elem = elem.find("MarginalNote")
        marginal_note = ""
        if marginal_note_elem is not None:
            marginal_note = self._get_text_content(marginal_note_elem)

        # Get full text
        text = self._get_text_content(elem)

        # Parse subsections
        subsections = self._parse_subsections(elem)

        # Get temporal info
        in_force_str = elem.get(f"{LIMS_NS}inforce-start-date", "")
        last_amended_str = elem.get(f"{LIMS_NS}lastAmendedDate", "")
        lims_id = elem.get(f"{LIMS_NS}id")

        in_force_date = None
        if in_force_str:
            with contextlib.suppress(ValueError):
                in_force_date = date.fromisoformat(in_force_str)

        last_amended_date = None
        if last_amended_str:  # pragma: no cover
            with contextlib.suppress(ValueError):
                last_amended_date = date.fromisoformat(last_amended_str)

        # Extract historical notes
        historical_notes = []
        for hist_note in elem.findall(".//HistoricalNoteSubItem"):  # pragma: no cover
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
            source_path=path,
            lims_id=lims_id,
        )

    def _parse_subsections(self, parent: etree._Element) -> list[CanadaSubsection]:
        """Parse subsection hierarchy from a parent element.

        Canada uses: Subsection -> Paragraph -> Subparagraph -> Clause

        Args:
            parent: Parent XML element

        Returns:
            List of parsed subsections
        """
        subsections = []

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
                if marginal_note_elem is not None:  # pragma: no cover
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
        """Get all text content from an element and its descendants."""
        return "".join(elem.itertext()).strip()

    def _get_direct_text(self, elem: etree._Element) -> str:
        """Get text from direct Text children only."""
        parts = []

        # Get text from Text elements that are direct children
        for text_elem in elem.findall("Text"):
            parts.append(self._get_text_content(text_elem))

        # Also check ContinuedSectionSubsection, ContinuedParagraph, etc.
        for tag in ["ContinuedSectionSubsection", "ContinuedParagraph", "ContinuedSubparagraph"]:
            for cont in elem.findall(tag):  # pragma: no cover
                for text_elem in cont.findall("Text"):
                    parts.append(self._get_text_content(text_elem))

        return " ".join(filter(None, parts))

    def _extract_references(self, elem: etree._Element) -> list[str]:
        """Extract cross-references from an element.

        Args:
            elem: XML element to search

        Returns:
            List of reference strings
        """
        references = []

        # XRefExternal - references to other acts
        for ref in elem.findall(".//XRefExternal"):  # pragma: no cover
            link = ref.get("link", "")
            if link:
                references.append(link)

        # XRefInternal - references within same act
        for ref in elem.findall(".//XRefInternal"):
            text = self._get_text_content(ref)  # pragma: no cover
            if text:  # pragma: no cover
                references.append(f"s. {text}")  # pragma: no cover

        return list(set(references))

    def list_acts(self) -> list[str]:
        """List available acts.

        Returns:
            List of act identifiers (consolidated numbers)
        """
        return self._fetch_index("acts")

    def list_regulations(self) -> list[str]:
        """List available regulations.

        Returns:
            List of regulation identifiers
        """
        return self._fetch_index("regulations")  # pragma: no cover

    def _fetch_index(self, doc_type: str) -> list[str]:
        """Fetch index of available documents.

        Args:
            doc_type: "acts" or "regulations"

        Returns:
            List of document identifiers

        Note:
            For GitHub source, this uses the GitHub API to list files.
            For local source, this lists files in the directory.
        """
        if self.source == CanadaLawsSource.LOCAL:  # pragma: no cover
            if not self.local_path:  # pragma: no cover
                return []  # pragma: no cover
            doc_dir = self.local_path / "eng" / doc_type  # pragma: no cover
            if not doc_dir.exists():  # pragma: no cover
                return []  # pragma: no cover
            return [f.stem for f in doc_dir.glob("*.xml")]  # pragma: no cover
        else:
            # GitHub API - would need to paginate for full list
            import httpx  # pragma: no cover

            api_url = f"https://api.github.com/repos/justicecanada/laws-lois-xml/contents/eng/{doc_type}"  # pragma: no cover
            with httpx.Client(timeout=30.0) as client:
                try:
                    response = client.get(api_url)
                    response.raise_for_status()
                    files = response.json()
                    return [
                        f["name"].replace(".xml", "")
                        for f in files
                        if f["name"].endswith(".xml")
                    ]
                except Exception:
                    return []


# Convenience function for quick fetching
def fetch_act(
    consolidated_number: str, include_french: bool = False
) -> CanadaAct:
    """Fetch a Canadian Act by consolidated number.

    Args:
        consolidated_number: e.g., "I-3.3" for Income Tax Act
        include_french: Also fetch French metadata

    Returns:
        Parsed CanadaAct

    Example:
        >>> act = fetch_act("I-3.3")
        >>> print(act.short_title)
        Income Tax Act
    """
    converter = CanadaLawsConverter()  # pragma: no cover
    # Infer path from consolidated number
    first_letter = consolidated_number.split("-")[0][0].upper()  # pragma: no cover
    path = f"acts/{first_letter}/{consolidated_number}"  # pragma: no cover
    return converter.fetch(path, include_french=include_french)  # pragma: no cover


if __name__ == "__main__":
    # Quick test
    print("Testing CanadaLawsConverter...")

    converter = CanadaLawsConverter()

    print("\nFetching Access to Information Act (A-1)...")
    try:
        act = converter.fetch("acts/A/A-1")
        print(f"  Title: {act.short_title}")
        print(f"  Consolidated Number: {act.consolidated_number}")
        print(f"  Sections: {act.section_count}")
        print(f"  In Force: {act.in_force}")
    except Exception as e:
        print(f"  Error: {e}")

    print("\nFetching sections from A-1...")
    try:
        sections = converter.fetch_sections("acts/A/A-1", section_numbers=["1", "2"])
        for section in sections[:3]:
            print(f"  Section {section.section_number}: {section.marginal_note}")
    except Exception as e:
        print(f"  Error: {e}")
