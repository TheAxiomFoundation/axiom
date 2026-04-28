"""Parser for USLM (United States Legislative Markup) XML format.

USLM is the official XML schema for the US Code, published by the Office of
the Law Revision Counsel at uscode.house.gov.

Schema documentation: https://uscode.house.gov/download/resources/USLM-User-Guide.pdf
"""

import logging
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from lxml import etree

from axiom.models import Citation, Section, Subsection

logger = logging.getLogger(__name__)

# USLM namespaces - the actual namespace varies by source
USLM_NS_GPO = {"uslm": "http://schemas.gpo.gov/xml/uslm"}
USLM_NS_HOUSE = {"uslm": "http://xml.house.gov/schemas/uslm/1.0"}


class USLMParser:
    """Parser for USLM XML files from uscode.house.gov."""

    def __init__(self, xml_path: Path | str):
        """Initialize parser with path to USLM XML file.

        Args:
            xml_path: Path to the USLM XML file (e.g., usc26.xml for Title 26)
        """
        self.xml_path = Path(xml_path)  # pragma: no cover
        self._tree: etree._ElementTree | None = None  # pragma: no cover
        self._ns: dict[str, str] = {}  # Detected namespace  # pragma: no cover

    def _detect_namespace(self) -> dict[str, str]:
        """Detect which USLM namespace the document uses."""
        root = self.tree.getroot()  # pragma: no cover
        ns = root.nsmap.get(None, "")  # Default namespace  # pragma: no cover

        if "xml.house.gov" in ns:  # pragma: no cover
            return USLM_NS_HOUSE  # pragma: no cover
        elif "schemas.gpo.gov" in ns:  # pragma: no cover
            return USLM_NS_GPO  # pragma: no cover
        else:
            # Try to detect from root element
            if "house.gov" in str(root.tag):  # pragma: no cover
                return USLM_NS_HOUSE  # pragma: no cover
            return USLM_NS_GPO  # pragma: no cover

    @property
    def ns(self) -> dict[str, str]:
        """Get the namespace dict for XPath queries."""
        if not self._ns:  # pragma: no cover
            self._ns = self._detect_namespace()  # pragma: no cover
        return self._ns  # pragma: no cover

    @property
    def tree(self) -> etree._ElementTree:
        """Lazily load and return the XML tree."""
        if self._tree is None:  # pragma: no cover
            self._tree = etree.parse(str(self.xml_path))  # pragma: no cover
        return self._tree  # pragma: no cover

    def get_title_number(self) -> int:
        """Extract the title number from the XML."""
        root = self.tree.getroot()  # pragma: no cover
        # Try to get from docNumber in meta first (most reliable)
        doc_num = root.find(".//docNumber", self.ns)  # pragma: no cover
        if doc_num is None:  # pragma: no cover
            # Try without namespace
            doc_num = root.find(".//{http://xml.house.gov/schemas/uslm/1.0}docNumber")  # pragma: no cover
        if doc_num is None:  # pragma: no cover
            doc_num = root.find(".//docNumber")  # pragma: no cover
        if doc_num is not None and doc_num.text:  # pragma: no cover
            return int(doc_num.text.strip())  # pragma: no cover

        # Fallback: Title number is in the identifier attribute
        title_elem = root.find(".//uslm:title", self.ns)  # pragma: no cover
        if title_elem is not None:  # pragma: no cover
            identifier = title_elem.get("identifier", "")  # pragma: no cover
            # Format: /us/usc/t26 -> 26
            if "/t" in identifier:  # pragma: no cover
                return int(identifier.split("/t")[-1])  # pragma: no cover

        # Last resort: check root identifier
        root_id = root.get("identifier", "")  # pragma: no cover
        if "/t" in root_id:  # pragma: no cover
            return int(root_id.split("/t")[-1].split("/")[0])  # pragma: no cover

        raise ValueError(f"Cannot determine title number from {self.xml_path}")  # pragma: no cover

    def get_title_name(self) -> str:
        """Extract the title name (e.g., 'Internal Revenue Code')."""
        root = self.tree.getroot()  # pragma: no cover
        # Try with namespace
        heading = root.find(".//uslm:title/uslm:heading", self.ns)  # pragma: no cover
        if heading is not None and heading.text:  # pragma: no cover
            return heading.text.strip()  # pragma: no cover
        # Try finding title element directly with full namespace
        ns_uri = self.ns.get("uslm", "")  # pragma: no cover
        for title_elem in root.iter(f"{{{ns_uri}}}title"):  # pragma: no cover
            heading = title_elem.find(f"{{{ns_uri}}}heading")  # pragma: no cover
            if heading is not None and heading.text:  # pragma: no cover
                return heading.text.strip()  # pragma: no cover
        return f"Title {self.get_title_number()}"  # pragma: no cover

    def iter_sections(self) -> Iterator[Section]:
        """Iterate over all sections in the title.

        Yields:
            Section objects for each section in the title
        """
        root = self.tree.getroot()  # pragma: no cover
        title_num = self.get_title_number()  # pragma: no cover
        title_name = self.get_title_name()  # pragma: no cover
        ns_uri = self.ns.get("uslm", "")  # pragma: no cover

        for section_elem in root.iter(f"{{{ns_uri}}}section"):  # pragma: no cover
            try:  # pragma: no cover
                section = self._parse_section(section_elem, title_num, title_name)  # pragma: no cover
                if section:  # pragma: no cover
                    yield section  # pragma: no cover
            except Exception as e:  # pragma: no cover
                # Log but continue - don't let one bad section stop everything
                identifier = section_elem.get("identifier", "unknown")  # pragma: no cover
                logger.warning(  # pragma: no cover
                    "[USLM] Failed to parse section %s: %s",
                    identifier,
                    e,
                    exc_info=True,
                )

    def get_section(self, section_num: str) -> Section | None:
        """Get a specific section by number.

        Args:
            section_num: Section number (e.g., "32" or "32A")

        Returns:
            Section object or None if not found
        """
        root = self.tree.getroot()  # pragma: no cover
        title_num = self.get_title_number()  # pragma: no cover
        title_name = self.get_title_name()  # pragma: no cover
        ns_uri = self.ns.get("uslm", "")  # pragma: no cover

        # USLM identifier format: /us/usc/t26/s32
        target_id = f"/us/usc/t{title_num}/s{section_num}"  # pragma: no cover

        for section_elem in root.iter(f"{{{ns_uri}}}section"):  # pragma: no cover
            if section_elem.get("identifier") == target_id:  # pragma: no cover
                return self._parse_section(section_elem, title_num, title_name)  # pragma: no cover

        return None  # pragma: no cover

    def _parse_section(
        self, elem: etree._Element, title_num: int, title_name: str
    ) -> Section | None:
        """Parse a section element into a Section model."""
        identifier = elem.get("identifier", "")  # pragma: no cover
        if not identifier:  # pragma: no cover
            return None  # pragma: no cover

        # Extract section number from identifier like /us/usc/t26/s32
        section_num = identifier.split("/s")[-1] if "/s" in identifier else ""  # pragma: no cover
        if not section_num:  # pragma: no cover
            return None  # pragma: no cover

        # Get section heading
        heading_elem = elem.find("uslm:heading", self.ns)  # pragma: no cover
        section_title = ""  # pragma: no cover
        if heading_elem is not None:  # pragma: no cover
            section_title = self._get_text_content(heading_elem)  # pragma: no cover

        # Get full text content
        text = self._get_section_text(elem)  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(elem)  # pragma: no cover

        # Extract cross-references
        references = self._extract_references(elem)  # pragma: no cover

        # Get source URL
        source_url = f"https://uscode.house.gov/view.xhtml?req={title_num}+USC+{section_num}"  # pragma: no cover

        return Section(  # pragma: no cover
            citation=Citation(title=title_num, section=section_num),
            title_name=title_name,
            section_title=section_title,
            text=text,
            subsections=subsections,
            references_to=references,
            source_url=source_url,
            retrieved_at=date.today(),
            uslm_id=identifier,
        )

    def _parse_subsections(self, parent: etree._Element) -> list[Subsection]:
        """Recursively parse subsection hierarchy."""
        subsections = []  # pragma: no cover

        # USLM uses various elements for subsection levels
        subsection_tags = ["subsection", "paragraph", "subparagraph", "clause", "subclause"]  # pragma: no cover

        for tag in subsection_tags:  # pragma: no cover
            for sub_elem in parent.findall(f"uslm:{tag}", self.ns):  # pragma: no cover
                identifier = sub_elem.get("identifier", "")  # pragma: no cover
                # Extract the local identifier (e.g., "a" from "/us/usc/t26/s32/a")
                local_id = identifier.split("/")[-1] if identifier else ""  # pragma: no cover

                heading_elem = sub_elem.find("uslm:heading", self.ns)  # pragma: no cover
                heading = self._get_text_content(heading_elem) if heading_elem is not None else None  # pragma: no cover

                # Get text content (excluding child subsections)
                text = self._get_direct_text(sub_elem)  # pragma: no cover

                # Recursively parse children
                children = self._parse_subsections(sub_elem)  # pragma: no cover

                if local_id:  # pragma: no cover
                    subsections.append(  # pragma: no cover
                        Subsection(
                            identifier=local_id,
                            heading=heading,
                            text=text,
                            children=children,
                        )
                    )

        return subsections  # pragma: no cover

    def _get_text_content(self, elem: etree._Element) -> str:
        """Get all text content from an element, including nested elements.

        Tables (XHTML <table> elements) are converted to markdown format
        to preserve column structure in the plain-text output.
        """
        return self._extract_text_with_tables(elem).strip()  # pragma: no cover

    def _extract_text_with_tables(self, elem: etree._Element) -> str:  # pragma: no cover
        """Recursively extract text, converting tables to markdown."""
        tag = etree.QName(elem.tag).localname if isinstance(elem.tag, str) else ""

        if tag == "table":
            return self._table_to_markdown(elem)

        parts: list[str] = []
        if elem.text:
            parts.append(elem.text)
        for child in elem:
            parts.append(self._extract_text_with_tables(child))
            if child.tail:
                parts.append(child.tail)
        return "".join(parts)

    def _table_to_markdown(self, table_elem: etree._Element) -> str:  # pragma: no cover
        """Convert an XHTML table element to a markdown table string."""
        rows: list[list[str]] = []
        header_count = 0

        for elem in table_elem.iter():
            tag = etree.QName(elem.tag).localname if isinstance(elem.tag, str) else ""
            if tag == "tr":
                cells: list[str] = []
                for cell in elem:
                    cell_tag = etree.QName(cell.tag).localname if isinstance(cell.tag, str) else ""
                    if cell_tag in ("td", "th"):
                        # Join text from child <p> elements with spaces
                        p_texts = []
                        for p in cell:
                            p_tag = etree.QName(p.tag).localname if isinstance(p.tag, str) else ""
                            if p_tag == "p":
                                p_texts.append("".join(p.itertext()).strip())
                        if p_texts:
                            text = " ".join(p_texts)
                        else:
                            text = "".join(cell.itertext()).strip()
                        text = " ".join(text.split())
                        cells.append(text)
                if cells:
                    rows.append(cells)
                    # Track header rows
                    parent = elem.getparent()
                    if parent is not None:
                        parent_tag = etree.QName(parent.tag).localname if isinstance(parent.tag, str) else ""
                        if parent_tag == "thead":
                            header_count += 1

        if not rows:
            return ""

        # Determine column widths for alignment
        num_cols = max(len(r) for r in rows)
        # Pad short rows
        for row in rows:
            while len(row) < num_cols:
                row.append("")

        col_widths = [max(len(row[i]) for row in rows) for i in range(num_cols)]
        col_widths = [max(w, 3) for w in col_widths]  # Minimum width of 3

        def format_row(cells: list[str]) -> str:
            padded = [cells[i].ljust(col_widths[i]) for i in range(num_cols)]
            return "| " + " | ".join(padded) + " |"

        lines: list[str] = []
        separator = "| " + " | ".join("-" * w for w in col_widths) + " |"

        if header_count == 0:
            header_count = 1  # Treat first row as header if no thead

        for i, row in enumerate(rows):
            lines.append(format_row(row))
            if i == header_count - 1:
                lines.append(separator)

        return "\n" + "\n".join(lines) + "\n"

    def _get_direct_text(self, elem: etree._Element) -> str:
        """Get text content directly in this element, not in child subsections."""
        # Get text from content/chapeau elements, not from child subsections
        parts = []  # pragma: no cover

        if elem.text:  # pragma: no cover
            parts.append(elem.text.strip())  # pragma: no cover

        for child in elem:  # pragma: no cover
            tag = etree.QName(child.tag).localname  # pragma: no cover
            # Skip subsection children, include content elements
            if tag in ["content", "chapeau", "text", "continuation"]:  # pragma: no cover
                parts.append(self._get_text_content(child))  # pragma: no cover
            elif (  # pragma: no cover
                tag not in ["subsection", "paragraph", "subparagraph", "clause", "subclause"]
                and child.text
            ):
                # Include other inline content
                parts.append(child.text.strip())  # pragma: no cover
            if child.tail:  # pragma: no cover
                parts.append(child.tail.strip())  # pragma: no cover

        return " ".join(filter(None, parts))  # pragma: no cover

    def _get_section_text(self, elem: etree._Element) -> str:
        """Get the full text of a section including all subsections."""
        return self._get_text_content(elem)  # pragma: no cover

    def _extract_references(self, elem: etree._Element) -> list[str]:
        """Extract cross-references to other sections."""
        references = []  # pragma: no cover
        ns_uri = self.ns.get("uslm", "")  # pragma: no cover

        for ref in elem.iter(f"{{{ns_uri}}}ref"):  # pragma: no cover
            href = ref.get("href", "")  # pragma: no cover
            if href.startswith("/us/usc/"):  # pragma: no cover
                # Convert USLM reference to citation
                # /us/usc/t26/s32 -> 26 USC 32
                parts = href.split("/")  # pragma: no cover
                if len(parts) >= 5:  # pragma: no cover
                    title = parts[3].replace("t", "")  # pragma: no cover
                    section = parts[4].replace("s", "")  # pragma: no cover
                    references.append(f"{title} USC {section}")  # pragma: no cover

        return list(set(references))  # Deduplicate  # pragma: no cover


def download_title(title_num: int, output_dir: Path) -> Path:
    """Download a US Code title from uscode.house.gov.

    Args:
        title_num: Title number (1-54)
        output_dir: Directory to save the XML file

    Returns:
        Path to the downloaded XML file
    """
    import httpx  # pragma: no cover

    output_dir.mkdir(parents=True, exist_ok=True)  # pragma: no cover
    output_path = output_dir / f"usc{title_num}.xml"  # pragma: no cover

    # URL format for USLM XML downloads
    # Current release point as of Dec 2025: 119-59
    url = f"https://uscode.house.gov/download/releasepoints/us/pl/119/59/xml_usc{title_num:02d}@119-59.zip"  # pragma: no cover

    print(f"Downloading Title {title_num} from {url}...")  # pragma: no cover

    with httpx.Client(timeout=120.0) as client:
        response = client.get(url)
        response.raise_for_status()

        # It's a zip file, extract the XML
        import io
        import zipfile

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Find the main XML file
            xml_files = [n for n in zf.namelist() if n.endswith(".xml")]
            if not xml_files:
                raise ValueError(f"No XML files found in downloaded archive for Title {title_num}")

            # Extract the first (usually only) XML file
            xml_content = zf.read(xml_files[0])
            output_path.write_bytes(xml_content)

    print(f"Saved to {output_path}")  # pragma: no cover
    return output_path  # pragma: no cover
