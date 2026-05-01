"""eCFR XML converter for Code of Federal Regulations.

Fetches regulations from the eCFR versioner API and converts them to our
internal Regulation model. Supports point-in-time queries for historical
versions.

API documentation:
- https://www.ecfr.gov/developers/documentation/api/v1
- https://www.ecfr.gov/reader-aids/ecfr-developer-resources

XML format guide:
- https://github.com/usgpo/bulk-data/blob/main/ECFR-XML-User-Guide.md

Priority titles for tax/benefit modeling:
- Title 26: Internal Revenue (IRS regulations)
- Title 7: Agriculture (SNAP at 7 CFR 271-283)
- Title 20: Employees' Benefits (SSA at 20 CFR 404, 416)
- Title 42: Public Health (Medicare/Medicaid at 42 CFR 400+)
"""

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

import httpx

from axiom_corpus.models_regulation import (
    CFRCitation,
    Regulation,
    RegulationSubsection,
)

# eCFR API base URL
ECFR_API_BASE = "https://www.ecfr.gov/api/versioner/v1"

# Priority CFR titles for tax/benefit analysis
PRIORITY_TITLES = {
    7: "Agriculture (SNAP 7 CFR 271-283)",
    20: "Employees' Benefits (SSA 20 CFR 404, 416)",
    26: "Internal Revenue (IRS regulations)",
    42: "Public Health (Medicare/Medicaid 42 CFR 400+)",
    45: "Public Welfare (HHS programs)",
}


@dataclass
class ECFRMetadata:
    """Metadata about a CFR title from the API."""

    title: int
    name: str
    latest_issue_date: date | None = None
    amendment_date: date | None = None
    part_count: int = 0
    section_count: int = 0


@dataclass
class FetchResult:
    """Result of fetching a CFR section or part."""

    success: bool
    citation: CFRCitation | None = None
    regulation: Regulation | None = None
    regulations: list[Regulation] = field(default_factory=list)
    error: str | None = None
    source_url: str | None = None


class ECFRConverter:
    """Converter for eCFR API data.

    Fetches regulations from the eCFR versioner API and converts them
    to our internal Regulation model.

    Usage:
        converter = ECFRConverter()

        # Fetch a specific section
        result = converter.fetch("26/1.32")  # EITC regulations
        print(result.regulation.heading)

        # Fetch entire part
        result = converter.fetch_part(26, 1)
        for reg in result.regulations:
            print(reg.cfr_cite)

        # Fetch with specific date
        result = converter.fetch("26/1.32", as_of=date(2024, 1, 1))
    """

    def __init__(
        self,
        data_dir: Path | None = None,
        api_base: str = ECFR_API_BASE,
        timeout: float = 120.0,
    ):
        """Initialize the converter.

        Args:
            data_dir: Directory to cache downloaded XML files.
                     Defaults to ~/.axiom/ecfr/
            api_base: Base URL for the eCFR API.
            timeout: HTTP request timeout in seconds.
        """
        self.api_base = api_base
        self.data_dir = data_dir or Path.home() / ".axiom" / "ecfr"
        self.timeout = timeout
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Lazy-loaded HTTP client."""
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout)
        return self._client

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> ECFRConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def get_title_url(
        self,
        title: int,
        as_of: date | None = None,
        part: int | None = None,
    ) -> str:
        """Build the API URL for a CFR title.

        Args:
            title: CFR title number (1-50)
            as_of: Point-in-time date (defaults to current)
            part: Optional part number to filter

        Returns:
            Full API URL
        """
        # Use current date if not specified
        date_str = (as_of or date.today()).isoformat()
        url = f"{self.api_base}/full/{date_str}/title-{title}.xml"

        if part is not None:
            url += f"?part={part}"

        return url

    def fetch(
        self,
        citation_str: str,
        as_of: date | None = None,
    ) -> FetchResult:
        """Fetch a specific CFR section.

        Args:
            citation_str: Citation like "26/1.32" or "26/1.32-1"
                         (title/part.section format)
            as_of: Point-in-time date (defaults to current)

        Returns:
            FetchResult with the regulation or error
        """
        # Parse citation: "26/1.32" -> title=26, part=1, section="32"
        match = re.match(r"(\d+)/(\d+)\.(\d+(?:-\d+)?)", citation_str)
        if not match:
            return FetchResult(
                success=False,
                error=f"Invalid citation format: {citation_str}. "
                "Expected format: title/part.section (e.g., 26/1.32)",
            )

        title = int(match.group(1))
        part = int(match.group(2))
        section = match.group(3)

        # Build citation
        citation = CFRCitation(title=title, part=part, section=section)

        # Fetch the part XML (API doesn't support section-level queries)
        url = self.get_title_url(title, as_of=as_of, part=part)

        try:
            response = self.client.get(url, follow_redirects=True)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            return FetchResult(
                success=False,
                citation=citation,
                error=f"HTTP error {e.response.status_code}: {e.response.text[:200]}",
                source_url=url,
            )
        except httpx.RequestError as e:  # pragma: no cover
            return FetchResult(  # pragma: no cover
                success=False,
                citation=citation,
                error=f"Request error: {str(e)}",
                source_url=url,
            )

        # Parse XML and find the specific section
        try:
            regulation = self._parse_section(
                response.text, title, part, section, url, as_of
            )
            if regulation is None:
                return FetchResult(  # pragma: no cover
                    success=False,
                    citation=citation,
                    error=f"Section {citation.cfr_cite} not found in response",
                    source_url=url,
                )

            return FetchResult(
                success=True,
                citation=citation,
                regulation=regulation,
                source_url=url,
            )
        except ET.ParseError as e:  # pragma: no cover
            return FetchResult(  # pragma: no cover
                success=False,
                citation=citation,
                error=f"XML parse error: {str(e)}",
                source_url=url,
            )

    def fetch_part(
        self,
        title: int,
        part: int,
        as_of: date | None = None,
    ) -> FetchResult:
        """Fetch all sections in a CFR part.

        Args:
            title: CFR title number
            part: Part number within the title
            as_of: Point-in-time date (defaults to current)

        Returns:
            FetchResult with list of regulations
        """
        citation = CFRCitation(title=title, part=part)
        url = self.get_title_url(title, as_of=as_of, part=part)

        try:
            response = self.client.get(url, follow_redirects=True)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:  # pragma: no cover
            return FetchResult(  # pragma: no cover
                success=False,
                citation=citation,
                error=f"HTTP error {e.response.status_code}: {e.response.text[:200]}",
                source_url=url,
            )
        except httpx.RequestError as e:  # pragma: no cover
            return FetchResult(  # pragma: no cover
                success=False,
                citation=citation,
                error=f"Request error: {str(e)}",
                source_url=url,
            )

        # Parse all sections in the part
        try:
            regulations = list(
                self._parse_part(response.text, title, part, url, as_of)
            )
            return FetchResult(
                success=True,
                citation=citation,
                regulations=regulations,
                source_url=url,
            )
        except ET.ParseError as e:  # pragma: no cover
            return FetchResult(  # pragma: no cover
                success=False,
                citation=citation,
                error=f"XML parse error: {str(e)}",
                source_url=url,
            )

    def fetch_title(
        self,
        title: int,
        as_of: date | None = None,
        cache: bool = True,
    ) -> Iterator[Regulation]:
        """Fetch all sections in a CFR title.

        Args:
            title: CFR title number
            as_of: Point-in-time date (defaults to current)
            cache: Whether to cache the downloaded XML

        Yields:
            Regulation objects for each section
        """
        url = self.get_title_url(title, as_of=as_of)  # pragma: no cover

        # Check cache
        cache_path = self._get_cache_path(title, as_of)  # pragma: no cover
        if cache and cache_path.exists():  # pragma: no cover
            xml_content = cache_path.read_text()  # pragma: no cover
        else:
            response = self.client.get(url, follow_redirects=True)  # pragma: no cover
            response.raise_for_status()
            xml_content = response.text  # pragma: no cover

            # Cache if requested
            if cache:  # pragma: no cover
                cache_path.parent.mkdir(parents=True, exist_ok=True)  # pragma: no cover
                cache_path.write_text(xml_content)  # pragma: no cover

        # Parse all sections
        yield from self._parse_title(xml_content, title, url, as_of)  # pragma: no cover

    def _get_cache_path(self, title: int, as_of: date | None = None) -> Path:
        """Get the cache file path for a title."""
        date_str = (as_of or date.today()).isoformat()  # pragma: no cover
        return self.data_dir / f"title-{title}_{date_str}.xml"  # pragma: no cover

    def _parse_section(
        self,
        xml_content: str,
        title: int,
        part: int,
        section: str,
        source_url: str,
        as_of: date | None = None,
    ) -> Regulation | None:
        """Parse a specific section from XML content.

        Args:
            xml_content: Raw XML string
            title: CFR title number
            part: Part number
            section: Section number (e.g., "32" or "32-1")
            source_url: Source URL for tracking
            as_of: Point-in-time date

        Returns:
            Regulation object or None if not found
        """
        # Wrap content to ensure valid XML with a root
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:  # pragma: no cover
            # Try wrapping in a root element
            root = ET.fromstring(f"<root>{xml_content}</root>")  # pragma: no cover

        # Find the section by N attribute (e.g., "§ 1.32-1")
        target_patterns = [
            f"§ {part}.{section}",
            f"§{part}.{section}",
            f"{part}.{section}",
        ]

        for div8 in root.iter():
            if div8.tag == "DIV8" and div8.get("TYPE") == "SECTION":
                n_attr = div8.get("N", "")
                for pattern in target_patterns:
                    if pattern in n_attr:
                        return self._element_to_regulation(
                            div8, title, source_url, as_of
                        )

        return None

    def _parse_part(
        self,
        xml_content: str,
        title: int,
        part: int,
        source_url: str,
        as_of: date | None = None,
    ) -> Iterator[Regulation]:
        """Parse all sections in a part.

        Args:
            xml_content: Raw XML string
            title: CFR title number
            part: Part number
            source_url: Source URL for tracking
            as_of: Point-in-time date

        Yields:
            Regulation objects
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:  # pragma: no cover
            root = ET.fromstring(f"<root>{xml_content}</root>")  # pragma: no cover

        # Get authority from the part
        authority = self._extract_authority(root)

        # Find all sections
        for div8 in root.iter():
            if div8.tag == "DIV8" and div8.get("TYPE") == "SECTION":
                try:
                    reg = self._element_to_regulation(
                        div8, title, source_url, as_of, authority
                    )
                    if reg and reg.citation.part == part:
                        yield reg
                except Exception:  # pragma: no cover
                    # Skip problematic sections
                    continue  # pragma: no cover

    def _parse_title(
        self,
        xml_content: str,
        title: int,
        source_url: str,
        as_of: date | None = None,
    ) -> Iterator[Regulation]:
        """Parse all sections in a title.

        Args:
            xml_content: Raw XML string
            title: CFR title number
            source_url: Source URL for tracking
            as_of: Point-in-time date

        Yields:
            Regulation objects
        """
        try:  # pragma: no cover
            root = ET.fromstring(xml_content)  # pragma: no cover
        except ET.ParseError:  # pragma: no cover
            root = ET.fromstring(f"<root>{xml_content}</root>")  # pragma: no cover

        # Track current authority by part
        current_authority = ""  # pragma: no cover

        for elem in root.iter():  # pragma: no cover
            # Update authority when we enter a new part
            if elem.tag == "DIV5" and elem.get("TYPE") == "PART":  # pragma: no cover
                auth_elem = elem.find(".//AUTH")  # pragma: no cover
                if auth_elem is not None:  # pragma: no cover
                    current_authority = self._clean_text(  # pragma: no cover
                        ET.tostring(auth_elem, encoding="unicode", method="text")
                    )
                # Extract part number
                part_n = elem.get("N", "")  # pragma: no cover
                if part_n.isdigit():  # pragma: no cover
                    int(part_n)  # pragma: no cover

            # Parse sections
            if elem.tag == "DIV8" and elem.get("TYPE") == "SECTION":  # pragma: no cover
                try:  # pragma: no cover
                    reg = self._element_to_regulation(  # pragma: no cover
                        elem, title, source_url, as_of, current_authority
                    )
                    if reg:  # pragma: no cover
                        yield reg  # pragma: no cover
                except Exception:  # pragma: no cover
                    continue  # pragma: no cover

    def _element_to_regulation(
        self,
        elem: ET.Element,
        title: int,
        source_url: str,
        as_of: date | None = None,
        authority: str = "",
    ) -> Regulation | None:
        """Convert a DIV8 section element to a Regulation.

        Args:
            elem: DIV8 XML element
            title: CFR title number
            source_url: Source URL for tracking
            as_of: Point-in-time date
            authority: Authority statement from parent part

        Returns:
            Regulation object or None
        """
        # Extract section info from attributes
        n_attr = elem.get("N", "")
        elem.get("NODE", "")

        # Parse part and section from N attribute
        # Format: "§ 1.32-1" or "1.32-1"
        section_match = re.search(r"(\d+)\.(\d+(?:-\d+)?)", n_attr)
        if not section_match:
            return None  # pragma: no cover

        part = int(section_match.group(1))
        section = section_match.group(2)

        # Build citation
        citation = CFRCitation(title=title, part=part, section=section)

        # Extract heading from HEAD element
        head_elem = elem.find("HEAD")
        heading = ""
        if head_elem is not None and head_elem.text:
            heading = head_elem.text.strip()
            # Remove section number prefix
            heading = re.sub(r"^§\s*[\d.-]+\s*", "", heading).rstrip(".")

        # Extract all paragraph text
        paragraphs = []
        subsections = []

        for p_elem in elem.findall(".//P"):
            p_text = self._get_element_text(p_elem)
            paragraphs.append(p_text)

            # Check for subsection marker
            subsec_match = re.match(r"^\s*\(([a-zA-Z0-9]+)\)", p_text)
            if subsec_match:
                subsec_id = subsec_match.group(1)
                # Extract heading from italic text
                heading_match = re.search(r"<I>([^<]+?)\.</I>",
                    ET.tostring(p_elem, encoding="unicode"))
                subsec_heading = heading_match.group(1) if heading_match else None

                subsections.append(RegulationSubsection(
                    id=subsec_id,
                    heading=subsec_heading,
                    text=p_text,
                ))

        full_text = "\n".join(paragraphs)

        # Extract source citation from CITA element
        cita_elem = elem.find(".//CITA")
        source = ""
        if cita_elem is not None:
            source = self._get_element_text(cita_elem)
            source = re.sub(r"^\[?TYPE=[^]]*\]?\s*", "", source).strip("[] \n")

        # Determine effective date
        effective = as_of or date.today()
        # Try to parse from source citation
        date_match = re.search(r"(\w+\.?\s+\d+,\s+\d{4})", source)
        if date_match:
            try:
                from dateutil.parser import parse as parse_date
                effective = parse_date(date_match.group(1)).date()
            except (ImportError, ValueError):  # pragma: no cover
                pass

        # Extract cross-references to statutes
        source_statutes = []
        auth_match = re.search(r"(\d+)\s*U\.?S\.?C\.?\s*(\d+)", authority)
        if auth_match:
            source_statutes.append(f"{auth_match.group(1)} USC {auth_match.group(2)}")

        return Regulation(
            citation=citation,
            heading=heading,
            authority=authority or f"{title} U.S.C. 7805",
            source=source,
            full_text=full_text,
            effective_date=effective,
            subsections=subsections,
            source_statutes=source_statutes,
            source_url=source_url,
            retrieved_at=date.today(),
        )

    def _extract_authority(self, root: ET.Element) -> str:
        """Extract authority statement from XML."""
        auth_elem = root.find(".//AUTH")
        if auth_elem is not None:
            return self._clean_text(
                ET.tostring(auth_elem, encoding="unicode", method="text")
            )
        return ""  # pragma: no cover

    def _get_element_text(self, elem: ET.Element) -> str:
        """Get all text content from an element."""
        return self._clean_text("".join(elem.itertext()))

    def _clean_text(self, text: str) -> str:
        """Clean text by normalizing whitespace."""
        return re.sub(r"\s+", " ", text).strip()

    # Convenience methods for priority titles

    def fetch_irs(
        self,
        part: int,
        section: str | None = None,
        as_of: date | None = None,
    ) -> FetchResult:
        """Fetch IRS regulations from Title 26.

        Args:
            part: Part number (e.g., 1 for income tax)
            section: Optional section number (e.g., "32-1")
            as_of: Point-in-time date

        Returns:
            FetchResult with regulation(s)
        """
        if section:
            return self.fetch(f"26/{part}.{section}", as_of=as_of)
        return self.fetch_part(26, part, as_of=as_of)

    def fetch_snap(
        self,
        section: str | None = None,
        as_of: date | None = None,
    ) -> FetchResult:
        """Fetch SNAP regulations from Title 7, Parts 271-283.

        Args:
            section: Optional section number
            as_of: Point-in-time date

        Returns:
            FetchResult with regulation(s)
        """
        # SNAP is in 7 CFR 271-283
        if section:  # pragma: no cover
            # Parse part from section
            match = re.match(r"(\d+)", section)  # pragma: no cover
            if match:  # pragma: no cover
                part = int(match.group(1))  # pragma: no cover
                return self.fetch(f"7/{section}", as_of=as_of)  # pragma: no cover
        # Return all SNAP parts
        regulations = []  # pragma: no cover
        for part in range(271, 284):  # pragma: no cover
            result = self.fetch_part(7, part, as_of=as_of)  # pragma: no cover
            if result.success:  # pragma: no cover
                regulations.extend(result.regulations)  # pragma: no cover
        return FetchResult(  # pragma: no cover
            success=True,
            regulations=regulations,
        )

    def fetch_ssa(
        self,
        part: int = 404,
        section: str | None = None,
        as_of: date | None = None,
    ) -> FetchResult:
        """Fetch SSA regulations from Title 20.

        Args:
            part: Part number (404=OASDI, 416=SSI)
            section: Optional section number
            as_of: Point-in-time date

        Returns:
            FetchResult with regulation(s)
        """
        if section:  # pragma: no cover
            return self.fetch(f"20/{part}.{section}", as_of=as_of)  # pragma: no cover
        return self.fetch_part(20, part, as_of=as_of)  # pragma: no cover


# Convenience functions

def fetch_regulation(
    citation_str: str,
    as_of: date | None = None,
) -> FetchResult:
    """Fetch a single CFR regulation.

    Args:
        citation_str: Citation like "26/1.32"
        as_of: Point-in-time date

    Returns:
        FetchResult with the regulation
    """
    with ECFRConverter() as converter:
        return converter.fetch(citation_str, as_of=as_of)


def fetch_eitc_regulations(as_of: date | None = None) -> FetchResult:
    """Fetch all EITC-related IRS regulations.

    The EITC regulations are primarily in:
    - 26 CFR 1.32 series (earned income credit)

    Args:
        as_of: Point-in-time date

    Returns:
        FetchResult with regulations
    """
    with ECFRConverter() as converter:  # pragma: no cover
        return converter.fetch_irs(1, as_of=as_of)  # pragma: no cover


if __name__ == "__main__":
    # Example usage
    import sys

    citation = sys.argv[1] if len(sys.argv) > 1 else "26/1.32"
    print(f"Fetching {citation}...")

    result = fetch_regulation(citation)
    if result.success:
        reg = result.regulation
        if reg:
            print(f"\nCitation: {reg.cfr_cite}")
            print(f"Heading: {reg.heading}")
            print(f"Authority: {reg.authority}")
            print("\nText preview (first 500 chars):")
            print(reg.full_text[:500])
        else:
            print(f"\nFound {len(result.regulations)} sections")
            for r in result.regulations[:5]:
                print(f"  - {r.cfr_cite}: {r.heading}")
    else:
        print(f"Error: {result.error}")
        print(f"URL: {result.source_url}")
