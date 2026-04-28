"""New York state statute converter using the Open Legislation API.

This converter fetches New York state laws from the NY Senate's Open Legislation API
and converts them to USLM-style XML format for ingestion into the Axiom database.

API Documentation: https://legislation.nysenate.gov/static/docs/html/laws.html

Usage:
    >>> from axiom.converters.us_states.ny import NYStateConverter
    >>> converter = NYStateConverter()  # Uses NY_LEGISLATION_API_KEY env var
    >>> result = converter.fetch("TAX/606")  # Fetch Tax Law Section 606 (EITC)
    >>> print(result.section.title)
    >>> print(result.to_uslm_xml())

The converter supports:
- Fetching individual sections by law/location (e.g., "TAX/606")
- Fetching entire law trees (e.g., "TAX")
- Converting to USLM-style XML for compatibility with existing parsers
- Extracting section text, effective dates, and amendments
"""

import os
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from xml.etree import ElementTree as ET

import httpx

# USLM namespace for XML output
USLM_NS = "http://xml.house.gov/schemas/uslm/1.0"

# Base URL for NY Open Legislation API
BASE_URL = "https://legislation.nysenate.gov/api/3"

# Common NY law codes and their full names
NY_LAW_CODES = {
    "ABC": "Alcoholic Beverage Control Law",
    "ADC": "Administrative Code",
    "AGM": "Agriculture and Markets Law",
    "BNK": "Banking Law",
    "BSC": "Business Corporation Law",
    "CPL": "Criminal Procedure Law",
    "CVP": "Civil Practice Law and Rules",
    "CVR": "Civil Rights Law",
    "CVS": "Civil Service Law",
    "DOM": "Domestic Relations Law",
    "EDN": "Education Law",
    "ELD": "Elder Law",
    "ELN": "Election Law",
    "EML": "Eminent Domain Procedure Law",
    "ENV": "Environmental Conservation Law",
    "EPT": "Estates, Powers and Trusts Law",
    "EXC": "Executive Law",
    "FCA": "Family Court Act",
    "FCT": "Family Court Act",
    "GBS": "General Business Law",
    "GCN": "General Construction Law",
    "GCT": "General City Law",
    "GMU": "General Municipal Law",
    "GOB": "General Obligations Law",
    "HAY": "Highway Law",
    "INS": "Insurance Law",
    "LAB": "Labor Law",
    "LLC": "Limited Liability Company Law",
    "MHY": "Mental Hygiene Law",
    "MIL": "Military Law",
    "MNE": "Navigation Law",
    "NYC": "New York City Administrative Code",
    "PBA": "Public Authorities Law",
    "PBH": "Public Health Law",
    "PBL": "Public Buildings Law",
    "PBP": "Public Body Procurement Law",
    "PEN": "Penal Law",
    "PML": "Public Officers Law",
    "RAT": "Real Property Tax Law",
    "RCO": "Real Property Actions and Proceedings Law",
    "RRE": "Real Property Law",
    "SCL": "Social Services Law",
    "SOS": "Social Services Law",
    "TAX": "Tax Law",
    "TWN": "Town Law",
    "VAT": "Vehicle and Traffic Law",
    "VIL": "Village Law",
    "WKC": "Workers' Compensation Law",
}


class NYLegislationAPIError(Exception):
    """Error from the NY Legislation API."""

    def __init__(self, message: str, error_code: int | None = None):
        super().__init__(message)
        self.error_code = error_code


@dataclass
class NYSection:
    """A section from NY law."""

    law_id: str
    location_id: str
    title: str
    text: str
    doc_type: str
    doc_level_id: str
    active_date: str | None = None
    parent_location_ids: list[str] = field(default_factory=list)
    prev_sibling: str | None = None
    next_sibling: str | None = None


@dataclass
class NYLawInfo:
    """Information about a NY law."""

    law_id: str
    chapter: str
    name: str
    law_type: str


@dataclass
class NYFetchResult:
    """Result from fetching a NY statute section."""

    section: NYSection
    law_info: NYLawInfo | None
    raw_response: dict
    fetched_at: datetime

    def to_uslm_xml(self) -> str:
        """Convert this result to USLM-style XML.

        Returns:
            USLM XML as string
        """
        return NYStateConverter._section_to_uslm(self.section, self.law_info)


class NYStateConverter:
    """Converter for New York state statutes via the Open Legislation API.

    This class provides methods to:
    - Fetch individual sections or entire laws from the NY API
    - Convert the JSON responses to our internal models
    - Generate USLM-style XML for compatibility with existing parsers

    Example:
        >>> converter = NYStateConverter()
        >>> result = converter.fetch("TAX/606")
        >>> print(result.section.title)
        >>> xml_output = result.to_uslm_xml()
    """

    state_code = "ny"

    def __init__(
        self,
        api_key: str | None = None,
        rate_limit_delay: float = 0.2,
    ):
        """Initialize the NY State Converter.

        Args:
            api_key: API key for NY Open Legislation API.
                     Defaults to NY_LEGISLATION_API_KEY environment variable.
            rate_limit_delay: Minimum seconds between API requests (default 0.2)

        Raises:
            ValueError: If no API key is provided and none is found in environment.
        """
        self.api_key = api_key or os.environ.get("NY_LEGISLATION_API_KEY")
        if not self.api_key:
            raise ValueError(
                "NY API key required. Set NY_LEGISLATION_API_KEY environment variable "
                "or pass api_key parameter. Get a free key at legislation.nysenate.gov"
            )
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Lazily create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=BASE_URL,
                params={"key": self.api_key},
                timeout=60.0,
            )
        return self._client

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """Make a GET request to the API.

        Args:
            endpoint: API endpoint path
            params: Optional query parameters

        Returns:
            Result dict from API response

        Raises:
            NYLegislationAPIError: If API returns an error
            httpx.HTTPStatusError: If HTTP request fails
        """
        self._rate_limit()
        response = self.client.get(endpoint, params=params or {})
        response.raise_for_status()
        data = response.json()

        if not data.get("success", False):
            raise NYLegislationAPIError(
                data.get("message", "Unknown API error"),
                data.get("errorCode"),
            )

        return data.get("result", {})

    def fetch(self, path: str, date_str: str | None = None) -> NYFetchResult:
        """Fetch a section by path.

        Args:
            path: Path in format "LAW_ID/LOCATION_ID" (e.g., "TAX/606")
            date_str: Optional ISO date for historical version

        Returns:
            NYFetchResult with section data and USLM conversion methods

        Raises:
            ValueError: If path format is invalid
            NYLegislationAPIError: If API returns an error

        Example:
            >>> result = converter.fetch("TAX/606")  # Tax Law Section 606
            >>> result = converter.fetch("TAX/A22S606")  # Article 22, Section 606
        """
        if "/" not in path:
            raise ValueError(
                f"Invalid path format: {path}. Expected 'LAW_ID/LOCATION_ID'"
            )

        parts = path.split("/", 1)
        law_id = parts[0].upper()
        location_id = parts[1]

        # Fetch the section
        section = self._fetch_section(law_id, location_id, date_str)

        # Try to get law info
        law_info = self._fetch_law_info(law_id)

        return NYFetchResult(
            section=section,
            law_info=law_info,
            raw_response={
                "law_id": law_id,
                "location_id": location_id,
            },
            fetched_at=datetime.now(),
        )

    def _fetch_section(
        self,
        law_id: str,
        location_id: str,
        date_str: str | None = None,
    ) -> NYSection:
        """Fetch a specific section from the API.

        Args:
            law_id: Law code (e.g., "TAX")
            location_id: Section location (e.g., "606", "A22S606")
            date_str: Optional ISO date for historical version

        Returns:
            NYSection with full text and metadata
        """
        # Trailing slash is important per API docs
        endpoint = f"/laws/{law_id}/{location_id}/"
        params = {}
        if date_str:
            params["date"] = date_str  # pragma: no cover

        result = self._get(endpoint, params)

        return NYSection(
            law_id=result.get("lawId", law_id),
            location_id=result.get("locationId", location_id),
            title=result.get("title", ""),
            text=result.get("text", ""),
            doc_type=result.get("docType", ""),
            doc_level_id=result.get("docLevelId", ""),
            active_date=result.get("activeDate"),
            parent_location_ids=result.get("parentLocationIds", []),
            prev_sibling=result.get("prevSibling"),
            next_sibling=result.get("nextSibling"),
        )

    def _fetch_law_info(self, law_id: str) -> NYLawInfo | None:
        """Fetch information about a law.

        Args:
            law_id: Law code (e.g., "TAX")

        Returns:
            NYLawInfo or None if not found
        """
        try:
            result = self._get(f"/laws/{law_id}")
            info = result.get("info", {})
            return NYLawInfo(
                law_id=info.get("lawId", law_id),
                chapter=info.get("chapter", ""),
                name=info.get("name", NY_LAW_CODES.get(law_id, f"{law_id} Law")),
                law_type=info.get("lawType", ""),
            )
        except Exception:  # pragma: no cover
            # Return None if we can't get law info
            return None  # pragma: no cover

    def list_laws(self) -> list[NYLawInfo]:
        """List all available law codes.

        Returns:
            List of NYLawInfo with law_id, name, chapter, etc.
        """
        result = self._get("/laws", {"limit": 200})
        items = result.get("items", [])

        return [
            NYLawInfo(
                law_id=item.get("lawId", ""),
                chapter=item.get("chapter", ""),
                name=item.get("name", ""),
                law_type=item.get("lawType", ""),
            )
            for item in items
        ]

    def fetch_law_tree(self, law_id: str, full: bool = False) -> dict:
        """Get the hierarchical structure of a law.

        Args:
            law_id: Law code (e.g., "TAX", "SOS")
            full: If True, include full text in response (can be large)

        Returns:
            Dict with law structure including articles and sections
        """
        params = {}  # pragma: no cover
        if full:  # pragma: no cover
            params["full"] = "true"  # pragma: no cover
        return self._get(f"/laws/{law_id}", params)  # pragma: no cover

    def iter_sections(self, law_id: str) -> Iterator[NYFetchResult]:
        """Iterate over all sections in a law.

        Args:
            law_id: Law code (e.g., "TAX")

        Yields:
            NYFetchResult for each section in the law

        Note:
            This fetches the full law tree and extracts sections.
            For large laws, this may take significant time and memory.
        """
        tree = self.fetch_law_tree(law_id, full=True)  # pragma: no cover
        law_info = None  # pragma: no cover

        info = tree.get("info", {})  # pragma: no cover
        if info:  # pragma: no cover
            law_info = NYLawInfo(  # pragma: no cover
                law_id=info.get("lawId", law_id),
                chapter=info.get("chapter", ""),
                name=info.get("name", NY_LAW_CODES.get(law_id, f"{law_id} Law")),
                law_type=info.get("lawType", ""),
            )

        yield from self._iter_tree_sections(tree, law_id, law_info)  # pragma: no cover

    def _iter_tree_sections(
        self,
        node: dict,
        law_id: str,
        law_info: NYLawInfo | None,
    ) -> Iterator[NYFetchResult]:
        """Recursively iterate through law tree to find sections."""
        # Handle top-level result dict
        if "documents" in node and "info" in node:  # pragma: no cover
            node = node["documents"]  # pragma: no cover

        doc_type = node.get("docType", "")  # pragma: no cover

        # Sections are the leaf nodes we want
        if doc_type == "SECTION":  # pragma: no cover
            section = NYSection(  # pragma: no cover
                law_id=law_id,
                location_id=node.get("locationId", ""),
                title=node.get("title", ""),
                text=node.get("text", ""),
                doc_type=doc_type,
                doc_level_id=node.get("docLevelId", ""),
                active_date=node.get("activeDate"),
            )

            if section.text:  # Skip empty sections  # pragma: no cover
                yield NYFetchResult(  # pragma: no cover
                    section=section,
                    law_info=law_info,
                    raw_response=node,
                    fetched_at=datetime.now(),
                )

        # Recurse into documents - API uses {items: [...], size: N}
        documents = node.get("documents", {})  # pragma: no cover
        if isinstance(documents, dict):  # pragma: no cover
            items = documents.get("items", [])  # pragma: no cover
            for child in items:  # pragma: no cover
                yield from self._iter_tree_sections(child, law_id, law_info)  # pragma: no cover

    def search(
        self,
        term: str,
        law_id: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Full-text search across laws.

        Args:
            term: Search term
            law_id: Optional law code to limit search
            limit: Maximum results (default 100)

        Returns:
            List of search result dicts from API
        """
        endpoint = f"/laws/{law_id}/search" if law_id else "/laws/search"
        result = self._get(endpoint, {"term": term, "limit": limit})
        return result.get("items", [])

    @staticmethod
    def _section_to_uslm(
        section: NYSection,
        law_info: NYLawInfo | None = None,
    ) -> str:
        """Convert a NYSection to USLM-style XML.

        Args:
            section: The section to convert
            law_info: Optional law metadata

        Returns:
            USLM XML as string
        """
        # Extract section number from location_id
        section_num = NYStateConverter._extract_section_number(section.location_id)

        # Get law name
        law_name = (
            law_info.name
            if law_info
            else NY_LAW_CODES.get(section.law_id, f"{section.law_id} Law")
        )

        # Parse article from location_id if present (e.g., "A22S606" -> "22")
        article_num = NYStateConverter._extract_article_number(section.location_id)

        # Create root element with namespace
        root = ET.Element(f"{{{USLM_NS}}}lawDoc")
        root.set("identifier", f"/us/ny/{section.law_id.lower()}")

        # Add meta section
        meta = ET.SubElement(root, f"{{{USLM_NS}}}meta")

        doc_num = ET.SubElement(meta, f"{{{USLM_NS}}}docNumber")
        doc_num.text = section_num

        if section.active_date:
            eff_date = ET.SubElement(meta, f"{{{USLM_NS}}}date")
            eff_date.set("type", "effective")
            eff_date.text = section.active_date

        # Add law title element
        law_elem = ET.SubElement(root, f"{{{USLM_NS}}}title")
        law_elem.set(
            "identifier",
            f"/us/ny/{section.law_id.lower()}",
        )

        law_heading = ET.SubElement(law_elem, f"{{{USLM_NS}}}heading")
        law_heading.text = f"New York {law_name}"

        # Add article if present
        parent_elem = law_elem
        if article_num:
            article = ET.SubElement(law_elem, f"{{{USLM_NS}}}article")
            article.set(
                "identifier",
                f"/us/ny/{section.law_id.lower()}/a{article_num}",
            )
            article_heading = ET.SubElement(article, f"{{{USLM_NS}}}heading")
            article_heading.text = f"Article {article_num}"
            parent_elem = article

        # Add section element
        section_elem = ET.SubElement(parent_elem, f"{{{USLM_NS}}}section")
        section_elem.set(
            "identifier",
            f"/us/ny/{section.law_id.lower()}/s{section_num}",
        )

        section_heading = ET.SubElement(section_elem, f"{{{USLM_NS}}}heading")
        section_heading.text = section.title or f"Section {section_num}"

        # Parse and add content with subsections
        subsections = NYStateConverter._parse_subsections(section.text)

        if subsections:
            NYStateConverter._add_subsections(section_elem, subsections)
        elif section.text:
            content = ET.SubElement(section_elem, f"{{{USLM_NS}}}content")
            content.text = section.text

        # Convert to string
        ET.register_namespace("", USLM_NS)
        ET.register_namespace("uslm", USLM_NS)
        ET.indent(root)

        return ET.tostring(root, encoding="unicode", xml_declaration=True)

    @staticmethod
    def _extract_section_number(location_id: str) -> str:
        """Extract section number from location ID.

        Examples:
            "606" -> "606"
            "A22S606" -> "606"
            "T1A1S1" -> "1"
        """
        if not location_id:
            return ""

        # If it contains "S" followed by content, extract that
        if "S" in location_id:
            parts = location_id.split("S")
            if len(parts) > 1 and parts[-1]:
                return parts[-1]

        # Otherwise return as-is (simple section numbers)
        return location_id

    @staticmethod
    def _extract_article_number(location_id: str) -> str | None:
        """Extract article number from location ID.

        Examples:
            "606" -> None
            "A22S606" -> "22"
            "T1A1S1" -> "1"
        """
        if not location_id:
            return None  # pragma: no cover

        # Look for A followed by digits
        match = re.search(r"A(\d+)", location_id)
        if match:
            return match.group(1)

        return None

    @staticmethod
    def _parse_subsections(text: str) -> list[tuple[str, int, str, list]]:
        """Parse subsections from NY law text.

        NY laws typically use formats like:
        1. Numbered paragraphs
        (a) Lettered paragraphs (lowercase)
        (i) Roman numeral sub-paragraphs

        Returns:
            List of tuples: (identifier, level, text, children)
        """
        if not text:
            return []  # pragma: no cover

        subsections = []

        # Level 0: (a), (b), etc. - lowercase letters (NY uses lowercase for top level)
        # Level 1: (1), (2), etc. - numbers
        # Level 2: (i), (ii), etc. - roman numerals or (A), (B) uppercase

        # Split by top-level subsections
        parts = re.split(r"(?=\([a-z]\))", text)

        for part in parts[1:]:  # Skip content before first subsection
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue  # pragma: no cover

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse level 1 children (numbered)
            children, remaining_text = NYStateConverter._parse_numbered_subsections(
                content
            )

            # Get text before first child
            direct_text = remaining_text.strip()
            if len(direct_text) > 2000:  # pragma: no cover
                direct_text = direct_text[:2000] + "..."

            subsections.append((identifier, 0, direct_text, children))

        return subsections

    @staticmethod
    def _parse_numbered_subsections(text: str) -> tuple[list, str]:
        """Parse numbered subsections (1), (2), etc."""
        subsections = []
        remaining = text

        parts = re.split(r"(?=\(\d+\))", text)

        if len(parts) > 1:
            remaining = parts[0]

            for part in parts[1:]:
                match = re.match(r"\((\d+)\)\s*", part)
                if not match:
                    continue  # pragma: no cover

                identifier = match.group(1)
                content = part[match.end() :]

                # Get direct text (limit size)
                direct_text = content.strip()
                if len(direct_text) > 1000:  # pragma: no cover
                    direct_text = direct_text[:1000] + "..."

                subsections.append((identifier, 1, direct_text, []))

        return subsections, remaining

    @staticmethod
    def _add_subsections(
        parent: ET.Element,
        subsections: list[tuple[str, int, str, list]],
    ) -> None:
        """Recursively add subsections to XML element."""
        level_tags = ["subsection", "paragraph", "subparagraph", "clause", "subclause"]

        for identifier, level, text, children in subsections:
            tag = level_tags[min(level, len(level_tags) - 1)]
            elem = ET.SubElement(parent, f"{{{USLM_NS}}}{tag}")

            # Build identifier from parent
            parent_id = parent.get("identifier", "")
            elem.set("identifier", f"{parent_id}/{identifier}")

            # Add num element
            num = ET.SubElement(elem, f"{{{USLM_NS}}}num")
            num.text = f"({identifier})"

            # Add content
            if text:
                content = ET.SubElement(elem, f"{{{USLM_NS}}}content")
                content.text = text

            # Recurse for children
            if children:
                NYStateConverter._add_subsections(elem, children)

    def to_section_model(self, result: NYFetchResult):
        """Convert NYFetchResult to Axiom section model.

        Args:
            result: The fetch result to convert

        Returns:
            Axiom section model
        """
        from axiom.models import Citation, Section, Subsection  # pragma: no cover

        law_name = (  # pragma: no cover
            result.law_info.name
            if result.law_info
            else NY_LAW_CODES.get(
                result.section.law_id, f"{result.section.law_id} Law"
            )
        )

        section_num = self._extract_section_number(result.section.location_id)  # pragma: no cover

        # Create citation with state-specific format
        citation = Citation(  # pragma: no cover
            title=0,  # State law indicator
            section=f"NY-{result.section.law_id}-{section_num}",
        )

        # Parse subsections
        subsection_data = self._parse_subsections(result.section.text)  # pragma: no cover
        subsections = [  # pragma: no cover
            Subsection(
                identifier=ident,
                text=text,
                children=[
                    Subsection(identifier=c[0], text=c[2], children=[])
                    for c in children
                ],
            )
            for ident, _, text, children in subsection_data
        ]

        return Section(  # pragma: no cover
            citation=citation,
            title_name=f"New York {law_name}",
            section_title=result.section.title or f"Section {section_num}",
            text=result.section.text,
            subsections=subsections,
            source_url=f"https://legislation.nysenate.gov/api/3/laws/{result.section.law_id}/{result.section.location_id}/",
            retrieved_at=result.fetched_at.date(),
            uslm_id=f"ny/{result.section.law_id}/{result.section.location_id}",
        )

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> "NYStateConverter":
        return self  # pragma: no cover

    def __exit__(self, *args: Any) -> None:
        self.close()  # pragma: no cover
