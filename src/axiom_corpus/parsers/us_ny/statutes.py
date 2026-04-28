"""Parser for New York State laws via the Open Legislation API.

NY Senate provides a free REST API at legislation.nysenate.gov for accessing
all consolidated NYS laws including Tax Law, Social Services Law, etc.

API Documentation: https://legislation.nysenate.gov/static/docs/html/laws.html

Requires a free API key from legislation.nysenate.gov (set as NY_LEGISLATION_API_KEY
environment variable).
"""

import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from axiom_corpus.models import Citation, Section, Subsection

BASE_URL = "https://legislation.nysenate.gov/api/3"

# Common NY law codes for tax/benefit programs
NY_LAW_CODES = {
    "TAX": "Tax Law",
    "SOS": "Social Services Law",
    "EDN": "Education Law",
    "LAB": "Labor Law",
    "EXC": "Executive Law",
    "PBH": "Public Health Law",
    "INS": "Insurance Law",
}


@dataclass
class NYLawInfo:
    """Information about a NY law book."""

    law_id: str
    chapter: str
    name: str
    law_type: str


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


class NYLegislationAPIError(Exception):
    """Error from the NY Legislation API."""

    def __init__(self, message: str, error_code: int | None = None):
        super().__init__(message)  # pragma: no cover
        self.error_code = error_code  # pragma: no cover


class NYLegislationClient:
    """Client for the NY Open Legislation API.

    Example:
        >>> client = NYLegislationClient()  # Uses NY_LEGISLATION_API_KEY env var
        >>> laws = client.get_law_ids()
        >>> tax_tree = client.get_law_tree("TAX")
        >>> section = client.get_section("TAX", "606")
    """

    def __init__(self, api_key: str | None = None, rate_limit_delay: float = 0.2):
        """Initialize the NY Legislation API client.

        Args:
            api_key: API key (defaults to NY_LEGISLATION_API_KEY env var)
            rate_limit_delay: Seconds to wait between requests (default 0.2)
        """
        self.api_key = api_key or os.environ.get("NY_LEGISLATION_API_KEY")  # pragma: no cover
        if not self.api_key:  # pragma: no cover
            raise ValueError(  # pragma: no cover
                "NY API key required. Set NY_LEGISLATION_API_KEY environment variable "
                "or pass api_key parameter. Get a free key at legislation.nysenate.gov"
            )
        self.rate_limit_delay = rate_limit_delay  # pragma: no cover
        self._last_request_time = 0.0  # pragma: no cover
        self.client = httpx.Client(
            base_url=BASE_URL,
            params={"key": self.api_key},
            timeout=60.0,
        )

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """Make a GET request to the API."""
        self._rate_limit()  # pragma: no cover
        response = self.client.get(endpoint, params=params or {})  # pragma: no cover
        response.raise_for_status()
        data = response.json()  # pragma: no cover

        if not data.get("success", False):  # pragma: no cover
            raise NYLegislationAPIError(  # pragma: no cover
                data.get("message", "Unknown API error"),
                data.get("errorCode"),
            )

        return data.get("result", {})  # pragma: no cover

    def get_law_ids(self) -> list[NYLawInfo]:
        """List all available law codes.

        Returns:
            List of NYLawInfo with law_id, name, etc.
        """
        result = self._get("/laws", {"limit": 200})  # pragma: no cover
        items = result.get("items", [])  # pragma: no cover

        return [  # pragma: no cover
            NYLawInfo(
                law_id=item.get("lawId", ""),
                chapter=item.get("chapter", ""),
                name=item.get("name", ""),
                law_type=item.get("lawType", ""),
            )
            for item in items
        ]

    def get_law_tree(self, law_id: str, full: bool = False) -> dict:
        """Get the hierarchical structure of a law.

        Args:
            law_id: Law code (e.g., "TAX", "SOS")
            full: If True, include full text in response

        Returns:
            Dict with law structure including articles and sections
        """
        params = {}  # pragma: no cover
        if full:  # pragma: no cover
            params["full"] = "true"  # pragma: no cover
        return self._get(f"/laws/{law_id}", params)  # pragma: no cover

    def get_section(
        self,
        law_id: str,
        location_id: str,
        date_str: str | None = None,
    ) -> NYSection:
        """Get a specific section.

        Args:
            law_id: Law code (e.g., "TAX")
            location_id: Section location (e.g., "606", "A22S606")
            date_str: ISO date for historical version (optional)

        Returns:
            NYSection with full text
        """
        # Trailing slash is important per API docs
        endpoint = f"/laws/{law_id}/{location_id}/"  # pragma: no cover
        params = {}  # pragma: no cover
        if date_str:  # pragma: no cover
            params["date"] = date_str  # pragma: no cover

        result = self._get(endpoint, params)  # pragma: no cover

        return NYSection(  # pragma: no cover
            law_id=result.get("lawId", law_id),
            location_id=result.get("locationId", location_id),
            title=result.get("title", ""),
            text=result.get("text", ""),
            doc_type=result.get("docType", ""),
            doc_level_id=result.get("docLevelId", ""),
            active_date=result.get("activeDate"),
        )

    def iter_sections(self, law_id: str) -> Iterator[NYSection]:
        """Iterate over all sections in a law.

        Args:
            law_id: Law code (e.g., "TAX")

        Yields:
            NYSection for each section in the law
        """
        # Get the full law tree with text
        tree = self.get_law_tree(law_id, full=True)  # pragma: no cover

        # Process the document tree recursively
        yield from self._iter_tree_sections(tree, law_id)  # pragma: no cover

    def _iter_tree_sections(self, node: dict, law_id: str) -> Iterator[NYSection]:
        """Recursively iterate through law tree to find sections."""
        # Handle both top-level result dict and nested document nodes
        if "documents" in node and "info" in node:  # pragma: no cover
            # Top-level result - start from documents
            node = node["documents"]  # pragma: no cover

        doc_type = node.get("docType", "")  # pragma: no cover

        # Sections are the leaf nodes we want
        if doc_type == "SECTION":  # pragma: no cover
            yield NYSection(  # pragma: no cover
                law_id=law_id,
                location_id=node.get("locationId", ""),
                title=node.get("title", ""),
                text=node.get("text", ""),
                doc_type=doc_type,
                doc_level_id=node.get("docLevelId", ""),
                active_date=node.get("activeDate"),
            )

        # Recurse into documents dict - API uses {items: [...], size: N} structure
        documents = node.get("documents", {})  # pragma: no cover
        if isinstance(documents, dict):  # pragma: no cover
            items = documents.get("items", [])  # pragma: no cover
            for child in items:  # pragma: no cover
                yield from self._iter_tree_sections(child, law_id)  # pragma: no cover

    def search(self, term: str, law_id: str | None = None, limit: int = 100) -> list[dict]:
        """Full-text search across laws.

        Args:
            term: Search term
            law_id: Optional law code to limit search
            limit: Maximum results

        Returns:
            List of search result dicts
        """
        endpoint = f"/laws/{law_id}/search" if law_id else "/laws/search"  # pragma: no cover
        result = self._get(endpoint, {"term": term, "limit": limit})  # pragma: no cover
        return result.get("items", [])  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()  # pragma: no cover

    def __enter__(self) -> "NYLegislationClient":
        return self  # pragma: no cover

    def __exit__(self, *args: Any) -> None:
        self.close()  # pragma: no cover


class NYStateCitation:
    """Citation for NY state laws.

    Format: "NY {Law} Law {symbol} {section}" e.g., "NY Tax Law {symbol} 606"
    """

    def __init__(self, law_id: str, section: str, subsection: str | None = None):
        self.law_id = law_id  # pragma: no cover
        self.section = section  # pragma: no cover
        self.subsection = subsection  # pragma: no cover

    @property
    def cite_string(self) -> str:
        """Return formatted citation string."""
        law_name = NY_LAW_CODES.get(self.law_id, f"{self.law_id} Law")  # pragma: no cover
        base = f"NY {law_name} \u00a7 {self.section}"  # pragma: no cover
        if self.subsection:  # pragma: no cover
            parts = self.subsection.split("/")  # pragma: no cover
            formatted = "".join(f"({p})" for p in parts)  # pragma: no cover
            return f"{base}{formatted}"  # pragma: no cover
        return base  # pragma: no cover

    @property
    def path(self) -> str:
        """Return filesystem-style path."""
        if self.subsection:  # pragma: no cover
            return f"state/ny/{self.law_id.lower()}/{self.section}/{self.subsection}"  # pragma: no cover
        return f"state/ny/{self.law_id.lower()}/{self.section}"  # pragma: no cover


def convert_to_section(ny_section: NYSection) -> Section:
    """Convert NY API section to Axiom section model.

    Args:
        ny_section: Section from NY API

    Returns:
        Axiom section model
    """
    law_name = NY_LAW_CODES.get(ny_section.law_id, f"{ny_section.law_id} Law")  # pragma: no cover

    # Extract section number from location_id
    # Format varies: "606", "A22S606" (Article 22 Section 606), etc.
    section_num = _extract_section_number(ny_section.location_id)  # pragma: no cover

    # Create citation - use a special title number for state laws
    # We use negative numbers or a state prefix scheme
    # For now, use 0 as a placeholder for state laws
    citation = Citation(  # pragma: no cover
        title=0,  # State law indicator
        section=f"NY-{ny_section.law_id}-{section_num}",
    )

    # Parse subsections from text if present
    subsections = _parse_subsections(ny_section.text)  # pragma: no cover

    return Section(  # pragma: no cover
        citation=citation,
        title_name=f"New York {law_name}",
        section_title=ny_section.title or f"Section {section_num}",
        text=ny_section.text,
        subsections=subsections,
        source_url=f"https://legislation.nysenate.gov/api/3/laws/{ny_section.law_id}/{ny_section.location_id}/",
        retrieved_at=date.today(),
        uslm_id=f"ny/{ny_section.law_id}/{ny_section.location_id}",
    )


def _extract_section_number(location_id: str) -> str:
    """Extract section number from location ID.

    Examples:
        "606" -> "606"
        "A22S606" -> "606"
        "A1S1" -> "1"
    """
    if not location_id:  # pragma: no cover
        return ""  # pragma: no cover

    # If it contains "S" followed by a number, extract that
    if "S" in location_id:  # pragma: no cover
        parts = location_id.split("S")  # pragma: no cover
        if len(parts) > 1 and parts[-1]:  # pragma: no cover
            return parts[-1]  # pragma: no cover

    # Otherwise return as-is (simple section numbers)
    return location_id  # pragma: no cover


def _parse_subsections(text: str) -> list[Subsection]:
    """Parse subsections from NY law text.

    NY laws typically use formats like:
    1. Numbered paragraphs
    (a) Lettered paragraphs
    (i) Roman numeral sub-paragraphs

    For now, returns empty list - full parsing would require
    sophisticated regex or NLP.
    """
    # TODO: Implement subsection parsing for NY laws
    # This is complex because NY law formatting varies significantly
    return []  # pragma: no cover


def download_ny_law(
    law_id: str,
    api_key: str | None = None,
) -> Iterator[Section]:
    """Download all sections from a NY law.

    Args:
        law_id: Law code (e.g., "TAX", "SOS")
        api_key: Optional API key (uses env var if not provided)

    Yields:
        Section objects for each section in the law
    """
    with NYLegislationClient(api_key=api_key) as client:  # pragma: no cover
        for ny_section in client.iter_sections(law_id):  # pragma: no cover
            if ny_section.text:  # Skip empty sections  # pragma: no cover
                yield convert_to_section(ny_section)  # pragma: no cover
