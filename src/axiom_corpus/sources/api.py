"""API source adapter for jurisdictions with JSON APIs.

Some jurisdictions provide public APIs:
- New York: legislation.nysenate.gov/api/3
- LegiScan: legiscan.com (all 50 states, requires API key)
"""

import re
from collections.abc import Iterator

import httpx

from axiom_corpus.models_statute import Statute, StatuteSubsection
from axiom_corpus.sources.base import SourceConfig, StatuteSource


class APISource(StatuteSource):
    """Source adapter for JSON API-based statute sources.

    Handles common API patterns:
    - Authentication (API keys, OAuth)
    - Pagination
    - Rate limiting
    - Response parsing
    """

    def __init__(self, config: SourceConfig):
        super().__init__(config)

    def _api_get(self, endpoint: str, params: dict | None = None) -> dict:
        """Make authenticated API request."""
        url = f"{self.config.base_url}{endpoint}"

        headers = {}
        if self.config.api_key:
            # Different APIs use different auth methods
            headers["X-API-Key"] = self.config.api_key

        response = self._get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_section(self, code: str, section: str, **kwargs) -> Statute | None:
        """Fetch section via API - must be implemented by subclass."""
        raise NotImplementedError("Subclass must implement get_section")

    def list_sections(self, code: str, **kwargs) -> Iterator[str]:
        """List sections via API - must be implemented by subclass."""
        raise NotImplementedError("Subclass must implement list_sections")


class NYLegislationSource(APISource):
    """Source adapter for NY Open Legislation API.

    API: legislation.nysenate.gov/api/3
    Docs: https://legislation.nysenate.gov/static/docs/html/
    """

    # NY law codes
    NY_CODES: dict[str, str] = {
        "TAX": "Tax Law",
        "LAB": "Labor Law",
        "ELN": "Election Law",
        "SSL": "Social Services Law",
        "PEN": "Penal Law",
        "CVP": "Civil Practice Law and Rules",
        "CVR": "Civil Rights Law",
        "INS": "Insurance Law",
        "EDN": "Education Law",
        "FIN": "Financial Services Law",
        "PBH": "Public Health Law",
        "ECL": "Environmental Conservation Law",
        "VTL": "Vehicle and Traffic Law",
        "EXC": "Executive Law",
        "GOV": "General Obligations Law",
        "WKC": "Workers' Compensation Law",
    }

    def __init__(self, api_key: str | None = None):
        config = SourceConfig(
            jurisdiction="us-ny",
            name="New York",
            source_type="api",
            base_url="https://legislation.nysenate.gov/api/3",
            api_key=api_key,
            codes=self.NY_CODES,
            rate_limit=0.5,
        )
        super().__init__(config)

    def get_section(self, code: str, section: str, **kwargs) -> Statute | None:
        """Fetch a section from NY Open Legislation API."""
        try:
            # API endpoint: /laws/{lawId}
            # lawId format: TAX601 (code + section)
            law_id = f"{code}{section}"
            data = self._api_get(f"/laws/{law_id}")

            # Handle nested response structure
            result = data.get("result", data)
            if isinstance(result, dict) and "text" in result:
                doc = result
            else:
                print(f"Unexpected response format for {law_id}")
                return None

            text = doc.get("text", "")
            title = doc.get("title", f"§ {section}")

            # Parse subsections from text
            subsections = self._parse_subsections(text)

            return self._create_statute(
                code=code,
                section=section,
                title=title,
                text=text,
                source_url=f"https://legislation.nysenate.gov/api/3/laws/{law_id}",
                subsections=subsections,
            )

        except httpx.HTTPError as e:
            print(f"Error fetching NY {code} § {section}: {e}")
            return None

    def _parse_subsections(self, text: str) -> list[StatuteSubsection]:
        """Parse subsections from NY statute text."""
        subsections = []
        pattern = r"\(([a-z]|\d+)\)\s*([^(]+?)(?=\([a-z]|\d+\)|$)"

        for match in re.finditer(pattern, text, re.DOTALL):
            marker = match.group(1)
            content = match.group(2).strip()
            if content:
                subsections.append(
                    StatuteSubsection(
                        identifier=marker,
                        text=content[:1000] if len(content) > 1000 else content,
                    )
                )
        return subsections

    def list_sections(self, code: str, **kwargs) -> Iterator[str]:
        """List sections in a NY law code."""
        try:
            # Get law tree structure
            data = self._api_get(f"/laws/{code}")
            result = data.get("result", {})

            # Navigate tree to find sections
            def extract_sections(node):
                if isinstance(node, dict):
                    loc_id = node.get("locationId", "")
                    if loc_id and not loc_id.endswith("-"):
                        # Extract section number from locationId
                        section = loc_id.replace(code, "").strip("-")
                        if section and section[0].isdigit():
                            yield section

                    # Recurse into children
                    for child in node.get("documents", {}).get("items", []):
                        yield from extract_sections(child)

            yield from extract_sections(result)

        except httpx.HTTPError as e:
            print(f"Error listing NY {code} sections: {e}")


class LegiScanSource(APISource):
    """Source adapter for LegiScan API.

    LegiScan provides bill tracking for all 50 states.
    API: https://legiscan.com/legiscan
    Note: Requires API key (free registration)
    """

    def __init__(self, api_key: str):
        config = SourceConfig(
            jurisdiction="",  # Set per-state
            name="LegiScan",
            source_type="api",
            base_url="https://api.legiscan.com",
            api_key=api_key,
            rate_limit=1.0,  # LegiScan has strict rate limits
        )
        super().__init__(config)

    def get_section(self, code: str, section: str, **kwargs) -> Statute | None:
        """LegiScan is for bills, not codified statutes."""
        raise NotImplementedError(
            "LegiScan provides bills, not codified statutes. "
            "Use for legislative tracking, not statute archive."
        )

    def list_sections(self, code: str, **kwargs) -> Iterator[str]:
        """Not applicable for LegiScan."""
        raise NotImplementedError("LegiScan provides bills, not codified statutes.")

    def search_bills(self, state: str, query: str, year: int | None = None) -> list[dict]:
        """Search bills in a state.

        Args:
            state: State abbreviation (e.g., "CA", "NY")
            query: Search terms
            year: Optional year filter

        Returns:
            List of bill metadata dicts
        """
        params = {
            "key": self.config.api_key,
            "op": "getSearch",
            "state": state,
            "query": query,
        }
        if year:
            params["year"] = year

        response = self._api_get("", params=params)
        return response.get("searchresult", {}).get("bills", [])
