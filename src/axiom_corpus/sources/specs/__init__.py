"""State statute specs loader.

Each state has a YAML spec file defining:
- jurisdiction: e.g., "us-wa"
- name: e.g., "Washington"
- base_url: The main statute site
- toc_urls: Table of contents pages to discover sections
- section_pattern: Regex to identify section URLs
- crawler_type: "html" | "playwright" | "archive_org"
- archive_org_id: For Archive.org bulk downloads
- selectors: CSS selectors for content extraction
- codes: Relevant title/code mappings
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import yaml


@dataclass
class StateSpec:
    """Parsed state specification."""
    jurisdiction: str
    name: str
    base_url: str
    crawler_type: str = "html"
    source_type: str = "html"
    toc_urls: list[str] = field(default_factory=list)
    section_pattern: Optional[str] = None
    archive_org_id: Optional[str] = None
    selectors: dict[str, str] = field(default_factory=lambda: {"content": "body", "title": "title"})
    codes: dict[str, str] = field(default_factory=dict)


# Cache loaded specs
_specs_cache: dict[str, StateSpec] = {}


def get_specs_dir() -> Path:
    """Get the specs directory path."""
    return Path(__file__).parent


def load_spec(jurisdiction: str) -> Optional[StateSpec]:
    """Load a state spec by jurisdiction ID."""
    if jurisdiction in _specs_cache:
        return _specs_cache[jurisdiction]

    spec_file = get_specs_dir() / f"{jurisdiction}.yaml"
    if not spec_file.exists():
        return None

    with open(spec_file) as f:
        data = yaml.safe_load(f)

    spec = StateSpec(
        jurisdiction=data.get("jurisdiction", jurisdiction),
        name=data.get("name", jurisdiction.upper()),
        base_url=data.get("base_url", ""),
        crawler_type=data.get("crawler_type", "html"),
        source_type=data.get("source_type", "html"),
        toc_urls=data.get("toc_urls", []),
        section_pattern=data.get("section_pattern"),
        archive_org_id=data.get("archive_org_id"),
        selectors=data.get("selectors", {"content": "body", "title": "title"}),
        codes=data.get("codes", {}),
    )

    _specs_cache[jurisdiction] = spec
    return spec


def load_all_specs() -> dict[str, StateSpec]:
    """Load all state specs from YAML files."""
    specs = {}
    for spec_file in get_specs_dir().glob("us-*.yaml"):
        jurisdiction = spec_file.stem
        spec = load_spec(jurisdiction)
        if spec:
            specs[jurisdiction] = spec
    return specs


def get_section_pattern(jurisdiction: str) -> Optional[str]:
    """Get the section URL pattern for a jurisdiction."""
    spec = load_spec(jurisdiction)
    return spec.section_pattern if spec else None


def get_crawler_type(jurisdiction: str) -> str:
    """Get the crawler type for a jurisdiction."""
    spec = load_spec(jurisdiction)
    return spec.crawler_type if spec else "html"


def is_archive_org_state(jurisdiction: str) -> bool:
    """Check if a state uses Archive.org bulk download."""
    spec = load_spec(jurisdiction)
    return spec is not None and spec.crawler_type == "archive_org"


def is_playwright_state(jurisdiction: str) -> bool:
    """Check if a state requires Playwright (JavaScript SPA)."""
    spec = load_spec(jurisdiction)
    return spec is not None and spec.crawler_type == "playwright"
