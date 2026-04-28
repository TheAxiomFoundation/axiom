"""Pure section-to-rules conversion, no IO.

Extracts the repeated pattern from SupabaseIngestor's 4 near-identical
conversion methods into a single reusable function.
"""

import re
from collections.abc import Iterator
from uuid import NAMESPACE_URL, uuid5

from axiom_corpus.models import Section, Subsection


def _deterministic_id(citation_path: str) -> str:
    """Generate deterministic UUID from citation path for idempotent upserts."""
    return str(uuid5(NAMESPACE_URL, f"axiom:{citation_path}"))


def _numeric_prefix(value: object) -> int | None:
    """Return a leading integer prefix when a legal label starts with one."""
    match = re.match(r"(\d+)", str(value)) if value is not None else None
    return int(match.group(1)) if match else None


def title_container_to_rule(
    section: Section,
    jurisdiction: str,
    doc_type: str = "statute",
) -> dict | None:
    """Build the title-level container row for a section, when one is real.

    State converters still use title=0 as a sentinel for "state law", not as
    a real title. Avoid creating fake ``.../0`` branches until those converters
    emit their native hierarchy.
    """
    title = str(section.citation.title)
    if not title or title == "0":
        return None

    citation_path = f"{jurisdiction}/{doc_type}/{title}"
    heading = section.title_name.strip() if section.title_name else f"Title {title}"

    return {
        "id": _deterministic_id(citation_path),
        "jurisdiction": jurisdiction,
        "doc_type": doc_type,
        "parent_id": None,
        "level": 0,
        "ordinal": _numeric_prefix(title),
        "heading": heading,
        "body": None,
        "effective_date": None,
        "source_url": None,
        "source_path": None,
        "citation_path": citation_path,
        "rulespec_path": None,
        "has_rulespec": False,
    }


def section_to_rules(
    section: Section,
    jurisdiction: str,
    doc_type: str = "statute",
) -> Iterator[dict]:
    """Convert a Section to rule dictionaries.

    Yields rule dicts for the section and all its subsections.

    Args:
        section: Parsed Section model.
        jurisdiction: e.g. "us", "us-oh", "uk".
        doc_type: e.g. "statute", "regulation".
    """
    sec_num = section.citation.section
    title = section.citation.title
    title_rule = title_container_to_rule(section, jurisdiction, doc_type)
    citation_path = f"{jurisdiction}/{doc_type}/{title}/{sec_num}"
    section_id = _deterministic_id(citation_path)
    section_level = 1 if title_rule else 0

    if title_rule:
        yield title_rule

    yield {
        "id": section_id,
        "jurisdiction": jurisdiction,
        "doc_type": doc_type,
        "parent_id": title_rule["id"] if title_rule else None,
        "level": section_level,
        "ordinal": _numeric_prefix(sec_num),
        "heading": section.section_title,
        "body": section.text,
        "effective_date": (section.effective_date.isoformat() if section.effective_date else None),
        "source_url": section.source_url,
        "source_path": None,
        "citation_path": citation_path,
        "rulespec_path": None,
        "has_rulespec": False,
    }

    yield from _subsections_to_rules(
        section.subsections,
        jurisdiction=jurisdiction,
        doc_type=doc_type,
        parent_id=section_id,
        level=section_level + 1,
        parent_path=citation_path,
    )


def _subsections_to_rules(
    subsections: list[Subsection],
    jurisdiction: str,
    doc_type: str,
    parent_id: str,
    level: int,
    parent_path: str,
) -> Iterator[dict]:
    """Convert subsections to rule dictionaries recursively."""
    for i, sub in enumerate(subsections):
        sub_key = sub.identifier if sub.identifier else str(i + 1)
        citation_path = f"{parent_path}/{sub_key}"
        sub_id = _deterministic_id(citation_path)

        yield {
            "id": sub_id,
            "jurisdiction": jurisdiction,
            "doc_type": doc_type,
            "parent_id": parent_id,
            "level": level,
            "ordinal": i + 1,
            "heading": sub.heading,
            "body": sub.text,
            "effective_date": None,
            "source_url": None,
            "source_path": None,
            "citation_path": citation_path,
            "rulespec_path": None,
            "has_rulespec": False,
        }

        if sub.children:
            yield from _subsections_to_rules(
                sub.children,
                jurisdiction=jurisdiction,
                doc_type=doc_type,
                parent_id=sub_id,
                level=level + 1,
                parent_path=citation_path,
            )
