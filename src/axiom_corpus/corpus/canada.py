"""Source-first adapter for Canadian federal statutes.

Wraps the existing ``CanadaLegislationFetcher`` and ``CanadaStatuteParser``
into the corpus pipeline shape (sources, inventory, provisions, coverage),
producing canonical citation paths so ``corpus.provisions`` rows for Canada
match the convention used by every other jurisdiction.

Path convention:

* ``canada/statute/{consolidated_number}`` — act-level container row
  (no body, heading is the act's short title).
* ``canada/statute/{consolidated_number}/{section_number}`` — section row
  (section_number preserves dots, e.g. ``7.2``).
* ``canada/statute/{consolidated_number}/{section_number}/{label_chain}`` —
  subsection / paragraph / subparagraph / clause row, where each label
  segment has its surrounding parens stripped (``(1)`` → ``1``, ``(a)`` →
  ``a``). Empty labels fall back to a 1-indexed ordinal so paths remain
  unique.

IDs are deterministic UUID5 of the citation path (matching the rest of the
system), so re-runs upsert in place.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TextIO

import httpx
import requests

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.fetchers.legislation_canada import (
    CanadaActReference,
    CanadaLegislationFetcher,
)
from axiom_corpus.models_canada import CanadaSubsection
from axiom_corpus.parsers.canada import CanadaStatuteParser

CANADA_LEGISLATION_BASE_URL = "https://laws-lois.justice.gc.ca"
CANADA_LEGISLATION_SOURCE_FORMAT = "canada-lims-xml"

# Match the canonical jurisdiction slug (matches axiom-foundation.org/repo-map.ts).
CANADA_JURISDICTION = "canada"


@dataclass(frozen=True)
class CanadaStatuteExtractReport:
    """Result of a Canadian federal statute extract run."""

    jurisdiction: str
    document_class: str
    act_count: int
    section_count: int
    subsection_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]
    skipped_act_count: int = 0
    errors: tuple[str, ...] = ()


def extract_canada_acts(
    store: CorpusArtifactStore,
    *,
    version: str,
    fetcher: CanadaLegislationFetcher | None = None,
    only_acts: Iterable[str] | None = None,
    limit_acts: int | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    progress_stream: TextIO | None = None,
) -> CanadaStatuteExtractReport:
    """Snapshot Canadian federal acts and emit normalized provisions.

    ``fetcher`` is injectable so tests can pass a fake. By default a fresh
    `CanadaLegislationFetcher` is constructed with its baked-in rate limit.
    """
    jurisdiction = CANADA_JURISDICTION
    doc_class = DocumentClass.STATUTE
    run_id = version
    expression_date_text = _expression_date_text(expression_date, source_as_of, run_id)

    owns_fetcher = fetcher is None
    if fetcher is None:
        fetcher = CanadaLegislationFetcher()

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    errors: list[str] = []
    skipped = 0
    seen_paths: set[str] = set()
    section_count = 0
    subsection_count = 0
    act_codes_processed: list[str] = []

    try:
        act_codes = _resolve_act_codes(
            fetcher, only_acts=only_acts, limit_acts=limit_acts, errors=errors
        )

        for index, code in enumerate(act_codes):
            if progress_stream is not None and (index == 0 or (index + 1) % 25 == 0):
                print(
                    f"canada: extracting act {index + 1}/{len(act_codes)} ({code})",
                    file=progress_stream,
                    flush=True,
                )
            try:
                xml_bytes = fetcher.download_act(code)
            except (httpx.HTTPError, requests.RequestException, OSError) as exc:
                errors.append(f"{code}: download failed: {exc}")
                skipped += 1
                continue

            artifact_path = store.source_path(jurisdiction, doc_class, run_id, f"{code}.xml")
            sha256 = store.write_bytes(artifact_path, xml_bytes)
            source_paths.append(artifact_path)
            source_key = _source_key(jurisdiction, run_id, code)

            try:
                parser = CanadaStatuteParser(artifact_path)
                act_short_title = _safe_short_title(parser, code)
            except Exception as exc:  # noqa: BLE001 - parser may raise anything
                errors.append(f"{code}: parse failed: {exc}")
                skipped += 1
                continue

            act_codes_processed.append(code)

            act_path = f"{jurisdiction}/{doc_class.value}/{code}"
            act_url = f"{CANADA_LEGISLATION_BASE_URL}/eng/acts/{code}/"
            if act_path not in seen_paths:
                seen_paths.add(act_path)
                items.append(_inventory_item(act_path, act_url, source_key, sha256))
                records.append(
                    _record(
                        citation_path=act_path,
                        parent_citation_path=None,
                        heading=act_short_title or code,
                        body=None,
                        level=0,
                        ordinal=None,
                        source_url=act_url,
                        source_path=source_key,
                        source_as_of=source_as_of,
                        expression_date_text=expression_date_text,
                        legal_identifier=code,
                    )
                )

            try:
                section_iter = list(parser.iter_sections())
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{code}: iter_sections failed: {exc}")
                continue

            for ordinal, section in enumerate(section_iter, start=1):
                section_path = f"{act_path}/{section.section_number}"
                if section_path in seen_paths:
                    continue
                seen_paths.add(section_path)
                section_url = (
                    f"{CANADA_LEGISLATION_BASE_URL}/eng/acts/{code}/"
                    f"section-{section.section_number}.html"
                )
                items.append(_inventory_item(section_path, section_url, source_key, sha256))
                records.append(
                    _record(
                        citation_path=section_path,
                        parent_citation_path=act_path,
                        heading=(section.marginal_note or "").strip() or None,
                        body=section.text or None,
                        level=1,
                        ordinal=_section_ordinal(section.section_number, ordinal),
                        source_url=section_url,
                        source_path=source_key,
                        source_as_of=source_as_of,
                        expression_date_text=expression_date_text,
                        legal_identifier=f"{code} s. {section.section_number}",
                    )
                )
                section_count += 1

                for sub_record, sub_item in _emit_subsections(
                    section.subsections,
                    parent_citation_path=section_path,
                    parent_legal_identifier=f"{code} s. {section.section_number}",
                    level=2,
                    section_url=section_url,
                    source_key=source_key,
                    sha256=sha256,
                    source_as_of=source_as_of,
                    expression_date_text=expression_date_text,
                    seen_paths=seen_paths,
                ):
                    items.append(sub_item)
                    records.append(sub_record)
                    subsection_count += 1
    finally:
        if owns_fetcher:
            fetcher.close()

    if not items:
        raise ValueError("no Canada provisions extracted")

    inventory_path = store.inventory_path(jurisdiction, doc_class, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path(jurisdiction, doc_class, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=doc_class.value,
        version=run_id,
    )
    coverage_path = store.coverage_path(jurisdiction, doc_class, run_id)
    store.write_json(coverage_path, coverage.to_mapping())

    return CanadaStatuteExtractReport(
        jurisdiction=jurisdiction,
        document_class=doc_class.value,
        act_count=len(act_codes_processed),
        section_count=section_count,
        subsection_count=subsection_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        skipped_act_count=skipped,
        errors=tuple(errors),
    )


def _resolve_act_codes(
    fetcher: CanadaLegislationFetcher,
    *,
    only_acts: Iterable[str] | None,
    limit_acts: int | None,
    errors: list[str],
) -> list[str]:
    if only_acts is not None:
        codes = list(only_acts)
    else:
        try:
            references: list[CanadaActReference] = fetcher.list_all_acts()
        except httpx.HTTPError as exc:
            raise RuntimeError(f"failed to list Canadian acts: {exc}") from exc
        codes = [ref.code for ref in references]
        if not codes:
            errors.append("list_all_acts returned no acts")
    if limit_acts is not None:
        codes = codes[: max(0, limit_acts)]
    return codes


def _emit_subsections(
    subsections: list[CanadaSubsection],
    *,
    parent_citation_path: str,
    parent_legal_identifier: str,
    level: int,
    section_url: str,
    source_key: str,
    sha256: str,
    source_as_of: str | None,
    expression_date_text: str | None,
    seen_paths: set[str],
) -> Iterator[tuple[ProvisionRecord, SourceInventoryItem]]:
    for ordinal, sub in enumerate(subsections, start=1):
        segment = _label_segment(sub.label, ordinal)
        sub_path = f"{parent_citation_path}/{segment}"
        if sub_path in seen_paths:
            continue
        seen_paths.add(sub_path)

        legal_identifier = f"{parent_legal_identifier}({segment})"
        record = _record(
            citation_path=sub_path,
            parent_citation_path=parent_citation_path,
            heading=(sub.marginal_note or "").strip() or None,
            body=sub.text or None,
            level=level,
            ordinal=ordinal,
            source_url=section_url,
            source_path=source_key,
            source_as_of=source_as_of,
            expression_date_text=expression_date_text,
            legal_identifier=legal_identifier,
        )
        item = _inventory_item(sub_path, section_url, source_key, sha256)
        yield record, item

        yield from _emit_subsections(
            sub.children,
            parent_citation_path=sub_path,
            parent_legal_identifier=legal_identifier,
            level=level + 1,
            section_url=section_url,
            source_key=source_key,
            sha256=sha256,
            source_as_of=source_as_of,
            expression_date_text=expression_date_text,
            seen_paths=seen_paths,
        )


_LABEL_PUNCT_RE = re.compile(r"[\s\(\)\[\]\.]+")


def _label_segment(raw_label: str, ordinal: int) -> str:
    """Convert a Canadian subsection label to a path-safe segment.

    ``(1)`` -> ``1``, ``(a)`` -> ``a``, ``(i)`` -> ``i``. Falls back to the
    1-indexed ``ordinal`` when the parser couldn't extract a label.
    """
    cleaned = _LABEL_PUNCT_RE.sub("", raw_label or "").strip()
    return cleaned or str(ordinal)


_SECTION_NUMERIC_PREFIX_RE = re.compile(r"^(\d+)")


def _section_ordinal(section_number: str, fallback: int) -> int:
    match = _SECTION_NUMERIC_PREFIX_RE.match(section_number or "")
    if match:
        return int(match.group(1))
    return fallback


def _safe_short_title(parser: CanadaStatuteParser, code: str) -> str | None:
    try:
        title = parser.get_short_title()
    except Exception:  # noqa: BLE001 - LIMS variants sometimes lack the element
        return None
    title = (title or "").strip()
    return title or None


def _inventory_item(
    citation_path: str,
    source_url: str,
    source_path: str,
    sha256: str,
) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=citation_path,
        source_url=source_url,
        source_path=source_path,
        source_format=CANADA_LEGISLATION_SOURCE_FORMAT,
        sha256=sha256,
    )


def _record(
    *,
    citation_path: str,
    parent_citation_path: str | None,
    heading: str | None,
    body: str | None,
    level: int,
    ordinal: int | None,
    source_url: str,
    source_path: str,
    source_as_of: str | None,
    expression_date_text: str | None,
    legal_identifier: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        jurisdiction=CANADA_JURISDICTION,
        document_class=DocumentClass.STATUTE.value,
        citation_path=citation_path,
        parent_citation_path=parent_citation_path,
        heading=heading,
        body=body,
        level=level,
        ordinal=ordinal,
        source_url=source_url,
        source_path=source_path,
        source_format=CANADA_LEGISLATION_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date_text,
        legal_identifier=legal_identifier,
        language="en",
    )


def _source_key(jurisdiction: str, run_id: str, code: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{code}.xml"


def _expression_date_text(
    expression_date: date | str | None,
    source_as_of: str | None,
    run_id: str,
) -> str | None:
    if isinstance(expression_date, date):
        return expression_date.isoformat()
    if expression_date is not None:
        return str(expression_date)
    if source_as_of:
        return source_as_of
    return run_id if _looks_like_iso_date(run_id) else None


def _looks_like_iso_date(value: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}", value or ""))


__all__ = [
    "CANADA_JURISDICTION",
    "CANADA_LEGISLATION_BASE_URL",
    "CANADA_LEGISLATION_SOURCE_FORMAT",
    "CanadaStatuteExtractReport",
    "extract_canada_acts",
]
