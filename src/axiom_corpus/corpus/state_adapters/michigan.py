"""Michigan Compiled Laws source-first corpus adapter."""

from __future__ import annotations

import html
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

MICHIGAN_MCL_BASE_URL = "https://legislature.mi.gov/documents/mcl/"
MICHIGAN_MCL_WEB_BASE_URL = "https://www.legislature.mi.gov"
MICHIGAN_MCL_INDEX_SOURCE_FORMAT = "michigan-mcl-index-html"
MICHIGAN_MCL_CHAPTER_SOURCE_FORMAT = "michigan-mcl-chapter-xml"
MICHIGAN_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_CHAPTER_HREF_RE = re.compile(r"Chapter%20(?P<chapter>\d+)\.xml$", re.I)
_MCL_REF_RE = re.compile(
    r"\bM\.?\s*C\.?\s*L\.?\s+"
    r"(?P<mcl>\d+[A-Za-z]?\.\d+[A-Za-z]?(?:\.\d+[A-Za-z]?)*(?:[a-z])?)",
    re.I,
)


@dataclass(frozen=True)
class MichiganChapterListing:
    """One chapter XML file listed in the official MCL directory."""

    chapter: str
    source_url: str
    relative_path: str
    ordinal: int


@dataclass(frozen=True)
class MichiganChapter:
    """One Michigan Compiled Laws chapter container."""

    chapter: str
    heading: str
    document_id: str
    repealed: bool
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter}"

    @property
    def citation_path(self) -> str:
        return f"us-mi/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"MCL Chapter {self.chapter}"


@dataclass(frozen=True)
class MichiganSection:
    """One Michigan Compiled Laws section parsed from chapter XML."""

    chapter: str
    citation_id: str
    display_number: str
    label: str
    heading: str
    body: str | None
    document_id: str
    repealed: bool
    history: tuple[str, ...]
    hierarchy: tuple[str, ...]
    references_to: tuple[str, ...]
    notes: tuple[str, ...]
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return self.citation_id

    @property
    def citation_path(self) -> str:
        return f"us-mi/statute/{self.citation_id}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-mi/statute/chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"MCL {self.display_number}"

    @property
    def status(self) -> str | None:
        if self.repealed or re.search(r"\bRepealed\b", self.heading, re.I):
            return "repealed"
        return None


@dataclass(frozen=True)
class _MichiganSource:
    relative_path: str
    source_url: str
    source_format: str
    data: bytes


@dataclass(frozen=True)
class _RecordedSource:
    source_url: str
    source_path: str
    source_format: str
    sha256: str


@dataclass(frozen=True)
class _MichiganChapterFetchResult:
    listing: MichiganChapterListing
    source: _MichiganSource | None = None
    error: BaseException | None = None


class _MichiganFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        base_url: str,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.base_url = base_url.rstrip("/") + "/"
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_index(self) -> _MichiganSource:
        relative_path = f"{MICHIGAN_MCL_INDEX_SOURCE_FORMAT}/index.html"
        return _MichiganSource(
            relative_path=relative_path,
            source_url=self.base_url,
            source_format=MICHIGAN_MCL_INDEX_SOURCE_FORMAT,
            data=self._fetch(relative_path, self.base_url),
        )

    def fetch_chapter(self, listing: MichiganChapterListing) -> _MichiganSource:
        return _MichiganSource(
            relative_path=listing.relative_path,
            source_url=listing.source_url,
            source_format=MICHIGAN_MCL_CHAPTER_SOURCE_FORMAT,
            data=self._fetch(listing.relative_path, listing.source_url),
        )

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            self._last_request_at = time.monotonic()

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_michigan_source(
            source_url,
            fetcher=self,
            request_delay_seconds=self.request_delay_seconds,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
        )
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return data


def extract_michigan_compiled_laws(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = MICHIGAN_MCL_BASE_URL,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 120.0,
    request_attempts: int = 3,
    workers: int = 8,
) -> StateStatuteExtractReport:
    """Snapshot official Michigan Compiled Laws XML and extract provisions."""
    jurisdiction = "us-mi"
    chapter_filter = _chapter_filter(only_title)
    run_id = _michigan_run_id(version, chapter_filter=chapter_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _MichiganFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        base_url=base_url,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    source_paths: list[Path] = []
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    seen: set[str] = set()
    chapter_count = 0
    section_count = 0
    remaining_sections = limit

    index_source = fetcher.fetch_index()
    index_recorded = _record_source(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        source=index_source,
    )
    source_paths.append(
        store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            index_source.relative_path,
        )
    )
    listings = parse_michigan_chapter_index(
        index_source.data,
        source=index_recorded,
        base_url=base_url,
    )
    if chapter_filter is not None:
        listings = tuple(listing for listing in listings if listing.chapter == chapter_filter)
    if not listings:
        raise ValueError(f"no Michigan Compiled Laws chapters selected: {only_title!r}")

    chapter_results = (
        (_fetch_michigan_chapter_source(fetcher, listing) for listing in listings)
        if limit is not None
        else _fetch_michigan_chapter_sources(fetcher, list(listings), workers=workers)
    )
    for fetch_result in chapter_results:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        if fetch_result.error is not None:
            errors.append(f"chapter {fetch_result.listing.chapter}: {fetch_result.error}")
            continue
        assert fetch_result.source is not None
        chapter_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=fetch_result.source,
        )
        source_paths.append(
            store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                fetch_result.source.relative_path,
            )
        )
        try:
            chapter, sections = parse_michigan_chapter_xml(
                fetch_result.source.data,
                listing=fetch_result.listing,
                source=chapter_recorded,
            )
        except ValueError as exc:
            errors.append(f"chapter {fetch_result.listing.chapter}: {exc}")
            continue
        if _append_unique(
            seen,
            items,
            records,
            _chapter_inventory_item(chapter),
            _chapter_record(
                chapter,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            ),
        ):
            chapter_count += 1
        for section in sections:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            if _append_unique(
                seen,
                items,
                records,
                _section_inventory_item(section),
                _section_record(
                    section,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                ),
            ):
                section_count += 1
                if remaining_sections is not None:
                    remaining_sections -= 1

    if not records:
        raise ValueError("no Michigan Compiled Laws provisions extracted")

    inventory_path = store.inventory_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
    coverage_path = store.coverage_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return StateStatuteExtractReport(
        jurisdiction=jurisdiction,
        title_count=chapter_count,
        container_count=chapter_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        errors=tuple(errors),
    )


def parse_michigan_chapter_index(
    html_data: str | bytes,
    *,
    source: _RecordedSource,
    base_url: str = MICHIGAN_MCL_BASE_URL,
) -> tuple[MichiganChapterListing, ...]:
    """Parse the official MCL XML directory listing into chapter targets."""
    soup = BeautifulSoup(_decode_html(html_data), "lxml")
    listings: list[MichiganChapterListing] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = str(anchor.get("href"))
        match = _CHAPTER_HREF_RE.search(href)
        if match is None:
            continue
        chapter = str(int(match.group("chapter")))
        if chapter in seen:
            continue
        seen.add(chapter)
        listings.append(
            MichiganChapterListing(
                chapter=chapter,
                source_url=urljoin(base_url, f"Chapter%20{chapter}.xml"),
                relative_path=f"{MICHIGAN_MCL_CHAPTER_SOURCE_FORMAT}/chapter-{chapter}.xml",
                ordinal=len(listings) + 1,
            )
        )
    return tuple(sorted(listings, key=lambda listing: int(listing.chapter)))


def parse_michigan_chapter_xml(
    xml_data: str | bytes,
    *,
    listing: MichiganChapterListing,
    source: _RecordedSource,
) -> tuple[MichiganChapter, tuple[MichiganSection, ...]]:
    """Parse one official MCL chapter XML file."""
    root = _parse_xml(xml_data)
    if root.tag != "MCLChapterInfo":
        raise ValueError(f"expected MCLChapterInfo root, got {root.tag!r}")
    chapter_number = _child_text(root, "Name") or listing.chapter
    chapter = MichiganChapter(
        chapter=chapter_number,
        heading=_child_text(root, "Title") or f"Chapter {chapter_number}",
        document_id=_child_text(root, "DocumentID"),
        repealed=_bool_child(root, "Repealed"),
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        ordinal=listing.ordinal,
    )
    sections: list[MichiganSection] = []
    _walk_michigan_documents(
        root,
        chapter=chapter,
        source=source,
        hierarchy=(),
        sections=sections,
    )
    return chapter, tuple(sections)


def _walk_michigan_documents(
    elem: ET.Element,
    *,
    chapter: MichiganChapter,
    source: _RecordedSource,
    hierarchy: tuple[str, ...],
    sections: list[MichiganSection],
) -> None:
    next_hierarchy = hierarchy
    if elem.tag == "MCLStatuteInfo":
        label = _statute_label(elem)
        if label:
            next_hierarchy = (*hierarchy, label)
    elif elem.tag == "MCLDivisionInfo":
        label = _division_label(elem)
        if label:
            next_hierarchy = (*hierarchy, label)
    elif elem.tag == "MCLSectionInfo":
        section = _parse_michigan_section(
            elem,
            chapter=chapter,
            source=source,
            hierarchy=hierarchy,
            ordinal=len(sections) + 1,
        )
        if section is not None:
            sections.append(section)
        return

    collection = elem.find("MCLDocumentInfoCollection")
    if collection is None:
        return
    for child in list(collection):
        _walk_michigan_documents(
            child,
            chapter=chapter,
            source=source,
            hierarchy=next_hierarchy,
            sections=sections,
        )


def _parse_michigan_section(
    elem: ET.Element,
    *,
    chapter: MichiganChapter,
    source: _RecordedSource,
    hierarchy: tuple[str, ...],
    ordinal: int,
) -> MichiganSection | None:
    mcl_number = _child_text(elem, "MCLNumber")
    if not mcl_number:
        return None
    display_number = _normalize_display_number(mcl_number)
    citation_id = _citation_id_from_display_number(display_number)
    body = _fragment_text(_child_text(elem, "BodyText"))
    history = tuple(_fragment_list(_child_text(elem, "HistoryText"), tags=("historydata",)))
    notes = tuple(
        note
        for value in (_child_text(elem, "EditorsNotes"), _child_text(elem, "Commentary"))
        for note in _fragment_list(value, tags=("text", "p"))
    )
    text_for_references = "\n".join([body or "", *history, *notes])
    return MichiganSection(
        chapter=chapter.chapter,
        citation_id=citation_id,
        display_number=display_number,
        label=_child_text(elem, "Label"),
        heading=_strip_terminal_period(_child_text(elem, "CatchLine")),
        body=body,
        document_id=_child_text(elem, "DocumentID"),
        repealed=_bool_child(elem, "Repealed"),
        history=history,
        hierarchy=hierarchy,
        references_to=tuple(_extract_references(text_for_references)),
        notes=notes,
        source_url=(
            _mcl_section_url(display_number)
            if _is_numeric_mcl_number(display_number)
            else source.source_url
        ),
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        ordinal=ordinal,
    )


def _fetch_michigan_chapter_sources(
    fetcher: _MichiganFetcher,
    listings: list[MichiganChapterListing],
    *,
    workers: int,
) -> list[_MichiganChapterFetchResult]:
    if not listings:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_michigan_chapter_source(fetcher, listing) for listing in listings]
    results: list[_MichiganChapterFetchResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_michigan_chapter_source, fetcher, listing): listing
            for listing in listings
        }
        for future in as_completed(future_map):
            listing = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_MichiganChapterFetchResult(listing=listing, error=exc))
    order = {listing.chapter: index for index, listing in enumerate(listings)}
    return sorted(results, key=lambda result: order[result.listing.chapter])


def _fetch_michigan_chapter_source(
    fetcher: _MichiganFetcher,
    listing: MichiganChapterListing,
) -> _MichiganChapterFetchResult:
    try:
        return _MichiganChapterFetchResult(
            listing=listing,
            source=fetcher.fetch_chapter(listing),
        )
    except BaseException as exc:  # pragma: no cover
        return _MichiganChapterFetchResult(listing=listing, error=exc)


def _chapter_inventory_item(chapter: MichiganChapter) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=chapter.citation_path,
        source_url=chapter.source_url,
        source_path=chapter.source_path,
        source_format=chapter.source_format,
        sha256=chapter.sha256,
        metadata={"kind": "chapter", "chapter": chapter.chapter},
    )


def _chapter_record(
    chapter: MichiganChapter,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    metadata: dict[str, Any] = {"kind": "chapter", "chapter": chapter.chapter}
    if chapter.repealed:
        metadata["status"] = "repealed"
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-mi",
        document_class=DocumentClass.STATUTE.value,
        citation_path=chapter.citation_path,
        body=None,
        heading=chapter.heading,
        citation_label=chapter.legal_identifier,
        version=version,
        source_url=chapter.source_url,
        source_path=chapter.source_path,
        source_id=chapter.source_id,
        source_format=chapter.source_format,
        source_document_id=None,
        source_as_of=source_as_of,
        expression_date=expression_date,
        level=0,
        ordinal=chapter.ordinal,
        kind="chapter",
        legal_identifier=chapter.legal_identifier,
        identifiers={
            "mcl:chapter": chapter.chapter,
            **({"mcl:document_id": chapter.document_id} if chapter.document_id else {}),
        },
        metadata=metadata,
    )


def _section_inventory_item(section: MichiganSection) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=section.citation_path,
        source_url=section.source_url,
        source_path=section.source_path,
        source_format=section.source_format,
        sha256=section.sha256,
        metadata=_section_metadata(section),
    )


def _section_record(
    section: MichiganSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-mi",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        body=section.body,
        heading=section.heading,
        citation_label=section.legal_identifier,
        version=version,
        source_url=section.source_url,
        source_path=section.source_path,
        source_id=section.source_id,
        source_format=section.source_format,
        source_document_id=None,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.parent_citation_path,
        parent_id=deterministic_provision_id(section.parent_citation_path),
        level=1,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=section.legal_identifier,
        identifiers={
            "mcl": section.display_number,
            "mcl:citation_id": section.citation_id,
            "mcl:chapter": section.chapter,
            **({"mcl:document_id": section.document_id} if section.document_id else {}),
            **({"mcl:label": section.label} if section.label else {}),
        },
        metadata=_section_metadata(section),
    )


def _section_metadata(section: MichiganSection) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "chapter": section.chapter,
        "mcl_number": section.display_number,
        "citation_id": section.citation_id,
        "document_id": section.document_id,
    }
    if section.label:
        metadata["label"] = section.label
    if section.hierarchy:
        metadata["hierarchy"] = list(section.hierarchy)
    if section.history:
        metadata["source_history"] = list(section.history)
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.notes:
        metadata["source_notes"] = list(section.notes)
    if section.status:
        metadata["status"] = section.status
    return metadata


def _append_unique(
    seen: set[str],
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    item: SourceInventoryItem,
    record: ProvisionRecord,
) -> bool:
    if item.citation_path in seen:
        return False
    seen.add(item.citation_path)
    items.append(item)
    records.append(record)
    return True


def _record_source(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    source: _MichiganSource,
) -> _RecordedSource:
    path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        source.relative_path,
    )
    sha = store.write_bytes(path, source.data)
    return _RecordedSource(
        source_url=source.source_url,
        source_path=_store_relative_path(store, path),
        source_format=source.source_format,
        sha256=sha,
    )


def _download_michigan_source(
    source_url: str,
    *,
    fetcher: _MichiganFetcher,
    request_delay_seconds: float,
    timeout_seconds: float,
    request_attempts: int,
) -> bytes:
    last_error: BaseException | None = None
    for attempt in range(1, request_attempts + 1):
        try:
            fetcher.wait_for_request_slot()
            response = requests.get(
                source_url,
                timeout=timeout_seconds,
                headers={"User-Agent": MICHIGAN_USER_AGENT},
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds, 0.25) * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"Michigan source request failed: {source_url}")


def _parse_xml(value: str | bytes) -> ET.Element:
    text = _decode_xml(value)
    text = re.sub(r'encoding=["\']utf-16["\']', 'encoding="utf-8"', text, flags=re.I)
    return ET.fromstring(text.encode("utf-8"))


def _decode_xml(value: str | bytes) -> str:
    if isinstance(value, str):
        return value
    try:
        if value.startswith((b"\xff\xfe", b"\xfe\xff")):
            return value.decode("utf-16")
        return value.decode("utf-8-sig")
    except UnicodeDecodeError:
        return value.decode("utf-16-le", errors="replace")


def _decode_html(value: str | bytes) -> str:
    if isinstance(value, str):
        return value
    return value.decode("utf-8", errors="replace")


def _child_text(elem: ET.Element, tag: str) -> str:
    child = elem.find(tag)
    if child is None or child.text is None:
        return ""
    return _clean_whitespace(child.text)


def _bool_child(elem: ET.Element, tag: str) -> bool:
    return _child_text(elem, tag).lower() == "true"


def _fragment_text(value: str) -> str | None:
    blocks = _fragment_list(value, tags=("section-number", "p"))
    if not blocks:
        return None
    return _normalize_body("\n\n".join(blocks))


def _fragment_list(value: str, *, tags: tuple[str, ...]) -> list[str]:
    text = _clean_whitespace(html.unescape(value))
    if not text:
        return []
    soup = BeautifulSoup(f"<root>{text}</root>", "html.parser")
    matched_tags = soup.find_all(list(tags))
    blocks = [_clean_text(tag) for tag in matched_tags if _clean_text(tag)]
    if not blocks:
        blocks = [_clean_text(soup)]
    if not blocks:
        return []
    return blocks


def _statute_label(elem: ET.Element) -> str | None:
    name = _child_text(elem, "Name")
    heading = _strip_terminal_period(_child_text(elem, "Heading"))
    short_title = _strip_terminal_period(_child_text(elem, "ShortTitle"))
    if heading and name:
        return f"{name}: {heading}"
    return heading or short_title or name or None


def _division_label(elem: ET.Element) -> str | None:
    division_type = _child_text(elem, "DivisionType").upper()
    division_number = _child_text(elem, "DivisionNumber")
    title = _strip_terminal_period(_child_text(elem, "DivisionTitle"))
    label = " ".join(part for part in (division_type, division_number) if part)
    if title and label:
        return f"{label}: {title}"
    return title or label or None


def _extract_references(text: str) -> list[str]:
    refs = [
        f"us-mi/statute/{_citation_id_from_display_number(match.group('mcl'))}"
        for match in _MCL_REF_RE.finditer(text)
    ]
    return _dedupe_preserve_order(refs)


def _mcl_section_url(mcl_number: str) -> str:
    object_name = "mcl-" + _normalize_display_number(mcl_number).replace(".", "-")
    return f"{MICHIGAN_MCL_WEB_BASE_URL}/Laws/MCL?objectName={quote(object_name)}"


def _normalize_display_number(value: str) -> str:
    text = _clean_whitespace(value)
    compact = text.replace(" ", "")
    if re.match(r"^\d+[A-Za-z]?\.\S+$", compact):
        return compact
    return text


def _citation_id_from_display_number(value: str) -> str:
    display = _normalize_display_number(value)
    if _is_numeric_mcl_number(display):
        return display
    slug = display.replace("§", "-section-")
    slug = re.sub(r"(?<=[a-z])(?=[A-Z])", "-", slug)
    slug = re.sub(r"[^0-9A-Za-z.]+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug).strip("-").lower()
    return slug or "unknown"


def _is_numeric_mcl_number(value: str) -> bool:
    return bool(re.match(r"^\d+[A-Za-z]?\.\S+$", _normalize_display_number(value)))


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:chapter|ch\.?)[-\s]*", "", text, flags=re.I)
    return str(int(text)) if text.isdigit() else text


def _michigan_run_id(
    version: str,
    *,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    if chapter_filter is None and limit is None:
        return version
    parts = [version, "us-mi"]
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _normalize_body(text: str) -> str | None:
    normalized = _clean_whitespace(text)
    normalized = re.sub(r"\n[ \t]+", "\n", normalized)
    normalized = re.sub(r"[ \t]+\n", "\n", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    normalized = normalized.strip()
    return normalized or None


def _clean_text(value: Any) -> str:
    text = value.get_text(" ", strip=True) if hasattr(value, "get_text") else str(value)
    return _clean_whitespace(text)


def _clean_whitespace(value: str) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", value.replace("\xa0", " ")).strip()


def _strip_terminal_period(value: str) -> str:
    return value.strip().removesuffix(".").strip()


def _store_relative_path(store: CorpusArtifactStore, path: Path) -> str:
    try:
        return path.relative_to(store.root).as_posix()
    except ValueError:
        return path.as_posix()


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
