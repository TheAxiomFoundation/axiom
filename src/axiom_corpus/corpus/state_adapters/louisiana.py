"""Louisiana Revised Statutes source-first corpus adapter."""

from __future__ import annotations

import base64
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

LOUISIANA_RS_BASE_URL = "https://www.legis.la.gov/Legis/"
LOUISIANA_RS_ROOT_FOLDER = "75"
LOUISIANA_ROOT_TOC_SOURCE_FORMAT = "louisiana-rs-root-toc-html"
LOUISIANA_TITLE_TOC_SOURCE_FORMAT = "louisiana-rs-title-toc-html"
LOUISIANA_LAWPRINT_SOURCE_FORMAT = "louisiana-rs-lawprint-html"
LOUISIANA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_TITLE_LABEL_RE = re.compile(r"^TITLE\s+(?P<title>\d+[A-Z]?)$", re.I)
_RS_LABEL_RE = re.compile(
    r"^RS\s+(?P<title>\d+[A-Z]?)(?::(?P<section>[0-9A-Za-z][0-9A-Za-z.\-]*))?$",
    re.I,
)
_LAW_HREF_RE = re.compile(r"\bLaw\.aspx\?d=(?P<document_id>\d+)", re.I)
_VIEWSTATE_PRINTABLE_STRING_RE = re.compile(rb"[ -~]{2,}")
_RS_REFERENCE_RE = re.compile(
    r"\bR\.?\s*S\.?\s*"
    r"(?P<title>\d+[A-Za-z]?)\s*:\s*"
    r"(?P<section>\d+[A-Za-z]?(?:\.\d+[A-Za-z]?)*(?:-[0-9A-Za-z.]+)?)"
    r"(?:\s*\([A-Za-z0-9]+\))*",
    re.I,
)
_HISTORY_START_RE = re.compile(
    r"^(?:Added|Amended|Enacted|Formerly|Redesignated|Reenacted|Renumbered|Repealed|"
    r"Transferred|Acts)\b",
    re.I,
)
_CENTERED_HIERARCHY_RE = re.compile(
    r"^(?:TITLE|CHAPTER|PART|SUBPART|SUBCHAPTER)\b",
    re.I,
)


@dataclass(frozen=True)
class LouisianaTitleListing:
    """One title discovered from the official Revised Statutes root TOC."""

    title: str
    heading: str
    folder: str
    source_url: str
    ordinal: int


@dataclass(frozen=True)
class LouisianaTitle:
    """One Louisiana Revised Statutes title TOC page."""

    listing: LouisianaTitleListing
    source_url: str
    source_path: str
    source_format: str
    sha256: str

    @property
    def title(self) -> str:
        return self.listing.title

    @property
    def heading(self) -> str:
        return self.listing.heading

    @property
    def source_id(self) -> str:
        return f"title-{self.title}"

    @property
    def citation_path(self) -> str:
        return f"us-la/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"La. Rev. Stat. title {self.title}"


@dataclass(frozen=True)
class LouisianaSectionListing:
    """One section link parsed from an official title TOC page."""

    title: str
    section: str
    heading: str
    document_id: str
    source_url: str
    ordinal: int

    @property
    def section_label(self) -> str:
        return f"{self.title}:{self.section}"

    @property
    def relative_source_name(self) -> str:
        return f"{LOUISIANA_LAWPRINT_SOURCE_FORMAT}/title-{self.title}/{self.document_id}.html"


@dataclass(frozen=True)
class LouisianaSection:
    """One Louisiana Revised Statutes section parsed from a printable law page."""

    listing: LouisianaSectionListing
    section_label: str
    heading: str
    body: str | None
    source_history: tuple[str, ...]
    hierarchy: tuple[str, ...]
    references_to: tuple[str, ...]
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    status: str | None = None

    @property
    def title(self) -> str:
        return self.section_label.split(":", 1)[0]

    @property
    def section(self) -> str:
        return self.section_label.split(":", 1)[1]

    @property
    def source_id(self) -> str:
        return self.section_label

    @property
    def citation_path(self) -> str:
        return f"us-la/statute/{self.source_id}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-la/statute/title-{self.title}"

    @property
    def legal_identifier(self) -> str:
        return f"La. Rev. Stat. \u00a7 {self.section_label}"


@dataclass(frozen=True)
class _LouisianaSource:
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
class _LouisianaTitlePage:
    listing: LouisianaTitleListing
    source: _LouisianaSource | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _LouisianaSectionPage:
    listing: LouisianaSectionListing
    source: _LouisianaSource | None = None
    error: BaseException | None = None


class _LouisianaFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        base_url: str,
        root_folder: str,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.base_url = base_url.rstrip("/") + "/"
        self.root_folder = root_folder
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_root(self) -> _LouisianaSource:
        relative_path = f"{LOUISIANA_ROOT_TOC_SOURCE_FORMAT}/folder-{self.root_folder}.html"
        source_url = self._toc_url(self.root_folder)
        return _LouisianaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=LOUISIANA_ROOT_TOC_SOURCE_FORMAT,
            data=self._fetch(relative_path, source_url),
        )

    def fetch_title(self, listing: LouisianaTitleListing) -> _LouisianaSource:
        relative_path = f"{LOUISIANA_TITLE_TOC_SOURCE_FORMAT}/title-{listing.title}.html"
        return _LouisianaSource(
            relative_path=relative_path,
            source_url=listing.source_url,
            source_format=LOUISIANA_TITLE_TOC_SOURCE_FORMAT,
            data=self._fetch(relative_path, listing.source_url),
        )

    def fetch_section(self, listing: LouisianaSectionListing) -> _LouisianaSource:
        return _LouisianaSource(
            relative_path=listing.relative_source_name,
            source_url=listing.source_url,
            source_format=LOUISIANA_LAWPRINT_SOURCE_FORMAT,
            data=self._fetch(listing.relative_source_name, listing.source_url),
        )

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            self._last_request_at = time.monotonic()

    def _toc_url(self, folder: str) -> str:
        return urljoin(self.base_url, f"Laws_Toc.aspx?folder={folder}&level=Parent")

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_louisiana_source(
            source_url,
            fetcher=self,
            request_delay_seconds=self.request_delay_seconds,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
        )
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return data


def extract_louisiana_revised_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = LOUISIANA_RS_BASE_URL,
    root_folder: str = LOUISIANA_RS_ROOT_FOLDER,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
    workers: int = 8,
) -> StateStatuteExtractReport:
    """Snapshot official Louisiana Revised Statutes pages and extract provisions."""
    jurisdiction = "us-la"
    title_filter = _title_filter(only_title)
    run_id = _louisiana_run_id(version, title_filter=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _LouisianaFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        base_url=base_url,
        root_folder=str(root_folder),
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    source_paths: list[Path] = []
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    seen: set[str] = set()
    title_count = 0
    section_count = 0
    remaining_sections = limit

    root_source = fetcher.fetch_root()
    root_recorded = _record_source(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        source=root_source,
    )
    source_paths.append(
        store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            root_source.relative_path,
        )
    )
    title_listings = parse_louisiana_root(
        root_source.data,
        source=root_recorded,
        base_url=base_url,
    )
    if title_filter is not None:
        title_listings = tuple(
            title for title in title_listings if title.title == title_filter
        )
    if not title_listings:
        raise ValueError(f"no Louisiana Revised Statutes titles selected: {only_title!r}")

    for title_page in _fetch_louisiana_title_pages(
        fetcher,
        list(title_listings),
        workers=workers,
    ):
        if remaining_sections is not None and remaining_sections <= 0:
            break
        if title_page.error is not None:
            errors.append(f"title {title_page.listing.title}: {title_page.error}")
            continue
        assert title_page.source is not None
        title_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=title_page.source,
        )
        source_paths.append(
            store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                title_page.source.relative_path,
            )
        )
        title, listings = parse_louisiana_title_page(
            title_page.source.data,
            title_listing=title_page.listing,
            source=title_recorded,
            base_url=base_url,
        )
        if _append_unique(
            seen,
            items,
            records,
            _title_inventory_item(title),
            _title_record(
                title,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            ),
        ):
            title_count += 1

        selected_listings: list[LouisianaSectionListing] = []
        for listing in listings:
            if (
                remaining_sections is not None
                and len(selected_listings) >= remaining_sections
            ):
                break
            selected_listings.append(listing)

        for section_page in _fetch_louisiana_section_pages(
            fetcher,
            selected_listings,
            workers=workers,
        ):
            if section_page.error is not None:
                errors.append(
                    f"section {section_page.listing.section_label}: {section_page.error}"
                )
                continue
            assert section_page.source is not None
            section_recorded = _record_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                source=section_page.source,
            )
            source_paths.append(
                store.source_path(
                    jurisdiction,
                    DocumentClass.STATUTE,
                    run_id,
                    section_page.source.relative_path,
                )
            )
            try:
                section = parse_louisiana_section_page(
                    section_page.source.data,
                    listing=section_page.listing,
                    source=section_recorded,
                )
            except ValueError as exc:
                errors.append(f"section {section_page.listing.section_label}: {exc}")
                continue
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
        raise ValueError("no Louisiana Revised Statutes provisions extracted")

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
        title_count=title_count,
        container_count=title_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        errors=tuple(errors),
    )


def parse_louisiana_root(
    html: str | bytes,
    *,
    source: _RecordedSource,
    base_url: str = LOUISIANA_RS_BASE_URL,
) -> tuple[LouisianaTitleListing, ...]:
    """Parse the official Louisiana Revised Statutes root page into titles."""
    soup = BeautifulSoup(_decode(html), "lxml")
    folder_by_label = _title_folder_map_from_root(soup)
    titles: list[LouisianaTitleListing] = []
    seen: set[str] = set()
    for row in soup.find_all("tr"):
        anchors = [anchor for anchor in row.find_all("a") if isinstance(anchor, Tag)]
        if len(anchors) < 2:
            continue
        label = _clean_text(anchors[0])
        match = _TITLE_LABEL_RE.match(label)
        if match is None:
            continue
        title = match.group("title")
        if title in seen:
            continue
        folder = _folder_from_href(anchors[0].get("href")) or folder_by_label.get(
            label.upper()
        )
        if folder is None:
            raise ValueError(f"missing Louisiana folder id for {label}")
        seen.add(title)
        titles.append(
            LouisianaTitleListing(
                title=title,
                heading=_strip_terminal_period(_clean_text(anchors[1])),
                folder=folder,
                source_url=urljoin(base_url, f"Laws_Toc.aspx?folder={folder}&level=Parent"),
                ordinal=len(titles) + 1,
            )
        )
    return tuple(titles)


def parse_louisiana_title_page(
    html: str | bytes,
    *,
    title_listing: LouisianaTitleListing,
    source: _RecordedSource,
    base_url: str = LOUISIANA_RS_BASE_URL,
) -> tuple[LouisianaTitle, tuple[LouisianaSectionListing, ...]]:
    """Parse one official title TOC page into section law-print links."""
    soup = BeautifulSoup(_decode(html), "lxml")
    title = LouisianaTitle(
        listing=title_listing,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
    )
    listings: list[LouisianaSectionListing] = []
    seen_document_ids: set[str] = set()
    for row in soup.find_all("tr"):
        anchors = [
            anchor
            for anchor in row.find_all("a", href=True)
            if isinstance(anchor, Tag) and _LAW_HREF_RE.search(str(anchor.get("href")))
        ]
        if len(anchors) < 2:
            continue
        label = _normalize_rs_label(_clean_text(anchors[0]))
        label_match = _RS_LABEL_RE.match(label)
        if label_match is None or label_match.group("section") is None:
            continue
        if label_match.group("title") != title.title:
            continue
        document_id = _document_id_from_href(str(anchors[0].get("href")))
        if document_id is None or document_id in seen_document_ids:
            continue
        seen_document_ids.add(document_id)
        listings.append(
            LouisianaSectionListing(
                title=title.title,
                section=_normalize_section(label_match.group("section")),
                heading=_strip_terminal_period(_clean_text(anchors[1])),
                document_id=document_id,
                source_url=urljoin(base_url, f"LawPrint.aspx?d={document_id}"),
                ordinal=len(listings) + 1,
            )
        )
    return title, tuple(listings)


def parse_louisiana_section_page(
    html: str | bytes,
    *,
    listing: LouisianaSectionListing,
    source: _RecordedSource,
) -> LouisianaSection:
    """Parse one official printable law page into normalized text."""
    soup = BeautifulSoup(_decode(html), "lxml")
    label_node = soup.find(id="LabelName")
    section_label = listing.section_label
    if isinstance(label_node, Tag):
        label_match = _RS_LABEL_RE.match(_normalize_rs_label(_clean_text(label_node)))
        if label_match is not None and label_match.group("section") is not None:
            section_label = (
                f"{label_match.group('title')}:"
                f"{_normalize_section(label_match.group('section'))}"
            )

    document = soup.find(id="LabelDocument")
    if not isinstance(document, Tag):
        raise ValueError("missing printable law document")
    paragraphs = [_clean_text(paragraph) for paragraph in document.find_all("p")]
    paragraphs = [paragraph for paragraph in paragraphs if paragraph]
    if not paragraphs:
        text = _clean_text(document)
        paragraphs = [text] if text else []
    if not paragraphs:
        heading = _strip_terminal_period(listing.heading) or section_label
        return LouisianaSection(
            listing=listing,
            section_label=section_label,
            heading=heading,
            body=None,
            source_history=(),
            hierarchy=(),
            references_to=(),
            source_url=source.source_url,
            source_path=source.source_path,
            source_format=source.source_format,
            sha256=source.sha256,
            status=_empty_page_status(heading),
        )

    section_index, heading = _section_heading_from_paragraphs(
        paragraphs,
        section_label=section_label,
        fallback_heading=listing.heading,
    )
    hierarchy = tuple(
        paragraph
        for paragraph in paragraphs[:section_index]
        if _CENTERED_HIERARCHY_RE.match(paragraph)
    )
    body_lines = paragraphs[section_index + 1 :]
    source_history: list[str] = []
    while body_lines and _is_history_text(body_lines[-1]):
        source_history.insert(0, body_lines.pop())
    body = _normalize_body("\n\n".join(body_lines))
    status = _status(heading, body, source_history)
    references_to = tuple(_extract_references("\n".join([heading, body or ""])))
    return LouisianaSection(
        listing=listing,
        section_label=section_label,
        heading=heading,
        body=body,
        source_history=tuple(source_history),
        hierarchy=hierarchy,
        references_to=references_to,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        status=status,
    )


def _fetch_louisiana_title_pages(
    fetcher: _LouisianaFetcher,
    listings: list[LouisianaTitleListing],
    *,
    workers: int,
) -> list[_LouisianaTitlePage]:
    if not listings:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_louisiana_title_page(fetcher, listing) for listing in listings]
    results: list[_LouisianaTitlePage] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_louisiana_title_page, fetcher, listing): listing
            for listing in listings
        }
        for future in as_completed(future_map):
            listing = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_LouisianaTitlePage(listing=listing, error=exc))
    order = {listing.title: index for index, listing in enumerate(listings)}
    return sorted(results, key=lambda page: order[page.listing.title])


def _fetch_louisiana_title_page(
    fetcher: _LouisianaFetcher,
    listing: LouisianaTitleListing,
) -> _LouisianaTitlePage:
    try:
        return _LouisianaTitlePage(listing=listing, source=fetcher.fetch_title(listing))
    except BaseException as exc:  # pragma: no cover
        return _LouisianaTitlePage(listing=listing, error=exc)


def _fetch_louisiana_section_pages(
    fetcher: _LouisianaFetcher,
    listings: list[LouisianaSectionListing],
    *,
    workers: int,
) -> list[_LouisianaSectionPage]:
    if not listings:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_louisiana_section_page(fetcher, listing) for listing in listings]
    results: list[_LouisianaSectionPage] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_louisiana_section_page, fetcher, listing): listing
            for listing in listings
        }
        for future in as_completed(future_map):
            listing = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_LouisianaSectionPage(listing=listing, error=exc))
    order = {listing.document_id: index for index, listing in enumerate(listings)}
    return sorted(results, key=lambda page: order[page.listing.document_id])


def _fetch_louisiana_section_page(
    fetcher: _LouisianaFetcher,
    listing: LouisianaSectionListing,
) -> _LouisianaSectionPage:
    try:
        return _LouisianaSectionPage(listing=listing, source=fetcher.fetch_section(listing))
    except BaseException as exc:  # pragma: no cover
        return _LouisianaSectionPage(listing=listing, error=exc)


def _title_inventory_item(title: LouisianaTitle) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=title.citation_path,
        source_url=title.source_url,
        source_path=title.source_path,
        source_format=title.source_format,
        sha256=title.sha256,
        metadata={"kind": "title", "title": title.title},
    )


def _title_record(
    title: LouisianaTitle,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(title.citation_path),
        jurisdiction="us-la",
        document_class=DocumentClass.STATUTE.value,
        citation_path=title.citation_path,
        body=None,
        heading=title.heading,
        citation_label=title.legal_identifier,
        version=version,
        source_url=title.source_url,
        source_path=title.source_path,
        source_id=title.source_id,
        source_format=title.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        level=0,
        ordinal=title.listing.ordinal,
        kind="title",
        legal_identifier=title.legal_identifier,
        identifiers={"louisiana:title": title.title},
        metadata={"kind": "title", "title": title.title, "folder": title.listing.folder},
    )


def _section_inventory_item(section: LouisianaSection) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=section.citation_path,
        source_url=section.source_url,
        source_path=section.source_path,
        source_format=section.source_format,
        sha256=section.sha256,
        metadata=_section_metadata(section),
    )


def _section_record(
    section: LouisianaSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-la",
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
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.parent_citation_path,
        parent_id=deterministic_provision_id(section.parent_citation_path),
        level=1,
        ordinal=section.listing.ordinal,
        kind="section",
        legal_identifier=section.legal_identifier,
        identifiers={
            "louisiana:title": section.title,
            "louisiana:section": section.section,
            "louisiana:rs": section.section_label,
            "louisiana:document_id": section.listing.document_id,
        },
        metadata=_section_metadata(section),
    )


def _section_metadata(section: LouisianaSection) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": section.title,
        "section": section.section,
        "section_label": section.section_label,
        "document_id": section.listing.document_id,
    }
    if section.hierarchy:
        metadata["hierarchy"] = list(section.hierarchy)
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
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
    source: _LouisianaSource,
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


def _download_louisiana_source(
    source_url: str,
    *,
    fetcher: _LouisianaFetcher,
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
                headers={"User-Agent": LOUISIANA_USER_AGENT},
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds, 0.25) * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"Louisiana source request failed: {source_url}")


def _title_folder_map_from_root(soup: BeautifulSoup) -> dict[str, str]:
    hidden = soup.find(id="__VIEWSTATE")
    value = hidden.get("value") if isinstance(hidden, Tag) else None
    if not value:
        return {}
    try:
        raw = base64.b64decode(str(value))
    except ValueError:
        return {}
    strings = [
        item.decode("latin1").rstrip("d")
        for item in _VIEWSTATE_PRINTABLE_STRING_RE.findall(raw)
    ]
    out: dict[str, str] = {}
    for index, item in enumerate(strings[:-1]):
        if not re.fullmatch(r"\d{2,5}", item):
            continue
        next_item = strings[index + 1]
        if _TITLE_LABEL_RE.match(next_item):
            out.setdefault(next_item.upper(), item)
    return out


def _folder_from_href(value: Any) -> str | None:
    if value is None:
        return None
    parsed = urlparse(str(value))
    query = parse_qs(parsed.query)
    folder = query.get("folder")
    return str(folder[0]) if folder else None


def _document_id_from_href(value: str) -> str | None:
    match = _LAW_HREF_RE.search(value)
    return match.group("document_id") if match is not None else None


def _section_heading_from_paragraphs(
    paragraphs: list[str],
    *,
    section_label: str,
    fallback_heading: str,
) -> tuple[int, str]:
    section_tail = re.escape(section_label.split(":", 1)[1])
    exact_re = re.compile(rf"^\u00a7+\s*{section_tail}\.?\s*(?P<heading>.*)$", re.I)
    generic_re = re.compile(r"^\u00a7+\s*[0-9A-Za-z.\-]+\.?\s*(?P<heading>.*)$", re.I)
    for index, paragraph in enumerate(paragraphs):
        match = exact_re.match(paragraph) or generic_re.match(paragraph)
        if match is None:
            continue
        heading = _strip_terminal_period(match.group("heading")) or fallback_heading
        return index, heading
    return 0, fallback_heading


def _is_history_text(value: str) -> bool:
    return bool(_HISTORY_START_RE.match(value))


def _status(heading: str, body: str | None, history: list[str]) -> str | None:
    text = "\n".join([heading, body or "", *history])
    empty_status = _empty_page_status(heading)
    if empty_status == "reserved":
        return empty_status
    if re.search(r"\bRepealed\b", text, re.I):
        return "repealed"
    if re.search(r"\bExpired\b", text, re.I):
        return "expired"
    return None


def _empty_page_status(heading: str) -> str:
    if re.search(r"\bReserved\b", heading, re.I):
        return "reserved"
    return "empty_official_page"


def _extract_references(text: str) -> list[str]:
    refs = [
        f"us-la/statute/{match.group('title')}:{_normalize_section(match.group('section'))}"
        for match in _RS_REFERENCE_RE.finditer(text)
    ]
    return _dedupe_preserve_order(refs)


def _normalize_rs_label(value: str) -> str:
    text = _clean_whitespace(value)
    text = re.sub(r"^R\.?\s*S\.?", "RS", text, flags=re.I)
    text = re.sub(r"\s*:\s*", ":", text)
    return text.strip().removesuffix(".")


def _normalize_section(value: str) -> str:
    text = _clean_whitespace(value)
    text = text.replace("\u2010", "-").replace("\u2011", "-")
    text = text.replace("\u2012", "-").replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", "", text)
    return text.removesuffix(".")


def _louisiana_run_id(
    version: str,
    *,
    title_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and limit is None:
        return version
    parts = [version, "us-la"]
    if title_filter is not None:
        parts.append(f"title-{title_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title|tit\.?)[-\s]*", "", text, flags=re.I)
    return text.upper() if text else None


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


def _decode(value: str | bytes) -> str:
    if isinstance(value, str):
        return value
    try:
        return value.decode("utf-8-sig")
    except UnicodeDecodeError:
        return value.decode("windows-1252", errors="replace")


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
