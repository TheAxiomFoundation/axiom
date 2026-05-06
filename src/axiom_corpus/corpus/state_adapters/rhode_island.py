"""Rhode Island General Laws source-first corpus adapter."""

from __future__ import annotations

import re
import time
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

RHODE_ISLAND_GENERAL_LAWS_BASE_URL = "https://webserver.rilegislature.gov/Statutes/"
RHODE_ISLAND_GENERAL_LAWS_INDEX = "Statutes.html"
RHODE_ISLAND_GENERAL_LAWS_SOURCE_FORMAT = "rhode-island-general-laws-html"
RHODE_ISLAND_GENERAL_LAWS_DEFAULT_YEAR = 2026
RHODE_ISLAND_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_TITLE_PATH_RE = re.compile(r"(?:^|/)TITLE(?P<title>\d+[A-Z]?(?:\.\d+)?)/INDEX\.HTM$", re.I)
_CHAPTER_PATH_RE = re.compile(
    r"(?:^|/)(?P<chapter>\d+[A-Z]?(?:\.\d+)?-\d+[A-Z]?(?:\.\d+)?)/INDEX\.HTM$",
    re.I,
)
_SECTION_CITE_PATTERN = (
    r"\d+[A-Z]?(?:\.\d+)?-\d+[A-Z]?(?:\.\d+)?-\d+[A-Z]?(?:\.\d+)?"
)
_SECTION_CITE_RE = re.compile(_SECTION_CITE_PATTERN)
_SECTION_CAPTION_RE = re.compile(
    rf"^\s*\u00a7{{1,2}}\s*"
    rf"(?P<numbers>{_SECTION_CITE_PATTERN}"
    rf"(?:\s*(?:,|\u2014|\u2013|-|to)\s*{_SECTION_CITE_PATTERN})*)"
    rf"\.\s*(?P<heading>.*)$",
    re.I,
)
_TEXT_REFERENCE_RE = re.compile(
    rf"(?:R\.?\s*I\.?\s+Gen\.?\s+Laws\s+)?\u00a7{{1,2}}\s*"
    rf"(?P<cite>{_SECTION_CITE_PATTERN})",
    re.I,
)
_EFFECTIVE_NOTE_RE = re.compile(
    r"(?P<bracket>\[(?:Heading\s+)?Effective[^\]]+\])|"
    r"(?P<paren>\((?:Heading\s+)?Effective[^)]+\))",
    re.I,
)
_EFFECTIVE_DATE_RE = re.compile(r"([A-Z][a-z]+ \d{1,2}, \d{4})")


@dataclass(frozen=True)
class RhodeIslandTitle:
    """Title entry parsed from the official General Laws title index."""

    number: str
    heading: str | None
    relative_path: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"title-{self.number}"

    @property
    def citation_path(self) -> str:
        return f"us-ri/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"R.I. Gen. Laws Title {self.number}"


@dataclass(frozen=True)
class RhodeIslandContainerLink:
    """Chapter, part, or article link from an official RI index page."""

    kind: str
    source_id: str
    display_number: str
    heading: str | None
    relative_path: str
    ordinal: int
    parent_citation_path: str | None
    level: int
    title: str | None
    chapter: str | None
    effective_notes: tuple[str, ...] = ()
    status: str | None = None
    source_year: int | None = None

    @property
    def citation_path(self) -> str:
        return f"us-ri/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"R.I. Gen. Laws {self.kind.title()} {self.display_number}"


@dataclass(frozen=True)
class RhodeIslandSection:
    """Section text parsed from one official RI section page."""

    source_id: str
    section: str
    display_number: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int
    references_to: tuple[str, ...]
    source_history: tuple[str, ...]
    effective_notes: tuple[str, ...]
    status: str | None
    title: str | None
    chapter: str | None
    range_end: str | None = None
    related_sections: tuple[str, ...] = ()
    variant: str | None = None

    @property
    def citation_path(self) -> str:
        return f"us-ri/statute/{self.source_id}"

    @property
    def canonical_citation_path(self) -> str:
        return f"us-ri/statute/{self.section}"

    @property
    def legal_identifier(self) -> str:
        marker = "\u00a7\u00a7" if self.range_end or self.related_sections else "\u00a7"
        return f"R.I. Gen. Laws {marker} {self.display_number}"


@dataclass(frozen=True)
class RhodeIslandIndexPage:
    """Parsed title/chapter/part/article index page."""

    heading: str | None
    source_history: tuple[str, ...]
    effective_notes: tuple[str, ...]
    status: str | None
    child_containers: tuple[RhodeIslandContainerLink, ...]
    section_links: tuple[RhodeIslandContainerLink, ...]


@dataclass(frozen=True)
class _RhodeIslandSourcePage:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _RecordedSource:
    source_url: str
    source_path: str
    sha256: str


class _RhodeIslandFetcher:
    def __init__(
        self,
        *,
        base_url: str,
        source_dir: Path | None,
        download_dir: Path | None,
    ) -> None:
        self.base_url = _base_url(base_url)
        self.source_dir = source_dir
        self.download_dir = download_dir

    def fetch(self, relative_path: str) -> _RhodeIslandSourcePage:
        normalized = _normalize_relative_path(relative_path)
        source_url = urljoin(self.base_url, normalized)
        if self.source_dir is not None:
            return _RhodeIslandSourcePage(
                relative_path=normalized,
                source_url=source_url,
                data=_read_source_file(self.source_dir, normalized),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            if cached_path.exists():
                return _RhodeIslandSourcePage(
                    relative_path=normalized,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )

        data = _download_rhode_island_page(source_url)
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            _write_cache_bytes(cached_path, data)
        return _RhodeIslandSourcePage(relative_path=normalized, source_url=source_url, data=data)


def extract_rhode_island_general_laws(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_year: int = RHODE_ISLAND_GENERAL_LAWS_DEFAULT_YEAR,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    only_chapter: str | int | None = None,
    limit: int | None = None,
    workers: int = 8,
    download_dir: str | Path | None = None,
    base_url: str = RHODE_ISLAND_GENERAL_LAWS_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official Rhode Island General Laws HTML and extract provisions."""
    jurisdiction = "us-ri"
    title_filter = _title_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter)
    if title_filter is None and chapter_filter is not None:
        title_filter = _title_from_chapter(chapter_filter)
    run_id = _rhode_island_run_id(
        version,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _RhodeIslandFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
    )

    source_paths: list[Path] = []
    source_by_relative: dict[str, _RecordedSource] = {}
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    errors: list[str] = []
    remaining_sections = limit

    root_page = fetcher.fetch(RHODE_ISLAND_GENERAL_LAWS_INDEX)
    _record_source_page(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        page=root_page,
        source_paths=source_paths,
        source_by_relative=source_by_relative,
    )
    titles = parse_rhode_island_general_laws_index(root_page.data)
    if title_filter is not None:
        titles = tuple(title for title in titles if title.number == title_filter)
    if not titles:
        raise ValueError(f"no Rhode Island General Laws titles selected for filter: {only_title!r}")

    selected_chapter_count = 0
    for title in titles:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        title_page = fetcher.fetch(title.relative_path)
        title_source = _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=title_page,
            source_paths=source_paths,
            source_by_relative=source_by_relative,
        )
        title_index = parse_rhode_island_title_index(title_page.data, title=title)
        title_heading = title_index.heading or title.heading
        if title.citation_path not in seen:
            seen.add(title.citation_path)
            title_count += 1
            container_count += 1
            _append_inventory_and_record(
                items,
                records,
                citation_path=title.citation_path,
                version=run_id,
                source=title_source,
                source_id=title.source_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="title",
                heading=title_heading,
                legal_identifier=title.legal_identifier,
                level=0,
                ordinal=title.ordinal,
                identifiers={"rhode_island:title": title.number},
                metadata={
                    "kind": "title",
                    "title": title.number,
                    "display_number": title.number,
                    "source_year": source_year,
                },
            )

        chapter_links = title_index.child_containers
        if chapter_filter is not None:
            chapter_links = tuple(link for link in chapter_links if link.source_id == chapter_filter)
        selected_chapter_count += len(chapter_links)
        for chapter_link in chapter_links:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            chapter_page = fetcher.fetch(chapter_link.relative_path)
            chapter_source = _record_source_page(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                page=chapter_page,
                source_paths=source_paths,
                source_by_relative=source_by_relative,
            )
            chapter_index = parse_rhode_island_container_index(
                chapter_page.data,
                parent=chapter_link,
            )
            chapter_provision = _container_from_link(
                chapter_link,
                page=chapter_index,
                source_year=source_year,
            )
            if chapter_provision.citation_path not in seen:
                seen.add(chapter_provision.citation_path)
                container_count += 1
                _append_container(
                    items,
                    records,
                    chapter_provision,
                    source=chapter_source,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )

            if chapter_index.child_containers:
                for child_link in chapter_index.child_containers:
                    if remaining_sections is not None and remaining_sections <= 0:
                        break
                    child_page = fetcher.fetch(child_link.relative_path)
                    child_source = _record_source_page(
                        store,
                        jurisdiction=jurisdiction,
                        run_id=run_id,
                        page=child_page,
                        source_paths=source_paths,
                        source_by_relative=source_by_relative,
                    )
                    child_index = parse_rhode_island_container_index(
                        child_page.data,
                        parent=child_link,
                    )
                    child_provision = _container_from_link(
                        child_link,
                        page=child_index,
                        source_year=source_year,
                    )
                    if child_provision.citation_path not in seen:
                        seen.add(child_provision.citation_path)
                        container_count += 1
                        _append_container(
                            items,
                            records,
                            child_provision,
                            source=child_source,
                            version=run_id,
                            source_as_of=source_as_of_text,
                            expression_date=expression_date_text,
                        )
                    remaining_sections, written = _append_sections_from_links(
                        items,
                        records,
                        seen,
                        fetcher=fetcher,
                        store=store,
                        jurisdiction=jurisdiction,
                        run_id=run_id,
                        source_paths=source_paths,
                        source_by_relative=source_by_relative,
                        section_links=child_index.section_links,
                        source_year=source_year,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                        remaining_sections=remaining_sections,
                        workers=workers,
                    )
                    section_count += written
            else:
                remaining_sections, written = _append_sections_from_links(
                    items,
                    records,
                    seen,
                    fetcher=fetcher,
                    store=store,
                    jurisdiction=jurisdiction,
                    run_id=run_id,
                    source_paths=source_paths,
                    source_by_relative=source_by_relative,
                    section_links=chapter_index.section_links,
                    source_year=source_year,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    remaining_sections=remaining_sections,
                    workers=workers,
                )
                section_count += written

    if chapter_filter is not None and selected_chapter_count == 0:
        raise ValueError(
            f"no Rhode Island General Laws chapters selected for filter: {only_chapter!r}"
        )
    if not items:
        raise ValueError("no Rhode Island General Laws provisions extracted")

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
        container_count=container_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        errors=tuple(errors),
    )


def parse_rhode_island_general_laws_index(
    html: str | bytes,
) -> tuple[RhodeIslandTitle, ...]:
    """Parse the official top-level Rhode Island General Laws title index."""
    soup = BeautifulSoup(html, "lxml")
    titles: list[RhodeIslandTitle] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):  # pragma: no cover
            continue
        relative_path = _normalize_relative_path(str(anchor.get("href") or ""))
        title_number = _title_from_relative(relative_path)
        if title_number is None or title_number in seen:
            continue
        seen.add(title_number)
        row = anchor.find_parent("tr")
        row_text = _clean_text(row.get_text(" ", strip=True)) if isinstance(row, Tag) else ""
        anchor_text = _clean_text(anchor.get_text(" ", strip=True))
        heading = _strip_leading_token(row_text or anchor_text, title_number)
        titles.append(
            RhodeIslandTitle(
                number=title_number,
                heading=heading,
                relative_path=relative_path,
                ordinal=len(titles),
            )
        )
    return tuple(titles)


def parse_rhode_island_title_index(
    html: str | bytes,
    *,
    title: RhodeIslandTitle,
) -> RhodeIslandIndexPage:
    """Parse one official title page and return chapter links."""
    soup = BeautifulSoup(html, "lxml")
    heading = _page_heading(soup, "title")
    chapter_links: list[RhodeIslandContainerLink] = []
    for ordinal, anchor in enumerate(_index_links(soup)):
        relative_path = _resolve_relative(title.relative_path, str(anchor.get("href") or ""))
        chapter = _chapter_from_relative(relative_path)
        if chapter is None:
            continue
        parsed = _parse_container_caption(
            _clean_text(anchor.get_text(" ", strip=True)),
            fallback_kind="chapter",
            fallback_number=chapter,
        )
        chapter_links.append(
            RhodeIslandContainerLink(
                kind="chapter",
                source_id=chapter,
                display_number=chapter,
                heading=parsed.heading,
                relative_path=relative_path,
                ordinal=ordinal,
                parent_citation_path=title.citation_path,
                level=1,
                title=title.number,
                chapter=chapter,
                effective_notes=parsed.effective_notes,
                status=parsed.status,
            )
        )
    return RhodeIslandIndexPage(
        heading=heading,
        source_history=_source_history(soup),
        effective_notes=_effective_notes(_clean_text(soup.get_text(" ", strip=True))),
        status=_status(heading, None, ()),
        child_containers=tuple(chapter_links),
        section_links=(),
    )


def parse_rhode_island_container_index(
    html: str | bytes,
    *,
    parent: RhodeIslandContainerLink,
) -> RhodeIslandIndexPage:
    """Parse one chapter, part, or article page."""
    soup = BeautifulSoup(html, "lxml")
    heading = _page_heading(soup, parent.kind)
    history = _source_history(soup)
    page_text = _clean_text(soup.get_text(" ", strip=True))
    effective_notes = _effective_notes(page_text)
    child_containers: list[RhodeIslandContainerLink] = []
    section_links: list[RhodeIslandContainerLink] = []

    for ordinal, anchor in enumerate(_index_links(soup)):
        href = str(anchor.get("href") or "")
        text = _clean_text(anchor.get_text(" ", strip=True))
        relative_path = _resolve_relative(parent.relative_path, href)
        if relative_path.lower().endswith("/index.htm"):
            parsed = _parse_container_caption(text, fallback_kind="part", fallback_number=str(ordinal + 1))
            kind = parsed.kind if parsed.kind in {"part", "article"} else "part"
            source_id = _child_container_source_id(parent.source_id, kind, parsed.display_number)
            child_containers.append(
                RhodeIslandContainerLink(
                    kind=kind,
                    source_id=source_id,
                    display_number=parsed.display_number,
                    heading=parsed.heading,
                    relative_path=relative_path,
                    ordinal=ordinal,
                    parent_citation_path=parent.citation_path,
                    level=parent.level + 1,
                    title=parent.title,
                    chapter=parent.chapter or parent.source_id,
                    effective_notes=parsed.effective_notes,
                    status=parsed.status,
                )
            )
        elif relative_path.lower().endswith(".htm"):
            parsed = _parse_section_link(text, relative_path=relative_path)
            if parsed.source_id is None:
                continue
            section_links.append(
                RhodeIslandContainerLink(
                    kind="section",
                    source_id=parsed.source_id,
                    display_number=parsed.display_number,
                    heading=parsed.heading,
                    relative_path=relative_path,
                    ordinal=ordinal,
                    parent_citation_path=parent.citation_path,
                    level=parent.level + 1,
                    title=parent.title,
                    chapter=parent.chapter or parent.source_id,
                    effective_notes=parsed.effective_notes,
                    status=parsed.status,
                )
            )
    return RhodeIslandIndexPage(
        heading=heading,
        source_history=history,
        effective_notes=effective_notes,
        status=_status(heading, None, history),
        child_containers=tuple(child_containers),
        section_links=tuple(section_links),
    )


def parse_rhode_island_section_html(
    html: str | bytes,
    *,
    parent_citation_path: str | None = None,
    level: int = 2,
    ordinal_start: int = 0,
    fallback_source_id: str | None = None,
) -> tuple[RhodeIslandSection, ...]:
    """Parse one official section HTML file into one or more normalized sections."""
    soup = BeautifulSoup(html, "lxml")
    section_blocks = _section_blocks(soup)
    occurrence_by_section: dict[str, int] = defaultdict(int)
    used_source_ids: set[str] = set()
    sections: list[RhodeIslandSection] = []

    for block in section_blocks:
        bold = _section_heading_tag(block)
        if bold is None:
            continue
        caption = _parse_section_caption(_clean_text(bold.get_text(" ", strip=True)))
        section_number = caption.source_id or fallback_source_id
        if section_number is None:
            continue
        body_lines, history, references = _section_body_history_references(block, bold)
        body = "\n".join(body_lines).strip() or None
        text_for_status = " ".join(
            part
            for part in (caption.heading, body, " ".join(history))
            if part
        )
        effective_notes = tuple(
            dict.fromkeys(caption.effective_notes + _effective_notes(text_for_status))
        )
        occurrence_by_section[section_number] += 1
        occurrence = occurrence_by_section[section_number]
        variant = _variant_for_occurrence(effective_notes, occurrence)
        source_id = _section_source_id(section_number, variant)
        if source_id in used_source_ids:
            variant = _disambiguated_variant(
                variant,
                occurrence=occurrence,
                section_number=section_number,
                used_source_ids=used_source_ids,
            )
            source_id = _section_source_id(section_number, variant)
        used_source_ids.add(source_id)
        chapter = _chapter_from_section(section_number)
        self_path = f"us-ri/statute/{section_number}"
        references_to = tuple(dict.fromkeys(ref for ref in references if ref != self_path))
        sections.append(
            RhodeIslandSection(
                source_id=source_id,
                section=section_number,
                display_number=caption.display_number,
                heading=caption.heading,
                body=body,
                parent_citation_path=parent_citation_path
                or (f"us-ri/statute/{chapter}" if chapter else None),
                level=level,
                ordinal=ordinal_start + len(sections),
                references_to=references_to,
                source_history=history,
                effective_notes=effective_notes,
                status=_status(caption.heading, body, history, effective_notes=effective_notes),
                title=_title_from_section(section_number),
                chapter=chapter,
                range_end=caption.range_end,
                related_sections=caption.related_sections,
                variant=variant,
            )
        )
    return tuple(sections)


@dataclass(frozen=True)
class _ParsedCaption:
    kind: str
    source_id: str | None
    display_number: str
    heading: str | None
    effective_notes: tuple[str, ...]
    status: str | None
    range_end: str | None = None
    related_sections: tuple[str, ...] = ()


def _append_sections_from_links(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    seen: set[str],
    *,
    fetcher: _RhodeIslandFetcher,
    store: CorpusArtifactStore,
    jurisdiction: str,
    run_id: str,
    source_paths: list[Path],
    source_by_relative: dict[str, _RecordedSource],
    section_links: Sequence[RhodeIslandContainerLink],
    source_year: int,
    source_as_of: str,
    expression_date: str,
    remaining_sections: int | None,
    workers: int,
) -> tuple[int | None, int]:
    links = list(section_links)
    if remaining_sections is not None:
        links = links[: max(0, remaining_sections)]
    pages = _fetch_pages(fetcher, [link.relative_path for link in links], workers=workers)
    written = 0
    for link, page in zip(links, pages, strict=True):
        source = _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=page,
            source_paths=source_paths,
            source_by_relative=source_by_relative,
        )
        parsed_sections = parse_rhode_island_section_html(
            page.data,
            parent_citation_path=link.parent_citation_path,
            level=link.level,
            ordinal_start=link.ordinal,
            fallback_source_id=link.source_id,
        )
        for section in parsed_sections:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            if section.citation_path in seen:
                continue
            seen.add(section.citation_path)
            written += 1
            _append_section(
                items,
                records,
                section,
                source=source,
                version=run_id,
                source_as_of=source_as_of,
                expression_date=expression_date,
                source_year=source_year,
            )
            if remaining_sections is not None:
                remaining_sections -= 1
    return remaining_sections, written


def _append_container(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    provision: RhodeIslandContainerLink,
    *,
    source: _RecordedSource,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> None:
    _append_inventory_and_record(
        items,
        records,
        citation_path=provision.citation_path,
        version=version,
        source=source,
        source_id=provision.source_id,
        source_as_of=source_as_of,
        expression_date=expression_date,
        kind=provision.kind,
        heading=provision.heading,
        legal_identifier=provision.legal_identifier,
        parent_citation_path=provision.parent_citation_path,
        level=provision.level,
        ordinal=provision.ordinal,
        identifiers=_container_identifiers(provision),
        metadata=_container_metadata(provision),
    )


def _append_section(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    section: RhodeIslandSection,
    *,
    source: _RecordedSource,
    version: str,
    source_as_of: str,
    expression_date: str,
    source_year: int,
) -> None:
    _append_inventory_and_record(
        items,
        records,
        citation_path=section.citation_path,
        version=version,
        source=source,
        source_id=section.source_id,
        source_as_of=source_as_of,
        expression_date=expression_date,
        kind="section",
        heading=section.heading,
        body=section.body,
        legal_identifier=section.legal_identifier,
        parent_citation_path=section.parent_citation_path,
        level=section.level,
        ordinal=section.ordinal,
        identifiers=_section_identifiers(section),
        metadata=_section_metadata(section, source_year=source_year),
    )


def _append_inventory_and_record(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    *,
    citation_path: str,
    version: str,
    source: _RecordedSource,
    source_id: str,
    source_as_of: str,
    expression_date: str,
    kind: str,
    heading: str | None,
    legal_identifier: str,
    level: int,
    ordinal: int | None,
    identifiers: dict[str, str],
    metadata: dict[str, Any],
    body: str | None = None,
    parent_citation_path: str | None = None,
) -> None:
    clean_metadata = {key: value for key, value in metadata.items() if value not in (None, [], ())}
    if parent_citation_path is not None:
        clean_metadata["parent_citation_path"] = parent_citation_path
    items.append(
        SourceInventoryItem(
            citation_path=citation_path,
            source_url=source.source_url,
            source_path=source.source_path,
            source_format=RHODE_ISLAND_GENERAL_LAWS_SOURCE_FORMAT,
            sha256=source.sha256,
            metadata=clean_metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(citation_path),
            jurisdiction="us-ri",
            document_class=DocumentClass.STATUTE.value,
            citation_path=citation_path,
            body=body,
            heading=heading,
            citation_label=legal_identifier,
            version=version,
            source_url=source.source_url,
            source_path=source.source_path,
            source_id=source_id,
            source_format=RHODE_ISLAND_GENERAL_LAWS_SOURCE_FORMAT,
            source_as_of=source_as_of,
            expression_date=expression_date,
            parent_citation_path=parent_citation_path,
            parent_id=(
                deterministic_provision_id(parent_citation_path)
                if parent_citation_path is not None
                else None
            ),
            level=level,
            ordinal=ordinal,
            kind=kind,
            legal_identifier=legal_identifier,
            identifiers=identifiers,
            metadata=clean_metadata,
        )
    )


def _record_source_page(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    page: _RhodeIslandSourcePage,
    source_paths: list[Path],
    source_by_relative: dict[str, _RecordedSource],
) -> _RecordedSource:
    artifact_relative = f"{RHODE_ISLAND_GENERAL_LAWS_SOURCE_FORMAT}/{page.relative_path}"
    artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        artifact_relative,
    )
    sha256 = store.write_bytes(artifact_path, page.data)
    if artifact_path not in source_paths:
        source_paths.append(artifact_path)
    source = _RecordedSource(
        source_url=page.source_url,
        source_path=_state_source_key(jurisdiction, run_id, artifact_relative),
        sha256=sha256,
    )
    source_by_relative[page.relative_path] = source
    return source


def _fetch_pages(
    fetcher: _RhodeIslandFetcher,
    relative_paths: Sequence[str],
    *,
    workers: int,
) -> tuple[_RhodeIslandSourcePage, ...]:
    if not relative_paths:
        return ()
    if workers <= 1 or len(relative_paths) == 1:
        return tuple(fetcher.fetch(path) for path in relative_paths)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        return tuple(executor.map(fetcher.fetch, relative_paths))


def _container_from_link(
    link: RhodeIslandContainerLink,
    *,
    page: RhodeIslandIndexPage,
    source_year: int,
) -> RhodeIslandContainerLink:
    metadata_status = page.status or link.status
    return RhodeIslandContainerLink(
        kind=link.kind,
        source_id=link.source_id,
        display_number=link.display_number,
        heading=page.heading or link.heading,
        relative_path=link.relative_path,
        ordinal=link.ordinal,
        parent_citation_path=link.parent_citation_path,
        level=link.level,
        title=link.title,
        chapter=link.chapter,
        effective_notes=tuple(dict.fromkeys(link.effective_notes + page.effective_notes)),
        status=metadata_status,
        source_year=source_year,
    )


def _container_identifiers(provision: RhodeIslandContainerLink) -> dict[str, str]:
    identifiers = {
        "rhode_island:kind": provision.kind,
        "rhode_island:source_id": provision.source_id,
        f"rhode_island:{provision.kind}": provision.display_number,
    }
    if provision.title:
        identifiers["rhode_island:title"] = provision.title
    if provision.chapter:
        identifiers["rhode_island:chapter"] = provision.chapter
    return identifiers


def _container_metadata(provision: RhodeIslandContainerLink) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "display_number": provision.display_number,
        "title": provision.title,
        "chapter": provision.chapter,
    }
    if provision.effective_notes:
        metadata["effective_notes"] = list(provision.effective_notes)
    if provision.status:
        metadata["status"] = provision.status
    if provision.source_year is not None:
        metadata["source_year"] = provision.source_year
    return metadata


def _section_identifiers(section: RhodeIslandSection) -> dict[str, str]:
    identifiers = {
        "rhode_island:section": section.section,
        "rhode_island:source_id": section.source_id,
    }
    if section.title:
        identifiers["rhode_island:title"] = section.title
    if section.chapter:
        identifiers["rhode_island:chapter"] = section.chapter
    if section.variant:
        identifiers["rhode_island:variant"] = section.variant
    return identifiers


def _section_metadata(section: RhodeIslandSection, *, source_year: int) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": section.title,
        "chapter": section.chapter,
        "section": section.section,
        "display_number": section.display_number,
        "source_year": source_year,
    }
    if section.parent_citation_path:
        metadata["parent_citation_path"] = section.parent_citation_path
    if section.range_end:
        metadata["range_end"] = section.range_end
    if section.related_sections:
        metadata["related_sections"] = list(section.related_sections)
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.effective_notes:
        metadata["effective_notes"] = list(section.effective_notes)
    if section.status:
        metadata["status"] = section.status
    if section.variant:
        metadata["variant"] = section.variant
        metadata["canonical_citation_path"] = section.canonical_citation_path
    return metadata


def _section_blocks(soup: BeautifulSoup) -> tuple[Tag, ...]:
    blocks: list[Tag] = []
    seen: set[int] = set()
    for bold in soup.find_all("b"):
        if not isinstance(bold, Tag):
            continue
        if _parse_section_caption(_clean_text(bold.get_text(" ", strip=True))).source_id is None:
            continue
        block = bold.find_parent("div")
        if not isinstance(block, Tag):
            continue
        marker = id(block)
        if marker in seen:
            continue
        seen.add(marker)
        blocks.append(block)
    return tuple(blocks)


def _section_heading_tag(block: Tag) -> Tag | None:
    for bold in block.find_all("b"):
        if not isinstance(bold, Tag):
            continue
        if _parse_section_caption(_clean_text(bold.get_text(" ", strip=True))).source_id:
            return bold
    return None


def _section_body_history_references(
    block: Tag,
    heading_tag: Tag,
) -> tuple[list[str], tuple[str, ...], tuple[str, ...]]:
    body_lines: list[str] = []
    history: list[str] = []
    references: list[str] = []
    heading_parent = heading_tag.find_parent("p")
    for paragraph in block.find_all("p", recursive=False):
        if paragraph is heading_parent:
            continue
        text = _clean_text(paragraph.get_text(" ", strip=True))
        if not text:
            continue
        body_lines.append(text)
        references.extend(_references_from_tag(paragraph))
        references.extend(_text_references(text))
    for nested in block.find_all("div", recursive=False):
        text = _clean_text(nested.get_text(" ", strip=True))
        if not text:
            continue
        text = re.sub(r"^History of Section\.\s*", "", text, flags=re.I).strip()
        if text:
            history.append(text)
            references.extend(_references_from_tag(nested))
            references.extend(_text_references(text))
    return body_lines, tuple(dict.fromkeys(history)), tuple(dict.fromkeys(references))


def _references_from_tag(root: Tag) -> tuple[str, ...]:
    refs: list[str] = []
    for anchor in root.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = str(anchor.get("href") or "")
        source_id = _section_from_relative(_normalize_relative_path(href))
        if source_id:
            refs.append(f"us-ri/statute/{source_id}")
    return tuple(dict.fromkeys(refs))


def _text_references(text: str) -> tuple[str, ...]:
    refs = [f"us-ri/statute/{match.group('cite')}" for match in _TEXT_REFERENCE_RE.finditer(text)]
    return tuple(dict.fromkeys(refs))


def _parse_section_link(text: str, *, relative_path: str) -> _ParsedCaption:
    parsed = _parse_section_caption(text)
    if parsed.source_id:
        return parsed
    source_id = _section_from_relative(relative_path)
    display = source_id or Path(relative_path).stem
    return _ParsedCaption(
        kind="section",
        source_id=source_id,
        display_number=display,
        heading=None,
        effective_notes=(),
        status=None,
    )


def _parse_section_caption(text: str) -> _ParsedCaption:
    match = _SECTION_CAPTION_RE.match(_clean_text(text))
    if not match:
        return _ParsedCaption(
            kind="section",
            source_id=None,
            display_number="",
            heading=None,
            effective_notes=(),
            status=None,
        )
    numbers = _normalize_display_number(match.group("numbers"))
    cites = _SECTION_CITE_RE.findall(numbers)
    heading = _clean_heading(match.group("heading"))
    effective_notes = _effective_notes(heading or "")
    return _ParsedCaption(
        kind="section",
        source_id=cites[0] if cites else None,
        display_number=numbers,
        heading=heading,
        effective_notes=effective_notes,
        status=_status(heading, None, (), effective_notes=effective_notes),
        range_end=cites[-1] if _has_range_separator(numbers) and len(cites) > 1 else None,
        related_sections=tuple(cites[1:]) if "," in numbers and len(cites) > 1 else (),
    )


def _parse_container_caption(
    text: str,
    *,
    fallback_kind: str,
    fallback_number: str,
) -> _ParsedCaption:
    cleaned = _clean_text(text)
    match = re.match(
        r"^(?P<kind>Chapter|Chapters|Part|Article)\s+"
        r"(?P<number>[0-9A-Za-z.]+(?:\s*(?:\u2014|\u2013|-|to)\s*[0-9A-Za-z.]+)?)"
        r"\s*(?P<heading>.*)$",
        cleaned,
        re.I,
    )
    if match:
        kind = match.group("kind").lower()
        if kind == "chapters":
            kind = "chapter"
        display_number = _normalize_display_number(match.group("number"))
        heading = _clean_heading(match.group("heading"))
    else:
        kind = fallback_kind
        display_number = fallback_number
        heading = _clean_heading(cleaned) if cleaned else None
    effective_notes = _effective_notes(heading or "")
    return _ParsedCaption(
        kind=kind,
        source_id=None,
        display_number=display_number,
        heading=heading,
        effective_notes=effective_notes,
        status=_status(heading, None, (), effective_notes=effective_notes),
    )


def _page_heading(soup: BeautifulSoup, kind: str) -> str | None:
    tag_name = {"title": "h1", "chapter": "h2", "part": "h3", "article": "h3"}.get(kind, "h3")
    for tag in soup.find_all(tag_name):
        if not isinstance(tag, Tag):
            continue
        text = _clean_text(tag.get_text(" ", strip=True))
        if not text or text.lower().startswith("index of"):
            continue
        if text.lower().startswith("r.i. gen. laws"):
            continue
        parsed = _parse_container_caption(
            text,
            fallback_kind=kind,
            fallback_number="",
        )
        if kind == "title":
            match = re.match(r"^Title\s+[0-9A-Z.]+\s+(?P<heading>.+)$", text, re.I)
            return _clean_heading(match.group("heading")) if match else text
        return parsed.heading or text
    return None


def _source_history(soup: BeautifulSoup) -> tuple[str, ...]:
    history: list[str] = []
    for paragraph in soup.find_all("p"):
        if not isinstance(paragraph, Tag):
            continue
        text = _clean_text(paragraph.get_text(" ", strip=True))
        if not text.lower().startswith("history of section."):
            continue
        cleaned = re.sub(r"^History of Section\.\s*", "", text, flags=re.I).strip()
        if cleaned:
            history.append(cleaned)
    return tuple(dict.fromkeys(history))


def _index_links(soup: BeautifulSoup) -> tuple[Tag, ...]:
    links: list[Tag] = []
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = str(anchor.get("href") or "")
        lowered = href.lower()
        if lowered.endswith(".htm") or lowered.endswith("/index.htm"):
            links.append(anchor)
    return tuple(links)


def _effective_notes(text: str) -> tuple[str, ...]:
    notes: list[str] = []
    for match in _EFFECTIVE_NOTE_RE.finditer(text):
        value = match.group("bracket") or match.group("paren") or ""
        value = value.strip("[]() ")
        value = _clean_text(value)
        if value:
            notes.append(value)
    return tuple(dict.fromkeys(notes))


def _status(
    heading: str | None,
    body: str | None,
    history: tuple[str, ...],
    *,
    effective_notes: tuple[str, ...] = (),
) -> str | None:
    status_text = " ".join(part for part in (heading, body, " ".join(history)) if part).lower()
    if "repealed" in status_text:
        return "repealed"
    if "reserved" in status_text:
        return "reserved"
    if "obsolete" in status_text:
        return "obsolete"
    if "superseded" in status_text:
        return "superseded"
    if effective_notes:
        normalized = " ".join(effective_notes).lower()
        if "until" in normalized or "through" in normalized:
            return "effective_until"
        return "future_or_conditional"
    return None


def _variant_for_occurrence(effective_notes: tuple[str, ...], occurrence: int) -> str | None:
    if occurrence == 1:
        return None
    for note in effective_notes:
        date_match = _EFFECTIVE_DATE_RE.search(note)
        if not date_match:
            continue
        try:
            value = datetime.strptime(date_match.group(1), "%B %d, %Y").date()
        except ValueError:
            continue
        return f"effective-{value.isoformat()}"
    return f"variant-{occurrence}"


def _disambiguated_variant(
    variant: str | None,
    *,
    occurrence: int,
    section_number: str,
    used_source_ids: set[str],
) -> str:
    root = variant or "variant"
    suffix = occurrence
    while True:
        candidate = f"{root}-{suffix}"
        if _section_source_id(section_number, candidate) not in used_source_ids:
            return candidate
        suffix += 1


def _section_source_id(section_number: str, variant: str | None) -> str:
    return section_number if variant is None else f"{section_number}@{variant}"


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    text = re.sub(r"^TITLE\s*", "", text, flags=re.I)
    if not re.fullmatch(r"\d+[A-Z]?(?:\.\d+)?", text):
        raise ValueError(f"invalid Rhode Island title filter: {value!r}")
    return text


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    text = re.sub(r"^CHAPTER\s*", "", text, flags=re.I)
    if not re.fullmatch(r"\d+[A-Z]?(?:\.\d+)?-\d+[A-Z]?(?:\.\d+)?", text):
        raise ValueError(f"invalid Rhode Island chapter filter: {value!r}")
    return text


def _rhode_island_run_id(
    version: str,
    *,
    title_filter: str | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and chapter_filter is None and limit is None:
        return version
    parts = [version, "us-ri"]
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter}")
    elif title_filter is not None:
        parts.append(f"title-{title_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _title_from_relative(relative_path: str) -> str | None:
    match = _TITLE_PATH_RE.search(relative_path)
    return match.group("title").upper() if match else None


def _chapter_from_relative(relative_path: str) -> str | None:
    match = _CHAPTER_PATH_RE.search(relative_path)
    return match.group("chapter").upper() if match else None


def _section_from_relative(relative_path: str) -> str | None:
    stem = Path(relative_path).stem.upper()
    first = stem.split("_", 1)[0]
    if re.fullmatch(_SECTION_CITE_PATTERN, first):
        return first
    return None


def _title_from_section(section: str) -> str | None:
    parts = section.split("-", 2)
    return parts[0] if len(parts) == 3 else None


def _chapter_from_section(section: str) -> str | None:
    parts = section.split("-", 2)
    if len(parts) < 2:
        return None
    return "-".join(parts[:2])


def _title_from_chapter(chapter: str) -> str:
    return chapter.split("-", 1)[0]


def _child_container_source_id(parent_source_id: str, kind: str, display_number: str) -> str:
    return f"{parent_source_id}/{kind}-{_slug(display_number, fallback='container')}"


def _strip_leading_token(text: str, token: str) -> str | None:
    cleaned = _clean_text(text)
    if cleaned.startswith(token):
        cleaned = cleaned[len(token) :].strip()
    return cleaned or None


def _normalize_display_number(value: str) -> str:
    text = _clean_text(value)
    text = re.sub(r"\s*(?:\u2014|\u2013|--| to )\s*", " - ", text, flags=re.I)
    text = re.sub(r"\s*,\s*", ", ", text)
    return text.strip()


def _has_range_separator(value: str) -> bool:
    return bool(re.search(r"\s-\s|\bto\b", value, re.I))


def _clean_heading(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = _clean_text(value).strip(" .")
    return cleaned or None


def _clean_text(value: str) -> str:
    text = value.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _slug(value: str, *, fallback: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or fallback


def _resolve_relative(current_relative: str, href: str) -> str:
    return _normalize_relative_path(urljoin(current_relative, href))


def _normalize_relative_path(value: str) -> str:
    parsed = urlparse(value)
    path = parsed.path if parsed.scheme or parsed.netloc else value
    path = unquote(path).replace("\\", "/")
    path = re.sub(r"/+", "/", path)
    marker = "/statutes/"
    lower_path = path.lower()
    if marker in lower_path:
        index = lower_path.index(marker) + len(marker)
        path = path[index:]
    path = path.lstrip("/")
    if not path:
        path = RHODE_ISLAND_GENERAL_LAWS_INDEX
    return path


def _base_url(value: str) -> str:
    return value.rstrip("/") + "/"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _read_source_file(source_dir: Path, relative_path: str) -> bytes:
    candidates = (
        source_dir / relative_path,
        source_dir / RHODE_ISLAND_GENERAL_LAWS_SOURCE_FORMAT / relative_path,
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate.read_bytes()
    raise FileNotFoundError(relative_path)


def _download_rhode_island_page(source_url: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            response = requests.get(
                source_url,
                headers={"User-Agent": RHODE_ISLAND_USER_AGENT},
                timeout=60,
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 3:
                break
            time.sleep(0.5 * 2**attempt)
    raise ValueError(f"failed to fetch Rhode Island source page {source_url}: {last_error}")


def _write_cache_bytes(path: Path, data: bytes) -> None:
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)
