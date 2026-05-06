"""Delaware Code source-first corpus adapter."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

DELAWARE_CODE_BASE_URL = "https://www.delcode.delaware.gov/"
DELAWARE_CODE_SOURCE_FORMAT = "delaware-code-html"
DELAWARE_USER_AGENT = "axiom-corpus/0.1 (Delaware statute ingestion; contact@axiom-foundation.org)"

_TITLE_HREF_RE = re.compile(r"(?:^|/)title(?P<title>\d+)/index\.html$", re.I)
_CHAPTER_HREF_RE = re.compile(r"(?:^|/)c(?P<chapter>\d+[A-Za-z]?)/index\.html$", re.I)
_SUBCHAPTER_HREF_RE = re.compile(r"(?:^|/)sc(?P<subchapter>\d+[A-Za-z]?)/index\.html$", re.I)
_TITLE_TEXT_RE = re.compile(r"Title\s+(?P<title>\d+)\s*[-.]\s*(?P<heading>.+)", re.I)
_CHAPTER_TEXT_RE = re.compile(r"Chapter\s+(?P<chapter>\d+[A-Za-z]?)\.\s*(?P<heading>.+)", re.I)
_SUBCHAPTER_TEXT_RE = re.compile(
    r"Subchapter\s+(?P<subchapter>[IVXLCDM0-9A-Za-z]+)\.\s*(?P<heading>.+)",
    re.I,
)
_PART_TEXT_RE = re.compile(r"Part\s+(?P<part>[IVXLCDM0-9A-Za-z]+)\.?", re.I)
_SECTION_SYMBOL_RE = re.compile(
    r"§+\s*(?P<section>[0-9A-Za-z][0-9A-Za-z.-]*(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?)"
)
_SECTION_OF_TITLE_RE = re.compile(
    r"§+\s*(?P<section>[0-9A-Za-z][0-9A-Za-z.-]*(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?)"
    r"\s+of\s+Title\s+(?P<title>\d+)",
    re.I,
)
_BRACKET_NOTE_RE = re.compile(r"\[(?P<note>[^\]]+)\]")


@dataclass(frozen=True)
class DelawareCodeProvision:
    """Parsed Delaware Code title/part/chapter/subchapter/section node."""

    kind: str
    title: str
    source_id: str
    display_number: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int | None
    source_url: str
    source_relative_path: str
    source_path: str
    sha256: str
    chapter: str | None = None
    part: str | None = None
    subchapter: str | None = None
    references_to: tuple[str, ...] = ()
    source_history: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    status: str | None = None
    canonical_citation_path: str | None = None
    variant: str | None = None

    @property
    def citation_path(self) -> str:
        if self.kind == "title":
            return f"us-de/statute/{self.title}"
        if self.kind == "part":
            return f"us-de/statute/{self.title}/{self.source_id}"
        if self.kind == "chapter":
            return f"us-de/statute/{self.title}/{self.chapter}"
        if self.kind == "subchapter":
            return f"us-de/statute/{self.title}/{self.chapter}/{self.source_id}"
        return f"us-de/statute/{self.title}/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        if self.kind == "title":
            return f"Del. Code tit. {self.title}"
        if self.kind == "part":
            return f"{self.title} Del. C. Part {self.display_number}"
        if self.kind == "chapter":
            return f"{self.title} Del. C. ch. {self.display_number}"
        if self.kind == "subchapter":
            return f"{self.title} Del. C. ch. {self.chapter}, subch. {self.display_number}"
        symbol = "§§" if re.fullmatch(r"\d+-\d+", self.display_number) else "§"
        return f"{self.title} Del. C. {symbol} {self.display_number}"


@dataclass(frozen=True)
class DelawareTitleLink:
    title: str
    heading: str
    relative_path: str
    source_url: str
    ordinal: int


@dataclass(frozen=True)
class DelawarePartLink:
    title: str
    source_id: str
    display_number: str
    heading: str | None
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-de/statute/{self.title}/{self.source_id}"


@dataclass(frozen=True)
class DelawareChapterLink:
    title: str
    chapter: str
    heading: str
    relative_path: str
    source_url: str
    ordinal: int
    part_source_id: str | None = None

    @property
    def citation_path(self) -> str:
        return f"us-de/statute/{self.title}/{self.chapter}"

    @property
    def parent_citation_path(self) -> str:
        if self.part_source_id is not None:
            return f"us-de/statute/{self.title}/{self.part_source_id}"
        return f"us-de/statute/{self.title}"


@dataclass(frozen=True)
class DelawareSubchapterLink:
    title: str
    chapter: str
    source_id: str
    display_number: str
    heading: str
    relative_path: str
    source_url: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-de/statute/{self.title}/{self.chapter}/{self.source_id}"


@dataclass(frozen=True)
class DelawareTitleParse:
    parts: tuple[DelawarePartLink, ...]
    chapters: tuple[DelawareChapterLink, ...]


@dataclass(frozen=True)
class DelawareChapterParse:
    subchapters: tuple[DelawareSubchapterLink, ...]
    sections: tuple[DelawareCodeProvision, ...]
    heading: str | None = None


@dataclass(frozen=True)
class _DelawareSourcePage:
    relative_path: str
    source_url: str
    data: bytes


class _DelawareFetcher:
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

    def fetch(self, relative_path: str) -> _DelawareSourcePage:
        normalized = _normalize_relative_path(relative_path)
        source_url = urljoin(self.base_url, normalized)
        if self.source_dir is not None:
            path = self.source_dir / normalized
            if not path.exists():
                raise FileNotFoundError(normalized)
            return _DelawareSourcePage(
                relative_path=normalized,
                source_url=source_url,
                data=path.read_bytes(),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            if cached_path.exists():
                return _DelawareSourcePage(
                    relative_path=normalized,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )

        data, resolved_url = _download_delaware_page(source_url)
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            _write_cache_bytes(cached_path, data)
        return _DelawareSourcePage(
            relative_path=normalized,
            source_url=resolved_url,
            data=data,
        )


def extract_delaware_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    only_chapter: str | int | None = None,
    limit: int | None = None,
    workers: int = 1,
    download_dir: str | Path | None = None,
    base_url: str = DELAWARE_CODE_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official Delaware Code HTML and extract normalized provisions."""
    _ = workers
    jurisdiction = "us-de"
    title_filter = _title_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter)
    run_id = _delaware_run_id(
        version,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _DelawareFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
    )

    source_paths: list[Path] = []
    source_pages: dict[str, tuple[str, str, str]] = {}
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    errors: list[str] = []
    skipped_source_count = 0
    title_count = 0
    container_count = 0
    section_count = 0
    remaining_sections = limit
    occurrence_by_section: dict[tuple[str, str], int] = {}

    root_page = fetcher.fetch("index.html")
    _record_source_page(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        page=root_page,
        source_paths=source_paths,
        source_pages=source_pages,
    )
    title_links = list(parse_delaware_code_index(root_page.data, base_url=fetcher.base_url))
    if title_filter is not None:
        title_links = [link for link in title_links if link.title == title_filter]
    if not title_links:
        raise ValueError(f"no Delaware Code titles selected for filter: {only_title!r}")

    for title_link in title_links:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        title_page = fetcher.fetch(title_link.relative_path)
        _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=title_page,
            source_paths=source_paths,
            source_pages=source_pages,
        )
        title_source_path, title_sha256, title_source_url = source_pages[title_page.relative_path]
        parsed_title = parse_delaware_title_html(
            title_page.data,
            title=title_link.title,
            current_relative_path=title_page.relative_path,
            base_url=fetcher.base_url,
        )
        selected_chapters = [
            chapter
            for chapter in parsed_title.chapters
            if chapter_filter is None or chapter.chapter == chapter_filter
        ]
        if not selected_chapters:
            errors.append(
                f"title {title_link.title}: no chapters selected for filter {only_chapter!r}"
            )
            continue

        title_heading = _title_heading(title_page.data) or title_link.heading
        title_provision = DelawareCodeProvision(
            kind="title",
            title=title_link.title,
            source_id=title_link.title,
            display_number=title_link.title,
            heading=title_heading,
            body=None,
            parent_citation_path=None,
            level=0,
            ordinal=title_link.ordinal,
            source_url=title_source_url,
            source_relative_path=title_page.relative_path,
            source_path=title_source_path,
            sha256=title_sha256,
        )
        if _append_provision(
            items,
            records,
            title_provision,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            seen=seen,
        ):
            title_count += 1
            container_count += 1

        part_by_source_id = {part.source_id: part for part in parsed_title.parts}
        appended_parts: set[str] = set()
        for chapter in selected_chapters:
            if chapter.part_source_id is not None and chapter.part_source_id not in appended_parts:
                part = part_by_source_id.get(chapter.part_source_id)
                if part is not None:
                    part_provision = _part_provision(
                        part,
                        source_url=title_source_url,
                        source_relative_path=title_page.relative_path,
                        source_path=title_source_path,
                        sha256=title_sha256,
                    )
                    if _append_provision(
                        items,
                        records,
                        part_provision,
                        version=run_id,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                        seen=seen,
                    ):
                        container_count += 1
                appended_parts.add(chapter.part_source_id)

            try:
                chapter_page = fetcher.fetch(chapter.relative_path)
            except OSError as exc:
                skipped_source_count += 1
                errors.append(f"{chapter.relative_path}: {exc}")
                continue
            _record_source_page(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                page=chapter_page,
                source_paths=source_paths,
                source_pages=source_pages,
            )
            chapter_source_path, chapter_sha256, chapter_source_url = source_pages[
                chapter_page.relative_path
            ]
            parsed_chapter = parse_delaware_chapter_html(
                chapter_page.data,
                title=chapter.title,
                chapter=chapter.chapter,
                current_relative_path=chapter_page.relative_path,
                base_url=fetcher.base_url,
                source_url=chapter_source_url,
                source_path=chapter_source_path,
                sha256=chapter_sha256,
                occurrence_by_section=occurrence_by_section,
            )
            chapter_provision = _chapter_provision(
                chapter,
                heading=parsed_chapter.heading or chapter.heading,
                source_url=chapter_source_url,
                source_relative_path=chapter_page.relative_path,
                source_path=chapter_source_path,
                sha256=chapter_sha256,
            )
            if _append_provision(
                items,
                records,
                chapter_provision,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                seen=seen,
            ):
                container_count += 1

            if parsed_chapter.subchapters:
                for subchapter in parsed_chapter.subchapters:
                    if remaining_sections is not None and remaining_sections <= 0:
                        break
                    try:
                        subchapter_page = fetcher.fetch(subchapter.relative_path)
                    except OSError as exc:
                        skipped_source_count += 1
                        errors.append(f"{subchapter.relative_path}: {exc}")
                        continue
                    _record_source_page(
                        store,
                        jurisdiction=jurisdiction,
                        run_id=run_id,
                        page=subchapter_page,
                        source_paths=source_paths,
                        source_pages=source_pages,
                    )
                    sub_source_path, sub_sha256, sub_source_url = source_pages[
                        subchapter_page.relative_path
                    ]
                    subchapter_provision = _subchapter_provision(
                        subchapter,
                        source_url=sub_source_url,
                        source_relative_path=subchapter_page.relative_path,
                        source_path=sub_source_path,
                        sha256=sub_sha256,
                    )
                    if _append_provision(
                        items,
                        records,
                        subchapter_provision,
                        version=run_id,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                        seen=seen,
                    ):
                        container_count += 1
                    parsed_subchapter = parse_delaware_chapter_html(
                        subchapter_page.data,
                        title=subchapter.title,
                        chapter=subchapter.chapter,
                        current_relative_path=subchapter_page.relative_path,
                        base_url=fetcher.base_url,
                        source_url=sub_source_url,
                        source_path=sub_source_path,
                        sha256=sub_sha256,
                        parent_citation_path=subchapter.citation_path,
                        subchapter=subchapter.source_id,
                        occurrence_by_section=occurrence_by_section,
                    )
                    for section in parsed_subchapter.sections:
                        if remaining_sections is not None and remaining_sections <= 0:
                            break
                        if _append_provision(
                            items,
                            records,
                            section,
                            version=run_id,
                            source_as_of=source_as_of_text,
                            expression_date=expression_date_text,
                            seen=seen,
                        ):
                            section_count += 1
                            if remaining_sections is not None:
                                remaining_sections -= 1
                continue

            for section in parsed_chapter.sections:
                if remaining_sections is not None and remaining_sections <= 0:
                    break
                if _append_provision(
                    items,
                    records,
                    section,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    seen=seen,
                ):
                    section_count += 1
                    if remaining_sections is not None:
                        remaining_sections -= 1

    if not items:
        raise ValueError("no Delaware Code provisions extracted")

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
        skipped_source_count=skipped_source_count,
        errors=tuple(errors),
    )


def parse_delaware_code_index(
    html: str | bytes,
    *,
    base_url: str = DELAWARE_CODE_BASE_URL,
) -> tuple[DelawareTitleLink, ...]:
    """Parse the official Delaware Code title index page."""
    soup = BeautifulSoup(_decode_html(html), "lxml")
    links: list[DelawareTitleLink] = []
    for anchor in soup.select(".title-links a[href]"):
        if not isinstance(anchor, Tag):
            continue
        href = _clean_href(anchor.get("href"))
        match = _TITLE_HREF_RE.search(href)
        if not match:
            continue
        title = str(int(match.group("title")))
        text = _clean_text(anchor.get_text(" ", strip=True))
        title_match = _TITLE_TEXT_RE.search(text)
        heading = _clean_text(title_match.group("heading")) if title_match else text
        links.append(
            DelawareTitleLink(
                title=title,
                heading=heading,
                relative_path=_normalize_relative_path(f"title{title}/index.html"),
                source_url=urljoin(_base_url(base_url), f"title{title}/index.html"),
                ordinal=len(links),
            )
        )
    return tuple(links)


def parse_delaware_title_html(
    html: str | bytes,
    *,
    title: str,
    current_relative_path: str,
    base_url: str = DELAWARE_CODE_BASE_URL,
) -> DelawareTitleParse:
    """Parse one official Delaware Code title table of contents page."""
    soup = BeautifulSoup(_decode_html(html), "lxml")
    content = soup.find(id="content") or soup
    parts: list[DelawarePartLink] = []
    chapters: list[DelawareChapterLink] = []
    current_part: DelawarePartLink | None = None
    for container in content.find_all("div", recursive=False):
        if not isinstance(container, Tag):
            continue
        direct_h3s = [
            _clean_text(tag.get_text(" ", strip=True))
            for tag in container.find_all("h3", recursive=False)
            if isinstance(tag, Tag)
        ]
        if direct_h3s:
            part_candidate = _part_from_h3s(title, direct_h3s, ordinal=len(parts))
            if part_candidate is not None:
                current_part = part_candidate
                parts.append(part_candidate)
        for anchor in container.select(".title-links a[href]"):
            if not isinstance(anchor, Tag):
                continue
            chapter = _chapter_link_from_anchor(
                anchor,
                title=title,
                current_relative_path=current_relative_path,
                base_url=base_url,
                ordinal=len(chapters),
                part_source_id=current_part.source_id if current_part else None,
            )
            if chapter is not None:
                chapters.append(chapter)
    if not chapters:
        for anchor in content.select(".title-links a[href]"):
            if not isinstance(anchor, Tag):
                continue
            chapter = _chapter_link_from_anchor(
                anchor,
                title=title,
                current_relative_path=current_relative_path,
                base_url=base_url,
                ordinal=len(chapters),
                part_source_id=None,
            )
            if chapter is not None:
                chapters.append(chapter)
    return DelawareTitleParse(parts=tuple(parts), chapters=tuple(chapters))


def parse_delaware_chapter_html(
    html: str | bytes,
    *,
    title: str,
    chapter: str,
    current_relative_path: str,
    base_url: str = DELAWARE_CODE_BASE_URL,
    source_url: str | None = None,
    source_path: str | None = None,
    sha256: str | None = None,
    parent_citation_path: str | None = None,
    subchapter: str | None = None,
    occurrence_by_section: dict[tuple[str, str], int] | None = None,
) -> DelawareChapterParse:
    """Parse one official Delaware Code chapter or subchapter HTML page."""
    soup = BeautifulSoup(_decode_html(html), "lxml")
    subchapters = tuple(
        _subchapter_link_from_anchor(
            anchor,
            title=title,
            chapter=chapter,
            current_relative_path=current_relative_path,
            base_url=base_url,
            ordinal=index,
        )
        for index, anchor in enumerate(soup.select(".title-links a[href]"))
        if isinstance(anchor, Tag) and _SUBCHAPTER_HREF_RE.search(_clean_href(anchor.get("href")))
    )
    section_parent = parent_citation_path or f"us-de/statute/{title}/{chapter}"
    occurrences = occurrence_by_section if occurrence_by_section is not None else {}
    sections = tuple(
        section
        for index, section_tag in enumerate(soup.select("div.Section"))
        if isinstance(section_tag, Tag)
        for section in (
            _section_from_tag(
                section_tag,
                title=title,
                chapter=chapter,
                subchapter=subchapter,
                parent_citation_path=section_parent,
                level=3 if subchapter else 2,
                ordinal=index,
                current_relative_path=current_relative_path,
                base_url=base_url,
                source_url=source_url or urljoin(_base_url(base_url), current_relative_path),
                source_path=source_path
                or _state_source_key("us-de", "unknown", f"{DELAWARE_CODE_SOURCE_FORMAT}/{current_relative_path}"),
                sha256=sha256 or "",
                occurrence_by_section=occurrences,
            ),
        )
    )
    return DelawareChapterParse(
        subchapters=tuple(item for item in subchapters if item is not None),
        sections=sections,
        heading=_chapter_heading(soup),
    )


def _section_from_tag(
    section_tag: Tag,
    *,
    title: str,
    chapter: str,
    subchapter: str | None,
    parent_citation_path: str,
    level: int,
    ordinal: int,
    current_relative_path: str,
    base_url: str,
    source_url: str,
    source_path: str,
    sha256: str,
    occurrence_by_section: dict[tuple[str, str], int],
) -> DelawareCodeProvision:
    head = section_tag.find("div", class_="SectionHead")
    if not isinstance(head, Tag):
        raise ValueError("Delaware section is missing SectionHead")
    raw_section_id = _clean_text(str(head.get("id") or ""))
    heading_text = _clean_text(head.get_text(" ", strip=True))
    section_id = raw_section_id or _section_id_from_heading(heading_text)
    heading = _section_heading_text(heading_text, section_id)
    body_lines, references = _section_body_and_references(section_tag, title=title)
    body = "\n".join(body_lines).strip() or None
    history = _section_history(section_tag)
    notes = _heading_notes(heading)
    status = _status(heading, body, history)
    occurrence_key = (title, section_id)
    occurrence_by_section[occurrence_key] = occurrence_by_section.get(occurrence_key, 0) + 1
    occurrence = occurrence_by_section[occurrence_key]
    variant = _variant_for_section(heading, occurrence)
    source_id = section_id if variant is None else f"{section_id}@{variant}"
    citation_path = f"us-de/statute/{title}/{source_id}"
    canonical = f"us-de/statute/{title}/{section_id}" if variant is not None else None
    references_to = tuple(dict.fromkeys(ref for ref in references if ref != citation_path))
    return DelawareCodeProvision(
        kind="section",
        title=title,
        chapter=chapter,
        part=None,
        subchapter=subchapter,
        source_id=source_id,
        display_number=section_id,
        heading=heading,
        body=body,
        parent_citation_path=parent_citation_path,
        level=level,
        ordinal=ordinal,
        source_url=source_url,
        source_relative_path=current_relative_path,
        source_path=source_path,
        sha256=sha256,
        references_to=references_to,
        source_history=history,
        notes=notes,
        status=status,
        canonical_citation_path=canonical,
        variant=variant,
    )


def _record_source_page(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    page: _DelawareSourcePage,
    source_paths: list[Path],
    source_pages: dict[str, tuple[str, str, str]],
) -> None:
    relative = f"{DELAWARE_CODE_SOURCE_FORMAT}/{page.relative_path}"
    artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        relative,
    )
    sha256 = store.write_bytes(artifact_path, page.data)
    source_paths.append(artifact_path)
    source_pages[page.relative_path] = (
        _state_source_key(jurisdiction, run_id, relative),
        sha256,
        page.source_url,
    )


def _append_provision(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    provision: DelawareCodeProvision,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
    seen: set[str],
) -> bool:
    if provision.citation_path in seen:
        return False
    seen.add(provision.citation_path)
    metadata = _metadata(provision)
    items.append(
        SourceInventoryItem(
            citation_path=provision.citation_path,
            source_url=provision.source_url,
            source_path=provision.source_path,
            source_format=DELAWARE_CODE_SOURCE_FORMAT,
            sha256=provision.sha256,
            metadata=metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(provision.citation_path),
            jurisdiction="us-de",
            document_class=DocumentClass.STATUTE.value,
            citation_path=provision.citation_path,
            body=provision.body,
            heading=provision.heading,
            citation_label=provision.legal_identifier,
            version=version,
            source_url=provision.source_url,
            source_path=provision.source_path,
            source_id=provision.source_id,
            source_format=DELAWARE_CODE_SOURCE_FORMAT,
            source_as_of=source_as_of,
            expression_date=expression_date,
            parent_citation_path=provision.parent_citation_path,
            parent_id=(
                deterministic_provision_id(provision.parent_citation_path)
                if provision.parent_citation_path
                else None
            ),
            level=provision.level,
            ordinal=provision.ordinal,
            kind=provision.kind,
            legal_identifier=provision.legal_identifier,
            identifiers=_identifiers(provision),
            metadata=metadata,
        )
    )
    return True


def _part_provision(
    part: DelawarePartLink,
    *,
    source_url: str,
    source_relative_path: str,
    source_path: str,
    sha256: str,
) -> DelawareCodeProvision:
    return DelawareCodeProvision(
        kind="part",
        title=part.title,
        part=part.source_id,
        source_id=part.source_id,
        display_number=part.display_number,
        heading=part.heading,
        body=None,
        parent_citation_path=f"us-de/statute/{part.title}",
        level=1,
        ordinal=part.ordinal,
        source_url=source_url,
        source_relative_path=source_relative_path,
        source_path=source_path,
        sha256=sha256,
    )


def _chapter_provision(
    chapter: DelawareChapterLink,
    *,
    heading: str | None,
    source_url: str,
    source_relative_path: str,
    source_path: str,
    sha256: str,
) -> DelawareCodeProvision:
    return DelawareCodeProvision(
        kind="chapter",
        title=chapter.title,
        chapter=chapter.chapter,
        part=chapter.part_source_id,
        source_id=chapter.chapter,
        display_number=chapter.chapter,
        heading=heading,
        body=None,
        parent_citation_path=chapter.parent_citation_path,
        level=2 if chapter.part_source_id else 1,
        ordinal=chapter.ordinal,
        source_url=source_url,
        source_relative_path=source_relative_path,
        source_path=source_path,
        sha256=sha256,
        status=_status(heading, None, ()),
    )


def _subchapter_provision(
    subchapter: DelawareSubchapterLink,
    *,
    source_url: str,
    source_relative_path: str,
    source_path: str,
    sha256: str,
) -> DelawareCodeProvision:
    return DelawareCodeProvision(
        kind="subchapter",
        title=subchapter.title,
        chapter=subchapter.chapter,
        subchapter=subchapter.source_id,
        source_id=subchapter.source_id,
        display_number=subchapter.display_number,
        heading=subchapter.heading,
        body=None,
        parent_citation_path=f"us-de/statute/{subchapter.title}/{subchapter.chapter}",
        level=2,
        ordinal=subchapter.ordinal,
        source_url=source_url,
        source_relative_path=source_relative_path,
        source_path=source_path,
        sha256=sha256,
        status=_status(subchapter.heading, None, ()),
    )


def _metadata(provision: DelawareCodeProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "title": provision.title,
        "display_number": provision.display_number,
        "source_relative_path": provision.source_relative_path,
    }
    if provision.chapter:
        metadata["chapter"] = provision.chapter
    if provision.part:
        metadata["part"] = provision.part
    if provision.subchapter:
        metadata["subchapter"] = provision.subchapter
    if provision.parent_citation_path:
        metadata["parent_citation_path"] = provision.parent_citation_path
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.source_history:
        metadata["source_history"] = list(provision.source_history)
    if provision.notes:
        metadata["notes"] = list(provision.notes)
    if provision.status:
        metadata["status"] = provision.status
    if provision.variant:
        metadata["variant"] = provision.variant
    if provision.canonical_citation_path:
        metadata["canonical_citation_path"] = provision.canonical_citation_path
    return metadata


def _identifiers(provision: DelawareCodeProvision) -> dict[str, str]:
    identifiers = {
        "delaware:title": provision.title,
        "delaware:kind": provision.kind,
        "delaware:source_id": provision.source_id,
    }
    if provision.chapter:
        identifiers["delaware:chapter"] = provision.chapter
    if provision.part:
        identifiers["delaware:part"] = provision.part
    if provision.subchapter:
        identifiers["delaware:subchapter"] = provision.subchapter
    if provision.variant:
        identifiers["delaware:variant"] = provision.variant
    return identifiers


def _part_from_h3s(
    title: str,
    h3s: list[str],
    *,
    ordinal: int,
) -> DelawarePartLink | None:
    if not h3s:
        return None
    match = _PART_TEXT_RE.fullmatch(h3s[0])
    if not match:
        return None
    display = _clean_text(match.group("part")).upper()
    heading = _clean_text(h3s[1]) if len(h3s) > 1 else None
    return DelawarePartLink(
        title=title,
        source_id=f"part-{_slug(display, fallback=f'part-{ordinal + 1}')}",
        display_number=display,
        heading=heading,
        ordinal=ordinal,
    )


def _chapter_link_from_anchor(
    anchor: Tag,
    *,
    title: str,
    current_relative_path: str,
    base_url: str,
    ordinal: int,
    part_source_id: str | None,
) -> DelawareChapterLink | None:
    href = _clean_href(anchor.get("href"))
    match = _CHAPTER_HREF_RE.search(href)
    if not match:
        return None
    chapter = _normalize_chapter(match.group("chapter"))
    text = _clean_text(anchor.get_text(" ", strip=True))
    text_match = _CHAPTER_TEXT_RE.search(text)
    heading = _clean_text(text_match.group("heading")) if text_match else text
    relative_path = _resolve_relative(current_relative_path, href)
    return DelawareChapterLink(
        title=title,
        chapter=chapter,
        heading=heading,
        relative_path=relative_path,
        source_url=urljoin(_base_url(base_url), relative_path),
        ordinal=ordinal,
        part_source_id=part_source_id,
    )


def _subchapter_link_from_anchor(
    anchor: Tag,
    *,
    title: str,
    chapter: str,
    current_relative_path: str,
    base_url: str,
    ordinal: int,
) -> DelawareSubchapterLink | None:
    href = _clean_href(anchor.get("href"))
    if not _SUBCHAPTER_HREF_RE.search(href):
        return None
    text = _clean_text(anchor.get_text(" ", strip=True))
    match = _SUBCHAPTER_TEXT_RE.search(text)
    display = _clean_text(match.group("subchapter")).upper() if match else str(ordinal + 1)
    heading = _clean_text(match.group("heading")) if match else text
    source_id = f"subchapter-{_slug(display, fallback=str(ordinal + 1))}"
    relative_path = _resolve_relative(current_relative_path, href)
    return DelawareSubchapterLink(
        title=title,
        chapter=chapter,
        source_id=source_id,
        display_number=display,
        heading=heading,
        relative_path=relative_path,
        source_url=urljoin(_base_url(base_url), relative_path),
        ordinal=ordinal,
    )


def _section_body_and_references(section_tag: Tag, *, title: str) -> tuple[list[str], tuple[str, ...]]:
    body_lines: list[str] = []
    references: list[str] = []
    for paragraph in section_tag.find_all("p", recursive=False):
        if not isinstance(paragraph, Tag):
            continue
        text = _clean_text(paragraph.get_text(" ", strip=True))
        if not text:
            continue
        body_lines.append(text)
        references.extend(_delaware_references(paragraph, current_title=title))
        references.extend(_delaware_text_references(text, current_title=title))
    return body_lines, tuple(dict.fromkeys(references))


def _section_history(section_tag: Tag) -> tuple[str, ...]:
    head = section_tag.find("div", class_="SectionHead")
    history: list[str] = []
    seen_head = False
    for child in section_tag.children:
        if child is head:
            seen_head = True
            continue
        if not seen_head:
            continue
        if isinstance(child, NavigableString):
            text = _clean_text(str(child))
            if text:
                history.append(text)
            continue
        if not isinstance(child, Tag):
            continue
        if child.name == "p":
            text = _clean_text(child.get_text(" ", strip=True))
            if _looks_like_source_history(text):
                history.append(text)
            continue
        if child.name == "br":
            continue
        text = _clean_text(child.get_text(" ", strip=True))
        if text:
            history.append(text)
    return tuple(dict.fromkeys(history))


def _delaware_references(root: Tag, *, current_title: str) -> tuple[str, ...]:
    refs: list[str] = []
    for anchor in root.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        ref = _reference_from_href(str(anchor.get("href") or ""), current_title=current_title)
        if ref:
            refs.append(ref)
    return tuple(dict.fromkeys(refs))


def _reference_from_href(href: str, *, current_title: str) -> str | None:
    url, fragment = urldefrag(_clean_href(href))
    if not fragment:
        return None
    path = urlparse(url).path
    title_match = re.search(r"/title(?P<title>\d+)/", path, re.I)
    title = str(int(title_match.group("title"))) if title_match else current_title
    section = _clean_text(fragment)
    if not _is_section_token(section):
        return None
    return f"us-de/statute/{title}/{section}"


def _delaware_text_references(text: str, *, current_title: str) -> tuple[str, ...]:
    refs: list[str] = []
    for match in _SECTION_OF_TITLE_RE.finditer(text):
        if _is_non_delaware_section_context(text, match.start()):
            continue
        refs.append(
            f"us-de/statute/{int(match.group('title'))}/{_clean_text(match.group('section'))}"
        )
    for match in _SECTION_SYMBOL_RE.finditer(text):
        if _is_non_delaware_section_context(text, match.start()):
            continue
        section = _clean_text(match.group("section"))
        refs.append(f"us-de/statute/{current_title}/{section}")
    return tuple(dict.fromkeys(refs))


def _is_non_delaware_section_context(text: str, start: int) -> bool:
    before = text[max(0, start - 40) : start].lower()
    return "u.s.c" in before or "del. laws" in before or "del. c. 1953" in before


def _section_heading_text(heading_text: str, section_id: str) -> str | None:
    pattern = re.compile(rf"^§+\s*{re.escape(section_id)}\.\s*(?P<heading>.*)$", re.I)
    match = pattern.match(heading_text)
    heading = _clean_text(match.group("heading")) if match else heading_text
    heading = heading.rstrip(".")
    return heading or None


def _section_id_from_heading(heading_text: str) -> str:
    match = _SECTION_SYMBOL_RE.search(heading_text)
    if not match:
        raise ValueError(f"cannot parse Delaware section id from heading: {heading_text!r}")
    return _clean_text(match.group("section"))


def _title_heading(html: str | bytes) -> str | None:
    soup = BeautifulSoup(_decode_html(html), "lxml")
    title_head = soup.find(id="TitleHead")
    if isinstance(title_head, Tag):
        h4 = title_head.find("h4")
        if isinstance(h4, Tag):
            text = _clean_text(h4.get_text(" ", strip=True))
            if text:
                return text
    h2 = soup.find("h2")
    if isinstance(h2, Tag):
        text = _clean_text(h2.get_text(" ", strip=True))
        if text:
            return text
    return None


def _chapter_heading(soup: BeautifulSoup) -> str | None:
    title_head = soup.find(id="TitleHead")
    if isinstance(title_head, Tag):
        h3 = title_head.find("h3")
        if isinstance(h3, Tag):
            text = _clean_text(h3.get_text(" ", strip=True))
            match = _CHAPTER_TEXT_RE.search(text)
            return _clean_text(match.group("heading")) if match else text
    return None


def _heading_notes(heading: str | None) -> tuple[str, ...]:
    if heading is None:
        return ()
    return tuple(_clean_text(match.group("note")) for match in _BRACKET_NOTE_RE.finditer(heading))


def _status(
    heading: str | None,
    body: str | None,
    history: tuple[str, ...],
) -> str | None:
    text = " ".join([heading or "", body or "", *history]).lower()
    if "repealed" in text:
        return "repealed"
    if "expired" in text:
        return "expired"
    if "transferred" in text:
        return "transferred"
    if "redesignated" in text:
        return "redesignated"
    if "reserved" in text:
        return "reserved"
    if "effective until" in text or "effective through" in text:
        return "effective_until"
    if "effective upon" in text or "[effective" in text:
        return "future_or_conditional"
    return None


def _looks_like_source_history(text: str) -> bool:
    return bool(
        re.match(
            r"^(?:Code \d{4}|[\dA-Za-z. ]+Del\. C\.|[\dA-Za-z. ]+Del\. Laws|"
            r"Repealed by|Expired by|expired by)",
            text,
            re.I,
        )
    )


def _variant_for_section(heading: str | None, occurrence: int) -> str | None:
    if occurrence == 1:
        return None
    notes = _heading_notes(heading)
    for note in notes:
        if "effective" in note.lower():
            return _slug(note, fallback=f"variant-{occurrence}")
    return f"variant-{occurrence}"


def _normalize_chapter(value: str) -> str:
    match = re.fullmatch(r"0*(?P<number>\d+)(?P<suffix>[A-Za-z]?)", value.strip())
    if not match:
        return value.strip().upper()
    return f"{int(match.group('number'))}{match.group('suffix').upper()}"


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    if not match:
        raise ValueError(f"invalid Delaware title filter: {value!r}")
    return str(int(match.group(0)))


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).replace("Chapter", "").strip()
    match = re.search(r"\d+[A-Za-z]?", cleaned)
    if not match:
        raise ValueError(f"invalid Delaware chapter filter: {value!r}")
    return _normalize_chapter(match.group(0))


def _delaware_run_id(
    version: str,
    *,
    title_filter: str | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and chapter_filter is None and limit is None:
        return version
    parts = [version, "us-de"]
    if title_filter is not None:
        parts.append(f"title-{title_filter}")
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter.lower()}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _download_delaware_page(url: str) -> tuple[bytes, str]:  # pragma: no cover
    last_error: requests.RequestException | None = None
    candidates = _download_candidates(url)
    for cycle in range(3):
        for candidate in candidates:
            for attempt in range(4):
                try:
                    response = requests.get(
                        candidate,
                        headers={"User-Agent": DELAWARE_USER_AGENT},
                        timeout=60,
                    )
                    if response.status_code == 403:
                        last_error = requests.HTTPError(
                            f"403 Client Error: Forbidden for url: {candidate}",
                            response=response,
                        )
                        break
                    if response.status_code in {429, 500, 502, 503, 504}:
                        if attempt + 1 < 4:
                            time.sleep(1.5 * (attempt + 1))
                            continue
                        response.raise_for_status()
                    response.raise_for_status()
                    return response.content, candidate
                except requests.RequestException as exc:
                    last_error = exc
                    if attempt + 1 < 4:
                        time.sleep(1.5 * (attempt + 1))
        if cycle + 1 < 3:
            time.sleep(3.0 * (cycle + 1))
    assert last_error is not None
    raise last_error


def _download_candidates(url: str) -> tuple[str, ...]:
    parsed = urlparse(url)
    host = parsed.netloc
    hosts = [host]
    alternate = host.removeprefix("www.") if host.startswith("www.") else f"www.{host}"
    if alternate and alternate not in hosts:
        hosts.append(alternate)
    paths = [parsed.path or "/"]
    if parsed.path.endswith("/index.html"):
        paths.append(parsed.path[: -len("index.html")])
    elif parsed.path.endswith("/"):
        paths.append(f"{parsed.path}index.html")
    candidates: list[str] = []
    for candidate_host in hosts:
        for candidate_path in paths:
            candidates.append(
                urlunparse(
                    (
                        parsed.scheme or "https",
                        candidate_host,
                        candidate_path,
                        "",
                        parsed.query,
                        "",
                    )
                )
            )
    return tuple(dict.fromkeys(candidates))


def _write_cache_bytes(path: Path, data: bytes) -> None:  # pragma: no cover
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _resolve_relative(current_relative: str, href: str) -> str:
    current_url = urljoin(DELAWARE_CODE_BASE_URL, current_relative)
    return _normalize_relative_path(urlparse(urljoin(current_url, href)).path)


def _normalize_relative_path(value: str) -> str:
    text = _clean_href(value)
    path = urlparse(text).path if "://" in text else text
    path = path.strip().lstrip("/")
    if not path:
        return "index.html"
    if path.endswith("/"):
        return f"{path}index.html"
    return path


def _base_url(base_url: str) -> str:
    return base_url if base_url.endswith("/") else f"{base_url}/"


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _decode_html(value: str | bytes) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _clean_href(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip()


def _clean_text(value: str | None) -> str:
    text = re.sub(r"[\s\u00a0\u2000-\u200b\u202f]+", " ", value or "").strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)


def _is_section_token(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z.-]*(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?", value))


def _slug(value: str, *, fallback: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or fallback
