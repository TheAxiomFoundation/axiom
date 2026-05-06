"""Oregon Revised Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

OREGON_ORS_BASE_URL = "https://www.oregonlegislature.gov/bills_laws/"
OREGON_ORS_LANDING_RELATIVE = "Pages/ORS.aspx"
OREGON_ORS_SOURCE_FORMAT = "oregon-ors-html"
OREGON_ORS_DEFAULT_YEAR = 2025
OREGON_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_CHAPTER_RE = re.compile(
    r"^Chapter\s+(?P<chapter>\d{1,3}[A-Z]?)\s*[\u2014-]\s*(?P<heading>.+)$",
    re.I,
)
_CHAPTER_DASH_ONLY_RE = re.compile(r"^Chapter\s+(?P<chapter>\d{1,3}[A-Z]?)\s*[\u2014-]\s*$", re.I)
_FORMER_CHAPTER_RE = re.compile(
    r"^Chapter\s+(?P<chapter>\d{1,3}[A-Z]?)\s+\(Former Provisions\)"
    r"(?:\s*[\u2014-]\s*(?P<heading>.+))?$",
    re.I,
)
_CHAPTER_NUMBER_ONLY_RE = re.compile(r"^Chapter\s+(?P<chapter>\d{1,3}[A-Z]?)$", re.I)
_EDITION_RE = re.compile(r"^(?P<year>\d{4})\s+EDITION$", re.I)
_SECTION_START_RE = re.compile(
    r"^(?P<section>\d{1,3}[A-Z]?\.\d{3,4}[A-Z]?)(?P<rest>(?:\s+|\.).*)?$"
)
_ORS_TEXT_RE = re.compile(r"\b(?:ORS\s+)?(?P<cite>\d{1,3}[A-Z]?\.\d{3,4}[A-Z]?)\b")
_TITLE_GROUP_RE = re.compile(
    r"(?P<title>\d{1,2}A?)\.\s+"
    r"(?P<heading>.+?)\s+-\s+"
    r"Chapters?\s+(?P<start>\d{1,3}[A-Z]?)"
    r"(?:\s*-\s*(?P<end>\d{1,3}[A-Z]?))?",
    re.I,
)
_SOURCE_HISTORY_RE = re.compile(
    r"\s*(?P<history>\[[^\[\]]*(?:c\.|Formerly|[Rr]epealed|[Rr]enumbered)[^\[\]]*\])$"
)
_FUTURE_TEXT_NOTE_RE = re.compile(
    r"^Note:\s+The amendments to\s+(?P<section>\d{1,3}[A-Z]?\.\d{3,4}[A-Z]?)"
    r".*?\bbecome\s+(?P<kind>operative|effective)\s+(?:on\s+)?(?P<date>[A-Z][a-z]+ \d{1,2}, \d{4})"
    r".*?text that is\s+(?P=kind)",
    re.I,
)
_DEFAULT_ALPHA_CHAPTER_SUFFIXES = ("A", "B", "C")


@dataclass(frozen=True)
class OregonOrsTitle:
    """Title metadata parsed from the official ORS landing page."""

    number: str
    heading: str
    start_chapter: str
    end_chapter: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-or/statute/title-{self.number}"

    @property
    def legal_identifier(self) -> str:
        return f"ORS Title {self.number}"


@dataclass(frozen=True)
class OregonOrsChapter:
    """Chapter source selected for extraction."""

    chapter: str
    heading: str | None = None
    title_number: str | None = None
    title_heading: str | None = None
    title_ordinal: int | None = None
    ordinal: int | None = None

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter}"

    @property
    def citation_path(self) -> str:
        return f"us-or/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"ORS Chapter {self.chapter}"

    @property
    def parent_citation_path(self) -> str | None:
        if self.title_number is None:
            return None
        return f"us-or/statute/title-{self.title_number}"


@dataclass(frozen=True)
class OregonOrsProvision:
    """Parsed ORS series or section provision from one chapter HTML page."""

    kind: str
    source_id: str
    display_number: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int
    references_to: tuple[str, ...] = ()
    source_history: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    status: str | None = None
    effective_note: str | None = None
    canonical_citation_path: str | None = None

    @property
    def citation_path(self) -> str:
        return f"us-or/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        if self.kind == "section":
            return f"ORS {self.display_number}"
        return f"ORS {self.display_number}"


@dataclass(frozen=True)
class OregonOrsChapterParse:
    """Parsed official ORS chapter page."""

    chapter: str
    heading: str | None
    source_year: int | None
    provisions: tuple[OregonOrsProvision, ...]


@dataclass(frozen=True)
class _OregonSourcePage:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _VariantCue:
    section: str
    slug: str
    note: str


class _OregonFetcher:
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

    def fetch_landing(self) -> _OregonSourcePage:
        return self.fetch(
            f"{OREGON_ORS_SOURCE_FORMAT}/ORS.aspx",
            urljoin(self.base_url, OREGON_ORS_LANDING_RELATIVE),
        )

    def fetch_chapter(self, chapter: str) -> _OregonSourcePage:
        token = _chapter_filter(chapter)
        return self.fetch(_chapter_relative_path(token), _chapter_url(token, self.base_url))

    def fetch(self, relative_path: str, source_url: str) -> _OregonSourcePage:
        normalized = _normalize_relative_path(relative_path)
        if self.source_dir is not None:
            source_path = _source_dir_file(self.source_dir, normalized)
            if source_path is None:
                raise ValueError(f"Oregon source file does not exist: {self.source_dir / normalized}")
            return _OregonSourcePage(
                relative_path=normalized,
                source_url=source_url,
                data=source_path.read_bytes(),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            if cached_path.exists():
                return _OregonSourcePage(
                    relative_path=normalized,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )

        data = _download_oregon_page(source_url)
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            cached_path.write_bytes(data)
        return _OregonSourcePage(relative_path=normalized, source_url=source_url, data=data)


def extract_oregon_ors(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_year: int = OREGON_ORS_DEFAULT_YEAR,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    only_chapter: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    workers: int = 8,
    base_url: str = OREGON_ORS_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official Oregon Legislature ORS HTML and extract provisions."""
    jurisdiction = "us-or"
    title_filter = _title_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter) if only_chapter is not None else None
    run_id = _oregon_run_id(
        version,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _OregonFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
    )

    source_paths: list[Path] = []
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    remaining_sections = limit

    landing_source_key: str | None = None
    landing_sha: str | None = None
    titles: tuple[OregonOrsTitle, ...] = ()
    try:
        landing_page = fetcher.fetch_landing()
    except ValueError:
        if source_dir is None:
            raise
    else:
        landing_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            landing_page.relative_path,
        )
        landing_sha = store.write_bytes(landing_path, landing_page.data)
        source_paths.append(landing_path)
        landing_source_key = _state_source_key(jurisdiction, run_id, landing_page.relative_path)
        titles = parse_oregon_ors_landing_html(landing_page.data)

    chapters = _selected_chapters(
        source_root=Path(source_dir) if source_dir is not None else None,
        titles=titles,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
        fetcher=fetcher,
        workers=workers,
    )
    if not chapters:
        raise ValueError(f"no Oregon ORS chapters selected for filters: {only_title!r}, {only_chapter!r}")

    title_by_number = {title.number: title for title in titles}
    for chapter in chapters:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        try:
            page = fetcher.fetch_chapter(chapter.chapter)
        except ValueError as exc:
            skipped_source_count += 1
            errors.append(f"chapter {chapter.chapter}: {exc}")
            continue

        artifact_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            page.relative_path,
        )
        sha256 = store.write_bytes(artifact_path, page.data)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, page.relative_path)

        parsed = parse_oregon_chapter_html(page.data)
        selected_chapter = _chapter_with_parse_metadata(chapter, parsed, titles)

        title = title_by_number.get(selected_chapter.title_number or "")
        if title is not None and title.citation_path not in seen:
            seen.add(title.citation_path)
            title_count += 1
            container_count += 1
            _append_record(
                items,
                records,
                citation_path=title.citation_path,
                version=run_id,
                source_url=urljoin(fetcher.base_url, OREGON_ORS_LANDING_RELATIVE),
                source_path=landing_source_key or source_key,
                source_id=f"title-{title.number}",
                sha256=landing_sha or sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="title",
                heading=title.heading,
                legal_identifier=title.legal_identifier,
                level=0,
                ordinal=title.ordinal,
                identifiers={"oregon:title": title.number},
                metadata={
                    "kind": "title",
                    "title": title.number,
                    "source_year": source_year,
                    "start_chapter": title.start_chapter,
                    "end_chapter": title.end_chapter,
                },
            )

        if selected_chapter.citation_path not in seen:
            seen.add(selected_chapter.citation_path)
            container_count += 1
            _append_record(
                items,
                records,
                citation_path=selected_chapter.citation_path,
                version=run_id,
                source_url=page.source_url,
                source_path=source_key,
                source_id=selected_chapter.source_id,
                sha256=sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="chapter",
                heading=selected_chapter.heading or parsed.heading,
                legal_identifier=selected_chapter.legal_identifier,
                parent_citation_path=selected_chapter.parent_citation_path,
                level=1 if selected_chapter.parent_citation_path else 0,
                ordinal=selected_chapter.ordinal,
                identifiers=_chapter_identifiers(selected_chapter),
                metadata={
                    "kind": "chapter",
                    "chapter": selected_chapter.chapter,
                    "title": selected_chapter.title_number,
                    "source_year": parsed.source_year or source_year,
                    "parent_citation_path": selected_chapter.parent_citation_path,
                },
            )

        for provision in parsed.provisions:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            if provision.citation_path in seen:
                continue
            seen.add(provision.citation_path)
            _append_record(
                items,
                records,
                citation_path=provision.citation_path,
                version=run_id,
                source_url=page.source_url,
                source_path=source_key,
                source_id=provision.source_id,
                sha256=sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind=provision.kind,
                heading=provision.heading,
                body=provision.body,
                legal_identifier=provision.legal_identifier,
                parent_citation_path=provision.parent_citation_path or selected_chapter.citation_path,
                level=provision.level,
                ordinal=provision.ordinal,
                identifiers=_provision_identifiers(provision, selected_chapter),
                metadata=_provision_metadata(provision, selected_chapter, parsed.source_year or source_year),
            )
            if provision.kind == "section":
                section_count += 1
                if remaining_sections is not None:
                    remaining_sections -= 1
            else:
                container_count += 1

    if not items:
        raise ValueError("no Oregon ORS provisions extracted")

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


def parse_oregon_ors_landing_html(html: str | bytes) -> tuple[OregonOrsTitle, ...]:
    """Parse title/chapter ranges from the official ORS landing page."""
    text = html.decode("utf-8", errors="ignore") if isinstance(html, bytes) else html
    titles: list[OregonOrsTitle] = []
    seen: set[str] = set()

    for raw_group in re.findall(r'groupString="([^"]+)"', text):
        group = unquote(raw_group).replace(";#", "")
        group = re.sub(r";+", ";", group)
        for match in _iter_title_group_matches(group):
            title = match.group("title").upper()
            if not _valid_title_number(title):
                continue
            if title in seen:
                continue
            start = _chapter_filter(match.group("start"))
            end = _chapter_filter(match.group("end") or start)
            titles.append(
                OregonOrsTitle(
                    number=title,
                    heading=_titlecase_heading(match.group("heading")),
                    start_chapter=start,
                    end_chapter=end,
                    ordinal=len(titles),
                )
            )
            seen.add(title)

    if titles:
        return tuple(titles)

    soup = BeautifulSoup(html, "lxml")
    for link in soup.find_all("a"):
        title = _clean_text(str(link.get("data-ors-title") or ""))
        heading = _clean_text(str(link.get("data-ors-title-heading") or ""))
        start = _clean_text(str(link.get("data-ors-start-chapter") or ""))
        end = _clean_text(str(link.get("data-ors-end-chapter") or ""))
        if not title or not heading or not start:
            continue
        normalized_title = title.upper()
        if normalized_title in seen:
            continue
        titles.append(
            OregonOrsTitle(
                number=normalized_title,
                heading=heading,
                start_chapter=_chapter_filter(start),
                end_chapter=_chapter_filter(end or start),
                ordinal=len(titles),
            )
        )
        seen.add(normalized_title)
    return tuple(titles)


def parse_oregon_chapter_html(html: str | bytes) -> OregonOrsChapterParse:
    """Parse one official Oregon Legislature ORS chapter HTML page."""
    texts = _paragraph_texts(html)
    chapter, chapter_heading = _parse_chapter_heading(texts)
    source_year = _parse_source_year(texts)
    content_start = _content_start_index(texts, chapter)
    toc_headings = _toc_headings(texts[:content_start], chapter)

    provisions: list[OregonOrsProvision] = []
    current_section: dict[str, Any] | None = None
    current_series_path: str | None = None
    pending_variant: _VariantCue | None = None
    series_counts: dict[str, int] = {}
    section_seen_counts: dict[str, int] = {}
    in_note_block = False

    def flush_section() -> None:
        nonlocal current_section
        if current_section is None:
            return
        body_lines = list(current_section["body_lines"])
        body_lines, source_history = _split_source_history(body_lines)
        notes = tuple(current_section["notes"])
        source_history = (*current_section["source_history"], *source_history)
        text_for_status = " ".join([current_section["raw_rest"], *body_lines, *source_history])
        status = current_section["status"] or _section_status(text_for_status)
        canonical = (
            f"us-or/statute/{current_section['section']}"
            if current_section["source_id"] != current_section["section"]
            else None
        )
        reference_text = "\n".join([*body_lines, *notes])
        references = _references_to(reference_text, current_section["section"])
        body = "\n".join(body_lines).strip() or current_section["raw_rest"] or None
        provisions.append(
            OregonOrsProvision(
                kind="section",
                source_id=current_section["source_id"],
                display_number=current_section["section"],
                heading=current_section["heading"],
                body=body,
                parent_citation_path=current_section["parent"],
                level=3 if current_section["parent"] else 2,
                ordinal=current_section["ordinal"],
                references_to=references,
                source_history=source_history,
                notes=notes,
                status=status,
                effective_note=current_section["effective_note"],
                canonical_citation_path=canonical,
            )
        )
        current_section = None

    for text in texts[content_start:]:
        if not text:
            continue
        start_match = _section_start_match(text, chapter)
        if start_match is not None:
            flush_section()
            section = start_match.group("section")
            raw_rest = _clean_section_rest(start_match.group("rest"))
            base_heading = toc_headings.get(section)
            variant = _consume_variant(pending_variant, section)
            if variant is not None:
                pending_variant = None
            seen_count = section_seen_counts.get(section, 0) + 1
            section_seen_counts[section] = seen_count
            source_id = section
            status = None
            effective_note = None
            if variant is not None:
                source_id = f"{section}@{variant.slug}"
                status = "future_or_conditional"
                effective_note = variant.note
            elif seen_count > 1:
                source_id = f"{section}@variant-{seen_count}"
                status = "variant"

            body_first = _body_after_heading(raw_rest, base_heading)
            current_section = {
                "section": section,
                "source_id": source_id,
                "heading": base_heading or _fallback_heading(raw_rest),
                "raw_rest": raw_rest,
                "body_lines": [body_first] if body_first else [],
                "notes": [],
                "source_history": [],
                "parent": current_series_path or f"us-or/statute/chapter-{chapter}",
                "ordinal": len(provisions),
                "status": status,
                "effective_note": effective_note,
            }
            in_note_block = False
            continue

        if _is_series_heading(text):
            flush_section()
            source_id = _series_source_id(chapter, text, series_counts)
            current_series_path = f"us-or/statute/{source_id}"
            provisions.append(
                OregonOrsProvision(
                    kind="series",
                    source_id=source_id,
                    display_number=f"Chapter {chapter}",
                    heading=text,
                    body=None,
                    parent_citation_path=f"us-or/statute/chapter-{chapter}",
                    level=2,
                    ordinal=len(provisions),
                )
            )
            in_note_block = False
            continue

        if current_section is None:
            continue

        if text.startswith("Note:"):
            current_section["notes"].append(text)
            variant = _variant_from_note(text)
            if variant is not None:
                pending_variant = variant
            in_note_block = True
        elif in_note_block:
            current_section["notes"].append(text)
        else:
            current_section["body_lines"].append(text)

    flush_section()
    return OregonOrsChapterParse(
        chapter=chapter,
        heading=chapter_heading,
        source_year=source_year,
        provisions=tuple(provisions),
    )


def _selected_chapters(
    *,
    source_root: Path | None,
    titles: tuple[OregonOrsTitle, ...],
    title_filter: str | None,
    chapter_filter: str | None,
    fetcher: _OregonFetcher,
    workers: int,
) -> tuple[OregonOrsChapter, ...]:
    if chapter_filter is not None:
        title = _title_for_chapter(chapter_filter, titles)
        if title_filter is not None and (title is None or title.number != title_filter):
            return ()
        return (_chapter_from_title(chapter_filter, title),)

    if source_root is not None:
        chapters = tuple(
            _chapter_from_title(token, _title_for_chapter(token, titles))
            for token in _iter_source_dir_chapter_tokens(source_root)
        )
    else:
        chapters = _discover_live_chapters(titles, fetcher=fetcher, workers=workers)

    if title_filter is not None:
        chapters = tuple(chapter for chapter in chapters if chapter.title_number == title_filter)
    return tuple(sorted(chapters, key=lambda chapter: _chapter_sort_key(chapter.chapter)))


def _discover_live_chapters(
    titles: tuple[OregonOrsTitle, ...],
    *,
    fetcher: _OregonFetcher,
    workers: int,
) -> tuple[OregonOrsChapter, ...]:
    if not titles:
        raise ValueError("official ORS landing page did not expose title chapter ranges")
    candidates = [
        (token, title)
        for title in titles
        for token in _candidate_chapter_tokens(title.start_chapter, title.end_chapter)
    ]
    found: list[OregonOrsChapter] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_to_candidate = {
            executor.submit(_live_chapter_exists, fetcher, token): (token, title)
            for token, title in candidates
        }
        for future in as_completed(future_to_candidate):
            token, title = future_to_candidate[future]
            if future.result():
                found.append(_chapter_from_title(token, title))
    return tuple(found)


def _live_chapter_exists(fetcher: _OregonFetcher, token: str) -> bool:
    try:
        fetcher.fetch_chapter(token)
    except ValueError:
        return False
    return True


def _chapter_with_parse_metadata(
    chapter: OregonOrsChapter,
    parsed: OregonOrsChapterParse,
    titles: tuple[OregonOrsTitle, ...],
) -> OregonOrsChapter:
    title = _title_for_chapter(parsed.chapter, titles)
    return OregonOrsChapter(
        chapter=parsed.chapter,
        heading=parsed.heading or chapter.heading,
        title_number=chapter.title_number or (title.number if title else None),
        title_heading=chapter.title_heading or (title.heading if title else None),
        title_ordinal=chapter.title_ordinal if chapter.title_ordinal is not None else (title.ordinal if title else None),
        ordinal=chapter.ordinal if chapter.ordinal is not None else _chapter_sort_key(parsed.chapter)[0],
    )


def _chapter_from_title(token: str, title: OregonOrsTitle | None) -> OregonOrsChapter:
    return OregonOrsChapter(
        chapter=token,
        title_number=title.number if title else None,
        title_heading=title.heading if title else None,
        title_ordinal=title.ordinal if title else None,
        ordinal=_chapter_sort_key(token)[0],
    )


def _iter_source_dir_chapter_tokens(source_root: Path) -> Iterator[str]:
    for path in sorted(source_root.rglob("ors*.html")):
        match = re.fullmatch(r"ors(?P<chapter>\d{1,3}[A-Z]?)\.html", path.name, re.I)
        if match:
            yield _chapter_filter(match.group("chapter"))


def _candidate_chapter_tokens(start: str, end: str) -> Iterator[str]:
    start_number, start_suffix = _chapter_sort_key(start)
    end_number, end_suffix = _chapter_sort_key(end)
    for number in range(start_number, end_number + 1):
        numeric = f"{number:03d}"
        if _chapter_in_range(numeric, start, end):
            yield numeric
        for suffix in _candidate_alpha_suffixes(
            number,
            start_number=start_number,
            start_suffix=start_suffix,
            end_number=end_number,
            end_suffix=end_suffix,
        ):
            token = f"{number:03d}{suffix}"
            if _chapter_in_range(token, start, end):
                yield token


def _candidate_alpha_suffixes(
    number: int,
    *,
    start_number: int,
    start_suffix: str,
    end_number: int,
    end_suffix: str,
) -> tuple[str, ...]:
    if start_suffix == "" == end_suffix:
        return _DEFAULT_ALPHA_CHAPTER_SUFFIXES
    low = start_suffix if number == start_number and start_suffix else "A"
    high = end_suffix if number == end_number and end_suffix else "Z"
    if number == end_number and not end_suffix:
        return ()
    return tuple(chr(code) for code in range(ord(low), ord(high) + 1))


def _chapter_in_range(token: str, start: str, end: str) -> bool:
    return _chapter_sort_key(start) <= _chapter_sort_key(token) <= _chapter_sort_key(end)


def _title_for_chapter(
    chapter: str,
    titles: tuple[OregonOrsTitle, ...],
) -> OregonOrsTitle | None:
    token = _chapter_filter(chapter)
    for title in titles:
        if _chapter_in_range(token, title.start_chapter, title.end_chapter):
            return title
    return None


def _append_record(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    *,
    citation_path: str,
    version: str,
    source_url: str | None,
    source_path: str | None,
    source_id: str,
    sha256: str | None,
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
    item_metadata = {
        **metadata,
        "kind": kind,
        "heading": heading,
        "parent_citation_path": parent_citation_path,
        "source_id": source_id,
    }
    items.append(
        SourceInventoryItem(
            citation_path=citation_path,
            source_url=source_url,
            source_path=source_path,
            source_format=OREGON_ORS_SOURCE_FORMAT,
            sha256=sha256,
            metadata=item_metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(citation_path),
            jurisdiction="us-or",
            document_class=DocumentClass.STATUTE.value,
            citation_path=citation_path,
            body=body,
            heading=heading,
            citation_label=legal_identifier,
            version=version,
            source_url=source_url,
            source_path=source_path,
            source_id=source_id,
            source_format=OREGON_ORS_SOURCE_FORMAT,
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
            metadata=metadata,
        )
    )


def _chapter_identifiers(chapter: OregonOrsChapter) -> dict[str, str]:
    identifiers = {"oregon:chapter": chapter.chapter}
    if chapter.title_number is not None:
        identifiers["oregon:title"] = chapter.title_number
    return identifiers


def _provision_identifiers(
    provision: OregonOrsProvision,
    chapter: OregonOrsChapter,
) -> dict[str, str]:
    identifiers = {"oregon:chapter": chapter.chapter, f"oregon:{provision.kind}": provision.display_number}
    if provision.kind == "section":
        identifiers["oregon:section"] = provision.display_number
    if chapter.title_number is not None:
        identifiers["oregon:title"] = chapter.title_number
    return identifiers


def _provision_metadata(
    provision: OregonOrsProvision,
    chapter: OregonOrsChapter,
    source_year: int,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "chapter": chapter.chapter,
        "title": chapter.title_number,
        "source_year": source_year,
        "references_to": list(provision.references_to),
        "source_history": list(provision.source_history),
        "notes": list(provision.notes),
        "status": provision.status,
        "effective_note": provision.effective_note,
    }
    if provision.canonical_citation_path is not None:
        metadata["canonical_citation_path"] = provision.canonical_citation_path
    return {key: value for key, value in metadata.items() if value not in (None, [], {})}


def _paragraph_texts(html: str | bytes) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    return [_clean_text(paragraph.get_text(" ", strip=True)) for paragraph in soup.find_all("p")]


def _parse_chapter_heading(texts: Iterable[str]) -> tuple[str, str | None]:
    text_list = list(texts)
    for index, text in enumerate(text_list):
        match = _CHAPTER_RE.match(text)
        if match:
            return _chapter_filter(match.group("chapter")), _clean_text(match.group("heading"))
        dash_only_match = _CHAPTER_DASH_ONLY_RE.match(text)
        if dash_only_match:
            return _chapter_filter(dash_only_match.group("chapter")), _next_heading_text(
                text_list[index + 1 :]
            )
        former_match = _FORMER_CHAPTER_RE.match(text)
        if former_match:
            heading = former_match.group("heading") or "Former Provisions"
            return _chapter_filter(former_match.group("chapter")), _clean_text(heading)
        number_match = _CHAPTER_NUMBER_ONLY_RE.match(text)
        if number_match:
            return _chapter_filter(number_match.group("chapter")), _next_heading_text(
                text_list[index + 1 :]
            )
    raise ValueError("Oregon ORS chapter page is missing a chapter heading")


def _next_heading_text(texts: Iterable[str]) -> str | None:
    for text in texts:
        cleaned = _clean_text(text)
        if not cleaned or _EDITION_RE.match(cleaned):
            continue
        return cleaned
    return None


def _parse_source_year(texts: Iterable[str]) -> int | None:
    for text in texts:
        match = _EDITION_RE.match(text)
        if match:
            return int(match.group("year"))
    return None


def _content_start_index(texts: list[str], chapter: str) -> int:
    first_section: str | None = None
    for index, text in enumerate(texts):
        match = _section_start_match(text, chapter)
        if match is None:
            continue
        section = match.group("section")
        if first_section is None:
            first_section = section
            continue
        if section == first_section:
            return _rewind_series_start(texts, index)
    for index, text in enumerate(texts):
        if _section_start_match(text, chapter) is not None:
            return _rewind_series_start(texts, index)
    return len(texts)


def _rewind_series_start(texts: list[str], index: int) -> int:
    start = index
    while start > 0 and _is_series_heading(texts[start - 1]):
        start -= 1
    return start


def _toc_headings(texts: Iterable[str], chapter: str) -> dict[str, str]:
    headings: dict[str, str] = {}
    for text in texts:
        match = _section_start_match(text, chapter)
        if match is None:
            continue
        rest = _clean_section_rest(match.group("rest"))
        if rest and rest not in {"."}:
            headings.setdefault(match.group("section"), rest.rstrip("."))
    return headings


def _section_start_match(text: str, chapter: str) -> re.Match[str] | None:
    match = _SECTION_START_RE.match(text)
    if match is None:
        return None
    section = match.group("section")
    if _chapter_sort_key(section.split(".", 1)[0]) != _chapter_sort_key(chapter):
        return None
    return match


def _iter_title_group_matches(group: str) -> Iterator[re.Match[str]]:
    for start in range(len(group)):
        match = _TITLE_GROUP_RE.match(group, start)
        if match is not None:
            yield match


def _valid_title_number(title: str) -> bool:
    match = re.fullmatch(r"(?P<number>\d{1,2})(?P<suffix>A?)", title)
    if match is None:
        return False
    number = int(match.group("number"))
    return 1 <= number <= 62


def _is_series_heading(text: str) -> bool:
    if _SECTION_START_RE.match(text) or _CHAPTER_RE.match(text) or _EDITION_RE.match(text):
        return False
    if len(text) > 140:
        return False
    if text.startswith("(") and text.endswith(")"):
        return True
    letters = re.sub(r"[^A-Za-z]+", "", text)
    return bool(letters) and letters.upper() == letters and len(letters) >= 3


def _series_source_id(chapter: str, heading: str, counts: dict[str, int]) -> str:
    slug = _slug(heading)
    count = counts.get(slug, 0) + 1
    counts[slug] = count
    suffix = f"-{count}" if count > 1 else ""
    return f"chapter-{chapter}/series-{slug}{suffix}"


def _body_after_heading(rest: str, heading: str | None) -> str | None:
    if not rest:
        return None
    if heading:
        normalized_heading = _clean_text(heading).rstrip(".")
        normalized_rest = _clean_text(rest)
        for prefix in (normalized_heading, f"{normalized_heading}."):
            if normalized_rest == prefix:
                return None
            if normalized_rest.startswith(f"{prefix} "):
                return normalized_rest[len(prefix) :].strip()
        return normalized_rest
    fallback_heading = _fallback_heading(rest)
    if fallback_heading:
        remainder = rest[len(fallback_heading) :].strip()
        return remainder.lstrip(". ").strip() or None
    return rest


def _fallback_heading(rest: str) -> str | None:
    if not rest or rest.startswith("["):
        return None
    match = re.match(r"(?P<heading>.+?)\.\s+\S", rest)
    if not match:
        return rest.rstrip(".") or None
    return match.group("heading").strip()


def _clean_section_rest(rest: str | None) -> str:
    return _clean_text((rest or "").lstrip(". "))


def _split_source_history(body_lines: list[str]) -> tuple[list[str], tuple[str, ...]]:
    history: list[str] = []
    cleaned = [line for line in body_lines if line]
    while cleaned:
        match = _SOURCE_HISTORY_RE.search(cleaned[-1])
        if match is None:
            break
        history.insert(0, match.group("history"))
        cleaned[-1] = _clean_text(cleaned[-1][: match.start()])
        if not cleaned[-1]:
            cleaned.pop()
    return cleaned, tuple(history)


def _section_status(text: str) -> str | None:
    if re.search(r"\brepealed\b", text, re.I):
        return "repealed"
    if re.search(r"\brenumbered\b", text, re.I):
        return "renumbered"
    return None


def _variant_from_note(note: str) -> _VariantCue | None:
    match = _FUTURE_TEXT_NOTE_RE.match(note)
    if match is None:
        return None
    try:
        parsed_date = datetime.strptime(match.group("date"), "%B %d, %Y").date()
    except ValueError:
        return None
    kind = match.group("kind").lower()
    return _VariantCue(
        section=match.group("section"),
        slug=f"{kind}-{parsed_date.isoformat()}",
        note=note,
    )


def _consume_variant(variant: _VariantCue | None, section: str) -> _VariantCue | None:
    if variant is not None and variant.section == section:
        return variant
    return None


def _references_to(text: str, self_section: str) -> tuple[str, ...]:
    refs: list[str] = []
    for match in _ORS_TEXT_RE.finditer(text):
        cite = match.group("cite")
        if cite == self_section:
            continue
        refs.append(f"us-or/statute/{cite}")
    return tuple(dict.fromkeys(refs))


def _download_oregon_page(url: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": OREGON_USER_AGENT},
                timeout=30,
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(0.5)
    raise ValueError(f"failed to fetch Oregon source page {url}: {last_error}")


def _source_dir_file(source_root: Path, relative_path: str) -> Path | None:
    candidates = [
        source_root / relative_path,
        source_root / Path(relative_path).name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _chapter_relative_path(chapter: str) -> str:
    return f"{OREGON_ORS_SOURCE_FORMAT}/ors{_chapter_filter(chapter)}.html"


def _chapter_url(chapter: str, base_url: str) -> str:
    return urljoin(_base_url(base_url), f"ors/ors{_chapter_filter(chapter)}.html")


def _oregon_run_id(
    version: str,
    *,
    title_filter: str | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    parts = [version]
    if title_filter is not None:
        parts.append(f"us-or-title-{title_filter.lower()}")
    if chapter_filter is not None:
        parts.append(f"us-or-chapter-{chapter_filter.lower()}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    text = re.sub(r"^(?:TITLE|T)\s*", "", text, flags=re.I)
    if not re.fullmatch(r"\d{1,2}A?", text):
        raise ValueError(f"invalid Oregon title filter: {value!r}")
    return text


def _chapter_filter(value: str | int) -> str:
    text = str(value).strip().upper()
    text = re.sub(r"^(?:CHAPTER|CH\.?|ORS)\s*", "", text, flags=re.I)
    text = text.removeprefix("ORS").strip()
    text = text.removeprefix("ORS").strip()
    text = text.removeprefix("OR").strip() if text.startswith("OR ") else text
    match = re.fullmatch(r"0*(?P<number>\d{1,3})(?P<suffix>[A-Z]?)", text)
    if match is None:
        raise ValueError(f"invalid Oregon chapter filter: {value!r}")
    return f"{int(match.group('number')):03d}{match.group('suffix')}"


def _chapter_sort_key(chapter: str) -> tuple[int, str]:
    match = re.fullmatch(r"0*(?P<number>\d{1,3})(?P<suffix>[A-Z]?)", chapter.upper())
    if match is None:
        raise ValueError(f"invalid Oregon chapter: {chapter!r}")
    return int(match.group("number")), match.group("suffix")


def _base_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/"


def _normalize_relative_path(relative_path: str) -> str:
    return relative_path.strip().lstrip("/").split("?", 1)[0]


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _clean_text(value: str | None) -> str:
    text = (value or "").replace("\xa0", " ").replace("\u200e", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)


def _titlecase_heading(heading: str) -> str:
    words = []
    for word in _clean_text(heading).replace(";;", ";").split():
        words.append(word if word.isupper() and len(word) <= 4 else word.capitalize())
    return " ".join(words)


def _slug(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "unnamed"
