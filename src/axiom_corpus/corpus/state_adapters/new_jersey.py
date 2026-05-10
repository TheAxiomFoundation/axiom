"""New Jersey statutes source-first corpus adapter."""

from __future__ import annotations

import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import urlparse

import requests

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

NEW_JERSEY_STATUTES_ZIP_URL = (
    "https://pub.njleg.state.nj.us/Statutes/STATUTES-TEXT.zip"
)
NEW_JERSEY_STATUTES_ZIP_SOURCE_FORMAT = "new-jersey-statutes-text-zip"
NEW_JERSEY_STATUTES_TEXT_SOURCE_FORMAT = "new-jersey-statutes-text"
NEW_JERSEY_STATUTES_TEXT_MEMBER = "STATUTES.TXT"
NEW_JERSEY_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_TITLE_RE = re.compile(r"^TITLE\s+(?P<title>[0-9A-Z]+)\s+(?P<heading>\S.*)$")
_APPENDIX_RE = re.compile(r"^APPENDIX\s+(?P<title>[A-Z])\s+(?P<heading>\S.*)$")
_SECTION_HEADER_RE = re.compile(
    r"^(?P<citation>(?:App\.[A-Z]|[0-9A-Za-z]+):[0-9A-Za-z]+"
    r"(?:[-.][0-9A-Za-z]+)*(?:\s+[0-9]+[A-Za-z]?)?)"
    r"\.?(?:\s+)(?P<heading>\S.*)$"
)
_REFERENCE_RE = re.compile(
    r"\b(?:N\.J\.S\.A\.\s*)?(?:C\.\s*)?"
    r"(?P<citation>(?:App\.[A-Z]|[0-9A-Za-z]+):[0-9A-Za-z]+"
    r"(?:[-.][0-9A-Za-z]+)*(?:\s+[0-9]+[A-Za-z]?)?)\b"
)
_SOURCE_HISTORY_RE = re.compile(
    r"^(?:L\.|P\.L\.|Amended|amended|Repealed|R\.S\.|Source:)",
    re.I,
)


@dataclass(frozen=True)
class NewJerseySource:
    """Recorded New Jersey statute text source."""

    source_url: str
    source_path: str
    source_format: str
    sha256: str


@dataclass(frozen=True)
class NewJerseyProvision:
    """One parsed New Jersey statute provision."""

    kind: str
    citation_label: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int
    title: str
    chapter: str | None = None
    source_history: tuple[str, ...] = ()
    references_to: tuple[str, ...] = ()
    status: str | None = None

    @property
    def source_id(self) -> str:
        if self.kind == "title":
            return f"title-{_slug(self.title)}"
        if self.kind == "chapter":
            assert self.chapter is not None
            return f"chapter-{_slug(self.title)}:{_slug(self.chapter)}"
        return _normalize_citation_label(self.citation_label).lower()

    @property
    def citation_path(self) -> str:
        return f"us-nj/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        if self.kind == "title":
            return f"N.J. Stat. Title {_display_title(self.title)}"
        if self.kind == "chapter":
            assert self.chapter is not None
            return f"N.J. Stat. Title {_display_title(self.title)}, ch. {self.chapter}"
        return f"N.J. Stat. § {self.citation_label}"


def extract_new_jersey_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_zip: str | Path | None = None,
    source_url: str = NEW_JERSEY_STATUTES_ZIP_URL,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    timeout_seconds: float = 180.0,
    request_attempts: int = 3,
) -> StateStatuteExtractReport:
    """Snapshot official New Jersey statutes bulk text and extract provisions."""
    jurisdiction = "us-nj"
    title_filter = _title_filter(only_title)
    run_id = _new_jersey_run_id(version, title_filter=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    text_bytes, source_paths = _new_jersey_text_bytes(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        source_dir=Path(source_dir) if source_dir is not None else None,
        source_zip=Path(source_zip) if source_zip is not None else None,
        source_url=source_url,
        download_dir=Path(download_dir) if download_dir is not None else None,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )
    text_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        f"{NEW_JERSEY_STATUTES_TEXT_SOURCE_FORMAT}/{NEW_JERSEY_STATUTES_TEXT_MEMBER}",
    )
    text_sha = store.write_bytes(text_path, text_bytes)
    source_paths.append(text_path)
    recorded_source = NewJerseySource(
        source_url=source_url,
        source_path=_store_relative_path(store, text_path),
        source_format=NEW_JERSEY_STATUTES_TEXT_SOURCE_FORMAT,
        sha256=text_sha,
    )

    provisions = parse_new_jersey_statutes_text(
        text_bytes,
        source=recorded_source,
        only_title=title_filter,
        limit=limit,
    )
    if not provisions:
        raise ValueError(f"no New Jersey statutes selected for filter: {only_title!r}")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    for provision in provisions:
        if provision.citation_path in seen:
            continue
        seen.add(provision.citation_path)
        items.append(_inventory_item(provision, source=recorded_source))
        records.append(
            _record(
                provision,
                version=run_id,
                source=recorded_source,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
        )
        if provision.kind == "title":
            title_count += 1
        elif provision.kind == "section":
            section_count += 1
        else:
            container_count += 1

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
    )


def parse_new_jersey_statutes_text(
    text: str | bytes,
    *,
    source: NewJerseySource,
    only_title: str | None = None,
    limit: int | None = None,
) -> tuple[NewJerseyProvision, ...]:
    """Parse official New Jersey statutes plain text into normalized records."""
    _ = source
    lines = _decode(text).splitlines()
    title_rows: list[tuple[int, str, str]] = []
    section_rows: list[tuple[int, str, str]] = []
    for index, line in enumerate(lines):
        stripped = _clean_whitespace(line)
        if not stripped:
            continue
        title_match = _TITLE_RE.match(stripped)
        if title_match is not None:
            title_rows.append(
                (
                    index,
                    _normalize_title(title_match.group("title")),
                    _title_heading(title_match.group("heading")),
                )
            )
            continue
        appendix_match = _APPENDIX_RE.match(stripped)
        if appendix_match is not None:
            title_rows.append(
                (
                    index,
                    f"App.{appendix_match.group('title').upper()}",
                    _title_heading(appendix_match.group("heading")),
                )
            )
            continue
        section_match = _SECTION_HEADER_RE.match(stripped)
        if section_match is not None:
            section_rows.append(
                (
                    index,
                    _normalize_citation_label(section_match.group("citation")),
                    _strip_terminal_period(section_match.group("heading")),
                )
            )

    if not section_rows:
        raise ValueError("New Jersey statutes text has no section headings")

    title_by_start = {row[0]: (row[1], row[2]) for row in title_rows}
    title_positions = [row[0] for row in title_rows]
    provisions: list[NewJerseyProvision] = []
    seen_titles: set[str] = set()
    seen_chapters: set[tuple[str, str]] = set()
    seen_sections: set[str] = set()
    current_title: tuple[str, str] | None = None
    section_limit_remaining = limit

    for row_index, (line_index, section_label, heading) in enumerate(section_rows):
        if section_limit_remaining is not None and section_limit_remaining <= 0:
            break
        if line_index in title_by_start:
            current_title = title_by_start[line_index]
        else:
            current_title = _nearest_title(title_positions, title_by_start, line_index)
        title, title_heading = _title_for_section(section_label, current_title)
        if only_title is not None and _title_filter(title) != only_title:
            continue
        if title not in seen_titles:
            seen_titles.add(title)
            provisions.append(
                NewJerseyProvision(
                    kind="title",
                    citation_label=title,
                    heading=title_heading,
                    body=None,
                    parent_citation_path=None,
                    level=0,
                    ordinal=len(seen_titles),
                    title=title,
                )
            )
        chapter = _chapter_for_section(section_label)
        chapter_key = (title, chapter)
        if chapter_key not in seen_chapters:
            seen_chapters.add(chapter_key)
            provisions.append(
                NewJerseyProvision(
                    kind="chapter",
                    citation_label=f"{title}:{chapter}",
                    heading=f"Chapter {chapter}",
                    body=None,
                    parent_citation_path=f"us-nj/statute/title-{_slug(title)}",
                    level=1,
                    ordinal=len(seen_chapters),
                    title=title,
                    chapter=chapter,
                )
            )
        if section_label in seen_sections:
            continue
        seen_sections.add(section_label)
        next_line_index = (
            section_rows[row_index + 1][0]
            if row_index + 1 < len(section_rows)
            else len(lines)
        )
        body_lines = lines[line_index + 1 : next_line_index]
        body = _normalize_body(body_lines)
        history = tuple(_source_history(body_lines))
        refs = tuple(_extract_references("\n".join([heading, body or ""]), section_label))
        provisions.append(
            NewJerseyProvision(
                kind="section",
                citation_label=section_label,
                heading=heading,
                body=body,
                parent_citation_path=f"us-nj/statute/chapter-{_slug(title)}:{_slug(chapter)}",
                level=2,
                ordinal=len(seen_sections),
                title=title,
                chapter=chapter,
                source_history=history,
                references_to=refs,
                status=_status(heading, body),
            )
        )
        if section_limit_remaining is not None:
            section_limit_remaining -= 1

    return tuple(provisions)


def _new_jersey_text_bytes(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    source_dir: Path | None,
    source_zip: Path | None,
    source_url: str,
    download_dir: Path | None,
    timeout_seconds: float,
    request_attempts: int,
) -> tuple[bytes, list[Path]]:
    source_paths: list[Path] = []
    if source_dir is not None:
        text_path = source_dir / NEW_JERSEY_STATUTES_TEXT_MEMBER
        if not text_path.exists():
            raise ValueError(f"New Jersey source text not found: {text_path}")
        return text_path.read_bytes(), source_paths

    if source_zip is not None:
        zip_bytes = source_zip.read_bytes()
    else:
        zip_bytes = _download_new_jersey_zip(
            source_url,
            download_dir=download_dir,
            timeout_seconds=timeout_seconds,
            request_attempts=request_attempts,
        )
    _require_zip(zip_bytes, source_url)
    zip_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        f"{NEW_JERSEY_STATUTES_ZIP_SOURCE_FORMAT}/{Path(urlparse(source_url).path).name}",
    )
    store.write_bytes(zip_path, zip_bytes)
    source_paths.append(zip_path)
    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        try:
            return archive.read(NEW_JERSEY_STATUTES_TEXT_MEMBER), source_paths
        except KeyError as exc:
            raise ValueError("New Jersey source ZIP missing STATUTES.TXT") from exc


def _download_new_jersey_zip(
    source_url: str,
    *,
    download_dir: Path | None,
    timeout_seconds: float,
    request_attempts: int,
) -> bytes:
    cached_path: Path | None = None
    if download_dir is not None:
        download_dir.mkdir(parents=True, exist_ok=True)
        cached_path = download_dir / Path(urlparse(source_url).path).name
        if cached_path.exists():
            return cached_path.read_bytes()
    last_error: BaseException | None = None
    for attempt in range(1, max(1, request_attempts) + 1):
        try:
            response = requests.get(
                source_url,
                timeout=timeout_seconds,
                headers={"User-Agent": NEW_JERSEY_USER_AGENT},
            )
            response.raise_for_status()
            data = response.content
            _require_zip(data, source_url)
            if cached_path is not None:
                _write_cache_bytes(cached_path, data)
            return data
        except requests.RequestException as exc:
            last_error = exc
            if attempt < request_attempts:
                time.sleep(0.5 * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"New Jersey source request failed: {source_url}")


def _require_zip(data: bytes, source: str) -> None:
    if not zipfile.is_zipfile(BytesIO(data)):
        raise ValueError(f"New Jersey source did not return a ZIP archive: {source}")


def _nearest_title(
    positions: list[int],
    titles: dict[int, tuple[str, str]],
    line_index: int,
) -> tuple[str, str] | None:
    current: tuple[str, str] | None = None
    for position in positions:
        if position > line_index:
            break
        current = titles[position]
    return current


def _title_for_section(
    section_label: str,
    current_title: tuple[str, str] | None,
) -> tuple[str, str]:
    parsed_title = section_label.split(":", 1)[0]
    if current_title is not None and current_title[0] == parsed_title:
        return current_title
    return parsed_title, _display_title(parsed_title)


def _chapter_for_section(section_label: str) -> str:
    remainder = section_label.split(":", 1)[1]
    return re.split(r"[-.]", remainder, maxsplit=1)[0]


def _inventory_item(
    provision: NewJerseyProvision,
    *,
    source: NewJerseySource,
) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=provision.citation_path,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        metadata=_metadata(provision),
    )


def _record(
    provision: NewJerseyProvision,
    *,
    version: str,
    source: NewJerseySource,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(provision.citation_path),
        jurisdiction="us-nj",
        document_class=DocumentClass.STATUTE.value,
        citation_path=provision.citation_path,
        body=provision.body,
        heading=provision.heading,
        citation_label=provision.legal_identifier,
        version=version,
        source_url=source.source_url,
        source_path=source.source_path,
        source_id=provision.source_id,
        source_format=source.source_format,
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
        metadata=_metadata(provision),
    )


def _identifiers(provision: NewJerseyProvision) -> dict[str, str]:
    identifiers = {"new_jersey:title": provision.title}
    if provision.chapter is not None:
        identifiers["new_jersey:chapter"] = provision.chapter
    if provision.kind == "section":
        identifiers["new_jersey:section"] = provision.citation_label
    return identifiers


def _metadata(provision: NewJerseyProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "title": provision.title,
    }
    if provision.chapter is not None:
        metadata["chapter"] = provision.chapter
    if provision.kind == "section":
        metadata["section"] = provision.citation_label
    if provision.source_history:
        metadata["source_history"] = list(provision.source_history)
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.status:
        metadata["status"] = provision.status
    return metadata


def _source_history(lines: list[str]) -> list[str]:
    history: list[str] = []
    for paragraph in _paragraphs(lines):
        if _SOURCE_HISTORY_RE.match(paragraph):
            history.append(paragraph)
    return history[-5:]


def _extract_references(text: str, self_label: str) -> list[str]:
    refs: list[str] = []
    for match in _REFERENCE_RE.finditer(text):
        label = _normalize_citation_label(match.group("citation"))
        if label != self_label:
            refs.append(f"us-nj/statute/{label.lower()}")
    return _dedupe_preserve_order(refs)


def _status(heading: str | None, body: str | None) -> str | None:
    text = "\n".join([heading or "", body or ""])
    if re.search(r"\bRepealed\b", text, re.I):
        return "repealed"
    if re.search(r"\bExpired\b", text, re.I):
        return "expired"
    if re.search(r"\bOmitted\b", text, re.I):
        return "omitted"
    return None


def _normalize_body(lines: list[str]) -> str | None:
    paragraphs = _paragraphs(lines)
    return "\n\n".join(paragraphs) or None


def _paragraphs(lines: list[str]) -> list[str]:
    paragraphs: list[str] = []
    current: list[str] = []
    for line in lines:
        clean = _clean_whitespace(line)
        if not clean:
            if current:
                paragraphs.append(" ".join(current))
                current = []
            continue
        current.append(clean)
    if current:
        paragraphs.append(" ".join(current))
    return paragraphs


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title|app(?:endix)?)[-\s]*", "", text, flags=re.I)
    if text.upper().startswith("APP."):
        return _slug(text)
    if re.fullmatch(r"[A-Za-z]", text):
        return _slug(f"App.{text.upper()}")
    return _slug(_normalize_title(text))


def _new_jersey_run_id(
    version: str,
    *,
    title_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and limit is None:
        return version
    parts = [version, "us-nj"]
    if title_filter is not None:
        parts.append(f"title-{title_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _normalize_title(value: str) -> str:
    text = _clean_whitespace(value)
    if text.lower().startswith("app."):
        return f"App.{text.split('.', 1)[1].upper()}"
    return text.upper()


def _display_title(value: str) -> str:
    if value.startswith("App."):
        return f"Appendix {value.split('.', 1)[1]}"
    return value


def _title_heading(value: str) -> str:
    return _strip_terminal_period(_clean_whitespace(value).title())


def _normalize_citation_label(value: str) -> str:
    text = _clean_whitespace(value)
    text = re.sub(r"\s+", "-", text)
    text = text.removesuffix(".")
    if text.lower().startswith("app."):
        prefix, rest = text.split(":", 1)
        text = f"App.{prefix.split('.', 1)[1].upper()}:{rest}"
    else:
        prefix, rest = text.split(":", 1)
        text = f"{prefix.upper()}:{rest}"
    return text


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _decode(value: str | bytes) -> str:
    if isinstance(value, str):
        return value
    return value.decode("cp1252", errors="replace")


def _clean_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def _strip_terminal_period(value: str) -> str:
    return value.strip().removesuffix(".").strip()


def _slug(value: str) -> str:
    text = value.strip().lower()
    text = text.replace(".", "-")
    text = re.sub(r"[^0-9a-z]+", "-", text)
    return text.strip("-")


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
