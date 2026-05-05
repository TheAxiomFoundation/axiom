"""Indiana Code source-first corpus adapter."""

from __future__ import annotations

import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore, sha256_bytes
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

INDIANA_CODE_BASE_URL = "https://iga.in.gov/ic/"
INDIANA_CODE_SOURCE_FORMAT = "indiana-code-html"
INDIANA_CODE_DEFAULT_YEAR = 2025
INDIANA_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 Chrome/120 Safari/537.36"
)

_HEADING_KINDS = ("title", "article", "chapter", "section")
_IC_TEXT_RE = re.compile(r"\bIC\s+(?P<cite>\d+(?:[-.]\d+[A-Za-z]?)+(?:-[A-Za-z])?)\b")


@dataclass(frozen=True)
class IndianaCodeProvision:
    """Parsed Indiana Code title/article/chapter/section node."""

    kind: str
    source_id: str
    display_number: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int | None
    references_to: tuple[str, ...]
    notes: tuple[str, ...]
    source_history: tuple[str, ...]
    derivation: tuple[str, ...]

    @property
    def title(self) -> str:
        return self.source_id.split("-", 1)[0]

    @property
    def citation_path(self) -> str:
        return f"us-in/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"IC {self.display_number}"


def extract_indiana_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_zip: str | Path | None = None,
    source_year: int = INDIANA_CODE_DEFAULT_YEAR,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = INDIANA_CODE_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official Indiana Code HTML and extract normalized provisions."""
    jurisdiction = "us-in"
    title_filter = _indiana_title_filter(only_title)
    run_id = _indiana_run_id(version, only_title=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    source_paths: list[Path] = []
    title_sources = tuple(
        _iter_indiana_title_sources(
            source_dir=Path(source_dir) if source_dir is not None else None,
            source_zip=Path(source_zip) if source_zip is not None else None,
            source_year=source_year,
            download_dir=Path(download_dir) if download_dir is not None else None,
            base_url=base_url,
            store=store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source_paths=source_paths,
        )
    )
    if title_filter is not None:
        title_sources = tuple(source for source in title_sources if source.title == title_filter)
    if not title_sources:
        raise ValueError(f"no Indiana Code title sources selected for filter: {only_title!r}")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    errors: list[str] = []
    remaining = limit

    for source in title_sources:
        if remaining is not None and remaining <= 0:
            break
        title_relative = _indiana_title_relative(source.title, source_year)
        artifact_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            title_relative,
        )
        sha256 = store.write_bytes(artifact_path, source.data)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, title_relative)
        source_url = _indiana_title_url(source.title, source_year, base_url)

        try:
            provisions = parse_indiana_title_html(source.data)
        except ValueError as exc:
            errors.append(f"title {source.title}: {exc}")
            continue
        if not provisions:
            errors.append(f"title {source.title}: no provisions parsed")
            continue

        for provision in provisions:
            if remaining is not None and remaining <= 0:
                break
            if provision.citation_path in seen:
                continue
            seen.add(provision.citation_path)
            items.append(
                _inventory_item(
                    provision,
                    source_url=source_url,
                    source_path=source_key,
                    sha256=sha256,
                    source_year=source_year,
                )
            )
            records.append(
                _provision_record(
                    provision,
                    version=run_id,
                    source_url=source_url,
                    source_path=source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    source_year=source_year,
                )
            )
            if provision.kind == "title":
                title_count += 1
            if provision.kind == "section":
                section_count += 1
            else:
                container_count += 1
            if remaining is not None:
                remaining -= 1

    if not items:
        raise ValueError("no Indiana Code provisions extracted")

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


def parse_indiana_title_html(html: str | bytes) -> tuple[IndianaCodeProvision, ...]:
    """Parse one official Indiana Code title HTML file."""
    soup = BeautifulSoup(html, "lxml")
    headings = [
        tag
        for tag in soup.find_all("div")
        if isinstance(tag, Tag) and _heading_kind(tag) is not None
    ]
    provisions: list[IndianaCodeProvision] = []
    current_title: str | None = None
    current_article: str | None = None
    current_chapter: str | None = None

    for ordinal, heading_tag in enumerate(headings):
        kind = _heading_kind(heading_tag)
        if kind is None:
            continue
        source_id = _clean_text(str(heading_tag.get("id") or ""))
        if not source_id:
            continue
        parsed_display_number = _display_number(heading_tag)
        heading = _short_description(heading_tag)
        if (
            parsed_display_number is None
            and heading is None
            and _next_heading_source_id(headings, ordinal) == source_id
        ):
            continue
        display_number = parsed_display_number or source_id
        parent: str | None = None
        level = _level_for_kind(kind)
        if kind == "title":
            current_title = source_id
            current_article = None
            current_chapter = None
        elif kind == "article":
            parent_id = current_title or _parent_from_display(display_number, 1)
            parent = f"us-in/statute/{parent_id}" if parent_id else None
            current_article = source_id
            current_chapter = None
        elif kind == "chapter":
            parent_id = current_article or _parent_from_display(display_number, 2)
            parent = f"us-in/statute/{parent_id}" if parent_id else None
            current_chapter = source_id
        elif kind == "section":
            parent_id = current_chapter or _parent_from_display(display_number, 3)
            parent = f"us-in/statute/{parent_id}" if parent_id else None

        body_lines, notes, history, derivation, references = _section_content(heading_tag)
        body = "\n".join(body_lines).strip() or None if kind == "section" else None
        self_path = f"us-in/statute/{source_id}"
        references_to = tuple(ref for ref in references if ref != self_path)
        provisions.append(
            IndianaCodeProvision(
                kind=kind,
                source_id=source_id,
                display_number=display_number,
                heading=heading,
                body=body,
                parent_citation_path=parent,
                level=level,
                ordinal=ordinal,
                references_to=tuple(dict.fromkeys(references_to)),
                notes=notes,
                source_history=history,
                derivation=derivation,
            )
        )
    return tuple(provisions)


@dataclass(frozen=True)
class _IndianaTitleSource:
    title: str
    data: bytes


def _iter_indiana_title_sources(
    *,
    source_dir: Path | None,
    source_zip: Path | None,
    source_year: int,
    download_dir: Path | None,
    base_url: str,
    store: CorpusArtifactStore,
    jurisdiction: str,
    run_id: str,
    source_paths: list[Path],
) -> Iterator[_IndianaTitleSource]:
    if source_dir is not None:
        yield from _iter_indiana_title_sources_from_dir(source_dir)
        return

    zip_bytes = _indiana_source_zip_bytes(
        source_zip=source_zip,
        source_year=source_year,
        download_dir=download_dir,
        base_url=base_url,
    )
    zip_relative = f"{INDIANA_CODE_SOURCE_FORMAT}/{source_year}-Indiana-Code-html.zip"
    zip_artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        zip_relative,
    )
    store.write_bytes(zip_artifact_path, zip_bytes)
    source_paths.append(zip_artifact_path)

    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        for member in _indiana_zip_title_members(archive, source_year):
            title = _title_from_html_name(member)
            yield _IndianaTitleSource(title=title, data=archive.read(member))


def _iter_indiana_title_sources_from_dir(source_dir: Path) -> Iterator[_IndianaTitleSource]:
    if not source_dir.exists():
        raise ValueError(f"Indiana source directory does not exist: {source_dir}")
    paths = [
        path
        for path in source_dir.rglob("*.html")
        if path.is_file() and re.fullmatch(r"\d+\.html", path.name)
    ]
    for path in sorted(paths, key=lambda item: int(item.stem)):
        yield _IndianaTitleSource(title=path.stem, data=path.read_bytes())


def _indiana_source_zip_bytes(
    *,
    source_zip: Path | None,
    source_year: int,
    download_dir: Path | None,
    base_url: str,
) -> bytes:
    if source_zip is not None:
        data = source_zip.read_bytes()
        _require_zip(data, source_zip.as_posix())
        return data

    source_url = _indiana_zip_url(source_year, base_url)
    if download_dir is not None:
        download_dir.mkdir(parents=True, exist_ok=True)
        cached_path = download_dir / Path(urlparse(source_url).path).name
        if cached_path.exists():
            data = cached_path.read_bytes()
            _require_zip(data, cached_path.as_posix())
            return data

    response = requests.get(
        source_url,
        headers={"User-Agent": INDIANA_USER_AGENT},
        timeout=120,
    )
    response.raise_for_status()
    data = response.content
    _require_zip(data, source_url)
    if download_dir is not None:
        with NamedTemporaryFile(dir=download_dir, delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)
        tmp_path.replace(cached_path)
    return data


def _require_zip(data: bytes, source: str) -> None:
    if not zipfile.is_zipfile(BytesIO(data)):
        raise ValueError(f"Indiana source did not return a ZIP archive: {source}")


def _indiana_zip_title_members(
    archive: zipfile.ZipFile,
    source_year: int,
) -> tuple[str, ...]:
    preferred_root = f"{source_year}_Indiana_Code_HTML"
    members = [
        name
        for name in archive.namelist()
        if "/" in name
        and Path(name).parent.name == preferred_root
        and re.fullmatch(r"\d+\.html", Path(name).name)
    ]
    if not members:
        members = [
            name
            for name in archive.namelist()
            if re.fullmatch(r"\d+\.html", Path(name).name)
        ]
    return tuple(sorted(members, key=lambda name: int(Path(name).stem)))


def _inventory_item(
    provision: IndianaCodeProvision,
    *,
    source_url: str,
    source_path: str,
    sha256: str,
    source_year: int,
) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=provision.citation_path,
        source_url=source_url,
        source_path=source_path,
        source_format=INDIANA_CODE_SOURCE_FORMAT,
        sha256=sha256,
        metadata=_metadata(provision, source_year=source_year),
    )


def _provision_record(
    provision: IndianaCodeProvision,
    *,
    version: str,
    source_url: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    source_year: int,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(provision.citation_path),
        jurisdiction="us-in",
        document_class=DocumentClass.STATUTE.value,
        citation_path=provision.citation_path,
        body=provision.body,
        heading=provision.heading,
        citation_label=provision.legal_identifier,
        version=version,
        source_url=source_url,
        source_path=source_path,
        source_id=provision.source_id,
        source_format=INDIANA_CODE_SOURCE_FORMAT,
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
        identifiers={
            "indiana:title": provision.title,
            f"indiana:{provision.kind}": provision.display_number,
            "indiana:source_id": provision.source_id,
        },
        metadata=_metadata(provision, source_year=source_year),
    )


def _metadata(provision: IndianaCodeProvision, *, source_year: int) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "title": provision.title,
        "source_year": source_year,
        "display_number": provision.display_number,
    }
    if provision.parent_citation_path:
        metadata["parent_citation_path"] = provision.parent_citation_path
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.notes:
        metadata["notes"] = list(provision.notes)
    if provision.source_history:
        metadata["source_history"] = list(provision.source_history)
    if provision.derivation:
        metadata["derivation"] = list(provision.derivation)
    status = _status(provision)
    if status:
        metadata["status"] = status
    return metadata


def _section_content(
    heading_tag: Tag,
) -> tuple[list[str], tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    if _heading_kind(heading_tag) != "section":
        return [], (), (), (), ()

    body_lines: list[str] = []
    notes: list[str] = []
    history: list[str] = []
    derivation: list[str] = []
    references: list[str] = []
    self_path = f"us-in/statute/{heading_tag.get('id')}"
    for sibling in heading_tag.next_siblings:
        if isinstance(sibling, NavigableString):
            if sibling.strip():
                text = _clean_text(str(sibling))
                if text:
                    body_lines.append(text)
            continue
        if not isinstance(sibling, Tag):
            continue
        if _heading_kind(sibling) is not None:
            break
        text = _clean_text(sibling.get_text(" ", strip=True))
        if not text:
            continue
        body_lines.append(text)
        if text.startswith("Note:"):
            notes.append(text)
        if "derivation" in set(sibling.get("class") or ()):
            derivation.append(text)
        if _looks_like_source_history(text):
            history.append(text)
        references.extend(_indiana_references(sibling, self_path=self_path))
        references.extend(_indiana_text_references(text, self_path=self_path))
    return (
        body_lines,
        tuple(dict.fromkeys(notes)),
        tuple(dict.fromkeys(history)),
        tuple(dict.fromkeys(derivation)),
        tuple(dict.fromkeys(references)),
    )


def _indiana_references(root: Tag, *, self_path: str) -> tuple[str, ...]:
    refs: list[str] = []
    for link in root.find_all("a"):
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "")
        if href.startswith("#"):
            ref = f"us-in/statute/{href[1:]}"
            if ref != self_path:
                refs.append(ref)
    return tuple(dict.fromkeys(refs))


def _indiana_text_references(text: str, *, self_path: str) -> tuple[str, ...]:
    refs: list[str] = []
    for match in _IC_TEXT_RE.finditer(text):
        ref = f"us-in/statute/{match.group('cite')}"
        if ref != self_path:
            refs.append(ref)
    return tuple(dict.fromkeys(refs))


def _looks_like_source_history(text: str) -> bool:
    return bool(
        re.match(
            r"^(?:As added|As amended|Formerly:|Repealed by|Amended by|"
            r"Renumbered|Recodified)",
            text,
            re.I,
        )
    )


def _status(provision: IndianaCodeProvision) -> str | None:
    heading = (provision.heading or "").lower()
    body = (provision.body or "").lower()
    if "repealed" in heading or body.startswith("repealed"):
        return "repealed"
    for note in provision.notes:
        normalized = note.lower()
        if re.match(r"note:\s*this version of section effective until\b", normalized):
            return "effective_until"
        if re.match(r"note:\s*this version of section effective\b", normalized):
            return "future_or_conditional"
    return None


def _heading_kind(tag: Tag) -> str | None:
    classes = set(tag.get("class") or ())
    for kind in _HEADING_KINDS:
        if kind in classes:
            return kind
    return None


def _next_heading_source_id(headings: list[Tag], index: int) -> str | None:
    for heading in headings[index + 1 :]:
        source_id = _clean_text(str(heading.get("id") or ""))
        if source_id:
            return source_id
    return None


def _display_number(tag: Tag) -> str | None:
    text = _span_text(tag, "ic_number")
    if not text:
        return None
    return re.sub(r"^IC\s+", "", text, flags=re.I).strip()


def _short_description(tag: Tag) -> str | None:
    return _span_text(tag, "shortdescription")


def _span_text(tag: Tag, span_id: str) -> str | None:
    span = tag.find("span", id=span_id)
    if not isinstance(span, Tag):
        return None
    text = _clean_text(span.get_text(" ", strip=True))
    return text or None


def _level_for_kind(kind: str) -> int:
    return {"title": 0, "article": 1, "chapter": 2, "section": 3}[kind]


def _parent_from_display(display_number: str, depth: int) -> str | None:
    parts = display_number.split("-")
    if len(parts) <= depth:
        return None
    return "-".join(parts[:depth])


def _title_from_html_name(value: str) -> str:
    name = Path(value).name
    if not re.fullmatch(r"\d+\.html", name):
        raise ValueError(f"not an Indiana title HTML file: {value}")
    return name.rsplit(".", 1)[0]


def _indiana_title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    if not match:
        raise ValueError(f"invalid Indiana title filter: {value!r}")
    return str(int(match.group(0)))


def _indiana_run_id(version: str, *, only_title: str | None, limit: int | None) -> str:
    if only_title is None and limit is None:
        return version
    parts = [version, "us-in"]
    if only_title is not None:
        parts.append(f"title-{only_title}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _indiana_title_relative(title: str, source_year: int) -> str:
    return f"{INDIANA_CODE_SOURCE_FORMAT}/{source_year}_Indiana_Code_HTML/{title}.html"


def _indiana_title_url(title: str, source_year: int, base_url: str) -> str:
    return urljoin(base_url, f"{source_year}/Title_{title}.html")


def _indiana_zip_url(source_year: int, base_url: str) -> str:
    return urljoin(base_url, f"{source_year}/{source_year}-Indiana-Code-html.zip")


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _clean_text(value: str | None) -> str:
    text = re.sub(r"\s+", " ", value or "").strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)


def indiana_source_zip_sha256(path: str | Path) -> str:
    """Return the source package checksum for release notes or manifests."""
    return sha256_bytes(Path(path).read_bytes())
