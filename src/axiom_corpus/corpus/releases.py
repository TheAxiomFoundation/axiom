"""Release manifests for named corpus scope sets."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from axiom_corpus.corpus.models import DocumentClass

ScopeKey = tuple[str, str, str]


@dataclass(frozen=True)
class ReleaseScope:
    jurisdiction: str
    document_class: str
    version: str

    @property
    def key(self) -> ScopeKey:
        return (self.jurisdiction, self.document_class, self.version)


@dataclass(frozen=True)
class ReleaseManifest:
    name: str
    scopes: tuple[ReleaseScope, ...]
    description: str | None = None

    @classmethod
    def load(cls, path: str | Path) -> ReleaseManifest:
        manifest_path = Path(path)
        data = _load_json_object(manifest_path)
        name = str(data.get("name") or manifest_path.stem)
        description_value = data.get("description")
        description = str(description_value) if description_value is not None else None
        raw_scopes = data.get("scopes")
        if not isinstance(raw_scopes, list):
            raise ValueError(f"Release manifest {manifest_path} must contain a scopes list")
        scopes = tuple(_parse_scope(scope, manifest_path=manifest_path) for scope in raw_scopes)
        _require_unique_scopes(scopes, manifest_path=manifest_path)
        return cls(name=name, description=description, scopes=scopes)

    @property
    def scope_keys(self) -> tuple[ScopeKey, ...]:
        return tuple(scope.key for scope in self.scopes)


def resolve_release_manifest_path(base: str | Path, release: str | Path) -> Path:
    """Resolve a release name or explicit path.

    Names such as "current" are resolved first under the artifact root and then
    under the tracked repository manifests directory.
    """
    release_path = Path(release)
    if release_path.exists() or release_path.suffix == ".json":
        return release_path
    candidates = (
        Path(base) / "releases" / f"{release}.json",
        Path("manifests") / "releases" / f"{release}.json",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text())
    except OSError as exc:
        raise FileNotFoundError(f"Release manifest not found: {path}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Release manifest {path} must be a JSON object")
    return data


def _parse_scope(raw_scope: Any, *, manifest_path: Path) -> ReleaseScope:
    if not isinstance(raw_scope, dict):
        raise ValueError(f"Release manifest {manifest_path} contains a non-object scope")
    jurisdiction = _required_string(raw_scope, "jurisdiction", manifest_path=manifest_path)
    document_class = _required_string(raw_scope, "document_class", manifest_path=manifest_path)
    version = _required_string(raw_scope, "version", manifest_path=manifest_path)
    try:
        DocumentClass(document_class)
    except ValueError as exc:
        raise ValueError(
            f"Release manifest {manifest_path} contains invalid document_class: {document_class}"
        ) from exc
    return ReleaseScope(
        jurisdiction=jurisdiction,
        document_class=document_class,
        version=version,
    )


def _required_string(data: dict[str, Any], key: str, *, manifest_path: Path) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Release manifest {manifest_path} scope missing {key}")
    return value


def _require_unique_scopes(scopes: Iterable[ReleaseScope], *, manifest_path: Path) -> None:
    seen: set[ScopeKey] = set()
    for scope in scopes:
        if scope.key in seen:
            raise ValueError(
                f"Release manifest {manifest_path} contains duplicate scope: "
                f"{scope.jurisdiction}/{scope.document_class}/{scope.version}"
            )
        seen.add(scope.key)
