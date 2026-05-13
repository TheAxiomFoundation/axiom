"""Path translation between rulespec-* repos and canonical corpus citation paths.

**No longer driven by the corpus pipeline.** The navigation builder used to
call into this module to compute `corpus.navigation_nodes.has_rulespec` by
walking local `rulespec-*` checkouts. That produced machine-dependent results
in production — the flag value depended on which laptop ran the rebuild and
which checkouts happened to be present. The CLI no longer invokes the walk;
encoded coverage is resolved by app consumers directly against GitHub
(see `axiom-foundation.org/src/lib/axiom/rulespec/repo-listing.ts`).

The module is kept as a library: the path-translation helpers
(`_repo_path_to_citation_path`, `_normalize_tail`) and the
`JURISDICTION_REPO_MAP` are still useful for anything that needs to convert
between repo-relative YAML paths and canonical citation paths. The
filesystem-walk functions (`discover_encoded_paths`,
`discover_encoded_paths_for_jurisdictions`) remain callable for development
use but are no longer wired into the navigation build path.

Follow-up: drop the `has_rulespec` column from `corpus.navigation_nodes` once
the foundation.org sitemap and document-browser consumers have migrated to
the GitHub-backed lookup. At that point this module can be deleted entirely.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

# Canonical jurisdiction slug -> rulespec-* repo directory name.
# Mirrors axiom-foundation.org/src/lib/axiom/repo-map.ts; keep in sync when
# new jurisdictions get rules repos.
JURISDICTION_REPO_MAP: dict[str, str] = {
    "us": "rulespec-us",
    "uk": "rulespec-uk",
    "canada": "rulespec-ca",
    "us-al": "rulespec-us-al",
    "us-ar": "rulespec-us-ar",
    "us-ca": "rulespec-us-ca",
    "us-co": "rulespec-us-co",
    "us-fl": "rulespec-us-fl",
    "us-ga": "rulespec-us-ga",
    "us-md": "rulespec-us-md",
    "us-nc": "rulespec-us-nc",
    "us-ny": "rulespec-us-ny",
    "us-sc": "rulespec-us-sc",
    "us-tn": "rulespec-us-tn",
    "us-tx": "rulespec-us-tx",
}

# Top-level bucket directory name -> citation_path bucket segment.
# Plural in the rules repo convention; singular in the corpus convention.
BUCKET_TO_CITATION_BUCKET: dict[str, str] = {
    "statutes": "statute",
    "regulations": "regulation",
    "policies": "policy",
}

# File suffixes that are RuleSpec source. Anything else (.test.yaml,
# .meta.yaml, README.md, scripts/, etc.) is skipped.
_EXCLUDED_SUFFIXES: tuple[str, ...] = (".test.yaml", ".meta.yaml")


def discover_encoded_paths(
    repo_root: str | Path,
    jurisdiction: str,
) -> set[str]:
    """Walk ``repo_root`` and return canonical corpus citation paths.

    A path qualifies when its file ends in ``.yaml`` and is not a test or
    meta overlay. Buckets outside `BUCKET_TO_CITATION_BUCKET` (e.g. an
    accidental ``scripts/`` checkin) pass through unchanged so callers can
    spot weird shapes in the data without crashing the build.

    Returns an empty set when the repo path doesn't exist — keeps the
    builder resilient to a missing optional checkout.
    """
    root = Path(repo_root)
    if not root.is_dir():
        return set()

    encoded: set[str] = set()
    for yaml_path in root.rglob("*.yaml"):
        rel = yaml_path.relative_to(root).as_posix()
        if _is_excluded(rel):
            continue
        if _is_under_hidden_or_tests(rel):
            continue
        citation = _repo_path_to_citation_path(rel, jurisdiction)
        if citation is not None:
            encoded.add(citation)
    return encoded


def discover_encoded_paths_for_jurisdictions(
    rulespec_root: str | Path,
    jurisdictions: Iterable[str],
) -> dict[str, set[str]]:
    """Discover encoded paths for several jurisdictions under one root dir.

    ``rulespec_root`` is the parent directory containing sibling
    ``rulespec-us``, ``rulespec-us-co``, ``rulespec-ca`` checkouts. Jurisdictions
    that don't have an entry in ``JURISDICTION_REPO_MAP`` (or whose repo
    isn't on disk) get an empty set.
    """
    root = Path(rulespec_root)
    out: dict[str, set[str]] = {}
    for jurisdiction in jurisdictions:
        repo_dir_name = JURISDICTION_REPO_MAP.get(jurisdiction)
        if repo_dir_name is None:
            out[jurisdiction] = set()
            continue
        candidate = root / repo_dir_name
        out[jurisdiction] = discover_encoded_paths(candidate, jurisdiction)
    return out


def _is_excluded(relative_path: str) -> bool:
    return relative_path.endswith(_EXCLUDED_SUFFIXES)


def _is_under_hidden_or_tests(relative_path: str) -> bool:
    parts = relative_path.split("/")
    if not parts:
        return False
    # Skip dotfiles/dirs and the conventional ``tests/`` fixture root.
    return any(part.startswith(".") or part == "tests" for part in parts[:-1])


def _repo_path_to_citation_path(relative_path: str, jurisdiction: str) -> str | None:
    """Translate ``statutes/7/2014/e/2.yaml`` into ``us/statute/7/2014/e/2``.

    Returns ``None`` for paths that don't have a leading bucket segment we
    recognise as containing RuleSpec encodings (e.g. a stray top-level
    ``CLAUDE.md`` ignored above, or a file directly at the repo root).
    """
    if not relative_path.endswith(".yaml"):
        return None
    stripped = relative_path[: -len(".yaml")]
    segments = stripped.split("/")
    if len(segments) < 2:
        return None
    repo_bucket = segments[0]
    citation_bucket = BUCKET_TO_CITATION_BUCKET.get(repo_bucket, repo_bucket)
    tail = list(segments[1:])
    tail = _normalize_tail(tail, jurisdiction=jurisdiction, repo_bucket=repo_bucket)
    if not tail:
        return f"{jurisdiction}/{citation_bucket}"
    return f"{jurisdiction}/{citation_bucket}/" + "/".join(tail)


def _normalize_tail(
    tail: list[str],
    *,
    jurisdiction: str,
    repo_bucket: str,
) -> list[str]:
    """Apply jurisdiction-specific tweaks so paths agree with the corpus.

    ``rulespec-us/regulations/7-cfr/...`` lands as ``us/regulation/7/...`` in the
    corpus — the publication-system suffix gets dropped on the title.
    Mirrors the app's ``normaliseTitleSegment``.
    """
    if not tail:
        return tail
    if jurisdiction == "us" and repo_bucket == "regulations":
        tail = list(tail)
        tail[0] = tail[0].removesuffix("-cfr")
    return tail
