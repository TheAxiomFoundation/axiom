"""Drive ingestion of every part in every CFR title via the eCFR Versioner.

Walks the structure JSON for each title, extracts every ``(chapter,
subchapter, part)`` triple, and reuses :mod:`scripts.ingest_cfr_parts` to
fetch the XML, build rows, and upsert into Supabase ``corpus.provisions``.

Design
------
* **Resilience over speed.** Per-part failures log and continue; one broken
  title doesn't block the rest. Progress is streamed to stdout so a tail
  makes sense as a live report.
* **No reserved titles.** Title 35 is historically reserved; the driver
  still calls the structure endpoint — if eCFR returns no parts, it moves
  on without erroring.
* **Deterministic UUIDs.** Every row keyed on ``uuid5(citation_path)``;
  safe to re-run after a partial failure.
* **Backoff on network errors.** Exponential, capped at 30s, max 3 retries
  per part. Avoids hammering eCFR if it's flaky.

Usage
-----
::

    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_all_cfr.py

    # Resume from a specific title
    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_all_cfr.py --start-title 26

    # Run one title
    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_all_cfr.py --only-title 26

    # Dry-run: walk the structure and print what WOULD be ingested without
    # fetching part XML or writing to Supabase.
    uv run python scripts/ingest_all_cfr.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

# Reuse the per-part module's building blocks.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingest_cfr_parts import (  # noqa: E402 — sys.path munge above
    DEFAULT_AS_OF,
    USER_AGENT,
    build_rows,
    chunked,
    fetch_part_xml,
    get_service_key,
    refresh_corpus_analytics,
    upsert_rows,
)

ECFR_STRUCTURE_URL = "https://www.ecfr.gov/api/versioner/v1/structure/{as_of}/title-{title}.json"
MAX_RETRIES = 3
BACKOFF_BASE_S = 2.0
BACKOFF_CAP_S = 30.0


@dataclass
class PartTarget:
    title: int
    chapter: str | None
    subchapter: str | None
    part: str

    def __str__(self) -> str:
        prefix = ""
        if self.chapter:
            prefix = f"Ch.{self.chapter}"
        if self.subchapter:
            prefix = f"{prefix}/Subch.{self.subchapter}" if prefix else f"Subch.{self.subchapter}"
        return f"{self.title} CFR {prefix + ' → ' if prefix else ''}Part {self.part}"


def fetch_structure(title: int, as_of: str) -> dict:
    url = ECFR_STRUCTURE_URL.format(as_of=as_of, title=title)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def walk_parts(
    node: dict,
    title: int,
    chapter: str | None = None,
    subchapter: str | None = None,
) -> list[PartTarget]:
    """DFS the structure tree, yielding every PART with its ancestry."""
    out: list[PartTarget] = []
    node_type = node.get("type")
    ident = node.get("identifier")
    if node_type == "chapter":
        chapter = ident
    elif node_type == "subchapter":
        subchapter = ident
    elif node_type == "part":
        # Some structures mark reserved parts with `reserved: true`.
        if not node.get("reserved"):
            out.append(PartTarget(title=title, chapter=chapter, subchapter=subchapter, part=ident))
        return out  # don't recurse into sections/subparts here

    for child in node.get("children", []) or []:
        out.extend(walk_parts(child, title, chapter, subchapter))
    return out


def ingest_title(
    title: int,
    as_of: str,
    service_key: str | None,
    dry_run: bool,
) -> tuple[int, int, int]:
    """Ingest every part of a single title.

    Returns (parts_attempted, parts_succeeded, rows_upserted).
    """
    print(f"\n=== Title {title} CFR ===", flush=True)
    try:
        structure = fetch_structure(title, as_of)
    except urllib.error.HTTPError as exc:
        print(f"  STRUCTURE FAILED: HTTP {exc.code} — skipping title", flush=True)
        return (0, 0, 0)
    except Exception as exc:
        print(f"  STRUCTURE FAILED: {exc} — skipping title", flush=True)
        return (0, 0, 0)

    targets = walk_parts(structure, title)
    if not targets:
        print("  (no live parts — reserved or empty title)", flush=True)
        return (0, 0, 0)

    print(f"  {len(targets)} parts discovered", flush=True)

    succeeded = 0
    rows_total = 0
    for idx, t in enumerate(targets, start=1):
        row_count = ingest_one_part(t, as_of, service_key, dry_run, idx, len(targets))
        if row_count is not None:
            succeeded += 1
            rows_total += row_count
    return (len(targets), succeeded, rows_total)


def ingest_one_part(
    target: PartTarget,
    as_of: str,
    service_key: str | None,
    dry_run: bool,
    idx: int,
    total: int,
) -> int | None:
    """Return row count on success, None on failure."""
    tag = f"[{idx:3d}/{total}] {target}"
    # Parts are sometimes numeric-with-suffix (e.g. "1a"); accept the raw string.
    try:
        part_num = int(target.part)
    except ValueError:
        print(f"  {tag} — non-numeric part id, skipping", flush=True)
        return None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            root = fetch_part_xml(target.title, part_num, as_of)
            break
        except urllib.error.HTTPError as exc:
            if exc.code in (404, 410):
                print(f"  {tag} — HTTP {exc.code}, skipping", flush=True)
                return None
            if attempt == MAX_RETRIES:
                print(f"  {tag} — HTTP {exc.code} after {attempt} tries, giving up", flush=True)
                return None
            sleep_s = min(BACKOFF_CAP_S, BACKOFF_BASE_S**attempt)
            print(f"  {tag} — HTTP {exc.code}, retrying in {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s)
        except Exception as exc:
            if attempt == MAX_RETRIES:
                print(f"  {tag} — {exc} after {attempt} tries, giving up", flush=True)
                return None
            sleep_s = min(BACKOFF_CAP_S, BACKOFF_BASE_S**attempt)
            print(f"  {tag} — {exc}, retrying in {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s)
    else:
        return None

    rows = build_rows(
        target.title,
        part_num,
        root,
        chapter=target.chapter,
        subchapter=target.subchapter,
    )
    if not rows:
        print(f"  {tag} — 0 rows built", flush=True)
        return 0

    if dry_run:
        print(f"  {tag} — would upsert {len(rows)} rows (dry-run)", flush=True)
        return len(rows)

    assert service_key is not None
    try:
        for batch in chunked(rows):
            upsert_rows(batch, service_key)
    except Exception as exc:
        print(f"  {tag} — upsert failed: {exc}", flush=True)
        return None
    print(f"  {tag} — {len(rows)} rows", flush=True)
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--as-of", default=DEFAULT_AS_OF)
    parser.add_argument("--start-title", type=int, default=1)
    parser.add_argument("--end-title", type=int, default=50)
    parser.add_argument("--only-title", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    service_key = None if args.dry_run else get_service_key()

    if args.only_title is not None:
        titles = [args.only_title]
    else:
        titles = list(range(args.start_title, args.end_title + 1))

    total_parts = 0
    total_rows = 0
    total_succeeded = 0
    started = time.time()

    for title in titles:
        attempted, succeeded, rows = ingest_title(title, args.as_of, service_key, args.dry_run)
        total_parts += attempted
        total_succeeded += succeeded
        total_rows += rows
        elapsed = time.time() - started
        print(
            f"  running total: {total_succeeded}/{total_parts} parts, "
            f"{total_rows} rows, {elapsed / 60:.1f} min elapsed",
            flush=True,
        )

    if not args.dry_run and total_rows > 0:
        assert service_key is not None
        refresh_corpus_analytics(service_key)
    print(
        f"\nDONE — {total_succeeded}/{total_parts} parts, {total_rows} rows, "
        f"{(time.time() - started) / 60:.1f} min",
        flush=True,
    )
    return 0 if total_succeeded == total_parts else 1


if __name__ == "__main__":
    raise SystemExit(main())
