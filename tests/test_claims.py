from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CLAIMS_ROOT = ROOT / "claims"
FRIENDLY_CONCEPT_ID = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")


def iter_claim_records() -> list[tuple[Path, int, dict[str, Any]]]:
    records: list[tuple[Path, int, dict[str, Any]]] = []
    if not CLAIMS_ROOT.exists():
        return records

    for path in sorted(CLAIMS_ROOT.rglob("*.jsonl")):
        for line_number, line in enumerate(path.read_text().splitlines(), start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            assert isinstance(payload, dict)
            records.append((path, line_number, payload))
    return records


def test_claim_subjects_use_legal_or_rulespec_pointers() -> None:
    invalid: list[str] = []

    for path, line_number, claim in iter_claim_records():
        subject = claim.get("subject")
        if not isinstance(subject, dict):
            invalid.append(f"{path.relative_to(ROOT)}:{line_number}: missing subject")
            continue

        subject_type = str(subject.get("type") or "")
        subject_id = str(subject.get("id") or "")
        if subject_type == "concept":
            invalid.append(
                f"{path.relative_to(ROOT)}:{line_number}: concept subject `{subject_id}`"
            )
        if FRIENDLY_CONCEPT_ID.match(subject_id):
            invalid.append(
                f"{path.relative_to(ROOT)}:{line_number}: friendly subject id `{subject_id}`"
            )

    assert invalid == []
