#!/usr/bin/env python3
"""Ingest all Canada federal acts into Supabase with robust error handling."""

import os
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from axiom.models_canada import CanadaSection
from axiom.parsers.canada import CanadaStatuteParser

# Configuration
DEFAULT_AXIOM_SUPABASE_URL = "https://swocpijqqahhuwtuahwc.supabase.co"
SUPABASE_URL = os.environ.get("AXIOM_SUPABASE_URL", DEFAULT_AXIOM_SUPABASE_URL)
AXIOM_PATH = Path.home() / ".axiom"
BATCH_SIZE = 50  # Smaller batch for reliability
MAX_RETRIES = 5


def get_service_key():
    """Get Supabase service role key from Management API."""
    access_token = os.environ.get("SUPABASE_ACCESS_TOKEN")
    if not access_token:
        raise ValueError("SUPABASE_ACCESS_TOKEN env var required")

    for attempt in range(3):
        try:
            with httpx.Client(timeout=60.0) as client:
                project_ref = SUPABASE_URL.split("//", 1)[1].split(".", 1)[0]
                response = client.get(
                    f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                response.raise_for_status()
                keys = response.json()
                for k in keys:
                    if k.get("name") == "service_role" and k.get("api_key"):
                        return k["api_key"]
        except Exception as e:
            print(f"Retry {attempt + 1}/3 getting key: {e}", flush=True)
            time.sleep(2)

    raise ValueError("Could not get service_role key")


def section_to_rules(section: CanadaSection, parent_id=None):
    """Convert a CanadaSection to rule dictionaries."""
    section_id = str(uuid4())

    ordinal = None
    try:
        if section.section_number and section.section_number.replace(".", "").isdigit():
            ordinal = int(section.section_number.split(".")[0])
    except ValueError, AttributeError:
        pass

    yield {
        "id": section_id,
        "jurisdiction": "canada",
        "doc_type": "statute",
        "parent_id": parent_id,
        "level": 0,
        "ordinal": ordinal,
        "heading": section.marginal_note,
        "body": section.text,
        "effective_date": section.in_force_date.isoformat() if section.in_force_date else None,
        "source_url": section.source_url,
        "source_path": section.source_path,
        "rulespec_path": None,
        "has_rulespec": False,
    }

    yield from subsections_to_rules(section.subsections, section_id, 1)


def subsections_to_rules(subsections, parent_id, level):
    """Convert subsections to rule dictionaries recursively."""
    for i, sub in enumerate(subsections):
        sub_id = str(uuid4())

        yield {
            "id": sub_id,
            "jurisdiction": "canada",
            "doc_type": "statute",
            "parent_id": parent_id,
            "level": level,
            "ordinal": i + 1,
            "heading": sub.marginal_note,
            "body": sub.text,
            "effective_date": None,
            "source_url": None,
            "source_path": None,
            "rulespec_path": None,
            "has_rulespec": False,
        }

        if sub.children:
            yield from subsections_to_rules(sub.children, sub_id, level + 1)


def insert_rules(rules, client, key, rest_url):
    """Insert rules into Supabase with retry logic."""
    if not rules:
        return 0

    for attempt in range(MAX_RETRIES):
        try:
            response = client.post(
                f"{rest_url}/provisions",
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json=rules,
            )
            response.raise_for_status()
            return len(rules)
        except httpx.HTTPStatusError as e:
            if e.response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                raise
        except httpx.ReadTimeout, httpx.ConnectTimeout:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                raise
    return 0


def ingest_act(cons_num, client, key, rest_url):
    """Ingest a single Canada act."""
    xml_path = AXIOM_PATH / "canada" / f"{cons_num}.xml"
    if not xml_path.exists():
        return 0

    parser = CanadaStatuteParser(xml_path)
    total_inserted = 0
    batch = []

    for section in parser.iter_sections():
        for rule in section_to_rules(section):
            batch.append(rule)
            if len(batch) >= BATCH_SIZE:
                try:
                    inserted = insert_rules(batch, client, key, rest_url)
                    total_inserted += inserted
                except Exception as e:
                    print(f"    Error batch: {e}", flush=True)
                batch = []

    if batch:
        try:
            inserted = insert_rules(batch, client, key, rest_url)
            total_inserted += inserted
        except Exception as e:
            print(f"    Error final batch: {e}", flush=True)

    return total_inserted


def main():
    print("Getting Supabase API key...", flush=True)
    key = get_service_key()
    rest_url = f"{SUPABASE_URL}/rest/v1"
    print("Got API key!", flush=True)

    # Get all Canada XML files
    canada_path = AXIOM_PATH / "canada"
    xml_files = sorted(canada_path.glob("*.xml"))
    total_files = len(xml_files)

    print(f"Found {total_files} Canada federal acts", flush=True)
    print("=" * 60, flush=True)

    total_rules = 0
    errors = 0

    with httpx.Client(timeout=300.0) as client:
        for i, xml_file in enumerate(xml_files):
            cons_num = xml_file.stem
            try:
                count = ingest_act(cons_num, client, key, rest_url)
                total_rules += count
                print(
                    f"[{i + 1}/{total_files}] {cons_num}: {count} rules (total: {total_rules})",
                    flush=True,
                )
            except Exception as e:
                errors += 1
                print(f"[{i + 1}/{total_files}] {cons_num}: ERROR - {e}", flush=True)

    print("", flush=True)
    print("=" * 60, flush=True)
    print("FINAL RESULTS", flush=True)
    print("=" * 60, flush=True)
    print(f"Total acts processed: {total_files}", flush=True)
    print(f"Total rules inserted: {total_rules}", flush=True)
    print(f"Total errors: {errors}", flush=True)


if __name__ == "__main__":
    main()
