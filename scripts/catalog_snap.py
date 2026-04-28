#!/usr/bin/env python3
"""
Catalog SNAP documents in Supabase.

This script creates source and version records for SNAP statute documents
and USDA FNS guidance that have been uploaded to R2.
"""

import hashlib
import os
from datetime import datetime
from pathlib import Path

import requests


def compute_file_hash(filepath: Path) -> str:
    """Compute SHA-256 hash of file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def get_file_size(filepath: Path) -> int:
    """Get file size in bytes."""
    return filepath.stat().st_size


def get_mime_type(filepath: Path) -> str:
    """Get MIME type based on file extension."""
    suffix = filepath.suffix.lower()
    mime_types = {
        ".xml": "application/xml",
        ".pdf": "application/pdf",
        ".html": "text/html",
    }
    return mime_types.get(suffix, "application/octet-stream")


def catalog_document(
    supabase_url: str,
    service_key: str,
    path: str,
    jurisdiction: str,
    doc_type: str,
    source_url: str,
    title: str,
    r2_key: str,
    local_filepath: Path,
    published_at: str = None,
    applies_from_year: int = None,
    applies_to_year: int = None,
    is_current: bool = True,
) -> dict:
    """
    Catalog a document in Supabase.

    Returns: dict with source_id and version_id
    """
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    # 1. Create or get source record
    source_data = {
        "path": path,
        "jurisdiction": jurisdiction,
        "doc_type": doc_type,
        "source_url": source_url,
        "title": title,
        "crawl_enabled": True,
        "last_crawl_at": datetime.now().isoformat(),
    }

    # Try to insert, on conflict do nothing and get existing
    response = requests.post(
        f"{supabase_url}/rest/v1/sources",
        headers=headers,
        json=source_data,
    )

    if response.status_code == 201:
        source = response.json()[0]
        print(f"Created source: {path} (ID: {source['id']})")
    elif response.status_code == 409:
        # Already exists, fetch it
        response = requests.get(
            f"{supabase_url}/rest/v1/sources",
            headers=headers,
            params={"path": f"eq.{path}"},
        )
        source = response.json()[0]
        print(f"Source already exists: {path} (ID: {source['id']})")
    else:
        print(f"Error creating source: {response.status_code} {response.text}")
        return None

    source_id = source["id"]

    # 2. Compute file metadata
    content_hash = compute_file_hash(local_filepath)
    file_size = get_file_size(local_filepath)
    mime_type = get_mime_type(local_filepath)

    # 3. Create version record
    version_data = {
        "source_id": source_id,
        "content_hash": content_hash,
        "r2_key": r2_key,
        "file_size_bytes": file_size,
        "mime_type": mime_type,
        "published_at": published_at,
        "retrieved_at": datetime.now().isoformat(),
        "applies_from_year": applies_from_year,
        "applies_to_year": applies_to_year,
        "is_current": is_current,
    }

    response = requests.post(
        f"{supabase_url}/rest/v1/versions",
        headers=headers,
        json=version_data,
    )

    if response.status_code == 201:
        version = response.json()[0]
        print(f"  Created version: {content_hash[:8]}... (ID: {version['id']})")
    elif response.status_code == 409:
        # Version already exists
        response = requests.get(
            f"{supabase_url}/rest/v1/versions",
            headers=headers,
            params={
                "source_id": f"eq.{source_id}",
                "content_hash": f"eq.{content_hash}",
            },
        )
        version = response.json()[0]
        print(f"  Version already exists: {content_hash[:8]}... (ID: {version['id']})")
    else:
        print(f"Error creating version: {response.status_code} {response.text}")
        return {"source_id": source_id}

    return {"source_id": source_id, "version_id": version["id"]}


def main():
    """Catalog all SNAP documents."""
    supabase_url = os.environ["AXIOM_SUPABASE_URL"]
    service_key = os.environ["AXIOM_SUPABASE_SERVICE_KEY"]

    data_dir = Path(__file__).parent.parent / "data" / "snap"

    results = []

    # 1. 7 USC Chapter 51 (full chapter)
    result = catalog_document(
        supabase_url=supabase_url,
        service_key=service_key,
        path="us/statute/7/51",
        jurisdiction="us",
        doc_type="statute",
        source_url="https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title7-chapter51&num=0&edition=prelim",
        title="7 USC Chapter 51 - Food Stamp Program (SNAP)",
        r2_key="us/statute/7/51/chapter51.xml",
        local_filepath=data_dir / "usc07-chapter51.xml",
        is_current=True,
    )
    results.append(result)

    # 2. 7 USC § 2014 (eligibility)
    result = catalog_document(
        supabase_url=supabase_url,
        service_key=service_key,
        path="us/statute/7/51/usc2014",
        jurisdiction="us",
        doc_type="statute",
        source_url="https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title7-section2014&num=0&edition=prelim",
        title="7 USC § 2014 - Eligible households",
        r2_key="us/statute/7/51/usc2014.xml",
        local_filepath=data_dir / "usc2014.xml",
        is_current=True,
    )
    results.append(result)

    # 3. 7 USC § 2017 (value of allotment)
    result = catalog_document(
        supabase_url=supabase_url,
        service_key=service_key,
        path="us/statute/7/51/usc2017",
        jurisdiction="us",
        doc_type="statute",
        source_url="https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title7-section2017&num=0&edition=prelim",
        title="7 USC § 2017 - Value of allotment",
        r2_key="us/statute/7/51/usc2017.xml",
        local_filepath=data_dir / "usc2017.xml",
        is_current=True,
    )
    results.append(result)

    # 4. SNAP FY2024 COLA guidance
    result = catalog_document(
        supabase_url=supabase_url,
        service_key=service_key,
        path="us/guidance/usda/fns/snap-fy2024-cola",
        jurisdiction="us",
        doc_type="guidance",
        source_url="https://www.fns.usda.gov/snap/fy-2024-cola",
        title="SNAP FY 2024 Cost-of-Living Adjustments",
        r2_key="us/guidance/usda/fns/snap-fy2024-cola.pdf",
        local_filepath=data_dir / "snap-fy2024-cola.pdf",
        published_at="2023-08-01",
        applies_from_year=2024,
        applies_to_year=2024,
        is_current=False,
    )
    results.append(result)

    # 5. SNAP FY2025 COLA guidance
    result = catalog_document(
        supabase_url=supabase_url,
        service_key=service_key,
        path="us/guidance/usda/fns/snap-fy2025-cola",
        jurisdiction="us",
        doc_type="guidance",
        source_url="https://www.fns.usda.gov/snap/fy-2025-cola",
        title="SNAP FY 2025 Cost-of-Living Adjustments",
        r2_key="us/guidance/usda/fns/snap-fy2025-cola.pdf",
        local_filepath=data_dir / "snap-fy2025-cola.pdf",
        published_at="2024-08-01",
        applies_from_year=2025,
        applies_to_year=None,  # Still current
        is_current=True,
    )
    results.append(result)

    print("\n" + "=" * 70)
    print("CATALOG COMPLETE")
    print("=" * 70)
    print(f"Total documents cataloged: {len(results)}")
    print("\nDocument IDs:")
    for i, result in enumerate(results, 1):
        if result:
            print(f"{i}. Source: {result['source_id']}, Version: {result.get('version_id', 'N/A')}")


if __name__ == "__main__":
    main()
