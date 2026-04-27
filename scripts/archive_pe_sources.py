#!/usr/bin/env python3
"""Archive PolicyEngine-US source documents.

Downloads PDFs and other documents referenced in PolicyEngine-US parameters.
"""

import hashlib
import json
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx

# State abbreviation mapping from domain patterns
DOMAIN_TO_STATE = {
    "revenue.state.mn.us": "mn",
    "michigan.gov": "mi",
    "revenue.wi.gov": "wi",
    "tax.virginia.gov": "va",
    "dfa.arkansas.gov": "ar",
    "tax.vermont.gov": "vt",
    "maine.gov": "me",
    "tax.colorado.gov": "co",
    "files.hawaii.gov": "hi",
    "oregon.gov": "or",
    "tax.ny.gov": "ny",
    "mtrevenue.gov": "mt",
    "marylandtaxes.gov": "md",
    "tax.ri.gov": "ri",
    "azdor.gov": "az",
    "portal.ct.gov": "ct",
    "dor.sc.gov": "sc",
    "tax.idaho.gov": "id",
    "otr.cfo.dc.gov": "dc",
    "ksrevenue.gov": "ks",
    "state.nj.us": "nj",
    "dor.mo.gov": "mo",
    "dor.georgia.gov": "ga",
    "tax.nd.gov": "nd",
    "ncdor.gov": "nc",
    "dam.assets.ohio.gov": "oh",
    "azleg.gov": "az",
    "ftb.ca.gov": "ca",
    "mass.gov": "ma",
    "tax.iowa.gov": "ia",
    "revenue.nebraska.gov": "ne",
    "tax.wv.gov": "wv",
    "oklahoma.gov": "ok",
    "nysenate.gov": "ny",
    "le.utah.gov": "ut",
    "dpss.lacounty.gov": "ca",
    "dpw.state.pa.us": "pa",
    "sandiegocounty.gov": "ca",
    "leg.wa.gov": "wa",
    "revenue.pa.gov": "pa",
    "legislature.mi.gov": "mi",
    "iga.in.gov": "in",
    "legis.la.gov": "la",
    "kslegislature.org": "ks",
    "rilin.state.ri.us": "ri",
    "scstatehouse.gov": "sc",
    "legislature.vermont.gov": "vt",
}


def get_state_from_url(url: str) -> str:
    """Extract state abbreviation from URL domain."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower().replace("www.", "")

    for pattern, state in DOMAIN_TO_STATE.items():
        if pattern in domain:
            return state

    return "other"


def url_to_filename(url: str) -> str:
    """Convert URL to a safe filename."""
    parsed = urlparse(url)
    path = parsed.path

    # Get the filename from the path
    if path.endswith(".pdf"):
        filename = Path(path).name
    else:
        # Use hash for complex URLs
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        filename = f"{url_hash}.html"

    # Clean up filename
    filename = re.sub(r"[^\w\-_\.]", "_", filename)
    return filename


def download_url(
    client: httpx.Client,
    url: str,
    output_dir: Path,
    progress_callback=None,
) -> dict:
    """Download a single URL."""
    state = get_state_from_url(url)
    state_dir = output_dir / state
    state_dir.mkdir(parents=True, exist_ok=True)

    filename = url_to_filename(url)
    output_file = state_dir / filename

    # Skip if already exists
    if output_file.exists():
        return {"status": "skipped", "file": str(output_file)}

    try:
        response = client.get(url, follow_redirects=True)
        response.raise_for_status()

        output_file.write_bytes(response.content)
        return {
            "status": "downloaded",
            "file": str(output_file),
            "size": len(response.content),
        }
    except Exception as e:
        return {"status": "failed", "error": str(e)}


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Archive PolicyEngine-US sources")
    parser.add_argument(
        "--urls-file",
        default="sources/policyengine-us/state_references.txt",
        help="File containing URLs to download",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path.home() / ".arch" / "policyengine-us"),
        help="Output directory",
    )
    parser.add_argument(
        "--pdf-only",
        action="store_true",
        help="Only download PDFs",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of downloads (0 = no limit)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.5,
        help="Seconds between requests",
    )
    args = parser.parse_args()

    # Read URLs
    urls_file = Path(args.urls_file)
    if not urls_file.exists():
        urls_file = Path(__file__).parent.parent / args.urls_file

    with open(urls_file) as f:
        urls = [line.strip() for line in f if line.strip()]

    if args.pdf_only:
        urls = [u for u in urls if ".pdf" in u.lower()]

    if args.limit > 0:
        urls = urls[: args.limit]

    print(f"Downloading {len(urls)} URLs to {args.output_dir}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}

    client = httpx.Client(
        timeout=60,
        headers={
            "User-Agent": "Atlas/1.0 (policy document archiver; contact@axiom-foundation.org)"
        },
        follow_redirects=True,
    )

    try:
        for i, url in enumerate(urls, 1):
            result = download_url(client, url, output_dir)
            stats[result["status"]] += 1

            if result["status"] == "downloaded":
                size_kb = result.get("size", 0) // 1024
                print(f"[{i}/{len(urls)}] {result['file']} ({size_kb}KB)")
            elif result["status"] == "failed":
                print(f"[{i}/{len(urls)}] FAILED: {url[:60]}... - {result['error']}")

            time.sleep(args.rate_limit)
    finally:
        client.close()

    print(f"\nDone: {stats}")

    # Save manifest
    manifest_file = output_dir / "manifest.json"
    manifest = {
        "source": str(urls_file),
        "total_urls": len(urls),
        "stats": stats,
    }
    manifest_file.write_text(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
