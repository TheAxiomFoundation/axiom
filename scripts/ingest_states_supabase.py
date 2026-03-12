#!/usr/bin/env python3
"""Ingest all state statutes into Supabase arch.rules table.

This script uses the existing state converters to parse local HTML files
(already scraped) and uploads the resulting sections to Supabase via
the SupabaseIngestor.

Usage:
    # All states with local data:
    python scripts/ingest_states_supabase.py

    # Specific state:
    python scripts/ingest_states_supabase.py --state oh

    # Dry run (parse only, no upload):
    python scripts/ingest_states_supabase.py --dry-run
"""

import argparse
import importlib
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.ingest.supabase import SupabaseIngestor

# Map state codes to converter modules
STATE_CONVERTERS = {
    'ak': 'atlas.converters.us_states.ak',
    'al': 'atlas.converters.us_states.al',
    'ar': 'atlas.converters.us_states.ar',
    'az': 'atlas.converters.us_states.az',
    'ca': 'atlas.converters.us_states.ca',
    'co': 'atlas.converters.us_states.co',
    'ct': 'atlas.converters.us_states.ct',
    'de': 'atlas.converters.us_states.de',
    'fl': 'atlas.converters.us_states.fl',
    'hi': 'atlas.converters.us_states.hi',
    'id': 'atlas.converters.us_states.id_',
    'il': 'atlas.converters.us_states.il',
    'in': 'atlas.converters.us_states.in_',
    'ks': 'atlas.converters.us_states.ks',
    'ky': 'atlas.converters.us_states.ky',
    'la': 'atlas.converters.us_states.la',
    'ma': 'atlas.converters.us_states.ma',
    'md': 'atlas.converters.us_states.md',
    'me': 'atlas.converters.us_states.me',
    'mi': 'atlas.converters.us_states.mi',
    'mn': 'atlas.converters.us_states.mn',
    'mo': 'atlas.converters.us_states.mo',
    'ms': 'atlas.converters.us_states.ms',
    'mt': 'atlas.converters.us_states.mt',
    'nc': 'atlas.converters.us_states.nc',
    'nd': 'atlas.converters.us_states.nd',
    'ne': 'atlas.converters.us_states.ne',
    'nh': 'atlas.converters.us_states.nh',
    'nj': 'atlas.converters.us_states.nj',
    'nm': 'atlas.converters.us_states.nm',
    'nv': 'atlas.converters.us_states.nv',
    'ny': 'atlas.converters.us_states.ny',
    'oh': 'atlas.converters.us_states.oh',
    'ok': 'atlas.converters.us_states.ok',
    'or': 'atlas.converters.us_states.or_',
    'pa': 'atlas.converters.us_states.pa',
    'ri': 'atlas.converters.us_states.ri',
    'sc': 'atlas.converters.us_states.sc',
    'sd': 'atlas.converters.us_states.sd',
    'tn': 'atlas.converters.us_states.tn',
    'tx': 'atlas.converters.us_states.tx',
    'ut': 'atlas.converters.us_states.ut',
    'va': 'atlas.converters.us_states.va',
    'vt': 'atlas.converters.us_states.vt',
    'wa': 'atlas.converters.us_states.wa',
    'wi': 'atlas.converters.us_states.wi',
    'wv': 'atlas.converters.us_states.wv',
    'wy': 'atlas.converters.us_states.wy',
}

# Section number extraction patterns per state filename format
FILENAME_PATTERNS = {
    'oh': (r'section-(.+)\.html', None),
    'ak': (r'statutes\.asp_(.+)\.html', lambda m: m.replace('-', '.')),
    'tx': (r'_(\d+-\d+)\.html', lambda m: m.replace('-', '.')),
    'de': (r'title(\d+)_c(\d+)', None),  # chapter-level files
    'nv': (r'NRS-(\w+)\.html', None),     # chapter-level files
}


def load_converter(state: str):
    """Load the converter class for a state."""
    module_path = STATE_CONVERTERS.get(state)
    if not module_path:
        return None

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        print(f"  Could not import {module_path}: {e}")
        return None

    # Find the converter class
    class_name = f"{state.upper()}Converter"
    if hasattr(module, class_name):
        return getattr(module, class_name)()

    # Try alternate naming (NYStateConverter, etc)
    for name in dir(module):
        if name.endswith('Converter') and name != 'Converter':
            return getattr(module, name)()

    return None


def parse_oh_file(converter, filepath: Path) -> list:
    """Parse an Ohio HTML file into Section models."""
    match = re.search(r'section-(.+)\.html', filepath.name)
    if not match:
        return []
    section_num = match.group(1)
    with open(filepath) as f:
        html = f.read()
    try:
        parsed = converter._parse_section_html(html, section_num, f"file://{filepath}")
        section = converter._to_section(parsed)
        return [section]
    except Exception as e:
        print(f"    Error parsing {filepath.name}: {e}")
        return []


def parse_generic_section_file(converter, filepath: Path, state: str) -> list:
    """Try to parse a single-section HTML file using the converter."""
    with open(filepath) as f:
        html = f.read()

    # Try to extract section number from filename
    # Pattern: look for the most number-like part of the filename
    name = filepath.stem
    # Remove common prefixes
    for prefix in [f'{state.upper()}-', f'{state}-', 'section-', 'sec-', 'Section_']:
        name = name.replace(prefix, '')

    # Try various section number extraction patterns
    patterns = [
        r'section-(.+?)(?:\.html)?$',
        r'_(\d[\d.]+)$',
        r'(\d[\d.]+)$',
    ]
    section_num = None
    for pat in patterns:
        m = re.search(pat, filepath.stem)
        if m:
            section_num = m.group(1)
            break

    if not section_num:
        section_num = filepath.stem

    try:
        if hasattr(converter, '_parse_section_html'):
            parsed = converter._parse_section_html(html, section_num, f"file://{filepath}")
            if hasattr(converter, '_to_section'):
                section = converter._to_section(parsed)
                return [section]
        elif hasattr(converter, 'to_section_model'):
            section = converter.to_section_model(html, section_num)
            return [section]
    except Exception as e:
        # Silently skip unparseable files
        pass

    return []


def get_chapters_info(converter, state: str):
    """Get chapter information from the converter module."""
    module_path = STATE_CONVERTERS.get(state)
    if not module_path:
        return []

    mod = importlib.import_module(module_path)

    chapters = []
    if state == 'ak':
        if hasattr(mod, 'AK_TAX_CHAPTERS'):
            for ch in getattr(mod, 'AK_TAX_CHAPTERS').keys():
                chapters.append((ch, 43))
        if hasattr(mod, 'AK_WELFARE_CHAPTERS'):
            for ch in getattr(mod, 'AK_WELFARE_CHAPTERS').keys():
                chapters.append((ch, 47))
    elif state == 'tx':
        if hasattr(mod, 'TX_TAX_CHAPTERS'):
            for ch in getattr(mod, 'TX_TAX_CHAPTERS').keys():
                chapters.append((ch, 'TX'))
        if hasattr(mod, 'TX_WELFARE_CHAPTERS'):
            for ch in getattr(mod, 'TX_WELFARE_CHAPTERS').keys():
                chapters.append((ch, 'HR'))
    else:
        for attr in ['TAX_CHAPTERS', 'WELFARE_CHAPTERS',
                     f'{state.upper()}_TAX_CHAPTERS',
                     f'{state.upper()}_WELFARE_CHAPTERS']:
            if hasattr(mod, attr):
                for ch in getattr(mod, attr).keys():
                    chapters.append((ch, None))

        if not chapters:
            for attr in ['TITLES', f'{state.upper()}_TITLES', 'TAX_TITLES']:
                if hasattr(mod, attr):
                    for t in getattr(mod, attr).keys():
                        chapters.append((str(t), None))

    return chapters


def ingest_state_live(state: str, ingestor: SupabaseIngestor, dry_run: bool = False) -> dict:
    """Fetch sections live from state legislature sites and ingest to Supabase."""
    stats = {'sections': 0, 'errors': 0, 'skipped': 0}

    converter = load_converter(state)
    if not converter:
        print(f"  No converter found for {state}")
        stats['skipped'] = 1
        return stats

    chapters = get_chapters_info(converter, state)
    if not chapters:
        print(f"  No chapters defined for {state}")
        stats['skipped'] = 1
        return stats

    print(f"  {len(chapters)} chapters to process")

    for chapter_num, title_or_code in chapters:
        display = f"{title_or_code}-{chapter_num}" if title_or_code else str(chapter_num)
        try:
            # Fetch sections from live site
            if state == 'ak' and title_or_code:
                sections = list(converter.iter_chapter(title_or_code, chapter_num))
            elif state == 'tx' and title_or_code:
                sections = list(converter.iter_chapter(title_or_code, chapter_num))
            elif hasattr(converter, 'iter_chapter'):
                sections = list(converter.iter_chapter(chapter_num))
            elif hasattr(converter, 'fetch_chapter'):
                sections = converter.fetch_chapter(chapter_num)
                if isinstance(sections, dict):
                    sections = list(sections.values())
            else:
                continue

            if not sections:
                continue

            # Convert to rules and upsert
            all_rules = []
            for section in sections:
                rules = list(ingestor._state_section_to_rules(section, state))
                all_rules.extend(rules)

            # Deduplicate
            seen = set()
            unique = []
            for r in all_rules:
                path = r.get('citation_path', '')
                if path and path not in seen:
                    seen.add(path)
                    unique.append(r)

            if unique and not dry_run:
                # Batch upsert
                for i in range(0, len(unique), 50):
                    batch = unique[i:i+50]
                    ingestor._upsert_rules(batch)

            stats['sections'] += len(unique)
            print(f"    ch {display}: {len(unique)} rules")

            # Rate limit
            time.sleep(0.3)

        except Exception as e:
            print(f"    ch {display}: ERROR {e}")
            stats['errors'] += 1

    return stats


def ingest_state_local(state: str, ingestor: SupabaseIngestor, dry_run: bool = False) -> dict:
    """Parse local HTML files and ingest to Supabase."""
    stats = {'sections': 0, 'errors': 0, 'skipped': 0}

    data_dir = Path(__file__).parent.parent / 'data' / 'statutes' / f'us-{state}'
    if not data_dir.exists():
        stats['skipped'] = 1
        return stats

    html_files = sorted(data_dir.glob('*.html'))
    if not html_files:
        stats['skipped'] = 1
        return stats

    converter = load_converter(state)
    if not converter:
        print(f"  No converter for {state}")
        stats['skipped'] = 1
        return stats

    print(f"  {len(html_files)} local HTML files")

    all_rules = []
    for filepath in html_files:
        if state == 'oh':
            sections = parse_oh_file(converter, filepath)
        else:
            sections = parse_generic_section_file(converter, filepath, state)

        for section in sections:
            try:
                rules = list(ingestor._state_section_to_rules(section, state))
                all_rules.extend(rules)
            except Exception as e:
                stats['errors'] += 1

    # Deduplicate
    seen = set()
    unique = []
    for r in all_rules:
        path = r.get('citation_path', '')
        if path and path not in seen:
            seen.add(path)
            unique.append(r)

    if unique and not dry_run:
        for i in range(0, len(unique), 50):
            batch = unique[i:i+50]
            ingestor._upsert_rules(batch)

    stats['sections'] = len(unique)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Ingest state statutes to Supabase")
    parser.add_argument("--state", help="Single state code (e.g., oh)")
    parser.add_argument("--mode", choices=["local", "live", "auto"], default="auto",
                        help="local=parse existing HTML, live=fetch from sites, auto=local first then live")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ingestor = SupabaseIngestor()
    data_root = Path(__file__).parent.parent / 'data' / 'statutes'

    if args.state:
        states = [args.state.lower()]
    else:
        # All states with either local data or converters
        states = sorted(STATE_CONVERTERS.keys())

    total = {'sections': 0, 'errors': 0, 'skipped': 0}

    for state in states:
        print(f"\n{'='*50}")
        print(f"  {state.upper()}")
        print(f"{'='*50}")

        state_dir = data_root / f'us-{state}'
        has_local = state_dir.exists() and any(state_dir.glob('*.html'))

        if args.mode == 'local' or (args.mode == 'auto' and has_local):
            stats = ingest_state_local(state, ingestor, args.dry_run)
            mode_used = 'local'
        elif args.mode == 'live' or (args.mode == 'auto' and not has_local):
            stats = ingest_state_live(state, ingestor, args.dry_run)
            mode_used = 'live'
        else:
            continue

        for k in total:
            total[k] += stats[k]

        if stats['sections'] > 0:
            print(f"  -> {stats['sections']} rules ({mode_used})")
        elif stats['skipped']:
            print(f"  -> skipped (no data)")
        elif stats['errors']:
            print(f"  -> {stats['errors']} errors")

    print(f"\n{'='*50}")
    print(f"TOTAL: {total['sections']} rules, {total['errors']} errors, {total['skipped']} skipped")


if __name__ == "__main__":
    main()
