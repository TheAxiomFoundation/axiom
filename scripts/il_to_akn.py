#!/usr/bin/env python3
"""Convert Illinois Compiled Statutes (ILCS) to Akoma Ntoso XML.

This script fetches Illinois statutes from ilga.gov and converts them
to Akoma Ntoso XML format.

The ilga.gov website structure:
- Chapters page: /Legislation/ILCS/Chapters
- Acts page: /Legislation/ILCS/Acts?ChapterID=X&ChapterNumber=NN
- Articles page: /Legislation/ILCS/Articles?ActID=NNN&ChapterID=X
- Details page: /legislation/ILCS/details?ActID=NNN&ChapterID=X&ArticleID=Y

Usage:
    python scripts/il_to_akn.py [--chapters CHAPTERS] [--output-dir DIR]

Examples:
    python scripts/il_to_akn.py                    # Fetch all key chapters
    python scripts/il_to_akn.py --chapters 35     # Just Revenue chapter
    python scripts/il_to_akn.py --chapters 35,305 # Revenue and Public Aid
"""

import argparse
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from xml.dom import minidom
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Base URL
BASE_URL = "https://www.ilga.gov"

# Output directory
DEFAULT_OUTPUT_DIR = Path("/tmp/rules-us-il-akn")

# Illinois chapter information
IL_CHAPTERS: dict[int, str] = {
    5: "General Provisions",
    10: "Elections",
    15: "Executive Officers",
    20: "Executive Branch",
    25: "Legislature",
    30: "Finance",
    35: "Revenue",
    40: "Pensions",
    45: "Interstate Compacts",
    50: "Local Government",
    55: "Counties",
    60: "Townships",
    65: "Municipalities",
    70: "Special Districts",
    75: "Libraries",
    105: "Schools",
    110: "Higher Education",
    205: "Public Utilities",
    210: "Vehicles",
    215: "Roads and Bridges",
    220: "Railroads",
    225: "Professions and Occupations",
    230: "Public Health",
    235: "Children",
    240: "Aging",
    305: "Public Aid",
    310: "Welfare Services",
    320: "Housing",
    325: "Corrections",
    330: "Criminal Law",
    405: "Mental Health",
    410: "Hospitals",
    505: "Agriculture",
    510: "Animals",
    515: "Fish",
    520: "Wildlife",
    525: "Conservation",
    605: "Courts",
    625: "Motor Vehicles",
    705: "Civil Liabilities",
    710: "Contracts",
    715: "Business Organizations",
    720: "Criminal Offenses",
    725: "Criminal Procedure",
    730: "Corrections",
    735: "Civil Procedure",
    740: "Civil Liabilities",
    745: "Immunities",
    750: "Families",
    755: "Estates",
    760: "Trusts",
    765: "Property",
    770: "Insurance",
    805: "Business Organizations",
    810: "Financial Institutions",
    815: "Business Transactions",
    820: "Employment",
}

# Chapter IDs used in URL parameters
IL_CHAPTER_IDS: dict[int, int] = {
    5: 2,
    10: 3,
    15: 4,
    20: 5,
    25: 6,
    30: 7,
    35: 8,
    40: 9,
    45: 10,
    50: 11,
    55: 12,
    60: 13,
    65: 14,
    70: 15,
    75: 16,
    105: 17,
    110: 18,
    205: 20,
    305: 28,
    405: 33,
    505: 38,
    605: 43,
    625: 44,
    720: 49,
    725: 50,
    735: 52,
    765: 57,
    805: 61,
    815: 63,
    820: 64,
}


@dataclass
class ParsedSection:
    """Parsed Illinois statute section."""

    section_number: str  # e.g., "201"
    section_title: str  # e.g., "Tax imposed"
    text: str  # Full text content
    subsections: list["ParsedSubsection"] = field(default_factory=list)
    source_note: str | None = None


@dataclass
class ParsedSubsection:
    """A subsection within an Illinois statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list["ParsedSubsection"] = field(default_factory=list)


@dataclass
class ParsedArticle:
    """A parsed article containing sections."""

    article_number: str
    article_title: str
    sections: list[ParsedSection] = field(default_factory=list)


@dataclass
class ParsedAct:
    """A parsed act containing articles."""

    chapter_num: int
    act_num: int
    act_name: str
    act_id: int  # Database ActID from URL
    articles: list[ParsedArticle] = field(default_factory=list)
    sections: list[ParsedSection] = field(default_factory=list)  # For acts without articles


class ILFetcher:
    """Fetches Illinois statutes from ilga.gov."""

    def __init__(self, rate_limit_delay: float = 0.5):
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0.0
        self.client = httpx.Client(
            timeout=60.0,
            headers={"User-Agent": "Arch/1.0 (Statute Research; contact@axiom-foundation.org)"},
            follow_redirects=True,
        )

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str) -> str:
        """Make a rate-limited GET request."""
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    def get_acts_for_chapter(self, chapter_num: int) -> list[tuple[int, int, str]]:
        """Get list of acts in a chapter.

        Returns:
            List of (act_num, act_id, act_name) tuples
        """
        chapter_id = IL_CHAPTER_IDS.get(chapter_num, chapter_num)
        url = f"{BASE_URL}/Legislation/ILCS/Acts?ChapterID={chapter_id}&ChapterNumber={chapter_num}"

        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        acts = []
        # Pattern: "35 ILCS 5/ Illinois Income Tax Act."
        pattern = re.compile(rf"{chapter_num}\s+ILCS\s+(\d+)/\s+(.*)")

        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            match = pattern.match(text)
            if match:
                act_num = int(match.group(1))
                act_name = match.group(2).strip().rstrip(".")

                # Extract ActID from href
                href = link["href"]
                act_id_match = re.search(r"ActID=(\d+)", href)
                if act_id_match:
                    act_id = int(act_id_match.group(1))
                    acts.append((act_num, act_id, act_name))

        return acts

    def get_articles_for_act(self, act_id: int, chapter_id: int) -> list[tuple[int, str]]:
        """Get list of articles in an act.

        Returns:
            List of (article_id, article_title) tuples
        """
        url = f"{BASE_URL}/Legislation/ILCS/Articles?ActID={act_id}&ChapterID={chapter_id}"

        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        articles = []
        # Pattern: "Article 2-Tax Imposed"
        pattern = re.compile(r"Article\s+(\d+[A-Za-z]?)\s*[-–]\s*(.+)")

        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            match = pattern.match(text)
            if match:
                href = link["href"]
                # Extract ArticleID or build from pattern
                article_id_match = re.search(r"ArticleID=(\d+)", href)
                if article_id_match:
                    article_id = int(article_id_match.group(1))
                    articles.append((article_id, text))

        return articles

    def fetch_act_content(self, act_id: int, chapter_id: int) -> str:
        """Fetch the full act content HTML."""
        # Get articles first
        articles = self.get_articles_for_act(act_id, chapter_id)

        all_html = []

        if articles:
            # Fetch each article
            for article_id, article_title in articles:
                url = f"{BASE_URL}/legislation/ILCS/details?ActID={act_id}&ChapterID={chapter_id}&ArticleID={article_id}"
                try:
                    html = self._get(url)
                    all_html.append(f"<!-- Article: {article_title} -->\n{html}")
                except httpx.HTTPError as e:
                    print(f"    Warning: Failed to fetch article {article_id}: {e}")
        else:
            # Try fetching entire act
            url = f"{BASE_URL}/legislation/ILCS/details?ActID={act_id}&ChapterID={chapter_id}"
            try:
                html = self._get(url)
                all_html.append(html)
            except httpx.HTTPError as e:
                print(f"    Warning: Failed to fetch act: {e}")

        return "\n".join(all_html)

    def parse_sections_from_html(self, html: str) -> list[ParsedSection]:
        """Parse sections from the details page HTML."""
        soup = BeautifulSoup(html, "html.parser")
        sections = []

        # The text is in <code><font> elements
        # Pattern: "Sec. NNN. Title. Content..."
        text = soup.get_text(separator=" ", strip=True)

        # Find all section boundaries
        section_pattern = re.compile(r"Sec\.\s+(\d+[a-zA-Z\-\.]*)\.\s+([^.]+)\.\s*", re.MULTILINE)

        matches = list(section_pattern.finditer(text))

        for i, match in enumerate(matches):
            section_num = match.group(1)
            section_title = match.group(2).strip()

            # Get content until next section
            start = match.end()
            if i + 1 < len(matches):
                end = matches[i + 1].start()
            else:
                # Find end markers like "(Source:" or end of text
                source_match = re.search(r"\(Source:", text[start:])
                if source_match:
                    end = start + source_match.start() + 200  # Include source note
                else:
                    end = min(start + 10000, len(text))

            content = text[start:end].strip()

            # Extract source note if present
            source_note = None
            source_match = re.search(r"\(Source:\s*([^)]+)\)", content)
            if source_match:
                source_note = source_match.group(1).strip()[:500]

            # Clean content - stop at source note
            source_idx = content.find("(Source:")
            if source_idx > 0:
                content = content[:source_idx].strip()

            # Limit content size
            content = content[:8000]

            # Parse subsections
            subsections = self._parse_subsections(content)

            sections.append(
                ParsedSection(
                    section_number=section_num,
                    section_title=section_title,
                    text=content,
                    subsections=subsections,
                    source_note=source_note,
                )
            )

        return sections

    def _parse_subsections(self, text: str) -> list[ParsedSubsection]:
        """Parse hierarchical subsections from text."""
        subsections = []

        # Split by top-level subsections (a), (b), etc.
        parts = re.split(r"(?=\([a-z]\)\s)", text)

        for part in parts[1:]:  # Skip content before first (a)
            match = re.match(r"\(([a-z])\)\s*", part)
            if not match:
                continue

            identifier = match.group(1)
            content = part[match.end() :]

            # Parse second-level children (1), (2), etc.
            children = self._parse_subsection_level2(content)

            # Get text before first child
            if children:
                first_child_match = re.search(r"\(\d+\)", content)
                direct_text = (
                    content[: first_child_match.start()].strip()
                    if first_child_match
                    else content.strip()
                )
            else:
                direct_text = content.strip()

            # Clean up - stop at next lettered subsection
            next_sub = re.search(r"\([a-z]\)", direct_text)
            if next_sub:
                direct_text = direct_text[: next_sub.start()].strip()

            subsections.append(
                ParsedSubsection(
                    identifier=identifier,
                    text=direct_text[:2000],
                    children=children,
                )
            )

        return subsections

    def _parse_subsection_level2(self, text: str) -> list[ParsedSubsection]:
        """Parse level 2 subsections (1), (2), etc."""
        subsections = []
        parts = re.split(r"(?=\(\d+\)\s)", text)

        for part in parts[1:]:
            match = re.match(r"\((\d+)\)\s*", part)
            if not match:
                continue

            identifier = match.group(1)
            content = part[match.end() :]

            # Limit size
            next_num = re.search(r"\(\d+\)", content)
            if next_num:
                content = content[: next_num.start()]

            next_letter = re.search(r"\([a-z]\)", content)
            if next_letter:
                content = content[: next_letter.start()]

            subsections.append(
                ParsedSubsection(
                    identifier=identifier,
                    text=content.strip()[:2000],
                    children=[],
                )
            )

        return subsections

    def close(self):
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def create_akn_xml(
    chapter_num: int,
    act_num: int,
    act_name: str,
    sections: list[ParsedSection],
) -> str:
    """Create Akoma Ntoso XML from parsed sections."""
    # Register namespace
    ET.register_namespace("akn", AKN_NS)
    ET.register_namespace("", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "ILCS")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#arch")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-il/act/ilcs/{chapter_num}-{act_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-il/act/ilcs/{chapter_num}-{act_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", date.today().isoformat())
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#illinois-general-assembly")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-il")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", f"{chapter_num}-{act_num}")
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "ILCS")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-il/act/ilcs/{chapter_num}-{act_num}/eng@{date.today().isoformat()}"
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value", f"/akn/us-il/act/ilcs/{chapter_num}-{act_num}/eng@{date.today().isoformat()}"
    )
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", date.today().isoformat())
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#arch")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set(
        "value",
        f"/akn/us-il/act/ilcs/{chapter_num}-{act_num}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value",
        f"/akn/us-il/act/ilcs/{chapter_num}-{act_num}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", date.today().isoformat())
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#arch")

    # References
    refs = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    refs.set("source", "#arch")

    arch_ref = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    arch_ref.set("eId", "arch")
    arch_ref.set("href", "https://axiom-foundation.org")
    arch_ref.set("showAs", "Atlas")

    il_leg = ET.SubElement(refs, f"{{{AKN_NS}}}TLCOrganization")
    il_leg.set("eId", "illinois-general-assembly")
    il_leg.set("href", "https://www.ilga.gov")
    il_leg.set("showAs", "Illinois General Assembly")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Chapter container
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{chapter_num}")

    num = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    num.text = str(chapter_num)

    heading = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    heading.text = IL_CHAPTERS.get(chapter_num, f"Chapter {chapter_num}")

    # Article for the Act
    article = ET.SubElement(chapter, f"{{{AKN_NS}}}article")
    article.set("eId", f"chp_{chapter_num}__art_{act_num}")

    art_num = ET.SubElement(article, f"{{{AKN_NS}}}num")
    art_num.text = f"{chapter_num} ILCS {act_num}/"

    art_heading = ET.SubElement(article, f"{{{AKN_NS}}}heading")
    art_heading.text = act_name

    # Add sections
    for sec in sections:
        add_section_to_xml(article, sec, chapter_num, act_num)

    # Convert to string with pretty print
    xml_str = ET.tostring(root, encoding="unicode")

    try:
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ", encoding="UTF-8")
        lines = pretty_xml.decode("utf-8").split("\n")
        cleaned = [line for line in lines if line.strip()]
        return "\n".join(cleaned)
    except Exception:
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str


def add_section_to_xml(
    parent: ET.Element,
    section: ParsedSection,
    chapter_num: int,
    act_num: int,
) -> None:
    """Add a section element to the parent XML element."""
    safe_sec_num = re.sub(r"[^a-zA-Z0-9]", "-", section.section_number)
    sec_id = f"sec_{chapter_num}-{act_num}-{safe_sec_num}"

    sec_elem = ET.SubElement(parent, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", sec_id)

    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = section.section_number

    heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    heading.text = section.section_title or f"Section {section.section_number}"

    if section.subsections:
        for sub in section.subsections:
            add_subsection_to_xml(sec_elem, sub, sec_id, level=1)
    elif section.text:
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = section.text[:10000]

    if section.source_note:
        note = ET.SubElement(sec_elem, f"{{{AKN_NS}}}authorialNote")
        note.set("marker", "source")
        p = ET.SubElement(note, f"{{{AKN_NS}}}p")
        p.text = f"Source: {section.source_note}"


def add_subsection_to_xml(
    parent: ET.Element,
    subsection: ParsedSubsection,
    parent_id: str,
    level: int,
) -> None:
    """Add a subsection element to the parent XML element."""
    safe_id = re.sub(r"[^a-zA-Z0-9]", "", subsection.identifier)
    sub_id = f"{parent_id}__subsec_{safe_id}"

    if level == 1:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    num.text = f"({subsection.identifier})"

    if subsection.text:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection.text[:10000]

    for child in subsection.children:
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def convert_act(
    fetcher: ILFetcher,
    chapter_num: int,
    act_num: int,
    act_id: int,
    act_name: str,
    output_dir: Path,
) -> dict:
    """Convert a single act to Akoma Ntoso XML."""
    try:
        chapter_id = IL_CHAPTER_IDS.get(chapter_num, chapter_num)

        # Fetch HTML content
        html = fetcher.fetch_act_content(act_id, chapter_id)

        if not html:
            return {
                "chapter": chapter_num,
                "act": act_num,
                "act_name": act_name,
                "sections": 0,
                "success": False,
                "error": "No HTML content",
            }

        # Parse sections
        sections = fetcher.parse_sections_from_html(html)

        if not sections:
            return {
                "chapter": chapter_num,
                "act": act_num,
                "act_name": act_name,
                "sections": 0,
                "success": True,
                "error": None,
            }

        # Create AKN XML
        xml_content = create_akn_xml(chapter_num, act_num, act_name, sections)

        # Write output
        output_path = output_dir / f"ilcs-{chapter_num:03d}-{act_num:04d}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        return {
            "chapter": chapter_num,
            "act": act_num,
            "act_name": act_name,
            "sections": len(sections),
            "success": True,
            "error": None,
        }

    except Exception as e:
        return {
            "chapter": chapter_num,
            "act": act_num,
            "act_name": act_name,
            "sections": 0,
            "success": False,
            "error": str(e),
        }


def main():
    parser = argparse.ArgumentParser(
        description="Convert Illinois Compiled Statutes to Akoma Ntoso XML"
    )
    parser.add_argument(
        "--chapters",
        type=str,
        default="35,305",
        help="Comma-separated list of chapter numbers (default: 35,305)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.5,
        help="Seconds between requests (default: 0.5)",
    )
    parser.add_argument(
        "--max-acts",
        type=int,
        default=0,
        help="Maximum acts per chapter (0 = all)",
    )

    args = parser.parse_args()

    # Parse chapters
    chapter_nums = [int(c.strip()) for c in args.chapters.split(",")]

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("Illinois Compiled Statutes to Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output directory: {args.output_dir}")
    print(f"Chapters to process: {chapter_nums}")
    print()

    total_sections = 0
    successful_acts = 0
    failed_acts = 0
    empty_acts = 0

    with ILFetcher(rate_limit_delay=args.rate_limit) as fetcher:
        for chapter_num in chapter_nums:
            chapter_name = IL_CHAPTERS.get(chapter_num, f"Chapter {chapter_num}")
            print(f"\nChapter {chapter_num}: {chapter_name}")
            print("-" * 40)

            # Get acts for this chapter
            try:
                acts = fetcher.get_acts_for_chapter(chapter_num)
                print(f"  Found {len(acts)} acts")
            except Exception as e:
                print(f"  ERROR: Failed to get acts: {e}")
                continue

            if args.max_acts > 0:
                acts = acts[: args.max_acts]

            for act_num, act_id, act_name in acts:
                result = convert_act(
                    fetcher,
                    chapter_num,
                    act_num,
                    act_id,
                    act_name,
                    args.output_dir,
                )

                if result["success"]:
                    if result["sections"] > 0:
                        successful_acts += 1
                        total_sections += result["sections"]
                        print(
                            f"    [OK] {chapter_num} ILCS {act_num}/: {result['sections']} sections"
                        )
                    else:
                        empty_acts += 1
                        print(f"    [--] {chapter_num} ILCS {act_num}/: no sections found")
                else:
                    failed_acts += 1
                    print(f"    [FAIL] {chapter_num} ILCS {act_num}/: {result['error']}")

    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Successful acts:     {successful_acts}")
    print(f"  Empty acts:          {empty_acts}")
    print(f"  Failed acts:         {failed_acts}")
    print(f"  Total sections:      {total_sections}")
    print(f"  Output directory:    {args.output_dir}")

    output_files = list(args.output_dir.glob("*.xml"))
    print(f"  Output XML files:    {len(output_files)}")

    return 0 if failed_acts == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
