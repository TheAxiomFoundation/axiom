"""Fetcher for IRS guidance documents (Revenue Procedures, Revenue Rulings, Notices).

This module downloads and parses IRS guidance from:
- Internal Revenue Bulletin (IRB) HTML pages
- IRS.gov PDF documents
"""

import re
from datetime import date
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from axiom.models_guidance import GuidanceSection, GuidanceType, RevenueProcedure


class IRSGuidanceFetcher:
    """Fetch IRS guidance documents from official sources."""

    def __init__(self):
        self.base_url = "https://www.irs.gov"
        self.client = httpx.Client(timeout=30.0)

    def fetch_revenue_procedure(self, doc_number: str) -> RevenueProcedure:
        """Fetch a Revenue Procedure by document number.

        Args:
            doc_number: Document number like "2023-34"

        Returns:
            RevenueProcedure object with full content

        Example:
            >>> fetcher = IRSGuidanceFetcher()
            >>> rp = fetcher.fetch_revenue_procedure("2023-34")
            >>> print(rp.title)
        """
        year, num = doc_number.split("-")

        # Try to find the IRB URL for this Rev. Proc
        # Rev. Proc. 2023-34 was published in IRB 2023-48
        irb_url = self._find_irb_url(doc_number)

        if not irb_url:
            raise ValueError(f"Could not find IRB URL for Rev. Proc. {doc_number}")

        # Fetch the IRB page
        response = self.client.get(irb_url)
        response.raise_for_status()

        # Parse the HTML
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract the Rev. Proc. content
        return self._parse_revenue_procedure(soup, doc_number, irb_url)

    def _find_irb_url(self, doc_number: str) -> Optional[str]:
        """Find the IRB URL for a given Revenue Procedure.

        This uses a lookup table for common Rev. Procs. For a production system,
        this would query the IRS API or scrape the IRB index.
        """
        # Lookup table for EITC-related Rev. Procs
        known_urls = {
            "2023-34": "https://www.irs.gov/irb/2023-48_IRB",
            "2024-40": "https://www.irs.gov/irb/2024-50_IRB",
            "2022-38": "https://www.irs.gov/irb/2022-45_IRB",
            "2021-45": "https://www.irs.gov/irb/2021-48_IRB",
            "2020-45": "https://www.irs.gov/irb/2020-46_IRB",
        }
        return known_urls.get(doc_number)

    def _parse_revenue_procedure(
        self, soup: BeautifulSoup, doc_number: str, source_url: str
    ) -> RevenueProcedure:
        """Parse a Revenue Procedure from IRB HTML."""
        # Find the Rev. Proc. section in the IRB
        # The IRB contains multiple documents, need to find the right one
        rev_proc_heading = soup.find(
            lambda tag: tag.name in ["h2", "h3", "h4"]
            and f"Rev. Proc. {doc_number}" in tag.get_text()
        )

        if not rev_proc_heading:
            raise ValueError(f"Could not find Rev. Proc. {doc_number} in IRB page")

        # Extract title from heading
        title = self._extract_title(rev_proc_heading)

        # The actual content is typically in a following section
        # Look for the full document text which usually starts after some navigation
        # Find all paragraphs and divs after the heading
        content_elements = []

        # Start from the heading and collect all content until next major heading
        current = rev_proc_heading

        # Look ahead for the actual content (skip navigation elements)
        for elem in rev_proc_heading.find_all_next():
            # Stop at next major document heading
            if self._is_next_document(elem):
                break

            # Collect text-bearing elements
            if elem.name in ['p', 'div', 'section', 'article'] and elem.get_text(strip=True):
                # Skip navigation and header elements
                if 'nav' not in elem.get('class', []) and 'header' not in elem.get('class', []):
                    content_elements.append(elem)

        # If we didn't find much, try a different approach - get the main content area
        if len(content_elements) < 5:
            main_content = soup.find('main') or soup.find(id='content') or soup.find(class_='field-item')
            if main_content:
                # Find where our Rev. Proc starts
                found_heading = False
                content_elements = []
                for elem in main_content.find_all(['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5']):
                    if not found_heading and f"Rev. Proc. {doc_number}" in elem.get_text():
                        found_heading = True
                        continue
                    if found_heading:
                        if self._is_next_document(elem):
                            break
                        if elem.get_text(strip=True):
                            content_elements.append(elem)

        # Extract full text
        full_text = "\n\n".join(el.get_text(strip=True) for el in content_elements if el.get_text(strip=True))

        # Parse sections
        sections = self._parse_sections(content_elements)

        # Extract metadata from content
        year_match = re.search(r"202[0-9]", doc_number)
        year = int(year_match.group()) if year_match else date.today().year

        # Determine IRB citation from URL
        irb_match = re.search(r"/irb/(\d{4}-\d+)_IRB", source_url)
        irb_citation = irb_match.group(1).replace("-", "-") + " IRB" if irb_match else ""

        # Extract tax years (for EITC Rev. Procs, usually the following year)
        tax_years = self._extract_tax_years(full_text, year)

        # Subject areas
        subject_areas = self._extract_subject_areas(title, full_text)

        return RevenueProcedure(
            doc_number=doc_number,
            doc_type=GuidanceType.REV_PROC,
            title=title,
            irb_citation=irb_citation,
            published_date=date(year, 12, 1),  # Approximate - would need better parsing
            full_text=full_text,
            sections=sections,
            effective_date=None,  # Would need to parse from content
            tax_years=tax_years,
            subject_areas=subject_areas,
            parameters={},  # Would be populated by a separate parameter extractor
            source_url=source_url,
            pdf_url=None,  # Could be constructed if needed
            retrieved_at=date.today(),
        )

    def _extract_title(self, heading_tag) -> str:
        """Extract clean title from heading tag."""
        text = heading_tag.get_text(strip=True)
        # Remove the "Rev. Proc. 2023-34" prefix
        title = re.sub(r"^Rev\.\s*Proc\.\s*\d{4}-\d+\.?\s*", "", text)
        return title.strip()

    def _is_next_document(self, tag) -> bool:
        """Check if this tag marks the start of the next document."""
        if not tag or not tag.name:
            return False

        text = tag.get_text(strip=True)
        # Check for common document headers
        patterns = [
            r"^Rev\.\s*Proc\.\s*\d{4}-\d+",
            r"^Rev\.\s*Rul\.\s*\d{4}-\d+",
            r"^Notice\s*\d{4}-\d+",
            r"^Announcement\s*\d{4}-\d+",
        ]
        return any(re.match(pattern, text) for pattern in patterns)

    def _parse_sections(self, elements: list) -> list[GuidanceSection]:
        """Parse structured sections from HTML elements.

        IRS Rev. Procs typically have numbered sections like:
        SECTION 1. PURPOSE
        SECTION 2. BACKGROUND
        .01 Subsection
        .02 Subsection
        """
        sections = []
        current_section = None
        current_subsection = None

        for el in elements:
            text = el.get_text(strip=True)

            # Check for main section (SECTION 1. PURPOSE)
            section_match = re.match(r"SECTION\s+(\d+)\.\s+(.+)", text, re.IGNORECASE)
            if section_match:
                if current_section:
                    sections.append(current_section)

                section_num = section_match.group(1)
                heading = section_match.group(2)
                current_section = GuidanceSection(
                    section_num=section_num, heading=heading, text="", children=[]
                )
                current_subsection = None
                continue

            # Check for subsection (.01, .02, etc.)
            subsection_match = re.match(r"\.(\d+)\s+(.+)", text)
            if subsection_match and current_section:
                if current_subsection:
                    current_section.children.append(current_subsection)

                subsection_num = f".{subsection_match.group(1)}"
                rest = subsection_match.group(2)
                current_subsection = GuidanceSection(
                    section_num=subsection_num, heading=None, text=rest, children=[]
                )
                continue

            # Accumulate text to current section or subsection
            if text:
                if current_subsection:
                    current_subsection.text += "\n" + text  # pragma: no cover
                elif current_section:
                    current_section.text += "\n" + text

        # Add the last section
        if current_subsection and current_section:
            current_section.children.append(current_subsection)
        if current_section:
            sections.append(current_section)

        return sections

    def _extract_tax_years(self, text: str, doc_year: int) -> list[int]:
        """Extract applicable tax years from document text.

        EITC Rev. Procs typically apply to the year after publication.
        """
        # Look for explicit year mentions
        year_mentions = re.findall(r"\b(20\d{2})\b", text)
        if year_mentions:
            years = sorted(set(int(y) for y in year_mentions))
            # Filter to reasonable range (doc year to doc year + 2)
            return [y for y in years if doc_year <= y <= doc_year + 2]

        # Default: assume it applies to the year after publication
        return [doc_year + 1]

    def _extract_subject_areas(self, title: str, text: str) -> list[str]:
        """Extract subject areas from title and text."""
        subjects = set()

        # Common subject keywords
        keywords = {
            "EITC": ["earned income", "eitc"],
            "CTC": ["child tax credit", "ctc"],
            "Standard Deduction": ["standard deduction"],
            "Inflation Adjustment": ["inflation", "adjustment", "cost-of-living"],
            "Income Tax": ["income tax", "taxable income"],
        }

        combined_text = (title + " " + text).lower()

        for subject, terms in keywords.items():
            if any(term in combined_text for term in terms):
                subjects.add(subject)

        return sorted(subjects) if subjects else ["General"]

    def close(self):
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
