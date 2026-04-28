"""Parser for IRS guidance document structure.

Extracts structured information from Revenue Procedures, Revenue Rulings,
and Notices including:
- Document metadata (number, type, effective dates)
- Section structure (SECTION 1, SECTION 2, etc.)
- Subsections (.01, .02, etc.)
- IRC section references
- Parameter values (EITC amounts, standard deduction, etc.)
"""

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ParsedSection:
    """A parsed section from an IRS document."""

    section_num: str
    heading: Optional[str] = None
    text: str = ""
    children: list["ParsedSection"] = field(default_factory=list)


@dataclass
class ParsedDocument:
    """Result of parsing an IRS guidance document."""

    doc_number: str = ""
    doc_type: str = ""  # REV_PROC, REV_RUL, NOTICE
    effective_year: Optional[int] = None
    sections: list[ParsedSection] = field(default_factory=list)
    irc_sections: list[int] = field(default_factory=list)
    raw_text: str = ""


class IRSDocumentParser:
    """Parse IRS guidance documents into structured data.

    Handles Revenue Procedures, Revenue Rulings, and Notices with
    their standard section structure.
    """

    # Pattern for document number: Rev. Proc. 2024-40, Notice 2024-45, etc.
    DOC_NUMBER_PATTERNS = [
        (r"Rev\.\s*Proc\.\s*(\d{4}-\d+)", "REV_PROC"),
        (r"Revenue\s+Procedure\s+(\d{4}-\d+)", "REV_PROC"),
        (r"Rev\.\s*Rul\.\s*(\d{4}-\d+)", "REV_RUL"),
        (r"Revenue\s+Ruling\s+(\d{4}-\d+)", "REV_RUL"),
        (r"Notice\s+(\d{4}-\d+)", "NOTICE"),
    ]

    # Pattern for main sections: SECTION 1. PURPOSE
    # Need to handle leading whitespace due to indented test input
    SECTION_PATTERN = re.compile(
        r"^\s*SECTION\s+(\d+)\.\s+(.+?)(?:\n|$)", re.IGNORECASE | re.MULTILINE
    )

    # Pattern for subsections: .01 Tax Rate Tables.
    # Need to handle leading whitespace
    SUBSECTION_PATTERN = re.compile(r"^\s*\.(\d+)\s+(.+?)(?:\.\s|$)", re.MULTILINE)

    # Pattern for IRC section references: ss 1, 23, 24, 25A, 32
    IRC_REF_PATTERN = re.compile(r"(?:ss|section)\s+([\d,\s]+(?:[A-Z])?)", re.IGNORECASE)

    # Pattern for effective year: taxable years beginning in 2025
    EFFECTIVE_YEAR_PATTERN = re.compile(
        r"(?:taxable\s+years?\s+beginning\s+in|effective\s+for\s+(?:taxable\s+)?(?:years?\s+)?(?:beginning\s+)?(?:in\s+)?|for\s+(?:taxable\s+)?(?:year\s+)?)"
        r"(\d{4})",
        re.IGNORECASE,
    )

    def parse(self, text: str) -> ParsedDocument:
        """Parse an IRS document from extracted text.

        Args:
            text: Raw text extracted from the PDF

        Returns:
            ParsedDocument with structured content
        """
        result = ParsedDocument(raw_text=text)

        # Extract document number and type
        self._extract_doc_info(text, result)

        # Extract effective year
        self._extract_effective_year(text, result)

        # Extract IRC section references
        self._extract_irc_references(text, result)

        # Parse section structure
        self._parse_sections(text, result)

        return result

    def _extract_doc_info(self, text: str, result: ParsedDocument) -> None:
        """Extract document number and type."""
        for pattern, doc_type in self.DOC_NUMBER_PATTERNS:
            match = re.search(pattern, text)
            if match:
                result.doc_number = match.group(1)
                result.doc_type = doc_type
                return

    def _extract_effective_year(self, text: str, result: ParsedDocument) -> None:
        """Extract the effective tax year from the document."""
        # Look specifically in EFFECTIVE DATE section first
        effective_section = re.search(
            r"SECTION\s+\d+\.\s+EFFECTIVE\s+DATE.*?(?=SECTION|\Z)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        search_text = effective_section.group() if effective_section else text

        match = self.EFFECTIVE_YEAR_PATTERN.search(search_text)
        if match:
            result.effective_year = int(match.group(1))

    def _extract_irc_references(self, text: str, result: ParsedDocument) -> None:
        """Extract IRC section numbers referenced in the document."""
        sections = set()

        # Look for the "Also Part I" header that lists all sections
        also_match = re.search(
            r"\(Also\s+Part\s+I,?\s*(?:ss|sections?)?\s*([\d,\s\w]+)\)",
            text,
            re.IGNORECASE,
        )
        if also_match:
            # Parse comma-separated section numbers like "1, 23, 24, 25A, 32"
            section_text = also_match.group(1)
            # Extract numbers (and letter suffixes like 25A, 36B)
            for match in re.finditer(r"(\d+[A-Z]?)", section_text):
                try:
                    # Extract just the numeric part for storing
                    num_part = re.match(r"(\d+)", match.group(1)).group(1)
                    sections.add(int(num_part))
                except (ValueError, AttributeError):  # pragma: no cover
                    pass

        # Also look for individual section references
        for match in self.IRC_REF_PATTERN.finditer(text):
            section_text = match.group(1)
            for num_match in re.finditer(r"(\d+)", section_text):
                try:
                    sections.add(int(num_match.group(1)))
                except ValueError:  # pragma: no cover
                    pass

        result.irc_sections = sorted(sections)

    def _parse_sections(self, text: str, result: ParsedDocument) -> None:
        """Parse the document into sections and subsections."""
        # Find all SECTION headers
        section_matches = list(self.SECTION_PATTERN.finditer(text))

        if not section_matches:
            return

        for i, match in enumerate(section_matches):
            section_num = match.group(1)
            heading = match.group(2).strip()

            # Find the end of this section (start of next section or end of doc)
            start = match.end()
            if i + 1 < len(section_matches):
                end = section_matches[i + 1].start()
            else:
                end = len(text)

            section_text = text[start:end].strip()

            section = ParsedSection(
                section_num=section_num,
                heading=heading,
                text=section_text,
            )

            # Parse subsections within this section
            self._parse_subsections(section_text, section)

            result.sections.append(section)

    def _parse_subsections(self, text: str, parent_section: ParsedSection) -> None:
        """Parse .01, .02 style subsections within a section."""
        # Find subsection markers
        subsection_matches = list(self.SUBSECTION_PATTERN.finditer(text))

        if not subsection_matches:
            return

        for i, match in enumerate(subsection_matches):
            subsection_num = f".{match.group(1)}"
            heading_text = match.group(2).strip()

            # Find the end of this subsection
            start = match.end()
            if i + 1 < len(subsection_matches):
                end = subsection_matches[i + 1].start()
            else:
                end = len(text)

            subsection_text = text[start:end].strip()

            subsection = ParsedSection(
                section_num=subsection_num,
                heading=heading_text,
                text=subsection_text,
            )

            parent_section.children.append(subsection)


class IRSParameterExtractor:
    """Extract parameter values from IRS guidance documents.

    Focuses on inflation-adjusted amounts from Rev. Procs like:
    - EITC thresholds and credit amounts
    - Standard deduction amounts
    - Tax bracket thresholds
    - Child Tax Credit amounts
    """

    def extract(self, text: str) -> dict:
        """Extract all recognized parameters from document text.

        Args:
            text: Document text (from PDF extraction)

        Returns:
            Dictionary of parameter sets keyed by program name
        """
        params = {}

        # Try to extract each parameter type
        eitc_params = self._extract_eitc_params(text)
        if eitc_params:
            params["eitc"] = eitc_params

        sd_params = self._extract_standard_deduction_params(text)
        if sd_params:
            params["standard_deduction"] = sd_params

        ctc_params = self._extract_ctc_params(text)
        if ctc_params:
            params["ctc"] = ctc_params

        return params

    def _extract_eitc_params(self, text: str) -> Optional[dict]:
        """Extract EITC parameters from section .06 or similar."""
        # Find the EITC section
        eitc_section = re.search(
            r"\.0\d\s+Earned\s+Income\s+Credit.*?(?=\.\d{2}\s+[A-Z]|\Z)",
            text,
            re.IGNORECASE | re.DOTALL,
        )

        if not eitc_section:
            return None

        section_text = eitc_section.group()
        params = {
            "max_credit": {},
            "earned_income_amount": {},
            "phaseout_start": {"single": {}, "joint": {}},
            "phaseout_end": {"single": {}, "joint": {}},
        }

        # Extract amounts - handle both inline and multi-line formats
        # The real PDF has amounts on the same line or following lines
        # Match dollar amounts like $1,234 or 1,234 (at least one digit required)
        amount_pattern = r"\$?([\d][\d,]*)"

        # Helper to extract 4 amounts after a row header
        def extract_row_amounts(header_pattern: str) -> list[int] | None:
            """Find header and extract following 4 dollar amounts."""
            match = re.search(header_pattern, section_text, re.IGNORECASE)
            if not match:  # pragma: no cover
                return None

            # Look for amounts starting from the header
            after_header = section_text[match.start() :]
            # Find all dollar amounts in the next ~200 characters
            amounts = re.findall(amount_pattern, after_header[:300])
            # Filter out empty matches and convert
            valid_amounts = []
            for v in amounts:
                if v and v.strip():
                    try:
                        valid_amounts.append(int(v.replace(",", "")))
                    except ValueError:  # pragma: no cover
                        continue  # pragma: no cover
                if len(valid_amounts) >= 4:
                    break
            if len(valid_amounts) >= 4:
                return valid_amounts[:4]
            return None  # pragma: no cover

        # Maximum credit row - amounts are: One, Two, Three+, None
        max_amounts = extract_row_amounts(r"Maximum\s+(?:Amount\s+of\s+)?Credit")
        if max_amounts:
            params["max_credit"] = {
                "1": max_amounts[0],  # One
                "2": max_amounts[1],  # Two
                "3": max_amounts[2],  # Three or more
                "0": max_amounts[3],  # None
            }

        # Earned income amount row
        earned_amounts = extract_row_amounts(r"Earned\s+Income\s+Amount")
        if earned_amounts:
            params["earned_income_amount"] = {
                "1": earned_amounts[0],
                "2": earned_amounts[1],
                "3": earned_amounts[2],
                "0": earned_amounts[3],
            }

        # Threshold phaseout - Joint (first in Rev. Proc. 2024-40 format)
        # Look for "Threshold Phaseout Amount\n(Married Filing Jointly)"
        threshold_joint = extract_row_amounts(
            r"Threshold\s+Phaseout\s+Amount\s*\n?\s*\(Married\s+Filing\s+Jointly\)"
        )
        if threshold_joint:
            params["phaseout_start"]["joint"] = {
                "1": threshold_joint[0],
                "2": threshold_joint[1],
                "3": threshold_joint[2],
                "0": threshold_joint[3],
            }

        # Completed phaseout - Joint
        completed_joint = extract_row_amounts(
            r"Completed\s+Phaseout\s+Amount\s*\n?\s*\(Married\s+Filing\s+Jointly\)"
        )
        if completed_joint:
            params["phaseout_end"]["joint"] = {
                "1": completed_joint[0],
                "2": completed_joint[1],
                "3": completed_joint[2],
                "0": completed_joint[3],
            }

        # Threshold phaseout - Single/Other (may say "All other filing statuses")
        threshold_single = extract_row_amounts(
            r"Threshold\s+Phaseout\s+Amount\s*(?:\n?\s*)?(?:\(All\s+other|(?:\(Single))"
        )
        if threshold_single:
            params["phaseout_start"]["single"] = {
                "1": threshold_single[0],
                "2": threshold_single[1],
                "3": threshold_single[2],
                "0": threshold_single[3],
            }

        # Completed phaseout - Single/Other
        completed_single = extract_row_amounts(
            r"Completed\s+Phaseout\s+Amount\s*(?:\n?\s*)?(?:\(All\s+other|(?:\(Single))"
        )
        if completed_single:
            params["phaseout_end"]["single"] = {
                "1": completed_single[0],
                "2": completed_single[1],
                "3": completed_single[2],
                "0": completed_single[3],
            }

        return params if any(params["max_credit"]) else None

    def _extract_standard_deduction_params(self, text: str) -> Optional[dict]:
        """Extract standard deduction amounts from section .15 or similar."""
        # Find the standard deduction section
        sd_section = re.search(
            r"\.1\d\s+Standard\s+Deduction.*?(?=\.\d{2}|\Z)",
            text,
            re.IGNORECASE | re.DOTALL,
        )

        if not sd_section:
            return None

        section_text = sd_section.group()
        params = {}

        amount_pattern = r"\$?([\d,]+)"

        # Joint filers
        joint_match = re.search(
            r"(?:Married\s+Individuals?\s+Filing\s+Joint(?:ly)?|Joint\s+Returns?)\s+"
            + amount_pattern,
            section_text,
            re.IGNORECASE,
        )
        if joint_match:
            params["joint"] = int(joint_match.group(1).replace(",", ""))

        # Head of household
        hoh_match = re.search(
            r"Heads?\s+of\s+Household(?:s)?\s+" + amount_pattern,
            section_text,
            re.IGNORECASE,
        )
        if hoh_match:
            params["head_of_household"] = int(hoh_match.group(1).replace(",", ""))

        # Single (unmarried individuals)
        single_match = re.search(
            r"(?:Unmarried\s+Individuals?|Single)\s+(?:\([^)]+\)\s+)?" + amount_pattern,
            section_text,
            re.IGNORECASE,
        )
        if single_match:
            params["single"] = int(single_match.group(1).replace(",", ""))

        # Married filing separately
        mfs_match = re.search(
            r"Married\s+Individuals?\s+Filing\s+Separate(?:ly)?\s+" + amount_pattern,
            section_text,
            re.IGNORECASE,
        )
        if mfs_match:
            params["married_separate"] = int(mfs_match.group(1).replace(",", ""))

        # Dependent minimum
        dependent_match = re.search(
            r"(?:greater\s+of\s+\(1\)\s*)?\$?([\d,]+)(?:,\s+or\s+\(2\))?",
            section_text,
        )
        if "dependent" in section_text.lower() and dependent_match:
            # Look for the specific dependent amount pattern
            dep_amount = re.search(
                r"greater\s+of\s+\(1\)\s*\$?([\d,]+)", section_text, re.IGNORECASE
            )
            if dep_amount:
                params["dependent_min"] = int(dep_amount.group(1).replace(",", ""))

        # Aged or blind additional amounts
        aged_blind = re.search(
            r"additional\s+standard\s+deduction.*?(?:aged\s+or\s+blind).*?"
            r"\$?([\d,]+)\s*\(?married\)?.*?\$?([\d,]+)\s*\(?single\)?",
            section_text,
            re.IGNORECASE | re.DOTALL,
        )
        if aged_blind:
            params["aged_blind_married"] = int(aged_blind.group(1).replace(",", ""))
            params["aged_blind_single"] = int(aged_blind.group(2).replace(",", ""))

        return params if params else None

    def _extract_ctc_params(self, text: str) -> Optional[dict]:
        """Extract Child Tax Credit parameters."""
        params = {}

        # Find refundable max - look for "refundable ... is $X"
        # Format: "may be refundable under section 24(d)(1)(A) is $1,700"
        refund_match = re.search(
            r"refundable[^$]+\$([\d,]+)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if refund_match:
            params["refundable_max"] = int(refund_match.group(1).replace(",", ""))

        # Find phaseout thresholds - pattern like "$200,000 ($400,000 in the case of a joint return)"
        threshold_match = re.search(
            r"threshold\s+amount[^$]+\$([\d,]+)\s*\(\$([\d,]+)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if threshold_match:
            params["phaseout_threshold"] = {
                "single": int(threshold_match.group(1).replace(",", "")),
                "joint": int(threshold_match.group(2).replace(",", "")),
            }

        return params if params else None
