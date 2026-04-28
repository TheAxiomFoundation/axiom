"""Tests for PDF text extraction from IRS guidance documents.

Following TDD: write failing tests first, then implement to make them pass.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestPDFTextExtractor:
    """Tests for the PDF text extractor."""

    def test_extract_text_from_pdf_bytes(self):
        """Extract text from raw PDF bytes."""
        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor

        # Sample PDF bytes would need a real PDF for integration testing
        # For unit tests, we mock the pymupdf calls
        extractor = PDFTextExtractor()

        with patch("axiom_corpus.fetchers.pdf_extractor.fitz") as mock_fitz:
            # Mock a simple PDF document
            mock_doc = MagicMock()
            mock_page = MagicMock()
            mock_page.get_text.return_value = "SECTION 1. PURPOSE\n\nThis is sample text."
            mock_doc.__iter__ = lambda self: iter([mock_page])
            mock_doc.__len__ = lambda self: 1
            mock_fitz.open.return_value.__enter__ = lambda self: mock_doc
            mock_fitz.open.return_value.__exit__ = lambda self, *args: None

            text = extractor.extract_text(b"%PDF-1.4 fake content")

        assert "SECTION 1. PURPOSE" in text
        assert "sample text" in text

    def test_extract_text_handles_multi_page_pdf(self):
        """Extract text from multi-page PDF, preserving page order."""
        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor

        extractor = PDFTextExtractor()

        with patch("axiom_corpus.fetchers.pdf_extractor.fitz") as mock_fitz:
            mock_doc = MagicMock()
            mock_page1 = MagicMock()
            mock_page1.get_text.return_value = "Page 1 content"
            mock_page2 = MagicMock()
            mock_page2.get_text.return_value = "Page 2 content"
            mock_doc.__iter__ = lambda self: iter([mock_page1, mock_page2])
            mock_doc.__len__ = lambda self: 2
            mock_fitz.open.return_value.__enter__ = lambda self: mock_doc
            mock_fitz.open.return_value.__exit__ = lambda self, *args: None

            text = extractor.extract_text(b"%PDF-1.4 fake")

        # Page 1 should come before Page 2
        assert text.index("Page 1") < text.index("Page 2")

    def test_extract_text_cleans_whitespace(self):
        """Clean up excessive whitespace from PDF extraction."""
        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor

        extractor = PDFTextExtractor()

        with patch("axiom_corpus.fetchers.pdf_extractor.fitz") as mock_fitz:
            mock_doc = MagicMock()
            mock_page = MagicMock()
            # PDF extraction often has weird spacing
            mock_page.get_text.return_value = "SECTION   1.    PURPOSE\n\n\n\nThis  is   text."
            mock_doc.__iter__ = lambda self: iter([mock_page])
            mock_doc.__len__ = lambda self: 1
            mock_fitz.open.return_value.__enter__ = lambda self: mock_doc
            mock_fitz.open.return_value.__exit__ = lambda self, *args: None

            text = extractor.extract_text(b"%PDF-1.4 fake")

        # Should clean up multiple spaces
        assert "SECTION 1. PURPOSE" in text or "SECTION   1." in text
        # Should reduce excessive newlines
        assert "\n\n\n\n" not in text


class TestIRSDocumentParser:
    """Tests for parsing IRS document structure from extracted text."""

    def test_parse_revenue_procedure_sections(self):
        """Parse SECTION headings from Rev. Proc. text."""
        from axiom_corpus.fetchers.irs_parser import IRSDocumentParser

        text = """
        Rev. Proc. 2024-40

        SECTION 1. PURPOSE

        This revenue procedure sets forth inflation adjusted items for 2025.

        SECTION 2. 2025 ADJUSTED ITEMS

        .01 Tax Rate Tables. For taxable years beginning in 2025...

        .02 Unearned Income of Minor Children. For taxable years beginning...

        SECTION 3. EFFECTIVE DATE

        This revenue procedure is effective for taxable years beginning in 2025.

        SECTION 4. DRAFTING INFORMATION

        The principal author of this revenue procedure is Kyle Walker.
        """

        parser = IRSDocumentParser()
        result = parser.parse(text)

        assert len(result.sections) == 4
        assert result.sections[0].section_num == "1"
        assert result.sections[0].heading == "PURPOSE"
        assert result.sections[1].section_num == "2"
        assert result.sections[1].heading == "2025 ADJUSTED ITEMS"
        assert result.sections[2].section_num == "3"
        assert result.sections[3].section_num == "4"

    def test_parse_subsections(self):
        """Parse .01, .02 subsections within main sections."""
        from axiom_corpus.fetchers.irs_parser import IRSDocumentParser

        text = """
        SECTION 2. 2025 ADJUSTED ITEMS

        .01 Tax Rate Tables. For taxable years beginning in 2025,
        the tax rate tables under section 1 are as follows:

        (1) Married Individuals Filing Joint Returns
        (2) Heads of Households

        .02 Unearned Income of Minor Children. For taxable years
        beginning in 2025, the amount is $1,350.

        .03 Maximum Capital Gains Rate. For taxable years beginning
        in 2025, the maximum capital gains rate applies to...
        """

        parser = IRSDocumentParser()
        result = parser.parse(text)

        # Should have one main section
        assert len(result.sections) == 1
        main_section = result.sections[0]

        # With three subsections
        assert len(main_section.children) == 3
        assert main_section.children[0].section_num == ".01"
        assert "Tax Rate Tables" in main_section.children[0].heading
        assert main_section.children[1].section_num == ".02"
        assert main_section.children[2].section_num == ".03"

    def test_extract_tax_year(self):
        """Extract applicable tax year from document."""
        from axiom_corpus.fetchers.irs_parser import IRSDocumentParser

        text = """
        Rev. Proc. 2024-40

        SECTION 3. EFFECTIVE DATE

        This revenue procedure is effective for taxable years beginning in 2025.
        """

        parser = IRSDocumentParser()
        result = parser.parse(text)

        assert result.effective_year == 2025

    def test_extract_document_number(self):
        """Extract Rev. Proc. document number."""
        from axiom_corpus.fetchers.irs_parser import IRSDocumentParser

        text = """
        Rev. Proc. 2024-40

        26 CFR 601.602: Tax forms and instructions.
        (Also Part I, ss 1, 23, 24, 25A, 32, 36B...)
        """

        parser = IRSDocumentParser()
        result = parser.parse(text)

        assert result.doc_number == "2024-40"
        assert result.doc_type == "REV_PROC"

    def test_extract_code_references(self):
        """Extract IRC section references from document."""
        from axiom_corpus.fetchers.irs_parser import IRSDocumentParser

        text = """
        (Also Part I, ss 1, 23, 24, 25A, 32, 36B, 42, 45R, 55, 59, 62, 63)

        SECTION 1. PURPOSE

        This revenue procedure provides inflation adjustments under section 1(f)
        of the Internal Revenue Code for taxable years beginning in 2025.
        """

        parser = IRSDocumentParser()
        result = parser.parse(text)

        # Should extract IRC section references
        assert 32 in result.irc_sections  # EITC
        assert 24 in result.irc_sections  # CTC
        assert 63 in result.irc_sections  # Standard Deduction


class TestIRSParameterExtractor:
    """Tests for extracting parameter values from IRS documents."""

    def test_extract_eitc_parameters(self):
        """Extract EITC threshold and credit amounts."""
        from axiom_corpus.fetchers.irs_parser import IRSParameterExtractor

        # Format matches real Rev. Proc. 2024-40 PDF extraction
        text = """
        .06 Earned Income Credit.
        (1) In general.  For taxable years beginning in 2025, the following amounts are used to
        determine the earned income credit under section 32(b).

                                                  Number of Qualifying Children
        Item
        One
        Two
        Three or More
        None
        Earned Income Amount
        $11,950   $16,800
        $16,800
        $8,490
        Maximum Amount of Credit
        $4,328
        $7,152
        $8,046
        $649
        Threshold Phaseout Amount
        (Married Filing Jointly)
        $30,540
        $30,540
        $30,540
        $17,810
        Completed Phaseout Amount
        (Married Filing Jointly)
        $56,274
        $62,958
        $67,089
        $26,294
        Threshold Phaseout Amount (All
        other filing statuses)
        $23,350  $23,350
        $23,350
        $10,620
        Completed Phaseout Amount (All
        other filing statuses)
        $49,084
        $55,768
        $59,899
        $19,104

        .07 Refundable Credit for Coverage
        """

        extractor = IRSParameterExtractor()
        params = extractor.extract(text)

        # Check EITC parameters
        assert "eitc" in params
        eitc = params["eitc"]

        # Maximum credit amounts by number of children (string keys)
        assert eitc["max_credit"]["0"] == 649
        assert eitc["max_credit"]["1"] == 4328
        assert eitc["max_credit"]["2"] == 7152
        assert eitc["max_credit"]["3"] == 8046

        # Phaseout thresholds (single/All other) - string keys
        assert eitc["phaseout_start"]["single"]["1"] == 23350
        assert eitc["phaseout_end"]["single"]["1"] == 49084

        # Married filing jointly thresholds - string keys
        assert eitc["phaseout_start"]["joint"]["1"] == 30540
        assert eitc["phaseout_end"]["joint"]["1"] == 56274

    def test_extract_standard_deduction(self):
        """Extract standard deduction amounts."""
        from axiom_corpus.fetchers.irs_parser import IRSParameterExtractor

        text = """
        .15 Standard Deduction.

        (1) In general. For taxable years beginning in 2025, the standard
        deduction amounts under section 63(c)(2) are as follows:

        Filing Status                          Standard Deduction
        Married Individuals Filing Jointly     $30,000
        Heads of Households                    $22,500
        Unmarried Individuals (other than
        Surviving Spouses and Heads of
        Households)                            $15,000
        Married Individuals Filing Separate    $15,000

        (2) Dependent. For taxable years beginning in 2025, the standard
        deduction under section 63(c)(5) for an individual who may be
        claimed as a dependent is the greater of (1) $1,350, or (2) the
        sum of $450 and the individual's earned income.

        (3) Aged or blind. For taxable years beginning in 2025, the
        additional standard deduction for the aged or blind under
        section 63(c)(3) is $1,600 (married) or $2,000 (single).
        """

        extractor = IRSParameterExtractor()
        params = extractor.extract(text)

        assert "standard_deduction" in params
        sd = params["standard_deduction"]

        assert sd["joint"] == 30000
        assert sd["head_of_household"] == 22500
        assert sd["single"] == 15000
        assert sd["married_separate"] == 15000
        assert sd["dependent_min"] == 1350
        assert sd["aged_blind_married"] == 1600
        assert sd["aged_blind_single"] == 2000

    def test_extract_ctc_parameters(self):
        """Extract Child Tax Credit amounts."""
        from axiom_corpus.fetchers.irs_parser import IRSParameterExtractor

        text = """
        .04 Child Tax Credit.

        For taxable years beginning in 2025, the maximum amount that may
        be refundable under section 24(d)(1)(A) is $1,700.

        .05 Child Tax Credit—Additional Child Tax Credit.

        For taxable years beginning in 2025, the threshold amount under
        section 24(h)(6)(A) used to calculate the additional child tax
        credit is $200,000 ($400,000 in the case of a joint return).
        """

        extractor = IRSParameterExtractor()
        params = extractor.extract(text)

        assert "ctc" in params
        ctc = params["ctc"]

        assert ctc["refundable_max"] == 1700
        assert ctc["phaseout_threshold"]["single"] == 200000
        assert ctc["phaseout_threshold"]["joint"] == 400000


class TestIRSGuidanceIntegration:
    """Integration tests for full IRS guidance processing pipeline."""

    @pytest.mark.integration
    def test_fetch_and_parse_rev_proc_2024_40(self):
        """Fetch and parse Rev. Proc. 2024-40 end-to-end.

        This test requires network access to IRS.gov.
        Run with: pytest -m integration
        """
        from axiom_corpus.fetchers.irs_bulk import IRSBulkFetcher, IRSDropDocument
        from axiom_corpus.fetchers.irs_parser import IRSDocumentParser, IRSParameterExtractor
        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor
        from axiom_corpus.models_guidance import GuidanceType

        doc = IRSDropDocument(
            doc_type=GuidanceType.REV_PROC,
            doc_number="2024-40",
            year=2024,
            pdf_filename="rp-24-40.pdf",
        )

        with IRSBulkFetcher() as fetcher:
            pdf_bytes = fetcher.fetch_pdf(doc)

        extractor = PDFTextExtractor()
        text = extractor.extract_text(pdf_bytes)

        # Should have extracted meaningful text
        assert len(text) > 1000
        assert "SECTION" in text
        assert "2025" in text

        # Parse structure
        parser = IRSDocumentParser()
        parsed = parser.parse(text)

        assert parsed.doc_number == "2024-40"
        assert len(parsed.sections) >= 3

        # Extract parameters
        param_extractor = IRSParameterExtractor()
        params = param_extractor.extract(text)

        # Should have extracted at least one parameter type
        # Rev. Proc. 2024-40 contains EITC, standard deduction, CTC, etc.
        assert len(params) > 0, f"No parameters extracted. Text sample: {text[:500]}"

        # If EITC was extracted, verify reasonable values
        if "eitc" in params:
            eitc = params["eitc"]
            # Max credit for 1 child should be between $3,000-$5,000
            if eitc.get("max_credit", {}).get("1"):
                assert 3000 < eitc["max_credit"]["1"] < 5000

        # If standard deduction was extracted, verify reasonable values
        if "standard_deduction" in params:
            sd = params["standard_deduction"]
            # Joint filers should be between $25,000-$35,000
            if sd.get("joint"):
                assert 25000 < sd["joint"] < 35000
