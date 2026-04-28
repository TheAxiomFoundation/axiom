"""Tests for IRS guidance fetcher."""

from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from axiom.fetchers.irs_guidance import IRSGuidanceFetcher


class TestIRSGuidanceFetcherInit:
    def test_init(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            fetcher = IRSGuidanceFetcher()
            assert fetcher.base_url == "https://www.irs.gov"


class TestFindIrbUrl:
    def test_known_url(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            fetcher = IRSGuidanceFetcher()
            url = fetcher._find_irb_url("2023-34")
            assert url is not None
            assert "2023-48_IRB" in url

    def test_unknown_url(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            fetcher = IRSGuidanceFetcher()
            url = fetcher._find_irb_url("9999-99")
            assert url is None


class TestExtractTitle:
    def test_extracts_title(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            fetcher = IRSGuidanceFetcher()
            tag = BeautifulSoup(
                "<h2>Rev. Proc. 2023-34. Inflation Adjustments</h2>", "html.parser"
            ).find("h2")
            title = fetcher._extract_title(tag)
            assert title == "Inflation Adjustments"

    def test_strips_whitespace(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            fetcher = IRSGuidanceFetcher()
            tag = BeautifulSoup(
                "<h2>Rev. Proc. 2024-40  Cost of Living  </h2>", "html.parser"
            ).find("h2")
            title = fetcher._extract_title(tag)
            assert title.strip() != ""


class TestIsNextDocument:
    @pytest.fixture
    def fetcher(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            return IRSGuidanceFetcher()

    def test_rev_proc_heading(self, fetcher):
        tag = BeautifulSoup("<h2>Rev. Proc. 2024-40</h2>", "html.parser").find("h2")
        assert fetcher._is_next_document(tag) is True

    def test_rev_rul_heading(self, fetcher):
        tag = BeautifulSoup("<h2>Rev. Rul. 2024-10</h2>", "html.parser").find("h2")
        assert fetcher._is_next_document(tag) is True

    def test_notice_heading(self, fetcher):
        tag = BeautifulSoup("<h2>Notice 2024-50</h2>", "html.parser").find("h2")
        assert fetcher._is_next_document(tag) is True

    def test_announcement_heading(self, fetcher):
        tag = BeautifulSoup("<h3>Announcement 2024-30</h3>", "html.parser").find("h3")
        assert fetcher._is_next_document(tag) is True

    def test_regular_paragraph(self, fetcher):
        tag = BeautifulSoup("<p>Regular content text.</p>", "html.parser").find("p")
        assert fetcher._is_next_document(tag) is False

    def test_none_tag(self, fetcher):
        assert fetcher._is_next_document(None) is False


class TestParseSections:
    @pytest.fixture
    def fetcher(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            return IRSGuidanceFetcher()

    def test_parse_sections_basic(self, fetcher):
        html = """
        <div>
        <p>SECTION 1. PURPOSE</p>
        <p>This procedure provides guidance.</p>
        <p>SECTION 2. BACKGROUND</p>
        <p>The IRS established these rules.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        elements = soup.find_all("p")
        sections = fetcher._parse_sections(elements)
        assert len(sections) == 2
        assert sections[0].section_num == "1"
        assert sections[0].heading == "PURPOSE"
        assert sections[1].section_num == "2"

    def test_parse_sections_with_subsections(self, fetcher):
        html = """
        <div>
        <p>SECTION 1. PURPOSE</p>
        <p>.01 First subsection text.</p>
        <p>.02 Second subsection text.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        elements = soup.find_all("p")
        sections = fetcher._parse_sections(elements)
        assert len(sections) == 1
        assert len(sections[0].children) == 2
        assert sections[0].children[0].section_num == ".01"

    def test_parse_sections_empty(self, fetcher):
        sections = fetcher._parse_sections([])
        assert sections == []

    def test_parse_sections_accumulates_text(self, fetcher):
        html = """
        <div>
        <p>SECTION 1. PURPOSE</p>
        <p>First paragraph of text.</p>
        <p>Second paragraph of text.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        elements = soup.find_all("p")
        sections = fetcher._parse_sections(elements)
        assert "First paragraph" in sections[0].text
        assert "Second paragraph" in sections[0].text


class TestExtractTaxYears:
    @pytest.fixture
    def fetcher(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            return IRSGuidanceFetcher()

    def test_extracts_years_from_text(self, fetcher):
        text = "These amounts apply for tax year 2024 and tax year 2025."
        years = fetcher._extract_tax_years(text, 2023)
        assert 2024 in years
        assert 2025 in years

    def test_filters_to_reasonable_range(self, fetcher):
        text = "Published in 2023. Applies to 2024. Historical data from 2010."
        years = fetcher._extract_tax_years(text, 2023)
        assert 2010 not in years
        assert 2024 in years

    def test_default_next_year(self, fetcher):
        text = "No specific year mentioned."
        years = fetcher._extract_tax_years(text, 2023)
        assert years == [2024]


class TestExtractSubjectAreas:
    @pytest.fixture
    def fetcher(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            return IRSGuidanceFetcher()

    def test_eitc_subject(self, fetcher):
        subjects = fetcher._extract_subject_areas(
            "Earned Income Credit", "The EITC amounts."
        )
        assert "EITC" in subjects

    def test_ctc_subject(self, fetcher):
        subjects = fetcher._extract_subject_areas(
            "Child Tax Credit", "The CTC amounts."
        )
        assert "CTC" in subjects

    def test_inflation_subject(self, fetcher):
        subjects = fetcher._extract_subject_areas(
            "Cost-of-Living Adjustments", "Inflation adjustment amounts."
        )
        assert "Inflation Adjustment" in subjects

    def test_general_fallback(self, fetcher):
        subjects = fetcher._extract_subject_areas(
            "Technical Correction", "Some obscure technical text."
        )
        assert subjects == ["General"]

    def test_multiple_subjects(self, fetcher):
        subjects = fetcher._extract_subject_areas(
            "Income Tax and Standard Deduction", "The standard deduction for income tax."
        )
        assert "Income Tax" in subjects
        assert "Standard Deduction" in subjects


class TestFetchRevenueProcedure:
    def test_fetch_unknown_doc(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            fetcher = IRSGuidanceFetcher()
            with pytest.raises(ValueError, match="Could not find IRB URL"):
                fetcher.fetch_revenue_procedure("9999-99")

    def test_fetch_heading_not_found(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            fetcher = IRSGuidanceFetcher()
            mock_response = MagicMock()
            mock_response.text = "<html><body><p>No Rev. Proc. here</p></body></html>"
            mock_response.raise_for_status = MagicMock()
            fetcher.client.get.return_value = mock_response

            with pytest.raises(ValueError, match="Could not find Rev. Proc."):
                fetcher.fetch_revenue_procedure("2023-34")


class TestContextManager:
    def test_context_manager(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            with IRSGuidanceFetcher() as fetcher:
                assert fetcher is not None
            fetcher.client.close.assert_called_once()

    def test_close(self):
        with patch("axiom.fetchers.irs_guidance.httpx.Client"):
            fetcher = IRSGuidanceFetcher()
            fetcher.close()
            fetcher.client.close.assert_called_once()
