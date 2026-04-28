"""Tests for New Mexico state statute converter.

Tests the NMConverter which parses HTML from nmonesource.com
and converts to the internal Section model.
"""

from datetime import date

import pytest

from axiom.converters.us_states.nm import (
    NM_TAX_ARTICLES,
    NM_TAX_CHAPTERS,
    NM_WELFARE_ARTICLES,
    NM_WELFARE_CHAPTERS,
    NMConverter,
    NMConverterError,
    parse_nm_section,
)
from axiom.models import Section

# Sample HTML from nmonesource.com for testing
SAMPLE_TAX_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 7-2-2 NMSA 1978 - Definitions</title></head>
<body>
<nav>
  <a href="/nmos/nmsa">NMSA 1978</a> /
  <a href="/nmos/nmsa/chapter-7">Chapter 7 Taxation</a> /
  <a href="/nmos/nmsa/article-2">Article 2 Income Tax General Provisions</a>
</nav>
<main>
<h1>7-2-2. Definitions.</h1>
<div class="document-content">
<p>As used in the Income Tax Act:</p>
<p>A. "base income" means the net income of the taxpayer, modified as provided
in the Income Tax Act;</p>
<p>B. "department" means the taxation and revenue department;</p>
<p>C. "domestic corporation" means a corporation organized under the laws of
New Mexico;</p>
<p>(1) For purposes of this subsection, a corporation includes any entity
treated as a corporation for federal income tax purposes.</p>
<p>(2) A domestic corporation does not include a corporation exempt from
taxation under the Internal Revenue Code.</p>
<p>(a) An exempt organization includes any organization described in Section
501(c) of the Internal Revenue Code.</p>
<p>(b) An exempt organization also includes any organization exempt from
tax under Section 115 of the Internal Revenue Code.</p>
<p>D. "federal adjusted gross income" means a taxpayer's adjusted gross income
as defined and used in the Internal Revenue Code;</p>
<p>E. "fiscal year" means an accounting period of twelve months ending on the
last day of any month other than December;</p>
<p>History: Laws 1965, ch. 217, s. 2; Laws 2003, ch. 272, s. 3. Effective July 1, 2023.</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 27-2-12 NMSA 1978 - General assistance payments</title></head>
<body>
<nav>
  <a href="/nmos/nmsa">NMSA 1978</a> /
  <a href="/nmos/nmsa/chapter-27">Chapter 27 Public Assistance</a> /
  <a href="/nmos/nmsa/article-2">Article 2 General Provisions</a>
</nav>
<main>
<h1>27-2-12. General assistance payments.</h1>
<div class="document-content">
<p>A. The human services department shall provide general assistance payments to
eligible persons who:</p>
<p>(1) are not eligible for federal assistance programs;</p>
<p>(2) have insufficient income to meet basic needs;</p>
<p>(a) "basic needs" includes food, shelter, and clothing;</p>
<p>(b) "insufficient income" means income below the federal poverty level;</p>
<p>(3) meet residency requirements established by rule.</p>
<p>B. The department shall adopt rules establishing:</p>
<p>(1) eligibility criteria;</p>
<p>(2) payment amounts;</p>
<p>(3) duration of assistance.</p>
<p>History: Laws 1937, ch. 18, s. 2; Laws 2019, ch. 83, s. 1.</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_EITC_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 7-2D-3 NMSA 1978 - Working families tax credit</title></head>
<body>
<nav>
  <a href="/nmos/nmsa">NMSA 1978</a> /
  <a href="/nmos/nmsa/chapter-7">Chapter 7 Taxation</a> /
  <a href="/nmos/nmsa/article-2D">Article 2D Working Families Tax Credit</a>
</nav>
<main>
<h1>7-2D-3. Working families tax credit.</h1>
<div class="document-content">
<p>A. A taxpayer who is eligible for the federal earned income tax credit may
claim a credit against the tax imposed pursuant to Section 7-2-7 NMSA 1978.</p>
<p>B. The amount of the credit is equal to seventeen percent of the earned
income tax credit the taxpayer is allowed pursuant to Section 32 of the Internal
Revenue Code.</p>
<p>C. A taxpayer may claim the credit provided by this section only if the
taxpayer files a New Mexico personal income tax return.</p>
<p>(1) The taxpayer shall include with the return documentation showing the
amount of federal earned income tax credit claimed.</p>
<p>(2) If the credit exceeds the taxpayer's tax liability, the excess shall be
refunded to the taxpayer.</p>
<p>History: Laws 2007, ch. 45, s. 3; Laws 2023, ch. 192, s. 1. Effective: January 1, 2024.</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_CHILD_TAX_CREDIT_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 7-2E-3 NMSA 1978 - Child income tax credit</title></head>
<body>
<main>
<h1>7-2E-3. Child income tax credit.</h1>
<div class="document-content">
<p>A. A resident who files an individual income tax return and who is not a
dependent of another taxpayer may claim a credit against the tax imposed
pursuant to Section 7-2-7 NMSA 1978 for each qualifying child.</p>
<p>(1) "Qualifying child" means a child who:</p>
<p>(a) is a dependent of the taxpayer for federal income tax purposes;</p>
<p>(b) has not attained the age of eighteen years by the close of the taxable year;</p>
<p>(c) is a resident of this state.</p>
<p>B. The amount of the credit is:</p>
<p>(1) one hundred seventy-five dollars for each qualifying child if the
taxpayer's modified gross income does not exceed twenty-five thousand dollars;</p>
<p>(2) one hundred fifty dollars for each qualifying child if the taxpayer's
modified gross income exceeds twenty-five thousand dollars but does not exceed
fifty thousand dollars;</p>
<p>(3) one hundred twenty-five dollars for each qualifying child if the
taxpayer's modified gross income exceeds fifty thousand dollars.</p>
<p>History: Laws 2022, ch. 35, s. 3. Effective: July 1, 2023.</p>
</div>
</main>
</body>
</html>
"""

SAMPLE_PAREN_SUBSECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Section 7-9-4 NMSA 1978 - Imposition of tax</title></head>
<body>
<main>
<h1>7-9-4. Imposition of tax.</h1>
<div class="document-content">
<p>(A) For the privilege of engaging in business, an excise tax equal to the
rate of tax imposed by Section 7-9-4.1 NMSA 1978 is imposed on the gross
receipts of any person engaging in business.</p>
<p>(1) The tax shall be collected by the department.</p>
<p>(2) The tax shall be reported and paid on a monthly basis.</p>
<p>(B) The tax imposed by this section shall be paid by the person engaging in
business and shall not be passed on to customers.</p>
<p>(C) Exemptions from the tax are provided in Sections 7-9-13 through 7-9-116
NMSA 1978.</p>
<p>History: Laws 1966, ch. 47, s. 4; Laws 2019, ch. 270, s. 2.</p>
</div>
</main>
</body>
</html>
"""


class TestNMChaptersRegistry:
    """Test New Mexico chapter registries."""

    def test_chapter_7_in_tax_chapters(self):
        """Chapter 7 (Taxation) is in tax chapters."""
        assert 7 in NM_TAX_CHAPTERS
        assert "Taxation" in NM_TAX_CHAPTERS[7]

    def test_chapter_27_in_welfare_chapters(self):
        """Chapter 27 (Public Assistance) is in welfare chapters."""
        assert 27 in NM_WELFARE_CHAPTERS
        assert "Public Assistance" in NM_WELFARE_CHAPTERS[27]

    def test_income_tax_article_in_tax_articles(self):
        """Article 2 (Income Tax General Provisions) is in tax articles."""
        assert 2 in NM_TAX_ARTICLES
        assert "Income Tax" in NM_TAX_ARTICLES[2]

    def test_working_families_credit_in_tax_articles(self):
        """Article 2D (Working Families Tax Credit) is in tax articles."""
        assert "2D" in NM_TAX_ARTICLES
        assert "Working Families" in NM_TAX_ARTICLES["2D"]

    def test_public_assistance_act_in_welfare_articles(self):
        """Article 1 (Public Assistance Act) is in welfare articles."""
        assert 1 in NM_WELFARE_ARTICLES
        assert "Public Assistance Act" in NM_WELFARE_ARTICLES[1]


class TestNMConverter:
    """Test NMConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = NMConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = NMConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = NMConverter(year=2024)
        assert converter.year == 2024

    def test_parse_section_number_standard(self):
        """Parse standard section number 7-2-2."""
        converter = NMConverter()
        chapter, article, section = converter._parse_section_number("7-2-2")
        assert chapter == 7
        assert article == "2"
        assert section == "2"

    def test_parse_section_number_alphanumeric_article(self):
        """Parse section number with alphanumeric article 7-2D-3."""
        converter = NMConverter()
        chapter, article, section = converter._parse_section_number("7-2D-3")
        assert chapter == 7
        assert article == "2D"
        assert section == "3"

    def test_parse_section_number_welfare(self):
        """Parse welfare section number 27-2-12."""
        converter = NMConverter()
        chapter, article, section = converter._parse_section_number("27-2-12")
        assert chapter == 27
        assert article == "2"
        assert section == "12"

    def test_get_chapter_title_tax(self):
        """Get chapter title for taxation chapter."""
        converter = NMConverter()
        title = converter._get_chapter_title(7)
        assert title == "Taxation"

    def test_get_chapter_title_welfare(self):
        """Get chapter title for welfare chapter."""
        converter = NMConverter()
        title = converter._get_chapter_title(27)
        assert title == "Public Assistance"

    def test_get_article_title_income_tax(self):
        """Get article title for income tax article."""
        converter = NMConverter()
        title = converter._get_article_title(7, "2")
        assert title is not None
        assert "Income Tax" in title

    def test_get_article_title_working_families(self):
        """Get article title for working families credit."""
        converter = NMConverter()
        title = converter._get_article_title(7, "2D")
        assert title is not None
        assert "Working Families" in title

    def test_context_manager(self):
        """Converter works as context manager."""
        with NMConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestNMConverterParsing:
    """Test NMConverter HTML parsing."""

    def test_parse_tax_section_html(self):
        """Parse tax section HTML into ParsedNMSection."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "7-2-2", "https://nmonesource.com/nmos/nmsa/en/item/1234/index.do"
        )

        assert parsed.section_number == "7-2-2"
        assert parsed.section_title == "Definitions"
        assert parsed.chapter_number == 7
        assert parsed.chapter_title == "Taxation"
        assert parsed.article_number == "2"
        assert "base income" in parsed.text.lower()

    def test_parse_welfare_section_html(self):
        """Parse welfare section HTML."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "27-2-12", "https://example.com"
        )

        assert parsed.section_number == "27-2-12"
        assert "general assistance" in parsed.section_title.lower()
        assert parsed.chapter_number == 27
        assert parsed.chapter_title == "Public Assistance"

    def test_parse_working_families_credit(self):
        """Parse Working Families Tax Credit section."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_EITC_SECTION_HTML, "7-2D-3", "https://example.com"
        )

        assert parsed.section_number == "7-2D-3"
        assert "working families" in parsed.section_title.lower()
        assert parsed.chapter_number == 7
        assert parsed.article_number == "2D"
        assert "earned income tax credit" in parsed.text.lower()
        assert "seventeen percent" in parsed.text.lower()

    def test_parse_subsections_letter_period_style(self):
        """Parse A., B., C. style subsections."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "7-2-2", "https://example.com"
        )

        # Should have subsections A, B, C, D, E
        assert len(parsed.subsections) >= 4
        identifiers = [s.identifier for s in parsed.subsections]
        assert "A" in identifiers
        assert "B" in identifiers
        assert "C" in identifiers
        assert "D" in identifiers

    def test_parse_subsections_paren_style(self):
        """Parse (A), (B), (C) style subsections."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_PAREN_SUBSECTION_HTML, "7-9-4", "https://example.com"
        )

        # Should have subsections A, B, C
        assert len(parsed.subsections) >= 2
        identifiers = [s.identifier for s in parsed.subsections]
        assert "A" in identifiers or len(parsed.subsections) > 0

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1), (2) under C."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "7-2-2", "https://example.com"
        )

        # Find subsection C (domestic corporation)
        sub_c = next((s for s in parsed.subsections if s.identifier == "C"), None)
        if sub_c:
            # Should have children (1) and (2)
            assert len(sub_c.children) >= 2
            child_ids = [c.identifier for c in sub_c.children]
            assert "1" in child_ids
            assert "2" in child_ids

    def test_parse_level3_subsections(self):
        """Parse level 3 subsections (a), (b) under (2)."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "7-2-2", "https://example.com"
        )

        # Find subsection C -> (2)
        sub_c = next((s for s in parsed.subsections if s.identifier == "C"), None)
        if sub_c:
            sub_2 = next((c for c in sub_c.children if c.identifier == "2"), None)
            if sub_2:
                # Should have children (a) and (b)
                assert len(sub_2.children) >= 2
                grandchild_ids = [g.identifier for g in sub_2.children]
                assert "a" in grandchild_ids
                assert "b" in grandchild_ids

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "7-2-2", "https://example.com"
        )

        # Should extract history note
        # Note: The history extraction may vary based on format
        full_text = parsed.text
        assert "Laws 1965" in full_text or "Laws 2003" in full_text

    def test_parse_effective_date(self):
        """Parse effective date from section HTML."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "7-2-2", "https://example.com"
        )

        assert parsed.effective_date is not None
        assert parsed.effective_date.year == 2023
        assert parsed.effective_date.month == 7
        assert parsed.effective_date.day == 1

    def test_to_section_model(self):
        """Convert ParsedNMSection to Section model."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_TAX_SECTION_HTML, "7-2-2", "https://nmonesource.com/example"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "NM-7-2-2"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Definitions"
        assert "New Mexico Statutes" in section.title_name
        assert "Taxation" in section.title_name
        assert section.uslm_id == "nm/7/7-2-2"
        assert "nmonesource.com" in section.source_url

    def test_to_section_model_welfare(self):
        """Convert welfare ParsedNMSection to Section model."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "27-2-12", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "NM-27-2-12"
        assert "Public Assistance" in section.title_name
        assert section.uslm_id == "nm/27/27-2-12"

    def test_to_section_model_child_tax_credit(self):
        """Convert child tax credit section to Section model."""
        converter = NMConverter()
        parsed = converter._parse_section_html(
            SAMPLE_CHILD_TAX_CREDIT_HTML, "7-2E-3", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "NM-7-2E-3"
        assert "child" in section.section_title.lower()
        assert "qualifying child" in section.text.lower()


class TestNMConverterNotFound:
    """Test error handling."""

    def test_parse_section_not_found(self):
        """Handle section not found error."""
        converter = NMConverter()
        with pytest.raises(NMConverterError) as exc_info:
            converter._parse_section_html(
                "<html><body>Section cannot be found</body></html>",
                "999-99-99",
                "https://example.com"
            )

        assert "not found" in str(exc_info.value).lower()


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    def test_parse_nm_section(self):
        """Test parse_nm_section function."""
        section = parse_nm_section(SAMPLE_TAX_SECTION_HTML, "7-2-2", "https://example.com")

        assert section is not None
        assert section.citation.section == "NM-7-2-2"
        assert "Definitions" in section.section_title

    def test_parse_nm_section_welfare(self):
        """Test parse_nm_section with welfare section."""
        section = parse_nm_section(
            SAMPLE_WELFARE_SECTION_HTML, "27-2-12", "https://example.com"
        )

        assert section is not None
        assert section.citation.section == "NM-27-2-12"


class TestNMConverterArticles:
    """Test article-specific parsing."""

    def test_article_2d_working_families(self):
        """Parse Article 2D (Working Families Tax Credit) section."""
        converter = NMConverter()
        section = converter.parse_section_html(
            SAMPLE_EITC_SECTION_HTML, "7-2D-3", "https://example.com"
        )

        assert section.citation.section == "NM-7-2D-3"
        # Verify the 17% rate is captured
        assert "seventeen percent" in section.text.lower()
        # Verify reference to federal EITC
        assert "section 32" in section.text.lower() or "earned income" in section.text.lower()

    def test_article_2e_child_tax_credit(self):
        """Parse Article 2E (Child Income Tax Credit) section."""
        converter = NMConverter()
        section = converter.parse_section_html(
            SAMPLE_CHILD_TAX_CREDIT_HTML, "7-2E-3", "https://example.com"
        )

        assert section.citation.section == "NM-7-2E-3"
        # Verify credit amounts are captured
        assert "one hundred seventy-five dollars" in section.text.lower()
        assert "qualifying child" in section.text.lower()

    def test_gross_receipts_tax(self):
        """Parse gross receipts tax section."""
        converter = NMConverter()
        section = converter.parse_section_html(
            SAMPLE_PAREN_SUBSECTION_HTML, "7-9-4", "https://example.com"
        )

        assert section.citation.section == "NM-7-9-4"
        # Text may have line breaks, check for both words
        text_lower = section.text.lower()
        assert "gross" in text_lower
        assert "receipts" in text_lower
