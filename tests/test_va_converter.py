"""Tests for Virginia state statute converter.

Tests the VAConverter which fetches from law.lis.virginia.gov
and converts to the internal Section model.
"""

from datetime import date
from unittest.mock import patch

import pytest

from axiom.converters.us_states.va import (
    VA_TAX_TITLES,
    VA_TITLES,
    VA_WELFARE_TITLES,
    VAConverter,
    VAConverterError,
    download_va_chapter,
    fetch_va_section,
)
from axiom.models import Section

# Sample HTML from law.lis.virginia.gov for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Code of Virginia - § 58.1-301. Conformity to Internal Revenue Code</title></head>
<body>
<nav class="breadcrumb">
    <a href="/vacode/">Code of Virginia</a> »
    <a href="/vacode/title58.1/">Title 58.1. Taxation</a> »
    <a href="/vacode/title58.1/chapter3/">Chapter 3. Income Tax</a> »
    <a href="/vacode/title58.1/chapter3/article2/">Article 2. Conformity</a>
</nav>
<h1>§ 58.1-301. Conformity to Internal Revenue Code</h1>
<div id="vacode">
<p>A. Any term used in this chapter shall have the same meaning as when used in a comparable context in the Internal Revenue Code of 1954, and other provisions of the laws of the United States relating to federal income taxes, unless a different meaning is clearly required.</p>
<p>1. For purposes of this section, any reference to the Internal Revenue Code shall mean the Code as it exists on the date specified in § 58.1-301.</p>
<p>2. The Commissioner shall promulgate regulations as necessary to implement this section.</p>
<p>a. Such regulations shall be consistent with federal regulations.</p>
<p>b. Public comment periods shall be provided.</p>
<p>B. The Governor, by Executive Order, may provide that certain provisions of the Internal Revenue Code enacted after the date specified above shall apply to Virginia taxation.</p>
<p>1. Any such Executive Order shall be submitted to the General Assembly.</p>
<p>Code 1950, §§ 58-151.03, 58-151.011; 1971, Ex. Sess., c. 171; 1984, c. 675; 1987, cc. 9, 478; 2023, cc. 1, 763.</p>
</div>
</body>
</html>
"""

SAMPLE_WELFARE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>Code of Virginia - § 63.2-100. Definitions</title></head>
<body>
<nav class="breadcrumb">
    <a href="/vacode/">Code of Virginia</a> »
    <a href="/vacode/title63.2/">Title 63.2. Welfare (Social Services)</a> »
    <a href="/vacode/title63.2/chapter1/">Chapter 1. General Provisions</a>
</nav>
<h1>§ 63.2-100. Definitions</h1>
<div id="vacode">
<p>As used in this title, unless the context requires a different meaning:</p>
<p>"Abused or neglected child" means any child:</p>
<p>(1) Whose parents or other person responsible for his care creates or inflicts, threatens to create or inflict, or allows to be created or inflicted upon such child a physical or mental injury.</p>
<p>(2) Whose parents or other person responsible for his care neglects or refuses to provide care necessary for his health.</p>
<p>(3) Whose parents or other person responsible for his care abandons such child.</p>
<p>"Board" means the State Board of Social Services.</p>
<p>"Commissioner" means the Commissioner of Social Services.</p>
<p>Code 1950, §§ 63-101, 63-102; 1968, c. 578; 2024, cc. 37, 89.</p>
</div>
</body>
</html>
"""

SAMPLE_TITLE_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Code of Virginia - Title 58.1. Taxation</title></head>
<body>
<h1>Title 58.1. Taxation</h1>
<div id="contents">
<ul>
<li><a href="/vacode/58.1-1/">§ 58.1-1. Definitions</a></li>
<li><a href="/vacode/58.1-2/">§ 58.1-2. General provisions</a></li>
<li><a href="/vacode/58.1-300/">§ 58.1-300. Imposition of tax</a></li>
<li><a href="/vacode/58.1-301/">§ 58.1-301. Conformity to Internal Revenue Code</a></li>
<li><a href="/vacode/58.1-320/">§ 58.1-320. Tax rates</a></li>
</ul>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>Code of Virginia - Title 58.1. Taxation - Chapter 3. Income Tax</title></head>
<body>
<h1>Chapter 3. Income Tax</h1>
<div id="contents">
<ul>
<li><a href="/vacode/58.1-300/">§ 58.1-300. Imposition of tax</a></li>
<li><a href="/vacode/58.1-301/">§ 58.1-301. Conformity to Internal Revenue Code</a></li>
<li><a href="/vacode/58.1-302/">§ 58.1-302. Definitions</a></li>
<li><a href="/vacode/58.1-320/">§ 58.1-320. Tax rates</a></li>
</ul>
</div>
</body>
</html>
"""


class TestVATitlesRegistry:
    """Test Virginia title registries."""

    def test_title_58_1_in_tax_titles(self):
        """Title 58.1 (Taxation) is in tax titles."""
        assert "58.1" in VA_TAX_TITLES
        assert "Taxation" in VA_TAX_TITLES["58.1"]

    def test_title_63_2_in_welfare_titles(self):
        """Title 63.2 (Welfare) is in welfare titles."""
        assert "63.2" in VA_WELFARE_TITLES
        assert "Welfare" in VA_WELFARE_TITLES["63.2"]

    def test_titles_have_names(self):
        """All registered titles have names."""
        for title_num, title_name in VA_TITLES.items():
            assert title_name, f"Title {title_num} has no name"


class TestVAConverter:
    """Test VAConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = VAConverter()
        assert converter.rate_limit_delay == 0.5
        assert converter.year == date.today().year

    def test_init_custom_rate_limit(self):
        """Converter accepts custom rate limit."""
        converter = VAConverter(rate_limit_delay=1.0)
        assert converter.rate_limit_delay == 1.0

    def test_init_custom_year(self):
        """Converter accepts custom year."""
        converter = VAConverter(year=2024)
        assert converter.year == 2024

    def test_build_section_url(self):
        """Build correct URL for section fetch."""
        converter = VAConverter()
        url = converter._build_section_url("58.1-301")
        assert "law.lis.virginia.gov" in url
        assert "/vacode/58.1-301/" in url

    def test_build_section_url_welfare(self):
        """Build correct URL for welfare section."""
        converter = VAConverter()
        url = converter._build_section_url("63.2-100")
        assert "/vacode/63.2-100/" in url

    def test_build_title_url(self):
        """Build correct URL for title index."""
        converter = VAConverter()
        url = converter._build_title_url("58.1")
        assert "/vacode/title58.1/" in url

    def test_build_chapter_url(self):
        """Build correct URL for chapter index."""
        converter = VAConverter()
        url = converter._build_chapter_url("58.1", "3")
        assert "/vacode/title58.1/chapter3/" in url

    def test_extract_title_from_section(self):
        """Extract title number from section number."""
        converter = VAConverter()
        assert converter._extract_title_from_section("58.1-301") == "58.1"
        assert converter._extract_title_from_section("63.2-100") == "63.2"
        assert converter._extract_title_from_section("30-1") == "30"

    def test_context_manager(self):
        """Converter works as context manager."""
        with VAConverter() as converter:
            assert converter is not None
        # Client should be closed
        assert converter._client is None


class TestVAConverterParsing:
    """Test VAConverter HTML parsing."""

    def test_parse_section_html(self):
        """Parse section HTML into ParsedVASection."""
        converter = VAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "58.1-301", "https://example.com"
        )

        assert parsed.section_number == "58.1-301"
        assert parsed.section_title == "Conformity to Internal Revenue Code"
        assert parsed.title_number == "58.1"
        assert parsed.title_name == "Taxation"
        assert "Internal Revenue Code" in parsed.text
        assert parsed.source_url == "https://example.com"

    def test_parse_welfare_section_html(self):
        """Parse welfare section HTML."""
        converter = VAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "63.2-100", "https://example.com"
        )

        assert parsed.section_number == "63.2-100"
        assert parsed.section_title == "Definitions"
        assert parsed.title_number == "63.2"
        assert parsed.title_name == "Welfare (Social Services)"
        assert "Abused or neglected child" in parsed.text

    def test_parse_subsections_uppercase(self):
        """Parse uppercase letter subsections (A., B., etc.)."""
        converter = VAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "58.1-301", "https://example.com"
        )

        # Should have subsections A and B
        assert len(parsed.subsections) >= 2
        assert any(s.identifier == "A" for s in parsed.subsections)
        assert any(s.identifier == "B" for s in parsed.subsections)

    def test_parse_nested_subsections(self):
        """Parse nested subsections (1., 2. under A.)."""
        converter = VAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "58.1-301", "https://example.com"
        )

        # Find subsection A
        sub_a = next((s for s in parsed.subsections if s.identifier == "A"), None)
        assert sub_a is not None
        # Should have children 1 and 2
        assert len(sub_a.children) >= 2
        assert any(c.identifier == "1" for c in sub_a.children)
        assert any(c.identifier == "2" for c in sub_a.children)

    def test_parse_level3_subsections(self):
        """Parse level 3 subsections (a., b. under 2.)."""
        converter = VAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "58.1-301", "https://example.com"
        )

        # Find subsection A
        sub_a = next((s for s in parsed.subsections if s.identifier == "A"), None)
        assert sub_a is not None

        # Find subsection 2 under A
        sub_2 = next((c for c in sub_a.children if c.identifier == "2"), None)
        assert sub_2 is not None

        # Should have children a and b
        assert len(sub_2.children) >= 2
        assert any(c.identifier == "a" for c in sub_2.children)
        assert any(c.identifier == "b" for c in sub_2.children)

    def test_parse_numbered_subsections_fallback(self):
        """Parse numbered subsections (1), (2), etc. as fallback."""
        converter = VAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_WELFARE_SECTION_HTML, "63.2-100", "https://example.com"
        )

        # This section uses (1), (2), (3) format
        assert len(parsed.subsections) >= 3
        assert any(s.identifier == "1" for s in parsed.subsections)
        assert any(s.identifier == "2" for s in parsed.subsections)

    def test_parse_history(self):
        """Parse history note from section HTML."""
        converter = VAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "58.1-301", "https://example.com"
        )

        assert parsed.history is not None
        assert "Code 1950" in parsed.history

    def test_to_section_model(self):
        """Convert ParsedVASection to Section model."""
        converter = VAConverter()
        parsed = converter._parse_section_html(
            SAMPLE_SECTION_HTML, "58.1-301", "https://example.com"
        )
        section = converter._to_section(parsed)

        assert isinstance(section, Section)
        assert section.citation.section == "VA-58.1-301"
        assert section.citation.title == 0  # State law indicator
        assert section.section_title == "Conformity to Internal Revenue Code"
        assert "Code of Virginia" in section.title_name
        assert section.uslm_id == "va/58.1/58.1-301"
        assert section.source_url == "https://example.com"


class TestVAConverterFetching:
    """Test VAConverter HTTP fetching with mocks."""

    @patch.object(VAConverter, "_get")
    def test_fetch_section(self, mock_get):
        """Fetch and parse a single section."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        converter = VAConverter()
        section = converter.fetch_section("58.1-301")

        assert section is not None
        assert isinstance(section, Section)
        assert section.citation.section == "VA-58.1-301"
        assert "Conformity" in section.section_title

    @patch.object(VAConverter, "_get")
    def test_fetch_section_not_found(self, mock_get):
        """Handle section not found error."""
        mock_get.return_value = "<html><body>Section cannot be found</body></html>"

        converter = VAConverter()
        with pytest.raises(VAConverterError) as exc_info:
            converter.fetch_section("999.99-999")

        assert "not found" in str(exc_info.value).lower()

    @patch.object(VAConverter, "_get")
    def test_get_title_section_numbers(self, mock_get):
        """Get list of section numbers from title index."""
        mock_get.return_value = SAMPLE_TITLE_INDEX_HTML

        converter = VAConverter()
        sections = converter.get_title_section_numbers("58.1")

        assert len(sections) == 5
        assert "58.1-1" in sections
        assert "58.1-301" in sections
        assert "58.1-320" in sections

    @patch.object(VAConverter, "_get")
    def test_get_chapter_section_numbers(self, mock_get):
        """Get list of section numbers from chapter index."""
        mock_get.return_value = SAMPLE_CHAPTER_INDEX_HTML

        converter = VAConverter()
        sections = converter.get_chapter_section_numbers("58.1", "3")

        assert len(sections) == 4
        assert "58.1-300" in sections
        assert "58.1-301" in sections
        assert "58.1-320" in sections

    @patch.object(VAConverter, "_get")
    def test_iter_title(self, mock_get):
        """Iterate over sections in a title."""
        # First call returns index, subsequent calls return section HTML
        mock_get.side_effect = [
            SAMPLE_TITLE_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = VAConverter()
        sections = list(converter.iter_title("58.1"))

        assert len(sections) == 5
        assert all(isinstance(s, Section) for s in sections)

    @patch.object(VAConverter, "_get")
    def test_iter_chapter(self, mock_get):
        """Iterate over sections in a chapter."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        converter = VAConverter()
        sections = list(converter.iter_chapter("58.1", "3"))

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    @patch.object(VAConverter, "_get")
    def test_fetch_va_section(self, mock_get):
        """Test fetch_va_section function."""
        mock_get.return_value = SAMPLE_SECTION_HTML

        section = fetch_va_section("58.1-301")

        assert section is not None
        assert section.citation.section == "VA-58.1-301"

    @patch.object(VAConverter, "_get")
    def test_download_va_chapter(self, mock_get):
        """Test download_va_chapter function."""
        mock_get.side_effect = [
            SAMPLE_CHAPTER_INDEX_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
            SAMPLE_SECTION_HTML,
        ]

        sections = download_va_chapter("58.1", "3")

        assert len(sections) == 4
        assert all(isinstance(s, Section) for s in sections)


class TestVAConverterIntegration:
    """Integration tests that hit real law.lis.virginia.gov (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_income_tax_section(self):
        """Fetch Virginia Income Tax section 58.1-301."""
        converter = VAConverter()
        section = converter.fetch_section("58.1-301")

        assert section is not None
        assert section.citation.section == "VA-58.1-301"
        assert "internal revenue" in section.section_title.lower() or "conformity" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_tax_rates_section(self):
        """Fetch Virginia tax rates section 58.1-320."""
        converter = VAConverter()
        section = converter.fetch_section("58.1-320")

        assert section is not None
        assert section.citation.section == "VA-58.1-320"
        assert "rate" in section.text.lower() or "percent" in section.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_welfare_section(self):
        """Fetch Virginia welfare section 63.2-100."""
        converter = VAConverter()
        section = converter.fetch_section("63.2-100")

        assert section is not None
        assert section.citation.section == "VA-63.2-100"
        assert "definition" in section.section_title.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_title_sections(self):
        """Get list of sections in Title 58.1 (limited check)."""
        converter = VAConverter()
        sections = converter.get_title_section_numbers("58.1")

        assert len(sections) > 0
        # All sections should start with "58.1-"
        assert all(s.startswith("58.1-") for s in sections)
