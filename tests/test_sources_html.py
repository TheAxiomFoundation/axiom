"""Tests for the HTML source adapter.

Tests cover URL building, content finding, title/history extraction,
subsection parsing, section fetching, and section listing.
"""

from unittest.mock import MagicMock, patch

import pytest

from axiom_corpus.sources.base import SourceConfig
from axiom_corpus.sources.html import HTMLSource


def _make_config(**kwargs):
    defaults = {
        "jurisdiction": "us-oh",
        "name": "Ohio",
        "source_type": "html",
        "base_url": "https://codes.ohio.gov",
        "section_url_pattern": "/orc/{section}",
        "toc_url_pattern": "/orc/toc/{code}",
        "content_selector": ".section-content",
        "title_selector": "h1",
        "history_selector": ".history",
    }
    defaults.update(kwargs)
    return SourceConfig(**defaults)


class TestHTMLSourceURLs:
    def test_build_section_url(self):
        source = HTMLSource(_make_config())
        url = source._build_section_url("orc", "5747.02")
        assert url == "https://codes.ohio.gov/orc/5747.02"

    def test_build_section_url_absolute(self):
        config = _make_config(section_url_pattern="https://example.com/section/{section}")
        source = HTMLSource(config)
        url = source._build_section_url("orc", "5747.02")
        assert url == "https://example.com/section/5747.02"

    def test_build_section_url_no_pattern_raises(self):
        config = _make_config(section_url_pattern=None)
        source = HTMLSource(config)
        with pytest.raises(ValueError, match="section_url_pattern"):
            source._build_section_url("orc", "5747.02")

    def test_build_toc_url(self):
        source = HTMLSource(_make_config())
        url = source._build_toc_url("title57")
        assert url == "https://codes.ohio.gov/orc/toc/title57"

    def test_build_toc_url_no_pattern_raises(self):
        config = _make_config(toc_url_pattern=None)
        source = HTMLSource(config)
        with pytest.raises(ValueError, match="toc_url_pattern"):
            source._build_toc_url("title57")


class TestHTMLSourceParsing:
    def test_find_content_with_selector(self):
        from bs4 import BeautifulSoup

        html = '<div class="section-content"><p>Section text</p></div>'
        soup = BeautifulSoup(html, "html.parser")
        source = HTMLSource(_make_config())
        content = source._find_content(soup)
        assert content is not None
        assert "Section text" in content.get_text()

    def test_find_content_no_match(self):
        from bs4 import BeautifulSoup

        html = "<div><p>No matching class</p></div>"
        soup = BeautifulSoup(html, "html.parser")
        source = HTMLSource(_make_config())
        content = source._find_content(soup)
        assert content is None

    def test_find_content_no_selector(self):
        from bs4 import BeautifulSoup

        html = "<html><body><p>Fallback to body</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        config = _make_config(content_selector=None)
        source = HTMLSource(config)
        content = source._find_content(soup)
        assert content is not None

    def test_find_title_with_selector(self):
        from bs4 import BeautifulSoup

        html = "<h1>Section 5747.02 - Tax rates</h1>"
        soup = BeautifulSoup(html, "html.parser")
        source = HTMLSource(_make_config())
        title = source._find_title(soup, "5747.02")
        assert "Tax rates" in title or "5747.02" in title

    def test_find_title_no_selector(self):
        from bs4 import BeautifulSoup

        html = "<div>No heading</div>"
        soup = BeautifulSoup(html, "html.parser")
        config = _make_config(title_selector=None)
        source = HTMLSource(config)
        title = source._find_title(soup, "5747.02")
        assert title == "§ 5747.02"

    def test_find_title_no_match(self):
        from bs4 import BeautifulSoup

        html = "<div>No heading</div>"
        soup = BeautifulSoup(html, "html.parser")
        source = HTMLSource(_make_config())
        title = source._find_title(soup, "5747.02")
        assert "5747.02" in title

    def test_find_history(self):
        from bs4 import BeautifulSoup

        html = '<div class="history">Amended by HB 166 (2019)</div>'
        soup = BeautifulSoup(html, "html.parser")
        source = HTMLSource(_make_config())
        history = source._find_history(soup)
        assert "HB 166" in history

    def test_find_history_no_selector(self):
        from bs4 import BeautifulSoup

        html = "<div>No history</div>"
        soup = BeautifulSoup(html, "html.parser")
        config = _make_config(history_selector=None)
        source = HTMLSource(config)
        history = source._find_history(soup)
        assert history is None

    def test_find_history_no_match(self):
        from bs4 import BeautifulSoup

        html = "<div>No history element</div>"
        soup = BeautifulSoup(html, "html.parser")
        source = HTMLSource(_make_config())
        history = source._find_history(soup)
        assert history is None

    def test_parse_subsections(self):
        from bs4 import BeautifulSoup

        html = """
        <div class="section-content">
        <p>(a) First subsection text here.</p>
        <p>(b) Second subsection text here.</p>
        <p>(1) Numbered paragraph.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        content = soup.find("div")
        source = HTMLSource(_make_config())
        subs = source._parse_subsections(content)
        assert len(subs) >= 2


class TestHTMLSourceFetching:
    @patch.object(HTMLSource, "_get")
    def test_get_section_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.text = """
        <html><body>
        <h1>Section 5747.02 - Tax rates</h1>
        <div class="section-content">
        <p>(a) Tax is imposed at the following rates.</p>
        </div>
        <div class="history">Amended 2019</div>
        </body></html>
        """
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        source = HTMLSource(_make_config())
        result = source.get_section("orc", "5747.02")
        assert result is not None
        assert result.section == "5747.02"

    @patch.object(HTMLSource, "_get")
    def test_get_section_http_error(self, mock_get):
        import httpx

        mock_get.side_effect = httpx.HTTPError("Not found")

        source = HTMLSource(_make_config())
        result = source.get_section("orc", "9999.99")
        assert result is None

    @patch.object(HTMLSource, "_get")
    def test_get_section_no_content(self, mock_get):
        mock_response = MagicMock()
        mock_response.text = "<html><body><div>Empty</div></body></html>"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        source = HTMLSource(_make_config())
        result = source.get_section("orc", "5747.02")
        assert result is None

    @patch.object(HTMLSource, "_get")
    def test_list_sections(self, mock_get):
        mock_response = MagicMock()
        mock_response.text = """
        <html><body>
        <a href="/section-5747.01">5747.01</a>
        <a href="/section-5747.02">5747.02</a>
        <a href="/section-5747.03">5747.03</a>
        </body></html>
        """
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        source = HTMLSource(_make_config())
        sections = list(source.list_sections("title57"))
        assert len(sections) >= 3

    @patch.object(HTMLSource, "_get")
    def test_list_sections_http_error(self, mock_get):
        import httpx

        mock_get.side_effect = httpx.HTTPError("Server error")

        source = HTMLSource(_make_config())
        sections = list(source.list_sections("title57"))
        assert sections == []
