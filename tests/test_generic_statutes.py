"""Tests for the generic state statute parser."""

from unittest.mock import MagicMock, patch

from axiom_corpus.parsers.generic.statutes import (
    GEORGIA_CONFIG,
    ILLINOIS_CONFIG,
    MICHIGAN_CONFIG,
    NORTH_CAROLINA_CONFIG,
    OHIO_CONFIG,
    PENNSYLVANIA_CONFIG,
    STATE_PARSERS,
    GenericStateParser,
    StateConfig,
    StateSection,
    StateSubsection,
    get_georgia_parser,
    get_illinois_parser,
    get_michigan_parser,
    get_north_carolina_parser,
    get_ohio_parser,
    get_parser_for_state,
    get_pennsylvania_parser,
    list_supported_states,
)


class TestStateSection:
    def test_citation(self):
        sec = StateSection(
            state="OH",
            code="57",
            code_name="Taxation",
            section_num="5747.02",
            title="Income tax rate",
            text="Some text",
            url="https://example.com",
        )
        assert sec.citation == "OH 57 \u00a7 5747.02"

    def test_defaults(self):
        sec = StateSection(
            state="PA",
            code="72",
            code_name="Tax",
            section_num="1",
            title="Title",
            text="Text",
            url="http://example.com",
        )
        assert sec.subsections == []
        assert sec.chapter is None
        assert sec.history is None


class TestStateSubsection:
    def test_basic(self):
        sub = StateSubsection(identifier="a", text="First.")
        assert sub.identifier == "a"
        assert sub.text == "First."
        assert sub.children == []

    def test_nested(self):
        child = StateSubsection(identifier="1", text="Child.")
        parent = StateSubsection(identifier="a", text="Parent.", children=[child])
        assert len(parent.children) == 1


class TestStateConfigs:
    def test_ohio_config(self):
        assert OHIO_CONFIG.state_code == "OH"
        assert "codes.ohio.gov" in OHIO_CONFIG.base_url
        assert "57" in OHIO_CONFIG.codes

    def test_pennsylvania_config(self):
        assert PENNSYLVANIA_CONFIG.state_code == "PA"
        assert "palegis.us" in PENNSYLVANIA_CONFIG.base_url

    def test_illinois_config(self):
        assert ILLINOIS_CONFIG.state_code == "IL"
        assert "ilga.gov" in ILLINOIS_CONFIG.base_url

    def test_north_carolina_config(self):
        assert NORTH_CAROLINA_CONFIG.state_code == "NC"
        assert "ncleg.gov" in NORTH_CAROLINA_CONFIG.base_url

    def test_michigan_config(self):
        assert MICHIGAN_CONFIG.state_code == "MI"
        assert "legislature.mi.gov" in MICHIGAN_CONFIG.base_url

    def test_georgia_config(self):
        assert GEORGIA_CONFIG.state_code == "GA"
        assert "legis.ga.gov" in GEORGIA_CONFIG.base_url


class TestStateParsersRegistry:
    def test_all_states_registered(self):
        assert "OH" in STATE_PARSERS
        assert "PA" in STATE_PARSERS
        assert "IL" in STATE_PARSERS
        assert "NC" in STATE_PARSERS
        assert "MI" in STATE_PARSERS
        assert "GA" in STATE_PARSERS
        assert len(STATE_PARSERS) == 6


class TestGenericStateParser:
    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_init(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)
        assert parser.config.state_code == "OH"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_build_section_url_relative(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)
        url = parser._build_section_url("5747.02")
        assert url.startswith("https://codes.ohio.gov")
        assert "5747.02" in url

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_build_section_url_absolute(self, mock_client_cls):
        config = StateConfig(
            state_code="X",
            state_name="Test",
            base_url="https://test.com",
            section_url_pattern="https://other.com/section-{section}",
            toc_url_pattern="/toc",
            content_selector="main",
        )
        parser = GenericStateParser(config)
        url = parser._build_section_url("123")
        assert url == "https://other.com/section-123"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_build_toc_url(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)
        url = parser._build_toc_url(title="57")
        assert "57" in url
        assert url.startswith("https://codes.ohio.gov")

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_infer_code_from_section_match(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)
        code_id, code_name = parser._infer_code_from_section("5747.02")
        assert code_id == "57"
        assert code_name == "Taxation"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_infer_code_from_section_no_match(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)
        code_id, code_name = parser._infer_code_from_section("9999.99")
        # Falls back to first code
        assert code_id != ""

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_infer_code_from_section_empty_codes(self, mock_client_cls):
        config = StateConfig(
            state_code="X",
            state_name="Test",
            base_url="https://test.com",
            section_url_pattern="/section-{section}",
            toc_url_pattern="/toc",
            content_selector="main",
            codes={},
        )
        parser = GenericStateParser(config)
        code_id, code_name = parser._infer_code_from_section("123")
        assert code_id == ""
        assert code_name == "Unknown"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_parse_subsections(self, mock_client_cls):
        from bs4 import BeautifulSoup

        parser = GenericStateParser(OHIO_CONFIG)
        html = "<div>(a) First item (b) Second item (1) Numbered item</div>"
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div")
        subs = parser._parse_subsections(div)
        assert len(subs) >= 2

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_section_with_custom_parser(self, mock_client_cls):
        custom_result = StateSection(
            state="X",
            code="1",
            code_name="Test",
            section_num="1",
            title="Custom",
            text="Custom text",
            url="http://test.com",
        )

        def custom_parser(soup, url):
            return custom_result

        config = StateConfig(
            state_code="X",
            state_name="Test",
            base_url="https://test.com",
            section_url_pattern="/section-{section}",
            toc_url_pattern="/toc",
            content_selector="main",
            section_parser=custom_parser,
        )
        parser = GenericStateParser(config)
        mock_client_cls.return_value.get.return_value.text = "<html><body>Test</body></html>"
        mock_client_cls.return_value.get.return_value.raise_for_status = lambda: None

        # Mock _get to return HTML
        parser._get = MagicMock(return_value="<html><body>Test</body></html>")
        result = parser.get_section("1")
        assert result == custom_result

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_section_custom_parser_http_error(self, mock_client_cls):
        import httpx

        def custom_parser(soup, url):
            return None

        config = StateConfig(
            state_code="X",
            state_name="Test",
            base_url="https://test.com",
            section_url_pattern="/section-{section}",
            toc_url_pattern="/toc",
            content_selector="main",
            section_parser=custom_parser,
        )
        parser = GenericStateParser(config)
        parser._get = MagicMock(
            side_effect=httpx.HTTPError("Not found")
        )
        result = parser.get_section("1")
        assert result is None

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_section_default_parsing(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)
        html = """<html><body><main>
            <title>Section 5747.02 - Tax Rate</title>
            <p>(a) The rate shall be 5%.</p>
        </main></body></html>"""
        parser._get = MagicMock(return_value=html)
        result = parser.get_section("5747.02")
        assert result is not None
        assert result.section_num == "5747.02"
        assert result.state == "OH"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_section_no_content(self, mock_client_cls):
        config = StateConfig(
            state_code="X",
            state_name="Test",
            base_url="https://test.com",
            section_url_pattern="/section-{section}",
            toc_url_pattern="/toc",
            content_selector="div.nonexistent",
        )
        parser = GenericStateParser(config)
        parser._get = MagicMock(return_value="<html><body><p>No match</p></body></html>")
        result = parser.get_section("1")
        assert result is None

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_section_http_error(self, mock_client_cls):
        import httpx

        parser = GenericStateParser(OHIO_CONFIG)
        parser._get = MagicMock(side_effect=httpx.HTTPError("Connection error"))
        result = parser.get_section("5747.02")
        assert result is None

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_section_with_history_selector(self, mock_client_cls):
        config = StateConfig(
            state_code="X",
            state_name="Test",
            base_url="https://test.com",
            section_url_pattern="/section-{section}",
            toc_url_pattern="/toc",
            content_selector="main",
            history_selector="div.history",
        )
        parser = GenericStateParser(config)
        html = """<html><body>
            <main><p>Section text</p></main>
            <div class="history">Acts 2020</div>
        </body></html>"""
        parser._get = MagicMock(return_value=html)
        result = parser.get_section("1")
        assert result is not None
        assert result.history == "Acts 2020"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_list_sections_from_toc(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)
        html = """<html><body>
            <a href="/section-5747.02">Sec 5747.02</a>
            <a href="/section-5747.03">Sec 5747.03</a>
            <a href="/about">About</a>
        </body></html>"""
        parser._get = MagicMock(return_value=html)
        sections = list(parser.list_sections_from_toc(title="57"))
        assert len(sections) >= 2
        assert "5747.02" in sections

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_list_sections_from_toc_http_error(self, mock_client_cls):
        import httpx

        parser = GenericStateParser(OHIO_CONFIG)
        parser._get = MagicMock(side_effect=httpx.HTTPError("Error"))
        sections = list(parser.list_sections_from_toc(title="57"))
        assert sections == []

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_download_code(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)

        toc_html = """<html><body>
            <a href="/section-5747.01">Sec 5747.01</a>
            <a href="/section-5747.02">Sec 5747.02</a>
        </body></html>"""
        section_html = """<html><body><main>
            <title>Section Test</title>
            <p>Text</p>
        </main></body></html>"""

        call_count = 0

        def mock_get(url):
            nonlocal call_count
            call_count += 1
            if "title" in url or "toc" in url:
                return toc_html
            return section_html

        parser._get = MagicMock(side_effect=mock_get)
        sections = list(parser.download_code("57", max_sections=1))
        assert len(sections) == 1

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_del_closes_client(self, mock_client_cls):
        parser = GenericStateParser(OHIO_CONFIG)
        del parser
        # Should not raise


class TestConvenienceFunctions:
    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_ohio_parser(self, mock_client_cls):
        p = get_ohio_parser()
        assert p.config.state_code == "OH"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_pennsylvania_parser(self, mock_client_cls):
        p = get_pennsylvania_parser()
        assert p.config.state_code == "PA"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_illinois_parser(self, mock_client_cls):
        p = get_illinois_parser()
        assert p.config.state_code == "IL"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_north_carolina_parser(self, mock_client_cls):
        p = get_north_carolina_parser()
        assert p.config.state_code == "NC"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_michigan_parser(self, mock_client_cls):
        p = get_michigan_parser()
        assert p.config.state_code == "MI"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_get_georgia_parser(self, mock_client_cls):
        p = get_georgia_parser()
        assert p.config.state_code == "GA"


class TestGetParserForState:
    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_known_state(self, mock_client_cls):
        p = get_parser_for_state("OH")
        assert p is not None
        assert p.config.state_code == "OH"

    @patch("axiom_corpus.parsers.generic.statutes.httpx.Client")
    def test_case_insensitive(self, mock_client_cls):
        p = get_parser_for_state("oh")
        assert p is not None

    def test_unknown_state(self):
        p = get_parser_for_state("ZZ")
        assert p is None


class TestListSupportedStates:
    def test_returns_list(self):
        states = list_supported_states()
        assert isinstance(states, list)
        assert len(states) == 6
        codes = [s["code"] for s in states]
        assert "OH" in codes
        assert "PA" in codes
