"""Tests for the sources base module.

Tests cover SourceConfig, StatuteSource, and helper functions.
"""

from unittest.mock import MagicMock, patch

import pytest

from axiom.models_statute import Statute
from axiom.sources.base import SourceConfig, StatuteSource, load_source


class TestSourceConfig:
    def test_create_minimal(self):
        config = SourceConfig(
            jurisdiction="us-oh",
            name="Ohio",
            source_type="html",
            base_url="https://codes.ohio.gov",
        )
        assert config.jurisdiction == "us-oh"
        assert config.rate_limit == 0.5
        assert config.max_retries == 3
        assert config.api_key is None

    def test_create_full(self):
        config = SourceConfig(
            jurisdiction="us-ny",
            name="New York",
            source_type="api",
            base_url="https://legislation.nysenate.gov",
            api_key="test-key",
            section_url_pattern="/section/{section}",
            toc_url_pattern="/toc/{code}",
            content_selector=".content",
            title_selector="h1",
            history_selector=".history",
            codes={"TAX": "Tax Law", "EDN": "Education"},
            rate_limit=1.0,
            max_retries=5,
            custom_parser="axiom.parsers.us_ny.statutes",
        )
        assert config.api_key == "test-key"
        assert config.rate_limit == 1.0
        assert len(config.codes) == 2


class TestStatuteSource:
    def test_cannot_instantiate_directly(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
        )
        with pytest.raises(TypeError):
            StatuteSource(config)

    def test_concrete_source_init(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return None

            def list_sections(self, code, **kwargs):
                yield from []

        source = ConcreteSource(config)
        assert source.config.jurisdiction == "test"
        assert source._client is None

    def test_client_lazy_init(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return None

            def list_sections(self, code, **kwargs):
                yield from []

        source = ConcreteSource(config)
        client = source.client
        assert client is not None
        assert source._client is not None
        # Second call returns same client
        assert source.client is client

    def test_close(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return None

            def list_sections(self, code, **kwargs):
                yield from []

        source = ConcreteSource(config)
        _ = source.client  # Initialize
        source.close()
        assert source._client is None

    def test_close_without_client(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return None

            def list_sections(self, code, **kwargs):
                yield from []

        source = ConcreteSource(config)
        source.close()  # Should not raise

    def test_get_code_name(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
            codes={"TAX": "Tax Law"},
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return None

            def list_sections(self, code, **kwargs):
                yield from []

        source = ConcreteSource(config)
        assert source.get_code_name("TAX") == "Tax Law"
        assert source.get_code_name("UNK") == "UNK"

    def test_create_statute(self):
        config = SourceConfig(
            jurisdiction="us-oh",
            name="Ohio",
            source_type="html",
            base_url="https://codes.ohio.gov",
            codes={"ORC": "Ohio Revised Code"},
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return None

            def list_sections(self, code, **kwargs):
                yield from []

        source = ConcreteSource(config)
        statute = source._create_statute(
            code="ORC",
            section="5747.02",
            title="Tax rates",
            text="Tax imposed...",
            source_url="https://codes.ohio.gov/orc/5747.02",
        )
        assert isinstance(statute, Statute)
        assert statute.jurisdiction == "us-oh"
        assert statute.code == "ORC"
        assert statute.code_name == "Ohio Revised Code"
        assert statute.section == "5747.02"

    def test_download_code(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
            codes={"TAX": "Tax"},
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return self._create_statute(
                    code=code,
                    section=section,
                    title=f"Section {section}",
                    text="text",
                    source_url="https://example.com",
                )

            def list_sections(self, code, **kwargs):
                yield "1"
                yield "2"
                yield "3"

        source = ConcreteSource(config)
        statutes = list(source.download_code("TAX"))
        assert len(statutes) == 3

    def test_download_code_with_max(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
            codes={"TAX": "Tax"},
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return self._create_statute(
                    code=code,
                    section=section,
                    title=f"Section {section}",
                    text="text",
                    source_url="https://example.com",
                )

            def list_sections(self, code, **kwargs):
                yield from ["1", "2", "3", "4", "5"]

        source = ConcreteSource(config)
        statutes = list(source.download_code("TAX", max_sections=2))
        assert len(statutes) == 2

    def test_download_code_with_callback(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
            codes={"TAX": "Tax"},
        )
        calls = []

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return self._create_statute(
                    code=code,
                    section=section,
                    title=f"Section {section}",
                    text="text",
                    source_url="https://example.com",
                )

            def list_sections(self, code, **kwargs):
                yield "1"
                yield "2"

        source = ConcreteSource(config)
        list(source.download_code("TAX", progress_callback=lambda c, s: calls.append((c, s))))
        assert len(calls) == 2
        assert calls[0] == (1, "1")
        assert calls[1] == (2, "2")

    def test_download_code_skips_none(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
            codes={"TAX": "Tax"},
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                if section == "2":
                    return None
                return self._create_statute(
                    code=code,
                    section=section,
                    title=f"Section {section}",
                    text="text",
                    source_url="https://example.com",
                )

            def list_sections(self, code, **kwargs):
                yield "1"
                yield "2"
                yield "3"

        source = ConcreteSource(config)
        statutes = list(source.download_code("TAX"))
        assert len(statutes) == 2

    def test_download_jurisdiction(self):
        config = SourceConfig(
            jurisdiction="test",
            name="Test",
            source_type="html",
            base_url="https://example.com",
            codes={"TAX": "Tax", "EDN": "Education"},
        )

        class ConcreteSource(StatuteSource):
            def get_section(self, code, section, **kwargs):
                return self._create_statute(
                    code=code,
                    section=section,
                    title=f"Section {section}",
                    text="text",
                    source_url="https://example.com",
                )

            def list_sections(self, code, **kwargs):
                yield "1"

        source = ConcreteSource(config)
        statutes = list(source.download_jurisdiction())
        assert len(statutes) == 2


class TestLoadSource:
    @patch("axiom.sources.registry.get_source_for_jurisdiction")
    def test_load_existing(self, mock_get):
        mock_source = MagicMock()
        mock_get.return_value = mock_source
        result = load_source("us-oh")
        assert result is mock_source

    @patch("axiom.sources.registry.get_source_for_jurisdiction")
    def test_load_nonexistent_raises(self, mock_get):
        mock_get.return_value = None
        with pytest.raises(ValueError, match="No source configured"):
            load_source("zz")
