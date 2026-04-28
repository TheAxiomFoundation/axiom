"""Tests for the sources registry module."""



from axiom.sources.base import SourceConfig
from axiom.sources.registry import (
    _get_builtin_configs,
    _load_yaml_configs,
    get_all_configs,
    get_config_for_jurisdiction,
    get_source_for_jurisdiction,
    list_supported_jurisdictions,
    register_source,
)


class TestLoadYamlConfigs:
    def test_nonexistent_directory(self, tmp_path):
        configs = _load_yaml_configs(tmp_path / "nonexistent")
        assert configs == {}

    def test_empty_directory(self, tmp_path):
        configs = _load_yaml_configs(tmp_path)
        assert configs == {}

    def test_load_valid_yaml(self, tmp_path):
        yaml_content = """
jurisdiction: us-test
name: Test State
source_type: html
base_url: https://example.com
codes:
  TAX: Tax Law
rate_limit: 1.0
"""
        yaml_file = tmp_path / "us-test.yaml"
        yaml_file.write_text(yaml_content)

        configs = _load_yaml_configs(tmp_path)
        assert "us-test" in configs
        assert configs["us-test"].name == "Test State"
        assert configs["us-test"].rate_limit == 1.0
        assert configs["us-test"].codes == {"TAX": "Tax Law"}

    def test_load_empty_yaml(self, tmp_path):
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")

        configs = _load_yaml_configs(tmp_path)
        assert len(configs) == 0

    def test_load_invalid_yaml(self, tmp_path):
        yaml_file = tmp_path / "invalid.yaml"
        yaml_file.write_text("not: valid: yaml: {broken")

        _load_yaml_configs(tmp_path)
        # Should not raise, just skip

    def test_load_yaml_minimal(self, tmp_path):
        yaml_content = """
name: Minimal
base_url: https://example.com
"""
        yaml_file = tmp_path / "minimal.yaml"
        yaml_file.write_text(yaml_content)

        configs = _load_yaml_configs(tmp_path)
        assert "minimal" in configs
        assert configs["minimal"].source_type == "html"  # default

    def test_load_yaml_with_all_fields(self, tmp_path):
        yaml_content = """
jurisdiction: us-ny
name: New York
source_type: api
base_url: https://legislation.nysenate.gov
api_key: test-key
section_url_pattern: /section/{section}
toc_url_pattern: /toc/{code}
content_selector: .content
title_selector: h1
history_selector: .history
codes:
  TAX: Tax Law
  LAB: Labor Law
rate_limit: 1.5
max_retries: 5
custom_parser: axiom.parsers.us_ny
"""
        yaml_file = tmp_path / "us-ny.yaml"
        yaml_file.write_text(yaml_content)

        configs = _load_yaml_configs(tmp_path)
        config = configs["us-ny"]
        assert config.api_key == "test-key"
        assert config.max_retries == 5
        assert config.custom_parser == "axiom.parsers.us_ny"

    def test_load_multiple_yaml_files(self, tmp_path):
        for state in ["us-ca", "us-ny", "us-oh"]:
            yaml_file = tmp_path / f"{state}.yaml"
            yaml_file.write_text(f"""
jurisdiction: {state}
name: Test {state}
base_url: https://example.com/{state}
""")

        configs = _load_yaml_configs(tmp_path)
        assert len(configs) == 3


class TestGetBuiltinConfigs:
    def test_builtin_has_federal(self):
        configs = _get_builtin_configs()
        assert "us" in configs
        assert configs["us"].jurisdiction == "us"

    def test_builtin_has_ohio(self):
        configs = _get_builtin_configs()
        assert "us-oh" in configs


class TestGetAllConfigs:
    def test_returns_configs(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            configs = get_all_configs()
            assert isinstance(configs, dict)
            assert "us" in configs
        finally:
            registry_module._SOURCE_CONFIGS = old

    def test_caches_configs(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            configs1 = get_all_configs()
            configs2 = get_all_configs()
            assert configs1 is configs2
        finally:
            registry_module._SOURCE_CONFIGS = old


class TestGetConfigForJurisdiction:
    def test_known_jurisdiction(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            cfg = get_config_for_jurisdiction("us")
            assert cfg is not None
            assert cfg.jurisdiction == "us"
        finally:
            registry_module._SOURCE_CONFIGS = old

    def test_unknown_jurisdiction(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            cfg = get_config_for_jurisdiction("zz-unknown")
            assert cfg is None
        finally:
            registry_module._SOURCE_CONFIGS = old


class TestGetSourceForJurisdiction:
    def test_uslm_source(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            source = get_source_for_jurisdiction("us")
            assert source is not None
        finally:
            registry_module._SOURCE_CONFIGS = old

    def test_unknown_jurisdiction(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            source = get_source_for_jurisdiction("zz-unknown")
            assert source is None
        finally:
            registry_module._SOURCE_CONFIGS = old

    def test_html_source(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            source = get_source_for_jurisdiction("us-oh")
            assert source is not None
        finally:
            registry_module._SOURCE_CONFIGS = old

    def test_api_source(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            _ = get_all_configs()
            api_config = SourceConfig(
                jurisdiction="us-api-test",
                name="API Test",
                source_type="api",
                base_url="https://api.test.com",
            )
            registry_module._SOURCE_CONFIGS["us-api-test"] = api_config
            source = get_source_for_jurisdiction("us-api-test")
            assert source is not None
        finally:
            registry_module._SOURCE_CONFIGS = old

    def test_ny_api_source(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            _ = get_all_configs()
            ny_config = SourceConfig(
                jurisdiction="us-ny",
                name="New York",
                source_type="api",
                base_url="https://legislation.nysenate.gov",
                api_key="test-key",
            )
            registry_module._SOURCE_CONFIGS["us-ny"] = ny_config
            source = get_source_for_jurisdiction("us-ny")
            assert source is not None
        finally:
            registry_module._SOURCE_CONFIGS = old


class TestListSupportedJurisdictions:
    def test_returns_list(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = {}
        try:
            jurisdictions = list_supported_jurisdictions()
            assert isinstance(jurisdictions, list)
            assert len(jurisdictions) >= 1
            first = jurisdictions[0]
            assert "jurisdiction" in first
            assert "name" in first
            assert "source_type" in first
            assert "codes" in first
        finally:
            registry_module._SOURCE_CONFIGS = old


class TestRegisterSource:
    def test_register_new_source(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = dict(old)
        try:
            config = SourceConfig(
                jurisdiction="us-test-register",
                name="Test Register",
                source_type="html",
                base_url="https://test.example.com",
            )
            register_source("us-test-register", config)
            assert "us-test-register" in registry_module._SOURCE_CONFIGS
        finally:
            registry_module._SOURCE_CONFIGS = old

    def test_register_case_insensitive(self):
        import axiom.sources.registry as registry_module

        old = registry_module._SOURCE_CONFIGS
        registry_module._SOURCE_CONFIGS = dict(old)
        try:
            config = SourceConfig(
                jurisdiction="us-upper",
                name="Test",
                source_type="html",
                base_url="https://test.example.com",
            )
            register_source("US-UPPER", config)
            assert "us-upper" in registry_module._SOURCE_CONFIGS
        finally:
            registry_module._SOURCE_CONFIGS = old
