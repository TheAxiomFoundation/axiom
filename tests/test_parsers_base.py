"""Tests for parsers base module.

Tests cover StateCode enum, StateInfo, STATE_REGISTRY, BaseStateParser,
and helper functions.
"""

import pytest

from axiom_corpus.parsers.base import (
    STATE_REGISTRY,
    BaseStateParser,
    StateCode,
    StateInfo,
    get_state_info,
    get_supported_states,
)


class TestStateCode:
    def test_all_50_states_plus_dc_pr(self):
        # 50 states + DC + PR = 52
        assert len(StateCode) == 52

    def test_common_states(self):
        assert StateCode.NY.value == "NY"
        assert StateCode.CA.value == "CA"
        assert StateCode.TX.value == "TX"
        assert StateCode.FL.value == "FL"
        assert StateCode.DC.value == "DC"
        assert StateCode.PR.value == "PR"

    def test_enum_from_string(self):
        assert StateCode("NY") == StateCode.NY
        assert StateCode("CA") == StateCode.CA

    def test_invalid_state(self):
        with pytest.raises(ValueError):
            StateCode("ZZ")


class TestStateInfo:
    def test_create(self):
        info = StateInfo(
            code=StateCode.NY,
            name="New York",
            statute_source="https://legislation.nysenate.gov",
            api_available=True,
            api_key_required=True,
            api_key_env_var="NY_LEGISLATION_API_KEY",
            notes="Test notes",
        )
        assert info.code == StateCode.NY
        assert info.name == "New York"
        assert info.api_available is True
        assert info.api_key_required is True
        assert info.notes == "Test notes"

    def test_create_without_optional(self):
        info = StateInfo(
            code=StateCode.CA,
            name="California",
            statute_source="https://leginfo.legislature.ca.gov",
            api_available=False,
            api_key_required=False,
            api_key_env_var=None,
        )
        assert info.notes is None


class TestStateRegistry:
    def test_registry_has_entries(self):
        assert len(STATE_REGISTRY) > 0

    def test_ny_in_registry(self):
        assert StateCode.NY in STATE_REGISTRY
        assert STATE_REGISTRY[StateCode.NY].name == "New York"

    def test_ca_in_registry(self):
        assert StateCode.CA in STATE_REGISTRY
        assert STATE_REGISTRY[StateCode.CA].api_available is False

    def test_tx_in_registry(self):
        assert StateCode.TX in STATE_REGISTRY

    def test_fl_in_registry(self):
        assert StateCode.FL in STATE_REGISTRY


class TestGetSupportedStates:
    def test_returns_list(self):
        states = get_supported_states()
        assert isinstance(states, list)
        assert len(states) > 0

    def test_all_are_state_info(self):
        states = get_supported_states()
        assert all(isinstance(s, StateInfo) for s in states)


class TestGetStateInfo:
    def test_valid_state(self):
        info = get_state_info("NY")
        assert info is not None
        assert info.code == StateCode.NY

    def test_lowercase(self):
        info = get_state_info("ny")
        assert info is not None
        assert info.code == StateCode.NY

    def test_invalid_state(self):
        info = get_state_info("ZZ")
        assert info is None

    def test_empty_string(self):
        info = get_state_info("")
        assert info is None

    def test_state_not_in_registry(self):
        # A valid state code but not in registry
        info = get_state_info("WY")
        # May or may not be in registry
        # Just test it returns None or StateInfo
        assert info is None or isinstance(info, StateInfo)


class TestBaseStateParser:
    def test_is_abstract(self):
        with pytest.raises(TypeError):
            BaseStateParser()

    def test_download_all(self):
        """Test that download_all iterates over list_codes and download_code."""

        class MockParser(BaseStateParser):
            state_code = StateCode.NY

            def list_codes(self):
                return ["TAX", "EDN"]

            def download_code(self, code):
                yield from []

        parser = MockParser()
        result = list(parser.download_all())
        assert result == []
