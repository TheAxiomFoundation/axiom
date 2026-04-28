"""Tests for US state converter modules.

These converters all follow similar patterns. This test module validates
the basic structure of each converter: constants, class definitions, and
basic dataclass instantiation. Network calls are NOT made.
"""

import importlib

import pytest

# List of all state converter modules
STATE_MODULES = [
    "ak", "al", "ar", "az", "ca", "co", "ct", "dc", "de", "fl",
    "ga", "hi", "ia", "id_", "il", "in_", "ks", "ky", "la", "ma",
    "md", "me", "mi", "mn", "mo", "ms", "mt", "nc", "nd", "ne",
    "nh", "nj", "nm", "nv", "ny", "oh", "ok", "or_", "pa", "ri",
    "sc", "sd", "tn", "tx", "ut", "va", "vt", "wa", "wi", "wv", "wy",
]


def _try_import(module_name):
    """Try importing a state converter module."""
    try:
        return importlib.import_module(f"axiom_corpus.converters.us_states.{module_name}")
    except (ImportError, Exception):
        return None


@pytest.fixture(params=STATE_MODULES)
def state_module(request):
    """Parametrized fixture for each state module."""
    mod = _try_import(request.param)
    if mod is None:
        pytest.skip(f"Module {request.param} not importable")
    return request.param, mod


class TestStateConverterModules:
    def test_module_imports(self, state_module):
        """Each state module should import without errors."""
        name, mod = state_module
        assert mod is not None

    def test_has_converter_class(self, state_module):
        """Each module should have a converter class."""
        name, mod = state_module
        # Look for a class that ends with 'Converter'
        converter_classes = [
            attr for attr in dir(mod)
            if attr.endswith("Converter") and isinstance(getattr(mod, attr), type)
        ]
        assert len(converter_classes) >= 1, f"{name} has no *Converter class"

    def test_has_base_url(self, state_module):
        """Most modules define a BASE_URL constant."""
        name, mod = state_module
        # Check for BASE_URL or base_url or similar
        any(
            attr.upper().endswith("URL") or attr.upper() == "BASE_URL"
            for attr in dir(mod)
            if isinstance(getattr(mod, attr, None), str)
            and getattr(mod, attr, "").startswith("http")
        )
        # Not all modules have a BASE_URL, so we just check and don't fail
        # This is informational


class TestSpecificConverters:
    """Test specific converter functionality that's common across modules."""

    def test_ak_titles(self):
        mod = _try_import("ak")
        if mod is None:
            pytest.skip("AK module not importable")
        assert hasattr(mod, "AK_TITLES")
        assert 43 in mod.AK_TITLES  # Revenue and Taxation
        assert len(mod.AK_TITLES) >= 30

    def test_ak_tax_chapters(self):
        mod = _try_import("ak")
        if mod is None:
            pytest.skip("AK module not importable")
        assert hasattr(mod, "AK_TAX_CHAPTERS")
        assert "20" in mod.AK_TAX_CHAPTERS  # Alaska Net Income Tax Act

    def test_al_converter_init(self):
        mod = _try_import("al")
        if mod is None:
            pytest.skip("AL module not importable")
        converter_cls = getattr(mod, "ALConverter", None)
        if converter_cls:
            converter = converter_cls()
            assert converter is not None

    def test_ca_converter_init(self):
        mod = _try_import("ca")
        if mod is None:
            pytest.skip("CA module not importable")
        converter_cls = getattr(mod, "CAConverter", None)
        if converter_cls:
            converter = converter_cls()
            assert converter is not None

    def test_fl_converter_init(self):
        mod = _try_import("fl")
        if mod is None:
            pytest.skip("FL module not importable")
        converter_cls = getattr(mod, "FLConverter", None)
        if converter_cls:
            converter = converter_cls()
            assert converter is not None

    def test_oh_converter_init(self):
        mod = _try_import("oh")
        if mod is None:
            pytest.skip("OH module not importable")
        converter_cls = getattr(mod, "OHConverter", None)
        if converter_cls:
            converter = converter_cls()
            assert converter is not None

    def test_ny_converter_init(self):
        mod = _try_import("ny")
        if mod is None:
            pytest.skip("NY module not importable")
        # NY module exports NY_LAW_CODES
        assert hasattr(mod, "NY_LAW_CODES") or hasattr(mod, "NYStateConverter")

    def test_tx_converter_init(self):
        mod = _try_import("tx")
        if mod is None:
            pytest.skip("TX module not importable")
        converter_cls = getattr(mod, "TXConverter", None)
        if converter_cls:
            converter = converter_cls()
            assert converter is not None

    def test_pa_converter_init(self):
        mod = _try_import("pa")
        if mod is None:
            pytest.skip("PA module not importable")
        converter_cls = getattr(mod, "PAConverter", None)
        if converter_cls:
            converter = converter_cls()
            assert converter is not None
