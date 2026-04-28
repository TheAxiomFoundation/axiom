"""Tests for the Canadian legislation fetcher module."""

import pytest

try:
    from axiom.fetchers.legislation_canada import CanadaLegislationFetcher
    _IMPORT_OK = True
except ImportError:
    _IMPORT_OK = False

pytestmark = pytest.mark.skipif(not _IMPORT_OK, reason="Canada fetcher not importable")


class TestCanadaLegislationFetcher:
    def test_init(self):
        fetcher = CanadaLegislationFetcher()
        assert fetcher is not None
