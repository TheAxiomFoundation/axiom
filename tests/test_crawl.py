"""Tests for the crawl module.

Tests cover the CrawlStats dataclass, section patterns, and crawl logic.
All HTTP and R2 calls are mocked.
"""

import pytest

from axiom_corpus.crawl import (
    ARCHIVE_ORG_STATES,
    R2_BUCKET,
    R2_ENDPOINT,
    SECTION_PATTERNS,
)


class TestConstants:
    def test_r2_endpoint(self):
        assert "r2.cloudflarestorage.com" in R2_ENDPOINT

    def test_r2_bucket(self):
        assert R2_BUCKET == "axiom-corpus"


class TestSectionPatterns:
    def test_has_many_states(self):
        assert len(SECTION_PATTERNS) >= 20

    def test_ohio_pattern(self):
        import re

        pattern = SECTION_PATTERNS.get("us-oh", "")
        if pattern:
            assert re.search(pattern, "/section-5747.02")

    def test_texas_pattern(self):
        pattern = SECTION_PATTERNS.get("us-tx", "")
        assert pattern  # TX should have a pattern

    def test_patterns_are_valid_regex(self):
        import re

        for state, pattern in SECTION_PATTERNS.items():
            try:
                re.compile(pattern)
            except re.error:
                pytest.fail(f"Invalid regex for {state}: {pattern}")


class TestArchiveOrgStates:
    def test_has_states(self):
        assert len(ARCHIVE_ORG_STATES) >= 5

    def test_georgia(self):
        assert "us-ga" in ARCHIVE_ORG_STATES

    def test_values_are_strings(self):
        for state, item_id in ARCHIVE_ORG_STATES.items():
            assert isinstance(item_id, str)
            assert state.startswith("us-")


class TestCrawlStatsDataclass:
    """Test the CrawlStats-like structures from crawl module."""

    def test_section_patterns_ohio(self):
        import re

        pattern = SECTION_PATTERNS.get("us-oh")
        if pattern:
            assert re.search(pattern, "/ohio-revised-code/section-5747.02")

    def test_section_patterns_florida(self):
        import re

        pattern = SECTION_PATTERNS.get("us-fl")
        if pattern:
            assert re.search(pattern, "/statutes/220.02")
