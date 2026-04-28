"""Tests for the crawl_playwright module.

Tests cover PlaywrightStats dataclass and SPA state configurations.
Playwright browser calls are not invoked.
"""

import time

from axiom_corpus.crawl_playwright import SPA_STATES, PlaywrightStats


class TestPlaywrightStats:
    def test_create(self):
        stats = PlaywrightStats(jurisdiction="us-al", name="Alabama")
        assert stats.jurisdiction == "us-al"
        assert stats.name == "Alabama"
        assert stats.sections_discovered == 0
        assert stats.sections_fetched == 0
        assert stats.sections_failed == 0
        assert stats.bytes_fetched == 0
        assert stats.bytes_uploaded == 0

    def test_duration(self):
        stats = PlaywrightStats(jurisdiction="us-al", name="Alabama")
        # Duration should be positive since start_time is set
        assert stats.duration >= 0

    def test_duration_with_end_time(self):
        stats = PlaywrightStats(jurisdiction="us-al", name="Alabama")
        stats.start_time = 100.0
        stats.end_time = 110.0
        assert stats.duration == 10.0

    def test_rate_zero_duration(self):
        stats = PlaywrightStats(jurisdiction="us-al", name="Alabama")
        stats.start_time = time.time()
        stats.end_time = stats.start_time  # zero duration
        assert stats.rate == 0

    def test_rate_positive(self):
        stats = PlaywrightStats(jurisdiction="us-al", name="Alabama")
        stats.start_time = 100.0
        stats.end_time = 110.0
        stats.sections_fetched = 50
        assert stats.rate == 5.0

    def test_errors_count(self):
        stats = PlaywrightStats(jurisdiction="us-al", name="Alabama")
        assert stats.errors_count == 0

        stats.errors.append("Error 1")
        stats.errors.append("Error 2")
        assert stats.errors_count == 2

    def test_update_stats(self):
        stats = PlaywrightStats(jurisdiction="us-al", name="Alabama")
        stats.sections_discovered = 100
        stats.sections_fetched = 95
        stats.sections_failed = 5
        stats.bytes_fetched = 1024 * 1024

        assert stats.sections_discovered == 100
        assert stats.sections_fetched == 95
        assert stats.sections_failed == 5


class TestSPAStates:
    def test_has_alabama(self):
        assert "us-al" in SPA_STATES
        assert SPA_STATES["us-al"]["name"] == "Alabama"
        assert SPA_STATES["us-al"]["type"] == "graphql"

    def test_has_alaska(self):
        assert "us-ak" in SPA_STATES
        assert SPA_STATES["us-ak"]["name"] == "Alaska"
        assert SPA_STATES["us-ak"]["type"] == "js_nav"

    def test_has_texas(self):
        assert "us-tx" in SPA_STATES
        assert SPA_STATES["us-tx"]["name"] == "Texas"
        assert SPA_STATES["us-tx"]["type"] == "angular"

    def test_all_have_required_keys(self):
        for state_id, config in SPA_STATES.items():
            assert "name" in config, f"{state_id} missing name"
            assert "base_url" in config, f"{state_id} missing base_url"
            assert "start_url" in config, f"{state_id} missing start_url"
            assert "type" in config, f"{state_id} missing type"

    def test_urls_are_valid(self):
        for state_id, config in SPA_STATES.items():
            assert config["base_url"].startswith("https://"), f"{state_id} base_url not https"
            assert config["start_url"].startswith("https://"), f"{state_id} start_url not https"
