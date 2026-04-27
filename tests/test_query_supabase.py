"""Tests for the query supabase module.

Tests cover Rule/Section dataclasses and SupabaseQuery class.
HTTP calls are mocked.
"""

import os
from unittest.mock import patch

from atlas.query.supabase import Rule, Section, SupabaseQuery


def _make_rule(**kwargs):
    defaults = {
        "id": "us/statute/26/32",
        "jurisdiction": "us",
        "doc_type": "statute",
        "parent_id": None,
        "level": 0,
        "ordinal": 1,
        "heading": "Earned income tax credit",
        "body": "A tax credit is allowed...",
        "effective_date": "2024-01-01",
        "repeal_date": None,
        "source_url": "https://uscode.house.gov",
        "source_path": "26/32",
        "rulespec_path": "rules-us/statute/26/32.yaml",
        "has_rulespec": True,
        "citation_path": "26/32",
    }
    defaults.update(kwargs)
    return Rule(**defaults)


class TestRule:
    def test_create(self):
        rule = _make_rule()
        assert rule.id == "us/statute/26/32"
        assert rule.jurisdiction == "us"
        assert rule.has_rulespec is True

    def test_optional_fields(self):
        rule = _make_rule(
            heading=None,
            body=None,
            effective_date=None,
            repeal_date=None,
            source_url=None,
            source_path=None,
            rulespec_path=None,
            citation_path=None,
            ordinal=None,
        )
        assert rule.heading is None
        assert rule.body is None


class TestSection:
    def test_create(self):
        rule = _make_rule()
        section = Section(rule=rule, children=[])
        assert section.rule is rule
        assert section.children == []

    def test_full_text_no_children(self):
        rule = _make_rule()
        section = Section(rule=rule, children=[])
        text = section.full_text
        assert "Earned income tax credit" in text
        assert "A tax credit" in text

    def test_full_text_with_children(self):
        parent = _make_rule(heading="Main heading", body="Main body")
        child1 = _make_rule(heading="Sub A", body="Sub A body")
        child2 = _make_rule(heading="Sub B", body="Sub B body")

        section = Section(rule=parent, children=[child1, child2])
        text = section.full_text
        assert "Main heading" in text
        assert "Sub A" in text
        assert "Sub B" in text

    def test_full_text_no_heading(self):
        rule = _make_rule(heading=None)
        section = Section(rule=rule, children=[])
        text = section.full_text
        assert "A tax credit" in text

    def test_full_text_no_body(self):
        rule = _make_rule(body=None)
        section = Section(rule=rule, children=[])
        text = section.full_text
        assert "Earned income tax credit" in text

    def test_citation_with_source_path(self):
        rule = _make_rule(source_path="26/32")
        section = Section(rule=rule, children=[])
        assert section.citation == "26/32"

    def test_citation_without_source_path(self):
        rule = _make_rule(source_path=None)
        section = Section(rule=rule, children=[])
        assert section.citation == "us/statute/26/32"


class TestSupabaseQuery:
    def test_init_defaults(self):
        query = SupabaseQuery()
        assert query.url is not None
        assert query.anon_key is not None
        assert "rest/v1" in query.rest_url

    def test_init_with_explicit_url(self):
        query = SupabaseQuery(
            url="https://test.supabase.co",
            anon_key="test-key",
        )
        assert query.url == "https://test.supabase.co"
        assert query.anon_key == "test-key"
        assert query.rest_url == "https://test.supabase.co/rest/v1"
        assert query.headers["apikey"] == "test-key"

    def test_init_from_env(self):
        with patch.dict(os.environ, {"AXIOM_SUPABASE_URL": "https://env.supabase.co"}):
            query = SupabaseQuery()
            assert query.url == "https://env.supabase.co"
