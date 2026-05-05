import json

import pytest

from axiom_corpus.corpus.analytics import (
    build_analytics_report,
    load_provision_count_snapshot,
)
from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem


def test_analytics_report_groups_source_provision_and_supabase_counts(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us", "regulation", "2026-04-29"),
        [
            SourceInventoryItem(citation_path="us/regulation/7/273/1"),
            SourceInventoryItem(citation_path="us/regulation/7/273/2"),
        ],
    )
    store.write_provisions(
        store.provisions_path("us", "regulation", "2026-04-29"),
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
                body="Text.",
            )
        ],
    )

    report = build_analytics_report(
        store,
        version="2026-04-29",
        provision_counts={("us", "regulation"): 3},
    )

    assert report.totals_by_document_class()["regulation"]["source_count"] == 2
    assert report.totals_by_document_class()["regulation"]["provision_count"] == 1
    assert report.totals_by_document_class()["regulation"]["missing_count"] == 1
    assert report.totals()["supabase_count"] == 3
    assert report.to_mapping()["rows"][0]["coverage_complete"] is False


def test_load_provision_count_snapshot_supports_doc_type_rows(tmp_path):
    snapshot = tmp_path / "counts.json"
    snapshot.write_text(
        json.dumps(
            {
                "rows": [
                    {"jurisdiction": "us", "doc_type": "statute", "provision_count": 3},
                    {"jurisdiction": "us", "document_class": "regulation", "count": 5},
                    {"jurisdiction": "us-tn", "count": 7},
                ]
            }
        )
    )

    counts = load_provision_count_snapshot(snapshot)

    assert counts == {
        ("us", "statute"): 3,
        ("us", "regulation"): 5,
        ("us-tn", "statute"): 7,
    }


def test_load_provision_count_snapshot_supports_all_shapes(tmp_path):
    assert load_provision_count_snapshot(None) == {}

    plain = tmp_path / "plain.json"
    plain.write_text(json.dumps({"us": 2, "us-mt": 3}))
    assert load_provision_count_snapshot(plain) == {
        ("us", "statute"): 2,
        ("us-mt", "statute"): 3,
    }

    rows = tmp_path / "rows.json"
    rows.write_text(
        json.dumps(
            [
                {"jurisdiction": "us", "document_type": "regulation", "section_count": 4},
                {"jurisdiction": "", "count": 10},
                {"jurisdiction": "us", "document_class": "regulation", "count": 1},
            ]
        )
    )
    assert load_provision_count_snapshot(rows) == {("us", "regulation"): 5}

    invalid = tmp_path / "invalid.json"
    invalid.write_text(json.dumps("bad"))
    with pytest.raises(ValueError, match="count snapshot must be a JSON object or list"):
        load_provision_count_snapshot(invalid)


def test_analytics_report_merges_scoped_artifacts_and_filters(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-mt", "statute", "2026-05-05-title-1"),
        [
            SourceInventoryItem(citation_path="us-mt/statute/1"),
            SourceInventoryItem(citation_path="us-mt/statute/1-1"),
        ],
    )
    store.write_inventory(
        store.inventory_path("us-mt", "statute", "2026-05-05-title-2"),
        [
            SourceInventoryItem(citation_path="us-mt/statute/1-1"),
            SourceInventoryItem(citation_path="us-mt/statute/2"),
        ],
    )
    store.write_provisions(
        store.provisions_path("us-mt", "statute", "2026-05-05-title-1"),
        [
            ProvisionRecord(
                jurisdiction="us-mt",
                document_class="statute",
                citation_path="us-mt/statute/1",
                body="Title.",
            )
        ],
    )
    store.write_provisions(
        store.provisions_path("us-mt", "statute", "2026-05-05-title-2"),
        [
            ProvisionRecord(
                jurisdiction="us-mt",
                document_class="statute",
                citation_path="us-mt/statute/2",
                body="Title.",
            )
        ],
    )
    store.write_inventory(
        store.inventory_path("us", "regulation", "2026-05-05"),
        [SourceInventoryItem(citation_path="us/regulation/1")],
    )
    store.write_provisions(
        store.provisions_path("us", "regulation", "2026-05-05"),
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/1",
                body="Rule.",
            )
        ],
    )

    report = build_analytics_report(
        store,
        version="2026-05-05",
        provision_counts={("us-mt", "statute"): 2, ("us", "regulation"): 1},
        jurisdictions=("us-mt",),
        document_classes=("statute",),
    )

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.jurisdiction == "us-mt"
    assert row.source_count == 3
    assert row.provision_count == 2
    assert row.missing_count == 1
    assert row.supabase_count == 2


def test_analytics_report_prefers_exact_artifacts_over_scoped(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-mt", "statute", "2026-05-05"),
        [SourceInventoryItem(citation_path="us-mt/statute/exact")],
    )
    store.write_inventory(
        store.inventory_path("us-mt", "statute", "2026-05-05-title-1"),
        [SourceInventoryItem(citation_path="us-mt/statute/scoped")],
    )
    store.write_provisions(
        store.provisions_path("us-mt", "statute", "2026-05-05"),
        [
            ProvisionRecord(
                jurisdiction="us-mt",
                document_class="statute",
                citation_path="us-mt/statute/exact",
                body="Exact.",
            )
        ],
    )
    store.write_provisions(
        store.provisions_path("us-mt", "statute", "2026-05-05-title-1"),
        [
            ProvisionRecord(
                jurisdiction="us-mt",
                document_class="statute",
                citation_path="us-mt/statute/scoped",
                body="Scoped.",
            )
        ],
    )

    report = build_analytics_report(store, version="2026-05-05")

    assert report.rows[0].source_count == 1
    assert report.rows[0].provision_count == 1
    assert report.rows[0].coverage_complete is True
    assert report.totals_by_document_class()["statute"]["complete_count"] == 1


def test_analytics_report_handles_empty_store(tmp_path):
    report = build_analytics_report(CorpusArtifactStore(tmp_path / "empty"), version="2026-05-05")

    assert report.rows == ()
    assert report.totals()["source_count"] == 0
