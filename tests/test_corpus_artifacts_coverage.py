import pytest

from axiom_corpus.corpus.artifacts import CorpusArtifactStore, safe_segment, sha256_bytes
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem


def test_store_writes_inventory_and_provision_jsonl(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    inventory_path = store.inventory_path("us", "regulation", "2026-04-29")
    provisions_path = store.provisions_path("us", "regulation", "2026-04-29")

    store.write_inventory(
        inventory_path,
        [SourceInventoryItem(citation_path="us/regulation/7/273/1")],
    )
    store.write_provisions(
        provisions_path,
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
                body="Text.",
            )
        ],
    )

    assert load_source_inventory(inventory_path)[0].citation_path == "us/regulation/7/273/1"
    assert load_provisions(provisions_path)[0].body == "Text."


def test_store_paths_writes_and_iterates_provision_files(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    source_path = store.source_path(
        "us",
        DocumentClass.REGULATION,
        "2026-04-29",
        "official/xml/title-7.xml",
    )
    export_path = store.export_path(
        "jsonl",
        "us",
        DocumentClass.REGULATION,
        "2026-04-29",
        "title-7.jsonl",
    )
    json_path = store.coverage_path("us", DocumentClass.REGULATION, "2026-04-29")
    text_path = store.root / "notes" / "note.txt"
    provisions_path = store.provisions_path("us", DocumentClass.REGULATION, "2026-04-29")

    assert source_path.name == "title-7.xml"
    assert export_path.name == "title-7.jsonl"
    assert store.write_bytes(source_path, b"source") == sha256_bytes(b"source")
    assert store.write_text(text_path, "hello") == sha256_bytes(b"hello")
    assert store.write_json(json_path, {"ok": True}) == sha256_bytes(b'{\n  "ok": true\n}\n')
    store.write_provisions(
        provisions_path,
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
                body="Text.",
            )
        ],
    )

    missing_store = CorpusArtifactStore(tmp_path / "missing")
    assert list(missing_store.iter_provision_files()) == []
    assert list(store.iter_provision_files("us", DocumentClass.REGULATION, "2026-04-29")) == [
        provisions_path
    ]
    assert list(store.iter_provision_files("us", DocumentClass.REGULATION)) == [provisions_path]
    assert list(store.iter_provision_files("us")) == [provisions_path]


def test_safe_segment_rejects_unsafe_segments():
    assert safe_segment(" us:mt ") == "us-mt"
    with pytest.raises(ValueError, match="unsafe path segment"):
        safe_segment("../bad")
    with pytest.raises(ValueError, match="unsafe path segment"):
        safe_segment("")


def test_compare_provision_coverage_reports_missing_and_extra():
    report = compare_provision_coverage(
        (
            SourceInventoryItem(citation_path="us/regulation/7/273/1"),
            SourceInventoryItem(citation_path="us/regulation/7/273/2"),
        ),
        (
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
                body="Text.",
            ),
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/9",
                body="Text.",
            ),
        ),
        jurisdiction="us",
        document_class="regulation",
        version="2026-04-29",
    )

    assert report.matched_count == 1
    assert report.missing_from_provisions == ("us/regulation/7/273/2",)
    assert report.extra_provisions == ("us/regulation/7/273/9",)
    assert not report.complete
