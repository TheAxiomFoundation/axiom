import json

import pytest

from axiom_corpus.corpus.releases import ReleaseManifest, resolve_release_manifest_path


def test_release_manifest_loads_scope_keys(tmp_path):
    path = tmp_path / "current.json"
    path.write_text(
        json.dumps(
            {
                "name": "current",
                "scopes": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "policy",
                        "version": "2026-04-30",
                    }
                ],
            }
        )
    )

    manifest = ReleaseManifest.load(path)

    assert manifest.name == "current"
    assert manifest.scope_keys == (("us-co", "policy", "2026-04-30"),)


def test_release_manifest_rejects_duplicate_scopes(tmp_path):
    path = tmp_path / "current.json"
    scope = {
        "jurisdiction": "us-co",
        "document_class": "policy",
        "version": "2026-04-30",
    }
    path.write_text(json.dumps({"scopes": [scope, scope]}))

    with pytest.raises(ValueError, match="duplicate scope"):
        ReleaseManifest.load(path)


def test_resolve_release_manifest_path_prefers_base_release(tmp_path):
    base = tmp_path / "corpus"
    release_dir = base / "releases"
    release_dir.mkdir(parents=True)
    release_path = release_dir / "current.json"
    release_path.write_text(json.dumps({"scopes": []}))

    assert resolve_release_manifest_path(base, "current") == release_path
