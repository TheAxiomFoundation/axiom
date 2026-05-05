import json
import zipfile
from pathlib import Path

import pytest

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters import indiana
from axiom_corpus.corpus.state_adapters.indiana import (
    extract_indiana_code,
    parse_indiana_title_html,
)

SAMPLE_INDIANA_TITLE_HTML = """<!doctype html>
<html>
<body>
<div class="title" id="6">
  <span id="ic_number">IC 6</span>
  <span id="shortdescription">TITLE 6. TAXATION</span>
</div>
<div class="article" id="6-1.1">
  <span id="ic_number">IC 6-1.1</span>
  <span id="shortdescription">ARTICLE 1.1. PROPERTY TAXES</span>
</div>
<div class="chapter" id="6-1.1-1">
  <span id="ic_number">IC 6-1.1-1</span>
  <span id="shortdescription">Chapter 1. General Definitions</span>
</div>
<div class="section" id="6-1.1-1-1">
  <span id="ic_number">IC 6-1.1-1-1</span>
  <span id="shortdescription">Applicability</span>
</div>
<p>Sec. 1. This chapter applies unless <a href="#6-1.1-1-2">IC 6-1.1-1-2</a> applies.</p>
<p class="derivation">[1975 Property Tax Recodification Citation: New.]</p>
<p>Formerly: Acts 1975, P.L.47, SEC.1.</p>
<div class="section" id="6-1.1-1-2-b">
  <span id="ic_number">IC 6-1.1-1-2</span>
  <span id="shortdescription">Assessment date</span>
</div>
<p>Note: This version of section effective 1-1-2026.</p>
<p>Sec. 2. "Assessment date" has the meaning set forth in IC 6-1.1-2-1.5.</p>
</body>
</html>
"""


def test_parse_indiana_title_html_extracts_hierarchy_body_and_references():
    provisions = parse_indiana_title_html(SAMPLE_INDIANA_TITLE_HTML)

    assert [provision.kind for provision in provisions] == [
        "title",
        "article",
        "chapter",
        "section",
        "section",
    ]
    assert provisions[0].citation_path == "us-in/statute/6"
    assert provisions[1].parent_citation_path == "us-in/statute/6"
    assert provisions[2].parent_citation_path == "us-in/statute/6-1.1"
    assert provisions[3].citation_path == "us-in/statute/6-1.1-1-1"
    assert provisions[3].body is not None
    assert "This chapter applies" in provisions[3].body
    assert provisions[3].references_to == ("us-in/statute/6-1.1-1-2",)
    assert provisions[3].derivation == ("[1975 Property Tax Recodification Citation: New.]",)
    assert provisions[3].source_history == ("Formerly: Acts 1975, P.L.47, SEC.1.",)
    assert provisions[4].citation_path == "us-in/statute/6-1.1-1-2-b"
    assert provisions[4].legal_identifier == "IC 6-1.1-1-2"
    assert provisions[4].parent_citation_path == "us-in/statute/6-1.1-1"
    assert provisions[4].notes == ("Note: This version of section effective 1-1-2026.",)


def test_parse_indiana_title_html_skips_blank_duplicate_heading_before_real_heading():
    provisions = parse_indiana_title_html(
        """<!doctype html><html><body>
        <div class="title" id="3"><span id="ic_number">IC 3</span></div>
        <div class="article" id="3-5"><span id="ic_number">IC 3-5</span></div>
        <div class="chapter" id="3-5-2"><div style="clear: both;"></div></div>
        <div class="chapter" id="3-5-2">
          <span id="ic_number">IC 3-5-2</span>
          <span id="shortdescription">Chapter 2. Definitions</span>
        </div>
        </body></html>"""
    )

    chapter = provisions[-1]
    assert chapter.citation_path == "us-in/statute/3-5-2"
    assert chapter.heading == "Chapter 2. Definitions"
    assert chapter.legal_identifier == "IC 3-5-2"


def test_parse_indiana_title_html_handles_fallback_parents_and_repealed_status():
    provisions = parse_indiana_title_html(
        """<!doctype html><html><body>
        <div class="article" id="6-1"><span id="ic_number">IC 6-1</span></div>
        <div class="chapter" id="6-1-1"><span id="ic_number">IC 6-1-1</span></div>
        <div class="section" id="6-1-1-1">
          <span id="ic_number">IC 6-1-1-1</span>
          <span id="shortdescription">Repealed provision</span>
        </div>
        lead text
        <p>Repealed by P.L.1-2025.</p>
        <p>   </p>
        <div class="chapter" id="6-1-2"><div style="clear: both;"></div></div>
        </body></html>"""
    )

    assert provisions[0].parent_citation_path == "us-in/statute/6"
    assert provisions[1].parent_citation_path == "us-in/statute/6-1"
    assert provisions[2].body is not None
    assert "lead text" in provisions[2].body
    assert provisions[2].source_history == ("Repealed by P.L.1-2025.",)
    assert provisions[-1].citation_path == "us-in/statute/6-1-2"


def test_extract_indiana_code_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source" / "2025_Indiana_Code_HTML"
    source_dir.mkdir(parents=True)
    (source_dir / "6.html").write_text(SAMPLE_INDIANA_TITLE_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_indiana_code(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        source_year=2025,
        source_as_of="2025-06-30",
        expression_date="2025-06-30",
        only_title="6",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 3
    assert report.section_count == 2
    assert report.provisions_written == 5
    assert len(report.source_paths) == 1
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[0].source_format == "indiana-code-html"
    assert records[-1].citation_path == "us-in/statute/6-1.1-1-2-b"
    assert records[-1].metadata is not None
    assert records[-1].metadata["status"] == "future_or_conditional"


def test_extract_indiana_code_source_dir_without_filter_uses_plain_version(tmp_path):
    source_dir = tmp_path / "source" / "2025_Indiana_Code_HTML"
    source_dir.mkdir(parents=True)
    (source_dir / "6.html").write_text(SAMPLE_INDIANA_TITLE_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_indiana_code(store, version="2026-05-05", source_dir=source_dir)

    assert report.provisions_path.name == "2026-05-05.jsonl"


def test_extract_indiana_code_reports_empty_titles_and_continues(tmp_path):
    source_dir = tmp_path / "source" / "2025_Indiana_Code_HTML"
    source_dir.mkdir(parents=True)
    (source_dir / "5.html").write_text("<html><body></body></html>", encoding="utf-8")
    (source_dir / "6.html").write_text(SAMPLE_INDIANA_TITLE_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_indiana_code(store, version="2026-05-05", source_dir=source_dir)

    assert report.provisions_written == 5
    assert report.errors == ("title 5: no provisions parsed",)


def test_extract_indiana_code_rejects_missing_or_empty_selection(tmp_path):
    source_dir = tmp_path / "source" / "2025_Indiana_Code_HTML"
    source_dir.mkdir(parents=True)
    (source_dir / "6.html").write_text(SAMPLE_INDIANA_TITLE_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    with pytest.raises(ValueError, match="no Indiana Code title sources selected"):
        extract_indiana_code(store, version="2026-05-05", source_dir=source_dir, only_title="7")

    with pytest.raises(ValueError, match="no Indiana Code provisions extracted"):
        extract_indiana_code(
            store,
            version="2026-05-05",
            source_dir=source_dir,
            only_title="6",
            limit=0,
        )


def test_extract_indiana_code_deduplicates_source_ids_and_stops_after_limit(tmp_path):
    source_dir = tmp_path / "source" / "2025_Indiana_Code_HTML"
    source_dir.mkdir(parents=True)
    duplicated = SAMPLE_INDIANA_TITLE_HTML.replace(
        "</body>",
        """<div class="section" id="6-1.1-1-1">
          <span id="ic_number">IC 6-1.1-1-1</span>
          <span id="shortdescription">Duplicate heading</span>
        </div></body>""",
    )
    (source_dir / "6.html").write_text(duplicated, encoding="utf-8")
    (source_dir / "7.html").write_text(
        SAMPLE_INDIANA_TITLE_HTML.replace('id="6"', 'id="7"', 1),
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_indiana_code(store, version="2026-05-05", source_dir=source_dir, limit=5)
    records = load_provisions(report.provisions_path)

    assert report.provisions_written == 5
    assert [record.citation_path for record in records].count("us-in/statute/6-1.1-1-1") == 1


def test_extract_indiana_code_from_source_zip_preserves_zip_and_title_sources(tmp_path):
    source_zip = tmp_path / "2025-Indiana-Code-html.zip"
    with zipfile.ZipFile(source_zip, "w") as archive:
        archive.writestr(
            "2025-Indiana-Code-html/2025_Indiana_Code_HTML/6.html",
            SAMPLE_INDIANA_TITLE_HTML,
        )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_indiana_code(
        store,
        version="2026-05-05",
        source_zip=source_zip,
        source_year=2025,
        limit=1,
    )

    assert report.coverage.complete is True
    assert report.provisions_written == 1
    assert [path.name for path in report.source_paths] == [
        "2025-Indiana-Code-html.zip",
        "6.html",
    ]


def test_indiana_source_zip_helpers_validate_cache_and_fallback_members(tmp_path, monkeypatch):
    source_zip = tmp_path / "fallback.zip"
    with zipfile.ZipFile(source_zip, "w") as archive:
        archive.writestr("nested/6.html", SAMPLE_INDIANA_TITLE_HTML)
    data = source_zip.read_bytes()
    download_dir = tmp_path / "downloads"

    class Response:
        content = data

        def raise_for_status(self):
            return None

    calls = []

    def fake_get(url, *, headers, timeout):
        calls.append((url, headers, timeout))
        return Response()

    monkeypatch.setattr(indiana.requests, "get", fake_get)

    downloaded = indiana._indiana_source_zip_bytes(
        source_zip=None,
        source_year=2025,
        download_dir=download_dir,
        base_url="https://example.test/ic/",
    )
    cached = indiana._indiana_source_zip_bytes(
        source_zip=None,
        source_year=2025,
        download_dir=download_dir,
        base_url="https://example.test/ic/",
    )

    with zipfile.ZipFile(source_zip) as archive:
        assert indiana._indiana_zip_title_members(archive, 2025) == ("nested/6.html",)
    assert downloaded == data
    assert cached == data
    assert len(calls) == 1
    assert indiana._title_from_html_name("nested/6.html") == "6"
    with pytest.raises(ValueError, match="not an Indiana title HTML file"):
        indiana._title_from_html_name("README.txt")
    assert indiana.indiana_source_zip_sha256(source_zip) == indiana.sha256_bytes(data)


def test_indiana_source_zip_rejects_invalid_inputs(tmp_path):
    bad_zip = tmp_path / "bad.zip"
    bad_zip.write_bytes(b"not a zip")

    with pytest.raises(ValueError, match="did not return a ZIP"):
        indiana._indiana_source_zip_bytes(
            source_zip=bad_zip,
            source_year=2025,
            download_dir=None,
            base_url="https://example.test/ic/",
        )
    with pytest.raises(ValueError, match="does not exist"):
        tuple(indiana._iter_indiana_title_sources_from_dir(tmp_path / "missing"))
    with pytest.raises(ValueError, match="invalid Indiana title filter"):
        extract_indiana_code(
            CorpusArtifactStore(tmp_path / "corpus"),
            version="2026-05-05",
            source_dir=tmp_path,
            only_title="title x",
        )


def test_extract_indiana_code_cli(tmp_path, capsys):
    source_dir = tmp_path / "source" / "2025_Indiana_Code_HTML"
    source_dir.mkdir(parents=True)
    (source_dir / "6.html").write_text(SAMPLE_INDIANA_TITLE_HTML, encoding="utf-8")
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-indiana-code",
            "--base",
            str(base),
            "--version",
            "2026-05-05",
            "--source-dir",
            str(source_dir),
            "--only-title",
            "6",
            "--source-as-of",
            "2025-06-30",
            "--expression-date",
            "2025-06-30",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["adapter"] == "indiana-code"
    assert payload["coverage_complete"] is True
    assert Path(payload["provisions_path"]).exists()
