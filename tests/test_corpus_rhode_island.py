import json
import os
from pathlib import Path

import pytest
import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters import rhode_island as rhode_island_adapter
from axiom_corpus.corpus.state_adapters.rhode_island import (
    RHODE_ISLAND_GENERAL_LAWS_BASE_URL,
    extract_rhode_island_general_laws,
    parse_rhode_island_general_laws_index,
    parse_rhode_island_section_html,
)

SAMPLE_RI_INDEX = """<!doctype html>
<html><body>
<table>
<tr><td><a href="http://webserver.rilegislature.gov/Statutes/TITLE1/INDEX.HTM">1</a></td><td>Aeronautics</td></tr>
<tr><td><a href="http://webserver.rilegislature.gov//Statutes/TITLE6A/INDEX.HTM">6A</a></td><td>Uniform Commercial Code</td></tr>
<tr><td><a href="/Statutes/TITLE40.1/INDEX.HTM">40.1</a></td><td>Behavioral Healthcare</td></tr>
</table>
</body></html>
"""

SAMPLE_TITLE_1 = """<!doctype html>
<html><body>
<div><h1><center>Title 1<br>Aeronautics</center></h1></div>
<center><h3>Index of Chapters</h3></center>
<p><a href="1-1/INDEX.htm">Chapter 1-1&nbsp;Airports Division [Repealed.]</a></p>
<p><a href="1-2/INDEX.htm">Chapter 1-2&nbsp;Airports and Landing Fields</a></p>
</body></html>
"""

SAMPLE_CHAPTER_1_2 = """<!doctype html>
<html><body>
<div><h2><center>Chapter 2<br>Airports and Landing Fields</center></h2></div>
<center><h3>Index of Sections</h3></center>
<p><a href="1-2-1.htm">&sect;&nbsp;1-2-1.&nbsp;Powers of the airport corporation.</a></p>
<p><a href="1-2-5.htm">&sect;&nbsp;1-2-5.&nbsp;Repealed.</a></p>
</body></html>
"""

SAMPLE_SECTION_1_2_1 = """<!doctype html>
<html><body>
<div><h1><center>Title 1<br>Aeronautics</center></h1></div>
<div><h2><center>Chapter 2<br>Airports and Landing Fields</center></h2></div>
<p><center><h3>R.I. Gen. Laws &sect; 1-2-1</h3></center></p>
<div>
  <p style="margin-left:0px"><b>&sect;&nbsp;1-2-1.&nbsp;Powers of the airport corporation.</b></p>
  <p style="margin-left:0px">The corporation may act under &sect; 1-2-2.</p>
  <div><p>History of Section.<br>P.L. 1939, ch. 660, &sect; 106; G.L. 1956 &sect; 1-2-1.</p></div>
</div>
</body></html>
"""

SAMPLE_SECTION_1_2_5 = """<!doctype html>
<html><body>
<div><h1><center>Title 1<br>Aeronautics</center></h1></div>
<div><h2><center>Chapter 2<br>Airports and Landing Fields</center></h2></div>
<p><center><h3>R.I. Gen. Laws &sect; 1-2-5</h3></center></p>
<div>
  <p style="margin-left:0px"><b>&sect;&nbsp;1-2-5.&nbsp;Repealed.</b></p>
  <div><p>History of Section.<br>Repealed by P.L. 2000, ch. 371, &sect; 1.</p></div>
</div>
</body></html>
"""

SAMPLE_RANGE_SECTION = """<!doctype html>
<html><body>
<div>
  <p style="margin-left:0px"><b>&sect;&sect;&nbsp;1-1-2 &#8212; 1-1-5.&nbsp;Repealed.</b></p>
</div>
</body></html>
"""

SAMPLE_TITLE_6A = """<!doctype html>
<html><body>
<div><h1><center>Title 6A<br>Uniform Commercial Code</center></h1></div>
<center><h3>Index of Chapters</h3></center>
<p><a href="6A-11/INDEX.htm">Chapter 6A-11&nbsp;Transitional Provisions</a></p>
</body></html>
"""

SAMPLE_CHAPTER_6A_11 = """<!doctype html>
<html><body>
<div><h2><center>Chapter 11<br>Transitional Provisions</center></h2></div>
<center><h3>Index of Parts</h3></center>
<p><a href="6A-1/INDEX.htm">Part 1&nbsp;General Provisions and Definitions</a></p>
</body></html>
"""

SAMPLE_PART_6A_11_1 = """<!doctype html>
<html><body>
<div><h3><center>Part 1<br>General Provisions and Definitions</center></h3></div>
<center><h3>Index of Sections</h3></center>
<p><a href="6A-11-101_6A-11-101.htm">&sect;&nbsp;6A-11-101.&nbsp;Short title. [Effective until January 1, 2026.]6A-11-101.&nbsp;Short title. [Effective January 1, 2026.]</a></p>
</body></html>
"""

SAMPLE_EFFECTIVE_SECTION = """<!doctype html>
<html><body>
<div><h1><center>Title 6A<br>Uniform Commercial Code</center></h1></div>
<div><h2><center>Chapter 11<br>Transitional Provisions</center></h2></div>
<div><h3><center>Part 1<br>General Provisions and Definitions</center></h3></div>
<p><center><h3>R.I. Gen. Laws &sect; 6A-11-101</h3></center></p>
<div>
  <p style="margin-left:0px"><b>&sect;&nbsp;6A-11-101.&nbsp;Short title. [Effective until January 1, 2026.]</b></p>
  <p style="margin-left:0px">Old text.</p>
  <div><p>History of Section.<br>P.L. 2024, ch. 65, &sect; 12.</p></div>
</div>
<div>
  <p style="margin-left:0px"><b>&sect;&nbsp;6A-11-101.&nbsp;Short title. [Effective January 1, 2026.]</b></p>
  <p style="margin-left:0px">New text.</p>
  <div><p>History of Section.<br>P.L. 2025, ch. 1, &sect; 1, effective January 1, 2026.</p></div>
</div>
</body></html>
"""


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_source_dir(root: Path) -> Path:
    source_dir = root / "source"
    _write(source_dir / "Statutes.html", SAMPLE_RI_INDEX)
    _write(source_dir / "TITLE1/INDEX.HTM", SAMPLE_TITLE_1)
    _write(source_dir / "TITLE1/1-2/INDEX.htm", SAMPLE_CHAPTER_1_2)
    _write(source_dir / "TITLE1/1-2/1-2-1.htm", SAMPLE_SECTION_1_2_1)
    _write(source_dir / "TITLE1/1-2/1-2-5.htm", SAMPLE_SECTION_1_2_5)
    _write(source_dir / "TITLE6A/INDEX.HTM", SAMPLE_TITLE_6A)
    _write(source_dir / "TITLE6A/6A-11/INDEX.htm", SAMPLE_CHAPTER_6A_11)
    _write(source_dir / "TITLE6A/6A-11/6A-1/INDEX.htm", SAMPLE_PART_6A_11_1)
    _write(
        source_dir / "TITLE6A/6A-11/6A-1/6A-11-101_6A-11-101.htm",
        SAMPLE_EFFECTIVE_SECTION,
    )
    return source_dir


def test_parse_rhode_island_general_laws_index_extracts_title_links():
    titles = parse_rhode_island_general_laws_index(SAMPLE_RI_INDEX)

    assert [title.number for title in titles] == ["1", "6A", "40.1"]
    assert titles[0].citation_path == "us-ri/statute/title-1"
    assert titles[1].relative_path == "TITLE6A/INDEX.HTM"
    assert titles[2].heading == "Behavioral Healthcare"


def test_parse_rhode_island_section_html_handles_ranges_repealed_refs_and_history():
    range_section = parse_rhode_island_section_html(SAMPLE_RANGE_SECTION)[0]
    active_section = parse_rhode_island_section_html(SAMPLE_SECTION_1_2_1)[0]

    assert range_section.source_id == "1-1-2"
    assert range_section.display_number == "1-1-2 - 1-1-5"
    assert range_section.range_end == "1-1-5"
    assert range_section.status == "repealed"
    assert range_section.legal_identifier == "R.I. Gen. Laws §§ 1-1-2 - 1-1-5"
    assert active_section.body == "The corporation may act under § 1-2-2."
    assert active_section.references_to == ("us-ri/statute/1-2-2",)
    assert active_section.source_history == (
        "P.L. 1939, ch. 660, § 106; G.L. 1956 § 1-2-1.",
    )


def test_extract_rhode_island_general_laws_from_source_dir_writes_artifacts(tmp_path):
    source_dir = _write_source_dir(tmp_path)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_rhode_island_general_laws(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        source_as_of="2026-03-18",
        expression_date="2026-05-05",
        only_chapter="1-2",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    assert report.provisions_written == 4
    assert report.provisions_path.name == "2026-05-05-us-ri-chapter-1-2.jsonl"

    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert [item.citation_path for item in inventory] == [
        "us-ri/statute/title-1",
        "us-ri/statute/1-2",
        "us-ri/statute/1-2-1",
        "us-ri/statute/1-2-5",
    ]
    assert records[1].parent_citation_path == "us-ri/statute/title-1"
    assert records[2].parent_citation_path == "us-ri/statute/1-2"
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == ["us-ri/statute/1-2-2"]
    assert records[3].metadata["status"] == "repealed"


def test_extract_rhode_island_general_laws_handles_parts_and_effective_variants(tmp_path):
    source_dir = _write_source_dir(tmp_path)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_rhode_island_general_laws(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        only_chapter="6A-11",
    )

    records = load_provisions(report.provisions_path)
    assert report.coverage.complete
    assert [record.citation_path for record in records] == [
        "us-ri/statute/title-6A",
        "us-ri/statute/6A-11",
        "us-ri/statute/6A-11/part-1",
        "us-ri/statute/6A-11-101",
        "us-ri/statute/6A-11-101@effective-2026-01-01",
    ]
    assert records[2].kind == "part"
    assert records[3].metadata["status"] == "effective_until"
    assert records[4].metadata["status"] == "future_or_conditional"
    assert records[4].metadata["canonical_citation_path"] == "us-ri/statute/6A-11-101"
    assert records[4].parent_citation_path == "us-ri/statute/6A-11/part-1"


def test_extract_rhode_island_general_laws_cli_local_source(tmp_path, capsys):
    source_dir = _write_source_dir(tmp_path)

    exit_code = main(
        [
            "extract-rhode-island-general-laws",
            "--base",
            str(tmp_path / "corpus"),
            "--version",
            "2026-05-05",
            "--source-dir",
            str(source_dir),
            "--only-chapter",
            "1-2",
            "--source-as-of",
            "2026-03-18",
            "--expression-date",
            "2026-05-05",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["adapter"] == "rhode-island-general-laws"
    assert payload["coverage_complete"] is True
    assert payload["provisions_written"] == 4


def test_rhode_island_run_id_only_scopes_filtered_runs():
    assert (
        rhode_island_adapter._rhode_island_run_id(
            "2026-05-06",
            title_filter=None,
            chapter_filter=None,
            limit=None,
        )
        == "2026-05-06"
    )
    assert (
        rhode_island_adapter._rhode_island_run_id(
            "2026-05-06",
            title_filter="1",
            chapter_filter=None,
            limit=None,
        )
        == "2026-05-06-us-ri-title-1"
    )


def test_extract_state_statutes_dry_run_allows_live_rhode_island_source(tmp_path, capsys):
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        f"""
version: "2026-05-05"
sources:
  - source_id: us-ri-general-laws
    jurisdiction: us-ri
    document_class: statute
    adapter: rhode-island-general-laws
    source_url: {RHODE_ISLAND_GENERAL_LAWS_BASE_URL}
    options:
      only_chapter: "1-2"
      download_dir: /tmp/axiom-state-cache/us-ri
""",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "extract-state-statutes",
            "--base",
            str(tmp_path / "corpus"),
            "--manifest",
            str(manifest),
            "--dry-run",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["rows"][0]["adapter"] == "rhode-island-general-laws"
    assert payload["rows"][0]["source_path"] is None
    assert payload["rows"][0]["source_path_exists"] is True


@pytest.mark.skipif(
    os.environ.get("AXIOM_RUN_LIVE_RI_SMOKE") != "1",
    reason="set AXIOM_RUN_LIVE_RI_SMOKE=1 to hit the official RI source",
)
def test_extract_rhode_island_general_laws_live_smoke(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_rhode_island_general_laws(
        store,
        version="2026-05-05",
        only_chapter="1-2",
        limit=2,
        download_dir=Path("/tmp/axiom-state-cache/us-ri"),
        workers=2,
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    assert report.provisions_written == 4


def test_rhode_island_fetcher_cache_live_and_source_lookup(tmp_path, monkeypatch):
    download_dir = tmp_path / "download"
    cached = download_dir / "TITLE1" / "INDEX.HTM"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"cached")
    fetcher = rhode_island_adapter._RhodeIslandFetcher(
        base_url=RHODE_ISLAND_GENERAL_LAWS_BASE_URL,
        source_dir=None,
        download_dir=download_dir,
    )

    assert fetcher.fetch("TITLE1/INDEX.HTM").data == b"cached"
    monkeypatch.setattr(rhode_island_adapter, "_download_rhode_island_page", lambda url: b"live")
    live = fetcher.fetch("TITLE2/INDEX.HTM")
    assert live.data == b"live"
    assert (download_dir / "TITLE2" / "INDEX.HTM").read_bytes() == b"live"

    source_dir = tmp_path / "source"
    nested = source_dir / rhode_island_adapter.RHODE_ISLAND_GENERAL_LAWS_SOURCE_FORMAT / "TITLE3" / "INDEX.HTM"
    nested.parent.mkdir(parents=True)
    nested.write_bytes(b"nested")
    assert rhode_island_adapter._read_source_file(source_dir, "TITLE3/INDEX.HTM") == b"nested"
    with pytest.raises(FileNotFoundError):
        rhode_island_adapter._read_source_file(source_dir, "TITLE4/INDEX.HTM")


def test_rhode_island_index_container_and_section_edge_cases():
    titles = parse_rhode_island_general_laws_index(
        """<html><body>
        <a href="bad.htm">No title</a>
        <a href="/Statutes/TITLE1/INDEX.HTM">1 Aeronautics</a>
        <a href="/Statutes/TITLE1/INDEX.HTM">1 Duplicate</a>
        </body></html>"""
    )
    assert len(titles) == 1

    title = titles[0]
    title_page = rhode_island_adapter.parse_rhode_island_title_index(
        """<html><body>
        <h1>R.I. Gen. Laws</h1>
        <a href="bad.htm">Bad</a>
        <a href="1-1/INDEX.htm">Chapter 1-1 Valid</a>
        </body></html>""",
        title=title,
    )
    assert [link.source_id for link in title_page.child_containers] == ["1-1"]

    container = title_page.child_containers[0]
    parsed_container = rhode_island_adapter.parse_rhode_island_container_index(
        """<html><body>
        <h2>Chapter 1-1 Valid</h2>
        <p>History of Section. P.L. 1956, ch. 1.</p>
        <a href="sub/INDEX.htm">Article 2 Child Article</a>
        <a href="bad.htm">Bad section</a>
        <a href="1-1-1.htm">No caption but filename works</a>
        </body></html>""",
        parent=container,
    )
    assert parsed_container.child_containers[0].kind == "article"
    assert parsed_container.section_links[0].source_id == "1-1-1"
    assert parsed_container.source_history == ("P.L. 1956, ch. 1.",)

    duplicate_html = """<html><body>
    <div><p><b>&sect;&nbsp;1-1-1.&nbsp;Rule.</b></p><p>Base text.</p></div>
    <div><p><b>&sect;&nbsp;1-1-1.&nbsp;Rule. [Effective January 1, 2026.]</b></p><p>New text.</p></div>
    <div><p><b>&sect;&nbsp;1-1-1.&nbsp;Rule. [Effective January 1, 2026.]</b></p><p>Newest text.</p></div>
    <b>&sect;&nbsp;1-1-2.&nbsp;Orphan.</b>
    <div><p><b>No section here</b></p></div>
    </body></html>"""
    sections = parse_rhode_island_section_html(duplicate_html)
    assert [section.source_id for section in sections] == [
        "1-1-1",
        "1-1-1@effective-2026-01-01",
        "1-1-1@effective-2026-01-01-3",
    ]
    repeated_block = BeautifulSoup(
        """<div>
        <p><b>&sect;&nbsp;1-1-1.&nbsp;Rule.</b><b>&sect;&nbsp;1-1-2.&nbsp;Rule.</b></p>
        </div>""",
        "lxml",
    )
    assert len(rhode_island_adapter._section_blocks(repeated_block)) == 1
    assert (
        rhode_island_adapter._section_heading_tag(
            BeautifulSoup("<div><b>No section here</b></div>", "lxml").div
        )
        is None
    )


def test_rhode_island_append_and_metadata_helpers(tmp_path):
    source = rhode_island_adapter._RecordedSource(
        source_url="https://example.test/TITLE1/1-1/1-1-1.htm",
        source_path="sources/us-ri/statute/test/TITLE1/1-1/1-1-1.htm",
        sha256="abc",
    )
    section = rhode_island_adapter.RhodeIslandSection(
        section="1-1-1",
        source_id="1-1-1@variant-2",
        display_number="1-1-1 - 1-1-2",
        heading="Rule",
        body="Body",
        parent_citation_path="us-ri/statute/1-1",
        level=2,
        ordinal=0,
        title="1",
        chapter="1-1",
        range_end="1-1-2",
        related_sections=("1-1-2",),
        references_to=("us-ri/statute/1-1-3",),
        source_history=("P.L. 1956, ch. 1.",),
        effective_notes=("Effective January 1, 2026.",),
        status="future_or_conditional",
        variant="variant-2",
    )
    container = rhode_island_adapter.RhodeIslandContainerLink(
        kind="chapter",
        source_id="1-1",
        display_number="1-1",
        heading="Reserved",
        relative_path="TITLE1/1-1/INDEX.htm",
        ordinal=0,
        parent_citation_path="us-ri/statute/title-1",
        level=1,
        title="1",
        chapter="1-1",
        effective_notes=("Effective until January 1, 2026.",),
        status="reserved",
        source_year=2026,
    )
    items = []
    records = []
    rhode_island_adapter._append_container(
        items,
        records,
        container,
        source=source,
        version="test",
        source_as_of="2026-05-06",
        expression_date="2026-05-06",
    )
    rhode_island_adapter._append_section(
        items,
        records,
        section,
        source=source,
        version="test",
        source_as_of="2026-05-06",
        expression_date="2026-05-06",
        source_year=2026,
    )

    assert records[0].metadata["status"] == "reserved"
    assert records[1].metadata["range_end"] == "1-1-2"
    assert records[1].identifiers["rhode_island:variant"] == "variant-2"
    assert rhode_island_adapter._fetch_pages(
        rhode_island_adapter._RhodeIslandFetcher(
            base_url=RHODE_ISLAND_GENERAL_LAWS_BASE_URL,
            source_dir=tmp_path,
            download_dir=None,
        ),
        [],
        workers=2,
    ) == ()


def test_rhode_island_helper_status_filters_paths_and_download(monkeypatch, tmp_path):
    assert rhode_island_adapter._status("Reserved", None, ()) == "reserved"
    assert rhode_island_adapter._status("Obsolete", None, ()) == "obsolete"
    assert rhode_island_adapter._status("Superseded", None, ()) == "superseded"
    assert rhode_island_adapter._status(None, None, (), effective_notes=("Effective until 2026",)) == "effective_until"
    assert rhode_island_adapter._status(None, None, (), effective_notes=("Effective January 1, 2026",)) == "future_or_conditional"
    assert rhode_island_adapter._variant_for_occurrence(("Effective Not A Date",), 2) == "variant-2"
    assert (
        rhode_island_adapter._variant_for_occurrence(("Effective February 30, 2026.",), 2)
        == "variant-2"
    )
    assert rhode_island_adapter._variant_for_occurrence((), 2) == "variant-2"
    assert (
        rhode_island_adapter._disambiguated_variant(
            "variant",
            occurrence=2,
            section_number="1-1-1",
            used_source_ids={"1-1-1@variant-2"},
        )
        == "variant-3"
    )
    assert rhode_island_adapter._title_filter(None) is None
    assert rhode_island_adapter._chapter_filter(None) is None
    with pytest.raises(ValueError, match="invalid Rhode Island title filter"):
        rhode_island_adapter._title_filter("title x")
    with pytest.raises(ValueError, match="invalid Rhode Island chapter filter"):
        rhode_island_adapter._chapter_filter("chapter x")
    assert rhode_island_adapter._rhode_island_run_id(
        "2026-05-06",
        title_filter="1",
        chapter_filter=None,
        limit=2,
    ) == "2026-05-06-us-ri-title-1-limit-2"
    assert rhode_island_adapter._section_from_relative("bad.htm") is None
    assert rhode_island_adapter._title_from_section("bad") is None
    assert rhode_island_adapter._chapter_from_section("bad") is None
    assert rhode_island_adapter._clean_heading(None) is None
    assert rhode_island_adapter._normalize_relative_path("") == "Statutes.html"
    assert (
        rhode_island_adapter._normalize_relative_path("https://webserver.rilegislature.gov/Statutes/TITLE1/INDEX.HTM")
        == "TITLE1/INDEX.HTM"
    )
    assert rhode_island_adapter._date_text(None, "fallback") == "fallback"

    soup = BeautifulSoup(
        """<html><body>
        <h3>Index of Sections</h3>
        <a href="1-1-1.htm">Section</a>
        <a href="image.png">Image</a>
        <p>History of Section. P.L. 1956, ch. 1.</p>
        </body></html>""",
        "lxml",
    )
    assert len(rhode_island_adapter._index_links(soup)) == 1
    assert rhode_island_adapter._source_history(soup) == ("P.L. 1956, ch. 1.",)
    assert rhode_island_adapter._source_history(BeautifulSoup("<span>No history</span>", "lxml")) == ()
    assert (
        rhode_island_adapter._page_heading(
            BeautifulSoup("<h2>Index of Chapters</h2><h2>Chapter 1-1 Valid</h2>", "lxml"),
            "chapter",
        )
        == "Valid"
    )
    assert (
        rhode_island_adapter._page_heading(
            BeautifulSoup("<h1>R.I. Gen. Laws</h1><h1>Title 2 Rules</h1>", "lxml"),
            "title",
        )
        == "Rules"
    )
    assert rhode_island_adapter._page_heading(BeautifulSoup("<html></html>", "lxml"), "chapter") is None

    class OkResponse:
        content = b"ok"

        def raise_for_status(self):
            return None

    monkeypatch.setattr(rhode_island_adapter.requests, "get", lambda *args, **kwargs: OkResponse())
    assert rhode_island_adapter._download_rhode_island_page("https://example.test") == b"ok"

    def bad_get(*args, **kwargs):
        raise requests.RequestException("boom")

    monkeypatch.setattr(rhode_island_adapter.requests, "get", bad_get)
    monkeypatch.setattr(rhode_island_adapter.time, "sleep", lambda seconds: None)
    with pytest.raises(ValueError, match="failed to fetch Rhode Island source page"):
        rhode_island_adapter._download_rhode_island_page("https://example.test")

    cache_file = tmp_path / "cache.htm"
    rhode_island_adapter._write_cache_bytes(cache_file, b"cached")
    assert cache_file.read_bytes() == b"cached"


def test_rhode_island_extract_limit_filter_and_append_section_edges(tmp_path):
    source_dir = _write_source_dir(tmp_path)

    with pytest.raises(ValueError, match="no Rhode Island General Laws titles selected"):
        extract_rhode_island_general_laws(
            CorpusArtifactStore(tmp_path / "missing-title-corpus"),
            version="2026-05-05",
            source_dir=source_dir,
            only_title="99",
        )
    with pytest.raises(ValueError, match="no Rhode Island General Laws chapters selected"):
        extract_rhode_island_general_laws(
            CorpusArtifactStore(tmp_path / "missing-chapter-corpus"),
            version="2026-05-05",
            source_dir=source_dir,
            only_chapter="1-9",
        )
    with pytest.raises(ValueError, match="no Rhode Island General Laws provisions extracted"):
        extract_rhode_island_general_laws(
            CorpusArtifactStore(tmp_path / "limit-zero-corpus"),
            version="2026-05-05",
            source_dir=source_dir,
            only_title="1",
            limit=0,
        )

    fetcher = rhode_island_adapter._RhodeIslandFetcher(
        base_url=RHODE_ISLAND_GENERAL_LAWS_BASE_URL,
        source_dir=source_dir,
        download_dir=None,
    )
    store = CorpusArtifactStore(tmp_path / "append-corpus")
    link = rhode_island_adapter.RhodeIslandContainerLink(
        kind="section",
        source_id="1-2-1",
        display_number="1-2-1",
        heading="Powers of the airport corporation",
        relative_path="TITLE1/1-2/1-2-1.htm",
        ordinal=0,
        parent_citation_path="us-ri/statute/1-2",
        level=2,
        title="1",
        chapter="1-2",
    )

    remaining, written = rhode_island_adapter._append_sections_from_links(
        [],
        [],
        set(),
        fetcher=fetcher,
        store=store,
        jurisdiction="us-ri",
        run_id="test",
        section_links=(link,),
        source_paths=[],
        source_by_relative={},
        source_year=2026,
        source_as_of="2026-05-06",
        expression_date="2026-05-06",
        remaining_sections=0,
        workers=1,
    )
    assert remaining == 0
    assert written == 0

    seen = {"us-ri/statute/1-2-1"}
    remaining, written = rhode_island_adapter._append_sections_from_links(
        [],
        [],
        seen,
        fetcher=fetcher,
        store=store,
        jurisdiction="us-ri",
        run_id="test",
        section_links=(link,),
        source_paths=[],
        source_by_relative={},
        source_year=2026,
        source_as_of="2026-05-06",
        expression_date="2026-05-06",
        remaining_sections=None,
        workers=1,
    )
    assert remaining is None
    assert written == 0


def test_rhode_island_section_body_and_reference_edges():
    soup = BeautifulSoup(
        """<div>
        <p><b>&sect;&nbsp;1-2-1.&nbsp;Rule.</b></p>
        <p> </p>
        <p>See <a href="1-2-2.htm">related section</a> and &sect; 1-2-3.</p>
        <div>History of Section. P.L. 1956, ch. 1. See <a href="1-2-4.htm">link</a>.</div>
        </div>""",
        "lxml",
    )
    block = soup.find("div")
    heading = block.find("b")

    body, history, references = rhode_island_adapter._section_body_history_references(block, heading)
    assert body == ["See related section and § 1-2-3."]
    assert history == ("P.L. 1956, ch. 1. See link .",)
    assert references == (
        "us-ri/statute/1-2-2",
        "us-ri/statute/1-2-3",
        "us-ri/statute/1-2-4",
    )
    empty_nested = BeautifulSoup(
        "<div><p><b>&sect;&nbsp;1-2-1.&nbsp;Rule.</b></p><div> </div></div>",
        "lxml",
    )
    assert rhode_island_adapter._section_body_history_references(
        empty_nested.div,
        empty_nested.find("b"),
    ) == ([], (), ())
    assert rhode_island_adapter._parse_section_caption("not a section").source_id is None
    assert rhode_island_adapter._parse_container_caption(
        "Chapters 1-1 to 1-2 Repealed",
        fallback_kind="chapter",
        fallback_number="1-1",
    ).kind == "chapter"
    title_heading = rhode_island_adapter._page_heading(
        BeautifulSoup("<h1>Title 1 Aeronautics</h1>", "lxml"),
        "title",
    )
    assert title_heading == "Aeronautics"
