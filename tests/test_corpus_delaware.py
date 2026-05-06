import json
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters import delaware as delaware_adapter
from axiom_corpus.corpus.state_adapters.delaware import (
    extract_delaware_code,
    parse_delaware_chapter_html,
    parse_delaware_code_index,
    parse_delaware_title_html,
)

SAMPLE_DELAWARE_INDEX = """<!doctype html>
<html>
<body>
<div class="title-links"><a href="constitution/index.html">The Delaware Constitution</a></div>
<div class="title-links"><a href="title1/index.html">Title 1 - General Provisions</a></div>
<div class="title-links"><a href="title30/index.html">Title 30 - State Taxes</a></div>
</body>
</html>
"""

SAMPLE_DELAWARE_TITLE = """<!doctype html>
<html>
<body>
<div id="content" class="container container-home" role="main">
  <div class="row">
    <div class="col-xs-24"><span class="breadcrumb delcrumb">Title 30</span><h2>State Taxes</h2></div>
  </div>
  <div>
    <h3>Part II</h3>
    <h3>Income, Inheritance and Estate Taxes</h3>
    <div class="title-links"><a href="../title30/c011/index.html">Chapter 11. PERSONAL INCOME TAX</a></div>
    <div class="title-links"><a href="../title30/c014/index.html">Chapter 14. Gift Tax [Repealed].</a></div>
    <div class="title-links"><a href="../title30/c020a/index.html">Chapter 20A. Veterans’ Opportunity Credit</a></div>
  </div>
</div>
</body>
</html>
"""

SAMPLE_CHAPTER_WITH_SUBCHAPTERS = """<!doctype html>
<html>
<body>
<div id="content" class="container container-home" role="main">
  <div id="TitleHead">
    <h1>TITLE 30</h1>
    <h4>State Taxes</h4>
    <h2>Income, Inheritance and Estate Taxes</h2>
    <h3>CHAPTER 11. Personal Income Tax</h3>
  </div>
  <div class="title-links"><a href="../../title30/c011/sc01/index.html">Subchapter I. General Provisions</a></div>
</div>
</body>
</html>
"""

SAMPLE_SUBCHAPTER = """<!doctype html>
<html>
<body>
<div id="content" class="container container-home" role="main">
  <ul class="chaptersections">
    <li><a href="#1101">§ 1101</a></li>
    <li><a href="#1102">§ 1102</a></li>
  </ul>
  <div id="TitleHead">
    <h1>TITLE 30</h1>
    <h4>State Taxes</h4>
    <h2>Income, Inheritance and Estate Taxes</h2>
    <h3>CHAPTER 11. Personal Income Tax</h3>
    <h4>Subchapter I. General Provisions</h4>
  </div>
  <div id="CodeBody">
    <div class="Section">
      <div class="SectionHead" id="1101">§ 1101. Meaning of terms.</div>
      <p class="subsection">Any term used in this chapter has the same meaning unless <a href="../sc02/index.html#1105">§ 1105</a> applies. See also § 1102 of this title and 26 U.S.C. § 1.</p>
      30 Del. C. 1953, § 1101;
      <a href="https://legis.delaware.gov/SessionLaws?volume=57&amp;chapter=737">57 Del. Laws, c. 737, § 1</a>;
    </div>
    <div class="Section">
      <div class="SectionHead" id="1102">§ 1102. Future rule [Effective upon meeting the contingency in 85 Del. Laws, c. 1, § 2].</div>
      <p class="subsection">The Director shall publish rates.</p>
      85 Del. Laws, c. 1, § 2;
    </div>
  </div>
</div>
</body>
</html>
"""

SAMPLE_REPEALED_CHAPTER = """<!doctype html>
<html>
<body>
<div id="content" class="container container-home" role="main">
  <div id="TitleHead">
    <h1>TITLE 30</h1>
    <h4>State Taxes</h4>
    <h3>CHAPTER 14. Gift Tax [Repealed].</h3>
  </div>
  <div id="CodeBody">
    <div class="Section">
      <div class="SectionHead" id="1401-1409">§§ 1401-1409. Definitions; imposition; rates [Repealed].</div>
      <p class="subsection">Repealed by 71 Del. Laws, c. 130, § 1.</p>
    </div>
  </div>
</div>
</body>
</html>
"""


def _write_delaware_fixture_tree(base: Path) -> Path:
    source_dir = base / "source"
    (source_dir / "title30" / "c011" / "sc01").mkdir(parents=True)
    (source_dir / "title30" / "c014").mkdir(parents=True)
    (source_dir / "index.html").write_text(SAMPLE_DELAWARE_INDEX, encoding="utf-8")
    (source_dir / "title30" / "index.html").write_text(
        SAMPLE_DELAWARE_TITLE.replace(
            '<div class="title-links"><a href="../title30/c020a/index.html">Chapter 20A. Veterans’ Opportunity Credit</a></div>',
            "",
        ),
        encoding="utf-8",
    )
    (source_dir / "title30" / "c011" / "index.html").write_text(
        SAMPLE_CHAPTER_WITH_SUBCHAPTERS,
        encoding="utf-8",
    )
    (source_dir / "title30" / "c011" / "sc01" / "index.html").write_text(
        SAMPLE_SUBCHAPTER,
        encoding="utf-8",
    )
    (source_dir / "title30" / "c014" / "index.html").write_text(
        SAMPLE_REPEALED_CHAPTER,
        encoding="utf-8",
    )
    return source_dir


def test_parse_delaware_code_index_skips_constitution_and_extracts_titles():
    titles = parse_delaware_code_index(SAMPLE_DELAWARE_INDEX)

    assert [title.title for title in titles] == ["1", "30"]
    assert titles[1].heading == "State Taxes"
    assert titles[1].relative_path == "title30/index.html"


def test_parse_delaware_title_html_extracts_parts_and_alpha_chapters():
    parsed = parse_delaware_title_html(
        SAMPLE_DELAWARE_TITLE,
        title="30",
        current_relative_path="title30/index.html",
    )

    assert [part.source_id for part in parsed.parts] == ["part-ii"]
    assert parsed.parts[0].heading == "Income, Inheritance and Estate Taxes"
    assert [chapter.chapter for chapter in parsed.chapters] == ["11", "14", "20A"]
    assert parsed.chapters[-1].citation_path == "us-de/statute/30/20A"
    assert parsed.chapters[0].parent_citation_path == "us-de/statute/30/part-ii"


def test_parse_delaware_chapter_html_extracts_sections_history_refs_and_status():
    parsed = parse_delaware_chapter_html(
        SAMPLE_SUBCHAPTER,
        title="30",
        chapter="11",
        current_relative_path="title30/c011/sc01/index.html",
        source_url="https://www.delcode.delaware.gov/title30/c011/sc01/index.html",
        source_path="sources/us-de/statute/test/delaware-code-html/title30/c011/sc01/index.html",
        sha256="abc123",
        parent_citation_path="us-de/statute/30/11/subchapter-i",
        subchapter="subchapter-i",
    )

    assert [section.source_id for section in parsed.sections] == ["1101", "1102"]
    first = parsed.sections[0]
    assert first.citation_path == "us-de/statute/30/1101"
    assert first.parent_citation_path == "us-de/statute/30/11/subchapter-i"
    assert first.heading == "Meaning of terms"
    assert first.body is not None
    assert "Any term used" in first.body
    assert first.references_to == (
        "us-de/statute/30/1105",
        "us-de/statute/30/1102",
    )
    assert first.source_history == (
        "30 Del. C. 1953, § 1101;",
        "57 Del. Laws, c. 737, § 1",
        ";",
    )
    assert parsed.sections[1].status == "future_or_conditional"
    assert parsed.sections[1].notes == (
        "Effective upon meeting the contingency in 85 Del. Laws, c. 1, § 2",
    )


def test_parse_delaware_chapter_html_disambiguates_repeated_effective_sections():
    html = """<!doctype html><html><body>
    <div class="Section"><div class="SectionHead" id="801">§ 801. Rule [Effective through December 31, 2026].</div><p>Old rule.</p></div>
    <div class="Section"><div class="SectionHead" id="801">§ 801. Rule [Effective January 1, 2027].</div><p>New rule.</p></div>
    </body></html>"""

    parsed = parse_delaware_chapter_html(
        html,
        title="1",
        chapter="8",
        current_relative_path="title1/c008/index.html",
    )

    assert [section.source_id for section in parsed.sections] == [
        "801",
        "801@effective-january-1-2027",
    ]
    assert parsed.sections[1].canonical_citation_path == "us-de/statute/1/801"


def test_extract_delaware_code_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = _write_delaware_fixture_tree(tmp_path)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_delaware_code(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        source_as_of="2026-03-30",
        expression_date="2026-03-30",
        only_title="30",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 5
    assert report.section_count == 3
    assert report.provisions_written == 8
    assert report.provisions_path.name == "2026-05-05-us-de-title-30.jsonl"

    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[0].source_format == "delaware-code-html"
    assert [record.citation_path for record in records] == [
        "us-de/statute/30",
        "us-de/statute/30/part-ii",
        "us-de/statute/30/11",
        "us-de/statute/30/11/subchapter-i",
        "us-de/statute/30/1101",
        "us-de/statute/30/1102",
        "us-de/statute/30/14",
        "us-de/statute/30/1401-1409",
    ]
    assert records[4].id == records[4].id
    assert records[4].parent_id == records[3].id
    assert records[5].metadata is not None
    assert records[5].metadata["status"] == "future_or_conditional"
    assert records[-1].metadata is not None
    assert records[-1].metadata["status"] == "repealed"


def test_extract_delaware_code_cli_local_source(tmp_path, capsys):
    source_dir = _write_delaware_fixture_tree(tmp_path)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-delaware-code",
            "--base",
            str(base),
            "--version",
            "2026-05-05",
            "--source-dir",
            str(source_dir),
            "--only-title",
            "30",
            "--source-as-of",
            "2026-03-30",
            "--expression-date",
            "2026-03-30",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["adapter"] == "delaware-code"
    assert payload["coverage_complete"] is True
    assert payload["provisions_written"] == 8


def test_delaware_fetcher_uses_source_cache_and_live_download(tmp_path, monkeypatch):
    missing_source = delaware_adapter._DelawareFetcher(
        base_url="https://example.test/code",
        source_dir=tmp_path / "missing-source",
        download_dir=None,
    )
    with pytest.raises(FileNotFoundError):
        missing_source.fetch("title1/index.html")

    download_dir = tmp_path / "download"
    cached_path = download_dir / "title1" / "index.html"
    cached_path.parent.mkdir(parents=True)
    cached_path.write_bytes(b"cached")
    cached_fetcher = delaware_adapter._DelawareFetcher(
        base_url="https://example.test/code",
        source_dir=None,
        download_dir=download_dir,
    )

    assert cached_fetcher.fetch("title1/index.html").data == b"cached"

    def fake_download(url: str) -> tuple[bytes, str]:
        return b"live", f"{url}?resolved=1"

    monkeypatch.setattr(delaware_adapter, "_download_delaware_page", fake_download)
    live_page = cached_fetcher.fetch("title2/index.html")

    assert live_page.data == b"live"
    assert live_page.source_url.endswith("?resolved=1")
    assert (download_dir / "title2" / "index.html").read_bytes() == b"live"


def test_delaware_helpers_cover_edge_metadata_and_filters():
    part = delaware_adapter.DelawarePartLink(
        title="30",
        source_id="part-iii",
        display_number="III",
        heading=None,
        ordinal=2,
    )
    chapter = delaware_adapter.DelawareChapterLink(
        title="30",
        chapter="1",
        heading="General",
        relative_path="title30/c001/index.html",
        source_url="https://example.test/title30/c001/index.html",
        ordinal=0,
    )
    provision = delaware_adapter.DelawareCodeProvision(
        kind="section",
        title="30",
        source_id="101@variant-2",
        display_number="101",
        heading="Rule [Effective later]",
        body="Transferred and reserved text.",
        parent_citation_path=chapter.parent_citation_path,
        level=2,
        ordinal=1,
        source_url=chapter.source_url,
        source_relative_path=chapter.relative_path,
        source_path="sources/us-de/statute/test/title30/c001/index.html",
        sha256="abc",
        chapter="1",
        part=part.source_id,
        subchapter="subchapter-i",
        references_to=("us-de/statute/30/102",),
        source_history=("70 Del. Laws, c. 1, § 1",),
        notes=("Effective later",),
        status="transferred",
        canonical_citation_path="us-de/statute/30/101",
        variant="variant-2",
    )

    assert part.citation_path == "us-de/statute/30/part-iii"
    assert chapter.parent_citation_path == "us-de/statute/30"
    assert delaware_adapter._part_from_h3s("30", [], ordinal=0) is None
    assert delaware_adapter._part_from_h3s("30", ["Subtitle I"], ordinal=0) is None

    items = []
    records = []
    seen = set()
    assert delaware_adapter._append_provision(
        items,
        records,
        provision,
        version="test",
        source_as_of="2026-05-06",
        expression_date="2026-05-06",
        seen=seen,
    )
    assert not delaware_adapter._append_provision(
        items,
        records,
        provision,
        version="test",
        source_as_of="2026-05-06",
        expression_date="2026-05-06",
        seen=seen,
    )
    assert records[0].metadata["variant"] == "variant-2"
    assert records[0].identifiers["delaware:variant"] == "variant-2"

    assert delaware_adapter._heading_notes(None) == ()
    assert delaware_adapter._status("Expired", None, ()) == "expired"
    assert delaware_adapter._status("Transferred", None, ()) == "transferred"
    assert delaware_adapter._status("Redesignated", None, ()) == "redesignated"
    assert delaware_adapter._status("Reserved", None, ()) == "reserved"
    assert delaware_adapter._status("Rule", "Effective through 2026", ()) == "effective_until"
    assert delaware_adapter._variant_for_section("Rule [Plain duplicate]", 2) == "variant-2"
    assert delaware_adapter._normalize_chapter("00ab") == "00AB"
    assert delaware_adapter._title_filter(None) is None
    assert delaware_adapter._chapter_filter(None) is None
    assert delaware_adapter._delaware_run_id(
        "2026-05-06",
        title_filter=None,
        chapter_filter=None,
        limit=None,
    ) == "2026-05-06"
    assert delaware_adapter._delaware_run_id(
        "2026-05-06",
        title_filter="30",
        chapter_filter="1",
        limit=5,
    ) == "2026-05-06-us-de-title-30-chapter-1-limit-5"
    with pytest.raises(ValueError, match="invalid Delaware title filter"):
        delaware_adapter._title_filter("title x")
    with pytest.raises(ValueError, match="invalid Delaware chapter filter"):
        delaware_adapter._chapter_filter("chapter x")


def test_delaware_parser_fallbacks_and_reference_edges():
    fallback_title = """<html><body>
    <div id="content" class="container-home">
      <div class="title-links"><a href="c001/index.html">Chapter 1. General Rules</a></div>
    </div></body></html>"""
    parsed = parse_delaware_title_html(
        fallback_title,
        title="30",
        current_relative_path="title30/index.html",
    )

    assert parsed.chapters[0].chapter == "1"
    assert delaware_adapter._chapter_link_from_anchor(
        BeautifulSoup('<a href="not-a-chapter.html">Nope</a>', "lxml").a,
        title="30",
        current_relative_path="title30/index.html",
        base_url="https://example.test/",
        ordinal=0,
        part_source_id=None,
    ) is None
    assert delaware_adapter._subchapter_link_from_anchor(
        BeautifulSoup('<a href="not-a-subchapter.html">Nope</a>', "lxml").a,
        title="30",
        chapter="1",
        current_relative_path="title30/c001/index.html",
        base_url="https://example.test/",
        ordinal=0,
    ) is None

    soup = BeautifulSoup(
        """<div class="Section">
        <div class="SectionHead">§ 101. Rule.</div>
        stray history
        <br/>
        <span>Extra source text</span>
        </div>""",
        "lxml",
    )
    section = soup.find("div", class_="Section")

    assert delaware_adapter._section_history(section) == ("stray history", "Extra source text")
    body_lines, refs = delaware_adapter._section_body_and_references(
        BeautifulSoup("<div><p> </p><p>See § 103</p></div>", "lxml").div,
        title="30",
    )
    assert body_lines == ["See § 103"]
    assert refs == ("us-de/statute/30/103",)
    assert delaware_adapter._reference_from_href("title30/c001/index.html", current_title="30") is None
    assert delaware_adapter._reference_from_href("title30/c001/index.html#not valid!", current_title="30") is None
    assert delaware_adapter._delaware_text_references("26 U.S.C. § 1", current_title="30") == ()
    assert delaware_adapter._delaware_text_references("26 U.S.C. § 1 of Title 5", current_title="30") == ()
    assert delaware_adapter._delaware_text_references("See § 101 of Title 30", current_title="1") == (
        "us-de/statute/30/101",
        "us-de/statute/1/101",
    )
    assert delaware_adapter._delaware_text_references("See § 102", current_title="30") == (
        "us-de/statute/30/102",
    )
    with pytest.raises(ValueError, match="missing SectionHead"):
        delaware_adapter._section_from_tag(
            BeautifulSoup('<div class="Section"></div>', "lxml").div,
            title="30",
            chapter="1",
            subchapter=None,
            parent_citation_path="us-de/statute/30/1",
            level=2,
            ordinal=0,
            current_relative_path="title30/c001/index.html",
            base_url="https://example.test/",
            source_url="https://example.test/title30/c001/index.html",
            source_path="sources/test",
            sha256="abc",
            occurrence_by_section={},
        )
    with pytest.raises(ValueError, match="cannot parse Delaware section id"):
        delaware_adapter._section_id_from_heading("No section here")
    assert delaware_adapter._section_id_from_heading("§ 101 Rule") == "101"

    assert delaware_adapter._title_heading("<h2>Fallback Title</h2>") == "Fallback Title"
    assert (
        delaware_adapter._title_heading(
            '<div id="TitleHead"><h4>Official Title Heading</h4></div>'
        )
        == "Official Title Heading"
    )
    assert delaware_adapter._title_heading("<html></html>") is None
    candidates = delaware_adapter._download_candidates("https://www.example.test/title1/index.html")
    assert candidates == (
        "https://www.example.test/title1/index.html",
        "https://www.example.test/title1/",
        "https://example.test/title1/index.html",
        "https://example.test/title1/",
    )
    assert delaware_adapter._download_candidates("https://example.test/title1/") == (
        "https://example.test/title1/",
        "https://example.test/title1/index.html",
        "https://www.example.test/title1/",
        "https://www.example.test/title1/index.html",
    )
    assert delaware_adapter._normalize_relative_path("") == "index.html"
    assert delaware_adapter._normalize_relative_path("title1/") == "title1/index.html"
    assert delaware_adapter._date_text(None, "fallback") == "fallback"


def test_delaware_extract_limit_and_missing_source_edges(tmp_path):
    source_dir = _write_delaware_fixture_tree(tmp_path)

    limited = extract_delaware_code(
        CorpusArtifactStore(tmp_path / "limited-corpus"),
        version="2026-05-05",
        source_dir=source_dir,
        only_title="30",
        limit=1,
    )
    assert limited.section_count == 1
    assert limited.provisions_written == 6

    with pytest.raises(ValueError, match="no Delaware Code titles selected"):
        extract_delaware_code(
            CorpusArtifactStore(tmp_path / "empty-title-corpus"),
            version="2026-05-05",
            source_dir=source_dir,
            only_title="99",
        )
    with pytest.raises(ValueError, match="no Delaware Code provisions extracted"):
        extract_delaware_code(
            CorpusArtifactStore(tmp_path / "empty-chapter-corpus"),
            version="2026-05-05",
            source_dir=source_dir,
            only_title="30",
            only_chapter="99",
        )

    missing_chapter_source = tmp_path / "missing-chapter-source"
    (missing_chapter_source / "title30").mkdir(parents=True)
    (missing_chapter_source / "index.html").write_text(SAMPLE_DELAWARE_INDEX, encoding="utf-8")
    (missing_chapter_source / "title30" / "index.html").write_text(SAMPLE_DELAWARE_TITLE, encoding="utf-8")
    missing_report = extract_delaware_code(
        CorpusArtifactStore(tmp_path / "missing-chapter-corpus"),
        version="2026-05-05",
        source_dir=missing_chapter_source,
        only_title="30",
        only_chapter="20A",
    )
    assert missing_report.skipped_source_count == 1
    assert "title30/c020a/index.html" in missing_report.errors[0]

    missing_subchapter_source = tmp_path / "missing-subchapter-source"
    (missing_subchapter_source / "title30" / "c011").mkdir(parents=True)
    (missing_subchapter_source / "index.html").write_text(SAMPLE_DELAWARE_INDEX, encoding="utf-8")
    (missing_subchapter_source / "title30" / "index.html").write_text(
        SAMPLE_DELAWARE_TITLE.replace(
            '<div class="title-links"><a href="../title30/c014/index.html">Chapter 14. Gift Tax [Repealed].</a></div>',
            "",
        ).replace(
            '<div class="title-links"><a href="../title30/c020a/index.html">Chapter 20A. Veterans’ Opportunity Credit</a></div>',
            "",
        ),
        encoding="utf-8",
    )
    (missing_subchapter_source / "title30" / "c011" / "index.html").write_text(
        SAMPLE_CHAPTER_WITH_SUBCHAPTERS,
        encoding="utf-8",
    )
    subchapter_report = extract_delaware_code(
        CorpusArtifactStore(tmp_path / "missing-subchapter-corpus"),
        version="2026-05-05",
        source_dir=missing_subchapter_source,
        only_title="30",
        only_chapter="11",
    )
    assert subchapter_report.skipped_source_count == 1
    assert "sc01/index.html" in subchapter_report.errors[0]


def test_delaware_title_parser_global_fallback():
    parsed = parse_delaware_title_html(
        """<html><body>
        <main id="content">
          <section class="title-links"><a href="c002/index.html">Chapter 2. Fallback Chapter</a></section>
        </main>
        </body></html>""",
        title="30",
        current_relative_path="title30/index.html",
    )

    assert [chapter.chapter for chapter in parsed.chapters] == ["2"]
