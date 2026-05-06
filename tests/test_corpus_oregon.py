from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import pytest
import requests

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters import oregon as oregon_adapter
from axiom_corpus.corpus.state_adapters.oregon import (
    OREGON_ORS_BASE_URL,
    extract_oregon_ors,
    parse_oregon_chapter_html,
    parse_oregon_ors_landing_html,
)

SAMPLE_OREGON_LANDING = """<!doctype html>
<html><body>
<tbody groupString="%3B%2303%20-%20Landlord-Tenant%2C%20Domestic%20Relations%2C%20Probate%20-%20Chapters%2090-130%3B%23"></tbody>
<tbody groupString="%3B%2303%20-%20Landlord-Tenant%2C%20Domestic%20Relations%2C%20Probate%20-%20Chapters%2090-13010.%20Property%20Rights%20and%20Transactions%20-%20Chapters%2090-105%3B%23"></tbody>
<tbody groupString="%3B%2302%20-%20Business%20Organizations%2C%20Commercial%20Code%20-%20Chapters%2056-88%3B%23"></tbody>
<tbody groupString="%3B%2302%20-%20Business%20Organizations%2C%20Commercial%20Code%20-%20Chapters%2056-888.%20Commercial%20Transactions%20-%20Chapters%2071-84%3B%23"></tbody>
</body></html>
"""

SAMPLE_OREGON_CHAPTER_90 = """<!doctype html>
<html><body>
<p>Chapter 90 — Residential Landlord and Tenant</p>
<p>2025 EDITION</p>
<p>RESIDENTIAL LANDLORD AND TENANT</p>
<p>PROPERTY RIGHTS AND TRANSACTIONS</p>
<p>GENERAL PROVISIONS</p>
<p>90.100     Definitions</p>
<p>90.105     Short title</p>
<p>LANDLORD RIGHTS AND OBLIGATIONS</p>
<p>90.303     Evaluation of applicant</p>
<p>90.304     Statement of reasons for denial; remedy</p>
<p>GENERAL PROVISIONS</p>
<p>90.100 Definitions. As used in ORS 90.100 to 90.465, "landlord" means the owner of a dwelling unit. See ORS 105.100. [2025 c.1 §1]</p>
<p>90.105 Short title. This chapter may be cited as the Residential Landlord and Tenant Act.</p>
<p>90.110 [Repealed by 2025 c.2 §3]</p>
<p>LANDLORD RIGHTS AND OBLIGATIONS</p>
<p>90.303 Evaluation of applicant. (1) A landlord may not consider a dismissed eviction action.</p>
<p>Note: The amendments to 90.303 by section 10, chapter 39, Oregon Laws 2021, become operative January 2, 2028. See section 12, chapter 39, Oregon Laws 2021. The text that is operative on and after January 2, 2028, including amendments by section 5, chapter 226, Oregon Laws 2025, is set forth for the user's convenience.</p>
<p>90.303. (1) When evaluating an applicant, a landlord may not consider an eviction action older than five years.</p>
<p>90.304 Statement of reasons for denial; remedy. A landlord shall provide a written statement.</p>
</body></html>
"""

SAMPLE_OREGON_CHAPTER_79A = """<!doctype html>
<html><body>
<p>Chapter 79A — Secured Transactions</p>
<p>2025 EDITION</p>
<p>SECURED TRANSACTIONS</p>
<p>COMMERCIAL TRANSACTIONS</p>
<p>(Effectiveness and Attachment)</p>
<p>79A.1010 UCC 9-101. Short title</p>
<p>(Effectiveness and Attachment)</p>
<p>79A.1010 UCC 9-101. Short title. This chapter may be cited as Uniform Commercial Code-Secured Transactions.</p>
</body></html>
"""

SAMPLE_OREGON_FORMER_CHAPTER = """<!doctype html>
<html><body>
<p>Chapter 4 (Former Provisions)</p>
<p>CIRCUIT COURT TERMS</p>
<p>4.010 [Renumbered 3.232]</p>
</body></html>
"""

SAMPLE_OREGON_NAMED_FORMER_CHAPTER = """<!doctype html>
<html><body>
<p>Chapter 11 (Former Provisions) — Forms of Actions and Suits</p>
<p>FORMS OF ACTIONS AND SUITS</p>
<p>11.010 [Repealed by 1979 c.284 §199]</p>
</body></html>
"""

SAMPLE_OREGON_NUMBER_ONLY_CHAPTER = """<!doctype html>
<html><body>
<p>Chapter 5</p>
<p>2025 EDITION</p>
<p>County Courts (Judicial Functions)</p>
<p>5.010 Who holds court. The county court may sit at the courthouse.</p>
</body></html>
"""

SAMPLE_OREGON_DASH_ONLY_CHAPTER = """<!doctype html>
<html><body>
<p>Chapter 237 —</p>
<p>PUBLIC EMPLOYEE RETIREMENT GENERALLY</p>
<p>2025 EDITION</p>
<p>237.350 Definitions for ORS 237.350 to 237.380. Text.</p>
</body></html>
"""

SAMPLE_OREGON_NO_SECTION_CHAPTER = """<!doctype html>
<html><body>
<p>Chapter 27 (Former Provisions)</p>
<p>Submitting Controversy Without Action or Suit</p>
<p>Note: 27.010, 27.020 and 27.030 repealed by 1981 c.898 §53.</p>
</body></html>
"""


def _write_source_dir(root: Path) -> Path:
    source_dir = root / "source"
    source_dir.mkdir()
    (source_dir / "ORS.aspx").write_text(SAMPLE_OREGON_LANDING, encoding="utf-8")
    (source_dir / "ors090.html").write_text(SAMPLE_OREGON_CHAPTER_90, encoding="utf-8")
    return source_dir


def test_parse_oregon_ors_landing_html_extracts_title_ranges():
    titles = parse_oregon_ors_landing_html(SAMPLE_OREGON_LANDING)

    assert [(title.number, title.start_chapter, title.end_chapter) for title in titles] == [
        ("10", "090", "105"),
        ("8", "071", "084"),
    ]
    assert titles[0].citation_path == "us-or/statute/title-10"
    assert titles[0].heading == "Property Rights And Transactions"


def test_candidate_chapter_tokens_probe_common_alpha_chapters_without_full_alphabet():
    candidates = tuple(oregon_adapter._candidate_chapter_tokens("071", "084"))

    assert "071" in candidates
    assert "079A" in candidates
    assert "079B" in candidates
    assert "079C" in candidates
    assert "079D" not in candidates


def test_parse_oregon_chapter_html_handles_repealed_notes_and_variants():
    parsed = parse_oregon_chapter_html(SAMPLE_OREGON_CHAPTER_90)

    assert parsed.chapter == "090"
    assert parsed.heading == "Residential Landlord and Tenant"
    assert parsed.source_year == 2025
    assert [provision.source_id for provision in parsed.provisions] == [
        "chapter-090/series-general-provisions",
        "90.100",
        "90.105",
        "90.110",
        "chapter-090/series-landlord-rights-and-obligations",
        "90.303",
        "90.303@operative-2028-01-02",
        "90.304",
    ]
    definitions = parsed.provisions[1]
    assert definitions.heading == "Definitions"
    assert definitions.body == (
        'As used in ORS 90.100 to 90.465, "landlord" means the owner of a '
        "dwelling unit. See ORS 105.100."
    )
    assert definitions.source_history == ("[2025 c.1 §1]",)
    assert definitions.references_to == (
        "us-or/statute/90.465",
        "us-or/statute/105.100",
    )
    assert parsed.provisions[3].status == "repealed"

    future = parsed.provisions[6]
    assert future.status == "future_or_conditional"
    assert future.canonical_citation_path == "us-or/statute/90.303"
    assert future.heading == "Evaluation of applicant"
    assert future.body == (
        "(1) When evaluating an applicant, a landlord may not consider an eviction "
        "action older than five years."
    )
    assert future.effective_note is not None
    assert "January 2, 2028" in future.effective_note


def test_parse_oregon_chapter_html_preserves_ucc_headings_in_alpha_chapters():
    parsed = parse_oregon_chapter_html(SAMPLE_OREGON_CHAPTER_79A)

    assert parsed.chapter == "079A"
    assert parsed.provisions[0].kind == "series"
    assert parsed.provisions[1].source_id == "79A.1010"
    assert parsed.provisions[1].heading == "UCC 9-101. Short title"
    assert parsed.provisions[1].body == (
        "This chapter may be cited as Uniform Commercial Code-Secured Transactions."
    )


def test_parse_oregon_chapter_html_handles_former_provisions_chapters():
    parsed = parse_oregon_chapter_html(SAMPLE_OREGON_FORMER_CHAPTER)

    assert parsed.chapter == "004"
    assert parsed.heading == "Former Provisions"
    assert parsed.provisions[0].source_id == "chapter-004/series-circuit-court-terms"
    assert parsed.provisions[1].source_id == "4.010"
    assert parsed.provisions[1].status == "renumbered"


def test_parse_oregon_chapter_html_handles_named_former_provisions_chapters():
    parsed = parse_oregon_chapter_html(SAMPLE_OREGON_NAMED_FORMER_CHAPTER)

    assert parsed.chapter == "011"
    assert parsed.heading == "Forms of Actions and Suits"
    assert parsed.provisions[1].source_id == "11.010"
    assert parsed.provisions[1].status == "repealed"
    assert parsed.provisions[1].body == "[Repealed by 1979 c.284 §199]"


def test_parse_oregon_chapter_html_handles_number_only_chapter_heading():
    parsed = parse_oregon_chapter_html(SAMPLE_OREGON_NUMBER_ONLY_CHAPTER)

    assert parsed.chapter == "005"
    assert parsed.heading == "County Courts (Judicial Functions)"
    assert parsed.provisions[0].source_id == "5.010"


def test_parse_oregon_chapter_html_handles_dash_only_chapter_heading():
    parsed = parse_oregon_chapter_html(SAMPLE_OREGON_DASH_ONLY_CHAPTER)

    assert parsed.chapter == "237"
    assert parsed.heading == "PUBLIC EMPLOYEE RETIREMENT GENERALLY"
    assert parsed.provisions[0].source_id == "237.350"


def test_parse_oregon_chapter_html_allows_chapters_with_no_active_sections():
    parsed = parse_oregon_chapter_html(SAMPLE_OREGON_NO_SECTION_CHAPTER)

    assert parsed.chapter == "027"
    assert parsed.heading == "Former Provisions"
    assert parsed.provisions == ()


def test_extract_oregon_ors_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = _write_source_dir(tmp_path)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_oregon_ors(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        source_as_of="2025-11-19",
        expression_date="2025-11-19",
        only_chapter="90",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 4
    assert report.section_count == 6
    assert report.provisions_written == 10
    assert report.provisions_path.name == "2026-05-05-us-or-chapter-090.jsonl"
    assert [path.name for path in report.source_paths] == ["ORS.aspx", "ors090.html"]

    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert [item.citation_path for item in inventory] == [
        "us-or/statute/title-10",
        "us-or/statute/chapter-090",
        "us-or/statute/chapter-090/series-general-provisions",
        "us-or/statute/90.100",
        "us-or/statute/90.105",
        "us-or/statute/90.110",
        "us-or/statute/chapter-090/series-landlord-rights-and-obligations",
        "us-or/statute/90.303",
        "us-or/statute/90.303@operative-2028-01-02",
        "us-or/statute/90.304",
    ]
    assert records[0].legal_identifier == "ORS Title 10"
    assert records[1].parent_citation_path == "us-or/statute/title-10"
    assert records[3].source_url == urljoin(OREGON_ORS_BASE_URL, "ors/ors090.html")
    assert records[8].metadata is not None
    assert records[8].metadata["status"] == "future_or_conditional"
    assert records[8].metadata["canonical_citation_path"] == "us-or/statute/90.303"


def test_extract_oregon_ors_filters_title_and_reports_invalid_inputs(tmp_path):
    source_dir = _write_source_dir(tmp_path)
    store = CorpusArtifactStore(tmp_path / "corpus")

    with pytest.raises(ValueError, match="no Oregon ORS chapters selected"):
        extract_oregon_ors(store, version="2026-05-05", source_dir=source_dir, only_title="8")
    with pytest.raises(ValueError, match="invalid Oregon chapter filter"):
        extract_oregon_ors(store, version="2026-05-05", source_dir=source_dir, only_chapter="x")
    with pytest.raises(ValueError, match="invalid Oregon title filter"):
        extract_oregon_ors(store, version="2026-05-05", source_dir=source_dir, only_title="title x")


def test_extract_oregon_ors_real_source_smoke(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    try:
        report = extract_oregon_ors(
            store,
            version="2026-05-05",
            source_as_of="2026-05-05",
            expression_date="2026-05-05",
            only_chapter="316",
            download_dir=Path("/tmp/axiom-state-cache/us-or"),
        )
    except ValueError as exc:
        if "failed to fetch Oregon source page" in str(exc):
            pytest.skip(str(exc))
        raise

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.section_count > 100
    assert report.provisions_written > report.section_count
    assert any(path.name == "ors316.html" for path in report.source_paths)


def test_oregon_fetcher_uses_source_cache_and_live_download(tmp_path, monkeypatch):
    missing_fetcher = oregon_adapter._OregonFetcher(
        base_url=OREGON_ORS_BASE_URL,
        source_dir=tmp_path / "missing",
        download_dir=None,
    )
    with pytest.raises(ValueError, match="does not exist"):
        missing_fetcher.fetch_chapter("90")

    download_dir = tmp_path / "download"
    cached = download_dir / "oregon-ors-html" / "ors090.html"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"cached")
    cached_fetcher = oregon_adapter._OregonFetcher(
        base_url=OREGON_ORS_BASE_URL,
        source_dir=None,
        download_dir=download_dir,
    )

    assert cached_fetcher.fetch_chapter("90").data == b"cached"
    monkeypatch.setattr(oregon_adapter, "_download_oregon_page", lambda url: b"live")
    live_page = cached_fetcher.fetch_chapter("91")
    assert live_page.data == b"live"
    assert (download_dir / "oregon-ors-html" / "ors091.html").read_bytes() == b"live"


def test_oregon_live_discovery_and_helper_edges(monkeypatch, tmp_path):
    title = oregon_adapter.OregonOrsTitle(
        number="1",
        heading="Courts",
        start_chapter="001",
        end_chapter="001B",
        ordinal=0,
    )

    class FakeFetcher:
        def fetch_chapter(self, token: str):
            if token not in {"001", "001A"}:
                raise ValueError(token)
            return object()

    discovered = oregon_adapter._discover_live_chapters((title,), fetcher=FakeFetcher(), workers=1)
    assert [chapter.chapter for chapter in discovered] == ["001", "001A"]
    assert oregon_adapter._live_chapter_exists(FakeFetcher(), "001") is True
    assert oregon_adapter._live_chapter_exists(FakeFetcher(), "001B") is False
    with pytest.raises(ValueError, match="landing page"):
        oregon_adapter._discover_live_chapters((), fetcher=FakeFetcher(), workers=1)

    assert oregon_adapter._selected_chapters(
        source_root=None,
        titles=(title,),
        title_filter="2",
        chapter_filter="001",
        fetcher=FakeFetcher(),
        workers=1,
    ) == ()
    assert oregon_adapter._candidate_alpha_suffixes(
        2,
        start_number=1,
        start_suffix="A",
        end_number=2,
        end_suffix="",
    ) == ()
    assert oregon_adapter._title_for_chapter("999", (title,)) is None

    source_dir = tmp_path / "source"
    source_dir.mkdir()
    assert oregon_adapter._source_dir_file(source_dir, "missing.html") is None
    assert oregon_adapter.OregonOrsChapter(chapter="999").parent_citation_path is None


def test_oregon_landing_fallback_and_parser_edge_cases():
    fallback = """<html><body>
    <a data-ors-title="" data-ors-title-heading="Missing" data-ors-start-chapter="1"></a>
    <a data-ors-title="7" data-ors-title-heading="Public Facilities" data-ors-start-chapter="200" data-ors-end-chapter=""></a>
    <a data-ors-title="7" data-ors-title-heading="Duplicate" data-ors-start-chapter="201"></a>
    </body></html>"""
    titles = parse_oregon_ors_landing_html(fallback)

    assert [(title.number, title.start_chapter, title.end_chapter) for title in titles] == [
        ("7", "200", "200")
    ]
    assert parse_oregon_chapter_html(
        """<html><body>
        <p>Chapter 90 — Residential Landlord and Tenant</p>
        <p>90.100 Definitions</p>
        <p>90.100 Definitions. First text.</p>
        <p>90.100 Definitions. Duplicate text.</p>
        </body></html>"""
    ).provisions[1].source_id == "90.100@variant-2"

    assert oregon_adapter._parse_chapter_heading(["Chapter 12 —"])[1] is None
    with pytest.raises(ValueError, match="missing a chapter heading"):
        oregon_adapter._parse_chapter_heading(["No chapter"])
    assert oregon_adapter._next_heading_text(["2025 EDITION", ""]) is None
    assert oregon_adapter._section_start_match("91.010 Wrong chapter.", "090") is None
    assert not oregon_adapter._valid_title_number("title")
    assert not oregon_adapter._valid_title_number("99")
    assert oregon_adapter._is_series_heading("90.100 Definitions") is False
    assert oregon_adapter._body_after_heading("", "Heading") is None
    assert oregon_adapter._body_after_heading("Heading.", "Heading") is None
    assert oregon_adapter._body_after_heading("Plain heading", None) is None
    assert oregon_adapter._clean_section_rest(None) == ""
    assert oregon_adapter._section_status("renumbered 1.010") == "renumbered"
    assert oregon_adapter._variant_from_note("not a future note") is None
    assert (
        oregon_adapter._variant_from_note(
            "Note: The amendments to 90.303 by section 1, chapter 1, Oregon Laws 2025, become operative Bad Date. The text that is operative on and after Bad Date is set forth for the user's convenience."
        )
        is None
    )
    assert (
        oregon_adapter._variant_from_note(
            "Note: The amendments to 90.303 by section 1, chapter 1, Oregon Laws 2025, become operative February 30, 2028. The text that is operative on and after February 30, 2028, is set forth for the user's convenience."
        )
        is None
    )
    assert oregon_adapter._consume_variant(oregon_adapter._VariantCue("1.010", "s", "n"), "2.010") is None
    assert oregon_adapter._references_to("ORS 90.100 and ORS 90.105", "90.100") == (
        "us-or/statute/90.105",
    )
    assert oregon_adapter._oregon_run_id(
        "2026-05-06",
        title_filter="10",
        chapter_filter="090",
        limit=5,
    ) == "2026-05-06-us-or-title-10-us-or-chapter-090-limit-5"
    assert oregon_adapter._title_filter(None) is None
    with pytest.raises(ValueError, match="invalid Oregon chapter"):
        oregon_adapter._chapter_sort_key("bad")
    assert oregon_adapter._date_text(None, "fallback") == "fallback"
    assert oregon_adapter._date_text(date(2026, 5, 6), "fallback") == "2026-05-06"


def test_oregon_download_page_success_and_retry_failure(monkeypatch):
    class OkResponse:
        content = b"ok"

        def raise_for_status(self):
            return None

    assert_calls = {"count": 0}

    def ok_get(*args, **kwargs):
        assert_calls["count"] += 1
        return OkResponse()

    monkeypatch.setattr(oregon_adapter.requests, "get", ok_get)
    assert oregon_adapter._download_oregon_page("https://example.test/ors090.html") == b"ok"
    assert assert_calls["count"] == 1

    def bad_get(*args, **kwargs):
        raise requests.RequestException("boom")

    monkeypatch.setattr(oregon_adapter.requests, "get", bad_get)
    monkeypatch.setattr(oregon_adapter.time, "sleep", lambda seconds: None)
    with pytest.raises(ValueError, match="failed to fetch Oregon source page"):
        oregon_adapter._download_oregon_page("https://example.test/ors091.html")


def test_oregon_extract_without_landing_and_error_edges(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "ors090.html").write_text(SAMPLE_OREGON_CHAPTER_90, encoding="utf-8")
    report = extract_oregon_ors(
        CorpusArtifactStore(tmp_path / "no-landing-corpus"),
        version="2026-05-05",
        source_dir=source_dir,
        only_chapter="90",
    )

    assert report.title_count == 0
    assert len(report.source_paths) == 1
    assert report.provisions_written == 9

    with pytest.raises(ValueError, match="no Oregon ORS provisions extracted"):
        extract_oregon_ors(
            CorpusArtifactStore(tmp_path / "limit-zero-corpus"),
            version="2026-05-05",
            source_dir=source_dir,
            only_chapter="90",
            limit=0,
        )

    missing_chapter_root = tmp_path / "missing-chapter"
    missing_chapter_root.mkdir()
    missing_chapter_dir = _write_source_dir(missing_chapter_root)
    with pytest.raises(ValueError, match="no Oregon ORS provisions extracted"):
        extract_oregon_ors(
            CorpusArtifactStore(tmp_path / "missing-chapter-corpus"),
            version="2026-05-05",
            source_dir=missing_chapter_dir,
            only_chapter="91",
        )

    class FakeFetcher:
        def fetch_chapter(self, token: str):
            if token != "001":
                raise ValueError(token)
            return object()

    title = oregon_adapter.OregonOrsTitle(
        number="1",
        heading="Courts",
        start_chapter="001",
        end_chapter="001A",
        ordinal=0,
    )
    selected = oregon_adapter._selected_chapters(
        source_root=None,
        titles=(title,),
        title_filter=None,
        chapter_filter=None,
        fetcher=FakeFetcher(),
        workers=1,
    )
    assert [chapter.chapter for chapter in selected] == ["001"]
