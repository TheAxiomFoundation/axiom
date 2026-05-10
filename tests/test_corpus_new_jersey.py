from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.new_jersey import (
    NEW_JERSEY_STATUTES_TEXT_MEMBER,
    NEW_JERSEY_STATUTES_TEXT_SOURCE_FORMAT,
    NewJerseySource,
    extract_new_jersey_statutes,
    parse_new_jersey_statutes_text,
)

SAMPLE_STATUTES_TEXT = """
NEW JERSEY GENERAL AND PERMANENT STATUTES
(UPDATED THROUGH P.L.2025, c.271)

TITLE 1         ACTS, LAWS AND STATUTES

1:1-1.  General rules of construction
    Words and phrases shall be read with their context and may refer to 1:1-2.

     L.1937, c.188, s.1.

1:1-2  Words and phrases defined.
    Unless otherwise provided, the following words and phrases have the
    meanings herein given to them.

     amended 1948, c.4.

TITLE 52        STATE GOVERNMENT, DEPARTMENTS AND OFFICERS

52:9H 34  Findings, declarations.
    The Legislature finds and declares that public policy benefits from
    economic analyses.

    L.1993, c.149, s.1.

APPENDIX A     EMERGENCY AND TEMPORARY ACTS

App.A:10-1.  Authority to accept grants
    Agencies are authorized to accept grants.

     L.1942, c.226, s.1.
"""

SAMPLE_SOURCE = NewJerseySource(
    source_url="https://pub.njleg.state.nj.us/Statutes/STATUTES-TEXT.zip",
    source_path="sources/us-nj/statute/test/STATUTES.TXT",
    source_format=NEW_JERSEY_STATUTES_TEXT_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_new_jersey_statutes_text():
    provisions = parse_new_jersey_statutes_text(
        SAMPLE_STATUTES_TEXT,
        source=SAMPLE_SOURCE,
    )

    assert [provision.citation_path for provision in provisions] == [
        "us-nj/statute/title-1",
        "us-nj/statute/chapter-1:1",
        "us-nj/statute/1:1-1",
        "us-nj/statute/1:1-2",
        "us-nj/statute/title-52",
        "us-nj/statute/chapter-52:9h",
        "us-nj/statute/52:9h-34",
        "us-nj/statute/title-app-a",
        "us-nj/statute/chapter-app-a:10",
        "us-nj/statute/app.a:10-1",
    ]
    assert provisions[2].heading == "General rules of construction"
    assert provisions[2].references_to == ("us-nj/statute/1:1-2",)
    assert provisions[2].source_history == ("L.1937, c.188, s.1.",)
    assert provisions[6].citation_label == "52:9H-34"
    assert provisions[8].legal_identifier == "N.J. Stat. Title Appendix A, ch. 10"


def test_extract_new_jersey_statutes_from_source_dir_writes_complete_artifacts(
    tmp_path,
):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / NEW_JERSEY_STATUTES_TEXT_MEMBER).write_text(
        SAMPLE_STATUTES_TEXT,
        encoding="cp1252",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_new_jersey_statutes(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2026-05-09",
        expression_date="2026-05-09",
    )

    assert report.coverage.complete is True
    assert report.title_count == 3
    assert report.container_count == 3
    assert report.section_count == 4
    assert report.provisions_written == 10
    assert len(load_source_inventory(report.inventory_path)) == 10
    records = load_provisions(report.provisions_path)
    assert records[2].citation_path == "us-nj/statute/1:1-1"
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == ["us-nj/statute/1:1-2"]
