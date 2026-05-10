from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.louisiana import (
    LOUISIANA_LAWPRINT_SOURCE_FORMAT,
    LOUISIANA_ROOT_TOC_SOURCE_FORMAT,
    LOUISIANA_TITLE_TOC_SOURCE_FORMAT,
    _RecordedSource,
    extract_louisiana_revised_statutes,
    parse_louisiana_root,
    parse_louisiana_section_page,
    parse_louisiana_title_page,
)

SAMPLE_ROOT_HTML = """
<html><body>
<table>
  <tr>
    <td><a href="Laws_Toc.aspx?folder=88&amp;level=Parent">TITLE 14</a></td>
    <td><a href="Laws_Toc.aspx?folder=88&amp;level=Parent">Criminal Law</a></td>
  </tr>
</table>
</body></html>
"""

SAMPLE_TITLE_HTML = """
<html><body>
<div id="ctl00_ctl00_PageBody_PageContent_PanelResults2">
<table>
  <tr>
    <td><a href="Law.aspx?d=78223">RS 14</a></td>
    <td><a href="Law.aspx?d=78223">TITLE 14. CRIMINAL LAW</a></td>
  </tr>
  <tr>
    <td><a href="Law.aspx?d=78224">RS 14:1</a></td>
    <td><a href="Law.aspx?d=78224">Method of citation</a></td>
  </tr>
  <tr>
    <td><a href="Law.aspx?d=78337">RS 14:2</a></td>
    <td><a href="Law.aspx?d=78337">Definitions</a></td>
  </tr>
</table>
</div>
</body></html>
"""

SAMPLE_SECTION_HTML = """
<html><body>
<span id="LabelName">RS 14:2</span>
<span id="LabelDocument">
  <p style="text-align: center">TITLE 14 CRIMINAL LAW</p>
  <p style="text-align: center">CHAPTER 1. CRIMINAL CODE</p>
  <p style="text-align:left; text-indent: -0.5in">&sect;2. Definitions</p>
  <p style="text-align:left">A. In this Code the terms enumerated shall have the designated meanings.</p>
  <p style="text-align:left">Domestic abuse battery punishable under R.S. 14:35.3(L).</p>
  <p style="text-align:left">Distribution of fentanyl punishable under R.S. 40:967(B)(4)(f).</p>
  <p style="text-align:left">Amended by Acts 1962, No. 68, &sect;1; Acts 2024, No. 523, &sect;1.</p>
</span>
</body></html>
"""

SAMPLE_SECTION_1_HTML = """
<html><body>
<span id="LabelName">RS 14:1</span>
<span id="LabelDocument">
  <p style="text-align: center">TITLE 14 CRIMINAL LAW</p>
  <p style="text-align:left; text-indent: -0.5in">&sect;1. Method of citation</p>
  <p style="text-align:left">This Chapter shall be known as the Louisiana Criminal Code.</p>
</span>
</body></html>
"""

SAMPLE_EMPTY_RESERVED_HTML = """
<html><body>
<span id="LabelName">RS 46:2598</span>
<span id="LabelDocument"><div id="WPMainDoc"></div></span>
</body></html>
"""

SAMPLE_ROOT_SOURCE = _RecordedSource(
    source_url="https://www.legis.la.gov/Legis/Laws_Toc.aspx?folder=75&level=Parent",
    source_path="sources/us-la/statute/test/root.html",
    source_format=LOUISIANA_ROOT_TOC_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_louisiana_indexes_and_section_page():
    titles = parse_louisiana_root(SAMPLE_ROOT_HTML, source=SAMPLE_ROOT_SOURCE)
    assert [title.title for title in titles] == ["14"]
    assert titles[0].folder == "88"
    assert titles[0].heading == "Criminal Law"

    title_source = _RecordedSource(
        source_url=titles[0].source_url,
        source_path="sources/us-la/statute/test/title-14.html",
        source_format=LOUISIANA_TITLE_TOC_SOURCE_FORMAT,
        sha256="def",
    )
    title, listings = parse_louisiana_title_page(
        SAMPLE_TITLE_HTML,
        title_listing=titles[0],
        source=title_source,
    )
    assert title.citation_path == "us-la/statute/title-14"
    assert [listing.section_label for listing in listings] == ["14:1", "14:2"]
    assert listings[1].source_url.endswith("LawPrint.aspx?d=78337")

    section_source = _RecordedSource(
        source_url=listings[1].source_url,
        source_path="sources/us-la/statute/test/78337.html",
        source_format=LOUISIANA_LAWPRINT_SOURCE_FORMAT,
        sha256="ghi",
    )
    section = parse_louisiana_section_page(
        SAMPLE_SECTION_HTML,
        listing=listings[1],
        source=section_source,
    )
    assert section.section_label == "14:2"
    assert section.heading == "Definitions"
    assert section.hierarchy == ("TITLE 14 CRIMINAL LAW", "CHAPTER 1. CRIMINAL CODE")
    assert "terms enumerated" in (section.body or "")
    assert section.source_history == (
        "Amended by Acts 1962, No. 68, \u00a71; Acts 2024, No. 523, \u00a71.",
    )
    assert section.references_to == (
        "us-la/statute/14:35.3",
        "us-la/statute/40:967",
    )

    reserved = parse_louisiana_section_page(
        SAMPLE_EMPTY_RESERVED_HTML,
        listing=listings[1].__class__(
            title="46",
            section="2598",
            heading="[Reserved.]",
            document_id="1239773",
            source_url="https://www.legis.la.gov/Legis/LawPrint.aspx?d=1239773",
            ordinal=1,
        ),
        source=section_source,
    )
    assert reserved.heading == "[Reserved.]"
    assert reserved.body is None
    assert reserved.status == "reserved"


def test_extract_louisiana_revised_statutes_from_source_dir_writes_complete_artifacts(
    tmp_path,
):
    source_dir = tmp_path / "source"
    (source_dir / LOUISIANA_ROOT_TOC_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / LOUISIANA_TITLE_TOC_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / LOUISIANA_LAWPRINT_SOURCE_FORMAT / "title-14").mkdir(parents=True)
    (source_dir / LOUISIANA_ROOT_TOC_SOURCE_FORMAT / "folder-75.html").write_text(
        SAMPLE_ROOT_HTML,
        encoding="utf-8",
    )
    (source_dir / LOUISIANA_TITLE_TOC_SOURCE_FORMAT / "title-14.html").write_text(
        SAMPLE_TITLE_HTML,
        encoding="utf-8",
    )
    (
        source_dir / LOUISIANA_LAWPRINT_SOURCE_FORMAT / "title-14" / "78224.html"
    ).write_text(SAMPLE_SECTION_1_HTML, encoding="utf-8")
    (
        source_dir / LOUISIANA_LAWPRINT_SOURCE_FORMAT / "title-14" / "78337.html"
    ).write_text(SAMPLE_SECTION_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_louisiana_revised_statutes(
        store,
        version="2026-05-10",
        source_dir=source_dir,
        source_as_of="2026-05-10",
        expression_date="2026-05-10",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 2
    assert report.provisions_written == 3
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert len(inventory) == 3
    assert [record.citation_path for record in records] == [
        "us-la/statute/title-14",
        "us-la/statute/14:1",
        "us-la/statute/14:2",
    ]
    assert records[-1].metadata is not None
    assert records[-1].metadata["references_to"] == [
        "us-la/statute/14:35.3",
        "us-la/statute/40:967",
    ]
