from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.michigan import (
    MICHIGAN_MCL_CHAPTER_SOURCE_FORMAT,
    MICHIGAN_MCL_INDEX_SOURCE_FORMAT,
    _RecordedSource,
    extract_michigan_compiled_laws,
    parse_michigan_chapter_index,
    parse_michigan_chapter_xml,
)

SAMPLE_INDEX_HTML = """
<html><body><pre>
 1/14/2026  8:46 PM      3362986 <A HREF="/documents/mcl/Chapter%20206.xml">Chapter 206.xml</A><br>
 8/20/2025  7:26 PM      8663658 <A HREF="/documents/mcl/Chapter%20750.xml">Chapter 750.xml</A><br>
</pre></body></html>
"""

SAMPLE_CHAPTER_XML = """<?xml version="1.0" encoding="utf-8"?>
<MCLChapterInfo>
  <DocumentID>11034</DocumentID>
  <Repealed>false</Repealed>
  <Name>206</Name>
  <Title>INCOME TAX ACT OF 1967</Title>
  <MCLDocumentInfoCollection>
    <MCLStatuteInfo>
      <DocumentID>11035</DocumentID>
      <Repealed>false</Repealed>
      <Name>Act 281 of 1967</Name>
      <Heading>INCOME TAX ACT OF 1967</Heading>
      <ShortTitle>Income tax act of 1967</ShortTitle>
      <MCLDocumentInfoCollection>
        <MCLDivisionInfo>
          <DocumentID>47903</DocumentID>
          <Repealed>false</Repealed>
          <Name>206.1.D10</Name>
          <DivisionNumber>1</DivisionNumber>
          <DivisionType>PART</DivisionType>
          <MCLDocumentInfoCollection>
            <MCLSectionInfo>
              <DocumentID>11037</DocumentID>
              <Repealed>false</Repealed>
              <HistoryText>&lt;HistoryData&gt;1967, Act 281, Eff. Oct. 1, 1967&lt;/HistoryData&gt;</HistoryText>
              <MCLNumber>206.1</MCLNumber>
              <CatchLine>Income tax act of 1967; short title.</CatchLine>
              <Label>1</Label>
              <BodyText>&lt;Section-Body&gt;&lt;Section-Number&gt;Sec. 1.&lt;/Section-Number&gt;&lt;Paragraph&gt;&lt;P&gt;This act may be cited as the income tax act of 1967. See MCL 206.30.&lt;/P&gt;&lt;/Paragraph&gt;&lt;/Section-Body&gt;</BodyText>
            </MCLSectionInfo>
            <MCLSectionInfo>
              <DocumentID>11038</DocumentID>
              <Repealed>true</Repealed>
              <MCLNumber>206.2</MCLNumber>
              <CatchLine>Repealed. 2011, Act 38, Eff. Jan. 1, 2012.</CatchLine>
              <Label>2</Label>
              <BodyText />
            </MCLSectionInfo>
          </MCLDocumentInfoCollection>
        </MCLDivisionInfo>
      </MCLDocumentInfoCollection>
    </MCLStatuteInfo>
  </MCLDocumentInfoCollection>
</MCLChapterInfo>
"""

SAMPLE_INDEX_SOURCE = _RecordedSource(
    source_url="https://legislature.mi.gov/documents/mcl/",
    source_path="sources/us-mi/statute/test/index.html",
    source_format=MICHIGAN_MCL_INDEX_SOURCE_FORMAT,
    sha256="abc",
)

SAMPLE_CHAPTER_SOURCE = _RecordedSource(
    source_url="https://legislature.mi.gov/documents/mcl/Chapter%20206.xml",
    source_path="sources/us-mi/statute/test/chapter-206.xml",
    source_format=MICHIGAN_MCL_CHAPTER_SOURCE_FORMAT,
    sha256="def",
)


def test_parse_michigan_index_and_chapter_xml():
    listings = parse_michigan_chapter_index(
        SAMPLE_INDEX_HTML,
        source=SAMPLE_INDEX_SOURCE,
    )
    assert [listing.chapter for listing in listings] == ["206", "750"]
    assert listings[0].relative_path == "michigan-mcl-chapter-xml/chapter-206.xml"

    chapter, sections = parse_michigan_chapter_xml(
        SAMPLE_CHAPTER_XML,
        listing=listings[0],
        source=SAMPLE_CHAPTER_SOURCE,
    )
    assert chapter.citation_path == "us-mi/statute/chapter-206"
    assert chapter.heading == "INCOME TAX ACT OF 1967"
    assert [section.citation_path for section in sections] == [
        "us-mi/statute/206.1",
        "us-mi/statute/206.2",
    ]
    assert sections[0].hierarchy == (
        "Act 281 of 1967: INCOME TAX ACT OF 1967",
        "PART 1",
    )
    assert "Sec. 1." in (sections[0].body or "")
    assert sections[0].history == ("1967, Act 281, Eff. Oct. 1, 1967",)
    assert sections[0].references_to == ("us-mi/statute/206.30",)
    assert sections[1].body is None
    assert sections[1].status == "repealed"


def test_extract_michigan_compiled_laws_from_source_dir_writes_complete_artifacts(
    tmp_path,
):
    source_dir = tmp_path / "source"
    (source_dir / MICHIGAN_MCL_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / MICHIGAN_MCL_CHAPTER_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / MICHIGAN_MCL_INDEX_SOURCE_FORMAT / "index.html").write_text(
        SAMPLE_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / MICHIGAN_MCL_CHAPTER_SOURCE_FORMAT / "chapter-206.xml").write_text(
        SAMPLE_CHAPTER_XML,
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_michigan_compiled_laws(
        store,
        version="2026-05-10",
        source_dir=source_dir,
        source_as_of="2026-05-10",
        expression_date="2026-05-10",
        only_title="206",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 2
    assert report.provisions_written == 3
    assert len(load_source_inventory(report.inventory_path)) == 3
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-mi/statute/chapter-206",
        "us-mi/statute/206.1",
        "us-mi/statute/206.2",
    ]
    assert records[1].metadata is not None
    assert records[1].metadata["references_to"] == ["us-mi/statute/206.30"]
