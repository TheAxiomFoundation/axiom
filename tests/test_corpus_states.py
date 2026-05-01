import zipfile
from xml.sax.saxutils import escape

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.states import (
    extract_cic_html_release,
    extract_cic_odt_release,
    extract_colorado_docx_release,
    extract_dc_code,
    extract_ohio_revised_code,
    extract_texas_tcas,
    state_run_id,
)

SAMPLE_DC_INDEX = """<?xml version="1.0" encoding="utf-8"?>
<container xmlns="https://code.dccouncil.us/schemas/dc-library"
           xmlns:xi="http://www.w3.org/2001/XInclude"
           enacted="true">
  <prefix>Title</prefix>
  <num>1</num>
  <heading>Government Organization.</heading>
  <container>
    <prefix>Chapter</prefix>
    <num>1</num>
    <heading>General Provisions.</heading>
    <xi:include href="./sections/1-101.xml"/>
  </container>
</container>
"""


SAMPLE_DC_SECTION = """<?xml version="1.0" encoding="utf-8"?>
<section xmlns="https://code.dccouncil.us/schemas/dc-library">
  <num>1-101</num>
  <heading>Short title.</heading>
  <text>This chapter may be cited as the District Charter.</text>
  <para>
    <num>(a)</num>
    <text>See <cite path="§1-102">section 1-102</cite>.</text>
  </para>
  <annotations>
    <text type="History">Law 1-1.</text>
  </annotations>
</section>
"""


SAMPLE_CIC_HTML = """<!DOCTYPE html>
<html>
<body>
  <nav><h1><span>Title 67<br/>Taxes and Licenses</span></h1></nav>
  <main>
    <div>
      <h2 id="t67c01"><span>Chapter 1<br/>General Provisions</span></h2>
      <div>
        <h3 id="t67c01s67-1-101"><span>67-1-101. Short title.</span></h3>
        <p>This part may be cited as the Revenue Act.</p>
        <p>Cross-References. See <cite><a href="#t67c01s67-1-102">67-1-102</a></cite>.</p>
      </div>
    </div>
  </main>
</body>
</html>
"""

SAMPLE_CIC_ODT_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<office:document-content
    xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
    xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0">
  <office:body>
    <office:text>
      <text:p text:style-name="P1">Title 8.01. Civil Remedies and Procedure.</text:p>
      <text:p text:style-name="P2">Chapter 1. General Provisions.</text:p>
      <text:p text:style-name="P2">§ 8.01-1. Short title.</text:p>
      <text:p text:style-name="P3">Chapter 1. General Provisions.</text:p>
      <text:p text:style-name="P4">§ 8.01-1. Short title.</text:p>
      <text:p text:style-name="P5">Text</text:p>
      <text:p text:style-name="P6">This title may be cited as the Civil Remedies Act.</text:p>
      <text:p text:style-name="P5">History</text:p>
      <text:p text:style-name="P6">Acts 2026, c. 1.</text:p>
    </office:text>
  </office:body>
</office:document-content>
"""


SAMPLE_COLORADO_PARAGRAPHS = [
    "Colorado Revised Statutes 2025",
    "TITLE 26",
    "HUMAN SERVICES CODE",
    "ARTICLE 1",
    "Department of Human Services",
    "PART 1",
    "GENERAL PROVISIONS",
    "26-1-101. Short title. This title shall be known as the human services code.",
    "Source: L. 2025: Entire section added.",
    "26-1-102. Legislative declaration. (1) The general assembly declares the policy.",
]


SAMPLE_COLORADO_SUPPLEMENT_PARAGRAPHS = [
    "Colorado Revised Statutes 2025",
    "TITLE 39",
    "Taxation",
    "ARTICLE 22",
    "Income Tax",
    "39-22-516.8. Credit. (1) The credit is allowed.",
    "(2) [Insert 39-22-516.8(2)(b).pdf here]",
]


SAMPLE_COLORADO_VARIANT_PARAGRAPHS = [
    "Colorado Revised Statutes 2025",
    "TITLE 1",
    "ELECTIONS",
    "ARTICLE 7",
    "General Election",
    "1-7-1001.3. Definitions. [Editor's note: This version of this section is effective until March 1, 2026.] Current version.",
    "1-7-1001.3. Definitions. [Editor's note: This version of this section is effective March 1, 2026.] Future version.",
    "1-7-1002. Fees. A table row follows.",
    "1-7-1002.5 (4) 12.00",
    "Final paragraph.",
]


SAMPLE_OHIO_INDEX = """<!DOCTYPE html>
<html>
<body>
  <main>
    <a href="/ohio-revised-code/general-provisions">General Provisions</a>
    <a href="/ohio-revised-code/title-51">Title 51 | Public Welfare</a>
  </main>
</body>
</html>
"""


SAMPLE_OHIO_TITLE = """<!DOCTYPE html>
<html>
<body>
  <main>
    <h1>Title 51 | Public Welfare</h1>
    <a href="/ohio-revised-code/chapter-5160">Chapter 5160 | Medical Assistance Programs</a>
  </main>
</body>
</html>
"""


SAMPLE_OHIO_CHAPTER = """<!DOCTYPE html>
<html>
<body>
  <main>
    <h1>Chapter 5160 | Medical Assistance Programs</h1>
    <div class="list-content">
      <span class="content-head">
        <span class="content-head-text">
          <a href="section-5160.01">Section 5160.01 <span>|</span> Definitions.</a>
        </span>
      </span>
      <div class="content-body">
        <div class="laws-section-info">
          <div class="laws-section-info-module">
            <div class="label">Effective:</div>
            <div class="value">September 29, 2013</div>
          </div>
          <div class="laws-section-info-module">
            <div class="label">Latest Legislation: </div>
            <div class="value">House Bill 59 - 130th General Assembly</div>
          </div>
          <div class="laws-section-info-module no-print">
            <div class="label">PDF:</div>
            <div class="value"><a href="/assets/laws/revised-code/authenticated/51/5160/5160.01.pdf">Download Authenticated PDF</a></div>
          </div>
        </div>
        <section class="laws-body">
          <span>
            <p>As used in this chapter:</p>
            <p>(A) "Medicaid managed care organization" has the same meaning as in section <a class="section-link" href="/ohio-revised-code/section-5167.01">5167.01</a> of the Revised Code.</p>
          </span>
        </section>
      </div>
    </div>
    <div class="list-content">
      <span class="content-head">
        <span class="content-head-text">
          <a href="section-5160.011">Section 5160.011 <span>|</span> References to department.</a>
        </span>
      </span>
      <div class="content-body">
        <div class="laws-section-info">
          <div class="laws-section-info-module">
            <div class="label">Effective:</div>
            <div class="value">January 1, 2025</div>
          </div>
        </div>
        <section class="laws-body">
          <span>
            <p>References are deemed to refer to the department of medicaid.</p>
          </span>
          <div class="laws-notice"><p>Last updated January 1, 2025 at 5:35 AM</p></div>
        </section>
      </div>
    </div>
  </main>
</body>
</html>
"""


SAMPLE_TEXAS_HTML = """<html><body><pre xml:space="preserve">
<p class="center" style="font-weight:bold;">TAX CODE</p>
<p class="center" style="font-weight:bold;">TITLE 1. PROPERTY TAX CODE</p>
<p class="center" style="font-weight:bold;">SUBTITLE A. GENERAL PROVISIONS</p>
<p class="center" style="font-weight:bold;">CHAPTER 1. GENERAL PROVISIONS</p>
<p class="left"><a name="1.01"></a><a name="65126.55941"></a></p>
<p style="text-indent:7ex;" class="left"><a target="_blank"
 href="https://statutes.capitol.texas.gov/Docs/TX/htm/TX.1.htm#1.01"
 style="color:inherit;font-weight:bold;">Sec. 1.01.  SHORT TITLE.</a>
 This title may be cited as the Property Tax Code.</p>
<p class="left">Acts 1979, 66th Leg., ch. 841.</p>
<p class="center" style="font-weight:bold;">SUBCHAPTER A. DEFINITIONS</p>
<p class="left"><a name="1.03"></a><a name="65128.55943"></a></p>
<p style="text-indent:7ex;" class="left"><a target="_blank"
 href="https://statutes.capitol.texas.gov/Docs/TX/htm/TX.1.htm#1.03"
 style="color:inherit;font-weight:bold;">Sec. 1.03.  CONSTRUCTION OF TITLE.</a>
 The Code Construction Act (Chapter <a target="_blank"
 href="https://statutes.capitol.texas.gov/GetStatute.aspx?Code=GV&amp;Value=311">311</a>,
 Government Code) applies.</p>
</pre></body></html>
"""


def _write_odt(path, content_xml):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("mimetype", "application/vnd.oasis.opendocument.text")
        archive.writestr("content.xml", content_xml)


def _write_docx(path, paragraphs):
    body = "".join(
        f"<w:p><w:r><w:t>{escape(paragraph)}</w:t></w:r></w:p>" for paragraph in paragraphs
    )
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{body}</w:body>"
        "</w:document>"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("word/document.xml", document_xml)


def _write_pdf(path, text):
    import fitz

    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 72), text)
    document.save(path)
    document.close()


def _write_ohio_fixture(source_dir):
    title_dir = source_dir / "ohio-revised-code" / "titles"
    chapter_dir = source_dir / "ohio-revised-code" / "chapters"
    title_dir.mkdir(parents=True)
    chapter_dir.mkdir()
    (source_dir / "ohio-revised-code" / "index.html").write_text(SAMPLE_OHIO_INDEX)
    (title_dir / "title-51.html").write_text(SAMPLE_OHIO_TITLE)
    (chapter_dir / "chapter-5160.html").write_text(SAMPLE_OHIO_CHAPTER)


def _write_texas_fixture(source_dir):
    (source_dir / "assets").mkdir(parents=True)
    (source_dir / "trees").mkdir()
    html_dir = source_dir / "html" / "TX" / "htm"
    html_dir.mkdir(parents=True)
    (source_dir / "assets" / "StatuteCodeTree.json").write_text(
        """
{
  "StatuteCode": [
    {"codeID": "28", "code": "TX", "CodeName": "Tax Code"}
  ]
}
"""
    )
    (source_dir / "trees" / "TX.json").write_text(
        """
[
  {
    "name": "TITLE 1. PROPERTY TAX CODE",
    "value": "65122.55940",
    "valuePath": "S/28/65122.55940",
    "children": [
      {
        "name": "SUBTITLE A. GENERAL PROVISIONS",
        "value": "65123.55940",
        "valuePath": "S/28/65122.55940/65123.55940",
        "children": [
          {
            "name": "CHAPTER 1. GENERAL PROVISIONS",
            "value": "65124.55940",
            "valuePath": "S/28/65122.55940/65123.55940/65124.55940",
            "htmLink": "/TX/htm/TX.1.htm",
            "children": null
          }
        ]
      }
    ]
  }
]
"""
    )
    (html_dir / "TX.1.htm").write_text(SAMPLE_TEXAS_HTML)


def test_state_run_id_scopes_title_and_limit():
    assert (
        state_run_id("2026-04-29", jurisdiction="us-tn", only_title="067", limit=2)
        == "2026-04-29-us-tn-title-67-limit-2"
    )


def test_extract_dc_code_writes_inventory_provisions_and_coverage(tmp_path):
    source_dir = tmp_path / "dc" / "titles"
    title_dir = source_dir / "1"
    sections_dir = title_dir / "sections"
    sections_dir.mkdir(parents=True)
    (title_dir / "index.xml").write_text(SAMPLE_DC_INDEX)
    (sections_dir / "1-101.xml").write_text(SAMPLE_DC_SECTION)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_dc_code(
        store,
        version="2026-04-29",
        source_dir=source_dir,
        source_as_of="2026-04-01",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    assert report.provisions_written == 3
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert [item.citation_path for item in inventory] == [
        "us-dc/statute/1",
        "us-dc/statute/1/chapter-1",
        "us-dc/statute/1/1-101",
    ]
    assert records[-1].heading == "Short title."
    assert records[-1].parent_citation_path == "us-dc/statute/1/chapter-1"
    assert records[-1].metadata["references_to"] == ["us-dc/statute/1/1-102"]
    assert "District Charter" in records[-1].body


def test_extract_cic_html_release_writes_state_records(tmp_path):
    release_dir = tmp_path / "release76.2021.05.21"
    release_dir.mkdir()
    (release_dir / "gov.tn.tca.title.67.html").write_text(SAMPLE_CIC_HTML)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_cic_html_release(
        store,
        jurisdiction="us-tn",
        version="2021-05-21",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-tn/statute/67",
        "us-tn/statute/67/chapter-1",
        "us-tn/statute/67/67-1-101",
    ]
    assert records[-1].heading == "Short title."
    assert records[-1].source_as_of == "2021-05-21"
    assert "Revenue Act" in records[-1].body


def test_extract_cic_odt_release_writes_state_records(tmp_path):
    release_dir = tmp_path / "release90.2023.03"
    release_dir.mkdir()
    _write_odt(release_dir / "gov.va.code.title.08.01.odt", SAMPLE_CIC_ODT_CONTENT)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_cic_odt_release(
        store,
        jurisdiction="us-va",
        version="2023-03-01",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-va/statute/8.01",
        "us-va/statute/8.01/chapter-1",
        "us-va/statute/8.01/8.01-1",
    ]
    assert records[-1].heading == "Short title."
    assert records[-1].source_format == "cic-state-code-odt"
    assert records[-1].source_as_of == "2023-03-01"
    assert "Civil Remedies Act" in records[-1].body
    assert "History" not in records[-1].body
    assert "Acts 2026" in records[-1].body


def test_extract_texas_tcas_writes_state_records(tmp_path):
    source_dir = tmp_path / "texas"
    _write_texas_fixture(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_texas_tcas(
        store,
        version="2026-05-01",
        source_dir=source_dir,
        source_as_of="2026-05-01",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 5
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-tx/statute/tx",
        "us-tx/statute/tx/title-1",
        "us-tx/statute/tx/title-1/subtitle-a",
        "us-tx/statute/tx/title-1/subtitle-a/chapter-1",
        "us-tx/statute/tx/title-1/subtitle-a/chapter-1/subchapter-a",
        "us-tx/statute/tx/1.01",
        "us-tx/statute/tx/1.03",
    ]
    assert records[-1].parent_citation_path.endswith("/subchapter-a")
    assert records[-1].source_format == "texas-tcas-html"
    assert records[-1].metadata["references_to"] == ["us-tx/statute/gv/311"]
    assert "Code Construction Act" in records[-1].body


def test_extract_ohio_revised_code_writes_state_records(tmp_path):
    source_dir = tmp_path / "ohio"
    _write_ohio_fixture(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_ohio_revised_code(
        store,
        version="2026-05-01",
        source_dir=source_dir,
        source_as_of="2026-05-01",
        only_title="51",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-oh/statute/title-51",
        "us-oh/statute/chapter-5160",
        "us-oh/statute/5160.01",
        "us-oh/statute/5160.011",
    ]
    assert records[2].metadata["references_to"] == ["us-oh/statute/5167.01"]
    assert records[2].metadata["effective_date"] == "September 29, 2013"
    assert records[3].metadata["last_updated"] == "Last updated January 1, 2025 at 5:35 AM"
    assert "department of medicaid" in (records[3].body or "")


def test_extract_colorado_docx_release_writes_state_records(tmp_path):
    release_dir = tmp_path / "release2025"
    docx_dir = release_dir / "docx"
    docx_dir.mkdir(parents=True)
    _write_docx(docx_dir / "crs2025-title-26.docx", SAMPLE_COLORADO_PARAGRAPHS)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_colorado_docx_release(
        store,
        version="2026-04-29",
        release_dir=release_dir,
        source_as_of="2025-09-16",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 3
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-co/statute/26",
        "us-co/statute/26/article-1",
        "us-co/statute/26/article-1/part-1",
        "us-co/statute/26/26-1-101",
        "us-co/statute/26/26-1-102",
    ]
    assert records[0].heading == "HUMAN SERVICES CODE"
    assert records[-2].heading == "Short title."
    assert records[-2].parent_citation_path == "us-co/statute/26/article-1/part-1"
    assert records[-2].source_format == "colorado-crs-docx"
    assert records[-2].source_as_of == "2025-09-16"
    assert "human services code" in records[-2].body
    assert "Source: L. 2025" in records[-2].body


def test_extract_colorado_docx_release_inlines_supplement_pdfs(tmp_path):
    release_dir = tmp_path / "release2025"
    docx_dir = release_dir / "docx"
    supplement_dir = release_dir / "supplement-pdfs" / "Title 39"
    supplement_dir.mkdir(parents=True)
    docx_dir.mkdir(parents=True)
    _write_docx(docx_dir / "crs2025-title-39.docx", SAMPLE_COLORADO_SUPPLEMENT_PARAGRAPHS)
    _write_pdf(supplement_dir / "39-22-516.8(2)(b).pdf", "Supplement table text")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_colorado_docx_release(
        store,
        version="2026-04-29",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    assert len(report.source_paths) == 2
    records = load_provisions(report.provisions_path)
    section = records[-1]
    assert section.citation_path == "us-co/statute/39/39-22-516.8"
    assert "Supplement table text" in section.body
    assert section.metadata["supplement_pdf_files"] == ["39-22-516.8(2)(b).pdf"]
    inventory = load_source_inventory(report.inventory_path)
    assert inventory[-1].metadata["supplement_pdf_files"] == ["39-22-516.8(2)(b).pdf"]


def test_extract_colorado_docx_release_keeps_effective_date_variants(tmp_path):
    release_dir = tmp_path / "release2025"
    docx_dir = release_dir / "docx"
    docx_dir.mkdir(parents=True)
    _write_docx(docx_dir / "crs2025-title-01.docx", SAMPLE_COLORADO_VARIANT_PARAGRAPHS)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_colorado_docx_release(
        store,
        version="2026-04-29",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    records = load_provisions(report.provisions_path)
    section_paths = [record.citation_path for record in records if record.kind == "section"]
    assert section_paths == [
        "us-co/statute/1/1-7-1001.3",
        "us-co/statute/1/1-7-1001.3@this-version-of-this-section-is-effective-march-1-2026",
        "us-co/statute/1/1-7-1002",
    ]
    assert "Final paragraph." in records[-1].body
    assert "1-7-1002.5 (4) 12.00" in records[-1].body
    assert records[-2].metadata["variant"] == (
        "this-version-of-this-section-is-effective-march-1-2026"
    )


def test_extract_dc_code_cli(tmp_path, capsys):
    source_dir = tmp_path / "dc" / "titles"
    title_dir = source_dir / "1"
    sections_dir = title_dir / "sections"
    sections_dir.mkdir(parents=True)
    (title_dir / "index.xml").write_text(SAMPLE_DC_INDEX)
    (sections_dir / "1-101.xml").write_text(SAMPLE_DC_SECTION)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-dc-code",
            "--base",
            str(base),
            "--version",
            "2026-04-29",
            "--source-dir",
            str(source_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-dc"' in output
    assert '"provisions_written": 3' in output


def test_extract_colorado_docx_cli(tmp_path, capsys):
    release_dir = tmp_path / "release2025"
    docx_dir = release_dir / "docx"
    docx_dir.mkdir(parents=True)
    _write_docx(docx_dir / "crs2025-title-26.docx", SAMPLE_COLORADO_PARAGRAPHS)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-colorado-docx",
            "--base",
            str(base),
            "--version",
            "2026-04-29",
            "--release-dir",
            str(release_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-co"' in output
    assert '"provisions_written": 5' in output


def test_extract_texas_tcas_cli_local_source(tmp_path, capsys):
    source_dir = tmp_path / "texas"
    _write_texas_fixture(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-texas-tcas",
            "--base",
            str(base),
            "--version",
            "2026-05-01",
            "--source-dir",
            str(source_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-tx"' in output
    assert '"provisions_written": 7' in output


def test_extract_ohio_revised_code_cli_local_source(tmp_path, capsys):
    source_dir = tmp_path / "ohio"
    _write_ohio_fixture(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-ohio-revised-code",
            "--base",
            str(base),
            "--version",
            "2026-05-01",
            "--source-dir",
            str(source_dir),
            "--only-title",
            "51",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-oh"' in output
    assert '"provisions_written": 4' in output


def test_extract_cic_state_html_cli(tmp_path, capsys):
    release_dir = tmp_path / "release76.2021.05.21"
    release_dir.mkdir()
    (release_dir / "gov.tn.tca.title.67.html").write_text(SAMPLE_CIC_HTML)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-cic-state-html",
            "--base",
            str(base),
            "--version",
            "2021-05-21",
            "--jurisdiction",
            "us-tn",
            "--release-dir",
            str(release_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-tn"' in output
    assert '"provisions_written": 3' in output


def test_extract_cic_state_odt_cli(tmp_path, capsys):
    release_dir = tmp_path / "release90.2023.03"
    release_dir.mkdir()
    _write_odt(release_dir / "gov.va.code.title.08.01.odt", SAMPLE_CIC_ODT_CONTENT)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-cic-state-odt",
            "--base",
            str(base),
            "--version",
            "2023-03-01",
            "--jurisdiction",
            "us-va",
            "--release-dir",
            str(release_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-va"' in output
    assert '"provisions_written": 3' in output
