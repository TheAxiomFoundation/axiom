import json

import fitz  # type: ignore[import-untyped]

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.documents import extract_official_documents, google_drive_download_url
from axiom_corpus.corpus.io import load_provisions


def test_google_drive_download_url_converts_file_view():
    url = "https://drive.google.com/file/d/abc123XYZ/view?usp=drive_link"

    assert (
        google_drive_download_url(url) == "https://drive.google.com/uc?export=download&id=abc123XYZ"
    )


def test_extract_official_documents_from_local_html_and_pdf(tmp_path):
    html_path = tmp_path / "snap.html"
    long_html_text = "Long eligibility detail. " * 180
    html_path.write_text(
        f"""
        <html>
          <head><title>Ignored browser title</title></head>
          <body>
            <nav>Navigation should not become its own block.</nav>
            <main>
              <h1>Colorado SNAP Policy</h1>
              <h2>Eligibility</h2>
              <p>Households may qualify based on income and household size.</p>
              <p>{long_html_text}</p>
              <ul><li>County departments determine eligibility.</li></ul>
            </main>
          </body>
        </html>
        """
    )
    pdf_path = tmp_path / "waiver.pdf"
    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 72), "SNAP waiver approval\nApproved for a limited area.")
    document.save(pdf_path)
    document.close()
    manifest_path = tmp_path / "documents.yaml"
    manifest_path.write_text(
        f"""
documents:
  - source_id: co-snap-page
    jurisdiction: us-co
    document_class: policy
    title: Colorado SNAP Policy
    source_url: https://cdhs.colorado.gov/snap
    citation_path: us-co/policy/cdhs/snap
    source_format: html
    local_path: {json.dumps(str(html_path))}
    metadata:
      source_authority: Colorado Department of Human Services
      document_subtype: agency_page
  - source_id: co-snap-waiver
    jurisdiction: us-co
    document_class: policy
    title: Colorado SNAP Waiver Approval
    source_url: https://www.fns.usda.gov/example.pdf
    source_format: pdf
    local_path: {json.dumps(str(pdf_path))}
    metadata:
      source_authority: USDA Food and Nutrition Service
      document_subtype: waiver_approval
"""
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_official_documents(
        store,
        manifest_path=manifest_path,
        version="2026-04-30",
        source_as_of="2026-04-30",
    )

    assert report.document_count == 2
    assert report.block_count == 2
    assert report.provisions_written == 4
    assert report.coverage.complete
    assert report.inventory_path.exists()
    assert len(report.source_paths) == 2

    inventory = json.loads(report.inventory_path.read_text())
    assert [item["citation_path"] for item in inventory["items"]] == [
        "us-co/policy/cdhs/snap",
        "us-co/policy/cdhs/snap/block-1",
        "us-co/policy/co-snap-waiver",
        "us-co/policy/co-snap-waiver/page-1",
    ]
    records = load_provisions(report.provisions_path)
    page_record = next(record for record in records if record.kind == "page")
    assert page_record.body is not None
    assert "Approved for a limited area" in page_record.body
    assert page_record.source_id == "co-snap-waiver"
    assert page_record.source_document_id is None
    assert page_record.metadata is not None
    assert page_record.metadata["document_subtype"] == "waiver_approval"


def test_extract_official_documents_splits_numbered_pdf_sections(tmp_path):
    pdf_path = tmp_path / "rules.pdf"
    document = fitz.open()
    page = document.new_page()
    page.insert_text(
        (72, 72),
        "\n".join(
            [
                "IDAHO ADMINISTRATIVE CODE",
                "Section 000",
                "Page 1",
                "000.",
                "LEGAL AUTHORITY.",
                "Legal authority text.",
                "001.",
                "SCOPE.",
                "01.",
                "Scope body.",
                "002. -- 009.",
                "(RESERVED)",
            ]
        ),
    )
    document.save(pdf_path)
    document.close()
    manifest_path = tmp_path / "documents.yaml"
    manifest_path.write_text(
        f"""
documents:
  - source_id: idaho-rule
    jurisdiction: us-id
    document_class: regulation
    title: IDAPA 35.01.01 - Income Tax Administrative Rules
    source_url: https://adminrules.idaho.gov/rules/current/35/350101.pdf
    citation_path: us-id/regulation/idapa/35/01/01
    source_format: pdf
    local_path: {json.dumps(str(pdf_path))}
    extraction:
      segmentation: numbered_sections
      drop_lines:
        - IDAHO ADMINISTRATIVE CODE
"""
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_official_documents(
        store,
        manifest_path=manifest_path,
        version="2026-05-12-idapa-35-01-01",
    )

    assert report.block_count == 3
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-id/regulation/idapa/35/01/01",
        "us-id/regulation/idapa/35/01/01/000",
        "us-id/regulation/idapa/35/01/01/001",
        "us-id/regulation/idapa/35/01/01/002-009",
    ]
    first_section = records[1]
    assert first_section.kind == "section"
    assert first_section.heading == "000. LEGAL AUTHORITY."
    assert first_section.body == "Legal authority text."
    assert first_section.metadata is not None
    assert first_section.metadata["page_start"] == 1
    assert "section_end_label" not in first_section.metadata
    reserved_section = records[3]
    assert reserved_section.metadata is not None
    assert reserved_section.metadata["section_end_label"] == "009"


def test_extract_official_documents_splits_labeled_pdf_sections(tmp_path):
    pdf_path = tmp_path / "capi.pdf"
    document = fitz.open()
    first_page = document.new_page()
    first_page.insert_text(
        (72, 72),
        "\n".join(
            [
                "ELIGIBILITY AND ASSISTANCE STANDARDS",
                "49-001",
                "PROGRAM DEFINITION",
                ".1 Program definition text.",
                "CALIFORNIA-DSS-MANUAL-EAS",
                "Page 660.2",
            ]
        ),
    )
    second_page = document.new_page()
    second_page.insert_text(
        (72, 72),
        "\n".join(
            [
                "ELIGIBILITY AND ASSISTANCE STANDARDS",
                "49-001",
                "PROGRAM DEFINITION",
                ".2 Continued definition text.",
                "49-010",
                "ELIGIBILITY FOR CASH ASSISTANCE PROGRAM",
                "FOR IMMIGRANTS",
                "HANDBOOK BEGINS HERE",
                ".1 Eligibility text.",
            ]
        ),
    )
    document.save(pdf_path)
    document.close()
    manifest_path = tmp_path / "documents.yaml"
    manifest_path.write_text(
        f"""
documents:
  - source_id: ca-capi
    jurisdiction: us-ca
    document_class: regulation
    title: California CAPI Regulations
    source_url: https://cdss.ca.gov/Portals/9/CAPI/CAPI_Regulations-Accessible.pdf
    citation_path: us-ca/regulation/cdss/eas/49
    source_format: pdf
    local_path: {json.dumps(str(pdf_path))}
    extraction:
      segmentation: labeled_sections
      section_label_pattern: "^(?P<label>49-[0-9]{{3}})$"
      drop_lines:
        - ELIGIBILITY AND ASSISTANCE STANDARDS
        - CALIFORNIA-DSS-MANUAL-EAS
      drop_line_patterns:
        - "^Page \\\\d+(?:\\\\.\\\\d+)?$"
"""
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_official_documents(
        store,
        manifest_path=manifest_path,
        version="2026-05-12-capi",
    )

    assert report.block_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-ca/regulation/cdss/eas/49",
        "us-ca/regulation/cdss/eas/49/49-001",
        "us-ca/regulation/cdss/eas/49/49-010",
    ]
    definition = records[1]
    assert definition.body == ".1 Program definition text. .2 Continued definition text."
    eligibility = records[2]
    assert eligibility.heading == "49-010 ELIGIBILITY FOR CASH ASSISTANCE PROGRAM FOR IMMIGRANTS"
    assert eligibility.body == "HANDBOOK BEGINS HERE .1 Eligibility text."
    assert eligibility.metadata is not None
    assert eligibility.metadata["page_start"] == 2
