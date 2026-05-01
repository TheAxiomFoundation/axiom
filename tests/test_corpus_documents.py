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
    html_path.write_text(
        """
        <html>
          <head><title>Ignored browser title</title></head>
          <body>
            <nav>Navigation should not become its own block.</nav>
            <main>
              <h1>Colorado SNAP Policy</h1>
              <h2>Eligibility</h2>
              <p>Households may qualify based on income and household size.</p>
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
