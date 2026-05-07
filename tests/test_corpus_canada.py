"""Tests for the source-first Canada federal statute extractor."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.canada import (
    CANADA_JURISDICTION,
    extract_canada_acts,
)
from axiom_corpus.fetchers.legislation_canada import CanadaActReference

SAMPLE_CANADA_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Statute xmlns:lims="http://justice.gc.ca/lims" in-force="yes">
  <Identification>
    <ConsolidatedNumber>I-3.3</ConsolidatedNumber>
    <ShortTitle>Income Tax Act</ShortTitle>
    <LongTitle>An Act respecting income taxes</LongTitle>
  </Identification>
  <Body>
    <Section lims:id="sec-1">
      <Label>1</Label>
      <MarginalNote>Short title</MarginalNote>
      <Text>This Act may be cited as the Income Tax Act.</Text>
    </Section>
    <Section lims:id="sec-2">
      <Label>2</Label>
      <MarginalNote>Definitions</MarginalNote>
      <Text>In this Act, the following definitions apply.</Text>
      <Subsection>
        <Label>(1)</Label>
        <Text>amount means any amount.</Text>
        <Paragraph>
          <Label>(a)</Label>
          <Text>First paragraph.</Text>
          <Subparagraph>
            <Label>(i)</Label>
            <Text>First subparagraph.</Text>
          </Subparagraph>
        </Paragraph>
      </Subsection>
    </Section>
    <Section>
      <Label>7.2</Label>
      <Text>A decimal-numbered section.</Text>
    </Section>
  </Body>
</Statute>
"""


SECONDARY_ACT_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Statute xmlns:lims="http://justice.gc.ca/lims" in-force="yes">
  <Identification>
    <ConsolidatedNumber>P-4</ConsolidatedNumber>
    <ShortTitle>Patent Act</ShortTitle>
    <LongTitle>An Act respecting patents</LongTitle>
  </Identification>
  <Body>
    <Section>
      <Label>125</Label>
      <MarginalNote>Impeachment</MarginalNote>
      <Text>The body of section 125.</Text>
    </Section>
  </Body>
</Statute>
"""


class _FakeFetcher:
    """In-memory replacement for CanadaLegislationFetcher used by tests."""

    def __init__(self, acts: dict[str, bytes]):
        self._acts = acts
        self.list_calls = 0
        self.download_calls: list[str] = []
        self.closed = False

    def list_all_acts(self) -> list[CanadaActReference]:
        self.list_calls += 1
        return [CanadaActReference(code=code, title=None) for code in sorted(self._acts)]

    def download_act(self, code: str) -> bytes:
        self.download_calls.append(code)
        if code not in self._acts:
            from httpx import HTTPStatusError, Request, Response

            req = Request("GET", f"https://laws-lois.justice.gc.ca/eng/XML/{code}.xml")
            raise HTTPStatusError("not found", request=req, response=Response(404, request=req))
        return self._acts[code]

    def close(self) -> None:
        self.closed = True


def _store(tmp_path: Path) -> CorpusArtifactStore:
    return CorpusArtifactStore(tmp_path / "corpus")


def test_extract_canada_acts_emits_act_section_and_subsection_rows(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({"I-3.3": SAMPLE_CANADA_XML.encode()})
    store = _store(tmp_path)

    report = extract_canada_acts(
        store,
        version="2026-05-06",
        fetcher=fetcher,
        only_acts=["I-3.3"],
        source_as_of="2026-05-01",
    )

    assert report.jurisdiction == CANADA_JURISDICTION
    assert report.act_count == 1
    assert report.section_count == 3
    assert report.subsection_count == 3  # Subsection (1), Paragraph (a), Subparagraph (i)
    assert report.skipped_act_count == 0
    assert report.errors == ()

    provisions = [
        json.loads(line) for line in report.provisions_path.read_text().splitlines() if line.strip()
    ]
    paths = [p["citation_path"] for p in provisions]

    # Act, sections, then subsection chain
    assert "canada/statute/I-3.3" in paths
    assert "canada/statute/I-3.3/1" in paths
    assert "canada/statute/I-3.3/2" in paths
    assert "canada/statute/I-3.3/7.2" in paths
    assert "canada/statute/I-3.3/2/1" in paths
    assert "canada/statute/I-3.3/2/1/a" in paths
    assert "canada/statute/I-3.3/2/1/a/i" in paths


def test_extract_canada_acts_sets_proper_parent_chain(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({"I-3.3": SAMPLE_CANADA_XML.encode()})
    store = _store(tmp_path)

    report = extract_canada_acts(store, version="2026-05-06", fetcher=fetcher, only_acts=["I-3.3"])
    by_path = {
        json.loads(line)["citation_path"]: json.loads(line)
        for line in report.provisions_path.read_text().splitlines()
        if line.strip()
    }

    assert by_path["canada/statute/I-3.3"].get("parent_citation_path") is None
    assert by_path["canada/statute/I-3.3/2"]["parent_citation_path"] == "canada/statute/I-3.3"
    assert by_path["canada/statute/I-3.3/2/1"]["parent_citation_path"] == "canada/statute/I-3.3/2"
    assert (
        by_path["canada/statute/I-3.3/2/1/a"]["parent_citation_path"] == "canada/statute/I-3.3/2/1"
    )
    assert (
        by_path["canada/statute/I-3.3/2/1/a/i"]["parent_citation_path"]
        == "canada/statute/I-3.3/2/1/a"
    )


def test_extract_canada_acts_strips_parens_from_subsection_labels(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({"I-3.3": SAMPLE_CANADA_XML.encode()})
    store = _store(tmp_path)

    report = extract_canada_acts(store, version="2026-05-06", fetcher=fetcher, only_acts=["I-3.3"])
    paths = {
        json.loads(line)["citation_path"]
        for line in report.provisions_path.read_text().splitlines()
        if line.strip()
    }

    # Path segments are bare numbers/letters, never wrapped in parens.
    for p in paths:
        assert "(" not in p and ")" not in p


def test_extract_canada_acts_uses_act_short_title_as_root_heading(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({"I-3.3": SAMPLE_CANADA_XML.encode()})
    store = _store(tmp_path)

    report = extract_canada_acts(store, version="2026-05-06", fetcher=fetcher, only_acts=["I-3.3"])
    by_path = {
        json.loads(line)["citation_path"]: json.loads(line)
        for line in report.provisions_path.read_text().splitlines()
        if line.strip()
    }
    assert by_path["canada/statute/I-3.3"]["heading"] == "Income Tax Act"
    assert by_path["canada/statute/I-3.3/2"]["heading"] == "Definitions"


def test_extract_canada_acts_section_url_carries_section_number(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({"I-3.3": SAMPLE_CANADA_XML.encode()})
    store = _store(tmp_path)

    report = extract_canada_acts(store, version="2026-05-06", fetcher=fetcher, only_acts=["I-3.3"])
    by_path = {
        json.loads(line)["citation_path"]: json.loads(line)
        for line in report.provisions_path.read_text().splitlines()
        if line.strip()
    }
    assert (
        by_path["canada/statute/I-3.3/2"]["source_url"]
        == "https://laws-lois.justice.gc.ca/eng/acts/I-3.3/section-2.html"
    )
    assert (
        by_path["canada/statute/I-3.3/7.2"]["source_url"]
        == "https://laws-lois.justice.gc.ca/eng/acts/I-3.3/section-7.2.html"
    )


def test_extract_canada_acts_skips_failed_downloads(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({"I-3.3": SAMPLE_CANADA_XML.encode()})
    store = _store(tmp_path)

    report = extract_canada_acts(
        store,
        version="2026-05-06",
        fetcher=fetcher,
        only_acts=["I-3.3", "MISSING-ACT"],
    )

    assert report.act_count == 1
    assert report.skipped_act_count == 1
    assert any("MISSING-ACT" in err for err in report.errors)


def test_extract_canada_acts_handles_multiple_acts(tmp_path: Path) -> None:
    fetcher = _FakeFetcher(
        {
            "I-3.3": SAMPLE_CANADA_XML.encode(),
            "P-4": SECONDARY_ACT_XML.encode(),
        }
    )
    store = _store(tmp_path)

    report = extract_canada_acts(store, version="2026-05-06", fetcher=fetcher)

    assert report.act_count == 2
    assert {"I-3.3", "P-4"}.issubset(set(fetcher.download_calls))
    paths = {
        json.loads(line)["citation_path"]
        for line in report.provisions_path.read_text().splitlines()
        if line.strip()
    }
    assert "canada/statute/I-3.3" in paths
    assert "canada/statute/P-4" in paths
    assert "canada/statute/P-4/125" in paths


def test_extract_canada_acts_limit_acts_truncates_iteration(tmp_path: Path) -> None:
    fetcher = _FakeFetcher(
        {
            "A-1": SECONDARY_ACT_XML.replace("P-4", "A-1").replace("Patent", "A").encode(),
            "I-3.3": SAMPLE_CANADA_XML.encode(),
            "P-4": SECONDARY_ACT_XML.encode(),
        }
    )
    store = _store(tmp_path)

    report = extract_canada_acts(store, version="2026-05-06", fetcher=fetcher, limit_acts=2)
    assert report.act_count == 2


def test_extract_canada_acts_closes_owned_fetcher(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({"I-3.3": SAMPLE_CANADA_XML.encode()})
    store = _store(tmp_path)
    extract_canada_acts(store, version="2026-05-06", fetcher=fetcher, only_acts=["I-3.3"])
    # Injected fetcher is left open for the caller to manage.
    assert fetcher.closed is False


def test_extract_canada_acts_writes_inventory_and_coverage(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({"I-3.3": SAMPLE_CANADA_XML.encode()})
    store = _store(tmp_path)
    report = extract_canada_acts(store, version="2026-05-06", fetcher=fetcher, only_acts=["I-3.3"])
    inventory = json.loads(report.inventory_path.read_text())
    coverage = json.loads(report.coverage_path.read_text())
    assert "items" in inventory
    assert any(
        item["citation_path"] == "canada/statute/I-3.3/2/1/a/i" for item in inventory["items"]
    )
    assert coverage["matched_count"] == coverage["provision_count"]


def test_extract_canada_acts_raises_when_no_provisions_found(tmp_path: Path) -> None:
    fetcher = _FakeFetcher({})
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="no Canada provisions"):
        extract_canada_acts(store, version="2026-05-06", fetcher=fetcher, only_acts=["MISSING"])
