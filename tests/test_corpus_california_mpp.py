"""Adapter shape tests for the CDSS MPP CalFresh extractor."""

from axiom_corpus.corpus.california_mpp import _subsection_provision
from axiom_corpus.parsers.us_ca.regulations import MppSubsection


def _make_subsection(num: str, title: str, body: str) -> MppSubsection:
    return MppSubsection(num=num, title=title, body=body, parent_num="63-503")


def _provision(sub: MppSubsection):
    return _subsection_provision(
        sub,
        parent_citation_path="us-ca/regulation/mpp/63-503",
        ordinal=1,
        run_id="2026-05-12-cdss-mpp-calfresh",
        source_as_of="2026-05-12",
        expression_date="2026-05-12",
        source_url="https://example/fsman06.docx",
        source_path="data/corpus/sources/.../fsman06.docx",
        source_id="fsman06.docx",
    )


def test_subsection_with_separate_body_preserves_body_unchanged():
    """When the DOCX subsection has a header line + follow-on paragraphs, the
    parser populates both fields. The adapter must preserve body verbatim
    (no title duplication) so multi-paragraph subsections stay clean."""
    sub = _make_subsection(
        num="131",
        title="Using a calendar or fiscal month, households shall receive benefits prorated.",
        body="(a) Refer to Handbook Section 63-1101 for Reciprocal Table.",
    )
    prov = _provision(sub)
    assert prov.body == "(a) Refer to Handbook Section 63-1101 for Reciprocal Table."
    assert prov.heading.startswith(".131 Using a calendar or fiscal month")


def test_subsection_with_empty_body_falls_back_to_title():
    """Single-paragraph subsections (most of MPP §63) have the entire rule
    text captured as title with body empty. The encoder pipeline reads
    `body` to ground citation excerpts, so the adapter must surface the
    rule text in `body` rather than leaving it null."""
    sub = _make_subsection(
        num="132",
        title=(
            "After determining the prorated allotment, the CWD shall round "
            "the product down to the nearest lower whole dollar. If the "
            "computation results in an allotment of less than $10, then no "
            "issuance shall be made for the whole month."
        ),
        body="",
    )
    prov = _provision(sub)
    assert prov.body is not None
    assert "less than $10" in prov.body
    # Heading still carries the section marker + title for display continuity.
    assert prov.heading.startswith(".132 After determining")


def test_subsection_with_both_fields_empty_emits_null_body():
    """Defensive: a fully empty subsection should still emit a record with
    body=None rather than an empty string."""
    sub = _make_subsection(num="999", title="", body="")
    prov = _provision(sub)
    assert prov.body is None
