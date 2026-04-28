"""Tests for data models."""

import pytest

from axiom_corpus.models import Citation


class TestCitation:
    """Tests for Citation parsing and formatting."""

    def test_parse_simple_citation(self):
        """Parse simple citation like '26 USC 32'."""
        cite = Citation.from_string("26 USC 32")
        assert cite.title == 26
        assert cite.section == "32"
        assert cite.subsection is None

    def test_parse_citation_with_subsection(self):
        """Parse citation with subsection like '26 USC 32(a)'."""
        cite = Citation.from_string("26 USC 32(a)")
        assert cite.title == 26
        assert cite.section == "32"
        assert cite.subsection == "a"

    def test_parse_citation_with_nested_subsections(self):
        """Parse citation with nested subsections like '26 USC 32(a)(1)(A)'."""
        cite = Citation.from_string("26 USC 32(a)(1)(A)")
        assert cite.title == 26
        assert cite.section == "32"
        assert cite.subsection == "a/1/A"

    def test_parse_citation_with_periods(self):
        """Parse citation with U.S.C. format."""
        cite = Citation.from_string("26 U.S.C. 32")
        assert cite.title == 26
        assert cite.section == "32"

    def test_parse_citation_with_section_symbol(self):
        """Parse citation with section symbol."""
        cite = Citation.from_string("26 USC § 32")
        assert cite.title == 26
        assert cite.section == "32"

    def test_parse_section_with_letter(self):
        """Parse section numbers with letters like '32A'."""
        cite = Citation.from_string("26 USC 32A")
        assert cite.title == 26
        assert cite.section == "32A"

    def test_usc_cite_format(self):
        """Test USC citation string output."""
        cite = Citation(title=26, section="32", subsection="a/1")
        assert cite.usc_cite == "26 USC 32(a)(1)"

    def test_path_format(self):
        """Test filesystem path format."""
        cite = Citation(title=26, section="32", subsection="a/1")
        assert cite.path == "statute/26/32/a/1"

    def test_path_format_no_subsection(self):
        """Test path format without subsection."""
        cite = Citation(title=26, section="32")
        assert cite.path == "statute/26/32"

    def test_invalid_citation_raises(self):
        """Invalid citation string raises ValueError."""
        with pytest.raises(ValueError):
            Citation.from_string("not a citation")


class TestSubsectionFullText:
    """Tests for Subsection.full_text() recursive text aggregation."""

    def test_leaf_returns_own_text(self):
        """Leaf subsection returns its own text."""
        from axiom_corpus.models import Subsection

        sub = Subsection(identifier="a", text="The credit shall be allowed.")
        assert sub.full_text() == "The credit shall be allowed."

    def test_leaf_with_heading(self):
        """Leaf with heading includes heading prefix."""
        from axiom_corpus.models import Subsection

        sub = Subsection(
            identifier="a", heading="Allowance of credit", text="The credit shall be allowed."
        )
        result = sub.full_text()
        assert result == "(a) Allowance of credit\nThe credit shall be allowed."

    def test_nested_aggregates_descendants(self):
        """Nested subsection aggregates all descendants in order."""
        from axiom_corpus.models import Subsection

        child1 = Subsection(identifier="1", text="First child text.")
        child2 = Subsection(identifier="2", text="Second child text.")
        parent = Subsection(
            identifier="b",
            heading="Limitations",
            text="In general.",
            children=[child1, child2],
        )
        result = parent.full_text()
        assert "(b) Limitations" in result
        assert "In general." in result
        assert "First child text." in result
        assert "Second child text." in result
        # Order: heading, own text, then children
        lines = result.split("\n")
        assert lines[0] == "(b) Limitations"
        assert lines[1] == "In general."
        assert lines[2] == "First child text."
        assert lines[3] == "Second child text."

    def test_empty_text_with_children(self):
        """Subsection with empty text but children still works."""
        from axiom_corpus.models import Subsection

        child = Subsection(identifier="1", text="Child text.")
        parent = Subsection(identifier="c", text="", children=[child])
        result = parent.full_text()
        assert "Child text." in result

    def test_deeply_nested(self):
        """Three levels deep aggregates correctly."""
        from axiom_corpus.models import Subsection

        grandchild = Subsection(identifier="A", text="Grandchild.")
        child = Subsection(identifier="1", text="Child.", children=[grandchild])
        parent = Subsection(
            identifier="a", heading="Top", text="Parent.", children=[child]
        )
        result = parent.full_text()
        assert "Parent." in result
        assert "Child." in result
        assert "Grandchild." in result


class TestSectionGetSubsection:
    """Tests for Section.get_subsection() tree walking."""

    def _make_section(self):
        """Build a test section with known subsection tree."""
        from axiom_corpus.models import Citation, Section, Subsection

        return Section(
            citation=Citation(title=26, section="32"),
            title_name="Internal Revenue Code",
            section_title="Earned income",
            text="Full section text.",
            subsections=[
                Subsection(
                    identifier="a",
                    heading="Allowance of credit",
                    text="Credit shall be allowed.",
                ),
                Subsection(
                    identifier="b",
                    heading="Percentages and amounts",
                    text="In general.",
                    children=[
                        Subsection(
                            identifier="1",
                            heading="Percentages",
                            text="The credit percentage.",
                            children=[
                                Subsection(
                                    identifier="A",
                                    text="1 qualifying child: 34 percent.",
                                ),
                                Subsection(
                                    identifier="B",
                                    text="2 or more: 40 percent.",
                                ),
                            ],
                        ),
                        Subsection(
                            identifier="2",
                            heading="Amounts",
                            text="The earned income amount.",
                        ),
                    ],
                ),
                Subsection(identifier="c", heading="Definitions", text="Definitions."),
            ],
            source_url="https://uscode.house.gov",
            retrieved_at="2025-01-01",
        )

    def test_top_level(self):
        """Get top-level subsection 'a'."""
        section = self._make_section()
        sub = section.get_subsection("a")
        assert sub is not None
        assert sub.identifier == "a"
        assert sub.heading == "Allowance of credit"

    def test_nested_path(self):
        """Get nested subsection 'b/1/A'."""
        section = self._make_section()
        sub = section.get_subsection("b/1/A")
        assert sub is not None
        assert sub.identifier == "A"
        assert "34 percent" in sub.text

    def test_nonexistent_returns_none(self):
        """Nonexistent path returns None."""
        section = self._make_section()
        assert section.get_subsection("z") is None
        assert section.get_subsection("b/9") is None
        assert section.get_subsection("b/1/Z") is None

    def test_empty_path_returns_none(self):
        """Empty path returns None."""
        section = self._make_section()
        assert section.get_subsection("") is None

    def test_get_subsection_text(self):
        """get_subsection_text returns full recursive text."""
        section = self._make_section()
        text = section.get_subsection_text("b/1")
        assert text is not None
        assert "(1) Percentages" in text
        assert "The credit percentage." in text
        assert "34 percent" in text
        assert "40 percent" in text

    def test_get_subsection_text_missing(self):
        """get_subsection_text returns None for missing path."""
        section = self._make_section()
        assert section.get_subsection_text("z") is None
