"""Tests for storage backends."""

import tempfile
from datetime import date
from pathlib import Path

import pytest

from axiom.models import Citation, Section, Subsection
from axiom.storage.sqlite import SQLiteStorage


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)
    yield db_path
    db_path.unlink(missing_ok=True)


@pytest.fixture
def storage(temp_db):
    """Create a SQLite storage instance."""
    return SQLiteStorage(temp_db)


@pytest.fixture
def sample_section():
    """Create a sample section for testing."""
    return Section(
        citation=Citation(title=26, section="32"),
        title_name="Internal Revenue Code",
        section_title="Earned income",
        text="In the case of an eligible individual, there shall be allowed as a credit...",
        subsections=[
            Subsection(
                identifier="a",
                heading="Allowance of credit",
                text="In the case of an eligible individual...",
                children=[
                    Subsection(
                        identifier="1",
                        heading="In general",
                        text="The credit shall be...",
                        children=[],
                    )
                ],
            )
        ],
        references_to=["26 USC 24", "26 USC 152"],
        source_url="https://uscode.house.gov/view.xhtml?req=26+USC+32",
        retrieved_at=date.today(),
        uslm_id="/us/usc/t26/s32",
    )


class TestSQLiteStorage:
    """Tests for SQLite storage backend."""

    def test_store_and_retrieve_section(self, storage, sample_section):
        """Store a section and retrieve it."""
        storage.store_section(sample_section)
        retrieved = storage.get_section(26, "32")

        assert retrieved is not None
        assert retrieved.citation.title == 26
        assert retrieved.citation.section == "32"
        assert retrieved.section_title == "Earned income"
        assert "eligible individual" in retrieved.text

    def test_retrieve_nonexistent_section(self, storage):
        """Retrieving nonexistent section returns None."""
        result = storage.get_section(99, "999")
        assert result is None

    def test_search_returns_results(self, storage, sample_section):
        """Search finds stored sections."""
        storage.store_section(sample_section)
        results = storage.search("earned income")

        assert len(results) > 0
        assert results[0].citation.section == "32"

    def test_search_empty_query(self, storage, sample_section):
        """Search with no matches returns empty list."""
        storage.store_section(sample_section)
        results = storage.search("xyznonexistent")

        assert results == []

    def test_search_filter_by_title(self, storage, sample_section):
        """Search can filter by title."""
        storage.store_section(sample_section)

        # Should find in title 26
        results = storage.search("earned income", title=26)
        assert len(results) > 0

        # Should not find in title 42
        results = storage.search("earned income", title=42)
        assert len(results) == 0

    def test_cross_references_stored(self, storage, sample_section):
        """Cross-references are stored and retrievable."""
        storage.store_section(sample_section)

        refs_to = storage.get_references_to(26, "32")
        assert "26 USC 24" in refs_to
        assert "26 USC 152" in refs_to

    def test_upsert_updates_existing(self, storage, sample_section):
        """Storing same section twice updates it."""
        storage.store_section(sample_section)

        # Modify and store again
        sample_section.section_title = "Updated title"
        storage.store_section(sample_section)

        retrieved = storage.get_section(26, "32")
        assert retrieved.section_title == "Updated title"

    def test_subsections_preserved(self, storage, sample_section):
        """Nested subsections are preserved."""
        storage.store_section(sample_section)
        retrieved = storage.get_section(26, "32")

        assert len(retrieved.subsections) == 1
        assert retrieved.subsections[0].identifier == "a"
        assert len(retrieved.subsections[0].children) == 1
        assert retrieved.subsections[0].children[0].identifier == "1"
