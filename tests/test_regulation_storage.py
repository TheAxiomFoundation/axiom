"""Tests for regulation storage backend."""

from datetime import date


class TestRegulationStorageSchema:
    """Tests for regulation storage schema creation."""

    def test_creates_regulations_table(self, tmp_path):
        """Creates regulations table on init."""
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        assert "regulations" in storage.db.table_names()

    def test_creates_fts_table(self, tmp_path):
        """Creates FTS5 virtual table for full-text search."""
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        # Check FTS table exists
        tables = storage.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%fts%'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "regulations_fts" in table_names

    def test_creates_cfr_titles_table(self, tmp_path):
        """Creates cfr_titles metadata table."""
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        assert "cfr_titles" in storage.db.table_names()


class TestRegulationStore:
    """Tests for storing regulations."""

    def test_store_regulation(self, tmp_path):
        """Store a single regulation."""
        from axiom.models_regulation import CFRCitation, Regulation
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Earned income",
            authority="26 U.S.C. 32",
            source="T.D. 9954, 86 FR 12345",
            full_text="(a) In general. The earned income credit...",
            effective_date=date(2021, 1, 1),
        )

        storage.store_regulation(reg)

        # Verify stored
        result = storage.get_regulation(26, 1, "32-1")
        assert result is not None
        assert result.heading == "Earned income"

    def test_store_regulation_with_subsections(self, tmp_path):
        """Store regulation with subsections."""
        from axiom.models_regulation import CFRCitation, Regulation, RegulationSubsection
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        subsec = RegulationSubsection(
            id="a",
            heading="In general",
            text="(a) In general. The following rules apply...",
        )
        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Earned income",
            authority="26 U.S.C. 32",
            source="T.D. 9954",
            full_text="...",
            effective_date=date(2021, 1, 1),
            subsections=[subsec],
        )

        storage.store_regulation(reg)

        result = storage.get_regulation(26, 1, "32-1")
        assert len(result.subsections) == 1
        assert result.subsections[0].id == "a"

    def test_upsert_regulation(self, tmp_path):
        """Update existing regulation on re-store."""
        from axiom.models_regulation import CFRCitation, Regulation
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Original heading",
            authority="26 U.S.C. 32",
            source="T.D. 9954",
            full_text="Original text",
            effective_date=date(2021, 1, 1),
        )
        storage.store_regulation(reg)

        # Update
        reg.heading = "Updated heading"
        reg.full_text = "Updated text"
        storage.store_regulation(reg)

        result = storage.get_regulation(26, 1, "32-1")
        assert result.heading == "Updated heading"
        assert "Updated text" in result.full_text


class TestRegulationRetrieve:
    """Tests for retrieving regulations."""

    def test_get_regulation_not_found(self, tmp_path):
        """Returns None for non-existent regulation."""
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        result = storage.get_regulation(99, 99, "99-99")
        assert result is None

    def test_get_by_citation(self, tmp_path):
        """Get regulation by CFR citation object."""
        from axiom.models_regulation import CFRCitation, Regulation
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        reg = Regulation(
            citation=CFRCitation(title=26, part=1, section="32-1"),
            heading="Earned income",
            authority="26 U.S.C. 32",
            source="T.D. 9954",
            full_text="...",
            effective_date=date(2021, 1, 1),
        )
        storage.store_regulation(reg)

        citation = CFRCitation(title=26, part=1, section="32-1")
        result = storage.get_by_citation(citation)
        assert result is not None
        assert result.heading == "Earned income"


class TestRegulationSearch:
    """Tests for full-text search on regulations."""

    def test_search_by_text(self, tmp_path):
        """Search regulations by text content."""
        from axiom.models_regulation import CFRCitation, Regulation
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        # Add some regulations
        storage.store_regulation(
            Regulation(
                citation=CFRCitation(title=26, part=1, section="32-1"),
                heading="Earned income",
                authority="26 U.S.C. 32",
                source="T.D. 9954",
                full_text="The earned income credit applies to qualifying individuals...",
                effective_date=date(2021, 1, 1),
            )
        )
        storage.store_regulation(
            Regulation(
                citation=CFRCitation(title=26, part=1, section="24-1"),
                heading="Child tax credit",
                authority="26 U.S.C. 24",
                source="T.D. 9955",
                full_text="The child tax credit is available for qualifying children...",
                effective_date=date(2021, 1, 1),
            )
        )

        results = storage.search("earned income")
        assert len(results) >= 1
        assert any("32-1" in r.cfr_cite for r in results)

    def test_search_by_title(self, tmp_path):
        """Filter search by CFR title."""
        from axiom.models_regulation import CFRCitation, Regulation
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        # Add regulations in different titles
        storage.store_regulation(
            Regulation(
                citation=CFRCitation(title=26, part=1, section="32-1"),
                heading="Tax earned income",
                authority="26 U.S.C. 32",
                source="T.D. 9954",
                full_text="Tax rules for earned income...",
                effective_date=date(2021, 1, 1),
            )
        )
        storage.store_regulation(
            Regulation(
                citation=CFRCitation(title=7, part=273, section="1"),
                heading="SNAP eligibility",
                authority="7 U.S.C. 2011",
                source="72 FR 12345",
                full_text="SNAP earned income rules...",
                effective_date=date(2021, 1, 1),
            )
        )

        # Search only in title 26
        results = storage.search("earned income", title=26)
        assert len(results) >= 1
        assert all("26 CFR" in r.cfr_cite for r in results)


class TestCFRTitleMetadata:
    """Tests for CFR title metadata."""

    def test_list_cfr_titles(self, tmp_path):
        """List all ingested CFR titles."""
        from axiom.models_regulation import CFRCitation, Regulation
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        # Add a regulation
        storage.store_regulation(
            Regulation(
                citation=CFRCitation(title=26, part=1, section="32-1"),
                heading="Earned income",
                authority="26 U.S.C. 32",
                source="T.D. 9954",
                full_text="...",
                effective_date=date(2021, 1, 1),
            )
        )

        # Update title metadata
        storage.update_cfr_title_metadata(26, "Internal Revenue")

        titles = storage.list_cfr_titles()
        assert len(titles) >= 1
        assert titles[0]["number"] == 26
        assert titles[0]["name"] == "Internal Revenue"

    def test_count_regulations_by_title(self, tmp_path):
        """Count regulations in a title."""
        from axiom.models_regulation import CFRCitation, Regulation
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        # Add multiple regulations
        for section in ["32-1", "32-2", "24-1"]:
            storage.store_regulation(
                Regulation(
                    citation=CFRCitation(title=26, part=1, section=section),
                    heading=f"Section {section}",
                    authority="26 U.S.C. 7805",
                    source="T.D. 9954",
                    full_text="...",
                    effective_date=date(2021, 1, 1),
                )
            )

        count = storage.count_regulations(title=26)
        assert count == 3


class TestRegulationsByPart:
    """Tests for querying regulations by part."""

    def test_list_regulations_in_part(self, tmp_path):
        """List all regulations in a CFR part."""
        from axiom.models_regulation import CFRCitation, Regulation
        from axiom.storage.regulation import RegulationStorage

        db_path = tmp_path / "test.db"
        storage = RegulationStorage(db_path)

        # Add regulations in different parts
        storage.store_regulation(
            Regulation(
                citation=CFRCitation(title=26, part=1, section="32-1"),
                heading="Part 1, Section 32-1",
                authority="26 U.S.C. 32",
                source="T.D. 9954",
                full_text="...",
                effective_date=date(2021, 1, 1),
            )
        )
        storage.store_regulation(
            Regulation(
                citation=CFRCitation(title=26, part=31, section="3402-1"),
                heading="Part 31, Withholding",
                authority="26 U.S.C. 3402",
                source="T.D. 9955",
                full_text="...",
                effective_date=date(2021, 1, 1),
            )
        )

        # List only part 1
        regs = storage.list_regulations_in_part(26, 1)
        assert len(regs) == 1
        assert regs[0].citation.part == 1
