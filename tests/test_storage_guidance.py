"""Tests for the guidance storage module."""

from datetime import date

from axiom.models_guidance import (
    GuidanceSection,
    GuidanceType,
    RevenueProcedure,
)
from axiom.storage.guidance import GuidanceStorage


def _make_rev_proc(**kwargs):
    defaults = {
        "doc_number": "2023-34",
        "doc_type": GuidanceType.REV_PROC,
        "title": "Inflation Adjustments",
        "irb_citation": "2023-48 IRB",
        "published_date": date(2023, 11, 9),
        "full_text": "This procedure provides inflation adjustments...",
        "sections": [
            GuidanceSection(section_num=".01", text="Purpose"),
            GuidanceSection(section_num=".02", text="Scope"),
        ],
        "source_url": "https://www.irs.gov/irb/2023-48",
        "retrieved_at": date(2024, 1, 15),
    }
    defaults.update(kwargs)
    return RevenueProcedure(**defaults)


class TestGuidanceStorageInit:
    def test_init(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)
        assert storage.db_path == db_path

    def test_init_string_path(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        storage = GuidanceStorage(db_path=db_path)
        assert storage.db is not None


class TestGuidanceStorageStoreAndRetrieve:
    def test_store_revenue_procedure(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        rp = _make_rev_proc()
        storage.store_revenue_procedure(rp)

        # Verify stored
        row = storage.db.execute(
            "SELECT * FROM guidance_documents WHERE id = ?", ["rp-2023-34"]
        ).fetchone()
        assert row is not None

    def test_store_and_retrieve(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        rp = _make_rev_proc()
        storage.store_revenue_procedure(rp)
        result = storage.get_revenue_procedure("2023-34")

        assert result is not None
        assert result.doc_number == "2023-34"
        assert result.doc_type == GuidanceType.REV_PROC
        assert len(result.sections) == 2

    def test_get_nonexistent(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        # Create the table first
        storage.store_revenue_procedure(_make_rev_proc())

        result = storage.get_revenue_procedure("9999-99")
        assert result is None

    def test_store_twice_does_not_raise(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        rp1 = _make_rev_proc(title="Original Title")
        storage.store_revenue_procedure(rp1)

        rp2 = _make_rev_proc(title="Updated Title")
        # Should not raise even if inserting same doc again
        storage.store_revenue_procedure(rp2)

        result = storage.get_revenue_procedure("2023-34")
        assert result is not None


class TestGuidanceStorageDifferentTypes:
    def test_store_notice(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        rp = _make_rev_proc(
            doc_type=GuidanceType.NOTICE,
            doc_number="2024-01",
        )
        storage.store_revenue_procedure(rp)

        row = storage.db.execute(
            "SELECT * FROM guidance_documents WHERE id = ?", ["notice-2024-01"]
        ).fetchone()
        assert row is not None

    def test_store_rev_ruling(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        rp = _make_rev_proc(
            doc_type=GuidanceType.REV_RUL,
            doc_number="2024-05",
        )
        storage.store_revenue_procedure(rp)

        row = storage.db.execute(
            "SELECT * FROM guidance_documents WHERE id = ?", ["rr-2024-05"]
        ).fetchone()
        assert row is not None


class TestGuidanceStorageSectionConversion:
    def test_section_to_dict(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        section = GuidanceSection(
            section_num=".01",
            heading="Purpose",
            text="This provides...",
            children=[
                GuidanceSection(section_num=".01a", text="Sub-section"),
            ],
        )

        result = storage._section_to_dict(section)
        assert result["section_num"] == ".01"
        assert result["heading"] == "Purpose"
        assert len(result["children"]) == 1

    def test_dict_to_section(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        d = {
            "section_num": ".01",
            "heading": "Purpose",
            "text": "This provides...",
            "children": [
                {"section_num": ".01a", "text": "Sub-section", "children": []},
            ],
        }

        section = storage._dict_to_section(d)
        assert section.section_num == ".01"
        assert section.heading == "Purpose"
        assert len(section.children) == 1

    def test_dict_to_section_no_heading(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        d = {"section_num": "1", "text": "Content", "children": []}
        section = storage._dict_to_section(d)
        assert section.heading is None


class TestGuidanceStorageLinkToStatute:
    def test_link_guidance_to_statute(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = GuidanceStorage(db_path=db_path)

        # Create the refs table
        storage.db["guidance_statute_refs"].insert(
            {
                "guidance_id": "rp-2023-34",
                "statute_title": 26,
                "statute_section": "32",
                "ref_type": "implements",
                "excerpt": None,
            },
            ignore=True,
        )

        # Should not raise on duplicate
        storage.link_guidance_to_statute(
            "rp-2023-34", 26, "32", ref_type="implements"
        )
