"""Tests for the FastAPI application."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from axiom_corpus.api.main import (
    ReferencesResponse,
    SearchResponse,
    SearchResultResponse,
    SectionResponse,
    TitleResponse,
    create_app,
)
from axiom_corpus.models import Citation, SearchResult, Section, Subsection, TitleInfo


@pytest.fixture
def section():
    return Section(
        citation=Citation(title=26, section="32"),
        title_name="Internal Revenue Code",
        section_title="Earned income tax credit",
        text="Tax credit is allowed...",
        subsections=[
            Subsection(identifier="a", text="Allowance of credit"),
        ],
        source_url="https://uscode.house.gov",
        references_to=["26 USC 24"],
        referenced_by=["26 USC 1"],
        retrieved_at=date(2024, 1, 1),
    )


class TestSectionResponse:
    def test_from_section(self, section):
        response = SectionResponse.from_section(section)
        assert response.citation == "26 USC 32"
        assert response.title_name == "Internal Revenue Code"
        assert response.section_title == "Earned income tax credit"
        assert len(response.subsections) == 1
        assert response.references_to == ["26 USC 24"]
        assert response.referenced_by == ["26 USC 1"]


class TestSearchResultResponse:
    def test_from_result(self):
        result = SearchResult(
            citation=Citation(title=26, section="32"),
            section_title="Earned income tax credit",
            snippet="The earned income credit...",
            score=0.95,
        )
        response = SearchResultResponse.from_result(result)
        assert response.citation == "26 USC 32"
        assert response.score == 0.95


class TestSearchResponse:
    def test_create(self):
        response = SearchResponse(
            query="earned income",
            total=1,
            results=[
                SearchResultResponse(
                    citation="26 USC 32",
                    section_title="EITC",
                    snippet="...",
                    score=0.9,
                )
            ],
        )
        assert response.total == 1


class TestTitleResponse:
    def test_create(self):
        response = TitleResponse(
            number=26,
            name="Internal Revenue Code",
            section_count=1000,
            last_updated=date(2024, 1, 1),
            is_positive_law=True,
        )
        assert response.number == 26


class TestReferencesResponse:
    def test_create(self):
        response = ReferencesResponse(
            citation="26 USC 32",
            references_to=["26 USC 24"],
            referenced_by=["26 USC 1"],
        )
        assert response.citation == "26 USC 32"


class TestCreateApp:
    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_create_app(self, mock_archive):
        app = create_app(db_path=":memory:")
        assert app is not None
        assert app.title == "Axiom"

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_app_has_routes(self, mock_archive):
        app = create_app(db_path=":memory:")
        routes = [r.path for r in app.routes]
        assert "/" in routes
        assert "/v1/search" in routes


class TestAppEndpoints:
    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_root(self, mock_archive):
        from fastapi.testclient import TestClient

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Axiom"

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_search(self, mock_archive_cls):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.search.return_value = [
            SearchResult(
                citation=Citation(title=26, section="32"),
                section_title="EITC",
                snippet="earned income...",
                score=0.9,
            )
        ]

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/search?q=earned+income")
        assert response.status_code == 200
        data = response.json()
        assert data["query"] == "earned income"
        assert data["total"] == 1

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_get_section_found(self, mock_archive_cls, section):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.get.return_value = section

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/sections/26/32")
        assert response.status_code == 200

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_get_section_not_found(self, mock_archive_cls):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.get.return_value = None

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/sections/99/999")
        assert response.status_code == 404

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_get_subsection(self, mock_archive_cls, section):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.get.return_value = section

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/sections/26/32/a/1")
        assert response.status_code == 200

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_get_subsection_not_found(self, mock_archive_cls):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.get.return_value = None

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/sections/99/999/a")
        assert response.status_code == 404

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_get_by_citation(self, mock_archive_cls, section):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.get.return_value = section

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/citation/26 USC 32")
        assert response.status_code == 200

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_get_by_citation_not_found(self, mock_archive_cls):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.get.return_value = None

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/citation/26 USC 32")
        assert response.status_code == 404

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_get_references(self, mock_archive_cls):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.get_references.return_value = {
            "references_to": ["26 USC 24"],
            "referenced_by": ["26 USC 1"],
        }

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/references/26/32")
        assert response.status_code == 200
        data = response.json()
        assert data["citation"] == "26 USC 32"

    @patch("axiom_corpus.api.main.AxiomArchive")
    def test_list_titles(self, mock_archive_cls):
        from fastapi.testclient import TestClient

        mock_archive = MagicMock()
        mock_archive_cls.return_value = mock_archive
        mock_archive.list_titles.return_value = [
            TitleInfo(
                number=26,
                name="Internal Revenue Code",
                section_count=1000,
                last_updated=date(2024, 1, 1),
                is_positive_law=True,
            )
        ]

        app = create_app(db_path=":memory:")
        client = TestClient(app)
        response = client.get("/v1/titles")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["number"] == 26
