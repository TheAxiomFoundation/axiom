"""Tests for the CLI module.

Tests cover Click command group and individual commands using CliRunner.
All external dependencies (Arch, database, APIs) are mocked.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from click.testing import CliRunner

from atlas.cli import main
from atlas.models import Citation, SearchResult, Section, TitleInfo


def _make_section(**kwargs):
    defaults = {
        "citation": Citation(title=26, section="32"),
        "title_name": "Internal Revenue Code",
        "section_title": "Earned income tax credit",
        "text": "A tax credit is allowed under this section.",
        "subsections": [],
        "source_url": "https://uscode.house.gov",
        "retrieved_at": date(2024, 1, 1),
    }
    defaults.update(kwargs)
    return Section(**defaults)


class TestMainGroup:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "Atlas" in result.output or "atlas" in result.output.lower()


class TestGetCommand:
    @patch("atlas.cli.Arch")
    def test_get_found(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.get.return_value = _make_section()

        runner = CliRunner()
        result = runner.invoke(main, ["get", "26 USC 32"])
        assert result.exit_code == 0
        assert "Earned income" in result.output or "26" in result.output

    @patch("atlas.cli.Arch")
    def test_get_not_found(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.get.return_value = None

        runner = CliRunner()
        result = runner.invoke(main, ["get", "99 USC 999"])
        assert result.exit_code == 1

    @patch("atlas.cli.Arch")
    def test_get_json(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.get.return_value = _make_section()

        runner = CliRunner()
        result = runner.invoke(main, ["get", "26 USC 32", "--json"])
        assert result.exit_code == 0


class TestSearchCommand:
    @patch("atlas.cli.Arch")
    def test_search_with_results(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.search.return_value = [
            SearchResult(
                citation=Citation(title=26, section="32"),
                section_title="Earned income tax credit",
                snippet="earned income...",
                score=0.9,
            )
        ]

        runner = CliRunner()
        result = runner.invoke(main, ["search", "earned income"])
        assert result.exit_code == 0

    @patch("atlas.cli.Arch")
    def test_search_no_results(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.search.return_value = []

        runner = CliRunner()
        result = runner.invoke(main, ["search", "nonexistent query"])
        assert result.exit_code == 0
        assert "No results" in result.output

    @patch("atlas.cli.Arch")
    def test_search_with_title_filter(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.search.return_value = []

        runner = CliRunner()
        result = runner.invoke(main, ["search", "credit", "--title", "26"])
        assert result.exit_code == 0
        mock_arch.search.assert_called_once_with("credit", title=26, limit=10)


class TestTitlesCommand:
    @patch("atlas.cli.Arch")
    def test_titles_with_data(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.list_titles.return_value = [
            TitleInfo(
                number=26,
                name="Internal Revenue Code",
                section_count=2345,
                last_updated=date(2024, 1, 1),
                is_positive_law=True,
            )
        ]

        runner = CliRunner()
        result = runner.invoke(main, ["titles"])
        assert result.exit_code == 0
        assert "26" in result.output or "Internal Revenue" in result.output

    @patch("atlas.cli.Arch")
    def test_titles_empty(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.list_titles.return_value = []

        runner = CliRunner()
        result = runner.invoke(main, ["titles"])
        assert result.exit_code == 0
        assert "No titles" in result.output


class TestRefsCommand:
    @patch("atlas.cli.Arch")
    def test_refs(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.get_references.return_value = {
            "references_to": ["26 USC 24"],
            "referenced_by": ["26 USC 1"],
        }

        runner = CliRunner()
        result = runner.invoke(main, ["refs", "26 USC 32"])
        assert result.exit_code == 0


class TestServeCommand:
    def test_serve_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["serve", "--help"])
        assert result.exit_code == 0
        assert "host" in result.output.lower() or "port" in result.output.lower()


class TestEncodeCommand:
    def test_encode_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["encode", "--help"])
        assert result.exit_code == 0
        assert "citation" in result.output.lower() or "encode" in result.output.lower()


class TestValidateCommand:
    def test_validate_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["validate", "--help"])
        assert result.exit_code == 0


class TestDownloadCommand:
    def test_download_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["download", "--help"])
        assert result.exit_code == 0
        assert "title" in result.output.lower() or "download" in result.output.lower()


class TestIngestCommand:
    def test_ingest_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["ingest", "--help"])
        assert result.exit_code == 0


class TestValidateCommand:
    def test_validate_passing(self, tmp_path):
        rules_file = tmp_path / "rules.yaml"
        rules_file.write_text(
            "variable earned_income_credit {\n"
            '  reference "26 USC 32"\n'
            "  formula {\n"
            "    return 0\n"
            "  }\n"
            "}\n"
        )

        runner = CliRunner()
        result = runner.invoke(main, ["validate", str(tmp_path)])
        assert result.exit_code == 0

    def test_validate_no_definitions(self, tmp_path):
        rules_file = tmp_path / "rules.yaml"
        rules_file.write_text("// empty file\n")

        runner = CliRunner()
        result = runner.invoke(main, ["validate", str(tmp_path)])
        assert result.exit_code == 1

    def test_validate_with_warnings(self, tmp_path):
        rules_file = tmp_path / "rules.yaml"
        rules_file.write_text("parameter gov.irs.eitc.rate {}\n")

        runner = CliRunner()
        result = runner.invoke(main, ["validate", str(tmp_path)])
        # Parameters only, no formula, no reference -> warnings but should pass
        assert result.exit_code == 0

    def test_validate_file_not_found(self, tmp_path):
        tmp_path / "rules.yaml"
        # File doesn't exist, but tmp_path does exist as dir
        runner = CliRunner()
        result = runner.invoke(main, ["validate", str(tmp_path)])
        assert result.exit_code == 1


class TestDbOption:
    @patch("atlas.cli.Arch")
    def test_custom_db(self, mock_arch_cls):
        mock_arch = MagicMock()
        mock_arch_cls.return_value = mock_arch
        mock_arch.list_titles.return_value = []

        runner = CliRunner()
        result = runner.invoke(main, ["--db", "custom.db", "titles"])
        assert result.exit_code == 0
        mock_arch_cls.assert_called_once_with(db_path="custom.db")
