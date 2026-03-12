"""Tests for state_orchestrator — coordinates state statute ingestion."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import date

from atlas.ingest.state_orchestrator import StateOrchestrator
from atlas.models import Section, Subsection, Citation


def _make_section(section_num="5747.01", state="oh") -> Section:
    return Section(
        citation=Citation(title=0, section=f"{state.upper()}-{section_num}"),
        title_name="Test State Code",
        section_title="Test Section",
        text="Test text content",
        source_url=f"https://example.com/section-{section_num}",
        retrieved_at=date(2025, 1, 1),
    )


class TestIngestStateLocal:
    def test_ingest_state_local_parses_files(self, tmp_path):
        state_dir = tmp_path / "statutes" / "us-oh"
        state_dir.mkdir(parents=True)
        (state_dir / "ohio-revised-code_section-5747.01.html").write_text(
            "<html>test</html>"
        )
        (state_dir / "ohio-revised-code_section-5747.02.html").write_text(
            "<html>test2</html>"
        )
        mock_uploader = MagicMock()
        mock_uploader.upsert_all.return_value = 2
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)
        with patch.object(
            orch,
            "_parse_local_file",
            side_effect=[
                _make_section("5747.01"),
                _make_section("5747.02"),
            ],
        ):
            result = orch.ingest_state("oh", mode="local")
        assert result > 0
        mock_uploader.upsert_all.assert_called_once()

    def test_ingest_state_no_data(self, tmp_path):
        mock_uploader = MagicMock()
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)
        result = orch.ingest_state("zz", mode="local")
        assert result == 0


class TestIngestAllStates:
    def test_iterates_states_with_data(self, tmp_path):
        for state in ["oh", "ak", "ca"]:
            state_dir = tmp_path / "statutes" / f"us-{state}"
            state_dir.mkdir(parents=True)
            (state_dir / "test_section.html").write_text("<html>test</html>")
        mock_uploader = MagicMock()
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)
        with patch.object(orch, "ingest_state", return_value=10) as mock_ingest:
            result = orch.ingest_all_states(mode="local")
        assert mock_ingest.call_count == 3
        states_called = {call.args[0] for call in mock_ingest.call_args_list}
        assert states_called == {"oh", "ak", "ca"}


class TestErrorResilience:
    def test_error_in_one_state_continues(self, tmp_path):
        for state in ["oh", "ak"]:
            state_dir = tmp_path / "statutes" / f"us-{state}"
            state_dir.mkdir(parents=True)
            (state_dir / "test.html").write_text("<html>test</html>")
        mock_uploader = MagicMock()
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)

        def side_effect(state, **kwargs):
            if state == "oh":
                raise RuntimeError("OH converter failed")
            return 5

        with patch.object(orch, "ingest_state", side_effect=side_effect):
            result = orch.ingest_all_states(mode="local")
        assert "ak" in result
        assert result["ak"] == 5


class TestParseLocalFile:
    def test_parse_oh_filename_pattern(self, tmp_path):
        orch = StateOrchestrator(data_dir=tmp_path)
        section_num = orch._extract_section_number(
            "ohio-revised-code_section-5747.01.html", "oh"
        )
        assert section_num == "5747.01"

    def test_parse_ak_filename_pattern(self, tmp_path):
        orch = StateOrchestrator(data_dir=tmp_path)
        section_num = orch._extract_section_number(
            "statutes.asp_01-05-006.html", "ak"
        )
        assert section_num == "01.05.006"

    def test_parse_al_filename_pattern(self, tmp_path):
        """AL: code-of-alabama_section-1-1-1.1.html → 1-1-1.1"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "code-of-alabama_section-1-1-1.1.html", "al"
        ) == "1-1-1.1"

    def test_parse_az_filename_pattern(self, tmp_path):
        """AZ: viewdocument_...ars_20_00259.htm.html → 20-00259"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "viewdocument_docName-www.azleg.gov_ars_20_00259.htm.html", "az"
        ) == "20-00259"

    def test_parse_mn_filename_pattern(self, tmp_path):
        """MN: statutes_cite_105.63.html → 105.63"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "statutes_cite_105.63.html", "mn"
        ) == "105.63"

    def test_parse_wa_filename_pattern(self, tmp_path):
        """WA: RCW_default.aspx_cite-10.89.html → 10.89"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "RCW_default.aspx_cite-10.89.html", "wa"
        ) == "10.89"

    def test_parse_wi_filename_pattern(self, tmp_path):
        """WI: document_statutes_100.52.html → 100.52"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "document_statutes_100.52.html", "wi"
        ) == "100.52"

    def test_parse_ne_filename_pattern(self, tmp_path):
        """NE: laws_statutes.php_statute-13-2501.html → 13-2501"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "laws_statutes.php_statute-13-2501.html", "ne"
        ) == "13-2501"

    def test_parse_ne_print_variant(self, tmp_path):
        """NE: laws_statutes.php_statute-44-911_print-true.html → 44-911"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "laws_statutes.php_statute-44-911_print-true.html", "ne"
        ) == "44-911"

    def test_parse_tx_filename_pattern(self, tmp_path):
        """TX: Docs_AG_htm_AG.1.htm_1-001.html → AG.1-1-001"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "Docs_AG_htm_AG.1.htm_1-001.html", "tx"
        ) == "AG.1-1-001"

    def test_parse_me_filename_pattern(self, tmp_path):
        """ME: statutes_1_title1ch0sec0.html.html → 1-0"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "statutes_1_title1ch0sec0.html.html", "me"
        )
        assert result == "1-0"

    def test_parse_ri_filename_pattern(self, tmp_path):
        """RI: Statutes_TITLE1_INDEX.HTM.html → TITLE1"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "Statutes_TITLE1_INDEX.HTM.html", "ri"
        )
        assert result == "TITLE1"

    def test_parse_sc_filename_pattern(self, tmp_path):
        """SC: code_t02c003.php.html → 2-3"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "code_t02c003.php.html", "sc"
        )
        assert result == "2-3"

    def test_parse_la_filename_pattern(self, tmp_path):
        """LA: Legis_Laws_Toc.aspx_folder-1.html → 1"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "Legis_Laws_Toc.aspx_folder-1.html", "la"
        )
        assert result == "1"

    def test_parse_nh_filename_pattern(self, tmp_path):
        """NH: rsa_html_NHTOC_NHTOC-I.htm.html → I"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "rsa_html_NHTOC_NHTOC-I.htm.html", "nh"
        )
        assert result == "I"

    def test_parse_nv_filename_pattern(self, tmp_path):
        """NV: NRS_NRS-000.html.html → 000"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "NRS_NRS-000.html.html", "nv"
        )
        assert result == "000"

    def test_parse_pa_filename_pattern(self, tmp_path):
        """PA: statutes_consolidated_view-statute_txtType-HTM_ttl-10.html → 10"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "statutes_consolidated_view-statute_txtType-HTM_ttl-10.html", "pa"
        )
        assert result == "10"

    def test_skips_converters_without_parse_section_html(self, tmp_path):
        """States without _parse_section_html should be skipped gracefully."""
        state_dir = tmp_path / "statutes" / "us-de"
        state_dir.mkdir(parents=True)
        (state_dir / "title10_c009_index.html.html").write_text("<html>test</html>")
        mock_uploader = MagicMock()
        mock_uploader.upsert_all.return_value = 0
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)
        # Should not crash, just return 0
        result = orch.ingest_state("de", mode="local")
        assert result == 0


class TestGetAvailableStates:
    def test_lists_states_from_data_dirs(self, tmp_path):
        for state in ["oh", "ak", "ca"]:
            (tmp_path / "statutes" / f"us-{state}").mkdir(parents=True)
        (tmp_path / "statutes" / "state").mkdir(parents=True)
        orch = StateOrchestrator(data_dir=tmp_path)
        states = orch.get_available_states()
        assert set(states) == {"oh", "ak", "ca"}
