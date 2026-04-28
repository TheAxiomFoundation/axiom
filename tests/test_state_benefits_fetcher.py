"""Tests for state benefits policy data fetcher."""

from unittest.mock import patch

import pytest

from axiom_corpus.fetchers.state_benefits import (
    CCDFFetcher,
    CCDFPolicyData,
    SNAPSUAFetcher,
    StateBenefitsFetcher,
    SUAData,
    TANFFetcher,
    TANFPolicyData,
)


class TestSNAPSUAFetcher:
    """Tests for SNAP Standard Utility Allowance fetcher."""

    @pytest.fixture
    def fetcher(self):
        """Create a SNAP SUA fetcher instance."""
        return SNAPSUAFetcher()

    def test_sua_url_construction(self, fetcher):
        """Test URL construction for SUA files by fiscal year."""
        url_fy25 = fetcher.get_sua_url(2025)
        assert "2025-01-01-SUA-Table-FY25.xlsx" in url_fy25

        url_fy24 = fetcher.get_sua_url(2024)
        assert "2024-01-01-SUA-Table-FY24.xlsx" in url_fy24

    def test_parse_sua_data(self, fetcher, tmp_path):
        """Test parsing SUA Excel data."""
        # Create a mock Excel file with expected structure
        pd = pytest.importorskip("pandas")

        df = pd.DataFrame(
            {
                "State": ["Alabama", "Alaska - Central", "Arizona"],
                "HCSUA": [458, 1000, 250],
                "CSUA": [100, 0, 100],
                "LUA": [75, 0, 50],
                "TUA": [47, 0, 40],
            }
        )
        mock_path = tmp_path / "test_sua.xlsx"
        df.to_excel(mock_path, index=False)

        # This will test actual parsing when implemented
        # data = fetcher.parse_sua_file(mock_path)
        # assert len(data) == 3
        # assert data[0].state == "Alabama"

    def test_sua_data_model(self):
        """Test SUAData model validation."""
        sua = SUAData(
            fiscal_year=2025,
            state="Alabama",
            state_fips="01",
            hcsua=458,  # Heating and Cooling SUA
            csua=100,  # Cooling SUA
            lua=75,  # Limited Utility Allowance
            tua=47,  # Telephone Utility Allowance
            smd_offset=8,  # Standard Medical Deduction offset
        )
        assert sua.fiscal_year == 2025
        assert sua.state == "Alabama"
        assert sua.hcsua == 458


class TestTANFFetcher:
    """Tests for TANF (Welfare Rules Database) fetcher."""

    @pytest.fixture
    def fetcher(self):
        """Create a TANF fetcher instance."""
        return TANFFetcher()

    def test_wrd_table_url_construction(self, fetcher):
        """Test URL construction for WRD table files."""
        url = fetcher.get_table_url("II.A.4", 2023)
        assert "II.A.4%202023.xlsx" in url
        assert "wrd.urban.org" in url

    def test_available_tables(self, fetcher):
        """Test listing available TANF policy tables."""
        tables = fetcher.list_available_tables()
        assert "II.A.4" in tables  # Max monthly benefit
        assert "I.E.1" in tables  # Income eligibility
        assert "I.C.1" in tables  # Asset limits

    def test_tanf_policy_data_model(self):
        """Test TANFPolicyData model validation."""
        policy = TANFPolicyData(
            data_year=2023,
            state="California",
            state_fips="06",
            max_monthly_benefit_family_3=714,
            max_income_eligibility_family_3=1432,
            asset_limit=2250,
            time_limit_months=48,
        )
        assert policy.state == "California"
        assert policy.max_monthly_benefit_family_3 == 714


class TestCCDFFetcher:
    """Tests for CCDF (Child Care) policy fetcher."""

    @pytest.fixture
    def fetcher(self):
        """Create a CCDF fetcher instance."""
        return CCDFFetcher()

    def test_ccdf_database_url(self, fetcher):
        """Test URL for CCDF database download."""
        url = fetcher.get_database_url()
        assert "ccdf.urban.org" in url
        assert ".xlsx" in url

    def test_ccdf_policy_data_model(self):
        """Test CCDFPolicyData model validation."""
        policy = CCDFPolicyData(
            data_year=2023,
            state="Texas",
            state_fips="48",
            income_eligibility_percent_smi=85,
            income_eligibility_percent_fpl=200,
            max_copay_percent=7,
            min_age_months=0,
            max_age_years=13,
        )
        assert policy.state == "Texas"
        assert policy.income_eligibility_percent_smi == 85


class TestStateBenefitsFetcher:
    """Tests for the unified state benefits fetcher."""

    @pytest.fixture
    def fetcher(self):
        """Create a unified state benefits fetcher."""
        return StateBenefitsFetcher()

    def test_fetch_all_snap_sua_years(self, fetcher, tmp_path):
        """Test fetching SNAP SUA data for multiple years."""
        mock_content = b"mock xlsx content"

        with patch.object(fetcher.snap, "fetch_sua_file", return_value=mock_content):
            with patch.object(fetcher.snap, "save_file") as mock_save:
                fetcher.fetch_snap_sua(
                    fiscal_years=[2024, 2025],
                    output_dir=tmp_path,
                )
                assert mock_save.call_count == 2

    def test_fetch_tanf_tables(self, fetcher, tmp_path):
        """Test fetching TANF policy tables."""
        mock_content = b"mock xlsx content"

        with patch.object(fetcher.tanf, "fetch_table", return_value=mock_content):
            with patch.object(fetcher.tanf, "save_file") as mock_save:
                fetcher.fetch_tanf_tables(
                    tables=["II.A.4", "I.E.1"],
                    years=[2023],
                    output_dir=tmp_path,
                )
                assert mock_save.call_count == 2

    def test_fetch_ccdf_database(self, fetcher, tmp_path):
        """Test fetching CCDF policies database."""
        mock_content = b"mock xlsx content"

        with patch.object(fetcher.ccdf, "fetch_database", return_value=mock_content):
            with patch.object(fetcher.ccdf, "save_file") as mock_save:
                fetcher.fetch_ccdf_database(output_dir=tmp_path)
                assert mock_save.call_count == 1


class TestFIPSCodes:
    """Tests for state FIPS code lookup."""

    def test_state_name_to_fips(self):
        """Test converting state names to FIPS codes."""
        from axiom_corpus.fetchers.state_benefits import state_name_to_fips

        assert state_name_to_fips("Alabama") == "01"
        assert state_name_to_fips("California") == "06"
        assert state_name_to_fips("Texas") == "48"
        assert state_name_to_fips("District of Columbia") == "11"

    def test_state_abbrev_to_fips(self):
        """Test converting state abbreviations to FIPS codes."""
        from axiom_corpus.fetchers.state_benefits import state_abbrev_to_fips

        assert state_abbrev_to_fips("AL") == "01"
        assert state_abbrev_to_fips("CA") == "06"
        assert state_abbrev_to_fips("TX") == "48"
        assert state_abbrev_to_fips("DC") == "11"
