"""Fetchers for state benefit policy data.

This module provides fetchers for downloading and parsing state-level benefit
policy data from official government sources:

- SNAP Standard Utility Allowances (SUAs) from FNS/USDA
- TANF policies from the Welfare Rules Database (Urban Institute)
- CCDF child care policies from the CCDF Policies Database (Urban Institute)

Data Sources
------------

**SNAP Standard Utility Allowances (SUAs)**

Primary source: FNS/USDA annual tables
- URL: https://www.fns.usda.gov/snap/eligibility/deduction/standard-utility-allowances
- Authority: States submit SUAs to FNS as part of State Plan of Operations
- Regulation: 7 CFR 273.9(d)(6)(iii) - requires annual review by states
- Format: Excel spreadsheet with all states/territories

The FNS tables ARE the authoritative federal aggregation of state-reported values.
State-level primary sources (administrative code, policy manuals) are typically
not publicly accessible in scrapable format.

Allowance types:
- HCSUA: Heating and Cooling Standard Utility Allowance
- CSUA: Cooling-only Standard Utility Allowance
- LUA: Limited Utility Allowance (2+ utilities, excludes heating/cooling)
- TUA: Telephone Utility Allowance

Some states vary SUAs by:
- Household size: AZ, GU, HI, NC, TN, VA
- Geographic location: AK, NY

**TANF (Temporary Assistance for Needy Families)**

Source: Urban Institute Welfare Rules Database (WRD)
- URL: https://wrd.urban.org
- Coverage: 1996-present, all 50 states + DC
- Tables: Income eligibility, asset limits, benefit levels, time limits

**CCDF (Child Care and Development Fund)**

Source: Urban Institute CCDF Policies Database
- URL: https://ccdf.urban.org
- Coverage: All 50 states + DC + territories
- Topics: Income eligibility, copayments, reimbursement rates
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx

# FIPS codes for US states and territories
STATE_FIPS = {
    "Alabama": "01",
    "Alaska": "02",
    "Arizona": "04",
    "Arkansas": "05",
    "California": "06",
    "Colorado": "08",
    "Connecticut": "09",
    "Delaware": "10",
    "District of Columbia": "11",
    "Florida": "12",
    "Georgia": "13",
    "Hawaii": "15",
    "Idaho": "16",
    "Illinois": "17",
    "Indiana": "18",
    "Iowa": "19",
    "Kansas": "20",
    "Kentucky": "21",
    "Louisiana": "22",
    "Maine": "23",
    "Maryland": "24",
    "Massachusetts": "25",
    "Michigan": "26",
    "Minnesota": "27",
    "Mississippi": "28",
    "Missouri": "29",
    "Montana": "30",
    "Nebraska": "31",
    "Nevada": "32",
    "New Hampshire": "33",
    "New Jersey": "34",
    "New Mexico": "35",
    "New York": "36",
    "North Carolina": "37",
    "North Dakota": "38",
    "Ohio": "39",
    "Oklahoma": "40",
    "Oregon": "41",
    "Pennsylvania": "42",
    "Rhode Island": "44",
    "South Carolina": "45",
    "South Dakota": "46",
    "Tennessee": "47",
    "Texas": "48",
    "Utah": "49",
    "Vermont": "50",
    "Virginia": "51",
    "Washington": "53",
    "West Virginia": "54",
    "Wisconsin": "55",
    "Wyoming": "56",
    # Territories
    "Guam": "66",
    "Puerto Rico": "72",
    "Virgin Islands": "78",
}

STATE_ABBREV_TO_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "GU": "66",
    "PR": "72",
    "VI": "78",
}


def state_name_to_fips(state_name: str) -> str:
    """Convert a state name to its FIPS code.

    Args:
        state_name: Full state name (e.g., "California")

    Returns:
        Two-digit FIPS code (e.g., "06")

    Raises:
        KeyError: If state name is not recognized
    """
    # Handle regional variations (e.g., "Alaska - Central")
    base_state = state_name.split(" - ")[0].strip()
    return STATE_FIPS[base_state]


def state_abbrev_to_fips(abbrev: str) -> str:
    """Convert a state abbreviation to its FIPS code.

    Args:
        abbrev: Two-letter state abbreviation (e.g., "CA")

    Returns:
        Two-digit FIPS code (e.g., "06")

    Raises:
        KeyError: If abbreviation is not recognized
    """
    return STATE_ABBREV_TO_FIPS[abbrev.upper()]


@dataclass
class SUAData:
    """SNAP Standard Utility Allowance data for a state.

    These allowances are used to calculate shelter deductions in SNAP
    eligibility determination.
    """

    fiscal_year: int
    state: str
    state_fips: str
    hcsua: int  # Heating and Cooling Standard Utility Allowance
    csua: int  # Cooling Standard Utility Allowance (no heating)
    lua: int  # Limited Utility Allowance
    tua: int  # Telephone Utility Allowance
    smd_offset: int = 0  # Standard Medical Deduction offset (some states)
    region: str | None = None  # For states with regional variations (AK, HI)
    household_size: str | None = None  # For states with HH size variations


@dataclass
class TANFPolicyData:
    """TANF (Temporary Assistance for Needy Families) policy data for a state.

    Data from the Welfare Rules Database maintained by Urban Institute.
    """

    data_year: int
    state: str
    state_fips: str
    max_monthly_benefit_family_3: int | None = None  # Max benefit for 3-person family
    max_income_eligibility_family_3: int | None = None  # Max income for eligibility
    asset_limit: int | None = None  # Asset limit for eligibility
    time_limit_months: int | None = None  # Federal/state time limit
    earned_income_disregard: str | None = None  # Earned income disregard policy
    sanction_policy: str | None = None  # Work requirement sanction policy


@dataclass
class CCDFPolicyData:
    """CCDF (Child Care and Development Fund) policy data for a state.

    Data from the CCDF Policies Database maintained by Urban Institute.
    """

    data_year: int
    state: str
    state_fips: str
    income_eligibility_percent_smi: int | None = None  # % of state median income
    income_eligibility_percent_fpl: int | None = None  # % of federal poverty level
    max_copay_percent: float | None = None  # Max copay as % of income
    min_age_months: int | None = None  # Minimum child age for eligibility
    max_age_years: int | None = None  # Maximum child age for eligibility


class SNAPSUAFetcher:
    """Fetcher for SNAP Standard Utility Allowance data from FNS/USDA.

    The FNS publishes annual SUA tables as Excel files at:
    https://www.fns.usda.gov/snap/eligibility/deduction/standard-utility-allowances
    """

    BASE_URL = "https://www.fns.usda.gov/sites/default/files/resource-files"

    def __init__(self, timeout: float = 60.0):
        """Initialize the SNAP SUA fetcher.

        Args:
            timeout: HTTP request timeout in seconds
        """
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "Axiom/1.0 (Policy Research; +https://axiom-foundation.org)"},
        )

    def get_sua_url(self, fiscal_year: int) -> str:
        """Get the URL for a specific fiscal year's SUA table.

        Args:
            fiscal_year: Fiscal year (e.g., 2025 for FY2025)

        Returns:
            URL to the Excel file
        """
        # FNS uses format: 2025-01-01-SUA-Table-FY25.xlsx
        fy_short = str(fiscal_year)[-2:]
        return f"{self.BASE_URL}/{fiscal_year}-01-01-SUA-Table-FY{fy_short}.xlsx"

    def fetch_sua_file(self, fiscal_year: int) -> bytes:
        """Fetch the SUA Excel file for a fiscal year.

        Args:
            fiscal_year: Fiscal year to fetch

        Returns:
            Excel file content as bytes
        """
        url = self.get_sua_url(fiscal_year)
        response = self.client.get(url)
        response.raise_for_status()
        return response.content

    def save_file(self, content: bytes, output_path: Path) -> None:
        """Save file content to disk.

        Args:
            content: File content as bytes
            output_path: Path to save the file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)

    def parse_sua_file(self, file_path: Path) -> list[SUAData]:
        """Parse an SUA Excel file into structured data.

        Args:
            file_path: Path to the Excel file

        Returns:
            List of SUAData objects, one per state/region
        """
        import pandas as pd  # pragma: no cover

        # Read the Excel file, skipping header rows
        df = pd.read_excel(file_path, header=2)  # pragma: no cover

        # Extract fiscal year from filename
        fy_match = file_path.stem.split("FY")[-1]  # pragma: no cover
        fiscal_year = 2000 + int(fy_match[:2])  # pragma: no cover

        results = []  # pragma: no cover
        for _, row in df.iterrows():  # pragma: no cover
            state_col = row.iloc[0]  # pragma: no cover
            if pd.isna(state_col) or not isinstance(state_col, str):  # pragma: no cover
                continue  # pragma: no cover

            # Parse state name and optional region/household size
            state_name = state_col.strip()  # pragma: no cover
            region = None  # pragma: no cover
            household_size = None  # pragma: no cover

            if " - " in state_name:  # pragma: no cover
                parts = state_name.split(" - ")  # pragma: no cover
                state_name = parts[0].strip()  # pragma: no cover
                modifier = parts[1].strip()  # pragma: no cover
                # Check if it's a region (Alaska) or HH size (Arizona)
                if "member" in modifier.lower():  # pragma: no cover
                    household_size = modifier  # pragma: no cover
                else:
                    region = modifier  # pragma: no cover

            # Get FIPS code
            try:  # pragma: no cover
                state_fips = state_name_to_fips(state_name)  # pragma: no cover
            except KeyError:  # pragma: no cover
                continue  # Skip non-state rows  # pragma: no cover

            # Extract SUA values from columns
            # Column order: State, HCSUA, CSUA, LUA, TUA, [other cols], SMD Offset
            sua = SUAData(  # pragma: no cover
                fiscal_year=fiscal_year,
                state=state_name,
                state_fips=state_fips,
                hcsua=int(row.iloc[1]) if pd.notna(row.iloc[1]) else 0,
                csua=int(row.iloc[2]) if pd.notna(row.iloc[2]) else 0,
                lua=int(row.iloc[3]) if pd.notna(row.iloc[3]) else 0,
                tua=int(row.iloc[4]) if pd.notna(row.iloc[4]) else 0,
                smd_offset=int(row.iloc[-1]) if pd.notna(row.iloc[-1]) else 0,
                region=region,
                household_size=household_size,
            )
            results.append(sua)  # pragma: no cover

        return results  # pragma: no cover

    def close(self):
        """Close the HTTP client."""
        self.client.close()


class TANFFetcher:
    """Fetcher for TANF policy data from the Welfare Rules Database.

    The Urban Institute maintains the WRD with annual policy tables at:
    https://wrd.urban.org
    """

    BASE_URL = "https://wrd.urban.org/sites/default/files/documents"

    # Key TANF policy tables
    AVAILABLE_TABLES = {
        "I.C.1": "Asset limits for applicants",
        "I.E.1": "Income eligibility (gross income test)",
        "I.E.2": "Income eligibility (net income test)",
        "II.A.1": "Earned income disregards",
        "II.A.4": "Maximum monthly benefit (family of 3)",
        "III.B.3": "Work requirement sanctions",
        "IV.C.1": "Time limits",
        "L3": "Maximum income for eligibility (summary)",
        "L5": "Maximum monthly benefit (summary)",
        "L3-A": "Maximum income adjusted to current dollars",
        "L5-A": "Maximum benefit adjusted to current dollars",
    }

    def __init__(self, timeout: float = 60.0):
        """Initialize the TANF fetcher.

        Args:
            timeout: HTTP request timeout in seconds
        """
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "Axiom/1.0 (Policy Research; +https://axiom-foundation.org)"},
        )

    def get_table_url(self, table_id: str, year: int) -> str:
        """Get the URL for a specific table and year.

        Args:
            table_id: Table identifier (e.g., "II.A.4")
            year: Data year (e.g., 2023)

        Returns:
            URL to the Excel file
        """
        # URL pattern varies by year
        if year >= 2023:
            date_prefix = "2025-05"  # Latest release folder
        elif year >= 2022:
            date_prefix = "2024-02"
        else:
            date_prefix = "2023-10"

        # URL encode the table ID
        table_encoded = table_id.replace(".", ".").replace("-", "-")
        return f"{self.BASE_URL}/{date_prefix}/{table_encoded}%20{year}.xlsx"

    def list_available_tables(self) -> dict[str, str]:
        """List available TANF policy tables.

        Returns:
            Dictionary mapping table IDs to descriptions
        """
        return self.AVAILABLE_TABLES.copy()

    def fetch_table(self, table_id: str, year: int) -> bytes:
        """Fetch a specific TANF policy table.

        Args:
            table_id: Table identifier (e.g., "II.A.4")
            year: Data year (e.g., 2023)

        Returns:
            Excel file content as bytes
        """
        url = self.get_table_url(table_id, year)
        response = self.client.get(url)
        response.raise_for_status()
        return response.content

    def fetch_databook(self, year: int) -> bytes:
        """Fetch the full Welfare Rules Databook for a year.

        Args:
            year: Data year (e.g., 2023)

        Returns:
            Excel file content as bytes
        """
        if year >= 2023:
            url = f"{self.BASE_URL}/2025-05/2023%20Welfare%20Rules%20Databook%20Tables%20%28final%2012%2010%202024%29.xlsx"
        else:
            url = f"{self.BASE_URL}/2024-02/{year}%20Welfare%20Rules%20Databook%20Tables.xlsx"  # pragma: no cover
        response = self.client.get(url)
        response.raise_for_status()
        return response.content

    def save_file(self, content: bytes, output_path: Path) -> None:
        """Save file content to disk.

        Args:
            content: File content as bytes
            output_path: Path to save the file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)

    def close(self):
        """Close the HTTP client."""
        self.client.close()


class CCDFFetcher:
    """Fetcher for CCDF child care policy data.

    The Urban Institute maintains the CCDF Policies Database at:
    https://ccdf.urban.org
    """

    BASE_URL = "https://ccdf.urban.org/sites/default/files"

    def __init__(self, timeout: float = 60.0):
        """Initialize the CCDF fetcher.

        Args:
            timeout: HTTP request timeout in seconds
        """
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "Axiom/1.0 (Policy Research; +https://axiom-foundation.org)"},
        )

    def get_database_url(self) -> str:
        """Get the URL for the full CCDF database.

        Returns:
            URL to the latest database Excel file
        """
        # Latest version as of 2025
        return f"{self.BASE_URL}/CCDF%20Policies%20Database_Full%20Data%20Files_2025%2004%20%28Apr%29%2030.xlsx"

    def get_book_of_tables_url(self, year: int) -> str:
        """Get the URL for a specific year's Book of Tables PDF.

        Args:
            year: Data year (e.g., 2023)

        Returns:
            URL to the PDF
        """
        if year == 2023:
            return f"{self.BASE_URL}/opre-CCDF-policies-database-2023-may25.pdf"
        else:
            return f"{self.BASE_URL}/CCDF%20Policies%20Database%20{year}%20Book%20of%20Tables%20%28final%29.pdf"

    def fetch_database(self) -> bytes:
        """Fetch the full CCDF policies database.

        Returns:
            Excel file content as bytes
        """
        url = self.get_database_url()
        response = self.client.get(url)
        response.raise_for_status()
        return response.content

    def fetch_book_of_tables(self, year: int) -> bytes:
        """Fetch the Book of Tables PDF for a year.

        Args:
            year: Data year (e.g., 2023)

        Returns:
            PDF file content as bytes
        """
        url = self.get_book_of_tables_url(year)  # pragma: no cover
        response = self.client.get(url)  # pragma: no cover
        response.raise_for_status()
        return response.content  # pragma: no cover

    def save_file(self, content: bytes, output_path: Path) -> None:
        """Save file content to disk.

        Args:
            content: File content as bytes
            output_path: Path to save the file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)

    def close(self):
        """Close the HTTP client."""
        self.client.close()


class StateBenefitsFetcher:
    """Unified fetcher for all state benefit policy data.

    This class provides a single interface for fetching SNAP, TANF, and CCDF
    policy data from their respective official sources.
    """

    def __init__(self, timeout: float = 60.0):
        """Initialize the unified fetcher.

        Args:
            timeout: HTTP request timeout in seconds
        """
        self.snap = SNAPSUAFetcher(timeout=timeout)
        self.tanf = TANFFetcher(timeout=timeout)
        self.ccdf = CCDFFetcher(timeout=timeout)

    def fetch_snap_sua(
        self,
        fiscal_years: list[int],
        output_dir: Path,
        progress_callback: Callable[[str], None] | None = None,
    ) -> dict[int, Path]:
        """Fetch SNAP SUA data for multiple fiscal years.

        Args:
            fiscal_years: List of fiscal years to fetch (e.g., [2024, 2025])
            output_dir: Directory to save files
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary mapping fiscal year to saved file path
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        results = {}

        for fy in fiscal_years:
            if progress_callback:
                progress_callback(f"Fetching SNAP SUA data for FY{fy}...")

            try:
                content = self.snap.fetch_sua_file(fy)
                output_path = output_dir / f"SUA-Table-FY{str(fy)[-2:]}.xlsx"
                self.snap.save_file(content, output_path)
                results[fy] = output_path

                if progress_callback:
                    size_kb = len(content) / 1024
                    progress_callback(f"  Saved: {output_path.name} ({size_kb:.1f} KB)")

            except httpx.HTTPError as e:
                if progress_callback:
                    progress_callback(f"  ERROR: Failed to fetch FY{fy}: {e}")

        return results

    def fetch_tanf_tables(
        self,
        tables: list[str],
        years: list[int],
        output_dir: Path,
        progress_callback: Callable[[str], None] | None = None,
    ) -> dict[str, Path]:
        """Fetch TANF policy tables for specified years.

        Args:
            tables: List of table IDs (e.g., ["II.A.4", "I.E.1"])
            years: List of data years to fetch
            output_dir: Directory to save files
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary mapping table-year keys to saved file paths
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        results = {}

        for table in tables:
            for year in years:
                key = f"{table}_{year}"
                if progress_callback:
                    progress_callback(f"Fetching TANF table {table} for {year}...")

                try:
                    content = self.tanf.fetch_table(table, year)
                    # Replace dots and spaces in filename
                    safe_table = table.replace(".", "_").replace("-", "_")
                    output_path = output_dir / f"TANF_{safe_table}_{year}.xlsx"
                    self.tanf.save_file(content, output_path)
                    results[key] = output_path

                    if progress_callback:
                        size_kb = len(content) / 1024
                        progress_callback(f"  Saved: {output_path.name} ({size_kb:.1f} KB)")

                except httpx.HTTPError as e:
                    if progress_callback:
                        progress_callback(f"  ERROR: Failed to fetch {key}: {e}")

        return results

    def fetch_tanf_databook(
        self,
        year: int,
        output_dir: Path,
        progress_callback: Callable[[str], None] | None = None,
    ) -> Path | None:
        """Fetch the full TANF Welfare Rules Databook.

        Args:
            year: Data year
            output_dir: Directory to save file
            progress_callback: Optional callback for progress updates

        Returns:
            Path to saved file, or None if failed
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        if progress_callback:
            progress_callback(f"Fetching TANF Welfare Rules Databook for {year}...")

        try:
            content = self.tanf.fetch_databook(year)
            output_path = output_dir / f"WelfareRulesDatabook_{year}.xlsx"
            self.tanf.save_file(content, output_path)

            if progress_callback:
                size_mb = len(content) / (1024 * 1024)
                progress_callback(f"  Saved: {output_path.name} ({size_mb:.1f} MB)")

            return output_path

        except httpx.HTTPError as e:
            if progress_callback:
                progress_callback(f"  ERROR: Failed to fetch databook: {e}")
            return None

    def fetch_ccdf_database(
        self,
        output_dir: Path,
        progress_callback: Callable[[str], None] | None = None,
    ) -> Path | None:
        """Fetch the full CCDF policies database.

        Args:
            output_dir: Directory to save file
            progress_callback: Optional callback for progress updates

        Returns:
            Path to saved file, or None if failed
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        if progress_callback:
            progress_callback("Fetching CCDF Policies Database...")

        try:
            content = self.ccdf.fetch_database()
            output_path = output_dir / "CCDF_Policies_Database.xlsx"
            self.ccdf.save_file(content, output_path)

            if progress_callback:
                size_mb = len(content) / (1024 * 1024)
                progress_callback(f"  Saved: {output_path.name} ({size_mb:.1f} MB)")

            return output_path

        except httpx.HTTPError as e:
            if progress_callback:
                progress_callback(f"  ERROR: Failed to fetch CCDF database: {e}")
            return None

    def fetch_all(
        self,
        output_dir: Path,
        snap_fiscal_years: list[int] | None = None,
        tanf_years: list[int] | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> dict:
        """Fetch all state benefit policy data.

        Args:
            output_dir: Base directory for all downloads
            snap_fiscal_years: SNAP fiscal years (default: [2024, 2025])
            tanf_years: TANF data years (default: [2023])
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with paths to all downloaded files
        """
        if snap_fiscal_years is None:
            snap_fiscal_years = [2024, 2025]  # pragma: no cover
        if tanf_years is None:
            tanf_years = [2023]  # pragma: no cover

        results = {
            "snap_sua": {},
            "tanf": {},
            "ccdf": None,
        }

        # Fetch SNAP SUA data
        snap_dir = output_dir / "snap_sua"
        results["snap_sua"] = self.fetch_snap_sua(snap_fiscal_years, snap_dir, progress_callback)

        # Fetch TANF databook
        tanf_dir = output_dir / "tanf"
        for year in tanf_years:
            path = self.fetch_tanf_databook(year, tanf_dir, progress_callback)
            if path:
                results["tanf"][year] = path

        # Fetch CCDF database
        ccdf_dir = output_dir / "ccdf"
        results["ccdf"] = self.fetch_ccdf_database(ccdf_dir, progress_callback)

        return results

    def close(self):
        """Close all HTTP clients."""
        self.snap.close()
        self.tanf.close()
        self.ccdf.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
