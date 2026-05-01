"""Multi-state statute parser registry and base classes.

This module provides a unified interface for downloading state statutes
from various official sources. Each state has different APIs/formats:

- NY: Open Legislation API (REST, free API key)
- CA: leginfo.legislature.ca.gov (FTP/scrape)
- TX: statutes.capitol.texas.gov (bulk download)
- FL: leg.state.fl.us (web scrape)

Sources:
- OpenStates scrapers: https://github.com/openstates/openstates-scrapers
- LegiScan API: https://legiscan.com/legiscan
- Cornell LII: https://www.law.cornell.edu/states
"""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum

from axiom_corpus.models import Section


class StateCode(StrEnum):
    """US state and territory codes."""

    AL = "AL"  # Alabama
    AK = "AK"  # Alaska
    AZ = "AZ"  # Arizona
    AR = "AR"  # Arkansas
    CA = "CA"  # California
    CO = "CO"  # Colorado
    CT = "CT"  # Connecticut
    DE = "DE"  # Delaware
    FL = "FL"  # Florida
    GA = "GA"  # Georgia
    HI = "HI"  # Hawaii
    ID = "ID"  # Idaho
    IL = "IL"  # Illinois
    IN = "IN"  # Indiana
    IA = "IA"  # Iowa
    KS = "KS"  # Kansas
    KY = "KY"  # Kentucky
    LA = "LA"  # Louisiana
    ME = "ME"  # Maine
    MD = "MD"  # Maryland
    MA = "MA"  # Massachusetts
    MI = "MI"  # Michigan
    MN = "MN"  # Minnesota
    MS = "MS"  # Mississippi
    MO = "MO"  # Missouri
    MT = "MT"  # Montana
    NE = "NE"  # Nebraska
    NV = "NV"  # Nevada
    NH = "NH"  # New Hampshire
    NJ = "NJ"  # New Jersey
    NM = "NM"  # New Mexico
    NY = "NY"  # New York
    NC = "NC"  # North Carolina
    ND = "ND"  # North Dakota
    OH = "OH"  # Ohio
    OK = "OK"  # Oklahoma
    OR = "OR"  # Oregon
    PA = "PA"  # Pennsylvania
    RI = "RI"  # Rhode Island
    SC = "SC"  # South Carolina
    SD = "SD"  # South Dakota
    TN = "TN"  # Tennessee
    TX = "TX"  # Texas
    UT = "UT"  # Utah
    VT = "VT"  # Vermont
    VA = "VA"  # Virginia
    WA = "WA"  # Washington
    WV = "WV"  # West Virginia
    WI = "WI"  # Wisconsin
    WY = "WY"  # Wyoming
    DC = "DC"  # Washington D.C.
    PR = "PR"  # Puerto Rico


@dataclass
class StateInfo:
    """Information about a state's statute source."""

    code: StateCode
    name: str
    statute_source: str  # URL or description of source
    api_available: bool
    api_key_required: bool
    api_key_env_var: str | None
    notes: str | None = None


# Registry of known state statute sources
STATE_REGISTRY: dict[StateCode, StateInfo] = {
    StateCode.NY: StateInfo(
        code=StateCode.NY,
        name="New York",
        statute_source="https://legislation.nysenate.gov",
        api_available=True,
        api_key_required=True,
        api_key_env_var="NY_LEGISLATION_API_KEY",
        notes="Free API key from legislation.nysenate.gov",
    ),
    StateCode.CA: StateInfo(
        code=StateCode.CA,
        name="California",
        statute_source="https://leginfo.legislature.ca.gov",
        api_available=False,
        api_key_required=False,
        api_key_env_var=None,
        notes="FTP bulk download or web scrape required",
    ),
    StateCode.TX: StateInfo(
        code=StateCode.TX,
        name="Texas",
        statute_source="https://statutes.capitol.texas.gov",
        api_available=False,
        api_key_required=False,
        api_key_env_var=None,
        notes="Bulk download in various formats",
    ),
    StateCode.FL: StateInfo(
        code=StateCode.FL,
        name="Florida",
        statute_source="https://leg.state.fl.us/statutes",
        api_available=False,
        api_key_required=False,
        api_key_env_var=None,
        notes="Web scrape required",
    ),
}


class BaseStateParser(ABC):
    """Abstract base class for state statute parsers."""

    state_code: StateCode

    @abstractmethod
    def list_codes(self) -> list[str]:
        """List available law codes for this state.

        Returns:
            List of law code identifiers (e.g., ["TAX", "SOS", "EDN"])
        """
        ...

    @abstractmethod
    def download_code(self, code: str) -> Iterator[Section]:
        """Download all sections from a specific law code.

        Args:
            code: Law code identifier

        Yields:
            Section objects for each section in the code
        """
        ...

    def download_all(self) -> Iterator[Section]:
        """Download all law codes for this state.

        Yields:
            Section objects for all sections in all codes
        """
        for code in self.list_codes():
            yield from self.download_code(code)


def get_supported_states() -> list[StateInfo]:
    """Get list of states with implemented parsers."""
    return list(STATE_REGISTRY.values())


def get_state_info(state: str) -> StateInfo | None:
    """Get info about a specific state.

    Args:
        state: State code (e.g., "NY", "CA")

    Returns:
        StateInfo if found, None otherwise
    """
    try:
        code = StateCode(state.upper())
        return STATE_REGISTRY.get(code)
    except ValueError:
        return None
