"""Parsers for various legal document formats.

Organization:
- parsers/us/statutes.py - Federal US Code (USLM XML)
- parsers/us_ca/statutes.py - California
- parsers/us_fl/statutes.py - Florida
- parsers/us_ny/statutes.py - New York
- parsers/us_tx/statutes.py - Texas
- parsers/generic/statutes.py - Generic HTML parser for other states
- parsers/base.py - Base classes and state registry
"""

from axiom_corpus.parsers.us.statutes import USLMParser

# Generic state parser (new unified architecture)
try:
    from axiom_corpus.parsers.generic.statutes import (
        GenericStateParser,
        StateConfig,
        StateSection,
        StateSubsection,
        STATE_PARSERS,
        get_parser_for_state,
        list_supported_states,
    )
except ImportError:  # pragma: no cover
    GenericStateParser = None  # type: ignore[misc, assignment]
    StateConfig = None  # type: ignore[misc, assignment]
    StateSection = None  # type: ignore[misc, assignment]
    StateSubsection = None  # type: ignore[misc, assignment]
    STATE_PARSERS = {}
    get_parser_for_state = None  # type: ignore[misc, assignment]
    list_supported_states = None  # type: ignore[misc, assignment]

# State parsers - imported conditionally to avoid import errors
try:
    from axiom_corpus.parsers.us_ny.statutes import (
        NY_LAW_CODES,
        NYLegislationClient,
        NYStateCitation,
        download_ny_law,
    )
except ImportError:
    NY_LAW_CODES = {}
    NYLegislationClient = None  # type: ignore[misc, assignment]
    NYStateCitation = None  # type: ignore[misc, assignment]
    download_ny_law = None  # type: ignore[misc, assignment]

try:
    from axiom_corpus.parsers.us_ca.statutes import (
        CA_CODES,
        CACodeParser,
        CASection,
        CaliforniaStatutesParser,
    )
except ImportError:
    CA_CODES = {}
    CACodeParser = None  # type: ignore[misc, assignment]
    CASection = None  # type: ignore[misc, assignment]
    CaliforniaStatutesParser = None  # type: ignore[misc, assignment]

try:
    from axiom_corpus.parsers.us_fl.statutes import (
        FL_TAX_CHAPTERS,
        FL_WELFARE_CHAPTERS,
        FLStatutesClient,
        FLSection,
    )
except ImportError:
    FL_TAX_CHAPTERS = {}
    FL_WELFARE_CHAPTERS = {}
    FLStatutesClient = None  # type: ignore[misc, assignment]
    FLSection = None  # type: ignore[misc, assignment]

try:
    from axiom_corpus.parsers.us_tx.statutes import (
        TX_CODES,
        TXStatutesClient,
        TXSection,
    )
except ImportError:
    TX_CODES = {}
    TXStatutesClient = None  # type: ignore[misc, assignment]
    TXSection = None  # type: ignore[misc, assignment]

__all__ = [
    # Federal
    "USLMParser",
    # Generic state parser (new)
    "GenericStateParser",
    "StateConfig",
    "StateSection",
    "StateSubsection",
    "STATE_PARSERS",
    "get_parser_for_state",
    "list_supported_states",
    # New York
    "NY_LAW_CODES",
    "NYLegislationClient",
    "NYStateCitation",
    "download_ny_law",
    # California
    "CA_CODES",
    "CACodeParser",
    "CASection",
    "CaliforniaStatutesParser",
    # Florida
    "FL_TAX_CHAPTERS",
    "FL_WELFARE_CHAPTERS",
    "FLStatutesClient",
    "FLSection",
    # Texas
    "TX_CODES",
    "TXStatutesClient",
    "TXSection",
]
