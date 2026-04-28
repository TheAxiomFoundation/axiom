"""Axiom - Foundational archive for all raw government source files."""

from axiom.archive import AxiomArchive

# Legacy USC models (still used internally)
from axiom.models import Citation, SearchResult, Section, Subsection

# Unified statute model (new architecture)
from axiom.models_statute import (
    JURISDICTIONS,
    JurisdictionInfo,
    JurisdictionType,
    Statute,
    StatuteSearchResult,
    StatuteSubsection,
)

__version__ = "0.1.0"
__all__ = [
    # Main archive class
    "AxiomArchive",
    # Legacy USC models
    "Section",
    "Subsection",
    "Citation",
    "SearchResult",
    # Unified statute model
    "Statute",
    "StatuteSubsection",
    "StatuteSearchResult",
    "JurisdictionInfo",
    "JurisdictionType",
    "JURISDICTIONS",
]
