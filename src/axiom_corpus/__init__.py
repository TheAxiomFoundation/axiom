"""Axiom - Foundational archive for all raw government source files."""

from axiom_corpus.archive import AxiomArchive

# USC parser models
from axiom_corpus.models import Citation, SearchResult, Section, Subsection

# Structured statute models
from axiom_corpus.models_statute import (
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
    # USC parser models
    "Section",
    "Subsection",
    "Citation",
    "SearchResult",
    # Structured statute models
    "Statute",
    "StatuteSubsection",
    "StatuteSearchResult",
    "JurisdictionInfo",
    "JurisdictionType",
    "JURISDICTIONS",
]
