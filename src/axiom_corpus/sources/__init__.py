"""Source adapters for fetching statutes from various jurisdictions."""

from axiom_corpus.sources.api import APISource
from axiom_corpus.sources.base import SourceConfig, StatuteSource
from axiom_corpus.sources.html import HTMLSource
from axiom_corpus.sources.uslm import USLMSource

__all__ = [
    "StatuteSource",
    "SourceConfig",
    "USLMSource",
    "HTMLSource",
    "APISource",
]
