"""Source adapters for fetching statutes from various jurisdictions."""

from axiom_corpus.sources.base import StatuteSource, SourceConfig
from axiom_corpus.sources.uslm import USLMSource
from axiom_corpus.sources.html import HTMLSource
from axiom_corpus.sources.api import APISource

__all__ = [
    "StatuteSource",
    "SourceConfig",
    "USLMSource",
    "HTMLSource",
    "APISource",
]
