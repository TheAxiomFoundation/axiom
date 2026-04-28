"""Fetchers for downloading regulatory documents."""

from axiom_corpus.fetchers.ecfr import ECFRFetcher
from axiom_corpus.fetchers.state_benefits import (
    CCDFFetcher,
    CCDFPolicyData,
    SNAPSUAFetcher,
    StateBenefitsFetcher,
    SUAData,
    TANFFetcher,
    TANFPolicyData,
)

__all__ = [
    "ECFRFetcher",
    "SNAPSUAFetcher",
    "TANFFetcher",
    "CCDFFetcher",
    "StateBenefitsFetcher",
    "SUAData",
    "TANFPolicyData",
    "CCDFPolicyData",
]
