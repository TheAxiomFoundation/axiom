"""Fetchers for downloading regulatory documents."""

from axiom.fetchers.ecfr import ECFRFetcher
from axiom.fetchers.state_benefits import (
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
