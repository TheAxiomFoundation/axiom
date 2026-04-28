"""Fetcher for eCFR (Electronic Code of Federal Regulations) bulk data.

Downloads CFR titles from govinfo.gov and parses them into Regulation objects.

Source: https://www.govinfo.gov/bulkdata/ECFR
"""

import asyncio
from pathlib import Path
from typing import Iterator, Optional

import httpx

from axiom_corpus.models_regulation import Regulation
from axiom_corpus.parsers.cfr import CFRParser


# CFR titles that contain regulations (1-50, with some gaps)
CFR_TITLES = list(range(1, 51))


class ECFRFetcher:
    """Fetcher for eCFR bulk XML data from govinfo.gov.

    Downloads complete CFR title XML files and parses them into
    Regulation objects.
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        base_url: str = "https://www.govinfo.gov/bulkdata/ECFR",
    ):
        """Initialize the fetcher.

        Args:
            data_dir: Directory to store downloaded files.
                     Defaults to ~/.axiom/cfr/
            base_url: Base URL for eCFR bulk data.
        """
        self.base_url = base_url
        self.data_dir = data_dir or Path.home() / ".axiom" / "cfr"

    @property
    def available_titles(self) -> list[int]:
        """List of available CFR title numbers."""
        return CFR_TITLES.copy()

    def get_title_url(self, title: int) -> str:
        """Get the download URL for a CFR title.

        Args:
            title: CFR title number (1-50)

        Returns:
            URL to the XML file
        """
        return f"{self.base_url}/title-{title}/ECFR-title{title}.xml"

    async def _download_file(self, url: str, dest: Path) -> Path:
        """Download a file from URL to destination.

        Args:
            url: URL to download from
            dest: Destination path

        Returns:
            Path to downloaded file
        """
        dest.parent.mkdir(parents=True, exist_ok=True)

        async with httpx.AsyncClient() as client:
            response = await client.get(url, follow_redirects=True, timeout=300)
            response.raise_for_status()

            with open(dest, "wb") as f:
                f.write(response.content)

        return dest

    async def download_title(
        self,
        title: int,
        force: bool = False,
    ) -> Path:
        """Download a CFR title XML file.

        Args:
            title: CFR title number (1-50)
            force: Re-download even if file exists

        Returns:
            Path to the downloaded XML file
        """
        url = self.get_title_url(title)
        dest = self.data_dir / f"title-{title}.xml"

        if dest.exists() and not force:
            return dest

        return await self._download_file(url, dest)  # pragma: no cover

    def parse_title(
        self,
        xml_path: Path,
        parts: Optional[list[int]] = None,
    ) -> Iterator[Regulation]:
        """Parse a downloaded CFR title XML file.

        Args:
            xml_path: Path to the XML file
            parts: Optional list of part numbers to filter

        Yields:
            Regulation objects for each section
        """
        content = xml_path.read_text()
        parser = CFRParser(content)

        for reg in parser.iter_sections():
            if parts is None or reg.citation.part in parts:
                yield reg

    def get_title_metadata(self, xml_path: Path) -> dict:
        """Extract metadata from a CFR title XML file.

        Args:
            xml_path: Path to the XML file

        Returns:
            Dict with title_number, title_name, amendment_date
        """
        content = xml_path.read_text()
        parser = CFRParser(content)

        return {
            "title_number": parser.title_number,
            "title_name": parser.title_name,
            "amendment_date": parser.amendment_date,
        }

    def count_sections(self, xml_path: Path) -> int:
        """Count the number of sections in a CFR title.

        Args:
            xml_path: Path to the XML file

        Returns:
            Number of sections
        """
        return sum(1 for _ in self.parse_title(xml_path))


async def download_cfr_title(
    title: int,
    data_dir: Optional[Path] = None,
    force: bool = False,
) -> Path:
    """Convenience function to download a CFR title.

    Args:
        title: CFR title number
        data_dir: Optional data directory
        force: Re-download even if exists

    Returns:
        Path to downloaded file
    """
    fetcher = ECFRFetcher(data_dir=data_dir)
    return await fetcher.download_title(title, force=force)
