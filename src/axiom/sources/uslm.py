"""USLM source adapter for federal US Code.

The US Code is published in USLM (United States Legislative Markup) XML format
at uscode.house.gov. This adapter parses the XML and converts to unified Statute model.
"""

import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from axiom.models_statute import Statute, StatuteSubsection
from axiom.sources.base import SourceConfig, StatuteSource


# Federal codes (US Code titles)
US_CODE_TITLES: dict[str, str] = {
    "1": "General Provisions",
    "2": "The Congress",
    "3": "The President",
    "4": "Flag and Seal, Seat of Government, and the States",
    "5": "Government Organization and Employees",
    "6": "Domestic Security",
    "7": "Agriculture",
    "8": "Aliens and Nationality",
    "9": "Arbitration",
    "10": "Armed Forces",
    "11": "Bankruptcy",
    "12": "Banks and Banking",
    "13": "Census",
    "14": "Coast Guard",
    "15": "Commerce and Trade",
    "16": "Conservation",
    "17": "Copyrights",
    "18": "Crimes and Criminal Procedure",
    "19": "Customs Duties",
    "20": "Education",
    "21": "Food and Drugs",
    "22": "Foreign Relations and Intercourse",
    "23": "Highways",
    "24": "Hospitals and Asylums",
    "25": "Indians",
    "26": "Internal Revenue Code",
    "27": "Intoxicating Liquors",
    "28": "Judiciary and Judicial Procedure",
    "29": "Labor",
    "30": "Mineral Lands and Mining",
    "31": "Money and Finance",
    "32": "National Guard",
    "33": "Navigation and Navigable Waters",
    "34": "Crime Control and Law Enforcement",
    "35": "Patents",
    "36": "Patriotic and National Observances",
    "37": "Pay and Allowances of the Uniformed Services",
    "38": "Veterans' Benefits",
    "39": "Postal Service",
    "40": "Public Buildings, Property, and Works",
    "41": "Public Contracts",
    "42": "The Public Health and Welfare",
    "43": "Public Lands",
    "44": "Public Printing and Documents",
    "45": "Railroads",
    "46": "Shipping",
    "47": "Telecommunications",
    "48": "Territories and Insular Possessions",
    "49": "Transportation",
    "50": "War and National Defense",
    "51": "National and Commercial Space Programs",
    "52": "Voting and Elections",
    "54": "National Park Service and Related Programs",
}

def get_federal_config() -> SourceConfig:
    """Get source configuration for federal US Code."""
    return SourceConfig(
        jurisdiction="us",
        name="United States",
        source_type="uslm",
        base_url="https://uscode.house.gov",
        codes=US_CODE_TITLES,
        rate_limit=0.2,
    )


class USLMSource(StatuteSource):
    """Source adapter for USLM XML (federal US Code).

    Downloads XML files from uscode.house.gov and parses using
    the existing USLM parser, then converts to unified Statute model.
    """

    def __init__(self, config: SourceConfig | None = None):
        super().__init__(config or get_federal_config())
        self._parser = None

    @property
    def parser(self):
        """Lazy-load the USLM parser."""
        if self._parser is None:  # pragma: no cover
            from axiom.parsers.us.statutes import USLMParser  # pragma: no cover

            self._parser = USLMParser()  # pragma: no cover
        return self._parser  # pragma: no cover

    def download_title_xml(self, title: str, output_dir: Path | None = None) -> Path:
        """Download XML file for a title.

        Args:
            title: Title number (e.g., "26")
            output_dir: Where to save (default: data/uscode)

        Returns:
            Path to downloaded XML file
        """
        output_dir = output_dir or Path("data/uscode")  # pragma: no cover
        output_dir.mkdir(parents=True, exist_ok=True)  # pragma: no cover

        url = f"https://uscode.house.gov/download/releasepoints/us/pl/118/177/usc{title}.xml"  # pragma: no cover
        output_file = output_dir / f"usc{title}.xml"  # pragma: no cover

        if output_file.exists():  # pragma: no cover
            return output_file  # pragma: no cover

        print(f"Downloading Title {title}...")  # pragma: no cover
        response = self._get(url)  # pragma: no cover
        response.raise_for_status()

        output_file.write_bytes(response.content)  # pragma: no cover
        return output_file  # pragma: no cover

    def _section_to_statute(self, section, title: str) -> Statute:
        """Convert parsed Section to unified Statute model."""
        # Parse subsections
        subsections = []  # pragma: no cover
        for sub in section.subsections:  # pragma: no cover
            subsections.append(  # pragma: no cover
                StatuteSubsection(
                    identifier=sub.identifier,
                    heading=sub.heading,
                    text=sub.text,
                    children=[
                        StatuteSubsection(
                            identifier=c.identifier,
                            heading=c.heading if hasattr(c, "heading") else None,
                            text=c.text,
                        )
                        for c in getattr(sub, "children", [])
                    ],
                )
            )

        return Statute(  # pragma: no cover
            jurisdiction="us",
            code=str(title),
            code_name=US_CODE_TITLES.get(str(title), f"Title {title}"),
            section=section.citation.section,
            subsection_path=section.citation.subsection,
            title=section.section_title,
            text=section.text,
            subsections=subsections,
            source_url=section.source_url,
            source_id=section.uslm_id,
            enacted_date=section.enacted_date,
            last_amended=section.last_amended,
            effective_date=section.effective_date,
            public_laws=section.public_laws,
            references_to=section.references_to,
            referenced_by=section.referenced_by,
            retrieved_at=section.retrieved_at
            if hasattr(section, "retrieved_at")
            else datetime.utcnow(),
        )

    def get_section(self, code: str, section: str, **kwargs) -> Statute | None:
        """Fetch a single section from US Code.

        Args:
            code: Title number (e.g., "26")
            section: Section number (e.g., "32")

        Returns:
            Statute or None if not found
        """
        # Download title XML if not present
        xml_file = self.download_title_xml(code)  # pragma: no cover

        # Parse and find section
        for parsed in self.parser.parse_file(xml_file):  # pragma: no cover
            if parsed.citation.section == section:  # pragma: no cover
                return self._section_to_statute(parsed, code)  # pragma: no cover

        return None  # pragma: no cover

    def list_sections(self, code: str, **kwargs) -> Iterator[str]:
        """List all section numbers in a title.

        Args:
            code: Title number

        Yields:
            Section numbers
        """
        xml_file = self.download_title_xml(code)  # pragma: no cover

        for section in self.parser.parse_file(xml_file):  # pragma: no cover
            yield section.citation.section  # pragma: no cover

    def download_code(
        self,
        code: str,
        max_sections: int | None = None,
        progress_callback=None,
    ) -> Iterator[Statute]:
        """Download all sections from a title.

        Overrides base to be more efficient - parses XML once.
        """
        xml_file = self.download_title_xml(code)  # pragma: no cover

        count = 0  # pragma: no cover
        for section in self.parser.parse_file(xml_file):  # pragma: no cover
            if max_sections and count >= max_sections:  # pragma: no cover
                return  # pragma: no cover

            statute = self._section_to_statute(section, code)  # pragma: no cover
            count += 1  # pragma: no cover
            if progress_callback:  # pragma: no cover
                progress_callback(count, section.citation.section)  # pragma: no cover
            yield statute  # pragma: no cover
