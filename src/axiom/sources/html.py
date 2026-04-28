"""HTML source adapter for web scraping state statutes.

Most state legislatures publish statutes as HTML pages without APIs.
This adapter handles common patterns with configurable selectors.
"""

import re
from collections.abc import Iterator

import httpx
from bs4 import BeautifulSoup

from axiom.models_statute import Statute, StatuteSubsection
from axiom.sources.base import SourceConfig, StatuteSource


class HTMLSource(StatuteSource):
    """Source adapter for HTML-based statute websites.

    Configurable via SourceConfig for different state patterns:
    - URL patterns for section and TOC pages
    - CSS selectors for content, title, history
    - Custom parsing functions
    """

    def _build_section_url(self, code: str, section: str, **kwargs) -> str:
        """Build URL for a section page."""
        if not self.config.section_url_pattern:
            raise ValueError("section_url_pattern not configured")

        url = self.config.section_url_pattern.format(
            code=code,
            section=section,
            **kwargs,
        )
        if not url.startswith("http"):
            url = self.config.base_url + url
        return url

    def _build_toc_url(self, code: str, **kwargs) -> str:
        """Build URL for a TOC page."""
        if not self.config.toc_url_pattern:
            raise ValueError("toc_url_pattern not configured")

        url = self.config.toc_url_pattern.format(code=code, **kwargs)
        if not url.startswith("http"):
            url = self.config.base_url + url
        return url

    def _find_content(self, soup: BeautifulSoup) -> BeautifulSoup | None:
        """Find content element using configured selector."""
        if not self.config.content_selector:
            return soup.body

        for selector in self.config.content_selector.split(","):
            content = soup.select_one(selector.strip())
            if content:
                return content
        return None

    def _find_title(self, soup: BeautifulSoup, section: str) -> str:
        """Find section title using configured selector."""
        if self.config.title_selector:
            for selector in self.config.title_selector.split(","):
                title_el = soup.select_one(selector.strip())
                if title_el:
                    return title_el.get_text(strip=True)
        return f"§ {section}"

    def _find_history(self, soup: BeautifulSoup) -> str | None:
        """Find history note using configured selector."""
        if self.config.history_selector:
            history_el = soup.select_one(self.config.history_selector)
            if history_el:
                return history_el.get_text(strip=True)
        return None

    def _parse_subsections(self, content: BeautifulSoup) -> list[StatuteSubsection]:
        """Parse subsections using common patterns.

        Handles markers like (a), (1), (A), (i).
        """
        subsections = []
        text = content.get_text(separator="\n", strip=True)

        # Common subsection pattern
        pattern = r"\(([a-zA-Z]|\d+|[ivx]+)\)\s*([^(]+?)(?=\([a-zA-Z]|\d+|[ivx]+\)|$)"

        for match in re.finditer(pattern, text, re.DOTALL):
            marker = match.group(1)
            content_text = match.group(2).strip()
            if content_text:
                subsections.append(
                    StatuteSubsection(
                        identifier=marker,
                        text=content_text[:1000] if len(content_text) > 1000 else content_text,
                    )
                )

        return subsections

    def get_section(self, code: str, section: str, **kwargs) -> Statute | None:
        """Fetch a single section by scraping HTML."""
        url = self._build_section_url(code, section, **kwargs)

        try:
            response = self._get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            print(f"Error fetching {self.config.jurisdiction}/{code}/{section}: {e}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Find content
        content = self._find_content(soup)
        if not content:
            print(f"No content found for {self.config.jurisdiction}/{code}/{section}")
            return None

        text = content.get_text(separator="\n", strip=True)
        title = self._find_title(soup, section)
        history = self._find_history(soup)
        subsections = self._parse_subsections(content)

        return self._create_statute(
            code=code,
            section=section,
            title=title,
            text=text,
            source_url=url,
            subsections=subsections,
            history=history,
        )

    def list_sections(self, code: str, **kwargs) -> Iterator[str]:
        """List sections from TOC page."""
        try:
            url = self._build_toc_url(code, **kwargs)
            response = self._get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            print(f"Error fetching TOC for {self.config.jurisdiction}/{code}: {e}")
            return

        soup = BeautifulSoup(response.text, "html.parser")

        # Find links that look like section references
        section_pattern = re.compile(r"section[-_]?([\d.]+)", re.I)

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = section_pattern.search(href)
            if match:
                yield match.group(1)
