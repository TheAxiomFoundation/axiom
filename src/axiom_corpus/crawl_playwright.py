"""Playwright-based crawler for JavaScript SPA state statute sites.

Handles states that require JavaScript rendering:
- Alabama (React SPA with GraphQL)
- Alaska (JavaScript navigation)
- Texas (Angular SPA)

Usage:
    uv run python -m axiom_corpus.crawl_playwright us-al --max-sections 100
    uv run python -m axiom_corpus.crawl_playwright --all --dry-run
"""

import asyncio
import hashlib
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin, urlparse

import click
from playwright.async_api import Page, async_playwright

from axiom_corpus.crawl import R2_BUCKET, get_r2_client


@dataclass
class PlaywrightStats:
    """Stats for a Playwright crawl job."""

    jurisdiction: str
    name: str
    sections_discovered: int = 0
    sections_fetched: int = 0
    sections_failed: int = 0
    bytes_fetched: int = 0
    bytes_uploaded: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float = 0
    errors: list = field(default_factory=list)

    @property
    def duration(self) -> float:
        return (self.end_time or time.time()) - self.start_time

    @property
    def rate(self) -> float:
        if self.duration > 0:
            return self.sections_fetched / self.duration
        return 0

    @property
    def errors_count(self) -> int:
        return len(self.errors)


# State-specific configurations
SPA_STATES = {
    "us-al": {
        "name": "Alabama",
        "base_url": "https://alison.legislature.state.al.us",
        "start_url": "https://alison.legislature.state.al.us/code-of-alabama",
        "type": "graphql",
    },
    "us-ak": {
        "name": "Alaska",
        "base_url": "https://www.akleg.gov",
        "start_url": "https://www.akleg.gov/basis/statutes.asp",
        "type": "js_nav",
    },
    "us-tx": {
        "name": "Texas",
        "base_url": "https://statutes.capitol.texas.gov",
        "start_url": "https://statutes.capitol.texas.gov/",
        "type": "angular",
    },
}


class AlabamaCrawler:
    """Crawler for Alabama's React-based statute site.

    Alabama uses a hierarchical button-based navigation:
    - Title buttons expand to show chapters
    - Chapter buttons expand to show section links
    - Section links: /code-of-alabama?section=X-X-X
    """

    def __init__(self, page: Page, dry_run: bool = False):
        self.page = page
        self.dry_run = dry_run
        self.stats = PlaywrightStats(jurisdiction="us-al", name="Alabama")
        self._r2 = None

    @property
    def r2(self):
        if self._r2 is None and not self.dry_run:
            self._r2 = get_r2_client()
        return self._r2

    async def _get_title_buttons(self) -> list:
        """Get all title buttons by their text content."""
        buttons = await self.page.query_selector_all("button")
        title_buttons = []
        for btn in buttons:
            try:
                text = await btn.inner_text()
                if text.strip().startswith("Title"):
                    title_buttons.append(text.strip())
            except:
                pass
        return title_buttons

    async def _click_button_by_text(self, text_prefix: str) -> bool:
        """Click a button by its text content (re-locates to handle DOM changes)."""
        buttons = await self.page.query_selector_all("button")
        for btn in buttons:
            try:
                text = await btn.inner_text()
                if text.strip().startswith(text_prefix):
                    await btn.click()
                    return True
            except:
                pass
        return False

    async def discover_sections(self) -> list[dict]:
        """Discover all sections by expanding title and chapter buttons.

        Due to the page's virtualization, we reload for each title to ensure
        all buttons are accessible.
        """
        sections = []
        seen_urls = set()

        # First pass: get all title names
        print("[us-al] Loading Code of Alabama page...")
        await self.page.goto(
            "https://alison.legislature.state.al.us/code-of-alabama",
            wait_until="networkidle",
            timeout=60000,
        )
        await self.page.wait_for_timeout(3000)

        title_names = await self._get_title_buttons()
        print(f"[us-al] Found {len(title_names)} titles")

        for i, title_name in enumerate(title_names):
            # Extract just the title identifier (e.g., "Title 1", "Title 10A")
            parts = title_name.split()
            title_prefix = parts[0] + " " + parts[1]

            print(f"[us-al] Processing {i + 1}/{len(title_names)}: {title_name[:50]}...")

            # Reload page for each title to avoid virtualization issues
            await self.page.goto(
                "https://alison.legislature.state.al.us/code-of-alabama",
                wait_until="networkidle",
                timeout=60000,
            )
            await self.page.wait_for_timeout(2000)

            # Scroll to make sure the title is visible and click it
            if not await self._click_button_by_text(title_prefix):
                print(f"[us-al] Could not find button: {title_prefix}")
                continue

            await self.page.wait_for_timeout(1500)

            # Find all chapter buttons and get their names
            buttons = await self.page.query_selector_all("button")
            chapter_names = []
            for btn in buttons:
                try:
                    text = await btn.inner_text()
                    if text.strip().startswith("Chapter"):
                        chapter_names.append(text.strip())
                except:
                    pass

            # Click each chapter to reveal sections
            for chapter_name in chapter_names:
                chapter_prefix = " ".join(chapter_name.split()[:2])  # "Chapter X"
                await self._click_button_by_text(chapter_prefix)
                await self.page.wait_for_timeout(600)

            # Now collect all section links
            links = await self.page.query_selector_all("a[href*='section=']")
            for link in links:
                try:
                    href = await link.get_attribute("href")
                    text = await link.inner_text()
                    if href and href not in seen_urls:
                        seen_urls.add(href)
                        full_url = urljoin("https://alison.legislature.state.al.us", href)
                        sections.append(
                            {
                                "url": full_url,
                                "title": text.strip(),
                            }
                        )
                except:
                    pass

            # Progress update
            if (i + 1) % 5 == 0:
                print(f"[us-al] Found {len(sections)} sections so far...")

        self.stats.sections_discovered = len(sections)
        return sections

    async def fetch_section(self, section: dict) -> str | None:
        """Fetch a single section's content."""
        try:
            await self.page.goto(
                section["url"],
                wait_until="networkidle",
                timeout=30000,
            )
            await self.page.wait_for_timeout(500)

            # Get the main content
            content = await self.page.content()
            self.stats.sections_fetched += 1
            self.stats.bytes_fetched += len(content.encode())
            return content

        except Exception as e:
            self.stats.sections_failed += 1
            self.stats.errors.append(f"Fetch {section['url']}: {e}")
            return None

    def upload_to_r2(self, url: str, html: str) -> bool:
        """Upload section HTML to R2."""
        if self.dry_run:
            return True

        try:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            path = urlparse(url).path.strip("/").replace("/", "_")
            key = f"us-al/{path}_{url_hash}.html"

            self.r2.put_object(
                Bucket=R2_BUCKET,
                Key=key,
                Body=html.encode("utf-8"),
                ContentType="text/html",
            )
            return True
        except Exception as e:
            self.stats.errors.append(f"R2 upload: {e}")
            return False


class AlaskaCrawler:
    """Crawler for Alaska's JavaScript-based statute site.

    Alaska uses JavaScript click handlers on title links. Clicking a title
    expands to show chapters. Chapters contain direct section links.
    """

    def __init__(self, page: Page, dry_run: bool = False):
        self.page = page
        self.dry_run = dry_run
        self.stats = PlaywrightStats(jurisdiction="us-ak", name="Alaska")
        self._r2 = None

    @property
    def r2(self):
        if self._r2 is None and not self.dry_run:
            self._r2 = get_r2_client()
        return self._r2

    async def _get_title_names(self) -> list[str]:
        """Get all title names from the page."""
        titles = await self.page.evaluate("""() => {
            const links = document.querySelectorAll('a');
            const titles = [];
            for (const link of links) {
                const text = link.innerText.trim();
                if (text.startsWith('Title ') && text.includes('.')) {
                    titles.push(text);
                }
            }
            return titles;
        }""")
        return titles

    async def _click_title(self, title_text: str) -> bool:
        """Click a title using JavaScript (bypasses overlay issues)."""
        return await self.page.evaluate(
            """(titleText) => {
            const links = document.querySelectorAll('a');
            for (const link of links) {
                if (link.innerText.trim().startsWith(titleText.split('.')[0])) {
                    link.click();
                    return true;
                }
            }
            return false;
        }""",
            title_text,
        )

    async def discover_sections(self) -> list[dict]:
        """Discover all sections from Alaska's statute page."""
        sections = []
        seen_urls = set()

        print("[us-ak] Loading Alaska Statutes page...")
        await self.page.goto(
            "https://www.akleg.gov/basis/statutes.asp",
            wait_until="load",
            timeout=60000,
        )
        await self.page.wait_for_timeout(5000)

        # Get all title names
        title_names = await self._get_title_names()
        print(f"[us-ak] Found {len(title_names)} titles")

        for i, title_name in enumerate(title_names):
            print(f"[us-ak] Processing {i + 1}/{len(title_names)}: {title_name[:50]}...")

            # Reload page for each title to get clean state
            await self.page.goto(
                "https://www.akleg.gov/basis/statutes.asp",
                wait_until="load",
                timeout=60000,
            )
            await self.page.wait_for_timeout(3000)

            # Click the title using JavaScript
            clicked = await self._click_title(title_name)
            if not clicked:
                print(f"[us-ak] Could not click: {title_name[:50]}")
                continue

            await self.page.wait_for_timeout(2000)

            # Now extract chapter links
            chapters = await self.page.evaluate("""() => {
                const links = document.querySelectorAll('a');
                const chapters = [];
                for (const link of links) {
                    const text = link.innerText.trim();
                    if (text.startsWith('Chapter ')) {
                        const href = link.getAttribute('href') || '';
                        chapters.push({text: text, href: href});
                    }
                }
                return chapters;
            }""")

            # Click each chapter to get sections
            for chapter in chapters:
                # Click chapter
                await self.page.evaluate(
                    """(chapterText) => {
                    const links = document.querySelectorAll('a');
                    for (const link of links) {
                        if (link.innerText.trim() === chapterText) {
                            link.click();
                            return true;
                        }
                    }
                    return false;
                }""",
                    chapter["text"],
                )

                await self.page.wait_for_timeout(1000)

            # Collect all section links
            section_data = await self.page.evaluate("""() => {
                const links = document.querySelectorAll('a');
                const sections = [];
                for (const link of links) {
                    const href = link.getAttribute('href') || '';
                    const text = link.innerText.trim();
                    if (href.includes('sec=') || text.startsWith('Sec.')) {
                        sections.push({href: href, text: text});
                    }
                }
                return sections;
            }""")

            for sec in section_data:
                href = sec["href"]
                if href and href not in seen_urls:
                    seen_urls.add(href)
                    full_url = urljoin("https://www.akleg.gov", href)
                    sections.append(
                        {
                            "url": full_url,
                            "title": sec["text"],
                        }
                    )

            # Progress update
            if (i + 1) % 5 == 0:
                print(f"[us-ak] Found {len(sections)} sections so far...")

        self.stats.sections_discovered = len(sections)
        return sections

    async def fetch_section(self, section: dict) -> str | None:
        """Fetch a single section's content."""
        try:
            await self.page.goto(
                section["url"],
                wait_until="networkidle",
                timeout=30000,
            )
            await self.page.wait_for_timeout(500)

            content = await self.page.content()
            self.stats.sections_fetched += 1
            self.stats.bytes_fetched += len(content.encode())
            return content

        except Exception as e:
            self.stats.sections_failed += 1
            self.stats.errors.append(f"Fetch {section['url']}: {e}")
            return None

    def upload_to_r2(self, url: str, html: str) -> bool:
        """Upload section HTML to R2."""
        if self.dry_run:
            return True

        try:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            path = urlparse(url).path.strip("/").replace("/", "_")
            query = urlparse(url).query.replace("&", "_").replace("=", "-")
            key = f"us-ak/{path}_{query}_{url_hash}.html"

            self.r2.put_object(
                Bucket=R2_BUCKET,
                Key=key,
                Body=html.encode("utf-8"),
                ContentType="text/html",
            )
            return True
        except Exception as e:
            self.stats.errors.append(f"R2 upload: {e}")
            return False


class TexasCrawler:
    """Crawler for Texas's Angular-based statute site.

    Texas uses Angular Material with dropdown navigation. The URL pattern is:
    https://statutes.capitol.texas.gov/Docs/{CODE}/htm/{CODE}.{CHAPTER}.htm

    Where CODE is a 2-letter abbreviation (TX=Tax, AG=Agriculture, etc.)
    """

    # Code name to URL abbreviation mapping
    CODE_ABBREVS = {
        "Agriculture Code": "AG",
        "Alcoholic Beverage Code": "AL",
        "Business & Commerce Code": "BC",
        "Business Organizations Code": "BO",
        "Civil Practice and Remedies Code": "CP",
        "Code of Criminal Procedure": "CR",
        "Education Code": "ED",
        "Election Code": "EL",
        "Estates Code": "ES",
        "Family Code": "FA",
        "Finance Code": "FI",
        "Government Code": "GV",
        "Health and Safety Code": "HS",
        "Human Resources Code": "HR",
        "Insurance Code": "IN",
        "Labor Code": "LA",
        "Local Government Code": "LG",
        "Natural Resources Code": "NR",
        "Occupations Code": "OC",
        "Parks and Wildlife Code": "PW",
        "Penal Code": "PE",
        "Property Code": "PR",
        "Special District Local Laws Code": "SD",
        "Tax Code": "TX",
        "Transportation Code": "TN",
        "Utilities Code": "UT",
        "Water Code": "WA",
    }

    def __init__(self, page: Page, dry_run: bool = False):
        self.page = page
        self.dry_run = dry_run
        self.stats = PlaywrightStats(jurisdiction="us-tx", name="Texas")
        self._r2 = None

    @property
    def r2(self):
        if self._r2 is None and not self.dry_run:
            self._r2 = get_r2_client()
        return self._r2

    async def discover_sections(self) -> list[dict]:
        """Discover all sections from Texas statutes site."""
        sections = []
        seen_urls = set()

        print("[us-tx] Loading Texas Statutes page...")
        await self.page.goto(
            "https://statutes.capitol.texas.gov/",
            wait_until="load",
            timeout=60000,
        )
        await self.page.wait_for_timeout(5000)

        # Click on Select Code dropdown to get list of codes
        try:
            await self.page.click("mat-select", timeout=10000)
            await self.page.wait_for_timeout(2000)
        except Exception as e:
            print(f"[us-tx] Could not open dropdown: {e}")
            return []

        # Get all code options
        code_names = await self.page.evaluate("""() => {
            const opts = document.querySelectorAll('mat-option');
            return Array.from(opts).map(o => o.innerText.trim()).filter(t => t.includes('Code'));
        }""")

        print(f"[us-tx] Found {len(code_names)} codes")

        # Close dropdown
        await self.page.keyboard.press("Escape")
        await self.page.wait_for_timeout(500)

        for i, code_name in enumerate(code_names):
            abbrev = self.CODE_ABBREVS.get(code_name)
            if not abbrev:
                print(f"[us-tx] Unknown code: {code_name}")
                continue

            print(f"[us-tx] Processing {i + 1}/{len(code_names)}: {code_name}...")

            # Navigate to chapter 1 of this code
            chapter_url = f"https://statutes.capitol.texas.gov/Docs/{abbrev}/htm/{abbrev}.1.htm"

            await self.page.goto(chapter_url, wait_until="load", timeout=60000)
            await self.page.wait_for_timeout(3000)

            # Extract section links from the page
            section_data = await self.page.evaluate("""() => {
                const links = document.querySelectorAll('a');
                const sections = [];
                for (const link of links) {
                    const href = link.getAttribute('href') || '';
                    const text = link.innerText.trim();
                    if (href.includes('.htm#') && text.startsWith('Sec.')) {
                        sections.push({href: href, text: text});
                    }
                }
                return sections;
            }""")

            for sec in section_data:
                href = sec["href"]
                if href and href not in seen_urls:
                    seen_urls.add(href)
                    # Ensure absolute URL
                    if not href.startswith("http"):
                        href = urljoin("https://statutes.capitol.texas.gov/", href)
                    sections.append(
                        {
                            "url": href,
                            "title": sec["text"],
                            "code": code_name,
                        }
                    )

            # Try to find links to other chapters
            chapter_links = await self.page.evaluate(f"""() => {{
                const links = document.querySelectorAll('a');
                const chapters = [];
                for (const link of links) {{
                    const href = link.getAttribute('href') || '';
                    if (href.includes('{abbrev}.') && href.includes('.htm') && !href.includes('#')) {{
                        chapters.push(href);
                    }}
                }}
                return [...new Set(chapters)];
            }}""")

            # Visit each chapter
            for ch_href in chapter_links[:50]:  # Limit chapters per code
                if not ch_href.startswith("http"):
                    ch_href = urljoin("https://statutes.capitol.texas.gov/", ch_href)

                try:
                    await self.page.goto(ch_href, wait_until="load", timeout=30000)
                    await self.page.wait_for_timeout(1000)

                    ch_section_data = await self.page.evaluate("""() => {
                        const links = document.querySelectorAll('a');
                        const sections = [];
                        for (const link of links) {
                            const href = link.getAttribute('href') || '';
                            const text = link.innerText.trim();
                            if (href.includes('.htm#') && text.startsWith('Sec.')) {
                                sections.push({href: href, text: text});
                            }
                        }
                        return sections;
                    }""")

                    for sec in ch_section_data:
                        href = sec["href"]
                        if href and href not in seen_urls:
                            seen_urls.add(href)
                            if not href.startswith("http"):
                                href = urljoin("https://statutes.capitol.texas.gov/", href)
                            sections.append(
                                {
                                    "url": href,
                                    "title": sec["text"],
                                    "code": code_name,
                                }
                            )
                except:
                    pass

            # Progress update
            if (i + 1) % 5 == 0:
                print(f"[us-tx] Found {len(sections)} sections so far...")

        self.stats.sections_discovered = len(sections)
        return sections

    async def fetch_section(self, section: dict) -> str | None:
        """Fetch a single section's content."""
        try:
            await self.page.goto(
                section["url"],
                wait_until="networkidle",
                timeout=30000,
            )
            await self.page.wait_for_timeout(1000)

            content = await self.page.content()
            self.stats.sections_fetched += 1
            self.stats.bytes_fetched += len(content.encode())
            return content

        except Exception as e:
            self.stats.sections_failed += 1
            self.stats.errors.append(f"Fetch {section['url']}: {e}")
            return None

    def upload_to_r2(self, url: str, html: str) -> bool:
        """Upload section HTML to R2."""
        if self.dry_run:
            return True

        try:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            # Extract meaningful path from Texas URLs
            parsed = urlparse(url)
            query = parsed.query.replace("&", "_").replace("=", "-")
            key = f"us-tx/{query}_{url_hash}.html"

            self.r2.put_object(
                Bucket=R2_BUCKET,
                Key=key,
                Body=html.encode("utf-8"),
                ContentType="text/html",
            )
            return True
        except Exception as e:
            self.stats.errors.append(f"R2 upload: {e}")
            return False


async def crawl_state(
    state: str,
    output_dir: Path | None = None,
    max_sections: int | None = None,
    dry_run: bool = False,
    headless: bool = True,
) -> dict:
    """Crawl a SPA state and optionally save to disk.

    This is the main entry point for the unified CLI.

    Args:
        state: State abbreviation (e.g., 'al', 'ak', 'tx')
        output_dir: If provided, save HTML files here instead of R2
        max_sections: Limit number of sections
        dry_run: If True, don't save/upload

    Returns:
        Dict with crawl statistics
    """
    # Normalize jurisdiction
    jurisdiction = f"us-{state.lower()}" if not state.startswith("us-") else state

    stats = await crawl_spa_state(
        jurisdiction=jurisdiction,
        output_dir=output_dir,
        max_sections=max_sections,
        dry_run=dry_run,
        headless=headless,
    )

    return {
        "source": "playwright",
        "sections": stats.sections_fetched,
        "bytes": stats.bytes_fetched,
        "duration": stats.duration,
        "rate": stats.rate,
        "errors": stats.errors_count,
    }


async def crawl_spa_state(
    jurisdiction: str,
    output_dir: Path | None = None,
    max_sections: int | None = None,
    dry_run: bool = False,
    headless: bool = True,
) -> PlaywrightStats:
    """Crawl a single SPA state using Playwright."""
    if jurisdiction not in SPA_STATES:
        available = ", ".join(sorted(SPA_STATES.keys()))
        raise ValueError(f"Unknown SPA state: {jurisdiction}. Available: {available}")

    config = SPA_STATES[jurisdiction]
    print(f"\n{'=' * 60}")
    print(f"Crawling {config['name']} ({jurisdiction})")
    print(f"{'=' * 60}")

    # Prepare output directory if specified
    if output_dir and not dry_run:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Select crawler based on state
        if jurisdiction == "us-al":
            crawler = AlabamaCrawler(page, dry_run)
        elif jurisdiction == "us-ak":
            crawler = AlaskaCrawler(page, dry_run)
        elif jurisdiction == "us-tx":
            crawler = TexasCrawler(page, dry_run)
        else:
            raise ValueError(f"No crawler for {jurisdiction}")

        # Override upload to save to disk if output_dir specified
        if output_dir and not dry_run:

            def save_to_disk(url: str, html: str) -> bool:
                parsed = urlparse(url)
                path_parts = parsed.path.strip("/").replace("/", "_")
                query = (
                    parsed.query.replace("/", "_").replace("=", "-").replace("&", "_")
                    if parsed.query
                    else ""
                )
                fragment = (
                    parsed.fragment.replace("/", "_").replace(".", "-") if parsed.fragment else ""
                )

                # Build filename with all components
                parts = [path_parts] if path_parts else ["index"]
                if query:
                    parts.append(query)
                if fragment:
                    parts.append(fragment)
                filename = "_".join(parts) + ".html"

                # Sanitize: remove invalid filename chars (colons, etc)
                filename = re.sub(r'[:<>"|?*]', "-", filename)
                filename = re.sub(r"https?-__", "", filename)
                filename = re.sub(r"-+", "-", filename)

                # Truncate if too long
                if len(filename) > 200:
                    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
                    filename = f"{path_parts[:100]}_{url_hash}.html"
                    filename = re.sub(r'[:<>"|?*]', "-", filename)

                filepath = output_dir / filename
                filepath.write_text(html, encoding="utf-8")
                crawler.stats.bytes_uploaded += len(html.encode("utf-8"))
                return True

            crawler.upload_to_r2 = save_to_disk

        # Discover sections
        print(f"[{jurisdiction}] Discovering sections...")
        sections = await crawler.discover_sections()

        if max_sections:
            sections = sections[:max_sections]

        print(f"[{jurisdiction}] Found {len(sections)} sections to fetch")

        if not sections:
            crawler.stats.end_time = time.time()
            await browser.close()
            return crawler.stats

        # Fetch sections
        print(f"[{jurisdiction}] Fetching sections...")
        for i, section in enumerate(sections):
            if (i + 1) % 10 == 0:
                print(f"[{jurisdiction}] Progress: {i + 1}/{len(sections)} sections")

            html = await crawler.fetch_section(section)
            if html:
                crawler.upload_to_r2(section["url"], html)

        crawler.stats.end_time = time.time()
        await browser.close()

        return crawler.stats


async def crawl_all_spa_states(
    max_sections: int | None = None,
    dry_run: bool = False,
    headless: bool = True,
) -> list[PlaywrightStats]:
    """Crawl all SPA states."""
    results = []

    for jurisdiction in SPA_STATES:
        try:
            stats = await crawl_spa_state(
                jurisdiction,
                max_sections=max_sections,
                dry_run=dry_run,
                headless=headless,
            )
            results.append(stats)

            print(f"\n[{jurisdiction}] Complete:")
            print(f"  Sections discovered: {stats.sections_discovered}")
            print(f"  Sections fetched: {stats.sections_fetched}")
            print(f"  Sections failed: {stats.sections_failed}")
            print(f"  Data fetched: {stats.bytes_fetched / 1024:.1f} KB")
            print(f"  Duration: {stats.duration:.1f}s")
            print(f"  Rate: {stats.rate:.1f} sections/second")

        except Exception as e:
            print(f"[{jurisdiction}] Failed: {e}")
            results.append(
                PlaywrightStats(
                    jurisdiction=jurisdiction,
                    name=SPA_STATES[jurisdiction]["name"],
                    errors=[str(e)],
                )
            )

    # Summary
    print(f"\n{'=' * 60}")
    print("CRAWL COMPLETE")
    print(f"{'=' * 60}")

    total_sections = sum(s.sections_fetched for s in results)
    total_bytes = sum(s.bytes_fetched for s in results)
    total_time = sum(s.duration for s in results)

    print(f"States: {len(results)}")
    print(f"Total sections: {total_sections:,}")
    print(f"Total data: {total_bytes / 1024 / 1024:.1f} MB")
    print(f"Total time: {total_time:.1f}s")

    return results


@click.command()
@click.argument("jurisdiction", required=False)
@click.option("--all", "crawl_all", is_flag=True, help="Crawl all SPA states")
@click.option("--max-sections", type=int, help="Limit sections per state")
@click.option("--dry-run", is_flag=True, help="Don't upload to R2")
@click.option("--headed", is_flag=True, help="Run with visible browser")
def main(
    jurisdiction: str | None,
    crawl_all: bool,
    max_sections: int | None,
    dry_run: bool,
    headed: bool,
):
    """Crawl JavaScript SPA state statute sites using Playwright.

    Examples:

        # Test Alabama with visible browser
        uv run python -m axiom_corpus.crawl_playwright us-al --max-sections 10 --headed

        # Crawl all SPA states
        uv run python -m axiom_corpus.crawl_playwright --all

        # Dry run Texas
        uv run python -m axiom_corpus.crawl_playwright us-tx --dry-run
    """
    headless = not headed

    if crawl_all:
        asyncio.run(
            crawl_all_spa_states(
                max_sections=max_sections,
                dry_run=dry_run,
                headless=headless,
            )
        )
    elif jurisdiction:
        stats = asyncio.run(
            crawl_spa_state(
                jurisdiction,
                max_sections=max_sections,
                dry_run=dry_run,
                headless=headless,
            )
        )

        print(f"\n{stats.name}:")
        print(f"  Sections discovered: {stats.sections_discovered}")
        print(f"  Sections fetched: {stats.sections_fetched}")
        print(f"  Sections failed: {stats.sections_failed}")
        print(f"  Data fetched: {stats.bytes_fetched / 1024:.1f} KB")
        print(f"  Duration: {stats.duration:.1f}s")
        print(f"  Rate: {stats.rate:.1f} sections/second")
        if stats.errors:
            print(f"  Errors: {len(stats.errors)}")
            for e in stats.errors[:5]:
                print(f"    {e}")
    else:
        click.echo("Available SPA states:")
        for j, config in SPA_STATES.items():
            click.echo(f"  {j}: {config['name']}")
        click.echo("\nUsage: uv run python -m axiom_corpus.crawl_playwright us-al")


if __name__ == "__main__":
    main()
