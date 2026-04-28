"""Tests for state spec loader."""


from axiom_corpus.sources.specs import (
    StateSpec,
    get_crawler_type,
    get_section_pattern,
    get_specs_dir,
    is_archive_org_state,
    is_playwright_state,
    load_all_specs,
    load_spec,
)


class TestStateSpec:
    """Tests for StateSpec dataclass."""

    def test_default_values(self):
        """StateSpec has sensible defaults."""
        spec = StateSpec(
            jurisdiction="us-test",
            name="Test State",
            base_url="https://example.com",
        )
        assert spec.crawler_type == "html"
        assert spec.source_type == "html"
        assert spec.toc_urls == []
        assert spec.section_pattern is None
        assert spec.archive_org_id is None
        assert spec.selectors == {"content": "body", "title": "title"}
        assert spec.codes == {}

    def test_full_spec(self):
        """StateSpec accepts all fields."""
        spec = StateSpec(
            jurisdiction="us-test",
            name="Test State",
            base_url="https://example.com",
            crawler_type="playwright",
            source_type="html",
            toc_urls=["/toc1", "/toc2"],
            section_pattern=r"/section/\d+",
            selectors={"content": "div.main", "title": "h1"},
            codes={"1": "Title One"},
        )
        assert spec.jurisdiction == "us-test"
        assert spec.crawler_type == "playwright"
        assert len(spec.toc_urls) == 2
        assert spec.section_pattern == r"/section/\d+"


class TestLoadSpec:
    """Tests for load_spec function."""

    def test_load_existing_spec(self):
        """Load an existing state spec."""
        spec = load_spec("us-mn")
        assert spec is not None
        assert spec.jurisdiction == "us-mn"
        assert spec.name == "Minnesota"
        assert "revisor.mn.gov" in spec.base_url

    def test_load_nonexistent_spec(self):
        """Loading non-existent spec returns None."""
        spec = load_spec("us-nonexistent")
        assert spec is None

    def test_load_archive_org_spec(self):
        """Load Archive.org type spec."""
        spec = load_spec("us-va")
        assert spec is not None
        assert spec.crawler_type == "archive_org"
        assert spec.archive_org_id == "gov.va.code"

    def test_spec_has_codes(self):
        """Specs include code/title mappings."""
        spec = load_spec("us-mn")
        assert spec is not None
        assert len(spec.codes) > 0
        assert "290" in spec.codes  # Income tax
        assert "Individual Income Tax" in spec.codes["290"]

    def test_spec_caching(self):
        """Specs are cached after first load."""
        # Clear cache first
        from axiom_corpus.sources.specs import _specs_cache
        _specs_cache.clear()

        spec1 = load_spec("us-or")
        spec2 = load_spec("us-or")
        assert spec1 is spec2  # Same object (cached)


class TestLoadAllSpecs:
    """Tests for load_all_specs function."""

    def test_load_all_returns_dict(self):
        """load_all_specs returns dictionary of specs."""
        specs = load_all_specs()
        assert isinstance(specs, dict)
        assert len(specs) > 0

    def test_all_specs_have_jurisdiction_keys(self):
        """Dictionary keys match jurisdiction IDs."""
        specs = load_all_specs()
        for key, spec in specs.items():
            assert key == spec.jurisdiction
            assert key.startswith("us-")

    def test_all_specs_have_required_fields(self):
        """All specs have required fields populated."""
        specs = load_all_specs()
        for jurisdiction, spec in specs.items():
            assert spec.name, f"{jurisdiction} missing name"
            assert spec.base_url, f"{jurisdiction} missing base_url"
            assert spec.crawler_type in ("html", "playwright", "archive_org"), \
                f"{jurisdiction} has invalid crawler_type: {spec.crawler_type}"


class TestGetSectionPattern:
    """Tests for get_section_pattern function."""

    def test_html_state_has_pattern(self):
        """HTML crawler states have section patterns."""
        pattern = get_section_pattern("us-mn")
        assert pattern is not None
        assert r"\d" in pattern  # Contains digit pattern

    def test_archive_org_state_no_pattern(self):
        """Archive.org states typically have null pattern."""
        pattern = get_section_pattern("us-va")
        assert pattern is None

    def test_nonexistent_state(self):
        """Non-existent state returns None."""
        pattern = get_section_pattern("us-nonexistent")
        assert pattern is None


class TestGetCrawlerType:
    """Tests for get_crawler_type function."""

    def test_html_crawler(self):
        """HTML states return 'html'."""
        crawler = get_crawler_type("us-mn")
        assert crawler == "html"

    def test_archive_org_crawler(self):
        """Archive.org states return 'archive_org'."""
        crawler = get_crawler_type("us-va")
        assert crawler == "archive_org"

    def test_nonexistent_defaults_html(self):
        """Non-existent state defaults to 'html'."""
        crawler = get_crawler_type("us-nonexistent")
        assert crawler == "html"


class TestIsArchiveOrgState:
    """Tests for is_archive_org_state function."""

    def test_archive_org_state_true(self):
        """Archive.org states return True."""
        assert is_archive_org_state("us-va") is True
        assert is_archive_org_state("us-wy") is True

    def test_html_state_false(self):
        """HTML states return False."""
        assert is_archive_org_state("us-mn") is False
        assert is_archive_org_state("us-or") is False

    def test_nonexistent_state_false(self):
        """Non-existent state returns False."""
        assert is_archive_org_state("us-nonexistent") is False


class TestIsPlaywrightState:
    """Tests for is_playwright_state function."""

    def test_html_state_false(self):
        """HTML states return False."""
        assert is_playwright_state("us-mn") is False

    def test_archive_org_state_false(self):
        """Archive.org states return False."""
        assert is_playwright_state("us-va") is False

    def test_nonexistent_state_false(self):
        """Non-existent state returns False."""
        assert is_playwright_state("us-nonexistent") is False


class TestSpecsDir:
    """Tests for specs directory handling."""

    def test_specs_dir_exists(self):
        """Specs directory exists."""
        specs_dir = get_specs_dir()
        assert specs_dir.exists()
        assert specs_dir.is_dir()

    def test_specs_dir_has_yaml_files(self):
        """Specs directory contains YAML files."""
        specs_dir = get_specs_dir()
        yaml_files = list(specs_dir.glob("us-*.yaml"))
        assert len(yaml_files) > 0
