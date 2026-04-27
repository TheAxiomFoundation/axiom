"""Tests for the encoder module.

Tests cover Encoding dataclass, prompt construction, code extraction,
and file saving logic with mocked Claude API calls.
"""

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

from atlas.encoder import (
    SYSTEM_PROMPT,
    Encoding,
    _extract_code_block,
    encode_and_save,
    encode_section,
)
from atlas.models import Citation, Section, Subsection

# =============================================================================
# Helpers
# =============================================================================


def _make_section():
    return Section(
        citation=Citation(title=26, section="32"),
        title_name="Internal Revenue Code",
        section_title="Earned income tax credit",
        text="A tax credit is allowed...",
        subsections=[
            Subsection(identifier="a", text="Allowance of credit"),
            Subsection(identifier="b", text="Percentages and amounts"),
        ],
        source_url="https://uscode.house.gov/view.xhtml?req=26+USC+32",
        retrieved_at=date(2024, 1, 1),
    )


# =============================================================================
# Tests
# =============================================================================


class TestEncoding:
    def test_create(self):
        enc = Encoding(
            citation="26 USC 32",
            dsl="variable eitc { ... }",
            test_cases=[{"name": "test1"}],
            encoded_by="claude-opus",
            encoded_at=datetime(2024, 1, 1),
            model="claude-opus-4-20250514",
            prompt_tokens=1000,
            completion_tokens=2000,
        )
        assert enc.citation == "26 USC 32"
        assert enc.prompt_tokens == 1000


class TestExtractCodeBlock:
    def test_extract_rulespec(self):
        text = """Here is the code:

```rulespec
variable eitc {
  entity TaxUnit
}
```

And some more text.
"""
        result = _extract_code_block(text, "rulespec")
        assert "variable eitc" in result
        assert "entity TaxUnit" in result

    def test_extract_yaml(self):
        text = """Test cases:

```yaml
- name: test1
  input:
    earned_income: 10000
```
"""
        result = _extract_code_block(text, "yaml")
        assert "name: test1" in result

    def test_no_match(self):
        text = "No code blocks here"
        result = _extract_code_block(text, "rulespec")
        assert result == ""


class TestEncodeSection:
    @patch("atlas.encoder.Anthropic")
    def test_encode_section(self, mock_anthropic_cls):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [
            MagicMock(
                text="""Here is the encoding:

```rulespec
variable earned_income_credit {
  entity TaxUnit
  period Year
  dtype Money
}
```

```yaml
- name: basic_test
  input:
    earned_income: 10000
  expected:
    eitc: 500
```
"""
            )
        ]
        mock_response.usage.input_tokens = 500
        mock_response.usage.output_tokens = 800
        mock_client.messages.create.return_value = mock_response

        section = _make_section()
        result = encode_section(section)

        assert isinstance(result, Encoding)
        assert "earned_income_credit" in result.dsl
        assert len(result.test_cases) > 0
        assert result.prompt_tokens == 500
        assert result.completion_tokens == 800

    @patch("atlas.encoder.Anthropic")
    def test_encode_section_no_tests(self, mock_anthropic_cls):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="```rulespec\nvariable eitc {}\n```\n\nNo tests.")]
        mock_response.usage.input_tokens = 100
        mock_response.usage.output_tokens = 50
        mock_client.messages.create.return_value = mock_response

        section = _make_section()
        result = encode_section(section)
        assert result.dsl == "variable eitc {}"
        assert result.test_cases == []


class TestEncodeAndSave:
    @patch("atlas.encoder.encode_section")
    def test_encode_and_save(self, mock_encode, tmp_path):
        mock_encode.return_value = Encoding(
            citation="26 USC 32",
            dsl="variable eitc { ... }",
            test_cases=[{"name": "test1", "input": {"ei": 10000}}],
            encoded_by="claude-opus",
            encoded_at=datetime(2024, 1, 1),
            model="claude-opus-4-20250514",
            prompt_tokens=500,
            completion_tokens=800,
        )

        section = _make_section()
        result = encode_and_save(section, tmp_path)

        assert result.citation == "26 USC 32"

        # Check files were created
        section_dir = tmp_path / "federal" / "statute" / "26" / "32"
        assert (section_dir / "statute.md").exists()
        assert (section_dir / "rules.yaml").exists()
        assert (section_dir / "tests.yaml").exists()
        assert (section_dir / "metadata.json").exists()

        # Check content
        statute = (section_dir / "statute.md").read_text()
        assert "26 USC" in statute

        metadata = json.loads((section_dir / "metadata.json").read_text())
        assert metadata["citation"] == "26 USC 32"
        assert metadata["prompt_tokens"] == 500

    @patch("atlas.encoder.encode_section")
    def test_encode_and_save_no_tests(self, mock_encode, tmp_path):
        mock_encode.return_value = Encoding(
            citation="26 USC 32",
            dsl="variable eitc {}",
            test_cases=[],
            encoded_by="claude-opus",
            encoded_at=datetime(2024, 1, 1),
            model="claude-opus-4-20250514",
            prompt_tokens=100,
            completion_tokens=50,
        )

        section = _make_section()
        encode_and_save(section, tmp_path)

        section_dir = tmp_path / "federal" / "statute" / "26" / "32"
        assert not (section_dir / "tests.yaml").exists()


class TestSystemPrompt:
    def test_prompt_exists(self):
        assert len(SYSTEM_PROMPT) > 100

    def test_prompt_mentions_rulespec(self):
        assert "RuleSpec" in SYSTEM_PROMPT or "rulespec" in SYSTEM_PROMPT.lower()
