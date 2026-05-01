"""AI encoding pipeline: Claude reads statute, writes DSL."""

import contextlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml
from anthropic import Anthropic

from axiom_corpus.models import Section


@dataclass
class Encoding:
    """Result of encoding a statute section into DSL."""

    citation: str
    dsl: str
    test_cases: list[dict]
    encoded_by: str
    encoded_at: datetime
    model: str
    prompt_tokens: int
    completion_tokens: int


SYSTEM_PROMPT = """You are an expert at encoding US tax and benefit law into RuleSpec.

Given a statute section, produce executable DSL code that implements the legal rules.

## RuleSpec Syntax

```rulespec
// Variable definition
variable earned_income_credit {
  entity TaxUnit
  period Year
  dtype Money
  reference "26 USC § 32"

  formula {
    let phase_in = variable(eitc_phase_in)
    let max_credit = parameter(gov.irs.eitc.max_amount)
    let phase_out = variable(eitc_phase_out)
    return max(0, min(phase_in, max_credit) - phase_out)
  }
}

// Parameter definition (time-varying values from statute)
parameter gov.irs.eitc.phase_in_rate {
  description "EITC phase-in rate by number of children"
  reference "26 USC § 32(b)(1)"
  values {
    2024-01-01: {
      0: 0.0765,
      1: 0.34,
      2: 0.40,
      3: 0.45
    }
  }
}
```

## Key Rules

1. **Path = Citation**: Variable paths should mirror statute structure
   - `statute/26/32/a/1/` for 26 USC § 32(a)(1)

2. **Entity Types**: TaxUnit, Person, Household, SPMUnit

3. **Period Types**: Year, Month

4. **Data Types**: Money, Rate, Boolean, Integer, Enum

5. **Parameters**: Extract dollar amounts and percentages as parameters
   - Use `parameter(gov.irs.section.name)` syntax
   - Include reference to statute subsection

6. **Formulas**: Use functional style
   - `let` bindings for intermediate values
   - `variable(name)` to reference other variables
   - `parameter(path)` for policy parameters
   - `where`, `sum`, `any`, `all` for aggregations

## Output Format

Respond with:
1. **DSL Code**: The complete .yaml file content
2. **Test Cases**: YAML test cases covering edge cases
3. **Notes**: Any ambiguities or assumptions made

Wrap DSL in ```rulespec ... ``` blocks.
Wrap tests in ```yaml ... ``` blocks.
"""


def encode_section(section: Section, model: str = "claude-sonnet-4-20250514") -> Encoding:
    """Encode a statute section into RuleSpec using Claude.

    Args:
        section: The statute section to encode
        model: Claude model to use

    Returns:
        Encoding result with DSL code and test cases
    """
    client = Anthropic()

    # Build the user prompt
    user_prompt = f"""Encode the following statute section into RuleSpec:

## Citation
{section.citation.title} USC § {section.citation.section}
{section.section_title}

## Statute Text
{section.text[:15000]}  # Truncate if too long

## Subsections
"""
    for sub in section.subsections[:10]:  # Limit subsections
        user_prompt += f"\n### ({sub.identifier}) {sub.heading or ''}\n{sub.text[:2000]}\n"

    # Call Claude
    response = client.messages.create(
        model=model,
        max_tokens=8000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    # Extract DSL and tests from response
    content = response.content[0].text
    dsl = _extract_code_block(content, "rulespec")
    tests_yaml = _extract_code_block(content, "yaml")

    # Parse test cases
    test_cases = []
    if tests_yaml:
        with contextlib.suppress(Exception):
            test_cases = yaml.safe_load(tests_yaml) or []

    return Encoding(
        citation=f"{section.citation.title} USC § {section.citation.section}",
        dsl=dsl,
        test_cases=test_cases if isinstance(test_cases, list) else [test_cases],
        encoded_by=f"claude-{model}",
        encoded_at=datetime.now(),
        model=model,
        prompt_tokens=response.usage.input_tokens,
        completion_tokens=response.usage.output_tokens,
    )


def _extract_code_block(text: str, language: str) -> str:
    """Extract a code block with the given language from text."""
    pattern = rf"```{language}\n(.*?)```"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return ""


def encode_and_save(
    section: Section,
    output_dir: Path,
    model: str = "claude-sonnet-4-20250514",
) -> Encoding:
    """Encode a section and save to the local workspace.

    Args:
        section: Section to encode
        output_dir: Directory to save output (e.g., ~/.axiom/workspace)
        model: Claude model to use

    Returns:
        The encoding result
    """
    encoding = encode_section(section, model)

    # Create output directory
    section_dir = (
        output_dir / "federal" / "statute" / str(section.citation.title) / section.citation.section
    )
    section_dir.mkdir(parents=True, exist_ok=True)

    # Save statute text
    (section_dir / "statute.md").write_text(
        f"# {section.citation.title} USC § {section.citation.section}\n\n"
        f"## {section.section_title}\n\n"
        f"{section.text}\n"
    )

    # Save DSL
    (section_dir / "rules.yaml").write_text(encoding.dsl)

    # Save tests
    if encoding.test_cases:
        (section_dir / "tests.yaml").write_text(
            yaml.dump(encoding.test_cases, default_flow_style=False)
        )

    # Save metadata
    metadata = {
        "citation": encoding.citation,
        "encoded_by": encoding.encoded_by,
        "encoded_at": encoding.encoded_at.isoformat(),
        "model": encoding.model,
        "prompt_tokens": encoding.prompt_tokens,
        "completion_tokens": encoding.completion_tokens,
    }
    (section_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

    return encoding
