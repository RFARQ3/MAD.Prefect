"""Fail when the downstream coverage map loses evidence or points to missing tests."""

import ast
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
COVERAGE = ROOT / "docs/envblock-downstream-regression-coverage.md"


def coverage_section(start: str, end: str) -> str:
    return COVERAGE.read_text(encoding="utf-8").split(start, 1)[1].split(end, 1)[0]


def test_every_audited_definition_has_one_matching_coverage_row():
    section = coverage_section("## Definition inventory", "## Usage inventory")
    actual = re.findall(r"^\| (\d+) \| (Service\d+) \| ([^|]+) \|$", section, re.M)
    assert [int(number) for number, _, _ in actual] == list(range(1, 49))
    assert len(actual) == 48
    for number, _, families in actual:
        assert (families.strip() == "X") is (32 <= int(number) <= 35)
        if families.strip() != "X":
            assert "D" in families.split(", ")


def test_every_audited_usage_has_one_matching_coverage_row():
    definition_section = coverage_section("## Definition inventory", "## Usage inventory")
    definitions = re.findall(r"^\| (\d+) \| (Service\d+) \|", definition_section, re.M)
    section = coverage_section("## Usage inventory", "## Intentional conversion changes")
    actual = re.findall(r"^\| (\d+) \| (Service\d+) \| ([^|]+) \| ([^|]+) \|$", section, re.M)
    assert [int(number) for number, _, _, _ in actual] == list(range(1, 178))
    assert len(actual) == 177
    for number, service, owners, families in actual:
        assert owners.strip().split(", ") == [item for item, owner in definitions if owner == service]
        assert (families.strip() == "X") is (16 <= int(number) <= 20)
        for family in re.sub(r"\([^)]*\)", "", families).split(", "):
            assert family.strip().split(" ", 1)[0] in {"D", "N", "L", "P", "R", "S", "T", "B", "W", "C", "X"}


def test_coverage_report_references_existing_regression_tests():
    names = set()
    for filename in ("test_env_block.py", "test_env_block_downstream.py", "test_envblock_downstream_usage.py"):
        tree = ast.parse((ROOT / "tests" / filename).read_text(encoding="utf-8"))
        names.update(node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)))
    referenced = set(re.findall(r"`(test_[a-zA-Z0-9_]+)`", COVERAGE.read_text(encoding="utf-8")))
    assert referenced
    assert referenced <= names, f"Coverage report references missing tests: {referenced - names}"
