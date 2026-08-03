"""Prove the DQ-rule compiler is actually wired correctly for every Silver entity.

tests/contracts/test_data_contracts.py already checks that each contract's
quality rules are *structurally* valid ODCS (id/type/engine/expression
present). This goes one step further: it runs the real compiler
(``executable_rules``, the function that produces the native
``@dp.expect_all_or_drop`` / ``@dp.expect_all`` expectations actually enforced
by the pipeline) against every real Silver contract, at both severities, and
checks nothing declared gets silently lost. This is what makes category-2
"DQ runs as part of every pipeline run" trustworthy rather than aspirational:
a contract rule that looks fine in YAML but wouldn't actually compile (or
wouldn't actually get attached) is caught here, at pytest speed, with no live
Databricks connection required.
"""

from pathlib import Path

import pytest
import yaml

from framework.data_quality_validator import executable_rules

ROOT = Path(__file__).resolve().parents[2]
SILVER_CONTRACTS = sorted((ROOT / "contracts/silver").rglob("*.yml"))


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _declared_rule_ids(contract: dict, severity: str) -> set[str]:
    return {
        rule["id"]
        for rule in contract["schema"][0].get("quality", [])
        if rule.get("severity") == severity
    }


def test_exactly_the_approved_nineteen_silver_contracts_are_checked():
    assert len(SILVER_CONTRACTS) == 19


@pytest.mark.parametrize("path", SILVER_CONTRACTS, ids=lambda path: path.stem)
def test_every_declared_error_rule_compiles_to_an_expectation(path):
    contract = _load(path)
    declared = _declared_rule_ids(contract, "error")
    compiled = executable_rules(contract, "error")
    assert set(compiled) == declared, path


@pytest.mark.parametrize("path", SILVER_CONTRACTS, ids=lambda path: path.stem)
def test_every_declared_warning_rule_compiles_to_an_expectation(path):
    contract = _load(path)
    declared = _declared_rule_ids(contract, "warning")
    compiled = executable_rules(contract, "warning")
    assert set(compiled) == declared, path


def test_at_least_one_contract_declares_a_hard_error_rule():
    assert any(
        rule.get("severity") == "error"
        for path in SILVER_CONTRACTS
        for rule in _load(path)["schema"][0].get("quality", [])
    )
