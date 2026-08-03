"""Unit tests for the ODCS-quality-rule-to-Lakeflow-expectation compiler.

``framework.data_quality_validator.executable_rules`` is what turns each
Silver contract's declared quality rules into the native
``@dp.expect_all_or_drop`` / ``@dp.expect_all`` calls that actually enforce
data quality inside the running pipeline. Until now it was only covered
indirectly (via tests that check the *contract YAML shape*), never exercised
directly with fixtures that probe its filtering/validation branches. This
module needs no Spark session or Databricks runtime — it operates on plain
dicts.
"""

import pytest

from framework.data_contract_loader import DataContractError
from framework.data_quality_validator import executable_rules, required_field_rules


def _contract(quality_rules):
    return {"schema": [{"quality": quality_rules}]}


def _rule(**overrides):
    rule = {
        "id": "RULE_A",
        "severity": "error",
        "type": "custom",
        "engine": "databricks-lakeflow",
        "implementation": {"expression": "amount > 0"},
    }
    rule.update(overrides)
    return rule


class TestExecutableRules:
    def test_compiles_a_valid_matching_severity_rule(self):
        contract = _contract([_rule()])
        assert executable_rules(contract, "error") == {"RULE_A": "amount > 0"}

    def test_rule_with_a_different_severity_is_excluded_not_raised(self):
        contract = _contract([_rule(severity="warning")])
        assert executable_rules(contract, "error") == {}

    def test_multiple_rules_at_the_requested_severity_all_compile(self):
        contract = _contract([
            _rule(id="RULE_A", implementation={"expression": "amount > 0"}),
            _rule(id="RULE_B", implementation={"expression": "status IS NOT NULL"}),
        ])
        assert executable_rules(contract, "error") == {
            "RULE_A": "amount > 0",
            "RULE_B": "status IS NOT NULL",
        }

    def test_wrong_type_at_the_requested_severity_raises(self):
        contract = _contract([_rule(type="sql")])
        with pytest.raises(DataContractError):
            executable_rules(contract, "error")

    def test_wrong_engine_at_the_requested_severity_raises(self):
        contract = _contract([_rule(engine="great_expectations")])
        with pytest.raises(DataContractError):
            executable_rules(contract, "error")

    def test_missing_expression_at_the_requested_severity_raises(self):
        contract = _contract([_rule(implementation={})])
        with pytest.raises(DataContractError):
            executable_rules(contract, "error")

    def test_missing_id_at_the_requested_severity_raises(self):
        contract = _contract([_rule(id=None)])
        with pytest.raises(DataContractError):
            executable_rules(contract, "error")

    def test_no_quality_rules_returns_empty_dict(self):
        assert executable_rules({"schema": [{}]}, "error") == {}

    def test_rejects_contracts_with_more_than_one_schema_object(self):
        contract = {"schema": [{"quality": []}, {"quality": []}]}
        with pytest.raises(DataContractError):
            executable_rules(contract, "error")

    def test_rejects_contracts_with_no_schema_object(self):
        with pytest.raises(DataContractError):
            executable_rules({"schema": []}, "error")


class TestRequiredFieldRules:
    def test_builds_not_null_expression_for_required_fields_only(self):
        contract = {
            "schema": [{
                "properties": [
                    {"name": "application_id", "required": True},
                    {"name": "notes", "required": False},
                ],
            }],
        }
        assert required_field_rules(contract) == {
            "SOURCE_APPLICATION_ID_REQUIRED": "`application_id` IS NOT NULL",
        }

    def test_aggregates_required_fields_across_multiple_schema_entries(self):
        contract = {
            "schema": [
                {"properties": [{"name": "a", "required": True}]},
                {"properties": [{"name": "b", "required": True}]},
            ],
        }
        assert set(required_field_rules(contract)) == {"SOURCE_A_REQUIRED", "SOURCE_B_REQUIRED"}

    def test_no_required_fields_returns_empty_dict(self):
        contract = {"schema": [{"properties": [{"name": "optional", "required": False}]}]}
        assert required_field_rules(contract) == {}
