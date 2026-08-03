"""Unit tests for the pure dict/string helpers in dependency_validator.

These back the cross-entity FK and lifecycle-stage dependency checks
(``quarantine.dependency_violations``). No Spark session is required since
none of these three helpers touch a DataFrame.
"""

import pytest

from framework.dependency_validator import _custom_property, _hours, _primary_keys


class TestHours:
    def test_parses_a_plural_hour_value(self):
        assert _hours("2 hours") == 2

    def test_parses_a_singular_hour_value(self):
        assert _hours("1 hour") == 1

    def test_is_case_and_whitespace_tolerant(self):
        assert _hours("  3   HOURS  ") == 3

    def test_non_hour_unit_raises(self):
        with pytest.raises(ValueError):
            _hours("2 days")

    def test_missing_unit_raises(self):
        with pytest.raises(ValueError):
            _hours("2")


class TestCustomProperty:
    def test_returns_the_matching_property_value(self):
        contract = {"customProperties": [{"property": "g3:dependencies", "value": {"a": 1}}]}
        assert _custom_property(contract, "g3:dependencies") == {"a": 1}

    def test_returns_none_when_property_not_present(self):
        assert _custom_property({"customProperties": []}, "g3:dependencies") is None

    def test_returns_none_when_customproperties_key_missing(self):
        assert _custom_property({}, "g3:dependencies") is None


class TestPrimaryKeys:
    def test_returns_only_primary_key_columns_in_schema_order(self):
        contract = {
            "schema": [{
                "properties": [
                    {"name": "application_id", "primaryKey": True},
                    {"name": "notes", "primaryKey": False},
                    {"name": "revision", "primaryKey": True},
                ],
            }],
        }
        assert _primary_keys(contract) == ["application_id", "revision"]

    def test_returns_empty_list_when_no_primary_keys_declared(self):
        contract = {"schema": [{"properties": [{"name": "notes"}]}]}
        assert _primary_keys(contract) == []
