"""Compile ODCS quality extensions into Lakeflow row expectations."""

from pyspark.sql import DataFrame, functions as F

from framework.data_contract_loader import DataContractError


def executable_rules(contract: dict, severity: str) -> dict[str, str]:
    """Return Databricks row expectations for one ODCS severity.

    ODCS SQL rules are dataset queries returning a comparable value. Lakeflow
    expectations instead require a Boolean expression for each row, so Silver
    contracts use the ODCS custom extension point with the
    ``databricks-lakeflow`` engine.
    """
    compiled: dict[str, str] = {}
    schemas = contract.get("schema", [])
    if len(schemas) != 1:
        raise DataContractError("Runtime contracts must describe exactly one schema object")

    for rule in schemas[0].get("quality", []):
        implementation = rule.get("implementation", {})
        expression = implementation.get("expression") if isinstance(implementation, dict) else None
        if rule.get("severity") != severity:
            continue
        if (
            rule.get("type") != "custom"
            or rule.get("engine") != "databricks-lakeflow"
            or not rule.get("id")
            or not expression
        ):
            raise DataContractError(
                f"Every executable {severity} rule requires id and a "
                "databricks-lakeflow implementation.expression"
            )
        compiled[rule["id"]] = expression
    return compiled


def required_field_rules(contract: dict) -> dict[str, str]:
    """Compile the portable pre-transform subset of a Source contract.

    Required-field checks are deliberately enforced before modelling. Complex
    aggregate, distribution, and cross-entity Source rules remain contract
    evidence and are handled by post-transform/dependency validation.
    """
    rules: dict[str, str] = {}
    for schema in contract.get("schema", []):
        for field in schema.get("properties", []):
            if field.get("required"):
                name = field["name"]
                rules[f"SOURCE_{name.upper()}_REQUIRED"] = f"`{name}` IS NOT NULL"
    return rules


def failed_rule_ids(dataframe: DataFrame, rules: dict[str, str]):
    failures = [
        F.when(~F.coalesce(F.expr(expression), F.lit(False)), F.lit(rule_id))
        for rule_id, expression in rules.items()
    ]
    return F.array_compact(F.array(*failures)) if failures else F.array().cast("array<string>")


def with_quality_evidence(dataframe: DataFrame, contract: dict) -> DataFrame:
    hard_rules = executable_rules(contract, "hard")
    warning_rules = executable_rules(contract, "warn")
    return (
        dataframe
        .withColumn("failed_hard_rules", failed_rule_ids(dataframe, hard_rules))
        .withColumn("failed_warning_rules", failed_rule_ids(dataframe, warning_rules))
        .withColumn(
            "quality_status",
            F.when(F.size("failed_hard_rules") > 0, F.lit("QUARANTINED"))
            .when(F.size("failed_warning_rules") > 0, F.lit("WARNING"))
            .otherwise(F.lit("PASSED")),
        )
    )
