"""Compile approved contract rules into Spark SQL validation expressions."""

from pyspark.sql import DataFrame, functions as F

from framework.data_contract_loader import DataContractError


def executable_rules(contract: dict, severity: str) -> dict[str, str]:
    compiled = {}
    for rule in contract.get("rules", {}).get(severity, []):
        if not isinstance(rule, dict) or not rule.get("id") or not rule.get("expression"):
            raise DataContractError(
                f"Every {severity} rule requires id and expression before activation"
            )
        compiled[rule["id"]] = rule["expression"]
    return compiled


def with_quality_evidence(dataframe: DataFrame, contract: dict) -> DataFrame:
    hard_rules = executable_rules(contract, "hard")
    warning_rules = executable_rules(contract, "warn")
    hard_failures = [
        F.when(~F.expr(expression), F.lit(rule_id))
        for rule_id, expression in hard_rules.items()
    ]
    warning_failures = [
        F.when(~F.expr(expression), F.lit(rule_id))
        for rule_id, expression in warning_rules.items()
    ]
    hard_failure_array = F.array(*hard_failures) if hard_failures else F.array().cast("array<string>")
    warning_failure_array = (
        F.array(*warning_failures) if warning_failures else F.array().cast("array<string>")
    )
    return (
        dataframe
        .withColumn("failed_hard_rules", F.array_compact(hard_failure_array))
        .withColumn("failed_warning_rules", F.array_compact(warning_failure_array))
        .withColumn(
            "quality_status",
            F.when(F.size("failed_hard_rules") > 0, F.lit("QUARANTINED"))
            .when(F.size("failed_warning_rules") > 0, F.lit("WARNING"))
            .otherwise(F.lit("PASSED")),
        )
    )
