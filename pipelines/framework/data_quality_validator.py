"""Compile approved contract rules into Spark SQL validation expressions."""

from pyspark.sql import DataFrame, functions as F

from framework.data_contract_loader import DataContractError


def executable_rules(contract: dict, severity: str) -> dict[str, str]:
    """Pull one severity's rules (e.g. "hard" or "warn") out of a contract dict.

    Returns a {rule_id: sql_expression} mapping. Every rule must already have
    both an "id" and an "expression" — this is enforced here, before any SQL
    runs, so a contract with a typo or a half-written rule fails fast with a
    clear error instead of silently skipping validation or crashing later
    inside Spark with a confusing stack trace.
    """
    compiled = {}
    for rule in contract.get("rules", {}).get(severity, []):
        if not isinstance(rule, dict) or not rule.get("id") or not rule.get("expression"):
            raise DataContractError(
                f"Every {severity} rule requires id and expression before activation"
            )
        compiled[rule["id"]] = rule["expression"]
    return compiled


def with_quality_evidence(dataframe: DataFrame, contract: dict) -> DataFrame:
    """Check every row of `dataframe` against the contract's rules and label it.

    What this does, in plain terms:
      1. Load the contract's "hard" rules (must pass) and "warn" rules (nice
         to pass) as SQL boolean expressions.
      2. For each rule, test every row: if the rule's condition is NOT true,
         record that rule's id as a failure for that row.
         Note: rule expressions come straight from the contract YAML and are
         run as live SQL via F.expr — this is safe only because
         load_data_contract already required the contract to be
         APPROVED/ACTIVE, i.e. someone has reviewed these expressions before
         they can execute here.
      3. Attach two new columns per row:
         - "failed_hard_rules": list of hard rule ids that failed (empty if none)
         - "failed_warning_rules": list of warn rule ids that failed (empty if none)
      4. Attach a "quality_status" column summarising the row's outcome:
         - "QUARANTINED" if any hard rule failed (row is not trustworthy)
         - "WARNING" if only warn rules failed (row is usable but flagged)
         - "PASSED" if everything passed
    Downstream tables (see silver_cdc_transform.py) use "quality_status" to
    split rows into the clean table vs. the quarantine table.
    """
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
    # F.array() of zero elements loses its element type, so it must be cast back
    # to array<string> explicitly when a severity has no rules defined.
    hard_failure_array = F.array(*hard_failures) if hard_failures else F.array().cast("array<string>")
    warning_failure_array = (
        F.array(*warning_failures) if warning_failures else F.array().cast("array<string>")
    )
    return (
        dataframe
        # array_compact drops the nulls left behind by rules that passed
        # (F.when with no matching branch produces null), leaving only failures.
        .withColumn("failed_hard_rules", F.array_compact(hard_failure_array))
        .withColumn("failed_warning_rules", F.array_compact(warning_failure_array))
        .withColumn(
            "quality_status",
            F.when(F.size("failed_hard_rules") > 0, F.lit("QUARANTINED"))
            .when(F.size("failed_warning_rules") > 0, F.lit("WARNING"))
            .otherwise(F.lit("PASSED")),
        )
    )
