"""Contract-driven cross-entity validation for canonical Silver outputs."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from framework.data_contract_loader import load_layer_contract
from framework.refresh_policy import downstream_microbatch_spark_conf


CATALOG = "`0-ai-trust`"
DEPENDENCY_CONTRACTS = (
    "app_stage_history",
    "app_status_change",
    "app_missing_document",
    "app_lifecycle_event",
    "evt_case_event",
)
SCD2_ENTITIES = {"app_missing_document", "app_application", "evt_service_case"}


def _custom_property(contract: dict, name: str):
    return next(
        (item.get("value") for item in contract.get("customProperties", [])
         if item.get("property") == name),
        None,
    )


def _primary_keys(contract: dict) -> list[str]:
    return [
        item["name"]
        for item in contract["schema"][0]["properties"]
        if item.get("primaryKey")
    ]


def _hours(value: str) -> int:
    amount, unit = value.strip().lower().split(maxsplit=1)
    if not unit.startswith("hour"):
        raise ValueError(f"Only hour-based dependency grace periods are supported: {value}")
    return int(amount)


def _foreign_key_queries() -> list[str]:
    queries: list[str] = []
    for child_name in DEPENDENCY_CONTRACTS:
        contract = load_layer_contract("silver", child_name)
        dependency = _custom_property(contract, "g3:dependencies") or {}
        grace_hours = _hours(dependency["grace_period"])
        child_keys = _primary_keys(contract)
        child_key = "concat_ws('|', " + ", ".join(
            f"coalesce(c.`{column}`, '')" for column in child_keys
        ) + ")"
        child_current = "AND c.`__END_AT` IS NULL" if child_name in SCD2_ENTITIES else ""

        for rule in dependency.get("foreign_keys", []):
            parent_name = rule["parent_contract"]
            joins = " AND ".join(
                f"c.`{child}` = p.`{parent}`"
                for child, parent in zip(rule["columns"], rule["parent_columns"])
            )
            populated = " AND ".join(f"c.`{column}` IS NOT NULL" for column in rule["columns"])
            parent_current = "AND p.`__END_AT` IS NULL" if parent_name in SCD2_ENTITIES else ""
            parent_key = "concat_ws('|', " + ", ".join(
                f"coalesce(c.`{column}`, '')" for column in rule["columns"]
            ) + ")"
            queries.append(f"""
                SELECT
                  sha2(concat_ws('|', '{rule['id']}', {child_key}), 256) AS violation_id,
                  '{rule['id']}' AS rule_id,
                  '{child_name}' AS child_entity,
                  {child_key} AS child_key,
                  '{parent_name}' AS parent_entity,
                  {parent_key} AS parent_key,
                  'OPEN' AS violation_status,
                  c.processed_at + INTERVAL {grace_hours} HOURS AS grace_expires_at,
                  current_timestamp() AS detected_at,
                  to_json(named_struct('reason', 'parent_not_found')) AS details
                FROM {CATALOG}.silver.{child_name} c
                LEFT JOIN {CATALOG}.silver.{parent_name} p
                  ON {joins} {parent_current}
                WHERE {populated}
                  {child_current}
                  AND c.processed_at < current_timestamp() - INTERVAL {grace_hours} HOURS
                  AND p.`{rule['parent_columns'][0]}` IS NULL
            """)
    return queries


def _stage_queries() -> list[str]:
    contract = load_layer_contract("silver", "app_stage_history")
    dependency = _custom_property(contract, "g3:dependencies")
    lifecycle = dependency["lifecycle"]
    grace_hours = _hours(dependency["grace_period"])
    core = ", ".join(f"'{stage}'" for stage in lifecycle["required_core_stages"])
    terminal_cases = " ".join(
        f"WHEN '{outcome}' THEN '{stage}'"
        for outcome, stage in lifecycle["terminal_stage_by_outcome"].items()
    )
    transitions = ", ".join(f"'{item}'" for item in lifecycle["allowed_transitions"])

    completeness = f"""
        WITH observed AS (
          SELECT application_id, collect_set(upper(stage)) AS stages
          FROM {CATALOG}.silver.app_stage_history
          GROUP BY application_id
        ), evaluated AS (
          SELECT
            a.application_id,
            a.processed_at,
            coalesce(o.stages, array()) AS stages,
            filter(
              array({core}),
              stage -> NOT array_contains(coalesce(o.stages, array()), stage)
            ) AS missing_core,
            CASE upper(a.final_outcome) {terminal_cases} END AS expected_terminal
          FROM {CATALOG}.silver.app_application a
          LEFT JOIN observed o ON a.application_id = o.application_id
          WHERE a.`__END_AT` IS NULL
            AND upper(a.final_outcome) IN ({', '.join(repr(key) for key in lifecycle['terminal_stage_by_outcome'])})
            AND a.processed_at < current_timestamp() - INTERVAL {grace_hours} HOURS
        )
        SELECT
          sha2(concat_ws('|', 'APPLICATION_STAGE_COMPLETENESS', application_id), 256) AS violation_id,
          'APPLICATION_STAGE_COMPLETENESS' AS rule_id,
          'app_stage_history' AS child_entity,
          application_id AS child_key,
          'app_application' AS parent_entity,
          application_id AS parent_key,
          'OPEN' AS violation_status,
          processed_at + INTERVAL {grace_hours} HOURS AS grace_expires_at,
          current_timestamp() AS detected_at,
          to_json(named_struct(
            'missing_core_stages', missing_core,
            'expected_terminal_stage', expected_terminal,
            'observed_stages', stages
          )) AS details
        FROM evaluated
        WHERE size(missing_core) > 0
           OR expected_terminal IS NULL
           OR NOT array_contains(stages, expected_terminal)
    """

    transitions_query = f"""
        WITH ordered AS (
          SELECT
            history_id,
            application_id,
            processed_at,
            upper(stage) AS stage,
            lag(upper(stage)) OVER (
              PARTITION BY application_id ORDER BY entered_at, history_id
            ) AS previous_stage
          FROM {CATALOG}.silver.app_stage_history
        )
        SELECT
          sha2(concat_ws('|', 'APPLICATION_STAGE_TRANSITION', history_id), 256) AS violation_id,
          'APPLICATION_STAGE_TRANSITION' AS rule_id,
          'app_stage_history' AS child_entity,
          history_id AS child_key,
          'app_application' AS parent_entity,
          application_id AS parent_key,
          'OPEN' AS violation_status,
          processed_at + INTERVAL {grace_hours} HOURS AS grace_expires_at,
          current_timestamp() AS detected_at,
          to_json(named_struct(
            'previous_stage', previous_stage,
            'current_stage', stage
          )) AS details
        FROM ordered
        WHERE previous_stage IS NOT NULL
          AND processed_at < current_timestamp() - INTERVAL {grace_hours} HOURS
          AND concat(previous_stage, '->', stage) NOT IN ({transitions})
    """
    return [completeness, transitions_query]


def register_dependency_validation() -> None:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Dependency validation requires an active Spark session")
    query = "\nUNION ALL\n".join(_foreign_key_queries() + _stage_queries())

    @dp.materialized_view(
        name=f"{CATALOG}.quarantine.dependency_violations",
        comment="Current cross-entity violations that remain unresolved after contract grace periods",
        spark_conf=downstream_microbatch_spark_conf(),
        table_properties={
            "quality": "quarantine",
            "data_classification": "Highly Confidential",
        },
        cluster_by=["child_entity", "rule_id"],
    )
    def dependency_violations():
        return spark.sql(query)
