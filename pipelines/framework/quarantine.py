"""Shared quarantine targets and record envelopes for Silver validation."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F

from framework.data_contract_loader import contract_fingerprint
from framework.data_quality_validator import failed_rule_ids
from framework.refresh_policy import downstream_microbatch_spark_conf


CATALOG = "`0-ai-trust`"
RECORD_FAILURES = f"{CATALOG}.quarantine.record_failures"


dp.create_streaming_table(
    name=RECORD_FAILURES,
    comment="Contract failures excluded from canonical Silver",
    schema="""
      failure_id STRING,
      validation_stage STRING,
      contract_id STRING,
      contract_version STRING,
      entity_name STRING,
      record_key STRING,
      failed_rule_ids ARRAY<STRING>,
      record_payload STRING,
      source_reference STRING,
      processed_at TIMESTAMP,
      contract_hash STRING
    """,
    spark_conf=downstream_microbatch_spark_conf(),
    table_properties={
        "quality": "quarantine",
        "delta.enableChangeDataFeed": "true",
        "data_classification": "Highly Confidential",
    },
    cluster_by=["entity_name", "validation_stage"],
)


def quarantine_envelope(
    dataframe: DataFrame,
    contract: dict,
    entity_name: str,
    keys: list[str],
    hard_rules: dict[str, str],
    validation_stage: str = "POST_TRANSFORM",
) -> DataFrame:
    """Return only failed rows using a stable, schema-independent envelope."""
    failed = failed_rule_ids(dataframe, hard_rules)
    record_key = F.concat_ws("|", *[F.coalesce(F.col(key).cast("string"), F.lit("")) for key in keys])
    processed_at = (
        F.col("processed_at").cast("timestamp")
        if "processed_at" in dataframe.columns
        else F.current_timestamp()
    )
    source_reference = (
        F.col("source_table").cast("string")
        if "source_table" in dataframe.columns
        else F.lit(None).cast("string")
    )
    payload = F.to_json(F.struct(*[F.col(column) for column in dataframe.columns]))
    contract_hash = contract_fingerprint(contract)

    return (
        dataframe
        .withColumn("_failed_rule_ids", failed)
        .filter(F.size("_failed_rule_ids") > 0)
        .select(
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(entity_name),
                    record_key,
                    F.concat_ws(",", F.sort_array("_failed_rule_ids")),
                    processed_at.cast("string"),
                ),
                256,
            ).alias("failure_id"),
            F.lit(validation_stage).alias("validation_stage"),
            F.lit(contract["id"]).alias("contract_id"),
            F.lit(str(contract["version"])).alias("contract_version"),
            F.lit(entity_name).alias("entity_name"),
            record_key.alias("record_key"),
            F.col("_failed_rule_ids").alias("failed_rule_ids"),
            payload.alias("record_payload"),
            source_reference.alias("source_reference"),
            processed_at.alias("processed_at"),
            F.lit(contract_hash).alias("contract_hash"),
        )
    )


def register_record_quarantine_flow(
    entity_name: str,
    builder,
    contract: dict,
    keys: list[str],
    hard_rules: dict[str, str],
    validation_stage: str = "POST_TRANSFORM",
) -> None:
    """Append one entity's failures to the shared protected target."""
    flow_name = f"{entity_name}_{validation_stage.lower()}_quarantine"

    @dp.append_flow(
        target=RECORD_FAILURES,
        name=flow_name,
        spark_conf=downstream_microbatch_spark_conf(),
        comment=f"Quarantine {validation_stage.lower()} failures for {entity_name}",
    )
    def quarantined_records():
        return quarantine_envelope(
            builder(), contract, entity_name, keys, hard_rules, validation_stage
        )
