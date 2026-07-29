"""Shared Bronze-to-Silver primitives for incremental and SCD2 models."""

import hashlib
import hmac

from pyspark import pipelines as dp
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from framework.data_contract_loader import load_data_contract
from framework.data_quality_validator import executable_rules
from framework.refresh_policy import downstream_microbatch_spark_conf


CATALOG = "`0-ai-trust`"
TOKEN_SECRET_SCOPE = "g3-zero-trust"
TOKEN_SECRET_KEY = "pii_hash_salt"

SILVER_TABLE_PROPERTIES = {
    "quality": "silver",
    "delta.enableChangeDataFeed": "true",
    "data_classification": "Highly Confidential",
}

JOINED_CDF_CONTROL_COLUMNS = [
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
    "_operation",
    "_sequence_ts",
]
AUDIT_COLUMNS_EXCLUDED_FROM_HISTORY = [
    "pipeline_run_id",
    "source_table",
    "processed_at",
    "masking_status",
]

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("Silver model registration requires an active Spark session")
dbutils = DBUtils(spark)


def _latest_by(dataframe: DataFrame, keys: list[str], ordering: list) -> DataFrame:
    order_columns = [F.col(column) if isinstance(column, str) else column for column in ordering]
    rank = Window.partitionBy(*keys).orderBy(
        *[column.desc_nulls_last() for column in order_columns]
    )
    return dataframe.withColumn("_silver_rank", F.row_number().over(rank)).filter(
        F.col("_silver_rank") == 1
    ).drop("_silver_rank")


def _bronze_cdf(table_name: str) -> DataFrame:
    """Read the Delta change feed; Bronze is append-only, so changes are inserts."""
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.bronze.{table_name}")
        .filter(F.col("_change_type") == "insert")
    )


def cdc_change_stream(dataset: str) -> DataFrame:
    """Return source CDC records incrementally from the Bronze Delta CDF."""
    return _bronze_cdf(f"cdc_{dataset}").withColumn(
        "_sequence_ts",
        F.coalesce(
            F.col("_commit_ts"),
            F.col("_captured_at"),
            F.col("_ingested_at"),
            F.col("_commit_timestamp"),
        ).cast("timestamp"),
    )


def event_change_stream(dataset: str) -> DataFrame:
    """Return immutable events incrementally from the Bronze Delta CDF."""
    return _bronze_cdf(f"event_{dataset}")


def file_change_stream(dataset: str) -> DataFrame:
    """Return immutable file rows incrementally from the Bronze Delta CDF."""
    return _bronze_cdf(f"file_{dataset}")


def current_cdc_snapshot(dataset: str, keys: list[str]) -> DataFrame:
    """Resolve current source state for a private multi-source staging view."""
    source = spark.read.table(f"{CATALOG}.bronze.cdc_{dataset}")
    current = _latest_by(
        source,
        keys,
        ["_source_lsn", "_commit_ts", "_kafka_offset", "_ingested_at"],
    )
    return current.filter(F.col("_operation") != "DELETE")


def trimmed(column):
    value = F.trim(column.cast("string"))
    return F.when(value != "", value)


def masked_email(column):
    value = F.lower(trimmed(column))
    return F.when(
        value.rlike(r"^[^@]+@[^@]+$"),
        F.concat(F.substring_index(value, "@", 1).substr(1, 1), F.lit("***@"), F.substring_index(value, "@", -1)),
    )


def masked_phone(column):
    digits = F.regexp_replace(trimmed(column), r"\D", "")
    return F.when(F.length(digits) >= 2, F.concat(F.substring(digits, 1, 2), F.lit("XX XXX XXX")))


def masked_identifier(column):
    value = F.regexp_replace(trimmed(column), r"\s", "")
    return F.when(F.length(value) >= 3, F.concat(F.lit("********"), F.substring(value, -3, 3)))


def masked_card_number(column):
    value = F.regexp_replace(trimmed(column), r"\D", "")
    return F.when(F.length(value) >= 4, F.concat(F.lit("**** **** **** "), F.substring(value, -4, 4)))


def masked_amount(column):
    amount = F.regexp_replace(trimmed(column), r"[^0-9.-]", "").cast("decimal(18,2)")
    return (
        F.when(amount.isNull(), F.lit(None).cast("string"))
        .when(amount < 0, F.lit("NEGATIVE"))
        .when(amount < 1_000, F.lit("$0-$999"))
        .when(amount < 5_000, F.lit("$1K-$4,999"))
        .when(amount < 10_000, F.lit("$5K-$9,999"))
        .when(amount < 50_000, F.lit("$10K-$49,999"))
        .when(amount < 100_000, F.lit("$50K-$99,999"))
        .when(amount < 500_000, F.lit("$100K-$499,999"))
        .when(amount < 1_000_000, F.lit("$500K-$999,999"))
        .otherwise(F.lit("$1M+"))
    )


def hmac_name_token(column):
    """Use a custom UDF only because Spark has no native HMAC expression."""
    secret = dbutils.secrets.get(scope=TOKEN_SECRET_SCOPE, key=TOKEN_SECRET_KEY).encode("utf-8")

    @F.udf("string")
    def token(value):
        if value is None:
            return None
        canonical = " ".join(str(value).strip().lower().split())
        return hmac.new(secret, canonical.encode("utf-8"), hashlib.sha256).hexdigest()

    return token(column)


def with_audit_columns(
    dataframe: DataFrame,
    source_tables: list[str],
    lineage_columns: list,
    processed_columns: list,
    masking_status: str,
) -> DataFrame:
    """Attach stable row provenance; the pipeline event log remains run-authoritative."""
    lineage = [F.coalesce(column.cast("string"), F.lit("")) for column in lineage_columns]
    processed = [column.cast("timestamp") for column in processed_columns]
    processed_at = processed[0] if len(processed) == 1 else F.greatest(*processed)
    return (
        dataframe
        .withColumn(
            "pipeline_run_id",
            F.sha2(F.concat_ws("|", F.lit(",".join(sorted(source_tables))), *lineage), 256),
        )
        .withColumn("source_table", F.lit(",".join(sorted(source_tables))))
        .withColumn("processed_at", processed_at)
        .withColumn("masking_status", F.lit(masking_status))
    )


def _contract_rules(name: str) -> tuple[dict[str, str], dict[str, str]]:
    contract = load_data_contract(f"silver/{name}.yml")
    return executable_rules(contract, "hard"), executable_rules(contract, "warn")


def _register_scd2_target(
    name: str,
    source_name: str,
    keys: list[str],
    cluster_by: list[str],
    control_columns: list[str],
) -> None:
    target_name = f"{CATALOG}.silver.{name}"
    dp.create_streaming_table(
        name=target_name,
        comment=f"Silver SCD2 entity: {name}",
        spark_conf=downstream_microbatch_spark_conf(),
        table_properties={
            **SILVER_TABLE_PROPERTIES,
            "history_mode": "scd_type_2",
            "pipelines.cdc.tombstoneGCThresholdInSeconds": "604800",
        },
        cluster_by=cluster_by,
    )
    dp.create_auto_cdc_flow(
        target=target_name,
        source=source_name,
        keys=keys,
        sequence_by=F.col("_sequence_ts"),
        apply_as_deletes=F.expr("_operation = 'DELETE'"),
        except_column_list=control_columns,
        stored_as_scd_type="2",
        track_history_except_column_list=AUDIT_COLUMNS_EXCLUDED_FROM_HISTORY,
        name=f"{name}_scd2_changes",
    )


def publish_scd2_model(
    name: str,
    builder,
    keys: list[str],
    cluster_by: list[str] | None = None,
) -> None:
    """Publish a one-source CDC entity as a native AUTO CDC SCD2 table."""
    hard_rules, warning_rules = _contract_rules(name)
    source_name = f"_{name}_validated_changes"

    @dp.temporary_view(name=source_name, comment=f"Validated CDC changes for {name}")
    @dp.expect_all(warning_rules)
    @dp.expect_all_or_drop(hard_rules)
    def validated_changes():
        return builder()

    _register_scd2_target(
        name,
        source_name,
        keys,
        cluster_by or keys,
        ["_operation", "_sequence_ts"],
    )


def publish_joined_scd2_model(
    name: str,
    builder,
    keys: list[str],
    cluster_by: list[str] | None = None,
) -> None:
    """Publish a multi-source entity through a private current-state staging MV."""
    hard_rules, warning_rules = _contract_rules(name)
    staging_name = f"_{name}_current_state"
    source_name = f"_{name}_validated_changes"

    @dp.materialized_view(
        name=staging_name,
        comment=f"Private current-state join for {name}",
        private=True,
        spark_conf=downstream_microbatch_spark_conf(),
        table_properties={
            "quality": "silver_staging",
            "delta.enableChangeDataFeed": "true",
        },
    )
    def current_state():
        return builder()

    @dp.temporary_view(name=source_name, comment=f"Validated staging changes for {name}")
    @dp.expect_all(warning_rules)
    @dp.expect_all_or_drop(hard_rules)
    def validated_changes():
        return (
            spark.readStream
            .option("readChangeFeed", "true")
            .table(staging_name)
            .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
            .withColumn(
                "_operation",
                F.when(F.col("_change_type") == "delete", F.lit("DELETE")).otherwise(F.lit("UPSERT")),
            )
            .withColumn("_sequence_ts", F.col("_commit_timestamp").cast("timestamp"))
        )

    _register_scd2_target(
        name,
        source_name,
        keys,
        cluster_by or keys,
        JOINED_CDF_CONTROL_COLUMNS,
    )


def publish_append_model(
    name: str,
    builder,
    cluster_by: list[str] | None = None,
) -> None:
    """Publish an immutable event/history/file entity as a streaming table."""
    hard_rules, warning_rules = _contract_rules(name)
    target_name = f"{CATALOG}.silver.{name}"

    @dp.table(
        name=target_name,
        comment=f"Silver append-only entity: {name}",
        spark_conf=downstream_microbatch_spark_conf(),
        table_properties={**SILVER_TABLE_PROPERTIES, "history_mode": "append_only"},
        cluster_by=cluster_by,
    )
    @dp.expect_all(warning_rules)
    @dp.expect_all_or_drop(hard_rules)
    def append_only_entity():
        return builder()
