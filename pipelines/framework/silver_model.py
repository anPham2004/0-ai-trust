"""Shared Bronze-to-Silver primitives for incremental and SCD2 models."""

import hashlib
import hmac

from pyspark import pipelines as dp
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from framework.data_contract_loader import CONTRACT_ROOT, load_layer_contract
from framework.data_quality_validator import executable_rules, required_field_rules
from framework.quarantine import register_record_quarantine_flow
from framework.refresh_policy import downstream_microbatch_spark_conf


CATALOG = "`0-ai-trust`"
TOKEN_SECRET_SCOPE = "g3-zero-trust"
TOKEN_SECRET_KEY = "pii_hash_salt"

SILVER_TABLE_PROPERTIES = {
    "quality": "silver",
    "delta.enableChangeDataFeed": "true",
    "data_classification": "Highly Confidential",
}
APPEND_DEDUPLICATION_WATERMARK = "7 days"

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
SOURCE_VALIDATED_VIEWS: dict[tuple[str, str], str] = {}


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
    return spark.readStream.table(SOURCE_VALIDATED_VIEWS[("DATABASE", dataset)]).withColumn(
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
    return spark.readStream.table(SOURCE_VALIDATED_VIEWS[("EVENT", dataset)])


def file_change_stream(dataset: str) -> DataFrame:
    """Return immutable file rows incrementally from the Bronze Delta CDF."""
    return spark.readStream.table(SOURCE_VALIDATED_VIEWS[("FILE", dataset)])


def current_cdc_snapshot(dataset: str, keys: list[str]) -> DataFrame:
    """Resolve current source state for a private multi-source staging view."""
    contract = load_layer_contract("source", dataset)
    required = required_field_rules(contract)
    source = spark.read.table(f"{CATALOG}.bronze.cdc_{dataset}")
    if required:
        source = source.filter(" AND ".join(f"({expression})" for expression in required.values()))
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
    """Attach stable source-batch provenance.

    ``pipeline_run_id`` is the deterministic logical ingestion run for the
    contributing source batches. The Databricks update ID remains available in
    the pipeline event log and is not exposed as a supported row expression.
    """
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


def _contract_rules(name: str) -> tuple[dict, dict[str, str], dict[str, str]]:
    contract = load_layer_contract("silver", name)
    return (
        contract,
        executable_rules(contract, "error"),
        executable_rules(contract, "warning"),
    )


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
    contract, hard_rules, warning_rules = _contract_rules(name)
    source_name = f"_{name}_validated_changes"

    @dp.temporary_view(name=source_name, comment=f"Validated CDC changes for {name}")
    @dp.expect_all(warning_rules)
    @dp.expect_all_or_drop(hard_rules)
    def validated_changes():
        return builder()

    register_record_quarantine_flow(name, builder, contract, keys, hard_rules)

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
    """Publish a composite entity from periodic joined snapshots as SCD2.

    Lakeflow does not expose a materialized view's CDF metadata through its
    logical name inside the defining pipeline. AUTO CDC FROM SNAPSHOT is the
    native API for comparing each refreshed joined state with the previous one.
    """
    contract, hard_rules, warning_rules = _contract_rules(name)
    source_name = f"_{name}_validated_snapshot"

    @dp.materialized_view(
        name=source_name,
        comment=f"Validated private current-state snapshot for {name}",
        private=True,
        spark_conf=downstream_microbatch_spark_conf(),
        table_properties={"quality": "silver_staging"},
    )
    @dp.expect_all(warning_rules)
    @dp.expect_all_or_drop(hard_rules)
    def validated_snapshot():
        return builder()

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
        cluster_by=cluster_by or keys,
    )
    dp.create_auto_cdc_from_snapshot_flow(
        target=target_name,
        source=source_name,
        keys=keys,
        stored_as_scd_type="2",
        track_history_except_column_list=AUDIT_COLUMNS_EXCLUDED_FROM_HISTORY,
        name=f"{name}_scd2_snapshots",
    )


def publish_append_model(
    name: str,
    builder,
    keys: list[str],
    cluster_by: list[str] | None = None,
) -> None:
    """Publish idempotent immutable records as a streaming table.

    Pipeline checkpoints prevent a source row from being reprocessed. The
    watermark additionally removes business duplicates emitted more than once
    by an at-least-once source or repeated file delivery.
    """
    contract, hard_rules, warning_rules = _contract_rules(name)
    target_name = f"{CATALOG}.silver.{name}"
    source_name = f"_{name}_validated_records"

    @dp.temporary_view(name=source_name, comment=f"Validated immutable records for {name}")
    @dp.expect_all(warning_rules)
    @dp.expect_all_or_drop(hard_rules)
    def validated_records():
        return builder()

    register_record_quarantine_flow(name, builder, contract, keys, hard_rules)

    @dp.table(
        name=target_name,
        comment=f"Silver append-only entity: {name}",
        spark_conf=downstream_microbatch_spark_conf(),
        table_properties={**SILVER_TABLE_PROPERTIES, "history_mode": "append_only"},
        cluster_by=cluster_by or keys,
    )
    def append_only_entity():
        return (
            spark.readStream.table(source_name)
            .withWatermark("processed_at", APPEND_DEDUPLICATION_WATERMARK)
            .dropDuplicatesWithinWatermark(keys)
        )


def _source_contract_references() -> list[tuple[str, str]]:
    references: set[tuple[str, str]] = set()
    silver_root = CONTRACT_ROOT / "silver"
    for path in silver_root.rglob("*.yml"):
        contract = load_layer_contract("silver", path.stem.replace("-", "_"), require_active=False)
        sources = next(
            (item.get("value", []) for item in contract.get("customProperties", [])
             if item.get("property") == "g3:sources"),
            [],
        )
        for source in sources:
            references.add((source["type"], source["source_dataset"].removeprefix("public.")))
    return sorted(references)


def _register_source_validations() -> None:
    prefixes = {"DATABASE": "cdc", "EVENT": "event", "FILE": "file"}
    for source_type, dataset in _source_contract_references():
        contract = load_layer_contract("source", dataset)
        rules = required_field_rules(contract)
        keys = [
            field["name"]
            for field in contract["schema"][0]["properties"]
            if field.get("primaryKey")
        ]
        source_table = f"{prefixes[source_type]}_{dataset}"
        view_name = f"_source_{source_table}_validated"
        SOURCE_VALIDATED_VIEWS[(source_type, dataset)] = view_name

        def source_builder(table_name=source_table):
            return _bronze_cdf(table_name)

        @dp.temporary_view(
            name=view_name,
            comment=f"Source-contract pre-validation for {dataset}",
        )
        @dp.expect_all_or_drop(rules)
        def validated_source(builder=source_builder):
            return builder()

        register_record_quarantine_flow(
            f"source_{dataset}",
            source_builder,
            contract,
            keys,
            rules,
            validation_stage="PRE_TRANSFORM",
        )


_register_source_validations()
