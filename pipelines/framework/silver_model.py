"""Small, shared primitives for the approved Bronze-to-Silver model."""

import hashlib
import hmac

from pyspark import pipelines as dp
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from framework.data_contract_loader import load_data_contract
from framework.data_quality_validator import executable_rules, with_quality_evidence
from framework.refresh_policy import downstream_microbatch_spark_conf


CATALOG = "`0-ai-trust`"
TOKEN_SECRET_SCOPE = "g3-zero-trust"
TOKEN_SECRET_KEY = "pii_hash_salt"

SILVER_TABLE_PROPERTIES = {
    "quality": "silver",
    "delta.enableChangeDataFeed": "true",
    "data_classification": "Highly Confidential",
}

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


def latest_cdc(dataset: str, keys: list[str]) -> DataFrame:
    """Return the current source state using source-native ordering and deletes."""
    source = spark.read.table(f"{CATALOG}.bronze.cdc_{dataset}")
    current = _latest_by(
        source,
        keys,
        ["_source_lsn", "_commit_ts", "_kafka_offset", "_ingested_at"],
    )
    return current.filter(F.col("_operation") != "DELETE")


def latest_event(dataset: str, key: str, business_timestamp: str) -> DataFrame:
    """Deduplicate immutable events by their business identifier, retaining late events."""
    source = spark.read.table(f"{CATALOG}.bronze.event_{dataset}")
    return _latest_by(
        source,
        [key],
        [business_timestamp, "_kafka_offset", "_ingested_at"],
    )


def latest_file(dataset: str, key: str) -> DataFrame:
    """Resolve initial and incremental file rows to one current record per key."""
    source = spark.read.table(f"{CATALOG}.bronze.file_{dataset}")
    return _latest_by(
        source,
        [key],
        ["_source_file_modified_at", "_ingested_at", "_source_file"],
    )


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
    return (
        dataframe
        .withColumn(
            "pipeline_run_id",
            F.sha2(F.concat_ws("|", F.lit(",".join(sorted(source_tables))), *lineage), 256),
        )
        .withColumn("source_table", F.lit(",".join(sorted(source_tables))))
        .withColumn("processed_at", F.greatest(*processed))
        .withColumn("masking_status", F.lit(masking_status))
    )


def publish_silver_model(name: str, builder, cluster_by: list[str] | None = None) -> None:
    """Register native expectations, curated MV, and typed quarantine MV for one entity."""
    contract = load_data_contract(f"silver/{name}.yml")
    hard_rules = executable_rules(contract, "hard")
    warning_rules = executable_rules(contract, "warn")
    expectations = {**hard_rules, **warning_rules}
    evaluated_name = f"_{name}_quality_evaluated"
    target_name = f"{CATALOG}.silver.{name}"
    quarantine_name = f"{CATALOG}.silver.{name}_quarantine"

    @dp.temporary_view(name=evaluated_name, comment=f"Contract-evaluated rows for {name}")
    @dp.expect_all(expectations)
    def quality_evaluated():
        return with_quality_evidence(builder(), contract)

    @dp.materialized_view(
        name=target_name,
        comment=f"Approved Silver entity: {name}",
        spark_conf=downstream_microbatch_spark_conf(),
        table_properties=SILVER_TABLE_PROPERTIES,
        cluster_by=cluster_by,
    )
    def curated():
        return (
            spark.read.table(evaluated_name)
            .filter(F.size("failed_hard_rules") == 0)
            .withColumn(
                "dq_status",
                F.when(F.size("failed_warning_rules") > 0, F.lit("WARNING")).otherwise(F.lit("PASSED")),
            )
            .drop("failed_hard_rules", "failed_warning_rules", "quality_status")
        )

    @dp.materialized_view(
        name=quarantine_name,
        comment=f"Schema-compatible hard DQ failures for {name}",
        spark_conf=downstream_microbatch_spark_conf(),
        table_properties={**SILVER_TABLE_PROPERTIES, "quality": "quarantine"},
        cluster_by=cluster_by,
    )
    def quarantined():
        return (
            spark.read.table(evaluated_name)
            .filter(F.size("failed_hard_rules") > 0)
            .withColumn("dq_status", F.lit("FAILED"))
            .withColumn("rule_ids", F.col("failed_hard_rules"))
            .withColumn("failure_reason", F.concat_ws(",", F.col("failed_hard_rules")))
            .drop("failed_hard_rules", "failed_warning_rules", "quality_status")
        )
