"""Parse Debezium CDC envelopes from bronze.cdc_changes into per-table Silver tables.

Standard CDC processing flow, one table at a time:
  Step 1 - parse_table  : read bronze, filter to this table, parse payload, clean
  Step 2 - apply_changes: dedup + upsert (latest version wins) + delete handling

Table names and payload schemas are discovered from the data itself (not hard-coded),
so new tables/fields show up without a code change.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from framework.landing_stream_reader import external_bronze_table
from framework.refresh_policy import downstream_microbatch_spark_conf


def _spark() -> SparkSession:
    # Get the active Spark session used by every function below.
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("An active Spark session is required")
    return session


def extract_bronze_table(bronze_table_name: str) -> DataFrame:
    # 1. Read the raw CDC rows out of the Bronze layer.
    bronze_df = _spark().read.table(external_bronze_table(bronze_table_name))

    # 2. Keep operation + the pieces of raw_payload we need.
    #    before/after hold the row data; source.table says which table it came from.
    #    record_key/source_lsn/kafka_offset/captured_at ride along because
    #    apply_changes needs them to dedup and order versions of the same row.
    extracted_df = bronze_df.select(
        "operation",
        "record_key",
        "source_lsn",
        "kafka_offset",
        "captured_at",
        F.get_json_object(F.col("raw_payload"), "$.before").alias("before"),
        F.get_json_object(F.col("raw_payload"), "$.after").alias("after"),
        F.get_json_object(F.col("raw_payload"), "$.source.table").alias("table_name"),
    )

    # 3. operation has 3 values we care about: "c" (insert), "u" (update), "d" (delete).
    #    Insert/update rows carry their data in "after"; delete rows only have "before".
    current_state = (
        F.when(F.col("operation") == "c", F.col("after"))
        .when(F.col("operation") == "u", F.col("after"))
        .when(F.col("operation") == "d", F.col("before"))
        .otherwise(F.lit(None))
    )

    # 4. Output: which table each row belongs to, plus that row's data as JSON text.
    #    It stays text here because every table has a different schema - parse_table
    #    turns it into real typed columns one table at a time.
    return extracted_df.withColumn("current_state", current_state).select(
        "table_name",
        "current_state",
        "operation",
        "record_key",
        "source_lsn",
        "kafka_offset",
        "captured_at",
    )


def get_table_names(extracted_df: DataFrame) -> list[str]:
    # Find every distinct source table name present in the CDC data.
    rows = extracted_df.select("table_name").distinct().collect()
    return [row.table_name for row in rows if row.table_name is not None]


def parse_table(extracted_df: DataFrame, table_name: str) -> DataFrame:
    # STEP 1: filter to this table, parse its current_state JSON into real columns, clean.
    table_rows = extracted_df.filter(F.col("table_name") == table_name)

    # Infer this table's row schema by merging every current_state value together, so a
    # field that only some rows have is still picked up. schema_of_variant_agg spells
    # objects as OBJECT<...>, but from_json wants STRUCT<...>.
    schema_ddl = table_rows.select(
        F.schema_of_variant_agg(F.try_parse_json(F.col("current_state"))).alias("schema")
    ).first()["schema"].replace("OBJECT<", "STRUCT<")

    parsed_df = table_rows.withColumn("parsed", F.from_json(F.col("current_state"), schema_ddl))

    # Clean: drop rows whose payload was empty or malformed, so nothing silently
    # becomes an all-null row downstream.
    clean_df = parsed_df.filter(F.col("parsed").isNotNull())

    return clean_df.select(
        "parsed.*",
        "record_key",
        "operation",
        (F.col("operation") == "d").alias("is_deleted"),
        "source_lsn",
        "kafka_offset",
        "captured_at",
    )


def apply_changes(parsed_df: DataFrame) -> DataFrame:
    # STEP 2: dedup + upsert + delete handling.

    # Dedup: the same change can arrive more than once through Kafka replay/retry.
    # The same row version is always the same (record_key, source_lsn) pair.
    deduped_df = parsed_df.dropDuplicates(["record_key", "source_lsn"])

    # Upsert: keep only the newest version of each row. source_lsn is the source
    # commit order; kafka_offset breaks ties within the same LSN.
    latest_version = Window.partitionBy("record_key").orderBy(
        F.col("source_lsn").desc(), F.col("kafka_offset").desc()
    )
    current_df = (
        deduped_df
        .withColumn("version_rank", F.row_number().over(latest_version))
        .filter(F.col("version_rank") == 1)
        .drop("version_rank")
    )

    # Delete handling: soft delete. Rows whose newest version is a delete stay in
    # Silver flagged is_deleted = true, so downstream can still see the row existed.
    # For a hard delete instead, filter them out here: .filter(~F.col("is_deleted"))
    return current_df


def load_into_silver(bronze_table_name: str, table_name: str) -> DataFrame:
    # Run the whole flow for one table: extract -> step 1 -> step 2.
    extracted_df = extract_bronze_table(bronze_table_name)
    parsed_df = parse_table(extracted_df, table_name)
    return apply_changes(parsed_df)


# Register one Silver table per table found in bronze.cdc_changes.
# _extracted_df here is only used to discover table names up front; load_into_silver
# re-reads and re-extracts bronze.cdc_changes itself when each Silver table is built.
_bronze_table_name = "cdc_changes"
_extracted_df = extract_bronze_table(_bronze_table_name)

for table_name in get_table_names(_extracted_df):
    dp.table(
        lambda table_name=table_name: load_into_silver(_bronze_table_name, table_name),
        name=f"{table_name}_curated",
        spark_conf=downstream_microbatch_spark_conf(),
    )
