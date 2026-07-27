"""Source-native Auto Loader readers shared by Bronze ingestion definitions."""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


TRANSPORT_SCHEMA = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", LongType()),
        StructField("offset", LongType()),
        StructField("timestamp", LongType()),
        StructField("timestamp_type", LongType()),
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("captured_at", StringType()),
    ]
)


def _spark() -> SparkSession:
    # Fail fast with a clear message rather than a confusing AttributeError on None.
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("An active Spark session is required")
    return session


def external_bronze_table(table_name: str) -> str:
    # Conf-based catalog name lets tests/other environments override the "0-ai-trust" default.
    catalog = _spark().conf.get("zero_ai_trust.catalog", "0-ai-trust")
    return f"`{catalog}`.bronze.{table_name}"


def read_transport_stream(source_type: str):
    # Conf-based landing path mirrors external_bronze_table's override pattern.
    landing = _spark().conf.get(
        "zero_ai_trust.landing_path",
        "/Volumes/0-ai-trust/bronze/landing",
    )
    return (
        _spark().readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.includeExistingFiles", "true")
        # Managed file events avoid a directory-listing scan on every trigger.
        .option("cloudFiles.useManagedFileEvents", "true")
        .load(f"{landing}/{source_type}")
        .select(
            F.from_json("value", TRANSPORT_SCHEMA).alias("transport"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        )
        # Flatten the transport envelope so callers get plain top-level columns.
        .select("transport.*", "_source_file", "_source_file_modified_at")
    )
