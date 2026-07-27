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
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("An active Spark session is required")
    return session


def external_bronze_table(table_name: str) -> str:
    catalog = _spark().conf.get("zero_ai_trust.catalog", "0-ai-trust")
    return f"`{catalog}`.bronze.{table_name}"


def read_transport_stream(source_type: str):
    landing = _spark().conf.get(
        "zero_ai_trust.landing_path",
        "/Volumes/0-ai-trust/bronze/landing",
    )
    return (
        _spark().readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.useManagedFileEvents", "true")
        .load(f"{landing}/{source_type}")
        .select(
            F.from_json("value", TRANSPORT_SCHEMA).alias("transport"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        )
        .select("transport.*", "_source_file", "_source_file_modified_at")
    )
