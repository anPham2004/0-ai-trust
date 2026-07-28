"""Source-native Auto Loader readers shared by Bronze ingestion definitions."""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


# Shape of the JSON envelope that lands in the volume for every transport
# message (Kafka/Debezium record written out as one JSON line per file). Every
# Bronze source reads this same envelope shape, so the schema lives here once
# instead of being redefined per ingestion file.
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
    """Get the currently running Spark session, or fail clearly if there is none.

    Every function below needs an active session to read config or start a
    stream. Raising a plain, specific error here means a missing session shows
    up as an obvious message instead of a confusing NoneType crash somewhere
    else in the call chain.
    """
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("An active Spark session is required")
    return session


def external_bronze_table(table_name: str) -> str:
    """Build the fully-qualified `catalog.bronze.table_name` name for a Bronze table.

    The catalog name is read from the `zero_ai_trust.catalog` Spark conf (falling
    back to "0-ai-trust") rather than hard-coded, so the same code can run
    unchanged against a different catalog (e.g. a dev/test catalog) just by
    changing that one config value.
    """
    catalog = _spark().conf.get("zero_ai_trust.catalog", "0-ai-trust")
    return f"`{catalog}`.bronze.{table_name}"


def read_transport_stream(source_type: str):
    """Start a streaming read of raw landed files for one source type (e.g. "cdc").

    Steps:
      1. Look up the landing volume path from the `zero_ai_trust.landing_path`
         Spark conf (with a sane default), so this isn't hard-coded either.
      2. Use Databricks Auto Loader ("cloudFiles") to incrementally read new
         files as they arrive under `{landing_path}/{source_type}`, treating
         each file as plain text (one JSON envelope per line):
         - `includeExistingFiles=true` — on first run, pick up files that were
           already sitting in the volume, not just ones that land afterwards.
         - `useManagedFileEvents=true` — use Databricks-managed file
           notifications to detect new files instead of repeatedly listing the
           whole directory, so discovery stays fast and cheap as the volume grows.
      3. Parse each line's `value` column out of TRANSPORT_SCHEMA, and also keep
         two useful pieces of file provenance from Auto Loader's `_metadata`:
         which file a row came from, and when that file was last modified.
      4. Flatten the parsed `transport.*` fields back up to top-level columns
         alongside the provenance columns, so callers get a plain flat
         DataFrame instead of a nested struct.
    """
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
