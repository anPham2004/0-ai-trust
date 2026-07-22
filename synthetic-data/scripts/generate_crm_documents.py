"""Generate CRM missing_documents table.

Reads global_id + applicationId + customerId + submittedAt from
{CATALOG}.{SCHEMA}._tmp_applications (written by BPI script).
Produces N_DOCUMENTS rows written as Parquet.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd


# ---------------------------------------------------------------------------
# Pandas UDFs (Faker / numpy imported inside body only)
# ---------------------------------------------------------------------------

@F.pandas_udf(StringType())
def _udf_requested_at(submitted_series: pd.Series) -> pd.Series:
    """requestedAt = submittedAt + exponential offset (mean=2 days)."""
    import numpy as np
    results = []
    for ts_str in submitted_series:
        try:
            from datetime import datetime, timedelta
            base = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            offset_days = np.random.exponential(scale=2.0)
            results.append((base + timedelta(days=offset_days)).strftime("%Y-%m-%dT%H:%M:%S"))
        except Exception:
            results.append(ts_str)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_received_at(requested_series: pd.Series, status_series: pd.Series) -> pd.Series:
    """receivedAt = requestedAt + exponential wait (mean=8 days); null if status=required."""
    import numpy as np
    results = []
    for req_str, status in zip(requested_series, status_series):
        if status == "required":
            results.append(None)
            continue
        try:
            from datetime import datetime, timedelta
            base = datetime.strptime(req_str, "%Y-%m-%dT%H:%M:%S")
            offset_days = np.random.exponential(scale=8.0)
            results.append((base + timedelta(days=offset_days)).strftime("%Y-%m-%dT%H:%M:%S"))
        except Exception:
            results.append(None)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_expiry_date(received_series: pd.Series, doc_type_series: pd.Series, status_series: pd.Series) -> pd.Series:
    """expiryDate = receivedAt + days by docType; null if status=required or receivedAt null."""
    from datetime import datetime, timedelta
    EXPIRY_DAYS = {
        "payslip": 90, "ID_proof": 365, "address_proof": 90,
        "bank_statement": 90, "tax_return": 365, "other": 180,
    }
    results = []
    for recv_str, doc_type, status in zip(received_series, doc_type_series, status_series):
        if status == "required" or recv_str is None:
            results.append(None)
            continue
        try:
            base = datetime.strptime(recv_str, "%Y-%m-%dT%H:%M:%S")
            days = EXPIRY_DAYS.get(doc_type, 180)
            results.append((base + timedelta(days=days)).strftime("%Y-%m-%d"))
        except Exception:
            results.append(None)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_last_reminder_at(requested_series: pd.Series, reminders_series: pd.Series) -> pd.Series:
    """lastReminderAt = requestedAt + (reminders * ~7 days); null if reminders=0."""
    import numpy as np
    from datetime import datetime, timedelta
    results = []
    for req_str, reminders in zip(requested_series, reminders_series):
        if reminders == 0:
            results.append(None)
            continue
        try:
            base = datetime.strptime(req_str, "%Y-%m-%dT%H:%M:%S")
            jitter = np.random.normal(loc=0, scale=0.5)
            offset_days = reminders * (7.0 + jitter)
            results.append((base + timedelta(days=max(0.0, offset_days))).strftime("%Y-%m-%dT%H:%M:%S"))
        except Exception:
            results.append(None)
    return pd.Series(results)


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

def _generate_missing_documents(spark, cfg):
    N = cfg.N_DOCUMENTS
    SEED = cfg.SEED
    N_APPS = cfg.N_APPLICATIONS
    PARTITIONS = cfg.get_partitions(N)

    apps = spark.read.parquet(cfg.tmp_path("applications_fk"))
    apps_idx = apps.withColumn(
        "_app_pool_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )

    base = (
        spark.range(0, N, numPartitions=PARTITIONS)
        .withColumn("_fk_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 20))) % N_APPS).cast("int"))
    )

    docs_fk = (
        base
        .join(
            apps_idx.select("_app_pool_idx", "global_id", "applicationId", "customerId", "submittedAt"),
            base._fk_idx == apps_idx._app_pool_idx,
            "left"
        )
        .drop("_fk_idx", "_app_pool_idx")
    )

    # documentId
    docs_fk = docs_fk.withColumn("documentId", F.expr("uuid()"))

    # documentType: payslip 25%, ID_proof 20%, address_proof 20%, bank_statement 15%, tax_return 10%, other 10%
    r_dt = F.rand(SEED + 21)
    docs_fk = docs_fk.withColumn(
        "documentType",
        F.when(r_dt < 0.25, "payslip")
         .when(r_dt < 0.45, "ID_proof")
         .when(r_dt < 0.65, "address_proof")
         .when(r_dt < 0.80, "bank_statement")
         .when(r_dt < 0.90, "tax_return")
         .otherwise("other")
    )

    # status: verified 40%, received 25%, required 15%, invalid 10%, expired 10%
    r_st = F.rand(SEED + 22)
    docs_fk = docs_fk.withColumn(
        "status",
        F.when(r_st < 0.40, "verified")
         .when(r_st < 0.65, "received")
         .when(r_st < 0.80, "required")
         .when(r_st < 0.90, "invalid")
         .otherwise("expired")
    )

    # requestedAt
    docs_fk = docs_fk.withColumn("requestedAt", _udf_requested_at(F.col("submittedAt")))

    # receivedAt (null if status=required)
    docs_fk = docs_fk.withColumn("receivedAt", _udf_received_at(F.col("requestedAt"), F.col("status")))

    # expiryDate
    docs_fk = docs_fk.withColumn(
        "expiryDate",
        _udf_expiry_date(F.col("receivedAt"), F.col("documentType"), F.col("status"))
    )

    # remindersSent: exponential(mean=1.2) capped at 10; 0 if verified
    reminders_raw = F.least(
        F.lit(10),
        F.greatest(F.lit(0), (F.abs(F.randn(SEED + 23)) * 1.2).cast("int"))
    )
    docs_fk = docs_fk.withColumn(
        "remindersSent",
        F.when(F.col("status") == "verified", F.lit(0)).otherwise(reminders_raw)
    )

    # lastReminderAt
    docs_fk = docs_fk.withColumn(
        "lastReminderAt",
        _udf_last_reminder_at(F.col("requestedAt"), F.col("remindersSent"))
    )

    # rejectionReason: only for status=invalid
    r_rr = F.rand(SEED + 24)
    rejection = (
        F.when(r_rr < 0.25, "blurry_scan")
         .when(r_rr < 0.50, "document_expired")
         .when(r_rr < 0.70, "name_mismatch")
         .when(r_rr < 0.85, "wrong_document_type")
         .otherwise("poor_quality")
    )
    docs_fk = docs_fk.withColumn(
        "rejectionReason",
        F.when(F.col("status") == "invalid", rejection).otherwise(F.lit(None).cast("string"))
    )

    docs_final = docs_fk.select(
        "global_id", "documentId", "applicationId", "customerId",
        "documentType", "status", "requestedAt", "receivedAt",
        "expiryDate", "remindersSent", "lastReminderAt", "rejectionReason",
    )

    out_path = f"{cfg.OUTPUT_PATH}/missing_documents"
    docs_final.write.mode("overwrite").parquet(out_path)
    print(f"  missing_documents ({N:,} rows) → {out_path}")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(spark, cfg):
    print("\n=== CRM: missing_documents ===")
    _generate_missing_documents(spark, cfg)
    print("\n=== CRM documents generation complete ===")


if __name__ == "__main__":
    import config as cfg

    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
