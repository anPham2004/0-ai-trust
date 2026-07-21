"""Generate service_case_events table — activity log for service cases.

Each event records an action taken on a service case (note added, status changed,
escalated, etc.). FK to service_cases via cases_fk tmp.

Depends on: cases_fk (step 8).
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import config as cfg


def _generate_case_events(spark, cfg):
    N = cfg.N_CASE_EVENTS
    SEED = cfg.SEED

    # Load cases pool for caseId + global_id.
    cases = spark.read.parquet(cfg.tmp_path("cases_fk")).withColumn(
        "_case_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int"),
    )

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn(
            "_case_idx",
            (F.abs(F.hash(F.col("id"), F.lit(SEED + 540))) % cfg.N_CASES).cast("int"),
        )
    )
    base = base.join(
        cases.select("_case_idx", "global_id", "caseId"),
        on="_case_idx", how="left",
    ).drop("_case_idx")

    # eventType distribution.
    r = F.rand(SEED + 541)
    event_type = (
        F.when(r < 0.30, "NOTE_ADDED")
         .when(r < 0.55, "STATUS_CHANGED")
         .when(r < 0.70, "ASSIGNED")
         .when(r < 0.80, "ESCALATED")
         .when(r < 0.90, "DOCUMENT_ATTACHED")
         .otherwise("CUSTOMER_CONTACTED")
    )

    # eventTimestamp within config date range.
    start_epoch = F.lit(cfg.START_DATE.timestamp()).cast("long")
    secs = F.lit((cfg.END_DATE - cfg.START_DATE).total_seconds())
    event_ts = F.date_format(
        (start_epoch + (F.rand(SEED + 542) * secs).cast("long")).cast("timestamp"),
        "yyyy-MM-dd HH:mm:ss",
    )

    # performedBy: AGENT-XXXX (Pareto — top 20% agents handle 80% of events).
    agent_id = F.concat(
        F.lit("AGENT-"),
        F.lpad(
            F.floor(F.pow(F.rand(SEED + 543), 2.0) * 50).cast("string"),
            4, "0",
        ),
    )

    # description: short template per event type.
    desc = (
        F.when(F.col("eventType") == "NOTE_ADDED", "Internal note recorded by agent")
         .when(F.col("eventType") == "STATUS_CHANGED", "Case status updated")
         .when(F.col("eventType") == "ASSIGNED", "Case reassigned to new team member")
         .when(F.col("eventType") == "ESCALATED", "Case escalated to senior team")
         .when(F.col("eventType") == "DOCUMENT_ATTACHED", "Supporting document uploaded")
         .otherwise("Customer contacted via preferred channel")
    )

    df = (
        base
        .withColumn("eventId", F.expr("uuid()"))
        .withColumn("eventType", event_type)
        .withColumn("eventTimestamp", event_ts)
        .withColumn("performedBy", agent_id)
        .withColumn("description", desc)
        .select(
            "global_id", "eventId", "caseId",
            "eventType", "eventTimestamp", "performedBy", "description",
        )
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/service_case_events")
    print(f"  service_case_events: {N:,} rows")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(spark, cfg):
    print("=== CRM Case Events generation ===")
    _generate_case_events(spark, cfg)
    print("=== CRM Case Events done ===")


if __name__ == "__main__":
    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
