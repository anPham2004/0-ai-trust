"""Generate CRM support_interactions and service_cases tables.

Reads global_id + customerId from {CATALOG}.{SCHEMA}._tmp_customers.
FK distribution via hash(id, SEED) % N_CUSTOMERS pattern.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
import pandas as pd


# ---------------------------------------------------------------------------
# Shared FK helper
# ---------------------------------------------------------------------------

def _load_customers_with_idx(spark, cfg):
    """Read CDR customers temp table and add 0-based row_number for FK joins."""
    customers = spark.read.parquet(cfg.tmp_path("customers_fk"))
    return customers.withColumn(
        "_cust_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


# ---------------------------------------------------------------------------
# Pandas UDFs
# ---------------------------------------------------------------------------

@F.pandas_udf(StringType())
def _udf_biased_timestamp(ids: pd.Series) -> pd.Series:
    """Business-hours (9–17, 3x weight) and weekday-biased (78%) timestamps."""
    import numpy as np
    from faker import Faker
    fake = Faker()
    Faker.seed(0)
    results = []
    for _ in ids:
        # Weekday vs weekend: 78% weekday
        if np.random.random() < 0.78:
            dt = fake.date_time_between(start_date="-1y", end_date="now")
            # Nudge toward weekdays by regenerating until we hit Mon-Fri
            for _ in range(5):
                if dt.weekday() < 5:
                    break
                dt = fake.date_time_between(start_date="-1y", end_date="now")
        else:
            dt = fake.date_time_between(start_date="-1y", end_date="now")

        # Business hours bias: 3x weight 9–17
        if np.random.random() < 0.75:
            dt = dt.replace(hour=int(np.random.randint(9, 17)))
        results.append(dt.strftime("%Y-%m-%dT%H:%M:%S"))
    return pd.Series(results)


@F.pandas_udf(IntegerType())
def _udf_duration_by_channel(channels: pd.Series) -> pd.Series:
    """Log-normal duration per channel (in minutes)."""
    import numpy as np
    params = {
        "chat":   (2.0, 0.4),
        "phone":  (2.4, 0.4),
        "email":  (1.5, 0.3),
        "branch": (2.9, 0.4),
    }
    results = []
    for ch in channels:
        mu, sigma = params.get(ch, (2.0, 0.4))
        val = max(1, int(round(np.random.lognormal(mu, sigma))))
        results.append(val)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_fake_subject(ids: pd.Series) -> pd.Series:
    """Faker sentence (6 words) for case subject."""
    from faker import Faker
    fake = Faker()
    Faker.seed(0)
    return pd.Series([fake.sentence(nb_words=6) for _ in ids])


@F.pandas_udf(StringType())
def _udf_business_hours_ts(ids: pd.Series) -> pd.Series:
    """Business-hours datetime for case createdAt."""
    import numpy as np
    from faker import Faker
    fake = Faker()
    Faker.seed(0)
    results = []
    for _ in ids:
        dt = fake.date_time_between(start_date="-1y", end_date="now")
        if np.random.random() < 0.75:
            dt = dt.replace(hour=int(np.random.randint(9, 17)))
        results.append(dt.strftime("%Y-%m-%dT%H:%M:%S"))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_resolved_at(series: pd.Series) -> pd.Series:
    """createdAt + exponential wait (mean=5 days); returns None if open/in_progress."""
    import numpy as np
    from datetime import datetime, timedelta
    results = []
    for val in series:
        # val format: "createdAt|||status"
        parts = val.split("|||")
        created_str, status = parts[0], parts[1]
        if status in ("open", "in_progress"):
            results.append(None)
        else:
            created_dt = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%S")
            wait_seconds = np.random.exponential(scale=5 * 86400)
            resolved_dt = created_dt + timedelta(seconds=float(wait_seconds))
            results.append(resolved_dt.strftime("%Y-%m-%dT%H:%M:%S"))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_resolution_summary(series: pd.Series) -> pd.Series:
    """Faker paragraph (2 sentences); null when open or in_progress."""
    from faker import Faker
    fake = Faker()
    Faker.seed(0)
    results = []
    for status in series:
        if status in ("open", "in_progress"):
            results.append(None)
        else:
            results.append(fake.paragraph(nb_sentences=2))
    return pd.Series(results)


# ---------------------------------------------------------------------------
# support_interactions
# ---------------------------------------------------------------------------

def _generate_support_interactions(spark, cfg):
    n = cfg.N_INTERACTIONS
    partitions = cfg.get_partitions(n)

    customers_idx = _load_customers_with_idx(spark, cfg)

    base = (
        spark.range(0, n, numPartitions=partitions)
        .withColumn(
            "_cust_idx",
            (F.abs(F.hash(F.col("id"), F.lit(cfg.SEED))) % cfg.N_CUSTOMERS).cast("int")
        )
    )

    # FK join
    df = base.join(customers_idx, on="_cust_idx", how="left").drop("_cust_idx")

    # interactionId
    df = df.withColumn("interactionId", F.expr("uuid()"))

    # channel: chat 40%, phone 30%, email 20%, branch 10%
    df = df.withColumn(
        "channel",
        F.when(F.rand(cfg.SEED) < 0.40, "chat")
         .when(F.rand(cfg.SEED) < 0.70, "phone")
         .when(F.rand(cfg.SEED) < 0.90, "email")
         .otherwise("branch")
    )

    # timestamp: business hours + weekday bias
    df = df.withColumn("timestamp", _udf_biased_timestamp(F.col("id").cast("string")))

    # topic: Application_Status 35%, Account_Inquiry 25%, Document_Upload 20%, Complaint 12%, General 8%
    df = df.withColumn(
        "topic",
        F.when(F.rand(cfg.SEED) < 0.35, "Application_Status")
         .when(F.rand(cfg.SEED) < 0.60, "Account_Inquiry")
         .when(F.rand(cfg.SEED) < 0.80, "Document_Upload")
         .when(F.rand(cfg.SEED) < 0.92, "Complaint")
         .otherwise("General")
    )

    # base resolution: resolved 60%, escalated 20%, follow_up_required 15%, unresolved 5%
    df = df.withColumn(
        "_base_resolution",
        F.when(F.rand(cfg.SEED) < 0.60, "resolved")
         .when(F.rand(cfg.SEED) < 0.80, "escalated")
         .when(F.rand(cfg.SEED) < 0.95, "follow_up_required")
         .otherwise("unresolved")
    )

    # Complaint coherence: escalation rate 3x higher
    df = df.withColumn(
        "resolution",
        F.when(
            F.col("topic") == "Complaint",
            F.when(F.rand() < 0.45, "escalated")
             .when(F.rand() < 0.65, "follow_up_required")
             .otherwise("resolved")
        ).otherwise(F.col("_base_resolution"))
    ).drop("_base_resolution")

    # agentId: Pareto — 80% of rows pick from 40 popular agents, 20% from remaining 160
    df = df.withColumn(
        "_pareto_bucket",
        (F.abs(F.hash(F.col("id"), F.lit(42))) % 40).cast("int")
    ).withColumn(
        "agentId",
        F.when(
            F.rand(cfg.SEED) < 0.80,
            # Popular 40 agents (IDs 1–40)
            F.concat(F.lit("AGENT-"), F.lpad((F.col("_pareto_bucket") + 1).cast("string"), 4, "0"))
        ).otherwise(
            # Remaining 160 agents (IDs 41–200)
            F.concat(
                F.lit("AGENT-"),
                F.lpad(
                    ((F.abs(F.hash(F.col("id"), F.lit(99))) % 160) + 41).cast("string"),
                    4, "0"
                )
            )
        )
    ).drop("_pareto_bucket")

    # durationMinutes: log-normal per channel
    df = df.withColumn("durationMinutes", _udf_duration_by_channel(F.col("channel")))

    final = df.select(
        "global_id",
        "interactionId",
        "customerId",
        "channel",
        "timestamp",
        "topic",
        "resolution",
        "agentId",
        "durationMinutes",
    )

    out_path = f"{cfg.OUTPUT_PATH}/support_interactions"
    final.write.mode("overwrite").parquet(out_path)
    print(f"  Saved support_interactions ({n:,} rows) → {out_path}")


# ---------------------------------------------------------------------------
# service_cases
# ---------------------------------------------------------------------------

def _generate_service_cases(spark, cfg):
    n = cfg.N_CASES
    partitions = cfg.get_partitions(n)

    customers_idx = _load_customers_with_idx(spark, cfg)

    base = (
        spark.range(0, n, numPartitions=partitions)
        .withColumn(
            "_cust_idx",
            (F.abs(F.hash(F.col("id"), F.lit(cfg.SEED + 1))) % cfg.N_CUSTOMERS).cast("int")
        )
    )

    df = base.join(customers_idx, on="_cust_idx", how="left").drop("_cust_idx")

    # caseId
    df = df.withColumn("caseId", F.expr("uuid()"))

    # caseType: inquiry 40%, request 30%, complaint 20%, dispute 10%
    df = df.withColumn(
        "caseType",
        F.when(F.rand(cfg.SEED) < 0.40, "inquiry")
         .when(F.rand(cfg.SEED) < 0.70, "request")
         .when(F.rand(cfg.SEED) < 0.90, "complaint")
         .otherwise("dispute")
    )

    # subject: Faker sentence (6 words)
    df = df.withColumn("subject", _udf_fake_subject(F.col("id").cast("string")))

    # status: resolved 50%, closed 20%, open 15%, in_progress 15%
    df = df.withColumn(
        "status",
        F.when(F.rand(cfg.SEED) < 0.50, "resolved")
         .when(F.rand(cfg.SEED) < 0.70, "closed")
         .when(F.rand(cfg.SEED) < 0.85, "open")
         .otherwise("in_progress")
    )

    # priority: correlated with caseType
    df = df.withColumn(
        "priority",
        F.when(
            F.col("caseType").isin("complaint", "dispute"),
            F.when(F.rand() < 0.15, "critical")
             .when(F.rand() < 0.60, "high")
             .when(F.rand() < 0.95, "medium")
             .otherwise("low")
        ).otherwise(
            F.when(F.rand() < 0.02, "critical")
             .when(F.rand() < 0.12, "high")
             .when(F.rand() < 0.52, "medium")
             .otherwise("low")
        )
    )

    # createdAt: business-hours datetime
    df = df.withColumn("createdAt", _udf_business_hours_ts(F.col("id").cast("string")))

    # resolvedAt: createdAt + exponential wait; null if open/in_progress
    df = df.withColumn(
        "_resolve_input",
        F.concat(F.col("createdAt"), F.lit("|||"), F.col("status"))
    ).withColumn(
        "resolvedAt",
        _udf_resolved_at(F.col("_resolve_input"))
    ).drop("_resolve_input")

    # resolutionSummary: Faker paragraph; null if open/in_progress
    df = df.withColumn("resolutionSummary", _udf_resolution_summary(F.col("status")))

    # assignedTeam: correlated with caseType
    df = df.withColumn(
        "assignedTeam",
        F.when(
            F.col("caseType") == "dispute",
            F.when(F.rand() < 0.50, "Escalations")
             .when(F.rand() < 0.80, "Complaints")
             .when(F.rand() < 0.95, "Fraud")
             .otherwise("Support")
        ).when(
            F.col("caseType") == "complaint",
            F.when(F.rand() < 0.55, "Complaints")
             .when(F.rand() < 0.80, "Escalations")
             .when(F.rand() < 0.95, "Support")
             .otherwise("Fraud")
        ).otherwise(
            # inquiry / request
            F.when(F.rand() < 0.70, "Support")
             .when(F.rand() < 0.85, "Complaints")
             .when(F.rand() < 0.95, "Escalations")
             .otherwise("Fraud")
        )
    )

    # organisationId: ~35% of cases are for business customers.
    df = df.withColumn(
        "organisationId",
        F.when(F.rand(cfg.SEED + 502) < 0.35,
               F.concat(F.lit("ORG-"),
                        F.lpad((F.abs(F.hash(F.col("id"), F.lit(cfg.SEED + 503)))
                                % cfg.N_ORGANISATIONS).cast("string"), 5, "0")))
         .otherwise(F.lit(None).cast("string"))
    )

    # applicationId: ~40% of cases relate to a specific application.
    df = df.withColumn(
        "applicationId",
        F.when(F.rand(cfg.SEED + 504) < 0.40,
               F.concat(F.lit("Application_"),
                        F.lpad((F.abs(F.hash(F.col("id"), F.lit(cfg.SEED + 505)))
                                % cfg.N_APPLICATIONS).cast("string"), 9, "0")))
         .otherwise(F.lit(None).cast("string"))
    )

    # slaDeadline: derived from priority — critical=1d, high=3d, medium=5d, low=10d.
    df = df.withColumn(
        "slaDeadline",
        F.date_format(
            F.expr(f"""
                CASE priority
                    WHEN 'critical' THEN date_add(cast(createdAt as date), 1)
                    WHEN 'high'     THEN date_add(cast(createdAt as date), 3)
                    WHEN 'medium'   THEN date_add(cast(createdAt as date), 5)
                    ELSE                 date_add(cast(createdAt as date), 10)
                END
            """),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    final = df.select(
        "global_id",
        "caseId",
        "customerId",
        "caseType",
        "subject",
        "status",
        "priority",
        "createdAt",
        "resolvedAt",
        "resolutionSummary",
        "assignedTeam",
        "organisationId",
        "applicationId",
        "slaDeadline",
    )

    out_path = f"{cfg.OUTPUT_PATH}/service_cases"
    final.write.mode("overwrite").parquet(out_path)
    print(f"  Saved service_cases ({n:,} rows) → {out_path}")

    # Write cases_fk tmp for downstream (service_case_events).
    final.select("global_id", "caseId").write.mode("overwrite").parquet(
        cfg.tmp_path("cases_fk")
    )


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(spark, cfg):
    _generate_support_interactions(spark, cfg)
    _generate_service_cases(spark, cfg)


if __name__ == "__main__":
    import config as cfg

    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
