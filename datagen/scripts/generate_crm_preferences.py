"""Generate CRM customer_preferences table (1:1 with customers).

Reads global_id + customerId from {CATALOG}.{SCHEMA}._tmp_customers.
Produces N_PREFERENCES rows — one per customer — written as Parquet.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd


# ---------------------------------------------------------------------------
# Pandas UDFs (Faker imported inside body — never at module level)
# ---------------------------------------------------------------------------

@F.pandas_udf(StringType())
def _udf_satisfaction_score(opt_in: pd.Series) -> pd.Series:
    """Log-normal satisfaction score; marketingOptIn=False shifts mean down."""
    import numpy as np
    scores = []
    for opted_in in opt_in:
        mu = 1.3 if opted_in else 1.0
        val = float(min(5.0, max(1.0, round(np.random.lognormal(mu, 0.2), 1))))
        scores.append(str(val))
    return pd.Series(scores)


@F.pandas_udf(StringType())
def _udf_updated_at(ids: pd.Series) -> pd.Series:
    """Random datetime within config date range."""
    import numpy as np
    from faker import Faker
    fake = Faker()
    Faker.seed(0)
    results = []
    for _ in ids:
        results.append(fake.date_time_this_year().strftime("%Y-%m-%dT%H:%M:%S"))
    return pd.Series(results)


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

def _generate_preferences(spark, cfg):
    partitions = cfg.get_partitions(cfg.N_PREFERENCES)

    # Read the pre-existing CDR customers temp table
    customers = spark.read.parquet(cfg.tmp_path("customers_fk"))

    # Add row_number for 1:1 exact join (no hash-mod needed — same count)
    customers_with_idx = customers.withColumn(
        "_row_idx",
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    )

    # Preference base — one row per customer, preserve order via row_number
    prefs_base = (
        spark.range(1, cfg.N_PREFERENCES + 1, numPartitions=partitions)
        .withColumnRenamed("id", "_row_idx")
        .withColumn("_row_idx", F.col("_row_idx").cast("int"))
    )

    # Join 1:1 by row index so every customer gets exactly one preference row
    prefs = prefs_base.join(customers_with_idx, on="_row_idx", how="left").drop("_row_idx")

    # preferredContactChannel: email 40%, phone 25%, sms 20%, chat 15%
    prefs = prefs.withColumn(
        "preferredContactChannel",
        F.when(F.rand(cfg.SEED) < 0.40, "email")
         .when(F.rand(cfg.SEED) < 0.65, "phone")
         .when(F.rand(cfg.SEED) < 0.85, "sms")
         .otherwise("chat")
    )

    # preferredLanguage: en 70%, es 10%, zh 8%, vi 5%, fr 4%, ko 3%
    prefs = prefs.withColumn(
        "preferredLanguage",
        F.when(F.rand(cfg.SEED) < 0.70, "en")
         .when(F.rand(cfg.SEED) < 0.80, "es")
         .when(F.rand(cfg.SEED) < 0.88, "zh")
         .when(F.rand(cfg.SEED) < 0.93, "vi")
         .when(F.rand(cfg.SEED) < 0.97, "fr")
         .otherwise("ko")
    )

    # marketingOptIn: true 65%, false 35%
    prefs = prefs.withColumn(
        "marketingOptIn",
        F.rand(cfg.SEED) < 0.65
    )

    # satisfactionScore: log-normal, shifted lower when opted out
    prefs = prefs.withColumn(
        "satisfactionScore",
        _udf_satisfaction_score(F.col("marketingOptIn"))
    )

    # updatedAt: random datetime within date range
    prefs = prefs.withColumn(
        "updatedAt",
        _udf_updated_at(F.col("global_id"))
    )

    # Final column selection — explicit ordering
    prefs_final = prefs.select(
        "global_id",
        "customerId",
        "preferredContactChannel",
        "preferredLanguage",
        "marketingOptIn",
        "satisfactionScore",
        "updatedAt",
    )

    out_path = f"{cfg.OUTPUT_PATH}/customer_preferences"
    prefs_final.write.mode("overwrite").parquet(out_path)
    print(f"  Saved customer_preferences ({cfg.N_PREFERENCES:,} rows) → {out_path}")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(spark, cfg):
    _generate_preferences(spark, cfg)


if __name__ == "__main__":
    import config as cfg

    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
