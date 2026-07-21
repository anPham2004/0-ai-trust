"""BPI synthetic data: loan_applications + loan_application_events.

Reads global_id pool from customers_fk.
Writes Parquet to OUTPUT_PATH; persists _tmp/applications_fk for CRM FK reuse.
SEED offsets: +1 to +19.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd


def _load_customers_idx(spark, cfg):
    return spark.read.parquet(cfg.tmp_path("customers_fk")).withColumn(
        "cust_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


def _generate_applications(spark, cfg):
    N, SEED = cfg.N_APPLICATIONS, cfg.SEED

    @F.pandas_udf(StringType())
    def requested_amount_udf(ids: pd.Series) -> pd.Series:
        import numpy as np
        np.random.seed(SEED)
        v = np.random.lognormal(mean=10.4, sigma=0.55, size=len(ids))
        return ids.map(lambda i: f"{v[int(i) % len(v)]:.2f}")

    @F.pandas_udf(StringType())
    def accepted_offer_amount_udf(ids: pd.Series) -> pd.Series:
        """Accepted offer amount — slightly less than requested, nullable for denied/cancelled."""
        import numpy as np
        rng = np.random.default_rng(SEED + 7)
        results = []
        for _ in ids:
            if rng.random() < 0.58:  # ~58% get accepted offer
                results.append(f"{rng.lognormal(10.3, 0.5):.2f}")
            else:
                results.append(None)
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def monthly_cost_udf(ids: pd.Series) -> pd.Series:
        """Monthly repayment for selected offers."""
        import numpy as np
        rng = np.random.default_rng(SEED + 8)
        results = []
        for _ in ids:
            if rng.random() < 0.55:
                results.append(f"{rng.lognormal(6.0, 0.4):.2f}")
            else:
                results.append(None)
        return pd.Series(results)

    cust = _load_customers_idx(spark, cfg)
    secs = int((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_epoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("app_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED))) % cfg.N_CUSTOMERS).cast("int"))
        .withColumn("applicationId",
                    F.concat(F.lit("Application_"), F.lpad(F.col("id").cast("string"), 9, "0")))
    )
    base = (
        base.join(cust.select("cust_idx", "global_id", "customerId"), base.app_idx == cust.cust_idx, "left")
        .drop("app_idx", "cust_idx")
    )

    r1 = F.rand(SEED + 1)
    # BPI LoanGoal enum values
    loan_goal = (
        F.when(r1 < .18, "Home improvement")
         .when(r1 < .32, "Existing loan takeover")
         .when(r1 < .44, "Car")
         .when(r1 < .54, "Business goal")
         .when(r1 < .61, "Extra spending limit")
         .when(r1 < .67, "Remaining debt home")
         .when(r1 < .73, "Tax payments")
         .when(r1 < .78, "Boat")
         .when(r1 < .82, "Motorcycle")
         .when(r1 < .86, "Caravan / Camper")
         .when(r1 < .90, "Not speficied")
         .when(r1 < .95, "Other see explanation")
         .otherwise("Unknown")
    )
    # BPI ApplicationType enum values
    app_type = F.when(F.rand(SEED + 2) < .80, "New credit").otherwise("Limit raise")

    r3 = F.rand(SEED + 3)
    outcome = (
        F.when(r3 < .58, "Accepted").when(r3 < .80, "Denied")
         .when(r3 < .90, "Cancelled").when(r3 < .95, "Incomplete").otherwise("Pending")
    )
    credit = F.greatest(
        F.lit(300), F.least(F.lit(1000), (F.abs(F.randn(SEED + 4)) * 80 + 750).cast("int"))
    )
    submitted = F.date_format(
        (start_epoch + (F.rand(SEED + 5) * secs).cast("long")).cast("timestamp"),
        "yyyy-MM-dd HH:mm:ss"
    )
    updated = F.date_format(
        (F.unix_timestamp(submitted) + (F.rand(SEED + 6) * 180 * 86400).cast("long")).cast("timestamp"),
        "yyyy-MM-dd HH:mm:ss"
    )

    # numOffers: 0-5, higher weight on 1-3
    r_no = F.rand(SEED + 9)
    num_offers = (
        F.when(r_no < .10, F.lit(0))
         .when(r_no < .35, F.lit(1))
         .when(r_no < .65, F.lit(2))
         .when(r_no < .85, F.lit(3))
         .when(r_no < .95, F.lit(4))
         .otherwise(F.lit(5))
    ).cast("string")

    # numberOfTerms: 12/24/36/48/60 months, null when denied/cancelled
    r_nt = F.rand(SEED + 10)
    number_of_terms = (
        F.when(F.col("finalOutcome").isin("Denied", "Cancelled"),
               F.lit(None).cast("string"))
         .otherwise(
             F.when(r_nt < .10, "12").when(r_nt < .25, "24")
              .when(r_nt < .50, "36").when(r_nt < .70, "48")
              .when(r_nt < .88, "60").otherwise("72")
         )
    )

    # totalEvents: per application — approximate 3-15 events
    total_events = (F.rand(SEED + 11) * 12 + 3).cast("int").cast("string")

    # ~35% of applications are for business customers (linked to an organisation).
    org_id = (
        F.when(F.rand(SEED + 500) < 0.35,
               F.concat(F.lit("ORG-"),
                        F.lpad((F.abs(F.hash(F.col("applicationId"), F.lit(SEED + 501)))
                                % cfg.N_ORGANISATIONS).cast("string"), 5, "0")))
         .otherwise(F.lit(None).cast("string"))
    )

    df = (
        base
        .withColumn("loanGoal", loan_goal)
        .withColumn("applicationType", app_type)
        .withColumn("requestedAmount", requested_amount_udf(F.col("id")))
        .withColumn("creditScore", credit)
        .withColumn("finalOutcome", outcome)
        .withColumn("submittedAt", submitted)
        .withColumn("lastUpdatedAt", updated)
        .withColumn("numOffers", num_offers)
        .withColumn("acceptedOfferAmount", accepted_offer_amount_udf(F.col("id")))
        .withColumn("monthlyCost", monthly_cost_udf(F.col("id")))
        .withColumn("numberOfTerms", number_of_terms)
        .withColumn("totalEvents", total_events)
        .withColumn("organisationId", org_id)
        .select("global_id", "applicationId", "customerId",
                "loanGoal", "applicationType", "requestedAmount",
                "creditScore", "finalOutcome", "submittedAt", "lastUpdatedAt",
                "numOffers", "acceptedOfferAmount", "monthlyCost",
                "numberOfTerms", "totalEvents", "organisationId")
    )
    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/loan_applications")
    df.write.mode("overwrite").parquet(cfg.tmp_path("applications_fk"))
    print(f"  loan_applications: {N:,} rows")


def _generate_events(spark, cfg):
    N, SEED = cfg.N_EVENTS, cfg.SEED
    N_APPS = cfg.N_APPLICATIONS

    apps = spark.read.parquet(cfg.tmp_path("applications_fk")).withColumn(
        "app_pool_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )
    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("fk_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 10))) % N_APPS).cast("int"))
    )
    base = (
        base.join(apps.select("app_pool_idx", "applicationId", "global_id"), base.fk_idx == apps.app_pool_idx, "left")
        .drop("fk_idx", "app_pool_idx")
    )

    # BPI concept:name enum (24 distinct values)
    rc = F.rand(SEED + 11)
    concept = (
        F.when(rc < .12, "A_Create Application")
         .when(rc < .22, "A_Submitted")
         .when(rc < .30, "A_Accepted")
         .when(rc < .37, "A_Denied")
         .when(rc < .43, "A_Cancelled")
         .when(rc < .49, "A_Complete")
         .when(rc < .54, "A_Concept")
         .when(rc < .58, "A_Incomplete")
         .when(rc < .62, "A_Pending")
         .when(rc < .66, "A_Validating")
         .when(rc < .70, "O_Accepted")
         .when(rc < .73, "O_Cancelled")
         .when(rc < .76, "O_Create Offer")
         .when(rc < .79, "O_Created")
         .when(rc < .82, "O_Refused")
         .when(rc < .84, "O_Returned")
         .when(rc < .87, "O_Sent (mail and online)")
         .when(rc < .89, "O_Sent (online only)")
         .when(rc < .91, "W_Assess potential fraud")
         .when(rc < .93, "W_Call after offers")
         .when(rc < .95, "W_Call incomplete files")
         .when(rc < .97, "W_Complete application")
         .when(rc < .99, "W_Handle leads")
         .otherwise("W_Validate application")
    )

    # BPI lifecycle:transition enum (7 distinct values)
    rl = F.rand(SEED + 12)
    lifecycle = (
        F.when(rl < .35, "complete").when(rl < .65, "start")
         .when(rl < .73, "schedule").when(rl < .80, "suspend")
         .when(rl < .87, "resume").when(rl < .94, "withdraw")
         .otherwise("ate_abort")
    )

    # BPI Action enum (5 distinct values)
    ra = F.rand(SEED + 13)
    action = (
        F.when(ra < .40, "Created").when(ra < .65, "statechange")
         .when(ra < .82, "Obtained").when(ra < .93, "Released")
         .otherwise("Deleted")
    )

    # BPI EventOrigin enum (3 values; derived from concept prefix)
    event_origin = (
        F.when(F.col("conceptName").startswith("A_"), "Application")
         .when(F.col("conceptName").startswith("O_"), "Offer")
         .otherwise("Workflow")
    )

    # business-hours bias: 75% within 09-17
    days = (cfg.END_DATE - cfg.START_DATE).days
    bepoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))
    day_off = (F.rand(SEED + 16) * days).cast("long") * 86400
    hr_off = F.when(
        F.rand(SEED + 14) < .75,
        (9 * 3600 + (F.rand(SEED + 15) * 8 * 3600).cast("long"))
    ).otherwise((F.rand(SEED + 17) * 86400).cast("long"))
    ts = F.date_format((bepoch + day_off + hr_off).cast("timestamp"), "yyyy-MM-dd HH:mm:ss")

    # Pareto staff: 80% events → User_1-10
    staff_idx = F.when(
        F.rand(SEED + 18) < .80,
        (F.abs(F.hash(F.col("id"), F.lit(99))) % 10 + 1).cast("int")
    ).otherwise(
        (F.abs(F.hash(F.col("id"), F.lit(42))) % 100 + 1).cast("int")
    )
    org = F.concat(F.lit("User_"), staff_idx.cast("string"))

    # Offer-specific fields — only for O_* concept events
    is_offer_event = F.col("conceptName").startswith("O_")

    @F.pandas_udf(StringType())
    def _udf_offered_amount(ids: pd.Series) -> pd.Series:
        import numpy as np
        rng = np.random.default_rng(SEED + 19)
        return pd.Series([f"{rng.lognormal(10.3, 0.5):.2f}" for _ in ids])

    @F.pandas_udf(StringType())
    def _udf_first_withdrawal_amount(ids: pd.Series) -> pd.Series:
        import numpy as np
        rng = np.random.default_rng(SEED + 19 + 1)
        results = []
        for _ in ids:
            if rng.random() < 0.70:
                results.append(f"{rng.lognormal(10.2, 0.5):.2f}")
            else:
                results.append(None)
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def _udf_monthly_cost(ids: pd.Series) -> pd.Series:
        import numpy as np
        rng = np.random.default_rng(SEED + 19 + 2)
        return pd.Series([f"{rng.lognormal(6.0, 0.4):.2f}" for _ in ids])

    @F.pandas_udf(StringType())
    def _udf_credit_score(ids: pd.Series) -> pd.Series:
        import numpy as np
        rng = np.random.default_rng(SEED + 19 + 3)
        return pd.Series([f"{min(1000, max(0, int(rng.normal(750, 120)))):.1f}" for _ in ids])

    r_nt = F.rand(SEED + 19 + 4)
    number_of_terms_event = (
        F.when(r_nt < .10, "12.0").when(r_nt < .25, "24.0")
         .when(r_nt < .50, "36.0").when(r_nt < .70, "48.0")
         .when(r_nt < .88, "60.0").otherwise("72.0")
    )

    r_acc = F.rand(SEED + 19 + 5)
    accepted_col = (
        F.when(r_acc < .40, "True").when(r_acc < .80, "False").otherwise(F.lit(None).cast("string"))
    )
    selected_col = (
        F.when(F.col("accepted") == "True",
               F.when(F.rand(SEED + 19 + 6) < 0.70, "True").otherwise("False"))
         .when(F.col("accepted") == "False", F.lit("False"))
         .otherwise(F.lit(None).cast("string"))
    )

    df = (
        base
        .withColumn("eventId", F.expr("uuid()"))
        .withColumn("conceptName", concept)
        .withColumn("lifecycleTransition", lifecycle)
        .withColumn("timestamp", ts)
        .withColumn("orgResource", org)
        .withColumn("action", action)
        .withColumn("eventOrigin", event_origin)
        # Offer-scoped fields (null for non-offer events)
        .withColumn("offerId",
                    F.when(is_offer_event,
                           F.concat(F.lit("Offer_"), F.lpad(F.col("id").cast("string"), 9, "0")))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("offeredAmount",
                    F.when(is_offer_event, _udf_offered_amount(F.col("id")))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("firstWithdrawalAmount",
                    F.when(is_offer_event, _udf_first_withdrawal_amount(F.col("id")))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("numberOfTerms",
                    F.when(is_offer_event, number_of_terms_event)
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("monthlyCost",
                    F.when(is_offer_event, _udf_monthly_cost(F.col("id")))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("creditScore",
                    F.when(is_offer_event, _udf_credit_score(F.col("id")))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("accepted",
                    F.when(is_offer_event, accepted_col)
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("selected",
                    F.when(is_offer_event, selected_col)
                     .otherwise(F.lit(None).cast("string")))
        .select("global_id", "eventId", "applicationId",
                "conceptName", "lifecycleTransition", "timestamp",
                "orgResource", "action", "eventOrigin",
                "offerId", "offeredAmount", "firstWithdrawalAmount",
                "numberOfTerms", "monthlyCost", "creditScore",
                "accepted", "selected")
    )
    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/loan_application_events")
    print(f"  loan_application_events: {N:,} rows")


def run(spark, cfg):
    print("=== BPI generation ===")
    _generate_applications(spark, cfg)
    _generate_events(spark, cfg)
    print("=== BPI done ===")


if __name__ == "__main__":
    import config as cfg  # noqa: F811
    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
