"""Generate Customer Profile Extended tables: insurance_policies, kyc_records.

insurance_policies: many-to-1 with customers via hash-mod FK pattern.
kyc_records: 1:1 with customers via row_number match pattern.

SEED offsets: +400 to +499 (reserved for this script).
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd


# ---------------------------------------------------------------------------
# Shared FK helper
# ---------------------------------------------------------------------------

def _load_customers_idx(spark, cfg):
    """Read CDR customers temp table and add 0-based index for FK joins."""
    return spark.read.parquet(cfg.tmp_path("customers_fk")).withColumn(
        "cust_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


# ---------------------------------------------------------------------------
# Pandas UDFs (Faker / numpy imported inside body only)
# ---------------------------------------------------------------------------

@F.pandas_udf(StringType())
def _udf_premium_amount(policy_types: pd.Series) -> pd.Series:
    """Log-normal premium amount varying by policyType."""
    import numpy as np
    params = {
        "HOME_BUILDING":     (7.5, 0.3),
        "HOME_CONTENTS":     (6.2, 0.3),
        "CAR":               (7.0, 0.3),
        "LIFE":              (7.8, 0.4),
        "INCOME_PROTECTION": (7.3, 0.4),
        "LANDLORD":          (7.2, 0.3),
    }
    results = []
    for pt in policy_types:
        mu, sigma = params.get(pt, (7.0, 0.3))
        val = np.random.lognormal(mu, sigma)
        results.append(f"{val:.2f}")
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_cover_amount(policy_types: pd.Series) -> pd.Series:
    """Log-normal cover amount varying by policyType."""
    import numpy as np
    params = {
        "HOME_BUILDING":     (13.1, 0.3),
        "HOME_CONTENTS":     (10.8, 0.3),
        "CAR":               (10.3, 0.3),
        "LIFE":              (13.5, 0.4),
        "INCOME_PROTECTION": (11.5, 0.3),
        "LANDLORD":          (13.0, 0.3),
    }
    results = []
    for pt in policy_types:
        mu, sigma = params.get(pt, (11.0, 0.3))
        val = np.random.lognormal(mu, sigma)
        results.append(f"{val:.2f}")
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_end_date(start_dates: pd.Series) -> pd.Series:
    """endDate = startDate + 365 days."""
    from datetime import datetime, timedelta
    results = []
    for sd in start_dates:
        try:
            dt = datetime.strptime(str(sd), "%Y-%m-%d")
            results.append((dt + timedelta(days=365)).strftime("%Y-%m-%d"))
        except Exception:
            results.append(None)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_last_review_date(verification_dates: pd.Series) -> pd.Series:
    """lastReviewDate = verificationDate + random(0, 365) days."""
    import numpy as np
    from datetime import datetime, timedelta
    results = []
    for vd in verification_dates:
        try:
            dt = datetime.strptime(str(vd), "%Y-%m-%d")
            offset = int(np.random.randint(0, 366))
            results.append((dt + timedelta(days=offset)).strftime("%Y-%m-%d"))
        except Exception:
            results.append(None)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_next_review_date(last_review_dates: pd.Series) -> pd.Series:
    """nextReviewDate = lastReviewDate + 365 days."""
    from datetime import datetime, timedelta
    results = []
    for lrd in last_review_dates:
        try:
            dt = datetime.strptime(str(lrd), "%Y-%m-%d")
            results.append((dt + timedelta(days=365)).strftime("%Y-%m-%d"))
        except Exception:
            results.append(None)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_document_types(ids: pd.Series) -> pd.Series:
    """Random comma-separated subset (2-4 items) of KYC document types."""
    import numpy as np
    doc_pool = [
        "passport", "drivers_licence", "medicare_card",
        "utility_bill", "bank_statement", "birth_certificate",
    ]
    results = []
    for _ in ids:
        count = np.random.randint(2, 5)  # 2 to 4 inclusive
        chosen = np.random.choice(doc_pool, size=count, replace=False)
        results.append(",".join(chosen))
    return pd.Series(results)


# ---------------------------------------------------------------------------
# insurance_policies (many-to-1 with customers)
# ---------------------------------------------------------------------------

def _generate_insurance_policies(spark, cfg):
    N = cfg.N_INSURANCE_POLICIES
    SEED = cfg.SEED
    partitions = cfg.get_partitions(N)
    total_days = (cfg.END_DATE - cfg.START_DATE).days

    customers_idx = _load_customers_idx(spark, cfg)

    base = (
        spark.range(0, N, numPartitions=partitions)
        .withColumn(
            "cust_idx",
            (F.abs(F.hash(F.col("id"), F.lit(SEED + 400))) % cfg.N_CUSTOMERS).cast("int")
        )
    )

    # FK join — many-to-1
    df = base.join(customers_idx, on="cust_idx", how="left").drop("cust_idx")

    # policyId
    df = df.withColumn("policyId", F.expr("uuid()"))

    # policyType: HOME_BUILDING 25%, HOME_CONTENTS 20%, LANDLORD 10%, CAR 25%, LIFE 10%, INCOME_PROTECTION 10%
    r_pt = F.rand(SEED + 401)
    df = df.withColumn(
        "policyType",
        F.when(r_pt < 0.25, "HOME_BUILDING")
         .when(r_pt < 0.45, "HOME_CONTENTS")
         .when(r_pt < 0.55, "LANDLORD")
         .when(r_pt < 0.80, "CAR")
         .when(r_pt < 0.90, "LIFE")
         .otherwise("INCOME_PROTECTION")
    )

    # provider: Suncorp 20%, IAG/NRMA 18%, Allianz 15%, QBE 12%, AMP 10%, TAL 8%, MLC 7%, Zurich 5%, BankCorp Insurance 5%
    r_pv = F.rand(SEED + 402)
    df = df.withColumn(
        "provider",
        F.when(r_pv < 0.20, "Suncorp")
         .when(r_pv < 0.38, "IAG/NRMA")
         .when(r_pv < 0.53, "Allianz")
         .when(r_pv < 0.65, "QBE")
         .when(r_pv < 0.75, "AMP")
         .when(r_pv < 0.83, "TAL")
         .when(r_pv < 0.90, "MLC")
         .when(r_pv < 0.95, "Zurich")
         .otherwise("BankCorp Insurance")
    )

    # premiumAmount: log-normal by policyType
    df = df.withColumn("premiumAmount", _udf_premium_amount(F.col("policyType")))

    # premiumFrequency: MONTHLY 50%, ANNUAL 40%, QUARTERLY 10%
    r_pf = F.rand(SEED + 403)
    df = df.withColumn(
        "premiumFrequency",
        F.when(r_pf < 0.50, "MONTHLY")
         .when(r_pf < 0.90, "ANNUAL")
         .otherwise("QUARTERLY")
    )

    # coverAmount: log-normal by policyType
    df = df.withColumn("coverAmount", _udf_cover_amount(F.col("policyType")))

    # excessAmount: "250" 20%, "500" 40%, "750" 25%, "1000" 15%
    r_ea = F.rand(SEED + 404)
    df = df.withColumn(
        "excessAmount",
        F.when(r_ea < 0.20, "250")
         .when(r_ea < 0.60, "500")
         .when(r_ea < 0.85, "750")
         .otherwise("1000")
    )

    # startDate: random date within config date range
    df = df.withColumn(
        "startDate",
        F.date_add(
            F.lit(cfg.START_DATE.strftime("%Y-%m-%d")),
            (F.rand(SEED + 405) * total_days).cast("int")
        ).cast("string")
    )

    # endDate: startDate + 365 days
    df = df.withColumn("endDate", _udf_end_date(F.col("startDate")))

    # status: ACTIVE 80%, LAPSED 10%, CANCELLED 5%, CLAIMED 5%
    r_st = F.rand(SEED + 406)
    df = df.withColumn(
        "status",
        F.when(r_st < 0.80, "ACTIVE")
         .when(r_st < 0.90, "LAPSED")
         .when(r_st < 0.95, "CANCELLED")
         .otherwise("CLAIMED")
    )

    # autoRenewal: 75% true
    df = df.withColumn(
        "autoRenewal",
        (F.rand(SEED + 407) < 0.75).cast("boolean")
    )

    final = df.select(
        "global_id",
        "customerId",
        "policyId",
        "policyType",
        "provider",
        "premiumAmount",
        "premiumFrequency",
        "coverAmount",
        "excessAmount",
        "startDate",
        "endDate",
        "status",
        "autoRenewal",
    )

    out_path = f"{cfg.OUTPUT_PATH}/insurance_policies"
    final.write.mode("overwrite").parquet(out_path)
    print(f"  insurance_policies: {N:,} rows")


# ---------------------------------------------------------------------------
# kyc_records (1:1 with customers)
# ---------------------------------------------------------------------------

def _generate_kyc_records(spark, cfg):
    N = cfg.N_KYC_RECORDS
    SEED = cfg.SEED
    partitions = cfg.get_partitions(N)
    total_days = (cfg.END_DATE - cfg.START_DATE).days

    # 1:1 join pattern — row_number match (same as generate_crm_preferences.py)
    customers = spark.read.parquet(cfg.tmp_path("customers_fk"))
    customers_with_idx = customers.withColumn(
        "_row_idx",
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    )

    kyc_base = (
        spark.range(1, N + 1, numPartitions=partitions)
        .withColumnRenamed("id", "_row_idx")
        .withColumn("_row_idx", F.col("_row_idx").cast("int"))
    )

    # Join 1:1 by row index
    df = kyc_base.join(customers_with_idx, on="_row_idx", how="left").drop("_row_idx")

    # kycId
    df = df.withColumn("kycId", F.expr("uuid()"))

    # verificationStatus: VERIFIED 70%, PENDING 15%, FAILED 5%, EXPIRED 10%
    r_vs = F.rand(SEED + 410)
    df = df.withColumn(
        "verificationStatus",
        F.when(r_vs < 0.70, "VERIFIED")
         .when(r_vs < 0.85, "PENDING")
         .when(r_vs < 0.90, "FAILED")
         .otherwise("EXPIRED")
    )

    # verificationDate: random date in config date range
    df = df.withColumn(
        "verificationDate",
        F.date_add(
            F.lit(cfg.START_DATE.strftime("%Y-%m-%d")),
            (F.rand(SEED + 411) * total_days).cast("int")
        ).cast("string")
    )

    # verificationMethod: DOCUMENT 40%, ELECTRONIC 35%, IN_PERSON 15%, VIDEO 10%
    r_vm = F.rand(SEED + 412)
    df = df.withColumn(
        "verificationMethod",
        F.when(r_vm < 0.40, "DOCUMENT")
         .when(r_vm < 0.75, "ELECTRONIC")
         .when(r_vm < 0.90, "IN_PERSON")
         .otherwise("VIDEO")
    )

    # riskRating: LOW 60%, MEDIUM 30%, HIGH 8%, VERY_HIGH 2%
    r_rr = F.rand(SEED + 413)
    df = df.withColumn(
        "riskRating",
        F.when(r_rr < 0.60, "LOW")
         .when(r_rr < 0.90, "MEDIUM")
         .when(r_rr < 0.98, "HIGH")
         .otherwise("VERY_HIGH")
    )

    # pepStatus: 2% true (politically exposed person)
    df = df.withColumn(
        "pepStatus",
        (F.rand(SEED + 414) < 0.02).cast("boolean")
    )

    # sanctionsCheck: CLEAR 95%, MATCH 2%, PENDING 3%
    r_sc = F.rand(SEED + 415)
    df = df.withColumn(
        "sanctionsCheck",
        F.when(r_sc < 0.95, "CLEAR")
         .when(r_sc < 0.97, "MATCH")
         .otherwise("PENDING")
    )

    # lastReviewDate: verificationDate + random(0, 365) days
    df = df.withColumn(
        "lastReviewDate",
        _udf_last_review_date(F.col("verificationDate"))
    )

    # nextReviewDate: lastReviewDate + 365 days
    df = df.withColumn(
        "nextReviewDate",
        _udf_next_review_date(F.col("lastReviewDate"))
    )

    # documentTypes: random comma-separated subset of KYC docs (2-4 items)
    df = df.withColumn(
        "documentTypes",
        _udf_document_types(F.col("global_id"))
    )

    # organisationId: ~35% of records are KYB (business verification).
    SEED = cfg.SEED
    df = df.withColumn(
        "organisationId",
        F.when(F.rand(SEED + 508) < 0.35,
               F.concat(F.lit("ORG-"),
                        F.lpad((F.abs(F.hash(F.col("global_id"), F.lit(SEED + 509)))
                                % cfg.N_ORGANISATIONS).cast("string"), 5, "0")))
         .otherwise(F.lit(None).cast("string"))
    )

    # KYB-specific fields — populated only when organisationId is present.
    df = (df
        .withColumn("abnVerified",
            F.when(F.col("organisationId").isNotNull(),
                   (F.rand(SEED + 510) < 0.90).cast("boolean"))
             .otherwise(F.lit(None).cast("boolean")))
        .withColumn("asicCheckStatus",
            F.when(F.col("organisationId").isNotNull(),
                   F.when(F.rand(SEED + 511) < 0.85, "CURRENT")
                    .when(F.rand(SEED + 511) < 0.95, "PENDING")
                    .otherwise("DEREGISTERED"))
             .otherwise(F.lit(None).cast("string")))
        .withColumn("beneficialOwnershipVerified",
            F.when(F.col("organisationId").isNotNull(),
                   (F.rand(SEED + 512) < 0.80).cast("boolean"))
             .otherwise(F.lit(None).cast("boolean")))
    )

    final = df.select(
        "global_id",
        "customerId",
        "kycId",
        "verificationStatus",
        "verificationDate",
        "verificationMethod",
        "riskRating",
        "pepStatus",
        "sanctionsCheck",
        "lastReviewDate",
        "nextReviewDate",
        "documentTypes",
        "organisationId",
        "abnVerified",
        "asicCheckStatus",
        "beneficialOwnershipVerified",
    )

    out_path = f"{cfg.OUTPUT_PATH}/kyc_records"
    final.write.mode("overwrite").parquet(out_path)
    print(f"  kyc_records: {N:,} rows")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(spark, cfg):
    print("=== Customer Profile Extended generation ===")
    _generate_insurance_policies(spark, cfg)
    _generate_kyc_records(spark, cfg)
    print("=== Customer Profile Extended done ===")


if __name__ == "__main__":
    import config as cfg

    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
