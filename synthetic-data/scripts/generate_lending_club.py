"""Lending Club synthetic data: accepted_loans (151 fields) + rejected_applications (9 fields).

Reads global_id pool from customers_fk.
SEED offsets: +20 to +99 (shared with original; extended fields use +50 onwards avoiding collision).
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd


def _customers_idx(spark, cfg):
    return spark.read.parquet(cfg.tmp_path("customers_fk")).withColumn(
        "cust_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


# ---------------------------------------------------------------------------
# Shared pandas UDFs (imported inside body per project convention)
# ---------------------------------------------------------------------------

@F.pandas_udf(StringType())
def _udf_loan_amount(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(20)
    return ids.map(lambda _: f"{np.random.lognormal(9.8, 0.7):.2f}")


@F.pandas_udf(StringType())
def _udf_interest_rate(grades: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(21)
    base = {"A": 6.5, "B": 10.5, "C": 14.2, "D": 18.9, "E": 23.8, "F": 27.5, "G": 30.5}
    return pd.Series([f"{max(1.0, base.get(g, 10.5) + np.random.lognormal(0, .1) - 1.0):.2f}" for g in grades])


@F.pandas_udf(StringType())
def _udf_dti(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(22)
    v = np.random.lognormal(3.1, 0.5, size=len(ids))
    return ids.map(lambda i: f"{min(v[int(i) % len(v)], 50.0):.2f}")


@F.pandas_udf(StringType())
def _udf_annual_income(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(23)
    return ids.map(lambda _: f"{np.random.lognormal(11.2, 0.5):.2f}")


@F.pandas_udf(StringType())
def _udf_installment(ids: pd.Series) -> pd.Series:
    """Monthly payment ~lognormal."""
    import numpy as np; np.random.seed(51)
    return ids.map(lambda _: f"{np.random.lognormal(5.2, 0.4):.2f}")


@F.pandas_udf(StringType())
def _udf_emp_title(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker(); Faker.seed(52)
    return ids.map(lambda _: fake.job())


@F.pandas_udf(StringType())
def _udf_revol_bal(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(60)
    return ids.map(lambda _: f"{np.random.lognormal(9.5, 0.8):.2f}")


@F.pandas_udf(StringType())
def _udf_tot_cur_bal(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(70)
    return ids.map(lambda _: f"{np.random.lognormal(11.2, 0.7):.2f}")


@F.pandas_udf(StringType())
def _udf_out_prncp(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(80)
    return ids.map(lambda _: f"{np.random.lognormal(8.5, 0.9):.2f}")


@F.pandas_udf(StringType())
def _udf_total_pymnt(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(81)
    return ids.map(lambda _: f"{np.random.lognormal(9.0, 0.7):.2f}")


@F.pandas_udf(StringType())
def _udf_mon_yyyy(ids: pd.Series) -> pd.Series:
    """Random Mon-YYYY date within 2010-2022."""
    import numpy as np
    rng = np.random.default_rng(82)
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    results = []
    for _ in ids:
        m = months[int(rng.integers(0, 12))]
        y = int(rng.integers(2010, 2023))
        results.append(f"{m}-{y}")
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_earliest_cr_line(ids: pd.Series) -> pd.Series:
    """Earlier credit history: Mon-YYYY within 1990-2010."""
    import numpy as np
    rng = np.random.default_rng(83)
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    results = []
    for _ in ids:
        m = months[int(rng.integers(0, 12))]
        y = int(rng.integers(1990, 2011))
        results.append(f"{m}-{y}")
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_app_title(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker(); Faker.seed(33)
    return ids.map(lambda _: fake.sentence(nb_words=6))


@F.pandas_udf(StringType())
def _udf_requested_amount(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(30)
    return ids.map(lambda _: f"{np.random.lognormal(9.2, 0.7):.2f}")


@F.pandas_udf(StringType())
def _udf_rej_dti(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(31)
    v = np.random.lognormal(3.5, 0.6, size=len(ids))
    return ids.map(lambda i: f"{min(v[int(i) % len(v)], 60.0):.2f}")


@F.pandas_udf(StringType())
def _udf_risk_score(ids: pd.Series) -> pd.Series:
    import numpy as np; np.random.seed(32)
    v = np.random.normal(610, 90, size=len(ids))
    return ids.map(lambda i: str(int(max(300, min(850, v[int(i) % len(v)])))))


# ---------------------------------------------------------------------------
# accepted_loans — 151 fields across 14 groups
# ---------------------------------------------------------------------------

def _generate_accepted_loans(spark, cfg):
    N, SEED = cfg.N_ACCEPTED_LOANS, cfg.SEED

    cust = _customers_idx(spark, cfg)
    total_days = (cfg.END_DATE - cfg.START_DATE).days

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("fk_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 20))) % cfg.N_CUSTOMERS).cast("int"))
    )
    base = (
        base.join(cust.select("cust_idx", "global_id"), base.fk_idx == cust.cust_idx, "left")
        .drop("fk_idx", "cust_idx")
    )

    # ── grade / sub_grade / term (core) ──────────────────────────────────────
    rg = F.rand(SEED + 21)
    grade = (F.when(rg < .35, "A").when(rg < .63, "B").when(rg < .81, "C").when(rg < .91, "D")
              .when(rg < .97, "E").when(rg < .99, "F").otherwise("G"))
    sub_grade = F.concat(grade, (F.abs(F.hash(F.col("id"), F.lit(SEED + 22))) % 5 + 1).cast("string"))
    term = F.when(F.rand(SEED + 23) < .70, " 36 months").otherwise(" 60 months")

    # ── loan_status ──────────────────────────────────────────────────────────
    rls = F.rand(SEED + 24)
    loan_status = (
        F.when(rls < .55, "Fully Paid")
         .when(rls < .80, "Current")
         .when(rls < .95, "Charged Off")
         .when(rls < .98, "Late (16-30 days)")
         .otherwise("Late (31-120 days)")
    )

    # ── purpose ──────────────────────────────────────────────────────────────
    rp = F.rand(SEED + 25)
    purpose = (
        F.when(rp < .35, "debt_consolidation").when(rp < .60, "credit_card")
         .when(rp < .70, "home_improvement").when(rp < .76, "major_purchase")
         .when(rp < .80, "medical").when(rp < .83, "small_business")
         .when(rp < .86, "car").when(rp < .89, "vacation")
         .when(rp < .92, "moving").when(rp < .95, "house")
         .when(rp < .97, "renewable_energy").otherwise("other")
    )

    # ── emp_length / home_ownership ──────────────────────────────────────────
    re = F.rand(SEED + 26)
    emp_length = (
        F.when(re < .08, "< 1 year").when(re < .16, "1 year").when(re < .25, "2 years")
         .when(re < .33, "3 years").when(re < .40, "4 years").when(re < .47, "5 years")
         .when(re < .53, "6 years").when(re < .59, "7 years").when(re < .65, "8 years")
         .when(re < .70, "9 years").otherwise("10+ years")
    )
    rh = F.rand(SEED + 27)
    home_ownership = F.when(rh < .45, "RENT").when(rh < .85, "MORTGAGE").when(rh < .98, "OWN").otherwise("OTHER")

    # ── dates ────────────────────────────────────────────────────────────────
    issued = F.date_add(F.lit(cfg.START_DATE.strftime("%Y-%m-%d")), (F.rand(SEED + 28) * total_days).cast("int"))

    # ── funded amounts ───────────────────────────────────────────────────────
    loan_amt_col = _udf_loan_amount(F.col("id"))
    funded = F.format_number(loan_amt_col.cast("double") * (0.95 + F.rand(SEED + 29) * 0.05), 2)
    funded_inv = F.format_number(loan_amt_col.cast("double") * (0.90 + F.rand(SEED + 53) * 0.09), 2)

    # ── verification_status / application_type ───────────────────────────────
    rv = F.rand(SEED + 54)
    verification_status = (
        F.when(rv < .45, "Not Verified").when(rv < .75, "Source Verified").otherwise("Verified")
    )
    ra = F.rand(SEED + 55)
    application_type = F.when(ra < .90, "Individual").otherwise("Joint App")

    # ── borrower_location ────────────────────────────────────────────────────
    us_states = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
                 "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
                 "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
                 "IA", "NV", "AR", "MS", "KS", "NM", "NE", "WV", "ID", "HI",
                 "NH", "ME", "RI", "MT", "DE", "SD", "ND", "AK", "VT"]
    state_idx = F.abs(F.hash(F.col("id"), F.lit(SEED + 56))) % len(us_states)
    # Build state via when chain (49 states)
    state_col = F.lit("CA")
    for i, s in enumerate(us_states):
        state_col = F.when(state_idx == i, s).otherwise(state_col)

    zip_prefix = F.lpad((F.abs(F.hash(F.col("id"), F.lit(SEED + 57))) % 900 + 100).cast("string"), 3, "0")
    zip_code = F.concat(zip_prefix, F.lit("xx"))

    # ── credit_history ───────────────────────────────────────────────────────
    fico_low = (F.rand(SEED + 58) * 185 + 660).cast("int").cast("string")
    fico_high = (F.col("fico_range_low").cast("int") + 4).cast("string")

    # ── revolving_credit ─────────────────────────────────────────────────────
    revol_util = F.format_number(F.rand(SEED + 62) * 100, 1)
    total_rev_hi_lim = F.format_number(
        F.col("revol_bal").cast("double") / (F.rand(SEED + 63) * 0.6 + 0.1), 2
    )

    # ── loan_performance ─────────────────────────────────────────────────────
    out_prncp_inv_col = F.format_number(F.col("out_prncp").cast("double") * (0.85 + F.rand(SEED + 84) * 0.15), 2)
    total_pymnt_inv_col = F.format_number(F.col("total_pymnt").cast("double") * (0.85 + F.rand(SEED + 85) * 0.15), 2)
    total_rec_prncp_col = F.format_number(F.col("total_pymnt").cast("double") * (0.70 + F.rand(SEED + 86) * 0.20), 2)
    total_rec_int_col = F.format_number(
        F.col("total_pymnt").cast("double") - F.regexp_replace(F.col("total_rec_prncp"), ",", "").cast("double"), 2
    )
    last_fico_low = (F.rand(SEED + 87) * 185 + 660).cast("int").cast("string")

    # ── joint (only for Joint App ~10%) ──────────────────────────────────────
    is_joint = F.col("application_type") == "Joint App"

    # ── hardship_flag (~3%) ───────────────────────────────────────────────────
    hardship_rand = F.rand(SEED + 90)
    hardship_flag = F.when(hardship_rand < 0.03, "Y").otherwise("N")
    is_hardship = F.col("hardship_flag") == "Y"

    hardship_reasons = ["UNEMPLOYMENT", "MEDICAL", "INCOME_CURTAILMENT", "EXCESSIVE_OBLIGATIONS",
                        "DISABILITY", "DIVORCE", "FAMILY_DEATH", "NATURAL_DISASTER", "REDUCED_HOURS"]
    rhr = F.rand(SEED + 91)
    hardship_reason_col = (
        F.when(is_hardship,
               F.when(rhr < .25, "UNEMPLOYMENT").when(rhr < .45, "MEDICAL")
                .when(rhr < .60, "INCOME_CURTAILMENT").when(rhr < .72, "EXCESSIVE_OBLIGATIONS")
                .when(rhr < .80, "DISABILITY").when(rhr < .87, "DIVORCE")
                .when(rhr < .92, "FAMILY_DEATH").when(rhr < .97, "NATURAL_DISASTER")
                .otherwise("REDUCED_HOURS"))
         .otherwise(F.lit(None).cast("string"))
    )

    # ── debt_settlement_flag (~2%) ────────────────────────────────────────────
    ds_rand = F.rand(SEED + 95)
    debt_settlement_flag = F.when(ds_rand < 0.02, "Y").otherwise("N")
    is_ds = F.col("debt_settlement_flag") == "Y"

    df = (
        base
        # ── loan_identification ──
        .withColumn("loanId",            F.expr("uuid()"))        # id
        .withColumn("memberId",          F.expr("uuid()"))         # member_id
        .withColumn("url", F.concat(
            F.lit("https://lendingclub.com/browse/loanDetail.action?loan_id="),
            F.col("id").cast("string")))
        .withColumn("policy_code",       F.lit("1"))
        .withColumn("disbursement_method", F.lit("Cash"))
        # ── loan_terms ──
        .withColumn("loanAmount",        loan_amt_col)             # loan_amnt
        .withColumn("fundedAmount",      funded)                   # funded_amnt
        .withColumn("funded_amnt_inv",   funded_inv)
        .withColumn("term",              term)
        .withColumn("interestRate",      _udf_interest_rate(grade))  # int_rate
        .withColumn("installment",       _udf_installment(F.col("id")))
        .withColumn("grade",             grade)
        .withColumn("subGrade",          sub_grade)                # sub_grade
        # ── borrower_profile ──
        .withColumn("emp_title",         _udf_emp_title(F.col("id")))
        .withColumn("empLength",         emp_length)               # emp_length
        .withColumn("homeOwnership",     home_ownership)           # home_ownership
        .withColumn("annualIncome",      _udf_annual_income(F.col("id")))  # annual_inc
        .withColumn("verification_status", verification_status)
        .withColumn("application_type", application_type)
        .withColumn("dti",               _udf_dti(F.col("id")))
        .withColumn("purpose",           purpose)
        # ── loan_description ──
        .withColumn("desc",              F.lit(None).cast("string"))  # mostly blank
        .withColumn("title",             _udf_app_title(F.col("id")))
        .withColumn("pymnt_plan",        F.when(F.rand(SEED + 56) < 0.01, "y").otherwise("n"))
        # ── borrower_location ──
        .withColumn("zip_code",          zip_code)
        .withColumn("addr_state",        state_col)
        # ── credit_history ──
        .withColumn("earliest_cr_line",  _udf_earliest_cr_line(F.col("id")))
        .withColumn("fico_range_low",    fico_low)
        .withColumn("fico_range_high",   (F.col("fico_range_low").cast("int") + 4).cast("string"))
        .withColumn("delinq_2yrs",       (F.rand(SEED + 58) * 5).cast("int").cast("string"))
        .withColumn("inq_last_6mths",    (F.rand(SEED + 59) * 5).cast("int").cast("string"))
        .withColumn("mths_since_last_delinq",
                    F.when(F.rand(SEED + 60) < 0.40, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 60) * 60 + 1).cast("int").cast("string")))
        .withColumn("mths_since_last_record",
                    F.when(F.rand(SEED + 61) < 0.70, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 61) * 100 + 1).cast("int").cast("string")))
        .withColumn("open_acc",          (F.rand(SEED + 62) * 25 + 2).cast("int").cast("string"))
        .withColumn("pub_rec",           (F.rand(SEED + 63) * 3).cast("int").cast("string"))
        .withColumn("total_acc",         (F.rand(SEED + 64) * 60 + 5).cast("int").cast("string"))
        .withColumn("collections_12_mths_ex_med", (F.rand(SEED + 65) * 2).cast("int").cast("string"))
        .withColumn("mths_since_last_major_derog",
                    F.when(F.rand(SEED + 66) < 0.65, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 66) * 80 + 1).cast("int").cast("string")))
        .withColumn("acc_now_delinq",    (F.rand(SEED + 67) * 2).cast("int").cast("string"))
        .withColumn("tot_coll_amt",      F.format_number(F.rand(SEED + 68) * 5000, 2))
        .withColumn("pub_rec_bankruptcies", (F.rand(SEED + 69) * 2).cast("int").cast("string"))
        .withColumn("tax_liens",         (F.rand(SEED + 70) * 2).cast("int").cast("string"))
        .withColumn("chargeoff_within_12_mths", (F.rand(SEED + 71) * 2).cast("int").cast("string"))
        .withColumn("delinq_amnt",       F.format_number(F.rand(SEED + 72) * 1000, 2))
        # ── revolving_credit ──
        .withColumn("revol_bal",         _udf_revol_bal(F.col("id")))
        .withColumn("revol_util",        revol_util)
        .withColumn("total_rev_hi_lim",  total_rev_hi_lim)
        .withColumn("bc_open_to_buy",    F.format_number(F.rand(SEED + 73) * 20000, 2))
        .withColumn("bc_util",           F.format_number(F.rand(SEED + 74) * 100, 1))
        .withColumn("max_bal_bc",        F.format_number(F.rand(SEED + 75) * 15000, 2))
        .withColumn("num_actv_bc_tl",    (F.rand(SEED + 76) * 10).cast("int").cast("string"))
        .withColumn("num_bc_sats",       (F.rand(SEED + 77) * 15).cast("int").cast("string"))
        .withColumn("num_bc_tl",         (F.rand(SEED + 78) * 20).cast("int").cast("string"))
        .withColumn("num_actv_rev_tl",   (F.rand(SEED + 79) * 15).cast("int").cast("string"))
        .withColumn("num_op_rev_tl",     (F.rand(SEED + 80) * 18).cast("int").cast("string"))
        .withColumn("num_rev_accts",     (F.rand(SEED + 81) * 25).cast("int").cast("string"))
        .withColumn("num_rev_tl_bal_gt_0", (F.rand(SEED + 82) * 12).cast("int").cast("string"))
        .withColumn("open_rv_12m",       (F.rand(SEED + 83) * 5).cast("int").cast("string"))
        .withColumn("open_rv_24m",       (F.rand(SEED + 84) * 8).cast("int").cast("string"))
        .withColumn("percent_bc_gt_75",  F.format_number(F.rand(SEED + 85) * 100, 1))
        .withColumn("pct_tl_nvr_dlq",   F.format_number(F.rand(SEED + 86) * 40 + 60, 1))
        .withColumn("mths_since_recent_bc", (F.rand(SEED + 87) * 24).cast("int").cast("string"))
        .withColumn("mths_since_recent_bc_dlq",
                    F.when(F.rand(SEED + 88) < 0.50, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 88) * 60 + 1).cast("int").cast("string")))
        .withColumn("mths_since_recent_revol_delinq",
                    F.when(F.rand(SEED + 89) < 0.50, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 89) * 60 + 1).cast("int").cast("string")))
        # ── installment_credit ──
        .withColumn("total_bal_il",      F.format_number(F.rand(SEED + 90) * 80000, 2))
        .withColumn("il_util",
                    F.when(F.rand(SEED + 91) < 0.20, F.lit(None).cast("string"))
                     .otherwise(F.format_number(F.rand(SEED + 91) * 100, 1)))
        .withColumn("open_act_il",       (F.rand(SEED + 92) * 8).cast("int").cast("string"))
        .withColumn("open_il_12m",       (F.rand(SEED + 93) * 3).cast("int").cast("string"))
        .withColumn("open_il_24m",       (F.rand(SEED + 94) * 5).cast("int").cast("string"))
        .withColumn("mths_since_rcnt_il", (F.rand(SEED + 95) * 36).cast("int").cast("string"))
        .withColumn("num_il_tl",         (F.rand(SEED + 96) * 30).cast("int").cast("string"))
        .withColumn("total_il_high_credit_limit", F.format_number(F.rand(SEED + 97) * 200000, 2))
        .withColumn("mo_sin_old_il_acct", (F.rand(SEED + 98) * 200 + 12).cast("int").cast("string"))
        .withColumn("total_bal_ex_mort", F.format_number(F.rand(SEED + 99) * 100000, 2))
        # ── aggregate_credit ──
        .withColumn("tot_cur_bal",       _udf_tot_cur_bal(F.col("id")))
        .withColumn("tot_hi_cred_lim",   F.format_number(F.col("tot_cur_bal").cast("double") * 1.3, 2))
        .withColumn("total_bc_limit",    F.format_number(F.rand(SEED + 35) * 50000, 2))
        .withColumn("all_util",          F.format_number(F.rand(SEED + 36) * 100, 1))
        .withColumn("avg_cur_bal",       F.format_number(F.col("tot_cur_bal").cast("double") / (F.rand(SEED + 37) * 15 + 3), 2))
        .withColumn("acc_open_past_24mths", (F.rand(SEED + 38) * 10).cast("int").cast("string"))
        .withColumn("num_sats",          (F.rand(SEED + 39) * 30).cast("int").cast("string"))
        .withColumn("num_tl_120dpd_2m",
                    F.when(F.rand(SEED + 40) < 0.90, "0")
                     .otherwise((F.rand(SEED + 40) * 3 + 1).cast("int").cast("string")))
        .withColumn("num_tl_30dpd",      (F.rand(SEED + 41) * 3).cast("int").cast("string"))
        .withColumn("num_tl_90g_dpd_24m", (F.rand(SEED + 42) * 3).cast("int").cast("string"))
        .withColumn("num_tl_op_past_12m", (F.rand(SEED + 43) * 8).cast("int").cast("string"))
        .withColumn("inq_fi",
                    F.when(F.rand(SEED + 44) < 0.30, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 44) * 5).cast("int").cast("string")))
        .withColumn("total_cu_tl",
                    F.when(F.rand(SEED + 45) < 0.30, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 45) * 8).cast("int").cast("string")))
        .withColumn("inq_last_12m",
                    F.when(F.rand(SEED + 46) < 0.20, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 46) * 8).cast("int").cast("string")))
        .withColumn("open_acc_6m",
                    F.when(F.rand(SEED + 47) < 0.20, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 47) * 4).cast("int").cast("string")))
        .withColumn("mort_acc",          (F.rand(SEED + 48) * 5).cast("int").cast("string"))
        # ── additional_aggregate ──
        .withColumn("mo_sin_old_rev_tl_op", (F.rand(SEED + 49) * 200 + 6).cast("int").cast("string"))
        .withColumn("mo_sin_rcnt_rev_tl_op", (F.rand(SEED + 50) * 24).cast("int").cast("string"))
        .withColumn("mo_sin_rcnt_tl",    (F.rand(SEED + 51) * 12).cast("int").cast("string"))
        .withColumn("mths_since_recent_inq",
                    F.when(F.rand(SEED + 52) < 0.15, F.lit(None).cast("string"))
                     .otherwise((F.rand(SEED + 52) * 12).cast("int").cast("string")))
        .withColumn("num_accts_ever_120_pd", (F.rand(SEED + 53) * 4).cast("int").cast("string"))
        # ── loan_performance ──
        .withColumn("loanStatus",        loan_status)              # loan_status
        .withColumn("issuedAt",          issued.cast("string"))    # issue_d
        .withColumn("initial_list_status", F.when(F.rand(SEED + 54) < 0.70, "w").otherwise("f"))
        .withColumn("out_prncp",         _udf_out_prncp(F.col("id")))
        .withColumn("out_prncp_inv",     out_prncp_inv_col)
        .withColumn("total_pymnt",       _udf_total_pymnt(F.col("id")))
        .withColumn("total_pymnt_inv",   total_pymnt_inv_col)
        .withColumn("total_rec_prncp",   total_rec_prncp_col)
        .withColumn("total_rec_int",     total_rec_int_col)
        .withColumn("total_rec_late_fee", F.format_number(F.rand(SEED + 88) * 50, 2))
        .withColumn("recoveries",
                    F.when(F.col("loanStatus") == "Charged Off",
                           F.format_number(F.rand(SEED + 89) * 5000, 2))
                     .otherwise("0.00"))
        .withColumn("collection_recovery_fee",
                    F.when(F.col("loanStatus") == "Charged Off",
                           F.format_number(F.rand(SEED + 90) * 500, 2))
                     .otherwise("0.00"))
        .withColumn("last_pymnt_d",      _udf_mon_yyyy(F.col("id")))
        .withColumn("last_pymnt_amnt",   F.format_number(F.rand(SEED + 91) * 2000 + 50, 2))
        .withColumn("next_pymnt_d",
                    F.when(F.col("loanStatus").isin("Fully Paid", "Charged Off"),
                           F.lit(None).cast("string"))
                     .otherwise(_udf_mon_yyyy(F.col("id"))))
        .withColumn("last_credit_pull_d", _udf_mon_yyyy(F.col("id")))
        .withColumn("last_fico_range_low", last_fico_low)
        .withColumn("last_fico_range_high", (F.col("last_fico_range_low").cast("int") + 4).cast("string"))
        # ── joint_application (null for Individual) ──
        .withColumn("annual_inc_joint",
                    F.when(is_joint, F.format_number(F.rand(SEED + 92) * 150000 + 30000, 2))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("dti_joint",
                    F.when(is_joint, F.format_number(F.rand(SEED + 93) * 40 + 5, 2))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("verification_status_joint",
                    F.when(is_joint,
                           F.when(F.rand(SEED + 94) < .40, "Not Verified")
                            .when(F.rand(SEED + 94) < .70, "Source Verified")
                            .otherwise("Verified"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("revol_bal_joint",
                    F.when(is_joint, F.format_number(F.rand(SEED + 95) * 50000, 2))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_fico_range_low",
                    F.when(is_joint, (F.rand(SEED + 96) * 185 + 660).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_fico_range_high",
                    F.when(is_joint, (F.col("sec_app_fico_range_low").cast("int") + 4).cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_earliest_cr_line",
                    F.when(is_joint, _udf_earliest_cr_line(F.col("id")))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_inq_last_6mths",
                    F.when(is_joint, (F.rand(SEED + 97) * 4).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_mort_acc",
                    F.when(is_joint, (F.rand(SEED + 98) * 3).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_open_acc",
                    F.when(is_joint, (F.rand(SEED + 99) * 15 + 2).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_revol_util",
                    F.when(is_joint, F.format_number(F.rand(SEED + 92) * 100, 1))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_open_act_il",
                    F.when(is_joint, (F.rand(SEED + 93) * 6).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_num_rev_accts",
                    F.when(is_joint, (F.rand(SEED + 94) * 20).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_chargeoff_within_12_mths",
                    F.when(is_joint, (F.rand(SEED + 95) * 2).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_collections_12_mths_ex_med",
                    F.when(is_joint, (F.rand(SEED + 96) * 2).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("sec_app_mths_since_last_major_derog",
                    F.when(is_joint,
                           F.when(F.rand(SEED + 97) < 0.60, F.lit(None).cast("string"))
                            .otherwise((F.rand(SEED + 97) * 80 + 1).cast("int").cast("string")))
                     .otherwise(F.lit(None).cast("string")))
        # ── hardship_program (3% enrolled) ──
        .withColumn("hardship_flag",     hardship_flag)
        .withColumn("hardship_type",
                    F.when(is_hardship, F.lit("INTEREST ONLY")).otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_reason",   hardship_reason_col)
        .withColumn("hardship_status",
                    F.when(is_hardship,
                           F.when(F.rand(SEED + 92) < .50, "ACTIVE")
                            .when(F.rand(SEED + 92) < .80, "COMPLETED")
                            .otherwise("BROKEN"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("deferral_term",
                    F.when(is_hardship, F.lit("3.0")).otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_amount",
                    F.when(is_hardship, F.format_number(F.rand(SEED + 93) * 500 + 50, 2))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_start_date",
                    F.when(is_hardship, _udf_mon_yyyy(F.col("id"))).otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_end_date",
                    F.when(is_hardship, _udf_mon_yyyy(F.col("id"))).otherwise(F.lit(None).cast("string")))
        .withColumn("payment_plan_start_date",
                    F.when(is_hardship, _udf_mon_yyyy(F.col("id"))).otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_length",
                    F.when(is_hardship, F.lit("3.0")).otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_dpd",
                    F.when(is_hardship, (F.rand(SEED + 94) * 30).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_loan_status",
                    F.when(is_hardship,
                           F.when(F.rand(SEED + 95) < .50, "Current")
                            .when(F.rand(SEED + 95) < .75, "In Grace Period")
                            .when(F.rand(SEED + 95) < .90, "Late (16-30 days)")
                            .otherwise("Late (31-120 days)"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("orig_projected_additional_accrued_interest",
                    F.when(is_hardship, F.format_number(F.rand(SEED + 96) * 1000, 2))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_payoff_balance_amount",
                    F.when(is_hardship, F.format_number(F.rand(SEED + 97) * 15000, 2))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("hardship_last_payment_amount",
                    F.when(is_hardship, F.format_number(F.rand(SEED + 98) * 2000, 2))
                     .otherwise(F.lit(None).cast("string")))
        # ── debt_settlement (2% enrolled) ──
        .withColumn("debt_settlement_flag", debt_settlement_flag)
        .withColumn("debt_settlement_flag_date",
                    F.when(is_ds, _udf_mon_yyyy(F.col("id"))).otherwise(F.lit(None).cast("string")))
        .withColumn("settlement_status",
                    F.when(is_ds,
                           F.when(F.rand(SEED + 96) < .40, "ACTIVE")
                            .when(F.rand(SEED + 96) < .75, "COMPLETE")
                            .otherwise("BROKEN"))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("settlement_date",
                    F.when(is_ds, _udf_mon_yyyy(F.col("id"))).otherwise(F.lit(None).cast("string")))
        .withColumn("settlement_amount",
                    F.when(is_ds, F.format_number(F.rand(SEED + 97) * 10000 + 500, 2))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("settlement_percentage",
                    F.when(is_ds, F.format_number(F.rand(SEED + 98) * 40 + 30, 1))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("settlement_term",
                    F.when(is_ds, (F.rand(SEED + 99) * 24 + 3).cast("int").cast("string"))
                     .otherwise(F.lit(None).cast("string")))
    )

    final_cols = [
        "global_id",
        # loan_identification
        "loanId", "memberId", "url", "policy_code", "disbursement_method",
        # loan_terms
        "loanAmount", "fundedAmount", "funded_amnt_inv", "term", "interestRate",
        "installment", "grade", "subGrade",
        # borrower_profile
        "emp_title", "empLength", "homeOwnership", "annualIncome",
        "verification_status", "application_type", "dti", "purpose",
        # loan_description
        "desc", "title", "pymnt_plan",
        # borrower_location
        "zip_code", "addr_state",
        # credit_history
        "earliest_cr_line", "fico_range_low", "fico_range_high",
        "delinq_2yrs", "inq_last_6mths", "mths_since_last_delinq", "mths_since_last_record",
        "open_acc", "pub_rec", "total_acc", "collections_12_mths_ex_med",
        "mths_since_last_major_derog", "acc_now_delinq", "tot_coll_amt",
        "pub_rec_bankruptcies", "tax_liens", "chargeoff_within_12_mths", "delinq_amnt",
        # revolving_credit
        "revol_bal", "revol_util", "total_rev_hi_lim", "bc_open_to_buy", "bc_util",
        "max_bal_bc", "num_actv_bc_tl", "num_bc_sats", "num_bc_tl", "num_actv_rev_tl",
        "num_op_rev_tl", "num_rev_accts", "num_rev_tl_bal_gt_0", "open_rv_12m", "open_rv_24m",
        "percent_bc_gt_75", "pct_tl_nvr_dlq", "mths_since_recent_bc",
        "mths_since_recent_bc_dlq", "mths_since_recent_revol_delinq",
        # installment_credit
        "total_bal_il", "il_util", "open_act_il", "open_il_12m", "open_il_24m",
        "mths_since_rcnt_il", "num_il_tl", "total_il_high_credit_limit",
        "mo_sin_old_il_acct", "total_bal_ex_mort",
        # aggregate_credit
        "tot_cur_bal", "tot_hi_cred_lim", "total_bc_limit", "all_util", "avg_cur_bal",
        "acc_open_past_24mths", "num_sats", "num_tl_120dpd_2m", "num_tl_30dpd",
        "num_tl_90g_dpd_24m", "num_tl_op_past_12m", "inq_fi", "total_cu_tl",
        "inq_last_12m", "open_acc_6m", "mort_acc",
        # additional_aggregate
        "mo_sin_old_rev_tl_op", "mo_sin_rcnt_rev_tl_op", "mo_sin_rcnt_tl",
        "mths_since_recent_inq", "num_accts_ever_120_pd",
        # loan_performance
        "loanStatus", "issuedAt", "initial_list_status",
        "out_prncp", "out_prncp_inv", "total_pymnt", "total_pymnt_inv",
        "total_rec_prncp", "total_rec_int", "total_rec_late_fee",
        "recoveries", "collection_recovery_fee",
        "last_pymnt_d", "last_pymnt_amnt", "next_pymnt_d", "last_credit_pull_d",
        "last_fico_range_low", "last_fico_range_high",
        # joint_application
        "annual_inc_joint", "dti_joint", "verification_status_joint", "revol_bal_joint",
        "sec_app_fico_range_low", "sec_app_fico_range_high", "sec_app_earliest_cr_line",
        "sec_app_inq_last_6mths", "sec_app_mort_acc", "sec_app_open_acc",
        "sec_app_revol_util", "sec_app_open_act_il", "sec_app_num_rev_accts",
        "sec_app_chargeoff_within_12_mths", "sec_app_collections_12_mths_ex_med",
        "sec_app_mths_since_last_major_derog",
        # hardship_program
        "hardship_flag", "hardship_type", "hardship_reason", "hardship_status",
        "deferral_term", "hardship_amount", "hardship_start_date", "hardship_end_date",
        "payment_plan_start_date", "hardship_length", "hardship_dpd",
        "hardship_loan_status", "orig_projected_additional_accrued_interest",
        "hardship_payoff_balance_amount", "hardship_last_payment_amount",
        # debt_settlement
        "debt_settlement_flag", "debt_settlement_flag_date", "settlement_status",
        "settlement_date", "settlement_amount", "settlement_percentage", "settlement_term",
    ]

    df.select(final_cols).write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/accepted_loans")
    print(f"  accepted_loans: {N:,} rows ({len(final_cols) - 1} data fields)")


# ---------------------------------------------------------------------------
# rejected_applications — 9 fields
# ---------------------------------------------------------------------------

def _generate_rejected_applications(spark, cfg):
    N, SEED = cfg.N_REJECTED_APPS, cfg.SEED

    cust = _customers_idx(spark, cfg)
    total_days = (cfg.END_DATE - cfg.START_DATE).days

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("fk_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 50))) % cfg.N_CUSTOMERS).cast("int"))
    )
    base = (
        base.join(cust.select("cust_idx", "global_id"), base.fk_idx == cust.cust_idx, "left")
        .drop("fk_idx", "cust_idx")
    )

    rr = F.rand(SEED + 34)
    reason = (
        F.when(rr < .30, "DTI_too_high").when(rr < .55, "insufficient_credit_history")
         .when(rr < .75, "delinquencies").when(rr < .90, "credit_score_too_low")
         .otherwise("other")
    )
    app_date = F.date_add(
        F.lit(cfg.START_DATE.strftime("%Y-%m-%d")), (F.rand(SEED + 35) * total_days).cast("int")
    )

    # US state for rejected (same pool)
    us_states = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
                 "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
                 "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT"]
    state_idx = F.abs(F.hash(F.col("id"), F.lit(SEED + 36))) % len(us_states)
    state_col = F.lit("CA")
    for i, s in enumerate(us_states):
        state_col = F.when(state_idx == i, s).otherwise(state_col)

    zip_prefix = F.lpad((F.abs(F.hash(F.col("id"), F.lit(SEED + 37))) % 900 + 100).cast("string"), 3, "0")

    re = F.rand(SEED + 38)
    emp_length = (
        F.when(re < .08, "< 1 year").when(re < .16, "1 year").when(re < .25, "2 years")
         .when(re < .33, "3 years").when(re < .40, "4 years").when(re < .47, "5 years")
         .when(re < .53, "6 years").when(re < .59, "7 years").when(re < .65, "8 years")
         .when(re < .70, "9 years").otherwise("10+ years")
    )

    df = (
        base
        .withColumn("loanId",            F.expr("uuid()"))
        .withColumn("requestedAmount",   _udf_requested_amount(F.col("id")))
        .withColumn("applicationDate",   app_date.cast("string"))
        .withColumn("riskScore",         _udf_risk_score(F.col("id")))
        .withColumn("debtToIncomeRatio", _udf_rej_dti(F.col("id")))
        .withColumn("rejectionReason",   reason)
        .withColumn("applicationTitle",  _udf_app_title(F.col("id")))
        .withColumn("zipCode",           F.concat(zip_prefix, F.lit("xx")))
        .withColumn("state",             state_col)
        .withColumn("employmentLength",  emp_length)
        .withColumn("policyCode",        F.lit("1"))
        .select("global_id", "loanId", "requestedAmount", "applicationDate",
                "riskScore", "debtToIncomeRatio", "rejectionReason", "applicationTitle",
                "zipCode", "state", "employmentLength", "policyCode")
    )
    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/rejected_applications")
    print(f"  rejected_applications: {N:,} rows")


def run(spark, cfg):
    print("=== Lending Club generation ===")
    _generate_accepted_loans(spark, cfg)
    _generate_rejected_applications(spark, cfg)
    print("=== Lending Club done ===")


if __name__ == "__main__":
    import config as cfg  # noqa: F811
    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
