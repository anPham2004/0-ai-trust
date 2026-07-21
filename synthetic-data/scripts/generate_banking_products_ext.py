"""Banking Products Extended: mortgage_accounts, loan_accounts, credit_cards.

mortgage_accounts and loan_accounts FK to banking_accounts via accounts_fk.
credit_cards FK to customers via customers_fk.
SEED offsets: +300 to +399.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# FK helpers
# ---------------------------------------------------------------------------

def _load_accounts_idx(spark, cfg):
    return spark.read.parquet(cfg.tmp_path("accounts_fk")).withColumn(
        "acc_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


def _load_customers_idx(spark, cfg):
    return spark.read.parquet(cfg.tmp_path("customers_fk")).withColumn(
        "cust_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


# ---------------------------------------------------------------------------
# Mortgage accounts  (SEED +300 .. +319)
# ---------------------------------------------------------------------------

def _generate_mortgage_accounts(spark, cfg):
    N, SEED = cfg.N_MORTGAGE_ACCOUNTS, cfg.SEED

    # --- pandas UDFs (numpy imported inside body) ---

    @F.pandas_udf(StringType())
    def property_value_udf(ids):
        import numpy as np
        np.random.seed(SEED + 300)
        vals = np.random.lognormal(13.4, 0.4, size=len(ids))
        return ids.map(lambda i: f"{vals[int(i) % len(vals)]:.2f}")

    @F.pandas_udf(StringType())
    def loan_amount_udf(prop_values):
        import numpy as np
        np.random.seed(SEED + 301)
        lvrs = np.random.uniform(0.60, 0.95, size=len(prop_values))
        return prop_values.map(lambda pv: f"{float(pv) * lvrs[0]:.2f}") \
            if len(prop_values) == 0 else \
            prop_values.reset_index(drop=True).combine(
                prop_values.__class__(lvrs[:len(prop_values)]),
                lambda pv, lvr: f"{float(pv) * lvr:.2f}"
            )

    @F.pandas_udf(StringType())
    def interest_rate_udf(ids):
        import numpy as np
        np.random.seed(SEED + 302)
        rates = np.random.normal(6.2, 0.8, size=len(ids))
        return ids.map(lambda i: f"{max(2.0, rates[int(i) % len(rates)]):.2f}%")

    @F.pandas_udf(StringType())
    def repayment_amount_udf(loan_amounts, rates, terms):
        """Standard amortization: P * r * (1+r)^n / ((1+r)^n - 1)."""
        import numpy as np
        results = []
        for la, r, t in zip(loan_amounts, rates, terms):
            principal = float(la)
            annual_rate = float(r.replace("%", "")) / 100.0
            monthly_rate = annual_rate / 12.0
            n_months = int(t) * 12
            if monthly_rate == 0:
                payment = principal / n_months
            else:
                payment = principal * monthly_rate * (1 + monthly_rate) ** n_months / \
                          ((1 + monthly_rate) ** n_months - 1)
            results.append(f"{payment:.2f}")
        return loan_amounts.__class__(results)

    @F.pandas_udf(StringType())
    def lvr_percent_udf(loan_amounts, prop_values):
        results = []
        for la, pv in zip(loan_amounts, prop_values):
            lvr = float(la) / float(pv) * 100.0
            results.append(f"{lvr:.2f}")
        return loan_amounts.__class__(results)

    # --- build dataframe ---

    accts = _load_accounts_idx(spark, cfg)
    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("acc_fk", (F.abs(F.hash(F.col("id"), F.lit(SEED + 303))) % cfg.N_ACCOUNTS).cast("int"))
    )
    base = (
        base.join(accts.select("acc_idx", "global_id", "accountId"), base.acc_fk == accts.acc_idx, "left")
        .drop("acc_fk", "acc_idx")
    )

    r1 = F.rand(SEED + 304)
    interest_type = (
        F.when(r1 < 0.40, "FIXED")
        .when(r1 < 0.90, "VARIABLE")
        .otherwise("SPLIT")
    )

    r2 = F.rand(SEED + 305)
    loan_term = (
        F.when(r2 < 0.05, 15)
        .when(r2 < 0.20, 20)
        .when(r2 < 0.45, 25)
        .otherwise(30)
    )

    r3 = F.rand(SEED + 306)
    repayment_freq = (
        F.when(r3 < 0.60, "MONTHLY")
        .when(r3 < 0.90, "FORTNIGHTLY")
        .otherwise("WEEKLY")
    )

    df = (
        base
        .withColumn("mortgageId", F.expr("uuid()"))
        .withColumn("propertyValue", property_value_udf(F.col("id")))
        .withColumn("loanAmount", loan_amount_udf(F.col("propertyValue")))
        .withColumn("interestRate", interest_rate_udf(F.col("id")))
        .withColumn("interestType", interest_type)
        .withColumn("loanTerm", loan_term)
        .withColumn("repaymentAmount",
                    repayment_amount_udf(F.col("loanAmount"), F.col("interestRate"), F.col("loanTerm").cast("string")))
        .withColumn("repaymentFrequency", repayment_freq)
        .withColumn("offsetAccountId",
                    F.when(F.rand(SEED + 307) < 0.30, F.expr("uuid()")).otherwise(F.lit(None).cast("string")))
        .withColumn("startDate",
                    F.date_add(F.current_date(), -(F.rand(SEED + 308) * 3650).cast("int")).cast("string"))
        .withColumn("lvrPercent", lvr_percent_udf(F.col("loanAmount"), F.col("propertyValue")))
        .select("global_id", "accountId", "mortgageId", "propertyValue", "loanAmount",
                "interestRate", "interestType", "loanTerm", "repaymentAmount",
                "repaymentFrequency", "offsetAccountId", "startDate", "lvrPercent")
    )
    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/mortgage_accounts")
    print(f"  mortgage_accounts: {N:,} rows")


# ---------------------------------------------------------------------------
# Loan accounts  (SEED +320 .. +339)
# ---------------------------------------------------------------------------

def _generate_loan_accounts(spark, cfg):
    N, SEED = cfg.N_LOAN_ACCOUNTS, cfg.SEED

    # --- pandas UDFs ---

    @F.pandas_udf(StringType())
    def original_amount_udf(ids):
        import numpy as np
        np.random.seed(SEED + 320)
        vals = np.random.lognormal(10.1, 0.5, size=len(ids))
        return ids.map(lambda i: f"{vals[int(i) % len(vals)]:.2f}")

    @F.pandas_udf(StringType())
    def current_balance_udf(orig_amounts):
        import numpy as np
        np.random.seed(SEED + 321)
        factors = np.random.uniform(0.1, 0.95, size=len(orig_amounts))
        results = []
        for oa, f in zip(orig_amounts, factors):
            results.append(f"{float(oa) * f:.2f}")
        return orig_amounts.__class__(results)

    @F.pandas_udf(StringType())
    def loan_interest_rate_udf(ids):
        import numpy as np
        np.random.seed(SEED + 322)
        rates = np.random.normal(8.5, 1.5, size=len(ids))
        return ids.map(lambda i: f"{max(3.0, rates[int(i) % len(rates)]):.2f}%")

    @F.pandas_udf(StringType())
    def loan_repayment_udf(orig_amounts, rates, term_months):
        """Standard amortization: P * r * (1+r)^n / ((1+r)^n - 1)."""
        import numpy as np
        results = []
        for oa, r, tm in zip(orig_amounts, rates, term_months):
            principal = float(oa)
            annual_rate = float(r.replace("%", "")) / 100.0
            monthly_rate = annual_rate / 12.0
            n = int(tm)
            if monthly_rate == 0:
                payment = principal / n
            else:
                payment = principal * monthly_rate * (1 + monthly_rate) ** n / \
                          ((1 + monthly_rate) ** n - 1)
            results.append(f"{payment:.2f}")
        return orig_amounts.__class__(results)

    @F.pandas_udf(StringType())
    def maturity_date_udf(start_dates, term_months):
        import pandas as pd
        from dateutil.relativedelta import relativedelta
        from datetime import datetime
        results = []
        for sd, tm in zip(start_dates, term_months):
            try:
                dt = datetime.strptime(sd, "%Y-%m-%d")
                mat = dt + relativedelta(months=int(tm))
                results.append(mat.strftime("%Y-%m-%d"))
            except Exception:
                results.append(None)
        return start_dates.__class__(results)

    # --- build dataframe ---

    accts = _load_accounts_idx(spark, cfg)
    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("acc_fk", (F.abs(F.hash(F.col("id"), F.lit(SEED + 323))) % cfg.N_ACCOUNTS).cast("int"))
    )
    base = (
        base.join(accts.select("acc_idx", "global_id", "accountId"), base.acc_fk == accts.acc_idx, "left")
        .drop("acc_fk", "acc_idx")
    )

    r1 = F.rand(SEED + 324)
    loan_type = (
        F.when(r1 < 0.50, "PERSONAL")
        .when(r1 < 0.75, "CAR")
        .when(r1 < 0.90, "EDUCATION")
        .otherwise("OTHER")
    )

    r2 = F.rand(SEED + 325)
    term_months = (
        F.when(r2 < 0.10, 12)
        .when(r2 < 0.30, 24)
        .when(r2 < 0.60, 36)
        .when(r2 < 0.85, 48)
        .otherwise(60)
    )

    r3 = F.rand(SEED + 326)
    repayment_freq = (
        F.when(r3 < 0.70, "MONTHLY")
        .when(r3 < 0.95, "FORTNIGHTLY")
        .otherwise("WEEKLY")
    )

    r4 = F.rand(SEED + 327)
    status = (
        F.when(r4 < 0.80, "ACTIVE")
        .when(r4 < 0.95, "CLOSED")
        .otherwise("DEFAULT")
    )

    start_date = F.date_add(F.current_date(), -(F.rand(SEED + 328) * 1825).cast("int")).cast("string")

    df = (
        base
        .withColumn("loanId", F.expr("uuid()"))
        .withColumn("loanType", loan_type)
        .withColumn("originalAmount", original_amount_udf(F.col("id")))
        .withColumn("currentBalance", current_balance_udf(F.col("originalAmount")))
        .withColumn("interestRate", loan_interest_rate_udf(F.col("id")))
        .withColumn("termMonths", term_months)
        .withColumn("repaymentAmount",
                    loan_repayment_udf(F.col("originalAmount"), F.col("interestRate"),
                                       F.col("termMonths").cast("string")))
        .withColumn("repaymentFrequency", repayment_freq)
        .withColumn("status", status)
        .withColumn("startDate", start_date)
        .withColumn("maturityDate",
                    maturity_date_udf(F.col("startDate"), F.col("termMonths").cast("string")))
        .select("global_id", "accountId", "loanId", "loanType", "originalAmount",
                "currentBalance", "interestRate", "termMonths", "repaymentAmount",
                "repaymentFrequency", "status", "startDate", "maturityDate")
    )
    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/loan_accounts")
    print(f"  loan_accounts: {N:,} rows")


# ---------------------------------------------------------------------------
# Credit cards  (SEED +340 .. +359)
# ---------------------------------------------------------------------------

def _generate_credit_cards(spark, cfg):
    N, SEED = cfg.N_CREDIT_CARDS, cfg.SEED

    # --- pandas UDFs ---

    @F.pandas_udf(StringType())
    def credit_limit_udf(ids):
        import numpy as np
        np.random.seed(SEED + 340)
        vals = np.random.lognormal(8.9, 0.4, size=len(ids))
        return ids.map(lambda i: f"{vals[int(i) % len(vals)]:.2f}")

    @F.pandas_udf(StringType())
    def current_balance_udf(credit_limits):
        import numpy as np
        np.random.seed(SEED + 341)
        utilization = np.random.beta(2, 5, size=len(credit_limits))
        results = []
        for cl, u in zip(credit_limits, utilization):
            results.append(f"{float(cl) * u:.2f}")
        return credit_limits.__class__(results)

    @F.pandas_udf(StringType())
    def min_payment_udf(balances):
        results = []
        for b in balances:
            val = max(25.0, float(b) * 0.02)
            results.append(f"{val:.2f}")
        return balances.__class__(results)

    @F.pandas_udf(StringType())
    def cc_interest_rate_udf(ids):
        import numpy as np
        np.random.seed(SEED + 342)
        rates = np.random.normal(19.5, 2.0, size=len(ids))
        return ids.map(lambda i: f"{max(10.0, rates[int(i) % len(rates)]):.2f}%")

    # --- build dataframe ---

    custs = _load_customers_idx(spark, cfg)
    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("cust_fk", (F.abs(F.hash(F.col("id"), F.lit(SEED + 343))) % cfg.N_CUSTOMERS).cast("int"))
    )
    base = (
        base.join(custs.select("cust_idx", "global_id"), base.cust_fk == custs.cust_idx, "left")
        .drop("cust_fk", "cust_idx")
    )

    card_number = F.concat(
        F.lit("XXXX-XXXX-XXXX-"),
        F.lpad((F.abs(F.hash(F.col("id"), F.lit(SEED + 344))) % 10000).cast("string"), 4, "0")
    )

    r1 = F.rand(SEED + 345)
    card_type = (
        F.when(r1 < 0.45, "VISA")
        .when(r1 < 0.85, "MASTERCARD")
        .otherwise("AMEX")
    )

    r2 = F.rand(SEED + 346)
    annual_fee = (
        F.when(r2 < 0.30, "0")
        .when(r2 < 0.55, "59")
        .when(r2 < 0.75, "99")
        .when(r2 < 0.90, "149")
        .otherwise("299")
    )

    r3 = F.rand(SEED + 347)
    reward_program = (
        F.when(r3 < 0.30, "NONE")
        .when(r3 < 0.70, "POINTS")
        .when(r3 < 0.90, "CASHBACK")
        .otherwise("TRAVEL")
    )

    r4 = F.rand(SEED + 348)
    status = (
        F.when(r4 < 0.85, "ACTIVE")
        .when(r4 < 0.90, "SUSPENDED")
        .otherwise("CLOSED")
    )

    total_days = (cfg.END_DATE - cfg.START_DATE).days
    issued_date = F.date_add(
        F.lit(cfg.START_DATE.strftime("%Y-%m-%d")),
        (F.rand(SEED + 349) * total_days).cast("int")
    ).cast("string")

    df = (
        base
        .withColumn("creditCardId", F.expr("uuid()"))
        .withColumn("cardNumber", card_number)
        .withColumn("cardType", card_type)
        .withColumn("creditLimit", credit_limit_udf(F.col("id")))
        .withColumn("currentBalance", current_balance_udf(F.col("creditLimit")))
        .withColumn("minPayment", min_payment_udf(F.col("currentBalance")))
        .withColumn("interestRate", cc_interest_rate_udf(F.col("id")))
        .withColumn("annualFee", annual_fee)
        .withColumn("rewardProgram", reward_program)
        .withColumn("status", status)
        .withColumn("issuedDate", issued_date)
        # ~35% of credit cards belong to a business customer.
        .withColumn("organisationId",
            F.when(F.rand(SEED + 513) < 0.35,
                   F.concat(F.lit("ORG-"),
                            F.lpad((F.abs(F.hash(F.col("id"), F.lit(SEED + 514)))
                                    % cfg.N_ORGANISATIONS).cast("string"), 5, "0")))
             .otherwise(F.lit(None).cast("string")))
        .select("global_id", "creditCardId", "cardNumber", "cardType", "creditLimit",
                "currentBalance", "minPayment", "interestRate", "annualFee",
                "rewardProgram", "status", "issuedDate", "organisationId")
    )
    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/credit_cards")
    print(f"  credit_cards: {N:,} rows")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(spark, cfg):
    print("=== Banking Products Extended generation ===")
    _generate_mortgage_accounts(spark, cfg)
    _generate_loan_accounts(spark, cfg)
    _generate_credit_cards(spark, cfg)
    print("=== Banking Products Extended done ===")


if __name__ == "__main__":
    import config as cfg  # noqa: F811
    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
