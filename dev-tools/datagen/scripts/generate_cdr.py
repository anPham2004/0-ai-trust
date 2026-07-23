"""
Generate CDR tables: customers, banking_accounts, banking_transactions.

All three tables share global_id (UUID) as the cross-dataset customer join key.
Run directly or call run(spark, cfg) from a pipeline orchestrator.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd
from cdr_pandas_udfs import (
    udf_first_name, udf_last_name, udf_email, udf_phone,
    udf_balance, udf_merchant, udf_tx_amount,
)

_spark: list = []

# ── Module-level pandas UDFs (CDR enrichment) ─────────────────────────────────

@F.pandas_udf(StringType())
def _udf_middle_names(ids: pd.Series) -> pd.Series:
    import numpy as np
    from faker import Faker
    fake = Faker("en_AU"); Faker.seed(420)
    rng = np.random.default_rng(420)
    results = []
    for _ in ids:
        r = rng.random()
        if r < 0.70:
            results.append("")
        elif r < 0.90:
            results.append(fake.first_name())
        else:
            results.append(f"{fake.first_name()},{fake.first_name()}")
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_occupation_code(ids: pd.Series) -> pd.Series:
    import numpy as np
    CODES = ["121211", "133111", "251513", "312211", "411411",
             "612111", "711211", "841511", "899900", "143111"]
    rng = np.random.default_rng(421)
    return pd.Series([rng.choice(CODES) for _ in ids])


@F.pandas_udf(StringType())
def _udf_phone_numbers_json(phones: pd.Series) -> pd.Series:
    import json, numpy as np
    from faker import Faker
    fake = Faker("en_AU"); Faker.seed(422)
    rng = np.random.default_rng(422)
    PURPOSES = ["MOBILE", "HOME", "WORK", "UNSPECIFIED"]
    results = []
    for ph in phones:
        n = 1 if rng.random() < 0.70 else 2
        entries = []
        for i in range(n):
            entries.append({
                "isPreferred": i == 0,
                "purpose": rng.choice(PURPOSES),
                "countryCode": "+61",
                "fullNumber": ph if i == 0 else fake.phone_number(),
            })
        results.append(json.dumps(entries))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_email_addresses_json(emails: pd.Series) -> pd.Series:
    import json, numpy as np
    PURPOSES = ["HOME", "WORK", "OTHER"]
    rng = np.random.default_rng(423)
    return pd.Series([
        json.dumps([{"isPreferred": True, "purpose": rng.choice(PURPOSES), "address": e}])
        for e in emails
    ])


@F.pandas_udf(StringType())
def _udf_account_nickname(account_types: pd.Series) -> pd.Series:
    import numpy as np
    rng = np.random.default_rng(430)
    NICKNAMES = {
        "savings":     ["My Savings", "Savings Pot", "Rainy Day Fund", "Emergency Fund"],
        "transaction": ["Everyday Account", "Daily Spending", "Bills Account"],
        "loan":        ["Home Loan", "Car Loan", "Personal Loan", "My Mortgage"],
    }
    return pd.Series([rng.choice(NICKNAMES.get(at, ["My Account"])) for at in account_types])


@F.pandas_udf(StringType())
def _udf_product_name(categories: pd.Series) -> pd.Series:
    import numpy as np
    rng = np.random.default_rng(431)
    NAMES = {
        "TRANS_AND_SAVINGS_ACCOUNTS": ["Everyday Plus", "Performance Saver", "Smart Saver"],
        "TERM_DEPOSITS":              ["12-Month Term Deposit", "6-Month Term Deposit"],
        "RESIDENTIAL_MORTGAGES":      ["Home Loan Variable", "Home Loan Fixed 3yr"],
        "CRED_AND_CHRG_CARDS":        ["Platinum Credit Card", "Rewards Mastercard"],
        "PERS_LOANS":                 ["Personal Loan Express", "Unsecured Personal Loan"],
    }
    return pd.Series([rng.choice(NAMES.get(c, ["BankCorp Standard Product"])) for c in categories])


@F.pandas_udf(StringType())
def _udf_reference(ids: pd.Series) -> pd.Series:
    import numpy as np
    rng = np.random.default_rng(440)
    results = []
    for _ in ids:
        results.append("" if rng.random() < 0.30 else str(int(rng.integers(100000000, 999999999))))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_mcc_code(categories: pd.Series) -> pd.Series:
    MCC = {
        "groceries": "5411", "utilities": "4900", "transport": "4111",
        "dining": "5812", "healthcare": "8011", "entertainment": "7999",
        "shopping": "5311", "other": "5999",
    }
    return pd.Series([MCC.get(c, "5999") for c in categories])


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ts_col(start_ts: float, total_sec: float) -> F.Column:
    """Random ISO datetime string within [start, start+total_sec)."""
    return F.from_unixtime(
        (F.lit(start_ts) + F.rand() * F.lit(total_sec)).cast("long")
    )


def _attach_customer_fk(df, tmp_table: str, n_customers: int, seed: int):
    """Hash-distribute rows to customer pool; join global_id + customerId."""
    lookup = (
        _spark[0].read.parquet(tmp_table)
        .select("global_id", "customerId")
        .withColumn(
            "cust_idx",
            (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int"),
        )
    )
    return (
        df
        .withColumn("cust_idx", (F.abs(F.hash(F.col("id"), F.lit(seed))) % n_customers).cast("int"))
        .join(lookup, on="cust_idx", how="left")
        .drop("cust_idx")
    )


# ── Table generators ──────────────────────────────────────────────────────────

def _generate_customers(spark, cfg) -> str:
    n, seed = cfg.N_CUSTOMERS, cfg.SEED
    start_ts = cfg.START_DATE.timestamp()
    create_window = (cfg.END_DATE - cfg.START_DATE).total_seconds() - 180 * 86400
    ANZSCO_VERS = [
        "ANZSCO_1220.0_2006_V1.0", "ANZSCO_1220.0_2006_V1.1",
        "ANZSCO_1220.0_2013_V1.2", "ANZSCO_1220.0_2013_V1.3",
    ]

    df = (
        spark.range(0, n, numPartitions=cfg.get_partitions(n))
        .select(
            F.expr("uuid()").alias("global_id"),
            F.concat(F.lit("CUST-"), F.lpad(F.col("id").cast("string"), 5, "0")).alias("customerId"),
            udf_first_name(F.col("id")).alias("firstName"),
            udf_last_name(F.col("id")).alias("lastName"),
            udf_email(F.col("id")).alias("email"),
            udf_phone(F.col("id")).alias("phoneNumber"),
            F.when(F.rand(seed) < 0.50, "Male")
             .when(F.rand(seed) < 0.98, "Female")
             .otherwise("Non_binary").alias("gender"),
            F.when((F.abs(F.randn(seed)) * 12 + 42).cast("int") < 18, 18)
             .when((F.abs(F.randn(seed)) * 12 + 42).cast("int") > 80, 80)
             .otherwise((F.abs(F.randn(seed)) * 12 + 42).cast("int")).alias("age"),
            F.when(F.rand(seed) < 0.32, "NSW").when(F.rand(seed) < 0.58, "VIC")
             .when(F.rand(seed) < 0.78, "QLD").when(F.rand(seed) < 0.90, "WA")
             .when(F.rand(seed) < 0.96, "SA").when(F.rand(seed) < 0.98, "TAS")
             .when(F.rand(seed) < 0.99, "ACT").otherwise("NT").alias("state"),
            _ts_col(start_ts, create_window).alias("_c"),
        )
        .withColumn("createdAt", F.col("_c"))
        .withColumn("lastUpdateTime",
                    F.from_unixtime(F.unix_timestamp("_c") + (F.rand() * 180 * 86400).cast("long")))
        .drop("_c")
    )

    # CDR CommonPerson enrichment
    df = df.withColumn("middleNames", _udf_middle_names(F.monotonically_increasing_id()))
    df = df.withColumn(
        "prefix",
        F.when(F.col("gender") == "Male",
               F.when(F.rand(seed + 450) < 0.92, "Mr.").otherwise("Dr."))
         .when(F.col("gender") == "Female",
               F.when(F.rand(seed + 451) < 0.30, "Mrs.")
                .when(F.rand(seed + 451) < 0.60, "Ms.")
                .when(F.rand(seed + 451) < 0.88, "Miss")
                .otherwise("Dr."))
         .otherwise("Mx.")
    )
    df = df.withColumn(
        "suffix",
        F.when(F.rand(seed + 452) < 0.05, "Jr.").when(F.rand(seed + 452) < 0.09, "Sr.")
         .when(F.rand(seed + 452) < 0.12, "PhD").when(F.rand(seed + 452) < 0.13, "II")
         .when(F.rand(seed + 452) < 0.14, "III").otherwise(F.lit(None).cast("string"))
    )
    df = df.withColumn("occupationCode", _udf_occupation_code(F.monotonically_increasing_id()))
    df = df.withColumn(
        "occupationCodeVersion",
        F.when(F.rand(seed + 453) < 0.30, ANZSCO_VERS[0])
         .when(F.rand(seed + 453) < 0.55, ANZSCO_VERS[1])
         .when(F.rand(seed + 453) < 0.80, ANZSCO_VERS[2])
         .otherwise(ANZSCO_VERS[3])
    )
    df = df.withColumn("phoneNumbers", _udf_phone_numbers_json(F.col("phoneNumber")))
    df = df.withColumn("emailAddresses", _udf_email_addresses_json(F.col("email")))

    # Customer type: how the bank classifies this customer at onboarding.
    # INDIVIDUAL = personal/retail, BUSINESS = represents a registered company,
    # SOLE_TRADER = operates a business under own name (no separate legal entity).
    df = df.withColumn(
        "customerType",
        F.when(F.rand(seed + 454) < 0.55, "INDIVIDUAL")
         .when(F.rand(seed + 454) < 0.90, "BUSINESS")
         .otherwise("SOLE_TRADER")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/customers")
    print(f"  customers: {n:,} rows → {cfg.OUTPUT_PATH}/customers")

    tmp = cfg.tmp_path("customers_fk")
    df.select("global_id", "customerId").write.mode("overwrite").parquet(tmp)
    return tmp


def _generate_accounts(spark, cfg, customers_tmp: str) -> str:
    n, seed = cfg.N_ACCOUNTS, cfg.SEED
    start_ts = cfg.START_DATE.timestamp()
    total_sec = (cfg.END_DATE - cfg.START_DATE).total_seconds()

    df = (
        spark.range(0, n, numPartitions=cfg.get_partitions(n))
        .select(
            F.col("id"),
            F.expr("uuid()").alias("accountId"),
            F.when(F.rand(seed) < 0.55, "savings").when(F.rand(seed) < 0.90, "transaction")
             .otherwise("loan").alias("accountType"),
            (F.rand(seed) < 0.95).cast("boolean").alias("isOwner"),
            _ts_col(start_ts, total_sec).alias("openDate"),
            (F.rand(seed) < 0.88).cast("boolean").alias("isActive"),
        )
    )
    df = _attach_customer_fk(df, customers_tmp, cfg.N_CUSTOMERS, seed)
    df = df.withColumn("balance", udf_balance(F.col("accountType")))

    # CDR BankingAccount enrichment
    df = df.withColumn("creationDate", F.col("openDate"))
    df = df.withColumn("isOwned", F.col("isOwner"))
    df = df.withColumn("openStatus", F.when(F.col("isActive"), "OPEN").otherwise("CLOSED"))
    df = df.withColumn(
        "productCategory",
        F.when(F.col("accountType").isin("savings", "transaction"), "TRANS_AND_SAVINGS_ACCOUNTS")
         .when(F.rand(seed + 460) < 0.40, "RESIDENTIAL_MORTGAGES")
         .when(F.rand(seed + 460) < 0.70, "PERS_LOANS")
         .otherwise("CRED_AND_CHRG_CARDS")
    )
    df = df.withColumn("productName", _udf_product_name(F.col("productCategory")))
    df = df.withColumn(
        "displayName",
        F.concat(F.lit("Account-"), F.upper(F.col("accountType").substr(1, 3)),
                 F.lit("-"), F.lpad((F.abs(F.hash(F.col("accountId"))) % 10000).cast("string"), 4, "0"))
    )
    df = df.withColumn("nickname", _udf_account_nickname(F.col("accountType")))
    df = df.withColumn(
        "maskedNumber",
        F.concat(F.lit("xxxxx"),
                 F.lpad((F.abs(F.hash(F.col("accountId"))) % 10000).cast("string"), 4, "0"))
    )
    df = df.withColumn(
        "accountNumber",
        F.lpad((F.abs(F.hash(F.col("accountId"))) % 100000000).cast("string"), 8, "0")
    )
    df = df.withColumn(
        "accountOwnership",
        F.when(F.rand(seed + 461) < 0.80, "ONE_PARTY")
         .when(F.rand(seed + 461) < 0.95, "TWO_PARTY")
         .otherwise("MANY_PARTY")
    )

    # ~35% of accounts are business accounts linked to an organisation.
    # ORG IDs are deterministic (ORG-00000..ORG-{N_ORGANISATIONS-1}),
    # matching the pattern in generate_cdr_extended._generate_organisations.
    df = df.withColumn(
        "organisationId",
        F.when(F.rand(seed + 462) < 0.35,
               F.concat(F.lit("ORG-"),
                        F.lpad((F.abs(F.hash(F.col("accountId"), F.lit(seed + 463)))
                                % cfg.N_ORGANISATIONS).cast("string"), 5, "0")))
         .otherwise(F.lit(None).cast("string"))
    )

    df = df.select(
        "global_id", "accountId", "customerId", "organisationId",
        "accountType", "balance", "isOwner", "openDate", "isActive",
        "displayName", "nickname", "maskedNumber", "accountNumber",
        "productCategory", "productName", "accountOwnership",
        "openStatus", "isOwned", "creationDate",
    )
    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/banking_accounts")
    print(f"  banking_accounts: {n:,} rows → {cfg.OUTPUT_PATH}/banking_accounts")

    tmp = cfg.tmp_path("accounts_fk")
    df.select("global_id", "accountId", "organisationId").write.mode("overwrite").parquet(tmp)
    return tmp


def _generate_transactions(spark, cfg, customers_tmp: str, accounts_tmp: str) -> None:
    n, seed = cfg.N_TRANSACTIONS, cfg.SEED
    start_ts = cfg.START_DATE.timestamp()
    total_sec = (cfg.END_DATE - cfg.START_DATE).total_seconds()

    acc_lookup = (
        spark.read.parquet(accounts_tmp)
        .withColumn(
            "acc_idx",
            (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int"),
        )
    )

    df = (
        spark.range(0, n, numPartitions=cfg.get_partitions(n))
        .select(
            F.col("id"),
            F.expr("uuid()").alias("transactionId"),
            (F.abs(F.hash(F.col("id"), F.lit(seed))) % cfg.N_ACCOUNTS).cast("int").alias("acc_idx"),
            F.when(F.rand(seed) < 0.60, "debit").otherwise("credit").alias("transactionType"),
            _ts_col(start_ts, total_sec).alias("_ts"),
            udf_merchant(F.col("id")).alias("description"),
            F.when(F.rand(seed) < 0.20, "groceries").when(F.rand(seed) < 0.35, "utilities")
             .when(F.rand(seed) < 0.47, "transport").when(F.rand(seed) < 0.57, "dining")
             .when(F.rand(seed) < 0.65, "healthcare").when(F.rand(seed) < 0.72, "entertainment")
             .when(F.rand(seed) < 0.87, "shopping").otherwise("other").alias("category"),
        )
        .withColumn("_h", F.hour("_ts"))
        .withColumn(
            "timestamp",
            F.when(
                F.rand() < F.when(F.col("_h").between(9, 17), F.lit(1.0)).otherwise(F.lit(1.0 / 3)),
                F.col("_ts"),
            ).otherwise(
                F.from_unixtime(
                    F.unix_timestamp("_ts") - F.col("_h") * 3600
                    + (F.lit(9) + (F.rand() * 8).cast("int")) * 3600
                )
            ),
        )
        .drop("_ts", "_h")
        .join(acc_lookup, on="acc_idx", how="left").drop("acc_idx")
        .withColumn("amount", udf_tx_amount(F.col("transactionType")))
    )

    # CDR BankingTransaction enrichment
    df = df.withColumn(
        "type",
        F.when(F.col("transactionType") == "debit",
               F.when(F.rand(seed + 470) < 0.50, "PAYMENT")
                .when(F.rand(seed + 470) < 0.75, "DIRECT_DEBIT")
                .when(F.rand(seed + 470) < 0.90, "FEE")
                .otherwise("TRANSFER_OUTGOING"))
         .when(F.col("transactionType") == "credit",
               F.when(F.rand(seed + 471) < 0.55, "TRANSFER_INCOMING")
                .when(F.rand(seed + 471) < 0.85, "INTEREST_PAID")
                .otherwise("OTHER"))
         .otherwise("OTHER")
    )
    df = df.withColumn(
        "status",
        F.when(F.rand(seed + 472) < 0.92, "POSTED").otherwise("PENDING")
    )
    df = df.withColumn("reference", _udf_reference(F.col("id")))
    df = df.withColumn("isDetailAvailable", (F.rand(seed + 473) < 0.70).cast("boolean"))
    df = df.withColumn(
        "postingDateTime",
        F.from_unixtime(F.unix_timestamp("timestamp") + (F.rand(seed + 474) * 3600).cast("long"))
    )
    df = df.withColumn("valueDateTime", F.col("timestamp"))
    df = df.withColumn("executionDateTime", F.col("timestamp"))
    df = df.withColumn(
        "currency",
        F.when(F.rand(seed + 475) < 0.88, "AUD").when(F.rand(seed + 475) < 0.93, "USD")
         .when(F.rand(seed + 475) < 0.96, "GBP").when(F.rand(seed + 475) < 0.98, "EUR")
         .otherwise("JPY")
    )
    df = df.withColumn(
        "merchantName",
        F.when(F.col("type").isin("PAYMENT", "DIRECT_DEBIT"), F.col("description"))
         .otherwise(F.lit(None).cast("string"))
    )
    df = df.withColumn("merchantCategoryCode", _udf_mcc_code(F.col("category")))
    # BPAY fields for 5% of transactions
    df = df.withColumn(
        "billerCode",
        F.when(F.rand(seed + 476) < 0.05,
               F.lpad((F.abs(F.hash(F.col("id"), F.lit(seed + 477))) % 9000 + 1000).cast("string"), 4, "0"))
         .otherwise(F.lit(None).cast("string"))
    )
    df = df.withColumn(
        "billerName",
        F.when(F.col("billerCode").isNotNull(), F.col("description"))
         .otherwise(F.lit(None).cast("string"))
    )
    df = df.withColumn(
        "crn",
        F.when(F.col("billerCode").isNotNull(),
               F.lpad((F.abs(F.hash(F.col("id"))) % 1000000000).cast("string"), 9, "0"))
         .otherwise(F.lit(None).cast("string"))
    )
    df = df.withColumn(
        "apcaNumber",
        F.lpad((F.abs(F.hash(F.col("id"), F.lit(seed + 478))) % 1000000).cast("string"), 6, "0")
    )

    df = df.select(
        "global_id", "transactionId", "accountId",
        "amount", "transactionType", "type", "timestamp",
        "description", "category", "status",
        "reference", "isDetailAvailable",
        "postingDateTime", "valueDateTime", "executionDateTime",
        "currency", "merchantName", "merchantCategoryCode",
        "billerCode", "billerName", "crn", "apcaNumber",
    )
    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/banking_transactions")
    print(f"  banking_transactions: {n:,} rows → {cfg.OUTPUT_PATH}/banking_transactions")


# ── Public entry point ─────────────────────────────────────────────────────────

def run(spark, cfg) -> None:
    """Generate CDR tables: customers, banking_accounts, banking_transactions."""
    _spark.clear()
    _spark.append(spark)
    print("Generating CDR tables...")
    customers_tmp = _generate_customers(spark, cfg)
    accounts_tmp  = _generate_accounts(spark, cfg, customers_tmp)
    _generate_transactions(spark, cfg, customers_tmp, accounts_tmp)
    print("CDR generation complete.")


if __name__ == "__main__":
    import config as cfg
    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
