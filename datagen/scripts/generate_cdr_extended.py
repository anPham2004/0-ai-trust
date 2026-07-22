"""
Generate CDR extended tables: organisations, physical_addresses,
banking_products, banking_balances, banking_direct_debits,
banking_payees, banking_scheduled_payments.

Reads global_id pools from _tmp_customers and _tmp_accounts Parquet.
SEED offsets: +100 to +199 (unique per column).
Run directly or call run(spark, cfg) from a pipeline orchestrator.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd


# ── FK helpers ───────────────────────────────────────────────────────────────

def _load_customers_idx(spark, cfg):
    return spark.read.parquet(cfg.tmp_path("customers_fk")).withColumn(
        "cust_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


def _load_accounts_idx(spark, cfg):
    return spark.read.parquet(cfg.tmp_path("accounts_fk")).withColumn(
        "acc_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


# ── Table generators ────────────────────────────────────────────────────────

def _generate_organisations(spark, cfg):
    N, SEED = cfg.N_ORGANISATIONS, cfg.SEED

    @F.pandas_udf(StringType())
    def udf_abn(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 100)
        return ids.map(lambda _: fake.numerify("## ### ### ###"))

    @F.pandas_udf(StringType())
    def udf_acn(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 101)
        return ids.map(lambda _: fake.numerify("### ### ###"))

    @F.pandas_udf(StringType())
    def udf_business_name(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 102)
        return ids.map(lambda _: fake.company())

    @F.pandas_udf(StringType())
    def udf_legal_name(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 103)
        return ids.map(lambda _: fake.company() + " Pty Ltd")

    @F.pandas_udf(StringType())
    def udf_industry_code(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 104)
        return ids.map(lambda _: fake.numerify("####"))

    @F.pandas_udf(StringType())
    def udf_short_name(ids):
        from faker import Faker
        import numpy as np
        fake = Faker("en_AU")
        Faker.seed(SEED + 105)
        rng = np.random.default_rng(SEED + 105)
        results = []
        for _ in ids:
            w = fake.company().split()
            results.append(" ".join(w[:2]) if len(w) >= 2 else w[0])
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def udf_agent_first_name(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 106)
        return ids.map(lambda _: fake.first_name())

    @F.pandas_udf(StringType())
    def udf_agent_last_name(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 107)
        return ids.map(lambda _: fake.last_name())

    cust = _load_customers_idx(spark, cfg)
    secs = int((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_epoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("cust_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 108))) % cfg.N_CUSTOMERS).cast("int"))
    )
    base = (
        base.join(cust.select("cust_idx", "global_id"), on="cust_idx", how="left")
        .drop("cust_idx")
    )

    df = (
        base
        .withColumn("organisationId", F.concat(F.lit("ORG-"), F.lpad(F.col("id").cast("string"), 5, "0")))
        .withColumn("abn", udf_abn(F.col("id")))
        .withColumn("acn", udf_acn(F.col("id")))
        .withColumn("businessName", udf_business_name(F.col("id")))
        .withColumn("legalName", udf_legal_name(F.col("id")))
        .withColumn("shortName", udf_short_name(F.col("id")))
        .withColumn("organisationType",
                    F.when(F.rand(SEED + 109) < 0.60, "COMPANY")
                     .when(F.rand(SEED + 109) < 0.75, "TRUST")
                     .when(F.rand(SEED + 109) < 0.85, "PARTNERSHIP")
                     .when(F.rand(SEED + 109) < 0.95, "SOLE_TRADER")
                     .otherwise("OTHER"))
        .withColumn("industryCode", udf_industry_code(F.col("id")))
        .withColumn("industryCodeVersion",
                    F.when(F.rand(SEED + 110) < 0.70, "ANZSIC_2006").otherwise("ANZSIC_1993"))
        .withColumn("establishmentDate",
                    F.date_add(F.lit("1986-01-01"), (F.rand(SEED + 111) * 14600).cast("int")).cast("string"))
        .withColumn("lastUpdateTime",
                    F.date_format((start_epoch + (F.rand(SEED + 112) * secs).cast("long")).cast("timestamp"),
                                  "yyyy-MM-dd HH:mm:ss"))
        .withColumn("registeredCountry", F.lit("AUS"))
        .withColumn("isACNCRegistered", (F.rand(SEED + 113) < 0.15).cast("boolean"))
        .withColumn("agentFirstName", udf_agent_first_name(F.col("id")))
        .withColumn("agentLastName", udf_agent_last_name(F.col("id")))
        .withColumn("agentRole",
                    F.when(F.rand(SEED + 114) < 0.40, "PRINCIPAL")
                     .when(F.rand(SEED + 114) < 0.70, "DIRECTOR")
                     .when(F.rand(SEED + 114) < 0.85, "OWNER")
                     .when(F.rand(SEED + 114) < 0.95, "PARTNER")
                     .otherwise("OTHER"))
        .select("global_id", "organisationId", "abn", "acn", "businessName", "legalName",
                "shortName", "organisationType", "industryCode", "industryCodeVersion",
                "establishmentDate", "lastUpdateTime", "registeredCountry",
                "isACNCRegistered", "agentFirstName", "agentLastName", "agentRole")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/organisations")
    print(f"  organisations: {N:,} rows")


def _generate_addresses(spark, cfg):
    N, SEED = cfg.N_ADDRESSES, cfg.SEED

    @F.pandas_udf(StringType())
    def udf_address_line(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 120)
        return ids.map(lambda _: fake.street_address())

    @F.pandas_udf(StringType())
    def udf_suburb(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 121)
        return ids.map(lambda _: fake.city())

    @F.pandas_udf(StringType())
    def udf_postcode(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 122)
        return ids.map(lambda _: fake.postcode())

    @F.pandas_udf(StringType())
    def udf_simple_json(ids: pd.Series) -> pd.Series:
        """Simple address format as CDR SimpleAddress JSON."""
        import json
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 123)
        results = []
        for _ in ids:
            results.append(json.dumps({
                "mailingName": fake.name(),
                "addressLine1": fake.street_address(),
                "suburb": fake.city(),
                "state": fake.state_abbr(),
                "postcode": fake.postcode(),
                "country": "AUS",
            }))
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def udf_paf_json(ids: pd.Series) -> pd.Series:
        """Australia Post PAF address JSON — only for paf addressUType rows."""
        import json
        import numpy as np
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 124)
        rng = np.random.default_rng(SEED + 124)
        results = []
        for _ in ids:
            results.append(json.dumps({
                "dpid": fake.numerify("########"),
                "thoroughfareNumber1": int(rng.integers(1, 300)),
                "thoroughfareName1": fake.street_name().split()[0],
                "thoroughfareType1": fake.street_suffix(),
                "localityName": fake.city(),
                "postcode": fake.postcode(),
                "state": fake.state_abbr(),
            }))
        return pd.Series(results)

    cust = _load_customers_idx(spark, cfg)

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("cust_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 125))) % cfg.N_CUSTOMERS).cast("int"))
    )
    base = (
        base.join(cust.select("cust_idx", "global_id"), on="cust_idx", how="left")
        .drop("cust_idx")
    )

    # addressUType: 80% simple, 20% paf
    df = (
        base
        .withColumn("addressId", F.expr("uuid()"))
        .withColumn("purpose",
                    F.when(F.rand(SEED + 126) < 0.40, "REGISTERED")
                     .when(F.rand(SEED + 126) < 0.70, "MAIL")
                     .when(F.rand(SEED + 126) < 0.90, "WORK")
                     .otherwise("OTHER"))
        .withColumn("addressUType",
                    F.when(F.rand(SEED + 127) < 0.80, "simple").otherwise("paf"))
        # Legacy flat fields for backward compat
        .withColumn("addressLine1", udf_address_line(F.col("id")))
        .withColumn("suburb", udf_suburb(F.col("id")))
        .withColumn("state",
                    F.when(F.rand(SEED + 128) < 0.32, "NSW")
                     .when(F.rand(SEED + 128) < 0.58, "VIC")
                     .when(F.rand(SEED + 128) < 0.78, "QLD")
                     .when(F.rand(SEED + 128) < 0.90, "WA")
                     .when(F.rand(SEED + 128) < 0.96, "SA")
                     .when(F.rand(SEED + 128) < 0.98, "TAS")
                     .when(F.rand(SEED + 128) < 0.99, "ACT")
                     .otherwise("NT"))
        .withColumn("postcode", udf_postcode(F.col("id")))
        .withColumn("country",
                    F.when(F.rand(SEED + 129) < 0.98, "AU").otherwise("NZ"))
        # CDR nested address objects
        .withColumn("simple",
                    F.when(F.col("addressUType") == "simple", udf_simple_json(F.col("id")))
                     .otherwise(F.lit(None).cast("string")))
        .withColumn("paf",
                    F.when(F.col("addressUType") == "paf", udf_paf_json(F.col("id")))
                     .otherwise(F.lit(None).cast("string")))
        .select("global_id", "addressId", "purpose", "addressUType",
                "addressLine1", "suburb", "state", "postcode", "country",
                "simple", "paf")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/physical_addresses")
    print(f"  physical_addresses: {N:,} rows")


def _generate_banking_products(spark, cfg):
    N, SEED = cfg.N_BANKING_PRODUCTS, cfg.SEED
    secs = int((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_epoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))

    @F.pandas_udf(StringType())
    def udf_description(ids: pd.Series) -> pd.Series:
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 130)
        return ids.map(lambda _: fake.paragraph(nb_sentences=2))

    @F.pandas_udf(StringType())
    def udf_additional_info_json(ids: pd.Series) -> pd.Series:
        """CDR BankingProductAdditionalInformation JSON."""
        import json
        results = []
        for i in ids:
            results.append(json.dumps({
                "overviewUri": f"https://www.bankcorp.com.au/products/prod-{int(i):03d}/overview",
                "termsUri": f"https://www.bankcorp.com.au/products/prod-{int(i):03d}/terms",
                "eligibilityUri": f"https://www.bankcorp.com.au/products/prod-{int(i):03d}/eligibility",
                "feesAndPricingUri": f"https://www.bankcorp.com.au/products/prod-{int(i):03d}/fees",
                "bundleUri": None,
            }))
        return pd.Series(results)

    df = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("productId", F.concat(F.lit("PROD-"), F.lpad(F.col("id").cast("string"), 3, "0")))
        .withColumn("name",
                    F.when(F.rand(SEED + 131) < 0.20, "Everyday Transaction")
                     .when(F.rand(SEED + 131) < 0.35, "Online Saver")
                     .when(F.rand(SEED + 131) < 0.45, "Term Deposit 12M")
                     .when(F.rand(SEED + 131) < 0.55, "Home Loan Variable")
                     .when(F.rand(SEED + 131) < 0.65, "Home Loan Fixed")
                     .when(F.rand(SEED + 131) < 0.73, "Personal Loan")
                     .when(F.rand(SEED + 131) < 0.81, "Credit Card Platinum")
                     .when(F.rand(SEED + 131) < 0.88, "Credit Card Basic")
                     .when(F.rand(SEED + 131) < 0.93, "Car Loan")
                     .when(F.rand(SEED + 131) < 0.97, "Travel Card")
                     .otherwise("Business Loan"))
        .withColumn("description", udf_description(F.col("id")))
        .withColumn("productCategory",
                    F.when(F.rand(SEED + 132) < 0.30, "TRANS_AND_SAVINGS_ACCOUNTS")
                     .when(F.rand(SEED + 132) < 0.45, "TERM_DEPOSITS")
                     .when(F.rand(SEED + 132) < 0.60, "RESIDENTIAL_MORTGAGES")
                     .when(F.rand(SEED + 132) < 0.75, "CRED_AND_CHRG_CARDS")
                     .when(F.rand(SEED + 132) < 0.85, "PERS_LOANS")
                     .when(F.rand(SEED + 132) < 0.90, "TRAVEL_CARDS")
                     .when(F.rand(SEED + 132) < 0.95, "REGULATED_TRUST_ACCOUNTS")
                     .when(F.rand(SEED + 132) < 0.98, "MARGIN_LOANS")
                     .otherwise("LEASES"))
        .withColumn("brand", F.lit("BankCorp"))
        .withColumn("brandName", F.lit("BankCorp Australia"))
        .withColumn("applicationUri",
                    F.concat(F.lit("https://www.bankcorp.com.au/apply/"),
                             F.col("id").cast("string")))
        .withColumn("isTailored", (F.rand(SEED + 133) < 0.10).cast("boolean"))
        .withColumn("effectiveFrom",
                    F.date_add(F.lit("2020-01-01"), (F.rand(SEED + 134) * 1800).cast("int")).cast("string"))
        # effectiveTo: null for 70%, otherwise effectiveFrom + 2-5 years
        .withColumn("effectiveTo",
                    F.when(F.rand(SEED + 135) < 0.70, F.lit(None).cast("string"))
                     .otherwise(F.date_add(
                         F.lit("2020-01-01"),
                         (F.rand(SEED + 135) * 1800 + 730).cast("int")
                     ).cast("string")))
        .withColumn("lastUpdated",
                    F.date_format((start_epoch + (F.rand(SEED + 136) * secs).cast("long")).cast("timestamp"),
                                  "yyyy-MM-dd HH:mm:ss"))
        .withColumn("additionalInformation", udf_additional_info_json(F.col("id")))
        # cardArt: null for non-card products (simplified: 25% have card art)
        .withColumn("cardArt",
                    F.when(F.rand(SEED + 137) < 0.25,
                           F.concat(F.lit('[{"title":"Card Image","imageUri":"https://www.bankcorp.com.au/cards/art/'),
                                    F.col("id").cast("string"),
                                    F.lit('.png"}')))
                     .otherwise(F.lit(None).cast("string")))
        .select("productId", "name", "description", "productCategory", "brand", "brandName",
                "applicationUri", "isTailored", "effectiveFrom", "effectiveTo",
                "lastUpdated", "additionalInformation", "cardArt")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/banking_products")
    print(f"  banking_products: {N:,} rows")


def _generate_balances(spark, cfg):
    N, SEED = cfg.N_BANKING_BALANCES, cfg.SEED

    @F.pandas_udf(StringType())
    def udf_current_balance(ids):
        import numpy as np
        rng = np.random.default_rng(SEED + 140)
        vals = rng.lognormal(9.8, 0.8, size=len(ids))
        return ids.map(lambda i: f"{vals[int(i) % len(vals)]:.2f}")

    @F.pandas_udf(StringType())
    def udf_available_balance(balances):
        import numpy as np
        rng = np.random.default_rng(SEED + 141)
        factors = rng.uniform(0.8, 1.0, size=len(balances))
        return balances.map(lambda b: f"{float(b) * factors[0]:.2f}" if b else "0.00")

    @F.pandas_udf(StringType())
    def udf_credit_limit(ids):
        import numpy as np
        rng = np.random.default_rng(SEED + 142)
        results = []
        for _ in range(len(ids)):
            if rng.random() < 0.20:
                results.append(f"{rng.lognormal(9.2, 0.5):.2f}")
            else:
                results.append(None)
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def udf_amortised_limit(ids):
        """Amortised credit limit — only relevant for credit/revolving accounts (~10%)."""
        import numpy as np
        rng = np.random.default_rng(SEED + 143)
        results = []
        for _ in range(len(ids)):
            if rng.random() < 0.10:
                results.append(f"{rng.lognormal(9.0, 0.4):.2f}")
            else:
                results.append(None)
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def udf_purses_json(ids: pd.Series) -> pd.Series:
        """Multi-currency purses — only for travel/foreign currency accounts (~5%)."""
        import json
        import numpy as np
        rng = np.random.default_rng(SEED + 144)
        currencies = ["USD", "EUR", "GBP", "JPY", "SGD"]
        results = []
        for _ in ids:
            if rng.random() < 0.05:
                chosen = rng.choice(currencies, size=int(rng.integers(1, 3)), replace=False)
                purses = [{"amount": f"{rng.lognormal(6.0, 0.5):.2f}", "currency": c} for c in chosen]
                results.append(json.dumps(purses))
            else:
                results.append(None)
        return pd.Series(results)

    accts = _load_accounts_idx(spark, cfg)

    base = (
        spark.range(1, N + 1)
        .withColumnRenamed("id", "_row_idx")
    )
    accts_indexed = accts.withColumn(
        "_row_idx",
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    )

    joined = (
        base.join(accts_indexed.select("_row_idx", "global_id", "accountId"), on="_row_idx", how="left")
        .drop("_row_idx")
    )

    df = (
        joined
        .withColumn("_idx", F.monotonically_increasing_id())
        .withColumn("currentBalance", udf_current_balance(F.col("_idx")))
        .withColumn("availableBalance", udf_available_balance(F.col("currentBalance")))
        .withColumn("creditLimit", udf_credit_limit(F.col("_idx")))
        .withColumn("amortisedLimit", udf_amortised_limit(F.col("_idx")))
        .withColumn("currency", F.lit("AUD"))
        .withColumn("purses", udf_purses_json(F.col("_idx")))
        .select("global_id", "accountId", "currentBalance", "availableBalance",
                "creditLimit", "amortisedLimit", "currency", "purses")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/banking_balances")
    print(f"  banking_balances: {N:,} rows")


def _generate_direct_debits(spark, cfg):
    N, SEED = cfg.N_DIRECT_DEBITS, cfg.SEED
    secs = int((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_epoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))

    @F.pandas_udf(StringType())
    def udf_authorised_entity(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 150)
        return ids.map(lambda _: fake.company())

    @F.pandas_udf(StringType())
    def udf_debit_amount(ids):
        import numpy as np
        rng = np.random.default_rng(SEED + 151)
        vals = rng.lognormal(5.0, 0.8, size=len(ids))
        return ids.map(lambda i: f"{vals[int(i) % len(vals)]:.2f}")

    accts = _load_accounts_idx(spark, cfg)

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("acc_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 152))) % cfg.N_ACCOUNTS).cast("int"))
    )
    base = (
        base.join(accts.select("acc_idx", "global_id", "accountId"), on="acc_idx", how="left")
        .drop("acc_idx")
    )

    df = (
        base
        .withColumn("directDebitId", F.expr("uuid()"))
        .withColumn("authorisedEntity", udf_authorised_entity(F.col("id")))
        .withColumn("lastDebitAmount", udf_debit_amount(F.col("id")))
        .withColumn("lastDebitDateTime",
                    F.date_format((start_epoch + (F.rand(SEED + 153) * secs).cast("long")).cast("timestamp"),
                                  "yyyy-MM-dd HH:mm:ss"))
        .select("global_id", "accountId", "directDebitId", "authorisedEntity",
                "lastDebitAmount", "lastDebitDateTime")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/banking_direct_debits")
    print(f"  banking_direct_debits: {N:,} rows")


def _generate_payees(spark, cfg):
    N, SEED = cfg.N_PAYEES, cfg.SEED
    secs = int((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_epoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))

    @F.pandas_udf(StringType())
    def udf_nickname(ids):
        from faker import Faker
        import numpy as np
        fake = Faker("en_AU")
        Faker.seed(SEED + 160)
        rng = np.random.default_rng(SEED + 161)
        results = []
        for _ in range(len(ids)):
            if rng.random() < 0.50:
                results.append(fake.first_name())
            else:
                results.append(fake.company())
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def udf_description(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 162)
        return ids.map(lambda _: fake.sentence(nb_words=4))

    cust = _load_customers_idx(spark, cfg)

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("cust_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 163))) % cfg.N_CUSTOMERS).cast("int"))
    )
    base = (
        base.join(cust.select("cust_idx", "global_id"), on="cust_idx", how="left")
        .drop("cust_idx")
    )

    df = (
        base
        .withColumn("payeeId", F.expr("uuid()"))
        .withColumn("nickname", udf_nickname(F.col("id")))
        .withColumn("type",
                    F.when(F.rand(SEED + 164) < 0.60, "DOMESTIC")
                     .when(F.rand(SEED + 164) < 0.85, "BILLER")
                     .otherwise("INTERNATIONAL"))
        .withColumn("description", udf_description(F.col("id")))
        .withColumn("creationDate",
                    F.date_format((start_epoch + (F.rand(SEED + 165) * secs).cast("long")).cast("timestamp"),
                                  "yyyy-MM-dd").cast("string"))
        .select("global_id", "payeeId", "nickname", "type", "description", "creationDate")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/banking_payees")
    print(f"  banking_payees: {N:,} rows")


def _generate_scheduled_payments(spark, cfg):
    N, SEED = cfg.N_SCHEDULED_PAYMENTS, cfg.SEED

    @F.pandas_udf(StringType())
    def udf_sp_nickname(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 170)
        return ids.map(lambda _: fake.sentence(nb_words=3))

    @F.pandas_udf(StringType())
    def udf_sp_amount(ids):
        import numpy as np
        rng = np.random.default_rng(SEED + 171)
        vals = rng.lognormal(6.2, 0.7, size=len(ids))
        return ids.map(lambda i: f"{vals[int(i) % len(vals)]:.2f}")

    @F.pandas_udf(StringType())
    def udf_payer_reference(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 172)
        return ids.map(lambda _: fake.bothify("REF-????-####").upper())

    @F.pandas_udf(StringType())
    def udf_payee_reference(ids):
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 173)
        return ids.map(lambda _: fake.bothify("PAY-????-####").upper())

    @F.pandas_udf(StringType())
    def udf_from_json(ids: pd.Series) -> pd.Series:
        """CDR BankingScheduledPaymentFrom JSON — domestic account source."""
        import json
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 174)
        results = []
        for _ in ids:
            results.append(json.dumps({
                "accountId": f"ACC-{fake.numerify('######')}",
            }))
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def udf_payment_set_json(ids: pd.Series) -> pd.Series:
        """CDR BankingScheduledPaymentSet JSON array — payment destination(s)."""
        import json
        import numpy as np
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 175)
        rng = np.random.default_rng(SEED + 175)
        results = []
        for _ in ids:
            amount = f"{rng.lognormal(6.2, 0.7):.2f}"
            pay_type = rng.choice(["domestic", "biller"], p=[0.75, 0.25])
            if pay_type == "domestic":
                dest = {
                    "toUType": "accountId",
                    "accountId": f"ACC-{fake.numerify('######')}",
                    "amount": amount,
                    "currency": "AUD",
                }
            else:
                dest = {
                    "toUType": "biller",
                    "billerCode": fake.numerify("#####"),
                    "crn": fake.numerify("############"),
                    "billerName": fake.company(),
                    "amount": amount,
                    "currency": "AUD",
                }
            results.append(json.dumps([dest]))
        return pd.Series(results)

    @F.pandas_udf(StringType())
    def udf_recurrence_json(ids: pd.Series) -> pd.Series:
        """CDR BankingScheduledPaymentRecurrence JSON."""
        import json
        import numpy as np
        from faker import Faker
        fake = Faker("en_AU")
        Faker.seed(SEED + 176)
        rng = np.random.default_rng(SEED + 176)
        freq_map = {
            "WEEKLY": "P1W",
            "FORTNIGHTLY": "P2W",
            "MONTHLY": "P1M",
            "QUARTERLY": "P3M",
            "ANNUALLY": "P1Y",
        }
        freqs = list(freq_map.keys())
        weights = [0.15, 0.20, 0.45, 0.15, 0.05]
        results = []
        for _ in ids:
            freq = rng.choice(freqs, p=weights)
            interval = freq_map[freq]
            results.append(json.dumps({
                "recurrenceUType": "intervalSchedule",
                "intervalSchedule": {
                    "finalPaymentDate": (
                        fake.date_between(start_date="+30d", end_date="+5y").strftime("%Y-%m-%d")
                    ),
                    "paymentsRemaining": int(rng.integers(1, 60)),
                    "nonBusinessDayTreatment": rng.choice(["AFTER", "BEFORE", "ON", "ONLY"]),
                    "intervals": [{"dayInInterval": "01", "multiplier": interval}],
                },
            }))
        return pd.Series(results)

    accts = _load_accounts_idx(spark, cfg)

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("acc_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 177))) % cfg.N_ACCOUNTS).cast("int"))
    )
    base = (
        base.join(accts.select("acc_idx", "global_id", "accountId"), on="acc_idx", how="left")
        .drop("acc_idx")
    )

    df = (
        base
        .withColumn("scheduledPaymentId", F.expr("uuid()"))
        .withColumn("nickname", udf_sp_nickname(F.col("id")))
        .withColumn("status",
                    F.when(F.rand(SEED + 178) < 0.70, "ACTIVE")
                     .when(F.rand(SEED + 178) < 0.80, "SKIP")
                     .otherwise("INACTIVE"))
        .withColumn("payerReference", udf_payer_reference(F.col("id")))
        .withColumn("payeeReference", udf_payee_reference(F.col("id")))
        .withColumn("amount", udf_sp_amount(F.col("id")))
        .withColumn("frequency",
                    F.when(F.rand(SEED + 179) < 0.15, "WEEKLY")
                     .when(F.rand(SEED + 179) < 0.35, "FORTNIGHTLY")
                     .when(F.rand(SEED + 179) < 0.80, "MONTHLY")
                     .when(F.rand(SEED + 179) < 0.95, "QUARTERLY")
                     .otherwise("ANNUALLY"))
        .withColumn("nextPaymentDate",
                    F.date_add(F.current_date(), (F.rand(SEED + 180) * 90).cast("int")).cast("string"))
        .withColumn("from", udf_from_json(F.col("id")))
        .withColumn("paymentSet", udf_payment_set_json(F.col("id")))
        .withColumn("recurrence", udf_recurrence_json(F.col("id")))
        .select("global_id", "accountId", "scheduledPaymentId", "nickname",
                "status", "payerReference", "payeeReference",
                "amount", "frequency", "nextPaymentDate",
                "from", "paymentSet", "recurrence")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/banking_scheduled_payments")
    print(f"  banking_scheduled_payments: {N:,} rows")


# ── Public entry point ───────────────────────────────────────────────────────

def run(spark, cfg):
    print("=== CDR Extended generation ===")
    _generate_organisations(spark, cfg)
    _generate_addresses(spark, cfg)
    _generate_banking_products(spark, cfg)
    _generate_balances(spark, cfg)
    _generate_direct_debits(spark, cfg)
    _generate_payees(spark, cfg)
    _generate_scheduled_payments(spark, cfg)
    print("=== CDR Extended done ===")


if __name__ == "__main__":
    import config as cfg
    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
