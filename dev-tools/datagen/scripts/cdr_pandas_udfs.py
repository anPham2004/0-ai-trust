"""
Pandas UDFs for CDR table generation.
Faker is imported INSIDE each function body — required for Databricks serverless.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import pandas as pd


@F.pandas_udf(StringType())
def udf_first_name(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker("en_AU")
    Faker.seed(42)
    return pd.Series([fake.first_name() for _ in range(len(ids))])


@F.pandas_udf(StringType())
def udf_last_name(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker("en_AU")
    Faker.seed(42)
    return pd.Series([fake.last_name() for _ in range(len(ids))])


@F.pandas_udf(StringType())
def udf_email(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker("en_AU")
    Faker.seed(42)
    return pd.Series([fake.email() for _ in range(len(ids))])


@F.pandas_udf(StringType())
def udf_phone(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker("en_AU")
    Faker.seed(42)
    return pd.Series([fake.phone_number() for _ in range(len(ids))])


@F.pandas_udf(StringType())
def udf_balance(account_types: pd.Series) -> pd.Series:
    import numpy as np
    rng = np.random.default_rng(42)
    params = {"savings": (9.8, 0.8), "transaction": (7.9, 0.7), "loan": (10.7, 0.6)}
    results = []
    for atype in account_types:
        mu, sigma = params.get(atype, (9.0, 0.8))
        results.append(f"{rng.lognormal(mu, sigma):.2f}")
    return pd.Series(results)


@F.pandas_udf(StringType())
def udf_merchant(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker("en_AU")
    Faker.seed(42)
    return pd.Series([fake.company() for _ in range(len(ids))])


@F.pandas_udf(StringType())
def udf_tx_amount(tx_types: pd.Series) -> pd.Series:
    import numpy as np
    rng = np.random.default_rng(42)
    results = []
    for ttype in tx_types:
        val = rng.lognormal(6.5, 1.0)   # median ~$665
        results.append(f"{-val:.2f}" if ttype == "debit" else f"{val:.2f}")
    return pd.Series(results)
