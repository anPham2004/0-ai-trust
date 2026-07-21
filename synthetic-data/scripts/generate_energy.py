"""Generate Energy tables: energy_plans, energy_accounts, energy_service_points,
energy_invoices.

Reads global_id pool from _tmp/customers_fk.
Writes Parquet to OUTPUT_PATH; persists _tmp/energy_accounts_fk for downstream FK reuse.
SEED offsets: +200 to +299.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd


# ---------------------------------------------------------------------------
# FK helpers
# ---------------------------------------------------------------------------

def _load_customers_idx(spark, cfg):
    return spark.read.parquet(cfg.tmp_path("customers_fk")).withColumn(
        "cust_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


def _load_energy_accounts_idx(spark, cfg):
    return spark.read.parquet(cfg.tmp_path("energy_accounts_fk")).withColumn(
        "ea_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )


# ---------------------------------------------------------------------------
# Pandas UDFs (imports inside body — never at module level)
# ---------------------------------------------------------------------------

@F.pandas_udf(StringType())
def _udf_national_metering_id(ids: pd.Series) -> pd.Series:
    """10-digit NMI via Faker numerify."""
    from faker import Faker
    fake = Faker()
    Faker.seed(200)
    return pd.Series([fake.numerify("##########") for _ in ids])


@F.pandas_udf(StringType())
def _udf_invoice_amount(ids: pd.Series) -> pd.Series:
    """Log-normal invoice amount, mean ~$250."""
    import numpy as np
    np.random.seed(210)
    vals = np.random.lognormal(5.5, 0.5, size=len(ids))
    return pd.Series([f"{v:.2f}" for v in vals])


@F.pandas_udf(StringType())
def _udf_gst_amount(amounts: pd.Series) -> pd.Series:
    """GST = 10% of invoiceAmount."""
    return pd.Series([f"{float(a) * 0.1:.2f}" for a in amounts])


@F.pandas_udf(StringType())
def _udf_balance_at_issue(ids: pd.Series) -> pd.Series:
    """Log-normal balance, mean ~$100."""
    import numpy as np
    np.random.seed(220)
    vals = np.random.lognormal(4.6, 0.5, size=len(ids))
    return pd.Series([f"{v:.2f}" for v in vals])


@F.pandas_udf(StringType())
def _udf_location_json(ids: pd.Series) -> pd.Series:
    """CDR EnergyServicePointLocation JSON."""
    import json
    from faker import Faker
    fake = Faker("en_AU")
    Faker.seed(230)
    results = []
    for _ in ids:
        results.append(json.dumps({
            "addressUType": "simple",
            "simple": {
                "addressLine1": fake.street_address(),
                "suburb": fake.city(),
                "state": fake.state_abbr(),
                "postcode": fake.postcode(),
                "country": "AUS",
            },
        }))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_distribution_loss_factor_json(ids: pd.Series) -> pd.Series:
    """CDR EnergyServicePointDistributionLossFactor JSON."""
    import json
    import numpy as np
    rng = np.random.default_rng(231)
    results = []
    for _ in ids:
        results.append(json.dumps({
            "code": f"DLF{int(rng.integers(1000, 9999))}",
            "description": "Distribution Loss Factor",
            "lossValue": f"{rng.uniform(0.95, 1.05):.4f}",
        }))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_related_participants_json(ids: pd.Series) -> pd.Series:
    """CDR EnergyServicePointRelatedParticipants JSON array."""
    import json
    import numpy as np
    rng = np.random.default_rng(232)
    ROLES = ["FRMP", "LR", "DRSP", "ENM", "NSP", "RECB"]
    results = []
    for _ in ids:
        n = int(rng.integers(1, 4))
        roles = rng.choice(ROLES, size=n, replace=False)
        participants = [{"party": f"PART{int(rng.integers(1000, 9999))}", "role": r} for r in roles]
        results.append(json.dumps(participants))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_consumer_profile_json(ids: pd.Series) -> pd.Series:
    """CDR EnergyServicePointConsumerProfile JSON."""
    import json
    import numpy as np
    rng = np.random.default_rng(233)
    CLASSIFICATIONS = ["BUSINESS", "RESIDENTIAL"]
    THRESHOLDS = ["LOW", "MEDIUM", "HIGH"]
    results = []
    for _ in ids:
        results.append(json.dumps({
            "classification": rng.choice(CLASSIFICATIONS),
            "threshold": rng.choice(THRESHOLDS),
        }))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_meters_json(ids: pd.Series) -> pd.Series:
    """CDR EnergyServicePointMeters JSON array."""
    import json
    import numpy as np
    rng = np.random.default_rng(234)
    METER_TYPES = ["ACCUMULATION", "INTERVAL", "BASIC"]
    STATUS = ["CURRENT", "REPLACED", "DECOMMISSIONED"]
    results = []
    for _ in ids:
        n = int(rng.integers(1, 3))
        meters = []
        for _ in range(n):
            meters.append({
                "meterId": f"MTR{int(rng.integers(100000, 999999))}",
                "specifications": {
                    "status": rng.choice(STATUS),
                    "installationType": rng.choice(METER_TYPES),
                    "manufacturer": rng.choice(["Landis+Gyr", "Itron", "Honeywell", "ABB"]),
                    "model": f"Model-{int(rng.integers(100, 999))}",
                    "readType": rng.choice(["BASIC", "INTERVAL", "VISUAL"]),
                    "nextScheduledReadDate": f"2025-{int(rng.integers(1, 13)):02d}-{int(rng.integers(1, 28)):02d}",
                },
            })
        results.append(json.dumps(meters))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_service_points_ref_json(ids: pd.Series) -> pd.Series:
    """Invoice servicePoints reference JSON array."""
    import json
    import numpy as np
    rng = np.random.default_rng(240)
    results = []
    for _ in ids:
        results.append(json.dumps([{
            "servicePointId": f"NMI-{int(rng.integers(10000000, 99999999)):08d}",
            "usage": f"{rng.lognormal(4.0, 0.5):.3f}",
        }]))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_invoice_period_json(issue_dates: pd.Series) -> pd.Series:
    """Invoice billing period JSON."""
    import json
    from datetime import datetime, timedelta
    import numpy as np
    rng = np.random.default_rng(241)
    results = []
    for d in issue_dates:
        try:
            end_dt = datetime.strptime(str(d), "%Y-%m-%d")
            days = int(rng.integers(28, 92))
            start_dt = end_dt - timedelta(days=days)
            results.append(json.dumps({
                "startDate": start_dt.strftime("%Y-%m-%d"),
                "endDate": end_dt.strftime("%Y-%m-%d"),
            }))
        except Exception:
            results.append(None)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_electricity_json(amounts: pd.Series) -> pd.Series:
    """Invoice electricity charge breakdown JSON."""
    import json
    import numpy as np
    rng = np.random.default_rng(242)
    results = []
    for amt in amounts:
        try:
            total = float(amt)
            supply_charge = round(total * rng.uniform(0.08, 0.15), 2)
            usage_charge = round(total * rng.uniform(0.55, 0.70), 2)
            network_charge = round(total - supply_charge - usage_charge, 2)
            results.append(json.dumps({
                "totalUsageCharges": f"{usage_charge:.2f}",
                "totalGenerationCredits": "0.00",
                "totalOnceOffCharges": "0.00",
                "totalOnceOffDiscounts": "0.00",
                "otherCharges": [],
                "totalGst": f"{total * 0.1:.2f}",
                "supplyCharge": f"{supply_charge:.2f}",
                "networkCharges": f"{network_charge:.2f}",
            }))
        except Exception:
            results.append(None)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_gas_json(amounts: pd.Series) -> pd.Series:
    """Invoice gas charge breakdown JSON — for gas/dual-fuel accounts (~30%)."""
    import json
    import numpy as np
    rng = np.random.default_rng(244)
    results = []
    for amt in amounts:
        try:
            total = float(amt)
            supply_charge = round(total * rng.uniform(0.10, 0.18), 2)
            usage_charge = round(total * rng.uniform(0.60, 0.75), 2)
            network_charge = round(total - supply_charge - usage_charge, 2)
            results.append(json.dumps({
                "totalUsageCharges": f"{usage_charge:.2f}",
                "totalGenerationCredits": "0.00",
                "totalOnceOffCharges": "0.00",
                "totalOnceOffDiscounts": "0.00",
                "otherCharges": [],
                "totalGst": f"{total * 0.1:.2f}",
                "supplyCharge": f"{supply_charge:.2f}",
                "networkCharges": f"{network_charge:.2f}",
            }))
        except Exception:
            results.append(None)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_account_charges_json(amounts: pd.Series) -> pd.Series:
    """Invoice account-level charges JSON."""
    import json
    import numpy as np
    rng = np.random.default_rng(243)
    results = []
    for amt in amounts:
        try:
            total = float(amt)
            results.append(json.dumps({
                "totalChargesBeforeTax": f"{total:.2f}",
                "totalDiscountsBeforeTax": f"{round(total * rng.uniform(0, 0.05), 2):.2f}",
                "totalGst": f"{total * 0.1:.2f}",
                "totalChargesAfterTax": f"{total * 1.1:.2f}",
            }))
        except Exception:
            results.append(None)
    return pd.Series(results)


# ---------------------------------------------------------------------------
# Table generators
# ---------------------------------------------------------------------------

def _generate_energy_plans(spark, cfg):
    """Catalogue table — NO global_id, no customer FK."""
    N, SEED = cfg.N_ENERGY_PLANS, cfg.SEED

    total_secs = int((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_epoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))

    df = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .select(
            F.concat(F.lit("EPLAN-"), F.lpad(F.col("id").cast("string"), 3, "0")).alias("planId"),
            F.lit("EnergyAus").alias("brand"),
            F.lit("EnergyAus Australia").alias("brandName"),
            # displayName: 10 weighted plan names
            F.when(F.rand(SEED + 200) < 0.20, "Basic Electricity")
             .when(F.rand(SEED + 200) < 0.35, "Solar Saver")
             .when(F.rand(SEED + 200) < 0.50, "Gas Essential")
             .when(F.rand(SEED + 200) < 0.60, "Green Power 100")
             .when(F.rand(SEED + 200) < 0.70, "Business Electricity")
             .when(F.rand(SEED + 200) < 0.80, "Dual Fuel Bundle")
             .when(F.rand(SEED + 200) < 0.85, "EV Home Charger")
             .when(F.rand(SEED + 200) < 0.90, "Off-Peak Saver")
             .when(F.rand(SEED + 200) < 0.95, "Time of Use")
             .otherwise("Controlled Load")
             .alias("displayName"),
            # fuelType: ELECTRICITY 50%, GAS 30%, DUAL 20%
            F.when(F.rand(SEED + 201) < 0.50, "ELECTRICITY")
             .when(F.rand(SEED + 201) < 0.80, "GAS")
             .otherwise("DUAL")
             .alias("fuelType"),
            # type: STANDING 30%, MARKET 50%, REGULATED 20%
            F.when(F.rand(SEED + 202) < 0.30, "STANDING")
             .when(F.rand(SEED + 202) < 0.80, "MARKET")
             .otherwise("REGULATED")
             .alias("type"),
            # customerType: RESIDENTIAL 70%, BUSINESS 30%
            F.when(F.rand(SEED + 203) < 0.70, "RESIDENTIAL")
             .otherwise("BUSINESS")
             .alias("customerType"),
            # effectiveFrom: random date within ~3.3 years from 2022-01-01
            F.date_add(F.lit("2022-01-01"), (F.rand(SEED + 204) * 1200).cast("int"))
             .cast("string")
             .alias("effectiveFrom"),
            # lastUpdated: random datetime in config date range
            F.date_format(
                (start_epoch + (F.rand(SEED + 205) * total_secs).cast("long")).cast("timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            ).alias("lastUpdated"),
        )
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/energy_plans")
    print(f"  energy_plans: {N:,} rows")


def _generate_energy_accounts(spark, cfg):
    """FK to customers via customers_fk. Writes energy_accounts_fk tmp."""
    N, SEED = cfg.N_ENERGY_ACCOUNTS, cfg.SEED

    total_secs = int((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_epoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))

    cust = _load_customers_idx(spark, cfg)

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("cust_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 210))) % cfg.N_CUSTOMERS).cast("int"))
    )

    df = (
        base
        .join(cust.select("cust_idx", "global_id"), on="cust_idx", how="left")
        .drop("cust_idx")
        .withColumn("energyAccountId", F.expr("uuid()"))
        .withColumn("accountNumber", F.concat(F.lit("EA-"), F.lpad(F.col("id").cast("string"), 6, "0")))
        .withColumn("displayName", F.concat(F.lit("Energy Account "), F.col("id").cast("string")))
        # openStatus: OPEN 85%, CLOSED 15%
        .withColumn(
            "openStatus",
            F.when(F.rand(SEED + 211) < 0.85, "OPEN").otherwise("CLOSED")
        )
        # creationDate: random date in config date range
        .withColumn(
            "creationDate",
            F.date_add(
                F.lit(cfg.START_DATE.strftime("%Y-%m-%d")),
                (F.rand(SEED + 212) * (cfg.END_DATE - cfg.START_DATE).days).cast("int")
            ).cast("string")
        )
        # planId: FK to energy_plans
        .withColumn(
            "planId",
            F.concat(
                F.lit("EPLAN-"),
                F.lpad(
                    (F.abs(F.hash(F.col("id"), F.lit(SEED + 213))) % cfg.N_ENERGY_PLANS).cast("string"),
                    3, "0"
                )
            )
        )
        .select("global_id", "energyAccountId", "accountNumber", "displayName",
                "openStatus", "creationDate", "planId")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/energy_accounts")
    df.select("global_id", "energyAccountId").write.mode("overwrite").parquet(
        cfg.tmp_path("energy_accounts_fk")
    )
    print(f"  energy_accounts: {N:,} rows")


def _generate_service_points(spark, cfg):
    """FK to energy_accounts via energy_accounts_fk tmp."""
    N, SEED = cfg.N_ENERGY_SVC_POINTS, cfg.SEED

    ea = _load_energy_accounts_idx(spark, cfg)

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("ea_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 220))) % cfg.N_ENERGY_ACCOUNTS).cast("int"))
    )

    total_secs = int((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_epoch = F.unix_timestamp(F.lit(cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")))

    df = (
        base
        .join(ea.select("ea_idx", "global_id", "energyAccountId"), on="ea_idx", how="left")
        .drop("ea_idx")
        .withColumn("servicePointId", F.concat(F.lit("NMI-"), F.lpad(F.col("id").cast("string"), 8, "0")))
        .withColumn("nationalMeteringId", _udf_national_metering_id(F.col("id")))
        # CDR-compliant jurisdictionCode enum
        .withColumn(
            "jurisdictionCode",
            F.when(F.rand(SEED + 221) < 0.32, "NSW")
             .when(F.rand(SEED + 221) < 0.58, "VIC")
             .when(F.rand(SEED + 221) < 0.78, "QLD")
             .when(F.rand(SEED + 221) < 0.90, "WA")
             .when(F.rand(SEED + 221) < 0.96, "SA")
             .when(F.rand(SEED + 221) < 0.98, "TAS")
             .when(F.rand(SEED + 221) < 0.99, "ACT")
             .otherwise("NT")
        )
        # CDR-compliant servicePointClassification enum
        .withColumn(
            "servicePointClassification",
            F.when(F.rand(SEED + 222) < 0.70, "SMALL")
             .when(F.rand(SEED + 222) < 0.85, "LARGE")
             .when(F.rand(SEED + 222) < 0.90, "GENERATOR")
             .when(F.rand(SEED + 222) < 0.94, "WHOLESALE")
             .when(F.rand(SEED + 222) < 0.96, "NON_CONTEST_UNMETERED_LOAD")
             .when(F.rand(SEED + 222) < 0.98, "NON_REGISTERED_EMBEDDED_GENERATOR")
             .when(F.rand(SEED + 222) < 0.99, "DISTRIBUTION_WHOLESALE")
             .otherwise("EXTERNAL_PROFILE")
        )
        # CDR-compliant servicePointStatus enum
        .withColumn(
            "servicePointStatus",
            F.when(F.rand(SEED + 223) < 0.88, "ACTIVE")
             .when(F.rand(SEED + 223) < 0.93, "DE_ENERGISED")
             .when(F.rand(SEED + 223) < 0.96, "GREENFIELD")
             .when(F.rand(SEED + 223) < 0.98, "OFF_MARKET")
             .otherwise("EXTINCT")
        )
        # validFromDate: random date in config date range
        .withColumn(
            "validFromDate",
            F.date_add(
                F.lit(cfg.START_DATE.strftime("%Y-%m-%d")),
                (F.rand(SEED + 224) * (cfg.END_DATE - cfg.START_DATE).days).cast("int")
            ).cast("string")
        )
        # isGenerator: 5% true
        .withColumn(
            "isGenerator",
            (F.rand(SEED + 225) < 0.05).cast("boolean")
        )
        # lastUpdateDateTime: REQUIRED CDR field
        .withColumn(
            "lastUpdateDateTime",
            F.date_format(
                (start_epoch + (F.rand(SEED + 226) * total_secs).cast("long")).cast("timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        # CDR nested objects
        .withColumn("location", _udf_location_json(F.col("id")))
        .withColumn("distributionLossFactor", _udf_distribution_loss_factor_json(F.col("id")))
        .withColumn("relatedParticipants", _udf_related_participants_json(F.col("id")))
        .withColumn("consumerProfile", _udf_consumer_profile_json(F.col("id")))
        .withColumn("meters", _udf_meters_json(F.col("id")))
        .select("global_id", "energyAccountId", "servicePointId",
                "nationalMeteringId", "jurisdictionCode",
                "servicePointClassification", "servicePointStatus",
                "validFromDate", "lastUpdateDateTime", "isGenerator",
                "location", "distributionLossFactor", "relatedParticipants",
                "consumerProfile", "meters")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/energy_service_points")
    print(f"  energy_service_points: {N:,} rows")


def _generate_invoices(spark, cfg):
    """FK to energy_accounts via energy_accounts_fk tmp."""
    N, SEED = cfg.N_ENERGY_INVOICES, cfg.SEED

    ea = _load_energy_accounts_idx(spark, cfg)

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn("ea_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 230))) % cfg.N_ENERGY_ACCOUNTS).cast("int"))
    )

    df = (
        base
        .join(ea.select("ea_idx", "global_id", "energyAccountId"), on="ea_idx", how="left")
        .drop("ea_idx")
        .withColumn("invoiceNumber", F.concat(F.lit("INV-"), F.lpad(F.col("id").cast("string"), 7, "0")))
        # issueDate: random date in config date range
        .withColumn(
            "issueDate",
            F.date_add(
                F.lit(cfg.START_DATE.strftime("%Y-%m-%d")),
                (F.rand(SEED + 231) * (cfg.END_DATE - cfg.START_DATE).days).cast("int")
            ).cast("string")
        )
        # dueDate: issueDate + 21 days
        .withColumn(
            "dueDate",
            F.date_add(F.col("issueDate"), F.lit(21)).cast("string")
        )
        # invoiceAmount: log-normal mean ~$250
        .withColumn("invoiceAmount", _udf_invoice_amount(F.col("id")))
        # gstAmount: 10% of invoiceAmount
        .withColumn("gstAmount", _udf_gst_amount(F.col("invoiceAmount")))
        # CDR-compliant paymentStatus enum: PAID / PARTIALLY_PAID / NOT_PAID
        .withColumn(
            "paymentStatus",
            F.when(F.rand(SEED + 232) < 0.75, "PAID")
             .when(F.rand(SEED + 232) < 0.90, "PARTIALLY_PAID")
             .otherwise("NOT_PAID")
        )
        # balanceAtIssue: log-normal mean ~$100
        .withColumn("balanceAtIssue", _udf_balance_at_issue(F.col("id")))
        # CDR nested objects
        .withColumn("servicePoints", _udf_service_points_ref_json(F.col("id")))
        .withColumn("period", _udf_invoice_period_json(F.col("issueDate")))
        .withColumn("electricity", _udf_electricity_json(F.col("invoiceAmount")))
        .withColumn("accountCharges", _udf_account_charges_json(F.col("invoiceAmount")))
        # gas: present for ~30% of invoices (GAS/DUAL fuelType accounts)
        .withColumn(
            "gas",
            F.when(F.rand(SEED + 234) < 0.30, _udf_gas_json(F.col("invoiceAmount")))
             .otherwise(F.lit(None).cast("string"))
        )
        # payOnTimeDiscount: optional, 20% of invoices offer it
        .withColumn(
            "payOnTimeDiscount",
            F.when(F.rand(SEED + 233) < 0.20,
                   F.concat(
                       F.lit('{"discountAmount":"'),
                       F.round(F.col("invoiceAmount").cast("double") * 0.02, 2).cast("string"),
                       F.lit('","gstAmount":"'),
                       F.round(F.col("invoiceAmount").cast("double") * 0.002, 2).cast("string"),
                       F.lit('","date":"'),
                       F.col("dueDate"),
                       F.lit('"}')
                   ))
             .otherwise(F.lit(None).cast("string"))
        )
        .select("global_id", "energyAccountId", "invoiceNumber",
                "issueDate", "dueDate", "invoiceAmount", "gstAmount",
                "paymentStatus", "balanceAtIssue",
                "servicePoints", "period", "electricity", "gas",
                "accountCharges", "payOnTimeDiscount")
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/energy_invoices")
    print(f"  energy_invoices: {N:,} rows")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(spark, cfg):
    print("=== Energy generation ===")
    _generate_energy_plans(spark, cfg)
    _generate_energy_accounts(spark, cfg)
    _generate_service_points(spark, cfg)
    _generate_invoices(spark, cfg)
    print("=== Energy done ===")


if __name__ == "__main__":
    import config as cfg
    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
