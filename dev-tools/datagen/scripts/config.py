"""
Shared configuration for all synthetic data generation scripts.

SCALE is the single knob to resize the entire dataset:
  SCALE = 1    →  ~63K rows   (laptop default, ~2 min)
  SCALE = 10   →  ~630K rows  (~20 min)
  SCALE = 100  →  ~6.3M rows  (server recommended)

All row counts are derived from SCALE so FK ratios stay consistent.
Tables that use a 1:1 row_number join pattern are pinned to their parent
count — changing only those independently would cause NULL foreign keys.

Run:
    python3.11 dev-tools/datagen/scripts/run_all.py

Prerequisites:
    pip install -r requirements.txt
"""
import os
from datetime import datetime, timedelta

# ── Execution mode ─────────────────────────────────────────────────────────────
USE_SERVERLESS = False   # True = Databricks Connect serverless; False = local PySpark
CATALOG = "main"         # Only used when USE_SERVERLESS=True
SCHEMA  = "crm_synthetic"

# ── Output / temp paths ────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "output"))
TMP_PATH    = os.path.join(OUTPUT_PATH, "_tmp")

# ── Reproducibility ────────────────────────────────────────────────────────────
SEED = 42

# ── Date range ─────────────────────────────────────────────────────────────────
END_DATE   = datetime(2026, 7, 16)
START_DATE = END_DATE - timedelta(days=365)

# ══════════════════════════════════════════════════════════════════════════════
#  SCALE — the single knob to resize the entire dataset
#  Set to 1 for local dev, 10 for integration testing, 100+ for production.
# ══════════════════════════════════════════════════════════════════════════════
SCALE = 1

# ── Master entity ──────────────────────────────────────────────────────────────
N_CUSTOMERS          = 1_000 * SCALE   # source of every global_id

# ── CDR core — banking ─────────────────────────────────────────────────────────
#    Ratio: 1.5 accounts / customer, 10 transactions / account
N_ACCOUNTS           = round(1.5  * N_CUSTOMERS)
N_TRANSACTIONS       = round(10   * N_ACCOUNTS)

# ── CDR extended — account-level ──────────────────────────────────────────────
#    N_BANKING_BALANCES uses a 1:1 row_number join → MUST equal N_ACCOUNTS
N_BANKING_BALANCES   = N_ACCOUNTS                   # 1:1 with accounts
N_DIRECT_DEBITS      = round(1.5  * N_ACCOUNTS)     # 1.5 direct debits / account
N_SCHEDULED_PAYMENTS = round(0.8  * N_ACCOUNTS)     # 0.8 scheduled payments / account
N_MORTGAGE_ACCOUNTS  = round(0.35 * N_ACCOUNTS)     # ~35% of accounts have a mortgage
N_LOAN_ACCOUNTS      = round(0.25 * N_ACCOUNTS)     # ~25% of accounts have a personal loan

# ── CDR extended — customer-level ─────────────────────────────────────────────
N_ORGANISATIONS      = round(0.5  * N_CUSTOMERS)    # ~50% of customers are businesses
N_ORG_PARTY_RELS     = round(3.0  * N_ORGANISATIONS) # 3 party relationships per org
N_ORG_RELATIONSHIPS  = round(0.2  * N_ORGANISATIONS) # 20% of orgs have a related org
N_ADDRESSES          = round(2.5  * N_CUSTOMERS)    # 2.5 addresses / customer
N_PAYEES             = round(3.0  * N_CUSTOMERS)    # 3 saved payees / customer
N_CREDIT_CARDS       = round(1.2  * N_CUSTOMERS)    # 1.2 credit cards / customer

# ── CDR extended — catalogue (fixed; not scaled) ──────────────────────────────
N_BANKING_PRODUCTS   = 50

# ── BPI — loan applications ───────────────────────────────────────────────────
#    Child tables are proportional to N_APPLICATIONS, not N_CUSTOMERS
N_APPLICATIONS       = round(0.5  * N_CUSTOMERS)    # 0.5 applications / customer
N_EVENTS             = round(10   * N_APPLICATIONS)  # 10 events / application
N_DOCUMENTS          = round(3.0  * N_APPLICATIONS)  # 3 missing docs / application
N_STAGE_HISTORY      = round(5.0  * N_APPLICATIONS)  # 5 stage changes / application
N_STATUS_CHANGES     = round(4.0  * N_APPLICATIONS)  # 4 status changes / application

# ── Lending Club ──────────────────────────────────────────────────────────────
N_ACCEPTED_LOANS     = round(0.5  * N_CUSTOMERS)    # 0.5 accepted loans / customer
N_REJECTED_APPS      = round(0.25 * N_CUSTOMERS)    # 0.25 rejected apps / customer

# ── Energy ────────────────────────────────────────────────────────────────────
#    energy_service_points and energy_invoices FK to energy_accounts
N_ENERGY_ACCOUNTS    = round(2.0  * N_CUSTOMERS)    # 2 energy accounts / customer
N_ENERGY_SVC_POINTS  = round(1.5  * N_ENERGY_ACCOUNTS)  # 1.5 service points / energy account
N_ENERGY_INVOICES    = round(4.0  * N_ENERGY_ACCOUNTS)  # 4 invoices / energy account

# ── Energy — catalogue (fixed; not scaled) ────────────────────────────────────
N_ENERGY_PLANS       = 30

# ── CRM ───────────────────────────────────────────────────────────────────────
#    N_PREFERENCES uses a 1:1 row_number join → MUST equal N_CUSTOMERS
N_PREFERENCES        = N_CUSTOMERS                  # 1:1 with customers
N_INTERACTIONS       = round(3.0  * N_CUSTOMERS)    # 3 support interactions / customer
N_CASES              = round(0.8  * N_CUSTOMERS)    # 0.8 service cases / customer
N_CASE_EVENTS        = round(3.0  * N_CASES)        # 3 events per service case

# ── Customer profile extended ─────────────────────────────────────────────────
#    N_KYC_RECORDS uses a 1:1 row_number join → MUST equal N_CUSTOMERS
N_INSURANCE_POLICIES = round(1.5  * N_CUSTOMERS)    # 1.5 insurance policies / customer
N_KYC_RECORDS        = N_CUSTOMERS                  # 1:1 with customers


# ══════════════════════════════════════════════════════════════════════════════
#  Helper functions
# ══════════════════════════════════════════════════════════════════════════════

def get_partitions(n_rows: int) -> int:
    """Auto-select Spark partitions based on row count."""
    if n_rows < 100_000:
        return 8
    elif n_rows < 500_000:
        return 16
    elif n_rows < 1_000_000:
        return 32
    return 64


def tmp_path(name: str) -> str:
    """Return local Parquet path for an intermediate FK lookup table."""
    return os.path.join(TMP_PATH, name)


def get_spark():
    """Create a local PySpark session (no Databricks required)."""
    import sys
    os.environ["PYSPARK_PYTHON"]        = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("zero-ai-trust-datagen")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


# ── Bad data injection ─────────────────────────────────────────────────────────
INJECT_BAD_DATA = False
BAD_DATA_CONFIG = {
    "null_rate":      0.02,    # 2% nulls in required fields
    "outlier_rate":   0.01,    # 1% impossible values (e.g. negative amounts)
    "orphan_fk_rate": 0.005,   # 0.5% orphan foreign keys
}
