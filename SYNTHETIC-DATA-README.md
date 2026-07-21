# CRM Synthetic Data Generation Pipeline

**Generated:** 2026-07-22
**Goal:** Generate linked synthetic data across 32 tables covering CDR, BPI, Lending Club, Energy, Banking Products, CRM, Customer Profile, and Organisation Relationship domains -- answering all 37 Banker.AI + 13 Customer.AI + 29 Business Customer questions (66 total).

---

## What Was Delivered

### Schema Definitions

| File | Description |
|------|-------------|
| `ref-schema/cdr-schema/cdr-schema.json` | CDR (Australian banking) -- 18 entities |
| `ref-schema/bpi-schema/bpi-schema.json` | BPI 2017 process mining -- 2 entities |
| `ref-schema/lending-club-schema/lending-club-schema.json` | Lending Club loans -- 2 entities |
| `ref-schema/crm-schema/crm-schema.json` | CRM operational layer -- 6 entities |
| `ref-schema/crm-schema/crm-schema.md` | CRM schema documentation, FK diagram, distributions |
| `ref-schema/ai-questions-gap-analysis.md` | Full gap analysis -- all 37 questions mapped, 0 gaps remaining |

### Generation Scripts

| File | Tables Generated | Default Rows (SCALE=1) |
|------|-----------------|------------------------|
| `synthetic-data/scripts/config.py` | Shared configuration (row counts, paths, SCALE knob) | -- |
| `synthetic-data/scripts/cdr_pandas_udfs.py` | Faker UDF library (shared) | -- |
| `synthetic-data/scripts/generate_cdr.py` | `customers`, `banking_accounts`, `banking_transactions` | 17,500 |
| `synthetic-data/scripts/generate_cdr_extended.py` | `organisations`, `physical_addresses`, `banking_products`, `banking_balances`, `banking_direct_debits`, `banking_payees`, `banking_scheduled_payments` | 12,000 |
| `synthetic-data/scripts/generate_bpi.py` | `loan_applications`, `loan_application_events` | 5,500 |
| `synthetic-data/scripts/generate_lending_club.py` | `accepted_loans`, `rejected_applications` | 750 |
| `synthetic-data/scripts/generate_energy.py` | `energy_plans`, `energy_accounts`, `energy_service_points`, `energy_invoices` | 13,030 |
| `synthetic-data/scripts/generate_banking_products_ext.py` | `mortgage_accounts`, `loan_accounts`, `credit_cards` | 2,100 |
| `synthetic-data/scripts/generate_crm_preferences.py` | `customer_preferences` | 1,000 |
| `synthetic-data/scripts/generate_crm_interactions.py` | `support_interactions`, `service_cases` | 3,800 |
| `synthetic-data/scripts/generate_crm_documents.py` | `missing_documents` | 1,500 |
| `synthetic-data/scripts/generate_crm_history.py` | `application_stage_history`, `status_change_history` | 4,500 |
| `synthetic-data/scripts/generate_customer_profile_ext.py` | `insurance_policies`, `kyc_records` | 2,500 |
| `synthetic-data/scripts/generate_org_relationships.py` | `organisation_party_relationships`, `organisation_relationships` | 1,600 |
| `synthetic-data/scripts/generate_crm_case_events.py` | `service_case_events` | 2,400 |
| `synthetic-data/scripts/generate_schema.py` | Reads all Parquet output, writes `output/master-schema.json` | -- |
| `synthetic-data/scripts/run_all.py` | **Orchestrator** -- runs all 13 steps in FK order, validates output | ~67,180 total |

### 32 Output Tables

```
output/
├── customers/                            CDR customer profiles (master entity)
├── banking_accounts/                     CDR bank accounts (has organisationId FK)
├── banking_transactions/                 CDR transactions
├── organisations/                        CDR business entities (linked to customers via global_id)
├── physical_addresses/                   CDR customer/org addresses
├── banking_products/                     CDR product catalogue [CATALOGUE]
├── banking_balances/                     CDR account balances (1:1 with accounts)
├── banking_direct_debits/                CDR direct debit arrangements
├── banking_payees/                       CDR saved payees
├── banking_scheduled_payments/           CDR scheduled payments
├── loan_applications/                    BPI loan applications (has organisationId FK)
├── loan_application_events/              BPI process mining events
├── accepted_loans/                       Lending Club accepted loans (151 fields)
├── rejected_applications/                Lending Club rejections
├── energy_plans/                         Energy plan catalogue [CATALOGUE]
├── energy_accounts/                      Energy customer accounts
├── energy_service_points/                Energy meter/service points
├── energy_invoices/                      Energy billing invoices
├── mortgage_accounts/                    Banking mortgage products
├── loan_accounts/                        Banking personal loan products
├── credit_cards/                         Banking credit card products (has organisationId FK)
├── customer_preferences/                 CRM contact preferences + satisfaction
├── support_interactions/                 CRM support contact log
├── service_cases/                        CRM cases (has organisationId, applicationId, slaDeadline)
├── missing_documents/                    CRM document checklist per application
├── application_stage_history/            CRM stage timeline (has pendingActionParty)
├── status_change_history/                CRM status change audit log
├── insurance_policies/                   Customer insurance policies
├── kyc_records/                          Customer KYC/KYB verification (has organisationId, KYB fields)
├── organisation_party_relationships/     Org multi-party roles (directors, reps, owners)
├── organisation_relationships/           Org-to-org links (parent/subsidiary, trust) [CATALOGUE]
└── service_case_events/                  CRM case activity log (events per case)
```

---

## Key Design: `global_id`

Every table carries a `global_id` UUID -- the single cross-dataset join key that identifies a customer. Join any two tables on `global_id` without needing to know intermediate FK chains.

```
customers.global_id
  ├── banking_accounts.global_id
  │     ├── banking_accounts.organisationId → organisations.organisationId
  │     ├── banking_transactions.accountId
  │     ├── banking_balances.accountId
  │     ├── banking_direct_debits.accountId
  │     └── banking_scheduled_payments.accountId
  ├── organisations.global_id
  │     ├── organisation_party_relationships.organisationId
  │     └── organisation_relationships.sourceOrgId / .targetOrgId  [CATALOGUE]
  ├── physical_addresses.global_id
  ├── banking_payees.global_id
  ├── loan_applications.global_id
  │     ├── loan_applications.organisationId → organisations.organisationId
  │     ├── loan_application_events.global_id
  │     ├── missing_documents.global_id
  │     ├── application_stage_history.global_id  (+ pendingActionParty)
  │     └── status_change_history.global_id
  ├── accepted_loans.global_id
  ├── rejected_applications.global_id
  ├── energy_accounts.global_id
  │     ├── energy_service_points.energyAccountId
  │     └── energy_invoices.energyAccountId
  ├── mortgage_accounts.global_id
  ├── loan_accounts.global_id
  ├── credit_cards.global_id
  │     └── credit_cards.organisationId → organisations.organisationId
  ├── customer_preferences.global_id
  ├── support_interactions.global_id
  ├── service_cases.global_id
  │     ├── service_cases.organisationId → organisations.organisationId
  │     ├── service_cases.applicationId → loan_applications.applicationId
  │     └── service_case_events.caseId
  ├── insurance_policies.global_id
  └── kyc_records.global_id
        └── kyc_records.organisationId → organisations.organisationId (KYB)
```

### Customer Types & Business Accounts

The data models individual vs business at four levels:

**1. Customer level** -- `customers.customerType` classifies the customer at onboarding:

| Type | Distribution | Description |
|------|-------------|-------------|
| `INDIVIDUAL` | ~55% | Personal/retail banking customer |
| `BUSINESS` | ~35% | Represents a registered business entity |
| `SOLE_TRADER` | ~10% | Operates a business under own name, no separate legal entity |

**2. Account level** -- `banking_accounts.organisationId` (nullable FK to `organisations.organisationId`):
- `NULL` = personal account (belongs to the person)
- Set = business account (belongs to the organisation; person is the authorised agent)

**3. Organisation level** -- `organisations` table holds the business entity details, linked to a person via `global_id`.

**4. Organisation relationships** -- two new tables model business-centric data:
- `organisation_party_relationships`: multiple parties per org (directors, authorised reps, beneficial owners, signatories, guarantors, secretaries) with authority levels and active/inactive status
- `organisation_relationships`: org-to-org links (parent/subsidiary, trust/trustee, partnership, franchise, joint venture)

```
Philip (CUST-00042, customerType: BUSINESS, global_id: abc-123)
  ├── Account A (organisationId: NULL)        → Philip's personal savings
  ├── Account B (organisationId: ORG-00099)   → Netflix's business account
  ├── organisations
  │     └── ORG-00099: Netflix Pty Ltd (agentRole: DIRECTOR)
  ├── organisation_party_relationships
  │     ├── Philip → ORG-00099 (DIRECTOR, FULL authority, active)
  │     └── Sarah  → ORG-00099 (AUTHORISED_REPRESENTATIVE, LIMITED, active)
  └── organisation_relationships
        └── ORG-00099 → ORG-00050 (PARENT_SUBSIDIARY)
```

**5. Org-level FKs** -- `organisationId` (nullable) on key tables enables direct org-centric queries:
- `loan_applications.organisationId` (~35%) -- business loan applications
- `service_cases.organisationId` (~35%) + `applicationId` (~40%) + `slaDeadline` -- org-level case management
- `credit_cards.organisationId` (~35%) -- business credit cards
- `kyc_records.organisationId` (~35%) + KYB fields (`abnVerified`, `asicCheckStatus`, `beneficialOwnershipVerified`)
- `application_stage_history.pendingActionParty` -- who must act next (CUSTOMER, ORGANISATION, AUTHORISED_REPRESENTATIVE, DIRECTOR, GUARANTOR, INTERNAL_TEAM, THIRD_PARTY)

Transactions inherit the personal/business distinction from their parent account via `accountId`.

At SCALE=1: ~542 INDIVIDUAL, ~407 BUSINESS, ~51 SOLE_TRADER customers; ~986 personal accounts, ~514 business accounts.

---

## Prerequisites

### 1. Python 3.11+ (required)

PySpark 4.x requires Python 3.10+. On macOS the system `python3` is 3.9 -- use Homebrew Python:

```bash
brew install python@3.11   # if not already installed
```

### 2. Python dependencies

```bash
python3.11 -m pip install pyspark faker numpy pandas pyarrow
```

`databricks-connect` is **not** required for local mode.

### 3. Java 11+ (required by Spark)

```bash
java -version   # verify; install via brew install openjdk@21 if missing
```

---

## Configuration

Edit `synthetic-data/scripts/config.py` before running:

```python
# Required -- set these before running
CATALOG     = "main"              # your Unity Catalog catalog name
SCHEMA      = "crm_synthetic"    # schema/database name to use
OUTPUT_PATH = "../output"         # relative to scripts/, or Databricks Volume:
                                  # f"/Volumes/{CATALOG}/{SCHEMA}/crm_data"

# Scale -- the single knob to resize the entire dataset
SCALE = 1      # 1 = laptop default (~63K rows), 10 = ~630K, 100 = ~6.3M

# All row counts are derived from SCALE (see config.py for full list):
N_CUSTOMERS      = 1_000 * SCALE   # master pool
N_ACCOUNTS       = round(1.5 * N_CUSTOMERS)
N_TRANSACTIONS   = round(10 * N_ACCOUNTS)
N_ORGANISATIONS  = round(0.5 * N_CUSTOMERS)  # ~50% of customers have business entities
# ... (32 row count settings total)

# Optional -- inject dirty data for quality testing
INJECT_BAD_DATA = False
BAD_DATA_CONFIG = {
    "null_rate":      0.02,    # 2% nulls in required fields
    "outlier_rate":   0.01,    # 1% impossible values
    "orphan_fk_rate": 0.005,   # 0.5% orphan foreign keys
}
```

### Scale guide

| Use case | SCALE | Total rows | Est. time (local) |
|----------|-------|------------|-------------------|
| Demo / inspection | 1 | ~67,000 | ~30s |
| AI question testing | 10 | ~670,000 | 2-5 min |
| ML training / RAG | 100 | ~6,700,000 | 15-30 min |

---

## How to Run

### Full pipeline (recommended)

```bash
cd /Users/khoatran/Developer/G3/0-ai-trust/synthetic-data/scripts
python3.11 run_all.py
```

`run_all.py` runs all 13 steps in FK dependency order, prints progress and row counts, validates `global_id` integrity across all 32 tables, and cleans up temp Parquet.

Expected output:

```
========================================================
  CRM Synthetic Data Pipeline  (32 tables)
========================================================
  [1/13] CDR core ...
  [2/13] CDR extended ...
  [3/13] BPI tables ...
  [4/13] Lending Club tables ...
  [5/13] Energy tables ...
  [6/13] Banking Products Extended ...
  [7/13] CRM preferences ...
  [8/13] CRM interactions ...
  [9/13] CRM documents ...
  [10/13] CRM history ...
  [11/13] Customer Profile Extended ...
  [12/13] Org relationships ...
  [13/13] CRM case events ...
========================================================
  Validation
--------------------------------------------------------
  customers                            1,000 rows  [MASTER]
  banking_accounts                     1,500 rows  FK OK
  ...
  organisation_party_relationships     1,500 rows  FK OK
  service_case_events                  2,400 rows  FK OK
  organisation_relationships             100 rows  [CATALOGUE]
  All tables: 67,180 total rows
  global_id integrity: PASS
========================================================
  Done in ~30s
========================================================
```

### Run a single script

Each script can run independently (as long as upstream tmp Parquet files exist under `output/_tmp/`):

```bash
cd synthetic-data/scripts
python3.11 generate_cdr.py                    # step 1 -- always run first
python3.11 generate_cdr_extended.py           # step 2 (needs customers)
python3.11 generate_bpi.py                    # step 3 (needs customers)
python3.11 generate_lending_club.py           # step 4 (needs customers)
python3.11 generate_energy.py                 # step 5 (needs customers)
python3.11 generate_banking_products_ext.py   # step 6 (needs accounts)
python3.11 generate_crm_preferences.py        # step 7 (needs customers)
python3.11 generate_crm_interactions.py       # step 8 (needs customers)
python3.11 generate_crm_documents.py          # step 9 (needs applications)
python3.11 generate_crm_history.py            # step 10 (needs applications)
python3.11 generate_customer_profile_ext.py   # step 11 (needs customers)
python3.11 generate_org_relationships.py      # step 12 (needs customers + organisations)
python3.11 generate_crm_case_events.py        # step 13 (needs service_cases)
```

---

## Validate Output

### Row counts

```python
import pandas as pd, os

base = "synthetic-data/output"
for t in sorted(os.listdir(base)):
    path = f"{base}/{t}/"
    if os.path.isdir(path) and not t.startswith("_"):
        df = pd.read_parquet(path)
        print(f"{t:<35} {len(df):>10,} rows")
```

### FK integrity (`global_id` check)

```python
import pandas as pd

base = "synthetic-data/output"
customers = pd.read_parquet(f"{base}/customers/")
all_ids = set(customers["global_id"])

# All tables except catalogues (banking_products, energy_plans, organisation_relationships)
for t in sorted(os.listdir(base)):
    path = f"{base}/{t}/"
    if not os.path.isdir(path) or t.startswith("_") or t in ("banking_products", "energy_plans", "organisation_relationships"):
        continue
    df = pd.read_parquet(path)
    if "global_id" not in df.columns:
        continue
    orphans = ~df["global_id"].isin(all_ids)
    status = "PASS" if orphans.sum() == 0 else f"FAIL ({orphans.sum()} orphans)"
    print(f"{t:<35} {len(df):>8,} rows   {status}")
```

### Sample join -- business vs personal accounts

```python
import pandas as pd

base = "synthetic-data/output"
accounts = pd.read_parquet(f"{base}/banking_accounts/")
orgs     = pd.read_parquet(f"{base}/organisations/")

# Business accounts with organisation details
biz = accounts[accounts["organisationId"].notna()].merge(
    orgs[["organisationId", "businessName", "organisationType"]],
    on="organisationId", how="left"
)
print(f"Business accounts: {len(biz)}")
print(biz[["customerId", "organisationId", "businessName", "accountType", "balance"]].head())

# Personal accounts
personal = accounts[accounts["organisationId"].isna()]
print(f"\nPersonal accounts: {len(personal)}")
```

### Sample join -- answer a business question

```python
import pandas as pd

base = "synthetic-data/output"

# B2: "Has this customer contacted support recently?"
customers     = pd.read_parquet(f"{base}/customers/")
interactions  = pd.read_parquet(f"{base}/support_interactions/")

target = customers.iloc[0]["global_id"]
recent = interactions[interactions["global_id"] == target].sort_values("timestamp", ascending=False)
print(recent[["timestamp", "channel", "topic", "resolution"]].head(5))
```

---

## AI Question Coverage

All 66 business questions are answerable with the generated data:
- **37 original** (24 Banker.AI + 13 Customer.AI) -- person-centric
- **29 business customer** (BB1-BB43) -- organisation-centric

| Coverage | Original 37 | Business 29 | Combined 66 |
|----------|-------------|-------------|-------------|
| Fully covered | **37** | **29** | **66** |
| Not covered | 0 | 0 | **0** |

### Business customer questions -- resolved by

| Gap | Tables/columns that close it |
|-----|------------------------------|
| Org overview (BB1-BB5) | `organisations` + `organisationId` FK on accounts, loans, cases, KYC |
| Multi-party relationships (BB7-BB10) | `organisation_party_relationships` (directors, reps, owners, signatories) |
| Org-to-org relationships (BB11) | `organisation_relationships` (parent/subsidiary, trust, etc.) |
| KYC/KYB verification (BB13, BB16) | `kyc_records.organisationId` + `abnVerified`, `asicCheckStatus`, `beneficialOwnershipVerified` |
| Org accounts/arrangements (BB19-BB23) | `banking_accounts.organisationId` + `credit_cards.organisationId` |
| Business app status (BB26-BB35) | `loan_applications.organisationId` + `application_stage_history.pendingActionParty` |
| Org service cases (BB36, BB40-BB43) | `service_cases.organisationId/.applicationId/.slaDeadline` + `service_case_events` |

See `ref-schema/ai-questions-gap-analysis.md` for the original per-question breakdown.
See `synthetic-data/scripts/plans/reports/gap-analysis-260722-0135-business-customer-questions.md` for the business customer gap analysis.

---

## Local Execution -- Known Issues & Fixes

Fixes applied to make the originally Databricks-only scripts run in local PySpark mode.

### 1. Wrong Python version (PySpark 4.x requires Python 3.10+)

**Error:** `ImportError: cannot import name '_with_origin' from 'pyspark.errors.utils'`

**Fix:** Use Python 3.11+ (installed via Homebrew):
```bash
python3.11 run_all.py
python3.11 -m pip install pyspark faker numpy pandas pyarrow
```

### 2. Spark workers using wrong Python (pandas UDF crash)

**Error:** `EOFException occurred while reading the port number from pyspark.daemon's stdout`

**Fix:** Added to `config.py` -> `get_spark()`:
```python
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

### 3. Databricks-only APIs in all generation scripts

All scripts converted from Databricks-specific patterns to local PySpark:
- `DatabricksSession` -> `cfg.get_spark()`
- `spark.table("catalog.schema._tmp_X")` -> `spark.read.parquet(cfg.tmp_path("X_fk"))`
- `df.write.format("delta").saveAsTable(...)` -> `df.write.mode("overwrite").parquet(...)`
- `spark.sql("CREATE SCHEMA/VOLUME/DROP TABLE ...")` -> removed

### 4. Strict timestamp parsing in Spark 4.x

**Error:** `[CANNOT_PARSE_TIMESTAMP] Text '2025-07-16' could not be parsed at index 10`

**Fix:** Use full datetime format: `cfg.START_DATE.strftime("%Y-%m-%d %H:%M:%S")`

### 5. pyarrow missing (required for pandas UDFs)

**Fix:** `python3.11 -m pip install pyarrow`

---

## Reference

- Gap analysis: `ref-schema/ai-questions-gap-analysis.md`
- CRM schema: `ref-schema/crm-schema/crm-schema.md`
- Databricks skill reference: `ref-schema/databricks-synthetic-data-gen-analysis.md`
- Generation plan: `plans/260716-2230-crm-schema-missing-datasets/plan.md`
