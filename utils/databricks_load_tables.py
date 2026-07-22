# Databricks notebook — Load synthetic CRM data from Parquet → Delta
# Upload the /output folder to a Databricks Volume, then run this notebook.
#
# Usage:
#   1. Upload  synthetic-data/output/  to a Databricks Volume
#   2. Set VOLUME_PATH, CATALOG, SCHEMA below
#   3. Run All

# COMMAND ----------
# ── Config ────────────────────────────────────────────────────────────────────

VOLUME_PATH = "/Volumes/main/crm_synthetic/raw"   # ← path where output/ was uploaded
CATALOG     = "main"                               # ← catalog to create tables in
SCHEMA      = "crm_synthetic"                      # ← schema to create tables in

T = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------
# ── Step 1: Create schema + 32 Delta tables from Parquet ──────────────────────

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {T}")

# (table_name, has_global_id)
ALL_TABLES = [
    # CDR core
    "customers",
    "banking_accounts",
    "banking_transactions",
    # CDR extended
    "organisations",
    "physical_addresses",
    "banking_balances",
    "banking_direct_debits",
    "banking_payees",
    "banking_scheduled_payments",
    # BPI
    "loan_applications",
    "loan_application_events",
    # Lending Club
    "accepted_loans",
    "rejected_applications",
    # Energy
    "energy_accounts",
    "energy_service_points",
    "energy_invoices",
    # Banking products extended
    "mortgage_accounts",
    "loan_accounts",
    "credit_cards",
    # CRM
    "customer_preferences",
    "support_interactions",
    "service_cases",
    "missing_documents",
    "application_stage_history",
    "status_change_history",
    # Customer profile extended
    "insurance_policies",
    "kyc_records",
    # Org relationships
    "organisation_party_relationships",
    "service_case_events",
    # Catalogues (no global_id)
    "banking_products",
    "energy_plans",
    "organisation_relationships",
]

print("Creating tables...")
total_rows = 0
for t in ALL_TABLES:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T}.{t}
        USING DELTA
        AS SELECT * FROM parquet.`{VOLUME_PATH}/{t}/`
    """)
    count = spark.table(f"{T}.{t}").count()
    total_rows += count
    print(f"  {t:<40} {count:>10,} rows")

print(f"\n  Total: {total_rows:,} rows across {len(ALL_TABLES)} tables")

# COMMAND ----------
# ── Step 2: Set PK columns NOT NULL ───────────────────────────────────────────

not_nulls = [
    # CDR core
    (f"{T}.customers",                 "global_id"),
    (f"{T}.banking_accounts",          "accountId"),
    (f"{T}.banking_transactions",      "transactionId"),
    # CDR extended
    (f"{T}.organisations",             "organisationId"),
    (f"{T}.physical_addresses",        "addressId"),
    (f"{T}.banking_balances",          "accountId"),       # 1:1 PK is accountId
    (f"{T}.banking_direct_debits",     "directDebitId"),
    (f"{T}.banking_payees",            "payeeId"),
    (f"{T}.banking_scheduled_payments","scheduledPaymentId"),
    # BPI
    (f"{T}.loan_applications",         "applicationId"),
    (f"{T}.loan_application_events",   "eventId"),
    # Lending Club
    (f"{T}.accepted_loans",            "loanId"),
    (f"{T}.rejected_applications",     "loanId"),
    # Energy
    (f"{T}.energy_accounts",           "energyAccountId"),
    (f"{T}.energy_service_points",     "servicePointId"),
    (f"{T}.energy_invoices",           "invoiceNumber"),
    # Banking products extended
    (f"{T}.mortgage_accounts",         "mortgageId"),
    (f"{T}.loan_accounts",             "loanId"),
    (f"{T}.credit_cards",              "creditCardId"),
    # CRM
    (f"{T}.customer_preferences",      "global_id"),       # 1:1 PK is global_id
    (f"{T}.support_interactions",      "interactionId"),
    (f"{T}.service_cases",             "caseId"),
    (f"{T}.missing_documents",         "documentId"),
    (f"{T}.application_stage_history", "historyId"),
    (f"{T}.status_change_history",     "changeId"),
    # Customer profile extended
    (f"{T}.insurance_policies",                  "policyId"),
    (f"{T}.kyc_records",                         "kycId"),
    # Org relationships
    (f"{T}.organisation_party_relationships",     "relationshipId"),
    (f"{T}.service_case_events",                  "eventId"),
    # Catalogues
    (f"{T}.banking_products",                    "productId"),
    (f"{T}.energy_plans",                        "planId"),
    (f"{T}.organisation_relationships",          "relationshipId"),
]

print("Setting NOT NULL...")
for table, col in not_nulls:
    try:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL")
        print(f"  OK   {table.split('.')[-1]}.{col}")
    except Exception as e:
        print(f"  SKIP {table.split('.')[-1]}.{col}: {e}")

# COMMAND ----------
# ── Step 3: Primary key constraints ───────────────────────────────────────────

pks = [
    # CDR core
    (f"{T}.customers",                 "pk_customers",               "global_id"),
    (f"{T}.banking_accounts",          "pk_banking_accounts",        "accountId"),
    (f"{T}.banking_transactions",      "pk_banking_transactions",    "transactionId"),
    # CDR extended
    (f"{T}.organisations",             "pk_organisations",           "organisationId"),
    (f"{T}.physical_addresses",        "pk_physical_addresses",      "addressId"),
    (f"{T}.banking_balances",          "pk_banking_balances",        "accountId"),
    (f"{T}.banking_direct_debits",     "pk_banking_direct_debits",   "directDebitId"),
    (f"{T}.banking_payees",            "pk_banking_payees",          "payeeId"),
    (f"{T}.banking_scheduled_payments","pk_banking_scheduled_payments","scheduledPaymentId"),
    # BPI
    (f"{T}.loan_applications",         "pk_loan_applications",       "applicationId"),
    (f"{T}.loan_application_events",   "pk_loan_application_events", "eventId"),
    # Lending Club
    (f"{T}.accepted_loans",            "pk_accepted_loans",          "loanId"),
    (f"{T}.rejected_applications",     "pk_rejected_applications",   "loanId"),
    # Energy
    (f"{T}.energy_accounts",           "pk_energy_accounts",         "energyAccountId"),
    (f"{T}.energy_service_points",     "pk_energy_service_points",   "servicePointId"),
    (f"{T}.energy_invoices",           "pk_energy_invoices",         "invoiceNumber"),
    # Banking products extended
    (f"{T}.mortgage_accounts",         "pk_mortgage_accounts",       "mortgageId"),
    (f"{T}.loan_accounts",             "pk_loan_accounts",           "loanId"),
    (f"{T}.credit_cards",              "pk_credit_cards",            "creditCardId"),
    # CRM
    (f"{T}.customer_preferences",      "pk_customer_preferences",    "global_id"),
    (f"{T}.support_interactions",      "pk_support_interactions",    "interactionId"),
    (f"{T}.service_cases",             "pk_service_cases",           "caseId"),
    (f"{T}.missing_documents",         "pk_missing_documents",       "documentId"),
    (f"{T}.application_stage_history", "pk_application_stage_history","historyId"),
    (f"{T}.status_change_history",     "pk_status_change_history",   "changeId"),
    # Customer profile extended
    (f"{T}.insurance_policies",                 "pk_insurance_policies",                  "policyId"),
    (f"{T}.kyc_records",                        "pk_kyc_records",                         "kycId"),
    # Org relationships
    (f"{T}.organisation_party_relationships",   "pk_organisation_party_relationships",    "relationshipId"),
    (f"{T}.service_case_events",                "pk_service_case_events",                 "eventId"),
    # Catalogues
    (f"{T}.banking_products",                   "pk_banking_products",                    "productId"),
    (f"{T}.energy_plans",                       "pk_energy_plans",                        "planId"),
    (f"{T}.organisation_relationships",         "pk_organisation_relationships",          "relationshipId"),
]

print("Adding primary keys...")
for table, name, col in pks:
    try:
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} PRIMARY KEY ({col})")
        print(f"  OK   {name}")
    except Exception as e:
        print(f"  SKIP {name}: {e}")

# COMMAND ----------
# ── Step 4: Foreign key constraints ───────────────────────────────────────────
# Note: Databricks FK constraints are informational (not enforced at write time).
# They document relationships and are used by query optimizers.

fks = [
    # ── global_id → customers ─────────────────────────────────────────────────
    (f"{T}.banking_accounts",          "fk_banking_accounts_customer",          "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.banking_transactions",      "fk_banking_transactions_customer",      "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.organisations",             "fk_organisations_customer",             "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.physical_addresses",        "fk_physical_addresses_customer",        "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.banking_balances",          "fk_banking_balances_customer",          "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.banking_direct_debits",     "fk_banking_direct_debits_customer",     "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.banking_payees",            "fk_banking_payees_customer",            "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.banking_scheduled_payments","fk_banking_scheduled_payments_customer","global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.loan_applications",         "fk_loan_applications_customer",         "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.loan_application_events",   "fk_loan_application_events_customer",   "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.accepted_loans",            "fk_accepted_loans_customer",            "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.rejected_applications",     "fk_rejected_applications_customer",     "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.energy_accounts",           "fk_energy_accounts_customer",           "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.energy_service_points",     "fk_energy_service_points_customer",     "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.energy_invoices",           "fk_energy_invoices_customer",           "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.mortgage_accounts",         "fk_mortgage_accounts_customer",         "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.loan_accounts",             "fk_loan_accounts_customer",             "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.credit_cards",              "fk_credit_cards_customer",              "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.customer_preferences",      "fk_customer_preferences_customer",      "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.support_interactions",      "fk_support_interactions_customer",      "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.service_cases",             "fk_service_cases_customer",             "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.missing_documents",         "fk_missing_documents_customer",         "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.application_stage_history", "fk_application_stage_history_customer", "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.status_change_history",     "fk_status_change_history_customer",     "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.insurance_policies",        "fk_insurance_policies_customer",        "global_id",       f"{T}.customers",          "global_id"),
    (f"{T}.kyc_records",                        "fk_kyc_records_customer",                       "global_id",       f"{T}.customers",       "global_id"),
    (f"{T}.organisation_party_relationships",   "fk_org_party_relationships_customer",           "global_id",       f"{T}.customers",       "global_id"),
    (f"{T}.service_case_events",                "fk_service_case_events_customer",               "global_id",       f"{T}.customers",       "global_id"),

    # ── accountId → banking_accounts ──────────────────────────────────────────
    (f"{T}.banking_transactions",      "fk_banking_transactions_account",       "accountId",       f"{T}.banking_accounts",   "accountId"),
    (f"{T}.banking_balances",          "fk_banking_balances_account",           "accountId",       f"{T}.banking_accounts",   "accountId"),
    (f"{T}.banking_direct_debits",     "fk_banking_direct_debits_account",      "accountId",       f"{T}.banking_accounts",   "accountId"),
    (f"{T}.banking_scheduled_payments","fk_banking_scheduled_payments_account", "accountId",       f"{T}.banking_accounts",   "accountId"),
    (f"{T}.mortgage_accounts",         "fk_mortgage_accounts_account",          "accountId",       f"{T}.banking_accounts",   "accountId"),
    (f"{T}.loan_accounts",             "fk_loan_accounts_account",              "accountId",       f"{T}.banking_accounts",   "accountId"),

    # ── applicationId → loan_applications ─────────────────────────────────────
    (f"{T}.loan_application_events",   "fk_loan_application_events_app",        "applicationId",   f"{T}.loan_applications",  "applicationId"),
    (f"{T}.missing_documents",         "fk_missing_documents_app",              "applicationId",   f"{T}.loan_applications",  "applicationId"),
    (f"{T}.application_stage_history", "fk_application_stage_history_app",      "applicationId",   f"{T}.loan_applications",  "applicationId"),
    (f"{T}.status_change_history",     "fk_status_change_history_app",          "applicationId",   f"{T}.loan_applications",  "applicationId"),

    # ── energyAccountId → energy_accounts ─────────────────────────────────────
    (f"{T}.energy_service_points",     "fk_energy_service_points_account",      "energyAccountId", f"{T}.energy_accounts",   "energyAccountId"),
    (f"{T}.energy_invoices",           "fk_energy_invoices_account",            "energyAccountId", f"{T}.energy_accounts",   "energyAccountId"),

    # ── caseId → service_cases ────────────────────────────────────────────────
    (f"{T}.service_case_events",       "fk_service_case_events_case",           "caseId",          f"{T}.service_cases",     "caseId"),
]

print("Adding foreign keys...")
for table, name, col, ref_table, ref_col in fks:
    try:
        spark.sql(f"""
            ALTER TABLE {table}
            ADD CONSTRAINT {name}
            FOREIGN KEY ({col}) REFERENCES {ref_table}({ref_col})
        """)
        print(f"  OK   {name}")
    except Exception as e:
        print(f"  SKIP {name}: {e}")

# COMMAND ----------
# ── Step 5: Verify row counts and FK coverage ──────────────────────────────────

print(f"\n{'Table':<40} {'Rows':>10}  PK")
print("-" * 60)
for t in ALL_TABLES:
    count = spark.table(f"{T}.{t}").count()
    pk = next((col for _, name, col in pks if name == f"pk_{t}"), "—")
    print(f"  {t:<38} {count:>10,}  {pk}")
