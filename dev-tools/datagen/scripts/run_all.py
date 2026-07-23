"""
Orchestrator for the multi-domain synthetic data generation pipeline (32 tables).

Run:
    python3.11 dev-tools/datagen/scripts/run_all.py

Prerequisites:
    - pip install faker numpy pandas pyspark pyarrow python-dateutil
    - Edit config.py: set OUTPUT_PATH and row counts
"""
import time, sys, shutil

import config as cfg
import generate_cdr, generate_bpi, generate_lending_club
import generate_crm_preferences, generate_crm_interactions, generate_crm_documents
import generate_crm_history
import generate_cdr_extended, generate_energy
import generate_banking_products_ext, generate_customer_profile_ext
import generate_org_relationships, generate_crm_case_events

# Tables with global_id (checked for FK integrity)
ALL_TABLES = [
    # CDR core
    ("customers",                    cfg.N_CUSTOMERS),
    ("banking_accounts",             cfg.N_ACCOUNTS),
    ("banking_transactions",         cfg.N_TRANSACTIONS),
    # BPI
    ("loan_applications",            cfg.N_APPLICATIONS),
    ("loan_application_events",      cfg.N_EVENTS),
    # Lending Club
    ("accepted_loans",               cfg.N_ACCEPTED_LOANS),
    ("rejected_applications",        cfg.N_REJECTED_APPS),
    # CDR Extended (with global_id)
    ("organisations",                cfg.N_ORGANISATIONS),
    ("physical_addresses",           cfg.N_ADDRESSES),
    ("banking_balances",             cfg.N_BANKING_BALANCES),
    ("banking_direct_debits",        cfg.N_DIRECT_DEBITS),
    ("banking_payees",               cfg.N_PAYEES),
    ("banking_scheduled_payments",   cfg.N_SCHEDULED_PAYMENTS),
    # Energy (with global_id)
    ("energy_accounts",              cfg.N_ENERGY_ACCOUNTS),
    ("energy_service_points",        cfg.N_ENERGY_SVC_POINTS),
    ("energy_invoices",              cfg.N_ENERGY_INVOICES),
    # Banking Products Extended
    ("mortgage_accounts",            cfg.N_MORTGAGE_ACCOUNTS),
    ("loan_accounts",                cfg.N_LOAN_ACCOUNTS),
    ("credit_cards",                 cfg.N_CREDIT_CARDS),
    # CRM
    ("customer_preferences",         cfg.N_PREFERENCES),
    ("support_interactions",         cfg.N_INTERACTIONS),
    ("service_cases",                cfg.N_CASES),
    ("missing_documents",            cfg.N_DOCUMENTS),
    ("application_stage_history",    cfg.N_STAGE_HISTORY),
    ("status_change_history",        cfg.N_STATUS_CHANGES),
    # Customer Profile Extended
    ("insurance_policies",           cfg.N_INSURANCE_POLICIES),
    ("kyc_records",                  cfg.N_KYC_RECORDS),
    # Organisation Relationships
    ("organisation_party_relationships", cfg.N_ORG_PARTY_RELS),
    # CRM Case Events
    ("service_case_events",          cfg.N_CASE_EVENTS),
]

# Catalogue tables (no global_id — excluded from FK integrity check)
_CATALOGUE_TABLES = [
    ("banking_products",             cfg.N_BANKING_PRODUCTS),
    ("energy_plans",                 cfg.N_ENERGY_PLANS),
    ("organisation_relationships",   cfg.N_ORG_RELATIONSHIPS),
]

SEP = "=" * 56


def _run_step(label, fn, *args):
    """Run one generation step; catch and report errors without stopping pipeline."""
    t0 = time.time()
    try:
        fn(*args)
        print(f"  {label} done in {time.time()-t0:.1f}s")
        return True
    except Exception as exc:
        import traceback
        print(f"  {label} ERROR after {time.time()-t0:.1f}s: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return False


def _validate(spark):
    """Read output Parquet dirs, count rows, verify global_id FK integrity."""
    print(SEP, "  Validation", "-" * 56, sep="\n")
    try:
        master_df = spark.read.parquet(f"{cfg.OUTPUT_PATH}/customers").select("global_id")
        master_count = master_df.count()
        master_set = {r.global_id for r in master_df.collect()}
        print(f"  {'customers':<35} {master_count:>10,} rows  [MASTER]")
    except Exception as exc:
        print(f"  customers CANNOT READ: {exc}", file=sys.stderr); return

    total, all_ok = master_count, True

    # Validate tables with global_id (FK check)
    for name, _ in ALL_TABLES[1:]:
        try:
            df = spark.read.parquet(f"{cfg.OUTPUT_PATH}/{name}")
            count = df.count(); total += count
            orphans = {r.global_id for r in df.select("global_id").distinct().collect()} - master_set
            ok = not orphans
            if not ok: all_ok = False
            status = "FK OK" if ok else f"FK FAIL ({len(orphans)} orphans)"
            print(f"  {name:<35} {count:>10,} rows  {status}")
        except Exception as exc:
            print(f"  {name:<35} MISSING/ERROR: {exc}", file=sys.stderr); all_ok = False

    # Validate catalogue tables (no FK check)
    for name, _ in _CATALOGUE_TABLES:
        try:
            df = spark.read.parquet(f"{cfg.OUTPUT_PATH}/{name}")
            count = df.count(); total += count
            print(f"  {name:<35} {count:>10,} rows  [CATALOGUE]")
        except Exception as exc:
            print(f"  {name:<35} MISSING/ERROR: {exc}", file=sys.stderr); all_ok = False

    print("-" * 56)
    print(f"  All tables: {total:,} total rows")
    print(f"  global_id integrity: {'PASS' if all_ok else 'FAIL'}")


def _cleanup():
    try:
        shutil.rmtree(cfg.TMP_PATH, ignore_errors=True)
        print(f"  cleanup: removed {cfg.TMP_PATH}")
    except Exception as exc:
        print(f"  cleanup: {exc}", file=sys.stderr)


def main():
    t0 = time.time()
    print(SEP, "  CRM Synthetic Data Pipeline  (32 tables)", SEP, sep="\n")

    spark = cfg.get_spark()

    _run_step("[1/13] CDR core (customers, banking_accounts, banking_transactions)",
              generate_cdr.run, spark, cfg)
    _run_step("[2/13] CDR extended (organisations, physical_addresses, banking_products, "
              "banking_balances, banking_direct_debits, banking_payees, banking_scheduled_payments)",
              generate_cdr_extended.run, spark, cfg)
    _run_step("[3/13] BPI tables (loan_applications, loan_application_events)",
              generate_bpi.run, spark, cfg)
    _run_step("[4/13] Lending Club tables (accepted_loans, rejected_applications)",
              generate_lending_club.run, spark, cfg)
    _run_step("[5/13] Energy tables (energy_plans, energy_accounts, energy_service_points, energy_invoices)",
              generate_energy.run, spark, cfg)
    _run_step("[6/13] Banking Products Extended (mortgage_accounts, loan_accounts, credit_cards)",
              generate_banking_products_ext.run, spark, cfg)
    _run_step("[7/13] CRM preferences (customer_preferences)",
              generate_crm_preferences.run, spark, cfg)
    _run_step("[8/13] CRM interactions (support_interactions, service_cases)",
              generate_crm_interactions.run, spark, cfg)
    _run_step("[9/13] CRM documents (missing_documents)",
              generate_crm_documents.run, spark, cfg)
    _run_step("[10/13] CRM history (application_stage_history, status_change_history)",
              generate_crm_history.run, spark, cfg)
    _run_step("[11/13] Customer Profile Extended (insurance_policies, kyc_records)",
              generate_customer_profile_ext.run, spark, cfg)
    _run_step("[12/13] Org relationships (organisation_party_relationships, organisation_relationships)",
              generate_org_relationships.run, spark, cfg)
    _run_step("[13/13] CRM case events (service_case_events)",
              generate_crm_case_events.run, spark, cfg)

    _validate(spark)
    _cleanup()

    print(SEP, f"  Done in {time.time()-t0:.1f}s", SEP, sep="\n")
    spark.stop()


if __name__ == "__main__":
    main()
