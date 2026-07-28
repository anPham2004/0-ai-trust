"""Source-aligned dataset registry used to declare Bronze streaming tables."""


CDC_DATASETS = {
    "banking_accounts": "banking_core__banking_accounts",
    "banking_balances": "banking_core__banking_balances",
    "banking_direct_debits": "banking_core__banking_direct_debits",
    "banking_payees": "banking_core__banking_payees",
    "banking_scheduled_payments": "banking_core__banking_scheduled_payments",
    "banking_transactions": "banking_core__banking_transactions",
    "credit_cards": "banking_core__credit_cards",
    "customer_preferences": "customer_master__customer_preferences",
    "customers": "customer_master__customers",
    "energy_accounts": "energy_core__energy_accounts",
    "energy_invoices": "energy_core__energy_invoices",
    "energy_service_points": "energy_core__energy_service_points",
    "insurance_policies": "insurance_core__insurance_policies",
    "kyc_records": "customer_master__kyc_records",
    "loan_accounts": "lending_servicing__loan_accounts",
    "loan_applications": "lending_origination__loan_applications",
    "missing_documents": "lending_origination__missing_documents",
    "mortgage_accounts": "lending_servicing__mortgage_accounts",
    "organisation_party_relationships": "organisation_master__organisation_party_relationships",
    "organisation_relationships": "organisation_master__organisation_relationships",
    "organisations": "organisation_master__organisations",
    "physical_addresses": "customer_master__physical_addresses",
    "service_cases": "case_management__service_cases",
}

EVENT_DATASETS = (
    "application_stage_history",
    "loan_application_events",
    "service_case_events",
    "status_change_history",
    "support_interactions",
)

FILE_DATASETS = (
    "accepted_loans",
    "banking_products",
    "energy_plans",
    "rejected_applications",
)


def bronze_table_names() -> tuple[str, ...]:
    return tuple(
        [f"cdc_{name}" for name in CDC_DATASETS]
        + [f"event_{name}" for name in EVENT_DATASETS]
        + [f"file_{name}" for name in FILE_DATASETS]
    )
