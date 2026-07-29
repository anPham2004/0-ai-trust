"""Organisation and arrangement Silver entities."""

from pyspark.sql import functions as F

from framework.silver_model import (
    cdc_change_stream,
    current_cdc_snapshot,
    masked_amount,
    masked_card_number,
    masked_identifier,
    publish_joined_scd2_model,
    publish_scd2_model,
    trimmed,
    with_audit_columns,
)


def _single_source(dataset, key, columns, source_name=None, masking_status="CLEAN"):
    source = cdc_change_stream(dataset)
    selected = source.select(
        *columns,
        F.col("_operation"),
        F.col("_sequence_ts"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return with_audit_columns(
        selected,
        [source_name or f"cdc_{dataset}"],
        [F.col("_batch_id")],
        [F.col("_ingested_at")],
        masking_status,
    ).drop("_batch_id", "_ingested_at")


def build_ip_organisation():
    return _single_source("organisations", "organisationId", [
        F.col("global_id").cast("string").alias("global_id"),
        F.col("organisationId").cast("string").alias("organisation_id"),
        trimmed(F.col("businessName")).alias("business_name"),
        trimmed(F.col("legalName")).alias("legal_name"),
        trimmed(F.col("shortName")).alias("short_name"),
        F.upper(trimmed(F.col("organisationType"))).alias("organisation_type"),
        trimmed(F.col("industryCode")).alias("industry_code"),
        trimmed(F.col("industryCodeVersion")).alias("industry_code_version"),
        F.upper(trimmed(F.col("registeredCountry"))).alias("registered_country"),
        F.col("isACNCRegistered").cast("boolean").alias("is_acnc_registered"),
        F.upper(trimmed(F.col("agentRole"))).alias("agent_role"),
        masked_identifier(F.col("abn")).alias("abn_masked"),
        masked_identifier(F.col("acn")).alias("acn_masked"),
        F.to_date("establishmentDate").alias("establishment_date"),
        F.to_timestamp("lastUpdateTime").alias("last_updated_at"),
    ], masking_status="MASKED")


def build_ip_organisation_party_relationship():
    return _single_source("organisation_party_relationships", "relationshipId", [
        F.col("relationshipId").cast("string").alias("relationship_id"),
        F.col("global_id").cast("string").alias("global_id"),
        F.col("organisationId").cast("string").alias("organisation_id"),
        F.upper(trimmed(F.col("partyRole"))).alias("party_role"),
        F.upper(trimmed(F.col("authorityLevel"))).alias("authority_level"),
        F.col("isActive").cast("boolean").alias("is_active"),
        F.to_date("startDate").alias("relationship_start_date"),
        F.to_date("endDate").alias("relationship_end_date"),
    ])


def build_ip_organisation_relationship():
    return _single_source("organisation_relationships", "relationshipId", [
        F.col("relationshipId").cast("string").alias("relationship_id"),
        F.col("sourceOrgId").cast("string").alias("source_org_id"),
        F.col("targetOrgId").cast("string").alias("target_org_id"),
        F.upper(trimmed(F.col("relationshipType"))).alias("relationship_type"),
        F.col("isActive").cast("boolean").alias("is_active"),
        F.to_date("startDate").alias("relationship_start_date"),
    ])


def build_arr_banking_arrangement():
    accounts = current_cdc_snapshot("banking_accounts", ["accountId"]).alias("account")
    balances = current_cdc_snapshot("banking_balances", ["accountId"]).alias("balance")
    selected = accounts.join(balances, "accountId", "left").select(
        F.col("account.global_id").cast("string").alias("global_id"),
        F.col("accountId").cast("string").alias("account_id"),
        F.col("account.organisationId").cast("string").alias("organisation_id"),
        F.col("account.organisationId").isNotNull().alias("is_business_arrangement"),
        F.upper(trimmed(F.col("account.accountType"))).alias("account_type"),
        F.col("account.isActive").cast("boolean").alias("is_active"),
        F.col("account.isOwner").cast("boolean").alias("is_owner"),
        F.to_date(F.col("account.openDate")).alias("open_date"),
        masked_amount(F.col("balance.currentBalance")).alias("current_balance_masked"),
        masked_amount(F.col("balance.availableBalance")).alias("available_balance_masked"),
        F.upper(trimmed(F.col("balance.currency"))).alias("currency"),
        F.col("account._batch_id").alias("_account_batch"),
        F.col("balance._batch_id").alias("_balance_batch"),
        F.col("account._ingested_at").alias("_account_processed"),
        F.col("balance._ingested_at").alias("_balance_processed"),
    )
    return with_audit_columns(
        selected,
        ["cdc_banking_accounts", "cdc_banking_balances"],
        [F.col("_account_batch"), F.col("_balance_batch")],
        [F.col("_account_processed"), F.col("_balance_processed")],
        "MASKED",
    ).drop("_account_batch", "_balance_batch", "_account_processed", "_balance_processed")


def _account_owner_lookup():
    return current_cdc_snapshot("banking_accounts", ["accountId"]).select(
        F.col("accountId").alias("_owner_account_id"),
        F.col("organisationId").alias("_derived_organisation_id"),
        F.col("_batch_id").alias("_owner_batch"),
        F.col("_ingested_at").alias("_owner_processed"),
    )


def build_arr_loan_arrangement():
    loans = current_cdc_snapshot("loan_accounts", ["loanId"]).alias("loan")
    owners = _account_owner_lookup().alias("owner")
    selected = loans.join(owners, F.col("loan.accountId") == F.col("owner._owner_account_id"), "left").select(
        F.col("loan.global_id").cast("string").alias("global_id"),
        F.col("loan.loanId").cast("string").alias("loan_id"),
        F.col("loan.accountId").cast("string").alias("account_id"),
        F.col("owner._derived_organisation_id").cast("string").alias("organisation_id"),
        F.col("owner._derived_organisation_id").isNotNull().alias("is_business_arrangement"),
        F.upper(trimmed(F.col("loan.loanType"))).alias("loan_type"),
        F.upper(trimmed(F.col("loan.status"))).alias("status"),
        F.lit(None).cast("string").alias("interest_type"),
        F.upper(trimmed(F.col("loan.repaymentFrequency"))).alias("repayment_frequency"),
        F.col("loan.termMonths").cast("int").alias("term_months"),
        F.to_date(F.col("loan.startDate")).alias("loan_start_date"),
        F.to_date(F.col("loan.maturityDate")).alias("maturity_date"),
        masked_amount(F.col("loan.originalAmount")).alias("original_amount_masked"),
        masked_amount(F.col("loan.currentBalance")).alias("current_balance_masked"),
        F.col("loan._batch_id").alias("_loan_batch"),
        F.col("owner._owner_batch"),
        F.col("loan._ingested_at").alias("_loan_processed"),
        F.col("owner._owner_processed"),
    )
    return with_audit_columns(
        selected,
        ["cdc_banking_accounts", "cdc_loan_accounts"],
        [F.col("_loan_batch"), F.col("_owner_batch")],
        [F.col("_loan_processed"), F.col("_owner_processed")],
        "MASKED",
    ).drop("_loan_batch", "_owner_batch", "_loan_processed", "_owner_processed")


def build_arr_mortgage_arrangement():
    mortgages = current_cdc_snapshot("mortgage_accounts", ["mortgageId"]).alias("mortgage")
    owners = _account_owner_lookup().alias("owner")
    selected = mortgages.join(owners, F.col("mortgage.accountId") == F.col("owner._owner_account_id"), "left").select(
        F.col("mortgage.global_id").cast("string").alias("global_id"),
        F.col("mortgage.mortgageId").cast("string").alias("mortgage_id"),
        F.col("mortgage.accountId").cast("string").alias("account_id"),
        F.col("owner._derived_organisation_id").cast("string").alias("organisation_id"),
        F.col("owner._derived_organisation_id").isNotNull().alias("is_business_arrangement"),
        F.upper(trimmed(F.col("mortgage.interestType"))).alias("interest_type"),
        F.upper(trimmed(F.col("mortgage.repaymentFrequency"))).alias("repayment_frequency"),
        F.col("mortgage.loanTerm").cast("int").alias("loan_term"),
        F.to_date(F.col("mortgage.startDate")).alias("mortgage_start_date"),
        trimmed(F.col("mortgage.lvrPercent")).alias("lvr_percent"),
        masked_amount(F.col("mortgage.loanAmount")).alias("loan_amount_masked"),
        F.col("mortgage._batch_id").alias("_mortgage_batch"),
        F.col("owner._owner_batch"),
        F.col("mortgage._ingested_at").alias("_mortgage_processed"),
        F.col("owner._owner_processed"),
    )
    return with_audit_columns(
        selected,
        ["cdc_banking_accounts", "cdc_mortgage_accounts"],
        [F.col("_mortgage_batch"), F.col("_owner_batch")],
        [F.col("_mortgage_processed"), F.col("_owner_processed")],
        "MASKED",
    ).drop("_mortgage_batch", "_owner_batch", "_mortgage_processed", "_owner_processed")


def build_arr_credit_card_arrangement():
    return _single_source("credit_cards", "creditCardId", [
        F.col("global_id").cast("string").alias("global_id"),
        F.col("creditCardId").cast("string").alias("credit_card_id"),
        F.col("organisationId").cast("string").alias("organisation_id"),
        F.col("organisationId").isNotNull().alias("is_business_arrangement"),
        F.upper(trimmed(F.col("cardType"))).alias("card_type"),
        F.upper(trimmed(F.col("status"))).alias("status"),
        trimmed(F.col("rewardProgram")).alias("reward_program"),
        F.to_date("issuedDate").alias("issued_date"),
        masked_card_number(F.col("cardNumber")).alias("card_number_masked"),
        masked_amount(F.col("creditLimit")).alias("credit_limit_masked"),
    ], masking_status="MASKED")


publish_scd2_model("ip_organisation", build_ip_organisation, ["organisation_id"])
publish_scd2_model("ip_organisation_party_relationship", build_ip_organisation_party_relationship, ["relationship_id"])
publish_scd2_model("ip_organisation_relationship", build_ip_organisation_relationship, ["relationship_id"])
publish_joined_scd2_model("arr_banking_arrangement", build_arr_banking_arrangement, ["account_id"])
publish_joined_scd2_model("arr_loan_arrangement", build_arr_loan_arrangement, ["loan_id"])
publish_joined_scd2_model("arr_mortgage_arrangement", build_arr_mortgage_arrangement, ["mortgage_id"])
publish_scd2_model("arr_credit_card_arrangement", build_arr_credit_card_arrangement, ["credit_card_id"])
