"""Arrangement subject-area Silver models."""

from pyspark.sql import functions as F

from framework.silver_model import (
    cdc_change_stream,
    current_cdc_snapshot,
    masked_amount,
    masked_card_number,
    publish_joined_scd2_model,
    publish_scd2_model,
    trimmed,
    with_audit_columns,
)


def _single_source(dataset, columns, masking_status="CLEAN"):
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
        [f"cdc_{dataset}"],
        [F.col("_batch_id")],
        [F.col("_ingested_at")],
        masking_status,
    ).drop("_batch_id", "_ingested_at")


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


def build_arr_loan():
    return _single_source("loan_accounts", [
        F.col("global_id").cast("string").alias("global_id"),
        F.col("loanId").cast("string").alias("loan_id"),
        F.col("accountId").cast("string").alias("account_id"),
        F.upper(trimmed(F.col("loanType"))).alias("loan_type"),
        F.upper(trimmed(F.col("status"))).alias("status"),
        F.lit(None).cast("string").alias("interest_type"),
        F.upper(trimmed(F.col("repaymentFrequency"))).alias("repayment_frequency"),
        F.col("termMonths").cast("int").alias("term_months"),
        F.to_date("startDate").alias("start_date"),
        F.to_date("maturityDate").alias("maturity_date"),
        masked_amount(F.col("originalAmount")).alias("original_amount_masked"),
        masked_amount(F.col("currentBalance")).alias("current_balance_masked"),
    ], masking_status="MASKED")


def build_arr_mortgage():
    return _single_source("mortgage_accounts", [
        F.col("global_id").cast("string").alias("global_id"),
        F.col("mortgageId").cast("string").alias("mortgage_id"),
        F.col("accountId").cast("string").alias("account_id"),
        F.upper(trimmed(F.col("interestType"))).alias("interest_type"),
        F.upper(trimmed(F.col("repaymentFrequency"))).alias("repayment_frequency"),
        F.col("loanTerm").cast("int").alias("loan_term"),
        F.to_date("startDate").alias("start_date"),
        trimmed(F.col("lvrPercent")).alias("lvr_percent"),
        masked_amount(F.col("loanAmount")).alias("loan_amount_masked"),
    ], masking_status="MASKED")


def build_arr_credit_card():
    return _single_source("credit_cards", [
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


publish_joined_scd2_model("arr_banking_arrangement", build_arr_banking_arrangement, ["account_id"])
publish_scd2_model("arr_loan", build_arr_loan, ["loan_id"])
publish_scd2_model("arr_mortgage", build_arr_mortgage, ["mortgage_id"])
publish_scd2_model("arr_credit_card", build_arr_credit_card, ["credit_card_id"])
