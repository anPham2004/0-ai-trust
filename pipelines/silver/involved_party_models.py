"""Involved Party subject-area Silver models."""

from pyspark.sql import Window, functions as F

from framework.silver_model import (
    cdc_change_stream,
    current_cdc_snapshot,
    hmac_name_token,
    masked_email,
    masked_identifier,
    masked_phone,
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


def build_ip_individual():
    customers = current_cdc_snapshot("customers", ["global_id"]).alias("customer")
    preferences = current_cdc_snapshot("customer_preferences", ["customerId"]).alias("preference")

    addresses = current_cdc_snapshot("physical_addresses", ["addressId"])
    address_priority = (
        F.when(F.upper("purpose").isin("PRIMARY", "HOME", "RESIDENTIAL"), F.lit(0))
        .otherwise(F.lit(1))
    )
    address_window = Window.partitionBy("global_id").orderBy(
        address_priority,
        F.col("_commit_ts").desc_nulls_last(),
        F.col("addressId").asc(),
    )
    primary_address = (
        addresses.withColumn("_address_rank", F.row_number().over(address_window))
        .filter(F.col("_address_rank") == 1)
        .drop("_address_rank")
        .alias("address")
    )

    joined = (
        customers.join(preferences, F.col("customer.customerId") == F.col("preference.customerId"), "left")
        .join(primary_address, F.col("customer.global_id") == F.col("address.global_id"), "left")
    )
    full_name = F.concat_ws(" ", trimmed(F.col("customer.firstName")), trimmed(F.col("customer.lastName")))
    selected = joined.select(
        F.col("customer.global_id").cast("string").alias("global_id"),
        F.col("customer.customerId").cast("string").alias("customer_id"),
        F.upper(trimmed(F.col("customer.customerType"))).alias("customer_type"),
        trimmed(F.col("customer.prefix")).alias("prefix"),
        full_name.alias("full_name"),
        hmac_name_token(full_name).alias("name_token"),
        trimmed(F.col("customer.middleNames")).alias("middle_names"),
        F.upper(trimmed(F.col("customer.gender"))).alias("gender"),
        F.col("customer.age").cast("int").alias("age"),
        F.upper(trimmed(F.col("customer.state"))).alias("state"),
        trimmed(F.col("customer.occupationCode")).alias("occupation_code"),
        masked_email(F.col("customer.email")).alias("email_masked"),
        masked_phone(F.col("customer.phoneNumber")).alias("phone_masked"),
        F.upper(trimmed(F.col("preference.preferredContactChannel"))).alias("preferred_contact_channel"),
        trimmed(F.col("preference.preferredLanguage")).alias("preferred_language"),
        F.col("preference.marketingOptIn").cast("boolean").alias("marketing_opt_in"),
        trimmed(F.col("preference.satisfactionScore")).alias("satisfaction_score"),
        trimmed(F.col("address.suburb")).alias("address_suburb"),
        F.upper(trimmed(F.col("address.state"))).alias("address_state"),
        trimmed(F.col("address.postcode")).alias("address_postcode"),
        F.to_timestamp(F.col("customer.createdAt")).alias("customer_since"),
        F.to_timestamp(F.col("customer.lastUpdateTime")).alias("last_updated_at"),
        F.col("customer._batch_id").alias("_customer_batch"),
        F.col("preference._batch_id").alias("_preference_batch"),
        F.col("address._batch_id").alias("_address_batch"),
        F.col("customer._ingested_at").alias("_customer_processed"),
        F.col("preference._ingested_at").alias("_preference_processed"),
        F.col("address._ingested_at").alias("_address_processed"),
    )
    return with_audit_columns(
        selected,
        ["cdc_customers", "cdc_customer_preferences", "cdc_physical_addresses"],
        [F.col("_customer_batch"), F.col("_preference_batch"), F.col("_address_batch")],
        [F.col("_customer_processed"), F.col("_preference_processed"), F.col("_address_processed")],
        "MASKED",
    ).drop(*[column for column in selected.columns if column.startswith("_")])


def build_ip_kyc():
    source = cdc_change_stream("kyc_records")
    selected = source.select(
        F.col("global_id").cast("string").alias("global_id"),
        F.col("kycId").cast("string").alias("kyc_id"),
        F.col("organisationId").cast("string").alias("organisation_id"),
        F.when(F.col("organisationId").isNull(), F.lit("KYC")).otherwise(F.lit("KYB")).alias("record_type"),
        F.upper(trimmed(F.col("verificationStatus"))).alias("verification_status"),
        F.to_date("verificationDate").alias("verification_date"),
        F.upper(trimmed(F.col("verificationMethod"))).alias("verification_method"),
        F.upper(trimmed(F.col("riskRating"))).alias("risk_rating"),
        F.col("pepStatus").cast("boolean").alias("pep_status"),
        F.upper(trimmed(F.col("sanctionsCheck"))).alias("sanctions_check"),
        F.to_date("lastReviewDate").alias("last_review_date"),
        F.to_date("nextReviewDate").alias("next_review_date"),
        trimmed(F.col("documentTypes")).alias("document_types"),
        F.col("abnVerified").cast("boolean").alias("abn_verified"),
        F.upper(trimmed(F.col("asicCheckStatus"))).alias("asic_check_status"),
        F.col("beneficialOwnershipVerified").cast("boolean").alias("beneficial_ownership_verified"),
        F.col("_operation"),
        F.col("_sequence_ts"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return with_audit_columns(
        selected,
        ["cdc_kyc_records"],
        [F.col("_batch_id")],
        [F.col("_ingested_at")],
        "CLEAN",
    ).drop("_batch_id", "_ingested_at")


def build_ip_organisation():
    return _single_source("organisations", [
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


def build_ip_party_relationship():
    return _single_source("organisation_party_relationships", [
        F.col("relationshipId").cast("string").alias("relationship_id"),
        F.col("global_id").cast("string").alias("global_id"),
        F.col("organisationId").cast("string").alias("organisation_id"),
        F.upper(trimmed(F.col("partyRole"))).alias("party_role"),
        F.upper(trimmed(F.col("authorityLevel"))).alias("authority_level"),
        F.col("isActive").cast("boolean").alias("is_active"),
        F.to_date("startDate").alias("start_date"),
        F.to_date("endDate").alias("end_date"),
    ])


def build_ip_org_relationship():
    return _single_source("organisation_relationships", [
        F.col("relationshipId").cast("string").alias("relationship_id"),
        F.col("sourceOrgId").cast("string").alias("source_org_id"),
        F.col("targetOrgId").cast("string").alias("target_org_id"),
        F.upper(trimmed(F.col("relationshipType"))).alias("relationship_type"),
        F.col("isActive").cast("boolean").alias("is_active"),
        F.to_date("startDate").alias("start_date"),
    ])


publish_joined_scd2_model("ip_individual", build_ip_individual, ["global_id"])
publish_scd2_model("ip_kyc", build_ip_kyc, ["kyc_id"])
publish_scd2_model("ip_organisation", build_ip_organisation, ["organisation_id"])
publish_scd2_model("ip_party_relationship", build_ip_party_relationship, ["relationship_id"])
publish_scd2_model("ip_org_relationship", build_ip_org_relationship, ["relationship_id"])
