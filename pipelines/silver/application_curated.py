"""Application Silver entities."""

from pyspark.sql import Window, functions as F

from framework.silver_model import (
    latest_cdc,
    latest_event,
    latest_file,
    masked_amount,
    publish_silver_model,
    trimmed,
    with_audit_columns,
)


def _with_single_source_audit(dataframe, source_table, masking_status="CLEAN"):
    return with_audit_columns(
        dataframe,
        [source_table],
        [F.col("_batch_id")],
        [F.col("_ingested_at")],
        masking_status,
    ).drop("_batch_id", "_ingested_at")


def build_app_application():
    source = latest_cdc("loan_applications", ["applicationId"])
    selected = source.select(
        F.col("global_id").cast("string").alias("global_id"),
        F.col("applicationId").cast("string").alias("application_id"),
        F.col("organisationId").cast("string").alias("organisation_id"),
        F.col("organisationId").isNotNull().alias("is_business_application"),
        trimmed(F.col("numOffers")).alias("num_offers"),
        trimmed(F.col("loanGoal")).alias("loan_goal"),
        F.upper(trimmed(F.col("applicationType"))).alias("application_type"),
        F.upper(trimmed(F.col("finalOutcome"))).alias("final_outcome"),
        F.to_timestamp("submittedAt").alias("submitted_at"),
        F.to_timestamp("lastUpdatedAt").alias("last_updated_at"),
        masked_amount(F.col("requestedAmount")).alias("requested_amount_masked"),
        F.col("creditScore").cast("int").alias("credit_score"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_single_source_audit(selected, "cdc_loan_applications", "MASKED")


def build_app_application_stage_history():
    source = latest_event("application_stage_history", "historyId", "enteredAt")
    current_window = Window.partitionBy("applicationId").orderBy(
        F.to_timestamp("enteredAt").desc_nulls_last(),
        F.col("_kafka_offset").desc_nulls_last(),
        F.col("historyId").desc(),
    )
    selected = source.withColumn("_stage_rank", F.row_number().over(current_window)).select(
        F.col("historyId").cast("string").alias("history_id"),
        F.col("applicationId").cast("string").alias("application_id"),
        F.col("global_id").cast("string").alias("global_id"),
        F.upper(trimmed(F.col("stage"))).alias("stage"),
        F.to_timestamp("enteredAt").alias("entered_at"),
        F.to_timestamp("exitedAt").alias("exited_at"),
        F.when(F.col("exitedAt").isNotNull(), F.datediff(F.to_date("exitedAt"), F.to_date("enteredAt"))).cast("int").alias("duration_days"),
        (F.col("_stage_rank") == 1).alias("is_current_stage"),
        trimmed(F.col("assignedTeam")).alias("assigned_team"),
        F.to_timestamp("slaDeadline").alias("sla_deadline"),
        (F.to_timestamp("slaDeadline").isNotNull() & (F.current_timestamp() > F.to_timestamp("slaDeadline")) & (F.col("_stage_rank") == 1)).alias("is_sla_breached"),
        F.upper(trimmed(F.col("pendingActionParty"))).alias("pending_action_party"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_single_source_audit(selected, "event_application_stage_history")


def build_app_status_change_history():
    source = latest_event("status_change_history", "changeId", "changedAt")
    selected = source.select(
        F.col("changeId").cast("string").alias("change_id"),
        F.col("applicationId").cast("string").alias("application_id"),
        F.col("global_id").cast("string").alias("global_id"),
        F.upper(trimmed(F.col("oldStatus"))).alias("old_status"),
        F.upper(trimmed(F.col("newStatus"))).alias("new_status"),
        F.to_timestamp("changedAt").alias("changed_at"),
        trimmed(F.col("reason")).alias("reason"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_single_source_audit(selected, "event_status_change_history")


def build_app_document():
    source = latest_cdc("missing_documents", ["documentId"])
    expiry = F.to_date("expiryDate")
    status = F.upper(trimmed(F.col("status")))
    selected = source.select(
        F.col("documentId").cast("string").alias("document_id"),
        F.col("applicationId").cast("string").alias("application_id"),
        F.col("global_id").cast("string").alias("global_id"),
        trimmed(F.col("documentType")).alias("document_type"),
        status.alias("status"),
        ((status == "REJECTED") | (expiry.isNotNull() & (expiry < F.current_date()))).alias("is_invalid_or_expired"),
        F.to_timestamp("requestedAt").alias("requested_at"),
        F.to_timestamp("receivedAt").alias("received_at"),
        expiry.alias("expiry_date"),
        F.col("remindersSent").cast("int").alias("reminders_sent"),
        F.to_timestamp("lastReminderAt").alias("last_reminder_at"),
        trimmed(F.col("rejectionReason")).alias("rejection_reason"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_single_source_audit(selected, "cdc_missing_documents")


def build_app_application_event():
    source = latest_event("loan_application_events", "eventId", "timestamp")
    selected = source.select(
        F.col("eventId").cast("string").alias("event_id"),
        F.col("applicationId").cast("string").alias("application_id"),
        F.col("global_id").cast("string").alias("global_id"),
        trimmed(F.col("conceptName")).alias("concept_name"),
        trimmed(F.col("lifecycleTransition")).alias("lifecycle_transition"),
        F.to_timestamp("timestamp").alias("event_timestamp"),
        F.upper(trimmed(F.col("eventOrigin"))).alias("event_origin"),
        trimmed(F.col("action")).alias("action"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_single_source_audit(selected, "event_loan_application_events")


def build_app_accepted_loan():
    source = latest_file("accepted_loans", "loanId")
    selected = source.select(
        F.col("global_id").cast("string").alias("global_id"),
        F.col("loanId").cast("string").alias("loan_id"),
        F.upper(trimmed(F.col("loanStatus"))).alias("loan_status"),
        trimmed(F.col("purpose")).alias("purpose"),
        trimmed(F.col("term")).alias("term"),
        trimmed(F.col("grade")).alias("grade"),
        F.to_date("issuedAt").alias("issued_at"),
        masked_amount(F.col("loanAmount")).alias("loan_amount_masked"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_single_source_audit(selected, "file_accepted_loans", "MASKED")


def build_app_rejected_application():
    source = latest_file("rejected_applications", "loanId")
    selected = source.select(
        F.col("global_id").cast("string").alias("global_id"),
        F.col("loanId").cast("string").alias("loan_id"),
        trimmed(F.col("applicationTitle")).alias("application_title"),
        F.to_date("applicationDate").alias("application_date"),
        trimmed(F.col("rejectionReason")).alias("rejection_reason"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_single_source_audit(selected, "file_rejected_applications")


publish_silver_model("app_application", build_app_application, ["application_id"])
publish_silver_model("app_application_stage_history", build_app_application_stage_history, ["application_id"])
publish_silver_model("app_status_change_history", build_app_status_change_history, ["application_id"])
publish_silver_model("app_document", build_app_document, ["application_id"])
publish_silver_model("app_application_event", build_app_application_event, ["application_id"])
publish_silver_model("app_accepted_loan", build_app_accepted_loan, ["loan_id"])
publish_silver_model("app_rejected_application", build_app_rejected_application, ["loan_id"])
