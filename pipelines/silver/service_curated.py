"""Service and interaction Silver entities."""

from pyspark.sql import functions as F

from framework.silver_model import latest_cdc, latest_event, publish_silver_model, trimmed, with_audit_columns


def _with_audit(dataframe, source_table):
    return with_audit_columns(
        dataframe,
        [source_table],
        [F.col("_batch_id")],
        [F.col("_ingested_at")],
        "CLEAN",
    ).drop("_batch_id", "_ingested_at")


def build_evt_service_case():
    source = latest_cdc("service_cases", ["caseId"])
    status = F.lower(trimmed(F.col("status")))
    deadline = F.to_timestamp("slaDeadline")
    selected = source.select(
        F.col("caseId").cast("string").alias("case_id"),
        F.col("global_id").cast("string").alias("global_id"),
        F.col("organisationId").cast("string").alias("organisation_id"),
        F.col("applicationId").cast("string").alias("application_id"),
        deadline.alias("sla_deadline"),
        (deadline.isNotNull() & status.isin("open", "in_progress") & (F.current_timestamp() > deadline)).alias("is_sla_breached"),
        F.col("organisationId").isNotNull().alias("is_business_case"),
        F.upper(trimmed(F.col("caseType"))).alias("case_type"),
        trimmed(F.col("subject")).alias("subject"),
        status.alias("status"),
        F.upper(trimmed(F.col("priority"))).alias("priority"),
        trimmed(F.col("assignedTeam")).alias("assigned_team"),
        F.to_timestamp("createdAt").alias("created_at"),
        F.to_timestamp("resolvedAt").alias("resolved_at"),
        trimmed(F.col("resolutionSummary")).alias("resolution_summary"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_audit(selected, "cdc_service_cases")


def build_evt_support_interaction():
    source = latest_event("support_interactions", "interactionId", "timestamp")
    selected = source.select(
        F.col("interactionId").cast("string").alias("interaction_id"),
        F.col("global_id").cast("string").alias("global_id"),
        F.upper(trimmed(F.col("channel"))).alias("channel"),
        F.to_timestamp("timestamp").alias("interaction_timestamp"),
        trimmed(F.col("topic")).alias("topic"),
        trimmed(F.col("resolution")).alias("resolution"),
        F.col("durationMinutes").cast("int").alias("duration_minutes"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_audit(selected, "event_support_interactions")


def build_evt_service_case_event():
    source = latest_event("service_case_events", "eventId", "eventTimestamp")
    selected = source.select(
        F.col("eventId").cast("string").alias("event_id"),
        F.col("caseId").cast("string").alias("case_id"),
        F.col("global_id").cast("string").alias("global_id"),
        F.upper(trimmed(F.col("eventType"))).alias("event_type"),
        F.to_timestamp("eventTimestamp").alias("event_timestamp"),
        trimmed(F.col("description")).alias("description"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )
    return _with_audit(selected, "event_service_case_events")


publish_silver_model("evt_service_case", build_evt_service_case, ["case_id"])
publish_silver_model("evt_support_interaction", build_evt_support_interaction, ["global_id"])
publish_silver_model("evt_service_case_event", build_evt_service_case_event, ["case_id"])
