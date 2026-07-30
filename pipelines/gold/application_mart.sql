SET pipelines.trigger.interval=15 minutes;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_application_document
COMMENT 'Current application document checklist with deterministic status flags'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  d.document_id,
  d.application_id,
  c.customer_id,
  d.global_id,
  d.document_type,
  d.status AS document_status,
  d.requested_at,
  d.received_at,
  d.expiry_date,
  UPPER(d.status) IN ('PENDING', 'MISSING', 'REQUIRED') AS is_missing,
  UPPER(d.status) IN ('RECEIVED', 'VERIFIED') AS is_received,
  UPPER(d.status) IN ('REJECTED', 'INVALID') AS is_rejected,
  d.expiry_date < current_date() OR UPPER(d.status) = 'EXPIRED' AS is_expired,
  d.is_invalid_or_expired
    OR UPPER(d.status) IN ('INVALID', 'EXPIRED')
    OR d.expiry_date < current_date() AS is_invalid_or_expired,
  d.reminders_sent,
  d.last_reminder_at,
  d.rejection_reason,
  'PASSED' AS dq_status,
  d.processed_at,
  d.pipeline_run_id
FROM `0-ai-trust`.silver.app_missing_document d
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = d.global_id
WHERE d.`__END_AT` IS NULL
  AND d.masking_status IN ('MASKED', 'CLEAN');

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_application_timeline_event
COMMENT 'Canonical ordered stage, status, and lifecycle application history with record-level lineage'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
WITH stage_events AS (
  SELECT
    global_id,
    history_id AS source_record_id,
    application_id,
    entered_at AS event_timestamp,
    'STAGE' AS event_family,
    'STAGE_TRANSITION' AS event_type,
    lag(stage) OVER (PARTITION BY application_id ORDER BY entered_at, history_id) AS from_value,
    stage AS to_value,
    stage,
    CAST(NULL AS STRING) AS status,
    CAST(NULL AS STRING) AS concept_name,
    CAST(NULL AS STRING) AS lifecycle_transition,
    CAST(NULL AS STRING) AS event_origin,
    CAST(NULL AS STRING) AS action,
    assigned_team,
    pending_action_party,
    CAST(NULL AS STRING) AS recorded_reason,
    'app_stage_history' AS source_table,
    pipeline_run_id AS source_pipeline_run_id,
    masking_status
  FROM `0-ai-trust`.silver.app_stage_history
  WHERE masking_status IN ('MASKED', 'CLEAN')
), all_events AS (
  SELECT * FROM stage_events
  UNION ALL
  SELECT
    global_id, change_id, application_id, changed_at,
    'STATUS', 'STATUS_CHANGE', old_status, new_status,
    CAST(NULL AS STRING), new_status,
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), reason,
    'app_status_change', pipeline_run_id, masking_status
  FROM `0-ai-trust`.silver.app_status_change
  WHERE masking_status IN ('MASKED', 'CLEAN')
  UNION ALL
  SELECT
    global_id, event_id, application_id, event_timestamp,
    'APPLICATION', COALESCE(concept_name, 'APPLICATION_EVENT'),
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    concept_name, lifecycle_transition, event_origin, action,
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    'app_lifecycle_event', pipeline_run_id, masking_status
  FROM `0-ai-trust`.silver.app_lifecycle_event
  WHERE masking_status IN ('MASKED', 'CLEAN')
), sequenced AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY application_id
      ORDER BY event_timestamp, event_family, source_record_id
    ) AS event_sequence
  FROM all_events
)
SELECT
  e.global_id,
  CONCAT(e.source_table, ':', e.source_record_id) AS timeline_event_id,
  e.application_id,
  c.customer_id,
  e.event_timestamp,
  e.event_sequence,
  e.event_family,
  e.event_type,
  e.from_value,
  e.to_value,
  e.stage,
  e.status,
  e.concept_name,
  e.lifecycle_transition,
  e.event_origin,
  e.action,
  e.assigned_team,
  e.pending_action_party,
  e.recorded_reason,
  e.source_table,
  e.source_record_id,
  e.source_pipeline_run_id,
  'PASSED' AS dq_status,
  e.masking_status
FROM sequenced e
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = e.global_id;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_application_current
COMMENT 'One denormalised current row per application with stage, status, SLA, document, and inactivity context'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
WITH ranked_stages AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY application_id
      ORDER BY is_current_stage DESC, entered_at DESC, history_id DESC
    ) AS stage_rank
  FROM `0-ai-trust`.silver.app_stage_history
  WHERE masking_status IN ('MASKED', 'CLEAN')
), stage_summary AS (
  SELECT
    application_id,
    max(CASE WHEN stage_rank = 1 THEN stage END) AS current_stage,
    max(CASE WHEN stage_rank = 1 THEN entered_at END) AS current_stage_entered_at,
    max(CASE WHEN stage_rank = 2 THEN stage END) AS previous_stage,
    max(CASE WHEN stage_rank = 1 THEN assigned_team END) AS assigned_team,
    max(CASE WHEN stage_rank = 1 THEN pending_action_party END) AS pending_action_party,
    max(CASE WHEN stage_rank = 1 THEN sla_deadline END) AS stage_sla_deadline,
    bool_or(CASE WHEN stage_rank = 1 THEN is_sla_breached ELSE false END) AS is_stage_sla_breached,
    max(entered_at) AS latest_stage_activity_at,
    max(processed_at) AS stage_processed_at
  FROM ranked_stages
  GROUP BY application_id
), ranked_status AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY application_id ORDER BY changed_at DESC, change_id DESC) AS status_rank
  FROM `0-ai-trust`.silver.app_status_change
  WHERE masking_status IN ('MASKED', 'CLEAN')
), status_summary AS (
  SELECT
    application_id,
    max(CASE WHEN status_rank = 1 THEN new_status END) AS latest_status,
    max(CASE WHEN status_rank = 1 THEN changed_at END) AS status_last_changed_at,
    max(CASE WHEN status_rank = 1 THEN reason END) AS recorded_reason,
    max(processed_at) AS status_processed_at
  FROM ranked_status
  GROUP BY application_id
), document_summary AS (
  SELECT
    application_id,
    count(*) AS required_document_count,
    count_if(is_missing) AS missing_document_count,
    count_if(is_received) AS received_document_count,
    count_if(is_rejected) AS rejected_document_count,
    count_if(is_expired) AS expired_document_count,
    sum(COALESCE(reminders_sent, 0)) AS total_reminders_sent,
    greatest(max(received_at), max(requested_at), max(last_reminder_at)) AS latest_document_activity_at,
    max(processed_at) AS document_processed_at
  FROM `0-ai-trust`.gold.fact_application_document
  GROUP BY application_id
), lifecycle_summary AS (
  SELECT
    application_id,
    max(event_timestamp) AS latest_lifecycle_activity_at,
    max(processed_at) AS lifecycle_processed_at
  FROM `0-ai-trust`.silver.app_lifecycle_event
  WHERE masking_status IN ('MASKED', 'CLEAN')
  GROUP BY application_id
)
SELECT
  a.global_id,
  a.application_id,
  c.customer_id,
  a.is_business_application,
  a.loan_goal,
  a.application_type,
  a.submitted_at,
  a.last_updated_at,
  a.requested_amount_masked,
  a.final_outcome,
  s.current_stage,
  s.current_stage_entered_at,
  s.previous_stage,
  s.assigned_team,
  s.pending_action_party,
  s.stage_sla_deadline,
  datediff(current_date(), to_date(s.current_stage_entered_at)) AS days_in_current_stage,
  COALESCE(s.is_stage_sla_breached, false) AS is_stage_sla_breached,
  COALESCE(st.latest_status, a.final_outcome) AS latest_status,
  st.status_last_changed_at,
  st.recorded_reason,
  s.pending_action_party IS NOT NULL
    AND UPPER(s.pending_action_party) <> 'INTERNAL_TEAM' AS customer_action_required_flag,
  UPPER(s.pending_action_party) = 'INTERNAL_TEAM' AS internal_action_required_flag,
  COALESCE(d.required_document_count, 0) AS required_document_count,
  COALESCE(d.missing_document_count, 0) AS missing_document_count,
  COALESCE(d.received_document_count, 0) AS received_document_count,
  COALESCE(d.rejected_document_count, 0) AS rejected_document_count,
  COALESCE(d.expired_document_count, 0) AS expired_document_count,
  COALESCE(d.total_reminders_sent, 0) AS total_reminders_sent,
  greatest(
    a.last_updated_at,
    s.latest_stage_activity_at,
    st.status_last_changed_at,
    d.latest_document_activity_at,
    l.latest_lifecycle_activity_at
  ) AS latest_activity_at,
  greatest(
    a.last_updated_at,
    s.latest_stage_activity_at,
    st.status_last_changed_at,
    d.latest_document_activity_at,
    l.latest_lifecycle_activity_at
  ) < current_timestamp() - INTERVAL 14 DAYS AS is_inactive_over_14_days,
  'PASSED' AS dq_status,
  a.masking_status,
  greatest(a.processed_at, s.stage_processed_at, st.status_processed_at, d.document_processed_at, l.lifecycle_processed_at)
    AS last_refreshed_at,
  a.pipeline_run_id
FROM `0-ai-trust`.silver.app_application a
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = a.global_id
LEFT JOIN stage_summary s ON s.application_id = a.application_id
LEFT JOIN status_summary st ON st.application_id = a.application_id
LEFT JOIN document_summary d ON d.application_id = a.application_id
LEFT JOIN lifecycle_summary l ON l.application_id = a.application_id
WHERE a.`__END_AT` IS NULL
  AND a.masking_status IN ('MASKED', 'CLEAN');
