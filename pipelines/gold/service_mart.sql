SET pipelines.trigger.interval=15 minutes;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_service_case_current
COMMENT 'Current service-case state with ownership and SLA context'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  c.global_id,
  c.case_id,
  d.customer_id,
  c.organisation_id,
  c.application_id,
  c.is_business_case,
  c.case_type,
  c.subject AS case_subject,
  c.status AS case_status,
  c.priority,
  c.assigned_team,
  c.created_at,
  c.resolved_at,
  c.resolution_summary,
  c.sla_deadline,
  c.is_sla_breached,
  UPPER(c.status) IN ('OPEN', 'IN_PROGRESS', 'PENDING') AS is_open,
  datediff(COALESCE(to_date(c.resolved_at), current_date()), to_date(c.created_at)) AS case_age_days,
  CASE WHEN c.resolved_at IS NOT NULL
    THEN (unix_timestamp(c.resolved_at) - unix_timestamp(c.created_at)) / 3600.0
  END AS resolution_duration_hours,
  c.application_id IS NOT NULL AS application_related_flag,
  'PASSED' AS dq_status,
  c.masking_status,
  c.processed_at
FROM `0-ai-trust`.silver.evt_service_case c
LEFT JOIN `0-ai-trust`.gold.dim_customer d ON d.global_id = c.global_id
WHERE c.`__END_AT` IS NULL
  AND c.masking_status IN ('MASKED', 'CLEAN');

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_service_activity
COMMENT 'Canonical append-only support interaction and service-case event stream'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  s.global_id,
  d.customer_id,
  CAST(NULL AS STRING) AS case_id,
  CONCAT('SUPPORT_INTERACTION:', s.interaction_id) AS service_activity_id,
  CAST(NULL AS STRING) AS application_id,
  s.interaction_timestamp AS activity_timestamp,
  'SUPPORT_INTERACTION' AS activity_type,
  s.channel,
  s.topic,
  s.resolution,
  s.duration_minutes,
  CAST(NULL AS STRING) AS case_event_type,
  CAST(NULL AS STRING) AS description,
  true AS is_customer_contact,
  false AS is_internal_activity,
  UPPER(CONCAT_WS(' ', s.topic, s.resolution)) RLIKE 'FOLLOW.?UP|CALL.?BACK|PENDING' AS follow_up_required_flag,
  UPPER(CONCAT_WS(' ', s.topic, s.resolution)) RLIKE 'ESCALAT' AS escalated_flag,
  s.source_table,
  s.interaction_id AS source_record_id,
  'PASSED' AS dq_status,
  s.masking_status,
  s.processed_at
FROM `0-ai-trust`.silver.evt_support_interaction s
LEFT JOIN `0-ai-trust`.gold.dim_customer d ON d.global_id = s.global_id
WHERE s.masking_status IN ('MASKED', 'CLEAN')

UNION ALL

SELECT
  e.global_id,
  d.customer_id,
  e.case_id,
  CONCAT('CASE_EVENT:', e.event_id) AS service_activity_id,
  c.application_id,
  e.event_timestamp AS activity_timestamp,
  'SERVICE_CASE_EVENT' AS activity_type,
  CAST(NULL AS STRING) AS channel,
  CAST(NULL AS STRING) AS topic,
  CAST(NULL AS STRING) AS resolution,
  CAST(NULL AS INT) AS duration_minutes,
  e.event_type AS case_event_type,
  e.description,
  UPPER(e.event_type) = 'CUSTOMER_CONTACTED' AS is_customer_contact,
  UPPER(e.event_type) <> 'CUSTOMER_CONTACTED' AS is_internal_activity,
  UPPER(CONCAT_WS(' ', e.event_type, e.description)) RLIKE 'FOLLOW.?UP|CALL.?BACK|PENDING' AS follow_up_required_flag,
  UPPER(CONCAT_WS(' ', e.event_type, e.description)) RLIKE 'ESCALAT' AS escalated_flag,
  e.source_table,
  e.event_id AS source_record_id,
  'PASSED' AS dq_status,
  e.masking_status,
  e.processed_at
FROM `0-ai-trust`.silver.evt_case_event e
LEFT JOIN `0-ai-trust`.gold.fact_service_case_current c ON c.case_id = e.case_id
LEFT JOIN `0-ai-trust`.gold.dim_customer d ON d.global_id = e.global_id
WHERE e.masking_status IN ('MASKED', 'CLEAN');
