CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_shared_application_status AS
WITH current_stage AS (
  SELECT * EXCEPT (_rank)
  FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY application_id ORDER BY event_timestamp DESC, stage_fact_key DESC
    ) AS _rank
    FROM `0-ai-trust`.gold.fact_application_stage
    WHERE is_current_stage
  ) WHERE _rank = 1
), document AS (
  SELECT application_id,
    COUNT_IF(UPPER(status) = 'PENDING') AS pending_document_count,
    COUNT_IF(is_invalid_or_expired) > 0 AS has_invalid_documents,
    MAX(dq_status) AS dq_status
  FROM `0-ai-trust`.gold.dim_document
  GROUP BY application_id
)
SELECT
  a.application_id,
  a.global_id,
  CASE WHEN is_account_group_member('banker-assist-users') THEN a.organisation_id END AS organisation_id,
  a.loan_goal,
  a.application_type,
  a.final_outcome,
  a.submitted_at,
  a.last_updated_at,
  s.stage_name AS current_stage,
  s.entered_at AS current_stage_entered_at,
  s.duration_days AS duration_at_current_stage_days,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.sla_deadline END AS sla_deadline,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.is_sla_breached END AS is_sla_breached,
  CASE WHEN is_account_group_member('banker-assist-users') THEN DATEDIFF(current_date(), TO_DATE(a.last_updated_at)) END AS days_inactive,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.pending_action_party END AS pending_action_party,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.assigned_team END AS assigned_team,
  COALESCE(d.pending_document_count, 0) AS pending_document_count,
  COALESCE(d.has_invalid_documents, false) AS has_invalid_documents,
  CASE
    WHEN COALESCE(d.pending_document_count, 0) > 0 THEN 'Provide outstanding documents'
    WHEN s.pending_action_party IS NOT NULL THEN CONCAT('Pending action by ', s.pending_action_party)
    ELSE 'No next action recorded'
  END AS next_action_description,
  'SHARED' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  a.pipeline_run_id,
  CASE WHEN a.dq_status = 'WARNING' OR s.dq_status = 'WARNING' OR d.dq_status = 'WARNING'
    THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
  CASE WHEN s.stage_fact_key IS NULL THEN 'Current application stage is unavailable' ELSE CAST(NULL AS STRING) END AS known_limitations
FROM `0-ai-trust`.gold.dim_application AS a
LEFT JOIN current_stage AS s ON s.application_id = a.application_id
LEFT JOIN document AS d ON d.application_id = a.application_id;
