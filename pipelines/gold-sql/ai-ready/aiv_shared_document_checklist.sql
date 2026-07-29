CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_shared_document_checklist AS
SELECT
  d.document_id,
  d.application_id,
  d.global_id,
  d.document_type,
  d.status,
  d.is_invalid_or_expired,
  d.rejection_reason,
  d.requested_at,
  d.received_at,
  d.expiry_date,
  d.reminders_sent,
  d.last_reminder_at,
  a.loan_goal,
  'SHARED' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  d.pipeline_run_id,
  CASE WHEN d.dq_status = 'WARNING' OR a.dq_status = 'WARNING' THEN 'WARNING' ELSE 'PASSED' END AS dq_status
FROM `0-ai-trust`.gold.dim_document AS d
JOIN `0-ai-trust`.gold.dim_application AS a ON a.application_id = d.application_id;
