CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_shared_application_timeline AS
WITH timeline AS (
  SELECT
    f.*,
    UPPER(COALESCE(f.event_origin, '')) IN ('INTERNAL', 'INTERNAL_TEAM') AS is_internal_only
  FROM `0-ai-trust`.gold.fact_application_stage AS f
)
SELECT
  a.application_id,
  a.global_id,
  t.event_source,
  t.event_timestamp,
  t.stage_name,
  t.entered_at,
  t.old_status,
  t.new_status,
  t.change_reason,
  t.lifecycle_transition,
  t.event_origin,
  CASE WHEN is_account_group_member('banker-assist-users') THEN t.assigned_team END AS assigned_team,
  t.is_internal_only,
  'SHARED' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  t.pipeline_run_id,
  t.dq_status
FROM timeline AS t
JOIN `0-ai-trust`.gold.dim_application AS a ON a.application_id = t.application_id
WHERE NOT t.is_internal_only OR is_account_group_member('banker-assist-users');
