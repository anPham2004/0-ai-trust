CREATE OR REPLACE VIEW `0-ai-trust`.gold.aiv_shared_service_summary AS
SELECT
  s.service_fact_key,
  s.global_id,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.organisation_id END AS organisation_id,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.linked_application_id END AS linked_application_id,
  s.record_type,
  s.case_type,
  s.subject,
  s.case_status,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.priority END AS priority,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.assigned_team END AS assigned_team,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.sla_deadline END AS sla_deadline,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.is_sla_breached END AS is_sla_breached,
  s.case_created_at,
  s.case_resolved_at,
  s.resolution_summary,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.latest_event_type END AS latest_event_type,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.latest_event_timestamp END AS latest_event_timestamp,
  CASE WHEN is_account_group_member('banker-assist-users') THEN s.latest_event_description END AS latest_event_description,
  s.channel,
  s.interaction_timestamp,
  s.topic,
  s.resolution,
  p.preferred_contact_channel,
  'SHARED' AS usage_restriction,
  '1.0' AS context_version,
  current_timestamp() AS last_refreshed_at,
  s.pipeline_run_id,
  CASE WHEN s.dq_status = 'WARNING' OR p.dq_status = 'WARNING' THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
  CASE WHEN s.record_type = 'SERVICE_CASE_EVENT' AND s.case_id IS NULL THEN 'Parent service case is unavailable' ELSE CAST(NULL AS STRING) END AS known_limitations
FROM `0-ai-trust`.gold.fact_service_interaction AS s
JOIN `0-ai-trust`.gold.dim_party AS p
  ON p.party_key = s.global_id AND p.organisation_id IS NULL;
