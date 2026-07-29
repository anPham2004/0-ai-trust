MERGE INTO `0-ai-trust`.gold.fact_service_interaction AS target
USING (
  SELECT
    CONCAT('CASE:', case_id) AS service_fact_key,
    global_id,
    organisation_id,
    application_id AS linked_application_id,
    'SERVICE_CASE' AS record_type,
    case_id,
    CAST(NULL AS STRING) AS interaction_id,
    case_type,
    subject,
    status AS case_status,
    priority,
    assigned_team,
    CAST(sla_deadline AS TIMESTAMP) AS sla_deadline,
    is_sla_breached,
    CAST(created_at AS TIMESTAMP) AS case_created_at,
    CAST(resolved_at AS TIMESTAMP) AS case_resolved_at,
    resolution_summary,
    CAST(NULL AS STRING) AS latest_event_type,
    CAST(NULL AS TIMESTAMP) AS latest_event_timestamp,
    CAST(NULL AS STRING) AS latest_event_description,
    CAST(NULL AS STRING) AS channel,
    CAST(NULL AS TIMESTAMP) AS interaction_timestamp,
    CAST(NULL AS STRING) AS topic,
    CAST(NULL AS STRING) AS resolution,
    pipeline_run_id,
    current_timestamp() AS processed_at,
    'PASSED' AS dq_status
  FROM `0-ai-trust`.silver.evt_service_case
  WHERE `__END_AT` IS NULL AND masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('INTERACTION:', interaction_id), global_id, CAST(NULL AS STRING),
    CAST(NULL AS STRING), 'SUPPORT_INTERACTION', CAST(NULL AS STRING), interaction_id,
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), false, CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING), channel, CAST(interaction_timestamp AS TIMESTAMP), topic, resolution,
    pipeline_run_id, current_timestamp(), 'PASSED'
  FROM `0-ai-trust`.silver.evt_support_interaction
  WHERE masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('CASE_EVENT:', e.event_id), e.global_id, c.organisation_id, c.application_id,
    'SERVICE_CASE_EVENT', e.case_id, CAST(NULL AS STRING), c.case_type, c.subject, c.status,
    c.priority, c.assigned_team, CAST(c.sla_deadline AS TIMESTAMP), c.is_sla_breached,
    CAST(c.created_at AS TIMESTAMP), CAST(c.resolved_at AS TIMESTAMP), c.resolution_summary,
    e.event_type, CAST(e.event_timestamp AS TIMESTAMP), e.description, CAST(NULL AS STRING),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING), e.pipeline_run_id,
    current_timestamp(), 'PASSED'
  FROM `0-ai-trust`.silver.evt_case_event AS e
  JOIN `0-ai-trust`.silver.evt_service_case AS c
    ON c.case_id = e.case_id AND c.`__END_AT` IS NULL
  WHERE e.masking_status IN ('MASKED', 'CLEAN') AND c.masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.service_fact_key = source.service_fact_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
