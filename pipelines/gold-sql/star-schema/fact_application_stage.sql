MERGE INTO `0-ai-trust`.gold.fact_application_stage AS target
USING (
  SELECT
    CONCAT('STAGE:', history_id) AS stage_fact_key,
    application_id AS application_key,
    application_id,
    global_id,
    'STAGE' AS event_source,
    CAST(entered_at AS TIMESTAMP) AS event_timestamp,
    stage AS stage_name,
    CAST(entered_at AS TIMESTAMP) AS entered_at,
    CAST(exited_at AS TIMESTAMP) AS exited_at,
    duration_days,
    is_current_stage,
    CAST(sla_deadline AS TIMESTAMP) AS sla_deadline,
    is_sla_breached,
    pending_action_party,
    assigned_team,
    CAST(NULL AS STRING) AS old_status,
    CAST(NULL AS STRING) AS new_status,
    CAST(NULL AS STRING) AS change_reason,
    CAST(NULL AS STRING) AS lifecycle_transition,
    CAST(NULL AS STRING) AS event_origin,
    CAST(NULL AS STRING) AS action,
    pipeline_run_id,
    current_timestamp() AS processed_at,
    dq_status
  FROM `0-ai-trust`.silver.app_application_stage_history
  WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('STATUS:', change_id), application_id, application_id, global_id, 'STATUS_CHANGE',
    CAST(changed_at AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS INT), false, CAST(NULL AS TIMESTAMP), false,
    CAST(NULL AS STRING), CAST(NULL AS STRING), old_status, new_status, reason,
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), pipeline_run_id,
    current_timestamp(), dq_status
  FROM `0-ai-trust`.silver.app_status_change_history
  WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('APP_EVENT:', event_id), application_id, application_id, global_id, 'APP_EVENT',
    CAST(event_timestamp AS TIMESTAMP), concept_name, CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS INT), false, CAST(NULL AS TIMESTAMP), false,
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), lifecycle_transition, event_origin, action, pipeline_run_id,
    current_timestamp(), dq_status
  FROM `0-ai-trust`.silver.app_application_event
  WHERE dq_status IN ('PASSED', 'WARNING') AND masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.stage_fact_key = source.stage_fact_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
