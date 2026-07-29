MERGE INTO `0-ai-trust`.gold.dim_document AS target
USING (
  SELECT
    document_id AS document_key,
    document_id,
    application_id,
    global_id,
    document_type,
    status,
    is_invalid_or_expired,
    rejection_reason,
    CAST(requested_at AS TIMESTAMP) AS requested_at,
    CAST(received_at AS TIMESTAMP) AS received_at,
    expiry_date,
    reminders_sent,
    CAST(last_reminder_at AS TIMESTAMP) AS last_reminder_at,
    pipeline_run_id,
    current_timestamp() AS processed_at,
    'PASSED' AS dq_status
  FROM `0-ai-trust`.silver.app_missing_document
  WHERE `__END_AT` IS NULL
    AND masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.document_key = source.document_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
