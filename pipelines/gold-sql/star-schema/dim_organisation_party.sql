MERGE INTO `0-ai-trust`.gold.dim_organisation_party AS target
USING (
  SELECT
    r.relationship_id AS org_party_key,
    r.organisation_id,
    r.global_id,
    i.name_token,
    r.party_role,
    r.authority_level,
    r.is_active,
    r.relationship_start_date AS start_date,
    r.relationship_end_date AS end_date,
    i.preferred_contact_channel,
    r.pipeline_run_id,
    current_timestamp() AS processed_at,
    CASE WHEN r.dq_status = 'WARNING' OR i.dq_status = 'WARNING' THEN 'WARNING' ELSE 'PASSED' END AS dq_status
  FROM `0-ai-trust`.silver.ip_organisation_party_relationship AS r
  JOIN `0-ai-trust`.silver.ip_individual AS i ON i.global_id = r.global_id
  WHERE r.dq_status IN ('PASSED', 'WARNING')
    AND i.dq_status IN ('PASSED', 'WARNING')
    AND r.masking_status IN ('MASKED', 'CLEAN')
    AND i.masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.org_party_key = source.org_party_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
