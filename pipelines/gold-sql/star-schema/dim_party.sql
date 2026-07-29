MERGE INTO `0-ai-trust`.gold.dim_party AS target
USING (
  SELECT
    i.global_id AS party_key,
    i.global_id,
    i.customer_id,
    i.customer_type,
    i.name_token,
    i.preferred_contact_channel,
    i.preferred_language,
    i.marketing_opt_in,
    CAST(i.satisfaction_score AS STRING) AS satisfaction_score,
    i.address_state,
    i.address_postcode,
    CAST(i.customer_since AS TIMESTAMP) AS customer_since,
    CAST(i.last_updated_at AS TIMESTAMP) AS last_updated_at,
    CAST(NULL AS STRING) AS organisation_id,
    CAST(NULL AS STRING) AS business_name,
    CAST(NULL AS STRING) AS legal_name,
    CAST(NULL AS STRING) AS organisation_type,
    CAST(NULL AS STRING) AS industry_code,
    CAST(NULL AS STRING) AS registered_country,
    CAST(NULL AS DATE) AS establishment_date,
    CAST(NULL AS TIMESTAMP) AS org_last_updated_at,
    CAST(NULL AS BOOLEAN) AS is_acnc_registered,
    CAST(NULL AS STRING) AS abn_masked,
    i.pipeline_run_id,
    current_timestamp() AS processed_at,
    'PASSED' AS dq_status
  FROM `0-ai-trust`.silver.ip_individual AS i
  WHERE i.`__END_AT` IS NULL
    AND i.masking_status IN ('MASKED', 'CLEAN')

  UNION ALL

  SELECT
    CONCAT('ORG:', o.organisation_id) AS party_key,
    o.global_id,
    CAST(NULL AS STRING) AS customer_id,
    'ORGANISATION' AS customer_type,
    COALESCE(o.short_name, o.business_name) AS name_token,
    i.preferred_contact_channel,
    i.preferred_language,
    i.marketing_opt_in,
    CAST(NULL AS STRING) AS satisfaction_score,
    CAST(NULL AS STRING) AS address_state,
    CAST(NULL AS STRING) AS address_postcode,
    CAST(o.establishment_date AS TIMESTAMP) AS customer_since,
    CAST(o.last_updated_at AS TIMESTAMP) AS last_updated_at,
    o.organisation_id,
    o.business_name,
    o.legal_name,
    o.organisation_type,
    o.industry_code,
    o.registered_country,
    o.establishment_date,
    CAST(o.last_updated_at AS TIMESTAMP) AS org_last_updated_at,
    o.is_acnc_registered,
    o.abn_masked,
    o.pipeline_run_id,
    current_timestamp() AS processed_at,
    'PASSED' AS dq_status
  FROM `0-ai-trust`.silver.ip_organisation AS o
  LEFT JOIN `0-ai-trust`.silver.ip_individual AS i
    ON i.global_id = o.global_id
   AND i.`__END_AT` IS NULL
   AND i.masking_status IN ('MASKED', 'CLEAN')
  WHERE o.`__END_AT` IS NULL
    AND o.masking_status IN ('MASKED', 'CLEAN')
) AS source
ON target.party_key = source.party_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
