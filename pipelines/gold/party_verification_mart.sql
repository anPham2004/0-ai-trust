SET pipelines.trigger.interval=15 minutes;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.bridge_organisation_party_role (
  global_id STRING,
  relationship_id STRING NOT NULL,
  organisation_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  party_role STRING,
  authority_level STRING,
  is_active BOOLEAN,
  start_date DATE,
  end_date DATE,
  dq_status STRING,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  CONSTRAINT pk_bridge_organisation_party_role PRIMARY KEY (relationship_id),
  CONSTRAINT fk_bridge_organisation_party_role_organisation FOREIGN KEY (organisation_id)
    REFERENCES `0-ai-trust`.gold.dim_organisation (organisation_id),
  CONSTRAINT fk_bridge_organisation_party_role_customer FOREIGN KEY (customer_id)
    REFERENCES `0-ai-trust`.gold.dim_customer (customer_id)
)
COMMENT 'Current person-to-organisation role and authority relationships'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  r.global_id,
  r.relationship_id,
  r.organisation_id,
  c.customer_id,
  r.party_role,
  r.authority_level,
  r.is_active,
  r.start_date,
  r.end_date,
  'PASSED' AS dq_status,
  r.pipeline_run_id,
  r.processed_at
FROM `0-ai-trust`.silver.ip_party_relationship r
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = r.global_id
WHERE r.`__END_AT` IS NULL
  AND r.masking_status IN ('MASKED', 'CLEAN');

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.bridge_organisation_relationship (
  relationship_id STRING NOT NULL,
  source_organisation_id STRING NOT NULL,
  target_organisation_id STRING NOT NULL,
  relationship_type STRING,
  is_active BOOLEAN,
  start_date DATE,
  dq_status STRING,
  pipeline_run_id STRING,
  processed_at TIMESTAMP,
  CONSTRAINT pk_bridge_organisation_relationship PRIMARY KEY (relationship_id),
  CONSTRAINT fk_bridge_organisation_relationship_source FOREIGN KEY (source_organisation_id)
    REFERENCES `0-ai-trust`.gold.dim_organisation (organisation_id),
  CONSTRAINT fk_bridge_organisation_relationship_target FOREIGN KEY (target_organisation_id)
    REFERENCES `0-ai-trust`.gold.dim_organisation (organisation_id)
)
COMMENT 'Current organisation-to-organisation relationships'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Internal')
AS
SELECT
  relationship_id,
  source_org_id AS source_organisation_id,
  target_org_id AS target_organisation_id,
  relationship_type,
  is_active,
  start_date,
  'PASSED' AS dq_status,
  pipeline_run_id,
  processed_at
FROM `0-ai-trust`.silver.ip_org_relationship
WHERE `__END_AT` IS NULL
  AND masking_status IN ('MASKED', 'CLEAN');

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.bridge_application_party_authority (
  application_id STRING NOT NULL,
  organisation_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  party_role STRING NOT NULL,
  authority_level STRING,
  is_active BOOLEAN,
  derivation_method STRING,
  limitation_code STRING,
  CONSTRAINT pk_bridge_application_party_authority
    PRIMARY KEY (application_id, organisation_id, customer_id, party_role),
  CONSTRAINT fk_bridge_application_party_authority_application FOREIGN KEY (application_id)
    REFERENCES `0-ai-trust`.gold.fact_application_current (application_id),
  CONSTRAINT fk_bridge_application_party_authority_organisation FOREIGN KEY (organisation_id)
    REFERENCES `0-ai-trust`.gold.dim_organisation (organisation_id),
  CONSTRAINT fk_bridge_application_party_authority_customer FOREIGN KEY (customer_id)
    REFERENCES `0-ai-trust`.gold.dim_customer (customer_id)
)
COMMENT 'Application authority derived from organisation-level roles; not proof of an application-specific mandate'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Confidential')
AS
SELECT
  a.application_id,
  a.organisation_id,
  r.customer_id,
  r.party_role,
  max_by(r.authority_level, r.start_date) AS authority_level,
  bool_or(r.is_active) AS is_active,
  'ORGANISATION_PARTY_ROLE_LOOKUP' AS derivation_method,
  'APPLICATION_SPECIFIC_MANDATE_UNAVAILABLE' AS limitation_code
FROM `0-ai-trust`.silver.app_application a
JOIN `0-ai-trust`.gold.bridge_organisation_party_role r
  ON r.organisation_id = a.organisation_id
WHERE a.`__END_AT` IS NULL
  AND a.organisation_id IS NOT NULL
  AND a.masking_status IN ('MASKED', 'CLEAN')
GROUP BY a.application_id, a.organisation_id, r.customer_id, r.party_role;

CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.fact_verification_current (
  kyc_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  organisation_id STRING,
  record_type STRING,
  verification_status STRING,
  verification_method STRING,
  verification_date DATE,
  last_review_date DATE,
  next_review_date DATE,
  review_overdue_flag BOOLEAN,
  document_types STRING,
  abn_verified BOOLEAN,
  asic_check_status STRING,
  beneficial_ownership_verified BOOLEAN,
  risk_rating STRING,
  pep_status BOOLEAN,
  sanctions_check STRING,
  dq_status STRING,
  masking_status STRING,
  processed_at TIMESTAMP,
  CONSTRAINT pk_fact_verification_current PRIMARY KEY (kyc_id),
  CONSTRAINT fk_fact_verification_customer FOREIGN KEY (customer_id)
    REFERENCES `0-ai-trust`.gold.dim_customer (customer_id),
  CONSTRAINT fk_fact_verification_organisation FOREIGN KEY (organisation_id)
    REFERENCES `0-ai-trust`.gold.dim_organisation (organisation_id)
)
COMMENT 'Current KYC or KYB verification state; restricted compliance fields require separate grants'
TBLPROPERTIES ('quality' = 'gold', 'data_classification' = 'Highly Confidential')
AS
SELECT
  k.kyc_id,
  c.customer_id,
  k.organisation_id,
  k.record_type,
  k.verification_status,
  k.verification_method,
  k.verification_date,
  k.last_review_date,
  k.next_review_date,
  k.next_review_date < current_date() AS review_overdue_flag,
  k.document_types,
  k.abn_verified,
  k.asic_check_status,
  k.beneficial_ownership_verified,
  k.risk_rating,
  k.pep_status,
  k.sanctions_check,
  'PASSED' AS dq_status,
  k.masking_status,
  k.processed_at
FROM `0-ai-trust`.silver.ip_kyc k
LEFT JOIN `0-ai-trust`.gold.dim_customer c ON c.global_id = k.global_id
WHERE k.`__END_AT` IS NULL
  AND k.masking_status IN ('MASKED', 'CLEAN');
