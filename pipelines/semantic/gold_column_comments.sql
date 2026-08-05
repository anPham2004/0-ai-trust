-- ============================================================
-- GOLD TABLE-LEVEL COMMENTS — `0-ai-trust`
-- Purpose: Set table-level descriptions on all Gold objects so
--          Genie Ontology has clear, business-aware context per table.
--
-- ⚠️  IMPORTANT — WHY ALTER COLUMN IS NOT USED HERE:
--     All Gold objects are Materialized Views (CREATE OR REFRESH
--     MATERIALIZED VIEW). Databricks does NOT support:
--       ALTER VIEW ... ALTER COLUMN ... COMMENT
--     on Materialized Views. This causes:
--       EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE
--
--     ✅ TABLE-LEVEL COMMENTS (below) work fine on MVs.
--     ✅ COLUMN-LEVEL COMMENTS must be added directly inside
--        the pipeline DDL (pipelines/gold/*.sql) as COMMENT blocks
--        on each column in the SELECT clause, e.g.:
--          customer_id COMMENT 'Unique customer identifier...',
--
-- Run on a Databricks SQL warehouse. Safe to re-run.
-- ============================================================

-- ── dim_customer ──────────────────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.dim_customer
  SET TBLPROPERTIES (
    'comment' = 'Current masked customer profile for Banker.AI and Customer.AI. One row per customer_id. Contains preferences, satisfaction score, and demographic bands. Excludes raw PII — all name/email/phone fields are tokenised or redacted (masking_status = MASKED). dq_status gate must be checked before surfacing any row in AI output.'
  );

-- ── dim_organisation ─────────────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.dim_organisation
  SET TBLPROPERTIES (
    'comment' = 'Current organisation (business customer) profile for Banker.AI Business. One row per organisation_id. Contains legal name, trading name, entity type, industry code, and establishment date. Excludes risk classifications. dq_status gate must be checked before surfacing any row in AI output.'
  );

-- ── dim_date ─────────────────────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.dim_date
  SET TBLPROPERTIES (
    'comment' = 'Date dimension for joining to fact tables by calendar date. Contains calendar attributes (year, month, quarter, week) and business-day flags. Safe for all AI use cases — contains no PII or sensitive data.'
  );

-- ── dim_stage_action_policy ──────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.dim_stage_action_policy
  SET TBLPROPERTIES (
    'comment' = 'Policy table mapping application lifecycle stage + pending_action_party to compliance-approved next-action wording. banker_action_text is for internal banker use only. customer_safe_action_text is cleared for direct customer exposure. Currently returns POLICY_NOT_CONFIGURED for all stages — pending business owner sign-off.'
  );

-- ── fact_application_current ─────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.fact_application_current
  SET TBLPROPERTIES (
    'comment' = 'Current state of all loan and banking product applications. One row per application_id. Key fields: current_stage, pending_action_party, is_stage_sla_breached, is_inactive_over_14_days. recorded_reason is internal-only and must not be surfaced in Customer.AI (CR2 compliance).'
  );

-- ── fact_application_document ────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.fact_application_document
  SET TBLPROPERTIES (
    'comment' = 'Document checklist per application. One row per application_id + document_type. Tracks REQUESTED, RECEIVED, REJECTED, EXPIRED, VALIDATED status. rejection_reason is internal-only and must not be surfaced in Customer.AI (CR2 compliance).'
  );

-- ── fact_application_timeline_event ──────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.fact_application_timeline_event
  SET TBLPROPERTIES (
    'comment' = 'Full audit trail of application state changes. One row per event. Ordered by event_sequence within each application_id. recorded_reason is internal-only. Use from_value and to_value with event_type = STAGE_CHANGE to reconstruct stage history.'
  );

-- ── fact_subject_context_snapshot ────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.fact_subject_context_snapshot
  SET TBLPROPERTIES (
    'comment' = 'Pre-aggregated context snapshot per entity (CUSTOMER or ORGANISATION). One row per entity_id + entity_type. Contains pre-computed counts (open_case_count, missing_document_count, active_arrangement_count) for fast pre-call summary queries. Always filter on entity_type to avoid cross-entity data bleed. Zero-trust: risk_rating, pep_status, sanctions_check are never included.'
  );

-- ── bridge_organisation_party_role ───────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.bridge_organisation_party_role
  SET TBLPROPERTIES (
    'comment' = 'Person-to-organisation role bridge. Maps customer_id (individual) to organisation_id with their party_role (DIRECTOR, AUTHORISED_REPRESENTATIVE, SIGNATORY, BENEFICIAL_OWNER, GUARANTOR). Filter is_active = TRUE for current representatives.'
  );

-- ── bridge_organisation_relationship ─────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.bridge_organisation_relationship
  SET TBLPROPERTIES (
    'comment' = 'Organisation-to-organisation relationship bridge. Captures SUBSIDIARY, PARENT, GUARANTOR, AFFILIATED relationships. Filter is_active = TRUE for current relationships.'
  );

-- ── bridge_application_party_authority ───────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.bridge_application_party_authority
  SET TBLPROPERTIES (
    'comment' = 'Application-level signing authority bridge. Maps application_id to customer_id with their party_role and authority_level. Use to answer "Who is authorised to discuss this application?" (BB7). Derivation_method distinguishes EXPLICIT (recorded) from DERIVED (inferred from org role) authority.'
  );

-- ── fact_verification_current ────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.fact_verification_current
  SET TBLPROPERTIES (
    'comment' = 'Current KYC/KYB verification status per entity. Contains verification_status (VERIFIED, PENDING, FAILED), abn_verified, asic_check_status, beneficial_ownership_verified, and review schedule. Zero-trust: risk_rating, pep_status, and sanctions_check are intentionally excluded.'
  );

-- ── fact_service_case_current ────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.fact_service_case_current
  SET TBLPROPERTIES (
    'comment' = 'Current service case state per customer or organisation. Tracks OPEN, PENDING, RESOLVED, CLOSED status. resolution_summary is internal-only and must not be surfaced in Customer.AI. is_sla_breached flags cases requiring urgent escalation.'
  );

-- ── fact_arrangement_current ─────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.fact_arrangement_current
  SET TBLPROPERTIES (
    'comment' = 'Current state of banking product arrangements per customer or organisation. Covers LOAN, MORTGAGE, CREDIT_CARD, TRANSACTION_ACCOUNT. maturity_date identifies arrangements approaching expiry (BB17).'
  );

-- ── fact_service_activity ─────────────────────────────────────
ALTER MATERIALIZED VIEW `0-ai-trust`.gold.fact_service_activity
  SET TBLPROPERTIES (
    'comment' = 'Customer interaction and support activity log. One row per interaction. is_customer_contact = TRUE for direct customer touchpoints. escalated_flag indicates elevated dissatisfaction. Internal-only fields (is_internal_activity, follow_up_required_flag) must not be surfaced to customers.'
  );

-- ============================================================
-- VERIFICATION QUERY
-- Run after to confirm table-level comments were applied.
-- ============================================================
SELECT table_name, comment AS table_comment
FROM `0-ai-trust`.information_schema.tables
WHERE table_schema = 'gold'
  AND comment IS NOT NULL
ORDER BY table_name;

-- ============================================================
-- NEXT STEP: ADD COLUMN COMMENTS IN PIPELINE DDL
-- ============================================================
-- Column-level comments on Materialized Views must be defined
-- in the pipeline CREATE OR REFRESH MATERIALIZED VIEW statement.
-- Add a COMMENT clause to each column in the SELECT list, e.g.:
--
--   CREATE OR REFRESH MATERIALIZED VIEW `0-ai-trust`.gold.dim_customer
--   COMMENT 'Customer profile...'
--   AS SELECT
--     customer_id  COMMENT 'Unique customer identifier. Primary key...',
--     age_band     COMMENT 'Demographic age group: UNDER_18, 18_24...',
--     ...
--
-- This is tracked as a follow-up task in the pipeline enrichment backlog.
-- ============================================================
