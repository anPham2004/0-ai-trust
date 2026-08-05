-- ============================================================
-- UC DOMAIN TABLE TAGGING — `0-ai-trust`
-- Purpose: Tag all Gold and Semantic tables with domain,
--          audience, and compliance metadata so Genie Ontology
--          knows which tables belong to which AI use case.
--
-- Run on a Databricks SQL warehouse after metric views are deployed.
-- Tags are additive — re-running is safe.
-- ============================================================

-- ── Gold — shared across Banker.AI and Customer.AI ───────────
ALTER TABLE `0-ai-trust`.gold.dim_customer
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.dim_date
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Internal'
  );

ALTER TABLE `0-ai-trust`.gold.fact_service_activity
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.fact_service_case_current
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.fact_application_document
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.fact_application_timeline_event
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential',
    'zero_trust_enforced' = 'true'
  );

ALTER TABLE `0-ai-trust`.gold.dim_stage_action_policy
  SET TAGS (
    'domain'              = 'banker_ai,customer_ai',
    'audience'            = 'banker,customer',
    'ai_eligible'         = 'true',
    'data_classification' = 'Internal',
    'data_status'         = 'pending_business_approval'
  );

-- ── Gold — Banker.AI only ─────────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.dim_organisation
  SET TAGS (
    'domain'              = 'banker_ai',
    'audience'            = 'banker',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.fact_arrangement_current
  SET TAGS (
    'domain'              = 'banker_ai',
    'audience'            = 'banker',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.bridge_organisation_party_role
  SET TAGS (
    'domain'              = 'banker_ai',
    'audience'            = 'banker',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.bridge_organisation_relationship
  SET TAGS (
    'domain'              = 'banker_ai',
    'audience'            = 'banker',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.bridge_application_party_authority
  SET TAGS (
    'domain'              = 'banker_ai',
    'audience'            = 'banker',
    'ai_eligible'         = 'true',
    'data_classification' = 'Confidential'
  );

ALTER TABLE `0-ai-trust`.gold.fact_verification_current
  SET TAGS (
    'domain'              = 'banker_ai',
    'audience'            = 'banker',
    'ai_eligible'         = 'true',
    'data_classification' = 'Restricted',
    'zero_trust_note'     = 'risk_rating_pep_status_sanctions_check_excluded'
  );

-- ── Semantic — Banker.AI views ────────────────────────────────
ALTER TABLE `0-ai-trust`.semantic.mv_customer_overview
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_application_status
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_application_documents
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_application_timeline
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_service_cases
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_service_activity
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_application_next_action
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_organisation_overview
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_arrangement_portfolio
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_party_relationships
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_organisation_relationships
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_application_authority
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_verification_status
  SET TAGS ('domain' = 'banker_ai', 'audience' = 'banker', 'ai_eligible' = 'true');

-- ── Semantic — Customer.AI views ─────────────────────────────
ALTER TABLE `0-ai-trust`.semantic.mv_self_profile
  SET TAGS ('domain' = 'customer_ai', 'audience' = 'customer', 'ai_eligible' = 'true', 'rls_required' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_self_application_status
  SET TAGS ('domain' = 'customer_ai', 'audience' = 'customer', 'ai_eligible' = 'true', 'rls_required' = 'true', 'cr2_compliant' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_self_application_documents
  SET TAGS ('domain' = 'customer_ai', 'audience' = 'customer', 'ai_eligible' = 'true', 'rls_required' = 'true', 'cr2_compliant' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_self_application_timeline
  SET TAGS ('domain' = 'customer_ai', 'audience' = 'customer', 'ai_eligible' = 'true', 'rls_required' = 'true', 'cr2_compliant' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_self_service_cases
  SET TAGS ('domain' = 'customer_ai', 'audience' = 'customer', 'ai_eligible' = 'true', 'rls_required' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_self_service_activity
  SET TAGS ('domain' = 'customer_ai', 'audience' = 'customer', 'ai_eligible' = 'true', 'rls_required' = 'true');

ALTER TABLE `0-ai-trust`.semantic.mv_self_application_next_action
  SET TAGS ('domain' = 'customer_ai', 'audience' = 'customer', 'ai_eligible' = 'true', 'rls_required' = 'true');

-- ── Verification ──────────────────────────────────────────────
-- Run after to confirm tags were applied correctly
SELECT schema_name, table_name, tag_name, tag_value
FROM `0-ai-trust`.information_schema.table_tags
WHERE schema_name IN ('gold', 'semantic')
ORDER BY schema_name, table_name, tag_name;
