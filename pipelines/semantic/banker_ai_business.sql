-- Banker.AI — Business Customer (BB) Semantic Layer
-- Audience: Banker.AI agents (BB1-BB29)
-- 6 metric views deployed to `0-ai-trust`.semantic
-- Submit each statement separately via: databricks experimental aitools tools statement submit --file <file> --warehouse <ID>
--
-- Patches applied over base Opus output:
--   [1] Added field-level comment: annotations tracing each field to a business question ID (BB1-BB29)
--   [2] entity_type filter already present in joins — verified correct
--   [3] BB7 correctly served via mv_application_authority (pipeline enrichment not needed)
-- Note: BB22 (SLA breach per org role) and BB29 (handover requiring enriched pending_action_party)
--       remain partially covered until pending_action_party pipeline enrichment is complete.

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_organisation_overview
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.dim_organisation"
  comment: "Organisation profile and cross-domain activity snapshot (BB1-BB5)"
  joins:
    - name: ctx
      source: "`0-ai-trust`.gold.fact_subject_context_snapshot"
      on: "ctx.entity_id = source.organisation_id AND ctx.entity_type = 'ORGANISATION'"
  dimensions:
    - name: Organisation ID
      expr: source.organisation_id
      comment: "Primary lookup key — filter here to scope all BB questions to a specific organisation (BB1)"
    - name: Business Name
      expr: source.business_name
      comment: "Trading name of the organisation (BB1, BB2)"
    - name: Legal Name
      expr: source.legal_name
      comment: "Registered legal name — use for formal identification (BB2)"
    - name: Organisation Type
      expr: source.organisation_type
      comment: "Entity type, e.g. PTY_LTD, TRUST, PARTNERSHIP, SOLE_TRADER (BB2)"
    - name: Industry Code
      expr: source.industry_code
      comment: "ANZSIC industry classification code — identifies the sector the organisation operates in (BB3)"
    - name: Industry Code Version
      expr: source.industry_code_version
      comment: "Classification scheme version, e.g. ANZSIC 2006 — no lookup table yet for human-readable names (BB3)"
    - name: Establishment Year
      expr: "DATE_TRUNC('YEAR', source.establishment_date)"
      comment: "Year the organisation was established — use with Snapshot Timestamp to compute tenure as customer (BB4)"
    - name: Profile Last Updated
      expr: source.profile_last_updated_at
      comment: "When the organisation profile was last modified — BB5"
    - name: Latest Application Status
      expr: ctx.latest_application_status
      comment: "Current status of the most recent business application, e.g. PENDING, APPROVED (BB18, BB19)"
    - name: Latest Application Stage
      expr: ctx.latest_application_stage
      comment: "Current pipeline stage of the latest business application (BB19)"
    - name: Verification Status
      expr: ctx.verification_status
      comment: "KYB verification status: VERIFIED, PENDING, FAILED — high-level compliance indicator (BB11)"
  measures:
    - name: Open Case Count
      expr: SUM(ctx.open_case_count)
      comment: "Currently open service cases — BB25"
    - name: SLA Breached Case Count
      expr: SUM(ctx.sla_breached_case_count)
      comment: "Service cases that have exceeded their SLA deadline — BB27"
    - name: Open Application Count
      expr: SUM(ctx.open_application_count)
      comment: "Applications not yet in a terminal state — BB18"
    - name: Missing Document Count
      expr: SUM(ctx.missing_document_count)
      comment: "Outstanding missing documents across open applications — BB20"
    - name: Active Arrangement Count
      expr: SUM(ctx.active_arrangement_count)
      comment: "Total active banking products held by the organisation — BB13"
    - name: Active Authorised Representatives
      expr: SUM(ctx.active_authorised_representative_count)
      comment: "Pre-aggregated count of currently active authorised representatives — BB6, BB8. For individual names/roles, query mv_party_relationships."
    - name: Active Directors
      expr: SUM(ctx.active_director_count)
      comment: "Pre-aggregated count of currently active directors — BB8. For individual names, query mv_party_relationships."
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_arrangement_portfolio
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_arrangement_current"
  comment: "Business loan and arrangement portfolio (BB13-BB17)"
  dimensions:
    - name: Arrangement ID
      expr: arrangement_id
      comment: "Unique banking arrangement identifier"
    - name: Customer ID
      expr: customer_id
      comment: "Individual customer (director or signatory) linked to this arrangement"
    - name: Organisation ID
      expr: organisation_id
      comment: "Organisation this arrangement belongs to — primary filter for BB13-BB17"
    - name: Arrangement Type
      expr: arrangement_type
      comment: "High-level product type: LOAN, MORTGAGE, CREDIT_CARD, TRANSACTION_ACCOUNT (BB13, BB15)"
    - name: Arrangement Subtype
      expr: arrangement_subtype
      comment: "More specific product subtype, e.g. VARIABLE_RATE_LOAN, FIXED_LOAN (BB15)"
    - name: Arrangement Status
      expr: arrangement_status
      comment: "Lifecycle status: ACTIVE, INACTIVE, CLOSED, MATURED — use to find closed/approaching-maturity products (BB17)"
    - name: Is Active
      expr: is_active
      comment: "TRUE if the arrangement is currently active — filter for BB13, BB14"
    - name: Is Business Arrangement
      expr: is_business_arrangement
      comment: "TRUE if this is a commercial arrangement (vs. personal)"
    - name: Interest Type
      expr: interest_type
      comment: "FIXED or VARIABLE — relevant context for loan arrangements (BB16)"
    - name: Repayment Frequency
      expr: repayment_frequency
      comment: "How often repayments are due: MONTHLY, FORTNIGHTLY, WEEKLY"
    - name: Currency
      expr: currency
      comment: "Currency of the arrangement — typically AUD"
    - name: Start Year
      expr: "DATE_TRUNC('YEAR', start_date)"
      comment: "Year the arrangement was initiated — tenure context"
    - name: Maturity Date
      expr: maturity_date
      comment: "Date the arrangement matures or expires — use to identify arrangements approaching maturity (BB17)"
  measures:
    - name: Total Arrangements
      expr: COUNT(1)
      comment: "Total arrangements in scope — BB13"
    - name: Active Arrangements
      expr: COUNT_IF(is_active)
      comment: "Currently active arrangements — BB13, BB14"
    - name: Avg Days to Maturity
      expr: AVG(days_to_maturity)
      comment: "Average days until arrangements mature — identifies near-maturity products (BB17)"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_party_relationships
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.bridge_organisation_party_role"
  comment: "Person-to-organisation roles: director, authorised rep (BB6-BB10)"
  dimensions:
    - name: Customer ID
      expr: customer_id
      comment: "Individual person (director, authorised rep, etc.) — person-level identifier (BB6, BB8)"
    - name: Organisation ID
      expr: organisation_id
      comment: "Organisation this person is associated with — filter for BB6, BB8, BB9"
    - name: Party Role
      expr: party_role
      comment: "Role the person holds: DIRECTOR, AUTHORISED_REPRESENTATIVE, SIGNATORY, BENEFICIAL_OWNER, GUARANTOR (BB8)"
    - name: Authority Level
      expr: authority_level
      comment: "Scope of authority granted to this person — context for BB7 (who can discuss the application)"
    - name: Is Active
      expr: is_active
      comment: "TRUE if this role is currently active — FALSE indicates a former representative (BB9)"
    - name: Start Date
      expr: start_date
      comment: "When this person's role commenced — historical context"
    - name: End Date
      expr: end_date
      comment: "When this person's role ended — NULL if still active (BB9)"
  measures:
    - name: Total Roles
      expr: COUNT(1)
      comment: "Total role records in scope"
    - name: Active Roles
      expr: COUNT_IF(is_active)
      comment: "Currently active person-organisation roles — BB6"
    - name: Inactive Roles
      expr: "COUNT_IF(NOT is_active)"
      comment: "Historical or terminated roles — BB9: 'Are any reps no longer active?'"
    - name: Active Director Count
      expr: "COUNT_IF(is_active AND party_role = 'DIRECTOR')"
      comment: "Number of currently active directors — BB8"
    - name: Active Authorised Rep Count
      expr: "COUNT_IF(is_active AND party_role = 'AUTHORISED_REPRESENTATIVE')"
      comment: "Number of currently active authorised representatives — BB6"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_organisation_relationships
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.bridge_organisation_relationship"
  comment: "Organisation-to-organisation relationships with resolved names (BB8, BB9, BB10)"
  joins:
    - name: source_org
      source: "`0-ai-trust`.gold.dim_organisation"
      on: "source_org.organisation_id = source.source_organisation_id"
    - name: target_org
      source: "`0-ai-trust`.gold.dim_organisation"
      on: "target_org.organisation_id = source.target_organisation_id"
  dimensions:
    - name: Source Organisation ID
      expr: source.source_organisation_id
      comment: "The primary organisation in the relationship — filter here with the org you are researching (BB10)"
    - name: Source Organisation Name
      expr: source_org.business_name
      comment: "Business name of the source organisation — resolved for human-readable output (BB10)"
    - name: Target Organisation ID
      expr: source.target_organisation_id
      comment: "The related/connected organisation — e.g. subsidiary, parent, guarantor (BB10)"
    - name: Target Organisation Name
      expr: target_org.business_name
      comment: "Business name of the connected organisation — resolved for human-readable output (BB10)"
    - name: Relationship Type
      expr: source.relationship_type
      comment: "Nature of the relationship: SUBSIDIARY, PARENT, GUARANTOR, AFFILIATED — BB10"
    - name: Is Active
      expr: source.is_active
      comment: "TRUE if this inter-organisation relationship is currently active — BB10"
    - name: Start Date
      expr: source.start_date
      comment: "When this relationship was established — historical context"
  measures:
    - name: Total Relationships
      expr: COUNT(1)
      comment: "Total inter-organisation relationship records in scope — BB10"
    - name: Active Relationships
      expr: COUNT_IF(source.is_active)
      comment: "Currently active organisation-to-organisation relationships — BB10"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_application_authority
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.bridge_application_party_authority"
  comment: "Application signing authority by organisation and party (BB7, BB21, BB22)"
  dimensions:
    - name: Application ID
      expr: application_id
      comment: "Filter to a specific application to answer 'Who is authorised to discuss this application?' (BB7)"
    - name: Organisation ID
      expr: organisation_id
      comment: "Organisation the application belongs to — BB7"
    - name: Customer ID
      expr: customer_id
      comment: "Individual person who holds authority on this application (BB7)"
    - name: Party Role
      expr: party_role
      comment: "Role of the authority holder: DIRECTOR, AUTHORISED_REPRESENTATIVE, GUARANTOR (BB7)"
    - name: Authority Level
      expr: authority_level
      comment: "Scope of the granted authority, e.g. FULL, LIMITED, SIGNING_ONLY (BB7)"
    - name: Is Active
      expr: is_active
      comment: "TRUE if this authority record is currently active (BB7)"
    - name: Derivation Method
      expr: derivation_method
      comment: "How authority was determined: EXPLICIT (recorded) or DERIVED (inferred from org role)"
    - name: Limitation Code
      expr: limitation_code
      comment: "Any restrictions on this authority, e.g. DOCUMENT_SUBMISSION_ONLY (BB7)"
  measures:
    - name: Total Authority Records
      expr: COUNT(1)
      comment: "Total authority records for the application"
    - name: Active Authority Count
      expr: COUNT_IF(is_active)
      comment: "Currently active authority holders who can discuss this application (BB7)"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_verification_status
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_verification_current"
  comment: "KYC/KYB verification status — zero-trust fields excluded (BB11, BB12). Note: document_types is entity-level (comma-separated string); per-document rows require a source schema addition."
  # Zero-trust exclusions: risk_rating, pep_status, sanctions_check
  dimensions:
    - name: Customer ID
      expr: customer_id
      comment: "Individual person being verified — for KYC records (individuals within the organisation)"
    - name: Organisation ID
      expr: organisation_id
      comment: "Organisation being verified — primary filter for KYB questions (BB11, BB12)"
    - name: Record Type
      expr: record_type
      comment: "KYC (individual verification) or KYB (business entity verification) — BB11"
    - name: Verification Status
      expr: verification_status
      comment: "Current verification outcome: VERIFIED, PENDING, FAILED — BB11"
    - name: Verification Method
      expr: verification_method
      comment: "Method used: DIGITAL_ID, MANUAL_REVIEW, DOCUMENT_CHECK — BB11"
    - name: Verification Month
      expr: "DATE_TRUNC('MONTH', verification_date)"
      comment: "Month verification was completed — historical trend"
    - name: Last Review Date
      expr: last_review_date
      comment: "When the verification was last reviewed — BB12"
    - name: Next Review Date
      expr: next_review_date
      comment: "Scheduled date for next verification review — BB12"
    - name: Review Overdue
      expr: review_overdue_flag
      comment: "TRUE if the next review date has passed without a completed review — BB12"
    - name: Document Types
      expr: document_types
      comment: "Comma-separated list of document categories on record, e.g. PASSPORT,BUSINESS_REG — BB12"
    - name: ABN Verified
      expr: abn_verified
      comment: "TRUE if the organisation's ABN has been verified against ATO records — BB11"
    - name: ASIC Check Status
      expr: asic_check_status
      comment: "Result of ASIC company register check: PASSED, FAILED, NOT_CHECKED — BB11"
    - name: Beneficial Ownership Verified
      expr: beneficial_ownership_verified
      comment: "TRUE if all beneficial owners (>25% stake) have been identified and verified — BB11"
  measures:
    - name: Total Records
      expr: COUNT(1)
      comment: "Total verification records in scope"
    - name: Verified Count
      expr: "COUNT_IF(verification_status = 'VERIFIED')"
      comment: "Records with a VERIFIED status — BB11"
    - name: Pending Count
      expr: "COUNT_IF(verification_status = 'PENDING')"
      comment: "Records still awaiting verification — BB11"
    - name: Failed Count
      expr: "COUNT_IF(verification_status = 'FAILED')"
      comment: "Records where verification has failed — BB11"
    - name: Overdue Review Count
      expr: COUNT_IF(review_overdue_flag)
      comment: "Verification records where review is overdue — BB12"
    - name: Verified Document Type Count
      expr: "SIZE(SPLIT(document_types, ','))"
      comment: "Count of document type categories on record; entity-level only — not per-document rows (BB12)"
$$;
