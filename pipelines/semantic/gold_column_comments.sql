-- ============================================================
-- GOLD TABLE & COLUMN COMMENTS — `0-ai-trust`
-- Purpose: Enrich UC column-level metadata so Genie Ontology
--          extracts higher-quality, higher-authority snippets
--          from the Gold layer.
--
-- Run on a Databricks SQL warehouse. Safe to re-run.
-- Comments use business meaning, not technical descriptions.
-- ============================================================

-- ── dim_customer ──────────────────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN customer_id
  COMMENT 'Unique customer identifier. Primary key for all individual customer lookups. Join to all fact tables via customer_id.';

ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN age_band
  COMMENT 'Demographic age group: UNDER_18, 18_24, 25_34, 35_44, 45_54, 55_64, 65_PLUS. Age itself is excluded from all AI output — only the banded group is visible.';

ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN state
  COMMENT 'Australian state or territory of the customer primary address (e.g. NSW, VIC, QLD). Used for geographic context in banker pre-call summaries.';

ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN preferred_contact_channel
  COMMENT 'The communication channel the customer prefers: phone, email, branch. Bankers must use this channel when initiating contact. Do not contact via unpreferred channels without permission.';

ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN preferred_language
  COMMENT 'Customer preferred language for communications. Relevant when preparing handover notes or customer-facing written correspondence.';

ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN marketing_opt_in
  COMMENT 'TRUE if the customer has consented to receiving marketing communications. FALSE means no marketing material should be sent — respect this flag at all times.';

ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN satisfaction_score
  COMMENT 'Customer satisfaction score on a 1-5 scale (5 = highest). Derived from support surveys. A score below 3 indicates a dissatisfied customer — review open cases and recent interactions before calling.';

ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN dq_status
  COMMENT 'Data quality status: PASSED (all rules met), WARNING (soft rule failure — treat with caution and flag known_limitations to AI consumer), QUARANTINED (hard rule failure — must never be surfaced in AI output).';

ALTER TABLE `0-ai-trust`.gold.dim_customer
  ALTER COLUMN masking_status
  COMMENT 'Indicates whether PII masking has been applied: CLEAN (no masking needed) or MASKED (name, email, phone are tokenised or redacted). All records surfaced to AI must have masking_status = MASKED.';

-- ── dim_organisation ─────────────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.dim_organisation
  ALTER COLUMN organisation_id
  COMMENT 'Unique organisation identifier. Primary key for all business customer lookups. Join bridge and fact tables via organisation_id.';

ALTER TABLE `0-ai-trust`.gold.dim_organisation
  ALTER COLUMN business_name
  COMMENT 'Trading name of the business (may differ from legal_name). Use business_name for customer-facing references and handover notes.';

ALTER TABLE `0-ai-trust`.gold.dim_organisation
  ALTER COLUMN legal_name
  COMMENT 'Registered legal name of the organisation (as per ASIC records). Use legal_name for formal identification and regulatory documentation.';

ALTER TABLE `0-ai-trust`.gold.dim_organisation
  ALTER COLUMN organisation_type
  COMMENT 'Legal entity structure: PTY_LTD (private company), TRUST, PARTNERSHIP, SOLE_TRADER. Determines the applicable KYB verification pathway and documentation requirements.';

ALTER TABLE `0-ai-trust`.gold.dim_organisation
  ALTER COLUMN industry_code
  COMMENT 'ANZSIC 2006 industry classification code identifying the sector the business operates in. No human-readable lookup table yet — return the raw code.';

ALTER TABLE `0-ai-trust`.gold.dim_organisation
  ALTER COLUMN establishment_date
  COMMENT 'Date the organisation was legally established or incorporated. Used to compute customer tenure (how long the business has banked with NAB).';

ALTER TABLE `0-ai-trust`.gold.dim_organisation
  ALTER COLUMN dq_status
  COMMENT 'Data quality status: PASSED, WARNING (treat with caution), or QUARANTINED (must never be surfaced in AI output).';

-- ── dim_stage_action_policy ──────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.dim_stage_action_policy
  ALTER COLUMN stage
  COMMENT 'Application lifecycle stage this policy applies to, e.g. DOCUMENT_VERIFICATION, ASSESSMENT, APPROVED, DENIED. Combined with pending_action_party as the composite lookup key.';

ALTER TABLE `0-ai-trust`.gold.dim_stage_action_policy
  ALTER COLUMN pending_action_party
  COMMENT 'Who holds responsibility at this stage: CUSTOMER (awaiting document submission) or INTERNAL_TEAM (under bank review). Combined with stage as the composite lookup key.';

ALTER TABLE `0-ai-trust`.gold.dim_stage_action_policy
  ALTER COLUMN banker_action_text
  COMMENT 'Policy-approved wording for the banker to use when advising the customer about their next step. Compliance-vetted — do not rephrase or substitute. Currently returns POLICY_NOT_CONFIGURED pending business owner sign-off.';

ALTER TABLE `0-ai-trust`.gold.dim_stage_action_policy
  ALTER COLUMN customer_safe_action_text
  COMMENT 'Approved plain-language wording safe to surface directly to the customer explaining their next step. CR2-compliant — contains no internal process details. Currently returns POLICY_NOT_CONFIGURED pending business owner sign-off.';

ALTER TABLE `0-ai-trust`.gold.dim_stage_action_policy
  ALTER COLUMN default_sla_days
  COMMENT 'Expected number of business days for this stage to complete. Used to compute SLA breach risk — if days_in_current_stage exceeds this value, the application is at SLA risk.';

ALTER TABLE `0-ai-trust`.gold.dim_stage_action_policy
  ALTER COLUMN is_terminal_stage
  COMMENT 'TRUE if this stage represents a final outcome (APPROVED, DENIED, WITHDRAWN, CANCELLED). No next-action policy exists for terminal stages.';

ALTER TABLE `0-ai-trust`.gold.dim_stage_action_policy
  ALTER COLUMN next_action_code
  COMMENT 'Machine-readable action code summarising the expected next step, e.g. AWAIT_CUSTOMER_DOCS, PROCEED_TO_ASSESSMENT, CLOSE_APPLICATION. Use to drive workflow automation.';

-- ── fact_application_current ─────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN application_id
  COMMENT 'Unique identifier for a customer loan or banking product application. Use as the primary filter for all single-application queries.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN current_stage
  COMMENT 'Stage the application is currently in. Allowed values: SUBMITTED, DOCUMENT_VERIFICATION, ASSESSMENT, APPROVED, DENIED, WITHDRAWN, CANCELLED. Primary routing field for determining what action is needed next.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN pending_action_party
  COMMENT 'Indicates who holds responsibility for progressing the application: CUSTOMER (must upload documents) or INTERNAL_TEAM (under bank review). Used to answer "Who needs to act next?".';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN is_stage_sla_breached
  COMMENT 'TRUE if the application has been in its current stage longer than the policy-defined SLA target (dim_stage_action_policy.default_sla_days). Indicates the application requires urgent attention.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN is_inactive_over_14_days
  COMMENT 'TRUE if the application has had no status change for more than 14 calendar days. Key flag for identifying stalled applications in the banker portfolio review.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN recorded_reason
  COMMENT 'The reason recorded by the banker or system when the application was returned or status was changed. Internal operations field — MUST NOT be surfaced in Customer.AI views (CR2 compliance restriction).';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN missing_document_count
  COMMENT 'Pre-computed count of documents in REQUESTED status (not yet received). Avoids a join to fact_application_document for simple count queries.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN days_in_current_stage
  COMMENT 'Number of calendar days the application has been in its current stage. Compare to default_sla_days in dim_stage_action_policy to assess SLA risk.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN customer_action_required_flag
  COMMENT 'TRUE when the application is waiting for the customer to take action (e.g. submit outstanding documents). Use as the precondition before showing customer_safe_action_text.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN internal_action_required_flag
  COMMENT 'TRUE when the application is waiting for an internal bank team to take action, e.g. credit assessment or document review. Do not expose this to customers.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN loan_goal
  COMMENT 'Purpose of the loan application, e.g. HOME_PURCHASE, INVESTMENT, REFINANCE, DEBT_CONSOLIDATION. Customer-facing context for pre-call summaries and handover notes.';

ALTER TABLE `0-ai-trust`.gold.fact_application_current
  ALTER COLUMN application_type
  COMMENT 'Product category applied for, e.g. HOME_LOAN, PERSONAL_LOAN, CREDIT_CARD. Customer-facing context for pre-call summaries.';

-- ── fact_application_document ────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.fact_application_document
  ALTER COLUMN document_type
  COMMENT 'Type of document required for the application, e.g. PAYSLIP, PROOF_OF_ADDRESS, BANK_STATEMENT, PASSPORT, BUSINESS_REGISTRATION. Used to answer "What documents am I still missing?".';

ALTER TABLE `0-ai-trust`.gold.fact_application_document
  ALTER COLUMN document_status
  COMMENT 'Current state of the document: REQUESTED (outstanding), RECEIVED (uploaded by customer), REJECTED (not accepted by assessor), EXPIRED (previously valid, now out of date), VALIDATED (accepted).';

ALTER TABLE `0-ai-trust`.gold.fact_application_document
  ALTER COLUMN rejection_reason
  COMMENT 'Internal assessor note explaining why a document was rejected, e.g. ILLEGIBLE, INCORRECT_TYPE, EXPIRED_ON_RECEIPT. Internal field — MUST NOT be surfaced in Customer.AI views (CR2 restriction).';

ALTER TABLE `0-ai-trust`.gold.fact_application_document
  ALTER COLUMN is_missing
  COMMENT 'TRUE if the document has been requested but not yet received by the bank. Primary flag for "What documents does the customer still need to submit?".';

ALTER TABLE `0-ai-trust`.gold.fact_application_document
  ALTER COLUMN is_invalid_or_expired
  COMMENT 'TRUE for documents that are either rejected (assessor-rejected) or expired (past their validity date). Use this combined flag when answering "Which documents need attention?".';

ALTER TABLE `0-ai-trust`.gold.fact_application_document
  ALTER COLUMN expiry_date
  COMMENT 'Expiry date of the document. Used to proactively identify documents that will expire soon and prompt re-upload before they become a blocker.';

-- ── fact_application_timeline_event ──────────────────────────
ALTER TABLE `0-ai-trust`.gold.fact_application_timeline_event
  ALTER COLUMN event_family
  COMMENT 'High-level category of the event: STAGE_CHANGE (application moved stage), STATUS_CHANGE (status updated), DOCUMENT_EVENT (document submitted/rejected), ASSIGNMENT_CHANGE (team reassigned). Use to filter events by type.';

ALTER TABLE `0-ai-trust`.gold.fact_application_timeline_event
  ALTER COLUMN event_type
  COMMENT 'Specific event that occurred, e.g. STAGE_ENTERED, STATUS_RETURNED, DOCUMENT_REQUESTED, DOCUMENT_RECEIVED. Use with from_value/to_value to reconstruct a human-readable timeline.';

ALTER TABLE `0-ai-trust`.gold.fact_application_timeline_event
  ALTER COLUMN recorded_reason
  COMMENT 'Reason recorded by the banker or system at the time of this event — most relevant for STATUS_RETURNED events to answer "Why was this application returned?". Internal field — not safe for customer exposure (CR2).';

ALTER TABLE `0-ai-trust`.gold.fact_application_timeline_event
  ALTER COLUMN event_origin
  COMMENT 'Who triggered this event: SYSTEM (automated), BANKER (manual bank action), CUSTOMER (customer-initiated, e.g. document upload). Useful for understanding what caused a stage change.';

ALTER TABLE `0-ai-trust`.gold.fact_application_timeline_event
  ALTER COLUMN from_value
  COMMENT 'The state (stage or status) the application was in before this event. Use with event_type = STAGE_CHANGE to answer "What stage was it in before the current one?" (B18).';

ALTER TABLE `0-ai-trust`.gold.fact_application_timeline_event
  ALTER COLUMN to_value
  COMMENT 'The state (stage or status) the application moved to after this event. Use to reconstruct a step-by-step timeline of the application journey.';

-- ── fact_subject_context_snapshot ────────────────────────────
ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN entity_id
  COMMENT 'The customer_id or organisation_id this context snapshot covers. Use as the join key back to dim_customer or dim_organisation.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN entity_type
  COMMENT 'CUSTOMER (individual) or ORGANISATION (business entity). Always filter on entity_type when joining to fact_subject_context_snapshot to avoid cross-entity data bleed.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN verification_status
  COMMENT 'KYC status (for CUSTOMER rows) or KYB status (for ORGANISATION rows): VERIFIED, PENDING, FAILED. High-level compliance indicator — risk_rating, pep_status, and sanctions_check are intentionally excluded from this snapshot.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN dq_status
  COMMENT 'Data quality status: PASSED, WARNING (treat with caution and surface known_limitations to the AI consumer), or QUARANTINED (must never be included in any AI output). Every Gold AI record must pass this gate.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN masking_status
  COMMENT 'Indicates whether PII masking was applied to source records contributing to this snapshot: CLEAN or MASKED. All records surfaced to AI agents must have masking_status = MASKED.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN open_case_count
  COMMENT 'Pre-aggregated count of open (unresolved) service cases for this entity. A high number may indicate a dissatisfied customer requiring priority attention before a call or meeting.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN sla_breached_case_count
  COMMENT 'Pre-aggregated count of service cases that have exceeded their SLA deadline. A non-zero value indicates escalation may be required.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN missing_document_count
  COMMENT 'Pre-aggregated count of outstanding documents across all open applications for this entity. Enables fast "does this customer have outstanding docs?" checks without joining to fact_application_document.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN active_arrangement_count
  COMMENT 'Pre-aggregated count of active banking products held by this entity. Surfaced in pre-call summaries to give the banker a quick view of the customer product footprint.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN latest_application_status
  COMMENT 'Status of the most recently submitted application for this entity, e.g. PENDING, APPROVED, DENIED. Quick status check without joining to fact_application_current.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN latest_application_stage
  COMMENT 'Stage of the most recently submitted application, e.g. DOCUMENT_VERIFICATION, ASSESSMENT. Quick stage check without joining to fact_application_current.';

ALTER TABLE `0-ai-trust`.gold.fact_subject_context_snapshot
  ALTER COLUMN top_case_type_90d
  COMMENT 'The most frequently raised service case category for this entity in the last 90 days, e.g. CARD_DISPUTE, ADDRESS_CHANGE. Indicates the customer most common pain point.';

-- ── fact_service_case_current ────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.fact_service_case_current
  ALTER COLUMN case_type
  COMMENT 'Category of the service case, e.g. CARD_DISPUTE, ADDRESS_CHANGE, DOCUMENT_QUERY, PRODUCT_ENQUIRY, COMPLAINT. The most frequent case_type in the last 90 days is pre-aggregated as top_case_type_90d on fact_subject_context_snapshot.';

ALTER TABLE `0-ai-trust`.gold.fact_service_case_current
  ALTER COLUMN is_sla_breached
  COMMENT 'TRUE if the case has been open longer than the service SLA target. Indicates the case requires escalation or priority attention.';

ALTER TABLE `0-ai-trust`.gold.fact_service_case_current
  ALTER COLUMN resolution_summary
  COMMENT 'Internal summary of how the case was resolved. Ops-facing field — MUST NOT be surfaced in Customer.AI views (CR2 restriction).';

ALTER TABLE `0-ai-trust`.gold.fact_service_case_current
  ALTER COLUMN is_business_case
  COMMENT 'TRUE if this case was raised by or on behalf of a business customer. Use to filter to business cases in Banker.AI Business context (BB25, BB26).';

ALTER TABLE `0-ai-trust`.gold.fact_service_case_current
  ALTER COLUMN priority
  COMMENT 'Case priority level: LOW, MEDIUM, HIGH, URGENT. HIGH and URGENT cases should be flagged in the pre-call summary. Set by the support team based on customer impact.';

-- ── bridge_organisation_party_role ───────────────────────────
ALTER TABLE `0-ai-trust`.gold.bridge_organisation_party_role
  ALTER COLUMN party_role
  COMMENT 'The role the person holds in relation to the organisation: DIRECTOR, AUTHORISED_REPRESENTATIVE, SIGNATORY, BENEFICIAL_OWNER, GUARANTOR. Determines who can legally discuss and sign for the business.';

ALTER TABLE `0-ai-trust`.gold.bridge_organisation_party_role
  ALTER COLUMN authority_level
  COMMENT 'Scope of authority: FULL, LIMITED, SIGNING_ONLY, DOCUMENT_SUBMISSION_ONLY. Determines what the person is permitted to do on behalf of the organisation.';

ALTER TABLE `0-ai-trust`.gold.bridge_organisation_party_role
  ALTER COLUMN is_active
  COMMENT 'TRUE if this person currently holds the role. FALSE means the role has ended (former representative). Always filter to is_active = TRUE for current authority queries.';

-- ── fact_verification_current ────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.fact_verification_current
  ALTER COLUMN record_type
  COMMENT 'KYC (Know Your Customer — individual identity verification) or KYB (Know Your Business — entity verification). Filter by record_type for individual vs. business verification questions.';

ALTER TABLE `0-ai-trust`.gold.fact_verification_current
  ALTER COLUMN verification_status
  COMMENT 'Current verification outcome: VERIFIED (confirmed), PENDING (in progress), FAILED (unsuccessful). Zero Trust: risk_rating, pep_status, and sanctions_check are intentionally excluded from this Gold view.';

ALTER TABLE `0-ai-trust`.gold.fact_verification_current
  ALTER COLUMN abn_verified
  COMMENT 'TRUE if the organisation ABN has been cross-checked and confirmed against ATO records. A mandatory step in the KYB pathway for Australian business entities.';

ALTER TABLE `0-ai-trust`.gold.fact_verification_current
  ALTER COLUMN beneficial_ownership_verified
  COMMENT 'TRUE if all beneficial owners (holding >25% of the entity) have been identified and identity-verified. A critical AML/CTF requirement for business account opening.';

ALTER TABLE `0-ai-trust`.gold.fact_verification_current
  ALTER COLUMN review_overdue_flag
  COMMENT 'TRUE if the scheduled next_review_date has passed without a completed re-verification. Indicates the verification record is out of date and requires renewal before proceeding.';

ALTER TABLE `0-ai-trust`.gold.fact_verification_current
  ALTER COLUMN next_review_date
  COMMENT 'Scheduled date for the next periodic verification review. Check this when assessing whether ongoing compliance obligations are being met (BB12).';

-- ── fact_service_activity ─────────────────────────────────────
ALTER TABLE `0-ai-trust`.gold.fact_service_activity
  ALTER COLUMN activity_type
  COMMENT 'Type of the support activity: CALL, CHAT, EMAIL, BRANCH_VISIT, NOTE. Use to answer "When did the customer last call us?" or "What channel do they prefer to use?".';

ALTER TABLE `0-ai-trust`.gold.fact_service_activity
  ALTER COLUMN channel
  COMMENT 'Communication channel used for this activity. Cross-reference with preferred_contact_channel on dim_customer to confirm the customer was contacted through their preferred channel.';

ALTER TABLE `0-ai-trust`.gold.fact_service_activity
  ALTER COLUMN topic
  COMMENT 'Topic or subject of the interaction, e.g. LOAN_STATUS, DOCUMENT_QUERY, CARD_DISPUTE. Cross-reference with top_case_type_90d on fact_subject_context_snapshot for consistency.';

ALTER TABLE `0-ai-trust`.gold.fact_service_activity
  ALTER COLUMN is_customer_contact
  COMMENT 'TRUE if this activity was a direct interaction with the customer (e.g. inbound call, outbound email). FALSE for internal team activities (e.g. internal notes, team handovers). Filter to TRUE for customer interaction counts.';

ALTER TABLE `0-ai-trust`.gold.fact_service_activity
  ALTER COLUMN escalated_flag
  COMMENT 'TRUE if this interaction was escalated to a senior officer or manager. A non-zero count of escalated interactions indicates elevated customer dissatisfaction.';

-- ============================================================
-- VERIFICATION QUERY
-- Run after to confirm comments were applied correctly
-- ============================================================
SELECT table_name, column_name, comment
FROM `0-ai-trust`.information_schema.columns
WHERE table_schema = 'gold'
  AND comment IS NOT NULL
ORDER BY table_name, ordinal_position;
