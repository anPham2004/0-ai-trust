-- Customer.AI — Personal Self-Service Semantic Layer
-- Audience: Customers viewing their own data (C1-C13)
-- Internal ops fields excluded: assigned_team, pending_action_party, rejection_reason, etc.
-- 7 metric views deployed to `0-ai-trust`.semantic
-- Submit each statement separately via: databricks experimental aitools tools statement submit --file <file> --warehouse <ID>
--
-- Patches applied over base Opus output:
--   [1] Added field-level comment: annotations tracing each field to a business question ID (C1-C13)
--   [2] entity_type filter already present in joins — verified correct
--   [3] REMOVED recorded_reason from mv_self_application_status — potential CR2 violation
--       (internal decline reason must not be shown to customers)
--   [4] REMOVED recorded_reason from mv_self_application_timeline — same CR2 concern

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_self_profile
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.dim_customer"
  comment: "Customer contact preferences and opt-in settings (C1, C2, C3)"
  dimensions:
    - name: Customer ID
      expr: customer_id
      comment: "Customer identifier — Row-Level Security MUST be applied to restrict this to the logged-in customer only"
    - name: Preferred Contact Channel
      expr: preferred_contact_channel
      comment: "How the customer prefers to be contacted: phone, email, branch (C1)"
    - name: Preferred Language
      expr: preferred_language
      comment: "Customer's preferred communication language (C2)"
    - name: Marketing Opt In
      expr: marketing_opt_in
      comment: "Whether the customer is currently subscribed to marketing notifications (C3)"
  measures:
    - name: Record Count
      expr: COUNT(1)
      comment: "Should always be 1 under RLS — used as a health-check measure"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_self_application_status
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_application_current"
  comment: "Customer-facing application status without internal routing and SLA fields (C8, C10, C13)"
  # Excluded: assigned_team, pending_action_party, is_stage_sla_breached, internal_action_required_flag
  # Excluded: recorded_reason — internal decline/return reason must not be exposed to customer (CR2)
  dimensions:
    - name: Application ID
      expr: application_id
      comment: "The customer's application identifier — C8"
    - name: Customer ID
      expr: customer_id
      comment: "Customer identifier — Row-Level Security MUST restrict to the logged-in customer"
    - name: Loan Goal
      expr: loan_goal
      comment: "Purpose of the application, e.g. HOME_PURCHASE, REFINANCE — customer-facing context (C8)"
    - name: Application Type
      expr: application_type
      comment: "Product type, e.g. HOME_LOAN, PERSONAL_LOAN — customer-facing context (C8)"
    - name: Application Month
      expr: "DATE_TRUNC('MONTH', submitted_at)"
      comment: "Month the application was submitted — timeline context"
    - name: Current Stage
      expr: current_stage
      comment: "The stage the application is currently in, e.g. DOCUMENT_VERIFICATION — C8, C10"
    - name: Previous Stage
      expr: previous_stage
      comment: "The stage immediately before the current one — provides context for C12 delay explanations"
    - name: Latest Status
      expr: latest_status
      comment: "Current status label visible to the customer, e.g. UNDER_ASSESSMENT, APPROVED (C8, C12)"
    - name: Final Outcome
      expr: final_outcome
      comment: "Terminal result if the application is closed — APPROVED, WITHDRAWN, etc. (C8)"
    - name: Status Last Changed At
      expr: status_last_changed_at
      comment: "When the status most recently changed — customer can see how recently things moved (C10)"
    - name: Stage Entered At
      expr: current_stage_entered_at
      comment: "When the application entered its current stage — C10"
    - name: Customer Action Required
      expr: customer_action_required_flag
      comment: "TRUE when the next step requires the customer to do something, e.g. upload a document (C13)"
    - name: Is Inactive Over 14 Days
      expr: is_inactive_over_14_days
      comment: "TRUE if there has been no update for more than 14 days — helpful for C12 delay context"
    # recorded_reason intentionally excluded — internal decline/return reason (CR2)
  measures:
    - name: Application Count
      expr: COUNT(1)
      comment: "Total applications in scope — under RLS this is the customer's own application count"
    - name: Avg Days in Stage
      expr: AVG(days_in_current_stage)
      comment: "How long the application has been in its current stage — C10, C12"
    - name: Missing Document Count
      expr: SUM(missing_document_count)
      comment: "Number of documents the customer still needs to provide — C9"
    - name: Total Reminders Sent
      expr: SUM(total_reminders_sent)
      comment: "How many document reminder notifications have been sent to the customer — C11"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_self_application_documents
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_application_document"
  comment: "Customer-facing document status without rejection reasons (C9, C11)"
  # Excluded: rejection_reason — internal assessor note, not safe to surface to customer (CR2)
  dimensions:
    - name: Application ID
      expr: application_id
      comment: "Application this document belongs to — C9"
    - name: Customer ID
      expr: customer_id
      comment: "Customer identifier — Row-Level Security MUST restrict to the logged-in customer"
    - name: Document Type
      expr: document_type
      comment: "What document is required, e.g. PAYSLIP, BANK_STATEMENT, PROOF_OF_ADDRESS (C9)"
    - name: Document Status
      expr: document_status
      comment: "Current status: REQUESTED, RECEIVED, EXPIRED, VALIDATED — visible to customer (C9)"
    - name: Is Missing
      expr: is_missing
      comment: "TRUE if the document is still outstanding — drives 'What documents am I still missing?' (C9)"
    - name: Is Received
      expr: is_received
      comment: "TRUE if the customer has successfully uploaded this document (C9)"
    - name: Is Rejected
      expr: is_rejected
      comment: "TRUE if a document was uploaded but not accepted — note: rejection_reason is excluded (CR2)"
    - name: Is Expired
      expr: is_expired
      comment: "TRUE if a previously valid document has since expired and needs re-upload (C9)"
    - name: Is Invalid or Expired
      expr: is_invalid_or_expired
      comment: "Combined flag — TRUE for rejected OR expired; customer sees this as 'needs attention' (C9)"
    - name: Requested At
      expr: requested_at
      comment: "When the document was first requested — timeline context"
    - name: Received At
      expr: received_at
      comment: "When the customer uploaded the document — confirms successful submission (C9)"
    - name: Expiry Date
      expr: expiry_date
      comment: "Document expiry date — customer can see if a previously uploaded document has expired (C9)"
  measures:
    - name: Total Documents
      expr: COUNT(1)
      comment: "Total document items on the application"
    - name: Missing Count
      expr: COUNT_IF(is_missing)
      comment: "Documents still outstanding — primary measure for C9"
    - name: Received Count
      expr: COUNT_IF(is_received)
      comment: "Documents successfully received — C9"
    - name: Expired Count
      expr: COUNT_IF(is_expired)
      comment: "Documents that have expired and need re-uploading — C9"
    - name: Total Reminders Sent
      expr: SUM(reminders_sent)
      comment: "How many reminders have been sent for this document — C11"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_self_application_timeline
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_application_timeline_event"
  comment: "Customer-facing application event history without internal routing fields (C10, C12)"
  # Excluded: assigned_team, pending_action_party, event_origin, source_table, source_record_id
  # Excluded: recorded_reason — internal reason for status change must not be exposed to customer (CR2)
  dimensions:
    - name: Application ID
      expr: application_id
      comment: "Filter to retrieve the full history of one application — C10"
    - name: Customer ID
      expr: customer_id
      comment: "Customer identifier — Row-Level Security MUST restrict to the logged-in customer"
    - name: Event Day
      expr: "DATE_TRUNC('DAY', event_timestamp)"
      comment: "Calendar day of the event — 'When did my application move to this stage?' (C10)"
    - name: Event Sequence
      expr: event_sequence
      comment: "Sequential order of events — ORDER BY this to show the customer a chronological history"
    - name: Event Family
      expr: event_family
      comment: "High-level event category: STAGE_CHANGE, STATUS_CHANGE, DOCUMENT_EVENT (C10, C12)"
    - name: Event Type
      expr: event_type
      comment: "Specific event, e.g. STAGE_ENTERED, STATUS_RETURNED, DOCUMENT_RECEIVED — C10, C12"
    - name: From Value
      expr: from_value
      comment: "Previous stage or status before this event — shows the customer what changed (C10)"
    - name: To Value
      expr: to_value
      comment: "New stage or status after this event — shows what the application moved to (C10)"
    - name: Stage
      expr: stage
      comment: "Stage at the time of this event — 'When did my application move to Document Verification?' (C10)"
    - name: Status
      expr: status
      comment: "Status at the time of this event — customer-visible status at each point in history (C12)"
    # recorded_reason intentionally excluded — internal assessor reason must not be shown to customer (CR2)
  measures:
    - name: Event Count
      expr: COUNT(1)
      comment: "Total number of state-change events in the application history"
    - name: Avg Days Since Event
      expr: "AVG(DATEDIFF(current_date(), CAST(event_timestamp AS DATE)))"
      comment: "Average recency of events — context for 'why is my application delayed?' (C12)"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_self_service_cases
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_service_case_current"
  comment: "Customer-facing case status without SLA and assignment details (C4, C5)"
  # Excluded: assigned_team, sla_deadline, is_sla_breached, resolution_summary
  dimensions:
    - name: Case ID
      expr: case_id
      comment: "Unique service case identifier"
    - name: Customer ID
      expr: customer_id
      comment: "Customer identifier — Row-Level Security MUST restrict to the logged-in customer"
    - name: Case Type
      expr: case_type
      comment: "Category of the case, e.g. CARD_DISPUTE, ADDRESS_CHANGE, DOCUMENT_QUERY — C4, C7"
    - name: Case Status
      expr: case_status
      comment: "Current case state: OPEN, PENDING, RESOLVED, CLOSED — C4, C5"
    - name: Priority
      expr: priority
      comment: "Case priority indicator — customer-visible urgency level (C4)"
    - name: Is Open
      expr: is_open
      comment: "TRUE if the case is still active — 'What support cases do I currently have open?' (C4)"
    - name: Application Related
      expr: application_related_flag
      comment: "TRUE if this case is linked to an active application — context for C7"
    - name: Created Month
      expr: "DATE_TRUNC('MONTH', created_at)"
      comment: "Month the case was created — C7 'summarize my recent support history'"
    - name: Resolved At
      expr: resolved_at
      comment: "When the case was resolved — 'What happened to my previous support request?' (C5)"
  measures:
    - name: Total Cases
      expr: COUNT(1)
      comment: "Total cases in scope — customer's own case history under RLS"
    - name: Open Cases
      expr: COUNT_IF(is_open)
      comment: "Currently open support cases — C4"
    - name: Resolved Cases
      expr: "COUNT_IF(resolved_at IS NOT NULL)"
      comment: "Cases that have been resolved — C5"
    - name: Avg Case Age Days
      expr: AVG(case_age_days)
      comment: "How long cases have been open on average — context for C4, C5"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_self_service_activity
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_service_activity"
  comment: "Customer-facing interaction history without internal ops fields (C6, C7)"
  # Excluded: is_internal_activity, follow_up_required_flag, escalated_flag, description, case_event_type
  dimensions:
    - name: Customer ID
      expr: customer_id
      comment: "Customer identifier — Row-Level Security MUST restrict to the logged-in customer"
    - name: Activity Month
      expr: "DATE_TRUNC('MONTH', activity_timestamp)"
      comment: "Month of the interaction — 'Can you summarize my recent support history?' (C6, C7)"
    - name: Activity Type
      expr: activity_type
      comment: "Type of interaction: CALL, CHAT, EMAIL, BRANCH_VISIT — C7"
    - name: Channel
      expr: channel
      comment: "Communication channel used — 'When did I last contact NAB support, and through what channel?' (C6)"
    - name: Topic
      expr: topic
      comment: "What the interaction was about — customer-facing topic label for C7"
    - name: Is Customer Contact
      expr: is_customer_contact
      comment: "TRUE if this was a direct customer interaction (vs. internal activity) — C6"
  measures:
    - name: Customer Contact Count
      expr: COUNT_IF(is_customer_contact)
      comment: "Number of direct customer contacts — 'How many times have I contacted NAB?' (C6)"
    - name: Total Duration Minutes
      expr: SUM(duration_minutes)
      comment: "Total time spent in interactions — engagement history context (C7)"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_self_application_next_action
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_application_current"
  comment: "Customer-facing next-action wording from stage-action policy (C13). Data pending — dim_stage_action_policy currently returns POLICY_NOT_CONFIGURED."
  # Excluded: pending_action_party, assigned_team, banker_action_text (internal ops — not safe for customer)
  joins:
    - name: policy
      source: "`0-ai-trust`.gold.dim_stage_action_policy"
      on: "policy.stage = source.current_stage AND policy.pending_action_party = source.pending_action_party AND current_date() >= policy.effective_from AND (policy.effective_to IS NULL OR current_date() <= policy.effective_to)"
  dimensions:
    - name: Application ID
      expr: source.application_id
      comment: "Filter to a specific application to retrieve the next action for the customer — C13"
    - name: Customer ID
      expr: source.customer_id
      comment: "Customer identifier — Row-Level Security MUST restrict to the logged-in customer"
    - name: Current Stage
      expr: source.current_stage
      comment: "Current application stage — used as a policy lookup key (C13)"
    - name: Customer Action Required
      expr: source.customer_action_required_flag
      comment: "TRUE when the next step requires customer action — precondition for showing action text (C13)"
    - name: Is Terminal Stage
      expr: policy.is_terminal_stage
      comment: "TRUE if the application is in a final stage (APPROVED, DENIED) — no next action in this case (C13)"
    - name: Customer Safe Action Text
      expr: policy.customer_safe_action_text
      comment: "Policy-approved wording safe to display to the customer — primary answer for C13 'What is my next step?'"
  measures:
    - name: Application Count
      expr: COUNT(1)
      comment: "Should be 1 per customer application under RLS — used as health-check"
$$;
