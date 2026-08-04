-- Banker.AI — Individual Customer Semantic Layer
-- Audience: Banker.AI agents (B1-B24)
-- 7 metric views deployed to `0-ai-trust`.semantic
-- Submit via: databricks experimental aitools tools statement submit --file <file> --warehouse <ID>
--
-- Enrichment history:
--   [1] Added field-level comments: annotations tracing each field to a business question ID (B1-B24)
--   [2] entity_type filter already present in joins — verified correct
--   [3] recorded_reason retained here (banker-facing view; appropriate for B10)
--   [4] Added synonyms and sample_questions to all views for Genie Ontology snippet extraction

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_customer_overview
WITH METRICS LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.dim_customer"
  comment: "Customer profile, preferences, and cross-domain activity snapshot for Banker.AI (B1, B4, B5, B12, B24)"
  joins:
    - name: ctx
      source: "`0-ai-trust`.gold.fact_subject_context_snapshot"
      on: "ctx.entity_id = source.customer_id AND ctx.entity_type = 'CUSTOMER'"
  dimensions:
    - name: Customer ID
      expr: source.customer_id
      comment: "Primary lookup key — filter here to scope all Banker.AI questions to a specific customer (B1, B12, B24)"
      synonyms: [customer identifier, customer number, customer code, cust id]
    - name: Age Band
      expr: source.age_band
      comment: "Demographic age group (UNDER_18, 18_24, 25_34, 35_44, 45_54, 55_64, 65_PLUS) — context for B1, B12"
      synonyms: [age group, age range, age bracket, demographic]
    - name: State
      expr: source.state
      comment: "Customer's state of residence — geographic context for pre-meeting summary (B1)"
      synonyms: [location, state of residence, region]
    - name: Preferred Contact Channel
      expr: source.preferred_contact_channel
      comment: "Channel the banker should use when contacting the customer — phone, email, branch (B5)"
      synonyms: [contact preference, preferred channel, how to contact, contact method]
    - name: Preferred Language
      expr: source.preferred_language
      comment: "Customer's preferred communication language — relevant for handover summary (B12, B24)"
      synonyms: [language preference, spoken language]
    - name: Marketing Opt In
      expr: source.marketing_opt_in
      comment: "Whether customer has opted into marketing communications — context for B12"
      synonyms: [marketing consent, opt in status, marketing flag]
    - name: Satisfaction Score
      expr: source.satisfaction_score
      comment: "Customer satisfaction rating (1-5 scale) — use to assess dissatisfaction trend (B4)"
      synonyms: [CSAT, customer satisfaction, satisfaction rating, NPS proxy]
    - name: Latest Application Status
      expr: ctx.latest_application_status
      comment: "Current status of the most recent application, e.g. PENDING, APPROVED (B14)"
      synonyms: [application status, latest app status, current application state]
    - name: Latest Application Stage
      expr: ctx.latest_application_stage
      comment: "Current pipeline stage, e.g. DOCUMENT_VERIFICATION, ASSESSMENT (B16)"
      synonyms: [pipeline stage, current stage, application stage, workflow stage]
    - name: Latest Pending Action Party
      expr: ctx.latest_pending_action_party
      comment: "Indicates whether next action awaits CUSTOMER or INTERNAL_TEAM (B22)"
      synonyms: [action owner, who needs to act, next action party, pending party]
    - name: Latest Assigned Team
      expr: ctx.latest_assigned_team
      comment: "Team currently owning the latest application (B23)"
      synonyms: [responsible team, handling team, assigned team]
    - name: Top Case Type 90 Days
      expr: ctx.top_case_type_90d
      comment: "Most frequently raised case category in the last 90 days — e.g. CARD_DISPUTE, ADDRESS_CHANGE (B3)"
      synonyms: [most common issue, frequent issue, top complaint, common case type]
    - name: Verification Status
      expr: ctx.verification_status
      comment: "Individual KYC verification status: VERIFIED, PENDING, FAILED — compliance context for B1, B12"
      synonyms: [KYC status, identity verification, verification outcome, ID check status]
  measures:
    - name: Open Case Count
      expr: SUM(ctx.open_case_count)
      comment: "Number of currently open service cases — used to assess customer dissatisfaction (B4, B6)"
      synonyms: [active cases, open tickets, unresolved cases]
    - name: SLA Breached Case Count
      expr: SUM(ctx.sla_breached_case_count)
      comment: "Service cases that have exceeded their SLA deadline — B4"
      synonyms: [SLA breach count, overdue cases, breached cases]
    - name: Open Application Count
      expr: SUM(ctx.open_application_count)
      comment: "Applications not yet in a terminal state (APPROVED/DENIED/CANCELLED/WITHDRAWN) — B6"
      synonyms: [active applications, pending applications, in-progress applications]
    - name: Missing Document Count
      expr: SUM(ctx.missing_document_count)
      comment: "Outstanding missing documents across open applications — B7, B11"
      synonyms: [outstanding documents, missing docs, documents outstanding, docs needed]
    - name: Active Arrangement Count
      expr: SUM(ctx.active_arrangement_count)
      comment: "Total active banking products held by the customer — context for pre-call summary (B12, B24)"
      synonyms: [product count, active products, number of products, banking products held]
    - name: Recent Support Interactions 30d
      expr: SUM(ctx.recent_support_interaction_count_30d)
      comment: "Support interactions in the last 30 days — B2"
      synonyms: [recent contacts, recent interactions, contacts in last 30 days]
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_application_status
WITH METRICS LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_application_current"
  comment: "Current application status, stage, and action flags for Banker.AI (B6, B9, B14-B17, B22, B23)"
  dimensions:
    - name: Application ID
      expr: application_id
      comment: "Unique application identifier — primary filter for single-application questions (B14, B16, B17, B22, B23)"
      synonyms: [application number, app ID, loan application ID, application reference]
    - name: Customer ID
      expr: customer_id
      comment: "Customer who submitted the application — join key back to mv_customer_overview"
      synonyms: [customer number, customer identifier]
    - name: Is Business Application
      expr: is_business_application
      comment: "TRUE if this is a commercial/business application — used to route to mv_banker_business"
      synonyms: [business app flag, commercial application, is commercial]
    - name: Loan Goal
      expr: loan_goal
      comment: "Purpose of the loan, e.g. HOME_PURCHASE, INVESTMENT — context for B1, B12"
      synonyms: [loan purpose, application purpose, reason for loan, loan reason]
    - name: Application Type
      expr: application_type
      comment: "Product category, e.g. HOME_LOAN, PERSONAL_LOAN, CREDIT_CARD — B1 context"
      synonyms: [product type, loan type, application product, type of application]
    - name: Application Month
      expr: "DATE_TRUNC('MONTH', submitted_at)"
      comment: "Month the application was submitted — portfolio trend dimension"
    - name: Current Stage
      expr: current_stage
      comment: "Stage the application is currently in, e.g. DOCUMENT_VERIFICATION, ASSESSMENT (B16)"
      synonyms: [pipeline stage, current workflow stage, stage, where is the application]
    - name: Previous Stage
      expr: previous_stage
      comment: "Stage the application was in immediately before the current one (B18)"
    - name: Latest Status
      expr: latest_status
      comment: "Current status label — e.g. PENDING, APPROVED, RETURNED (B14)"
      synonyms: [current status, application state, status update]
    - name: Final Outcome
      expr: final_outcome
      comment: "Terminal result once application is closed — APPROVED, DENIED, WITHDRAWN, CANCELLED"
      synonyms: [application outcome, final decision, end result]
    - name: Pending Action Party
      expr: pending_action_party
      comment: "Who must act next: CUSTOMER or INTERNAL_TEAM (B22)"
      synonyms: [action owner, who needs to act next, ball in whose court, awaiting]
    - name: Assigned Team
      expr: assigned_team
      comment: "Operational team currently responsible for the application (B23)"
      synonyms: [responsible team, handling team, owner team, processing team]
    - name: Status Last Changed At
      expr: status_last_changed_at
      comment: "Timestamp of the most recent status change — B15"
    - name: Stage Entered At
      expr: current_stage_entered_at
      comment: "When the application entered its current stage — use to compute time-in-stage (B17, B19)"
    - name: Is Stage SLA Breached
      expr: is_stage_sla_breached
      comment: "TRUE if the application has exceeded its current-stage SLA deadline (B9)"
      synonyms: [SLA breached, overdue, SLA exceeded, past SLA, late application]
    - name: Is Inactive Over 14 Days
      expr: is_inactive_over_14_days
      comment: "TRUE if the application has had no activity for more than 14 days (B9)"
      synonyms: [stalled, inactive, dormant, no activity, stuck application]
    - name: Customer Action Required
      expr: customer_action_required_flag
      comment: "TRUE when the next action requires the customer, e.g. upload documents (B22)"
    - name: Internal Action Required
      expr: internal_action_required_flag
      comment: "TRUE when the next action is internal — e.g. assessor review (B22)"
    - name: Recorded Reason
      expr: recorded_reason
      comment: "Reason recorded when an application was returned or status changed — B10 (banker-only field)"
      synonyms: [return reason, decline reason, reason for return, why was it returned]
  measures:
    - name: Application Count
      expr: COUNT(1)
      comment: "Total number of applications matching the filter — portfolio count"
      synonyms: [total applications, number of applications, application volume]
    - name: Avg Days in Stage
      expr: AVG(days_in_current_stage)
      comment: "Average number of days applications have spent in the current stage (B17)"
      synonyms: [average stage duration, days in current stage, time in stage, how long in stage]
    - name: Missing Document Count
      expr: SUM(missing_document_count)
      comment: "Total missing documents across applications in scope — B7, B11"
      synonyms: [outstanding documents, missing docs]
    - name: Expired Document Count
      expr: SUM(expired_document_count)
      comment: "Total expired documents requiring re-upload — B21"
    - name: Total Reminders Sent
      expr: SUM(total_reminders_sent)
      comment: "Cumulative document reminders sent to customers — B11"
    - name: SLA Breached Count
      expr: COUNT_IF(is_stage_sla_breached)
      comment: "Number of applications that have breached stage SLA — B9"
      synonyms: [overdue applications, SLA breach count, late applications]
    - name: Inactive Application Count
      expr: COUNT_IF(is_inactive_over_14_days)
      comment: "Applications inactive for more than 14 days — B9"
      synonyms: [stalled applications, dormant applications, stuck applications]
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_application_documents
WITH METRICS LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_application_document"
  comment: "Document checklist and status per application for Banker.AI (B7, B11, B13, B20, B21)"
  dimensions:
    - name: Application ID
      expr: application_id
      comment: "Application this document belongs to — filter for single-application document queries (B20, B21)"
      synonyms: [application number, application reference]
    - name: Customer ID
      expr: customer_id
      comment: "Customer who submitted/is required to provide this document — filter for B7, B13"
    - name: Document Type
      expr: document_type
      comment: "Type of document required, e.g. PAYSLIP, PROOF_OF_ADDRESS, BANK_STATEMENT (B20)"
      synonyms: [document name, doc type, required document type, what document, type of document needed]
    - name: Document Status
      expr: document_status
      comment: "Current status: REQUESTED, RECEIVED, REJECTED, EXPIRED, VALIDATED (B20, B21)"
      synonyms: [doc status, document state, document condition, is the document received]
    - name: Is Missing
      expr: is_missing
      comment: "TRUE if the document has been requested but not yet received (B7, B11, B13)"
      synonyms: [outstanding, not submitted, missing document, document not received, not yet uploaded]
    - name: Is Received
      expr: is_received
      comment: "TRUE if the document has been successfully uploaded by the customer (B20)"
    - name: Is Rejected
      expr: is_rejected
      comment: "TRUE if the document was submitted but marked as invalid/unacceptable (B21)"
      synonyms: [rejected document, invalid document, document failed, doc rejected, not accepted]
    - name: Is Expired
      expr: is_expired
      comment: "TRUE if the document was previously valid but has since expired (B21)"
      synonyms: [expired document, document expired, no longer valid, out of date document]
    - name: Is Invalid or Expired
      expr: is_invalid_or_expired
      comment: "Combined flag — TRUE for rejected OR expired documents (B21)"
      synonyms: [needs attention, invalid or expired, document problem]
    - name: Rejection Reason
      expr: rejection_reason
      comment: "Reason the document was rejected, e.g. ILLEGIBLE, INCORRECT_TYPE, EXPIRED_ON_RECEIPT (B21). Banker-only field — excluded from Customer.AI (CR2)."
      synonyms: [why was document rejected, rejection note, doc rejection reason, assessor note]
    - name: Requested At
      expr: requested_at
      comment: "When the document was first requested from the customer"
    - name: Received At
      expr: received_at
      comment: "When the document was received — use to confirm receipt (B20)"
    - name: Expiry Date
      expr: expiry_date
      comment: "Document expiry date — use to identify documents expiring soon (B21)"
  measures:
    - name: Total Documents
      expr: COUNT(1)
      comment: "Total document items in scope for the application"
    - name: Missing Count
      expr: COUNT_IF(is_missing)
      comment: "Documents still outstanding — B7, B11, B13"
      synonyms: [number of missing docs, outstanding document count, how many documents missing]
    - name: Received Count
      expr: COUNT_IF(is_received)
      comment: "Documents successfully received — B20"
    - name: Rejected Count
      expr: COUNT_IF(is_rejected)
      comment: "Documents rejected or marked invalid — B21"
      synonyms: [rejected docs, invalid documents, how many rejected]
    - name: Expired Count
      expr: COUNT_IF(is_expired)
      comment: "Documents that have expired — B21"
      synonyms: [expired documents, how many expired docs]
    - name: Total Reminders Sent
      expr: SUM(reminders_sent)
      comment: "Total document reminder notifications sent to customer — B11"
      synonyms: [reminders sent, notifications sent, how many reminders, document reminders]
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_application_timeline
WITH METRICS LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_application_timeline_event"
  comment: "Full audit trail of application state changes for Banker.AI (B8, B10, B18, B19)"
  dimensions:
    - name: Application ID
      expr: application_id
      comment: "Filter to a single application ID to retrieve its complete stage history (B8)"
      synonyms: [application number, application reference]
    - name: Customer ID
      expr: customer_id
      comment: "Customer associated with the application"
    - name: Event Day
      expr: "DATE_TRUNC('DAY', event_timestamp)"
      comment: "Calendar day of the event — use to answer 'when did X happen?' (B19)"
    - name: Event Sequence
      expr: event_sequence
      comment: "Sequential order of events within an application — ORDER BY this to reconstruct timeline (B8)"
    - name: Event Family
      expr: event_family
      comment: "High-level category: STAGE_CHANGE, STATUS_CHANGE, DOCUMENT_EVENT, ASSIGNMENT_CHANGE (B8)"
      synonyms: [event category, event type group, type of event]
    - name: Event Type
      expr: event_type
      comment: "Specific event, e.g. STAGE_ENTERED, STATUS_RETURNED, DOCUMENT_REQUESTED (B8, B10)"
      synonyms: [what happened, event detail, specific event]
    - name: From Value
      expr: from_value
      comment: "Previous state before this event — e.g. the stage the application moved FROM (B18)"
      synonyms: [previous stage, previous status, from stage, before]
    - name: To Value
      expr: to_value
      comment: "New state after this event — e.g. the stage the application moved TO (B8)"
      synonyms: [new stage, moved to, current stage, after]
    - name: Stage
      expr: stage
      comment: "Stage at the time of this event — filter on STAGE_CHANGE events to find when stage was entered (B16, B19)"
    - name: Status
      expr: status
      comment: "Status at the time of this event (B8)"
    - name: Event Origin
      expr: event_origin
      comment: "Whether the event was triggered by SYSTEM, BANKER, or CUSTOMER action"
      synonyms: [who triggered this, event source, triggered by]
    - name: Assigned Team
      expr: assigned_team
      comment: "Team responsible at the time of this event (B23)"
    - name: Pending Action Party
      expr: pending_action_party
      comment: "Who held responsibility at this event: CUSTOMER or INTERNAL_TEAM (B22)"
    - name: Recorded Reason
      expr: recorded_reason
      comment: "Reason recorded at this event — critical for B10: 'Why was this application returned?' Banker-only field (CR2)."
      synonyms: [reason for return, decline reason, why returned, return note, what reason was given]
  measures:
    - name: Event Count
      expr: COUNT(1)
      comment: "Total number of state-change events in the application history (B8)"
      synonyms: [number of events, total events, how many changes]
    - name: Avg Days Since Event
      expr: "AVG(DATEDIFF(current_date(), CAST(event_timestamp AS DATE)))"
      comment: "Average age of events in scope — useful for recency analysis (B17)"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_service_cases
WITH METRICS LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_service_case_current"
  comment: "Open and resolved service cases per customer for Banker.AI (B3, B4, B13)"
  dimensions:
    - name: Case ID
      expr: case_id
      comment: "Unique service case identifier"
    - name: Customer ID
      expr: customer_id
      comment: "Customer the case belongs to — filter for B4, B13"
    - name: Organisation ID
      expr: organisation_id
      comment: "Organisation for business cases — use in mv_organisation_overview context"
    - name: Application ID
      expr: application_id
      comment: "Application the case is related to, if any — links case to application context (B13)"
    - name: Case Type
      expr: case_type
      comment: "Category of the case, e.g. CARD_DISPUTE, ADDRESS_CHANGE, DOCUMENT_QUERY (B3)"
      synonyms: [issue type, case category, type of complaint, complaint type]
    - name: Case Status
      expr: case_status
      comment: "Current case state: OPEN, PENDING, RESOLVED, CLOSED (B4, B13)"
      synonyms: [case state, is case open, case open or closed]
    - name: Priority
      expr: priority
      comment: "Case priority: LOW, MEDIUM, HIGH, URGENT — indicator of urgency (B4)"
      synonyms: [urgency, case urgency, priority level, how urgent]
    - name: Assigned Team
      expr: assigned_team
      comment: "Team currently handling this case"
    - name: Is Open
      expr: is_open
      comment: "TRUE if the case is still active and unresolved — B4, B13"
      synonyms: [active case, unresolved, still open, not closed]
    - name: Is SLA Breached
      expr: is_sla_breached
      comment: "TRUE if the case has exceeded its service SLA deadline — B4"
      synonyms: [SLA breach, overdue case, past SLA, late case]
    - name: Is Business Case
      expr: is_business_case
      comment: "TRUE if this case was raised by or on behalf of a business customer (BB25, BB26)"
      synonyms: [business case flag, commercial case, is it a business case]
    - name: Application Related
      expr: application_related_flag
      comment: "TRUE if this case is linked to an open application — B13"
    - name: Created Month
      expr: "DATE_TRUNC('MONTH', created_at)"
      comment: "Month the case was created — trend dimension for B2"
    - name: Resolved At
      expr: resolved_at
      comment: "Timestamp when case was resolved — use to check outcome of previous cases (B5)"
  measures:
    - name: Total Cases
      expr: COUNT(1)
      comment: "Total number of cases in scope"
    - name: Open Cases
      expr: COUNT_IF(is_open)
      comment: "Currently open and unresolved cases — B4"
      synonyms: [active cases, open ticket count, unresolved cases, number of open cases]
    - name: Resolved Cases
      expr: "COUNT_IF(resolved_at IS NOT NULL)"
      comment: "Cases that have been resolved — B5"
    - name: SLA Breached Cases
      expr: COUNT_IF(is_sla_breached)
      comment: "Cases that have exceeded their service SLA — B4"
      synonyms: [overdue cases, SLA breach count, late cases, breached cases]
    - name: Avg Case Age Days
      expr: AVG(case_age_days)
      comment: "Average age of cases in scope — indicator of resolution speed (B4)"
      synonyms: [average case duration, how long cases have been open]
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_service_activity
WITH METRICS LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_service_activity"
  comment: "Customer interaction and support activity log for Banker.AI (B2)"
  dimensions:
    - name: Customer ID
      expr: customer_id
      comment: "Customer involved in this activity — filter to answer 'Has this customer contacted us recently?' (B2)"
    - name: Case ID
      expr: case_id
      comment: "Service case this activity belongs to — links to mv_service_cases (B13)"
    - name: Application ID
      expr: application_id
      comment: "Application this activity is related to, if any"
    - name: Activity Month
      expr: "DATE_TRUNC('MONTH', activity_timestamp)"
      comment: "Month of the activity — trend dimension (B2)"
    - name: Activity Type
      expr: activity_type
      comment: "Type of activity: CALL, CHAT, EMAIL, BRANCH_VISIT, NOTE — B2"
      synonyms: [interaction type, contact type, type of contact, how did they contact us]
    - name: Channel
      expr: channel
      comment: "Communication channel used — corroborates Preferred Contact Channel from customer profile (B2, B5)"
      synonyms: [contact channel, communication method, how contacted, channel used]
    - name: Topic
      expr: topic
      comment: "Topic of the interaction — corroborates Top Case Type from mv_customer_overview (B3)"
      synonyms: [reason for contact, what was discussed, contact topic, inquiry topic]
    - name: Case Event Type
      expr: case_event_type
      comment: "Internal case event classification: NOTE_ADDED, ESCALATED, ASSIGNED, RESOLVED — B2"
    - name: Is Customer Contact
      expr: is_customer_contact
      comment: "TRUE if this was a direct interaction with the customer (vs. internal activity) — B2"
    - name: Is Internal Activity
      expr: is_internal_activity
      comment: "TRUE for internal-only activities not visible to customer"
    - name: Follow Up Required
      expr: follow_up_required_flag
      comment: "TRUE if a follow-up action was flagged after this interaction — B11"
    - name: Escalated
      expr: escalated_flag
      comment: "TRUE if this interaction was escalated — indicator of dissatisfaction (B4)"
      synonyms: [was it escalated, escalation flag, raised to supervisor]
  measures:
    - name: Total Interactions
      expr: COUNT(1)
      comment: "Total activity records in scope — B2"
      synonyms: [total contacts, number of interactions, contact count, how many times contacted]
    - name: Customer Contact Count
      expr: COUNT_IF(is_customer_contact)
      comment: "Direct customer contacts — primary measure for 'Has this customer contacted us recently?' (B2)"
      synonyms: [direct contacts, customer-facing contacts, number of customer contacts]
    - name: Total Duration Minutes
      expr: SUM(duration_minutes)
      comment: "Total time spent on interactions — engagement depth indicator"
$$;

CREATE OR REPLACE VIEW `0-ai-trust`.semantic.mv_application_next_action
WITH METRICS LANGUAGE YAML
AS $$
  version: 1.1
  source: "`0-ai-trust`.gold.fact_application_current"
  comment: "Approved next-action wording from stage-action policy for Banker.AI (B22, B24). dim_stage_action_policy currently returns POLICY_NOT_CONFIGURED — pending business owner approval."
  joins:
    - name: policy
      source: "`0-ai-trust`.gold.dim_stage_action_policy"
      on: "policy.stage = source.current_stage AND policy.pending_action_party = source.pending_action_party AND current_date() >= policy.effective_from AND (policy.effective_to IS NULL OR current_date() <= policy.effective_to)"
  dimensions:
    - name: Application ID
      expr: source.application_id
      comment: "Filter to a specific application to retrieve its policy-governed next action (B22)"
      synonyms: [application number, application reference]
    - name: Customer ID
      expr: source.customer_id
      comment: "Customer associated with this application"
    - name: Current Stage
      expr: source.current_stage
      comment: "Stage used to look up the applicable action policy (B16)"
      synonyms: [pipeline stage, what stage is it in]
    - name: Pending Action Party
      expr: source.pending_action_party
      comment: "Who must act next — used as a policy lookup key alongside Current Stage (B22)"
      synonyms: [who needs to act, action owner, awaiting]
    - name: Assigned Team
      expr: source.assigned_team
      comment: "Team to contact for internal follow-up (B23)"
    - name: Is Terminal Stage
      expr: policy.is_terminal_stage
      comment: "TRUE if the application has reached a final stage (APPROVED, DENIED, etc.) — B22"
      synonyms: [is application finished, is it a final stage, terminal stage, end state]
    - name: Next Action Code
      expr: policy.next_action_code
      comment: "Machine-readable action code, e.g. AWAIT_CUSTOMER_DOCS, PROCEED_TO_ASSESSMENT (B22)"
    - name: Banker Action Text
      expr: policy.banker_action_text
      comment: "Policy-approved text for the banker to communicate to the customer — use directly in B24 handover notes"
      synonyms: [what to say to customer, banker guidance text, approved message, handover message, banker script, banker wording]
    - name: Customer Safe Action Text
      expr: policy.customer_safe_action_text
      comment: "Approved wording safe to share with the customer — also used in Customer.AI C13"
      synonyms: [customer message, what to tell customer, safe message for customer, customer-facing action text]
    - name: Default SLA Days
      expr: policy.default_sla_days
      comment: "Expected number of days for this stage to complete — used in SLA breach assessment (B9)"
      synonyms: [SLA target, expected days, stage SLA, target days, how many days should this take]
  measures:
    - name: Application Count
      expr: COUNT(1)
      comment: "Number of applications matching the current filter — portfolio count"
      synonyms: [total applications, application volume]
$$;
