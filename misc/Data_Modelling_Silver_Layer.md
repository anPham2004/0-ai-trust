# Data Modelling – Silver Layer


## Subject Area Involved Party


### Table 1: `ip_individual`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | customers.global_id | True | Internal | None | PK |
| customer_id | STRING | customers.customerId | True | Internal | None |  |
| customer_type | STRING | customers.customerType | True | Internal | None | INDIVIDUAL/BUSINESS/SOLE_TRADER |
| prefix | STRING | customers.prefix | False | Highly Confidential | Redact | PII |
| full_name | STRING | customers.firstName + customers.lastName | True | Highly Confidential | Hash → name_token | Direct PII |
| name_token | STRING | derived | True | Internal | SHA-256 of full_name | Used in AI output |
| middle_names | STRING | customers.middleNames | False | Highly Confidential | Redact | PII |
| gender | STRING | customers.gender | False | Confidential | None |  |
| age | INT | customers.age | False | Confidential | None |  |
| state | STRING | customers.state | False | Internal | None |  |
| occupation_code | STRING | customers.occupationCode | False | Confidential | None | ANZSCO |
| email_masked | STRING | customers.email | True | Highly Confidential | a***@domain.com | Never expose raw |
| phone_masked | STRING | customers.phoneNumber | True | Highly Confidential | 04XX XXX XXX | Never expose raw |
| preferred_contact_channel | STRING | customer_preferences.preferredContactChannel | False | Internal | None | B5, C1 |
| preferred_language | STRING | customer_preferences.preferredLanguage | False | Internal | None | C2 |
| marketing_opt_in | BOOLEAN | customer_preferences.marketingOptIn | False | Internal | None | C3 |
| satisfaction_score | STRING | customer_preferences.satisfactionScore | False | Confidential | None | B4 |
| address_suburb | STRING | physical_addresses.suburb | False | Confidential | None |  |
| address_state | STRING | physical_addresses.state | False | Internal | None |  |
| address_postcode | STRING | physical_addresses.postcode | False | Confidential | None |  |
| customer_since | TIMESTAMP | customers.createdAt | False | Internal | None |  |
| last_updated_at | TIMESTAMP | customers.lastUpdateTime | False | Internal | None |  |
| pipeline_run_id | STRING | pipeline | True | Public | None | Lineage |
| source_table | STRING | pipeline | True | Public | None | customers |
| processes_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None | PASSED/FAILED/WARNING |
| masking_status | STRING | pipeline | True | Public | None | MASKED/CLEAN |

### Table 2: `ip_organisation`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | organisations.global_id | True | Internal | None | FK → ip_individual |
| organisation_id | STRING | organisations.organisationId | True | Internal | None | PK |
| business_name | STRING | organisations.businessName | True | Confidential | None |  |
| legal_name | STRING | organisations.legalName | False | Confidential | None |  |
| short_name | STRING | organisations.shortName | False | Internal | None |  |
| organisation_type | STRING | organisations.organisationType | True | Internal | None |  |
| industry_code | STRING | organisations.industryCode | False | Internal | None |  |
| industry_code_version | STRING | organisations.industryCodeVersion | False | Internal | None |  |
| registered_country | STRING | organisations.registeredCountry | False | Internal | None | always AUS |
| is_acnc_registered | BOOLEAN | organisations.isACNCRegistered | False | Internal | None | charity flag |
| agent_role | STRING | organisations.agentRole | False | Internal | None | PRINCIPLE/DIRECTOR/OWNER |
| abn_masked | STRING | organisations.abn | False | Highly Confidential | Last 3 digits visible |  |
| acn_masked | STRING | organisations.acn | False | Highly Confidential | Last 3 digits visible |  |
| establishment_date | DATE | organisations.establishmentDate | False | Internal | None | BB4 |
| last_updated_at | TIMESTAMP | organisations.lastUpdateTime | False | Internal | None | BB5 |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 3: source table `ip_organisation_party_relationship`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| relationship_id | STRING | organisation_party_relationships.relationshipId | True | Internal | None | PK |
| global_id | STRING | organisation_party_relationships.global_id | True | Internal | None | FK → ip_individual (the person) |
| organisation_id | STRING | organisation_party_relationships.organisationId | True | Internal | None | FK → ip_organisation |
| party_role | STRING | organisation_party_relationships.partyRole | True | Internal | None | BB6, BB8 - DIRECTOR/AUTHORISED_REPRESENTATIVE/BENEFICIAL_OWNER/SIGNATORY/GUARANTOR/SECRETARY |
| authority_level | STRING | organisation_party_relationships.authorityLevel | True | Internal | None | BB7 - FULL/LIMITED/VIEW_ONLY |
| is_active | BOOLEAN | organisation_party_relationships.isActive | True | Internal | None | BB9 |
| start_date | DATE | organisation_party_relationships.startDate | False | Internal | None |  |
| end_date | DATE | organisation_party_relationships.endDate | False | Internal | None | BB9 - null nếu còn active |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 4: source table `ip_organisation_relationship`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| relationship_id | STRING | organisation_relationships.relationshipId | True | Internal | None | PK |
| source_org_id | STRING | organisation_relationships.sourceOrgId | True | Internal | None | FK → ip_organisation |
| target_org_id | STRING | organisation_relationships.targetOrgId | True | Internal | None | FK → ip_organisation |
| relationship_type | STRING | organisation_relationships.relationshipType | True | Internal | None | PARENT_SUBSIDIARY/TRUST_TRUSTEE/PARTNERSHIP/FRANCHISE/JOINT_VENTURE |
| is_active | BOOLEAN | organisation_relationships.isActive | True | Internal | None |  |
| start_date | DATE | organisation_relationships.startDate | False | Internal | None |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 5: source table `ip_kyc_verification`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | kyc_records.global_id | True | Internal | None | FK → ip_individual |
| kyc_id | STRING | kyc_records.kycId | True | Internal | None | PK |
| organisation_id | STRING | kyc_records.organisationId | False | Internal | None | NULL = KYC, populated = KYB |
| record_type | STRING | derived | True | Internal | None | KYC /KYB (derived từ organisationId IS NULL) |
| verification_status | STRING | kyc_records.verificationStatus | True | Internal | None | B11 - VERIFIED/PENDING/FAILED |
| verification_date | DATE | kyc_records.verificationDate | False | Internal | None |  |
| verification_method | STRING | kyc_records.verificationMethod | False | Internal | None |  |
| risk_rating | STRING | kyc_records.riskRating | True | Highly Confidential | Redact from AI output | Banker-only |
| pep_status | BOOLEAN | kyc_records.pepStatus | True | Highly Confidential | Redact from AI output | Banker-only |
| sanctions_check | STRING | kyc_records.sanctionsCheck | True | Highly Confidential | Redact from AI output | Banker-only |
| last_review_date | DATE | kyc_records.lastReviewDate | False | Internal | None |  |
| next_review_date | DATE | kyc_records.nextReviewDate | False | Internal | None |  |
| document_types | STRING | kyc_records.documentTypes | False | Internal | None | BB12 |
| abn_verified | BOOLEAN | kyc_records.abnVerified | False | Internal | None | KYB only |
| asic_check_status | STRING | kyc_records.asicCheckStatus | False | Internal | None | KYB only |
| beneficial_ownership_verified | BOOLEAN | kyc_records.beneficialOwnershipVerified | False | Internal | None | KYB only |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

## Subject Area Arrangement


### Table 1: `arr_banking_arrangement`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | banking_accounts.global_id | True | Internal | None | FK → ip_individual |
| account_id | STRING | banking_accounts.accountId | True | Internal | None | PK |
| organisation_id | STRING | banking_accounts.organisationId | False | Internal | None | NULL = personal, populated = business |
| is_business_arrangement | BOOLEAN | derived: organisationId IS NOT NULL | True | Internal | None | BB13, BB14, BB15 |
| account_type | STRING | banking_accounts.accountType | True | Internal | None |  |
| is_active | BOOLEAN | banking_accounts.isActive | True | Internal | None |  |
| is_owner | BOOLEAN | banking_accounts.isOwner | False | Internal | None |  |
| open_date | DATE | banking_accounts.openDate | False | Internal | None |  |
| current_balance_masked | STRING | banking_balances.currentBalance | False | Highly Confidential | $X,XXX range bucket | Never exact |
| available_balance_masked | STRING | banking_balances.availableBalance | False | Highly Confidential | $X,XXX range bucket |  |
| currency | STRING | banking_balances.currency | False | Internal | None |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 2: source table `arr_loan_arrangement` 

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | loan_accounts.global_id | True | Internal | None | FK |
| loan_id | STRING | loan_accounts.loanId | True | Internal | None | PK |
| account_id | STRING | loan_accounts.accountId | False | Internal | None | FK → arr_banking_arrangement |
| organisation_id | STRING | derived: banking_accounts.organisationId via loan_accounts.accountId → banking_accounts.accountId | False | Internal | None | NULL = personal, populated = business |
| is_business_arrangement | BOOLEAN | derived : organisationId IS NOT NULL | True | Internal | None | BB13, BB14, BB15 |
| loan_type | STRING | loan_accounts.loanType | True | Internal | None |  |
| status | STRING | loan_accounts.status | True | Internal | None |  |
| interest_type | STRING | derived from interestRate | False | Internal | None | FIXED/VARIABLE |
| repayment_frequency | STRING | loan_accounts.repaymentFrequency | False | Internal | None |  |
| term_months | INT | loan_accounts.termMonths | False | Internal | None |  |
| start_date | DATE | loan_accounts.startDate | False | Internal | None |  |
| maturity_date | DATE | loan_accounts.maturityDate | False | Internal | None |  |
| original_amount_masked | STRING | loan_accounts.originalAmount | False | Highly Confidential | Range bucket |  |
| current_balance_masked | STRING | loan_accounts.currentBalance | False | Highly Confidential | Range bucket |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 3: source table `arr_mortgage_arrangement`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | mortgage_accounts.global_id | True | Internal | None | FK |
| mortgage_id | STRING | mortgage_accounts.mortgageId | True | Internal | None | PK |
| account_id | STRING | mortgage_accounts.accountId | False | Internal | None | FK |
| organisation_id | STRING | derived: banking_accounts.organisationId via mortgage_accounts.accountId → banking_accounts.accountId | False | Internal | None | NULL = personal, populated = business |
| is_business_arrangement | BOOLEAN | derived : organisationId IS NOT NULL | True | Internal | None | BB13, BB14, BB15 |
| interest_type | STRING | mortgage_accounts.interestType | False | Internal | None |  |
| repayment_frequency | STRING | mortgage_accounts.repaymentFrequency | False | Internal | None |  |
| loan_term | INT | mortgage_accounts.loanTerm | False | Internal | None |  |
| start_date | DATE | mortgage_accounts.startDate | False | Internal | None |  |
| lvr_percent | STRING | mortgage_accounts.lvrPercent | False | Confidential | None | Banker-only |
| loan_amount_masked | STRING | mortgage_accounts.loanAmount | False | Highly Confidential | Range bucket |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |


### Table 4: source table `arr_credit_card_arrangement`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | credit_cards.global_id | True | Internal | None | FK |
| credit_card_id | STRING | credit_cards.creditCardId | True | Internal | None | PK |
| organisation_id | STRING | credit_cards.organisationId | False | Internal | None | NULL = personal, populated = business |
| is_business_arrangement | BOOLEAN | derived : organisationId IS NOT NULL | True | Internal | None | BB13, BB14, BB15 |
| card_type | STRING | credit_cards.cardType | True | Internal | None |  |
| status | STRING | credit_cards.status | True | Internal | None |  |
| reward_program | STRING | credit_cards.rewardProgram | False | Internal | None |  |
| issued_date | DATE | credit_cards.issuedDate | False | Internal | None |  |
| card_number_masked | STRING | credit_cards.cardNumber | True | Highly Confidential | **** **** **** XXXX |  |
| credit_limit_masked | STRING | credit_cards.creditLimit | False | Highly Confidential | Range bucket |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

## Subject Area Application


### Table 1: `app_application`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | loan_applications.global_id | True | Internal | None | FK |
| application_id | STRING | loan_applications.applicationId | True | Internal | None | PK |
| organisation_id | STRING | loan_applications.organisationId | False | Internal | None | BB18, BB19 |
| is_business_application | BOOLEAN | derived | True | Internal | None |  |
| num_offers | STRING | loan_applications.numOffers | False | Internal | None |  |
| loan_goal | STRING | loan_applications.loanGoal | True | Internal | None |  |
| application_type | STRING | loan_applications.applicationType | True | Internal | None |  |
| final_outcome | STRING | loan_applications.finalOutcome | False | Internal | None | APPROVED/REJECTED/PENDING |
| submitted_at | TIMESTAMP | loan_applications.submittedAt | True | Internal | None |  |
| last_updated_at | TIMESTAMP | loan_applications.lastUpdatedAt | True | Internal | None | B15 |
| requested_amount_masked | STRING | loan_applications.requestedAmount | False | Highly Confidential | Range bucket |  |
| credit_score | INT | loan_applications.creditScore | False | Highly Confidential | Redact from AI output | Banker-only, BR8 |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 2: source table `app_application_stage`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| history_id | STRING | application_stage_history.historyId | True | Internal | None | PK |
| application_id | STRING | application_stage_history.applicationId | True | Internal | None | FK → app_application |
| global_id | STRING | application_stage_history.global_id | True | Internal | None | FK |
| stage | STRING | application_stage_history.stage | True | Internal | None | B16 |
| entered_at | TIMESTAMP | application_stage_history.enteredAt | True | Internal | None | B19 |
| exited_at | TIMESTAMP | application_stage_history.exitedAt | False | Internal | None |  |
| duration_days | INT | derived: exited_at - entered_at | False | Internal | None | B17 - computed |
| is_current_stage | BOOLEAN | derived: exitedAt ISNULL | False | Internal | None | B16 |
| assigned_team | STRING | application_stage_history.assignedTeam | False | Internal | None | B23 |
| sla_deadline | TIMESTAMP | application_stage_history.slaDeadline | False | Internal | None | B9 |
| is_sla_breached | BOOLEAN | derived: now() > slaDeadline | False | Internal | None | B9 |
| pending_action_party | STRING | application_stage_history.pendingActionParty | False | Internal | None | B22: CUSTOMER/ORGANISATION/AUTHORISED_REPRESENTATIVE/DIRECTOR/GUARANTOR/THIRD_PARTY/INTERNAL_TEAM |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 3: source table `app_status_change`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| change_id | STRING | status_change_history.changeId | True | Internal | None | PK |
| application_id | STRING | status_change_history.applicationId | True | Internal | None | FK |
| global_id | STRING | status_change_history.global_id | True | Internal | None | FK |
| old_status | STRING | status_change_history.oldStatus | True | Internal | None | B18 |
| new_status | STRING | status_change_history.newStatus | True | Internal | None |  |
| changed_at | TIMESTAMP | status_change_history.changedAt | True | Internal | None |  |
| reason | STRING | status_change_history.reason | False | Internal | None | B10, C12 |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 4: source table `app_missing_document`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| document_id | STRING | missing_documents.documentId | True | Internal | None | PK |
| application_id | STRING | missing_documents.applicationId | True | Internal | None | FK |
| global_id | STRING | missing_documents.global_id | True | Internal | None | FK |
| document_type | STRING | missing_documents.documentType | True | Internal | None | B20 |
| status | STRING | missing_documents.status | True | Internal | None | PENDING/RECEIVED/REJECTED |
| is_invalid_or_expired | BOOLEAN | derived: status = REJECTED OR expiryDate < now() | True | Internal | None | B21 |
| requested_at | TIMESTAMP | missing_documents.requestedAt | False | Internal | None |  |
| received_at | TIMESTAMP | missing_documents.receivedAt | False | Internal | None |  |
| expiry_date | DATE | missing_documents.expiryDate | False | Internal | None |  |
| reminders_sent | INT | missing_documents.remindersSent | False | Internal | None | C11 |
| last_reminder_at | TIMESTAMP | missing_documents.lastReminderAt | False | Internal | None |  |
| rejection_reason | STRING | missing_documents.rejectionReason | False | Internal | None |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 5: source table `app_application_event` 

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| event_id | STRING | loan_application_events.eventId | True | Internal | None | PK |
| application_id | STRING | loan_application_events.applicationId | True | Internal | None | FK |
| global_id | STRING | loan_application_events.global_id | True | Internal | None | FK |
| concept_name | STRING | loan_application_events.conceptName | False | Internal | None |  |
| lifecycle_transition | STRING | loan_application_events.lifecycleTransition | True | Internal | None |  |
| event_timestamp | TIMESTAMP | loan_application_events.timestamp | True | Internal | None |  |
| event_origin | STRING | loan_application_events.eventOrigin | False | Internal | None | SYSTEM/CUSTOMER/BANKER |
| action | STRING | loan_application_events.action | False | Internal | None |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 6: source table `app_accepted_loan`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | accepted_loans.global_id | True | Internal | None | FK |
| loan_id | STRING | accepted_loans.loanId | True | Internal | None | PK |
| loan_status | STRING | accepted_loans.loanStatus | True | Internal | None |  |
| purpose | STRING | accepted_loans.purpose | False | Internal | None |  |
| term | STRING | accepted_loans.term | False | Internal | None |  |
| grade | STRING | accepted_loans.grade | False | Confidential | None | Banker-only |
| issued_at | DATE | accepted_loans.issuedAt | False | Internal | None |  |
| loan_amount_masked | STRING | accepted_loans.loanAmount | False | Highly Confidential | Range bucket |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 7: source table `app_rejected_application`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| global_id | STRING | rejected_applications.global_id | True | Internal | None | FK |
| loan_id | STRING | rejected_applications.loanId | True | Internal | None | PK |
| application_title | STRING | rejected_applications.applicationTitle | False | Internal | None |  |
| application_date | DATE | rejected_applications.applicationDate | False | Internal | None |  |
| rejection_reason | STRING | rejected_applications.rejectionReason | False | Confidential | None | Banker-only |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

## Subject Area Event


### Table 1: `evt_service_case`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| case_id | STRING | service_cases.caseId | True | Internal | None | PK |
| global_id | STRING | service_cases.global_id | True | Internal | None | FK |
| organisation_id | STRING | service_cases.organisationId | False | Internal | None | BB25, BB26 |
| application_id | STRING | service_cases.applicationId | False | Internal | None | BB26 - link case vowis application |
| sla_deadline | TIMESTAMP | service_cases.slaDeadline | False | Internal | None | BB27 |
| is_sla_breached | BOOLEAN | derived: now() > slaDeadline AND status IN ('open', 'in_progress') | False | Internal | None | BB27 |
| is_business_case | BOOLEAN | derived: organisationId IS NOT NULL | True | Internal | None |  |
| case_type | STRING | service_cases.caseType | True | Internal | None | B3 |
| subject | STRING | service_cases.subject | True | Internal | None | OPEN/CLOSED/PENDING |
| status | STRING | service_cases.status | True | Internal | None |  |
| priority | STRING | service_cases.priority | False | Internal | None |  |
| assigned_team | STRING | service_cases.assignedTeam | False | Internal | None |  |
| created_at | TIMESTAMP | service_cases.createdAt | True | Internal | None |  |
| resolved_at | TIMESTAMP | service_cases.resolvedAt | False | Internal | None | C5 |
| resolution_summary | STRING | service_cases.resolutionSummary | False | Internal | None | C5 |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 2: source table `evt_support_interaction`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| interaction_id | STRING | support_interactions.interactionId | True | Internal | None | PK |
| global_id | STRING | support_interactions.global_id | True | Internal | None | FK |
| channel | STRING | support_interactions.channel | True | Internal | None | C6 |
| interaction_timestamp | TIMESTAMP | support_interactions.timestamp | True | Internal | None | C6 |
| topic | STRING | support_interactions.topic | False | Internal | None | B2, C7 |
| resolution | STRING | support_interactions.resolution | False | Internal | None |  |
| duration_minutes | INT | support_interactions.durationMinutes | False | Internal | None |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |

### Table 3: source table `evt_service_case_event`

| Field | Type | Source Field | Required | PII | Masking | Notes |
|---|---|---|---|---|---|---|
| event_id | STRING | service_case_events.eventId | True | Internal | None | PK |
| case_id | STRING | service_case_events.caseId | True | Internal | None | FK → evt_service_case |
| global_id | STRING | service_case_events.global_id | True | Internal | None | FK |
| event_type | STRING | service_case_events.eventType | True | Internal | None | NOTE_ADDED/STATUS_CHANGED/ASSIGNED/ESCALATED/DOCUMENT_ATTACHED/CUSTOMER_CONTACTED |
| event_timestamp | TIMESTAMP | service_case_events.eventTimestamp | True | Internal | None |  |
| description | STRING | service_case_events.description | False | Internal | None |  |
| pipeline_run_id | STRING | pipeline | True | Public | None |  |
| source_table | STRING | pipeline | True | Public | None |  |
| processed_at | TIMESTAMP | pipeline | True | Public | None |  |
| dq_status | STRING | pipeline | True | Public | None |  |
| masking_status | STRING | pipeline | True | Public | None |  |
