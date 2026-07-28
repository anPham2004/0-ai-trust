
"""Source-native record schemas for the three Bronze ingestion streams.

    bronze.cdc_changes    23 DATABASE datasets, Debezium CDC envelopes (JSON)
    bronze.kafka_events    5 EVENT datasets, Kafka business-event envelopes (JSON)
    bronze.file_arrivals   4 FILE datasets, whole CSV files kept as raw bytes

Type mapping observed in the real payloads:
    SCHEMA.md string -> JSON string    ("requestedAmount":"29022.39")
    SCHEMA.md int32  -> JSON number    ("creditScore":887)
    SCHEMA.md bool   -> JSON boolean
"""

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# DATABASE source — 23 datasets, Debezium CDC -> bronze.cdc_changes
# ---------------------------------------------------------------------------

CUSTOMERS = StructType([
    StructField("global_id", StringType()),
    StructField("customerId", StringType()),
    StructField("firstName", StringType()),
    StructField("lastName", StringType()),
    StructField("email", StringType()),
    StructField("phoneNumber", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("state", StringType()),
    StructField("createdAt", StringType()),
    StructField("lastUpdateTime", StringType()),
    StructField("middleNames", StringType()),
    StructField("prefix", StringType()),
    StructField("suffix", StringType()),
    StructField("occupationCode", StringType()),
    StructField("occupationCodeVersion", StringType()),
    # JSON arrays stored as text at source — kept as text here.
    StructField("phoneNumbers", StringType()),
    StructField("emailAddresses", StringType()),
    StructField("customerType", StringType()),
])

CUSTOMER_PREFERENCES = StructType([
    StructField("global_id", StringType()),
    StructField("customerId", StringType()),
    StructField("preferredContactChannel", StringType()),
    StructField("preferredLanguage", StringType()),
    StructField("marketingOptIn", BooleanType()),
    StructField("satisfactionScore", StringType()),
    StructField("updatedAt", StringType()),
])

KYC_RECORDS = StructType([
    StructField("global_id", StringType()),
    StructField("customerId", StringType()),
    StructField("kycId", StringType()),
    StructField("verificationStatus", StringType()),
    StructField("verificationDate", StringType()),
    StructField("verificationMethod", StringType()),
    StructField("riskRating", StringType()),
    StructField("pepStatus", BooleanType()),
    StructField("sanctionsCheck", StringType()),
    StructField("lastReviewDate", StringType()),
    StructField("nextReviewDate", StringType()),
    StructField("documentTypes", StringType()),
    StructField("organisationId", StringType()),
    # KYB fields — only populated when organisationId is present.
    StructField("abnVerified", BooleanType()),
    StructField("asicCheckStatus", StringType()),
    StructField("beneficialOwnershipVerified", BooleanType()),
])

PHYSICAL_ADDRESSES = StructType([
    StructField("global_id", StringType()),
    StructField("addressId", StringType()),
    StructField("purpose", StringType()),
    StructField("addressUType", StringType()),
    StructField("addressLine1", StringType()),
    StructField("suburb", StringType()),
    StructField("state", StringType()),
    StructField("postcode", StringType()),
    StructField("country", StringType()),
    # Only one of these is populated, per addressUType.
    StructField("simple", StringType()),
    StructField("paf", StringType()),
])

ORGANISATIONS = StructType([
    StructField("global_id", StringType()),
    StructField("organisationId", StringType()),
    StructField("abn", StringType()),
    StructField("acn", StringType()),
    StructField("businessName", StringType()),
    StructField("legalName", StringType()),
    StructField("shortName", StringType()),
    StructField("organisationType", StringType()),
    StructField("industryCode", StringType()),
    StructField("industryCodeVersion", StringType()),
    StructField("establishmentDate", StringType()),
    StructField("lastUpdateTime", StringType()),
    StructField("registeredCountry", StringType()),
    StructField("isACNCRegistered", BooleanType()),
    StructField("agentFirstName", StringType()),
    StructField("agentLastName", StringType()),
    StructField("agentRole", StringType()),
])

ORGANISATION_PARTY_RELATIONSHIPS = StructType([
    StructField("global_id", StringType()),
    StructField("relationshipId", StringType()),
    StructField("organisationId", StringType()),
    StructField("partyRole", StringType()),
    StructField("isActive", BooleanType()),
    StructField("startDate", StringType()),
    StructField("endDate", StringType()),
    StructField("authorityLevel", StringType()),
])

# Catalogue table — no global_id, so it cannot be joined on the universal key.
ORGANISATION_RELATIONSHIPS = StructType([
    StructField("relationshipId", StringType()),
    StructField("sourceOrgId", StringType()),
    StructField("targetOrgId", StringType()),
    StructField("relationshipType", StringType()),
    StructField("isActive", BooleanType()),
    StructField("startDate", StringType()),
])

BANKING_ACCOUNTS = StructType([
    StructField("global_id", StringType()),
    StructField("accountId", StringType()),
    StructField("customerId", StringType()),
    StructField("organisationId", StringType()),
    StructField("accountType", StringType()),
    StructField("balance", StringType()),
    StructField("isOwner", BooleanType()),
    StructField("openDate", StringType()),
    StructField("isActive", BooleanType()),
    StructField("displayName", StringType()),
    StructField("nickname", StringType()),
    StructField("maskedNumber", StringType()),
    StructField("accountNumber", StringType()),
    StructField("productCategory", StringType()),
    StructField("productName", StringType()),
    StructField("accountOwnership", StringType()),
    StructField("openStatus", StringType()),
    StructField("isOwned", BooleanType()),
    StructField("creationDate", StringType()),
])

BANKING_BALANCES = StructType([
    StructField("global_id", StringType()),
    StructField("accountId", StringType()),
    StructField("currentBalance", StringType()),
    StructField("availableBalance", StringType()),
    StructField("creditLimit", StringType()),
    StructField("amortisedLimit", StringType()),
    StructField("currency", StringType()),
    StructField("purses", StringType()),
])

BANKING_DIRECT_DEBITS = StructType([
    StructField("global_id", StringType()),
    StructField("accountId", StringType()),
    StructField("directDebitId", StringType()),
    StructField("authorisedEntity", StringType()),
    StructField("lastDebitAmount", StringType()),
    StructField("lastDebitDateTime", StringType()),
])

BANKING_PAYEES = StructType([
    StructField("global_id", StringType()),
    StructField("payeeId", StringType()),
    StructField("nickname", StringType()),
    StructField("type", StringType()),
    StructField("description", StringType()),
    StructField("creationDate", StringType()),
])

BANKING_SCHEDULED_PAYMENTS = StructType([
    StructField("global_id", StringType()),
    StructField("accountId", StringType()),
    StructField("scheduledPaymentId", StringType()),
    StructField("nickname", StringType()),
    StructField("status", StringType()),
    StructField("payerReference", StringType()),
    StructField("payeeReference", StringType()),
    StructField("amount", StringType()),
    StructField("frequency", StringType()),
    StructField("nextPaymentDate", StringType()),
    # `from` is a SQL reserved word — this is why callers must read fields with
    # getField() rather than a dotted `struct.from` path.
    StructField("from", StringType()),
    StructField("paymentSet", StringType()),
    StructField("recurrence", StringType()),
])

BANKING_TRANSACTIONS = StructType([
    StructField("global_id", StringType()),
    StructField("transactionId", StringType()),
    StructField("accountId", StringType()),
    StructField("amount", StringType()),
    StructField("transactionType", StringType()),
    StructField("type", StringType()),
    StructField("timestamp", StringType()),
    StructField("description", StringType()),
    StructField("category", StringType()),
    StructField("status", StringType()),
    StructField("reference", StringType()),
    StructField("isDetailAvailable", BooleanType()),
    StructField("postingDateTime", StringType()),
    StructField("valueDateTime", StringType()),
    StructField("executionDateTime", StringType()),
    StructField("currency", StringType()),
    StructField("merchantName", StringType()),
    StructField("merchantCategoryCode", StringType()),
    StructField("billerCode", StringType()),
    StructField("billerName", StringType()),
    StructField("crn", StringType()),
    StructField("apcaNumber", StringType()),
])

CREDIT_CARDS = StructType([
    StructField("global_id", StringType()),
    StructField("creditCardId", StringType()),
    StructField("cardNumber", StringType()),
    StructField("cardType", StringType()),
    StructField("creditLimit", StringType()),
    StructField("currentBalance", StringType()),
    StructField("minPayment", StringType()),
    StructField("interestRate", StringType()),
    StructField("annualFee", StringType()),
    StructField("rewardProgram", StringType()),
    StructField("status", StringType()),
    StructField("issuedDate", StringType()),
    StructField("organisationId", StringType()),
])

LOAN_APPLICATIONS = StructType([
    StructField("global_id", StringType()),
    StructField("applicationId", StringType()),
    StructField("customerId", StringType()),
    StructField("loanGoal", StringType()),
    StructField("applicationType", StringType()),
    StructField("requestedAmount", StringType()),
    StructField("creditScore", IntegerType()),
    StructField("finalOutcome", StringType()),
    StructField("submittedAt", StringType()),
    StructField("lastUpdatedAt", StringType()),
    StructField("numOffers", StringType()),
    StructField("acceptedOfferAmount", StringType()),
    StructField("monthlyCost", StringType()),
    StructField("numberOfTerms", StringType()),
    StructField("totalEvents", StringType()),
    StructField("organisationId", StringType()),
])

MISSING_DOCUMENTS = StructType([
    StructField("global_id", StringType()),
    StructField("documentId", StringType()),
    StructField("applicationId", StringType()),
    StructField("customerId", StringType()),
    StructField("documentType", StringType()),
    StructField("status", StringType()),
    StructField("requestedAt", StringType()),
    StructField("receivedAt", StringType()),
    StructField("expiryDate", StringType()),
    StructField("remindersSent", IntegerType()),
    StructField("lastReminderAt", StringType()),
    StructField("rejectionReason", StringType()),
])

MORTGAGE_ACCOUNTS = StructType([
    StructField("global_id", StringType()),
    StructField("accountId", StringType()),
    StructField("mortgageId", StringType()),
    StructField("propertyValue", StringType()),
    StructField("loanAmount", StringType()),
    StructField("interestRate", StringType()),
    StructField("interestType", StringType()),
    StructField("loanTerm", IntegerType()),
    StructField("repaymentAmount", StringType()),
    StructField("repaymentFrequency", StringType()),
    StructField("offsetAccountId", StringType()),
    StructField("startDate", StringType()),
    StructField("lvrPercent", StringType()),
])

LOAN_ACCOUNTS = StructType([
    StructField("global_id", StringType()),
    StructField("accountId", StringType()),
    StructField("loanId", StringType()),
    StructField("loanType", StringType()),
    StructField("originalAmount", StringType()),
    StructField("currentBalance", StringType()),
    StructField("interestRate", StringType()),
    StructField("termMonths", IntegerType()),
    StructField("repaymentAmount", StringType()),
    StructField("repaymentFrequency", StringType()),
    StructField("status", StringType()),
    StructField("startDate", StringType()),
    StructField("maturityDate", StringType()),
])

SERVICE_CASES = StructType([
    StructField("global_id", StringType()),
    StructField("caseId", StringType()),
    StructField("customerId", StringType()),
    StructField("caseType", StringType()),
    StructField("subject", StringType()),
    StructField("status", StringType()),
    StructField("priority", StringType()),
    StructField("createdAt", StringType()),
    StructField("resolvedAt", StringType()),
    StructField("resolutionSummary", StringType()),
    StructField("assignedTeam", StringType()),
    StructField("organisationId", StringType()),
    StructField("applicationId", StringType()),
    StructField("slaDeadline", StringType()),
])

ENERGY_ACCOUNTS = StructType([
    StructField("global_id", StringType()),
    StructField("energyAccountId", StringType()),
    StructField("accountNumber", StringType()),
    StructField("displayName", StringType()),
    StructField("openStatus", StringType()),
    StructField("creationDate", StringType()),
    StructField("planId", StringType()),
])

ENERGY_SERVICE_POINTS = StructType([
    StructField("global_id", StringType()),
    StructField("energyAccountId", StringType()),
    StructField("servicePointId", StringType()),
    StructField("nationalMeteringId", StringType()),
    StructField("jurisdictionCode", StringType()),
    StructField("servicePointClassification", StringType()),
    StructField("servicePointStatus", StringType()),
    StructField("validFromDate", StringType()),
    StructField("lastUpdateDateTime", StringType()),
    StructField("isGenerator", BooleanType()),
    # Nested CDR objects, stored as JSON text at source.
    StructField("location", StringType()),
    StructField("distributionLossFactor", StringType()),
    StructField("relatedParticipants", StringType()),
    StructField("consumerProfile", StringType()),
    StructField("meters", StringType()),
])

ENERGY_INVOICES = StructType([
    StructField("global_id", StringType()),
    StructField("energyAccountId", StringType()),
    StructField("invoiceNumber", StringType()),
    StructField("issueDate", StringType()),
    StructField("dueDate", StringType()),
    StructField("invoiceAmount", StringType()),
    StructField("gstAmount", StringType()),
    StructField("paymentStatus", StringType()),
    StructField("balanceAtIssue", StringType()),
    # Nested CDR objects, stored as JSON text at source.
    StructField("servicePoints", StringType()),
    StructField("period", StringType()),
    StructField("electricity", StringType()),
    StructField("gas", StringType()),
    StructField("accountCharges", StringType()),
    StructField("payOnTimeDiscount", StringType()),
])

INSURANCE_POLICIES = StructType([
    StructField("global_id", StringType()),
    StructField("customerId", StringType()),
    StructField("policyId", StringType()),
    StructField("policyType", StringType()),
    StructField("provider", StringType()),
    StructField("premiumAmount", StringType()),
    StructField("premiumFrequency", StringType()),
    StructField("coverAmount", StringType()),
    StructField("excessAmount", StringType()),
    StructField("startDate", StringType()),
    StructField("endDate", StringType()),
    StructField("status", StringType()),
    StructField("autoRenewal", BooleanType()),
])

CDC_TABLE_SCHEMAS = {
    "customers": CUSTOMERS,
    "customer_preferences": CUSTOMER_PREFERENCES,
    "kyc_records": KYC_RECORDS,
    "physical_addresses": PHYSICAL_ADDRESSES,
    "organisations": ORGANISATIONS,
    "organisation_party_relationships": ORGANISATION_PARTY_RELATIONSHIPS,
    "organisation_relationships": ORGANISATION_RELATIONSHIPS,
    "banking_accounts": BANKING_ACCOUNTS,
    "banking_balances": BANKING_BALANCES,
    "banking_direct_debits": BANKING_DIRECT_DEBITS,
    "banking_payees": BANKING_PAYEES,
    "banking_scheduled_payments": BANKING_SCHEDULED_PAYMENTS,
    "banking_transactions": BANKING_TRANSACTIONS,
    "credit_cards": CREDIT_CARDS,
    "loan_applications": LOAN_APPLICATIONS,
    "missing_documents": MISSING_DOCUMENTS,
    "mortgage_accounts": MORTGAGE_ACCOUNTS,
    "loan_accounts": LOAN_ACCOUNTS,
    "service_cases": SERVICE_CASES,
    "energy_accounts": ENERGY_ACCOUNTS,
    "energy_service_points": ENERGY_SERVICE_POINTS,
    "energy_invoices": ENERGY_INVOICES,
    "insurance_policies": INSURANCE_POLICIES,
}


# ---------------------------------------------------------------------------
# EVENT source — 5 datasets, Apache Kafka -> bronze.kafka_events
# ---------------------------------------------------------------------------
# These are historical facts, not mutable entities: there is no before/after
# image and no operation type, so the envelope carries the record once, under
# `payload`.

APPLICATION_STAGE_HISTORY = StructType([
    StructField("historyId", StringType()),
    StructField("global_id", StringType()),
    StructField("applicationId", StringType()),
    StructField("stage", StringType()),
    StructField("enteredAt", StringType()),
    StructField("exitedAt", StringType()),
    StructField("assignedTeam", StringType()),
    StructField("assignedTo", StringType()),
    StructField("slaDeadline", StringType()),
    StructField("pendingActionParty", StringType()),
])

LOAN_APPLICATION_EVENTS = StructType([
    StructField("global_id", StringType()),
    StructField("eventId", StringType()),
    StructField("applicationId", StringType()),
    StructField("conceptName", StringType()),
    StructField("lifecycleTransition", StringType()),
    StructField("timestamp", StringType()),
    StructField("orgResource", StringType()),
    StructField("action", StringType()),
    StructField("eventOrigin", StringType()),
    # Offer fields — populated only for O_* concept names.
    StructField("offerId", StringType()),
    StructField("offeredAmount", StringType()),
    StructField("firstWithdrawalAmount", StringType()),
    StructField("numberOfTerms", StringType()),
    StructField("monthlyCost", StringType()),
    StructField("creditScore", StringType()),
    StructField("accepted", StringType()),
    StructField("selected", StringType()),
])

STATUS_CHANGE_HISTORY = StructType([
    StructField("changeId", StringType()),
    StructField("global_id", StringType()),
    StructField("applicationId", StringType()),
    StructField("oldStatus", StringType()),
    StructField("newStatus", StringType()),
    StructField("changedAt", StringType()),
    StructField("changedBy", StringType()),
    StructField("reason", StringType()),
])

SERVICE_CASE_EVENTS = StructType([
    StructField("global_id", StringType()),
    StructField("eventId", StringType()),
    StructField("caseId", StringType()),
    StructField("eventType", StringType()),
    StructField("eventTimestamp", StringType()),
    StructField("performedBy", StringType()),
    StructField("description", StringType()),
])

SUPPORT_INTERACTIONS = StructType([
    StructField("global_id", StringType()),
    StructField("interactionId", StringType()),
    StructField("customerId", StringType()),
    StructField("channel", StringType()),
    StructField("timestamp", StringType()),
    StructField("topic", StringType()),
    StructField("resolution", StringType()),
    StructField("agentId", StringType()),
    StructField("durationMinutes", IntegerType()),
])

EVENT_TABLE_SCHEMAS = {
    "application_stage_history": APPLICATION_STAGE_HISTORY,
    "loan_application_events": LOAN_APPLICATION_EVENTS,
    "status_change_history": STATUS_CHANGE_HISTORY,
    "service_case_events": SERVICE_CASE_EVENTS,
    "support_interactions": SUPPORT_INTERACTIONS,
}


# ---------------------------------------------------------------------------
# FILE source — 4 datasets, S3 micro-batch -> bronze.file_arrivals
# ---------------------------------------------------------------------------

ACCEPTED_LOANS = StructType([
    StructField("global_id", StringType()),
    StructField("loanId", StringType()),
    StructField("memberId", StringType()),
    StructField("url", StringType()),
    StructField("policy_code", StringType()),
    StructField("disbursement_method", StringType()),
    StructField("loanAmount", StringType()),
    StructField("fundedAmount", StringType()),
    StructField("funded_amnt_inv", StringType()),
    StructField("term", StringType()),
    StructField("interestRate", StringType()),
    StructField("installment", StringType()),
    StructField("grade", StringType()),
    StructField("subGrade", StringType()),
    StructField("emp_title", StringType()),
    StructField("empLength", StringType()),
    StructField("homeOwnership", StringType()),
    StructField("annualIncome", StringType()),
    StructField("verification_status", StringType()),
    StructField("application_type", StringType()),
    StructField("dti", StringType()),
    StructField("purpose", StringType()),
    StructField("desc", StringType()),
    StructField("title", StringType()),
    StructField("pymnt_plan", StringType()),
    StructField("zip_code", StringType()),
    StructField("addr_state", StringType()),
    StructField("earliest_cr_line", StringType()),
    StructField("fico_range_low", StringType()),
    StructField("fico_range_high", StringType()),
    StructField("delinq_2yrs", StringType()),
    StructField("inq_last_6mths", StringType()),
    StructField("mths_since_last_delinq", StringType()),
    StructField("mths_since_last_record", StringType()),
    StructField("open_acc", StringType()),
    StructField("pub_rec", StringType()),
    StructField("total_acc", StringType()),
    StructField("collections_12_mths_ex_med", StringType()),
    StructField("mths_since_last_major_derog", StringType()),
    StructField("acc_now_delinq", StringType()),
    StructField("tot_coll_amt", StringType()),
    StructField("pub_rec_bankruptcies", StringType()),
    StructField("tax_liens", StringType()),
    StructField("chargeoff_within_12_mths", StringType()),
    StructField("delinq_amnt", StringType()),
    StructField("revol_bal", StringType()),
    StructField("revol_util", StringType()),
    StructField("total_rev_hi_lim", StringType()),
    StructField("bc_open_to_buy", StringType()),
    StructField("bc_util", StringType()),
    StructField("max_bal_bc", StringType()),
    StructField("num_actv_bc_tl", StringType()),
    StructField("num_bc_sats", StringType()),
    StructField("num_bc_tl", StringType()),
    StructField("num_actv_rev_tl", StringType()),
    StructField("num_op_rev_tl", StringType()),
    StructField("num_rev_accts", StringType()),
    StructField("num_rev_tl_bal_gt_0", StringType()),
    StructField("open_rv_12m", StringType()),
    StructField("open_rv_24m", StringType()),
    StructField("percent_bc_gt_75", StringType()),
    StructField("pct_tl_nvr_dlq", StringType()),
    StructField("mths_since_recent_bc", StringType()),
    StructField("mths_since_recent_bc_dlq", StringType()),
    StructField("mths_since_recent_revol_delinq", StringType()),
    StructField("total_bal_il", StringType()),
    StructField("il_util", StringType()),
    StructField("open_act_il", StringType()),
    StructField("open_il_12m", StringType()),
    StructField("open_il_24m", StringType()),
    StructField("mths_since_rcnt_il", StringType()),
    StructField("num_il_tl", StringType()),
    StructField("total_il_high_credit_limit", StringType()),
    StructField("mo_sin_old_il_acct", StringType()),
    StructField("total_bal_ex_mort", StringType()),
    StructField("tot_cur_bal", StringType()),
    StructField("tot_hi_cred_lim", StringType()),
    StructField("total_bc_limit", StringType()),
    StructField("all_util", StringType()),
    StructField("avg_cur_bal", StringType()),
    StructField("acc_open_past_24mths", StringType()),
    StructField("num_sats", StringType()),
    StructField("num_tl_120dpd_2m", StringType()),
    StructField("num_tl_30dpd", StringType()),
    StructField("num_tl_90g_dpd_24m", StringType()),
    StructField("num_tl_op_past_12m", StringType()),
    StructField("inq_fi", StringType()),
    StructField("total_cu_tl", StringType()),
    StructField("inq_last_12m", StringType()),
    StructField("open_acc_6m", StringType()),
    StructField("mort_acc", StringType()),
    StructField("mo_sin_old_rev_tl_op", StringType()),
    StructField("mo_sin_rcnt_rev_tl_op", StringType()),
    StructField("mo_sin_rcnt_tl", StringType()),
    StructField("mths_since_recent_inq", StringType()),
    StructField("num_accts_ever_120_pd", StringType()),
    StructField("loanStatus", StringType()),
    StructField("issuedAt", StringType()),
    StructField("initial_list_status", StringType()),
    StructField("out_prncp", StringType()),
    StructField("out_prncp_inv", StringType()),
    StructField("total_pymnt", StringType()),
    StructField("total_pymnt_inv", StringType()),
    StructField("total_rec_prncp", StringType()),
    StructField("total_rec_int", StringType()),
    StructField("total_rec_late_fee", StringType()),
    StructField("recoveries", StringType()),
    StructField("collection_recovery_fee", StringType()),
    StructField("last_pymnt_d", StringType()),
    StructField("last_pymnt_amnt", StringType()),
    StructField("next_pymnt_d", StringType()),
    StructField("last_credit_pull_d", StringType()),
    StructField("last_fico_range_low", StringType()),
    StructField("last_fico_range_high", StringType()),
    # Joint-application fields — populated only for application_type "Joint App".
    StructField("annual_inc_joint", StringType()),
    StructField("dti_joint", StringType()),
    StructField("verification_status_joint", StringType()),
    StructField("revol_bal_joint", StringType()),
    StructField("sec_app_fico_range_low", StringType()),
    StructField("sec_app_fico_range_high", StringType()),
    StructField("sec_app_earliest_cr_line", StringType()),
    StructField("sec_app_inq_last_6mths", StringType()),
    StructField("sec_app_mort_acc", StringType()),
    StructField("sec_app_open_acc", StringType()),
    StructField("sec_app_revol_util", StringType()),
    StructField("sec_app_open_act_il", StringType()),
    StructField("sec_app_num_rev_accts", StringType()),
    StructField("sec_app_chargeoff_within_12_mths", StringType()),
    StructField("sec_app_collections_12_mths_ex_med", StringType()),
    StructField("sec_app_mths_since_last_major_derog", StringType()),
    # Hardship program — populated only when hardship_flag = "Y" (~3%).
    StructField("hardship_flag", StringType()),
    StructField("hardship_type", StringType()),
    StructField("hardship_reason", StringType()),
    StructField("hardship_status", StringType()),
    StructField("deferral_term", StringType()),
    StructField("hardship_amount", StringType()),
    StructField("hardship_start_date", StringType()),
    StructField("hardship_end_date", StringType()),
    StructField("payment_plan_start_date", StringType()),
    StructField("hardship_length", StringType()),
    StructField("hardship_dpd", StringType()),
    StructField("hardship_loan_status", StringType()),
    StructField("orig_projected_additional_accrued_interest", StringType()),
    StructField("hardship_payoff_balance_amount", StringType()),
    StructField("hardship_last_payment_amount", StringType()),
    # Debt settlement — populated only when debt_settlement_flag = "Y" (~2%).
    StructField("debt_settlement_flag", StringType()),
    StructField("debt_settlement_flag_date", StringType()),
    StructField("settlement_status", StringType()),
    StructField("settlement_date", StringType()),
    StructField("settlement_amount", StringType()),
    StructField("settlement_percentage", StringType()),
    StructField("settlement_term", StringType()),
])

REJECTED_APPLICATIONS = StructType([
    StructField("global_id", StringType()),
    StructField("loanId", StringType()),
    StructField("requestedAmount", StringType()),
    StructField("applicationDate", StringType()),
    StructField("riskScore", StringType()),
    StructField("debtToIncomeRatio", StringType()),
    StructField("rejectionReason", StringType()),
    StructField("applicationTitle", StringType()),
    StructField("zipCode", StringType()),
    StructField("state", StringType()),
    StructField("employmentLength", StringType()),
    StructField("policyCode", StringType()),
])

# Catalogue table — no global_id, so it cannot be joined on the universal key.
BANKING_PRODUCTS = StructType([
    StructField("productId", StringType()),
    StructField("name", StringType()),
    StructField("description", StringType()),
    StructField("productCategory", StringType()),
    StructField("brand", StringType()),
    StructField("brandName", StringType()),
    StructField("applicationUri", StringType()),
    # bool in master-schema.json; text in the CSV.
    StructField("isTailored", StringType()),
    StructField("effectiveFrom", StringType()),
    StructField("effectiveTo", StringType()),
    StructField("lastUpdated", StringType()),
    # CDR JSON objects embedded as text inside the CSV cell.
    StructField("additionalInformation", StringType()),
    StructField("cardArt", StringType()),
])

# Catalogue table — no global_id, so it cannot be joined on the universal key.
ENERGY_PLANS = StructType([
    StructField("planId", StringType()),
    StructField("brand", StringType()),
    StructField("brandName", StringType()),
    StructField("displayName", StringType()),
    StructField("fuelType", StringType()),
    StructField("type", StringType()),
    StructField("customerType", StringType()),
    StructField("effectiveFrom", StringType()),
    StructField("lastUpdated", StringType()),
])

FILE_TABLE_SCHEMAS = {
    "accepted_loans": ACCEPTED_LOANS,
    "rejected_applications": REJECTED_APPLICATIONS,
    "banking_products": BANKING_PRODUCTS,
    "energy_plans": ENERGY_PLANS,
}


# ---------------------------------------------------------------------------
# Envelopes
# ---------------------------------------------------------------------------

# Debezium "source" block: where and when the change happened in PostgreSQL.
# `schema` is deliberately left out — only the table name is wanted, and fields
# omitted from a from_json schema are simply ignored.
DEBEZIUM_SOURCE_SCHEMA = StructType([
    StructField("version", StringType()),
    StructField("connector", StringType()),
    StructField("name", StringType()),
    StructField("db", StringType()),
    StructField("table", StringType()),
    StructField("snapshot", StringType()),
    StructField("ts_ms", LongType()),
    StructField("ts_us", LongType()),
    StructField("ts_ns", LongType()),
    StructField("txId", LongType()),
    StructField("lsn", LongType()),
    StructField("xmin", LongType()),
    # JSON-encoded string, not a real array: "[\"83074744\",\"83075032\"]".
    StructField("sequence", StringType()),
])


def debezium_envelope_schema(record_schema):
    """Wrap one CDC table's row schema in the Debezium envelope.

    `before` and `after` share the same shape (both are a full copy of the row),
    so only the record schema changes per table. `transaction` sits at the root,
    not inside `source`, and is kept as raw JSON text so enabling connector
    transaction metadata later cannot break the parse.
    """
    return StructType([
        StructField("before", record_schema),
        StructField("after", record_schema),
        StructField("source", DEBEZIUM_SOURCE_SCHEMA),
        StructField("transaction", StringType()),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("ts_us", LongType()),
        StructField("ts_ns", LongType()),
    ])


def kafka_event_envelope_schema(record_schema):
    """Wrap one event table's row schema in the Kafka business-event envelope.

    Shape is set by the producers in `source-simulator` (bootstrap-loader and
    activity-generator): fixed envelope fields plus the record under `payload`.
    Producers may emit a partial payload — `from_json` returns NULL for missing
    fields, so a partial event degrades into NULLs rather than failing.
    """
    return StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_version", StringType()),
        StructField("occurred_at", StringType()),
        StructField("producer", StringType()),
        StructField("correlation_id", StringType()),
        StructField("source_dataset", StringType()),
        StructField("payload", record_schema),
    ])
