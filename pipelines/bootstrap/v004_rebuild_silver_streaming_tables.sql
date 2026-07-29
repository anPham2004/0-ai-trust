-- Run once after stopping the pipeline. This removes the superseded Silver
-- materialized-view model so the same pipeline can create streaming tables.
-- Bronze and immutable landing data are not changed.

DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_individual;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_kyc_kyb_record;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_organisation;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_organisation_party_relationship;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_organisation_relationship;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.arr_banking_arrangement;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.arr_loan_arrangement;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.arr_mortgage_arrangement;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.arr_credit_card_arrangement;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_application;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_application_stage_history;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_status_change_history;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_document;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_application_event;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_accepted_loan;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_rejected_application;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.evt_service_case;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.evt_support_interaction;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.evt_service_case_event;

DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_individual_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_kyc_kyb_record_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_organisation_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_organisation_party_relationship_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.ip_organisation_relationship_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.arr_banking_arrangement_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.arr_loan_arrangement_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.arr_mortgage_arrangement_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.arr_credit_card_arrangement_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_application_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_application_stage_history_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_status_change_history_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_document_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_application_event_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_accepted_loan_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.app_rejected_application_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.evt_service_case_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.evt_support_interaction_quarantine;
DROP MATERIALIZED VIEW IF EXISTS `0-ai-trust`.silver.evt_service_case_event_quarantine;
