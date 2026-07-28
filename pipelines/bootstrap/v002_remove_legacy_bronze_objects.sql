-- Run once after stopping the legacy three-table pipeline and before the
-- source-aligned pipeline completes its first update.
DROP VIEW IF EXISTS `0-ai-trust`.bronze.cdc_scd2;
DROP VIEW IF EXISTS `0-ai-trust`.bronze.kafka_events_deduplicated;
DROP TABLE IF EXISTS `0-ai-trust`.bronze.cdc_changes;
DROP TABLE IF EXISTS `0-ai-trust`.bronze.kafka_events;
DROP TABLE IF EXISTS `0-ai-trust`.bronze.file_arrivals;
