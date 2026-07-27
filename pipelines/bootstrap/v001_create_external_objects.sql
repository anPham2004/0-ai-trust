-- Run once with a principal that owns the storage credential/external location.
CREATE CATALOG IF NOT EXISTS `0-ai-trust`
MANAGED LOCATION 's3://g3-assignment/g3/0-ai-trust/__managed'
COMMENT 'Zero Trust AI data product; all business data is stored in customer-owned S3';

CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.bronze
COMMENT 'Incremental native ingestion and source history';
CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.silver
COMMENT 'Reserved modelling template: validated and quarantined data';
CREATE SCHEMA IF NOT EXISTS `0-ai-trust`.gold
COMMENT 'Reserved modelling template: masked AI-ready data products';

-- Databricks creates this convenience schema with every new catalog. It is not
-- part of the data product and is removed to keep the project namespace strict.
DROP SCHEMA IF EXISTS `0-ai-trust`.default;

CREATE EXTERNAL VOLUME IF NOT EXISTS `0-ai-trust`.bronze.landing
LOCATION 's3://g3-assignment/g3/0-ai-trust/bronze/landing'
COMMENT 'Immutable source-native landing zone';
