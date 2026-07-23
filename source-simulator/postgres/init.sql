CREATE SCHEMA IF NOT EXISTS integration;

CREATE TABLE IF NOT EXISTS integration.bootstrap_audit (
  dataset TEXT PRIMARY KEY,
  source_record_count BIGINT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS integration.bootstrap_state (
  source_fingerprint TEXT PRIMARY KEY,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Exact source-replica tables are created by bootstrap-loader from the real Parquet schemas.
