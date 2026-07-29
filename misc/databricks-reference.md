# Databricks Reference: Key Terms, Concepts & How-To

A comprehensive reference covering Medallion Architecture, data pipelines, modeling, governance, AI/ML, and all major Databricks platform capabilities.

---

## Table of Contents

1. [Medallion Architecture](#1-medallion-architecture)
2. [Delta Lake](#2-delta-lake)
3. [Data Pipelines](#3-data-pipelines)
4. [Data Modeling](#4-data-modeling)
5. [Unity Catalog & Governance](#5-unity-catalog--governance)
6. [Metric Views](#6-metric-views)
7. [Compute](#7-compute)
8. [AI & Machine Learning](#8-ai--machine-learning)
9. [Streaming](#9-streaming)
10. [Storage & Files](#10-storage--files)
11. [Asset Bundles (DABs)](#11-asset-bundles-dabs)
12. [Observability & System Tables](#12-observability--system-tables)
13. [Quick Decision Map](#13-quick-decision-map)

---

## 1. Medallion Architecture

### Definition

A layered data organization pattern where raw data flows through progressively cleaner zones:
`Bronze → Silver → Gold`

Each layer has a single responsibility. Data is never deleted from a lower layer — higher layers are derived from it.

### Layers

#### Bronze (Raw / Landing)

| Property | Value |
|---|---|
| **Grain** | Exactly as received from the source |
| **Format** | Delta table (preserves original schema) |
| **Transformations** | None — append-only ingestion |
| **Retention** | Long-term (source of truth for replay) |
| **Contains** | Raw JSON, CSV, CDC events, API payloads |

**Purpose:** Immutable audit log of all source data. Enables replay and re-processing.

```python
# Typical Bronze write — no transformation
df_raw.write.format("delta").mode("append").saveAsTable("bronze.crm.accounts_raw")
```

**Metadata columns to add at Bronze:**
- `_ingest_timestamp` — when the record was loaded
- `_source_file` / `_source_system` — provenance
- `_batch_id` / `_job_run_id` — lineage traceability

---

#### Silver (Cleansed / Conformed)

| Property | Value |
|---|---|
| **Grain** | One row per business entity event (after dedup) |
| **Format** | Delta table |
| **Transformations** | Type casting, null handling, dedup, CDC merge |
| **PII** | Masked or tokenized |
| **Contains** | Cleaned, deduplicated, validated records |

**Purpose:** Reliable, queryable business records. Source of truth for Gold modeling.

```sql
-- Silver CDC merge pattern
MERGE INTO silver.crm.accounts AS target
USING bronze.crm.accounts_raw AS source
ON target.account_id = source.account_id
WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Silver contract fields (standard):**
- `_silver_timestamp` — when the Silver record was written
- `_is_deleted` — soft delete flag
- `_source_system` — originating system

---

#### Gold (Business / Serving)

| Property | Value |
|---|---|
| **Grain** | Defined per business use case |
| **Format** | Delta table (fact + dimension tables) |
| **Transformations** | Business rules, joins, aggregations |
| **Audience** | Analysts, dashboards, AI, Genie |
| **Contains** | KPIs, conformed dimensions, business facts |

**Purpose:** Optimized for consumption. Built on Silver; never reads Bronze directly.

```
Silver Layer
     │
     ▼
Gold Core Tables (clean facts + dimensions)
     │
     ▼
Metric Views (semantic + governance layer)
     │
     ▼
AI / Genie / Dashboards / SQL queries
```

---

### Medallion Flow Summary

```
Source Systems
     │ (Lakeflow Connect / Autoloader / Kafka)
     ▼
Bronze (raw, append-only, immutable)
     │ (DLT pipeline / Spark job)
     ▼
Silver (cleansed, deduped, CDC merged)
     │ (DLT pipeline / Spark job)
     ▼
Gold (business facts + dimensions)
     │
     ▼
Metric Views → Genie / Dashboards / SQL
```

---

## 2. Delta Lake

### Definition

The open-source storage layer that powers all Databricks tables. Built on Parquet files + a transaction log (`_delta_log`).

### Key Features

| Feature | What It Gives You |
|---|---|
| **ACID transactions** | Concurrent reads/writes without corruption |
| **Time travel** | Query any historical version with `VERSION AS OF` or `TIMESTAMP AS OF` |
| **Schema enforcement** | Rejects writes that break the schema |
| **Schema evolution** | `mergeSchema` option to add new columns safely |
| **MERGE** | Upsert (insert + update + delete) in one statement |
| **Z-ordering** | Co-locate related data for faster range queries |
| **Liquid clustering** | Auto-adaptive clustering (replaces static partitioning) |
| **Deletion vectors** | Mark deleted rows without rewriting files |
| **Change Data Feed (CDF)** | Track row-level changes (`_change_type`, `_commit_version`) |

### Key Commands

```sql
-- Time travel
SELECT * FROM silver.crm.accounts VERSION AS OF 10;
SELECT * FROM silver.crm.accounts TIMESTAMP AS OF '2026-01-01';

-- Read CDC changes between versions
SELECT * FROM table_changes('silver.crm.accounts', 5, 10);

-- Optimize file sizes + Z-order for common query column
OPTIMIZE gold.lending.fact_applications ZORDER BY (customer_id, application_date);

-- Liquid clustering (preferred over ZORDER for new tables)
ALTER TABLE gold.lending.fact_applications CLUSTER BY (customer_id, application_date);

-- Vacuum old files (default 7-day retention)
VACUUM gold.lending.fact_applications RETAIN 168 HOURS;

-- Show history
DESCRIBE HISTORY silver.crm.accounts;
```

### Delta vs Parquet

| | Delta | Parquet |
|---|---|---|
| ACID | Yes | No |
| MERGE/UPDATE/DELETE | Yes | No |
| Time travel | Yes | No |
| Schema enforcement | Yes | No |
| Streaming + batch unified | Yes | No |

---

## 3. Data Pipelines

### 3.1 Lakeflow Declarative Pipelines (DLT)

**Definition:** A declarative framework for building reliable, auto-scaling ETL pipelines. You define *what* to compute; Databricks manages *how* to run it (ordering, retries, restarts).

**Use case:** Bronze → Silver → Gold transformations with built-in data quality.

```python
# Modern API: "from pyspark import pipelines as dp" (NOT "import dlt" — legacy)
from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Bronze: raw ingestion (Streaming Table)
@dp.table(comment="Raw accounts from CRM")
def bronze_accounts():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("/Volumes/landing/crm/accounts/")
    )

# Silver: cleansed with quality constraints
@dp.table(comment="Cleansed accounts")
@dp.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
@dp.expect("valid_status", "status IN ('active', 'closed', 'pending')")
def silver_accounts():
    return spark.readStream.table("bronze_accounts").select(
        col("account_id").cast("string"),
        col("status"),
        col("created_date").cast("date"),
    )
```

**Table types (modern API):**
- `@dp.table` — Streaming Table (streaming source) or Materialized View (batch source)
- `@dp.temporary_view` — pipeline-private, not persisted to UC
- `@dp.expect` / `@dp.expect_or_drop` / `@dp.expect_or_fail` — data quality rules

> **Legacy DLT notice:** `import dlt`, `@dlt.table`, `dlt.read_stream`, `@dlt.view` are the deprecated DLT API. Always migrate to `from pyspark import pipelines as dp`.

**Pipeline modes:**
- **Triggered** — run once and stop (batch)
- **Continuous** — run 24/7 (near real-time streaming)

---

### 3.2 Databricks Jobs

**Definition:** Orchestration layer for scheduling and running tasks (notebooks, Python scripts, DLT pipelines, SQL queries, dbt) with dependency management.

**Use case:** Multi-step workflows, scheduled pipelines, CI/CD triggers.

```bash
# Deploy a job via CLI
databricks jobs create --json @job-config.json

# Run a job now (positional job_id — no --job-id flag)
databricks jobs run-now 12345

# Run with parameters
databricks jobs run-now --json '{"job_id": 12345, "job_parameters": {"env": "prod"}}'

# Get run status
databricks jobs get-run --run-id 67890
```

**Key concepts:**
- **Task** — individual unit of work (notebook, wheel, JAR, SQL, DLT pipeline)
- **Task dependency** — `depends_on` controls execution order (DAG)
- **Cluster per task** or **shared job cluster** — cost trade-off
- **Retry policy** — automatic retries on failure
- **Repair run** — re-run only failed tasks without restarting succeeded ones

---

### 3.3 Lakeflow Connect (Managed Ingestion)

**Definition:** No-code connectors for ingesting data from SaaS and databases (Salesforce, ServiceNow, PostgreSQL, MySQL, etc.) directly into Delta tables.

**Use case:** Replace custom ingestion scripts with managed, monitored connectors.

```bash
# Create an ingestion pipeline (ingestion_definition, NOT libraries block)
databricks pipelines create --json '{
  "name": "salesforce_to_uc",
  "ingestion_definition": {
    "connection_name": "my_salesforce_oauth_connection",
    "objects": [
      {"table": {"source_schema": "salesforce", "source_table": "Account",
                 "destination_catalog": "main", "destination_schema": "salesforce_raw"}}
    ]
  }
}'

# Trigger a run (triggered-only — no continuous mode)
databricks pipelines start-update <pipeline-id>
```

> **Anti-pattern:** Lakeflow Connect uses `ingestion_definition`, NOT a `libraries` block. Also `continuous: true` is rejected — pipelines are triggered-only, scheduled via a Jobs `pipeline_task`.

---

### 3.4 Auto Loader (cloudFiles)

**Definition:** Structured Streaming source that incrementally ingests new files from cloud storage (S3, ADLS, GCS) as they arrive.

**Use case:** File-based Bronze ingestion (JSON, CSV, Parquet, Avro, etc.).

```python
df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/checkpoints/accounts/schema")
        .load("/landing/crm/accounts/")
)
```

**Why over manual file listing:** Auto Loader tracks which files were processed (checkpoint), detects schema changes, and scales to millions of files.

---

## 4. Data Modeling

### 4.1 Grain

**Definition:** The precise statement of what **one row represents** in a table.

**Rule:** Declare grain before designing any table. Every dimension must be at or above the grain; every measure aggregates *from* the grain.

| Table | Grain |
|---|---|
| `fact_applications` | One row = one loan application |
| `fact_application_daily_status` | One row = one application, one day |
| `dim_customer` | One row = one customer (current state) |
| `fact_payments` | One row = one payment transaction |

---

### 4.2 Fact Tables

**Definition:** Tables that record business events or measurements. Contain foreign keys to dimensions + numeric measures.

**Types:**
- **Transaction fact** — one row per event (payment, application submitted)
- **Snapshot fact** — state at a point in time (daily account balance)
- **Accumulating snapshot** — tracks lifecycle milestones (application → approval → disbursement)

```sql
-- Example: accumulating snapshot fact
CREATE TABLE gold.lending.fact_applications (
  application_sk        BIGINT NOT NULL,  -- surrogate key
  application_id        STRING NOT NULL,  -- natural key
  customer_sk           BIGINT,
  product_sk            BIGINT,
  submitted_date_sk     INT,
  approved_date_sk      INT,              -- NULL until approved
  disbursed_date_sk     INT,              -- NULL until disbursed
  loan_amount           DECIMAL(18,2),
  approved_amount       DECIMAL(18,2),
  days_to_approval      INT,
  current_status        STRING
) USING DELTA
CLUSTER BY (customer_sk, submitted_date_sk);
```

---

### 4.3 Dimension Tables

**Definition:** Descriptive context for facts. Contain attributes used for filtering, grouping, and labeling.

**Types:**
- **SCD Type 1** — overwrite (no history)
- **SCD Type 2** — add new row with effective dates (full history)
- **Conformed dimension** — shared across multiple fact tables (e.g., `dim_customer`, `dim_date`)

```sql
-- SCD Type 2 dimension
CREATE TABLE gold.shared.dim_customer (
  customer_sk       BIGINT NOT NULL,  -- surrogate key (auto-increment)
  customer_id       STRING NOT NULL,  -- natural key
  full_name         STRING,
  segment           STRING,
  risk_tier         STRING,
  _eff_start_date   DATE NOT NULL,
  _eff_end_date     DATE,             -- NULL = current record
  _is_current       BOOLEAN NOT NULL
) USING DELTA;
```

---

### 4.4 Star Schema

**Definition:** One central fact table surrounded by denormalized dimension tables. Optimized for query performance.

```
dim_customer ──┐
dim_product  ──┤── fact_applications ──┬── dim_date
dim_branch   ──┘                       └── dim_status
```

---

### 4.5 Snowflake Schema

**Definition:** Extension of star schema where dimensions are normalized (split into sub-dimensions). More storage-efficient but adds join complexity.

```
dim_customer ──► dim_address ──► dim_city ──► dim_region
```

**When to use:** When dimension attributes change independently (city/region rarely change with customer).

---

### 4.6 Surrogate Key vs Natural Key

| | Surrogate Key | Natural Key |
|---|---|---|
| **Definition** | System-generated integer (BIGINT IDENTITY) | Business identifier (account_id, customer_id) |
| **Purpose** | Join performance, SCD Type 2 stability | Business lookups, deduplication |
| **Stability** | Never changes | Can change (source system migration) |
| **Use in Gold** | FK/PK relationships in fact/dim tables | Include for traceability |

---

### 4.7 CDC (Change Data Capture)

**Definition:** Pattern to capture row-level changes (INSERT, UPDATE, DELETE) from source systems and propagate them downstream.

**Delta Lake CDF fields:**
- `_change_type` — `insert`, `update_preimage`, `update_postimage`, `delete`
- `_commit_version` — Delta version of the change
- `_commit_timestamp` — when the change was committed

```sql
-- Enable CDF on a table
ALTER TABLE silver.crm.accounts SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Read changes from Silver for Gold merge
SELECT * FROM table_changes('silver.crm.accounts', startVersion => 100)
WHERE _change_type IN ('insert', 'update_postimage');
```

---

## 5. Unity Catalog & Governance

### Definition

Unity Catalog (UC) is Databricks' unified governance layer for all data and AI assets across workspaces. Three-level namespace: `catalog.schema.table`.

### Securable Hierarchy

```
Metastore (account-level)
└── Catalog
    └── Schema (Database)
        ├── Table / View / Metric View
        ├── Volume (files)
        └── Function
```

### Privilege Model

```sql
-- Minimum grants to read a table
GRANT USE CATALOG ON CATALOG analytics TO `data_readers`;
GRANT USE SCHEMA  ON SCHEMA analytics.gold TO `data_readers`;
GRANT SELECT      ON TABLE analytics.gold.fact_applications TO `data_readers`;

-- Grant on entire schema (inherits to all tables)
GRANT SELECT ON SCHEMA analytics.gold TO `data_readers`;

-- Check who has access
SHOW GRANTS ON TABLE analytics.gold.fact_applications;

-- Transfer ownership
ALTER TABLE analytics.gold.fact_applications OWNER TO `data_engineers`;
```

### Fine-Grained Access

**Row filters** — filter rows based on current user:
```sql
CREATE FUNCTION gold.shared.filter_by_branch(branch_id STRING)
  RETURNS BOOLEAN
  RETURN is_account_group_member('branch_' || branch_id);

ALTER TABLE gold.lending.fact_applications
  SET ROW FILTER gold.shared.filter_by_branch ON (branch_id);
```

**Column masks** — mask PII for unauthorized users:
```sql
CREATE FUNCTION gold.shared.mask_nric(nric STRING)
  RETURNS STRING
  RETURN CASE WHEN is_account_group_member('pii_viewers') THEN nric ELSE 'XXXX' END;

ALTER TABLE silver.crm.customers
  ALTER COLUMN nric SET MASK gold.shared.mask_nric;
```

### External Locations & Storage Credentials

```sql
-- Create storage credential (AWS IAM role)
CREATE STORAGE CREDENTIAL my_s3_cred
  WITH AWS IAM ROLE 'arn:aws:iam::123456789:role/databricks-role';

-- Create external location backed by the credential
CREATE EXTERNAL LOCATION landing_zone
  URL 's3://my-bucket/landing/'
  WITH (STORAGE CREDENTIAL my_s3_cred);
```

### Volumes

**Definition:** UC-governed file storage under a three-part namespace (`catalog.schema.volume`). Replaces DBFS for unstructured files.

```bash
# Upload files to a volume
databricks fs cp -r /tmp/data dbfs:/Volumes/catalog/schema/volume/dest

# List volume contents
databricks fs ls dbfs:/Volumes/catalog/schema/volume/
```

**Managed volume** — files stored in UC-managed cloud path.
**External volume** — files stored at a custom external location.

---

## 6. Metric Views

### Definition

A Unity Catalog object (`CREATE VIEW … WITH METRICS LANGUAGE YAML`) that defines reusable, governed business metrics. Separates dimension groupings from measure definitions — consumers choose aggregation level at query time.

### Why Over Standard Views

| | Standard View | Metric View |
|---|---|---|
| Aggregation flexibility | Fixed at creation | Flexible at query time |
| Safe ratio re-aggregation | No | Yes |
| Declarative joins | No | Yes (YAML) |
| Built-in materialization | No | Yes (experimental) |
| Genie / AI/BI native | Limited | Native |

### Create a Metric View

```sql
CREATE OR REPLACE VIEW gold.lending.application_metrics
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: gold.lending.fact_applications
  comment: "Lending KPIs for application funnel analysis"
  dimensions:
    - name: Application Month
      expr: DATE_TRUNC('MONTH', submitted_date)
    - name: Product Type
      expr: product_type
    - name: Branch
      expr: branch_name
    - name: Current Status
      expr: current_status
  measures:
    - name: Application Count
      expr: COUNT(1)
    - name: Total Loan Amount
      expr: SUM(loan_amount)
    - name: Approval Rate
      expr: COUNT(CASE WHEN current_status = 'approved' THEN 1 END) / COUNT(1)
    - name: Avg Days to Approval
      expr: AVG(days_to_approval)
$$;
```

### Query a Metric View

```sql
-- Always use MEASURE() for aggregated values
SELECT
  `Application Month`,
  `Product Type`,
  MEASURE(`Application Count`)  AS application_count,
  MEASURE(`Approval Rate`)      AS approval_rate,
  MEASURE(`Avg Days to Approval`) AS avg_days
FROM gold.lending.application_metrics
WHERE extract(year FROM `Application Month`) = 2026
GROUP BY ALL
ORDER BY `Application Month`;
```

### Multi-table Metric View (with joins)

```sql
CREATE OR REPLACE VIEW gold.lending.customer_application_metrics
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: gold.lending.fact_applications
  joins:
    - name: customer
      source: gold.shared.dim_customer
      on: source.customer_sk = customer.customer_sk AND customer._is_current = true
  dimensions:
    - name: Customer Segment
      expr: customer.segment
    - name: Risk Tier
      expr: customer.risk_tier
    - name: Application Month
      expr: DATE_TRUNC('MONTH', source.submitted_date)
  measures:
    - name: Application Count
      expr: COUNT(1)
    - name: Total Exposure
      expr: SUM(source.loan_amount)
$$;
```

> **Join syntax:** `source.` refers to the primary source table; `<join_name>.` refers to the joined table by the join's `name` field. SQL-style table aliases in the `source:` field are not valid YAML.

### Constraints
- `SELECT *` not supported — must name dimensions/measures
- All measures must use `MEASURE()` wrapper
- Joins must be declared in YAML, not in the query
- Requires DBR 17.2+ (v1.1 YAML)

---

## 7. Compute

### 7.1 All-Purpose Clusters

**Definition:** Long-running clusters for interactive development (notebooks, SQL editor). Billed per DBU while running.

**Use:** Development, exploration, iterative work. **Not** for scheduled production jobs (too expensive to leave on).

### 7.2 Job Clusters

**Definition:** Ephemeral clusters that start with a job and terminate when it finishes.

**Use:** Scheduled production pipelines. Cost-efficient — no idle time.

### 7.3 SQL Warehouses

**Definition:** Optimized compute for SQL workloads (BI dashboards, SQL editor, metric view queries, Genie).

**Types:**
- **Classic** — traditional multi-node
- **Serverless** — instant start, no cluster management, auto-scales to zero

```bash
# List warehouses
databricks warehouses list

# Get warehouse details
databricks warehouses get <warehouse-id>
```

### 7.4 Serverless Compute

**Definition:** Databricks-managed compute that starts in seconds and auto-scales. No cluster configuration needed.

**Use:** SQL warehouses, DLT pipelines, Databricks Apps, notebook serverless mode.

### 7.5 Instance Pools

**Definition:** Pre-allocated VM pool to reduce cluster start time by reusing idle instances.

**Use:** Jobs with many short-lived clusters (reduce 3-5 min startup to ~30 sec).

---

## 8. AI & Machine Learning

### 8.1 MLflow

**Definition:** Open-source platform (built into Databricks) for tracking experiments, packaging models, and managing the model lifecycle.

**Key concepts:**
- **Experiment** — group of runs (parent folder must exist before `set_experiment`)
- **Run** — single training execution (logs params, metrics, artifacts)
- **Model registry** — versioned model store with movable aliases (`@prod`, `@challenger`; UC dropped `Staging`/`Production` stages)

```python
import mlflow

# REQUIRED: point to UC registry (default is legacy workspace registry)
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Users/me@example.com/my_project/experiment")

with mlflow.start_run():
    mlflow.log_param("learning_rate", 0.01)
    mlflow.log_metric("accuracy", 0.92)
    mlflow.sklearn.log_model(model, "model",
                             registered_model_name="catalog.schema.credit_score_model")

# UC uses movable aliases — NOT deprecated Staging/Production stages
from mlflow.tracking import MlflowClient
client = MlflowClient(registry_uri="databricks-uc")
client.set_registered_model_alias("catalog.schema.credit_score_model", "prod", version="3")

# Load by alias
model = mlflow.pyfunc.load_model("models:/catalog.schema.credit_score_model@prod")
```

---

### 8.2 Model Serving (Mosaic AI Model Serving)

**Definition:** Deploy ML models as REST API endpoints with auto-scaling. Supports custom models, Foundation Models, and external models.

```bash
# Create a serving endpoint (NAME is a positional arg, then --json for config)
databricks serving-endpoints create credit-score-endpoint --json '{
  "served_entities": [{
    "entity_name": "catalog.schema.credit_score_model",
    "entity_version": "3",
    "scale_to_zero_enabled": true,
    "workload_size": "Small"
  }],
  "traffic_config": {
    "routes": [{"served_entity_name": "credit_score_model-3", "traffic_percentage": 100}]
  }
}'

# Poll readiness — check BOTH fields (state.ready AND state.config_update)
databricks serving-endpoints get credit-score-endpoint \
  | jq '{ready: .state.ready, config_update: .state.config_update}'
# Ready when: ready == "READY" AND config_update == "NOT_UPDATING"
```

> **Deprecated field:** `served_models` (old API). Current API uses `served_entities` with `entity_name` / `entity_version`.

---

### 8.3 Vector Search

**Definition:** Managed similarity search index for embeddings. Used to build RAG (Retrieval-Augmented Generation) pipelines.

**Use case:** Semantic document search, customer FAQ retrieval, similar-case lookup.

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create endpoint
w.vector_search_endpoints.create_endpoint(
    name="my-vs-endpoint",
    endpoint_type="STANDARD"  # or "STORAGE_OPTIMIZED" (7x lower cost, 300-500ms latency)
)

# Create a delta sync index with managed embeddings
w.vector_search_indexes.create_index(
    name="catalog.schema.document_embeddings",
    endpoint_name="my-vs-endpoint",
    primary_key="doc_id",
    index_type="DELTA_SYNC",
    delta_sync_index_spec={
        "source_table": "catalog.schema.documents",
        "embedding_source_columns": [{        # plural — list of columns
            "name": "content",
            "embedding_model_endpoint_name": "databricks-gte-large-en"  # 1024-dim
        }],
        "pipeline_type": "TRIGGERED"
    }
)
```

**Endpoint types:**
- `STANDARD` — 20–50ms latency, up to 320M vectors
- `STORAGE_OPTIMIZED` — 300–500ms latency, 1B+ vectors, 7× lower cost

---

### 8.4 AI Functions

**Definition:** Built-in SQL functions that call LLMs inline within SQL queries. No Python code needed.

| Function | Signature | What It Does |
|---|---|---|
| `ai_query` | `(endpoint, request)` | Call any model serving endpoint (last resort) |
| `ai_classify` | `(content, labels_json, [options])` | Classify text; labels is a JSON string |
| `ai_extract` | `(content, schema_json, [options])` | Extract fields; schema is a JSON string |
| `ai_summarize` | `(content, [max_words])` | Summarize text |
| `ai_mask` | `(content, labels ARRAY<STRING>)` | Mask PII entities → `[MASKED]` |
| `ai_analyze_sentiment` | `(content)` | Returns `positive`/`negative`/`neutral`/`mixed` |
| `ai_similarity` | `(expr1, expr2)` | Semantic similarity score 0.0–1.0 |
| `ai_parse_document` | `(content BINARY, [options])` | Parse PDF/Office/image docs (DBR 17.3+) |
| `ai_forecast` | `(observed TABLE, horizon, ...)` | Time series forecasting (table-valued) |

> **Important:** `ai_classify` and `ai_extract` take **JSON strings** for labels/schema, NOT Python `ARRAY()`. Results are `VARIANT` — extract with `:response[0]::STRING`. Materialize once to Delta; never re-call per query (slow + billed per token).

```sql
-- Classify — labels is a JSON string; extract result with :response[0]::STRING
SELECT
  application_id,
  ai_classify(comments, '["positive","negative","neutral"]',
              map('version','2.0')):response[0]::STRING AS sentiment
FROM gold.lending.fact_applications
WHERE comments IS NOT NULL;

-- Extract fields — schema is a JSON string (v2.0 recommended)
SELECT
  ai_extract(notes,
    '{"income":{"type":"number"},"employment_type":{"type":"string"},"years_employed":{"type":"number"}}',
    map('version','2.0')):response AS extracted
FROM silver.crm.customer_notes;

-- Mask PII — labels is an ARRAY<STRING>
SELECT ai_mask(message, array('person', 'email', 'phone')) AS message_safe
FROM raw_messages;
```

---

### 8.5 Agent Bricks / Genie

**Definition:** Natural language interface over Unity Catalog data. Users ask questions in plain English; Genie generates SQL and returns answers.

**Genie Space** — curated workspace with trusted tables, metrics, and example questions.

**Agent Bricks** — build multi-agent AI systems (supervisor + specialized agents) on top of Databricks data.

---

### 8.6 MLflow Evaluation

**Definition:** Framework for evaluating LLM and GenAI applications (response quality, relevance, faithfulness, toxicity).

```python
import mlflow

with mlflow.start_run():
    results = mlflow.evaluate(
        model=rag_chain,
        data=eval_dataset,
        model_type="question-answering",
        evaluators="default"
    )
    print(results.metrics)
```

---

## 9. Streaming

### 9.1 Spark Structured Streaming

**Definition:** Continuous processing engine built on Spark. Treats a stream as an unbounded table — same DataFrame API as batch.

**Trigger modes:**
- `Trigger.Once()` — process all available data, then stop (single micro-batch)
- `Trigger.AvailableNow()` — like Once but uses multiple micro-batches (preferred over Once)
- `Trigger.ProcessingTime("5 minutes")` — run every N time units
- `Trigger.Continuous("1 second")` — true continuous (experimental)

**Production checklist:**
- Checkpoint must be on a UC Volume (`/Volumes/...`), NOT DBFS
- One unique checkpoint location per stream
- Use fixed-size clusters for streaming (no autoscaling)
- Watermark required for stateful operations (windowed aggregations)

```python
# Read from Kafka, write to Delta (Silver)
(
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "payments")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/catalog/schema/volume/checkpoints/payments")  # UC Volume, not DBFS
        .trigger(availableNow=True)
        .toTable("silver.payments.transactions")
)
```

---

### 9.2 Zerobus Streaming Ingest

**Definition:** Managed, low-latency streaming ingestion from event brokers (Kafka, Kinesis, Event Hubs) into Delta tables. Zero-code alternative to custom Structured Streaming jobs.

---

## 10. Storage & Files

### 10.1 Delta Table Storage Layout

```
catalog/schema/table_name/
├── _delta_log/           ← transaction log (JSON + Parquet checkpoints)
│   ├── 00000000000000000000.json
│   └── 00000000000000000010.checkpoint.parquet
├── part-00000-xxx.parquet
└── part-00001-xxx.parquet
```

### 10.2 Iceberg & UniForm

**Definition:** Databricks tables can expose an Iceberg-compatible metadata layer (UniForm) so external engines (Trino, Spark on other clouds, Snowflake) can read Delta tables via Iceberg REST Catalog (IRC).

```sql
-- Enable UniForm on an existing Delta table (all three properties required)
ALTER TABLE gold.lending.fact_applications
SET TBLPROPERTIES (
  'delta.columnMapping.mode'          = 'name',
  'delta.enableIcebergCompatV2'       = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

> **Note:** UniForm generates Iceberg metadata asynchronously — brief delay before external engines see latest data. CDF (`delta.enableChangeDataFeed`) is NOT supported on UniForm tables.

### 10.3 Delta Sharing

**Definition:** Open protocol for sharing live data across organizations without copying it. Recipient uses their own compute (Spark, Pandas, Power BI).

### 10.4 DBFS (Legacy)

**Definition:** Databricks File System — abstraction over cloud storage. Still used with `dbfs:/Volumes/…` prefix for CLI operations. For new projects, prefer Volumes over raw DBFS paths.

---

## 11. Asset Bundles (DABs)

### Definition

Infrastructure-as-code for Databricks. Define jobs, pipelines, clusters, permissions, and model serving endpoints in YAML; deploy via CLI.

### Structure

```
my-project/
├── databricks.yml          ← root bundle config
├── resources/
│   ├── bronze_pipeline.yml
│   ├── silver_gold_job.yml
│   └── model_serving.yml
└── src/
    ├── pipelines/
    └── notebooks/
```

### Key Commands

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to a target environment
databricks bundle deploy --target dev
databricks bundle deploy --target prod

# Run a resource after deployment
databricks bundle run bronze_pipeline

# Destroy deployed resources
databricks bundle destroy --target dev
```

### `databricks.yml` Example

```yaml
bundle:
  name: lending-data-platform

targets:
  dev:
    default: true
    workspace:
      host: https://adb-xxxx.azuredatabricks.net

resources:
  pipelines:
    bronze_silver_pipeline:
      name: bronze-silver-lending
      schema: dev_catalog.bronze   # "schema" in modern API; "target" is legacy DLT
      libraries:
        - notebook:
            path: ./src/pipelines/bronze_silver.py

  jobs:
    gold_refresh:
      name: gold-layer-refresh
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: Asia/Singapore
      tasks:
        - task_key: refresh_facts
          notebook_task:
            notebook_path: ./src/notebooks/gold_refresh.py
```

---

## 12. Observability & System Tables

### Definition

System tables are Unity Catalog built-in tables under the `system` catalog that provide audit, billing, lineage, and compute observability across the entire account.

### Key System Tables

| Table | What It Tracks |
|---|---|
| `system.access.audit` | All API calls, permission changes, login events |
| `system.access.table_lineage` | Which tables read from / write to which tables |
| `system.access.column_lineage` | Column-level data lineage |
| `system.billing.usage` | DBU consumption by workspace, SKU, and date |
| `system.billing.list_prices` | DBU pricing by SKU |
| `system.compute.clusters` | Cluster creation, config, and termination history |
| `system.compute.warehouse_events` | SQL warehouse start/stop/autoscale events |
| `system.lakeflow.pipeline_events` | DLT pipeline run events and errors |
| `system.query.history` | Executed SQL queries with duration and user |

### Common Queries

```sql
-- Who accessed a sensitive table in the last 7 days?
SELECT event_time, user_identity.email, action_name
FROM system.access.audit
WHERE request_params.table_full_name = 'gold.lending.fact_applications'
  AND event_date >= current_date() - 7
ORDER BY event_time DESC;

-- What tables depend on silver.crm.accounts?
SELECT DISTINCT target_table_full_name
FROM system.access.table_lineage
WHERE source_table_full_name = 'silver.crm.accounts'
  AND event_date >= current_date() - 30;

-- DBU cost by workspace this month
SELECT workspace_id, sku_name, SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= date_trunc('month', current_date())
GROUP BY 1, 2
ORDER BY dbus DESC;

-- Slowest queries this week
SELECT statement_id, executed_by, duration / 1000 AS duration_sec, query_text
FROM system.query.history
WHERE start_time >= current_date() - 7
ORDER BY duration DESC
LIMIT 20;
```

---

## 13. Quick Decision Map

### What tool for what job?

| Need | Tool |
|---|---|
| Ingest files from cloud storage | Auto Loader (`cloudFiles`) |
| Ingest from SaaS / database (no code) | Lakeflow Connect |
| Ingest from Kafka / Kinesis | Zerobus or Structured Streaming |
| Build Bronze → Silver → Gold ETL | Lakeflow Declarative Pipelines (DLT) |
| Orchestrate multi-step workflows | Databricks Jobs |
| Govern data access | Unity Catalog + GRANT/REVOKE |
| Mask PII / row-level security | Column masks + Row filters |
| Define reusable KPIs | Metric Views |
| Natural language queries | Genie |
| BI dashboards | AI/BI Dashboards |
| Train ML models | MLflow + Databricks ML |
| Serve ML models as API | Model Serving |
| Semantic document search | Vector Search + Embeddings |
| LLM enrichment in SQL | AI Functions (`ai_classify`, `ai_extract`) |
| Deploy infrastructure as code | Asset Bundles (DABs) |
| Monitor cost / audit / lineage | System Tables |
| Share data externally | Delta Sharing |
| External engine reads Delta | UniForm / Iceberg REST Catalog |

### Medallion Layer Responsibility Map

| Concern | Layer |
|---|---|
| Raw data preservation | Bronze |
| Dedup, cleanse, type-cast, CDC merge | Silver |
| Business facts + conformed dimensions | Gold |
| What metrics can be queried + by whom | Metric Views |
| Row/column visibility per user | Unity Catalog row filters + column masks |

---

*Last updated: 2026-07-27 | Branch: feat/bronze-silver*
