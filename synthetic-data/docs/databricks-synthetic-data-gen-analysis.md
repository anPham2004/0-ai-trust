# Databricks Synthetic Data Gen — Skill Analysis

**Generated:** 2026-07-16
**Source:** `/Users/khoatran/Developer/G3/databricks-agent-skills/`
**Focus:** `skills/databricks-synthetic-data-gen/`

---

## 1. Repo Overview

**databricks-agent-skills** is a skills repository for AI coding assistants (Claude Code, Cursor, Copilot, Codex, Antigravity) providing Databricks-specific guidance and workflows.

- **31 stable skills** covering Databricks products (Jobs, Pipelines, Apps, Model Serving, Vector Search, Unity Catalog, etc.)
- **Experimental skills** in `/experimental/` (best-effort, not officially supported)
- **Hooks** — prompt router, session context primer, auth-failure hints
- **Commands** — `/databricks:setup` (auth/onboarding), `/databricks:doctor` (health check)
- **Plugins** — per-provider bundles for claude, codex, copilot, cursor

### All 31 Skills

| # | Skill | Description |
|---|-------|-------------|
| 1 | databricks-agent-bricks | Agent building |
| 2 | databricks-ai-functions | AI functions |
| 3 | databricks-aibi-dashboards | AI/BI dashboards |
| 4 | databricks-app-design | App design |
| 5 | databricks-apps | Apps |
| 6 | databricks-apps-python | Python apps |
| 7 | databricks-core | Parent skill — CLI, auth, data exploration |
| 8 | databricks-dabs | Databricks Asset Bundles |
| 9 | databricks-data-discovery | Data discovery |
| 10 | databricks-dbsql | Databricks SQL |
| 11 | databricks-docs | Documentation |
| 12 | databricks-execution-compute | Execution compute |
| 13 | databricks-iceberg | Iceberg tables |
| 14 | databricks-jobs | Jobs orchestration |
| 15 | databricks-lakebase | Lakebase |
| 16 | databricks-lakeflow-connect | Lakeflow Connect |
| 17 | databricks-metric-views | Metric views |
| 18 | databricks-ml-training | ML training |
| 19 | databricks-mlflow-evaluation | MLflow evaluation |
| 20 | databricks-model-serving | Model serving |
| 21 | databricks-pipelines | Pipelines |
| 22 | databricks-python-sdk | Python SDK |
| 23 | databricks-serverless-migration | Serverless migration |
| 24 | databricks-spark-structured-streaming | Spark structured streaming |
| 25 | **databricks-synthetic-data-gen** | Synthetic data generation |
| 26 | databricks-unity-catalog | Unity Catalog |
| 27 | databricks-unstructured-pdf-generation | Unstructured PDF generation |
| 28 | databricks-vector-search | Vector search |
| 29 | databricks-zerobus-ingest | Zerobus ingestion |

---

## 2. Synthetic Data Gen Skill — Deep Dive

**Location:** `skills/databricks-synthetic-data-gen/`
**Parent skill:** `databricks-core`
**Version:** 0.1.0

### File Structure

```
skills/databricks-synthetic-data-gen/
├── SKILL.md                          # 267 lines — main skill doc
├── references/
│   ├── 1-data-patterns.md            # 146 lines — distribution patterns
│   └── 2-troubleshooting.md          # 344 lines — common errors & fixes
├── scripts/
│   └── generate_synthetic_data.py    # 300 lines — reference implementation
└── agents/
    └── openai.yaml                   # Codex marketplace metadata
```

### Tech Stack

| Component | Purpose | Notes |
|-----------|---------|-------|
| **Databricks Connect** | Spark session from local env | Serverless recommended (no cluster startup delay) |
| **Spark** | Distributed data generation | `spark.range()` + transformations + `write.parquet()` |
| **Faker** | Realistic data generation | Imported INSIDE pandas UDFs, not at module level |
| **Pandas UDFs** | Batch processing with Faker | `@F.pandas_udf()` for parallel execution |
| **NumPy** | Statistical distributions | `np.random.lognormal()`, `np.random.exponential()` |
| **Pandas** | UDF parameter/return handling | Series-based batch operations |
| **Holidays** | Seasonal/holiday pattern detection | Optional for time-based multipliers |
| **Unity Catalog** | Storage organization | Catalog.schema.volume hierarchy |
| **Volumes** | Raw data storage | `/Volumes/{catalog}/{schema}/raw_data/` |
| **Delta Tables** | Intermediate storage | For referential integrity joins |
| **Parquet/JSON/CSV** | Output formats | Parquet default for raw data |

### Core Workflow (3 Steps)

#### Step 1: Gather Requirements (mandatory)

- Catalog/Schema — NEVER default
- Domain (e.g., e-commerce, support tickets, IoT, financial)
- Business story — propose one if not provided

#### Step 2: Present Plan with Story (before ANY code)

- Show output location prominently
- Describe business story with specific incident/anomaly
- Present table specification with assumptions
- Show business metrics with $ impact
- Ask user approval

#### Step 3: Generate with Validated Patterns (after approval)

- Use Spark + Faker + Pandas UDFs
- Respect performance rules (no `.cache()` on serverless)
- Write master tables to Delta first
- Create child tables with valid foreign keys

### 10 Key Principles

| # | Principle | Detail |
|---|-----------|--------|
| 1 | Data tells a story | Business impact ($), root cause analysis, actionable insights |
| 2 | All data serves the story | No orphan columns, coherent across tables |
| 3 | Industry terms, simple schema | Domain vocabulary, few tables, clear relationships |
| 4 | Never uniform distributions | Skewed categories, log-normal amounts, 80/20 patterns |
| 5 | Enough data for trends | ~100K+ rows for main tables |
| 6 | Always ask for catalog/schema | Never default |
| 7 | Present plan for approval | Show tables, distributions, assumptions |
| 8 | Master tables first | Parent before children, write to Delta, read back for FKs |
| 9 | Spark + Faker + Pandas UDFs | Scalable, parallel generation |
| 10 | Databricks Connect Serverless | Default execution environment |

### Anti-Patterns (NEVER do)

- `.cache()` or `.persist()` on serverless — not supported
- Python loops or `.collect()` on driver — use Spark parallelism
- Scalar UDFs — use `pandas_udf` for batch processing
- Broadcast variables with serverless
- Default catalog/schema
- Uniform/flat distributions

---

## 3. Data Patterns Reference

### Distribution Patterns

| Pattern | Use Case | Code Pattern |
|---------|----------|--------------|
| **Log-normal** | Monetary amounts, durations | `np.random.lognormal(mean=5.5, sigma=0.8)` → ~$245 median |
| **Pareto (80/20)** | Revenue concentration | 20% of customers = 80% of revenue |
| **Weighted categorical** | Enum fields | `F.when(F.rand() < 0.6, "Free").when(F.rand() < 0.9, "Pro").otherwise("Enterprise")` |
| **Exponential** | Time-to-event, wait times | `np.random.exponential(scale=mean_wait)` |
| **Seasonal** | Time-based patterns | Weekday/weekend multipliers, business hours, holidays |

### Row Coherence

Attributes within a row must correlate:
- Enterprise tier → higher amounts, faster resolution
- High priority → shorter SLA, senior team assignment
- Complaint case → lower satisfaction, more follow-ups

### Temporal Patterns

- Business hours (9am–5pm) for internal actions
- Skewed weekday (70%) vs weekend (30%) for customer interactions
- Seasonality multipliers for volume (e.g., end-of-quarter spikes)
- Trends over time (growth, degradation, cyclic)

---

## 4. Reference Implementation Details

**File:** `scripts/generate_synthetic_data.py` (300 lines)

### Configuration Block

```python
USE_SERVERLESS = True
CATALOG = "<YOUR_CATALOG>"
SCHEMA = "<YOUR_SCHEMA>"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
N_CUSTOMERS = 10_000
N_ORDERS = 50_000
PARTITIONS = 16
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=180)
```

### Session Creation

```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless(True).getOrCreate()
```

### Pandas UDFs (Faker-based)

```python
@F.pandas_udf("string")
def fake_name(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker()
    return pd.Series([fake.name() for _ in range(len(ids))])
```

Similar UDFs for: `fake_company()`, `fake_address()`, `fake_email()`, `generate_lognormal_amount()`

### Customer Table Generation

- ID format: `CUST-00001`
- Tier distribution: Free 60%, Pro 30%, Enterprise 10%
- Region distribution: North 40%, South 25%, East 20%, West 15%
- Amounts: Log-normal by tier (Enterprise $1800, Pro $245, Free $55)
- Created date: Random within 2 years before start date
- Output: `{VOLUME_PATH}/customers` (Parquet)

### Orders Table with FK Integrity

- ID format: `ORD-000001`
- FK pattern: hash-based customer_idx → lookup join → valid customer_id
- Status: delivered 65%, shipped 15%, processing 10%, pending 5%, cancelled 5%
- Amount: tier-based log-normal
- **Critical pattern:** Write customers to temp Delta table, read back for FK join (NO `.cache()`)

### Bad Data Injection (Optional)

```python
INJECT_BAD_DATA = False  # Set to True for data quality testing
BAD_DATA_CONFIG = {
    "null_rate": 0.02,           # 2% nulls
    "outlier_rate": 0.01,        # 1% impossible values
    "orphan_fk_rate": 0.01,      # 1% orphan foreign keys
}
```

Injects: null customer_ids, negative amounts (-999.99), orphan FKs (CUST-NONEXISTENT)

### Partition Strategy

| Row Count | Partitions |
|-----------|-----------|
| <100K | 8 |
| 100K–500K | 16 |
| 500K–1M | 32 |
| 1M+ | 64+ |

---

## 5. Troubleshooting Reference

### Critical Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: faker` | Faker not installed locally | `pip install faker`, import inside UDF |
| `cache()/persist() not supported` | Serverless doesn't support | Write to Delta, read back |
| `F.window vs Window` | Wrong import | `from pyspark.sql.window import Window` for `row_number()`, `rank()` |
| Scalar UDF slow | Row-by-row execution | Use `@F.pandas_udf()` for batch processing |
| Referential integrity violations | FK not validated | Master table first → Delta → read back → join |

### Validation Queries

```sql
-- Row counts
SELECT COUNT(*) FROM catalog.schema.customers;

-- Distribution verification
SELECT tier, COUNT(*) as cnt, ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 1) as pct
FROM catalog.schema.customers GROUP BY tier;

-- Referential integrity check
SELECT COUNT(*) as orphan_orders
FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

---

## 6. Applicability to Gap Analysis

### Context

The [AI Questions Gap Analysis](./ai-questions-gap-analysis.md) identified **6 missing datasets** needed to answer 37 business questions for Banker.AI and Customer.AI personas. Only 1/37 questions is mostly covered; 18 (49%) have zero coverage.

**Schemas in scope:** CDR, BPI 2017, Lending Club (MLAR excluded per requirement)

### What the Skill Covers Well

| Gap Analysis Need | Skill Support |
|---|---|
| Fully synthetic entities (service_case, support_interaction, missing_document) | Core use case — master/child table generation with realistic distributions |
| FK relationships (customer_id → service_case, application_id → missing_document) | Built-in pattern: write master to Delta, read back for FK joins |
| Weighted enums (case_type, channel, status, priority) | Native via `F.when(F.rand())` chains |
| Time-series data (interaction timestamps, stage history) | Temporal patterns with seasonality, business hours |
| Data quality testing (null rates, outliers, orphan FKs) | Built-in `BAD_DATA_CONFIG` toggle |
| Scale (100K+ rows for trends) | Spark-based, scales to millions |

### Mapping Missing Datasets to Generation Strategy

#### 1. `service_case` — Fully Synthetic

| Field | Generation Strategy |
|-------|---------------------|
| `case_id` | UUID or `CASE-00001` format |
| `customer_id` | FK join from Customer master table |
| `case_type` | Weighted: inquiry 40%, request 30%, complaint 20%, dispute 10% |
| `subject` | Faker `fake.sentence()` via pandas UDF |
| `status` | Weighted: resolved 50%, closed 20%, open 15%, in_progress 15% |
| `priority` | Weighted: low 30%, medium 40%, high 20%, critical 10% |
| `created_at` | Random datetime within 180-day range, business hours bias |
| `resolved_at` | `created_at` + log-normal resolution time (null if open) |
| `resolution_summary` | Faker `fake.paragraph()` (null if open) |
| `assigned_team` | Weighted: Support 50%, Complaints 25%, Escalations 15%, Fraud 10% |

**Row coherence:** complaint/dispute → higher priority, longer resolution; inquiry → lower priority, faster resolution

#### 2. `support_interaction` — Fully Synthetic

| Field | Generation Strategy |
|-------|---------------------|
| `interaction_id` | UUID |
| `customer_id` | FK join from Customer |
| `channel` | Weighted: chat 40%, phone 30%, email 20%, branch 10% |
| `timestamp` | Temporal pattern: business hours weighted, weekday bias |
| `topic` | Weighted categories from domain vocabulary |
| `resolution` | Weighted: resolved 60%, escalated 20%, follow_up 15%, unresolved 5% |
| `agent_id` | Random from staff pool (Pareto: 20% agents handle 80% interactions) |
| `duration_minutes` | Log-normal: chat ~8min, phone ~12min, branch ~20min |

**Row coherence:** channel → duration correlation; escalated → longer duration

#### 3. `missing_document` — Linked to Application

| Field | Generation Strategy |
|-------|---------------------|
| `document_id` | UUID |
| `application_id` | FK join from Application |
| `customer_id` | FK join from Customer |
| `document_type` | Weighted: payslip 25%, ID_proof 20%, address_proof 20%, bank_statement 15%, tax_return 10%, other 10% |
| `status` | Weighted: verified 40%, received 25%, required 15%, invalid 10%, expired 10% |
| `requested_at` | Application created_at + small offset |
| `received_at` | `requested_at` + exponential wait (null if required) |
| `expiry_date` | `received_at` + 90/180/365 days (null if N/A) |
| `reminders_sent` | 0–5, exponential distribution |
| `last_reminder_at` | Based on reminders_sent count |
| `rejection_reason` | Null unless status=invalid; then weighted reasons |

**Row coherence:** more reminders → longer wait; expired docs → older received_at

#### 4. Customer Preferences Extension

| Field | Generation Strategy |
|-------|---------------------|
| `preferred_contact_channel` | Weighted: email 40%, phone 25%, sms 20%, chat 15% |
| `preferred_language` | Weighted: en 70%, es 10%, zh 8%, vi 5%, other 7% |
| `marketing_opt_in` | Boolean: true 65%, false 35% |
| `satisfaction_score` | Log-normal centered at 3.8/5.0, slight left skew |

**Row coherence:** marketing_opt_in correlates with satisfaction_score

#### 5. `application_stage_history` — Derived from BPI or Synthetic

| Field | Generation Strategy |
|-------|---------------------|
| `history_id` | UUID |
| `application_id` | FK from Application |
| `stage` | CRM stages: Submitted → Document Verification → Assessment → Approved/Denied |
| `entered_at` | Sequential timestamps per application |
| `exited_at` | `entered_at` + log-normal stage duration (null if current) |
| `assigned_team` | Weighted per stage (Intake, Review, Underwriting) |
| `assigned_to` | Random from team pool |
| `sla_deadline` | `entered_at` + fixed SLA per stage type |

**Option A:** Transform BPI `LoanApplicationEvent` → map `concept:name` to CRM stages
**Option B:** Fully synthetic with realistic stage sequencing

#### 6. `status_change_history` — Derived from BPI or Synthetic

| Field | Generation Strategy |
|-------|---------------------|
| `change_id` | UUID |
| `application_id` | FK from Application |
| `old_status` | Previous stage |
| `new_status` | Next stage |
| `changed_at` | Stage transition timestamp |
| `changed_by` | Staff ID (from BPI `org:resource` or synthetic) |
| `reason` | Weighted reasons per transition type |

---

## 7. Caveats & Limitations

### Infrastructure Dependency

The skill requires **Databricks workspace** with:
- Databricks Connect CLI (>= v1.0.0)
- Unity Catalog access
- Serverless compute

If no Databricks environment is available, the **patterns** (distributions, FK integrity, Faker UDFs) can be adapted to:
- Plain PySpark (local mode)
- Pandas + Faker (for <30K rows)
- Polars + Faker (for local performance)

### Not a Schema-Driven Generator

The skill is a **prompt/guide for AI assistants**, not a CLI tool that reads schema JSON files. Your existing schema contracts (CDR, BPI, Lending Club) serve as **reference documentation** for the AI, not direct inputs to a pipeline.

### Existing Data Enrichment

The skill generates data from scratch. For:
- Adding preference fields to existing Customer entity → custom enrichment logic needed
- Transforming BPI events into CRM stages → custom ETL pipeline needed

### BPI Stage Mapping

BPI `concept:name` values are Dutch financial process terms:
- `W_Completeren aanvraag` → Complete Application
- `W_Valideren aanvraag` → Validate Application
- `A_Accepted` → Application Accepted

A mapping table is needed to translate to English CRM stages.

---

## 8. Recommendation

### Use the skill as a pattern library, not its infrastructure

| Reusable Pattern | Application |
|-----------------|-------------|
| Pandas UDF + Faker | `support_interaction.resolution`, `service_case.subject`, free-text fields |
| Weighted categorical distribution | All enum fields (case_type, channel, status, priority) |
| Log-normal amounts | `duration_minutes`, `resolution_time`, `satisfaction_score` |
| Master → Delta → FK join | All parent-child relationships |
| Bad data injection | Data quality testing layer |
| Temporal patterns | `timestamp`, `created_at`, `resolved_at` with realistic distributions |
| Row coherence rules | Cross-field correlation (tier→amount, priority→resolution_time) |

### Suggested Approach

1. **Define schemas** for 6 missing datasets (field names, types, enums, distributions) — already done in gap analysis
2. **Write PySpark generation scripts** following skill patterns — weighted categoricals, log-normal, temporal, FK integrity
3. **If Databricks available** → use Databricks Connect serverless for scale, output to Unity Catalog Volumes
4. **If local-only** → adapt to plain PySpark local mode or pandas+Faker, output to Parquet/CSV
5. **Apply bad data injection** for data quality testing scenarios
6. **Validate** with distribution checks, FK integrity queries, row coherence spot checks

### Coverage Impact

Generating all 6 missing datasets closes the gap analysis:

| Before | After |
|--------|-------|
| 1 mostly covered | 7 mostly covered |
| 6 derivable | 6 fully covered (made explicit) |
| 12 partial | 12 fully covered |
| 18 not covered | 12 fully covered |
| **49% zero coverage** | **0% zero coverage** |
