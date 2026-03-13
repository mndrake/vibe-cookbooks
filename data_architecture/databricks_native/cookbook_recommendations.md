# Cookbook Recommendations — Databricks Native Stack

> **Scope:** This document covers the Databricks native stack only — PySpark, Spark SQL, Delta Lake, Delta Live Tables, Databricks Workflows, and Databricks Asset Bundles. It does not cover dbt, AutomateDV, or any external orchestration tooling.
>
> This is a **decision guide**, not an implementation guide. Each recommendation points to the relevant cookbook section for step-by-step implementation details.

---

## How to Use This Document

Use this file when you know the business requirement and need to identify which cookbook section to read. Each section below is organised by scenario. Find the scenario that matches your situation, follow the recommendation, and navigate to the linked cookbook section for implementation.

---

## Ingestion

### Which Ingestion Method Should I Use?

| Scenario | Recommended Method | Cookbook Section |
|----------|--------------------|-----------------|
| Files arrive continuously in ADLS/S3/GCS and must be ingested without reprocessing | **Auto Loader** | `ingestion/ingestion_cookbook.md` — File Ingestion — Auto Loader |
| A one-time or scheduled bulk load from existing files already in cloud storage | **COPY INTO** | `ingestion/ingestion_cookbook.md` — File Ingestion — COPY INTO |
| Files arrive from a partner or vendor via SFTP | **Native SFTP Connector** | `ingestion/ingestion_cookbook.md` — File Ingestion — SFTP |
| Events arrive from Kafka, Azure Event Hubs, or Kinesis with sub-minute latency requirements | **Structured Streaming** | `ingestion/ingestion_cookbook.md` — Streaming Ingestion — Structured Streaming |
| Real-time event ingestion with declarative pipeline management, auto-scaling, and data quality rules | **Delta Live Tables** | `ingestion/ingestion_cookbook.md` — Streaming Ingestion — Delta Live Tables |
| Extracting data from a relational database (SQL Server, PostgreSQL, MySQL, Oracle) | **JDBC** | `ingestion/ingestion_cookbook.md` — Database Ingestion — JDBC |
| Ingesting from a SaaS application (Salesforce, Workday, ServiceNow, Google Analytics) | **Lakeflow Connect** | `ingestion/ingestion_cookbook.md` — Managed Ingestion — Lakeflow Connect |
| A third-party connector (Fivetran, Airbyte) already manages the source integration | **Partner Connectors** | `ingestion/ingestion_cookbook.md` — Managed Ingestion — Partner Connectors |

**Decision rule:** If files land in cloud storage, use Auto Loader (continuous) or COPY INTO (batch). If the source is a database or API, use JDBC, Lakeflow Connect, or a partner connector. If sub-minute latency matters, use Structured Streaming or DLT.

---

### Auto Loader vs. COPY INTO

Use this table when both methods could apply (files in cloud storage, batch or micro-batch schedule):

| Factor | Auto Loader | COPY INTO |
|--------|-------------|-----------|
| File tracking | Automatic via checkpoint — processes only new files | Manual state managed by Databricks — tracks loaded files internally |
| Schema evolution | Built-in `cloudFiles.schemaEvolutionMode` options | Manual — schema must be declared or inferred at each run |
| Streaming support | Native — runs as a streaming query | Batch only |
| Trigger model | `availableNow` (micro-batch) or continuous | Runs as a discrete SQL command |
| Best for | Ongoing, recurring file ingestion | One-time loads or ad hoc ingestion of files already in storage |
| Pattern reference | `ingestion_patterns.md` — Auto Loader vs. COPY INTO | Same |

**Recommendation:** Default to Auto Loader for all ongoing file ingestion. Use COPY INTO only for one-time historical loads or when the source delivers infrequent, predictable batches that do not justify streaming infrastructure.

---

### Structured Streaming vs. Delta Live Tables

Both support streaming. Choose based on operational model:

| Factor | Structured Streaming (manual) | Delta Live Tables |
|--------|-------------------------------|-------------------|
| Pipeline management | Manual — you manage checkpoints, restarts, and backfill logic | Managed — Databricks handles restarts, checkpoints, and event logging |
| Data quality | Manual `filter` / `when` logic | Declarative `EXPECT` constraints with quarantine and failure modes |
| Backfill | Manual job re-runs against source | `FULL REFRESH` command restores pipeline from source |
| Cost model | Job cluster (per run) or always-on cluster | DLT pipeline compute (Enhanced Autoscaling) |
| Schema enforcement | Manual | Automatic — DLT infers and evolves schema |
| Best for | Simple streaming jobs where full DLT overhead is not justified | Multi-hop streaming pipelines with quality gates and operational visibility |

**Recommendation:** For new streaming pipelines on Databricks, prefer DLT. Use Structured Streaming directly when DLT pipeline overhead is not justified (e.g., a single-hop passthrough into a Bronze table from a managed source that already guarantees quality).

---

## Processing and Transformation

### Which Transformation Method Should I Use?

| Scenario | Recommended Approach | Cookbook Section |
|----------|----------------------|-----------------|
| Upsert — insert new records, update existing ones based on a key | **MERGE INTO** | `processing/processing_cookbook.md` — Delta Lake Transformations — MERGE INTO |
| Correct or update records already in a Delta table | **UPDATE** | `processing/processing_cookbook.md` — Delta Lake Transformations — UPDATE and DELETE |
| Remove records that should no longer be present (GDPR, corrections) | **DELETE** | `processing/processing_cookbook.md` — Delta Lake Transformations — UPDATE and DELETE |
| Compute aggregates, running totals, or ranked values across a dataset | **Window functions and aggregations** | `processing/processing_cookbook.md` — Aggregations and Summarization |
| Standardise, type-cast, null-handle, and deduplicate source records | **Cleansing and deduplication** | `processing/processing_cookbook.md` — Data Cleansing and Deduplication |
| Track current state of a slowly changing attribute — no history | **SCD Type 1** | `processing/processing_cookbook.md` — Slowly Changing Dimensions — SCD Type 1 |
| Maintain full history of attribute changes with effective dates | **SCD Type 2 (Delta MERGE)** | `processing/processing_cookbook.md` — Slowly Changing Dimensions — SCD Type 2 (Native Delta MERGE) |
| SCD Type 2 in a declarative, managed pipeline with automatic restart | **SCD Type 2 via DLT** | `processing/processing_cookbook.md` — Slowly Changing Dimensions — SCD Type 2 via DLT APPLY CHANGES INTO |
| Enrich a fact stream with dimension attributes at query time | **Joins and Enrichment** | `processing/processing_cookbook.md` — Joins and Enrichment |
| Process only records changed since the last run | **Incremental Load Patterns** | `processing/processing_cookbook.md` — Incremental Load Patterns — Databricks Jobs |

---

### SCD Type 2 — Delta MERGE vs. DLT APPLY CHANGES INTO

| Factor | Delta MERGE (manual) | DLT APPLY CHANGES INTO |
|--------|----------------------|------------------------|
| Implementation | Two-pass MERGE pattern in PySpark or Spark SQL | Single declarative statement in a DLT pipeline notebook |
| Restart / recovery | Manual — re-run logic must be idempotent by design | Managed — DLT handles restart and replay automatically |
| Late-arriving records | Custom logic required | Handled natively via `SEQUENCE BY` column |
| Out-of-order records | Custom logic required | Handled natively via `SEQUENCE BY` column |
| Suitable for | Databricks Jobs (non-DLT pipelines) | DLT pipelines only |
| Complexity | Higher — explicit closed-row / open-row logic | Lower — declarative intent |

**Recommendation:** Use `APPLY CHANGES INTO` (DLT) for all new Silver-layer SCD Type 2 pipelines. Use the Delta MERGE pattern only when the transformation must run in a Databricks Job (non-DLT) context, or when the DLT compute model does not fit the workload.

---

### Incremental Load Strategy Selection

| Strategy | When to Use | Notes |
|----------|------------|-------|
| Append-only (`watermark` on timestamp) | Source records are immutable once written; only new records arrive | Simplest; use with Auto Loader or Structured Streaming |
| MERGE incremental | Records can be updated at the source after initial delivery | Use Change Data Feed (CDF) or a `modified_at` watermark to identify changed records |
| Partition overwrite | Source delivers complete snapshots of a partition (e.g., all records for a specific date) | Use `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` to replace only affected partitions |

---

## Performance Tuning

### Which Optimization Should I Apply?

| Scenario | Recommended Action | Cookbook Section |
|----------|--------------------|-----------------|
| Table has accumulated many small files from frequent micro-batch or streaming writes | **OPTIMIZE (compaction)** | `performance/performance_cookbook.md` — Delta Lake Optimization — OPTIMIZE and Compaction |
| Queries filter on 1–3 high-cardinality columns and read performance is slow | **ZORDER** (in conjunction with OPTIMIZE) | `performance/performance_cookbook.md` — Delta Lake Optimization — ZORDER Clustering |
| Table access patterns change frequently or are unpredictable; ZORDER re-application overhead is high | **Liquid Clustering** | `performance/performance_cookbook.md` — Delta Lake Optimization — Liquid Clustering |
| Table is large and queries almost always filter on a low-cardinality column (e.g., date, region) | **Static partitioning** | `performance/performance_cookbook.md` — Partitioning Strategy |
| Old table snapshots are consuming excessive storage | **VACUUM** | `performance/performance_cookbook.md` — Delta Lake Optimization — VACUUM |
| Skewed data is causing slow shuffles; joins produce unexpected plan shapes | **Adaptive Query Execution (AQE)** | `performance/performance_cookbook.md` — Adaptive Query Execution (AQE) |
| Vectorised execution is needed for compute-heavy SQL workloads | **Enable Photon** | `performance/performance_cookbook.md` — Infrastructure Pre-Requisites — Enabling Photon on a Cluster |

---

### OPTIMIZE / ZORDER vs. Liquid Clustering vs. Static Partitioning

| Technique | Best Fit | Avoid When |
|-----------|---------|-----------|
| **OPTIMIZE + ZORDER** | Stable, well-understood filter columns; table is written infrequently; 1–3 clustering columns | More than 3 filter columns; write-heavy tables where re-OPTIMIZE cost is prohibitive |
| **Liquid Clustering** | Evolving or mixed access patterns; write-heavy tables; replacing an existing ZORDER table | Table must support ZORDER for compatibility with a legacy reader that inspects file statistics |
| **Static partitioning** | Single low-cardinality column (e.g., `event_date`) filtered in almost every query; very large tables | High-cardinality columns; tables with fewer than ~10 GB total data; columns that are rarely filtered |

**Recommendation:** For most new tables, start with **Liquid Clustering** — it requires no `OPTIMIZE` re-runs and adapts automatically. Only use static partitioning when partition pruning is the dominant read pattern and cardinality is guaranteed to remain low.

---

### VACUUM Retention Guidance

- Default retention is 7 days — do not reduce below 7 days in production unless you have explicitly disabled the safety check.
- Reducing retention removes the ability to time-travel to snapshots within the deleted range.
- For tables with high write frequency (streaming, micro-batch), consider increasing retention to 30 days to support operational debugging.
- Run `DESCRIBE HISTORY <table>` to inspect what versions are available before vacuuming.

---

## Security and Governance

### Which Security Pattern Should I Apply?

| Scenario | Recommended Pattern | Cookbook Section |
|----------|--------------------|-----------------|
| Control which teams or service principals can read, write, or manage tables | **RBAC with Unity Catalog** | `security/security_cookbook.md` — RBAC with Unity Catalog |
| Store and retrieve credentials, API keys, or connection strings securely | **Secrets Management** | `security/security_cookbook.md` — Secrets Management |
| Restrict which rows a user sees based on their group membership or attribute | **Row-Level Security (RLS) with Dynamic Views** | `security/security_cookbook.md` — Row-Level Security (RLS) with Dynamic Views |
| Mask PII or sensitive column values for non-privileged users while preserving query access | **Column Masking with Dynamic Views** | `security/security_cookbook.md` — Column Masking with Dynamic Views |
| Apply column masks natively without managing a separate view layer (Unity Catalog only) | **Unity Catalog Native Column Masks** | `security/security_cookbook.md` — Unity Catalog Native Column Masks |
| Tag tables and columns with classification labels for governance and discovery | **Unity Catalog Tag Management** | `security/security_cookbook.md` — Unity Catalog Tag Management |

---

### RLS: Dynamic Views vs. Unity Catalog Native Row Filters

| Factor | Dynamic Views | Unity Catalog Native Row Filters |
|--------|--------------|----------------------------------|
| Mechanism | `CREATE VIEW` with `CASE`/`WHERE` using `current_user()` or `is_account_group_member()` | `ALTER TABLE ... SET ROW FILTER` — filter applied at the table level |
| Requires view layer | Yes — consumers query the view, not the base table | No — consumers query the base table directly |
| Unity Catalog version required | Any | Databricks Runtime 12.2+ with Unity Catalog |
| Applies to external tools (PowerBI, Tableau via SQL Warehouse) | Consumers must reference the view | Yes — filter is enforced regardless of access path |
| Performance | View adds one logical layer; typically negligible | Filter predicate pushed into the scan |
| Best for | Backwards-compatible RLS on existing tables; workspaces not yet on UC row filters | New tables on a UC-enabled workspace where consumers connect via SQL Warehouse |

**Recommendation:** Prefer Unity Catalog native row filters for all new tables on a Unity Catalog-enabled workspace. Use dynamic views only when native row filters are unavailable or when consumers access data through a path that bypasses UC (e.g., direct cluster access without UC).

---

### Column Masking: Dynamic Views vs. Unity Catalog Native Column Masks

| Factor | Dynamic Views | Unity Catalog Native Column Masks |
|--------|--------------|----------------------------------|
| Mechanism | Expose selected columns through a view; PII columns replaced with `CASE` expressions | `ALTER TABLE ... ALTER COLUMN ... SET MASK` — mask applied at table level |
| Requires view layer | Yes | No |
| Applies across all access paths | Consumers must use the view | Yes — enforced at the engine level |
| Best for | Legacy environments; tables accessed by non-UC-aware consumers | New tables on a UC-enabled workspace (Runtime 12.2+) |

**Recommendation:** Mirror the recommendation for RLS — prefer UC native column masks on Unity Catalog-enabled workspaces. Use dynamic views for backward compatibility or mixed-access scenarios.

---

### Secret Scope Strategy

| Scenario | Recommended Approach |
|----------|---------------------|
| Credentials used by a specific pipeline or team | Use a Databricks secret scope with ACLs scoped to the service principal or group running the pipeline |
| Credentials shared across multiple pipelines | Store in a shared scope but grant read access only to the service principals that need it — do not use a single workspace-wide scope without ACLs |
| Credentials managed by Azure Key Vault | Use a Key Vault-backed secret scope to avoid duplicating secrets in Databricks |
| Credentials in notebooks for development | Use `dbutils.secrets.get()` — never hardcode credentials in notebook cells, even in development |

---

## Compute Selection

### Job Cluster vs. SQL Warehouse vs. DLT Pipeline Compute

| Workload | Recommended Compute | Notes |
|----------|---------------------|-------|
| PySpark transformation notebooks or jobs | **Job cluster** (Standard or Photon) | Scale-out distributed processing; use Photon clusters for heavy SQL workloads |
| Ad hoc SQL queries, dashboards, BI tool connections | **SQL Warehouse** (Serverless or Pro) | Serverless starts faster and scales to zero; Pro warehouses support concurrency better |
| Delta Live Tables pipelines | **DLT pipeline compute** (Enhanced Autoscaling) | Auto-scales based on pipeline load; managed by Databricks |
| Single-node processing (small data, dbt-style single-machine jobs) | **Single-node cluster** (`num_workers: 0`) | Use `spark.master: local[*, 4]`; avoids shuffle overhead for small datasets |
| Serverless DLT | **Serverless DLT** | No cluster configuration required; fastest startup; recommended for new DLT pipelines |

**Recommendation:** Default to SQL Warehouse (Serverless) for interactive SQL and BI. Default to Job clusters with Photon for PySpark batch jobs. Use DLT pipeline compute for all DLT pipelines. Avoid using interactive all-purpose clusters for production job runs.

---

## Orchestration

### Databricks Workflows vs. Delta Live Tables vs. Notebook-only Runs

| Pattern | When to Use |
|---------|------------|
| **Databricks Workflows (Jobs)** | Multi-task pipelines with dependencies, branching, or notification requirements; PySpark notebooks that are not DLT; scheduled batch processing |
| **Delta Live Tables** | Streaming or micro-batch pipelines requiring declarative quality enforcement, managed restarts, and auto-scaling; multi-hop Bronze → Silver → Gold within a single pipeline |
| **Notebook manual run** | Development and testing only — never use manual notebook runs for production data pipelines |
| **Asset Bundle (`databricks bundle deploy`)** | Infrastructure-as-code deployment of Jobs, DLT pipelines, and their cluster configurations; enables environment promotion (dev → staging → prod) |

**Recommendation:** All production pipelines should be deployed as Databricks Asset Bundles and triggered via Databricks Workflows. Use DLT for streaming and multi-hop pipelines. Use Jobs for everything else.

---

## Cross-Cutting Recommendations

### Development Workflow (Local → Databricks)

1. Develop and iterate in an interactive notebook on a Databricks cluster — use the notebook UI for faster feedback.
2. When logic is stable, extract to a Python file in a Databricks Asset Bundle project.
3. Deploy with `databricks bundle deploy --target dev` and run the job interactively.
4. Promote to production with `databricks bundle deploy --target prod`.

Never use interactive all-purpose clusters for scheduled production job runs — use job clusters or serverless compute with a defined schedule in Databricks Workflows.

### Change Data Feed (CDF)

Enable Change Data Feed on Delta tables when:

- A downstream pipeline needs to process only changed records efficiently (incremental MERGE strategy).
- A Structured Streaming pipeline reads change events (`readChangeFeed`) rather than full table scans.
- Audit requirements demand a record of which rows were inserted, updated, or deleted.

**Do not enable CDF by default on all tables** — it increases storage usage. Enable it deliberately on tables where downstream incremental processing justifies the overhead.

### Unity Catalog — Minimum Privilege for Pipeline Service Principals

Every pipeline service principal should have only:

- `USE CATALOG` on the target catalog
- `USE SCHEMA` on the target schema
- `CREATE TABLE` on the target schema
- `SELECT` on source schemas required for reading

Do not assign `ALL PRIVILEGES` or metastore admin rights to pipeline service principals. See `security/security_cookbook.md` — RBAC with Unity Catalog for the full grant pattern.

---

## Quick-Reference Index

| I want to... | Go to |
|-------------|-------|
| Ingest files from ADLS/S3 continuously | `ingestion/ingestion_cookbook.md` — Auto Loader |
| Ingest a historical file load | `ingestion/ingestion_cookbook.md` — COPY INTO |
| Ingest from a relational database | `ingestion/ingestion_cookbook.md` — JDBC |
| Ingest from Salesforce / Workday / SaaS | `ingestion/ingestion_cookbook.md` — Lakeflow Connect |
| Ingest from Kafka / Event Hubs | `ingestion/ingestion_cookbook.md` — Structured Streaming |
| Build a streaming pipeline with quality gates | `ingestion/ingestion_cookbook.md` — Delta Live Tables |
| Upsert records into a Delta table | `processing/processing_cookbook.md` — MERGE INTO |
| Track slowly changing attributes without history | `processing/processing_cookbook.md` — SCD Type 1 |
| Track slowly changing attributes with full history | `processing/processing_cookbook.md` — SCD Type 2 |
| Process only changed records since last run | `processing/processing_cookbook.md` — Incremental Load Patterns |
| Deduplicate source records | `processing/processing_cookbook.md` — Data Cleansing and Deduplication |
| Fix small file problems | `performance/performance_cookbook.md` — OPTIMIZE and Compaction |
| Speed up filtered queries on a large table | `performance/performance_cookbook.md` — ZORDER or Liquid Clustering |
| Control who can read which tables | `security/security_cookbook.md` — RBAC with Unity Catalog |
| Store connection credentials securely | `security/security_cookbook.md` — Secrets Management |
| Restrict which rows a user sees | `security/security_cookbook.md` — Row-Level Security |
| Mask PII columns for non-privileged users | `security/security_cookbook.md` — Column Masking |
| Understand ingestion architecture trade-offs | `ingestion/ingestion_patterns.md` |
| Understand processing architecture trade-offs | `processing/processing_patterns.md` |
| Understand performance architecture trade-offs | `performance/performance_patterns.md` |
| Understand security architecture trade-offs | `security/security_patterns.md` |
