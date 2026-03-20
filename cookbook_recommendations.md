# Cookbook Recommendations

This document organises recommended content across all major data architecture domains.
Each domain has a single self-contained cookbook (`*_cookbook.md`) that covers both
method selection (decision tables) and step-by-step implementation.

---

## File Structure

```
data_architecture/
├── cookbook_template.md
├── cookbook_recommendations.md
├── ingestion/
│   └── ingestion_cookbook.md
├── processing/
│   └── processing_cookbook.md
├── performance/
│   └── performance_cookbook.md
└── security/
    └── security_cookbook.md
```

---

## 1. Ingestion — `ingestion/ingestion_cookbook.md`

### Method Selection

| Topic | Description |
|-------|-------------|
| Decision Criteria | Best For / Avoid When comparison across Auto Loader, COPY INTO, Structured Streaming, JDBC, and Lakeflow Connect. |
| Schema Evolution Behaviour | How each method handles new or changed columns and what happens without explicit configuration. |
| Batch vs. Streaming | Latency, cost, failure recovery, and operational complexity comparison. |

### File Ingestion

| Method | Description |
|--------|-------------|
| Auto Loader | Incrementally ingest files from ADLS Gen2 using `cloudFiles` format with checkpoint-based state tracking and configurable schema evolution. |
| COPY INTO | Idempotent, SQL-based batch ingestion from a cloud storage path into a Delta table. |

### Streaming Ingestion

| Method | Description |
|--------|-------------|
| Structured Streaming | Micro-batch or continuous ingestion from Azure Event Hubs or Kafka. Covers consumer groups, checkpointing, and Event Hubs Capture for backfill. When sub-minute latency is not required, a Kafka Connector landing files in ADLS Gen2 with Auto Loader or COPY INTO is an alternative. |

### Database Ingestion

| Method | Description |
|--------|-------------|
| JDBC | Watermark-based incremental extract from relational databases (SQL Server, PostgreSQL, MySQL, Oracle) with partition parallelism. |

### Managed Ingestion

| Method | Description |
|--------|-------------|
| Lakeflow Connect | Native Databricks connector for SaaS sources (Salesforce, Workday, ServiceNow, SQL Server, Google Analytics). GA as of March 2026. |

---

## 2. Processing, Summarizing, and Transformation — `processing/processing_cookbook.md`

### Design Decisions

| Topic | Description |
|-------|-------------|
| Medallion Layer Responsibilities | Bronze → Silver → Gold layer ownership, data contracts, and what each layer should and should not do. |
| Orchestration: Databricks Jobs vs. SDP | When to use a Databricks Job pipeline vs. Lakeflow Spark Declarative Pipelines based on complexity, quality enforcement, and operational needs. |
| Transformation Tool: PySpark vs. Spark SQL vs. SDP | Decision guide for choosing the transformation interface by team skills, testability, and pipeline complexity. |

### Transformations

| Topic | Description |
|-------|-------------|
| Delta Lake Transformations — MERGE INTO | Upsert records into a Delta table based on a business key. |
| Delta Lake Transformations — UPDATE and DELETE | Correct or remove records already written to a Delta table. |
| Aggregations and Summarization | `GROUP BY`, window functions (`OVER`, `PARTITION BY`), rollups, and cube. |
| Data Cleansing and Deduplication | Null handling, type casting, `dropDuplicates` (Python) vs. `ROW_NUMBER()` (SQL). |
| Joins and Enrichment | Broadcast joins, shuffle joins, and skew handling. |

### SCD and Incremental Patterns

| Topic | Description |
|-------|-------------|
| SCD Type 1 | Overwrite a slowly changing attribute with no history retained. |
| SCD Type 2 (Native Delta MERGE) | Track full history of attribute changes using a two-pass MERGE pattern. |
| SCD Type 2 via SDP APPLY CHANGES INTO | Track history declaratively in an SDP pipeline. |
| Incremental Load Patterns — Databricks Jobs | Process only records changed since the last run (append, MERGE, or partition overwrite). |

### Reuse and Orchestration

| Topic | Description |
|-------|-------------|
| Reusable Transformation Logic | Native Python functions and Spark SQL UDFs shared across notebooks and jobs. |
| Date Spine Generation | Generate a complete date sequence in native Spark for time-series joins and gap-filling. |
| Lakeflow Spark Declarative Pipelines — Multi-Hop Pipeline | Declarative Bronze → Silver pipeline with `@dlt.table`, `@dlt.expect` quality rules, and managed cluster lifecycle. |
| Pipeline Orchestration — Databricks Jobs and Asset Bundles | Define and deploy multi-task Databricks Jobs using Asset Bundles (YAML). |

**Key differentiators:**
- `MERGE` vs. full overwrite vs. append: when each is appropriate and cost implications.
- Delta MERGE INTO vs. SDP `APPLY CHANGES INTO` for SCD Type 2: manual vs. declarative approach.

---

## 3. Performance Tuning — `performance/performance_cookbook.md`

### Design Decisions

| Topic | Description |
|-------|-------------|
| Delta Lake Storage Optimization | When to use OPTIMIZE + ZORDER vs. Liquid Clustering vs. static partitioning — these are complementary but serve different access patterns. |
| Compute Selection: Spark Cluster vs. SQL Warehouse | When to use a Job cluster vs. an all-purpose cluster vs. a SQL Warehouse for ETL, streaming, and ad hoc workloads. |
| AQE Configuration Reference | What AQE handles automatically vs. what still requires manual configuration. |
| Photon Eligibility | Which operation types Photon accelerates and which it does not. |

### Delta Lake Optimization

| Topic | Description |
|-------|-------------|
| OPTIMIZE and Compaction | File compaction for small-file accumulation; when to run and how to schedule. |
| ZORDER Clustering | Multi-dimensional clustering for range and equality filters; column selection guidance. |
| Liquid Clustering | Auto-adaptive clustering as an alternative to static partitioning and ZORDER. |
| VACUUM | Reclaim storage from old Delta snapshots; retention period trade-offs with time travel. |
| Partitioning Strategy | When and how to partition; avoiding over-partitioning on high-cardinality columns. |

### Query and Pipeline Optimization

| Topic | Description |
|-------|-------------|
| Adaptive Query Execution (AQE) | Partition coalescing, skew join optimisation, and broadcast join conversion. |
| Photon Engine | Confirming Photon is active; eligible vs. non-eligible operations. |
| Caching | `spark.catalog.cacheTable`, `.cache()`, `.persist()` — when helpful vs. harmful. |
| Cluster Sizing and Autoscaling | Right-sizing for ETL vs. ad hoc; autoscaling configuration trade-offs. |
| Statistics and Predicate Pushdown | `ANALYZE TABLE`, column pruning, and avoiding full scans. |
| Native Incremental Loading with DeltaTable MERGE | Incremental MERGE pattern optimised for Delta performance. |
| Lakeflow SDP Pipeline Performance | Tuning SDP pipeline cluster size, pipeline mode, and checkpoint configuration. |

---

## 4. Security (RBAC, RLS, Masking) — `security/security_cookbook.md`

### Design Decisions

| Topic | Description |
|-------|-------------|
| Unity Catalog Native Features vs. Dynamic Views | When to use UC-native row filters and column masks vs. dynamic views; feature requirements and trade-offs. |
| Role Reference | Admin, data steward, and data consumer privilege sets scoped to the Unity Catalog hierarchy. |
| Pipeline Service Principal Privilege Model | Minimum privilege set per pipeline layer scoped to Medallion layer boundaries. |
| Security Enforcement by Medallion Layer | Where to enforce RBAC, RLS, and masking in a Bronze → Silver → Gold pipeline. |

### Access Control

| Topic | Description |
|-------|-------------|
| RBAC with Unity Catalog | `GRANT` / `REVOKE` on catalogs, schemas, tables, and views for users and service principals. |
| Secrets Management | Store and retrieve credentials using Databricks Secrets (`dbutils.secrets`) to avoid hardcoded credentials in notebooks and jobs. |

### Row-Level and Column-Level Security

| Topic | Description |
|-------|-------------|
| Row-Level Security (RLS) with Dynamic Views | Filter rows based on `current_user()` or group membership via a view layer. |
| Column Masking with Dynamic Views | Mask PII or sensitive column values for non-privileged users via a view layer. |
| Unity Catalog Native Column Masks | Apply column-level masking directly on the base table without a separate view (UC Premium required). |

### Audit and Governance

| Topic | Description |
|-------|-------------|
| Unity Catalog Tag Management | Tag tables and columns with classification labels for governance and data discovery. |
| Audit Logging | Query Databricks audit logs via `system.access.audit`. |
| Data Encryption | At-rest encryption defaults and customer-managed keys (CMK) for ADLS Gen2. |

**Key differentiators:**
- UC-native RLS/masking vs. dynamic views: native features are simpler to manage but require Unity Catalog Premium.
