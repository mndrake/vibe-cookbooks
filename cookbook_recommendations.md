# Cookbook Recommendations

This document outlines recommended cookbooks for each major data architecture domain. Each entry describes the cookbook's focus, suggested methods to cover, and the file path where it should live.

---

## 1. Ingestion

**File:** `data_architecture/ingestion/ingestion_cookbook.md`

### File Ingestion
| Method | Description |
|--------|-------------|
| Auto Loader | Incrementally ingest files from cloud storage (ADLS, S3, GCS) using `cloudFiles` format. Ideal for continuous or scheduled batch pipelines. |
| COPY INTO | Idempotent, SQL-based batch ingestion from cloud storage into Delta tables. Best for ad hoc or scheduled loads where re-runs must be safe. |

### Streaming Ingestion
| Method | Description |
|--------|-------------|
| Structured Streaming | Micro-batch or continuous stream processing from Kafka, Event Hubs, or Kinesis into Delta Lake. |
| Delta Live Tables (DLT) | Declarative pipeline framework for building reliable streaming and batch pipelines with built-in data quality checks. |

### Ad Hoc / Interactive Ingestion
| Method | Description |
|--------|-------------|
| Notebook Pattern | Manual or exploratory ingestion using `spark.read` / `spark.write` directly in a notebook. Useful for one-time loads or prototyping. |
| REST API Ingestion | Pull data from external APIs using Python `requests` or `httpx`, then write to Delta. |

**Key differentiators to highlight:**
- Auto Loader vs. COPY INTO: Auto Loader tracks state automatically; COPY INTO is simpler but requires idempotent sources.
- Streaming vs. batch latency trade-offs.
- Schema evolution handling across methods.

---

## 2. Processing, Summarizing, and Transformation

**File:** `data_architecture/processing/processing_cookbook.md`

### Recommended Cookbooks

| Topic | Description |
|-------|-------------|
| Delta Lake Transformations | Read, transform, and overwrite Delta tables using merge, update, and delete operations. Cover both Python (PySpark) and SQL. |
| Aggregations and Summarization | Common patterns: `GROUP BY`, window functions (`OVER`, `PARTITION BY`), rollups, and cube. |
| Data Cleansing and Deduplication | Handling nulls, type casting, deduplication with `dropDuplicates` (Python) and `ROW_NUMBER()` (SQL). |
| Slowly Changing Dimensions (SCD) | SCD Type 1 (overwrite) and Type 2 (history tracking) using Delta `MERGE INTO`. |
| ETL Pipeline Patterns | Medallion architecture (Bronze → Silver → Gold): raw ingestion, cleansing, and aggregation layers. |
| Joins and Enrichment | Broadcast joins for small lookup tables, shuffle joins for large datasets, and skew handling strategies. |

**Key differentiators to highlight:**
- PySpark DataFrame API vs. Spark SQL for each transformation pattern.
- When to use `MERGE` vs. full overwrite vs. append.
- Medallion layer responsibilities and data contracts.

---

## 3. Performance Tuning

**File:** `data_architecture/performance/performance_cookbook.md`

### Recommended Cookbooks

| Topic | Description |
|-------|-------------|
| Delta Lake Optimization | `OPTIMIZE` for compaction, `ZORDER BY` for multi-dimensional clustering, `VACUUM` for storage cleanup. |
| Partitioning Strategy | When and how to partition Delta tables; avoiding over-partitioning pitfalls. |
| Caching | `spark.catalog.cacheTable` and `.cache()` / `.persist()` — when they help and when they hurt. |
| Adaptive Query Execution (AQE) | Enable and configure AQE for dynamic partition coalescing, skew join optimization, and converting sort-merge joins to broadcast joins. |
| Photon Engine | When Photon accelerates workloads and how to confirm it is active. |
| Cluster Sizing and Autoscaling | Right-sizing clusters for ETL vs. ad hoc queries; autoscaling configuration trade-offs. |
| Query Optimization | Predicate pushdown, column pruning, statistics collection (`ANALYZE TABLE`), and avoiding full scans. |

**Key differentiators to highlight:**
- `ZORDER` vs. partitioning: complementary but serve different access patterns.
- AQE settings that are on by default vs. those requiring explicit configuration.
- Photon-eligible vs. non-eligible operations.

---

## 4. Security (RBAC, RLS, Masking)

**File:** `data_architecture/security/security_cookbook.md`

### Recommended Cookbooks

| Topic | Description |
|-------|-------------|
| Unity Catalog RBAC | Granting and revoking privileges on catalogs, schemas, tables, and views using `GRANT` / `REVOKE`. Covers admin roles vs. data steward roles. |
| Row-Level Security (RLS) | Implementing RLS using dynamic views that filter rows based on `current_user()` or group membership. |
| Column Masking | Masking PII and sensitive columns using dynamic views or Unity Catalog column masks (SQL and Python). |
| Dynamic Views | Building views that adapt output based on the calling user's identity or group — combines RLS and column masking. |
| Data Encryption | At-rest encryption defaults and customer-managed keys (CMK) for ADLS/S3 integration. |
| Audit Logging | Enabling and querying Databricks audit logs via Unity Catalog system tables (`system.access.audit`). |
| Secrets Management | Using Databricks Secrets (CLI + Python) to avoid hardcoding credentials in notebooks and jobs. |

**Key differentiators to highlight:**
- Unity Catalog RBAC vs. legacy table ACLs: Unity Catalog is the recommended path forward.
- Dynamic views vs. Unity Catalog native RLS/masking: native features are simpler but require Unity Catalog.
- `current_user()` vs. group-based filtering trade-offs.

---

## Suggested File Structure

```
data_architecture/
├── ingestion/
│   └── ingestion_cookbook.md
├── processing/
│   └── processing_cookbook.md
├── performance/
│   └── performance_cookbook.md
└── security/
    └── security_cookbook.md
```
