# Cookbook Recommendations

This document organises recommended content across all major data architecture domains.
Content is split into two types:

- **Architectural Pattern** — A decision or design guide explaining *what*, *when*, and *why*.
  These are reference docs: trade-off analysis, layer structure, and when to choose one approach over another.
  They do not contain step-by-step implementation code.
- **Cookbook** — A step-by-step implementation guide explaining *how*.
  These are actionable docs: prerequisites, Python and SQL examples, validation steps, and references.

---

## Suggested File Structure

```
data_architecture/
├── ingestion/
│   ├── ingestion_patterns.md               ← architectural pattern
│   └── ingestion_cookbook.md               ← cookbook
├── processing/
│   ├── processing_patterns.md              ← architectural pattern
│   └── processing_cookbook.md              ← cookbook
├── performance/
│   ├── performance_patterns.md             ← architectural pattern
│   └── performance_cookbook.md             ← cookbook
└── security/
    ├── security_patterns.md                ← architectural pattern
    └── security_cookbook.md                ← cookbook
```

---

## 1. Ingestion

### Architectural Pattern — `ingestion/ingestion_patterns.md`

| Topic | Description |
|-------|-------------|
| Ingestion method selection | Decision guide: Auto Loader vs. COPY INTO vs. Structured Streaming vs. Lakeflow Spark Declarative Pipelines vs. Notebook pattern. When to use each based on latency, volume, and re-run safety requirements. |
| Batch vs. streaming trade-offs | Latency, cost, complexity, and failure recovery implications of each approach. |
| Schema evolution strategy | How each ingestion method handles schema changes and what the downstream Delta impact is. |
| Database Ingestion (JDBC) | Understand watermark strategies, parallelism, and push-down optimisation for JDBC sources. |
| Managed Ingestion | Understand the trade-offs between Lakeflow Connect and third-party connectors. |

### Cookbook — `ingestion/ingestion_cookbook.md`

#### File Ingestion
| Method | Description |
|--------|-------------|
| Auto Loader | Incrementally ingest files from cloud storage (ADLS, S3, GCS) using `cloudFiles` format. Includes checkpoint configuration, schema inference, and schema evolution handling. |
| COPY INTO | Idempotent, SQL-based batch ingestion from cloud storage into Delta tables. Covers idempotency guarantees and re-run behavior. |
| SFTP | Receive files from a partner or vendor system via SFTP. |

#### Streaming Ingestion
| Method | Description |
|--------|-------------|
| Structured Streaming | Micro-batch or continuous stream processing from Kafka, Event Hubs, or Kinesis into Delta Lake. |

#### Database Ingestion
| Method | Description |
|--------|-------------|
| JDBC | Extract from a relational database (SQL Server, PostgreSQL, MySQL, Oracle). |

#### Managed Ingestion
| Method | Description |
|--------|-------------|
| Lakeflow Connect | Ingest from SaaS applications (Salesforce, Workday, SQL Server) natively on Databricks. |
| Partner Connectors | Integrate Fivetran or Airbyte as the managed ingestion layer. |

#### Ad Hoc / Interactive Ingestion
| Method | Description |
|--------|-------------|
| Notebook Pattern | Manual or exploratory ingestion using `spark.read` / `spark.write`. Useful for one-time loads or prototyping. |

**Key differentiators to call out:**
- Auto Loader vs. COPY INTO: state tracking, schema evolution, and re-run behavior differences.

---

## 2. Processing, Summarizing, and Transformation

### Architectural Pattern — `processing/processing_patterns.md`

| Topic | Description |
|-------|-------------|
| Medallion architecture | Bronze → Silver → Gold layer responsibilities, data contracts between layers, and when to add or collapse layers. |
| Native Pipeline Layer Mapping | How PySpark notebooks, SDP pipelines, and Databricks Jobs map to Medallion layers. |
| PySpark vs. Spark SQL vs. SDP | Decision guide for choosing the transformation interface based on team skills, testability, and pipeline complexity. |

### Cookbook — `processing/processing_cookbook.md`

#### Lakeflow Spark Declarative Pipelines (SDP)
| Topic | Description |
|-------|-------------|
| Multi-Hop Pipeline | Declarative Bronze → Silver pipeline with `@dlt.table`, `@dlt.expect` quality rules, and managed cluster lifecycle. Covers Python decorator and SQL `CONSTRAINT ... EXPECT` syntax, pipeline modes (triggered vs. continuous), and managed table lifecycle. |

#### Native Databricks / PySpark Patterns
| Topic | Description |
|-------|-------------|
| Delta Lake Transformations | `MERGE INTO`, `UPDATE`, `DELETE` in both PySpark and SQL. Functional differences, not just syntax. |
| Aggregations and Summarization | `GROUP BY`, window functions (`OVER`, `PARTITION BY`), rollups, and cube. |
| Data Cleansing and Deduplication | Null handling, type casting, `dropDuplicates` (Python) vs. `ROW_NUMBER()` (SQL). |
| SCD Type 1 and Type 2 | Overwrite and history-tracking patterns using Delta `MERGE INTO` and SDP `APPLY CHANGES INTO`. |
| Joins and Enrichment | Broadcast joins, shuffle joins, and skew handling. |
| Incremental Load Patterns | Process only records changed since the last run (append, MERGE, or partition overwrite). |

**Key differentiators to call out:**
- `MERGE` vs. full overwrite vs. append: when each is appropriate and cost implications.
- Delta MERGE INTO vs. SDP `APPLY CHANGES INTO` for SCD Type 2: declarative vs. manual approach.

---

## 3. Performance Tuning

### Architectural Pattern — `performance/performance_patterns.md`

| Topic | Description |
|-------|-------------|
| Delta Lake storage optimization strategy | When to use `OPTIMIZE` + `ZORDER` vs. partitioning vs. liquid clustering. These are complementary but serve different access patterns and should not be applied blindly. |
| Cluster vs. SQL Warehouse selection | When to use a Spark cluster (ETL, PySpark, streaming) vs. Databricks SQL Warehouse (ad hoc queries, BI). Cost and performance implications. |
| AQE and Photon scope | What AQE covers automatically vs. what requires configuration; which operation types Photon accelerates vs. does not. |

### Cookbook — `performance/performance_cookbook.md`

#### Delta Lake Optimization
| Topic | Description |
|-------|-------------|
| OPTIMIZE and compaction | File compaction for small-file problems; when to run and how to schedule. |
| ZORDER clustering | Multi-dimensional clustering for range and equality filters; column selection guidance. |
| VACUUM | Cleaning up old Delta files; retention period trade-offs with time travel. |
| Liquid Clustering | Auto-adaptive clustering as an alternative to static partitioning and ZORDER. |
| Partitioning strategy | When and how to partition; avoiding over-partitioning. |

#### Query and Pipeline Optimization
| Topic | Description |
|-------|-------------|
| Adaptive Query Execution (AQE) | Configuration for partition coalescing, skew join optimization, and broadcast join conversion. |
| Photon Engine | Confirming Photon is active; eligible vs. non-eligible operations. |
| Caching | `spark.catalog.cacheTable`, `.cache()`, `.persist()` — when helpful vs. harmful. |
| Cluster sizing and autoscaling | Right-sizing for ETL vs. ad hoc; autoscaling configuration trade-offs. |
| Statistics and predicate pushdown | `ANALYZE TABLE`, column pruning, and avoiding full scans. |

---

## 4. Security (RBAC, RLS, Masking)

### Architectural Pattern — `security/security_patterns.md`

| Topic | Description |
|-------|-------------|
| Unity Catalog governance model | Catalog → Schema → Table hierarchy; admin roles vs. data steward roles vs. data consumer roles. When to use Unity Catalog native features vs. dynamic views. |
| Pipeline Service Principal Privilege Model | Minimum privilege set per pipeline layer scoped to Medallion layer boundaries. |
| Governance for multi-layer architectures | Applying RBAC, RLS, and masking consistently across Medallion layers, and where enforcement should live (Bronze vs. Gold boundary). |

### Cookbook — `security/security_cookbook.md`

#### Unity Catalog Access Control
| Topic | Description |
|-------|-------------|
| RBAC with Unity Catalog | `GRANT` / `REVOKE` on catalogs, schemas, tables, and views. Admin vs. data steward privilege sets. |
| Secrets Management | Databricks Secrets via CLI and Python to avoid hardcoded credentials in notebooks and jobs. |

#### Row-Level and Column-Level Security
| Topic | Description |
|-------|-------------|
| Row-Level Security (RLS) | Dynamic views filtering rows based on `current_user()` or group membership. |
| Column Masking | Masking PII using dynamic views or Unity Catalog native column masks. |
| Dynamic Views | Combining RLS and column masking in a single view. |

#### Audit, Encryption, and Metadata
| Topic | Description |
|-------|-------------|
| Audit Logging | Querying Databricks audit logs via `system.access.audit`. |
| Data Encryption | At-rest encryption defaults and customer-managed keys (CMK) for ADLS/S3. |
| Unity Catalog Tag Management | Tag tables and columns with classification labels for governance and data discovery. |

**Key differentiators to call out:**
- Unity Catalog RBAC vs. legacy table ACLs: Unity Catalog is the recommended path forward.
- Unity Catalog native RLS/masking vs. dynamic views: native features are simpler but require Unity Catalog Premium.
