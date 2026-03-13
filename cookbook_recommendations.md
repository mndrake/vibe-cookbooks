# Cookbook Recommendations

This document organizes recommended content across all major data architecture domains.
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
├── data_vault/
│   ├── dv2_architecture.md                 ← architectural pattern
│   ├── dv2_staging_cookbook.md             ← cookbook
│   ├── dv2_raw_vault_cookbook.md           ← cookbook
│   ├── dv2_business_vault_cookbook.md      ← cookbook
│   └── dv2_information_mart_cookbook.md    ← cookbook
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
| Ingestion method selection | Decision guide: Auto Loader vs. COPY INTO vs. Structured Streaming vs. DLT vs. Notebook pattern. When to use each based on latency, volume, and re-run safety requirements. |
| Batch vs. streaming trade-offs | Latency, cost, complexity, and failure recovery implications of each approach. |
| Schema evolution strategy | How each ingestion method handles schema changes and what the downstream Delta impact is. |
| Raw staging for Data Vault | How to design the staging layer when feeding a Data Vault 2.0 pipeline: hash key derivation, ghost records, and AutomateDV `stage` macro role. |
| dbt seeds as a reference ingestion pattern | When dbt seeds are appropriate (small, static reference data) vs. when full ingestion pipelines are needed. |

### Cookbook — `ingestion/ingestion_cookbook.md`

#### File Ingestion
| Method | Description |
|--------|-------------|
| Auto Loader | Incrementally ingest files from cloud storage (ADLS, S3, GCS) using `cloudFiles` format. Includes checkpoint configuration, schema inference, and schema evolution handling. |
| COPY INTO | Idempotent, SQL-based batch ingestion from cloud storage into Delta tables. Covers idempotency guarantees and re-run behavior. |

#### Streaming Ingestion
| Method | Description |
|--------|-------------|
| Structured Streaming | Micro-batch or continuous stream processing from Kafka, Event Hubs, or Kinesis into Delta Lake. |
| Delta Live Tables (DLT) | Declarative pipeline framework for reliable streaming and batch pipelines with built-in data quality expectations. |

#### Ad Hoc / Interactive Ingestion
| Method | Description |
|--------|-------------|
| Notebook Pattern | Manual or exploratory ingestion using `spark.read` / `spark.write`. Useful for one-time loads or prototyping. |
| REST API Ingestion | Pull data from external APIs using Python `requests` or `httpx`, then write to Delta. |

#### dbt-Specific Ingestion Patterns
| Method | Description |
|--------|-------------|
| dbt Seeds | Loading small CSV reference/lookup tables via `dbt seed`. Covers row count limits and when not to use seeds. |
| AutomateDV Stage Macro | Building the raw staging layer with hash key and hashdiff derivation before loading vault structures. Hash algorithm comparison: MD5 vs. SHA-256 on Databricks/Photon. |

**Key differentiators to call out:**
- Auto Loader vs. COPY INTO: state tracking, schema evolution, and re-run behavior differences.
- `dbt-utils.generate_surrogate_key` vs. AutomateDV native hashing: null handling, column concatenation order, and uppercase normalization.

---

## 2. Processing, Summarizing, and Transformation

### Architectural Pattern — `processing/processing_patterns.md`

| Topic | Description |
|-------|-------------|
| Medallion architecture | Bronze → Silver → Gold layer responsibilities, data contracts between layers, and when to add or collapse layers. |
| Medallion vs. Data Vault 2.0 | When to choose medallion (simpler, BI-focused) vs. Data Vault (auditability, multi-source integration, enterprise scale). Hybrid approaches. |
| dbt project layer mapping | How dbt staging, intermediate, and mart models map to medallion and Data Vault layers. |
| SCD vs. Satellite design | When a Data Vault Satellite is the right alternative to SCD Type 2, and the philosophical difference (append-only history vs. managed history). |
| PySpark vs. Spark SQL vs. dbt | Decision guide for choosing the transformation interface based on team skills, testability, and pipeline complexity. |

### Cookbook — `processing/processing_cookbook.md`

#### Native Databricks / PySpark Patterns
| Topic | Description |
|-------|-------------|
| Delta Lake Transformations | `MERGE INTO`, `UPDATE`, `DELETE` in both PySpark and SQL. Functional differences, not just syntax. |
| Aggregations and Summarization | `GROUP BY`, window functions (`OVER`, `PARTITION BY`), rollups, and cube. |
| Data Cleansing and Deduplication | Null handling, type casting, `dropDuplicates` (Python) vs. `ROW_NUMBER()` (SQL). |
| SCD Type 1 and Type 2 | Overwrite and history-tracking patterns using Delta `MERGE INTO`. |
| Joins and Enrichment | Broadcast joins, shuffle joins, and skew handling. |

#### dbt Patterns
| Topic | Description |
|-------|-------------|
| dbt Models on Databricks | Model materializations: `table`, `view`, `incremental`, `ephemeral`. Databricks SQL Warehouse vs. Spark cluster as execution target. |
| dbt Incremental Strategies | `append`, `merge`, `insert_overwrite` — how each maps to Delta operations and when to use each. |
| dbt Snapshots for SCD Type 2 | Low-code SCD Type 2 alternative to manual `MERGE INTO`. |
| dbt Macros and dbt-utils | Reusable logic with macros; `pivot`/`unpivot`, `date_spine`, `get_column_values` from dbt-utils for dynamic models. |
| dbt Testing with dbt-utils | `unique_combination_of_columns`, `expression_is_true`, `accepted_range` for data quality at each layer. |

**Key differentiators to call out:**
- `MERGE` vs. full overwrite vs. append: when each is appropriate and cost implications.
- dbt incremental `merge` strategy vs. raw Delta `MERGE INTO`: what AutomateDV handles automatically vs. what requires custom SQL.
- dbt `ref()` DAG vs. manual join chains: readability and dependency management trade-offs.

---

## 3. Data Vault 2.0

### Architectural Pattern — `data_vault/dv2_architecture.md`

| Topic | Description |
|-------|-------------|
| Data Vault 2.0 overview | Core concepts: Hubs, Links, Satellites, Reference tables, Business Vault, and Information Marts. Design philosophy: auditability, parallelism, append-only loading. |
| When to use Data Vault 2.0 | Decision guide: scale thresholds, auditability requirements, multi-source integration complexity, and team maturity. When medallion is sufficient. |
| AutomateDV + dbt-databricks setup | `packages.yml` configuration, version pinning, compatibility matrix between AutomateDV and dbt-databricks versions. |
| Layer responsibilities | Raw Vault (structural, no business rules) vs. Business Vault (derived rules, computed columns) vs. Information Mart (star schema or flat tables for BI). |
| Hash key design | Hash algorithm selection (MD5 vs. SHA-256), column ordering conventions, null substitution, and uppercase normalization rules. Consistency requirements across sources. |
| Unity Catalog governance alignment | How vault layers map to Unity Catalog schemas; tagging hubs/links as structural vs. satellites as sensitive/PII. |

### Cookbook — `data_vault/dv2_staging_cookbook.md`

| Topic | Description |
|-------|-------------|
| AutomateDV `stage` macro | Building staging models: hash key derivation, hashdiff computation, ghost/default record injection. |
| Hashdiff column design | Which columns to include, ordering conventions, and performance cost of wide-table hashdiff computation on Databricks. |
| dbt-utils `generate_surrogate_key` | When to use alongside or instead of AutomateDV hashing. |

### Cookbook — `data_vault/dv2_raw_vault_cookbook.md`

| Vault Structure | AutomateDV Macro | Description |
|-----------------|------------------|-------------|
| Hubs | `hub` | Loading unique business keys; idempotency; multi-source hub loading from a single model. |
| Links | `link` | Capturing relationships between business keys; grain definition. |
| Non-Historized Links | `nh_link` | Immutable relationship records with no effectivity tracking. |
| Satellites | `sat` | Descriptive context with hashdiff-based change detection to prevent duplicate records. |
| Multi-Active Satellites | `ma_sat` | Multiple simultaneously active records of the same type (e.g., multiple phone numbers). |
| Effectivity Satellites | `eff_sat` | Tracking link open/close dates for relationship lifecycle. |
| Transactional Links | `t_link` | Immutable, fact-style event/transaction records. |
| Reference Structures | `ref_hub`, `ref_sat` | Vault-style handling of static reference and lookup data. |

### Cookbook — `data_vault/dv2_business_vault_cookbook.md`

| Topic | Description |
|-------|-------------|
| Derived business rules | Adding computed columns and soft business rules on top of raw vault without modifying raw structures. |
| Point-in-Time (PIT) tables | `pit` macro: pre-joining satellite snapshots at a given point in time to avoid expensive multi-satellite joins at query time. When to materialize as a table and refresh frequency. |
| Bridge tables | `bridge` macro: spanning multiple links for mart-layer query performance. |
| dbt-utils for Business Vault | `expression_is_true` for business rule validation; `unique_combination_of_columns` for grain checks. |

### Cookbook — `data_vault/dv2_information_mart_cookbook.md`

| Topic | Description |
|-------|-------------|
| Star schema from vault | Building dimension and fact tables from PIT and Bridge structures. |
| Flat wide tables | Denormalized tables for BI tools that struggle with multi-join vault queries. |
| dbt-utils `pivot` / `unpivot` | Transforming EAV-style satellite structures into columnar mart tables. |
| dbt-utils `date_spine` | Generating date grain for PIT table snapshots and time-series marts. |

---

## 4. Performance Tuning

### Architectural Pattern — `performance/performance_patterns.md`

| Topic | Description |
|-------|-------------|
| Delta Lake storage optimization strategy | When to use `OPTIMIZE` + `ZORDER` vs. partitioning vs. liquid clustering. These are complementary but serve different access patterns and should not be applied blindly. |
| Cluster vs. SQL Warehouse selection | When to use a Spark cluster (ETL, PySpark, streaming) vs. Databricks SQL Warehouse (ad hoc queries, dbt, BI). Cost and performance implications. |
| Vault-specific performance patterns | Why PIT and Bridge tables exist as performance structures in Data Vault 2.0 and when they justify their maintenance overhead. |
| AQE and Photon scope | What AQE covers automatically vs. what requires configuration; which operation types Photon accelerates vs. does not. |

### Cookbook — `performance/performance_cookbook.md`

#### Delta Lake Optimization
| Topic | Description |
|-------|-------------|
| OPTIMIZE and compaction | File compaction for small-file problems; when to run and how to schedule. |
| ZORDER clustering | Multi-dimensional clustering for range and equality filters; column selection guidance. |
| VACUUM | Cleaning up old Delta files; retention period trade-offs with time travel. |
| Liquid Clustering | Auto-adaptive clustering as an alternative to static partitioning and ZORDER. |
| Partitioning strategy | When and how to partition; avoiding over-partitioning; satellite table partitioning by `load_date`. |

#### Query and Pipeline Optimization
| Topic | Description |
|-------|-------------|
| Adaptive Query Execution (AQE) | Configuration for partition coalescing, skew join optimization, and broadcast join conversion. |
| Photon Engine | Confirming Photon is active; eligible vs. non-eligible operations; Photon and hash key generation performance. |
| Caching | `spark.catalog.cacheTable`, `.cache()`, `.persist()` — when helpful vs. harmful. |
| Cluster sizing and autoscaling | Right-sizing for ETL vs. ad hoc; autoscaling configuration trade-offs. |
| Statistics and predicate pushdown | `ANALYZE TABLE`, column pruning, and avoiding full scans. |

#### dbt-Specific Performance
| Topic | Description |
|-------|-------------|
| dbt model materialization strategy | When `incremental` vs. `table` vs. `view` affects Databricks job cost and runtime. |
| AutomateDV incremental loading | How append-only vault structures map to dbt-databricks incremental strategies; `insert_overwrite` for satellites, `merge` with `unique_key` for hubs and links. |
| Hashdiff computation cost | Performance impact of hashing wide satellite tables on every staging run; column selection to minimize cost. |
| dbt compiled SQL inspection | Using `target/compiled/` to audit generated SQL and identify optimization opportunities before running. |

---

## 5. Security (RBAC, RLS, Masking)

### Architectural Pattern — `security/security_patterns.md`

| Topic | Description |
|-------|-------------|
| Unity Catalog governance model | Catalog → Schema → Table hierarchy; admin roles vs. data steward roles vs. data consumer roles. When to use Unity Catalog native features vs. dynamic views. |
| Data Vault governance alignment | Hubs and links as broadly accessible structural assets vs. satellites as the natural boundary for sensitive data governance. RLS and masking at the satellite level rather than on wide tables. |
| dbt service principal privilege model | Minimum privilege set per pipeline layer: `CREATE TABLE` on vault schemas, `SELECT` on staging schemas. How to scope service principal permissions to match Data Vault layer boundaries. |
| Governance for multi-layer architectures | Applying RBAC, RLS, and masking consistently across medallion or Data Vault layers, and where enforcement should live (source layer vs. mart layer). |

### Cookbook — `security/security_cookbook.md`

#### Unity Catalog Access Control
| Topic | Description |
|-------|-------------|
| RBAC with Unity Catalog | `GRANT` / `REVOKE` on catalogs, schemas, tables, and views. Admin vs. data steward privilege sets. |
| dbt Grants block | Declarative post-model `GRANT` statements via dbt `grants` config; replacing manual grant scripts. |
| Secrets Management | Databricks Secrets via CLI and Python to avoid hardcoded credentials in notebooks and jobs. |

#### Row-Level and Column-Level Security
| Topic | Description |
|-------|-------------|
| Row-Level Security (RLS) | Dynamic views filtering rows based on `current_user()` or group membership. |
| Column Masking | Masking PII using dynamic views or Unity Catalog native column masks. |
| Dynamic Views | Combining RLS and column masking in a single view; designing for satellite-level application in Data Vault. |
| dbt models as RLS wrappers | Generating dynamic views with embedded RLS logic as dbt models. |

#### Audit, Encryption, and Metadata
| Topic | Description |
|-------|-------------|
| Audit Logging | Querying Databricks audit logs via `system.access.audit`; dbt query history in Databricks SQL by service principal. |
| Data Encryption | At-rest encryption defaults and customer-managed keys (CMK) for ADLS/S3. |
| Column classification with dbt | Using `schema.yml` meta tags to document PII and sensitive columns; feeding Unity Catalog tagging workflows. |

**Key differentiators to call out:**
- Unity Catalog RBAC vs. legacy table ACLs: Unity Catalog is the recommended path forward.
- Unity Catalog native RLS/masking vs. dynamic views: native features are simpler but require Unity Catalog Premium.
- Satellite-level security in Data Vault vs. column-level security on wide tables: vault structure naturally reduces masking surface area.
