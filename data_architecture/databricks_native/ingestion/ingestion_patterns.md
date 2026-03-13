# Ingestion Architectural Patterns
## Databricks Native Stack

> **Scope:** This document covers ingestion using Databricks platform features only — Auto Loader, COPY INTO, Structured Streaming, Delta Live Tables, JDBC, SFTP connector, Lakeflow Connect, and Databricks workflows. It does not cover dbt, AutomateDV, or any external orchestration tooling. For the equivalent guide covering the Databricks + dbt + AutomateDV stack, see `../databricks_and_dbt/ingestion/ingestion_patterns.md`.

---

## Overview

This document describes the architectural patterns and design decisions that govern data ingestion on Databricks using the native Databricks toolchain. It is a decision and design reference, not a step-by-step implementation guide. Step-by-step examples for each method are found in `ingestion_cookbook.md` in the same directory.

This document is intended for data architects, senior data engineers, and technical leads who are selecting or reviewing ingestion patterns for a Databricks-based data platform. It covers method selection criteria, batch versus streaming trade-offs, schema evolution strategies, raw staging for Data Vault 2.0 using native PySpark and SQL, and native reference data loading patterns using COPY INTO and Delta Lake.

---

## Ingestion Method Selection

### Overview

Databricks supports multiple ingestion methods, each suited to different latency requirements, operational models, and data volumes. Choosing the wrong method for a use case leads to avoidable operational overhead, unnecessary cost, or incorrect data. This section provides a structured comparison to guide that selection.

### Decision Criteria

| Method | Best For | Avoid When |
|--------|----------|------------|
| **Auto Loader** | Continuous or scheduled file arrival in cloud storage (ADLS, S3, GCS); large file volumes where checkpoint-based state tracking is important; tables that require schema evolution over time | You need sub-minute event-level latency from a message bus; files are delivered once via a one-off process |
| **COPY INTO** | Scheduled batch loads from a known cloud storage path; scenarios where idempotency is critical and re-runs must not create duplicates; simple batch pipelines without schema evolution needs | You need schema to auto-evolve as new columns arrive; you need automatic state management without a checkpoint directory |
| **Structured Streaming** | Sub-minute latency ingestion from Kafka, Azure Event Hubs, or Kinesis; event-driven architectures where consumer lag must be minimised; stateful aggregations with watermarking | The source is cloud storage files rather than a message bus; your team lacks the operational capability to manage streaming job recovery |
| **Delta Live Tables (DLT)** | Managed declarative pipelines where simplicity and built-in data quality are the priority; teams that want automated retry, lineage, and observability without writing custom orchestration logic | You need fine-grained control over trigger timing or compute configuration that DLT's managed runtime does not expose; budget is constrained (DLT incurs a DBU premium) |
| **Notebook Pattern** | One-off or exploratory data loads during development or investigation; historical backfills run once by a human | Any recurring production load; any scenario where re-run safety or auditability is required |
| **JDBC** | Ingesting data directly from relational databases (SQL Server, PostgreSQL, MySQL, Oracle) where cloud storage is not the source; incremental or full extract from OLTP systems | Source data volumes are very large and partition-based parallelism cannot be applied; real-time latency requirements (JDBC is a batch-pull mechanism) |
| **SFTP (Native Connector)** | Receiving files from partner or vendor systems that deliver via SFTP; organisations that want a managed connector without custom Python scripting | ⚠️ **Public preview as of March 2026** — not recommended for critical production workloads without validating preview stability; not suitable where the source SFTP server has connectivity restrictions incompatible with Databricks-managed egress |
| **Lakeflow Connect** | Managed ingestion from SaaS applications (Salesforce, Workday, ServiceNow, Google Analytics) and databases where building a custom connector is not justified; teams that want a fully native Databricks-managed pipeline with Unity Catalog governance and serverless compute | Sources not yet on the Lakeflow Connect connector catalogue; organisations with strict data residency requirements that need careful evaluation of data paths |
| **Partner Connectors (Fivetran, Airbyte)** | SaaS sources not yet covered by Lakeflow Connect; organisations already invested in a specific connector platform | Sources supported natively by Lakeflow Connect, where consolidating on the Databricks-native toolchain is preferred |

### Trade-offs

Auto Loader and Structured Streaming both use Spark's checkpoint mechanism, which provides exactly-once guarantees by tracking which files or offsets have been processed. This is powerful but requires that the checkpoint directory be durable (on cloud storage, not ephemeral local storage), and that it is never deleted unless you intend to reprocess from the beginning. COPY INTO tracks processed files inside the Delta table's transaction log, making it simpler to reason about — no external checkpoint directory is needed — but this also means that COPY INTO's tracking is tied to that specific Delta table and cannot be reused if the table is recreated.

Delta Live Tables sits above all of these methods in the abstraction stack. It manages checkpointing, retries, and cluster lifecycle automatically. The cost of that abstraction is reduced control: DLT pipelines run on DLT-managed clusters with fixed configuration options, and trigger timing is controlled by the pipeline's continuous or triggered mode rather than by arbitrary cron logic.

The Notebook Pattern has no place in production recurring ingestion. It has no state tracking, no deduplication guarantee, and no audit trail beyond the notebook run history.

Lakeflow Connect is the preferred choice for new SaaS ingestion implementations on the native Databricks stack — it runs entirely within Databricks infrastructure and integrates with Unity Catalog lineage and access control without requiring a third-party service.

### See Also

- [Auto Loader documentation — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)
- [COPY INTO documentation — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)
- [Delta Live Tables overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/)
- [Structured Streaming — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/)
- [Lakeflow Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- [SFTP ingestion — Azure Databricks (public preview)](https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp)
- `ingestion_cookbook.md` — step-by-step implementation for each method

---

## Batch vs. Streaming Trade-offs

### Overview

The choice between batch and streaming ingestion is one of the most consequential architectural decisions in a data platform. It affects latency, cost, operational complexity, and failure recovery behaviour.

### Decision Criteria

| Dimension | Batch | Micro-Batch (Structured Streaming with `trigger(availableNow=True)`) | Continuous Streaming |
|-----------|-------|----------------------------------------------------------------------|----------------------|
| **Latency** | Minutes to hours | Minutes, depending on trigger interval and cluster startup time | Seconds to sub-minute |
| **Cost model** | Job cluster per run; cost proportional to run frequency and duration | Job cluster per trigger cycle; startup overhead matters for short cycles | Always-on cluster; cost is continuous regardless of data volume |
| **Failure recovery** | Re-run from the start of the batch window | Resume from checkpoint | Resume from last committed offset; Kafka retention must be sufficient |
| **Operational complexity** | Low | Medium — checkpoint directory must be managed | High — consumer lag, watermarking, stateful operations require monitoring |
| **Deduplication** | Simpler — batch boundaries are explicit | Requires watermarking or deduplication logic | Requires watermarking and careful stateful design |
| **Downstream freshness** | Stale between runs | Near-real-time | Real-time or near-real-time |

### Trade-offs

Micro-batch mode (`trigger(availableNow=True)`) provides exactly-once guarantees via checkpoint while running on a job cluster that terminates after each cycle — significantly cheaper than a continuously running stream and more reliable than pure batch. For most enterprise use cases with latency requirements of five to thirty minutes, micro-batch is the optimal choice.

Continuous streaming should only be chosen when the business genuinely demands sub-minute freshness and the organisation is prepared to invest in the operational tooling to support it.

### See Also

- [Structured Streaming trigger types — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers)
- [Auto Loader availableNow trigger — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/production)

---

## Schema Evolution Strategy

### Overview

Schema evolution is the process by which a data pipeline handles changes to the structure of incoming data. Handling it incorrectly leads to pipeline failures, silently dropped columns, or corrupt downstream tables. Each ingestion method has fundamentally different behaviour when schema changes occur.

### Per-Method Behaviour

| Method | Schema Evolution Support | Behaviour on Schema Change |
|--------|--------------------------|---------------------------|
| **Auto Loader** | Yes — configurable via `cloudFiles.schemaEvolutionMode` | `addNewColumns`: new columns added automatically. `rescue`: unexpected columns captured in `_rescued_data`. `failOnNewColumns`: pipeline fails on new column (useful for controlled environments). `none`: new columns silently dropped. |
| **COPY INTO** | No | Columns not in the target schema are silently dropped. Missing columns are written as `null`. No automatic schema evolution. |
| **Structured Streaming** | Limited | Unknown columns dropped by default. Schema changes after stream start cause failure unless the checkpoint is deleted and the stream restarted. |
| **Delta Live Tables** | Yes | DLT automatically adds new columns to managed tables. Quality expectations are evaluated after schema evolution. |
| **Notebook Pattern** | Manual | Schema must be updated manually in the notebook before the next run. |
| **JDBC** | No automatic evolution | New source columns require a manual `ALTER TABLE ... ADD COLUMN` on the Delta target, or `mergeSchema` enabled on the write. |
| **SFTP (Native Connector)** | Depends on format | Follows the underlying file format reader. JSON with compatible `schemaEvolutionMode` options behaves similarly to Auto Loader. CSV with inferred schema may fail on new columns. |
| **Lakeflow Connect** | Yes — automatic | New source columns automatically added on the next pipeline run. Prior rows have `null` for the new column. Deleted source columns retained in Delta with `null`. |

### Recommendations

For bronze-layer ingestion using Auto Loader, `addNewColumns` is appropriate — it ensures all source data lands without loss. `rescue` provides an additional safety net for highly variable schemas. `failOnNewColumns` is appropriate for silver or gold layer pipelines where uncontrolled schema drift should trigger investigation.

Column renames and removals are breaking changes that no method handles automatically without data loss. Schema evolution strategies must include a process for communicating source schema changes to downstream consumers, not just a technical mechanism for handling them.

For Data Vault 2.0 pipelines using native PySpark/SQL staging (see below), hash keys and hashdiff columns are derived from source columns. Any change to the columns included in a hash key is a business logic change that requires deprecating the existing structure and creating a new one with the corrected hash definition.

### See Also

- [Auto Loader schema evolution — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/schema)
- [Delta table schema evolution — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema)

---

## Raw Staging for Data Vault 2.0 (Native PySpark / SQL)

### Overview

In a Data Vault 2.0 architecture, the staging layer prepares raw source data for vault loading by deriving the surrogate keys (hash keys) and change-detection hashes (hashdiffs) that Data Vault structures depend on. On the native Databricks stack, this is implemented directly in PySpark or Spark SQL using the built-in `MD5()`, `SHA2()`, `CONCAT_WS()`, and `COALESCE()` functions — no external macro library is required.

The staging layer is typically implemented as a Delta Live Tables pipeline (for managed, observable staging) or as a PySpark job / SQL view (for lighter-weight implementations). The design considerations are identical to the dbt/AutomateDV approach — only the implementation mechanism differs.

### Design Considerations

**Hash algorithm consistency.** All staging models across all sources must use the same hash algorithm. `MD5()` is the most common choice — it produces 32-character hex strings and is computationally inexpensive. `SHA2(expr, 256)` produces 64-character hex strings and is appropriate when the organisation has a policy against MD5. The choice must be made at platform design time and must not change after vault tables have been populated, as every hash value would change, breaking all existing vault records.

**Column ordering in hash keys.** Hash keys are computed from an ordered concatenation of business key columns using `CONCAT_WS`. The order of columns in the concatenation must be fixed and documented at the time the staging model is first built. `CONCAT_WS('||', order_id, customer_id)` and `CONCAT_WS('||', customer_id, order_id)` produce different hash values. Column ordering must be agreed and recorded in the data dictionary before the first load.

**Null substitution strategy.** Business key columns must not contain `null` before hashing, because `CONCAT_WS` skips `null` values — `CONCAT_WS('||', 'A', NULL, 'B')` produces `'A||B'`, which is the same as `CONCAT_WS('||', 'A', 'B')`. Use `COALESCE(col, '^^')` to substitute a sentinel value before concatenation. The sentinel must not appear in any legitimate business key value.

**Hashdiff column scope.** A hashdiff column is a hash of all descriptive (non-key) attribute columns in a satellite source. The columns included in the hashdiff must match exactly the columns loaded into that satellite. Adding or removing a column from the hashdiff definition after the satellite has been populated will cause every existing record to re-evaluate as changed on the next load.

**Implementation as a DLT view.** The staging layer should be implemented as a DLT streaming view (`@dlt.view`) rather than a materialized table where possible, as staging is a transformation step, not a persistence layer. This avoids unnecessary storage duplication and keeps lineage clean.

### See Also

- [Delta Live Tables Python API — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/python-ref)
- [MD5 function — Databricks SQL](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/md5)
- [SHA2 function — Databricks SQL](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/sha2)
- `ingestion_cookbook.md` — Native Data Vault Staging implementation examples
- `../../data_vault/` — Raw vault, business vault, and information mart cookbook

---

## Reference Data Loading (Native)

### Overview

Reference data — country codes, currency codes, product status mappings, and similar small, static lookup tables — must be managed carefully regardless of whether dbt is in the toolchain. On the native Databricks stack, the appropriate pattern depends on the size of the dataset, the update frequency, and who is responsible for updates.

### Pattern Selection

| Pattern | When to Use | Mechanism |
|---------|-------------|-----------|
| **COPY INTO from cloud storage** | Reference data managed by a technical team; data is stored as CSV or Parquet files in ADLS/S3; idempotent loads required | Store the reference CSV in a controlled ADLS path; use COPY INTO to load idempotently; update by uploading a new version of the file and re-running COPY INTO with `FORCE = TRUE` |
| **`INSERT OVERWRITE` or `CREATE OR REPLACE TABLE`** | Very small reference tables (< 100 rows); managed directly in a Databricks notebook or SQL script; change history tracked via Delta table history | Write the reference data directly as SQL `VALUES` in a notebook or SQL file; version the SQL script in git |
| **Auto Loader from cloud storage** | Reference data that changes incrementally over time; new rows added periodically without full reload; standard Auto Loader pipeline with `addNewColumns` schema evolution | Standard Auto Loader pattern; checkpoint tracks which versions of the file have been processed |
| **Delta table managed via Databricks workflow** | Reference data updated by a business team via a form or API; loaded into Delta by a scheduled Databricks Job that calls an API or reads from a shared location | Schedule a Databricks Job to read from the shared source and MERGE into the Delta reference table |

### Trade-offs

COPY INTO from cloud storage is the closest native equivalent to dbt seeds: a file in a shared location is the source of truth, COPY INTO provides idempotent loading, and the file itself can be version-controlled in git and deployed as part of a CI/CD pipeline (e.g., uploaded to ADLS via Databricks Asset Bundles or a CI pipeline step). Unlike dbt seeds, COPY INTO from cloud storage scales to millions of rows without performance issues.

`INSERT OVERWRITE` with SQL `VALUES` is appropriate only for the smallest reference tables (a handful of rows). For anything larger, it becomes impractical to maintain SQL literal values in a script.

The key operational advantage of the COPY INTO pattern over dbt seeds is that business analysts or operations staff can update reference data by replacing a file in a known cloud storage location — they do not need git access or dbt knowledge. A scheduled Databricks Job monitors the path and reloads when a new file is detected.

### See Also

- [COPY INTO — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)
- [Delta table `INSERT OVERWRITE` — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-dml-insert-overwrite-table)
- `ingestion_cookbook.md` — Reference Data Loading implementation examples

---

## Database Ingestion (JDBC)

### Overview

JDBC ingestion is the standard pattern for extracting data directly from relational database systems into Databricks. Spark's JDBC data source reads data over a JDBC connection and materialises it as a DataFrame, written to Delta Lake. It is a batch-pull mechanism suitable for incremental or full extraction from OLTP systems.

### Decision Criteria

| Factor | Guidance |
|--------|----------|
| **Full vs. incremental extract** | Full extract: read the entire table each run — simple but expensive for large tables. Incremental: filter on a watermark column (`updated_at`, sequence ID) to read only changed rows since the last run. Requires a reliable, indexed watermark column. |
| **Parallelism** | Default JDBC reads are single-threaded. Configure `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound` for parallel reads. The partition column must be numeric or date-type and indexed on the source. |
| **Source load** | Parallel reads issue multiple concurrent queries. Use a read replica where available. Tune `numPartitions` to stay within the source's connection limit. |
| **Credential management** | Store all JDBC credentials in Databricks Secrets. Never hardcode in notebooks or job parameters. |

### Trade-offs

Increasing `numPartitions` improves throughput on the Databricks side but places proportionally more load on the source database. JDBC does not detect hard deletes — rows physically removed from the source remain in the Delta target unless a separate reconciliation job removes them. For full delete propagation, use a CDC tool (Debezium) that reads the database transaction log.

### See Also

- [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/external-systems/jdbc)
- `ingestion_cookbook.md` — JDBC implementation with partition tuning examples

---

## Managed Ingestion

### Overview

Managed ingestion patterns handle extraction from source systems via a connector service. On the native Databricks stack, **Lakeflow Connect** is the preferred choice — it runs on serverless compute within Databricks, is governed by Unity Catalog, and does not require data to transit third-party infrastructure.

**Partner connectors (Fivetran, Airbyte)** are appropriate where the source is not yet on the Lakeflow Connect catalogue or where an existing connector platform investment is in place.

### Lakeflow Connect

As of March 2026, Lakeflow Connect is generally available for Salesforce, Workday, and SQL Server, with additional connectors available in preview. Key characteristics:

- Runs entirely on Databricks serverless compute — no third-party data transit
- Governed by Unity Catalog: lineage, access control, and audit apply to ingested tables
- Automatic schema evolution: new source columns are automatically added on the next pipeline run
- CI/CD support via Databricks Asset Bundles
- Cost model: serverless DBU consumption, not per-row fees

### Partner Connectors

Partner connectors (Fivetran, Airbyte) remain appropriate where the source is not on Lakeflow Connect's catalogue. Key governance consideration: source data transits the vendor's infrastructure. Obtain a Data Processing Agreement (DPA) for any personally identifiable or regulated data. Isolate connector output to a dedicated schema to limit the blast radius of the connector service principal's permissions.

### See Also

- [Lakeflow Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- [Databricks Partner Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partner-connect/)
- `ingestion_cookbook.md` — Lakeflow Connect and Partner Connector implementation examples
