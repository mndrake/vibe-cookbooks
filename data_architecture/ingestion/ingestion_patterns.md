# Ingestion Architectural Patterns
## Databricks

> **Scope:** This document covers ingestion using Databricks platform features only — Auto Loader, COPY INTO, Structured Streaming, JDBC, Lakeflow Connect, and Databricks workflows. For multi-hop pipeline orchestration using Lakeflow Spark Declarative Pipelines (SDP), see `../processing/processing_patterns.md`.

---

## Overview

This document describes the architectural patterns and design decisions that govern data ingestion on Databricks using the native Databricks toolchain. It is a decision and design reference, not a step-by-step implementation guide. Step-by-step examples for each method are found in `ingestion_cookbook.md` in the same directory.

This document is intended for data architects, senior data engineers, and technical leads who are selecting or reviewing ingestion patterns for a Databricks-based data platform. It covers method selection criteria, batch versus streaming trade-offs, and schema evolution strategies.

---

## Landing Zone Architecture

> **Architecture diagram:** [Medallion architecture overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion) shows how raw data flows from source systems through landing zones into Bronze, Silver, and Gold Delta tables. [ADLS Gen2 introduction — Microsoft](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) includes a diagram of the hierarchical namespace used as a landing zone.

### What Is a Landing Zone?

A landing zone is a cloud storage location (ADLS Gen2, S3, or GCS) where raw data files are deposited by upstream systems before Databricks reads them. Databricks does not control how data arrives in the landing zone — that is the responsibility of the upstream system. Common upstream delivery mechanisms include:

- **File export pipelines** (Azure Data Factory, AWS Glue, Informatica, Talend) that extract from OLTP or ERP systems and write files to cloud storage
- **SaaS application exports** (Salesforce data export, Workday report delivery) scheduled to drop CSV or Parquet files to a storage account
- **Direct writes from IoT or application services** that write events or snapshots to cloud storage buckets

> **Scope note:** Configuration of upstream delivery tools (Azure Data Factory, SFTP servers, SaaS export schedules) is outside the scope of this cookbook. This document covers what Databricks does with data once it is in the landing zone — or when a landing zone is not needed at all.

### When You Need a Landing Zone

The following ingestion methods require data to be present in cloud storage before Databricks can read it:

| Method | Landing Zone Required | Reason |
|--------|-----------------------|--------|
| **Auto Loader** | Yes | Reads file paths from ADLS/S3/GCS; upstream must deposit files there first |
| **COPY INTO** | Yes | Reads from a cloud storage path; files must already be present at that path |

For these methods, the landing zone is the contractual boundary between the upstream delivery system and the Databricks pipeline. The upstream system writes; Databricks reads. Neither side needs to know about the other's schedule.

### When You Can Bypass a Landing Zone

The following ingestion methods read directly from the source system without requiring a cloud storage staging step:

| Method | Landing Zone Required | Source |
|--------|-----------------------|--------|
| **JDBC** | No | Reads directly from a relational database (SQL Server, PostgreSQL, MySQL, Oracle) |
| **Lakeflow Connect** | No | Managed connector reads from SaaS APIs (Salesforce, Workday, etc.) and writes directly to Delta tables |
| **Structured Streaming (Kafka/Event Hubs/Kinesis)** | No | Reads from a message broker offset — no file system staging involved |

Direct ingestion simplifies the architecture (fewer storage accounts, fewer permissions, no file lifecycle management) but removes the raw file audit trail that a landing zone provides.

### Design Considerations

- **Landing zone enables replay:** Raw files retained in the landing zone can be reprocessed if a Databricks Workflows pipeline run fails, if data is corrupted downstream, or if a new processing requirement emerges. Once data has been ingested via JDBC or a managed connector and the source system has rolled over its data, reprocessing may not be possible.
- **Landing zone decouples delivery from processing:** The upstream system deposits files on its own schedule; Auto Loader or COPY INTO processes them on the Databricks Workflows schedule. The two are fully decoupled. With JDBC or streaming, the Databricks pipeline must connect to the source system at extraction time — a source outage directly blocks the pipeline.
- **Landing zone adds storage cost and file lifecycle management:** Raw files in the landing zone accumulate over time. Define a retention policy (e.g., retain for 30 days, then archive to cool tier or delete) and enforce it with storage lifecycle rules — not Databricks logic.
- **SaaS source ingestion options:** SaaS applications such as Salesforce and Workday expose file export mechanisms (Salesforce Data Export, Workday Report-as-a-Service) that can populate a landing zone on a scheduled basis. These are appropriate when the export schedule meets latency requirements, the export scope covers the required objects, and the operational model already includes landing zone file management. Lakeflow Connect handles extraction, scheduling, and incremental loading within Databricks without requiring a separate file export pipeline or landing zone. For new implementations where the required source objects are available in the Lakeflow Connect connector catalogue, evaluate the operational overhead of each approach: a file export pipeline requires landing zone storage, lifecycle management, and an upstream scheduling tool; Lakeflow Connect requires serverless compute cost per sync cycle and dependency on connector availability. Choose based on which operational model is more appropriate for your team.

### See Also

- [Azure Data Lake Storage Gen2 — Microsoft Documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Auto Loader — reading from cloud storage](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)
- [Lakeflow Connect — direct SaaS ingestion](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)

---

## Ingestion Method Selection

> **Architecture diagram:** [Auto Loader overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/) includes a diagram comparing Auto Loader's file discovery and checkpoint mechanism against COPY INTO. [Lakeflow Connect overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) shows the managed connector architecture with Unity Catalog governance.

### Overview

Databricks supports multiple ingestion methods, each suited to different latency requirements, operational models, and data volumes. Choosing the wrong method for a use case leads to avoidable operational overhead, unnecessary cost, or incorrect data. This section provides a structured comparison to guide that selection.

### Decision Criteria

| Method | Best For | Avoid When |
|--------|----------|------------|
| **Auto Loader** | Continuous or scheduled file arrival in cloud storage (ADLS, S3, GCS); large file volumes where checkpoint-based state tracking is important; tables that require schema evolution over time | You need sub-minute event-level latency from a message bus; files are delivered once via a one-off process |
| **COPY INTO** | Scheduled batch loads from a known cloud storage path; scenarios where idempotency is critical and re-runs must not create duplicates; simple batch pipelines without schema evolution needs | You need schema to auto-evolve as new columns arrive; you need automatic state management without a checkpoint directory |
| **Structured Streaming** | Sub-minute latency ingestion from Kafka, Azure Event Hubs, or Kinesis; event-driven architectures where consumer lag must be minimised; stateful aggregations with watermarking | The source is cloud storage files rather than a message bus; your team lacks the operational capability to manage streaming job recovery |
| **Notebook Pattern** | One-off or exploratory data loads during development or investigation; historical backfills run once by a human | Any recurring production load; any scenario where re-run safety or auditability is required |
| **JDBC** | Ingesting data directly from relational databases (SQL Server, PostgreSQL, MySQL, Oracle) where cloud storage is not the source; incremental or full extract from OLTP systems | Source data volumes are very large and partition-based parallelism cannot be applied; real-time latency requirements (JDBC is a batch-pull mechanism) |
| **Lakeflow Connect** | SaaS or database sources covered by the GA connector catalogue, where reducing the operational surface of the ingestion pipeline is prioritised over cost optimisation for low-volume syncs. GA as of March 2026: Salesforce, Workday, SQL Server, ServiceNow, and Google Analytics. See the [connector catalogue](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) for the current list including preview connectors. | Sources not yet on the Lakeflow Connect connector catalogue; organisations with strict data residency requirements that need careful evaluation of data paths; low-volume, low-frequency syncs where serverless compute cost exceeds the cost of an equivalent JDBC or file-based pipeline |

### Trade-offs

Auto Loader and Structured Streaming both use Spark's checkpoint mechanism to track which files or offsets have been processed. When writing to Delta Lake with a durable checkpoint, each file or message is written to Delta once — provided the checkpoint is intact and the Delta write completes. If the checkpoint is deleted, both Auto Loader and Structured Streaming reprocess from the beginning of the source. This requires that the checkpoint directory be durable (on cloud storage, not ephemeral local storage), and that it is never deleted unless a full reprocess is intended. COPY INTO tracks processed files inside the Delta table's transaction log, making it simpler to reason about — no external checkpoint directory is needed — but this also means that COPY INTO's tracking is tied to that specific Delta table and cannot be reused if the table is recreated.

The Notebook Pattern reads from the source and writes to Delta without any built-in state tracking, checkpoint, or deduplication mechanism. Each run must implement its own watermark logic or will reprocess the full source. If a notebook run fails partway through, restarting it re-reads from the beginning, potentially writing duplicate rows unless the write mode is `overwrite`. It has no structured audit trail beyond the notebook run history. For a one-off or exploratory load these limitations are acceptable. For any load that must run on a schedule, be safe to restart, and produce an auditable record, use Auto Loader, COPY INTO, or Structured Streaming.

For SaaS sources covered by the GA connector catalogue, Lakeflow Connect runs extraction and loading on Databricks serverless compute without requiring a third-party connector tool or custom integration code. Data does not transit third-party connector infrastructure (it passes directly from the SaaS source API to Databricks serverless compute), and Unity Catalog lineage and access control apply to the ingested tables. Evaluate against third-party connector tools (Fivetran, Airbyte) or a file export plus landing zone approach based on connector catalogue coverage, cost model (serverless DBU per sync cycle vs. per-row or subscription pricing), and whether keeping all data flows within Databricks governance is a requirement. For sources not yet on the connector catalogue, for organisations with strict data residency requirements, or where an existing third-party connector tool is already in use, land files in ADLS Gen2 and apply Auto Loader.

### See Also

- [Auto Loader documentation — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)
- [COPY INTO documentation — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)
- [Structured Streaming — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/)
- [Lakeflow Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- `ingestion_cookbook.md` — step-by-step implementation for each method

---

## Batch vs. Streaming Trade-offs

> **Architecture diagram:** [Structured Streaming programming guide — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/) includes diagrams of the micro-batch execution model and trigger types. [Auto Loader production guide — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/production) shows the `availableNow` trigger lifecycle compared to continuous streaming.

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

Micro-batch mode (`trigger(availableNow=True)`) terminates the cluster after each run, avoiding the continuous cost of a running stream. For pipelines where latency requirements are measured in minutes rather than seconds, and where event volume does not require continuous processing to stay within the trigger window, this reduces cost relative to continuous streaming. For very high-throughput sources where the volume arriving between trigger cycles exceeds what a single cluster can process within the window, or where sub-minute latency is a hard requirement, continuous streaming is required regardless of cost. Evaluate based on your actual event volume, the time budget per trigger cycle, and the cluster startup cost for your workload.

### See Also

- [Structured Streaming trigger types — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers)
- [Auto Loader availableNow trigger — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/production)
- [Table streaming reads and writes — Delta Lake](https://docs.delta.io/latest/delta-streaming.html)

---

## Schema Evolution Strategy

> **Architecture diagram:** [Auto Loader schema inference and evolution — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/schema) diagrams the `schemaEvolutionMode` options and how `_rescued_data` captures unexpected fields. [Delta table schema evolution — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema) illustrates `mergeSchema` and `overwriteSchema` behaviour.

### Overview

Schema evolution is the process by which a data pipeline handles changes to the structure of incoming data. Handling it incorrectly leads to pipeline failures, silently dropped columns, or corrupt downstream tables. Each ingestion method has fundamentally different behaviour when schema changes occur.

### Per-Method Behaviour

| Method | Schema Evolution Support | Behaviour on Schema Change |
|--------|--------------------------|---------------------------|
| **Auto Loader** | Yes — configurable via `cloudFiles.schemaEvolutionMode` | `addNewColumns`: new columns added automatically. `rescue`: unexpected columns captured in `_rescued_data`. `failOnNewColumns`: pipeline fails on new column (useful for controlled environments). `none`: new columns silently dropped. |
| **COPY INTO** | Limited — new columns can be added with `mergeSchema = 'true'` in `COPY_OPTIONS` | Without `mergeSchema`, columns not in the target schema are silently dropped and missing columns are written as `null`. With `mergeSchema = 'true'` in `COPY_OPTIONS`, new columns in source files are automatically added to the target table. This must be explicitly set on each run; there is no automatic detection. |
| **Structured Streaming** | Limited | Unknown columns dropped by default. Schema changes after stream start cause failure unless the checkpoint is deleted and the stream restarted. |
| **Notebook Pattern** | Manual | Schema must be updated manually in the notebook before the next run. |
| **JDBC** | No automatic evolution | New source columns require a manual `ALTER TABLE ... ADD COLUMN` on the Delta target, or `mergeSchema` enabled on the write. |
| **Lakeflow Connect** | Yes — automatic | New source columns automatically added on the next pipeline run. Prior rows have `null` for the new column. Deleted source columns retained in Delta with `null`. |

### Recommendations

For bronze-layer ingestion using Auto Loader, `addNewColumns` is appropriate — it ensures all source data lands without loss. `rescue` provides an additional safety net for highly variable schemas. `failOnNewColumns` is appropriate for silver or gold layer pipelines where uncontrolled schema drift should trigger investigation.

Column renames and removals are breaking changes that no method handles automatically without data loss. Schema evolution strategies must include a process for communicating source schema changes to downstream consumers, not just a technical mechanism for handling them.

### See Also

- [Auto Loader schema evolution — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/schema)
- [Delta table schema evolution — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema)
- [Delta Lake schema evolution — Delta Lake](https://docs.delta.io/latest/delta-schema-evolution.html)

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
| **Credential management** | Store all JDBC credentials in Databricks Secrets. Never hardcode in notebooks or job parameters. On Azure Databricks, choose between Azure Key Vault-backed secret scopes (when credentials are rotated centrally by a secrets management team) or Databricks-managed scopes (when Key Vault is not available or the additional resource overhead is not justified) — see the Infrastructure Prerequisites section of `ingestion_cookbook.md` for setup guidance on both. |

### Trade-offs

Increasing `numPartitions` improves throughput on the Databricks side but places proportionally more load on the source database. JDBC does not detect hard deletes — rows physically removed from the source remain in the Delta target unless a separate reconciliation job removes them. For full delete propagation, use a CDC tool (Debezium) that reads the database transaction log.

### See Also

- [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/external-systems/jdbc)
- `ingestion_cookbook.md` — JDBC implementation with partition tuning examples

---

## Managed Ingestion

### Overview

Managed ingestion patterns handle extraction from source systems via a connector service. On the native Databricks stack, **Lakeflow Connect** runs on serverless compute within Databricks and is governed by Unity Catalog. For sources covered by the connector catalogue, Lakeflow Connect provides a managed connector that runs on Databricks serverless compute. Teams that would otherwise use a third-party connector tool or build a custom integration should evaluate Lakeflow Connect against those alternatives based on connector coverage, cost, and operational model.

### Lakeflow Connect

As of March 2026, Lakeflow Connect is generally available for Salesforce, Workday, SQL Server, ServiceNow, and Google Analytics, with additional connectors available in preview. Characteristics:

- **Compute:** Runs on Databricks serverless compute. Data does not transit third-party infrastructure. Requires Unity Catalog to be enabled — lineage, access control, and audit are applied to ingested tables via Unity Catalog governance.
- **Schema evolution:** New source columns are automatically added to the Delta table schema on the next pipeline run. Column deletions in the source are not propagated — the column is retained in Delta with `null` values for rows synced after the deletion. Column renames produce a new column; the prior column persists with historical values. Downstream pipelines must account for both cases.
- **Hard delete propagation:** Hard deletes (rows physically removed from the source) are not propagated to the Delta table for the Salesforce connector as of March 2026 — deleted rows remain in Delta until a manual reconciliation job removes them. Soft deletes are handled at the next sync cycle. If hard delete propagation is required, use a CDC-based approach (Debezium) or a separate reconciliation job that compares source and target row counts on a schedule.
- **CI/CD:** Supports deployment via Databricks Asset Bundles.
- **Cost:** Billed at the Databricks serverless DBU rate based on compute time, not per row ingested. For low-volume, low-frequency syncs, the per-trigger compute overhead may exceed what a per-row pricing model would cost — evaluate based on sync frequency and data volume.

### See Also

- [Lakeflow Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- `ingestion_cookbook.md` — Lakeflow Connect implementation examples
