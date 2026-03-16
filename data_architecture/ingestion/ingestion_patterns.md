# Ingestion Architectural Patterns
## Databricks

> **Scope:** This document covers ingestion using Databricks platform features only — Auto Loader, COPY INTO, Structured Streaming, JDBC, SFTP connector, Lakeflow Connect, and Databricks workflows. For multi-hop pipeline orchestration using Lakeflow Spark Declarative Pipelines (SDP), see `../processing/processing_patterns.md`.

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
- **SFTP push patterns** where partner or vendor systems push files to a Databricks-managed SFTP endpoint, which then lands them in cloud storage
- **Direct writes from IoT or application services** that write events or snapshots to cloud storage buckets

> **Scope note:** Configuration of upstream delivery tools (Azure Data Factory, SFTP servers, SaaS export schedules) is outside the scope of this cookbook. This document covers what Databricks does with data once it is in the landing zone — or when a landing zone is not needed at all.

### When You Need a Landing Zone

The following ingestion methods require data to be present in cloud storage before Databricks can read it:

| Method | Landing Zone Required | Reason |
|--------|-----------------------|--------|
| **Auto Loader** | Yes | Reads file paths from ADLS/S3/GCS; upstream must deposit files there first |
| **COPY INTO** | Yes | Reads from a cloud storage path; files must already be present at that path |
| **SFTP Native Connector** | No | The native SFTP connector reads directly from the SFTP server into Spark — no intermediate cloud storage staging step is required. The GA alternative (paramiko + Auto Loader) does require a landing zone. |

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
- **Direct ingestion via connectors is the right choice for SaaS sources:** SaaS applications (Salesforce, Workday) do not expose a reliable file export that would populate a landing zone automatically. Lakeflow Connect is the correct pattern on the native stack — attempting to land SaaS data via file export introduces fragility and latency.

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
| **SFTP (Native Connector)** | Receiving files from partner or vendor systems that deliver via SFTP; organisations that want a managed connector without custom Python scripting | ⚠️ **Public preview as of March 2026** — not recommended for critical production workloads without validating preview stability; not suitable where the source SFTP server has connectivity restrictions incompatible with Databricks-managed egress |
| **Lakeflow Connect** | Managed ingestion from SaaS applications and databases where building a custom connector is not justified; teams that want a fully native Databricks-managed pipeline with Unity Catalog governance and serverless compute. GA as of March 2026: Salesforce, Workday, SQL Server. See the [connector catalogue](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) for the current list including preview connectors. | Sources not yet on the Lakeflow Connect connector catalogue; organisations with strict data residency requirements that need careful evaluation of data paths |

### Trade-offs

Auto Loader and Structured Streaming both use Spark's checkpoint mechanism, which provides exactly-once guarantees by tracking which files or offsets have been processed. This is powerful but requires that the checkpoint directory be durable (on cloud storage, not ephemeral local storage), and that it is never deleted unless you intend to reprocess from the beginning. COPY INTO tracks processed files inside the Delta table's transaction log, making it simpler to reason about — no external checkpoint directory is needed — but this also means that COPY INTO's tracking is tied to that specific Delta table and cannot be reused if the table is recreated.

The Notebook Pattern has no place in production recurring ingestion. It has no state tracking, no deduplication guarantee, and no audit trail beyond the notebook run history.

Lakeflow Connect is the preferred choice for new SaaS ingestion implementations on the native Databricks stack — it runs entirely within Databricks infrastructure and integrates with Unity Catalog lineage and access control without requiring a third-party service.

### See Also

- [Auto Loader documentation — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)
- [COPY INTO documentation — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)
- [Structured Streaming — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/)
- [Lakeflow Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- [SFTP ingestion — Azure Databricks (public preview)](https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp)
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

Micro-batch mode (`trigger(availableNow=True)`) provides exactly-once guarantees via checkpoint while running on a job cluster that terminates after each cycle — significantly cheaper than a continuously running stream and more reliable than pure batch. For most enterprise use cases with latency requirements of five to thirty minutes, micro-batch is the optimal choice.

Continuous streaming should only be chosen when the business genuinely demands sub-minute freshness and the organisation is prepared to invest in the operational tooling to support it.

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
| **COPY INTO** | No | Columns not in the target schema are silently dropped. Missing columns are written as `null`. No automatic schema evolution. |
| **Structured Streaming** | Limited | Unknown columns dropped by default. Schema changes after stream start cause failure unless the checkpoint is deleted and the stream restarted. |
| **Notebook Pattern** | Manual | Schema must be updated manually in the notebook before the next run. |
| **JDBC** | No automatic evolution | New source columns require a manual `ALTER TABLE ... ADD COLUMN` on the Delta target, or `mergeSchema` enabled on the write. |
| **SFTP (Native Connector)** | Depends on format | Follows the underlying file format reader. JSON with compatible `schemaEvolutionMode` options behaves similarly to Auto Loader. CSV with inferred schema may fail on new columns. |
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
| **Credential management** | Store all JDBC credentials in Databricks Secrets. Never hardcode in notebooks or job parameters. On Azure Databricks, use Azure Key Vault-backed secret scopes as the recommended backend — see the Infrastructure Prerequisites section of `ingestion_cookbook.md` for setup guidance. |

### Trade-offs

Increasing `numPartitions` improves throughput on the Databricks side but places proportionally more load on the source database. JDBC does not detect hard deletes — rows physically removed from the source remain in the Delta target unless a separate reconciliation job removes them. For full delete propagation, use a CDC tool (Debezium) that reads the database transaction log.

### See Also

- [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/external-systems/jdbc)
- `ingestion_cookbook.md` — JDBC implementation with partition tuning examples

---

## Managed Ingestion

### Overview

Managed ingestion patterns handle extraction from source systems via a connector service. On the native Databricks stack, **Lakeflow Connect** is the preferred choice — it runs on serverless compute within Databricks, is governed by Unity Catalog, and does not require data to transit third-party infrastructure.

### Lakeflow Connect

As of March 2026, Lakeflow Connect is generally available for Salesforce, Workday, and SQL Server, with additional connectors available in preview. Key characteristics:

- Runs entirely on Databricks serverless compute — no third-party data transit
- Governed by Unity Catalog: lineage, access control, and audit apply to ingested tables
- Automatic schema evolution: new source columns are automatically added on the next pipeline run
- CI/CD support via Databricks Asset Bundles
- Cost model: serverless DBU consumption, not per-row fees

### See Also

- [Lakeflow Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- `ingestion_cookbook.md` — Lakeflow Connect implementation examples
