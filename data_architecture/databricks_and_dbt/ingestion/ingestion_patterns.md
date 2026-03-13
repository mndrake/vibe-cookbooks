# Ingestion Architectural Patterns

## Overview

This document describes the architectural patterns and design decisions that govern data ingestion on Databricks. It is a decision and design reference, not a step-by-step implementation guide. Step-by-step examples for each method are found in `ingestion_cookbook.md` in the same directory.

This document is intended for data architects, senior data engineers, and technical leads who are selecting or reviewing ingestion patterns for a Databricks-based data platform. It covers method selection criteria, batch versus streaming trade-offs, schema evolution strategies, raw staging for Data Vault 2.0 pipelines, and the appropriate use of dbt seeds as a reference data ingestion pattern.

---

## Landing Zone Architecture

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
| **SFTP Native Connector** | Yes | Databricks SFTP connector lands received files into a configured cloud storage path, then processes them |

For these methods, the landing zone is the contractual boundary between the upstream delivery system and the Databricks pipeline. The upstream system writes; Databricks reads. Neither side needs to know about the other's schedule.

### When You Can Bypass a Landing Zone

The following ingestion methods read directly from the source system without requiring a cloud storage staging step:

| Method | Landing Zone Required | Source |
|--------|-----------------------|--------|
| **JDBC** | No | Reads directly from a relational database (SQL Server, PostgreSQL, MySQL, Oracle) |
| **Lakeflow Connect** | No | Managed connector reads from SaaS APIs (Salesforce, Workday, etc.) and writes directly to Delta tables |
| **Partner Connectors (Fivetran, Airbyte)** | No | Connector service reads from source and writes directly to Delta; no file staging step |
| **Structured Streaming (Kafka/Event Hubs/Kinesis)** | No | Reads from a message broker offset — no file system staging involved |

Direct ingestion simplifies the architecture (fewer storage accounts, fewer permissions, no file lifecycle management) but removes the raw file audit trail that a landing zone provides.

### Design Considerations

- **Landing zone enables replay:** Raw files retained in the landing zone can be reprocessed if a Databricks pipeline run fails, if data is corrupted downstream, or if a new processing requirement emerges. Once data has been ingested via JDBC or a managed connector and the source system has rolled over its data, reprocessing may not be possible.
- **Landing zone decouples delivery from processing:** The upstream system deposits files on its own schedule; Auto Loader or COPY INTO processes them on the Databricks schedule. The two are fully decoupled. With JDBC or streaming, the Databricks pipeline must connect to the source system at extraction time — a source outage directly blocks the pipeline.
- **Data Vault architectures favour landing zone + Auto Loader:** The Raw Vault loading principle (never modify source data, load exactly as received) is most naturally satisfied by landing raw files and loading them unchanged via Auto Loader into a staging layer before hashing. JDBC or connector-based ingestion can also feed a Raw Vault, but the raw file is not retained.
- **Landing zone adds storage cost and file lifecycle management:** Raw files in the landing zone accumulate over time. Define a retention policy (e.g., retain for 30 days, then archive to cool tier or delete) and enforce it with storage lifecycle rules — not Databricks logic.
- **Direct ingestion via connectors is the right choice for SaaS sources:** SaaS applications (Salesforce, Workday) do not expose a reliable file export that would populate a landing zone automatically. Lakeflow Connect or a partner connector is the correct pattern — attempting to land SaaS data via file export introduces fragility and latency.

### See Also

- [Azure Data Lake Storage Gen2 — Microsoft Documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Auto Loader — reading from cloud storage](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [Lakeflow Connect — direct SaaS ingestion](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)

---

## Ingestion Method Selection

### Overview

Databricks supports multiple ingestion methods, each suited to different latency requirements, operational models, and data volumes. Choosing the wrong method for a use case leads to avoidable operational overhead, unnecessary cost, or incorrect data — for example, using a Notebook Pattern in production leads to manual re-run risk, while using Delta Live Tables for a simple one-time historical load introduces unnecessary managed infrastructure. This section provides a structured comparison to guide that selection.

### Decision Criteria

The following table maps each ingestion method to the scenarios where it excels and the conditions under which it should be avoided.

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
| **Partner Connectors (Fivetran, Airbyte)** | SaaS sources not yet covered by Lakeflow Connect; organisations already invested in a specific connector platform; cases where the partner connector's catalogue breadth exceeds Lakeflow Connect's current offering | Sources supported natively by Lakeflow Connect, where consolidating on the Databricks-native toolchain is preferred |

### Trade-offs

Auto Loader and Structured Streaming both use Spark's checkpoint mechanism, which provides exactly-once guarantees by tracking which files or offsets have been processed. This is powerful but requires that the checkpoint directory be durable (on cloud storage, not ephemeral local storage), and that it is never deleted unless you intend to reprocess from the beginning. COPY INTO tracks processed files inside the Delta table's transaction log, making it simpler to reason about — no external checkpoint directory is needed — but this also means that COPY INTO's tracking is tied to that specific Delta table and cannot be reused if the table is recreated.

Delta Live Tables sits above all of these methods in the abstraction stack. It manages checkpointing, retries, and cluster lifecycle automatically. The cost of that abstraction is reduced control: DLT pipelines run on DLT-managed clusters with fixed configuration options, and trigger timing is controlled by the pipeline's continuous or triggered mode rather than by arbitrary cron logic. Teams with strong Spark expertise and a need for fine-grained control will find DLT limiting; teams that want operational simplicity and built-in data quality will find it the right choice.

The Notebook Pattern has no place in production recurring ingestion. It has no state tracking, no deduplication guarantee, and no audit trail beyond the notebook run history. It is documented here to be explicitly excluded from production use cases, not to endorse it.

Lakeflow Connect and third-party partner connectors both remove the burden of building and maintaining connectors for SaaS sources. Lakeflow Connect is the preferred choice for new implementations because it runs entirely within Databricks (serverless compute, Unity Catalog governance, Lakeflow Jobs orchestration) and does not require data to transit third-party infrastructure. Partner connectors (Fivetran, Airbyte) remain appropriate where the source is not yet on Lakeflow Connect's catalogue or where an existing investment in a connector platform exists.

### See Also

- [Auto Loader documentation — Databricks](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [COPY INTO documentation — Databricks](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html)
- [Delta Live Tables overview — Databricks](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Structured Streaming programming guide — Apache Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Lakeflow Connect overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- [SFTP ingestion — Azure Databricks (public preview)](https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp)
- `ingestion_cookbook.md` — step-by-step implementation for each method

---

## Batch vs. Streaming Trade-offs

### Overview

The choice between batch and streaming ingestion is one of the most consequential architectural decisions in a data platform. It affects latency, cost, operational complexity, and failure recovery behaviour. Many platforms default to batch ingestion because it is simpler to build and operate, then later attempt to retrofit streaming — a costly migration. This section provides the criteria to make the right choice upfront.

### Decision Criteria

| Dimension | Batch | Micro-Batch (Structured Streaming with `trigger(availableNow=True)`) | Continuous Streaming |
|-----------|-------|----------------------------------------------------------------------|----------------------|
| **Latency** | Minutes to hours, depending on schedule frequency | Minutes, depending on trigger interval and cluster startup time | Seconds to sub-minute |
| **Cost model** | Job cluster spun up per run; cost is proportional to run frequency and duration | Job cluster per trigger cycle; startup overhead matters for short cycles | Always-on cluster or provisioned throughput; cost is continuous regardless of data volume |
| **Failure recovery** | Re-run the job from the beginning of the batch window; source files must still be available | Resume from checkpoint; no data re-read if checkpoint is intact | Resume from last committed offset in checkpoint; Kafka retention must be sufficient |
| **Operational complexity** | Low — standard job scheduling, no persistent state to manage | Medium — checkpoint directory must be managed and monitored | High — cluster health, consumer lag, watermarking, stateful operations, and backpressure all require monitoring |
| **Deduplication** | Simpler — batch boundaries are explicit | Requires watermarking or deduplication logic if micro-batches overlap | Requires watermarking and careful stateful design to handle late-arriving events |
| **Downstream freshness** | Downstream tables are stale between runs | Near-real-time downstream freshness | Real-time or near-real-time downstream freshness |

### Trade-offs

Micro-batch mode — Structured Streaming with `trigger(availableNow=True)` or `trigger(processingTime="N minutes")` — is the most commonly misunderstood option. It provides a middle ground: it uses the checkpoint mechanism of streaming (giving exactly-once guarantees and resumable state) while running on a job cluster that terminates after each trigger cycle. This makes it significantly cheaper than a continuously running stream and more reliable than pure batch, because it will only process files or offsets that have not yet been committed to the checkpoint. For most enterprise use cases with latency requirements in the range of five to thirty minutes, micro-batch is the optimal choice.

Pure continuous streaming should only be chosen when the business requirement genuinely demands sub-minute data freshness and the organisation is prepared to invest in the operational tooling — consumer lag dashboards, alerting on checkpoint health, Kafka retention policy management — that running a continuous stream requires. Continuous streaming without adequate operational maturity is a significant operational risk.

Batch ingestion remains the right choice for daily or hourly warehouse loads, historical backfills, and any scenario where the data source does not support a streaming interface. The simplicity of batch should not be underestimated: a job that runs, succeeds or fails, and has a clear start and end is far easier to reason about than a long-running stream.

### See Also

- [Structured Streaming trigger types — Databricks](https://docs.databricks.com/en/structured-streaming/triggers.html)
- [Auto Loader trigger once and availableNow — Databricks](https://docs.databricks.com/en/ingestion/auto-loader/production.html)
- `ingestion_cookbook.md` — Structured Streaming and Auto Loader implementation examples

---

## Schema Evolution Strategy

### Overview

Schema evolution is the process by which a data pipeline handles changes to the structure of incoming data — new columns added by the source system, columns renamed or removed, or data type changes. Handling schema evolution incorrectly leads to pipeline failures, silently dropped columns, or corrupt downstream tables. Different ingestion methods have fundamentally different behaviour when schema changes occur, and the strategy must be chosen deliberately at design time rather than discovered at the point of failure.

### Per-Method Behaviour

| Method | Schema Evolution Support | Behaviour on Schema Change |
|--------|--------------------------|---------------------------|
| **Auto Loader** | Yes — configurable via `cloudFiles.schemaEvolutionMode` | `addNewColumns` (default): new columns are added to the target Delta table automatically, existing records have `null` for the new column. `rescue`: unexpected columns are captured in a `_rescued_data` JSON column rather than causing failure. `failOnNewColumns`: pipeline fails if a new column is detected — useful for environments where uncontrolled schema change is not acceptable. `none`: schema changes are ignored and new columns are silently dropped. |
| **COPY INTO** | No | COPY INTO uses the schema of the target Delta table. If the incoming files contain columns not present in the target table, those columns are silently dropped. If the incoming files are missing columns present in the target table, those columns are written as `null`. There is no mechanism to automatically evolve the target table schema. |
| **Structured Streaming (manual)** | Limited — requires explicit handling | By default, Structured Streaming with a defined schema will drop unknown columns. Schema inference at stream start will read the schema from the first batch; subsequent schema changes will cause the stream to fail unless the checkpoint is deleted and the stream is restarted with the new schema. |
| **Delta Live Tables** | Yes — DLT infers and evolves schema automatically for streaming tables | DLT will automatically add new columns to the managed Delta table when they appear in the source. Data quality expectations are evaluated after schema evolution, so expectations referencing columns that have not yet appeared will not fail the pipeline. |
| **Notebook Pattern** | Manual only | The schema must be explicitly defined by the author. Any schema change requires the notebook to be updated manually before the next run. |
| **JDBC** | No automatic evolution | JDBC reads use the schema inferred from the source table at read time. New source columns require a manual `ALTER TABLE ... ADD COLUMN` on the Delta target, or `mergeSchema` enabled on the write. |
| **SFTP (Native Connector)** | Dependent on file format | Behaviour follows the underlying file format reader (CSV, JSON, Parquet). For JSON sources with `cloudFiles.schemaEvolutionMode` compatible options, new columns can be handled similarly to Auto Loader. CSV sources with inferred schema may fail on new columns without explicit configuration. |
| **Lakeflow Connect** | Yes — automatic | All managed connectors automatically handle new and deleted columns unless you opt out. When a new column appears in the source, Databricks automatically ingests it on the next pipeline run. Rows prior to the schema change have `null` for the new column. |
| **dbt Seeds** | No | Seeds are loaded from a fixed CSV file with an inferred or explicitly declared schema in `schema.yml`. Schema changes require the CSV and schema declaration to be updated and a full `dbt seed --full-refresh` to be run. |

### Recommendations

For production pipelines using Auto Loader, the `addNewColumns` mode is appropriate for most bronze-layer ingestion where the goal is to land raw data completely and without loss. The `rescue` mode provides an additional safety net for sources with highly variable schemas, as it ensures no data is lost even when columns are entirely unexpected. The `failOnNewColumns` mode is appropriate for silver or gold layer pipelines where the downstream schema is tightly controlled and an unexpected column likely indicates a source system issue that requires investigation before proceeding.

Regardless of method, any column rename or column removal in the source is a breaking change that no method handles automatically without data loss. A column rename appears to the ingestion layer as the old column being dropped (written as `null`) and a new column being added. Downstream consumers that depend on the old column name will receive `null` values without any pipeline failure to alert them. Schema evolution strategies should therefore include a process for communicating source schema changes to downstream consumers, not just a technical mechanism for handling them in the pipeline.

For Data Vault 2.0 pipelines, the hash key and hashdiff columns in the staging layer are derived from source columns. Any change to the columns included in a hash key is a business logic change, not merely a schema change, and must be handled by deprecating the existing hub or satellite and creating a new one with the corrected hash definition.

### See Also

- [Auto Loader schema evolution — Databricks](https://docs.databricks.com/en/ingestion/auto-loader/schema.html)
- [Delta table schema evolution — Databricks](https://docs.databricks.com/en/delta/update-schema.html)
- [AutomateDV documentation — schema considerations](https://automate-dv.readthedocs.io/en/latest/)

---

## Raw Staging for Data Vault 2.0

### Overview

In a Data Vault 2.0 architecture, the staging layer is the entry point for all vault loading pipelines. It sits between the raw ingestion layer (bronze, where data lands exactly as received from the source) and the vault loading layer (where hubs, links, and satellites are populated). The staging layer is not a persistent storage layer — it is a transformation step, typically implemented as a dbt view or ephemeral model, that prepares raw source data for vault loading by deriving the surrogate keys and change-detection hashes that Data Vault structures depend on.

The AutomateDV `stage` macro (in dbt) handles the mechanical work of building a staging model: it derives hash keys from business key columns, computes hashdiff columns from descriptive attribute sets, injects ghost or default records, and applies any necessary null substitutions. Understanding the design decisions that must be made before the staging layer is built is essential — errors in staging hash logic propagate into every vault structure loaded from that stage and cannot be corrected without reprocessing historical data.

### Design Considerations

**Hash algorithm consistency.** All staging models across all sources must use the same hash algorithm. AutomateDV supports MD5 and SHA-256. MD5 is sufficient for most use cases and produces shorter hash values (16 bytes versus 32 bytes), which reduces storage and join cost across large vault tables. SHA-256 is appropriate when the organisation has a security policy that prohibits MD5 usage. The algorithm is set in `dbt_project.yml` as a variable (`hash: MD5` or `hash: SHA`) and applies globally. It cannot be changed after vault tables have been populated without reprocessing the entire vault.

**Column ordering in hash keys.** Hash keys are computed from an ordered concatenation of business key columns. The order of columns in the hash key definition must be fixed and documented at the time the staging model is first built. If the column order changes — even temporarily — the resulting hash values will differ from previously computed values, causing existing records in hubs and satellites to appear as new records on the next load. Column ordering conventions must be agreed and recorded in the project's data dictionary before the first load runs in any environment above development.

**Null substitution strategy.** Business key columns used in hash key derivation must not contain `null` values, because `null` in a concatenation produces unpredictable results depending on the concatenation method used. AutomateDV provides a `null_columns` parameter in the `stage` macro to substitute a placeholder value (typically an empty string or a domain-specific sentinel like `'UNKNOWN'`) for `null` business keys before hashing. The choice of placeholder must be agreed across all sources and must not conflict with legitimate business key values.

**Hashdiff column scope.** A hashdiff column is a hash of all descriptive (non-key) attribute columns in a satellite source. Its purpose is to detect row-level changes efficiently without comparing every column individually. The columns included in the hashdiff for each satellite must match exactly the columns that will be loaded into that satellite. Adding or removing a column from the hashdiff definition after the satellite has been populated will cause every existing record to be re-evaluated as changed on the next load, resulting in a large volume of new satellite records that represent no actual business change.

**Ghost record injection.** AutomateDV's `stage` macro supports injecting a ghost record — a row with surrogate placeholder values for all hash key columns — that represents the "unknown" or "not applicable" member in the vault. Ghost records allow fact tables and link satellites to reference a hub member that accounts for late-arriving dimensions without producing referential integrity violations.

### See Also

- [AutomateDV staging documentation](https://automate-dv.readthedocs.io/en/latest/tutorial/tut_staging/)
- [Data Vault 2.0 standard — Dan Linstedt](https://www.danlinstedt.com/solutions-2/data-vault-basics/)
- [AutomateDV dbt package — PyPI](https://pypi.org/project/dbt-automate-dv/)
- `ingestion_cookbook.md` — AutomateDV Stage Macro implementation example

---

## dbt Seeds as a Reference Ingestion Pattern

### Overview

dbt seeds are CSV files stored inside the dbt project repository (under the `seeds/` directory) that dbt loads into the data warehouse as tables when `dbt seed` is run. They are version-controlled alongside the project's transformation models, making them the most auditable and reproducible way to manage small, static reference data — country codes, currency codes, product status mappings, regional hierarchies, and similar lookup tables that change rarely and where the change history matters.

Seeds are not a general-purpose ingestion mechanism. They are appropriate for a narrow category of data and should not be used outside that category.

### When to Use / When to Avoid

Seeds are appropriate when all of the following conditions are met:

- The data is small. A practical upper limit is a few thousand rows. dbt seeds are loaded by reading the CSV file in the dbt process and generating a `CREATE OR REPLACE TABLE` statement — there is no bulk-load mechanism. Loading hundreds of thousands of rows via a seed is slow and ties up the dbt runner.
- The data changes rarely — at most a few times per year. Each time the data changes, an engineer must update the CSV, commit the change to the repository, open a pull request, and run `dbt seed` in the target environment.
- The change history matters. Because seed CSV files are committed to the version control repository, every change is traceable with a commit hash, author, date, and commit message.
- The data is truly static reference data, not a slowly changing dimension. A slowly changing dimension requires a purpose-built SCD pipeline, not a seed.

Seeds should be avoided when:

- The data volume exceeds a few thousand rows. For larger reference datasets, use COPY INTO or Auto Loader to load from cloud storage instead.
- The data is updated by a non-technical team who do not have access to the dbt repository or are not familiar with the pull request workflow.
- The data has a meaningful update frequency (weekly or more often). The overhead of the version control workflow does not justify itself for frequently changing data.
- The seed is being used as a workaround for a missing dimension table in the warehouse.

One significant advantage of seeds over other reference data patterns is that the seed CSV is testable with dbt's built-in test framework. Column-level uniqueness, not-null, accepted-values, and relationships tests can be declared in `schema.yml` alongside the seed definition and will run as part of `dbt test`.

### See Also

- [dbt seeds documentation](https://docs.getdbt.com/docs/build/seeds)
- [dbt seed configuration in dbt_project.yml](https://docs.getdbt.com/reference/seed-configs)
- `ingestion_cookbook.md` — dbt Seeds implementation example with sample CSV and compiled SQL

---

## Database Ingestion (JDBC)

### Overview

JDBC ingestion is the standard pattern for extracting data directly from relational database systems into Databricks. Unlike file-based ingestion, the source is a live database rather than files in cloud storage. Spark's JDBC data source reads data over a JDBC connection and materialises it as a DataFrame, which is then written to Delta Lake.

JDBC ingestion is a batch-pull mechanism. It does not support streaming or low-latency use cases. It is the correct pattern when the source system is a relational database that does not expose a file export, CDC feed, or event stream interface.

### Decision Criteria

| Factor | Guidance |
|--------|----------|
| **Full vs. incremental extract** | Full extract: read the entire table on every run. Simple to implement but expensive for large tables. Incremental extract: filter on a watermark column (`updated_at`, `inserted_at`, or a sequence ID) to read only changed rows since the last run. Requires a reliable, indexed watermark column in the source. |
| **Parallelism** | By default, a JDBC read is single-threaded — all rows are fetched through a single connection. For large tables, parallelism is configured via `numPartitions`, `partitionColumn`, `lowerBound`, and `upperBound`. Spark splits the read into N parallel queries, each fetching a non-overlapping range of the partition column. The partition column must be numeric or date-type and should be indexed on the source to avoid full table scans on every partition query. |
| **Source load** | Parallel JDBC reads issue multiple concurrent queries against the source database. On a production OLTP system this can cause contention. Schedule JDBC ingestion during off-peak hours and limit `numPartitions` accordingly. Use a read replica where available. |
| **Credential management** | JDBC connection strings include usernames and passwords. These must never be hardcoded in notebooks or job configurations. Store credentials in Databricks Secrets and reference them at runtime via `dbutils.secrets.get()`. |

### Trade-offs

The primary limitation of JDBC ingestion is scalability under parallel load. Increasing `numPartitions` improves Databricks-side throughput but places proportionally more load on the source database. There is no universally correct value — it must be tuned per source based on the database's available connections and query concurrency limits.

JDBC does not provide a change data capture (CDC) mechanism. Incremental JDBC ingestion based on a watermark column will miss hard deletes — rows that are physically removed from the source table will not appear in the incremental extract and will remain in the Delta target indefinitely unless a separate reconciliation job identifies and removes them. If full delete propagation is required, consider a CDC tool (Debezium, Qlik Replicate) that reads the database transaction log rather than querying the table directly.

### See Also

- [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/external-systems/jdbc)
- [Spark JDBC data source — Apache Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
- `ingestion_cookbook.md` — JDBC implementation with partition tuning examples

---

## Managed Ingestion

### Overview

Managed ingestion covers patterns where a connector service — either Databricks-native or third-party — handles extraction from source systems and delivers data to Delta tables. The key distinction within this category is whether data transits Databricks infrastructure or a third-party service.

- **Lakeflow Connect** — Databricks-native managed connectors for SaaS applications and databases. Runs on serverless compute within Databricks, governed by Unity Catalog, orchestrated by Lakeflow Jobs. Data does not leave Databricks infrastructure.
- **Partner Connectors (Fivetran, Airbyte)** — third-party managed services that extract data from source systems and land it in Delta tables. Data transits the connector vendor's infrastructure before arriving in Databricks.

For new implementations, Lakeflow Connect is the preferred choice where the source is on its connector catalogue, as it eliminates the data residency and governance complexity of third-party connectors.

### Lakeflow Connect

Lakeflow Connect provides managed connectors for ingesting data from SaaS applications and databases directly into Delta tables. As of March 2026, it is generally available for Salesforce, Workday, and SQL Server, with ServiceNow and Google Analytics available for additional sources. The connector catalogue continues to expand.

Key characteristics:

- **Fully native to Databricks.** Pipelines run on serverless compute, are governed by Unity Catalog, and are orchestrated by Lakeflow Jobs. No external infrastructure is required.
- **Incremental by default.** Lakeflow Connect uses incremental reads to ingest only changed data, reducing load on source systems and improving pipeline efficiency.
- **Automatic schema evolution.** New columns in the source are automatically added to the Delta target on the next pipeline run. Deleted columns are retained in Delta with `null` values going forward.
- **CI/CD support.** Pipelines can be deployed via Databricks Asset Bundles, enabling source control, code review, and environment promotion workflows.
- **Serverless pricing.** Cost is based on serverless DBU consumption during pipeline runs, not a per-row or per-record fee.

**When Lakeflow Connect is appropriate:**
- The source is a SaaS application or database on the connector catalogue
- The organisation wants a fully managed, Databricks-native pipeline with no third-party data transit
- Unity Catalog governance (lineage, access control, audit) must apply to the ingestion layer

**When Lakeflow Connect is not appropriate:**
- The required source is not yet on the connector catalogue — check the current list in the Databricks documentation
- The organisation has an existing investment in Fivetran or Airbyte with a catalogue that exceeds Lakeflow Connect's current offering

### Partner Connectors (Fivetran, Airbyte, and Partner Connect)

Partner connectors remain appropriate where the source is not supported by Lakeflow Connect or where an existing connector platform investment is in place. Configuration on the Databricks side is minimal: the connector service principal needs `CREATE TABLE` and `MODIFY` on the target schema. The connector lands data into Delta tables (typically in a bronze schema), after which normal transformation pipelines take over.

**Key governance consideration:** Partner connectors extract data using credentials that have access to the source system, and that data transits the connector vendor's infrastructure before arriving in Databricks. This must be assessed against data residency requirements, privacy regulations (GDPR, HIPAA), and contractual data handling obligations before deploying a partner connector for sensitive data. Obtain a Data Processing Agreement (DPA) from the connector vendor for any personally identifiable or regulated data.

### See Also

- [Lakeflow Connect overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- [Databricks Partner Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partner-connect/)
- [Fivetran Databricks connector documentation](https://fivetran.com/docs/destinations/databricks)
- [Airbyte Databricks destination documentation](https://docs.airbyte.com/integrations/destinations/databricks)
- `ingestion_cookbook.md` — Lakeflow Connect and Partner Connector implementation examples
