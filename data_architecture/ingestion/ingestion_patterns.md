# Ingestion Architectural Patterns

## Overview

This document describes the architectural patterns and design decisions that govern data ingestion on Databricks. It is a decision and design reference, not a step-by-step implementation guide. Step-by-step examples for each method are found in `ingestion_cookbook.md` in the same directory.

This document is intended for data architects, senior data engineers, and technical leads who are selecting or reviewing ingestion patterns for a Databricks-based data platform. It covers method selection criteria, batch versus streaming trade-offs, schema evolution strategies, raw staging for Data Vault 2.0 pipelines, and the appropriate use of dbt seeds as a reference data ingestion pattern.

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

### Trade-offs

Auto Loader and Structured Streaming both use Spark's checkpoint mechanism, which provides exactly-once guarantees by tracking which files or offsets have been processed. This is powerful but requires that the checkpoint directory be durable (on cloud storage, not ephemeral local storage), and that it is never deleted unless you intend to reprocess from the beginning. COPY INTO tracks processed files inside the Delta table's transaction log, making it simpler to reason about — no external checkpoint directory is needed — but this also means that COPY INTO's tracking is tied to that specific Delta table and cannot be reused if the table is recreated.

Delta Live Tables sits above all of these methods in the abstraction stack. It manages checkpointing, retries, and cluster lifecycle automatically. The cost of that abstraction is reduced control: DLT pipelines run on DLT-managed clusters with fixed configuration options, and trigger timing is controlled by the pipeline's continuous or triggered mode rather than by arbitrary cron logic. Teams with strong Spark expertise and a need for fine-grained control will find DLT limiting; teams that want operational simplicity and built-in data quality will find it the right choice.

The Notebook Pattern has no place in production recurring ingestion. It has no state tracking, no deduplication guarantee, and no audit trail beyond the notebook run history. It is documented here to be explicitly excluded from production use cases, not to endorse it.

### See Also

- [Auto Loader documentation — Databricks](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [COPY INTO documentation — Databricks](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html)
- [Delta Live Tables overview — Databricks](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Structured Streaming programming guide — Apache Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
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
| **Delta Live Tables** | Yes — DLT infers and evolves schema automatically for streaming tables | DLT will automatically add new columns to the managed Delta table when they appear in the source. Data quality expectations (`@dlt.expect`, `@dlt.expect_or_drop`) are evaluated after schema evolution, so expectations referencing columns that have not yet appeared will not fail the pipeline. |
| **Notebook Pattern** | Manual only | The schema must be explicitly defined by the author. Any schema change requires the notebook to be updated manually before the next run. This is a significant maintenance burden and a source of production incidents. |
| **dbt Seeds** | No | Seeds are loaded from a fixed CSV file with an inferred or explicitly declared schema in `schema.yml`. Schema changes require the CSV and schema declaration to be updated and a full `dbt seed --full-refresh` to be run. |

### Recommendations

For production pipelines using Auto Loader, the `addNewColumns` mode is appropriate for most bronze-layer ingestion where the goal is to land raw data completely and without loss. The `rescue` mode provides an additional safety net for sources with highly variable schemas, as it ensures no data is lost even when columns are entirely unexpected. The `failOnNewColumns` mode is appropriate for silver or gold layer pipelines where the downstream schema is tightly controlled and an unexpected column likely indicates a source system issue that requires investigation before proceeding.

Regardless of method, any column rename or column removal in the source is a breaking change that no method handles automatically without data loss. A column rename appears to the ingestion layer as the old column being dropped (written as `null`) and a new column being added. Downstream consumers that depend on the old column name will receive `null` values without any pipeline failure to alert them. Schema evolution strategies should therefore include a process for communicating source schema changes to downstream consumers, not just a technical mechanism for handling them in the pipeline.

For Data Vault 2.0 pipelines, the hash key and hashdiff columns in the staging layer are derived from source columns. Any change to the columns included in a hash key is a business logic change, not merely a schema change, and must be handled by deprecating the existing hub or satellite and creating a new one with the corrected hash definition. This is not a limitation of the tooling — it is a fundamental property of the vault pattern.

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

**Null substitution strategy.** Business key columns used in hash key derivation must not contain `null` values, because `null` in a concatenation produces unpredictable results depending on the concatenation method used. AutomateDV provides a `null_columns` parameter in the `stage` macro to substitute a placeholder value (typically an empty string or a domain-specific sentinel like `'UNKNOWN'`) for `null` business keys before hashing. The choice of placeholder must be agreed across all sources and must not conflict with legitimate business key values. For example, using `'0'` as a null substitute for a numeric customer ID is only safe if `0` is not a valid customer ID in the source system.

**Hashdiff column scope.** A hashdiff column is a hash of all descriptive (non-key) attribute columns in a satellite source. Its purpose is to detect row-level changes efficiently without comparing every column individually. The columns included in the hashdiff for each satellite must match exactly the columns that will be loaded into that satellite. Adding or removing a column from the hashdiff definition after the satellite has been populated will cause every existing record to be re-evaluated as changed on the next load, resulting in a large volume of new satellite records that represent no actual business change.

**Ghost record injection.** AutomateDV's `stage` macro supports injecting a ghost record — a row with surrogate placeholder values for all hash key columns — that represents the "unknown" or "not applicable" member in the vault. Ghost records allow fact tables and link satellites to reference a hub member that accounts for late-arriving dimensions without producing referential integrity violations. Whether to inject ghost records must be decided at platform design time, as the placeholder hash values must be consistent across all hubs that the staging layer feeds.

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
- The data changes rarely — at most a few times per year. Each time the data changes, an engineer must update the CSV, commit the change to the repository, open a pull request, and run `dbt seed` in the target environment. This is the correct process for data that should be change-controlled, but it is too heavyweight for data that changes weekly.
- The change history matters. Because seed CSV files are committed to the version control repository, every change is traceable with a commit hash, author, date, and commit message. For regulatory reference data — for example, ISO country codes, financial instrument classifications, or tax rate tables — this auditability is valuable.
- The data is truly static reference data, not a slowly changing dimension. A slowly changing dimension (customer address history, product category hierarchy with effective dates) requires a purpose-built SCD pipeline, not a seed.

Seeds should be avoided when:

- The data volume exceeds a few thousand rows. For larger reference datasets, use COPY INTO or Auto Loader to load from cloud storage instead.
- The data is updated by a non-technical team (business analysts, operations staff) who do not have access to the dbt repository or are not familiar with the pull request workflow. In these cases, a Delta table loaded from a shared cloud storage location is more appropriate.
- The data has a meaningful update frequency (weekly or more often). The overhead of the version control workflow does not justify itself for frequently changing data.
- The seed is being used as a workaround for a missing dimension table in the warehouse. If the reference data properly belongs in a hub, satellite, or reference table in the vault, it should be loaded there through the appropriate vault loading pattern.

One significant advantage of seeds over other reference data patterns is that the seed CSV is testable with dbt's built-in test framework. Column-level uniqueness, not-null, accepted-values, and relationships tests can be declared in `schema.yml` alongside the seed definition and will run as part of `dbt test`, giving the platform the same data quality coverage for reference data as for modelled tables.

### See Also

- [dbt seeds documentation](https://docs.getdbt.com/docs/build/seeds)
- [dbt seed configuration in dbt_project.yml](https://docs.getdbt.com/reference/seed-configs)
- `ingestion_cookbook.md` — dbt Seeds implementation example with sample CSV and compiled SQL
