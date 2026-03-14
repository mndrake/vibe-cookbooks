# Processing, Summarizing, and Transformation Architectural Patterns — Native Databricks

> **Scope note:** This document is the **native Databricks version** of the processing patterns reference.
> It covers patterns implemented entirely with PySpark, Spark SQL, Delta Lake, Delta Live Tables (DLT),
> and Databricks Asset Bundles — no dbt dependency is required.
> For the dbt-integrated version of these patterns, see
> [../../../databricks_and_dbt/processing/processing_patterns.md](../../../databricks_and_dbt/processing/processing_patterns.md).

---

## Overview

This document describes the key architectural patterns used for processing, summarising, and transforming data on Databricks using only native Databricks tooling. It is a **decision and design reference** — not a step-by-step implementation guide. For implementation details, see the [Processing, Summarising, and Transformation Cookbook (Native)](./processing_cookbook.md).

The patterns covered here are:

- **Medallion Architecture** — the canonical layered data organisation pattern for Delta Lake
- **Medallion vs. Data Vault 2.0** — when to choose one over the other, and how to combine them
- **Native Pipeline Layer Mapping** — how PySpark notebooks, DLT pipelines, and Databricks Jobs map to medallion layers
- **SCD vs. Satellite Design** — history tracking strategies and their philosophical differences
- **PySpark vs. Spark SQL vs. DLT** — choosing the right native transformation tool for the job

---

## Medallion Architecture

### Overview

The Medallion Architecture is a data organisation pattern for Delta Lake that progressively increases data quality across three named layers: Bronze, Silver, and Gold. Each layer has a clear purpose, a defined set of transformations it is responsible for, and explicit contracts with the layers it depends on. The pattern is advocated by Databricks and is the default structural choice for most Lakehouse implementations.

The primary design goal of Medallion is **progressive trust** — raw data is preserved intact, and quality increases with each layer so that downstream consumers always work with the cleanest, most reliable data available to them.

### Layer Responsibilities

**Bronze — Raw Ingestion Layer**

- Stores data exactly as received from the source, including malformed records, schema drift, and duplicates.
- Schema is inferred or schema-on-read; no enforcement of business types at this layer.
- Append-only: records are never updated or deleted from Bronze. If a source correction arrives, it is a new record in Bronze.
- No business logic is applied. Transformations are limited to adding ingestion metadata (e.g., `_ingestion_timestamp`, `_source_file`, `_batch_id`).
- Typically stored in Delta format with Auto Loader or COPY INTO loading patterns.
- Serves as the permanent audit record of what was received and when.

**Silver — Cleansed and Conformed Layer**

- Applies type casting, null handling, deduplication, and schema enforcement.
- Business keys are validated and standardised (e.g., consistent casing, trimmed whitespace).
- Joins across source systems may be applied here when the join is structural rather than analytical (e.g., joining an order header with order lines from the same source).
- SCD Type 2 patterns for dimension tracking belong in Silver when dimensions are shared across domains. Native implementation uses Delta MERGE INTO (two-pass pattern) or DLT `APPLY CHANGES INTO`.
- Silver is the foundation layer for most analytical workloads — it should be trusted, typed, and stable.
- Schema changes require a versioned migration, not silent drift.

**Gold — Aggregated and Business-Ready Layer**

- Contains aggregated, domain-specific views of the data: fact tables, aggregated metrics, wide denormalised tables optimised for BI tools.
- Directly consumed by dashboards, reports, ML feature stores, and APIs.
- Business logic — including KPI definitions, fiscal calendar adjustments, and attribution rules — lives here, not in Silver.
- Gold tables are often partitioned and Z-ORDERed by the dimensions most frequently filtered by end consumers.
- Multiple Gold schemas may exist, each owned by a different business domain (e.g., `gold_finance`, `gold_marketing`), all built from the same Silver foundation.
- Orchestrated by Databricks Jobs (task clusters or serverless) or by DLT Gold-layer datasets.

### When to Add or Collapse Layers

**Adding a layer — Quarantine / Rejected Records**

When data quality failures at Bronze-to-Silver are frequent enough to require operational attention, a quarantine layer (sometimes called Bronze+, Raw Staging, or Rejected) is warranted. Malformed or unresolvable records are written to the quarantine layer rather than silently dropped or held in Bronze. This layer:

- Enables operational alerting when rejection rates exceed thresholds.
- Allows reprocessing when the upstream issue is resolved.
- Keeps Silver clean without losing any source data.

In native Databricks pipelines, the quarantine layer is typically a separate Delta table written in the same Job task that performs Bronze-to-Silver cleansing, using a conditional `filter` / `otherwise` branch in PySpark or a `CASE` expression in SQL.

**Adding a layer — Intermediate / Enriched Silver**

For complex pipelines with many joins and lookups between Silver sources, a named intermediate layer (Silver+, Enriched, or Integrated) can reduce the complexity of Gold logic and make the lineage clearer. In native pipelines, this is expressed as an intermediate DLT dataset or a dedicated Job task writing to a `catalog.silver_enriched` schema.

**Collapsing layers**

For small teams, simple pipelines, or bounded domains with a single source system, the Bronze-Silver boundary can be collapsed. If source data is already clean, typed, and deduplicated at ingestion (e.g., a CDC feed from a well-governed OLTP system), writing directly to a single cleansed layer with ingestion metadata is acceptable.

Never collapse Bronze and Gold. The separation of raw ingestion from business-logic aggregation is the core architectural invariant of Medallion.

### Data Contracts Between Layers

A data contract at a layer boundary defines what the consuming layer can depend on. Without explicit contracts, schema drift in Bronze silently breaks Silver, and business logic changes in Silver silently break Gold.

| Contract Element | Bronze → Silver | Silver → Gold |
|---|---|---|
| Schema | Inferred or explicit; Silver documents the canonical typed schema it expects | Silver publishes a stable, versioned schema; Gold documents which columns it depends on |
| Null policy | Bronze accepts nulls freely | Silver defines which columns are non-nullable and the handling for violations |
| Deduplication key | Not applied in Bronze | Silver defines the deduplication key and the tiebreaking rule (e.g., latest by `event_timestamp`) |
| Latency SLA | Defined by ingestion pattern (e.g., micro-batch every 5 minutes) | Defined by business reporting cadence (e.g., hourly refresh for Gold) |
| Breaking change protocol | Bronze may change structure; Silver must handle or quarantine | Silver schema changes require versioning and migration before Gold is updated |

In native pipelines, contracts are enforced via Unity Catalog table constraints (`ALTER TABLE ... ADD CONSTRAINT`), DLT `EXPECT` and `EXPECT OR DROP` quality rules, and Databricks Jobs health checks (row count assertions after each task).

**The cardinal rule: no business logic in Bronze.** Business logic includes KPI calculations, fiscal period assignments, domain-specific aggregations, and any transformation whose definition is owned by the business rather than by the ingestion pipeline.

### See Also

- [Databricks Medallion Architecture overview](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake best practices](https://docs.databricks.com/en/delta/best-practices.html)
- [Processing, Summarising, and Transformation Cookbook (Native)](./processing_cookbook.md)

---

## Medallion vs. Data Vault 2.0

### Overview

Medallion and Data Vault 2.0 (DV2) are both layered data organisation patterns, but they solve different problems and reflect different priorities.

**Medallion** is optimised for simplicity, speed of delivery, and BI-focused consumption. It is the right default for most Lakehouse projects.

**Data Vault 2.0** is a formal modelling methodology optimised for enterprise-scale integration of multiple source systems, full auditability, and parallel loading. It introduces a fixed set of model types — Hubs, Links, and Satellites — that decompose business keys, relationships, and descriptive attributes into separate tables with explicit loading rules.

### Decision Criteria

Choose **Medallion** when:

- The team is building a new Lakehouse and wants to move quickly to delivering BI value.
- The primary consumers are BI tools and analysts — not a downstream enterprise data warehouse.
- Source systems are few (one to three) or the data is domain-bounded.
- Schema flexibility and ease of onboarding new sources are more important than strict modelling discipline.

Choose **Data Vault 2.0** when:

- Multiple source systems feed the same business entities (e.g., customer records from CRM, ERP, and a legacy billing system all need to be integrated under a single business key).
- A full, immutable audit trail of every change to every attribute is a compliance or regulatory requirement.
- Pipeline load parallelism is critical — DV2's append-only, independently loadable Hub/Link/Sat structure allows highly parallel loading without cross-table locking.
- The downstream consumers include a formal reporting layer (Information Mart) that must be isolated from raw integration complexity.

**Key trade-off summary:**

| Dimension | Medallion | Data Vault 2.0 |
|---|---|---|
| Time to first delivery | Faster | Slower (modelling overhead upfront) |
| Multi-source integration | Manual join logic in Silver/Gold | Structured via Hubs and Links |
| Audit trail | Dependent on layer design | Built-in and immutable by design |
| Load parallelism | Limited by join dependencies | High — Hubs, Links, Sats load independently |
| Query complexity for consumers | Lower | Higher — Information Marts required for usability |
| Team skill requirement | General Spark/SQL | Data Vault methodology knowledge needed |

### Hybrid Approaches

A practical hybrid is common in organisations that need both rapid BI delivery and long-term enterprise integration capability:

1. Use Medallion for Bronze and Silver ingestion and cleansing. This preserves the simplicity of Auto Loader-based ingestion and Delta Lake quality guarantees.
2. Feed a Data Vault integration layer from Silver. Silver's cleansed, typed, deduplicated records are the ideal input to Hub, Link, and Satellite loading.
3. Build Information Marts from the Data Vault. Information Marts replace the Gold layer for DV-modelled domains. For non-DV domains, a standard Gold aggregation layer remains appropriate.

In a native Databricks implementation, each Hub, Link, and Satellite load is a separate Databricks Jobs task (PySpark notebook or DLT dataset) that appends to its target table. Task dependencies are expressed in the Jobs DAG.

### See Also

- [Data Vault Alliance — Data Vault 2.0 overview](https://www.datavaultalliance.com/info/what-is-data-vault-2-0/)
- [Medallion vs. Data Vault on Databricks — community discussion](https://community.databricks.com)
- [Data Vault cookbook](../data_vault/)

---

## Native Pipeline Layer Mapping

### Overview

In a native Databricks pipeline (without dbt), transformation logic is organised into notebooks, PySpark scripts, Spark SQL scripts, DLT pipelines, and Databricks Jobs. This section documents how those artefacts map to Medallion layers so that pipeline structure is predictable and consistent.

The two primary orchestration mechanisms are:

- **Databricks Jobs** — a DAG of tasks (notebooks, Python scripts, SQL scripts, Delta Live Tables pipelines, JAR tasks) with dependency management, scheduling, retry logic, and alerting. The standard choice for operational batch and streaming pipelines.
- **Delta Live Tables (DLT)** — a declarative pipeline framework where datasets are defined as SQL or Python `LIVE TABLE` or `STREAMING LIVE TABLE` definitions. DLT manages pipeline state, quality rules, lineage, and incremental processing automatically.

### Mapping to Medallion — Databricks Jobs

| Pipeline Artefact | Medallion Layer | Responsibility |
|---|---|---|
| Auto Loader / COPY INTO notebook | Bronze | Lands raw files as Delta tables; appends ingestion metadata |
| Cleansing and deduplication notebook | Silver — cleansing | Casts types, trims whitespace, handles nulls, deduplicates via ROW_NUMBER() |
| Enrichment / integration notebook | Silver — integration | Joins and enrichment across multiple Silver tables |
| Aggregation / Gold notebook | Gold | Aggregated, domain-specific fact and dimension tables |

Each notebook corresponds to one or more Databricks Jobs tasks. Task dependencies are declared in the Jobs task dependency graph. A failing upstream task prevents downstream tasks from running, which mirrors the contract enforcement between Medallion layers.

**Materialisation equivalence:**

| dbt materialisation | Native Databricks equivalent |
|---|---|
| `view` | Spark SQL `CREATE OR REPLACE VIEW` or Unity Catalog view |
| `table` | `CREATE OR REPLACE TABLE AS SELECT` (CTAS) |
| `incremental` (merge) | Delta MERGE INTO in a PySpark or SQL notebook task |
| `incremental` (append) | `df.write.format("delta").mode("append").saveAsTable(...)` |
| `incremental` (insert_overwrite) | `df.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").saveAsTable(...)` |

### Mapping to Medallion — Delta Live Tables

| DLT Dataset Type | Medallion Layer | Notes |
|---|---|---|
| `STREAMING LIVE TABLE` with Auto Loader source | Bronze | Incremental ingestion from cloud storage; DLT manages checkpoints |
| `LIVE TABLE` or `STREAMING LIVE TABLE` with cleansing logic | Silver | `EXPECT` rules enforce quality; `EXPECT OR DROP` quarantines bad records |
| `APPLY CHANGES INTO` | Silver — SCD | Native DLT CDC / SCD Type 1 and SCD Type 2 without manual MERGE logic |
| `LIVE TABLE` with aggregations | Gold | DLT refreshes Gold tables when Silver dependencies update |

**`APPLY CHANGES INTO` is the DLT-native equivalent of dbt snapshots** for SCD Type 1 and SCD Type 2 history tracking. It handles the expire-and-insert logic automatically from a CDC source (Change Data Feed or a sequence-keyed source).

### See Also

- [Databricks Jobs documentation](https://docs.databricks.com/aws/en/jobs/)
- [Delta Live Tables documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [DLT APPLY CHANGES INTO](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)

---

## SCD vs. Satellite Design

### Overview

Both Slowly Changing Dimensions (SCD) and Data Vault Satellites solve the same fundamental problem: how to track the history of changes to descriptive attributes over time. They solve it with different philosophical approaches, different structural conventions, and different implications for data quality, performance, and auditability.

This is a design decision made at the Silver layer or Raw Vault layer, not at the Gold/Information Mart layer. Both patterns can feed the same downstream aggregations.

### When to Use SCD (Native Databricks Implementation)

Use SCD Type 2 when:

- The architectural pattern in use is Medallion (not Data Vault).
- The team is familiar with SCD conventions and the tooling (Delta MERGE INTO two-pass pattern, or DLT `APPLY CHANGES INTO`) is already in place.
- The dimension being tracked is used directly in BI tools without a Data Vault join path.
- The history requirement is analytical — understanding what a customer's status was at the time of a transaction — rather than a compliance requirement for a full immutable record of every system event.

**Native implementation options:**

| Approach | When to Use |
|---|---|
| Two-pass Delta MERGE INTO (PySpark or SQL) | Batch pipelines; full control over SCD logic; no DLT dependency |
| DLT `APPLY CHANGES INTO` | Streaming or incremental pipelines; DLT manages SCD logic automatically from a CDC source |

**SCD Type 1** (overwrite, no history): implemented as a simple `MERGE INTO ... WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`.

**SCD Type 2** (versioned rows, effective date ranges): implemented as a two-pass MERGE (expire + insert) or via `APPLY CHANGES INTO STORED AS SCD TYPE 2` in DLT.

### When to Use Satellites

Use Satellite design when:

- The architectural pattern in use is Data Vault 2.0.
- A full, immutable audit trail is a compliance or regulatory requirement. Satellites are append-only — no record is ever updated or deleted.
- High-volume parallel loading is needed. Satellites load independently from Hubs and Links via separate Databricks Jobs tasks.
- The team needs to track attribute history from multiple source systems independently. Multiple Satellites can be attached to a single Hub.
- Retroactive corrections must be distinguished from genuine business changes.

### Key Philosophical Differences

| Dimension | SCD Type 2 | Satellite |
|---|---|---|
| Mutability | Mutable — historical records can be corrected by updating `end_date` and inserting a replacement | Immutable — no record is ever updated or deleted |
| Audit guarantee | Best-effort — history reflects the current understanding of what was true | Absolute — every load event is permanently preserved |
| Load mechanism | Two-pass MERGE or DLT `APPLY CHANGES INTO` | Append-only INSERT — no MERGE required |
| Source system isolation | One SCD table per attribute group, typically | Multiple Satellites per Hub, one per source system if needed |
| Query complexity | Simple — filter on `is_current = true` or between effective dates | Requires join through Hub and Link; Information Mart layer hides complexity |
| Correction model | Correct the record; history reflects the corrected understanding | All states are preserved; the correction is a new record |
| Appropriate architecture | Medallion | Data Vault 2.0 |

### See Also

- [Delta Lake SCD patterns — Databricks documentation](https://docs.databricks.com/aws/en/delta/merge)
- [DLT APPLY CHANGES INTO — SCD Type 2](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [Processing, Summarising, and Transformation Cookbook (Native) — SCD Type 2](./processing_cookbook.md)

---

## PySpark vs. Spark SQL vs. DLT

### Overview

On Databricks, there are three primary native transformation tools, each with distinct strengths. They are not mutually exclusive — most mature pipelines use more than one. The decision of which tool to use for a given transformation is driven by the nature of the logic, the team's skills, and the operational requirements of the pipeline.

### Decision Criteria

**PySpark**

Use PySpark when:

- The transformation involves complex procedural logic that cannot be expressed cleanly as declarative SQL (e.g., iterative algorithms, conditional branching based on runtime data, dynamic schema generation).
- The pipeline includes ML feature engineering or model inference integrated into the transformation step.
- The pipeline uses Structured Streaming — PySpark's streaming API (`readStream`, `writeStream`, `foreachBatch`) is the primary interface for streaming workloads.
- Low-level control over Spark execution (e.g., custom partitioning, broadcast join hints in code, UDF/UDAF registration) is needed.
- Reusable transformation logic should be packaged as native Python functions (equivalent to dbt macros, but unit-testable with pytest).

**Spark SQL**

Use Spark SQL when:

- The transformation is a straightforward SELECT, JOIN, GROUP BY, or WINDOW operation that any analyst familiar with ANSI SQL can read and maintain.
- Ad hoc exploration or prototyping is the goal — Spark SQL in a notebook is fast to write and iterate.
- The team includes analysts who are not Python developers. SQL lowers the contribution barrier.
- The transformation is being developed in a Databricks SQL notebook or a SQL Warehouse query.
- Readability and accessibility for audit or compliance review are important — SQL is universally readable.

**Delta Live Tables (DLT)**

Use DLT when:

- The transformation pipeline requires declarative quality rules (`EXPECT`, `EXPECT OR DROP`, `EXPECT OR FAIL`) that are enforced and tracked automatically with built-in quarantine metrics.
- SCD Type 2 history tracking is needed from a CDC source — `APPLY CHANGES INTO` eliminates manual MERGE logic.
- Pipeline lineage, data quality metrics, and incremental processing should be managed by the platform rather than bespoke notebook logic.
- The team is building a long-lived, maintained transformation layer that multiple people contribute to over time (DLT is the native Databricks equivalent of dbt's project management capabilities).
- The pipeline must handle both batch and streaming sources in the same DAG — DLT supports mixed source types transparently.

**Tool selection summary:**

| Criterion | PySpark | Spark SQL | DLT |
|---|---|---|---|
| Complex procedural logic | Best | Poor | Limited |
| Streaming pipelines | Best | Limited | Best (declarative) |
| ML feature engineering | Best | Poor | Poor |
| Simple analytical transformations | Good | Best | Good |
| Ad hoc exploration | Good | Best | Poor |
| Data quality enforcement | Manual | Manual | Best (native EXPECT rules) |
| SCD / CDC handling | Manual two-pass MERGE | Manual two-pass MERGE | Best (APPLY CHANGES INTO) |
| Pipeline lineage and metrics | Manual | Manual | Best (built-in) |
| Analyst-accessible SQL | Poor | Best | Good |
| CI/CD with Asset Bundles | Yes | Yes | Yes |

**Reusable logic without dbt macros:**

dbt macros are replaced by native Python functions defined in a shared utility module (e.g., `src/transforms/fiscal_calendar.py`) and imported into PySpark notebooks, or by Spark SQL User-Defined Functions (UDFs) registered with `spark.udf.register()`. Both approaches are testable with pytest and deployable via Databricks Asset Bundles.

### See Also

- [PySpark API documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks SQL reference](https://docs.databricks.com/en/sql/language-manual/index.html)
- [Delta Live Tables documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Processing, Summarising, and Transformation Cookbook (Native)](./processing_cookbook.md)
