# Processing, Summarizing, and Transformation Architectural Patterns

## Databricks

> **Scope note:** This document covers patterns implemented entirely with PySpark, Spark SQL, Delta Lake, Delta Live Tables (DLT), and Databricks Asset Bundles.

---

## Overview

This document describes the key architectural patterns used for processing, summarising, and transforming data on Databricks using only native Databricks tooling. It is a **decision and design reference** — not a step-by-step implementation guide. For implementation details, see the [Processing, Summarising, and Transformation Cookbook (Native)](./processing_cookbook.md).

The patterns covered here are:

- **Medallion Architecture** — the canonical layered data organisation pattern for Delta Lake
- **Native Pipeline Layer Mapping** — how PySpark notebooks, DLT pipelines, and Databricks Jobs map to medallion layers
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
- [Processing, Summarising, and Transformation Cookbook](./processing_cookbook.md)

---

## Native Pipeline Layer Mapping

### Overview

In a native Databricks pipeline, transformation logic is organised into notebooks, PySpark scripts, Spark SQL scripts, DLT pipelines, and Databricks Jobs. This section documents how those artefacts map to Medallion layers so that pipeline structure is predictable and consistent.

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

**Common materialisation patterns:**

| Pattern | Native Databricks Implementation |
|---|---|
| View | Spark SQL `CREATE OR REPLACE VIEW` or Unity Catalog view |
| Full refresh table | `CREATE OR REPLACE TABLE AS SELECT` (CTAS) |
| Incremental merge | Delta MERGE INTO in a PySpark or SQL notebook task |
| Incremental append | `df.write.format("delta").mode("append").saveAsTable(...)` |
| Incremental partition overwrite | `df.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").saveAsTable(...)` |

### Mapping to Medallion — Delta Live Tables

| DLT Dataset Type | Medallion Layer | Notes |
|---|---|---|
| `STREAMING LIVE TABLE` with Auto Loader source | Bronze | Incremental ingestion from cloud storage; DLT manages checkpoints |
| `LIVE TABLE` or `STREAMING LIVE TABLE` with cleansing logic | Silver | `EXPECT` rules enforce quality; `EXPECT OR DROP` quarantines bad records |
| `APPLY CHANGES INTO` | Silver — SCD | Native DLT CDC / SCD Type 1 and SCD Type 2 without manual MERGE logic |
| `LIVE TABLE` with aggregations | Gold | DLT refreshes Gold tables when Silver dependencies update |

**`APPLY CHANGES INTO`** handles SCD Type 1 and SCD Type 2 history tracking automatically from a CDC source (Change Data Feed or a sequence-keyed source).

### See Also

- [Databricks Jobs documentation](https://learn.microsoft.com/en-us/azure/databricks/jobs/)
- [Delta Live Tables documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [DLT APPLY CHANGES INTO](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)

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
- Reusable transformation logic should be packaged as native Python functions (unit-testable with pytest).

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
- The team is building a long-lived, maintained transformation layer that multiple people contribute to over time (DLT provides declarative pipeline management with built-in quality rules, lineage, and observability).
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

**Reusable transformation logic:**

Native Python functions defined in a shared utility module (e.g., `src/transforms/fiscal_calendar.py`) and imported into PySpark notebooks, or Spark SQL User-Defined Functions (UDFs) registered with `spark.udf.register()`, provide reusable transformation logic. Both approaches are testable with pytest and deployable via Databricks Asset Bundles.

### See Also

- [PySpark API documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks SQL reference](https://docs.databricks.com/en/sql/language-manual/index.html)
- [Delta Live Tables documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Processing, Summarising, and Transformation Cookbook](./processing_cookbook.md)
