# Processing, Summarizing, and Transformation Architectural Patterns

## Overview

This document describes the key architectural patterns used for processing, summarising, and transforming data on Databricks. It is a **decision and design reference** — not a step-by-step implementation guide. For implementation details, see the [Processing, Summarising, and Transformation Cookbook](./processing_cookbook.md).

The patterns covered here are:

- **Medallion Architecture** — the canonical layered data organisation pattern for Delta Lake
- **Medallion vs. Data Vault 2.0** — when to choose one over the other, and how to combine them
- **dbt Project Layer Mapping** — how dbt model types map to medallion and Data Vault 2.0 layers
- **SCD vs. Satellite Design** — history tracking strategies and their philosophical differences
- **PySpark vs. Spark SQL vs. dbt** — choosing the right transformation tool for the job

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
- SCD Type 2 patterns for dimension tracking belong in Silver when dimensions are shared across domains.
- Silver is the foundation layer for most analytical workloads — it should be trusted, typed, and stable.
- Schema changes require a versioned migration, not silent drift.

**Gold — Aggregated and Business-Ready Layer**

- Contains aggregated, domain-specific views of the data: fact tables, aggregated metrics, wide denormalised tables optimised for BI tools.
- Directly consumed by dashboards, reports, ML feature stores, and APIs.
- Business logic — including KPI definitions, fiscal calendar adjustments, and attribution rules — lives here, not in Silver.
- Gold tables are often partitioned and Z-ORDERed by the dimensions most frequently filtered by end consumers.
- Multiple Gold schemas may exist, each owned by a different business domain (e.g., `gold_finance`, `gold_marketing`), all built from the same Silver foundation.

### When to Add or Collapse Layers

**Adding a layer — Quarantine / Rejected Records**

When data quality failures at Bronze-to-Silver are frequent enough to require operational attention, a quarantine layer (sometimes called Bronze+, Raw Staging, or Rejected) is warranted. Malformed or unresolvable records are written to the quarantine layer rather than silently dropped or held in Bronze. This layer:

- Enables operational alerting when rejection rates exceed thresholds.
- Allows reprocessing when the upstream issue is resolved.
- Keeps Silver clean without losing any source data.

A quarantine layer is most valuable when: source systems are unreliable, data quality SLAs exist, or downstream consumers require provenance for any rejected record.

**Adding a layer — Intermediate / Enriched Silver**

For complex pipelines with many joins and lookups between Silver sources, a named intermediate layer (Silver+, Enriched, or Integrated) can reduce the complexity of Gold logic and make the lineage clearer. This is optional and should only be added when the number of Silver-to-Gold transformations is large enough that Gold models become difficult to reason about without intermediate materialisation.

**Collapsing layers**

For small teams, simple pipelines, or bounded domains with a single source system, the Bronze-Silver boundary can be collapsed. If source data is already clean, typed, and deduplicated at ingestion (e.g., a CDC feed from a well-governed OLTP system), writing directly to a single cleansed layer with ingestion metadata is acceptable. The cost of the Bronze layer in this case — storage, latency, and pipeline maintenance — may outweigh the benefit of the full audit record.

Never collapse Bronze and Gold. The separation of raw ingestion from business-logic aggregation is the core architectural invariant of Medallion.

### Data Contracts Between Layers

A data contract at a layer boundary defines what the consuming layer can depend on. Without explicit contracts, schema drift in Bronze silently breaks Silver, and business logic changes in Silver silently break Gold.

At each layer boundary, agree on and document:

| Contract Element | Bronze → Silver | Silver → Gold |
|---|---|---|
| Schema | Inferred or explicit; Silver documents the canonical typed schema it expects | Silver publishes a stable, versioned schema; Gold documents which columns it depends on |
| Null policy | Bronze accepts nulls freely | Silver defines which columns are non-nullable and the handling for violations |
| Deduplication key | Not applied in Bronze | Silver defines the deduplication key and the tiebreaking rule (e.g., latest by `event_timestamp`) |
| Latency SLA | Defined by ingestion pattern (e.g., micro-batch every 5 minutes) | Defined by business reporting cadence (e.g., hourly refresh for Gold) |
| Breaking change protocol | Bronze may change structure; Silver must handle or quarantine | Silver schema changes require versioning and migration before Gold is updated |

**The cardinal rule: no business logic in Bronze.** Business logic includes KPI calculations, fiscal period assignments, domain-specific aggregations, and any transformation whose definition is owned by the business rather than by the ingestion pipeline. Placing business logic in Bronze couples the raw record format to business rules, making both harder to change independently.

### See Also

- [Databricks Medallion Architecture overview](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake best practices](https://docs.databricks.com/en/delta/best-practices.html)
- [Processing, Summarising, and Transformation Cookbook](./processing_cookbook.md)

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
- The team does not have prior Data Vault experience.
- Schema flexibility and ease of onboarding new sources are more important than strict modelling discipline.

Choose **Data Vault 2.0** when:

- Multiple source systems feed the same business entities (e.g., customer records from CRM, ERP, and a legacy billing system all need to be integrated under a single business key).
- A full, immutable audit trail of every change to every attribute is a compliance or regulatory requirement.
- Pipeline load parallelism is critical — DV2's append-only, independently loadable Hub/Link/Sat structure allows highly parallel loading without cross-table locking.
- The team has existing Data Vault modelling experience and tooling (e.g., AutomateDV/dbtvault).
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
2. Feed a Data Vault integration layer from Silver. Silver's cleansed, typed, deduplicated records are the ideal input to Hub, Link, and Satellite loading — they are clean enough to load without further quality handling.
3. Build Information Marts from the Data Vault. Information Marts replace the Gold layer for DV-modelled domains. For non-DV domains (e.g., a simple single-source feed), a standard Gold aggregation layer is still appropriate.

This hybrid avoids forcing a pure DV model on simple ingestion scenarios while preserving the DV's strengths for complex multi-source integration.

### See Also

- [Data Vault Alliance — Data Vault 2.0 overview](https://www.datavaultalliance.com/info/what-is-data-vault-2-0/)
- [AutomateDV (dbtvault) documentation](https://automate-dv.readthedocs.io/en/latest/)
- [Medallion vs. Data Vault on Databricks — community discussion](https://community.databricks.com)
- [Data Vault cookbook](../data_vault/)

---

## dbt Project Layer Mapping

### Overview

dbt organises transformation logic into model types that correspond loosely to transformation stages. In practice, teams use dbt's model directory structure and materialisation settings to implement Medallion layers or Data Vault 2.0 constructs. This section documents the conventional mappings so that dbt project structure is predictable and consistent with the underlying architectural pattern.

dbt itself is not opinionated about which architectural pattern you implement — it provides the execution framework, materialisation options, testing, and lineage. The architectural pattern is expressed through how you name, organise, and configure your models.

### Mapping to Medallion

| dbt Model Type | Medallion Layer | Responsibility |
|---|---|---|
| Sources (`sources.yml`) | Bronze (reference only) | dbt does not own Bronze loading; sources declare the Bronze tables that dbt reads from |
| Staging models (`models/staging/`) | Silver — cleansing | One staging model per source table; casts types, renames columns to standard names, handles nulls, applies deduplication |
| Intermediate models (`models/intermediate/`) | Silver — integration | Joins and enrichment across multiple staging models from the same or different sources |
| Mart models (`models/marts/`) | Gold | Aggregated, domain-specific fact and dimension tables for BI consumption |

**Materialisation guidance:**

- Staging models: `view` or `ephemeral` unless they are reused by many downstream models, in which case `table` or `incremental`.
- Intermediate models: `ephemeral` or `view` for simple enrichment; `table` for complex joins that are expensive to recompute.
- Mart models: `table` or `incremental` — these are the production assets consumed by BI tools.

### Mapping to Data Vault 2.0

| dbt Model Type | Data Vault 2.0 Construct | Notes |
|---|---|---|
| Staging models (`models/staging/`) | Raw Staging (RSA) | Adds hash keys, load timestamps, and record source. Uses AutomateDV `stage` macro |
| Hub models (`models/raw_vault/hubs/`) | Hubs | Loads distinct business keys. Uses AutomateDV `hub` macro |
| Link models (`models/raw_vault/links/`) | Links | Captures relationships between Hubs. Uses AutomateDV `link` macro |
| Satellite models (`models/raw_vault/satellites/`) | Satellites | Tracks descriptive attribute history. Uses AutomateDV `sat` macro |
| Mart models (`models/information_mart/`) | Information Mart | Joins and aggregates Raw Vault constructs into business-consumable views or tables |

**Key difference from Medallion mapping:** In a DV2 project, staging models are not Silver in the Medallion sense — they are the DV2 raw staging layer, which adds hash keys and metadata but does not perform business cleansing. The cleansing responsibility either sits in the Silver layer upstream of dbt (if using the hybrid approach) or is handled within the staging model itself.

### See Also

- [dbt best practices — project structure](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
- [AutomateDV dbt project setup](https://automate-dv.readthedocs.io/en/latest/tutorial/tut_getting_started/)
- [dbt-databricks adapter documentation](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)

---

## SCD vs. Satellite Design

### Overview

Both Slowly Changing Dimensions (SCD) and Data Vault Satellites solve the same fundamental problem: how to track the history of changes to descriptive attributes over time. They solve it with different philosophical approaches, different structural conventions, and different implications for data quality, performance, and auditability.

This is a design decision made at the Silver layer or Raw Vault layer, not at the Gold/Information Mart layer. Both patterns can feed the same downstream aggregations.

### When to Use SCD

Use SCD Type 2 when:

- The architectural pattern in use is Medallion (not Data Vault).
- The team is familiar with SCD conventions and the tooling (Delta MERGE, dbt snapshots) is already in place.
- The dimension being tracked is used directly in BI tools without a Data Vault join path between the Raw Vault and the Information Mart.
- The history requirement is analytical — understanding what a customer's status was at the time of a transaction — rather than a compliance requirement for a full immutable record of every system event.
- Record corrections are acceptable. SCD Type 2 allows an existing historical record to be corrected (e.g., fixing a typo in a name for records that were previously active). This makes the history mutable but practically correct.

**SCD Type 1** (overwrite, no history): use when history is not needed and the latest value is always the authoritative value. Appropriate for frequently corrected reference data where the old value has no analytical meaning (e.g., correcting a postal code lookup table).

**SCD Type 2** (versioned rows, effective date ranges): use when the history of changes is analytically significant — for example, when you need to report what segment a customer belonged to at the time of a purchase.

### When to Use Satellites

Use Satellite design when:

- The architectural pattern in use is Data Vault 2.0.
- A full, immutable audit trail is a compliance or regulatory requirement. Satellites are append-only — no record is ever updated or deleted, which means every state the system has ever been in is permanently preserved.
- High-volume parallel loading is needed. Because Satellites load independently from Hubs and Links (no cross-table joins required at load time), they can be loaded in parallel at scale without transaction contention.
- The team needs to track attribute history from multiple source systems independently. Multiple Satellites can be attached to a single Hub, each tracking attributes from a different source — a structure that SCD cannot replicate cleanly.
- Retroactive corrections must be distinguished from genuine business changes. In a Satellite, a correction is a new record with a new load timestamp and a record source indicating the correction origin. The original record is never changed.

### Key Philosophical Differences

| Dimension | SCD Type 2 | Satellite |
|---|---|---|
| Mutability | Mutable — historical records can be corrected by updating `end_date` and inserting a replacement | Immutable — no record is ever updated or deleted |
| Audit guarantee | Best-effort — history reflects the current understanding of what was true | Absolute — every load event is permanently preserved |
| Load mechanism | MERGE (insert new version, expire old version in a single operation) | Append-only INSERT — no MERGE required |
| Source system isolation | One SCD table per attribute group, typically | Multiple Satellites per Hub, one per source system if needed |
| Query complexity | Simple — filter on `is_current = true` or between effective dates | Requires join through Hub and Link; Information Mart layer hides complexity for consumers |
| Correction model | Correct the record; history reflects the corrected understanding | All states are preserved; the correction is a new record, not a replacement |
| Appropriate architecture | Medallion | Data Vault 2.0 |

### See Also

- [Delta Lake SCD patterns — Databricks documentation](https://docs.databricks.com/en/delta/slowly-changing-data.html)
- [AutomateDV Satellite documentation](https://automate-dv.readthedocs.io/en/latest/objects/satellites/)
- [dbt snapshots documentation](https://docs.getdbt.com/docs/build/snapshots)
- [Processing, Summarising, and Transformation Cookbook — SCD Type 2](./processing_cookbook.md)

---

## PySpark vs. Spark SQL vs. dbt

### Overview

On Databricks, there are three primary transformation tools, each with distinct strengths. They are not mutually exclusive — most mature pipelines use more than one. The decision of which tool to use for a given transformation is driven by the nature of the logic, the team's skills, and the operational requirements of the pipeline.

### Decision Criteria

**PySpark**

Use PySpark when:

- The transformation involves complex procedural logic that cannot be expressed cleanly as declarative SQL (e.g., iterative algorithms, conditional branching based on runtime data, dynamic schema generation).
- The pipeline includes ML feature engineering or model inference integrated into the transformation step.
- The pipeline uses Structured Streaming — PySpark's streaming API (`readStream`, `writeStream`, `foreachBatch`) is the primary interface for streaming workloads. Spark SQL can be used within a streaming pipeline but PySpark owns the stream topology.
- The team has strong Python skills and limited SQL depth.
- Low-level control over Spark execution (e.g., custom partitioning, broadcast join hints in code, UDF/UDAF registration) is needed.

**Spark SQL**

Use Spark SQL when:

- The transformation is a straightforward SELECT, JOIN, GROUP BY, or WINDOW operation that any analyst familiar with ANSI SQL can read and maintain.
- Ad hoc exploration or prototyping is the goal — Spark SQL in a notebook is fast to write and iterate.
- The team includes analysts who are not Python developers. SQL lowers the contribution barrier.
- The transformation is being developed in a Databricks SQL notebook or a SQL Warehouse query, where PySpark is not available.
- Readability and accessibility for audit or compliance review are important — SQL is universally readable.

**dbt**

Use dbt when:

- The transformation pipeline requires systematic testing, documentation, and lineage that is checked into version control and runs in CI/CD.
- Data quality is a first-class concern — dbt's built-in test framework (`unique`, `not_null`, `accepted_values`, `relationships`) provides a structured way to assert data contracts at each model boundary.
- The team is building a long-lived, maintained transformation layer that multiple people contribute to over time.
- Incremental materialisation strategies need to be configurable and reusable across models without writing bespoke MERGE logic for each table.
- The organisation uses a data catalogue or lineage tool (e.g., Databricks Unity Catalog lineage, DataHub, Atlan) that integrates with dbt's manifest or metadata artifacts.

**Important constraint:** dbt SQL models run on a Databricks SQL Warehouse or Spark cluster via the dbt-databricks adapter. dbt Python models run on a Spark cluster. A SQL Warehouse must be provisioned and the `http_path` configured in `profiles.yml` before dbt SQL models can run.

**Tool selection summary:**

| Criterion | PySpark | Spark SQL | dbt |
|---|---|---|---|
| Complex procedural logic | Best | Poor | Poor |
| Streaming pipelines | Best | Limited | Not supported |
| ML feature engineering | Best | Poor | Poor |
| Simple analytical transformations | Good | Best | Good |
| Ad hoc exploration | Good | Best | Poor |
| Tested, version-controlled pipelines | Manual | Manual | Best |
| Data quality enforcement | Manual | Manual | Best |
| CI/CD integration | Manual | Manual | Best |
| Lineage and documentation | Manual | Manual | Best |
| Analyst-accessible SQL | Poor | Best | Good |

### See Also

- [PySpark API documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks SQL reference](https://docs.databricks.com/en/sql/language-manual/index.html)
- [dbt-databricks adapter documentation](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
- [dbt Python models on Databricks](https://docs.getdbt.com/docs/build/python-models)
- [Processing, Summarising, and Transformation Cookbook](./processing_cookbook.md)
