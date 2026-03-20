# Cookbook Recommendations — Databricks

> **Scope:** Focused on native Databricks tooling — PySpark, Spark SQL, Delta Lake, Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT), Databricks Workflows, and Databricks Asset Bundles.

---

## Folder Layout

```
data_architecture/
├── cookbook_recommendations.md   ← this file
├── cookbook_template.md          ← template for new cookbooks
│
├── ingestion/
│   └── ingestion_cookbook.md
│
├── processing/
│   └── processing_cookbook.md
│
├── performance/
│   └── performance_cookbook.md
│
└── security/
    └── security_cookbook.md
```

---

## Document Summaries

### `ingestion/ingestion_cookbook.md`

Self-contained guide covering method selection and step-by-step implementation for all native ingestion methods. Includes decision tables for method selection, schema evolution, and batch vs. streaming trade-offs.

| Section | Research when you need to... |
|---------|------------------------------|
| Method Selection — Decision Criteria | Compare Auto Loader, COPY INTO, Structured Streaming, JDBC, and Lakeflow Connect on latency, schema evolution, and operational complexity |
| Method Selection — Schema Evolution Behaviour | Understand what each method does when a new column arrives in source data |
| Method Selection — Batch vs. Streaming | Choose between batch, micro-batch, and continuous streaming based on latency, cost, and failure recovery requirements |
| File Ingestion — Auto Loader | Incrementally ingest files from ADLS Gen2 into Delta Lake without reprocessing |
| File Ingestion — COPY INTO | Perform an idempotent batch load from files already in cloud storage |
| Streaming Ingestion — Structured Streaming | Consume events from Azure Event Hubs or Kafka at low latency |
| Database Ingestion — JDBC | Extract from a relational database (SQL Server, PostgreSQL, MySQL, Oracle) |
| Managed Ingestion — Lakeflow Connect | Ingest from SaaS applications (Salesforce, Workday, ServiceNow, Google Analytics) |
| Sources Not Covered | Route any other source type (SFTP, ERP, mainframe, custom API) through an ADLS Gen2 landing zone |

---

### `processing/processing_cookbook.md`

Self-contained guide covering pipeline design decisions and step-by-step implementation for Delta Lake transformations, aggregations, SCD patterns, incremental load strategies, and pipeline orchestration.

| Section | Research when you need to... |
|---------|------------------------------|
| Design Decisions — Medallion Layer Responsibilities | Understand what Bronze, Silver, and Gold layers should and should not own |
| Design Decisions — Orchestration: Jobs vs. SDP | Choose between Databricks Jobs and Lakeflow Spark Declarative Pipelines for a new pipeline |
| Design Decisions — Transformation Tool | Choose between PySpark, Spark SQL, and SDP as the transformation interface |
| Delta Lake Transformations — MERGE INTO | Upsert records into a Delta table based on a business key |
| Delta Lake Transformations — UPDATE and DELETE | Correct or remove records already written to a Delta table |
| Aggregations and Summarization | Compute aggregates, running totals, and ranked values using window functions |
| Data Cleansing and Deduplication | Standardise types, handle nulls, and remove duplicate records |
| Slowly Changing Dimensions — SCD Type 1 | Maintain current state of a slowly changing attribute with no history |
| Slowly Changing Dimensions — SCD Type 2 (Native Delta MERGE) | Track full history of attribute changes using a two-pass MERGE pattern |
| Slowly Changing Dimensions — SCD Type 2 via SDP | Track history declaratively in an SDP pipeline using `APPLY CHANGES INTO` |
| Joins and Enrichment | Enrich a fact stream or table with dimension attributes |
| Incremental Load Patterns — Databricks Jobs | Process only records changed since the last run (append, MERGE, or partition overwrite) |
| Reusable Transformation Logic | Share Python functions and Spark SQL UDFs across notebooks and jobs |
| Date Spine Generation | Generate a complete date sequence in native Spark for time-series joins and gap-filling |
| Lakeflow SDP — Multi-Hop Pipeline | Build a declarative Bronze → Silver pipeline with quality rules and managed cluster lifecycle |
| Pipeline Orchestration — Databricks Jobs and Asset Bundles | Define and deploy multi-task Databricks Jobs using Asset Bundles (YAML) |

---

### `performance/performance_cookbook.md`

Self-contained guide covering storage optimisation decisions and step-by-step implementation for Delta Lake tuning, query optimisation, and compute configuration.

| Section | Research when you need to... |
|---------|------------------------------|
| Design Decisions — Delta Lake Storage Optimization | Choose between OPTIMIZE/ZORDER, Liquid Clustering, and static partitioning for a table's access pattern |
| Design Decisions — Compute Selection | Decide whether to use a Job cluster, all-purpose cluster, or SQL Warehouse for a workload |
| Design Decisions — AQE Configuration Reference | Understand which Spark optimisations are automatic and which require explicit configuration |
| Design Decisions — Photon Eligibility | Confirm which operations in your pipeline will benefit from Photon |
| Delta Lake Optimization — OPTIMIZE and Compaction | Fix small file accumulation on tables written frequently by streaming or micro-batch jobs |
| Delta Lake Optimization — ZORDER Clustering | Speed up filtered queries on 1–3 high-cardinality columns |
| Delta Lake Optimization — Liquid Clustering | Apply automatic, rewrite-free clustering to tables with evolving or mixed access patterns |
| Delta Lake Optimization — VACUUM | Reclaim storage from old Delta table snapshots and expired file versions |
| Partitioning Strategy | Partition a large table on a low-cardinality column to enable partition pruning |
| Adaptive Query Execution (AQE) | Configure partition coalescing, skew join optimisation, and broadcast join conversion |
| Photon Engine | Enable and verify vectorised execution for compute-heavy SQL workloads |
| Caching | Decide when `cacheTable`, `.cache()`, or `.persist()` helps vs. wastes memory |
| Cluster Sizing and Autoscaling | Right-size a cluster for ETL vs. ad hoc workloads; configure autoscaling safely |
| Statistics and Predicate Pushdown | Run `ANALYZE TABLE` and use column pruning to avoid full scans |
| Native Incremental Loading with DeltaTable MERGE | Use the DeltaTable API for performance-optimised incremental MERGE patterns |
| Lakeflow SDP Pipeline Performance | Tune SDP pipeline cluster size, pipeline mode, and checkpoint configuration |

---

### `security/security_cookbook.md`

Self-contained guide covering governance design decisions and step-by-step implementation for Unity Catalog access control, secrets management, row-level security, column masking, tagging, audit logging, and encryption.

| Section | Research when you need to... |
|---------|------------------------------|
| Design Decisions — UC Native Features vs. Dynamic Views | Choose between Unity Catalog native row filters/column masks and dynamic views |
| Design Decisions — Role Reference | Understand the privilege sets for admin, data steward, and data consumer roles across the Unity Catalog hierarchy |
| Design Decisions — Pipeline Service Principal Privileges | Define the minimum privilege set for a service principal scoped to Medallion pipeline layers |
| Design Decisions — Security Enforcement by Layer | Decide where to enforce RBAC, RLS, and masking in a Bronze → Silver → Gold pipeline |
| RBAC with Unity Catalog | Grant and revoke privileges on catalogs, schemas, and tables for users and service principals |
| Secrets Management | Store and retrieve credentials, API keys, or connection strings securely using `dbutils.secrets` |
| Row-Level Security (RLS) with Dynamic Views | Restrict which rows a user sees based on group membership or column value |
| Column Masking with Dynamic Views | Mask PII or sensitive column values for non-privileged users via a view layer |
| Unity Catalog Native Column Masks | Apply column-level masking directly on the base table without a separate view (UC Premium required) |
| Unity Catalog Tag Management | Tag tables and columns with classification labels for governance and data discovery |
| Audit Logging | Query Databricks audit logs via `system.access.audit` to trace data access and permission changes |
| Data Encryption | Configure at-rest encryption defaults and customer-managed keys (CMK) for ADLS Gen2 |
