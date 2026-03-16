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
│   ├── ingestion_cookbook.md
│   └── ingestion_patterns.md
│
├── processing/
│   ├── processing_cookbook.md
│   └── processing_patterns.md
│
├── performance/
│   ├── performance_cookbook.md
│   └── performance_patterns.md
│
└── security/
    ├── security_cookbook.md
    └── security_patterns.md
```

---

## Document Summaries

### `ingestion/ingestion_cookbook.md`

Step-by-step implementation guide for all native ingestion methods. Each method section contains a problem statement, Python and SQL examples, and discussion of concerns.

| Section | Research when you need to... |
|---------|------------------------------|
| File Ingestion — Auto Loader | Incrementally ingest files from ADLS/S3/GCS into Delta Lake without reprocessing |
| File Ingestion — COPY INTO | Perform a one-time or ad hoc bulk load from files already in cloud storage |
| File Ingestion — SFTP | Receive files from a partner or vendor system via SFTP |
| Streaming Ingestion — Structured Streaming | Consume events from Kafka, Azure Event Hubs, or Kinesis at low latency |
| Database Ingestion — JDBC | Extract from a relational database (SQL Server, PostgreSQL, MySQL, Oracle) |
| Managed Ingestion — Lakeflow Connect | Ingest from SaaS applications (Salesforce, Workday, ServiceNow) |
| Managed Ingestion — Partner Connectors | Integrate Fivetran or Airbyte as the managed ingestion layer |

---

### `ingestion/ingestion_patterns.md`

Architecture and design reference for ingestion decisions. Read this before committing to an ingestion pattern.

| Section | Research when you need to... |
|---------|------------------------------|
| Landing Zone Architecture | Understand when a cloud storage landing zone is required vs. when to bypass it |
| Ingestion Method Selection | Compare Auto Loader, COPY INTO, Structured Streaming, JDBC, and managed connectors side by side |
| Batch vs. Streaming Trade-offs | Choose between batch and streaming based on latency, cost, and operational complexity |
| Schema Evolution Strategy | Decide how to handle source schema changes for each ingestion method |
| Database Ingestion (JDBC) | Understand watermark strategies, parallelism, and push-down optimisation for JDBC sources |
| Managed Ingestion | Understand the trade-offs between Lakeflow Connect and third-party connectors |

---

### `processing/processing_cookbook.md`

Step-by-step implementation guide for Delta Lake transformations, aggregations, SCD patterns, and incremental load strategies.

| Section | Research when you need to... |
|---------|------------------------------|
| Lakeflow Spark Declarative Pipelines — Multi-Hop Pipeline | Build a declarative Bronze → Silver pipeline with quality rules, managed cluster lifecycle, and pipeline modes |
| Delta Lake Transformations — MERGE INTO | Upsert records into a Delta table based on a business key |
| Delta Lake Transformations — UPDATE and DELETE | Correct or remove records already written to a Delta table |
| Aggregations and Summarization | Compute aggregates, running totals, and ranked values using window functions |
| Data Cleansing and Deduplication | Standardise types, handle nulls, and remove duplicate records |
| Slowly Changing Dimensions — SCD Type 1 | Maintain current state of a slowly changing attribute with no history |
| Slowly Changing Dimensions — SCD Type 2 (Native Delta MERGE) | Track full history of attribute changes using a two-pass MERGE pattern |
| Slowly Changing Dimensions — SCD Type 2 via SDP | Track history declaratively in an SDP pipeline using `APPLY CHANGES INTO` |
| Joins and Enrichment | Enrich a fact stream or table with dimension attributes |
| Incremental Load Patterns — Databricks Jobs | Process only records changed since the last run (append, MERGE, or partition overwrite) |

---

### `processing/processing_patterns.md`

Architecture and design reference for transformation decisions. Read this when choosing between pipeline tools or structural patterns.

| Section | Research when you need to... |
|---------|------------------------------|
| Medallion Architecture | Understand the Bronze → Silver → Gold layering model and the responsibilities of each layer |
| Native Pipeline Layer Mapping | Map PySpark notebooks, SDP pipelines, and Databricks Jobs to Medallion layers |
| PySpark vs. Spark SQL vs. SDP | Choose the right native transformation tool for a given workload |

---

### `performance/performance_cookbook.md`

Step-by-step implementation guide for Delta Lake storage optimisation, compute configuration, and query tuning.

| Section | Research when you need to... |
|---------|------------------------------|
| Delta Lake Optimization — OPTIMIZE and Compaction | Fix small file accumulation on tables written frequently by streaming or micro-batch jobs |
| Delta Lake Optimization — ZORDER Clustering | Speed up filtered queries on 1–3 high-cardinality columns |
| Delta Lake Optimization — Liquid Clustering | Apply automatic, rewrite-free clustering to tables with evolving or mixed access patterns |
| Delta Lake Optimization — VACUUM | Reclaim storage from old Delta table snapshots and expired file versions |
| Partitioning Strategy | Partition a large table on a low-cardinality column to enable partition pruning |
| Adaptive Query Execution (AQE) | Understand what Spark optimises automatically and where manual tuning is still needed |
| Enabling Photon | Enable vectorised execution on a Databricks cluster for compute-heavy SQL workloads |

---

### `performance/performance_patterns.md`

Architecture and design reference for performance decisions. Read this before choosing a storage layout or compute type.

| Section | Research when you need to... |
|---------|------------------------------|
| Delta Lake Storage Optimization Strategy | Choose between OPTIMIZE/ZORDER, Liquid Clustering, and static partitioning |
| Cluster vs. SQL Warehouse Selection | Decide whether to use a Job cluster, all-purpose cluster, or SQL Warehouse for a workload |
| Adaptive Query Execution (AQE) and Photon Scope | Understand which operations benefit from AQE and Photon and which do not |

---

### `security/security_cookbook.md`

Step-by-step implementation guide for Unity Catalog access control, secrets management, row-level security, and column masking.

| Section | Research when you need to... |
|---------|------------------------------|
| RBAC with Unity Catalog | Grant and revoke privileges on catalogs, schemas, and tables for users and service principals |
| Secrets Management | Store and retrieve credentials, API keys, or connection strings securely using `dbutils.secrets` |
| Row-Level Security (RLS) with Dynamic Views | Restrict which rows a user sees based on group membership or column value |
| Column Masking with Dynamic Views | Mask PII or sensitive column values for non-privileged users via a view layer |
| Unity Catalog Native Column Masks | Apply column-level masking directly on the base table without a separate view (UC-native) |
| Unity Catalog Tag Management | Tag tables and columns with classification labels for governance and data discovery |

---

### `security/security_patterns.md`

Architecture and design reference for governance decisions. Read this when designing the security model for a new platform or pipeline.

| Section | Research when you need to... |
|---------|------------------------------|
| Unity Catalog Governance Model | Understand the catalog → schema → table hierarchy, role definitions, and when to use native features vs. dynamic views |
| Pipeline Service Principal Privilege Model | Define the minimum privilege set for a service principal scoped to pipeline layers |
| Governance for Multi-Layer Architectures | Decide where to enforce security in a Medallion pipeline (Bronze, Silver, or Gold boundary) |
