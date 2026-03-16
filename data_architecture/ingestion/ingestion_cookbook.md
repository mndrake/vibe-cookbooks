# Ingestion Cookbook
## Databricks

> **Scope:** This cookbook covers ingestion using Databricks platform features only — Auto Loader, COPY INTO, Structured Streaming, JDBC, and Lakeflow Connect. For multi-hop pipeline orchestration using Lakeflow Spark Declarative Pipelines (SDP), see `../processing/processing_cookbook.md`.

---

## Introduction

This cookbook provides practical, step-by-step guidance for data ingestion on Databricks using only the Databricks native toolchain. It covers file ingestion, streaming ingestion, database ingestion, and managed ingestion.

> **Data is stored in ADLS Gen2, not Databricks.** Databricks is a compute and orchestration platform — all table data, checkpoints, and Delta files are stored in your own Azure Data Lake Storage Gen2 account. This is why every ingestion method in this cookbook requires an ADLS Gen2 storage account and a Unity Catalog external location to be configured before data can be written. See [Azure Databricks high-level architecture](https://learn.microsoft.com/en-us/azure/databricks/getting-started/high-level-architecture) for details.

The architectural rationale for choosing between methods is covered in `ingestion_patterns.md` in the same directory.

**How to use this cookbook with the patterns doc:** Use `ingestion_patterns.md` to select a method based on your source type, latency, and operational requirements, then return here for the implementation. Quick navigation:

| If your source is... | Jump to... |
|----------------------|-----------|
| Files arriving in cloud storage (continuous) | [File Ingestion — Auto Loader](#file-ingestion--auto-loader) |
| Files arriving on a schedule (batch) | [File Ingestion — COPY INTO](#file-ingestion--copy-into) |
| Kafka / Azure Event Hubs | [Streaming Ingestion — Structured Streaming](#streaming-ingestion--structured-streaming) |
| Relational database (SQL Server, PostgreSQL) | [Database Ingestion — JDBC](#database-ingestion--jdbc) |
| SaaS application (Salesforce, Workday) | [Managed Ingestion — Lakeflow Connect](#managed-ingestion--lakeflow-connect) |
| Any other source (ERP, mainframe, on-premises, SFTP partner delivery, custom API) | Use ADF or another orchestration tool to land files in an ADLS Gen2 container, then apply [File Ingestion — Auto Loader](#file-ingestion--auto-loader) (continuous/incremental) or [File Ingestion — COPY INTO](#file-ingestion--copy-into) (scheduled batch). See [Sources Not Covered in This Cookbook](#sources-not-covered-in-this-cookbook). |

---

## Development Environment Pre-Requisites

> **On Databricks (interactive notebooks or Asset Bundle jobs):** PySpark, Delta Lake (`delta-spark`), and `dbutils` are pre-installed with every Databricks Runtime. No `pip install` commands are needed to run the code examples in this cookbook on a Databricks cluster.
>
> **Local development:** The tools below are required on your local machine for Databricks CLI operations, Asset Bundle deployment, and running unit tests outside Databricks.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.10+ | Local dev | Required for the Databricks CLI and local PySpark unit tests; 3.10+ aligns with DBR 13.3 LTS bundled Python version |
| Apache Spark via PySpark | 3.4+ | Local dev only | Bundled with Databricks Runtime — `pip install pyspark` only for local unit testing |
| Databricks CLI | Latest | Local dev | Used for secrets management, workspace interaction, and deploying Databricks Asset Bundles |
| delta-spark | Match DBR version | Local dev only | Bundled with Databricks Runtime — `pip install delta-spark` only for local unit testing; version must match your DBR's bundled Delta Lake version |

Configure your Databricks CLI connection (local machine):

```bash
# Option 1: OAuth (recommended for interactive use)
databricks auth login --host https://<your-workspace>.azuredatabricks.net

# Option 2: Personal Access Token (PAT) — common in CI/CD pipelines where
# OAuth device flows are not available (matches the pattern used in
# processing_cookbook.md, security_cookbook.md, and performance_cookbook.md)
databricks configure --token
# Enter your workspace URL and PAT when prompted.

# Verify authentication
databricks clusters list
```

### Unity Catalog Prerequisite

All table references in this cookbook use three-part naming (`catalog.schema.table`). Unity Catalog must be enabled on your workspace, and the referenced catalogs and schemas must already exist before running the examples. Replace `main`, `bronze`, `silver`, and similar names with your actual catalog and schema names.

```sql
-- Create schemas if they do not exist (run as a catalog admin)
CREATE SCHEMA IF NOT EXISTS main.bronze;
CREATE SCHEMA IF NOT EXISTS main.silver;
CREATE SCHEMA IF NOT EXISTS main.bronze_lakeflow;
```

See [Unity Catalog — getting started](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started) for workspace enablement steps.

### Storage Access (ADLS Gen2)

Every `abfss://` URI in this cookbook requires the Databricks cluster to be authorised to read from or write to the storage account. Configure access via a **Unity Catalog external location** backed by a storage credential (managed identity or service principal). This is a one-time admin task per storage account.

The examples in this cookbook reference three containers on the storage account. Ensure all three exist and are covered by Unity Catalog external locations before running the examples:

| Container | Purpose | Referenced By |
|-----------|---------|---------------|
| `raw` | Source data files (landing zone) | Auto Loader, COPY INTO |
| `checkpoints` | Streaming and Auto Loader checkpoint state | Auto Loader, Structured Streaming |
| `ops` | Bad records, dead-letter output | COPY INTO `badRecordsPath` |

See [External locations — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations) for setup instructions.

### Databricks Secrets

All credential-dependent examples use `dbutils.secrets.get(scope="...", key="...")`.

> **Azure Key Vault-backed secret scopes** are appropriate when: credentials are rotated centrally by a secrets management team and you do not want to update Databricks scope values on each rotation; or when Key Vault access policies are the authoritative access control mechanism for your organisation. Create an AKV-backed scope via the Databricks UI: **Settings → Developer → Manage secret scopes → Create** and provide your Key Vault DNS name and resource ID. See [Azure Key Vault-backed secret scopes — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes) for the full setup guide.
>
> **Databricks-managed scopes** (shown in the CLI example below) are appropriate when Key Vault is not available or when the additional Azure resource and access policy management overhead is not justified. The `dbutils.secrets.get()` call is identical regardless of scope backend.
>
> **Granting access:** A job running under a service principal must have `READ` permission on the secret scope to call `dbutils.secrets.get()`. Grant it with: `databricks secrets put-acl --scope <scope-name> --principal <service-principal-name> --permission READ`. Without this, the job fails with a permission denied error at runtime, not at deployment time. See the permissions section of the Databricks Secrets documentation linked below.

```bash
# Databricks-managed scope fallback (use AKV-backed scopes in production)
databricks secrets create-scope --scope jdbc-secrets
databricks secrets create-scope --scope eventhubs-secrets

# Add a secret (enter value at the interactive prompt — never stored in shell history)
# Syntax: databricks secrets put-secret <scope> <key>
databricks secrets put-secret jdbc-secrets sql-user
databricks secrets put-secret jdbc-secrets sql-password
```

See [Databricks Secrets — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secrets) for full documentation including permissions management.

---

## Infrastructure Prerequisites

Before running any example in this cookbook, ensure the following infrastructure is in place. These are one-time setup tasks typically performed by a workspace admin.

### Databricks Runtime Version

All examples in this cookbook require **Databricks Runtime (DBR) 13.3 LTS or later**. Specific minimum version requirements:

| Feature | Minimum DBR |
|---------|-------------|
| Auto Loader `schemaEvolutionMode`, `trigger(availableNow=True)` | 10.4 LTS |
| `trigger(once=True)` deprecated — use `trigger(availableNow=True)` | 11.3 LTS |
| Liquid Clustering (referenced in performance cookbook) | 13.3 LTS |

**Recommendation:** Use **DBR 14.3 LTS or later** for new workloads — it is the current long-term support release as of March 2026 and includes all features referenced in this cookbook.

### Cluster Configuration

| Scenario | Recommended Configuration |
|----------|--------------------------|
| Auto Loader / JDBC batch jobs | Job cluster, auto-terminate after job; start with 2–4 workers, scale based on actual throughput |
| Structured Streaming (continuous) | Job cluster with auto-scaling, or a [Lakeflow Jobs continuous trigger](https://learn.microsoft.com/en-us/azure/databricks/jobs/triggers) (a job configured to restart automatically when the run terminates — distinct from a standard job task); always-on incurs continuous cost |
| JDBC with `numPartitions = 8` | At least 4 workers so partitions distribute across executors; single-node clusters will serialise reads |
| One-off loads / development | All-purpose cluster; not recommended for production recurring jobs due to cost and contention |

See [Cluster configuration — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/compute/configure) and the `performance_cookbook.md` in this repository for sizing guidance.

---

## File Ingestion

### File Ingestion — Auto Loader

> **Architecture diagram:** [Auto Loader overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/) includes diagrams of the checkpoint-based file tracking mechanism and the difference between directory listing and file notification discovery modes.

Auto Loader (`cloudFiles` format) incrementally ingests files from cloud storage (ADLS Gen2, S3, GCS) into Delta Lake. It records processed files in a checkpoint directory on durable storage. On each trigger, it reads files not yet recorded in the checkpoint and writes them to Delta in a transactional commit. Each file is processed once provided the checkpoint is intact and the Delta write completes. If the checkpoint is deleted, Auto Loader reprocesses all files from the source path on the next run — in append-mode pipelines (the default), this reprocessing inserts duplicate rows into the target Delta table. Auto Loader has no built-in deduplication on reprocessing; if a checkpoint is lost and the pipeline runs in append mode, deduplicate the target table manually using `MERGE` or `ROW_NUMBER()` before the pipeline resumes normal operation.

#### Problem

Files arrive continuously in an ADLS Gen2 container from an upstream system. They must be ingested into a Delta table as they arrive, without reprocessing files already loaded and without missing new arrivals.

#### Solution

Use `spark.readStream.format("cloudFiles")` with a checkpoint on durable cloud storage. Write using `writeStream` with `trigger(availableNow=True)` for scheduled micro-batch or `trigger(processingTime="5 minutes")` for continuous micro-batch.

##### Python

```python
checkpoint_path = "abfss://checkpoints@mystorageaccount.dfs.core.windows.net/orders_autoloader"
source_path = "abfss://raw@mystorageaccount.dfs.core.windows.net/orders/"

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    # schemaLocation stores the schema Auto Loader infers from source files.
    # It is separate from checkpointLocation (which tracks which files have been processed).
    # Both must be set. Storing it as a subdirectory of checkpoint_path keeps both together.
    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
    # schemaEvolutionMode controls new-column handling at the READ side. With addNewColumns,
    # when a new column is first detected the stream stops with UnknownFieldException, Auto Loader
    # updates the schema at the schema location, and the stream must be restarted. In a scheduled
    # job, the run that detects the new column fails; the following run succeeds.
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    # mergeSchema must also be set on the WRITE side. schemaEvolutionMode alone is not
    # sufficient — it accepts new columns into the DataFrame, but the Delta write will fail
    # with AnalysisException if the target table does not yet have the column and mergeSchema
    # is not enabled. Both options are required together for end-to-end schema evolution.
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("main.bronze.orders")
)
```

##### SQL

Auto Loader is configured in Python. Query the resulting Delta table with SQL:

```sql
-- Validate records landed correctly
SELECT
    COUNT(*) AS total_rows,
    MIN(_metadata.file_modification_time) AS earliest_file,
    MAX(_metadata.file_modification_time) AS latest_file
FROM main.bronze.orders;

-- Inspect rescued data (if schemaEvolutionMode = 'rescue')
SELECT _rescued_data
FROM main.bronze.orders
WHERE _rescued_data IS NOT NULL
LIMIT 10;
```

#### Discussion and Concerns

- **Checkpoint durability:** The checkpoint directory must be on durable cloud storage. Deleting it causes Auto Loader to reprocess all files from the beginning.
- **`schemaLocation`:** Auto Loader uses `schemaLocation` to store the schema it infers from the source files — this is separate from the checkpoint directory (which tracks which files have been processed). They are co-located in the example (`checkpoint_path + "/schema"`) for convenience, but they serve different functions. If you change `checkpoint_path` for a different table, update `schemaLocation` to match — pointing two Auto Loader streams at the same `schemaLocation` will cause them to share schema state, which leads to incorrect schema inference. If the schema location is corrupted or contains a stale schema, delete only the `schemaLocation` subdirectory (not the full checkpoint directory) and let Auto Loader re-infer the schema from the source files on the next run. Deleting only the schema location resets schema inference without resetting file tracking state.
- **`schemaEvolutionMode` and `mergeSchema` — both required for schema evolution:** These are two independent mechanisms that must both be set. `cloudFiles.schemaEvolutionMode` governs the **read side**: it controls whether Auto Loader's schema inference accepts new columns from incoming source files. `mergeSchema` governs the **write side**: it controls whether the Delta Lake writer adds new columns to the target table when it encounters a column not already in the table schema. Setting `schemaEvolutionMode = "addNewColumns"` without `mergeSchema = "true"` means the new column is present in the in-memory DataFrame but the Delta write fails at runtime with `AnalysisException: cannot write nullable column to non-nullable table`. Setting `mergeSchema = "true"` without `addNewColumns` means the writer would accept schema changes, but Auto Loader would not include the new column in the DataFrame in the first place. Both must be set together for end-to-end column addition.
  **Operational note — `addNewColumns` and the restart cycle:** When `addNewColumns` encounters a new column for the first time, the stream does not add it silently. The stream **stops with `UnknownFieldException`**, Auto Loader updates the stored schema at the schema location, and the stream must be restarted before the new column is included in the output. For a scheduled Databricks Job, this means: the job run that first sees the new column fails; the next scheduled run starts with the updated schema and succeeds. This is expected behavior. Alert on consecutive job failures — a single schema-evolution failure followed by success is normal; two or more consecutive failures indicates a different problem.
  Mode choices for `schemaEvolutionMode`: `addNewColumns` for bronze ingestion where all source columns must be captured. `failOnNewColumns` for silver/gold tables where schema drift should trigger investigation. `rescue` for highly variable sources. **`none` silently drops any column in the source file that is not already in the inferred schema** — do not use `none` unless you have a separate mechanism to validate that no new columns exist before each run, or you will lose data without an error.
- **`trigger(availableNow=True)` vs. `trigger(once=True)`:** `availableNow=True` is the modern replacement for the deprecated `once=True`. Use `availableNow=True` for all new pipelines. `trigger(once=True)` processes a single micro-batch and then stops, which may leave unprocessed files if more than one batch of data has arrived; this is the main reason it was replaced.
- **Target table creation:** `.toTable("main.bronze.orders")` creates the Delta table automatically on first run if it does not exist, provided the executing principal has `CREATE TABLE` on the target schema. No `CREATE TABLE` DDL is required before the first run.
- **File discovery mode:** Auto Loader defaults to **directory listing** mode — it polls the storage path on each trigger cycle to find new files. For landing zones with thousands of files or high file-arrival frequency, consider **file notification mode**, which delivers lower latency and reduced storage API costs. On **DBR 14.3+** with a Unity Catalog external location, the recommended approach is managed file events: enable file events on the external location, then set `.option("cloudFiles.useManagedFileEvents", "true")` — Databricks manages the notification infrastructure automatically with no manual queue setup. On earlier runtimes or without Unity Catalog, use the classic file notification mode: `.option("cloudFiles.useNotifications", "true")`, which requires one-time setup of a storage event queue. See [Auto Loader file notification mode — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/file-notification-mode) for setup steps for both approaches.

#### See Also

- [Auto Loader — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)
- [Auto Loader schema evolution — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/schema)

---

### File Ingestion — COPY INTO

COPY INTO is an idempotent, SQL-based command that loads files from cloud storage into a Delta table. It tracks which files have been loaded in the Delta table's transaction log, so re-running the same command will not duplicate data.

#### Problem

CSV files arrive in ADLS Gen2 on a daily schedule and must be loaded into a Delta table. The load must be safe to re-run — if the job fails partway through and is restarted, it must not insert duplicate rows.

#### Solution

Use `COPY INTO` targeting the destination Delta table. COPY INTO reads from the path and skips files it has already loaded.

> **Prerequisite — create the target table first:** Unlike Auto Loader's `.toTable()`, `COPY INTO` requires the target Delta table to already exist. It will raise `TABLE_OR_VIEW_NOT_FOUND` if the table does not exist. Run the `CREATE TABLE IF NOT EXISTS` DDL below before the first load.

##### Python

```python
# Step 1: Create the target table before the first load (run once)
spark.sql("""
    CREATE TABLE IF NOT EXISTS main.bronze.sales_transactions (
        transaction_id STRING,
        sale_date      DATE,
        amount         DOUBLE,
        product_id     STRING,
        customer_id    STRING
    )
    USING DELTA
""")

# Step 2: Load files idempotently
spark.sql("""
    COPY INTO main.bronze.sales_transactions
    FROM 'abfss://raw@mystorageaccount.dfs.core.windows.net/sales/2026/03/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS (
        'header'         = 'true',
        'inferSchema'    = 'false',
        'delimiter'      = ',',
        'badRecordsPath' = 'abfss://ops@mystorageaccount.dfs.core.windows.net/bad_records/sales/'
    )
    COPY_OPTIONS (
        'mergeSchema' = 'false'
    )
""")
```

##### SQL

```sql
COPY INTO main.bronze.sales_transactions
FROM 'abfss://raw@mystorageaccount.dfs.core.windows.net/sales/2026/03/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header'         = 'true',
  'inferSchema'    = 'false',
  'delimiter'      = ',',
  'badRecordsPath' = 'abfss://ops@mystorageaccount.dfs.core.windows.net/bad_records/sales/'
)
COPY_OPTIONS (
  'mergeSchema' = 'false'
);

-- Validate: check row count and latest load timestamp
SELECT
    COUNT(*) AS rows_loaded,
    MAX(_metadata.file_modification_time) AS latest_file
FROM main.bronze.sales_transactions;
```

#### Discussion and Concerns

- **COPY INTO vs. Auto Loader:** COPY INTO is simpler — no streaming context, no checkpoint directory, pure SQL. It does not support automatic schema evolution: new columns in source files are silently dropped unless `'mergeSchema' = 'true'` is set in `COPY_OPTIONS`, which adds new columns to the target table but must be explicitly enabled on each run. Auto Loader with `addNewColumns` is the better choice when schema drift is expected.
- **Directory scanning is not recursive by default:** COPY INTO reads files at the exact path specified. It does **not** recurse into subdirectories unless `'recursiveFileLookup' = 'true'` is added to `FORMAT_OPTIONS`. If files are organized under date-partitioned subdirectories (e.g., `sales/2026/03/15/`, `sales/2026/03/16/`), either enable `recursiveFileLookup` and point COPY INTO at the root path, or point COPY INTO at each subdirectory explicitly. Auto Loader supports recursive path scanning via glob patterns (e.g., `abfss://raw@.../sales/2026/03/**/*.csv`).
- **`badRecordsPath`:** Routes malformed rows to a separate storage path rather than aborting the entire load. Without it, a single corrupt record fails the full COPY INTO command. Point `badRecordsPath` to a container **outside** the source data hierarchy (e.g., an `ops` container) to avoid COPY INTO attempting to re-ingest the bad record files on subsequent runs. Monitor the bad records path as part of pipeline health checks.
- **Idempotency scope:** COPY INTO tracks loaded files per Delta table. If the target table is dropped and recreated, COPY INTO reloads all files on the next run.
- **`inferSchema` and pre-defined DDL:** When `inferSchema = 'true'` is set on a COPY INTO command targeting a table with an existing schema, COPY INTO infers types from the source file and attempts to cast them to the target column types at write time. This can cause errors or silent coercion — for example, if a CSV `sale_date` column is inferred as `STRING` but the target DDL defines it as `DATE`. For tables where the DDL already defines the target schema, set `inferSchema = 'false'`: COPY INTO reads all CSV values as strings and the Delta writer handles casting to the target column types, producing a predictable and explicit type mapping. Use `inferSchema = 'true'` only when creating a new table without a pre-defined DDL and when accepting inferred column types at the bronze layer is explicitly intended.

#### See Also

- [COPY INTO — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)

---

## Streaming Ingestion

### Streaming Ingestion — Structured Streaming

This section covers Azure Event Hubs using the `eventhubs` Spark connector. Structured Streaming also supports Kafka (using `format("kafka")` with `kafka.bootstrap.servers` and `subscribe` options) and Amazon Kinesis (using the `read_kinesis` SQL table-valued function or the Kinesis PySpark connector) — the checkpoint and Delta write patterns are the same, but the source-specific connection options differ. See [Kafka connector — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/kafka) and [read_kinesis — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/read_kinesis) for those source configurations.

Structured Streaming tracks the last committed source offset in a checkpoint directory. On restart, it resumes from the last committed offset. When writing to Delta Lake with a durable checkpoint, each message from Event Hubs is written to Delta once, provided the source retains messages at that offset. If the Event Hub retention window expires before the stream restarts, messages between the last checkpoint offset and the earliest available offset are unrecoverable. **Detecting the gap:** when the checkpoint offset is older than the Event Hub retention window, the connector may raise an error (e.g., `OFFSET_OUT_OF_RANGE`) or, depending on connector configuration, resume from the earliest available offset without an error — meaning data loss can be silent. After any extended stream outage, compare the row count and latest `enqueuedTime` in the Delta table against the Event Hub's earliest available offset and message count (visible in the Azure portal under **Event Hubs namespace → your Event Hub → Metrics**) to confirm no gap exists. **Recovery:** messages that fell outside the retention window cannot be re-read from Event Hubs. If [Event Hubs Capture](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview) is enabled, backfill the gap using COPY INTO or Auto Loader targeting the Avro capture files (`FILEFORMAT = AVRO`) — see the Capture Discussion bullet below for path details and setup guidance. If no secondary source exists, document the loss, update downstream row count SLAs accordingly, and increase the Event Hub retention period to reduce the risk of recurrence. The exactly-once guarantee applies to the Spark-to-Delta write layer; it does not prevent duplicate messages produced upstream of the message bus.

> **Architecture diagram:** [Structured Streaming programming guide — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/) diagrams the micro-batch execution model, offset tracking, and checkpoint recovery. [Azure Event Hubs — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/streaming-event-hubs) covers Event Hubs connection options and consumer group configuration.

#### Problem

Order events are published to Azure Event Hubs at high volume and must be written to a bronze Delta table with sub-minute latency, with automatic recovery on failure.

#### Solution

> **Prerequisite — install the Azure Event Hubs connector:** The `eventhubs` format requires the `com.microsoft.azure:azure-eventhubs-spark_2.12:<version>` Maven library installed on the cluster before running the code below. Install it via **Compute → your cluster → Libraries → Install New → Maven**. Match the version to your Databricks Runtime's Scala version; see [azure-eventhubs-spark releases](https://github.com/Azure/azure-event-hubs-spark/releases) for the latest compatible version. Without this library, the code below fails immediately with `DataSourceNotFoundException: Failed to find data source: eventhubs`.

Use `spark.readStream.format("eventhubs")` with a durable checkpoint.

##### Python

```python
connection_string = dbutils.secrets.get(scope="eventhubs-secrets", key="connection-string")
eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        connection_string
    ),
    # Use a dedicated consumer group for each Spark job reading from this Event Hub.
    # The $Default group is shared by all consumers that do not specify one — two jobs
    # using $Default compete for partitions and one will silently receive zero events.
    # Create the consumer group in the Azure portal or via CLI before running this job.
    "eventhubs.consumerGroup": "orders-bronze-loader",
}
checkpoint_path = "abfss://checkpoints@mystorageaccount.dfs.core.windows.net/orders_stream"
order_schema = "order_id STRING, customer_id STRING, order_total DOUBLE, order_date TIMESTAMP"

from pyspark.sql.functions import col, from_json

(
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
    .select(from_json(col("body").cast("string"), order_schema).alias("data"), "enqueuedTime")
    .select("data.*", "enqueuedTime")
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    # Use availableNow=True for Databricks Jobs (processes all available partitions then terminates).
    # Use processingTime="1 minute" only in a long-running notebook or a Continuous Job — it never terminates.
    .trigger(availableNow=True)
    .toTable("main.bronze.orders_stream")
)
```

##### SQL

Monitor the resulting Delta table with SQL:

```sql
SELECT
    DATE_TRUNC('minute', enqueuedTime)                                             AS event_minute,
    COUNT(*)                                                                        AS event_count,
    AVG(UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(enqueuedTime))        AS avg_lag_seconds
FROM main.bronze.orders_stream
WHERE enqueuedTime >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY 1
ORDER BY 1 DESC;
```

#### Discussion and Concerns

- **Checkpoint location:** Store checkpoints on durable cloud storage. Deleting the checkpoint causes the stream to restart from the beginning of the Event Hub retention window.
- **Event Hubs Capture (recommended):** Enable [Event Hubs Capture](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview) in the Azure portal (**Event Hubs namespace → your Event Hub → Capture → On**) to write a durable copy of all messages to ADLS Gen2 in Avro format. Capture is independent of the stream checkpoint — it runs continuously regardless of whether the Spark job is running. If the stream checkpoint offset falls outside the Event Hub retention window, the Capture files provide a backfill path. Capture files land under the path `{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}.avro`. To backfill from Capture files, point COPY INTO or Auto Loader at the capture container and specify `FILEFORMAT = AVRO` (not CSV or JSON). Without Capture enabled, messages that fall outside the Event Hub retention window are permanently unrecoverable.
- **Consumer groups:** Event Hubs assigns partition ownership per consumer group. If two Spark jobs read from the same Event Hub without specifying a consumer group, both default to `$Default` and compete for partitions — the result is that one job receives no events with no error raised. Use a dedicated consumer group per Spark job (`eventhubs.consumerGroup` in `eh_conf`). Create the consumer group in the Azure portal (**Event Hubs namespace → your Event Hub → Consumer groups**) before starting the stream.
- **`sc._jvm` and the encryption call:** `sc` is the `SparkContext`, automatically available in Databricks notebooks. The `sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(...)` call is a Py4J bridge into the Java library — it is required because the Event Hubs connector expects the connection string in encrypted form. This call is only available in Databricks notebook and job cluster environments where the Event Hubs library is installed; it will raise a `NameError` in standalone Python scripts that do not have `sc` pre-initialised.
- **Stream lifecycle — Job vs. notebook:** `trigger(availableNow=True)` (used in the code above) processes all available Event Hub partitions and then terminates — the correct choice for Databricks Jobs where each task must terminate for the job to complete. Use `trigger(processingTime="1 minute")` only when running in a long-running notebook or a Databricks Workflows **Continuous Job** — this trigger starts a stream that never terminates on its own. To stop a running stream gracefully from a notebook: `query = stream.start(); query.awaitTermination(); query.stop()`.
- **`from_json` and malformed messages:** `from_json` returns `null` for all fields when a message body does not match the declared schema (wrong field types, malformed JSON, encoding issues) — it does not raise an error. Monitor for rows where `order_id IS NULL` in the bronze table to detect schema mismatches or upstream message format changes. When null rows appear, retain them in the bronze table for investigation (do not delete — they are evidence of upstream drift), alert the pipeline owner, and resolve by either updating `order_schema` to match the new source format or coordinating with the upstream producer to fix the message structure. **Updating `order_schema` is a code change** — the variable is a string literal in the notebook or job script. For `trigger(availableNow=True)` jobs, deploy the updated code and the next scheduled run picks up the new schema. For `trigger(processingTime=...)` continuous streams, the running stream must be stopped first (the in-memory schema is fixed at stream start), the code updated, and the stream restarted; the existing checkpoint remains valid provided only new columns are added or column types are widened.

#### See Also

- [Structured Streaming — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/)
- [Table streaming reads and writes — Delta Lake](https://docs.delta.io/latest/delta-streaming.html)

---

## Database Ingestion

### Database Ingestion — JDBC

Spark's JDBC data source reads directly from relational databases over a JDBC connection. It is the standard pattern when data cannot be exported to cloud storage first and no CDC feed is available.

> **Architecture diagram:** [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/external-data/jdbc) includes a diagram of parallel partition reads showing how `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` split the source table into concurrent range queries.

#### Problem

An Azure SQL Database must be ingested into Delta Lake on a scheduled basis. The table contains tens of millions of rows and must be parallelised to meet the ingestion window.

#### Solution

Use `spark.read.format("jdbc")` with partition configuration. Write to Delta using MERGE for incremental loads or overwrite for full loads.

##### Python

```python
from pyspark.sql.functions import max as spark_max
from delta.tables import DeltaTable

jdbc_url = "jdbc:sqlserver://myserver.database.windows.net:1433;database=SourceDB"
jdbc_user = dbutils.secrets.get(scope="jdbc-secrets", key="sql-user")
jdbc_password = dbutils.secrets.get(scope="jdbc-secrets", key="sql-password")

# Determine partition bounds from the source table to avoid skewed partitions.
# lowerBound and upperBound control partition generation, not row filtering —
# all rows matching the WHERE clause are read regardless of these values.
bounds_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query", "SELECT MIN(order_id) AS lo, MAX(order_id) AS hi FROM dbo.orders")
    .load()
    .collect()[0]
)
lower_bound = str(bounds_df["lo"])
upper_bound = str(bounds_df["hi"])

# Retrieve the last loaded watermark from the target table.
# On the first run, the table is empty and last_ts is None — the fallback triggers a full load.
try:
    last_ts = spark.table("main.bronze.orders").select(spark_max("updated_at")).collect()[0][0]
    watermark = last_ts.strftime("%Y-%m-%d %H:%M:%S") if last_ts else "1900-01-01 00:00:00"
except Exception:
    watermark = "1900-01-01 00:00:00"

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query", f"SELECT * FROM dbo.orders WHERE updated_at >= '{watermark}'")
    .option("partitionColumn", "order_id")
    .option("lowerBound", lower_bound)
    .option("upperBound", upper_bound)
    .option("numPartitions", "8")
    .load()
)

target = DeltaTable.forName(spark, "main.bronze.orders")
(
    target.alias("t")
    .merge(df.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

##### SQL

```sql
-- Create the target table before the first JDBC load.
-- DeltaTable.forName() in the Python MERGE will raise AnalysisException if the table does not exist.
CREATE TABLE IF NOT EXISTS main.bronze.orders (
    order_id     BIGINT,
    customer_id  STRING,
    order_total  DOUBLE,
    updated_at   TIMESTAMP
)
USING DELTA;

-- SQL cannot pass runtime credentials to JDBC options directly.
-- Use Python above to load data; validate with SQL:

SELECT
    COUNT(*)        AS rows_in_target,
    MAX(updated_at) AS latest_updated_at
FROM main.bronze.orders;

SELECT *
FROM main.bronze.orders
WHERE updated_at >= current_date - 1
ORDER BY updated_at DESC
LIMIT 20;
```

#### Discussion and Concerns

- **Parallel reads increase source load:** 8 partitions = 8 concurrent connections. Use a read replica where available.
- **Partition bounds:** `lowerBound` and `upperBound` define how Spark splits the read into `numPartitions` parallel range queries — they do not filter rows. Setting them to values far outside the actual data range produces heavily skewed partitions. The example above queries the actual `MIN`/`MAX` before each load to keep partitions balanced.
- **Watermark management:** The watermark is derived from `MAX(updated_at)` of the target table at the start of each run, so no manual date update is needed between runs. On the first run the table is empty and the fallback value `1900-01-01` causes a full load. **First-run partial failure produces a permanent data gap:** if the full load fails partway through (timeout, network drop, source connection error), the target table contains partial data with a non-null `MAX(updated_at)`. The next run picks up from that watermark — any source rows with `updated_at` earlier than the partial load's maximum are permanently skipped. If a first full load fails, truncate the target table before restarting: `TRUNCATE TABLE main.bronze.orders`. This forces the fallback to `1900-01-01` and triggers a clean full load on the next run.
- **Full extract (overwrite):** For tables that must be fully refreshed each run (no reliable watermark column, or the table is small enough to reload in full), replace the MERGE block with: `df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.bronze.orders")`. **Do not use MERGE for a full-refresh pattern** — MERGE on a full load leaves rows in the target that were deleted from the source, because MERGE only acts on matched and unmatched rows from the source; rows in the target with no matching source row are untouched by default.
- **MERGE cardinality:** Delta raises a `MERGE_CARDINALITY_VIOLATION` error if the MERGE `ON` condition matches multiple target rows to a single source row. This happens when `partitionColumn` is not a unique key of the source table. Verify that the column used in `t.order_id = s.order_id` is a unique key before using it as both the partition column and the merge key.
- **Hard deletes are invisible:** Incremental JDBC based on `updated_at` will not detect deleted rows. Use a CDC tool (Debezium) if delete propagation is required.
- **SQL Server driver:** Included in Databricks Runtime. For PostgreSQL, install the `org.postgresql:postgresql:<version>` Maven library via the cluster Libraries tab (e.g., `org.postgresql:postgresql:42.7.3` for a DBR 14.x cluster — check the [PostgreSQL JDBC driver releases](https://jdbc.postgresql.org/download/) for the latest version compatible with your JVM). For MySQL, install `com.mysql:mysql-connector-j:<version>` (e.g., `com.mysql:mysql-connector-j:9.1.0`). **Installing a library on a running cluster requires a cluster restart before the library is available to the JVM.** If you install the driver and run the notebook immediately without restarting, the job will fail with `ClassNotFoundException` for the driver class — not a connection error.
- **`query` and `partitionColumn` interaction:** When `query` is specified alongside `partitionColumn`, Spark wraps the query as a subquery for each partition: `SELECT * FROM (<your query>) WHERE order_id BETWEEN <lower> AND <upper>`. SQL Server handles this correctly. If your JDBC driver does not support subquery wrapping, remove the `query` option and use `.option("dbtable", "dbo.orders")` combined with a database view that applies the filter, or remove the partition options and accept a single-partition sequential read.

#### See Also

- [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/external-data/jdbc)
- [DeltaTable Python API — Delta Lake](https://docs.delta.io/api/latest/python/spark/)

---

## Managed Ingestion

### Managed Ingestion — Lakeflow Connect

> **Architecture diagram:** [Lakeflow Connect overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) shows the architecture of Lakeflow Connect: SaaS source → Databricks serverless compute → Delta tables under Unity Catalog, with no data transiting third-party infrastructure.

Lakeflow Connect provides Databricks-native managed connectors for SaaS applications and databases. As of March 2026, it is GA for Salesforce, Workday, SQL Server, ServiceNow, and Google Analytics. Pipelines run on serverless compute within Databricks, governed by Unity Catalog.

#### Problem

The data platform must ingest data from Salesforce into Delta Lake on a recurring schedule. Available options include Salesforce Data Export to a landing zone with Auto Loader, a third-party connector tool, or Lakeflow Connect. This section covers the Lakeflow Connect implementation.

#### Solution

Create a Lakeflow Connect pipeline for Salesforce. The connector handles incremental extraction, schema evolution, and scheduling natively.

> **Before running the code below:** Create the pipeline and configure credentials first. The code below grants permissions to the pipeline's service principal and validates the landed data — it assumes the pipeline already exists.
>
> **UI:** Databricks UI → Ingestion → Create pipeline → select Salesforce → enter your Salesforce OAuth credentials (connected app client ID, client secret, instance URL, and environment type) → select the objects to replicate → configure the destination catalog and schema → set the sync frequency.
>
> **Salesforce connected app:** A connected app is an OAuth client registered in Salesforce Setup (**Setup → App Manager → New Connected App**). Configure it with the scopes `api`, `refresh_token`, and `offline_access`. The client ID and client secret are generated by Salesforce — use these values for the OAuth credentials above. The "environment type" field distinguishes production orgs from sandbox orgs (sandbox credentials use a different token endpoint and will fail silently if the wrong type is selected).
>
> **Asset Bundles:** Define the pipeline in `databricks.yml` using the `pipelines:` resource key (verify the exact key against the current [Databricks Asset Bundles schema](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/reference) — the Lakeflow Connect pipeline resource key is subject to change as the feature matures) and deploy with `databricks bundle deploy`. See [Lakeflow Connect Asset Bundles](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) for the YAML schema and credential configuration reference.

##### Python (Databricks Asset Bundles setup)

```python
# Grant the pipeline service principal access to the destination schema.
# Run once by a Unity Catalog admin after provisioning the pipeline.
#
# IMPORTANT: Run these grants only after confirming the pipeline was successfully
# provisioned. Databricks creates the pipeline service principal at provisioning
# time — if the pipeline failed to provision, the service principal may not exist.
# Grants against a nonexistent principal succeed silently but have no effect,
# making the subsequent runtime permission error difficult to diagnose.
#
# To find the actual service principal name: Databricks UI → Settings →
# Identity and access → Service principals, and look for the principal
# created when the Lakeflow Connect pipeline was provisioned. Verify it appears
# in the list before running the grants below. Replace "lakeflow-pipeline-sp"
# with the actual principal name or application ID shown in the UI.

pipeline_sp = "lakeflow-pipeline-sp"  # replace with the actual service principal name

spark.sql(f"GRANT USE CATALOG ON CATALOG main TO `{pipeline_sp}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA main.bronze_lakeflow TO `{pipeline_sp}`")
spark.sql(f"GRANT CREATE TABLE ON SCHEMA main.bronze_lakeflow TO `{pipeline_sp}`")
spark.sql(f"GRANT MODIFY ON SCHEMA main.bronze_lakeflow TO `{pipeline_sp}`")

# Verify landed tables
display(spark.sql("SHOW TABLES IN main.bronze_lakeflow"))

# Query ingested data
df_accounts = spark.table("main.bronze_lakeflow.salesforce_account")
display(df_accounts.limit(10))
```

##### SQL

```sql
SHOW TABLES IN main.bronze_lakeflow;

-- Inspect Lakeflow Connect sync metadata.
-- _databricks_synced: TIMESTAMP column added by Lakeflow Connect to every replicated table.
-- It records when Databricks committed the row to Delta Lake (not when the record changed
-- in the source system). Use it to inspect pipeline throughput and identify sync batches.
-- Do not use it as a proxy for source data freshness or change time.
SELECT
    id,
    name,
    industry,
    annual_revenue,
    _databricks_synced
FROM main.bronze_lakeflow.salesforce_account
ORDER BY _databricks_synced DESC
LIMIT 50;

-- Volume per sync batch
SELECT
    DATE_TRUNC('hour', _databricks_synced) AS sync_hour,
    COUNT(*)                               AS rows_synced
FROM main.bronze_lakeflow.salesforce_account
GROUP BY 1
ORDER BY 1 DESC;
```

#### Discussion and Concerns

- **Connector catalogue:** GA for Salesforce, Workday, SQL Server, ServiceNow, and Google Analytics as of March 2026. Additional connectors are available in public preview. Check [documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) for the current list. **Preview connectors should not be used for production workloads without evaluating the risk:** connector API contracts, schema behaviour, and authentication mechanisms may change between preview and GA without notice; Databricks may alter or remove a preview connector; and if a connector is discontinued, the replacement migration path may require rebuilding the ingestion pipeline from scratch. For production use, confirm the connector is GA and review the release notes for schema or API changes at each Databricks platform version update.
- **Unity Catalog required:** Lakeflow Connect requires Unity Catalog — not available with the legacy Hive metastore.
- **Schema evolution:** New columns automatically added. Deleted source columns retained in Delta with `null` values — filter downstream as needed. Column renames in the source produce a new column in Delta; the prior column persists with its historical values. Downstream pipelines must account for both the old and new column names after a rename.
- **Asset Bundles CI/CD:** Define pipelines in `databricks.yml` and deploy via the Databricks CLI for source control and environment promotion.
- **Cost:** Lakeflow Connect is billed at the Databricks serverless DBU rate based on compute time, not per row ingested. For low-volume, low-frequency syncs, the per-trigger compute overhead can exceed the cost of an equivalent JDBC job — evaluate against your sync frequency, data volume, and the operational overhead each approach requires. See `ingestion_patterns.md` for the full cost trade-off discussion.

#### See Also

- [Lakeflow Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- [Databricks Asset Bundles — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)

---


## Sources Not Covered in This Cookbook

Some source systems — ERP platforms, proprietary databases, on-premises applications, mainframes, custom APIs — do not have a native Databricks connector and are not covered by Lakeflow Connect. A common pattern for these sources is a **landing zone approach**. If the source exposes a JDBC endpoint, direct database ingestion (see the [JDBC section](#database-ingestion--jdbc) above) may also apply — evaluate based on source connectivity, data volume, and whether a raw file audit trail is required.

1. An external orchestration tool extracts data from the source and writes it as files (CSV, JSON, Parquet, or Avro) to a cloud storage landing zone (ADLS Gen2 container, S3 prefix, or GCS bucket). Azure Data Factory (ADF) is the most common tool on Azure; AWS Glue and Informatica are common alternatives.
2. Databricks reads the files from the landing zone using **Auto Loader** (for ongoing incremental file arrival) or **COPY INTO** (for scheduled batch loads). All patterns in the [File Ingestion](#file-ingestion) section apply directly.

The landing zone acts as the contractual boundary between the upstream extraction tool and the Databricks pipeline. Neither side needs to know about the other's schedule — the upstream tool writes when data is ready; Databricks reads when triggered. This also provides a raw file audit trail that enables reprocessing if a downstream pipeline fails.

For the full architectural context — when a landing zone is required vs. when direct ingestion bypasses it, retention policies, and trade-offs — see the **Landing Zone Architecture** section in `ingestion_patterns.md`.

> **Note:** Configuring the external orchestration tool (ADF pipelines, AWS Glue jobs, etc.) is outside the scope of this cookbook. This cookbook covers what Databricks does once data is in the landing zone.

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | How to Check | What to Look For |
|--------|-------------|-----------------|
| Auto Loader / Structured Streaming | `spark.streams.active`; Databricks Jobs run history | Streams stopped without error; checkpoint files not advancing |
| COPY INTO load history | `DESCRIBE HISTORY main.bronze.my_table` | Operations where `operation = 'COPY INTO'`; `numAddedFiles` is non-zero on expected run days |
| Lakeflow Connect | Databricks UI → Ingestion → Lakeflow pipelines | Pipelines not run within expected window; connector errors in event log |
| JDBC job duration | Databricks Jobs run history | Durations trending upward — may indicate source table growth requiring `numPartitions` adjustment |

### Common Failure Patterns and Remediation

| Failure | Likely Cause | Remediation |
|---------|-------------|-------------|
| Auto Loader stream stops with `FileNotFoundException` on checkpoint | Checkpoint directory deleted or moved | Restore from backup; if unavailable, delete checkpoint and reprocess from beginning. **In append-mode pipelines, reprocessing from the beginning inserts duplicate rows** — deduplicate the target table (using `MERGE` or `ROW_NUMBER()`) before the pipeline resumes normal operation. See the Auto Loader introductory section above for the full duplication warning. |
| COPY INTO loads zero rows after schema change | New source files have columns not in target Delta schema | `ALTER TABLE ... ADD COLUMNS (...)` then re-run COPY INTO |
| JDBC job significantly slower | Source table growth; insufficient `numPartitions`; index fragmentation | Increase `numPartitions`; request source DBA to rebuild indexes; use read replica |
| Structured Streaming resumes with offset gap or `OFFSET_OUT_OF_RANGE` error | Stream was paused longer than the Event Hub retention window; messages in the gap are no longer available | Detect the gap: compare `MAX(enqueuedTime)` in the Delta table against the Event Hub's earliest available offset in the Azure portal. If Event Hubs Capture is enabled, backfill by pointing COPY INTO or Auto Loader at the Avro capture files (`FILEFORMAT = AVRO`; path pattern: `{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/...`). If Capture is not enabled, the messages are unrecoverable — document the data loss and enable Capture going forward. See the Event Hubs Capture Discussion bullet above for setup details. |
| Lakeflow Connect authentication error | OAuth token expired or credentials rotated | Update connection credentials in Lakeflow Connect configuration |
| Lakeflow Connect lands duplicate rows | Connector backfill triggered (e.g., after reconnection or pipeline reset) | Deduplicate in silver using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY _databricks_synced DESC)` — `_databricks_synced` is the Lakeflow Connect sync timestamp column present on all replicated tables |
| Auto Loader / COPY INTO encounters malformed or corrupt files | Source file contains rows with unexpected types, extra fields, or corrupt encoding | For Auto Loader: set `cloudFiles.schemaEvolutionMode = 'rescue'` so unexpected fields land in `_rescued_data` rather than failing the stream. For COPY INTO: add `'badRecordsPath' = 'abfss://ops@mystorageaccount.dfs.core.windows.net/bad_records/<table>/'` to `COPY_OPTIONS` to route bad records to a container **outside** the source data hierarchy (e.g., an `ops` container) — placing bad records in the source container causes COPY INTO to attempt re-ingestion of those files on subsequent runs. Monitor the rescue path and bad records path as part of your pipeline health checks. |
