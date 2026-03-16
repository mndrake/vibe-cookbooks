# Ingestion Cookbook
## Databricks

> **Scope:** This cookbook covers ingestion using Databricks platform features only — Auto Loader, COPY INTO, SFTP connector, Structured Streaming, JDBC, Lakeflow Connect, and Partner Connectors. For multi-hop pipeline orchestration using Lakeflow Spark Declarative Pipelines (SDP), see `../processing/processing_cookbook.md`.

---

## Introduction

This cookbook provides practical, step-by-step guidance for data ingestion on Databricks using only the Databricks native toolchain. It covers file ingestion, streaming ingestion, database ingestion, and managed ingestion.

The architectural rationale for choosing between methods is covered in `ingestion_patterns.md` in the same directory.

**How to use this cookbook with the patterns doc:** Use `ingestion_patterns.md` to select a method based on your source type, latency, and operational requirements, then return here for the implementation. Quick navigation:

| If your source is... | Jump to... |
|----------------------|-----------|
| Files arriving in cloud storage (continuous) | [File Ingestion — Auto Loader](#file-ingestion--auto-loader) |
| Files arriving on a schedule (batch) | [File Ingestion — COPY INTO](#file-ingestion--copy-into) |
| SFTP partner delivery | [File Ingestion — SFTP](#file-ingestion--sftp-native-databricks-connector) |
| Kafka / Azure Event Hubs | [Streaming Ingestion — Structured Streaming](#streaming-ingestion--structured-streaming) |
| Relational database (SQL Server, PostgreSQL) | [Database Ingestion — JDBC](#database-ingestion--jdbc) |
| SaaS application (Salesforce, Workday) | [Managed Ingestion — Lakeflow Connect](#managed-ingestion--lakeflow-connect) |
| Third-party connector (Fivetran, Airbyte) | [Managed Ingestion — Partner Connectors](#managed-ingestion--partner-connectors-fivetran-airbyte) |

---

## Infrastructure Prerequisites

Before running any example in this cookbook, ensure the following infrastructure is in place. These are one-time setup tasks typically performed by a workspace admin.

### Databricks Runtime Version

All examples in this cookbook require **Databricks Runtime (DBR) 13.3 LTS or later**. Specific minimum version requirements:

| Feature | Minimum DBR |
|---------|-------------|
| Auto Loader `schemaEvolutionMode`, `trigger(availableNow=True)` | 11.3 LTS |
| SFTP native connector | 13.3 LTS |
| Liquid Clustering (referenced in performance cookbook) | 13.3 LTS |

**Recommendation:** Use **DBR 14.3 LTS or later** for new workloads — it is the current long-term support release as of March 2026 and includes all features referenced in this cookbook.

### Cluster Configuration

| Scenario | Recommended Configuration |
|----------|--------------------------|
| Auto Loader / JDBC batch jobs | Job cluster, auto-terminate after job; start with 2–4 workers, scale based on actual throughput |
| Structured Streaming (continuous) | Job cluster with auto-scaling, or a Databricks Continuous Job; always-on incurs continuous cost |
| JDBC with `numPartitions = 8` | At least 4 workers so partitions distribute across executors; single-node clusters will serialise reads |
| One-off loads / development | All-purpose cluster; not recommended for production recurring jobs due to cost and contention |

See [Cluster configuration — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/compute/configure) and the `performance_cookbook.md` in this repository for sizing guidance.

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
CREATE SCHEMA IF NOT EXISTS main.bronze_fivetran;
```

See [Unity Catalog — getting started](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started) for workspace enablement steps.

### Storage Access (ADLS Gen2)

Every `abfss://` URI in this cookbook requires the Databricks cluster to be authorised to read from or write to the storage account. Configure access via a **Unity Catalog external location** backed by a storage credential (managed identity or service principal). This is a one-time admin task per storage account.

See [External locations — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-external-locations-and-credentials) for setup instructions.

### Databricks Secrets

All credential-dependent examples use `dbutils.secrets.get(scope="...", key="...")`.

> **Recommended: Azure Key Vault-backed secret scopes.** On Azure Databricks, create secret scopes backed by Azure Key Vault so that credentials are managed centrally in Key Vault and consumed transparently via `dbutils.secrets.get()`. This is the standard enterprise approach — secrets are governed by Key Vault access policies rather than per-scope ACLs in Databricks, and rotation is handled in Key Vault without changes to notebook code. Create an AKV-backed scope via the Databricks UI: **Settings → Developer → Manage secret scopes → Create** and provide your Key Vault DNS name and resource ID. See [Azure Key Vault-backed secret scopes — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes) for the full setup guide.
>
> If Key Vault is not available, create Databricks-managed scopes using the CLI (shown below). The `dbutils.secrets.get()` call is identical regardless of scope backend.

```bash
# Databricks-managed scope fallback (use AKV-backed scopes in production)
databricks secrets create-scope --scope jdbc-secrets
databricks secrets create-scope --scope sftp-secrets
databricks secrets create-scope --scope eventhubs-secrets

# Add a secret (prompts for value — value is never stored in shell history)
databricks secrets put-secret --scope jdbc-secrets --key sql-user
databricks secrets put-secret --scope jdbc-secrets --key sql-password
```

See [Databricks Secrets — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secrets) for full documentation including permissions management.

---

## File Ingestion

### File Ingestion — Auto Loader

> **Architecture diagram:** [Auto Loader overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/) includes diagrams of the checkpoint-based file tracking mechanism and the difference between directory listing and file notification discovery modes.

Auto Loader (`cloudFiles` format) incrementally ingests files from cloud storage (ADLS Gen2, S3, GCS) into Delta Lake. It tracks which files have been processed using a checkpoint directory, providing exactly-once delivery guarantees without manual tracking.

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
    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
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
- **`schemaEvolutionMode` choices:** `addNewColumns` for bronze ingestion where all source columns must be captured. `failOnNewColumns` for silver/gold tables where schema drift should trigger investigation. `rescue` for highly variable sources.
- **`trigger(availableNow=True)` vs. `trigger(once=True)`:** `availableNow=True` is the modern replacement for the deprecated `once=True`. Use `availableNow=True` for all new pipelines.
- **Target table creation:** `.toTable("main.bronze.orders")` creates the Delta table automatically on first run if it does not exist, provided the executing principal has `CREATE TABLE` on the target schema. No `CREATE TABLE` DDL is required before the first run.
- **File discovery mode:** Auto Loader defaults to **directory listing** mode — it polls the storage path on each trigger cycle to find new files. For landing zones with thousands of files or high file-arrival frequency, consider **file notification mode**, which uses cloud storage events (Azure Event Grid / SQS) to detect new files with lower latency and reduced API costs: `.option("cloudFiles.useNotifications", "true")`. File notification mode requires a one-time setup of a storage queue resource. See [Auto Loader file detection modes](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/file-detection-modes) for setup steps.

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
        'header' = 'true',
        'inferSchema' = 'true',
        'delimiter' = ','
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
  'header'       = 'true',
  'inferSchema'  = 'true',
  'delimiter'    = ','
)
COPY_OPTIONS (
  'mergeSchema'  = 'false'
);

-- Validate: check row count and latest load timestamp
SELECT
    COUNT(*) AS rows_loaded,
    MAX(_metadata.file_modification_time) AS latest_file
FROM main.bronze.sales_transactions;
```

#### Discussion and Concerns

- **COPY INTO vs. Auto Loader:** COPY INTO is simpler — no streaming context, no checkpoint directory, pure SQL — but does not support schema evolution. Auto Loader with `addNewColumns` is the better choice when schema drift is expected.
- **Idempotency scope:** COPY INTO tracks loaded files per Delta table. If the target table is dropped and recreated, COPY INTO reloads all files on the next run.
- **`inferSchema = 'true'` for bronze CSV:** Schema inference is acceptable at the bronze layer when column types are not known in advance — for example, raw CSV files from an external partner. For tables with fixed, known column types, always define the DDL explicitly and omit `inferSchema`.

#### See Also

- [COPY INTO — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)

---

### File Ingestion — SFTP (Native Databricks Connector)

> ⚠️ **Public preview as of March 2026.** Not recommended for critical production workloads without validating preview stability in your environment. Monitor the [release notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/) for the GA announcement.

The Databricks native SFTP connector reads files directly from an SFTP server into a Spark DataFrame or Delta table, without custom Python file-transfer code or an intermediate cloud storage landing zone.

#### Problem

A partner delivers CSV files to an SFTP server nightly. The files must be ingested into a bronze Delta table without building a custom download script or provisioning an intermediate ADLS landing zone.

#### Solution

Use `spark.read.format("sftp")` (batch) or `spark.readStream.format("sftp")` (incremental) with credentials in Databricks Secrets.

##### Python

```python
sftp_host = "sftp.partner.example.com"
sftp_user = dbutils.secrets.get(scope="sftp-secrets", key="sftp-user")
sftp_password = dbutils.secrets.get(scope="sftp-secrets", key="sftp-password")

# Batch read
df = (
    spark.read.format("sftp")
    .option("host", sftp_host)
    .option("username", sftp_user)
    .option("password", sftp_password)
    .option("fileType", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "/outbound/orders/")
    .load()
)
df.write.format("delta").mode("append").saveAsTable("main.bronze.partner_orders")

# Incremental read with checkpoint
checkpoint_path = "abfss://checkpoints@mystorageaccount.dfs.core.windows.net/sftp_partner_orders"
(
    spark.readStream.format("sftp")
    .option("host", sftp_host)
    .option("username", sftp_user)
    .option("password", sftp_password)
    .option("fileType", "csv")
    .option("header", "true")
    .option("path", "/outbound/orders/")
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("main.bronze.partner_orders")
)
```

##### SQL

SQL does not support direct SFTP reads. Use Python to land data into Delta, then query with SQL:

```sql
SELECT
    COUNT(*)        AS total_rows,
    MIN(order_date) AS earliest_order,
    MAX(order_date) AS latest_order
FROM main.bronze.partner_orders;
```

#### Discussion and Concerns

- **Public preview:** As of March 2026, this connector is in public preview ([docs](https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp)). Test in non-production before using in critical pipelines.
- **GA alternative:** Download files from SFTP to ADLS using `paramiko`, then process from cloud storage with Auto Loader or COPY INTO — this two-stage approach is fully GA.
- **Batch mode re-run safety:** The batch `spark.read.format("sftp")` path has no built-in file tracking. If the job fails and is retried, or is triggered manually, it re-reads all SFTP files and appends them again, creating duplicate rows. For production workloads, prefer the incremental (streaming) path shown above — the checkpoint tracks processed files. If batch is required, add a post-load deduplication step: `DELETE FROM main.bronze.partner_orders WHERE order_id IN (SELECT order_id FROM main.bronze.partner_orders GROUP BY order_id HAVING COUNT(*) > 1 AND _ingested_at < MAX(_ingested_at))`, or use `MERGE` with `ROW_NUMBER()` to keep only the latest record per key.
- **Partial files:** SFTP sources do not provide atomic delivery guarantees. A common convention is for the upstream system to write a zero-byte `.done` file (e.g., `orders_20260314.done`) after completing the corresponding data file upload. The Databricks pipeline lists the SFTP directory at the start of each run, identifies data files that have a matching `.done` file present, and processes only those pairs. The native SFTP connector does not implement this filter natively — it requires a pre-processing step (e.g., a Python Databricks Workflows task) that lists the directory, identifies complete pairs, and passes the confirmed file list to the read task. The GA alternative (paramiko → ADLS → Auto Loader) allows the `.done` check to be performed before files are moved to the landing zone.

#### See Also

- [SFTP ingestion — Azure Databricks (public preview)](https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp)

---

## Streaming Ingestion

### Streaming Ingestion — Structured Streaming

Structured Streaming ingests from Kafka, Azure Event Hubs, or Amazon Kinesis with sub-minute latency and exactly-once semantics via checkpoint.

> **Architecture diagram:** [Structured Streaming programming guide — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/) diagrams the micro-batch execution model, offset tracking, and checkpoint recovery. [Azure Event Hubs with Spark — Microsoft](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-spark-connector) shows the Event Hubs partition-to-Spark-partition mapping.

#### Problem

Order events are published to Azure Event Hubs at high volume and must be written to a bronze Delta table with sub-minute latency, with automatic recovery on failure.

#### Solution

Use `spark.readStream.format("eventhubs")` with a durable checkpoint.

##### Python

```python
connection_string = dbutils.secrets.get(scope="eventhubs-secrets", key="connection-string")
eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        connection_string
    )
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
    .trigger(processingTime="1 minute")
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
- **Azure Event Hubs connector:** Install `com.microsoft.azure:azure-eventhubs-spark_2.12:<version>` as a Maven library via the cluster Libraries tab (Compute → your cluster → Libraries → Install New → Maven). Match the version to your Databricks Runtime's Scala version and check [azure-eventhubs-spark releases](https://github.com/Azure/azure-event-hubs-spark/releases) for the latest compatible version.
- **`sc._jvm` and the encryption call:** `sc` is the `SparkContext`, automatically available in Databricks notebooks. The `sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(...)` call is a Py4J bridge into the Java library — it is required because the Event Hubs connector expects the connection string in encrypted form. This call is only available in Databricks notebook and job cluster environments where the Event Hubs library is installed; it will raise a `NameError` in standalone Python scripts that do not have `sc` pre-initialised.
- **Stream lifecycle — Job vs. notebook:** `trigger(processingTime="1 minute")` starts a continuous stream that never terminates. In a **Databricks Job**, a task must terminate for the job to complete — a non-terminating stream will block the job indefinitely. Use `trigger(availableNow=True)` for job-friendly execution: it processes all available Event Hub partitions and then terminates. Use `trigger(processingTime="1 minute")` only when running in a long-running notebook or in a Databricks Workflows **Continuous Job** type. To stop a running stream gracefully from a notebook: `query = stream.start(); query.awaitTermination(); query.stop()`.

#### See Also

- [Structured Streaming — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/)
- [Table streaming reads and writes — Delta Lake](https://docs.delta.io/latest/delta-streaming.html)

---

## Database Ingestion

### Database Ingestion — JDBC

Spark's JDBC data source reads directly from relational databases over a JDBC connection. It is the standard pattern when data cannot be exported to cloud storage first and no CDC feed is available.

> **Architecture diagram:** [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/external-systems/jdbc) includes a diagram of parallel partition reads showing how `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` split the source table into concurrent range queries.

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
- **Watermark management:** The watermark is derived from `MAX(updated_at)` of the target table at the start of each run, so no manual date update is needed between runs. On the first run the table is empty and the fallback value `1900-01-01` causes a full load.
- **Hard deletes are invisible:** Incremental JDBC based on `updated_at` will not detect deleted rows. Use a CDC tool (Debezium) if delete propagation is required.
- **SQL Server driver:** Included in Databricks Runtime. For PostgreSQL/MySQL, install the driver JAR via the cluster Libraries tab.
- **`query` and `partitionColumn` interaction:** When `query` is specified alongside `partitionColumn`, Spark wraps the query as a subquery for each partition: `SELECT * FROM (<your query>) WHERE order_id BETWEEN <lower> AND <upper>`. SQL Server handles this correctly. If your JDBC driver does not support subquery wrapping, remove the `query` option and use `.option("dbtable", "dbo.orders")` combined with a database view that applies the filter, or remove the partition options and accept a single-partition sequential read.

#### See Also

- [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/external-systems/jdbc)
- [DeltaTable Python API — Delta Lake](https://docs.delta.io/latest/api/python/api/delta.tables.DeltaTable.html)

---

## Managed Ingestion

### Managed Ingestion — Lakeflow Connect

> **Architecture diagram:** [Lakeflow Connect overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) shows the architecture of Lakeflow Connect: SaaS source → Databricks serverless compute → Delta tables under Unity Catalog, with no data transiting third-party infrastructure.

Lakeflow Connect provides Databricks-native managed connectors for SaaS applications and databases. As of March 2026, it is GA for Salesforce, Workday, and SQL Server. Pipelines run on serverless compute within Databricks, governed by Unity Catalog.

#### Problem

The data platform must ingest data from Salesforce without building or maintaining a custom API connector, and without source data transiting third-party infrastructure.

#### Solution

Create a Lakeflow Connect pipeline for Salesforce. The connector handles incremental extraction, schema evolution, and scheduling natively.

> **Before running the code below:** Create the pipeline and configure credentials first. The code below grants permissions to the pipeline's service principal and validates the landed data — it assumes the pipeline already exists.
>
> **UI:** Databricks UI → Ingestion → Create pipeline → select Salesforce → enter your Salesforce OAuth credentials (connected app client ID, client secret, instance URL, and environment type) → select the objects to replicate → configure the destination catalog and schema → set the sync frequency.
>
> **Asset Bundles:** Define the pipeline in `databricks.yml` under `ingestion_pipelines:` and deploy with `databricks bundle deploy`. See [Lakeflow Connect Asset Bundles](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) for the YAML schema and credential configuration reference.

##### Python (Databricks Asset Bundles setup)

```python
# Grant the pipeline service principal access to the destination schema
# Run once by a Unity Catalog admin after provisioning the pipeline

spark.sql("GRANT USE CATALOG ON CATALOG main TO `lakeflow-pipeline-sp`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.bronze_lakeflow TO `lakeflow-pipeline-sp`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.bronze_lakeflow TO `lakeflow-pipeline-sp`")
spark.sql("GRANT MODIFY ON SCHEMA main.bronze_lakeflow TO `lakeflow-pipeline-sp`")

# Verify landed tables
display(spark.sql("SHOW TABLES IN main.bronze_lakeflow"))

# Query ingested data
df_accounts = spark.table("main.bronze_lakeflow.salesforce_account")
display(df_accounts.limit(10))
```

##### SQL

```sql
SHOW TABLES IN main.bronze_lakeflow;

-- Inspect Lakeflow Connect sync metadata
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

- **Connector catalogue:** GA for Salesforce, Workday, SQL Server as of March 2026. Check [documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) for the current list.
- **Unity Catalog required:** Lakeflow Connect requires Unity Catalog — not available with the legacy Hive metastore.
- **Schema evolution:** New columns automatically added. Deleted source columns retained in Delta with `null` values — filter downstream as needed.
- **Asset Bundles CI/CD:** Define pipelines in `databricks.yml` and deploy via the Databricks CLI for source control and environment promotion.

#### See Also

- [Lakeflow Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- [Databricks Asset Bundles — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)

---

### Managed Ingestion — Partner Connectors (Fivetran, Airbyte)

Partner connectors are appropriate where a source is not yet on the Lakeflow Connect catalogue or where an existing connector platform investment is in place.

#### Problem

The data platform needs to ingest from HubSpot, which is not yet supported by Lakeflow Connect. A managed connector with automatic schema migration and backfill is preferred.

#### Solution

Provision a Fivetran or Airbyte connector via Databricks Partner Connect. Grant the connector service principal Unity Catalog privileges. The connector lands data into a dedicated bronze schema.

##### Python

```python
# One-time Unity Catalog setup — run as a catalog admin
spark.sql("GRANT USE CATALOG ON CATALOG main TO `fivetran-sp@myorg.com`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.bronze_fivetran TO `fivetran-sp@myorg.com`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.bronze_fivetran TO `fivetran-sp@myorg.com`")
spark.sql("GRANT MODIFY ON SCHEMA main.bronze_fivetran TO `fivetran-sp@myorg.com`")

display(spark.sql("SHOW TABLES IN main.bronze_fivetran"))
df_contacts = spark.table("main.bronze_fivetran.hubspot_contact")
display(df_contacts.limit(10))
```

##### SQL

```sql
-- Fivetran metadata columns
SELECT
    id,
    email,
    lifecycle_stage,
    _fivetran_synced,
    _fivetran_deleted
FROM main.bronze_fivetran.hubspot_contact
WHERE _fivetran_deleted = false
ORDER BY _fivetran_synced DESC
LIMIT 50;
```

#### Discussion and Concerns

- **Provisioning via Partner Connect:** Databricks UI → Data → Partner Connect → search for Fivetran or Airbyte → Connect → follow the wizard (it provisions a service principal, SQL warehouse connection, and destination schema automatically) → complete source configuration in the partner tool's UI (enter source credentials, select tables, set sync frequency). The service principal name shown in the Partner Connect confirmation screen is the principal to grant Unity Catalog privileges (as in the Python code above).
- **Data transits third-party infrastructure:** Assess against GDPR, HIPAA, and data residency requirements. Obtain a Data Processing Agreement (DPA) for regulated data.
- **Isolate connector schemas:** Grant the connector principal access only to its dedicated schema.
- **`_fivetran_deleted = true` records are retained:** Downstream silver transformations must filter `WHERE _fivetran_deleted = false`.
- **Cost model:** Partner connector pricing (per MAR or per volume) is separate from Databricks DBU costs. Compare against Lakeflow Connect for sources where both options are available.

#### See Also

- [Databricks Partner Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partner-connect/)
- [Fivetran Databricks destination](https://fivetran.com/docs/destinations/databricks)

---

## Sources Not Covered in This Cookbook

Some source systems — ERP platforms, proprietary databases, on-premises applications, mainframes, custom APIs — do not have a native Databricks connector and are not covered by Lakeflow Connect or the major partner connectors. The standard pattern for these sources is a **landing zone approach**:

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
| Auto Loader stream stops with `FileNotFoundException` on checkpoint | Checkpoint directory deleted or moved | Restore from backup; if unavailable, delete checkpoint and reprocess from beginning |
| COPY INTO loads zero rows after schema change | New source files have columns not in target Delta schema | `ALTER TABLE ... ADD COLUMNS (...)` then re-run COPY INTO |
| JDBC job significantly slower | Source table growth; insufficient `numPartitions`; index fragmentation | Increase `numPartitions`; request source DBA to rebuild indexes; use read replica |
| Lakeflow Connect authentication error | OAuth token expired or credentials rotated | Update connection credentials in Lakeflow Connect configuration |
| Partner connector lands duplicate rows | Connector backfill triggered (e.g., after reconnection) | Deduplicate in silver using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_synced DESC)` |
| Auto Loader / COPY INTO encounters malformed or corrupt files | Source file contains rows with unexpected types, extra fields, or corrupt encoding | For Auto Loader: set `cloudFiles.schemaEvolutionMode = 'rescue'` so unexpected fields land in `_rescued_data` rather than failing the stream. For COPY INTO: add `'badRecordsPath' = 'abfss://...'` to `COPY_OPTIONS` to route bad records to a separate path instead of aborting the load. Monitor the rescue path and bad records path as part of your pipeline health checks. |
