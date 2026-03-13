# Ingestion Cookbook

---

## Introduction

This cookbook provides practical, step-by-step guidance for data ingestion on Databricks. It covers the primary ingestion categories used in a modern Databricks data platform — file ingestion, streaming ingestion, database ingestion, managed ingestion, and ad hoc ingestion — as well as dbt-specific ingestion patterns for reference data and Data Vault 2.0 staging.

Each method section is self-contained: it describes the problem being solved, provides working Python and SQL examples using realistic table and column names, and documents the trade-offs and concerns a practitioner should understand before using the method in production.

The architectural rationale for choosing between these methods — latency requirements, cost model, schema evolution strategy, and operational complexity — is covered in `ingestion_patterns.md` in the same directory. This cookbook focuses on implementation.

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

Install the following tools before working with the examples in this cookbook.

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.9+ | Required for PySpark and dbt-databricks |
| Apache Spark via PySpark | 3.4+ | Installed automatically with Databricks Runtime; install locally with `pip install pyspark` for unit testing |
| Databricks CLI | Latest (`databricks-sdk`) | Used for secrets management, workspace interaction, and deploying jobs |
| dbt-databricks | Latest | Required only for the dbt Seeds and AutomateDV Stage Macro sections |

Configure your Databricks CLI connection:

```bash
# Authenticate using OAuth (recommended for interactive use)
databricks auth login --host https://<your-workspace>.azuredatabricks.net

# Verify authentication
databricks clusters list
```

---

## File Ingestion

### File Ingestion — Auto Loader

Auto Loader (`cloudFiles` format) incrementally ingests files from cloud storage (ADLS Gen2, S3, GCS) into Delta Lake. It tracks which files have been processed using a checkpoint directory and a file notification or directory listing mechanism, providing exactly-once delivery guarantees without requiring manual tracking.

#### Problem

Files arrive continuously in an ADLS Gen2 container from an upstream system. The files must be ingested into a Delta table as they arrive, without reprocessing files that have already been loaded and without missing any new arrivals.

#### Solution

Use `spark.readStream.format("cloudFiles")` with a checkpoint directory on durable cloud storage. Write using `writeStream` with `trigger(availableNow=True)` for scheduled micro-batch execution or `trigger(processingTime="5 minutes")` for continuous micro-batch.

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

Auto Loader cannot be configured directly in SQL — use the Python API. Once data is landed in the Delta table, query it with SQL as normal:

```sql
-- Validate records landed correctly
SELECT
    COUNT(*)          AS total_rows,
    MIN(_metadata.file_modification_time) AS earliest_file,
    MAX(_metadata.file_modification_time) AS latest_file
FROM main.bronze.orders;

-- Inspect rescued data for schema drift (if schemaEvolutionMode = 'rescue')
SELECT _rescued_data
FROM main.bronze.orders
WHERE _rescued_data IS NOT NULL
LIMIT 10;
```

#### Discussion and Concerns

- **Checkpoint durability:** The checkpoint directory must be on durable cloud storage (ADLS, S3, GCS). Deleting the checkpoint causes Auto Loader to reprocess all files from the beginning.
- **`schemaEvolutionMode` choices:** `addNewColumns` is appropriate for bronze ingestion where all source columns should be captured. `failOnNewColumns` is appropriate for silver or gold tables where uncontrolled schema drift should trigger an alert. `rescue` captures unexpected columns in `_rescued_data` — useful for highly variable sources.
- **`trigger(availableNow=True)` vs. `trigger(once=True)`:** `availableNow=True` is the modern replacement for the deprecated `once=True`. Use `availableNow=True` for all new pipelines.
- **File notification vs. directory listing:** Auto Loader defaults to directory listing for small volumes and can be configured to use cloud storage event notifications (Azure Event Grid, AWS SQS) for high-volume scenarios where directory listing at scale becomes expensive.

#### See Also

- [Auto Loader documentation — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)
- [Auto Loader schema evolution — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/schema)

---

### File Ingestion — COPY INTO

COPY INTO is an idempotent, SQL-based command that loads files from cloud storage into a Delta table. It tracks which files have been loaded in the Delta table's transaction log, so re-running the same command on the same path will not duplicate data.

#### Problem

A set of CSV files arrives in ADLS Gen2 on a daily schedule and must be loaded into a Delta table. The load must be safe to re-run — if the job fails partway through and is restarted, it must not insert duplicate rows.

#### Solution

Use `COPY INTO` targeting the destination Delta table. Specify the file format and any format options. COPY INTO reads from the path and skips any files it has already loaded.

##### Python

```python
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
  'header' = 'true',
  'inferSchema'  = 'true',
  'delimiter' = ','
)
COPY_OPTIONS (
  'mergeSchema' = 'false'
);

-- Validate: check row count and latest load timestamp
SELECT COUNT(*) AS rows_loaded, MAX(_metadata.file_modification_time) AS latest_file
FROM main.bronze.sales_transactions;
```

#### Discussion and Concerns

- **COPY INTO vs. Auto Loader:** COPY INTO is simpler — no streaming context, no checkpoint directory to manage, pure SQL — but it does not support schema evolution. If new columns appear in the source files, COPY INTO silently drops them. Auto Loader with `addNewColumns` is the better choice for sources where schema drift is expected.
- **Idempotency scope:** COPY INTO tracks loaded files per Delta table. If the target table is dropped and recreated, COPY INTO will reload all files on the next run. This is different from Auto Loader, where the checkpoint is stored independently of the table.
- **Performance:** COPY INTO does not parallelize across a Spark cluster the same way Auto Loader does for very high file volumes. For thousands of small files, Auto Loader's distributed processing may be more efficient.

#### See Also

- [COPY INTO — Azure Databricks SQL reference](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)

---

### File Ingestion — SFTP (Native Databricks Connector)

> ⚠️ **Public preview as of March 2026.** This connector is not recommended for critical production workloads without first validating preview stability in your environment. Preview features may change between releases and are typically excluded from production SLAs. Review the Databricks preview policy and monitor the [release notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/) for the GA announcement.

The Databricks native SFTP connector reads files directly from an SFTP server into a Spark DataFrame or Delta table, without requiring custom Python file-transfer code (`paramiko`) or an intermediate cloud storage landing zone.

#### Problem

A partner delivers CSV files to an SFTP server on a nightly schedule. The files must be ingested into a bronze Delta table without building and maintaining a bespoke Python download script, and without the operational overhead of provisioning and managing an intermediate ADLS landing zone.

#### Solution

Use `spark.read.format("sftp")` (batch) or `spark.readStream.format("sftp")` (incremental) with credentials stored in Databricks Secrets.

##### Python

```python
# Credentials from Databricks Secrets — never hardcode in notebooks
sftp_host = "sftp.partner.example.com"
sftp_user = dbutils.secrets.get(scope="sftp-secrets", key="sftp-user")
sftp_password = dbutils.secrets.get(scope="sftp-secrets", key="sftp-password")

# Batch read: load all CSV files from a remote SFTP directory
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

# Incremental read using streaming with checkpoint
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

SQL does not support direct SFTP reads. Use the Python approach above to land data into Delta, then query with SQL:

```sql
-- Validate records ingested from SFTP
SELECT
    COUNT(*)     AS total_rows,
    MIN(order_date) AS earliest_order,
    MAX(order_date) AS latest_order
FROM main.bronze.partner_orders;

-- Check for obvious data quality issues
SELECT *
FROM main.bronze.partner_orders
WHERE order_id IS NULL
   OR order_date IS NULL
LIMIT 20;
```

#### Discussion and Concerns

- **Public preview:** As of March 2026, the Databricks native SFTP connector is in public preview ([documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp)). Test thoroughly in a non-production environment before promoting to production. Do not use for critical pipelines without evaluating preview stability and understanding that the feature may change.
- **Alternative two-stage pattern (GA):** For environments where the preview connector is not appropriate, the established GA pattern is: (1) download files from SFTP to ADLS/S3 using a Python notebook or job with `paramiko`, (2) process from cloud storage using Auto Loader or COPY INTO. This two-stage approach gives full control over file transfer retry logic and partial file handling.
- **Partial files:** SFTP sources do not provide atomic file delivery guarantees. A file may be visible in the directory listing while still being written by the upstream system. Coordinate with the partner on a file-ready signalling convention (e.g., a `.done` sentinel file) to avoid reading partial files.
- **Credential management:** Store SFTP passwords and private keys in Databricks Secrets. For key-based authentication, store the private key content as a secret and reference it via the `privateKey` option.
- **SFTP server compatibility:** Not all SFTP server implementations expose consistent directory listing and file attribute APIs. Validate connectivity and incremental file detection against the specific SFTP server software before committing.

#### See Also

- [SFTP ingestion — Azure Databricks (public preview)](https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp)
- `ingestion_patterns.md` — Ingestion method selection decision table

---

## Streaming Ingestion

### Streaming Ingestion — Structured Streaming

Structured Streaming is Spark's unified streaming API. It ingests from Kafka, Azure Event Hubs, Amazon Kinesis, or Delta tables (Delta Change Data Feed) and writes to Delta Lake with exactly-once semantics using a checkpoint.

#### Problem

Order events are published to an Azure Event Hub at high volume and must be written to a bronze Delta table with sub-minute latency. The pipeline must recover automatically from failures without replaying events already written.

#### Solution

Use `spark.readStream.format("eventhubs")` with the Azure Event Hubs connector. Write using `writeStream` with a durable checkpoint.

##### Python

```python
from pyspark.sql.functions import col, from_json, schema_of_json

# Event Hubs connection string from Databricks Secrets
connection_string = dbutils.secrets.get(scope="eventhubs-secrets", key="connection-string")

eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        connection_string
    )
}

checkpoint_path = "abfss://checkpoints@mystorageaccount.dfs.core.windows.net/orders_stream"

# Define the schema of the JSON payload in the Event Hub body
order_schema = "order_id STRING, customer_id STRING, order_total DOUBLE, order_date TIMESTAMP"

(
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
    # Event Hub body is binary — cast to string then parse JSON
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

Structured Streaming cannot be configured in SQL. Use the Python API to run the stream. Query the resulting Delta table with SQL:

```sql
-- Monitor stream lag by comparing event time to ingest time
SELECT
    DATE_TRUNC('minute', enqueuedTime) AS event_minute,
    COUNT(*)                           AS event_count,
    AVG(UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(enqueuedTime)) AS avg_lag_seconds
FROM main.bronze.orders_stream
WHERE enqueuedTime >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY 1
ORDER BY 1 DESC;
```

#### Discussion and Concerns

- **Checkpoint location is critical:** The checkpoint directory holds the consumer offset state. Deleting it causes the stream to restart from the beginning of the Event Hub's retention window (or the configured starting position). Store checkpoints in durable cloud storage, never on DBFS root or ephemeral cluster storage.
- **`trigger(processingTime)` vs. `trigger(availableNow=True)`:** For a continuously running stream, use `processingTime`. For a scheduled micro-batch that terminates after each run (cheaper), use `availableNow=True` in a Databricks Job.
- **Schema changes:** If the JSON schema in the Event Hub changes (new fields), the stream will silently drop unknown fields unless `mergeSchema` is enabled and `schemaEvolutionMode` is configured. Coordinate schema changes with the upstream producer.
- **Event Hubs connector version:** The Azure Event Hubs connector for Spark (`com.microsoft.azure:azure-eventhubs-spark`) must be installed on the cluster as a Maven library. Check the Databricks Runtime release notes for the recommended version.

#### See Also

- [Structured Streaming — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/)
- [Azure Event Hubs connector for Spark — GitHub](https://github.com/Azure/azure-event-hubs-spark)

---

### Streaming Ingestion — Delta Live Tables (DLT)

Delta Live Tables is Databricks' declarative pipeline framework. You declare the transformations; DLT manages compute, retries, checkpointing, and data quality enforcement.

#### Problem

A medallion pipeline (bronze → silver → gold) must be built for customer order data. The team wants built-in data quality checks, automatic schema inference, and managed cluster lifecycle — without writing custom orchestration logic or managing checkpoint directories.

#### Solution

Define pipeline tables using `@dlt.table` decorators and `@dlt.expect` annotations for data quality. Deploy as a DLT pipeline (triggered or continuous) via the Databricks UI, CLI, or Databricks Asset Bundles.

##### Python

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

# Bronze: raw ingest from Auto Loader source
@dlt.table(
    name="orders_bronze",
    comment="Raw orders landed from ADLS via Auto Loader",
    table_properties={"quality": "bronze"}
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/pipelines/orders/schema")
        .load("abfss://raw@mystorageaccount.dfs.core.windows.net/orders/")
    )

# Silver: cleansed and validated
@dlt.table(
    name="orders_silver",
    comment="Cleansed orders with data quality enforcement",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_total",  "order_total > 0")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
        .select(
            col("order_id"),
            col("customer_id"),
            col("order_total").cast("double"),
            col("order_date").cast("date"),
            current_timestamp().alias("_ingested_at")
        )
    )
```

##### SQL

```sql
-- Bronze table definition in SQL DLT syntax
CREATE OR REFRESH STREAMING TABLE orders_bronze
COMMENT 'Raw orders landed from ADLS via Auto Loader'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT * FROM cloud_files(
  'abfss://raw@mystorageaccount.dfs.core.windows.net/orders/',
  'json',
  map('cloudFiles.schemaLocation', '/pipelines/orders/schema')
);

-- Silver table with quality constraints
CREATE OR REFRESH STREAMING TABLE orders_silver (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_total  EXPECT (order_total > 0)     ON VIOLATION DROP ROW
)
COMMENT 'Cleansed and validated orders'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
    order_id,
    customer_id,
    CAST(order_total AS DOUBLE) AS order_total,
    CAST(order_date  AS DATE)   AS order_date,
    current_timestamp()         AS _ingested_at
FROM STREAM(LIVE.orders_bronze);
```

#### Python vs. SQL Differences

| Aspect | Python | SQL |
|--------|--------|-----|
| Data quality annotations | `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail` decorators | `CONSTRAINT ... EXPECT ... ON VIOLATION` clause in table DDL |
| Complex transformations | Full PySpark DataFrame API available | Limited to Spark SQL expressions |
| Reusable functions | Python functions can be imported and reused across pipeline files | SQL DLT is declarative per statement; no function reuse across definitions |
| Schema inference | Automatic from readStream source | Automatic from `cloud_files()` and STREAM() sources |

#### Discussion and Concerns

- **DLT incurs a DBU premium:** DLT pipelines run on DLT-managed clusters with a higher DBU multiplier than standard job clusters. For cost-sensitive workloads, evaluate whether a standard Structured Streaming job achieves the same outcome at lower cost.
- **Pipeline mode:** Triggered mode (the default) runs the pipeline once and terminates. Continuous mode runs indefinitely with low-latency processing. For most batch-oriented bronze ingestion, triggered mode is appropriate and significantly cheaper.
- **Data quality metrics:** DLT records expectation pass/fail counts in the pipeline event log (`system.lakeflow.*` tables). These are invaluable for monitoring data quality trends over time.

#### See Also

- [Delta Live Tables overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/)
- [DLT expectations — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations)

---

## Database Ingestion

### Database Ingestion — JDBC

Spark's JDBC data source reads directly from relational databases (SQL Server, PostgreSQL, MySQL, Oracle) over a JDBC connection. It is the standard pattern when data cannot be exported to cloud storage first and no CDC feed is available.

#### Problem

A source system is an Azure SQL Database that must be ingested into Delta Lake on a scheduled basis. The database does not produce file exports. The table contains tens of millions of rows, so a single-threaded read must be parallelised to meet the ingestion window.

#### Solution

Use `spark.read.format("jdbc")` with partition configuration to parallelise the read. Write the result to Delta using MERGE for incremental loads or overwrite for full loads.

##### Python

```python
# Credentials from Databricks Secrets
jdbc_url = "jdbc:sqlserver://myserver.database.windows.net:1433;database=SourceDB"
jdbc_user = dbutils.secrets.get(scope="jdbc-secrets", key="sql-user")
jdbc_password = dbutils.secrets.get(scope="jdbc-secrets", key="sql-password")

# Parallel incremental read — partitioned by order_id range
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    # Incremental filter — only rows updated since the last run
    .option("query",
        "SELECT * FROM dbo.orders WHERE updated_at >= '2026-03-12 00:00:00'"
    )
    # Partition the read into 8 parallel queries across order_id range
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "8")
    .load()
)

# Incremental merge into Delta target
from delta.tables import DeltaTable

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
-- Note: SQL cannot pass runtime credentials directly to JDBC options.
-- Use the Python approach above to load data, then query the Delta target with SQL.

-- Alternatively, using a pre-registered Unity Catalog connection (if configured):
-- CREATE FOREIGN TABLE ... allows SQL-based access without embedding credentials.

-- Validate incremental load result
SELECT
    COUNT(*)          AS rows_in_target,
    MAX(updated_at)   AS latest_updated_at
FROM main.bronze.orders;

-- Spot-check for expected incremental records
SELECT *
FROM main.bronze.orders
WHERE updated_at >= current_date - 1
ORDER BY updated_at DESC
LIMIT 20;
```

#### Python vs. SQL Differences

| Aspect | Python | SQL |
|--------|--------|-----|
| Credential injection | `dbutils.secrets.get()` at runtime — secure and auditable | SQL JDBC options cannot reference Databricks Secrets directly; credentials would need to be hardcoded or passed via session parameters — avoid this |
| Parallelism control | Full control via `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound` | Not available in SQL JDBC options |
| Incremental MERGE | Full PySpark DeltaTable merge API | SQL `MERGE INTO` works well against Delta targets once data is staged |

#### Discussion and Concerns

- **Parallel reads increase source load:** `numPartitions = 8` issues 8 concurrent queries. Coordinate with the source DBA to confirm connection limits. Use a read replica where available.
- **Hard deletes are invisible:** Incremental JDBC based on `updated_at` will not detect rows deleted from the source. Implement a periodic full reconciliation job or use a CDC tool (Debezium) if delete propagation is required.
- **`query` and `partitionColumn` are mutually exclusive in some driver versions:** When using a custom `query` option, some JDBC drivers require wrapping it in a subquery for Spark to apply partition splits. If partition splits do not apply, use `dbtable` with a WHERE clause in a view or inline SQL instead.
- **SQL Server driver:** Included in Databricks Runtime. For PostgreSQL (`org.postgresql.Driver`) and MySQL (`com.mysql.cj.jdbc.Driver`), install the driver JAR via the cluster Libraries tab.

#### See Also

- [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/external-systems/jdbc)
- [Spark JDBC data source options — Apache Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
- `ingestion_patterns.md` — Database Ingestion decision criteria

---

## Managed Ingestion

### Managed Ingestion — Lakeflow Connect

Lakeflow Connect provides Databricks-native managed connectors for ingesting data from SaaS applications and databases. As of March 2026, it is generally available for Salesforce, Workday, and SQL Server, with additional connectors available in preview. Pipelines run on serverless compute, are governed by Unity Catalog, and are orchestrated by Lakeflow Jobs — no external infrastructure is required.

#### Problem

The data platform must ingest data from Salesforce (accounts, opportunities, contacts) on a scheduled basis. The data engineering team does not have capacity to build and maintain a custom Salesforce API connector. The organisation requires that source data not transit third-party infrastructure, and wants ingestion pipelines visible in Unity Catalog lineage.

#### Solution

Create a Lakeflow Connect pipeline for Salesforce via the Databricks UI, CLI, or Databricks Asset Bundles. The connector handles incremental extraction, schema evolution, and scheduling.

##### Python (Databricks Asset Bundles / SDK)

```python
# Databricks Asset Bundle pipeline definition (databricks.yml excerpt)
# Deploy with: databricks bundle deploy --target prod

# pipeline definition in resources/lakeflow_salesforce.yml:
# pipelines:
#   salesforce_ingestion:
#     name: "salesforce_bronze_ingestion"
#     ingestion_pipeline:
#       connection_name: "salesforce_prod_connection"
#       objects:
#         - source_catalog: salesforce
#           source_schema: salesforce
#           source_table_name: Account
#           destination_catalog: main
#           destination_schema: bronze_lakeflow
#           destination_table_name: salesforce_account
#         - source_catalog: salesforce
#           source_schema: salesforce
#           source_table_name: Opportunity
#           destination_catalog: main
#           destination_schema: bronze_lakeflow
#           destination_table_name: salesforce_opportunity

# Grant the pipeline service principal access to the destination schema
spark.sql("GRANT USE CATALOG ON CATALOG main TO `lakeflow-pipeline-sp`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.bronze_lakeflow TO `lakeflow-pipeline-sp`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.bronze_lakeflow TO `lakeflow-pipeline-sp`")
spark.sql("GRANT MODIFY ON SCHEMA main.bronze_lakeflow TO `lakeflow-pipeline-sp`")

# Once the pipeline has run, query the ingested data
df_accounts = spark.table("main.bronze_lakeflow.salesforce_account")
display(df_accounts.limit(10))
```

##### SQL

```sql
-- Verify the pipeline has landed tables in the destination schema
SHOW TABLES IN main.bronze_lakeflow;

-- Inspect metadata added by Lakeflow Connect
-- _databricks_synced: timestamp of the last sync for this row
SELECT
    id,
    name,
    industry,
    annual_revenue,
    _databricks_synced
FROM main.bronze_lakeflow.salesforce_account
ORDER BY _databricks_synced DESC
LIMIT 50;

-- Count rows per sync batch to validate incremental loading
SELECT
    DATE_TRUNC('hour', _databricks_synced) AS sync_hour,
    COUNT(*)                               AS rows_synced
FROM main.bronze_lakeflow.salesforce_account
GROUP BY 1
ORDER BY 1 DESC;
```

#### Discussion and Concerns

- **Connector catalogue:** As of March 2026, generally available connectors include Salesforce, Workday, and SQL Server. Google Analytics and ServiceNow are available for additional sources. Check the [Lakeflow Connect documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) for the current catalogue and preview status of individual connectors.
- **Unity Catalog requirement:** Lakeflow Connect requires Unity Catalog. It is not available in workspaces using the legacy Hive metastore.
- **Automatic schema evolution:** New columns in the source are automatically added to the Delta target on the next pipeline run. Deleted source columns are retained in Delta with `null` values — downstream transformations must handle this.
- **Serverless compute pricing:** Lakeflow Connect pipelines run on serverless compute, billed per DBU consumed during pipeline execution. There is no per-row or per-record fee, unlike some third-party connector services.
- **CI/CD with Asset Bundles:** Lakeflow Connect pipelines can be defined as code in `databricks.yml` and deployed via Databricks Asset Bundles, enabling source control, peer review, and environment promotion (dev → staging → prod).

#### See Also

- [Lakeflow Connect overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/)
- [Databricks Asset Bundles — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)
- `ingestion_patterns.md` — Managed Ingestion decision criteria

---

### Managed Ingestion — Partner Connectors (Fivetran, Airbyte)

Partner connectors are third-party managed services that extract data from source systems and land it in Delta tables. They are appropriate where the source is not yet supported by Lakeflow Connect or where an existing connector platform investment is in place.

#### Problem

The data platform needs to ingest data from HubSpot (a SaaS CRM not currently supported by Lakeflow Connect natively). The data engineering team wants a managed connector with automatic schema migration and backfill, not a custom API integration.

#### Solution

Provision a Fivetran or Airbyte connector via Databricks Partner Connect. Grant the connector service principal the necessary Unity Catalog privileges. The connector lands data into a dedicated bronze schema; downstream dbt or PySpark pipelines transform it.

##### Python

```python
# One-time setup: grant the Fivetran service principal access to the target schema
# Run once by a Unity Catalog admin after provisioning the connector

spark.sql("GRANT USE CATALOG ON CATALOG main TO `fivetran-sp@myorg.com`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.bronze_fivetran TO `fivetran-sp@myorg.com`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.bronze_fivetran TO `fivetran-sp@myorg.com`")
spark.sql("GRANT MODIFY ON SCHEMA main.bronze_fivetran TO `fivetran-sp@myorg.com`")

# Verify tables have been landed by the connector
display(spark.sql("SHOW TABLES IN main.bronze_fivetran"))

# Read connector output for downstream transformation
df_contacts = spark.table("main.bronze_fivetran.hubspot_contact")
display(df_contacts.limit(10))
```

##### SQL

```sql
-- Verify connector-landed tables are accessible
SHOW TABLES IN main.bronze_fivetran;

-- Fivetran adds metadata columns to every table:
--   _fivetran_synced   — timestamp of last sync
--   _fivetran_deleted  — soft-delete flag (true = deleted in source)
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

-- Airbyte metadata columns (if using Airbyte instead of Fivetran):
--   _airbyte_raw_id, _airbyte_extracted_at, _airbyte_normalized_at
SELECT
    id,
    email,
    _airbyte_extracted_at
FROM main.bronze_airbyte.hubspot_contact
ORDER BY _airbyte_extracted_at DESC
LIMIT 50;
```

#### Discussion and Concerns

- **Data transits third-party infrastructure:** Partner connectors route data through the connector vendor's infrastructure before delivering to Databricks. Assess against GDPR, HIPAA, data residency requirements, and contractual obligations. Obtain a Data Processing Agreement (DPA) from the vendor for any regulated or personally identifiable data.
- **Isolate connector schemas:** Grant the connector service principal access only to a dedicated schema (e.g., `bronze_fivetran`). Do not share this schema with manually managed tables — the connector has `MODIFY` permission and can overwrite tables in the schema.
- **Connector metadata columns must be filtered downstream:** Fivetran's `_fivetran_deleted = true` records are not physically deleted from Delta — they are soft-delete markers. Downstream silver transformations must filter `WHERE _fivetran_deleted = false` to exclude deleted records.
- **Schema changes:** Fivetran automatically adds new columns to the Delta target. Dropped source columns are retained with `null` values. Understand the connector's specific schema migration behaviour before relying on the target schema in downstream models.
- **Cost model:** Partner connector costs are typically based on monthly active rows (MAR) or data volume, billed separately from Databricks DBUs. Include connector costs in the platform cost model and compare against Lakeflow Connect's serverless DBU pricing for sources where both options are available.

#### See Also

- [Databricks Partner Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partner-connect/)
- [Fivetran Databricks destination documentation](https://fivetran.com/docs/destinations/databricks)
- [Airbyte Databricks destination documentation](https://docs.airbyte.com/integrations/destinations/databricks)
- `ingestion_patterns.md` — Managed Ingestion decision criteria and governance considerations

---

## Ad Hoc Ingestion

### Ad Hoc Ingestion — Notebook Pattern

The Notebook Pattern is a direct `spark.read` / `spark.write` approach executed manually in a Databricks notebook. It is appropriate for one-off historical backfills and exploratory data investigation. It is **not appropriate for any recurring production pipeline**.

#### Problem

A one-time historical backfill of three years of order data must be loaded from a set of Parquet files in ADLS into a Delta table. This is a single human-initiated operation — it will never be scheduled or repeated.

#### Solution

Read the files with `spark.read`, apply any necessary transformations, and write to Delta. Document the run in a notebook comment or markdown cell for auditability.

##### Python

```python
# One-time backfill: read three years of historical Parquet files
df = (
    spark.read
    .format("parquet")
    .option("mergeSchema", "true")
    .load("abfss://archive@mystorageaccount.dfs.core.windows.net/orders/2023/")
)

# Basic validation before writing
print(f"Row count: {df.count():,}")
print(f"Schema: {df.dtypes}")
df.show(5)

# Write to Delta — overwrite mode for a clean backfill into a new table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("main.bronze.orders_historical_backfill")
)

print("Backfill complete.")
```

##### SQL

```sql
-- Create the target table and load from Parquet in one statement (Unity Catalog)
CREATE OR REPLACE TABLE main.bronze.orders_historical_backfill
USING DELTA
AS
SELECT *
FROM parquet.`abfss://archive@mystorageaccount.dfs.core.windows.net/orders/2023/`;

-- Validate row count
SELECT COUNT(*) AS total_rows FROM main.bronze.orders_historical_backfill;
```

#### Python vs. SQL Differences

| Aspect | Python | SQL |
|--------|--------|-----|
| Pre-write validation | Easy — inspect schema, row count, sample rows before committing the write | SQL `CREATE TABLE AS SELECT` executes atomically; preview requires a separate SELECT first |
| Schema merging | `mergeSchema` option handles files with slightly different schemas across the directory | `CREATE TABLE AS SELECT` from `parquet.` path infers a unified schema automatically |
| Error handling | Try/except blocks can handle format or schema errors before the write | SQL statement fails atomically; no partial writes |

#### Discussion and Concerns

- **No deduplication or state tracking:** The Notebook Pattern has no mechanism to prevent duplicate loads if run more than once. Running the same notebook twice on the same source data will produce duplicate rows unless the write is `overwrite` mode.
- **Not for production scheduling:** Never schedule a notebook as a recurring Databricks Job as a substitute for a proper ingestion pipeline. Use Auto Loader, COPY INTO, or DLT for recurring loads.
- **Auditability:** Record the backfill in the notebook's markdown cells or a dedicated run log table: who ran it, when, what source path, how many rows, and why. This is the only audit trail available for manual notebook runs.

#### See Also

- [spark.read API — Apache Spark](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.read.html)

---

## dbt Ingestion Patterns

### dbt Seeds

dbt seeds load small, static CSV files from the dbt project repository into Delta tables. They are version-controlled, testable with dbt's test framework, and appropriate for reference data (country codes, currency codes, status mappings) that changes rarely and where change history matters.

#### Problem

A country code reference table is needed in the silver layer. The table contains 250 rows (ISO 3166 country codes), changes at most once a year when a new country code is added, and must be auditable — every change to the reference data should be traceable to a pull request and commit.

#### Solution

Place the CSV in the `seeds/` directory of the dbt project, declare column types and tests in `schema.yml`, and run `dbt seed` to load into Delta.

##### Seeds CSV (`seeds/ref_country_codes.csv`)

```csv
country_code,country_name,region,is_active
GB,United Kingdom,EMEA,true
US,United States,AMER,true
DE,Germany,EMEA,true
JP,Japan,APAC,true
```

##### dbt schema.yml (`seeds/schema.yml`)

```yaml
version: 2

seeds:
  - name: ref_country_codes
    description: "ISO 3166 country code reference table. Updated via PR when new codes are added."
    config:
      column_types:
        country_code: varchar(2)
        country_name: varchar(100)
        region: varchar(10)
        is_active: boolean
    columns:
      - name: country_code
        description: "ISO 3166-1 alpha-2 country code"
        tests:
          - unique
          - not_null
      - name: region
        tests:
          - not_null
          - accepted_values:
              values: ['EMEA', 'AMER', 'APAC', 'LATAM']
```

##### Python (run via CLI, not notebook)

```bash
# Load seeds into the Databricks target
dbt seed --target prod

# Full refresh — truncate and reload (use when CSV has been significantly changed)
dbt seed --full-refresh --target prod

# Run tests against the seed table
dbt test --select ref_country_codes --target prod
```

##### SQL (validate after load)

```sql
-- Verify seed loaded correctly
SELECT COUNT(*) AS total_rows FROM main.silver.ref_country_codes;

-- Check for any unexpected region values
SELECT DISTINCT region FROM main.silver.ref_country_codes;

-- Join seed to a fact table to verify referential integrity
SELECT
    o.order_id,
    o.ship_country,
    c.country_name,
    c.region
FROM main.silver.orders AS o
LEFT JOIN main.silver.ref_country_codes AS c
  ON o.ship_country = c.country_code
WHERE c.country_code IS NULL   -- orders with unmatched country codes
LIMIT 20;
```

#### Discussion and Concerns

- **Size limit:** dbt seeds are loaded via a generated `INSERT` statement, not a bulk load. Keep seeds below a few thousand rows. For larger reference datasets, use COPY INTO or Auto Loader from a cloud storage source.
- **Non-technical update process:** Seeds require a git commit and `dbt seed` run to update. If business analysts need to update reference data without git access, a Delta table loaded from a shared ADLS CSV file (via COPY INTO) is more appropriate.
- **Full refresh implications:** `dbt seed --full-refresh` truncates and reloads the table. This is safe for seeds (all data is in the CSV) but will break any downstream query running against the table at the same moment. Run full refreshes during maintenance windows.

#### See Also

- [dbt seeds documentation](https://docs.getdbt.com/docs/build/seeds)
- `ingestion_patterns.md` — dbt Seeds as a Reference Ingestion Pattern

---

### dbt — AutomateDV Stage Macro

The AutomateDV `stage` macro (from the `automate_dv` dbt package) generates a staging model for Data Vault 2.0 pipelines. It derives hash key columns (for hubs and links) and hashdiff columns (for satellites) from source columns, applying configurable null substitution.

#### Problem

A raw source table `main.bronze.raw_orders` must be prepared for vault loading. The vault requires a hash key for the order hub (derived from `order_id`), a hash key for the customer hub (derived from `customer_id`), a link hash key (derived from both), and a hashdiff for the order satellite (derived from all descriptive columns).

#### Solution

Install the `automate_dv` dbt package, configure the `stage` macro in a staging dbt model, and run `dbt run --select stg_orders`.

##### dbt Packages (`packages.yml`)

```yaml
packages:
  - package: Datavault-UK/automate_dv
    version: [">=0.10.0", "<0.11.0"]
```

##### dbt Project Variables (`dbt_project.yml`)

```yaml
vars:
  automate_dv:
    hash: MD5          # MD5 or SHA (SHA-256) — must be consistent across all staging models
    concat_string: "||"
    null_placeholder: "^^"
```

##### Staging Model (`models/staging/stg_orders.sql`)

```sql
{{
    config(
        materialized = 'view'
    )
}}

{%- set yaml_metadata -%}
source_model: "raw_orders"
derived_columns:
  RECORD_SOURCE: "!ORDERS_SYSTEM"
  LOAD_DATE: "current_date()"
hashed_columns:
  ORDER_HK:
    is_hashdiff: false
    columns:
      - ORDER_ID
  CUSTOMER_HK:
    is_hashdiff: false
    columns:
      - CUSTOMER_ID
  ORDER_CUSTOMER_HK:
    is_hashdiff: false
    columns:
      - ORDER_ID
      - CUSTOMER_ID
  ORDER_HASHDIFF:
    is_hashdiff: true
    columns:
      - ORDER_TOTAL
      - ORDER_STATUS
      - SHIP_COUNTRY
      - SHIP_DATE
null_columns:
  ORDER_ID: "^^"
  CUSTOMER_ID: "^^"
include_source_columns: true
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(var_dict=metadata_dict) }}
```

##### Compiled SQL (approximate output)

```sql
-- AutomateDV compiles the stage macro to approximate this SQL:
SELECT
    MD5(COALESCE(NULLIF(UPPER(TRIM(CAST(ORDER_ID AS VARCHAR))), ''), '^^'))
        AS ORDER_HK,
    MD5(COALESCE(NULLIF(UPPER(TRIM(CAST(CUSTOMER_ID AS VARCHAR))), ''), '^^'))
        AS CUSTOMER_HK,
    MD5(CONCAT_WS('||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(ORDER_ID AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(CUSTOMER_ID AS VARCHAR))), ''), '^^')
    ))  AS ORDER_CUSTOMER_HK,
    MD5(CONCAT_WS('||',
        COALESCE(CAST(ORDER_TOTAL AS VARCHAR), '^^'),
        COALESCE(CAST(ORDER_STATUS AS VARCHAR), '^^'),
        COALESCE(CAST(SHIP_COUNTRY AS VARCHAR), '^^'),
        COALESCE(CAST(SHIP_DATE AS VARCHAR), '^^')
    ))  AS ORDER_HASHDIFF,
    'ORDERS_SYSTEM' AS RECORD_SOURCE,
    current_date() AS LOAD_DATE,
    -- All source columns (include_source_columns: true)
    ORDER_ID, CUSTOMER_ID, ORDER_TOTAL, ORDER_STATUS, SHIP_COUNTRY, SHIP_DATE
FROM {{ ref('raw_orders') }}
```

#### Discussion and Concerns

- **Hash algorithm consistency:** The `hash` variable in `dbt_project.yml` applies to every staging model in the project. Switching from MD5 to SHA-256 after vault tables are populated requires reprocessing the entire vault.
- **Column ordering in composite keys:** `ORDER_ID || CUSTOMER_ID` and `CUSTOMER_ID || ORDER_ID` produce different hash values. Column order in `hashed_columns` definitions must be fixed and documented before the first production load.
- **Hashdiff column scope:** Every descriptive column loaded into a satellite must be included in its hashdiff. Adding a column to the hashdiff after the satellite has been populated causes all existing records to re-evaluate as changed on the next load.
- **`include_source_columns = true`:** Passes all original source columns through to the staging view. Downstream vault loading macros select the columns they need. Setting this to `false` with explicit `ranked_columns` gives tighter control over what is exposed.

#### See Also

- [AutomateDV stage macro documentation](https://automate-dv.readthedocs.io/en/latest/tutorial/tut_staging/)
- [AutomateDV hash configuration](https://automate-dv.readthedocs.io/en/latest/best_practices/hashing/)
- [AutomateDV dbt package — GitHub](https://github.com/Datavault-UK/automate-dv)
- `ingestion_patterns.md` — Raw Staging for Data Vault 2.0

---

## Managing Your Environment

### Monitoring Your Environment in Production

Use the following signals to observe ingestion pipelines in a running production environment.

| Signal | How to Check | What to Look For |
|--------|-------------|-----------------|
| Auto Loader / Structured Streaming checkpoint health | `spark.streams.active` in a notebook; Databricks Jobs run history | Streams that have stopped without an error logged; checkpoint files that are not advancing |
| COPY INTO load history | `DESCRIBE HISTORY main.bronze.my_table` | Rows where `operation = 'COPY INTO'`; verify `operationMetrics.numAddedFiles` is non-zero on expected run days |
| DLT pipeline health | Databricks UI → Delta Live Tables → pipeline event log; `system.lakeflow.*` system tables | Expectation failure rates exceeding thresholds; pipeline runs that terminate in `FAILED` state |
| Lakeflow Connect pipeline status | Databricks UI → Ingestion → Lakeflow pipelines; Lakeflow Jobs run history | Pipelines that have not run within the expected schedule window; connector errors in event logs |
| JDBC job run duration | Databricks Jobs run history; job cluster metrics | Run durations trending upward (may indicate source table growth requiring `numPartitions` adjustment or index maintenance on the source) |
| Data freshness | Query `MAX(ingested_at)` or `MAX(_metadata.file_modification_time)` on bronze tables | Tables where `MAX(ingested_at)` is older than the expected pipeline frequency |

### Common Failure Patterns and Remediation

| Failure | Likely Cause | Remediation |
|---------|-------------|-------------|
| Auto Loader stream stops with `FileNotFoundException` on checkpoint | Checkpoint directory was deleted or moved | Restore checkpoint from backup if available; if not, delete the checkpoint and reprocess from the beginning (accept reprocessing cost) |
| COPY INTO loads zero rows after a schema change | New source files have columns not in the target Delta schema | Run `ALTER TABLE ... ADD COLUMNS (...)` to add the new columns, then re-run COPY INTO |
| JDBC job runs significantly slower than previous runs | Source table has grown; `numPartitions` insufficient; source index fragmentation | Increase `numPartitions`; request index maintenance from the source DBA; consider reading from a read replica |
| Lakeflow Connect pipeline fails with authentication error | OAuth token expired or credentials rotated | Update the connection credentials in the Lakeflow Connect connection configuration via the Databricks UI or API |
| Partner connector lands duplicate rows | Connector backfill triggered (e.g., after reconnection) | Deduplicate using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_synced DESC)` in the downstream silver transformation |
| DLT pipeline fails immediately after source schema change | New column not matching an `@dlt.expect` constraint | Review the constraint definition; add the new column to the constraint or update the expectation to handle it |
