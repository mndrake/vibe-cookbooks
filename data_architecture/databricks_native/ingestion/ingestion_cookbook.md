# Ingestion Cookbook
## Databricks Native Stack

> **Scope:** This cookbook covers ingestion using Databricks platform features only — Auto Loader, COPY INTO, SFTP connector, Structured Streaming, Delta Live Tables, JDBC, Lakeflow Connect, Partner Connectors, and native reference data loading with COPY INTO. It does not cover dbt, dbt seeds, or AutomateDV. For the equivalent guide covering the Databricks + dbt + AutomateDV stack, see `../databricks_and_dbt/ingestion/ingestion_cookbook.md`.

---

## Introduction

This cookbook provides practical, step-by-step guidance for data ingestion on Databricks using only the Databricks native toolchain. It covers file ingestion, streaming ingestion, database ingestion, managed ingestion, and native reference data loading. Data Vault 2.0 staging using native PySpark and SQL hash derivation is covered here and in the data vault cookbooks under `../data_vault/`.

The architectural rationale for choosing between methods is covered in `ingestion_patterns.md` in the same directory.

---

## Development Environment Pre-Requisites

> **On Databricks (interactive notebooks or Asset Bundle jobs):** PySpark, Delta Lake (`delta-spark`), Delta Live Tables, and `dbutils` are pre-installed with every Databricks Runtime. No `pip install` commands are needed to run the code examples in this cookbook on a Databricks cluster.
>
> **Local development:** The tools below are required on your local machine for Databricks CLI operations, Asset Bundle deployment, and running unit tests outside Databricks.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.9+ | Local dev | Required for the Databricks CLI and local PySpark unit tests |
| Apache Spark via PySpark | 3.4+ | Local dev only | Bundled with Databricks Runtime — `pip install pyspark` only for local unit testing |
| Databricks CLI | Latest | Local dev | Used for secrets management, workspace interaction, and deploying Databricks Asset Bundles |
| delta-spark | Match DBR version | Local dev only | Bundled with Databricks Runtime — `pip install delta-spark` only for local unit testing; version must match your DBR's bundled Delta Lake version |

Configure your Databricks CLI connection (local machine):

```bash
# Authenticate using OAuth (recommended for interactive use)
databricks auth login --host https://<your-workspace>.azuredatabricks.net

# Verify authentication
databricks clusters list
```

---

## File Ingestion

### File Ingestion — Auto Loader

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
- **Partial files:** SFTP sources do not provide atomic delivery guarantees. Use a `.done` sentinel file convention to avoid reading partial files.

#### See Also

- [SFTP ingestion — Azure Databricks (public preview)](https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp)

---

## Streaming Ingestion

### Streaming Ingestion — Structured Streaming

Structured Streaming ingests from Kafka, Azure Event Hubs, or Amazon Kinesis with sub-minute latency and exactly-once semantics via checkpoint.

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
- **Azure Event Hubs connector:** Install `com.microsoft.azure:azure-eventhubs-spark` as a Maven library on the cluster.

#### See Also

- [Structured Streaming — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/)
- [Table streaming reads and writes — Delta Lake](https://docs.delta.io/latest/delta-streaming.html)

---

### Streaming Ingestion — Delta Live Tables (DLT)

Delta Live Tables is Databricks' declarative pipeline framework. DLT manages compute, retries, checkpointing, and data quality enforcement.

#### Problem

A medallion pipeline (bronze → silver → gold) must be built for order data with built-in data quality checks, automatic schema inference, and managed cluster lifecycle.

#### Solution

Define pipeline tables using `@dlt.table` decorators and `@dlt.expect` annotations. Deploy as a DLT pipeline via the Databricks UI, CLI, or Databricks Asset Bundles.

##### Python

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

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
-- Bronze
CREATE OR REFRESH STREAMING TABLE orders_bronze
COMMENT 'Raw orders from ADLS'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT * FROM cloud_files(
  'abfss://raw@mystorageaccount.dfs.core.windows.net/orders/',
  'json',
  map('cloudFiles.schemaLocation', '/pipelines/orders/schema')
);

-- Silver with quality constraints
CREATE OR REFRESH STREAMING TABLE orders_silver (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_total  EXPECT (order_total > 0)     ON VIOLATION DROP ROW
)
COMMENT 'Cleansed orders'
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
| Quality annotations | `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail` decorators | `CONSTRAINT ... EXPECT ... ON VIOLATION` clause |
| Complex transformations | Full PySpark DataFrame API | Limited to Spark SQL expressions |
| Reusable functions | Python functions importable across pipeline files | No cross-definition function reuse |

#### Discussion and Concerns

- **DLT incurs a DBU premium:** Evaluate whether a standard Structured Streaming job achieves the same outcome at lower cost for cost-sensitive workloads.
- **Pipeline mode:** Triggered mode (default) runs once and terminates. Continuous mode runs indefinitely. Triggered mode is appropriate and cheaper for batch-oriented bronze ingestion.

#### See Also

- [Delta Live Tables — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/)
- [DLT expectations — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations)

---

## Database Ingestion

### Database Ingestion — JDBC

Spark's JDBC data source reads directly from relational databases over a JDBC connection. It is the standard pattern when data cannot be exported to cloud storage first and no CDC feed is available.

#### Problem

An Azure SQL Database must be ingested into Delta Lake on a scheduled basis. The table contains tens of millions of rows and must be parallelised to meet the ingestion window.

#### Solution

Use `spark.read.format("jdbc")` with partition configuration. Write to Delta using MERGE for incremental loads or overwrite for full loads.

##### Python

```python
jdbc_url = "jdbc:sqlserver://myserver.database.windows.net:1433;database=SourceDB"
jdbc_user = dbutils.secrets.get(scope="jdbc-secrets", key="sql-user")
jdbc_password = dbutils.secrets.get(scope="jdbc-secrets", key="sql-password")

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query", "SELECT * FROM dbo.orders WHERE updated_at >= '2026-03-12 00:00:00'")
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "8")
    .load()
)

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
- **Hard deletes are invisible:** Incremental JDBC based on `updated_at` will not detect deleted rows. Use a CDC tool (Debezium) if delete propagation is required.
- **SQL Server driver:** Included in Databricks Runtime. For PostgreSQL/MySQL, install the driver JAR via the cluster Libraries tab.

#### See Also

- [JDBC ingestion — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/external-systems/jdbc)
- [DeltaTable Python API — Delta Lake](https://docs.delta.io/latest/api/python/api/delta.tables.DeltaTable.html)

---

## Managed Ingestion

### Managed Ingestion — Lakeflow Connect

Lakeflow Connect provides Databricks-native managed connectors for SaaS applications and databases. As of March 2026, it is GA for Salesforce, Workday, and SQL Server. Pipelines run on serverless compute within Databricks, governed by Unity Catalog.

#### Problem

The data platform must ingest data from Salesforce without building or maintaining a custom API connector, and without source data transiting third-party infrastructure.

#### Solution

Create a Lakeflow Connect pipeline for Salesforce. The connector handles incremental extraction, schema evolution, and scheduling natively.

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

- **Data transits third-party infrastructure:** Assess against GDPR, HIPAA, and data residency requirements. Obtain a Data Processing Agreement (DPA) for regulated data.
- **Isolate connector schemas:** Grant the connector principal access only to its dedicated schema.
- **`_fivetran_deleted = true` records are retained:** Downstream silver transformations must filter `WHERE _fivetran_deleted = false`.
- **Cost model:** Partner connector pricing (per MAR or per volume) is separate from Databricks DBU costs. Compare against Lakeflow Connect for sources where both options are available.

#### See Also

- [Databricks Partner Connect — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partner-connect/)
- [Fivetran Databricks destination](https://fivetran.com/docs/destinations/databricks)

---

## Reference Data Loading (Native)

Reference data (country codes, currency codes, product status mappings) should be managed via COPY INTO from cloud storage on the native Databricks stack. This provides the same idempotency guarantee as dbt seeds, with better scalability and no git dependency for updates.

### Reference Data — COPY INTO from Cloud Storage

#### Problem

A country code reference table (250 rows) must be maintained in the silver layer. The data changes at most once a year. Business analysts need to be able to update it without needing git access.

#### Solution

Store the reference CSV in a controlled ADLS path (version-controlled in your deployment pipeline). Use COPY INTO for idempotent loading. A scheduled Databricks Job monitors the path and reloads when a new file is detected.

##### Python

```python
# Initial load or reload after file update
spark.sql("""
    COPY INTO main.silver.ref_country_codes
    FROM 'abfss://reference@mystorageaccount.dfs.core.windows.net/country_codes/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('force' = 'false')
""")

# Use FORCE = TRUE to reload all files even if previously loaded (after a file update)
spark.sql("""
    COPY INTO main.silver.ref_country_codes
    FROM 'abfss://reference@mystorageaccount.dfs.core.windows.net/country_codes/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('force' = 'true')
""")

# Validate
display(spark.sql("SELECT COUNT(*) AS total_rows FROM main.silver.ref_country_codes"))
```

##### SQL

```sql
-- Create the reference table with explicit column types (no schema inference for reference data)
CREATE TABLE IF NOT EXISTS main.silver.ref_country_codes (
    country_code VARCHAR(2)   NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    region       VARCHAR(10)  NOT NULL,
    is_active    BOOLEAN      NOT NULL
)
USING DELTA
COMMENT 'ISO 3166 country code reference. Source: abfss://reference/country_codes/';

-- Load idempotently
COPY INTO main.silver.ref_country_codes
FROM 'abfss://reference@mystorageaccount.dfs.core.windows.net/country_codes/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('force' = 'false');

-- Validate: check for unexpected region values
SELECT DISTINCT region FROM main.silver.ref_country_codes;

-- Validate: check for nulls in key columns
SELECT COUNT(*) AS null_codes
FROM main.silver.ref_country_codes
WHERE country_code IS NULL OR country_name IS NULL;

-- View change history via Delta table versioning (equivalent to dbt seed git history)
DESCRIBE HISTORY main.silver.ref_country_codes;
```

#### Discussion and Concerns

- **Change tracking:** Unlike dbt seeds, which track changes via git commit history, COPY INTO change history is visible in Delta's transaction log via `DESCRIBE HISTORY`. For a full audit trail including who uploaded the source file and when, use ADLS storage access logs and the Delta history together.
- **`FORCE = TRUE`:** When the reference CSV is updated and re-uploaded to the same path, COPY INTO with `force = false` will not reload it (the file is already tracked). Use `force = true` when you intentionally want to reload an updated file.
- **Schema definition:** Always define the target table DDL explicitly before the first COPY INTO load for reference data. Do not rely on `inferSchema` for tables where column types are fixed and known.
- **Validation:** Run the SQL validation queries above as part of the Databricks Job that performs the COPY INTO. Add a notebook task after the COPY INTO task that runs assertions and fails the job if validation checks fail.

#### See Also

- [COPY INTO — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)
- [DESCRIBE HISTORY — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-describe-history)

---

## Data Vault Staging — Native PySpark and SQL

On the native Databricks stack, Data Vault 2.0 staging (hash key and hashdiff derivation) is implemented directly using PySpark functions or Spark SQL, deployed as a Delta Live Tables pipeline or a standalone PySpark job. No external macro library (AutomateDV) is required.

### Problem

A raw source table `main.bronze.raw_orders` must be prepared for vault loading. The vault requires: a hash key for the order hub (from `order_id`), a hash key for the customer hub (from `customer_id`), a link hash key (from both), and a hashdiff for the order satellite (from all descriptive columns).

### Solution

#### Python (DLT View)

```python
import dlt
from pyspark.sql.functions import md5, concat_ws, coalesce, lit, col, current_date

# Implement as a DLT view — no materialisation needed for a staging layer
@dlt.view(
    name="stg_orders",
    comment="Data Vault 2.0 staging: hash keys and hashdiff for orders"
)
def stg_orders():
    null_sub = lit("^^")  # Null substitution sentinel

    return (
        dlt.read_stream("raw_orders")
        # Single-column hash keys
        .withColumn("order_hk",
            md5(coalesce(col("order_id").cast("string"), null_sub))
        )
        .withColumn("customer_hk",
            md5(coalesce(col("customer_id").cast("string"), null_sub))
        )
        # Composite hash key for link
        .withColumn("order_customer_hk",
            md5(concat_ws("||",
                coalesce(col("order_id").cast("string"),    null_sub),
                coalesce(col("customer_id").cast("string"), null_sub)
            ))
        )
        # Hashdiff: hash of all satellite descriptive columns
        .withColumn("order_hashdiff",
            md5(concat_ws("||",
                coalesce(col("order_total").cast("string"),  null_sub),
                coalesce(col("order_status").cast("string"), null_sub),
                coalesce(col("ship_country").cast("string"), null_sub),
                coalesce(col("ship_date").cast("string"),    null_sub)
            ))
        )
        .withColumn("record_source", lit("ORDERS_SYSTEM"))
        .withColumn("load_date", current_date())
    )
```

#### SQL (DLT View)

```sql
-- DLT view — non-materialised staging layer
CREATE OR REFRESH STREAMING VIEW stg_orders
COMMENT 'Data Vault 2.0 staging: hash keys and hashdiff for orders'
AS
SELECT
    -- Single-column hash keys
    MD5(COALESCE(CAST(order_id AS STRING),    '^^')) AS order_hk,
    MD5(COALESCE(CAST(customer_id AS STRING), '^^')) AS customer_hk,

    -- Composite hash key for the order-customer link
    MD5(CONCAT_WS('||',
        COALESCE(CAST(order_id AS STRING),    '^^'),
        COALESCE(CAST(customer_id AS STRING), '^^')
    )) AS order_customer_hk,

    -- Hashdiff: all descriptive columns for the order satellite
    -- Column order is fixed — must not change after first load
    MD5(CONCAT_WS('||',
        COALESCE(CAST(order_total AS STRING),  '^^'),
        COALESCE(CAST(order_status AS STRING), '^^'),
        COALESCE(CAST(ship_country AS STRING), '^^'),
        COALESCE(CAST(ship_date AS STRING),    '^^')
    )) AS order_hashdiff,

    'ORDERS_SYSTEM'  AS record_source,
    current_date()   AS load_date,

    -- Pass all source columns through for downstream vault loaders
    order_id,
    customer_id,
    order_total,
    order_status,
    ship_country,
    ship_date

FROM STREAM(LIVE.raw_orders);
```

#### Standalone PySpark (non-DLT)

```python
from pyspark.sql.functions import md5, concat_ws, coalesce, lit, col, current_date

null_sub = lit("^^")

df_staged = (
    spark.table("main.bronze.raw_orders")
    .withColumn("order_hk",
        md5(coalesce(col("order_id").cast("string"), null_sub)))
    .withColumn("customer_hk",
        md5(coalesce(col("customer_id").cast("string"), null_sub)))
    .withColumn("order_customer_hk",
        md5(concat_ws("||",
            coalesce(col("order_id").cast("string"),    null_sub),
            coalesce(col("customer_id").cast("string"), null_sub))))
    .withColumn("order_hashdiff",
        md5(concat_ws("||",
            coalesce(col("order_total").cast("string"),  null_sub),
            coalesce(col("order_status").cast("string"), null_sub),
            coalesce(col("ship_country").cast("string"), null_sub),
            coalesce(col("ship_date").cast("string"),    null_sub))))
    .withColumn("record_source", lit("ORDERS_SYSTEM"))
    .withColumn("load_date", current_date())
)

# Write staging to a temporary view for downstream vault loaders
df_staged.createOrReplaceTempView("stg_orders")
```

### Validation

```sql
-- Validate: no null hash keys (null substitution worked)
SELECT COUNT(*) AS null_hk_count
FROM stg_orders
WHERE order_hk IS NULL OR customer_hk IS NULL OR order_customer_hk IS NULL;

-- Validate: hashdiff is non-null for all rows
SELECT COUNT(*) AS null_hashdiff_count
FROM stg_orders
WHERE order_hashdiff IS NULL;

-- Spot-check: verify hash values are deterministic for known inputs
SELECT
    order_id,
    order_hk,
    MD5(order_id) AS expected_hk,
    (order_hk = MD5(order_id)) AS matches
FROM stg_orders
WHERE order_id IS NOT NULL
LIMIT 5;
```

### Discussion and Concerns

- **`CONCAT_WS` skips nulls — `COALESCE` is mandatory:** `CONCAT_WS('||', 'A', NULL, 'B')` produces `'A||B'`, the same as `CONCAT_WS('||', 'A', 'B')`. Without `COALESCE`, two rows with different numbers of null values in their business keys can produce the same hash. Always substitute with `'^^'` or another sentinel before concatenation.
- **Hash algorithm choice:** Use `MD5()` for most cases (32-character hex, computationally inexpensive). Use `SHA2(expr, 256)` for 64-character hex if your organisation has a policy against MD5. The choice must be made at platform design time — changing after vault tables are populated requires a full reprocessing of the vault.
- **Column ordering is permanent:** The column order in `CONCAT_WS` expressions in the hash key and hashdiff definitions must never change after the first production load. Document the column order per entity in the data dictionary.
- **Hashdiff scope must match satellite scope:** Every column that will be loaded into a satellite must be included in its hashdiff. Adding a column later changes every existing hashdiff value, causing all existing satellite records to re-evaluate as changed on the next load.
- **DLT view vs. materialised table:** Implement staging as a DLT view (not a streaming table) — staging is a transformation step, not a storage layer. Views avoid unnecessary data duplication and maintain a clean lineage graph.
- **SHA2 syntax:** In Spark SQL, `SHA2(expr, 256)` returns a hex string. In PySpark, use `sha2(col, 256)` from `pyspark.sql.functions`. Both produce the same output as `SHA2(CAST(expr AS STRING), 256)` for string inputs.

### See Also

- [MD5 function — Databricks SQL](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/md5)
- [SHA2 function — Databricks SQL](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/sha2)
- [CONCAT_WS function — Databricks SQL](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/concat_ws)
- [Delta Live Tables Python API — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/python-ref)
- `../data_vault/dv2_staging_cookbook.md` — Hub, link, and satellite loading using the staged data
- `ingestion_patterns.md` — Raw Staging for Data Vault 2.0 (design considerations)
- [Delta Lake schema evolution — Delta Lake](https://docs.delta.io/latest/delta-schema-evolution.html)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | How to Check | What to Look For |
|--------|-------------|-----------------|
| Auto Loader / Structured Streaming | `spark.streams.active`; Databricks Jobs run history | Streams stopped without error; checkpoint files not advancing |
| COPY INTO load history | `DESCRIBE HISTORY main.bronze.my_table` | Operations where `operation = 'COPY INTO'`; `numAddedFiles` is non-zero on expected run days |
| DLT pipeline health | Databricks UI → Delta Live Tables; `system.lakeflow.*` system tables | Expectation failure rates; pipelines terminating in `FAILED` state |
| Lakeflow Connect | Databricks UI → Ingestion → Lakeflow pipelines | Pipelines not run within expected window; connector errors in event log |
| JDBC job duration | Databricks Jobs run history | Durations trending upward — may indicate source table growth requiring `numPartitions` adjustment |
| Reference data freshness | `DESCRIBE HISTORY main.silver.ref_country_codes` | Last `COPY INTO` operation timestamp vs. expected update cadence |

### Common Failure Patterns and Remediation

| Failure | Likely Cause | Remediation |
|---------|-------------|-------------|
| Auto Loader stream stops with `FileNotFoundException` on checkpoint | Checkpoint directory deleted or moved | Restore from backup; if unavailable, delete checkpoint and reprocess from beginning |
| COPY INTO loads zero rows after schema change | New source files have columns not in target Delta schema | `ALTER TABLE ... ADD COLUMNS (...)` then re-run COPY INTO |
| JDBC job significantly slower | Source table growth; insufficient `numPartitions`; index fragmentation | Increase `numPartitions`; request source DBA to rebuild indexes; use read replica |
| Lakeflow Connect authentication error | OAuth token expired or credentials rotated | Update connection credentials in Lakeflow Connect configuration |
| Partner connector lands duplicate rows | Connector backfill triggered (e.g., after reconnection) | Deduplicate in silver using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_synced DESC)` |
| DLT pipeline fails after source schema change | New column not matching a quality constraint | Review constraint; update expectation to handle the new column |
| Staging hash key mismatch (Data Vault) | Column ordering changed in `CONCAT_WS` expression | Restore original column order; audit which vault records have corrupted hash values; if significant, reprocess from bronze |
