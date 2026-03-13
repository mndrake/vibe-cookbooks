# Ingestion Cookbook

---

## Introduction

This cookbook provides practical, step-by-step guidance for data ingestion on Databricks. It covers the three primary ingestion categories used in a modern Databricks data platform — file ingestion, streaming ingestion, and ad hoc ingestion — as well as dbt-specific ingestion patterns for reference data and Data Vault 2.0 staging.

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

# Or authenticate using a personal access token
databricks configure --token
# When prompted: enter your workspace URL and PAT

# Verify the connection
databricks clusters list
```

For the dbt-based sections, configure `~/.dbt/profiles.yml`:

```yaml
vibe_cookbooks:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <your-workspace>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<your-warehouse-id>
      token: "{{ env_var('DBT_TOKEN') }}"
      catalog: main
      schema: bronze_dev
    prod:
      type: databricks
      host: <your-workspace>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<your-warehouse-id>
      token: "{{ env_var('DBT_TOKEN') }}"
      catalog: main
      schema: bronze
```

Set the `DBT_TOKEN` environment variable to your Databricks personal access token before running any dbt commands. Do not hardcode tokens in `profiles.yml`.

### Getting a New Starter Project

To start a new dbt project for ingestion work from scratch:

```bash
# Install dbt-databricks
pip install dbt-databricks automate-dv

# Scaffold a new dbt project
dbt init vibe_ingestion
cd vibe_ingestion

# Verify connection using the profile configured above
dbt debug

# Add AutomateDV as a dbt package dependency
# Add the following to packages.yml in the project root:
# packages:
#   - package: Datavault-UK/automate_dv
#     version: [">=0.10.0", "<0.11.0"]

dbt deps
```

For Databricks notebooks, create a new notebook in the Databricks workspace attached to a cluster running Databricks Runtime 13.3 LTS or later with Unity Catalog enabled.

### Getting the Source for an Existing Project

To work with an existing ingestion project:

```bash
git clone https://github.com/<your-org>/vibe-ingestion.git
cd vibe-ingestion

# Install Python dependencies
pip install -r requirements.txt

# Install dbt package dependencies (AutomateDV and others declared in packages.yml)
dbt deps

# Verify connection and configuration
dbt debug
```

For Databricks notebook-based pipelines, import the repository into the Databricks workspace using the Repos feature (`Workspace > Repos > Add Repo`) and pull the latest branch.

---

## Infrastructure Pre-Requisites

### Infrastructure Required

The following Databricks and cloud infrastructure is required to run the examples in this cookbook. Not all components are required for every method — see the notes column for method-specific requirements.

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment for all examples | Unity Catalog must be enabled; required for `main` catalog references throughout this cookbook |
| All-Purpose Cluster or Job Cluster (Databricks Runtime 13.3 LTS+) | Compute for notebook and streaming jobs | All-Purpose for development; Job Clusters for production scheduled runs |
| Cloud Storage (ADLS Gen2, S3, or GCS) | Source location for file ingestion examples | Mount or Unity Catalog External Location configured; examples use ADLS paths |
| Unity Catalog — `main` catalog with `bronze` and `silver` schemas | Target for output tables | Requires `CREATE TABLE` and `USE SCHEMA` privileges on both schemas |
| Azure Event Hubs or Apache Kafka | Source for streaming ingestion examples | Event Hubs connection string or Kafka bootstrap servers required for Structured Streaming section |
| Databricks SQL Warehouse | Required for COPY INTO and dbt seed examples | Serverless SQL Warehouse is sufficient |
| dbt-databricks and AutomateDV installed | Required for dbt Seeds and AutomateDV sections | See Development Environment Pre-Requisites above |

### Enabling Auto Loader Checkpointing

Auto Loader and Structured Streaming jobs require a durable checkpoint directory on cloud storage. The checkpoint directory persists the processing state between job runs. If the checkpoint directory is deleted, the job will reprocess all data from the beginning on the next run.

Configure a checkpoint location on ADLS Gen2 as follows. The path must be in a storage container that the cluster's service principal or managed identity has write access to.

```python
# Recommended checkpoint path convention:
# abfss://<container>@<storage-account>.dfs.core.windows.net/checkpoints/<pipeline-name>/

checkpoint_base = "abfss://checkpoints@mystorageaccount.dfs.core.windows.net/checkpoints"

# Example for an Auto Loader job loading customer events:
checkpoint_path = f"{checkpoint_base}/bronze_customer_events"
```

On AWS, use an S3 path: `s3://my-bucket/checkpoints/bronze_customer_events/`

On GCP, use a GCS path: `gs://my-bucket/checkpoints/bronze_customer_events/`

The checkpoint directory is created automatically by Spark on first run if the cluster has write access to the path. Verify that it has been created after the first successful run before declaring the pipeline healthy.

---

## File Ingestion — Auto Loader

Auto Loader is Databricks's incremental file ingestion framework built on Structured Streaming. It monitors a cloud storage path for new files, processes only files that have not yet been loaded, and writes results to a Delta table. It maintains processing state in a checkpoint directory, so it can be stopped and restarted safely without re-reading files that have already been processed.

### Problem

A source system deposits CSV, JSON, or Parquet files in an ADLS container on a recurring schedule — hourly, daily, or continuously throughout the day. The ingestion pipeline must load only new files on each run, must not reprocess files that were successfully loaded in a previous run, and must handle schema changes in the source files without requiring manual intervention or pipeline downtime.

### Solution

Use `spark.readStream` with `format("cloudFiles")` to read from the cloud storage path. Configure `cloudFiles.format` for the file type, `cloudFiles.schemaLocation` to persist the inferred schema between runs, and `checkpointLocation` on the write side to track which files have been processed. Write to a Delta table in Unity Catalog.

Use `trigger(availableNow=True)` to process all files currently in the landing zone and then stop — this is the recommended mode for scheduled batch-style pipelines as it avoids paying for a continuously running cluster.

#### Python Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Source and destination configuration
landing_path = "abfss://landing@mystorageaccount.dfs.core.windows.net/customer_events/"
schema_location = "abfss://checkpoints@mystorageaccount.dfs.core.windows.net/schemas/bronze_customer_events/"
checkpoint_location = "abfss://checkpoints@mystorageaccount.dfs.core.windows.net/checkpoints/bronze_customer_events/"
target_table = "main.bronze.customer_events"

# Read incrementally from cloud storage using Auto Loader
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # Rescue unexpected columns into _rescued_data rather than failing
    .option("cloudFiles.inferColumnTypes", "true")
    .load(landing_path)
)

# Add ingestion metadata columns for lineage and debugging
from pyspark.sql.functions import current_timestamp, input_file_name

df = df.withColumn("_ingested_at", current_timestamp()) \
       .withColumn("_source_file", input_file_name())

# Write to Delta table with trigger(availableNow=True) for scheduled batch mode
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

# Wait for the trigger-once run to complete before the job exits
query.awaitTermination()
```

#### SQL Example

The SQL equivalent for Auto Loader is a Delta Live Tables pipeline definition. DLT provides a cloud_files() table-valued function that wraps Auto Loader and manages the checkpoint and schema location automatically.

```sql
-- Delta Live Tables pipeline definition (Python DLT notebook or SQL DLT notebook)
-- This SQL runs inside a DLT pipeline, not a standard SQL warehouse

CREATE OR REFRESH STREAMING TABLE main.bronze.customer_events
COMMENT "Bronze layer: raw customer events ingested from landing zone via Auto Loader"
AS
SELECT
    *,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _source_file
FROM cloud_files(
    'abfss://landing@mystorageaccount.dfs.core.windows.net/customer_events/',
    'json',
    map(
        'cloudFiles.inferColumnTypes', 'true',
        'cloudFiles.schemaEvolutionMode', 'addNewColumns'
    )
);
```

Outside of DLT, there is no native SQL syntax to invoke Auto Loader directly. Use the Python API shown above, or the DLT SQL syntax if your pipeline is managed by Delta Live Tables.

### Discussion and Concerns

- **Schema evolution mode selection:** `addNewColumns` is appropriate for bronze-layer ingestion where completeness is the priority. Use `failOnNewColumns` for silver or gold layer pipelines where unexpected columns indicate a source system issue. Use `rescue` mode if the source schema is highly variable and you want to capture unexpected columns in `_rescued_data` without failing.
- **Checkpoint directory management:** The checkpoint directory must never be deleted in production unless you intend to reprocess all files from the beginning. Accidental deletion is the most common cause of duplicate data in Auto Loader pipelines. Store checkpoints in a dedicated storage container with soft-delete enabled.
- **`trigger(availableNow=True)` vs. continuous mode:** For most batch-style pipelines, `availableNow=True` is the right choice — the job runs, processes all new files, and stops. Continuous mode keeps the cluster alive between file arrivals, which is appropriate only for sub-minute latency requirements and will incur cost even when no new files are present.
- **File notification vs. directory listing mode:** Auto Loader can operate in directory listing mode (default, polls the storage path) or file notification mode (receives event notifications via Azure Event Grid or SNS). File notification mode is more efficient for high file volumes and reduces API call costs. Configure with `cloudFiles.useNotifications = true` and follow the notification setup guide in the Databricks documentation.
- **`_ingested_at` and `_source_file` columns:** Adding these metadata columns at ingestion time is strongly recommended. They enable root-cause analysis when data quality issues are discovered downstream, without needing to re-read the source files.

### See Also

- [Auto Loader documentation — Databricks](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [Auto Loader schema evolution — Databricks](https://docs.databricks.com/en/ingestion/auto-loader/schema.html)
- [Auto Loader in production — Databricks](https://docs.databricks.com/en/ingestion/auto-loader/production.html)
- `ingestion_patterns.md` — Ingestion Method Selection and Schema Evolution Strategy

---

## File Ingestion — COPY INTO

COPY INTO is a Databricks SQL command that loads files from cloud storage into a Delta table idempotently. It tracks which files have been loaded by recording file paths inside the Delta table's transaction log, so re-running the same COPY INTO command will not load files that have already been successfully ingested.

### Problem

A scheduled batch job needs to load Parquet or CSV files from a cloud storage path into a Delta table once per hour. The job must be safe to re-run — if it fails partway through and is retried, it must not insert duplicate records. The pipeline does not need to handle schema evolution, and the operational team prefers SQL-based pipelines over PySpark streaming jobs for simplicity.

### Solution

Use the `COPY INTO` SQL command, targeting the Delta table and specifying the source cloud storage path and file format. COPY INTO records loaded file paths in the Delta transaction log and skips them on subsequent executions. Run it using `spark.sql()` from a Python job, or directly in a Databricks SQL notebook or SQL Warehouse query.

#### Python Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COPY INTO executed via spark.sql() for use in a Python job or notebook
result = spark.sql("""
    COPY INTO main.bronze.order_items
    FROM 'abfss://landing@mystorageaccount.dfs.core.windows.net/order_items/'
    FILEFORMAT = PARQUET
    FORMAT_OPTIONS (
        'mergeSchema' = 'false'
    )
    COPY_OPTIONS (
        'mergeSchema' = 'false',
        'force' = 'false'
    )
""")

# Display the result — COPY INTO returns a summary row showing files copied and rows inserted
result.show()
```

To validate after loading:

```python
# Confirm the load via DESCRIBE HISTORY
spark.sql("DESCRIBE HISTORY main.bronze.order_items").show(5, truncate=False)
```

#### SQL Example

```sql
-- Create the target Delta table if it does not yet exist
-- COPY INTO will not create the table automatically
CREATE TABLE IF NOT EXISTS main.bronze.order_items (
    order_id        BIGINT,
    product_id      BIGINT,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    order_date      DATE,
    customer_id     BIGINT
)
USING DELTA
LOCATION 'abfss://delta@mystorageaccount.dfs.core.windows.net/bronze/order_items/';

-- Load new files from the landing zone
-- Files already loaded in a previous run are automatically skipped
COPY INTO main.bronze.order_items
FROM 'abfss://landing@mystorageaccount.dfs.core.windows.net/order_items/'
FILEFORMAT = PARQUET
FORMAT_OPTIONS (
    'mergeSchema' = 'false'
)
COPY_OPTIONS (
    'mergeSchema' = 'false',
    'force' = 'false'
);

-- Review load history
DESCRIBE HISTORY main.bronze.order_items;
```

To force a full reload (reprocess all files, including those already loaded):

```sql
-- WARNING: Setting force = 'true' reloads all files and will create duplicates
-- unless the target table is truncated first. Use only for full reloads.
COPY INTO main.bronze.order_items
FROM 'abfss://landing@mystorageaccount.dfs.core.windows.net/order_items/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('force' = 'true');
```

### Discussion and Concerns

- **No schema evolution:** COPY INTO uses the schema of the target Delta table. New columns in source files are silently dropped; missing columns are written as `null`. If the source schema changes, the target table must be manually altered with `ALTER TABLE ... ADD COLUMN` before the next load, or data will be lost.
- **Idempotency is table-scoped:** COPY INTO tracks loaded files inside the Delta transaction log of the specific target table. If the target table is dropped and recreated, the tracking history is lost and all files in the source path will be reloaded on the next run.
- **No external checkpoint directory:** Unlike Auto Loader, COPY INTO does not require a separate checkpoint directory. The tracking state lives inside the Delta table itself. This simplifies operations but also means the state cannot be transferred to a different table.
- **`force = 'true'` creates duplicates:** Setting `force = 'true'` bypasses file tracking and reloads all files. This will create duplicates unless the target table is truncated first. Use with caution and only during deliberate full-reload operations.
- **Performance:** COPY INTO is optimised for batch loads of many small-to-medium files. For very large individual files (multi-GB Parquet), Auto Loader with Structured Streaming will typically provide better parallelism.
- **COPY INTO vs. Auto Loader comparison:** COPY INTO is simpler and SQL-native; Auto Loader is more powerful but requires a streaming runtime. For simple scheduled batch loads without schema evolution needs, COPY INTO is the lower-complexity choice.

### See Also

- [COPY INTO documentation — Databricks](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html)
- [COPY INTO vs. Auto Loader — Databricks](https://docs.databricks.com/en/ingestion/auto-loader/index.html#when-to-use-auto-loader-vs-copy-into)
- `ingestion_patterns.md` — Ingestion Method Selection

---

## Streaming Ingestion — Structured Streaming

Structured Streaming is Apache Spark's stream processing engine, available natively on Databricks. It reads from streaming sources — Apache Kafka, Azure Event Hubs (which exposes a Kafka-compatible endpoint), Kinesis, or Delta tables with Change Data Feed — and writes to Delta tables or other sinks with exactly-once guarantees via checkpoint-based offset tracking.

### Problem

An application publishes customer interaction events to an Azure Event Hubs namespace at a rate of several thousand events per minute. The data platform must ingest these events into a Delta table in the bronze layer within two minutes of them being published. The pipeline must handle transient failures gracefully and resume from where it left off without data loss or duplication.

### Solution

Use `spark.readStream` with the `kafka` format (Event Hubs exposes a Kafka-compatible endpoint). Read raw events from the topic, parse the JSON `value` column, and write to a Delta table with a checkpoint location for exactly-once delivery.

#### Python Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

spark = SparkSession.builder.getOrCreate()

# Event schema expected from the source application
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_timestamp", LongType(), True)  # Unix epoch milliseconds
])

# Azure Event Hubs connection string stored in Databricks Secrets
# Never hardcode connection strings — always use dbutils.secrets.get()
eventhubs_connection_string = dbutils.secrets.get(
    scope="ingestion-secrets",
    key="eventhubs-customer-events-connection-string"
)

# Event Hubs Kafka endpoint configuration
# Event Hubs namespace exposes a Kafka endpoint on port 9093
kafka_bootstrap_servers = "<your-namespace>.servicebus.windows.net:9093"
topic_name = "customer-events"

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topic_name)
    .option("startingOffsets", "latest")  # Use "earliest" for backfill
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="$ConnectionString" password="{eventhubs_connection_string}";')
    .option("failOnDataLoss", "false")  # Continue if offsets are out of retention range
    .load()
)

# Parse the JSON value column
df_parsed = (
    df_raw
    .select(
        from_json(col("value").cast("string"), event_schema).alias("event"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp")
    )
    .select(
        "event.*",
        "partition",
        "offset",
        "kafka_timestamp",
        current_timestamp().alias("_ingested_at")
    )
)

# Write to Delta with a 1-minute micro-batch trigger
checkpoint_location = "abfss://checkpoints@mystorageaccount.dfs.core.windows.net/checkpoints/bronze_customer_events_stream/"

query = (
    df_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime="1 minute")
    .toTable("main.bronze.customer_events_stream")
)

query.awaitTermination()
```

#### SQL Example

The SQL equivalent for a managed streaming pipeline is a Delta Live Tables streaming table definition. This runs inside a DLT pipeline and manages checkpoints automatically.

```sql
-- Delta Live Tables pipeline — SQL notebook
-- This SQL must run inside a DLT pipeline context

CREATE OR REFRESH STREAMING LIVE TABLE main.bronze.customer_events_stream
COMMENT "Bronze: raw customer events ingested from Azure Event Hubs via Kafka protocol"
AS
SELECT
    event_data:event_id::STRING       AS event_id,
    event_data:customer_id::BIGINT    AS customer_id,
    event_data:event_type::STRING     AS event_type,
    event_data:session_id::STRING     AS session_id,
    event_data:page_url::STRING       AS page_url,
    event_data:event_timestamp::BIGINT AS event_timestamp,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    current_timestamp()               AS _ingested_at
FROM (
    SELECT
        PARSE_JSON(CAST(value AS STRING)) AS event_data,
        partition                          AS kafka_partition,
        offset                             AS kafka_offset,
        timestamp                          AS kafka_timestamp
    FROM stream(
        read_kafka(
            bootstrapServers => '<your-namespace>.servicebus.windows.net:9093',
            subscribe        => 'customer-events',
            startingOffsets  => 'latest'
        )
    )
);
```

### Discussion and Concerns

- **Trigger modes:** `trigger(processingTime="1 minute")` runs a micro-batch every minute and keeps the cluster alive between batches. `trigger(availableNow=True)` processes all available data and stops — use this for scheduled jobs that run on a cron schedule rather than continuously. `trigger(once=True)` is deprecated in favour of `availableNow=True`.
- **Stateful operations and watermarking:** Aggregations (counts, sums, distinct counts) over streaming data require stateful processing. Use `withWatermark("event_timestamp", "10 minutes")` to tell Spark how long to wait for late-arriving events before finalising a window. Without a watermark, state grows unboundedly and the job will eventually run out of memory.
- **`failOnDataLoss`:** Setting this to `false` allows the stream to continue if Kafka offsets are outside the retention window (e.g., if the job was paused for longer than the Kafka retention period). Set to `true` in environments where data loss is unacceptable — the job will fail and alert, requiring a decision on whether to reset offsets or restore from a backup.
- **Event Hubs consumer group:** Each streaming job should use a dedicated Kafka consumer group. Multiple jobs reading the same topic should each have their own consumer group to track offsets independently. Configure with `.option("kafka.group.id", "bronze-customer-events-consumer")`.
- **Secret management:** Connection strings and API keys must always be stored in Databricks Secrets and retrieved with `dbutils.secrets.get()`. Never commit secrets to notebooks or source code.

### See Also

- [Structured Streaming with Kafka — Databricks](https://docs.databricks.com/en/structured-streaming/kafka.html)
- [Azure Event Hubs with Kafka — Microsoft](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)
- [Structured Streaming watermarking — Apache Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)
- `ingestion_patterns.md` — Batch vs. Streaming Trade-offs

---

## Streaming Ingestion — Delta Live Tables (DLT)

Delta Live Tables is a declarative pipeline framework built on Databricks. It manages the Structured Streaming runtime — checkpoints, cluster lifecycle, retries, and schema evolution — automatically. Pipelines are defined as Python or SQL notebooks that declare tables using decorators or SQL keywords, and DLT executes and orchestrates them.

### Problem

The data engineering team needs a managed ingestion pipeline that reads files from a landing zone, applies data quality rules, and materialises a cleaned silver table — without the team having to manually manage checkpoint directories, handle cluster restarts, or write retry logic. The pipeline must surface data quality metrics in a built-in observability layer so that data issues are visible without querying the tables themselves.

### Solution

Define a DLT pipeline with a bronze streaming table reading from `cloud_files()` and a silver live table applying `@dlt.expect` quality expectations. DLT manages all infrastructure, checkpointing, and retry logic.

#### Python Example

```python
import dlt
from pyspark.sql.functions import col, current_timestamp, when, trim

# Bronze table: raw ingestion from cloud storage via Auto Loader
# DLT manages the checkpoint location automatically
@dlt.table(
    name="customer_events_raw",
    comment="Bronze: raw customer events from landing zone, no transformations applied",
    table_properties={"quality": "bronze"}
)
def customer_events_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("abfss://landing@mystorageaccount.dfs.core.windows.net/customer_events/")
        .withColumn("_ingested_at", current_timestamp())
    )


# Silver table: cleaned and validated customer events
# @dlt.expect decorators enforce data quality rules
@dlt.table(
    name="customer_events",
    comment="Silver: validated and cleaned customer events",
    table_properties={"quality": "silver"}
)
@dlt.expect("event_id is not null", "event_id IS NOT NULL")
@dlt.expect("customer_id is valid", "customer_id > 0")
@dlt.expect_or_drop("event_type is known",
                     "event_type IN ('page_view', 'add_to_cart', 'purchase', 'session_start', 'session_end')")
def customer_events():
    return (
        dlt.read_stream("customer_events_raw")
        .select(
            col("event_id"),
            col("customer_id").cast("long"),
            trim(col("event_type")).alias("event_type"),
            col("session_id"),
            col("page_url"),
            col("event_timestamp"),
            col("_ingested_at")
        )
        .filter(col("event_id").isNotNull())
    )
```

#### SQL Example

```sql
-- Delta Live Tables pipeline — SQL notebook

-- Bronze table: raw ingestion from cloud storage
CREATE OR REFRESH STREAMING TABLE customer_events_raw
COMMENT "Bronze: raw customer events from landing zone"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
    *,
    current_timestamp() AS _ingested_at
FROM cloud_files(
    'abfss://landing@mystorageaccount.dfs.core.windows.net/customer_events/',
    'json',
    map('cloudFiles.inferColumnTypes', 'true', 'cloudFiles.schemaEvolutionMode', 'addNewColumns')
);

-- Silver table: validated and cleaned customer events
CREATE OR REFRESH STREAMING TABLE customer_events (
    CONSTRAINT event_id_not_null    EXPECT (event_id IS NOT NULL),
    CONSTRAINT customer_id_valid    EXPECT (customer_id > 0),
    CONSTRAINT event_type_known     EXPECT (event_type IN ('page_view', 'add_to_cart', 'purchase', 'session_start', 'session_end')) ON VIOLATION DROP ROW
)
COMMENT "Silver: validated and cleaned customer events"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
    event_id,
    CAST(customer_id AS BIGINT)     AS customer_id,
    TRIM(event_type)                AS event_type,
    session_id,
    page_url,
    event_timestamp,
    _ingested_at
FROM STREAM(LIVE.customer_events_raw)
WHERE event_id IS NOT NULL;
```

### Discussion and Concerns

- **DLT vs. raw Structured Streaming:** DLT abstracts away checkpoint management, cluster lifecycle, and retry logic, reducing the operational burden significantly. The trade-off is reduced flexibility: DLT pipelines run on DLT-managed clusters with fixed configuration, trigger timing is controlled by the pipeline mode (continuous or triggered), and not all Structured Streaming options are available inside DLT. Teams with complex stateful aggregations or fine-grained trigger requirements should use raw Structured Streaming.
- **DLT compute and cost model:** DLT pipelines run on dedicated DLT clusters, not on shared all-purpose clusters. DLT incurs a DBU premium (Enhanced tier adds approximately 25% to the DBU cost; Core tier is lower). Evaluate the cost model against the operational savings before choosing DLT.
- **`@dlt.expect` vs. `@dlt.expect_or_drop` vs. `@dlt.expect_or_fail`:** `@dlt.expect` records violations as a metric but does not drop or fail; use this for monitoring soft rules. `@dlt.expect_or_drop` removes violating rows from the output; use for rows that cannot be safely loaded downstream. `@dlt.expect_or_fail` stops the pipeline on any violation; use only for hard constraints where bad data must not proceed at all.
- **Pipeline modes:** Triggered mode runs the pipeline on demand or on a schedule and stops when all available data has been processed — equivalent to `trigger(availableNow=True)`. Continuous mode keeps the pipeline running and processes new data with low latency. Use triggered mode for most batch-oriented ingestion; use continuous only for genuine low-latency requirements.
- **DLT event log:** DLT writes detailed pipeline events (expectations passed/failed, rows dropped, runtime errors) to a managed Delta table (`system.storage.dlt_event_log` or the pipeline-level `event_log` table). Query this table for data quality observability.

### See Also

- [Delta Live Tables documentation — Databricks](https://docs.databricks.com/en/delta-live-tables/index.html)
- [DLT expectations — Databricks](https://docs.databricks.com/en/delta-live-tables/expectations.html)
- [DLT pipeline modes — Databricks](https://docs.databricks.com/en/delta-live-tables/updates.html)
- `ingestion_patterns.md` — Ingestion Method Selection, Batch vs. Streaming Trade-offs

---

## Ad Hoc Ingestion — Notebook Pattern

The Notebook Pattern is the simplest possible ingestion approach: read a file with `spark.read`, apply minimal transformations, and write to a Delta table. It has no state tracking, no checkpoint, and no deduplication. It is appropriate for one-time or exploratory loads only.

### Problem

A data analyst has received a one-time export of historical customer records as a CSV file and needs to load it into a Delta table in the bronze layer for exploration and profiling. The load will happen once; there is no recurring pipeline. The analyst needs to get the data into the lakehouse quickly without setting up formal pipeline infrastructure.

### Solution

Use `spark.read` with an explicit schema definition to read the CSV file and write it to a Delta table with `mode("overwrite")` for a full replacement or `mode("append")` if appending to an existing dataset.

#### Python Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, DecimalType
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

# Define the schema explicitly — do not rely on CSV inference for production-quality loads
# Inferring schema from CSV reads the file twice and can produce wrong types for IDs and dates
customer_schema = StructType([
    StructField("customer_id",    LongType(),         nullable=False),
    StructField("first_name",     StringType(),       nullable=True),
    StructField("last_name",      StringType(),       nullable=True),
    StructField("email",          StringType(),       nullable=True),
    StructField("date_of_birth",  DateType(),         nullable=True),
    StructField("country_code",   StringType(),       nullable=True),
    StructField("credit_limit",   DecimalType(12, 2), nullable=True),
    StructField("created_date",   DateType(),         nullable=True)
])

source_path = "abfss://landing@mystorageaccount.dfs.core.windows.net/adhoc/customer_export_20260101.csv"

df = (
    spark.read
    .format("csv")
    .schema(customer_schema)
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .option("nullValue", "")
    .load(source_path)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(source_path))
)

# Preview the data before writing
df.show(10)
print(f"Row count: {df.count()}")

# Write to Unity Catalog Delta table
# Use overwrite for a clean one-time load; use append only if you intend to add to existing data
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("main.bronze.customer_historical_load")
)

print("Load complete.")
spark.sql("SELECT COUNT(*) AS row_count FROM main.bronze.customer_historical_load").show()
```

#### SQL Example

Databricks SQL supports reading files directly using the `read_files()` table-valued function (available in Databricks Runtime 13.3+ and SQL Warehouses with Photon). This is the SQL-native equivalent of the Notebook Pattern for ad hoc exploration.

```sql
-- Create a Delta table from a CSV file in a single statement
-- read_files() reads directly from cloud storage without a prior COPY INTO or Auto Loader step
CREATE OR REPLACE TABLE main.bronze.customer_historical_load
USING DELTA
AS
SELECT
    CAST(customer_id  AS BIGINT)         AS customer_id,
    first_name,
    last_name,
    email,
    CAST(date_of_birth AS DATE)          AS date_of_birth,
    country_code,
    CAST(credit_limit AS DECIMAL(12, 2)) AS credit_limit,
    CAST(created_date AS DATE)           AS created_date,
    current_timestamp()                  AS _ingested_at
FROM read_files(
    'abfss://landing@mystorageaccount.dfs.core.windows.net/adhoc/customer_export_20260101.csv',
    format          => 'csv',
    header          => true,
    inferSchema     => false,
    schema          => 'customer_id STRING, first_name STRING, last_name STRING, email STRING,
                        date_of_birth STRING, country_code STRING, credit_limit STRING, created_date STRING'
);

-- Validate the load
SELECT COUNT(*) AS row_count FROM main.bronze.customer_historical_load;
```

### Discussion and Concerns

- **No state tracking or deduplication:** If this notebook is run twice, the table will contain duplicate records (with `mode("overwrite")`, the entire table is replaced; with `mode("append")`, every row is duplicated). There is no mechanism to prevent this automatically. If there is any possibility the load may be re-run, add an explicit deduplication step using `MERGE INTO` or a `ROW_NUMBER()` window function before writing.
- **Not suitable for production recurring loads:** The Notebook Pattern has no checkpoint, no alerting, no audit trail beyond the notebook run history, and no recovery mechanism beyond re-running the notebook manually. Any ingestion that recurs on a schedule must use Auto Loader, COPY INTO, or DLT.
- **Schema definition:** Always define the schema explicitly for production-quality loads. CSV schema inference reads the entire file to infer types and may misclassify numeric IDs as integers or dates as strings. An explicit schema also serves as documentation of the expected structure.
- **`overwriteSchema = true`:** Required when using `mode("overwrite")` if the schema of the new data differs from the existing table schema. This replaces the table schema entirely — use with caution if downstream consumers depend on the existing schema.

### See Also

- [read_files() table-valued function — Databricks](https://docs.databricks.com/en/sql/language-manual/functions/read_files.html)
- [Delta table write operations — Databricks](https://docs.databricks.com/en/delta/delta-update.html)
- `ingestion_patterns.md` — Ingestion Method Selection (Notebook Pattern)

---

## Ad Hoc Ingestion — REST API Ingestion

REST API ingestion covers the pattern of pulling data from an external HTTP API, handling pagination, and landing the results in a Delta table. Unlike file or stream ingestion, there is no native Databricks or Spark connector for generic REST APIs — this is a Python-only pattern.

### Problem

A business system exposes its data via a paginated REST API (for example, a CRM, ticketing system, or third-party data provider). The data platform needs to pull records from this API on a daily basis and land them in a Delta table for downstream processing. The API returns JSON responses and uses cursor-based pagination. API keys must be handled securely.

### Solution

Use the `requests` Python library to call the API, handle pagination in a loop, accumulate the results, and write them to a Delta table using `spark.createDataFrame()`. Retrieve the API key from Databricks Secrets using `dbutils.secrets.get()`.

#### Python Example

```python
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import date

spark = SparkSession.builder.getOrCreate()

# Retrieve the API key from Databricks Secrets — never hardcode credentials
api_key = dbutils.secrets.get(scope="ingestion-secrets", key="crm-api-key")

base_url = "https://api.example-crm.com/v2"
endpoint = "/contacts"

headers = {
    "Authorization": f"Bearer {api_key}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Pagination parameters — this example uses cursor-based pagination
# Adjust for your API's pagination mechanism (page number, offset, next_token, etc.)
params = {
    "limit": 100,
    "updated_since": "2026-01-01T00:00:00Z"
}

all_records = []
page_count = 0
max_pages = 500  # Safety limit to prevent runaway loops

cursor = None

while page_count < max_pages:
    if cursor:
        params["cursor"] = cursor

    response = requests.get(
        url=f"{base_url}{endpoint}",
        headers=headers,
        params=params,
        timeout=30
    )

    # Raise an exception for HTTP 4xx/5xx responses
    response.raise_for_status()

    payload = response.json()
    records = payload.get("data", [])

    if not records:
        break

    all_records.extend(records)
    page_count += 1

    # Get the next cursor; if absent or null, we have reached the last page
    cursor = payload.get("pagination", {}).get("next_cursor")
    if not cursor:
        break

    print(f"Fetched page {page_count}, total records so far: {len(all_records)}")

print(f"Fetch complete. Total records: {len(all_records)} across {page_count} pages.")

if not all_records:
    print("No records returned from API. Skipping write.")
else:
    # Convert to a Spark DataFrame
    # Use spark.read.json on an RDD of JSON strings for schema inference,
    # or define an explicit schema for robustness
    rdd = spark.sparkContext.parallelize([json.dumps(record) for record in all_records])
    df = spark.read.json(rdd)

    # Add ingestion metadata
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_ingestion_date", lit(str(date.today())))

    # Append to the raw landing table — use merge for upsert if needed
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("main.bronze.crm_contacts_raw")
    )

    print(f"Wrote {df.count()} records to main.bronze.crm_contacts_raw")
```

#### SQL Example

There is no SQL equivalent for REST API ingestion. SQL engines, including Databricks SQL, cannot make outbound HTTP requests natively. This pattern is Python-only. Once the data has been landed in a Delta table by the Python job above, standard SQL transformations can operate on it.

### Discussion and Concerns

- **Rate limiting:** Most APIs enforce rate limits (requests per minute or requests per day). Add retry logic with exponential backoff for HTTP 429 (Too Many Requests) responses. The `requests` library's `Retry` adapter from `urllib3` can be used, or a simple `time.sleep()` between pages for low-volume APIs.
- **Error handling:** `response.raise_for_status()` raises an exception for any 4xx or 5xx response. Wrap the request loop in a `try/except` block to handle transient failures gracefully and log the error before retrying or aborting.
- **Secret management:** API keys, OAuth tokens, and connection strings must always be stored in Databricks Secrets (`dbutils.secrets.put` via CLI, or via the Databricks UI). Retrieve them at runtime with `dbutils.secrets.get(scope, key)`. The value is redacted in notebook output automatically.
- **Idempotency:** The example above uses `mode("append")`, which will insert duplicate records if the job is re-run for the same date range. For idempotent re-runs, use a `MERGE INTO` statement after loading to deduplicate on the business key (e.g., `contact_id`), or partition the landing table by `_ingestion_date` and overwrite the partition on re-run using dynamic partition overwrite.
- **Volume considerations:** This pattern is designed for APIs returning thousands to tens of thousands of records per run. For APIs with millions of records, consider whether the source system provides a bulk export endpoint (CSV or Parquet download) which can be loaded more efficiently via COPY INTO or Auto Loader.
- **OAuth token refresh:** If the API uses OAuth 2.0 with short-lived access tokens, implement token refresh logic before the loop. Store the client ID and client secret in Databricks Secrets and request a new token at the start of each job run.

### See Also

- [Databricks Secrets documentation](https://docs.databricks.com/en/security/secrets/index.html)
- [requests library documentation](https://requests.readthedocs.io/en/latest/)
- [Delta Lake merge documentation — Databricks](https://docs.databricks.com/en/delta/merge.html)

---

## dbt Ingestion — dbt Seeds

dbt seeds load small, static CSV files from the dbt project repository into the data warehouse as Delta tables. They are version-controlled with the project, making them the most auditable way to manage reference data such as country codes, currency codes, status mappings, and product category hierarchies.

### Problem

The data model requires a lookup table mapping ISO 3166-1 alpha-2 country codes to country names and regions. This table has 249 rows, changes at most once every few years when new countries are recognised, and must be consistent across all environments (dev, staging, prod). Inconsistency between environments in a prior project caused reporting discrepancies that took two days to diagnose.

### Solution

Place the CSV file in the `seeds/` directory of the dbt project, declare its schema and configuration in `dbt_project.yml`, and run `dbt seed` to load it into the target catalog and schema.

#### dbt_project.yml Seed Configuration

```yaml
# dbt_project.yml (excerpt)

name: vibe_ingestion
version: "1.0.0"

profile: vibe_cookbooks

models:
  vibe_ingestion:
    bronze:
      +schema: bronze
      +materialized: table

seeds:
  vibe_ingestion:
    +schema: reference          # Seeds load into main.reference (catalog.schema)
    +quote_columns: false
    country_codes:
      +column_types:
        country_code:   varchar(2)
        country_name:   varchar(100)
        region:         varchar(50)
        sub_region:     varchar(100)
        is_active:      boolean
```

#### Sample seeds/country_codes.csv

```csv
country_code,country_name,region,sub_region,is_active
AD,Andorra,Europe,Southern Europe,true
AE,United Arab Emirates,Asia,Western Asia,true
AF,Afghanistan,Asia,Southern Asia,true
AG,Antigua and Barbuda,Americas,Caribbean,true
AL,Albania,Europe,Southern Europe,true
AM,Armenia,Asia,Western Asia,true
AO,Angola,Africa,Middle Africa,true
AR,Argentina,Americas,South America,true
AT,Austria,Europe,Western Europe,true
AU,Australia,Oceania,Australia and New Zealand,true
AZ,Azerbaijan,Asia,Western Asia,true
BA,Bosnia and Herzegovina,Europe,Southern Europe,true
BB,Barbados,Americas,Caribbean,true
BD,Bangladesh,Asia,Southern Asia,true
BE,Belgium,Europe,Western Europe,true
BF,Burkina Faso,Africa,Western Africa,true
BG,Bulgaria,Europe,Eastern Europe,true
GB,United Kingdom,Europe,Northern Europe,true
US,United States of America,Americas,Northern America,true
ZA,South Africa,Africa,Southern Africa,true
ZW,Zimbabwe,Africa,Eastern Africa,true
```

#### Running the Seed

```bash
# Load all seeds into the target environment
dbt seed --target dev

# Load a specific seed by name
dbt seed --select country_codes --target dev

# Full refresh — drops and recreates the seed table (required when the CSV changes)
dbt seed --full-refresh --select country_codes --target prod
```

#### SQL Example (Compiled Output)

dbt compiles a seed into a `CREATE OR REPLACE TABLE` statement. The compiled SQL (visible in `target/compiled/`) looks like this:

```sql
-- Compiled by dbt: target/compiled/vibe_ingestion/seeds/country_codes.csv.sql
-- This SQL is generated by dbt and executed against the SQL Warehouse

CREATE OR REPLACE TABLE main.reference.country_codes (
    country_code  VARCHAR(2),
    country_name  VARCHAR(100),
    region        VARCHAR(50),
    sub_region    VARCHAR(100),
    is_active     BOOLEAN
)
USING DELTA;

INSERT INTO main.reference.country_codes VALUES
    ('AD', 'Andorra',                  'Europe',   'Southern Europe',            true),
    ('AE', 'United Arab Emirates',     'Asia',     'Western Asia',               true),
    ('AF', 'Afghanistan',              'Asia',     'Southern Asia',              true),
    -- ... (remaining rows)
    ('ZW', 'Zimbabwe',                 'Africa',   'Eastern Africa',             true);
```

### Discussion and Concerns

- **Row count practical limit:** dbt seeds generate a single INSERT statement with all rows. For tables with more than 10,000 rows, this INSERT becomes very large and slow. Seeds with row counts approaching this limit should be replaced with a COPY INTO pipeline reading from a CSV in cloud storage.
- **Full-refresh only:** Seeds are always fully replaced on each run — there is no incremental mode. Every `dbt seed` run drops and recreates the table. This is appropriate for small, rarely changing reference data but is not acceptable for any dataset where historical records must be preserved.
- **Version control advantage:** Because the seed CSV is committed to the repository, every change to the reference data is traceable via git history. This is a significant advantage over reference data managed as ad hoc Delta tables — you can see exactly what the country code mapping was on any date in the past by checking out the corresponding commit.
- **Environment consistency:** Running `dbt seed` as part of the CI/CD pipeline ensures the reference tables are identical across dev, staging, and prod. This eliminates the class of environment-specific discrepancy described in the Problem statement above.
- **Testing seeds:** Add schema tests in `schema.yml` alongside the seed definition:

```yaml
# models/schema.yml or seeds/schema.yml
seeds:
  - name: country_codes
    columns:
      - name: country_code
        tests:
          - unique
          - not_null
      - name: country_name
        tests:
          - not_null
```

Run `dbt test --select country_codes` to validate the seed data after loading.

### See Also

- [dbt seeds documentation](https://docs.getdbt.com/docs/build/seeds)
- [dbt seed configuration reference](https://docs.getdbt.com/reference/seed-configs)
- `ingestion_patterns.md` — dbt Seeds as a Reference Ingestion Pattern

---

## dbt Ingestion — AutomateDV Stage Macro

AutomateDV (formerly dbtvault) is a dbt package that provides macros for building Data Vault 2.0 structures on Databricks. The `stage` macro is the entry point for all vault loading — it derives hash keys and hashdiff columns from raw source data and prepares it for loading into hubs, links, and satellites.

### Problem

Raw customer order data has been ingested into `main.bronze.orders_raw` by an Auto Loader pipeline. Before this data can be loaded into the Data Vault 2.0 vault structures (the `hub_customer`, `hub_order`, `link_customer_order`, and `sat_order_details` tables), hash keys must be derived from the business keys and a hashdiff column must be computed from all descriptive attributes. These derived columns must be computed consistently across every pipeline run using the same algorithm and column ordering.

### Solution

Create a dbt model that uses the `automate_dv.stage()` macro. The macro accepts a source model or table reference, a list of columns to hash as key columns, a list of columns to include in the hashdiff, and any derived or null-substituted columns. dbt compiles this into a SQL view or table that downstream vault loading macros reference.

#### dbt Model: models/staging/stg_orders.sql

```sql
-- models/staging/stg_orders.sql
-- AutomateDV staging model for the orders source
-- This model is typically materialised as a view (no storage cost, always current)

{{
    config(
        materialized = 'view',
        schema       = 'staging'
    )
}}

{%- set yaml_metadata -%}
source_model: "orders_raw"
derived_columns:
  LOAD_DATE: "CAST(CURRENT_DATE() AS DATE)"
  RECORD_SOURCE: "'main.bronze.orders_raw'"
null_columns:
  CUSTOMER_ID: "UNKNOWN"
  ORDER_ID: "UNKNOWN"
hashed_columns:
  HK_CUSTOMER:
    - "CUSTOMER_ID"
  HK_ORDER:
    - "ORDER_ID"
  HK_CUSTOMER_ORDER:
    - "CUSTOMER_ID"
    - "ORDER_ID"
  HASHDIFF_ORDER_DETAILS:
    is_hashdiff: true
    columns:
      - "ORDER_STATUS"
      - "TOTAL_AMOUNT"
      - "CURRENCY_CODE"
      - "SHIPPING_ADDRESS"
      - "BILLING_ADDRESS"
      - "DISCOUNT_CODE"
      - "NOTES"
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{{ automate_dv.stage(
    include_source_columns = true,
    source_model           = metadata_dict["source_model"],
    derived_columns        = metadata_dict["derived_columns"],
    null_columns           = metadata_dict["null_columns"],
    hashed_columns         = metadata_dict["hashed_columns"]
) }}
```

#### dbt_project.yml Variables (Hash Algorithm Configuration)

```yaml
# dbt_project.yml (excerpt)
# The hash algorithm is set once for the entire project
# All staging models use this setting — it cannot differ between models

vars:
  automate_dv:
    hash: MD5                    # MD5 (default) or SHA for SHA-256
    concat_string: "||"          # Column separator used before hashing
    null_placeholder: "^^"       # Substituted for NULL values in hash inputs
```

#### Running the Staging Model

```bash
# Compile the staging model to inspect the generated SQL before running
dbt compile --select stg_orders

# Run the staging model
dbt run --select stg_orders --target dev

# Run all staging models
dbt run --select staging --target dev
```

#### SQL Example (Compiled Output)

The compiled SQL for the AutomateDV stage macro (visible in `target/compiled/`) looks like this:

```sql
-- Compiled output: target/compiled/vibe_ingestion/models/staging/stg_orders.sql
-- Generated by the automate_dv.stage() macro

SELECT
    -- Hashed key columns (MD5 of concatenated, null-substituted business keys)
    MD5(NULLIF(UPPER(TRIM(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '^^'))), ''))
        AS HK_CUSTOMER,

    MD5(NULLIF(UPPER(TRIM(COALESCE(CAST(ORDER_ID AS VARCHAR), '^^'))), ''))
        AS HK_ORDER,

    MD5(NULLIF(CONCAT_WS('||',
        UPPER(TRIM(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '^^'))),
        UPPER(TRIM(COALESCE(CAST(ORDER_ID AS VARCHAR), '^^')))
    ), ''))
        AS HK_CUSTOMER_ORDER,

    -- Hashdiff column (MD5 of all descriptive attribute columns, ordered)
    MD5(NULLIF(CONCAT_WS('||',
        UPPER(TRIM(COALESCE(CAST(ORDER_STATUS AS VARCHAR),       '^^'))),
        UPPER(TRIM(COALESCE(CAST(TOTAL_AMOUNT AS VARCHAR),       '^^'))),
        UPPER(TRIM(COALESCE(CAST(CURRENCY_CODE AS VARCHAR),      '^^'))),
        UPPER(TRIM(COALESCE(CAST(SHIPPING_ADDRESS AS VARCHAR),   '^^'))),
        UPPER(TRIM(COALESCE(CAST(BILLING_ADDRESS AS VARCHAR),    '^^'))),
        UPPER(TRIM(COALESCE(CAST(DISCOUNT_CODE AS VARCHAR),      '^^'))),
        UPPER(TRIM(COALESCE(CAST(NOTES AS VARCHAR),              '^^')))
    ), ''))
        AS HASHDIFF_ORDER_DETAILS,

    -- Derived columns added by the stage macro
    CAST(CURRENT_DATE() AS DATE)    AS LOAD_DATE,
    'main.bronze.orders_raw'        AS RECORD_SOURCE,

    -- All original source columns included because include_source_columns = true
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    TOTAL_AMOUNT,
    CURRENCY_CODE,
    SHIPPING_ADDRESS,
    BILLING_ADDRESS,
    DISCOUNT_CODE,
    NOTES,
    ORDER_DATE,
    UPDATED_AT

FROM main.bronze.orders_raw
```

#### Python Note

There is no Python equivalent for the AutomateDV stage macro. The macro is a dbt SQL macro that generates SQL at compile time. Python-based vault loading (using PySpark directly) would require manually implementing the hash concatenation and MD5/SHA-256 computation shown in the compiled SQL above — this is feasible but significantly increases the risk of inconsistency between models. The dbt macro approach is strongly preferred.

### Discussion and Concerns

- **Hash algorithm consistency:** The `hash` variable in `dbt_project.yml` applies to every staging model in the project. All models must use the same algorithm. If you switch from MD5 to SHA-256 after vault tables have been populated, every hash value in every hub, link, and satellite changes. This is effectively a rebuild of the entire vault — plan for a full reprocessing exercise if a hash algorithm change is ever required.
- **Column ordering in composite keys:** The order of columns in `hashed_columns` definitions is the order in which they are concatenated before hashing. `['CUSTOMER_ID', 'ORDER_ID']` and `['ORDER_ID', 'CUSTOMER_ID']` produce different hash values. Column order must be documented in the project's data dictionary and must never change once the vault is in production. Code review must verify column ordering in every new staging model.
- **Hashdiff column scope:** Every descriptive column that will be loaded into a satellite must be included in the hashdiff for that satellite. If a column is added to the satellite later, the hashdiff definition must be updated, which will cause all existing records to be evaluated as changed on the next load (because the hashdiff value changes when new columns are included). Plan for this by ensuring the hashdiff scope matches the satellite scope from the beginning.
- **`include_source_columns = true`:** This passes all original source columns through to the staging view alongside the derived hash columns. Downstream vault loading macros (`hub`, `link`, `sat`) reference the staging model and select the columns they need. Setting this to `false` and explicitly listing `ranked_columns` gives more control over which columns are exposed.
- **Null placeholder:** The `null_placeholder` variable (`^^` by default) is substituted for `NULL` values before hashing. This must be a value that cannot appear in any business key. Verify that `^^` does not conflict with any business key values in the source systems before deploying to production.

### See Also

- [AutomateDV stage macro documentation](https://automate-dv.readthedocs.io/en/latest/tutorial/tut_staging/)
- [AutomateDV hash configuration](https://automate-dv.readthedocs.io/en/latest/best_practices/hashing/)
- [AutomateDV dbt package — GitHub](https://github.com/Datavault-UK/automate-dv)
- `ingestion_patterns.md` — Raw Staging for Data Vault 2.0

---

## Managing Your Environment

### Monitoring Your Environment in Production

Use the following signals to observe ingestion pipelines in a running production environment.

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Auto Loader checkpoint lag | Spark UI > Streaming tab on the running job, or `system.lakeflow.job_run_timeline` | Consumer lag growing over time; checkpoint directory inaccessible (job fails on startup) |
| COPY INTO load history | `DESCRIBE HISTORY main.bronze.<table>` or `system.storage.table_history` | `numCopiedFiles` and `numOutputRows` — zero on a scheduled run indicates no new files arrived (may be expected or may indicate a source delivery issue) |
| Streaming job run status | Databricks Jobs UI > Job Run History, or `system.lakeflow.job_runs` | Consecutive failures; retry count exceeding threshold; job duration increasing (backlog growth) |
| DLT pipeline event log | DLT Pipeline UI > Event Log tab, or `SELECT * FROM <pipeline_event_log> ORDER BY timestamp DESC` | Expectation failure counts; rows dropped by `EXPECT ON VIOLATION DROP ROW`; pipeline restarts |
| Auto Loader file backlog | Auto Loader metrics exposed via Spark UI > Streaming tab: `numFilesOutstanding` | Growing backlog indicates the pipeline is not keeping up with file arrival rate; consider increasing cluster size or trigger frequency |
| dbt seed run status | dbt Cloud job run history, or CI/CD pipeline logs | Seed failures indicate the CSV file has a formatting issue or column type mismatch; test failures indicate reference data quality regression |
| Delta table freshness | `DESCRIBE HISTORY main.bronze.<table>` — check `timestamp` of the latest `WRITE` operation | A table that has not been written to within the expected SLA window indicates a pipeline failure |
| Kafka/Event Hubs consumer lag | Azure Monitor (Event Hubs metrics) or Kafka consumer group lag tool (`kafka-consumer-groups.sh`) | Consumer group lag exceeding the defined SLA threshold (e.g., > 5 minutes of events unprocessed) |

### Metrics for Success

The following checklist defines what "working correctly" looks like for ingestion pipelines in production. These are observable, not aspirational — each item can be verified by querying a system table or checking a UI.

- [ ] No duplicate records after a job re-run: validate with `SELECT order_id, COUNT(*) FROM main.bronze.orders_raw GROUP BY order_id HAVING COUNT(*) > 1` returning zero rows
- [ ] Checkpoint directory exists and is non-empty for all streaming jobs: verify by listing the checkpoint path in ADLS/S3/GCS and confirming the `offsets/` subdirectory is present
- [ ] Schema evolution handled without pipeline failure: confirm via Auto Loader stream metrics that the schema location has been updated and new columns are visible in `DESCRIBE TABLE main.bronze.<table>`
- [ ] COPY INTO `DESCRIBE HISTORY` shows `numCopiedFiles` increasing on each scheduled run: zero on a run where new files were expected indicates a source delivery failure or path misconfiguration
- [ ] DLT pipeline shows zero failed expectations for critical (`EXPECT ON VIOLATION FAIL`) rules: query the DLT event log for `expectation_violated` events with severity `ERROR`
- [ ] Streaming consumer lag is below the agreed SLA (e.g., < 5 minutes for near-real-time pipelines): observable in Spark UI Streaming tab or Azure Monitor
- [ ] dbt seed tests pass after every `dbt seed` run: `dbt test --select country_codes` returns no failures
- [ ] AutomateDV staging models produce non-null hash key columns for all records with non-null business keys: validate with `SELECT COUNT(*) FROM main.staging.stg_orders WHERE HK_ORDER IS NULL AND ORDER_ID IS NOT NULL` returning zero
