# Performance Tuning Guide

## Databricks

> This guide uses Delta Lake, PySpark, Databricks Workflows, and Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT) exclusively. It is self-contained — no companion document is required.

---

## Introduction

This guide provides architectural decision guidance and practical, step-by-step implementation for **performance tuning** on Databricks. It covers Delta Lake storage optimisation, Adaptive Query Execution, Photon, caching, cluster sizing, statistics, native incremental loading patterns, and Lakeflow Spark Declarative Pipelines incremental pipeline performance. No prior knowledge of the specific tuning technique is assumed.

Each method section follows a consistent structure: the problem being solved, the recommended solution with Python and SQL examples, any known concerns or trade-offs, and links to further reading.

---

## Design Decisions

Use this section to select the right optimization approach before implementing.

### Delta Lake Storage Optimization

| Scenario | Recommended Approach |
|----------|----------------------|
| New table, filter patterns unknown or evolving | Liquid Clustering |
| Existing table, 1–3 stable high-cardinality filter columns | OPTIMIZE + ZORDER BY |
| Large table filtered almost exclusively on a date column | Static partitioning by date (day or month), then ZORDER on secondary columns |
| Filter column is already a partition key | ZORDER on that column has no effect — ZORDER secondary columns only |
| Table receives many small writes (streaming or micro-batch) | OPTIMIZE on a scheduled cadence; consider Liquid Clustering to reduce maintenance overhead |
| Over-partitioned table with millions of directories | Rewrite with coarser partitioning or migrate to Liquid Clustering |

**Key constraints:** Liquid Clustering (DBR 13.3+) is incompatible with static partitioning — cannot be applied to tables already using `PARTITIONED BY`. ZORDER must be re-applied on every OPTIMIZE run; it degrades as new data is written.

### Compute Selection: Spark Cluster vs. SQL Warehouse

**Use a Spark Cluster when:** running ETL (PySpark, Scala, Java); streaming pipelines (Structured Streaming or SDP); ML training/inference; interactive PySpark notebooks; Lakeflow SDP pipelines.

**Use a SQL Warehouse when:** ad hoc SQL queries or BI tool connections (Tableau, Power BI, Looker via JDBC/ODBC); dashboards with concurrent users; short analytical queries where per-query billing matters.

**Cost rule of thumb:** Serverless SQL Warehouse (≈2 s startup) bills per query execution second and scales to zero when idle — cheapest for sporadic queries. Job clusters bill DBU continuously from start to auto-termination; always set auto-termination. All-purpose clusters left running overnight accumulate DBU waste with no benefit.

### AQE Configuration Reference

AQE is enabled by default on DBR 7.3+. The defaults are appropriate for most workloads; tune for large or heavily skewed tables:

| Configuration Key | Default | Purpose |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` | Master toggle — verify not disabled on your cluster |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256 MB | Minimum partition size to be considered skewed |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | A partition is skewed if this many times larger than the median partition |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Increase if large dimension tables are not being broadcast |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | 1 MB | Minimum size for a coalesced output partition |

### Photon Eligibility

| Operation | Photon Eligible | Notes |
|---|---|---|
| SQL queries (SELECT, WHERE, GROUP BY, ORDER BY, HAVING, WINDOW) | Yes | Largest gains on scan-heavy aggregation |
| Delta reads/writes, MERGE INTO, COPY INTO | Yes | |
| Sort/hash aggregation, hash joins | Yes | |
| Python UDFs | **No** | Rewrite as pandas UDFs or SQL expressions to restore Photon coverage |
| Scala / Java UDFs | **No** | |
| RDD operations | **No** | Bypasses the Spark SQL engine entirely |
| Streaming stateful ops (`mapGroupsWithState`, watermarks) | **No** | |
| MLlib pipelines | **No** | |

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

> **On Databricks (interactive notebooks or jobs):** PySpark, Delta Lake (`delta-spark`), and Photon are pre-installed with every Databricks Runtime. No `pip install` is needed to run the code examples in this cookbook on a cluster.
>
> **Local development:** The tools below are installed on your local machine for CLI operations and local testing.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.10+ | Local dev | Required for the Databricks CLI and local PySpark unit tests |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | 0.200+ | Local dev | Used for workspace interaction and job configuration |
| `delta-spark` | Bundled with Databricks Runtime | Databricks (bundled) | Pre-installed; no separate install needed on a cluster |
| Databricks SDK for Python | Latest | Local dev (optional) | Used for cluster config examples |

Configure your Databricks connection (local machine):

```bash
# Authenticate the Databricks CLI
databricks configure --token

# Verify connection
databricks clusters list
```

### Getting the Source for an Existing Project

```bash
git clone <repository_url>
cd <project_directory>
databricks bundle validate
databricks bundle deploy --target dev
```

---

## Infrastructure Pre-Requisites

### Infrastructure Required

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment | Unity Catalog recommended |
| All-Purpose or Job Cluster | PySpark, streaming, SDP pipelines | DBR 13.3+ required for Liquid Clustering |
| SQL Warehouse (Serverless or Classic) | SQL queries, BI tools, maintenance SQL | Photon is always active on SQL Warehouses |
| Unity Catalog — Catalog / Schema | Target for output tables | Requires `CREATE TABLE` privilege |
| ADLS Gen2 / S3 / GCS | Source data for ingestion examples | External location must be configured in Unity Catalog |

### Enabling Photon on a Cluster

Photon is enabled by selecting a Databricks Runtime version that includes Photon in the cluster configuration UI (the runtime name includes "Photon"). It cannot be enabled after cluster creation — it must be selected at creation time. Photon is always active on SQL Warehouses and does not require configuration there.

---

## Delta Lake Optimization — OPTIMIZE and Compaction

Frequent incremental writes — from streaming pipelines, Auto Loader batch loads, or SDP pipeline runs — produce many small Parquet files. Delta reads every file in a table scan, so thousands of small files multiply I/O operations and Spark task overhead. OPTIMIZE compacts these files into larger ones, reducing scan time.

### Problem

A Delta table has accumulated thousands of small files from frequent streaming writes or incremental loads. Queries against the table are slower than expected, and `DESCRIBE DETAIL` on the table shows a high `numFiles` count with a low average file size.

### Solution

Run OPTIMIZE to compact small files into larger ones (targeting approximately 1 GB per output file). For large tables, scope the operation to a recent date partition using a `WHERE` clause to limit the time and resource cost of the operation.

#### Python Example

```python
# Full table compaction — use during off-peak hours for large tables
spark.sql("OPTIMIZE catalog_name.schema_name.my_table")

# Scoped compaction — compact only recently written partitions
# This is faster and sufficient when only recent data has small files
spark.sql("""
    OPTIMIZE catalog_name.schema_name.my_table
    WHERE event_date >= current_date() - 7
""")

# Verify the result — check numFiles and avgFileSize after OPTIMIZE
spark.sql("DESCRIBE DETAIL catalog_name.schema_name.my_table").show(truncate=False)
```

#### SQL Example

```sql
-- Full table compaction
OPTIMIZE catalog_name.schema_name.my_table;

-- Scoped compaction for recently written date partitions
OPTIMIZE catalog_name.schema_name.my_table
WHERE event_date >= current_date() - 7;

-- Verify the result
DESCRIBE DETAIL catalog_name.schema_name.my_table;
```

### Discussion and Concerns

- **Schedule during off-peak hours:** OPTIMIZE is a resource-intensive operation on large tables. It is safe to run while reads are in progress (Delta's MVCC isolation model ensures read consistency), but it competes for cluster resources.
- **Scoped OPTIMIZE is sufficient for incremental pipelines:** If only the last N days of data receive new writes, there is no need to compact the entire table history on every run. Use `WHERE event_date >= ...` to target only the affected range.
- **OPTIMIZE does not delete data:** It rewrites files but does not change logical table contents. Old file versions are retained for Delta time travel until VACUUM is run.
- **Automate with Databricks Workflows:** Schedule OPTIMIZE as a daily or weekly Databricks Workflows job, separate from your data load job, so that compaction does not add to load pipeline latency.

### See Also

- [OPTIMIZE — Databricks SQL Language Manual](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html)
- [Delta Lake File Management — Databricks Documentation](https://docs.databricks.com/en/delta/optimize.html)
- [Delta Lake optimizations — Delta Lake](https://docs.delta.io/latest/optimizations-oss.html)
- [Performance Tuning Architectural Patterns](./performance_cookbook.md)

---

## Delta Lake Optimization — ZORDER Clustering

OPTIMIZE compacts files but does not control which rows end up in which files. ZORDER BY adds a clustering step: it sorts and interleaves rows on the specified columns so that rows with similar values for those columns end up co-located in the same files. Delta's file-skipping mechanism can then use per-file min/max statistics to skip entire files when a query filter does not overlap with the file's range.

### Problem

Queries on a Delta table always filter on the same 1–2 high-cardinality columns (e.g., `customer_id`, `event_type`), but full table scans are occurring — Delta is reading every file despite the filter. The table is large enough that the unnecessary I/O is causing queries to miss SLA.

### Solution

Run OPTIMIZE with ZORDER BY on the high-cardinality filter columns. This must be repeated on every subsequent OPTIMIZE run to maintain clustering quality as new files are written.

#### Python Example

```python
# OPTIMIZE with ZORDER BY on the primary filter columns
spark.sql("""
    OPTIMIZE catalog_name.schema_name.my_table
    ZORDER BY (customer_id, event_type)
""")

# Verify clustering quality — inspect numFilesAdded and numFilesRemoved
spark.sql("DESCRIBE DETAIL catalog_name.schema_name.my_table").show(truncate=False)

# Verify file skipping is occurring on a query — look for "filesSkipped" in the query metrics
# Run EXPLAIN to check the query plan
spark.sql("""
    EXPLAIN FORMATTED
    SELECT * FROM catalog_name.schema_name.my_table
    WHERE customer_id = 'C123' AND event_type = 'purchase'
""").show(truncate=False)
```

#### SQL Example

```sql
-- OPTIMIZE with ZORDER BY
OPTIMIZE catalog_name.schema_name.my_table
ZORDER BY (customer_id, event_type);

-- Check table detail for file counts and average file size
DESCRIBE DETAIL catalog_name.schema_name.my_table;

-- Verify file skipping on a representative filter query
EXPLAIN FORMATTED
SELECT *
FROM catalog_name.schema_name.my_table
WHERE customer_id = 'C123'
  AND event_type = 'purchase';
-- In the EXPLAIN output, look for "PushedFilters" and "numFiles" in the scan node
-- After ZORDER, the number of files scanned should be significantly lower
```

### Discussion and Concerns

- **ZORDER degrades with more than 3–4 columns:** The Z-curve locality guarantee weakens in higher dimensions. If queries filter on more than 3 columns, consider Liquid Clustering instead, which handles multi-column clustering more robustly.
- **ZORDER must be re-applied on every OPTIMIZE run:** New files written after the last ZORDER run are not clustered. If OPTIMIZE runs without ZORDER BY, the new compacted files will also not be clustered.
- **Do not ZORDER on a partition column:** Within a partition, all files already contain only that partition value, so ZORDER on that column has no effect. Apply ZORDER only to non-partition columns.
- **Check effectiveness with DESCRIBE DETAIL and the Spark UI:** The `numFilesAdded` and `numFilesRemoved` values in `DESCRIBE DETAIL` show compaction activity. The Spark UI's SQL tab shows `filesSkipped` in the scan metrics for queries after ZORDERing.

### See Also

- [OPTIMIZE ZORDER BY — Databricks SQL Language Manual](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html)
- [Delta Lake Data Skipping — Databricks Documentation](https://docs.databricks.com/en/delta/data-skipping.html)
- [Performance Tuning Architectural Patterns — Delta Lake Storage Optimization](./performance_cookbook.md)

---

## Delta Lake Optimization — Liquid Clustering

ZORDER requires knowing the query filter columns in advance and re-applying clustering on every OPTIMIZE run. Liquid Clustering (Databricks Runtime 13.3+) is an adaptive replacement: it applies incremental, automatic clustering as data is written, supports multiple clustering columns without degradation, and allows clustering columns to be changed without rewriting the table.

### Problem

Query filter patterns on a table are not stable — different queries filter on different columns, and static ZORDER does not help all of them. Alternatively, the table is new and the access patterns are not yet known.

### Solution

Enable Liquid Clustering at table creation by specifying `CLUSTER BY`. Run OPTIMIZE without `ZORDER BY` to trigger incremental clustering. Liquid Clustering handles the rest automatically.

#### Python Example

```python
from delta.tables import DeltaTable

# Create a new table with Liquid Clustering using the Delta Python API
(
    DeltaTable.createIfNotExists(spark)
    .tableName("catalog_name.schema_name.my_table")
    .addColumn("customer_id", "STRING")
    .addColumn("event_type", "STRING")
    .addColumn("event_date", "DATE")
    .addColumn("amount", "DOUBLE")
    .clusterBy("customer_id", "event_date")
    .execute()
)

# Run OPTIMIZE to trigger incremental clustering — no ZORDER BY needed
spark.sql("OPTIMIZE catalog_name.schema_name.my_table")

# Change clustering columns later without rewriting the table
spark.sql("""
    ALTER TABLE catalog_name.schema_name.my_table
    CLUSTER BY (event_type, event_date)
""")
```

#### SQL Example

```sql
-- Create a table with Liquid Clustering enabled
CREATE TABLE IF NOT EXISTS catalog_name.schema_name.my_table (
    customer_id   STRING,
    event_type    STRING,
    event_date    DATE,
    amount        DOUBLE
)
CLUSTER BY (customer_id, event_date);

-- Run OPTIMIZE to trigger incremental clustering
-- Do NOT add ZORDER BY — it is incompatible with Liquid Clustering
OPTIMIZE catalog_name.schema_name.my_table;

-- Change clustering columns at any time — no table rewrite required
ALTER TABLE catalog_name.schema_name.my_table
CLUSTER BY (event_type, event_date);
```

### Discussion and Concerns

- **Requires DBR 13.3+:** Liquid Clustering is not available on older Databricks Runtime versions. Check your workspace's available runtimes before migrating existing tables.
- **Incompatible with static partitioning:** A table cannot have both `PARTITIONED BY` and `CLUSTER BY`. Do not attempt to add Liquid Clustering to an existing partitioned table — create a new table instead.
- **OPTIMIZE without ZORDER BY triggers clustering:** When Liquid Clustering is enabled, running `OPTIMIZE` applies incremental clustering automatically. Do not add `ZORDER BY` — it is ignored and may produce unexpected behaviour.
- **Clustering column changes are non-destructive:** `ALTER TABLE CLUSTER BY` changes the clustering definition for future OPTIMIZE runs but does not immediately recluster existing data. Data written before the change retains its previous clustering until the next OPTIMIZE pass touches those files.

### See Also

- [Liquid Clustering — Databricks Documentation](https://docs.databricks.com/en/delta/clustering.html)
- [Migrate from Partitioning to Liquid Clustering — Databricks Documentation](https://docs.databricks.com/en/delta/clustering.html#migrate-to-liquid-clustering)
- [Performance Tuning Architectural Patterns — Delta Lake Storage Optimization](./performance_cookbook.md)

---

## Delta Lake Optimization — VACUUM

Delta's MVCC model retains old file versions for time travel and transaction rollback. Over time, this accumulates large volumes of obsolete Parquet files in cloud storage that are no longer referenced by any Delta transaction but continue to incur storage costs. VACUUM removes these files.

### Problem

Old versions of Delta table files are accumulating in cloud storage, increasing storage costs. `DESCRIBE HISTORY` shows many old versions, and direct inspection of the storage container shows many Parquet files that are not referenced by the current table version.

### Solution

Run VACUUM to delete files older than the retention threshold. The default retention period is 7 days (168 hours), which preserves time travel for the last 7 days. Do not reduce below this threshold unless you have explicitly disabled time travel for the table.

#### Python Example

```python
# Preview what VACUUM would delete without deleting anything
spark.sql("VACUUM catalog_name.schema_name.my_table DRY RUN").show(truncate=False)

# Run VACUUM with the default 7-day (168-hour) retention
spark.sql("VACUUM catalog_name.schema_name.my_table RETAIN 168 HOURS")

# For tables where time travel is not needed and you want aggressive cleanup,
# you can reduce retention — but first disable the retention check:
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM catalog_name.schema_name.my_table RETAIN 24 HOURS")
# Re-enable the check after the operation
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
```

#### SQL Example

```sql
-- DRY RUN — shows which files would be deleted, without deleting them
VACUUM catalog_name.schema_name.my_table DRY RUN;

-- Run VACUUM with explicit 7-day retention
VACUUM catalog_name.schema_name.my_table RETAIN 168 HOURS;

-- Check the table history to understand what versions remain after VACUUM
DESCRIBE HISTORY catalog_name.schema_name.my_table;
```

### Discussion and Concerns

- **VACUUM is irreversible:** Files deleted by VACUUM cannot be recovered. Always run `DRY RUN` first on production tables to confirm the set of files that will be deleted.
- **Do not reduce below 7 days without disabling the safety check:** Databricks enforces a minimum retention of 7 days by default to prevent accidental data loss. Reducing below this requires explicitly disabling `spark.databricks.delta.retentionDurationCheck.enabled`, which is a deliberate override.
- **VACUUM deletes time travel capability:** After VACUUM, you can no longer query versions of the table older than the retention window using `VERSION AS OF` or `TIMESTAMP AS OF`.
- **Schedule VACUUM separately from OPTIMIZE:** Running both in the same job is fine, but be aware that OPTIMIZE creates new file versions — run OPTIMIZE first, then VACUUM, so that the files compacted away by OPTIMIZE are also eligible for VACUUM cleanup.

### See Also

- [VACUUM — Databricks SQL Language Manual](https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html)
- [Delta Lake Time Travel — Databricks Documentation](https://docs.databricks.com/en/delta/history.html)

---

## Partitioning Strategy

Static partitioning divides a Delta table into directory trees based on column values. Queries that filter on the partition column can skip entire partition directories without reading any file metadata. For large tables filtered primarily on a date column, this is the most effective form of data skipping available.

### Problem

A large Delta table is queried with a `WHERE` clause on a date column but full table scans are occurring because the table is not partitioned. `DESCRIBE DETAIL` shows that no partition pruning is taking place, and the query plan shows a full scan of all files.

### Solution

Partition the table on a low-cardinality date column (e.g., `event_date` at day or month granularity). For existing tables, this requires rewriting the table with the partition applied.

#### Python Example

```python
# Write a new table with partitioning — use when creating the table for the first time
df.write \
    .format("delta") \
    .partitionBy("event_date") \
    .mode("overwrite") \
    .saveAsTable("catalog_name.schema_name.my_table")

# For an existing table that needs to be repartitioned, read and rewrite it
df_existing = spark.read.table("catalog_name.schema_name.my_table")
df_existing.write \
    .format("delta") \
    .partitionBy("event_date") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("catalog_name.schema_name.my_table_partitioned")

# Verify partition pruning on a filter query
spark.sql("""
    EXPLAIN FORMATTED
    SELECT * FROM catalog_name.schema_name.my_table
    WHERE event_date = '2026-03-01'
""").show(truncate=False)
# Look for "PartitionFilters" in the scan node to confirm pruning is active
```

#### SQL Example

```sql
-- Create a new partitioned table
CREATE TABLE IF NOT EXISTS catalog_name.schema_name.my_table (
    customer_id   STRING,
    event_type    STRING,
    amount        DOUBLE,
    event_date    DATE
)
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS catalog_name.schema_name.sat_customer_details (
    customer_hk     BINARY,
    hashdiff        BINARY,
    customer_name   STRING,
    email           STRING,
    load_date       TIMESTAMP,
    record_source   STRING,
    load_date_day   DATE  -- derived column: CAST(load_date AS DATE)
)
PARTITIONED BY (load_date_day);

-- Verify partition pruning is active
EXPLAIN FORMATTED
SELECT * FROM catalog_name.schema_name.my_table
WHERE event_date = '2026-03-01';
```

### Discussion and Concerns

- **Over-partitioning is worse than no partitioning:** Partitioning on a high-cardinality column (e.g., `customer_id`, `order_id`) creates millions of tiny partition directories. This inflates the Hive metastore, slows query planning, and produces millions of small files. Use ZORDER or Liquid Clustering for high-cardinality columns.
- **Partition columns should have 100–10,000 distinct values:** Day-level date columns typically fall in this range for multi-year tables. Month-level date columns are safer for very large tables.
- **Changing partition columns requires a full table rewrite:** Unlike Liquid Clustering, static partitioning cannot be changed with an ALTER TABLE. Plan the partition column carefully before creating a production table.
- **Liquid Clustering is incompatible with static partitioning:** Do not combine `PARTITIONED BY` and `CLUSTER BY` on the same table.

### See Also

- [Delta Lake Partitioning — Databricks Documentation](https://docs.databricks.com/en/delta/partitions.html)
- [Data Skipping for Delta Lake — Databricks Documentation](https://docs.databricks.com/en/delta/data-skipping.html)
- [Performance Tuning Architectural Patterns — Delta Lake Storage Optimization](./performance_cookbook.md)

---

## Adaptive Query Execution (AQE)

AQE re-optimises query plans at runtime using statistics collected during execution. It automatically handles common performance problems — shuffle partition skew, sub-optimal join strategies, and oversized or undersized shuffle partitions — without requiring manual query hints or configuration on most workloads.

### Problem

Spark jobs have shuffle partitions that are too large (causing out-of-memory errors or spill) or too small (causing excessive task scheduling overhead), or sort-merge joins that could be rewritten as broadcast joins at runtime. These problems are hard to predict at plan time because the optimizer must estimate data sizes before execution.

### Solution

Enable AQE (it is on by default on Databricks Runtime 7.3+) and tune the key thresholds for your workload. For most production clusters, verifying the default configuration and adjusting the broadcast join threshold is sufficient.

#### Python Example

```python
# Verify AQE is enabled on the current session
aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
print(f"AQE enabled: {aqe_enabled}")  # Should print: AQE enabled: true

# Adjust the broadcast join threshold — increase if large dimension tables
# are not being broadcast when they should be
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # 50 MB

# Adjust the skew partition threshold — lower to detect skew earlier
spark.conf.set(
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
    str(128 * 1024 * 1024)  # 128 MB
)

# Adjust the skew factor — a partition is skewed if it is N times larger than the median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")

# Run a query and observe AQE behaviour in the Spark UI SQL tab
# Look for "AQE" labels on plan nodes indicating runtime re-optimisation
result = spark.sql("""
    SELECT customer_id, SUM(amount) AS total
    FROM catalog_name.schema_name.my_table
    GROUP BY customer_id
""")
result.show(5)
```

#### SQL Example

```sql
-- Verify AQE is enabled for the current session
SET spark.sql.adaptive.enabled;

-- Set the skew join threshold (bytes) — 256 MB default
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456;

-- Set the broadcast join threshold (bytes) — 10 MB default, increase for larger dimensions
SET spark.sql.autoBroadcastJoinThreshold = 52428800;  -- 50 MB

-- Disable skew join optimisation for a specific query where AQE is splitting partitions
-- incorrectly (use only when you have confirmed the table is uniformly distributed)
SELECT /*+ SKEW_JOIN(orders) */
    o.customer_id,
    SUM(o.amount) AS total
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.customer_id;
```

### Discussion and Concerns

- **AQE is on by default on Databricks — but verify it has not been disabled:** Some older cluster configurations or job templates may have explicitly set `spark.sql.adaptive.enabled = false`. Check with `spark.conf.get("spark.sql.adaptive.enabled")`.
- **Monitor AQE behaviour in the Spark UI:** The SQL tab labels plan nodes with "AQE" when a runtime reoptimisation was applied. Use this to confirm that broadcast join conversion and skew handling are firing on your queries.
- **AQE and autoBroadcastJoinThreshold:** The default 10 MB threshold is conservative. If your dimension tables are 10–100 MB after filtering, increase the threshold so AQE can convert sort-merge joins to broadcast joins at runtime.
- **Skew join hints override AQE:** The `SKEW_JOIN` hint tells Spark which tables to watch for skew. It does not disable AQE globally — it is a per-table signal. Use it only when AQE's automatic detection is producing incorrect results for a known-uniform table.

### See Also

- [Adaptive Query Execution — Databricks Documentation](https://docs.databricks.com/en/optimizations/aqe.html)
- [AQE Configuration Reference — Apache Spark Documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- [Performance Tuning Architectural Patterns — AQE and Photon Scope](./performance_cookbook.md)

---

## Photon Engine

Photon is Databricks' vectorised, native C++ query engine that replaces the JVM Spark execution layer for supported operations. It provides the largest speedups on scan-heavy SQL workloads and Delta Lake reads and writes. Photon is always active on SQL Warehouses; on clusters, it requires selecting a Photon-enabled runtime at cluster creation.

### Problem

SQL queries and Delta operations are slower than expected and it is unclear whether Photon is active on the current compute. Alternatively, a Python UDF is a known performance bottleneck and the team wants to understand whether it can be moved to a Photon-eligible form.

### Solution

Verify that Photon is enabled on the current cluster, use Photon-eligible operations, and rewrite Python UDFs as pandas UDFs or SQL expressions to restore Photon coverage.

#### Python Example

```python
# Check whether Photon is enabled on the current cluster
photon_enabled = spark.conf.get("spark.databricks.photon.enabled")
print(f"Photon enabled: {photon_enabled}")

# Example: a Python scalar UDF bypasses Photon entirely
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# This UDF runs in the Python interpreter — Photon does not accelerate it
@udf(returnType=StringType())
def classify_amount_python_udf(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    else:
        return "low"

# Alternative 1: rewrite as a SQL expression — fully Photon-eligible
from pyspark.sql.functions import when, col

df_classified = df.withColumn(
    "amount_class",
    when(col("amount") > 1000, "high")
    .when(col("amount") > 100, "medium")
    .otherwise("low")
)

# Alternative 2: rewrite as a pandas UDF (vectorised) — partially Photon-eligible
# The vectorised execution reduces Python overhead significantly
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(StringType())
def classify_amount_pandas_udf(amount: pd.Series) -> pd.Series:
    return amount.apply(
        lambda x: "high" if x > 1000 else ("medium" if x > 100 else "low")
    )
```

#### SQL Example

```sql
-- This query is fully Photon-eligible: scan, filter, aggregation, sort
SELECT
    customer_id,
    event_type,
    COUNT(*)        AS event_count,
    SUM(amount)     AS total_amount,
    AVG(amount)     AS avg_amount
FROM catalog_name.schema_name.my_table
WHERE event_date >= '2026-01-01'
GROUP BY customer_id, event_type
ORDER BY total_amount DESC
LIMIT 100;

-- COPY INTO is Photon-eligible
COPY INTO catalog_name.schema_name.my_table
FROM 'abfss://container@storage.dfs.core.windows.net/raw/events/'
FILEFORMAT = PARQUET;

-- Check query history in the SQL Warehouse UI for the Photon indicator
-- Navigate to: SQL Editor -> Query History -> select a query -> check "Photon" label
```

### Discussion and Concerns

- **Photon must be selected at cluster creation time:** The Photon runtime label appears in the cluster creation UI as a suffix on the runtime name (e.g., "14.3 LTS (Scala 2.12, Spark 3.5.0) + Photon"). It cannot be enabled on a running cluster.
- **Python UDFs are the most common reason Photon does not deliver expected speedups:** If a pipeline has many Python UDFs, those operations execute in the Python interpreter and Photon does not apply. Rewriting UDFs as SQL `CASE/WHEN` expressions or built-in Spark SQL functions (`when()`, `regexp_replace()`, `date_format()`, etc.) makes them Photon-eligible.
- **Streaming stateful operations are not Photon-eligible:** If a Structured Streaming job uses `mapGroupsWithState`, `flatMapGroupsWithState`, or watermark-based aggregations, those specific operations run on the JVM engine. Stateless streaming operations (filter, projection, append-mode aggregation) are Photon-eligible.
- **SQL Warehouse query history shows a Photon indicator per query:** In the Databricks SQL Editor, navigate to Query History and select a query to see whether Photon was used and what fraction of the query plan ran on Photon.

### See Also

- [Photon Engine — Databricks Documentation](https://docs.databricks.com/en/compute/photon.html)
- [Supported Photon Operations — Databricks Documentation](https://docs.databricks.com/en/compute/photon.html#supported-operators)
- [Performance Tuning Architectural Patterns — AQE and Photon Scope](./performance_cookbook.md)

---

## Caching

When the same large Delta table or DataFrame is read multiple times in a notebook or pipeline — for example, once for a count validation and once for a transformation — Databricks re-reads the data from cloud storage on each pass unless it is cached. Caching stores the DataFrame in Spark memory so subsequent reads do not incur storage I/O.

### Problem

The same large Delta table is read multiple times in a notebook or pipeline, causing repeated storage I/O and slow overall execution. Profiling shows that the scan step is the bottleneck and the data volume is stable (not changing mid-execution).

### Solution

Cache the DataFrame or table in Spark memory before the repeated reads. Unpersist the cache when the data is no longer needed to free memory for other operations.

#### Python Example

```python
# Read and cache a DataFrame — cache is lazy (not materialised until an action is triggered)
df = spark.read.table("catalog_name.schema_name.my_table")
df.cache()

# Trigger materialisation with an action so subsequent reads hit the cache
row_count = df.count()
print(f"Cached {row_count} rows")

# Now perform multiple operations — each reads from cache, not storage
agg_result = df.groupBy("event_type").agg({"amount": "sum"})
filtered_result = df.filter(df.event_date >= "2026-01-01")

# Cache a table by name — useful for tables used across multiple notebook cells
spark.catalog.cacheTable("catalog_name.schema_name.my_table")

# Unpersist the DataFrame cache when done to free memory
df.unpersist()

# Uncache a table cached by name
spark.catalog.uncacheTable("catalog_name.schema_name.my_table")
```

#### SQL Example

```sql
-- Cache a table by name (lazy — materialised on first query after CACHE)
CACHE TABLE catalog_name.schema_name.my_table;

-- Trigger materialisation immediately with a SELECT COUNT(*)
SELECT COUNT(*) FROM catalog_name.schema_name.my_table;

-- Subsequent queries read from cache
SELECT event_type, SUM(amount) FROM catalog_name.schema_name.my_table GROUP BY event_type;

-- Uncache the table when done
UNCACHE TABLE catalog_name.schema_name.my_table;
```

### Discussion and Concerns

- **CACHE TABLE is lazy:** The table is not actually cached until an action (a query, a count, a show) is executed against it. To guarantee the cache is warm before dependent operations run, trigger it with `SELECT COUNT(*) FROM table` immediately after `CACHE TABLE`.
- **Cache is stored in Spark executor memory:** Large tables can evict other data from the cache (Spark uses an LRU eviction policy). If the cluster is memory-constrained, caching a very large table may hurt performance by evicting data needed by other operations.
- **Cache is lost when the cluster restarts:** Cached data is not persisted to storage. After a cluster restart, the cache must be re-warmed.
- **Do not cache tables that are frequently updated:** If a Delta table is being written to concurrently, the cached version quickly becomes stale. Cache is appropriate for stable reference tables or for data that is read-only within the scope of the current job run.

### See Also

- [Caching in Databricks — Databricks Documentation](https://docs.databricks.com/en/optimizations/disk-cache.html)
- [Spark Disk Cache vs. DataFrame Cache — Databricks Documentation](https://docs.databricks.com/en/optimizations/disk-cache.html#cache-types)

---

## Cluster Sizing and Autoscaling

Choosing the wrong cluster size is one of the most common sources of both poor performance and unnecessary cost on Databricks. An undersized cluster causes job failures from out-of-memory errors or unacceptably slow runtimes; an oversized cluster wastes DBUs.

### Problem

A Databricks job cluster is either too small (jobs fail with out-of-memory errors or take far longer than expected) or too large (the cluster is mostly idle and incurring unnecessary cost). There is no baseline configuration to reference.

### Solution

Right-size the cluster based on workload type. Use memory-optimised instances for aggregation-heavy jobs, compute-optimised instances for shuffle-heavy joins, and standard instances for general ETL. For interactive clusters, always set auto-termination.

#### Python Example

```python
# Example cluster configuration dict for a production ETL job cluster
# (used when creating a job via the Databricks Jobs API or SDK)
etl_cluster_config = {
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_E8ds_v4",   # Memory-optimised: 8 vCPUs, 64 GB RAM (Azure)
    "num_workers": 4,
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.databricks.delta.preview.enabled": "true"
    },
    "autotermination_minutes": 30
}

# Example cluster config for an interactive/development cluster
interactive_cluster_config = {
    "spark_version": "14.3.x-photon-scala2.12",  # Photon enabled
    "node_type_id": "Standard_DS3_v2",            # General purpose: 4 vCPUs, 14 GB RAM
    "autoscale": {
        "min_workers": 1,
        "max_workers": 4
    },
    "autotermination_minutes": 60  # Always set on interactive clusters
}

# For streaming jobs (including DLT), use a fixed-size cluster — autoscaling can cause instability
streaming_cluster_config = {
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_DS4_v2",
    "num_workers": 4,  # Fixed — not autoscaling
    "autotermination_minutes": 0  # No auto-termination for long-running streaming jobs
}
```

#### SQL Example

```sql
-- SQL Warehouses are sized using T-shirt sizes, not raw VM counts
-- Sizing guide for SQL Warehouse selection:
--   2X-Small (1 cluster):  Ad hoc queries, development, < 1 GB scans
--   X-Small  (1 cluster):  Small dashboards, < 10 GB scans
--   Small    (1 cluster):  Medium dashboards, Workflows SQL tasks, < 100 GB scans
--   Medium   (2 clusters): Production BI, concurrent users, < 1 TB scans
--   Large    (4 clusters): Heavy concurrent workloads, > 1 TB scans
--   X-Large  (8 clusters): Enterprise-scale concurrency

-- Check current warehouse configuration
SELECT id, name, size, state, auto_stop_mins
FROM system.information_schema.warehouses
WHERE name = 'my_production_warehouse';
```

### Discussion and Concerns

- **ETL clusters: choose instance type based on workload bottleneck.** Aggregation-heavy jobs (GROUP BY on large tables, many joins) benefit from memory-optimised instances (e.g., Azure `E`-series, AWS `r`-series). Shuffle-heavy jobs (many wide joins, sorts) benefit from compute-optimised instances with more vCPUs.
- **Autoscaling is beneficial for variable interactive workloads but adds latency:** When Spark needs additional nodes, they must be provisioned and join the cluster — this can take 1–3 minutes for standard instances. Jobs with strict SLAs may prefer fixed-size clusters.
- **SDP and streaming jobs should use fixed-size clusters:** Autoscaling during streaming or SDP execution can cause executors to be removed while they hold state, leading to stage retries and potential data loss. Use a fixed worker count sized for peak throughput.
- **Always set auto-termination on interactive clusters:** A single forgotten interactive cluster running overnight at 8 DBU/hour incurs ~64 DBUs of waste. Set `autotermination_minutes` to 30–60 minutes on all interactive clusters.
- **SQL Warehouse sizing:** Start with X-Small or Small for development, and scale up based on observed query times and concurrency. Serverless SQL Warehouses scale automatically — start with a smaller T-shirt size and let the serverless layer handle burst.

### See Also

- [Compute Configuration — Databricks Documentation](https://docs.databricks.com/en/compute/configure.html)
- [SQL Warehouse Sizing — Databricks Documentation](https://docs.databricks.com/en/compute/sql-warehouse/create.html)
- [Performance Tuning Architectural Patterns — Cluster vs. SQL Warehouse Selection](./performance_cookbook.md)

---

## Statistics and Predicate Pushdown

Delta Lake's file-skipping mechanism depends on per-file column statistics (min, max, null count) that are collected when data is written. If statistics are missing or stale, the query engine cannot skip files — even if the filter predicate would logically exclude most of the table. `ANALYZE TABLE` refreshes these statistics.

### Problem

Delta queries are not using file-level statistics for data skipping. `EXPLAIN` output shows that `PushedFilters` does not contain the WHERE clause columns, or that the number of files scanned equals the total file count despite a highly selective filter. Statistics may be missing for columns added after initial table creation, or stale after a large data load.

### Solution

Collect column statistics with `ANALYZE TABLE` and verify that predicate pushdown is active using `EXPLAIN`.

#### Python Example

```python
# Collect statistics for specific columns — faster than COMPUTE STATISTICS FOR ALL COLUMNS
spark.sql("""
    ANALYZE TABLE catalog_name.schema_name.my_table
    COMPUTE STATISTICS FOR COLUMNS customer_id, event_date
""")

# Collect statistics for all columns — use after initial load or large data changes
spark.sql("""
    ANALYZE TABLE catalog_name.schema_name.my_table
    COMPUTE STATISTICS FOR ALL COLUMNS
""")

# Verify predicate pushdown — run EXPLAIN and check for PushedFilters
explain_output = spark.sql("""
    EXPLAIN FORMATTED
    SELECT *
    FROM catalog_name.schema_name.my_table
    WHERE customer_id = 'C123'
      AND event_date >= '2026-01-01'
""").collect()

for row in explain_output:
    print(row[0])
# Look for "PushedFilters: [IsNotNull(customer_id), EqualTo(customer_id, C123), ...]"
# and "numFiles: <number>" — a lower number than total files confirms skipping
```

#### SQL Example

```sql
-- Collect statistics for specific high-selectivity columns
ANALYZE TABLE catalog_name.schema_name.my_table
COMPUTE STATISTICS FOR COLUMNS customer_id, event_date;

-- Collect statistics for all columns
ANALYZE TABLE catalog_name.schema_name.my_table
COMPUTE STATISTICS FOR ALL COLUMNS;

-- Verify predicate pushdown is active
EXPLAIN FORMATTED
SELECT *
FROM catalog_name.schema_name.my_table
WHERE customer_id = 'C123'
  AND event_date >= '2026-01-01';
-- In the output, look for:
-- "PushedFilters: [IsNotNull(customer_id), EqualTo(customer_id,C123), ...]"
-- "numFiles: N" where N is significantly less than total file count
```

### Discussion and Concerns

- **Delta collects statistics on the first 32 columns by default:** Columns beyond position 32 in the schema do not receive automatic statistics. If high-selectivity filter columns are beyond this position, re-order the schema (create a new table) or use `ANALYZE TABLE` explicitly.
- **Run ANALYZE after large data changes:** After a bulk insert, a `MERGE INTO`, or an OPTIMIZE run, statistics may be stale. Schedule `ANALYZE TABLE COMPUTE STATISTICS FOR COLUMNS` as part of your post-load pipeline step for the most critical filter columns.
- **Predicate pushdown is automatic for Delta:** Unlike some external table formats, Delta does not require any configuration to enable predicate pushdown. If `EXPLAIN` shows full file scans despite a selective filter, the cause is typically missing or stale statistics, not a disabled feature.
- **EXPLAIN output is verbose:** Focus on the `Scan` node in the EXPLAIN output. Look for `PushedFilters` containing your WHERE clause predicates and `numFiles` being less than the total file count. A `numFiles` equal to the total confirms that skipping is not occurring.

### See Also

- [ANALYZE TABLE — Databricks SQL Language Manual](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-analyze-table.html)
- [Delta Lake Data Skipping — Databricks Documentation](https://docs.databricks.com/en/delta/data-skipping.html)
- [Performance Tuning Architectural Patterns — Delta Lake Storage Optimization](./performance_cookbook.md)

---

## Native Incremental Loading with DeltaTable MERGE

When loading data incrementally into Delta tables — for marts, business vault models, or any large table that is too expensive to fully rebuild on every run — use `DeltaTable.merge()` (PySpark) or `MERGE INTO` (SQL) to apply only new or changed rows.

### Problem

A mart model or business vault table is rebuilt as a full `CREATE OR REPLACE TABLE AS SELECT` on every pipeline run, reprocessing gigabytes of data even when only a small fraction has changed since the last run. This extends pipeline runtime and increases compute cost.

### Solution

Replace the full rebuild with a `MERGE INTO` (upsert) operation using a watermark column (`updated_at`, `LOAD_DATE`) to identify new or changed rows. Apply only those rows to the target table.

#### Python Example — DeltaTable MERGE for incremental mart refresh

```python
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from datetime import datetime, timedelta

def incremental_refresh_dim_customer(spark):
    """
    Incrementally refresh dim_customer by merging only rows
    whose satellite LOAD_DATE is more recent than the current
    dimension record's mart_refreshed_at timestamp.
    """
    # Identify the last refresh watermark
    last_refresh = spark.sql(
        "SELECT MAX(mart_refreshed_at) AS last_refresh FROM main.marts.dim_customer"
    ).collect()[0]["last_refresh"]

    if last_refresh is None:
        # First run — full load
        last_refresh = datetime(2000, 1, 1)

    # Build incremental source: only customers with satellite changes since last refresh
    today = datetime.now().date()

    pit = (
        spark.table("main.business_vault.pit_customer")
        .filter(F.col("AS_OF_DATE") == F.lit(today))
    )
    hub = spark.table("main.raw_vault.hub_customer")
    sat_details = spark.table("main.raw_vault.sat_customer_details").filter(
        F.col("LOAD_DATE") > F.lit(last_refresh)
    )
    sat_marketing = spark.table("main.raw_vault.sat_customer_marketing")

    # Build the changed customers source DataFrame
    changed_customers = (
        sat_details.select("CUSTOMER_HK").distinct()
        .join(pit, on="CUSTOMER_HK", how="inner")
        .join(hub.select("CUSTOMER_HK", "CUSTOMER_ID"), on="CUSTOMER_HK")
        .join(
            sat_details.select("CUSTOMER_HK", "CUSTOMER_NAME", "EMAIL_ADDRESS",
                               "PHONE_NUMBER", "LOAD_DATE").alias("d"),
            on="CUSTOMER_HK"
        )
        .join(
            sat_marketing.select("CUSTOMER_HK", "MARKETING_SEGMENT",
                                 "OPTED_IN_EMAIL", "LOAD_DATE").alias("m"),
            on="CUSTOMER_HK", how="left"
        )
        .select(
            "CUSTOMER_ID", "CUSTOMER_HK",
            F.col("d.CUSTOMER_NAME").alias("customer_name"),
            F.col("d.EMAIL_ADDRESS").alias("email_address"),
            F.col("d.PHONE_NUMBER").alias("phone_number"),
            F.col("m.MARKETING_SEGMENT").alias("marketing_segment"),
            F.col("m.OPTED_IN_EMAIL").alias("opted_in_email"),
            F.current_timestamp().alias("mart_refreshed_at")
        )
    )

    # MERGE INTO dim_customer — update existing rows, insert new ones
    dim = DeltaTable.forName(spark, "main.marts.dim_customer")
    (
        dim.alias("t")
        .merge(
            changed_customers.alias("s"),
            "t.customer_id = s.CUSTOMER_ID"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"dim_customer incremental refresh complete — {changed_customers.count()} rows merged")
```

#### SQL Example — MERGE INTO for incremental mart refresh

```sql
-- Incremental MERGE INTO dim_customer.
-- Source: CTE that selects only customers with satellite changes since last refresh.
-- On match: update all columns. On no match: insert the new customer row.

MERGE INTO main.marts.dim_customer AS t
USING (
    WITH pit AS (
        SELECT CUSTOMER_HK, SAT_CUSTOMER_DETAILS_LDTS
        FROM main.business_vault.pit_customer
        WHERE AS_OF_DATE = CURRENT_DATE()
    ),
    changed_details AS (
        SELECT d.CUSTOMER_HK, d.CUSTOMER_NAME, d.EMAIL_ADDRESS, d.PHONE_NUMBER, d.LOAD_DATE
        FROM main.raw_vault.sat_customer_details d
        WHERE d.LOAD_DATE > (SELECT MAX(mart_refreshed_at) FROM main.marts.dim_customer)
    )
    SELECT
        h.CUSTOMER_ID,
        h.CUSTOMER_HK,
        d.CUSTOMER_NAME   AS customer_name,
        d.EMAIL_ADDRESS   AS email_address,
        d.PHONE_NUMBER    AS phone_number,
        CURRENT_TIMESTAMP() AS mart_refreshed_at
    FROM changed_details d
    INNER JOIN main.raw_vault.hub_customer h ON d.CUSTOMER_HK = h.CUSTOMER_HK
    INNER JOIN pit p ON d.CUSTOMER_HK = p.CUSTOMER_HK
) AS s
ON t.customer_id = s.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET
    t.customer_name      = s.customer_name,
    t.email_address      = s.email_address,
    t.phone_number       = s.phone_number,
    t.mart_refreshed_at  = s.mart_refreshed_at
WHEN NOT MATCHED THEN INSERT *;
```

**Functional difference between SQL and Python approaches:** The SQL `MERGE INTO` runs entirely on the SQL Warehouse using Spark SQL and is simpler to write. The Python `DeltaTable.merge()` is more flexible for complex watermark logic and can be embedded in SDP pipelines or Workflows Python tasks. Both produce the same Delta MERGE operation on disk. For standard incremental mart refreshes scheduled as Workflows SQL tasks, the SQL approach is preferred. For programmatic or parameterised pipelines, use the Python API.

#### Validation — SQL

```sql
-- Verify watermark advances correctly after each incremental run
SELECT MAX(mart_refreshed_at) AS last_refresh
FROM main.marts.dim_customer;
-- Should increase by approximately the pipeline interval on each run

-- Verify no duplicate customer_id entries were introduced
SELECT customer_id, COUNT(*) AS cnt
FROM main.marts.dim_customer
GROUP BY customer_id
HAVING cnt > 1;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Watermark column must be a reliable change indicator:** The `LOAD_DATE` or `updated_at` column used to identify changed rows must be set on every insert and update in the source system. If the source does not maintain a reliable watermark, incremental loading is not appropriate — use a full rebuild (`CREATE OR REPLACE TABLE`) instead.
- **`MERGE INTO` for incremental loads:** Use a natural business key (`CUSTOMER_ID` or similar) as the merge condition — this is the same key used to uniquely identify each record in the target table.
- **Delta MERGE with `whenNotMatchedInsertAll` and `whenMatchedUpdateAll` is append-upsert:** This is the correct strategy for dimension and derived mart tables. For append-only structures (Raw Vault hubs, links, satellites), use `whenNotMatchedInsertAll` only — no update clause.
- **Schedule OPTIMIZE after large incremental runs:** `MERGE INTO` produces many small files (one per matched row). After a large incremental refresh, run a scoped `OPTIMIZE` on the target table to compact the newly written files.

### See Also

- [Delta Lake MERGE INTO — Databricks Documentation](https://docs.databricks.com/en/delta/merge.html)
- [DeltaTable.merge() Python API — Delta Lake](https://docs.delta.io/latest/api/python/api/delta.tables.DeltaTable.html)

---

## Lakeflow Spark Declarative Pipelines Pipeline Performance

SDP pipelines have their own performance levers: pipeline mode (triggered vs. continuous), table materialisation (streaming table vs. materialized view), and expectations enforcement overhead. Tuning these correctly has a significant impact on pipeline latency and cost.

### Problem

An SDP pipeline is taking longer than expected, consuming excessive resources, or producing more small files than anticipated. It is unclear whether the bottleneck is the pipeline mode, the table type, or the expectations layer.

### Solution

Choose the correct pipeline mode and table type for each workload, and monitor pipeline performance using the SDP event log and Databricks Workflows UI.

#### Python Example — SDP table type selection

```python
import dlt
import pyspark.sql.functions as F


# Use @dlt.table for streaming tables — append-only, processes only new records
# Best for: append-only sources (raw vault, raw ingestion), low-latency requirements
@dlt.table(
    name="hub_customer_incremental",
    comment="Hub customer — streaming table, processes only new records"
)
def hub_customer_incremental():
    return (
        dlt.readStream("stg_customer")
        .select("CUSTOMER_HK", "CUSTOMER_ID", "LOAD_DATE", "RECORD_SOURCE")
        .distinct()
    )


# Use @dlt.table (non-streaming) for materialised views — full recompute each run
# Best for: business vault derived models, PIT tables, dimension tables
@dlt.table(
    name="bv_customer_derived",
    comment="Business vault derived customer — full recompute on each pipeline run"
)
def bv_customer_derived():
    sat = spark.table("main.raw_vault.sat_customer_details")
    hub = spark.table("main.raw_vault.hub_customer")
    window = Window.partitionBy("CUSTOMER_HK").orderBy(F.col("LOAD_DATE").desc())
    return (
        sat.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .join(hub.select("CUSTOMER_HK", "CUSTOMER_ID"), on="CUSTOMER_HK")
    )
```

#### Python Example — SDP pipeline mode configuration

```python
# Configure pipeline mode in databricks.yml (Databricks Asset Bundle)
# Triggered mode: runs once on schedule, shuts down when complete
# Cost-optimised for batch workloads

# bundles/pipeline.yml:
# resources:
#   pipelines:
#     vault_pipeline:
#       name: "Processing Pipeline"
#       continuous: false  # triggered mode — runs on Workflows schedule
#       photon: true
#       channel: "CURRENT"
#       clusters:
#         - label: default
#           num_workers: 4
#           node_type_id: "Standard_E8ds_v4"

# For continuous mode (low-latency streaming):
# continuous: true
# (pipeline runs continuously, processes records as they arrive)
# Use for: real-time vault loading from Kafka/Event Hubs sources
```

#### SQL Example — Monitor SDP pipeline performance

```sql
-- Query the SDP event log for pipeline run duration and table statistics
SELECT
    origin.flow_name,
    timestamp,
    event_type,
    details:flow_progress:metrics:num_output_rows    AS output_rows,
    details:flow_progress:metrics:num_input_rows     AS input_rows
FROM event_log('<pipeline_id>')
WHERE event_type = 'flow_progress'
  AND details:flow_progress:status = 'COMPLETED'
ORDER BY timestamp DESC
LIMIT 50;

-- Check for expectation violations
SELECT
    origin.flow_name,
    details:flow_progress:data_quality:expectations[0]:name AS expectation_name,
    details:flow_progress:data_quality:dropped_records AS dropped,
    details:flow_progress:data_quality:passed_records  AS passed
FROM event_log('<pipeline_id>')
WHERE details:flow_progress:data_quality IS NOT NULL
ORDER BY timestamp DESC;
```

### Discussion and Concerns

- **Triggered mode is cheaper for batch pipelines:** Triggered SDP pipelines start a cluster, run the pipeline to completion, and terminate the cluster. Continuous mode keeps the cluster running indefinitely. For nightly batch loads, use triggered mode.
- **Streaming tables produce many small files:** Each micro-batch in a streaming SDP table writes a new set of small files. Run a scoped `OPTIMIZE WHERE load_date >= current_date() - 1` as a post-pipeline Workflows task to compact the newly written files.
- **Materialised views in SDP are full-recompute:** SDP `@dlt.table` without `readStream()` is a materialised view that is fully recomputed on each pipeline run. For large business vault models (PIT tables, bridge tables), consider building them outside SDP as Workflows SQL tasks to avoid the SDP overhead.
- **Expectations (`@dlt.expect_*`) add processing overhead:** Each expectation adds a filter-and-count step to the pipeline execution. For very high-throughput pipelines, limit expectations to critical columns only. Use Databricks Lakehouse Monitoring for broader data quality monitoring outside the pipeline.

### See Also

- [Lakeflow Spark Declarative Pipelines documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [SDP pipeline modes — triggered vs. continuous](https://docs.databricks.com/en/delta-live-tables/pipeline-mode.html)
- [SDP event log](https://docs.databricks.com/en/delta-live-tables/observability.html)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Slow query alerts | SQL Warehouse Query History (Databricks SQL Editor) | Queries exceeding defined duration thresholds; full-scan indicators on Delta tables |
| Job duration trends | Databricks Workflows UI / `system.lakeflow.job_run_timeline` | Increasing run times over time indicating file growth or stats staleness |
| OPTIMIZE run time | Databricks Workflows UI (dedicated OPTIMIZE job) | OPTIMIZE taking longer than the maintenance window — signal to switch to scoped OPTIMIZE or Liquid Clustering |
| Small file count | `DESCRIBE DETAIL catalog.schema.table` | `numFiles` high and `avgFileSize` below 128 MB — OPTIMIZE is overdue |
| Photon query ratio | Cluster Metrics / SQL Warehouse query history | Low Photon coverage indicating Python UDFs or non-eligible operations dominating |
| AQE plan changes | Spark UI SQL tab | "AQE" labels on plan nodes — confirm skew handling and broadcast join conversion are firing |
| Delta time travel versions | `DESCRIBE HISTORY catalog.schema.table` | Excessive version accumulation — VACUUM may be overdue |
| SDP pipeline duration | SDP event log / Workflows UI | Increasing pipeline run times — may indicate small file accumulation or growing table scans |
| Incremental MERGE performance | Workflows job run history — mart refresh task | MERGE taking as long as a full rebuild — watermark column may not be filtering correctly |

### Metrics for Success

- [ ] OPTIMIZE runs complete within the defined maintenance window (e.g., under 30 minutes for the daily compaction job)
- [ ] No production query exceeds the defined SLA after optimisation (e.g., no dashboard query takes longer than 10 seconds)
- [ ] `DESCRIBE DETAIL` on all production Delta tables shows `avgFileSize` between 128 MB and 1 GB
- [ ] AQE is confirmed enabled on all production clusters (`spark.sql.adaptive.enabled = true`)
- [ ] Incremental MERGE jobs process only changed rows — run times are proportional to the change volume, not the full table size
- [ ] VACUUM runs weekly on all production tables; no table retains unreferenced files older than 14 days
- [ ] Photon is confirmed active on all SQL Warehouse queries (Photon indicator in query history)
- [ ] SDP pipelines in triggered mode complete and terminate within the scheduled batch window
