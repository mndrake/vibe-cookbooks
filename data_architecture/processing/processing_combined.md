# Processing, Summarizing, and Transformation Guide

## Databricks

> **Scope note:** All patterns are implemented with PySpark, Spark SQL, Delta Lake, Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT), and Databricks Jobs. This guide is self-contained — no companion document is required.

---

## Introduction

This guide provides architectural decision guidance and practical, step-by-step implementation for **processing, summarising, and transforming data** on Databricks using only native Databricks tooling. It covers Delta Lake transformations (MERGE, UPDATE, DELETE), aggregations and window functions, data cleansing and deduplication, Slowly Changing Dimension (SCD) patterns (implemented with Delta MERGE INTO and SDP APPLY CHANGES INTO), incremental load patterns using Databricks Jobs and SDP, reusable transformation logic using native Python functions and Spark SQL UDFs, date spine generation using `sequence()` and `explode()`, and pipeline orchestration using Databricks Jobs and Databricks Asset Bundles.

Each method section follows a consistent structure: the problem being solved, the recommended solution with Python and SQL examples, any known concerns or trade-offs, and links to further reading.

---

## Design Decisions

Use this section to select the right architecture and tooling before diving into the implementation sections.

### Medallion Layer Responsibilities

| Layer | Purpose | Key Rules |
|-------|---------|-----------|
| **Bronze** | Raw ingestion — stores data exactly as received | Append-only. No business logic. Add ingestion metadata only (`_ingestion_timestamp`, `_source_file`). Schema is inferred or schema-on-read. |
| **Silver** | Cleansed and conformed — trusted, typed, stable | Apply type casting, deduplication, null handling, schema enforcement. SCD Type 2 patterns belong here. No consumer-facing KPIs. |
| **Gold** | Aggregated and business-ready | KPI definitions, fiscal calendar adjustments, denormalised fact/dimension tables. Directly consumed by dashboards, ML feature stores, and APIs. Multiple Gold schemas may exist per business domain. |

**Cardinal rules:** Never put business logic in Bronze. Never collapse Bronze and Gold. For small teams or single-source pipelines with already-clean data, the Bronze–Silver boundary may be collapsed.

For complex pipelines, an optional **Quarantine** layer (between Bronze and Silver) captures records that fail quality checks, enabling alerting and reprocessing without polluting Silver. An optional **Enriched Silver** layer reduces Gold complexity when many joins and lookups are required across multiple Silver sources.

### Orchestration: Databricks Jobs vs. Lakeflow Spark Declarative Pipelines (SDP)

| Criterion | Databricks Jobs | SDP |
|-----------|-----------------|-----|
| Built-in data quality enforcement | Manual (notebook assertions) | Native (`EXPECT`, `EXPECT OR DROP`, `EXPECT OR FAIL`) |
| SCD Type 2 / CDC handling | Manual two-pass MERGE | `APPLY CHANGES INTO` (automatic) |
| Pipeline lineage and quality metrics | Manual | Built-in |
| Mixed batch + streaming sources in same DAG | Requires separate tasks | Native |
| Cost overhead | Standard DBU | DBU premium for managed platform services |
| **Best for** | Simple to medium pipelines; cost-sensitive workloads; teams with strong Python skills | Long-lived pipelines requiring quality enforcement, CDC, and built-in observability |

### Transformation Tool: PySpark vs. Spark SQL vs. SDP

| Criterion | PySpark | Spark SQL | SDP |
|-----------|---------|-----------|-----|
| Complex procedural logic | Best | Poor | Limited |
| Streaming pipelines | Best | Limited | Best (declarative) |
| ML feature engineering | Best | Poor | Poor |
| Simple analytical transformations | Good | Best | Good |
| Ad hoc exploration | Good | Best | Poor |
| Data quality enforcement | Manual | Manual | Best (native EXPECT rules) |
| SCD / CDC handling | Manual two-pass MERGE | Manual two-pass MERGE | Best (APPLY CHANGES INTO) |
| Pipeline lineage and metrics | Manual | Manual | Built-in |
| Analyst-accessible SQL | Poor | Best | Good |

Most mature pipelines use more than one tool. A common pattern: PySpark for Bronze ingestion and complex joins, Spark SQL for Silver cleansing logic that analysts must review, SDP when CDC or quality rules are required.

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

> **On Databricks (interactive notebooks or Asset Bundle jobs):** PySpark, Delta Lake, and Lakeflow Spark Declarative Pipelines (SDP) are pre-installed with every Databricks Runtime. No `pip install` is needed to run the code examples in this cookbook on a cluster.
>
> **Local development:** The tools below are installed on your local machine for CLI operations and Asset Bundle deployment.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.9+ | Local dev | Required for the Databricks CLI and local PySpark unit tests |
| PySpark | Provided by Databricks Runtime | Local dev only | Bundled with Databricks Runtime — `pip install pyspark` only for local unit testing |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | Latest (v0.200+) | Local dev | Used for workspace interaction, secrets management, and deploying Asset Bundles |
| [Databricks Asset Bundles (DAB)](https://docs.databricks.com/en/dev-tools/bundles/index.html) | Bundled with Databricks CLI v0.200+ | Local dev | Used to define and deploy Jobs, SDP pipelines, and permissions as code |

Install Python dependencies (local machine only — not needed on Databricks clusters):

```bash
pip install databricks-cli
```

Authenticate the Databricks CLI:

```bash
# Interactive token-based authentication
databricks configure --token

# Verify connection
databricks clusters list
```

### Setting Up a New Project with Databricks Asset Bundles

To scaffold a new native Databricks project:

```bash
# Initialise a new bundle from the default template
databricks bundle init

# Or use a specific template
databricks bundle init --template default-python
```

This creates a `databricks.yml` bundle manifest, a `resources/` folder for Job and SDP pipeline definitions, and a `src/` folder for notebook and Python source code.

Validate and deploy the bundle:

```bash
# Validate the bundle configuration (dry run)
databricks bundle validate

# Deploy to your target environment (dev, staging, prod)
databricks bundle deploy --target dev

# Run a specific job defined in the bundle
databricks bundle run --target dev my_processing_job
```

### Getting the Source for an Existing Project

```bash
git clone <repository_url>
cd <project_directory>

# Install Python dependencies
pip install -r requirements.txt

# Authenticate the CLI
databricks configure --token

# Validate the bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy --target dev
```

Review `databricks.yml` for workspace host, cluster configuration, job definitions, and SDP pipeline references before deploying. Check `resources/` for Job task DAGs and SDP pipeline YAML definitions.

---

## Infrastructure Pre-Requisites

### Infrastructure Required

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment for all PySpark and SQL examples | Unity Catalog must be enabled for catalog-qualified table references |
| Spark Cluster | Required for PySpark examples and SDP pipelines | Databricks Runtime 12.2 LTS or later recommended for deletion vector support |
| Databricks SQL Warehouse | Required for Spark SQL examples in SQL Editor and notebook SQL cells | Serverless or Pro tier; Standard tier does not support `CREATE TABLE AS SELECT` with Unity Catalog in all regions |
| Unity Catalog | Target for all output Delta tables | Requires `CREATE TABLE` privilege on the target schema |
| Delta tables for source data | Input to all transformation examples | Source tables should exist in a Bronze or Silver schema before running examples |

### Enabling Change Data Feed on Delta Tables

Several patterns in this cookbook benefit from Change Data Feed (CDF), which exposes row-level changes (insert, update, delete) as a readable stream. Enable CDF on a table with:

```sql
ALTER TABLE catalog.schema.my_table
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

CDF is required for efficient CDC-based MERGE patterns where only changed rows from the source need to be processed, and is the recommended source format for SDP `APPLY CHANGES INTO` pipelines.

---

## Delta Lake Transformations — MERGE INTO

The MERGE INTO statement (also called an upsert) applies inserts, updates, and deletes to a Delta table in a single atomic operation. It is the primary mechanism for writing incremental data to Silver and Gold Delta tables from a staged or streaming source.

### Problem

A Silver `orders` table contains the current state of orders. New and updated order records arrive in a staged source table every hour. Records already present in `orders` should be updated with the latest values; records not yet present should be inserted. A full reload of the table on every run is too slow and expensive at the expected data volumes.

### Solution

Use `MERGE INTO` to apply only the changed rows. The merge key is the business key that uniquely identifies a record across both the source and the target.

#### Python Example

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Reference the target Delta table by its Unity Catalog name
target_table = DeltaTable.forName(spark, "catalog.silver.orders")

# Load the staged source data — typically from a Bronze Delta table
# filtered to the current micro-batch or processing window
source_df = spark.table("catalog.bronze.orders_staged")

# Perform the merge: update existing records, insert new ones
(
    target_table.alias("target")
    .merge(
        source_df.alias("source"),
        condition="target.order_id = source.order_id"
    )
    .whenMatchedUpdateAll()        # Update all columns when the key matches
    .whenNotMatchedInsertAll()     # Insert the full row when the key is new
    .execute()
)
```

#### SQL Example

```sql
-- Upsert staged orders into the Silver orders table
MERGE INTO catalog.silver.orders AS target
USING catalog.bronze.orders_staged AS source
  ON target.order_id = source.order_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;
```

To merge only a subset of columns rather than all (`*`), replace `UPDATE SET *` with explicit column assignments:

```sql
WHEN MATCHED THEN
  UPDATE SET
    target.status        = source.status,
    target.updated_at    = source.updated_at,
    target.total_amount  = source.total_amount
```

### Discussion and Concerns

- **Delta versioning:** Each MERGE generates a new Delta table version. `DESCRIBE HISTORY catalog.silver.orders` will record each merge as a separate version, which is useful for auditing but means version history grows with every run.
- **Full table scan risk:** Without data skipping, MERGE performs a full scan of the target table to find matching rows. Apply `OPTIMIZE` with `ZORDER BY order_id` on the target table so that Delta's data skipping can narrow the scan to relevant files. This can reduce MERGE execution time by an order of magnitude on large tables.
- **Streaming incompatibility:** MERGE is not supported directly on a streaming Delta table. For streaming MERGE patterns, use `foreachBatch` to apply a batch MERGE within each micro-batch. See the [Structured Streaming ingestion cookbook](../ingestion/) for this pattern.
- **`UPDATE SET *` semantics:** `UPDATE SET *` requires that the source and target have identical column names. If the source has additional columns not present in the target, `UPDATE SET *` will fail. Explicitly list columns or use schema evolution (`ALTER TABLE ... ADD COLUMN`) to align schemas before the merge.

### See Also

- [Databricks MERGE INTO documentation](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html)
- [Delta Lake upsert tutorial](https://docs.databricks.com/en/delta/merge.html)
- [DeltaTable Python API — Delta Lake](https://docs.delta.io/latest/api/python/api/delta.tables.DeltaTable.html)
- [Processing Architectural Patterns (Native) — Medallion layer responsibilities](./processing_patterns.md)

---

## Delta Lake Transformations — UPDATE and DELETE

Targeted UPDATE and DELETE operations allow individual records or groups of records to be corrected or removed from a Delta table without reloading the entire table. This is common in SCD Type 1 corrections, GDPR right-to-erasure requests, and late-arriving data corrections.

### Problem

A Silver `customers` table contains records with an incorrect `country_code` value due to a source system bug affecting a specific date range. The correction has been identified: all records where `country_code = 'XX'` and `created_at BETWEEN '2024-01-01' AND '2024-03-31'` should have `country_code` set to `'GB'`. Additionally, a set of test records inserted during development must be permanently deleted.

### Solution

Use `DeltaTable.update()` and `DeltaTable.delete()` in Python, or `UPDATE` and `DELETE FROM` in SQL, with a predicate that targets only the affected rows.

#### Python Example

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

customers = DeltaTable.forName(spark, "catalog.silver.customers")

# Correct the country code for affected records
customers.update(
    condition=(col("country_code") == "XX") &
              (col("created_at") >= "2024-01-01") &
              (col("created_at") <= "2024-03-31"),
    set={"country_code": lit("GB")}
)

# Delete test records inserted during development
customers.delete(
    condition=col("customer_id").startswith("TEST-")
)
```

#### SQL Example

```sql
-- Correct country code for affected records
UPDATE catalog.silver.customers
SET country_code = 'GB'
WHERE country_code = 'XX'
  AND created_at BETWEEN '2024-01-01' AND '2024-03-31';

-- Delete test records
DELETE FROM catalog.silver.customers
WHERE customer_id LIKE 'TEST-%';
```

To verify the changes before committing, use a `SELECT` with the same predicate first:

```sql
SELECT COUNT(*), country_code
FROM catalog.silver.customers
WHERE country_code = 'XX'
  AND created_at BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY country_code;
```

### Discussion and Concerns

- **Delta versioning:** Each UPDATE and DELETE generates a new Delta version. The original data remains accessible via time travel (`SELECT * FROM catalog.silver.customers VERSION AS OF <n>`) for the duration of the Delta log retention period (default 30 days).
- **Deletion vectors (Databricks Runtime 12.2+):** When deletion vectors are enabled on a table, DELETE operations mark rows as deleted in a separate deletion vector file rather than immediately rewriting affected Parquet files. This makes DELETEs dramatically faster for small deletes on large files. Enable with: `ALTER TABLE catalog.silver.customers SET TBLPROPERTIES (delta.enableDeletionVectors = true);`
- **File proliferation from heavy deletes:** Large DELETE operations without deletion vectors rewrite affected files, potentially creating many small files. Follow heavy deletes with `OPTIMIZE catalog.silver.customers` to compact files and restore query performance.
- **GDPR/right-to-erasure:** For compliance deletions, verify that the Delta log retention and time travel window are configured appropriately. The deleted data remains in the underlying Parquet files until `VACUUM` is run. Use `VACUUM catalog.silver.customers RETAIN 0 HOURS` (requires `spark.databricks.delta.retentionDurationCheck.enabled = false`) only after confirming no downstream processes depend on time travel for the affected table.

### See Also

- [Delta Lake UPDATE documentation](https://docs.databricks.com/en/sql/language-manual/delta-update.html)
- [Delta Lake DELETE documentation](https://docs.databricks.com/en/sql/language-manual/delta-delete-from.html)
- [Deletion vectors in Delta Lake](https://docs.databricks.com/en/delta/deletion-vectors.html)
- [Table deletes, updates, and merges — Delta Lake](https://docs.delta.io/latest/delta-update.html)
- [DeltaTable Python API — Delta Lake](https://docs.delta.io/latest/api/python/api/delta.tables.DeltaTable.html)

---

## Aggregations and Summarization

Aggregation and summarisation transforms raw or cleansed data into summary metrics for reporting, dashboarding, and analytical consumption. The Gold layer in Medallion architecture is primarily composed of aggregated tables built from Silver sources.

### Problem

A Silver `order_line_items` table contains one row per order line, with columns for `order_id`, `customer_id`, `product_id`, `region`, `order_date`, `quantity`, and `unit_price`. The Gold layer needs: (1) total revenue and order count by region and month, (2) a running total of revenue per customer over time, and (3) a ROLLUP for subtotals at region and national level.

### Solution

Use DataFrame aggregations with `groupBy().agg()` for standard aggregations, `Window` functions for running totals, and SQL `GROUP BY ROLLUP` for hierarchical subtotals.

#### Python Example

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

df = spark.table("catalog.silver.order_line_items") \
    .withColumn("revenue", F.col("quantity") * F.col("unit_price")) \
    .withColumn("order_month", F.date_trunc("month", F.col("order_date")))

# --- Standard aggregation: revenue and order count by region and month ---
region_monthly = df.groupBy("region", "order_month").agg(
    F.sum("revenue").alias("total_revenue"),
    F.countDistinct("order_id").alias("order_count"),
    F.avg("revenue").alias("avg_line_revenue")
)

region_monthly.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("catalog.gold.revenue_by_region_month")

# --- Window function: running total of revenue per customer ---
customer_window = Window.partitionBy("customer_id") \
    .orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_with_running_total = df.withColumn(
    "running_revenue",
    F.sum("revenue").over(customer_window)
)
```

#### SQL Example

```sql
-- Standard aggregation: revenue and order count by region and month
SELECT
    region,
    DATE_TRUNC('month', order_date)          AS order_month,
    SUM(quantity * unit_price)               AS total_revenue,
    COUNT(DISTINCT order_id)                 AS order_count,
    AVG(quantity * unit_price)               AS avg_line_revenue
FROM catalog.silver.order_line_items
GROUP BY region, DATE_TRUNC('month', order_date)
ORDER BY region, order_month;

-- ROLLUP: subtotals at region level and a grand total
SELECT
    region,
    DATE_TRUNC('month', order_date) AS order_month,
    SUM(quantity * unit_price)      AS total_revenue
FROM catalog.silver.order_line_items
GROUP BY ROLLUP (region, DATE_TRUNC('month', order_date))
ORDER BY region NULLS LAST, order_month NULLS LAST;

-- Window function: running total per customer
SELECT
    customer_id,
    order_date,
    SUM(quantity * unit_price) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM catalog.silver.order_line_items;
```

### Discussion and Concerns

- **ROLLUP vs. CUBE vs. GROUPING SETS:**
  - `ROLLUP(a, b)` produces subtotals for `(a, b)`, `(a)`, and `()` (grand total) — useful for hierarchical dimensions like region > month.
  - `CUBE(a, b)` produces all combinations: `(a, b)`, `(a)`, `(b)`, `()` — useful when all cross-dimension subtotals are needed.
  - `GROUPING SETS((a, b), (a), ())` gives explicit control over which combinations to compute — preferred when only specific subtotal levels are needed.
- **Window function memory:** Window functions with large partitions can cause memory pressure on executors. Filter the input DataFrame to the smallest necessary window before applying the function, and consider whether the window can be replaced with a pre-aggregated join.
- **Python vs. SQL functional difference:** `F.countDistinct()` in PySpark and `COUNT(DISTINCT ...)` in SQL are equivalent. However, `approx_count_distinct()` / `APPROX_COUNT_DISTINCT()` can be significantly faster for very high cardinality counts at the cost of a small estimation error.

### See Also

- [Databricks SQL window functions reference](https://docs.databricks.com/en/sql/language-manual/sql-ref-window-functions.html)
- [PySpark Window API documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [Databricks SQL ROLLUP, CUBE, GROUPING SETS](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-groupby.html)

---

## Data Cleansing and Deduplication

Cleansing and deduplication transforms raw Bronze records into the trusted, typed, deduplicated records that define the Silver layer.

### Problem

A Bronze table `catalog.bronze.raw_customers` contains customer records ingested from a CSV file. The data has several quality problems: the `email` column contains nulls and empty strings; `phone_number` includes leading and trailing whitespace; `signup_date` is stored as a string in mixed formats; and duplicate `customer_id` values exist because the source system sent the same record in multiple batches, with the latest record being the most recent by `_ingestion_timestamp`.

### Solution

Build a cleansing pipeline that addresses each quality dimension in sequence, then write the result to `catalog.silver.customers`.

#### Python Example

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

raw = spark.table("catalog.bronze.raw_customers")

# Step 1: Cast and standardise types
typed = raw \
    .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd")) \
    .withColumn("customer_id", F.col("customer_id").cast("string")) \
    .withColumn("phone_number", F.trim(F.col("phone_number")))

# Step 2: Handle nulls and empty strings
cleaned = typed \
    .withColumn("email",
        F.when(
            (F.col("email").isNull()) | (F.col("email") == ""),
            F.lit(None)
        ).otherwise(F.lower(F.trim(F.col("email"))))
    ) \
    .dropna(subset=["customer_id"])  # Drop records with no business key

# Step 3: Deduplicate — keep the latest record per customer_id
dedup_window = Window.partitionBy("customer_id") \
    .orderBy(F.col("_ingestion_timestamp").desc())

deduped = cleaned \
    .withColumn("_row_num", F.row_number().over(dedup_window)) \
    .filter(F.col("_row_num") == 1) \
    .drop("_row_num")

# Step 4: Write to Silver
deduped.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("catalog.silver.customers")
```

#### SQL Example

```sql
-- Cleanse and deduplicate raw customers into Silver
-- Uses ROW_NUMBER() to retain only the latest record per customer_id

CREATE OR REPLACE TABLE catalog.silver.customers AS
WITH typed AS (
    SELECT
        CAST(customer_id AS STRING)                             AS customer_id,
        TRIM(phone_number)                                      AS phone_number,
        TO_DATE(signup_date, 'yyyy-MM-dd')                      AS signup_date,
        -- Normalise email: lowercase, trim, null out empty strings
        CASE
            WHEN email IS NULL OR TRIM(email) = '' THEN NULL
            ELSE LOWER(TRIM(email))
        END                                                     AS email,
        _ingestion_timestamp
    FROM catalog.bronze.raw_customers
    WHERE customer_id IS NOT NULL  -- Drop records with no business key
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _ingestion_timestamp DESC
        ) AS row_num
    FROM typed
)
SELECT
    customer_id,
    phone_number,
    signup_date,
    email
FROM ranked
WHERE row_num = 1;
```

### Discussion and Concerns

- **Tiebreaking on equal timestamps:** Both `dropDuplicates()` and `ROW_NUMBER()` require a deterministic ordering column to choose between duplicates with equal values. If `_ingestion_timestamp` has second-level granularity and multiple duplicates arrive in the same second, the choice of which row to keep is arbitrary. Use a composite sort key (e.g., `_ingestion_timestamp DESC, _source_file_offset DESC`) for deterministic deduplication.
- **`dropDuplicates()` vs. `ROW_NUMBER()`:** `dropDuplicates(["customer_id"])` in PySpark keeps an arbitrary row when timestamps are equal — it does not respect ordering. Always use `ROW_NUMBER()` via a Window function when the choice of which duplicate to retain is meaningful.
- **Streaming deduplication:** In a Structured Streaming pipeline, use `dropDuplicates(["customer_id"], watermark_column)` with a watermark to bound the state store. Without a watermark, the deduplication state store grows without bound.
- **Python vs. SQL difference:** The PySpark pipeline is multi-step and easy to unit test step-by-step. The SQL CTE approach compresses the logic into a single statement, which is more concise but harder to debug incrementally. For production pipelines with complex cleansing rules, the PySpark approach is generally preferred for its testability.

### See Also

- [PySpark DataFrame cleansing functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Databricks SQL COALESCE, CAST, TRIM](https://docs.databricks.com/en/sql/language-manual/functions/coalesce.html)
- [Structured Streaming deduplication](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-deduplication)

---

## Slowly Changing Dimensions — SCD Type 1

SCD Type 1 is the simplest history strategy: when a dimension record changes, overwrite the existing row with the new values. No history is retained.

### Problem

A `dim_customer` dimension table in the Gold layer stores customer attributes used in BI reports. The source system sends a full extract of current customer records each day. If a customer's `email` or `region` has changed, the dimension should reflect the latest value. No historical tracking of previous values is required.

### Solution

Use a Delta MERGE to overwrite existing records with the latest source values and insert any new customers that do not yet exist in the dimension.

#### Python Example

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load the daily full extract from Silver
source_df = spark.table("catalog.silver.customers")

# Reference the Gold dimension table
dim_customer = DeltaTable.forName(spark, "catalog.gold.dim_customer")

# SCD Type 1: overwrite matching records, insert new ones
(
    dim_customer.alias("target")
    .merge(
        source_df.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

#### SQL Example

```sql
-- SCD Type 1: overwrite dim_customer with the latest customer attributes
MERGE INTO catalog.gold.dim_customer AS target
USING catalog.silver.customers AS source
  ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    target.email      = source.email,
    target.region     = source.region,
    target.phone      = source.phone,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, region, phone, created_at, updated_at)
  VALUES (source.customer_id, source.email, source.region,
          source.phone, source.created_at, source.updated_at);
```

### Discussion and Concerns

- **When SCD Type 1 is appropriate:** SCD Type 1 is the right choice when the previous value has no analytical meaning once it is corrected. It is not appropriate when historical reporting needs to reflect what value was in place at the time of a transaction — use SCD Type 2 for that requirement.
- **Impact on historical facts:** Overwriting a dimension record with SCD Type 1 retroactively changes the value for all historical fact records that join to it. Discuss with business stakeholders whether a given attribute change should be corrected (SCD1) or tracked (SCD2).
- **No surrogate key required:** Because SCD Type 1 maintains one row per business key, the business key (`customer_id`) is sufficient as the dimension key for fact table joins.

### See Also

- [Databricks SCD documentation](https://learn.microsoft.com/en-us/azure/databricks/delta/merge)
- [Table deletes, updates, and merges — Delta Lake](https://docs.delta.io/latest/delta-update.html)
- [Processing Architectural Patterns (Native) — SCD vs. Satellite design](./processing_patterns.md)
- [SCD Type 2 method — next section in this cookbook](#slowly-changing-dimensions--scd-type-2-native-delta-merge)

---

## Slowly Changing Dimensions — SCD Type 2 (Native Delta MERGE)

SCD Type 2 preserves the full history of dimension changes by adding a new row for each change, tracking the effective date range and a flag indicating the currently active version. This allows fact tables to join to the dimension version that was active at the time of the transaction.

The native Databricks implementation uses a two-pass Delta MERGE pattern. For pipelines already using Lakeflow Spark Declarative Pipelines, see [SCD Type 2 via SDP APPLY CHANGES INTO](#slowly-changing-dimensions--scd-type-2-via-sdp-apply-changes-into) below.

### Problem

A `dim_customer` dimension tracks customer segment assignments (`segment`: Bronze, Silver, Gold). Customers are re-segmented periodically. Sales reports must accurately reflect the segment each customer belonged to at the time of their purchase — not their current segment. Each time a customer's segment changes, the previous version must be closed and a new version inserted.

### Solution

Use a two-pass Delta MERGE: the first pass expires changed records; the second pass inserts new current versions.

#### Python Example

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Load today's snapshot of current customer segments from Silver
source_df = spark.table("catalog.silver.customers") \
    .withColumn("effective_from", F.current_date()) \
    .withColumn("effective_to", F.lit("9999-12-31").cast("date")) \
    .withColumn("is_current", F.lit(True))

dim_customer = DeltaTable.forName(spark, "catalog.silver.dim_customer")

# Pass 1: Expire records for customers whose tracked attributes have changed
(
    dim_customer.alias("target")
    .merge(
        source_df.alias("source"),
        """target.customer_id = source.customer_id
           AND target.is_current = true
           AND (target.name != source.name OR target.address != source.address)"""
    )
    .whenMatchedUpdate(set={
        "effective_to": "current_date()",
        "is_current":   "false"
    })
    .execute()
)

# Pass 2: Insert new current versions for changed and brand-new customers
# Identify rows that need insertion: source records not currently active in the target
dim_current = spark.table("catalog.silver.dim_customer") \
    .filter(F.col("is_current") == True) \
    .select("customer_id", "name", "address")

new_versions = source_df.alias("s").join(
    dim_current.alias("t"),
    on=(
        (F.col("s.customer_id") == F.col("t.customer_id")) &
        (F.col("s.name") == F.col("t.name")) &
        (F.col("s.address") == F.col("t.address"))
    ),
    how="left_anti"  # Rows in source with no matching current record in target
)

(
    dim_customer.alias("target")
    .merge(
        new_versions.alias("source"),
        # Prevent duplicate inserts: only insert if this exact version is not current
        """target.customer_id = source.customer_id
           AND target.is_current = true
           AND target.name = source.name
           AND target.address = source.address"""
    )
    .whenNotMatchedInsertAll()
    .execute()
)
```

#### SQL Example

```sql
-- Pass 1: Expire records for customers whose tracked attributes have changed
MERGE INTO main.silver.dim_customer AS t
USING (
    SELECT
        customer_id,
        name,
        address,
        current_timestamp() AS effective_from,
        CAST('9999-12-31' AS DATE) AS effective_to,
        true AS is_current
    FROM main.bronze.raw_customer_updates
) AS s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.name != s.name OR t.address != s.address) THEN
    UPDATE SET t.effective_to = current_date(), t.is_current = false;

-- Pass 2: Insert new current records for changed customers and net-new customers
MERGE INTO main.silver.dim_customer AS target
USING (
    SELECT
        s.customer_id,
        s.name,
        s.address,
        current_date()             AS effective_from,
        CAST('9999-12-31' AS DATE) AS effective_to,
        true                       AS is_current
    FROM main.bronze.raw_customer_updates s
    WHERE NOT EXISTS (
        SELECT 1
        FROM main.silver.dim_customer t
        WHERE t.customer_id = s.customer_id
          AND t.is_current  = true
          AND t.name        = s.name
          AND t.address     = s.address
    )
) AS source
ON target.customer_id = source.customer_id
   AND target.is_current = true
   AND target.name       = source.name
   AND target.address    = source.address
WHEN NOT MATCHED THEN INSERT *;
```

Query the current version:

```sql
SELECT * FROM main.silver.dim_customer
WHERE is_current = true;
```

Query the version active at a specific point in time:

```sql
SELECT * FROM main.silver.dim_customer
WHERE effective_from <= '2024-06-01'
  AND effective_to   >  '2024-06-01';
```

### Discussion and Concerns

- **Surrogate key requirement:** SCD Type 2 tables must include a surrogate key (e.g., `customer_sk` as a `BIGINT GENERATED ALWAYS AS IDENTITY` column) in addition to the business key. Fact tables join to the dimension using the surrogate key to pin the fact to the specific historical version active at transaction time.
- **Two-pass ordering:** The expire pass (Pass 1) must complete before the insert pass (Pass 2). In a Databricks Jobs pipeline, these are sequential notebook tasks with an explicit dependency edge.
- **Table size growth:** Every attribute change adds a new row. Run `OPTIMIZE catalog.silver.dim_customer ZORDER BY (customer_id)` regularly to maintain file compaction and query performance.
- **Python vs. SQL difference:** The SQL `NOT EXISTS` subquery in Pass 2 is clean and readable. The PySpark equivalent uses a `left_anti` join, which compiles to the same Spark plan but is more explicit about the anti-join intent. Both approaches produce identical results.

### See Also

- [Databricks SCD Type 2 with Delta Lake](https://learn.microsoft.com/en-us/azure/databricks/delta/merge)
- [Table deletes, updates, and merges — Delta Lake](https://docs.delta.io/latest/delta-update.html)
- [SCD Type 2 via SDP APPLY CHANGES INTO — next method](#slowly-changing-dimensions--scd-type-2-via-sdp-apply-changes-into)
- [Processing Architectural Patterns (Native) — SCD vs. Satellite design](./processing_patterns.md)

---

## Slowly Changing Dimensions — SCD Type 2 via SDP APPLY CHANGES INTO

Lakeflow Spark Declarative Pipelines provides a declarative `APPLY CHANGES INTO` statement that automates SCD Type 2 history tracking from a CDC source. It replaces the manual two-pass MERGE pattern with a single dataset definition that SDP manages incrementally.

### Problem

A `dim_customer` dimension needs SCD Type 2 history tracking on `name` and `address` attributes. The source is a Change Data Feed (CDF) or a sequence-keyed CDC stream. Writing and maintaining the two-pass MERGE logic manually in a batch Job is error-prone. The team wants a declarative, platform-managed solution.

### Solution

Define an `APPLY CHANGES INTO` target in an SDP pipeline. SDP handles the CDC apply logic, sequence ordering, and type-2 versioning automatically.

#### Python Example (SDP pipeline notebook)

```python
import dlt
from pyspark.sql import functions as F

# Define the streaming source (CDF from Bronze)
@dlt.view
def raw_customer_updates():
    return (
        spark.readStream
             .format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", 0)
             .table("main.bronze.raw_customers")
    )

# Apply SCD Type 2 changes into the Silver dimension table
dlt.apply_changes(
    target      = "main.silver.dim_customer",
    source      = "raw_customer_updates",
    keys        = ["customer_id"],
    sequence_by = "updated_at",            # Ordering column to resolve conflicts
    stored_as_scd_type = 2,                # SCD Type 2: keeps full history
    track_history_column_list = ["name", "address"]  # Only track changes to these columns
)
```

#### SQL Example (SDP pipeline notebook)

```sql
-- Define the CDC source view
CREATE OR REFRESH STREAMING LIVE VIEW raw_customer_updates AS
SELECT * FROM STREAM(main.bronze.raw_customers);

-- Apply SCD Type 2 changes into the Silver dimension
APPLY CHANGES INTO main.silver.dim_customer
FROM STREAM(LIVE.raw_customer_updates)
KEYS (customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2
TRACK HISTORY ON name, address;
```

SDP automatically manages the `__START_AT` and `__END_AT` metadata columns for each historical version. Query the current version:

```sql
SELECT * FROM main.silver.dim_customer
WHERE __END_AT IS NULL;
```

Query the version active at a specific point in time:

```sql
SELECT * FROM main.silver.dim_customer
WHERE __START_AT <= '2024-06-01'
  AND (__END_AT > '2024-06-01' OR __END_AT IS NULL);
```

### Discussion and Concerns

- **SDP manages SCD metadata columns:** Unlike the manual two-pass MERGE pattern, you do not define `effective_from`, `effective_to`, or `is_current` columns. SDP adds `__START_AT` and `__END_AT` columns automatically. Column names can be customised via `track_history_except_column_list` parameter.
- **Sequence ordering is critical:** The `SEQUENCE BY` column must be monotonically increasing per key. SDP uses this column to determine which change event is the most recent when multiple events arrive in the same micro-batch.
- **Hard deletes:** To handle hard deletes from the source, add `APPLY AS DELETE WHEN operation = "DELETE"` to the APPLY CHANGES statement when reading from a CDF source that includes delete operations.
- **SDP vs. batch MERGE trade-off:** `APPLY CHANGES INTO` is significantly simpler to implement and maintain than the two-pass MERGE pattern. However, it requires running the pipeline in SDP, which has its own compute and management overhead. For simple batch pipelines that do not benefit from SDP's streaming and quality features, the manual two-pass MERGE may be a lighter-weight choice.

### See Also

- [SDP APPLY CHANGES INTO documentation](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [SDP SCD Type 2 reference](https://docs.databricks.com/en/delta-live-tables/cdc.html#scd-type-2)
- [SCD Type 2 via two-pass MERGE — previous method](#slowly-changing-dimensions--scd-type-2-native-delta-merge)

---

## Joins and Enrichment

Joining a large fact table to one or more dimension or lookup tables is one of the most common Gold layer operations. Choosing the right join strategy significantly impacts query performance, especially when one table is very large and the other is small enough to fit in memory.

### Problem

A Gold `fact_orders` table (500 million rows) must be enriched with attributes from a `dim_product` lookup table (50,000 rows) and a `dim_region` lookup table (200 rows). The enrichment runs daily as part of a Gold refresh pipeline. Join performance is critical — the pipeline must complete within a 30-minute SLA.

### Solution

Use broadcast joins for the small dimension tables, which replicate the small table to every executor and eliminate the shuffle phase entirely.

#### Python Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.getOrCreate()

fact_orders = spark.table("catalog.silver.fact_orders")
dim_product = spark.table("catalog.gold.dim_product")
dim_region  = spark.table("catalog.gold.dim_region")

# Broadcast the small dimension tables to avoid shuffle joins
enriched = fact_orders \
    .join(broadcast(dim_product), on="product_id", how="left") \
    .join(broadcast(dim_region),  on="region_id",  how="left")

enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .saveAsTable("catalog.gold.fact_orders_enriched")
```

#### SQL Example

```sql
-- Use the BROADCAST hint to force broadcast joins for small dimension tables
SELECT /*+ BROADCAST(dp), BROADCAST(dr) */
    fo.order_id,
    fo.order_date,
    fo.quantity,
    fo.unit_price,
    dp.product_name,
    dp.category,
    dr.region_name,
    dr.country
FROM catalog.silver.fact_orders fo
LEFT JOIN catalog.gold.dim_product dp ON fo.product_id = dp.product_id
LEFT JOIN catalog.gold.dim_region  dr ON fo.region_id  = dr.region_id;
```

### Discussion and Concerns

- **Broadcast join threshold:** Spark automatically broadcasts tables smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). Dimension tables larger than 10 MB but small enough to fit in executor memory can be broadcast manually with the `broadcast()` function or `BROADCAST` hint. Set the threshold higher if all dimension tables are trusted to fit in memory: `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")`.
- **When broadcast is not appropriate:** Do not broadcast a table that exceeds available executor memory per node. Check table size with `DESCRIBE DETAIL catalog.gold.dim_product` before forcing a broadcast.
- **Skew joins:** If the fact table has a heavily skewed join key, use AQE (Adaptive Query Execution, enabled by default in Databricks Runtime 7.3+) to mitigate skew automatically.
- **Bucketing for repeated large-to-large joins:** If the same two large tables are joined repeatedly in production, consider bucketing both tables on the join key with the same number of buckets. Bucketing co-partitions the data at write time, eliminating the shuffle at join time entirely.

### See Also

- [Databricks join hints documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-hints.html)
- [Adaptive Query Execution in Databricks](https://docs.databricks.com/en/optimizations/aqe.html)
- [Delta Lake OPTIMIZE and Z-ORDER](https://docs.databricks.com/en/delta/optimize.html)

---

## Incremental Load Patterns — Databricks Jobs

In native Databricks pipelines, incremental loading (processing only new or changed records since the last run) is implemented using Databricks Jobs with explicit watermark logic in each task. This section shows the three main native incremental load strategies.

### Problem

A Gold `fact_web_events` table receives 50 million new rows per day. The pipeline must be incremental — loading only new records on each run. The team needs to understand which native strategy (append, merge, partition overwrite) is appropriate for their load pattern.

### Solution

Choose a native strategy based on the load pattern, then implement it as a Databricks Jobs notebook task.

#### Strategy 1: Append-only incremental (Python)

Use when: the source guarantees no duplicates and no late-arriving updates.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Read the current high watermark from the target table
max_loaded = spark.sql(
    "SELECT MAX(event_timestamp) FROM catalog.gold.fact_web_events"
).collect()[0][0]

# Load only records newer than the watermark
new_records = spark.table("catalog.silver.stg_web_events") \
    .filter(F.col("event_timestamp") > max_loaded)

# Append to the target table
new_records.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("catalog.gold.fact_web_events")
```

```sql
-- SQL equivalent: insert only new records
INSERT INTO catalog.gold.fact_web_events
SELECT *
FROM catalog.silver.stg_web_events
WHERE event_timestamp > (
    SELECT MAX(event_timestamp) FROM catalog.gold.fact_web_events
);
```

#### Strategy 2: MERGE incremental (Python)

Use when: the source may send duplicates or late-arriving updates that need to overwrite existing rows.

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

target = DeltaTable.forName(spark, "catalog.gold.fact_web_events")

# Load recent source records (include a lookback window to catch late arrivals)
max_loaded = spark.sql(
    "SELECT MAX(event_timestamp) FROM catalog.gold.fact_web_events"
).collect()[0][0]

source_df = spark.table("catalog.silver.stg_web_events") \
    .filter(F.col("event_timestamp") > max_loaded)

(
    target.alias("target")
    .merge(
        source_df.alias("source"),
        "target.event_id = source.event_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

```sql
-- SQL equivalent: upsert new and updated records
MERGE INTO catalog.gold.fact_web_events AS target
USING (
    SELECT *
    FROM catalog.silver.stg_web_events
    WHERE event_timestamp > (
        SELECT MAX(event_timestamp) FROM catalog.gold.fact_web_events
    )
) AS source
  ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

#### Strategy 3: Partition overwrite incremental (Python)

Use when: the table is partitioned by date, each partition is fully defined by one pipeline run, and no row-level updates are needed within a partition.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Load today's events — these will replace any existing partition for today's date
todays_events = spark.table("catalog.silver.stg_web_events") \
    .filter(F.col("event_date") == F.current_date())

# Dynamic partition overwrite: replaces only the partitions present in the source
todays_events.write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .saveAsTable("catalog.gold.fact_web_events")
```

```sql
-- SQL equivalent: replace today's partition atomically
INSERT OVERWRITE catalog.gold.fact_web_events
PARTITION (event_date = current_date())
SELECT * FROM catalog.silver.stg_web_events
WHERE event_date = current_date();
```

### Discussion and Concerns

- **Append — fastest, but creates duplicates on re-run:** If the pipeline fails midway and is re-run, `append` will insert duplicate records. Use append only when the source guarantees exactly-once delivery, or when duplicates are deduplicated at query time.
- **MERGE — safest, but slowest:** MERGE requires scanning the target table to find matching rows. Z-ORDER the target table on the merge key column to improve performance. MERGE is the correct strategy when the source may send duplicates or late arrivals.
- **Partition overwrite — good middle ground:** Replaces entire partitions atomically. Faster than MERGE (no row-level matching) and safe on re-run (re-running the same partition is idempotent). Use when the table is partitioned by date and each partition is fully defined by one pipeline run.
- **Full reload:** When a full reload is needed (after schema changes or data corruption), use `df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(...)` or `CREATE OR REPLACE TABLE ... AS SELECT ...`. Full reloads should not be part of a regular production schedule for large tables.

### See Also

- [Delta Lake write modes documentation — Delta Lake](https://docs.delta.io/latest/delta-update.html)
- [Delta Lake MERGE performance tuning](https://docs.databricks.com/en/delta/merge.html#performance-tuning)
- [Databricks Jobs documentation](https://learn.microsoft.com/en-us/azure/databricks/jobs/)

---

## Reusable Transformation Logic — Native Python Functions and Spark SQL UDFs

Reusable transformation logic is achieved through native Python functions (for PySpark pipelines) and Spark SQL User-Defined Functions (for SQL notebooks and DLT pipelines).

### Problem

Multiple Gold notebooks need to apply the same fiscal quarter calculation logic. Two notebooks need pivoting (converting row values into columns) and one notebook needs a dense date spine. Copy-pasting the SQL for each notebook is error-prone and hard to maintain.

### Solution

Define reusable logic as a native Python utility module (for PySpark use) and as a registered Spark SQL UDF (for SQL use). Package the module as part of a Databricks Asset Bundle for deployment.

#### Python Example — Reusable function module

Create `src/transforms/fiscal_calendar.py`:

```python
from pyspark.sql import functions as F
from pyspark.sql import Column

def fiscal_quarter(date_col: Column) -> Column:
    """
    Returns the fiscal quarter label (Q1-Q4) for a date column.
    Fiscal year starts in April (April=Q1, July=Q2, October=Q3, January=Q4).
    """
    return (
        F.when(F.month(date_col).isin(4, 5, 6),  F.lit("Q1"))
         .when(F.month(date_col).isin(7, 8, 9),  F.lit("Q2"))
         .when(F.month(date_col).isin(10, 11, 12), F.lit("Q3"))
         .when(F.month(date_col).isin(1, 2, 3),  F.lit("Q4"))
    )
```

Use the function in a Gold notebook:

```python
import sys
sys.path.insert(0, "/Workspace/Repos/my-project/src")

from transforms.fiscal_calendar import fiscal_quarter
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.table("catalog.silver.fact_orders") \
    .withColumn("fiscal_quarter", fiscal_quarter(F.col("order_date")))
```

Dynamic pivot using PySpark:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# PySpark pivot resolves column values at runtime — no static list required
pivot_df = spark.table("catalog.silver.fact_orders") \
    .groupBy("order_month") \
    .pivot("category", ["Electronics", "Clothing", "Books"]) \
    .agg(F.sum("revenue"))

pivot_df.write.format("delta").mode("overwrite") \
    .saveAsTable("catalog.gold.sales_pivot")
```

#### SQL Example — Registered Spark SQL UDF

Register the fiscal quarter logic as a permanent SQL UDF in Unity Catalog:

```sql
-- Register a reusable SQL function in Unity Catalog
CREATE OR REPLACE FUNCTION catalog.gold.fiscal_quarter(order_date DATE)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Returns fiscal quarter label (Q1-Q4). Fiscal year starts in April.'
AS $$
  CASE
    WHEN MONTH(order_date) IN (4, 5, 6)   THEN 'Q1'
    WHEN MONTH(order_date) IN (7, 8, 9)   THEN 'Q2'
    WHEN MONTH(order_date) IN (10, 11, 12) THEN 'Q3'
    WHEN MONTH(order_date) IN (1, 2, 3)   THEN 'Q4'
  END
$$;
```

Use the function in any SQL notebook or SDP pipeline in the same catalog:

```sql
SELECT
    order_id,
    order_date,
    revenue,
    catalog.gold.fiscal_quarter(order_date) AS fiscal_quarter
FROM catalog.silver.fact_orders;
```

SQL pivot (static column list):

```sql
SELECT *
FROM (
    SELECT order_month, category, revenue
    FROM catalog.silver.fact_orders
)
PIVOT (
    SUM(revenue)
    FOR category IN ('Electronics', 'Clothing', 'Books')
);
```

### Discussion and Concerns

- **Unity Catalog SQL functions are persistent:** Unity Catalog SQL functions are stored in the catalog and callable from any notebook, SDP pipeline, or SQL Warehouse in the same catalog without re-registration.
- **PySpark pivot is dynamic; SQL PIVOT is static:** `groupBy().pivot().agg()` in PySpark resolves pivot values at runtime from the data. SQL `PIVOT` requires a static list of values. If the set of pivot values changes, the SQL query must be updated. For dynamic pivoting in SQL, use `CASE`-based aggregations or route to PySpark.
- **Unit testing native functions:** PySpark functions in a Python module can be unit-tested with pytest and a local SparkSession. SQL UDFs can be tested with `SELECT catalog.gold.fiscal_quarter('2024-05-01')` directly in a notebook. Neither requires a full pipeline run to validate.
- **Packaging for Asset Bundles:** Include the `src/` folder in `databricks.yml` as a Python wheel or notebook library so that all Job tasks in the bundle have access to the shared utility module.

### See Also

- [Unity Catalog user-defined functions](https://docs.databricks.com/en/udf/unity-catalog.html)
- [PySpark pivot documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.pivot.html)
- [Databricks Asset Bundles — library configuration](https://docs.databricks.com/en/dev-tools/bundles/index.html)

---

## Date Spine Generation — Native Spark

A dense sequence of dates (to ensure no dates are missing from a time series) is achieved in Spark with `sequence()` + `explode()` (SQL or PySpark) or with `date_add()` combined with a range.

### Problem

A Gold time series table must contain one row per day for a specified date range, even for days with no source events. Without a dense date spine, days with zero activity are missing from the output, which breaks running totals, period-over-period comparisons, and BI time series charts.

### Solution

Generate a date range using `sequence()` in SQL or PySpark's `F.sequence()` + `F.explode()`, then join the fact data to the spine.

#### Python Example

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Generate a dense date spine from 2023-01-01 to 2025-12-31
date_spine = spark.range(1).select(
    F.explode(
        F.sequence(
            F.to_date(F.lit("2023-01-01")),
            F.to_date(F.lit("2025-12-31")),
            F.expr("INTERVAL 1 DAY")
        )
    ).alias("date_day")
)

# Add calendar attributes
from transforms.fiscal_calendar import fiscal_quarter

date_spine_enriched = date_spine \
    .withColumn("year",          F.year("date_day")) \
    .withColumn("month",         F.month("date_day")) \
    .withColumn("fiscal_quarter", fiscal_quarter(F.col("date_day"))) \
    .withColumn("day_of_week",   F.dayofweek("date_day"))

# Join the date spine to fact data, filling gaps with zero
daily_revenue = spark.table("catalog.silver.fact_orders") \
    .groupBy(F.to_date("order_date").alias("date_day")) \
    .agg(F.sum("revenue").alias("daily_revenue"))

result = date_spine_enriched \
    .join(daily_revenue, on="date_day", how="left") \
    .fillna(0, subset=["daily_revenue"])

result.write.format("delta").mode("overwrite") \
    .saveAsTable("catalog.gold.daily_revenue_spine")
```

#### SQL Example

```sql
-- Generate a dense date spine using sequence() and explode()
WITH date_spine AS (
    SELECT
        explode(
            sequence(
                DATE '2023-01-01',
                DATE '2025-12-31',
                INTERVAL 1 DAY
            )
        ) AS date_day
),
enriched_spine AS (
    SELECT
        date_day,
        YEAR(date_day)  AS year,
        MONTH(date_day) AS month,
        catalog.gold.fiscal_quarter(date_day) AS fiscal_quarter,
        DAYOFWEEK(date_day) AS day_of_week
    FROM date_spine
),
daily_agg AS (
    SELECT
        DATE(order_date)      AS date_day,
        SUM(revenue)          AS daily_revenue
    FROM catalog.silver.fact_orders
    GROUP BY DATE(order_date)
)
SELECT
    s.date_day,
    s.year,
    s.month,
    s.fiscal_quarter,
    s.day_of_week,
    COALESCE(d.daily_revenue, 0) AS daily_revenue
FROM enriched_spine s
LEFT JOIN daily_agg d ON s.date_day = d.date_day
ORDER BY s.date_day;
```

To generate a date spine anchored to actual data boundaries (dynamic start/end):

```sql
WITH bounds AS (
    SELECT MIN(DATE(order_date)) AS start_date, MAX(DATE(order_date)) AS end_date
    FROM catalog.silver.fact_orders
),
date_spine AS (
    SELECT explode(sequence(b.start_date, b.end_date, INTERVAL 1 DAY)) AS date_day
    FROM bounds b
)
SELECT date_day FROM date_spine ORDER BY date_day;
```

### Discussion and Concerns

- **`sequence()` is native to Spark SQL and PySpark:** No external packages are required.
- **Performance:** `sequence()` generates the full date array in a single row before `explode()` materialises it. For very long date ranges (multi-decade spines at day granularity), the array size is still small (< 20,000 elements for 50 years). Performance is not a concern for typical date spines.
- **Granularity:** Change `INTERVAL 1 DAY` to `INTERVAL 1 MONTH`, `INTERVAL 1 HOUR`, etc. for different granularities. For sub-day granularities (hourly, minutely), consider whether a full pre-generated spine is necessary or whether the spine can be replaced with a `date_trunc` groupBy on the fact data.
- **Calendar enrichment:** The date spine generates structural date attributes. Business calendar attributes (holidays, fiscal periods for non-April fiscal years, trading days) should be maintained in a separate calendar reference table and joined to the spine.

### See Also

- [Spark SQL `sequence()` function](https://docs.databricks.com/en/sql/language-manual/functions/sequence.html)
- [Spark SQL `explode()` function](https://docs.databricks.com/en/sql/language-manual/functions/explode.html)
- [Reusable Transformation Logic — Native Python Functions and Spark SQL UDFs](#reusable-transformation-logic--native-python-functions-and-spark-sql-udfs)

---

## Lakeflow Spark Declarative Pipelines — Multi-Hop Pipeline

> **Architecture diagram:** [Lakeflow Spark Declarative Pipelines overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/) includes a pipeline DAG diagram showing table dependencies, data quality expectation enforcement points, and the Bronze → Silver → Gold lineage graph. [Pipeline monitoring — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/observability) shows the event log and observability dashboard.

Lakeflow Spark Declarative Pipelines (SDP) is Databricks' declarative pipeline framework. SDP manages compute provisioning, checkpointing, retry logic, and data quality enforcement automatically. The full architectural context — when to choose SDP over a Databricks Jobs pipeline, pipeline modes, and cost trade-offs — is in `processing_patterns.md`.

### Problem

A Bronze → Silver medallion pipeline must be built for order data with built-in data quality checks, automatic schema inference on the bronze layer, and managed cluster lifecycle. Failed quality checks must be tracked and quarantined rather than silently dropped or causing pipeline failure.

### Solution

Define pipeline datasets using `@dlt.table` decorators and `@dlt.expect` annotations (Python) or `CREATE OR REFRESH STREAMING TABLE` with `CONSTRAINT ... EXPECT` clauses (SQL). Deploy as an SDP pipeline via the Databricks UI, CLI, or Databricks Asset Bundles.

#### Python Example

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

# Bronze: ingest from cloud storage via Auto Loader.
# spark.readStream is used here because cloud storage is an *external* source,
# not an SDP-managed table. dlt.read_stream() is only for SDP-managed tables.
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

# Silver: cleanse and enforce quality rules.
# dlt.read_stream() is correct here because orders_bronze is an SDP-managed table.
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

#### SQL Example

```sql
-- Bronze: ingest from cloud storage via cloud_files() (Auto Loader in SQL syntax)
CREATE OR REFRESH STREAMING TABLE orders_bronze
COMMENT 'Raw orders from ADLS'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT * FROM cloud_files(
  'abfss://raw@mystorageaccount.dfs.core.windows.net/orders/',
  'json',
  map('cloudFiles.schemaLocation', '/pipelines/orders/schema')
);

-- Silver: cleanse with inline CONSTRAINT quality rules
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
| Reusable functions | Python functions importable across pipeline files | No cross-definition function reuse in SQL |

### Discussion and Concerns

- **`spark.readStream` vs. `dlt.read_stream()`:** The bronze layer uses `spark.readStream.format("cloudFiles")` because cloud storage is an external source, not an SDP-managed table. `dlt.read_stream()` is for reading SDP-managed tables (those defined with `@dlt.table` or `CREATE OR REFRESH STREAMING TABLE`). Using `spark.table()` on an SDP-managed table bypasses incremental processing; using `dlt.read_stream()` on a cloud storage path raises a resolution error.
- **Pipeline mode:** Triggered mode (default) runs once and terminates — appropriate for batch-oriented pipelines. Continuous mode runs indefinitely. For most Bronze → Silver pipelines, triggered mode is cheaper and sufficient.
- **Managed table lifecycle:** Tables created inside an SDP pipeline are managed by the pipeline. If the pipeline is deleted, the managed tables and their data are also deleted. To retain tables after pipeline deletion, write to external Delta tables using an external storage location.
- **One pipeline per managed table:** An SDP-managed table can only be written by the pipeline that created it. To share data between pipelines, materialise to an external (non-SDP-managed) Delta table.
- **Deploying a pipeline:** Create via the Databricks UI (Lakeflow Spark Declarative Pipelines → Create pipeline), via the CLI (`databricks pipelines create --json '{"name":"orders","libraries":[{"notebook":{"path":"/path/to/pipeline_notebook"}}]}'`), or via Databricks Asset Bundles with a `pipelines:` block in `databricks.yml`. See [Create a pipeline](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/configure-pipeline) for the full reference.
- **DBU premium:** SDP incurs a DBU premium over equivalent Structured Streaming on standard clusters. For simple pipelines where quality enforcement can be done with notebook assertions, a Jobs-based PySpark pipeline may be cheaper.

### See Also

- [Lakeflow Spark Declarative Pipelines — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/)
- [SDP expectations — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations)
- [SDP APPLY CHANGES INTO — Azure Databricks](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [Slowly Changing Dimensions — SCD Type 2 via SDP APPLY CHANGES INTO](#slowly-changing-dimensions--scd-type-2-via-sdp-apply-changes-into)
- `processing_patterns.md` — when to choose SDP vs. Databricks Jobs

---

## Pipeline Orchestration — Databricks Jobs and Asset Bundles

Databricks Jobs is the native orchestration layer for batch and streaming pipelines. It provides a DAG of tasks that can include notebook tasks, Python script tasks, SQL tasks, SDP pipeline tasks, and Databricks Asset Bundle deployments.

### Problem

A multi-step processing pipeline consists of: (1) Bronze cleansing to Silver, (2) SCD Type 2 dimension update, (3) Gold aggregation, and (4) post-run data quality assertions. The pipeline must run daily on a schedule, retry failed tasks automatically, and alert the team on failure. Running each notebook manually is not sustainable.

### Solution

Define the pipeline as a Databricks Job using Databricks Asset Bundles. Each logical step is a separate task with explicit dependencies.

#### Python Example — Asset Bundle job definition

Create `resources/processing_job.yml` in the bundle:

```yaml
resources:
  jobs:
    daily_processing_job:
      name: "Daily Processing Pipeline"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"   # Daily at 06:00 UTC
        timezone_id: "UTC"
      email_notifications:
        on_failure:
          - data-team-alerts@example.com
      tasks:
        - task_key: bronze_to_silver
          description: "Cleanse and deduplicate Bronze customers to Silver"
          notebook_task:
            notebook_path: src/notebooks/bronze_to_silver.py
            base_parameters:
              catalog: "main"
              env:    "prod"
          existing_cluster_id: "${var.cluster_id}"
          max_retries: 2
          retry_on_timeout: true

        - task_key: scd_type2_update
          description: "Two-pass SCD Type 2 MERGE for dim_customer"
          depends_on:
            - task_key: bronze_to_silver
          notebook_task:
            notebook_path: src/notebooks/scd_type2_dim_customer.py
          existing_cluster_id: "${var.cluster_id}"
          max_retries: 1

        - task_key: gold_aggregation
          description: "Aggregate Silver orders into Gold fact table"
          depends_on:
            - task_key: scd_type2_update
          notebook_task:
            notebook_path: src/notebooks/gold_aggregation.py
          existing_cluster_id: "${var.cluster_id}"
          max_retries: 1

        - task_key: data_quality_assertions
          description: "Run post-pipeline data quality checks"
          depends_on:
            - task_key: gold_aggregation
          notebook_task:
            notebook_path: src/notebooks/quality_assertions.py
          existing_cluster_id: "${var.cluster_id}"
          max_retries: 0   # Do not retry quality checks; failure should halt the pipeline
```

Deploy and run:

```bash
# Deploy the bundle to dev
databricks bundle deploy --target dev

# Trigger a manual run of the job
databricks bundle run --target dev daily_processing_job

# Monitor the run
databricks jobs list-runs --job-id <job_id>
```

#### Python Example — Post-pipeline quality assertions notebook

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

assertions = []

# Assert: exactly one current row per customer_id in dim_customer
duplicates = spark.sql("""
    SELECT customer_id, COUNT(*) AS cnt
    FROM main.silver.dim_customer
    WHERE is_current = true
    GROUP BY customer_id
    HAVING cnt > 1
""")
assertions.append(("SCD uniqueness", duplicates.count()))

# Assert: Gold row count is non-decreasing (must be >= yesterday's count)
today_count    = spark.table("main.gold.fact_orders_enriched").count()
yesterday_count = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM main.gold.fact_orders_enriched VERSION AS OF 1
""").collect()[0]["cnt"]
assertions.append(("Gold row count non-decreasing", 0 if today_count >= yesterday_count else 1))

# Assert: no null order_ids in Gold
null_keys = spark.sql("""
    SELECT COUNT(*) AS cnt FROM main.gold.fact_orders_enriched
    WHERE order_id IS NULL
""").collect()[0]["cnt"]
assertions.append(("No null order_ids", null_keys))

# Raise an exception if any assertion fails
failures = [(name, count) for name, count in assertions if count > 0]
if failures:
    raise AssertionError(f"Data quality assertions failed: {failures}")

print("All data quality assertions passed.")
```

#### SQL Example — Equivalent quality assertions as SQL statements

```sql
-- Assert: exactly one current row per customer_id in dim_customer
-- Returns rows if the assertion FAILS (expected: zero rows)
SELECT customer_id, COUNT(*) AS duplicate_count
FROM main.silver.dim_customer
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Assert: no null order_ids in Gold
-- Returns rows if the assertion FAILS (expected: zero rows)
SELECT COUNT(*) AS null_key_count
FROM main.gold.fact_orders_enriched
WHERE order_id IS NULL;

-- Assert: Gold row count is non-decreasing (check current vs. previous Delta version)
SELECT
    (SELECT COUNT(*) FROM main.gold.fact_orders_enriched)                       AS current_count,
    (SELECT COUNT(*) FROM main.gold.fact_orders_enriched VERSION AS OF 1)       AS previous_count;
```

### Discussion and Concerns

- **Pipeline definition as code:** Notebook tasks execute the transformations and the quality assertions task runs post-pipeline checks. The bundle YAML is the pipeline definition file checked into version control.
- **Task retries and failure handling:** Set `max_retries` per task to handle transient cluster failures. Set `max_retries: 0` on quality assertion tasks so that a quality failure halts the pipeline immediately rather than retrying.
- **Alerting:** Configure `email_notifications` or webhook notifications on the job for on-failure and on-success alerts. Databricks also integrates with PagerDuty and Slack via notification destinations.
- **Serverless compute:** For tasks that do not require large clusters, use `serverless: true` on the task definition to use Databricks Serverless compute, which eliminates cluster startup time and reduces cost for short-running tasks.
- **CI/CD integration:** Add `databricks bundle validate` and `databricks bundle deploy --target staging` as CI steps in your Git provider's pipeline (GitHub Actions, Azure DevOps, GitLab CI). This deploys the bundle to a staging environment on every pull request merge.

### See Also

- [Databricks Jobs documentation](https://learn.microsoft.com/en-us/azure/databricks/jobs/)
- [Databricks Asset Bundles documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Databricks Jobs CI/CD with GitHub Actions](https://docs.databricks.com/en/dev-tools/bundles/ci-cd.html)
- [Lakeflow Spark Declarative Pipelines orchestration](https://docs.databricks.com/en/delta-live-tables/index.html)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Job run status | Databricks Jobs UI — job run history; `system.lakeflow.job_run_timeline` | Failed runs, retry counts, jobs exceeding SLA duration |
| Quality assertion failures | Job run logs in Databricks Jobs UI — task output tab | Any assertion failure should halt the pipeline (raise exception in assertion task) |
| Delta table version history | `DESCRIBE HISTORY catalog.silver.customers` | Unexpected write gaps (no new version when pipeline should have run), accidental full overwrites |
| Row count trend | Monitoring query on `system.information_schema.tables` or a custom row count assertion in the pipeline | Sudden drops in row count may indicate an upstream ingestion failure or incorrect incremental filter |
| MERGE execution time | Spark UI job timeline in Databricks (click through from Jobs run); `system.query.history` | MERGE jobs exceeding SLA threshold — indicates table needs OPTIMIZE/Z-ORDER or AQE tuning |
| Data freshness | `DESCRIBE DETAIL catalog.silver.customers` — `lastModified` timestamp | Tables not refreshed within expected cadence |
| Deletion vector compaction | `DESCRIBE DETAIL` — `numDeletionVectorRows` | High deletion vector row count indicates OPTIMIZE is needed to compact files |
| SDP pipeline health | Lakeflow Spark Declarative Pipelines UI — pipeline graph; event log table `system.event_log` | Failed datasets, quality rule violation rates, pipeline restart loops |

### Metrics for Success

- [ ] Quality assertion notebook passes on every scheduled run — all assertions produce zero violation rows in job logs
- [ ] No unexpected nulls in Silver key columns — null key assertion in the post-pipeline quality task produces zero rows
- [ ] Deduplication check passes: row count after deduplication equals the count of distinct values on the deduplication key column
- [ ] SCD Type 2 dimension tables have exactly one `is_current = true` (or `__END_AT IS NULL` for SDP) record per business key — validated by post-run assertion query
- [ ] MERGE execution time for all incremental tasks is within the defined SLA (e.g., under 15 minutes for a 500 million row fact table with Z-ORDER on the merge key)
- [ ] Gold table row counts are non-decreasing on each daily run (unless a deliberate delete or correction run has been executed) — monitored via the row count assertion task
- [ ] Databricks Asset Bundle validates and deploys successfully in CI on every pull request merge to main
