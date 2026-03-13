# Processing, Summarizing, and Transformation Cookbook

---

## Introduction

This cookbook provides practical, step-by-step guidance for **processing, summarising, and transforming data** on Databricks. It covers Delta Lake transformations (MERGE, UPDATE, DELETE), aggregations and window functions, data cleansing and deduplication, Slowly Changing Dimension (SCD) patterns, join optimisation, and dbt transformation patterns including incremental strategies, snapshots, and macros.

Each method section follows a consistent structure: the problem being solved, the recommended solution with Python and SQL examples, any known concerns or trade-offs, and links to further reading. For architectural guidance on when to choose between these patterns, see the [Processing, Summarising, and Transformation Architectural Patterns](./processing_patterns.md) document.

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

Install the following tools before proceeding:

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.9+ | Required for PySpark and dbt-databricks |
| PySpark | Provided by Databricks Runtime | Do not install PySpark manually when running on a Databricks cluster; install locally for unit testing |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | Latest (v0.200+) | Used for workspace interaction and secrets management |
| [dbt-databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup) | 1.7+ | Required for all dbt-based examples |
| [dbt-utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) | 1.0+ | Required for `dbt_utils.pivot()` and `dbt_utils.date_spine()` examples |

Install Python dependencies:

```bash
pip install databricks-cli dbt-databricks dbt-utils
```

Authenticate the Databricks CLI:

```bash
# Interactive token-based authentication
databricks configure --token

# Verify connection
databricks clusters list
```

Configure dbt in `~/.dbt/profiles.yml`:

```yaml
my_databricks_project:
  target: dev
  outputs:
    dev:
      type: databricks
      host: adb-1234567890123456.7.azuredatabricks.net
      http_path: /sql/1.0/warehouses/abcdef1234567890
      token: "{{ env_var('DBT_TOKEN') }}"
      schema: dev_processing
      threads: 4
```

Set the `DBT_TOKEN` environment variable before running dbt:

```bash
export DBT_TOKEN=<your-databricks-personal-access-token>
```

### Getting a New Starter Project

To scaffold a new dbt project for Databricks:

```bash
dbt init my_processing_project
cd my_processing_project

# Verify the connection to your Databricks workspace
dbt debug
```

When prompted by `dbt init`, select `databricks` as the database type and enter your workspace host and SQL Warehouse HTTP path.

Create a `packages.yml` file in the project root to add dbt-utils:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Install packages:

```bash
dbt deps
```

### Getting the Source for an Existing Project

```bash
git clone <repository_url>
cd <project_directory>

# Install Python dependencies
pip install -r requirements.txt

# Install dbt package dependencies
dbt deps

# Verify connection and configuration
dbt debug
```

Review `dbt_project.yml` for model paths, materialisation defaults, and variable definitions before running models. Check `packages.yml` for required package versions.

---

## Infrastructure Pre-Requisites

### Infrastructure Required

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment for all PySpark and SQL examples | Unity Catalog must be enabled for catalog-qualified table references |
| Spark Cluster | Required for PySpark examples and dbt Python models | Databricks Runtime 12.2 LTS or later recommended for deletion vector support |
| Databricks SQL Warehouse | Required for dbt SQL model execution and Spark SQL examples in SQL Editor | Serverless or Pro tier; Standard tier does not support `CREATE TABLE AS SELECT` with Unity Catalog in all regions |
| Unity Catalog | Target for all output Delta tables | Requires `CREATE TABLE` privilege on the target schema |
| Delta tables for source data | Input to all transformation examples | Source tables should exist in a Bronze or Silver schema before running examples |

### Enabling Change Data Feed on Delta Tables

Several patterns in this cookbook benefit from Change Data Feed (CDF), which exposes row-level changes (insert, update, delete) as a readable stream. Enable CDF on a table with:

```sql
ALTER TABLE catalog.schema.my_table
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

CDF is required for efficient CDC-based MERGE patterns where only changed rows from the source need to be processed.

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
- [Processing Architectural Patterns — Medallion layer responsibilities](./processing_patterns.md)

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
  - `GROUPING SETS((a, b), (a), ())` gives explicit control over which combinations to compute — preferred when only specific subtotal levels are needed, as it avoids unnecessary computation.
- **Window function memory:** Window functions with large partitions (e.g., `PARTITION BY customer_id` on a table with millions of distinct customers and thousands of rows per customer) can cause memory pressure on executors. Filter the input DataFrame to the smallest necessary window before applying the function, and consider whether the window can be replaced with a pre-aggregated join.
- **Python vs. SQL functional difference:** `F.countDistinct()` in PySpark and `COUNT(DISTINCT ...)` in SQL are equivalent. However, `approx_count_distinct()` / `APPROX_COUNT_DISTINCT()` can be significantly faster for very high cardinality counts at the cost of a small estimation error — consider it for exploratory Gold tables where exact counts are not required.

### See Also

- [Databricks SQL window functions reference](https://docs.databricks.com/en/sql/language-manual/sql-ref-window-functions.html)
- [PySpark Window API documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [Databricks SQL ROLLUP, CUBE, GROUPING SETS](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-groupby.html)

---

## Data Cleansing and Deduplication

Cleansing and deduplication transforms raw Bronze records into the trusted, typed, deduplicated records that define the Silver layer. This step is the first transformation applied to any new data source and is the gate through which Bronze data must pass before it can be used analytically.

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

- **Tiebreaking on equal timestamps:** Both `dropDuplicates()` and `ROW_NUMBER()` require a deterministic ordering column to choose between duplicates with equal values. If `_ingestion_timestamp` has second-level granularity and multiple duplicates arrive in the same second, the choice of which row to keep is arbitrary. For deterministic deduplication, use a composite sort key (e.g., `_ingestion_timestamp DESC, _source_file_offset DESC`).
- **`dropDuplicates()` vs. `ROW_NUMBER()`:** `dropDuplicates(["customer_id"])` in PySpark keeps an arbitrary row when timestamps are equal — it does not respect ordering. Always use `ROW_NUMBER()` via a Window function when the choice of which duplicate to retain is meaningful.
- **Streaming deduplication:** In a Structured Streaming pipeline, use `dropDuplicates(["customer_id"], watermark_column)` with a watermark to bound the state store. Without a watermark, the deduplication state store grows without bound. See the Streaming Ingestion cookbook for this pattern.
- **Python vs. SQL difference:** The PySpark pipeline is multi-step and easy to unit test step-by-step. The SQL CTE approach compresses the logic into a single statement, which is more concise but harder to debug incrementally. For production pipelines with complex cleansing rules, the PySpark approach is generally preferred for its testability.

### See Also

- [PySpark DataFrame cleansing functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Databricks SQL COALESCE, CAST, TRIM](https://docs.databricks.com/en/sql/language-manual/functions/coalesce.html)
- [Structured Streaming deduplication](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-deduplication)

---

## Slowly Changing Dimensions — SCD Type 1

SCD Type 1 is the simplest history strategy: when a dimension record changes, overwrite the existing row with the new values. No history is retained. This is appropriate when the previous value is meaningless once corrected — for example, fixing a typo in a customer name or updating a reference code.

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

- **When SCD Type 1 is appropriate:** SCD Type 1 is the right choice when the previous value has no analytical meaning once it is corrected. Examples: fixing a typo in a name, updating an email address, correcting a region assignment that was originally wrong. It is not appropriate when historical reporting needs to reflect what value was in place at the time of a transaction — use SCD Type 2 for that requirement.
- **Impact on historical facts:** Overwriting a dimension record with SCD Type 1 retroactively changes the value for all historical fact records that join to it. In a star schema, this means a report for last year will now show the updated customer region, not the region that was in place last year. This is intentional for corrections but may be surprising for genuine attribute changes (e.g., a customer moving region). Discuss with business stakeholders whether a given attribute change should be corrected (SCD1) or tracked (SCD2).
- **No surrogate key required:** Because SCD Type 1 maintains one row per business key, the business key (`customer_id`) is sufficient as the dimension key for fact table joins. A surrogate key adds no value in a pure SCD Type 1 dimension.

### See Also

- [Databricks SCD documentation](https://docs.databricks.com/en/delta/slowly-changing-data.html)
- [Processing Architectural Patterns — SCD vs. Satellite design](./processing_patterns.md)
- [SCD Type 2 method — next section in this cookbook](#slowly-changing-dimensions--scd-type-2)

---

## Slowly Changing Dimensions — SCD Type 2

SCD Type 2 preserves the full history of dimension changes by adding a new row for each change, tracking the effective date range and a flag indicating the currently active version. This allows fact tables to join to the dimension version that was active at the time of the transaction.

### Problem

A `dim_customer` dimension tracks customer segment assignments (`segment`: Bronze, Silver, Gold). Customers are re-segmented periodically. Sales reports must accurately reflect the segment each customer belonged to at the time of their purchase — not their current segment. Each time a customer's segment changes, the previous version must be closed (with an `end_date` and `is_current = false`) and a new version inserted (with a new `start_date` and `is_current = true`).

### Solution

Use a multi-clause Delta MERGE that handles three cases: closing expired records for customers whose attributes have changed, inserting the new version for those same customers, and inserting entirely new customers.

#### Python Example

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Load today's snapshot of current customer segments from Silver
source_df = spark.table("catalog.silver.customers") \
    .withColumn("start_date", F.current_date()) \
    .withColumn("end_date", F.lit(None).cast("date")) \
    .withColumn("is_current", F.lit(True))

dim_customer = DeltaTable.forName(spark, "catalog.gold.dim_customer")

# Step 1: Expire records for customers whose segment has changed
(
    dim_customer.alias("target")
    .merge(
        source_df.alias("source"),
        """target.customer_id = source.customer_id
           AND target.is_current = true
           AND target.segment != source.segment"""
    )
    .whenMatchedUpdate(set={
        "end_date":   "current_date()",
        "is_current": "false"
    })
    .execute()
)

# Step 2: Insert new versions for changed customers and brand-new customers
# Identify customers that need a new row (changed or new)
changed_or_new = source_df.alias("source").join(
    spark.table("catalog.gold.dim_customer")
        .filter(F.col("is_current") == True)
        .alias("target"),
    on="customer_id",
    how="left_anti"
).union(
    source_df.alias("s").join(
        spark.table("catalog.gold.dim_customer")
            .filter(F.col("is_current") == False)
            .filter(F.col("end_date") == F.current_date())
            .alias("t"),
        on="s.customer_id == t.customer_id",
        how="inner"
    ).select("s.*")
)

(
    dim_customer.alias("target")
    .merge(
        changed_or_new.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true AND target.segment = source.segment"
    )
    .whenNotMatchedInsertAll()
    .execute()
)
```

#### SQL Example

```sql
-- Step 1: Expire records for customers whose segment has changed
MERGE INTO catalog.gold.dim_customer AS target
USING catalog.silver.customers AS source
  ON  target.customer_id = source.customer_id
  AND target.is_current  = true
  AND target.segment    != source.segment
WHEN MATCHED THEN
  UPDATE SET
    target.end_date   = current_date(),
    target.is_current = false;

-- Step 2: Insert new versions for changed customers and net-new customers
MERGE INTO catalog.gold.dim_customer AS target
USING (
    -- Customers with a changed segment: source record not found as current in target
    SELECT
        s.customer_id,
        s.segment,
        s.email,
        s.region,
        current_date()  AS start_date,
        NULL            AS end_date,
        true            AS is_current
    FROM catalog.silver.customers s
    WHERE NOT EXISTS (
        SELECT 1 FROM catalog.gold.dim_customer t
        WHERE t.customer_id = s.customer_id
          AND t.is_current  = true
          AND t.segment     = s.segment
    )
) AS source
ON target.customer_id = source.customer_id
   AND target.is_current = true
   AND target.segment = source.segment
WHEN NOT MATCHED THEN
  INSERT (customer_id, segment, email, region, start_date, end_date, is_current)
  VALUES (source.customer_id, source.segment, source.email, source.region,
          source.start_date, source.end_date, source.is_current);
```

### Discussion and Concerns

- **Surrogate key requirement:** SCD Type 2 tables must include a surrogate key (e.g., `customer_sk` as a generated identity column) in addition to the business key (`customer_id`). Fact tables join to the dimension using the surrogate key to pin the fact to the specific historical version active at transaction time. Without a surrogate key, the join cannot be made unambiguous when multiple versions exist.
- **Table size growth:** Every attribute change adds a new row. On high-change-rate dimensions (e.g., a customer segment that changes monthly for millions of customers), the SCD Type 2 table can grow very large. Run `OPTIMIZE catalog.gold.dim_customer ZORDER BY (customer_id)` regularly to maintain file compaction and query performance.
- **Two-pass MERGE limitation:** Delta MERGE does not support inserting and updating the same target row in a single pass for SCD Type 2 (expire old + insert new). The two-step approach above (expire in one MERGE, insert in a second MERGE) is the recommended pattern. The steps must run in order within the same pipeline execution.
- **dbt alternative:** dbt snapshots (see the [dbt Snapshots for SCD Type 2 method](#dbt-snapshots-for-scd-type-2)) automate this pattern without writing the MERGE logic manually. For teams using dbt, snapshots are the preferred implementation.

### See Also

- [Databricks SCD Type 2 with Delta Lake](https://docs.databricks.com/en/delta/slowly-changing-data.html)
- [dbt Snapshots for SCD Type 2 — see method below](#dbt-snapshots-for-scd-type-2)
- [Processing Architectural Patterns — SCD vs. Satellite design](./processing_patterns.md)

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
- **When broadcast is not appropriate:** Do not broadcast a table that exceeds available executor memory per node. Broadcasting a table that is too large causes out-of-memory errors on executors. Check table size with `DESCRIBE DETAIL catalog.gold.dim_product` before forcing a broadcast.
- **Skew joins:** If the fact table has a heavily skewed join key (e.g., a small number of `product_id` values represent 80% of rows), the join will produce skewed tasks that slow the pipeline. Use the `SKEW JOIN` hint or AQE (Adaptive Query Execution, enabled by default in Databricks Runtime 7.3+) to mitigate skew automatically.
- **Bucketing for repeated large-to-large joins:** If the same two large tables are joined repeatedly in production (e.g., fact-to-fact or Silver-to-Silver joins), consider bucketing both tables on the join key with the same number of buckets. Bucketing co-partitions the data at write time, eliminating the shuffle at join time entirely. This is a write-time optimisation and requires both tables to be written with the bucket specification.

### See Also

- [Databricks join hints documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-hints.html)
- [Adaptive Query Execution in Databricks](https://docs.databricks.com/en/optimizations/aqe.html)
- [Delta Lake OPTIMIZE and Z-ORDER](https://docs.databricks.com/en/delta/optimize.html)

---

## dbt Models on Databricks

dbt (data build tool) provides a structured, testable, version-controlled framework for running SQL and Python transformations on Databricks. A dbt model is a single SELECT statement (SQL) or a Python function (Python model) that dbt materialises as a table or view. dbt manages dependencies, runs tests, and generates documentation automatically.

### Problem

A Silver-to-Gold transformation pipeline needs to be version-controlled, tested, and integrated with CI/CD. The pipeline joins multiple Silver tables, applies business logic, and materialises a Gold fact table incrementally (processing only new records since the last run). Ad hoc notebook-based transformations are no longer maintainable as the number of models grows.

### Solution

Implement the transformation as a dbt incremental model. SQL models run on the Databricks SQL Warehouse; Python dbt models run on a Spark cluster.

#### Python Example

A Python dbt model (saved as `models/marts/fact_orders.py`):

```python
import pyspark.sql.functions as F

def model(dbt, session):
    # dbt.ref() resolves model dependencies and handles incremental filtering
    dbt.config(
        materialized="incremental",
        unique_key="order_id"
    )

    orders     = dbt.ref("stg_orders")
    order_lines = dbt.ref("stg_order_lines")

    # Join and enrich
    fact = orders.join(order_lines, on="order_id", how="inner") \
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))

    # Incremental filter: only process records newer than the last run
    if dbt.is_incremental():
        max_loaded = session.sql(
            f"SELECT MAX(order_date) FROM {dbt.this}"
        ).collect()[0][0]
        fact = fact.filter(F.col("order_date") > max_loaded)

    return fact
```

Run the model:

```bash
# Run a specific model
dbt run --select fact_orders

# Full refresh (truncate and reload)
dbt run --select fact_orders --full-refresh
```

#### SQL Example

A SQL dbt incremental model (saved as `models/marts/fact_orders.sql`):

```sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    ol.product_id,
    ol.quantity,
    ol.unit_price,
    ol.quantity * ol.unit_price AS revenue
FROM {{ ref('stg_orders') }}          o
JOIN {{ ref('stg_order_lines') }}     ol USING (order_id)

{% if is_incremental() %}
  -- On incremental runs, only process new records
  WHERE o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

### Discussion and Concerns

- **SQL model execution target:** dbt SQL models run on the SQL Warehouse configured in `profiles.yml`. A SQL Warehouse must be running before `dbt run` is invoked. Use Databricks Jobs to start the warehouse automatically as part of a scheduled pipeline.
- **Python dbt models require a Spark cluster:** Python dbt models use the PySpark API and must run on a Spark cluster (not a SQL Warehouse). Configure a cluster `http_path` in `profiles.yml` or use dbt's `--target` flag to route Python models to a separate cluster profile.
- **Incremental models are more cost-efficient for large tables:** A full-refresh model truncates and reloads the entire table on every run. For large Gold tables, this is expensive. Incremental models process only new or changed rows, which is faster and cheaper. The trade-off is complexity: the incremental filter logic must be correct, and full-refresh is occasionally needed to fix historical data or schema changes.
- **Testing:** Add dbt tests to `models/marts/fact_orders.yml` to validate the model after each run: `unique` on `order_id`, `not_null` on `order_id` and `order_date`, and a custom row count assertion for unexpected drops.

### See Also

- [dbt incremental models documentation](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt Python models on Databricks](https://docs.getdbt.com/docs/build/python-models)
- [dbt-databricks adapter setup](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)

---

## dbt Incremental Strategies

dbt's incremental materialisation supports three strategies for Delta tables on Databricks. Choosing the wrong strategy can result in duplicate records, unnecessary full table scans, or poor performance on large tables.

### Problem

A Gold `fact_web_events` table receives 50 million new rows per day. The dbt model must be incremental — loading only new records on each run. The team needs to understand which of the three available strategies (`append`, `merge`, `insert_overwrite`) is appropriate for their load pattern and what the trade-offs are.

### Solution

Configure the strategy in the model's `config` block. The three strategies have distinct behaviours.

#### Python Example

Reset a model to a full reload when strategy or logic changes:

```bash
# Full refresh: drop and recreate the table from scratch
dbt run --select fact_web_events --full-refresh
```

#### SQL Example

**Strategy 1: `append` — insert only, no deduplication**

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp,
    page_url
FROM {{ ref('stg_web_events') }}

{% if is_incremental() %}
  WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
```

**Strategy 2: `merge` — upsert using a unique key**

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id'
    )
}}

SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp,
    page_url
FROM {{ ref('stg_web_events') }}

{% if is_incremental() %}
  WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
```

**Strategy 3: `insert_overwrite` — replace entire partitions**

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['date(event_timestamp)']
    )
}}

SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp,
    page_url
FROM {{ ref('stg_web_events') }}
```

### Discussion and Concerns

- **`append` — fastest, but creates duplicates on re-run:** If the pipeline fails midway and is re-run, `append` will insert duplicate records for the rows already written in the failed run. Use `append` only when: (1) the source guarantees exactly-once delivery, or (2) duplicate records are acceptable and deduplicated at query time.
- **`merge` — safest, but slowest:** `merge` uses a Delta MERGE under the hood, which requires scanning the target table to find matching rows. On very large tables, this is significantly slower than `append` or `insert_overwrite`. Z-ORDER the target table on the `unique_key` column to improve MERGE performance. `merge` is the correct strategy when: the source may send duplicates, late-arriving records must update previously loaded rows, or re-run safety is required.
- **`insert_overwrite` — good middle ground for partitioned tables:** `insert_overwrite` replaces entire partitions atomically. It is faster than `merge` (no row-level matching) and safe on re-run (re-running the same partition is idempotent). Use it when: the table is partitioned by date, each partition is fully defined by one pipeline run, and no row-level updates are needed within a partition.
- **`full-refresh` behaviour:** When `--full-refresh` is passed, dbt drops and recreates the target table regardless of strategy. This is appropriate after breaking schema changes or to recover from data corruption, but should not be part of a regular production schedule for large tables.

### See Also

- [dbt incremental strategies for Databricks](https://docs.getdbt.com/docs/build/incremental-strategy)
- [Delta Lake MERGE performance tuning](https://docs.databricks.com/en/delta/merge.html#performance-tuning)
- [dbt Models on Databricks — previous method in this cookbook](#dbt-models-on-databricks)

---

## dbt Snapshots for SCD Type 2

dbt snapshots automate SCD Type 2 history tracking without requiring the developer to write the MERGE logic manually. dbt manages the `dbt_valid_from`, `dbt_valid_to`, `dbt_updated_at`, and `dbt_scd_id` columns and handles expiring old records and inserting new versions on each snapshot run.

### Problem

A `dim_customer` dimension needs SCD Type 2 history tracking on `segment` and `region` attributes. Writing and maintaining the two-pass MERGE logic manually is error-prone. The team wants a repeatable, version-controlled solution that handles the history logic automatically.

### Solution

Define a dbt snapshot in the `snapshots/` directory of the dbt project. Run it with `dbt snapshot`.

#### Python Example

Run the snapshot:

```bash
# Run all snapshots
dbt snapshot

# Run a specific snapshot
dbt snapshot --select snap_customers

# Include snapshots in a full pipeline run (via orchestration)
dbt snapshot && dbt run
```

#### SQL Example

Snapshot definition saved as `snapshots/snap_customers.sql`:

```sql
{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        target_database='catalog',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    email,
    region,
    segment,
    updated_at
FROM {{ source('silver', 'customers') }}

{% endsnapshot %}
```

dbt will create and manage `catalog.snapshots.snap_customers` with the following additional columns automatically:

| Column | Description |
|---|---|
| `dbt_scd_id` | Unique hash identifier for each historical row |
| `dbt_updated_at` | Timestamp of when dbt last processed this row |
| `dbt_valid_from` | Effective start date of this version |
| `dbt_valid_to` | Effective end date (NULL for the current version) |

Query the current version:

```sql
SELECT * FROM catalog.snapshots.snap_customers
WHERE dbt_valid_to IS NULL;
```

Query the version active at a specific point in time:

```sql
SELECT * FROM catalog.snapshots.snap_customers
WHERE dbt_valid_from <= '2024-06-01'
  AND (dbt_valid_to > '2024-06-01' OR dbt_valid_to IS NULL);
```

### Discussion and Concerns

- **`dbt_scd_id` is not a business key:** The `dbt_scd_id` is a hash of the row's values at a point in time. It changes if the row is reprocessed and should never be used as a join key in downstream models. Use `customer_id` + `dbt_valid_from` as the composite identifier for a specific historical version.
- **Snapshots grow indefinitely:** Each `dbt snapshot` run adds rows for every changed record. There is no built-in purge mechanism. Monitor the snapshot table size and define a retention policy (e.g., archive rows with `dbt_valid_to` older than 7 years) if long-term storage cost is a concern.
- **`dbt snapshot` runs separately from `dbt run`:** Snapshots are not included in `dbt run`. They must be run with `dbt snapshot` as a separate step. In orchestration (e.g., Databricks Jobs or Airflow), run `dbt snapshot` before `dbt run` to ensure snapshot tables are up to date before downstream models that depend on them are executed.
- **`invalidate_hard_deletes`:** When set to `True`, dbt sets `dbt_valid_to` to the current timestamp for any row in the source that has been deleted since the last snapshot run. Without this setting, deleted source records remain open-ended in the snapshot, which can cause incorrect results in downstream models that filter on `dbt_valid_to IS NULL`.

### See Also

- [dbt snapshots documentation](https://docs.getdbt.com/docs/build/snapshots)
- [dbt snapshot configuration reference](https://docs.getdbt.com/reference/snapshot-configs)
- [Processing Architectural Patterns — SCD vs. Satellite design](./processing_patterns.md)

---

## dbt Macros and dbt-utils

dbt macros are reusable Jinja-SQL functions defined in the `macros/` directory of a dbt project. They reduce duplication across models, enforce consistent logic (e.g., a standard way to compute fiscal quarter), and make complex SQL readable. The `dbt-utils` package provides a library of pre-built utility macros for common operations like pivoting, generating date spines, and testing column relationships.

### Problem

Multiple Gold models need to apply the same fiscal quarter calculation logic. Two models need pivoting (converting row values into columns) and one model needs a dense date spine to ensure no dates are missing from a time series. Copy-pasting the SQL for each model is error-prone and hard to maintain.

### Solution

Define a custom macro for the fiscal quarter logic. Use `dbt_utils.pivot()` for the pivot operation and `dbt_utils.date_spine()` for the date spine.

#### Python Example

Run the models that use macros and verify:

```bash
# Run models that use macros
dbt run --select +gold_sales_pivot +gold_date_spine

# Test the output of the models
dbt test --select gold_sales_pivot gold_date_spine
```

#### SQL Example

**Define a custom macro** in `macros/fiscal_quarter.sql`:

```sql
{% macro fiscal_quarter(date_col) %}
    CASE
        WHEN MONTH({{ date_col }}) IN (4, 5, 6)  THEN 'Q1'
        WHEN MONTH({{ date_col }}) IN (7, 8, 9)  THEN 'Q2'
        WHEN MONTH({{ date_col }}) IN (10, 11, 12) THEN 'Q3'
        WHEN MONTH({{ date_col }}) IN (1, 2, 3)  THEN 'Q4'
    END
{% endmacro %}
```

**Use the macro in a model** (`models/marts/gold_sales_summary.sql`):

```sql
SELECT
    order_id,
    order_date,
    revenue,
    {{ fiscal_quarter('order_date') }} AS fiscal_quarter
FROM {{ ref('fact_orders') }}
```

**Use `dbt_utils.pivot()`** to pivot product category revenue into columns (`models/marts/gold_sales_pivot.sql`):

```sql
SELECT
    order_month,
    {{ dbt_utils.pivot(
        column     = 'category',
        values     = ['Electronics', 'Clothing', 'Books'],
        agg        = 'SUM',
        then_value = 'revenue',
        else_value = '0',
        quote_identifiers = false
    ) }}
FROM {{ ref('fact_orders') }}
GROUP BY order_month
```

**Use `dbt_utils.date_spine()`** to generate a complete date series (`models/marts/gold_date_spine.sql`):

```sql
WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2023-01-01' as date)",
        end_date   = "cast('2025-12-31' as date)"
    ) }}
)
SELECT
    date_day,
    YEAR(date_day)  AS year,
    MONTH(date_day) AS month,
    {{ fiscal_quarter('date_day') }} AS fiscal_quarter
FROM date_spine
```

**Declare `dbt-utils` in `packages.yml`:**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Install packages before running:

```bash
dbt deps
```

### Discussion and Concerns

- **Test macros in isolation:** Macros are Jinja-templated SQL compiled at runtime. A syntax error in a macro will cause all models that use it to fail. Use `dbt compile --select <model>` to render the compiled SQL without executing it — this is the fastest way to validate macro output before a full run.
- **Version compatibility:** The dbt-utils package version must be compatible with the installed dbt-core version. Check the [dbt-utils changelog](https://github.com/dbt-labs/dbt-utils/blob/main/CHANGELOG.md) for the compatible version range and pin it explicitly in `packages.yml`. A mismatch causes `dbt deps` to fail with a resolution error.
- **`dbt_utils.pivot()` column list is static:** The `values` parameter in `dbt_utils.pivot()` must be a hardcoded list — it cannot be dynamically derived from query results at compile time. If the set of pivot values changes, the model must be updated and re-run. For dynamic pivoting, use PySpark with `groupBy().pivot().agg()`, which resolves pivot values at runtime.
- **`dbt_utils.date_spine()` and calendar tables:** `date_spine` generates a sequence of dates but does not include business calendar attributes (holidays, fiscal periods). Join the output to a manually maintained calendar dimension for full calendar enrichment.

### See Also

- [dbt macros documentation](https://docs.getdbt.com/docs/build/jinja-macros)
- [dbt-utils package on dbt Hub](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)
- [dbt compile command](https://docs.getdbt.com/reference/commands/compile)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| dbt job run status | Databricks Jobs UI — job run history; `system.lakeflow.job_run_timeline` | Failed runs, retry counts, jobs exceeding SLA duration |
| dbt test failures | Job run logs in Databricks Jobs UI; dbt Cloud (if in use) run results tab | Any test failure should block downstream models via `on-run-fail` hooks |
| Delta table version history | `DESCRIBE HISTORY catalog.silver.customers` | Unexpected write gaps (no new version when pipeline should have run), accidental full overwrites |
| Row count trend | Custom dbt test using `dbt_utils.recency` or a monitoring query on table row counts over time | Sudden drops in row count may indicate an upstream ingestion failure or incorrect incremental filter |
| MERGE execution time | Spark UI job timeline in Databricks (click through from Jobs run); `system.query.history` for SQL Warehouse jobs | MERGE jobs exceeding SLA threshold — indicates table needs OPTIMIZE/Z-ORDER or AQE tuning |
| Data freshness | `DESCRIBE DETAIL catalog.silver.customers` — `lastModified` timestamp; dbt `source freshness` command | Tables not refreshed within expected cadence |
| Deletion vector compaction | `DESCRIBE DETAIL` — `numDeletionVectorRows` | High deletion vector row count indicates OPTIMIZE is needed to compact files |

### Metrics for Success

- [ ] dbt tests pass on every scheduled run — `unique`, `not_null`, and `relationships` tests produce zero failures in job logs
- [ ] No unexpected nulls in Silver key columns — `not_null` test on all primary and foreign key columns in staging and mart models
- [ ] Deduplication check passes: row count after deduplication equals the count of distinct values on the deduplication key column
- [ ] SCD Type 2 dimension tables have exactly one `is_current = true` record per business key — validated by a custom dbt test or a post-run assertion query
- [ ] MERGE execution time for all incremental models is within the defined SLA (e.g., under 15 minutes for a 500 million row fact table with Z-ORDER on the merge key)
- [ ] dbt snapshot tables have no open-ended records for business keys that no longer exist in the source (if `invalidate_hard_deletes = True` is configured)
- [ ] Gold table row counts are non-decreasing on each daily run (unless a deliberate delete or correction run has been executed) — monitored via a row count assertion in the pipeline orchestration
