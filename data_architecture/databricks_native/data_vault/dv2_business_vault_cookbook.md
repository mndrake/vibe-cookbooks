# Data Vault 2.0 Business Vault Cookbook

## Databricks Native Stack

> This file is the **Databricks-native** version of the Data Vault 2.0 Business Vault cookbook.
> It uses Delta Live Tables (DLT), PySpark, DeltaTable MERGE, and Spark SQL exclusively.
> No dbt, AutomateDV, or dbt-utils dependencies are required.
>
> Equivalent dbt + AutomateDV version: [../../../databricks_and_dbt/data_vault/dv2_business_vault_cookbook.md](../../../databricks_and_dbt/data_vault/dv2_business_vault_cookbook.md)

---

## Introduction

This cookbook provides practical, step-by-step guidance for **building the Business Vault layer** on Databricks using the native stack. The Business Vault sits between the Raw Vault and the Information Mart: it applies business rules, derives metrics, and pre-computes complex join paths (via PIT and Bridge tables) that make mart queries fast and maintainable.

All Business Vault models read from Raw Vault structures. They never modify Raw Vault tables and are always rebuilt or incrementally maintained separately.

PIT and Bridge tables are implemented using native PySpark window functions and DeltaTable MERGE operations instead of AutomateDV macros. Derived business rules are implemented as native Spark SQL `CREATE OR REPLACE TABLE AS SELECT` or DLT Python views.

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.10+ | Required for Databricks Asset Bundles CLI |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | 0.200+ | Bundle deployment and workspace interaction |
| `delta-spark` | Bundled with Databricks Runtime | `DeltaTable` API — no separate install needed |
| Delta Live Tables runtime | Current channel | Provided by Databricks — no installation needed |

### Getting a New Starter Project

```bash
databricks bundle init
# Configure databricks.yml, then:
databricks bundle validate
databricks bundle deploy --target dev
```

### Getting the Source for an Existing Project

```bash
git clone <repository_url>
cd data_vault_bundle
databricks bundle validate
databricks bundle deploy --target dev
```

---

## Infrastructure Pre-Requisites

### Infrastructure Required

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment | Unity Catalog enabled |
| DLT Pipeline (serverless or classic) | Compute for business vault pipeline | Photon enabled |
| Unity Catalog — `business_vault` schema | Target for business vault tables | Pipeline service principal needs `CREATE TABLE`, `INSERT`, `SELECT` |
| Unity Catalog — `raw_vault` schema | Source for all business vault models | `SELECT` privilege required |

### Business Vault Schema Setup

```sql
-- Create the business_vault schema
CREATE SCHEMA IF NOT EXISTS main.business_vault
  COMMENT 'Data Vault 2.0 business vault — derived rules, PIT, and bridge tables';

-- Grant privileges to the DLT pipeline service principal
GRANT USE SCHEMA, CREATE TABLE, INSERT, SELECT ON SCHEMA main.business_vault
  TO `dlt-pipeline-service-principal@your-org.com`;

-- Data engineers get SELECT for debugging
GRANT USE SCHEMA, SELECT ON SCHEMA main.business_vault
  TO `data-engineers@your-org.com`;
```

---

## Derived Business Rules

Derived business rules read from Raw Vault structures (typically satellites or hubs) and add computed columns, derived identifiers, or soft business logic without modifying any Raw Vault table.

### Problem

The customer satellite stores `CUSTOMER_NAME` in a comma-separated `LAST_NAME, FIRST_NAME` format because that is how the source CRM delivers it. Downstream consumers all need `FULL_NAME`. Similarly, a canonical `EMAIL_DOMAIN` must be derived from `EMAIL_ADDRESS` for segmentation. Adding this derivation in every downstream query is a maintenance burden — it must live in a single, reusable business vault model.

### Solution

Create a DLT Python view or `CREATE OR REPLACE TABLE` that SELECTs from the most recent satellite version and adds computed columns.

#### SQL Example — Derived customer attributes (Spark SQL)

```sql
-- Business Vault: Derived customer attributes.
-- Reads the latest version of each customer's attributes from the satellite,
-- then adds FULL_NAME (concatenated) and EMAIL_DOMAIN (extracted).
-- This model does NOT modify sat_customer_details.

CREATE OR REPLACE TABLE main.business_vault.bv_customer_derived AS

WITH latest_customer AS (
    SELECT
        CUSTOMER_HK,
        CUSTOMER_NAME,
        EMAIL_ADDRESS,
        PHONE_NUMBER,
        BILLING_ADDRESS,
        CITY,
        POSTCODE,
        COUNTRY_CODE,
        LOAD_DATE,
        RECORD_SOURCE,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_HK
            ORDER BY LOAD_DATE DESC
        ) AS rn
    FROM main.raw_vault.sat_customer_details
),

current_customer AS (
    SELECT * FROM latest_customer WHERE rn = 1
)

SELECT
    c.CUSTOMER_HK,
    h.CUSTOMER_ID,

    -- Derived: full name from satellite payload columns (LAST_NAME, FIRST_NAME format)
    TRIM(CONCAT(
        COALESCE(TRIM(SPLIT(c.CUSTOMER_NAME, ',')[1]), ''), ' ',
        COALESCE(TRIM(SPLIT(c.CUSTOMER_NAME, ',')[0]), '')
    ))                                              AS FULL_NAME,

    c.CUSTOMER_NAME,
    c.EMAIL_ADDRESS,

    -- Derived: extract email domain for segmentation
    LOWER(SPLIT(c.EMAIL_ADDRESS, '@')[1])           AS EMAIL_DOMAIN,

    c.PHONE_NUMBER,
    c.BILLING_ADDRESS,
    c.CITY,
    c.POSTCODE,
    c.COUNTRY_CODE,
    c.LOAD_DATE                                     AS LAST_UPDATED

FROM current_customer c
INNER JOIN main.raw_vault.hub_customer h
    ON c.CUSTOMER_HK = h.CUSTOMER_HK;
```

#### Python Example — DLT view

```python
# DLT Python model equivalent for bv_customer_derived.
# Declare as a DLT view so downstream DLT tables can reference it with dlt.read().

import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window


@dlt.view(
    name="bv_customer_derived",
    comment="Current-state derived customer attributes from the raw vault satellite"
)
def bv_customer_derived():
    sat = dlt.read("sat_customer_details")
    hub = spark.table("main.raw_vault.hub_customer")

    # Select most recent satellite row per customer
    window = Window.partitionBy("CUSTOMER_HK").orderBy(F.col("LOAD_DATE").desc())
    current_customer = (
        sat
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Derive FULL_NAME (LAST_NAME, FIRST_NAME → "FIRST LAST") and EMAIL_DOMAIN
    derived = (
        current_customer
        .withColumn(
            "FULL_NAME",
            F.trim(F.concat_ws(
                " ",
                F.trim(F.element_at(F.split(F.col("CUSTOMER_NAME"), ","), 2)),
                F.trim(F.element_at(F.split(F.col("CUSTOMER_NAME"), ","), 1))
            ))
        )
        .withColumn(
            "EMAIL_DOMAIN",
            F.lower(F.element_at(F.split(F.col("EMAIL_ADDRESS"), "@"), 2))
        )
    )

    return derived.join(
        hub.select("CUSTOMER_HK", "CUSTOMER_ID"),
        on="CUSTOMER_HK",
        how="inner"
    ).select(
        "CUSTOMER_HK", "CUSTOMER_ID", "FULL_NAME", "CUSTOMER_NAME",
        "EMAIL_ADDRESS", "EMAIL_DOMAIN", "PHONE_NUMBER",
        "BILLING_ADDRESS", "CITY", "POSTCODE", "COUNTRY_CODE",
        F.col("LOAD_DATE").alias("LAST_UPDATED")
    )
```

**Functional difference between SQL and Python DLT approaches:** The SQL `CREATE OR REPLACE TABLE` is executed as a Databricks Workflows task and runs on a SQL Warehouse or cluster. The DLT Python `@dlt.view` is declared in a DLT pipeline notebook and runs on the DLT cluster. Use the DLT Python approach when the business vault is part of a DLT pipeline that also loads the raw vault. Use the Workflows SQL task approach for standalone refresh jobs.

#### Validation — SQL

```sql
-- Verify no null FULL_NAME values (indicates CUSTOMER_NAME is null in the satellite)
SELECT COUNT(*) AS null_full_name_count
FROM main.business_vault.bv_customer_derived
WHERE FULL_NAME IS NULL OR TRIM(FULL_NAME) = '';
-- Expected: 0 (investigate source if non-zero)

-- Verify EMAIL_DOMAIN is well-formed
SELECT COUNT(*) AS malformed_email_domain
FROM main.business_vault.bv_customer_derived
WHERE EMAIL_DOMAIN IS NOT NULL
  AND EMAIL_DOMAIN NOT LIKE '%.%';
-- Expected: 0
```

### Discussion and Concerns

- **Never modify Raw Vault tables for business rules:** If a business rule is applied by updating or inserting into a satellite or hub, the audit trail is corrupted. Business rules live exclusively in Business Vault models that SELECT from Raw Vault.
- **Materialise as a Delta table, not a view:** Business vault models that are queried by PIT or Bridge refresh jobs should be materialised as Delta tables for performance. Live views that reach back into raw vault satellites over large histories are expensive.
- **Document each rule:** Each computed column should include an inline SQL comment explaining the business logic. Rules change — without documentation, future engineers cannot safely modify them.
- **Full-refresh vs. incremental:** If the derived model reads from the full satellite history to compute a current-state snapshot (like `bv_customer_derived` above), it must be a full `CREATE OR REPLACE TABLE`. If it derives attributes row-by-row from a satellite without look-back, it can be built as an append-only DLT streaming table.

### See Also

- [Delta Live Tables Python API](https://docs.databricks.com/en/delta-live-tables/python-ref.html)
- [dv2_raw_vault_cookbook.md — Satellite Loading](./dv2_raw_vault_cookbook.md)
- [dv2_architecture.md — Business Vault](./dv2_architecture.md)

---

## Point-in-Time (PIT) Tables

Point-in-Time tables are Business Vault structures that resolve the correct satellite version for a given Hub record at any given point in time. They eliminate the need for complex multi-way joins and window functions in every downstream mart query.

### Problem

Querying a customer's attributes as they were on a specific date requires joining the hub to multiple satellites and applying a `WHERE LOAD_DATE <= target_date` filter with a `MAX(LOAD_DATE)` per customer per satellite. Across five satellites, this produces five separate window function subqueries. Every downstream mart model repeats this logic, making the codebase fragile and expensive to query.

### Solution

Build a PIT table natively using a date spine generated with `SEQUENCE + EXPLODE` (or a pre-built calendar table) and window functions. For each customer and each snapshot date, store the correct `LOAD_DATE` from each satellite. Downstream mart models join to the PIT table and then look up the satellite row at exactly the stored load date — a simple equality join rather than a range scan with aggregation.

#### SQL Example — PIT table for the CUSTOMER hub

```sql
-- Point-in-Time table for the CUSTOMER hub.
-- For each date in the snapshot window and each customer, stores the LOAD_DATE
-- of the correct row in each satellite as of that date.
-- Refresh this table on the same schedule as the mart refresh.

CREATE OR REPLACE TABLE main.business_vault.pit_customer AS

WITH date_spine AS (
    -- Generate one row per day from the earliest customer load date to today
    SELECT EXPLODE(SEQUENCE(
        (SELECT CAST(MIN(LOAD_DATE) AS DATE) FROM main.raw_vault.hub_customer),
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS AS_OF_DATE
),

all_customers AS (
    SELECT CUSTOMER_HK, CAST(LOAD_DATE AS DATE) AS FIRST_SEEN_DATE
    FROM main.raw_vault.hub_customer
),

-- Cross join customers with dates — include only dates on/after first appearance
customer_date_grid AS (
    SELECT c.CUSTOMER_HK, d.AS_OF_DATE
    FROM all_customers c
    JOIN date_spine d ON d.AS_OF_DATE >= c.FIRST_SEEN_DATE
),

-- For each satellite, find the latest LOAD_DATE that is <= the snapshot date
pit_details AS (
    SELECT
        g.CUSTOMER_HK,
        g.AS_OF_DATE,
        MAX(s.LOAD_DATE) AS SAT_CUSTOMER_DETAILS_LDTS
    FROM customer_date_grid g
    LEFT JOIN main.raw_vault.sat_customer_details s
        ON g.CUSTOMER_HK = s.CUSTOMER_HK
       AND CAST(s.LOAD_DATE AS DATE) <= g.AS_OF_DATE
    GROUP BY g.CUSTOMER_HK, g.AS_OF_DATE
),

pit_marketing AS (
    SELECT
        g.CUSTOMER_HK,
        g.AS_OF_DATE,
        MAX(s.LOAD_DATE) AS SAT_CUSTOMER_MARKETING_LDTS
    FROM customer_date_grid g
    LEFT JOIN main.raw_vault.sat_customer_marketing s
        ON g.CUSTOMER_HK = s.CUSTOMER_HK
       AND CAST(s.LOAD_DATE AS DATE) <= g.AS_OF_DATE
    GROUP BY g.CUSTOMER_HK, g.AS_OF_DATE
),

pit_preferences AS (
    SELECT
        g.CUSTOMER_HK,
        g.AS_OF_DATE,
        MAX(s.LOAD_DATE) AS SAT_CUSTOMER_PREFERENCES_LDTS
    FROM customer_date_grid g
    LEFT JOIN main.raw_vault.sat_customer_preferences s
        ON g.CUSTOMER_HK = s.CUSTOMER_HK
       AND CAST(s.LOAD_DATE AS DATE) <= g.AS_OF_DATE
    GROUP BY g.CUSTOMER_HK, g.AS_OF_DATE
)

SELECT
    d.CUSTOMER_HK,
    d.AS_OF_DATE,
    d.SAT_CUSTOMER_DETAILS_LDTS,
    m.SAT_CUSTOMER_MARKETING_LDTS,
    p.SAT_CUSTOMER_PREFERENCES_LDTS
FROM pit_details d
INNER JOIN pit_marketing   m ON d.CUSTOMER_HK = m.CUSTOMER_HK AND d.AS_OF_DATE = m.AS_OF_DATE
INNER JOIN pit_preferences p ON d.CUSTOMER_HK = p.CUSTOMER_HK AND d.AS_OF_DATE = p.AS_OF_DATE;
```

#### Python Example — PIT construction with PySpark

```python
# PySpark PIT construction — use when the number of satellites is dynamic
# or when PIT is built inside a DLT pipeline task.

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import date

def build_pit_customer(spark, snapshot_date: date = None):
    """
    Build or refresh the pit_customer table.
    snapshot_date: if provided, build a single-day PIT for incremental refresh.
                   If None, rebuild the full history.
    """
    hub = spark.table("main.raw_vault.hub_customer")
    sat_details = spark.table("main.raw_vault.sat_customer_details")
    sat_marketing = spark.table("main.raw_vault.sat_customer_marketing")
    sat_preferences = spark.table("main.raw_vault.sat_customer_preferences")

    # Build date spine
    if snapshot_date:
        start_str = snapshot_date.strftime("%Y-%m-%d")
        end_str = snapshot_date.strftime("%Y-%m-%d")
    else:
        start_str = hub.agg(F.min(F.col("LOAD_DATE").cast("date"))).collect()[0][0]
        end_str = date.today().strftime("%Y-%m-%d")

    dates_df = spark.sql(f"""
        SELECT EXPLODE(SEQUENCE(DATE '{start_str}', DATE '{end_str}', INTERVAL 1 DAY))
        AS AS_OF_DATE
    """)

    # Customer grid
    customers = hub.select(
        "CUSTOMER_HK",
        F.col("LOAD_DATE").cast("date").alias("FIRST_SEEN_DATE")
    )
    grid = customers.crossJoin(dates_df).filter(
        F.col("AS_OF_DATE") >= F.col("FIRST_SEEN_DATE")
    )

    def resolve_satellite(sat_df, hk_col, ldts_alias):
        """
        For each (CUSTOMER_HK, AS_OF_DATE) in the grid, find the latest satellite
        LOAD_DATE that is <= AS_OF_DATE.
        """
        sat = sat_df.select(
            F.col(hk_col).alias("CUSTOMER_HK"),
            F.col("LOAD_DATE").cast("date").alias("SAT_DATE"),
            "LOAD_DATE"
        )
        joined = grid.alias("g").join(
            sat.alias("s"),
            (F.col("g.CUSTOMER_HK") == F.col("s.CUSTOMER_HK")) &
            (F.col("s.SAT_DATE") <= F.col("g.AS_OF_DATE")),
            how="left"
        )
        return joined.groupBy("g.CUSTOMER_HK", "g.AS_OF_DATE").agg(
            F.max("s.LOAD_DATE").alias(ldts_alias)
        )

    pit_details = resolve_satellite(sat_details, "CUSTOMER_HK", "SAT_CUSTOMER_DETAILS_LDTS")
    pit_marketing = resolve_satellite(sat_marketing, "CUSTOMER_HK", "SAT_CUSTOMER_MARKETING_LDTS")
    pit_preferences = resolve_satellite(sat_preferences, "CUSTOMER_HK", "SAT_CUSTOMER_PREFERENCES_LDTS")

    result = (
        pit_details
        .join(pit_marketing,   on=["CUSTOMER_HK", "AS_OF_DATE"])
        .join(pit_preferences, on=["CUSTOMER_HK", "AS_OF_DATE"])
    )

    result.write.format("delta").mode("overwrite").saveAsTable("main.business_vault.pit_customer")
    print(f"pit_customer refreshed — {result.count()} rows")
```

**Functional difference between SQL and Python approaches:** The SQL `CREATE OR REPLACE TABLE` is suitable for full-history PIT rebuilds scheduled as a Databricks Workflows SQL task. The Python function is more flexible: it supports incremental single-day refresh (pass `snapshot_date`) and can be embedded in a DLT pipeline. For large customer bases, the incremental Python approach is significantly faster.

#### Validation — SQL

```sql
-- Verify PIT table has one row per (CUSTOMER_HK, AS_OF_DATE)
SELECT CUSTOMER_HK, AS_OF_DATE, COUNT(*) AS cnt
FROM main.business_vault.pit_customer
GROUP BY CUSTOMER_HK, AS_OF_DATE
HAVING cnt > 1;
-- Expected: 0 rows

-- Verify no null satellite LOAD_DATE pointers for customers with known satellite data
-- (A null pointer means the customer had no satellite record as of that date — acceptable
-- for dates before the customer first appeared, but not after)
SELECT p.CUSTOMER_HK, p.AS_OF_DATE,
       p.SAT_CUSTOMER_DETAILS_LDTS
FROM main.business_vault.pit_customer p
INNER JOIN main.raw_vault.hub_customer h
    ON p.CUSTOMER_HK = h.CUSTOMER_HK
WHERE p.AS_OF_DATE >= CAST(h.LOAD_DATE AS DATE)
  AND p.SAT_CUSTOMER_DETAILS_LDTS IS NULL;
-- Expected: 0 rows (customers should have a satellite record from their first load date)
```

### Discussion and Concerns

- **PIT tables do not store payload:** The PIT table stores pointers (load dates) to the correct satellite version for each snapshot date. The actual attribute values are retrieved by joining the PIT load date to the satellite in mart models. This design keeps PIT tables compact.
- **Refresh schedule must align with mart refresh:** A stale PIT table causes the mart to serve outdated data. In Databricks Workflows, schedule the PIT refresh as an upstream task in the mart pipeline job, using task dependencies to enforce ordering.
- **Materialise as a Delta table:** A view that executes window functions on-the-fly across large satellite histories is prohibitively slow. Always materialise as a Delta table and partition by `AS_OF_DATE` for large tables.
- **Include only required satellites:** Every satellite added to a PIT table increases the table width and the refresh cost. Include only the satellites that are actually consumed by downstream marts.
- **Date spine granularity:** The example generates one PIT row per day per customer. For hourly reporting, use `INTERVAL 1 HOUR` — but be aware this multiplies the PIT table size by 24.

### See Also

- [Databricks Workflows task dependencies](https://docs.databricks.com/en/jobs/index.html)
- [dv2_information_mart_cookbook.md](./dv2_information_mart_cookbook.md)
- [dv2_architecture.md — Business Vault](./dv2_architecture.md)

---

## Bridge Tables

Bridge tables pre-compute the multi-hop join path across the vault graph, spanning one or more Links to enable efficient mart queries that navigate relationships between multiple Hub entities.

### Problem

Building a fact table that reports on customer orders requires joining `hub_customer` → `link_customer_order` → `hub_order`. Adding line item detail requires extending the path to `link_order_line_item` → `hub_product`. Every mart model that needs customer-order-product grain must reproduce this four-way join. Changes to the vault structure (a new intermediate link) break every mart query simultaneously.

### Solution

Build a Bridge table natively by joining Hub and Link structures in a `CREATE OR REPLACE TABLE AS SELECT` statement (or a DLT Python table). The Bridge pre-computes the join path so mart models retrieve all relevant hash keys from a single bridge join rather than a multi-hop vault traversal.

#### SQL Example — Bridge spanning CUSTOMER → CUSTOMER_ORDER LINK → ORDER

```sql
-- Bridge table spanning CUSTOMER → CUSTOMER_ORDER link → ORDER.
-- Pre-computes the join path so mart models can retrieve all relevant hash keys
-- from a single bridge join rather than a multi-hop vault traversal.
-- Includes a snapshot date (AS_OF_DATE) so marts can filter to today.

CREATE OR REPLACE TABLE main.business_vault.bridge_customer_orders AS

WITH date_spine AS (
    SELECT EXPLODE(SEQUENCE(
        (SELECT CAST(MIN(LOAD_DATE) AS DATE) FROM main.raw_vault.hub_customer),
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS AS_OF_DATE
),

-- Active link relationships (effectivity satellite = open or no effectivity satellite)
active_links AS (
    SELECT
        l.CUSTOMER_ORDER_HK,
        l.CUSTOMER_HK,
        l.ORDER_HK,
        CAST(l.LOAD_DATE AS DATE) AS LINK_LOAD_DATE
    FROM main.raw_vault.link_customer_order l
    -- Optionally join effectivity satellite to filter open relationships only:
    -- LEFT JOIN main.raw_vault.eff_sat_customer_order e
    --     ON l.CUSTOMER_ORDER_HK = e.CUSTOMER_ORDER_HK
    -- WHERE e.EFF_END_DATE IS NULL  -- open relationship
),

-- Cross join link rows with dates that are >= the link load date
bridge_raw AS (
    SELECT
        al.CUSTOMER_ORDER_HK,
        al.CUSTOMER_HK,
        al.ORDER_HK,
        ds.AS_OF_DATE
    FROM active_links al
    JOIN date_spine ds ON ds.AS_OF_DATE >= al.LINK_LOAD_DATE
)

SELECT
    CUSTOMER_ORDER_HK,
    CUSTOMER_HK,
    ORDER_HK,
    AS_OF_DATE
FROM bridge_raw;
```

#### Python Example — DLT table for Bridge

```python
# DLT Python bridge table — declare in a DLT pipeline notebook.
# Refreshed as part of the business vault DLT pipeline.

import dlt
import pyspark.sql.functions as F
from datetime import date


@dlt.table(
    name="bridge_customer_orders",
    comment="Bridge spanning CUSTOMER → CUSTOMER_ORDER link → ORDER with daily snapshots"
)
def bridge_customer_orders():
    link = spark.table("main.raw_vault.link_customer_order")

    # Build date spine from earliest link load date to today
    earliest = link.agg(F.min(F.col("LOAD_DATE").cast("date"))).collect()[0][0]
    today = date.today()

    dates_df = spark.sql(f"""
        SELECT EXPLODE(SEQUENCE(DATE '{earliest}', DATE '{today}', INTERVAL 1 DAY))
        AS AS_OF_DATE
    """)

    # Active links with their load date as a date type
    active_links = link.select(
        "CUSTOMER_ORDER_HK",
        "CUSTOMER_HK",
        "ORDER_HK",
        F.col("LOAD_DATE").cast("date").alias("LINK_LOAD_DATE")
    )

    # Join with date spine — include only dates on/after link load date
    return (
        active_links.alias("l")
        .join(dates_df.alias("d"), F.col("d.AS_OF_DATE") >= F.col("l.LINK_LOAD_DATE"))
        .select("CUSTOMER_ORDER_HK", "CUSTOMER_HK", "ORDER_HK", "AS_OF_DATE")
    )
```

#### Python Example — Multi-hop Bridge (CUSTOMER → ORDER → PRODUCT)

```python
# Extend the bridge to span an additional link hop:
# CUSTOMER → CUSTOMER_ORDER link → ORDER → ORDER_LINE_ITEM link → PRODUCT

from pyspark.sql import functions as F

def build_bridge_customer_order_product(spark):
    link_co = spark.table("main.raw_vault.link_customer_order")
    link_ol = spark.table("main.raw_vault.link_order_line_item")

    today = date.today().strftime("%Y-%m-%d")

    # First hop: customer → order
    hop1 = link_co.select(
        "CUSTOMER_ORDER_HK",
        "CUSTOMER_HK",
        "ORDER_HK",
        F.col("LOAD_DATE").cast("date").alias("LOAD_DATE_HOP1")
    )

    # Second hop: order → product via line item link
    hop2 = link_ol.select(
        "ORDER_HK",
        "PRODUCT_HK",
        F.col("LOAD_DATE").cast("date").alias("LOAD_DATE_HOP2")
    )

    # Join hops — bridge spans both relationships
    bridge = hop1.join(hop2, on="ORDER_HK", how="inner")
    bridge = bridge.withColumn(
        "BRIDGE_LOAD_DATE",
        F.greatest("LOAD_DATE_HOP1", "LOAD_DATE_HOP2")
    )

    # Add snapshot date (today only — for incremental bridge refresh)
    result = bridge.withColumn("AS_OF_DATE", F.lit(today).cast("date"))

    result.select(
        "CUSTOMER_ORDER_HK", "CUSTOMER_HK", "ORDER_HK", "PRODUCT_HK",
        "AS_OF_DATE"
    ).write.format("delta").mode("overwrite").saveAsTable(
        "main.business_vault.bridge_customer_order_product"
    )
```

**Functional difference between SQL and Python approaches:** The SQL `CREATE OR REPLACE TABLE` is a full-history rebuild on every run. The Python DLT approach declares the bridge as a live table that DLT manages incrementally. For large link tables, the Python incremental approach (appending only today's new snapshot rows) avoids a full rebuild.

#### Validation — SQL

```sql
-- Verify bridge row count is >= link row count
-- (bridge has more rows if the date spine adds historical snapshots)
SELECT COUNT(*) AS bridge_rows FROM main.business_vault.bridge_customer_orders;
SELECT COUNT(*) AS link_rows   FROM main.raw_vault.link_customer_order;

-- Verify all ORDER_HK values in the bridge resolve to hub_order
SELECT b.CUSTOMER_ORDER_HK
FROM main.business_vault.bridge_customer_orders b
LEFT JOIN main.raw_vault.hub_order o ON b.ORDER_HK = o.ORDER_HK
WHERE o.ORDER_HK IS NULL;
-- Expected: 0 rows

-- Verify no gaps: every active link relationship appears in today's bridge
SELECT l.CUSTOMER_ORDER_HK
FROM main.raw_vault.link_customer_order l
LEFT JOIN main.business_vault.bridge_customer_orders b
    ON l.CUSTOMER_ORDER_HK = b.CUSTOMER_ORDER_HK
   AND b.AS_OF_DATE = CURRENT_DATE()
WHERE b.CUSTOMER_ORDER_HK IS NULL;
-- Expected: 0 rows (all links should appear in today's bridge snapshot)
```

### Discussion and Concerns

- **Refresh on the same schedule as PIT:** Bridge and PIT tables are consumed together by mart models. If bridge is stale relative to PIT, mart joins will miss rows. In Databricks Workflows, configure bridge and PIT refresh as parallel tasks that both must complete before mart tasks start.
- **Bridge tables can span multiple links:** Add additional JOIN hops to extend the join path (e.g., customer → order → line item → product). Each additional hop increases bridge width and refresh cost.
- **Bridge does not filter by effectivity by default:** A standard bridge table includes all link relationships. For mart models that need only currently active relationships, join the bridge to the effectivity satellite and filter on open/close dates.
- **Partition bridge tables by AS_OF_DATE:** For large bridge tables, partition by `AS_OF_DATE` and cluster by the primary Hub HK. Mart models filter by `AS_OF_DATE = CURRENT_DATE()`, which will partition prune efficiently.

### See Also

- [Delta Lake OPTIMIZE and ZORDER](https://docs.databricks.com/en/delta/optimizations/file-mgmt.html)
- [dv2_information_mart_cookbook.md](./dv2_information_mart_cookbook.md)

---

## Native Data Quality Checks for Business Vault

The native equivalent of dbt-utils test suites is Databricks SQL assertions, DLT `@dlt.expect_all_or_drop` constraints, and Databricks Lakehouse Monitoring.

### Problem

The business vault applies derived business rules and pre-computed join paths. If a business rule is violated (e.g., a customer tier is assigned an invalid value) or a grain constraint is broken (e.g., the PIT table has duplicate rows per customer per date), the mart will silently serve incorrect data to consumers. These failures must be detected and surfaced before the mart refresh runs.

### Solution

Implement data quality checks using DLT expectations (for real-time pipeline enforcement) or Databricks SQL assertions (for Workflows pipeline gates).

#### SQL Example — Assertion queries as pipeline gate

```sql
-- Run these assertions as individual SQL tasks in the Databricks Workflow job.
-- Configure the task to fail the job on non-zero result, blocking downstream mart tasks.

-- Grain check: one row per (CUSTOMER_HK, AS_OF_DATE) in pit_customer
SELECT COUNT(*) AS pit_grain_violations
FROM (
    SELECT CUSTOMER_HK, AS_OF_DATE, COUNT(*) AS cnt
    FROM main.business_vault.pit_customer
    GROUP BY CUSTOMER_HK, AS_OF_DATE
    HAVING cnt > 1
);
-- Job fails if result > 0

-- Email format check on bv_customer_derived
SELECT COUNT(*) AS email_without_at
FROM main.business_vault.bv_customer_derived
WHERE EMAIL_ADDRESS IS NOT NULL
  AND EMAIL_ADDRESS NOT LIKE '%@%';
-- Expected: 0

-- Referential integrity: all ORDER_HK in bridge resolve to hub_order
SELECT COUNT(*) AS orphan_order_hk
FROM main.business_vault.bridge_customer_orders b
LEFT JOIN main.raw_vault.hub_order o ON b.ORDER_HK = o.ORDER_HK
WHERE o.ORDER_HK IS NULL;
-- Expected: 0
```

To enforce this as a pipeline gate in a Databricks Workflow, use a Python task:

```python
# Python Workflows task: data quality gate for the business vault.
# Raises an exception if any assertion fails, blocking downstream tasks.

from databricks.sdk.runtime import *

assertions = {
    "pit_grain": """
        SELECT COUNT(*) AS cnt FROM (
            SELECT CUSTOMER_HK, AS_OF_DATE, COUNT(*) AS n
            FROM main.business_vault.pit_customer
            GROUP BY CUSTOMER_HK, AS_OF_DATE HAVING n > 1
        )
    """,
    "email_format": """
        SELECT COUNT(*) AS cnt
        FROM main.business_vault.bv_customer_derived
        WHERE EMAIL_ADDRESS IS NOT NULL AND EMAIL_ADDRESS NOT LIKE '%@%'
    """,
    "bridge_referential_integrity": """
        SELECT COUNT(*) AS cnt
        FROM main.business_vault.bridge_customer_orders b
        LEFT JOIN main.raw_vault.hub_order o ON b.ORDER_HK = o.ORDER_HK
        WHERE o.ORDER_HK IS NULL
    """,
}

failed = []
for name, query in assertions.items():
    count = spark.sql(query).collect()[0]["cnt"]
    if count > 0:
        failed.append(f"{name}: {count} violation(s)")

if failed:
    raise AssertionError(
        "Business vault data quality gate FAILED. "
        "Mart refresh blocked.\n" + "\n".join(failed)
    )

print("All business vault data quality assertions passed.")
```

#### Python Example — DLT expectations in the pipeline

```python
# DLT expectations enforce data quality at pipeline execution time.
# Rows that violate constraints are dropped or the pipeline is halted.

import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="bv_customer_derived_validated",
    comment="Derived customer attributes with DLT quality constraints enforced"
)
@dlt.expect_or_drop("valid_email", "EMAIL_ADDRESS LIKE '%@%'")
@dlt.expect_or_drop("non_null_customer_hk", "CUSTOMER_HK IS NOT NULL")
@dlt.expect_or_fail("no_duplicate_customers",
                    "COUNT(*) OVER (PARTITION BY CUSTOMER_HK) = 1")
def bv_customer_derived_validated():
    return dlt.read("bv_customer_derived")
```

**Functional difference between SQL assertions and DLT expectations:** SQL assertion queries in a Workflows pipeline gate run after the business vault tables are fully built and block downstream tasks. DLT expectations enforce constraints row-by-row at pipeline execution time — `expect_or_drop` removes violating rows silently (and logs them), while `expect_or_fail` halts the pipeline. Use SQL assertions for end-of-stage gates; use DLT expectations for row-level quality enforcement during loading.

#### Validation — SQL

```sql
-- Check the DLT event log for expectation violations
SELECT
    expectation_name,
    SUM(passed_records)  AS passed,
    SUM(failed_records)  AS failed
FROM (
    SELECT
        details:flow_progress:data_quality:dropped_records AS failed_records,
        details:flow_progress:data_quality:passed_records  AS passed_records,
        details:flow_progress:data_quality:expectations[0]:name AS expectation_name
    FROM event_log('<pipeline_id>')
    WHERE event_type = 'flow_progress'
      AND details:flow_progress:data_quality IS NOT NULL
)
GROUP BY expectation_name;
```

### Discussion and Concerns

- **`expect_or_fail` vs. `expect_or_drop` vs. SQL assertion:** `expect_or_fail` halts the pipeline and prevents any bad data from being written — use this for critical grain constraints. `expect_or_drop` silently removes bad rows and logs them — use this for advisory quality checks. SQL assertions in Workflows are the simplest option for post-load gates that block downstream tasks.
- **Run quality checks before mart refresh:** The Workflows job task order should be: (1) raw vault DLT pipeline, (2) business vault DLT pipeline or SQL tasks, (3) data quality assertion task, (4) mart refresh tasks. Configure task dependencies to enforce this ordering.
- **Monitor expectation violation counts over time:** Use Databricks Lakehouse Monitoring or a custom dashboard on the DLT event log to track violation trends. A sudden spike in `expect_or_drop` failures may indicate a source system data quality issue.

### See Also

- [Delta Live Tables data quality expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
- [Databricks Lakehouse Monitoring](https://docs.databricks.com/en/lakehouse-monitoring/index.html)
- [Databricks Workflows task dependencies](https://docs.databricks.com/en/jobs/index.html)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| PIT table row count | `SELECT COUNT(*) FROM main.business_vault.pit_customer` in scheduled query | Row count should grow by (new customers × days in schedule window) per run |
| Bridge table staleness | `SELECT MAX(AS_OF_DATE) FROM main.business_vault.bridge_customer_orders` | Should match current date after nightly refresh |
| Data quality gate result | Databricks Workflows job run status — assertion task exit code | Any assertion failure must trigger an alert and block mart refresh |
| DLT expectation violation counts | DLT Event Log / Databricks Lakehouse Monitoring | Target: 0 `expect_or_fail` violations; `expect_or_drop` violations reviewed weekly |
| Business vault refresh duration | Databricks Workflows UI / job run history | PIT and bridge refreshes grow over time — monitor for unexpected slowdowns |

### Metrics for Success

- [ ] PIT table is refreshed within the scheduled SLA (e.g., within 30 minutes of raw vault refresh completing)
- [ ] All data quality assertions pass on every pipeline run — zero failures
- [ ] DLT `expect_or_drop` violations are reviewed in the weekly data quality review
- [ ] Bridge tables are refreshed on the same schedule as PIT tables — never stale relative to PIT
- [ ] No business vault model directly reads from `staging` schema — all reads are from `raw_vault` only
