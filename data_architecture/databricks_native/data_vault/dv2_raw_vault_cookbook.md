# Data Vault 2.0 Raw Vault Cookbook

## Databricks Native Stack

> This file is the **Databricks-native** version of the Data Vault 2.0 raw vault cookbook.
> It uses Delta Live Tables (DLT), PySpark, DeltaTable MERGE, and Spark SQL exclusively.
> No dbt, AutomateDV, or dbt-utils dependencies are required.
>
> Equivalent dbt + AutomateDV version: [../../../databricks_and_dbt/data_vault/dv2_raw_vault_cookbook.md](../../../databricks_and_dbt/data_vault/dv2_raw_vault_cookbook.md)

---

## Introduction

This cookbook provides practical, step-by-step guidance for **loading Raw Vault structures** on Databricks using the native stack. It covers Hub, Link, Non-Historized Link, Satellite, Multi-Active Satellite, Effectivity Satellite, Transactional Link, and Reference structures — the complete set of building blocks required for a production Raw Vault.

All structures in this cookbook are append-only and idempotent. They read from staging views built using the patterns in [dv2_staging_cookbook.md](./dv2_staging_cookbook.md).

Hub and Link loading uses `DeltaTable.merge()` with `whenNotMatchedInsertAll()` (no update clause) to ensure append-only idempotency. Satellite loading uses an anti-join against the latest loaded hashdiff before appending new rows.

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
| DLT Pipeline (serverless or classic) | Compute for raw vault pipeline | Photon enabled |
| Unity Catalog — `raw_vault` schema | Target for vault tables | Pipeline service principal needs `CREATE TABLE`, `INSERT`, `SELECT` |
| Unity Catalog — `staging` schema | Source for staging views | `SELECT` privilege required |

### Raw Vault Schema Setup

```sql
-- Create the raw_vault schema
CREATE SCHEMA IF NOT EXISTS main.raw_vault
  COMMENT 'Data Vault 2.0 raw vault — structural, append-only, no business rules';

-- Grant privileges to the DLT pipeline service principal
GRANT USE SCHEMA, CREATE TABLE, INSERT, SELECT ON SCHEMA main.raw_vault
  TO `dlt-pipeline-service-principal@your-org.com`;

-- Data engineers get SELECT only — no modification of raw vault
GRANT USE SCHEMA, SELECT ON SCHEMA main.raw_vault
  TO `data-engineers@your-org.com`;
```

---

## Hub Loading

A Hub stores the first appearance of each unique business key across all contributing source systems. It is the identity anchor for a business entity in the vault.

### Problem

Multiple source systems — a CRM, an e-commerce platform, and a mobile application — each send customer records independently. Some customers exist in all three systems. Loading each source independently creates duplicate natural keys; joining them in an ad-hoc view is fragile and not auditable. The vault needs a single, deduplicated record for each unique customer identity, regardless of which source delivered it first.

### Solution

Use a `DeltaTable.merge()` with `whenNotMatchedInsertAll()` to load only new natural keys (those not already present in the hub). Because hubs are append-only, there is no update clause — a match on the hash key means "already exists, skip". Multiple staging sources are UNIONed before the merge.

#### Python Example — `src/raw_vault/hub_customer.py`

```python
# Hub for the CUSTOMER business entity.
# Loads from three staging sources: CRM, e-commerce, and mobile app.
# The hub stores the first appearance of each CUSTOMER_HK across all sources.
# Re-running this pipeline is safe — it is idempotent by design (MERGE WHEN NOT MATCHED).

import dlt
from delta.tables import DeltaTable
from pyspark.sql.functions import col

@dlt.table(
    name="hub_customer",
    comment="Hub — unique CUSTOMER business keys from all source systems",
    table_properties={"delta.appendOnly": "true"}
)
def hub_customer():
    # Union all staging sources before deduplication
    crm_customers = dlt.read_stream("stg_crm_customer").select(
        col("CUSTOMER_HK"), col("customer_id").alias("CUSTOMER_ID"),
        col("LOAD_DATE"), col("RECORD_SOURCE")
    )
    ecommerce_customers = dlt.read_stream("stg_ecommerce_customer").select(
        col("CUSTOMER_HK"), col("customer_id").alias("CUSTOMER_ID"),
        col("LOAD_DATE"), col("RECORD_SOURCE")
    )
    mobile_customers = dlt.read_stream("stg_mobile_customer").select(
        col("CUSTOMER_HK"), col("customer_id").alias("CUSTOMER_ID"),
        col("LOAD_DATE"), col("RECORD_SOURCE")
    )

    return crm_customers.union(ecommerce_customers).union(mobile_customers).distinct()
```

When the hub table is created for the first time and you need subsequent incremental MERGE behaviour in a non-DLT context (e.g., a Databricks Workflow notebook task), use the explicit MERGE pattern:

```python
# Hub MERGE pattern for use in Databricks Workflow notebook tasks
# (alternative to DLT for teams using a pure notebook-based pipeline)
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Read and union all staging sources
stg_union = (
    spark.table("main.staging.stg_crm_customer")
    .select("CUSTOMER_HK", "customer_id", "LOAD_DATE", "RECORD_SOURCE")
    .union(
        spark.table("main.staging.stg_ecommerce_customer")
        .select("CUSTOMER_HK", "customer_id", "LOAD_DATE", "RECORD_SOURCE")
    )
    .union(
        spark.table("main.staging.stg_mobile_customer")
        .select("CUSTOMER_HK", "customer_id", "LOAD_DATE", "RECORD_SOURCE")
    )
    .dropDuplicates(["CUSTOMER_HK"])
)

hub = DeltaTable.forName(spark, "main.raw_vault.hub_customer")

hub.alias("t").merge(
    stg_union.alias("s"),
    "t.CUSTOMER_HK = s.CUSTOMER_HK"
).whenNotMatchedInsertAll().execute()
# No WHEN MATCHED clause — hubs are append-only
```

#### SQL Example — Hub MERGE

```sql
-- Hub load using SQL MERGE INTO (run as a Databricks Workflow SQL task or notebook)
-- Equivalent to the PySpark MERGE above.
-- No WHEN MATCHED clause — hubs are append-only.

MERGE INTO main.raw_vault.hub_customer AS t
USING (
  SELECT CUSTOMER_HK, customer_id AS CUSTOMER_ID, LOAD_DATE, RECORD_SOURCE
  FROM main.staging.stg_crm_customer
  UNION
  SELECT CUSTOMER_HK, customer_id AS CUSTOMER_ID, LOAD_DATE, RECORD_SOURCE
  FROM main.staging.stg_ecommerce_customer
  UNION
  SELECT CUSTOMER_HK, customer_id AS CUSTOMER_ID, LOAD_DATE, RECORD_SOURCE
  FROM main.staging.stg_mobile_customer
) AS s
ON t.CUSTOMER_HK = s.CUSTOMER_HK
WHEN NOT MATCHED THEN INSERT *;
```

**Functional difference between Python and SQL hub loading:**
- Both approaches produce identical results. The Python `DeltaTable.merge()` is preferable when the logic is embedded in a notebook or when the union of staging sources involves conditional logic. The SQL `MERGE INTO` is preferable in SQL task nodes in Databricks Workflows or when the team prefers SQL-first development.
- Both are idempotent: running the same merge twice on the same staging data inserts zero rows on the second run.

#### Validation — SQL

```sql
-- Verify no duplicate natural keys in the hub
SELECT CUSTOMER_ID, COUNT(*) AS cnt
FROM main.raw_vault.hub_customer
GROUP BY CUSTOMER_ID
HAVING cnt > 1;
-- Expected: 0 rows

-- Verify all expected source systems are represented
SELECT RECORD_SOURCE, COUNT(*) AS row_count
FROM main.raw_vault.hub_customer
GROUP BY RECORD_SOURCE
ORDER BY RECORD_SOURCE;
-- Expected: rows for CRM, ECOMMERCE, MOBILE

-- Verify no null hash keys
SELECT COUNT(*) AS null_hk_count
FROM main.raw_vault.hub_customer
WHERE CUSTOMER_HK IS NULL;
-- Expected: 0
```

### Discussion and Concerns

- **Hub is idempotent:** The `WHEN NOT MATCHED` clause prevents duplicate natural keys from being inserted. Re-running the hub load on the same data produces no new rows.
- **No update clause:** Never add a `WHEN MATCHED THEN UPDATE` clause to a hub merge. Hub rows are immutable once inserted. If the source delivers a corrected natural key, it is a different entity and will produce a different hash key.
- **Multi-source loading:** Union the staging sources before the merge. The hub will record whichever source system's record arrived first as the `LOAD_DATE` and `RECORD_SOURCE` for that natural key.
- **No descriptive data in the hub:** The hub contains only the hash key, natural key, load date, and record source. Customer name, email, and all other descriptive attributes belong in a satellite.
- **Composite natural keys:** If the business key is composite (e.g., `SYSTEM_ID + CUSTOMER_ID`), concatenate and hash both columns consistently in the staging pipeline. Pass both columns separately to the hub as additional natural key columns.

### See Also

- [DeltaTable MERGE API](https://docs.databricks.com/en/delta/merge.html)
- [dv2_staging_cookbook.md](./dv2_staging_cookbook.md)
- [dv2_architecture.md — Hubs](./dv2_architecture.md#hubs)

---

## Link Loading

A Link records the structural relationship between two or more business entities. It captures when that relationship first appeared in the source data.

### Problem

An order management system records that customer C-001 placed order O-5042. An e-commerce platform records that the same customer placed order O-7891. Both relationships need to be recorded in the vault — not overwriting each other, and not losing the history of either. A simple foreign key on an order table cannot represent this without duplication across source systems.

### Solution

Use a `DeltaTable.merge()` with `whenNotMatchedInsertAll()` on the link hash key. Each row represents one unique combination of foreign hash keys. No update clause is used.

#### Python Example — `src/raw_vault/link_customer_order.py`

```python
# Link between the CUSTOMER and ORDER business entities.
# One row per unique (CUSTOMER_HK, ORDER_HK) combination.
# The link records when this customer-order relationship first appeared.

import dlt
from delta.tables import DeltaTable
from pyspark.sql.functions import col

@dlt.table(
    name="link_customer_order",
    comment="Link — CUSTOMER to ORDER relationship",
    table_properties={"delta.appendOnly": "true"}
)
def link_customer_order():
    return (
        dlt.read_stream("stg_order")
        .select(
            col("CUSTOMER_ORDER_HK"),
            col("CUSTOMER_HK"),
            col("ORDER_HK"),
            col("LOAD_DATE"),
            col("RECORD_SOURCE")
        )
        .dropDuplicates(["CUSTOMER_ORDER_HK"])
    )
```

For the explicit MERGE pattern in a Databricks Workflow notebook:

```python
from delta.tables import DeltaTable

stg_link = (
    spark.table("main.staging.stg_order")
    .select("CUSTOMER_ORDER_HK", "CUSTOMER_HK", "ORDER_HK", "LOAD_DATE", "RECORD_SOURCE")
    .dropDuplicates(["CUSTOMER_ORDER_HK"])
)

link = DeltaTable.forName(spark, "main.raw_vault.link_customer_order")

link.alias("t").merge(
    stg_link.alias("s"),
    "t.CUSTOMER_ORDER_HK = s.CUSTOMER_ORDER_HK"
).whenNotMatchedInsertAll().execute()
```

#### SQL Example — Link MERGE

```sql
MERGE INTO main.raw_vault.link_customer_order AS t
USING (
  SELECT DISTINCT CUSTOMER_ORDER_HK, CUSTOMER_HK, ORDER_HK, LOAD_DATE, RECORD_SOURCE
  FROM main.staging.stg_order
) AS s
ON t.CUSTOMER_ORDER_HK = s.CUSTOMER_ORDER_HK
WHEN NOT MATCHED THEN INSERT *;
```

#### Validation — SQL

```sql
-- Verify no duplicate link hash keys
SELECT CUSTOMER_ORDER_HK, COUNT(*) AS cnt
FROM main.raw_vault.link_customer_order
GROUP BY CUSTOMER_ORDER_HK
HAVING cnt > 1;
-- Expected: 0 rows

-- Verify all foreign keys resolve to their respective hubs
SELECT l.CUSTOMER_ORDER_HK
FROM main.raw_vault.link_customer_order l
LEFT JOIN main.raw_vault.hub_customer c ON l.CUSTOMER_HK = c.CUSTOMER_HK
WHERE c.CUSTOMER_HK IS NULL;
-- Expected: 0 rows (all customer FKs resolve)

SELECT l.CUSTOMER_ORDER_HK
FROM main.raw_vault.link_customer_order l
LEFT JOIN main.raw_vault.hub_order o ON l.ORDER_HK = o.ORDER_HK
WHERE o.ORDER_HK IS NULL;
-- Expected: 0 rows (all order FKs resolve)
```

### Discussion and Concerns

- **Link grain:** The link stores one row per unique relationship instance. `DISTINCT` / `dropDuplicates` on the link hash key before the merge ensures that if three source systems report the same (CUSTOMER_HK, ORDER_HK) pair, only one row is inserted.
- **Link does not track relationship end:** A link row records when a relationship first appeared. It has no close date. To track when a relationship was dissolved, use an Effectivity Satellite alongside the link (see below).
- **Three-way and higher-arity links:** Add more foreign key columns to the select and include them in the `dropDuplicates` key. The link hash key is then computed from all foreign keys combined in the staging pipeline.

### See Also

- [dv2_architecture.md — Links](./dv2_architecture.md#links)

---

## Non-Historized Link

A Non-Historized Link (NH Link) represents an immutable, fact-like relationship — one that by definition never changes or ends once it has been recorded.

### Problem

A payment processing system records each payment transaction as an event: payment P-9901 was made by customer C-001 against invoice I-4421. This event is immutable — once recorded, it never changes and it never ends. A standard link with an optional effectivity satellite is overkill for this case; there is no lifecycle to track.

### Solution

The physical structure of an NH Link is identical to a standard Link: a MERGE with `whenNotMatchedInsertAll()` on the link hash key, with no update clause. The distinction is architectural intent — the absence of an effectivity satellite signals that this relationship is immutable.

#### Python Example — `src/raw_vault/nh_link_payment.py`

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="nh_link_payment",
    comment="Non-historized link — immutable PAYMENT event relationships (CUSTOMER, INVOICE, PAYMENT)",
    table_properties={"delta.appendOnly": "true"}
)
def nh_link_payment():
    return (
        dlt.read_stream("stg_payment")
        .select(
            col("CUSTOMER_INVOICE_PAYMENT_HK"),
            col("CUSTOMER_HK"),
            col("INVOICE_HK"),
            col("PAYMENT_HK"),
            col("LOAD_DATE"),
            col("RECORD_SOURCE")
        )
        .dropDuplicates(["CUSTOMER_INVOICE_PAYMENT_HK"])
    )
```

#### SQL Example — NH Link MERGE

```sql
MERGE INTO main.raw_vault.nh_link_payment AS t
USING (
  SELECT DISTINCT
    CUSTOMER_INVOICE_PAYMENT_HK,
    CUSTOMER_HK, INVOICE_HK, PAYMENT_HK,
    LOAD_DATE, RECORD_SOURCE
  FROM main.staging.stg_payment
) AS s
ON t.CUSTOMER_INVOICE_PAYMENT_HK = s.CUSTOMER_INVOICE_PAYMENT_HK
WHEN NOT MATCHED THEN INSERT *;
```

#### Validation — SQL

```sql
-- Verify row count is monotonically increasing (nh_link never loses rows)
SELECT COUNT(*) AS total_payment_events
FROM main.raw_vault.nh_link_payment;

-- Verify no duplicate payment link hash keys
SELECT CUSTOMER_INVOICE_PAYMENT_HK, COUNT(*) AS cnt
FROM main.raw_vault.nh_link_payment
GROUP BY CUSTOMER_INVOICE_PAYMENT_HK
HAVING cnt > 1;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Use NH Link only for truly immutable relationships:** If there is any possibility that the relationship could be corrected, reversed, or dissolved, use a standard link with an effectivity satellite instead.
- **No payload in NH Link:** An NH Link stores only structural columns (hash keys, load date, record source). Monetary values, quantities, or event attributes belong in a Transactional Link (see below).

---

## Satellite Loading

A Satellite tracks the full change history of descriptive attributes for a Hub or Link. It is the only vault structure that accumulates over time in response to changing source data.

### Problem

A customer updates their email address. The CRM system sends the new record. The vault must retain the previous email address (for audit purposes) and record the new one as the current version. No existing rows should be updated or deleted; the previous state must remain permanently accessible.

### Solution

An anti-join between the incoming staging data and the most recent hashdiff already in the satellite identifies which rows represent genuine changes. Only those rows are appended. This is equivalent to AutomateDV's `sat` macro logic, implemented natively.

#### Python Example — `src/raw_vault/sat_customer_details.py`

```python
# Satellite for descriptive attributes of the CUSTOMER hub.
# Append-only: new rows are inserted only when CUSTOMER_HASHDIFF changes.
# Partitioned by LOAD_DATE for query performance.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="sat_customer_details",
    comment="Satellite — descriptive attributes for CUSTOMER hub",
    partition_cols=["LOAD_DATE"],
    table_properties={"delta.appendOnly": "true"}
)
@dlt.expect("parent_hk_not_null", "CUSTOMER_HK IS NOT NULL")
@dlt.expect("hashdiff_not_null",  "CUSTOMER_HASHDIFF IS NOT NULL")
def sat_customer_details():
    incoming = dlt.read_stream("stg_crm_customer").select(
        "CUSTOMER_HK", "CUSTOMER_HASHDIFF", "LOAD_DATE", "RECORD_SOURCE",
        "customer_name", "email_address", "phone_number",
        "billing_address", "city", "postcode", "country_code"
    )

    # Latest hashdiff already in the satellite (if it exists)
    try:
        current_sat = spark.table("main.raw_vault.sat_customer_details")
        window = Window.partitionBy("CUSTOMER_HK").orderBy(F.col("LOAD_DATE").desc())
        latest = (
            current_sat
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .select("CUSTOMER_HK", "CUSTOMER_HASHDIFF")
        )
        # Anti-join: keep only rows where (CUSTOMER_HK, CUSTOMER_HASHDIFF) is new
        new_rows = incoming.join(
            latest,
            on=["CUSTOMER_HK", "CUSTOMER_HASHDIFF"],
            how="left_anti"
        )
    except Exception:
        # Table does not exist yet on first run — all rows are new
        new_rows = incoming

    return new_rows
```

For a Databricks Workflow notebook task using explicit append:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

stg = spark.table("main.staging.stg_crm_customer").select(
    "CUSTOMER_HK", "CUSTOMER_HASHDIFF", "LOAD_DATE", "RECORD_SOURCE",
    "customer_name", "email_address", "phone_number",
    "billing_address", "city", "postcode", "country_code"
)

# Latest hashdiff per customer already in the satellite
current_sat = spark.table("main.raw_vault.sat_customer_details")
window = Window.partitionBy("CUSTOMER_HK").orderBy(F.col("LOAD_DATE").desc())
latest = (
    current_sat
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .select("CUSTOMER_HK", "CUSTOMER_HASHDIFF")
)

new_rows = stg.join(latest, on=["CUSTOMER_HK", "CUSTOMER_HASHDIFF"], how="left_anti")
new_rows.write.mode("append").partitionBy("LOAD_DATE").saveAsTable(
    "main.raw_vault.sat_customer_details"
)
```

#### SQL Example — Satellite append

```sql
-- Satellite load using INSERT INTO ... SELECT with anti-join (SQL equivalent)
-- Run as a Databricks Workflow SQL task or notebook cell.
-- Append-only: only rows with a new (CUSTOMER_HK, CUSTOMER_HASHDIFF) combination are inserted.

INSERT INTO main.raw_vault.sat_customer_details
SELECT
  s.CUSTOMER_HK,
  s.CUSTOMER_HASHDIFF,
  s.LOAD_DATE,
  s.RECORD_SOURCE,
  s.customer_name,
  s.email_address,
  s.phone_number,
  s.billing_address,
  s.city,
  s.postcode,
  s.country_code
FROM main.staging.stg_crm_customer s
WHERE NOT EXISTS (
  SELECT 1
  FROM (
    SELECT CUSTOMER_HK, CUSTOMER_HASHDIFF,
           ROW_NUMBER() OVER (PARTITION BY CUSTOMER_HK ORDER BY LOAD_DATE DESC) AS rn
    FROM main.raw_vault.sat_customer_details
  ) latest
  WHERE latest.rn = 1
    AND latest.CUSTOMER_HK       = s.CUSTOMER_HK
    AND latest.CUSTOMER_HASHDIFF = s.CUSTOMER_HASHDIFF
);
```

**Functional difference between Python and SQL satellite loading:**
- Both approaches apply the same anti-join logic to detect genuine changes. The Python approach is more readable for complex multi-column satellites and is easier to test with unit tests.
- The SQL `NOT EXISTS` subquery with `ROW_NUMBER()` is equivalent to the PySpark window-function anti-join. On large satellites, both benefit from `ZORDER BY CUSTOMER_HK` on the satellite table to speed up the latest-row lookup.
- In DLT, the Python approach is preferred because DLT streaming handles incremental state automatically. Outside DLT, both SQL and Python are equally valid.

#### Validation — SQL

```sql
-- Verify no consecutive rows with identical hashdiff for the same customer
WITH ranked AS (
  SELECT
    CUSTOMER_HK,
    CUSTOMER_HASHDIFF,
    LOAD_DATE,
    LAG(CUSTOMER_HASHDIFF) OVER (
      PARTITION BY CUSTOMER_HK ORDER BY LOAD_DATE
    ) AS prev_hashdiff
  FROM main.raw_vault.sat_customer_details
)
SELECT COUNT(*) AS duplicate_hashdiff_count
FROM ranked
WHERE CUSTOMER_HASHDIFF = prev_hashdiff;
-- Expected: 0

-- Verify total row count is growing (satellite never loses rows)
SELECT COUNT(*) AS total_rows, MAX(LOAD_DATE) AS last_load
FROM main.raw_vault.sat_customer_details;
```

### Discussion and Concerns

- **Never update or delete satellite rows:** Satellites are the permanent historical record. Any change to existing rows violates the audit trail. Set `delta.appendOnly = true` on the table to enforce this at the storage level.
- **Partition by LOAD_DATE on Databricks:** Satellites grow indefinitely. Partitioning by load date ensures that queries filtering to recent data do not scan the entire table history. Run `OPTIMIZE main.raw_vault.sat_customer_details ZORDER BY (CUSTOMER_HK)` after each load to maintain file compaction and point-lookup performance.
- **One satellite per source system per entity:** If customer attributes come from both a CRM and an MDM system, create two satellites (`sat_customer_crm_details` and `sat_customer_mdm_details`) rather than combining them. This isolates source system changes and preserves source fidelity.

### See Also

- [Delta Lake Append-Only Tables](https://docs.databricks.com/en/delta/table-properties.html)
- [Delta Lake OPTIMIZE and ZORDER](https://docs.databricks.com/en/delta/optimize.html)

---

## Multi-Active Satellite

A Multi-Active Satellite (MA Satellite) is used when a single hub record has multiple simultaneously active records of the same attribute type — for example, a customer with multiple phone numbers, all currently in use.

### Problem

A customer has a mobile phone, a work phone, and a home phone. All three are currently active. A standard satellite stores one row per customer per change event, which cannot represent multiple concurrent active values without losing history or combining values into a denormalised string.

### Solution

Add a Child Dependent Key (CDK) column to the satellite. The CDK + parent hash key together form the effective primary key. The anti-join for change detection is performed on (CUSTOMER_HK, PHONE_TYPE, CUSTOMER_PHONE_HASHDIFF) rather than just (CUSTOMER_HK, CUSTOMER_PHONE_HASHDIFF).

#### Python Example — `src/raw_vault/ma_sat_customer_phone.py`

```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="ma_sat_customer_phone",
    comment="Multi-Active Satellite — customer phone numbers (CDK: PHONE_TYPE)",
    table_properties={"delta.appendOnly": "true"}
)
def ma_sat_customer_phone():
    incoming = dlt.read_stream("stg_crm_customer_phones").select(
        "CUSTOMER_HK", "PHONE_TYPE",
        "CUSTOMER_PHONE_HASHDIFF",
        "LOAD_DATE", "RECORD_SOURCE",
        "phone_number", "is_primary"
    )

    try:
        current_sat = spark.table("main.raw_vault.ma_sat_customer_phone")
        window = Window.partitionBy("CUSTOMER_HK", "PHONE_TYPE").orderBy(
            F.col("LOAD_DATE").desc()
        )
        latest = (
            current_sat
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .select("CUSTOMER_HK", "PHONE_TYPE", "CUSTOMER_PHONE_HASHDIFF")
        )
        new_rows = incoming.join(
            latest,
            on=["CUSTOMER_HK", "PHONE_TYPE", "CUSTOMER_PHONE_HASHDIFF"],
            how="left_anti"
        )
    except Exception:
        new_rows = incoming

    return new_rows
```

#### SQL Example — MA Satellite append

```sql
INSERT INTO main.raw_vault.ma_sat_customer_phone
SELECT
  s.CUSTOMER_HK, s.PHONE_TYPE,
  s.CUSTOMER_PHONE_HASHDIFF,
  s.LOAD_DATE, s.RECORD_SOURCE,
  s.phone_number, s.is_primary
FROM main.staging.stg_crm_customer_phones s
WHERE NOT EXISTS (
  SELECT 1
  FROM (
    SELECT CUSTOMER_HK, PHONE_TYPE, CUSTOMER_PHONE_HASHDIFF,
           ROW_NUMBER() OVER (
             PARTITION BY CUSTOMER_HK, PHONE_TYPE ORDER BY LOAD_DATE DESC
           ) AS rn
    FROM main.raw_vault.ma_sat_customer_phone
  ) latest
  WHERE latest.rn = 1
    AND latest.CUSTOMER_HK            = s.CUSTOMER_HK
    AND latest.PHONE_TYPE             = s.PHONE_TYPE
    AND latest.CUSTOMER_PHONE_HASHDIFF = s.CUSTOMER_PHONE_HASHDIFF
);
```

#### Validation — SQL

```sql
-- Verify each (CUSTOMER_HK, PHONE_TYPE) combination has no consecutive duplicate hashdiffs
WITH ranked AS (
  SELECT
    CUSTOMER_HK, PHONE_TYPE,
    CUSTOMER_PHONE_HASHDIFF, LOAD_DATE,
    LAG(CUSTOMER_PHONE_HASHDIFF) OVER (
      PARTITION BY CUSTOMER_HK, PHONE_TYPE ORDER BY LOAD_DATE
    ) AS prev_hashdiff
  FROM main.raw_vault.ma_sat_customer_phone
)
SELECT COUNT(*) AS consecutive_duplicate_count
FROM ranked
WHERE CUSTOMER_PHONE_HASHDIFF = prev_hashdiff;
-- Expected: 0
```

### Discussion and Concerns

- **CDK selection is critical:** The CDK must be a stable, source-provided value that distinguishes concurrent active records. Do not use a sequence number or row number as a CDK — it is not a business concept and will not be consistent across loads.
- **Querying MA satellites is more complex:** Retrieving the current active set requires filtering to the latest load date per (CUSTOMER_HK, PHONE_TYPE) combination rather than just the latest per CUSTOMER_HK.

---

## Effectivity Satellite

An Effectivity Satellite tracks when a Link relationship became active and when it was dissolved. It is the vault mechanism for recording relationship lifecycle events.

### Problem

A customer is assigned to a sales region. Six months later, the customer moves and is reassigned to a different region. The Link records the structural relationship, but it does not record that the first assignment ended or that the second one began. Without effectivity tracking, it is impossible to determine which region a customer was assigned to on a given date.

### Solution

Create an effectivity satellite table with open and close date columns. New open events are appended when the relationship is first seen. Close events are appended (as new rows with an `END_DATE` set) when the source delivers a relationship dissolution signal.

#### Python Example — `src/raw_vault/eff_sat_customer_order.py`

```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="eff_sat_customer_order",
    comment="Effectivity Satellite — lifecycle of the CUSTOMER-ORDER relationship",
    table_properties={"delta.appendOnly": "true"}
)
def eff_sat_customer_order():
    incoming = dlt.read_stream("stg_order").select(
        "CUSTOMER_ORDER_HK", "CUSTOMER_HK", "ORDER_HK",
        "LOAD_DATE", "RECORD_SOURCE",
        F.col("LOAD_DATE").alias("START_DATE"),
        F.lit(None).cast("timestamp").alias("END_DATE")
    )

    try:
        current_eff = spark.table("main.raw_vault.eff_sat_customer_order")
        window = Window.partitionBy("CUSTOMER_ORDER_HK").orderBy(
            F.col("LOAD_DATE").desc()
        )
        latest = (
            current_eff
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .select("CUSTOMER_ORDER_HK")
        )
        # Only append rows for relationships not already open
        new_rows = incoming.join(latest, on="CUSTOMER_ORDER_HK", how="left_anti")
    except Exception:
        new_rows = incoming

    return new_rows
```

#### SQL Example — Effectivity Satellite append (open events)

```sql
-- Insert open effectivity records for new customer-order relationships
INSERT INTO main.raw_vault.eff_sat_customer_order
SELECT
  s.CUSTOMER_ORDER_HK,
  s.CUSTOMER_HK,
  s.ORDER_HK,
  s.LOAD_DATE,
  s.RECORD_SOURCE,
  s.LOAD_DATE  AS START_DATE,
  NULL         AS END_DATE
FROM main.staging.stg_order s
WHERE NOT EXISTS (
  SELECT 1 FROM main.raw_vault.eff_sat_customer_order e
  WHERE e.CUSTOMER_ORDER_HK = s.CUSTOMER_ORDER_HK
    AND e.END_DATE IS NULL
);

-- Insert close effectivity records when a relationship dissolution event arrives
INSERT INTO main.raw_vault.eff_sat_customer_order
SELECT
  s.CUSTOMER_ORDER_HK,
  s.CUSTOMER_HK,
  s.ORDER_HK,
  s.LOAD_DATE,
  s.RECORD_SOURCE,
  e.START_DATE,
  s.LOAD_DATE  AS END_DATE
FROM main.staging.stg_order_cancellations s
INNER JOIN main.raw_vault.eff_sat_customer_order e
  ON s.CUSTOMER_ORDER_HK = e.CUSTOMER_ORDER_HK
 AND e.END_DATE IS NULL;
```

#### Validation — SQL

```sql
-- Verify no open relationships have a close date before the open date
SELECT COUNT(*) AS invalid_effectivity_count
FROM main.raw_vault.eff_sat_customer_order
WHERE END_DATE IS NOT NULL
  AND END_DATE < START_DATE;
-- Expected: 0

-- Verify row count per link hash key is reasonable
SELECT CUSTOMER_ORDER_HK, COUNT(*) AS version_count
FROM main.raw_vault.eff_sat_customer_order
GROUP BY CUSTOMER_ORDER_HK
ORDER BY version_count DESC
LIMIT 20;
```

### Discussion and Concerns

- **Relationship close events require a dedicated staging source:** The source system must provide a signal that a relationship ended (e.g., an order cancellation event). This end event is staged in a separate staging view and appended to the effectivity satellite with a populated `END_DATE`.
- **`END_DATE` is set by the close staging record:** The open record has `END_DATE = NULL`. The close record has `END_DATE = LOAD_DATE` of the close event. This preserves the full lifecycle as two rows, not an update to the open row.

---

## Transactional Link

A Transactional Link models an immutable business event that has both a relationship (multiple entities involved) and a payload (attributes of the event itself).

### Problem

A payment of £149.99 was made by customer C-001 against invoice I-4421 using payment method PM-VISA on 2025-03-10. This is an immutable fact: the payment happened, the amount is fixed, and the event will never change. A standard link cannot hold payload columns. A satellite attached to the link would model the payment attributes as potentially changing, which is architecturally incorrect for an immutable transaction.

### Solution

Create a Transactional Link table that stores payload columns alongside foreign keys. Loading is identical to a standard link MERGE — append-only with no update clause.

#### Python Example — `src/raw_vault/t_link_payment.py`

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="t_link_payment",
    comment="Transactional Link — immutable PAYMENT event with payload (amount, currency, date)",
    table_properties={"delta.appendOnly": "true"}
)
def t_link_payment():
    return (
        dlt.read_stream("stg_payment")
        .select(
            col("PAYMENT_HK"),
            col("CUSTOMER_HK"),
            col("INVOICE_HK"),
            col("PAYMENT_METHOD_HK"),
            col("payment_amount").alias("PAYMENT_AMOUNT"),
            col("currency_code").alias("CURRENCY_CODE"),
            col("payment_date").alias("PAYMENT_DATE"),
            col("LOAD_DATE"),
            col("RECORD_SOURCE")
        )
        .dropDuplicates(["PAYMENT_HK"])
    )
```

#### SQL Example — Transactional Link MERGE

```sql
MERGE INTO main.raw_vault.t_link_payment AS t
USING (
  SELECT DISTINCT
    PAYMENT_HK, CUSTOMER_HK, INVOICE_HK, PAYMENT_METHOD_HK,
    payment_amount AS PAYMENT_AMOUNT,
    currency_code  AS CURRENCY_CODE,
    payment_date   AS PAYMENT_DATE,
    LOAD_DATE, RECORD_SOURCE
  FROM main.staging.stg_payment
) AS s
ON t.PAYMENT_HK = s.PAYMENT_HK
WHEN NOT MATCHED THEN INSERT *;
```

#### Validation — SQL

```sql
-- Verify no negative payment amounts
SELECT COUNT(*) AS negative_amount_count
FROM main.raw_vault.t_link_payment
WHERE PAYMENT_AMOUNT < 0;
-- Expected: 0 in most payment systems

-- Verify all foreign keys resolve
SELECT t.PAYMENT_HK
FROM main.raw_vault.t_link_payment t
LEFT JOIN main.raw_vault.hub_customer c ON t.CUSTOMER_HK = c.CUSTOMER_HK
WHERE c.CUSTOMER_HK IS NULL;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Use T-Link for events with monetary value or event payload:** If a link relationship has attributes that are facts of the event (not descriptions of the relationship that could change), use a T-Link.
- **T-Link payload is never updated:** Like all vault structures, T-Links are append-only. If a payment record is corrected, model the correction as a new event (reversal + re-entry), not an update to the original row.

---

## Reference Structures

Reference structures apply vault-pattern governance to lookup and reference data — static or slowly-changing data such as country codes, currency codes, product categories, and status values.

### Problem

Country codes are used across multiple vault structures. Without vault-style hash keys and load dates on the reference data, it cannot be joined to the vault using the same hash key conventions and cannot participate in PIT tables.

### Solution

Create Reference Hub and Reference Satellite tables using the same MERGE and anti-join patterns as operational vault structures. The source is a managed Delta table populated from a static reference dataset (CSV volume, reference API, or seed file).

#### Python Example — Reference Hub

```python
# src/raw_vault/ref_hub_country.py
import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="ref_hub_country",
    comment="Reference Hub — COUNTRY codes",
    table_properties={"delta.appendOnly": "true"}
)
def ref_hub_country():
    # Batch source (LIVE view, not streaming) — reference data is fully reloaded
    return (
        dlt.read("stg_ref_country")
        .select("COUNTRY_HK", "country_code", "LOAD_DATE", "RECORD_SOURCE")
        .dropDuplicates(["COUNTRY_HK"])
    )
```

#### SQL Example — Reference Hub MERGE

```sql
MERGE INTO main.raw_vault.ref_hub_country AS t
USING (
  SELECT DISTINCT COUNTRY_HK, country_code AS COUNTRY_CODE, LOAD_DATE, RECORD_SOURCE
  FROM main.staging.stg_ref_country
) AS s
ON t.COUNTRY_HK = s.COUNTRY_HK
WHEN NOT MATCHED THEN INSERT *;
```

#### SQL Example — Reference Satellite

```sql
-- Reference Satellite: append only when COUNTRY_HASHDIFF changes
INSERT INTO main.raw_vault.ref_sat_country_details
SELECT
  s.COUNTRY_HK, s.COUNTRY_HASHDIFF, s.LOAD_DATE, s.RECORD_SOURCE,
  s.country_name, s.region, s.currency_code, s.dialling_code
FROM main.staging.stg_ref_country s
WHERE NOT EXISTS (
  SELECT 1
  FROM (
    SELECT COUNTRY_HK, COUNTRY_HASHDIFF,
           ROW_NUMBER() OVER (PARTITION BY COUNTRY_HK ORDER BY LOAD_DATE DESC) AS rn
    FROM main.raw_vault.ref_sat_country_details
  ) latest
  WHERE latest.rn = 1
    AND latest.COUNTRY_HK      = s.COUNTRY_HK
    AND latest.COUNTRY_HASHDIFF = s.COUNTRY_HASHDIFF
);
```

#### Validation — SQL

```sql
-- Verify all expected country codes are present
SELECT COUNT(*) AS country_count
FROM main.raw_vault.ref_hub_country;

-- Verify no duplicate country codes
SELECT COUNTRY_CODE, COUNT(*) AS cnt
FROM main.raw_vault.ref_hub_country
GROUP BY COUNTRY_CODE
HAVING cnt > 1;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Feed from a managed Delta table:** Reference data should be loaded from a managed Delta table in Unity Catalog, not read directly from a file on each pipeline run. This makes the reference data queryable, versionable, and governable independently of the vault pipeline.
- **Hash key conventions apply to reference structures:** The same hashing algorithm, null substitution, and uppercasing conventions that apply to regular vault structures must be applied to reference staging pipelines.

### See Also

- [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [dv2_staging_cookbook.md — Reference Staging](./dv2_staging_cookbook.md#staging--multi-source-staging-and-reference-data)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Hub row count growth | `SELECT COUNT(*) FROM hub_customer` in Databricks SQL query | Stagnant count may indicate staging pipeline failure |
| Satellite hashdiff uniqueness per entity | Custom SQL check on (CUSTOMER_HK, LOAD_DATE) after each load | Duplicates indicate satellite anti-join failure |
| Link referential integrity | SQL check: link FK left-join to hub where hub key IS NULL | Missing hub rows indicate staging pipeline ordering issue |
| Satellite partition count | `DESCRIBE DETAIL main.raw_vault.sat_customer_details` | Unexpected partition explosion indicates bad LOAD_DATE value |
| DLT pipeline run duration | Databricks Workflows UI / DLT pipeline event log | Hub/link runs should be seconds; satellite runs grow over time |
| DLT expectation violations | DLT pipeline UI — Expectations panel | Any `FAIL UPDATE` violations must trigger an alert |

### Running OPTIMIZE After Each Load

```sql
-- Run OPTIMIZE after satellite loads to maintain file compaction and Z-order performance
OPTIMIZE main.raw_vault.sat_customer_details ZORDER BY (CUSTOMER_HK);
OPTIMIZE main.raw_vault.sat_order_details    ZORDER BY (ORDER_HK);

-- Schedule this as a SQL task in the Databricks Workflow immediately after the DLT pipeline task
```

### Metrics for Success

- [ ] Hub tables have no duplicate natural keys (`CUSTOMER_ID` unique in `hub_customer`)
- [ ] Satellite tables have no consecutive rows with identical hashdiff for the same parent hash key
- [ ] All link foreign keys resolve to rows in their respective hub tables
- [ ] All satellite tables have `delta.appendOnly = true` set — verified with `SHOW TBLPROPERTIES`
- [ ] Satellite tables are partitioned by `LOAD_DATE` and optimised with `ZORDER BY` on the parent hash key after each load
- [ ] DLT pipeline expectation violations (null hash keys, null hashdiffs) are zero on every pipeline run
