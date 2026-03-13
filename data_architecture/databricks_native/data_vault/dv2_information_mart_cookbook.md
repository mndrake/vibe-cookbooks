# Data Vault 2.0 Information Mart Cookbook

## Databricks Native Stack

> This file is the **Databricks-native** version of the Data Vault 2.0 Information Mart cookbook.
> It uses PySpark, Spark SQL, and Delta Lake exclusively.
> No dbt, dbt-utils, or AutomateDV dependencies are required.
>
> Equivalent dbt + AutomateDV version: [../../../databricks_and_dbt/data_vault/dv2_information_mart_cookbook.md](../../../databricks_and_dbt/data_vault/dv2_information_mart_cookbook.md)

---

## Introduction

This cookbook provides practical, step-by-step guidance for **building Information Marts** from a Data Vault 2.0 Raw Vault and Business Vault on Databricks using the native stack. It covers star schema construction from PIT and Bridge tables, flat wide table patterns, EAV pivot, and date spine generation for time-series reporting.

Information Mart models are the consumer-facing layer of the architecture. They must be simple, fast, and correct — all vault complexity is resolved in the Business Vault before the mart is built.

All patterns in this cookbook use:
- Spark SQL `CREATE OR REPLACE TABLE AS SELECT` for full-refresh mart builds
- PySpark `DataFrame` APIs for dynamic or parameterised mart construction
- `SEQUENCE + EXPLODE` for date spine generation (replacing `dbt_utils.date_spine`)
- PySpark `GroupedData.pivot()` for EAV pivoting (replacing `dbt_utils.pivot`)

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

> **On Databricks (interactive notebooks or Asset Bundle jobs):** PySpark, Delta Lake (`delta-spark`), and Delta Live Tables are pre-installed with every Databricks Runtime. No `pip install` is needed to run the code examples in this cookbook on a cluster.
>
> **Local development:** The tools below are installed on your local machine for CLI operations and Asset Bundle deployment.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.10+ | Local dev | Required for the Databricks CLI |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | 0.200+ | Local dev | Bundle deployment and workspace interaction |
| `delta-spark` | Bundled with Databricks Runtime | Databricks (bundled) | Pre-installed; no separate install needed on a cluster |
| Delta Live Tables runtime | Current channel | Databricks (bundled) | Provided by Databricks — no installation needed |

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
| SQL Warehouse (Serverless or Pro) | Compute for mart refresh SQL tasks | Photon enabled; size for concurrent BI queries |
| Unity Catalog — `marts` schema | Target for dimension and fact tables | Pipeline service principal needs `CREATE TABLE`, `INSERT`, `SELECT` |
| Unity Catalog — `business_vault` schema | Source for PIT and Bridge tables | `SELECT` privilege required |
| Unity Catalog — `raw_vault` schema | Source for transactional links and reference data | `SELECT` privilege required |

### Marts Schema Setup

```sql
-- Create the marts schema
CREATE SCHEMA IF NOT EXISTS main.marts
  COMMENT 'Data Vault 2.0 information marts — star schema dimensions and facts for BI consumption';

-- Grant privileges to the pipeline service principal
GRANT USE SCHEMA, CREATE TABLE, INSERT, SELECT ON SCHEMA main.marts
  TO `pipeline-service-principal@your-org.com`;

-- BI tool service account and analysts get SELECT only
GRANT USE SCHEMA, SELECT ON SCHEMA main.marts
  TO `bi-service-account@your-org.com`;

GRANT USE SCHEMA, SELECT ON SCHEMA main.marts
  TO `data-analysts@your-org.com`;
```

---

## Star Schema from Vault

Star schema Information Marts are the primary delivery artefact for BI tools, dashboards, and analyst workbooks. They present a clean, denormalised view of the business domain with one row per business key in dimensions and one row per event at the defined grain in fact tables.

### Problem

The vault graph — hubs, links, and satellites — is designed for auditability and parallel loading, not for BI query performance. Querying across five satellites for a single hub, filtering to the correct point in time, and navigating two or more links to build a fact table requires vault expertise that BI consumers and dashboard tools do not have. The mart must hide this complexity entirely.

### Solution

Use PIT and Bridge tables built in the Business Vault as the foundation for mart models. Dimension tables join the PIT table to its satellites using equality joins on the stored load dates. Fact tables are built by joining the Bridge table to transactional links and their satellites.

#### SQL Example — `dim_customer` dimension table

```sql
-- Dimension table for CUSTOMER.
-- One row per customer (current snapshot from PIT as of today).
-- Source: pit_customer (Business Vault) → sat_customer_details + sat_customer_marketing.
-- Never queries raw vault directly — all joins go through PIT.

CREATE OR REPLACE TABLE main.marts.dim_customer AS

WITH pit AS (
    -- Get today's PIT snapshot (AS_OF_DATE = current date)
    SELECT
        CUSTOMER_HK,
        SAT_CUSTOMER_DETAILS_LDTS,
        SAT_CUSTOMER_MARKETING_LDTS
    FROM main.business_vault.pit_customer
    WHERE AS_OF_DATE = CURRENT_DATE()
),

customer_details AS (
    SELECT
        CUSTOMER_HK,
        CUSTOMER_NAME,
        EMAIL_ADDRESS,
        PHONE_NUMBER,
        BILLING_ADDRESS,
        CITY,
        POSTCODE,
        COUNTRY_CODE,
        LOAD_DATE
    FROM main.raw_vault.sat_customer_details
),

customer_marketing AS (
    SELECT
        CUSTOMER_HK,
        MARKETING_SEGMENT,
        OPTED_IN_EMAIL,
        OPTED_IN_SMS,
        PREFERRED_CHANNEL,
        LOAD_DATE
    FROM main.raw_vault.sat_customer_marketing
)

SELECT
    h.CUSTOMER_ID                       AS customer_id,
    h.CUSTOMER_HK                       AS customer_hk,

    -- Demographics from customer details satellite
    d.CUSTOMER_NAME                     AS customer_name,
    d.EMAIL_ADDRESS                     AS email_address,
    d.PHONE_NUMBER                      AS phone_number,
    d.BILLING_ADDRESS                   AS billing_address,
    d.CITY                              AS city,
    d.POSTCODE                          AS postcode,
    d.COUNTRY_CODE                      AS country_code,

    -- Marketing attributes from marketing satellite
    m.MARKETING_SEGMENT                 AS marketing_segment,
    m.OPTED_IN_EMAIL                    AS opted_in_email,
    m.OPTED_IN_SMS                      AS opted_in_sms,
    m.PREFERRED_CHANNEL                 AS preferred_channel,

    -- Mart metadata
    CURRENT_TIMESTAMP()                 AS mart_refreshed_at

FROM pit
INNER JOIN main.raw_vault.hub_customer h  ON pit.CUSTOMER_HK = h.CUSTOMER_HK
INNER JOIN customer_details d             ON pit.CUSTOMER_HK = d.CUSTOMER_HK
                                        AND pit.SAT_CUSTOMER_DETAILS_LDTS = d.LOAD_DATE
LEFT  JOIN customer_marketing m           ON pit.CUSTOMER_HK = m.CUSTOMER_HK
                                        AND pit.SAT_CUSTOMER_MARKETING_LDTS = m.LOAD_DATE;
```

#### SQL Example — `fct_orders` fact table

```sql
-- Fact table for ORDER events.
-- One row per order at the (customer, order, order_date) grain.
-- Source: bridge_customer_orders → hub_customer + hub_order + sat_order_details.

CREATE OR REPLACE TABLE main.marts.fct_orders AS

WITH bridge AS (
    SELECT
        CUSTOMER_HK,
        ORDER_HK,
        CUSTOMER_ORDER_HK,
        AS_OF_DATE
    FROM main.business_vault.bridge_customer_orders
    WHERE AS_OF_DATE = CURRENT_DATE()
),

order_pit AS (
    SELECT
        ORDER_HK,
        SAT_ORDER_DETAILS_LDTS
    FROM main.business_vault.pit_order
    WHERE AS_OF_DATE = CURRENT_DATE()
),

order_details AS (
    SELECT
        ORDER_HK,
        ORDER_DATE,
        ORDER_STATUS,
        TOTAL_AMOUNT,
        CURRENCY_CODE,
        LOAD_DATE
    FROM main.raw_vault.sat_order_details
)

SELECT
    -- Surrogate key for the fact table (mart-layer, not a vault hash key)
    MD5(CONCAT_WS('||',
        COALESCE(CAST(b.CUSTOMER_HK AS STRING), '^^'),
        COALESCE(CAST(b.ORDER_HK    AS STRING), '^^')
    ))                                          AS order_fact_sk,

    -- Foreign keys to dimensions
    hc.CUSTOMER_ID                              AS customer_id,
    ho.ORDER_NUMBER                             AS order_number,

    -- Hash keys for vault lineage tracing
    b.CUSTOMER_HK                               AS customer_hk,
    b.ORDER_HK                                  AS order_hk,

    -- Degenerate dimensions
    od.ORDER_DATE                               AS order_date,
    od.ORDER_STATUS                             AS order_status,

    -- Measures
    od.TOTAL_AMOUNT                             AS order_total_amount,
    od.CURRENCY_CODE                            AS currency_code,

    -- Mart metadata
    CURRENT_TIMESTAMP()                         AS mart_refreshed_at

FROM bridge b
INNER JOIN main.raw_vault.hub_customer hc  ON b.CUSTOMER_HK = hc.CUSTOMER_HK
INNER JOIN main.raw_vault.hub_order    ho  ON b.ORDER_HK    = ho.ORDER_HK
INNER JOIN order_pit                   op  ON b.ORDER_HK    = op.ORDER_HK
INNER JOIN order_details               od  ON op.ORDER_HK   = od.ORDER_HK
                                         AND op.SAT_ORDER_DETAILS_LDTS = od.LOAD_DATE;
```

**Note on surrogate keys:** The `MD5(CONCAT_WS(...))` pattern replaces `dbt_utils.generate_surrogate_key`. It produces the same composite hash key from the grain columns using the same null-safe pattern as vault hash key derivation. If the mart already retains vault hash keys (`CUSTOMER_HK`, `ORDER_HK`), the surrogate key is optional — use it only if the BI tool requires a single integer or string primary key on the fact table.

#### Python Example — PySpark dimension construction

```python
# PySpark equivalent of dim_customer — use when embedded in a DLT pipeline
# or when mart construction needs Python-specific logic (UDF scoring, etc.)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import date


def build_dim_customer(spark: SparkSession) -> None:
    today = date.today()

    pit = (
        spark.table("main.business_vault.pit_customer")
        .filter(F.col("AS_OF_DATE") == F.lit(today))
        .select("CUSTOMER_HK", "SAT_CUSTOMER_DETAILS_LDTS", "SAT_CUSTOMER_MARKETING_LDTS")
    )

    hub = spark.table("main.raw_vault.hub_customer").select("CUSTOMER_HK", "CUSTOMER_ID")

    sat_details = spark.table("main.raw_vault.sat_customer_details").select(
        "CUSTOMER_HK", "CUSTOMER_NAME", "EMAIL_ADDRESS", "PHONE_NUMBER",
        "BILLING_ADDRESS", "CITY", "POSTCODE", "COUNTRY_CODE", "LOAD_DATE"
    )

    sat_marketing = spark.table("main.raw_vault.sat_customer_marketing").select(
        "CUSTOMER_HK", "MARKETING_SEGMENT", "OPTED_IN_EMAIL",
        "OPTED_IN_SMS", "PREFERRED_CHANNEL", "LOAD_DATE"
    )

    # Join PIT to satellites using equality on load date pointer
    dim = (
        pit
        .join(hub, on="CUSTOMER_HK", how="inner")
        .join(
            sat_details.alias("d"),
            (F.col("pit.CUSTOMER_HK") == F.col("d.CUSTOMER_HK")) &
            (F.col("pit.SAT_CUSTOMER_DETAILS_LDTS") == F.col("d.LOAD_DATE")),
            how="inner"
        )
        .join(
            sat_marketing.alias("m"),
            (F.col("pit.CUSTOMER_HK") == F.col("m.CUSTOMER_HK")) &
            (F.col("pit.SAT_CUSTOMER_MARKETING_LDTS") == F.col("m.LOAD_DATE")),
            how="left"
        )
        .select(
            F.col("hub.CUSTOMER_ID").alias("customer_id"),
            F.col("pit.CUSTOMER_HK").alias("customer_hk"),
            F.col("d.CUSTOMER_NAME").alias("customer_name"),
            F.col("d.EMAIL_ADDRESS").alias("email_address"),
            F.col("d.PHONE_NUMBER").alias("phone_number"),
            F.col("d.BILLING_ADDRESS").alias("billing_address"),
            F.col("d.CITY").alias("city"),
            F.col("d.POSTCODE").alias("postcode"),
            F.col("d.COUNTRY_CODE").alias("country_code"),
            F.col("m.MARKETING_SEGMENT").alias("marketing_segment"),
            F.col("m.OPTED_IN_EMAIL").alias("opted_in_email"),
            F.col("m.OPTED_IN_SMS").alias("opted_in_sms"),
            F.col("m.PREFERRED_CHANNEL").alias("preferred_channel"),
            F.current_timestamp().alias("mart_refreshed_at")
        )
    )

    dim.write.format("delta").mode("overwrite").saveAsTable("main.marts.dim_customer")
    print(f"dim_customer refreshed — {dim.count()} rows")
```

#### Validation — SQL

```sql
-- Verify dim_customer has exactly one row per customer
SELECT customer_id, COUNT(*) AS cnt
FROM main.marts.dim_customer
GROUP BY customer_id
HAVING cnt > 1;
-- Expected: 0 rows

-- Verify no null foreign keys in the fact table
SELECT COUNT(*) AS null_customer_id
FROM main.marts.fct_orders
WHERE customer_id IS NULL;
-- Expected: 0

SELECT COUNT(*) AS null_order_number
FROM main.marts.fct_orders
WHERE order_number IS NULL;
-- Expected: 0

-- Verify all fact rows resolve to a dimension row
SELECT f.order_fact_sk
FROM main.marts.fct_orders f
LEFT JOIN main.marts.dim_customer d ON f.customer_id = d.customer_id
WHERE d.customer_id IS NULL;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Never query Raw Vault directly from mart models:** All mart models must read from PIT and Bridge tables, or from Business Vault derived models. Direct joins from a mart to a satellite bypass the PIT time-alignment logic and produce incorrect current-state snapshots.
- **Use `LEFT JOIN` for optional satellite relationships:** If a customer may not have a marketing satellite row, use `LEFT JOIN` for that satellite. `INNER JOIN` against a satellite that has no row for some hub records silently drops those hub records from the dimension.
- **Include vault hash keys in the fact table:** Retaining `CUSTOMER_HK` and `ORDER_HK` in the fact table allows vault lineage tracing (joining back to the raw vault for audit purposes) without exposing natural keys that may change across source systems.
- **Mart surrogate keys:** Use the `MD5(CONCAT_WS(...))` pattern (matching the vault hash key derivation) to create a mart-layer surrogate key for the fact table grain. This is distinct from the vault hash key — it is a convenience key for the BI layer.

### See Also

- [dv2_business_vault_cookbook.md — PIT Tables](./dv2_business_vault_cookbook.md#point-in-time-pit-tables)
- [dv2_business_vault_cookbook.md — Bridge Tables](./dv2_business_vault_cookbook.md#bridge-tables)
- [Kimball Star Schema Design](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)

---

## Flat Wide Tables

Flat wide tables denormalise multiple satellites into a single table for BI consumers and data science workflows that cannot or do not want to work with a star schema.

### Problem

A data science team requires a single DataFrame with all customer attributes — demographics, contact details, marketing preferences, and behavioural signals — merged into one wide table. Their tooling (Pandas, scikit-learn pipelines) performs best with a single table input. Directing them to the star schema with a multi-join query is impractical for their workflow.

### Solution

Build a Spark SQL model that joins multiple satellites via the PIT table into a single wide table. The PIT table ensures time-consistent attribute resolution across all satellites for each customer.

#### SQL Example — `wide_customer`

```sql
-- Wide flat table combining three customer satellites into a single denormalised row.
-- One row per customer; all attributes from demographics, contact, and preferences satellites.
-- Intended for data science feature stores and bulk export workflows.

CREATE OR REPLACE TABLE main.marts.wide_customer AS

WITH pit AS (
    SELECT
        CUSTOMER_HK,
        SAT_CUSTOMER_DETAILS_LDTS,
        SAT_CUSTOMER_CONTACT_LDTS,
        SAT_CUSTOMER_PREFERENCES_LDTS
    FROM main.business_vault.pit_customer
    WHERE AS_OF_DATE = CURRENT_DATE()
),

demographics AS (
    SELECT CUSTOMER_HK, LOAD_DATE,
           FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, GENDER
    FROM main.raw_vault.sat_customer_demographics
),

contact AS (
    SELECT CUSTOMER_HK, LOAD_DATE,
           EMAIL_ADDRESS, PHONE_NUMBER, BILLING_ADDRESS, CITY, POSTCODE, COUNTRY_CODE
    FROM main.raw_vault.sat_customer_contact
),

preferences AS (
    SELECT CUSTOMER_HK, LOAD_DATE,
           PREFERRED_LANGUAGE, PREFERRED_CURRENCY, PREFERRED_CHANNEL,
           NEWSLETTER_OPT_IN, PRODUCT_CATEGORY_INTEREST
    FROM main.raw_vault.sat_customer_preferences
)

SELECT
    h.CUSTOMER_ID,
    h.CUSTOMER_HK,

    -- Demographics
    dem.FIRST_NAME,
    dem.LAST_NAME,
    CONCAT(dem.FIRST_NAME, ' ', dem.LAST_NAME)  AS FULL_NAME,
    dem.DATE_OF_BIRTH,
    DATEDIFF(CURRENT_DATE(), dem.DATE_OF_BIRTH) / 365.25
                                                AS AGE_YEARS,
    dem.GENDER,

    -- Contact
    con.EMAIL_ADDRESS,
    con.PHONE_NUMBER,
    con.BILLING_ADDRESS,
    con.CITY,
    con.POSTCODE,
    con.COUNTRY_CODE,

    -- Preferences
    pref.PREFERRED_LANGUAGE,
    pref.PREFERRED_CURRENCY,
    pref.PREFERRED_CHANNEL,
    pref.NEWSLETTER_OPT_IN,
    pref.PRODUCT_CATEGORY_INTEREST,

    CURRENT_TIMESTAMP()                         AS MART_REFRESHED_AT

FROM pit
INNER JOIN main.raw_vault.hub_customer h
    ON pit.CUSTOMER_HK = h.CUSTOMER_HK
LEFT JOIN demographics dem
    ON pit.CUSTOMER_HK = dem.CUSTOMER_HK
   AND pit.SAT_CUSTOMER_DETAILS_LDTS = dem.LOAD_DATE
LEFT JOIN contact con
    ON pit.CUSTOMER_HK = con.CUSTOMER_HK
   AND pit.SAT_CUSTOMER_CONTACT_LDTS = con.LOAD_DATE
LEFT JOIN preferences pref
    ON pit.CUSTOMER_HK = pref.CUSTOMER_HK
   AND pit.SAT_CUSTOMER_PREFERENCES_LDTS = pref.LOAD_DATE;
```

#### Python Example — PySpark wide table construction

```python
# PySpark wide table — useful when the set of satellites is dynamic
# or when additional Python-based feature derivation is needed.

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import date


def build_wide_customer(spark: SparkSession) -> None:
    today = date.today()

    pit = (
        spark.table("main.business_vault.pit_customer")
        .filter(F.col("AS_OF_DATE") == F.lit(today))
    )
    hub = spark.table("main.raw_vault.hub_customer").select("CUSTOMER_HK", "CUSTOMER_ID")

    satellites = {
        "demographics": {
            "table": "main.raw_vault.sat_customer_demographics",
            "pit_ldts": "SAT_CUSTOMER_DETAILS_LDTS",
            "cols": ["FIRST_NAME", "LAST_NAME", "DATE_OF_BIRTH", "GENDER"],
        },
        "contact": {
            "table": "main.raw_vault.sat_customer_contact",
            "pit_ldts": "SAT_CUSTOMER_CONTACT_LDTS",
            "cols": ["EMAIL_ADDRESS", "PHONE_NUMBER", "BILLING_ADDRESS",
                     "CITY", "POSTCODE", "COUNTRY_CODE"],
        },
        "preferences": {
            "table": "main.raw_vault.sat_customer_preferences",
            "pit_ldts": "SAT_CUSTOMER_PREFERENCES_LDTS",
            "cols": ["PREFERRED_LANGUAGE", "PREFERRED_CURRENCY", "PREFERRED_CHANNEL",
                     "NEWSLETTER_OPT_IN", "PRODUCT_CATEGORY_INTEREST"],
        },
    }

    result = pit.join(hub, on="CUSTOMER_HK", how="inner")

    for alias, config in satellites.items():
        sat = spark.table(config["table"]).select(
            "CUSTOMER_HK", "LOAD_DATE", *config["cols"]
        ).alias(alias)

        result = result.join(
            sat,
            (result["CUSTOMER_HK"] == sat["CUSTOMER_HK"]) &
            (result[config["pit_ldts"]] == sat["LOAD_DATE"]),
            how="left"
        )

    # Add derived columns
    result = (
        result
        .withColumn("FULL_NAME",
                    F.concat_ws(" ", F.col("FIRST_NAME"), F.col("LAST_NAME")))
        .withColumn("AGE_YEARS",
                    F.datediff(F.current_date(), F.col("DATE_OF_BIRTH")) / 365.25)
        .withColumn("MART_REFRESHED_AT", F.current_timestamp())
    )

    result.write.format("delta").mode("overwrite").saveAsTable("main.marts.wide_customer")
    print(f"wide_customer refreshed — {result.count()} rows")
```

#### Validation — SQL

```sql
-- Verify one row per customer
SELECT CUSTOMER_ID, COUNT(*) AS cnt
FROM main.marts.wide_customer
GROUP BY CUSTOMER_ID
HAVING cnt > 1;
-- Expected: 0 rows

-- Verify FULL_NAME is populated for all customers with demographics
SELECT COUNT(*) AS null_full_name
FROM main.marts.wide_customer
WHERE FULL_NAME IS NULL OR TRIM(FULL_NAME) = '';
-- Expected: 0 for customers who have a demographics satellite row
```

### Discussion and Concerns

- **Wide tables are expensive to maintain:** Any change to a satellite's payload columns, or the addition of a new satellite, requires a full rebuild. For large customer tables (tens of millions of rows), use `MERGE INTO` for incremental maintenance: identify changed rows via CUSTOMER_HK and refresh only those rows.
- **Document the satellite dependencies:** Add a comment block at the top of the SQL listing which satellites feed each column group. When a satellite changes, this comment identifies which wide tables are affected.
- **Data science consumers may prefer Delta Sharing:** For data science teams that need fresh data frequently, consider exposing the wide table via Delta Sharing rather than a direct Unity Catalog query. This decouples their access from the mart refresh schedule.
- **Avoid very wide tables in BI tools:** Wide tables with 100+ columns cause performance issues in tools like Power BI and Tableau. For BI consumers, prefer the star schema. Wide tables are best suited for bulk export, ML feature stores, and programmatic access.

### See Also

- [Delta Lake MERGE INTO](https://docs.databricks.com/en/delta/merge.html)
- [Databricks Feature Store](https://docs.databricks.com/en/machine-learning/feature-store/index.html)

---

## Native Pivot for EAV Satellites

Some satellite data arrives in a key-value (Entity-Attribute-Value, or EAV) structure where each row stores an attribute name and its value, rather than having one column per attribute. BI tools require this to be pivoted into columnar form.

### Problem

A product attributes satellite stores data as `(PRODUCT_HK, ATTRIBUTE_NAME, ATTRIBUTE_VALUE)` rows. Each product has up to 12 attributes: weight, dimensions, colour, material, brand, and so on. The source system emits these as key-value pairs rather than as a fixed-width record. Querying this structure in a BI tool requires pivoting on `ATTRIBUTE_NAME`, which the tool cannot do natively.

### Solution

Use PySpark `GroupedData.pivot()` to transform the EAV satellite into a columnar table. Unlike `dbt_utils.pivot()` which determines columns at compile time, PySpark `pivot()` determines column values at runtime and handles dynamic attribute sets automatically.

#### Python Example — EAV pivot with PySpark

```python
# PySpark pivot: transforms (PRODUCT_HK, ATTRIBUTE_NAME, ATTRIBUTE_VALUE)
# into one column per distinct ATTRIBUTE_NAME.
# Column values are determined dynamically at runtime.

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


def build_dim_product_attributes(spark: SparkSession) -> None:
    hub = spark.table("main.raw_vault.hub_product").select("PRODUCT_HK", "PRODUCT_SKU")

    # Latest satellite row per (PRODUCT_HK, ATTRIBUTE_NAME)
    sat = spark.table("main.raw_vault.sat_product_attributes")
    window = Window.partitionBy("PRODUCT_HK", "ATTRIBUTE_NAME").orderBy(
        F.col("LOAD_DATE").desc()
    )
    latest = (
        sat
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select("PRODUCT_HK", "ATTRIBUTE_NAME", "ATTRIBUTE_VALUE")
    )

    # Pivot ATTRIBUTE_NAME into columns
    # PySpark determines the column list at runtime from the data
    pivoted = (
        latest
        .groupBy("PRODUCT_HK")
        .pivot("ATTRIBUTE_NAME")   # Automatically discovers all distinct attribute names
        .agg(F.first("ATTRIBUTE_VALUE"))
    )

    # Join to hub to add natural key
    result = pivoted.join(hub, on="PRODUCT_HK", how="inner")

    result.write.format("delta").mode("overwrite").saveAsTable(
        "main.marts.dim_product_attributes"
    )
    print(f"dim_product_attributes refreshed — {result.count()} rows")
```

To fix the column set (prevent schema changes between runs when attribute names change):

```python
# Fixed-column pivot: supply the explicit list of attribute names.
# This prevents schema drift between pipeline runs.

KNOWN_ATTRIBUTE_NAMES = [
    "weight", "height_cm", "width_cm", "depth_cm",
    "colour", "material", "brand", "country_of_origin"
]

pivoted_fixed = (
    latest
    .groupBy("PRODUCT_HK")
    .pivot("ATTRIBUTE_NAME", KNOWN_ATTRIBUTE_NAMES)  # Schema is fixed to this list
    .agg(F.first("ATTRIBUTE_VALUE"))
)
```

#### SQL Example — Manual CASE-based pivot (Spark SQL)

```sql
-- Spark SQL CASE-based pivot equivalent.
-- Use when the attribute set is small and fixed, and PySpark is not available.

CREATE OR REPLACE TABLE main.marts.dim_product_attributes AS

WITH latest_attributes AS (
    SELECT
        PRODUCT_HK,
        ATTRIBUTE_NAME,
        ATTRIBUTE_VALUE,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_HK, ATTRIBUTE_NAME
            ORDER BY LOAD_DATE DESC
        ) AS rn
    FROM main.raw_vault.sat_product_attributes
),

current_attributes AS (
    SELECT PRODUCT_HK, ATTRIBUTE_NAME, ATTRIBUTE_VALUE
    FROM latest_attributes WHERE rn = 1
)

SELECT
    h.PRODUCT_SKU                               AS product_sku,
    h.PRODUCT_HK                                AS product_hk,
    MAX(CASE WHEN ATTRIBUTE_NAME = 'weight'       THEN ATTRIBUTE_VALUE END) AS weight,
    MAX(CASE WHEN ATTRIBUTE_NAME = 'height_cm'    THEN ATTRIBUTE_VALUE END) AS height_cm,
    MAX(CASE WHEN ATTRIBUTE_NAME = 'width_cm'     THEN ATTRIBUTE_VALUE END) AS width_cm,
    MAX(CASE WHEN ATTRIBUTE_NAME = 'colour'       THEN ATTRIBUTE_VALUE END) AS colour,
    MAX(CASE WHEN ATTRIBUTE_NAME = 'material'     THEN ATTRIBUTE_VALUE END) AS material,
    MAX(CASE WHEN ATTRIBUTE_NAME = 'brand'        THEN ATTRIBUTE_VALUE END) AS brand
FROM current_attributes ca
INNER JOIN main.raw_vault.hub_product h ON ca.PRODUCT_HK = h.PRODUCT_HK
GROUP BY h.PRODUCT_SKU, h.PRODUCT_HK;
```

**Functional difference between SQL and Python approaches:** The SQL CASE pattern is static — the column list is hard-coded and must be maintained manually when attributes are added. The PySpark `pivot()` without a fixed column list is fully dynamic but may change schema between runs. For production marts where schema stability is required, use the Python fixed-column approach or the SQL CASE approach. For exploratory marts or data science workloads where schema flexibility is acceptable, use PySpark `pivot()` without a fixed column list.

#### Validation — SQL

```sql
-- Verify one row per product in the pivoted dimension
SELECT product_sku, COUNT(*) AS cnt
FROM main.marts.dim_product_attributes
GROUP BY product_sku
HAVING cnt > 1;
-- Expected: 0 rows

-- Verify columns were generated correctly
DESCRIBE TABLE main.marts.dim_product_attributes;
-- Should show one column per distinct ATTRIBUTE_NAME from sat_product_attributes
```

### Discussion and Concerns

- **PySpark pivot determines columns at runtime:** Unlike `dbt_utils.pivot` (which compiles to a fixed SQL SELECT), PySpark pivot runs a preliminary scan to discover distinct column values before executing the pivot. On large attribute satellites, supply the fixed column list to avoid this scan and prevent unexpected schema changes.
- **Large numbers of attributes create very wide tables:** Pivoting an EAV satellite with 200 distinct attribute names produces a 200-column table. BI tools handle this poorly. Consider whether a `MAP` column (e.g., `MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(ATTRIBUTE_NAME, ATTRIBUTE_VALUE)))`) is more appropriate for high-cardinality attribute sets.
- **All pivoted columns have the same data type:** PySpark `pivot` aggregates using `first()`, so all columns inherit the type of `ATTRIBUTE_VALUE`. If attributes have different types (integer weight, string colour), they are all returned as STRING. Cast in a downstream mart model if specific types are required.

### See Also

- [PySpark pivot function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.pivot.html)
- [Databricks Spark SQL PIVOT clause](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-pivot.html)

---

## Date Spine for Time-Series Marts

A date spine is a continuous sequence of dates used as the backbone for time-series reporting. It ensures that every date has a row in the mart, including dates on which no events occurred — preventing gaps in trend charts and running totals.

### Problem

A customer activity report needs to show daily order counts for the last 365 days. On days when a customer placed no orders, the report must still show a zero rather than a gap in the time series. Simply aggregating the orders fact table by date will omit dates with no orders; joining to a pre-built calendar table requires that table to exist and be maintained separately.

### Solution

Use `SEQUENCE + EXPLODE` in Spark SQL (or PySpark) to generate a continuous date series inline. Join the date spine to the PIT table or fact table to ensure every date has a row.

#### SQL Example — Time-series mart with date spine

```sql
-- Time-series mart: customer daily order activity.
-- One row per (customer, date) combination for every date in the reporting window.
-- Dates with no orders show order_count = 0 and order_total = 0.
-- Uses SEQUENCE + EXPLODE to guarantee no gaps in the date series.

CREATE OR REPLACE TABLE main.marts.mart_customer_daily_activity AS

WITH date_spine AS (
    -- Generate one row per day from 2020-01-01 to today using SEQUENCE + EXPLODE
    SELECT EXPLODE(SEQUENCE(
        DATE '2020-01-01',
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS date_day
),

customers AS (
    SELECT CUSTOMER_HK, CUSTOMER_ID
    FROM main.raw_vault.hub_customer
),

-- Cross join customers with the date spine to create the complete grid
-- Limit to dates on or after the customer's first appearance in the vault
customer_date_grid AS (
    SELECT
        c.CUSTOMER_HK,
        c.CUSTOMER_ID,
        ds.date_day
    FROM customers c
    CROSS JOIN date_spine ds
    INNER JOIN main.raw_vault.hub_customer hc
        ON c.CUSTOMER_HK = hc.CUSTOMER_HK
    WHERE ds.date_day >= CAST(hc.LOAD_DATE AS DATE)
),

daily_orders AS (
    SELECT
        f.customer_id,
        CAST(f.order_date AS DATE)              AS order_date,
        COUNT(DISTINCT f.order_number)          AS order_count,
        SUM(f.order_total_amount)               AS order_total
    FROM main.marts.fct_orders f
    GROUP BY f.customer_id, CAST(f.order_date AS DATE)
)

SELECT
    g.CUSTOMER_ID                               AS customer_id,
    g.date_day                                  AS activity_date,
    COALESCE(o.order_count,  0)                 AS order_count,
    COALESCE(o.order_total,  0.00)              AS order_total,
    CURRENT_TIMESTAMP()                         AS mart_refreshed_at
FROM customer_date_grid g
LEFT JOIN daily_orders o
    ON g.CUSTOMER_ID = o.customer_id
   AND g.date_day    = o.order_date;
```

For a standalone date dimension (calendar table):

```sql
-- Standalone date dimension generated from a date spine.
-- Refresh infrequently — only needs to be rebuilt when the end date extends.

CREATE OR REPLACE TABLE main.marts.dim_date AS

WITH spine AS (
    SELECT EXPLODE(SEQUENCE(
        DATE '2015-01-01',
        CAST(CURRENT_DATE() + INTERVAL 2 YEARS AS DATE),
        INTERVAL 1 DAY
    )) AS date_day
)

SELECT
    date_day                                    AS date_id,
    date_day                                    AS full_date,
    YEAR(date_day)                              AS year_number,
    QUARTER(date_day)                           AS quarter_number,
    MONTH(date_day)                             AS month_number,
    DATE_FORMAT(date_day, 'MMMM')               AS month_name,
    WEEKOFYEAR(date_day)                        AS week_of_year,
    DAYOFWEEK(date_day)                         AS day_of_week,
    DATE_FORMAT(date_day, 'EEEE')               AS day_name,
    DAYOFMONTH(date_day)                        AS day_of_month,
    CASE WHEN DAYOFWEEK(date_day) IN (1, 7)
         THEN FALSE ELSE TRUE END               AS is_weekday,
    -- Fiscal year (example: fiscal year starts April 1)
    CASE WHEN MONTH(date_day) >= 4
         THEN YEAR(date_day)
         ELSE YEAR(date_day) - 1
    END                                         AS fiscal_year
FROM spine;
```

#### Python Example — PySpark date spine

```python
# PySpark date spine — use when the date spine is generated inside a DLT pipeline
# or alongside other PySpark transformations.

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import date


def build_date_spine(spark: SparkSession, start: str = "2015-01-01") -> None:
    """Generate and persist a date dimension from start to 2 years from today."""
    today = date.today()
    end_date = date(today.year + 2, today.month, today.day).strftime("%Y-%m-%d")

    spine = spark.sql(f"""
        SELECT EXPLODE(SEQUENCE(
            DATE '{start}',
            DATE '{end_date}',
            INTERVAL 1 DAY
        )) AS date_day
    """)

    dim_date = (
        spine
        .withColumn("date_id",       F.col("date_day"))
        .withColumn("year_number",   F.year("date_day"))
        .withColumn("quarter_number",F.quarter("date_day"))
        .withColumn("month_number",  F.month("date_day"))
        .withColumn("month_name",    F.date_format("date_day", "MMMM"))
        .withColumn("week_of_year",  F.weekofyear("date_day"))
        .withColumn("day_of_week",   F.dayofweek("date_day"))
        .withColumn("day_name",      F.date_format("date_day", "EEEE"))
        .withColumn("day_of_month",  F.dayofmonth("date_day"))
        .withColumn("is_weekday",    ~F.col("day_of_week").isin(1, 7))
        .withColumn("fiscal_year",
            F.when(F.month("date_day") >= 4, F.year("date_day"))
             .otherwise(F.year("date_day") - 1)
        )
    )

    dim_date.write.format("delta").mode("overwrite").saveAsTable("main.marts.dim_date")
    print(f"dim_date refreshed — {dim_date.count()} rows")
```

**Functional difference between SQL and Python:** `SEQUENCE + EXPLODE` in a Spark SQL task runs on a SQL Warehouse and is the direct native equivalent of `dbt_utils.date_spine`. The PySpark function is more flexible — it can be embedded in DLT pipelines or Workflows Python tasks and accepts parameterised start/end dates. Both generate identical output. Prefer the SQL approach for standalone date dimension refreshes scheduled as Workflows SQL tasks; use the Python approach when the date spine is one step in a larger PySpark pipeline.

#### Validation — SQL

```sql
-- Verify no gaps in the date spine (every consecutive date pair is 1 day apart)
WITH consecutive AS (
    SELECT
        date_id,
        LEAD(date_id) OVER (ORDER BY date_id) AS next_date
    FROM main.marts.dim_date
)
SELECT COUNT(*) AS gap_count
FROM consecutive
WHERE next_date IS NOT NULL
  AND DATEDIFF(next_date, date_id) != 1;
-- Expected: 0

-- Verify date spine covers the expected range
SELECT MIN(date_id) AS first_date, MAX(date_id) AS last_date
FROM main.marts.dim_date;
-- Expected: first_date = 2015-01-01, last_date = approximately 2 years from today

-- Verify time-series mart has no gaps for an active customer
SELECT activity_date, order_count
FROM main.marts.mart_customer_daily_activity
WHERE customer_id = 'C-001'
  AND activity_date BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY activity_date;
-- Expected: 31 rows, one per day, order_count = 0 for days with no orders
```

### Discussion and Concerns

- **Large date ranges produce large tables:** A date spine from 2015 to 2027 contains approximately 4,400 rows. Cross-joining with 1 million customers produces 4.4 billion rows. Materialise the cross-join mart carefully — consider whether a pre-aggregated version (e.g., monthly totals per customer) is sufficient. Use `PARTITION BY activity_date` and `CLUSTER BY customer_id` to improve query pruning.
- **Materialise the date dimension as a table and refresh infrequently:** The `dim_date` table only needs to be rebuilt when the end date extends (e.g., annually to add the new year). Run the rebuild as an infrequent Workflows job — not as part of the nightly mart refresh.
- **Use as a spine in PIT models:** The same `SEQUENCE + EXPLODE` pattern is used to generate the date spine in PIT table construction (see [dv2_business_vault_cookbook.md — PIT Tables](./dv2_business_vault_cookbook.md#point-in-time-pit-tables)).
- **Fiscal calendar adjustments:** The example implements a fiscal year starting April 1. Adjust the `CASE` expression to match your organisation's fiscal calendar. Document the fiscal year definition in a comment block at the top of the SQL.

### See Also

- [Databricks Spark SQL SEQUENCE function](https://docs.databricks.com/en/sql/language-manual/functions/sequence.html)
- [Databricks Spark SQL EXPLODE function](https://docs.databricks.com/en/sql/language-manual/functions/explode.html)
- [dv2_business_vault_cookbook.md — PIT Tables](./dv2_business_vault_cookbook.md#point-in-time-pit-tables)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Mart row count vs. expected | `SELECT COUNT(*) FROM main.marts.dim_customer` after each refresh | Sudden drop indicates a PIT or staging failure upstream |
| Mart freshness timestamp | `SELECT MAX(mart_refreshed_at) FROM main.marts.fct_orders` | Stale timestamp (older than the refresh SLA) indicates a pipeline failure |
| Query performance on star schema | SQL Warehouse query history / EXPLAIN output | Full table scans on large satellites indicate missing PIT join; check query plan |
| Null foreign keys in fact tables | Assertion queries on `customer_id`, `order_number` | Any null FK indicates a broken join to a dimension or a missing hub row |
| Date spine gaps | Gap check query on `main.marts.dim_date.date_id` | Any gap in the date series breaks time-series aggregations |
| Dim table row count stability | Monitor `SELECT COUNT(*) FROM main.marts.dim_customer` over time | Unexpected large increases may indicate PIT misconfiguration (duplicates) |

### Metrics for Success

- [ ] Mart refreshes complete within the reporting SLA (e.g., nightly batch completes before 07:00)
- [ ] Dimension tables have exactly one row per business key (`CUSTOMER_ID` unique in `dim_customer`)
- [ ] Fact tables have no null foreign keys (`customer_id IS NOT NULL` and `order_number IS NOT NULL`)
- [ ] Date spine in `dim_date` has no gaps from the minimum start date to the maximum end date
- [ ] All mart queries return results consistent with the PIT snapshot date (no unexpectedly stale attributes)
- [ ] EXPLAIN output for key mart queries shows partition pruning and no full scans across large satellite tables
