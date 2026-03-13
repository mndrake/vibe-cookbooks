# Data Vault 2.0 Information Mart Cookbook

## Introduction

This cookbook provides practical, step-by-step guidance for **building Information Marts** from a Data Vault 2.0 Raw Vault and Business Vault on Databricks using dbt and dbt-utils. It covers star schema construction from PIT and Bridge tables, flat wide table patterns, EAV pivot, and date spine generation for time-series reporting.

Information Mart models are the consumer-facing layer of the architecture. They must be simple, fast, and correct — all vault complexity is resolved in the Business Vault before the mart is built.

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

> **On Databricks:** dbt does not run on Databricks clusters. It runs on your local machine and submits SQL to Databricks via the SQL Warehouse HTTP path. PySpark and Delta Lake are pre-installed on every Databricks cluster — no local installation of these is needed to run dbt models.
>
> **Local development:** All tools below are installed on your local machine.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.9+ | Local dev | Required for dbt-databricks |
| [dbt-databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup) | 1.7.x | Local dev | Core transformation framework — runs locally, connects to Databricks |
| [dbt-utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) | 1.3.0 | Local dev | pivot, date_spine, and test macros; installed via `dbt deps` |
| [automate_dv](https://automate-dv.readthedocs.io/en/latest/) | 0.10.2 | Local dev | Required for PIT and Bridge (upstream dependency); installed via `dbt deps` |
| Databricks CLI | Latest | Local dev | Workspace interaction and secrets management |

Configure your dbt profile in `~/.dbt/profiles.yml`:

```yaml
data_vault_project:
  target: dev
  outputs:
    dev:
      type: databricks
      host: your-workspace.azuredatabricks.net
      http_path: /sql/1.0/warehouses/your_warehouse_id
      token: "{{ env_var('DBT_TOKEN') }}"
      schema: marts
      catalog: main
```

### Getting a New Starter Project

```bash
dbt init data_vault_project
cd data_vault_project
dbt deps    # Install dbt-utils and automate_dv
dbt debug   # Verify connection
```

### Getting the Source for an Existing Project

```bash
git clone <repository_url>
cd data_vault_project
pip install -r requirements.txt
dbt deps
dbt debug
```

---

## Infrastructure Pre-Requisites

### Infrastructure Required

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment | Unity Catalog enabled |
| SQL Warehouse (Serverless or Pro) | Compute for dbt runs | Photon enabled; size for concurrent BI queries |
| Unity Catalog — `marts` schema | Target for dimension and fact tables | dbt service principal needs `CREATE TABLE`, `INSERT`, `SELECT` |
| Unity Catalog — `business_vault` schema | Source for PIT and Bridge tables | `SELECT` privilege required |
| Unity Catalog — `raw_vault` schema | Source for transactional links and reference data | `SELECT` privilege required |

### Marts Schema Setup

```sql
-- Create the marts schema
CREATE SCHEMA IF NOT EXISTS main.marts
  COMMENT 'Data Vault 2.0 information marts — star schema dimensions and facts for BI consumption';

-- Grant privileges to the dbt service principal
GRANT CREATE TABLE, INSERT, SELECT ON SCHEMA main.marts
  TO `dbt-service-principal@your-org.com`;

-- BI tool service account and analysts get SELECT only
GRANT SELECT ON SCHEMA main.marts
  TO `bi-service-account@your-org.com`;

GRANT SELECT ON SCHEMA main.marts
  TO `data-analysts@your-org.com`;
```

---

## Star Schema from Vault

Star schema Information Marts are the primary delivery artefact for BI tools, dashboards, and analyst workbooks. They present a clean, denormalised view of the business domain with one row per business key in dimensions and one row per event at the defined grain in fact tables.

### Problem

The vault graph — hubs, links, and satellites — is designed for auditability and parallel loading, not for BI query performance. Querying across five satellites for a single hub, filtering to the correct point in time, and navigating two or more links to build a fact table requires vault expertise that BI consumers and dashboard tools do not have. The mart must hide this complexity entirely.

### Solution

Use PIT and Bridge tables built in the Business Vault as the foundation for mart models. Mart models join the PIT table to its satellites (using equality joins on the stored load dates) to build dimension tables. Fact tables are built by joining the Bridge table to transactional links and their satellites.

#### SQL Example — `models/marts/dim_customer.sql`

```sql
-- Dimension table for CUSTOMER.
-- One row per customer (current snapshot from PIT as of today).
-- Source: pit_customer (Business Vault) → sat_customer_details + sat_customer_marketing.
-- Never queries raw vault directly — all joins go through PIT.

{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

WITH pit AS (
    -- Get today's PIT snapshot (AS_OF_DATE = current date)
    SELECT
        CUSTOMER_HK,
        SAT_CUSTOMER_DETAILS_LDTS,
        SAT_CUSTOMER_MARKETING_LDTS
    FROM {{ ref('pit_customer') }}
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
    FROM {{ ref('sat_customer_details') }}
),

customer_marketing AS (
    SELECT
        CUSTOMER_HK,
        MARKETING_SEGMENT,
        OPTED_IN_EMAIL,
        OPTED_IN_SMS,
        PREFERRED_CHANNEL,
        LOAD_DATE
    FROM {{ ref('sat_customer_marketing') }}
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
INNER JOIN {{ ref('hub_customer') }}    h  ON pit.CUSTOMER_HK = h.CUSTOMER_HK
INNER JOIN customer_details             d  ON pit.CUSTOMER_HK = d.CUSTOMER_HK
                                         AND pit.SAT_CUSTOMER_DETAILS_LDTS = d.LOAD_DATE
LEFT  JOIN customer_marketing           m  ON pit.CUSTOMER_HK = m.CUSTOMER_HK
                                         AND pit.SAT_CUSTOMER_MARKETING_LDTS = m.LOAD_DATE
```

#### SQL Example — `models/marts/fct_orders.sql`

```sql
-- Fact table for ORDER events.
-- One row per order at the (customer, order, order_date) grain.
-- Source: bridge_customer_orders → t_link_payment + sat_order_details.

{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

WITH bridge AS (
    SELECT
        CUSTOMER_HK,
        ORDER_HK,
        CUSTOMER_ORDER_HK,
        AS_OF_DATE
    FROM {{ ref('bridge_customer_orders') }}
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
    FROM {{ ref('sat_order_details') }}
),

order_pit AS (
    SELECT
        ORDER_HK,
        SAT_ORDER_DETAILS_LDTS
    FROM {{ ref('pit_order') }}
    WHERE AS_OF_DATE = CURRENT_DATE()
)

SELECT
    -- Surrogate key for the fact table (not a vault hash key)
    {{ dbt_utils.generate_surrogate_key(['b.CUSTOMER_HK', 'b.ORDER_HK']) }}
                                        AS order_fact_sk,

    -- Foreign keys to dimensions
    hc.CUSTOMER_ID                      AS customer_id,
    ho.ORDER_NUMBER                     AS order_number,

    -- Hash keys for vault lineage tracing
    b.CUSTOMER_HK                       AS customer_hk,
    b.ORDER_HK                          AS order_hk,

    -- Degenerate dimensions
    od.ORDER_DATE                       AS order_date,
    od.ORDER_STATUS                     AS order_status,

    -- Measures
    od.TOTAL_AMOUNT                     AS order_total_amount,
    od.CURRENCY_CODE                    AS currency_code,

    -- Mart metadata
    CURRENT_TIMESTAMP()                 AS mart_refreshed_at

FROM bridge b
INNER JOIN {{ ref('hub_customer') }}    hc ON b.CUSTOMER_HK = hc.CUSTOMER_HK
INNER JOIN {{ ref('hub_order') }}       ho ON b.ORDER_HK    = ho.ORDER_HK
INNER JOIN order_pit                    op ON b.ORDER_HK    = op.ORDER_HK
INNER JOIN order_details                od ON op.ORDER_HK   = od.ORDER_HK
                                         AND op.SAT_ORDER_DETAILS_LDTS = od.LOAD_DATE
```

#### Python Note

Information mart models are built using dbt SQL models. There is no direct PySpark equivalent for the PIT-based join pattern. PySpark can construct the same SELECT logic, but the PIT and Bridge tables are the critical upstream dependency — without them, the join logic must be reproduced in PySpark, which introduces the same fragility the PIT and Bridge are designed to eliminate. Use dbt SQL models for all mart construction.

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
- **Use `LEFT JOIN` for optional satellite relationships:** If a customer may not have a marketing satellite row (e.g., B2B customers are not marketed to), use `LEFT JOIN` for that satellite. `INNER JOIN` against a satellite that has no row for some hub records silently drops those hub records from the dimension.
- **Include vault hash keys in the fact table:** Retaining `CUSTOMER_HK` and `ORDER_HK` in the fact table allows vault lineage tracing (joining back to the raw vault for audit purposes) without exposing natural keys that may change across source systems.
- **Mart surrogate keys vs. vault hash keys:** Use `dbt_utils.generate_surrogate_key` to create a mart-layer surrogate key for the fact table grain. This is distinct from the vault hash key — it is a convenience key for the BI layer, not a vault structure.

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

Build a dbt model that joins multiple satellites via the PIT table into a single wide table. The PIT table ensures time-consistent attribute resolution across all satellites for each customer.

#### SQL Example — `models/marts/wide_customer.sql`

```sql
-- Wide flat table combining three customer satellites into a single denormalised row.
-- One row per customer; all attributes from demographics, contact, and preferences satellites.
-- Intended for data science feature stores and bulk export workflows.

{{
    config(
        materialized='table',
        tags=['marts', 'wide_table']
    )
}}

WITH pit AS (
    SELECT
        CUSTOMER_HK,
        SAT_CUSTOMER_DETAILS_LDTS,
        SAT_CUSTOMER_CONTACT_LDTS,
        SAT_CUSTOMER_PREFERENCES_LDTS
    FROM {{ ref('pit_customer') }}
    WHERE AS_OF_DATE = CURRENT_DATE()
),

demographics AS (
    SELECT CUSTOMER_HK, LOAD_DATE,
           FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, GENDER
    FROM {{ ref('sat_customer_demographics') }}
),

contact AS (
    SELECT CUSTOMER_HK, LOAD_DATE,
           EMAIL_ADDRESS, PHONE_NUMBER, BILLING_ADDRESS, CITY, POSTCODE, COUNTRY_CODE
    FROM {{ ref('sat_customer_contact') }}
),

preferences AS (
    SELECT CUSTOMER_HK, LOAD_DATE,
           PREFERRED_LANGUAGE, PREFERRED_CURRENCY, PREFERRED_CHANNEL,
           NEWSLETTER_OPT_IN, PRODUCT_CATEGORY_INTEREST
    FROM {{ ref('sat_customer_preferences') }}
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
INNER JOIN {{ ref('hub_customer') }} h
    ON pit.CUSTOMER_HK = h.CUSTOMER_HK
LEFT JOIN demographics dem
    ON pit.CUSTOMER_HK = dem.CUSTOMER_HK
   AND pit.SAT_CUSTOMER_DETAILS_LDTS = dem.LOAD_DATE
LEFT JOIN contact con
    ON pit.CUSTOMER_HK = con.CUSTOMER_HK
   AND pit.SAT_CUSTOMER_CONTACT_LDTS = con.LOAD_DATE
LEFT JOIN preferences pref
    ON pit.CUSTOMER_HK = pref.CUSTOMER_HK
   AND pit.SAT_CUSTOMER_PREFERENCES_LDTS = pref.LOAD_DATE
```

#### Python Note

There is no PySpark equivalent for this pattern that is preferable to the SQL dbt model. A Python dbt model could replicate the logic using multiple DataFrame joins, but the PIT equality join (`pit.LOAD_DATE = sat.LOAD_DATE`) is most naturally expressed in SQL. Use the SQL dbt model.

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
-- Expected: 0 (LEFT JOIN means customers without demographics satellite will have NULL — investigate those separately)
```

### Discussion and Concerns

- **Wide tables are expensive to maintain:** Any change to a satellite's payload columns, or the addition of a new satellite to the wide table, requires a full rebuild of the wide table. For large customer tables (tens of millions of rows), this rebuild can be slow. Use incremental materialisation where the wide table can be partitioned by a date column.
- **Document the satellite dependencies:** Add a comment block at the top of the model listing which satellites feed each column group. When a satellite changes, this comment identifies which wide tables are affected.
- **Data science consumers may prefer Delta Sharing:** For data science teams that need fresh data frequently, consider exposing the wide table via Delta Sharing rather than a direct Unity Catalog query. This decouples their access from the mart refresh schedule.
- **Avoid very wide tables in BI tools:** Wide tables with 100+ columns cause performance issues in tools like Power BI and Tableau. For BI consumers, prefer the star schema. Wide tables are best suited for bulk export, ML feature stores, and programmatic access.

### See Also

- [Delta Lake Incremental Materialisation — dbt-databricks](https://docs.getdbt.com/reference/resource-configs/databricks-configs#incremental-models)
- [Databricks Feature Store](https://docs.databricks.com/en/machine-learning/feature-store/index.html)

---

## dbt-utils pivot for EAV Satellites

Some satellite data arrives in a key-value (Entity-Attribute-Value, or EAV) structure where each row stores an attribute name and its value, rather than having one column per attribute. BI tools require this to be pivoted into columnar form.

### Problem

A product attributes satellite stores data as `(PRODUCT_HK, ATTRIBUTE_NAME, ATTRIBUTE_VALUE)` rows. Each product has up to 12 attributes: weight, dimensions, colour, material, brand, and so on. The source system emits these as key-value pairs rather than as a fixed-width record. Querying this structure in a BI tool requires pivoting on `ATTRIBUTE_NAME`, which the tool cannot do natively.

### Solution

Use `dbt_utils.pivot()` to generate a pivoted SQL SELECT statement at compile time. Combine with `dbt_utils.get_column_values()` to retrieve the distinct attribute names dynamically from the source table.

#### SQL Example — `models/marts/dim_product_attributes.sql`

```sql
-- Dimension table for PRODUCT attributes, pivoted from EAV satellite format.
-- sat_product_attributes stores (PRODUCT_HK, ATTRIBUTE_NAME, ATTRIBUTE_VALUE).
-- This model pivots ATTRIBUTE_NAME into columns for BI consumption.
-- dbt_utils.get_column_values() retrieves the distinct attribute names at compile time.

{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'pivot']
    )
}}

-- Retrieve distinct attribute names from the satellite at compile time
-- This runs a query against the database during dbt compile — requires the source table to exist
{%- set attribute_names = dbt_utils.get_column_values(
    table=ref('sat_product_attributes_latest'),
    column='ATTRIBUTE_NAME'
) -%}

WITH latest_attributes AS (
    -- Get the most recent attribute value per (PRODUCT_HK, ATTRIBUTE_NAME)
    SELECT
        PRODUCT_HK,
        ATTRIBUTE_NAME,
        ATTRIBUTE_VALUE
    FROM {{ ref('sat_product_attributes_latest') }}
)

SELECT
    h.PRODUCT_SKU                       AS product_sku,
    h.PRODUCT_HK                        AS product_hk,

    -- Pivot each ATTRIBUTE_NAME into a column
    -- dbt_utils.pivot generates: MAX(CASE WHEN ATTRIBUTE_NAME = 'weight' THEN ATTRIBUTE_VALUE END) AS weight
    {{ dbt_utils.pivot(
        column='ATTRIBUTE_NAME',
        values=attribute_names,
        agg='MAX',
        then_value='ATTRIBUTE_VALUE',
        else_value='NULL',
        quote_identifiers=false
    ) }}

FROM latest_attributes la
INNER JOIN {{ ref('hub_product') }} h ON la.PRODUCT_HK = h.PRODUCT_HK
GROUP BY h.PRODUCT_SKU, h.PRODUCT_HK
```

The `sat_product_attributes_latest` model referenced above is a Business Vault model that pre-filters to the most recent row per (PRODUCT_HK, ATTRIBUTE_NAME):

```sql
-- models/business_vault/sat_product_attributes_latest.sql
-- Pre-filtered view of the product attributes satellite: most recent row per attribute per product.

{{
    config(materialized='table', tags=['business_vault', 'derived_rules'])
}}

WITH ranked AS (
    SELECT
        PRODUCT_HK,
        ATTRIBUTE_NAME,
        ATTRIBUTE_VALUE,
        LOAD_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_HK, ATTRIBUTE_NAME
            ORDER BY LOAD_DATE DESC
        ) AS rn
    FROM {{ ref('sat_product_attributes') }}
)
SELECT PRODUCT_HK, ATTRIBUTE_NAME, ATTRIBUTE_VALUE, LOAD_DATE
FROM ranked
WHERE rn = 1
```

To inspect the SQL that `dbt_utils.pivot` generates, run `dbt compile --select dim_product_attributes` and inspect `target/compiled/your_project/models/marts/dim_product_attributes.sql`. For the attribute values `['weight', 'colour', 'material']`, the generated SQL is:

```sql
-- Generated by dbt_utils.pivot — do not edit directly
MAX(CASE WHEN ATTRIBUTE_NAME = 'weight'   THEN ATTRIBUTE_VALUE END) AS weight,
MAX(CASE WHEN ATTRIBUTE_NAME = 'colour'   THEN ATTRIBUTE_VALUE END) AS colour,
MAX(CASE WHEN ATTRIBUTE_NAME = 'material' THEN ATTRIBUTE_VALUE END) AS material
```

#### Python Note

There is no dbt macro equivalent in PySpark. The PySpark `pivot()` function provides similar functionality:

```python
# PySpark pivot equivalent — for understanding only, not for use in dbt SQL models
from pyspark.sql import functions as F

df = spark.table("main.business_vault.sat_product_attributes_latest")

pivoted = (
    df
    .groupBy("PRODUCT_HK")
    .pivot("ATTRIBUTE_NAME")   # PySpark pivot — equivalent to dbt_utils.pivot
    .agg(F.first("ATTRIBUTE_VALUE"))
)
```

**Functional difference:** PySpark `pivot()` determines column values at runtime and can handle dynamic attribute sets. `dbt_utils.pivot()` with `get_column_values()` determines column values at compile time — the schema of the output table is fixed at the time `dbt run` is called, not at query time. If a new attribute is added to the satellite after the model was last compiled, it will not appear in the output until `dbt run` is executed again.

#### Validation — SQL

```sql
-- Verify one row per product in the pivoted dimension
SELECT product_sku, COUNT(*) AS cnt
FROM main.marts.dim_product_attributes
GROUP BY product_sku
HAVING cnt > 1;
-- Expected: 0 rows

-- Verify columns were generated correctly (run after dbt compile)
DESCRIBE TABLE main.marts.dim_product_attributes;
-- Should show one column per distinct ATTRIBUTE_NAME from sat_product_attributes_latest
```

### Discussion and Concerns

- **`get_column_values()` runs at compile time:** The list of pivot columns is determined when `dbt compile` or `dbt run` executes. If new attribute names are added to the satellite between pipeline runs, they will appear in the pivoted output only after the next `dbt run`. This is generally acceptable for slowly-changing reference attributes.
- **Large numbers of attributes create very wide tables:** Pivoting an EAV satellite with 200 distinct attribute names produces a 200-column table. BI tools handle this poorly and Spark SQL compiles the CASE expression block for every row. Consider whether a JSON column (e.g., `MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(ATTRIBUTE_NAME, ATTRIBUTE_VALUE)))`) is more appropriate for high-cardinality attribute sets.
- **All pivoted columns have the same data type:** `dbt_utils.pivot` generates `MAX(CASE WHEN ... THEN ATTRIBUTE_VALUE END)`, so all columns inherit the type of `ATTRIBUTE_VALUE`. If attributes have different types (integer weight, string colour), they are all returned as STRING. Cast in a downstream model or mart if specific types are required.

### See Also

- [dbt-utils pivot Documentation](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#pivot-source)
- [dbt-utils get_column_values](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#get_column_values-source)
- [PySpark pivot function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.pivot.html)

---

## dbt-utils date_spine for Time-Series Marts

A date spine is a continuous sequence of dates (or other time granularities) used as the backbone for time-series reporting. It ensures that every date has a row in the mart, including dates on which no events occurred — preventing gaps in trend charts and running totals.

### Problem

A customer activity report needs to show daily order counts for the last 365 days. On days when a customer placed no orders, the report must still show a zero rather than a gap in the time series. Simply aggregating the orders fact table by date will omit dates with no orders; joining to a pre-built calendar table requires that table to exist and be maintained separately.

### Solution

Use `dbt_utils.date_spine()` to generate a continuous date series inline in a dbt model. Join the date spine to the PIT table or fact table to ensure every date has a row.

#### SQL Example — `models/marts/mart_customer_daily_activity.sql`

```sql
-- Time-series mart: customer daily order activity.
-- One row per (customer, date) combination for every date in the reporting window.
-- Dates with no orders show order_count = 0 and order_total = 0.
-- Uses date_spine to guarantee no gaps in the date series.

{{
    config(
        materialized='table',
        tags=['marts', 'time_series']
    )
}}

WITH date_spine AS (
    -- Generate one row per day from 2020-01-01 to today
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(current_date() as date)"
    ) }}
),

customers AS (
    SELECT CUSTOMER_HK, CUSTOMER_ID
    FROM {{ ref('hub_customer') }}
),

-- Cross join customers with the date spine to create the complete grid
customer_date_grid AS (
    SELECT
        c.CUSTOMER_HK,
        c.CUSTOMER_ID,
        ds.date_day
    FROM customers c
    CROSS JOIN date_spine ds
    -- Limit to dates on or after the customer's first appearance in the vault
    INNER JOIN {{ ref('hub_customer') }} hc ON c.CUSTOMER_HK = hc.CUSTOMER_HK
    WHERE ds.date_day >= CAST(hc.LOAD_DATE AS DATE)
),

daily_orders AS (
    SELECT
        hc.CUSTOMER_ID,
        CAST(od.ORDER_DATE AS DATE)             AS order_date,
        COUNT(DISTINCT fo.order_number)         AS order_count,
        SUM(fo.order_total_amount)              AS order_total
    FROM {{ ref('fct_orders') }} fo
    INNER JOIN {{ ref('dim_customer') }} hc ON fo.customer_id = hc.customer_id
    INNER JOIN {{ ref('fct_orders') }} od ON fo.order_fact_sk = od.order_fact_sk
    GROUP BY hc.CUSTOMER_ID, CAST(od.ORDER_DATE AS DATE)
)

SELECT
    g.CUSTOMER_ID                               AS customer_id,
    g.date_day                                  AS activity_date,
    COALESCE(o.order_count,  0)                 AS order_count,
    COALESCE(o.order_total,  0.00)              AS order_total,
    CURRENT_TIMESTAMP()                         AS mart_refreshed_at
FROM customer_date_grid g
LEFT JOIN daily_orders o
    ON g.CUSTOMER_ID = o.CUSTOMER_ID
   AND g.date_day    = o.order_date
```

For a standalone date dimension (calendar table), generate the spine independently:

```sql
-- models/marts/dim_date.sql
-- Standalone date dimension generated from a date spine.
-- Refresh infrequently — only needs to be rebuilt when the end date extends.

{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'date']
    )
}}

WITH spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast(current_date() + interval 2 years as date)"
    ) }}
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
FROM spine
```

The SQL that `dbt_utils.date_spine` generates for the `day` datepart on Databricks:

```sql
-- Generated by dbt_utils.date_spine — do not edit directly
WITH rawdata AS (
    SELECT EXPLODE(SEQUENCE(
        CAST('2020-01-01' AS DATE),
        CAST(current_date() AS DATE),
        INTERVAL 1 DAY
    )) AS date_day
)
SELECT date_day FROM rawdata
```

#### Python Note

In PySpark, a date spine can be generated using `spark.sql` with the `SEQUENCE` function, or programmatically with `pandas.date_range`:

```python
# PySpark equivalent of dbt_utils.date_spine
# Use this in Python dbt models or notebooks where the date spine is needed alongside PySpark logic

from pyspark.sql import functions as F
from datetime import date

start_date = date(2020, 1, 1)
end_date = date.today()

date_spine_df = spark.sql(f"""
    SELECT EXPLODE(SEQUENCE(
        DATE '{start_date}',
        DATE '{end_date}',
        INTERVAL 1 DAY
    )) AS date_day
""")
```

**Functional difference between SQL and Python:** `dbt_utils.date_spine()` in a dbt SQL model is compiled to a `SEQUENCE + EXPLODE` expression that runs entirely on the SQL Warehouse. The PySpark version runs on a cluster. For standalone date spine generation in a mart, the SQL macro is preferred. For date spine operations embedded in complex PySpark transformations, the PySpark approach is more natural.

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

- **Large date ranges produce large tables:** A date spine from 2015 to 2027 contains approximately 4,400 rows. Cross-joining with 1 million customers produces 4.4 billion rows. Materialise the cross-join mart carefully — consider whether it is needed at full grain or whether a pre-aggregated version (e.g., monthly totals per customer) is sufficient.
- **Materialise the date dimension as a table and refresh infrequently:** The `dim_date` table only needs to be rebuilt when the end date extends (e.g., annually to add the new year). Use a `--full-refresh` flag explicitly rather than scheduling daily rebuilds.
- **Use as a spine in PIT models:** The `date_range` parameter in AutomateDV's `pit()` macro accepts a date range that defines the spine over which PIT snapshots are generated. For daily PIT snapshots, the date spine granularity must match.
- **Fiscal calendar adjustments:** The example above implements a fiscal year starting April 1. Adjust the `CASE` expression to match your organisation's fiscal calendar. Document the fiscal year definition in the model's `schema.yml` description.

### See Also

- [dbt-utils date_spine Documentation](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#date_spine-source)
- [AutomateDV PIT date_range](https://automate-dv.readthedocs.io/en/latest/macros/pit/)
- [dv2_business_vault_cookbook.md — PIT Tables](./dv2_business_vault_cookbook.md#point-in-time-pit-tables)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Mart row count vs. expected | `SELECT COUNT(*) FROM dim_customer` after each refresh | Sudden drop indicates a PIT or staging failure upstream |
| Mart freshness timestamp | `SELECT MAX(mart_refreshed_at) FROM fct_orders` | Stale timestamp (older than the refresh SLA) indicates a pipeline failure |
| Query performance on star schema | SQL Warehouse query history / EXPLAIN output | Full table scans on large satellites indicate missing PIT join; check query plan |
| Null foreign keys in fact tables | dbt test `not_null` on `customer_id`, `order_number` | Any null FK indicates a broken join to a dimension or a missing hub row |
| Date spine gaps | dbt test `dbt_utils.sequential_values` on `dim_date.date_id` | Any gap in the date series breaks time-series aggregations |
| Dim table row count stability | Monitor `SELECT COUNT(*) FROM dim_customer` over time | Unexpected large increases may indicate PIT misconfiguration (duplicates) |

### Metrics for Success

- [ ] Mart refreshes complete within the reporting SLA (e.g., nightly batch completes before 07:00)
- [ ] Dimension tables have exactly one row per business key (`CUSTOMER_ID` unique in `dim_customer`)
- [ ] Fact tables have no null foreign keys (`customer_id IS NOT NULL` and `order_number IS NOT NULL`)
- [ ] Date spine in `dim_date` has no gaps from the minimum start date to the maximum end date
- [ ] All mart queries return results consistent with the PIT snapshot date (no unexpectedly stale attributes)
- [ ] EXPLAIN output for key mart queries shows partition pruning and no full scans across large satellite tables
