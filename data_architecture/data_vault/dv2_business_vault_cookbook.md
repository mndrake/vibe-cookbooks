# Data Vault 2.0 Business Vault Cookbook

## Introduction

This cookbook provides practical, step-by-step guidance for **building the Business Vault layer** on Databricks using dbt, AutomateDV, and dbt-utils. The Business Vault sits between the Raw Vault and the Information Mart: it applies business rules, derives metrics, and pre-computes complex join paths (via PIT and Bridge tables) that make mart queries fast and maintainable.

All Business Vault models read from Raw Vault structures. They never modify Raw Vault tables and are always rebuilt or incrementally maintained separately.

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.9+ | Required for dbt-databricks |
| [dbt-databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup) | 1.7.x | Core transformation framework |
| [automate_dv](https://automate-dv.readthedocs.io/en/latest/) | 0.10.2 | PIT and Bridge macro library |
| [dbt-utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) | 1.3.0 | date_spine and test macros |
| Databricks CLI | Latest | Workspace interaction |

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
      schema: business_vault
      catalog: main
```

### Getting a New Starter Project

```bash
dbt init data_vault_project
cd data_vault_project
dbt deps    # Install automate_dv and dbt-utils
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
| SQL Warehouse (Serverless or Pro) | Compute for dbt runs | Photon enabled |
| Unity Catalog — `business_vault` schema | Target for business vault tables | dbt service principal needs `CREATE TABLE`, `INSERT`, `SELECT` |
| Unity Catalog — `raw_vault` schema | Source for all business vault models | `SELECT` privilege required |

### Business Vault Schema Setup

```sql
-- Create the business_vault schema
CREATE SCHEMA IF NOT EXISTS main.business_vault
  COMMENT 'Data Vault 2.0 business vault — derived rules, PIT, and bridge tables';

-- Grant privileges to the dbt service principal
GRANT CREATE TABLE, INSERT, SELECT ON SCHEMA main.business_vault
  TO `dbt-service-principal@your-org.com`;

-- Data engineers get SELECT for debugging
GRANT SELECT ON SCHEMA main.business_vault
  TO `data-engineers@your-org.com`;
```

---

## Derived Business Rules

Derived business rules are dbt models that read from Raw Vault structures (typically satellites or hubs) and add computed columns, derived identifiers, or soft business logic without modifying any Raw Vault table.

### Problem

The customer satellite stores `FIRST_NAME` and `LAST_NAME` as separate columns because that is how the source CRM delivers them. Downstream consumers — marts, dashboards, and ML pipelines — all need a single `FULL_NAME` column. Adding this derivation in every downstream query is a maintenance burden; it must live in a single, reusable business vault model. Similarly, a canonical `CUSTOMER_TIER` (Gold / Silver / Bronze) is computed from the customer's lifetime spend, which is a business rule that may change over time but must not modify the raw satellite history.

### Solution

Create a dbt model in `models/business_vault/` that SELECT from the most recent satellite version and adds computed columns.

#### SQL Example — `models/business_vault/bv_customer_derived.sql`

```sql
-- Business Vault: Derived customer attributes.
-- Reads the latest version of each customer's attributes from the satellite,
-- then adds FULL_NAME (concatenated) and CUSTOMER_EMAIL_DOMAIN (extracted).
-- This model does NOT modify sat_customer_details.

{{
    config(
        materialized='table',
        tags=['business_vault', 'derived_rules']
    )
}}

WITH latest_customer AS (
    -- Select only the most recent satellite row per customer hash key
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
    FROM {{ ref('sat_customer_details') }}
),

current_customer AS (
    SELECT * FROM latest_customer WHERE rn = 1
)

SELECT
    c.CUSTOMER_HK,
    h.CUSTOMER_ID,

    -- Derived: full name from satellite payload columns
    TRIM(CONCAT(
        COALESCE(SPLIT(c.CUSTOMER_NAME, ',')[1], ''), ' ',
        COALESCE(SPLIT(c.CUSTOMER_NAME, ',')[0], '')
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
INNER JOIN {{ ref('hub_customer') }} h
    ON c.CUSTOMER_HK = h.CUSTOMER_HK
```

#### Python Example — `models/business_vault/bv_customer_derived.py`

```python
# Python dbt model equivalent for bv_customer_derived.
# Use when the derivation logic is more naturally expressed in PySpark
# (e.g., complex string parsing, ML scoring, or UDF application).

import pyspark.sql.functions as F
from pyspark.sql.window import Window


def model(dbt, spark):
    dbt.config(
        materialized="table",
        tags=["business_vault", "derived_rules"]
    )

    sat = dbt.ref("sat_customer_details")
    hub = dbt.ref("hub_customer")

    # Select most recent satellite row per customer
    window = Window.partitionBy("CUSTOMER_HK").orderBy(F.col("LOAD_DATE").desc())
    current_customer = (
        sat
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Derive FULL_NAME and EMAIL_DOMAIN
    derived = current_customer.withColumn(
        "FULL_NAME",
        F.trim(F.concat_ws(
            " ",
            F.trim(F.element_at(F.split(F.col("CUSTOMER_NAME"), ","), 2)),
            F.trim(F.element_at(F.split(F.col("CUSTOMER_NAME"), ","), 1))
        ))
    ).withColumn(
        "EMAIL_DOMAIN",
        F.lower(F.element_at(F.split(F.col("EMAIL_ADDRESS"), "@"), 2))
    )

    # Join to hub to pick up the natural key
    result = derived.join(
        hub.select("CUSTOMER_HK", "CUSTOMER_ID"),
        on="CUSTOMER_HK",
        how="inner"
    )

    return result.select(
        "CUSTOMER_HK", "CUSTOMER_ID", "FULL_NAME", "CUSTOMER_NAME",
        "EMAIL_ADDRESS", "EMAIL_DOMAIN", "PHONE_NUMBER",
        "BILLING_ADDRESS", "CITY", "POSTCODE", "COUNTRY_CODE",
        F.col("LOAD_DATE").alias("LAST_UPDATED")
    )
```

**Functional difference between SQL and Python dbt models:** The SQL model runs entirely on the SQL Warehouse using Spark SQL. The Python model runs on a Spark cluster (not SQL Warehouse). Python models support UDFs, MLflow model scoring, and complex Python libraries. SQL models are simpler to debug and run faster for standard transformations. Prefer SQL unless Python-specific capabilities are required.

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
- **Materialise as a table, not a view:** Business vault models that are queried by PIT or Bridge models should be materialised as Delta tables for performance. Views that reach back into raw vault satellites over large histories are expensive.
- **Document each rule:** Each computed column should include an inline SQL comment or dbt `description` in `schema.yml` explaining the business logic. Rules change — without documentation, future engineers cannot safely modify them.
- **Full-refresh vs. incremental:** If the derived model reads from the full satellite history to compute a current-state snapshot (like `bv_customer_derived` above), it must be full-refresh. If it derives attributes row-by-row from a satellite without look-back, it can be incremental.

### See Also

- [dbt Python Models on Databricks](https://docs.databricks.com/en/partners/prep/dbt.html)
- [dv2_raw_vault_cookbook.md — Satellite Loading](./dv2_raw_vault_cookbook.md#satellite-loading)
- [dv2_architecture.md — Business Vault](./dv2_architecture.md#business-vault)

---

## Point-in-Time (PIT) Tables

Point-in-Time tables are Business Vault structures that resolve the correct satellite version for a given Hub record at any given point in time. They eliminate the need for complex multi-way joins and window functions in every downstream mart query.

### Problem

Querying a customer's attributes as they were on a specific date requires joining the hub to multiple satellites (demographics, contact details, preferences) and applying a `WHERE LOAD_DATE <= target_date` filter with a `MAX(LOAD_DATE)` per customer per satellite. Across five satellites, this produces five separate window function subqueries. Every downstream mart model repeats this logic, making the codebase fragile and expensive to query.

### Solution

The `automate_dv.pit` macro generates a PIT table that stores, for each customer and each snapshot date, the correct `LOAD_DATE` from each satellite. Downstream mart models join to the PIT table and then look up the satellite row at exactly the stored load date — a simple equality join rather than a range scan with aggregation.

#### SQL Example — `models/business_vault/pit_customer.sql`

```sql
-- Point-in-Time table for the CUSTOMER hub.
-- For each date in the date spine and each customer, stores the LOAD_DATE of the
-- correct row in each satellite as of that date.
-- Refresh this table on the same schedule as the mart refresh.

{{
    config(
        materialized='table',
        tags=['business_vault', 'pit']
    )
}}

{%- set source_model = 'hub_customer' -%}
{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_ldts = 'LOAD_DATE' -%}

{%- set satellites = {
    'sat_customer_details': {
        'pk': 'CUSTOMER_HK',
        'ldts': 'LOAD_DATE'
    },
    'sat_customer_preferences': {
        'pk': 'CUSTOMER_HK',
        'ldts': 'LOAD_DATE'
    },
    'sat_customer_marketing': {
        'pk': 'CUSTOMER_HK',
        'ldts': 'LOAD_DATE'
    }
} -%}

{%- set date_range = {
    'start_date': '2020-01-01',
    'end_date': dbt_utils.pretty_time(format="%Y-%m-%d")
} -%}

{{ automate_dv.pit(source_model=source_model,
                   src_pk=src_pk,
                   src_ldts=src_ldts,
                   satellites=satellites,
                   date_range=date_range) }}
```

#### Python Note

There is no direct Python dbt model equivalent for the AutomateDV PIT macro. The macro generates complex window-function SQL that is most efficiently expressed in Spark SQL via the macro. Implement PIT exclusively through dbt SQL models using AutomateDV.

#### Validation — SQL

```sql
-- Verify PIT table has one row per (CUSTOMER_HK, snapshot date)
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
WHERE p.AS_OF_DATE >= h.LOAD_DATE
  AND p.SAT_CUSTOMER_DETAILS_LDTS IS NULL;
-- Expected: 0 rows (customers should have a satellite record from their first load date)
```

### Discussion and Concerns

- **PIT tables do not store payload:** The PIT table stores pointers (load dates) to the correct satellite version for each snapshot date. The actual attribute values are retrieved by joining the PIT load date to the satellite in mart models. This design keeps PIT tables compact.
- **Refresh schedule must align with mart refresh:** A stale PIT table causes the mart to serve outdated data. Schedule the PIT refresh as an upstream step in the mart pipeline job.
- **Materialise as a table:** A PIT view that executes window functions on-the-fly across large satellite histories is prohibitively slow. Always materialise as a Delta table.
- **Include only required satellites:** Every satellite added to a PIT table increases the table width and the refresh cost. Include only the satellites that are actually consumed by downstream marts.
- **Date spine granularity:** The `date_range` generates one PIT row per date per customer. For hourly reporting, use an hourly spine — but be aware this multiplies the PIT table size by 24.

### See Also

- [AutomateDV PIT Table Documentation](https://automate-dv.readthedocs.io/en/latest/macros/pit/)
- [dbt_utils date_spine](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#date_spine-source)

---

## Bridge Tables

Bridge tables pre-compute the multi-hop join path across the vault graph, spanning one or more Links to enable efficient mart queries that navigate relationships between multiple Hub entities.

### Problem

Building a fact table that reports on customer orders requires joining `hub_customer` → `link_customer_order` → `hub_order`. Adding line item detail requires extending the path to `link_order_line_item` → `hub_product`. Every mart model that needs customer-order-product grain must reproduce this four-way join. Changes to the vault structure (a new intermediate link) break every mart query simultaneously.

### Solution

The `automate_dv.bridge` macro generates a Bridge table that pre-computes the join path. The mart model joins to the bridge table as a single step, retrieving all the relevant hash keys without executing multi-hop vault joins at query time.

#### SQL Example — `models/business_vault/bridge_customer_orders.sql`

```sql
-- Bridge table spanning CUSTOMER → CUSTOMER_ORDER link → ORDER.
-- Pre-computes the join path so mart models can retrieve all relevant hash keys
-- from a single bridge join rather than a multi-hop vault traversal.

{{
    config(
        materialized='table',
        tags=['business_vault', 'bridge']
    )
}}

{%- set source_model = 'hub_customer' -%}
{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_ldts = 'LOAD_DATE' -%}

{%- set bridge_walk = {
    'link_customer_order': {
        'bridge_link_pk': 'CUSTOMER_ORDER_HK',
        'bridge_start_pk': 'CUSTOMER_HK',
        'bridge_end_pk': 'ORDER_HK',
        'bridge_join_pk': 'CUSTOMER_HK',
        'bridge_load_date': 'LOAD_DATE'
    }
} -%}

{%- set date_range = {
    'start_date': '2020-01-01',
    'end_date': dbt_utils.pretty_time(format="%Y-%m-%d")
} -%}

{{ automate_dv.bridge(source_model=source_model,
                      src_pk=src_pk,
                      src_ldts=src_ldts,
                      bridge_walk=bridge_walk,
                      date_range=date_range) }}
```

#### Python Note

No direct Python equivalent for the AutomateDV Bridge macro. Implement bridge tables exclusively through dbt SQL models.

#### Validation — SQL

```sql
-- Verify bridge row count is >= link row count
-- (bridge may have more rows if the date spine adds historical snapshots)
SELECT COUNT(*) AS bridge_rows FROM main.business_vault.bridge_customer_orders;
SELECT COUNT(*) AS link_rows   FROM main.raw_vault.link_customer_order;

-- Verify all ORDER_HK values in the bridge resolve to hub_order
SELECT b.CUSTOMER_ORDER_HK
FROM main.business_vault.bridge_customer_orders b
LEFT JOIN main.raw_vault.hub_order o ON b.ORDER_HK = o.ORDER_HK
WHERE o.ORDER_HK IS NULL;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Refresh on the same schedule as PIT:** Bridge and PIT tables are consumed together by mart models. If bridge is stale relative to PIT, mart joins will miss rows. Schedule them as sequential steps in the same pipeline job.
- **Bridge tables can span multiple links:** Add additional entries to `bridge_walk` to extend the join path (e.g., customer → order → line item → product). Each additional hop increases bridge width and refresh cost.
- **Bridge does not filter by effectivity:** A standard bridge table includes all link relationships, regardless of whether the effectivity satellite shows them as currently active. For mart models that need only currently active relationships, filter the bridge join on the effectivity satellite's current open/close dates.

### See Also

- [AutomateDV Bridge Table Documentation](https://automate-dv.readthedocs.io/en/latest/macros/bridge/)
- [dv2_information_mart_cookbook.md](./dv2_information_mart_cookbook.md)

---

## dbt-utils for Business Vault Validation

dbt-utils provides a suite of generic tests that can be applied to Business Vault models in `schema.yml` to enforce grain, business rules, and data quality constraints before data reaches the Information Mart.

### Problem

The business vault applies derived business rules and pre-computed join paths. If a business rule is violated (e.g., a customer tier is assigned an invalid value) or a grain constraint is broken (e.g., the PIT table has duplicate rows per customer per date), the mart will silently serve incorrect data to consumers. These failures must be detected and surfaced before the mart refresh runs.

### Solution

Define dbt-utils tests in `schema.yml` for each business vault model. Run `dbt test --select business_vault` as a pipeline step that must pass before downstream mart models are built.

#### SQL Example — `models/business_vault/schema.yml`

```yaml
version: 2

models:
  - name: bv_customer_derived
    description: "Current-state derived customer attributes from the raw vault satellite"
    columns:
      - name: CUSTOMER_HK
        description: "Primary key — hash of CUSTOMER_ID"
        tests:
          - not_null
          - unique
      - name: FULL_NAME
        description: "Derived full name — concatenation of FIRST_NAME and LAST_NAME"
        tests:
          - not_null
      - name: EMAIL_ADDRESS
        tests:
          - not_null
          # Business rule: email must contain an @ symbol
          - dbt_utils.expression_is_true:
              expression: "EMAIL_ADDRESS LIKE '%@%'"
              severity: error

  - name: pit_customer
    description: "Point-in-Time table for the CUSTOMER hub"
    tests:
      # Grain constraint: one row per customer per snapshot date
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - CUSTOMER_HK
            - AS_OF_DATE
          severity: error
    columns:
      - name: CUSTOMER_HK
        tests:
          - not_null
      - name: AS_OF_DATE
        tests:
          - not_null

  - name: bridge_customer_orders
    description: "Bridge table spanning customer to order via link_customer_order"
    columns:
      - name: CUSTOMER_HK
        tests:
          - not_null
          - relationships:
              to: ref('hub_customer')
              field: CUSTOMER_HK
              severity: error
      - name: ORDER_HK
        tests:
          - not_null
          - relationships:
              to: ref('hub_order')
              field: ORDER_HK
              severity: error
```

For a transactional link, add business rule tests on payload columns:

```yaml
  - name: t_link_payment
    columns:
      - name: PAYMENT_AMOUNT
        tests:
          # Critical business rule: payment amounts must be positive
          - dbt_utils.expression_is_true:
              expression: "PAYMENT_AMOUNT > 0"
              severity: error
          - not_null
      - name: CURRENCY_CODE
        tests:
          - not_null
          - accepted_values:
              values: ['GBP', 'USD', 'EUR', 'CAD', 'AUD']
              severity: warn
```

#### Python Example — running dbt tests from the pipeline

```python
# Running dbt tests as part of a Databricks Job or notebook
# This is run as a shell command from a Databricks Job task of type "dbt"
# or from a Python notebook using subprocess

import subprocess
import sys

result = subprocess.run(
    ["dbt", "test", "--select", "business_vault", "--profiles-dir", "/root/.dbt"],
    capture_output=True,
    text=True
)

print(result.stdout)
print(result.stderr)

if result.returncode != 0:
    # Fail the Databricks Job task — prevents downstream mart models from running
    raise RuntimeError(
        f"dbt test failed for business_vault layer. "
        f"Review test output above before proceeding to mart refresh.\n"
        f"Exit code: {result.returncode}"
    )
```

**Functional difference between SQL and Python approaches:** The `schema.yml` tests are defined once and executed as SQL on the SQL Warehouse by dbt. The Python snippet wraps the dbt CLI invocation to enforce that a test failure blocks downstream pipeline steps. Both are needed: the YAML defines what to test; the Python wrapper enforces the pipeline gate.

#### Validation — SQL

```sql
-- Query dbt test results from the dbt artifacts (after a dbt test run)
-- This requires the dbt Elementary package or a custom metadata table
-- populated from dbt's run_results.json artifact

-- Manual equivalent: check the assertion directly
SELECT COUNT(*) AS email_without_at
FROM main.business_vault.bv_customer_derived
WHERE EMAIL_ADDRESS NOT LIKE '%@%';
-- Expected: 0 (mirrors the expression_is_true test)

SELECT COUNT(*) AS negative_payments
FROM main.raw_vault.t_link_payment
WHERE PAYMENT_AMOUNT <= 0;
-- Expected: 0 (mirrors the expression_is_true test)
```

### Discussion and Concerns

- **`error` vs. `warn` severity:** Use `severity: error` for tests that represent data quality failures that would produce incorrect BI output (null keys, negative amounts, broken referential integrity). Use `severity: warn` for advisory checks (accepted values for reference codes, unusual ranges) that should be investigated but should not block pipeline execution.
- **Run tests before mart refresh:** The pipeline job sequence should be: (1) raw vault models, (2) business vault models, (3) dbt test on business vault, (4) mart models. If step 3 fails on any error-severity test, step 4 must not run.
- **`unique_combination_of_columns` is the grain test:** For PIT tables and Bridge tables where the grain is a combination of keys and dates, `dbt_utils.unique_combination_of_columns` is the correct test. The built-in `unique` test applies to a single column only.
- **`relationships` tests verify referential integrity:** The `relationships` test in dbt checks that every value in a foreign key column exists in the referenced table. Use this on bridge table hash keys to catch vault loading failures before they propagate to marts.

### See Also

- [dbt-utils Test Documentation](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#tests)
- [dbt Generic Tests](https://docs.getdbt.com/docs/build/tests)
- [dbt Test Severity](https://docs.getdbt.com/reference/resource-configs/severity)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| PIT table row count | `SELECT COUNT(*) FROM pit_customer` in scheduled query | Row count should grow by (new customers × days in schedule window) per run |
| Bridge table staleness | `SELECT MAX(AS_OF_DATE) FROM bridge_customer_orders` | Should match current date minus one day after nightly refresh |
| dbt test pass/fail rate | Databricks Jobs UI — dbt test task exit code | Any `error`-severity test failure must trigger an alert and block mart refresh |
| Business rule violation count | dbt test results / custom monitoring table | Target: 0 `error`-severity violations; `warn` violations reviewed weekly |
| Business vault model run duration | Databricks Jobs UI / query history | PIT and bridge refreshes grow over time — monitor for unexpected slowdowns |

### Metrics for Success

- [ ] PIT table is refreshed within the scheduled SLA (e.g., within 30 minutes of raw vault refresh completing)
- [ ] All dbt tests at `error` severity pass on every pipeline run — zero failures
- [ ] Business rule violations reported at `warn` severity are reviewed in the weekly data quality review
- [ ] Bridge tables are refreshed on the same schedule as PIT tables — never stale relative to PIT
- [ ] No business vault model directly reads from `staging` schema — all reads are from `raw_vault` only
