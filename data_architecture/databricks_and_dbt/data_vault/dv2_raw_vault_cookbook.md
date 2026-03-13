# Data Vault 2.0 Raw Vault Cookbook

## Introduction

This cookbook provides practical, step-by-step guidance for **loading Raw Vault structures** on Databricks using dbt and AutomateDV. It covers Hub, Link, Non-Historized Link, Satellite, Multi-Active Satellite, Effectivity Satellite, Transactional Link, and Reference structures — the complete set of building blocks required for a production Raw Vault.

All models in this cookbook are append-only and idempotent. They read from staging models built using the patterns in [dv2_staging_cookbook.md](./dv2_staging_cookbook.md).

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.9+ | Required for dbt-databricks |
| [dbt-databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup) | 1.7.x | Core transformation framework |
| [automate_dv](https://automate-dv.readthedocs.io/en/latest/) | 0.10.2 | Generates all vault-pattern SQL |
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
      schema: raw_vault
      catalog: main
```

### Getting a New Starter Project

```bash
dbt init data_vault_project
cd data_vault_project
dbt deps    # Install automate_dv after adding to packages.yml
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
| Unity Catalog — `raw_vault` schema | Target for vault tables | dbt service principal needs `CREATE TABLE` and `INSERT` |
| Unity Catalog — `staging` schema | Source for staging models | `SELECT` privilege required |

### Raw Vault Schema Setup

```sql
-- Create the raw_vault schema
CREATE SCHEMA IF NOT EXISTS main.raw_vault
  COMMENT 'Data Vault 2.0 raw vault — structural, append-only, no business rules';

-- Grant privileges to the dbt service principal
GRANT CREATE TABLE, INSERT, SELECT ON SCHEMA main.raw_vault
  TO `dbt-service-principal@your-org.com`;

-- Data engineers get SELECT only — no modification of raw vault
GRANT SELECT ON SCHEMA main.raw_vault
  TO `data-engineers@your-org.com`;
```

---

## Hub Loading

A Hub stores the first appearance of each unique business key across all contributing source systems. It is the identity anchor for a business entity in the vault.

### Problem

Multiple source systems — a CRM, an e-commerce platform, and a mobile application — each send customer records independently. Some customers exist in all three systems. Loading each source independently creates duplicate natural keys; joining them in an ad-hoc view is fragile and not auditable. The vault needs a single, deduplicated record for each unique customer identity, regardless of which source delivered it first.

### Solution

The `automate_dv.hub` macro generates an `INSERT INTO ... SELECT` pattern that loads only new natural keys (those not already present in the hub), from one or more staging sources in a single model.

#### SQL Example — `models/raw_vault/hub_customer.sql`

```sql
-- Hub for the CUSTOMER business entity.
-- Loads from three staging sources: CRM, e-commerce, and mobile app.
-- The hub stores the first appearance of each CUSTOMER_ID across all sources.
-- Re-running this model is safe — it is idempotent by design.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['raw_vault', 'hub']
    )
}}

{%- set source_models = ['stg_crm_customer',
                         'stg_ecommerce_customer',
                         'stg_mobile_customer'] -%}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_nk = 'CUSTOMER_ID' -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.hub(src_pk=src_pk,
                   src_nk=src_nk,
                   src_ldts=src_ldts,
                   src_source=src_source,
                   source_model=source_models) }}
```

#### Python Note

AutomateDV hub loading is a dbt SQL macro. There is no direct PySpark equivalent. In a non-dbt context, the equivalent pattern would be a `MERGE INTO` statement using `WHEN NOT MATCHED THEN INSERT`. However, because vault hubs must be append-only (never updated), a `MERGE` with an update clause is incorrect. Use `INSERT INTO ... SELECT ... WHERE NOT EXISTS` instead.

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

- **Hub is idempotent:** AutomateDV generates a `WHERE NOT EXISTS` guard that prevents duplicate natural keys from being inserted. Re-running the hub load on the same data produces no new rows.
- **Multi-source loading:** Passing a list to `source_model` causes AutomateDV to UNION the staging sources before deduplication. The hub will record whichever source system's record arrived first as the `LOAD_DATE` and `RECORD_SOURCE` for that natural key.
- **No descriptive data in the hub:** The hub contains only the hash key, natural key, load date, and record source. Customer name, email, and all other descriptive attributes belong in a satellite. Never add payload columns to a hub.
- **Composite natural keys:** If the business key is composite (e.g., `SYSTEM_ID + CUSTOMER_ID`), pass both columns as a list to `src_nk`. AutomateDV concatenates and hashes them consistently with the staging model.

### See Also

- [AutomateDV Hub Macro Documentation](https://automate-dv.readthedocs.io/en/latest/macros/hub/)
- [dv2_staging_cookbook.md](./dv2_staging_cookbook.md)
- [dv2_architecture.md — Hubs](./dv2_architecture.md#hubs)

---

## Link Loading

A Link records the structural relationship between two or more business entities. It captures when that relationship first appeared in the source data.

### Problem

An order management system records that customer C-001 placed order O-5042. An e-commerce platform records that the same customer placed order O-7891. Both relationships need to be recorded in the vault — not overwriting each other, and not losing the history of either. A simple foreign key on an order table cannot represent this without duplication across source systems.

### Solution

The `automate_dv.link` macro creates a Link table that stores one row per unique combination of foreign hash keys. Each row records the relationship, the load date, and the record source.

#### SQL Example — `models/raw_vault/link_customer_order.sql`

```sql
-- Link between the CUSTOMER and ORDER business entities.
-- One row per unique (CUSTOMER_HK, ORDER_HK) combination.
-- The link records when this customer-order relationship first appeared.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['raw_vault', 'link']
    )
}}

{%- set source_model = 'stg_order' -%}
{%- set src_pk = 'CUSTOMER_ORDER_HK' -%}
{%- set src_fk = ['CUSTOMER_HK', 'ORDER_HK'] -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.link(src_pk=src_pk,
                    src_fk=src_fk,
                    src_ldts=src_ldts,
                    src_source=src_source,
                    source_model=source_model) }}
```

#### Python Note

No direct PySpark equivalent. In a non-dbt context, an `INSERT INTO ... SELECT DISTINCT ... WHERE NOT EXISTS` pattern replicates the append-only deduplication behaviour.

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

- **Link grain:** The link stores one row per unique relationship instance, not per source system occurrence. If three source systems report the same (CUSTOMER_HK, ORDER_HK) pair, only one row is inserted.
- **Link does not track relationship end:** A link row records when a relationship first appeared. It has no close date. To track when a relationship was dissolved (e.g., an order was cancelled, a customer was removed from a segment), use an Effectivity Satellite alongside the link.
- **Three-way and higher-arity links:** A link can connect three or more hubs by adding more foreign keys to `src_fk`. The link hash key is then computed from all foreign keys combined.
- **Referential integrity:** Ideally all foreign keys in a link resolve to rows in their respective hubs. Ghost record injection (enabled in staging) provides a synthetic hub row for cases where a relationship arrives before the referenced entity.

### See Also

- [AutomateDV Link Macro Documentation](https://automate-dv.readthedocs.io/en/latest/macros/link/)
- [dv2_architecture.md — Links](./dv2_architecture.md#links)

---

## Non-Historized Link

A Non-Historized Link (NH Link) represents an immutable, fact-like relationship — one that by definition never changes or ends once it has been recorded.

### Problem

A payment processing system records each payment transaction as an event: payment P-9901 was made by customer C-001 against invoice I-4421. This event is immutable — once recorded, it never changes and it never ends. A standard link with an optional effectivity satellite is overkill and misleading for this case; there is no lifecycle to track.

### Solution

The `automate_dv.nh_link` macro generates an append-only link with no effectivity tracking. It is semantically identical to a standard link but signals to the vault design that this relationship is immutable by nature.

#### SQL Example — `models/raw_vault/nh_link_payment.sql`

```sql
-- Non-historized link for PAYMENT events.
-- A payment event is immutable: once recorded, it never changes.
-- One row per unique (CUSTOMER_HK, INVOICE_HK, PAYMENT_HK) combination.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['raw_vault', 'nh_link']
    )
}}

{%- set source_model = 'stg_payment' -%}
{%- set src_pk = 'CUSTOMER_INVOICE_PAYMENT_HK' -%}
{%- set src_fk = ['CUSTOMER_HK', 'INVOICE_HK', 'PAYMENT_HK'] -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.nh_link(src_pk=src_pk,
                       src_fk=src_fk,
                       src_ldts=src_ldts,
                       src_source=src_source,
                       source_model=source_model) }}
```

#### Python Note

No direct PySpark equivalent. The physical structure is identical to a standard link; the distinction is architectural intent.

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

- **Use nh_link only for truly immutable relationships:** If there is any possibility that the relationship could be corrected, reversed, or dissolved, use a standard link with an effectivity satellite instead.
- **No payload in nh_link:** A non-historized link stores only structural columns (hash keys, load date, record source). Monetary values, quantities, or event attributes belong in a Transactional Link (see below).

### See Also

- [AutomateDV Non-Historized Link Documentation](https://automate-dv.readthedocs.io/en/latest/macros/nh_link/)

---

## Satellite Loading

A Satellite tracks the full change history of descriptive attributes for a Hub or Link. It is the only vault structure that accumulates over time in response to changing source data.

### Problem

A customer updates their email address. The CRM system sends the new record. The vault must retain the previous email address (for audit purposes) and record the new one as the current version. No existing rows should be updated or deleted; the previous state must remain permanently accessible.

### Solution

The `automate_dv.sat` macro generates an `INSERT INTO ... SELECT` that inserts new rows only when the incoming hashdiff differs from the most recently loaded hashdiff for the same parent hash key.

#### SQL Example — `models/raw_vault/sat_customer_details.sql`

```sql
-- Satellite for descriptive attributes of the CUSTOMER hub.
-- Append-only: new rows are inserted when CUSTOMER_HASHDIFF changes.
-- Partitioned by LOAD_DATE for query performance on Databricks.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        partition_by={
            'field': 'LOAD_DATE',
            'data_type': 'date',
            'granularity': 'day'
        },
        tags=['raw_vault', 'satellite']
    )
}}

{%- set source_model = 'stg_crm_customer' -%}
{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_hashdiff = 'CUSTOMER_HASHDIFF' -%}
{%- set src_payload = ['CUSTOMER_NAME',
                       'EMAIL_ADDRESS',
                       'PHONE_NUMBER',
                       'BILLING_ADDRESS',
                       'CITY',
                       'POSTCODE',
                       'COUNTRY_CODE'] -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.sat(src_pk=src_pk,
                   src_hashdiff=src_hashdiff,
                   src_payload=src_payload,
                   src_ldts=src_ldts,
                   src_source=src_source,
                   source_model=source_model) }}
```

#### Python Note

No direct PySpark equivalent for the AutomateDV satellite macro. In a non-dbt context, the logic is:

```python
# Conceptual equivalent — not recommended for vault pipelines managed by dbt
# This is for understanding the satellite insert logic only

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Get the most recent hashdiff per customer hash key already in the satellite
latest_in_sat = (
    spark.table("main.raw_vault.sat_customer_details")
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("CUSTOMER_HK").orderBy(F.col("LOAD_DATE").desc())
    ))
    .filter(F.col("rn") == 1)
    .select("CUSTOMER_HK", "CUSTOMER_HASHDIFF")
)

# Anti-join staging against latest satellite state on (CUSTOMER_HK, CUSTOMER_HASHDIFF)
new_rows = (
    spark.table("main.staging.stg_crm_customer")
    .join(latest_in_sat, on=["CUSTOMER_HK", "CUSTOMER_HASHDIFF"], how="left_anti")
)

new_rows.write.mode("append").saveAsTable("main.raw_vault.sat_customer_details")
```

#### Validation — SQL

```sql
-- Verify no consecutive rows with identical hashdiff for the same customer
-- (would indicate satellite load is inserting duplicates)
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

- **Never update or delete satellite rows:** Satellites are the permanent historical record. Any change to existing rows violates the audit trail. Use append-only incremental materialisation (`incremental_strategy: 'append'`).
- **Partition by LOAD_DATE on Databricks:** Satellites grow indefinitely. Partitioning by load date ensures that queries filtering to recent data do not scan the entire table history. Use `OPTIMIZE` and `ZORDER BY CUSTOMER_HK` to improve point-lookup performance.
- **One satellite per source system per entity:** If customer attributes come from both a CRM and an MDM system, create two satellites (`sat_customer_crm_details` and `sat_customer_mdm_details`) rather than combining them. This isolates source system changes and preserves source fidelity.

### See Also

- [AutomateDV Satellite Macro Documentation](https://automate-dv.readthedocs.io/en/latest/macros/sat/)
- [Delta Lake Incremental Strategies — dbt-databricks](https://docs.getdbt.com/reference/resource-configs/databricks-configs#incremental-models)

---

## Multi-Active Satellite

A Multi-Active Satellite (MA Satellite) is used when a single hub record has multiple simultaneously active records of the same attribute type — for example, a customer with multiple phone numbers, all currently in use.

### Problem

A customer has a mobile phone, a work phone, and a home phone. All three are currently active. A standard satellite stores one row per customer per change event, which cannot represent multiple concurrent active values without losing history or combining values into a denormalised string.

### Solution

The `automate_dv.ma_sat` macro supports a Child Dependent Key (CDK) — a column that distinguishes between the multiple active records for the same parent. The CDK + parent hash key together form the effective primary key of the satellite.

#### SQL Example — `models/raw_vault/ma_sat_customer_phone.sql`

```sql
-- Multi-Active Satellite for customer phone numbers.
-- A single customer may have multiple phone numbers active simultaneously.
-- PHONE_TYPE (MOBILE, WORK, HOME) is the Child Dependent Key.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['raw_vault', 'ma_satellite']
    )
}}

{%- set source_model = 'stg_crm_customer_phones' -%}
{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_cdk = ['PHONE_TYPE'] -%}
{%- set src_hashdiff = 'CUSTOMER_PHONE_HASHDIFF' -%}
{%- set src_payload = ['PHONE_NUMBER', 'IS_PRIMARY'] -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.ma_sat(src_pk=src_pk,
                      src_cdk=src_cdk,
                      src_hashdiff=src_hashdiff,
                      src_payload=src_payload,
                      src_ldts=src_ldts,
                      src_source=src_source,
                      source_model=source_model) }}
```

#### Python Note

No PySpark equivalent for the AutomateDV MA satellite macro.

#### Validation — SQL

```sql
-- Verify each (CUSTOMER_HK, PHONE_TYPE) combination has no consecutive duplicate hashdiffs
WITH ranked AS (
    SELECT
        CUSTOMER_HK,
        PHONE_TYPE,
        CUSTOMER_PHONE_HASHDIFF,
        LOAD_DATE,
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
- **Prefer standard satellite if the use case is simple:** A standard satellite with a `PHONE_TYPE` column in the payload, and a separate row per phone type per change event, is often sufficient and simpler to query. Use MA satellite only when multiple records must be independently tracked and versioned.
- **Querying MA satellites is more complex:** Retrieving the current active set requires filtering to the latest load date per (CUSTOMER_HK, PHONE_TYPE) combination rather than just the latest per CUSTOMER_HK.

### See Also

- [AutomateDV Multi-Active Satellite Documentation](https://automate-dv.readthedocs.io/en/latest/macros/ma_sat/)

---

## Effectivity Satellite

An Effectivity Satellite tracks when a Link relationship became active and when it was dissolved. It is the vault mechanism for recording relationship lifecycle events.

### Problem

A customer is assigned to a sales region. Six months later, the customer moves and is reassigned to a different region. The Link records the structural relationship, but it does not record that the first assignment ended or that the second one began. Without effectivity tracking, it is impossible to determine which region a customer was assigned to on a given date.

### Solution

The `automate_dv.eff_sat` macro creates an effectivity satellite that stores open and close dates for link relationships.

#### SQL Example — `models/raw_vault/eff_sat_customer_order.sql`

```sql
-- Effectivity Satellite for the CUSTOMER_ORDER link.
-- Records when the customer-order relationship was opened and, if applicable, closed.
-- Used to determine whether a relationship was active at any given point in time.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['raw_vault', 'eff_satellite']
    )
}}

{%- set source_model = 'stg_order' -%}
{%- set src_pk = 'CUSTOMER_ORDER_HK' -%}
{%- set src_dfk = 'CUSTOMER_HK' -%}
{%- set src_sfk = 'ORDER_HK' -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.eff_sat(src_pk=src_pk,
                       src_dfk=src_dfk,
                       src_sfk=src_sfk,
                       src_ldts=src_ldts,
                       src_source=src_source,
                       source_model=source_model) }}
```

#### Python Note

No PySpark equivalent. The effectivity satellite logic (detecting open and close events from source data) is complex and must be implemented in the staging layer — the staging model for an effectivity satellite must identify which records represent a relationship opening vs. a relationship close.

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

- **Effectivity satellite works alongside the link:** The link records that a relationship exists. The effectivity satellite records when it was active. Both are required to answer time-bound relationship queries.
- **Relationship close events require a dedicated staging model:** The source system must provide a signal that a relationship ended (e.g., an order cancellation event, a customer unsubscription). This end event is staged separately and fed to the effectivity satellite to close the open record.
- **`src_dfk` vs `src_sfk`:** AutomateDV distinguishes the Driving Foreign Key (the entity whose activity drives the relationship lifecycle, e.g., the customer) from the Secondary Foreign Key (the counterpart entity, e.g., the order). The driving FK determines which side of the relationship controls the open/close logic.

### See Also

- [AutomateDV Effectivity Satellite Documentation](https://automate-dv.readthedocs.io/en/latest/macros/eff_sat/)

---

## Transactional Link

A Transactional Link models an immutable business event that has both a relationship (multiple entities involved) and a payload (attributes of the event itself).

### Problem

A payment of £149.99 was made by customer C-001 against invoice I-4421 using payment method PM-VISA on 2025-03-10. This is an immutable fact: the payment happened, the amount is fixed, and the event will never change. A standard link cannot hold payload columns (amount, currency, payment method). A satellite attached to the link would model the payment attributes as potentially changing, which is architecturally incorrect for an immutable transaction.

### Solution

The `automate_dv.t_link` macro creates a transactional link that stores payload columns alongside foreign keys. It is append-only and the payload is never updated.

#### SQL Example — `models/raw_vault/t_link_payment.sql`

```sql
-- Transactional Link for PAYMENT events.
-- An immutable record of each payment: who paid, against which invoice,
-- using which payment method, for how much.
-- Payload columns (AMOUNT, CURRENCY, PAYMENT_METHOD) are stored directly on the t_link.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['raw_vault', 't_link']
    )
}}

{%- set source_model = 'stg_payment' -%}
{%- set src_pk = 'PAYMENT_HK' -%}
{%- set src_fk = ['CUSTOMER_HK', 'INVOICE_HK', 'PAYMENT_METHOD_HK'] -%}
{%- set src_payload = ['PAYMENT_AMOUNT', 'CURRENCY_CODE', 'PAYMENT_DATE'] -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.t_link(src_pk=src_pk,
                      src_fk=src_fk,
                      src_payload=src_payload,
                      src_ldts=src_ldts,
                      src_source=src_source,
                      source_model=source_model) }}
```

#### Python Note

No PySpark equivalent for the AutomateDV t_link macro.

#### Validation — SQL

```sql
-- Verify no negative payment amounts (business rule violation)
SELECT COUNT(*) AS negative_amount_count
FROM main.raw_vault.t_link_payment
WHERE PAYMENT_AMOUNT < 0;
-- Expected: 0 in most payment systems (returns may be modelled separately)

-- Verify all foreign keys resolve
SELECT t.PAYMENT_HK
FROM main.raw_vault.t_link_payment t
LEFT JOIN main.raw_vault.hub_customer c ON t.CUSTOMER_HK = c.CUSTOMER_HK
WHERE c.CUSTOMER_HK IS NULL;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Use t_link for events with monetary value or event payload:** If a link relationship has attributes that are facts of the event (not descriptions of the relationship that could change), use a t_link.
- **t_link vs. satellite on a link:** A satellite on a link models attributes that could change (a relationship's status, terms, priority). A t_link models immutable event attributes. Choose based on whether the payload is expected to ever change.
- **t_link payload is never updated:** Like all vault structures, t_links are append-only. If a payment record is corrected, model the correction as a new event (e.g., a reversal + re-entry), not an update to the original row.

### See Also

- [AutomateDV Transactional Link Documentation](https://automate-dv.readthedocs.io/en/latest/macros/t_link/)

---

## Reference Structures

Reference structures apply vault-pattern governance to lookup and reference data — static or slowly-changing data such as country codes, currency codes, product categories, and status values.

### Problem

Country codes are used across multiple vault structures (customer billing address, shipping address, payment currency). Without vault-style hash keys and load dates on the reference data, it cannot be joined to the vault using the same hash key conventions and cannot participate in PIT tables.

### Solution

The `automate_dv.ref_hub` and `automate_dv.ref_sat` macros create reference structures that follow the same hash key conventions as regular vault Hubs and Satellites.

#### SQL Example — `models/raw_vault/ref_hub_country.sql`

```sql
-- Reference Hub for COUNTRY codes.
-- Loaded from a dbt seed file (data/ref_country_codes.csv) or a dedicated reference staging model.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['raw_vault', 'ref_hub']
    )
}}

{%- set source_model = 'stg_ref_country' -%}
{%- set src_pk = 'COUNTRY_HK' -%}
{%- set src_nk = 'COUNTRY_CODE' -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.ref_hub(src_pk=src_pk,
                       src_nk=src_nk,
                       src_ldts=src_ldts,
                       src_source=src_source,
                       source_model=source_model) }}
```

#### SQL Example — `models/raw_vault/ref_sat_country_details.sql`

```sql
-- Reference Satellite for COUNTRY descriptive attributes.
-- Tracks changes to country names, regions, and currency codes over time.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['raw_vault', 'ref_satellite']
    )
}}

{%- set source_model = 'stg_ref_country' -%}
{%- set src_pk = 'COUNTRY_HK' -%}
{%- set src_hashdiff = 'COUNTRY_HASHDIFF' -%}
{%- set src_payload = ['COUNTRY_NAME', 'REGION', 'CURRENCY_CODE', 'DIALLING_CODE'] -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.ref_sat(src_pk=src_pk,
                       src_hashdiff=src_hashdiff,
                       src_payload=src_payload,
                       src_ldts=src_ldts,
                       src_source=src_source,
                       source_model=source_model) }}
```

The staging model for reference structures typically reads from a dbt seed:

```sql
-- models/staging/stg_ref_country.sql
{{
    config(materialized='view', tags=['staging', 'reference'])
}}

{%- set yaml_metadata -%}
source_model: 'ref_country_codes'
derived_columns:
  RECORD_SOURCE: "!REFERENCE_DATA"
  LOAD_DATE: "CAST(CURRENT_TIMESTAMP() AS TIMESTAMP)"
hashed_columns:
  COUNTRY_HK:
    - 'COUNTRY_CODE'
  COUNTRY_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'COUNTRY_NAME'
      - 'CURRENCY_CODE'
      - 'DIALLING_CODE'
      - 'REGION'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     null_columns=[]) }}
```

#### Python Note

No PySpark equivalent. Reference structures use the same AutomateDV macros as standard vault structures.

#### Validation — SQL

```sql
-- Verify all expected country codes are present
SELECT COUNT(*) AS country_count
FROM main.raw_vault.ref_hub_country;
-- Compare against the known number of rows in the dbt seed

-- Verify no duplicate country codes
SELECT COUNTRY_CODE, COUNT(*) AS cnt
FROM main.raw_vault.ref_hub_country
GROUP BY COUNTRY_CODE
HAVING cnt > 1;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Feed from dbt seeds or dedicated reference staging:** Reference data that rarely changes should be seeded from CSV files managed in version control. Reference data that arrives from an operational system (e.g., a product category API) should use a dedicated staging model.
- **Hash key conventions apply to reference structures:** The same hashing algorithm, null substitution, and uppercasing conventions that apply to regular vault structures must be applied to reference staging models.

### See Also

- [AutomateDV Reference Table Documentation](https://automate-dv.readthedocs.io/en/latest/macros/ref_table/)
- [dbt Seeds Documentation](https://docs.getdbt.com/docs/build/seeds)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Hub row count growth | `SELECT COUNT(*) FROM hub_customer` in scheduled dbt test | Stagnant count may indicate staging failure |
| Satellite hashdiff uniqueness per entity | dbt test `dbt_utils.unique_combination_of_columns` on (CUSTOMER_HK, LOAD_DATE) | Duplicates indicate satellite re-insertion |
| Link referential integrity | dbt test `relationships` between link FKs and hub PKs | Missing hub rows indicate ghost record issue |
| Satellite partition count | Unity Catalog table properties / `DESCRIBE DETAIL` | Unexpected partition explosion indicates bad LOAD_DATE |
| dbt model run duration | Databricks Jobs UI / job run timeline | Hub/link runs should be seconds; satellite runs grow over time |

### Metrics for Success

- [ ] Hub tables have no duplicate natural keys (`CUSTOMER_ID` unique in `hub_customer`)
- [ ] Satellite tables have no consecutive rows with identical hashdiff for the same parent hash key
- [ ] All link foreign keys resolve to rows in their respective hub tables
- [ ] All AutomateDV incremental models use `incremental_strategy: 'append'` — never `merge` or `delete+insert`
- [ ] Satellite tables are partitioned by `LOAD_DATE` and optimised with `ZORDER BY` on the parent hash key
- [ ] dbt tests for referential integrity pass on every pipeline run
