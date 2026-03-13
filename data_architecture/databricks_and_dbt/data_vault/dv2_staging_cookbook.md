# Data Vault 2.0 Staging Cookbook

## Introduction

This cookbook provides practical, step-by-step guidance for **staging data in a Data Vault 2.0 pipeline** on Databricks. It covers how to derive hash keys, compute hashdiff columns, and prepare source data for vault loading using the AutomateDV `stage` macro and dbt-utils `generate_surrogate_key`. These patterns are prerequisites for all Raw Vault loading.

Each method section follows a consistent structure: the problem being solved, the recommended solution with code examples, any known concerns or trade-offs, and links to further reading.

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

> **dbt execution environments:**
> - **Local development:** `dbt-databricks` runs on your local machine, submitting SQL to a Databricks SQL Warehouse via `http_path` in `~/.dbt/profiles.yml`.
> - **Databricks Asset Bundle jobs (production):** When deployed via `databricks bundle deploy`, dbt runs on a Databricks single-node job cluster (`num_workers: 0`) or serverless environment. The bundle installs `dbt-databricks` as a pypi library; workspace credentials (`DBT_HOST`, `DBT_ACCESS_TOKEN`) are injected by Databricks automatically. SQL is submitted to a SQL Warehouse via the bundle's `dbt_profiles/profiles.yml`.
>
> In both cases, dbt submits SQL to a **SQL Warehouse** — it does not use Spark directly. PySpark is pre-installed on Databricks clusters but not needed for dbt workloads. For production Asset Bundle deployments, only the Databricks CLI is required locally.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.9+ | Local dev | Required for dbt-databricks |
| [dbt-databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup) | 1.7.x | Local dev | Core transformation framework — runs locally, connects to Databricks |
| [automate_dv](https://automate-dv.readthedocs.io/en/latest/) | 0.10.2 | Local dev | Data Vault macro library; installed via `dbt deps` |
| [dbt-utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) | 1.3.0 | Local dev | General-purpose dbt utility macros; installed via `dbt deps` |
| Databricks CLI | Latest | Local dev | Workspace and secrets interaction |

Configure your Databricks connection:

```bash
# Authenticate the Databricks CLI
databricks configure --token

# Verify connection
databricks clusters list
```

For dbt, configure `~/.dbt/profiles.yml`:

```yaml
data_vault_project:
  target: dev
  outputs:
    dev:
      type: databricks
      host: your-workspace.azuredatabricks.net
      http_path: /sql/1.0/warehouses/your_warehouse_id
      token: "{{ env_var('DBT_TOKEN') }}"
      schema: staging
      catalog: main
```

### Getting a New Starter Project

```bash
dbt init data_vault_project
cd data_vault_project
# Add packages.yml (see dv2_architecture.md for content)
dbt deps    # Installs automate_dv and dbt-utils
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
| Databricks Workspace | Execution environment | Unity Catalog must be enabled |
| SQL Warehouse (Serverless or Pro) | Compute for dbt runs | Photon enabled; size to source volume |
| Unity Catalog — `staging` schema | Target for staging models | dbt service principal needs `CREATE TABLE` |
| Unity Catalog — source schema or external table | Source data from ingestion layer | `SELECT` privilege required |

### Unity Catalog Staging Schema Setup

```sql
-- Create the staging schema (run as workspace admin or catalog owner)
CREATE SCHEMA IF NOT EXISTS main.staging
  COMMENT 'Data Vault 2.0 staging layer — hashed and prepped for vault loading';

-- Grant privileges to the dbt service principal
GRANT CREATE TABLE, SELECT ON SCHEMA main.staging
  TO `dbt-service-principal@your-org.com`;
```

---

## Staging — AutomateDV Stage Macro

The `automate_dv.stage` macro is the primary entry point for all Data Vault staging. It accepts a source model reference and a configuration of derived columns, hashed columns, and null columns, then generates the SQL to produce a staging table ready for Vault loading.

### Problem

Raw ingested data from source systems contains natural keys and payload columns, but no hash keys, hashdiff columns, or metadata columns. Creating these manually in SQL across many source models is error-prone: column ordering mistakes silently produce incorrect hashes, and NULL handling is often inconsistent. Any inconsistency in hash key computation breaks the integration promise of the vault.

### Solution

Use the `automate_dv.stage` macro in each staging model. The macro enforces consistent hashing, null substitution, and column ordering based on the global `vars` configuration in `dbt_project.yml`.

The following example is a complete staging model for a customer source table.

#### SQL Example — `models/staging/stg_customer.sql`

```sql
-- Staging model for the CRM customer source.
-- Produces CUSTOMER_HK (hub hash key) and CUSTOMER_HASHDIFF (satellite change detection).
-- Source: main.bronze.crm_customer (ingested by Auto Loader in the ingestion layer)

{{
    config(
        materialized='view',
        tags=['staging', 'crm']
    )
}}

{%- set yaml_metadata -%}
source_model: 'raw_crm_customer'
derived_columns:
  RECORD_SOURCE: "!CRM"
  LOAD_DATE: "CAST(CURRENT_TIMESTAMP() AS TIMESTAMP)"
hashed_columns:
  CUSTOMER_HK:
    - 'CUSTOMER_ID'
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'CUSTOMER_NAME'
      - 'EMAIL_ADDRESS'
      - 'PHONE_NUMBER'
      - 'BILLING_ADDRESS'
      - 'CITY'
      - 'POSTCODE'
      - 'COUNTRY_CODE'
null_columns:
  - 'PHONE_NUMBER'
  - 'BILLING_ADDRESS'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     null_columns=metadata_dict['null_columns']) }}
```

The `raw_crm_customer` reference resolves to a dbt source defined in `models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: raw_crm_customer
    database: main
    schema: bronze
    tables:
      - name: raw_crm_customer
        description: "Raw CRM customer records ingested by Auto Loader"
```

#### Python Note

The AutomateDV `stage` macro is a dbt SQL macro. There is no direct PySpark equivalent. When staging data outside of dbt (e.g., in a Databricks notebook), hash key derivation must be implemented manually using `md5(upper(trim(coalesce(column, '^^'))))` in PySpark or Spark SQL. This is strongly discouraged in a vault project because it bypasses the consistency controls that AutomateDV enforces centrally.

#### Validation — SQL

After running `dbt run --select stg_customer`, validate the output:

```sql
-- Verify no null hash keys
SELECT COUNT(*) AS null_hk_count
FROM main.staging.stg_customer
WHERE CUSTOMER_HK IS NULL;
-- Expected: 0

-- Verify no null hashdiff
SELECT COUNT(*) AS null_hashdiff_count
FROM main.staging.stg_customer
WHERE CUSTOMER_HASHDIFF IS NULL;
-- Expected: 0

-- Verify row count matches source
SELECT
    (SELECT COUNT(*) FROM main.bronze.raw_crm_customer) AS source_count,
    (SELECT COUNT(*) FROM main.staging.stg_customer)    AS staging_count;
-- Expected: source_count = staging_count
```

### Discussion and Concerns

- **Ghost record injection:** When `enable_ghost_records: true` is set in `dbt_project.yml`, AutomateDV injects a row with all-null business keys into every staging model. This ghost record propagates into Hubs and Links, ensuring that Satellite rows that arrive without a resolvable Hub record do not violate referential integrity. Enable this in production environments.
- **Column ordering is immutable after first load:** The columns listed in `hashed_columns` determine the concatenation order before hashing. Once data has been loaded to the vault using a given order, changing the order produces different hash values. Any existing vault rows with the old hash become orphaned. Document the ordering and enforce it via code review.
- **`LOAD_DATE` must not be in the hashdiff:** The `LOAD_DATE` column must never appear in the `CUSTOMER_HASHDIFF` column list. Including it would cause every row to appear as a change on every load.
- **`include_source_columns=true`:** This setting passes all source columns through to the staging model alongside the derived and hashed columns. Set to `false` if you want to explicitly control which columns are present in the staging output.

### See Also

- [AutomateDV Stage Macro Reference](https://automate-dv.readthedocs.io/en/latest/macros/stage/)
- [dv2_architecture.md — Hash Key Design](./dv2_architecture.md#hash-key-design)
- [dv2_raw_vault_cookbook.md](./dv2_raw_vault_cookbook.md)

---

## Staging — Hashdiff Column Design

The hashdiff column is the mechanism by which satellites detect whether a row's descriptive attributes have changed since the last load. Correct hashdiff design is critical: errors in column selection produce missed changes (silent data quality failures) or false positives (unnecessary duplicate rows).

### Problem

A satellite must insert a new row only when one or more of a customer's descriptive attributes has changed since the last time that customer's data was loaded. If the hashdiff is computed from too few columns, genuine changes are missed. If it is computed from columns that change on every load (like a load timestamp), every row appears as a change and the satellite bloats with duplicate rows.

### Solution

Include every payload column — and only payload columns — in the hashdiff. The hashdiff must reflect the full descriptive state of the entity at load time.

#### SQL Example — Hashdiff configuration in `stg_customer.sql`

```sql
-- The CUSTOMER_HASHDIFF covers all seven payload columns.
-- No load metadata (LOAD_DATE, RECORD_SOURCE) is included.
-- Column order is fixed alphabetically per project standard.

{%- set yaml_metadata -%}
source_model: 'raw_crm_customer'
derived_columns:
  RECORD_SOURCE: "!CRM"
  LOAD_DATE: "CAST(CURRENT_TIMESTAMP() AS TIMESTAMP)"
hashed_columns:
  CUSTOMER_HK:
    - 'CUSTOMER_ID'
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'BILLING_ADDRESS'
      - 'CITY'
      - 'COUNTRY_CODE'
      - 'CUSTOMER_NAME'
      - 'EMAIL_ADDRESS'
      - 'PHONE_NUMBER'
      - 'POSTCODE'
{%- endset -%}
```

#### Python Note

There is no direct Python equivalent for the AutomateDV hashdiff macro. In PySpark, the equivalent operation would be:

```python
from pyspark.sql import functions as F

# Manual hashdiff equivalent — for reference only, not recommended in vault pipelines
# AutomateDV enforces uppercase, null substitution, and column ordering automatically
payload_cols = ["BILLING_ADDRESS", "CITY", "COUNTRY_CODE",
                "CUSTOMER_NAME", "EMAIL_ADDRESS", "PHONE_NUMBER", "POSTCODE"]

concat_expr = F.concat_ws(
    "||^^||",
    *[F.upper(F.coalesce(F.col(c), F.lit("^^"))) for c in payload_cols]
)

df = df.withColumn("CUSTOMER_HASHDIFF", F.md5(concat_expr))
```

This approach requires manual maintenance and is not synchronised with the AutomateDV null substitution and uppercasing conventions. Use it only outside dbt contexts.

#### Validation — SQL

```sql
-- Verify hashdiff is not constant (would indicate all rows look identical to the satellite)
SELECT COUNT(DISTINCT CUSTOMER_HASHDIFF) AS distinct_hashdiffs,
       COUNT(*)                           AS total_rows
FROM main.staging.stg_customer;
-- Expected: distinct_hashdiffs << total_rows only if the source has many duplicates

-- Verify no rows have a null hashdiff
SELECT COUNT(*) AS null_hashdiff_count
FROM main.staging.stg_customer
WHERE CUSTOMER_HASHDIFF IS NULL;
-- Expected: 0
```

### Discussion and Concerns

- **Do NOT include load metadata in the hashdiff:** `LOAD_DATE`, `RECORD_SOURCE`, and any hash key columns must never appear in the hashdiff column list. They change on every load or are derived, not source payload.
- **Column ordering is fixed and documented:** The columns in the hashdiff must appear in the same order on every run. AutomateDV applies alphabetical ordering by default when `is_hashdiff: true`. Verify this matches your project standard before deploying.
- **Adding a column to the hashdiff is a breaking change:** If a new payload column is added to the hashdiff definition after data has already been loaded, every existing satellite row will appear to have changed on the next load, because the hashdiff now covers more columns than the previously stored value. Plan column additions carefully and document the impact on existing satellite history.
- **Hashdiff collisions:** MD5 collision probability is negligible for business data volumes (< billions of rows), but be aware that two distinct payloads could theoretically produce the same hashdiff. SHA-256 further reduces this probability at the cost of slightly larger storage.

### See Also

- [AutomateDV Hashdiff Documentation](https://automate-dv.readthedocs.io/en/latest/macros/stage/#hashed-columns)
- [dv2_raw_vault_cookbook.md — Satellite Loading](./dv2_raw_vault_cookbook.md#satellite-loading)

---

## Staging — dbt-utils generate_surrogate_key

The `dbt_utils.generate_surrogate_key` macro provides a simpler, dbt-native approach to key generation. It is not a replacement for AutomateDV hashing in a full vault pipeline, but it is useful for non-vault models, reference tables, or environments where AutomateDV is not installed.

### Problem

In some staging models — particularly for reference data, intermediate models, or marts — a hash-based surrogate key is needed without the full AutomateDV staging framework. Using a raw `MD5()` call is less portable and does not benefit from dbt's adapter-aware null handling.

### Solution

Use `dbt_utils.generate_surrogate_key()` to produce a hash-based key from one or more source columns.

#### SQL Example

```sql
-- Reference staging model for country codes.
-- Uses generate_surrogate_key because this model feeds a ref_hub, not a standard hub.
-- For standard vault Hub/Link models, use automate_dv.stage instead.

{{
    config(
        materialized='view',
        tags=['staging', 'reference']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['country_code']) }}           AS COUNTRY_HK,
    UPPER(TRIM(country_code))                                          AS COUNTRY_CODE,
    country_name,
    region,
    currency_code,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP)                             AS LOAD_DATE,
    'REFERENCE_DATA'                                                   AS RECORD_SOURCE
FROM {{ source('reference', 'raw_country_codes') }}
```

For a composite key across multiple columns:

```sql
-- Composite surrogate key across customer_id and order_id
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'order_id']) }} AS CUSTOMER_ORDER_SK,
    customer_id,
    order_id,
    order_date
FROM {{ source('ecommerce', 'raw_orders') }}
```

#### Python Example

```python
# dbt_utils.generate_surrogate_key is a dbt SQL macro with no PySpark equivalent.
# The underlying implementation uses MD5 on a coalesce + concatenation of the input columns.
# In PySpark, the equivalent is:

from pyspark.sql import functions as F

df = df.withColumn(
    "COUNTRY_HK",
    F.md5(F.coalesce(F.upper(F.trim(F.col("country_code"))), F.lit("")))
)
```

#### Validation — SQL

```sql
-- Verify no null surrogate keys
SELECT COUNT(*) AS null_sk_count
FROM main.staging.stg_ref_country
WHERE COUNTRY_HK IS NULL;
-- Expected: 0

-- Verify uniqueness of generated key for reference data
SELECT COUNTRY_HK, COUNT(*) AS cnt
FROM main.staging.stg_ref_country
GROUP BY COUNTRY_HK
HAVING cnt > 1;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Do not mix `generate_surrogate_key` and AutomateDV hashing in the same vault pipeline:** `generate_surrogate_key` applies `coalesce(col, '')` but does not uppercase values or apply the AutomateDV null substitution character (`^^`). A Hub row hashed by AutomateDV and a reference to the same natural key hashed by `generate_surrogate_key` will produce different hash values. The two approaches must never be used for the same entity within a single vault instance.
- **`generate_surrogate_key` uses MD5 by default:** On dbt-databricks, the macro generates a Spark-compatible MD5 expression. The exact SQL emitted varies by adapter — inspect the compiled SQL in `target/compiled/` to verify.
- **Appropriate use cases:** `generate_surrogate_key` is suitable for marts (where it is used as a dimension surrogate key, not a vault hash key), reference staging models, and utility models outside the vault graph.

### See Also

- [dbt-utils generate_surrogate_key Documentation](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#generate_surrogate_key-source)
- [AutomateDV Stage Macro](https://automate-dv.readthedocs.io/en/latest/macros/stage/)
- [dv2_architecture.md — Hash Key Design](./dv2_architecture.md#hash-key-design)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Null hash key count | dbt test results / `system.lakeflow` | Any non-zero count indicates a hashing failure in staging |
| Staging row count vs. source row count | dbt test `assert_equal_rowcount` / custom test | Mismatch indicates dropped or duplicated rows in staging |
| Hashdiff null count | dbt test results | Null hashdiff causes satellite to miss change detection |
| Staging model run time | Databricks Jobs UI / SQL Warehouse query history | Slow staging runs indicate unindexed source tables or large source volumes |
| Ghost record presence | Query `WHERE CUSTOMER_HK = md5('^^')` | Confirm ghost records are present when enabled |

### Metrics for Success

- [ ] Zero null hash key columns (`CUSTOMER_HK IS NOT NULL`) in all staging models after each run
- [ ] Zero null hashdiff columns (`CUSTOMER_HASHDIFF IS NOT NULL`) in all staging models
- [ ] Staging row count equals source row count for each source table on each run
- [ ] All hashed columns use the same algorithm (MD5 or SHA-256) — no mixed hashing within a vault pipeline
- [ ] Column ordering in all hashdiff definitions is documented and has not changed since the initial build
