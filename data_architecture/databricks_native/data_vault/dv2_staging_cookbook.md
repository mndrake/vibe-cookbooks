# Data Vault 2.0 Staging Cookbook

## Databricks Native Stack

> This file is the **Databricks-native** version of the Data Vault 2.0 staging cookbook.
> It uses Delta Live Tables (DLT), PySpark, and Spark SQL exclusively.
> No dbt, AutomateDV, or dbt-utils dependencies are required.
>
> Equivalent dbt + AutomateDV version: [../../../databricks_and_dbt/data_vault/dv2_staging_cookbook.md](../../../databricks_and_dbt/data_vault/dv2_staging_cookbook.md)

---

## Introduction

This cookbook provides practical, step-by-step guidance for **staging data in a Data Vault 2.0 pipeline** on Databricks using the native stack. It covers how to derive hash keys, compute hashdiff columns, and prepare source data for vault loading using Delta Live Tables (DLT) Python and SQL pipelines with native PySpark and Spark SQL functions. These patterns are prerequisites for all Raw Vault loading.

Hash key derivation replaces AutomateDV's `stage` macro with `MD5()`, `SHA2()`, `CONCAT_WS()`, and `COALESCE()` applied consistently in every staging pipeline. Orchestration replaces `dbt run` with Databricks Workflows (Lakeflow Jobs).

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.10+ | Required for Databricks Asset Bundles CLI |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | 0.200+ | Bundle deployment and workspace interaction |
| [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html) | Latest | Optional — for programmatic pipeline triggering |
| Delta Live Tables runtime | Current channel | Provided by Databricks — no installation needed |

Configure your Databricks CLI connection:

```bash
# Authenticate the Databricks CLI (OAuth or PAT)
databricks configure

# Verify connection
databricks clusters list

# Verify Unity Catalog access
databricks catalogs list
```

### Getting a New Starter Project

```bash
# Install the Databricks CLI
pip install databricks-cli

# Create a new Asset Bundle from the default template
databricks bundle init

# Edit databricks.yml to point to your workspace
# Then validate and deploy
databricks bundle validate
databricks bundle deploy --target dev
```

### Getting the Source for an Existing Project

```bash
git clone <repository_url>
cd data_vault_bundle

# Deploy the bundle
databricks bundle validate
databricks bundle deploy --target dev
```

---

## Infrastructure Pre-Requisites

### Infrastructure Required

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment | Unity Catalog must be enabled |
| DLT Pipeline (serverless or classic) | Compute for staging pipeline | Photon enabled; set to triggered mode for batch |
| Unity Catalog — `staging` schema | Target for staging views/tables | Pipeline service principal needs `USE SCHEMA`, `CREATE TABLE` |
| Unity Catalog — source schema or external table | Source data from ingestion layer | `SELECT` privilege required on source tables |

### Unity Catalog Staging Schema Setup

```sql
-- Create the staging schema (run as workspace admin or catalog owner)
CREATE SCHEMA IF NOT EXISTS main.staging
  COMMENT 'Data Vault 2.0 staging layer — hashed and prepped for vault loading';

-- Grant privileges to the DLT pipeline service principal
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA main.staging
  TO `dlt-pipeline-service-principal@your-org.com`;
```

---

## Staging — Native Hash Key Derivation with DLT

Hash keys and hashdiff columns are derived inline in DLT staging notebooks using native PySpark functions. There is no macro library involved — the derivation is explicit, auditable, and co-located with the staging logic.

### Problem

Raw ingested data from source systems contains natural keys and payload columns, but no hash keys, hashdiff columns, or metadata columns. Creating these manually across many staging pipelines is error-prone: column ordering mistakes silently produce incorrect hashes, and NULL handling is inconsistent. Any inconsistency in hash key computation breaks the integration promise of the vault.

### Solution

Implement hash key derivation in each DLT staging notebook using the shared `vault_utils.py` helper (see [dv2_architecture.md](./dv2_architecture.md)). The helper enforces consistent null substitution (`^^`), uppercasing, trimming, and column ordering. Each staging notebook produces a DLT view or streaming view consumed by Raw Vault pipelines.

#### Python DLT Example — `src/staging/stg_customer.py`

```python
# Staging pipeline for the CRM customer source.
# Produces CUSTOMER_HK (hub hash key) and CUSTOMER_HASHDIFF (satellite change detection).
# Source: main.bronze.crm_customer (ingested by Auto Loader in the ingestion layer)
# Output: LIVE.stg_crm_customer (DLT streaming view in the staging pipeline)

import dlt
from pyspark.sql.functions import (
    md5, concat_ws, coalesce, lit, upper, trim, col, current_timestamp
)

NULL_SUB = lit("^^")
SEP = "||"

def normalise(c):
    return upper(trim(coalesce(c.cast("string"), NULL_SUB)))

@dlt.view(
    name="stg_crm_customer",
    comment="Staged CRM customer records with hash keys and hashdiff"
)
@dlt.expect("customer_hk_not_null", "CUSTOMER_HK IS NOT NULL")
@dlt.expect("customer_hashdiff_not_null", "CUSTOMER_HASHDIFF IS NOT NULL")
def stg_crm_customer():
    return (
        spark.readStream.table("main.bronze.crm_customer")
        .select(
            # Hub hash key: single column
            md5(normalise(col("customer_id"))).alias("CUSTOMER_HK"),

            # Satellite hashdiff: all payload columns, alphabetical order, no metadata
            md5(concat_ws(SEP,
                normalise(col("billing_address")),
                normalise(col("city")),
                normalise(col("country_code")),
                normalise(col("customer_name")),
                normalise(col("email_address")),
                normalise(col("phone_number")),
                normalise(col("postcode"))
            )).alias("CUSTOMER_HASHDIFF"),

            # Metadata
            lit("CRM").alias("RECORD_SOURCE"),
            current_timestamp().alias("LOAD_DATE"),

            # Source payload columns
            col("customer_id"),
            col("customer_name"),
            col("email_address"),
            col("phone_number"),
            col("billing_address"),
            col("city"),
            col("postcode"),
            col("country_code")
        )
    )
```

#### SQL DLT Example — `src/staging/stg_customer.sql`

```sql
-- Staging pipeline for the CRM customer source (SQL DLT syntax).
-- Produces the same output as the Python version above.
-- Choose Python or SQL per team preference; both are fully supported by DLT.

CREATE OR REFRESH STREAMING VIEW stg_crm_customer
  COMMENT "Staged CRM customer records with hash keys and hashdiff"
  CONSTRAINT customer_hk_not_null    EXPECT (CUSTOMER_HK IS NOT NULL)    ON VIOLATION FAIL UPDATE
  CONSTRAINT customer_hashdiff_not_null EXPECT (CUSTOMER_HASHDIFF IS NOT NULL) ON VIOLATION FAIL UPDATE
AS
SELECT
  -- Hub hash key
  MD5(UPPER(TRIM(COALESCE(CAST(customer_id AS STRING), '^^'))))  AS CUSTOMER_HK,

  -- Satellite hashdiff: all payload columns, alphabetical order, no metadata
  MD5(CONCAT_WS('||',
    UPPER(TRIM(COALESCE(CAST(billing_address AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(city            AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(country_code    AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(customer_name   AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(email_address   AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(phone_number    AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(postcode        AS STRING), '^^')))
  ))                                                              AS CUSTOMER_HASHDIFF,

  -- Metadata
  'CRM'                                                          AS RECORD_SOURCE,
  CURRENT_TIMESTAMP()                                            AS LOAD_DATE,

  -- Source payload columns
  customer_id,
  customer_name,
  email_address,
  phone_number,
  billing_address,
  city,
  postcode,
  country_code

FROM STREAM(main.bronze.crm_customer);
```

**Functional difference between Python and SQL DLT staging:**
- Both approaches produce identical output. Python is preferable when the staging logic involves complex transformations, UDFs, or conditional column selection. SQL is preferable for teams more comfortable with SQL and for simpler projections.
- Python DLT notebooks run on the DLT cluster using the PySpark API. SQL DLT notebooks use Spark SQL. Both are executed and managed by the DLT runtime.
- Both support `@dlt.expect` / `CONSTRAINT ... EXPECT` data quality expectations, which fail or drop rows based on the violation mode.

#### Validation — SQL

After triggering the staging DLT pipeline, validate the output:

```sql
-- Verify no null hash keys
SELECT COUNT(*) AS null_hk_count
FROM main.staging.stg_crm_customer
WHERE CUSTOMER_HK IS NULL;
-- Expected: 0

-- Verify no null hashdiff
SELECT COUNT(*) AS null_hashdiff_count
FROM main.staging.stg_crm_customer
WHERE CUSTOMER_HASHDIFF IS NULL;
-- Expected: 0

-- Verify row count matches source
SELECT
  (SELECT COUNT(*) FROM main.bronze.crm_customer)    AS source_count,
  (SELECT COUNT(*) FROM main.staging.stg_crm_customer) AS staging_count;
-- Expected: source_count = staging_count
```

#### Validation — Python

```python
# Run from a Databricks notebook after the pipeline completes
staging = spark.table("main.staging.stg_crm_customer")
source  = spark.table("main.bronze.crm_customer")

null_hk       = staging.filter("CUSTOMER_HK IS NULL").count()
null_hashdiff = staging.filter("CUSTOMER_HASHDIFF IS NULL").count()
source_count  = source.count()
staging_count = staging.count()

assert null_hk == 0,       f"Null hash keys found: {null_hk}"
assert null_hashdiff == 0, f"Null hashdiffs found: {null_hashdiff}"
assert source_count == staging_count, (
    f"Row count mismatch: source={source_count}, staging={staging_count}"
)
print("All staging validations passed.")
```

### Discussion and Concerns

- **Column ordering is immutable after first load:** The columns passed to `CONCAT_WS` determine the concatenation order before hashing. Once data has been loaded to the vault using a given order, changing the order produces different hash values. Any existing vault rows with the old hash become orphaned. Document the ordering in a project-level standard and enforce it via code review.
- **`LOAD_DATE` must not be in the hashdiff:** The `LOAD_DATE` column must never appear in the `CUSTOMER_HASHDIFF` column list. Including it would cause every row to appear as a change on every load.
- **Ghost record injection:** Unlike AutomateDV, ghost record injection is not automatic in the native stack. If referential integrity for orphan satellite rows is required, add a ghost record manually by inserting a row with `MD5('^^')` as the hash key into each hub after initial table creation. This is done as a one-time SQL statement, not on every pipeline run.
- **`ON VIOLATION FAIL UPDATE` vs `ON VIOLATION DROP ROW`:** Use `FAIL UPDATE` for constraints that must never be violated (null hash keys). Use `DROP ROW` for soft deduplication rules where a small number of bad rows are expected and should be excluded without failing the pipeline.

### See Also

- [Delta Live Tables Expectations Documentation](https://docs.databricks.com/en/delta-live-tables/expectations.html)
- [dv2_architecture.md — Hash Key Design](./dv2_architecture.md#hash-key-design)
- [dv2_raw_vault_cookbook.md](./dv2_raw_vault_cookbook.md)

---

## Staging — Hashdiff Column Design

The hashdiff column is the mechanism by which satellites detect whether a row's descriptive attributes have changed since the last load. Correct hashdiff design is critical: errors in column selection produce missed changes (silent data quality failures) or false positives (unnecessary duplicate rows).

### Problem

A satellite must insert a new row only when one or more of a customer's descriptive attributes has changed since the last time that customer's data was loaded. If the hashdiff is computed from too few columns, genuine changes are missed. If it is computed from columns that change on every load (like a load timestamp), every row appears as a change and the satellite bloats with duplicate rows.

### Solution

Include every payload column — and only payload columns — in the hashdiff. The hashdiff must reflect the full descriptive state of the entity at load time. Apply columns in alphabetical order by column name.

#### Python Example — Hashdiff derivation

```python
from pyspark.sql.functions import md5, concat_ws, coalesce, lit, upper, trim, col

NULL_SUB = lit("^^")
SEP = "||"

def normalise(c):
    return upper(trim(coalesce(c.cast("string"), NULL_SUB)))

# The CUSTOMER_HASHDIFF covers all seven payload columns.
# No load metadata (LOAD_DATE, RECORD_SOURCE) is included.
# Column order is fixed alphabetically per project standard.
customer_hashdiff = md5(concat_ws(SEP,
    normalise(col("billing_address")),  # alphabetical: B
    normalise(col("city")),             # C
    normalise(col("country_code")),     # C (country after city)
    normalise(col("customer_name")),    # C (customer after country)
    normalise(col("email_address")),    # E
    normalise(col("phone_number")),     # P
    normalise(col("postcode"))          # P (postcode after phone)
))

df = df.withColumn("CUSTOMER_HASHDIFF", customer_hashdiff)
```

#### SQL Example — Hashdiff derivation

```sql
-- The CUSTOMER_HASHDIFF covers all seven payload columns.
-- No load metadata (LOAD_DATE, RECORD_SOURCE) is included.
-- Column order is fixed alphabetically per project standard.

MD5(CONCAT_WS('||',
  UPPER(TRIM(COALESCE(CAST(billing_address AS STRING), '^^'))),
  UPPER(TRIM(COALESCE(CAST(city            AS STRING), '^^'))),
  UPPER(TRIM(COALESCE(CAST(country_code    AS STRING), '^^'))),
  UPPER(TRIM(COALESCE(CAST(customer_name   AS STRING), '^^'))),
  UPPER(TRIM(COALESCE(CAST(email_address   AS STRING), '^^'))),
  UPPER(TRIM(COALESCE(CAST(phone_number    AS STRING), '^^'))),
  UPPER(TRIM(COALESCE(CAST(postcode        AS STRING), '^^')))
)) AS CUSTOMER_HASHDIFF
```

#### Validation — SQL

```sql
-- Verify hashdiff is not constant (would indicate all rows look identical to the satellite)
SELECT COUNT(DISTINCT CUSTOMER_HASHDIFF) AS distinct_hashdiffs,
       COUNT(*)                           AS total_rows
FROM main.staging.stg_crm_customer;
-- Expected: distinct_hashdiffs << total_rows only if the source has many duplicates

-- Verify no rows have a null hashdiff
SELECT COUNT(*) AS null_hashdiff_count
FROM main.staging.stg_crm_customer
WHERE CUSTOMER_HASHDIFF IS NULL;
-- Expected: 0
```

### Discussion and Concerns

- **Do NOT include load metadata in the hashdiff:** `LOAD_DATE`, `RECORD_SOURCE`, and any hash key columns must never appear in the hashdiff column list. They change on every load or are derived, not source payload.
- **Column ordering is fixed and documented:** The columns in the hashdiff must appear in the same order on every run. Alphabetical ordering by column name is the project standard. Document this in the project standard and enforce it in code review.
- **Adding a column to the hashdiff is a breaking change:** If a new payload column is added to the hashdiff definition after data has already been loaded, every existing satellite row will appear to have changed on the next load, because the hashdiff now covers more columns than the previously stored value. Plan column additions carefully and document the impact on existing satellite history.
- **Hashdiff collisions:** MD5 collision probability is negligible for business data volumes (< billions of rows), but be aware that two distinct payloads could theoretically produce the same hashdiff. SHA-256 further reduces this probability at the cost of slightly larger storage.

### See Also

- [dv2_raw_vault_cookbook.md — Satellite Loading](./dv2_raw_vault_cookbook.md#satellite-loading)
- [dv2_architecture.md — Hash Key Design](./dv2_architecture.md#hash-key-design)

---

## Staging — Multi-Source Staging and Reference Data

Reference data and multi-source staging follow the same hash derivation pattern as operational source staging, but the sources are different: reference data is loaded from a Delta table populated from a static CSV (uploaded to a Unity Catalog volume) or from a managed reference API.

### Problem

Country codes, currency codes, and product categories are used across multiple vault structures. Without vault-style hash keys and load dates on reference data, it cannot be joined using the same hash key conventions and cannot participate in PIT tables.

### Solution

Create a dedicated DLT staging view for each reference dataset. The hash key derivation pattern is identical to operational staging. The source is a Delta table rather than a streaming source.

#### Python Example — Reference staging

```python
# Staging pipeline for country code reference data.
# Source: main.reference.raw_country_codes (static Delta table loaded from CSV)
# Output: LIVE.stg_ref_country

import dlt
from pyspark.sql.functions import md5, concat_ws, coalesce, lit, upper, trim, col, current_timestamp

NULL_SUB = lit("^^")
SEP = "||"

def normalise(c):
    return upper(trim(coalesce(c.cast("string"), NULL_SUB)))

@dlt.view(
    name="stg_ref_country",
    comment="Staged country code reference data with hash keys and hashdiff"
)
@dlt.expect("country_hk_not_null", "COUNTRY_HK IS NOT NULL")
def stg_ref_country():
    # Reference data is batch (not streaming) — use spark.table, not readStream
    return (
        spark.table("main.reference.raw_country_codes")
        .select(
            md5(normalise(col("country_code"))).alias("COUNTRY_HK"),

            md5(concat_ws(SEP,
                normalise(col("country_name")),
                normalise(col("currency_code")),
                normalise(col("dialling_code")),
                normalise(col("region"))
            )).alias("COUNTRY_HASHDIFF"),

            lit("REFERENCE_DATA").alias("RECORD_SOURCE"),
            current_timestamp().alias("LOAD_DATE"),

            col("country_code"),
            col("country_name"),
            col("region"),
            col("currency_code"),
            col("dialling_code")
        )
    )
```

#### SQL Example — Reference staging

```sql
-- Reference staging for country codes (SQL DLT syntax)
CREATE OR REFRESH LIVE VIEW stg_ref_country
  COMMENT "Staged country code reference data with hash keys and hashdiff"
  CONSTRAINT country_hk_not_null EXPECT (COUNTRY_HK IS NOT NULL) ON VIOLATION FAIL UPDATE
AS
SELECT
  MD5(UPPER(TRIM(COALESCE(CAST(country_code AS STRING), '^^'))))  AS COUNTRY_HK,

  MD5(CONCAT_WS('||',
    UPPER(TRIM(COALESCE(CAST(country_name   AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(currency_code  AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(dialling_code  AS STRING), '^^'))),
    UPPER(TRIM(COALESCE(CAST(region         AS STRING), '^^')))
  ))                                                               AS COUNTRY_HASHDIFF,

  'REFERENCE_DATA'                                                 AS RECORD_SOURCE,
  CURRENT_TIMESTAMP()                                              AS LOAD_DATE,
  country_code,
  country_name,
  region,
  currency_code,
  dialling_code

FROM main.reference.raw_country_codes;
-- Note: LIVE VIEW (not STREAMING VIEW) because the source is a batch Delta table, not a stream
```

**Functional difference between streaming and batch DLT views:**
- `CREATE OR REFRESH STREAMING VIEW` / `spark.readStream.table()` is used for sources that receive new rows incrementally (e.g., Auto Loader ingestion targets, Kafka topics, append-only Bronze tables).
- `CREATE OR REFRESH LIVE VIEW` / `spark.table()` is used for batch sources that are fully reloaded on each pipeline run (e.g., reference data, small lookup tables).
- Reference data staging should always use batch views because reference tables may have corrections or additions that replace existing rows — streaming views would miss corrections.

#### Validation — SQL

```sql
-- Verify all expected country codes are present
SELECT COUNT(*) AS country_count
FROM main.staging.stg_ref_country;
-- Compare against the known number of rows in the source reference table

-- Verify uniqueness of generated key for reference data
SELECT COUNTRY_HK, COUNT(*) AS cnt
FROM main.staging.stg_ref_country
GROUP BY COUNTRY_HK
HAVING cnt > 1;
-- Expected: 0 rows
```

### Discussion and Concerns

- **Do not mix hash conventions across staging pipelines:** All staging pipelines — operational and reference — must use the same `UPPER() + TRIM() + COALESCE(..., '^^')` normalisation pattern and the same algorithm (MD5 or SHA-256). A reference hub row hashed differently from the hub row produced by an operational staging pipeline will produce different hash values for the same real-world entity.
- **Loading reference data from a CSV volume:** Upload the CSV to a Unity Catalog volume and read it using `spark.read.csv()` in a Databricks notebook, then write it to a managed Delta table. The DLT staging view then reads from that managed Delta table.

```python
# Load reference CSV into a managed Delta table (run once, outside DLT)
df_ref = spark.read.option("header", "true").csv(
    "/Volumes/main/reference/uploads/country_codes.csv"
)
df_ref.write.mode("overwrite").saveAsTable("main.reference.raw_country_codes")
```

### See Also

- [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [DLT Streaming vs Batch Sources](https://docs.databricks.com/en/delta-live-tables/load.html)
- [dv2_raw_vault_cookbook.md — Reference Structures](./dv2_raw_vault_cookbook.md#reference-structures)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Null hash key count | DLT pipeline event log / pipeline UI expectations panel | Any `FAIL UPDATE` violation count > 0 indicates a hashing failure |
| Staging row count vs. source row count | DLT pipeline metrics / custom query after each run | Mismatch indicates dropped rows — check `DROP ROW` expectation counts |
| Hashdiff null count | DLT expectations panel for `*_hashdiff_not_null` constraint | Null hashdiff causes satellite to miss change detection |
| Staging pipeline run time | Databricks Workflows UI / DLT pipeline event log | Slow staging runs indicate unoptimised source tables or large source volumes |
| Pipeline expectation violation history | `SELECT * FROM main.staging.__dlt_expectations` | Review violation trends over time |

### Querying DLT Expectation Results

```sql
-- DLT writes expectation results to an internal event log table
-- Access it via the pipeline's event log (cluster must have read access)
SELECT
  timestamp,
  details:flow_name::STRING    AS pipeline_step,
  details:name::STRING         AS expectation_name,
  details:passed_records::INT  AS passed_records,
  details:failed_records::INT  AS failed_records
FROM event_log('<pipeline-id>')
WHERE event_type = 'flow_progress'
  AND details:metrics IS NOT NULL
ORDER BY timestamp DESC;
```

### Metrics for Success

- [ ] Zero `FAIL UPDATE` violations for null hash key constraints on all staging views after each pipeline run
- [ ] Zero null hashdiff columns (`CUSTOMER_HASHDIFF IS NOT NULL`) in all staging views
- [ ] Staging row count equals source row count for each source table on each run
- [ ] All hashed columns use the same algorithm (MD5 or SHA-256) — no mixed hashing within a vault pipeline
- [ ] Column ordering in all hashdiff derivations is documented and has not changed since the initial build
- [ ] The shared `vault_utils.py` module is imported by every staging notebook — no inline hash derivation outside the shared module
