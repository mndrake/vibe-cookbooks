# Security Cookbook

## Introduction

This cookbook provides practical, step-by-step guidance for **implementing data security and governance** on Databricks with Unity Catalog. It covers role-based access control, secrets management, row-level security, column masking, audit logging, encryption configuration, and dbt-specific governance patterns. It is intended to be self-contained — no prior knowledge of Unity Catalog governance is assumed.

Each method section follows a consistent structure: the problem being solved, the recommended solution with Python and SQL examples, any known concerns or trade-offs, and links to further reading.

For the architectural decisions behind these patterns — when to use native Unity Catalog features versus dynamic views, where to enforce security in a multi-layer architecture — see `security_patterns.md` in the same directory.

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

> **On Databricks (interactive notebooks or jobs):** Unity Catalog SQL runs directly in notebooks or on a SQL Warehouse — no local tooling needed for the native SQL examples. dbt does not run on Databricks clusters; it runs on your local machine and submits SQL via the SQL Warehouse HTTP path.
>
> **Local development:** All tools below are installed on your local machine.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.9+ | Local dev | Required for dbt-databricks and the Databricks CLI |
| [dbt-databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup) | 1.6+ | Local dev | Runs locally; required for dbt grants and Unity Catalog tag integration examples |
| Databricks CLI | 0.200+ | Local dev | Required for secrets management examples |

Configure your Databricks connection:

```bash
# Authenticate the Databricks CLI
databricks configure --token

# Verify connection
databricks clusters list
```

For dbt, configure `~/.dbt/profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: databricks
      host: adb-<workspace-id>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<warehouse-id>
      token: "{{ env_var('DBT_TOKEN') }}"
      schema: dev_marts
      catalog: main
```

### Getting a New Starter Project

```bash
dbt init my_security_project
cd my_security_project
dbt debug  # Verify connection
```

### Getting the Source for an Existing Project

```bash
git clone <repository_url>
cd <project_directory>
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
| Unity Catalog Metastore | Governance layer | Must be attached to the workspace |
| SQL Warehouse | Query execution for SQL examples | Serverless or Pro warehouse recommended |
| Databricks Groups | Identity management | Sync from AAD/Okta or manage in the account console |
| Databricks Secret Scope | Credential storage | Required for the Secrets Management example |

### Enabling Unity Catalog

Unity Catalog requires a metastore to be created in the Databricks account console and attached to the workspace. This is a one-time setup performed by the Databricks account admin. Verify that Unity Catalog is active:

```sql
-- Confirm Unity Catalog is enabled and accessible
SHOW CATALOGS;
```

If `SHOW CATALOGS` returns results, Unity Catalog is active. If the command is not recognised, the workspace is running on the legacy Hive metastore and Unity Catalog must be enabled before proceeding.

---

## RBAC with Unity Catalog

Role-based access control (RBAC) in Databricks is implemented through Unity Catalog `GRANT` and `REVOKE` statements. Groups — not individual users — are the recommended grant target. Groups are managed in the Databricks account console or synced from an identity provider such as Azure Active Directory or Okta.

### Problem

Different teams (data engineers, analysts, BI consumers) need different levels of access to Databricks data assets. Access must be manageable at scale, auditable for compliance, and revocable when roles change. Managing grants to individual users does not scale and creates audit gaps when users move between teams.

### Solution

Use Unity Catalog `GRANT` statements scoped to Databricks groups. The following example sets up a complete privilege structure for a Data Vault pipeline with three roles: a dbt service principal (`dbt_service_principal`), a data engineering group (`data_engineers`), and an analyst group (`analysts`).

#### Python Example

```python
# Apply the full privilege set for a Data Vault pipeline using spark.sql()
# Run these in a Databricks notebook or as part of a setup script

# --- Catalog-level: all roles need USE CATALOG to navigate the catalog ---
spark.sql("GRANT USE CATALOG ON CATALOG main TO `data_engineers`")
spark.sql("GRANT USE CATALOG ON CATALOG main TO `analysts`")
spark.sql("GRANT USE CATALOG ON CATALOG main TO `dbt_service_principal`")

# --- raw_vault schema: engineers and SP only ---
spark.sql("GRANT USE SCHEMA ON SCHEMA main.raw_vault TO `data_engineers`")
spark.sql("GRANT SELECT ON SCHEMA main.raw_vault TO `data_engineers`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.raw_vault TO `dbt_service_principal`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.raw_vault TO `dbt_service_principal`")
spark.sql("GRANT SELECT ON SCHEMA main.raw_vault TO `dbt_service_principal`")

# --- business_vault schema: SP needs CREATE and SELECT; engineers can read ---
spark.sql("GRANT USE SCHEMA ON SCHEMA main.business_vault TO `data_engineers`")
spark.sql("GRANT SELECT ON SCHEMA main.business_vault TO `data_engineers`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.business_vault TO `dbt_service_principal`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.business_vault TO `dbt_service_principal`")
spark.sql("GRANT SELECT ON SCHEMA main.business_vault TO `dbt_service_principal`")

# --- marts schema: analysts get SELECT; SP materialises the mart tables ---
spark.sql("GRANT USE SCHEMA ON SCHEMA main.marts TO `analysts`")
spark.sql("GRANT SELECT ON TABLE main.marts.dim_customer TO `analysts`")
spark.sql("GRANT SELECT ON TABLE main.marts.dim_product TO `analysts`")
spark.sql("GRANT SELECT ON TABLE main.marts.fact_orders TO `analysts`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.marts TO `dbt_service_principal`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.marts TO `dbt_service_principal`")
spark.sql("GRANT SELECT ON SCHEMA main.marts TO `dbt_service_principal`")

# --- Verify the grants on a sensitive table ---
spark.sql("SHOW GRANTS ON TABLE main.marts.dim_customer").show(truncate=False)
```

#### SQL Example

```sql
-- Catalog-level navigation grant — required for all roles before any schema or table access
GRANT USE CATALOG ON CATALOG main TO `data_engineers`;
GRANT USE CATALOG ON CATALOG main TO `analysts`;
GRANT USE CATALOG ON CATALOG main TO `dbt_service_principal`;

-- raw_vault: data engineers read-only; dbt SP reads and materialises
GRANT USE SCHEMA, SELECT ON SCHEMA main.raw_vault TO `data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA main.raw_vault TO `dbt_service_principal`;

-- business_vault: same pattern — engineers read, SP creates and reads
GRANT USE SCHEMA, SELECT ON SCHEMA main.business_vault TO `data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA main.business_vault TO `dbt_service_principal`;

-- marts: analysts get SELECT on specific consumer-facing tables only
GRANT USE SCHEMA ON SCHEMA main.marts TO `analysts`;
GRANT SELECT ON TABLE main.marts.dim_customer TO `analysts`;
GRANT SELECT ON TABLE main.marts.dim_product TO `analysts`;
GRANT SELECT ON TABLE main.marts.fact_orders TO `analysts`;

-- marts: dbt SP can materialise and read all mart objects
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA main.marts TO `dbt_service_principal`;

-- Audit current grants on a table
SHOW GRANTS ON TABLE main.marts.dim_customer;

-- Revoke a grant if a role changes
REVOKE SELECT ON TABLE main.marts.dim_customer FROM `analysts`;
```

### Discussion and Concerns

- **Groups, not users:** Always grant to groups managed in the Databricks account console. When a user joins or leaves a team, add or remove them from the group — do not re-run GRANT/REVOKE statements for every individual.
- **Schema grants do not cascade to existing tables:** `GRANT SELECT ON SCHEMA` gives the grantee the right to access future tables created in that schema, but does not automatically grant `SELECT` on tables that already exist. For existing tables, you must also run `GRANT SELECT ON TABLE` or use `GRANT SELECT ON ALL TABLES IN SCHEMA`. Use `GRANT SELECT ON ALL TABLES IN SCHEMA main.marts TO \`analysts\`` if you intend to expose all tables in a schema.
- **AAD/Okta group sync:** When groups are synced from an external identity provider, group membership is managed in AAD or Okta. Changes propagate to Databricks on the next sync cycle (typically minutes to one hour). Test group membership with `SELECT IS_MEMBER('analysts')` in a notebook to confirm sync.
- **Audit grants regularly:** Run `SHOW GRANTS ON SCHEMA main.raw_vault` weekly for sensitive schemas and compare against the expected role list. Unexpected grants are a security finding.

### See Also

- [Unity Catalog privileges — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html)
- [Sync identities from AAD — Databricks](https://docs.databricks.com/en/administration-guide/users-groups/scim/index.html)
- `security_patterns.md` — role definitions and privilege scope decisions

---

## dbt Grants Block

dbt materialises models by running `CREATE OR REPLACE TABLE` (or equivalent) statements. Each time a model is fully refreshed, the resulting table is a new object, and any `GRANT` statements previously applied to it are lost. Without automated grant management, every dbt model rebuild requires a manual privilege re-application step.

### Problem

Every time a dbt model is rebuilt with a full refresh, the `GRANT` statements on the resulting table are dropped along with the old table object. Re-applying grants manually after each pipeline run is error-prone and creates windows of broken access for downstream consumers.

### Solution

Use dbt's `grants` configuration block to define which roles should have `SELECT` on a model's output table. dbt applies the specified grants as post-hooks after the model materialises, ensuring access is restored automatically on every run.

#### Python Example

```python
# dbt triggers post-hooks automatically after materialisation — there is no Python
# runtime code to write for the grants themselves. The dbt run command handles it:

# Run dbt models and apply grants in one step
# (executed from the terminal or a Databricks job running dbt)
# dbt run --select marts.dim_customer

# To verify grants were applied after a run, use spark.sql() in a notebook:
spark.sql("SHOW GRANTS ON TABLE main.marts.dim_customer").show(truncate=False)
```

#### SQL Example

```sql
-- Model-level grants config (place in the model's .sql file header as a Jinja config block)
-- This example is for models/marts/dim_customer.sql

{{
  config(
    materialized='table',
    grants={
      'select': ['analysts', 'bi_service_account']
    }
  )
}}

SELECT
    c.customer_hk,
    c.customer_id,
    cd.full_name,
    cd.email,
    cd.phone,
    cd.region
FROM {{ ref('hub_customer') }} c
LEFT JOIN {{ ref('sat_customer_details_masked') }} cd
    ON c.customer_hk = cd.customer_hk
    AND cd.dbt_valid_to IS NULL
```

```yaml
# Project-level grants config — apply the same grants to all models in a folder
# Place in dbt_project.yml under the models: key

models:
  my_project:
    marts:
      +grants:
        select:
          - analysts
          - bi_service_account
    raw_vault:
      +grants:
        select:
          - data_engineers
          - dbt_service_principal
```

```yaml
# To replace existing grants on each run rather than accumulating them,
# set copy_grants: true in dbt_project.yml

models:
  my_project:
    marts:
      +copy_grants: true
      +grants:
        select:
          - analysts
          - bi_service_account
```

### Discussion and Concerns

- **Additive vs. replace behaviour:** By default, dbt grants are additive — if `analysts` was previously granted and you add `bi_service_account` to the grants config, both will be present after the next run. Set `copy_grants: true` in `dbt_project.yml` to replace the full set of grants with exactly what is defined in the config on each run. Use `copy_grants: true` in production to prevent grant drift.
- **Post-hook timing:** dbt applies grants after the model has been materialised and committed. There is a brief window between materialisation and grant application during which the new table exists but is not yet accessible to consumer groups. For most batch pipelines this is acceptable; for pipelines with SLA-sensitive consumers, plan the run schedule to account for this.
- **Schema-level grants are separate:** dbt grants config manages `SELECT` on table objects only. `USE SCHEMA` and `USE CATALOG` grants must be managed separately (via `GRANT` statements in a setup script or a dbt on-run-start hook). A consumer who loses `USE SCHEMA` will not be able to access any table in the schema even if they have `SELECT` on individual tables.
- **Incremental models:** For incremental models, dbt does not re-run `CREATE OR REPLACE TABLE` — it runs a `MERGE` or `INSERT`. Grants are re-applied as post-hooks regardless of materialisation mode, so incremental runs also enforce the grants config correctly.

### See Also

- [dbt grants configuration — dbt docs](https://docs.getdbt.com/reference/resource-configs/grants)
- [dbt-databricks Unity Catalog integration — dbt docs](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
- `security_patterns.md` — dbt service principal privilege model

---

## Secrets Management

Notebooks and job scripts frequently contain hardcoded credentials — storage account keys, API tokens, database connection strings — that are visible in version control, job run logs, and notebook revision history. This is a critical security risk: any user with access to the notebook or the job run output can retrieve the credential.

### Problem

Credentials stored directly in notebooks or job configurations are exposed in version control, notebook revision history, and job run logs. A single committed storage key can grant unrestricted access to an ADLS storage account containing all raw and production data.

### Solution

Use Databricks Secrets to store credentials outside of code. Secrets are stored in a secret scope, retrieved at runtime using `dbutils.secrets.get()`, and automatically redacted in notebook output so they cannot appear in logs. The Databricks CLI is used to create scopes and store secrets — these operations require Databricks workspace admin rights or the `Create Secret Scope` permission.

#### Python Example

```python
# Step 1: Create a secret scope (run once from the terminal using the Databricks CLI)
# databricks secrets create-scope my-scope

# Step 2: Store a secret in the scope (run from the terminal)
# databricks secrets put-secret my-scope storage-key --string-value "<your-storage-key>"

# Step 3: Retrieve the secret at runtime in a notebook or job script
storage_key = dbutils.secrets.get(scope="my-scope", key="storage-key")

# Step 4: Use the secret to configure ADLS access via Spark config
# The key is passed directly — it is never stored in a variable visible in logs
spark.conf.set(
    "fs.azure.account.key.mystorageaccount.dfs.core.windows.net",
    dbutils.secrets.get(scope="my-scope", key="storage-key")
)

# Step 5: Verify that the secret is redacted in output
# The following print statement will output [REDACTED] — not the actual key value
print(dbutils.secrets.get(scope="my-scope", key="storage-key"))

# Step 6: Read from ADLS using the configured key
df = spark.read.format("delta").load(
    "abfss://raw@mystorageaccount.dfs.core.windows.net/landing/"
)
df.show(5)

# List all secrets in a scope (returns key names only — not values)
dbutils.secrets.list("my-scope")
```

```bash
# Databricks CLI commands for secret management
# (run from a terminal with the Databricks CLI configured)

# Create a secret scope (Databricks-managed backend)
databricks secrets create-scope my-scope

# Store a secret interactively (prompts for the value — not echoed to terminal)
databricks secrets put-secret my-scope storage-key

# Store a secret from a file (useful in CI/CD pipelines)
databricks secrets put-secret my-scope storage-key --bytes-value "$(cat storage_key.txt)"

# List all scopes
databricks secrets list-scopes

# List keys in a scope (does not reveal values)
databricks secrets list-secrets my-scope

# Delete a secret
databricks secrets delete-secret my-scope storage-key
```

#### SQL Example

```sql
-- There is no SQL equivalent for creating secret scopes or retrieving secrets.
-- Secrets are only accessible in Python and Scala via dbutils.secrets,
-- or via Spark configuration references set before a SQL query runs.

-- After configuring the Spark session with a secret-backed credential (in Python),
-- SQL queries can use the configured data source transparently:

-- This query works after spark.conf.set(..., dbutils.secrets.get(...)) has been run
SELECT *
FROM delta.`abfss://raw@mystorageaccount.dfs.core.windows.net/landing/orders/`
LIMIT 10;
```

### Discussion and Concerns

- **Secrets are always redacted in output:** Databricks replaces any secret value that appears in notebook cell output with `[REDACTED]`. This applies to `print()`, `display()`, and exception messages. It does not apply to values written to files, Delta tables, or external systems — never write a secret value to a table or log file.
- **Prefer managed identity over key-based auth:** Where the Databricks workspace is deployed on Azure, configure a managed identity or service principal with Azure RBAC roles on the ADLS storage account instead of using storage account keys. Managed identities do not require key rotation, are scoped to specific resources, and cannot be extracted from the workspace.
- **Secret scope permissions:** By default, the creator of a secret scope has full control. Grant read access to other users or groups using `databricks secrets put-acl my-scope <group-name> READ`. Without an explicit ACL grant, other users cannot read secrets from the scope even if they know the scope and key names.
- **Azure Key Vault-backed scopes:** On Azure Databricks, secret scopes can be backed by Azure Key Vault, allowing secrets to be managed centrally in Key Vault and consumed transparently via `dbutils.secrets.get()`. This is the recommended approach for enterprise environments where Key Vault is the standard credential store.

### See Also

- [Databricks Secrets — Databricks](https://docs.databricks.com/en/security/secrets/index.html)
- [Azure Key Vault-backed secret scope — Databricks](https://docs.databricks.com/en/security/secrets/secret-scopes.html#azure-key-vault-backed-scopes)
- [Managed identities for Azure Databricks — Microsoft](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/cloud-configurations/azure/managed-identities-storage)

---

## Row-Level Security (RLS) with Dynamic Views

A shared table containing data for multiple regions, departments, or customer segments cannot safely be exposed to all users without filtering. Maintaining separate tables per user group is operationally unsustainable — it multiplies storage, pipeline complexity, and schema management overhead proportionally with the number of groups.

### Problem

Analysts in the EMEA region should only see customer records where `region = 'EMEA'`. Analysts in APAC should only see `region = 'APAC'`. Both groups query the same mart table. Separate tables per region are not feasible at scale, and a single unfiltered table exposes all regions to all analysts.

### Solution

Create a dynamic view in the marts schema that uses `current_user()` and `IS_MEMBER()` to filter rows at query time. Grant `SELECT` to analysts on the view, not on the underlying table. Revoke direct access to the underlying table from consumer-facing groups.

#### Python Example

```python
# Create the user-to-region mapping table (run once as setup)
spark.sql("""
    CREATE TABLE IF NOT EXISTS main.security.user_region_mapping (
        email     STRING NOT NULL,
        region    STRING NOT NULL
    )
    USING DELTA
""")

# Insert sample mappings
spark.sql("""
    INSERT INTO main.security.user_region_mapping VALUES
    ('alice@example.com',  'EMEA'),
    ('bob@example.com',    'APAC'),
    ('charlie@example.com','AMER')
""")

# Create the RLS dynamic view
spark.sql("""
    CREATE OR REPLACE VIEW main.marts.v_customer_regional AS
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.region,
        c.account_status
    FROM main.marts.dim_customer c
    WHERE
        -- Filter to the calling user's assigned region
        c.region = (
            SELECT region
            FROM main.security.user_region_mapping
            WHERE email = current_user()
        )
        -- OR the caller is a member of the admin group (sees all regions)
        OR IS_MEMBER('admin_group')
""")

# Grant SELECT on the view to analysts — NOT on the underlying table
spark.sql("GRANT SELECT ON VIEW main.marts.v_customer_regional TO `analysts`")

# Ensure analysts cannot bypass the view by querying the table directly
spark.sql("REVOKE SELECT ON TABLE main.marts.dim_customer FROM `analysts`")

# Test: check which region the current notebook user sees
spark.sql("SELECT DISTINCT region FROM main.marts.v_customer_regional").show()
```

#### SQL Example

```sql
-- Create the security mapping table
CREATE TABLE IF NOT EXISTS main.security.user_region_mapping (
    email  STRING NOT NULL,
    region STRING NOT NULL
)
USING DELTA;

-- Populate with user-to-region assignments
INSERT INTO main.security.user_region_mapping VALUES
    ('alice@example.com',   'EMEA'),
    ('bob@example.com',     'APAC'),
    ('charlie@example.com', 'AMER');

-- Create the dynamic view with row-level filtering
CREATE OR REPLACE VIEW main.marts.v_customer_regional AS
SELECT
    customer_id,
    full_name,
    email,
    region,
    account_status
FROM main.marts.dim_customer
WHERE
    region = (
        SELECT region
        FROM main.security.user_region_mapping
        WHERE email = current_user()
    )
    OR IS_MEMBER('admin_group');

-- Grant on the view only — consumers must not access the base table directly
GRANT SELECT ON VIEW main.marts.v_customer_regional TO `analysts`;
REVOKE SELECT ON TABLE main.marts.dim_customer FROM `analysts`;

-- Validate: this should return only the rows for the calling user's region
SELECT DISTINCT region FROM main.marts.v_customer_regional;
```

### Discussion and Concerns

- **Grant on the view, not the table:** The RLS view is only effective if the underlying table's `SELECT` grant is revoked from consumer groups. If analysts retain `SELECT` on `dim_customer`, they can bypass the view entirely. Always pair view creation with a `REVOKE` on the underlying table.
- **`IS_MEMBER()` checks Databricks group membership:** `IS_MEMBER('admin_group')` returns `true` if the calling user is a member of the Databricks group named `admin_group`. Group names are case-sensitive. Test with `SELECT IS_MEMBER('admin_group')` in a notebook to confirm the value before relying on it for security logic.
- **`current_user()` returns the calling user's email address:** This is the Databricks account email — the same email used to log in to the workspace. In job runs, `current_user()` returns the identity of the service principal or user who owns the job, not the identity of the user who triggered the run.
- **Performance:** The correlated subquery against `user_region_mapping` is evaluated per query. For large `dim_customer` tables, ensure that the `region` column is included in the table's `ZORDER` or liquid clustering key, and that `user_region_mapping` is small enough to be broadcast. The filter is pushed down to the underlying Delta table scan when Photon is enabled, but the effectiveness of predicate pushdown depends on the clustering and statistics of the table.
- **View recreation on logic change:** Any change to the RLS logic requires dropping and recreating the view. This removes the view object and — on Delta — requires re-granting `SELECT` on the new view object.

### See Also

- [Dynamic views for row-level security — Databricks](https://docs.databricks.com/en/views/dynamic.html)
- [current_user() function — Databricks](https://docs.databricks.com/en/sql/language-manual/functions/current_user.html)
- [IS_MEMBER() function — Databricks](https://docs.databricks.com/en/sql/language-manual/functions/is_member.html)
- `security_patterns.md` — when to use dynamic views versus native Unity Catalog masking

---

## Column Masking with Dynamic Views

A table containing PII — email addresses, phone numbers, national identifiers — needs to be accessible to analysts for analysis, but the raw PII values should only be visible to users with explicit authorisation. Maintaining two copies of the table (one masked, one unmasked) doubles storage costs and creates synchronisation risk.

### Problem

`sat_customer_details` in the raw vault contains `email` and `phone` columns. Data engineers need to see the real values for debugging. General analysts should see masked values. A single access policy covering both groups with the same underlying table is required.

### Solution

Create a dynamic view that conditionally reveals or masks column values based on the calling user's group membership. Grant `SELECT` on the view to all users who need access to the table's non-sensitive columns. Revoke direct table access from all non-engineer groups.

#### Python Example

```python
# Create the column-masking dynamic view
spark.sql("""
    CREATE OR REPLACE VIEW main.marts.v_customer_masked AS
    SELECT
        customer_hk,
        load_date,
        record_source,
        full_name,
        -- Email: show full value to pii_viewers, mask for everyone else
        CASE
            WHEN IS_MEMBER('pii_viewers')
            THEN email
            ELSE CONCAT(LEFT(email, 2), '****@****.com')
        END AS email,
        -- Phone: show full value to pii_viewers, replace all digits with * for others
        CASE
            WHEN IS_MEMBER('pii_viewers')
            THEN phone
            ELSE REGEXP_REPLACE(phone, '[0-9]', '*')
        END AS phone,
        region,
        account_status
    FROM main.raw_vault.sat_customer_details
""")

# Grant SELECT on the masked view to the general analyst group
spark.sql("GRANT SELECT ON VIEW main.marts.v_customer_masked TO `analysts`")

# pii_viewers group also gets SELECT — they see real values through the same view
spark.sql("GRANT SELECT ON VIEW main.marts.v_customer_masked TO `pii_viewers`")

# Revoke direct satellite access from analyst and pii_viewer groups
# (engineers and the dbt SP retain their schema-level grants)
spark.sql("REVOKE SELECT ON TABLE main.raw_vault.sat_customer_details FROM `analysts`")

# Test: run as a non-pii_viewers user to confirm masking is applied
spark.sql("""
    SELECT customer_hk, email, phone
    FROM main.marts.v_customer_masked
    LIMIT 5
""").show(truncate=False)
```

#### SQL Example

```sql
-- Create the column-masking dynamic view on the PII satellite
CREATE OR REPLACE VIEW main.marts.v_customer_masked AS
SELECT
    customer_hk,
    load_date,
    record_source,
    full_name,
    CASE
        WHEN IS_MEMBER('pii_viewers')
        THEN email
        ELSE CONCAT(LEFT(email, 2), '****@****.com')
    END AS email,
    CASE
        WHEN IS_MEMBER('pii_viewers')
        THEN phone
        ELSE REGEXP_REPLACE(phone, '[0-9]', '*')
    END AS phone,
    region,
    account_status
FROM main.raw_vault.sat_customer_details;

-- Grant on the view — not the underlying satellite table
GRANT SELECT ON VIEW main.marts.v_customer_masked TO `analysts`;
GRANT SELECT ON VIEW main.marts.v_customer_masked TO `pii_viewers`;

-- Confirm masking is active for the current session
-- (result depends on whether the calling user is in pii_viewers)
SELECT email, phone
FROM main.marts.v_customer_masked
LIMIT 10;
```

### Discussion and Concerns

- **Masking logic lives in the view definition:** If the masking pattern changes (e.g., a new policy requires showing the full domain but masking the username), the view must be dropped and recreated. This removes the view object, requiring re-grants. Script both the `CREATE VIEW` and `GRANT` statements together in version control so they can be rerun atomically.
- **Unity Catalog native column masks as an alternative:** On Databricks Runtime 12.2+ with Unity Catalog Premium, native column mask functions provide a cleaner solution — one function definition, applied to the table directly, without requiring view proliferation. See the **Unity Catalog Native Column Masks** section below for implementation details.
- **`REGEXP_REPLACE(phone, '[0-9]', '*')`:** This replaces every digit in the phone string with `*`, preserving formatting characters like hyphens and parentheses. Adjust the regex pattern to match your organisation's phone number format and masking requirement.
- **View performance on large satellites:** The `CASE WHEN IS_MEMBER(...)` expression is evaluated per row. It is a constant for the duration of the query (the group membership is resolved once at query start), so the performance overhead is minimal and equivalent to a simple `CASE WHEN <boolean constant> THEN ... ELSE ...` evaluation.

### See Also

- [Dynamic views — Databricks](https://docs.databricks.com/en/views/dynamic.html)
- [REGEXP_REPLACE — Databricks SQL](https://docs.databricks.com/en/sql/language-manual/functions/regexp_replace.html)
- `security_patterns.md` — satellite-level security boundary design

---

## Unity Catalog Native Column Masks

Each dynamic view created for column masking is a schema object that must be created, granted, and maintained independently. In a Data Vault with many satellite tables containing PII columns, this proliferates view objects and distributes masking logic across many view definitions — making it difficult to audit what the current masking policy is and to update it centrally.

### Problem

Masking logic for the `email` column is duplicated across five different views covering five different satellites. When the organisation's PII masking policy changes, all five views must be individually recreated. There is no single place to see the full masking policy.

### Solution

Define a Unity Catalog column mask function once, then apply it to each column using `ALTER TABLE ... ALTER COLUMN ... SET MASK`. The function is reusable across all tables in the catalog, and updating the function definition updates the masking behaviour on all tables it is applied to simultaneously.

#### Python Example

```python
# Step 1: Create the mask function in a dedicated security schema
spark.sql("""
    CREATE FUNCTION IF NOT EXISTS main.security.mask_email(email STRING)
    RETURNS STRING
    RETURN CASE
        WHEN IS_MEMBER('pii_viewers')
        THEN email
        ELSE CONCAT(LEFT(email, 2), '****@****.com')
    END
""")

spark.sql("""
    CREATE FUNCTION IF NOT EXISTS main.security.mask_phone(phone STRING)
    RETURNS STRING
    RETURN CASE
        WHEN IS_MEMBER('pii_viewers')
        THEN phone
        ELSE REGEXP_REPLACE(phone, '[0-9]', '*')
    END
""")

# Step 2: Apply the mask function to the email and phone columns on the satellite table
spark.sql("""
    ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN email SET MASK main.security.mask_email
""")

spark.sql("""
    ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN phone SET MASK main.security.mask_phone
""")

# Step 3: Apply the same email mask function to another satellite — no new function needed
spark.sql("""
    ALTER TABLE main.raw_vault.sat_account_contact
    ALTER COLUMN contact_email SET MASK main.security.mask_email
""")

# Step 4: Verify the mask is applied — query the table directly (no view needed)
spark.sql("""
    SELECT customer_hk, email, phone
    FROM main.raw_vault.sat_customer_details
    LIMIT 5
""").show(truncate=False)

# Step 5: Remove a mask if needed
spark.sql("""
    ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN email DROP MASK
""")
```

#### SQL Example

```sql
-- Step 1: Create mask functions in the security schema
CREATE FUNCTION IF NOT EXISTS main.security.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_MEMBER('pii_viewers')
    THEN email
    ELSE CONCAT(LEFT(email, 2), '****@****.com')
END;

CREATE FUNCTION IF NOT EXISTS main.security.mask_phone(phone STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_MEMBER('pii_viewers')
    THEN phone
    ELSE REGEXP_REPLACE(phone, '[0-9]', '*')
END;

-- Step 2: Apply masks to the satellite table columns
ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN email SET MASK main.security.mask_email;

ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN phone SET MASK main.security.mask_phone;

-- Step 3: Apply the same email mask to a second satellite — function is reused
ALTER TABLE main.raw_vault.sat_account_contact
    ALTER COLUMN contact_email SET MASK main.security.mask_email;

-- Step 4: Query the table directly — masking is applied transparently
SELECT customer_hk, email, phone
FROM main.raw_vault.sat_customer_details
LIMIT 10;

-- Step 5: List all column masks applied in the catalog (Unity Catalog system tables)
SELECT table_catalog, table_schema, table_name, column_name, mask_name
FROM main.information_schema.column_masks
ORDER BY table_schema, table_name;

-- Step 6: Drop a mask when it is no longer needed
ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN email DROP MASK;
```

### Discussion and Concerns

- **Requires Unity Catalog Premium:** Native column masks are a Unity Catalog Premium feature. Confirm that the workspace subscription includes Premium before using this feature. On workspaces without Premium, use dynamic views as described in the previous section.
- **Requires Databricks Runtime 12.2+:** The `ALTER TABLE ... SET MASK` syntax was introduced in DBR 12.2. Clusters running on older runtimes will not support this command.
- **Masks are applied transparently — no view required:** Queries against the underlying table automatically receive masked output. This means consumers can query the table directly (with `SELECT` on the table) and the mask is still enforced. This eliminates the need to revoke table access and redirect consumers to a view.
- **Mask function updates apply immediately to all masked columns:** If `mask_email` is updated (e.g., to use a different masking pattern), every column in every table that references `main.security.mask_email` immediately uses the new logic. Test mask function changes in a non-production environment before updating in production.
- **Mask functions appear in Unity Catalog lineage:** Column mask functions are tracked in Unity Catalog's lineage graph — you can see which tables and columns a mask function is applied to via the Unity Catalog UI or `information_schema.column_masks`.

### See Also

- [Unity Catalog column masks — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)
- [ALTER TABLE SET MASK — Databricks SQL](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html)
- `security_patterns.md` — native masks versus dynamic views trade-offs

---

## Audit Logging

Compliance requirements (SOC 2, GDPR, HIPAA) typically require evidence that access to sensitive data is logged and that logs are retained for a defined period. Databricks Unity Catalog provides system tables that record all data access events — who accessed what, when, and from which query.

### Problem

The compliance team requires a report of all access to the `sat_customer_details` table in the last 30 days, including the user identity and the action taken. Databricks job run history does not contain the data-level query detail needed to satisfy this requirement.

### Solution

Query the Unity Catalog `system.access.audit` table, which records all Unity Catalog operations including `SELECT`, `CREATE TABLE`, `GRANT`, and `REVOKE`. Filter by action type and time window to produce the required compliance report.

#### Python Example

```python
from pyspark.sql import functions as F

# Query all SELECT and READ events in the last 7 days
audit_df = spark.sql("""
    SELECT
        event_time,
        user_identity.email       AS user_email,
        action_name,
        request_params.table_full_name AS table_accessed,
        request_params.query     AS query_text,
        response.status_code     AS status_code
    FROM system.access.audit
    WHERE action_name IN ('commandSubmit', 'runCommand', 'read', 'select')
      AND event_time >= current_timestamp() - INTERVAL 7 DAYS
    ORDER BY event_time DESC
""")

audit_df.show(20, truncate=False)

# Filter to accesses on a specific sensitive table
sensitive_table_access = spark.sql("""
    SELECT
        event_time,
        user_identity.email AS user_email,
        action_name,
        request_params
    FROM system.access.audit
    WHERE
        -- Look for query events that reference the sensitive table
        (
            lower(to_json(request_params)) LIKE '%sat_customer_details%'
            OR lower(to_json(request_params)) LIKE '%main.raw_vault%'
        )
        AND event_time >= current_timestamp() - INTERVAL 30 DAYS
    ORDER BY event_time DESC
""")

sensitive_table_access.show(50, truncate=False)

# Export audit data for long-term compliance storage
(
    sensitive_table_access
    .write
    .format("delta")
    .mode("append")
    .save("abfss://compliance@mystorageaccount.dfs.core.windows.net/audit_export/")
)
```

#### SQL Example

```sql
-- All access events in the last 7 days across the workspace
SELECT
    event_time,
    user_identity.email       AS user_email,
    action_name,
    request_params,
    response.status_code      AS status_code
FROM system.access.audit
WHERE action_name IN ('commandSubmit', 'runCommand', 'read', 'select')
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY event_time DESC
LIMIT 500;

-- All access events referencing the sensitive satellite table in the last 30 days
SELECT
    event_time,
    user_identity.email AS user_email,
    action_name,
    request_params
FROM system.access.audit
WHERE
    lower(to_json(request_params)) LIKE '%sat_customer_details%'
    AND event_time >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY event_time DESC;

-- Weekly summary of access counts by user and action type
SELECT
    date_trunc('week', event_time) AS week_start,
    user_identity.email            AS user_email,
    action_name,
    COUNT(*)                       AS event_count
FROM system.access.audit
WHERE event_time >= current_timestamp() - INTERVAL 90 DAYS
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;

-- GRANT and REVOKE events — identify privilege changes
SELECT
    event_time,
    user_identity.email AS user_email,
    action_name,
    request_params
FROM system.access.audit
WHERE action_name IN ('grantPrivilege', 'revokePrivilege')
  AND event_time >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### Discussion and Concerns

- **Access requirement:** `system.access.audit` requires Unity Catalog. The querying user must have either Metastore Admin rights or be explicitly granted access to the `system` catalog via `GRANT USE CATALOG ON CATALOG system TO <user>`. Verify access with `SELECT COUNT(*) FROM system.access.audit` — a permission denied error indicates the grant is missing.
- **Audit log retention:** Databricks retains audit log data in system tables for a defined period (typically 365 days, but this can vary by workspace configuration). For compliance requirements with longer retention windows (e.g., 7 years for financial records), export audit log data to long-term storage regularly. The Python example above shows an export to ADLS.
- **dbt query history:** dbt jobs run as the configured service principal identity. All queries run by dbt are visible in the Databricks SQL query history filtered by the service principal's email address. This provides model-level query visibility in addition to the system table audit log.
- **`action_name` values vary by Databricks version:** The exact `action_name` strings in `system.access.audit` depend on the Databricks runtime and feature version. Use `SELECT DISTINCT action_name FROM system.access.audit LIMIT 100` to explore the action names present in your workspace before building compliance queries.

### See Also

- [Unity Catalog system tables — Databricks](https://docs.databricks.com/en/administration-guide/system-tables/index.html)
- [Audit log reference — Databricks](https://docs.databricks.com/en/administration-guide/account-settings/audit-logs.html)
- [system.access.audit — Databricks](https://docs.databricks.com/en/administration-guide/system-tables/audit.html)

---

## Data Encryption

Databricks workspaces encrypt data at rest using Databricks-managed keys by default. Regulatory frameworks (GDPR Article 32, HIPAA technical safeguards, FedRAMP) may require customer-managed keys (CMK), where the organisation controls the encryption key lifecycle — including rotation and revocation.

### Problem

A compliance audit requires evidence that data at rest on Databricks cluster nodes and DBFS is encrypted with a key the organisation owns and controls. Databricks-managed default encryption does not satisfy this requirement because the organisation cannot demonstrate key custody or perform key revocation.

### Solution

Configure customer-managed keys for the Databricks workspace. This is an infrastructure-level configuration — it is not performed in notebooks or SQL. The steps below describe the Azure Databricks implementation using Azure Key Vault and Terraform.

#### Python Example

```python
# There is no runtime Python code for configuring CMK.
# CMK is an infrastructure configuration applied at workspace provisioning time
# or updated post-provisioning via the Azure Portal or Terraform.

# To verify that CMK is configured on the workspace, use the Azure CLI:
# az databricks workspace show \
#     --name my-databricks-workspace \
#     --resource-group my-resource-group \
#     --query "parameters.encryption"

# To verify encryption is active from within a notebook, inspect workspace properties.
# The following returns workspace metadata visible to the current user:
workspace_info = spark.conf.get("spark.databricks.workspaceUrl")
print(f"Workspace: {workspace_info}")

# Encryption status is not visible from within Spark — verify via Azure CLI or portal.
# The check below confirms managed disk encryption is configured:
# az disk show --name <managed-disk-name> \
#     --resource-group <resource-group> \
#     --query "encryption"
```

```hcl
# Terraform configuration for Azure Databricks workspace with CMK
# (illustrative — adjust to your Azure environment)

resource "azurerm_key_vault" "databricks_cmk" {
  name                = "my-databricks-cmk-kv"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "premium"  # Premium required for HSM-backed keys
}

resource "azurerm_key_vault_key" "databricks_key" {
  name         = "databricks-managed-disk-key"
  key_vault_id = azurerm_key_vault.databricks_cmk.id
  key_type     = "RSA"
  key_size     = 2048
  key_opts     = ["decrypt", "encrypt", "sign", "unwrapKey", "verify", "wrapKey"]
}

resource "azurerm_databricks_workspace" "main" {
  name                        = "my-databricks-workspace"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"

  managed_disk_cmk_key_vault_key_id              = azurerm_key_vault_key.databricks_key.id
  managed_disk_cmk_rotation_to_latest_version_enabled = true
}
```

#### SQL Example

```sql
-- There is no SQL equivalent for configuring customer-managed keys.
-- CMK is an infrastructure configuration. After CMK is configured,
-- all existing SQL operations continue to work transparently —
-- encryption and decryption are handled by the storage layer, invisible to queries.

-- Verify the current workspace encryption configuration by inspecting system properties:
-- (This is informational — not a security control itself)
SELECT *
FROM system.information_schema.information_schema_catalog_name;
```

### Discussion and Concerns

- **Scope of CMK:** On Azure Databricks, CMK applies to managed disks (cluster node local storage) and the DBFS root storage account. It does **not** automatically encrypt data in external ADLS storage accounts (e.g., where Delta tables are stored in your own storage account). Configure ADLS encryption with CMK separately in the Azure Portal or via the storage account Terraform resource (`azurerm_storage_account` with `customer_managed_key` block).
- **CMK rotation:** Key rotation can be performed in Azure Key Vault without service interruption if `managed_disk_cmk_rotation_to_latest_version_enabled = true` is set. Without auto-rotation, manual key rotation requires planned workspace maintenance. Establish a key rotation schedule and test rotation in a non-production workspace before applying it to production.
- **Key revocation risk:** If the CMK is revoked (e.g., the Key Vault is deleted or the key is disabled), the Databricks workspace will be unable to decrypt managed disks and will become unavailable. CMK revocation is a data destruction event — use it deliberately. Ensure that key access policies include the Databricks managed identity with `get`, `wrapKey`, and `unwrapKey` permissions.
- **Verification:** After configuring CMK, verify with the Azure CLI: `az databricks workspace show --name <workspace> --resource-group <rg> --query "parameters.encryption"`. The response should include the Key Vault key URI. This provides the evidence artefact required for compliance audit.

### See Also

- [Customer-managed keys for Azure Databricks — Microsoft](https://learn.microsoft.com/en-us/azure/databricks/security/keys/customer-managed-keys-managed-disks-azure)
- [Databricks encryption overview — Databricks](https://docs.databricks.com/en/security/encryption/index.html)
- [Azure Key Vault managed HSM — Microsoft](https://learn.microsoft.com/en-us/azure/key-vault/managed-hsm/overview)

---

## dbt Column Classification with schema.yml

PII and sensitive columns are often undiscovered until a data breach or a compliance audit reveals that a column named `customer_email` in a mart table was accessible to all analysts without masking. Without a systematic approach to identifying which columns contain sensitive data, governance controls cannot be applied consistently.

### Problem

The data engineering team needs to identify all PII columns across the dbt project to determine which columns require masking or access restriction. There is no existing inventory of sensitive columns, and the PII surface area is unknown.

### Solution

Tag sensitive columns in dbt `schema.yml` using `meta` properties. This creates a machine-readable, version-controlled inventory of column sensitivity classifications. Use `dbt docs generate` to publish the catalog and make the tags visible. On `dbt-databricks` 1.6+, these tags propagate automatically to Unity Catalog column tags, enabling Unity Catalog governance tooling to consume them.

#### Python Example

```python
# Generate and serve the dbt documentation catalog
# Run from the terminal in the dbt project directory:
# dbt docs generate
# dbt docs serve  (opens a browser with the lineage graph and column documentation)

# After dbt docs generate, the catalog.json contains all column metadata including tags.
# Parse it programmatically to extract the PII column inventory:

import json

with open("target/catalog.json") as f:
    catalog = json.load(f)

pii_columns = []
for node_id, node in catalog.get("nodes", {}).items():
    for col_name, col_info in node.get("columns", {}).items():
        meta = col_info.get("meta", {})
        if meta.get("sensitivity") == "PII":
            pii_columns.append({
                "node": node_id,
                "column": col_name,
                "pii_type": meta.get("pii_type", "unknown"),
                "description": col_info.get("description", "")
            })

import pandas as pd
pii_df = pd.DataFrame(pii_columns)
print(pii_df.to_string(index=False))
```

#### SQL Example

```yaml
# models/raw_vault/schema.yml
# Column-level PII classification using dbt meta tags

version: 2

models:
  - name: sat_customer_details
    description: >
      Customer PII satellite. Contains all personal descriptive attributes for the
      customer entity. Access is restricted to the dbt service principal and authorised
      data engineers. Analysts access masked versions via the marts layer.
    columns:
      - name: customer_hk
        description: SHA-256 hash key derived from customer_id. Not sensitive.
        tests:
          - not_null
          - unique

      - name: load_date
        description: Timestamp of when this record was loaded into the vault.
        tests:
          - not_null

      - name: record_source
        description: Source system identifier for data lineage.
        tests:
          - not_null

      - name: hashdiff
        description: SHA-256 hash of all descriptive attributes for change detection.
        tests:
          - not_null

      - name: full_name
        description: Customer full name as received from the source system.
        meta:
          sensitivity: PII
          pii_type: name
          masking_required: true
          data_owner: customer-data-steward@example.com

      - name: email
        description: Customer email address.
        meta:
          sensitivity: PII
          pii_type: email
          masking_required: true
          masking_function: main.security.mask_email
          data_owner: customer-data-steward@example.com
        tests:
          - not_null

      - name: phone
        description: Customer phone number in E.164 format.
        meta:
          sensitivity: PII
          pii_type: phone
          masking_required: true
          masking_function: main.security.mask_phone
          data_owner: customer-data-steward@example.com

      - name: region
        description: Customer geographic region. Not sensitive — used for RLS filtering.
        tests:
          - not_null
          - accepted_values:
              values: ['EMEA', 'APAC', 'AMER', 'LATAM']

      - name: account_status
        description: Current account status. Not sensitive.
        tests:
          - accepted_values:
              values: ['active', 'suspended', 'closed']
```

```yaml
# dbt_project.yml — project-level configuration for Unity Catalog tag propagation
# (requires dbt-databricks 1.6+)

name: my_data_vault_project
version: '1.0.0'
config-version: 2

models:
  my_data_vault_project:
    raw_vault:
      +persist_docs:
        relation: true
        columns: true  # Propagates column descriptions to Unity Catalog
```

### Discussion and Concerns

- **Meta tags are documentation only — they do not enforce anything at runtime:** A column tagged `sensitivity: PII` in `schema.yml` is not automatically masked or access-restricted. The tag is a signal that a masking control should be applied, but the control itself must be created separately (via a Unity Catalog column mask function or a dynamic view). Treat the schema.yml tags as the governance requirement and the mask function as the enforcement.
- **Unity Catalog tag propagation (dbt-databricks 1.6+):** When `persist_docs` is enabled and `dbt-databricks` version 1.6 or later is used, dbt propagates column descriptions to Unity Catalog. In conjunction with Unity Catalog's column tagging feature, the `meta` values can also be propagated as Unity Catalog tags using a custom post-hook or the `tags` key in `schema.yml`. This creates an auditable lineage from the dbt model definition to the Unity Catalog governance UI.
- **`masking_function` meta key is a convention, not a built-in feature:** The `masking_function` key shown in the example is a custom convention — dbt does not natively read this key and apply masks. It serves as documentation that links the column to the function name it should be masked with. A post-hook or a separate script could read this value and execute the `ALTER TABLE ... SET MASK` command.
- **`dbt test` validates reference data in seeds:** For seed files that contain reference data used in joins (e.g., a list of valid region codes), column-level tests (`not_null`, `unique`, `accepted_values`) can be declared in `schema.yml` alongside the column meta tags. This ensures the reference data itself is tested as part of the dbt pipeline, not just the models that consume it.

### See Also

- [dbt schema.yml reference — dbt docs](https://docs.getdbt.com/reference/model-properties)
- [dbt meta config — dbt docs](https://docs.getdbt.com/reference/resource-properties/meta)
- [dbt-databricks Unity Catalog column tags — dbt docs](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
- [Unity Catalog column tags — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html)

---

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Unusual data access patterns | `system.access.audit` filtered by action type and time | Access by unexpected users, access outside business hours, high-frequency SELECT on PII satellites |
| GRANT/REVOKE changes | `system.access.audit` filtered by `action_name IN ('grantPrivilege', 'revokePrivilege')` | Unexpected privilege escalations, grants to individual users (rather than groups), grants on raw vault schemas |
| Schema-level grant audit | `SHOW GRANTS ON SCHEMA main.raw_vault` (and each sensitive schema) | Any group or user that is not in the expected list; analysts appearing in raw vault grants |
| dbt service principal privilege review | `SHOW GRANTS ON SCHEMA main.<each_schema>` filtered to the SP identity | SP grants that exceed the minimum privilege set — especially `MODIFY`, `ALL PRIVILEGES`, or cross-catalog grants |
| Secret scope access | `databricks secrets list-acls my-scope` (Databricks CLI) | ACLs that include unexpected users or groups; READ grants to service accounts that no longer exist |
| Unity Catalog tag coverage | `SELECT * FROM main.information_schema.column_masks` and cross-reference with dbt catalog PII column inventory | PII columns without a mask applied; mask functions applied to non-PII columns |

### Metrics for Success

- [ ] All PII columns identified in dbt `schema.yml` have a corresponding Unity Catalog column mask applied (`main.information_schema.column_masks` count matches the PII column inventory)
- [ ] dbt service principal has no Metastore Admin, Catalog Owner, or `ALL PRIVILEGES` grants; `SHOW GRANTS` returns only the minimum privilege set documented in `security_patterns.md`
- [ ] Audit log retention meets the compliance requirement (e.g., 90 days in `system.access.audit`; export to long-term storage verified for periods beyond the system table retention window)
- [ ] All production secrets are stored in Databricks Secret Scopes — zero occurrences of hardcoded credentials in notebooks, job configurations, or version control (verify with a codebase scan for connection string patterns)
- [ ] `SHOW GRANTS ON TABLE main.marts.dim_customer` (and all other consumer-facing mart tables) returns only the expected analyst and BI service account groups — no engineers, no service principals beyond the dbt SP
- [ ] Dynamic views and column mask functions are documented in version control and can be recreated from source — no orphaned view objects in the marts schema without a corresponding creation script
