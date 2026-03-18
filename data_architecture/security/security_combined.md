# Security Guide

## Databricks

> **Scope note:** This guide covers security implementation using **native Databricks and Unity Catalog features only**. It is self-contained — no companion document is required.

---

## Introduction

This guide provides architectural decision guidance and practical, step-by-step implementation for **data security and governance** on Databricks with Unity Catalog using only native platform features. It covers role-based access control, secrets management, row-level security, column masking, Unity Catalog tag management, audit logging, encryption configuration, and service principal management. No prior knowledge of Unity Catalog governance is assumed.

Each method section follows a consistent structure: the problem being solved, the recommended solution with Python and SQL examples, any known concerns or trade-offs, and links to further reading.

---

## Design Decisions

Use this section to select the right security approach before implementing.

### Unity Catalog Native Features vs. Dynamic Views

| | Unity Catalog Native Row Filters / Column Masks | Dynamic Views |
|---|---|---|
| **Use when** | UC Premium available; DBR 12.2+; masking logic fits a SQL function expression; same masking logic reused across many tables | UC Premium not available; masking logic requires multi-table subqueries or complex multi-column interaction; small number of affected tables; BI tools that do not honour UC row filter metadata |
| **How it works** | Applied transparently to the underlying table — consumers query the table directly and receive masked results | Consumers are directed to a `v_`-prefixed view; masking is embedded in the view definition |
| **Key risk** | None for consumers — underlying table access is still controlled by grants | If the underlying table `SELECT` grant is not revoked from consumer roles, masking is bypassed entirely |

### Role Reference

| Role | Privileges | Scope |
|------|-----------|-------|
| **Metastore Admin** | Full control: create/delete catalogs, manage external locations, assign ownership | Databricks account level |
| **Catalog Owner** | Manages all schemas and objects within a catalog; can grant/revoke | Catalog level |
| **Data Steward** | Manages schema-level grants; can grant `SELECT` to consumers; cannot create/drop schemas | Schema level |
| **Pipeline Service Principal** | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `SELECT` on required schemas; no admin rights | Catalog + schema level (scoped) |
| **Data Engineer** | `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY` on raw/vault schemas | Catalog + schema level |
| **Data Consumer / Analyst** | `USE CATALOG`, `USE SCHEMA`, `SELECT` on Gold/marts schema only | Gold schema only |
| **BI Service Account** | Same as Data Consumer, scoped to specific tables or views | Specific tables or views |

Always grant to groups, not individual users.

### Minimum Privilege Set for Pipeline Service Principal

| Schema | Required Privilege | Purpose |
|--------|-------------------|---------|
| Catalog (`main`) | `USE CATALOG` | Navigate the catalog |
| `bronze` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` | Materialise raw ingested tables; read for downstream steps |
| `silver` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` on `bronze` | Materialise cleansed and conformed tables |
| `gold` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` on `silver` | Materialise aggregated, business-ready tables |
| `security` (if present) | `USE SCHEMA`, `SELECT` | Read user-region mapping tables used in RLS |

The service principal must never have Metastore Admin, Catalog Owner, or `DROP` rights on tables it did not create.

### Security Enforcement by Medallion Layer

| Layer | Access Control Approach |
|-------|------------------------|
| **Bronze** | Restrict by identity — ingestion SPs and data engineers only. **No RLS.** Column masking on known PII only if policy requires it. Applying RLS to append-only Bronze tables causes full table scans with no partition pruning benefit. |
| **Silver** | Restrict by identity — data engineers and transformation SPs. No consumer access. No RLS. |
| **Gold** | Apply RLS and column masking here. Grant `SELECT` to analyst groups and BI service accounts. This is the primary enforcement point for all data access controls. |
| **Dynamic views (Gold)** | Prefix with `v_`. Grant `SELECT` on the view only; revoke direct table `SELECT` from consumer roles or masking is bypassed. |

---

## Development Environment Pre-Requisites

### Installing Your Development Environment

> **On Databricks (interactive notebooks or jobs):** PySpark and Unity Catalog SQL are available in every Databricks Runtime. SQL examples in this cookbook can be run directly in a notebook or SQL Warehouse — no local installation needed.
>
> **Local development:** The tools below are installed on your local machine for CLI operations and infrastructure management.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.9+ | Local dev | Required for the Databricks CLI |
| Databricks CLI | 0.200+ | Local dev | Required for secrets management and service principal examples |
| Terraform (optional) | 1.5+ | Local dev | Required for infrastructure-as-code grant management examples |

Configure your Databricks connection (local machine):

```bash
# Authenticate the Databricks CLI
databricks configure --token

# Verify connection
databricks clusters list
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

Use Unity Catalog `GRANT` statements scoped to Databricks groups. The following example sets up a complete privilege structure for a pipeline with three roles: a pipeline service principal (`pipeline_service_principal`), a data engineering group (`data_engineers`), and an analyst group (`analysts`).

#### Python Example

```python
# Apply the full privilege set for a Databricks pipeline using spark.sql()
# Run these in a Databricks notebook or as part of a setup script

# --- Catalog-level: all roles need USE CATALOG to navigate the catalog ---
spark.sql("GRANT USE CATALOG ON CATALOG main TO `data_engineers`")
spark.sql("GRANT USE CATALOG ON CATALOG main TO `analysts`")
spark.sql("GRANT USE CATALOG ON CATALOG main TO `pipeline_service_principal`")

# --- raw_vault schema: engineers and SP only ---
spark.sql("GRANT USE SCHEMA ON SCHEMA main.raw_vault TO `data_engineers`")
spark.sql("GRANT SELECT ON SCHEMA main.raw_vault TO `data_engineers`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.raw_vault TO `pipeline_service_principal`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.raw_vault TO `pipeline_service_principal`")
spark.sql("GRANT SELECT ON SCHEMA main.raw_vault TO `pipeline_service_principal`")

# --- business_vault schema: SP needs CREATE and SELECT; engineers can read ---
spark.sql("GRANT USE SCHEMA ON SCHEMA main.business_vault TO `data_engineers`")
spark.sql("GRANT SELECT ON SCHEMA main.business_vault TO `data_engineers`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.business_vault TO `pipeline_service_principal`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.business_vault TO `pipeline_service_principal`")
spark.sql("GRANT SELECT ON SCHEMA main.business_vault TO `pipeline_service_principal`")

# --- marts schema: analysts get SELECT; SP materialises the mart tables ---
spark.sql("GRANT USE SCHEMA ON SCHEMA main.marts TO `analysts`")
spark.sql("GRANT SELECT ON TABLE main.marts.dim_customer TO `analysts`")
spark.sql("GRANT SELECT ON TABLE main.marts.dim_product TO `analysts`")
spark.sql("GRANT SELECT ON TABLE main.marts.fact_orders TO `analysts`")
spark.sql("GRANT USE SCHEMA ON SCHEMA main.marts TO `pipeline_service_principal`")
spark.sql("GRANT CREATE TABLE ON SCHEMA main.marts TO `pipeline_service_principal`")
spark.sql("GRANT SELECT ON SCHEMA main.marts TO `pipeline_service_principal`")

# --- Verify the grants on a sensitive table ---
spark.sql("SHOW GRANTS ON TABLE main.marts.dim_customer").show(truncate=False)
```

#### SQL Example

```sql
-- Catalog-level navigation grant — required for all roles before any schema or table access
GRANT USE CATALOG ON CATALOG main TO `data_engineers`;
GRANT USE CATALOG ON CATALOG main TO `analysts`;
GRANT USE CATALOG ON CATALOG main TO `pipeline_service_principal`;

-- raw_vault: data engineers read-only; pipeline SP reads and materialises
GRANT USE SCHEMA, SELECT ON SCHEMA main.raw_vault TO `data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA main.raw_vault TO `pipeline_service_principal`;

-- business_vault: same pattern — engineers read, SP creates and reads
GRANT USE SCHEMA, SELECT ON SCHEMA main.business_vault TO `data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA main.business_vault TO `pipeline_service_principal`;

-- marts: analysts get SELECT on specific consumer-facing tables only
GRANT USE SCHEMA ON SCHEMA main.marts TO `analysts`;
GRANT SELECT ON TABLE main.marts.dim_customer TO `analysts`;
GRANT SELECT ON TABLE main.marts.dim_product TO `analysts`;
GRANT SELECT ON TABLE main.marts.fact_orders TO `analysts`;

-- marts: pipeline SP can materialise and read all mart objects
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA main.marts TO `pipeline_service_principal`;

-- Audit current grants on a table
SHOW GRANTS ON TABLE main.marts.dim_customer;

-- Revoke a grant if a role changes
REVOKE SELECT ON TABLE main.marts.dim_customer FROM `analysts`;
```

### Discussion and Concerns

- **Groups, not users:** Always grant to groups managed in the Databricks account console. When a user joins or leaves a team, add or remove them from the group — do not re-run GRANT/REVOKE statements for every individual.
- **Schema grants do not cascade to existing tables:** `GRANT SELECT ON SCHEMA` gives the grantee the right to access future tables created in that schema, but does not automatically grant `SELECT` on tables that already exist. For existing tables, you must also run `GRANT SELECT ON TABLE` or use `GRANT SELECT ON ALL TABLES IN SCHEMA`. Use `GRANT SELECT ON ALL TABLES IN SCHEMA main.marts TO \`analysts\`` if you intend to expose all tables in a schema.
- **AAD/Okta group sync:** When groups are synced from an external identity provider, group membership is managed in AAD or Okta. Changes propagate to Databricks on the next sync cycle (typically minutes to one hour). Test group membership with `SELECT IS_MEMBER('analysts')` in a notebook to confirm sync.
- **Maintaining grants after table recreation:** When a pipeline drops and recreates a table (e.g., a full-refresh pattern using `CREATE OR REPLACE TABLE`), the resulting table is a new object and previously applied `GRANT` statements on that specific table object are lost. To handle this durably, use schema-level grants (`GRANT SELECT ON SCHEMA`) for consumer groups where appropriate, or include `GRANT` statements in the pipeline notebook/job after each table recreation step. See the **Post-Pipeline Grant Re-application** section below for patterns.
- **Audit grants regularly:** Run `SHOW GRANTS ON SCHEMA main.raw_vault` weekly for sensitive schemas and compare against the expected role list. Unexpected grants are a security finding.

### Post-Pipeline Grant Re-application

Native Databricks pipelines require explicit grant re-application when tables are recreated. Use one of the following patterns:

#### Pattern 1: Schema-Level Grants (Simplest)

```sql
-- Grant at schema level so all current and future tables inherit access
-- This is the simplest approach when you want to expose an entire schema layer
GRANT SELECT ON ALL TABLES IN SCHEMA main.marts TO `analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA main.marts TO `bi_service_account`;

-- For new tables created after this grant, schema-level grants apply automatically
-- (for Unity Catalog managed schemas on DBR 12.0+)
```

#### Pattern 2: Notebook Post-Step Grant Block (Targeted)

```python
# Include this block at the end of any notebook that recreates tables
# Ensures grants are re-applied atomically after each pipeline run

tables_to_grant = [
    "main.marts.dim_customer",
    "main.marts.dim_product",
    "main.marts.fact_orders",
]

groups_to_grant = ["analysts", "bi_service_account"]

for table in tables_to_grant:
    for group in groups_to_grant:
        spark.sql(f"GRANT SELECT ON TABLE {table} TO `{group}`")
        print(f"Granted SELECT on {table} to {group}")
```

#### Pattern 3: Databricks Workflow Task (Post-Job Hook)

```python
# In a Databricks Workflow, add a dedicated "Grant" task that runs after
# any task that recreates tables. This task runs the grant SQL statements:

grant_statements = [
    "GRANT SELECT ON TABLE main.marts.dim_customer TO `analysts`",
    "GRANT SELECT ON TABLE main.marts.dim_product TO `analysts`",
    "GRANT SELECT ON TABLE main.marts.fact_orders TO `analysts`",
    "GRANT SELECT ON TABLE main.marts.dim_customer TO `bi_service_account`",
]

for stmt in grant_statements:
    spark.sql(stmt)
    print(f"Executed: {stmt}")

# Verify
spark.sql("SHOW GRANTS ON TABLE main.marts.dim_customer").show(truncate=False)
```

### See Also

- [Unity Catalog privileges — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html)
- [Sync identities from AAD — Databricks](https://docs.databricks.com/en/administration-guide/users-groups/scim/index.html)
- `security_patterns.md` — role definitions and privilege scope decisions

---

## Secrets Management

Notebooks and job scripts frequently contain hardcoded credentials — storage account keys, API tokens, database connection strings — that are visible in version control, job run logs, and notebook revision history. This is a critical security risk: any user with access to the notebook or the job run output can retrieve the credential.

### Problem

Credentials stored directly in notebooks or job configurations are exposed in version control, notebook revision history, and job run logs. A single committed storage key can grant unrestricted access to an ADLS storage account containing all raw and production data.

### Solution

Use Databricks Secrets to store credentials outside of code. Secrets are stored in a secret scope, retrieved at runtime using `dbutils.secrets.get()`, and automatically redacted in notebook output so they cannot appear in logs.

> **Recommended: Azure Key Vault-backed secret scopes.** On Azure Databricks, create secret scopes backed by Azure Key Vault. Credentials are managed centrally in Key Vault, governed by Key Vault access policies, and consumed transparently via `dbutils.secrets.get()` with no code changes. Rotation and access auditing are handled in Key Vault without touching notebook code or Databricks ACLs. Create an AKV-backed scope via **Settings → Developer → Manage secret scopes → Create** — provide your Key Vault DNS name and resource ID. See [Azure Key Vault-backed secret scopes — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes).
>
> Databricks-managed scopes (created via the CLI) are appropriate for non-Azure environments or when Key Vault is not available.

The Databricks CLI is used to create Databricks-managed scopes and store secrets — these operations require Databricks workspace admin rights or the `Create Secret Scope` permission.

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
- **Secret scope permissions:** By default, the creator of a secret scope has full control. Grant read access to other users, groups, or service principals using `databricks secrets put-acl my-scope <principal-name> READ`. Without an explicit ACL grant, other principals cannot read secrets from the scope even if they know the scope and key names. **Service principals running jobs require an explicit grant:** a job running under a service principal fails with a permission denied error at runtime, not at deployment time — the missing grant is not visible during job configuration or test runs with interactive credentials.
- **Azure Key Vault-backed scopes:** On Azure Databricks, secret scopes can be backed by Azure Key Vault, allowing secrets to be managed centrally in Key Vault and consumed transparently via `dbutils.secrets.get()`. This is the recommended approach for enterprise environments where Key Vault is the standard credential store.

### See Also

- [Databricks Secrets — Databricks](https://docs.databricks.com/en/security/secrets/index.html)
- [Azure Key Vault-backed secret scope — Databricks](https://docs.databricks.com/en/security/secrets/secret-scopes.html#azure-key-vault-backed-scopes)
- [Managed identities for Azure Databricks — Microsoft](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/azure-managed-identities)

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
- **View recreation on logic change:** Any change to the RLS logic requires dropping and recreating the view. This removes the view object and requires re-granting `SELECT` on the new view object. Script both the `CREATE VIEW` and `GRANT` statements together in version control so they can be rerun atomically.

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
# (engineers and the pipeline SP retain their schema-level grants)
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

Each dynamic view created for column masking is a schema object that must be created, granted, and maintained independently. In pipelines with many tables containing PII columns, this proliferates view objects and distributes masking logic across many view definitions — making it difficult to audit what the current masking policy is and to update it centrally.

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

## Unity Catalog Tag Management

PII and sensitive columns are often undiscovered until a data breach or a compliance audit reveals that a column named `customer_email` in a mart table was accessible to all analysts without masking. Without a systematic approach to identifying which columns contain sensitive data, governance controls cannot be applied consistently.

### Problem

The data engineering team needs to identify all PII columns across the data platform to determine which columns require masking or access restriction. There is no existing inventory of sensitive columns, and the PII surface area is unknown. The classification must be machine-readable and queryable via standard Unity Catalog system tables.

### Solution

Apply tags to tables and columns using native Unity Catalog `ALTER TABLE SET TAGS` and `ALTER TABLE ALTER COLUMN SET TAGS` statements. Tags are stored in Unity Catalog and are queryable via `system.information_schema.table_tags` and `system.information_schema.column_tags`. This creates a governed, version-controlled inventory of sensitivity classifications that can drive downstream masking and access control decisions.

#### Python Example

```python
# Apply table-level tags to classify a PII satellite
spark.sql("""
    ALTER TABLE main.raw_vault.sat_customer_details
    SET TAGS ('pii' = 'true', 'data_owner' = 'customer-data-steward@example.com', 'classification' = 'confidential')
""")

# Apply column-level tags to identify specific PII columns
spark.sql("""
    ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN email
    SET TAGS ('pii_type' = 'email', 'classification' = 'sensitive', 'masking_required' = 'true')
""")

spark.sql("""
    ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN phone
    SET TAGS ('pii_type' = 'phone', 'classification' = 'sensitive', 'masking_required' = 'true')
""")

spark.sql("""
    ALTER TABLE main.raw_vault.sat_customer_details
    ALTER COLUMN full_name
    SET TAGS ('pii_type' = 'name', 'classification' = 'sensitive', 'masking_required' = 'true')
""")

# Query all PII-tagged tables across the catalog
pii_tables = spark.sql("""
    SELECT table_catalog, table_schema, table_name, tag_name, tag_value
    FROM system.information_schema.table_tags
    WHERE tag_name = 'pii' AND tag_value = 'true'
    ORDER BY table_schema, table_name
""")
pii_tables.show(truncate=False)

# Query all sensitive columns — use this as the PII column inventory
pii_columns = spark.sql("""
    SELECT table_catalog, table_schema, table_name, column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE tag_name = 'classification' AND tag_value = 'sensitive'
    ORDER BY table_schema, table_name, column_name
""")
pii_columns.show(truncate=False)

# Cross-reference: find sensitive columns that do NOT yet have a column mask applied
unmasked_pii = spark.sql("""
    SELECT
        ct.table_catalog,
        ct.table_schema,
        ct.table_name,
        ct.column_name,
        ct.tag_value AS classification,
        cm.mask_name
    FROM system.information_schema.column_tags ct
    LEFT JOIN main.information_schema.column_masks cm
        ON ct.table_catalog = cm.table_catalog
        AND ct.table_schema  = cm.table_schema
        AND ct.table_name    = cm.table_name
        AND ct.column_name   = cm.column_name
    WHERE ct.tag_name = 'masking_required'
      AND ct.tag_value = 'true'
      AND cm.mask_name IS NULL
    ORDER BY ct.table_schema, ct.table_name, ct.column_name
""")
unmasked_pii.show(truncate=False)
```

#### SQL Example

```sql
-- Apply tags to a table
ALTER TABLE main.silver.customers
SET TAGS ('pii' = 'true', 'data_owner' = 'customer_team');

-- Apply tags to a column
ALTER TABLE main.silver.customers
ALTER COLUMN email
SET TAGS ('pii_type' = 'email', 'classification' = 'sensitive');

ALTER TABLE main.silver.customers
ALTER COLUMN phone
SET TAGS ('pii_type' = 'phone', 'classification' = 'sensitive');

-- Apply table-level tags to a PII satellite
ALTER TABLE main.raw_vault.sat_customer_details
SET TAGS (
    'pii'           = 'true',
    'data_owner'    = 'customer-data-steward@example.com',
    'classification'= 'confidential'
);

-- Apply column-level tags for detailed PII classification
ALTER TABLE main.raw_vault.sat_customer_details
ALTER COLUMN email
SET TAGS (
    'pii_type'          = 'email',
    'classification'    = 'sensitive',
    'masking_required'  = 'true'
);

ALTER TABLE main.raw_vault.sat_customer_details
ALTER COLUMN phone
SET TAGS (
    'pii_type'          = 'phone',
    'classification'    = 'sensitive',
    'masking_required'  = 'true'
);

-- Query system catalog for all PII-tagged tables
SELECT table_name, tag_name, tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'pii' AND tag_value = 'true';

-- Query system catalog for all sensitive columns
SELECT table_catalog, table_schema, table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE tag_name = 'classification' AND tag_value = 'sensitive'
ORDER BY table_schema, table_name, column_name;

-- Find columns tagged masking_required=true that have no column mask applied yet
-- Use this as a governance gap report
SELECT
    ct.table_catalog,
    ct.table_schema,
    ct.table_name,
    ct.column_name,
    ct.tag_value     AS classification,
    cm.mask_name
FROM system.information_schema.column_tags ct
LEFT JOIN main.information_schema.column_masks cm
    ON  ct.table_catalog = cm.table_catalog
    AND ct.table_schema  = cm.table_schema
    AND ct.table_name    = cm.table_name
    AND ct.column_name   = cm.column_name
WHERE ct.tag_name   = 'masking_required'
  AND ct.tag_value  = 'true'
  AND cm.mask_name  IS NULL
ORDER BY ct.table_schema, ct.table_name, ct.column_name;

-- Remove a tag from a column (when classification changes)
ALTER TABLE main.raw_vault.sat_customer_details
ALTER COLUMN email
UNSET TAGS ('pii_type');
```

### Discussion and Concerns

- **Tags are metadata only — they do not enforce anything at runtime:** A column tagged `classification: sensitive` is not automatically masked or access-restricted. The tag is a signal that a masking control should be applied. Treat the tags as the governance requirement and the mask function (`ALTER TABLE ... SET MASK`) as the enforcement. The gap report query above is the tool for identifying the delta between stated requirements and applied controls.
- **Tag values are strings:** All tag values in Unity Catalog are strings. Use consistent values across the team — define a controlled vocabulary (e.g., `'true'`/`'false'` for boolean flags, `'email'`/`'phone'`/`'name'` for PII types) and document it. Inconsistent capitalisation or spelling will cause the gap report to miss entries.
- **Tags are queryable via system catalog:** Unity Catalog tags are live metadata queryable by any user with access to `system.information_schema`. This makes them suitable as the authoritative, real-time source for compliance reporting.
- **Tags survive table recreation:** Unity Catalog column tags are stored at the column metadata level and persist across `INSERT OVERWRITE` and `MERGE` operations. However, `DROP TABLE` and `CREATE TABLE` removes all tags. When a pipeline recreates a table, tag re-application must be included in the pipeline steps alongside grant re-application.
- **Terraform for tag management at scale:** For catalogs with many tables, manage tags via Terraform using the `databricks_table` or `databricks_grants` resources, or via the Databricks REST API (`PATCH /api/2.1/unity-catalog/tables/{full_name}`). This keeps tag definitions in version control alongside infrastructure code.

### See Also

- [Unity Catalog tags — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html)
- [ALTER TABLE SET TAGS — Databricks SQL](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html)
- [system.information_schema.column_tags — Databricks](https://docs.databricks.com/en/sql/language-manual/information-schema/column_tags.html)
- `security_patterns.md` — governance enforcement layer decisions

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
- **Pipeline job query history:** Jobs run as the configured service principal identity. All queries run by pipeline jobs are visible in the Databricks SQL query history filtered by the service principal's email address. This provides job-level query visibility in addition to the system table audit log.
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

## Managing Your Environment

### Monitoring Your Environment in Production

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| Unusual data access patterns | `system.access.audit` filtered by action type and time | Access by unexpected users, access outside business hours, high-frequency SELECT on PII satellites |
| GRANT/REVOKE changes | `system.access.audit` filtered by `action_name IN ('grantPrivilege', 'revokePrivilege')` | Unexpected privilege escalations, grants to individual users (rather than groups), grants on raw vault schemas |
| Schema-level grant audit | `SHOW GRANTS ON SCHEMA main.raw_vault` (and each sensitive schema) | Any group or user that is not in the expected list; analysts appearing in raw vault grants |
| Pipeline service principal privilege review | `SHOW GRANTS ON SCHEMA main.<each_schema>` filtered to the SP identity | SP grants that exceed the minimum privilege set — especially `MODIFY`, `ALL PRIVILEGES`, or cross-catalog grants |
| Secret scope access | `databricks secrets list-acls my-scope` (Databricks CLI) | ACLs that include unexpected users or groups; READ grants to service accounts that no longer exist |
| Unity Catalog tag coverage | `SELECT * FROM system.information_schema.column_tags WHERE tag_name = 'masking_required' AND tag_value = 'true'` cross-referenced against `main.information_schema.column_masks` | PII columns without a mask applied; mask functions applied to non-PII columns |
| Tag gap report | Gap query in the Unity Catalog Tag Management section above | Columns tagged `masking_required=true` with no corresponding mask function applied |

### Metrics for Success

- [ ] All PII columns tagged `masking_required=true` in `system.information_schema.column_tags` have a corresponding Unity Catalog column mask applied (`main.information_schema.column_masks` count matches the tagged PII column count)
- [ ] Pipeline service principal has no Metastore Admin, Catalog Owner, or `ALL PRIVILEGES` grants; `SHOW GRANTS` returns only the minimum privilege set documented in `security_patterns.md`
- [ ] Audit log retention meets the compliance requirement (e.g., 90 days in `system.access.audit`; export to long-term storage verified for periods beyond the system table retention window)
- [ ] All production secrets are stored in Databricks Secret Scopes — zero occurrences of hardcoded credentials in notebooks, job configurations, or version control (verify with a codebase scan for connection string patterns)
- [ ] `SHOW GRANTS ON TABLE main.marts.dim_customer` (and all other consumer-facing mart tables) returns only the expected analyst and BI service account groups — no engineers, no service principals beyond the pipeline SP
- [ ] Dynamic views and column mask functions are documented in version control and can be recreated from source — no orphaned view objects in the marts schema without a corresponding creation script
- [ ] Unity Catalog tags are applied to all tables and columns in PII schemas — `system.information_schema.table_tags` and `column_tags` coverage verified against the data asset inventory
