# Security Architectural Patterns

## Databricks

> **Scope note:** This file covers security architectural patterns using **native Databricks and Unity Catalog features only**.

---

## Overview

This document describes the architectural patterns and design decisions that govern data security and governance on Databricks. It is a **decision and design reference** — not a step-by-step implementation guide. For implementation details, see `security_cookbook.md` in the same directory.

The patterns covered here are:

- **Unity Catalog Governance Model** — the hierarchy, role structure, and when to use native features versus dynamic views
- **Pipeline Service Principal Privilege Model** — the minimum privilege set for a service principal scoped to pipeline layers
- **Governance for Multi-Layer Architectures** — where security should be enforced in a Medallion pipeline

---

## Unity Catalog Governance Model

### Hierarchy Overview

Unity Catalog organises all data assets in a four-level hierarchy. Every securable object — table, view, function, volume — sits within this hierarchy, and permissions are inherited or explicitly granted at each level.

```
Metastore
└── Catalog
    └── Schema
        └── Table / View / Function / Volume
```

**Metastore** is the top-level governance boundary. There is one metastore per region per Databricks account. All catalogs, schemas, and tables within a workspace are registered in the metastore.

**Catalog** is the primary isolation boundary between environments (e.g., `dev`, `staging`, `prod`) or business domains. A team or project typically owns one catalog. Privileges on a catalog do not automatically cascade to objects within it — `USE CATALOG` grants the right to navigate the catalog, not to read its tables.

**Schema** is the unit of organisation within a catalog. Schemas typically map to pipeline layers (e.g., `raw_vault`, `business_vault`, `marts`) or to functional areas within a domain. `USE SCHEMA` is required for any operation within the schema. `CREATE TABLE` on a schema grants the right to materialise new objects — this should be restricted to service principals and pipeline accounts only.

**Table / View** is the most granular level at which `SELECT`, `INSERT`, `MODIFY`, and `ALL PRIVILEGES` can be granted. Row-level security and column masking are applied at this level.

### Role Definitions

The following roles represent a practical mapping for a production data platform. These are logical roles — in practice they map to Databricks groups, which can be synced from Azure Active Directory, Okta, or other identity providers.

| Role | Privileges | Scope |
|------|-----------|-------|
| **Metastore Admin** | Full control over the metastore, including creating and deleting catalogs, managing external locations, and assigning ownership | Databricks account level |
| **Catalog Owner** | Manages all schemas and objects within a specific catalog; can grant and revoke privileges on catalog-level objects | Catalog level |
| **Data Steward** | Manages schema-level grants for a specific schema; can grant `SELECT` to consumers; cannot create or drop schemas | Schema level |
| **Pipeline Service Principal** | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `SELECT` on required schemas; no admin rights | Catalog + schema level (scoped) |
| **Data Engineer** | `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY` on raw and vault schemas; no access to marts without explicit grant | Catalog + schema level |
| **Data Consumer / Analyst** | `USE CATALOG`, `USE SCHEMA`, `SELECT` on marts or Gold schema only; no access to raw or vault layers | Marts / Gold schema only |
| **BI Service Account** | Same as Data Consumer, scoped to specific tables or views exposed to BI tools | Specific tables or views |

Groups should be used rather than individual user grants. Granting to individual users does not scale, is difficult to audit, and breaks when users change roles or leave the organisation.

### When to Use Unity Catalog Native Features vs. Dynamic Views

Both approaches solve the same problems — row-level access control and column masking — but they differ significantly in how they are implemented, maintained, and audited.

**Use Unity Catalog native row filters and column masks when:**

- The Databricks workspace is running on Databricks Runtime 12.2 or later and Unity Catalog is configured.
- Unity Catalog Premium is available (native row filters and column masks are a Premium feature).
- The masking or filtering logic can be expressed as a SQL function — a `CASE WHEN IS_MEMBER(...) THEN ... ELSE ...` expression or equivalent.
- Multiple tables share the same masking requirement for the same column type (e.g., every `email` column across all satellites should be masked the same way). Native column mask functions are reusable — one function definition applied to many tables.
- The organisation wants a centralised, auditable registry of masking policies in the `system.information_schema` and Unity Catalog lineage graph.

**Use dynamic views when:**

- Unity Catalog Premium is not available, or the workspace is on an older runtime that does not support native masking.
- The masking or filtering logic is too complex for a single `RETURN` expression — for example, multi-table lookups, subqueries across security mapping tables, or logic that involves multiple columns interacting.
- The number of affected tables is small (one or two views), making a full native masking policy disproportionate overhead.
- The team needs to support consumers who query via tools that do not honour Unity Catalog row filter metadata correctly (some third-party BI connectors).

**Key trade-off:** Native masks are applied transparently to the underlying table — consumers query the table directly and receive masked results. Dynamic views require consumers to be directed to the view rather than the table. This means the underlying table must have its `SELECT` grant removed from consumer-facing roles, and only the view should be granted. If consumers can still reach the underlying table, the masking is bypassed. This access control discipline is easier to enforce with native masks.

### See Also

- [Unity Catalog privileges and securable objects — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html)
- [Unity Catalog row filters and column masks — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)
- [Unity Catalog best practices — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)
- `security_cookbook.md` — RBAC, RLS, column masking, and Unity Catalog tag implementation examples

---

## Pipeline Service Principal Privilege Model

### Minimum Privilege Set by Layer

The pipeline service principal is the account under which scheduled Databricks jobs and notebook workflows run in CI/CD and production. It should operate under the principle of least privilege: it needs exactly the rights required to read sources and materialise outputs, and nothing more.

The service principal should never have:
- Metastore Admin or Catalog Owner privileges
- Access to catalogs outside the project catalog (e.g., no cross-catalog `SELECT`)
- `DROP` rights on tables it did not create (prevents accidental destruction of vault history)
- Direct access to secrets or credential stores beyond what is required for its connection configuration

The following table defines the minimum privilege set for each layer in a native Databricks pipeline:

| Schema | Required Privilege | Purpose |
|--------|-------------------|---------|
| Catalog (`main`) | `USE CATALOG` | Navigate the catalog |
| `bronze` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` | Materialise raw ingested tables; read them for downstream steps |
| `silver` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` on `bronze` | Materialise cleansed and conformed tables |
| `gold` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` on `silver` | Materialise aggregated, business-ready tables |
| `security` (if present) | `USE SCHEMA`, `SELECT` | Read user-region mapping tables used in dynamic view RLS |

`CREATE TABLE` implies the ability to create new tables and replace existing ones (`CREATE OR REPLACE TABLE`) within the schema. It does not grant the ability to drop tables the SP did not create unless `MODIFY` is also granted. For Delta Lake `MERGE` and full-refresh patterns, `CREATE TABLE` and `SELECT` are sufficient in most cases.

### Scoping to Pipeline Layer Boundaries

A key security property of the Medallion layer structure is that each layer only needs to read from the layer immediately below it. The Gold SP role does not need access to `bronze` directly — it reads from `silver`, which in turn was loaded from `bronze`. This layered dependency chain means that access permissions follow the same hierarchy as the data pipeline.

In practice, if a single service principal runs all pipeline steps across all layers, it needs `SELECT` on `bronze`, `silver`, and `gold` schemas simultaneously (because cross-schema queries in notebooks resolve at query time). The SP's `SELECT` access to raw schemas should therefore be treated as an operational requirement, not a privilege escalation — the SP is not a data consumer, and its access to raw Bronze tables does not expose PII to human users.

To reduce risk:

- The pipeline service principal should have its access audited regularly using `SHOW GRANTS ON SCHEMA <schema>`.
- Service principal credentials (OAuth tokens or PATs) should be rotated according to the organisation's credential rotation policy.
- The SP should be a non-human service account that cannot be used for interactive login.

### Managing Service Principals via REST API and Terraform

Service principals can be created and managed programmatically:

```bash
# Create a service principal using the Databricks CLI
databricks service-principals create --display-name "pipeline-sp-prod"

# List all service principals
databricks service-principals list

# Generate an OAuth token for the service principal (M2M OAuth)
databricks auth token --host https://<workspace>.azuredatabricks.net
```

```hcl
# Terraform: create a Databricks service principal and assign it to a group
resource "databricks_service_principal" "pipeline_sp" {
  display_name = "pipeline-sp-prod"
}

resource "databricks_group_member" "pipeline_sp_member" {
  group_id  = databricks_group.data_engineers.id
  member_id = databricks_service_principal.pipeline_sp.id
}

# Grant the minimum required privileges to the SP
resource "databricks_grants" "raw_vault_sp" {
  schema = "main.raw_vault"
  grant {
    principal  = databricks_service_principal.pipeline_sp.display_name
    privileges = ["USE SCHEMA", "CREATE TABLE", "SELECT"]
  }
}
```

### See Also

- [Databricks service principals — Databricks](https://docs.databricks.com/en/administration-guide/users-groups/service-principals.html)
- [Service principal M2M OAuth — Databricks](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html)
- [Databricks Terraform provider — Databricks](https://registry.terraform.io/providers/databricks/databricks/latest/docs)
- `security_cookbook.md` — native GRANT/REVOKE implementation examples

---

## Governance for Multi-Layer Architectures

### Where to Enforce Security

A common mistake in multi-layer data architectures is applying security controls at every layer, including raw ingestion layers that are never queried by end users. This creates maintenance overhead without meaningful security benefit, and — in the case of row-level security — can impose a significant query performance penalty on tables that are supposed to be fast pipeline inputs rather than consumer-facing surfaces.

The governing principle is: **enforce security at the consumption layer, not at the raw layer.**

- Raw layers (Bronze, Raw Vault) should be access-controlled by restricting which identities can reach them at all — only pipeline service principals and senior data engineers should have `SELECT` on raw schemas.
- The mart or Gold layer is where row-level security and column masking should be applied, because that is the layer consumer-facing queries touch.
- Applying RLS to a Bronze table or Raw Vault satellite is not the primary defence — it is a defence-in-depth measure for cases where direct vault access by engineers cannot be prevented.

### Medallion Architecture Enforcement Points

In a Medallion (Bronze / Silver / Gold) architecture:

| Layer | Access Control Approach |
|-------|------------------------|
| **Bronze** | Restrict by identity — only ingestion service principals and data engineers. No RLS. Column masking on known PII columns if engineers must have access and the organisation requires it. |
| **Silver** | Restrict by identity — data engineers and transformation service principals. No consumer access. No RLS (Silver is not consumer-facing). |
| **Gold** | Apply RLS and column masking here. Grant `SELECT` to analyst groups and BI service accounts on Gold views/tables. This is the enforcement point for all data access controls. |
| **Dynamic views (Gold)** | Where native Unity Catalog masking is not available, create `v_` prefixed views in the Gold schema with embedded `current_user()` / `IS_MEMBER()` logic. Grant on the view only — revoke direct table access from consumer groups. |

Never apply row-level security to Bronze tables. Bronze tables are large append-only logs. RLS predicates on append-only tables with no partition pruning alignment will cause full table scans on every query. The intent of Bronze is pipeline throughput, not consumer access.

### See Also

- [Medallion architecture — Databricks](https://docs.databricks.com/en/lakehouse/medallion.html)
- [Unity Catalog row filters and column masks — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)
- `security_cookbook.md` — RLS with dynamic views, Unity Catalog native column masks, Unity Catalog tag management, and audit logging examples
- [Medallion layer responsibilities — Processing Patterns](../processing/processing_patterns.md)
