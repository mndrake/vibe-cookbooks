# Security Architectural Patterns

## Overview

This document describes the architectural patterns and design decisions that govern data security and governance on Databricks. It is a **decision and design reference** — not a step-by-step implementation guide. For implementation details, see `security_cookbook.md` in the same directory.

The patterns covered here are:

- **Unity Catalog Governance Model** — the hierarchy, role structure, and when to use native features versus dynamic views
- **Data Vault Governance Alignment** — how hub, link, and satellite boundaries map to natural security enforcement points
- **dbt Service Principal Privilege Model** — the minimum privilege set for a dbt service principal scoped to Data Vault layers
- **Governance for Multi-Layer Architectures** — where security should be enforced in Medallion and Data Vault pipelines

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
- `security_cookbook.md` — RBAC, RLS, and column masking implementation examples

---

## Data Vault Governance Alignment

### Hub and Link Access Patterns

Hubs and Links are the structural backbone of a Data Vault. They contain only business keys (hubs) and relationship records between business keys (links) — no descriptive attributes, no PII, no sensitive business metrics.

Because of this, hubs and links can be treated as broadly accessible reference structures within the vault. Data engineers building downstream transformations need to join through hubs and links to resolve entity relationships; restricting this access creates unnecessary friction without providing meaningful security benefit.

Recommended access posture for hubs and links:

- Data engineers: `SELECT` on the `raw_vault` schema (which gives access to all hub and link tables).
- Pipeline service principals: `SELECT` on `raw_vault` for downstream transformation steps.
- Analysts should not access hub and link tables directly — they should work through mart-layer views that resolve relationships into business-meaningful joins.

The exception is when a hub contains a business key that is itself sensitive — for example, a hub built on a national identity number or a tax identifier. In these cases, the hub itself requires the same treatment as a satellite: restricted access and column masking on the key column. This is rare but must be identified at design time.

### Satellite-Level Security Boundary

Satellites contain all descriptive data — which means they contain PII, personal attributes, financial details, health data, and any other sensitive business information. The satellite is the natural and correct security boundary in a Data Vault.

This is architecturally significant: rather than applying row-level security and column masking across a wide, denormalised consumer-facing table with 50–100 columns, the PII is concentrated in a small number of satellite tables, each with a focused set of attributes. A customer PII satellite might contain six columns: hash key, load date, record source, hashdiff, email, and phone. Applying column masking to two columns on one satellite is significantly simpler than managing masking across a 60-column mart table.

Design principles for satellite-level security:

- PII satellites should be in a schema with access restricted to pipeline service principals and authorised data stewards. Analysts and BI users should never have direct `SELECT` on PII satellite tables.
- Column masking or dynamic views that expose masked satellite attributes belong at the mart layer, not on the satellite itself (exception: when engineers require direct vault access and the organisation cannot prohibit this, apply masking at the satellite level as a defence-in-depth measure).
- Hashdiff columns contain no readable data and require no masking — they are hash values computed from the attribute set and carry no business meaning on their own.

### Recommended Schema-Level Privilege Structure

The following structure reflects a Data Vault project with a `raw_vault`, `business_vault`, and `marts` schema inside a single catalog. Adjust catalog and schema names to match your naming convention.

| Schema | Role / Principal | Privileges | Rationale |
|--------|-----------------|-----------|-----------|
| `raw_vault` | dbt service principal | `USE SCHEMA`, `CREATE TABLE`, `SELECT` | Pipeline materialisation and cross-satellite joins |
| `raw_vault` | Data engineers | `USE SCHEMA`, `SELECT` | Debugging, lineage investigation |
| `raw_vault` | Analysts | No access | Satellites contain raw PII; analysts work through marts |
| `business_vault` | dbt service principal | `USE SCHEMA`, `CREATE TABLE`, `SELECT` on `raw_vault` | Materialises PITs, bridges, derived satellites |
| `business_vault` | Data engineers | `USE SCHEMA`, `SELECT` | Debugging |
| `business_vault` | Analysts | No access | Business vault is an intermediate layer, not consumer-facing |
| `marts` | dbt service principal | `USE SCHEMA`, `CREATE TABLE`, `SELECT` on `business_vault` | Materialises information mart tables |
| `marts` | Analysts | `USE SCHEMA`, `SELECT` on specific tables/views | Governed, consumer-facing access with RLS applied |
| `marts` | BI service account | `USE SCHEMA`, `SELECT` on specific tables/views | Same as analysts, scoped to BI-consumed objects |

### See Also

- [Data Vault 2.0 standard — Dan Linstedt](https://www.danlinstedt.com/solutions-2/data-vault-basics/)
- [AutomateDV documentation](https://automate-dv.readthedocs.io/en/latest/)
- `security_cookbook.md` — RBAC setup and dynamic view RLS examples
- `/home/user/vibe-cookbooks/data_architecture/data_vault/dv2_architecture.md` — Data Vault layer definitions

---

## dbt Service Principal Privilege Model

### Minimum Privilege Set by Layer

The dbt service principal is the account under which dbt jobs run in CI/CD and scheduled production runs. It should operate under the principle of least privilege: it needs exactly the rights required to read sources and materialise outputs, and nothing more.

The service principal should never have:
- Metastore Admin or Catalog Owner privileges
- Access to catalogs outside the project catalog (e.g., no cross-catalog `SELECT`)
- `DROP` rights on tables it did not create (prevents accidental destruction of vault history)
- Direct access to secrets or credential stores beyond what is required for its connection configuration

The following table defines the minimum privilege set for each layer in a Data Vault dbt project:

| Schema | Required Privilege | Purpose |
|--------|-------------------|---------|
| Catalog (`main`) | `USE CATALOG` | Navigate the catalog |
| `staging` | `USE SCHEMA`, `SELECT` (on raw source tables) | Read raw ingested data to build staging models |
| `raw_vault` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` | Materialise hub, link, and satellite tables; read them for downstream models |
| `business_vault` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` on `raw_vault` | Materialise PIT tables, bridge tables, derived satellites |
| `marts` | `USE SCHEMA`, `CREATE TABLE`, `SELECT` on `business_vault` | Materialise information mart tables |
| `security` (if present) | `USE SCHEMA`, `SELECT` | Read user-region mapping tables used in dynamic view RLS |

`CREATE TABLE` implies the ability to create new tables and replace existing ones (`CREATE OR REPLACE TABLE`) within the schema. It does not grant the ability to drop tables the SP did not create unless `MODIFY` is also granted. For dbt's incremental materialisation and full-refresh behaviour, `CREATE TABLE` and `SELECT` are sufficient in most cases.

### Scoping to Data Vault Layer Boundaries

A key security property of the Data Vault layer structure is that each layer only needs to read from the layer immediately below it. The marts SP role does not need access to `raw_vault` directly — it reads from `business_vault`, which in turn was loaded from `raw_vault`. This layered dependency chain means that access permissions follow the same hierarchy as the data pipeline.

In practice, if a single service principal runs all dbt models across all layers, it needs `SELECT` on `raw_vault`, `business_vault`, and `marts` schemas simultaneously (because dbt resolves cross-schema `ref()` calls at query time). The SP's `SELECT` access to raw schemas should therefore be treated as an operational requirement, not a privilege escalation — the SP is not a data consumer, and its access to raw satellites does not expose PII to human users.

To reduce risk:

- The dbt service principal should have its access audited regularly using `SHOW GRANTS ON SCHEMA <schema>`.
- Service principal credentials (OAuth tokens or PATs) should be rotated according to the organisation's credential rotation policy.
- The SP should be a non-human service account that cannot be used for interactive login.

### See Also

- [Databricks service principals — Databricks](https://docs.databricks.com/en/administration-guide/users-groups/service-principals.html)
- [dbt-databricks connection configuration — dbt docs](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
- [dbt grants configuration — dbt docs](https://docs.getdbt.com/reference/resource-configs/grants)
- `security_cookbook.md` — dbt Grants Block implementation example

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

### Data Vault Enforcement Points

In a Data Vault architecture:

| Layer | Access Control Approach |
|-------|------------------------|
| **Staging (dbt views/ephemeral)** | No persistent security objects needed — staging is transient. The underlying raw table access is controlled at the raw schema level. |
| **Raw Vault (Hubs, Links)** | Restrict by identity. Hubs and links contain only keys — broadly accessible to engineers, not to analysts. |
| **Raw Vault (Satellites)** | Restrict by identity. Column masking as defence-in-depth if direct vault access by engineers cannot be prohibited. Do not apply RLS — vault satellites are pipeline tables, not consumer surfaces. |
| **Business Vault (PITs, Bridges)** | Restrict by identity — pipeline SP and data engineers only. |
| **Information Marts** | Apply RLS and column masking here. This is the consumer-facing layer and the correct enforcement point. Grant `SELECT` to analyst and BI groups on mart tables or views only. |

The Information Mart layer in Data Vault maps directly to the Gold layer in Medallion in terms of where security enforcement belongs. The vault's structural separation of business keys (broadly accessible) from descriptive attributes (restricted) means that access control can be applied with surgical precision at the satellite and mart levels without needing to mask hundreds of columns on wide denormalised tables.

**Exception — PII satellite column masking:** When the organisation cannot restrict direct vault access by engineers (e.g., engineers need to run exploratory queries on satellites during incident investigation), apply column masking on PII columns in PII satellites as a defence-in-depth measure. This protects against accidental exposure of PII in query results and audit logs, even if the engineer has `SELECT` on the schema.

### See Also

- [Medallion architecture — Databricks](https://docs.databricks.com/en/lakehouse/medallion.html)
- [Unity Catalog row filters and column masks — Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)
- `security_cookbook.md` — RLS with dynamic views, Unity Catalog native column masks, and audit logging examples
- `/home/user/vibe-cookbooks/data_architecture/processing/processing_patterns.md` — Medallion layer responsibilities
