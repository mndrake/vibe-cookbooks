# Data Vault 2.0 Architectural Patterns

## Overview

Data Vault 2.0 (DV2) is a modelling methodology and architecture pattern designed for enterprise data warehouses where auditability, scalability, and agility are non-negotiable requirements. It was formalised by Dan Linstedt and is standardised under the CDVDM (Common Data Vault Data Model) specification.

Unlike dimensional modelling (Kimball) or third-normal form (Inmon), Data Vault separates structure from meaning. Raw data lands in vault tables exactly as it arrived, with no business rules applied. Business logic is layered on top in a separate Business Vault tier. Consumer-ready star schemas or flat tables are built in Information Marts.

On Databricks, Data Vault 2.0 is implemented using dbt as the transformation framework, AutomateDV (formerly datavault4dbt) as the dbt macro library that generates vault-pattern SQL, and dbt-utils for general-purpose testing and utility macros. Delta Lake's append-optimised write semantics and Photon's vectorised execution make it a strong physical target for vault structures.

---

## Core Concepts

### Hubs

A Hub represents a unique business entity — specifically, its unique business key (natural key). A Hub contains exactly four column types:

- **Hash Key** (`_HK`): a surrogate key derived by hashing the natural key using a consistent algorithm (MD5 or SHA-256)
- **Natural Key** (`_NK` or the source column name): the actual business identifier from the source system (e.g., `CUSTOMER_ID`, `ORDER_NUMBER`)
- **Load Date** (`LOAD_DATE` or `LDTS`): the timestamp at which this record first appeared in the vault
- **Record Source** (`RECORD_SOURCE` or `RSRC`): the system of origin for this record

A Hub stores only the **first** appearance of each natural key. It is idempotent — re-running a Hub load never produces duplicates. Descriptive attributes such as name, address, and status are never stored in a Hub; they belong in Satellites.

Example: `HUB_CUSTOMER` stores one row per unique `CUSTOMER_ID`. Whether that customer came from a CRM system, an e-commerce platform, or a mobile app, the Hub unifies them under a single hash key.

### Links

A Link records the **relationship** between two or more business entities (Hubs). It captures the structural association at the lowest possible grain — one row per unique combination of foreign hash keys.

A Link contains:
- **Link Hash Key** (`_HK`): a surrogate derived from hashing the combination of all foreign keys in the relationship
- **Foreign Hash Keys**: one column per Hub involved in the relationship (e.g., `CUSTOMER_HK`, `ORDER_HK`)
- **Load Date**: when this relationship first appeared
- **Record Source**: the source system

Like Hubs, Links are append-only and store only the first occurrence of each relationship. They do not track when a relationship ended — that is the responsibility of an Effectivity Satellite.

Example: `LINK_CUSTOMER_ORDER` has one row for each unique (CUSTOMER_HK, ORDER_HK) combination. If the same customer places 100 orders, there are 100 rows — one per unique relationship instance.

### Satellites

A Satellite stores the **descriptive attributes** (payload) associated with a Hub or Link. It is the only vault structure that changes over time, and it does so through **append-only** inserts — no updates, no deletes.

Change detection is performed using a **Hashdiff** column: a hash computed over all payload columns. When a new row arrives from staging, its hashdiff is compared to the most recent hashdiff for the same parent hash key. If they differ, a new row is inserted. If they are identical, the row is discarded.

A Satellite contains:
- **Parent Hash Key**: the `_HK` from the Hub or Link this satellite describes
- **Load Date**: when this version of the attributes was loaded
- **Hashdiff** (`_HASHDIFF`): a hash of all payload columns
- **Record Source**
- **Payload columns**: the actual descriptive data (e.g., `CUSTOMER_NAME`, `EMAIL`, `PHONE`)

Because satellites are append-only and grow without bound, they are partitioned by `LOAD_DATE` on Databricks for query performance.

### Reference Structures

Reference structures (`REF_HUB` and `REF_SAT`) are vault-pattern equivalents of lookup or reference tables. They follow the same structural conventions as regular Hub/Satellite pairs but are loaded from static or slowly-changing reference data rather than operational source systems.

Examples include country code tables, currency codes, product category hierarchies, and status code mappings. Using reference structures rather than plain seed tables ensures that vault-style governance (hash keys, load dates, record sources) is applied consistently across all data in the warehouse.

### Business Vault

The Business Vault is a logical layer built on top of the Raw Vault. It contains models that apply business rules, derived metrics, computed keys, and soft business logic without modifying any Raw Vault structures.

Typical Business Vault artefacts include:
- Derived business keys (e.g., a canonical customer identifier resolved from multiple source keys)
- Computed attributes (e.g., `FULL_NAME` concatenated from `FIRST_NAME` and `LAST_NAME`)
- Point-in-Time (PIT) tables
- Bridge tables
- Derived metrics aggregated across satellites

The Business Vault is the layer where interpretation begins. Everything in the Raw Vault is structural and source-faithful.

### Information Marts

Information Marts are the consumer-facing layer of the architecture. They present data as star schemas or flat denormalised tables, designed for BI tools, analysts, and downstream applications.

Marts are built from PIT and Bridge tables, which pre-compute the complex multi-way join paths across the vault graph. Mart models must never query Raw Vault tables directly — they always go through the Business Vault intermediaries.

---

## When to Use Data Vault 2.0

### Decision Criteria

Data Vault 2.0 is the right choice when two or more of the following conditions are true:

1. **Multiple source systems** feed the same business entities. Three or more source systems contributing to the same entities (customers, products, orders) create integration challenges that vault's multi-source Hub loading handles natively.
2. **Full audit trail is a compliance requirement.** Regulatory frameworks (GDPR, SOX, HIPAA, FCA) require that every version of every attribute be retained with a timestamp and source. Satellites provide this by design.
3. **Parallel loading is required for performance.** Hubs, Links, and Satellites have no foreign key dependencies on each other at load time. They can be loaded in parallel, which is critical for large daily batch volumes.
4. **The team has Data Vault expertise or is investing in building it.** DV2 has a steeper initial learning curve than dimensional modelling. Without team familiarity, early-stage productivity will be lower.
5. **Schema evolution is frequent.** New source columns can be added as new Satellites without modifying existing structures. This agility is a core DV2 value proposition.

### When Medallion Is Sufficient

The Medallion architecture (Bronze / Silver / Gold) is simpler to implement and operate. Prefer it when:

- **A single source system** feeds the warehouse. Integration complexity — the primary driver for vault — is absent.
- **The team is small** (fewer than 4–5 data engineers). The overhead of maintaining Hub/Link/Satellite triples and PIT tables is not justified at small scale.
- **BI-focused output is the primary goal** and auditability is not a regulatory requirement. Medallion Gold layers can satisfy most BI consumers more directly than vault + mart.
- **Speed of delivery is prioritised over auditability.** The first release of a Medallion pipeline can be operational within days. A correctly structured vault implementation requires more upfront design work.
- **The data domain is narrow and stable.** If the schema rarely changes and the entity model is simple, vault's flexibility for schema evolution is not needed.

### See Also

- [Data Vault Alliance — What is Data Vault 2.0?](https://www.datavaultalliance.com/news/about-data-vault-2-0/)
- [AutomateDV Documentation](https://automate-dv.readthedocs.io/en/latest/)
- [Databricks Lakehouse Architecture Guide](https://docs.databricks.com/en/lakehouse-architecture/index.html)

---

## Layer Responsibilities

### Raw Vault

The Raw Vault is a **structural, source-faithful, append-only** layer. Its only purpose is to integrate data from multiple source systems into a consistent structural form without applying any business logic.

Rules that must be enforced in the Raw Vault:

- **No business rules.** Do not rename, reclassify, combine, or interpret source values. If the source sends a status code of `X`, store `X`.
- **Append-only.** Never issue UPDATE or DELETE statements against Hub, Link, or Satellite tables. New information is represented as new rows.
- **Hash keys only.** Raw Vault tables reference each other via hash keys, never via natural keys or integer sequences from source systems.
- **No joins to business vault or mart.** Raw Vault models read from staging only.
- **Full source fidelity.** If the source column is NULL, store NULL (subject to null substitution rules applied in staging before hashing).

The Raw Vault is the permanent, immutable record of all data that has ever entered the warehouse. It is never rebuilt from scratch.

### Business Vault

The Business Vault interprets and enriches raw vault data. It is the appropriate location for:

- **Computed business keys**: resolving multiple source system identifiers to a canonical enterprise identifier
- **Derived attributes**: `FULL_NAME`, `AGE_BAND`, `CREDIT_TIER`
- **Soft business rules**: rules that may change over time and that the business is still refining
- **PIT tables**: point-in-time snapshots that materialise the correct satellite version for each date
- **Bridge tables**: pre-computed join paths across the vault graph

Business Vault models are full-refresh or incremental. They may be rebuilt when business rules change. They must never modify Raw Vault structures.

### Information Mart

The Information Mart presents data to end consumers in the simplest possible form. It contains:

- **Dimension tables** (`DIM_`): flat, single-version-of-truth entity tables for BI use
- **Fact tables** (`FCT_`): grain-defined event or transaction tables
- **Wide flat tables**: denormalised single-table outputs for tools that cannot handle star schema joins

Rules for the Information Mart:

- Never expose vault joins to BI users. All complexity is resolved in PIT and Bridge tables before the mart is built.
- Materialise as Delta tables, not views. Mart queries over views that reach back to raw vault are prohibitively slow.
- Refresh on a schedule aligned with business reporting needs (typically daily or hourly).
- One row per business key in dimension tables. One row per event/transaction at the defined grain in fact tables.

### See Also

- [AutomateDV Layer Documentation](https://automate-dv.readthedocs.io/en/latest/tutorial/tut_staging/)
- [dbt Materialisation Strategies](https://docs.getdbt.com/docs/build/materializations)
- [dv2_staging_cookbook.md](./dv2_staging_cookbook.md)
- [dv2_raw_vault_cookbook.md](./dv2_raw_vault_cookbook.md)
- [dv2_business_vault_cookbook.md](./dv2_business_vault_cookbook.md)
- [dv2_information_mart_cookbook.md](./dv2_information_mart_cookbook.md)

---

## Hash Key Design

### Algorithm Selection

Two hashing algorithms are in common use for Data Vault 2.0:

| Algorithm | Pros | Cons | Recommended When |
|-----------|------|------|-----------------|
| MD5 | Faster on Photon; 16-byte output; widely supported | Higher (though still negligible) collision probability | Performance-sensitive pipelines; non-regulated environments |
| SHA-256 | Lower collision probability; 32-byte output | Slightly slower; larger storage footprint | Compliance-driven environments (SOX, HIPAA, FCA) |

**The algorithm must be chosen once and applied consistently across every staging model in the project.** AutomateDV sets the algorithm globally in `dbt_project.yml` via the `hash` variable. There is no supported path for mixing algorithms within a single vault instance.

### Column Ordering Conventions

The order in which source columns are concatenated before hashing determines the hash value. Two models that hash the same business key columns in different orders will produce different hash values.

Conventions to enforce:

1. **Document column ordering in a project-level standard**, not in individual model comments.
2. **Alphabetical ordering** is the most common default (AutomateDV applies alphabetical order by default for multi-column hashes).
3. **Natural keys for Hubs** are single-column in most cases; ordering is only relevant for composite natural keys and all Link foreign key combinations.
4. **Never reorder columns after the initial build.** Reordering changes every hash value, which cascades as false-positive changes across all satellites and renders historical rows orphaned.

### Null Substitution

NULL values in source columns create hashing inconsistencies: different database engines may hash NULL differently, or the concatenation may produce unexpected results.

AutomateDV enforces a null substitution character before hashing. The standard substitution string is `^^` (two carets), configured via the `concat_char` variable in `dbt_project.yml`.

Rules:

- Apply null substitution to **all columns** included in a hash, not just business key columns.
- The substitution character must be a value that **cannot appear in real data**. Review source system constraints before confirming the choice of `^^`.
- Document the substitution character in the project standard. If it needs to change, all hashes must be recomputed from scratch.

### Consistency Requirements

Hash key consistency is the single most important correctness property of a Data Vault implementation. Inconsistencies between staging models that contribute to the same Hub or Link will result in the same real-world entity being assigned multiple hash keys, breaking the integration promise of the vault.

Enforce consistency by:

- Applying `UPPER()` to all string values before hashing. AutomateDV does this automatically when `hashtype` is configured correctly.
- Trimming leading/trailing whitespace from all string columns before hashing.
- Applying the same date/timestamp format mask to all date columns before hashing.
- Using a single shared staging base model (or dbt source with column casting) rather than repeating transformation logic in each individual staging model.

### See Also

- [AutomateDV Hash Configuration](https://automate-dv.readthedocs.io/en/latest/macros/stage/)
- [Data Vault 2.0 Standard — Hash Key Specification](https://www.datavaultalliance.com)
- [dv2_staging_cookbook.md — Staging section](./dv2_staging_cookbook.md)

---

## AutomateDV and dbt-databricks Setup

### packages.yml Configuration

Add the following to `packages.yml` in the root of your dbt project. Pin specific versions to avoid breaking changes from upstream package releases.

```yaml
packages:
  - package: Datavault-UK/automate_dv
    version: 0.10.2

  - package: dbt-labs/dbt_utils
    version: 1.3.0
```

Run `dbt deps` to install the packages after updating this file.

Check the [AutomateDV releases page](https://github.com/Datavault-UK/automate_dv/releases) and [dbt-utils releases page](https://github.com/dbt-labs/dbt-utils/releases) for the latest stable versions before pinning.

### Version Compatibility

AutomateDV has specific compatibility requirements with dbt-core and dbt-databricks. The following combinations are known to work as of the AutomateDV 0.10.x series:

| AutomateDV | dbt-core | dbt-databricks | Notes |
|------------|----------|----------------|-------|
| 0.10.2 | 1.7.x | 1.7.x | Recommended as of early 2025 |
| 0.10.1 | 1.6.x | 1.6.x | Supported |
| 0.9.x | 1.5.x | 1.5.x | Legacy; still in use at some sites |

Always verify compatibility in the [AutomateDV documentation](https://automate-dv.readthedocs.io/en/latest/support/) before upgrading dbt-core or dbt-databricks in an existing vault project.

### dbt_project.yml Variables

AutomateDV reads configuration from project-level variables. Define them in the `vars` block of `dbt_project.yml`:

```yaml
vars:
  # Hashing algorithm — 'MD5' or 'SHA' (SHA-256)
  hash: MD5

  # Null substitution character — must not appear in real source data
  concat_char: '^^'

  # Maximum varchar length for hash key columns on Databricks
  # STRING type is variable-length — this is used for documentation/typing only
  max_varchar: 'STRING'

  # Enable/disable ghost record injection for referential integrity
  enable_ghost_records: true
```

The `hash` and `concat_char` variables affect every model that calls an AutomateDV macro. Set them once and do not change them after the vault has been populated.

### See Also

- [AutomateDV dbt_project.yml Variables Reference](https://automate-dv.readthedocs.io/en/latest/tutorial/tut_init/)
- [dbt-databricks Setup Guide](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
- [dbt packages documentation](https://docs.getdbt.com/docs/build/packages)

---

## Unity Catalog Governance Alignment

### Schema Structure

A Data Vault 2.0 implementation on Databricks with Unity Catalog should use separate schemas to enforce layer boundaries and apply access controls at the schema level.

Recommended schema layout within a single Unity Catalog catalog (e.g., `main` or a project-specific catalog):

| Schema | Contents | dbt Models |
|--------|----------|------------|
| `staging` | Staging models with hash keys and hashdiffs | `models/staging/` |
| `raw_vault` | Hubs, Links, Satellites, Reference structures | `models/raw_vault/` |
| `business_vault` | PIT, Bridge, derived business rules | `models/business_vault/` |
| `marts` | Dimensions, Facts, wide flat tables | `models/marts/` |

Configure the schema mapping in `dbt_project.yml`:

```yaml
models:
  your_project_name:
    staging:
      +schema: staging
      +materialized: view
    raw_vault:
      +schema: raw_vault
      +materialized: incremental
      +incremental_strategy: append
    business_vault:
      +schema: business_vault
      +materialized: table
    marts:
      +schema: marts
      +materialized: table
```

### Tagging Strategy

Apply Unity Catalog tags to vault structures to support data discovery and governance tooling:

| Tag | Applied To | Purpose |
|-----|-----------|---------|
| `classification:structural` | Hubs, Links | Identifies structural vault tables — no business payload |
| `classification:descriptive` | Satellites | Contains payload data; sensitivity varies by satellite |
| `sensitivity:pii` | Satellites with PII columns (name, email, address) | Triggers masking policy application |
| `sensitivity:financial` | Satellites with financial data | Triggers audit logging policy |
| `layer:raw_vault` | All raw vault tables | Used for lineage filtering in Unity Catalog |
| `layer:mart` | All mart tables | Identifies consumer-facing tables |

Tags are applied using `ALTER TABLE ... SET TAGS` in a post-hook or via Terraform/Pulumi for infrastructure-as-code deployments.

### Access Patterns by Layer

Enforce least-privilege access using Unity Catalog grants at the schema level:

| Principal | `staging` | `raw_vault` | `business_vault` | `marts` |
|-----------|-----------|-------------|-----------------|---------|
| dbt service principal | SELECT | CREATE TABLE, INSERT, SELECT | CREATE TABLE, INSERT, SELECT | CREATE TABLE, INSERT, SELECT |
| Data engineers | SELECT | SELECT | SELECT | SELECT |
| Data analysts | — | — | SELECT (non-PII only) | SELECT |
| BI tool service account | — | — | — | SELECT |
| PII data stewards | — | SELECT (with masking) | SELECT (with masking) | SELECT (with masking) |

Masking policies for PII columns (email, phone, full name) are applied at the `raw_vault` and `marts` layers. Non-privileged roles see masked values; the dbt service principal and PII stewards see clear text.

### See Also

- [Unity Catalog Privileges and Securable Objects](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html)
- [Unity Catalog Column Masking](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)
- [dbt-databricks Unity Catalog Configuration](https://docs.getdbt.com/reference/resource-configs/databricks-configs#using-unity-catalog-with-your-dbt-project)
