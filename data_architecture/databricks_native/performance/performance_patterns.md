> **Native Databricks version.** This document covers performance tuning patterns using Databricks-native tooling only (Delta Lake, DLT pipelines, PySpark jobs, Databricks Workflows). For the dbt-integrated version of these patterns, see [`../../../databricks_and_dbt/performance/performance_patterns.md`](../../../databricks_and_dbt/performance/performance_patterns.md).

---

# Performance Tuning Architectural Patterns

## Overview

This document describes the key architectural decisions and design trade-offs for performance tuning on Databricks. It is a decision and design reference — not a step-by-step guide. For implementation details, see the [Performance Cookbook](./performance_cookbook.md).

The four pattern areas covered here are:

1. **Delta Lake Storage Optimization Strategy** — how to choose between OPTIMIZE/ZORDER, static partitioning, and Liquid Clustering
2. **Cluster vs. SQL Warehouse Selection** — when to use each compute type and what the cost implications are
3. **Vault-Specific Performance Patterns** — why Point-in-Time (PIT) and Bridge tables exist in Data Vault and when their overhead is justified
4. **Adaptive Query Execution (AQE) and Photon Scope** — what each engine feature covers automatically and where it stops

---

## Delta Lake Storage Optimization Strategy

### Overview

Delta Lake stores data as Parquet files in cloud object storage. Left unmanaged, frequent small writes — from streaming, micro-batch, or incremental loads — accumulate thousands of small files. Small files degrade read performance because Spark must open and scan each file individually, multiplying I/O overhead and driver-side planning cost.

Databricks provides three complementary mechanisms to address this: OPTIMIZE (file compaction with optional ZORDER clustering), static partitioning, and Liquid Clustering. These are not mutually exclusive in all cases, but they operate at different layers of the storage layout and have meaningfully different trade-offs.

### OPTIMIZE and ZORDER vs. Partitioning vs. Liquid Clustering

**OPTIMIZE and File Compaction**

OPTIMIZE rewrites small files into larger ones, targeting approximately 1 GB per output file. It does not change the logical structure of the table — it only compacts what is already there. OPTIMIZE can be run at any time and is safe to run while reads are in progress. For large tables, it can be scoped to a partition range using a `WHERE` clause (e.g., `WHERE event_date >= current_date - 7`) to limit the operation to recently written data.

**ZORDER Clustering**

ZORDER BY is an optional extension to OPTIMIZE. Rather than simply compacting files, ZORDER sorts and interleaves data on the specified columns so that rows with similar values for those columns end up in the same files. This enables Delta's file-skipping mechanism to skip entire files when a query filters on a ZORDERed column — the query engine reads the file-level min/max statistics and excludes files whose ranges do not overlap the filter predicate.

ZORDER is effective for 1–3 high-cardinality filter columns on non-partitioned or coarsely partitioned tables. Its effectiveness degrades with more columns because the Z-curve locality guarantee weakens in higher dimensions. Critically, ZORDER must be re-applied on every OPTIMIZE run to maintain clustering quality as new data is written.

**ZORDER is not effective on a column that is also used as a partition key.** Within a partition directory, all files already contain only one value of the partition column, so there is no clustering benefit to apply. Applying ZORDER to a partition column wastes compute.

**Static Partitioning**

Partitioning physically separates data into directory trees based on column values. Queries that filter on the partition column exactly (e.g., `WHERE event_date = '2026-03-01'`) can skip all directories that do not match, without reading any file-level metadata. This is the most aggressive form of data skipping available, but it comes at a cost: over-partitioning (e.g., partitioning by a high-cardinality column like `user_id` or `order_id`) creates millions of tiny directories and files, which is worse than no partitioning at all.

Static partitioning is appropriate when:

- The partition column has low cardinality (100–10,000 distinct values)
- Queries almost always filter on that column as an equality or range predicate
- The table is large enough that partition pruning produces a meaningful reduction in scanned data
- The partition column is stable — changing the partition key of an existing table is expensive and disruptive

For Data Vault satellites, partitioning by `LOAD_DATE` truncated to day or month is a common pattern, but only when the satellite is queried with date-range filters and the load cadence produces enough daily volume to justify it.

**Liquid Clustering**

Liquid Clustering (available from Databricks Runtime 13.3+) is an adaptive replacement for both ZORDER and static partitioning. It uses a space-filling curve to cluster data on the specified columns, but the clustering is applied incrementally and automatically as data is written — rather than requiring a full OPTIMIZE pass. Clustering columns can be changed with `ALTER TABLE ... CLUSTER BY` without rewriting the table.

Liquid Clustering is the preferred choice for new tables where:

- Query filter patterns are not stable or are expected to evolve
- Multiple different columns are filtered by different queries
- The table is large and the cost of periodic full OPTIMIZE + ZORDER runs is significant

Liquid Clustering is **incompatible with static partitioning on the same table**. It cannot be applied to tables that already have `PARTITIONED BY` defined.

### Decision Criteria

| Scenario | Recommended Approach |
|----------|----------------------|
| New table, filter patterns unknown or evolving | Liquid Clustering |
| Existing table, 1–3 stable high-cardinality filter columns | OPTIMIZE + ZORDER BY |
| Large table filtered almost exclusively on a date column | Static partitioning by date (day or month), then ZORDER on secondary columns |
| Filter column is already a partition key | ZORDER on that column has no effect — ZORDER on secondary columns only |
| Table receives many small writes (streaming or micro-batch) | OPTIMIZE on a scheduled cadence; consider Liquid Clustering to reduce maintenance |
| Over-partitioned table with millions of directories | Rewrite with coarser partitioning or migrate to Liquid Clustering |

### See Also

- [Delta Lake OPTIMIZE — Databricks Documentation](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html)
- [Liquid Clustering — Databricks Documentation](https://docs.databricks.com/en/delta/clustering.html)
- [Delta Lake File Skipping — Databricks Documentation](https://docs.databricks.com/en/delta/data-skipping.html)
- [Delta Lake optimizations — Delta Lake](https://docs.delta.io/latest/optimizations-oss.html)
- [Performance Cookbook — Delta Lake Optimization](./performance_cookbook.md)

---

## Cluster vs. SQL Warehouse Selection

### Overview

Databricks provides two compute surfaces for running queries and code: **all-purpose and job clusters** (Spark clusters running on VMs) and **SQL Warehouses** (a managed, serverless-capable compute layer optimised for SQL). Choosing the wrong compute type for a workload is a common source of both poor performance and unnecessary cost.

The distinction is not merely a UI preference — the two surfaces have different startup characteristics, billing models, scaling behaviours, and capability scopes.

### Decision Criteria

**Use a Spark Cluster when:**

- Running ETL jobs written in PySpark, Scala, or Java
- Running streaming pipelines (Structured Streaming or Delta Live Tables)
- Executing machine learning training or inference workloads
- Running notebooks interactively with PySpark code
- Orchestrating workflows with the Databricks Jobs API that require a long-running driver process
- Running Delta Live Tables (DLT) pipelines — DLT runs on clusters managed by the DLT runtime

**Use a SQL Warehouse when:**

- Running ad hoc SQL queries from the Databricks SQL Editor
- Connecting BI tools (Tableau, Power BI, Looker) to Databricks via JDBC/ODBC
- Running short-duration analytical queries where startup time is acceptable and per-query cost matters
- Serving dashboards or embedded analytics where the warehouse will be shared across many concurrent users
- Running SQL-based Databricks Workflows tasks that do not require PySpark

### Cost Implications

**SQL Warehouse cost behaviour:**

- SQL Warehouses scale to zero when idle and stop consuming DBUs. For infrequent workloads, this makes them significantly cheaper than leaving a cluster running.
- **Serverless SQL Warehouse** starts in approximately 2 seconds and has no warm-up cost. It bills per query execution second, not per cluster uptime.
- **Classic (Pro/Standard) SQL Warehouse** takes approximately 2 minutes to start. It bills per cluster-uptime DBU, similar to a job cluster, but scales down to zero when idle.
- For BI tools with consistent daytime usage, a classic warehouse may be cheaper than serverless if it stays active throughout the working day. For sporadic usage, serverless is almost always cheaper.

**Cluster cost behaviour:**

- Clusters bill DBUs continuously from start to auto-termination, regardless of whether they are executing code.
- Auto-termination is critical on interactive clusters. A cluster left running overnight at 8 DBU/hour incurs ~64 DBUs of waste.
- Autoscaling reduces cost for variable workloads but adds latency when scaling up — nodes must be provisioned and join the cluster. For streaming jobs, autoscaling can cause instability during scale-down events; fixed-size clusters are preferred for steady streaming workloads.
- Job clusters (created and destroyed per job run) are cheaper than all-purpose clusters for production ETL because they only run for the duration of the job.

### See Also

- [SQL Warehouses — Databricks Documentation](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
- [Serverless SQL Warehouses — Databricks Documentation](https://docs.databricks.com/en/compute/sql-warehouse/serverless.html)
- [Cluster Configuration — Databricks Documentation](https://docs.databricks.com/en/compute/configure.html)
- [Delta Live Tables — Databricks Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Performance Cookbook — Cluster Sizing and Autoscaling](./performance_cookbook.md)

---

## Vault-Specific Performance Patterns

### Overview

Data Vault models distribute data across many narrow tables: one hub per business entity, one link per relationship, and one satellite per source system or attribute group per hub or link. This normalised structure provides excellent auditability and flexibility for loading, but it creates a query-time cost: reconstructing a business object from a vault requires joining a hub to 5–10 satellites, each of which may contain hundreds of millions of rows at different load timestamps.

Point-in-Time (PIT) tables and Bridge tables are vault-layer constructs designed to absorb this join complexity at load time, so that it does not have to be recalculated at every query.

### Why PIT and Bridge Tables Exist

**Point-in-Time (PIT) Tables**

A satellite stores every version of an attribute group over time, keyed by hash key and `LOAD_DATE`. To reconstruct the correct version of all attributes for a business entity as of a given date, a query must find the most recent satellite row for each satellite where `LOAD_DATE <= as_of_date`. Across 5–10 satellites, this means 5–10 correlated subqueries or lateral joins — and on large satellites, each of those subqueries is expensive.

A PIT table pre-computes these "latest as of each snapshot date" pointers. For each snapshot date in a defined grain (typically daily), it stores the `LOAD_DATE` of the correct row for each satellite. This reduces the mart-layer join from:

```
hub JOIN sat_1 (latest version) JOIN sat_2 (latest version) JOIN ... JOIN sat_N (latest version)
```

to:

```
pit JOIN sat_1 ON pit.sat_1_load_date JOIN sat_2 ON pit.sat_2_load_date JOIN ... JOIN sat_N ON pit.sat_N_load_date
```

The number of joins is the same, but each satellite join is now an equality join on `hash_key + LOAD_DATE` — a key lookup rather than a range scan with a subquery. This is dramatically more efficient for the query engine to plan and execute.

In native Databricks pipelines, PIT tables are built and refreshed via PySpark MERGE or INSERT OVERWRITE jobs scheduled in Databricks Workflows, or as DLT tables in the gold/business-vault layer.

**Bridge Tables**

Bridge tables serve a similar purpose for multi-hop link traversals. In a vault model, following a chain of relationships (e.g., Order -> Order Line -> Product -> Product Category) requires joining through multiple link tables. A Bridge table pre-computes the full traversal path and stores it as a flat lookup table, eliminating the multi-hop join at query time.

In a native Databricks architecture, bridge tables are written by PySpark jobs that execute the multi-hop join once per load cycle and write the result to a Delta table. The DLT pipeline equivalent is a materialized DLT table in the business vault or gold layer.

### When They Justify Their Overhead

PIT and Bridge tables are not free. They must be refreshed after every vault load cycle. For a vault with 20 satellites and daily PIT snapshots, PIT refresh adds a non-trivial pipeline step. For some architectures, this is the longest-running step in the load.

**PIT and Bridge tables are justified when:**

- Mart-layer queries against the vault graph exceed reporting SLAs and profiling confirms the bottleneck is satellite join time
- BI tools (Tableau, Power BI) generate SQL against the vault layer directly — these tools often produce sub-optimal multi-join queries that a PIT table can significantly simplify
- The vault has more than 4–5 satellites on a hub and the hub is queried frequently
- Time-travel queries (as-of-date reporting) are a core use case — PIT tables make these queries straightforward

**PIT and Bridge tables are NOT justified when:**

- The vault is primarily a landing zone and all reporting is done against a separate gold/mart layer that is pre-computed by PySpark jobs or DLT pipelines
- Query volumes are low and latency requirements are not aggressive
- The vault is still in early development and the satellite structure is likely to change — PIT tables require maintenance when satellites are added or renamed

### See Also

- [Data Vault 2.0 — Dan Linstedt's Site](https://danlinstedt.com/)
- [Delta Live Tables — Databricks Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Data Vault Cookbook](../data_vault/)
- [Performance Cookbook — Data Vault Incremental Loading with PySpark MERGE](./performance_cookbook.md)

---

## Adaptive Query Execution (AQE) and Photon Scope

### AQE: What It Covers Automatically

Adaptive Query Execution (AQE) is enabled by default on Databricks Runtime 7.3+. It re-optimises a query plan at runtime using statistics collected during query execution — after shuffle and join operations have produced actual row counts and partition sizes. This is a significant advantage over static query planning, which must estimate these values before execution begins.

AQE performs the following optimisations automatically, without any configuration:

**Dynamic partition coalescing:** After a shuffle, AQE examines the output partition sizes. If many output partitions are small, it merges them into fewer, larger partitions. This eliminates the overhead of thousands of tiny shuffle tasks that would otherwise each consume driver and executor resources to schedule and execute.

**Broadcast join conversion (runtime):** If a sort-merge join was planned because both sides appeared large at planning time, but one side turns out to be small after filtering, AQE replaces the sort-merge join with a broadcast join at runtime. Broadcast joins are significantly faster for small-large joins because they eliminate the shuffle of the larger table.

**Skew join optimisation:** AQE detects partition skew — where one or a few partitions contain a disproportionately large number of rows — and splits those skewed partitions into multiple smaller partitions, parallelising the work and preventing a small number of tasks from becoming bottlenecks.

### AQE: What Requires Configuration

AQE's automatic behaviours have configurable thresholds. The defaults are appropriate for many workloads, but production tuning may require adjusting them:

| Configuration Key | Default | Purpose |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` (on Databricks) | Master toggle — verify this is not disabled on your cluster |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `268435456` (256 MB) | Minimum size for a partition to be considered skewed |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | A partition is skewed if it is this many times larger than the median partition |
| `spark.sql.autoBroadcastJoinThreshold` | `10485760` (10 MB) | Maximum size for a table to be broadcast; increase if large dimension tables are not being broadcast |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1048576` (1 MB) | Minimum size for a coalesced partition |

Skew join optimisation can be disabled for a specific query using the `SKEW_JOIN` hint, which is useful if AQE's skew handling is splitting partitions incorrectly for a known-uniform table:

```sql
SELECT /*+ SKEW_JOIN(orders) */ * FROM orders JOIN customers ON orders.customer_id = customers.id
```

Monitoring: AQE-rewritten query plans are labelled in the Spark UI's SQL tab. Look for "AQE" annotations on nodes to confirm which optimisations were applied.

### Photon: Eligible Operations

Photon is a Databricks-native vectorised query engine written in C++ that replaces the Spark JVM execution engine for supported operations. It is available on clusters with the "Photon" label in the runtime selector and on SQL Warehouses (where it is always active).

Photon provides the largest speedups for operations that process large volumes of data column-by-column:

- SQL queries (SELECT, WHERE, GROUP BY, ORDER BY, HAVING)
- Delta Lake reads and writes (including MERGE INTO, COPY INTO)
- Sort and hash aggregation
- Hash joins (inner, left, right, full outer)
- Window functions
- String operations and date/time functions
- Parquet and Delta file scanning

On eligible workloads, Photon commonly delivers 2–4x speedup over the standard Spark JVM engine, with the largest gains on scan-heavy aggregation queries.

### Photon: Non-Eligible Operations

Photon does not accelerate all operations. Falling back to the JVM engine is automatic and transparent — but understanding the boundaries prevents false expectations about Photon's impact on mixed workloads:

| Operation | Photon Eligible | Notes |
|---|---|---|
| Python UDFs | No | Executes in Python process; bypasses Photon entirely. Rewrite as pandas UDFs (vectorised) or SQL expressions to recover Photon coverage |
| Scala / Java UDFs | No | JVM-native but not Photon-native |
| RDD operations | No | RDD API bypasses the Spark SQL engine entirely |
| Structured Streaming stateful operations | No | Operations with `mapGroupsWithState`, `flatMapGroupsWithState`, and watermark-based aggregations run on the JVM engine |
| Some ML operations (MLlib) | No | MLlib pipelines use JVM-based implementations |
| Python-based Feature Engineering with complex logic | Partial | Spark SQL portions may use Photon; Python UDF portions will not |

The Databricks query history UI for SQL Warehouses shows a Photon indicator per query. For cluster workloads, the Spark UI SQL tab shows which plan nodes were executed by Photon vs. the JVM engine.

### See Also

- [Adaptive Query Execution — Databricks Documentation](https://docs.databricks.com/en/optimizations/aqe.html)
- [Photon Engine — Databricks Documentation](https://docs.databricks.com/en/compute/photon.html)
- [Delta Lake Performance — Databricks Documentation](https://learn.microsoft.com/en-us/azure/databricks/delta/optimize)
- [Performance Cookbook — AQE and Photon](./performance_cookbook.md)
