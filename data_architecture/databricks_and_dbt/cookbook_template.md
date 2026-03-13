# [Cookbook Title] Cookbook

> **How to use this template:**
> - Replace all `[bracketed placeholders]` with content specific to your cookbook.
> - The **Development Environment** and **Infrastructure Pre-Requisites** sections are written once per cookbook file.
> - The **Method section** (Problem → Solution → Discussion → See Also) is repeated once per method covered. Copy the full block for each additional method.
> - Remove any subsections that genuinely do not apply, but do not remove them simply because they are hard to fill in.

---

## Introduction

This cookbook provides practical, step-by-step guidance for **[ingestion type / processing pattern / domain]** on Databricks. It covers [brief description of what patterns or methods are included] and is intended to be self-contained — no prior knowledge of the specific pattern is assumed.

Each method section follows a consistent structure: the problem being solved, the recommended solution with Python and SQL examples, any known concerns or trade-offs, and links to further reading.

---

## Development Environment Pre-Requisites

This section covers what a developer needs installed and configured locally before working with the examples in this cookbook.

### Installing Your Development Environment

> **On Databricks (interactive notebooks or jobs):** PySpark and Delta Lake are pre-installed with every Databricks Runtime — no `pip install` is needed to run the code examples on a cluster.
>
> **dbt execution environments:**
> - **Local development:** `dbt-databricks` runs on your local machine, submitting SQL to a Databricks SQL Warehouse via `http_path` in `~/.dbt/profiles.yml`.
> - **Databricks Asset Bundle jobs (production):** When deployed via `databricks bundle deploy`, dbt runs on a Databricks single-node job cluster (`num_workers: 0`) or serverless environment. The bundle installs `dbt-databricks` as a pypi library; workspace credentials are injected automatically. SQL is submitted to a SQL Warehouse.
>
> For production Asset Bundle deployments, only the Databricks CLI is required locally — `dbt-databricks` is installed on the job cluster by the bundle.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.9+ | Local dev | Required for dbt-databricks and the Databricks CLI |
| [dbt-databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup) | Latest | Local dev | Only required for dbt-based examples — runs locally, connects to Databricks |
| Databricks CLI | Latest | Local dev | Used for secrets management and workspace interaction |
| [Tool name] | [Version] | [Local dev / Databricks (bundled)] | [Purpose] |

Configure your Databricks connection (local machine):

```bash
# Authenticate the Databricks CLI
databricks configure --token

# Verify connection
databricks clusters list
```

For dbt, configure `~/.dbt/profiles.yml`:

```yaml
[profile_name]:
  target: dev
  outputs:
    dev:
      type: databricks
      host: [your-workspace-host]
      http_path: [your-sql-warehouse-http-path]
      token: "{{ env_var('DBT_TOKEN') }}"
      schema: [your_dev_schema]
```

### Getting a New Starter Project

To start a new project from scratch:

```bash
# Clone the starter repository or scaffold a new dbt project
# [Insert organisation-specific starter project instructions here]

dbt init [project_name]
cd [project_name]
dbt debug  # Verify connection
```

### Getting the Source for an Existing Project

To work with an existing project:

```bash
git clone [repository_url]
cd [project_directory]
pip install -r requirements.txt   # Install Python dependencies
dbt deps                           # Install dbt package dependencies (if applicable)
dbt debug                          # Verify connection and configuration
```

> For details on the project's specific data models, pipelines, or business context, refer to: [link to project-specific documentation].

---

## Infrastructure Pre-Requisites

### Infrastructure Required

The following Databricks and cloud infrastructure is required to run the examples in this cookbook. The associated architectural pattern document provides further design detail — this section is a summary for setup purposes.

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment | Unity Catalog must be enabled for security examples |
| [Cluster / SQL Warehouse] | Compute | [Specify type and sizing guidance] |
| [Storage account — ADLS / S3 / GCS] | Source data location | Required for file ingestion examples |
| [Unity Catalog — Catalog / Schema] | Target for output tables | Requires `CREATE TABLE` privilege |
| [Additional component] | [Purpose] | [Notes] |

### [Infrastructure Element — Optional]

> Include this subsection if a specific infrastructure component requires further setup explanation (e.g., configuring an Event Hub namespace, setting up a Unity Catalog metastore, enabling Change Data Feed on a Delta table). Remove this subsection if not needed.

[Setup steps or configuration details for the specific infrastructure element.]

---

## [Method Name]

> *Repeat this entire section for each method covered in the cookbook (e.g., Auto Loader, COPY INTO, Hub Loading, MERGE INTO). Use the ingestion type or transformation category as the H2 heading and the specific method as the H3.*

[Brief synopsis — 2–3 sentences describing what this method is, what problem space it operates in, and when a practitioner would reach for it.]

### Problem

[Describe the specific problem or requirement this method addresses. What situation does a data engineer face that leads them to consider this approach? Be concrete — include the data volume, latency, or governance constraint that makes this method relevant.]

### Solution

[Explain how this method solves the problem. Lead with the approach, then provide the implementation.]

#### Python Example

```python
# [Descriptive comment explaining what this block does]

[Python / PySpark code here]
```

#### SQL Example

```sql
-- [Descriptive comment explaining what this block does]

[SQL code here]
```

### Discussion and Concerns

[Any trade-offs, known limitations, edge cases, or open questions that a practitioner should be aware of before using this method in production. Include topics that need further design consideration or decisions that are context-dependent.]

- **[Concern 1]:** [Explanation]
- **[Concern 2]:** [Explanation]
- **[Open question]:** [What needs to be decided or investigated]

### See Also

- [Official Databricks documentation link — descriptive title](URL)
- [Related cookbook or pattern document in this repository](relative_path)
- [Community notebook or Tech Talk](URL)

---

<!-- Repeat the Method section above for each additional method -->

---

## Managing Your Environment

### Monitoring Your Environment in Production

[Describe how to observe this pattern in a running production environment. Include relevant Databricks monitoring surfaces: job run history, pipeline event logs, DLT pipeline UI, Spark UI, query history in SQL Warehouse, or Unity Catalog audit logs.]

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| [Job failures] | [Databricks Jobs UI / `system.lakeflow`] | [Retry counts, error codes] |
| [Data freshness] | [Table properties / `DESCRIBE HISTORY`] | [Last `OPTIMIZE` or write timestamp] |
| [Query performance] | [SQL Warehouse query history] | [Slow queries, full scans] |
| [Signal] | [Location] | [Threshold or indicator] |

### Metrics for Success

[Define what "working correctly" looks like for this pattern in production. These should be observable, not aspirational.]

- [ ] [Metric 1 — e.g., "Zero duplicate records in hub tables as validated by `unique_combination_of_columns` test"]
- [ ] [Metric 2 — e.g., "Streaming pipeline lag stays below 5 minutes under normal load"]
- [ ] [Metric 3 — e.g., "All PII columns return masked values for non-privileged users"]
- [ ] [Metric 4]
