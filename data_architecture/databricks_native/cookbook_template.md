# [Cookbook Title] Cookbook

## Databricks Native Stack

> **How to use this template:**
> - Replace all `[bracketed placeholders]` with content specific to your cookbook.
> - The **Development Environment** and **Infrastructure Pre-Requisites** sections are written once per cookbook file.
> - The **Method section** (Problem → Solution → Discussion → See Also) is repeated once per method covered. Copy the full block for each additional method.
> - Remove any subsections that genuinely do not apply, but do not remove them simply because they are hard to fill in.
> - For the equivalent dbt + AutomateDV version of a cookbook, see the template at `../databricks_and_dbt/cookbook_template.md`.

---

## Introduction

This cookbook provides practical, step-by-step guidance for **[ingestion type / processing pattern / domain]** on Databricks using the native Databricks toolchain. It covers [brief description of what patterns or methods are included] and is intended to be self-contained — no prior knowledge of the specific pattern is assumed. No dbt, AutomateDV, or external orchestration tooling is required.

Each method section follows a consistent structure: the problem being solved, the recommended solution with Python and SQL examples, any known concerns or trade-offs, and links to further reading.

---

## Development Environment Pre-Requisites

This section covers what a developer needs installed and configured locally before working with the examples in this cookbook.

### Installing Your Development Environment

> **On Databricks (interactive notebooks or Asset Bundle jobs):** PySpark, Delta Lake (`delta-spark`), Delta Live Tables, and `dbutils` are pre-installed with every Databricks Runtime. No `pip install` is needed to run the code examples in this cookbook on a Databricks cluster.
>
> **Local development:** The tools below are installed on your local machine for CLI operations, Asset Bundle deployment, and running unit tests outside Databricks.

| Tool | Version | Environment | Notes |
|------|---------|-------------|-------|
| Python | 3.10+ | Local dev | Required for the Databricks CLI and local unit tests |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | 0.200+ | Local dev | Used for workspace interaction, secrets management, and Asset Bundle deployment |
| `delta-spark` | Bundled with Databricks Runtime | Databricks (bundled) | Pre-installed; `pip install delta-spark` only needed for local unit testing — version must match your DBR |
| Delta Live Tables runtime | Current channel | Databricks (bundled) | Provided by Databricks — no installation needed |
| [Tool name] | [Version] | [Local dev / Databricks (bundled)] | [Purpose] |

Configure your Databricks CLI connection (local machine):

```bash
# Authenticate using OAuth (recommended for interactive use)
databricks auth login --host https://<your-workspace>.azuredatabricks.net

# Verify authentication
databricks clusters list

# Verify Unity Catalog access
databricks catalogs list
```

### Getting a New Starter Project

```bash
# Initialise a new Databricks Asset Bundle from the default template
# (local machine only — not needed on Databricks clusters)
databricks bundle init

# Configure databricks.yml with your workspace host and cluster details, then:
databricks bundle validate
databricks bundle deploy --target dev
```

### Getting the Source for an Existing Project

```bash
git clone [repository_url]
cd [project_directory]

# Install local dependencies (local machine only — not needed on Databricks clusters)
pip install -r requirements.txt

# Validate and deploy the bundle
databricks bundle validate
databricks bundle deploy --target dev
```

> For details on the project's specific data models, pipelines, or business context, refer to: [link to project-specific documentation].

---

## Infrastructure Pre-Requisites

### Infrastructure Required

The following Databricks and cloud infrastructure is required to run the examples in this cookbook. The associated architectural pattern document provides further design detail — this section is a summary for setup purposes.

| Component | Purpose | Notes |
|-----------|---------|-------|
| Databricks Workspace | Execution environment | Unity Catalog must be enabled |
| [Job cluster / SQL Warehouse / DLT pipeline] | Compute | [Specify type and sizing guidance] |
| [Storage account — ADLS / S3 / GCS] | Source data location | Required for file ingestion examples |
| [Unity Catalog — Catalog / Schema] | Target for output tables | Requires `CREATE TABLE` privilege |
| [Additional component] | [Purpose] | [Notes] |

### [Infrastructure Element — Optional]

> Include this subsection if a specific infrastructure component requires further setup explanation (e.g., configuring an ADLS landing zone path, enabling Change Data Feed on a Delta table, creating a DLT pipeline in the workspace). Remove this subsection if not needed.

[Setup steps or configuration details for the specific infrastructure element.]

---

## [Method Name]

> *Repeat this entire section for each method covered in the cookbook (e.g., Auto Loader, DeltaTable MERGE, Hub Loading, APPLY CHANGES INTO). Use the ingestion type or transformation category as the H2 heading and the specific method as the H3.*

[Brief synopsis — 2–3 sentences describing what this method is, what problem space it operates in, and when a practitioner would reach for it.]

### Problem

[Describe the specific problem or requirement this method addresses. What situation does a data engineer face that leads them to consider this approach? Be concrete — include the data volume, latency, or governance constraint that makes this method relevant.]

### Solution

[Explain how this method solves the problem. Lead with the approach, then provide the implementation.]

#### Python Example

```python
# [Descriptive comment explaining what this block does]

[PySpark code here]
```

#### SQL Example

```sql
-- [Descriptive comment explaining what this block does]

[Spark SQL code here]
```

### Discussion and Concerns

[Any trade-offs, known limitations, edge cases, or open questions that a practitioner should be aware of before using this method in production. Include topics that need further design consideration or decisions that are context-dependent.]

- **[Concern 1]:** [Explanation]
- **[Concern 2]:** [Explanation]
- **[Open question]:** [What needs to be decided or investigated]

### See Also

- [Official Azure Databricks documentation link — descriptive title](URL)
- [Related cookbook or pattern document in this repository](relative_path)
- [Community notebook or Tech Talk](URL)

---

<!-- Repeat the Method section above for each additional method -->

---

## Managing Your Environment

### Monitoring Your Environment in Production

[Describe how to observe this pattern in a running production environment. Include relevant Databricks monitoring surfaces: Databricks Jobs UI, DLT pipeline event logs, Spark UI, Unity Catalog audit logs, or `system.lakeflow` system tables.]

| Signal | Where to Find It | What to Watch For |
|--------|-----------------|-------------------|
| [Job failures] | [Databricks Jobs UI / `system.lakeflow.job_run_timeline`] | [Retry counts, error codes] |
| [Data freshness] | [Table properties / `DESCRIBE HISTORY`] | [Last write timestamp] |
| [Query performance] | [Spark UI / SQL Warehouse query history] | [Slow queries, full scans] |
| [Signal] | [Location] | [Threshold or indicator] |

### Metrics for Success

[Define what "working correctly" looks like for this pattern in production. These should be observable, not aspirational.]

- [ ] [Metric 1 — e.g., "Zero duplicate records in hub tables as validated by primary key uniqueness check"]
- [ ] [Metric 2 — e.g., "Streaming pipeline lag stays below 5 minutes under normal load"]
- [ ] [Metric 3 — e.g., "All PII columns return masked values for non-privileged users"]
- [ ] [Metric 4]
