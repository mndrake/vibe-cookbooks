# Skill: review-cookbook

You are performing a structural compliance check on a vibe-cookbooks cookbook against the rules
in `CLAUDE.md`.

## Steps

1. Ask the user for the file path to review. If not provided, check the most recently modified
   `*.md` file under `data_architecture/`.

2. Read `CLAUDE.md` in full.

3. Read the target file in full.

4. Check the file against each rule below and report **pass** or **fail** per check.
   For every failure, include the line number(s) and the corrective action required.

### Checks

| # | Rule | Pass condition |
|---|------|---------------|
| 1 | **Top-level section names are domain types** | Every H2 section (except Introduction, Method Selection, Dev Pre-Requisites, Infrastructure Pre-Requisites, Managing Your Environment) names a domain type (e.g., "File Ingestion", "Streaming Ingestion"), NOT a method name (e.g., "Auto Loader", "Kafka") |
| 2 | **Method subsections are H3 under a domain type H2** | Each method (Auto Loader, COPY INTO, JDBC, etc.) appears as `### Method Name` under its parent domain type `## Section` |
| 3 | **Python and SQL examples in every method section** | Every method H3 section contains both a `#### Python` and a `#### SQL` subsection with a fenced code block |
| 4 | **File location and naming** | File is under `data_architecture/{domain}/` and named `{topic}_cookbook.md` or `{topic}_combined.md` |
| 5 | **Design Decisions section present** | A `## Design Decisions` or `### Decision Criteria` section exists and contains at least one table |
| 6 | **See Also present per method** | Every method H3 section contains a `#### See Also` subsection with at least one link |
| 7 | **No hardcoded credentials** | No literal connection strings, passwords, or access keys in code blocks (only `dbutils.secrets.get(...)` or environment variable references) |

5. Output a summary table of all checks with pass/fail status, then list each failure with its
   line number and corrective action.

6. If all checks pass, state that the file is compliant and ready for `/critique-cookbook`.
