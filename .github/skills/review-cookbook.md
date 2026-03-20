---
mode: agent
tools:
  - codebase
  - editFiles
  - search
description: Check a cookbook for structural compliance against project conventions.
---

# Skill: review-cookbook

Perform a structural compliance check on a vibe-cookbooks cookbook against the rules in
`#file:.github/copilot-instructions.md`.

## Steps

1. Ask the user for the file path to review. If not provided, identify the most recently modified
   `*_cookbook.md` under `data_architecture/`.

2. Read `#file:.github/copilot-instructions.md` in full.

3. Read the target cookbook file in full.

4. Check the file against each rule below and report **pass** or **fail** per check.
   For every failure include the line number(s) and the corrective action required.

### Checks

| # | Rule | Pass condition |
|---|------|----------------|
| 1 | **Top-level H2 sections are domain types** | Every H2 (except Introduction, Method Selection / Design Decisions, Dev Pre-Requisites, Infrastructure Pre-Requisites, Managing Your Environment) names a domain type — not a method name |
| 2 | **Methods are H3 under a domain type H2** | Each method appears as `### Method Name` under its parent domain type `## Section` |
| 3 | **Python and SQL in every method H3** | Every method H3 contains both `#### Python` and `#### SQL` with fenced code blocks |
| 4 | **File location and naming** | File is under `data_architecture/{domain}/` and named `{topic}_cookbook.md` |
| 5 | **Design Decisions section present** | `## Design Decisions` or `### Decision Criteria` exists and contains at least one table |
| 6 | **See Also per method** | Every method H3 contains `#### See Also` with at least one link |
| 7 | **No hardcoded credentials** | No literal passwords, keys, or connection strings — only `dbutils.secrets.get(...)` or environment variable references |

5. Output a summary table of all checks with pass/fail status, then list each failure with its
   line number and corrective action.

6. If all checks pass, state that the file is compliant and ready for `critique-cookbook`.
