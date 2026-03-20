# GitHub Copilot Instructions — vibe-cookbooks

## Project Purpose

This project provides practical, step-by-step data architecture cookbooks for Databricks on Azure,
using only free and open resources. All content is in Python and SQL. Cookbooks are self-contained,
actionable, and organised by architecture domain.

---

## Cookbook Structure Rules

These rules apply to every `*_cookbook.md` file. Violations must be corrected before a cookbook
is considered complete.

### Section order (every cookbook must follow this sequence)

1. `## Introduction` — scope, audience, quick navigation table
2. `## Design Decisions` — method selection tables only (see below)
3. `## Development Environment Pre-Requisites`
4. `## Infrastructure Pre-Requisites`
5. Domain type `##` sections (e.g., `## File Ingestion`, `## Streaming Ingestion`)
6. `## Managing Your Environment`

### Top-level section naming

- H2 sections MUST be **domain types** (e.g., `## File Ingestion`, `## Database Ingestion`)
- NEVER use a method name as an H2 (e.g., `## Auto Loader`, `## JDBC`, `## Kafka` are all wrong)
- Each domain type H2 contains one or more method H3 subsections

### Method subsection structure

Every method `### Section` must contain:

| Subsection | Required content |
|---|---|
| `#### Python` | Fenced Python code block |
| `#### SQL` | Fenced SQL code block |
| `#### Discussion and Concerns` | Limitations, gotchas, operational considerations |
| `#### See Also` | At least one link to official Databricks or Azure documentation |

### Design Decisions section — what to include

Include only content that prevents selecting the wrong method or misconfiguring a key option:

- Decision tables (Best For / Avoid When, scenario → recommended approach)
- Per-method behaviour tables (schema evolution, eligibility, layer responsibility)
- One sentence of context per table at most

### Design Decisions section — what NOT to include

- Trade-off prose paragraphs
- Architecture diagram callouts
- "Overview" paragraphs
- Content already in a method's Discussion section

---

## File Naming and Location

| Rule | Example |
|---|---|
| Named `{topic}_cookbook.md` | `ingestion_cookbook.md` |
| Located under `data_architecture/{domain}/` | `data_architecture/ingestion/` |

---

## Code Sample Standards

- Always provide both Python and SQL examples in the same file
- Never hardcode credentials — use `dbutils.secrets.get(scope=..., key=...)` or environment variables
- Use official Databricks documentation as the authoritative source for API names and defaults

---

## Accuracy Review Criteria

When fact-checking or reviewing content for accuracy:

- **Change is warranted** — the content is factually wrong, refers to a removed/deprecated feature,
  or would mislead a data architect or senior data engineer working on Azure Databricks
- **No change needed** — the content is accurate, intentionally simplified for the audience, or
  is an acceptable trade-off between precision and readability; state the reason
- When GA/preview status, version-specific behaviour, or recently changed APIs are in question,
  search official Databricks or Azure documentation before deciding

---

## Skills

Reusable prompt files for common tasks are in `.github/skills/`. Open a skill file in Copilot Chat
to run it, or reference it with `#file:.github/skills/{skill}.md`.

| Skill | When to use |
|---|---|
| `new-cookbook.md` | Scaffold a new `*_cookbook.md` from the template |
| `review-cookbook.md` | Check a cookbook for CLAUDE.md structural compliance |
| `critique-cookbook.md` | Review for completeness, tone, assumptions, and conciseness |
| `verify-cookbook.md` | Validate technical claims against vendor documentation |
| `polish-cookbook.md` | Run all three checks in an iterative loop until the cookbook is clean |

---

## References

- [Azure Databricks Documentation](https://learn.microsoft.com/en-us/azure/databricks/)
- [Databricks Tech Talks & Notebooks](https://www.databricks.com/resources/webinar)
- [Cookbook template](../data_architecture/cookbook_template.md)
- [Cookbook recommendations](../data_architecture/cookbook_recommendations.md)
