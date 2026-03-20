# Copilot Project Instructions

## Project Purpose

This project provides practical, step-by-step data architecture cookbooks for Databricks, focusing on free/open resources and hands-on examples in Python and SQL. Cookbooks are organized by architecture domain (e.g., ingestion, processing, performance, security) and are intended to be actionable, modular, and easy to extend.

## Coding and Content Guidelines

- **Resource Selection:** Only use free and open resources. Prefer official Databricks documentation, Databricks Tech Talks, and reputable community notebooks.
- **Language Preference:** Prioritize Python and SQL for all code samples and explanations.
- **Cookbook Structure:**
  - Top-level sections MUST be domain types (e.g., File Ingestion, Streaming Ingestion, Database Ingestion). **Do NOT use method names (e.g., Auto Loader, COPY INTO) as top-level sections.**
  - Each domain type section contains one or more method subsections (e.g., Auto Loader, COPY INTO, JDBC)
  - Each method subsection includes:
    - Title and objective
    - Prerequisites (if needed)
    - Step-by-step guide with both Python and SQL examples in the same file whenever possible
    - References to official docs or sample notebooks
- **File Organization:**
  - Organize cookbooks by topic under the `data_architecture/` folder (e.g., `data_architecture/ingestion/`)
  - Use descriptive filenames that reference the topic (e.g., `ingestion_combined.md`)
- **Template Usage:**
  - Follow the provided cookbook template for new entries
  - Ensure each new cookbook is actionable and self-contained
  - Always include both Python and SQL examples where possible

## Example Topics (expand as needed)

- Ingestion (Auto Loader, COPY INTO, Structured Streaming, JDBC, Lakeflow Connect)
- Processing, Summarizing, and Transformation
- Performance Tuning
- Security (RBAC, RLS, masking, etc.)

## References

- [Azure Databricks Documentation](https://learn.microsoft.com/en-us/azure/databricks/)
- [Databricks Tech Talks & Notebooks](https://www.databricks.com/resources/webinar)

## Reviewing Critical or Accuracy Items

When given a critical or accuracy review task (e.g., fact-checking, validating technical claims, reviewing deprecated features, or assessing whether content is still current):

1. **Analyze the item in context.** Consider the target audience (data architects, engineers) and the purpose of the document (practical, actionable guidance for Databricks on Azure).
2. **Use web search when needed.** If the accuracy of a claim cannot be determined from existing knowledge — particularly for version-specific features, GA/preview status, API changes, or newly released capabilities — perform a web search against official Databricks or Azure documentation to verify.
3. **Apply reasonable judgment.** Not every minor wording difference warrants a change. Use the following criteria:
   - **Change is warranted** if the content is factually incorrect, refers to a deprecated/removed feature, or would mislead the target audience.
   - **No change needed** if the content is still accurate, is intentionally simplified for the audience, or reflects an acceptable trade-off between precision and readability. In this case, briefly state why no change is required.
4. **Document the rationale.** Whether or not a change is made, state the finding and the reasoning so the reviewer understands the decision.

## Combined Guide Documents

All guides in this project are `*_combined.md` files — self-contained documents that include both method selection guidance (decision tables) and implementation steps. There are no separate `*_patterns.md` or `*_cookbook.md` files.

New guides are created directly as `{topic}_combined.md` using `data_architecture/cookbook_template.md` as the base. See the `/new-cookbook` skill for a guided scaffolding workflow.

### Structure

Every combined guide follows this section order:

1. **Introduction** — scope, audience, quick navigation table
2. **Design Decisions** — method selection tables (Best For / Avoid When, schema evolution behaviour, batch vs. streaming, etc.) placed before implementation sections
3. **Development Environment Pre-Requisites**
4. **Infrastructure Pre-Requisites**
5. **Domain type sections** (e.g., File Ingestion, Streaming Ingestion) — each containing method subsections
6. **Managing Your Environment** — monitoring, troubleshooting, common errors

### What to Include in the Design Decisions Section

Include only content that **prevents the reader from selecting the wrong method or misconfiguring a key option**:

- **Decision tables** (Best For / Avoid When, scenario → recommended approach)
- **Per-method behaviour tables** (e.g., schema evolution behaviour, layer responsibilities, eligibility tables)
- **Minimum required context** to make the tables self-explanatory (one sentence per table at most)

Prefer a table over prose. If a decision can be described in prose only, convert it to a compact table or a short bullet list.

### What NOT to Include in Design Decisions

- Trade-off and rationale prose paragraphs
- Architecture diagram callouts
- "Overview" paragraphs
- Content already covered in the method's Discussion sections

### Filename and Location

Use the naming convention `{topic}_combined.md` in `data_architecture/{domain}/` (e.g., `data_architecture/ingestion/ingestion_combined.md`).


