# Copilot Project Instructions

## Project Purpose

This project provides practical, step-by-step data architecture cookbooks for Databricks, focusing on free/open resources and hands-on examples in Python and SQL. Cookbooks are organized by architecture domain (e.g., ingestion, processing, performance, security) and are intended to be actionable, modular, and easy to extend.

## Coding and Content Guidelines

- **Resource Selection:** Only use free and open resources. Prefer official Databricks documentation, Databricks Tech Talks, and reputable community notebooks.
- **Language Preference:** Prioritize Python and SQL for all code samples and explanations.
- **Cookbook Structure:**
  - Top-level sections MUST be ingestion types only (e.g., File Ingestion, Streaming Ingestion, Ad Hoc Ingestion). **Do NOT use method names (e.g., Auto Loader, COPY INTO) as top-level sections.**
  - Each ingestion type section contains one or more method subsections (e.g., Auto Loader, COPY INTO, Notebook Pattern)
  - Each method subsection includes:
    - Title and objective
    - Prerequisites (if needed)
    - Step-by-step guide with both Python and SQL examples in the same file whenever possible
    - References to official docs or sample notebooks
- **File Organization:**
  - Organize cookbooks by topic under the `data_architecture/` folder (e.g., `data_architecture/ingestion/`)
  - Use descriptive filenames that reference the topic (e.g., `ingestion_cookbook.md`)
- **Template Usage:**
  - Follow the provided cookbook template for new entries
  - Ensure each new cookbook is actionable and self-contained
  - Always include both Python and SQL examples where possible

## Example Topics (expand as needed)

- Ingestion (Auto Loader, COPY INTO, Structured Streaming, Notebook Patterns)
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

When a topic has both a `*_patterns.md` and a `*_cookbook.md` file, a `*_combined.md` can be created to serve as a self-contained guide that does not require the reader to open two documents.

### When to Create a Combined Guide

Create a combined guide when:
- The patterns document contains **decision tables** whose absence from the cookbook would lead a reader to select the wrong method, wrong configuration, or wrong architecture — resulting in errors, data loss, or avoidable rework.
- The patterns document does **not** merely repeat what the cookbook already says in its Discussion sections.

### Process

1. **Copy the cookbook** as the base file (`cp topic_cookbook.md topic_combined.md`). This avoids timeout risk from writing a large file from scratch.
2. **Edit the title and intro**: update the title from "Cookbook" to "Guide", remove any "companion document" callout that refers the reader to the patterns doc, update the scope note to state the guide is self-contained.
3. **Insert a "Design Decisions" section** after the intro (before the Dev Pre-Requisites section) containing the content selected from the patterns document.
4. **Do not touch the implementation sections** — they are carried over verbatim from the cookbook.

### What to Include from the Patterns Document

Include only content that **prevents the reader from selecting the wrong method or misconfiguring a key option**. In practice this means:

- **Decision tables** (Best For / Avoid When, scenario → recommended approach)
- **Per-method behaviour tables** (e.g., schema evolution behaviour, layer responsibilities, eligibility tables)
- **Minimum required context** to make the tables self-explanatory (one sentence per table at most)

Prefer a table over prose. If the patterns document describes a decision in prose only, convert it to a compact table or a short bullet list before including it.

### What NOT to Include

- Trade-off and rationale prose paragraphs (the reader can consult the patterns doc for depth)
- Architecture diagram callouts
- "Overview" paragraphs describing what the patterns section covers
- "See Also" link lists from patterns sections (the cookbook already has these per method)
- Content already covered in the cookbook's Discussion sections

### Filename and Location

Use the naming convention `{topic}_combined.md` in the same directory as the source files (e.g., `data_architecture/ingestion/ingestion_combined.md`).


