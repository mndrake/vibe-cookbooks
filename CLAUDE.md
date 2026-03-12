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
    - Explicitly call out any differences in behavior, features, or limitations between Python and SQL approaches (not just syntax)
    - Validation steps (Python and SQL)
    - References to official docs or sample notebooks
- **File Organization:**
  - Organize cookbooks by topic under the `data_architecture/` folder (e.g., `data_architecture/ingestion/`)
  - Use descriptive filenames that reference the topic (e.g., `ingestion_cookbook.md`)
- **Template Usage:**
  - Follow the provided cookbook template for new entries
  - Ensure each new cookbook is actionable and self-contained
  - Always include both Python and SQL examples where possible, and highlight any functional differences

## Example Topics (expand as needed)

- Ingestion (Auto Loader, COPY INTO, Structured Streaming, Notebook Patterns)
- Processing, Summarizing, and Transformation
- Performance Tuning
- Security (RBAC, RLS, masking, etc.)

## References

- [Azure Databricks Documentation](https://learn.microsoft.com/en-us/azure/databricks/)
- [Databricks Tech Talks & Notebooks](https://www.databricks.com/resources/webinar)

## Contribution

Follow the cookbook structure and guidelines above when adding new content. Ensure all examples are tested and working before submitting.
