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

## Contribution

Follow the cookbook structure and guidelines above when adding new content. Ensure all examples are tested and working before submitting.
