---
name: new-cookbook
description: Scaffold a new Databricks architecture cookbook from the project template. Use when the user wants to create a new cookbook for a topic that does not yet exist.
argument-hint: <topic> [domain-folder]
user-invocable: true
disable-model-invocation: false
---

# new-cookbook

Scaffold a new `*_cookbook.md` for the vibe-cookbooks project.

## Steps

1. Ask the user for:
   - **Topic name** (e.g., "orchestration", "monitoring", "data quality")
   - **Target domain folder** under `data_architecture/` — one of: `ingestion`, `processing`,
     `security`, `performance`, or a new folder name if this is a new domain

2. Read `#file:data_architecture/cookbook_template.md` in full.

3. Read `#file:.github/copilot-instructions.md` and note every structure rule before writing
   anything. Key rules:
   - H2 sections MUST be **domain types** (e.g., "File Ingestion", "Database Ingestion"),
     NEVER method names (e.g., never "Auto Loader", "JDBC" as H2 sections)
   - Each domain type H2 contains one or more method H3 subsections
   - Every method H3 MUST include both `#### Python` and `#### SQL` code blocks
   - File must be named `{topic}_cookbook.md` in `data_architecture/{domain}/`

4. Create the file at `data_architecture/{domain}/{topic}_cookbook.md` using the template
   structure. Pre-populate:
   - Correct H1 title and H2 Databricks label
   - Introduction paragraph (2–3 sentences on scope and self-containment)
   - A `## Design Decisions` section with a stub decision table (Best For / Avoid When columns)
     placed after Introduction and before Dev Pre-Requisites
   - Dev Pre-Requisites and Infrastructure Pre-Requisites sections from the template
   - At least one domain type H2 section with one method H3 subsection containing:
     - `#### Python` — stub fenced code block
     - `#### SQL` — stub fenced code block
     - `#### Discussion and Concerns` — one placeholder bullet
     - `#### See Also` — one placeholder link

5. After creating the file, summarise what was scaffolded and remind the user to:
   - Fill in Problem statements with real scenarios
   - Replace stub code blocks with working Python and SQL examples
   - Complete the Design Decisions table
   - Run `/polish-cookbook` once content is ready — it runs all three quality gates
     (structural compliance, critique, technical verification) in a loop and commits when all pass
   - Alternatively run gates individually: `/review-cookbook` (structure),
     `/critique-cookbook` (completeness and tone), `/verify-cookbook` (accuracy)

6. Stage and commit:
   ```
   git add data_architecture/{domain}/{topic}_cookbook.md
   git commit -m "scaffold: add {topic} cookbook"
   ```
