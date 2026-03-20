# Skill: new-cookbook

You are creating a new Databricks cookbook for the vibe-cookbooks project.

## Steps

1. Ask the user for:
   - **Topic name** (e.g., "orchestration", "monitoring", "data quality")
   - **Target domain folder** under `data_architecture/` — one of: `ingestion`, `processing`,
     `security`, `performance`, or a new folder name if this is a new domain

2. Read `data_architecture/cookbook_template.md` in full.

3. Read `CLAUDE.md` and note every structure rule before writing anything. Key rules:
   - Top-level sections MUST be **domain types** (e.g., "Batch Loads", "Streaming Loads"),
     NEVER method names (e.g., never "Auto Loader", "Kafka", "COPY INTO" as top-level sections)
   - Each domain type section contains one or more **method subsections**
   - Every method section MUST include both **Python and SQL examples** in the same file
   - File must be named `{topic}_cookbook.md` in `data_architecture/{domain}/`

4. Scaffold the file at `data_architecture/{domain}/{topic}_cookbook.md` using the template
   structure. Pre-populate:
   - Correct H1 title and H2 Databricks label
   - Introduction paragraph (2–3 sentences, scope and self-containment)
   - A `## Design Decisions` section with a stub decision table (Best For / Avoid When columns)
     placed after the Introduction and before Dev Pre-Requisites
   - Dev Pre-Requisites and Infrastructure Pre-Requisites sections from the template
   - At least one domain type H2 section with one method H3 subsection, each containing:
     - Problem paragraph
     - Solution with `#### Python` and `#### SQL` sub-sections (stub code blocks)
     - `#### Discussion and Concerns` with one placeholder bullet
     - `#### See Also` with one placeholder link

5. After creating the file, summarise what was scaffolded and remind the user to:
   - Fill in the Problem statements with real scenarios
   - Replace stub code blocks with working Python and SQL examples
   - Complete the Design Decisions table
   - Run `/polish-cookbook` once content is complete — it runs structural compliance,
     critique, and technical verification in a single iterative loop and commits when
     all three gates pass
   - Alternatively, run the gates individually: `/review-cookbook` (structure),
     `/critique-cookbook` (completeness, tone), `/verify-cookbook` (accuracy)

6. Commit with message: `scaffold: add {topic} cookbook`
