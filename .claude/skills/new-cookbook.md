# Skill: new-cookbook

You are researching and writing a complete, production-ready Databricks cookbook for the vibe-cookbooks project. Do not scaffold and leave placeholders — write real content.

## Steps

### 1. Gather inputs

Ask the user for:
- **Topic name** (e.g., "orchestration", "monitoring", "data quality")
- **Target domain folder** under `data_architecture/` — one of: `ingestion`, `processing`, `security`, `performance`, or a new folder name

### 2. Read project standards

Read both files in full before writing anything:
- `data_architecture/cookbook_template.md`
- `CLAUDE.md`

Key structural rules from CLAUDE.md (do not violate these):
- Top-level sections MUST be **domain types** (e.g., "Batch Loads", "Streaming Loads") — NEVER method names as H2 headings
- Each domain type H2 contains one or more **method H3 subsections**
- Every method section MUST include both **Python and SQL examples** in the same file
- File named `{topic}_cookbook.md` in `data_architecture/{domain}/`

### 3. Research the topic

Use web search to gather current, accurate information. Search for:
- Official Azure Databricks documentation for each method in scope (verify GA vs preview status)
- Databricks Tech Talk notebooks or blog posts for real-world patterns and code examples
- Known limitations, gotchas, and production concerns for each method
- Current API names, parameter names, and syntax (avoid deprecated APIs)

Run at least 3–5 searches covering different methods or aspects of the topic. Prefer:
- `learn.microsoft.com/azure/databricks`
- `docs.databricks.com`
- Databricks engineering blog and Tech Talk notebooks

### 4. Plan the cookbook structure

Before writing, decide:
- What are the **domain type sections** (H2) — these are categories of problems, not method names
- What **methods** (H3) belong under each domain type
- What Design Decision tables are needed to help readers choose the right method

Ensure at least 2 domain type sections and at least 2 methods per section where the topic supports it.

### 5. Write the complete cookbook

Create `data_architecture/{domain}/{topic}_cookbook.md` using the template structure. Every section must be fully written — no `[placeholder]` text, no stub code blocks, no "fill this in later" comments.

#### Introduction
Write 3–4 sentences: what the cookbook covers, who it is for, what Databricks capabilities it uses, and that it is self-contained.

Include a navigation table listing each domain type section and its methods.

#### Design Decisions section
Place this after the Introduction and before Dev Pre-Requisites. Include:
- A **method selection table** with columns: Method | Best For | Avoid When
- Any per-method behaviour tables that prevent misconfiguration (e.g., schema evolution behaviour, compute type eligibility)
- No prose paragraphs — tables and short bullet lists only

#### Development Environment Pre-Requisites
Copy the standard section from the template. Add any topic-specific tools (e.g., Kafka client libraries, specific Python packages) with real version numbers.

#### Infrastructure Pre-Requisites
List the real infrastructure components required for the examples in this specific cookbook. Be concrete — name the Unity Catalog object types, compute types, and storage account configuration needed.

#### Domain type sections (H2) and method subsections (H3)

For each method, write:

**Problem** — A concrete scenario a data engineer faces. Include specifics: data volume, latency requirement, governance constraint, or operational context that makes this method relevant. 2–4 sentences.

**Solution** — Explain the approach, then provide complete, working code.

- `#### Python Example` — A complete PySpark or Python code block that actually runs. Use realistic table names, paths, and variable names. Include all necessary imports. Add inline comments explaining non-obvious lines.
- `#### SQL Example` — An equivalent or complementary Spark SQL block. If a pure SQL equivalent does not exist, provide a SQL block that covers the closest SQL surface (e.g., `COPY INTO`, `MERGE`, DDL) and note what differs from the Python path.

**Discussion and Concerns** — 3–5 real bullets covering production concerns, known limitations, cost implications, or design decisions the reader must make. Base these on the research findings.

**See Also** — 2–4 real links to official docs pages, Databricks Tech Talk notebooks, or related cookbooks in this repo. Use the actual URLs found during research.

#### Managing Your Environment
Write real monitoring signals for this topic. Fill in the monitoring table with actual Databricks system tables, UI surfaces, and metric names relevant to the cookbook's methods. Write 3–5 real "Metrics for Success" checkboxes.

### 6. Self-review before saving

Before writing the file, verify:
- [ ] No `[bracketed placeholder]` text remains anywhere
- [ ] Every code block contains real, runnable code (not `[PySpark code here]`)
- [ ] Every `See Also` section has real URLs (found during research)
- [ ] All H2 headings are domain types, not method names
- [ ] Both Python and SQL examples exist in every method section
- [ ] Design Decisions section contains only tables/bullets, no prose paragraphs
- [ ] The Design Decisions section appears before Dev Pre-Requisites

### 7. Commit

Commit with message: `feat: add complete {topic} cookbook`

Do not remind the user to fill anything in — the cookbook should be complete and ready for the `/polish-cookbook` review pass.
