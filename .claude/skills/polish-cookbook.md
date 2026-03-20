# Skill: polish-cookbook

You are orchestrating a multi-pass quality loop on a vibe-cookbooks cookbook. Your goal is to
drive the file to a state where all three quality gates (structural compliance, critical critique,
technical verification) produce no findings that warrant a change.

---

## Setup

1. Ask the user for the file path to polish. If not provided, use the most recently modified
   `*_cookbook.md` under `data_architecture/`.
2. Read `CLAUDE.md` in full.
3. Read the target cookbook in full.
4. State the file you are polishing and confirm you are starting iteration 1.

---

## The Loop

Repeat the following sequence up to **3 iterations**. Stop early when the exit condition is met.

### Step 1 — Structural Compliance (sequential, fast)

Apply the `/review-cookbook` checks inline:

| # | Rule | Pass condition |
|---|------|----------------|
| 1 | Top-level H2 sections are domain types | No method name (Auto Loader, JDBC, Kafka, etc.) appears as an H2 outside of Introduction / Method Selection / Dev Pre-Requisites / Infrastructure Pre-Requisites / Managing Your Environment |
| 2 | Methods are H3 under a domain type H2 | Every method appears as `### Method Name` under its parent domain type `## Section` |
| 3 | Python and SQL in every method H3 | Every method H3 contains both `#### Python` and `#### SQL` with fenced code blocks |
| 4 | File location and naming | File is under `data_architecture/{domain}/` named `{topic}_cookbook.md` |
| 5 | Design Decisions section present | `## Design Decisions` or `### Decision Criteria` exists with at least one table |
| 6 | See Also per method | Every method H3 contains `#### See Also` with at least one link |
| 7 | No hardcoded credentials | No literal passwords, keys, or connection strings — only `dbutils.secrets.get(...)` or env var references |

For each failure: apply the fix immediately before proceeding to Step 2. If fix requires
structural reorganisation (e.g., an H2 is a method name), do the reorganisation now.

### Step 2 — Parallel Review (spawn two agents simultaneously)

Spawn these two agents **in parallel** using the Agent tool:

**Agent A — Critique**
> "You are a critical technical documentation reviewer. Review `{file_path}` strictly from the
> perspective of data architects and senior data engineers on Azure Databricks.
>
> Evaluate these eight categories and report only findings that warrant a change to the document:
> 1. Completeness — gaps a reader needs to execute a step
> 2. Assumptions — unstated context not reasonable for this audience
> 3. Ambiguity — statements with two reasonable readings where one misleads
> 4. Sequencing — undeclared step dependencies that would cause failure
> 5. Error scenarios — missing guidance for likely failure modes
> 6. Inconsistencies — conflicting terms or config across sections
> 7. Tone — marketing language, unqualified superlatives, advocacy without criteria
> 8. Conciseness — content that adds no decision or execution value for this audience
>
> For each finding: state What / Where (section + line) / Impact.
> Tag each finding as either **CHANGE** (warrants editing the document) or **NO CHANGE**
> (acceptable trade-off, intentional simplification, or outside scope).
> End with a count: X CHANGE findings, Y NO CHANGE findings."

**Agent B — Technical Verification**
> "You are a technical accuracy verifier. Read `{file_path}` in full.
>
> For each section, fetch the vendor documentation URLs listed in its See Also block.
> Compare every factual claim in the section against what those URLs actually state.
>
> Flag any discrepancy where:
> - Behaviour, defaults, or constraints differ between the document and vendor docs
> - A feature is described as GA/preview/deprecated contrary to vendor docs
> - A code example uses an API, parameter, or syntax not present in vendor docs
> - A See Also URL returns 404, redirects to unrelated content, or no longer covers the topic
> - A Best For / Avoid When cell depends on a factual premise contradicted by vendor docs
>
> For each finding: state Document / Section / Claim / Source URL + what it actually says /
> Discrepancy / Impact.
> Tag each finding as **CHANGE** or **NO CHANGE** using the judgment criteria in CLAUDE.md.
> End with a count: X CHANGE findings, Y NO CHANGE findings."

### Step 3 — Apply Findings

Collect all CHANGE-tagged findings from both agents. Apply them to the file in this order:

1. Structural fixes (if any remain from Step 1)
2. Tone and conciseness removals (reduces noise for re-verification)
3. Completeness and ambiguity additions
4. Technical corrections (behaviour, defaults, code fixes)
5. Broken or stale See Also links (update or remove)

After applying all changes, re-read the modified file to confirm edits are coherent and have not
introduced new inconsistencies.

### Step 4 — Iteration Report

Output a concise report for this iteration:

```
## Iteration N Report

### Structural Compliance
- Checks passed: X / 7
- Fixes applied: [list or "none"]

### Critique (Agent A)
- CHANGE findings applied: X
- NO CHANGE findings noted: Y
- Key changes: [2–4 bullet summary]

### Technical Verification (Agent B)
- CHANGE findings applied: X
- NO CHANGE findings noted: Y
- Key changes: [2–4 bullet summary]

### Exit condition met? [YES / NO — reason]
```

---

## Exit Condition

Stop iterating when **both agents in Step 2 return 0 CHANGE findings** and all structural
checks pass. The file is considered polished.

If 3 iterations complete without reaching the exit condition, stop and report:
- Which checks are still failing
- Why they could not be resolved automatically (e.g., requires vendor clarification, missing
  upstream data, ambiguous scope)
- Recommended manual actions

---

## Completion

When the exit condition is met:

1. Output a final summary:
   ```
   ## Polish Complete

   File: {file_path}
   Iterations required: N
   Total CHANGE findings applied: X (critique: A, verification: B, structural: C)

   The cookbook passes all three quality gates and is ready to commit.
   ```

2. Commit with message: `docs: polish {topic} cookbook — {N} iteration(s), {X} findings resolved`

3. Remind the user that `/verify-cookbook` can be re-run independently at any time if upstream
   vendor documentation changes.
