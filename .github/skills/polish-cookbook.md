---
mode: agent
tools:
  - codebase
  - editFiles
  - fetch
  - runCommands
  - search
description: >
  Drive a cookbook through all three quality gates (structural compliance, critique,
  technical verification) in a loop until all gates pass, then commit.
---

# Skill: polish-cookbook

Orchestrate a multi-pass quality loop on a cookbook. Iterate until structural compliance,
critique, and technical verification all return zero CHANGE findings, then commit.

---

## Setup

1. Ask the user for the file path to polish. If not provided, use the most recently modified
   `*_cookbook.md` under `data_architecture/`.
2. Read `#file:.github/copilot-instructions.md` in full.
3. Read the target cookbook in full.
4. State the file you are polishing and that you are starting iteration 1.

---

## The Loop

Repeat the following sequence up to **3 iterations**. Stop early when the exit condition is met.

---

### Step 1 — Structural Compliance

Check the file against all seven rules. Apply each fix before proceeding to Step 2.

| # | Rule | Pass condition |
|---|------|----------------|
| 1 | **H2 sections are domain types** | No method name is an H2 outside of Introduction / Design Decisions / Dev Pre-Requisites / Infrastructure Pre-Requisites / Managing Your Environment |
| 2 | **Methods are H3 under domain type H2** | Every method appears as `### Method Name` under its parent domain type `## Section` |
| 3 | **Python and SQL in every method H3** | Every method H3 contains both `#### Python` and `#### SQL` with fenced code blocks |
| 4 | **File location and naming** | File is under `data_architecture/{domain}/` named `{topic}_cookbook.md` |
| 5 | **Design Decisions section present** | `## Design Decisions` or `### Decision Criteria` exists with at least one table |
| 6 | **See Also per method** | Every method H3 contains `#### See Also` with at least one link |
| 7 | **No hardcoded credentials** | Only `dbutils.secrets.get(...)` or env var references in code blocks |

---

### Step 2 — Critical Critique

Apply the `critique-cookbook` review inline against the current state of the file.

Evaluate all eight categories:

1. **Completeness** — gaps a reader needs to execute a step
2. **Assumptions** — unstated context not reasonable for this audience
3. **Ambiguity** — statements with two reasonable readings where one misleads
4. **Sequencing** — undeclared dependencies that would cause failure
5. **Error scenarios** — missing guidance for likely failure modes
6. **Inconsistencies** — conflicting terms or config across sections
7. **Tone** — marketing language, unqualified superlatives, advocacy without criteria
8. **Conciseness** — content with no decision or execution value for this audience

For each finding, tag it **CHANGE** or **NO CHANGE**. Collect all CHANGE findings.

---

### Step 3 — Technical Verification

Apply the `verify-cookbook` checks inline against the current state of the file.

For each section, fetch the vendor documentation URLs from its See Also block.
Compare every factual claim against what those URLs actually state.

Flag (as **CHANGE** or **NO CHANGE**):
- Behaviour, defaults, or constraints that differ from vendor documentation
- GA/preview/deprecated status that contradicts vendor documentation
- Code examples using APIs, parameters, or syntax not present in vendor documentation
- See Also URLs that are broken, redirect to unrelated content, or no longer cover the topic
- Best For / Avoid When cells whose underlying factual premise contradicts vendor documentation

If fetching all See Also URLs in one pass is impractical for a long document, work through
the sections sequentially and state which sections were verified in each pass.

---

### Step 4 — Apply Findings

Collect all CHANGE-tagged findings from Steps 2 and 3. Apply them in this order:

1. Tone and conciseness removals (reduces noise for the next verification pass)
2. Completeness and ambiguity additions
3. Sequencing clarifications
4. Technical corrections (behaviour, defaults, code fixes)
5. Broken or stale See Also links (update URL or remove)

After applying all changes, re-read the modified sections to confirm edits are coherent and
have not introduced new inconsistencies.

---

### Step 5 — Iteration Report

Output a concise report:

```
## Iteration N Report

### Structural Compliance
- Checks passed: X / 7
- Fixes applied: [list or "none"]

### Critique
- CHANGE findings applied: X
- NO CHANGE findings noted: Y
- Key changes: [2–4 bullet summary]

### Technical Verification
- CHANGE findings applied: X
- NO CHANGE findings noted: Y
- Key changes: [2–4 bullet summary]

### Exit condition met? YES / NO — reason
```

---

## Exit Condition

Stop iterating when **Steps 2 and 3 both return 0 CHANGE findings** and all seven structural
checks pass. The file is considered polished.

If 3 iterations complete without reaching the exit condition, stop and report:
- Which checks are still failing
- Why they could not be resolved automatically (e.g., requires vendor clarification, ambiguous
  scope, missing upstream information)
- Recommended manual actions before re-running this skill

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

2. Stage and commit:
   ```
   git add {file_path}
   git commit -m "docs: polish {topic} cookbook — {N} iteration(s), {X} findings resolved"
   ```

3. Remind the user that `verify-cookbook` can be re-run independently at any time if upstream
   vendor documentation changes.
