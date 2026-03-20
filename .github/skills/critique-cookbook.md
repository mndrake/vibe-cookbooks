---
mode: agent
tools:
  - codebase
  - search
description: Critically review a cookbook for completeness, tone, assumptions, and conciseness.
---

# Skill: critique-cookbook

Critically review a vibe-cookbooks cookbook. Your job is to find gaps, ambiguities, and failures —
not to validate what's there.

## Setup

Ask the user for the file path to review, then read it in full before starting.

---

## Review Instructions

Review the target cookbook strictly from the perspective of its stated audience: data architects
and senior data engineers on Azure Databricks. Do not infer intent, fill gaps with assumed
knowledge, or give the benefit of the doubt. If something is unclear to a reader who only knows
what the document tells them, flag it.

---

## Review Categories

### 1. Completeness

What does a reader need to know to successfully execute each step that the documentation fails
to provide? List every gap explicitly.

### 2. Assumptions

What knowledge or context does the documentation assume without stating? Is each assumption
reasonable for the stated audience?

### 3. Ambiguity

Where could a reader reasonably interpret something two different ways? Which interpretation
would lead them astray?

### 4. Sequencing

Are there dependencies between steps not made explicit? Could someone follow the documented
steps in order and still fail?

### 5. Error Scenarios

Does the documentation tell readers what to do when things go wrong, or only the happy path?

### 6. Inconsistencies

Do any terms, configurations, or conventions conflict with each other across sections?

### 7. Tone and Neutrality

This is internal engineering documentation, not vendor marketing. Flag any language that:

- Promotes a tool rather than describing what it does and its constraints
  (e.g., "preferred choice", "best-in-class", "seamlessly integrates")
- Frames Problem/Solution sections to make a specific vendor feature appear to be the natural
  or only answer, rather than stating the actual business requirement neutrally
- Lists product capabilities as selling points rather than observable technical behaviour
- Uses unqualified superlatives or guarantees ("exactly-once", "automatic schema evolution")
  without stating the conditions under which they hold and those under which they break
- Presents one option as recommended without stating the specific technical criteria that
  justify the preference
- Omits or minimises limitations of a recommended approach while emphasising those of alternatives

For each tone finding, quote the specific language, state why it reads as advocacy, and suggest
the factual description that should replace it.

### 8. Conciseness

Flag content that:

- Explains concepts the stated audience already possesses by definition
- Provides background or motivation that does not change what a reader does or decides
- Repeats information stated elsewhere without adding precision or new context
- Elaborates beyond the detail required to execute or decide
- Includes caveats for scenarios outside documented scope

For each conciseness finding, quote or locate the content and mark it:

| Disposition | Meaning |
|-------------|---------|
| **REMOVE** | Adds no decision or execution value for this audience |
| **CONDENSE** | Has value but is expressed at greater length than this audience requires |

---

## Output Requirements

For each finding across all eight categories, state:

1. **What** the problem is
2. **Where** it occurs (section name and approximate line or paragraph)
3. **Impact** — what a reader would likely do wrong, or what cognitive overhead the content
   imposes and why it does not survive removal

Tag each finding as **CHANGE** (warrants editing the document) or **NO CHANGE** (acceptable
trade-off, intentional simplification for audience, or out of scope).

Do not summarise what the documentation does well. Focus entirely on what would cause a reader
to fail, get stuck, be misled, or waste time.

End with a summary table: Category | Finding # | Where | One-line summary | CHANGE / NO CHANGE.
