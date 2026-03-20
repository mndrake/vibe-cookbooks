---
name: verify-cookbook
description: Validate every technical claim in a cookbook against vendor documentation. Use when the user wants to fact-check a cookbook's accuracy, check for deprecated APIs, or confirm that Best For/Avoid When cells are supported by vendor documentation.
argument-hint: <file-path> [companion-file-path]
user-invocable: true
disable-model-invocation: false
---

# verify-cookbook

Validate the technical accuracy of a vibe-cookbooks cookbook against the vendor documentation
linked within it.

## Setup

Ask the user for the file path to verify. If they also provide a companion document path,
confirm you will check cross-document consistency as a final pass.

---

## How to Work

1. Read the entire document first. Build an inventory of every section and every See Also link.

2. If the document contains decision matrices, comparison tables, or feature-support tables,
   treat every cell as an independent factual claim. A table comparing six methods across eight
   dimensions contains 48 verifiable assertions — do not skim it.

3. Work through the document section by section. For each section:
   a. Identify every factual claim about tool behaviour, configuration options, defaults,
      limitations, prerequisites, error handling, feature support or absence, comparative
      performance or cost characteristics, and conditions under which a method should or
      should not be used.
   b. Fetch the vendor documentation URLs from that section's See Also block and any inline links.
   c. Read the fetched documentation carefully.
   d. Compare each claim against what the vendor documentation actually states.

4. If a section's linked documentation does not cover a specific claim, say so explicitly —
   "the linked documentation does not address this claim" is a finding, not a pass.

5. If a vendor URL returns a 404, redirects, or has been restructured so the content no longer
   matches what the document references, flag it as a broken or stale reference.

6. If a companion document was provided, verify cross-document consistency after completing
   the per-document checks. For every method or feature discussed in both documents, confirm
   factual claims (behaviour, prerequisites, limitations, GA/preview status) are stated
   consistently. Contradictions between the two documents are findings even if each is
   individually consistent with vendor documentation.

---

## What to Flag

Flag any case where:

- The document states a behaviour and vendor documentation states a different behaviour. Quote both.
- The document states a default value and vendor documentation states a different default.
- The document claims a feature exists (or does not) and vendor documentation says otherwise.
- The document describes prerequisites or constraints that vendor documentation contradicts.
- The document states something is GA, preview, deprecated, or unavailable and vendor
  documentation indicates a different status.
- A code example uses an API, method name, option key, or syntax that vendor documentation
  does not recognise or has deprecated.
- A See Also link is broken, redirects to unrelated content, or no longer covers the topic.
- A Best For / Avoid When cell depends on a factual premise contradicted by vendor documentation.
- A trade-off assertion makes a comparative claim without vendor documentation supporting the
  comparison, or where vendor documentation qualifies the comparison with conditions the
  document omits.

Do NOT flag:

- Stylistic paraphrasing where meaning is equivalent to vendor documentation
- Information the document adds beyond vendor docs, unless it contradicts them
- Architectural opinions or recommendations, unless they assert incorrect tool behaviour

---

## How to Report Findings

For each finding state:

- **Section** — which section of the document contains the claim
- **Claim** — quote or closely paraphrase the specific statement
- **Source** — the vendor documentation URL and what it actually says (quote or paraphrase)
- **Discrepancy** — a plain statement of the contradiction
- **Impact** — what a reader would do wrong if they trust the document over vendor documentation

Tag each finding as **CHANGE** (factually incorrect or misleading for the audience) or
**NO CHANGE** (intentional simplification that is not materially misleading).

If you complete a section and find no discrepancies, state: "Verified N claims in [Section] —
no contradictions found." Do not elaborate on what went well.

At the end, provide a summary table:
Section | Claim (brief) | Discrepancy (brief) | CHANGE / NO CHANGE
