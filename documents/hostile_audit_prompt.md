# Prompt addenda for the Copilot review and hostile audit

## Use these additions to sharpen the main review prompt

Add the following section after the existing review themes.

---

## EXTRA REVIEW THEME — COMPREHENSION DEBT AND ACCIDENTAL COMPLEXITY

You must explicitly audit whether the codebase has been over-written, over-engineered, unnecessarily abstracted, or structured in ways that reduce human comprehensibility without delivering corresponding technical benefit.

Do not treat this as a style-only question.

You must identify:

1. files or functions that are too large and mix too many responsibilities
2. business logic trapped in UI code or script wrappers
3. duplicated helper logic that suggests drift or missing abstraction
4. compatibility layers, fallback columns, or normalization conventions that make state hard to reason about
5. areas where the code is more indirect or more modular than the problem actually requires
6. places where the current structure increases the chance of correctness bugs because the authoritative behavior is hard to locate

For each substantial finding, classify it as one of:

* necessary domain complexity
* accidental complexity
* over-abstraction
* duplication/drift
* script-library boundary failure
* UI/business-logic entanglement

Do not stop at saying “this file is large.”
For every high-value finding, explain:

* what makes it hard to understand
* why that matters technically
* what structure would be simpler
* whether the simplification should be done now in this branch

You are explicitly allowed to reduce indirection, remove unnecessary helper layers, collapse gratuitous abstractions, and move business logic into clearer ownership boundaries when the result is materially easier to reason about and well tested.

---

## EXTRA REVIEW THEME — REVIEW-APP PERFORMANCE AND RERUN COST

You must explicitly audit the Streamlit review app for avoidable slowness.

This is not optional.

You must distinguish between:

* one-time file-load cost
* per-rerun cost during normal interaction
* grouped-view rendering cost
* row-view rendering cost
* save-path cost

You must specifically inspect for:

1. repeated full-dataframe scans during normal reruns
2. repeated connected-component or graph-style traversals
3. repeated per-fingerprint regrouping or refiltering
4. repeated string normalization of the same columns on every interaction
5. row-wise `iterrows` or `apply(axis=1)` in hot paths
6. avoidable `df.copy()` usage in interaction loops
7. expensive derived masks or summaries recomputed eagerly instead of reused
8. business logic recomputation that should be cached or precomputed once per loaded dataset

Use a realistically sized synthetic or fixture-based dataset if needed. Do not limit yourself to tiny test fixtures.

For each meaningful hotspot, report:

* exact function(s)
* why the current pattern is slow
* asymptotic shape if obvious
* whether it is user-facing or only batch-time
* the simplest concrete fix
* what regression/performance check should be added

You are allowed to add lightweight profiling or benchmark helpers if they are useful for establishing a performance baseline in this cleanup branch.

---

## EXTRA FILES TO TREAT AS HIGH PRIORITY FOR THIS THEME

In addition to the existing file list, pay special attention to:

* `src/ynab_il_importer/review_app/app.py`
* `src/ynab_il_importer/review_app/state.py`
* `src/ynab_il_importer/review_app/validation.py`
* `src/ynab_il_importer/review_app/io.py`
* `src/ynab_il_importer/upload_prep.py`
* `scripts/build_proposed_transactions.py`
* `scripts/build_cross_budget_review_rows.py`

---

## MANDATORY QUESTIONS FOR THIS REVIEW

You must answer these explicitly in the backlog or final handoff:

1. Which parts of the current structure are genuinely necessary, and which parts are accidental complexity?
2. Is `review_app/app.py` carrying responsibilities that belong elsewhere?
3. Are there duplicated helper families that should be consolidated now?
4. Are there any string-backed boolean fields being interpreted unsafely?
5. What are the top three causes of user-visible slowness in the review app?
6. Which performance fixes are low-risk and high-value enough to do in this branch?
7. Which large files should be decomposed immediately, and along what boundaries?

---

## ADDITIONAL TESTING / VERIFICATION REQUIREMENTS

For this theme, add or perform verification for:

* one realistic review-app smoke dataset that is materially larger than the tiny unit-test fixtures
* at least one check that the review app’s expensive derived state does not regress badly after refactor
* targeted tests for any extracted pure logic from the review app
* explicit tests for string-backed boolean normalization if any unsafe coercion is found

---

## EXTRA STOP CONDITION

Do not declare this review complete if the branch still has obvious user-facing slowness caused by repeated whole-dataframe work in the review app and the issue is merely documented rather than addressed, unless there is a genuine blocking design decision.

---

## Use these additions to sharpen the hostile audit prompt

Add the following section after the current audit priorities.

---

## EXTRA HOSTILE AUDIT PRIORITY — UNNECESSARY COMPLEXITY AND SLUGGISH INTERACTION

Be actively suspicious of code that “works” but is much harder to understand or much slower than it needs to be.

Do not excuse any of the following just because tests pass:

* giant files with mixed responsibilities
* UI code that contains business rules and state mutation semantics
* duplicated helper logic that can silently drift
* script files acting like hidden library modules
* repeated full-dataframe work on every Streamlit rerun
* hot-path use of row-wise pandas patterns where a vectorized or precomputed structure is obvious
* unsafe coercion of string-backed boolean fields

If the branch leaves in place a major source of review-app slowness or comprehension debt without a strong reason, that is a FIX LIST item.

When auditing the review app, be especially skeptical of:

1. component validation and blocker computation
2. grouped-mode construction by fingerprint
3. repeated recalculation of masks, tags, and summaries on each rerun
4. use of `astype(bool)` on CSV-backed fields like `reviewed` or `hidden`
5. partial refactors that move code around without simplifying the ownership model

If you find performance-sensitive code that still does repeated whole-dataframe traversals per interaction, demand one of:

* a real fix
* a convincing measurement showing it is not materially costly

If neither is present, it is not done.

---

## EXTRA FIX-LIST RULE FOR THIS THEME

For any complexity/performance-related FIX LIST item, include:

* whether the problem is user-facing or only maintenance-facing
* why the current structure is harder than necessary
* the simplest acceptable restructuring
* the exact measurement, smoke test, or regression test required before approval

---

## My own top concerns that these addenda are meant to force the review to catch

1. `review_app/app.py` is too large and mixes rendering, state semantics, validation orchestration, and mutation logic.
2. The review app appears to recompute too much full-dataframe state on every rerun.
3. Connected-component validation looks like a likely major hotspot.
4. Grouped mode appears to rebuild groups in a way that can become unnecessarily expensive.
5. There are multiple duplicated helper families across modules.
6. Some important boolean fields appear vulnerable to unsafe string-to-bool coercion.
7. `scripts/build_proposed_transactions.py` still looks like a core library module wearing a script filename.
