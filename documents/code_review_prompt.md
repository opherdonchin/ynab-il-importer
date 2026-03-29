# YNAB importer pre-review: understandability and performance

## Bottom line

The concern is justified.

This codebase does not just have ordinary rough edges. It shows several strong signs of rapid accretion:

* core behavior spread across large files with mixed responsibilities
* duplicated helper logic in multiple modules
* UI code carrying business rules and data mutation logic
* repeated full-dataframe work in the Streamlit rerun path
* naming and column-shape adaptation layers that make the code harder to reason about

The app slowness concern also looks real. There are at least two clear implementation patterns in the review app that can become expensive enough to feel slow in normal use, especially as review files grow.

---

## What looks like vibe-coding debt rather than deliberate structure

### 1. The review app is too large and too entangled

The clearest example is `src/ynab_il_importer/review_app/app.py`.

* about 2180 lines
* `main()` alone is about 756 lines
* `_render_row_controls()` is about 287 lines

This is not just a style complaint. The file mixes:

* CLI initialization
* file loading
* category refresh from API
* session-state lifecycle
* filter construction
* readiness/save-state derivation
* connected-component validation logic orchestration
* row rendering
* group rendering
* mutation of dataframe state
* review acceptance logic
* save / save-and-quit / quit-wrapper behavior

That means the application’s behavior is hard to understand locally. You often have to hold too much of the file in your head at once.

### 2. Domain logic is trapped inside the UI layer

The review app is not just rendering controls. It is also deciding behavior.

Examples:

* `_component_error_lookup`
* `_blocker_series`
* `_primary_state_series`
* `_apply_review_state`
* `_apply_competing_row_resolution`
* `_allowed_decision_actions`
* parts of `_render_row_controls`

Those are not purely presentational concerns. They are core review semantics. That makes the app harder to test, harder to profile, and harder to trust.

### 3. Duplicate logic suggests drift rather than clean abstraction

There are multiple cases where essentially the same helper logic exists in several modules.

Important examples:

* text normalization helpers repeated in bank identity, card identity, bank reconciliation, card reconciliation, cross-budget reconciliation, upload prep, and map updates
* amount/date parsing helpers duplicated across importers
* CLI/grouping helper families duplicated between `src/ynab_il_importer/cli.py` and `scripts/build_groups.py`
* `connected_component_mask` exists in both `review_app/state.py` and `review_app/validation.py`

The problem is not merely duplication. It is that logic can now diverge silently.

### 4. The selected-column model is harder to reason about than it should be

The review stack carries several overlapping field families:

* `payee_selected` and `category_selected`
* `target_payee_selected` and `target_category_selected`
* `source_payee_selected` and `source_category_selected`

There is also fallback behavior where generic selected columns alias to target-selected columns. This may be intentional for compatibility, but it increases cognitive load and makes it easy to miss which columns are authoritative at each stage.

### 5. Business-critical logic lives in scripts instead of `src/`

`scripts/build_proposed_transactions.py` is about 1164 lines and contains core proposal-generation logic rather than being a thin wrapper.

That script includes:

* source dedupe logic
* rule application logic
* review-row construction
* suggestion generation
* row-id generation
* import-id candidate generation
* institutional target-suggestion logic

This is exactly the kind of file that becomes hard to maintain because it is nominally “just a script” while actually acting like a central library module.

### 6. Some structure looks over-specified without being clarifying

The code often has multiple small helper layers for text conversion, series coercion, fallback columns, and state labels. In principle that can improve clarity. Here it often does the opposite because:

* similar helpers are redefined in several places
* the same dataframe columns are repeatedly re-derived
* the helper graph is large enough that local understanding is difficult

This is not under-structured code. It is unevenly structured code.

---

## Likely real correctness-adjacent readability problems

These are not only aesthetic issues. They can hide bugs.

### 1. Unsafe boolean parsing appears in important paths

Several modules use `astype(bool)` on CSV-backed columns such as `reviewed` or `hidden`.

That is dangerous because string values like `"FALSE"` or `"0"` become truthy under pandas boolean coercion.

Important locations include:

* `src/ynab_il_importer/review_app/state.py`
* `src/ynab_il_importer/map_updates.py`
* `src/ynab_il_importer/upload_prep.py`
* `scripts/prepare_ynab_upload.py`

This is the kind of bug that becomes more likely when code paths are hard to understand and helper conventions are inconsistent.

### 2. Options are represented as separate payee/category lists

The review model often carries `payee_options` and `category_options` independently rather than preserving explicit payee-category pairings.

That makes the UI and review logic easier to build superficially, but it can blur whether all displayed combinations are actually valid combinations.

Even if this is tolerated operationally, the code should force the reviewer to check it explicitly.

### 3. The review semantics are spread across multiple modules with partial overlap

Relevant semantics are distributed across:

* `review_app/io.py`
* `review_app/model.py`
* `review_app/state.py`
* `review_app/validation.py`
* `review_app/app.py`
* `review_reconcile.py`
* `upload_prep.py`

That is not automatically bad, but here the boundaries are not crisp enough. It is easy for one layer to normalize, interpret, or propagate a field slightly differently from another.

---

## Performance findings: likely reasons the app feels slow

## 1. Connected-component validation in the app is an obvious hotspot

The worst current pattern is in the review app blocker computation.

`app.py` does this on each rerun:

* iterate through all rows
* for each unseen row, call `connected_component_mask`
* call `review_component_errors`
* `review_component_errors` calls `connected_component_mask` again

`connected_component_mask` itself scans the whole dataframe while expanding the component.

So the app is doing repeated whole-dataframe graph traversals during normal reruns.

A quick synthetic benchmark of the same pattern took roughly 46 seconds for 4000 rows. That is more than enough to explain a sluggish UI.

This should be treated as a priority performance defect.

### 2. Grouped view repeatedly rescans the full dataframe by fingerprint

In grouped mode, the app effectively does:

* build a fingerprint list
* for each fingerprint on the page, filter the full dataframe again to get that group
* rebuild option summaries by iterating the group rows again
* later repeat similar work for rows within the group

This creates avoidable repeated scans.

A simple pre-grouped structure or cached mapping from fingerprint to row indices would be much cheaper.

### 3. Too much full-dataframe recomputation happens on every Streamlit rerun

At the top of `main()`, the app recomputes many derived masks and labels every interaction:

* summary counts
  n- modified mask
* changed mask
* saved mask
* uncategorized mask
* blocker series
* primary state series
* row kind series
* action series
* suggestion series
* map-update filter series
* search text series
* save-state labels
* inference/progress/persistence tags

Some of this is necessary. But too much of it is done eagerly and globally on every rerun, even when the user is only expanding one row or changing one widget.

### 4. The code repeatedly re-normalizes the same columns

Across app/state/validation/io, the same columns are repeatedly converted with patterns like:

* `.astype("string").fillna("").str.strip()`
* repeated recomputation of id series
* repeated recomputation of selected-value series

This is probably not the dominant bottleneck compared with component validation, but it adds up and makes the performance model harder to reason about.

### 5. Heavy use of dataframe copies in interaction paths

The code frequently does `df.copy()` in paths that run during interactions, including row/group apply flows and some filtering/preparation paths.

Some copies are good defensive practice. But here the pattern suggests the code often takes the safest local route rather than a deliberately profiled route.

### 6. Row rendering work is large and repeated

Both row mode and grouped mode do a lot of per-row work:

* parse semicolon options repeatedly
* build summary text repeatedly
* build category labels repeatedly
* render large detail sections inside expanders

This is not catastrophic by itself, but combined with the expensive global recomputation it likely contributes to the app feeling heavy.

---

## Prioritized issues for the review to target

## Priority 1

### A. Refactor the review app around a real state/model boundary

Goal:

* move review semantics out of `app.py`
* keep `app.py` mostly as wiring and rendering
* centralize selected-column conventions and component validation logic

The reviewer should not stop at “split into smaller functions.” The split should change responsibility boundaries.

### B. Remove repeated whole-dataframe component traversals from the rerun path

Likely direction:

* precompute connected components once per loaded dataframe or once per edit batch
* cache component membership and component-level validation
* avoid recomputing graph structure on every rerun

### C. Replace repeated per-fingerprint full scans in grouped mode

Likely direction:

* build `fingerprint -> row indices` once
* build group summaries once per filtered dataframe
* reuse those structures while rendering the page

### D. Audit every use of `astype(bool)` on string-backed review data

This should be treated as correctness work, not style work.

## Priority 2

### E. Pull proposal-generation business logic out of `scripts/build_proposed_transactions.py`

That script should become a thin CLI wrapper around library code.

### F. Collapse duplicate helper families into shared utilities where it clarifies behavior

Especially:

* text normalization
* boolean normalization
* selected-column handling
* repeated option parsing/joining conventions

### G. Make the review-row contract more explicit and singular

The code should make it obvious which columns are authoritative for:

* review display
* persistence
* reconciliation
* upload prep

## Priority 3

### H. Reduce duplicated state derivations in the app

There are multiple overlapping notions of:

* readiness
* blocker
* changed/modified/saved
* updated/reviewed/settled

Some of this is legitimate, but the reviewer should ask whether the current number of parallel status systems is actually helping.

---

## What I would ask Copilot to do specifically on the understandability side

1. Identify the top 10 files or functions whose size and mixed responsibility create the most comprehension debt.
2. Distinguish necessary domain complexity from accidental complexity.
3. Mark every case where core behavior lives in a script rather than a reusable library module.
4. Identify duplicated helper logic and decide which duplications are worth consolidating.
5. Identify where column compatibility layers and fallback semantics make the state model hard to reason about.
6. Propose concrete extractions that reduce responsibility overlap rather than just chopping functions arbitrarily.

---

## What I would ask Copilot to do specifically on the performance side

1. Profile the review app on a realistically sized CSV, not just tiny test fixtures.
2. Measure grouped mode and row mode separately.
3. Specifically profile:

   * component validation / blocker derivation
   * group construction by fingerprint
   * filter recomputation
   * save path
4. Count repeated full-dataframe scans in the normal rerun path.
5. Identify hot uses of `iterrows`, row-wise `apply`, and repeated string normalization in interaction paths.
6. Separate one-time load costs from per-interaction rerun costs.

---

## My current diagnosis

The repo is not merely messy. It is carrying a real amount of comprehension debt and some avoidable runtime cost.

The strongest evidence is in the review app:

* too much logic in one file
* too much state in too many derived forms
* duplicate validation/state logic
* expensive whole-dataframe work on every rerun

If the Copilot review focuses only on correctness and test pass/fail, it may miss the deeper problem: the code can work and still be too opaque and too heavy for comfortable maintenance.

That is exactly the area where the review should press hardest.
