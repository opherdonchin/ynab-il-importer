# Cleanup Implementation Plan — Post-Audit Pass

Branch: `code-review-refactor` at commit `aa347a9`
Updated: 2026-03-30

This plan replaces the prior cleanup plan. It is based on:
- the hostile audit report (`documents/hostile_audit_report.md`)
- the current codebase state after the first Codex pass

## What Was Already Done (Phases 0–5 from the prior plan)

All completed. Key outcomes:
- ✅ 200 tests passing
- ✅ `safe_types.py` with `normalize_flag_series` replaces all unsafe CSV-backed `astype(bool)`
- ✅ `connected_component_mask` single-sourced in `validation.py`
- ✅ `precompute_components` and `precompute_component_errors` exist in `validation.py`
- ✅ Fingerprint groupby precomputation in grouped mode
- ✅ `changed_mask` NaN-comparison bug fixed
- ✅ 6 business logic functions extracted from `app.py` to `state.py`, `validation.py`, `model.py`
- ✅ 31 new/expanded tests covering extracted functions

## What the Hostile Audit Found

The first pass improved intra-call efficiency (components computed per-component rather than per-row) but did NOT cache results between Streamlit reruns. `blocker_series()` triggers O(n²) component graph recomputation on every user interaction: every filter change, scroll, or view switch.

**This is the gating issue.** The branch cannot be declared reviewable until the dominant user-facing performance cost is addressed, not just documented.

## Cleanup Goals (this pass)

1. Eliminate repeated whole-dataframe component traversal on non-mutation reruns.
2. Avoid redundant `precompute_components` calls within a single user interaction.
3. Extract the last business logic functions from `app.py`.
4. Add a performance regression tripwire.
5. Keep each change small, tested, and independently verifiable.

## Guardrails

1. Do not change external behavior.
2. Preserve all existing tests. Add focused tests for each change.
3. Do not refactor rendering structure (main() decomposition, `_render_row_controls` split). That is deferred.
4. Do not spread into unrelated domains.

## Execution Order

### Task 1: Cache derived series between non-mutation reruns (CRITICAL)

**Problem:** `blocker_series()` calls `component_error_lookup()` → `precompute_components()` → `connected_component_mask()` on every Streamlit rerun. Cost is O(n²) for pair-based review data. The component graph only changes when the DataFrame is mutated.

**Implementation:**

1. Add a `_df_generation` counter to `st.session_state`, initialized to `0`.
2. Increment `_df_generation` at every point in `app.py` where `st.session_state["df"]` is assigned or mutated. There are currently 6 such sites:
   - `_load_df()` (line ~211)
   - `_render_row_controls()` form submission (line ~1100)
   - "Accept all set decisions" button (line ~1291)
   - Grouped-mode "Apply group values" (line ~1788)
   - Any save path that reloads the DataFrame
   These are easy to identify: search for `st.session_state["df"] =`.
3. At the top of `main()`, before the 17-series computation block (line ~1157), check:
   ```python
   current_gen = st.session_state.get("_df_generation", 0)
   cached_gen = st.session_state.get("_series_generation", -1)
   if current_gen != cached_gen:
       # recompute all derived series
       ...
       st.session_state["_series_generation"] = current_gen
       st.session_state["_cached_series"] = { ... }
   else:
       # reuse cached series
       ... = st.session_state["_cached_series"]
   ```
4. Store in `_cached_series`: `blocker_series`, `primary_state_series`, `row_kind_series`, `action_series`, `suggestion_series`, `map_update_series`, `search_text`, `uncategorized_mask`, `inference_tag`, `progress_tag`, `save_state`, `persistence_tag`, `changed_mask`, `reviewed_mask`, `updated_mask`, `saved_mask`, `unsaved_mask`, `inconsistent`, and the numeric counts (`counts`, `modified`, `base_count`, `updated_confirmed_count`, `saved_reviewed_count`, `uncategorized_count`).
5. Also stash `_cached_component_map` from the `blocker_series` computation so Task 2 can reuse it.

**How to stash the component map:** Modify `blocker_series()` in `validation.py` to return a tuple `(series, component_map)` instead of just the series. Update the single call site in `app.py`. Do NOT change the function signature — add a new function `blocker_series_with_components(df)` that returns `(blocker_series, component_map)`, and keep `blocker_series(df)` as a thin wrapper that discards the map.

**Files to modify:**
- `src/ynab_il_importer/review_app/app.py` — caching logic at top of `main()`, generation increment at mutation points
- `src/ynab_il_importer/review_app/validation.py` — add `blocker_series_with_components()`

**Definition of done:**
- Non-mutation Streamlit reruns (filter changes, scrolling, view switches) do not call `precompute_components()`.
- All 200+ tests still pass.
- A new test confirms that calling `blocker_series_with_components` returns both the series and a dict mapping indices to component IDs.

### Task 2: Pass cached component map to `apply_review_state` (MODERATE)

**Problem:** `apply_review_state()` calls `precompute_components()` from scratch at line 359 of `validation.py`. When called from `app.py`, the component map was already computed by `blocker_series()` earlier in the same run.

**Implementation:**

1. Add an optional `component_map: dict[Any, int] | None = None` parameter to `apply_review_state()`.
2. When provided, skip the internal `precompute_components()` call and use the cached map.
3. When not provided (e.g. in tests), fall back to computing it internally (current behavior).
4. At the 3 call sites in `app.py` (lines ~1094, ~1291, ~1788), pass `st.session_state.get("_cached_component_map")`.

**Files to modify:**
- `src/ynab_il_importer/review_app/validation.py` — add optional parameter to `apply_review_state()`
- `src/ynab_il_importer/review_app/app.py` — pass cached map at call sites

**Definition of done:**
- `precompute_components()` is called at most once per user interaction in the review-then-save flow.
- Existing `apply_review_state` tests still pass without providing the parameter.

### Task 3: Extract remaining business logic from app.py (LOW)

**Problem:** Three pure business logic functions live in `app.py` and cannot be tested without importing it.

**Implementation:**

1. Move `_derive_inference_tags(df)` to `review_app/state.py` as `derive_inference_tags(df)`.
2. Move `_initial_inference_tags(df, base)` to `review_app/state.py` as `initial_inference_tags(df, base)`.
3. Move `_apply_row_filters(df, ...)` to `review_app/state.py` as `apply_row_filters(df, ...)`.
4. Replace the definitions in `app.py` with imports from `review_app.state`.
5. Add one test per extracted function in `tests/test_review.py` or `tests/test_review_app.py`.

**Files to modify:**
- `src/ynab_il_importer/review_app/state.py` — add 3 functions
- `src/ynab_il_importer/review_app/app.py` — replace definitions with imports

**Definition of done:**
- Functions are importable and testable from `review_app.state`.
- All tests pass.

### Task 4: Add performance regression test (MODERATE)

**Problem:** No benchmark or smoke test exists for the review app's expensive derived state. Future performance regressions are invisible.

**Implementation:**

1. Create `tests/test_review_perf.py`.
2. Add a fixture factory `make_review_df(n)` that generates `n` review rows with realistic column shapes: `source_row_id`, `target_row_id`, `fingerprint`, `decision_action`, `source_payee_selected`, `target_payee_selected`, `source_category_selected`, `target_category_selected`, `reviewed`, `workflow_type`, `match_status`. Rows should form ~n/2 two-row components (paired by source/target IDs) to simulate real pair-based data.
3. Add a test that:
   - Creates a 500-row dataset using the factory
   - Calls `blocker_series()` (or `blocker_series_with_components()`)
   - Asserts completion within 10 seconds (generous bound; the real expectation is <2s)
   - Asserts the result has the correct length and dtype
4. Add a test that calls the full series computation block (all 17 series) on the 500-row dataset and asserts completion within a combined wall-clock bound.

**Files to create:**
- `tests/test_review_perf.py`

**Definition of done:**
- `pixi run pytest tests/test_review_perf.py` passes.
- The test serves as a regression tripwire for future changes.

### Task 5: Add targeted io.py tests (LOW)

**Problem:** `review_app/io.py` has 4 functions and 111 lines. Its functions are exercised indirectly but lack targeted edge-case tests.

**Implementation:**

1. Add to `tests/test_review.py` (or a new `tests/test_review_io.py`):
   - Test `load_proposed_transactions` with empty CSV (just headers) → returns empty DataFrame with expected columns.
   - Test `save_reviewed_transactions` → `load_proposed_transactions` round-trip preserves column types and values.
   - Test `load_proposed_transactions` with missing required columns → raises or handles gracefully.

**Files to modify:**
- `tests/test_review.py` or create `tests/test_review_io.py`

**Definition of done:**
- 3 new tests pass.

## Verification

Run `pixi run pytest` after each task.

Key test files:
- `tests/test_review.py`
- `tests/test_review_app.py`
- `tests/test_upload_prep.py`
- `tests/test_review_perf.py` (new)

## Out of Scope

- main() structural decomposition (sidebar/row-view/grouped-view split)
- `_render_row_controls` mutation/rendering split
- `scripts/build_proposed_transactions.py` decomposition
- YNAB API rate limiting
- Broad deduplication outside the review/upload path
- New product features or workflow changes
