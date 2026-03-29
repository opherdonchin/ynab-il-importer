# Cleanup Implementation Plan

Branch: `code-review-refactor`

This plan replaces the prior Codex task list with a repo-grounded implementation plan based on:
- `documents/plan.md`
- `documents/code_review_prompt.md`
- `documents/hostile_audit_prompt.md`
- the current codebase shape as inspected on `2026-03-29`

Two current-state notes:
- `documents/code_review_potential_issues.md` is not present in the repo, so it is not treated as an input source for this pass.
- The baseline test suite is not fully green before cleanup work starts. `tests/test_review_app_wrapper.py::test_build_streamlit_command_includes_control_dir_and_resume` currently fails because `scripts/review_app.py::_build_streamlit_command()` requires `profile` while the test still calls it without that argument.

## Cleanup Goals

1. Remove correctness hazards caused by unsafe coercion of CSV-backed booleans.
2. Cut user-visible review-app rerun cost by removing repeated whole-dataframe traversals.
3. Move review semantics out of `review_app/app.py` so rendering and business logic have clearer ownership.
4. Fix small baseline drift that prevents trustworthy green test runs.
5. Prefer small, reviewable slices with tests after each logical unit.

## Guardrails

1. Keep external behavior stable unless the current behavior is clearly a bug.
2. Preserve existing tests and add focused unit tests around extracted logic.
3. Treat review-app interaction cost as a first-class issue, not a documentation-only issue.
4. Do not spread refactors into unrelated domains just because duplication exists elsewhere.

## Execution Order

### Phase 0: Re-establish a trustworthy baseline

1. Fix the `scripts/review_app.py` / `tests/test_review_app_wrapper.py` mismatch by restoring a safe default for `profile`.
2. Run `pixi run pytest`.

Definition of done:
- the full suite is green before larger cleanup slices continue

### Phase 1: Safe boolean handling

1. Introduce a shared safe boolean normalization module under `src/ynab_il_importer/`.
2. Re-export the shared parser from `review_app/validation.py` so current imports remain valid.
3. Replace unsafe `astype(bool)` usage on review and category CSV-backed fields, starting with:
   - `src/ynab_il_importer/upload_prep.py`
   - `src/ynab_il_importer/map_updates.py`
   - `src/ynab_il_importer/review_app/state.py`
   - `scripts/prepare_ynab_upload.py`
   - review-app call sites that still read raw dataframe columns directly
4. Fix producer-side category exports so `hidden` is written as boolean data instead of `"False"` strings.
5. Add direct parser tests and upload-prep regression tests for string-backed `"False"` / `"True"` values.

Definition of done:
- no unsafe CSV-backed boolean coercion remains in the targeted review/upload path
- regression tests cover the hidden-category and reviewed-row paths

### Phase 2: Connected-component and blocker performance

1. Remove the duplicate `connected_component_mask` implementation from `review_app/state.py`.
2. Add component precomputation helpers in `review_app/validation.py`.
3. Refactor blocker derivation so connected components and component errors are computed once per component, not once per row.
4. Add tests for component labeling and error propagation.

Definition of done:
- one connected-component implementation remains
- blocker computation scales with component count rather than row count

### Phase 3: Grouped-view performance

1. Precompute fingerprint-to-index groupings once per rerun.
2. Stop rescanning the full dataframe per fingerprint in grouped mode.
3. Replace grouped-mode `iterrows()` option aggregation with vectorized split/explode flows.
4. Keep group rendering behavior unchanged.

Definition of done:
- grouped mode no longer performs repeated `df[df["fingerprint"] == fp]` scans inside render loops

### Phase 4: Review semantics extraction

1. Move validation/business-rule functions out of `review_app/app.py` into `review_app/validation.py`, `review_app/state.py`, and `review_app/model.py`.
2. Keep `app.py` focused on wiring, rendering, and Streamlit session management.
3. Update tests to call the extracted functions from their owning modules.
4. Add missing unit tests for blockers, primary-state mapping, allowed actions, competing-row resolution, search text, and uncategorized detection.

Definition of done:
- review semantics are unit-testable without importing the Streamlit app module
- `review_app/app.py` materially shrinks and becomes easier to navigate

### Phase 5: Focused follow-up fixes

1. Fix `review_app/state.py::changed_mask()` so rows absent from the baseline are always marked changed.
2. Do a narrow sweep for any remaining hot-path `iterrows()`, unsafe bool coercions, or duplicated helpers exposed by the earlier refactors.
3. Keep this sweep surgical; no broad opportunistic rewrites.

Definition of done:
- the known NaN / missing-baseline weakness is covered by tests
- no remaining high-value cleanup item is left merely documented if it is cheap and low-risk to fix now

## Verification

Run `pixi run pytest` after each logical unit.

Important verification areas:
- `tests/test_review.py`
- `tests/test_review_app.py`
- `tests/test_upload_prep.py`
- `tests/test_map_updates.py`
- `tests/test_review_app_wrapper.py`
- `tests/test_ynab_api.py` if API-touching cleanup happens later

## Out of Scope For This Pass

- moving all proposal-generation logic out of `scripts/build_proposed_transactions.py`
- broad deduplication across bank/card/cross-budget pipelines that is not needed for the review/upload path
- new product features or workflow changes
