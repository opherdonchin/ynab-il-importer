# Cleanup Implementation Plan — Post-Second-Audit残余

Branch: `code-review-refactor` at commit `55adca5`
Updated: 2026-03-30

This plan covers only the remaining items from the second hostile audit.
All prior tasks are complete.

## What Is Done

All critical and moderate items from the original cleanup plan and both hostile audit passes:
- ✅ Safe boolean handling (`safe_types.py`, all CSV-backed review/upload paths)
- ✅ Component precomputation (single-sourced in `validation.py`)
- ✅ Generation-counter caching (non-mutation reruns skip all expensive computation)
- ✅ Component map passed to `apply_review_state` (no redundant traversal)
- ✅ Fingerprint groupby precomputation in grouped mode
- ✅ `changed_mask` NaN-comparison bug fixed
- ✅ 9 business logic functions extracted from `app.py`
- ✅ Performance regression test (500-row blocker + cache verification)
- ✅ IO round-trip tests
- ✅ 208 tests passing
- ✅ Stop condition for "repeated whole-dataframe work" is satisfied

## Remaining Items (from Pass 2 FIX LIST)

### Task A: Loosen perf test bound (LOW, quick)

Change `assert duration < 10` to `assert duration < 30` in `tests/test_review_perf.py::test_blocker_series_with_components_smoke_500_rows`. The test measures 8.6s on the development machine; a 10s bound will flake on slower CI runners. The test's value is catching order-of-magnitude regressions, not enforcing a precise wall-clock target.

### Task B: Fix or remove README dangling reference (MODERATE, quick)

`README.md` references `documents/review_app_workflow.md` three times (lines 15, 134, 301). The file does not exist. Either create it with review-app workflow content drawn from the decisions docs and plan.md, or remove the references and link to existing docs.

### Task C: Clean up `mappings/account_name_map.csv` (MODERATE, requires domain knowledge)

Row 9 (`bank,0005,,,`) has empty YNAB mapping fields. Row 10 (`card,1950`) has only 2 of 5 fields (truncated data). Determine whether these accounts are active, and either complete the mapping or remove the rows.

### Task D: Fix `--approved` CLI flag in `scripts/prepare_ynab_upload.py` (LOW, quick)

Replace `type=lambda v: v.lower() not in {"false", "0", "no"}` with `choices=["true", "false"]` and explicit boolean mapping, or `action="store_true"`. Currently accepts any arbitrary string as True.

### Task E: Update `REPOSITORY_LAYOUT.md` (LOW, quick)

Add mention of `safe_types.py` and the `review_app/` subpackage structure (model.py, validation.py, state.py, io.py) to the `src/` section.

## Out of Scope

- `connected_component_mask` union-find optimization (future scaling improvement)
- `_render_row_controls` mutation/rendering split
- `main()` view-mode decomposition
- `scripts/build_proposed_transactions.py` library extraction
- YNAB API rate limiting
- New product features
- `scripts/build_proposed_transactions.py` decomposition
- YNAB API rate limiting
- Broad deduplication outside the review/upload path
- New product features or workflow changes
