# Cleanup Implementation Plan — Post-Third-Audit

Branch: `code-review-refactor` after commit `68bbec5`
Updated: 2026-03-30

## What Is Done

All critical and moderate items from the original cleanup plan and all three hostile audit passes:
- ✅ Safe boolean handling (`safe_types.py`, all CSV-backed review/upload paths)
- ✅ Component precomputation (single-sourced in `validation.py`, union-find)
- ✅ Generation-counter caching (non-mutation reruns skip all expensive computation)
- ✅ Component map passed to `apply_review_state` (no redundant traversal at validation layer)
- ✅ Fingerprint groupby precomputation in grouped mode
- ✅ `changed_mask` NaN-comparison bug fixed
- ✅ 9 business logic functions extracted from `app.py`
- ✅ Performance regression test (500-row blocker + cache verification)
- ✅ IO round-trip tests
- ✅ Strict `--approved` CLI parsing with focused test coverage
- ✅ Live `documents/review_app_workflow.md` added for README workflow links
- ✅ Invalid/truncated rows removed from `mappings/account_name_map.csv`
- ✅ `REPOSITORY_LAYOUT.md` updated for `review_app/` package structure and `safe_types.py`
- ✅ Union-find component discovery (replaces repeated BFS)
- ✅ 210 tests passing
- ✅ Stop condition for "repeated whole-dataframe work" is satisfied

## Pass 3 Remaining Items

Performance profiling revealed the per-mutation bottleneck is no longer the graph algorithm (9ms) but per-row pandas overhead (7s at 500 rows). Items ordered by expected impact.

### HIGH priority

**1. Eliminate double `validate_row` calls**
`blocker_series_with_components` calls `validate_row` twice per row: once inside `precompute_component_errors` → `review_component_errors`, once inside `blocker_label`. Precompute row errors once, pass to both.

**2. Add scalar `normalize_decision_action` helper**
`validate_row` wraps a single string in `pd.Series` to call `normalize_decision_actions`. This costs 1.03ms per call (1,027× overhead vs plain string ops). Add a scalar version for per-row use.

**3. Split `main()` and `_render_row_controls()`**
`main()` is 739 lines; `_render_row_controls` is 289 lines. Extract `_render_sidebar()`, `_render_row_view()`, `_render_group_view()`, `_build_edit_form()`, `_handle_form_submit()`.

### MEDIUM priority

**4. Pass `component_map` through `apply_row_edit`**
`state.py::apply_row_edit` calls `connected_component_mask(df, idx)` which recomputes the full union-find. The caller has the map but doesn't pass it.

**5. Consolidate string normalization**
`.astype("string").fillna("").str.strip()` appears 15 times. Centralize in a shared helper.

**6. `StrEnum` for decision actions**
Raw string literals appear 20+ times. A `DecisionAction(StrEnum)` would catch typos at import time.

**7. Replace `iterrows()` in hot paths**
5 call sites in the review_app package. Vectorize with `np.select` / `.where()` or at minimum use `.apply()`.

**8. Expand `--approved` test coverage**
Test only covers `true`/`false` of 6 valid values. Add `@pytest.mark.parametrize` over all valid + invalid values. Test `_parse_bool_arg` directly.

### LOW priority

**9. Refactor `precompute_component_errors` to use groupby**
Passes full DataFrame for 2-row slice operations 250 times. Single groupby would be cleaner.

**10. Remove dead code `accept_defaults_mask()`**
Returns all-False unconditionally. Zero callers.

**11. Resolve circular import**
`state.py` → `validation.py` → `state.py` cycle hidden by lazy import on line 516. Move shared logic to a common module.

**12. Define editable-column constant**
Same 7-column list appears 3 times in `state.py`.

**13. Assert correctness in perf test**
`test_blocker_series_with_components_smoke_500_rows` checks duration but not output validity.

**14. Split multi-assertion round-trip test**
14 assertions in one test; split by field group for better failure diagnostics.

**15. Fix README `build-payee-map` example**
Missing required `--parsed`, `--matched-pairs`, `--out-dir` arguments.
