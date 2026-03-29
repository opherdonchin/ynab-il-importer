# Hostile Audit Report — `code-review-refactor` Branch

## Audit History

| Pass | Commit | Date | Outcome |
|------|--------|------|---------|
| 1 | `aa347a9` | 2026-03-29 | BLOCKED — blocker_series recomputes O(n²) component graph on every Streamlit rerun |
| 2 | `55adca5` | 2026-03-30 | See below |

---

## MANDATORY QUESTIONS

### 1. Which parts of the current structure are genuinely necessary, and which parts are accidental complexity?

**Genuinely necessary:**

- The source/target review model with connected-component validation — transactions share source/target IDs and must be validated as a group. This graph-style constraint is real domain complexity.
- The blocker/state/action classification chain — the app needs to know whether a row is blocked, what state it's in, and what actions are allowed. These are distinct concepts.
- Fingerprint-based grouping — grouping related rows for bulk review is a core UX pattern.
- Safe boolean parsing via `safe_types.py` — CSV-backed string columns require explicit coercion. This is necessary.
- The 17-series top-of-main computation block — these series drive filtering, badges, and sidebar controls. Most are cheap vectorized pandas ops. The problem is not that they exist but that one of them (`blocker_series`) chains through O(n²) component traversal on every rerun.

**Accidental complexity:**

- `blocker_series()` recomputes the entire component graph on every Streamlit rerun via `component_error_lookup()` → `precompute_components()` → `connected_component_mask()`. The function names say "precompute" but nothing is cached between reruns. The precomputation only avoids per-row redundancy within a single call; the call itself is fully repeated.
- `_render_row_controls()` at 287 lines mixes form rendering, edit application, fingerprint bulk ops, competing-row resolution, and review-state transitions. It is a rendering function that mutates the DataFrame. The rendering/mutation boundary is not clean.
- `_derive_inference_tags()`, `_initial_inference_tags()`, `_apply_row_filters()` are business logic functions living in `app.py`. They were identified for extraction but left behind.
- `apply_review_state()` calls `precompute_components()` from scratch every time a user reviews a row, even though the component structure is already known from the blocker series computation earlier in the same Streamlit run.
- `blocker_series()` itself uses `df.iterrows()` — a Python loop over every row calling `blocker_label()` with `validate_row()` per row.

### 2. Is `review_app/app.py` carrying responsibilities that belong elsewhere?

**Yes.** After the Codex refactor, app.py dropped from 2180 to 1912 lines and main() is 777 lines. Material progress. But:

| Function | Lines | Problem |
|----------|-------|---------|
| `_render_row_controls` | 836–1122 (287 lines) | Mixes rendering with mutation: applies row edits, fingerprint bulk ops, competing-row resolution, validation, and review-state transitions. The mutation logic should be in `model.py` or `validation.py`; `app.py` should only call it and render the result. |
| `_derive_inference_tags` | 633–650 (18 lines) | Pure business logic: classifies rows by match_status and missing fields. Belongs in `state.py`. |
| `_initial_inference_tags` | 652–664 (13 lines) | Pure business logic: maps base inference tags to current rows. Belongs in `state.py`. |
| `_apply_row_filters` | 666–699 (34 lines) | Pure filtering logic using state-derived series. Belongs in `state.py`. |
| `main()` lines 1134–1910 | 777 lines | Orchestration, sidebar, view routing, pagination, rendering, and inline mutation handling. Decomposing main() along view-mode boundaries (sidebar, row-view, grouped-view) would improve navigability. |

### 3. Are there duplicated helper families that should be consolidated now?

**Mostly resolved.** The Codex pass consolidated:
- `connected_component_mask` — now single-sourced in `validation.py` ✓
- Boolean parsing — now via `safe_types.normalize_flag_series` ✓
- `_id_series`, `_text`, `_truthy` — still duplicated between `validation.py` and `state.py` but these are trivial one-liners. Not worth a FIX LIST item.

**One remaining consolidation debt:** `_bool_series` in `state.py` and `_truthy` in `validation.py` both wrap `normalize_flag_series` but with slightly different shapes. Maintenance-facing only; not a FIX LIST item for this pass.

### 4. Are there any string-backed boolean fields being interpreted unsafely?

**No.** The Codex pass replaced all unsafe `astype(bool)` calls on CSV-backed string columns:
- `upload_prep.py` lines 53, 155, 175 → `normalize_flag_series()` ✓
- `map_updates.py` line 34 → `normalize_flag_series()` ✓
- `review_app/state.py` → `_bool_series()` wraps `normalize_flag_series()` ✓
- `scripts/prepare_ynab_upload.py` → safe coercion ✓
- `scripts/build_categories_from_ynab_snapshot.py` → writes `False` (bool) instead of `"False"` (string) ✓

The two remaining `astype(bool)` calls in `app.py` (lines 1073, 1166) operate on intermediate boolean series from logical operations, not CSV strings. Clean.

### 5. What are the top three causes of user-visible slowness in the review app?

**#1: `blocker_series()` recomputes the full component graph on every rerun.**

`blocker_series()` → `component_error_lookup()` → `precompute_components()` → calls `connected_component_mask()` once per component. Each `connected_component_mask()` does O(n) work (BFS over the full DataFrame with pandas series operations). With C components (typically n/2 for pair-based data), total cost is O(n²/2). Plus `blocker_series()` itself uses `iterrows()` over all n rows, calling `validate_row()` each time. This runs on every single user interaction (filter change, scroll, button click).

For 240 rows: ~29K DataFrame operations per rerun.
For 1000 rows: ~500K DataFrame operations per rerun.
For 6000 rows: ~18M DataFrame operations per rerun.

**No caching, no invalidation, no `st.cache_data`.** The component structure only changes when the user edits a row. Between edits, every rerun recomputes the same result.

**#2: `apply_review_state()` recomputes `precompute_components()` from scratch on save/review.**

Called at three points in app.py (lines 1094, 1291, 1788). Each call does a fresh `precompute_components()` traversal of the full DataFrame. This happens on user-triggered actions (review row, accept all, apply group) so it's less frequent than #1, but still wasteful since the component structure was already computed by `blocker_series()` moments earlier in the same Streamlit run.

**#3: The 17-series top-of-main computation block runs on every rerun (lines 1157–1189).**

Most of these are cheap vectorized pandas operations (O(n) each). The cumulative cost for 240 rows is negligible. For 6000 rows it adds up but is dominated by #1. The real cost center is `blocker_series` at line 1169 which triggers the component computation chain.

**NOT in the top 3:**
- Grouped-mode fingerprint groupby (lines 1554–1561) is now properly precomputed via `groupby()` dict comprehension. ✓
- The `iterrows()` at line 1680 for group decision collection only processes visible page rows (10–50), not the full dataset. Acceptable.

### 6. Which performance fixes are low-risk and high-value enough to do in this branch?

**High-value, low-risk:**

1. **Cache `blocker_series` and dependent data in `st.session_state`** with a generation counter that increments only when the DataFrame is mutated. All 17 derived series at top of main() can be gated behind a `_series_generation` check: recompute only if the DataFrame has changed since last computation. This eliminates the dominant O(n²) cost on non-mutation reruns. Implementation: ~30 lines. Risk: low (pure caching with explicit invalidation).

2. **Pass the already-computed `component_map` from `blocker_series` through to `apply_review_state`** so the review path doesn't recompute components from scratch. This requires `blocker_series` to return or stash the component map. Implementation: ~15 lines. Risk: low (pure plumbing).

**Medium-value, low-risk:**

3. **Vectorize `blocker_series()` internals.** Replace the `iterrows()` + `blocker_label()` loop with vectorized pandas operations. `validate_row()` is per-row but could be reimplemented as column-wise mask checks. This would remove Python loop overhead but the dominant cost is the component graph, not the per-row labeling. Implementation: ~50 lines. Risk: medium (requires careful functional equivalence testing).

### 7. Which large files should be decomposed immediately, and along what boundaries?

**`review_app/app.py` (1912 lines)** — decompose along these boundaries:

| Extract | Target | What moves | Estimated size |
|---------|--------|------------|---------------|
| Mutation logic from `_render_row_controls` | `model.py` | Row edit application, fingerprint bulk ops, competing-row resolution, review-state transition calls | ~100 lines out of 287 |
| `_derive_inference_tags`, `_initial_inference_tags` | `state.py` | Inference tag classification and mapping | ~31 lines |
| `_apply_row_filters` | `state.py` | Multi-dimension filter application | ~34 lines |

After these extractions, app.py drops to ~1750 lines and `_render_row_controls` becomes ~180 lines of actual rendering. main() structural decomposition (sidebar/row-view/grouped-view split) would help navigability but is higher-risk and can be deferred.

**`scripts/build_proposed_transactions.py`** — still acts as a hidden library module. Out of scope for this pass per the cleanup plan guardrails.

---

## FIX LIST

### FIX 1: `blocker_series` recomputes full component graph on every Streamlit rerun (CRITICAL)

**User-facing or maintenance-facing:** USER-FACING. Every filter change, scroll, or click triggers a full O(n²) component traversal.

**Why the current structure is harder than necessary:** `precompute_components()` and `precompute_component_errors()` exist but are invoked fresh inside `component_error_lookup()` on every call to `blocker_series()`. The "precompute" naming is misleading — it suggests caching but delivers none. The component graph structure only changes when the DataFrame is mutated (row edit, review, save). Between mutations, every rerun wastes this work.

**Simplest acceptable restructuring:**
1. Add a `_series_generation` counter to `st.session_state`, initialized to 0, incremented when any mutation occurs (row edit, review, accept-all, save, load).
2. At top of main(), check whether `_series_generation` has changed since the last series computation. If not, reuse the cached series from `st.session_state["_cached_series"]`. If changed, recompute all 17 series and cache them with the current generation.
3. This makes non-mutation reruns (filter changes, scrolling, view switches) effectively free.

**Measurement required:** A timed benchmark comparing rerun cost before and after caching, using a synthetic dataset of 500+ rows. The test should confirm that non-mutation reruns do not call `precompute_components()`.

### FIX 2: `apply_review_state` redundantly recomputes component graph (MODERATE)

**User-facing or maintenance-facing:** USER-FACING. Adds avoidable latency to every review/save action.

**Why the current structure is harder than necessary:** `apply_review_state()` calls `precompute_components()` independently at lines 359 (validation.py). When called from app.py, the component graph was already computed by `blocker_series()` earlier in the same run. The graph is recomputed from scratch even though the only change may be marking a few rows as `reviewed=True`.

**Simplest acceptable restructuring:** Accept an optional `component_map` parameter in `apply_review_state()`. When `blocker_series()` stashes its `component_map` (as part of FIX 1 caching), pass it to `apply_review_state()` at the call sites in app.py. Fall back to internal computation when not provided (for test convenience).

**Measurement required:** Confirm via logging or assertion that `precompute_components` is called at most once per user interaction in the review-then-save flow.

### FIX 3: `blocker_series` uses `iterrows()` over all rows (LOW)

**User-facing or maintenance-facing:** Maintenance-facing primarily. The Python loop overhead is small compared to the component graph cost (FIX 1).

**Why the current structure is harder than necessary:** `blocker_series()` iterates every row via `df.iterrows()` calling `blocker_label()` → `validate_row()` per row. This is O(n) Python-loop work on top of the O(n²) component work. Once FIX 1 caches the component computation, this becomes the dominant per-rerun cost (unless also cached).

**Simplest acceptable restructuring:** If FIX 1 caches the entire `blocker_series` result, this becomes moot — the iterrows runs only on mutation. If FIX 1 is not implemented, vectorize `blocker_label` using column-wise mask checks instead of per-row dispatch. Alternatively, accept that it's low-priority if FIX 1 is done.

**Measurement required:** Only needed if FIX 1 is not implemented. Measure per-row cost of `blocker_label()` on a 500-row dataset.

### FIX 4: Three business logic functions remain in app.py (LOW)

**User-facing or maintenance-facing:** MAINTENANCE-FACING. Does not affect users.

**Why the current structure is harder than necessary:** `_derive_inference_tags`, `_initial_inference_tags`, and `_apply_row_filters` are pure business logic that don't touch Streamlit APIs. They were identified for extraction in the cleanup plan but left behind. Having them in app.py means they cannot be unit-tested without importing the app module.

**Simplest acceptable restructuring:** Move `_derive_inference_tags` and `_initial_inference_tags` to `state.py`. Move `_apply_row_filters` to `state.py` (it already calls `state.apply_filters` internally). Update imports in app.py.

**Measurement required:** Existing test suite passes. Add 1 targeted test per extracted function.

### FIX 5: No performance benchmark or smoke test (MODERATE)

**User-facing or maintenance-facing:** MAINTENANCE-FACING. Without a benchmark, future regressions are invisible.

**Why the current structure is harder than necessary:** The hostile audit requires "one realistic review-app smoke dataset that is materially larger than the tiny unit-test fixtures" and "at least one check that the review app's expensive derived state does not regress badly after refactor." Neither exists.

**Simplest acceptable restructuring:** Add a test fixture factory that generates N review rows with realistic column shapes (source/target IDs, fingerprints, decision actions, payee/category values). Add one benchmark test that calls `blocker_series()` on a 500-row dataset and asserts it completes within a reasonable wall-clock bound (e.g., 5 seconds). This serves as a regression tripwire.

**Measurement required:** The test itself IS the measurement.

### FIX 6: `review_app/io.py` has minimal direct tests (LOW)

**User-facing or maintenance-facing:** MAINTENANCE-FACING.

**Why the current structure is harder than necessary:** `io.py` has 4 functions (111 lines). Two of them (`load_proposed_transactions`, `save_reviewed_transactions`) are exercised in `test_review.py` but not with targeted edge-case coverage (missing columns, empty DataFrames, round-trip fidelity).

**Simplest acceptable restructuring:** Add 2-3 focused tests: empty-input handling, round-trip save/load preserving column types, and missing-column error handling.

**Measurement required:** Tests pass.

---

## ITEMS NOT ON THE FIX LIST (with reasoning)

| Item | Reason |
|------|--------|
| `_render_row_controls` at 287 lines | Mutation/rendering split would be beneficial but is a higher-risk refactor with no correctness impact. Defer to a follow-up pass. |
| main() at 777 lines | Material improvement from 1000+ lines. Sidebar/row-view/grouped-view split would help navigability but the function is sequential orchestration, not tangled logic. Defer. |
| `scripts/build_proposed_transactions.py` decomposition | Explicitly out of scope per cleanup plan guardrails. |
| YNAB API rate limiting | Not review-app related. Separate concern. |
| Trivial helper duplication (`_text`, `_truthy`) | One-liners with no drift risk. |
| `iterrows()` at line 1680 in grouped mode | Only processes visible page rows (10-50). Not a meaningful hotspot. |

---

## EXTRA STOP CONDITION ASSESSMENT

> "Do not declare this review complete if the branch still has obvious user-facing slowness caused by repeated whole-dataframe work in the review app and the issue is merely documented rather than addressed."

**This branch is NOT complete.** FIX 1 is the gating item. `blocker_series()` triggers a full O(n²) component graph recomputation on every Streamlit rerun. The `precompute_components()` function exists, which is a step forward from the pre-Codex state (where components were computed per-row), but the result is not cached between reruns. Every filter change, scroll, or view switch pays the full cost.

The fix is well-defined (generation-counter caching in session_state) and low-risk. It must be implemented, not merely documented, before this branch can be declared reviewable.

---

## SUMMARY SCORECARD (Pass 1)

| Cleanup Plan Phase | Status | Assessment |
|-------------------|--------|------------|
| Phase 0: Trustworthy baseline | ✅ DONE | 200 tests passing |
| Phase 1: Safe boolean handling | ✅ DONE | All CSV-backed boolean coercion replaced with `normalize_flag_series` |
| Phase 2: Component/blocker performance | ⚠️ PARTIAL | Precomputation exists but is not cached between reruns. FIX 1 required. |
| Phase 3: Grouped-view performance | ✅ DONE | Fingerprint groupby precomputation works correctly |
| Phase 4: Review semantics extraction | ⚠️ PARTIAL | 6 functions moved out; 3 business logic functions remain in app.py (FIX 4) |
| Phase 5: Focused follow-up fixes | ✅ DONE | `changed_mask` NaN bug fixed, safe_types module created |
| Performance verification | ❌ MISSING | No benchmark or smoke test exists (FIX 5) |

---
---

# PASS 2 — Hostile Audit at `55adca5` (2026-03-30)

Branch: `code-review-refactor` at commit `55adca5`

## Scope

This audit covers the two new commits (`d7b8823`, `55adca5`) implementing the fixes required by Pass 1. It also broadens scope to data files, documentation, configuration, scripts, and any previously unaudited artifacts.

## Codex Implementation Summary (commit `d7b8823`)

| Change | Files | Assessment |
|--------|-------|------------|
| Generation-counter caching for derived series | `app.py` (+269/-132) | Reviewed in detail — CORRECT |
| `blocker_series_with_components()` returning component_map | `validation.py` (+34/-10) | Reviewed — CORRECT |
| `apply_review_state()` accepting optional `component_map` | `validation.py` | Reviewed — CORRECT |
| Business logic extraction: `derive_inference_tags`, `initial_inference_tags`, `apply_row_filters` | `state.py` (+76), `app.py` (-68) | Reviewed — CORRECT |
| Performance regression test | `test_review_perf.py` (+104) | Reviewed — see Finding 2 |
| IO round-trip tests | `test_review_io.py` (+73) | Reviewed — ADEQUATE |
| Updated filter test import | `test_review_app.py` (+1/-1) | Reviewed — CORRECT |

## MANDATORY QUESTIONS — Updated Answers

### 1. Accidental vs necessary complexity?

**Resolved since Pass 1:**
- The generation-counter caching eliminates the accidental complexity of recomputing `blocker_series` on every rerun. Non-mutation reruns are now free.
- Business logic extraction is complete — `_derive_inference_tags`, `_initial_inference_tags`, `_apply_row_filters` now live in `state.py`.

**Remaining accidental complexity:**
- `connected_component_mask()` uses BFS via full-DataFrame pandas series operations. For pair-based data (components of size 2), this is O(n) per component × O(n/2) components = O(n²). A dict-based union-find would be O(n·α(n)) ≈ O(n). This is the next scaling bottleneck.
- `blocker_series_with_components()` still iterates all rows via `df.iterrows()` calling `validate_row()` + `blocker_label()` per row. This O(n) Python loop contributes ~30% of the per-mutation cost. Vectorizable but only worth it once the O(n²) component traversal is addressed.

### 2. Is app.py carrying responsibilities that belong elsewhere?

**Improved.** Three business logic functions extracted to `state.py`. Remaining app.py responsibilities are:
- UI rendering and Streamlit wiring (appropriate)
- `_render_row_controls()` at 287 lines (mixes rendering with mutation application — debt, but tolerable)
- `main()` at ~777 lines (sequential orchestration — large but not tangled)

No further extraction is blocking for this branch.

### 3. Duplicated helpers?

No new duplication introduced. Prior consolidation remains.

### 4. Unsafe string-backed boolean coercion?

**Still clean** in the review/upload path. The two `astype(bool)` calls in `app.py` (lines 657, 1109) operate on intermediate boolean masks, not CSV strings.

### 5. Top causes of user-visible slowness?

**#1 is now FIXED.** Previously: every interaction triggered O(n²) component computation. Now: only mutations trigger it.

Revised top causes:
1. **Per-mutation O(n²) component recomputation** via `connected_component_mask()`. Measured: 4.0s at 240 rows, 8.6s at 500 rows. Runs once per row edit/review action. Tolerable at current data sizes but poor scaling.
2. **Per-mutation O(n) iterrows in blocker_series_with_components**. Runs once per mutation, dominated by #1.
3. **Per-mutation 17-series recomputation**. Cheap vectorized pandas ops; negligible at current sizes.

### 6. Low-risk, high-value performance fixes for this branch?

**Already done:** generation-counter caching, component_map passthrough.

**Next-value fix (deferred):** Replace `connected_component_mask` BFS with union-find over `source_row_id`/`target_row_id`. Would reduce per-mutation cost from O(n²) to O(n). Low risk but material implementation. Not blocking for this branch.

### 7. Which large files should be decomposed immediately?

**No further decomposition is blocking.** `app.py` is at ~1912 lines (down from 2180). The _render_row_controls mutation/rendering split and main() view-mode split would help navigability but are higher-risk and not required for merge.

## CACHING CORRECTNESS — DETAILED AUDIT

| Aspect | Status | Evidence |
|--------|--------|---------|
| All `st.session_state["df"] =` go through `_set_review_frames` | ✅ | Grep confirms 0 unprotected assignments |
| All `st.session_state["df_original"] =` go through `_set_review_frames` | ✅ | Grep confirms 0 unprotected assignments |
| All `st.session_state["df_base"] =` go through `_set_review_frames` | ✅ | Grep confirms 0 unprotected assignments |
| `_bump_df_generation` increments counter AND clears stale cache | ✅ | Pops `_cached_series` and `_cached_component_map` |
| `_get_cached_derived_state` recomputes when generation mismatches | ✅ | Test confirms: 1 call with same gen → 1 computation; bump gen → 2nd computation |
| No in-place mutation of session_state DataFrame | ✅ | All mutation paths copy first (`working_df = df.copy()`) |
| Save path correctly bumps generation | ✅ | `_set_review_frames(original=df.copy())` at save |
| Category refresh does NOT need to bump generation | ✅ | Categories are UI-only (dropdown options), not in derived state |
| `component_map` cached and passed to `apply_review_state` | ✅ | Extracted from `derived["component_map"]`, passed at all 3 call sites |

## PERFORMANCE — MEASURED SCALING

```
  50 rows:  0.65s  (blocker_series_with_components)
 100 rows:  1.67s
 240 rows:  4.02s  ← current Aikido working set
 500 rows:  8.60s  ← perf test dataset
```

With caching: this cost is paid **once** per mutation. Non-mutation reruns (filter, scroll, view switch) are effectively free. Prior to caching, EVERY interaction paid this cost.

## FIX LIST — Pass 2

### FIX 2.1: Perf test bound is fragile (LOW)

**User-facing or maintenance-facing:** MAINTENANCE-FACING.

**Problem:** `test_blocker_series_with_components_smoke_500_rows` asserts `duration < 10`. Measured at 8.6s. On slower CI machines or under load, this will flake. The test is a good idea but the bound is too tight.

**Fix:** Raise the bound to `< 30` (3x headroom). The test's value is as a regression tripwire against order-of-magnitude regressions, not as a precise timing gate. Alternatively, mark it with `@pytest.mark.slow` and exclude from default CI if timing is unstable.

**Measurement:** The test itself.

### FIX 2.2: README references non-existent `documents/review_app_workflow.md` (MODERATE)

**User-facing or maintenance-facing:** USER-FACING (documentation).

**Problem:** `README.md` references `documents/review_app_workflow.md` three times (lines 15, 134, 301). The file does not exist. Any new contributor following the README hits a dead end.

**Fix:** Either create the document with review-app workflow content, or remove the references and link to the actual docs (`documents/project_context.md`, `documents/decisions/unified_review_model_design.md`).

**Measurement:** `Test-Path documents/review_app_workflow.md` returns True, or references are removed.

### FIX 2.3: `mappings/account_name_map.csv` has corrupt/incomplete rows (MODERATE)

**User-facing or maintenance-facing:** Potentially USER-FACING if the mapping is consumed at runtime.

**Problem:** The last two rows are incomplete:
- Line 9: `bank,0005,,,` — empty YNAB account name and ID
- Line 10: `card,1950` — only 2 fields instead of 5 (truncated)

If a source transaction matches account `0005` or card `1950`, the mapping lookup silently returns empty strings, which may cause downstream issues in upload prep or identity resolution.

**Fix:** Determine whether these accounts are still active. If active, fill in the correct YNAB mapping. If inactive/stale, remove the rows. If intentionally unmapped, add a comment or sentinel value.

**Measurement:** All rows in `account_name_map.csv` have exactly 5 fields with no empty required values.

### FIX 2.4: `scripts/prepare_ynab_upload.py` --approved flag is permissive (LOW)

**User-facing or maintenance-facing:** USER-FACING (CLI behavior).

**Problem:** The `--approved` argument uses `type=lambda v: v.lower() not in {"false", "0", "no"}`, which means `--approved typo` or `--approved anything` silently evaluates to True. Only an explicit false-ish value registers as False.

**Fix:** Replace the lambda with `action="store_true"` (if the flag should be on/off), or use `choices=["true", "false"]` with explicit mapping. This is consistent with how `--cleared` uses `choices=["cleared", "uncleared"]` two lines above.

**Measurement:** `--approved invalid` raises an argparse error.

### FIX 2.5: `REPOSITORY_LAYOUT.md` doesn't reflect new source files (LOW)

**User-facing or maintenance-facing:** MAINTENANCE-FACING.

**Problem:** The layout doc describes directory-level retention rules but doesn't mention key new files: `safe_types.py`, `review_app/model.py`, `test_review_perf.py`, `test_review_io.py`, `hostile_audit_report.md`. The doc is designed as a high-level guide rather than a file inventory, so individual file omission is normal. But `safe_types.py` and `model.py` represent new architectural boundaries that deserve mention.

**Fix:** Add a "Key source modules" section or update the `src/` description to mention the review_app subpackage structure.

**Measurement:** Review of REPOSITORY_LAYOUT.md during the next doc sweep.

## ITEMS NOT ON THE FIX LIST (with reasoning)

| Item | Reason |
|------|--------|
| `connected_component_mask` is O(n²) | Real improvement but not blocking for this branch. The caching eliminates repeated recomputation; the per-mutation cost at 240 rows (4s) is tolerable. Document as future optimization. |
| `blocker_series_with_components` uses `iterrows()` | Only runs on mutation; dominated by component computation cost. Moot once caching is in place. |
| `_render_row_controls` at 287 lines | Mutation/rendering mixed but functional. Higher-risk refactor, no correctness impact. Defer. |
| `main()` at ~777 lines | Sequential orchestration. Not tangled. Defer. |
| `build_proposed_transactions.py` decomposition | Out of scope per existing guardrails. |
| Trivial helper duplication (`_text`, `_truthy`) | One-liners with no drift risk. |
| `config/ynab.local.toml` root `budget_id` key | Legacy support flag; `.example.toml` already documents it as a comment. |

## STOP CONDITION ASSESSMENT

> "Do not declare this review complete if the branch still has obvious user-facing slowness caused by repeated whole-dataframe work in the review app and the issue is merely documented rather than addressed."

**The stop condition is now SATISFIED.**

Evidence:
1. **Non-mutation reruns are free.** The generation-counter cache (`_get_cached_derived_state`) ensures that filter changes, scrolling, view switches, and sidebar interactions do not trigger `precompute_components()` or any expensive computation. Test `test_cached_derived_state_skips_recompute_when_generation_is_unchanged` proves this with a 500-row dataset.
2. **Mutation reruns compute once.** The O(n²) component graph runs exactly once per DataFrame mutation (row edit, review, accept-all). At 240 rows (current working set), this takes ~4 seconds — noticeable but acceptable for an action the user explicitly triggered.
3. **Component map is reused.** `apply_review_state()` accepts the cached component map from `blocker_series_with_components`, avoiding redundant graph traversal within the same user interaction.
4. **The remaining O(n²) cost is per-mutation, not per-rerun.** The stop condition specifically targets "repeated whole-dataframe work." Post-caching, the work happens exactly once per state change, which is the minimum possible.

## SUMMARY SCORECARD (Pass 2)

| Area | Status | Assessment |
|------|--------|------------|
| Generation-counter caching | ✅ DONE | Correct implementation, verified by test and code audit |
| Cache invalidation correctness | ✅ DONE | All mutation paths go through `_set_review_frames` |
| Component map reuse | ✅ DONE | Passed to all 3 `apply_review_state` call sites |
| Business logic extraction | ✅ DONE | 3 functions moved to `state.py` |
| Performance regression test | ✅ DONE | 500-row benchmark exists (bound should be loosened) |
| IO round-trip tests | ✅ DONE | 3 new tests in `test_review_io.py` |
| Test suite | ✅ DONE | 208 tests passing |
| Stop condition | ✅ MET | Non-mutation reruns are free; per-mutation cost is tolerable |
| README dangling reference | ❌ OPEN | `review_app_workflow.md` doesn't exist |
| Account name map data quality | ❌ OPEN | Truncated/incomplete rows |
| Perf test timing bound | ⚠️ FRAGILE | 8.6s actual vs 10s bound |
| --approved CLI flag | ⚠️ PERMISSIVE | Accepts arbitrary strings as True |
