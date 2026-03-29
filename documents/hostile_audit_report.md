# Hostile Audit Report — `code-review-refactor` Branch

## Audit History

| Pass | Commit | Date | Outcome |
|------|--------|------|---------|
| 1 | `aa347a9` | 2026-03-29 | BLOCKED — blocker_series recomputes O(n²) component graph on every Streamlit rerun |
| 2 | `55adca5` | 2026-03-30 | PASS — stop condition met; 5 low/moderate items remaining |
| 3 | `68bbec5` | 2026-03-30 | See below |
| 4 | `75ce9a0` | 2026-03-31 | Readability & clarity — 2 HIGH, 5 MEDIUM, 10 LOW |

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

---

## Pass 4 — Readability & Clarity Audit (Newcomer Walkthrough)

**Scope:** This pass reads the repository as a newcomer would — starting from the README, following breadcrumbs into the code, and flagging every point where a first-time reader would stumble, lose context, or need to search elsewhere to understand what they're looking at. This is not about correctness or performance; it's about whether the codebase is understandable.

**Method:** Read files in the order a newcomer would discover them. Note every point of confusion. Build up understanding, then evaluate the whole.

---

### Walk-in Experience

A newcomer enters through `README.md`, which is thorough on *how to use the tool* — it documents two workflows (bootstrap and ongoing), lists CLI commands, and describes the review UI. But it never explains the code architecture. A reader finishes the README knowing *what buttons to press* but not *how the code is organized* or *what modules call what*.

`REPOSITORY_LAYOUT.md` lists directories and gives brief module descriptions, but doesn't explain data flow or module relationships. After reading both entry documents, a newcomer knows the project has a `src/` package with 25+ modules, a `scripts/` directory, and a `review_app/` subpackage — but cannot draw a mental diagram of how data moves through the system.

### Finding 1 — No architectural overview anywhere

**Severity: HIGH (readability)**

There is no single document or diagram that shows:
- The data pipeline: raw bank/card files → normalization → fingerprinting → pairing → proposal building → human review → upload prep → YNAB API
- Which modules implement which stages
- Which scripts orchestrate which library calls

The information exists in fragments across README (workflow steps), REPOSITORY_LAYOUT (module names), and project_context.md (priorities and reading order), but nobody has drawn the picture. A newcomer must read 3 documents and still guess at the module graph.

**Recommendation:** Add an "Architecture" section to README or a standalone `ARCHITECTURE.md` with a data-flow diagram and module-to-stage mapping.

### Finding 2 — 25 flat modules with no grouping or orientation

**Severity: MEDIUM**

The package `src/ynab_il_importer/` contains 25 flat modules. A newcomer sees:
```
account_map.py, bank_identity.py, bank_reconciliation.py, card_identity.py,
card_reconciliation.py, cli.py, config.py, cross_budget_pairing.py,
cross_budget_reconciliation.py, export.py, fingerprint.py, io_leumi.py,
io_leumi_card_html.py, io_leumi_xls.py, io_max.py, io_ynab.py, map_updates.py,
normalize.py, pairing.py, reconciliation_packets.py, review_reconcile.py,
rules.py, safe_types.py, upload_prep.py, workflow_profiles.py, ynab_api.py
```

These fall into natural groups but nothing communicates the grouping:
- **IO adapters:** `io_leumi.py`, `io_leumi_xls.py`, `io_leumi_card_html.py`, `io_max.py`, `io_ynab.py` (plus `export.py`, which lacks the `io_` prefix)
- **Domain logic:** `rules.py`, `fingerprint.py`, `pairing.py`, `bank_reconciliation.py`, `card_reconciliation.py`, `cross_budget_reconciliation.py`, `cross_budget_pairing.py`, `upload_prep.py`, `map_updates.py`
- **Identity/normalization:** `bank_identity.py`, `card_identity.py`, `normalize.py`, `account_map.py`
- **Infrastructure:** `config.py`, `safe_types.py`, `workflow_profiles.py`, `ynab_api.py`, `reconciliation_packets.py`
- **Orchestration:** `cli.py`, `review_app/`, `review_reconcile.py`

`__init__.py` exports only `__version__`. It could at minimum carry a module-level docstring listing these groups.

**Recommendation:** Add a docstring to `__init__.py` listing the module groups, or add sub-packages for the natural clusters. At minimum, sort the import graph in the developer's head.

### Finding 3 — No module-level docstrings

**Severity: MEDIUM**

Most modules lack any docstring explaining their purpose. A newcomer opening `state.py`, `validation.py`, `model.py`, `io.py`, `pairing.py`, `fingerprint.py`, `map_updates.py` etc. sees imports and immediate function definitions. There is no orientation sentence telling the reader "this module is responsible for X."

Examples of modules that would benefit most from a one-line docstring:
- `safe_types.py` — name says "safe types" but it's about boolean parsing from CSV strings
- `review_reconcile.py` — name implies "reviewing reconciliation" but it carries over prior review decisions to new proposed rows
- `config.py` — defines `ProjectPaths` with hardcoded paths; `workflow_profiles.py` also manages config via TOML — the split isn't obvious
- `export.py` — is an IO module but doesn't follow the `io_*` naming convention
- `model.py` — a newcomer wonders "model of what?" (it's the review-row data model)

**Recommendation:** Add a one-line module docstring to each file. This is a 10-minute task that eliminates the most common newcomer stall point.

### Finding 4 — `app.py` main() is 739 lines of sequential orchestration

**Severity: HIGH (readability)**

`main()` spans lines 1171–1909. It does:
1. Page configuration and initialization (~15 lines)
2. Data loading and ensure-loaded checks (~10 lines)
3. Unpack 25+ variables from derived state dict (~30 lines)
4. Sidebar: file management, save/reload, quit buttons (~50 lines)
5. Summary statistics (~30 lines)
6. View mode radio and "Accept all" button (~20 lines)
7. Filter multiselects (~60 lines)
8. Search (~10 lines)
9. State legend and matrix display (~40 lines)
10. Row view mode: pagination + per-row rendering loop (~90 lines)
11. Grouped view mode: fingerprint grouping, group controls, "Apply to all" logic, per-row-in-group rendering (~350 lines)

These are distinct phases with clear boundaries but they're all in one function. A newcomer has to hold the entire 739-line function in memory to understand any part of it. The grouped view section (point 11) is ~350 lines of group setup, widget creation, apply logic, and row rendering nested 3 `for`/`with` levels deep.

**Recommendation:** Extract `_render_sidebar()`, `_render_row_view()`, `_render_grouped_view()` as top-level functions called from `main()`. This doesn't change behavior — just gives the reader chapter headings.

### Finding 5 — The derived-state dict is a 25-key untyped bag

**Severity: MEDIUM**

`_compute_derived_state()` returns a `dict[str, Any]` with 25+ keys. `main()` then unpacks everything into local variables:
```python
d = _get_cached_derived_state(df, ...)
blocker_series = d["blocker_series"]
component_map = d["component_map"]
primary_state_series = d["primary_state_series"]
# ... 22 more lines
```

This is a custom struct without any of the benefits of a typed struct. Key names are stringly-typed, autocomplete doesn't work, typos fail silently at dict-lookup time, and there's no single place to see the complete schema.

**Recommendation:** Replace the dict with a `@dataclass` or `NamedTuple` called `DerivedState`. This makes the contract self-documenting and gives IDE support.

### Finding 6 — `_render_row_controls` has 14 parameters

**Severity: MEDIUM**

```python
def _render_row_controls(
    df, idx, category_choices, category_group_map, payee_defaults,
    category_defaults, show_apply, group_fingerprint, updated_mask,
    component_map, row_order, row_page_size, ...
)
```

A newcomer does not know what "row controls" means, cannot guess which parameters drive rendering vs. mutation, and must read the full 289-line body to understand the signature.

**Recommendation:** Group related parameters into a context object or split rendering from mutation handling.

### Finding 7 — `EDITOR_STATE_PREFIXES` and `EDITOR_STATE_KEYS` are unexplained

**Severity: LOW**

Lines ~30-50 of `app.py` define tuples and sets of magic strings used to namespace Streamlit session state keys:
```python
EDITOR_STATE_PREFIXES = ("payee_select_", "payee_override_", ...)
EDITOR_STATE_KEYS = {"review_df", "original_df", "baseline_df", ...}
```

These drive `_editor_key()` which prefixes widget keys with `_ed_`. A newcomer encounters these constants and has no idea what "editor state" means, why keys need prefixing, or what happens if they collide. A brief comment would suffice.

### Finding 8 — Inline HTML/CSS scattered across 5+ functions

**Severity: LOW**

`_render_status_badges`, `_render_primary_state_banner`, `_render_primary_state_strip`, `_render_secondary_tag_badges`, `_inject_primary_state_css`, and `_render_primary_state_legend` all build raw HTML strings with inline CSS. The `_inject_primary_state_css` function injects a `<style>` block targeting Streamlit internal `data-testid` attributes, which will break when Streamlit updates its internal DOM.

For a newcomer, these functions are walls of string concatenation that obscure the rendering intent. The actual *what* (a colored badge, a colored strip) is buried in *how* (CSS properties, hex colors).

**Recommendation:** Consolidate the color/style definitions into a single constants dict (partially done via `_PRIMARY_STATE_META`) and consider extracting the HTML builders into a thin `review_app/ui.py` module. This isn't urgent but would improve readability.

### Finding 9 — The dual-column `payee_selected` / `target_payee_selected` pattern is never explained in code

**Severity: MEDIUM**

Throughout the codebase, `payee_selected` and `target_payee_selected` coexist as columns. `state.py`'s `series_or_default` silently falls back from one to the other. `io.py` copies between them on load and on save. A newcomer encounters:
```python
if col == "payee_selected" and "target_payee_selected" in df.columns:
    return df["target_payee_selected"]...
```
…and wonders: are they the same thing? Why do both exist?

The answer is in `documents/decisions/unified_review_model_schema.md`: `payee_selected` is a legacy unsuffixed alias kept for backward compatibility during the transition. But this is never mentioned in a code comment. Every new reader will spend 10 minutes figuring this out.

**Recommendation:** Add a one-line comment where the fallback logic lives: `# Legacy alias: payee_selected → target_payee_selected (see unified_review_model_schema.md)`.

### Finding 10 — Scripts carry significant business logic with sys.path hacks

**Severity: MEDIUM**

`scripts/build_proposed_transactions.py` (900+ lines) starts with:
```python
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
```

This is a code smell: the script bypasses normal package installation to import from `src/`. Other scripts do this too. The business logic in this script (deduplication, pairing orchestration, review-row construction) is untestable without the same `sys.path` hack. Tests use `importlib.util.spec_from_file_location` to load these scripts as modules.

**Recommendation:** Already flagged in plan.md as item H (pull proposal-generation logic into `src/`). Reinforcing: this is the biggest single barrier to a newcomer understanding the test/source relationship.

### Finding 11 — `review_reconcile.py` naming is misleading

**Severity: LOW**

The module name suggests "reviewing reconciliation results." It actually carries forward prior review decisions when rerunning build_proposed_transactions. The function names inside (`forward_reviewed_decisions`, etc.) are clear, but the module name makes a newcomer look in the wrong place.

### Finding 12 — Five test files for the review subsystem with opaque naming

**Severity: LOW**

| File | Actually tests |
|------|---------------|
| `test_review.py` | validation.py and state.py (unit tests) |
| `test_review_app.py` | Streamlit app via AppTest (integration) |
| `test_review_app_wrapper.py` | scripts/review_app.py launcher (CLI integration) |
| `test_review_io.py` | review_app/io.py (unit) |
| `test_review_perf.py` | Performance regression (synthetic benchmark) |

`test_review.py` is the one that tests `validation.py` and `state.py`, but its name suggests it tests "review" generically. A newcomer would expect `test_review_validation.py` and `test_review_state.py`.

### Finding 13 — Circular imports via function-body imports

**Severity: LOW**

Three locations use deferred imports to break circular dependencies:
- `state.py` → `validation.py` (in `primary_state_series()` and `apply_row_edit()`)
- `model.py` → `validation.py` (in `competing_row_scope()`)

A newcomer encountering `import ynab_il_importer.review_app.validation as review_validation` *inside a function body* wonders if this is a mistake. The circular dependency is structural: `state` ↔ `validation` ↔ `model` form a triangle. The current approach works but is not self-documenting.

**Recommendation:** A brief comment like `# Deferred import: circular dependency with validation.py` would eliminate the confusion.

### Finding 14 — `.astype("string").fillna("").str.strip()` repeated 15+ times

**Severity: LOW**

This defensive string-coercion chain appears throughout `state.py`, `validation.py`, `map_updates.py`, `upload_prep.py`, and `app.py`. Each instance is slightly different (some add `.str.casefold()`, some don't). A newcomer reads through the code and keeps seeing this incantation without understanding why it's necessary (answer: pandas CSV columns can contain NaN, floats, and non-string types).

Already flagged in Pass 3 as item #5. Reinforcing that this is also a *readability* issue, not just a style one.

### Finding 15 — `_text()` helper redefined identically in 3 modules

**Severity: LOW**

`validation.py`, `upload_prep.py` (as `_normalize_text`), and `map_updates.py` all define:
```python
def _text(value: Any) -> str:
    return str(value or "").strip()
```

Identical body, different names. A newcomer wonders if they differ. They don't.

### Finding 16 — `scripts/review_app.py` calls private API from `app.py`

**Severity: LOW**

The launcher script calls `review_app._build_arg_parser()`, `review_app._default_reviewed_path()`, and `review_app._effective_categories_path()`. All underscore-prefixed. If these are meant to be called from outside the module, they should be public.

### Finding 17 — `review_app_workflow.md` exists but isn't linked from README or REPOSITORY_LAYOUT

**Severity: LOW**

A well-written document about the review app loop exists at `documents/review_app_workflow.md`, but a newcomer would never find it because neither README nor REPOSITORY_LAYOUT references it.

---

### Holistic Assessment

**Things that are genuinely good:**
- The `documents/decisions/` directory is excellent — design decisions are explicit, reasoned, and versioned
- The review model vocabulary (Fix/Decide/Settled, component validation, competing-row resolution) is internally consistent and well-thought-out
- `model.py` (114 lines) is a good example of size and focus — small, clear functions, no mixed concerns
- `io.py` (111 lines) is similarly well-scoped
- `safe_types.py` and `normalize.py` are correctly extracted as shared utilities
- Test coverage is broad (210 tests) and tests like `test_review.py` serve as effective documentation of validation rules

**The core readability bottleneck:**
The codebase has a *top-heavy* problem. The entry points (README, REPOSITORY_LAYOUT) explain usage but not architecture. The package (`__init__.py`) is silent. The main application file (`app.py`) carries 1913 lines of mixed rendering, UI state management, and business-logic orchestration. The 25 flat modules have no visible grouping. A newcomer can understand any *individual* module, but cannot quickly build a mental map of how they relate.

**If I were grading this like a high school CS project:**
The student clearly understands the domain deeply and has built a working, well-tested system. The individual functions are generally well-named and do what they say. But the project reads like it was built incrementally (which it was) without stepping back to explain the whole to a reader. The README teaches *operation* but not *comprehension*. The package teaches *nothing* — you must read every file. The review app works but its main function is the length of a short story.

---

### Pass 4 Summary Table

| # | Severity | Category | Finding |
|---|----------|----------|---------|
| 1 | HIGH | Architecture | No architectural overview (data-flow diagram, module-to-stage map) |
| 2 | MEDIUM | Organization | 25 flat modules with no grouping or orientation |
| 3 | MEDIUM | Documentation | No module-level docstrings |
| 4 | HIGH | Readability | `main()` is 739 lines of sequential orchestration |
| 5 | MEDIUM | Design | Derived-state dict is a 25-key untyped bag |
| 6 | MEDIUM | Design | `_render_row_controls` has 14 parameters |
| 7 | LOW | Documentation | `EDITOR_STATE_PREFIXES`/`EDITOR_STATE_KEYS` unexplained |
| 8 | LOW | Readability | Inline HTML/CSS scattered across 5+ functions |
| 9 | MEDIUM | Documentation | Dual-column `payee_selected`/`target_payee_selected` never explained in code |
| 10 | MEDIUM | Organization | Scripts carry business logic with sys.path hacks |
| 11 | LOW | Naming | `review_reconcile.py` naming is misleading |
| 12 | LOW | Naming | Five test files for review subsystem with opaque naming |
| 13 | LOW | Design | Circular imports via function-body imports (without comments) |
| 14 | LOW | Style | `.astype("string").fillna("").str.strip()` repeated 15+ times |
| 15 | LOW | Style | `_text()` helper redefined identically in 3 modules |
| 16 | LOW | API | Launcher script calls private underscore-prefixed functions |
| 17 | LOW | Documentation | `review_app_workflow.md` not linked from README or REPOSITORY_LAYOUT |

**Totals:** 2 HIGH, 5 MEDIUM, 10 LOW

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
| README dangling reference | ✅ FIXED | `review_app_workflow.md` now exists |
| Account name map data quality | ✅ FIXED | Truncated rows removed |
| Perf test timing bound | ✅ RETAINED | Union-find cut 500-row time to ~7s; bound kept at `< 10` |
| --approved CLI flag | ✅ FIXED | Rejects invalid values with `ArgumentTypeError` |

---

## Pass 3 — Hostile Audit at Commit `68bbec5`

### Scope

Expanded scope: code style, modern Python idioms, algorithmic analysis, test quality, documentation accuracy, and data file integrity. Graded as a high-school CS project with emphasis on teaching good programming practice.

### What Codex Delivered

Codex addressed all 5 items from the Pass 2 FIX LIST:

1. **Union-find component discovery** — `precompute_components` rewritten from repeated-BFS O(C×n) to union-find O(n·α(n)). Implementation is **correct**: path compression with halving, union by rank, separate `first_by_source`/`first_by_target` dicts (preserving the intentional separation of source and target ID namespaces). `connected_component_mask` now delegates to `precompute_components`.
2. **`review_app_workflow.md`** — Created, accurate, well-organized. All 3 README references now resolve.
3. **`account_name_map.csv`** — Two truncated rows removed. File now has consistent 5-column structure, no trailing newline issues.
4. **`--approved` CLI fix** — `_parse_bool_arg` is a clean, testable function. Parser extracted to `_build_parser()`. Test covers accept + reject paths.
5. **`REPOSITORY_LAYOUT.md`** — Updated with `review_app/` subpackage and `safe_types.py`.

**All items verified. 210 tests passing. Union-find is algorithmically correct.**

### Findings

#### FIX 1 — PERFORMANCE: `validate_row` called twice per row (HIGH)

`blocker_series_with_components` calls `validate_row` for every row **twice**:

- Once inside `precompute_component_errors` → `review_component_errors` → iterrows + `validate_row(row)`
- Once inside the blocker_label list comprehension → `blocker_label(row)` → `validate_row(row)`

Measured: 1000 `validate_row` calls for 500 rows. Each call takes ~2.6ms. Total redundant work: ~1.3s at 500 rows.

**Fix**: Precompute row-level errors once and pass them to both consumers.

#### FIX 2 — PERFORMANCE: `normalize_decision_actions` wraps scalars in pd.Series (HIGH)

`validate_row` normalizes a single action string by wrapping it in a 1-element `pd.Series`:

```python
action = normalize_decision_actions(pd.Series([row.get("decision_action", "")])).iloc[0]
```

Measured overhead: creating and operating on a 1-element Series costs **1.03ms** per call. The equivalent scalar operation (`str.strip()`) costs **0.0002ms** — a **1,027× overhead**. At 1000 calls per blocker computation, this accounts for ~1s of the 7s total.

**Fix**: Add a scalar `normalize_decision_action(value: str) -> str` helper. Reserve the Series version for batch operations only.

#### FIX 3 — PERFORMANCE: `apply_row_edit` recomputes union-find per row (MEDIUM)

`state.py::apply_row_edit` line 517:
```python
from ynab_il_importer.review_app.validation import connected_component_mask
df.loc[connected_component_mask(df, idx), "reviewed"] = bool(reviewed)
```

`connected_component_mask` now delegates to `precompute_components` (full union-find over all rows). This runs once per `apply_row_edit` call. The caller `apply_review_state` already has a `component_map` but doesn't pass it through.

Impact: O(n) per touched row (not O(n²) anymore thanks to union-find), but still wasteful when the map is already computed. For typical k=1 row edits this is ~9ms, so LOW urgency, but a clean-code issue: the architectural intent of caching the component_map is undermined by a function that ignores it.

**Fix**: Accept optional `component_map` parameter in `apply_row_edit`. Use existing map to compute the mask.

#### FIX 4 — PERFORMANCE: `review_component_errors` receives full DataFrame (LOW)

`precompute_component_errors` calls `review_component_errors(df, start_idx, component_mask=...)` for each of 250 components, passing the **full 500-row DataFrame** each time. Inside, it does `df.loc[component_mask].copy()` to extract 2 rows. A single `groupby` would replace 250 mask-filter-copy operations with one grouped iteration.

Measured: 250× mask+copy costs 102ms vs groupby at 9ms. Not blocking at current sizes but a style issue — passing a 500-row frame to process 2 rows is the wrong abstraction.

**Fix (deferred)**: Refactor `precompute_component_errors` to use `df.groupby(component_series)` and pass each group directly to a simplified validation function.

#### FIX 5 — STYLE: `main()` is 739 lines (HIGH)

`app.py::main()` spans lines 1171–1909. Contains sidebar rendering, row view, grouped view, navigation, and session management all in one function. This is **untestable** and violates single-responsibility. The 289-line `_render_row_controls` (lines 871–1159) is similarly oversized, mixing form construction, submission logic, competing row resolution, and state mutation.

This is not new debt but the cleanup branch has not addressed it despite extracting 9 helper functions. The extraction moved leaf-level computations out but left the orchestration monolith intact.

**Fix**: Extract `_render_sidebar()`, `_render_row_view()`, `_render_group_view()`. Split `_render_row_controls` into `_build_edit_form()` and `_handle_form_submit()`.

#### FIX 6 — STYLE: Repeated `.astype("string").fillna("").str.strip()` (MEDIUM)

This 4-method chain appears **15 times** across `validation.py`, `state.py`, `io.py`, and `model.py`. `state.py` already has `series_or_default()` which does exactly this, but it's not used consistently. `validation.py` has its own `_id_series` doing the same thing.

**Fix**: Consolidate on a single `normalize_string_series(series)` helper in a shared location (e.g., `safe_types.py` or a new `review_app/common.py`).

#### FIX 7 — STYLE: String-literal dispatch for decision actions (MEDIUM)

Decision action values (`"create_source"`, `"keep_match"`, `"delete_both"`, etc.) appear as raw string literals in 20+ places across `validation.py`, `state.py`, and `app.py`. There are 5 separate `frozenset` / `set` constants at the top of `validation.py` grouping subsets of these strings, plus `NO_DECISION = "No decision"`. But the individual action names have no canonical definition — a typo like `"create_sourxe"` would silently fail to match any set.

**Fix**: Define a `DecisionAction` `StrEnum` (Python 3.11+, which is the project's minimum). Use members in all sets and comparisons. Typos then become import errors.

#### FIX 8 — STYLE: `iterrows()` in hot paths (MEDIUM)

5 `iterrows()` calls in the review_app package:
- `blocker_series_with_components` blocker_label comprehension (validation.py:210)
- `review_component_errors` per-row validation (validation.py:294)
- `primary_state_series` row-by-row state classification (state.py:260)
- `_load_categories` parsing loop (app.py:261)
- grouped view rendering loop (app.py:1709)

The first three are on the mutation hot path. `iterrows()` is the single slowest way to iterate a DataFrame — the pandas documentation itself discourages it. For rows that need scalar per-row logic, `df.apply(..., axis=1)` is marginally better but still slow. The real fix for `blocker_label` and `primary_state_series` is to vectorize: compute the blocker/state as masks and assign via `np.select` or chained `.where()`.

#### FIX 9 — STYLE: Dead code `accept_defaults_mask()` (LOW)

`state.py:78` — function returns `pd.Series([False] * len(df), ...)` unconditionally. No callers detected in the entire codebase. Remove it.

#### FIX 10 — STYLE: Circular import via lazy import (LOW)

`state.py:516` uses a function-body import to avoid circular dependency:
```python
from ynab_il_importer.review_app.validation import connected_component_mask
```
`state.py` → `validation.py` → `state.py` forms a cycle. The lazy import works but hides the dependency graph. The proper fix is to move shared types/functions (like component computation) to a module that doesn't import from either `state` or `validation`.

#### FIX 11 — STYLE: Duplicated column lists (LOW)

The editable-column list `["source_payee_selected", "source_category_selected", ..., "reviewed"]` appears 3 times in `state.py` (in `modified_mask`, `changed_mask`, and implicitly in `apply_row_edit`). Define once as a module constant.

#### FIX 12 — TEST: `test_prepare_ynab_upload_script.py` coverage gaps (MEDIUM)

- `_parse_bool_arg` accepts 6 values (`true`, `false`, `1`, `0`, `yes`, `no`) but tests only cover `true` and `false`.
- Rejection test uses `pytest.raises(SystemExit)` which passes for **any** argparse failure (missing required args, wrong flag name, etc.) — not specific to the `--approved` validation.
- No direct unit test for `_parse_bool_arg` — only tested indirectly through the parser.

**Fix**: Add `@pytest.mark.parametrize` over all 6 valid values + 3 invalid values. Test `_parse_bool_arg` directly.

#### FIX 13 — TEST: Perf test asserts duration but not correctness (LOW)

`test_blocker_series_with_components_smoke_500_rows` checks `duration < 10` and `len(blocker_series) == len(df)` but never checks that the blocker values are **correct** — a broken-but-fast implementation would pass. Add at least one assertion on the content (e.g., all values are in the known blocker vocabulary).

#### FIX 14 — TEST: Multi-field assertion tests (LOW)

`test_review_io.py::test_save_then_load_round_trip_preserves_review_fields` has 14 assertions in one test. When one fails, the remaining assertions are skipped, making it hard to diagnose which fields broke. Use `pytest.raises` subtests or split per field-group.

#### FIX 15 — DOC: README `build-payee-map` example is incomplete (LOW)

README lines 160–164 show `pixi run ynab-il build-payee-map` but omit required arguments (`--parsed`, `--matched-pairs`, `--out-dir`). Not copy-paste-runnable.

### Performance Profile After Union-Find

Measured at 500 rows:

| Component | Time | % of Total |
|-----------|------|-----------|
| `precompute_components` (union-find) | **0.009s** | <1% |
| `precompute_component_errors` (250 components × `review_component_errors`) | **5.08s** | 72% |
| `iterrows` + `blocker_label` (500 rows × `validate_row`) | **2.16s** | 31% |
| `uncategorized_mask` | 0.002s | <1% |
| **Total: `blocker_series_with_components`** | **~7.0s** | 100% |

The union-find itself is blazing fast. The bottleneck has shifted entirely to per-row validation overhead:
- `validate_row` costs ~2.6ms/call (dominated by 1-element `pd.Series` creation in `normalize_decision_actions`)
- It runs 2× per row (once in component error checking, once in blocker labeling)
- `review_component_errors` creates pandas Series operations on 2-row subframes 250 times

**Key insight**: The algorithmic win from union-find (O(n²) → O(n·α(n))) improved wall time by only ~19% (8.6s → 7.0s) because the asymptotic bottleneck was never the graph algorithm — it was the O(n) scan with ~7ms/row constant factor from pandas per-row overhead. Halving the constant factor (eliminate double-validation + scalar Series wrapping) would yield a larger improvement than the algorithmic change did.

### Scaling Projection

| Rows | Current | After FIX 1+2 (est.) |
|------|---------|----------------------|
| 240 | 3.8s | ~1.5s |
| 500 | 7.0s | ~2.8s |
| 1000 | 13.5s | ~5.4s |

### Pass 3 FIX LIST Summary

| # | Severity | Category | Item |
|---|----------|----------|------|
| 1 | HIGH | Perf | `validate_row` called 2× per row — precompute row errors once |
| 2 | HIGH | Perf | `normalize_decision_actions` wraps scalars in pd.Series (1,027× overhead) |
| 3 | MEDIUM | Perf | `apply_row_edit` ignores available `component_map`, recomputes union-find |
| 4 | LOW | Perf | `precompute_component_errors` passes full DataFrame for 2-row slices |
| 5 | HIGH | Style | `main()` is 739 lines, `_render_row_controls` is 289 lines |
| 6 | MEDIUM | Style | `.astype("string").fillna("").str.strip()` repeated 15 times |
| 7 | MEDIUM | Style | Decision actions as raw strings — should be `StrEnum` |
| 8 | MEDIUM | Style | `iterrows()` in hot paths (5 call sites) |
| 9 | LOW | Style | Dead code `accept_defaults_mask()` |
| 10 | LOW | Style | Circular import via function-body `from ... import` |
| 11 | LOW | Style | Editable-column list duplicated 3 times |
| 12 | MEDIUM | Test | `test_prepare_ynab_upload_script.py` covers 2 of 6 valid values |
| 13 | LOW | Test | Perf test asserts timing but not correctness of output |
| 14 | LOW | Test | Multi-assertion round-trip test hides failures |
| 15 | LOW | Doc | README `build-payee-map` example missing required args |

### Verdict

**PASS — all prior audit items addressed correctly. No regressions. Union-find is correct.**

The remaining findings are style, idiom, and micro-optimization issues. None are blocking. The most impactful next move would be FIX 1+2 (eliminate double-validation and scalar-Series overhead) which would roughly halve the per-mutation cost with minimal risk. FIX 5 (splitting `main()`) is the biggest maintainability debt but highest effort.

### Pass 3 Status Table

| Area | Status | Assessment |
|------|--------|------------|
| Union-find correctness | ✅ VERIFIED | Path compression + union by rank. Separate source/target namespaces. Edge cases handled. |
| Prior FIX LIST items | ✅ ALL FIXED | 5/5 items from Pass 2 addressed |
| Test suite | ✅ GREEN | 210 tests passing in 71s |
| `validate_row` double-call | ⚠️ NEW | 1000 calls for 500 rows |
| Scalar pd.Series overhead | ⚠️ NEW | 1,027× overhead vs plain string ops |
| `main()` function size | ⚠️ EXISTING | 739 lines — untestable monolith |
| String normalization DRY | ⚠️ EXISTING | 15 repetitions of same 4-method chain |
| Decision action type safety | ⚠️ NEW | Raw string literals, no `StrEnum` |
| Per-mutation cost | ✅ IMPROVED | 7.0s at 500 rows (was 8.6s), 3.8s at 240 rows (was 4.0s) |
| Non-mutation rerun cost | ✅ FREE | Generation-counter caching verified |
