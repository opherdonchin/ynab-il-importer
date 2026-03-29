# Hostile Audit Report — `code-review-refactor` Branch

Branch: `code-review-refactor` at commit `aa347a9`  
Audit date: 2026-03-30  
Auditor: Copilot (hostile audit against `hostile_audit_prompt.md` criteria)

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

## SUMMARY SCORECARD

| Cleanup Plan Phase | Status | Assessment |
|-------------------|--------|------------|
| Phase 0: Trustworthy baseline | ✅ DONE | 200 tests passing |
| Phase 1: Safe boolean handling | ✅ DONE | All CSV-backed boolean coercion replaced with `normalize_flag_series` |
| Phase 2: Component/blocker performance | ⚠️ PARTIAL | Precomputation exists but is not cached between reruns. FIX 1 required. |
| Phase 3: Grouped-view performance | ✅ DONE | Fingerprint groupby precomputation works correctly |
| Phase 4: Review semantics extraction | ⚠️ PARTIAL | 6 functions moved out; 3 business logic functions remain in app.py (FIX 4) |
| Phase 5: Focused follow-up fixes | ✅ DONE | `changed_mask` NaN bug fixed, safe_types module created |
| Performance verification | ❌ MISSING | No benchmark or smoke test exists (FIX 5) |
