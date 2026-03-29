# Code Cleanup Plan for Codex

This plan is for a Codex agent to implement, followed by human review.
All work is on branch `code-review-refactor`.

## Ground Rules

1. **Run `pixi run pytest` after every logical unit of work.** If tests fail, fix
   before moving on. Never leave the branch in a failing state.
2. **Do not change external behavior** unless explicitly called out as a bug fix.
3. **Preserve every existing test.** Add new tests; never delete passing tests.
4. **Commit after each numbered task** with a descriptive message referencing the
   task number (e.g. `task 1: add safe_bool_series to validation`).
5. **Prefer small, reviewable commits** over one giant change.
6. **Do not add docstrings, comments, or type annotations** to code you did not
   change in substance. Do not refactor code that this plan does not mention.

---

## Task 0: Orientation

Read these files first:
- `documents/project_context.md`
- `documents/plan.md`
- `documents/decisions/unified_review_model_design.md`
- `documents/decisions/unified_review_model_schema.md`

These give the domain context: source/target review model, decision actions,
selected-column conventions, and the review-app purpose.

Run the full test suite once to establish a green baseline:
```
pixi run pytest
```

---

## Task 1: Fix the `astype(bool)` bug on string-backed CSV columns

### Problem

`upload_prep.py` lines 154 and 185 do:
```python
active = active[~active["hidden"].astype(bool)]
```
When the `hidden` column contains string `"False"` (written by
`build_categories_from_ynab_snapshot.py` lines 51, 76), pandas coerces the
non-empty string `"False"` to `True`, **inverting** the filter. Visible
categories are excluded; hidden ones are kept.

Similarly, `upload_prep.py` line 58 does `df["reviewed"].astype(bool)` directly.
It happens to be safe today because `review_app/io.py` normalizes `reviewed` on
load, but this is fragile — it will break if a CSV is loaded outside the review
app pipeline.

### What to do

1. In `src/ynab_il_importer/review_app/validation.py`, the function
   `normalize_flag_series` already exists and is correct:
   ```python
   def normalize_flag_series(series: pd.Series) -> pd.Series:
       text = series.astype("string").fillna("").str.strip().str.lower()
       return text.isin(TRUE_VALUES)
   ```
   This is the canonical safe boolean parser. `TRUE_VALUES` is
   `{"1", "true", "t", "yes", "y"}`.

2. **Move** `normalize_flag_series` and `TRUE_VALUES` out of
   `review_app/validation.py` into a new shared module:
   `src/ynab_il_importer/safe_types.py`. Keep the original imports working by
   re-exporting from `validation.py`:
   ```python
   from ynab_il_importer.safe_types import normalize_flag_series, TRUE_VALUES
   ```

3. **Replace every `astype(bool)` call on a column that could hold CSV string
   data** with `normalize_flag_series(series)`. The full list of call sites to
   audit and fix:

   | File | Line(s) | Column | Action |
   |------|---------|--------|--------|
   | `upload_prep.py` | 58 | `reviewed` | Replace with `normalize_flag_series` |
   | `upload_prep.py` | 154 | `hidden` | Replace with `normalize_flag_series` |
   | `upload_prep.py` | 185 | `hidden` | Replace with `normalize_flag_series` |
   | `map_updates.py` | 31 | generic column | Replace with `normalize_flag_series` |
   | `scripts/prepare_ynab_upload.py` | 112 | `reviewed` | Replace with `normalize_flag_series` |
   | `review_app/app.py` | 1453 | `reviewed` (from df column) | Evaluate — if this reads from session_state df that was already normalized on load, confirm it's safe. If it reads the raw column, replace. |
   | `review_app/app.py` | 1589 | `reviewed` | Same evaluation. |

   **Do not change** `astype(bool)` calls on columns that are internally
   computed as boolean (e.g. `updated_mask`, `eligible_mask`, `saved`). Those
   are already safe. Use judgment: if the upstream is `pd.Series` of actual
   `bool` or `int`, leave it alone.

4. In `build_categories_from_ynab_snapshot.py`, change:
   ```python
   "hidden": ["False"] * len(categories),
   ```
   to:
   ```python
   "hidden": [False] * len(categories),
   ```
   at both line 51 and line 76. This fixes the producer side too.

5. **Add a test** in a new file `tests/test_safe_types.py`:
   - `test_normalize_flag_series_string_false` — asserts `"False"` → `False`
   - `test_normalize_flag_series_string_true` — asserts `"True"` → `True`
   - `test_normalize_flag_series_string_zero` — asserts `"0"` → `False`
   - `test_normalize_flag_series_empty` — asserts `""` → `False`
   - `test_normalize_flag_series_nan` — asserts `NaN` → `False`
   - `test_normalize_flag_series_bool_true` — asserts Python `True` → `True`

6. **Add a test** that exercises the hidden-category filter through upload_prep.
   Create a DataFrame with `hidden="False"` (string) and confirm that
   `_category_lookup` keeps those categories (does not incorrectly exclude them).

### Definition of done
- `pixi run pytest` passes
- `grep -rn "\.astype(bool)" src/ scripts/` shows zero hits on CSV-backed string
  columns (internal boolean masks are fine)

---

## Task 2: Eliminate the duplicated `connected_component_mask`

### Problem

`connected_component_mask` is defined identically in:
- `src/ynab_il_importer/review_app/state.py` line 226
- `src/ynab_il_importer/review_app/validation.py` line 93

The copy in `state.py` is called from `state.py:apply_row_edit` (line 333 via
`related_rows_mask` which doesn't use it directly — but `apply_row_edit` is
nearby). The copy in `validation.py` is called from `validation.py` itself and
from `app.py`.

### What to do

1. **Delete** `connected_component_mask` from `state.py`.
2. **Import** it from `validation.py` in `state.py`:
   ```python
   from ynab_il_importer.review_app.validation import connected_component_mask
   ```
3. Confirm all call sites still resolve. The callers are:
   - `app.py` line 657 → calls `review_validation.connected_component_mask` → still works
   - `app.py` line 846 → same
   - `state.py` line ~280 area (if used by `related_rows_mask`) → needs the import
   - `validation.py` line 172 → local call, still works

4. Run tests.

### Definition of done
- Only one definition of `connected_component_mask` exists in the codebase
- `pixi run pytest` passes

---

## Task 3: Precompute connected components to fix the O(n²) blocker computation

### Problem

`app.py:_component_error_lookup` iterates every row, calls
`validation.connected_component_mask` per unseen row (which scans the full df),
then `review_component_errors` calls `connected_component_mask` *again*
internally. This is O(n × m) where m is dataframe size. For 4000 rows this
takes ~46 seconds.

### What to do

1. **Add** a new function in `validation.py`:
   ```python
   def precompute_components(df: pd.DataFrame) -> dict[Any, int]:
       """Return a mapping from row index to component id (integer).
       
       Each connected component gets a unique integer label.
       """
   ```
   Implementation: iterate rows, skip seen ones, call
   `connected_component_mask` once per component, assign a component label to
   all rows in that component. Return `{row_idx: component_label}`.

2. **Add** a second function:
   ```python
   def precompute_component_errors(
       df: pd.DataFrame,
       component_map: dict[Any, int],
   ) -> dict[int, list[str]]:
       """Validate each component once. Return {component_label: [errors]}."""
   ```
   For each unique component label, pick one start_idx, call
   `review_component_errors` once (which internally calls
   `connected_component_mask` — but only once per component now).

3. **Rewrite** `app.py:_component_error_lookup` to:
   ```python
   def _component_error_lookup(df: pd.DataFrame) -> dict[Any, list[str]]:
       component_map = review_validation.precompute_components(df)
       component_errors = review_validation.precompute_component_errors(df, component_map)
       return {idx: component_errors.get(label, []) for idx, label in component_map.items()}
   ```

4. **Optimize further**: Modify `review_component_errors` to accept an optional
   pre-built component mask or component DataFrame, so it doesn't call
   `connected_component_mask` again internally. This eliminates the double-scan
   per component. Signature:
   ```python
   def review_component_errors(
       df: pd.DataFrame,
       start_idx: Any,
       *,
       component_mask: pd.Series | None = None,
   ) -> list[str]:
   ```
   If `component_mask` is provided, use `df.loc[component_mask]` instead of
   calling `connected_component_mask`.

5. **Add tests**:
   - `test_precompute_components_single_component` — all rows connected → one label
   - `test_precompute_components_two_components` — disconnected groups get different labels
   - `test_precompute_component_errors_propagates` — errors from one row appear for all rows in its component

### Definition of done
- `_component_error_lookup` makes at most N calls to `connected_component_mask`
  where N is the number of distinct components, not the number of rows
- `pixi run pytest` passes

---

## Task 4: Precompute fingerprint groups for grouped mode

### Problem

In `app.py` grouped mode (around line 1830+), for each fingerprint on the
current page:
```python
group = df[df["fingerprint"].astype("string").fillna("") == fp]
```
This rescans the entire dataframe per fingerprint. Then `iterrows()` is called
twice on the group to collect payee/category options.

### What to do

1. **Before** the grouped-mode pagination loop, build the index once:
   ```python
   fp_series = df["fingerprint"].astype("string").fillna("")
   fp_to_indices = fp_series.groupby(fp_series).apply(lambda g: g.index.tolist()).to_dict()
   ```
   Or equivalently use `df.groupby(...)`.

2. **Replace** the per-fingerprint filter:
   ```python
   group = df.loc[fp_to_indices.get(fp, [])]
   ```

3. **Replace** the `iterrows()` option-collection loops with vectorized logic:
   ```python
   group_payee_options = (
       group["payee_options"]
       .astype("string").fillna("")
       .str.split(";")
       .explode()
       .str.strip()
       .loc[lambda s: s != ""]
       .drop_duplicates()
       .tolist()
   )
   ```
   Same pattern for `category_options`.

4. This pattern appears twice in grouped mode (once for the group header
   summary around line 1843, and again around line 1927 inside the expander).
   Both should use the precomputed group and vectorized options.

### Definition of done
- No `df[df["fingerprint"]... == fp]` scans inside a loop
- No `iterrows()` for option collection in grouped mode
- `pixi run pytest` passes

---

## Task 5: Extract review-semantic functions from `app.py` into `state.py`/`validation.py`

### Problem

`app.py` contains ~15 functions that compute review semantics (blockers,
primary state, allowed actions, competing-row resolution) rather than rendering
UI. These cannot be unit-tested independently, and they make `app.py` ~2180
lines.

### What to do

Move each function below. The target module is chosen based on responsibility:

**Move to `validation.py`** (these compute errors, blockers, allowed actions):
- `_component_error_lookup` → `component_error_lookup` (already partially
  refactored in Task 3)
- `_blocker_label` → `blocker_label`
- `_blocker_series` → `blocker_series`
- `_allowed_decision_actions` → `allowed_decision_actions`
- `_apply_review_state` → `apply_review_state`

**Move to `state.py`** (these compute derived state series):
- `_primary_state_series` → `primary_state_series`
- `_row_kind_series` → `row_kind_series`
- `_action_series` → `action_series`
- `_suggestion_series` → `suggestion_series`
- `_map_update_filter_series` → `map_update_filter_series`
- `_search_text_series` → `search_text_series`
- `_truthy_series` → `truthy_series`
- `_uncategorized_mask` → `uncategorized_mask`
- `_required_category_missing_mask` → `required_category_missing_mask`

**Move to `model.py`** (this computes competing-row scope):
- `_competing_row_scope` → `competing_row_scope`
- `_apply_competing_row_resolution` → `apply_competing_row_resolution`

### Rules for the move

- Remove the leading underscore (these become public APIs of their module).
- Import them back into `app.py` and call them via the module prefix
  (e.g. `review_state.primary_state_series(df, blocker_series)`).
- Do not change the function body except to update internal cross-references
  (e.g. if `_blocker_series` calls `_uncategorized_mask`, update to call the
  moved version).
- Do not change signatures.
- After moving, `app.py` should be rendering/wiring only — no business logic
  functions defined in it except tiny formatting helpers.

### Definition of done
- `app.py` has zero functions that compute review semantics (blockers, state,
  validation, competing-row logic)
- `app.py` line count drops by ~500 lines
- `pixi run pytest` passes

---

## Task 6: Add unit tests for the extracted functions

### Problem

The review app has 26 integration tests but nearly zero unit tests for the
individual state/validation functions. The extraction in Task 5 makes unit
testing trivial.

### What to do

Add tests to `tests/test_review.py` (or a new `tests/test_review_state.py` if
the file is getting large):

1. **`blocker_series`**: build a small df with a reviewed row missing a payee →
   assert blocker is `"Missing payee"`.
2. **`blocker_series`**: build a df where all rows are settled → assert all
   blockers are `"None"`.
3. **`primary_state_series`**: build a df with one Fix, one Decide, one Settled
   row → assert the three-way mapping.
4. **`allowed_decision_actions`**: test institutional row excludes source mutations.
5. **`allowed_decision_actions`**: test cross-budget row includes source mutations.
6. **`apply_review_state`**: test that marking a row reviewed with `No decision`
   returns an error.
7. **`apply_competing_row_resolution`**: test that accepting `keep_match`
   auto-ignores competing rows.
8. **`uncategorized_mask`**: test that `"Uncategorized"` in the category column
   is detected.
9. **`search_text_series`**: build a row with known payee/memo → assert the
   search text contains both.

### Definition of done
- At least 9 new unit tests covering the extracted functions
- `pixi run pytest` passes

---

## Task 7: Fix the `changed_mask` NaN-comparison weakness

### Problem

In `state.py:changed_mask` (line ~131):
```python
aligned = baseline.reindex(current.index)
changed = (current != aligned).any(axis=1)
```
When `current` has rows not present in `baseline`, `reindex` fills with `NaN`.
Then `"some_value" != NaN` evaluates to `NaN` (not `True`) due to pandas NaN
propagation. `.any(axis=1)` treats `NaN` as `False`. **Result: new rows not in
baseline are NOT marked as changed.**

In practice this may not fire often because the review app loads both `df` and
`base` from the same file. But if it ever does fire, it silently drops new rows
from the "changed" set, which means they won't show the "Unsaved" badge.

### What to do

1. After the `reindex`:
   ```python
   aligned = baseline.reindex(current.index)
   missing_in_base = aligned.isna().all(axis=1)
   changed = (current != aligned).any(axis=1) | missing_in_base
   ```
   (Use `.fillna` approach if preferred — the key is that rows absent from
   baseline must be marked as changed.)

2. Apply the same fix to the fallback path (line ~136):
   ```python
   baseline = base[cols].reindex(df.index)
   missing_in_base = baseline.isna().all(axis=1)
   return (current != baseline).any(axis=1) | missing_in_base
   ```

3. **Add a test**: create a `df` with 3 rows and a `base` with only 2 of those
   rows. Assert that `changed_mask` returns `True` for the new row.

### Definition of done
- New rows not in baseline are always marked as changed
- `pixi run pytest` passes

---

## Task 8: Add YNAB API rate-limit resilience

### Problem

`ynab_api.py` has no rate limiting, no retry logic, and no error differentiation.
A 429 from YNAB crashes the process identically to a 500 or a network timeout.

### What to do

1. Add a retry wrapper using `time.sleep` and exponential backoff
   (do NOT add a new dependency like `tenacity`). Place it in `ynab_api.py`:
   ```python
   def _request_with_retry(method, url, *, max_retries=3, **kwargs):
       for attempt in range(max_retries + 1):
           response = method(url, **kwargs)
           if response.status_code == 429:
               wait = min(2 ** attempt * 2, 30)
               time.sleep(wait)
               continue
           if response.status_code >= 500 and attempt < max_retries:
               time.sleep(2 ** attempt)
               continue
           return response
       return response  # return last response even if still failing
   ```

2. Use this in `_ynab_get` and `_ynab_post` (and `_ynab_patch` if it exists).

3. Differentiate error messages:
   - 401: `"YNAB API authentication failed (check API token)"`
   - 429: `"YNAB API rate limit exceeded after retries"`
   - 5xx: `"YNAB API server error ({status_code})"`

4. **Add tests** using `unittest.mock.patch` on `requests.get`/`requests.post`:
   - Simulate a 429 → retry → 200 → success
   - Simulate a 500 → retry → 200 → success
   - Simulate a 429 → 429 → 429 → 429 → raise

### Definition of done
- YNAB API calls retry on 429/5xx with backoff
- Error messages distinguish auth failure from rate limit from server error
- `pixi run pytest` passes

---

## Task 9: Cleanup — remove dead/redundant code from `app.py`

After Tasks 1–8, do a cleanup pass:

1. **Remove** any functions from `app.py` that were fully moved in Task 5
   and are no longer called locally.
2. **Remove** any `import` statements that are no longer used.
3. **Verify** `app.py` line count is under ~1400 lines. If it's still over
   1500, look for additional rendering helpers that could move to a new
   `review_app/rendering.py` module (CSS injection, badge formatting, etc.).
4. **Run** `pixi run pytest`.

### Definition of done
- No dead imports or unreachable functions in `app.py`
- `pixi run pytest` passes
- Commit with message `task 9: cleanup dead code from app.py`

---

## Task 10: General sweep — find and fix anything else you notice

This task gives Codex freedom to catch additional issues. Scan the codebase for:

1. **Any remaining `astype(bool)` on CSV-backed columns** that Task 1 may have
   missed. Check `scripts/` as well.
2. **Any `iterrows()` in hot paths** (per-rerun, per-interaction). Cold-path
   `iterrows()` in batch scripts is acceptable.
3. **Bare `except Exception: pass`** that swallows errors in places where the
   failure should be reported. The ones in `is_proper_format()` functions are
   fine. Look for others.
4. **Inconsistent text normalization**: the canonical pattern is
   `.astype("string").fillna("").str.strip()`. If you see
   `.str.lower().str.strip()` vs `.str.strip().str.lower()` inconsistencies,
   pick the canonical order and align.
5. **Duplicate helper functions** that have identical implementations in
   multiple files. Consolidate only if the deduplication is safe (same
   signature, same callers).

Do **not** refactor large structural changes beyond what is listed. Do **not**
add features. Keep changes minimal and test-preserving.

### Definition of done
- No new bugs introduced
- `pixi run pytest` passes
- Commit with message `task 10: general sweep fixes`

---

## Commit checklist

After all tasks, run:
```
pixi run pytest
```

If all tests pass, create a final summary commit:
```
git add -A
git commit -m "code-review-refactor: all tasks complete"
```

The branch is ready for human review at that point.
