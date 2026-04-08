# Task: Migrate review_app session state from pandas to Polars

## Background

Read `documents/plan.md` and `documents/project_context.md` first.

The review app (`src/ynab_il_importer/review_app/app.py`) runs as a Streamlit app. It keeps the active review working frame in `st.session_state` as a `pd.DataFrame`. All the actual review logic already operates natively on `pl.DataFrame` — functions like `review_state.apply_row_edit`, `review_model.apply_to_same_fingerprint`, `review_validation.apply_review_state` all take and return `pl.DataFrame`. The pandas DataFrames are only used as a storage and access intermediary, with adapter wrappers converting in and out at each call.

The goal of this task is to remove all of that and hold `pl.DataFrame` in session state directly.

## Scope

Files to migrate (ordered by dependency):

1. **`src/ynab_il_importer/review_app/state.py`** — most public functions still take `pd.DataFrame`
2. **`src/ynab_il_importer/review_app/validation.py`** — most validation functions still take `pd.DataFrame`
3. **`src/ynab_il_importer/map_updates.py`** — `build_map_update_candidates` takes `pd.DataFrame`
4. **`src/ynab_il_importer/review_app/app.py`** — session state, adapter wrappers, row access patterns

**Do not touch:**
- `src/ynab_il_importer/review_app/io.py` — boundary layer, already clean
- `src/ynab_il_importer/review_app/working_schema.py` — already Polars
- `src/ynab_il_importer/review_app/model.py` — already Polars except for two inner functions; those are called only from within verified-Polars wrappers
- `src/ynab_il_importer/artifacts/` or `src/ynab_il_importer/upload_prep.py` — not in scope

## The "index" problem — decide this first

The current pandas working frame uses `.index` (integer 0, 1, 2, …) as row identifiers. Every row in the app has an `idx` that is its pandas index label. Key usage sites:

- `df.loc[idx]` — get a row by label
- `filtered.index` — list of indices after filtering
- `updated.loc[idx]` — update a row by label
- widget state keys like `f"payee_select_{idx}"`

**Read how `apply_row_edit` and `apply_to_same_fingerprint` use `idx` in `state.py`** before migrating. These already work with Polars and handle `idx`. Understanding what they expect tells you the representation to use.

The natural Polars approach is: row position is an integer 0, 1, 2, …, and after filtering you return positions relative to the original unfiltered working frame. The working frame already attaches `"_row_pos"` via `with_row_index("_row_pos")` in the helpers view. Row access becomes `df.row(idx)` (returns a tuple) or `df.to_dicts()[idx]` (returns a dict), or for mutation you use `with_columns` + `when/then` guarded by position equality.

Confirm this is consistent with what `apply_row_edit` already expects before proceeding.

## Migration approach

### Step 1: Read the whole existing code

Before making any changes, read these files completely:
- `src/ynab_il_importer/review_app/state.py`
- `src/ynab_il_importer/review_app/validation.py`
- `src/ynab_il_importer/map_updates.py`
- `src/ynab_il_importer/review_app/app.py`

Also check all their tests under `tests/`.

### Step 2: Migrate state.py

For each function that takes `pd.DataFrame`, convert the signature and body to `pl.DataFrame`. Patterns:

| pandas | polars |
|--------|--------|
| `df: pd.DataFrame` | `df: pl.DataFrame` |
| `df.empty` | `df.is_empty()` |
| `df.columns` | `df.columns` (same) |
| `df[col]` | `df[col]` or `df.get_column(col)` |
| `df.loc[idx]` as dict | `df.row(idx, named=True)` |
| `df.index` (as list of ints) | `list(range(len(df)))` |
| `pd.Series([...], index=df.index)` | `pl.Series([...])` (no index) |
| `series.isin([...])` | `series.is_in([...])` |
| `series.fillna("")` | `series.fill_null("")` |
| `series.astype("string").str.strip()` | `series.cast(pl.Utf8).str.strip_chars()` |
| `series.eq(other)` | `series == other` |
| `series.any()` / `series.all()` | `.any()` / `.all()` on `pl.Series` |
| `(df[cols] != other[cols]).any(axis=1)` | row-by-row comparison via Polars `.with_columns` + element comparison |
| `df.copy()` | not needed — Polars is immutable |
| `pd.concat([a, b], ignore_index=True)` | `pl.concat([a, b])` |
| `df.groupby(col).agg(...)` | `df.group_by(col).agg(...)` |
| `df.reset_index(drop=True)` | not needed |
| `df.sort_values(col)` | `df.sort(col)` |
| `df[mask]` where mask is bool series | `df.filter(mask)` |

For functions that return `pd.Series` (like masks, flags), return `pl.Series` instead. Check whether callers in `app.py` use the result as a boolean mask — if so, `pl.Series` of `pl.Boolean` is the direct replacement.

For `view_row_lookup(view: pl.DataFrame, index: pd.Index | list[Any])` — change `pd.Index` to `list[int]` (it already accepts `list[Any]`).

For `apply_filters` which returns `pd.DataFrame`, return `pl.DataFrame` instead.

Run `pixi run pytest tests/ -q` after completing state.py. Fix any failures before proceeding.

### Step 3: Migrate validation.py

Same patterns as above. Key function `inconsistent_fingerprints` does groupby/nunique — use `df.group_by("fingerprint").agg(...)` in Polars. `build_validation_state` and `refresh_validation_state` iterate with `.iterrows()` — replace with `.to_dicts()` iteration or index-based access.

`compute_components` already has a Polars branch — simplify it to only use the Polars path.

Run tests after completing validation.py.

### Step 4: Migrate map_updates.py

`build_map_update_candidates(current_df, base_df)` takes two `pd.DataFrame` inputs. Migrate to `pl.DataFrame`. The function does groupby/agg/sort — all have direct Polars equivalents. The internal helpers `_string_series`, `_bool_series`, `_selected_series`, `_normalize_for_compare`, `_changed_mask` can all become private Polars operations. It also calls `export.write_dataframe` — check what that expects and update if needed.

Run tests after completing map_updates.py.

### Step 5: Migrate app.py

With state.py, validation.py, map_updates.py all taking `pl.DataFrame`, app.py is now straightforward:

1. **Remove adapter wrappers** — `_call_apply_row_edit`, `_call_apply_to_same_fingerprint`, `_call_apply_competing_row_resolution`, `_call_apply_review_state`, `_accept_reviewed_components` all wrap Polars functions with pd→Polars→pd conversion. Remove them. Call the underlying functions directly.

2. **Fix `_load_df` and `_load_base`** — remove `.to_pandas()` at the end:
   ```python
   # Before:
   df = review_io.project_review_artifact_to_working_dataframe(...).to_pandas()
   # After:
   df = review_io.project_review_artifact_to_working_dataframe(...)
   ```

3. **Fix `_set_review_frames`** — `df` is now `pl.DataFrame`. Remove `include_index=False` from `pl.from_pandas(df, include_index=False)` — just pass `df` directly to `working_schema.build_working_dataframe(df)`. Remove `_refresh_validation_state(df, ...)` call — check if validation_state refresh still needs the frame.

4. **Fix `_require_groupable_review_rows`** — rewrite to use Polars:
   ```python
   def _require_groupable_review_rows(df: pl.DataFrame) -> None:
       if "fingerprint" not in df.columns:
           raise ValueError("...")
       blank = df.filter(pl.col("fingerprint").is_null() | (pl.col("fingerprint").str.strip_chars() == ""))
       if blank.height > 0:
           raise ValueError("...")
   ```

5. **Fix `_canonical_review_bundle`** — `df` is now `pl.DataFrame`. Change type annotation and remove `pl.from_pandas(df, include_index=False)`. Change `helper_lookup = review_state.view_row_lookup(helpers, df.index)` to `helper_lookup = review_state.view_row_lookup(helpers, list(range(len(df))))`.

6. **Fix `_apply_staged_row_widget_values`** — this is the most involved change. Currently iterates over `indices` and does `updated.loc[idx]`. With Polars:
   - `updated` is now `pl.DataFrame`
   - `row = updated.row(idx, named=True)` gives a dict for row at position `idx`
   - `_call_apply_row_edit(updated, idx, ...)` → `review_state.apply_row_edit(updated, idx, ...)`
   - `if idx not in updated.index` → `if idx >= len(updated)` (or `idx < 0`)
   - `updated = _call_apply_row_edit(updated, idx, ...)` → `updated = review_state.apply_row_edit(updated, idx, ...)`
   - Remove `df.copy()` (not needed for Polars)
   - `_selected_side_value(row, side, field)` takes `row: pd.Series` — change to take `row: dict` (it already uses `row.get()` which works for both)
   - Update `_selected_side_value` signature to `row: dict`

7. **Fix `_grouped_row_indices`** — this currently takes `pd.DataFrame` and builds a Polars frame from it. With Polars input, simplify:
   ```python
   def _grouped_row_indices(filtered: pl.DataFrame) -> tuple[list[str], dict[str, list[int]]]:
       if filtered.is_empty():
           return [], {}
       fingerprints, position_map = review_state.grouped_row_indices(
           filtered.select("fingerprint")
       )
       return fingerprints, position_map
   ```

8. **Fix `_compute_derived_state`** — all calls to `review_state.*` and `review_validation.*` functions now pass `pl.DataFrame` instead of `pd.DataFrame`. Replace any `pd.Series` construction with `pl.Series`. Replace `save_state = pd.Series(...)` with `pl.Series(...)`. Replace `progress_tag = pd.Series(...)` with `pl.Series(...)`. Check `map_updates.build_map_update_candidates(current_df, base_df)` — update to pass Polars.

9. **Fix `_render_row_controls`** — `row = df.loc[idx]` → `row = df.row(idx, named=True)`. Check all `row.get(...)` calls — they work the same for dicts.

10. **Fix `_render_target_split_editor_dialog`** — `if idx not in df.index` → `if idx >= len(df) or idx < 0`. `row = df.loc[idx]` → `row = df.row(idx, named=True)`.

11. **Fix `_format_amount` and `_pick_summary_text` and `_row_context_lines` and `_summary_date` and `_summary_account` and `_target_split_editor_rows` and `_source_context_caption`** — all take `row: pd.Series`. Change to `row: dict`. They all use `row.get(...)` which already works on dicts.

12. **Fix `_split_summary_suffix` and `_helper_text`** — take `helper_row: pd.Series | None`. Change to `helper_row: dict | None`.

13. **Fix `_render_row_details`** — `row` comes from `df.loc[idx]`. Change to dict.

14. **Fix `_render_split_action_buttons`** and `_open_target_split_editor`** and `_close_target_split_editor`** — take `row: pd.Series`. Change to `row: dict`.

15. **Fix `_format_option_summary`** if it uses pd-specific operations.

16. **Fix `_load_categories`** — uses `df.iterrows()`. Convert to `df.to_dicts()` or `for row in df.iter_rows(named=True):`.

17. **Remove `import pandas as pd`** from app.py once all usages are gone.

18. Check for any remaining `pd.to_numeric(...)` calls — replace with Python `float(...)` wrapped in try/except or just direct float conversion.

Run tests after completing app.py.

### Step 6: Final verification

Run `pixi run pytest tests/ -q` — all 306 tests must pass (or more if new tests were added). The working frame type is now `pl.DataFrame` throughout. There should be no remaining `pd.DataFrame` references in `state.py`, `validation.py`, `map_updates.py`, or `app.py` except where pandas is needed for an explicit file I/O boundary.

## Commit discipline

- Commit after each file (state.py, validation.py, map_updates.py, app.py) with tests green
- Update `documents/plan.md` before the final commit on app.py

## Working style

- Do not add comments or docstrings to unchanged code
- Do not preserve backward compatibility with pandas callers — there are none outside the files in scope
- When in doubt about a pandas operation, read the Polars docs or look at existing Polars usage in the same file for a pattern
- Row access: `df.row(idx, named=True)` → returns a `dict[str, Any]`; `df[idx]` → returns a `pl.DataFrame` of 1 row; `df.to_dicts()[idx]` → returns a dict
- Boolean masks: Polars boolean `pl.Series` can be used directly in `.filter()` and in boolean operations (`&`, `|`, `~`)
- When state.py or validation.py returns `pl.Series` instead of `pd.Series`, app.py callers that used `.sum()`, `.any()`, etc. can call the same methods on Polars Series
- `pl.concat([a, b])` (not `pd.concat`) — works the same as `pd.concat` without `ignore_index`
- Polars DataFrames are immutable: operations return new frames, never mutate in place
