# Task: Migrate upload_prep.py from pandas to Polars

## Background

Read `documents/plan.md` and `documents/project_context.md` first.

A previous agent migrated the review app session state (`state.py`, `validation.py`, `map_updates.py`, `app.py`) from pandas to Polars. That work is committed and tests pass. You can read the commit `0a8beca` ("Migrate review app session state to Polars") for style precedent.

The upload pipeline has the same structural problem. `upload_prep.py` accepts `pl.DataFrame` at its public boundary, then immediately converts to pandas on the very first line of real logic (`_working_frame_to_pandas`), does everything in pandas, and returns `pd.DataFrame`. All callers in `scripts/prepare_ynab_upload.py` then have to convert back or live with mixed types. This task eliminates that internal pandas conversion.

## Scope

**Primary file:**
- `src/ynab_il_importer/upload_prep.py`

**Test file:**
- `tests/test_upload_prep.py`

**Caller to update (for return type changes):**
- `scripts/prepare_ynab_upload.py`

**Do not touch:**
- `src/ynab_il_importer/ynab_api.py` — `categories_to_dataframe` still returns `pd.DataFrame`; the boundary at `categories_df` stays pandas for now (see below)
- `src/ynab_il_importer/review_app/io.py`, `working_schema.py`, `model.py` — not in scope
- Any other module not listed above

## What the module does

`upload_prep.py` has three layers:

1. **Row selection and normalization** — filters for `create_target + reviewed` rows; normalizes text fields; combines memo; resolves payee/category column name variants.
2. **Split explosion** — `_explode_target_splits_for_upload` iterates over rows with `target_splits` lists and emits one flat row per split line (plus one flat row for non-split rows). After explosion, grouped rows with the same `upload_transaction_id` form one YNAB transaction with subtransactions.
3. **ID resolution and assembly** — account IDs, category IDs, payee IDs, import IDs (occurrence-counted fallback `YNAB:amount:date:N`), and transfer payee IDs are all resolved from lookup dicts and joined onto the prepared frame. Then `assemble_upload_transaction_units` groups by `upload_transaction_id` and builds the final unit records (one dict per YNAB transaction, with a `subtransactions` list for splits).

The public function chain is:
```
load_upload_working_frame(path) -> pl.DataFrame          # already Polars, keep
uploadable_account_mask(working_df, accounts) -> pd.Series   # TARGET: return pl.Series
ready_mask(working_df) -> pd.Series                          # TARGET: return pl.Series
validate_ready_for_upload(working_df) -> None                # stays void
prepare_upload_transactions(working_df, ...) -> pd.DataFrame # TARGET: return pl.DataFrame
assemble_upload_transaction_units(prepared_df) -> pd.DataFrame  # TARGET: accept+return pl.DataFrame
upload_payload_records(prepared_df) -> list[dict]            # TARGET: accept pl.DataFrame
upload_preflight(prepared_df, existing_transactions) -> dict # TARGET: accept pl.DataFrame
verify_upload_response(prepared_df, response) -> dict        # TARGET: accept pl.DataFrame
summarize_upload_response(response) -> dict                  # no dataframe, keep as-is
classify_upload_result(summary, ...) -> dict                 # no dataframe, keep as-is
```

## Migration approach

### Step 1: Read everything before making any changes

Read these files in full:
- `src/ynab_il_importer/upload_prep.py`
- `tests/test_upload_prep.py`
- `scripts/prepare_ynab_upload.py`

### Step 2: Understand the boundary decisions

**The `categories_df` parameter stays `pd.DataFrame` for now.**

`categories_df` comes from `ynab_api.categories_to_dataframe()` which returns `pd.DataFrame`. That module is not in scope. Keep `_category_lookup`, `_category_alias_lookup`, and `_uncategorized_category_id` operating on `pd.DataFrame`. They iterate small category lists, so there is no performance reason to change them. They currently use `iterrows()`; converting them to Polars would require also changing `ynab_api.py`, which is out of scope.

**The `_transactions_frame` internal function stays as a dict-row-list -> DataFrame builder, but use Polars.**

`_transactions_frame` takes `list[dict]` (raw YNAB API response), constructs a flat frame, parses a date column. Replace the `pd.DataFrame` output with `pl.DataFrame`. Adjust all callers (`upload_preflight`, `verify_upload_response`, `summarize_upload_response`) accordingly.

**`safe_types.normalize_flag_series` currently takes `pd.Series`.** 

Check `safe_types.py`. If it's a simple string-to-bool normalization, inline the equivalent Polars expression rather than calling it. Do not modify `safe_types.py` itself (it's out of scope). Look at how `state.py` and `validation.py` handled this in the previous migration commit (`0a8beca`).

### Step 3: Convert the internal working frame

Remove `_working_frame_to_pandas`. Everywhere it was called, work directly on `pl.DataFrame`.

The pandas patterns and their Polars equivalents in this file:

| pandas | polars |
|--------|--------|
| `df.empty` | `df.is_empty()` |
| `df.loc[mask]` / `df[mask]` | `df.filter(mask)` |
| `df.loc[mask, col] = val` | `df.with_columns(pl.when(mask).then(val).otherwise(pl.col(col)).alias(col))` |
| `series.astype("string").fillna("").str.strip()` | `pl.col(name).cast(pl.Utf8).fill_null("").str.strip_chars()` |
| `pd.to_numeric(series, errors="coerce").fillna(0.0)` | `pl.col(name).cast(pl.Float64, strict=False).fill_null(0.0)` |
| `series.map(func)` (element-wise) | `pl.col(name).map_elements(func, return_dtype=pl.Utf8)` |
| `series.isin(set_)` | `pl.col(name).is_in(list(set_))` |
| `df.groupby(col).cumcount()` | `pl.int_range(0, pl.len()).over(col)` |
| `series.eq("")` | `pl.col(name) == ""` |
| `series.ne("")` | `pl.col(name) != ""` |
| `df.apply(func, axis=1)` | `pl.struct([cols]).map_elements(lambda row: func(row), return_dtype=...)` |
| `df.copy()` | `df.clone()` (or nothing if not needed) |
| `df.reset_index(drop=True)` | not needed — Polars has no index |
| `df.set_index(col)["other_col"]` | `dict(zip(df[col].to_list(), df["other_col"].to_list()))` |
| `df.sort_values([cols])` | `df.sort([cols])` |
| `df.drop_duplicates(subset=[cols])` | `df.unique(subset=[cols], keep="first")` — check ordering |
| `df.merge(other, on=cols, how="left")` | `df.join(other, on=cols, how="left")` |

**Row access in `assemble_upload_transaction_units` and `upload_preflight`:**

These currently use `df.groupby(...).iterrows()` patterns for building list-of-dicts results. Keep using Python iteration over Polars row dicts — this is fine for the small assembled result sets. Use `df.group_by(col, maintain_order=True)` to get groups, then iterate with `.rows(named=True)` or `.to_dicts()`. Do not try to express the subtransaction assembly as a pure Polars expression — it produces nested Python structures and iteration is clearer.

### Step 4: Migrate _nonzero_amount_mask, _decision_action_mask, and series helpers

These currently return `pd.Series`. Make them return `pl.Series` (or inline them as `pl.Expr` where they're only used in `filter()`). Decide case by case — if a helper is called in multiple places and returning a `pl.Series` is clean, return `pl.Series`. If it's only called once inside a `filter()`, express it as a `pl.Expr` directly.

### Step 5: Migrate _explode_target_splits_for_upload

This is the most complex piece. It currently iterates `df.iterrows()` and builds a list of flat dicts, then wraps them in `pd.DataFrame`.

Keep the logic as Python iteration — it handles nested `target_splits` lists and sets up `parent_*` columns. Convert the output to `pl.DataFrame` using `pl.from_dicts(rows, infer_schema_length=None)`. At the end, ensure column order matches what the rest of the pipeline expects (use `.select([ordered_cols])` to project).

The column set after explosion should include all original columns plus: `parent_target_payee_selected`, `parent_memo`, `subtransaction_memo`, `upload_is_split`.

### Step 6: Migrate _source_import_id

This currently takes `pd.Series` as `row`. Change its signature to accept `dict[str, Any]` — it only uses `.get()` calls so no other changes are needed. It is already called via `df.apply(lambda row: _source_import_id(row), axis=1)`; replace with `pl.struct([relevant_cols]).map_elements(lambda row: _source_import_id(row), return_dtype=pl.Utf8)`.

### Step 7: Migrate occurrence_order block in prepare_upload_transactions

This block computes `import_occurrence` (per account/date/amount group occurrence count) and assigns final `import_id`. In Polars:

```python
occurrence_order = (
    df.unique(subset=["upload_row_position"], keep="first")
    .sort(["account_id", "date", "import_amount_milliunits", "transaction_id", "upload_transaction_id", "upload_row_position"])
    .with_columns(
        (pl.int_range(0, pl.len()).over(["account_id", "date", "import_amount_milliunits"]) + 1)
        .alias("import_occurrence")
    )
    .with_columns(
        pl.struct(["source_import_id", "bank_txn_id", "card_txn_id", "source", "source_source_system", "source_transaction_id", "transaction_id", "import_amount_milliunits", "date", "import_occurrence"])
        .map_elements(
            lambda row: (
                _source_import_id(row)
                or f"YNAB:{int(row['import_amount_milliunits'])}:{row['date']}:{int(row['import_occurrence'])}"
            ),
            return_dtype=pl.Utf8,
        )
        .alias("import_id")
    )
)
occurrence_map = dict(zip(occurrence_order["upload_row_position"].to_list(), occurrence_order["import_id"].to_list()))
df = df.with_columns(
    pl.col("upload_row_position").map_elements(lambda pos: occurrence_map.get(pos, ""), return_dtype=pl.Utf8).alias("import_id")
)
```

Adjust for whichever columns are actually available in scope.

### Step 8: Migrate assemble_upload_transaction_units

This takes `prepared_df: pd.DataFrame` and returns `pd.DataFrame`. Change both to `pl.DataFrame`.

Internally, it:
1. Fills missing columns with defaults — do this with `.with_columns(pl.lit(val).alias(col))` guarded by `if col not in df.columns`.
2. Normalizes string columns — use `.with_columns([pl.col(c).cast(pl.Utf8).fill_null("").str.strip_chars() for c in text_cols])`.
3. Groups by `upload_transaction_id` and assembles unit dicts — keep this as Python iteration using `df.group_by("upload_transaction_id", maintain_order=True)` with `.iter_groups()`.
4. Checks for duplicate `upload_transaction_id` — use `.filter(pl.col("upload_transaction_id").is_duplicated())`.

Return `pl.from_dicts(unit_rows)` with `schema` if needed to ensure column types.

### Step 9: Migrate upload_preflight and verify_upload_response

Both currently take `pd.DataFrame` for `prepared_df`. Both call `assemble_upload_transaction_units(prepared_df)` which after Step 8 returns `pl.DataFrame`. Update all downstream operations to Polars.

`upload_preflight` uses:
- `.groupby(["account_id", "import_id"]).size()` — use `.group_by(["account_id", "import_id"]).agg(pl.len())` then filter where `pl.col("len") > 1`
- `.merge(candidates, on=..., suffixes=...)` — use `.join(candidates, on=...)` with explicit alias renames
- `(merged["date_key_prepared"] - merged["date_key_existing"]).abs().dt.days` — parse dates to `pl.Date` first with `.str.to_date(strict=False)`, then compute `(d1 - d2).dt.total_days().abs()`
- iterrows loops over small sets — keep as Python iteration via `.to_dicts()`

`_transactions_frame` (used by preflight and verify) — convert the `pd.DataFrame` return to `pl.DataFrame`. Parse the date column with `.with_columns(pl.col("date").str.to_date(strict=False).alias("date_key"))`.

### Step 10: Update callers in prepare_ynab_upload.py

The goal is to push pandas isolation downward: callers should not need to be aware of pandas at all. Apply this rule consistently — if a function now returns `pl.Series` or `pl.DataFrame`, update its caller to use it natively; never convert at the call site in the script. If the returned value is then passed into a function that internally still needs pandas (e.g., some future helper), that receiving function is responsible for converting at the top of its own body.

After changes, the following return types change:

- `upload_prep.ready_mask(reviewed)` — returns `pl.Series` (Boolean). The caller currently does `.astype(bool).tolist()`; use `.to_list()` directly on `pl.Series`. Filter the working frame with `reviewed.filter(pl.Series("ready_mask", mask.to_list()))` or `reviewed.filter(mask)` (Polars accepts a boolean `pl.Series` in `filter()`).

- `upload_prep.uploadable_account_mask(reviewed, accounts)` — returns `pl.Series` (Boolean). `~account_mask` works natively on `pl.Series` (use `account_mask.not_()`). `.sum()` works natively. `reviewed.filter(account_mask)` works directly.

- `upload_prep.prepare_upload_transactions(...)` — returns `pl.DataFrame`. The caller passes it to `upload_prep.upload_preflight(prepared, ...)` and `upload_prep.upload_payload_records(prepared)` — both now accept `pl.DataFrame`. Also `prepared.empty` → `prepared.is_empty()`.

### Step 11: Update tests

`tests/test_upload_prep.py` heavily uses `pd.DataFrame` for constructing `prepared_df` inputs directly (in `test_upload_payload_records_uses_transaction_units`, `test_assemble_upload_transaction_units_builds_split_units_from_grouped_rows`, `test_upload_payload_records_rejects_unsupported_split_transfer_units`). After migration these now need to be `pl.DataFrame`.

The fixture `_reviewed_df` currently returns `pl.from_pandas(pd.DataFrame(...))` — you can simplify to `pl.DataFrame(...)` directly if the types cooperate.

The `_categories()` and `_api_like_categories()` fixtures return `pd.DataFrame` — these stay as-is since `categories_df` stays pandas.

Anywhere a test asserted `isinstance(result, pd.DataFrame)` or `isinstance(result, pd.Series)` or used `.tolist()` on a pandas result, update accordingly.

## Commit

After the full test suite passes (`pixi run pytest tests/ -q`), commit with message:
`Migrate upload_prep.py internals to Polars`

## What not to do

- Do not migrate `ynab_api.py` or `safe_types.py`.
- Do not migrate `categories_df` to Polars — the parameter type stays `pd.DataFrame`.
- Do not try to remove `import pandas as pd` entirely — it is still needed for `categories_df` and the `_category_*` helpers.
- Do not use `.iterrows()` on large frames (the working frame, the prepared frame). Reserve Python iteration for small, structurally complex assembly steps (subtransaction building, unit assembly) where expressing it as a Polars expression would be harder to read.
- Do not add compatibility layers. If a function now takes `pl.DataFrame`, update the callers, not the type signature.
- Do not refactor logic beyond what is needed to remove pandas. The business logic in `_source_import_id`, `_transfer_target`, `_category_alias`, and the category lookup helpers should remain semantically identical.
