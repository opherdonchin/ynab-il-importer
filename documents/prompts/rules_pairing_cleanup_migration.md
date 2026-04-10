# Polars Migration — rules, pairing, safe_types, transaction_io, ynab_api

## Background

`state.py`, `validation.py`, `map_updates.py`, `app.py`, and `upload_prep.py` are already fully
Polars. This task eliminates the remaining pandas islands in core library modules and their
immediate callers.

## Files to Change (work in dependency order)

Work top-down so each file you edit depends only on already-migrated upstream modules.

1. `src/ynab_il_importer/ynab_api.py`
2. `src/ynab_il_importer/safe_types.py` + `tests/test_safe_types.py`
3. `src/ynab_il_importer/export.py`
4. `src/ynab_il_importer/upload_prep.py` (categories section only)
5. `src/ynab_il_importer/artifacts/transaction_io.py`
6. `src/ynab_il_importer/pairing.py`
7. `src/ynab_il_importer/rules.py`
8. `src/ynab_il_importer/review_app/map_updates.py` (one line)
9. `scripts/bootstrap_pairs.py`
10. `scripts/prepare_ynab_upload.py` (two lines)
11. `scripts/download_ynab_categories.py` (two lines)
12. `src/ynab_il_importer/review_app/app.py` (one line)
13. `tests/test_rules.py`
14. `tests/test_pairing_loaders.py`

## Do Not Touch

- `src/ynab_il_importer/review_app/io.py` — intentional pandas I/O boundary, left as-is
- `src/ynab_il_importer/fingerprint.py`
- `src/ynab_il_importer/bank_reconciliation.py`
- `src/ynab_il_importer/card_reconciliation.py`
- `scripts/io_leumi*.py`, `scripts/io_max.py`, `scripts/io_ynab.py` — raw file parsers
- Any reconciliation script not explicitly named in scope above
- `src/ynab_il_importer/export.py` column outputs — preserve CSV encoding and path logic exactly

---

## Step 1 — `src/ynab_il_importer/ynab_api.py`

### What to change

`categories_to_dataframe` and `categories_from_transactions_to_dataframe` currently return
`pd.DataFrame`. Change both to return `pl.DataFrame`.

**`categories_to_dataframe`:** The function builds a `rows: list[dict[str, Any]]` and then does
`pd.DataFrame(rows)`. Replace with:

```python
if not rows:
    return pl.DataFrame(
        {
            "category_group": pl.Series([], dtype=pl.Utf8),
            "category_group_id": pl.Series([], dtype=pl.Utf8),
            "category_name": pl.Series([], dtype=pl.Utf8),
            "category_id": pl.Series([], dtype=pl.Utf8),
            "hidden": pl.Series([], dtype=pl.Boolean),
        }
    )
return pl.from_dicts(rows)
```

The empty-frame guard is important: callers check `len(df) == 0` or `.is_empty()` and without it
`pl.from_dicts([])` raises.

**`categories_from_transactions_to_dataframe`:** Same pattern — replace `pd.DataFrame(rows)` at
the end with the same empty-guard + `pl.from_dicts(rows)`.

Add `import polars as pl` at the top of `ynab_api.py` (after existing imports). Remove or keep
`import pandas as pd` only if it is still used elsewhere in the file — check before removing.

Do not change any other function in `ynab_api.py`.

---

## Step 2 — `src/ynab_il_importer/safe_types.py` and `tests/test_safe_types.py`

### What to change

After this migration `normalize_flag_series(series: pd.Series)` will have no remaining callers
(the only caller was `upload_prep.py`'s `_category_lookup`/`_category_alias_lookup`, which are
being migrated to Polars in Step 4). Remove the function and the `import pandas as pd` line from
`safe_types.py`. Keep `TRUE_VALUES` — it is still imported by `review_app/io.py`.

In `tests/test_safe_types.py`, delete all test functions that test `normalize_flag_series`.
Delete the `import pandas as pd`, `import math`, and `from ynab_il_importer.safe_types import
normalize_flag_series` lines. If there are no remaining tests in the file after removal, delete
the file entirely.

---

## Step 3 — `src/ynab_il_importer/export.py`

### What to change

`write_dataframe` currently takes `pd.DataFrame`. Many scripts that are not being migrated
(raw parsers, reconciliation scripts) still pass `pd.DataFrame`. `map_updates.py` currently
calls `export.write_dataframe(out.to_pandas(), path)`. After this migration we want that
`.to_pandas()` call removed.

Make `write_dataframe` accept either type:

```python
import polars as pl

def write_dataframe(df: pd.DataFrame | pl.DataFrame, path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(df, pl.DataFrame):
        df.write_csv(output_path)
    else:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
```

Note: `pl.DataFrame.write_csv` does not take an `encoding` argument. The CSV text output is
identical. The `utf-8-sig` BOM was for Excel on Windows; omit it for the Polars path (categories
files are loaded programmatically, not opened in Excel).

Do not change the `wrote_message`, `display_path`, or `report_message` functions.

---

## Step 4 — `src/ynab_il_importer/upload_prep.py` (categories section only)

### What to change

Only the three private functions `_category_lookup`, `_category_alias_lookup`, and
`_uncategorized_category_id` need changing. All other functions in `upload_prep.py` are already
Polars and must not be touched.

Remove the import `from ynab_il_importer.safe_types import normalize_flag_series` (it was only
used in these three helpers and will no longer be needed).

**`_category_lookup(categories_df: pl.DataFrame) -> dict[str, str]`:**

```python
def _category_lookup(categories_df: pl.DataFrame) -> dict[str, str]:
    active = categories_df
    if "hidden" in active.columns:
        active = active.filter(~pl.col("hidden").cast(pl.Boolean).fill_null(False))

    names = active["category_name"].cast(pl.Utf8).fill_null("").map_elements(
        _normalize_text, return_dtype=pl.Utf8
    )
    name_list = names.to_list()
    name_counts = Counter(name_list)
    duplicates = sorted(name for name, count in name_counts.items() if name and count > 1)
    if duplicates:
        raise ValueError(f"Duplicate YNAB category names: {duplicates}")

    return {
        _normalize_text(row["category_name"]): _normalize_text(row["category_id"])
        for row in active.iter_rows(named=True)
        if _normalize_text(row["category_name"])
    }
```

**`_category_alias_lookup(categories_df: pl.DataFrame) -> dict[str, str]`:**

```python
def _category_alias_lookup(categories_df: pl.DataFrame) -> dict[str, str]:
    active = categories_df
    if "hidden" in active.columns:
        active = active.filter(~pl.col("hidden").cast(pl.Boolean).fill_null(False))

    alias_to_id: dict[str, str] = {}
    duplicate_aliases: list[str] = []
    for row in active.iter_rows(named=True):
        name = _normalize_text(row.get("category_name", ""))
        category_id = _normalize_text(row.get("category_id", ""))
        alias = _category_alias(name)
        if not alias or not category_id:
            continue
        if alias in alias_to_id and alias_to_id[alias] != category_id:
            duplicate_aliases.append(alias)
            continue
        alias_to_id[alias] = category_id

    if duplicate_aliases:
        raise ValueError(
            f"Ambiguous simplified YNAB category aliases: {sorted(set(duplicate_aliases))}"
        )
    return alias_to_id
```

**`_uncategorized_category_id(categories_df: pl.DataFrame) -> str`:** Only the type annotation
changes; the body stays the same since it delegates to the above two functions.

Update the public function `prepare_upload_transactions(reviewed, accounts, categories_df, ...)` —
change the `categories_df: pd.DataFrame` type annotation to `categories_df: pl.DataFrame`. The
body already delegates to the private helpers; no other changes needed there.

---

## Step 5 — `src/ynab_il_importer/artifacts/transaction_io.py`

### What to change

Remove all `isinstance(data, pd.DataFrame)` and `isinstance(csv_projection, pd.DataFrame)`
branches. All callers already pass `pl.DataFrame`. After removal, the functions accept only
`pl.DataFrame`.

**`_to_arrow_table`:** Remove the `isinstance(data, pd.DataFrame)` branch entirely. The function
only needs the Polars path (`data.to_arrow()`).

**`write_flat_transaction_artifacts`:** Remove the `isinstance(data, pd.DataFrame)` branch
(`pl.from_pandas(data)` adapter). The function now unconditionally works on a `pl.DataFrame`.

**`write_canonical_transaction_artifacts`:** Remove the `isinstance(csv_projection, pd.DataFrame)`
branch. Same — unconditional Polars path.

**`load_flat_transaction_projection`:** Change the return type from `pd.DataFrame` to
`pl.DataFrame`. In the parquet branch, remove the `.to_pandas()` call — `pl.read_parquet` already
returns `pl.DataFrame`. In the CSV branch, change `pd.read_csv(path)` to `pl.read_csv(path)`.
Update the return type annotation.

After all removals: if `pandas` is no longer imported anywhere in `transaction_io.py`, remove
the `import pandas as pd` line.

---

## Step 6 — `src/ynab_il_importer/pairing.py`

### Overview

`pairing.py` is pure pandas. All three entry-point functions (`_prepare_source`, `_prepare_ynab`,
`match_pairs`) take `pd.DataFrame` inputs and return `pd.DataFrame`. Migrate all to `pl.DataFrame`.

### Internal helpers

**`_series_or_default(df, col, dtype, default)`:** This returns a `pd.Series` with a fallback
default. Migrated signature:

```python
def _series_or_default(
    df: pl.DataFrame, col: str, dtype: pl.PolarsDataType, default: Any = None
) -> pl.Series:
    if col in df.columns:
        return df[col].cast(dtype, strict=False)
    return pl.Series([default] * len(df), dtype=dtype)
```

**`_pick_raw_text(df, columns)`:** Picks the first non-blank column. Migrate to:

```python
def _pick_raw_text(df: pl.DataFrame, columns: list[str]) -> pl.Series:
    for col in columns:
        if col in df.columns:
            s = df[col].cast(pl.Utf8).fill_null("").str.strip_chars()
            if s.ne("").any():
                return s
    return pl.Series([""] * len(df), dtype=pl.Utf8)
```

**`_pick_raw_text_by_source(df, source_name, columns)`:** The pandas version uses
`df.loc[mask, col]` to conditionally pick source-specific text. Migrated:

```python
def _pick_raw_text_by_source(
    df: pl.DataFrame, source_name: str, columns: list[str]
) -> pl.Series:
    mask = df["source"].cast(pl.Utf8).fill_null("").eq(source_name)
    subset = df.filter(mask)
    picked = _pick_raw_text(subset, columns)
    # Build a full-length series: fill positions where mask is False with ""
    result = pl.Series([""] * len(df), dtype=pl.Utf8)
    indices = mask.arg_true()
    return result.scatter(indices, picked)
```

### `_prepare_source` and `_prepare_ynab`

These construct a working frame with standardised key columns. Migrate the pandas idioms:

- `pd.to_datetime(df[col], errors="coerce")` → `df[col].cast(pl.Date, strict=False)` (or
  `pl.col(col).str.to_date(format=None, strict=False)` if the column is string)
- `pd.to_numeric(df[col], errors="coerce")` → `df[col].cast(pl.Float64, strict=False)`
- `.fillna(...)` → `.fill_null(...)`
- `.dropna(subset=[...])` → `.drop_nulls(subset=[...])`
- Column assignment uses `.with_columns([...])` chains

Return type: `pl.DataFrame`.

### `_join_pairs`

Replace `source_df.merge(ynab_df, on=[...], how="inner")` with:

```python
return source_df.join(ynab_df, on=[JOIN_KEY_COLUMNS], how="inner")
```

where `JOIN_KEY_COLUMNS` is whatever key list the original merge used.

### `match_pairs`

- The `groupby().size()` that computes `ambiguous_key` mask → `.group_by(key_cols).agg(pl.len().alias("_key_count"))` followed by a join back and filter
- Return type: `pl.DataFrame`
- `normalize.normalize_text` is a scalar function — call it via `pl.col(...).map_elements(normalize.normalize_text, return_dtype=pl.Utf8)`

Update the function signature: `match_pairs(source_df: pl.DataFrame, ynab_df: pl.DataFrame) -> pl.DataFrame`.

Remove `import pandas as pd` from `pairing.py` after all references are gone.

---

## Step 7 — `src/ynab_il_importer/rules.py`

### Overview

Migrate `normalize_payee_map_rules`, `load_payee_map`, and `prepare_transactions_for_rules` to
use `pl.DataFrame`. Keep the procedural matching loop (`apply_payee_map_rules`, `_rule_matches`,
`_compile_active_rules`, `_candidate_rules_for_txn`) as Python iteration, but replace `iterrows()`
with `iter_rows(named=True)` so rows are plain Python `dict`s. The internal matching logic does
not change — dict `.get()` and `[]` access remain.

### `_blank_to_none`

Replace `pd.isna(value)` with a pure-Python check. Add `import math` at the top of the file:

```python
def _blank_to_none(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text if text else None
```

### `_match_amount_bucket`

Change `txn: pd.Series` to `txn: dict[str, Any]`. Replace:

```python
amount = pd.to_numeric(pd.Series([txn.get("amount_value")]), errors="coerce").fillna(0.0).iloc[0]
```

with:

```python
try:
    amount = float(txn.get("amount_value") or 0.0)
except (TypeError, ValueError):
    amount = 0.0
```

### `_rule_matches` and `_candidate_rules_for_txn`

Change type annotations from `pd.Series` to `dict[str, Any]`. The bodies use `.get(col)` which
works on both — no logic changes needed.

### `_compile_active_rules`

Replace `active_rules.iterrows()` with `active_rules.iter_rows(named=True)`. Each row is now a
plain `dict`. The function still returns
`tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]`.

```python
def _compile_active_rules(
    rules: pl.DataFrame,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    active_rules = rules.filter(pl.col("is_active"))
    by_fingerprint: dict[str, list[dict[str, Any]]] = {}
    wildcard_rules: list[dict[str, Any]] = []
    for rule in active_rules.iter_rows(named=True):
        fingerprint = _blank_to_none(rule.get("fingerprint"))
        if fingerprint is None:
            wildcard_rules.append(rule)
            continue
        by_fingerprint.setdefault(fingerprint, []).append(rule)
    return by_fingerprint, wildcard_rules
```

### `_compute_specificity`

Change `rule: pd.Series` to `rule: dict[str, Any]`. No other changes — dict access is the same.

### `normalize_payee_map_rules`

Change input and output from `pd.DataFrame` to `pl.DataFrame`. Translation guide:

| Pandas | Polars |
|---|---|
| `df.copy()` | not needed (Polars is immutable) |
| `out[col] = ""` for missing columns | `df = df.with_columns(pl.lit("").alias(col))` for each missing col |
| `out = out[PAYEE_MAP_COLUMNS].copy()` | `df = df.select(PAYEE_MAP_COLUMNS)` |
| `out["rule_id"].astype("string").fillna("").str.strip()` | `pl.col("rule_id").cast(pl.Utf8).fill_null("").str.strip_chars()` |
| `(out["rule_id"] == "").any()` | `(df["rule_id"] == "").any()` |
| `out["rule_id"][out["rule_id"].duplicated()].unique().tolist()` | `df.filter(pl.col("rule_id").is_duplicated())["rule_id"].unique().to_list()` |
| `out["is_active"] = out["is_active"].map(_normalize_is_active)` | `pl.col("is_active").map_elements(_normalize_is_active, return_dtype=pl.Boolean)` |
| `out["priority"] = out["priority"].map(_normalize_priority)` | `pl.col("priority").map_elements(_normalize_priority, return_dtype=pl.Int64)` |
| `out[col] = out[col].map(lambda v, c=col: _normalize_key_value(c, v))` | `pl.col(col).map_elements(lambda v: _normalize_key_value(col, v), return_dtype=pl.Utf8)` |
| `out.apply(_compute_specificity, axis=1)` | `pl.struct(RULE_KEY_COLUMNS).map_elements(lambda row: _compute_specificity(row), return_dtype=pl.Int64)` — note: update `_compute_specificity` to accept `dict` |
| `out["payee_canonical"].map(_blank_to_none)` | `pl.col("payee_canonical").map_elements(_blank_to_none, return_dtype=pl.Utf8)` |

Use a single `.with_columns([...])` block where possible.

### `load_payee_map`

Change return type to `pl.DataFrame`. Change `pd.read_csv(map_path, dtype="string").fillna("")`
to `pl.read_csv(map_path, infer_schema_length=0).fill_null("")`.

### `_pick_col` helper (replaces `_pick_series`)

Add this helper at module level:

```python
def _pick_col(df: pl.DataFrame, columns: list[str], default: str = "") -> pl.Series:
    for col in columns:
        if col in df.columns:
            s = df[col].cast(pl.Utf8).fill_null("").str.strip_chars()
            if s.ne("").any():
                return s
    return pl.Series([default] * len(df), dtype=pl.Utf8)
```

Remove the old `_pick_series` function.

### `prepare_transactions_for_rules`

Change input and output from `pd.DataFrame` to `pl.DataFrame`. Key translations:

Replace all `_pick_series(out, [...])` calls with `_pick_col(df, [...])`.

`out["txn_kind"] = _pick_series(out, ["txn_kind"]).str.lower()` →
```python
df = df.with_columns(_pick_col(df, ["txn_kind"]).str.to_lowercase().alias("txn_kind"))
```

For `currency` with `replace("", "ILS")`:
```python
currency = _pick_col(df, ["currency"], default="ILS")
currency = pl.when(currency == "").then(pl.lit("ILS")).otherwise(currency).str.to_uppercase()
df = df.with_columns(currency.alias("currency"))
```

For **direction** computation, use `pl.when/then/otherwise` chains:

```python
# Start with existing direction column or blank
if "direction" in df.columns:
    direction = df["direction"].cast(pl.Utf8).fill_null("").str.strip_chars().str.to_lowercase()
else:
    direction = pl.Series([""] * len(df), dtype=pl.Utf8)

if "inflow_ils" in df.columns or "outflow_ils" in df.columns:
    inflow = (
        df["inflow_ils"].cast(pl.Float64, strict=False).fill_null(0.0)
        if "inflow_ils" in df.columns
        else pl.Series([0.0] * len(df), dtype=pl.Float64)
    )
    outflow = (
        df["outflow_ils"].cast(pl.Float64, strict=False).fill_null(0.0)
        if "outflow_ils" in df.columns
        else pl.Series([0.0] * len(df), dtype=pl.Float64)
    )
    flow_direction = pl.Series(
        ["inflow" if i > 0 else "outflow" if o > 0 else "zero"
         for i, o in zip(inflow.to_list(), outflow.to_list())],
        dtype=pl.Utf8,
    )
    direction = pl.when(direction != "").then(direction).otherwise(flow_direction)
elif "amount_ils" in df.columns:
    amount_vals = df["amount_ils"].cast(pl.Float64, strict=False).fill_null(0.0)
    amount_direction = pl.Series(
        ["inflow" if v > 0 else "outflow" if v < 0 else "zero"
         for v in amount_vals.to_list()],
        dtype=pl.Utf8,
    )
    direction = pl.when(direction != "").then(direction).otherwise(amount_direction)
    inflow = pl.when(amount_vals > 0).then(amount_vals).otherwise(pl.lit(0.0))
    outflow = pl.when(amount_vals < 0).then(amount_vals.abs()).otherwise(pl.lit(0.0))

direction = pl.when(direction == "").then(pl.lit("zero")).otherwise(direction)
df = df.with_columns(direction.alias("direction"))
```

For **amount_value**:
```python
if "inflow_ils" in df.columns or "outflow_ils" in df.columns:
    amount_value = pl.when(df["direction"] == "inflow").then(inflow).otherwise(outflow)
elif "amount_ils" in df.columns:
    amount_value = pl.when(df["direction"] == "inflow").then(inflow).otherwise(outflow)
else:
    amount_value = pl.Series([0.0] * len(df), dtype=pl.Float64)
df = df.with_columns(amount_value.alias("amount_value"))
```

For `description_clean_norm` using `normalize.normalize_text`:
```python
raw_for_norm = _pick_col(df, [
    "description_clean_norm", "description_clean", "merchant_raw",
    "description_raw", "raw_norm", "raw_text",
])
df = df.with_columns(
    raw_for_norm.map_elements(normalize.normalize_text, return_dtype=pl.Utf8)
    .alias("description_clean_norm")
)
```

For the `fingerprint` validation:
```python
if "fingerprint" not in df.columns:
    raise ValueError("Transactions are missing required fingerprint column")
fingerprint = df["fingerprint"].cast(pl.Utf8).fill_null("").str.strip_chars()
if (fingerprint == "").any():
    raise ValueError("Transactions contain empty fingerprint values")
df = df.with_columns(fingerprint.alias("fingerprint"))
```

Build `example_text` similarly:
```python
df = df.with_columns(
    _pick_col(df, ["description_raw", "raw_text", "description_clean",
                   "merchant_raw", "description_clean_norm"])
    .alias("example_text")
)
```

Return `df` at the end.

### `apply_payee_map_rules`

Change inputs to `pl.DataFrame`. The matching loop now uses `tx.iter_rows(named=True)`:

```python
def apply_payee_map_rules(transactions: pl.DataFrame, rules: pl.DataFrame) -> pl.DataFrame:
    tx = prepare_transactions_for_rules(transactions)
    compiled_rules = _compile_active_rules(rules)

    results: list[dict[str, Any]] = []
    for txn in tx.iter_rows(named=True):
        ...  # same logic as before — txn is a dict
    
    return pl.from_dicts(results, schema={
        "payee_canonical_suggested": pl.Utf8,
        "category_target_suggested": pl.Utf8,
        "match_rule_id": pl.Utf8,
        "match_specificity_score": pl.Int64,
        "match_status": pl.Utf8,
        "match_candidate_rule_ids": pl.Utf8,
        "match_rule_count": pl.Int64,
    })
```

The `results` list build loop body is **unchanged** — it still appends dicts. Only
`pd.DataFrame(results, index=tx.index)` at the end becomes `pl.from_dicts(results, schema=...)`.

Also update the `ranked` sort key — `pd.Series`-style attribute access (`rule["priority"]`) on
dicts works without changes. `int(r["priority"])` and `int(r["_specificity"])` still work since
dict values from `iter_rows(named=True)` are Python scalars.

Remove `import pandas as pd` from `rules.py` after all references are gone.

---

## Step 8 — `src/ynab_il_importer/review_app/map_updates.py`

### What to change

One line (line ~157):

```python
# Before
prepared = pl.from_pandas(rules.prepare_transactions_for_rules(candidates.to_pandas()))

# After
prepared = rules.prepare_transactions_for_rules(candidates)
```

`candidates` is already a `pl.DataFrame` (`current.filter(candidate_mask)`). `prepare_transactions_for_rules` now accepts `pl.DataFrame` directly. No other changes in this file.

Also remove `export.write_dataframe(out.to_pandas(), path)` → `export.write_dataframe(out, path)`
(Step 3 made `export.write_dataframe` Polars-aware; drop the `.to_pandas()` call).

---

## Step 9 — `scripts/bootstrap_pairs.py`

### What to change

Currently:
```python
source_df = transaction_io.load_flat_transaction_projection(args.source)
ynab_df = transaction_io.load_flat_transaction_projection(args.ynab)
if extra_source_paths:
    ...
    source_df = pd.concat([source_df] + extra_frames)
pairs_df = pairing.match_pairs(source_df, ynab_df)
export.write_dataframe(pairs_df, args.out)
```

After `load_flat_transaction_projection` returns `pl.DataFrame` and `match_pairs` accepts
`pl.DataFrame`:

- Replace `pd.concat([source_df] + extra_frames)` with `pl.concat([source_df] + extra_frames)`
  (same API, different namespace)
- Remove `import pandas as pd` if no other pandas usage remains
- `export.write_dataframe(pairs_df, args.out)` stays as-is (Step 3 handles both types)

---

## Step 10 — `scripts/prepare_ynab_upload.py`

### What to change

Two usages of the old pandas-style empty check:

```python
# Before
if categories.empty:
    ...
    if not categories.empty:

# After
if categories.is_empty():
    ...
    if not categories.is_empty():
```

No other changes needed. The `categories` variable is now `pl.DataFrame` from
`ynab_api.categories_to_dataframe`. The `prepare_upload_transactions(categories_df=categories)`
call is unchanged — it now receives `pl.DataFrame` which Step 4 handles.

---

## Step 11 — `scripts/download_ynab_categories.py`

### What to change

Two lines:

```python
# Before
if df.empty:
    ...
export.write_dataframe(df, out_path)

# After
if df.is_empty():
    ...
export.write_dataframe(df, out_path)  # unchanged — Step 3 handles pl.DataFrame
```

Remove `import pandas as pd` if it has no other use in the file.

---

## Step 12 — `src/ynab_il_importer/review_app/app.py`

### What to change

One block (around line 497):

```python
# Before
df.to_csv(categories_path, index=False, encoding="utf-8-sig")

# After
df.write_csv(categories_path)
```

`df` is now a `pl.DataFrame` from `ynab_api.categories_to_dataframe`. The `len(df) == 0` check
on the line before already works on `pl.DataFrame`.

---

## Step 13 — `tests/test_rules.py`

### What to change

The test helpers and fixtures pass `pd.DataFrame` to `normalize_payee_map_rules` and
`apply_payee_map_rules`. After migration these functions take `pl.DataFrame`.

**Helper `_rules`:** Change from:
```python
def _rules(rows: list[dict[str, object]]) -> pd.DataFrame:
    return rules_mod.normalize_payee_map_rules(pd.DataFrame(rows))
```
to:
```python
def _rules(rows: list[dict[str, object]]) -> pl.DataFrame:
    return rules_mod.normalize_payee_map_rules(pl.DataFrame(rows))
```

**Transaction fixtures:** All `pd.DataFrame([...])` constructions in test bodies → `pl.DataFrame([...])`.

**Assertions:** Change `out["match_status"].tolist()` → `out["match_status"].to_list()`.
Change `out.loc[0, "match_status"]` etc. → `out[0, "match_status"]` (Polars integer row index).

Add `import polars as pl` and remove `import pandas as pd` from the test file.

---

## Step 14 — `tests/test_pairing_loaders.py`

### What to change

The monkeypatched `match_pairs` lambda uses `source_df.loc[0, "fingerprint"]` (pandas syntax)
and returns `pd.DataFrame()`. After migration:

```python
# Before
lambda source_df, ynab_df: (
    captured.update(
        {
            "source_fingerprint": source_df.loc[0, "fingerprint"],
            "ynab_fingerprint": ynab_df.loc[0, "fingerprint"],
        }
    )
    or pd.DataFrame()
),

# After
lambda source_df, ynab_df: (
    captured.update(
        {
            "source_fingerprint": source_df[0, "fingerprint"],
            "ynab_fingerprint": ynab_df[0, "fingerprint"],
        }
    )
    or pl.DataFrame()
),
```

Remove `import pandas as pd` if no other pandas usage remains in the test file.

---

## Verification

Run after all edits:

```
pixi run pytest tests/ -q
```

All 306+ tests must pass. Fix any failures before committing.

Also confirm no remaining pandas imports in the migrated files by running:

```
grep -rn "import pandas" src/ynab_il_importer/rules.py src/ynab_il_importer/pairing.py \
  src/ynab_il_importer/safe_types.py src/ynab_il_importer/artifacts/transaction_io.py \
  src/ynab_il_importer/upload_prep.py src/ynab_il_importer/ynab_api.py
```

(Some may still be present in `ynab_api.py` for other functions — check before removing.)

---

## Commit

```
git add -A
git commit -m "Migrate rules, pairing, safe_types, transaction_io, ynab_api to Polars"
```
