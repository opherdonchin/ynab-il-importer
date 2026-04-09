# Reconciliation Polars Migration Plan

## Scope and Goals

Migrate `review_reconcile.py`, `bank_reconciliation.py`, and (deferred) `card_reconciliation.py`
away from the pandas internal path.  The public interfaces already accept `pl.DataFrame`; the
goal is to remove the `to_pandas()` bridge and all pandas row-iteration inside the active path.

## Slices

### Slice 1 — `review_reconcile.py` (complete)

Status: **implemented** (`review_reconcile.py` now pure-Polars, no `import pandas`).

**Design**: Two-pass join/coalesce instead of row-mutation.

Pass 1 — direct match by occurrence key:
- `_add_occurrence_key`: `transaction_id + "|" + cum_count().over("transaction_id") - 1`
- `_prepare_polars`: normalize string/float/date/bool columns via `with_columns`
- Left-join new onto old on `_occurrence_key` (suffix `_old`), track `_has_old_match`
- `_should_use_old = _has_old_match AND NOT (new.reviewed AND NOT old.reviewed)`
- Apply coalesce for `PRESERVED_REVIEW_COLUMNS`; apply coalesce for `PRESERVED_EDIT_COLUMNS`
  only when `old.changed = True`
- Count = `int(result["_should_use_old"].sum())`

Pass 2 — fallback match by `(date, outflow_ils, inflow_ils, fingerprint)`:
- `remaining_old` = old rows whose `_occurrence_key` is not in any new occurrence key
- Collect unanimous group decisions from `remaining_old` (Python group-by + serialized-payload
  uniqueness check)
- For each remaining_new row (`_should_use_old = False`): if key maps to a unanimous decision,
  `new_group_count == 1`, and NOT (new.reviewed AND NOT old_decision.reviewed), apply decision
- Updates applied column-by-column via `Series(col, updated_list, dtype=original_dtype)` 
  to preserve Object dtype for struct/dict columns

Helper functions kept:
- `_extract_payload_dict`, `_serialize_payload`, `_has_any_review_value` — same semantics as
  old `_preserved_payload`, `_serialized_payload`, `_decision_value_counts`

Functions deleted:
- `_normalize_bool_series`, `_decision_value_counts`, `_preserved_payload`,
  `_serialized_payload`, `_prepare`, `_used_old_mask`, `_occurrence_key_series`,
  `_should_preserve_new_row`, `_reconcile_reviewed_transactions_pandas`

No `import pandas` remains.

---

### Slice 2 — `bank_reconciliation.py` frame prep (complete)

Status: **implemented**.

**Changes:**

`_prepare_bank_dataframe` → `_build_bank_source_frame`:
- Removes the trailing `.to_pandas()` call
- Integrates `_legacy_import_ids` as a Polars expression inline at the end
- Returns `pl.DataFrame` instead of `pd.DataFrame`

`_legacy_import_ids` (pandas helper) → integrated into `_build_bank_source_frame`:
- Polars equivalent of sort + groupby.cumcount():
  ```python
  .sort(["account_key", "date_key", "amount_milliunits", "stable_key", "_row_nr"])
  .with_columns(
      (pl.lit(1).cum_sum().over(["account_key", "date_key", "amount_milliunits"]))
      .alias("import_occurrence")
  )
  .with_columns(
      pl.concat_str([
          pl.lit("YNAB:"),
          pl.col("amount_milliunits").cast(pl.Utf8),
          pl.lit(":"),
          pl.col("date_key"),
          pl.lit(":"),
          pl.col("import_occurrence").cast(pl.Utf8),
      ]).alias("legacy_import_id")
  )
  ```
- Sort is restored after computation to preserve original row order

`_prepare_ynab_transactions` → `_build_bank_ynab_frame`:
- Uses `pl.from_dicts(rows, schema=BANK_YNAB_FRAME_SCHEMA)` instead of `pd.DataFrame(rows)`
- Uses `datetime.strptime` (stdlib) for date parsing instead of `pd.to_datetime`
- Uses `pd_row.get(col)` → direct dict access
- Returns `pl.DataFrame`

`_resolve_account`:
- Updated to accept `pl.DataFrame` instead of `pd.DataFrame`
- Uses `.to_series()`, `.drop_nulls()`, `.unique()` for extracting account_id / account_name values

`_filter_account_transactions`:
- Updated to accept and return `pl.DataFrame`
- Uses `.filter()`, `.with_row_index()` instead of pandas boolean indexing

`_lineage_maps`:
- Updated to accept `pl.DataFrame`
- Uses `enumerate(df.iter_rows(named=True))` instead of `iterrows()`

All existing resolution loops (`plan_bank_match_sync`, `plan_uncleared_ynab_triage`,
`plan_bank_statement_reconciliation`, `_resolve_reconciliation_rows`) + diagnostic helpers:
- Bank rows: `bank_source_frame.iter_rows(named=True)` → row dicts
- YNAB rows: `list(ynab_df.iter_rows(named=True))`; index lookup `ynab_rows[idx]` replaces `ynab_df.loc[idx]`
- Candidate filtering: Polars `.filter()` replaces pandas boolean frame indexing
- `candidates.empty` → `candidates.is_empty()`
- `candidates.iloc[0]` → `candidates.row(0, named=True)` (returns dict)
- `pd.isna(...)` removed; stdlib date parsing used throughout

Report DataFrames (`SYNC_REPORT_COLUMNS`, `UNCLEARED_TRIAGE_COLUMNS`, `RECONCILIATION_REPORT_COLUMNS`)
are still built as `pd.DataFrame(rows, columns=...)` because callers (`sync_bank_matches.py`,
`reconcile_bank_statement.py`) use pandas-style operations on them.  These are diagnostic
outputs, not working data, and callers are not in scope for this slice.

---

### Slice 3 — bank resolution loops (deferred)

Status: **deferred**.

The sequential resolution loops and balance replay are inherently sequential (external
`append_bank_txn_id_marker` raises on conflict; balance replay is a running total fold).
They should stay as Python iteration over `iter_rows(named=True)`.  The main gain from Slice 2
is that the frame construction is now Polars-native; Slice 3 would add filtering helpers in
Polars but provide minimal additional benefit.  Defer until a real pain point emerges.

---

### Slice 4 — `card_reconciliation.py` (deferred)

Status: **deferred, pending audit**.

Card reconciliation shares the same pattern as bank.  Audit before planning.

---

## Open Choices Resolved

| OC | Decision |
|----|----------|
| OC-1 | Exclusive stamp validation stays Python for now (ValueError on memo conflict) |
| OC-2 | `_legacy_import_ids` integrated inline into `_build_bank_source_frame` as Polars expressions |
| OC-3 | Bank row iteration order preserved from original frame (no sort needed in loop) |
| OC-4 | `_resolve_account` and `_filter_account_transactions` updated to Polars in Slice 2 |
| OC-5 | `plan_uncleared_ynab_triage` updated in Slice 2 (uses same frame prep path) |

## Test Coverage

Existing tests:
- `tests/test_review_reconcile.py` — 6 behavioral tests; all pass after Slice 1 rewrite
- `tests/test_bank_reconciliation.py` — parity tests for bank sync; verified after Slice 2

No new fixtures added.  Parity tests against real data remain a future item.
