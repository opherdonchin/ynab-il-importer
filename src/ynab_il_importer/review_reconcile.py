from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any

import polars as pl


PRESERVED_REVIEW_COLUMNS = [
    "source_payee_selected",
    "source_category_selected",
    "target_payee_selected",
    "target_category_selected",
    "decision_action",
    "update_maps",
    "reviewed",
    "changed",
    "memo_append",
]
PRESERVED_EDIT_COLUMNS = [
    "source_splits",
    "target_splits",
    "source_current_transaction",
    "target_current_transaction",
    "source_original_transaction",
    "target_original_transaction",
]
FALLBACK_KEY_COLUMNS = [
    "date",
    "outflow_ils",
    "inflow_ils",
    "fingerprint",
]


def _normalize_date_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    if not s:
        return ""
    try:
        return str(datetime.strptime(s[:10], "%Y-%m-%d").date())
    except (ValueError, TypeError):
        return ""


def _prepare_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize to canonical column types for reconciliation comparison."""
    exprs: list[pl.Expr] = []

    str_cols = [
        "transaction_id",
        "source_payee_selected",
        "source_category_selected",
        "target_payee_selected",
        "target_category_selected",
        "decision_action",
        "update_maps",
        "fingerprint",
        "memo_append",
    ]
    for col in str_cols:
        if col in df.columns:
            exprs.append(pl.col(col).cast(pl.Utf8).fill_null("").str.strip_chars())
        else:
            exprs.append(pl.lit("").cast(pl.Utf8).alias(col))

    for col in ["outflow_ils", "inflow_ils"]:
        if col in df.columns:
            exprs.append(
                pl.col(col).cast(pl.Float64).fill_nan(0.0).fill_null(0.0).round(2)
            )
        else:
            exprs.append(pl.lit(0.0).cast(pl.Float64).alias(col))

    if "date" in df.columns:
        exprs.append(
            pl.col("date")
            .map_elements(_normalize_date_value, return_dtype=pl.Utf8)
            .fill_null("")
            .alias("date")
        )
    else:
        exprs.append(pl.lit("").cast(pl.Utf8).alias("date"))

    for col in ["reviewed", "changed"]:
        if col in df.columns:
            exprs.append(
                pl.col(col)
                .cast(pl.Utf8)
                .fill_null("")
                .str.strip_chars()
                .str.to_uppercase()
                .is_in(["1", "TRUE", "YES", "Y"])
                .alias(col)
            )
        else:
            exprs.append(pl.lit(False).alias(col))

    # Add any missing edit columns as null (preserves existing ones as-is)
    for col in PRESERVED_EDIT_COLUMNS:
        if col not in df.columns:
            exprs.append(pl.lit(None).alias(col))

    return df.with_columns(exprs)


def _add_occurrence_key(df: pl.DataFrame) -> pl.DataFrame:
    """Add _occurrence_key = 'transaction_id|<0-indexed occurrence within group>'.

    Replicates pandas groupby.cumcount() semantics: within each transaction_id
    group (in original row order), assigns occurrence indices 0, 1, 2, ...
    """
    return df.with_columns(
        (
            pl.col("transaction_id")
            + "|"
            + (pl.col("transaction_id").cum_count().over("transaction_id") - 1).cast(
                pl.Utf8
            )
        ).alias("_occurrence_key")
    )


def _extract_payload_dict(row: dict[str, Any]) -> dict[str, Any]:
    payload = {col: row.get(col) for col in PRESERVED_REVIEW_COLUMNS}
    if bool(row.get("changed", False)):
        payload.update({col: row.get(col) for col in PRESERVED_EDIT_COLUMNS})
    return payload


def _serialize_payload(payload: dict[str, Any]) -> tuple:
    out: list[Any] = []
    for col in [*PRESERVED_REVIEW_COLUMNS, *PRESERVED_EDIT_COLUMNS]:
        v = payload.get(col)
        if isinstance(v, (dict, list)):
            out.append(json.dumps(v, sort_keys=True, ensure_ascii=False))
        else:
            out.append(v)
    return tuple(out)


def _has_any_review_value(row: dict[str, Any]) -> bool:
    return bool(
        row.get("source_payee_selected")
        or row.get("source_category_selected")
        or row.get("target_payee_selected")
        or row.get("target_category_selected")
        or row.get("decision_action")
        or row.get("update_maps")
        or row.get("reviewed")
        or row.get("changed")
        or row.get("memo_append")
    )


def reconcile_reviewed_transactions(
    old_reviewed: pl.DataFrame,
    new_proposed: pl.DataFrame,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    old_prep = _prepare_polars(old_reviewed)
    new_prep = _prepare_polars(new_proposed)

    old_with_key = _add_occurrence_key(old_prep)
    new_with_key = _add_occurrence_key(new_prep)

    # --- Pass 1: direct match by occurrence key ---

    # Build old join side: preserved columns + match flag
    old_join_frame = old_with_key.select(
        ["_occurrence_key"] + PRESERVED_REVIEW_COLUMNS + PRESERVED_EDIT_COLUMNS
    ).with_columns(pl.lit(True).alias("_has_old_match"))

    # Left-join new (left) onto old (right); old columns get "_old" suffix on collision.
    joined = new_with_key.join(
        old_join_frame, on="_occurrence_key", how="left", suffix="_old"
    )
    joined = joined.with_columns(pl.col("_has_old_match").fill_null(False))

    # should_use_old: direct match exists AND NOT (new.reviewed=True AND old.reviewed=False)
    should_use_old_expr = pl.col("_has_old_match") & ~(
        pl.col("reviewed") & ~pl.col("reviewed_old").fill_null(False)
    )
    joined = joined.with_columns(should_use_old_expr.alias("_should_use_old"))

    # Coalesce preserved review columns from old when should_use_old.
    # These are simple types (String, Boolean) and work cleanly with when/then/otherwise.
    joined = joined.with_columns(
        [
            pl.when(pl.col("_should_use_old"))
            .then(pl.col(f"{col}_old"))
            .otherwise(pl.col(col))
            .alias(col)
            for col in PRESERVED_REVIEW_COLUMNS
        ]
    )

    # Coalesce edit columns (may be struct/object types) via Python list operations to
    # avoid Polars type-unification errors when new is Null and old is Struct.
    should_use_old_list = joined["_should_use_old"].to_list()
    changed_old_list = joined["changed_old"].fill_null(False).to_list()
    for col in PRESERVED_EDIT_COLUMNS:
        old_col_name = f"{col}_old"
        new_values = joined[col].to_list()
        old_values = (
            joined[old_col_name].to_list()
            if old_col_name in joined.columns
            else [None] * len(joined)
        )
        merged_values = [
            old_v if (su and bool(co)) else new_v
            for new_v, old_v, su, co in zip(
                new_values, old_values, should_use_old_list, changed_old_list
            )
        ]
        # Use pl.Object for edit columns when any non-None value exists.
        # pl.from_pandas may produce Struct, String, or Null dtypes for complex nested
        # columns depending on the input; pl.Object is the safe common ground.
        dtype = pl.Object if any(v is not None for v in merged_values) else pl.Null
        drop_these = [col, old_col_name] if old_col_name in joined.columns else [col]
        joined = joined.drop(drop_these).with_columns(
            pl.Series(col, merged_values, dtype=dtype)
        )

    direct_matches = int(joined["_should_use_old"].sum())

    # Drop all "_old"-suffixed and join-helper columns.
    # Keep _should_use_old (needed for pass 2) and _occurrence_key (dropped at the very end).
    drop_cols = [
        c
        for c in joined.columns
        if (c.endswith("_old") and c != "_should_use_old") or c == "_has_old_match"
    ]
    result = joined.drop(drop_cols)

    # --- Pass 2: fallback match by (date, outflow_ils, inflow_ils, fingerprint) ---

    # remaining_old = old rows whose occurrence key does not appear in any new occurrence key
    new_occ_keys: set[str] = set(new_with_key["_occurrence_key"].to_list())
    remaining_old_rows = [
        row
        for row in old_with_key.iter_rows(named=True)
        if row["_occurrence_key"] not in new_occ_keys
    ]

    fallback_matches = 0

    if remaining_old_rows:
        # Group remaining_old rows by fallback key; collect unanimous payload decisions
        old_groups: dict[tuple, list[dict]] = defaultdict(list)
        for row in remaining_old_rows:
            key = (
                str(row["date"]),
                round(float(row["outflow_ils"]), 2),
                round(float(row["inflow_ils"]), 2),
                str(row["fingerprint"]),
            )
            old_groups[key].append(row)

        old_group_counts: Counter[tuple] = Counter(
            {k: len(v) for k, v in old_groups.items()}
        )
        decision_sets: dict[tuple, dict] = {}
        for key, rows in old_groups.items():
            payloads = [
                _extract_payload_dict(r) for r in rows if _has_any_review_value(r)
            ]
            if not payloads:
                continue
            if len({_serialize_payload(p) for p in payloads}) == 1:
                decision_sets[key] = payloads[0]

        if decision_sets:
            # Count remaining_new rows per fallback key (rows not covered by direct match)
            new_group_counts: Counter[tuple] = Counter()
            for row in result.iter_rows(named=True):
                if row["_should_use_old"]:
                    continue
                key = (
                    str(row["date"]),
                    round(float(row["outflow_ils"]), 2),
                    round(float(row["inflow_ils"]), 2),
                    str(row["fingerprint"]),
                )
                new_group_counts[key] += 1

            # Identify fallback hits: {result_row_index: decision_payload}
            fallback_updates: dict[int, dict] = {}
            for i, row in enumerate(result.iter_rows(named=True)):
                if row["_should_use_old"]:
                    continue
                key = (
                    str(row["date"]),
                    round(float(row["outflow_ils"]), 2),
                    round(float(row["inflow_ils"]), 2),
                    str(row["fingerprint"]),
                )
                if key not in decision_sets:
                    continue
                if new_group_counts[key] != 1:
                    continue
                if old_group_counts[key] < 1:
                    continue
                decision = decision_sets[key]
                # Don't override a new "auto-reviewed" row with an old un-reviewed decision
                if bool(row.get("reviewed", False)) and not bool(
                    decision.get("reviewed", False)
                ):
                    continue
                fallback_updates[i] = decision
                fallback_matches += 1

            if fallback_updates:
                # Apply updates column by column, preserving each column's existing dtype
                # (critical for pl.Object columns holding Python dicts/lists).
                for col in list(PRESERVED_REVIEW_COLUMNS) + list(
                    PRESERVED_EDIT_COLUMNS
                ):
                    is_edit_col = col in PRESERVED_EDIT_COLUMNS
                    values = result[col].to_list()
                    changed = False
                    for row_idx, payload in fallback_updates.items():
                        if is_edit_col and not bool(payload.get("changed", False)):
                            continue
                        values[row_idx] = payload.get(col, values[row_idx])
                        changed = True
                    if not changed:
                        continue
                    dtype = pl.Object if any(v is not None for v in values) else pl.Null
                    result = result.drop(col).with_columns(
                        pl.Series(col, values, dtype=dtype)
                    )

    # Drop all internal helper columns
    drop_final = [c for c in result.columns if c.startswith("_")]
    result = result.drop(drop_final)

    untouched = len(result) - direct_matches - fallback_matches
    return result, {
        "direct_matches": direct_matches,
        "fallback_matches": fallback_matches,
        "untouched_rows": untouched,
    }
