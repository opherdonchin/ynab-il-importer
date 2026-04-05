from __future__ import annotations

from collections import Counter
import json
from typing import Any

import pandas as pd
import polars as pl


PRESERVED_COLUMNS = [
    "source_payee_selected",
    "source_category_selected",
    "target_payee_selected",
    "target_category_selected",
    "decision_action",
    "update_maps",
    "reviewed",
    "changed",
    "memo_append",
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


def _normalize_bool_series(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.upper()
        .isin(["1", "TRUE", "YES", "Y"])
    )


def _decision_value_counts(row: pd.Series) -> int:
    source_payee = str(row.get("source_payee_selected", "") or "").strip()
    source_category = str(row.get("source_category_selected", "") or "").strip()
    target_payee = str(row.get("target_payee_selected", "") or "").strip()
    target_category = str(row.get("target_category_selected", "") or "").strip()
    decision_action = str(row.get("decision_action", "") or "").strip()
    update_maps = str(row.get("update_maps", "") or "").strip()
    reviewed = bool(row.get("reviewed", False))
    changed = bool(row.get("changed", False))
    memo_append = str(row.get("memo_append", "") or "").strip()
    return int(
        bool(
            source_payee
            or source_category
            or target_payee
            or target_category
            or decision_action
            or update_maps
            or reviewed
            or changed
            or memo_append
        )
    )


def _preserved_payload(row: pd.Series) -> dict[str, Any]:
    return {column: row.get(column) for column in PRESERVED_COLUMNS}


def _serialized_payload(payload: dict[str, Any]) -> tuple[Any, ...]:
    serialized: list[Any] = []
    for column in PRESERVED_COLUMNS:
        value = payload.get(column)
        if isinstance(value, (dict, list)):
            serialized.append(json.dumps(value, sort_keys=True, ensure_ascii=False))
        else:
            serialized.append(value)
    return tuple(serialized)


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [
        "transaction_id",
        "source_payee_selected",
        "source_category_selected",
        "target_payee_selected",
        "target_category_selected",
        "decision_action",
        "update_maps",
        "fingerprint",
        "memo_append",
    ]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype("string").fillna("").str.strip()

    for col in ["outflow_ils", "inflow_ils"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).round(2)

    if "date" not in out.columns:
        out["date"] = ""
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")

    if "reviewed" not in out.columns:
        out["reviewed"] = False
    else:
        out["reviewed"] = _normalize_bool_series(out["reviewed"])
    if "changed" not in out.columns:
        out["changed"] = False
    else:
        out["changed"] = _normalize_bool_series(out["changed"])
    for col in [
        "source_splits",
        "target_splits",
        "source_current_transaction",
        "target_current_transaction",
        "source_original_transaction",
        "target_original_transaction",
    ]:
        if col not in out.columns:
            out[col] = None
    return out


def _used_old_mask(old: pd.DataFrame, new: pd.DataFrame) -> pd.Series:
    old_keys = _occurrence_key_series(old)
    new_keys = set(_occurrence_key_series(new).tolist())
    return old_keys.isin(new_keys)


def _occurrence_key_series(df: pd.DataFrame) -> pd.Series:
    transaction_id = df["transaction_id"].astype("string").fillna("")
    occurrence = transaction_id.groupby(transaction_id, dropna=False).cumcount().astype("string")
    return transaction_id + "|" + occurrence


def _should_preserve_new_row(old_row: pd.Series, new_row: pd.Series) -> bool:
    old_reviewed = bool(old_row.get("reviewed", False))
    new_reviewed = bool(new_row.get("reviewed", False))
    return new_reviewed and not old_reviewed


def reconcile_reviewed_transactions(
    old_reviewed: pl.DataFrame,
    new_proposed: pl.DataFrame,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    merged, stats = _reconcile_reviewed_transactions_pandas(
        old_reviewed.to_pandas(),
        new_proposed.to_pandas(),
    )
    return pl.from_pandas(merged), stats


def _reconcile_reviewed_transactions_pandas(
    old_reviewed: pd.DataFrame,
    new_proposed: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    old = _prepare(old_reviewed)
    new = _prepare(new_proposed)
    result = new.copy()
    old["_occurrence_key"] = _occurrence_key_series(old)
    result["_occurrence_key"] = _occurrence_key_series(result)

    if "reviewed" not in result.columns:
        result["reviewed"] = False
    if "decision_action" not in result.columns:
        result["decision_action"] = ""

    direct_matches = 0
    fallback_matches = 0
    untouched = 0

    old_by_id = old.set_index("_occurrence_key")
    direct_candidates = result["_occurrence_key"].isin(old_by_id.index)
    if direct_candidates.any():
        matched = old_by_id.reindex(result.loc[direct_candidates, "_occurrence_key"])
        matched.index = result.loc[direct_candidates].index
        preserve_direct = pd.Series(
            [
                _should_preserve_new_row(old_row, new_row)
                for (_, old_row), (_, new_row) in zip(
                    matched.iterrows(),
                    result.loc[direct_candidates].iterrows(),
                    strict=False,
                )
            ],
            index=result.loc[direct_candidates].index,
        )
        direct_mask = direct_candidates.copy()
        direct_mask.loc[direct_candidates] = ~preserve_direct
        for col in PRESERVED_COLUMNS:
            result.loc[direct_mask, col] = matched.loc[~preserve_direct, col].to_numpy()
        direct_matches = int(direct_mask.sum())
    else:
        direct_mask = direct_candidates

    used_old = _used_old_mask(old, result)
    remaining_old = old.loc[~used_old].copy()
    remaining_new_mask = ~direct_mask
    remaining_new = result.loc[remaining_new_mask].copy()

    if not remaining_old.empty and not remaining_new.empty:
        decision_sets: dict[tuple[Any, ...], dict[str, Any]] = {}
        old_counts = Counter()
        for key, group in remaining_old.groupby(FALLBACK_KEY_COLUMNS, dropna=False):
            payloads = [_preserved_payload(row) for _, row in group.iterrows() if _decision_value_counts(row)]
            serialized_payloads = {_serialized_payload(payload) for payload in payloads}
            if len(serialized_payloads) == 1 and payloads:
                decision_sets[key] = payloads[0]
                old_counts[key] = len(group)

        new_group_counts = Counter()
        for key, group in remaining_new.groupby(FALLBACK_KEY_COLUMNS, dropna=False):
            new_group_counts[key] = len(group)

        for idx, row in remaining_new.iterrows():
            key = tuple(row[col] for col in FALLBACK_KEY_COLUMNS)
            if key not in decision_sets:
                continue
            if new_group_counts[key] != 1:
                continue
            if old_counts[key] < 1:
                continue
            if _should_preserve_new_row(
                pd.Series({"reviewed": bool(decision_sets[key].get("reviewed", False))}),
                row,
            ):
                continue
            for column, value in decision_sets[key].items():
                result.at[idx, column] = value
            fallback_matches += 1

    result = result.drop(columns=["_occurrence_key"], errors="ignore")
    untouched = len(result) - direct_matches - fallback_matches
    return result, {
        "direct_matches": direct_matches,
        "fallback_matches": fallback_matches,
        "untouched_rows": untouched,
    }
