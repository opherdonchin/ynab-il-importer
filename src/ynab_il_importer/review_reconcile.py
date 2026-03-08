from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd


DECISION_COLUMNS = [
    "payee_selected",
    "category_selected",
    "update_map",
    "reviewed",
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
    payee = str(row.get("payee_selected", "") or "").strip()
    category = str(row.get("category_selected", "") or "").strip()
    update_map = bool(row.get("update_map", False))
    reviewed = bool(row.get("reviewed", False))
    return int(bool(payee or category or update_map or reviewed))


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["transaction_id", "payee_selected", "category_selected", "fingerprint"]:
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

    for col in ["update_map", "reviewed"]:
        if col not in out.columns:
            out[col] = False
        else:
            out[col] = _normalize_bool_series(out[col])
    return out


def _used_old_mask(old: pd.DataFrame, new: pd.DataFrame) -> pd.Series:
    old_ids = old["transaction_id"].astype("string").fillna("")
    new_ids = set(new["transaction_id"].astype("string").fillna("").tolist())
    return old_ids.isin(new_ids)


def reconcile_reviewed_transactions(
    old_reviewed: pd.DataFrame,
    new_proposed: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    old = _prepare(old_reviewed)
    new = _prepare(new_proposed)
    result = new.copy()

    if "reviewed" not in result.columns:
        result["reviewed"] = False

    direct_matches = 0
    fallback_matches = 0
    untouched = 0

    old_by_id = (
        old.drop_duplicates(subset=["transaction_id"], keep="last")
        .set_index("transaction_id")[DECISION_COLUMNS]
    )
    direct_mask = result["transaction_id"].isin(old_by_id.index)
    if direct_mask.any():
        matched = old_by_id.reindex(result.loc[direct_mask, "transaction_id"])
        for col in DECISION_COLUMNS:
            result.loc[direct_mask, col] = matched[col].to_numpy()
        direct_matches = int(direct_mask.sum())

    used_old = _used_old_mask(old, result)
    remaining_old = old.loc[~used_old].copy()
    remaining_new_mask = ~direct_mask
    remaining_new = result.loc[remaining_new_mask].copy()

    if not remaining_old.empty and not remaining_new.empty:
        decision_sets: dict[tuple[Any, ...], tuple[Any, ...]] = {}
        old_counts = Counter()
        for key, group in remaining_old.groupby(FALLBACK_KEY_COLUMNS, dropna=False):
            tuples = {
                (
                    str(row.get("payee_selected", "") or "").strip(),
                    str(row.get("category_selected", "") or "").strip(),
                    bool(row.get("update_map", False)),
                    bool(row.get("reviewed", False)),
                )
                for _, row in group.iterrows()
                if _decision_value_counts(row)
            }
            if len(tuples) == 1:
                decision_sets[key] = next(iter(tuples))
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
            payee, category, update_map, reviewed = decision_sets[key]
            result.at[idx, "payee_selected"] = payee
            result.at[idx, "category_selected"] = category
            result.at[idx, "update_map"] = bool(update_map)
            result.at[idx, "reviewed"] = bool(reviewed)
            fallback_matches += 1

    untouched = len(result) - direct_matches - fallback_matches
    return result, {
        "direct_matches": direct_matches,
        "fallback_matches": fallback_matches,
        "untouched_rows": untouched,
    }
