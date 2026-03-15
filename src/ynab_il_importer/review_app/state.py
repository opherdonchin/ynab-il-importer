from __future__ import annotations

from typing import Any

import pandas as pd

import ynab_il_importer.review_app.model as model


def series_or_default(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype("string").fillna("")
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def summary_counts(df: pd.DataFrame) -> dict[str, int]:
    payee_blank = series_or_default(df, "payee_selected").str.strip() == ""
    category_blank = series_or_default(df, "category_selected").str.strip() == ""
    transfer_payee = series_or_default(df, "payee_selected").map(model.is_transfer_payee)
    missing_category = category_blank & ~transfer_payee
    unresolved = payee_blank | missing_category
    update_map = df.get("update_map", pd.Series([False] * len(df), index=df.index)).astype(
        bool
    )
    return {
        "total": len(df),
        "missing_payee": int(payee_blank.sum()),
        "missing_category": int(missing_category.sum()),
        "unresolved": int(unresolved.sum()),
        "update_map": int(update_map.sum()),
    }


def accept_defaults_mask(df: pd.DataFrame) -> pd.Series:
    payee = series_or_default(df, "payee_selected").str.strip()
    category = series_or_default(df, "category_selected").str.strip()
    transfer_payee = payee.map(model.is_transfer_payee)
    reviewed = df.get("reviewed", pd.Series([False] * len(df), index=df.index)).astype(bool)
    return (~reviewed) & payee.ne("") & (transfer_payee | category.ne(""))


def modified_mask(df: pd.DataFrame, original: pd.DataFrame | None) -> pd.Series:
    if original is None or original.empty:
        return pd.Series([False] * len(df), index=df.index)
    cols = ["payee_selected", "category_selected", "update_map"]
    for col in cols:
        if col not in df.columns or col not in original.columns:
            return pd.Series([False] * len(df), index=df.index)
    current = df[cols].copy()
    base = original[cols].copy()
    base["update_map"] = base["update_map"].astype(bool)
    current["update_map"] = current["update_map"].astype(bool)
    return (current != base).any(axis=1)


def modified_count(df: pd.DataFrame, original: pd.DataFrame | None) -> int:
    return int(modified_mask(df, original).sum())


def changed_mask(df: pd.DataFrame, base: pd.DataFrame | None) -> pd.Series:
    if base is None or base.empty:
        return pd.Series([False] * len(df), index=df.index)
    cols = ["payee_selected", "category_selected"]
    for col in cols:
        if col not in df.columns or col not in base.columns:
            return pd.Series([False] * len(df), index=df.index)
    if "transaction_id" in df.columns and "transaction_id" in base.columns:
        df_ids = df["transaction_id"].astype("string").fillna("")
        base_ids = base["transaction_id"].astype("string").fillna("")
        df_keys = df_ids + "|" + df_ids.groupby(df_ids).cumcount().astype("string")
        base_keys = base_ids + "|" + base_ids.groupby(base_ids).cumcount().astype("string")
        current = df.assign(_key=df_keys).set_index("_key")[cols].copy()
        baseline = base.assign(_key=base_keys).set_index("_key")[cols].copy()
        aligned = baseline.reindex(current.index)
        changed = (current != aligned).any(axis=1)
        return pd.Series(changed.to_numpy(), index=df.index)

    current = df[cols].copy()
    baseline = base[cols].reindex(df.index)
    return (current != baseline).any(axis=1)


def saved_mask(
    original: pd.DataFrame | None, base: pd.DataFrame | None, current_index: pd.Index
) -> pd.Series:
    if original is None or original.empty:
        return pd.Series([False] * len(current_index), index=current_index)

    changed = changed_mask(original, base).reindex(original.index, fill_value=False)
    if "reviewed" in original.columns:
        reviewed = original["reviewed"].astype(bool).fillna(False)
    else:
        reviewed = pd.Series([False] * len(original), index=original.index)

    saved = (changed | reviewed).reindex(current_index, fill_value=False)
    return saved.astype(bool)


def apply_filters(df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    filtered = df.copy()

    match_status = filters.get("match_status")
    if match_status:
        filtered = filtered[filtered["match_status"].isin(match_status)]

    reviewed = df.get("reviewed", pd.Series([False] * len(df), index=df.index)).astype(bool)
    reviewed_mode = str(filters.get("reviewed_mode", "") or "").strip().lower()
    if reviewed_mode == "unreviewed":
        filtered = filtered[~reviewed.reindex(filtered.index, fill_value=False)]
    elif reviewed_mode == "reviewed":
        filtered = filtered[reviewed.reindex(filtered.index, fill_value=False)]

    payee_blank = series_or_default(filtered, "payee_selected").str.strip() == ""
    category_blank = series_or_default(filtered, "category_selected").str.strip() == ""
    transfer_payee = series_or_default(filtered, "payee_selected").map(model.is_transfer_payee)
    unresolved = payee_blank | (category_blank & ~transfer_payee)
    if filters.get("unresolved_only"):
        filtered = filtered[unresolved]
    if filters.get("missing_payee_only"):
        filtered = filtered[payee_blank]
    if filters.get("missing_category_only"):
        filtered = filtered[category_blank & ~transfer_payee]

    fingerprint_query = str(filters.get("fingerprint_query", "") or "").strip().casefold()
    if fingerprint_query:
        filtered = filtered[
            series_or_default(filtered, "fingerprint")
            .str.casefold()
            .str.contains(fingerprint_query, regex=False)
        ]

    payee_query = str(filters.get("payee_query", "") or "").strip().casefold()
    if payee_query:
        payee_text = (
            series_or_default(filtered, "payee_selected")
            + " "
            + series_or_default(filtered, "payee_options")
        )
        filtered = filtered[
            payee_text.str.casefold().str.contains(payee_query, regex=False)
        ]

    memo_query = str(filters.get("memo_query", "") or "").strip().casefold()
    if memo_query:
        memo_text = (
            series_or_default(filtered, "memo")
            + " "
            + series_or_default(filtered, "description_raw")
            + " "
            + series_or_default(filtered, "description_clean")
        )
        filtered = filtered[
            memo_text.str.casefold().str.contains(memo_query, regex=False)
        ]

    source_query = str(filters.get("source_query", "") or "").strip().casefold()
    if source_query:
        filtered = filtered[
            series_or_default(filtered, "source")
            .str.casefold()
            .str.contains(source_query, regex=False)
        ]

    account_query = str(filters.get("account_query", "") or "").strip().casefold()
    if account_query:
        filtered = filtered[
            series_or_default(filtered, "account_name")
            .str.casefold()
            .str.contains(account_query, regex=False)
        ]

    return filtered


def most_common_value(series: pd.Series) -> str:
    clean = series.astype("string").fillna("").str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return ""
    return str(clean.value_counts().idxmax())


def most_common_by_fingerprint(df: pd.DataFrame, column: str) -> dict[str, str]:
    if "fingerprint" not in df.columns or column not in df.columns:
        return {}
    result: dict[str, str] = {}
    for fp, grp in df.groupby("fingerprint"):
        values = grp[column].astype("string").fillna("").str.strip()
        values = values[values != ""]
        if values.empty:
            continue
        result[fp] = values.value_counts().idxmax()
    return result


def apply_row_edit(
    df: pd.DataFrame,
    idx: Any,
    *,
    payee: str,
    category: str,
    update_map: bool,
    reviewed: bool = True,
) -> pd.DataFrame:
    df.at[idx, "payee_selected"] = payee
    df.at[idx, "category_selected"] = category
    df.at[idx, "update_map"] = bool(update_map)
    if "reviewed" in df.columns:
        df.at[idx, "reviewed"] = bool(reviewed)
    return df
