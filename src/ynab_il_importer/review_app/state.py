from __future__ import annotations

from typing import Any

import pandas as pd

import ynab_il_importer.review_app.model as model


def series_or_default(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype("string").fillna("")
    if col == "payee_selected" and "target_payee_selected" in df.columns:
        return df["target_payee_selected"].astype("string").fillna("")
    if col == "category_selected" and "target_category_selected" in df.columns:
        return df["target_category_selected"].astype("string").fillna("")
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _decision_action_series(df: pd.DataFrame) -> pd.Series:
    return (
        series_or_default(df, "decision_action")
        .str.strip()
        .replace("", "No decision")
        .str.casefold()
    )


def _update_maps_series(df: pd.DataFrame) -> pd.Series:
    if "update_maps" in df.columns:
        return df["update_maps"].astype("string").fillna("").str.strip()
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return df[col].astype(bool).fillna(False)


def _id_series(df: pd.DataFrame, col: str) -> pd.Series:
    return series_or_default(df, col).str.strip()


def _missing_value_masks(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    decision_action = _decision_action_series(df)
    create_target = decision_action.eq("create_target")
    payee_blank = series_or_default(df, "payee_selected").str.strip() == ""
    category_blank = series_or_default(df, "category_selected").str.strip() == ""
    transfer_payee = series_or_default(df, "payee_selected").map(model.is_transfer_payee)
    missing_payee = create_target & payee_blank
    missing_category = create_target & category_blank & ~transfer_payee
    return missing_payee, missing_category, create_target


def unresolved_mask(df: pd.DataFrame) -> pd.Series:
    missing_payee, missing_category, _ = _missing_value_masks(df)
    decision_action = _decision_action_series(df)
    match_status = series_or_default(df, "match_status").str.strip().str.casefold()
    pending_decision = match_status.isin(["ambiguous", "source_only", "target_only"]) & decision_action.eq("no decision")
    return missing_payee | missing_category | pending_decision


def summary_counts(df: pd.DataFrame) -> dict[str, int]:
    missing_payee, missing_category, _ = _missing_value_masks(df)
    unresolved = unresolved_mask(df)
    update_maps = _update_maps_series(df).ne("")
    return {
        "total": len(df),
        "missing_payee": int(missing_payee.sum()),
        "missing_category": int(missing_category.sum()),
        "unresolved": int(unresolved.sum()),
        "update_maps": int(update_maps.sum()),
    }


def accept_defaults_mask(df: pd.DataFrame) -> pd.Series:
    return pd.Series([False] * len(df), index=df.index)


def modified_mask(df: pd.DataFrame, original: pd.DataFrame | None) -> pd.Series:
    if original is None or original.empty:
        return pd.Series([False] * len(df), index=df.index)
    cols = [
        col
        for col in [
            "source_payee_selected",
            "source_category_selected",
            "target_payee_selected",
            "target_category_selected",
            "update_maps",
            "decision_action",
            "reviewed",
        ]
        if col in df.columns and col in original.columns
    ]
    if not cols:
        return pd.Series([False] * len(df), index=df.index)
    return (df[cols] != original[cols]).any(axis=1)


def modified_count(df: pd.DataFrame, original: pd.DataFrame | None) -> int:
    return int(modified_mask(df, original).sum())


def changed_mask(df: pd.DataFrame, base: pd.DataFrame | None) -> pd.Series:
    if base is None or base.empty:
        return pd.Series([False] * len(df), index=df.index)
    cols = [
        col
        for col in [
            "source_payee_selected",
            "source_category_selected",
            "target_payee_selected",
            "target_category_selected",
            "update_maps",
            "decision_action",
            "reviewed",
        ]
        if col in df.columns and col in base.columns
    ]
    if not cols:
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


def saved_mask(original: pd.DataFrame | None, base: pd.DataFrame | None, current_index: pd.Index) -> pd.Series:
    if original is None or original.empty:
        return pd.Series([False] * len(current_index), index=current_index)

    changed = changed_mask(original, base).reindex(original.index, fill_value=False)
    reviewed = _bool_series(original, "reviewed")

    saved = (changed | reviewed).reindex(current_index, fill_value=False)
    return saved.astype(bool)


def apply_filters(df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    filtered = df.copy()

    match_status = filters.get("match_status")
    if match_status:
        filtered = filtered[filtered["match_status"].isin(match_status)]

    reviewed = _bool_series(df, "reviewed")
    reviewed_mode = str(filters.get("reviewed_mode", "") or "").strip().lower()
    if reviewed_mode == "unreviewed":
        filtered = filtered[~reviewed.reindex(filtered.index, fill_value=False)]
    elif reviewed_mode == "reviewed":
        filtered = filtered[reviewed.reindex(filtered.index, fill_value=False)]

    missing_payee, missing_category, _ = _missing_value_masks(filtered)
    unresolved = unresolved_mask(filtered)
    if filters.get("unresolved_only"):
        filtered = filtered[unresolved]
    if filters.get("missing_payee_only"):
        filtered = filtered[missing_payee]
    if filters.get("missing_category_only"):
        filtered = filtered[missing_category]

    fingerprint_query = str(filters.get("fingerprint_query", "") or "").strip().casefold()
    if fingerprint_query:
        filtered = filtered[
            series_or_default(filtered, "fingerprint").str.casefold().str.contains(fingerprint_query, regex=False)
        ]

    payee_query = str(filters.get("payee_query", "") or "").strip().casefold()
    if payee_query:
        payee_text = series_or_default(filtered, "payee_selected") + " " + series_or_default(filtered, "payee_options")
        filtered = filtered[payee_text.str.casefold().str.contains(payee_query, regex=False)]

    memo_query = str(filters.get("memo_query", "") or "").strip().casefold()
    if memo_query:
        memo_text = series_or_default(filtered, "memo") + " " + series_or_default(filtered, "description_raw") + " " + series_or_default(filtered, "description_clean")
        filtered = filtered[memo_text.str.casefold().str.contains(memo_query, regex=False)]

    source_query = str(filters.get("source_query", "") or "").strip().casefold()
    if source_query:
        filtered = filtered[
            series_or_default(filtered, "source").str.casefold().str.contains(source_query, regex=False)
        ]

    account_query = str(filters.get("account_query", "") or "").strip().casefold()
    if account_query:
        filtered = filtered[
            series_or_default(filtered, "account_name").str.casefold().str.contains(account_query, regex=False)
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


def connected_component_mask(df: pd.DataFrame, start_idx: Any) -> pd.Series:
    if start_idx not in df.index:
        return pd.Series([False] * len(df), index=df.index)

    source_ids = _id_series(df, "source_row_id")
    target_ids = _id_series(df, "target_row_id")
    component = pd.Series([False] * len(df), index=df.index)
    pending_rows = {start_idx}
    seen_sources: set[str] = set()
    seen_targets: set[str] = set()

    while pending_rows:
        row_mask = pd.Series(df.index.isin(pending_rows), index=df.index)
        new_rows = row_mask & ~component
        if not new_rows.any():
            break
        component |= new_rows
        current_sources = {value for value in source_ids.loc[new_rows].tolist() if value}
        current_targets = {value for value in target_ids.loc[new_rows].tolist() if value}
        seen_sources |= current_sources
        seen_targets |= current_targets
        pending_mask = (
            source_ids.isin(seen_sources)
            | target_ids.isin(seen_targets)
        ) & ~component
        pending_rows = set(df.index[pending_mask])

    return component


def related_rows_mask(
    df: pd.DataFrame,
    idx: Any,
    *,
    include_source: bool = False,
    include_target: bool = False,
) -> pd.Series:
    if idx not in df.index:
        return pd.Series([False] * len(df), index=df.index)

    mask = pd.Series([False] * len(df), index=df.index)
    mask.at[idx] = True

    if include_source:
        source_row_id = _id_series(df, "source_row_id").get(idx, "")
        if source_row_id:
            mask |= _id_series(df, "source_row_id").eq(source_row_id)
    if include_target:
        target_row_id = _id_series(df, "target_row_id").get(idx, "")
        if target_row_id:
            mask |= _id_series(df, "target_row_id").eq(target_row_id)
    return mask


def apply_row_edit(
    df: pd.DataFrame,
    idx: Any,
    *,
    payee: str | None = None,
    category: str | None = None,
    source_payee: str | None = None,
    source_category: str | None = None,
    target_payee: str | None = None,
    target_category: str | None = None,
    update_maps: str | None = None,
    reviewed: bool | None = None,
    decision_action: str | None = None,
) -> pd.DataFrame:
    target_payee = payee if target_payee is None else target_payee
    target_category = category if target_category is None else target_category

    source_row_id = _id_series(df, "source_row_id").get(idx, "")
    target_row_id = _id_series(df, "target_row_id").get(idx, "")

    if source_payee is not None or source_category is not None:
        source_mask = _id_series(df, "source_row_id").eq(source_row_id) if source_row_id else pd.Series([False] * len(df), index=df.index)
        if not source_row_id:
            source_mask.at[idx] = True
        if source_payee is not None and "source_payee_selected" in df.columns:
            df.loc[source_mask, "source_payee_selected"] = str(source_payee).strip()
        if source_category is not None and "source_category_selected" in df.columns:
            df.loc[source_mask, "source_category_selected"] = str(source_category).strip()

    if target_payee is not None or target_category is not None:
        target_mask = _id_series(df, "target_row_id").eq(target_row_id) if target_row_id else pd.Series([False] * len(df), index=df.index)
        if not target_row_id:
            target_mask.at[idx] = True
        if target_payee is not None:
            if "payee_selected" in df.columns:
                df.loc[target_mask, "payee_selected"] = str(target_payee).strip()
            if "target_payee_selected" in df.columns:
                df.loc[target_mask, "target_payee_selected"] = str(target_payee).strip()
        if target_category is not None:
            if "category_selected" in df.columns:
                df.loc[target_mask, "category_selected"] = str(target_category).strip()
            if "target_category_selected" in df.columns:
                df.loc[target_mask, "target_category_selected"] = str(target_category).strip()

    if update_maps is not None and "update_maps" in df.columns:
        df.at[idx, "update_maps"] = str(update_maps).strip()

    if decision_action is not None and "decision_action" in df.columns:
        df.at[idx, "decision_action"] = str(decision_action).strip()
    if reviewed is not None and "reviewed" in df.columns:
        df.loc[connected_component_mask(df, idx), "reviewed"] = bool(reviewed)
    return df
