from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl

import ynab_il_importer.review_app.model as model
from ynab_il_importer.safe_types import normalize_flag_series


def _txn_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _txn_text(txn: dict[str, Any], field: str) -> str:
    return str(txn.get(field, "") or "").strip()


def _txn_splits(txn: dict[str, Any]) -> list[dict[str, Any]]:
    splits = txn.get("splits") or []
    return [split for split in splits if isinstance(split, dict)]


def _txn_split_text(txn: dict[str, Any]) -> str:
    parts: list[str] = []
    for split in _txn_splits(txn):
        for field in ["split_id", "payee_raw", "category_raw", "memo"]:
            text = _txn_text(split, field)
            if text:
                parts.append(text)
    return " ".join(parts)


def canonical_review_helpers(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(
            [
                pl.lit(False).alias("source_is_split"),
                pl.lit(False).alias("target_is_split"),
                pl.lit(0).alias("source_split_count"),
                pl.lit(0).alias("target_split_count"),
                pl.lit("").alias("source_display_payee"),
                pl.lit("").alias("target_display_payee"),
                pl.lit("").alias("source_display_category"),
                pl.lit("").alias("target_display_category"),
                pl.lit("").alias("source_display_account"),
                pl.lit("").alias("target_display_account"),
                pl.lit("").alias("source_display_date"),
                pl.lit("").alias("target_display_date"),
            ]
        )

    source_txn = pl.col("source_transaction")
    target_txn = pl.col("target_transaction")

    return df.with_columns(
        [
            source_txn
            .map_elements(
                lambda txn: bool(_txn_splits(_txn_mapping(txn))),
                return_dtype=pl.Boolean,
                skip_nulls=False,
            )
            .alias("source_is_split"),
            target_txn
            .map_elements(
                lambda txn: bool(_txn_splits(_txn_mapping(txn))),
                return_dtype=pl.Boolean,
                skip_nulls=False,
            )
            .alias("target_is_split"),
            source_txn
            .map_elements(
                lambda txn: len(_txn_splits(_txn_mapping(txn))),
                return_dtype=pl.Int64,
                skip_nulls=False,
            )
            .alias("source_split_count"),
            target_txn
            .map_elements(
                lambda txn: len(_txn_splits(_txn_mapping(txn))),
                return_dtype=pl.Int64,
                skip_nulls=False,
            )
            .alias("target_split_count"),
            source_txn
            .map_elements(
                lambda txn: _txn_text(_txn_mapping(txn), "payee_raw"),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("source_display_payee"),
            target_txn
            .map_elements(
                lambda txn: _txn_text(_txn_mapping(txn), "payee_raw"),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("target_display_payee"),
            source_txn
            .map_elements(
                lambda txn: model.normalize_category_value(
                    _txn_text(_txn_mapping(txn), "category_raw")
                ),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("source_display_category"),
            target_txn
            .map_elements(
                lambda txn: model.normalize_category_value(
                    _txn_text(_txn_mapping(txn), "category_raw")
                ),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("target_display_category"),
            source_txn
            .map_elements(
                lambda txn: _txn_text(_txn_mapping(txn), "account_name")
                or _txn_text(_txn_mapping(txn), "source_account"),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("source_display_account"),
            target_txn
            .map_elements(
                lambda txn: _txn_text(_txn_mapping(txn), "account_name")
                or _txn_text(_txn_mapping(txn), "source_account"),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("target_display_account"),
            source_txn
            .map_elements(
                lambda txn: _txn_text(_txn_mapping(txn), "date"),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("source_display_date"),
            target_txn
            .map_elements(
                lambda txn: _txn_text(_txn_mapping(txn), "date"),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("target_display_date"),
        ]
    )


def canonical_search_text_series(df: pl.DataFrame) -> pd.Series:
    if df.is_empty():
        return pd.Series(dtype="string")

    helpers = canonical_review_helpers(df)
    table = df.with_columns(
        [
            pl.col("source_transaction")
            .map_elements(
                lambda txn: _txn_text(_txn_mapping(txn), "memo"),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("source_transaction_memo"),
            pl.col("target_transaction")
            .map_elements(
                lambda txn: _txn_text(_txn_mapping(txn), "memo"),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("target_transaction_memo"),
            pl.col("source_transaction")
            .map_elements(
                lambda txn: _txn_split_text(_txn_mapping(txn)),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("source_split_text"),
            pl.col("target_transaction")
            .map_elements(
                lambda txn: _txn_split_text(_txn_mapping(txn)),
                return_dtype=pl.String,
                skip_nulls=False,
            )
            .alias("target_split_text"),
        ]
    )
    search_columns = [
        "review_transaction_id",
        "payee_options",
        "category_options",
        "match_status",
        "decision_action",
        "update_maps",
        "source_context_kind",
        "source_context_category_name",
        "source_context_matching_split_ids",
        "target_context_kind",
        "target_context_matching_split_ids",
        "memo_append",
        "source_transaction_memo",
        "target_transaction_memo",
        "source_split_text",
        "target_split_text",
    ]
    view = table.select(search_columns).to_pandas()
    helper_view = helpers.select(
        [
            "source_display_payee",
            "target_display_payee",
            "source_display_category",
            "target_display_category",
            "source_display_account",
            "target_display_account",
            "source_display_date",
            "target_display_date",
        ]
    ).to_pandas()
    text = pd.Series([""] * len(view), index=view.index, dtype="string")
    for column in list(view.columns) + list(helper_view.columns):
        frame = view if column in view.columns else helper_view
        text = text + " " + frame[column].astype("string").fillna("")
    return text.str.casefold()


def series_or_default(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype("string").fillna("")
    # ``payee_selected`` / ``category_selected`` are target-side compatibility
    # aliases used by some review flows and older helpers. Persisted review data
    # remains side-specific in ``target_*_selected`` columns.
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
    return normalize_flag_series(df[col])


def _id_series(df: pd.DataFrame, col: str) -> pd.Series:
    return series_or_default(df, col).str.strip()


def _component_mask_from_map(
    df: pd.DataFrame,
    idx: Any,
    component_map: dict[Any, int],
) -> pd.Series:
    component_label = component_map.get(idx)
    if component_label is None:
        return pd.Series([False] * len(df), index=df.index)
    return pd.Series(
        [component_map.get(current_idx) == component_label for current_idx in df.index],
        index=df.index,
    )


def _missing_value_masks(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    decision_action = _decision_action_series(df)
    create_target = decision_action.eq("create_target")
    payee_blank = series_or_default(df, "payee_selected").str.strip() == ""
    payee = series_or_default(df, "payee_selected").str.strip()
    category = series_or_default(df, "category_selected").map(model.normalize_category_value)
    category_blank = category.eq("")
    no_category_required = category.map(model.is_no_category_required)
    transfer_payee = payee.map(model.is_transfer_payee)
    missing_payee = create_target & payee_blank
    missing_category = create_target & (category_blank | no_category_required) & ~transfer_payee
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
        missing_in_base = aligned.isna().all(axis=1)
        changed = (current != aligned).any(axis=1) | missing_in_base
        return pd.Series(changed.to_numpy(), index=df.index)

    current = df[cols].copy()
    baseline = base[cols].reindex(df.index)
    missing_in_base = baseline.isna().all(axis=1)
    return (current != baseline).any(axis=1) | missing_in_base


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


def required_category_missing_mask(df: pd.DataFrame) -> pd.Series:
    payee = series_or_default(df, "payee_selected").str.strip()
    category = series_or_default(df, "category_selected").map(model.normalize_category_value)
    transfer = payee.map(model.is_transfer_payee)
    return (category.eq("") | category.map(model.is_no_category_required)) & ~transfer


def uncategorized_mask(df: pd.DataFrame) -> pd.Series:
    payee = series_or_default(df, "payee_selected").str.strip()
    transfer = payee.map(model.is_transfer_payee)
    category = (
        series_or_default(df, "category_selected")
        .map(model.normalize_category_value)
        .str.casefold()
    )
    return category.str.contains("uncategorized", regex=False) & ~transfer


def truthy_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return normalize_flag_series(df[column])


def primary_state_series(df: pd.DataFrame, blocker_series: pd.Series) -> pd.Series:
    reviewed = truthy_series(df, "reviewed")
    blocker = blocker_series.astype("string").fillna("").str.strip()
    action = action_series(df)
    states: list[str] = []
    for idx in df.index:
        if bool(reviewed.loc[idx]) and blocker.loc[idx] in {"", "None"}:
            states.append("Settled")
        elif blocker.loc[idx] not in {"", "None"}:
            states.append("Fix")
        elif action.loc[idx] != "No decision":
            states.append("Decide")
        else:
            states.append("Fix")
    return pd.Series(states, index=df.index, dtype="string")


def row_kind_series(df: pd.DataFrame) -> pd.Series:
    match_status = series_or_default(df, "match_status").str.strip().str.casefold()
    labels = pd.Series(["Other"] * len(df), index=df.index, dtype="string")
    labels = labels.where(~match_status.eq("matched_cleared"), "Matched cleared")
    labels = labels.where(~match_status.eq("matched_auto"), "Matched")
    labels = labels.where(~match_status.eq("source_only"), "Source only")
    labels = labels.where(~match_status.eq("target_only"), "Target only")
    labels = labels.where(~match_status.eq("ambiguous"), "Ambiguous")
    labels = labels.where(~match_status.eq("unrecognized"), "Unrecognized")
    return labels


def action_series(df: pd.DataFrame) -> pd.Series:
    import ynab_il_importer.review_app.validation as review_validation

    return review_validation.normalize_decision_actions(
        series_or_default(df, "decision_action")
    ).astype("string")


def suggestion_series(df: pd.DataFrame) -> pd.Series:
    source_present = truthy_series(df, "source_present")
    target_present = truthy_series(df, "target_present")
    source_payee_selected = series_or_default(df, "source_payee_selected").str.strip()
    source_category_selected = series_or_default(df, "source_category_selected").str.strip()
    target_payee_selected = series_or_default(df, "target_payee_selected").str.strip()
    target_category_selected = series_or_default(df, "target_category_selected").str.strip()
    payee_options = series_or_default(df, "payee_options").str.strip()
    category_options = series_or_default(df, "category_options").str.strip()
    has_missing_side_suggestions = (
        ~source_present
        & (
            source_payee_selected.ne("")
            | source_category_selected.ne("")
        )
    ) | (
        ~target_present
        & (
            target_payee_selected.ne("")
            | target_category_selected.ne("")
            | payee_options.ne("")
            | category_options.ne("")
        )
    )
    return pd.Series(
        ["Has suggestions" if bool(value) else "No suggestions" for value in has_missing_side_suggestions],
        index=df.index,
        dtype="string",
    )


def map_update_filter_series(df: pd.DataFrame) -> pd.Series:
    has_updates = series_or_default(df, "update_maps").str.strip().ne("")
    return pd.Series(
        ["Has update_maps" if bool(value) else "No update_maps" for value in has_updates],
        index=df.index,
        dtype="string",
    )


def search_text_series(df: pd.DataFrame) -> pd.Series:
    columns = [
        "fingerprint",
        "memo",
        "memo_append",
        "description_raw",
        "description_clean",
        "payee_options",
        "category_options",
        "source_payee_current",
        "source_payee_selected",
        "source_category_selected",
        "target_payee_current",
        "target_payee_selected",
        "target_category_selected",
        "source_memo",
        "target_memo",
        "source_account",
        "target_account",
        "account_name",
        "source",
        "decision_action",
        "update_maps",
    ]
    parts = [series_or_default(df, column) for column in columns]
    text = pd.Series([""] * len(df), index=df.index, dtype="string")
    for part in parts:
        text = text + " " + part.astype("string").fillna("")
    return text.str.casefold()


def _row_key_series(df: pd.DataFrame) -> pd.Series:
    if "transaction_id" not in df.columns:
        return pd.Series(df.index.astype("string"), index=df.index, dtype="string")
    txn_id = df["transaction_id"].astype("string").fillna("")
    occurrence = txn_id.groupby(txn_id).cumcount().astype("string")
    return txn_id + "|" + occurrence


def derive_inference_tags(df: pd.DataFrame) -> pd.Series:
    match_status = series_or_default(df, "match_status").str.strip().str.lower()
    payee = series_or_default(df, "payee_selected").str.strip()
    missing_required = payee.eq("") | required_category_missing_mask(df)

    inferred = pd.Series(["unique"] * len(df), index=df.index, dtype="string")
    inferred = inferred.where(~match_status.eq("none"), "unrecognized")
    inferred = inferred.where(~match_status.eq("ambiguous"), "ambiguous")
    inferred = inferred.where(
        ~(~match_status.isin(["none", "ambiguous"]) & missing_required), "missing"
    )
    unknown = (
        ~match_status.isin(["", "none", "ambiguous", "unique"])
        & ~missing_required
    )
    inferred = inferred.where(~unknown, match_status)
    return inferred


def initial_inference_tags(df: pd.DataFrame, base: pd.DataFrame | None) -> pd.Series:
    fallback = derive_inference_tags(df)
    if base is None or base.empty:
        return fallback

    base_keys = _row_key_series(base)
    base_inference = derive_inference_tags(base)
    base_map = pd.Series(base_inference.to_numpy(), index=base_keys)

    current_keys = _row_key_series(df)
    aligned = current_keys.map(base_map)
    return aligned.fillna(fallback).astype("string")


def apply_row_filters(
    df: pd.DataFrame,
    *,
    primary_state: list[str],
    row_kind: list[str],
    action_filter: list[str],
    save_status: list[str],
    blocker_filter: list[str],
    suggestion_filter: list[str],
    map_update_filter: list[str],
    primary_state_series: pd.Series,
    row_kind_series: pd.Series,
    action_series: pd.Series,
    save_state: pd.Series,
    blocker_series: pd.Series,
    suggestion_series: pd.Series,
    map_update_series: pd.Series,
    search_query: str,
    search_text: pd.Series,
) -> pd.DataFrame:
    mask = pd.Series([True] * len(df), index=df.index)
    mask &= primary_state_series.isin(primary_state)
    mask &= row_kind_series.isin(row_kind)
    mask &= action_series.isin(action_filter)
    mask &= save_state.isin(save_status)
    mask &= blocker_series.isin(blocker_filter)
    mask &= suggestion_series.isin(suggestion_filter)
    mask &= map_update_series.isin(map_update_filter)

    if search_query:
        mask &= search_text.str.contains(search_query, regex=False)

    return df[mask]


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
    memo_append: str | None = None,
    update_maps: str | None = None,
    reviewed: bool | None = None,
    decision_action: str | None = None,
    component_map: dict[Any, int] | None = None,
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
    if memo_append is not None and "memo_append" in df.columns:
        df.at[idx, "memo_append"] = str(memo_append).strip()

    if decision_action is not None and "decision_action" in df.columns:
        df.at[idx, "decision_action"] = str(decision_action).strip()
    if reviewed is not None and "reviewed" in df.columns:
        if component_map is None:
            from ynab_il_importer.review_app.validation import connected_component_mask

            reviewed_mask = connected_component_mask(df, idx)
        else:
            reviewed_mask = _component_mask_from_map(df, idx, component_map)
        df.loc[reviewed_mask, "reviewed"] = bool(reviewed)
    return df
