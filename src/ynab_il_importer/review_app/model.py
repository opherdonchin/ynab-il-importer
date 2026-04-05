from __future__ import annotations

import math
from typing import Any

import pandas as pd
import polars as pl


NO_CATEGORY_REQUIRED = "None"


def parse_option_string(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    parts = [part.strip() for part in text.split(";")]
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        if not part or part in seen:
            continue
        ordered.append(part)
        seen.add(part)
    return ordered


def resolve_selected_value(selected_value: Any, override_value: Any) -> str:
    override = "" if override_value is None else str(override_value).strip()
    if override:
        return override
    return "" if selected_value is None else str(selected_value).strip()


def is_transfer_payee(value: Any) -> bool:
    text = "" if value is None else str(value).strip()
    return text.startswith("Transfer :")


def normalize_category_value(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    if text.casefold() == NO_CATEGORY_REQUIRED.casefold():
        return NO_CATEGORY_REQUIRED
    return text


def is_no_category_required(value: Any) -> bool:
    return normalize_category_value(value) == NO_CATEGORY_REQUIRED


def apply_to_same_fingerprint(
    df: pl.DataFrame,
    fingerprint: str,
    payee: str | None = None,
    category: str | None = None,
    update_maps: str | None = None,
    decision_action: str | None = None,
    reviewed: bool | None = None,
    eligible_mask: pl.Series | list[bool] | None = None,
) -> pl.DataFrame:
    pandas_df = df.to_pandas()
    updated = _apply_to_same_fingerprint_pandas(
        pandas_df,
        fingerprint,
        payee=payee,
        category=category,
        update_maps=update_maps,
        decision_action=decision_action,
        reviewed=reviewed,
        eligible_mask=_eligible_mask_for_index(eligible_mask, pandas_df.index),
    )
    return pl.from_pandas(updated)


def _eligible_mask_for_index(
    eligible_mask: pl.Series | list[bool] | None,
    index: pd.Index,
) -> pd.Series | None:
    if eligible_mask is None:
        return None
    if isinstance(eligible_mask, pl.Series):
        values = eligible_mask.cast(pl.Boolean, strict=False).fill_null(False).to_list()
    else:
        values = [bool(value) for value in eligible_mask]
    series = pd.Series(values, dtype=bool)
    if len(series) < len(index):
        series = series.reindex(range(len(index)), fill_value=False)
    return pd.Series(series.iloc[: len(index)].to_list(), index=index, dtype=bool)


def _apply_to_same_fingerprint_pandas(
    df: pd.DataFrame,
    fingerprint: str,
    *,
    payee: str | None = None,
    category: str | None = None,
    update_maps: str | None = None,
    decision_action: str | None = None,
    reviewed: bool | None = None,
    eligible_mask: pd.Series | None = None,
) -> pd.DataFrame:
    updated = df.copy()
    mask = updated["fingerprint"].astype("string").fillna("").str.strip() == str(fingerprint).strip()
    if eligible_mask is not None:
        mask = mask & eligible_mask
    if payee is not None:
        updated.loc[mask, "payee_selected"] = payee
        if "target_payee_current" in updated.columns:
            updated.loc[mask, "target_payee_current"] = payee
        if "target_payee_selected" in updated.columns:
            updated.loc[mask, "target_payee_selected"] = payee
    if category is not None:
        updated.loc[mask, "category_selected"] = category
        if "target_category_current" in updated.columns:
            updated.loc[mask, "target_category_current"] = category
        if "target_category_selected" in updated.columns:
            updated.loc[mask, "target_category_selected"] = category
    if update_maps is not None and "update_maps" in updated.columns:
        updated.loc[mask, "update_maps"] = str(update_maps).strip()
    if decision_action is not None and "decision_action" in updated.columns:
        updated.loc[mask, "decision_action"] = str(decision_action).strip()
    if reviewed is not None:
        updated.loc[mask, "reviewed"] = bool(reviewed)
    if "changed" in updated.columns:
        updated.loc[mask, "changed"] = True
    return updated


def competing_row_scope(decision_action: str) -> tuple[bool, bool]:
    import ynab_il_importer.review_app.validation as review_validation

    action = review_validation.normalize_decision_action(decision_action)
    include_source = action in (
        review_validation.SOURCE_MATCH_ACTIONS | review_validation.SOURCE_DELETE_ACTIONS
    )
    include_target = action in (
        review_validation.TARGET_MATCH_ACTIONS | review_validation.TARGET_DELETE_ACTIONS
    )
    return include_source, include_target


def apply_competing_row_resolution(
    df: pl.DataFrame,
    indices: list[Any],
) -> tuple[pl.DataFrame, list[Any]]:
    pandas_df = df.to_pandas()
    updated, touched = _apply_competing_row_resolution_pandas(pandas_df, indices)
    return pl.from_pandas(updated), touched


def _apply_competing_row_resolution_pandas(
    df: pd.DataFrame,
    indices: list[Any],
) -> tuple[pd.DataFrame, list[Any]]:
    import ynab_il_importer.review_app.state as review_state
    import ynab_il_importer.review_app.validation as review_validation

    updated = df.copy()
    touched: list[Any] = []
    for idx in dict.fromkeys(indices):
        if idx not in updated.index:
            continue
        action = review_validation.normalize_decision_action(
            updated.loc[idx, "decision_action"] if "decision_action" in updated.columns else ""
        )
        if action in {review_validation.NO_DECISION, "ignore_row"}:
            continue
        include_source, include_target = competing_row_scope(action)
        if not include_source and not include_target:
            continue
        competing_indices = review_state.related_row_indices(
            updated,
            idx,
            include_source=include_source,
            include_target=include_target,
        )
        competing_indices = [current_idx for current_idx in competing_indices if current_idx != idx]
        if not competing_indices:
            continue
        if "decision_action" in updated.columns:
            updated.loc[competing_indices, "decision_action"] = "ignore_row"
        if "changed" in updated.columns:
            updated.loc[competing_indices, "changed"] = True
        touched.extend(competing_indices)
    return updated, list(dict.fromkeys(touched))
