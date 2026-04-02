from __future__ import annotations

from typing import Any

import pandas as pd


NO_CATEGORY_REQUIRED = "None"


def parse_option_string(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
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
    df: pd.DataFrame,
    fingerprint: str,
    payee: str | None = None,
    category: str | None = None,
    update_maps: str | None = None,
    decision_action: str | None = None,
    reviewed: bool | None = None,
    eligible_mask: pd.Series | None = None,
) -> pd.DataFrame:
    mask = df["fingerprint"].astype("string").fillna("").str.strip() == str(fingerprint).strip()
    if eligible_mask is not None:
        eligible = eligible_mask.reindex(df.index, fill_value=False).astype(bool)
        mask = mask & eligible
    if payee is not None:
        df.loc[mask, "payee_selected"] = payee
        if "target_payee_selected" in df.columns:
            df.loc[mask, "target_payee_selected"] = payee
    if category is not None:
        df.loc[mask, "category_selected"] = category
        if "target_category_selected" in df.columns:
            df.loc[mask, "target_category_selected"] = category
    if update_maps is not None and "update_maps" in df.columns:
        df.loc[mask, "update_maps"] = str(update_maps).strip()
    if decision_action is not None and "decision_action" in df.columns:
        df.loc[mask, "decision_action"] = str(decision_action).strip()
    if reviewed is not None:
        df.loc[mask, "reviewed"] = bool(reviewed)
    return df


def competing_row_scope(decision_action: str) -> tuple[bool, bool]:
    import ynab_il_importer.review_app.validation as review_validation

    action = review_validation.normalize_decision_actions(pd.Series([decision_action])).iloc[0]
    include_source = action in (
        review_validation.SOURCE_MATCH_ACTIONS | review_validation.SOURCE_DELETE_ACTIONS
    )
    include_target = action in (
        review_validation.TARGET_MATCH_ACTIONS | review_validation.TARGET_DELETE_ACTIONS
    )
    return include_source, include_target


def apply_competing_row_resolution(
    df: pd.DataFrame,
    indices: list[Any],
) -> list[Any]:
    import ynab_il_importer.review_app.state as review_state
    import ynab_il_importer.review_app.validation as review_validation

    touched: list[Any] = []
    for idx in dict.fromkeys(indices):
        if idx not in df.index:
            continue
        action = review_validation.normalize_decision_actions(
            pd.Series([df.loc[idx, "decision_action"] if "decision_action" in df.columns else ""])
        ).iloc[0]
        if action in {review_validation.NO_DECISION, "ignore_row"}:
            continue
        include_source, include_target = competing_row_scope(action)
        if not include_source and not include_target:
            continue
        mask = review_state.related_rows_mask(
            df,
            idx,
            include_source=include_source,
            include_target=include_target,
        )
        if idx in mask.index:
            mask.loc[idx] = False
        competing_indices = df.index[mask].tolist()
        if not competing_indices:
            continue
        if "decision_action" in df.columns:
            df.loc[mask, "decision_action"] = "ignore_row"
        touched.extend(competing_indices)
    return list(dict.fromkeys(touched))
