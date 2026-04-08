from __future__ import annotations

import math
from typing import Any

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


def _eligible_mask_values(
    eligible_mask: pl.Series | list[bool] | None,
    length: int,
) -> list[bool] | None:
    if eligible_mask is None:
        return None
    if isinstance(eligible_mask, pl.Series):
        values = eligible_mask.cast(pl.Boolean, strict=False).fill_null(False).to_list()
    else:
        values = [bool(value) for value in eligible_mask]
    if len(values) < length:
        values.extend([False] * (length - len(values)))
    return values[:length]


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
    import ynab_il_importer.review_app.state as review_state

    if df.is_empty() or "fingerprint" not in df.columns:
        return df

    fingerprint_value = str(fingerprint).strip()
    rows = df.to_dicts()
    eligible_values = _eligible_mask_values(eligible_mask, len(rows))
    touched_indices: list[int] = []

    for idx, row in enumerate(rows):
        row_fingerprint = str(row.get("fingerprint", "") or "").strip()
        if row_fingerprint != fingerprint_value:
            continue
        if eligible_values is not None and not eligible_values[idx]:
            continue
        touched_indices.append(idx)
        if payee is not None:
            row["payee_selected"] = payee
            if "target_payee_selected" in row:
                row["target_payee_selected"] = payee
        if category is not None:
            normalized_category = normalize_category_value(category)
            row["category_selected"] = normalized_category
            if "target_category_selected" in row:
                row["target_category_selected"] = normalized_category
        if update_maps is not None and "update_maps" in row:
            row["update_maps"] = str(update_maps).strip()
        if decision_action is not None and "decision_action" in row:
            row["decision_action"] = str(decision_action).strip()
        if reviewed is not None and "reviewed" in row:
            row["reviewed"] = bool(reviewed)

    if not touched_indices:
        return df

    updated = pl.from_dicts(rows, infer_schema_length=None)
    if payee is not None or category is not None:
        updated = review_state._update_current_transaction_values(
            updated,
            touched_indices,
            side="target",
            payee=payee,
            category=category,
        )
    updated = review_state._recompute_presence(updated, touched_indices)
    updated = review_state.recompute_changed_for_rows(updated, touched_indices)
    updated = review_state.rebuild_working_rows(updated, touched_indices)
    return updated


def apply_competing_row_resolution(
    df: pl.DataFrame,
    indices: list[Any],
) -> tuple[pl.DataFrame, list[Any]]:
    import ynab_il_importer.review_app.state as review_state
    import ynab_il_importer.review_app.validation as review_validation

    if df.is_empty():
        return df, []

    rows = df.to_dicts()
    touched: list[Any] = []
    for idx in dict.fromkeys(indices):
        if not isinstance(idx, int) or idx < 0 or idx >= len(rows):
            continue
        action = review_validation.normalize_decision_action(
            rows[idx].get("decision_action", review_validation.NO_DECISION)
        )
        if action in {review_validation.NO_DECISION, "ignore_row"}:
            continue
        include_source, include_target = competing_row_scope(action)
        if not include_source and not include_target:
            continue
        competing_indices = review_state.related_row_indices(
            df,
            idx,
            include_source=include_source,
            include_target=include_target,
        )
        competing_indices = [current_idx for current_idx in competing_indices if current_idx != idx]
        if not competing_indices:
            continue
        for current_idx in competing_indices:
            if "decision_action" in rows[current_idx]:
                rows[current_idx]["decision_action"] = "ignore_row"
        touched.extend(competing_indices)

    if not touched:
        return df, []

    updated = pl.from_dicts(rows, infer_schema_length=None)
    updated = review_state._recompute_presence(updated, touched)
    return updated, list(dict.fromkeys(touched))
