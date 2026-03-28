from __future__ import annotations

from typing import Any

import pandas as pd

import ynab_il_importer.review_app.model as model


TRUE_VALUES = {"1", "true", "t", "yes", "y"}
NO_DECISION = "No decision"
UPDATE_MAP_TOKENS = (
    "fingerprint_add_source",
    "fingerprint_limit_source",
    "payee_add_fingerprint",
    "payee_limit_fingerprint",
)
SOURCE_MUTATION_ACTIONS = {"create_source", "delete_source", "delete_both"}
SOURCE_MATCH_ACTIONS = {"keep_match", "create_target"}
TARGET_MATCH_ACTIONS = {"keep_match", "create_source"}
SOURCE_DELETE_ACTIONS = {"delete_source", "delete_both"}
TARGET_DELETE_ACTIONS = {"delete_target", "delete_both"}


def normalize_flag_series(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip().str.lower()
    return text.isin(TRUE_VALUES)


def normalize_update_maps(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def normalize_decision_actions(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    return text.mask(text.eq(""), NO_DECISION)


def parse_update_maps(value: Any) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    seen: set[str] = set()
    ordered: list[str] = []
    for token in text.split(";"):
        token_text = token.strip()
        if not token_text or token_text in seen:
            continue
        ordered.append(token_text)
        seen.add(token_text)
    return ordered


def join_update_maps(values: list[str]) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        ordered.append(text)
        seen.add(text)
    return ";".join(ordered)


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _truthy(value: Any) -> bool:
    return _text(value).casefold() in TRUE_VALUES or bool(value) is True


def _selected_value(row: pd.Series, field: str, *, side: str) -> str:
    side_key = f"{side}_{field}_selected"
    if side_key in row.index:
        return _text(row.get(side_key))
    if side == "target":
        return _text(row.get(f"{field}_selected"))
    return ""


def _id_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    return df[column].astype("string").fillna("").str.strip()


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


def validate_row(row: pd.Series) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    source_payee = _selected_value(row, "payee", side="source")
    source_category = _selected_value(row, "category", side="source")
    target_payee = _selected_value(row, "payee", side="target")
    target_category = _selected_value(row, "category", side="target")
    action = normalize_decision_actions(pd.Series([row.get("decision_action", "")])).iloc[0]
    reviewed = _truthy(row.get("reviewed", False))
    workflow_type = _text(row.get("workflow_type")).casefold()
    source_category_required = not model.is_transfer_payee(source_payee)
    category_required = not model.is_transfer_payee(target_payee)
    update_maps = parse_update_maps(row.get("update_maps", ""))

    if reviewed and action == NO_DECISION:
        errors.append("reviewed row cannot have No decision")
    if workflow_type == "institutional" and action in SOURCE_MUTATION_ACTIONS:
        errors.append(f"{action} is not allowed for institutional sources")
    if action == "create_source":
        if not source_payee:
            errors.append("missing source payee")
        if source_category_required and not source_category:
            errors.append("missing source category")
    if action == "create_target":
        if not target_payee:
            errors.append("missing target payee")
        if category_required and not target_category:
            errors.append("missing target category")

    if ";" in target_payee:
        warnings.append("payee contains ';'")
    if ";" in target_category:
        warnings.append("category contains ';'")

    payee_options = model.parse_option_string(row.get("payee_options", ""))
    category_options = model.parse_option_string(row.get("category_options", ""))
    if target_payee and payee_options and target_payee not in payee_options:
        warnings.append("payee not in options")
    if target_category and category_options and target_category not in category_options:
        warnings.append("category not in options")
    for token in update_maps:
        if token not in UPDATE_MAP_TOKENS:
            warnings.append(f"unknown update_maps token: {token}")

    return errors, warnings


def review_component_errors(df: pd.DataFrame, start_idx: Any) -> list[str]:
    component = df.loc[connected_component_mask(df, start_idx)].copy()
    if component.empty:
        return []

    errors: list[str] = []
    actions = normalize_decision_actions(
        component.get("decision_action", pd.Series([""] * len(component), index=component.index))
    )
    source_ids = _id_series(component, "source_row_id")
    target_ids = _id_series(component, "target_row_id")
    workflow_type = component.get(
        "workflow_type",
        pd.Series([""] * len(component), index=component.index, dtype="string"),
    ).astype("string").fillna("").str.strip().str.casefold()

    if actions.eq(NO_DECISION).any():
        errors.append("connected rows still contain No decision")
    if ((workflow_type == "institutional") & actions.isin(SOURCE_MUTATION_ACTIONS)).any():
        errors.append("institutional rows cannot create or delete on the source side")

    for idx, row in component.iterrows():
        row_errors, _ = validate_row(row)
        errors.extend([f"row {idx}: {message}" for message in row_errors])

    for source_id in sorted({value for value in source_ids.tolist() if value}):
        group_actions = actions.loc[source_ids == source_id]
        if int(group_actions.isin(SOURCE_MATCH_ACTIONS).sum()) > 1:
            errors.append(f"source transaction {source_id} has multiple reviewed match outcomes")
        if group_actions.isin(SOURCE_MATCH_ACTIONS).any() and group_actions.isin(SOURCE_DELETE_ACTIONS).any():
            errors.append(f"source transaction {source_id} is both matched and deleted")

    for target_id in sorted({value for value in target_ids.tolist() if value}):
        group_actions = actions.loc[target_ids == target_id]
        if int(group_actions.isin(TARGET_MATCH_ACTIONS).sum()) > 1:
            errors.append(f"target transaction {target_id} has multiple reviewed match outcomes")
        if group_actions.isin(TARGET_MATCH_ACTIONS).any() and group_actions.isin(TARGET_DELETE_ACTIONS).any():
            errors.append(f"target transaction {target_id} is both matched and deleted")

    return errors


def inconsistent_fingerprints(df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for side in ["source", "target"]:
        id_col = f"{side}_row_id"
        payee_col = f"{side}_payee_selected"
        category_col = f"{side}_category_selected"
        if id_col not in df.columns or payee_col not in df.columns or category_col not in df.columns:
            continue
        ids = _id_series(df, id_col)
        combos = (
            df[payee_col].astype("string").fillna("").str.strip()
            + "||"
            + df[category_col].astype("string").fillna("").str.strip()
        )
        grouped = (
            pd.DataFrame({"row_id": ids, "_combo": combos})
            .loc[ids.ne("")]
            .groupby("row_id", dropna=False)["_combo"]
            .nunique()
            .reset_index(name="combo_count")
        )
        if not grouped.empty:
            grouped.insert(0, "side", side)
            frames.append(grouped[grouped["combo_count"] > 1])

    if not frames:
        return pd.DataFrame(columns=["side", "row_id", "combo_count"])
    return pd.concat(frames, ignore_index=True)
