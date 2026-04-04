from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl

import ynab_il_importer.review_app.state as review_state
from ynab_il_importer.safe_types import TRUE_VALUES, normalize_flag_series as _normalize_flag_series

import ynab_il_importer.review_app.model as model


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


def normalize_update_maps(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def normalize_flag_series(series: pd.Series) -> pd.Series:
    return _normalize_flag_series(series)


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


def normalize_decision_action(value: Any) -> str:
    text = _text(value)
    return text or NO_DECISION


def _truthy(value: Any) -> bool:
    return _text(value).casefold() in TRUE_VALUES or bool(value) is True


def _numeric(value: Any) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])


def _row_signed_amount(row: Any) -> float:
    return _numeric(_row_get(row, "inflow_ils", 0.0)) - _numeric(_row_get(row, "outflow_ils", 0.0))


def _row_has(row: Any, key: str) -> bool:
    if isinstance(row, pd.Series):
        return key in row.index
    if isinstance(row, dict):
        return key in row
    return hasattr(row, "__contains__") and key in row


def _row_get(row: Any, key: str, default: Any = "") -> Any:
    if isinstance(row, pd.Series):
        return row.get(key, default)
    if isinstance(row, dict):
        return row.get(key, default)
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key, default)
    return default


def _selected_value(row: Any, field: str, *, side: str) -> str:
    side_key = f"{side}_{field}_selected"
    if _row_has(row, side_key):
        value = _text(_row_get(row, side_key))
    elif side == "target":
        value = _text(_row_get(row, f"{field}_selected"))
    else:
        value = ""
    if field == "category":
        return model.normalize_category_value(value)
    return value


def _id_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    return df[column].astype("string").fillna("").str.strip()


def _id_list(frame: pd.DataFrame | pl.DataFrame, column: str) -> list[str]:
    if isinstance(frame, pd.DataFrame):
        return _id_series(frame, column).tolist()
    if column not in frame.columns:
        return [""] * frame.height
    return (
        frame.select(pl.col(column).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        .to_series()
        .to_list()
    )


def _component_map_from_lists(
    *,
    index_values: list[Any],
    source_ids: list[str],
    target_ids: list[str],
) -> dict[Any, int]:
    if not index_values:
        return {}

    parent = list(range(len(index_values)))
    rank = [0] * len(index_values)

    def find(pos: int) -> int:
        while parent[pos] != pos:
            parent[pos] = parent[parent[pos]]
            pos = parent[pos]
        return pos

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        if rank[left_root] < rank[right_root]:
            parent[left_root] = right_root
        elif rank[left_root] > rank[right_root]:
            parent[right_root] = left_root
        else:
            parent[right_root] = left_root
            rank[left_root] += 1

    first_by_source: dict[str, int] = {}
    first_by_target: dict[str, int] = {}

    for pos, source_id in enumerate(source_ids):
        if source_id:
            prior = first_by_source.get(source_id)
            if prior is None:
                first_by_source[source_id] = pos
            else:
                union(pos, prior)

    for pos, target_id in enumerate(target_ids):
        if target_id:
            prior = first_by_target.get(target_id)
            if prior is None:
                first_by_target[target_id] = pos
            else:
                union(pos, prior)

    root_to_component: dict[int, int] = {}
    component_map: dict[Any, int] = {}
    next_component_id = 0
    for pos, idx in enumerate(index_values):
        root = find(pos)
        if root not in root_to_component:
            root_to_component[root] = next_component_id
            next_component_id += 1
        component_map[idx] = root_to_component[root]

    return component_map


def _component_members(component_map: dict[Any, int]) -> dict[int, list[Any]]:
    members: dict[int, list[Any]] = {}
    for idx, label in component_map.items():
        members.setdefault(label, []).append(idx)
    return members


def _row_items(df: pd.DataFrame | pl.DataFrame, indices: list[Any]) -> list[tuple[Any, Any]]:
    if isinstance(df, pd.DataFrame):
        return [(idx, df.loc[idx]) for idx in indices if idx in df.index]
    rows = df.to_dicts()
    items: list[tuple[Any, Any]] = []
    for idx in indices:
        if not isinstance(idx, int) or idx < 0 or idx >= len(rows):
            continue
        items.append((idx, rows[idx]))
    return items


def connected_component_mask(df: pd.DataFrame, start_idx: Any) -> pd.Series:
    if start_idx not in df.index:
        return pd.Series([False] * len(df), index=df.index)
    component_map = precompute_components(df)
    component_label = component_map.get(start_idx)
    if component_label is None:
        return pd.Series([False] * len(df), index=df.index)
    return pd.Series(
        [component_map.get(idx) == component_label for idx in df.index],
        index=df.index,
    )


def precompute_components(df: pd.DataFrame | pl.DataFrame) -> dict[Any, int]:
    if isinstance(df, pd.DataFrame):
        if df.empty:
            return {}
        index_values = list(df.index)
    else:
        if df.is_empty():
            return {}
        index_values = list(range(df.height))
    source_ids = _id_list(df, "source_row_id")
    target_ids = _id_list(df, "target_row_id")
    return _component_map_from_lists(
        index_values=index_values,
        source_ids=source_ids,
        target_ids=target_ids,
    )


def precompute_component_errors(
    df: pd.DataFrame | pl.DataFrame,
    component_map: dict[Any, int],
    *,
    row_errors_by_index: dict[Any, list[str]] | None = None,
) -> dict[int, list[str]]:
    component_errors: dict[int, list[str]] = {}
    if not component_map:
        return component_errors

    component_members = _component_members(component_map)
    for label, indices in component_members.items():
        start_idx = indices[0]
        component_errors[label] = review_component_errors(
            df,
            start_idx,
            component_indices=indices,
            row_errors_by_index=row_errors_by_index,
        )

    return component_errors


def component_error_lookup(df: pd.DataFrame | pl.DataFrame) -> dict[Any, list[str]]:
    component_map = precompute_components(df)
    component_errors = precompute_component_errors(df, component_map)
    return {
        idx: component_errors.get(component_label, [])
        for idx, component_label in component_map.items()
    }


def blocker_series_with_components(
    df: pd.DataFrame,
    *,
    component_map: dict[Any, int] | None = None,
) -> tuple[pd.Series, dict[Any, int]]:
    uncategorized = review_state.uncategorized_mask(df)
    row_errors_by_index = precompute_row_errors(df)
    if component_map is None:
        component_map = precompute_components(df)
    component_errors = precompute_component_errors(
        df,
        component_map,
        row_errors_by_index=row_errors_by_index,
    )
    values = [
        blocker_label(
            row,
            component_errors=component_errors.get(component_map.get(idx, -1), []),
            uncategorized=bool(uncategorized.loc[idx]),
            row_errors=row_errors_by_index.get(idx, []),
        )
        for idx, row in df.iterrows()
    ]
    return pd.Series(values, index=df.index, dtype="string"), component_map


def validate_row(row: Any) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    source_payee = _selected_value(row, "payee", side="source")
    source_category = _selected_value(row, "category", side="source")
    target_payee = _selected_value(row, "payee", side="target")
    target_category = _selected_value(row, "category", side="target")
    action = normalize_decision_action(_row_get(row, "decision_action", ""))
    reviewed = _truthy(_row_get(row, "reviewed", False))
    workflow_type = _text(_row_get(row, "workflow_type")).casefold()
    source_split_mode = model.normalize_split_mode(_row_get(row, "source_split_mode", ""))
    target_split_mode = model.normalize_split_mode(_row_get(row, "target_split_mode", ""))
    source_splits = model.effective_split_records(row, side="source")
    target_splits = model.effective_split_records(row, side="target")
    source_category_required = not model.is_transfer_payee(source_payee)
    category_required = not model.is_transfer_payee(target_payee)
    source_no_category_required = model.is_no_category_required(source_category)
    target_no_category_required = model.is_no_category_required(target_category)
    update_maps = parse_update_maps(_row_get(row, "update_maps", ""))
    parent_signed_amount = _row_signed_amount(row)

    if reviewed and action == NO_DECISION:
        errors.append("reviewed row cannot have No decision")
    if workflow_type == "institutional" and action in SOURCE_MUTATION_ACTIONS:
        errors.append(f"{action} is not allowed for institutional sources")
    if action == "create_source":
        if not source_payee:
            errors.append("missing source payee")
        if (
            source_split_mode != "split"
            and source_category_required
            and (not source_category or source_no_category_required)
        ):
            errors.append("missing source category")
    if action == "create_target":
        if not target_payee:
            errors.append("missing target payee")
        if (
            target_split_mode != "split"
            and category_required
            and (not target_category or target_no_category_required)
        ):
            errors.append("missing target category")

    for side, splits, split_mode in [
        ("source", source_splits, source_split_mode),
        ("target", target_splits, target_split_mode),
    ]:
        if split_mode != "split":
            continue
        if not splits:
            errors.append(f"{side} split edit has no split lines")
            continue
        if len(splits) < 2:
            errors.append(f"{side} split edit needs at least two split lines")
        split_total = sum(model.split_amount_ils(split) for split in splits)
        if round(split_total, 2) != round(parent_signed_amount, 2):
            errors.append(f"{side} split totals do not match parent amount")
        for line_index, split in enumerate(splits, start=1):
            if not _text(split.get("split_id", "")):
                errors.append(f"{side} split line {line_index} is missing split_id")
            line_payee = _text(split.get("payee_raw", ""))
            line_category = model.normalize_category_value(split.get("category_raw", ""))
            if side == "target" and not line_payee:
                errors.append(f"{side} split line {line_index} is missing payee")
            if model.is_transfer_payee(line_payee):
                errors.append(f"{side} split line {line_index} uses unsupported transfer payee")
            if not line_category or model.is_no_category_required(line_category):
                errors.append(f"{side} split line {line_index} is missing category")

    if ";" in target_payee:
        warnings.append("payee contains ';'")
    if ";" in target_category:
        warnings.append("category contains ';'")

    payee_options = model.parse_option_string(_row_get(row, "payee_options", ""))
    category_options = model.parse_option_string(_row_get(row, "category_options", ""))
    if target_payee and payee_options and target_payee not in payee_options:
        warnings.append("payee not in options")
    if target_category and category_options and target_category not in category_options:
        warnings.append("category not in options")
    for token in update_maps:
        if token not in UPDATE_MAP_TOKENS:
            warnings.append(f"unknown update_maps token: {token}")

    return errors, warnings


def precompute_row_errors(df: pd.DataFrame | pl.DataFrame) -> dict[Any, list[str]]:
    if isinstance(df, pd.DataFrame):
        return {
            idx: validate_row(row)[0]
            for idx, row in df.iterrows()
        }
    return {
        idx: validate_row(row)[0]
        for idx, row in enumerate(df.to_dicts())
    }


def review_component_errors(
    df: pd.DataFrame | pl.DataFrame,
    start_idx: Any,
    *,
    component_mask: pd.Series | None = None,
    component_indices: list[Any] | None = None,
    row_errors_by_index: dict[Any, list[str]] | None = None,
) -> list[str]:
    if component_indices is None:
        if isinstance(df, pd.DataFrame):
            if component_mask is None:
                component_mask = connected_component_mask(df, start_idx)
            else:
                component_mask = component_mask.reindex(df.index, fill_value=False)
            component_indices = df.index[component_mask].tolist()
        else:
            component_map = precompute_components(df)
            component_label = component_map.get(start_idx)
            if component_label is None:
                return []
            component_indices = _component_members(component_map).get(component_label, [])

    component_rows = _row_items(df, component_indices)
    if not component_rows:
        return []

    errors: list[str] = []
    actions = [normalize_decision_action(_row_get(row, "decision_action", "")) for _, row in component_rows]
    source_ids = [_text(_row_get(row, "source_row_id", "")) for _, row in component_rows]
    target_ids = [_text(_row_get(row, "target_row_id", "")) for _, row in component_rows]
    workflow_types = [_text(_row_get(row, "workflow_type", "")).casefold() for _, row in component_rows]

    if any(action == NO_DECISION for action in actions):
        errors.append("connected rows still contain No decision")
    if any(
        workflow_type == "institutional" and action in SOURCE_MUTATION_ACTIONS
        for workflow_type, action in zip(workflow_types, actions, strict=False)
    ):
        errors.append("institutional rows cannot create or delete on the source side")

    for idx, row in component_rows:
        row_errors = (
            row_errors_by_index.get(idx, [])
            if row_errors_by_index is not None
            else validate_row(row)[0]
        )
        errors.extend([f"row {idx}: {message}" for message in row_errors])

    for source_id in sorted({value for value in source_ids if value}):
        group_actions = [action for sid, action in zip(source_ids, actions, strict=False) if sid == source_id]
        if sum(1 for action in group_actions if action in SOURCE_MATCH_ACTIONS) > 1:
            errors.append(f"source transaction {source_id} has multiple reviewed match outcomes")
        if any(action in SOURCE_MATCH_ACTIONS for action in group_actions) and any(
            action in SOURCE_DELETE_ACTIONS for action in group_actions
        ):
            errors.append(f"source transaction {source_id} is both matched and deleted")

    for target_id in sorted({value for value in target_ids if value}):
        group_actions = [action for tid, action in zip(target_ids, actions, strict=False) if tid == target_id]
        if sum(1 for action in group_actions if action in TARGET_MATCH_ACTIONS) > 1:
            errors.append(f"target transaction {target_id} has multiple reviewed match outcomes")
        if any(action in TARGET_MATCH_ACTIONS for action in group_actions) and any(
            action in TARGET_DELETE_ACTIONS for action in group_actions
        ):
            errors.append(f"target transaction {target_id} is both matched and deleted")

    return errors


def blocker_label(
    row: Any,
    *,
    component_errors: list[str],
    uncategorized: bool,
    row_errors: list[str] | None = None,
) -> str:
    if row_errors is None:
        row_errors = validate_row(row)[0]
    action = normalize_decision_action(_row_get(row, "decision_action", ""))
    combined_errors = list(row_errors) + list(component_errors)

    if any(
        ("multiple reviewed match outcomes" in error) or ("both matched and deleted" in error)
        for error in combined_errors
    ):
        return "Contradiction in component"
    if any(
        ("institutional" in error.casefold()) and ("source" in error.casefold())
        for error in combined_errors
    ):
        return "Institutional source mutation"
    if action == NO_DECISION or any("No decision" in error for error in combined_errors):
        return "No decision"
    if any(("missing" in error) and ("payee" in error) for error in row_errors):
        return "Missing payee"
    if any(("missing" in error) and ("category" in error) for error in row_errors):
        return "Missing category"
    return "None"


def blocker_series(df: pd.DataFrame) -> pd.Series:
    blocker_values, _ = blocker_series_with_components(df)
    return blocker_values


def allowed_decision_actions(row: Any) -> list[str]:
    workflow_type = str(_row_get(row, "workflow_type", "") or "").strip().casefold()
    source_present = _truthy(_row_get(row, "source_present", False))
    target_present = _truthy(_row_get(row, "target_present", False))

    actions = [NO_DECISION, "ignore_row"]
    if source_present and target_present:
        actions = [NO_DECISION, "keep_match", "delete_source", "delete_target", "delete_both", "ignore_row"]
    elif source_present and not target_present:
        actions = [NO_DECISION, "create_target", "delete_source", "ignore_row"]
    elif target_present and not source_present:
        actions = [NO_DECISION, "create_source", "delete_target", "ignore_row"]

    if workflow_type == "institutional":
        actions = [
            action
            for action in actions
            if action not in SOURCE_MUTATION_ACTIONS
        ]

    ordered: list[str] = []
    for action in actions:
        if action not in ordered:
            ordered.append(action)
    return ordered


def apply_review_state(
    edited_df: pl.DataFrame,
    indices: list[Any],
    *,
    reviewed: bool,
    component_map: dict[Any, int] | None = None,
) -> tuple[pl.DataFrame, list[str]]:
    updated, errors = _apply_review_state_pandas(
        edited_df.to_pandas(),
        indices,
        reviewed=reviewed,
        component_map=component_map,
    )
    return pl.from_pandas(updated), errors


def _apply_review_state_pandas(
    edited_df: pd.DataFrame,
    indices: list[Any],
    *,
    reviewed: bool,
    component_map: dict[Any, int] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    touched = [idx for idx in dict.fromkeys(indices) if idx in edited_df.index]
    if not touched:
        return edited_df.copy(), []

    updated = edited_df.copy()
    for idx in touched:
        updated = review_state._apply_row_edit_pandas(
            updated,
            idx,
            reviewed=reviewed,
            component_map=component_map,
        )

    if not reviewed:
        return updated, []

    errors: list[str] = []
    if component_map is None:
        component_map = precompute_components(updated)
    component_series = pd.Series(component_map).reindex(updated.index)
    seen_components: set[int] = set()
    for idx in touched:
        component_label = component_map.get(idx)
        if component_label is None or component_label in seen_components:
            continue
        seen_components.add(component_label)
        component_mask = component_series.eq(component_label).fillna(False)
        errors.extend(
            review_component_errors(updated, idx, component_mask=component_mask)
        )

    if errors:
        reverted = edited_df.copy()
        for idx in touched:
            reverted = review_state._apply_row_edit_pandas(
                reverted,
                idx,
                reviewed=False,
                component_map=component_map,
            )
        unique_errors = list(dict.fromkeys(errors))
        return reverted, unique_errors

    return updated, []


def apply_review_state_best_effort(
    edited_df: pl.DataFrame,
    indices: list[Any],
    *,
    reviewed: bool,
    component_map: dict[Any, int] | None = None,
) -> tuple[pl.DataFrame, list[str], list[Any]]:
    updated, errors, reviewed_indices = _apply_review_state_best_effort_pandas(
        edited_df.to_pandas(),
        indices,
        reviewed=reviewed,
        component_map=component_map,
    )
    return pl.from_pandas(updated), errors, reviewed_indices


def _apply_review_state_best_effort_pandas(
    edited_df: pd.DataFrame,
    indices: list[Any],
    *,
    reviewed: bool,
    component_map: dict[Any, int] | None = None,
) -> tuple[pd.DataFrame, list[str], list[Any]]:
    touched = [idx for idx in dict.fromkeys(indices) if idx in edited_df.index]
    if not touched:
        return edited_df.copy(), [], []

    if not reviewed:
        updated, errors = _apply_review_state_pandas(
            edited_df,
            touched,
            reviewed=reviewed,
            component_map=component_map,
        )
        return updated, errors, touched if not errors else []

    working = edited_df.copy()
    reviewed_indices: list[Any] = []
    errors: list[str] = []
    if component_map is None:
        component_map = precompute_components(working)

    grouped_indices: dict[int, list[Any]] = {}
    for idx in touched:
        label = component_map.get(idx)
        if label is None:
            continue
        grouped_indices.setdefault(label, []).append(idx)

    for component_label in sorted(grouped_indices):
        component_indices = grouped_indices[component_label]
        updated, component_errors = _apply_review_state_pandas(
            working,
            component_indices,
            reviewed=True,
            component_map=component_map,
        )
        if component_errors:
            errors.extend(component_errors)
            continue
        working = updated
        reviewed_indices.extend(component_indices)

    return working, list(dict.fromkeys(errors)), reviewed_indices


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
