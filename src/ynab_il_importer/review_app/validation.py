from __future__ import annotations

from typing import Any

import polars as pl

import ynab_il_importer.review_app.state as review_state
from ynab_il_importer.safe_types import TRUE_VALUES

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
TARGET_MATCH_ACTIONS = {"keep_match", "create_source", "update_target"}
SOURCE_DELETE_ACTIONS = {"delete_source", "delete_both"}
TARGET_DELETE_ACTIONS = {"delete_target", "delete_both"}


def _text_list(values: pl.Series | list[Any]) -> list[str]:
    if isinstance(values, pl.Series):
        raw_values = values.cast(pl.Utf8, strict=False).fill_null("").to_list()
    else:
        raw_values = list(values)
    return [str(value or "").strip() for value in raw_values]


def _bool_series(values: pl.Series | list[Any]) -> pl.Series:
    if isinstance(values, pl.Series):
        raw_values = values.to_list()
    else:
        raw_values = list(values)
    return pl.Series(
        [str(value or "").strip().casefold() in TRUE_VALUES for value in raw_values],
        dtype=pl.Boolean,
    )


def normalize_update_maps(series: pl.Series) -> pl.Series:
    return pl.Series(_text_list(series), dtype=pl.Utf8)


def normalize_flag_series(series: pl.Series) -> pl.Series:
    return _bool_series(series)


def normalize_decision_actions(series: pl.Series) -> pl.Series:
    return pl.Series([value or NO_DECISION for value in _text_list(series)], dtype=pl.Utf8)


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


def _row_has(row: Any, key: str) -> bool:
    if isinstance(row, dict):
        return key in row
    return hasattr(row, "__contains__") and key in row


def _row_get(row: Any, key: str, default: Any = "") -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key, default)
    return default


def _required_row_value(row: Any, key: str) -> Any:
    if not _row_has(row, key):
        raise ValueError(f"Review rows must include {key}")
    return _row_get(row, key)


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


def _optional_bool(row: Any, key: str) -> bool | None:
    if not _row_has(row, key):
        return None
    value = _row_get(row, key, None)
    if value is None or value == "":
        return None
    return _truthy(value)


def _id_series(df: pl.DataFrame, column: str) -> pl.Series:
    if column not in df.columns:
        return pl.Series([""] * len(df), dtype=pl.Utf8)
    return df.get_column(column).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()


def _id_list(frame: pl.DataFrame, column: str) -> list[str]:
    if column not in frame.columns:
        return [""] * frame.height
    return [str(value or "").strip() for value in _id_series(frame, column).to_list()]


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


def _row_items(rows: list[dict[str, Any]], indices: list[Any]) -> list[tuple[Any, Any]]:
    items: list[tuple[Any, Any]] = []
    for idx in indices:
        if not isinstance(idx, int) or idx < 0 or idx >= len(rows):
            continue
        items.append((idx, rows[idx]))
    return items


def connected_component_mask(df: pl.DataFrame, start_idx: Any) -> pl.Series:
    if not isinstance(start_idx, int) or start_idx < 0 or start_idx >= len(df):
        return pl.Series([False] * len(df), dtype=pl.Boolean)
    component_map = compute_components(df)
    component_label = component_map.get(start_idx)
    if component_label is None:
        return pl.Series([False] * len(df), dtype=pl.Boolean)
    return pl.Series(
        [component_map.get(idx) == component_label for idx in range(len(df))],
        dtype=pl.Boolean,
    )


def compute_components(df: pl.DataFrame) -> dict[Any, int]:
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


def compute_component_errors(
    df: pl.DataFrame,
    component_map: dict[Any, int],
    *,
    row_errors_by_index: dict[Any, list[str]] | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> dict[int, list[str]]:
    component_errors: dict[int, list[str]] = {}
    if not component_map:
        return component_errors

    row_cache = rows if rows is not None else df.to_dicts()
    component_members = _component_members(component_map)
    for label, indices in component_members.items():
        start_idx = indices[0]
        component_errors[label] = review_component_errors(
            df,
            start_idx,
            component_indices=indices,
            row_errors_by_index=row_errors_by_index,
            rows=row_cache,
        )

    return component_errors


def blocker_series_with_components(
    df: pl.DataFrame,
    *,
    component_map: dict[Any, int] | None = None,
) -> tuple[pl.Series, dict[Any, int]]:
    state = build_validation_state(df, component_map=component_map)
    return state["blocker_series"], state["component_map"]


def blocker_series_from_state(
    df: pl.DataFrame,
    *,
    component_map: dict[Any, int],
    row_errors_by_index: dict[Any, list[str]],
    component_errors: dict[int, list[str]],
    rows: list[dict[str, Any]] | None = None,
) -> pl.Series:
    uncategorized = review_state.uncategorized_mask(df)
    row_cache = rows if rows is not None else df.to_dicts()
    values = [
        blocker_label(
            row,
            component_errors=component_errors.get(component_map.get(idx, -1), []),
            uncategorized=bool(uncategorized[idx]),
            row_errors=row_errors_by_index.get(idx, []),
        )
        for idx, row in enumerate(row_cache)
    ]
    return pl.Series(values, dtype=pl.Utf8)


def build_validation_state(
    df: pl.DataFrame,
    *,
    component_map: dict[Any, int] | None = None,
) -> dict[str, Any]:
    if component_map is None:
        component_map = compute_components(df)
    rows = df.to_dicts()
    row_errors_by_index = {
        idx: validate_row(row)[0]
        for idx, row in enumerate(rows)
    }
    component_errors = compute_component_errors(
        df,
        component_map,
        row_errors_by_index=row_errors_by_index,
        rows=rows,
    )
    blocker_series = blocker_series_from_state(
        df,
        component_map=component_map,
        row_errors_by_index=row_errors_by_index,
        component_errors=component_errors,
        rows=rows,
    )
    return {
        "index": list(range(len(df))),
        "component_map": component_map,
        "row_errors_by_index": row_errors_by_index,
        "component_errors": component_errors,
        "blocker_series": blocker_series,
    }


def refresh_validation_state(
    df: pl.DataFrame,
    *,
    validation_state: dict[str, Any] | None = None,
    changed_indices: list[Any] | None = None,
) -> dict[str, Any]:
    if validation_state is None or changed_indices is None:
        return build_validation_state(df)

    cached_index = list(validation_state.get("index", []))
    current_index = list(range(len(df)))
    if cached_index != current_index:
        return build_validation_state(df)

    component_map = validation_state.get("component_map")
    if not isinstance(component_map, dict) or set(component_map.keys()) != set(current_index):
        return build_validation_state(df)

    row_errors_by_index = {
        idx: list(messages)
        for idx, messages in dict(validation_state.get("row_errors_by_index", {})).items()
    }
    component_errors = {
        int(label): list(messages)
        for label, messages in dict(validation_state.get("component_errors", {})).items()
    }
    blocker_series = validation_state.get("blocker_series")
    if not isinstance(blocker_series, pl.Series) or len(blocker_series) != len(df):
        blocker_series = pl.Series([""] * len(df), dtype=pl.Utf8)
    else:
        blocker_series = pl.Series(blocker_series.to_list(), dtype=pl.Utf8)

    touched = [idx for idx in dict.fromkeys(changed_indices) if isinstance(idx, int) and 0 <= idx < len(df)]
    if not touched:
        return {
            "index": current_index,
            "component_map": component_map,
            "row_errors_by_index": row_errors_by_index,
            "component_errors": component_errors,
            "blocker_series": blocker_series,
        }

    for idx in touched:
        row_errors_by_index[idx] = validate_row(df.row(idx, named=True))[0]

    component_members = _component_members(component_map)
    touched_components = {
        component_map[idx]
        for idx in touched
        if idx in component_map
    }
    uncategorized = review_state.uncategorized_mask(df)
    for component_label in touched_components:
        indices = component_members.get(component_label, [])
        if not indices:
            continue
        start_idx = indices[0]
        component_errors[component_label] = review_component_errors(
            df,
            start_idx,
            component_indices=indices,
            row_errors_by_index=row_errors_by_index,
        )
        for idx in indices:
            if 0 <= idx < len(df):
                current = blocker_series.to_list()
                current[idx] = blocker_label(
                    df.row(idx, named=True),
                    component_errors=component_errors.get(component_label, []),
                    uncategorized=bool(uncategorized[idx]),
                    row_errors=row_errors_by_index.get(idx, []),
                )
                blocker_series = pl.Series(current, dtype=pl.Utf8)

    return {
        "index": current_index,
        "component_map": component_map,
        "row_errors_by_index": row_errors_by_index,
        "component_errors": component_errors,
        "blocker_series": blocker_series,
    }


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
    source_category_required = model.category_required_for_payee(
        source_payee,
        current_account_on_budget=_optional_bool(row, "source_account_on_budget"),
        transfer_target_on_budget=_optional_bool(
            row, "source_transfer_account_on_budget"
        ),
    )
    category_required = model.category_required_for_payee(
        target_payee,
        current_account_on_budget=_optional_bool(row, "target_account_on_budget"),
        transfer_target_on_budget=_optional_bool(
            row, "target_transfer_account_on_budget"
        ),
    )
    source_no_category_required = model.is_no_category_required(source_category)
    target_no_category_required = model.is_no_category_required(target_category)
    update_maps = parse_update_maps(_row_get(row, "update_maps", ""))

    if reviewed and action == NO_DECISION:
        errors.append("accepted row cannot have No decision")
    if workflow_type == "institutional" and action in SOURCE_MUTATION_ACTIONS:
        errors.append(f"{action} is not allowed for institutional sources")
    if action == "create_source":
        if not source_payee:
            errors.append("missing source payee")
        if source_category_required and (not source_category or source_no_category_required):
            errors.append("missing source category")
    if action in {"create_target", "update_target"}:
        if not target_payee:
            errors.append("missing target payee")
        if category_required and (not target_category or target_no_category_required):
            errors.append("missing target category")

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


def validate_target_split_transaction(
    row: dict[str, Any],
    target_transaction: dict[str, Any],
) -> list[str]:
    from ynab_il_importer.artifacts.review_schema import validate_review_record

    record = {
        "source_current": row.get("source_current"),
        "source_original": row.get("source_original"),
        "target_current": target_transaction,
        "target_original": row.get("target_original"),
        "changed": True,
    }
    return [
        message
        for message in validate_review_record(record)
        if message.startswith("target_")
    ]


def compute_row_errors(df: pl.DataFrame) -> dict[Any, list[str]]:
    return {
        idx: validate_row(row)[0]
        for idx, row in enumerate(df.to_dicts())
    }


def review_component_errors(
    df: pl.DataFrame,
    start_idx: Any,
    *,
    component_mask: pl.Series | None = None,
    component_indices: list[Any] | None = None,
    row_errors_by_index: dict[Any, list[str]] | None = None,
    rows: list[dict[str, Any]] | dict[Any, dict[str, Any]] | None = None,
) -> list[str]:
    if component_indices is None:
        if component_mask is not None:
            component_indices = [
                idx for idx, keep in enumerate(component_mask.to_list()) if bool(keep)
            ]
        else:
            component_map = compute_components(df)
            component_label = component_map.get(start_idx)
            if component_label is None:
                return []
            component_indices = _component_members(component_map).get(component_label, [])

    row_cache = rows if rows is not None else df.to_dicts()
    if isinstance(row_cache, dict):
        component_rows = [
            (idx, row_cache[idx])
            for idx in component_indices
            if idx in row_cache and isinstance(row_cache[idx], dict)
        ]
    else:
        component_rows = _row_items(row_cache, component_indices)
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
            errors.append(f"source transaction {source_id} has multiple accepted match outcomes")
        if any(action in SOURCE_MATCH_ACTIONS for action in group_actions) and any(
            action in SOURCE_DELETE_ACTIONS for action in group_actions
        ):
            errors.append(f"source transaction {source_id} is both matched and deleted")

    for target_id in sorted({value for value in target_ids if value}):
        group_actions = [action for tid, action in zip(target_ids, actions, strict=False) if tid == target_id]
        if sum(1 for action in group_actions if action in TARGET_MATCH_ACTIONS) > 1:
            errors.append(f"target transaction {target_id} has multiple accepted match outcomes")
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
        ("multiple accepted match outcomes" in error) or ("both matched and deleted" in error)
        for error in combined_errors
    ):
        return "Contradiction in component"
    if any(
        ("institutional" in error.casefold()) and ("source" in error.casefold())
        for error in combined_errors
    ):
        return "Institutional source mutation"
    if action == NO_DECISION or any("No decision" in error for error in combined_errors):
        return "Decision required"
    if any(("missing" in error) and ("payee" in error) for error in row_errors):
        return "Missing payee"
    if any(("missing" in error) and ("category" in error) for error in row_errors):
        return "Missing category"
    return "None"


def blocker_series(df: pl.DataFrame) -> pl.Series:
    blocker_values, _ = blocker_series_with_components(df)
    return blocker_values


def allowed_decision_actions(row: Any) -> list[str]:
    workflow_type = str(_row_get(row, "workflow_type", "") or "").strip().casefold()
    source_present = _truthy(_required_row_value(row, "source_present"))
    target_present = _truthy(_required_row_value(row, "target_present"))

    actions = [NO_DECISION, "ignore_row"]
    if source_present and target_present:
        actions = [NO_DECISION, "keep_match", "delete_source", "delete_target", "delete_both", "ignore_row"]
    elif source_present and not target_present:
        actions = [NO_DECISION, "create_target", "delete_source", "ignore_row"]
    elif target_present and not source_present:
        actions = [NO_DECISION, "create_source", "delete_target", "ignore_row"]

    if workflow_type == "institutional":
        if target_present and not source_present:
            actions = [NO_DECISION, "update_target", "delete_target", "ignore_row"]
        else:
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
    touched = [idx for idx in dict.fromkeys(indices) if isinstance(idx, int) and 0 <= idx < len(edited_df)]
    if not touched:
        return edited_df, []

    if component_map is None:
        component_map = compute_components(edited_df)

    updated, affected_indices = review_state.apply_review_flag(
        edited_df,
        touched,
        reviewed=reviewed,
        component_map=component_map,
    )
    if not affected_indices:
        return edited_df, []

    if not reviewed:
        return updated, []

    errors: list[str] = []
    component_indices_map = _component_members(component_map)
    seen_components: set[int] = set()
    for idx in touched:
        component_label = component_map.get(idx)
        if component_label is None or component_label in seen_components:
            continue
        seen_components.add(component_label)
        component_indices = component_indices_map.get(component_label, [])
        component_rows = {
            current_idx: updated.row(current_idx, named=True)
            for current_idx in component_indices
        }
        row_errors_by_index = {
            current_idx: validate_row(row)[0]
            for current_idx, row in component_rows.items()
        }
        errors.extend(
            review_component_errors(
                updated,
                idx,
                component_indices=component_indices,
                row_errors_by_index=row_errors_by_index,
                rows=component_rows,
            )
        )

    if errors:
        reverted, _ = review_state.apply_review_flag(
            updated,
            touched,
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
    touched = [idx for idx in dict.fromkeys(indices) if isinstance(idx, int) and 0 <= idx < len(edited_df)]
    if not touched:
        return edited_df, [], []

    if not reviewed:
        updated, errors = apply_review_state(
            edited_df,
            touched,
            reviewed=reviewed,
            component_map=component_map,
        )
        return updated, errors, touched if not errors else []

    working = edited_df
    reviewed_indices: list[Any] = []
    errors: list[str] = []
    if component_map is None:
        component_map = compute_components(working)

    grouped_indices: dict[int, list[Any]] = {}
    for idx in touched:
        label = component_map.get(idx)
        if label is None:
            continue
        grouped_indices.setdefault(label, []).append(idx)

    for component_label in sorted(grouped_indices):
        component_indices = grouped_indices[component_label]
        updated, component_errors = apply_review_state(
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


def inconsistent_fingerprints(df: pl.DataFrame) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []

    for side in ["source", "target"]:
        id_col = f"{side}_row_id"
        payee_col = f"{side}_payee_selected"
        category_col = f"{side}_category_selected"
        if id_col not in df.columns or payee_col not in df.columns or category_col not in df.columns:
            continue
        grouped = (
            df.select(
                pl.col(id_col).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().alias("row_id"),
                pl.concat_str(
                    [
                        pl.col(payee_col).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars(),
                        pl.lit("||"),
                        pl.col(category_col).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars(),
                    ]
                ).alias("_combo"),
            )
            .filter(pl.col("row_id").ne(""))
            .group_by("row_id")
            .agg(pl.col("_combo").n_unique().alias("combo_count"))
            .filter(pl.col("combo_count") > 1)
            .with_columns(pl.lit(side).alias("side"))
        )
        if not grouped.is_empty():
            frames.append(grouped.select("side", "row_id", "combo_count"))

    if not frames:
        return pl.DataFrame({"side": [], "row_id": [], "combo_count": []})
    return pl.concat(frames)
